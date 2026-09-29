//! Triggers (Phase 4.1, decisions 68A-73A): what a fire starts, and a
//! webhook trigger's secret.
use std::collections::BTreeMap;

use protocol::{Limits, Owner, RunId, RunSpec, SESSION_ID};

use harness::StoreError;

use crate::inference::run_cancelled_event;
use crate::queue::RedisRunQueue;
use crate::spawner::{enqueue, EnqueueError, OnPushFailure};
use crate::store::{RunStore, StoredTrigger, TriggerId, TriggerKind};

/// The metadata key the server sets on a run a trigger started (decision
/// 68A): its trigger's id. A caller that sends it is refused.
pub const TRIGGER_KEY: &str = "gol.trigger";

/// The thread every run of trigger `id` belongs to, so its fires are the
/// cards of one board.
pub fn trigger_thread(id: TriggerId) -> String {
    format!("trigger-{id}")
}

/// What a fire did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fired {
    /// Queued this run.
    Run(RunId),
    /// Nothing: the trigger is paused.
    Paused,
    /// Nothing: no such trigger of the principal's.
    NotFound,
    /// Nothing: its agent is no longer the principal's.
    AgentNotFound,
}

/// Why a fire failed, for its caller to retry or not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FireError {
    /// Nothing was stored: firing again is safe.
    NotStored(String),
    /// The store failed after the run may have been stored; a stored run is
    /// pending, and the queue sweep pushes it.
    MaybeStored(String),
}

/// Fires `owner`'s trigger `id` (decision 68A): an ordinary queued run of
/// its agent, at the version the owner keeps now, with its input and run
/// template, in the trigger's thread and marked `TRIGGER_KEY`. Queued the
/// way a child run is: pending before it is stored, and left to the queue
/// sweep if the push fails. A paused trigger does not fire.
///
/// The trigger is read again once the run is stored. A trigger paused in
/// between (the owner's stop pauses before it records itself) has its run
/// cancelled: without that, a run stored after the stop would escape it.
/// A pause after that read comes before the stop, which then covers the
/// run.
pub fn fire_trigger(
    store: &dyn RunStore,
    queue: &RedisRunQueue,
    owner: &Owner,
    id: TriggerId,
) -> Result<Fired, FireError> {
    let unread = |error: StoreError| FireError::NotStored(error.to_string());
    let triggers = store
        .triggers()
        .ok_or_else(|| FireError::NotStored("the store keeps no triggers".to_string()))?;
    let Some(trigger) = triggers.trigger(owner, id).map_err(unread)? else {
        return Ok(Fired::NotFound);
    };
    if !trigger.enabled {
        return Ok(Fired::Paused);
    }
    let Some(agent) = store
        .agent(trigger.agent_id)
        .map_err(unread)?
        .filter(|agent| agent.owner.is(&trigger.owner))
    else {
        return Ok(Fired::AgentNotFound);
    };
    let spec = run_of(
        &trigger,
        agent.manifest.version,
        agent.manifest.required_capabilities,
    );
    enqueue(store, queue, &spec, OnPushFailure::LeaveToSweep).map_err(|error| match error {
        EnqueueError::Queue(error) => FireError::NotStored(error),
        EnqueueError::Push(error) => FireError::MaybeStored(error),
        EnqueueError::Store(error) => FireError::MaybeStored(error.to_string()),
    })?;
    let paused = triggers
        .trigger(owner, id)
        .map_err(|error| FireError::MaybeStored(error.to_string()))?
        .is_some_and(|trigger| !trigger.enabled);
    if paused {
        store
            .append_events(spec.run_id, vec![run_cancelled_event(&spec)])
            .map_err(|error| FireError::MaybeStored(error.to_string()))?;
        return Ok(Fired::Paused);
    }
    Ok(Fired::Run(spec.run_id))
}

/// The run a fire of `trigger` starts.
fn run_of(
    trigger: &StoredTrigger,
    version: String,
    capabilities: Vec<protocol::Capability>,
) -> RunSpec {
    let metadata: BTreeMap<String, String> = [
        (SESSION_ID.to_string(), trigger_thread(trigger.id)),
        (TRIGGER_KEY.to_string(), trigger.id.to_string()),
    ]
    .into_iter()
    .collect();
    RunSpec::builder()
        .owner(trigger.owner.clone())
        .agent(trigger.agent_id, version)
        .input(trigger.input.clone())
        .placement(trigger.placement)
        .work_model(trigger.work_model.clone())
        .capabilities(capabilities)
        .limits(trigger.limits.unwrap_or(Limits {
            max_steps: 8,
            max_model_calls: 4,
        }))
        .metadata(metadata)
        .build()
}

/// The shortest webhook key the server takes: every owner sees a message and
/// its tag (their trigger's id and secret), so a short key could be found
/// offline, and every trigger's secret with it.
pub const MIN_WEBHOOK_KEY_BYTES: usize = 32;

/// The most triggers one principal keeps.
pub const MAX_TRIGGERS: usize = 100;

/// A webhook trigger's secret (decision 73A): HMAC-SHA256, under the
/// server's webhook key, of `gol-webhook-v1:<trigger id>:<rotation>`, as
/// lowercase hex. Never stored: shown once when made or rotated, and derived
/// again to check a request. `None` for a schedule trigger.
pub fn webhook_secret(key: &[u8], trigger: &StoredTrigger) -> Option<String> {
    let TriggerKind::Webhook { rotation } = trigger.kind else {
        return None;
    };
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key);
    let message = format!("gol-webhook-v1:{}:{rotation}", trigger.id);
    let tag = ring::hmac::sign(&key, message.as_bytes());
    Some(
        tag.as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
    )
}
