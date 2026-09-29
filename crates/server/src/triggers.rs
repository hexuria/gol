//! Triggers (Phase 4.1, decisions 68A-73A): what a fire starts, and a
//! webhook trigger's secret.
use std::collections::BTreeMap;

use protocol::{Limits, Owner, RunId, RunSpec, SESSION_ID};

use std::cell::Cell;

use harness::StoreError;

use crate::queue::RedisRunQueue;
use crate::spawner::{enqueue_unless, EnqueueError, Gated, OnPushFailure};
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
    /// Run `run` may be stored. It was never pushed unless it was queued
    /// before the failure; a caller that finds the trigger paused ends it.
    MaybeStored { run: RunId, message: String },
}

/// Fires `owner`'s trigger `id` (decision 68A): an ordinary queued run of
/// its agent, at the version the owner keeps now, with its input and run
/// template, in the trigger's thread and marked `TRIGGER_KEY`. Queued the
/// way a child run is: pending before it is stored, and left to the queue
/// sweep if the push fails. A paused trigger does not fire.
///
/// The trigger is read again once the run is stored and before it is
/// pushed. A trigger paused (the owner's stop pauses before it records
/// itself) or deleted in between holds the run: it is cancelled before any
/// worker can see it, and never pushed. A pause after that read comes
/// before the stop's record, and the stop covers a run stored before it. A
/// read that fails holds the run too.
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
    let run = spec.run_id;
    // Why the run was held, if it was: `Err` for a read that failed.
    let held: Cell<Option<Result<Fired, String>>> = Cell::new(None);
    let hold = || {
        let why = match triggers.trigger(owner, id) {
            Ok(Some(trigger)) if trigger.enabled => return false,
            Ok(Some(_)) => Ok(Fired::Paused),
            Ok(None) => Ok(Fired::NotFound),
            Err(error) => Err(error.to_string()),
        };
        held.set(Some(why));
        true
    };
    let gated = enqueue_unless(store, queue, &spec, OnPushFailure::LeaveToSweep, &hold).map_err(
        |error| match error {
            EnqueueError::Queue(message) => FireError::NotStored(message),
            EnqueueError::Push(message) | EnqueueError::Held(message) => {
                FireError::MaybeStored { run, message }
            }
            EnqueueError::Store(error) => FireError::MaybeStored {
                run,
                message: error.to_string(),
            },
        },
    )?;
    match (gated, held.take()) {
        (Gated::Held, Some(Err(message))) => Err(FireError::MaybeStored {
            run,
            message: format!("the trigger could not be read again, so the run was held: {message}"),
        }),
        (Gated::Held, Some(Ok(fired))) => Ok(fired),
        _ => Ok(Fired::Run(run)),
    }
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
