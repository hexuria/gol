//! Triggers (Phase 4.1, decisions 68A-73A): what a fire starts, and a
//! webhook trigger's secret.
use std::collections::BTreeMap;

use protocol::{Limits, Owner, RunId, RunSpec, SESSION_ID};

use std::cell::Cell;

use harness::StoreError;

use crate::queue::RedisRunQueue;
use crate::spawner::{enqueue_fire, settle_fire, EnqueueError, Fire, Gate};
use crate::store::{RunStore, StoredTrigger, TriggerId, TriggerKind, TriggerStore};

/// The metadata key the server sets on a run a trigger started (decision
/// 68A): its trigger's id. A caller that sends it is refused.
pub const TRIGGER_KEY: &str = "gol.trigger";

/// The trigger's generation a fire read (Phase 4.2), on its run.
const GENERATION_KEY: &str = "gol.generation";

/// The tick a scheduled fire fired (Phase 4.2), on its run.
const TICK_KEY: &str = "gol.tick";

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
    /// Nothing: the tick it was asked to fire is no longer the trigger's
    /// next (another scheduler fired it and moved it on, or it was resumed
    /// to a later one).
    Moved,
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
/// template, in the trigger's thread and marked `TRIGGER_KEY`, through
/// `spawner::enqueue_fire`. A paused trigger does not fire.
///
/// The trigger is read again once the run is stored and before it is
/// pushed. A trigger paused (the owner's stop pauses before it records
/// itself) or deleted in between holds the run: it is cancelled before any
/// worker can see it, and never pushed. A pause after that read comes
/// before the stop's record, and the stop covers a run stored before it. A
/// read that fails leaves the run stored and unpushed, and a failed push
/// leaves it pending: a scheduled fire's next try of the tick (the same run
/// id) gates and pushes it.
pub fn fire_trigger(
    store: &dyn RunStore,
    queue: &RedisRunQueue,
    owner: &Owner,
    id: TriggerId,
) -> Result<Fired, FireError> {
    fire(store, queue, owner, id, None).map(|(fired, _)| fired)
}

/// What keys a fire's run id, when something does.
enum Key {
    /// A scheduled fire: its run id, and its tick.
    Tick(RunId, i64),
    /// A webhook's fire (Phase 4.3): its run id, the sender's event id, and
    /// the body, which follows the trigger's input.
    Event(RunId, String, String),
}

/// The namespace of a webhook fire's run id (UUID v5 over the trigger and
/// the sender's event id).
const EVENT_RUN_NAMESPACE: uuid::Uuid =
    uuid::Uuid::from_u128(0x7d3a_19c4_e2b8_4a56_9f01_3c6e_8b24_d5a7);

/// The event id a webhook fire answered, on its run.
const EVENT_KEY: &str = "gol.event";

/// What a webhook request did (Phase 4.3, decisions 76A and 80A).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hooked {
    /// Queued this run for the event: its first delivery, or a retry that
    /// pushed a run an earlier delivery stored but never pushed.
    Started(RunId),
    /// A replay of an event whose run is queued, running or done.
    Duplicate(RunId),
    /// Nothing: the trigger is paused, or was paused or resumed while the
    /// event's run waited, so the run was held, now or on an earlier
    /// delivery.
    Refused,
    /// Nothing: its agent is no longer the owner's.
    AgentNotFound,
    /// Nothing: the trigger is gone.
    NotFound,
}

/// How a webhook event's stored run answers another delivery of the event
/// (80A), from its log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Replay {
    /// Only its queued events: settled, or queued already.
    Waiting,
    /// Held (cancelled) before any worker scheduled it: refused again.
    Refused,
    /// Scheduled, or ended any other way: a duplicate.
    Duplicate,
}

fn replay_of(events: &[protocol::Event]) -> Replay {
    use protocol::EventPayload::{RunCancelled, RunScheduled};
    if crate::spawner::still_waiting(events) {
        Replay::Waiting
    } else if events
        .iter()
        .any(|event| matches!(event.payload, RunCancelled))
        && !events
            .iter()
            .any(|event| matches!(event.payload, RunScheduled))
    {
        Replay::Refused
    } else {
        Replay::Duplicate
    }
}

/// Fires webhook `trigger` for the sender's event `event` with `body`
/// (Phase 4.3, decisions 74A, 77A and 80A). The run's id comes from the
/// trigger and the event id, so a replay of the event fires nothing new;
/// its input is the trigger's input, a blank line, and the body. A replay
/// is answered from the event's stored run: queued, running or done is a
/// duplicate; held before it ran is refused again; still waiting (an
/// earlier delivery stored it and failed before its push) is settled as
/// the queue sweep settles it, by the generation it recorded.
pub fn fire_webhook(
    store: &dyn RunStore,
    queue: &RedisRunQueue,
    trigger: &StoredTrigger,
    event: &str,
    body: &str,
) -> Result<Hooked, FireError> {
    let mut name = trigger.id.as_uuid().as_bytes().to_vec();
    name.extend_from_slice(event.as_bytes());
    let run = RunId::from_uuid(uuid::Uuid::new_v5(&EVENT_RUN_NAMESPACE, &name));
    let stored = store
        .run(run)
        .map_err(|error| FireError::NotStored(error.to_string()))?;
    if let Some(stored) = stored {
        match replay_of(&stored.events) {
            Replay::Refused => return Ok(Hooked::Refused),
            Replay::Duplicate => return Ok(Hooked::Duplicate(run)),
            Replay::Waiting => {}
        }
        return match settle_fired_run(queue, store, &stored.spec) {
            Ok(Fire::Pushed) => Ok(Hooked::Started(run)),
            Ok(Fire::Found) => Ok(Hooked::Duplicate(run)),
            Ok(Fire::Held) => Ok(Hooked::Refused),
            Err(message) => Err(FireError::MaybeStored { run, message }),
        };
    }
    let key = Key::Event(run, event.to_string(), body.to_string());
    Ok(
        match fire(store, queue, &trigger.owner, trigger.id, Some(key))? {
            (Fired::Run(run), true) => Hooked::Started(run),
            // Another delivery of the event stored the run first: answered
            // from its log, as a replay is.
            (Fired::Run(run), false) => {
                let events = store
                    .run(run)
                    .map_err(|error| FireError::MaybeStored {
                        run,
                        message: error.to_string(),
                    })?
                    .map(|stored| stored.events)
                    .unwrap_or_default();
                match replay_of(&events) {
                    Replay::Waiting => Hooked::Started(run),
                    Replay::Refused => Hooked::Refused,
                    Replay::Duplicate => Hooked::Duplicate(run),
                }
            }
            (Fired::Paused | Fired::Moved, _) => Hooked::Refused,
            (Fired::AgentNotFound, _) => Hooked::AgentNotFound,
            (Fired::NotFound, _) => Hooked::NotFound,
        },
    )
}

/// The namespace of a scheduled fire's run id (UUID v5 over the trigger and
/// its tick).
const TICK_RUN_NAMESPACE: uuid::Uuid =
    uuid::Uuid::from_u128(0x2c1e_7a90_5b3d_4f61_8e2a_6d0c_9b47_13f5);

/// `fire_trigger` for trigger `id`'s tick at `tick_ms` (Phase 4.2): the
/// run's id comes from the trigger and the tick, so the tick fired twice
/// (two schedulers, or one that died between the fire and moving the
/// trigger to its next tick) is one run, stored once and pushed once.
pub fn fire_trigger_at(
    store: &dyn RunStore,
    queue: &RedisRunQueue,
    owner: &Owner,
    id: TriggerId,
    tick_ms: i64,
) -> Result<Fired, FireError> {
    let mut name = id.as_uuid().as_bytes().to_vec();
    name.extend_from_slice(&tick_ms.to_le_bytes());
    let run = RunId::from_uuid(uuid::Uuid::new_v5(&TICK_RUN_NAMESPACE, &name));
    fire(store, queue, owner, id, Some(Key::Tick(run, tick_ms))).map(|(fired, _)| fired)
}

fn fire(
    store: &dyn RunStore,
    queue: &RedisRunQueue,
    owner: &Owner,
    id: TriggerId,
    key: Option<Key>,
) -> Result<(Fired, bool), FireError> {
    let unread = |error: StoreError| FireError::NotStored(error.to_string());
    let triggers = store
        .triggers()
        .ok_or_else(|| FireError::NotStored("the store keeps no triggers".to_string()))?;
    let Some(trigger) = triggers.trigger(owner, id).map_err(unread)? else {
        return Ok((Fired::NotFound, false));
    };
    if !trigger.enabled {
        return Ok((Fired::Paused, false));
    }
    if let Some(Key::Tick(_, at)) = key {
        if trigger.next_fire_ms != Some(at) {
            return Ok((Fired::Moved, false));
        }
    }
    let Some(agent) = store
        .agent(trigger.agent_id)
        .map_err(unread)?
        .filter(|agent| agent.owner.is(&trigger.owner))
    else {
        return Ok((Fired::AgentNotFound, false));
    };
    let mut spec = run_of(
        &trigger,
        agent.manifest.version,
        agent.manifest.required_capabilities,
    );
    // A top-level run: its lineage names no root, so the id is its own.
    match key {
        Some(Key::Tick(run_id, at)) => {
            spec.run_id = run_id;
            spec.metadata.insert(TICK_KEY.to_string(), at.to_string());
        }
        Some(Key::Event(run_id, event, body)) => {
            spec.run_id = run_id;
            spec.input = format!("{}\n\n{body}", spec.input);
            spec.metadata.insert(EVENT_KEY.to_string(), event);
        }
        None => {}
    }
    let run = spec.run_id;
    // Why the run was held, if it was.
    let held: Cell<Option<Fired>> = Cell::new(None);
    let gate = || gate_of(triggers, owner, id, trigger.generation, &held);
    let fired = enqueue_fire(store, queue, &spec, &gate).map_err(|error| match error {
        EnqueueError::Queue(message) => FireError::NotStored(message),
        EnqueueError::Push(message) | EnqueueError::Held(message) => {
            FireError::MaybeStored { run, message }
        }
        EnqueueError::Store(error) => FireError::MaybeStored {
            run,
            message: error.to_string(),
        },
    })?;
    Ok(match (fired, held.take()) {
        (Fire::Held, Some(why)) => (why, false),
        (fired, _) => (Fired::Run(run), fired == Fire::Pushed),
    })
}

/// The gate of a fire of trigger `id` that read it at `generation` (Phase
/// 4.2): open while the trigger runs at that generation; shut (the run is
/// held) once it is paused, resumed since (a resumed trigger owes nothing
/// for the ticks it was paused) or gone. `held` says which.
fn gate_of(
    triggers: &dyn TriggerStore,
    owner: &Owner,
    id: TriggerId,
    generation: u64,
    held: &Cell<Option<Fired>>,
) -> Gate {
    match triggers.trigger(owner, id) {
        Ok(Some(trigger)) if trigger.enabled && trigger.generation == generation => Gate::Open,
        Ok(Some(trigger)) => {
            held.set(Some(if trigger.enabled {
                Fired::Moved
            } else {
                Fired::Paused
            }));
            Gate::Shut
        }
        Ok(None) => {
            held.set(Some(Fired::NotFound));
            Gate::Shut
        }
        Err(error) => Gate::Unknown(format!(
            "the trigger could not be read again, so the run was not pushed: {error}"
        )),
    }
}

/// Settles a trigger's stored run the queue sweep found pending (Phase
/// 4.2): gated as its fire gates it, by the generation it recorded, then
/// pushed once or held. A run a server cannot place (its trigger or
/// generation unreadable) is held.
pub(crate) fn settle_fired_run(
    queue: &RedisRunQueue,
    store: &dyn RunStore,
    spec: &RunSpec,
) -> Result<Fire, String> {
    let triggers = store
        .triggers()
        .ok_or_else(|| "the store keeps no triggers".to_string())?;
    let id = spec
        .metadata
        .get(TRIGGER_KEY)
        .and_then(|id| id.parse::<TriggerId>().ok());
    let generation = spec
        .metadata
        .get(GENERATION_KEY)
        .and_then(|generation| generation.parse::<u64>().ok());
    let (Some(id), Some(generation)) = (id, generation) else {
        return settle_fire(store, queue, spec, &|| Gate::Shut)
            .map_err(|error| format!("{error:?}"));
    };
    let held = Cell::new(None);
    settle_fire(store, queue, spec, &|| {
        gate_of(triggers, &spec.owner, id, generation, &held)
    })
    .map_err(|error| format!("{error:?}"))
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
        (GENERATION_KEY.to_string(), trigger.generation.to_string()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inference::{queued_events, run_cancelled_event, run_failed_event, system_event};
    use protocol::{
        AgentId, CredentialSource, EventPayload, ExecutionPlacement, FailureClass, ModelProvider,
        WorkModel,
    };

    fn spec() -> RunSpec {
        RunSpec::builder()
            .owner(Owner::new("https://issuer.test", "user-1", "tenant-1"))
            .agent(AgentId::new(), "1")
            .input("hello")
            .placement(ExecutionPlacement::Local)
            .work_model(WorkModel {
                provider: ModelProvider::OpenAI,
                model_name: "gpt-test".to_string(),
                credential: CredentialSource::PlatformGateway,
            })
            .limits(Limits {
                max_steps: 4,
                max_model_calls: 1,
            })
            .build()
    }

    // A webhook event's run answers another delivery from its log (80A):
    // only queued, it waits; cancelled before any worker scheduled it, it
    // was held and is refused; scheduled, or ended any other way (a worker
    // fails a run it cannot start before scheduling it), it is a duplicate.
    #[test]
    fn a_replay_is_classified_from_the_runs_log() {
        let spec = spec();
        let queued = queued_events(&spec);
        assert_eq!(replay_of(&queued), Replay::Waiting);
        let mut held = queued.clone();
        held.push(run_cancelled_event(&spec));
        assert_eq!(replay_of(&held), Replay::Refused);
        let mut failed = queued.clone();
        failed.push(run_failed_event(
            &spec,
            FailureClass::Infrastructure,
            "started too often".to_string(),
        ));
        assert_eq!(replay_of(&failed), Replay::Duplicate);
        let mut scheduled = queued.clone();
        scheduled.push(system_event(&spec, EventPayload::RunScheduled));
        assert_eq!(replay_of(&scheduled), Replay::Duplicate);
        scheduled.push(run_cancelled_event(&spec));
        assert_eq!(replay_of(&scheduled), Replay::Duplicate, "stopped mid-run");
    }
}
