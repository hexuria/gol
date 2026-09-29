//! Starting runs on the queue: `enqueue`, which `create_run` and the owned
//! spawner share, and `OwnedSpawner`, which carries out an authorized
//! delegation (Phase 1.2).
use std::collections::BTreeMap;
use std::sync::Arc;

use harness::{AgentSpawner, ChildRequest, StartedChild, StoreError};
use protocol::{FailureClass, RunId, RunSpec, SESSION_ID};

use crate::inference::{queued_events, run_failed_event};
use crate::queue::RedisRunQueue;
use crate::store::{is_terminal, PutRun, RunStore, StoredRun};

/// Why `enqueue` did not queue a run.
#[derive(Debug)]
pub(crate) enum EnqueueError {
    /// Redis refused the pend: nothing was stored.
    Queue(String),
    /// The store failed; the run may have been stored, and was ended if so.
    Store(StoreError),
    /// The run is stored but the push failed; the run was ended.
    Push(String),
}

/// What `enqueue` does when the push fails after the store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OnPushFailure {
    /// End the run and report the error: `create_run`, whose id is fresh and
    /// whose client is told the create failed.
    End,
    /// Leave the run stored and pending for the sweep to push, and report it
    /// stored: a child, whose id is fixed by its request and could never be
    /// started again once ended.
    LeaveToSweep,
}

/// Queues `spec` as a new run. The run is pending before it is stored, so a
/// process that dies between the store and the push leaves it to the sweep
/// (C6). A run already stored under this id is not pushed again: see
/// `settle_existing`.
pub(crate) fn enqueue(
    store: &dyn RunStore,
    queue: &RedisRunQueue,
    spec: &RunSpec,
    on_push_failure: OnPushFailure,
) -> Result<PutRun, EnqueueError> {
    let run_id = spec.run_id;
    queue.pend(run_id).map_err(EnqueueError::Queue)?;
    let put = match store.put_run(StoredRun {
        spec: spec.clone(),
        events: queued_events(spec),
    }) {
        Ok(put) => put,
        Err(error) => {
            // The put may have committed before it failed: end the run in case
            // it did, so it is not left queued and never pushed. A store that
            // holds no such run refuses the append.
            end(store, spec, "store unavailable".to_string());
            return Err(EnqueueError::Store(error));
        }
    };
    if put == PutRun::Stored {
        if let Err(error) = queue.push(run_id) {
            match on_push_failure {
                // Stored but never to be picked up: end it, so it is not left
                // open. Its pending entry stays; the sweep finds it ended.
                OnPushFailure::End => {
                    end(store, spec, format!("queue push failed: {error}"));
                    return Err(EnqueueError::Push(error));
                }
                OnPushFailure::LeaveToSweep => {
                    eprintln!("gol: push run {run_id} failed, left to the sweep: {error}");
                }
            }
        }
    }
    Ok(put)
}

/// After an `enqueue` found `spec`'s run already stored, and put it on the
/// pending set again: take it off pending if it is already queued, claimed or
/// past waiting, so no sweep pushes it a second time. A run still waiting and
/// on no list lost its producer between the store and the push; its entry
/// stays for the sweep.
fn settle_existing(
    queue: &RedisRunQueue,
    run_id: RunId,
    events: &[protocol::Event],
) -> Result<(), String> {
    let waiting = events.iter().all(|event| {
        matches!(
            event.payload,
            protocol::EventPayload::RunCreated
                | protocol::EventPayload::RunQueued
                | protocol::EventPayload::UserMessage { .. }
        )
    });
    if !waiting || queue.queued_or_claimed(run_id)? {
        queue.unpend(run_id)?;
    }
    Ok(())
}

/// Whether a stored log ended before a worker ever scheduled it.
fn never_ran(events: &[protocol::Event]) -> bool {
    events.iter().any(|event| is_terminal(&event.payload))
        && !events
            .iter()
            .any(|event| matches!(event.payload, protocol::EventPayload::RunScheduled))
}

fn end(store: &dyn RunStore, spec: &RunSpec, message: String) {
    let failed = run_failed_event(spec, FailureClass::Infrastructure, message);
    if let Err(error) = store.append_events(spec.run_id, vec![failed]) {
        eprintln!("gol: could not end run {}: {error}", spec.run_id);
    }
}

/// Starts a delegation's child as a queued run, owned by the parent's owner.
/// The target must be an agent that owner holds (decision 7A). The child
/// gets the capabilities its manifest asks for that the parent also holds
/// (14), the parent's placement, work model and session (15), and the
/// budget the driver carved (13).
pub struct OwnedSpawner {
    store: Arc<dyn RunStore>,
    queue: Option<Arc<RedisRunQueue>>,
}

impl OwnedSpawner {
    /// Without a queue, every delegation is refused.
    pub fn new(store: Arc<dyn RunStore>, queue: Option<Arc<RedisRunQueue>>) -> Self {
        Self { store, queue }
    }
}

impl AgentSpawner for OwnedSpawner {
    /// The reasons of a refusal go into the parent's run log, which clients
    /// read: they are fixed words, and a store or queue error's detail goes
    /// to stderr only.
    fn start(&self, request: ChildRequest<'_>) -> Result<StartedChild, String> {
        let spec = self.child_spec(request)?;
        self.enqueue_child(request.parent, spec)
    }
}

impl OwnedSpawner {
    /// The child `request` asks for: a run of an agent the parent's owner
    /// holds, with the capabilities both allow. Its id is derived from the
    /// request, so asking again names the same child.
    pub(crate) fn child_spec(&self, request: ChildRequest<'_>) -> Result<RunSpec, String> {
        if self.queue.is_none() {
            return Err("delegation needs the run queue".to_string());
        }
        let parent = request.parent;
        let agent = match self.store.agent(request.agent_id) {
            Ok(Some(agent)) => agent,
            Ok(None) => return Err("no such agent".to_string()),
            Err(error) => {
                eprintln!("gol: delegate from run {}: {error}", parent.run_id);
                return Err("store unavailable".to_string());
            }
        };
        if !agent.owner.is(&parent.owner) {
            return Err("agent belongs to another owner".to_string());
        }
        let capabilities = agent
            .manifest
            .required_capabilities
            .iter()
            .filter(|capability| parent.capabilities.contains(capability))
            .cloned()
            .collect();
        let metadata: BTreeMap<String, String> = parent
            .metadata
            .get(SESSION_ID)
            .map(|session| (SESSION_ID.to_string(), session.clone()))
            .into_iter()
            .collect();
        let spec = RunSpec::builder()
            .owner(parent.owner.clone())
            .agent(agent.manifest.id, agent.manifest.version.clone())
            .input(request.input)
            .placement(parent.placement)
            .work_model(parent.work_model.clone())
            .capabilities(capabilities)
            .limits(request.limits)
            .metadata(metadata)
            .child_of(parent, request.step)
            .build();
        Ok(spec)
    }

    /// Stores and queues `spec`, a child of `parent`, once.
    pub(crate) fn enqueue_child(
        &self,
        parent: &RunSpec,
        spec: RunSpec,
    ) -> Result<StartedChild, String> {
        let queue = self
            .queue
            .as_ref()
            .ok_or_else(|| "delegation needs the run queue".to_string())?;
        match enqueue(
            self.store.as_ref(),
            queue,
            &spec,
            OnPushFailure::LeaveToSweep,
        ) {
            Ok(PutRun::Stored) => Ok(StartedChild {
                run_id: spec.run_id,
                limits: spec.limits,
            }),
            // Asked for before: the same child, with the limits it was stored
            // with. One that ended before any worker scheduled it will never
            // run, so it is not reported as started.
            Ok(PutRun::Existed) => match self.store.run(spec.run_id) {
                Ok(Some(child)) if never_ran(&child.events) => {
                    // Asking put it back on pending; it will never run.
                    if let Err(error) = queue.unpend(spec.run_id) {
                        eprintln!("gol: delegate from run {}: {error}", parent.run_id);
                    }
                    Err("the child could not be started".to_string())
                }
                Ok(Some(child)) => {
                    if let Err(error) = settle_existing(queue, spec.run_id, &child.events) {
                        eprintln!("gol: delegate from run {}: {error}", parent.run_id);
                    }
                    Ok(StartedChild {
                        run_id: spec.run_id,
                        limits: child.spec.limits,
                    })
                }
                Ok(None) => Err("store unavailable".to_string()),
                Err(error) => {
                    eprintln!("gol: delegate from run {}: {error}", parent.run_id);
                    Err("store unavailable".to_string())
                }
            },
            Err(error) => {
                eprintln!("gol: delegate from run {}: {error:?}", parent.run_id);
                Err(match error {
                    EnqueueError::Queue(_) => "queue unavailable",
                    EnqueueError::Store(_) => "store unavailable",
                    EnqueueError::Push(_) => "queue push failed",
                }
                .to_string())
            }
        }
    }
}
