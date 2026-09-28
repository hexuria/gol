//! Starting runs on the queue: `enqueue`, which `create_run` and the owned
//! spawner share, and `OwnedSpawner`, which carries out an authorized
//! delegation (Phase 1.2).
use std::collections::BTreeMap;
use std::sync::Arc;

use harness::{AgentSpawner, ChildRequest, StoreError};
use protocol::{FailureClass, RunId, RunSpec, SESSION_ID};

use crate::inference::{queued_events, run_failed_event};
use crate::queue::RedisRunQueue;
use crate::store::{PutRun, RunStore, StoredRun};

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

/// Queues `spec` as a new run. The run is pending before it is stored, so a
/// process that dies between the store and the push leaves it to the sweep
/// (C6). A run already stored under this id is not pushed again; its pending
/// entry is left for the sweep, which pushes it if it is still waiting and
/// drops the entry if it has moved on.
pub(crate) fn enqueue(
    store: &dyn RunStore,
    queue: &RedisRunQueue,
    spec: &RunSpec,
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
            // Stored but never to be picked up: end it, so it is not left
            // open. Its pending entry stays; the sweep finds it ended.
            end(store, spec, format!("queue push failed: {error}"));
            return Err(EnqueueError::Push(error));
        }
    }
    Ok(put)
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
    fn start(&self, request: ChildRequest<'_>) -> Result<RunId, String> {
        let queue = self
            .queue
            .as_ref()
            .ok_or_else(|| "delegation needs the run queue".to_string())?;
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
        match enqueue(self.store.as_ref(), queue, &spec) {
            Ok(_) => Ok(spec.run_id),
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
