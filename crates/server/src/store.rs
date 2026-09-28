use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use harness::StoreError;
use protocol::{AgentId, ArtifactId, Capability, Event, EventPayload, Owner, RunId, RunSpec};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AgentManifest {
    pub id: AgentId,
    pub version: String,
    pub instructions: String,
    pub tools: Vec<String>,
    pub required_capabilities: Vec<Capability>,
}

/// A manifest and the principal that stored it. Only that principal may
/// replace it or start runs of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredAgent {
    pub manifest: AgentManifest,
    pub owner: Owner,
}

/// What `RunStore::put_agent` did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutAgent {
    /// Stored, as a new agent or replacing the caller's own.
    Stored,
    /// Another principal owns that agent id. Nothing was written.
    OwnedByOther,
}

#[derive(Clone, Debug)]
pub struct StoredRun {
    pub spec: RunSpec,
    pub events: Vec<Event>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredArtifact {
    pub id: ArtifactId,
    pub run_id: RunId,
    pub name: String,
    pub body: Vec<u8>,
}

/// What `RunStore::append_events` did. The log only grows, and it ends at the
/// first terminal event (formal/runlog/RunLog.tla).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Append {
    Appended,
    /// The stored log already holds a terminal event (see `is_terminal`).
    /// Nothing was written.
    Terminal,
    /// No run is stored under that id. Nothing was written.
    Missing,
}

/// The payloads that end a run: `DispatchPhase::is_terminal` after the reducer.
pub fn is_terminal(payload: &EventPayload) -> bool {
    matches!(
        payload,
        EventPayload::RunCompleted { .. }
            | EventPayload::RunFailed { .. }
            | EventPayload::RunCancelled
            | EventPayload::RunExpired
    )
}

/// A batch of events, as `put_run` and `append_events` take it, holds at most
/// one terminal event: the log ends at the first.
pub(crate) fn check_one_terminal(events: &[Event]) -> Result<(), StoreError> {
    if events
        .iter()
        .filter(|event| is_terminal(&event.payload))
        .count()
        > 1
    {
        return Err(StoreError::new(
            "a batch holds more than one terminal event",
        ));
    }
    Ok(())
}

/// Every method can fail with a StoreError: the store is unreachable, or a
/// write's outcome is unknown. Callers report it and do not retry a write,
/// which may have committed.
pub trait RunStore: Send + Sync {
    /// Store a manifest, unless another principal already owns its id. The
    /// check and the write are one atomic step.
    fn put_agent(&self, agent: StoredAgent) -> Result<PutAgent, StoreError>;
    fn agent(&self, id: AgentId) -> Result<Option<StoredAgent>, StoreError>;
    /// Store a new run. A run already stored under that id keeps its spec and
    /// events, so a redelivered put cannot drop anything appended since. A
    /// batch with more than one terminal event is refused.
    fn put_run(&self, run: StoredRun) -> Result<(), StoreError>;
    /// Append `events` onto the stored run in one atomic step, unless the
    /// stored log is already terminal or the run is missing. `spec` and the
    /// events already stored never change. A batch with more than one
    /// terminal event is refused.
    fn append_events(&self, id: RunId, events: Vec<Event>) -> Result<Append, StoreError>;
    fn run(&self, id: RunId) -> Result<Option<StoredRun>, StoreError>;
    /// The run with at most `limit` of its events: those after the first
    /// `after`. A store that keeps events in order by row overrides this to
    /// read only the page.
    fn run_page(
        &self,
        id: RunId,
        after: usize,
        limit: usize,
    ) -> Result<Option<StoredRun>, StoreError> {
        Ok(self.run(id)?.map(|mut run| {
            run.events = run.events.into_iter().skip(after).take(limit).collect();
            run
        }))
    }
    fn put_artifact(&self, artifact: StoredArtifact) -> Result<(), StoreError>;
    fn artifact(&self, id: ArtifactId) -> Result<Option<StoredArtifact>, StoreError>;
}

/// The in-memory store. A poisoned lock (a writer panicked holding it) still
/// serves reads, since every write completes before its guard drops; a write
/// under a poisoned lock is refused as a StoreError (owner decision 3A).
#[derive(Default)]
pub struct InMemoryStore {
    agents: Mutex<HashMap<AgentId, StoredAgent>>,
    runs: Mutex<HashMap<RunId, StoredRun>>,
    artifacts: Mutex<HashMap<ArtifactId, StoredArtifact>>,
}

fn read<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(PoisonError::into_inner)
}

fn write<'a, T>(lock: &'a Mutex<T>, name: &str) -> Result<MutexGuard<'a, T>, StoreError> {
    lock.lock()
        .map_err(|_| StoreError::new(format!("{name} store lock poisoned")))
}

impl RunStore for InMemoryStore {
    fn put_agent(&self, agent: StoredAgent) -> Result<PutAgent, StoreError> {
        let mut agents = write(&self.agents, "agent")?;
        let id = agent.manifest.id;
        if agents
            .get(&id)
            .is_some_and(|stored| !stored.owner.is(&agent.owner))
        {
            return Ok(PutAgent::OwnedByOther);
        }
        agents.insert(id, agent);
        Ok(PutAgent::Stored)
    }

    fn agent(&self, id: AgentId) -> Result<Option<StoredAgent>, StoreError> {
        Ok(read(&self.agents).get(&id).cloned())
    }

    fn put_run(&self, run: StoredRun) -> Result<(), StoreError> {
        check_one_terminal(&run.events)?;
        write(&self.runs, "run")?
            .entry(run.spec.run_id)
            .or_insert(run);
        Ok(())
    }

    fn append_events(&self, id: RunId, events: Vec<Event>) -> Result<Append, StoreError> {
        check_one_terminal(&events)?;
        let mut runs = write(&self.runs, "run")?;
        let Some(stored) = runs.get_mut(&id) else {
            return Ok(Append::Missing);
        };
        if stored
            .events
            .iter()
            .any(|event| is_terminal(&event.payload))
        {
            return Ok(Append::Terminal);
        }
        stored.events.extend(events);
        Ok(Append::Appended)
    }

    fn run(&self, id: RunId) -> Result<Option<StoredRun>, StoreError> {
        Ok(read(&self.runs).get(&id).cloned())
    }

    fn put_artifact(&self, artifact: StoredArtifact) -> Result<(), StoreError> {
        write(&self.artifacts, "artifact")?.insert(artifact.id, artifact);
        Ok(())
    }

    fn artifact(&self, id: ArtifactId) -> Result<Option<StoredArtifact>, StoreError> {
        Ok(read(&self.artifacts).get(&id).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::{
        AgentId, CredentialSource, ExecutionPlacement, Limits, ModelProvider, WorkModel,
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

    // Owner decision 3A: a writer that panicked holding the lock leaves reads
    // working and refuses later writes as a StoreError.
    #[test]
    fn a_poisoned_lock_serves_reads_and_refuses_writes() {
        let store = InMemoryStore::default();
        let first = spec();
        let run_id = first.run_id;
        store
            .put_run(StoredRun {
                spec: first,
                events: Vec::new(),
            })
            .unwrap();
        let poisoner = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let _held = store.runs.lock().unwrap();
                    panic!("writer panicked holding the lock");
                })
                .join()
        });
        assert!(poisoner.is_err());
        assert!(store.runs.is_poisoned());

        assert_eq!(store.run(run_id).map(|run| run.is_some()), Ok(true));
        let refused = StoreError::new("run store lock poisoned");
        assert_eq!(
            store.put_run(StoredRun {
                spec: spec(),
                events: Vec::new(),
            }),
            Err(refused.clone())
        );
        assert_eq!(store.append_events(run_id, Vec::new()), Err(refused));
    }

    fn completed(spec: &RunSpec, outcome: &str) -> Event {
        Event::record(
            protocol::EventSource::new(
                spec.run_id,
                spec.agent_id,
                &spec.agent_version,
                protocol::Actor::System,
                protocol::Timestamp::now(),
            ),
            EventPayload::RunCompleted {
                outcome: outcome.to_string(),
            },
        )
    }

    // A batch may hold one terminal event: the log ends at it.
    #[test]
    fn a_batch_with_two_terminal_events_is_refused() {
        let store = InMemoryStore::default();
        let spec = spec();
        let two = vec![completed(&spec, "a"), completed(&spec, "b")];
        assert!(store
            .put_run(StoredRun {
                spec: spec.clone(),
                events: two.clone(),
            })
            .is_err());
        assert!(store.run(spec.run_id).unwrap().is_none(), "nothing stored");
        store
            .put_run(StoredRun {
                spec: spec.clone(),
                events: Vec::new(),
            })
            .unwrap();
        assert!(store.append_events(spec.run_id, two).is_err());
        assert_eq!(
            store
                .run(spec.run_id)
                .map(|run| run.map(|run| run.events.len())),
            Ok(Some(0))
        );
    }
}
