use std::collections::HashMap;
use std::sync::Mutex;

use protocol::{AgentId, ArtifactId, Capability, Event, EventPayload, RunId, RunSpec};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AgentManifest {
    pub id: AgentId,
    pub version: String,
    pub instructions: String,
    pub tools: Vec<String>,
    pub required_capabilities: Vec<Capability>,
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

pub trait RunStore: Send + Sync {
    fn put_agent(&self, agent: AgentManifest);
    /// Store a new run. A run already stored under that id keeps its spec and
    /// events, so a redelivered put cannot drop anything appended since.
    fn put_run(&self, run: StoredRun);
    /// Append `events` onto the stored run in one atomic step, unless the
    /// stored log is already terminal or the run is missing. `spec` and the
    /// events already stored never change.
    fn append_events(&self, id: RunId, events: Vec<Event>) -> Append;
    fn run(&self, id: RunId) -> Option<StoredRun>;
    fn put_artifact(&self, artifact: StoredArtifact);
    fn artifact(&self, id: ArtifactId) -> Option<StoredArtifact>;
}

#[derive(Default)]
pub struct InMemoryStore {
    agents: Mutex<HashMap<AgentId, AgentManifest>>,
    runs: Mutex<HashMap<RunId, StoredRun>>,
    artifacts: Mutex<HashMap<ArtifactId, StoredArtifact>>,
}

impl RunStore for InMemoryStore {
    fn put_agent(&self, agent: AgentManifest) {
        self.agents
            .lock()
            .expect("agent store")
            .insert(agent.id, agent);
    }

    fn put_run(&self, run: StoredRun) {
        self.runs
            .lock()
            .expect("run store")
            .entry(run.spec.run_id)
            .or_insert(run);
    }

    fn append_events(&self, id: RunId, events: Vec<Event>) -> Append {
        let mut runs = self.runs.lock().expect("run store");
        let Some(stored) = runs.get_mut(&id) else {
            return Append::Missing;
        };
        if stored
            .events
            .iter()
            .any(|event| is_terminal(&event.payload))
        {
            return Append::Terminal;
        }
        stored.events.extend(events);
        Append::Appended
    }

    fn run(&self, id: RunId) -> Option<StoredRun> {
        self.runs.lock().expect("run store").get(&id).cloned()
    }

    fn put_artifact(&self, artifact: StoredArtifact) {
        self.artifacts
            .lock()
            .expect("artifact store")
            .insert(artifact.id, artifact);
    }

    fn artifact(&self, id: ArtifactId) -> Option<StoredArtifact> {
        self.artifacts
            .lock()
            .expect("artifact store")
            .get(&id)
            .cloned()
    }
}
