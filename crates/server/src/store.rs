use std::collections::HashMap;
use std::sync::Mutex;

use protocol::{AgentId, ArtifactId, Capability, Event, RunId, RunSpec};

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

pub trait RunStore: Send + Sync {
    fn put_agent(&self, agent: AgentManifest);
    fn put_run(&self, run: StoredRun);
    /// Append `events` onto the run already stored. `spec` and the events
    /// already stored stay as they are. A missing run is left missing.
    fn append_events(&self, id: RunId, events: Vec<Event>);
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
            .insert(run.spec.run_id, run);
    }

    fn append_events(&self, id: RunId, events: Vec<Event>) {
        let mut runs = self.runs.lock().expect("run store");
        if let Some(stored) = runs.get_mut(&id) {
            stored.events.extend(events);
        }
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
