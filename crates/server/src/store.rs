use std::collections::HashMap;
use std::sync::Mutex;

use protocol::{AgentId, Capability, Event, RunId, RunSpec};

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

pub trait RunStore: Send + Sync {
    fn put_agent(&self, agent: AgentManifest);
    fn put_run(&self, run: StoredRun);
    fn run(&self, id: RunId) -> Option<StoredRun>;
}

#[derive(Default)]
pub struct InMemoryStore {
    agents: Mutex<HashMap<AgentId, AgentManifest>>,
    runs: Mutex<HashMap<RunId, StoredRun>>,
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

    fn run(&self, id: RunId) -> Option<StoredRun> {
        self.runs.lock().expect("run store").get(&id).cloned()
    }
}
