use protocol::{AgentId, Limits, RunId, RunSpec};

/// A delegation the driver asks a spawner to carry out: start `agent_id`
/// with `input` as a child of `parent`, asked for by the parent's `step`,
/// with `limits` carved from the parent's budget.
#[derive(Clone, Copy, Debug)]
pub struct ChildRequest<'a> {
    pub parent: &'a RunSpec,
    pub step: u32,
    pub agent_id: AgentId,
    pub input: &'a str,
    pub limits: Limits,
}

/// A child a spawner started, or had already started for the same request:
/// its id, and the limits it was stored with. Those can differ from the
/// request's when the child already existed, and they are what the parent
/// has given.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StartedChild {
    pub run_id: RunId,
    pub limits: Limits,
}

/// Starts child runs. The driver records the child as `ChildStarted`, or the
/// error as the reason of `DelegateRefused`.
pub trait AgentSpawner: Send + Sync {
    fn start(&self, request: ChildRequest<'_>) -> Result<StartedChild, String>;
}
