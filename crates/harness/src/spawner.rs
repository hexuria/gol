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

/// Starts child runs. The driver records the child's id as `ChildStarted`,
/// or the error as the reason of `DelegateRefused`.
pub trait AgentSpawner: Send + Sync {
    fn start(&self, request: ChildRequest<'_>) -> Result<RunId, String>;
}
