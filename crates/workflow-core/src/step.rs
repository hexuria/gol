#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowStep {
    pub commands: Vec<WorkflowCommand>,
    pub wait: WaitCondition,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkflowCommand {
    ExecuteTool(ToolSpec),
    Complete,
    Fail,
    SpawnAgent(AgentSpec),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitCondition {
    None,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolSpec {
    pub name: String,
    pub input: String,
}

impl ToolSpec {
    pub fn new(name: &str, input: &str) -> Self {
        Self {
            name: name.to_string(),
            input: input.to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentSpec {
    pub agent: String,
    pub input: String,
}

impl AgentSpec {
    pub fn new(agent: &str, input: &str) -> Self {
        Self {
            agent: agent.to_string(),
            input: input.to_string(),
        }
    }
}
