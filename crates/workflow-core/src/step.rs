#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowStep {
    pub commands: Vec<WorkflowCommand>,
    pub wait: WaitCondition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkflowCommand {
    ExecuteTool(ToolSpec),
    Complete,
    Fail,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitCondition {
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ToolSpec {
    pub name: &'static str,
}
