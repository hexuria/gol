use crate::step::WorkflowStep;

pub trait WorkflowDriver {
    fn evaluate(&self, ctx: &WorkflowContext, history: &History) -> WorkflowStep;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkflowContext;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct History {
    pub counter: Option<i64>,
}
