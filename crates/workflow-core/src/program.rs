use crate::driver::{History, WorkflowContext, WorkflowDriver};
use crate::step::{ToolSpec, WaitCondition, WorkflowCommand, WorkflowStep};

pub struct CounterBranch;

impl WorkflowDriver for CounterBranch {
    fn evaluate(&self, _ctx: &WorkflowContext, history: &History) -> WorkflowStep {
        let command = match history.counter {
            None => WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" }),
            Some(0) => WorkflowCommand::Complete,
            Some(_) => WorkflowCommand::Fail,
        };
        WorkflowStep {
            commands: vec![command],
            wait: WaitCondition::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CounterBranch;
    use crate::driver::{History, WorkflowContext, WorkflowDriver};
    use crate::step::{ToolSpec, WaitCondition, WorkflowCommand};

    #[test]
    fn branch_on_recorded_counter() {
        let ctx = WorkflowContext;
        let branch = CounterBranch;

        let recorded_zero = History { counter: Some(0) };
        let zero_first = branch.evaluate(&ctx, &recorded_zero);
        let zero_second = branch.evaluate(&ctx, &recorded_zero);
        assert_eq!(zero_first.commands, [WorkflowCommand::Complete]);
        assert_eq!(zero_first.wait, WaitCondition::None);
        assert_eq!(zero_first, zero_second);

        let recorded_one = History { counter: Some(1) };
        let one_first = branch.evaluate(&ctx, &recorded_one);
        let one_second = branch.evaluate(&ctx, &recorded_one);
        assert_eq!(one_first.commands, [WorkflowCommand::Fail]);
        assert_eq!(one_first.wait, WaitCondition::None);
        assert_eq!(one_first, one_second);

        assert_ne!(zero_first, one_first);

        let unrecorded = History { counter: None };
        let empty = branch.evaluate(&ctx, &unrecorded);
        assert_eq!(
            empty.commands,
            [WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" })]
        );
        assert_eq!(empty.wait, WaitCondition::None);
    }
}
