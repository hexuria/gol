use crate::driver::{History, WorkflowContext, WorkflowDriver};
use crate::step::{ToolSpec, WaitCondition, WorkflowCommand, WorkflowStep};

#[path = "id.rs"]
pub mod id;

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

        let run = super::id::WorkflowRunId("counter-branch");
        let zero_id = super::id::effect_id(run, &recorded_zero, 0);
        let zero_id_again = super::id::effect_id(run, &recorded_zero, 0);
        assert_eq!(zero_id, zero_id_again);
        assert_eq!(zero_id.path, super::id::Path::Zero);
        assert_eq!(zero_id.workflow, super::id::WorkflowRunId("counter-branch"));
        assert_eq!(zero_id.sequence, 0);

        let one_id = super::id::effect_id(run, &recorded_one, 0);
        let one_id_again = super::id::effect_id(run, &recorded_one, 0);
        assert_eq!(one_id, one_id_again);
        assert_eq!(one_id.path, super::id::Path::Nonzero);
        assert_ne!(one_id, zero_id);

        let recorded_two = History { counter: Some(2) };
        let two_id = super::id::effect_id(run, &recorded_two, 0);
        assert_eq!(two_id, one_id);

        let unrecorded_id = super::id::effect_id(run, &unrecorded, 0);
        assert_eq!(unrecorded_id.path, super::id::Path::Unrecorded);
        assert_ne!(unrecorded_id, zero_id);
        assert_ne!(unrecorded_id, one_id);

        let zero_next = super::id::effect_id(run, &recorded_zero, 1);
        assert_ne!(zero_id, zero_next);

        let other_run = super::id::WorkflowRunId("other-run");
        let other_id = super::id::effect_id(other_run, &recorded_zero, 0);
        assert_ne!(zero_id, other_id);
    }
}
