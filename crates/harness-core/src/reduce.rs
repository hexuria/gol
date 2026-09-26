use workflow_core::{
    History, WaitCondition, WorkflowCommand, WorkflowContext, WorkflowDriver, WorkflowStep,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkflowRun {
    Open,
    Completed,
    Failed,
    /// The driver returned no command or more than one. A step names exactly
    /// one command, so the run cannot go on.
    Invalid,
}

pub fn transition(
    driver: &impl WorkflowDriver,
    ctx: &WorkflowContext,
    history: &History,
) -> (WorkflowRun, Vec<WorkflowCommand>) {
    let WorkflowStep {
        commands,
        wait: WaitCondition::None,
    } = driver.evaluate(ctx, history);
    let next = match commands.as_slice() {
        [WorkflowCommand::ExecuteTool(_) | WorkflowCommand::SpawnAgent] => WorkflowRun::Open,
        [WorkflowCommand::Complete] => WorkflowRun::Completed,
        [WorkflowCommand::Fail] => WorkflowRun::Failed,
        [] | [_, _, ..] => WorkflowRun::Invalid,
    };
    (next, commands)
}

#[cfg(test)]
mod tests {
    use super::{transition, WorkflowRun};
    use workflow_core::{CounterBranch, History, ToolSpec, WorkflowCommand, WorkflowContext};

    #[test]
    fn emits_effects_without_calling_a_tool() {
        let driver = CounterBranch;
        let ctx = WorkflowContext;

        let unrecorded = History { counter: None };
        let first = transition(&driver, &ctx, &unrecorded);
        let second = transition(&driver, &ctx, &unrecorded);
        assert_eq!(first, second);
        assert_eq!(first.0, WorkflowRun::Open);
        assert_eq!(
            first.1,
            [WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" })]
        );
        assert_eq!(unrecorded.counter, None);

        let zero = History { counter: Some(0) };
        let zero_first = transition(&driver, &ctx, &zero);
        let zero_second = transition(&driver, &ctx, &zero);
        assert_eq!(zero_first, zero_second);
        assert_eq!(zero_first.0, WorkflowRun::Completed);
        assert_eq!(zero_first.1, [WorkflowCommand::Complete]);
        assert_eq!(zero.counter, Some(0));

        let one = History { counter: Some(1) };
        let one_first = transition(&driver, &ctx, &one);
        let one_second = transition(&driver, &ctx, &one);
        assert_eq!(one_first, one_second);
        assert_eq!(one_first.0, WorkflowRun::Failed);
        assert_eq!(one_first.1, [WorkflowCommand::Fail]);
        assert_eq!(one.counter, Some(1));

        let two = History { counter: Some(2) };
        let two_result = transition(&driver, &ctx, &two);
        assert_eq!(two_result, one_first);
        assert_eq!(two.counter, Some(2));
    }
}
