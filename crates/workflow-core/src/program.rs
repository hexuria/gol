use crate::driver::{History, WorkflowContext, WorkflowDriver};
use crate::step::{ToolSpec, WaitCondition, WorkflowCommand, WorkflowStep};

#[path = "id.rs"]
pub mod id;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowProgram {
    pub root: Decision,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    Tool(ToolName),
    Complete,
    Fail,
    OnCounter {
        missing: Box<Decision>,
        zero: Box<Decision>,
        other: Box<Decision>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolName {
    Counter,
}

pub fn counter_program() -> WorkflowProgram {
    WorkflowProgram {
        root: Decision::OnCounter {
            missing: Box::new(Decision::Tool(ToolName::Counter)),
            zero: Box::new(Decision::Complete),
            other: Box::new(Decision::Fail),
        },
    }
}

pub fn evaluate_program(program: &WorkflowProgram, history: &History) -> WorkflowStep {
    WorkflowStep {
        commands: vec![command(&program.root, history)],
        wait: WaitCondition::None,
    }
}

fn command(decision: &Decision, history: &History) -> WorkflowCommand {
    match decision {
        Decision::Tool(ToolName::Counter) => {
            WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" })
        }
        Decision::Complete => WorkflowCommand::Complete,
        Decision::Fail => WorkflowCommand::Fail,
        Decision::OnCounter {
            missing,
            zero,
            other,
        } => {
            let arm = match history.counter {
                None => missing,
                Some(0) => zero,
                Some(_) => other,
            };
            command(arm, history)
        }
    }
}

pub struct CounterBranch;

impl WorkflowDriver for CounterBranch {
    fn evaluate(&self, _ctx: &WorkflowContext, history: &History) -> WorkflowStep {
        evaluate_program(&counter_program(), history)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Handle {
    pub sequence: u32,
    pub command: WorkflowCommand,
}

pub fn spawn(sequence: u32, command: WorkflowCommand) -> Handle {
    Handle { sequence, command }
}

pub struct JoinBranch;

impl WorkflowDriver for JoinBranch {
    fn evaluate(&self, _ctx: &WorkflowContext, _history: &History) -> WorkflowStep {
        WorkflowStep {
            commands: vec![
                WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" }),
                WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" }),
            ],
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
