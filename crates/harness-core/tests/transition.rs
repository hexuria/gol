//! `transition` names the run's next state for every command list a driver can
//! return. A list that is not exactly one command is invalid, never `Open`.

use harness_core::{transition, WorkflowRun};
use workflow_core::{
    History, ToolSpec, WaitCondition, WorkflowCommand, WorkflowContext, WorkflowDriver,
    WorkflowStep,
};

struct Returns(Vec<WorkflowCommand>);

impl WorkflowDriver for Returns {
    fn evaluate(&self, _ctx: &WorkflowContext, _history: &History) -> WorkflowStep {
        WorkflowStep {
            commands: self.0.clone(),
            wait: WaitCondition::None,
        }
    }
}

fn next(commands: Vec<WorkflowCommand>) -> WorkflowRun {
    let driver = Returns(commands.clone());
    let (run, emitted) = transition(&driver, &WorkflowContext, &History::default());
    assert_eq!(emitted, commands);
    run
}

const COUNTER: WorkflowCommand = WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" });

#[test]
fn empty_command_list_is_invalid() {
    assert_eq!(next(vec![]), WorkflowRun::Invalid);
}

#[test]
fn two_commands_are_invalid() {
    assert_eq!(next(vec![COUNTER, COUNTER]), WorkflowRun::Invalid);
    assert_eq!(
        next(vec![WorkflowCommand::Complete, WorkflowCommand::Fail]),
        WorkflowRun::Invalid
    );
    assert_eq!(
        next(vec![
            WorkflowCommand::SpawnAgent,
            WorkflowCommand::SpawnAgent
        ]),
        WorkflowRun::Invalid
    );
}

#[test]
fn each_single_command_has_its_own_next_state() {
    assert_eq!(next(vec![COUNTER]), WorkflowRun::Open);
    assert_eq!(next(vec![WorkflowCommand::SpawnAgent]), WorkflowRun::Open);
    assert_eq!(
        next(vec![WorkflowCommand::Complete]),
        WorkflowRun::Completed
    );
    assert_eq!(next(vec![WorkflowCommand::Fail]), WorkflowRun::Failed);
}
