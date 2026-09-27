//! `transition` names the run's next state for every command list a driver can
//! return. Several tools together are a join and stay `Open`; no command, or
//! several commands that are not all tools, is invalid, never `Open`.

use harness_core::{transition, WorkflowRun};
use workflow_core::{
    AgentSpec, History, JoinBranch, ToolSpec, WaitCondition, WorkflowCommand, WorkflowContext,
    WorkflowDriver, WorkflowStep,
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

fn counter() -> WorkflowCommand {
    WorkflowCommand::ExecuteTool(ToolSpec::new("counter", ""))
}

fn spawn_agent() -> WorkflowCommand {
    WorkflowCommand::SpawnAgent(AgentSpec::new("child", ""))
}

#[test]
fn empty_command_list_is_invalid() {
    assert_eq!(next(vec![]), WorkflowRun::Invalid);
}

#[test]
fn two_commands_are_invalid_unless_both_are_tools() {
    assert_eq!(
        next(vec![WorkflowCommand::Complete, WorkflowCommand::Fail]),
        WorkflowRun::Invalid
    );
    assert_eq!(
        next(vec![counter(), WorkflowCommand::Complete]),
        WorkflowRun::Invalid
    );
    assert_eq!(
        next(vec![spawn_agent(), spawn_agent()]),
        WorkflowRun::Invalid
    );
}

// A join runs several tools at once (`runtime_tokio::host::join_all`), so the
// run is still open.
#[test]
fn a_join_of_tools_is_open() {
    assert_eq!(next(vec![counter(), counter()]), WorkflowRun::Open);
    assert_eq!(
        next(vec![counter(), counter(), counter()]),
        WorkflowRun::Open
    );
    let (run, commands) = transition(&JoinBranch, &WorkflowContext, &History::default());
    assert_eq!(run, WorkflowRun::Open);
    assert_eq!(commands, [counter(), counter()]);
}

#[test]
fn each_single_command_has_its_own_next_state() {
    assert_eq!(next(vec![counter()]), WorkflowRun::Open);
    assert_eq!(next(vec![spawn_agent()]), WorkflowRun::Open);
    assert_eq!(
        next(vec![WorkflowCommand::Complete]),
        WorkflowRun::Completed
    );
    assert_eq!(next(vec![WorkflowCommand::Fail]), WorkflowRun::Failed);
}
