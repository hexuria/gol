use crate::driver::{History, Record, WorkflowContext, WorkflowDriver};
use crate::step::{AgentSpec, ToolSpec, WaitCondition, WorkflowCommand, WorkflowStep};

#[path = "id.rs"]
pub mod id;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowProgram {
    pub root: Decision,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    Tool {
        name: String,
        input: String,
    },
    SpawnAgent {
        agent: String,
        input: String,
    },
    /// Its decisions in order: each tool call and spawn waits for the one
    /// before it to be recorded.
    Seq(Vec<Decision>),
    Complete,
    Fail,
    OnCounter {
        missing: Box<Decision>,
        zero: Box<Decision>,
        other: Box<Decision>,
    },
}

pub fn counter_program() -> WorkflowProgram {
    WorkflowProgram {
        root: Decision::OnCounter {
            missing: Box::new(Decision::Tool {
                name: "counter".to_string(),
                input: String::new(),
            }),
            zero: Box::new(Decision::Complete),
            other: Box::new(Decision::Fail),
        },
    }
}

/// The next command for this history. The program is walked with a cursor
/// into the records: a tool call or spawn whose record is at the cursor is
/// done and moves the cursor on, and the first one without a record is the
/// command. A record that does not match its decision, or a program that runs
/// out without completing, is `Fail`.
pub fn evaluate_program(program: &WorkflowProgram, history: &History) -> WorkflowStep {
    let command = match walk(&program.root, history, 0) {
        Walk::Next(command) => command,
        Walk::Done(_) => WorkflowCommand::Fail,
    };
    WorkflowStep {
        commands: vec![command],
        wait: WaitCondition::None,
    }
}

enum Walk {
    /// The command this decision issues now.
    Next(WorkflowCommand),
    /// Every record this decision needs is present; the cursor after them.
    Done(usize),
}

fn walk(decision: &Decision, history: &History, cursor: usize) -> Walk {
    match decision {
        Decision::Tool { name, input } => match history.records.get(cursor) {
            None => Walk::Next(WorkflowCommand::ExecuteTool(ToolSpec {
                name: name.clone(),
                input: input.clone(),
            })),
            Some(Record::Tool { name: recorded, .. }) if recorded == name => Walk::Done(cursor + 1),
            Some(_) => Walk::Next(WorkflowCommand::Fail),
        },
        Decision::SpawnAgent { agent, input } => match history.records.get(cursor) {
            None => Walk::Next(WorkflowCommand::SpawnAgent(AgentSpec {
                agent: agent.clone(),
                input: input.clone(),
            })),
            Some(Record::AgentSpawned { agent: recorded }) if recorded == agent => {
                Walk::Done(cursor + 1)
            }
            Some(_) => Walk::Next(WorkflowCommand::Fail),
        },
        Decision::Seq(decisions) => {
            let mut cursor = cursor;
            for decision in decisions {
                match walk(decision, history, cursor) {
                    Walk::Done(next) => cursor = next,
                    next @ Walk::Next(_) => return next,
                }
            }
            Walk::Done(cursor)
        }
        Decision::Complete => Walk::Next(WorkflowCommand::Complete),
        Decision::Fail => Walk::Next(WorkflowCommand::Fail),
        Decision::OnCounter {
            missing,
            zero,
            other,
        } => {
            let arm = match id::path(history) {
                id::Path::Unrecorded => missing,
                id::Path::Zero => zero,
                id::Path::Nonzero => other,
            };
            walk(arm, history, cursor)
        }
    }
}

pub struct CounterBranch;

impl WorkflowDriver for CounterBranch {
    fn evaluate(&self, _ctx: &WorkflowContext, history: &History) -> WorkflowStep {
        evaluate_program(&counter_program(), history)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
                WorkflowCommand::ExecuteTool(ToolSpec::new("counter", "")),
                WorkflowCommand::ExecuteTool(ToolSpec::new("counter", "")),
            ],
            wait: WaitCondition::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CounterBranch;
    use crate::driver::{History, Record, WorkflowContext, WorkflowDriver};
    use crate::step::{ToolSpec, WaitCondition, WorkflowCommand};

    #[test]
    fn branch_on_recorded_counter() {
        let ctx = WorkflowContext;
        let branch = CounterBranch;

        let recorded_zero = History::new(vec![Record::counter(0)]);
        let zero_first = branch.evaluate(&ctx, &recorded_zero);
        let zero_second = branch.evaluate(&ctx, &recorded_zero);
        assert_eq!(zero_first.commands, [WorkflowCommand::Complete]);
        assert_eq!(zero_first.wait, WaitCondition::None);
        assert_eq!(zero_first, zero_second);

        let recorded_one = History::new(vec![Record::counter(1)]);
        let one_first = branch.evaluate(&ctx, &recorded_one);
        let one_second = branch.evaluate(&ctx, &recorded_one);
        assert_eq!(one_first.commands, [WorkflowCommand::Fail]);
        assert_eq!(one_first.wait, WaitCondition::None);
        assert_eq!(one_first, one_second);

        assert_ne!(zero_first, one_first);

        let unrecorded = History::default();
        let empty = branch.evaluate(&ctx, &unrecorded);
        assert_eq!(
            empty.commands,
            [WorkflowCommand::ExecuteTool(ToolSpec::new("counter", ""))]
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

        let recorded_two = History::new(vec![Record::counter(2)]);
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
