use harness::{AgentSpawner, ChildRequest, StartedChild};
use protocol::{AgentId, RunSpec};
use workflow_core::{
    spawn, AgentSpec, History, Record, WaitCondition, WorkflowCommand, WorkflowContext,
    WorkflowDriver, WorkflowStep,
};

use crate::Journal;

/// What a workflow's spawn did on this replay: the child the spawner started,
/// or why it did not start one.
pub type Spawned = Result<StartedChild, String>;

/// Evaluates the workflow over its journal and carries out a single command.
/// A spawn starts the named agent through `spawner` as a child of `parent`,
/// the workflow's own run, and commits `AgentSpawned` only once the child is
/// started; a refused spawn is not journaled, so the next replay asks again.
/// A kill between the start and the commit asks again with the same request,
/// which names the same child.
pub fn replay(
    driver: &impl WorkflowDriver,
    ctx: &WorkflowContext,
    journal: &mut Journal,
    parent: &RunSpec,
    spawner: &dyn AgentSpawner,
    stand_in: &mut dyn FnMut() -> i64,
) -> std::io::Result<(WorkflowStep, Option<Spawned>)> {
    let history = journal.history()?;
    let step = driver.evaluate(ctx, &history);
    let spawned = match step.commands.as_slice() {
        [WorkflowCommand::SpawnAgent(agent)] => {
            let started = start_child(parent, spawner, &history, agent);
            if started.is_ok() {
                journal.commit(&Record::AgentSpawned {
                    agent: agent.agent.clone(),
                })?;
            }
            Some(started)
        }
        _ => None,
    };
    if let [WorkflowCommand::ExecuteTool(tool)] = step.commands.as_slice() {
        if tool.name == "counter" {
            let value = stand_in();
            journal.commit(&Record::counter(value))?;
        }
    }
    Ok((step, spawned))
}

/// Starts `agent` as the child the spawn at the journal's end asks for. The
/// workflow names its agent by id (decision 1.3-2A), and the child gets the
/// parent's limits (1.3-3A).
fn start_child(
    parent: &RunSpec,
    spawner: &dyn AgentSpawner,
    history: &History,
    agent: &AgentSpec,
) -> Spawned {
    let agent_id: AgentId = agent
        .agent
        .parse()
        .map_err(|_| "no such agent".to_string())?;
    let step = u32::try_from(history.records.len())
        .map_err(|_| "the workflow journal is too long".to_string())?;
    spawner.start(ChildRequest {
        parent,
        step,
        agent_id,
        input: &agent.input,
        limits: parent.limits,
    })
}

pub fn join_all(
    driver: &impl WorkflowDriver,
    ctx: &WorkflowContext,
    journal: &mut Journal,
    stand_in: &mut dyn FnMut(u32) -> i64,
) -> std::io::Result<()> {
    let step = driver.evaluate(ctx, &History::default());
    if step.wait != WaitCondition::None {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "wait",
        ));
    }
    let (first_command, second_command) = match step.commands.as_slice() {
        [first @ WorkflowCommand::ExecuteTool(a), second @ WorkflowCommand::ExecuteTool(b)]
            if a.name == "counter" && b.name == "counter" =>
        {
            (first.clone(), second.clone())
        }
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "command",
            ));
        }
    };
    let first = spawn(0, first_command);
    let second = spawn(1, second_command);
    match journal.history()?.records.len() {
        0 => {
            let value = stand_in(first.sequence);
            journal.commit(&Record::counter(value))?;
            let value = stand_in(second.sequence);
            journal.commit(&Record::counter(value))?;
        }
        1 => {
            let value = stand_in(second.sequence);
            journal.commit(&Record::counter(value))?;
        }
        2 => {}
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "journal length",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{join_all, replay};
    use crate::{frame, start_frame, Journal};
    use std::cell::{Cell, RefCell};
    use std::fs::{self, OpenOptions};
    use std::io::Read;
    use std::path::Path;
    use workflow_core::{
        AgentSpec, CounterBranch, History, JoinBranch, Record, ToolSpec, WaitCondition,
        WorkflowCommand, WorkflowContext, WorkflowDriver, WorkflowStep,
    };

    fn read_path(path: &Path) -> Vec<u8> {
        let mut file = OpenOptions::new().read(true).open(path).unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        buf
    }

    /// The bytes of a journal holding `records`.
    /// The workflow's own run.
    fn parent() -> protocol::RunSpec {
        protocol::RunSpec::builder()
            .owner(protocol::Owner::new("local", "runtime-tokio", "local"))
            .agent(protocol::AgentId::new(), "1")
            .input("hello")
            .placement(protocol::ExecutionPlacement::Local)
            .work_model(protocol::WorkModel {
                provider: protocol::ModelProvider::OpenAI,
                model_name: "gpt-test".to_string(),
                credential: protocol::CredentialSource::PlatformGateway,
            })
            .build()
    }

    /// A spawner for workflows that spawn nothing.
    struct NoSpawner;

    impl harness::AgentSpawner for NoSpawner {
        fn start(
            &self,
            _request: harness::ChildRequest<'_>,
        ) -> Result<harness::StartedChild, String> {
            panic!("this workflow spawns nothing")
        }
    }

    fn journal_bytes(records: &[Record]) -> Vec<u8> {
        let mut bytes = start_frame();
        for record in records {
            bytes.extend(frame(record).unwrap());
        }
        bytes
    }

    #[test]
    fn second_pass_makes_zero_repeat_calls() {
        let dir = std::env::temp_dir().join(format!("gol-host-{}-replay", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("log");
        let mut journal = Journal::open(&path).unwrap();
        assert_eq!(read_path(&path), start_frame());

        let calls = Cell::new(0u32);
        let mut stand_in = || {
            calls.set(calls.get() + 1);
            0i64
        };
        let driver = CounterBranch;
        let ctx = WorkflowContext;

        let (first, first_harness) = replay(
            &driver,
            &ctx,
            &mut journal,
            &parent(),
            &NoSpawner,
            &mut stand_in,
        )
        .unwrap();
        assert!(first_harness.is_none());
        assert_eq!(calls.get(), 1);
        assert_eq!(
            first.commands,
            [WorkflowCommand::ExecuteTool(ToolSpec::new("counter", ""))]
        );
        assert_eq!(first.wait, WaitCondition::None);
        assert_eq!(read_path(&path), journal_bytes(&[Record::counter(0)]));

        let (second, second_harness) = replay(
            &driver,
            &ctx,
            &mut journal,
            &parent(),
            &NoSpawner,
            &mut stand_in,
        )
        .unwrap();
        assert!(second_harness.is_none());
        assert_eq!(calls.get(), 1);
        assert_eq!(second.commands, [WorkflowCommand::Complete]);
        assert_eq!(second.wait, WaitCondition::None);
        assert_eq!(read_path(&path), journal_bytes(&[Record::counter(0)]));
    }

    #[test]
    fn appends_one_record_while_the_other_handle_is_outstanding() {
        let dir = std::env::temp_dir().join(format!("gol-host-{}-join", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("log");
        let mut journal = Journal::open(&path).unwrap();
        assert_eq!(read_path(&path), start_frame());

        let calls = RefCell::new(Vec::new());
        let mut stand_in = |sequence: u32| {
            let bytes = read_path(&path);
            match sequence {
                0 => {
                    assert!(calls.borrow().is_empty());
                    calls.borrow_mut().push(sequence);
                    7i64
                }
                1 => {
                    assert_eq!(bytes, journal_bytes(&[Record::counter(7)]));
                    assert_eq!(calls.borrow().as_slice(), &[0]);
                    calls.borrow_mut().push(sequence);
                    9i64
                }
                _ => panic!("unexpected sequence"),
            }
        };
        let driver = JoinBranch;
        let ctx = WorkflowContext;

        join_all(&driver, &ctx, &mut journal, &mut stand_in).unwrap();
        assert_eq!(calls.borrow().as_slice(), &[0, 1]);
        let expected = journal_bytes(&[Record::counter(7), Record::counter(9)]);
        assert_eq!(read_path(&path), expected);

        join_all(&driver, &ctx, &mut journal, &mut stand_in).unwrap();
        assert_eq!(calls.borrow().as_slice(), &[0, 1]);
        assert_eq!(read_path(&path), expected);

        let partial = dir.join("partial");
        fs::write(&partial, journal_bytes(&[Record::counter(7)])).unwrap();
        let mut partial_journal = Journal::open(&partial).unwrap();
        let partial_calls = RefCell::new(Vec::new());
        let mut partial_stand_in = |sequence: u32| {
            partial_calls.borrow_mut().push(sequence);
            match sequence {
                0 => 7i64,
                1 => 9i64,
                _ => panic!("unexpected sequence"),
            }
        };
        join_all(&driver, &ctx, &mut partial_journal, &mut partial_stand_in).unwrap();
        assert_eq!(partial_calls.borrow().as_slice(), &[1]);
        assert_eq!(read_path(&partial), expected);
    }

    #[test]
    fn only_a_spawn_starts_a_child() {
        let spec = parent();
        let delegate = protocol::Effect::Delegate {
            agent_id: protocol::AgentId::new(),
            input: "child".to_string(),
        };
        assert_eq!(
            protocol::authorize(&spec, &delegate, &[]),
            protocol::PolicyDecision::Deny {
                reason: "missing capability: agent.delegate".to_string(),
            }
        );
        // `perform` does not authorize (the decide step does). Performing a
        // delegation directly, with no spawner configured, records it as
        // refused and starts nothing.
        let mut denied = harness::Driver::boot(spec.clone()).unwrap();
        let events_before = denied.events().len();
        let harness_before = denied.state().harness.clone();
        denied.perform(
            &[delegate],
            &[],
            &harness::UnavailableModel,
            &harness::InMemory::default(),
        );
        assert_eq!(denied.events().len(), events_before + 1);
        assert!(matches!(
            denied.events().last().map(|event| &event.payload),
            Some(protocol::EventPayload::DelegateRefused { reason, .. })
                if reason == "no agent spawner is configured"
        ));
        assert_eq!(denied.state().harness, harness_before);
        assert_eq!(denied.state().children, 0);
        assert!(denied
            .events()
            .iter()
            .all(|event| event.envelope.parent_run_id.is_none()));

        struct Fixed {
            command: WorkflowCommand,
        }

        impl WorkflowDriver for Fixed {
            fn evaluate(&self, _ctx: &WorkflowContext, _history: &History) -> WorkflowStep {
                WorkflowStep {
                    commands: vec![self.command.clone()],
                    wait: WaitCondition::None,
                }
            }
        }

        let dir = std::env::temp_dir().join(format!("gol-host-{}-spawn", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        for command in [
            WorkflowCommand::ExecuteTool(ToolSpec::new("counter", "")),
            WorkflowCommand::ExecuteTool(ToolSpec::new("other", "")),
            WorkflowCommand::Complete,
            WorkflowCommand::Fail,
        ] {
            let case = dir.join(format!("{command:?}"));
            let mut case_journal = Journal::open(&case).unwrap();
            let case_calls = Cell::new(0u32);
            let mut case_stand_in = || {
                case_calls.set(case_calls.get() + 1);
                0i64
            };
            let (step, harness) = replay(
                &Fixed {
                    command: command.clone(),
                },
                &WorkflowContext,
                &mut case_journal,
                &spec,
                &NoSpawner,
                &mut case_stand_in,
            )
            .unwrap();
            assert_eq!(step.commands, std::slice::from_ref(&command));
            assert!(harness.is_none());
            match command {
                WorkflowCommand::ExecuteTool(tool) if tool.name == "counter" => {
                    assert_eq!(case_calls.get(), 1);
                    assert_eq!(read_path(&case), journal_bytes(&[Record::counter(0)]));
                }
                WorkflowCommand::ExecuteTool(_)
                | WorkflowCommand::Complete
                | WorkflowCommand::Fail
                | WorkflowCommand::SpawnAgent(_) => {
                    assert_eq!(case_calls.get(), 0);
                    assert_eq!(read_path(&case), start_frame());
                }
            }
        }

        let failed = dir.join("failed");
        fs::write(&failed, journal_bytes(&[Record::counter(1)])).unwrap();
        let mut failed_journal = Journal::open(&failed).unwrap();
        let fail_calls = Cell::new(0u32);
        let mut fail_stand_in = || {
            fail_calls.set(fail_calls.get() + 1);
            0i64
        };
        let (step, harness) = replay(
            &CounterBranch,
            &WorkflowContext,
            &mut failed_journal,
            &parent(),
            &NoSpawner,
            &mut fail_stand_in,
        )
        .unwrap();
        assert_eq!(step.commands, [WorkflowCommand::Fail]);
        assert!(harness.is_none());
        assert_eq!(fail_calls.get(), 0);
        assert_eq!(read_path(&failed), journal_bytes(&[Record::counter(1)]));

        struct TwoSpawn;

        impl WorkflowDriver for TwoSpawn {
            fn evaluate(&self, _ctx: &WorkflowContext, _history: &History) -> WorkflowStep {
                WorkflowStep {
                    commands: vec![
                        WorkflowCommand::SpawnAgent(AgentSpec::new("child", "")),
                        WorkflowCommand::SpawnAgent(AgentSpec::new("child", "")),
                    ],
                    wait: WaitCondition::None,
                }
            }
        }

        let joined = dir.join("join");
        let mut joined_journal = Journal::open(&joined).unwrap();
        let join_calls = RefCell::new(Vec::new());
        let mut join_stand_in = |sequence: u32| {
            join_calls.borrow_mut().push(sequence);
            0i64
        };
        let error = join_all(
            &TwoSpawn,
            &WorkflowContext,
            &mut joined_journal,
            &mut join_stand_in,
        )
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "command");
        assert!(join_calls.borrow().is_empty());
        assert_eq!(read_path(&joined), start_frame());
    }
}
