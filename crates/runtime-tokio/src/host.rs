use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;

use workflow_core::{
    spawn, History, ToolSpec, WaitCondition, WorkflowCommand, WorkflowContext, WorkflowDriver,
    WorkflowStep,
};

use crate::Journal;

pub fn replay(
    driver: &impl WorkflowDriver,
    ctx: &WorkflowContext,
    path: &Path,
    journal: &mut Journal,
    stand_in: &mut dyn FnMut() -> i64,
) -> std::io::Result<(WorkflowStep, Option<protocol::RunState>)> {
    let recorded = read_path(path)?;
    let history = history_from_committed(&recorded)?;
    let step = driver.evaluate(ctx, &history);
    let harness = match step.commands.as_slice() {
        [command] => on_command(*command),
        _ => None,
    };
    if let [WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" })] = step.commands.as_slice() {
        let value = stand_in();
        journal.commit(&value.to_le_bytes())?;
    }
    Ok((step, harness))
}

fn read_path(path: &Path) -> std::io::Result<Vec<u8>> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    Ok(buf)
}

fn history_from_committed(bytes: &[u8]) -> std::io::Result<History> {
    match bytes.len() {
        0 => Ok(History { counter: None }),
        8 => {
            let array: [u8; 8] = bytes.try_into().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "journal length")
            })?;
            Ok(History {
                counter: Some(i64::from_le_bytes(array)),
            })
        }
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "journal length",
        )),
    }
}

fn join_all(
    driver: &impl WorkflowDriver,
    ctx: &WorkflowContext,
    path: &Path,
    journal: &mut Journal,
    stand_in: &mut dyn FnMut(u32) -> i64,
) -> std::io::Result<()> {
    let step = driver.evaluate(ctx, &History { counter: None });
    if step.wait != WaitCondition::None {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "wait",
        ));
    }
    let (first_command, second_command) = match step.commands.as_slice() {
        [first @ WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" }), second @ WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" })] => {
            (*first, *second)
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
    let length = std::fs::metadata(path)?.len();
    match length {
        0 => {
            let value = stand_in(first.sequence);
            journal.commit(&value.to_le_bytes())?;
            let value = stand_in(second.sequence);
            journal.commit(&value.to_le_bytes())?;
        }
        8 => {
            let value = stand_in(second.sequence);
            journal.commit(&value.to_le_bytes())?;
        }
        16 => {}
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "journal length",
            ));
        }
    }
    Ok(())
}

fn on_command(command: WorkflowCommand) -> Option<protocol::RunState> {
    match command {
        WorkflowCommand::SpawnAgent => {
            let mut driver = harness::Driver::boot(
                protocol::RunSpec::builder()
                    .agent(protocol::AgentId::new(), "1")
                    .input("hello")
                    .placement(protocol::ExecutionPlacement::Local)
                    .work_model(protocol::WorkModel {
                        provider: protocol::ModelProvider::OpenAI,
                        model_name: "gpt-test".to_string(),
                        credential: protocol::CredentialSource::PlatformGateway,
                    })
                    .build(),
            )
            .unwrap();
            let mut decider = harness::ScriptedDecider::new([protocol::Effect::Complete {
                outcome: "done".to_string(),
            }]);
            harness::run_to_completion(
                &mut driver,
                &mut decider,
                &[],
                &harness::UnavailableModel,
                &mut harness::InMemory::default(),
            )
            .unwrap();
            Some(driver.state())
        }
        WorkflowCommand::ExecuteTool(_) | WorkflowCommand::Complete | WorkflowCommand::Fail => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{join_all, replay};
    use crate::Journal;
    use std::cell::{Cell, RefCell};
    use std::fs::{self, OpenOptions};
    use std::io::Read;
    use std::path::Path;
    use workflow_core::{
        CounterBranch, History, JoinBranch, ToolSpec, WaitCondition, WorkflowCommand,
        WorkflowContext, WorkflowDriver, WorkflowStep,
    };

    fn read_path(path: &Path) -> Vec<u8> {
        let mut file = OpenOptions::new().read(true).open(path).unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        buf
    }

    #[test]
    fn second_pass_makes_zero_repeat_calls() {
        let dir = std::env::temp_dir().join(format!("gol-host-{}-replay", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("log");
        let mut journal = Journal::open(&path).unwrap();
        assert!(read_path(&path).is_empty());

        let calls = Cell::new(0u32);
        let mut stand_in = || {
            calls.set(calls.get() + 1);
            0i64
        };
        let driver = CounterBranch;
        let ctx = WorkflowContext;

        let (first, first_harness) =
            replay(&driver, &ctx, &path, &mut journal, &mut stand_in).unwrap();
        assert!(first_harness.is_none());
        assert_eq!(calls.get(), 1);
        assert_eq!(
            first.commands,
            [WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" })]
        );
        assert_eq!(first.wait, WaitCondition::None);
        assert_eq!(read_path(&path), 0i64.to_le_bytes());

        let (second, second_harness) =
            replay(&driver, &ctx, &path, &mut journal, &mut stand_in).unwrap();
        assert!(second_harness.is_none());
        assert_eq!(calls.get(), 1);
        assert_eq!(second.commands, [WorkflowCommand::Complete]);
        assert_eq!(second.wait, WaitCondition::None);
        assert_eq!(read_path(&path), 0i64.to_le_bytes());
    }

    #[test]
    fn appends_one_record_while_the_other_handle_is_outstanding() {
        let dir = std::env::temp_dir().join(format!("gol-host-{}-join", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("log");
        let mut journal = Journal::open(&path).unwrap();
        assert!(read_path(&path).is_empty());

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
                    assert_eq!(bytes.len(), 8);
                    assert_eq!(bytes, 7i64.to_le_bytes());
                    assert_eq!(calls.borrow().as_slice(), &[0]);
                    calls.borrow_mut().push(sequence);
                    9i64
                }
                _ => panic!("unexpected sequence"),
            }
        };
        let driver = JoinBranch;
        let ctx = WorkflowContext;

        join_all(&driver, &ctx, &path, &mut journal, &mut stand_in).unwrap();
        assert_eq!(calls.borrow().as_slice(), &[0, 1]);
        let mut expected = 7i64.to_le_bytes().to_vec();
        expected.extend_from_slice(&9i64.to_le_bytes());
        assert_eq!(read_path(&path), expected);
        assert_eq!(read_path(&path).len(), 16);

        join_all(&driver, &ctx, &path, &mut journal, &mut stand_in).unwrap();
        assert_eq!(calls.borrow().as_slice(), &[0, 1]);
        assert_eq!(read_path(&path).len(), 16);
        assert_eq!(read_path(&path), expected);

        let partial = dir.join("partial");
        fs::write(&partial, 7i64.to_le_bytes()).unwrap();
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
        join_all(
            &driver,
            &ctx,
            &partial,
            &mut partial_journal,
            &mut partial_stand_in,
        )
        .unwrap();
        assert_eq!(partial_calls.borrow().as_slice(), &[1]);
        assert_eq!(read_path(&partial), expected);
        assert_eq!(read_path(&partial).len(), 16);
    }

    #[test]
    fn spawn_agent_calls_run_to_completion_once() {
        let spec = protocol::RunSpec::builder()
            .agent(protocol::AgentId::new(), "1")
            .input("hello")
            .placement(protocol::ExecutionPlacement::Local)
            .work_model(protocol::WorkModel {
                provider: protocol::ModelProvider::OpenAI,
                model_name: "gpt-test".to_string(),
                credential: protocol::CredentialSource::PlatformGateway,
            })
            .build();
        let delegate = protocol::Effect::Delegate {
            agent_id: protocol::AgentId::new(),
            input: "child".to_string(),
        };
        assert_eq!(
            protocol::authorize(&spec, &delegate, &[]),
            protocol::PolicyDecision::Deny {
                reason: "effect is not implemented in this slice".to_string(),
            }
        );
        let mut denied = harness::Driver::boot(spec).unwrap();
        let events_before = denied.events().len();
        let harness_before = denied.state().harness.clone();
        denied.perform(
            &[delegate],
            &[],
            &harness::UnavailableModel,
            &mut harness::InMemory::default(),
        );
        assert_eq!(denied.events().len(), events_before);
        assert_eq!(denied.state().harness, harness_before);
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
                    commands: vec![self.command],
                    wait: WaitCondition::None,
                }
            }
        }

        let dir = std::env::temp_dir().join(format!("gol-host-{}-spawn", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("log");
        let mut journal = Journal::open(&path).unwrap();
        let calls = Cell::new(0u32);
        let mut stand_in = || {
            calls.set(calls.get() + 1);
            0i64
        };
        let (step, harness) = replay(
            &Fixed {
                command: WorkflowCommand::SpawnAgent,
            },
            &WorkflowContext,
            &path,
            &mut journal,
            &mut stand_in,
        )
        .unwrap();
        assert_eq!(calls.get(), 0);
        assert!(read_path(&path).is_empty());
        assert_eq!(step.commands, [WorkflowCommand::SpawnAgent]);
        assert_eq!(step.wait, WaitCondition::None);
        let state = harness.unwrap();
        assert_eq!(state.steps, 1);
        assert_eq!(state.model_calls, 0);
        assert_eq!(
            state.harness,
            protocol::HarnessState::Completed {
                outcome: "done".to_string(),
            }
        );
        assert_eq!(
            state.dispatch,
            protocol::DispatchPhase::Completed {
                outcome: "done".to_string(),
            }
        );

        for command in [
            WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" }),
            WorkflowCommand::ExecuteTool(ToolSpec { name: "other" }),
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
                &Fixed { command },
                &WorkflowContext,
                &case,
                &mut case_journal,
                &mut case_stand_in,
            )
            .unwrap();
            assert_eq!(step.commands, [command]);
            assert!(harness.is_none());
            match command {
                WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" }) => {
                    assert_eq!(case_calls.get(), 1);
                    assert_eq!(read_path(&case), 0i64.to_le_bytes());
                }
                WorkflowCommand::ExecuteTool(_)
                | WorkflowCommand::Complete
                | WorkflowCommand::Fail
                | WorkflowCommand::SpawnAgent => {
                    assert_eq!(case_calls.get(), 0);
                    assert!(read_path(&case).is_empty());
                }
            }
        }

        let failed = dir.join("failed");
        fs::write(&failed, 1i64.to_le_bytes()).unwrap();
        let mut failed_journal = Journal::open(&failed).unwrap();
        let fail_calls = Cell::new(0u32);
        let mut fail_stand_in = || {
            fail_calls.set(fail_calls.get() + 1);
            0i64
        };
        let (step, harness) = replay(
            &CounterBranch,
            &WorkflowContext,
            &failed,
            &mut failed_journal,
            &mut fail_stand_in,
        )
        .unwrap();
        assert_eq!(step.commands, [WorkflowCommand::Fail]);
        assert!(harness.is_none());
        assert_eq!(fail_calls.get(), 0);
        assert_eq!(read_path(&failed).len(), 8);

        struct TwoSpawn;

        impl WorkflowDriver for TwoSpawn {
            fn evaluate(&self, _ctx: &WorkflowContext, _history: &History) -> WorkflowStep {
                WorkflowStep {
                    commands: vec![WorkflowCommand::SpawnAgent, WorkflowCommand::SpawnAgent],
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
            &joined,
            &mut joined_journal,
            &mut join_stand_in,
        )
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "command");
        assert!(join_calls.borrow().is_empty());
        assert!(read_path(&joined).is_empty());
    }
}
