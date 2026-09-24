use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;

use workflow_core::{
    History, ToolSpec, WaitCondition, WorkflowCommand, WorkflowContext, WorkflowDriver,
    WorkflowStep, spawn,
};

use crate::Journal;

pub fn replay(
    driver: &impl WorkflowDriver,
    ctx: &WorkflowContext,
    path: &Path,
    journal: &mut Journal,
    stand_in: &mut dyn FnMut() -> i64,
) -> std::io::Result<WorkflowStep> {
    let recorded = read_path(path)?;
    let history = history_from_committed(&recorded)?;
    let step = driver.evaluate(ctx, &history);
    if let [WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" })] = step.commands.as_slice()
    {
        let value = stand_in();
        journal.commit(&value.to_le_bytes())?;
    }
    Ok(step)
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

#[cfg(test)]
mod tests {
    use super::{join_all, replay};
    use crate::Journal;
    use std::cell::{Cell, RefCell};
    use std::fs::{self, OpenOptions};
    use std::io::Read;
    use std::path::Path;
    use workflow_core::{
        CounterBranch, JoinBranch, ToolSpec, WaitCondition, WorkflowCommand, WorkflowContext,
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

        let first = replay(&driver, &ctx, &path, &mut journal, &mut stand_in).unwrap();
        assert_eq!(calls.get(), 1);
        assert_eq!(
            first.commands,
            [WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" })]
        );
        assert_eq!(first.wait, WaitCondition::None);
        assert_eq!(read_path(&path), 0i64.to_le_bytes());

        let second = replay(&driver, &ctx, &path, &mut journal, &mut stand_in).unwrap();
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
}
