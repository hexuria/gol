use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;

use workflow_core::{
    History, ToolSpec, WorkflowCommand, WorkflowContext, WorkflowDriver, WorkflowStep,
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

#[cfg(test)]
mod tests {
    use super::replay;
    use crate::Journal;
    use std::cell::Cell;
    use std::fs::{self, OpenOptions};
    use std::io::Read;
    use std::path::Path;
    use workflow_core::{
        CounterBranch, ToolSpec, WaitCondition, WorkflowCommand, WorkflowContext,
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
}
