#![forbid(unsafe_code)]
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use runtime_tokio::Journal;
use workflow_core::{
    CounterBranch, ToolSpec, WaitCondition, WorkflowCommand, WorkflowContext, WorkflowDriver,
    WorkflowStep,
};

fn main() {
    if let Err(error) = entry() {
        let _ = writeln!(std::io::stderr(), "{error}");
        std::process::exit(1);
    }
}

fn entry() -> std::io::Result<()> {
    let mut args = std::env::args().skip(1);
    let journal_path = PathBuf::from(arg(&mut args)?);
    let effect_path = PathBuf::from(arg(&mut args)?);
    let next_path = PathBuf::from(arg(&mut args)?);
    match arg(&mut args)?.as_str() {
        "after-commit" => after_commit(&journal_path, &effect_path, &next_path),
        "before-commit" => before_commit(&journal_path, &effect_path, &next_path),
        "run" => run(&journal_path, &effect_path, &next_path),
        _ => Err(input("mode")),
    }
}

fn after_commit(journal_path: &Path, effect_path: &Path, next_path: &Path) -> std::io::Result<()> {
    create_empty(effect_path)?;
    create_empty(next_path)?;
    let mut journal = Journal::open(journal_path)?;
    match command(&evaluate(journal_path)?)? {
        WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" }) => {
            let value = stand_in(effect_path)?;
            journal.commit(&value.to_le_bytes())?;
            read_stdin()?;
            match command(&evaluate(journal_path)?)? {
                WorkflowCommand::Complete => append_one(next_path),
                _ => Err(input("command")),
            }
        }
        _ => Err(input("command")),
    }
}

fn before_commit(journal_path: &Path, effect_path: &Path, next_path: &Path) -> std::io::Result<()> {
    create_empty(effect_path)?;
    create_empty(next_path)?;
    let mut journal = Journal::open(journal_path)?;
    match command(&evaluate(journal_path)?)? {
        WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" }) => {
            let value = stand_in(effect_path)?;
            read_stdin()?;
            journal.commit(&value.to_le_bytes())
        }
        _ => Err(input("command")),
    }
}

fn run(journal_path: &Path, effect_path: &Path, next_path: &Path) -> std::io::Result<()> {
    let mut journal = Journal::open(journal_path)?;
    loop {
        match command(&evaluate(journal_path)?)? {
            WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" }) => {
                let value = stand_in(effect_path)?;
                journal.commit(&value.to_le_bytes())?;
            }
            WorkflowCommand::Complete => {
                append_one(next_path)?;
                return Ok(());
            }
            WorkflowCommand::ExecuteTool(_)
            | WorkflowCommand::Fail
            | WorkflowCommand::SpawnAgent => {
                return Err(input("command"));
            }
        }
    }
}

fn evaluate(journal_path: &Path) -> std::io::Result<WorkflowStep> {
    let recorded = read_committed(journal_path)?;
    let history = history_from_committed(&recorded)?;
    let step = CounterBranch.evaluate(&WorkflowContext, &history);
    if step.wait != WaitCondition::None {
        return Err(input("wait"));
    }
    Ok(step)
}

fn history_from_committed(bytes: &[u8]) -> std::io::Result<workflow_core::History> {
    match bytes.len() {
        0 => Ok(workflow_core::History { counter: None }),
        8 => {
            let array: [u8; 8] = bytes.try_into().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "journal length")
            })?;
            Ok(workflow_core::History {
                counter: Some(i64::from_le_bytes(array)),
            })
        }
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "journal length",
        )),
    }
}

fn command(step: &WorkflowStep) -> std::io::Result<WorkflowCommand> {
    match step.commands.as_slice() {
        [command] => Ok(*command),
        _ => Err(input("command")),
    }
}

fn stand_in(effect_path: &Path) -> std::io::Result<i64> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(effect_path)?;
    file.write_all(&[1])?;
    Ok(0i64)
}

fn append_one(path: &Path) -> std::io::Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(&[1])?;
    Ok(())
}

fn create_empty(path: &Path) -> std::io::Result<()> {
    File::create(path)?;
    Ok(())
}

fn read_committed(path: &Path) -> std::io::Result<Vec<u8>> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    Ok(buf)
}

fn read_stdin() -> std::io::Result<()> {
    let mut byte = [0u8; 1];
    std::io::stdin().read_exact(&mut byte)?;
    Ok(())
}

fn arg(args: &mut impl Iterator<Item = String>) -> std::io::Result<String> {
    args.next().ok_or_else(|| input("args"))
}

fn input(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}
