#![forbid(unsafe_code)]
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use harness::{AgentSpawner, ChildRequest, StartedChild};
use runtime_tokio::{frame, replay, start_frame, Journal};
use workflow_core::{
    evaluate_program, CounterBranch, Decision, History, Record, WaitCondition, WorkflowCommand,
    WorkflowContext, WorkflowDriver, WorkflowProgram, WorkflowStep,
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
        "tear-at" => tear_at(&journal_path, &effect_path, &next_path, cut(&mut args)?),
        "tear-start-at" => tear_start_at(&journal_path, &effect_path, &next_path, cut(&mut args)?),
        "run" => run(&journal_path, &effect_path, &next_path),
        "spawn-before-commit" => spawn_once(&journal_path, &effect_path, &next_path, Kill::InStart),
        "spawn-after-commit" => {
            spawn_once(&journal_path, &effect_path, &next_path, Kill::AfterCommit)
        }
        "run-spawn" => run_spawn(&journal_path, &effect_path, &next_path),
        _ => Err(input("mode")),
    }
}

fn after_commit(journal_path: &Path, effect_path: &Path, next_path: &Path) -> std::io::Result<()> {
    create_empty(effect_path)?;
    create_empty(next_path)?;
    let mut journal = Journal::open(journal_path)?;
    match command(&evaluate(&journal)?)? {
        WorkflowCommand::ExecuteTool(tool) if tool.name == "counter" => {
            let value = stand_in(effect_path)?;
            journal.commit(&Record::counter(value))?;
            read_stdin()?;
            match command(&evaluate(&journal)?)? {
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
    match command(&evaluate(&journal)?)? {
        WorkflowCommand::ExecuteTool(tool) if tool.name == "counter" => {
            let value = stand_in(effect_path)?;
            read_stdin()?;
            journal.commit(&Record::counter(value))
        }
        _ => Err(input("command")),
    }
}

/// Runs the counter, then writes the first `cut` bytes of its record, as a
/// commit killed part way leaves them, and waits to be killed.
fn tear_at(
    journal_path: &Path,
    effect_path: &Path,
    next_path: &Path,
    cut: usize,
) -> std::io::Result<()> {
    create_empty(effect_path)?;
    create_empty(next_path)?;
    let journal = Journal::open(journal_path)?;
    match command(&evaluate(&journal)?)? {
        WorkflowCommand::ExecuteTool(tool) if tool.name == "counter" => {
            let value = stand_in(effect_path)?;
            append(journal_path, torn(&frame(&Record::counter(value))?, cut)?)?;
            read_stdin()
        }
        _ => Err(input("command")),
    }
}

/// Writes the first `cut` bytes of a new journal's `Start` frame, as a first
/// open killed part way leaves them, and waits to be killed.
fn tear_start_at(
    journal_path: &Path,
    effect_path: &Path,
    next_path: &Path,
    cut: usize,
) -> std::io::Result<()> {
    create_empty(effect_path)?;
    create_empty(next_path)?;
    std::fs::write(journal_path, torn(&start_frame(), cut)?)?;
    read_stdin()
}

/// The first `cut` bytes of `frame`, when they are some but not all of it.
fn torn(frame: &[u8], cut: usize) -> std::io::Result<&[u8]> {
    match frame.get(..cut) {
        Some(prefix) if cut > 0 && cut < frame.len() => Ok(prefix),
        _ => Err(input("cut")),
    }
}

fn run(journal_path: &Path, effect_path: &Path, next_path: &Path) -> std::io::Result<()> {
    let mut journal = Journal::open(journal_path)?;
    loop {
        match command(&evaluate(&journal)?)? {
            WorkflowCommand::ExecuteTool(tool) if tool.name == "counter" => {
                let value = stand_in(effect_path)?;
                journal.commit(&Record::counter(value))?;
            }
            WorkflowCommand::Complete => {
                append_one(next_path)?;
                return Ok(());
            }
            WorkflowCommand::ExecuteTool(_)
            | WorkflowCommand::Fail
            | WorkflowCommand::SpawnAgent(_) => {
                return Err(input("command"));
            }
        }
    }
}

fn evaluate(journal: &Journal) -> std::io::Result<WorkflowStep> {
    let step = CounterBranch.evaluate(&WorkflowContext, &journal.history()?);
    if step.wait != WaitCondition::None {
        return Err(input("wait"));
    }
    Ok(step)
}

fn command(step: &WorkflowStep) -> std::io::Result<WorkflowCommand> {
    match step.commands.as_slice() {
        [command] => Ok(command.clone()),
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
    append(path, &[1])
}

fn append(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(bytes)
}

fn create_empty(path: &Path) -> std::io::Result<()> {
    File::create(path)?;
    Ok(())
}

fn read_stdin() -> std::io::Result<()> {
    let mut byte = [0u8; 1];
    std::io::stdin().read_exact(&mut byte)?;
    Ok(())
}

fn arg(args: &mut impl Iterator<Item = String>) -> std::io::Result<String> {
    args.next().ok_or_else(|| input("args"))
}

fn cut(args: &mut impl Iterator<Item = String>) -> std::io::Result<usize> {
    arg(args)?.parse().map_err(|_| input("cut"))
}

fn input(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

/// The agent the spawning workflow starts, named by id.
const CHILD: &str = "00000000-0000-4000-8000-00000000c41d";

/// Spawns `CHILD` with input "draft", then completes.
struct SpawnsOne;

impl WorkflowDriver for SpawnsOne {
    fn evaluate(&self, _ctx: &WorkflowContext, history: &History) -> WorkflowStep {
        evaluate_program(
            &WorkflowProgram {
                root: Decision::Seq(vec![
                    Decision::SpawnAgent {
                        agent: CHILD.to_string(),
                        input: "draft".to_string(),
                    },
                    Decision::Complete,
                ]),
            },
            history,
        )
    }
}

/// The workflow's own run: the same in every process, as a stored run is.
fn parent() -> std::io::Result<protocol::RunSpec> {
    let mut spec = protocol::RunSpec::builder()
        .owner(protocol::Owner::new("local", "replay-proof", "local"))
        .agent(protocol::AgentId::new(), "1")
        .input("plan")
        .placement(protocol::ExecutionPlacement::Local)
        .work_model(protocol::WorkModel {
            provider: protocol::ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: protocol::CredentialSource::PlatformGateway,
        })
        .build();
    spec.run_id = "00000000-0000-4000-8000-0000000000aa"
        .parse()
        .map_err(|_| input("parent"))?;
    Ok(spec)
}

/// Where a spawning process waits to be killed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kill {
    /// Inside the spawner, after it recorded the child and before the commit.
    InStart,
    /// After `AgentSpawned` is committed.
    AfterCommit,
}

/// Records each child it is asked for as one line of the effect file: the
/// parent, the step, the agent and the input, which together name the child.
struct FileSpawner<'a> {
    effect_path: &'a Path,
    kill: Option<Kill>,
}

impl AgentSpawner for FileSpawner<'_> {
    fn start(&self, request: ChildRequest<'_>) -> Result<StartedChild, String> {
        let line = format!(
            "{} {} {} {}\n",
            request.parent.run_id, request.step, request.agent_id, request.input
        );
        append(self.effect_path, line.as_bytes()).map_err(|error| error.to_string())?;
        if self.kill == Some(Kill::InStart) {
            read_stdin().map_err(|error| error.to_string())?;
        }
        Ok(StartedChild {
            run_id: protocol::RunId::new(),
            limits: request.limits,
        })
    }
}

fn spawn_once(
    journal_path: &Path,
    effect_path: &Path,
    next_path: &Path,
    kill: Kill,
) -> std::io::Result<()> {
    create_empty(effect_path)?;
    create_empty(next_path)?;
    let mut journal = Journal::open(journal_path)?;
    let spawner = FileSpawner {
        effect_path,
        kill: Some(kill),
    };
    let (step, spawned) = replay(
        &SpawnsOne,
        &WorkflowContext,
        &mut journal,
        &parent()?,
        &spawner,
        &mut || 0,
    )?;
    match (step.commands.as_slice(), spawned) {
        ([WorkflowCommand::SpawnAgent(_)], Some(Ok(_))) => read_stdin(),
        _ => Err(input("command")),
    }
}

fn run_spawn(journal_path: &Path, effect_path: &Path, next_path: &Path) -> std::io::Result<()> {
    let mut journal = Journal::open(journal_path)?;
    let spawner = FileSpawner {
        effect_path,
        kill: None,
    };
    let parent = parent()?;
    loop {
        let (step, spawned) = replay(
            &SpawnsOne,
            &WorkflowContext,
            &mut journal,
            &parent,
            &spawner,
            &mut || 0,
        )?;
        match (step.commands.as_slice(), spawned) {
            ([WorkflowCommand::SpawnAgent(_)], Some(Ok(_))) => {}
            ([WorkflowCommand::Complete], None) => return append_one(next_path),
            _ => return Err(input("command")),
        }
    }
}
