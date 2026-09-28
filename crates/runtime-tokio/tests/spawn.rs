//! A workflow's `SpawnAgent` starts a child run through the spawner (Phase
//! 1.3). The workflow's own run is the child's parent, the journal position
//! is the step, and the child gets the parent's limits. `AgentSpawned` is
//! committed only after the spawner started the child; a refused spawn is not
//! journaled, and the next replay asks again.
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

use harness::{AgentSpawner, ChildRequest, StartedChild};
use protocol::{
    AgentId, CredentialSource, ExecutionPlacement, Limits, ModelProvider, Owner, RunId, RunSpec,
    WorkModel,
};
use runtime_tokio::{replay, Journal};
use workflow_core::{
    evaluate_program, AgentSpec, Decision, History, Record, WorkflowCommand, WorkflowContext,
    WorkflowDriver, WorkflowProgram, WorkflowStep,
};

fn scratch(case: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("gol-spawn-{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join(case);
    let _ = fs::remove_file(&path);
    path
}

fn parent() -> RunSpec {
    RunSpec::builder()
        .owner(Owner::new("https://issuer.test", "user-1", "tenant-1"))
        .agent(AgentId::new(), "1")
        .input("plan the trip")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::Anthropic,
            model_name: "m".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .limits(Limits {
            max_steps: 6,
            max_model_calls: 3,
        })
        .build()
}

/// What a spawner was asked for.
#[derive(Debug, PartialEq, Eq)]
struct Asked {
    parent: RunId,
    step: u32,
    agent_id: AgentId,
    input: String,
    limits: Limits,
}

/// Starts every child it is asked for, or refuses them all with `refusal`.
struct Fake {
    asked: Mutex<Vec<Asked>>,
    refusal: Option<String>,
}

impl Fake {
    fn starting() -> Self {
        Self {
            asked: Mutex::new(Vec::new()),
            refusal: None,
        }
    }

    fn refusing(reason: &str) -> Self {
        Self {
            asked: Mutex::new(Vec::new()),
            refusal: Some(reason.to_string()),
        }
    }

    fn asked(&self) -> usize {
        self.asked.lock().unwrap().len()
    }
}

impl AgentSpawner for Fake {
    fn start(&self, request: ChildRequest<'_>) -> Result<StartedChild, String> {
        self.asked.lock().unwrap().push(Asked {
            parent: request.parent.run_id,
            step: request.step,
            agent_id: request.agent_id,
            input: request.input.to_string(),
            limits: request.limits,
        });
        match &self.refusal {
            Some(reason) => Err(reason.clone()),
            None => Ok(StartedChild {
                run_id: RunId::new(),
                limits: request.limits,
            }),
        }
    }
}

/// Runs `decisions` in order, then completes.
struct Program(Vec<Decision>);

impl WorkflowDriver for Program {
    fn evaluate(&self, _ctx: &WorkflowContext, history: &History) -> WorkflowStep {
        let mut decisions = self.0.clone();
        decisions.push(Decision::Complete);
        evaluate_program(
            &WorkflowProgram {
                root: Decision::Seq(decisions),
            },
            history,
        )
    }
}

fn spawn(agent: &str, input: &str) -> Decision {
    Decision::SpawnAgent {
        agent: agent.to_string(),
        input: input.to_string(),
    }
}

fn no_counter() -> i64 {
    panic!("no counter in this workflow")
}

#[test]
fn a_spawn_starts_the_named_agent_with_its_input() {
    let writer = AgentId::new();
    let parent = parent();
    let spawner = Fake::starting();
    let mut journal = Journal::open(&scratch("starts")).unwrap();
    let (step, spawned) = replay(
        &Program(vec![spawn(&writer.to_string(), "draft")]),
        &WorkflowContext,
        &mut journal,
        &parent,
        &spawner,
        &mut no_counter,
    )
    .unwrap();

    assert_eq!(
        step.commands,
        [WorkflowCommand::SpawnAgent(AgentSpec::new(
            &writer.to_string(),
            "draft"
        ))]
    );
    assert!(matches!(spawned, Some(Ok(_))), "{spawned:?}");
    assert_eq!(
        *spawner.asked.lock().unwrap(),
        [Asked {
            parent: parent.run_id,
            step: 0,
            agent_id: writer,
            input: "draft".to_string(),
            limits: parent.limits,
        }]
    );
    assert_eq!(
        journal.history().unwrap(),
        History::new(vec![Record::AgentSpawned {
            agent: writer.to_string(),
        }])
    );
}

// The step is the spawn's place in the journal, so two spawns of one agent
// with one input are two children.
#[test]
fn a_later_spawn_asks_with_its_journal_position() {
    let writer = AgentId::new().to_string();
    let parent = parent();
    let spawner = Fake::starting();
    let mut journal = Journal::open(&scratch("position")).unwrap();
    let program = Program(vec![spawn(&writer, "draft"), spawn(&writer, "draft")]);
    for _ in 0..3 {
        replay(
            &program,
            &WorkflowContext,
            &mut journal,
            &parent,
            &spawner,
            &mut no_counter,
        )
        .unwrap();
    }
    let steps: Vec<u32> = spawner
        .asked
        .lock()
        .unwrap()
        .iter()
        .map(|asked| asked.step)
        .collect();
    assert_eq!(steps, [0, 1]);
    assert_eq!(journal.history().unwrap().records.len(), 2);
}

#[test]
fn a_replayed_spawn_does_not_start_again() {
    let writer = AgentId::new().to_string();
    let parent = parent();
    let spawner = Fake::starting();
    let path = scratch("replayed");
    let program = Program(vec![spawn(&writer, "draft")]);
    let mut journal = Journal::open(&path).unwrap();
    replay(
        &program,
        &WorkflowContext,
        &mut journal,
        &parent,
        &spawner,
        &mut no_counter,
    )
    .unwrap();
    // A new process opens the journal and replays it.
    let mut reopened = Journal::open(&path).unwrap();
    let (step, spawned) = replay(
        &program,
        &WorkflowContext,
        &mut reopened,
        &parent,
        &spawner,
        &mut no_counter,
    )
    .unwrap();
    assert_eq!(step.commands, [WorkflowCommand::Complete]);
    assert!(spawned.is_none());
    assert_eq!(spawner.asked(), 1);
    assert_eq!(reopened.history().unwrap().records.len(), 1);
}

#[test]
fn a_refused_spawn_is_not_journaled() {
    let writer = AgentId::new().to_string();
    let parent = parent();
    let spawner = Fake::refusing("agent belongs to another owner");
    let mut journal = Journal::open(&scratch("refused")).unwrap();
    let program = Program(vec![spawn(&writer, "draft")]);
    for asked in 1..=2 {
        let (step, spawned) = replay(
            &program,
            &WorkflowContext,
            &mut journal,
            &parent,
            &spawner,
            &mut no_counter,
        )
        .unwrap();
        assert!(matches!(
            step.commands.as_slice(),
            [WorkflowCommand::SpawnAgent(_)]
        ));
        assert_eq!(
            spawned.map(|started| started.map(|_| ())),
            Some(Err("agent belongs to another owner".to_string()))
        );
        assert_eq!(spawner.asked(), asked);
        assert_eq!(journal.history().unwrap(), History::default());
    }
    // The retry is the same request, so it names the same child.
    let asked = spawner.asked.lock().unwrap();
    assert_eq!(asked[0], asked[1]);
}

// Decision 1.3-2A: a workflow names its agent by id. Name lookup comes with
// POST /v1/workflows (D4).
#[test]
fn a_spawn_of_an_agent_that_is_not_an_id_is_refused() {
    let parent = parent();
    let spawner = Fake::starting();
    let mut journal = Journal::open(&scratch("not-an-id")).unwrap();
    let (_, spawned) = replay(
        &Program(vec![spawn("writer", "draft")]),
        &WorkflowContext,
        &mut journal,
        &parent,
        &spawner,
        &mut no_counter,
    )
    .unwrap();
    assert_eq!(
        spawned.map(|started| started.map(|_| ())),
        Some(Err("no such agent".to_string()))
    );
    assert_eq!(spawner.asked(), 0);
    assert_eq!(journal.history().unwrap(), History::default());
}
