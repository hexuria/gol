//! Delegation in the driver. An authorized `Delegate` is performed by the
//! driver like a memory access: it records what happened and the run goes on.
//! Until a spawner is wired in, what happens is a refusal. Every event of a
//! child run names its parent.

use std::sync::{Arc, Mutex};

use harness::{
    run_to_completion, AgentSpawner, ChildRequest, Driver, InMemory, ScriptedDecider, StartedChild,
    UnavailableModel,
};
use protocol::{
    AgentId, Capability, CredentialSource, Effect, EventPayload, ExecutionPlacement, FailureClass,
    HarnessState, Limits, ModelProvider, Owner, RunId, RunSpec, WorkModel,
};

fn spec(capabilities: &[&str]) -> RunSpec {
    RunSpec::builder()
        .owner(Owner::new("https://issuer.test", "user-1", "tenant-1"))
        .agent(AgentId::new(), "1")
        .input("hi")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::Anthropic,
            model_name: "m".into(),
            credential: CredentialSource::PlatformGateway,
        })
        .capabilities(
            capabilities
                .iter()
                .map(|name| Capability::new(*name))
                .collect(),
        )
        .build()
}

fn run(spec: RunSpec, effects: Vec<Effect>) -> Driver {
    let mut driver = Driver::boot(spec).unwrap();
    let mut decider = ScriptedDecider::new(effects);
    run_to_completion(
        &mut driver,
        &mut decider,
        &[],
        &UnavailableModel,
        &InMemory::default(),
    )
    .unwrap();
    driver
}

fn delegate(agent_id: AgentId) -> Effect {
    Effect::Delegate {
        agent_id,
        input: "draft".to_string(),
    }
}

fn complete() -> Effect {
    Effect::Complete {
        outcome: "done".to_string(),
    }
}

#[test]
fn a_delegate_with_no_spawner_is_refused_and_the_run_goes_on() {
    let agent = AgentId::new();
    let driver = run(spec(&["agent.delegate"]), vec![delegate(agent), complete()]);
    let payloads: Vec<&EventPayload> = driver.events().iter().map(|e| &e.payload).collect();
    assert!(
        payloads.contains(&&EventPayload::EffectAuthorized {
            effect: delegate(agent)
        }),
        "{payloads:?}"
    );
    assert!(
        payloads.contains(&&EventPayload::DelegateRefused {
            agent_id: agent,
            reason: "no agent spawner is configured".to_string(),
        }),
        "{payloads:?}"
    );
    assert_eq!(
        driver.state().harness,
        HarnessState::Completed {
            outcome: "done".to_string()
        }
    );
    assert_eq!(driver.state().children, 0);
}

#[test]
fn a_delegate_without_its_capability_is_denied() {
    let agent = AgentId::new();
    let driver = run(spec(&[]), vec![delegate(agent), complete()]);
    assert!(driver.events().iter().any(|event| event.payload
        == EventPayload::EffectDenied {
            effect: delegate(agent),
            reason: "missing capability: agent.delegate".to_string(),
        }));
    assert!(!driver
        .events()
        .iter()
        .any(|event| matches!(event.payload, EventPayload::DelegateRefused { .. })));
}

#[test]
fn every_event_of_a_child_run_names_its_parent() {
    let parent = spec(&[]);
    let child = RunSpec::builder()
        .owner(parent.owner.clone())
        .agent(AgentId::new(), "1")
        .input("draft")
        .placement(parent.placement)
        .work_model(parent.work_model.clone())
        .child_of(&parent, 1)
        .build();
    let driver = run(child, vec![complete()]);
    assert!(!driver.events().is_empty());
    assert!(driver
        .events()
        .iter()
        .all(|event| event.envelope.parent_run_id == Some(parent.run_id)));
    let top = run(parent, vec![complete()]);
    assert!(top
        .events()
        .iter()
        .all(|event| event.envelope.parent_run_id.is_none()));
}

/// What a spawner was asked: parent, step, agent, input and limits.
type Asked = (RunId, u32, AgentId, String, Limits);

/// A spawner that records what it was asked and answers with `answer`.
struct Fake {
    asked: Mutex<Vec<Asked>>,
    answer: Result<(), String>,
}

impl Fake {
    fn new(answer: Result<(), String>) -> Arc<Self> {
        Arc::new(Self {
            asked: Mutex::new(Vec::new()),
            answer,
        })
    }
}

impl AgentSpawner for Fake {
    fn start(&self, request: ChildRequest<'_>) -> Result<StartedChild, String> {
        self.asked.lock().unwrap().push((
            request.parent.run_id,
            request.step,
            request.agent_id,
            request.input.to_string(),
            request.limits,
        ));
        self.answer.clone().map(|()| StartedChild {
            run_id: RunId::new(),
            limits: request.limits,
        })
    }
}

fn run_with(spec: RunSpec, spawner: Arc<Fake>, effects: Vec<Effect>) -> Driver {
    let mut driver = Driver::boot(spec)
        .unwrap()
        .with_spawner(spawner, Vec::new());
    let mut decider = ScriptedDecider::new(effects);
    run_to_completion(
        &mut driver,
        &mut decider,
        &[],
        &UnavailableModel,
        &InMemory::default(),
    )
    .unwrap();
    driver
}

fn limited(steps: u32, calls: u32) -> RunSpec {
    let mut spec = spec(&["agent.delegate"]);
    spec.limits = Limits {
        max_steps: steps,
        max_model_calls: calls,
    };
    spec
}

fn started(driver: &Driver) -> Vec<(AgentId, Limits)> {
    driver
        .events()
        .iter()
        .filter_map(|event| match &event.payload {
            EventPayload::ChildStarted {
                agent_id, limits, ..
            } => Some((*agent_id, *limits)),
            _ => None,
        })
        .collect()
}

fn refusals(driver: &Driver) -> Vec<String> {
    driver
        .events()
        .iter()
        .filter_map(|event| match &event.payload {
            EventPayload::DelegateRefused { reason, .. } => Some(reason.clone()),
            _ => None,
        })
        .collect()
}

// The spawner is asked for the named agent with the input, at the step that
// asked, with half of what the parent had left: 8 steps less the one this
// decision cost is 7, so 3; 4 model calls, so 2.
#[test]
fn a_delegate_starts_a_child_through_the_spawner() {
    let spec = limited(8, 4);
    let parent = spec.run_id;
    let agent = AgentId::new();
    let fake = Fake::new(Ok(()));
    let driver = run_with(spec, fake.clone(), vec![delegate(agent), complete()]);
    let half = Limits {
        max_steps: 3,
        max_model_calls: 2,
    };
    assert_eq!(
        *fake.asked.lock().unwrap(),
        vec![(parent, 1, agent, "draft".to_string(), half)]
    );
    assert_eq!(started(&driver), vec![(agent, half)]);
    let state = driver.state();
    assert_eq!(state.children, 1);
    assert_eq!((state.given_steps, state.given_model_calls), (3, 2));
    assert!(matches!(state.harness, HarnessState::Completed { .. }));
}

// What a child was given is spent for the parent: of 8 steps, the delegate
// costs 1 and the child takes 3, so the parent has 4 more decisions before
// its budget ends it.
#[test]
fn a_parent_keeps_only_what_it_did_not_give() {
    let wait = Effect::Wait {
        reason: "w".to_string(),
    };
    let mut effects = vec![delegate(AgentId::new())];
    effects.extend(std::iter::repeat_n(wait.clone(), 10));
    let driver = run_with(limited(8, 4), Fake::new(Ok(())), effects);
    let waits = driver
        .events()
        .iter()
        .filter(|event| {
            event.payload
                == EventPayload::EffectDecided {
                    effect: wait.clone(),
                }
        })
        .count();
    assert_eq!(waits, 4);
    assert!(matches!(
        driver.state().harness,
        HarnessState::Failed {
            class: FailureClass::Budget,
            ..
        }
    ));
}

// A parent with fewer than 2 steps or model calls left has nothing to give.
#[test]
fn a_delegate_with_too_little_budget_left_is_refused() {
    for spec in [limited(2, 4), limited(8, 1)] {
        let fake = Fake::new(Ok(()));
        let driver = run_with(
            spec,
            fake.clone(),
            vec![delegate(AgentId::new()), complete()],
        );
        assert!(fake.asked.lock().unwrap().is_empty());
        assert_eq!(
            refusals(&driver),
            vec!["not enough budget left to give a child".to_string()]
        );
        assert_eq!(driver.state().children, 0);
    }
}

// A run may start MAX_CHILDREN (10) children; the eleventh is refused.
#[test]
fn the_eleventh_child_is_refused() {
    let fake = Fake::new(Ok(()));
    let effects = std::iter::repeat_with(|| delegate(AgentId::new()))
        .take(11)
        .chain([complete()])
        .collect();
    let driver = run_with(limited(100_000, 100_000), fake.clone(), effects);
    assert_eq!(started(&driver).len(), 10);
    assert_eq!(
        refusals(&driver),
        vec!["already started 10 children".to_string()]
    );
    assert_eq!(fake.asked.lock().unwrap().len(), 10);
}

// A spawner's refusal is recorded, and takes nothing from the budget.
#[test]
fn a_spawner_refusal_is_recorded() {
    let driver = run_with(
        limited(8, 4),
        Fake::new(Err("agent belongs to another owner".to_string())),
        vec![delegate(AgentId::new()), complete()],
    );
    assert_eq!(
        refusals(&driver),
        vec!["agent belongs to another owner".to_string()]
    );
    let state = driver.state();
    assert_eq!(state.children, 0);
    assert_eq!((state.given_steps, state.given_model_calls), (0, 0));
}

/// A model that always answers.
struct Answering;

impl harness::ModelCompletion for Answering {
    fn complete(
        &self,
        _request: &protocol::ModelRequest,
    ) -> Result<protocol::ModelMessage, String> {
        Ok(protocol::ModelMessage {
            role: protocol::MessageRole::Assistant,
            text: "ok".into(),
        })
    }
}

// Model calls given to a child are spent for the parent too: of 4, the child
// takes 2, so the parent's third model call ends it on its budget.
#[test]
fn a_parent_keeps_only_the_model_calls_it_did_not_give() {
    let mut spec = spec(&["agent.delegate", "model.call"]);
    spec.limits = Limits {
        max_steps: 100,
        max_model_calls: 4,
    };
    let model = Effect::ModelCall {
        prompt: "p".to_string(),
    };
    let mut effects = vec![delegate(AgentId::new())];
    effects.extend(std::iter::repeat_n(model.clone(), 5));
    let mut driver = Driver::boot(spec)
        .unwrap()
        .with_spawner(Fake::new(Ok(())), Vec::new());
    let mut decider = ScriptedDecider::new(effects);
    run_to_completion(
        &mut driver,
        &mut decider,
        &[],
        &Answering,
        &InMemory::default(),
    )
    .unwrap();
    assert_eq!(driver.state().model_calls, 2);
    assert!(matches!(
        driver.state().harness,
        HarnessState::Failed {
            class: FailureClass::Budget,
            ..
        }
    ));
}

/// A spawner that names the same child for every request, as the owned
/// spawner does for the same request, and reports the limits that child was
/// first stored with.
struct SameChild {
    run_id: RunId,
    stored: Mutex<Option<Limits>>,
}

impl SameChild {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            run_id: RunId::new(),
            stored: Mutex::new(None),
        })
    }
}

impl AgentSpawner for SameChild {
    fn start(&self, request: ChildRequest<'_>) -> Result<StartedChild, String> {
        let limits = *self.stored.lock().unwrap().get_or_insert(request.limits);
        Ok(StartedChild {
            run_id: self.run_id,
            limits,
        })
    }
}

// The same delegation decided twice names one child: it is counted, and its
// budget given, once.
#[test]
fn the_same_child_twice_is_counted_once() {
    let agent = AgentId::new();
    let mut driver = Driver::boot(limited(8, 4))
        .unwrap()
        .with_spawner(SameChild::new(), Vec::new());
    let mut decider = ScriptedDecider::new(vec![delegate(agent), delegate(agent), complete()]);
    run_to_completion(
        &mut driver,
        &mut decider,
        &[],
        &UnavailableModel,
        &InMemory::default(),
    )
    .unwrap();
    assert_eq!(started(&driver).len(), 2);
    let state = driver.state();
    assert_eq!(state.children, 1);
    assert_eq!((state.given_steps, state.given_model_calls), (3, 2));
}

// Performing a delegation when the run is not running starts nothing.
#[test]
fn a_delegate_performed_while_not_running_is_refused() {
    let fake = Fake::new(Ok(()));
    let mut driver = Driver::boot(limited(8, 4))
        .unwrap()
        .with_spawner(fake.clone(), Vec::new());
    driver.cancel();
    driver.perform(
        &[delegate(AgentId::new())],
        &[],
        &UnavailableModel,
        &InMemory::default(),
    );
    assert!(fake.asked.lock().unwrap().is_empty());
    assert_eq!(
        refusals(&driver),
        vec!["the run is not running".to_string()]
    );
}

// A parent run again from an empty log (its first run crashed after the
// child was started) may reach the same child after a longer prefix and ask
// for a smaller carve. It records the child's stored limits, so its budget
// still counts what the child really has.
#[test]
fn a_replayed_parent_records_the_stored_childs_limits() {
    let agent = AgentId::new();
    let spawner = SameChild::new();
    let wait = Effect::Wait {
        reason: "w".to_string(),
    };
    let first = {
        let mut driver = Driver::boot(limited(8, 4))
            .unwrap()
            .with_spawner(spawner.clone(), Vec::new());
        let mut decider = ScriptedDecider::new(vec![delegate(agent), complete()]);
        run_to_completion(
            &mut driver,
            &mut decider,
            &[],
            &UnavailableModel,
            &InMemory::default(),
        )
        .unwrap();
        driver.state().given_steps
    };
    assert_eq!(first, 3);
    let mut again = Driver::boot(limited(8, 4))
        .unwrap()
        .with_spawner(spawner, Vec::new());
    let mut decider = ScriptedDecider::new(vec![
        wait.clone(),
        wait.clone(),
        wait,
        delegate(agent),
        complete(),
    ]);
    run_to_completion(
        &mut again,
        &mut decider,
        &[],
        &UnavailableModel,
        &InMemory::default(),
    )
    .unwrap();
    assert_eq!(again.state().given_steps, 3);
}
