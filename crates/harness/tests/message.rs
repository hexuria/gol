//! Messages between agents in the driver (Phase 2.2). An authorized
//! `SendMessage` is performed like a delegation: the deliverer accepts it
//! (`MessageSent`) or not (`MessageRefused`), and without a deliverer every
//! message is refused. A tell goes on. An ask waits: `run_until` returns with
//! the harness in `WaitingForMessage`, and the reply to that ask, or its
//! timeout, answers the step when the run is resumed.
use std::sync::{Arc, Mutex};

use harness::{
    run_until, Boundary, Driver, InMemory, MessageDeliverer, MessageRequest, ScriptedDecider,
    UnavailableModel,
};
use protocol::{
    Actor, AgentId, Capability, CredentialSource, Effect, Event, EventPayload, EventSource,
    ExecutionPlacement, HarnessState, Limits, MessageId, ModelProvider, Owner, RunId, RunSpec,
    Timestamp, WorkModel,
};
use uuid::Uuid;

fn spec() -> RunSpec {
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
        .capabilities(vec![Capability::new("agent.message")])
        .limits(Limits {
            max_steps: 6,
            max_model_calls: 2,
        })
        .build()
}

fn writer() -> AgentId {
    AgentId::from_uuid(Uuid::from_u128(9))
}

fn sent_id() -> MessageId {
    MessageId::from_uuid(Uuid::from_u128(1))
}

fn message(expects_reply: bool, timeout_secs: Option<u32>) -> Effect {
    Effect::SendMessage {
        to: writer(),
        body: "draft the plan".to_string(),
        expects_reply,
        reply_to: None,
        timeout_secs,
    }
}

fn complete() -> Effect {
    Effect::Complete {
        outcome: "done".to_string(),
    }
}

/// What a deliverer was asked for.
#[derive(Debug, PartialEq, Eq)]
struct Asked {
    from: RunId,
    decision: u32,
    to: AgentId,
    body: String,
    expects_reply: bool,
    reply_to: Option<MessageId>,
    timeout_secs: Option<u32>,
}

/// Accepts every message as `sent_id()`, or refuses them all.
struct Fake {
    asked: Mutex<Vec<Asked>>,
    refusal: Option<String>,
}

impl Fake {
    fn accepting() -> Arc<Self> {
        Arc::new(Self {
            asked: Mutex::new(Vec::new()),
            refusal: None,
        })
    }

    fn refusing(reason: &str) -> Arc<Self> {
        Arc::new(Self {
            asked: Mutex::new(Vec::new()),
            refusal: Some(reason.to_string()),
        })
    }
}

impl MessageDeliverer for Fake {
    fn send(&self, request: MessageRequest<'_>) -> Result<MessageId, String> {
        self.asked.lock().unwrap().push(Asked {
            from: request.from.run_id,
            decision: request.decision,
            to: request.to,
            body: request.body.to_string(),
            expects_reply: request.expects_reply,
            reply_to: request.reply_to,
            timeout_secs: request.timeout_secs,
        });
        match &self.refusal {
            Some(reason) => Err(reason.clone()),
            None => Ok(sent_id()),
        }
    }
}

/// Runs `driver` with `script` until it ends or waits.
fn run(driver: &mut Driver, script: Vec<Effect>) {
    let mut decider = ScriptedDecider::new(script);
    run_until(
        driver,
        &mut decider,
        &[],
        &UnavailableModel,
        &InMemory::default(),
        &mut |_| Boundary::Continue,
    )
    .unwrap();
}

fn payloads(driver: &Driver) -> Vec<EventPayload> {
    driver
        .events()
        .iter()
        .map(|event| event.payload.clone())
        .collect()
}

fn event(spec: &RunSpec, payload: EventPayload) -> Event {
    Event::record(
        EventSource::for_spec(spec, Actor::System, Timestamp::now()),
        payload,
    )
}

#[test]
fn a_tell_is_sent_and_the_run_goes_on() {
    let spec = spec();
    let deliverer = Fake::accepting();
    let mut driver = Driver::boot(spec.clone())
        .unwrap()
        .with_deliverer(deliverer.clone());
    run(&mut driver, vec![message(false, None), complete()]);
    assert!(payloads(&driver).contains(&EventPayload::MessageSent {
        message_id: sent_id(),
        to: writer(),
        expects_reply: false,
    }));
    assert!(matches!(
        driver.state().harness,
        HarnessState::Completed { .. }
    ));
    assert_eq!(
        *deliverer.asked.lock().unwrap(),
        [Asked {
            from: spec.run_id,
            decision: 1,
            to: writer(),
            body: "draft the plan".to_string(),
            expects_reply: false,
            reply_to: None,
            timeout_secs: None,
        }]
    );
}

// An ask parks the run: run_until returns with the harness waiting, and the
// decider is not asked again. Resumed with the reply, the run goes on.
#[test]
fn an_ask_waits_for_its_reply_and_the_reply_answers_it() {
    let spec = spec();
    let deliverer = Fake::accepting();
    let mut driver = Driver::boot(spec.clone())
        .unwrap()
        .with_deliverer(deliverer.clone());
    let mut decider = ScriptedDecider::new([message(true, Some(60)), complete()]);
    run_until(
        &mut driver,
        &mut decider,
        &[],
        &UnavailableModel,
        &InMemory::default(),
        &mut |_| Boundary::Continue,
    )
    .unwrap();
    assert_eq!(
        driver.state().harness,
        HarnessState::WaitingForMessage {
            step: 1,
            attempt: 0,
            message_id: sent_id(),
        }
    );
    assert_eq!(deliverer.asked.lock().unwrap()[0].timeout_secs, Some(60));
    let parked = driver.events().to_vec();
    assert!(!parked
        .iter()
        .any(|event| matches!(event.payload, EventPayload::RunCompleted { .. })));

    // A message that is not the reply leaves it waiting.
    let mut log = parked.clone();
    log.push(event(
        &spec,
        EventPayload::MessageReceived {
            message_id: MessageId::new(),
            from_agent: writer(),
            from_run: RunId::new(),
            body: "unrelated".to_string(),
            reply_to: None,
        },
    ));
    let mut resumed = Driver::resume(spec.clone(), log.clone()).unwrap();
    run(&mut resumed, vec![complete()]);
    assert!(matches!(
        resumed.state().harness,
        HarnessState::WaitingForMessage { .. }
    ));

    // The reply answers the asking step, and the run finishes.
    log.push(event(
        &spec,
        EventPayload::MessageReceived {
            message_id: MessageId::new(),
            from_agent: writer(),
            from_run: RunId::new(),
            body: "the plan".to_string(),
            reply_to: Some(sent_id()),
        },
    ));
    let mut answered = Driver::resume(spec, log).unwrap();
    run(&mut answered, vec![complete()]);
    assert!(matches!(
        answered.state().harness,
        HarnessState::Completed { .. }
    ));
}

#[test]
fn an_ask_that_timed_out_goes_on_without_a_reply() {
    let spec = spec();
    let mut driver = Driver::boot(spec.clone())
        .unwrap()
        .with_deliverer(Fake::accepting());
    run(&mut driver, vec![message(true, Some(5))]);
    let mut log = driver.events().to_vec();
    log.push(event(
        &spec,
        EventPayload::AskTimedOut {
            message_id: sent_id(),
        },
    ));
    let mut resumed = Driver::resume(spec, log).unwrap();
    run(&mut resumed, vec![complete()]);
    assert!(matches!(
        resumed.state().harness,
        HarnessState::Completed { .. }
    ));
}

// Until a deliverer is wired in (Phase 2.1+2.3), every message is refused
// and the run goes on.
#[test]
fn a_message_without_a_deliverer_is_refused() {
    let mut driver = Driver::boot(spec()).unwrap();
    run(&mut driver, vec![message(true, None), complete()]);
    assert!(payloads(&driver).contains(&EventPayload::MessageRefused {
        to: writer(),
        reason: "no message deliverer is configured".to_string(),
    }));
    assert!(matches!(
        driver.state().harness,
        HarnessState::Completed { .. }
    ));
}

// A deliverer that refuses leaves an ask not waiting.
#[test]
fn a_refused_ask_does_not_wait() {
    let mut driver = Driver::boot(spec())
        .unwrap()
        .with_deliverer(Fake::refusing("agent belongs to another owner"));
    run(&mut driver, vec![message(true, None), complete()]);
    assert!(payloads(&driver).contains(&EventPayload::MessageRefused {
        to: writer(),
        reason: "agent belongs to another owner".to_string(),
    }));
    assert!(matches!(
        driver.state().harness,
        HarnessState::Completed { .. }
    ));
}

// Without agent.message the policy denies it, and the deliverer is never
// asked.
#[test]
fn a_message_without_the_capability_is_denied() {
    let mut spec = spec();
    spec.capabilities.clear();
    let deliverer = Fake::accepting();
    let mut driver = Driver::boot(spec)
        .unwrap()
        .with_deliverer(deliverer.clone());
    run(&mut driver, vec![message(false, None), complete()]);
    assert!(payloads(&driver).iter().any(|payload| matches!(
        payload,
        EventPayload::EffectDenied { reason, .. } if reason == "missing capability: agent.message"
    )));
    assert!(deliverer.asked.lock().unwrap().is_empty());
}

// Review of #73: two tells in one harness step are two messages. The request
// names each by the run's decision count, which a resumed run repeats for
// the same decision, so a deliverer can tell them apart and dedupe a resend.
#[test]
fn two_tells_in_one_step_are_two_requests() {
    let spec = spec();
    let deliverer = Fake::accepting();
    let mut driver = Driver::boot(spec)
        .unwrap()
        .with_deliverer(deliverer.clone());
    run(
        &mut driver,
        vec![message(false, None), message(false, None), complete()],
    );
    let decisions: Vec<u32> = deliverer
        .asked
        .lock()
        .unwrap()
        .iter()
        .map(|asked| asked.decision)
        .collect();
    assert_eq!(decisions, [1, 2]);
}
