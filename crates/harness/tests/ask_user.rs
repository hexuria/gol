//! A question to the user in the driver (Phase 3.5). An authorized
//! `AskUser` is put as `UserAsked` when the run can wait for the answer (a
//! queued worker parks it, `with_user_questions`), and refused as
//! `UserAskRefused` otherwise. The asking step waits: `run_until` returns
//! with the harness in `WaitingForMessage`, and `UserAnswered` answers it
//! when the run is resumed.
use harness::{run_until, Boundary, Driver, InMemory, ScriptedDecider, UnavailableModel};
use protocol::{
    Actor, AgentId, Capability, CredentialSource, Effect, Event, EventPayload, EventSource,
    ExecutionPlacement, HarnessState, Limits, MessageId, ModelProvider, Owner, RunSpec, Timestamp,
    WorkModel,
};

fn spec(capabilities: &[&str]) -> RunSpec {
    RunSpec::builder()
        .owner(Owner::new("https://issuer.test", "user-1", "tenant-1"))
        .agent(AgentId::new(), "1")
        .input("book a flight")
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
        .limits(Limits {
            max_steps: 6,
            max_model_calls: 2,
        })
        .build()
}

fn ask() -> Effect {
    Effect::AskUser {
        prompt: "Which airport?".to_string(),
    }
}

fn complete() -> Effect {
    Effect::Complete {
        outcome: "done".to_string(),
    }
}

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

/// The question the log put, as (id, prompt).
fn asked(driver: &Driver) -> Vec<(MessageId, String)> {
    payloads(driver)
        .into_iter()
        .filter_map(|payload| match payload {
            EventPayload::UserAsked { message_id, prompt } => Some((message_id, prompt)),
            _ => None,
        })
        .collect()
}

// The question is put and the run waits; the decider is not asked again
// until the answer comes. Resumed with the answer, the run goes on.
#[test]
fn a_question_waits_for_the_users_answer_and_the_answer_answers_it() {
    let spec = spec(&["user.ask"]);
    let mut driver = Driver::boot(spec.clone()).unwrap().with_user_questions();
    run(&mut driver, vec![ask(), complete()]);
    let questions = asked(&driver);
    assert_eq!(questions.len(), 1);
    let (question, prompt) = questions[0].clone();
    assert_eq!(prompt, "Which airport?");
    assert_eq!(
        driver.state().harness,
        HarnessState::WaitingForMessage {
            step: 1,
            attempt: 0,
            message_id: question,
        }
    );

    // Resumed with no answer, it still waits and asks nothing again.
    let mut log = driver.events().to_vec();
    let mut resumed = Driver::resume(spec.clone(), log.clone())
        .unwrap()
        .with_user_questions();
    run(&mut resumed, vec![ask(), complete()]);
    assert_eq!(asked(&resumed).len(), 1);
    assert!(matches!(
        resumed.state().harness,
        HarnessState::WaitingForMessage { .. }
    ));

    // An answer to another question leaves it waiting; its own answers it.
    log.push(event(
        &spec,
        EventPayload::UserAnswered {
            message_id: MessageId::new(),
            text: "LAX".to_string(),
        },
    ));
    let mut other = Driver::resume(spec.clone(), log.clone())
        .unwrap()
        .with_user_questions();
    run(&mut other, vec![complete()]);
    assert!(matches!(
        other.state().harness,
        HarnessState::WaitingForMessage { .. }
    ));
    log.push(event(
        &spec,
        EventPayload::UserAnswered {
            message_id: question,
            text: "SFO".to_string(),
        },
    ));
    let mut answered = Driver::resume(spec, log).unwrap().with_user_questions();
    run(&mut answered, vec![complete()]);
    assert!(matches!(
        answered.state().harness,
        HarnessState::Completed { .. }
    ));
}

// A run that cannot wait (it runs inside its request) is refused, and goes
// on.
#[test]
fn a_question_from_a_run_that_cannot_wait_is_refused() {
    let mut driver = Driver::boot(spec(&["user.ask"])).unwrap();
    run(&mut driver, vec![ask(), complete()]);
    assert!(payloads(&driver).contains(&EventPayload::UserAskRefused {
        reason: "the run cannot wait for an answer".to_string(),
    }));
    assert_eq!(asked(&driver), Vec::new());
    assert!(matches!(
        driver.state().harness,
        HarnessState::Completed { .. }
    ));
}

// Without user.ask the question is denied (58A), and the run goes on.
#[test]
fn a_question_without_the_capability_is_denied() {
    let mut driver = Driver::boot(spec(&[])).unwrap().with_user_questions();
    run(&mut driver, vec![ask(), complete()]);
    assert!(payloads(&driver).iter().any(|payload| matches!(
        payload,
        EventPayload::EffectDenied { effect: Effect::AskUser { .. }, reason }
            if reason == "missing capability: user.ask"
    )));
    assert_eq!(asked(&driver), Vec::new());
}
