//! An effect the policy allows but the harness cannot act on in its current
//! state is denied with a reason, never authorized and silently dropped.
//! `protocol::applicable` is the rule; these tests drive it through the
//! public `Driver` the way the server does.

use harness::{Driver, EchoTool, ScriptedDecider, Tool};
use protocol::{
    AgentId, Capability, CredentialSource, Effect, EventPayload, ExecutionPlacement, HarnessState,
    InvocationId, Limits, ModelProvider, RunSpec, ToolDescriptor, WorkModel,
};

fn boot() -> Driver {
    let spec = RunSpec::builder()
        .agent(AgentId::new(), "1")
        .input("hi")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::Anthropic,
            model_name: "m".into(),
            credential: CredentialSource::PlatformGateway,
        })
        .capabilities(vec![
            Capability::new("tool.echo"),
            Capability::new("model.call"),
        ])
        .limits(Limits {
            max_steps: 5,
            max_model_calls: 4,
        })
        .build();
    Driver::boot(spec).unwrap()
}

fn descriptors() -> [ToolDescriptor; 1] {
    [EchoTool.descriptor()]
}

fn echo(invocation: InvocationId) -> Effect {
    Effect::ToolCall {
        name: "echo".into(),
        input: "x".into(),
        invocation,
    }
}

fn decide(driver: &mut Driver, effect: Effect) -> Vec<Effect> {
    driver
        .decide(&mut ScriptedDecider::new(vec![effect]), &descriptors())
        .unwrap()
}

/// The payloads the last decision appended, after its `EffectDecided`.
fn after_last_decision(driver: &Driver) -> Vec<EventPayload> {
    let events = driver.events();
    let decided = events
        .iter()
        .rposition(|event| matches!(event.payload, EventPayload::EffectDecided { .. }))
        .expect("a decision was made");
    events[decided + 1..]
        .iter()
        .map(|event| event.payload.clone())
        .collect()
}

fn denied_as_not_applicable(payloads: &[EventPayload], effect: &Effect, state: &str) {
    assert_eq!(
        payloads,
        [EventPayload::EffectDenied {
            effect: effect.clone(),
            reason: format!("not applicable while {state}"),
        }],
    );
}

fn wait_for_echo(driver: &mut Driver) -> InvocationId {
    let invocation = InvocationId::new();
    assert_eq!(decide(driver, echo(invocation)), [echo(invocation)]);
    assert!(matches!(
        driver.state().harness,
        HarnessState::WaitingForTool { .. }
    ));
    invocation
}

#[test]
fn tool_call_after_answer_is_denied_not_dropped() {
    let mut driver = boot();
    let first = wait_for_echo(&mut driver);
    assert!(driver.deliver_tool_result("echo", first, "x"));
    let before = driver.state().harness;
    assert!(matches!(
        before,
        HarnessState::Running { answered: true, .. }
    ));

    let second = echo(InvocationId::new());
    assert_eq!(decide(&mut driver, second.clone()), []);
    denied_as_not_applicable(&after_last_decision(&driver), &second, "running, answered");
    assert_eq!(driver.state().harness, before);
    assert_eq!(driver.state().steps, 2);
}

#[test]
fn a_complete_while_waiting_for_a_tool_is_denied() {
    let mut driver = boot();
    wait_for_echo(&mut driver);
    let before = driver.state().harness;

    let complete = Effect::Complete {
        outcome: "done".into(),
    };
    assert_eq!(decide(&mut driver, complete.clone()), []);
    denied_as_not_applicable(
        &after_last_decision(&driver),
        &complete,
        "waiting for a tool",
    );
    assert_eq!(driver.state().harness, before);
    // It cannot finish the run, so it is charged a step (A2).
    assert_eq!(driver.state().steps, 2);
}

#[test]
fn a_model_call_while_waiting_for_a_tool_is_denied() {
    let mut driver = boot();
    wait_for_echo(&mut driver);
    let before = driver.state().harness;

    let model = Effect::ModelCall { prompt: "p".into() };
    assert_eq!(decide(&mut driver, model.clone()), []);
    denied_as_not_applicable(&after_last_decision(&driver), &model, "waiting for a tool");
    assert_eq!(driver.state().harness, before);
    // A denied model call is not a model call.
    assert_eq!(driver.state().model_calls, 0);
}

#[test]
fn applicable_effects_are_authorized() {
    let mut driver = boot();

    let model = Effect::ModelCall { prompt: "p".into() };
    assert_eq!(
        decide(&mut driver, model.clone()),
        std::slice::from_ref(&model)
    );
    assert_eq!(
        after_last_decision(&driver),
        [EventPayload::EffectAuthorized { effect: model }],
    );

    let invocation = InvocationId::new();
    assert_eq!(decide(&mut driver, echo(invocation)), [echo(invocation)]);
    assert_eq!(
        after_last_decision(&driver),
        [EventPayload::EffectAuthorized {
            effect: echo(invocation)
        }],
    );
    assert!(driver.deliver_tool_result("echo", invocation, "x"));

    let complete = Effect::Complete {
        outcome: "done".into(),
    };
    assert_eq!(decide(&mut driver, complete.clone()), []);
    assert_eq!(
        after_last_decision(&driver),
        [
            EventPayload::EffectAuthorized { effect: complete },
            EventPayload::RunCompleted {
                outcome: "done".into()
            },
        ],
    );
}
