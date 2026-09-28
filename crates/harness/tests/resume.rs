//! Resumable runs (Phase 1.5a). `Driver::resume` rebuilds a driver from a
//! stored log; `run_until` goes on from it step by step and stops at a
//! terminal event or at a stop request, read at each step boundary. A log cut
//! after an effect was authorized and before its result performs that effect
//! again, a tool with the same invocation (decision 1.5a-3A).
use harness::{
    run_to_completion, run_until, Driver, EchoTool, InMemory, ModelCompletion, ScriptedDecider,
    Tool,
};
use proptest::prelude::*;
use protocol::{
    fold, AgentId, Capability, CredentialSource, Effect, Event, EventPayload, ExecutionPlacement,
    InvocationId, Limits, MessageRole, ModelMessage, ModelProvider, ModelRequest, Owner, RunSpec,
    WorkModel,
};

fn spec(limits: Limits) -> RunSpec {
    RunSpec::builder()
        .owner(Owner::new("https://issuer.test", "user-1", "tenant-1"))
        .agent(AgentId::new(), "1")
        .input("hello")
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
        .limits(limits)
        .build()
}

fn roomy() -> RunSpec {
    spec(Limits {
        max_steps: 8,
        max_model_calls: 4,
    })
}

/// A model that answers every prompt with its own text, so a repeated call
/// records the same answer.
struct Echoing;

impl ModelCompletion for Echoing {
    fn complete(&self, request: &ModelRequest) -> Result<ModelMessage, String> {
        Ok(ModelMessage {
            role: MessageRole::Assistant,
            text: format!("re: {}", request.prompt),
        })
    }
}

/// Counts the calls it answers.
#[derive(Default)]
struct Counting {
    calls: std::cell::Cell<u32>,
}

impl Tool for Counting {
    fn descriptor(&self) -> protocol::ToolDescriptor {
        EchoTool::descriptor()
    }
    fn call(&self, input: &str) -> Result<String, String> {
        self.calls.set(self.calls.get() + 1);
        Ok(input.to_string())
    }
}

fn tool_call(n: u128) -> Effect {
    Effect::ToolCall {
        name: "echo".into(),
        input: format!("call {n}"),
        invocation: InvocationId::from_uuid(uuid::Uuid::from_u128(n)),
    }
}

fn model_call() -> Effect {
    Effect::ModelCall {
        prompt: "hello".into(),
    }
}

fn complete() -> Effect {
    Effect::Complete {
        outcome: "done".into(),
    }
}

fn payloads(events: &[Event]) -> Vec<EventPayload> {
    events.iter().map(|event| event.payload.clone()).collect()
}

fn decisions(events: &[Event]) -> usize {
    events
        .iter()
        .filter(|event| matches!(event.payload, EventPayload::EffectDecided { .. }))
        .count()
}

/// Runs `script` from a fresh boot to its end.
fn uninterrupted(spec: &RunSpec, script: &[Effect]) -> Vec<Event> {
    let mut driver = Driver::boot(spec.clone()).unwrap();
    let mut decider = ScriptedDecider::new(script.to_vec());
    let echo = EchoTool;
    let _ = run_to_completion(
        &mut driver,
        &mut decider,
        &[&echo],
        &Echoing,
        &InMemory::default(),
    );
    driver.events().to_vec()
}

/// Resumes from `log`, and goes on with the decisions `log` has not taken.
fn resumed(spec: &RunSpec, script: &[Effect], log: &[Event]) -> Vec<Event> {
    let mut driver = Driver::resume(spec.clone(), log.to_vec()).unwrap();
    let taken = decisions(log).min(script.len());
    let mut decider = ScriptedDecider::new(script[taken..].to_vec());
    let echo = EchoTool;
    let _ = run_until(
        &mut driver,
        &mut decider,
        &[&echo],
        &Echoing,
        &InMemory::default(),
        &|| false,
    );
    driver.events().to_vec()
}

/// A tool the run holds no capability for: the policy denies it.
fn unheld_tool() -> Effect {
    Effect::ToolCall {
        name: "nope".into(),
        input: "x".into(),
        invocation: InvocationId::from_uuid(uuid::Uuid::from_u128(99)),
    }
}

/// A delegation without `agent.delegate`: the policy denies it.
fn delegation() -> Effect {
    Effect::Delegate {
        agent_id: AgentId::from_uuid(uuid::Uuid::from_u128(5)),
        input: "draft".into(),
    }
}

fn effect() -> impl Strategy<Value = Effect> {
    prop_oneof![
        (1u128..6).prop_map(tool_call),
        Just(model_call()),
        Just(complete()),
        Just(unheld_tool()),
        Just(delegation()),
    ]
}

// proptest's default of 256 cases.
proptest! {
    // A resumed driver holds its log and the harness state of its fold. The
    // one event it adds is the RunCompleted of a harness that had completed.
    #[test]
    fn resume_equals_the_fold_of_every_prefix(
        script in prop::collection::vec(effect(), 0..8),
        max_steps in 0u32..6,
        max_model_calls in 0u32..3,
    ) {
        let spec = spec(Limits { max_steps, max_model_calls });
        let log = uninterrupted(&spec, &script);
        for cut in 1..=log.len() {
            let prefix = &log[..cut];
            let driver = Driver::resume(spec.clone(), prefix.to_vec()).unwrap();
            let events = payloads(driver.events());
            let expected = payloads(prefix);
            prop_assert_eq!(&events[..cut], expected.as_slice());
            prop_assert_eq!(driver.state().harness, fold(&spec, prefix).harness);
            match &events[cut..] {
                [] => {}
                [EventPayload::RunCompleted { .. }] => {
                    let completed = matches!(
                        fold(&spec, prefix).harness,
                        protocol::HarnessState::Completed { .. }
                    );
                    prop_assert!(completed);
                }
                extra => prop_assert!(false, "resume added {:?}", extra),
            }
        }
    }

    // A run cut anywhere and resumed with the same decider, model and tools
    // records what the uninterrupted run recorded: an effect cut after its
    // authorization is performed again and its result recorded once.
    #[test]
    fn a_resumed_run_ends_like_an_uninterrupted_one(
        script in prop::collection::vec(effect(), 0..8),
        max_steps in 0u32..6,
        max_model_calls in 0u32..3,
    ) {
        let spec = spec(Limits { max_steps, max_model_calls });
        let log = uninterrupted(&spec, &script);
        for cut in 1..=log.len() {
            prop_assert_eq!(
                payloads(&resumed(&spec, &script, &log[..cut])),
                payloads(&log),
                "cut at {}", cut
            );
        }
    }
}

// A run's log from before its harness started (as the queue stores it) gets
// its RunStarted on resume; a started run does not get a second one.
#[test]
fn resume_does_not_restart_a_started_run() {
    let spec = roomy();
    let log = uninterrupted(&spec, &[tool_call(1), complete()]);
    let started = |events: &[Event]| {
        events
            .iter()
            .filter(|event| matches!(event.payload, EventPayload::RunStarted))
            .count()
    };
    let cut = &log[..3];
    let driver = Driver::resume(spec.clone(), cut.to_vec()).unwrap();
    assert_eq!(started(driver.events()), 1);
    let fresh = Driver::resume(spec, Vec::new()).unwrap();
    assert_eq!(payloads(fresh.events()), [EventPayload::RunStarted]);
}

#[test]
fn a_stop_request_ends_the_run_at_the_next_step_boundary() {
    let spec = roomy();
    let mut driver = Driver::boot(spec).unwrap();
    let mut decider = ScriptedDecider::new([tool_call(1), tool_call(2), complete()]);
    let echo = EchoTool;
    let asked = std::cell::Cell::new(0);
    // Stop once the first step is done.
    let should_stop = || {
        asked.set(asked.get() + 1);
        asked.get() > 1
    };
    run_until(
        &mut driver,
        &mut decider,
        &[&echo],
        &Echoing,
        &InMemory::default(),
        &should_stop,
    )
    .unwrap();
    let events = payloads(driver.events());
    assert_eq!(events.last(), Some(&EventPayload::RunCancelled));
    assert_eq!(decisions(driver.events()), 1);
    assert_eq!(
        events
            .iter()
            .filter(|payload| matches!(payload, EventPayload::ToolResult { .. }))
            .count(),
        1
    );
    assert!(driver.state().harness.is_terminal());
}

// Killed after the tool call was authorized, before its result was recorded:
// the resumed run calls the tool again with the same invocation.
#[test]
fn a_log_ending_mid_tool_call_calls_the_tool_again() {
    let spec = roomy();
    let log = uninterrupted(&spec, &[tool_call(7), complete()]);
    let authorized = log
        .iter()
        .position(|event| {
            matches!(
                &event.payload,
                EventPayload::EffectAuthorized {
                    effect: Effect::ToolCall { .. }
                }
            )
        })
        .unwrap();
    let mut driver = Driver::resume(spec, log[..=authorized].to_vec()).unwrap();
    let tool = Counting::default();
    let mut decider = ScriptedDecider::new([complete()]);
    run_until(
        &mut driver,
        &mut decider,
        &[&tool],
        &Echoing,
        &InMemory::default(),
        &|| false,
    )
    .unwrap();
    assert_eq!(tool.calls.get(), 1);
    let results: Vec<&EventPayload> = driver
        .events()
        .iter()
        .map(|event| &event.payload)
        .filter(|payload| matches!(payload, EventPayload::ToolResult { .. }))
        .collect();
    assert!(matches!(
        results.as_slice(),
        [EventPayload::ToolResult { invocation, output, .. }]
            if invocation == &InvocationId::from_uuid(uuid::Uuid::from_u128(7))
                && output == "call 7"
    ));
    assert!(matches!(
        driver.events().last().map(|event| &event.payload),
        Some(EventPayload::RunCompleted { .. })
    ));
}

// Cut between the authorized Complete and its RunCompleted: the harness has
// finished, so resume records the RunCompleted that ends the log.
#[test]
fn a_log_cut_before_run_completed_is_completed_on_resume() {
    let spec = roomy();
    let log = uninterrupted(&spec, &[complete()]);
    assert!(matches!(
        log.last().map(|event| &event.payload),
        Some(EventPayload::RunCompleted { .. })
    ));
    let driver = Driver::resume(spec, log[..log.len() - 1].to_vec()).unwrap();
    assert_eq!(payloads(driver.events()), payloads(&log));
}

#[test]
fn a_terminal_log_resumes_to_nothing() {
    let spec = roomy();
    let log = uninterrupted(&spec, &[model_call(), complete()]);
    let mut driver = Driver::resume(spec, log.clone()).unwrap();
    let mut decider = ScriptedDecider::new([tool_call(1)]);
    let echo = EchoTool;
    run_until(
        &mut driver,
        &mut decider,
        &[&echo],
        &Echoing,
        &InMemory::default(),
        &|| true,
    )
    .unwrap();
    assert_eq!(payloads(driver.events()), payloads(&log));
}
