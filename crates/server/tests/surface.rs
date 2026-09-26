use protocol::{
    Actor, AgentId, CredentialSource, Effect, Event, EventPayload, EventSource, ExecutionPlacement,
    InvocationId, MessageRole, ModelMessage, ModelProvider, RunId, RunSpec, Timestamp, WorkModel,
};
use serde_json::Value;
use server::{
    accept_subscription_completion, ag_ui_events, json_render_spec, open_turn, GatewayCall,
    GatewayPoster, InMemoryStore, MemorySandbox,
};

fn event(run_id: RunId, payload: EventPayload) -> Event {
    Event::record(
        EventSource::new(
            run_id,
            AgentId::new(),
            "1",
            Actor::Agent,
            Timestamp::unix_millis(1),
        ),
        payload,
    )
}

#[test]
fn ag_ui_maps_tool_result_and_completion() {
    let run_id = RunId::new();
    let events = ag_ui_events(
        run_id,
        &[
            event(
                run_id,
                EventPayload::ToolResult {
                    name: "echo".to_string(),
                    invocation: protocol::InvocationId::new(),
                    step: 1,
                    attempt: 0,
                    output: "hello".to_string(),
                },
            ),
            event(
                run_id,
                EventPayload::ModelResponded {
                    message: ModelMessage {
                        role: MessageRole::Assistant,
                        text: "noted".to_string(),
                    },
                },
            ),
            event(
                run_id,
                EventPayload::RunCompleted {
                    outcome: "done".to_string(),
                },
            ),
        ],
    );
    let types: Vec<_> = events
        .iter()
        .filter_map(|event| event.get("type").and_then(|value| value.as_str()))
        .collect();
    assert_eq!(
        types,
        [
            "RUN_STARTED",
            "TOOL_CALL_START",
            "TOOL_CALL_ARGS",
            "TOOL_CALL_END",
            "TOOL_CALL_RESULT",
            "TEXT_MESSAGE_START",
            "TEXT_MESSAGE_CONTENT",
            "TEXT_MESSAGE_END",
            "TEXT_MESSAGE_START",
            "TEXT_MESSAGE_CONTENT",
            "TEXT_MESSAGE_END",
            "RUN_FINISHED",
        ]
    );
}

fn json_has_string(value: &Value, needle: &str) -> bool {
    match value {
        Value::String(text) => text == needle,
        Value::Array(items) => items.iter().any(|item| json_has_string(item, needle)),
        Value::Object(map) => map.values().any(|item| json_has_string(item, needle)),
        _ => false,
    }
}

#[test]
fn ag_ui_args_delta_is_the_authorized_input() {
    let run_id = RunId::new();
    let first = InvocationId::new();
    let second = InvocationId::new();
    let events = ag_ui_events(
        run_id,
        &[
            event(
                run_id,
                EventPayload::EffectAuthorized {
                    effect: Effect::ToolCall {
                        name: "ping".to_string(),
                        input: "hi".to_string(),
                        invocation: first,
                    },
                },
            ),
            event(
                run_id,
                EventPayload::ToolResult {
                    name: "ping".to_string(),
                    invocation: first,
                    step: 1,
                    attempt: 0,
                    output: "pong:hi".to_string(),
                },
            ),
            event(
                run_id,
                EventPayload::EffectAuthorized {
                    effect: Effect::ToolCall {
                        name: "ping".to_string(),
                        input: "be".to_string(),
                        invocation: second,
                    },
                },
            ),
            event(
                run_id,
                EventPayload::ToolResult {
                    name: "ping".to_string(),
                    invocation: second,
                    step: 1,
                    attempt: 0,
                    output: "pong:be".to_string(),
                },
            ),
        ],
    );
    let args: Vec<_> = events
        .iter()
        .filter(|event| event["type"] == "TOOL_CALL_ARGS")
        .collect();
    let results: Vec<_> = events
        .iter()
        .filter(|event| event["type"] == "TOOL_CALL_RESULT")
        .collect();
    assert_eq!(args.len(), 2);
    assert_eq!(args[0]["delta"], "hi");
    assert_eq!(args[1]["delta"], "be");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["content"], "pong:hi");
    assert_eq!(results[1]["content"], "pong:be");
    for args_event in &args {
        assert!(!json_has_string(args_event, "pong:hi"));
        assert!(!json_has_string(args_event, "pong:be"));
    }
    for event in &events {
        let delta = event.get("delta").and_then(Value::as_str);
        assert_ne!(delta, Some("pong:hi"));
        assert_ne!(delta, Some("pong:be"));
    }

    let orphan = ag_ui_events(
        run_id,
        &[event(
            run_id,
            EventPayload::ToolResult {
                name: "ping".to_string(),
                invocation: InvocationId::new(),
                step: 1,
                attempt: 0,
                output: "pong:hi".to_string(),
            },
        )],
    );
    let orphan_args = orphan
        .iter()
        .find(|event| event["type"] == "TOOL_CALL_ARGS")
        .expect("args");
    // AG-UI requires delta to be a string. With no authorization in the slice it is empty.
    assert_eq!(orphan_args["delta"], "");
    assert!(!json_has_string(orphan_args, "pong:hi"));
    assert_eq!(
        orphan
            .iter()
            .find(|event| event["type"] == "TOOL_CALL_RESULT")
            .expect("result")["content"],
        "pong:hi"
    );
}

#[test]
fn json_render_spec_names_the_outcome() {
    let spec = json_render_spec("hello", "done");
    assert_eq!(spec["root"], "screen");
    assert_eq!(spec["elements"]["screen"]["children"][2], "outcome");
    assert_eq!(spec["elements"]["outcome"]["props"]["text"], "done");
    assert_eq!(spec["elements"]["input"]["props"]["text"], "hello");
}

struct NoPoster;

impl GatewayPoster for NoPoster {
    fn complete(&self, _call: &GatewayCall) -> Result<String, String> {
        Err("subscription must not post".to_string())
    }
}

fn texts(events: &[Value]) -> Vec<&str> {
    events
        .iter()
        .filter(|event| event["type"] == "TEXT_MESSAGE_CONTENT")
        .filter_map(|event| event["delta"].as_str())
        .collect()
}

fn types(events: &[Value]) -> Vec<&str> {
    events
        .iter()
        .filter_map(|event| event["type"].as_str())
        .collect()
}

fn reply(run_id: RunId, role: MessageRole, text: &str) -> Event {
    event(
        run_id,
        EventPayload::ModelResponded {
            message: ModelMessage {
                role,
                text: text.to_string(),
            },
        },
    )
}

fn completed(run_id: RunId, outcome: &str) -> Event {
    event(
        run_id,
        EventPayload::RunCompleted {
            outcome: outcome.to_string(),
        },
    )
}

#[test]
fn coworker_text_emitted_once() {
    let spec = RunSpec::builder()
        .agent(AgentId::new(), "1")
        .input("hello from the desktop")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::Anthropic,
            model_name: "claude-fixture".to_string(),
            credential: CredentialSource::BringYourOwn {
                secret_ref: "desktop-subscription".to_string(),
            },
        })
        .build();
    let store = InMemoryStore::default();
    let sandbox = MemorySandbox::default();
    open_turn(&store, spec.clone(), &NoPoster, &sandbox).expect("open");
    let turn = accept_subscription_completion(&store, spec.run_id, "fixture reply", &sandbox)
        .expect("completion");
    let reply_id = turn
        .events
        .iter()
        .find(|event| matches!(event.payload, EventPayload::ModelResponded { .. }))
        .expect("reply")
        .envelope
        .event_id
        .to_string();

    let events = ag_ui_events(spec.run_id, &turn.events);
    assert_eq!(
        types(&events),
        [
            "RUN_STARTED",
            "TEXT_MESSAGE_START",
            "TEXT_MESSAGE_CONTENT",
            "TEXT_MESSAGE_END",
            "TEXT_MESSAGE_START",
            "TEXT_MESSAGE_CONTENT",
            "TEXT_MESSAGE_END",
            "RUN_FINISHED",
        ]
    );
    assert_eq!(texts(&events), ["hello from the desktop", "fixture reply"]);
    // The reply's own message survives; the outcome message is the one dropped.
    assert_eq!(events[4]["messageId"], Value::String(reply_id));
    assert_eq!(events[4]["role"], "assistant");
}

#[test]
fn harness_reply_then_complete_emitted_once() {
    // The Driver's log: the reply, then Complete decided and authorized, then RunCompleted.
    let run_id = RunId::new();
    let complete = Effect::Complete {
        outcome: "done".to_string(),
    };
    let events = ag_ui_events(
        run_id,
        &[
            reply(run_id, MessageRole::Assistant, "done"),
            event(
                run_id,
                EventPayload::EffectDecided {
                    effect: complete.clone(),
                },
            ),
            event(run_id, EventPayload::EffectAuthorized { effect: complete }),
            completed(run_id, "done"),
        ],
    );
    assert_eq!(
        types(&events),
        [
            "RUN_STARTED",
            "TEXT_MESSAGE_START",
            "TEXT_MESSAGE_CONTENT",
            "TEXT_MESSAGE_END",
            "RUN_FINISHED",
        ]
    );
    assert_eq!(texts(&events), ["done"]);
}

#[test]
fn an_empty_reply_between_keeps_the_outcome_dropped() {
    let run_id = RunId::new();
    let events = ag_ui_events(
        run_id,
        &[
            reply(run_id, MessageRole::Assistant, "done"),
            reply(run_id, MessageRole::Assistant, ""),
            completed(run_id, "done"),
        ],
    );
    assert_eq!(texts(&events), ["done"]);
}

#[test]
fn an_outcome_that_does_not_repeat_the_last_assistant_reply_is_emitted() {
    let run_id = RunId::new();
    let cases = [
        vec![
            event(
                run_id,
                EventPayload::UserMessage {
                    text: "same".to_string(),
                },
            ),
            completed(run_id, "same"),
        ],
        vec![
            reply(run_id, MessageRole::User, "same"),
            completed(run_id, "same"),
        ],
        vec![
            reply(run_id, MessageRole::Assistant, "same"),
            reply(run_id, MessageRole::System, "note"),
            completed(run_id, "same"),
        ],
        vec![
            reply(run_id, MessageRole::Assistant, "same"),
            reply(run_id, MessageRole::Assistant, "other"),
            completed(run_id, "same"),
        ],
        vec![
            reply(run_id, MessageRole::Assistant, "same"),
            event(
                run_id,
                EventPayload::ToolResult {
                    name: "echo".to_string(),
                    invocation: InvocationId::new(),
                    step: 1,
                    attempt: 0,
                    output: "ok".to_string(),
                },
            ),
            completed(run_id, "same"),
        ],
    ];
    for (case, log) in cases.iter().enumerate() {
        let events = ag_ui_events(run_id, log);
        let outcome: Vec<_> = events
            .iter()
            .filter(|event| event["messageId"] == format!("outcome-{run_id}").as_str())
            .map(|event| event["type"].as_str().expect("type"))
            .collect();
        assert_eq!(
            outcome,
            [
                "TEXT_MESSAGE_START",
                "TEXT_MESSAGE_CONTENT",
                "TEXT_MESSAGE_END"
            ],
            "case {case}"
        );
        assert_eq!(texts(&events).last(), Some(&"same"), "case {case}");
    }
}

#[test]
fn an_empty_outcome_emits_no_text_message() {
    let run_id = RunId::new();
    let events = ag_ui_events(run_id, &[completed(run_id, "")]);
    assert_eq!(types(&events), ["RUN_STARTED", "RUN_FINISHED"]);
}

#[test]
fn run_expired_maps_to_run_error() {
    let run_id = RunId::new();
    let events = ag_ui_events(run_id, &[event(run_id, EventPayload::RunExpired)]);
    assert_eq!(events.len(), 2);
    assert_eq!(events[1]["type"], "RUN_ERROR");
    assert_eq!(events[1]["message"], "expired");
}

#[test]
fn tool_call_id_is_invocation_id() {
    let run_id = RunId::new();
    let invocation = InvocationId::new();
    let result = event(
        run_id,
        EventPayload::ToolResult {
            name: "ping".to_string(),
            invocation,
            step: 1,
            attempt: 0,
            output: "pong:hi".to_string(),
        },
    );
    let events = ag_ui_events(
        run_id,
        &[
            event(
                run_id,
                EventPayload::EffectAuthorized {
                    effect: Effect::ToolCall {
                        name: "ping".to_string(),
                        input: "hi".to_string(),
                        invocation,
                    },
                },
            ),
            result.clone(),
        ],
    );
    let ids: Vec<_> = events
        .iter()
        .filter_map(|event| event.get("toolCallId"))
        .collect();
    assert_eq!(ids.len(), 4);
    for id in ids {
        assert_eq!(id, &Value::String(invocation.to_string()));
    }
    let tool_result = events
        .iter()
        .find(|event| event["type"] == "TOOL_CALL_RESULT")
        .expect("result");
    assert_eq!(
        tool_result["messageId"],
        Value::String(format!("result-{}", result.envelope.event_id))
    );
}
