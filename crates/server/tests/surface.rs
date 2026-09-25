use protocol::{
    Actor, AgentId, Effect, Event, EventPayload, EventSource, InvocationId, MessageRole,
    ModelMessage, RunId, Timestamp,
};
use serde_json::Value;
use server::{ag_ui_events, json_render_spec};

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
