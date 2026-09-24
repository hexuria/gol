use protocol::{Actor, AgentId, Event, EventPayload, MessageRole, ModelMessage, RunId, Timestamp};
use server::{ag_ui_events, json_render_spec};

fn event(run_id: RunId, payload: EventPayload) -> Event {
    Event::record(
        run_id,
        AgentId::new(),
        "1",
        None,
        Actor::Agent,
        None,
        Timestamp::unix_millis(1),
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

#[test]
fn json_render_spec_names_the_outcome() {
    let spec = json_render_spec("hello", "done");
    assert_eq!(spec["root"], "screen");
    assert_eq!(spec["elements"]["screen"]["children"][2], "outcome");
    assert_eq!(spec["elements"]["outcome"]["props"]["text"], "done");
    assert_eq!(spec["elements"]["input"]["props"]["text"], "hello");
}
