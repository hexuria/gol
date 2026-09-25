use std::collections::HashMap;

use protocol::{Effect, Event, EventPayload, InvocationId, MessageRole, RunId};
use serde_json::{json, Value};

pub fn ag_ui_events(run_id: RunId, events: &[Event]) -> Vec<Value> {
    let run = run_id.to_string();
    let mut out = vec![json!({
        "type": "RUN_STARTED",
        "threadId": run,
        "runId": run,
    })];
    // One pass: the authorized input for each tool invocation.
    let inputs: HashMap<InvocationId, &str> = events
        .iter()
        .filter_map(|event| match &event.payload {
            EventPayload::EffectAuthorized {
                effect:
                    Effect::ToolCall {
                        input, invocation, ..
                    },
            } => Some((*invocation, input.as_str())),
            _ => None,
        })
        .collect();
    for event in events {
        let id = event.envelope.event_id.to_string();
        match &event.payload {
            EventPayload::ToolResult {
                name,
                output,
                invocation,
                ..
            } => {
                let delta = inputs.get(invocation).copied().unwrap_or("");
                out.push(json!({
                    "type": "TOOL_CALL_START",
                    "toolCallId": id,
                    "toolCallName": name,
                }));
                out.push(json!({
                    "type": "TOOL_CALL_ARGS",
                    "toolCallId": id,
                    "delta": delta,
                }));
                out.push(json!({
                    "type": "TOOL_CALL_END",
                    "toolCallId": id,
                }));
                out.push(json!({
                    "type": "TOOL_CALL_RESULT",
                    "messageId": format!("result-{id}"),
                    "toolCallId": id,
                    "content": output,
                }));
            }
            EventPayload::UserMessage { text } if !text.is_empty() => {
                out.push(json!({"type": "TEXT_MESSAGE_START", "messageId": id, "role": "user"}));
                out.push(json!({"type": "TEXT_MESSAGE_CONTENT", "messageId": id, "delta": text}));
                out.push(json!({"type": "TEXT_MESSAGE_END", "messageId": id}));
            }
            EventPayload::ModelResponded { message } if !message.text.is_empty() => {
                let role = match message.role {
                    MessageRole::Assistant => "assistant",
                    MessageRole::User => "user",
                    MessageRole::System => "system",
                };
                out.push(json!({"type": "TEXT_MESSAGE_START", "messageId": id, "role": role}));
                out.push(
                    json!({"type": "TEXT_MESSAGE_CONTENT", "messageId": id, "delta": message.text}),
                );
                out.push(json!({"type": "TEXT_MESSAGE_END", "messageId": id}));
            }
            EventPayload::RunCompleted { outcome } => {
                let message_id = format!("outcome-{run}");
                out.push(json!({"type": "TEXT_MESSAGE_START", "messageId": message_id, "role": "assistant"}));
                out.push(json!({"type": "TEXT_MESSAGE_CONTENT", "messageId": message_id, "delta": outcome}));
                out.push(json!({"type": "TEXT_MESSAGE_END", "messageId": message_id}));
                out.push(json!({"type": "RUN_FINISHED", "threadId": run, "runId": run}));
            }
            EventPayload::RunFailed { message, .. } => {
                out.push(json!({"type": "RUN_ERROR", "message": message}));
            }
            EventPayload::RunCancelled => {
                out.push(json!({"type": "RUN_ERROR", "message": "cancelled"}));
            }
            _ => {}
        }
    }
    out
}

pub fn json_render_spec(input: &str, outcome: &str) -> Value {
    json!({
        "root": "screen",
        "elements": {
            "screen": {
                "type": "Stack",
                "props": { "direction": "vertical", "gap": 8 },
                "children": ["title", "input", "outcome"]
            },
            "title": { "type": "Text", "props": { "text": "gol" } },
            "input": { "type": "Text", "props": { "text": input } },
            "outcome": { "type": "Text", "props": { "text": outcome } }
        }
    })
}
