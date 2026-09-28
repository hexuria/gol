//! D1: `ModelResponded` carries the tokens a call used, when its provider
//! reported them. Logs from before D1 have no usage and still load.
use protocol::{EventPayload, MessageRole, ModelMessage, Usage};
use serde_json::json;

#[test]
fn a_model_response_without_usage_still_loads() {
    let payload: EventPayload = serde_json::from_value(json!({
        "ModelResponded": {"message": {"role": "Assistant", "text": "hi"}}
    }))
    .expect("an old model response loads");
    assert_eq!(
        payload,
        EventPayload::ModelResponded {
            message: ModelMessage {
                role: MessageRole::Assistant,
                text: "hi".to_string(),
            },
            usage: None,
        }
    );
}

#[test]
fn a_model_responses_usage_round_trips() {
    let payload = EventPayload::ModelResponded {
        message: ModelMessage {
            role: MessageRole::Assistant,
            text: "hi".to_string(),
        },
        usage: Some(Usage {
            input_tokens: 12,
            output_tokens: 3,
        }),
    };
    let value = serde_json::to_value(&payload).expect("serialize");
    assert_eq!(
        serde_json::from_value::<EventPayload>(value).expect("deserialize"),
        payload
    );
    let bare = EventPayload::ModelResponded {
        message: ModelMessage {
            role: MessageRole::Assistant,
            text: "hi".to_string(),
        },
        usage: None,
    };
    let text = serde_json::to_string(&bare).expect("serialize");
    assert!(!text.contains("usage"), "{text}");
}
