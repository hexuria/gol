use protocol::{MessageRole, ModelMessage, Usage};
use serde_json::Value;

use crate::providers::tokens;
use crate::{Completion, GatewayError};

pub fn chat_body(model_name: &str, prompt: &str) -> Value {
    serde_json::json!({
        "model": model_name,
        "messages": [{ "role": "user", "content": prompt }]
    })
}

pub fn parse_chat_completion(body: &Value) -> Result<Completion, GatewayError> {
    let message = body
        .get("choices")
        .and_then(|choices| choices.get(0))
        .and_then(|choice| choice.get("message"))
        .ok_or_else(|| GatewayError::Malformed("missing choices[0].message".to_string()))?;
    let text = message
        .get("content")
        .and_then(Value::as_str)
        .ok_or_else(|| GatewayError::Malformed("missing message content".to_string()))?
        .to_string();
    let role = match message.get("role").and_then(Value::as_str) {
        Some("assistant") => MessageRole::Assistant,
        Some("system") => MessageRole::System,
        Some("user") => MessageRole::User,
        _ => return Err(GatewayError::Malformed("missing message role".to_string())),
    };
    Ok(Completion {
        message: ModelMessage { role, text },
        usage: chat_usage(body),
    })
}

/// `usage.prompt_tokens` and `usage.completion_tokens`, when both are there.
fn chat_usage(body: &Value) -> Option<Usage> {
    tokens(body.get("usage")?, "prompt_tokens", "completion_tokens")
}
