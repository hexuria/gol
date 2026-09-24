use protocol::{MessageRole, ModelMessage};
use serde_json::Value;

use crate::GatewayError;

pub fn chat_body(model_name: &str, prompt: &str) -> Value {
    serde_json::json!({
        "model": model_name,
        "messages": [{ "role": "user", "content": prompt }]
    })
}

pub fn parse_chat_completion(body: &Value) -> Result<ModelMessage, GatewayError> {
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
    Ok(ModelMessage { role, text })
}
