use protocol::{MessageRole, ModelMessage};
use serde_json::Value;

use crate::openai::{chat_body, parse_chat_completion};
use crate::GatewayError;

pub fn anthropic_body(model_name: &str, prompt: &str) -> Value {
    serde_json::json!({
        "model": model_name,
        "max_tokens": 1024,
        "messages": [{ "role": "user", "content": prompt }]
    })
}

pub fn parse_anthropic(body: &Value) -> Result<ModelMessage, GatewayError> {
    let text = body
        .get("content")
        .and_then(|content| content.get(0))
        .and_then(|block| block.get("text"))
        .and_then(Value::as_str)
        .ok_or_else(|| GatewayError::Malformed("missing content[0].text".to_string()))?;
    Ok(ModelMessage {
        role: MessageRole::Assistant,
        text: text.to_string(),
    })
}

pub fn gemini_body(prompt: &str) -> Value {
    serde_json::json!({
        "contents": [{
            "role": "user",
            "parts": [{ "text": prompt }]
        }]
    })
}

pub fn parse_gemini(body: &Value) -> Result<ModelMessage, GatewayError> {
    let text = body
        .get("candidates")
        .and_then(|candidates| candidates.get(0))
        .and_then(|candidate| candidate.get("content"))
        .and_then(|content| content.get("parts"))
        .and_then(|parts| parts.get(0))
        .and_then(|part| part.get("text"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            GatewayError::Malformed("missing candidates[0].content.parts[0].text".to_string())
        })?;
    Ok(ModelMessage {
        role: MessageRole::Assistant,
        text: text.to_string(),
    })
}

pub fn system_one_body(model_name: &str, prompt: &str) -> Value {
    chat_body(model_name, prompt)
}

pub fn parse_system_one(body: &Value) -> Result<ModelMessage, GatewayError> {
    parse_chat_completion(body)
}
