use protocol::{MessageRole, ModelMessage, ModelProvider, Usage};
use serde_json::Value;

use crate::openai::{chat_body, parse_chat_completion};
use crate::GatewayError;

/// A provider's answer: the message, and the tokens the call used when the
/// provider reported them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Completion {
    pub message: ModelMessage,
    pub usage: Option<Usage>,
}

/// Parses `provider`'s response body. Bodies come from the network, so any
/// JSON is refused with `Malformed` rather than panicking.
pub fn parse(provider: ModelProvider, body: &Value) -> Result<Completion, GatewayError> {
    match provider {
        ModelProvider::OpenAI | ModelProvider::SystemOne => parse_chat_completion(body),
        ModelProvider::Anthropic => parse_anthropic(body),
        ModelProvider::Gemini => parse_gemini(body),
    }
}

/// `input` and `output` from `counts`, when both are unsigned integers.
pub(crate) fn tokens(counts: &Value, input: &str, output: &str) -> Option<Usage> {
    Some(Usage {
        input_tokens: counts.get(input)?.as_u64()?,
        output_tokens: counts.get(output)?.as_u64()?,
    })
}

pub fn anthropic_body(model_name: &str, prompt: &str) -> Value {
    serde_json::json!({
        "model": model_name,
        "max_tokens": 1024,
        "messages": [{ "role": "user", "content": prompt }]
    })
}

fn parse_anthropic(body: &Value) -> Result<Completion, GatewayError> {
    let text = body
        .get("content")
        .and_then(|content| content.get(0))
        .and_then(|block| block.get("text"))
        .and_then(Value::as_str)
        .ok_or_else(|| GatewayError::Malformed("missing content[0].text".to_string()))?;
    Ok(Completion {
        message: ModelMessage {
            role: MessageRole::Assistant,
            text: text.to_string(),
        },
        usage: body
            .get("usage")
            .and_then(|usage| tokens(usage, "input_tokens", "output_tokens")),
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

fn parse_gemini(body: &Value) -> Result<Completion, GatewayError> {
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
    Ok(Completion {
        message: ModelMessage {
            role: MessageRole::Assistant,
            text: text.to_string(),
        },
        usage: body
            .get("usageMetadata")
            .and_then(|usage| tokens(usage, "promptTokenCount", "candidatesTokenCount")),
    })
}

pub fn system_one_body(model_name: &str, prompt: &str) -> Value {
    chat_body(model_name, prompt)
}
