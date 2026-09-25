#![forbid(unsafe_code)]
//! Local fixture proxy. It answers Claude Messages, Codex Responses, Grok
//! cli-chat-proxy Responses, and the gol platform gateway. It does not call
//! Anthropic, OpenAI, xAI, or any other vendor.

use axum::body::Body;
use axum::extract::Request;
use axum::http::{header::CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

pub const CLAUDE_TEXT: &str = "fixture assistant text";
pub const CODEX_TEXT: &str = "fixture codex text";
pub const GROK_TEXT: &str = "fixture grok text";
pub const GATEWAY_TEXT: &str = "fixture gateway completion";

const PASSTHROUGH: &[&str] = &[
    "anthropic-version",
    "anthropic-beta",
    "x-claude-code-session-id",
];

pub fn router() -> Router {
    Router::new()
        .route("/v1/messages", post(claude_messages))
        .route("/v1/messages/count_tokens", post(claude_count_tokens))
        .route("/v1/models", get(claude_models))
        .route("/v1/responses", post(grok_responses))
        .route("/v1/chat/completions", post(grok_responses))
        .route("/v1/gateway/complete", post(gateway_complete))
        .fallback(fallback)
}

async fn fallback(request: Request) -> Response {
    if request.method() == Method::POST && is_codex_responses(request.uri().path()) {
        return codex_responses(request).await;
    }
    (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response()
}

fn is_codex_responses(path: &str) -> bool {
    path.trim_end_matches('/')
        .ends_with("/backend-api/codex/responses")
}

async fn claude_messages(request: Request) -> Response {
    let (headers, body) = parts(request).await;
    if !claude_authorized(&headers) {
        return unauthorized();
    }
    if wants_stream(&headers, &body) {
        return with_passthrough(sse(CLAUDE_TEXT), &headers);
    }
    with_passthrough(
        Json(json!({
            "id": "msg_fixture",
            "type": "message",
            "role": "assistant",
            "model": body.get("model").and_then(Value::as_str).unwrap_or("fixture"),
            "stop_reason": "end_turn",
            "content": [{"type": "text", "text": CLAUDE_TEXT}],
            "usage": {"input_tokens": 1, "output_tokens": 1}
        }))
        .into_response(),
        &headers,
    )
}

async fn claude_count_tokens(request: Request) -> Response {
    let (headers, _) = parts(request).await;
    if !claude_authorized(&headers) {
        return unauthorized();
    }
    with_passthrough(Json(json!({"input_tokens": 1})).into_response(), &headers)
}

async fn claude_models(headers: HeaderMap) -> Response {
    if !claude_authorized(&headers) {
        return unauthorized();
    }
    with_passthrough(
        Json(json!({
            "data": [{"id": "fixture", "type": "model"}]
        }))
        .into_response(),
        &headers,
    )
}

async fn codex_responses(request: Request) -> Response {
    let (headers, body) = parts(request).await;
    if bearer(&headers).is_none() || header_value(&headers, "chatgpt-account-id").is_none() {
        return unauthorized();
    }
    let text = CODEX_TEXT;
    if wants_stream(&headers, &body) {
        return sse(text);
    }
    Json(response_body("resp_codex_fixture", "codex-fixture", text)).into_response()
}

async fn grok_responses(request: Request) -> Response {
    let (headers, body) = parts(request).await;
    if bearer(&headers).is_none()
        || header_value(&headers, "x-xai-token-auth").as_deref() != Some("xai-grok-cli")
    {
        return unauthorized();
    }
    let model = header_value(&headers, "x-grok-model-override")
        .unwrap_or_else(|| "grok-fixture".to_string());
    if wants_stream(&headers, &body) {
        return sse(GROK_TEXT);
    }
    Json(response_body("resp_grok_fixture", &model, GROK_TEXT)).into_response()
}

async fn gateway_complete(request: Request) -> Response {
    let (headers, body) = parts(request).await;
    if bearer(&headers).is_none() {
        return unauthorized();
    }
    Json(json!({
        "id": "gw_fixture",
        "object": "gol.gateway.completion",
        "role": "assistant",
        "model": body.get("model").and_then(Value::as_str).unwrap_or("gateway-fixture"),
        "text": GATEWAY_TEXT
    }))
    .into_response()
}

fn response_body(id: &str, model: &str, text: &str) -> Value {
    json!({
        "id": id,
        "object": "response",
        "status": "completed",
        "model": model,
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": text}]
        }]
    })
}

async fn parts(request: Request) -> (HeaderMap, Value) {
    let (parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, 1024 * 1024)
        .await
        .unwrap_or_default();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (parts.headers, json)
}

fn claude_authorized(headers: &HeaderMap) -> bool {
    const FIXTURE: &str = "gol-desktop-fixture";
    header_value(headers, "x-api-key").as_deref() == Some(FIXTURE)
        || bearer(headers).as_deref() == Some(FIXTURE)
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    let value = header_value(headers, "authorization")?;
    let token = value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))?;
    let token = token.trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let value = headers.get(name)?.to_str().ok()?.trim().to_string();
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn wants_stream(headers: &HeaderMap, body: &Value) -> bool {
    body.get("stream").and_then(Value::as_bool).unwrap_or(false)
        || header_value(headers, "accept")
            .map(|value| value.contains("text/event-stream"))
            .unwrap_or(false)
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": "unauthorized"})),
    )
        .into_response()
}

fn sse(text: &str) -> Response {
    let delta = json!({
        "type": "content_block_delta",
        "delta": {"type": "text_delta", "text": text}
    });
    let body = format!(
        "event: message_start\ndata: {{\"type\":\"message_start\",\"message\":{{\"role\":\"assistant\"}}}}\n\n\
         event: content_block_delta\ndata: {delta}\n\n\
         event: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n"
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/event-stream")
        .body(Body::from(body))
        .expect("sse response")
}

fn with_passthrough(mut response: Response, headers: &HeaderMap) -> Response {
    for name in PASSTHROUGH {
        let Some(value) = header_value(headers, name) else {
            continue;
        };
        let Ok(header_name) = HeaderName::try_from(*name) else {
            continue;
        };
        let Ok(header_value) = HeaderValue::try_from(value) else {
            continue;
        };
        response.headers_mut().insert(header_name, header_value);
    }
    response
}
