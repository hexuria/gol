use proxy::{CLAUDE_TEXT, CODEX_TEXT, GATEWAY_TEXT, GROK_TEXT};

async fn serve() -> String {
    let app = proxy::router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://{addr}")
}

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

#[tokio::test]
async fn claude_messages_require_one_auth_header_and_answer_from_a_fixture() {
    let base = serve().await;
    let client = client();

    let missing = client
        .post(format!("{base}/v1/messages?beta=true"))
        .json(&serde_json::json!({"messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .expect("post");
    assert_eq!(missing.status(), 401);

    let keyed = client
        .post(format!("{base}/v1/messages?beta=true"))
        .header("x-api-key", "gol-desktop-fixture")
        .header("anthropic-version", "2023-06-01")
        .header("anthropic-beta", "claude-code-20250219,oauth-2025-04-20")
        .header("x-claude-code-session-id", "session-1")
        .json(&serde_json::json!({
            "model": "claude-fixture",
            "max_tokens": 32,
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .expect("post");
    assert_eq!(keyed.status(), 200);
    assert_eq!(
        keyed
            .headers()
            .get("anthropic-version")
            .and_then(|v| v.to_str().ok()),
        Some("2023-06-01")
    );
    assert_eq!(
        keyed
            .headers()
            .get("anthropic-beta")
            .and_then(|v| v.to_str().ok()),
        Some("claude-code-20250219,oauth-2025-04-20")
    );
    assert_eq!(
        keyed
            .headers()
            .get("x-claude-code-session-id")
            .and_then(|v| v.to_str().ok()),
        Some("session-1")
    );
    let body: serde_json::Value = keyed.json().await.expect("json");
    assert_eq!(
        body.pointer("/content/0/text")
            .and_then(|value| value.as_str()),
        Some(CLAUDE_TEXT)
    );

    let bearer = client
        .post(format!("{base}/v1/messages"))
        .header("authorization", "Bearer gol-desktop-fixture")
        .json(&serde_json::json!({"messages": []}))
        .send()
        .await
        .expect("post");
    assert_eq!(bearer.status(), 200);
}

#[tokio::test]
async fn claude_stream_is_server_sent_events() {
    let base = serve().await;
    let response = client()
        .post(format!("{base}/v1/messages?beta=true"))
        .header("x-api-key", "gol-desktop-fixture")
        .header("anthropic-version", "2023-06-01")
        .json(&serde_json::json!({"stream": true, "messages": []}))
        .send()
        .await
        .expect("post");
    assert_eq!(response.status(), 200);
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.starts_with("text/event-stream"),
        "{content_type}"
    );
    let body = response.text().await.expect("text");
    assert!(body.contains("data:"));
    assert!(body.contains(CLAUDE_TEXT));
}

#[tokio::test]
async fn codex_subscription_shape_requires_bearer_and_account() {
    let base = serve().await;
    let client = client();
    let body = serde_json::json!({"store": false, "stream": false, "input": "hi"});

    let missing = client
        .post(format!("{base}/backend-api/codex/responses"))
        .json(&body)
        .send()
        .await
        .expect("post");
    assert_eq!(missing.status(), 401);

    let no_account = client
        .post(format!("{base}/backend-api/codex/responses"))
        .header("authorization", "Bearer codex-fixture")
        .json(&body)
        .send()
        .await
        .expect("post");
    assert_eq!(no_account.status(), 401);

    let ready = client
        .post(format!("{base}/loopback/backend-api/codex/responses"))
        .header("authorization", "Bearer codex-fixture")
        .header("ChatGPT-Account-ID", "acct_fixture")
        .header("originator", "codex_cli_rs")
        .json(&body)
        .send()
        .await
        .expect("post");
    assert_eq!(ready.status(), 200);
    let payload: serde_json::Value = ready.json().await.expect("json");
    assert_eq!(
        payload
            .pointer("/output/0/content/0/text")
            .and_then(|value| value.as_str()),
        Some(CODEX_TEXT)
    );
}

#[tokio::test]
async fn grok_session_shape_requires_bearer_and_cli_token_header() {
    let base = serve().await;
    let client = client();
    let body = serde_json::json!({"input": "hi"});

    let missing = client
        .post(format!("{base}/v1/responses"))
        .header("authorization", "Bearer grok-fixture")
        .json(&body)
        .send()
        .await
        .expect("post");
    assert_eq!(missing.status(), 401);

    let wrong = client
        .post(format!("{base}/v1/responses"))
        .header("authorization", "Bearer grok-fixture")
        .header("X-XAI-Token-Auth", "something-else")
        .json(&body)
        .send()
        .await
        .expect("post");
    assert_eq!(wrong.status(), 401);

    let ready = client
        .post(format!("{base}/v1/responses"))
        .header("authorization", "Bearer grok-fixture")
        .header("X-XAI-Token-Auth", "xai-grok-cli")
        .header("x-grok-model-override", "grok-4.6")
        .json(&body)
        .send()
        .await
        .expect("post");
    assert_eq!(ready.status(), 200);
    let payload: serde_json::Value = ready.json().await.expect("json");
    assert_eq!(
        payload.get("model").and_then(|value| value.as_str()),
        Some("grok-4.6")
    );
    assert_eq!(
        payload
            .pointer("/output/0/content/0/text")
            .and_then(|value| value.as_str()),
        Some(GROK_TEXT)
    );
}

#[tokio::test]
async fn platform_gateway_is_a_separate_path_and_requires_a_bearer() {
    let base = serve().await;
    let client = client();

    let missing = client
        .post(format!("{base}/v1/gateway/complete"))
        .json(&serde_json::json!({"input": "hi"}))
        .send()
        .await
        .expect("post");
    assert_eq!(missing.status(), 401);

    let ready = client
        .post(format!("{base}/v1/gateway/complete"))
        .header("authorization", "Bearer gol-gateway-local")
        .header("x-gol-caller", "server")
        .json(&serde_json::json!({"input": "hi", "model": "gateway-fixture"}))
        .send()
        .await
        .expect("post");
    assert_eq!(ready.status(), 200);
    let payload: serde_json::Value = ready.json().await.expect("json");
    assert_eq!(
        payload.get("text").and_then(|value| value.as_str()),
        Some(GATEWAY_TEXT)
    );
}

#[test]
fn proxy_dependencies_do_not_include_an_http_client() {
    let manifest = include_str!("../Cargo.toml");
    let runtime = manifest.split("[dev-dependencies]").next().expect("split");
    assert!(!runtime.contains("reqwest"));
    assert!(!runtime.contains("ureq"));
    assert!(!runtime.contains("api.anthropic.com"));
    assert!(!runtime.contains("api.openai.com"));
    assert!(!runtime.contains("api.x.ai"));
}
