//! A store that cannot answer is a 503 with a fixed body. The detail goes to
//! stderr, never to the caller.
mod common;

use std::sync::Arc;

use protocol::{AgentId, ArtifactId, Event, RunId};
use server::{
    router, Append, PutAgent, RunStore, StoreError, StoredAgent, StoredArtifact, StoredRun,
};

const DETAIL: &str = "connection refused by 10.0.0.7";

/// Every method fails, as a store behind a dead connection does.
struct DownStore;

fn down<T>() -> Result<T, StoreError> {
    Err(StoreError::new(DETAIL))
}

impl RunStore for DownStore {
    fn put_agent(&self, _agent: StoredAgent) -> Result<PutAgent, StoreError> {
        down()
    }

    fn agent(&self, _id: AgentId) -> Result<Option<StoredAgent>, StoreError> {
        down()
    }

    fn put_run(&self, _run: StoredRun) -> Result<(), StoreError> {
        down()
    }

    fn append_events(&self, _id: RunId, _events: Vec<Event>) -> Result<Append, StoreError> {
        down()
    }

    fn run(&self, _id: RunId) -> Result<Option<StoredRun>, StoreError> {
        down()
    }

    fn put_artifact(&self, _artifact: StoredArtifact) -> Result<(), StoreError> {
        down()
    }

    fn artifact(&self, _id: ArtifactId) -> Result<Option<StoredArtifact>, StoreError> {
        down()
    }
}

async fn serve() -> String {
    let app = router(
        Arc::new(DownStore),
        "http://127.0.0.1:9",
        common::authenticator(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn send(request: reqwest::RequestBuilder) -> (u16, String) {
    let response = request
        .header("authorization", common::bearer())
        .send()
        .await
        .unwrap();
    (response.status().as_u16(), response.text().await.unwrap())
}

const BODY: &str = r#"{"error":"store unavailable"}"#;

#[tokio::test]
async fn a_run_read_from_a_down_store_is_503() {
    let base = serve().await;
    let client = reqwest::Client::new();
    let id = RunId::new();
    for path in ["", "/events", "/ag-ui", "/ui"] {
        let (status, body) = send(client.get(format!("{base}/v1/runs/{id}{path}"))).await;
        assert_eq!((status, body.as_str()), (503, BODY), "GET {path}");
    }
}

#[tokio::test]
async fn an_agent_write_to_a_down_store_is_503() {
    let base = serve().await;
    let (status, body) = send(
        reqwest::Client::new()
            .post(format!("{base}/v1/agents"))
            .json(&serde_json::json!({
                "id": AgentId::new(),
                "version": "1",
                "instructions": "i",
                "tools": [],
                "required_capabilities": []
            })),
    )
    .await;
    assert_eq!((status, body.as_str()), (503, BODY));
}

#[tokio::test]
async fn a_run_create_on_a_down_store_is_503() {
    let base = serve().await;
    let (status, body) = send(reqwest::Client::new().post(format!("{base}/v1/runs")).json(
        &serde_json::json!({
            "agent_id": AgentId::new(),
            "agent_version": "1",
            "input": "hello",
            "placement": "Local",
            "work_model": {
                "provider": "OpenAI",
                "model_name": "gpt-test",
                "credential": "PlatformGateway"
            },
            "limits": { "max_steps": 8, "max_model_calls": 4 }
        }),
    ))
    .await;
    assert_eq!((status, body.as_str()), (503, BODY));
}

#[tokio::test]
async fn a_coworker_turn_on_a_down_store_is_503() {
    let base = serve().await;
    let client = reqwest::Client::new();
    let (status, body) = send(client.post(format!("{base}/v1/coworker/turns")).json(
        &serde_json::json!({
            "agent_id": AgentId::new(),
            "agent_version": "1",
            "input": "hello",
            "placement": "Local",
            "work_model": {
                "provider": "OpenAI",
                "model_name": "gpt-test",
                "credential": "PlatformGateway"
            }
        }),
    ))
    .await;
    assert_eq!((status, body.as_str()), (503, BODY), "open");
    let id = RunId::new();
    for (path, json) in [
        ("completion", serde_json::json!({"text": "done"})),
        ("fail", serde_json::json!({"message": "nope"})),
    ] {
        let (status, body) = send(
            client
                .post(format!("{base}/v1/coworker/turns/{id}/{path}"))
                .json(&json),
        )
        .await;
        assert_eq!((status, body.as_str()), (503, BODY), "{path}");
    }
}
