//! A store that cannot answer is a 503 with a fixed body. The detail goes to
//! stderr, never to the caller.
mod common;

use std::sync::{Arc, Mutex};

use protocol::{AgentId, ArtifactId, Event, RunId};
use server::{
    router, router_with_sandbox, Append, GatewayCall, GatewayPoster, InMemoryStore, MemorySandbox,
    PutAgent, RunStore, StoreError, StoredAgent, StoredArtifact, StoredRun,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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

/// An in-memory store whose named methods fail. The rest answer, so a test
/// reaches the store call it means to fail.
#[derive(Default)]
struct PartlyDown {
    inner: InMemoryStore,
    down: Mutex<Vec<&'static str>>,
}

impl PartlyDown {
    fn fail(&self, method: &'static str) {
        self.down.lock().unwrap().push(method);
    }

    fn check(&self, method: &'static str) -> Result<(), StoreError> {
        if self.down.lock().unwrap().contains(&method) {
            return down();
        }
        Ok(())
    }
}

impl RunStore for PartlyDown {
    fn put_agent(&self, agent: StoredAgent) -> Result<PutAgent, StoreError> {
        self.check("put_agent")?;
        self.inner.put_agent(agent)
    }

    fn agent(&self, id: AgentId) -> Result<Option<StoredAgent>, StoreError> {
        self.check("agent")?;
        self.inner.agent(id)
    }

    fn put_run(&self, run: StoredRun) -> Result<(), StoreError> {
        self.check("put_run")?;
        self.inner.put_run(run)
    }

    fn append_events(&self, id: RunId, events: Vec<Event>) -> Result<Append, StoreError> {
        self.check("append_events")?;
        self.inner.append_events(id, events)
    }

    fn run(&self, id: RunId) -> Result<Option<StoredRun>, StoreError> {
        self.check("run")?;
        self.inner.run(id)
    }

    fn put_artifact(&self, artifact: StoredArtifact) -> Result<(), StoreError> {
        self.check("put_artifact")?;
        self.inner.put_artifact(artifact)
    }

    fn artifact(&self, id: ArtifactId) -> Result<Option<StoredArtifact>, StoreError> {
        self.check("artifact")?;
        self.inner.artifact(id)
    }
}

struct DownPoster;

impl GatewayPoster for DownPoster {
    fn complete(&self, _call: &GatewayCall) -> Result<String, String> {
        Err("gateway down".to_string())
    }
}

/// A Jev that always completes the run.
async fn jev() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "model": "jev-latest",
            "usage": {"input_tokens": 1, "output_tokens": 1},
            "answers": {"effect": {"type": "choice", "choice": "complete", "confidence": 1.0,
                "probabilities": {"echo": 0.0, "model": 0.0, "complete": 1.0}}}
        })))
        .mount(&server)
        .await;
    server
}

async fn serve_partly(store: Arc<PartlyDown>, jev: &MockServer) -> String {
    let app = router_with_sandbox(
        store,
        jev.uri(),
        Arc::new(DownPoster),
        Arc::new(MemorySandbox::default()),
        common::authenticator(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn put_agent(base: &str) -> AgentId {
    let id = AgentId::new();
    let (status, _) = send(
        reqwest::Client::new()
            .post(format!("{base}/v1/agents"))
            .json(&serde_json::json!({
                "id": id,
                "version": "1",
                "instructions": "i",
                "tools": [],
                "required_capabilities": []
            })),
    )
    .await;
    assert_eq!(status, 200);
    id
}

async fn create_run(base: &str, agent_id: AgentId) -> (u16, String) {
    send(
        reqwest::Client::new()
            .post(format!("{base}/v1/runs"))
            .json(&serde_json::json!({
                "agent_id": agent_id,
                "agent_version": "1",
                "input": "hello",
                "placement": "Local",
                "work_model": {
                    "provider": "OpenAI",
                    "model_name": "gpt-test",
                    "credential": "PlatformGateway"
                },
                "limits": { "max_steps": 8, "max_model_calls": 4 }
            })),
    )
    .await
}

// Each store call that create_run makes after the agent lookup: the first
// put, the append of what the harness did, and the read back.
#[tokio::test]
async fn a_run_create_whose_later_store_call_fails_is_503() {
    let jev = jev().await;
    for method in ["put_run", "append_events", "run"] {
        let store = Arc::new(PartlyDown::default());
        let base = serve_partly(store.clone(), &jev).await;
        let agent_id = put_agent(&base).await;
        assert_eq!(create_run(&base, agent_id).await.0, 200, "{method} up");
        store.fail(method);
        let (status, body) = create_run(&base, agent_id).await;
        assert_eq!((status, body.as_str()), (503, BODY), "{method} down");
    }
}

fn turn(credential: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "agent_id": AgentId::new(),
        "agent_version": "1",
        "input": "hello",
        "placement": "Local",
        "work_model": {"provider": "Anthropic", "model_name": "claude-fixture",
            "credential": credential},
        "capabilities": ["model.call"],
    })
}

// The run is found and open, and only the append of the completion fails.
#[tokio::test]
async fn a_completion_whose_append_fails_is_503() {
    let jev = jev().await;
    let store = Arc::new(PartlyDown::default());
    let base = serve_partly(store.clone(), &jev).await;
    let client = reqwest::Client::new();
    let opened = client
        .post(format!("{base}/v1/coworker/turns"))
        .header("authorization", common::bearer())
        .json(&turn(
            serde_json::json!({"BringYourOwn": {"secret_ref": "desktop-subscription"}}),
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(opened.status().as_u16(), 200);
    let run_id = opened.json::<serde_json::Value>().await.unwrap()["run_id"].clone();
    let run_id = run_id.as_str().unwrap().to_string();
    store.fail("append_events");
    let (status, body) = send(
        client
            .post(format!("{base}/v1/coworker/turns/{run_id}/completion"))
            .json(&serde_json::json!({"text": "done"})),
    )
    .await;
    assert_eq!((status, body.as_str()), (503, BODY));
}

// Ending a turn that already failed is best effort: a store that cannot take
// the RunFailed append leaves the caller with the failure that led there.
#[tokio::test]
async fn a_failed_turn_reports_its_own_error_when_the_store_cannot_end_it() {
    let jev = jev().await;
    let store = Arc::new(PartlyDown::default());
    store.fail("append_events");
    let base = serve_partly(store, &jev).await;
    let (status, body) = send(
        reqwest::Client::new()
            .post(format!("{base}/v1/coworker/turns"))
            .json(&turn(serde_json::json!("PlatformGateway"))),
    )
    .await;
    assert_eq!(
        (status, body.as_str()),
        (502, r#"{"error":"gateway down"}"#)
    );
}

// Without GOL_DATABASE_URL the server keeps runs and memory in memory, and
// does not read the pool size.
#[test]
fn no_database_url_keeps_the_stores_in_memory() {
    let env = [("GOL_DATABASE_POOL_SIZE".to_string(), "x".to_string())]
        .into_iter()
        .collect();
    let stores = server::stores_from_env(&env).expect("stores");
    assert!(stores.runs.run(RunId::new()).expect("store").is_none());
}
