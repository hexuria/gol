//! Runs and agent manifests belong to the principal that created them. Every
//! read, completion and failure checks the owner; another principal sees 404.
mod common;

use std::sync::Arc;

use protocol::{AgentId, Capability, RunId, RunState};
use server::{
    router_with_sandbox, AgentManifest, GatewayCall, GatewayPoster, InMemoryStore, MemorySandbox,
    RunStore,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct NoPoster;

impl GatewayPoster for NoPoster {
    fn complete(&self, _call: &GatewayCall) -> Result<String, String> {
        Err("subscription must not post".to_string())
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

struct Server {
    base: String,
    store: Arc<InMemoryStore>,
    _jev: MockServer,
}

async fn serve() -> Server {
    let jev = jev().await;
    let store = Arc::new(InMemoryStore::default());
    let app = router_with_sandbox(
        store.clone(),
        jev.uri(),
        Arc::new(NoPoster),
        Arc::new(MemorySandbox::default()),
        common::authenticator(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Server {
        base: format!("http://{addr}"),
        store,
        _jev: jev,
    }
}

const ALICE: &str = "alice";
const BOB: &str = "bob";

async fn send(
    method: reqwest::Method,
    url: String,
    user: &str,
    body: Option<serde_json::Value>,
) -> (u16, serde_json::Value) {
    let mut request = reqwest::Client::new()
        .request(method, url)
        .header("authorization", common::bearer_for(user));
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    (
        status,
        response.json().await.unwrap_or(serde_json::Value::Null),
    )
}

fn manifest(id: AgentId, version: &str, capabilities: &[&str]) -> serde_json::Value {
    serde_json::to_value(AgentManifest {
        id,
        version: version.to_string(),
        instructions: "finish".to_string(),
        tools: vec!["echo".to_string()],
        required_capabilities: capabilities.iter().map(|c| Capability::new(*c)).collect(),
    })
    .unwrap()
}

fn run_body(agent_id: AgentId, version: &str) -> serde_json::Value {
    serde_json::json!({
        "agent_id": agent_id,
        "agent_version": version,
        "input": "hello",
        "placement": "Local",
        "work_model": {"provider": "OpenAI", "model_name": "gpt-test", "credential": "PlatformGateway"},
    })
}

async fn agent(server: &Server, user: &str, capabilities: &[&str]) -> AgentId {
    let id = AgentId::new();
    let (status, _) = send(
        reqwest::Method::POST,
        format!("{}/v1/agents", server.base),
        user,
        Some(manifest(id, "1", capabilities)),
    )
    .await;
    assert_eq!(status, 200);
    id
}

async fn run(server: &Server, user: &str) -> RunId {
    let agent_id = agent(server, user, &["tool.echo"]).await;
    let (status, body) = send(
        reqwest::Method::POST,
        format!("{}/v1/runs", server.base),
        user,
        Some(run_body(agent_id, "1")),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    serde_json::from_value::<RunState>(body).unwrap().run_id
}

async fn turn(server: &Server, user: &str) -> RunId {
    let (status, body) = send(
        reqwest::Method::POST,
        format!("{}/v1/coworker/turns", server.base),
        user,
        Some(serde_json::json!({
            "agent_id": AgentId::new(),
            "agent_version": "1",
            "input": "hello",
            "placement": "Local",
            "work_model": {"provider": "Anthropic", "model_name": "claude-fixture",
                "credential": {"BringYourOwn": {"secret_ref": "desktop-subscription"}}},
            "capabilities": ["model.call"],
        })),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    serde_json::from_value(body["run_id"].clone()).unwrap()
}

async fn read(server: &Server, user: &str, suffix: &str, id: RunId) -> u16 {
    send(
        reqwest::Method::GET,
        format!("{}/v1/runs/{id}{suffix}", server.base),
        user,
        None,
    )
    .await
    .0
}

#[tokio::test]
async fn other_principal_404_on_run() {
    let server = serve().await;
    let id = run(&server, ALICE).await;
    assert_eq!(read(&server, ALICE, "", id).await, 200);
    assert_eq!(read(&server, BOB, "", id).await, 404);
}

#[tokio::test]
async fn other_principal_404_on_events() {
    let server = serve().await;
    let id = run(&server, ALICE).await;
    assert_eq!(read(&server, ALICE, "/events", id).await, 200);
    assert_eq!(read(&server, BOB, "/events", id).await, 404);
}

#[tokio::test]
async fn other_principal_404_on_ag_ui() {
    let server = serve().await;
    let id = run(&server, ALICE).await;
    assert_eq!(read(&server, ALICE, "/ag-ui", id).await, 200);
    assert_eq!(read(&server, BOB, "/ag-ui", id).await, 404);
}

#[tokio::test]
async fn other_principal_404_on_ui() {
    let server = serve().await;
    let id = run(&server, ALICE).await;
    assert_eq!(read(&server, ALICE, "/ui", id).await, 200);
    assert_eq!(read(&server, BOB, "/ui", id).await, 404);
}

// Another principal cannot finish a turn, and the owner still can.
#[tokio::test]
async fn other_principal_cannot_complete() {
    let server = serve().await;
    let id = turn(&server, ALICE).await;
    let url = format!("{}/v1/coworker/turns/{id}/completion", server.base);
    let text = Some(serde_json::json!({"text": "done"}));
    let before = server.store.run(id).unwrap().events.len();
    assert_eq!(
        send(reqwest::Method::POST, url.clone(), BOB, text.clone())
            .await
            .0,
        404
    );
    assert_eq!(server.store.run(id).unwrap().events.len(), before);
    assert_eq!(send(reqwest::Method::POST, url, ALICE, text).await.0, 200);
}

#[tokio::test]
async fn other_principal_cannot_fail() {
    let server = serve().await;
    let id = turn(&server, ALICE).await;
    let url = format!("{}/v1/coworker/turns/{id}/fail", server.base);
    let message = Some(serde_json::json!({"message": "proxy: refused"}));
    let before = server.store.run(id).unwrap().events.len();
    assert_eq!(
        send(reqwest::Method::POST, url.clone(), BOB, message.clone())
            .await
            .0,
        404
    );
    assert_eq!(server.store.run(id).unwrap().events.len(), before);
    assert_eq!(
        send(reqwest::Method::POST, url, ALICE, message).await.0,
        200
    );
}

// A run records its owner and takes its capabilities from the manifest.
#[tokio::test]
async fn capabilities_from_manifest() {
    let server = serve().await;
    let agent_id = agent(&server, ALICE, &["tool.echo", "model.call"]).await;
    let (status, body) = send(
        reqwest::Method::POST,
        format!("{}/v1/runs", server.base),
        ALICE,
        Some(run_body(agent_id, "1")),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let id = serde_json::from_value::<RunState>(body).unwrap().run_id;
    let spec = server.store.run(id).unwrap().spec;
    assert_eq!(
        spec.capabilities,
        vec![Capability::new("tool.echo"), Capability::new("model.call")]
    );
    assert_eq!(spec.owner.subject, ALICE);
    assert_eq!(spec.owner.issuer, common::ISSUER);
    assert_eq!(spec.owner.tenant, "tenant-1");
}

// A run body cannot name capabilities, or any other unknown field.
#[tokio::test]
async fn body_capabilities_400() {
    let server = serve().await;
    let agent_id = agent(&server, ALICE, &["tool.echo"]).await;
    for (field, value) in [
        ("capabilities", serde_json::json!(["tool.shell"])),
        ("owner", serde_json::json!("bob")),
    ] {
        let mut body = run_body(agent_id, "1");
        body[field] = value;
        let (status, _) = send(
            reqwest::Method::POST,
            format!("{}/v1/runs", server.base),
            ALICE,
            Some(body),
        )
        .await;
        assert_eq!(status, 400, "{field}");
    }
}

// A run needs a stored manifest its caller owns; any other agent is 404.
#[tokio::test]
async fn unknown_agent_404() {
    let server = serve().await;
    let url = format!("{}/v1/runs", server.base);
    let (status, body) = send(
        reqwest::Method::POST,
        url.clone(),
        ALICE,
        Some(run_body(AgentId::new(), "1")),
    )
    .await;
    assert_eq!(
        (status, body),
        (404, serde_json::json!({"error": "agent not found"}))
    );
    let bobs = agent(&server, BOB, &["tool.echo"]).await;
    let (status, body) = send(reqwest::Method::POST, url, ALICE, Some(run_body(bobs, "1"))).await;
    assert_eq!(
        (status, body),
        (404, serde_json::json!({"error": "agent not found"}))
    );
}

// The run's agent_version must be the stored manifest's.
#[tokio::test]
async fn agent_version_must_match_the_manifest() {
    let server = serve().await;
    let agent_id = agent(&server, ALICE, &["tool.echo"]).await;
    let (status, _) = send(
        reqwest::Method::POST,
        format!("{}/v1/runs", server.base),
        ALICE,
        Some(run_body(agent_id, "2")),
    )
    .await;
    assert_eq!(status, 409);
}

// The owner may replace a manifest; another principal may not.
#[tokio::test]
async fn only_the_owner_replaces_a_manifest() {
    let server = serve().await;
    let id = agent(&server, ALICE, &["tool.echo"]).await;
    let url = format!("{}/v1/agents", server.base);
    let (status, body) = send(
        reqwest::Method::POST,
        url.clone(),
        BOB,
        Some(manifest(id, "9", &["tool.shell"])),
    )
    .await;
    assert_eq!(
        (status, body),
        (
            409,
            serde_json::json!({"error": "agent belongs to another principal"})
        )
    );
    let stored = server.store.agent(id).unwrap();
    assert_eq!(
        (
            stored.manifest.version.as_str(),
            stored.owner.subject.as_str()
        ),
        ("1", ALICE)
    );
    let (status, _) = send(
        reqwest::Method::POST,
        url,
        ALICE,
        Some(manifest(id, "2", &["tool.echo"])),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(server.store.agent(id).unwrap().manifest.version, "2");
}

// Owning a run is the issuer and subject; the same subject in another tenant
// is the same principal.
#[tokio::test]
async fn the_owner_is_issuer_and_subject() {
    let server = serve().await;
    let id = run(&server, ALICE).await;
    let other_tenant = format!(
        "Bearer {}",
        common::sign(common::eddsa(), &common::claims(ALICE, "tenant-2"))
    );
    let response = reqwest::Client::new()
        .get(format!("{}/v1/runs/{id}", server.base))
        .header("authorization", other_tenant)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
}

// formal/agentowner FirstOwnerKeeps and OnlyOwnerStores, on the in-memory
// store: principals racing to put the same agent id leave it with the first
// one to store it, and no other is ever told its put was stored.
#[test]
fn first_owner_keeps_the_agent() {
    use server::{PutAgent, StoredAgent};
    for _ in 0..20 {
        let store = Arc::new(InMemoryStore::default());
        let id = AgentId::new();
        let threads: Vec<_> = ["alice", "bob", "carol"]
            .into_iter()
            .map(|subject| {
                let store = store.clone();
                std::thread::spawn(move || {
                    let owner = protocol::Owner::new(common::ISSUER, subject, "tenant-1");
                    (0..20)
                        .map(|version| {
                            let manifest: AgentManifest =
                                serde_json::from_value(manifest(id, &version.to_string(), &[]))
                                    .unwrap();
                            store.put_agent(StoredAgent {
                                manifest,
                                owner: owner.clone(),
                            })
                        })
                        .map(|put| (subject, put))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let puts: Vec<_> = threads
            .into_iter()
            .flat_map(|t| t.join().unwrap())
            .collect();
        let owner = store.agent(id).unwrap().owner.subject;
        for (subject, put) in puts {
            let expected = if subject == owner {
                PutAgent::Stored
            } else {
                PutAgent::OwnedByOther
            };
            assert_eq!(put, expected, "{subject} with owner {owner}");
        }
    }
}

// Another principal's run answers exactly as a run that does not exist: the
// same status and the same body, on every route.
#[tokio::test]
async fn another_principals_run_looks_missing() {
    let server = serve().await;
    let run_id = run(&server, ALICE).await;
    let turn_id = turn(&server, ALICE).await;
    let missing = RunId::new();
    for suffix in ["", "/events", "/ag-ui", "/ui"] {
        let get = |id: RunId| {
            send(
                reqwest::Method::GET,
                format!("{}/v1/runs/{id}{suffix}", server.base),
                BOB,
                None,
            )
        };
        assert_eq!(get(run_id).await, get(missing).await, "{suffix}");
    }
    for (route, body) in [
        ("completion", serde_json::json!({"text": "done"})),
        ("fail", serde_json::json!({"message": "x"})),
    ] {
        let post = |id: RunId| {
            send(
                reqwest::Method::POST,
                format!("{}/v1/coworker/turns/{id}/{route}", server.base),
                BOB,
                Some(body.clone()),
            )
        };
        assert_eq!(post(turn_id).await, post(missing).await, "{route}");
    }
}

// The owner is issuer and subject on the store too: the same subject from
// another tenant replaces the manifest, and the replacement is stored.
#[test]
fn the_owner_replaces_from_any_tenant_and_no_one_else_does() {
    use server::{PutAgent, StoredAgent};
    let store = InMemoryStore::default();
    let id = AgentId::new();
    let put = |subject: &str, tenant: &str, version: &str| {
        store.put_agent(StoredAgent {
            manifest: serde_json::from_value(manifest(id, version, &[])).unwrap(),
            owner: protocol::Owner::new(common::ISSUER, subject, tenant),
        })
    };
    assert_eq!(put(ALICE, "tenant-1", "1"), PutAgent::Stored);
    assert_eq!(put(BOB, "tenant-1", "9"), PutAgent::OwnedByOther);
    assert_eq!(store.agent(id).unwrap().manifest.version, "1");
    assert_eq!(put(ALICE, "tenant-2", "2"), PutAgent::Stored);
    let stored = store.agent(id).unwrap();
    assert_eq!(stored.manifest.version, "2");
    assert_eq!(stored.owner.subject, ALICE);
    assert_eq!(stored.owner.tenant, "tenant-2");
}

// Only a bad body is a 400; a request without a JSON content type keeps its
// own 415.
#[tokio::test]
async fn only_a_bad_run_body_is_400() {
    let server = serve().await;
    let response = reqwest::Client::new()
        .post(format!("{}/v1/runs", server.base))
        .header("authorization", common::bearer_for(ALICE))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 415);
    let (status, _) = send(
        reqwest::Method::POST,
        format!("{}/v1/runs", server.base),
        ALICE,
        Some(serde_json::json!({"agent_id": 7})),
    )
    .await;
    assert_eq!(status, 400);
}
