use std::sync::{Arc, Mutex};
use std::time::Duration;

use protocol::{AgentId, ArtifactId, Capability, Event, EventPayload, HarnessState, RunId};
use server::{
    router, router_with_queue, AgentManifest, InMemoryStore, RunStore, StoredArtifact, StoredRun,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn choice(effect: &str) -> serde_json::Value {
    let mut probabilities = serde_json::json!({"echo": 0.0, "model": 0.0, "complete": 0.0});
    probabilities[effect] = serde_json::json!(1.0);
    serde_json::json!({
        "model": "jev-latest",
        "usage": {"input_tokens": 1, "output_tokens": 1},
        "answers": {
            "effect": {
                "type": "choice",
                "choice": effect,
                "confidence": 1.0,
                "probabilities": probabilities
            }
        }
    })
}

async fn jev_mock() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(choice("echo")))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(choice("complete")))
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn post_run_reads_completed_and_events() {
    let jev = jev_mock().await;
    let app = router(Arc::new(InMemoryStore::default()), jev.uri());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let client = reqwest::Client::new();
    let base = format!("http://{addr}");
    let agent_id = AgentId::new();
    post_when_up(
        &client,
        &format!("{base}/v1/agents"),
        &AgentManifest {
            id: agent_id,
            version: "1".to_string(),
            instructions: "Echo the input, then finish.".to_string(),
            tools: vec!["echo".to_string()],
            required_capabilities: vec![Capability::new("tool.echo")],
        },
    )
    .await;

    let created = post_when_up(
        &client,
        &format!("{base}/v1/runs"),
        &serde_json::json!({
            "agent_id": agent_id,
            "agent_version": "1",
            "input": "hello",
            "placement": "Local",
            "work_model": {
                "provider": "OpenAI",
                "model_name": "gpt-test",
                "credential": "PlatformGateway"
            },
            "capabilities": ["tool.echo"],
            "limits": { "max_steps": 8, "max_model_calls": 4 }
        }),
    )
    .await;
    let created: protocol::RunState = created.json().await.expect("run json");
    assert_eq!(
        created.harness,
        HarnessState::Completed {
            outcome: "done".to_string()
        }
    );

    let fetched = client
        .get(format!("{base}/v1/runs/{}", created.run_id))
        .header("authorization", "Bearer gol-gateway-local")
        .send()
        .await
        .expect("get run")
        .error_for_status()
        .expect("get status")
        .json::<protocol::RunState>()
        .await
        .expect("fetched json");
    assert_eq!(fetched.harness, created.harness);

    let events = client
        .get(format!("{base}/v1/runs/{}/events", created.run_id))
        .header("authorization", "Bearer gol-gateway-local")
        .send()
        .await
        .expect("get events")
        .error_for_status()
        .expect("events status")
        .json::<Vec<protocol::Event>>()
        .await
        .expect("events json");
    assert!(events.iter().any(|event| matches!(
        &event.payload,
        EventPayload::ToolResult { name, output, .. }
            if name == "echo" && output == "hello"
    )));
    assert!(events
        .iter()
        .any(|event| matches!(&event.payload, EventPayload::RunCompleted { outcome } if outcome == "done")));

    let ag_ui = client
        .get(format!("{base}/v1/runs/{}/ag-ui", created.run_id))
        .header("authorization", "Bearer gol-gateway-local")
        .send()
        .await
        .expect("ag-ui")
        .error_for_status()
        .expect("ag-ui status")
        .json::<Vec<serde_json::Value>>()
        .await
        .expect("ag-ui json");
    assert_eq!(
        ag_ui.first().and_then(|event| event.get("type")),
        Some(&serde_json::json!("RUN_STARTED"))
    );
    assert!(ag_ui
        .iter()
        .any(|event| event.get("type") == Some(&serde_json::json!("TOOL_CALL_RESULT"))));
    assert_eq!(
        ag_ui.last().and_then(|event| event.get("type")),
        Some(&serde_json::json!("RUN_FINISHED"))
    );

    let ui = client
        .get(format!("{base}/v1/runs/{}/ui", created.run_id))
        .header("authorization", "Bearer gol-gateway-local")
        .send()
        .await
        .expect("ui")
        .error_for_status()
        .expect("ui status")
        .json::<serde_json::Value>()
        .await
        .expect("ui json");
    assert_eq!(
        ui.get("root").and_then(|value| value.as_str()),
        Some("screen")
    );
    assert_eq!(
        ui.pointer("/elements/outcome/props/text")
            .and_then(|value| value.as_str()),
        Some("done")
    );
}

#[tokio::test]
async fn reverse_and_box_placements_complete() {
    for placement in ["Reverse", "Box"] {
        let jev = jev_mock().await;
        let app = router(Arc::new(InMemoryStore::default()), jev.uri());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        let client = reqwest::Client::new();
        let response = post_when_up(
            &client,
            &format!("http://{addr}/v1/runs"),
            &serde_json::json!({
                "agent_id": AgentId::new(),
                "agent_version": "1",
                "input": "hello",
                "placement": placement,
                "work_model": {
                    "provider": "OpenAI",
                    "model_name": "gpt-test",
                    "credential": "PlatformGateway"
                },
                "capabilities": ["tool.echo"],
                "limits": { "max_steps": 8, "max_model_calls": 4 }
            }),
        )
        .await;
        assert!(response.status().is_success(), "{placement}");
        let created: protocol::RunState = response.json().await.expect("run json");
        assert_eq!(
            created.harness,
            HarnessState::Completed {
                outcome: "done".to_string()
            }
        );
    }
}

#[tokio::test]
async fn run_calls_jev_system_one() {
    let jev = jev_mock().await;
    let app = router(Arc::new(InMemoryStore::default()), jev.uri());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    let client = reqwest::Client::new();
    let response = post_when_up(
        &client,
        &format!("http://{addr}/v1/runs"),
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
            "capabilities": ["tool.echo"],
            "limits": { "max_steps": 8, "max_model_calls": 4 }
        }),
    )
    .await;
    assert!(response.status().is_success());
    let requests = jev.received_requests().await.expect("requests");
    assert!(
        requests
            .iter()
            .any(|request| request.url.path() == "/v1/systemone"),
        "server run did not call Jev"
    );
}

async fn post_when_up(
    client: &reqwest::Client,
    url: &str,
    body: &impl serde::Serialize,
) -> reqwest::Response {
    let mut last = None;
    for _ in 0..20 {
        match client
            .post(url)
            .header("authorization", "Bearer gol-gateway-local")
            .json(body)
            .send()
            .await
        {
            Ok(response) => return response,
            Err(error) => {
                last = Some(error);
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
    panic!("server did not accept {url}: {last:?}");
}

#[tokio::test]
async fn missing_bearer_is_401_and_does_not_start_the_run() {
    let jev = jev_mock().await;
    let app = router(Arc::new(InMemoryStore::default()), jev.uri());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let client = reqwest::Client::new();
    let base = format!("http://{addr}");
    let missing = RunId::new();
    let routes = [
        ("POST", format!("{base}/v1/agents")),
        ("POST", format!("{base}/v1/runs")),
        ("GET", format!("{base}/v1/runs/{missing}")),
        ("GET", format!("{base}/v1/runs/{missing}/events")),
        ("GET", format!("{base}/v1/runs/{missing}/ag-ui")),
        ("GET", format!("{base}/v1/runs/{missing}/ui")),
        ("POST", format!("{base}/v1/coworker/turns")),
        (
            "POST",
            format!("{base}/v1/coworker/turns/{missing}/completion"),
        ),
    ];
    let headers: [Option<(&str, &str)>; 4] = [
        None,
        Some(("authorization", "")),
        Some(("authorization", "Bearer ")),
        Some(("x-api-key", "gol-desktop-fixture")),
    ];

    for (method, url) in &routes {
        for header in headers {
            let response = send_when_up(&client, method, url, header).await;
            assert_eq!(response.status().as_u16(), 401, "{method} {url} {header:?}");
            let body: serde_json::Value = response.json().await.expect("json");
            assert_eq!(body, serde_json::json!({"error": "unauthorized"}));
        }
    }

    let requests = jev.received_requests().await.expect("requests");
    assert!(
        !requests
            .iter()
            .any(|request| request.url.path() == "/v1/systemone"),
        "rejected create_run called Jev"
    );
}

#[tokio::test]
async fn redis_push_failure_does_not_run_the_harness() {
    let jev = jev_mock().await;
    let store = Arc::new(WatchedMemory::new());
    let app = router_with_queue(
        store.clone(),
        jev.uri(),
        Some("redis://127.0.0.1:6390".to_string()),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let response = post_when_up(
        &reqwest::Client::new(),
        &format!("http://{addr}/v1/runs"),
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
            "capabilities": ["tool.echo"],
            "limits": { "max_steps": 8, "max_model_calls": 4 }
        }),
    )
    .await;
    assert_eq!(response.status().as_u16(), 502);

    let ids = store.ids.lock().expect("ids").clone();
    assert_eq!(ids.len(), 1, "user message was not stored");
    let stored = store.run(ids[0]).expect("stored run");
    assert!(matches!(
        stored.events.as_slice(),
        [protocol::Event {
            payload: EventPayload::UserMessage { text },
            ..
        }] if text == "hello"
    ));
    assert!(!stored
        .events
        .iter()
        .any(|event| matches!(event.payload, EventPayload::RunCompleted { .. })));

    let requests = jev.received_requests().await.expect("requests");
    assert!(
        !requests
            .iter()
            .any(|request| request.url.path() == "/v1/systemone"),
        "push failure still called Jev"
    );
}

async fn send_when_up(
    client: &reqwest::Client,
    method: &str,
    url: &str,
    header: Option<(&str, &str)>,
) -> reqwest::Response {
    let mut last = None;
    for _ in 0..20 {
        let mut request = client.request(method.parse().expect("method"), url);
        if method == "POST" {
            request = request.json(&serde_json::json!({}));
        }
        if let Some((name, value)) = header {
            request = request.header(name, value);
        }
        match request.send().await {
            Ok(response) => return response,
            Err(error) => {
                last = Some(error);
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
    panic!("server did not accept {url}: {last:?}");
}

struct WatchedMemory {
    inner: InMemoryStore,
    ids: Mutex<Vec<RunId>>,
}

impl WatchedMemory {
    fn new() -> Self {
        Self {
            inner: InMemoryStore::default(),
            ids: Mutex::new(Vec::new()),
        }
    }
}

impl RunStore for WatchedMemory {
    fn put_agent(&self, agent: AgentManifest) {
        self.inner.put_agent(agent);
    }

    fn put_run(&self, run: StoredRun) {
        self.ids.lock().expect("ids").push(run.spec.run_id);
        self.inner.put_run(run);
    }

    fn replace_run(&self, run: StoredRun) {
        self.inner.replace_run(run);
    }

    fn append_events(&self, id: RunId, events: Vec<Event>) {
        self.inner.append_events(id, events);
    }

    fn run(&self, id: RunId) -> Option<StoredRun> {
        self.inner.run(id)
    }

    fn put_artifact(&self, artifact: StoredArtifact) {
        self.inner.put_artifact(artifact);
    }

    fn artifact(&self, id: ArtifactId) -> Option<StoredArtifact> {
        self.inner.artifact(id)
    }
}
