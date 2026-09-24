use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::Request;
use axum::middleware::Next;
use protocol::{AgentId, EventPayload, HarnessState};
use proxy::GATEWAY_TEXT;
use server::{
    box_container_name, ensure_fixture_proxy, router_with_gateway, router_with_sandbox,
    GatewayCall, GatewayPoster, HttpGatewayPoster, InMemoryStore, MemorySandbox, RunStore,
    SandboxHost,
};

async fn proxy_server() -> (String, Arc<Mutex<Vec<String>>>) {
    let paths = Arc::new(Mutex::new(Vec::new()));
    let seen = paths.clone();
    let app = proxy::router().layer(axum::middleware::from_fn(
        move |request: Request<Body>, next: Next| {
            let seen = seen.clone();
            async move {
                {
                    seen.lock()
                        .expect("paths")
                        .push(request.uri().path().to_string());
                }
                next.run(request).await
            }
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (format!("http://{addr}"), paths)
}

fn turn_body(placement: &str, credential: serde_json::Value, input: &str) -> serde_json::Value {
    serde_json::json!({
        "agent_id": AgentId::new(),
        "agent_version": "1",
        "input": input,
        "placement": placement,
        "work_model": {
            "provider": "Anthropic",
            "model_name": "claude-fixture",
            "credential": credential
        },
        "capabilities": ["model.call"],
        "limits": { "max_steps": 8, "max_model_calls": 4 }
    })
}

fn subscription() -> serde_json::Value {
    serde_json::json!({ "BringYourOwn": { "secret_ref": "desktop-subscription" } })
}

async fn post_when_up(
    client: &reqwest::Client,
    url: &str,
    body: &impl serde::Serialize,
) -> reqwest::Response {
    let mut last = None;
    for _ in 0..30 {
        match client.post(url).json(body).send().await {
            Ok(response) => return response,
            Err(error) => {
                last = Some(error);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
    panic!("post {url} failed: {last:?}");
}

async fn listen(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://{addr}")
}

struct SeeingPoster {
    store: Arc<InMemoryStore>,
    called: Arc<AtomicBool>,
}

impl GatewayPoster for SeeingPoster {
    fn complete(&self, call: &GatewayCall) -> Result<String, String> {
        self.called.store(true, Ordering::SeqCst);
        let stored = self.store.run(call.run_id).expect("user message stored");
        assert!(
            stored.events.iter().any(|event| matches!(
                &event.payload,
                EventPayload::UserMessage { text } if *text == call.input
            )),
            "proxy ran before the user message was recorded"
        );
        assert!(
            !stored
                .events
                .iter()
                .any(|event| matches!(event.payload, EventPayload::ModelResponded { .. })),
            "completion was recorded before the proxy returned"
        );
        Ok("seen-before-proxy".to_string())
    }
}

#[tokio::test]
async fn gateway_records_the_user_message_before_it_posts_to_the_proxy() {
    let store = Arc::new(InMemoryStore::default());
    let called = Arc::new(AtomicBool::new(false));
    let app = router_with_gateway(
        store.clone(),
        "http://127.0.0.1:9",
        Arc::new(SeeingPoster {
            store: store.clone(),
            called: called.clone(),
        }),
    );
    let base = listen(app).await;
    let response = post_when_up(
        &reqwest::Client::new(),
        &format!("{base}/v1/coworker/turns"),
        &turn_body("Box", serde_json::json!("PlatformGateway"), "ship the box"),
    )
    .await
    .error_for_status()
    .expect("status");
    let body: serde_json::Value = response.json().await.expect("json");
    assert!(called.load(Ordering::SeqCst));
    assert_eq!(body["credential_mode"], "gateway");
    assert_eq!(body["completion"], "seen-before-proxy");
    assert_eq!(body["computer"]["started_by"], "server");
    assert_eq!(body["computer"]["image"], "gol-agent:production");
    assert!(!body["computer"]["command"]
        .as_str()
        .unwrap()
        .contains("gateway"));
}

struct BoxWatch {
    store: Arc<InMemoryStore>,
    sandbox: Arc<MemorySandbox>,
    saw_live: Arc<AtomicBool>,
}

impl GatewayPoster for BoxWatch {
    fn complete(&self, call: &GatewayCall) -> Result<String, String> {
        let name = box_container_name(call.run_id);
        assert!(
            self.sandbox.exists(&name),
            "open_turn finished the turn before the container existed"
        );
        assert_ne!(name, "gol-agent-box");
        let stored = self.store.run(call.run_id).expect("user message stored");
        assert!(
            !stored
                .events
                .iter()
                .any(|event| matches!(event.payload, EventPayload::RunCompleted { .. })),
            "turn completed before the sandbox existed"
        );
        self.saw_live.store(true, Ordering::SeqCst);
        Ok("boxed".to_string())
    }
}

struct OkPoster;

impl GatewayPoster for OkPoster {
    fn complete(&self, _call: &GatewayCall) -> Result<String, String> {
        Ok("boxed".to_string())
    }
}

#[tokio::test]
async fn a_second_box_run_does_not_reuse_the_container_name() {
    let sandbox = Arc::new(MemorySandbox::default());
    let app = router_with_sandbox(
        Arc::new(InMemoryStore::default()),
        "http://127.0.0.1:9",
        Arc::new(OkPoster),
        sandbox.clone(),
    );
    let base = listen(app).await;
    let client = reqwest::Client::new();
    let mut names = Vec::new();
    for input in ["first box", "second box"] {
        let body = post_when_up(
            &client,
            &format!("{base}/v1/coworker/turns"),
            &turn_body("Box", serde_json::json!("PlatformGateway"), input),
        )
        .await
        .error_for_status()
        .expect("status")
        .json::<serde_json::Value>()
        .await
        .expect("json");
        let name = body["computer"]["name"].as_str().expect("name").to_string();
        let command = body["computer"]["command"].as_str().expect("command");
        assert_ne!(name, "gol-agent-box");
        assert!(command.contains(&name));
        assert!(command.contains("-v gol-workspace:/workspace"));
        assert!(!command.contains("sleep"));
        assert!(!command.split_whitespace().any(|arg| arg == "-d"));
        names.push(name);
    }
    let provisioned = sandbox.provisioned();
    assert_eq!(
        provisioned, names,
        "response names drifted from the sandbox"
    );
    assert_ne!(
        provisioned[0], provisioned[1],
        "second run reused {}",
        provisioned[0]
    );
}

#[tokio::test]
async fn the_box_sandbox_is_gone_after_the_run() {
    let store = Arc::new(InMemoryStore::default());
    let sandbox = Arc::new(MemorySandbox::default());
    let saw_live = Arc::new(AtomicBool::new(false));
    let app = router_with_sandbox(
        store.clone(),
        "http://127.0.0.1:9",
        Arc::new(BoxWatch {
            store: store.clone(),
            sandbox: sandbox.clone(),
            saw_live: saw_live.clone(),
        }),
        sandbox.clone(),
    );
    let base = listen(app).await;
    let body = post_when_up(
        &reqwest::Client::new(),
        &format!("{base}/v1/coworker/turns"),
        &turn_body("Box", serde_json::json!("PlatformGateway"), "ship the box"),
    )
    .await
    .error_for_status()
    .expect("status")
    .json::<serde_json::Value>()
    .await
    .expect("json");
    let name = body["computer"]["name"].as_str().expect("name");
    assert!(saw_live.load(Ordering::SeqCst));
    assert_eq!(name, box_container_name(parse_run_id(&body)));
    assert!(
        !sandbox.exists(name),
        "sandbox {name} was still present after the run"
    );
    assert_eq!(body["completion"], "boxed");
}

fn parse_run_id(body: &serde_json::Value) -> protocol::RunId {
    body["run_id"]
        .as_str()
        .expect("run id")
        .parse()
        .expect("run id uuid")
}

#[tokio::test]
async fn four_modes_only_let_the_server_post_in_gateway_mode() {
    let (proxy_url, paths) = proxy_server().await;
    let client = reqwest::Client::new();

    for (placement, credential, mode) in [
        ("Local", subscription(), "subscription"),
        ("Box", subscription(), "subscription"),
        ("Local", serde_json::json!("PlatformGateway"), "gateway"),
        ("Box", serde_json::json!("PlatformGateway"), "gateway"),
    ] {
        paths.lock().expect("paths").clear();
        let app = router_with_gateway(
            Arc::new(InMemoryStore::default()),
            "http://127.0.0.1:9",
            Arc::new(HttpGatewayPoster {
                url: proxy_url.clone(),
                token: "gol-gateway-local".to_string(),
            }),
        );
        let base = listen(app).await;
        let created = post_when_up(
            &client,
            &format!("{base}/v1/coworker/turns"),
            &turn_body(placement, credential, "hello from the desktop"),
        )
        .await
        .error_for_status()
        .expect("status")
        .json::<serde_json::Value>()
        .await
        .expect("json");
        assert_eq!(created["credential_mode"], mode, "{placement}");
        assert_eq!(created["user_message"], "hello from the desktop");
        let run_id = created["run_id"].as_str().expect("run id");
        let events: Vec<protocol::Event> = client
            .get(format!("{base}/v1/runs/{run_id}/events"))
            .send()
            .await
            .expect("events")
            .error_for_status()
            .expect("events status")
            .json()
            .await
            .expect("events json");
        let user_at = events
            .iter()
            .position(|event| {
                matches!(&event.payload, EventPayload::UserMessage { text } if text == "hello from the desktop")
            })
            .expect("user message");
        let seen = paths.lock().expect("paths").clone();
        if mode == "gateway" {
            assert_eq!(seen, vec!["/v1/gateway/complete".to_string()]);
            assert_eq!(created["completion"], GATEWAY_TEXT);
            let model_at = events
                .iter()
                .position(|event| {
                    matches!(&event.payload, EventPayload::ModelResponded { message } if message.text == GATEWAY_TEXT)
                })
                .expect("model");
            assert!(user_at < model_at);
            assert!(matches!(
                fold_harness(&events),
                HarnessState::Completed { .. }
            ));
            let rejected = client
                .post(format!("{base}/v1/coworker/turns/{run_id}/completion"))
                .json(&serde_json::json!({"text": "desktop tried"}))
                .send()
                .await
                .expect("reject");
            assert_eq!(rejected.status(), 409);
            assert_eq!(paths.lock().expect("paths").len(), 1);
        } else {
            assert!(
                seen.is_empty(),
                "subscription posted to the proxy: {seen:?}"
            );
            assert!(created["completion"].is_null());
            let accepted = client
                .post(format!("{base}/v1/coworker/turns/{run_id}/completion"))
                .json(&serde_json::json!({"text": "fixture assistant text"}))
                .send()
                .await
                .expect("completion")
                .error_for_status()
                .expect("completion status")
                .json::<serde_json::Value>()
                .await
                .expect("completion json");
            assert_eq!(accepted["completion"], "fixture assistant text");
            assert!(paths.lock().expect("paths").is_empty());
            let after: Vec<protocol::Event> = client
                .get(format!("{base}/v1/runs/{run_id}/events"))
                .send()
                .await
                .expect("events")
                .json()
                .await
                .expect("events json");
            let user_at = after
                .iter()
                .position(|event| matches!(event.payload, EventPayload::UserMessage { .. }))
                .expect("user");
            let model_at = after
                .iter()
                .position(|event| matches!(event.payload, EventPayload::ModelResponded { .. }))
                .expect("model");
            assert!(user_at < model_at);
        }
        if placement == "Box" {
            assert_eq!(created["computer"]["started_by"], "server");
            assert_eq!(created["computer"]["image"], "gol-agent:production");
        } else {
            assert_eq!(created["computer"]["started_by"], "desktop");
            assert_eq!(created["computer"]["image"], "gol-agent:local");
        }
        let command = created["computer"]["command"].as_str().unwrap();
        assert!(!command.to_ascii_lowercase().contains("bearer"));
        assert!(!command.contains("api."));
    }
}

fn fold_harness(events: &[protocol::Event]) -> HarnessState {
    events.iter().fold(HarnessState::Idle, |state, event| {
        protocol::reduce(state, event).0
    })
}

#[test]
fn fixture_proxy_guard_rejects_vendor_hosts() {
    assert!(ensure_fixture_proxy("https://api.anthropic.com").is_err());
    assert!(ensure_fixture_proxy("https://api.openai.com/v1").is_err());
    assert!(ensure_fixture_proxy("https://api.x.ai/v1").is_err());
    assert!(ensure_fixture_proxy("http://127.0.0.1:43124").is_ok());
}

#[test]
fn images_share_one_contract_and_name_both_placements() {
    let local = include_str!("../../../images/local/Dockerfile");
    let production = include_str!("../../../images/production/Dockerfile");
    let entry = include_str!("../../../images/agent-entrypoint.sh");
    assert!(entry.contains("does not call the inference proxy"));
    assert!(local.contains("org.gol.placement=\"local\""));
    assert!(production.contains("org.gol.placement=\"box\""));
    for source in [local, production] {
        assert!(source.contains("xfce4-session"));
        assert!(source.contains("AGENT"));
        assert!(source.contains("rustup"));
        assert!(source.contains("/workspace"));
        assert!(source.contains("agent-entrypoint.sh"));
        assert!(source.contains("does not call the inference proxy"));
        let lower = source.to_ascii_lowercase();
        assert!(!lower.contains("api_key"));
        assert!(!lower.contains("sk-"));
        assert!(!lower.contains("authorization"));
    }
}
