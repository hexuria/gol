use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::Request;
use axum::middleware::Next;
use protocol::{
    Actor, AgentId, ArtifactId, Capability, CredentialSource, DispatchPhase, Event, EventPayload,
    EventSource, ExecutionPlacement, HarnessState, Limits, MessageRole, ModelProvider, RunId,
    RunSpec, Timestamp, WorkModel,
};
use proxy::GATEWAY_TEXT;
use server::{
    accept_subscription_completion, box_container_name, box_workspace_volume, computer_plan,
    ensure_fixture_proxy, open_turn, router_with_gateway, router_with_sandbox, AgentManifest,
    Append, DockerSandbox, GatewayCall, GatewayPoster, HttpGatewayPoster, InMemoryStore,
    MemorySandbox, RunStore, SandboxHost, StoredArtifact, StoredRun, TurnError,
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
        let run_id = body["run_id"].as_str().expect("run id");
        assert_ne!(name, "gol-agent-box");
        assert!(command.contains(&name));
        let mount = format!("-v gol-workspace-{run_id}:/workspace");
        assert!(command.contains(&mount), "{command}");
        assert_eq!(
            box_workspace_volume(run_id),
            format!("gol-workspace-{run_id}")
        );
        assert!(!command.contains("-v gol-workspace:/workspace"));
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
async fn subscription_box_keeps_the_sandbox_until_the_turn_completes() {
    let store = Arc::new(InMemoryStore::default());
    let sandbox = Arc::new(MemorySandbox::default());
    let box_name = Arc::new(Mutex::new(None::<String>));
    let model_called = Arc::new(AtomicBool::new(false));
    let alive_during_model_call = Arc::new(AtomicBool::new(false));
    let expected = box_name.clone();
    let during = sandbox.clone();
    let called = model_called.clone();
    let alive = alive_during_model_call.clone();
    let proxy = proxy::router().layer(axum::middleware::from_fn(
        move |request: Request<Body>, next: Next| {
            let expected = expected.clone();
            let during = during.clone();
            let called = called.clone();
            let alive = alive.clone();
            async move {
                if request.uri().path() == "/v1/messages" {
                    let name = expected
                        .lock()
                        .expect("box name")
                        .clone()
                        .expect("model call before the box name is known");
                    called.store(true, Ordering::SeqCst);
                    alive.store(during.exists(&name), Ordering::SeqCst);
                }
                next.run(request).await
            }
        },
    ));
    let proxy_url = listen(proxy).await;
    let app = router_with_sandbox(
        store,
        "http://127.0.0.1:9",
        Arc::new(OkPoster),
        sandbox.clone(),
    );
    let base = listen(app).await;
    let client = reqwest::Client::new();
    let opened = post_when_up(
        &client,
        &format!("{base}/v1/coworker/turns"),
        &turn_body("Box", subscription(), "hold the box"),
    )
    .await
    .error_for_status()
    .expect("open")
    .json::<serde_json::Value>()
    .await
    .expect("json");
    assert!(opened["completion"].is_null());
    let name = opened["computer"]["name"]
        .as_str()
        .expect("name")
        .to_string();
    let run_id = opened["run_id"].as_str().expect("run id");
    let events = events_of(&client, &base, run_id).await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event.payload, EventPayload::RunCompleted { .. })),
        "subscription turn completed inside open_turn"
    );
    *box_name.lock().expect("box name") = Some(name.clone());

    let model = post_model(&client, &format!("{proxy_url}/v1/messages?beta=true")).await;
    model.error_for_status().expect("model status");
    assert!(
        model_called.load(Ordering::SeqCst),
        "desktop model call did not run"
    );
    assert!(
        alive_during_model_call.load(Ordering::SeqCst),
        "sandbox {name} was gone during the desktop model call"
    );

    let completed = post_when_up(
        &client,
        &format!("{base}/v1/coworker/turns/{run_id}/completion"),
        &serde_json::json!({ "text": proxy::CLAUDE_TEXT }),
    )
    .await
    .error_for_status()
    .expect("completion")
    .json::<serde_json::Value>()
    .await
    .expect("json");
    assert_eq!(completed["completion"], proxy::CLAUDE_TEXT);
    assert!(
        !sandbox.exists(&name),
        "sandbox still present after the turn completed"
    );
    let events = events_of(&client, &base, run_id).await;
    assert!(events
        .iter()
        .any(|event| matches!(event.payload, EventPayload::RunCompleted { .. })));
}

async fn post_model(client: &reqwest::Client, url: &str) -> reqwest::Response {
    let body = serde_json::json!({
        "model": "claude-fixture",
        "max_tokens": 64,
        "stream": false,
        "messages": [{ "role": "user", "content": "hold the box" }],
    });
    let mut last = None;
    for _ in 0..30 {
        match client
            .post(url)
            .header("x-api-key", "gol-desktop-fixture")
            .header("anthropic-version", "2023-06-01")
            .json(&body)
            .send()
            .await
        {
            Ok(response) => return response,
            Err(error) => {
                last = Some(error);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
    panic!("model call {url} failed: {last:?}");
}

struct FailingRemove {
    live: Mutex<HashSet<String>>,
}

impl FailingRemove {
    fn call(&self, args: &[String]) -> Result<(), String> {
        let name = docker_target(args);
        match args.first().map(String::as_str) {
            Some("create") => {
                self.live.lock().expect("docker").insert(name);
                Ok(())
            }
            Some("start") => {
                if self.live.lock().expect("docker").contains(&name) {
                    Ok(())
                } else {
                    Err(format!("container {name} was not created"))
                }
            }
            Some("rm") => Err(format!("docker rm -f {name} failed")),
            Some("inspect") => {
                if self.live.lock().expect("docker").contains(&name) {
                    Ok(())
                } else {
                    Err(format!("container {name} is gone"))
                }
            }
            other => Err(format!("unexpected docker {other:?}")),
        }
    }
}

fn workspace_mount_arg(args: &[String]) -> String {
    args.windows(2)
        .find(|pair| pair[0] == "-v")
        .map(|pair| pair[1].clone())
        .expect("docker create is missing a workspace mount")
}

fn docker_target(args: &[String]) -> String {
    if let Some(index) = args.iter().position(|arg| arg == "--name") {
        return args.get(index + 1).cloned().unwrap_or_default();
    }
    args.last().cloned().unwrap_or_default()
}

#[test]
fn a_failed_sandbox_destroy_does_not_complete_the_turn() {
    let state = Arc::new(FailingRemove {
        live: Mutex::new(HashSet::new()),
    });
    let command = state.clone();
    let sandbox = DockerSandbox::from_command(move |args| command.call(args));
    let store = InMemoryStore::default();
    let spec = RunSpec::builder()
        .agent(AgentId::new(), "1")
        .input("ship the box")
        .placement(ExecutionPlacement::Box)
        .work_model(WorkModel {
            provider: ModelProvider::Anthropic,
            model_name: "claude-fixture".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .capabilities(vec![Capability::new("model.call")])
        .limits(Limits {
            max_steps: 8,
            max_model_calls: 4,
        })
        .build();
    let error =
        open_turn(&store, spec.clone(), &OkPoster, &sandbox).expect_err("rm must fail the turn");
    match error {
        TurnError::Sandbox(message) => assert!(message.contains("rm"), "{message}"),
        other => panic!("expected sandbox error, got {other:?}"),
    }
    let stored = store.run(spec.run_id).expect("run stored");
    assert!(
        !stored
            .events
            .iter()
            .any(|event| matches!(event.payload, EventPayload::RunCompleted { .. })),
        "failed destroy still marked the turn complete"
    );
    assert!(sandbox.exists(&box_container_name(spec.run_id)));
}

fn box_gateway_spec(input: &str) -> RunSpec {
    RunSpec::builder()
        .agent(AgentId::new(), "1")
        .input(input)
        .placement(ExecutionPlacement::Box)
        .work_model(WorkModel {
            provider: ModelProvider::Anthropic,
            model_name: "claude-fixture".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .capabilities(vec![Capability::new("model.call")])
        .limits(Limits {
            max_steps: 8,
            max_model_calls: 4,
        })
        .build()
}

#[test]
fn two_box_runs_do_not_share_a_workspace_volume() {
    let mounts = Arc::new(Mutex::new(Vec::new()));
    let record = mounts.clone();
    let sandbox = DockerSandbox::from_command(move |args| {
        if args.first().map(String::as_str) == Some("create") {
            record
                .lock()
                .expect("mounts")
                .push(workspace_mount_arg(args));
        }
        Ok(())
    });

    let mut expected = Vec::new();
    for input in ["first box", "second box"] {
        let spec = box_gateway_spec(input);
        let outcome =
            open_turn(&InMemoryStore::default(), spec.clone(), &OkPoster, &sandbox).expect("turn");
        let volume = format!("gol-workspace-{}", spec.run_id);
        let mount = format!("{volume}:/workspace");
        assert_eq!(box_workspace_volume(spec.run_id), volume);
        assert!(
            outcome.computer.command.contains(&format!("-v {mount}")),
            "{}",
            outcome.computer.command
        );
        assert!(
            !outcome
                .computer
                .command
                .contains("-v gol-workspace:/workspace"),
            "shared workspace mounted: {}",
            outcome.computer.command
        );
        expected.push(mount);
    }

    let mounts = mounts.lock().expect("mounts");
    assert_eq!(mounts.len(), 2, "each run must mount a workspace");
    assert_ne!(
        mounts[0], mounts[1],
        "two runs shared workspace volume {}",
        mounts[0]
    );
    assert_eq!(&mounts[0], &expected[0]);
    assert_eq!(&mounts[1], &expected[1]);
    assert_ne!(mounts[0], "gol-workspace:/workspace");
    assert_ne!(mounts[1], "gol-workspace:/workspace");

    let local_a = protocol::RunId::new();
    let local_b = protocol::RunId::new();
    let local_mounts = [local_a, local_b].map(|run_id| {
        let command = computer_plan(ExecutionPlacement::Local, run_id).command;
        let mount = format!("gol-workspace-{run_id}:/workspace");
        assert!(command.contains(&format!("-v {mount}")), "{command}");
        assert!(
            !command.contains("-v gol-workspace:/workspace"),
            "{command}"
        );
        mount
    });
    assert_ne!(
        local_mounts[0], local_mounts[1],
        "two local runs shared workspace volume {}",
        local_mounts[0]
    );
}

async fn events_of(client: &reqwest::Client, base: &str, run_id: &str) -> Vec<protocol::Event> {
    client
        .get(format!("{base}/v1/runs/{run_id}/events"))
        .header("authorization", "Bearer gol-gateway-local")
        .send()
        .await
        .expect("events")
        .error_for_status()
        .expect("events status")
        .json()
        .await
        .expect("events json")
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
            .header("authorization", "Bearer gol-gateway-local")
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
                .header("authorization", "Bearer gol-gateway-local")
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
                .header("authorization", "Bearer gol-gateway-local")
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
                .header("authorization", "Bearer gol-gateway-local")
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

const LATE_USER_MESSAGE: &str = "note arrived after the snapshot";

/// Another writer stores `payload` right after the first `run` snapshot is taken.
struct WriteAfterSnapshot {
    inner: InMemoryStore,
    pending: Mutex<Option<EventPayload>>,
}

impl WriteAfterSnapshot {
    fn new(inner: InMemoryStore, payload: EventPayload) -> Self {
        Self {
            inner,
            pending: Mutex::new(Some(payload)),
        }
    }

    fn late_user_message(inner: InMemoryStore) -> Self {
        Self::new(
            inner,
            EventPayload::UserMessage {
                text: LATE_USER_MESSAGE.to_string(),
            },
        )
    }
}

struct SilentPoster;

impl GatewayPoster for SilentPoster {
    fn complete(&self, _call: &GatewayCall) -> Result<String, String> {
        Err("subscription must not post".to_string())
    }
}

impl RunStore for WriteAfterSnapshot {
    fn put_agent(&self, agent: AgentManifest) {
        self.inner.put_agent(agent);
    }

    fn put_run(&self, run: StoredRun) {
        self.inner.put_run(run);
    }

    fn append_events(&self, id: RunId, events: Vec<Event>) -> Append {
        self.inner.append_events(id, events)
    }

    fn run(&self, id: RunId) -> Option<StoredRun> {
        let snapshot = self.inner.run(id)?;
        if let Some(payload) = self.pending.lock().expect("pending").take() {
            let late = Event::record(
                EventSource::new(
                    snapshot.spec.run_id,
                    snapshot.spec.agent_id,
                    &snapshot.spec.agent_version,
                    Actor::System,
                    Timestamp::now(),
                ),
                payload,
            );
            assert_eq!(self.inner.append_events(id, vec![late]), Append::Appended);
        }
        Some(snapshot)
    }

    fn put_artifact(&self, artifact: StoredArtifact) {
        self.inner.put_artifact(artifact);
    }

    fn artifact(&self, id: ArtifactId) -> Option<StoredArtifact> {
        self.inner.artifact(id)
    }
}

#[test]
fn an_event_stored_after_run_returns_stays_ahead_of_the_completion() {
    let spec = RunSpec::builder()
        .agent(AgentId::new(), "1")
        .input("hello from the desktop")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::Anthropic,
            model_name: "claude-fixture".to_string(),
            credential: CredentialSource::BringYourOwn {
                secret_ref: "desktop-subscription".to_string(),
            },
        })
        .build();
    assert_ne!(LATE_USER_MESSAGE, spec.input);
    let inner = InMemoryStore::default();
    let opened = open_turn(
        &inner,
        spec.clone(),
        &SilentPoster,
        &MemorySandbox::default(),
    )
    .expect("open");
    assert!(opened.completion.is_none());
    assert_eq!(opened.credential_mode, "subscription");
    let prefix_ids: Vec<_> = opened
        .events
        .iter()
        .map(|event| event.envelope.event_id)
        .collect();
    assert_eq!(prefix_ids.len(), 3);
    let store = WriteAfterSnapshot::late_user_message(inner);
    let outcome = accept_subscription_completion(
        &store,
        spec.run_id,
        "  fixture assistant text  ",
        &MemorySandbox::default(),
    )
    .expect("completion");
    assert_eq!(
        outcome.completion.as_deref(),
        Some("fixture assistant text")
    );
    assert_eq!(outcome.credential_mode, "subscription");
    assert_eq!(outcome.events.len(), 5);
    assert_eq!(outcome.events[0].envelope.event_id, prefix_ids[0]);
    assert_eq!(outcome.events[1].envelope.event_id, prefix_ids[1]);
    assert_eq!(outcome.events[2].envelope.event_id, prefix_ids[2]);
    assert!(
        !outcome.events.iter().any(|event| {
            matches!(&event.payload, EventPayload::UserMessage { text } if text == LATE_USER_MESSAGE)
        }),
        "the returned log is the loaded prefix plus the two completion events"
    );

    let stored = store.run(spec.run_id).expect("stored run");
    assert_eq!(stored.spec, spec);
    let events = &stored.events;
    assert_eq!(events.len(), 6);
    assert_eq!(events[0].envelope.event_id, prefix_ids[0]);
    assert!(matches!(events[0].payload, EventPayload::RunCreated));
    assert_eq!(events[1].envelope.event_id, prefix_ids[1]);
    assert!(matches!(events[1].payload, EventPayload::RunStarted));
    assert_eq!(events[2].envelope.event_id, prefix_ids[2]);
    assert!(matches!(
        &events[2].payload,
        EventPayload::UserMessage { text } if text == &spec.input
    ));
    assert!(matches!(
        &events[3].payload,
        EventPayload::UserMessage { text } if text == LATE_USER_MESSAGE
    ));
    assert!(matches!(
        &events[4].payload,
        EventPayload::ModelResponded { message }
            if message.role == MessageRole::Assistant && message.text == "fixture assistant text"
    ));
    assert!(matches!(
        &events[5].payload,
        EventPayload::RunCompleted { outcome } if outcome == "fixture assistant text"
    ));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event.payload, EventPayload::RunCreated))
            .count(),
        1
    );
    let folded = protocol::fold(&stored.spec, events);
    assert_eq!(
        folded.harness,
        HarnessState::Completed {
            outcome: "fixture assistant text".to_string(),
        }
    );
    assert_eq!(
        folded.dispatch,
        DispatchPhase::Completed {
            outcome: "fixture assistant text".to_string(),
        }
    );
}

fn fold_harness(events: &[protocol::Event]) -> HarnessState {
    let spec = RunSpec::builder()
        .agent(AgentId::new(), "1")
        .input("hello")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .limits(Limits {
            max_steps: 8,
            max_model_calls: 4,
        })
        .build();
    events.iter().fold(HarnessState::Idle, |state, event| {
        protocol::reduce(state, event, &spec).0
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

fn subscription_spec(placement: ExecutionPlacement) -> RunSpec {
    RunSpec::builder()
        .agent(AgentId::new(), "1")
        .input("hello from the desktop")
        .placement(placement)
        .work_model(WorkModel {
            provider: ModelProvider::Anthropic,
            model_name: "claude-fixture".to_string(),
            credential: CredentialSource::BringYourOwn {
                secret_ref: "desktop-subscription".to_string(),
            },
        })
        .build()
}

fn completions(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match &event.payload {
            EventPayload::RunCompleted { outcome } => Some(outcome.clone()),
            _ => None,
        })
        .collect()
}

// formal/runlog AckedDurable: the old rollback wrote the pre-completion snapshot back
// over the row and dropped a user message another writer had stored in between.
#[test]
fn a_failed_destroy_keeps_a_message_stored_during_the_turn() {
    let state = Arc::new(FailingRemove {
        live: Mutex::new(HashSet::new()),
    });
    let command = state.clone();
    let sandbox = DockerSandbox::from_command(move |args| command.call(args));
    let spec = subscription_spec(ExecutionPlacement::Box);
    let inner = InMemoryStore::default();
    open_turn(&inner, spec.clone(), &SilentPoster, &sandbox).expect("open");
    let store = WriteAfterSnapshot::late_user_message(inner);

    let error = accept_subscription_completion(&store, spec.run_id, "done", &sandbox)
        .expect_err("rm must fail the completion");

    assert!(matches!(error, TurnError::Sandbox(_)), "{error:?}");
    let events = store.run(spec.run_id).expect("run").events;
    assert_eq!(events.len(), 4);
    assert!(matches!(
        &events[3].payload,
        EventPayload::UserMessage { text } if text == LATE_USER_MESSAGE
    ));
    assert_eq!(completions(&events), Vec::<String>::new());
    assert!(sandbox.exists(&box_container_name(spec.run_id)));
}

// formal/runlog AtMostOneTerminal: two completions that both read an open turn
// each appended a RunCompleted.
#[test]
fn a_completion_racing_another_completion_is_a_conflict() {
    let spec = subscription_spec(ExecutionPlacement::Local);
    let inner = InMemoryStore::default();
    open_turn(
        &inner,
        spec.clone(),
        &SilentPoster,
        &MemorySandbox::default(),
    )
    .expect("open");
    let store = WriteAfterSnapshot::new(
        inner,
        EventPayload::RunCompleted {
            outcome: "the other writer".to_string(),
        },
    );

    let error = accept_subscription_completion(
        &store,
        spec.run_id,
        "this writer",
        &MemorySandbox::default(),
    )
    .expect_err("the second completion is refused");

    assert!(
        matches!(error, TurnError::Conflict("turn already completed")),
        "{error:?}"
    );
    let events = store.run(spec.run_id).expect("run").events;
    assert_eq!(completions(&events), vec!["the other writer".to_string()]);
    assert_eq!(events.len(), 4);
}

// A completion answers an open coworker turn. `create_run`'s queued path stores a run
// with only its user message, so the harness is idle; a run waiting on a tool has not
// answered. Neither is an open turn, and the log must not gain a RunCompleted.
#[test]
fn a_completion_for_a_run_that_is_not_an_open_turn_is_a_conflict() {
    let spec = subscription_spec(ExecutionPlacement::Local);
    let at = |payload| {
        Event::record(
            EventSource::new(
                spec.run_id,
                spec.agent_id,
                &spec.agent_version,
                Actor::System,
                Timestamp::unix_millis(0),
            ),
            payload,
        )
    };
    let user_message = at(EventPayload::UserMessage {
        text: spec.input.clone(),
    });
    let queued = vec![user_message.clone()];
    let waiting = vec![
        at(EventPayload::RunCreated),
        at(EventPayload::RunStarted),
        user_message,
        at(EventPayload::EffectAuthorized {
            effect: protocol::Effect::ToolCall {
                name: "echo".to_string(),
                input: "x".to_string(),
                invocation: protocol::InvocationId::new(),
            },
        }),
    ];
    let invocation = protocol::InvocationId::new();
    let answered = vec![
        at(EventPayload::RunCreated),
        at(EventPayload::RunStarted),
        at(EventPayload::UserMessage {
            text: spec.input.clone(),
        }),
        at(EventPayload::EffectAuthorized {
            effect: protocol::Effect::ToolCall {
                name: "echo".to_string(),
                input: "x".to_string(),
                invocation,
            },
        }),
        at(EventPayload::ToolResult {
            name: "echo".to_string(),
            invocation,
            step: 1,
            attempt: 0,
            output: "x".to_string(),
        }),
    ];
    for (label, events) in [
        ("queued", queued),
        ("waiting for a tool", waiting),
        ("already answered", answered),
    ] {
        let store = InMemoryStore::default();
        store.put_run(StoredRun {
            spec: spec.clone(),
            events: events.clone(),
        });

        let error =
            accept_subscription_completion(&store, spec.run_id, "done", &MemorySandbox::default())
                .expect_err(label);

        assert!(
            matches!(error, TurnError::Conflict("turn is not open")),
            "{label}: {error:?}"
        );
        let stored = store.run(spec.run_id).expect("run").events;
        assert_eq!(stored, events, "{label}");
    }
}

// formal/runlog NothingAfterTerminal and AckedDurable at the store boundary.
#[test]
fn the_store_log_grows_and_ends_at_the_first_terminal_event() {
    let spec = subscription_spec(ExecutionPlacement::Local);
    let store = InMemoryStore::default();
    let at = |payload| {
        Event::record(
            EventSource::new(
                spec.run_id,
                spec.agent_id,
                &spec.agent_version,
                Actor::System,
                Timestamp::now(),
            ),
            payload,
        )
    };
    let message = at(EventPayload::UserMessage {
        text: "first".to_string(),
    });
    let late = at(EventPayload::UserMessage {
        text: "late".to_string(),
    });
    let done = at(EventPayload::RunCompleted {
        outcome: "done".to_string(),
    });

    assert_eq!(
        store.append_events(spec.run_id, vec![message.clone()]),
        Append::Missing
    );
    store.put_run(StoredRun {
        spec: spec.clone(),
        events: vec![message.clone()],
    });
    assert_eq!(
        store.append_events(spec.run_id, vec![late.clone()]),
        Append::Appended
    );
    store.put_run(StoredRun {
        spec: spec.clone(),
        events: vec![message.clone()],
    });
    assert_eq!(
        store.append_events(spec.run_id, vec![done.clone()]),
        Append::Appended
    );
    assert_eq!(
        store.append_events(spec.run_id, vec![at(EventPayload::RunCancelled)]),
        Append::Terminal
    );

    let ids: Vec<_> = store
        .run(spec.run_id)
        .expect("run")
        .events
        .iter()
        .map(|event| event.envelope.event_id)
        .collect();
    assert_eq!(
        ids,
        vec![
            message.envelope.event_id,
            late.envelope.event_id,
            done.envelope.event_id
        ]
    );
}
