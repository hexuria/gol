//! D2: the server calls the run's work model through the gateway, with the
//! platform's key for its provider from the environment (decision 21A). A
//! queued run and a run started inline both record the provider's answer and
//! the tokens it used. A provider with no key, and System One without a base
//! URL, fail the call with a Dependency failure. Needs Postgres and Redis, as
//! `queue_worker.rs` does.
mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use harness::InMemory;
use protocol::{
    AgentId, Capability, CredentialSource, EventPayload, ExecutionPlacement, FailureClass, Limits,
    ModelProvider, Owner, RunId, RunSpec, Usage, WorkModel,
};
use serde_json::json;
use server::{
    queued_events, router_with_memory, AgentManifest, InMemoryStore, ModelsConfig, PostgresStore,
    RedisRunQueue, RunStore, StoredRun, Worker,
};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const POSTGRES_URL: &str = "postgres://gol:gol@127.0.0.1/gol";
const REDIS_URL: &str = "redis://127.0.0.1/";

fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

#[test]
fn models_config_reads_keys_base_urls_and_the_timeout() {
    let config = ModelsConfig::from_env(&env(&[
        ("GOL_ANTHROPIC_API_KEY", "sk-ant"),
        ("GOL_ANTHROPIC_BASE_URL", "http://127.0.0.1:1"),
        ("GOL_OPENAI_API_KEY", ""),
        ("GOL_MODEL_TIMEOUT_SECS", "5"),
    ]))
    .expect("config");
    assert_eq!(config.api_key(ModelProvider::Anthropic), Some("sk-ant"));
    assert_eq!(
        config.base_url(ModelProvider::Anthropic),
        Some("http://127.0.0.1:1")
    );
    // An empty key is no key.
    assert_eq!(config.api_key(ModelProvider::OpenAI), None);
    assert_eq!(config.api_key(ModelProvider::Gemini), None);
    assert_eq!(config.timeout(), Duration::from_secs(5));
    assert_eq!(
        ModelsConfig::from_env(&env(&[])).expect("config").timeout(),
        Duration::from_secs(60)
    );
    // A key read from a secret file keeps its trailing newline; it is trimmed.
    assert_eq!(
        ModelsConfig::from_env(&env(&[("GOL_GEMINI_API_KEY", "sk-gem\n")]))
            .expect("config")
            .api_key(ModelProvider::Gemini),
        Some("sk-gem")
    );
    // A plain-HTTP base URL would send the key in the clear; only a local
    // host (a mock or a proxy on this machine) may use one.
    for local in [
        "http://127.0.0.1:9",
        "http://localhost:9",
        "http://[::1]:9",
        "https://api.example.com",
    ] {
        assert!(
            ModelsConfig::from_env(&env(&[("GOL_OPENAI_BASE_URL", local)])).is_ok(),
            "{local}"
        );
    }
    // Review of #71: the host is the one a URL parser finds, so userinfo that
    // looks like a local host does not make a remote host local.
    for remote in [
        "http://api.example.com",
        "ftp://x",
        "api.example.com",
        "http://127.0.0.1:9@evil.com",
        "http://localhost:9@evil.com",
        "http://[::1]:9@evil.com",
        "https://user:pass@api.example.com",
    ] {
        assert!(
            ModelsConfig::from_env(&env(&[("GOL_OPENAI_BASE_URL", remote)])).is_err(),
            "{remote}"
        );
    }
    // Debug output never shows a key.
    let debug = format!(
        "{:?}",
        ModelsConfig::default().with_key(ModelProvider::OpenAI, "sk-secret")
    );
    assert!(!debug.contains("sk-secret"), "{debug}");
    for bad in ["0", "601", "soon"] {
        assert!(
            ModelsConfig::from_env(&env(&[("GOL_MODEL_TIMEOUT_SECS", bad)])).is_err(),
            "{bad}"
        );
    }
}

/// A System One response that picks `label`.
fn answer(label: &str) -> serde_json::Value {
    json!({
        "model": "jev-latest",
        "usage": {"input_tokens": 1, "output_tokens": 1},
        "answers": {"effect": {"type": "choice", "choice": label,
            "confidence": 1.0, "probabilities": {label: 1.0}}}
    })
}

/// One Jev per store, each picking `model` once, then `complete`.
async fn jevs() -> (Vec<MockServer>, Vec<String>) {
    let servers = vec![jev().await, jev().await];
    let uris = servers.iter().map(MockServer::uri).collect();
    (servers, uris)
}

/// Jev picks `model`, then `complete`.
async fn jev() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(answer("model")))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(answer("complete")))
        .mount(&server)
        .await;
    server
}

/// Anthropic, answering "the answer" to the platform's key, having used 30
/// input and 5 output tokens.
async fn anthropic() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(header("x-api-key", "sk-ant-test"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "content": [{"type": "text", "text": "the answer"}],
            "usage": {"input_tokens": 30, "output_tokens": 5}
        })))
        .mount(&server)
        .await;
    server
}

fn spec(provider: ModelProvider) -> RunSpec {
    RunSpec::builder()
        .owner(Owner::new("https://issuer.test", "user-1", "tenant-1"))
        .agent(AgentId::new(), "1")
        .input("what is it?")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider,
            model_name: "model-test".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .capabilities(vec![Capability::new("model.call")])
        .limits(Limits {
            max_steps: 4,
            max_model_calls: 2,
        })
        .build()
}

fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::spawn(work).join().expect("thread")
}

/// Runs a queued run of `spec` once on a worker whose models come from
/// `config`, on each store with a Jev of its own (`jev_uris`, one per
/// store), and returns each stored log's payloads.
fn run_queued(
    jev_uris: Vec<String>,
    spec: RunSpec,
    config: ModelsConfig,
) -> Vec<Vec<EventPayload>> {
    blocking(move || {
        let stores: Vec<Arc<dyn RunStore>> = vec![
            Arc::new(InMemoryStore::default()),
            Arc::new(PostgresStore::connect(POSTGRES_URL).expect("connect")),
        ];
        let config = Arc::new(config);
        stores
            .into_iter()
            .zip(jev_uris)
            .map(|(store, jev_uri)| {
                let mut spec = spec.clone();
                spec.run_id = RunId::new();
                store
                    .put_run(StoredRun {
                        events: queued_events(&spec),
                        spec: spec.clone(),
                    })
                    .expect("put run");
                let key = format!("gol:test:{}", RunId::new());
                RedisRunQueue::with_key(REDIS_URL, &key)
                    .push(spec.run_id)
                    .expect("push");
                let worker = Worker::builder()
                    .queue(RedisRunQueue::with_key(REDIS_URL, &key))
                    .store(store.clone())
                    .memory(Arc::new(InMemory::default()))
                    .jev(jev_uri)
                    .models(config.clone())
                    .build();
                assert_eq!(worker.work_one().expect("work"), Some(spec.run_id));
                store
                    .run(spec.run_id)
                    .expect("read")
                    .expect("run")
                    .events
                    .into_iter()
                    .map(|event| event.payload)
                    .collect()
            })
            .collect()
    })
}

fn responded(payloads: &[EventPayload]) -> Vec<(String, Option<Usage>)> {
    payloads
        .iter()
        .filter_map(|payload| match payload {
            EventPayload::ModelResponded { message, usage } => Some((message.text.clone(), *usage)),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_queued_runs_model_call_reaches_its_provider() {
    let (_jevs, jev_uris) = jevs().await;
    let anthropic = anthropic().await;
    let config = ModelsConfig::default()
        .with_key(ModelProvider::Anthropic, "sk-ant-test")
        .with_base_url(ModelProvider::Anthropic, anthropic.uri());
    for payloads in run_queued(jev_uris, spec(ModelProvider::Anthropic), config) {
        assert_eq!(
            responded(&payloads),
            [(
                "the answer".to_string(),
                Some(Usage {
                    input_tokens: 30,
                    output_tokens: 5,
                })
            )]
        );
        assert!(matches!(
            payloads.last(),
            Some(EventPayload::RunCompleted { .. })
        ));
    }
}

// Decision 21A: no key for the provider fails the call, and the run, with a
// Dependency failure that says so.
#[tokio::test(flavor = "multi_thread")]
async fn a_provider_with_no_key_fails_the_run() {
    let (_jevs, jev_uris) = jevs().await;
    for payloads in run_queued(
        jev_uris,
        spec(ModelProvider::Anthropic),
        ModelsConfig::default(),
    ) {
        assert!(
            matches!(
                payloads.last(),
                Some(EventPayload::RunFailed { class: FailureClass::Dependency, message })
                    if message.contains("no platform key")
            ),
            "{:?}",
            payloads.last()
        );
    }
}

// System One has no public host: without GOL_SYSTEMONE_BASE_URL its key alone
// does not make it a work model.
#[tokio::test(flavor = "multi_thread")]
async fn system_one_without_a_base_url_fails_the_run() {
    let (_jevs, jev_uris) = jevs().await;
    let config = ModelsConfig::default().with_key(ModelProvider::SystemOne, "sk-s1");
    for payloads in run_queued(jev_uris, spec(ModelProvider::SystemOne), config) {
        assert!(
            matches!(
                payloads.last(),
                Some(EventPayload::RunFailed {
                    class: FailureClass::Dependency,
                    message,
                }) if message.contains("no platform key or base URL")
            ),
            "{:?}",
            payloads.last()
        );
    }
}

// A run started inline (no queue) calls its model the same way.
#[tokio::test(flavor = "multi_thread")]
async fn an_inline_runs_model_call_reaches_its_provider() {
    let jev = jev().await;
    let anthropic = anthropic().await;
    let config = ModelsConfig::default()
        .with_key(ModelProvider::Anthropic, "sk-ant-test")
        .with_base_url(ModelProvider::Anthropic, anthropic.uri());
    let app = router_with_memory(
        Arc::new(InMemoryStore::default()),
        Arc::new(InMemory::default()),
        jev.uri(),
        None,
        Arc::new(server::HttpGatewayPoster::from_env()),
        Arc::new(config),
        common::authenticator(),
    );
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
    client
        .post(format!("{base}/v1/agents"))
        .header("authorization", common::bearer())
        .json(&AgentManifest {
            id: agent_id,
            version: "1".to_string(),
            name: String::new(),
            description: String::new(),
            instructions: "Ask the model.".to_string(),
            tools: Vec::new(),
            required_capabilities: vec![Capability::new("model.call")],
        })
        .send()
        .await
        .expect("post agent")
        .error_for_status()
        .expect("agent stored");
    let created: protocol::RunState = client
        .post(format!("{base}/v1/runs"))
        .header("authorization", common::bearer())
        .json(&json!({
            "agent_id": agent_id,
            "agent_version": "1",
            "input": "what is it?",
            "placement": "Local",
            "work_model": {
                "provider": "Anthropic",
                "model_name": "model-test",
                "credential": "PlatformGateway"
            },
            "limits": {"max_steps": 4, "max_model_calls": 2}
        }))
        .send()
        .await
        .expect("post run")
        .error_for_status()
        .expect("run created")
        .json()
        .await
        .expect("run json");
    let events: Vec<protocol::Event> = client
        .get(format!("{base}/v1/runs/{}/events", created.run_id))
        .header("authorization", common::bearer())
        .send()
        .await
        .expect("get events")
        .error_for_status()
        .expect("events")
        .json()
        .await
        .expect("events json");
    let payloads: Vec<EventPayload> = events.into_iter().map(|event| event.payload).collect();
    assert_eq!(
        responded(&payloads),
        [(
            "the answer".to_string(),
            Some(Usage {
                input_tokens: 30,
                output_tokens: 5,
            })
        )]
    );
}

/// Anthropic failing every call with an error body that repeats the key it
/// was sent, as some gateways' debug pages do.
async fn anthropic_echoing_the_key() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(401).set_body_string("invalid x-api-key: sk-ant-test"))
        .mount(&server)
        .await;
    server
}

// Review of #71: the run log, which clients read, gets a fixed message for
// a failed model call. The provider's error body (which may repeat the
// platform's key) and the URL go to stderr only.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_model_call_does_not_put_the_providers_error_in_the_log() {
    let (_jevs, jev_uris) = jevs().await;
    let anthropic = anthropic_echoing_the_key().await;
    let config = ModelsConfig::default()
        .with_key(ModelProvider::Anthropic, "sk-ant-test")
        .with_base_url(ModelProvider::Anthropic, anthropic.uri());
    for payloads in run_queued(jev_uris, spec(ModelProvider::Anthropic), config) {
        let text = format!("{payloads:?}");
        assert!(!text.contains("sk-ant-test"), "{text}");
        assert!(!text.contains("127.0.0.1"), "{text}");
        assert!(
            matches!(
                payloads.last(),
                Some(EventPayload::RunFailed { class: FailureClass::Dependency, message })
                    if message == "model call to Anthropic failed"
            ),
            "{:?}",
            payloads.last()
        );
    }
}

// Decision 22A through a real run: a bring-your-own credential fails the
// run on the server.
#[tokio::test(flavor = "multi_thread")]
async fn a_bring_your_own_run_fails_on_the_server() {
    let (_jevs, jev_uris) = jevs().await;
    let mut byo = spec(ModelProvider::Anthropic);
    byo.work_model.credential = CredentialSource::BringYourOwn {
        secret_ref: "vault:alice/anthropic".to_string(),
    };
    let config = ModelsConfig::default().with_key(ModelProvider::Anthropic, "sk-ant-test");
    for payloads in run_queued(jev_uris, byo, config) {
        let text = format!("{payloads:?}");
        assert!(!text.contains("sk-ant-test"), "{text}");
        assert!(
            matches!(
                payloads.last(),
                Some(EventPayload::RunFailed { class: FailureClass::Dependency, message })
                    if message.contains("bring-your-own")
            ),
            "{:?}",
            payloads.last()
        );
    }
}
