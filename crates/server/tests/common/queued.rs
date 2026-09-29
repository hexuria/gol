//! A server on a Redis queue of its own, with a worker, for tests that run
//! queued runs over HTTP (Phase 3.3 threads, 3.4 stop): stores, a mocked
//! Jev, agents, requests, and the worker's next run.
use std::sync::Arc;

use harness::InMemory;
use protocol::{AgentId, Capability, RunId};
use serde_json::{json, Value};
use server::{
    router_with_queue, AgentManifest, GatewayPoster, InMemoryStore, MessageStore, PostgresStore,
    RedisRunQueue, RunStore, SandboxHost, StoredAgent, Worker,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub const POSTGRES_URL: &str = "postgres://gol:gol@127.0.0.1/gol";

pub type Store = Arc<dyn RunStore>;

/// A Postgres store kept for the whole binary: built on a plain thread (the
/// Postgres client refuses to start on a Tokio worker) and never dropped, so
/// no Tokio worker closes its connections either.
pub static POSTGRES: std::sync::OnceLock<Store> = std::sync::OnceLock::new();

pub fn stores() -> Vec<Store> {
    let postgres = POSTGRES
        .get_or_init(|| {
            std::thread::spawn(|| {
                Arc::new(PostgresStore::connect(POSTGRES_URL).expect("connect")) as Store
            })
            .join()
            .expect("thread")
        })
        .clone();
    vec![Arc::new(InMemoryStore::default()) as Store, postgres]
}

/// The same stores, each also as its messages store, for a worker that
/// delivers messages (a run that asks is parked). A Postgres store of its
/// own, kept like `POSTGRES`.
pub fn stores_with_messages() -> Vec<(Store, Arc<dyn MessageStore>)> {
    static PARKING: std::sync::OnceLock<Arc<PostgresStore>> = std::sync::OnceLock::new();
    let postgres = PARKING
        .get_or_init(|| {
            std::thread::spawn(|| Arc::new(PostgresStore::connect(POSTGRES_URL).expect("connect")))
                .join()
                .expect("thread")
        })
        .clone();
    let memory = Arc::new(InMemoryStore::default());
    vec![
        (memory.clone() as Store, memory as Arc<dyn MessageStore>),
        (postgres.clone() as Store, postgres as Arc<dyn MessageStore>),
    ]
}

pub async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(work).await.expect("blocking")
}

/// Jev answers each label once, in order, and repeats the last one.
pub async fn jev(labels: &[&str]) -> MockServer {
    let server = MockServer::start().await;
    let answer = |label: &str| {
        json!({
            "model": "jev-latest",
            "usage": {"input_tokens": 1, "output_tokens": 1},
            "answers": {"effect": {"type": "choice", "choice": label,
                "confidence": 1.0, "probabilities": {label: 1.0}}}
        })
    };
    let (last, first) = labels.split_last().expect("at least one answer");
    for label in first {
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .respond_with(ResponseTemplate::new(200).set_body_json(answer(label)))
            .up_to_n_times(1)
            .mount(&server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(answer(last)))
        .mount(&server)
        .await;
    server
}

/// The queue of Redis database `db`, emptied: this test's alone.
pub fn redis_url(db: u8) -> String {
    let url = format!("redis://127.0.0.1/{db}");
    let client = redis::Client::open(url.clone()).expect("redis");
    let mut connection = client.get_connection().expect("redis connection");
    redis::cmd("FLUSHDB")
        .query::<()>(&mut connection)
        .expect("flush");
    url
}

pub struct Server {
    pub base: String,
    pub store: Store,
    pub worker: Arc<Worker>,
    /// This server's Redis database, which its queue and worker share.
    pub redis: String,
    jev: String,
}

pub async fn serve(store: Store, jev: &MockServer, db: u8) -> Server {
    let url = blocking(move || redis_url(db)).await;
    let app = router_with_queue(
        store.clone(),
        jev.uri(),
        Some(url.clone()),
        super::authenticator(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let worker = Arc::new(
        Worker::builder()
            .queue(RedisRunQueue::open(url.clone()))
            .store(store.clone())
            .memory(Arc::new(InMemory::default()))
            .jev(jev.uri())
            .build(),
    );
    Server {
        base: format!("http://{addr}"),
        store,
        worker,
        redis: url,
        jev: jev.uri(),
    }
}

impl Server {
    /// The same server, with a worker that delivers messages through
    /// `messages`.
    pub fn with_messages(mut self, messages: Arc<dyn MessageStore>) -> Self {
        self.worker = Arc::new(
            Worker::builder()
                .queue(RedisRunQueue::open(self.redis.clone()))
                .store(self.store.clone())
                .memory(Arc::new(InMemory::default()))
                .jev(self.jev.clone())
                .messages(messages)
                .build(),
        );
        self
    }

    /// The same server, with a worker whose background coworker turns
    /// (Phase 3.6) call `poster` and run in `sandbox`.
    pub fn with_turns(
        mut self,
        poster: Arc<dyn GatewayPoster>,
        sandbox: Arc<dyn SandboxHost>,
    ) -> Self {
        self.worker = Arc::new(
            Worker::builder()
                .queue(RedisRunQueue::open(self.redis.clone()))
                .store(self.store.clone())
                .memory(Arc::new(InMemory::default()))
                .jev(self.jev.clone())
                .poster(poster)
                .sandbox(sandbox)
                .build(),
        );
        self
    }

    /// Stores an agent of `user`'s, with `capabilities`.
    pub async fn agent(&self, user: &str, name: &str, capabilities: &[&str]) -> AgentId {
        let id = AgentId::new();
        let store = self.store.clone();
        let (name, user) = (name.to_string(), user.to_string());
        let capabilities: Vec<Capability> = capabilities
            .iter()
            .map(|name| Capability::new(*name))
            .collect();
        blocking(move || {
            store
                .put_agent(StoredAgent {
                    manifest: AgentManifest {
                        id,
                        version: "1".to_string(),
                        name: name.clone(),
                        description: format!("The {name}."),
                        instructions: "Do it.".to_string(),
                        tools: Vec::new(),
                        required_capabilities: capabilities,
                    },
                    owner: protocol::Owner::new(super::ISSUER, user, "tenant-1"),
                })
                .expect("put agent");
        })
        .await;
        id
    }

    /// Stores `id` again, as `name` at `version`.
    pub async fn put_version(&self, user: &str, id: AgentId, name: &str, version: &str) -> bool {
        let store = self.store.clone();
        let (name, user, version) = (name.to_string(), user.to_string(), version.to_string());
        blocking(move || {
            store
                .put_agent(StoredAgent {
                    manifest: AgentManifest {
                        id,
                        version,
                        name: name.clone(),
                        description: format!("The {name}."),
                        instructions: "Do it.".to_string(),
                        tools: Vec::new(),
                        required_capabilities: Vec::new(),
                    },
                    owner: protocol::Owner::new(super::ISSUER, user, "tenant-1"),
                })
                .is_ok()
        })
        .await
    }

    pub async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        user: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        let mut request = reqwest::Client::new()
            .request(method, format!("{}{path}", self.base))
            .header("authorization", super::bearer_for(user));
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.expect("send");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    pub async fn get(&self, path: &str, user: &str) -> (u16, Value) {
        self.send(reqwest::Method::GET, path, user, None).await
    }

    pub async fn post(&self, path: &str, user: &str, body: Value) -> (u16, Value) {
        self.send(reqwest::Method::POST, path, user, Some(body))
            .await
    }

    /// Runs the next queued run to its end.
    pub async fn work(&self) -> Option<RunId> {
        let worker = self.worker.clone();
        blocking(move || worker.work_one().expect("work")).await
    }

    /// Starts a thread with `agent`, and returns its id and first run.
    pub async fn start(&self, user: &str, agent: AgentId, input: &str) -> (String, String) {
        let (status, body) = self
            .post(
                "/v1/threads",
                user,
                json!({
                    "agent_id": agent,
                    "agent_version": "1",
                    "input": input,
                    "placement": "Local",
                    "work_model": {"provider": "OpenAI", "model_name": "gpt-test",
                        "credential": "PlatformGateway"},
                    "limits": {"max_steps": 32, "max_model_calls": 16},
                }),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        (
            body["thread_id"].as_str().expect("thread id").to_string(),
            body["run"]["run_id"].as_str().expect("run id").to_string(),
        )
    }
}

pub fn fresh_user() -> String {
    format!("user-{}", RunId::new())
}
