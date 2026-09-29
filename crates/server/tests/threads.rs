//! Threads and the board (Phase 3.3, decisions 47A-50A): a thread is a
//! coordinator run of an agent the caller picks, named by its session id;
//! its Complete is the quick answer, its delegations are the tasks, and a
//! follow-up is a new coordinator run in the same session. The board lists
//! a thread's runs as cards, with their state from the fold of each log. On
//! both stores; needs Postgres and Redis, as `pg_redis.rs` does. The queued
//! tests each use a Redis database of their own.
mod common;

use std::sync::Arc;

use harness::InMemory;
use protocol::{AgentId, Capability, RunId};
use serde_json::{json, Value};
use server::{
    router_with_queue, AgentManifest, InMemoryStore, PostgresStore, RedisRunQueue, RunStore,
    StoredAgent, Worker,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const POSTGRES_URL: &str = "postgres://gol:gol@127.0.0.1/gol";

type Store = Arc<dyn RunStore>;

/// A Postgres store kept for the whole binary: built on a plain thread (the
/// Postgres client refuses to start on a Tokio worker) and never dropped, so
/// no Tokio worker closes its connections either.
static POSTGRES: std::sync::OnceLock<Store> = std::sync::OnceLock::new();

fn stores() -> Vec<Store> {
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

async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(work).await.expect("blocking")
}

/// Jev answers each label once, in order, and repeats the last one.
async fn jev(labels: &[&str]) -> MockServer {
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
fn redis_url(db: u8) -> String {
    let url = format!("redis://127.0.0.1/{db}");
    let client = redis::Client::open(url.clone()).expect("redis");
    let mut connection = client.get_connection().expect("redis connection");
    redis::cmd("FLUSHDB")
        .query::<()>(&mut connection)
        .expect("flush");
    url
}

struct Server {
    base: String,
    store: Store,
    worker: Arc<Worker>,
}

async fn serve(store: Store, jev: &MockServer, db: u8) -> Server {
    let url = blocking(move || redis_url(db)).await;
    let app = router_with_queue(
        store.clone(),
        jev.uri(),
        Some(url.clone()),
        common::authenticator(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let worker = Arc::new(
        Worker::builder()
            .queue(RedisRunQueue::open(url))
            .store(store.clone())
            .memory(Arc::new(InMemory::default()))
            .jev(jev.uri())
            .build(),
    );
    Server {
        base: format!("http://{addr}"),
        store,
        worker,
    }
}

impl Server {
    /// Stores an agent of `user`'s, with `capabilities`.
    async fn agent(&self, user: &str, name: &str, capabilities: &[&str]) -> AgentId {
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
                    owner: protocol::Owner::new(common::ISSUER, user, "tenant-1"),
                })
                .expect("put agent");
        })
        .await;
        id
    }

    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        user: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        let mut request = reqwest::Client::new()
            .request(method, format!("{}{path}", self.base))
            .header("authorization", common::bearer_for(user));
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.expect("send");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    async fn get(&self, path: &str, user: &str) -> (u16, Value) {
        self.send(reqwest::Method::GET, path, user, None).await
    }

    async fn post(&self, path: &str, user: &str, body: Value) -> (u16, Value) {
        self.send(reqwest::Method::POST, path, user, Some(body))
            .await
    }

    /// Runs the next queued run to its end.
    async fn work(&self) -> Option<RunId> {
        let worker = self.worker.clone();
        blocking(move || worker.work_one().expect("work")).await
    }

    /// Starts a thread with `agent`, and returns its id and first run.
    async fn start(&self, user: &str, agent: AgentId, input: &str) -> (String, String) {
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

fn fresh_user() -> String {
    format!("user-{}", RunId::new())
}

/// The board's cards: (run, state, parent).
fn cards(board: &Value) -> Vec<(String, String, Option<String>)> {
    board["cards"]
        .as_array()
        .expect("cards")
        .iter()
        .map(|card| {
            (
                card["run_id"].as_str().expect("run").to_string(),
                card["state"].as_str().expect("state").to_string(),
                card["parent"].as_str().map(str::to_string),
            )
        })
        .collect()
}

// Case 1: the coordinator hands two asks to other agents and answers the
// question itself. Its answer is stored while both tasks are still queued,
// and the board shows the coordinator with its two tasks. The tasks run
// next, one of them delegating in turn: the board shows every run of the
// thread at any depth, and filters by state.
#[tokio::test(flavor = "multi_thread")]
async fn two_asks_and_a_question_answer_at_once() {
    for store in stores() {
        let jev = jev(&[
            "delegate:writer",
            "delegate:researcher",
            "complete",
            // The writer, then the researcher, which delegates in turn,
            // then its own task.
            "complete",
            "delegate:writer",
            "complete",
            "complete",
        ])
        .await;
        let server = serve(store, &jev, 7).await;
        let user = fresh_user();
        let coordinator = server
            .agent(&user, "coordinator", &["agent.delegate"])
            .await;
        server.agent(&user, "writer", &[]).await;
        server.agent(&user, "researcher", &["agent.delegate"]).await;
        let (thread, first) = server.start(&user, coordinator, "plan a trip").await;

        assert_eq!(
            server.work().await.map(|run| run.to_string()),
            Some(first.clone())
        );
        let (status, board) = server
            .get(&format!("/v1/threads/{thread}/board"), &user)
            .await;
        assert_eq!(status, 200, "{board}");
        let now = cards(&board);
        assert_eq!(now.len(), 3, "{board}");
        assert_eq!(now[0], (first.clone(), "completed".to_string(), None));
        assert_eq!(board["cards"][0]["outcome"], "done");
        assert_eq!(
            board["cards"][0]["children"]
                .as_array()
                .expect("children")
                .len(),
            2
        );
        for task in &now[1..] {
            assert_eq!(task.1, "queued", "{board}");
            assert_eq!(task.2.as_deref(), Some(first.as_str()));
        }

        // The writer, the researcher, and the researcher's own task.
        for _ in 0..3 {
            assert!(server.work().await.is_some());
        }
        let (_, board) = server
            .get(&format!("/v1/threads/{thread}/board"), &user)
            .await;
        let all = cards(&board);
        assert_eq!(all.len(), 4, "{board}");
        assert!(all.iter().all(|card| card.1 == "completed"), "{board}");
        let researcher = &now[2].0;
        assert!(all
            .iter()
            .any(|card| card.2.as_deref() == Some(researcher.as_str())));
        let (_, queued) = server
            .get(&format!("/v1/threads/{thread}/board?state=queued"), &user)
            .await;
        assert_eq!(cards(&queued), Vec::new());
        let (_, done) = server
            .get(
                &format!("/v1/threads/{thread}/board?state=completed"),
                &user,
            )
            .await;
        assert_eq!(cards(&done).len(), 4);
    }
}

// A follow-up is a new coordinator run of the same agent in the same thread;
// the thread lists once, with its runs counted.
#[tokio::test(flavor = "multi_thread")]
async fn a_follow_up_is_a_new_run_in_the_same_thread() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store, &jev, 8).await;
        let user = fresh_user();
        let coordinator = server.agent(&user, "coordinator", &[]).await;
        let (thread, first) = server.start(&user, coordinator, "hello").await;
        assert!(server.work().await.is_some());
        let (status, body) = server
            .post(
                &format!("/v1/threads/{thread}/messages"),
                &user,
                json!({"input": "and then?"}),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        let second = body["run"]["run_id"].as_str().expect("run").to_string();
        assert_ne!(second, first);
        assert!(server.work().await.is_some());
        let (_, board) = server
            .get(&format!("/v1/threads/{thread}/board"), &user)
            .await;
        assert_eq!(
            cards(&board),
            [
                (first, "completed".to_string(), None),
                (second, "completed".to_string(), None)
            ]
        );
        assert_eq!(board["cards"][1]["agent_id"], coordinator.to_string());
        let (status, threads) = server.get("/v1/threads", &user).await;
        assert_eq!(status, 200);
        assert_eq!(
            threads["threads"],
            json!([{
                "thread_id": thread,
                "agent_id": coordinator,
                "runs": 2,
                "started_at": threads["threads"][0]["started_at"],
            }])
        );
        // A newer thread lists first; pages take the rest.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let (newer, _) = server.start(&user, coordinator, "another").await;
        let (_, threads) = server.get("/v1/threads", &user).await;
        let listed: Vec<&str> = threads["threads"]
            .as_array()
            .expect("threads")
            .iter()
            .map(|thread| thread["thread_id"].as_str().expect("id"))
            .collect();
        assert_eq!(listed, [newer.as_str(), thread.as_str()]);
        let (_, rest) = server.get("/v1/threads?after=1&limit=1", &user).await;
        assert_eq!(rest["threads"][0]["thread_id"], thread);
    }
}

// Another principal's thread is not found, and is not listed.
#[tokio::test(flavor = "multi_thread")]
async fn another_principals_thread_is_not_found() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store, &jev, 9).await;
        let (alice, bob) = (fresh_user(), fresh_user());
        let coordinator = server.agent(&alice, "coordinator", &[]).await;
        let (thread, _) = server.start(&alice, coordinator, "hello").await;
        let board = format!("/v1/threads/{thread}/board");
        assert_eq!(server.get(&board, &bob).await.0, 404);
        let follow_up = format!("/v1/threads/{thread}/messages");
        assert_eq!(
            server
                .post(&follow_up, &bob, json!({"input": "mine now"}))
                .await
                .0,
            404
        );
        let (status, threads) = server.get("/v1/threads", &bob).await;
        assert_eq!(status, 200);
        assert_eq!(threads["threads"], json!([]));
        // A session the caller names itself is refused: a thread's session
        // is the server's to choose.
        let (status, _) = server
            .post(
                "/v1/threads",
                &alice,
                json!({
                    "agent_id": coordinator, "agent_version": "1", "input": "x",
                    "placement": "Local",
                    "work_model": {"provider": "OpenAI", "model_name": "gpt-test",
                        "credential": "PlatformGateway"},
                    "metadata": {"session_id": thread},
                }),
            )
            .await;
        assert_eq!(status, 400);
    }
}

// Decision 49A: a database whose runs table predates the thread columns gets
// them at connect, filled from each run's spec.
#[test]
fn an_old_database_gets_its_columns_backfilled() {
    let mut admin = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("connect");
    let schema = format!("p33_old_{}", std::process::id());
    let owner = protocol::Owner::new(common::ISSUER, fresh_user(), "tenant-1");
    let agent = AgentId::new();
    let run = RunId::new();
    let spec = json!({
        "run_id": run, "owner": owner, "agent_id": agent, "agent_version": "1",
        "input": "old", "placement": "Local",
        "work_model": {"provider": "OpenAI", "model_name": "gpt-test",
            "credential": "PlatformGateway"},
        "capabilities": [], "limits": {"max_steps": 4, "max_model_calls": 1},
        "metadata": {"session_id": "old-thread"},
    });
    admin
        .batch_execute(&format!(
            "drop schema if exists {schema} cascade;
             create schema {schema};
             create table {schema}.runs (id uuid primary key, spec jsonb not null);"
        ))
        .expect("old schema");
    admin
        .execute(
            &format!("insert into {schema}.runs (id, spec) values ($1, $2)"),
            &[&run.as_uuid(), &spec],
        )
        .expect("old run");
    let url = format!("{POSTGRES_URL}?options=-csearch_path%3D{schema}");
    let listed = PostgresStore::connect(&url)
        .and_then(|store| store.threads().expect("threads").threads_of(&owner, 0, 50));
    admin
        .batch_execute(&format!("drop schema {schema} cascade"))
        .expect("drop");
    let listed = listed.expect("connect and list");
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0].thread_id, "old-thread");
    assert_eq!(listed[0].agent_id, agent);
    assert_eq!(listed[0].runs, 1);
}
