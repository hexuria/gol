//! Background coworker turns (Phase 3.6, decisions 62A-67A). A gateway turn
//! outside a Box posted with `background: true` is stored queued and
//! answered 202 at once; a queue worker then runs it as a quick turn runs:
//! one gateway completion, then the completion stored. A Box turn does not
//! run in the background yet (67A). A quick turn is unchanged. On both
//! stores; needs Postgres and Redis, as `pg_redis.rs` does.
mod common;

use std::sync::{Arc, Mutex};

use common::queued::{blocking, fresh_user, jev, serve, stores, Server, Store};
use protocol::{AgentId, EventPayload, FailureClass, RunId};
use serde_json::{json, Value};
use server::{GatewayCall, GatewayPoster, InMemoryStore, MemorySandbox, RedisRunQueue};

/// A gateway that answers `answer`, and records each call.
struct Gateway {
    answer: Result<String, String>,
    calls: Mutex<Vec<GatewayCall>>,
}

impl Gateway {
    fn answering(text: &str) -> Arc<Self> {
        Arc::new(Self {
            answer: Ok(text.to_string()),
            calls: Mutex::new(Vec::new()),
        })
    }

    fn calls(&self) -> usize {
        self.calls.lock().expect("calls").len()
    }
}

impl GatewayPoster for Gateway {
    fn complete(&self, call: &GatewayCall) -> Result<String, String> {
        self.calls.lock().expect("calls").push(call.clone());
        self.answer.clone()
    }
}

fn turn(placement: &str, credential: Value, background: Option<bool>, session: &str) -> Value {
    let mut body = json!({
        "agent_id": AgentId::new(),
        "agent_version": "1",
        "input": "draft the memo",
        "placement": placement,
        "work_model": {"provider": "OpenAI", "model_name": "gpt-test", "credential": credential},
        "metadata": {"session_id": session},
    });
    if let Some(background) = background {
        body["background"] = json!(background);
    }
    body
}

fn platform() -> Value {
    json!("PlatformGateway")
}

async fn payloads(store: &Store, run: &str) -> Vec<EventPayload> {
    let (store, run): (Store, RunId) = (store.clone(), run.parse().expect("id"));
    blocking(move || {
        store
            .run(run)
            .expect("read")
            .expect("stored")
            .events
            .into_iter()
            .map(|event| event.payload)
            .collect()
    })
    .await
}

fn kinds(payloads: &[EventPayload]) -> Vec<&'static str> {
    payloads.iter().map(EventPayload::event_type).collect()
}

async fn post_turn(server: &Server, user: &str, body: Value) -> (u16, Value) {
    server.post("/v1/coworker/turns", user, body).await
}

/// The first card of `session`'s board.
async fn first_card(server: &Server, user: &str, session: &str) -> Value {
    let (status, board) = server
        .get(&format!("/v1/threads/{session}/board"), user)
        .await;
    assert_eq!(status, 200, "{board}");
    board["cards"][0].clone()
}

// A turn without `background`, or with it false, is a quick turn as before:
// answered in the request, and nothing is queued. (A subscription turn, so
// no gateway is called.)
#[tokio::test(flavor = "multi_thread")]
async fn a_quick_turn_is_unchanged() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store.clone(), &jev, 1).await;
        let user = fresh_user();
        let subscription = json!({"BringYourOwn": {"secret_ref": "desktop"}});
        for background in [None, Some(false)] {
            let (status, body) = post_turn(
                &server,
                &user,
                turn("Local", subscription.clone(), background, "s"),
            )
            .await;
            assert_eq!(status, 200, "{body}");
            assert_eq!(body["credential_mode"], "subscription");
            let run = body["run_id"].as_str().expect("run").to_string();
            assert_eq!(
                kinds(&payloads(&store, &run).await),
                ["run.created", "run.started", "message.user"]
            );
        }
        assert_eq!(server.work().await, None, "nothing is queued");
    }
}

// A background turn is answered 202 at once, queued; it is a card on its
// session's board, and once a worker runs it, completed with the
// gateway's text.
#[tokio::test(flavor = "multi_thread")]
async fn a_background_turn_is_a_task_on_the_board() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let gateway = Gateway::answering("the memo");
        let server = serve(store.clone(), &jev, 2)
            .await
            .with_turns(gateway.clone(), Arc::new(MemorySandbox::default()));
        let user = fresh_user();
        let session = format!("s-{}", RunId::new());
        let (status, body) = post_turn(
            &server,
            &user,
            turn("Local", platform(), Some(true), &session),
        )
        .await;
        assert_eq!(status, 202, "{body}");
        assert_eq!(body["state"], "queued");
        assert_eq!(body["placement"], "Local");
        let run = body["run_id"].as_str().expect("run").to_string();
        assert_eq!(gateway.calls(), 0, "nothing runs in the request");
        let card = first_card(&server, &user, &session).await;
        assert_eq!(
            (card["run_id"].as_str(), card["state"].as_str()),
            (Some(run.as_str()), Some("queued"))
        );
        assert!(server.work().await.is_some());
        let card = first_card(&server, &user, &session).await;
        assert_eq!(card["state"], "completed", "{card}");
        assert_eq!(gateway.calls(), 1);
    }
}

// The worker runs the turn as a quick turn: stored queued with its user
// message, then the ladder, one gateway call with the turn's input, and
// the completion.
#[tokio::test(flavor = "multi_thread")]
async fn a_background_turn_completes_on_the_worker() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let gateway = Gateway::answering("the memo");
        let server = serve(store.clone(), &jev, 3)
            .await
            .with_turns(gateway.clone(), Arc::new(MemorySandbox::default()));
        let user = fresh_user();
        let (_, body) = post_turn(&server, &user, turn("Local", platform(), Some(true), "s")).await;
        let run = body["run_id"].as_str().expect("run").to_string();
        assert!(server.work().await.is_some());
        let log = payloads(&store, &run).await;
        assert_eq!(
            kinds(&log),
            [
                "run.created",
                "run.queued",
                "message.user",
                "run.scheduled",
                "run.provisioning",
                "run.starting",
                "run.started",
                "model.responded",
                "run.completed"
            ]
        );
        let calls = gateway.calls.lock().expect("calls").clone();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].input, "draft the memo");
        assert_eq!(calls[0].run_id.to_string(), run);
        assert!(matches!(
            &log[7],
            EventPayload::ModelResponded { message, .. } if message.text == "the memo"
        ));
        assert_eq!(server.work().await, None);
    }
}

// A gateway failure fails the turn, as a quick turn's does.
#[tokio::test(flavor = "multi_thread")]
async fn a_gateway_failure_fails_a_background_turn() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let gateway = Arc::new(Gateway {
            answer: Err("the proxy is down".to_string()),
            calls: Mutex::new(Vec::new()),
        });
        let server = serve(store.clone(), &jev, 6)
            .await
            .with_turns(gateway, Arc::new(MemorySandbox::default()));
        let user = fresh_user();
        let (_, body) = post_turn(&server, &user, turn("Local", platform(), Some(true), "s")).await;
        let run = body["run_id"].as_str().expect("run").to_string();
        assert!(server.work().await.is_some());
        let ends: Vec<EventPayload> = payloads(&store, &run)
            .await
            .into_iter()
            .filter(server::is_terminal)
            .collect();
        assert!(
            matches!(
                ends.as_slice(),
                [EventPayload::RunFailed { class: FailureClass::Dependency, message }]
                    if message == "proxy: the proxy is down"
            ),
            "{ends:?}"
        );
    }
}

// A stopped background turn never calls the gateway (66A): the stop cancels
// it queued, and the worker acknowledges it.
#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_background_turn_never_calls_the_gateway() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let gateway = Gateway::answering("the memo");
        let server = serve(store.clone(), &jev, 7)
            .await
            .with_turns(gateway.clone(), Arc::new(MemorySandbox::default()));
        let user = fresh_user();
        let (_, body) = post_turn(&server, &user, turn("Local", platform(), Some(true), "s")).await;
        let run = body["run_id"].as_str().expect("run").to_string();
        let (status, body) = server
            .post(&format!("/v1/runs/{run}/stop"), &user, json!({}))
            .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["cancelled"], json!([run]));
        assert!(server.work().await.is_some());
        assert_eq!(gateway.calls(), 0);
    }
}

// A redelivered turn (65A): a worker died after the turn started. The next
// worker runs it from there, calling the gateway once, and a further
// delivery changes nothing: one terminal event.
#[tokio::test(flavor = "multi_thread")]
async fn a_redelivered_turn_ends_once() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let gateway = Gateway::answering("the memo");
        let server = serve(store.clone(), &jev, 8)
            .await
            .with_turns(gateway.clone(), Arc::new(MemorySandbox::default()));
        let user = fresh_user();
        let (_, body) = post_turn(&server, &user, turn("Local", platform(), Some(true), "s")).await;
        let run = body["run_id"].as_str().expect("run").to_string();
        let id: RunId = run.parse().expect("id");
        // What the dead worker left: the turn scheduled and started.
        let runs = store.clone();
        blocking(move || {
            let spec = runs.run(id).expect("read").expect("stored").spec;
            let started = [
                EventPayload::RunScheduled,
                EventPayload::RunProvisioning,
                EventPayload::RunStarting,
                EventPayload::RunStarted,
            ]
            .into_iter()
            .map(|payload| {
                protocol::Event::record(
                    protocol::EventSource::for_spec(
                        &spec,
                        protocol::Actor::System,
                        protocol::Timestamp::now(),
                    ),
                    payload,
                )
            })
            .collect();
            assert_eq!(
                runs.append_events(id, started),
                Ok(server::Append::Appended)
            );
        })
        .await;
        assert!(server.work().await.is_some());
        assert_eq!(gateway.calls(), 1);
        let url = server.redis.clone();
        blocking(move || RedisRunQueue::open(url).push(id).expect("push")).await;
        assert!(server.work().await.is_some());
        assert_eq!(gateway.calls(), 1, "a finished turn is not run again");
        let log = payloads(&store, &run).await;
        assert_eq!(
            log.iter()
                .filter(|payload| server::is_terminal(payload))
                .count(),
            1
        );
        assert_eq!(kinds(&log).last(), Some(&"run.completed"));
        assert_eq!(
            kinds(&log)
                .iter()
                .filter(|kind| **kind == "run.started")
                .count(),
            1,
            "started once"
        );
    }
}

// A thread a background turn began takes follow-ups: the follow-up is a
// run, not a turn (the server's marker is not copied), so it is not refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_thread_begun_by_a_background_turn_takes_a_follow_up() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store.clone(), &jev, 11).await.with_turns(
            Gateway::answering("the memo"),
            Arc::new(MemorySandbox::default()),
        );
        let user = fresh_user();
        let session = format!("s-{}", RunId::new());
        let agent = server.agent(&user, "solo", &[]).await;
        let mut body = turn("Local", platform(), Some(true), &session);
        body["agent_id"] = json!(agent);
        let (status, _) = post_turn(&server, &user, body).await;
        assert_eq!(status, 202);
        let (status, body) = server
            .post(
                &format!("/v1/threads/{session}/messages"),
                &user,
                json!({"input": "and again"}),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        let follow_up = body["run"]["run_id"].as_str().expect("run").to_string();
        while server.work().await.is_some() {}
        let log = payloads(&store, &follow_up).await;
        assert!(
            kinds(&log).contains(&"effect.decided"),
            "run through the harness, not as a turn: {:?}",
            kinds(&log)
        );
        assert_eq!(kinds(&log).last(), Some(&"run.completed"));
    }
}

// What a background turn refuses: a subscription credential (63A, the
// desktop makes that call itself), and a server without a queue (64A).
// The turn marker is the server's: a caller that sends it is refused (62A),
// on a turn or a run.
#[tokio::test(flavor = "multi_thread")]
async fn a_background_turn_is_refused_where_it_cannot_run() {
    let jev = jev(&["complete"]).await;
    let store: Store = Arc::new(InMemoryStore::default());
    let server = serve(store.clone(), &jev, 9).await;
    let user = fresh_user();
    let subscription = json!({"BringYourOwn": {"secret_ref": "desktop"}});
    let (status, body) =
        post_turn(&server, &user, turn("Local", subscription, Some(true), "s")).await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body["error"],
        "a subscription turn cannot run in the background"
    );
    let (status, body) = post_turn(&server, &user, turn("Box", platform(), Some(true), "s")).await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["error"], "a Box turn cannot run in the background yet");
    let mut marked = turn("Local", platform(), Some(true), "s");
    marked["metadata"]["gol.turn"] = json!("1");
    let (status, body) = post_turn(&server, &user, marked).await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["error"], "gol.turn is set by the server");
    let agent = server.agent(&user, "solo", &[]).await;
    let (status, body) = server
        .post(
            "/v1/runs",
            &user,
            json!({
                "agent_id": agent, "agent_version": "1", "input": "x", "placement": "Local",
                "work_model": {"provider": "OpenAI", "model_name": "gpt-test",
                    "credential": "PlatformGateway"},
                "metadata": {"gol.turn": "1"},
            }),
        )
        .await;
    assert_eq!(status, 400, "{body}");

    let app = server::router(store, jev.uri(), common::authenticator());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let response = reqwest::Client::new()
        .post(format!("http://{addr}/v1/coworker/turns"))
        .header("authorization", common::bearer_for(&user))
        .json(&turn("Local", platform(), Some(true), "s"))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status().as_u16(), 503);
    // A request the server would refuse anyway is 400 first.
    let response = reqwest::Client::new()
        .post(format!("http://{addr}/v1/coworker/turns"))
        .header("authorization", common::bearer_for(&user))
        .json(&turn("Box", platform(), Some(true), "s"))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status().as_u16(), 400);
}

/// What another writer does once a worker starts a turn.
#[derive(Clone, Copy)]
enum AfterStart {
    /// Ends it: a worker whose lease lapsed finishing the same turn.
    End,
    /// Stops it: a stop whose cancel found the log moved by the start.
    Stop,
    /// Nothing, but the store then cannot take the turn's completion.
    RefuseCompletion,
}

/// An in-memory store where, once a worker starts a turn, another writer
/// does `then` at once.
struct EndedAfterStart {
    inner: InMemoryStore,
    then: AfterStart,
}

impl server::RunStore for EndedAfterStart {
    fn put_agent(
        &self,
        agent: server::StoredAgent,
    ) -> Result<server::PutAgent, server::StoreError> {
        self.inner.put_agent(agent)
    }
    fn agent(&self, id: AgentId) -> Result<Option<server::StoredAgent>, server::StoreError> {
        self.inner.agent(id)
    }
    fn agents_of(
        &self,
        owner: &protocol::Owner,
    ) -> Result<Vec<server::StoredAgent>, server::StoreError> {
        self.inner.agents_of(owner)
    }
    fn put_run(&self, run: server::StoredRun) -> Result<server::PutRun, server::StoreError> {
        self.inner.put_run(run)
    }
    fn append_events(
        &self,
        id: RunId,
        events: Vec<protocol::Event>,
    ) -> Result<server::Append, server::StoreError> {
        let completes = events
            .iter()
            .any(|event| matches!(event.payload, EventPayload::RunCompleted { .. }));
        if completes && matches!(self.then, AfterStart::RefuseCompletion) {
            return Err(server::StoreError::new("the store is unreachable"));
        }
        self.inner.append_events(id, events)
    }
    fn append_events_after(
        &self,
        id: RunId,
        seen: usize,
        events: Vec<protocol::Event>,
    ) -> Result<server::Append, server::StoreError> {
        let starts = events
            .iter()
            .any(|event| matches!(event.payload, EventPayload::RunStarted));
        let appended = self.inner.append_events_after(id, seen, events)?;
        if starts && appended == server::Append::Appended {
            let spec = self.inner.run(id)?.expect("stored").spec;
            if let AfterStart::RefuseCompletion = self.then {
                return Ok(appended);
            }
            if let AfterStart::Stop = self.then {
                let stops = self.inner.stops().expect("stops");
                stops.put_stop(&spec.owner, &server::StopScope::Run(id))?;
                return Ok(appended);
            }
            let end = protocol::Event::record(
                protocol::EventSource::for_spec(
                    &spec,
                    protocol::Actor::Gateway,
                    protocol::Timestamp::now(),
                ),
                EventPayload::RunCompleted {
                    outcome: "by the other worker".to_string(),
                },
            );
            assert_eq!(
                self.inner.append_events(id, vec![end]),
                Ok(server::Append::Appended)
            );
        }
        Ok(appended)
    }
    fn run(&self, id: RunId) -> Result<Option<server::StoredRun>, server::StoreError> {
        self.inner.run(id)
    }
    fn put_artifact(&self, artifact: server::StoredArtifact) -> Result<(), server::StoreError> {
        self.inner.put_artifact(artifact)
    }
    fn artifact(
        &self,
        id: protocol::ArtifactId,
    ) -> Result<Option<server::StoredArtifact>, server::StoreError> {
        self.inner.artifact(id)
    }
    fn threads(&self) -> Option<&dyn server::ThreadStore> {
        self.inner.threads()
    }
    fn stops(&self) -> Option<&dyn server::StopStore> {
        self.inner.stops()
    }
}

// A turn another worker ended just after this one started it (forced) gets
// no gateway call from this one; one terminal event.
#[tokio::test(flavor = "multi_thread")]
async fn a_turn_ended_by_another_worker_is_not_run_again() {
    let (store, _server, gateway, run) = started_then(AfterStart::End, 10).await;
    assert_eq!(gateway.calls(), 0);
    let ends: Vec<EventPayload> = payloads(&store, &run)
        .await
        .into_iter()
        .filter(server::is_terminal)
        .collect();
    assert!(
        matches!(ends.as_slice(), [EventPayload::RunCompleted { .. }]),
        "{ends:?}"
    );
}

// A stop that lands just after the worker started the turn (forced: the
// stop's own cancel would have found the log moved) is seen before the
// gateway call (66A): the turn is cancelled, the gateway never called.
#[tokio::test(flavor = "multi_thread")]
async fn a_stop_racing_the_start_cancels_before_the_gateway() {
    let (store, _server, gateway, run) = started_then(AfterStart::Stop, 12).await;
    assert_eq!(gateway.calls(), 0);
    let ends: Vec<EventPayload> = payloads(&store, &run)
        .await
        .into_iter()
        .filter(server::is_terminal)
        .collect();
    assert_eq!(ends, [EventPayload::RunCancelled]);
}

// A completion the store could not take leaves the turn open, and the
// worker does not acknowledge it, so it is delivered again.
#[tokio::test(flavor = "multi_thread")]
async fn a_completion_the_store_refused_leaves_the_turn_open() {
    let store: Store = Arc::new(EndedAfterStart {
        inner: InMemoryStore::default(),
        then: AfterStart::RefuseCompletion,
    });
    let jev = jev(&["complete"]).await;
    let gateway = Gateway::answering("the memo");
    let server = serve(store.clone(), &jev, 13)
        .await
        .with_turns(gateway.clone(), Arc::new(MemorySandbox::default()));
    let user = fresh_user();
    let (_, body) = post_turn(&server, &user, turn("Local", platform(), Some(true), "s")).await;
    let run = body["run_id"].as_str().expect("run").to_string();
    let worker = server.worker.clone();
    let worked = blocking(move || worker.work_one()).await;
    assert!(worked.is_err(), "not acknowledged: {worked:?}");
    assert_eq!(gateway.calls(), 1);
    let log = payloads(&store, &run).await;
    assert!(!log.iter().any(server::is_terminal), "{:?}", kinds(&log));
}

/// A background turn run by a worker on an `EndedAfterStart` store that does
/// `then`: (store, server, gateway, run).
async fn started_then(then: AfterStart, db: u8) -> (Store, Server, Arc<Gateway>, String) {
    let store: Store = Arc::new(EndedAfterStart {
        inner: InMemoryStore::default(),
        then,
    });
    let jev = jev(&["complete"]).await;
    let gateway = Gateway::answering("the memo");
    let server = serve(store.clone(), &jev, db)
        .await
        .with_turns(gateway.clone(), Arc::new(MemorySandbox::default()));
    let user = fresh_user();
    let (_, body) = post_turn(&server, &user, turn("Local", platform(), Some(true), "s")).await;
    let run = body["run_id"].as_str().expect("run").to_string();
    assert!(server.work().await.is_some());
    (store, server, gateway, run)
}
