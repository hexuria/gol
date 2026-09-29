//! A task asks you (Phase 3.5, decisions 56A-61A). A queued task that puts
//! a question to its user (`AskUser`, with `user.ask`) is parked, holding no
//! worker, until the answer comes: `POST /v1/runs/{id}/reply`, or a
//! message to its thread. There "T<n>: text" answers task T<n>, a bare
//! message answers the one task that waits, and with two or more waiting it
//! is refused with the questions. A question has no timeout; a stop ends it.
//! On both stores; needs Postgres and Redis, as `pg_redis.rs` does.
mod common;

use std::sync::{Arc, Mutex};

use common::queued::{
    blocking, fresh_user, jev, serve, stores, stores_with_messages, Server, Store,
};
use protocol::{
    Actor, AgentId, ArtifactId, Event, EventPayload, EventSource, MessageId, Owner, RunId,
    Timestamp,
};
use serde_json::{json, Value};
use server::{
    Append, InMemoryStore, PutAgent, PutRun, RedisRunQueue, RunStore, StoreError, StoredAgent,
    StoredArtifact, StoredRun,
};

async fn log(store: &Store, run: &str) -> Vec<EventPayload> {
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

/// The questions `run` put, as (id, prompt).
async fn questions(store: &Store, run: &str) -> Vec<(MessageId, String)> {
    log(store, run)
        .await
        .into_iter()
        .filter_map(|payload| match payload {
            EventPayload::UserAsked { message_id, prompt } => Some((message_id, prompt)),
            _ => None,
        })
        .collect()
}

async fn answers(store: &Store, run: &str) -> Vec<String> {
    log(store, run)
        .await
        .into_iter()
        .filter_map(|payload| match payload {
            EventPayload::UserAnswered { text, .. } => Some(text),
            _ => None,
        })
        .collect()
}

async fn completed(store: &Store, run: &str) -> bool {
    log(store, run)
        .await
        .iter()
        .any(|payload| matches!(payload, EventPayload::RunCompleted { .. }))
}

async fn parked_and_queued(server: &Server) -> (Vec<(RunId, MessageId)>, Vec<RunId>) {
    let queue = RedisRunQueue::open(server.redis.clone());
    blocking(move || {
        (
            queue.parked().expect("parked"),
            queue.queued().expect("queued"),
        )
    })
    .await
}

/// The thread's board, as (label, run, state, question).
async fn board(server: &Server, user: &str, thread: &str) -> Vec<(String, String, String, Value)> {
    let (status, board) = server
        .get(&format!("/v1/threads/{thread}/board"), user)
        .await;
    assert_eq!(status, 200, "{board}");
    board["cards"]
        .as_array()
        .expect("cards")
        .iter()
        .map(|card| {
            (
                card["label"].as_str().expect("label").to_string(),
                card["run_id"].as_str().expect("run").to_string(),
                card["state"].as_str().expect("state").to_string(),
                card["question"].clone(),
            )
        })
        .collect()
}

async fn reply(server: &Server, user: &str, run: &str, text: &str) -> (u16, Value) {
    server
        .post(
            &format!("/v1/runs/{run}/reply"),
            user,
            json!({"text": text}),
        )
        .await
}

async fn say(server: &Server, user: &str, thread: &str, input: &str) -> (u16, Value) {
    server
        .post(
            &format!("/v1/threads/{thread}/messages"),
            user,
            json!({"input": input}),
        )
        .await
}

// A task that asks is parked: the worker is free, nothing is queued, and
// its card waits with the question (the input, since it has no assistant
// text yet, 59A) under label T1.
#[tokio::test(flavor = "multi_thread")]
async fn a_waiting_task_holds_no_worker() {
    for store in stores() {
        let jev = jev(&["ask_user", "complete"]).await;
        let server = serve(store.clone(), &jev, 1).await;
        let user = fresh_user();
        let agent = server.agent(&user, "planner", &["user.ask"]).await;
        let (thread, run) = server.start(&user, agent, "plan a trip").await;
        assert!(server.work().await.is_some());
        let asked = questions(&store, &run).await;
        assert_eq!(asked.len(), 1);
        assert_eq!(asked[0].1, "plan a trip");
        let (parked, queued) = parked_and_queued(&server).await;
        assert_eq!(parked, vec![(run.parse().expect("id"), asked[0].0)]);
        assert_eq!(queued, Vec::new());
        assert_eq!(server.work().await, None, "no worker is held");
        assert_eq!(
            board(&server, &user, &thread).await,
            vec![(
                "T1".to_string(),
                run.clone(),
                "waiting".to_string(),
                json!("plan a trip")
            )]
        );
    }
}

// The reply resumes the task: it is woken, runs on with the answer in its
// log (Jev's next request carries it), and ends.
#[tokio::test(flavor = "multi_thread")]
async fn a_reply_resumes_it() {
    for store in stores() {
        let jev = jev(&["ask_user", "complete"]).await;
        let server = serve(store.clone(), &jev, 2).await;
        let user = fresh_user();
        let agent = server.agent(&user, "planner", &["user.ask"]).await;
        let (thread, run) = server.start(&user, agent, "plan a trip").await;
        assert!(server.work().await.is_some());
        let (status, body) = reply(&server, &user, &run, "SFO").await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["answered"], json!(run));
        assert_eq!(answers(&store, &run).await, ["SFO"]);
        assert!(server.work().await.is_some(), "woken");
        assert!(completed(&store, &run).await);
        let requests = jev.received_requests().await.expect("requests");
        let last: Value = serde_json::from_slice(&requests.last().expect("asked").body).unwrap();
        assert!(last["state"]["recent_events"].to_string().contains("SFO"));
        assert_eq!(board(&server, &user, &thread).await[0].3, Value::Null);
        // Once answered, it waits no more.
        let (status, _) = reply(&server, &user, &run, "again").await;
        assert_eq!(status, 409);
    }
}

// Two tasks wait in one thread: a bare message asks which (61A), with each
// task's label and question; "T2: SFO" answers the second, and then the
// one left takes a bare answer.
#[tokio::test(flavor = "multi_thread")]
async fn an_ambiguous_answer_asks_which_task() {
    for store in stores() {
        let jev = jev(&["ask_user", "ask_user", "complete"]).await;
        let server = serve(store.clone(), &jev, 3).await;
        let user = fresh_user();
        let agent = server.agent(&user, "planner", &["user.ask"]).await;
        let (thread, first) = server.start(&user, agent, "flights").await;
        // Nothing waits yet: a message is a follow-up.
        let (status, body) = say(&server, &user, &thread, "hotels").await;
        assert_eq!(status, 200, "{body}");
        let second = body["run"]["run_id"].as_str().expect("run").to_string();
        while server.work().await.is_some() {}
        let (status, body) = say(&server, &user, &thread, "SFO").await;
        assert_eq!(status, 409, "{body}");
        assert_eq!(body["error"], "which task?");
        let (q1, q2) = (
            questions(&store, &first).await[0].0,
            questions(&store, &second).await[0].0,
        );
        assert_eq!(
            body["waiting"],
            json!([
                {"label": "T1", "run_id": first, "question_id": q1, "question": "flights"},
                {"label": "T2", "run_id": second, "question_id": q2, "question": "hotels"},
            ])
        );
        // An answer takes no limits.
        let (status, body) = server
            .post(
                &format!("/v1/threads/{thread}/messages"),
                &user,
                json!({"input": "T2: SFO", "limits": {"max_steps": 4, "max_model_calls": 1}}),
            )
            .await;
        assert_eq!(status, 400, "{body}");
        let (status, body) = say(&server, &user, &thread, "T2: SFO").await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["answered"], json!({"label": "T2", "run_id": second}));
        assert_eq!(answers(&store, &second).await, ["SFO"]);
        assert_eq!(answers(&store, &first).await, Vec::<String>::new());
        // A label that does not wait is refused while one does.
        let (status, body) = say(&server, &user, &thread, "T2: again").await;
        assert_eq!(status, 409, "{body}");
        let (status, body) = say(&server, &user, &thread, "LAX").await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["answered"], json!({"label": "T1", "run_id": first}));
        assert_eq!(answers(&store, &first).await, ["LAX"]);
        while server.work().await.is_some() {}
        assert!(completed(&store, &first).await && completed(&store, &second).await);
        // Labels never change as the thread grows (60A): a third run is T3.
        let (status, body) = say(&server, &user, &thread, "trains").await;
        assert_eq!(status, 200, "{body}");
        let third = body["run"]["run_id"].as_str().expect("run").to_string();
        let labels: Vec<(String, String)> = board(&server, &user, &thread)
            .await
            .into_iter()
            .map(|(label, run, _, _)| (label, run))
            .collect();
        assert_eq!(
            labels,
            [
                ("T1".to_string(), first),
                ("T2".to_string(), second),
                ("T3".to_string(), third)
            ]
        );
    }
}

// What a reply cannot answer: a task that is not waiting (409), another
// principal's task (404), and an answer that is blank (400) or too long
// (413).
#[tokio::test(flavor = "multi_thread")]
async fn replies_that_cannot_answer_are_refused() {
    for store in stores() {
        let jev = jev(&["ask_user", "complete"]).await;
        let server = serve(store.clone(), &jev, 4).await;
        let (alice, bob) = (fresh_user(), fresh_user());
        let agent = server.agent(&alice, "planner", &["user.ask"]).await;
        let (_, run) = server.start(&alice, agent, "plan").await;
        let (status, _) = reply(&server, &alice, &run, "early").await;
        assert_eq!(status, 409, "a queued task is not waiting");
        assert!(server.work().await.is_some());
        let (status, _) = reply(&server, &bob, &run, "mine").await;
        assert_eq!(status, 404);
        let (status, body) = server
            .post(
                &format!("/v1/runs/{run}/reply"),
                &alice,
                json!({"text": "SFO", "question_id": MessageId::new()}),
            )
            .await;
        assert_eq!(status, 409, "another question: {body}");
        let (status, _) = reply(&server, &alice, &run, "   ").await;
        assert_eq!(status, 400);
        let long = "x".repeat(protocol::MAX_MESSAGE_BYTES + 1);
        let (status, _) = reply(&server, &alice, &run, &long).await;
        assert_eq!(status, 413);
        assert_eq!(answers(&store, &run).await, Vec::<String>::new());
    }
}

// The ask sweep never times a question out (57A): past any deadline the
// task still waits, parked.
#[tokio::test(flavor = "multi_thread")]
async fn the_sweep_never_times_out_a_question() {
    for (store, messages) in stores_with_messages() {
        let jev = jev(&["ask_user", "complete"]).await;
        let server = serve(store.clone(), &jev, 5)
            .await
            .with_messages(messages.clone());
        let user = fresh_user();
        let agent = server.agent(&user, "planner", &["user.ask"]).await;
        let (_, run) = server.start(&user, agent, "plan").await;
        assert!(server.work().await.is_some());
        let (runs, url) = (store.clone(), server.redis.clone());
        let swept = blocking(move || {
            let later =
                Timestamp::unix_millis(Timestamp::now().as_unix_millis() + 30 * 24 * 3600 * 1000);
            server::sweep_asks(
                &RedisRunQueue::open(url),
                runs.as_ref(),
                messages.as_ref(),
                later,
            )
            .expect("sweep")
        })
        .await;
        // The shared messages table may hold other tests' asks; this run
        // is not among those the sweep answered or woke.
        assert!(!swept.contains(&run.parse().expect("id")), "{swept:?}");
        assert!(!log(&store, &run)
            .await
            .iter()
            .any(|payload| matches!(payload, EventPayload::AskTimedOut { .. })));
        assert_eq!(parked_and_queued(&server).await.0.len(), 1);
    }
}

// A stop ends a waiting question: the task is cancelled and woken, and
// nothing stays parked.
#[tokio::test(flavor = "multi_thread")]
async fn a_stop_ends_a_waiting_question() {
    for store in stores() {
        let jev = jev(&["ask_user", "complete"]).await;
        let server = serve(store.clone(), &jev, 6).await;
        let user = fresh_user();
        let agent = server.agent(&user, "planner", &["user.ask"]).await;
        let (_, run) = server.start(&user, agent, "plan").await;
        assert!(server.work().await.is_some());
        let (status, body) = server
            .post(&format!("/v1/runs/{run}/stop"), &user, json!({}))
            .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["cancelled"], json!([run]));
        while server.work().await.is_some() {}
        assert_eq!(parked_and_queued(&server).await, (Vec::new(), Vec::new()));
        let (status, _) = reply(&server, &user, &run, "late").await;
        assert_eq!(status, 409);
    }
}

/// An in-memory store that, once armed, lands `payload` on the run just
/// before the next conditional append: another writer between a reply's read
/// and its answer.
struct Racing {
    inner: InMemoryStore,
    armed: Mutex<Vec<EventPayload>>,
    /// Records a stop of the run just after its question is stored: a stop
    /// that lands before the worker parks the run.
    stop_after_question: Mutex<bool>,
}

impl Racing {
    fn arm(&self, payloads: Vec<EventPayload>) {
        *self.armed.lock().expect("armed") = payloads;
    }
}

impl RunStore for Racing {
    fn put_agent(&self, agent: StoredAgent) -> Result<PutAgent, StoreError> {
        self.inner.put_agent(agent)
    }
    fn agent(&self, id: AgentId) -> Result<Option<StoredAgent>, StoreError> {
        self.inner.agent(id)
    }
    fn agents_of(&self, owner: &Owner) -> Result<Vec<StoredAgent>, StoreError> {
        self.inner.agents_of(owner)
    }
    fn put_run(&self, run: StoredRun) -> Result<PutRun, StoreError> {
        self.inner.put_run(run)
    }
    fn append_events(&self, id: RunId, events: Vec<Event>) -> Result<Append, StoreError> {
        self.inner.append_events(id, events)
    }
    fn append_events_after(
        &self,
        id: RunId,
        seen: usize,
        events: Vec<Event>,
    ) -> Result<Append, StoreError> {
        let armed = std::mem::take(&mut *self.armed.lock().expect("armed"));
        if !armed.is_empty() {
            let spec = self.inner.run(id)?.expect("stored").spec;
            let late = armed
                .into_iter()
                .map(|payload| {
                    Event::record(
                        EventSource::for_spec(&spec, Actor::System, Timestamp::now()),
                        payload,
                    )
                })
                .collect();
            assert_eq!(self.inner.append_events(id, late), Ok(Append::Appended));
        }
        let asks = events
            .iter()
            .any(|event| matches!(event.payload, EventPayload::UserAsked { .. }));
        let appended = self.inner.append_events_after(id, seen, events)?;
        if asks
            && appended == Append::Appended
            && std::mem::take(&mut *self.stop_after_question.lock().expect("stop"))
        {
            let owner = self.inner.run(id)?.expect("stored").spec.owner;
            let stops = self.inner.stops().expect("stops");
            stops.put_stop(&owner, &server::StopScope::Run(id))?;
        }
        Ok(appended)
    }
    fn run(&self, id: RunId) -> Result<Option<StoredRun>, StoreError> {
        self.inner.run(id)
    }
    fn put_artifact(&self, artifact: StoredArtifact) -> Result<(), StoreError> {
        self.inner.put_artifact(artifact)
    }
    fn artifact(&self, id: ArtifactId) -> Result<Option<StoredArtifact>, StoreError> {
        self.inner.artifact(id)
    }
    fn threads(&self) -> Option<&dyn server::ThreadStore> {
        self.inner.threads()
    }
    fn stops(&self) -> Option<&dyn server::StopStore> {
        self.inner.stops()
    }
}

async fn racing(db: u8) -> (Arc<Racing>, Server, String, String, MessageId) {
    let (racing, server, user, _, run, question) = racing_in_thread(db).await;
    (racing, server, user, run, question)
}

/// A server on a `Racing` store with one task parked on its question, as
/// (store, server, user, thread, run, question).
async fn racing_in_thread(db: u8) -> (Arc<Racing>, Server, String, String, String, MessageId) {
    let racing = Arc::new(Racing {
        inner: InMemoryStore::default(),
        armed: Mutex::new(Vec::new()),
        stop_after_question: Mutex::new(false),
    });
    let store: Store = racing.clone();
    let jev = jev(&["ask_user", "complete"]).await;
    let server = serve(store.clone(), &jev, db).await;
    let user = fresh_user();
    let agent = server.agent(&user, "planner", &["user.ask"]).await;
    let (thread, run) = server.start(&user, agent, "plan").await;
    assert!(server.work().await.is_some());
    let question = questions(&store, &run).await[0].0;
    (racing, server, user, thread, run, question)
}

// Two replies race (forced: the other lands between this one's read and
// its append): one answer is kept, and the late reply is told the task no
// longer waits.
#[tokio::test(flavor = "multi_thread")]
async fn two_replies_race_one_answer() {
    let (racing, server, user, run, question) = racing(7).await;
    racing.arm(vec![EventPayload::UserAnswered {
        message_id: question,
        text: "first".to_string(),
    }]);
    let (status, body) = reply(&server, &user, &run, "second").await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(answers(&server.store, &run).await, ["first"]);
}

// A reply races a stop (forced: the cancel lands between the reply's read
// and its append): the run keeps its one terminal event and no answer.
#[tokio::test(flavor = "multi_thread")]
async fn a_reply_racing_a_stop_keeps_one_outcome() {
    let (racing, server, user, run, _) = racing(8).await;
    racing.arm(vec![EventPayload::RunCancelled]);
    let (status, body) = reply(&server, &user, &run, "SFO").await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(answers(&server.store, &run).await, Vec::<String>::new());
    let ends: Vec<EventPayload> = log(&server.store, &run)
        .await
        .into_iter()
        .filter(server::is_terminal)
        .collect();
    assert_eq!(ends, [EventPayload::RunCancelled]);
}

// A run carried out inside its request cannot wait, so Jev is never offered
// the question, even with user.ask.
#[tokio::test(flavor = "multi_thread")]
async fn an_inline_run_is_not_offered_a_question() {
    let jev = jev(&["complete"]).await;
    let store: Store = Arc::new(InMemoryStore::default());
    let app = server::router(store.clone(), jev.uri(), common::authenticator());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let user = fresh_user();
    let agent = AgentId::new();
    let runs = store.clone();
    let owner = Owner::new(common::ISSUER, user.clone(), "tenant-1");
    blocking(move || {
        runs.put_agent(StoredAgent {
            manifest: server::AgentManifest {
                id: agent,
                version: "1".to_string(),
                name: "planner".to_string(),
                description: String::new(),
                instructions: "Plan.".to_string(),
                tools: Vec::new(),
                required_capabilities: vec![protocol::Capability::new("user.ask")],
            },
            owner,
        })
        .expect("put agent");
    })
    .await;
    let response = reqwest::Client::new()
        .post(format!("http://{addr}/v1/runs"))
        .header("authorization", common::bearer_for(&user))
        .json(&json!({
            "agent_id": agent,
            "agent_version": "1",
            "input": "plan",
            "placement": "Local",
            "work_model": {"provider": "OpenAI", "model_name": "gpt-test",
                "credential": "PlatformGateway"},
            "limits": {"max_steps": 8, "max_model_calls": 4},
        }))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status().as_u16(), 200);
    let requests = jev.received_requests().await.expect("requests");
    let first: Value = serde_json::from_slice(&requests[0].body).unwrap();
    let offered = first["questions"]["effect"]["criteria"]
        .as_object()
        .expect("criteria");
    assert!(offered.contains_key("complete"), "{offered:?}");
    assert!(!offered.contains_key("ask_user"), "{offered:?}");
}

// A run whose log ended while it waited (here a completion the harness
// does not take while it waits) waits on nobody: a reply is 409, its card
// shows no question, and a message to its thread is a follow-up.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_that_ended_while_waiting_asks_nothing() {
    let (racing, server, user, thread, run, _) = racing_in_thread(9).await;
    racing.arm(vec![EventPayload::RunCompleted {
        outcome: "done elsewhere".to_string(),
    }]);
    let (status, body) = reply(&server, &user, &run, "SFO").await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(answers(&server.store, &run).await, Vec::<String>::new());
    let cards = board(&server, &user, &thread).await;
    assert_eq!(cards[0].3, Value::Null, "{cards:?}");
    let (status, body) = say(&server, &user, &thread, "next").await;
    assert_eq!(status, 200, "{body}");
    assert!(body["run"]["run_id"].is_string(), "a follow-up: {body}");
}

// An answer whose wake was lost is not left parked: the ask sweep sees the
// log no longer waits and wakes the run, which then finishes.
#[tokio::test(flavor = "multi_thread")]
async fn an_answer_whose_wake_was_lost_is_woken_by_the_sweep() {
    for (store, messages) in stores_with_messages() {
        let jev = jev(&["ask_user", "complete"]).await;
        let server = serve(store.clone(), &jev, 10)
            .await
            .with_messages(messages.clone());
        let user = fresh_user();
        let agent = server.agent(&user, "planner", &["user.ask"]).await;
        let (_, run) = server.start(&user, agent, "plan").await;
        assert!(server.work().await.is_some());
        let question = questions(&store, &run).await[0].0;
        // The answer lands, as the reply appends it, and no wake follows.
        let (runs, id) = (store.clone(), run.parse::<RunId>().expect("id"));
        blocking(move || {
            let spec = runs.run(id).expect("read").expect("stored").spec;
            let answer = Event::record(
                EventSource::for_spec(&spec, Actor::System, Timestamp::now()),
                EventPayload::UserAnswered {
                    message_id: question,
                    text: "SFO".to_string(),
                },
            );
            assert_eq!(runs.append_events(id, vec![answer]), Ok(Append::Appended));
        })
        .await;
        let (runs, url) = (store.clone(), server.redis.clone());
        let swept = blocking(move || {
            server::sweep_asks(
                &RedisRunQueue::open(url),
                runs.as_ref(),
                messages.as_ref(),
                Timestamp::now(),
            )
            .expect("sweep")
        })
        .await;
        assert!(swept.contains(&id), "{swept:?}");
        assert!(server.work().await.is_some(), "woken");
        assert!(completed(&store, &run).await);
    }
}

// A stop that lands after the question is stored and before the park (so
// the stop found the run neither queued nor parked) is seen at the park:
// the run is woken, and its worker cancels it.
#[tokio::test(flavor = "multi_thread")]
async fn a_stop_before_the_park_still_ends_the_question() {
    let racing = Arc::new(Racing {
        inner: InMemoryStore::default(),
        armed: Mutex::new(Vec::new()),
        stop_after_question: Mutex::new(true),
    });
    let store: Store = racing.clone();
    let jev = jev(&["ask_user", "complete"]).await;
    let server = serve(store.clone(), &jev, 11).await;
    let user = fresh_user();
    let agent = server.agent(&user, "planner", &["user.ask"]).await;
    let (_, run) = server.start(&user, agent, "plan").await;
    assert!(
        server.work().await.is_some(),
        "asks, and is woken at the park"
    );
    assert_eq!(parked_and_queued(&server).await.0, Vec::new());
    assert!(server.work().await.is_some(), "claimed and cancelled");
    let ends: Vec<EventPayload> = log(&store, &run)
        .await
        .into_iter()
        .filter(server::is_terminal)
        .collect();
    assert_eq!(ends, [EventPayload::RunCancelled]);
}

// A parked question whose run is stopped without the stop cancelling it
// (as when the stop could not reach it) is woken by the ask sweep and
// cancelled: a stop ends a question even without a timeout.
#[tokio::test(flavor = "multi_thread")]
async fn the_sweep_wakes_a_stopped_question() {
    for (store, messages) in stores_with_messages() {
        let jev = jev(&["ask_user", "complete"]).await;
        let server = serve(store.clone(), &jev, 12)
            .await
            .with_messages(messages.clone());
        let user = fresh_user();
        let agent = server.agent(&user, "planner", &["user.ask"]).await;
        let (_, run) = server.start(&user, agent, "plan").await;
        assert!(server.work().await.is_some());
        let id: RunId = run.parse().expect("id");
        let runs = store.clone();
        blocking(move || {
            let owner = runs.run(id).expect("read").expect("stored").spec.owner;
            runs.stops()
                .expect("stops")
                .put_stop(&owner, &server::StopScope::Run(id))
                .expect("stop");
        })
        .await;
        let (runs, url) = (store.clone(), server.redis.clone());
        let swept = blocking(move || {
            server::sweep_asks(
                &RedisRunQueue::open(url),
                runs.as_ref(),
                messages.as_ref(),
                Timestamp::now(),
            )
            .expect("sweep")
        })
        .await;
        assert!(swept.contains(&id), "{swept:?}");
        assert!(server.work().await.is_some());
        assert!(log(&store, &run)
            .await
            .contains(&EventPayload::RunCancelled));
    }
}

// Between a reply's read and its append the task was answered elsewhere and
// went on to ask another question (forced): the reply, meant for the first
// question, is refused rather than taken as the answer to the second.
#[tokio::test(flavor = "multi_thread")]
async fn a_reply_does_not_answer_the_next_question() {
    let (racing, server, user, run, question) = racing(13).await;
    let next = MessageId::new();
    racing.arm(vec![
        EventPayload::UserAnswered {
            message_id: question,
            text: "from another tab".to_string(),
        },
        EventPayload::StepAdvanced,
        EventPayload::UserAsked {
            message_id: next,
            prompt: "Which day?".to_string(),
        },
    ]);
    let (status, body) = reply(&server, &user, &run, "SFO").await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(answers(&server.store, &run).await, ["from another tab"]);
}
