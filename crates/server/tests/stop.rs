//! Stop (Phase 3.4, decisions 52A-55A): a run, a thread and an owner each
//! have a stop button. A stop covers the runs that existed when it was made
//! and every run under them, whenever those start; later work runs as usual.
//! The stop cancels covered runs no worker holds (queued, or parked on an
//! ask, which it also wakes); a worker cancels a covered run it holds before
//! it runs it and at its next step boundary; the spawner and the deliverer
//! refuse new work under a stopped run. The store keeps one terminal event
//! whichever writer lands first. On both stores; needs Postgres and Redis,
//! as `pg_redis.rs` does.
mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::queued::{
    blocking, fresh_user, jev, serve, stores, stores_with_messages, Server, Store,
};
use harness::{AgentSpawner, ChildRequest, MessageDeliverer, MessageRequest};
use protocol::{
    Actor, AgentId, ArtifactId, Event, EventPayload, EventSource, Limits, Owner, RunId, RunSpec,
    Timestamp,
};
use serde_json::{json, Value};
use server::{
    is_terminal, Append, InMemoryStore, OwnedDeliverer, OwnedSpawner, PutAgent, PutRun,
    RedisRunQueue, RunStore, StopScope, StopStore, StoreError, StoredAgent, StoredArtifact,
    StoredRun,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// The terminal events of `run`'s log.
async fn terminals(store: &Store, run: &str) -> Vec<EventPayload> {
    let (store, run): (Store, RunId) = (store.clone(), run.parse().expect("id"));
    blocking(move || {
        store
            .run(run)
            .expect("read")
            .expect("stored")
            .events
            .into_iter()
            .filter(|event| is_terminal(&event.payload))
            .map(|event| event.payload)
            .collect()
    })
    .await
}

async fn spec_of(store: &Store, run: &str) -> RunSpec {
    let (store, run): (Store, RunId) = (store.clone(), run.parse().expect("id"));
    blocking(move || store.run(run).expect("read").expect("stored").spec).await
}

fn cancelled(terminals: &[EventPayload]) -> bool {
    matches!(terminals, [EventPayload::RunCancelled])
}

/// The runs of a thread's board, as (run, state), in order.
async fn board(server: &Server, user: &str, thread: &str) -> Vec<(String, String)> {
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
                card["run_id"].as_str().expect("run").to_string(),
                card["state"].as_str().expect("state").to_string(),
            )
        })
        .collect()
}

/// A coordinator that delegates to a writer and a researcher, then answers;
/// the worker runs it, leaving both tasks queued. Returns its thread and run.
async fn coordinate(server: &Server, user: &str) -> (String, String) {
    let coordinator = server.agent(user, "coordinator", &["agent.delegate"]).await;
    server.agent(user, "writer", &[]).await;
    server.agent(user, "researcher", &["agent.delegate"]).await;
    let (thread, first) = server.start(user, coordinator, "plan a trip").await;
    assert_eq!(
        server.work().await.map(|run| run.to_string()),
        Some(first.clone())
    );
    (thread, first)
}

// A stop reaches runs no worker holds: the coordinator's queued tasks are
// cancelled by the stop itself, and a worker that claims one afterwards
// acknowledges it without running it (Jev is asked nothing more).
#[tokio::test(flavor = "multi_thread")]
async fn a_queued_task_never_starts() {
    for store in stores() {
        let jev = jev(&["delegate:writer", "delegate:researcher", "complete"]).await;
        let server = serve(store.clone(), &jev, 1).await;
        let user = fresh_user();
        let (thread, first) = coordinate(&server, &user).await;
        let (status, body) = server
            .post(&format!("/v1/runs/{first}/stop"), &user, json!({}))
            .await;
        assert_eq!(status, 200, "{body}");
        let cards = board(&server, &user, &thread).await;
        assert_eq!(cards[0].1, "completed", "the stop does not undo an end");
        for task in &cards[1..] {
            assert_eq!(task.1, "cancelled", "{cards:?}");
            assert!(cancelled(&terminals(&store, &task.0).await));
        }
        let asked = jev.received_requests().await.expect("requests").len();
        assert!(server.work().await.is_some());
        assert!(server.work().await.is_some());
        assert_eq!(server.work().await, None);
        assert_eq!(
            jev.received_requests().await.expect("requests").len(),
            asked,
            "a cancelled task never asks Jev"
        );
    }
}

/// A Jev that answers `echo`, then `echo` after `delay`, then `complete`.
async fn slow_second_step(delay: Duration) -> MockServer {
    let server = MockServer::start().await;
    let answer = |label: &str| {
        json!({
            "model": "jev-latest",
            "usage": {"input_tokens": 1, "output_tokens": 1},
            "answers": {"effect": {"type": "choice", "choice": label,
                "confidence": 1.0, "probabilities": {label: 1.0}}}
        })
    };
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(answer("echo")))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(delay)
                .set_body_json(answer("echo")),
        )
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

// A stop reaches a run a worker holds at its next step boundary, whichever
// button covers it: stopped while the run waits on its second decision, it
// is cancelled once that step is stored, and never asks for a third.
#[tokio::test(flavor = "multi_thread")]
async fn a_running_task_stops_at_its_next_step() {
    for store in stores() {
        for scope in ["run", "thread", "owner"] {
            let jev = slow_second_step(Duration::from_secs(3)).await;
            let server = serve(store.clone(), &jev, 2).await;
            let user = fresh_user();
            let agent = server.agent(&user, "echoer", &["tool.echo"]).await;
            let (thread, run) = server.start(&user, agent, "hello").await;
            let worker = server.worker.clone();
            let working = tokio::task::spawn_blocking(move || worker.work_one().expect("work"));
            // Wait until the second decision is asked for, then stop.
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while jev.received_requests().await.expect("requests").len() < 2 {
                assert!(std::time::Instant::now() < deadline, "the second step");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let button = match scope {
                "run" => format!("/v1/runs/{run}/stop"),
                "thread" => format!("/v1/threads/{thread}/stop"),
                _ => "/v1/stop".to_string(),
            };
            let (status, body) = server.post(&button, &user, json!({})).await;
            assert_eq!(status, 200, "{scope}: {body}");
            assert_eq!(body["cancelled"], json!([]), "{scope}: a worker holds it");
            assert!(working.await.expect("worker").is_some());
            assert!(cancelled(&terminals(&store, &run).await), "{scope}");
            assert_eq!(jev.received_requests().await.expect("requests").len(), 2);
        }
    }
}

// A stop covers every run under the run stopped: a task a stopped run's
// child starts later is cancelled when its worker claims it.
#[tokio::test(flavor = "multi_thread")]
async fn stop_cascades_to_the_whole_chain() {
    for store in stores() {
        // The coordinator delegates to the researcher and ends; the
        // researcher delegates to the writer and ends. Both end before the
        // stop; the writer's task, two levels under the run stopped, is still
        // queued, and the stop cancels it.
        let jev = jev(&[
            "delegate:researcher",
            "complete",
            "delegate:writer",
            "complete",
            "complete",
        ])
        .await;
        let server = serve(store.clone(), &jev, 3).await;
        let user = fresh_user();
        let coordinator = server
            .agent(&user, "coordinator", &["agent.delegate"])
            .await;
        server.agent(&user, "researcher", &["agent.delegate"]).await;
        server.agent(&user, "writer", &[]).await;
        let (thread, first) = server.start(&user, coordinator, "plan").await;
        assert!(server.work().await.is_some(), "the coordinator");
        assert!(server.work().await.is_some(), "the researcher");
        let cards = board(&server, &user, &thread).await;
        assert_eq!(cards.len(), 3, "{cards:?}");
        let grandchild = cards[2].0.clone();
        assert_eq!(cards[2].1, "queued");
        let (status, _) = server
            .post(&format!("/v1/runs/{first}/stop"), &user, json!({}))
            .await;
        assert_eq!(status, 200);
        assert!(cancelled(&terminals(&store, &grandchild).await));
        let cards = board(&server, &user, &thread).await;
        assert_eq!(
            cards.iter().map(|card| card.1.as_str()).collect::<Vec<_>>(),
            ["completed", "completed", "cancelled"]
        );
    }
}

// The spawner and the deliverer refuse new work under a stopped run: no
// child or task starts, and nothing is queued.
#[tokio::test(flavor = "multi_thread")]
async fn a_child_started_during_a_stop_is_refused() {
    for store in stores() {
        let jev = jev(&["delegate:writer", "delegate:researcher", "complete"]).await;
        let server = serve(store.clone(), &jev, 4).await;
        let user = fresh_user();
        let (_, first) = coordinate(&server, &user).await;
        let (status, _) = server
            .post(&format!("/v1/runs/{first}/stop"), &user, json!({}))
            .await;
        assert_eq!(status, 200);
        let parent = spec_of(&store, &first).await;
        let writer = server.agent(&user, "late", &[]).await;
        let queue = Arc::new(RedisRunQueue::open(server.redis.clone()));
        let (runs, queued) = (store.clone(), queue.clone());
        let refused = blocking(move || {
            let limits = Limits {
                max_steps: 3,
                max_model_calls: 2,
            };
            let spawned =
                OwnedSpawner::new(runs.clone(), Some(queued.clone())).start(ChildRequest {
                    parent: &parent,
                    step: 9,
                    agent_id: writer,
                    input: "late",
                    limits,
                });
            let messages: Arc<dyn server::MessageStore> =
                Arc::new(server::InMemoryStore::default());
            let sent = OwnedDeliverer::builder()
                .store(runs)
                .messages(messages)
                .queue(queued)
                .build()
                .send(MessageRequest {
                    from: &parent,
                    decision: 9,
                    to: writer,
                    body: "late",
                    expects_reply: false,
                    reply_to: None,
                    timeout_secs: None,
                    limits: Some(limits),
                });
            (spawned.map(|_| ()), sent.map(|_| ()))
        })
        .await;
        assert_eq!(refused.0, Err("the chain is stopped".to_string()));
        assert_eq!(refused.1, Err("the chain is stopped".to_string()));
        let waiting = blocking(move || queue.queued().expect("queued")).await;
        assert_eq!(
            waiting.len(),
            2,
            "only the two cancelled tasks: {waiting:?}"
        );
    }
}

fn record(spec: &RunSpec, payload: EventPayload) -> Event {
    Event::record(
        EventSource::for_spec(spec, Actor::System, Timestamp::now()),
        payload,
    )
}

// A stop and an end race on one run: whichever the store takes first is the
// run's one terminal event, in either order (the store's terminal refusal,
// RunLogFail's Fail against a completer).
#[tokio::test(flavor = "multi_thread")]
async fn stop_racing_completion_keeps_one_terminal() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store.clone(), &jev, 5).await;
        let user = fresh_user();
        let agent = server.agent(&user, "solo", &[]).await;
        // The completion lands first, then the stop.
        let (_, ended) = server.start(&user, agent, "one").await;
        let spec = spec_of(&store, &ended).await;
        let runs = store.clone();
        blocking(move || {
            runs.append_events(
                spec.run_id,
                vec![record(
                    &spec,
                    EventPayload::RunCompleted {
                        outcome: "done".to_string(),
                    },
                )],
            )
            .expect("append");
        })
        .await;
        let (status, _) = server
            .post(&format!("/v1/runs/{ended}/stop"), &user, json!({}))
            .await;
        assert_eq!(status, 200);
        assert!(matches!(
            terminals(&store, &ended).await.as_slice(),
            [EventPayload::RunCompleted { .. }]
        ));
        // The stop lands first, then the completion.
        let (_, stopped) = server.start(&user, agent, "two").await;
        let (status, _) = server
            .post(&format!("/v1/runs/{stopped}/stop"), &user, json!({}))
            .await;
        assert_eq!(status, 200);
        let spec = spec_of(&store, &stopped).await;
        let runs = store.clone();
        let late = blocking(move || {
            runs.append_events(
                spec.run_id,
                vec![record(
                    &spec,
                    EventPayload::RunCompleted {
                        outcome: "late".to_string(),
                    },
                )],
            )
        })
        .await;
        assert_eq!(late, Ok(server::Append::Terminal));
        assert!(cancelled(&terminals(&store, &stopped).await));
    }
}

// A thread's stop covers its runs only; an owner's stop covers every run the
// principal owns, and nobody else's. Work started after a stop runs (54A).
#[tokio::test(flavor = "multi_thread")]
async fn a_thread_stop_and_an_owner_stop_cover_their_runs_only() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store.clone(), &jev, 6).await;
        let (alice, bob) = (fresh_user(), fresh_user());
        let hers = server.agent(&alice, "solo", &[]).await;
        let his = server.agent(&bob, "solo", &[]).await;
        let (a, a_run) = server.start(&alice, hers, "a").await;
        let (_, b_run) = server.start(&alice, hers, "b").await;
        let (_, bob_run) = server.start(&bob, his, "c").await;

        let (status, _) = server
            .post(&format!("/v1/threads/{a}/stop"), &alice, json!({}))
            .await;
        assert_eq!(status, 200);
        assert!(cancelled(&terminals(&store, &a_run).await));
        assert_eq!(terminals(&store, &b_run).await, Vec::new());

        // A follow-up after the thread's stop runs as usual.
        let (status, body) = server
            .post(
                &format!("/v1/threads/{a}/messages"),
                &alice,
                json!({"input": "again"}),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        let after = body["run"]["run_id"].as_str().expect("run").to_string();
        while server.work().await.is_some() {}
        assert!(matches!(
            terminals(&store, &after).await.as_slice(),
            [EventPayload::RunCompleted { .. }]
        ));
        // A second stop of the thread covers the work started since.
        let (status, body) = server
            .post(
                &format!("/v1/threads/{a}/messages"),
                &alice,
                json!({"input": "once more"}),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        let again = body["run"]["run_id"].as_str().expect("run").to_string();
        let (status, _) = server
            .post(&format!("/v1/threads/{a}/stop"), &alice, json!({}))
            .await;
        assert_eq!(status, 200);
        assert!(cancelled(&terminals(&store, &again).await));

        let (_, c_run) = server.start(&alice, hers, "c").await;
        let (status, _) = server.post("/v1/stop", &alice, json!({})).await;
        assert_eq!(status, 200);
        assert!(cancelled(&terminals(&store, &c_run).await));
        // Work started after the owner's stop runs as usual.
        let (_, d_run) = server.start(&alice, hers, "d").await;
        while server.work().await.is_some() {}
        assert!(matches!(
            terminals(&store, &d_run).await.as_slice(),
            [EventPayload::RunCompleted { .. }]
        ));
        // Bob's run is his own: it runs to its end.
        assert!(matches!(
            terminals(&store, &bob_run).await.as_slice(),
            [EventPayload::RunCompleted { .. }]
        ));
    }
}

// A run parked on an ask is cancelled by a stop, and woken so its worker
// acknowledges it: nothing stays parked. Its asked task, under it, is
// cancelled too.
#[tokio::test(flavor = "multi_thread")]
async fn a_parked_asker_is_cancelled_by_a_stop() {
    for (store, messages) in stores_with_messages() {
        let jev = jev(&["ask:writer", "complete"]).await;
        let server = serve(store.clone(), &jev, 7).await.with_messages(messages);
        let user = fresh_user();
        let researcher = server.agent(&user, "researcher", &["agent.message"]).await;
        server.agent(&user, "writer", &[]).await;
        let (thread, asker) = server.start(&user, researcher, "what is the plan?").await;
        assert!(server.work().await.is_some());
        let queue = RedisRunQueue::open(server.redis.clone());
        let parked = blocking(move || queue.parked().expect("parked")).await;
        assert_eq!(parked.len(), 1, "the researcher is parked");

        let (status, _) = server
            .post(&format!("/v1/runs/{asker}/stop"), &user, json!({}))
            .await;
        assert_eq!(status, 200);
        assert!(cancelled(&terminals(&store, &asker).await));
        let cards = board(&server, &user, &thread).await;
        assert!(cards.iter().all(|card| card.1 == "cancelled"), "{cards:?}");
        while server.work().await.is_some() {}
        let queue = RedisRunQueue::open(server.redis.clone());
        let (parked, queued) = blocking(move || {
            (
                queue.parked().expect("parked"),
                queue.queued().expect("queued"),
            )
        })
        .await;
        assert_eq!(parked, Vec::new());
        assert_eq!(queued, Vec::new());
    }
}

// Another principal's run and thread cannot be stopped, and a stop of one's
// own touches nothing of theirs.
#[tokio::test(flavor = "multi_thread")]
async fn another_principals_run_cannot_be_stopped() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store.clone(), &jev, 8).await;
        let (alice, bob) = (fresh_user(), fresh_user());
        let hers = server.agent(&alice, "solo", &[]).await;
        let (thread, run) = server.start(&alice, hers, "mine").await;
        let (status, _) = server
            .post(&format!("/v1/runs/{run}/stop"), &bob, json!({}))
            .await;
        assert_eq!(status, 404);
        let (status, body) = server
            .post(&format!("/v1/threads/{thread}/stop"), &bob, json!({}))
            .await;
        assert_eq!(status, 404, "{body}");
        let (status, _) = server.post("/v1/stop", &bob, json!({})).await;
        assert_eq!(status, 200);
        assert_eq!(terminals(&store, &run).await, Vec::new());
        // The worker's check does not take Bob's stop for hers either.
        while server.work().await.is_some() {}
        assert!(matches!(
            terminals(&store, &run).await.as_slice(),
            [EventPayload::RunCompleted { .. }]
        ));
        let _: Value = body;
    }
}

// A child that slipped past the spawner's check (started as the stop was
// made) is still under the stopped run: its worker cancels it at its claim,
// before it runs.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_child_is_cancelled_when_claimed() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store.clone(), &jev, 9).await;
        let user = fresh_user();
        let agent = server.agent(&user, "solo", &[]).await;
        let (_, parent) = server.start(&user, agent, "parent").await;
        let (status, _) = server
            .post(&format!("/v1/runs/{parent}/stop"), &user, json!({}))
            .await;
        assert_eq!(status, 200);
        // The child, stored and queued after the stop, as a spawner that
        // checked just before it would.
        let parent_spec = spec_of(&store, &parent).await;
        let child = RunSpec::builder()
            .owner(parent_spec.owner.clone())
            .agent(agent, "1")
            .input("late")
            .placement(parent_spec.placement)
            .work_model(parent_spec.work_model.clone())
            .child_of(&parent_spec, 1)
            .build();
        let child_id = child.run_id;
        let (runs, url) = (store.clone(), server.redis.clone());
        blocking(move || {
            runs.put_run(server::StoredRun {
                events: server::queued_events(&child),
                spec: child,
            })
            .expect("put");
            RedisRunQueue::open(url).push(child_id).expect("push");
        })
        .await;
        let asked = jev.received_requests().await.expect("requests").len();
        while server.work().await.is_some() {}
        assert!(cancelled(&terminals(&store, &child_id.to_string()).await));
        // Cancelled before it ran: never scheduled, never started.
        let runs = store.clone();
        let log = blocking(move || runs.run(child_id).expect("read").expect("stored").events).await;
        assert!(
            !log.iter().any(|event| matches!(
                event.payload,
                EventPayload::RunScheduled | EventPayload::RunStarted
            )),
            "{log:?}"
        );
        assert_eq!(
            jev.received_requests().await.expect("requests").len(),
            asked,
            "the late child never asks Jev"
        );
    }
}

// The runs a stop covers, as each store reads them: a thread's open runs
// only, and an owner's own open runs, not another principal's.
#[tokio::test(flavor = "multi_thread")]
async fn a_stop_reads_the_open_runs_of_its_scope_only() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store.clone(), &jev, 11).await;
        let (alice, bob) = (fresh_user(), fresh_user());
        let hers = server.agent(&alice, "solo", &[]).await;
        let his = server.agent(&bob, "solo", &[]).await;
        let (a, a_run) = server.start(&alice, hers, "a").await;
        let (_, b_run) = server.start(&alice, hers, "b").await;
        server.start(&bob, his, "c").await;
        let owner = spec_of(&store, &a_run).await.owner;
        let runs = store.clone();
        let (thread, everything) = blocking(move || {
            let stops = runs.stops().expect("stops");
            let ids = |scope: StopScope| {
                let mut ids: Vec<String> = stops
                    .open_runs_under(&owner, &scope)
                    .expect("read")
                    .iter()
                    .map(|run| run.spec.run_id.to_string())
                    .collect();
                ids.sort();
                ids
            };
            (ids(StopScope::Thread(a)), ids(StopScope::Owner))
        })
        .await;
        assert_eq!(thread, vec![a_run.clone()]);
        let mut hers = vec![a_run, b_run];
        hers.sort();
        assert_eq!(everything, hers);
    }
}

/// What an `Interleaved` store does once, between the stop's steps.
enum Hook {
    /// Appends `RunScheduled` onto each run just after a stop reads the
    /// runs it covers: a worker claiming the run between the stop's read and
    /// its cancel.
    ClaimAfterRead,
    /// Stores this run just after a stop is recorded: work started between
    /// the stop and its read.
    PutAfterStop(Box<StoredRun>),
    /// Fails the stop's cancel of a run.
    FailCancel,
}

/// An in-memory store that runs its `Hook` once.
struct Interleaved {
    inner: InMemoryStore,
    hook: Mutex<Option<Hook>>,
}

impl Interleaved {
    fn store(hook: Hook) -> Store {
        Arc::new(Self {
            inner: InMemoryStore::default(),
            hook: Mutex::new(Some(hook)),
        })
    }

    /// Takes the hook if it is the one `wanted` names.
    fn take(&self, wanted: impl Fn(&Hook) -> bool) -> Option<Hook> {
        let mut hook = self.hook.lock().expect("hook");
        if hook.as_ref().is_some_and(wanted) {
            hook.take()
        } else {
            None
        }
    }
}

impl RunStore for Interleaved {
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
        if self.take(|hook| matches!(hook, Hook::FailCancel)).is_some() {
            return Err(StoreError::new("the store is unreachable"));
        }
        self.inner.append_events_after(id, seen, events)
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
    fn stops(&self) -> Option<&dyn StopStore> {
        Some(self)
    }
}

impl StopStore for Interleaved {
    fn put_stop(&self, owner: &Owner, scope: &StopScope) -> Result<(), StoreError> {
        self.inner.put_stop(owner, scope)?;
        if let Some(Hook::PutAfterStop(run)) =
            self.take(|hook| matches!(hook, Hook::PutAfterStop(_)))
        {
            assert_eq!(self.inner.put_run(*run), Ok(PutRun::Stored));
        }
        Ok(())
    }
    fn stopped(&self, run: RunId) -> Result<bool, StoreError> {
        self.inner.stopped(run)
    }
    fn open_runs_under(
        &self,
        owner: &Owner,
        scope: &StopScope,
    ) -> Result<Vec<StoredRun>, StoreError> {
        let snapshot = self.inner.open_runs_under(owner, scope)?;
        if self
            .take(|hook| matches!(hook, Hook::ClaimAfterRead))
            .is_some()
        {
            for run in &snapshot {
                assert_eq!(
                    self.inner.append_events(
                        run.spec.run_id,
                        vec![record(&run.spec, EventPayload::RunScheduled)]
                    ),
                    Ok(Append::Appended)
                );
            }
        }
        Ok(snapshot)
    }
}

// The stop cancels only onto the log it read (RunLogWorkers' conditional
// append): a worker that claimed the run in between moved the log, so the
// stop leaves the run to that worker, which cancels it at its check.
#[tokio::test(flavor = "multi_thread")]
async fn a_claim_between_the_stops_read_and_its_cancel_is_left_to_the_worker() {
    let store = Interleaved::store(Hook::ClaimAfterRead);
    let jev = jev(&["complete"]).await;
    let server = serve(store.clone(), &jev, 10).await;
    let user = fresh_user();
    let agent = server.agent(&user, "solo", &[]).await;
    let (_, run) = server.start(&user, agent, "one").await;
    let (status, body) = server
        .post(&format!("/v1/runs/{run}/stop"), &user, json!({}))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["cancelled"], json!([]), "the log moved under the stop");
    assert_eq!(terminals(&store, &run).await, Vec::new());
    while server.work().await.is_some() {}
    assert!(cancelled(&terminals(&store, &run).await));
    assert_eq!(jev.received_requests().await.expect("requests").len(), 0);
}

// A run stored between an owner's stop and the stop's read of their runs
// came after the stop: the stop leaves it, and it runs to its end.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_stored_just_after_a_stop_is_not_cancelled() {
    let user = fresh_user();
    let owner = Owner::new(common::ISSUER, &user, "tenant-1");
    let agent = AgentId::new();
    let spec = RunSpec::builder()
        .owner(owner)
        .agent(agent, "1")
        .input("after")
        .placement(protocol::ExecutionPlacement::Local)
        .work_model(protocol::WorkModel {
            provider: protocol::ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: protocol::CredentialSource::PlatformGateway,
        })
        .build();
    let late = spec.run_id;
    let store = Interleaved::store(Hook::PutAfterStop(Box::new(StoredRun {
        events: server::queued_events(&spec),
        spec,
    })));
    let jev = jev(&["complete"]).await;
    let server = serve(store.clone(), &jev, 12).await;
    let stored = server.put_version(&user, agent, "solo", "1").await;
    assert!(stored);
    let (_, before) = server.start(&user, agent, "before").await;
    let (status, body) = server.post("/v1/stop", &user, json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["cancelled"], json!([before]));
    // Queued as its request would have queued it.
    let url = server.redis.clone();
    blocking(move || RedisRunQueue::open(url).push(late).expect("push")).await;
    while server.work().await.is_some() {}
    assert!(matches!(
        terminals(&store, &late.to_string()).await.as_slice(),
        [EventPayload::RunCompleted { .. }]
    ));
}

// A cancel the store fails answers 503; the stop is recorded, so the
// worker still cancels the run when it claims it.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_cancel_answers_503_and_the_worker_still_cancels() {
    let store = Interleaved::store(Hook::FailCancel);
    let jev = jev(&["complete"]).await;
    let server = serve(store.clone(), &jev, 13).await;
    let user = fresh_user();
    let agent = server.agent(&user, "solo", &[]).await;
    let (_, run) = server.start(&user, agent, "one").await;
    let (status, body) = server
        .post(&format!("/v1/runs/{run}/stop"), &user, json!({}))
        .await;
    assert_eq!(status, 503, "{body}");
    assert_eq!(terminals(&store, &run).await, Vec::new());
    while server.work().await.is_some() {}
    assert!(cancelled(&terminals(&store, &run).await));
}

// A child an older server stored has no `parent_run` column, only its
// spec's lineage: a stop of its parent still covers it. Postgres only.
#[test]
fn a_stop_covers_a_child_an_older_server_stored() {
    let store = common::queued::POSTGRES
        .get_or_init(|| {
            Arc::new(server::PostgresStore::connect(common::queued::POSTGRES_URL).expect("connect"))
                as Store
        })
        .clone();
    let owner = Owner::new(common::ISSUER, fresh_user(), "tenant-1");
    let spec = |parent: Option<&RunSpec>| {
        let builder = RunSpec::builder()
            .owner(owner.clone())
            .agent(AgentId::new(), "1")
            .input("x")
            .placement(protocol::ExecutionPlacement::Local)
            .work_model(protocol::WorkModel {
                provider: protocol::ModelProvider::OpenAI,
                model_name: "gpt-test".to_string(),
                credential: protocol::CredentialSource::PlatformGateway,
            });
        match parent {
            Some(parent) => builder.child_of(parent, 1).build(),
            None => builder.build(),
        }
    };
    let parent = spec(None);
    let child = spec(Some(&parent));
    let parent_id = parent.run_id;
    assert_eq!(
        store.put_run(StoredRun {
            events: server::queued_events(&parent),
            spec: parent.clone(),
        }),
        Ok(PutRun::Stored)
    );
    let mut admin =
        postgres::Client::connect(common::queued::POSTGRES_URL, postgres::NoTls).expect("admin");
    admin
        .execute(
            "insert into runs (id, spec) values ($1, $2)",
            &[
                &child.run_id.as_uuid(),
                &serde_json::to_value(&child).expect("spec"),
            ],
        )
        .expect("an older server's insert");
    let stops = store.stops().expect("stops");
    assert_eq!(stops.stopped(child.run_id), Ok(false));
    stops
        .put_stop(&owner, &StopScope::Run(parent_id))
        .expect("stop");
    assert_eq!(stops.stopped(child.run_id), Ok(true));
}

// Runs an older server stored have no owner, thread or parent columns until
// the next connect fills them (51A). Every stop scope still reads them,
// through their spec, so the stop cancels them at once rather than leaving
// them to a worker's claim. Postgres only.
#[test]
fn every_stop_reads_the_runs_an_older_server_stored() {
    let store = common::queued::POSTGRES
        .get_or_init(|| {
            Arc::new(server::PostgresStore::connect(common::queued::POSTGRES_URL).expect("connect"))
                as Store
        })
        .clone();
    // This binary's other Postgres store connects now, not while the
    // older server's rows below must stay unfilled.
    drop(stores_with_messages());
    let owner = Owner::new(common::ISSUER, fresh_user(), "tenant-1");
    let thread = format!("old-{}", RunId::new());
    let other_thread = format!("old-{}", RunId::new());
    let bob = Owner::new(common::ISSUER, fresh_user(), "tenant-1");
    let spec_of = |owner: &Owner, thread: &str, parent: Option<&RunSpec>| {
        let builder = RunSpec::builder()
            .owner(owner.clone())
            .agent(AgentId::new(), "1")
            .input("x")
            .placement(protocol::ExecutionPlacement::Local)
            .work_model(protocol::WorkModel {
                provider: protocol::ModelProvider::OpenAI,
                model_name: "gpt-test".to_string(),
                credential: protocol::CredentialSource::PlatformGateway,
            })
            .metadata(
                [(protocol::SESSION_ID.to_string(), thread.to_string())]
                    .into_iter()
                    .collect(),
            );
        match parent {
            Some(parent) => builder.child_of(parent, 1).build(),
            None => builder.build(),
        }
    };
    let spec = |parent: Option<&RunSpec>| spec_of(&owner, &thread, parent);
    // A parent this server stored, its child and a root an older one did;
    // in another thread an older server's root and its child; and another
    // principal's run in the first thread, which no stop of this one reads.
    let parent = spec(None);
    let child = spec(Some(&parent));
    let root = spec(None);
    let old_root = spec_of(&owner, &other_thread, None);
    let old_child = spec_of(&owner, &other_thread, Some(&old_root));
    let bobs = spec_of(&bob, &thread, None);
    assert_eq!(
        store.put_run(StoredRun {
            events: server::queued_events(&parent),
            spec: parent.clone(),
        }),
        Ok(PutRun::Stored)
    );
    let mut admin =
        postgres::Client::connect(common::queued::POSTGRES_URL, postgres::NoTls).expect("admin");
    for old in [&child, &root, &old_root, &old_child, &bobs] {
        admin
            .execute(
                "insert into runs (id, spec) values ($1, $2)",
                &[
                    &old.run_id.as_uuid(),
                    &serde_json::to_value(old).expect("spec"),
                ],
            )
            .expect("an older server's insert");
    }
    let stops = store.stops().expect("stops");
    let read = |scope: StopScope| {
        let mut ids: Vec<RunId> = stops
            .open_runs_under(&owner, &scope)
            .expect("read")
            .iter()
            .map(|run| run.spec.run_id)
            .collect();
        ids.sort_by_key(|id| id.as_uuid());
        ids
    };
    let sorted = |mut ids: Vec<RunId>| {
        ids.sort_by_key(|id| id.as_uuid());
        ids
    };
    assert_eq!(
        read(StopScope::Run(parent.run_id)),
        sorted(vec![parent.run_id, child.run_id])
    );
    assert_eq!(
        read(StopScope::Thread(thread.clone())),
        sorted(vec![parent.run_id, child.run_id, root.run_id])
    );
    assert_eq!(
        read(StopScope::Owner),
        sorted(vec![
            parent.run_id,
            child.run_id,
            root.run_id,
            old_root.run_id,
            old_child.run_id
        ])
    );
    // A run stop from an older server's root reaches its child.
    assert_eq!(
        read(StopScope::Run(old_root.run_id)),
        sorted(vec![old_root.run_id, old_child.run_id])
    );
    // Its root is not a thread root a follow-up could start from: a
    // follow-up reads only filled rows (the stop button's own fallback is
    // tested over HTTP below).
    let found = store
        .threads()
        .expect("threads")
        .thread_root(&owner, &other_thread)
        .expect("root");
    assert_eq!(found, None);
    // A thread stop covers the older server's root, through its session.
    assert_eq!(stops.stopped(root.run_id), Ok(false));
    stops
        .put_stop(&owner, &StopScope::Thread(thread.clone()))
        .expect("stop");
    assert_eq!(stops.stopped(root.run_id), Ok(true));
    // The older server's rows stayed unfilled throughout, so the spec
    // branches, not the columns, answered: a connect elsewhere in this
    // binary (51A) could have filled them.
    let olds: Vec<uuid::Uuid> = [&child, &root, &old_root, &old_child, &bobs]
        .iter()
        .map(|old| old.run_id.as_uuid())
        .collect();
    let unfilled: i64 = admin
        .query_one(
            "select count(*) from runs where id = any($1) and owner_issuer is null",
            &[&olds],
        )
        .expect("count")
        .get(0);
    assert_eq!(unfilled, 5, "a connect filled the rows mid-test");
}

// A thread an older server started, before its columns are filled: its stop
// button finds it through its open runs and cancels them, and a message to
// it is 404, not a second coordinator beside a run it cannot see. Postgres
// only.
#[tokio::test(flavor = "multi_thread")]
async fn a_thread_an_older_server_started_can_be_stopped_not_followed_up() {
    // This binary's Postgres stores connect now, not while the row below
    // must stay unfilled.
    drop(blocking(stores_with_messages).await);
    let store = stores().pop().expect("postgres");
    let jev = jev(&["complete"]).await;
    let server = serve(store.clone(), &jev, 14).await;
    let user = fresh_user();
    let owner = Owner::new(common::ISSUER, user.clone(), "tenant-1");
    let thread = format!("old-{}", RunId::new());
    let root = RunSpec::builder()
        .owner(owner)
        .agent(AgentId::new(), "1")
        .input("x")
        .placement(protocol::ExecutionPlacement::Local)
        .work_model(protocol::WorkModel {
            provider: protocol::ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: protocol::CredentialSource::PlatformGateway,
        })
        .metadata(
            [(protocol::SESSION_ID.to_string(), thread.clone())]
                .into_iter()
                .collect(),
        )
        .build();
    let (id, spec) = (root.run_id, serde_json::to_value(&root).expect("spec"));
    // Each admin connection lives and drops inside its blocking call: the
    // Postgres client cannot be dropped on a Tokio worker.
    let admin =
        || postgres::Client::connect(common::queued::POSTGRES_URL, postgres::NoTls).expect("admin");
    blocking(move || {
        admin()
            .execute(
                "insert into runs (id, spec) values ($1, $2)",
                &[&id.as_uuid(), &spec],
            )
            .expect("an older server's insert");
    })
    .await;
    let (status, body) = server
        .post(
            &format!("/v1/threads/{thread}/messages"),
            &user,
            json!({"input": "more"}),
        )
        .await;
    assert_eq!(status, 404, "{body}");
    let (status, body) = server
        .post(&format!("/v1/threads/{thread}/stop"), &user, json!({}))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["cancelled"], json!([id]));
    // Another principal's stop of that thread finds nothing of theirs.
    let (status, _) = server
        .post(
            &format!("/v1/threads/{thread}/stop"),
            &fresh_user(),
            json!({}),
        )
        .await;
    assert_eq!(status, 404);
    let unfilled: i64 = blocking(move || {
        admin()
            .query_one(
                "select count(*) from runs where id = $1 and owner_issuer is null",
                &[&id.as_uuid()],
            )
            .expect("count")
            .get(0)
    })
    .await;
    assert_eq!(unfilled, 1, "a connect filled the row mid-test");
}
