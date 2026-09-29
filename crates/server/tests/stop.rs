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

use std::sync::Arc;
use std::time::Duration;

use common::queued::{
    blocking, fresh_user, jev, serve, stores, stores_with_messages, Server, Store,
};
use harness::{AgentSpawner, ChildRequest, MessageDeliverer, MessageRequest};
use protocol::{Actor, Event, EventPayload, EventSource, Limits, RunId, RunSpec, Timestamp};
use serde_json::{json, Value};
use server::{is_terminal, OwnedDeliverer, OwnedSpawner, RedisRunQueue};
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

// A stop reaches a run a worker holds at its next step boundary: stopped
// while the run waits on its second decision, it is cancelled once that
// step is stored, and never asks for a third.
#[tokio::test(flavor = "multi_thread")]
async fn a_running_task_stops_at_its_next_step() {
    for store in stores() {
        let jev = slow_second_step(Duration::from_millis(800)).await;
        let server = serve(store.clone(), &jev, 2).await;
        let user = fresh_user();
        let agent = server.agent(&user, "echoer", &["tool.echo"]).await;
        let (_, run) = server.start(&user, agent, "hello").await;
        let worker = server.worker.clone();
        let working = tokio::task::spawn_blocking(move || worker.work_one().expect("work"));
        // Wait until the second decision is asked for, then stop.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while jev.received_requests().await.expect("requests").len() < 2 {
            assert!(std::time::Instant::now() < deadline, "the second step");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let (status, _) = server
            .post(&format!("/v1/runs/{run}/stop"), &user, json!({}))
            .await;
        assert_eq!(status, 200);
        assert!(working.await.expect("worker").is_some());
        assert!(cancelled(&terminals(&store, &run).await));
        assert_eq!(jev.received_requests().await.expect("requests").len(), 2);
    }
}

// A stop covers every run under the run stopped: a task a stopped run's
// child starts later is cancelled when its worker claims it.
#[tokio::test(flavor = "multi_thread")]
async fn stop_cascades_to_the_whole_chain() {
    for store in stores() {
        // The coordinator delegates to the researcher; the researcher, then
        // stopped mid-run is not needed: it delegates to the writer and ends
        // before the stop, and the writer's task starts after it.
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

        let (_, c_run) = server.start(&alice, hers, "c").await;
        let (status, _) = server.post("/v1/stop", &alice, json!({})).await;
        assert_eq!(status, 200);
        assert!(cancelled(&terminals(&store, &c_run).await));
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
