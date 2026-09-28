//! The Redis run queue and its workers (C4): a claim takes the run and its
//! lease in one step, a worker acknowledges only after the run's terminal
//! event is stored, and the reaper hands back a run whose lease expired.
//! Needs Postgres and Redis, as `pg_redis.rs` does.
use std::sync::Arc;
use std::time::Duration;

use harness::InMemory;
use protocol::{
    AgentId, CredentialSource, Event, EventPayload, ExecutionPlacement, Limits, ModelProvider,
    Owner, RunId, RunSpec, WorkModel,
};
use server::{
    is_terminal, queued_events, Append, PostgresStore, QueueTiming, RedisRunQueue, RunStore,
    StoredRun, Worker,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const POSTGRES_URL: &str = "postgres://gol:gol@127.0.0.1/gol";
const REDIS_URL: &str = "redis://127.0.0.1/";

/// Short enough for a test to watch a lease expire.
fn quick() -> QueueTiming {
    QueueTiming {
        lease: Duration::from_millis(400),
        heartbeat: Duration::from_millis(100),
        reap_every: Duration::from_millis(150),
        idle_wait: Duration::from_millis(20),
    }
}

fn spec() -> RunSpec {
    RunSpec::builder()
        .owner(Owner::new("https://issuer.test", "user-1", "tenant-1"))
        .agent(AgentId::new(), "1")
        .input("hello")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .limits(Limits {
            max_steps: 4,
            max_model_calls: 1,
        })
        .build()
}

/// A Jev that completes every run, after `delay`.
async fn jev(delay: Duration) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_delay(delay).set_body_json(
            serde_json::json!({
                "model": "jev-latest",
                "usage": {"input_tokens": 1, "output_tokens": 1},
                "answers": {"effect": {"type": "choice", "choice": "complete",
                    "confidence": 1.0,
                    "probabilities": {"echo": 0.0, "model": 0.0, "complete": 1.0}}}
            }),
        ))
        .mount(&server)
        .await;
    server
}

struct Setup {
    queue: RedisRunQueue,
    store: Arc<dyn RunStore>,
    run_id: RunId,
    key: String,
}

/// A run stored in Postgres as the producer stores it, and pushed on a
/// queue of its own.
fn queued_run() -> Setup {
    queued_run_in(Arc::new(
        PostgresStore::connect(POSTGRES_URL).expect("connect"),
    ))
}

/// The same, in `store`.
fn queued_run_in(store: Arc<dyn RunStore>) -> Setup {
    let key = format!("gol:test:{}", RunId::new());
    let queue = RedisRunQueue::with_key(REDIS_URL, &key);
    let spec = spec();
    let run_id = spec.run_id;
    store
        .put_run(StoredRun {
            events: queued_events(&spec),
            spec,
        })
        .expect("put run");
    queue.push(run_id).expect("push");
    Setup {
        queue,
        store,
        run_id,
        key,
    }
}

fn worker(setup: &Setup, jev_uri: &str) -> Worker {
    Worker::new(
        RedisRunQueue::with_key(REDIS_URL, &setup.key),
        setup.store.clone(),
        Arc::new(InMemory::default()),
        jev_uri,
        quick(),
    )
}

fn terminals(store: &dyn RunStore, run_id: RunId) -> Vec<Event> {
    store
        .run(run_id)
        .expect("store")
        .expect("run")
        .events
        .into_iter()
        .filter(|event| is_terminal(&event.payload))
        .collect()
}

/// Runs `work` on a plain thread: the stores and the queue block, which the
/// Postgres client refuses on a Tokio worker.
fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::spawn(work).join().expect("thread")
}

// A worker that claims a run and dies holds a lease that runs out; the reaper
// hands the run back, and another worker ends it.
#[tokio::test(flavor = "multi_thread")]
async fn worker_crash_after_claim_redelivered() {
    let jev = jev(Duration::ZERO).await;
    crash_after_claim(jev.uri(), queued_run).await;
    crash_after_claim(jev.uri(), || {
        queued_run_in(Arc::new(server::InMemoryStore::default()))
    })
    .await;
}

async fn crash_after_claim(uri: String, setup: fn() -> Setup) {
    blocking(move || {
        let setup = setup();
        let crashed = worker(&setup, &uri);
        let next = worker(&setup, &uri);
        let claim = crashed.claim().expect("claim").expect("a run");
        assert_eq!(claim.run_id(), setup.run_id);
        drop(claim);
        std::thread::sleep(quick().lease * 2);
        assert_eq!(setup.queue.reap().expect("reap"), [setup.run_id]);
        assert_eq!(next.work_one().expect("work"), Some(setup.run_id));
        assert!(matches!(
            terminals(setup.store.as_ref(), setup.run_id).as_slice(),
            [Event {
                payload: EventPayload::RunCompleted { .. },
                ..
            }]
        ));
        assert_eq!(setup.queue.processing().expect("processing"), []);
        assert_eq!(setup.queue.queued().expect("queued"), []);
    });
}

// Forced: worker A loads the run, its lease runs out, and B claims, runs and
// acknowledges it; then A runs Jev too and records. The store keeps one
// terminal event, A's record is refused, and A's acknowledgement leaves the
// queue empty.
#[tokio::test(flavor = "multi_thread")]
async fn two_workers_one_terminal() {
    let jev = jev(Duration::ZERO).await;
    two_workers(jev.uri(), queued_run).await;
    assert_eq!(jev.received_requests().await.expect("requests").len(), 2);
    two_workers(jev.uri(), || {
        queued_run_in(Arc::new(server::InMemoryStore::default()))
    })
    .await;
    assert_eq!(jev.received_requests().await.expect("requests").len(), 4);
}

async fn two_workers(uri: String, setup: fn() -> Setup) {
    blocking(move || {
        let setup = setup();
        let a = worker(&setup, &uri);
        let b = worker(&setup, &uri);
        let claim = a.claim().expect("claim").expect("a run");
        let stored = claim.prepare().expect("prepare").expect("open run");
        std::thread::sleep(quick().lease * 2);
        assert_eq!(setup.queue.reap().expect("reap"), [setup.run_id]);
        assert_eq!(b.work_one().expect("work"), Some(setup.run_id));
        let events = claim.execute(&stored);
        assert_eq!(claim.record(events).expect("record"), Append::Terminal);
        claim.ack().expect("ack");
        assert_eq!(terminals(setup.store.as_ref(), setup.run_id).len(), 1);
        assert_eq!(setup.queue.processing().expect("processing"), []);
        assert_eq!(setup.queue.queued().expect("queued"), []);
    });
}

// Owner decision 3A: a redelivered run that already ended is acknowledged
// without asking Jev again.
#[tokio::test(flavor = "multi_thread")]
async fn an_ended_run_is_acknowledged_without_jev() {
    let jev = jev(Duration::ZERO).await;
    let uri = jev.uri();
    blocking(move || {
        let setup = queued_run();
        let stored = setup.store.run(setup.run_id).expect("store").expect("run");
        let failed = server::run_failed_event(
            &stored.spec,
            protocol::FailureClass::Infrastructure,
            "ended elsewhere".to_string(),
        );
        assert_eq!(
            setup.store.append_events(setup.run_id, vec![failed]),
            Ok(Append::Appended)
        );
        assert_eq!(
            worker(&setup, &uri).work_one().expect("work"),
            Some(setup.run_id)
        );
        assert_eq!(terminals(setup.store.as_ref(), setup.run_id).len(), 1);
        assert_eq!(setup.queue.processing().expect("processing"), []);
    });
    assert!(jev.received_requests().await.expect("requests").is_empty());
}

// The heartbeat keeps a slow run's lease alive: the reaper, called all
// through the run, never hands it back.
#[tokio::test(flavor = "multi_thread")]
async fn the_heartbeat_keeps_a_slow_run_leased() {
    let jev = jev(Duration::from_millis(1500)).await;
    let uri = jev.uri();
    blocking(move || {
        let setup = queued_run();
        let worker = worker(&setup, &uri);
        let reaper = RedisRunQueue::with_key(REDIS_URL, &setup.key);
        let working = std::thread::spawn(move || worker.work_one().expect("work"));
        let mut reaped = Vec::new();
        while !working.is_finished() {
            reaped.extend(reaper.reap().expect("reap"));
            std::thread::sleep(quick().reap_every);
        }
        assert_eq!(working.join().expect("thread"), Some(setup.run_id));
        assert_eq!(reaped, []);
        assert_eq!(terminals(setup.store.as_ref(), setup.run_id).len(), 1);
        assert_eq!(setup.queue.queued().expect("queued"), []);
    });
}
// A claimed run whose lease is live stays with its worker.
#[test]
fn the_reaper_leaves_a_live_lease() {
    let setup = queued_run();
    let worker = Worker::new(
        RedisRunQueue::with_key(REDIS_URL, &setup.key),
        setup.store.clone(),
        Arc::new(InMemory::default()),
        "http://127.0.0.1:9",
        QueueTiming {
            lease: Duration::from_secs(30),
            ..quick()
        },
    );
    let claim = worker.claim().expect("claim").expect("a run");
    assert_eq!(setup.queue.reap().expect("reap"), []);
    assert_eq!(
        setup.queue.processing().expect("processing"),
        [setup.run_id]
    );
    claim.ack().expect("ack");
    assert_eq!(setup.queue.processing().expect("processing"), []);
}

// An empty queue claims nothing.
#[test]
fn an_empty_queue_claims_nothing() {
    let key = format!("gol:test:{}", RunId::new());
    let worker = Worker::new(
        RedisRunQueue::with_key(REDIS_URL, &key),
        Arc::new(PostgresStore::connect(POSTGRES_URL).expect("connect")),
        Arc::new(InMemory::default()),
        "http://127.0.0.1:9",
        quick(),
    );
    assert!(worker.claim().expect("claim").is_none());
    assert_eq!(worker.work_one().expect("work"), None);
}
