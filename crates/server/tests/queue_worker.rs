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
    is_terminal, queued_events, Append, InMemoryStore, PostgresStore, Prepared, QueueTiming,
    RedisRunQueue, RunStore, StoreError, StoredRun, Worker,
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
        max_backoff: Duration::from_millis(200),
        max_deliveries: 5,
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
    worker_with(setup, jev_uri, quick())
}

fn worker_with(setup: &Setup, jev_uri: &str, timing: QueueTiming) -> Worker {
    Worker::builder()
        .queue(RedisRunQueue::with_key(REDIS_URL, &setup.key))
        .store(setup.store.clone())
        .memory(Arc::new(InMemory::default()))
        .jev(jev_uri)
        .timing(timing)
        .build()
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
        assert_eq!(
            setup.queue.starts(setup.run_id),
            Ok(0),
            "the ack clears the count"
        );
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
        let Prepared::Open(open) = claim.prepare().expect("prepare") else {
            panic!("the run is open");
        };
        std::thread::sleep(quick().lease * 2);
        assert_eq!(setup.queue.reap().expect("reap"), [setup.run_id]);
        assert_eq!(b.work_one().expect("work"), Some(setup.run_id));
        let done = open.execute().record().expect("record");
        assert_eq!(done.recorded(), Some(Append::Terminal));
        done.ack().expect("ack");
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
    let worker = worker_with(
        &setup,
        "http://127.0.0.1:9",
        QueueTiming {
            lease: Duration::from_secs(30),
            ..quick()
        },
    );
    let claim = worker.claim().expect("claim").expect("a run");
    assert_eq!(claim.run_id(), setup.run_id);
    assert_eq!(setup.queue.reap().expect("reap"), []);
    assert_eq!(
        setup.queue.processing().expect("processing"),
        [setup.run_id]
    );
    assert_eq!(setup.queue.queued().expect("queued"), []);
}

// An empty queue claims nothing.
#[test]
fn an_empty_queue_claims_nothing() {
    let setup = Setup {
        queue: RedisRunQueue::with_key(REDIS_URL, "unused"),
        store: Arc::new(InMemoryStore::default()),
        run_id: RunId::new(),
        key: format!("gol:test:{}", RunId::new()),
    };
    let worker = worker(&setup, "http://127.0.0.1:9");
    assert!(worker.claim().expect("claim").is_none());
    assert_eq!(worker.work_one().expect("work"), None);
}

/// The in-memory store, except that every append fails.
struct AppendsFail(InMemoryStore);

impl RunStore for AppendsFail {
    fn put_agent(&self, agent: server::StoredAgent) -> Result<server::PutAgent, StoreError> {
        self.0.put_agent(agent)
    }
    fn agent(&self, id: AgentId) -> Result<Option<server::StoredAgent>, StoreError> {
        self.0.agent(id)
    }
    fn put_run(&self, run: StoredRun) -> Result<(), StoreError> {
        self.0.put_run(run)
    }
    fn append_events(&self, _id: RunId, _events: Vec<Event>) -> Result<Append, StoreError> {
        Err(StoreError::new("appends are down"))
    }
    fn run(&self, id: RunId) -> Result<Option<StoredRun>, StoreError> {
        self.0.run(id)
    }
    fn put_artifact(&self, artifact: server::StoredArtifact) -> Result<(), StoreError> {
        self.0.put_artifact(artifact)
    }
    fn artifact(
        &self,
        id: protocol::ArtifactId,
    ) -> Result<Option<server::StoredArtifact>, StoreError> {
        self.0.artifact(id)
    }
}

// A worker whose record fails does not acknowledge: the run stays in
// processing under its lease, for the reaper to hand back once it expires.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_record_is_not_acknowledged() {
    let jev = jev(Duration::ZERO).await;
    let uri = jev.uri();
    blocking(move || {
        let setup = queued_run_in(Arc::new(AppendsFail(InMemoryStore::default())));
        let error = worker(&setup, &uri)
            .work_one()
            .expect_err("the record failed");
        assert!(error.contains("appends are down"), "{error}");
        assert_eq!(
            setup.queue.processing().expect("processing"),
            [setup.run_id]
        );
        std::thread::sleep(quick().lease * 2);
        assert_eq!(setup.queue.reap().expect("reap"), [setup.run_id]);
    });
}

// Only a start counts toward max_deliveries: with 2, a run started twice
// without ending still runs a second time, and on its third start it is
// failed instead, without Jev, and leaves the queue with its count cleared.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_started_too_often_is_failed() {
    let jev = jev(Duration::ZERO).await;
    started_too_often(jev.uri(), queued_run).await;
    started_too_often(jev.uri(), || {
        queued_run_in(Arc::new(InMemoryStore::default()))
    })
    .await;
    assert!(jev.received_requests().await.expect("requests").is_empty());
}

async fn started_too_often(uri: String, setup: fn() -> Setup) {
    blocking(move || {
        let setup = setup();
        let timing = QueueTiming {
            max_deliveries: 2,
            ..quick()
        };
        for start in 1..=2 {
            let worker = worker_with(&setup, &uri, timing);
            let claim = worker.claim().expect("claim").expect("a run");
            let Prepared::Open(open) = claim.prepare().expect("prepare") else {
                panic!("start {start} still runs");
            };
            drop(open);
            assert_eq!(setup.queue.starts(setup.run_id), Ok(start));
            std::thread::sleep(timing.lease * 2);
            assert_eq!(setup.queue.reap().expect("reap"), [setup.run_id]);
        }
        let next = worker_with(&setup, &uri, timing);
        assert_eq!(next.work_one().expect("work"), Some(setup.run_id));
        assert!(matches!(
            terminals(setup.store.as_ref(), setup.run_id).as_slice(),
            [Event {
                payload: EventPayload::RunFailed { message, .. },
                ..
            }] if message == "started 2 times without ending"
        ));
        assert_eq!(setup.queue.processing().expect("processing"), []);
        assert_eq!(setup.queue.queued().expect("queued"), []);
        assert_eq!(setup.queue.starts(setup.run_id), Ok(0));
    });
}

/// The in-memory store, except that every read of a run fails.
struct ReadsFail(InMemoryStore);

impl RunStore for ReadsFail {
    fn put_agent(&self, agent: server::StoredAgent) -> Result<server::PutAgent, StoreError> {
        self.0.put_agent(agent)
    }
    fn agent(&self, id: AgentId) -> Result<Option<server::StoredAgent>, StoreError> {
        self.0.agent(id)
    }
    fn put_run(&self, run: StoredRun) -> Result<(), StoreError> {
        self.0.put_run(run)
    }
    fn append_events(&self, id: RunId, events: Vec<Event>) -> Result<Append, StoreError> {
        self.0.append_events(id, events)
    }
    fn run(&self, _id: RunId) -> Result<Option<StoredRun>, StoreError> {
        Err(StoreError::new("reads are down"))
    }
    fn put_artifact(&self, artifact: server::StoredArtifact) -> Result<(), StoreError> {
        self.0.put_artifact(artifact)
    }
    fn artifact(
        &self,
        id: protocol::ArtifactId,
    ) -> Result<Option<server::StoredArtifact>, StoreError> {
        self.0.artifact(id)
    }
}

// A claim that cannot load its run queues it again, behind the other runs,
// releases its lease, and counts no start: an outage of the store does not
// use up a run's starts.
#[test]
fn a_load_error_releases_the_claim_without_counting() {
    let setup = queued_run_in(Arc::new(ReadsFail(InMemoryStore::default())));
    let error = worker(&setup, "http://127.0.0.1:9")
        .work_one()
        .expect_err("the load failed");
    assert!(error.contains("reads are down"), "{error}");
    assert_eq!(setup.queue.queued().expect("queued"), [setup.run_id]);
    assert_eq!(setup.queue.processing().expect("processing"), []);
    assert_eq!(setup.queue.starts(setup.run_id), Ok(0));
    assert_eq!(setup.queue.reap().expect("reap"), []);
}

// A run id queued twice is claimed once: the second claim finds it leased
// and drops the duplicate.
#[test]
fn a_run_queued_twice_is_claimed_once() {
    let setup = queued_run();
    setup.queue.push(setup.run_id).expect("push again");
    let worker = worker(&setup, "http://127.0.0.1:9");
    let first = worker.claim().expect("claim").expect("a run");
    assert_eq!(first.run_id(), setup.run_id);
    assert!(worker.claim().expect("second claim").is_none());
    assert_eq!(
        setup.queue.processing().expect("processing"),
        [setup.run_id]
    );
    assert_eq!(setup.queue.queued().expect("queued"), []);
}

// An entry that is not a run id is dropped when claimed, with an error,
// instead of cycling through the queue for ever.
#[test]
fn a_malformed_entry_is_dropped() {
    let setup = queued_run();
    let mut redis = redis::Client::open(REDIS_URL)
        .expect("client")
        .get_connection()
        .expect("connect");
    redis::cmd("RPUSH")
        .arg(format!("{{{}}}", setup.key))
        .arg("not-a-run")
        .query::<()>(&mut redis)
        .expect("push");
    let worker = worker(&setup, "http://127.0.0.1:9");
    let error = worker.claim().map(|_| ()).expect_err("malformed");
    assert!(error.contains("not-a-run"), "{error}");
    let processing: Vec<String> = redis::cmd("LRANGE")
        .arg(format!("{{{}}}:processing", setup.key))
        .arg(0)
        .arg(-1)
        .query(&mut redis)
        .expect("processing");
    assert!(processing.is_empty(), "{processing:?}");
    assert_eq!(setup.queue.queued().expect("queued"), [setup.run_id]);
}

/// The in-memory store, except that the first read of a run waits at a gate
/// and then fails, like a load on a connection that hung and timed out.
struct FirstLoadHangsThenFails {
    inner: InMemoryStore,
    hung: std::sync::Mutex<bool>,
    entered: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    gate: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}

impl RunStore for FirstLoadHangsThenFails {
    fn put_agent(&self, agent: server::StoredAgent) -> Result<server::PutAgent, StoreError> {
        self.inner.put_agent(agent)
    }
    fn agent(&self, id: AgentId) -> Result<Option<server::StoredAgent>, StoreError> {
        self.inner.agent(id)
    }
    fn put_run(&self, run: StoredRun) -> Result<(), StoreError> {
        self.inner.put_run(run)
    }
    fn append_events(&self, id: RunId, events: Vec<Event>) -> Result<Append, StoreError> {
        self.inner.append_events(id, events)
    }
    fn run(&self, id: RunId) -> Result<Option<StoredRun>, StoreError> {
        let first = !std::mem::replace(&mut *self.hung.lock().unwrap(), true);
        if first {
            let entered = self.entered.lock().unwrap().take().expect("entered");
            let gate = self.gate.lock().unwrap().take().expect("gate");
            entered.send(()).expect("enter");
            gate.recv().expect("gate");
            return Err(StoreError::new("the connection hung, then timed out"));
        }
        self.inner.run(id)
    }
    fn put_artifact(&self, artifact: server::StoredArtifact) -> Result<(), StoreError> {
        self.inner.put_artifact(artifact)
    }
    fn artifact(
        &self,
        id: protocol::ArtifactId,
    ) -> Result<Option<server::StoredArtifact>, StoreError> {
        self.inner.artifact(id)
    }
}

// The counterexample formal/runqueue finds for an unguarded release, forced:
// A's load outlasts its lease; the reaper hands the run back and B claims and
// opens it; A's load then fails. A's release must leave B's claim alone, so
// when B dies the reaper still finds the run and hands it back.
#[test]
fn a_late_release_leaves_the_new_holders_claim() {
    let (entered, entered_rx) = std::sync::mpsc::channel();
    let (gate, gate_rx) = std::sync::mpsc::channel();
    let store = Arc::new(FirstLoadHangsThenFails {
        inner: InMemoryStore::default(),
        hung: std::sync::Mutex::new(false),
        entered: std::sync::Mutex::new(Some(entered)),
        gate: std::sync::Mutex::new(Some(gate_rx)),
    });
    let setup = queued_run_in(store);
    let a = worker(&setup, "http://127.0.0.1:9");
    let b = worker(&setup, "http://127.0.0.1:9");
    let c = worker(&setup, "http://127.0.0.1:9");
    std::thread::scope(|scope| {
        let a_work = scope.spawn(|| a.work_one());
        entered_rx.recv().expect("A is inside its load");
        std::thread::sleep(quick().lease * 2);
        assert_eq!(setup.queue.reap().expect("reap"), [setup.run_id]);
        let b_claim = b.claim().expect("claim").expect("B claims the reaped run");
        let Prepared::Open(b_open) = b_claim.prepare().expect("prepare") else {
            panic!("the run is open");
        };
        gate.send(()).expect("fail A's load");
        assert!(a_work.join().expect("A").is_err());
        assert_eq!(
            setup.queue.processing().expect("processing"),
            [setup.run_id]
        );
        assert_eq!(setup.queue.queued().expect("queued"), []);
        assert!(c.claim().expect("claim").is_none(), "nothing to claim");
        drop(b_open);
    });
    std::thread::sleep(quick().lease * 2);
    assert_eq!(setup.queue.reap().expect("reap"), [setup.run_id]);
}

/// The in-memory store, except that one run can never be loaded.
struct OneUnloadable {
    inner: InMemoryStore,
    bad: RunId,
}

impl RunStore for OneUnloadable {
    fn put_agent(&self, agent: server::StoredAgent) -> Result<server::PutAgent, StoreError> {
        self.inner.put_agent(agent)
    }
    fn agent(&self, id: AgentId) -> Result<Option<server::StoredAgent>, StoreError> {
        self.inner.agent(id)
    }
    fn put_run(&self, run: StoredRun) -> Result<(), StoreError> {
        self.inner.put_run(run)
    }
    fn append_events(&self, id: RunId, events: Vec<Event>) -> Result<Append, StoreError> {
        self.inner.append_events(id, events)
    }
    fn run(&self, id: RunId) -> Result<Option<StoredRun>, StoreError> {
        if id == self.bad {
            return Err(StoreError::new("json: the stored run does not decode"));
        }
        self.inner.run(id)
    }
    fn put_artifact(&self, artifact: server::StoredArtifact) -> Result<(), StoreError> {
        self.inner.put_artifact(artifact)
    }
    fn artifact(
        &self,
        id: protocol::ArtifactId,
    ) -> Result<Option<server::StoredArtifact>, StoreError> {
        self.inner.artifact(id)
    }
}

// A run that can never be loaded goes to the back of the queue each time,
// so the runs queued behind it still run.
#[tokio::test(flavor = "multi_thread")]
async fn an_unloadable_run_does_not_hold_up_the_queue() {
    let jev = jev(Duration::ZERO).await;
    let uri = jev.uri();
    blocking(move || {
        let bad = spec();
        let store = Arc::new(OneUnloadable {
            inner: InMemoryStore::default(),
            bad: bad.run_id,
        });
        let setup = queued_run_in(store.clone());
        let bad_setup = Setup {
            queue: RedisRunQueue::with_key(REDIS_URL, &setup.key),
            store: store.clone(),
            run_id: bad.run_id,
            key: setup.key.clone(),
        };
        // The unloadable run is the oldest: the first a claim takes.
        store
            .inner
            .put_run(StoredRun {
                events: queued_events(&bad),
                spec: bad,
            })
            .expect("put run");
        let good: Vec<RunId> = (0..3)
            .map(|_| {
                let spec = spec();
                let run_id = spec.run_id;
                store
                    .put_run(StoredRun {
                        events: queued_events(&spec),
                        spec,
                    })
                    .expect("put run");
                run_id
            })
            .collect();
        let pushes: Vec<RunId> = std::iter::once(setup.run_id)
            .chain(good.iter().copied())
            .collect();
        redis_rpush(&setup.key, bad_setup.run_id);
        for run_id in pushes.iter().skip(1) {
            setup.queue.push(*run_id).expect("push");
        }
        let worker = worker(&setup, &uri);
        for _ in 0..8 {
            let _ = worker.work_one();
        }
        for run_id in pushes {
            assert_eq!(terminals(setup.store.as_ref(), run_id).len(), 1, "{run_id}");
        }
        assert_eq!(setup.queue.queued().expect("queued"), [bad_setup.run_id]);
    });
}

/// Puts `id` at the claiming end of the queue under `key`: the oldest run.
fn redis_rpush(key: &str, id: RunId) {
    let mut redis = redis::Client::open(REDIS_URL)
        .expect("client")
        .get_connection()
        .expect("connect");
    redis::cmd("RPUSH")
        .arg(format!("{{{key}}}"))
        .arg(id.to_string())
        .query::<()>(&mut redis)
        .expect("push");
}
