//! The Redis run queue and its workers (C4): a claim takes the run and its
//! lease in one step, a worker acknowledges only after the run's terminal
//! event is stored, and the reaper hands back a run whose lease expired.
//! Needs Postgres and Redis, as `pg_redis.rs` does.
mod common;

use std::sync::Arc;
use std::time::Duration;

use harness::InMemory;
use protocol::{
    AgentId, CredentialSource, Event, EventPayload, ExecutionPlacement, Limits, ModelProvider,
    Owner, RunId, RunSpec, WorkModel,
};
use server::{
    is_terminal, queued_events, reap_forever, sweep, Append, InMemoryStore, PostgresStore,
    Prepared, QueueTiming, RedisRunQueue, RunStore, StoreError, StoredRun, Worker,
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
        sweep_after: Duration::ZERO,
        forget_after: Duration::ZERO,
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
// acknowledges it; then A executes. A's first write (the scheduling ladder)
// finds its lease gone, so A stores nothing and never asks Jev (Phase 1.5b:
// before, A ran the whole run and its one append was refused as terminal). The store keeps one terminal event, and A's acknowledgement
// leaves the queue empty.
#[tokio::test(flavor = "multi_thread")]
async fn two_workers_one_terminal() {
    let jev = jev(Duration::ZERO).await;
    two_workers(jev.uri(), queued_run).await;
    assert_eq!(jev.received_requests().await.expect("requests").len(), 1);
    two_workers(jev.uri(), || {
        queued_run_in(Arc::new(server::InMemoryStore::default()))
    })
    .await;
    assert_eq!(jev.received_requests().await.expect("requests").len(), 2);
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
        // A's lease ran out, so its first write stops at the lease check and
        // stores nothing (reported as `Moved`; its release does nothing).
        assert_eq!(done.recorded(), Some(Append::Moved));
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
    fn agents_of(&self, owner: &Owner) -> Result<Vec<server::StoredAgent>, StoreError> {
        self.0.agents_of(owner)
    }
    fn put_run(&self, run: StoredRun) -> Result<server::PutRun, StoreError> {
        self.0.put_run(run)
    }
    fn append_events(&self, _id: RunId, _events: Vec<Event>) -> Result<Append, StoreError> {
        Err(StoreError::new("appends are down"))
    }
    fn append_events_after(
        &self,
        _id: RunId,
        _seen: usize,
        _events: Vec<Event>,
    ) -> Result<Append, StoreError> {
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
    fn agents_of(&self, owner: &Owner) -> Result<Vec<server::StoredAgent>, StoreError> {
        self.0.agents_of(owner)
    }
    fn put_run(&self, run: StoredRun) -> Result<server::PutRun, StoreError> {
        self.0.put_run(run)
    }
    fn append_events(&self, id: RunId, events: Vec<Event>) -> Result<Append, StoreError> {
        self.0.append_events(id, events)
    }
    fn append_events_after(
        &self,
        id: RunId,
        seen: usize,
        events: Vec<Event>,
    ) -> Result<Append, StoreError> {
        self.0.append_events_after(id, seen, events)
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
    fn agents_of(&self, owner: &Owner) -> Result<Vec<server::StoredAgent>, StoreError> {
        self.inner.agents_of(owner)
    }
    fn put_run(&self, run: StoredRun) -> Result<server::PutRun, StoreError> {
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
        self.inner.append_events_after(id, seen, events)
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
    fn agents_of(&self, owner: &Owner) -> Result<Vec<server::StoredAgent>, StoreError> {
        self.inner.agents_of(owner)
    }
    fn put_run(&self, run: StoredRun) -> Result<server::PutRun, StoreError> {
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
        self.inner.append_events_after(id, seen, events)
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

/// A run as a producer leaves it when it dies between the store and the
/// push: pending, stored as created and queued, and not on the queue.
fn stranded_run() -> Setup {
    let key = format!("gol:test:{}", RunId::new());
    let queue = RedisRunQueue::with_key(REDIS_URL, &key);
    let store: Arc<dyn RunStore> = Arc::new(PostgresStore::connect(POSTGRES_URL).expect("connect"));
    let spec = spec();
    let run_id = spec.run_id;
    queue.pend(run_id).expect("pend");
    store
        .put_run(StoredRun {
            events: queued_events(&spec),
            spec,
        })
        .expect("put run");
    Setup {
        queue,
        store,
        run_id,
        key,
    }
}

// A producer that dies between storing a run and pushing it leaves the run
// pending. The sweep pushes it, and a worker ends it.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_stored_but_not_pushed_is_queued_by_the_sweep() {
    let jev = jev(Duration::ZERO).await;
    let uri = jev.uri();
    blocking(move || {
        let setup = stranded_run();
        assert_eq!(setup.queue.pending().expect("pending"), [setup.run_id]);
        assert_eq!(setup.queue.queued().expect("queued"), []);
        assert_eq!(
            sweep(
                &setup.queue,
                setup.store.as_ref(),
                Duration::ZERO,
                Duration::ZERO
            ),
            Ok(vec![setup.run_id])
        );
        assert_eq!(setup.queue.pending().expect("pending"), []);
        assert_eq!(setup.queue.queued().expect("queued"), [setup.run_id]);
        assert_eq!(
            worker(&setup, &uri).work_one().expect("work"),
            Some(setup.run_id)
        );
        assert!(matches!(
            terminals(setup.store.as_ref(), setup.run_id).as_slice(),
            [Event {
                payload: EventPayload::RunCompleted { .. },
                ..
            }]
        ));
        assert_eq!(setup.queue.processing().expect("processing"), []);
    });
}

// Inside its grace a pending run is left alone: its producer may be about
// to push it.
#[test]
fn a_pending_run_inside_its_grace_is_left() {
    let setup = stranded_run();
    assert_eq!(
        sweep(
            &setup.queue,
            setup.store.as_ref(),
            Duration::from_secs(60),
            Duration::ZERO
        ),
        Ok(vec![])
    );
    assert_eq!(setup.queue.pending().expect("pending"), [setup.run_id]);
    assert_eq!(setup.queue.queued().expect("queued"), []);
}

// A pending run that is not stored yet may belong to a producer whose put is
// still in flight: it stays pending until `forget_after`, then is dropped.
#[test]
fn a_pending_run_not_yet_stored_is_kept_until_forgotten() {
    let queue = RedisRunQueue::with_key(REDIS_URL, format!("gol:test:{}", RunId::new()));
    let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
    let run_id = RunId::new();
    queue.pend(run_id).expect("pend");
    assert_eq!(
        sweep(&queue, &store, Duration::ZERO, Duration::from_secs(3600)),
        Ok(vec![])
    );
    assert_eq!(queue.pending().expect("pending"), [run_id]);
    assert_eq!(
        sweep(&queue, &store, Duration::ZERO, Duration::ZERO),
        Ok(vec![])
    );
    assert_eq!(queue.pending().expect("pending"), []);
}

/// Postgres, cutting the queue's Redis connections while it loads `cut_on`.
struct CutsRedis {
    inner: PostgresStore,
    proxy: common::redis_proxy::RedisProxy,
    cut_on: RunId,
}

impl RunStore for CutsRedis {
    fn put_agent(&self, agent: server::StoredAgent) -> Result<server::PutAgent, StoreError> {
        self.inner.put_agent(agent)
    }
    fn agent(&self, id: AgentId) -> Result<Option<server::StoredAgent>, StoreError> {
        self.inner.agent(id)
    }
    fn agents_of(&self, owner: &Owner) -> Result<Vec<server::StoredAgent>, StoreError> {
        self.inner.agents_of(owner)
    }
    fn put_run(&self, run: StoredRun) -> Result<server::PutRun, StoreError> {
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
        self.inner.append_events_after(id, seen, events)
    }
    fn run(&self, id: RunId) -> Result<Option<StoredRun>, StoreError> {
        if id == self.cut_on {
            self.proxy.cut();
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

// A Redis error on one pending run does not stop the sweep: the next run is
// still pushed, and the failed one stays pending for the next sweep.
#[test]
fn a_redis_error_on_one_run_does_not_stop_the_sweep() {
    let proxy = common::redis_proxy::RedisProxy::start();
    let queue = RedisRunQueue::with_key(proxy.url(0), format!("gol:test:{}", RunId::new()));
    let postgres = PostgresStore::connect(POSTGRES_URL).expect("connect");
    let (first, second) = (spec(), spec());
    for spec in [&first, &second] {
        queue.pend(spec.run_id).expect("pend");
        postgres
            .put_run(StoredRun {
                events: queued_events(spec),
                spec: spec.clone(),
            })
            .expect("put run");
        std::thread::sleep(Duration::from_millis(5));
    }
    let store = CutsRedis {
        inner: postgres,
        proxy,
        cut_on: first.run_id,
    };
    assert_eq!(
        sweep(&queue, &store, Duration::ZERO, Duration::ZERO),
        Ok(vec![second.run_id])
    );
    assert_eq!(queue.pending().expect("pending"), [first.run_id]);
    assert_eq!(queue.queued().expect("queued"), [second.run_id]);
}

// A pending run that was never stored (its producer died before the store,
// or the store failed) is dropped once forgotten, not pushed.
#[test]
fn a_pending_run_that_was_never_stored_is_dropped() {
    let queue = RedisRunQueue::with_key(REDIS_URL, format!("gol:test:{}", RunId::new()));
    let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
    let run_id = RunId::new();
    queue.pend(run_id).expect("pend");
    assert_eq!(
        sweep(&queue, &store, Duration::ZERO, Duration::ZERO),
        Ok(vec![])
    );
    assert_eq!(queue.pending().expect("pending"), []);
    assert_eq!(queue.queued().expect("queued"), []);
}

// A pending run whose log has moved past created and queued (a worker
// started it, or it ended) is not pushed again; its entry is dropped. The
// push takes a run off pending in the same script, so this only guards a
// run that reached the queue some other way; "started" is what a worker
// appends when it opens a run.
#[test]
fn a_pending_run_that_moved_on_is_not_pushed() {
    for ended in [false, true] {
        let setup = stranded_run();
        let stored = setup.store.run(setup.run_id).expect("store").expect("run");
        let next: Vec<Event> = if ended {
            vec![server::run_failed_event(
                &stored.spec,
                protocol::FailureClass::Infrastructure,
                "ended elsewhere".to_string(),
            )]
        } else {
            [
                EventPayload::RunScheduled,
                EventPayload::RunProvisioning,
                EventPayload::RunStarting,
            ]
            .into_iter()
            .map(|payload| {
                Event::record(
                    protocol::EventSource::new(
                        setup.run_id,
                        stored.spec.agent_id,
                        &stored.spec.agent_version,
                        protocol::Actor::System,
                        protocol::Timestamp::now(),
                    ),
                    payload,
                )
            })
            .collect()
        };
        assert_eq!(
            setup.store.append_events(setup.run_id, next),
            Ok(Append::Appended)
        );
        assert_eq!(
            sweep(
                &setup.queue,
                setup.store.as_ref(),
                Duration::ZERO,
                Duration::ZERO
            ),
            Ok(vec![]),
            "ended: {ended}"
        );
        assert_eq!(
            setup.queue.pending().expect("pending"),
            [],
            "ended: {ended}"
        );
        assert_eq!(setup.queue.queued().expect("queued"), [], "ended: {ended}");
    }
}

// A malformed pending entry that cannot be dropped (here Redis refuses
// ZREM to this client) is logged and left, and the sweep still returns
// rather than failing. (That the sweep goes on past a failed entry is
// `a_redis_error_on_one_run_does_not_stop_the_sweep`.)
#[test]
fn a_malformed_entry_that_cannot_be_dropped_does_not_end_the_sweep() {
    let user = format!("gol-nozrem-{}", RunId::new());
    let mut admin = redis::Client::open(REDIS_URL)
        .expect("client")
        .get_connection()
        .expect("connect");
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&user)
        .arg("on")
        .arg(">pw")
        .arg("~*")
        .arg("&*")
        .arg("+@all")
        .arg("-zrem")
        .query::<()>(&mut admin)
        .expect("acl user");
    let key = format!("gol:test:{}", RunId::new());
    redis::cmd("ZADD")
        .arg(format!("{{{key}}}:pending"))
        .arg(0)
        .arg("not-a-run-id")
        .query::<()>(&mut admin)
        .expect("malformed entry");
    let queue = RedisRunQueue::with_key(format!("redis://{user}:pw@127.0.0.1/"), &key);
    let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
    let swept = sweep(&queue, &store, Duration::ZERO, Duration::ZERO);
    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&user)
        .query::<()>(&mut admin)
        .expect("drop acl user");
    assert_eq!(swept, Ok(vec![]));
    let left: Vec<String> = redis::cmd("ZRANGE")
        .arg(format!("{{{key}}}:pending"))
        .arg(0)
        .arg(-1)
        .query(&mut admin)
        .expect("pending entries");
    assert_eq!(left, ["not-a-run-id"]);
}

// The producer's push takes the run off pending in the same step.
#[test]
fn a_push_clears_the_pending_entry() {
    let queue = RedisRunQueue::with_key(REDIS_URL, format!("gol:test:{}", RunId::new()));
    let run_id = RunId::new();
    queue.pend(run_id).expect("pend");
    queue.push(run_id).expect("push");
    assert_eq!(queue.pending().expect("pending"), []);
    assert_eq!(queue.queued().expect("queued"), [run_id]);
}

// The reaper's loop sweeps too: a stranded run is queued within a few rounds
// once it has been pending for `sweep_after`.
#[test]
fn the_reaper_sweeps_stranded_runs() {
    let setup = stranded_run();
    let queue = RedisRunQueue::with_key(REDIS_URL, &setup.key);
    let store = setup.store.clone();
    // The loop runs for as long as the test process does.
    std::thread::spawn(move || reap_forever(&queue, store.as_ref(), quick()));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while setup.queue.queued().expect("queued").is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the reaper never swept the run"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(setup.queue.queued().expect("queued"), [setup.run_id]);
    assert_eq!(setup.queue.pending().expect("pending"), []);
}

// Every event the server records for a child run names its parent.
#[test]
fn the_servers_events_for_a_child_name_its_parent() {
    let parent = spec();
    let child = RunSpec::builder()
        .owner(parent.owner.clone())
        .agent(AgentId::new(), "1")
        .input("draft")
        .placement(parent.placement)
        .work_model(parent.work_model.clone())
        .child_of(&parent, 1)
        .build();
    let mut events = queued_events(&child);
    events.push(server::run_failed_event(
        &child,
        protocol::FailureClass::Infrastructure,
        "stopped".to_string(),
    ));
    assert!(events
        .iter()
        .all(|event| event.envelope.parent_run_id == Some(parent.run_id)));
    assert!(queued_events(&parent)
        .iter()
        .all(|event| event.envelope.parent_run_id.is_none()));
}
