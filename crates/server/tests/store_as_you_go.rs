//! Store as you go (Phase 1.5b). A queue worker appends a run's events at
//! each step boundary with a conditional append that the store refuses once
//! the log is no longer as long as the worker last saw (`Append::Moved`). A
//! redelivered run resumes from its stored log, so recorded decisions and
//! tool results are not repeated; a worker that finds the log moved reloads
//! and resumes. Needs Postgres and Redis, as `queue_worker.rs` does.
use std::sync::{Arc, Mutex};

use harness::{run_until, Boundary, Driver, EchoTool, InMemory, ScriptedDecider};
use protocol::{
    Actor, AgentId, CredentialSource, Effect, Event, EventPayload, EventSource, ExecutionPlacement,
    InvocationId, Limits, ModelProvider, Owner, RunId, RunSpec, Timestamp, WorkModel,
};
use serde_json::json;
use server::{
    is_terminal, queued_events, Append, InMemoryStore, PostgresStore, RedisRunQueue, RunStore,
    StoredRun, Worker,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const POSTGRES_URL: &str = "postgres://gol:gol@127.0.0.1/gol";
const REDIS_URL: &str = "redis://127.0.0.1/";

fn stores() -> Vec<Arc<dyn RunStore>> {
    vec![
        Arc::new(InMemoryStore::default()),
        Arc::new(PostgresStore::connect(POSTGRES_URL).expect("connect")),
    ]
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
        .capabilities(vec![protocol::Capability::new("tool.echo")])
        .limits(Limits {
            max_steps: 6,
            max_model_calls: 1,
        })
        .build()
}

fn event(spec: &RunSpec, payload: EventPayload) -> Event {
    Event::record(
        EventSource::for_spec(spec, Actor::System, Timestamp::now()),
        payload,
    )
}

fn message(spec: &RunSpec, text: &str) -> Event {
    event(
        spec,
        EventPayload::UserMessage {
            text: text.to_string(),
        },
    )
}

fn payloads(events: &[Event]) -> Vec<EventPayload> {
    events.iter().map(|event| event.payload.clone()).collect()
}

fn count(events: &[Event], wanted: fn(&EventPayload) -> bool) -> usize {
    events.iter().filter(|event| wanted(&event.payload)).count()
}

/// Runs `work` on a plain thread: the stores and the queue block, which the
/// Postgres client refuses on a Tokio worker.
fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::spawn(work).join().expect("thread")
}

#[test]
fn a_conditional_append_is_refused_once_the_log_moved() {
    for store in stores() {
        let spec = spec();
        let id = spec.run_id;
        store
            .put_run(StoredRun {
                events: queued_events(&spec),
                spec: spec.clone(),
            })
            .expect("put run");
        assert_eq!(
            store.append_events_after(id, 3, vec![message(&spec, "a")]),
            Ok(Append::Appended)
        );
        // Another writer appended since this one read three events.
        assert_eq!(
            store.append_events_after(id, 3, vec![message(&spec, "b")]),
            Ok(Append::Moved)
        );
        let ended = server::run_failed_event(
            &spec,
            protocol::FailureClass::Infrastructure,
            "stop".to_string(),
        );
        assert_eq!(
            store.append_events_after(id, 4, vec![ended]),
            Ok(Append::Appended)
        );
        assert_eq!(
            store.append_events_after(id, 5, vec![message(&spec, "c")]),
            Ok(Append::Terminal)
        );
        assert_eq!(
            store.append_events_after(RunId::new(), 0, vec![message(&spec, "d")]),
            Ok(Append::Missing)
        );
        let events = store.run(id).expect("read").expect("run").events;
        assert_eq!(events.len(), 5);
        assert_eq!(
            payloads(&events[3..4]),
            [EventPayload::UserMessage {
                text: "a".to_string()
            }]
        );
    }
}

// Sixteen writers that all read the same log append at once: one lands, the
// others are told the log moved.
#[test]
fn racing_conditional_appends_one_lands() {
    for store in stores() {
        let spec = spec();
        let id = spec.run_id;
        store
            .put_run(StoredRun {
                events: queued_events(&spec),
                spec: spec.clone(),
            })
            .expect("put run");
        let start = Arc::new(std::sync::Barrier::new(16));
        let racers: Vec<_> = (0..16)
            .map(|n| {
                let (store, start, spec) = (store.clone(), start.clone(), spec.clone());
                std::thread::spawn(move || {
                    start.wait();
                    store.append_events_after(id, 3, vec![message(&spec, &n.to_string())])
                })
            })
            .collect();
        let results: Vec<Append> = racers
            .into_iter()
            .map(|racer| racer.join().unwrap().expect("append"))
            .collect();
        assert_eq!(
            results.iter().filter(|r| **r == Append::Appended).count(),
            1
        );
        assert_eq!(results.iter().filter(|r| **r == Append::Moved).count(), 15);
        assert_eq!(store.run(id).expect("read").expect("run").events.len(), 4);
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

/// Jev answers each label once, in order, and repeats the last one.
async fn jev(labels: &[&str]) -> MockServer {
    let server = MockServer::start().await;
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

fn worker(store: Arc<dyn RunStore>, key: &str, jev_uri: &str) -> Worker {
    Worker::builder()
        .queue(RedisRunQueue::with_key(REDIS_URL, key))
        .store(store)
        .memory(Arc::new(InMemory::default()))
        .jev(jev_uri)
        .build()
}

/// `spec`'s run as a worker that died after its first step stored it: the
/// queue's events, the scheduling ladder, and one echo step.
fn one_step_stored(spec: &RunSpec) -> Vec<Event> {
    let mut events = queued_events(spec);
    for payload in [
        EventPayload::RunScheduled,
        EventPayload::RunProvisioning,
        EventPayload::RunStarting,
    ] {
        events.push(event(spec, payload));
    }
    let mut driver = Driver::resume(spec.clone(), events).expect("resume");
    let mut decider = ScriptedDecider::new([Effect::ToolCall {
        name: "echo".to_string(),
        input: "hello".to_string(),
        invocation: InvocationId::new(),
    }]);
    let echo = EchoTool;
    let mut boundaries = 0;
    run_until(
        &mut driver,
        &mut decider,
        &[&echo],
        &harness::UnavailableModel,
        &InMemory::default(),
        &mut |_| {
            boundaries += 1;
            if boundaries > 1 {
                Boundary::Pause
            } else {
                Boundary::Continue
            }
        },
    )
    .expect("first step");
    driver.events().to_vec()
}

// A worker died after storing a step. The next delivery resumes from the
// stored log: Jev is asked only for what comes after, and the step is not
// recorded twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_redelivered_run_does_not_repeat_recorded_steps() {
    for which in 0..2 {
        let server = jev(&["complete"]).await;
        let uri = server.uri();
        blocking(move || {
            let store = stores().swap_remove(which);
            let spec = spec();
            let stored = one_step_stored(&spec);
            store
                .put_run(StoredRun {
                    spec: spec.clone(),
                    events: stored.clone(),
                })
                .expect("put run");
            let key = format!("gol:test:{}", RunId::new());
            RedisRunQueue::with_key(REDIS_URL, &key)
                .push(spec.run_id)
                .expect("push");
            let worker = worker(store.clone(), &key, &uri);
            assert_eq!(worker.work_one().expect("work"), Some(spec.run_id));
            let events = store.run(spec.run_id).expect("read").expect("run").events;
            assert_eq!(payloads(&events[..stored.len()]), payloads(&stored));
            assert_eq!(count(&events, |p| matches!(p, EventPayload::RunStarted)), 1);
            assert_eq!(
                count(&events, |p| matches!(p, EventPayload::ToolResult { .. })),
                1
            );
            assert!(matches!(
                events.last().map(|event| &event.payload),
                Some(EventPayload::RunCompleted { .. })
            ));
        });
        assert_eq!(server.received_requests().await.expect("requests").len(), 1);
    }
}

/// A store that lets another writer append just before the worker's
/// `at`-th conditional append (the `WriteAfterSnapshot` pattern), and
/// records the log length after each of the worker's appends.
struct AppendsBefore {
    inner: Arc<dyn RunStore>,
    at: usize,
    write: Mutex<Option<Vec<Event>>>,
    /// Set: a new user message lands before every conditional append.
    every: Option<RunSpec>,
    /// Set: this run ends before the second read of it (a worker's reload).
    end_on_reload: Option<Event>,
    /// Set: at the `at`-th append, this Redis key (the run's lease) passes to
    /// another holder, as when the lease ran out under a live worker.
    steal_lease: Option<String>,
    reads: Mutex<usize>,
    calls: Mutex<usize>,
    lengths: Mutex<Vec<usize>>,
}

impl AppendsBefore {
    fn new(inner: Arc<dyn RunStore>, at: usize, write: Vec<Event>) -> Self {
        Self {
            inner,
            at,
            write: Mutex::new(Some(write)),
            every: None,
            end_on_reload: None,
            steal_lease: None,
            reads: Mutex::new(0),
            calls: Mutex::new(0),
            lengths: Mutex::new(Vec::new()),
        }
    }
}

impl RunStore for AppendsBefore {
    fn put_agent(
        &self,
        agent: server::StoredAgent,
    ) -> Result<server::PutAgent, server::StoreError> {
        self.inner.put_agent(agent)
    }
    fn agent(&self, id: AgentId) -> Result<Option<server::StoredAgent>, server::StoreError> {
        self.inner.agent(id)
    }
    fn agents_of(&self, owner: &Owner) -> Result<Vec<server::StoredAgent>, server::StoreError> {
        self.inner.agents_of(owner)
    }
    fn put_run(&self, run: StoredRun) -> Result<server::PutRun, server::StoreError> {
        self.inner.put_run(run)
    }
    fn append_events(&self, id: RunId, events: Vec<Event>) -> Result<Append, server::StoreError> {
        self.inner.append_events(id, events)
    }
    fn append_events_after(
        &self,
        id: RunId,
        seen: usize,
        events: Vec<Event>,
    ) -> Result<Append, server::StoreError> {
        let call = {
            let mut calls = self.calls.lock().unwrap();
            *calls += 1;
            *calls
        };
        if call == self.at {
            if let Some(write) = self.write.lock().unwrap().take() {
                self.inner.append_events(id, write)?;
            }
            if let Some(lease) = &self.steal_lease {
                let mut redis = redis::Client::open(REDIS_URL)
                    .expect("client")
                    .get_connection()
                    .expect("connect");
                redis::cmd("SET")
                    .arg(lease)
                    .arg("another-worker")
                    .query::<()>(&mut redis)
                    .expect("steal the lease");
            }
        }
        if let Some(spec) = &self.every {
            self.inner
                .append_events(id, vec![message(spec, &format!("again {call}"))])?;
        }
        let appended = self.inner.append_events_after(id, seen, events)?;
        if appended == Append::Appended {
            let length = self.inner.run(id)?.map_or(0, |run| run.events.len());
            self.lengths.lock().unwrap().push(length);
        }
        Ok(appended)
    }
    fn run(&self, id: RunId) -> Result<Option<StoredRun>, server::StoreError> {
        let read = {
            let mut reads = self.reads.lock().unwrap();
            *reads += 1;
            *reads
        };
        if read == 2 {
            if let Some(end) = &self.end_on_reload {
                self.inner.append_events(id, vec![end.clone()])?;
            }
        }
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
}

/// Runs a queued run of `spec` once, with another writer appending `write`
/// just before the worker's `at`-th conditional append. Returns the spec, the
/// stored log, and the log's length after each of the worker's appends.
fn run_with_write(
    which: usize,
    uri: String,
    at: usize,
    write: fn(&RunSpec) -> Vec<Event>,
) -> (RunSpec, Vec<Event>, Vec<usize>) {
    let inner = stores().swap_remove(which);
    let spec = spec();
    inner
        .put_run(StoredRun {
            spec: spec.clone(),
            events: queued_events(&spec),
        })
        .expect("put run");
    let store = Arc::new(AppendsBefore::new(inner, at, write(&spec)));
    let key = format!("gol:test:{}", RunId::new());
    let queue = RedisRunQueue::with_key(REDIS_URL, &key);
    queue.push(spec.run_id).expect("push");
    let worker = worker(store.clone(), &key, &uri);
    assert_eq!(worker.work_one().expect("work"), Some(spec.run_id));
    let events = store.run(spec.run_id).expect("read").expect("run").events;
    let lengths = store.lengths.lock().unwrap().clone();
    (spec, events, lengths)
}

// The log grows while the run runs: the ladder, RunStarted, each step, and
// the tail are separate appends, each extending the one before.
#[tokio::test(flavor = "multi_thread")]
async fn a_running_runs_log_grows_step_by_step() {
    for which in 0..2 {
        let server = jev(&["echo", "echo", "complete"]).await;
        let uri = server.uri();
        let (_, events, lengths) = blocking(move || run_with_write(which, uri, 0, |_| Vec::new()));
        assert_eq!(lengths.len(), 5, "{lengths:?}");
        assert!(lengths.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(lengths.last(), Some(&events.len()));
        assert_eq!(count(&events, is_terminal), 1);
    }
}

// A user message lands mid-run, just before the worker stores its first
// step. That append is refused as moved; the worker reloads and resumes from
// the stored log, so the unstored step is decided again (Jev is asked once
// more) and the run ends with the message kept once and one step stored.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_message_mid_run_is_absorbed() {
    for which in 0..2 {
        let server = jev(&["echo", "echo", "complete"]).await;
        let uri = server.uri();
        let (_, events, _) = blocking(move || {
            run_with_write(which, uri, 3, |spec| vec![message(spec, "one more thing")])
        });
        assert_eq!(
            count(&events, |p| matches!(
                p,
                EventPayload::UserMessage { text } if text == "one more thing"
            )),
            1
        );
        assert_eq!(
            count(&events, |p| matches!(p, EventPayload::RunScheduled)),
            1
        );
        assert_eq!(count(&events, |p| matches!(p, EventPayload::RunStarted)), 1);
        assert_eq!(
            count(&events, |p| matches!(p, EventPayload::ToolResult { .. })),
            1
        );
        assert!(matches!(
            events.last().map(|event| &event.payload),
            Some(EventPayload::RunCompleted { .. })
        ));
        assert_eq!(server.received_requests().await.expect("requests").len(), 3);
    }
}

// A stale holder: another worker ended the run before this one's first
// append. The append is refused as terminal, nothing of this worker's is
// stored, and the run keeps one end.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_holders_append_is_refused() {
    for which in 0..2 {
        let server = jev(&["complete"]).await;
        let uri = server.uri();
        let (spec, events, _) = blocking(move || {
            run_with_write(which, uri, 1, |spec| {
                vec![server::run_failed_event(
                    spec,
                    protocol::FailureClass::Infrastructure,
                    "ended by the other holder".to_string(),
                )]
            })
        });
        let mut expected = payloads(&queued_events(&spec));
        expected.push(EventPayload::RunFailed {
            class: protocol::FailureClass::Infrastructure,
            message: "ended by the other holder".to_string(),
        });
        assert_eq!(payloads(&events), expected);
        assert_eq!(server.received_requests().await.expect("requests").len(), 0);
    }
}

// Another writer appends before every one of this worker's appends. After
// three reloads the worker stores no end and hands the run back to the queue
// instead of acknowledging it, so the run is not lost.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_that_keeps_moving_is_handed_back() {
    for which in 0..2 {
        let server = jev(&["complete"]).await;
        let uri = server.uri();
        blocking(move || {
            let inner = stores().swap_remove(which);
            let spec = spec();
            inner
                .put_run(StoredRun {
                    spec: spec.clone(),
                    events: queued_events(&spec),
                })
                .expect("put run");
            let mut store = AppendsBefore::new(inner, 0, Vec::new());
            store.every = Some(spec.clone());
            let store = Arc::new(store);
            let key = format!("gol:test:{}", RunId::new());
            let queue = RedisRunQueue::with_key(REDIS_URL, &key);
            queue.push(spec.run_id).expect("push");
            let worker = worker(store.clone(), &key, &uri);
            assert_eq!(worker.work_one().expect("work"), Some(spec.run_id));
            let events = store.run(spec.run_id).expect("read").expect("run").events;
            assert_eq!(count(&events, is_terminal), 0);
            assert!(store.lengths.lock().unwrap().is_empty());
            assert_eq!(*store.calls.lock().unwrap(), 4);
            assert_eq!(queue.queued().expect("queued"), [spec.run_id]);
            assert_eq!(queue.processing().expect("processing"), []);
        });
    }
}

// Forced: the worker's first append is refused as moved (a user message
// landed), and before its reload another writer ends the run. The reload
// finds the end, so the worker stores nothing, reports the log terminal, and
// never asks Jev.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_ended_before_the_reload_is_left_ended() {
    for which in 0..2 {
        let server = jev(&["complete"]).await;
        let uri = server.uri();
        blocking(move || {
            let inner = stores().swap_remove(which);
            let spec = spec();
            inner
                .put_run(StoredRun {
                    spec: spec.clone(),
                    events: queued_events(&spec),
                })
                .expect("put run");
            let mut store = AppendsBefore::new(inner, 1, vec![message(&spec, "late")]);
            let end = server::run_failed_event(
                &spec,
                protocol::FailureClass::Infrastructure,
                "ended elsewhere".to_string(),
            );
            store.end_on_reload = Some(end);
            let store = Arc::new(store);
            let key = format!("gol:test:{}", RunId::new());
            RedisRunQueue::with_key(REDIS_URL, &key)
                .push(spec.run_id)
                .expect("push");
            let worker = worker(store.clone(), &key, &uri);
            let claim = worker.claim().expect("claim").expect("a run");
            let server::Prepared::Open(open) = claim.prepare().expect("prepare") else {
                panic!("the run is open");
            };
            let done = open.execute().record().expect("record");
            assert_eq!(done.recorded(), Some(Append::Terminal));
            done.ack().expect("ack");
            let events = store.run(spec.run_id).expect("read").expect("run").events;
            let mut expected = payloads(&queued_events(&spec));
            expected.push(EventPayload::UserMessage {
                text: "late".to_string(),
            });
            expected.push(EventPayload::RunFailed {
                class: protocol::FailureClass::Infrastructure,
                message: "ended elsewhere".to_string(),
            });
            assert_eq!(payloads(&events), expected);
        });
        assert_eq!(server.received_requests().await.expect("requests").len(), 0);
    }
}

// The lease passes to another worker while this one runs (it ran out under
// a live worker). At its next step boundary this worker renews, finds the
// lease gone, and stops: it stores nothing more, never asks Jev, and leaves
// the run in the new holder's hands instead of handing it back.
#[tokio::test(flavor = "multi_thread")]
async fn a_worker_that_lost_its_lease_stops_at_the_next_boundary() {
    for which in 0..2 {
        let server = jev(&["complete"]).await;
        let uri = server.uri();
        blocking(move || {
            let inner = stores().swap_remove(which);
            let spec = spec();
            inner
                .put_run(StoredRun {
                    spec: spec.clone(),
                    events: queued_events(&spec),
                })
                .expect("put run");
            let key = format!("gol:test:{}", RunId::new());
            let mut store = AppendsBefore::new(inner, 1, Vec::new());
            store.steal_lease = Some(format!("{{{key}}}:lease:{}", spec.run_id));
            let store = Arc::new(store);
            let queue = RedisRunQueue::with_key(REDIS_URL, &key);
            queue.push(spec.run_id).expect("push");
            let worker = worker(store.clone(), &key, &uri);
            assert_eq!(worker.work_one().expect("work"), Some(spec.run_id));
            let events = store.run(spec.run_id).expect("read").expect("run").events;
            assert_eq!(
                payloads(&events[3..]),
                [
                    EventPayload::RunScheduled,
                    EventPayload::RunProvisioning,
                    EventPayload::RunStarting,
                ]
            );
            assert_eq!(queue.processing().expect("processing"), [spec.run_id]);
            assert_eq!(queue.queued().expect("queued"), []);
        });
        assert_eq!(server.received_requests().await.expect("requests").len(), 0);
    }
}

/// Steals `spec`'s lease on queue `key` for another worker.
fn steal_lease(key: &str, spec: &RunSpec) {
    let mut redis = redis::Client::open(REDIS_URL)
        .expect("client")
        .get_connection()
        .expect("connect");
    redis::cmd("SET")
        .arg(format!("{{{key}}}:lease:{}", spec.run_id))
        .arg("another-worker")
        .query::<()>(&mut redis)
        .expect("steal the lease");
}

// Review of #69: the lease is lost while Jev decides the step that would end
// the run (a completion, or a decider error). run_until returns without a
// boundary, so the write that ends the run must check the lease too: this
// worker stores no end, and leaves the run in processing for its new holder.
#[tokio::test(flavor = "multi_thread")]
async fn a_worker_that_lost_its_lease_during_the_last_step_does_not_end_the_run() {
    for which in 0..2 {
        for fails in [false, true] {
            let server = MockServer::start().await;
            let response = if fails {
                ResponseTemplate::new(500)
            } else {
                ResponseTemplate::new(200).set_body_json(answer("complete"))
            };
            Mock::given(method("POST"))
                .and(path("/v1/systemone"))
                .respond_with(response.set_delay(std::time::Duration::from_millis(600)))
                .mount(&server)
                .await;
            let uri = server.uri();
            blocking(move || {
                let store = stores().swap_remove(which);
                let spec = spec();
                store
                    .put_run(StoredRun {
                        spec: spec.clone(),
                        events: queued_events(&spec),
                    })
                    .expect("put run");
                let key = format!("gol:test:{}", RunId::new());
                let queue = RedisRunQueue::with_key(REDIS_URL, &key);
                queue.push(spec.run_id).expect("push");
                let worker = worker(store.clone(), &key, &uri);
                let worked = std::thread::scope(|scope| {
                    let running = scope.spawn(|| worker.work_one());
                    // Jev is answering the first decision.
                    std::thread::sleep(std::time::Duration::from_millis(250));
                    steal_lease(&key, &spec);
                    running.join().expect("worker")
                });
                assert_eq!(worked.expect("work"), Some(spec.run_id));
                let events = store.run(spec.run_id).expect("read").expect("run").events;
                assert_eq!(count(&events, is_terminal), 0, "{:?}", payloads(&events));
                assert_eq!(queue.processing().expect("processing"), [spec.run_id]);
            });
        }
    }
}

// Review of #69: a resumed run first performs what its log left pending (here
// a delegation cut after its authorization). A worker whose lease is already
// gone must not perform it: no child run is started, nothing is stored, and
// the run stays with its new holder.
#[tokio::test(flavor = "multi_thread")]
async fn a_worker_without_its_lease_does_not_redo_a_pending_effect() {
    for which in 0..2 {
        let server = jev(&["complete"]).await;
        let uri = server.uri();
        blocking(move || {
            let store = stores().swap_remove(which);
            let owner = Owner::new("https://issuer.test", format!("user-{}", RunId::new()), "t");
            let writer = AgentId::new();
            store
                .put_agent(server::StoredAgent {
                    manifest: server::AgentManifest {
                        id: writer,
                        version: "1".to_string(),
                        name: "writer".to_string(),
                        description: String::new(),
                        instructions: "Write.".to_string(),
                        tools: Vec::new(),
                        required_capabilities: Vec::new(),
                    },
                    owner: owner.clone(),
                })
                .expect("put agent");
            let mut spec = spec();
            spec.owner = owner;
            // Room to give a child two steps and two model calls.
            spec.limits = Limits {
                max_steps: 8,
                max_model_calls: 4,
            };
            spec.capabilities
                .push(protocol::Capability::new("agent.delegate"));
            // The log a worker left when it died after authorizing the
            // delegation and before starting the child.
            let mut events = queued_events(&spec);
            for payload in [
                EventPayload::RunScheduled,
                EventPayload::RunProvisioning,
                EventPayload::RunStarting,
            ] {
                events.push(event(&spec, payload));
            }
            let mut driver = Driver::resume(spec.clone(), events).expect("resume");
            let mut decider = ScriptedDecider::new([Effect::Delegate {
                agent_id: writer,
                input: "draft".to_string(),
            }]);
            let _ = run_until(
                &mut driver,
                &mut decider,
                &[],
                &harness::UnavailableModel,
                &InMemory::default(),
                &mut |_| Boundary::Continue,
            );
            let authorized = driver
                .events()
                .iter()
                .position(|event| matches!(event.payload, EventPayload::EffectAuthorized { .. }))
                .expect("authorized");
            let stored = driver.events()[..=authorized].to_vec();
            store
                .put_run(StoredRun {
                    spec: spec.clone(),
                    events: stored.clone(),
                })
                .expect("put run");
            let key = format!("gol:test:{}", RunId::new());
            let queue = RedisRunQueue::with_key(REDIS_URL, &key);
            queue.push(spec.run_id).expect("push");
            let worker = worker(store.clone(), &key, &uri);
            let claim = worker.claim().expect("claim").expect("a run");
            let server::Prepared::Open(open) = claim.prepare().expect("prepare") else {
                panic!("the run is open");
            };
            steal_lease(&key, &spec);
            let done = open.execute().record().expect("record");
            done.ack().expect("ack");
            let events = store.run(spec.run_id).expect("read").expect("run").events;
            assert_eq!(payloads(&events), payloads(&stored));
            assert_eq!(queue.queued().expect("queued"), []);
            assert_eq!(queue.processing().expect("processing"), [spec.run_id]);
        });
        assert_eq!(server.received_requests().await.expect("requests").len(), 0);
    }
}
