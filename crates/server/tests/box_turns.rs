//! Box background coworker turns (the 67A follow-up, decisions 82A-86C;
//! `formal/runlog/BoxTurn.tla`). Each delivery of a turn (an attempt) has
//! its own sandbox, `gol-box-<run>-<n>` (82A). A worker checks its lease
//! before it provisions, after the provision and after the gateway call; one
//! that lost it removes its own sandbox and stops (85A). Before a Box turn
//! ends, for any reason, the sandboxes of attempts 1 to n are removed, each
//! confirmed gone; one that cannot be leaves the turn open (84A). The
//! reaper removes what the host still lists for ended turns (86C). On both
//! stores; needs Redis, as `pg_redis.rs` does.
mod common;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use common::queued::{stores, Store};
use harness::InMemory;
use protocol::{
    Actor, AgentId, CredentialSource, Event, EventPayload, EventSource, ExecutionPlacement, Limits,
    ModelProvider, Owner, RunId, RunSpec, Timestamp, WorkModel,
};
use server::{
    is_terminal, queued_events, sweep_sandboxes, GatewayCall, GatewayPoster, QueueTiming,
    RedisRunQueue, SandboxError, SandboxHost, StopScope, StoredRun, Worker,
};

const REDIS_URL: &str = "redis://127.0.0.1/";

/// A Box background turn, as `POST /v1/coworker/turns` stores it.
fn spec() -> RunSpec {
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert("gol.turn".to_string(), "1".to_string());
    RunSpec::builder()
        .owner(Owner::new("https://issuer.test", "user-1", "tenant-1"))
        .agent(AgentId::new(), "1")
        .input("draft the memo")
        .placement(ExecutionPlacement::Box)
        .work_model(WorkModel {
            provider: ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .limits(Limits {
            max_steps: 4,
            max_model_calls: 1,
        })
        .metadata(metadata)
        .build()
}

fn event(spec: &RunSpec, payload: EventPayload) -> Event {
    Event::record(
        EventSource::for_spec(spec, Actor::System, Timestamp::now()),
        payload,
    )
}

fn attempt(spec: &RunSpec, n: u32) -> String {
    format!("gol-box-{}-{n}", spec.run_id)
}

/// What a test sandbox host was asked to do, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Op {
    Provision(String),
    Destroy(String),
}

type Hook = Box<dyn Fn(&str) + Send + Sync>;

/// An in-process sandbox host that records what it did, can run a hook on a
/// provision, and can fail a number of destroys.
#[derive(Default)]
struct Sandboxes {
    live: Mutex<HashSet<String>>,
    ops: Mutex<Vec<Op>>,
    failing_destroys: Mutex<u32>,
    on_provision: Mutex<Option<Hook>>,
    on_destroy: Mutex<Option<Hook>>,
}

impl Sandboxes {
    fn up(&self, name: &str) {
        self.live.lock().expect("live").insert(name.to_string());
    }
    fn live(&self) -> HashSet<String> {
        self.live.lock().expect("live").clone()
    }
    fn ops(&self) -> Vec<Op> {
        self.ops.lock().expect("ops").clone()
    }
    fn fail_destroys(&self, n: u32) {
        *self.failing_destroys.lock().expect("fails") = n;
    }
    fn on_provision(&self, hook: impl Fn(&str) + Send + Sync + 'static) {
        *self.on_provision.lock().expect("hook") = Some(Box::new(hook));
    }
    fn on_destroy(&self, hook: impl Fn(&str) + Send + Sync + 'static) {
        *self.on_destroy.lock().expect("hook") = Some(Box::new(hook));
    }
}

impl SandboxHost for Sandboxes {
    fn provision(&self, name: &str) -> Result<(), SandboxError> {
        self.ops
            .lock()
            .expect("ops")
            .push(Op::Provision(name.to_string()));
        self.up(name);
        if let Some(hook) = self.on_provision.lock().expect("hook").as_ref() {
            hook(name);
        }
        Ok(())
    }
    fn destroy(&self, name: &str) -> Result<(), SandboxError> {
        self.ops
            .lock()
            .expect("ops")
            .push(Op::Destroy(name.to_string()));
        let mut fails = self.failing_destroys.lock().expect("fails");
        if *fails > 0 {
            *fails -= 1;
            return Err(SandboxError::Host("the host is unreachable".to_string()));
        }
        self.live.lock().expect("live").remove(name);
        if let Some(hook) = self.on_destroy.lock().expect("hook").as_ref() {
            hook(name);
        }
        Ok(())
    }
    fn exists(&self, name: &str) -> bool {
        self.live.lock().expect("live").contains(name)
    }
    fn absent(&self, name: &str) -> Result<bool, SandboxError> {
        Ok(!self.exists(name))
    }
    fn list(&self) -> Result<Vec<String>, SandboxError> {
        let mut names: Vec<String> = self.live().into_iter().collect();
        names.sort();
        Ok(names)
    }
    fn launches_docker(&self) -> bool {
        false
    }
}

/// A gateway that answers, records the sandboxes live at each call, and
/// can run a hook during the call.
struct Gateway {
    sandboxes: Arc<Sandboxes>,
    seen: Mutex<Vec<HashSet<String>>>,
    during: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

impl Gateway {
    fn new(sandboxes: Arc<Sandboxes>) -> Arc<Self> {
        Arc::new(Self {
            sandboxes,
            seen: Mutex::new(Vec::new()),
            during: Mutex::new(None),
        })
    }
    fn calls(&self) -> Vec<HashSet<String>> {
        self.seen.lock().expect("seen").clone()
    }
}

impl GatewayPoster for Gateway {
    fn complete(&self, _: &GatewayCall) -> Result<String, String> {
        self.seen.lock().expect("seen").push(self.sandboxes.live());
        if let Some(during) = self.during.lock().expect("during").as_ref() {
            during();
        }
        Ok("the memo".to_string())
    }
}

struct Setup {
    store: Store,
    queue: RedisRunQueue,
    key: String,
    sandboxes: Arc<Sandboxes>,
    gateway: Arc<Gateway>,
    worker: Worker,
    spec: RunSpec,
}

fn timing(max_deliveries: u32) -> QueueTiming {
    QueueTiming {
        max_deliveries,
        ..QueueTiming::default()
    }
}

/// A queued Box background turn on `store`, on a queue of its own, and a
/// worker for it.
fn setup(store: Store, max_deliveries: u32) -> Setup {
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
    let sandboxes = Arc::new(Sandboxes::default());
    let gateway = Gateway::new(sandboxes.clone());
    let worker = Worker::builder()
        .queue(RedisRunQueue::with_key(REDIS_URL, &key))
        .store(store.clone())
        .memory(Arc::new(InMemory::default()))
        .jev("http://127.0.0.1:9")
        .poster(gateway.clone())
        .sandbox(sandboxes.clone())
        .timing(timing(max_deliveries))
        .build();
    Setup {
        store,
        queue,
        key,
        sandboxes,
        gateway,
        worker,
        spec,
    }
}

impl Setup {
    /// As a worker that died during attempt 1 left it: a start counted, the
    /// ladder and `RunStarted` stored, its sandbox up.
    fn died_in_attempt_one(&self) {
        let run = self.spec.run_id;
        assert_eq!(self.queue.start(run).expect("start"), 1);
        let ladder = [
            EventPayload::RunScheduled,
            EventPayload::RunProvisioning,
            EventPayload::RunStarting,
            EventPayload::RunStarted,
        ]
        .into_iter()
        .map(|payload| event(&self.spec, payload))
        .collect();
        self.store.append_events(run, ladder).expect("append");
        self.sandboxes.up(&attempt(&self.spec, 1));
    }

    /// This claim's lease runs out, and the reaper puts the run back.
    fn expire_lease(&self) {
        let mut redis = redis::Client::open(REDIS_URL)
            .expect("client")
            .get_connection()
            .expect("connect");
        redis::cmd("DEL")
            .arg(format!("{{{}}}:lease:{}", self.key, self.spec.run_id))
            .query::<()>(&mut redis)
            .expect("expire the lease");
        self.queue.reap().expect("reap");
        assert_eq!(self.queue.queued().expect("queued"), [self.spec.run_id]);
    }

    fn payloads(&self) -> Vec<&'static str> {
        self.store
            .run(self.spec.run_id)
            .expect("read")
            .expect("stored")
            .events
            .iter()
            .map(|event| event.payload.event_type())
            .collect()
    }

    fn ended(&self) -> bool {
        self.store
            .run(self.spec.run_id)
            .expect("read")
            .expect("stored")
            .events
            .iter()
            .any(|event| is_terminal(&event.payload))
    }
}

/// Gives `run`'s lease on queue `key` to another worker.
fn steal(key: &str, run: RunId) {
    let mut redis = redis::Client::open(REDIS_URL)
        .expect("client")
        .get_connection()
        .expect("connect");
    redis::cmd("SET")
        .arg(format!("{{{key}}}:lease:{run}"))
        .arg("another-worker")
        .query::<()>(&mut redis)
        .expect("steal the lease");
}

// A Box background turn runs in its attempt's sandbox: provisioned before
// the gateway call, up during it, removed before the completion is stored.
#[test]
fn a_box_turn_runs_in_its_attempts_sandbox() {
    for store in stores() {
        let s = setup(store, 5);
        assert_eq!(s.worker.work_one().expect("work"), Some(s.spec.run_id));
        let one = attempt(&s.spec, 1);
        assert_eq!(s.gateway.calls(), [HashSet::from([one.clone()])]);
        assert_eq!(
            s.sandboxes.ops(),
            [Op::Provision(one.clone()), Op::Destroy(one)]
        );
        assert!(s.sandboxes.live().is_empty());
        assert_eq!(s.payloads().last(), Some(&"run.completed"));
    }
}

// A redelivered turn is attempt 2, in a sandbox of its own; before it ends,
// attempt 1's sandbox, left by the worker that died, is removed too.
#[test]
fn a_redelivered_turn_removes_the_earlier_attempts_sandbox() {
    for store in stores() {
        let s = setup(store, 5);
        s.died_in_attempt_one();
        assert_eq!(s.worker.work_one().expect("work"), Some(s.spec.run_id));
        let (one, two) = (attempt(&s.spec, 1), attempt(&s.spec, 2));
        assert_eq!(s.gateway.calls(), [HashSet::from([one, two.clone()])]);
        assert_eq!(s.sandboxes.ops()[0], Op::Provision(two));
        assert!(s.sandboxes.live().is_empty(), "{:?}", s.sandboxes.live());
        assert_eq!(s.payloads().last(), Some(&"run.completed"));
    }
}

// A stop on a redelivered Box turn: its worker removes attempt 1's sandbox,
// then cancels it, and never calls the gateway (83A, 84A).
#[test]
fn a_stop_on_a_redelivered_turn_removes_its_sandbox_then_cancels() {
    for store in stores() {
        let s = setup(store, 5);
        s.died_in_attempt_one();
        let stops = s.store.stops().expect("stops");
        stops
            .put_stop(&s.spec.owner, &StopScope::Run(s.spec.run_id))
            .expect("stop");
        assert_eq!(s.worker.work_one().expect("work"), Some(s.spec.run_id));
        assert!(s.gateway.calls().is_empty());
        assert!(s.sandboxes.live().is_empty());
        assert_eq!(s.payloads().last(), Some(&"run.cancelled"));
    }
}

// Past max_deliveries a Box turn is not run again: its sandboxes are
// removed, then it is failed. One that cannot be removed leaves the turn
// open, its claim unacknowledged, so it comes back once its lease expires
// (not at once, which would spin), and the next delivery cleans up (84A).
#[test]
fn past_max_deliveries_a_box_turn_is_cleaned_up_then_failed() {
    for store in stores() {
        let s = setup(store, 1);
        s.died_in_attempt_one();
        s.sandboxes.fail_destroys(1);
        assert!(s.worker.work_one().is_err());
        assert!(!s.ended(), "{:?}", s.payloads());
        assert!(s.sandboxes.live().contains(&attempt(&s.spec, 1)));
        assert_eq!(s.queue.queued().expect("queued"), []);
        assert_eq!(s.queue.processing().expect("processing"), [s.spec.run_id]);
        s.expire_lease();
        assert_eq!(s.worker.work_one().expect("work"), Some(s.spec.run_id));
        assert!(s.gateway.calls().is_empty());
        assert!(s.sandboxes.live().is_empty());
        assert_eq!(s.payloads().last(), Some(&"run.failed"));
    }
}

// Past max_deliveries, a lease lost while the sandboxes are removed: the
// check just before the end finds it gone, so this delivery ends nothing
// (85A); the new holder fails the turn.
#[test]
fn a_lease_lost_during_the_last_cleanup_stores_no_end() {
    for store in stores() {
        let s = setup(store, 1);
        s.died_in_attempt_one();
        let (key, run) = (s.key.clone(), s.spec.run_id);
        s.sandboxes.on_destroy(move |_| steal(&key, run));
        assert_eq!(s.worker.work_one().expect("work"), Some(s.spec.run_id));
        assert!(s.sandboxes.live().is_empty());
        assert!(!s.ended(), "{:?}", s.payloads());
    }
}

// A removal that fails before the completion is stored leaves the turn
// open, its claim unacknowledged; once its lease expires, the next
// delivery ends it clean.
#[test]
fn a_failed_removal_keeps_the_turn_open() {
    for store in stores() {
        let s = setup(store, 5);
        s.sandboxes.fail_destroys(1);
        assert!(s.worker.work_one().is_err());
        assert!(!s.ended(), "{:?}", s.payloads());
        assert_eq!(s.queue.queued().expect("queued"), []);
        s.expire_lease();
        assert_eq!(s.worker.work_one().expect("work"), Some(s.spec.run_id));
        assert!(s.sandboxes.live().is_empty());
        assert_eq!(s.payloads().last(), Some(&"run.completed"));
    }
}

// A worker that finds its lease gone right after it provisioned removes its
// own sandbox, and nothing else (an earlier attempt's is the new holder's
// to clean up), calls no gateway, and stores no end (85A).
#[test]
fn a_worker_that_lost_its_lease_after_the_provision_removes_its_sandbox() {
    for store in stores() {
        let s = setup(store, 5);
        s.died_in_attempt_one();
        let (key, run) = (s.key.clone(), s.spec.run_id);
        s.sandboxes.on_provision(move |_| steal(&key, run));
        assert_eq!(s.worker.work_one().expect("work"), Some(s.spec.run_id));
        assert!(s.gateway.calls().is_empty());
        assert_eq!(s.sandboxes.live(), HashSet::from([attempt(&s.spec, 1)]));
        assert!(!s.ended(), "{:?}", s.payloads());
        assert_eq!(s.queue.processing().expect("processing"), [s.spec.run_id]);
    }
}

// A worker whose lease ran out during the gateway call removes its own
// sandbox, and nothing else, and stores no end: the new holder runs the
// turn (85A).
#[test]
fn a_worker_that_lost_its_lease_during_the_call_removes_its_sandbox() {
    for store in stores() {
        let s = setup(store, 5);
        s.died_in_attempt_one();
        let (key, run) = (s.key.clone(), s.spec.run_id);
        *s.gateway.during.lock().expect("during") = Some(Box::new(move || steal(&key, run)));
        assert_eq!(s.worker.work_one().expect("work"), Some(s.spec.run_id));
        assert_eq!(s.gateway.calls().len(), 1);
        assert_eq!(s.sandboxes.live(), HashSet::from([attempt(&s.spec, 1)]));
        assert!(!s.ended(), "{:?}", s.payloads());
    }
}

// A stop that comes while the sandbox is provisioned: the sandbox is
// removed and the turn cancelled before any gateway call.
#[test]
fn a_stop_during_the_provision_cancels_before_the_call() {
    for store in stores() {
        let s = setup(store, 5);
        let (stopped, owner, run) = (s.store.clone(), s.spec.owner.clone(), s.spec.run_id);
        s.sandboxes.on_provision(move |_| {
            stopped
                .stops()
                .expect("stops")
                .put_stop(&owner, &StopScope::Run(run))
                .expect("stop");
        });
        assert_eq!(s.worker.work_one().expect("work"), Some(s.spec.run_id));
        assert!(s.gateway.calls().is_empty());
        assert!(s.sandboxes.live().is_empty());
        assert_eq!(s.payloads().last(), Some(&"run.cancelled"));
    }
}

// The lease runs out while the worker cleans up, after the call: the
// check just before the append finds it gone, so this worker stores no
// end; the new holder ends the turn (85A).
#[test]
fn a_lease_lost_during_the_cleanup_stores_no_end() {
    for store in stores() {
        let s = setup(store, 5);
        let (key, run) = (s.key.clone(), s.spec.run_id);
        s.sandboxes.on_destroy(move |_| steal(&key, run));
        assert_eq!(s.worker.work_one().expect("work"), Some(s.spec.run_id));
        assert_eq!(s.gateway.calls().len(), 1);
        assert!(s.sandboxes.live().is_empty());
        assert!(!s.ended(), "{:?}", s.payloads());
    }
}

// A later attempt provisioned while this worker, still holding what it
// last saw as its lease, ends the turn: this worker removes attempts up to
// its own, so the later sandbox is left (EndClean's exception), and the
// sweep removes it once the turn has ended (86C).
#[test]
fn a_later_attempts_sandbox_is_left_to_the_sweep() {
    for store in stores() {
        let s = setup(store, 5);
        let (later, sandboxes) = (attempt(&s.spec, 2), s.sandboxes.clone());
        let provisioned = later.clone();
        *s.gateway.during.lock().expect("during") =
            Some(Box::new(move || sandboxes.up(&provisioned)));
        assert_eq!(s.worker.work_one().expect("work"), Some(s.spec.run_id));
        assert_eq!(s.payloads().last(), Some(&"run.completed"));
        assert_eq!(s.sandboxes.live(), HashSet::from([later.clone()]));
        let removed = sweep_sandboxes(s.store.as_ref(), s.sandboxes.as_ref()).expect("sweep");
        assert_eq!(removed, [later]);
        assert!(s.sandboxes.live().is_empty());
    }
}

// A worker that provisions after another ended the turn and took the
// lease (it read the turn open before) finds its lease gone, removes its
// own sandbox, and ends nothing: the turn keeps one end.
#[test]
fn a_provision_after_the_turn_ended_is_removed_by_its_worker() {
    for store in stores() {
        let s = setup(store, 5);
        let (key, run) = (s.key.clone(), s.spec.run_id);
        let ender = s.store.clone();
        let spec = s.spec.clone();
        s.sandboxes.on_provision(move |_| {
            ender
                .append_events(run, vec![event(&spec, EventPayload::RunCancelled)])
                .expect("end");
            steal(&key, run);
        });
        assert_eq!(s.worker.work_one().expect("work"), Some(s.spec.run_id));
        assert!(s.gateway.calls().is_empty());
        assert!(s.sandboxes.live().is_empty());
        let events = s.store.run(run).expect("read").expect("stored").events;
        let ends: Vec<_> = events
            .iter()
            .filter(|event| is_terminal(&event.payload))
            .collect();
        assert_eq!(ends.len(), 1, "{:?}", s.payloads());
        assert_eq!(s.payloads().last(), Some(&"run.cancelled"));
    }
}

// The reaper removes the sandboxes the host lists for ended turns, and
// leaves those of open turns (86C).
#[test]
fn the_sweep_removes_the_sandboxes_of_ended_turns_only() {
    for store in stores() {
        let open = setup(store.clone(), 5);
        let ended = setup(store, 5);
        ended
            .store
            .append_events(
                ended.spec.run_id,
                vec![event(&ended.spec, EventPayload::RunCancelled)],
            )
            .expect("cancel");
        let sandboxes = Sandboxes::default();
        let (left, running) = (attempt(&ended.spec, 1), attempt(&open.spec, 1));
        sandboxes.up(&left);
        sandboxes.up(&running);
        sandboxes.up("gol-box-not-a-run");
        let removed = sweep_sandboxes(open.store.as_ref(), &sandboxes).expect("sweep");
        assert_eq!(removed, [left]);
        assert!(sandboxes.live().contains(&running));
        assert!(sandboxes.live().contains("gol-box-not-a-run"));
    }
}
