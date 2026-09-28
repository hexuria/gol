//! Queue workers and the reaper (C4): threads in the server process, started
//! when `GOL_REDIS_URL` is set (owner decision 1A).
//!
//! A claim moves through types: `Claim::prepare` gives `Prepared::Open` (a
//! run to execute) or `Prepared::Done` (nothing to run); `Open::execute`
//! gives `Executed`, and `Executed::record` gives `Done`. Only `Done` can
//! acknowledge, so a run leaves the queue only after its record or a finding
//! that it already ended. Delivery is at least once: a redelivered run starts
//! over, and repeats its Jev calls.
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Arc;
use std::time::Duration;

use harness::{DelegateTarget, Memory};
use protocol::{Capability, Event, EventPayload, FailureClass, RunId, RunSpec};

use crate::http::{harness_events, Delegation};
use crate::inference::{dispatch_events, run_failed_event};
use crate::queue::{QueueTiming, RedisRunQueue};
use crate::spawner::OwnedSpawner;
use crate::store::{is_terminal, Append, RunStore, StoredRun};

/// Takes runs off the queue and runs them to their end.
pub struct Worker {
    /// Shared with the spawner that queues a run's children.
    queue: Arc<RedisRunQueue>,
    store: Arc<dyn RunStore>,
    memory: Arc<dyn Memory>,
    jev_base_url: String,
    timing: QueueTiming,
}

/// Builder state: a required input not given yet.
pub struct Missing;
/// Builder state: a required input given.
pub struct Given;

/// A `Worker` whose queue, store, memory and Jev address are each required
/// before `build` exists; the timing is optional.
pub struct WorkerBuilder<Q, S, M, J> {
    queue: Option<RedisRunQueue>,
    store: Option<Arc<dyn RunStore>>,
    memory: Option<Arc<dyn Memory>>,
    jev_base_url: Option<String>,
    timing: QueueTiming,
    states: PhantomData<(Q, S, M, J)>,
}

impl Worker {
    pub fn builder() -> WorkerBuilder<Missing, Missing, Missing, Missing> {
        WorkerBuilder {
            queue: None,
            store: None,
            memory: None,
            jev_base_url: None,
            timing: QueueTiming::default(),
            states: PhantomData,
        }
    }
}

impl<Q, S, M, J> WorkerBuilder<Q, S, M, J> {
    fn to<Q2, S2, M2, J2>(self) -> WorkerBuilder<Q2, S2, M2, J2> {
        WorkerBuilder {
            queue: self.queue,
            store: self.store,
            memory: self.memory,
            jev_base_url: self.jev_base_url,
            timing: self.timing,
            states: PhantomData,
        }
    }

    pub fn timing(mut self, timing: QueueTiming) -> Self {
        self.timing = timing;
        self
    }
}

impl<S, M, J> WorkerBuilder<Missing, S, M, J> {
    pub fn queue(mut self, queue: RedisRunQueue) -> WorkerBuilder<Given, S, M, J> {
        self.queue = Some(queue);
        self.to()
    }
}

impl<Q, M, J> WorkerBuilder<Q, Missing, M, J> {
    pub fn store(mut self, store: Arc<dyn RunStore>) -> WorkerBuilder<Q, Given, M, J> {
        self.store = Some(store);
        self.to()
    }
}

impl<Q, S, J> WorkerBuilder<Q, S, Missing, J> {
    pub fn memory(mut self, memory: Arc<dyn Memory>) -> WorkerBuilder<Q, S, Given, J> {
        self.memory = Some(memory);
        self.to()
    }
}

impl<Q, S, M> WorkerBuilder<Q, S, M, Missing> {
    pub fn jev(mut self, base_url: impl Into<String>) -> WorkerBuilder<Q, S, M, Given> {
        self.jev_base_url = Some(base_url.into());
        self.to()
    }
}

impl WorkerBuilder<Given, Given, Given, Given> {
    pub fn build(self) -> Worker {
        let (Some(queue), Some(store), Some(memory), Some(jev_base_url)) =
            (self.queue, self.store, self.memory, self.jev_base_url)
        else {
            unreachable!("every required input is given in this state")
        };
        Worker {
            queue: Arc::new(queue),
            store,
            memory,
            jev_base_url,
            timing: self.timing,
        }
    }
}

/// A run this worker claimed, with the token its lease holds.
#[must_use = "a dropped claim waits out its lease before the run is redelivered"]
pub struct Claim<'a> {
    worker: &'a Worker,
    run_id: RunId,
    token: String,
}

/// What `Claim::prepare` found.
#[must_use = "a dropped claim waits out its lease before the run is redelivered"]
pub enum Prepared<'a> {
    /// An open run to execute.
    Open(Open<'a>),
    /// Nothing to run: the run already ended, was just failed for too many
    /// deliveries, or is not stored.
    Done(Done<'a>),
}

/// A claimed run that is open, with its stored log.
#[must_use = "a dropped claim waits out its lease before the run is redelivered"]
pub struct Open<'a> {
    claim: Claim<'a>,
    stored: Box<StoredRun>,
}

/// A claimed run executed, with the events to record.
#[must_use = "a dropped claim waits out its lease before the run is redelivered"]
pub struct Executed<'a> {
    claim: Claim<'a>,
    events: Vec<Event>,
}

/// A claimed run whose log is terminal, or that is not stored: the only
/// state that can acknowledge.
#[must_use = "a dropped claim waits out its lease before the run is redelivered"]
pub struct Done<'a> {
    claim: Claim<'a>,
    recorded: Option<Append>,
}

impl Worker {
    /// What `spec`'s run may delegate with: an owned spawner on this worker's
    /// queue, and its owner's agents when it holds `agent.delegate`. A store
    /// that cannot list them offers none.
    fn delegation(&self, spec: &RunSpec) -> Delegation {
        let spawner = Arc::new(OwnedSpawner::new(
            self.store.clone(),
            Some(self.queue.clone()),
        ));
        if !spec
            .capabilities
            .contains(&Capability::new("agent.delegate"))
        {
            return (spawner, Vec::new());
        }
        let targets = match self.store.agents_of(&spec.owner) {
            Ok(agents) => agents
                .into_iter()
                .map(|agent| DelegateTarget {
                    agent_id: agent.manifest.id,
                    name: agent.manifest.name,
                    description: agent.manifest.description,
                })
                .collect(),
            Err(error) => {
                eprintln!("gol: queue worker: agents for run {}: {error}", spec.run_id);
                Vec::new()
            }
        };
        (spawner, targets)
    }

    /// Claims the oldest queued run, with its lease, or nothing when the
    /// queue is empty.
    pub fn claim(&self) -> Result<Option<Claim<'_>>, String> {
        let token = uuid::Uuid::new_v4().to_string();
        Ok(self
            .queue
            .claim(&token, self.timing.lease)?
            .map(|run_id| Claim {
                worker: self,
                run_id,
                token,
            }))
    }

    /// Claims one run and takes it to its end: runs it unless its log has
    /// already ended (owner decision 3A), records what the harness did, and
    /// only then acknowledges it. `None` when the queue was empty. An error
    /// before the acknowledgement leaves the lease to expire, and the reaper
    /// hands the run back.
    pub fn work_one(&self) -> Result<Option<RunId>, String> {
        let Some(claim) = self.claim()? else {
            return Ok(None);
        };
        let done = match claim.prepare()? {
            Prepared::Open(open) => open.execute().record()?,
            Prepared::Done(done) => done,
        };
        done.ack().map(Some)
    }

    /// Works the queue for as long as the process runs. An error or a panic
    /// in one run leaves that run's lease to expire; the worker waits, twice
    /// as long after each failure in a row, and goes on.
    pub fn work_forever(&self) {
        let mut failures = 0u32;
        loop {
            match catch_unwind(AssertUnwindSafe(|| self.work_one())) {
                Ok(Ok(Some(_))) => failures = 0,
                Ok(Ok(None)) => {
                    failures = 0;
                    std::thread::sleep(self.timing.idle_wait);
                }
                Ok(Err(error)) => {
                    eprintln!("gol: queue worker: {error}");
                    failures += 1;
                    std::thread::sleep(backoff(self.timing, failures));
                }
                Err(_) => {
                    eprintln!("gol: queue worker: a run panicked; its lease will expire");
                    failures += 1;
                    std::thread::sleep(backoff(self.timing, failures));
                }
            }
        }
    }
}

/// `idle_wait` doubled for each failure in a row, at most `max_backoff`.
fn backoff(timing: QueueTiming, failures: u32) -> Duration {
    let factor = 2u32.saturating_pow(failures.saturating_sub(1).min(16));
    timing
        .idle_wait
        .saturating_mul(factor)
        .min(timing.max_backoff)
}

impl<'a> Claim<'a> {
    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Loads the run. A run whose log ended, or that is not stored, needs
    /// nothing more. An open run counts a start; one started more than
    /// `max_deliveries` times is failed instead of run again. A run that
    /// cannot be loaded or started is handed straight back to the back of
    /// the queue, and does not count.
    pub fn prepare(self) -> Result<Prepared<'a>, String> {
        let worker = self.worker;
        let run_id = self.run_id();
        let loaded = match worker.store.run(run_id) {
            Ok(loaded) => loaded,
            Err(error) => {
                if let Err(release) = worker.queue.release(run_id, &self.token) {
                    eprintln!("gol: queue worker: release run {run_id}: {release}");
                }
                return Err(format!("load run {run_id}: {error}"));
            }
        };
        let Some(stored) = loaded else {
            eprintln!("gol: queue worker: run {run_id} is not stored; dropping it from the queue");
            return Ok(Prepared::Done(Done {
                claim: self,
                recorded: None,
            }));
        };
        if stored
            .events
            .iter()
            .any(|event| is_terminal(&event.payload))
        {
            return Ok(Prepared::Done(Done {
                claim: self,
                recorded: None,
            }));
        }
        let starts = match worker.queue.start(run_id) {
            Ok(starts) => starts,
            Err(error) => {
                if let Err(release) = worker.queue.release(run_id, &self.token) {
                    eprintln!("gol: queue worker: release run {run_id}: {release}");
                }
                return Err(format!("start run {run_id}: {error}"));
            }
        };
        if starts > worker.timing.max_deliveries {
            let before = starts - 1;
            let failed = run_failed_event(
                &stored.spec,
                FailureClass::Infrastructure,
                format!(
                    "started {before} time{} without ending",
                    if before == 1 { "" } else { "s" }
                ),
            );
            return Executed {
                claim: self,
                events: vec![failed],
            }
            .record()
            .map(Prepared::Done);
        }
        Ok(Prepared::Open(Open {
            claim: self,
            stored: Box::new(stored),
        }))
    }
}

impl<'a> Open<'a> {
    /// Runs the harness with Jev, renewing the lease every heartbeat, and
    /// returns what to record: scheduled, provisioning and starting, then the
    /// harness's events, which end the run (with `RunFailed` if the harness
    /// could not finish).
    pub fn execute(self) -> Executed<'a> {
        let worker = self.claim.worker;
        let claim = &self.claim;
        let spec = &self.stored.spec;
        let events = std::thread::scope(|scope| {
            let (stop, stopped) = mpsc::channel::<()>();
            std::thread::Builder::new()
                .name("gol-heartbeat".to_string())
                .spawn_scoped(scope, move || claim.heartbeat(&stopped))
                .expect("spawn the heartbeat thread");
            let mut events = dispatch_events(spec);
            let (harness, _outcome) = harness_events(
                &worker.jev_base_url,
                spec,
                worker.memory.as_ref(),
                Some(worker.delegation(spec)),
            );
            events.extend(harness);
            drop(stop);
            events
        });
        Executed {
            claim: self.claim,
            events,
        }
    }
}

impl<'a> Executed<'a> {
    /// Appends the events. The store refuses them once the log is terminal,
    /// so two workers on one run leave one terminal event. An error keeps the
    /// claim from acknowledging.
    pub fn record(self) -> Result<Done<'a>, String> {
        let recorded = self
            .claim
            .worker
            .store
            .append_events(self.claim.run_id(), self.events)
            .map_err(|error| error.to_string())?;
        Ok(Done {
            claim: self.claim,
            recorded: Some(recorded),
        })
    }
}

impl Done<'_> {
    /// What the record did, if there was one.
    pub fn recorded(&self) -> Option<Append> {
        self.recorded
    }

    /// Acknowledges the run: off the processing list, and its lease released
    /// if this claim still holds it.
    pub fn ack(self) -> Result<RunId, String> {
        let claim = self.claim;
        claim.worker.queue.ack(claim.run_id(), &claim.token)?;
        Ok(claim.run_id())
    }
}

impl Claim<'_> {
    /// Renews the lease every heartbeat until `stopped` closes, or until the
    /// lease is another worker's: the store keeps one terminal event
    /// whichever records first.
    fn heartbeat(&self, stopped: &mpsc::Receiver<()>) {
        let timing = self.worker.timing;
        while let Err(RecvTimeoutError::Timeout) = stopped.recv_timeout(timing.heartbeat) {
            match self
                .worker
                .queue
                .renew(self.run_id(), &self.token, timing.lease)
            {
                Ok(true) => {}
                Ok(false) => {
                    eprintln!("gol: queue worker: lost the lease on run {}", self.run_id());
                    return;
                }
                Err(error) => {
                    eprintln!("gol: queue worker: renew run {}: {error}", self.run_id())
                }
            }
        }
    }
}

/// Hands back runs whose lease ran out, and sweeps runs left pending, every
/// `reap_every`, for as long as the process runs.
pub fn reap_forever(queue: &RedisRunQueue, store: &dyn RunStore, timing: QueueTiming) {
    loop {
        match catch_unwind(AssertUnwindSafe(|| queue.reap())) {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => eprintln!("gol: queue reaper: {error}"),
            Err(_) => eprintln!("gol: queue reaper: reaping panicked"),
        }
        match catch_unwind(AssertUnwindSafe(|| {
            sweep(queue, store, timing.sweep_after, timing.forget_after)
        })) {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => eprintln!("gol: queue sweep: {error}"),
            Err(_) => eprintln!("gol: queue sweep: sweeping panicked"),
        }
        std::thread::sleep(timing.reap_every);
    }
}

/// Pushes each run pending for at least `after` that its producer stored but
/// never pushed: the producer died in between (C6). A pending run whose log
/// has moved past created and queued is taken off pending. A pending run not
/// in the store is kept (its put may still be in flight) until it has been
/// pending for `forget_after`, then dropped. Returns the runs it pushed. A run
/// the store cannot load, or that Redis cannot push or unpend now, stays
/// pending for the next sweep; the others are still swept.
pub fn sweep(
    queue: &RedisRunQueue,
    store: &dyn RunStore,
    after: Duration,
    forget_after: Duration,
) -> Result<Vec<RunId>, String> {
    let forgotten = queue.pending_for(forget_after)?;
    let mut pushed = Vec::new();
    for run_id in queue.pending_for(after)? {
        match store.run(run_id) {
            Ok(Some(run)) if waiting(&run) => match queue.push(run_id) {
                Ok(()) => pushed.push(run_id),
                Err(error) => eprintln!("gol: queue sweep: push run {run_id}: {error}"),
            },
            Ok(None) if !forgotten.contains(&run_id) => {}
            Ok(_) => {
                if let Err(error) = queue.unpend(run_id) {
                    eprintln!("gol: queue sweep: unpend run {run_id}: {error}");
                }
            }
            Err(error) => eprintln!("gol: queue sweep: load run {run_id}: {error}"),
        }
    }
    Ok(pushed)
}

/// Whether `run`'s log is still as its producer stored it: created, queued
/// and its message.
fn waiting(run: &StoredRun) -> bool {
    run.events.iter().all(|event| {
        matches!(
            event.payload,
            EventPayload::RunCreated | EventPayload::RunQueued | EventPayload::UserMessage { .. }
        )
    })
}

/// The queue the server runs, from its environment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueSettings {
    pub redis_url: String,
    pub workers: usize,
}

/// `GOL_REDIS_URL` (unset or empty: no queue) with `GOL_WORKERS` worker
/// threads, 2 by default (owner decision 1A for C4). The queue needs
/// `GOL_DATABASE_URL`: its run ids outlive the process, and so must the runs.
pub fn queue_from_env(env: &BTreeMap<String, String>) -> Result<Option<QueueSettings>, String> {
    let Some(redis_url) = env.get("GOL_REDIS_URL").filter(|url| !url.is_empty()) else {
        return Ok(None);
    };
    if env.get("GOL_DATABASE_URL").is_none_or(|url| url.is_empty()) {
        return Err(
            "GOL_REDIS_URL needs GOL_DATABASE_URL: queued run ids outlive the process, and a run \
             kept in memory would not"
                .to_string(),
        );
    }
    let workers = match env.get("GOL_WORKERS").filter(|count| !count.is_empty()) {
        None => 2,
        Some(count) => count
            .parse::<usize>()
            .ok()
            .filter(|count| (1..=256).contains(count))
            .ok_or_else(|| format!("GOL_WORKERS must be a count from 1 to 256, not {count:?}"))?,
    };
    Ok(Some(QueueSettings {
        redis_url: redis_url.clone(),
        workers,
    }))
}

/// Starts `settings.workers` worker threads and one reaper, which also
/// sweeps, all running for as long as the process does.
pub fn start_queue(
    settings: &QueueSettings,
    store: Arc<dyn RunStore>,
    memory: Arc<dyn Memory>,
    jev_base_url: &str,
) -> Result<(), String> {
    let timing = QueueTiming::default();
    for index in 0..settings.workers {
        let worker = Worker::builder()
            .queue(RedisRunQueue::open(&settings.redis_url))
            .store(store.clone())
            .memory(memory.clone())
            .jev(jev_base_url)
            .timing(timing)
            .build();
        std::thread::Builder::new()
            .name(format!("gol-worker-{index}"))
            .spawn(move || worker.work_forever())
            .map_err(|error| error.to_string())?;
    }
    let reaper = RedisRunQueue::open(&settings.redis_url);
    std::thread::Builder::new()
        .name("gol-reaper".to_string())
        .spawn(move || reap_forever(&reaper, store.as_ref(), timing))
        .map_err(|error| error.to_string())?;
    Ok(())
}
