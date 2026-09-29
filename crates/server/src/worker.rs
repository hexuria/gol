//! Queue workers and the reaper (C4): threads in the server process, started
//! when `GOL_REDIS_URL` is set (owner decision 1A).
//!
//! A claim moves through types: `Claim::prepare` gives `Prepared::Open` (a
//! run to execute) or `Prepared::Done` (nothing to run); `Open::execute`
//! gives `Executed`, and `Executed::record` gives `Done`. Only `Done` can
//! acknowledge, so a run leaves the queue only after its record or a finding
//! that it already ended. Delivery is at least once.
//!
//! A worker stores a run as it goes (Phase 1.5b): each step's events are
//! appended at the step boundary with `append_events_after`, which the store
//! refuses once another writer moved the log. A redelivered run resumes from
//! its stored log, so a stored step is not decided again; a step that was not
//! stored when its worker died is.
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Arc;
use std::time::Duration;

use harness::{
    run_until, Boundary, DelegateTarget, Driver, EchoTool, JevDecider, Memory, RunMemory,
};
use protocol::{
    fold, Capability, DispatchPhase, Event, EventPayload, FailureClass, HarnessState, MessageId,
    RunId, RunSpec, Timestamp,
};

use crate::deliverer::{answered, deliver, task_answer, OwnedDeliverer};
use crate::http::{jev_client, Delegation};
use crate::inference::{dispatch_events, run_failed_event};
use crate::models::ModelsConfig;
use crate::queue::{QueueTiming, RedisRunQueue};
use crate::spawner::OwnedSpawner;
use crate::store::{is_terminal, Append, MessageStore, RunStore, StoredMessage, StoredRun};

/// Takes runs off the queue and runs them to their end.
pub struct Worker {
    /// Shared with the spawner that queues a run's children.
    queue: Arc<RedisRunQueue>,
    store: Arc<dyn RunStore>,
    memory: Arc<dyn Memory>,
    jev_base_url: String,
    timing: QueueTiming,
    /// The work model each run calls (D2).
    models: Arc<ModelsConfig>,
    /// Where messages between agents are kept. With it the worker delivers
    /// a run's messages and parks a run that waits on an ask (Phase 2.3);
    /// without it every message is refused.
    messages: Option<Arc<dyn MessageStore>>,
}

/// Builder state: a required input not given yet.
pub struct Missing;
/// Builder state: a required input given.
pub struct Given;

/// A `Worker` whose queue, store, memory and Jev address are each required
/// before `build` exists; the timing and the models are optional.
pub struct WorkerBuilder<Q, S, M, J> {
    queue: Option<RedisRunQueue>,
    store: Option<Arc<dyn RunStore>>,
    memory: Option<Arc<dyn Memory>>,
    jev_base_url: Option<String>,
    timing: QueueTiming,
    models: Arc<ModelsConfig>,
    messages: Option<Arc<dyn MessageStore>>,
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
            models: Arc::new(ModelsConfig::default()),
            messages: None,
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
            models: self.models,
            messages: self.messages,
            states: PhantomData,
        }
    }

    pub fn timing(mut self, timing: QueueTiming) -> Self {
        self.timing = timing;
        self
    }

    /// The platform's keys for the runs' work models. Without them every
    /// model call fails.
    pub fn models(mut self, models: Arc<ModelsConfig>) -> Self {
        self.models = models;
        self
    }

    /// Where messages between agents are kept (Phase 2.3).
    pub fn messages(mut self, messages: Arc<dyn MessageStore>) -> Self {
        self.messages = Some(messages);
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
            models: self.models,
            messages: self.messages,
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
    /// Events still to append in one go (a run started too often).
    events: Vec<Event>,
    /// What storing the run step by step came to, when it was.
    stored: Option<Result<Outcome, String>>,
}

/// What a run executed step by step came to.
enum Outcome {
    /// What the store answered to its last append.
    Stored(Append),
    /// Its harness waits on the ask; it is to be parked (Phase 2.3).
    Waiting(MessageId),
}

/// A claimed run whose log is terminal, or that is not stored: the only
/// state that can acknowledge.
#[must_use = "a dropped claim waits out its lease before the run is redelivered"]
pub struct Done<'a> {
    claim: Claim<'a>,
    recorded: Option<Append>,
    /// The ask the run waits on: it is parked, not acknowledged.
    waiting: Option<MessageId>,
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
        // The owner's agents are the targets of a delegation and of a
        // message (decision 32A).
        let reaches = |capability| spec.capabilities.contains(&Capability::new(capability));
        if !reaches("agent.delegate") && !reaches("agent.message") {
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
                waiting: None,
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
                waiting: None,
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
                stored: None,
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
    /// stores the run as it goes: the scheduling ladder for a queued run,
    /// then each step's events at its boundary, then the tail that ends the
    /// run (with `RunFailed` if the decider failed).
    pub fn execute(self) -> Executed<'a> {
        let worker = self.claim.worker;
        let claim = &self.claim;
        let stored = *self.stored;
        let spec = stored.spec.clone();
        let outcome = std::thread::scope(|scope| {
            let (stop, stopped) = mpsc::channel::<()>();
            std::thread::Builder::new()
                .name("gol-heartbeat".to_string())
                .spawn_scoped(scope, move || claim.heartbeat(&stopped))
                .expect("spawn the heartbeat thread");
            let outcome = worker.store_as_it_goes(stored, &claim.token);
            drop(stop);
            outcome
        });
        // This worker ended a task that was asked: its end is the reply
        // (decision 31A). A failed delivery is left to the sweep.
        if let Ok(Outcome::Stored(Append::Appended)) = &outcome {
            worker.answer_ask(&spec);
        }
        Executed {
            claim: self.claim,
            events: Vec::new(),
            stored: Some(outcome),
        }
    }
}

/// How many times a worker reloads a run whose log another writer moved
/// before it hands the run back to the queue.
const RELOADS: usize = 3;

/// Why `Worker::run_from` stopped.
enum Stopped {
    /// What the store answered to its last append.
    Store(Append),
    /// Its claim no longer holds the run's lease.
    LostLease,
    /// Its harness waits on the reply to this ask.
    Waiting(MessageId),
}

impl Worker {
    /// Runs `stored` from its log, and again from a reloaded log each time
    /// another writer moved it, up to `RELOADS` times. `Appended` when this
    /// worker ended the run, `Terminal` when another writer did, `Moved` when
    /// it gave up or lost its lease (`Done::ack` then releases the run, which
    /// does nothing once another worker holds the lease).
    fn store_as_it_goes(&self, stored: StoredRun, token: &str) -> Result<Outcome, String> {
        let spec = stored.spec;
        let mut events = stored.events;
        let mut reloads = 0;
        loop {
            match self.run_from(&spec, events, token)? {
                Stopped::Store(Append::Moved) if reloads < RELOADS => reloads += 1,
                Stopped::Store(other) => return Ok(Outcome::Stored(other)),
                Stopped::LostLease => return Ok(Outcome::Stored(Append::Moved)),
                Stopped::Waiting(ask) => return Ok(Outcome::Waiting(ask)),
            }
            events = match self.store.run(spec.run_id).map_err(|e| e.to_string())? {
                Some(run) => run.events,
                None => return Ok(Outcome::Stored(Append::Missing)),
            };
        }
    }

    /// Delivers the end of `spec`'s run, a task that was asked and has
    /// ended, to the run that asked it (decision 31A).
    fn answer_ask(&self, spec: &RunSpec) {
        let Some(messages) = &self.messages else {
            return;
        };
        let answered = || -> Result<(), String> {
            let Some(ask) = messages
                .ask_of_task(spec.run_id)
                .map_err(|error| error.to_string())?
            else {
                return Ok(());
            };
            let Some(task) = self
                .store
                .run(spec.run_id)
                .map_err(|error| error.to_string())?
            else {
                return Ok(());
            };
            match task_answer(spec.run_id, spec.agent_id, &ask, &task.events) {
                Some(answer) => deliver(
                    self.store.as_ref(),
                    messages.as_ref(),
                    &self.queue,
                    &ask,
                    answer,
                ),
                None => Ok(()),
            }
        };
        if let Err(error) = answered() {
            eprintln!("gol: queue worker: answer of task {}: {error}", spec.run_id);
        }
    }

    /// Runs `spec`'s run from `events`, its stored log, appending as it goes.
    /// Stops at the first append the store refuses, and reports it, or at
    /// the first step boundary where `token` no longer holds the run's lease:
    /// the lease ran out under this worker and another may be running it.
    fn run_from(
        &self,
        spec: &RunSpec,
        mut events: Vec<Event>,
        token: &str,
    ) -> Result<Stopped, String> {
        // Another writer ended it: a reload finds the log as it was left.
        if events.iter().any(|event| is_terminal(&event.payload)) {
            return Ok(Stopped::Store(Append::Terminal));
        }
        let run_id = spec.run_id;
        let mut seen = events.len();
        // Every write first checks that this claim still holds the lease: a
        // lease can run out during a long step (a Jev or model call), and the
        // write after it may be the one that ends the run. A Redis error
        // keeps going, as the heartbeat does.
        let holds = || {
            !matches!(
                self.queue.renew(run_id, token, self.timing.lease),
                Ok(false)
            )
        };
        let append = |seen: usize, new: Vec<Event>| {
            if !holds() {
                return Ok(Stopped::LostLease);
            }
            self.store
                .append_events_after(run_id, seen, new)
                .map(Stopped::Store)
                .map_err(|error| error.to_string())
        };
        // A queued run is scheduled first, as one append.
        if fold(spec, &events).dispatch == DispatchPhase::Queued {
            let ladder = dispatch_events(spec);
            match append(seen, ladder.clone())? {
                Stopped::Store(Append::Appended) => {}
                refused => return Ok(refused),
            }
            seen += ladder.len();
            events.extend(ladder);
        }
        let mut driver = match Driver::resume(spec.clone(), events) {
            Ok(driver) => driver,
            Err(error) => {
                let failed = run_failed_event(
                    spec,
                    FailureClass::Infrastructure,
                    format!("cannot resume: {error:?}"),
                );
                return append(seen, vec![failed]);
            }
        };
        // run_until first finishes the step the log was cut in, which can
        // perform an effect (a tool call, a delegation) before any boundary.
        if !holds() {
            return Ok(Stopped::LostLease);
        }
        let (spawner, targets) = self.delegation(spec);
        driver = driver.with_spawner(spawner, targets);
        if let Some(messages) = &self.messages {
            driver = driver.with_deliverer(Arc::new(OwnedDeliverer::new(
                self.store.clone(),
                messages.clone(),
                self.queue.clone(),
            )));
        }
        let mut decider = match jev_client(&self.jev_base_url) {
            Ok(client) => JevDecider::new(client),
            Err(message) => {
                let mut tail = driver.events()[seen..].to_vec();
                tail.push(run_failed_event(
                    spec,
                    FailureClass::Dependency,
                    format!("decider: {message}"),
                ));
                return append(seen, tail);
            }
        };
        let echo = EchoTool;
        let models = self.models.model_for(spec);
        let memory = RunMemory::new(self.memory.as_ref());
        let mut refused = None;
        let outcome = run_until(
            &mut driver,
            &mut decider,
            &[&echo],
            &models,
            &memory,
            &mut |driver| {
                let new = &driver.events()[seen..];
                if new.is_empty() {
                    return Boundary::Continue;
                }
                match append(seen, new.to_vec()) {
                    Ok(Stopped::Store(Append::Appended)) => {
                        seen = driver.events().len();
                        Boundary::Continue
                    }
                    other => {
                        refused = Some(other);
                        Boundary::Pause
                    }
                }
            },
        );
        if let Some(refused) = refused {
            return refused;
        }
        let mut tail = driver.events()[seen..].to_vec();
        if let Err(error) = outcome {
            tail.push(run_failed_event(
                spec,
                FailureClass::Dependency,
                format!("decider: {}", error.message),
            ));
        }
        let stopped = if tail.is_empty() {
            Stopped::Store(Append::Appended)
        } else {
            append(seen, tail)?
        };
        // Stored up to the ask it waits on: the run is parked (decision 34A).
        if let (
            Stopped::Store(Append::Appended),
            HarnessState::WaitingForMessage { message_id, .. },
        ) = (&stopped, &driver.state().harness)
        {
            return Ok(Stopped::Waiting(*message_id));
        }
        Ok(stopped)
    }
}

impl<'a> Executed<'a> {
    /// Appends the events. The store refuses them once the log is terminal,
    /// so two workers on one run leave one terminal event. An error keeps the
    /// claim from acknowledging.
    pub fn record(self) -> Result<Done<'a>, String> {
        let (recorded, waiting) = match self.stored {
            Some(stored) => match stored? {
                Outcome::Stored(append) => (Some(append), None),
                Outcome::Waiting(ask) => (None, Some(ask)),
            },
            None => (
                Some(
                    self.claim
                        .worker
                        .store
                        .append_events(self.claim.run_id(), self.events)
                        .map_err(|error| error.to_string())?,
                ),
                None,
            ),
        };
        Ok(Done {
            claim: self.claim,
            recorded,
            waiting,
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
        let run_id = claim.run_id();
        // Waiting on an ask: parked while this claim holds the lease, then
        // woken at once if the answer came before the park (decision 34A,
        // `formal/runqueue` `Park` then `Recheck`).
        if let Some(ask) = self.waiting {
            let queue = &claim.worker.queue;
            if queue.park(run_id, &claim.token, ask)? {
                let events = claim
                    .worker
                    .store
                    .run(run_id)
                    .map_err(|error| error.to_string())?
                    .map(|run| run.events)
                    .unwrap_or_default();
                if answered(&events, ask) {
                    queue.wake(run_id)?;
                }
            }
            return Ok(run_id);
        }
        // Still open: other writers kept moving the log. The run goes back
        // on the queue, and its next delivery resumes from the stored log.
        if self.recorded == Some(Append::Moved) {
            claim.worker.queue.release(claim.run_id(), &claim.token)?;
            return Ok(claim.run_id());
        }
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

/// Hands back runs whose lease ran out, and sweeps runs left pending and
/// the asks of parked runs, every `reap_every`, for as long as the process
/// runs.
pub fn reap_forever(
    queue: &RedisRunQueue,
    store: &dyn RunStore,
    messages: Option<&dyn MessageStore>,
    timing: QueueTiming,
) {
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
        if let Some(messages) = messages {
            match catch_unwind(AssertUnwindSafe(|| {
                sweep_asks(queue, store, messages, Timestamp::now())
            })) {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => eprintln!("gol: ask sweep: {error}"),
                Err(_) => eprintln!("gol: ask sweep: sweeping panicked"),
            }
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

/// Finishes what a crash can leave undone around an ask (decision 34A):
/// wakes a parked run whose log already holds its answer, or that has
/// ended; delivers the end of an asked task nobody delivered; and answers
/// an open ask past its deadline, with its task's end if the task has
/// ended, else `AskTimedOut` (decision 28A). Returns the runs it woke or
/// answered. A run it cannot load or deliver to now is left for the next
/// sweep; the others are still swept.
pub fn sweep_asks(
    queue: &RedisRunQueue,
    store: &dyn RunStore,
    messages: &dyn MessageStore,
    now: Timestamp,
) -> Result<Vec<RunId>, String> {
    let mut swept = Vec::new();
    for (run_id, ask) in queue.parked()? {
        // A parked run the store lost is woken too: a worker acknowledges it.
        let settled = match store.run(run_id) {
            Ok(Some(run)) => {
                answered(&run.events, ask)
                    || run.events.iter().any(|event| is_terminal(&event.payload))
            }
            Ok(None) => true,
            Err(error) => {
                eprintln!("gol: ask sweep: load run {run_id}: {error}");
                continue;
            }
        };
        let result = if settled {
            queue.wake(run_id).map(|_| ())
        } else {
            match messages.message(ask) {
                Ok(Some(ask)) => match task_end(store, &ask) {
                    Some(answer) => deliver(store, messages, queue, &ask, answer),
                    None => continue,
                },
                Ok(None) => continue,
                Err(error) => Err(error.to_string()),
            }
        };
        match result {
            Ok(()) => swept.push(run_id),
            Err(error) => eprintln!("gol: ask sweep: run {run_id}: {error}"),
        }
    }
    for ask in messages
        .open_asks_due(now)
        .map_err(|error| error.to_string())?
    {
        let answer =
            task_end(store, &ask).unwrap_or(EventPayload::AskTimedOut { message_id: ask.id });
        match deliver(store, messages, queue, &ask, answer) {
            Ok(()) => swept.push(ask.from_run),
            Err(error) => eprintln!("gol: ask sweep: ask {}: {error}", ask.id),
        }
    }
    Ok(swept)
}

/// The answer `ask`'s task gives now that it has ended, if it has.
fn task_end(store: &dyn RunStore, ask: &StoredMessage) -> Option<EventPayload> {
    let task = store.run(ask.task_run?).ok()??;
    task_answer(task.spec.run_id, task.spec.agent_id, ask, &task.events)
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
/// sweeps, all running for as long as the process does. Queued runs may
/// message each other through `messages` (Phase 2.3).
pub fn start_queue(
    settings: &QueueSettings,
    store: Arc<dyn RunStore>,
    memory: Arc<dyn Memory>,
    messages: Arc<dyn MessageStore>,
    jev_base_url: &str,
    models: Arc<ModelsConfig>,
) -> Result<(), String> {
    let timing = QueueTiming::default();
    for index in 0..settings.workers {
        let worker = Worker::builder()
            .queue(RedisRunQueue::open(&settings.redis_url))
            .store(store.clone())
            .memory(memory.clone())
            .jev(jev_base_url)
            .timing(timing)
            .models(models.clone())
            .messages(messages.clone())
            .build();
        std::thread::Builder::new()
            .name(format!("gol-worker-{index}"))
            .spawn(move || worker.work_forever())
            .map_err(|error| error.to_string())?;
    }
    let reaper = RedisRunQueue::open(&settings.redis_url);
    std::thread::Builder::new()
        .name("gol-reaper".to_string())
        .spawn(move || reap_forever(&reaper, store.as_ref(), Some(messages.as_ref()), timing))
        .map_err(|error| error.to_string())?;
    Ok(())
}
