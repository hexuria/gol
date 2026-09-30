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

use harness::{run_until, Boundary, DelegateTarget, Driver, JevDecider, Memory, RunMemory};
use protocol::{
    fold, Capability, DispatchPhase, Event, EventPayload, FailureClass, HarnessState, MessageId,
    RunId, RunSpec, Timestamp,
};

use crate::deliverer::{
    ask_state, deliver, is_user_question, task_answer, AskState, IfUnsent, OwnedDeliverer,
};
use crate::http::{jev_client, Delegation};
use crate::inference::{
    box_attempt_name, box_run_of, completion_events, dispatch_events, finish_turn, gateway_call,
    is_turn, run_cancelled_event, run_failed_event, sandbox_from_env, system_event,
    workspace_run_of, GatewayPoster, HttpGatewayPoster, SandboxError, SandboxHost, TurnError,
};
use crate::models::ModelsConfig;
use crate::queue::{QueueTiming, RedisRunQueue};
use crate::spawner::OwnedSpawner;
use crate::store::{
    is_terminal, Append, MessageStore, OutboxStore, RunStore, StoredMessage, StoredRun,
};

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
    /// The gateway and the sandbox host a background coworker turn uses
    /// (Phase 3.6).
    poster: Arc<dyn GatewayPoster>,
    sandbox: Arc<dyn SandboxHost>,
    /// The catalog each run's tools come from (item 8a): without it, a run
    /// has `echo` alone.
    catalog_dir: Option<std::path::PathBuf>,
}

/// Builder state: a required input not given yet.
pub struct Missing;
/// Builder state: a required input given.
pub struct Given;

/// A `Worker` whose queue, store, memory and Jev address are each required
/// before `build` exists; the timing, the models, the messages, the gateway
/// and the sandbox host are optional.
pub struct WorkerBuilder<Q, S, M, J> {
    queue: Option<RedisRunQueue>,
    store: Option<Arc<dyn RunStore>>,
    memory: Option<Arc<dyn Memory>>,
    jev_base_url: Option<String>,
    timing: QueueTiming,
    models: Arc<ModelsConfig>,
    messages: Option<Arc<dyn MessageStore>>,
    poster: Option<Arc<dyn GatewayPoster>>,
    sandbox: Option<Arc<dyn SandboxHost>>,
    catalog_dir: Option<std::path::PathBuf>,
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
            poster: None,
            sandbox: None,
            catalog_dir: crate::tools::catalog_dir_from_env(),
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
            poster: self.poster,
            sandbox: self.sandbox,
            catalog_dir: self.catalog_dir,
            states: PhantomData,
        }
    }

    /// The catalog directory runs take their catalog tools from, in place
    /// of `GOL_CATALOG_DIR`.
    pub fn catalog_dir(mut self, dir: std::path::PathBuf) -> Self {
        self.catalog_dir = Some(dir);
        self
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

    /// The gateway a background coworker turn calls. Without it, the one the
    /// environment names (`HttpGatewayPoster::from_env`).
    pub fn poster(mut self, poster: Arc<dyn GatewayPoster>) -> Self {
        self.poster = Some(poster);
        self
    }

    /// Where a background Box turn's sandbox runs. Without it, the one the
    /// environment names (`sandbox_from_env`).
    pub fn sandbox(mut self, sandbox: Arc<dyn SandboxHost>) -> Self {
        self.sandbox = Some(sandbox);
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
            poster: self
                .poster
                .unwrap_or_else(|| Arc::new(HttpGatewayPoster::from_env())),
            sandbox: self.sandbox.unwrap_or_else(sandbox_from_env),
            catalog_dir: self.catalog_dir,
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
    /// Which delivery of the run this is: its start count.
    attempt: u32,
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
            // A Box turn is failed only once every attempt's sandbox is gone
            // (84A). Until then its claim is left to expire, so it comes
            // back after its lease, and each later delivery only cleans up.
            // The lease is checked again before the end: cleaning took time
            // this claim renewed nothing in (85A).
            if is_box_turn(&stored.spec) {
                let attempts = starts.min(worker.timing.max_deliveries);
                worker.clean_box(run_id, attempts).map_err(|error| {
                    format!("run {run_id}: {error}; it is retried once its lease expires")
                })?;
                if matches!(
                    worker.queue.renew(run_id, &self.token, worker.timing.lease),
                    Ok(false)
                ) {
                    return Ok(Prepared::Done(Done {
                        claim: self,
                        recorded: Some(Append::Moved),
                        waiting: None,
                    }));
                }
            }
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
            attempt: starts,
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
        let attempt = self.attempt;
        let spec = stored.spec.clone();
        let outcome = std::thread::scope(|scope| {
            let (stop, stopped) = mpsc::channel::<()>();
            std::thread::Builder::new()
                .name("gol-heartbeat".to_string())
                .spawn_scoped(scope, move || claim.heartbeat(&stopped))
                .expect("spawn the heartbeat thread");
            let outcome = worker.store_as_it_goes(stored, &claim.token, attempt);
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
    fn store_as_it_goes(
        &self,
        stored: StoredRun,
        token: &str,
        attempt: u32,
    ) -> Result<Outcome, String> {
        let spec = stored.spec;
        let mut events = stored.events;
        let mut reloads = 0;
        loop {
            match self.run_from(&spec, events, token, attempt)? {
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
            // An ask its asker has not logged yet is left open: the sweep
            // delivers this end once the asker has logged it and is parked.
            match task_answer(spec.run_id, spec.agent_id, ask.id, &task.events) {
                Some(answer) => deliver(
                    self.store.as_ref(),
                    messages.as_ref(),
                    &self.queue,
                    ask.from_run,
                    ask.id,
                    answer,
                    IfUnsent::Leave,
                )
                .map(|_| ()),
                None => Ok(()),
            }
        };
        if let Err(error) = answered() {
            eprintln!("gol: queue worker: answer of task {}: {error}", spec.run_id);
        }
    }

    /// Whether a stop covers run `run`. A store that cannot say is taken as
    /// no stop: the run goes on, and the next boundary asks again.
    fn stopped(&self, run: RunId) -> bool {
        match self.store.stops().map(|stops| stops.stopped(run)) {
            Some(Ok(stopped)) => stopped,
            Some(Err(error)) => {
                eprintln!("gol: queue worker: stop check of run {run}: {error}");
                false
            }
            None => false,
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
        attempt: u32,
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
        // A stop covers the run (Phase 3.4): it ends cancelled, unrun. A Box
        // turn first loses every attempt's sandbox (84A).
        if self.stopped(run_id) {
            if is_box_turn(spec) {
                return self.end_box(spec, attempt, vec![run_cancelled_event(spec)], &holds);
            }
            return append(seen, vec![run_cancelled_event(spec)]);
        }
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
        // A background coworker turn (Phase 3.6) runs as a quick turn does,
        // not through the harness.
        if is_turn(spec) {
            return self.run_turn(spec, events, seen, attempt, &append, &holds);
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
        // A queued run can wait for its user's answer: it is parked.
        driver = driver.with_spawner(spawner, targets).with_user_questions();
        if let Some(messages) = &self.messages {
            driver = driver.with_deliverer(Arc::new(
                OwnedDeliverer::builder()
                    .store(self.store.clone())
                    .messages(messages.clone())
                    .queue(self.queue.clone())
                    .build(),
            ));
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
        // Its tools: echo, and, with a catalog, the catalog tools its
        // owner's manifest names (8a). A store that cannot answer, or an
        // agent that is missing or another principal's, leaves the run to
        // its next delivery, once its lease expires, as a catalog that does
        // not load does: a tool it had must not go missing mid-run. Without
        // a catalog there is nothing to look up.
        let named = match &self.catalog_dir {
            None => Vec::new(),
            Some(_) => self
                .store
                .agent(spec.agent_id)
                .map_err(|error| format!("run {run_id}: its agent: {error}"))?
                .filter(|agent| agent.owner.is(&spec.owner))
                .map(|agent| agent.manifest.tools)
                .ok_or_else(|| {
                    format!(
                        "run {run_id}: its agent is unavailable; it is retried once its lease expires"
                    )
                })?,
        };
        let tools = crate::tools::RunTools::load(self.catalog_dir.as_deref(), named)
            .map_err(|error| format!("run {run_id}: {error}"))?;
        let models = self.models.model_for(spec);
        let memory = RunMemory::new(self.memory.as_ref());
        let mut refused = None;
        let outcome = run_until(
            &mut driver,
            &mut decider,
            &tools.tools(),
            &models,
            &memory,
            &mut |driver| {
                // A stop made while the run ran ends it here: the step's
                // events and its RunCancelled are stored as the tail.
                if self.stopped(run_id) {
                    return Boundary::Cancel;
                }
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

impl Worker {
    /// Runs background coworker turn `spec` from its log (`events`, the
    /// first `seen` stored; its user message was stored with it queued), as
    /// delivery `attempt`: starts it, then calls the gateway and stores the
    /// completion, or the failure (`finish_turn`, or `run_box_turn` in a Box).
    /// Only a gateway turn runs in the background (decision 63A); one stored
    /// otherwise is failed, not run. A turn a dead worker started is picked
    /// up where its log stops, and the gateway is called again (65A). A turn
    /// left open (the store could not answer) is not acknowledged, so it is
    /// tried again.
    fn run_turn(
        &self,
        spec: &RunSpec,
        mut events: Vec<Event>,
        mut seen: usize,
        attempt: u32,
        append: &dyn Fn(usize, Vec<Event>) -> Result<Stopped, String>,
        holds: &dyn Fn() -> bool,
    ) -> Result<Stopped, String> {
        if !matches!(
            spec.work_model.credential,
            protocol::CredentialSource::PlatformGateway
        ) {
            let failed = run_failed_event(
                spec,
                FailureClass::Infrastructure,
                "only a gateway turn runs in the background".to_string(),
            );
            if is_box_turn(spec) {
                return self.end_box(spec, attempt, vec![failed], holds);
            }
            return append(seen, vec![failed]);
        }
        let started = events
            .iter()
            .any(|event| matches!(event.payload, EventPayload::RunStarted));
        if !started {
            let begun = vec![system_event(spec, EventPayload::RunStarted)];
            match append(seen, begun.clone())? {
                Stopped::Store(Append::Appended) => {}
                refused => return Ok(refused),
            }
            seen += begun.len();
            events.extend(begun);
        }
        // A stop that read the turn queued and lost the race to this
        // worker's start (its cancel found the log moved) is seen here,
        // before the gateway is called (66A).
        if self.stopped(spec.run_id) {
            if is_box_turn(spec) {
                return self.end_box(spec, attempt, vec![run_cancelled_event(spec)], holds);
            }
            return append(seen, vec![run_cancelled_event(spec)]);
        }
        if !holds() {
            return Ok(Stopped::LostLease);
        }
        // Read again just before the gateway call: a worker whose lease
        // lapsed may have ended the turn since this one loaded it.
        match self
            .store
            .run(spec.run_id)
            .map_err(|error| error.to_string())?
        {
            None => return Ok(Stopped::Store(Append::Missing)),
            Some(run) if run.events.iter().any(|event| is_terminal(&event.payload)) => {
                return Ok(Stopped::Store(Append::Terminal));
            }
            Some(_) => {}
        }
        if is_box_turn(spec) {
            return self.run_box_turn(spec, attempt, holds);
        }
        match finish_turn(
            self.store.as_ref(),
            spec.clone(),
            events,
            self.poster.as_ref(),
            self.sandbox.as_ref(),
        ) {
            Ok(_) => Ok(Stopped::Store(Append::Appended)),
            Err(TurnError::Conflict(_)) => Ok(Stopped::Store(Append::Terminal)),
            Err(TurnError::NotFound) => Ok(Stopped::Store(Append::Missing)),
            // A gateway failure ends the turn failed; anything else leaves
            // it open.
            Err(error) => match self.store.run(spec.run_id) {
                Ok(Some(run)) if run.events.iter().any(|event| is_terminal(&event.payload)) => {
                    Ok(Stopped::Store(Append::Appended))
                }
                _ => Err(format!("turn left open: {error:?}")),
            },
        }
    }
}

impl Worker {
    /// Runs Box turn `spec` as delivery `attempt`, in a sandbox of its own,
    /// `gol-box-<run>-<attempt>` (82A): the lease checked, the provision,
    /// the lease checked, the gateway call, the lease checked, then
    /// `end_box` with the completion or the failure (84A, 85A). A worker
    /// that lost its lease removes its own sandbox and stops: the turn is
    /// its new holder's (`formal/runlog/BoxTurn.tla`).
    fn run_box_turn(
        &self,
        spec: &RunSpec,
        attempt: u32,
        holds: &dyn Fn() -> bool,
    ) -> Result<Stopped, String> {
        let name = box_attempt_name(spec.run_id, attempt);
        if !holds() {
            return Ok(Stopped::LostLease);
        }
        if let Err(SandboxError::Host(message)) = self.sandbox.provision(&name) {
            let failed = run_failed_event(
                spec,
                FailureClass::Environment,
                format!("provision: {message}"),
            );
            return self.end_box(spec, attempt, vec![failed], holds);
        }
        if !holds() {
            self.drop_own(&name);
            return Ok(Stopped::LostLease);
        }
        // A stop that came during the provision: no gateway call.
        if self.stopped(spec.run_id) {
            return self.end_box(spec, attempt, vec![run_cancelled_event(spec)], holds);
        }
        let answer = self.poster.complete(&gateway_call(spec));
        if !holds() {
            self.drop_own(&name);
            return Ok(Stopped::LostLease);
        }
        let end = match answer {
            Ok(text) => completion_events(spec, &text),
            Err(message) => vec![run_failed_event(
                spec,
                FailureClass::Dependency,
                format!("proxy: {message}"),
            )],
        };
        self.end_box(spec, attempt, end, holds)
    }

    /// Ends Box turn `spec` with `end` once the sandboxes of attempts 1 to
    /// `attempt` are gone (84A), checking the lease just before the append.
    /// A sandbox that cannot be removed leaves the turn open: the claim is
    /// not acknowledged, so the run comes back once its lease expires.
    fn end_box(
        &self,
        spec: &RunSpec,
        attempt: u32,
        end: Vec<Event>,
        holds: &dyn Fn() -> bool,
    ) -> Result<Stopped, String> {
        self.clean_box(spec.run_id, attempt).map_err(|error| {
            format!(
                "run {}: {error}; it is retried once its lease expires",
                spec.run_id
            )
        })?;
        if !holds() {
            return Ok(Stopped::LostLease);
        }
        self.store
            .append_events(spec.run_id, end)
            .map(Stopped::Store)
            .map_err(|error| error.to_string())
    }

    /// Removes the sandboxes of attempts 1 to `attempts` of Box turn `run`,
    /// each confirmed gone (84A).
    fn clean_box(&self, run: RunId, attempts: u32) -> Result<(), String> {
        for attempt in 1..=attempts {
            let name = box_attempt_name(run, attempt);
            if matches!(self.sandbox.absent(&name), Ok(true)) {
                continue;
            }
            if let Err(SandboxError::Host(message)) = self.sandbox.destroy(&name) {
                return Err(format!("sandbox {name} was not removed: {message}"));
            }
            if !matches!(self.sandbox.absent(&name), Ok(true)) {
                return Err(format!("sandbox {name} is not confirmed gone"));
            }
        }
        Ok(())
    }

    /// Removes this worker's own sandbox once it lost its lease. One that
    /// cannot be removed is left to the turn's next cleanup, or the reaper.
    fn drop_own(&self, name: &str) {
        if let Err(SandboxError::Host(message)) = self.sandbox.destroy(name) {
            eprintln!("gol: queue worker: sandbox {name} was not removed: {message}");
        }
    }
}

/// Whether `spec` is a background coworker turn in a Box.
fn is_box_turn(spec: &RunSpec) -> bool {
    is_turn(spec) && spec.placement == protocol::ExecutionPlacement::Box
}

/// Removes every Box sandbox `sandbox` lists whose turn has ended (86C):
/// what a worker that lost its lease, or died, left behind. Then the
/// workspace volume of each ended run the host lists no sandbox of (88A),
/// which the host refuses while a container mounts it (89A); a refusal is
/// tried again by the next sweep. A sandbox or volume of an open turn, or of
/// no stored run, is left. The names removed, sandboxes first.
pub fn sweep_sandboxes(
    store: &dyn RunStore,
    sandbox: &dyn SandboxHost,
) -> Result<Vec<String>, String> {
    let listed = sandbox
        .list()
        .map_err(|SandboxError::Host(message)| message)?;
    let mut removed = Vec::new();
    let mut left = Vec::new();
    for name in listed {
        let Some(run) = box_run_of(&name) else {
            continue;
        };
        if !has_ended(store, run) {
            left.push(run);
            continue;
        }
        match sandbox.destroy(&name) {
            Ok(()) => removed.push(name),
            Err(SandboxError::Host(message)) => {
                left.push(run);
                eprintln!("gol: sandbox sweep: {name} was not removed: {message}")
            }
        }
    }
    let volumes = match sandbox.list_volumes() {
        Ok(volumes) => volumes,
        Err(SandboxError::Host(message)) => {
            eprintln!("gol: sandbox sweep: volumes: {message}");
            return Ok(removed);
        }
    };
    for name in volumes {
        let Some(run) = workspace_run_of(&name) else {
            continue;
        };
        // Only a Box run's volume is this host's: a Local or Reverse run's
        // is the desktop's, even when the desktop shares this daemon.
        if left.contains(&run) || !ended_box(store, run) {
            continue;
        }
        match sandbox.remove_volume(&name) {
            Ok(()) => removed.push(name),
            Err(SandboxError::Host(message)) => {
                eprintln!("gol: sandbox sweep: volume {name} was not removed: {message}")
            }
        }
    }
    Ok(removed)
}

/// Whether run `run` is stored and its log has ended. A store that cannot
/// say is taken as no: the sweep leaves it for next time.
fn has_ended(store: &dyn RunStore, run: RunId) -> bool {
    stored_ended(store, run).is_some()
}

/// Whether run `run` is a Box run whose log has ended.
fn ended_box(store: &dyn RunStore, run: RunId) -> bool {
    stored_ended(store, run)
        .is_some_and(|stored| stored.spec.placement == protocol::ExecutionPlacement::Box)
}

/// Run `run`, if it is stored and its log has ended.
fn stored_ended(store: &dyn RunStore, run: RunId) -> Option<StoredRun> {
    match store.run(run) {
        Ok(Some(stored))
            if stored
                .events
                .iter()
                .any(|event| is_terminal(&event.payload)) =>
        {
            Some(stored)
        }
        Ok(_) => None,
        Err(error) => {
            eprintln!("gol: sandbox sweep: run {run}: {error}");
            None
        }
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
        // `formal/runqueue` `Park` then `Recheck`), or a stop did: a stop
        // that found the run neither queued nor parked yet left it to this
        // worker, and a woken stopped run is cancelled when it is claimed.
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
                if ask_state(&events, ask) != AskState::Open || claim.worker.stopped(run_id) {
                    queue.wake(run_id, ask)?;
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

/// How long an outbox entry is kept (decision 38A): a stream resuming from
/// an older number is told to reload (Phase 3.2).
pub const OUTBOX_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Hands back runs whose lease ran out, sweeps runs left pending and the
/// asks of parked runs, and prunes outbox entries older than
/// `OUTBOX_RETENTION`, every `reap_every`, for as long as the process runs.
pub fn reap_forever(
    queue: &RedisRunQueue,
    store: &dyn RunStore,
    messages: Option<&dyn MessageStore>,
    outbox: Option<&dyn OutboxStore>,
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
        if let Some(outbox) = outbox {
            let before = Timestamp::unix_millis(
                Timestamp::now()
                    .as_unix_millis()
                    .saturating_sub(OUTBOX_RETENTION.as_millis() as i64),
            );
            match catch_unwind(AssertUnwindSafe(|| outbox.prune_outbox(before))) {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => eprintln!("gol: outbox prune: {error}"),
                Err(_) => eprintln!("gol: outbox prune: pruning panicked"),
            }
        }
        std::thread::sleep(timing.reap_every);
    }
}

/// How often a server looks through its host's Box sandboxes (86C).
const SANDBOX_SWEEP_EVERY: Duration = Duration::from_secs(30);

/// Starts the thread that removes the Box sandboxes and volumes of ended
/// runs (`sweep_sandboxes_forever`). Every server runs one, queue or not:
/// quick Box turns leave volumes too.
pub fn start_sandbox_sweep(
    store: Arc<dyn RunStore>,
    sandbox: Arc<dyn SandboxHost>,
) -> Result<(), String> {
    std::thread::Builder::new()
        .name("gol-sandbox-sweep".to_string())
        .spawn(move || sweep_sandboxes_forever(store.as_ref(), sandbox.as_ref()))
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// Removes the Box sandboxes of ended turns (`sweep_sandboxes`) every
/// `SANDBOX_SWEEP_EVERY`, for as long as the process runs. A thread of its
/// own: a slow host does not hold up the reaper.
pub fn sweep_sandboxes_forever(store: &dyn RunStore, sandbox: &dyn SandboxHost) {
    loop {
        match catch_unwind(AssertUnwindSafe(|| sweep_sandboxes(store, sandbox))) {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => eprintln!("gol: sandbox sweep: {error}"),
            Err(_) => eprintln!("gol: sandbox sweep: sweeping panicked"),
        }
        std::thread::sleep(SANDBOX_SWEEP_EVERY);
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
            // A trigger's run is pushed only past its gate (Phase 4.2):
            // settled as its fire settles it, pushed once or held.
            Ok(Some(run))
                if waiting(&run)
                    && run.spec.metadata.contains_key(crate::triggers::TRIGGER_KEY) =>
            {
                match crate::triggers::settle_fired_run(queue, store, &run.spec) {
                    Ok(crate::spawner::Fire::Pushed) => pushed.push(run_id),
                    Ok(_) => {}
                    Err(error) => eprintln!("gol: queue sweep: settle run {run_id}: {error}"),
                }
            }
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

/// Finishes what a crash can leave undone around an ask (decision 34A).
/// For each parked run: wakes it if its log no longer waits on its ask (the
/// answer landed, or the run ended) or the store lost it; otherwise
/// delivers the end of the asked task if the task has ended, or, when the
/// ask is closed or gone while the log still waits on it, `AskTimedOut`.
/// Then answers each open ask past its deadline with its task's end if the
/// task has ended, else `AskTimedOut` (decision 28A), closing one its asker
/// never logged. Returns the runs it woke or answered. A run or ask it
/// cannot load or deliver to now is left for the next sweep; the others are
/// still swept.
pub fn sweep_asks(
    queue: &RedisRunQueue,
    store: &dyn RunStore,
    messages: &dyn MessageStore,
    now: Timestamp,
) -> Result<Vec<RunId>, String> {
    let mut swept = Vec::new();
    for (run_id, ask) in queue.parked()? {
        let (waits, question) = match store.run(run_id) {
            Ok(Some(run)) => (
                ask_state(&run.events, ask) == AskState::Open
                    && !run.events.iter().any(|event| is_terminal(&event.payload)),
                is_user_question(&run.events, ask),
            ),
            // A parked run the store lost is woken: a worker acknowledges it.
            Ok(None) => (false, false),
            Err(error) => {
                eprintln!("gol: ask sweep: load run {run_id}: {error}");
                continue;
            }
        };
        let result = if !waits {
            queue.wake(run_id, ask).map(|_| true)
        } else if question {
            // A question to the user has no timeout (decision 57A): it
            // waits for the answer, or a stop. A stopped one is woken, and
            // cancelled when it is claimed; one whose stop cannot be read
            // now is looked at again by the next sweep.
            match store.stops().map(|stops| stops.stopped(run_id)) {
                Some(Ok(true)) => queue.wake(run_id, ask).map(|_| true),
                Some(Ok(false)) | None => continue,
                Some(Err(error)) => {
                    eprintln!("gol: ask sweep: stop check of run {run_id}: {error}");
                    continue;
                }
            }
        } else {
            let timed_out = EventPayload::AskTimedOut { message_id: ask };
            let answer = match messages.message(ask) {
                Ok(Some(row)) => match task_end(store, &row) {
                    Ok(Some(answer)) => Some(answer),
                    // Closed while the log still waits: at its deadline,
                    // before the asker logged it. A timeout is final, so
                    // one the store cannot confirm waits for the next sweep.
                    Ok(None) => match open(messages, &row) {
                        Ok(false) => Some(timed_out),
                        Ok(true) => None,
                        Err(error) => {
                            eprintln!("gol: ask sweep: open ask {ask}: {error}");
                            None
                        }
                    },
                    Err(error) => {
                        eprintln!("gol: ask sweep: task of ask {ask}: {error}");
                        None
                    }
                },
                Ok(None) => Some(timed_out),
                Err(error) => {
                    eprintln!("gol: ask sweep: load ask {ask}: {error}");
                    None
                }
            };
            match answer {
                Some(answer) => {
                    deliver(store, messages, queue, run_id, ask, answer, IfUnsent::Close)
                }
                None => continue,
            }
        };
        match result {
            Ok(true) => swept.push(run_id),
            Ok(false) => {}
            Err(error) => eprintln!("gol: ask sweep: run {run_id}: {error}"),
        }
    }
    for ask in messages
        .open_asks_due(now)
        .map_err(|error| error.to_string())?
    {
        let answer = match task_end(store, &ask) {
            Ok(Some(answer)) => answer,
            Ok(None) => EventPayload::AskTimedOut { message_id: ask.id },
            // Not read now: a timeout would be final, so the next sweep
            // looks again.
            Err(error) => {
                eprintln!("gol: ask sweep: task of ask {}: {error}", ask.id);
                continue;
            }
        };
        match deliver(
            store,
            messages,
            queue,
            ask.from_run,
            ask.id,
            answer,
            IfUnsent::Close,
        ) {
            Ok(true) => swept.push(ask.from_run),
            Ok(false) => {}
            Err(error) => eprintln!("gol: ask sweep: ask {}: {error}", ask.id),
        }
    }
    Ok(swept)
}

/// Whether `ask` is still open in the messages store; an error when the
/// store cannot say now.
fn open(messages: &dyn MessageStore, ask: &StoredMessage) -> Result<bool, String> {
    let Some(task) = ask.task_run else {
        return Ok(false);
    };
    Ok(messages
        .ask_of_task(task)
        .map_err(|error| error.to_string())?
        .is_some_and(|open| open.id == ask.id))
}

/// The answer `ask`'s task gives now that it has ended, if it has; an error
/// when the task cannot be read now.
fn task_end(store: &dyn RunStore, ask: &StoredMessage) -> Result<Option<EventPayload>, String> {
    let Some(task) = ask.task_run else {
        return Ok(None);
    };
    Ok(store
        .run(task)
        .map_err(|error| error.to_string())?
        .and_then(|task| task_answer(task.spec.run_id, task.spec.agent_id, ask.id, &task.events)))
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

/// How often each server's scheduler looks for due triggers.
const SCHEDULE_EVERY: std::time::Duration = std::time::Duration::from_secs(1);

/// Starts `settings.workers` worker threads, one scheduler and one reaper,
/// which also sweeps, all running for as long as the process does. Queued
/// runs may message each other through `messages` (Phase 2.3). The workers
/// use `sandbox`, the server's one sandbox host, which its sandbox sweep
/// (`start_sandbox_sweep`) looks through too.
pub fn start_queue(
    settings: &QueueSettings,
    stores: crate::stores::Stores,
    jev_base_url: &str,
    models: Arc<ModelsConfig>,
    sandbox: Arc<dyn SandboxHost>,
) -> Result<(), String> {
    let crate::stores::Stores {
        runs: store,
        memory,
        messages,
        outbox,
    } = stores;
    let timing = QueueTiming::default();
    for index in 0..settings.workers {
        let worker = Worker::builder()
            .queue(RedisRunQueue::open(&settings.redis_url))
            .sandbox(sandbox.clone())
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
    // One scheduler per server fires schedule triggers (Phase 4.2).
    let (scheduled, scheduler) = (
        store.clone(),
        Arc::new(RedisRunQueue::open(&settings.redis_url)),
    );
    std::thread::Builder::new()
        .name("gol-scheduler".to_string())
        .spawn(move || crate::scheduler::schedule_forever(scheduled, scheduler, SCHEDULE_EVERY))
        .map_err(|error| error.to_string())?;
    let reaper = RedisRunQueue::open(&settings.redis_url);
    std::thread::Builder::new()
        .name("gol-reaper".to_string())
        .spawn(move || {
            reap_forever(
                &reaper,
                store.as_ref(),
                Some(messages.as_ref()),
                Some(outbox.as_ref()),
                timing,
            )
        })
        .map_err(|error| error.to_string())?;
    Ok(())
}
