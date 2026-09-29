use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use harness::StoreError;
use protocol::{
    AgentId, ArtifactId, Capability, Event, EventPayload, MessageId, Owner, RunId, RunSpec,
    Timestamp,
};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AgentManifest {
    pub id: AgentId,
    pub version: String,
    /// What a delegating run's decider calls this agent. Manifests stored
    /// before names existed have none.
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub instructions: String,
    pub tools: Vec<String>,
    pub required_capabilities: Vec<Capability>,
}

/// A manifest and the principal that stored it. Only that principal may
/// replace it or start runs of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredAgent {
    pub manifest: AgentManifest,
    pub owner: Owner,
}

/// What `RunStore::put_agent` did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutAgent {
    /// Stored, as a new agent or replacing the caller's own.
    Stored,
    /// Another principal owns that agent id. Nothing was written.
    OwnedByOther,
}

#[derive(Clone, Debug)]
pub struct StoredRun {
    pub spec: RunSpec,
    pub events: Vec<Event>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredArtifact {
    pub id: ArtifactId,
    pub run_id: RunId,
    pub name: String,
    pub body: Vec<u8>,
}

/// What `RunStore::put_run` did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutRun {
    /// The run is new, and is now stored.
    Stored,
    /// A run was already stored under this id. Nothing was written.
    Existed,
}

/// What `RunStore::append_events` did. The log only grows, and it ends at the
/// first terminal event (formal/runlog/RunLog.tla).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Append {
    Appended,
    /// The stored log already holds a terminal event (see `is_terminal`).
    /// Nothing was written.
    Terminal,
    /// No run is stored under that id. Nothing was written.
    Missing,
    /// `append_events_after` only: the stored log is no longer as long as
    /// the writer saw it; another writer appended since. Nothing was written.
    Moved,
}

/// The payloads that end a run: `DispatchPhase::is_terminal` after the reducer.
pub fn is_terminal(payload: &EventPayload) -> bool {
    matches!(
        payload,
        EventPayload::RunCompleted { .. }
            | EventPayload::RunFailed { .. }
            | EventPayload::RunCancelled
            | EventPayload::RunExpired
    )
}

/// A batch of events, as `put_run` and `append_events` take it, ends at its
/// terminal event, if it has one: the log ends at the first.
pub(crate) fn check_one_terminal(events: &[Event]) -> Result<(), StoreError> {
    let after_terminal = events
        .iter()
        .position(|event| is_terminal(&event.payload))
        .is_some_and(|at| at + 1 < events.len());
    if after_terminal {
        return Err(StoreError::new(
            "a batch holds an event after its terminal event",
        ));
    }
    Ok(())
}

/// Every method can fail with a StoreError: the store is unreachable, or a
/// write's outcome is unknown. Callers report it and do not retry a write,
/// which may have committed.
pub trait RunStore: Send + Sync {
    /// Store a manifest, unless another principal already owns its id. The
    /// check and the write are one atomic step.
    fn put_agent(&self, agent: StoredAgent) -> Result<PutAgent, StoreError>;
    fn agent(&self, id: AgentId) -> Result<Option<StoredAgent>, StoreError>;
    /// The agents `owner` holds (`Owner::is`), in id order.
    fn agents_of(&self, owner: &Owner) -> Result<Vec<StoredAgent>, StoreError>;
    /// Store a new run. A run already stored under that id keeps its spec and
    /// events, so a redelivered put cannot drop anything appended since. A
    /// batch with an event after its terminal event is refused.
    fn put_run(&self, run: StoredRun) -> Result<PutRun, StoreError>;
    /// Append `events` onto the stored run in one atomic step, unless the
    /// stored log is already terminal or the run is missing. `spec` and the
    /// events already stored never change. A batch with an event after its
    /// terminal event is refused.
    fn append_events(&self, id: RunId, events: Vec<Event>) -> Result<Append, StoreError>;
    /// `append_events`, and refused as `Moved` unless the stored log is still
    /// `seen` events long, in the same atomic step. A writer that stores a
    /// run step by step (a queue worker, Phase 1.5b) appends with this, so
    /// no other writer's events land between what it read and what it adds.
    fn append_events_after(
        &self,
        id: RunId,
        seen: usize,
        events: Vec<Event>,
    ) -> Result<Append, StoreError>;
    fn run(&self, id: RunId) -> Result<Option<StoredRun>, StoreError>;
    /// The run with at most `limit` of its events: those after the first
    /// `after`. A store that keeps events in order by row overrides this to
    /// read only the page.
    fn run_page(
        &self,
        id: RunId,
        after: usize,
        limit: usize,
    ) -> Result<Option<StoredRun>, StoreError> {
        Ok(self.run(id)?.map(|mut run| {
            run.events = run.events.into_iter().skip(after).take(limit).collect();
            run
        }))
    }
    fn put_artifact(&self, artifact: StoredArtifact) -> Result<(), StoreError>;
    fn artifact(&self, id: ArtifactId) -> Result<Option<StoredArtifact>, StoreError>;
    /// The outbox this store numbers its events in, if it keeps one
    /// (Phase 3.1); the streams read it (Phase 3.2).
    fn outbox(&self) -> Option<&dyn OutboxStore> {
        None
    }

    /// The threads this store can list, if it keeps them (Phase 3.3).
    fn threads(&self) -> Option<&dyn ThreadStore> {
        None
    }
    /// The stops this store keeps, if it keeps them (Phase 3.4).
    fn stops(&self) -> Option<&dyn StopStore> {
        None
    }
}

/// A message between agents (Phase 2), as its deliverer stored it: from
/// `from_run`'s `decision`, which names it (decision 30A), to `to_agent`. A
/// tell or an ask started `task_run` for the target; an ask may have a
/// `deadline` for its timeout (28A), counted from its first send, and
/// `timeout_secs` is the timeout it asked for. `hop` is the sender's
/// delegation depth.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredMessage {
    pub id: MessageId,
    pub owner: Owner,
    pub from_run: RunId,
    pub from_agent: AgentId,
    pub decision: u32,
    pub to_agent: AgentId,
    pub body: String,
    pub expects_reply: bool,
    pub reply_to: Option<MessageId>,
    pub task_run: Option<RunId>,
    pub deadline: Option<Timestamp>,
    /// Stored within the message's JSON body, as `hop` is; rows stored
    /// without them read `None` and 0.
    #[serde(default)]
    pub timeout_secs: Option<u32>,
    #[serde(default)]
    pub hop: u32,
}

/// What `MessageStore::put_message` did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PutMessage {
    Stored,
    /// That run's decision already sent this message; nothing was written.
    Existed(Box<StoredMessage>),
}

/// Where messages between agents are kept (Phase 2.1).
pub trait MessageStore: Send + Sync {
    /// Stores `message` unless its run and decision already sent one, in
    /// one atomic step.
    fn put_message(&self, message: StoredMessage) -> Result<PutMessage, StoreError>;
    fn message(&self, id: MessageId) -> Result<Option<StoredMessage>, StoreError>;
    /// The open (unanswered) ask whose task is `task_run`: the one that
    /// task's end answers.
    fn ask_of_task(&self, task_run: RunId) -> Result<Option<StoredMessage>, StoreError>;
    /// Marks the ask answered, once: true for the call that did.
    fn answer(&self, ask: MessageId) -> Result<bool, StoreError>;
    /// Asks not yet answered whose deadline is at or before `now`.
    fn open_asks_due(&self, now: Timestamp) -> Result<Vec<StoredMessage>, StoreError>;
}

/// One event in its owner's outbox (Phase 3.1): `seq` numbers it among all
/// the events of runs its owner's principal holds, from 1 with no gap
/// (decisions 16A, 36A), and `run_seq` is its place in its run's log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxEntry {
    pub seq: u64,
    pub run_id: RunId,
    pub run_seq: u64,
    pub event: Event,
}

/// A hint that a principal's outbox has new entries (Phase 3.2): a stable
/// hash of the principal, the same in every process, so one server's write
/// can wake another server's stream. It carries no state: a stream woken by
/// it re-reads after its cursor, so a lost or extra hint costs a read, not
/// an event. `ALL` wakes every stream.
pub type Hint = u64;

/// The hint that wakes every stream, after hints may have been lost.
pub const ALL: Hint = 0;

/// The hint for `owner`'s principal: FNV-1a over issuer, NUL, subject (the
/// tenant is not part of the principal, decision 36A). Never `ALL`.
pub fn principal_hint(owner: &Owner) -> Hint {
    principal_hint_of(&owner.issuer, &owner.subject)
}

pub(crate) fn principal_hint_of(issuer: &str, subject: &str) -> Hint {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in issuer.bytes().chain([0]).chain(subject.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash.max(1)
}

/// Where a store publishes its hints: every stream of this process
/// subscribes. A receiver that falls behind is told it lagged and re-reads.
/// `live` says whether hints from other processes arrive (Postgres: the
/// listener is connected); streams poll faster while it is false.
pub(crate) struct Hints {
    sender: tokio::sync::broadcast::Sender<Hint>,
    live: Arc<AtomicBool>,
}

impl Hints {
    /// Hints for a store that is the only writer (in memory): always live.
    pub(crate) fn local() -> Self {
        Self::new(true)
    }

    /// Hints that are live once a listener says so.
    pub(crate) fn listened() -> Self {
        Self::new(false)
    }

    fn new(live: bool) -> Self {
        Self {
            sender: tokio::sync::broadcast::channel(1024).0,
            live: Arc::new(AtomicBool::new(live)),
        }
    }

    pub(crate) fn send(&self, hint: Hint) {
        // No subscriber is not an error: no stream is waiting.
        let _ = self.sender.send(hint);
    }

    pub(crate) fn subscribe(&self) -> HintStream {
        HintStream {
            receiver: self.sender.subscribe(),
            live: self.live.clone(),
        }
    }

    pub(crate) fn sender(&self) -> tokio::sync::broadcast::Sender<Hint> {
        self.sender.clone()
    }

    pub(crate) fn live(&self) -> Arc<AtomicBool> {
        self.live.clone()
    }
}

impl Default for Hints {
    fn default() -> Self {
        Self::local()
    }
}

/// A stream's subscription to its store's hints.
pub struct HintStream {
    pub receiver: tokio::sync::broadcast::Receiver<Hint>,
    live: Arc<AtomicBool>,
}

impl HintStream {
    /// Whether hints from every writer arrive now. Relaxed is enough: the
    /// flag orders nothing, it only picks how often a waiting stream polls,
    /// and a stale read costs at most one slower or faster poll.
    pub fn live(&self) -> bool {
        self.live.load(Ordering::Relaxed)
    }
}

/// A page of an owner's outbox, read at one moment: its entries, and the
/// number up to which entries were pruned. A reader whose cursor is below
/// `pruned_through` has lost entries and must reload (Phase 3.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxPage {
    pub entries: Vec<OutboxEntry>,
    pub pruned_through: u64,
    /// The principal's last number: a stream with no cursor starts after it.
    pub last: u64,
}

/// The per-owner outbox that every event a run store keeps is numbered in,
/// in the same step as the event (`formal/outbox`). A reader resumes after
/// the last number it saw.
pub trait OutboxStore: Send + Sync {
    /// The entries of `owner`'s principal numbered after `after`, in order,
    /// at most `limit`.
    fn outbox_after(
        &self,
        owner: &Owner,
        after: u64,
        limit: usize,
    ) -> Result<OutboxPage, StoreError>;
    /// For each principal, removes its entries up to the last one stored
    /// before `before`, and returns how many went. What stays is a suffix of
    /// its numbers even when writers' clocks differ, and its count stays, so
    /// numbers never restart (decision 38A).
    fn prune_outbox(&self, before: Timestamp) -> Result<u64, StoreError>;
    /// Hints of new entries, from this store and (Postgres) from any other
    /// process writing the same database, from now on.
    fn hints(&self) -> HintStream;
}

/// An outbox row: what `OutboxEntry` reads through to the run's log.
struct OutboxRow {
    principal: (String, String),
    seq: u64,
    run_id: RunId,
    run_seq: u64,
    stored_ms: i64,
}

/// The in-memory outbox: each principal's count and pruned-through number,
/// and the rows in the order they were numbered.
#[derive(Default)]
struct Outbox {
    counts: HashMap<(String, String), u64>,
    pruned: HashMap<(String, String), u64>,
    rows: Vec<OutboxRow>,
}

/// The key an owner's outbox is kept under: the principal (decision 36A).
fn principal(owner: &Owner) -> (String, String) {
    (owner.issuer.clone(), owner.subject.clone())
}

/// A thread (Phase 3.3, decision 47A): the runs of one session of one
/// principal. `agent_id` is its coordinator's, the agent of its first run
/// with no parent; `started_ms` is when that run was stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadSummary {
    pub thread_id: String,
    pub agent_id: AgentId,
    pub started_ms: i64,
    pub runs: u64,
}

/// The thread a run belongs to: its session id, if it has one that is not
/// empty.
pub fn thread_of(spec: &RunSpec) -> Option<&str> {
    spec.metadata
        .get(protocol::SESSION_ID)
        .map(String::as_str)
        .filter(|session| !session.is_empty())
}

/// When a run was stored, for ordering: its first event's time.
pub(crate) fn created_ms(run: &StoredRun) -> i64 {
    run.events
        .first()
        .map(|event| event.envelope.at.as_unix_millis())
        .unwrap_or(0)
}

/// Threads by principal (decision 48A).
pub trait ThreadStore: Send + Sync {
    /// `owner`'s principal's threads, newest first: after the first `after`,
    /// at most `limit`.
    fn threads_of(
        &self,
        owner: &Owner,
        after: usize,
        limit: usize,
    ) -> Result<Vec<ThreadSummary>, StoreError>;
    /// The runs of `owner`'s principal's thread `thread`, oldest first, at
    /// most `limit`. Empty for a thread that is not the principal's.
    fn runs_of_thread(
        &self,
        owner: &Owner,
        thread: &str,
        limit: usize,
    ) -> Result<Vec<StoredRun>, StoreError>;
    /// The spec of the thread's coordinator: its first run with no parent.
    /// None for a thread that is not the principal's.
    fn thread_root(&self, owner: &Owner, thread: &str) -> Result<Option<RunSpec>, StoreError>;
}

/// What a stop covers (Phase 3.4, decision 52A): a run and every run under
/// it, a thread, or everything its principal owns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StopScope {
    Run(RunId),
    Thread(String),
    Owner,
}

impl StopScope {
    /// The scope's kind and key, as a stop is stored.
    pub(crate) fn kind_and_key(&self) -> (&'static str, String) {
        match self {
            Self::Run(run) => ("run", run.to_string()),
            Self::Thread(thread) => ("thread", thread.clone()),
            Self::Owner => ("owner", String::new()),
        }
    }
}

/// Stop requests (Phase 3.4, decisions 53A and 54A). A stop covers the runs
/// its scope names that existed when it was made, and every run under them
/// whenever those start: a run is stopped when a stop by its principal names
/// it or a run above it, its thread, or the principal, and was made at or
/// after that run was created.
pub trait StopStore: Send + Sync {
    /// Records `owner`'s stop of `scope`, made `at`.
    fn put_stop(&self, owner: &Owner, scope: &StopScope, at: Timestamp) -> Result<(), StoreError>;
    /// Whether a stop covers the stored run `run`. False for a run not stored.
    fn stopped(&self, run: RunId) -> Result<bool, StoreError>;
    /// The runs of `owner`'s principal that `scope` covers and that have not
    /// ended: the run and every run under it, the thread's, or all of them.
    fn open_runs_under(
        &self,
        owner: &Owner,
        scope: &StopScope,
    ) -> Result<Vec<StoredRun>, StoreError>;
}

/// Whether a stop of `scope`, made at `at`, covers `run` itself (not the
/// runs under it).
fn stop_names(scope: &StopScope, at: i64, run: &StoredRun) -> bool {
    at >= created_ms(run)
        && match scope {
            StopScope::Run(id) => *id == run.spec.run_id,
            StopScope::Thread(thread) => thread_of(&run.spec) == Some(thread.as_str()),
            StopScope::Owner => true,
        }
}

/// A stop as the in-memory store keeps it: its principal, scope and time.
type StopEntry = ((String, String), StopScope, i64);

/// The in-memory store. A poisoned lock (a writer panicked holding it) still
/// serves reads, since every write completes before its guard drops; a write
/// under a poisoned lock is refused as a StoreError (owner decision 3A).
#[derive(Default)]
pub struct InMemoryStore {
    agents: Mutex<HashMap<AgentId, StoredAgent>>,
    runs: Mutex<HashMap<RunId, StoredRun>>,
    artifacts: Mutex<HashMap<ArtifactId, StoredArtifact>>,
    /// Each message, and whether it (an ask) is answered.
    messages: Mutex<HashMap<MessageId, (StoredMessage, bool)>>,
    /// A writer takes it only while holding `runs`, after it: the run, then
    /// the count. Reads and prunes take it alone.
    outbox: Mutex<Outbox>,
    /// Each stop: its principal, scope and time. Taken alone.
    stops: Mutex<Vec<StopEntry>>,
    hints: Hints,
}

impl InMemoryStore {
    /// Numbers `count` events stored in `run`'s log after `stored` events,
    /// in its owner's outbox. The caller holds the runs lock.
    fn number(
        &self,
        owner: &Owner,
        run: RunId,
        stored: usize,
        count: usize,
    ) -> Result<(), StoreError> {
        let mut outbox = write(&self.outbox, "outbox")?;
        let principal = principal(owner);
        let first = outbox.counts.get(&principal).copied().unwrap_or(0) + 1;
        let stored_ms = Timestamp::now().as_unix_millis();
        for n in 0..count as u64 {
            outbox.rows.push(OutboxRow {
                principal: principal.clone(),
                seq: first + n,
                run_id: run,
                run_seq: stored as u64 + n + 1,
                stored_ms,
            });
        }
        outbox.counts.insert(principal, first + count as u64 - 1);
        // A stream woken now re-reads, and waits on `runs` for the events.
        self.hints.send(principal_hint(owner));
        Ok(())
    }

    /// `append_events`, and with `seen` also refused as `Moved` unless the
    /// log is `seen` events long: all under the lock.
    fn append_checked(
        &self,
        id: RunId,
        seen: Option<usize>,
        events: Vec<Event>,
    ) -> Result<Append, StoreError> {
        check_one_terminal(&events)?;
        let mut runs = write(&self.runs, "run")?;
        let Some(stored) = runs.get_mut(&id) else {
            return Ok(Append::Missing);
        };
        if stored
            .events
            .iter()
            .any(|event| is_terminal(&event.payload))
        {
            return Ok(Append::Terminal);
        }
        if seen.is_some_and(|seen| seen != stored.events.len()) {
            return Ok(Append::Moved);
        }
        self.number(&stored.spec.owner, id, stored.events.len(), events.len())?;
        stored.events.extend(events);
        Ok(Append::Appended)
    }
}

impl MessageStore for InMemoryStore {
    fn put_message(&self, message: StoredMessage) -> Result<PutMessage, StoreError> {
        let mut messages = write(&self.messages, "message")?;
        if let Some((existing, _)) = messages.values().find(|(stored, _)| {
            stored.from_run == message.from_run && stored.decision == message.decision
        }) {
            return Ok(PutMessage::Existed(Box::new(existing.clone())));
        }
        messages.insert(message.id, (message, false));
        Ok(PutMessage::Stored)
    }

    fn message(&self, id: MessageId) -> Result<Option<StoredMessage>, StoreError> {
        Ok(read(&self.messages)
            .get(&id)
            .map(|(message, _)| message.clone()))
    }

    fn ask_of_task(&self, task_run: RunId) -> Result<Option<StoredMessage>, StoreError> {
        Ok(read(&self.messages)
            .values()
            .find(|(message, answered)| {
                !answered && message.expects_reply && message.task_run == Some(task_run)
            })
            .map(|(message, _)| message.clone()))
    }

    fn answer(&self, ask: MessageId) -> Result<bool, StoreError> {
        let mut messages = write(&self.messages, "message")?;
        Ok(match messages.get_mut(&ask) {
            Some((_, answered)) if !*answered => {
                *answered = true;
                true
            }
            _ => false,
        })
    }

    fn open_asks_due(&self, now: Timestamp) -> Result<Vec<StoredMessage>, StoreError> {
        let mut due: Vec<StoredMessage> = read(&self.messages)
            .values()
            .filter(|(message, answered)| {
                !answered
                    && message.expects_reply
                    && message
                        .deadline
                        .is_some_and(|deadline| deadline.as_unix_millis() <= now.as_unix_millis())
            })
            .map(|(message, _)| message.clone())
            .collect();
        due.sort_by_key(|message| message.id.as_uuid());
        Ok(due)
    }
}

fn read<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(PoisonError::into_inner)
}

fn write<'a, T>(lock: &'a Mutex<T>, name: &str) -> Result<MutexGuard<'a, T>, StoreError> {
    lock.lock()
        .map_err(|_| StoreError::new(format!("{name} store lock poisoned")))
}

impl RunStore for InMemoryStore {
    fn put_agent(&self, agent: StoredAgent) -> Result<PutAgent, StoreError> {
        let mut agents = write(&self.agents, "agent")?;
        let id = agent.manifest.id;
        if agents
            .get(&id)
            .is_some_and(|stored| !stored.owner.is(&agent.owner))
        {
            return Ok(PutAgent::OwnedByOther);
        }
        agents.insert(id, agent);
        Ok(PutAgent::Stored)
    }

    fn agent(&self, id: AgentId) -> Result<Option<StoredAgent>, StoreError> {
        Ok(read(&self.agents).get(&id).cloned())
    }

    fn agents_of(&self, owner: &Owner) -> Result<Vec<StoredAgent>, StoreError> {
        let mut agents: Vec<StoredAgent> = read(&self.agents)
            .values()
            .filter(|agent| agent.owner.is(owner))
            .cloned()
            .collect();
        agents.sort_by_key(|agent| agent.manifest.id.as_uuid());
        Ok(agents)
    }

    fn put_run(&self, run: StoredRun) -> Result<PutRun, StoreError> {
        check_one_terminal(&run.events)?;
        match write(&self.runs, "run")?.entry(run.spec.run_id) {
            std::collections::hash_map::Entry::Occupied(_) => Ok(PutRun::Existed),
            std::collections::hash_map::Entry::Vacant(slot) => {
                self.number(&run.spec.owner, run.spec.run_id, 0, run.events.len())?;
                slot.insert(run);
                Ok(PutRun::Stored)
            }
        }
    }

    fn append_events(&self, id: RunId, events: Vec<Event>) -> Result<Append, StoreError> {
        self.append_checked(id, None, events)
    }

    fn append_events_after(
        &self,
        id: RunId,
        seen: usize,
        events: Vec<Event>,
    ) -> Result<Append, StoreError> {
        self.append_checked(id, Some(seen), events)
    }

    fn run(&self, id: RunId) -> Result<Option<StoredRun>, StoreError> {
        Ok(read(&self.runs).get(&id).cloned())
    }

    fn put_artifact(&self, artifact: StoredArtifact) -> Result<(), StoreError> {
        write(&self.artifacts, "artifact")?.insert(artifact.id, artifact);
        Ok(())
    }

    fn artifact(&self, id: ArtifactId) -> Result<Option<StoredArtifact>, StoreError> {
        Ok(read(&self.artifacts).get(&id).cloned())
    }

    fn outbox(&self) -> Option<&dyn OutboxStore> {
        Some(self)
    }
    fn threads(&self) -> Option<&dyn ThreadStore> {
        Some(self)
    }

    fn stops(&self) -> Option<&dyn StopStore> {
        Some(self)
    }
}

impl StopStore for InMemoryStore {
    fn put_stop(&self, owner: &Owner, scope: &StopScope, at: Timestamp) -> Result<(), StoreError> {
        write(&self.stops, "stop")?.push((principal(owner), scope.clone(), at.as_unix_millis()));
        Ok(())
    }

    fn stopped(&self, run: RunId) -> Result<bool, StoreError> {
        let stops = read(&self.stops).clone();
        let runs = read(&self.runs);
        // The run, then each run above it, up its lineage.
        let mut next = Some(run);
        while let Some(id) = next {
            let Some(member) = runs.get(&id) else {
                break;
            };
            let key = principal(&member.spec.owner);
            if stops
                .iter()
                .any(|(owner, scope, at)| *owner == key && stop_names(scope, *at, member))
            {
                return Ok(true);
            }
            next = member.spec.lineage.parent;
        }
        Ok(false)
    }

    fn open_runs_under(
        &self,
        owner: &Owner,
        scope: &StopScope,
    ) -> Result<Vec<StoredRun>, StoreError> {
        let runs = read(&self.runs);
        // Whether `id` is `under` or a run below it.
        let within = |id: RunId, under: RunId| {
            let mut next = Some(id);
            while let Some(current) = next {
                if current == under {
                    return true;
                }
                next = runs.get(&current).and_then(|run| run.spec.lineage.parent);
            }
            false
        };
        Ok(runs
            .values()
            .filter(|run| run.spec.owner.is(owner))
            .filter(|run| !run.events.iter().any(|event| is_terminal(&event.payload)))
            .filter(|run| match scope {
                StopScope::Run(under) => within(run.spec.run_id, *under),
                StopScope::Thread(thread) => thread_of(&run.spec) == Some(thread.as_str()),
                StopScope::Owner => true,
            })
            .cloned()
            .collect())
    }
}

impl ThreadStore for InMemoryStore {
    fn threads_of(
        &self,
        owner: &Owner,
        after: usize,
        limit: usize,
    ) -> Result<Vec<ThreadSummary>, StoreError> {
        let runs = read(&self.runs);
        let mut owned: Vec<&StoredRun> = runs
            .values()
            .filter(|run| run.spec.owner.is(owner) && thread_of(&run.spec).is_some())
            .collect();
        owned.sort_by_key(|run| (created_ms(run), run.spec.run_id.as_uuid()));
        // One pass, oldest first: a thread's first run sets it, and its first
        // run with no parent (its coordinator) names its agent and start.
        let mut threads: HashMap<&str, (ThreadSummary, bool)> = HashMap::new();
        for run in owned {
            let id = thread_of(&run.spec).expect("filtered");
            let root = run.spec.lineage.parent.is_none();
            let (thread, rooted) = threads.entry(id).or_insert((
                ThreadSummary {
                    thread_id: id.to_string(),
                    agent_id: run.spec.agent_id,
                    started_ms: created_ms(run),
                    runs: 0,
                },
                false,
            ));
            thread.runs += 1;
            if root && !*rooted {
                thread.agent_id = run.spec.agent_id;
                thread.started_ms = created_ms(run);
                *rooted = true;
            }
        }
        let mut threads: Vec<ThreadSummary> =
            threads.into_values().map(|(thread, _)| thread).collect();
        threads.sort_by(|a, b| {
            b.started_ms
                .cmp(&a.started_ms)
                .then_with(|| a.thread_id.cmp(&b.thread_id))
        });
        Ok(threads.into_iter().skip(after).take(limit).collect())
    }

    fn thread_root(&self, owner: &Owner, thread: &str) -> Result<Option<RunSpec>, StoreError> {
        Ok(read(&self.runs)
            .values()
            .filter(|run| {
                run.spec.owner.is(owner)
                    && thread_of(&run.spec) == Some(thread)
                    && run.spec.lineage.parent.is_none()
            })
            .min_by_key(|run| (created_ms(run), run.spec.run_id.as_uuid()))
            .map(|run| run.spec.clone()))
    }

    fn runs_of_thread(
        &self,
        owner: &Owner,
        thread: &str,
        limit: usize,
    ) -> Result<Vec<StoredRun>, StoreError> {
        let runs = read(&self.runs);
        let mut found: Vec<StoredRun> = runs
            .values()
            .filter(|run| run.spec.owner.is(owner) && thread_of(&run.spec) == Some(thread))
            .cloned()
            .collect();
        found.sort_by_key(|run| (created_ms(run), run.spec.run_id.as_uuid()));
        found.truncate(limit);
        Ok(found)
    }
}

impl OutboxStore for InMemoryStore {
    fn outbox_after(
        &self,
        owner: &Owner,
        after: u64,
        limit: usize,
    ) -> Result<OutboxPage, StoreError> {
        let principal = principal(owner);
        let (rows, pruned_through, last) = {
            let outbox = read(&self.outbox);
            let rows: Vec<(u64, RunId, u64)> = outbox
                .rows
                .iter()
                .filter(|row| row.principal == principal && row.seq > after)
                .take(limit)
                .map(|row| (row.seq, row.run_id, row.run_seq))
                .collect();
            (
                rows,
                outbox.pruned.get(&principal).copied().unwrap_or(0),
                outbox.counts.get(&principal).copied().unwrap_or(0),
            )
        };
        // Numbered under the runs lock with their events, so every row's
        // event is stored.
        let runs = read(&self.runs);
        let entries = rows
            .into_iter()
            .map(|(seq, run_id, run_seq)| {
                let event = runs
                    .get(&run_id)
                    .and_then(|run| run.events.get(run_seq as usize - 1))
                    .ok_or_else(|| StoreError::new(format!("outbox entry {seq} has no event")))?
                    .clone();
                Ok(OutboxEntry {
                    seq,
                    run_id,
                    run_seq,
                    event,
                })
            })
            .collect::<Result<_, StoreError>>()?;
        Ok(OutboxPage {
            entries,
            pruned_through,
            last,
        })
    }

    fn hints(&self) -> HintStream {
        self.hints.subscribe()
    }

    fn prune_outbox(&self, before: Timestamp) -> Result<u64, StoreError> {
        let mut outbox = write(&self.outbox, "outbox")?;
        let mut through: HashMap<(String, String), u64> = HashMap::new();
        for row in &outbox.rows {
            if row.stored_ms < before.as_unix_millis() {
                let last = through.entry(row.principal.clone()).or_insert(0);
                *last = (*last).max(row.seq);
            }
        }
        let kept = outbox.rows.len();
        outbox.rows.retain(|row| {
            through
                .get(&row.principal)
                .is_none_or(|through| row.seq > *through)
        });
        let pruned = (kept - outbox.rows.len()) as u64;
        for (principal, through) in through {
            let last = outbox.pruned.entry(principal).or_insert(0);
            *last = (*last).max(through);
        }
        Ok(pruned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::{
        AgentId, CredentialSource, ExecutionPlacement, Limits, ModelProvider, WorkModel,
    };

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

    // A second put of the same run stores nothing and says so; the first
    // spec and events stay.
    #[test]
    fn a_second_put_of_a_run_reports_it_existed() {
        let store = InMemoryStore::default();
        let spec = spec();
        let run = StoredRun {
            spec: spec.clone(),
            events: vec![],
        };
        assert_eq!(store.put_run(run.clone()), Ok(PutRun::Stored));
        let mut other = run;
        other.spec.input = "changed".to_string();
        assert_eq!(store.put_run(other), Ok(PutRun::Existed));
        assert_eq!(store.run(spec.run_id).unwrap().unwrap().spec, spec);
    }

    // Decision 38A across clock skew, in the in-memory store: an entry
    // stored with an earlier time than the one numbered before it takes that
    // one with it, so what stays is a suffix of the principal's numbers.
    #[test]
    fn pruning_takes_a_prefix_across_clock_skew() {
        let store = InMemoryStore::default();
        let spec = spec();
        let message = |text: &str| {
            Event::record(
                protocol::EventSource::for_spec(&spec, protocol::Actor::System, Timestamp::now()),
                EventPayload::UserMessage {
                    text: text.to_string(),
                },
            )
        };
        let run = StoredRun {
            spec: spec.clone(),
            events: vec![message("a"), message("b"), message("c")],
        };
        assert_eq!(store.put_run(run), Ok(PutRun::Stored));
        for row in &mut store.outbox.lock().unwrap().rows {
            row.stored_ms = if row.seq == 2 { 1_000 } else { 9_000 };
        }
        assert_eq!(store.prune_outbox(Timestamp::unix_millis(5_000)), Ok(2));
        let page = store.outbox_after(&spec.owner, 0, usize::MAX).unwrap();
        assert_eq!(page.pruned_through, 2);
        assert_eq!(
            page.entries
                .iter()
                .map(|entry| entry.seq)
                .collect::<Vec<_>>(),
            [3]
        );
    }

    // Owner decision 3A: a writer that panicked holding the lock leaves reads
    // working and refuses later writes as a StoreError.
    #[test]
    fn a_poisoned_lock_serves_reads_and_refuses_writes() {
        let store = InMemoryStore::default();
        let first = spec();
        let run_id = first.run_id;
        store
            .put_run(StoredRun {
                spec: first,
                events: Vec::new(),
            })
            .unwrap();
        let poisoner = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let _held = store.runs.lock().unwrap();
                    panic!("writer panicked holding the lock");
                })
                .join()
        });
        assert!(poisoner.is_err());
        assert!(store.runs.is_poisoned());

        assert_eq!(store.run(run_id).map(|run| run.is_some()), Ok(true));
        let refused = StoreError::new("run store lock poisoned");
        assert_eq!(
            store.put_run(StoredRun {
                spec: spec(),
                events: Vec::new(),
            }),
            Err(refused.clone())
        );
        assert_eq!(store.append_events(run_id, Vec::new()), Err(refused));
    }

    fn completed(spec: &RunSpec, outcome: &str) -> Event {
        Event::record(
            protocol::EventSource::new(
                spec.run_id,
                spec.agent_id,
                &spec.agent_version,
                protocol::Actor::System,
                protocol::Timestamp::now(),
            ),
            EventPayload::RunCompleted {
                outcome: outcome.to_string(),
            },
        )
    }

    // A batch ends at its terminal event: one after it is refused.
    #[test]
    fn an_event_after_a_terminal_in_one_batch_is_refused() {
        let store = InMemoryStore::default();
        let spec = spec();
        let message = Event::record(
            protocol::EventSource::new(
                spec.run_id,
                spec.agent_id,
                &spec.agent_version,
                protocol::Actor::System,
                protocol::Timestamp::now(),
            ),
            EventPayload::UserMessage {
                text: "late".to_string(),
            },
        );
        let batch = vec![completed(&spec, "done"), message];
        assert!(store
            .put_run(StoredRun {
                spec: spec.clone(),
                events: batch.clone(),
            })
            .is_err());
        store
            .put_run(StoredRun {
                spec: spec.clone(),
                events: Vec::new(),
            })
            .unwrap();
        assert!(store.append_events(spec.run_id, batch).is_err());
        assert_eq!(
            store
                .run(spec.run_id)
                .map(|run| run.map(|run| run.events.len())),
            Ok(Some(0))
        );
    }

    // A batch may hold one terminal event: the log ends at it.
    #[test]
    fn a_batch_with_two_terminal_events_is_refused() {
        let store = InMemoryStore::default();
        let spec = spec();
        let two = vec![completed(&spec, "a"), completed(&spec, "b")];
        assert!(store
            .put_run(StoredRun {
                spec: spec.clone(),
                events: two.clone(),
            })
            .is_err());
        assert!(store.run(spec.run_id).unwrap().is_none(), "nothing stored");
        store
            .put_run(StoredRun {
                spec: spec.clone(),
                events: Vec::new(),
            })
            .unwrap();
        assert!(store.append_events(spec.run_id, two).is_err());
        assert_eq!(
            store
                .run(spec.run_id)
                .map(|run| run.map(|run| run.events.len())),
            Ok(Some(0))
        );
    }
}
