use std::panic::{catch_unwind, AssertUnwindSafe};

use postgres::fallible_iterator::FallibleIterator;
use std::time::Duration;

use postgres::{IsolationLevel, NoTls};
use protocol::{AgentId, ArtifactId, Event, MessageId, Owner, RunId, RunSpec, Timestamp};
use serde_json::Value;

use harness::StoreError;

use crate::store::{
    check_one_terminal, created_ms, is_terminal, principal_hint_of, thread_of, Append, Hint,
    HintStream, Hints, MessageStore, OutboxEntry, OutboxPage, OutboxStore, PutAgent, PutMessage,
    PutRun, RunStore, StopScope, StopStore, StoredAgent, StoredArtifact, StoredMessage, StoredRun,
    StoredTrigger, ThreadStore, ThreadSummary, TriggerId, TriggerStore, ALL,
};

/// The run store in Postgres over a pool of connections (C2, owner decision
/// 1A). The pool checks a connection before lending it, so one whose backend
/// died is replaced before a call uses it. A call that fails mid-statement
/// reports a StoreError and is not retried, since a write may have committed
/// (C1 decision 2A).
pub struct PostgresStore {
    pool: r2d2::Pool<Connections>,
    /// The listener's own connection, outside the pool (decision 40A).
    listen: postgres::Config,
    hints: Hints,
    /// Whether the listener thread was started.
    listening: std::sync::Mutex<bool>,
}

/// The channel a principal's hint is notified on, after each append commits
/// (decision 46A).
const CHANNEL: &str = "gol_outbox";

/// How long the listener waits for a notification before it checks that its
/// connection is still alive.
const LISTEN_CHECK: Duration = Duration::from_secs(30);

/// Listens for `CHANNEL` on its own connection and forwards each hint, for
/// as long as the process runs. While it is connected, `live` is true. After
/// a failure (a dead connection, found within `LISTEN_CHECK`, or a panic) it
/// reconnects after a second and wakes every stream, since hints may have
/// been lost while it was down.
fn listen(
    config: postgres::Config,
    hints: tokio::sync::broadcast::Sender<Hint>,
    live: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    use std::sync::atomic::Ordering;
    loop {
        let listened = catch_unwind(AssertUnwindSafe(|| -> Result<(), String> {
            let mut client = config.connect(NoTls).map_err(|error| error.to_string())?;
            client
                .batch_execute(&format!("listen {CHANNEL}"))
                .map_err(|error| error.to_string())?;
            live.store(true, Ordering::Relaxed);
            let _ = hints.send(ALL);
            loop {
                {
                    let mut notifications = client.notifications();
                    let mut waiting = notifications.timeout_iter(LISTEN_CHECK);
                    while let Some(notification) =
                        waiting.next().map_err(|error| error.to_string())?
                    {
                        if let Ok(hint) = notification.payload().parse::<Hint>() {
                            let _ = hints.send(hint);
                        }
                    }
                }
                // Quiet for LISTEN_CHECK: a half-open connection would stay
                // quiet forever, so ask the server.
                if !client.is_valid(Duration::from_secs(5)).is_ok() {
                    return Err("the connection is gone".to_string());
                }
            }
        }));
        live.store(false, Ordering::Relaxed);
        match listened {
            Ok(Err(error)) => eprintln!("gol: outbox listener: {error}"),
            Ok(Ok(())) => {}
            Err(_) => eprintln!("gol: outbox listener: panicked"),
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Opens and checks the pool's connections. A connect that panics (the
/// postgres crate unwraps building its runtime, which fails when the process
/// is out of file descriptors) is an error here: r2d2 counts a pending connect
/// until its job returns, so a panic would leak that slot for good.
struct Connections {
    config: postgres::Config,
}

impl r2d2::ManageConnection for Connections {
    type Connection = postgres::Client;
    type Error = StoreError;

    fn connect(&self) -> Result<postgres::Client, StoreError> {
        catch_unwind(AssertUnwindSafe(|| self.config.connect(NoTls)))
            .map_err(|_| StoreError::new("postgres connect panicked"))?
            .map_err(sql)
    }

    fn is_valid(&self, client: &mut postgres::Client) -> Result<(), StoreError> {
        client.simple_query("").map(|_| ()).map_err(sql)
    }

    fn has_broken(&self, client: &mut postgres::Client) -> bool {
        client.is_closed()
    }
}

/// How a store's pool of connections is sized.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolOptions {
    /// Connections open at most, at least one.
    pub max_size: u32,
    /// Idle connections the pool keeps open ahead of need, at most
    /// `max_size`. With none, a caller waiting on a full pool is not woken
    /// when a busy connection comes back broken, and waits out the timeout.
    pub min_idle: u32,
}

impl Default for PoolOptions {
    /// Eight connections, none kept idle.
    fn default() -> Self {
        Self {
            max_size: 8,
            min_idle: 0,
        }
    }
}

const SCHEMA: &str = "
create table if not exists agents (
    id uuid primary key,
    manifest jsonb not null,
    owner_issuer text not null,
    owner_subject text not null,
    owner_tenant text not null
);
-- The store's order (Phase 3.4): each run insert and each stop takes the
-- next number, so whether a stop came after a run does not rest on any
-- server's clock.
create sequence if not exists gol_order;
create table if not exists runs (
    id uuid primary key,
    spec jsonb not null,
    -- From the spec, written once by put_run (Phase 3.3, decision 48A).
    owner_issuer text,
    owner_subject text,
    thread_id text,
    parent_run uuid,
    created_ms bigint,
    stored_seq bigint default nextval('gol_order')
);
create table if not exists run_events (
    run_id uuid not null references runs (id),
    seq bigint not null,
    body jsonb not null,
    terminal boolean not null,
    primary key (run_id, seq)
);

-- The per-owner outbox (Phase 3.1): each principal's count, and a row per
-- stored event, numbered in the transaction that stores it, after the run
-- row's lock (decision 16A, formal/outbox). A row points at its event.
create table if not exists outbox_counters (
    owner_issuer text not null,
    owner_subject text not null,
    last bigint not null,
    -- The principal's numbers up to this one were pruned (decision 38A).
    pruned bigint not null default 0,
    primary key (owner_issuer, owner_subject)
);
create table if not exists outbox (
    owner_issuer text not null,
    owner_subject text not null,
    seq bigint not null,
    run_id uuid not null,
    run_seq bigint not null,
    stored_ms bigint not null,
    primary key (owner_issuer, owner_subject, seq)
);

-- Stop requests (Phase 3.4): a principal's stop of a run, a thread, or all
-- of its runs, and when it was made.
-- The latest stop of each scope: a later stop of the same scope covers
-- everything an earlier one did.
create table if not exists stops (
    owner_issuer text not null,
    owner_subject text not null,
    kind text not null,
    key text not null,
    requested_seq bigint not null default nextval('gol_order'),
    primary key (owner_issuer, owner_subject, kind, key)
);

-- A principal's triggers (Phase 4.1): the trigger as JSON, with its owner,
-- whether it runs, and when the scheduler fires it next (Phase 4.2) as
-- columns, which the stored JSON's copies of them never override.
create table if not exists triggers (
    id uuid primary key,
    owner_issuer text not null,
    owner_subject text not null,
    body jsonb not null,
    enabled boolean not null,
    next_fire_ms bigint,
    created_ms bigint not null
);

create table if not exists messages (
    id uuid primary key,
    from_run uuid not null,
    decision bigint not null,
    task_run uuid,
    expects_reply boolean not null,
    deadline_ms bigint,
    answered boolean not null default false,
    body jsonb not null,
    unique (from_run, decision)
);

create table if not exists artifacts (
    id uuid primary key,
    run_id uuid not null,
    name text not null,
    body bytea not null
);
";

/// Indexes created once, each as (table, name, statement): an existing one
/// is found in the catalog, not by `create index if not exists`.
const INDEXES: [(&str, &str, &str); 9] = [
    // A stop check reads a principal's stops, and walks runs by parent.
    (
        "stops",
        "stops_by_owner",
        "create index stops_by_owner on stops (owner_issuer, owner_subject)",
    ),
    (
        "runs",
        "runs_by_parent",
        "create index runs_by_parent on runs (parent_run)",
    ),
    // The running triggers by their next tick, for the scheduler.
    (
        "triggers",
        "triggers_due",
        "create index triggers_due on triggers (next_fire_ms) where enabled",
    ),
    // A principal's triggers, oldest first.
    (
        "triggers",
        "triggers_by_owner",
        "create index triggers_by_owner on triggers (owner_issuer, owner_subject, created_ms)",
    ),
    // A principal's threads, and a thread's runs in order.
    (
        "runs",
        "runs_by_thread",
        "create index runs_by_thread on runs (owner_issuer, owner_subject, thread_id, created_ms)",
    ),
    (
        "run_events",
        "run_events_one_terminal",
        "create unique index run_events_one_terminal on run_events (run_id) where terminal",
    ),
    // The pruner reads each principal's oldest entries.
    (
        "outbox",
        "outbox_by_stored",
        "create index outbox_by_stored on outbox (stored_ms)",
    ),
    // The open asks, by the task that answers them and by their deadline: a
    // worker reads the first for every run it ends, the ask sweep the second.
    (
        "messages",
        "messages_open_by_task",
        "create index messages_open_by_task on messages (task_run)
             where expects_reply and not answered",
    ),
    (
        "messages",
        "messages_open_by_deadline",
        "create index messages_open_by_deadline on messages (deadline_ms)
             where expects_reply and not answered",
    ),
];

fn ensure_schema(client: &mut postgres::Client) -> Result<(), StoreError> {
    let mut tx = client.transaction().map_err(sql)?;
    tx.query_one("select pg_advisory_xact_lock(872346)", &[])
        .map_err(sql)?;
    tx.batch_execute(SCHEMA).map_err(sql)?;
    // A runs table from before Phase 3.3 has no thread columns (decision
    // 49A): they are added and filled from each run's spec, once. Looked up
    // in the catalog first, since `alter table` locks the table even when
    // there is nothing to do.
    let threaded = tx
        .query_opt(
            "select 1 from pg_attribute
             where attrelid = 'runs'::regclass and attname = 'thread_id' and not attisdropped",
            &[],
        )
        .map_err(sql)?;
    if threaded.is_none() {
        // The table is locked for the alter: give up rather than wait long
        // behind a writer, and let the next connect try again.
        tx.batch_execute(
            "set local lock_timeout = '10s';
             alter table runs
                 add column owner_issuer text,
                 add column owner_subject text,
                 add column thread_id text,
                 add column parent_run uuid,
                 add column created_ms bigint;",
        )
        .map_err(sql)?;
    }
    // A runs table from before Phase 3.4 has no place in the store's order:
    // each run gets one (in no particular order, all before any stop).
    let ordered = tx
        .query_opt(
            "select 1 from pg_attribute
             where attrelid = 'runs'::regclass and attname = 'stored_seq' and not attisdropped",
            &[],
        )
        .map_err(sql)?;
    if ordered.is_none() {
        tx.batch_execute(
            "set local lock_timeout = '10s';
             alter table runs add column stored_seq bigint default nextval('gol_order');",
        )
        .map_err(sql)?;
    }
    // Every run without its columns: all of them just after the alter, and
    // any an older server stored since (a rolling deploy). A run with no
    // first event is dated now.
    tx.batch_execute(
        "update runs set
             owner_issuer = spec->'owner'->>'issuer',
             owner_subject = spec->'owner'->>'subject',
             thread_id = nullif(spec->'metadata'->>'session_id', ''),
             parent_run = (spec->'lineage'->>'parent')::uuid,
             created_ms = coalesce(
                 (select (body->'envelope'->>'at')::bigint from run_events
                  where run_events.run_id = runs.id and run_events.seq = 1),
                 (extract(epoch from clock_timestamp()) * 1000)::bigint)
         where owner_issuer is null;",
    )
    .map_err(sql)?;
    // `create index if not exists` locks the table even when the index is
    // there, and would wait behind any writer stalled mid-append.
    for (table, name, create) in INDEXES {
        // Looked up on the table itself, not by name on the search path.
        let index = tx
            .query_opt(
                "select 1 from pg_index join pg_class on pg_class.oid = pg_index.indexrelid
                 where pg_index.indrelid = $1::text::regclass and pg_class.relname = $2",
                &[&table, &name],
            )
            .map_err(sql)?;
        if index.is_none() {
            tx.batch_execute(create).map_err(sql)?;
        }
    }
    // `create table if not exists` leaves a table from an older schema as it
    // was. Fail here, at connect, rather than on the first write; dropping
    // the transaction rolls back anything created above.
    tx.batch_execute("select owner_issuer, owner_subject, owner_tenant from agents limit 0")
        .map_err(sql)?;
    tx.batch_execute("select pruned from outbox_counters limit 0")
        .map_err(sql)?;
    tx.batch_execute("select requested_seq from stops limit 0")
        .map_err(sql)?;
    tx.batch_execute("select enabled, next_fire_ms from triggers limit 0")
        .map_err(sql)?;
    // Resolved like the queries resolve `runs`, whatever the role may see.
    let events_column = tx
        .query_opt(
            "select 1 from pg_attribute
             where attrelid = to_regclass('runs') and attname = 'events' and not attisdropped",
            &[],
        )
        .map_err(sql)?;
    if events_column.is_some() {
        return Err(StoreError::new(
            "runs keeps its events in one jsonb column, from before per-row events: \
             drop tables runs and run_events",
        ));
    }
    tx.commit().map_err(sql)
}

impl PostgresStore {
    /// A store with a pool of at most eight connections.
    ///
    /// The store blocks: call it from a blocking thread, never from an async
    /// task (the postgres client panics inside a Tokio runtime).
    pub fn connect(url: &str) -> Result<Self, StoreError> {
        Self::connect_with(url, PoolOptions::default())
    }

    /// A store with a pool of at most `size` connections, at least one. The
    /// first connection checks the schema, so a database the store cannot use
    /// fails here.
    pub fn connect_with_pool_size(url: &str, size: u32) -> Result<Self, StoreError> {
        Self::connect_with(
            url,
            PoolOptions {
                max_size: size,
                ..PoolOptions::default()
            },
        )
    }

    /// A store with a pool sized by `options`.
    pub fn connect_with(url: &str, options: PoolOptions) -> Result<Self, StoreError> {
        let size = options.max_size;
        if size == 0 {
            return Err(StoreError::new(
                "a postgres pool needs at least one connection",
            ));
        }
        let mut config: postgres::Config = url.parse().map_err(sql)?;
        // Zero means "wait indefinitely" to libpq; the parser drops it today,
        // and r2d2 refuses a zero wait by panicking, so zero counts as unset.
        let timeout = config
            .get_connect_timeout()
            .copied()
            .filter(|timeout| !timeout.is_zero())
            .unwrap_or(CONNECT_TIMEOUT);
        config.connect_timeout(timeout);
        // A caller waits for a free connection at most this long, whatever
        // the URL's connect_timeout: requests should not queue for a day.
        let wait = timeout.min(MAX_POOL_WAIT);
        let pool = r2d2::Pool::builder()
            .max_size(size)
            .min_idle(Some(options.min_idle.min(size)))
            .test_on_check_out(true)
            .connection_timeout(wait)
            .build_unchecked(Connections {
                config: config.clone(),
            });
        let store = Self {
            pool,
            listen: config,
            hints: Hints::listened(),
            listening: std::sync::Mutex::new(false),
        };
        store.with_client(ensure_schema)?;
        Ok(store)
    }

    /// Tells the streams that `principal` has new entries, after the append
    /// committed (decision 46A): this process's own at once, and every other
    /// server by a NOTIFY in a statement of its own, so the append's commit
    /// never takes Postgres's notify lock. A failure costs only speed, not an
    /// event: streams also poll.
    fn notify(&self, client: &mut postgres::Client, principal: &(String, String)) {
        let hint = principal_hint_of(&principal.0, &principal.1);
        self.hints.send(hint);
        if let Err(error) =
            client.execute("select pg_notify($1, $2)", &[&CHANNEL, &hint.to_string()])
        {
            eprintln!("gol: outbox notify: {error}");
        }
    }

    /// Runs `op` on a pooled connection. The pool tests a connection before
    /// lending it and discards one whose backend is gone.
    fn with_client<T>(
        &self,
        op: impl FnOnce(&mut postgres::Client) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let mut client = self
            .pool
            .get()
            .map_err(|error| StoreError::new(format!("postgres pool: {error}")))?;
        op(&mut client)
    }
}

/// How long a connect may take, and a caller may wait for a pooled
/// connection, when the URL sets no `connect_timeout`.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The longest a caller waits for a pooled connection.
const MAX_POOL_WAIT: Duration = Duration::from_secs(30);

/// The full error for stderr: `postgres::Error` displays only its kind.
fn sql(error: postgres::Error) -> StoreError {
    let detail = match (error.as_db_error(), std::error::Error::source(&error)) {
        (Some(db), _) => format!(
            "{error}: {} {}: {}",
            db.severity(),
            db.code().code(),
            db.message()
        ),
        (None, Some(source)) => format!("{error}: {source}"),
        (None, None) => error.to_string(),
    };
    StoreError::new(detail)
}

fn json(error: serde_json::Error) -> StoreError {
    StoreError::new(format!("json: {error}"))
}

impl PostgresStore {
    /// `append_events`, and with `seen` also refused as `Moved` unless the
    /// log is `seen` events long: all under the run row's lock.
    fn append_checked(
        &self,
        id: RunId,
        seen: Option<usize>,
        events: Vec<Event>,
    ) -> Result<Append, StoreError> {
        let rows = EventRows::new(&events)?;
        let id = id.as_uuid();
        self.with_client(|client| {
            // Read committed whatever the session default: each statement
            // after the row lock sees what the lock's last holder committed.
            let mut tx = read_committed(client)?;
            let locked = tx
                .query_opt(
                    // An older server's row has no owner columns yet.
                    "select coalesce(owner_issuer, spec->'owner'->>'issuer'),
                            coalesce(owner_subject, spec->'owner'->>'subject')
                     from runs where id = $1 for update",
                    &[&id],
                )
                .map_err(sql)?;
            let Some(locked) = locked else {
                return Ok(Append::Missing);
            };
            let principal: (String, String) = (
                locked.try_get(0).map_err(sql)?,
                locked.try_get(1).map_err(sql)?,
            );
            // Two index lookups: the partial index answers the first, the
            // primary key the second.
            let ended = tx
                .query_opt(
                    "select 1 from run_events where run_id = $1 and terminal",
                    &[&id],
                )
                .map_err(sql)?;
            if ended.is_some() {
                return Ok(Append::Terminal);
            }
            let last = tx
                .query_opt(
                    "select seq from run_events where run_id = $1 order by seq desc limit 1",
                    &[&id],
                )
                .map_err(sql)?;
            let last: i64 = match last {
                Some(row) => row.try_get(0).map_err(sql)?,
                None => 0,
            };
            // Rows are numbered from 1 with no gaps, so the last is the length.
            if seen.is_some_and(|seen| i64::try_from(seen) != Ok(last)) {
                return Ok(Append::Moved);
            }
            rows.insert(&mut tx, id, last, &principal)?;
            tx.commit().map_err(sql)?;
            self.notify(client, &principal);
            Ok(Append::Appended)
        })
    }
}

impl ThreadStore for PostgresStore {
    fn threads_of(
        &self,
        owner: &Owner,
        after: usize,
        limit: usize,
    ) -> Result<Vec<ThreadSummary>, StoreError> {
        let after = i64::try_from(after).unwrap_or(i64::MAX);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = self.with_client(|client| {
            client
                .query(
                    "select thread_id, count(*),
                         coalesce(
                             (array_agg(spec->>'agent_id' order by created_ms, id)
                                 filter (where parent_run is null))[1],
                             (array_agg(spec->>'agent_id' order by created_ms, id))[1]),
                         coalesce(min(created_ms) filter (where parent_run is null),
                                  min(created_ms)) as started
                     from runs
                     where owner_issuer = $1 and owner_subject = $2 and thread_id is not null
                     group by thread_id
                     order by started desc, thread_id
                     offset $3 limit $4",
                    &[&owner.issuer, &owner.subject, &after, &limit],
                )
                .map_err(sql)
        })?;
        rows.iter()
            .map(|row| {
                let agent: String = row.try_get(2).map_err(sql)?;
                let runs: i64 = row.try_get(1).map_err(sql)?;
                Ok(ThreadSummary {
                    thread_id: row.try_get(0).map_err(sql)?,
                    agent_id: agent
                        .parse()
                        .map_err(|_| StoreError::new("a run's agent_id is not an id"))?,
                    started_ms: row.try_get(3).map_err(sql)?,
                    runs: u64::try_from(runs).unwrap_or(0),
                })
            })
            .collect()
    }

    /// One snapshot: the thread's runs, oldest first, then their events.
    fn runs_of_thread(
        &self,
        owner: &Owner,
        thread: &str,
        limit: usize,
    ) -> Result<Vec<StoredRun>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let (specs, events) = self.with_client(|client| {
            let mut tx = client
                .build_transaction()
                .read_only(true)
                .isolation_level(IsolationLevel::RepeatableRead)
                .start()
                .map_err(sql)?;
            let specs = tx
                .query(
                    "select id, spec from runs
                     where owner_issuer = $1 and owner_subject = $2 and thread_id = $3
                     order by created_ms, stored_seq, id
                     limit $4",
                    &[&owner.issuer, &owner.subject, &thread, &limit],
                )
                .map_err(sql)?;
            let ids: Vec<uuid::Uuid> = specs
                .iter()
                .map(|row| row.try_get(0))
                .collect::<Result<_, _>>()
                .map_err(sql)?;
            let events = tx
                .query(
                    "select run_id, body from run_events
                     where run_id = any($1)
                     order by run_id, seq",
                    &[&ids],
                )
                .map_err(sql)?;
            tx.commit().map_err(sql)?;
            Ok((specs, events))
        })?;
        runs_with_events(&specs, &events)
    }

    fn thread_root(&self, owner: &Owner, thread: &str) -> Result<Option<RunSpec>, StoreError> {
        let row = self.with_client(|client| {
            client
                .query_opt(
                    "select spec from runs
                     where owner_issuer = $1 and owner_subject = $2 and thread_id = $3
                       and parent_run is null
                     order by created_ms, id
                     limit 1",
                    &[&owner.issuer, &owner.subject, &thread],
                )
                .map_err(sql)
        })?;
        row.map(|row| {
            let spec: serde_json::Value = row.try_get(0).map_err(sql)?;
            serde_json::from_value(spec).map_err(json)
        })
        .transpose()
    }
}

impl StopStore for PostgresStore {
    /// One statement: the stop, numbered in the store's order, replacing an
    /// earlier stop of the same scope.
    fn put_stop(&self, owner: &Owner, scope: &StopScope) -> Result<(), StoreError> {
        let (kind, key) = scope.kind_and_key();
        self.with_client(|client| {
            client
                .execute(
                    "insert into stops (owner_issuer, owner_subject, kind, key)
                     values ($1, $2, $3, $4)
                     on conflict (owner_issuer, owner_subject, kind, key)
                     do update set requested_seq = nextval('gol_order')",
                    &[&owner.issuer, &owner.subject, &kind, &key],
                )
                .map(|_| ())
                .map_err(sql)
        })
    }

    /// One statement: the run and each run above it, up `parent_run` (or the
    /// spec's parent, for a row an older server stored), each against its
    /// principal's stops.
    fn stopped(&self, run: RunId) -> Result<bool, StoreError> {
        let run = run.as_uuid();
        self.with_client(|client| {
            client
                .query_one(
                    "with recursive chain as (
                         select id, coalesce(thread_id, nullif(spec->'metadata'->>'session_id', ''))
                                    as thread_id,
                                stored_seq,
                                coalesce(parent_run, (spec->'lineage'->>'parent')::uuid) as parent,
                                coalesce(owner_issuer, spec->'owner'->>'issuer') as issuer,
                                coalesce(owner_subject, spec->'owner'->>'subject') as subject
                         from runs where id = $1
                         union all
                         select runs.id,
                                coalesce(runs.thread_id,
                                         nullif(runs.spec->'metadata'->>'session_id', '')),
                                runs.stored_seq,
                                coalesce(runs.parent_run, (runs.spec->'lineage'->>'parent')::uuid),
                                coalesce(runs.owner_issuer, runs.spec->'owner'->>'issuer'),
                                coalesce(runs.owner_subject, runs.spec->'owner'->>'subject')
                         from runs join chain on runs.id = chain.parent
                     )
                     select exists (
                         select 1 from chain join stops
                           on stops.owner_issuer = chain.issuer
                          and stops.owner_subject = chain.subject
                         where (stops.kind = 'run' and stops.key = chain.id::text)
                            or (stops.requested_seq > coalesce(chain.stored_seq, 0)
                                and ((stops.kind = 'thread' and stops.key = chain.thread_id)
                                     or stops.kind = 'owner')))",
                    &[&run],
                )
                .and_then(|row| row.try_get(0))
                .map_err(sql)
        })
    }

    /// One snapshot: the covered runs that have not ended, one query per
    /// scope, then their events.
    fn open_runs_under(
        &self,
        owner: &Owner,
        scope: &StopScope,
    ) -> Result<Vec<StoredRun>, StoreError> {
        let (specs, events) = self.with_client(|client| {
            let mut tx = client
                .build_transaction()
                .read_only(true)
                .isolation_level(IsolationLevel::RepeatableRead)
                .start()
                .map_err(sql)?;
            let open = "not exists (
                            select 1 from run_events
                            where run_events.run_id = runs.id and run_events.terminal)";
            // The runs an older server stored without their columns, until
            // the next connect fills them (51A), read by their spec. Each
            // query reads the filled rows and these apart (`union all`; no
            // row is both), so the filled half keeps its index.
            let unfilled = "owner_issuer is null
                            and spec->'owner'->>'issuer' = $1
                            and spec->'owner'->>'subject' = $2";
            let mine = format!("(owner_issuer = $1 and owner_subject = $2 or {unfilled})");
            let specs = match scope {
                StopScope::Run(run) => tx.query(
                    &format!(
                        "with recursive tree as (
                             select id from runs where id = $3 and {mine}
                             union all
                             select runs.id from runs join tree
                               on runs.parent_run = tree.id
                               or runs.owner_issuer is null
                                  and runs.spec->'lineage'->>'parent' = tree.id::text
                         )
                         select runs.id, runs.spec from runs join tree on runs.id = tree.id
                         where {mine} and {open}"
                    ),
                    &[&owner.issuer, &owner.subject, &run.as_uuid()],
                ),
                StopScope::Thread(thread) => tx.query(
                    &format!(
                        "select id, spec from runs
                         where owner_issuer = $1 and owner_subject = $2 and thread_id = $3
                           and {open}
                         union all
                         select id, spec from runs
                         where {unfilled} and spec->'metadata'->>'session_id' = $3
                           and {open}"
                    ),
                    &[&owner.issuer, &owner.subject, thread],
                ),
                StopScope::Owner => tx.query(
                    &format!(
                        "select id, spec from runs
                         where owner_issuer = $1 and owner_subject = $2 and {open}
                         union all
                         select id, spec from runs where {unfilled} and {open}"
                    ),
                    &[&owner.issuer, &owner.subject],
                ),
            }
            .map_err(sql)?;
            let ids: Vec<uuid::Uuid> = specs
                .iter()
                .map(|row| row.try_get(0))
                .collect::<Result<_, _>>()
                .map_err(sql)?;
            let events = tx
                .query(
                    "select run_id, body from run_events where run_id = any($1) order by run_id, seq",
                    &[&ids],
                )
                .map_err(sql)?;
            tx.commit().map_err(sql)?;
            Ok((specs, events))
        })?;
        runs_with_events(&specs, &events)
    }
}

/// Stored runs from `specs` rows (id, spec), in their order, with their
/// events from `events` rows (run_id, body), in each run's order.
fn runs_with_events(
    specs: &[postgres::Row],
    events: &[postgres::Row],
) -> Result<Vec<StoredRun>, StoreError> {
    let mut by_run: std::collections::HashMap<uuid::Uuid, Vec<Event>> =
        std::collections::HashMap::new();
    for row in events {
        let id: uuid::Uuid = row.try_get(0).map_err(sql)?;
        let body: serde_json::Value = row.try_get(1).map_err(sql)?;
        by_run
            .entry(id)
            .or_default()
            .push(serde_json::from_value(body).map_err(json)?);
    }
    specs
        .iter()
        .map(|row| {
            let id: uuid::Uuid = row.try_get(0).map_err(sql)?;
            let spec: serde_json::Value = row.try_get(1).map_err(sql)?;
            Ok(StoredRun {
                spec: serde_json::from_value(spec).map_err(json)?,
                events: by_run.remove(&id).unwrap_or_default(),
            })
        })
        .collect()
}

impl OutboxStore for PostgresStore {
    /// One snapshot: the outbox rows joined to their events, and how far the
    /// principal's entries were pruned. An owner's numbers commit in order,
    /// so a read never sees a later number before an earlier one.
    fn outbox_after(
        &self,
        owner: &Owner,
        after: u64,
        limit: usize,
    ) -> Result<OutboxPage, StoreError> {
        let after = i64::try_from(after).unwrap_or(i64::MAX);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let (pruned, last, rows) = self.with_client(|client| {
            let mut tx = client
                .build_transaction()
                .read_only(true)
                .isolation_level(IsolationLevel::RepeatableRead)
                .start()
                .map_err(sql)?;
            let (pruned, last): (i64, i64) = tx
                .query_opt(
                    "select pruned, last from outbox_counters
                     where owner_issuer = $1 and owner_subject = $2",
                    &[&owner.issuer, &owner.subject],
                )
                .map_err(sql)?
                .map(|row| Ok::<_, postgres::Error>((row.try_get(0)?, row.try_get(1)?)))
                .transpose()
                .map_err(sql)?
                .unwrap_or((0, 0));
            let rows = tx
                .query(
                    "select outbox.seq, outbox.run_id, outbox.run_seq, run_events.body
                     from outbox left join run_events
                       on run_events.run_id = outbox.run_id and run_events.seq = outbox.run_seq
                     where outbox.owner_issuer = $1 and outbox.owner_subject = $2
                       and outbox.seq > $3
                     order by outbox.seq
                     limit $4",
                    &[&owner.issuer, &owner.subject, &after, &limit],
                )
                .map_err(sql)?;
            tx.commit().map_err(sql)?;
            Ok((pruned, last, rows))
        })?;
        let entries = rows
            .iter()
            .map(|row| {
                let seq: i64 = row.try_get(0).map_err(sql)?;
                let run_id: uuid::Uuid = row.try_get(1).map_err(sql)?;
                let run_seq: i64 = row.try_get(2).map_err(sql)?;
                // Written with its event, so a missing one is corruption,
                // not a gap to pass over.
                let body: Option<serde_json::Value> = row.try_get(3).map_err(sql)?;
                let body = body
                    .ok_or_else(|| StoreError::new(format!("outbox entry {seq} has no event")))?;
                Ok(OutboxEntry {
                    seq: u64::try_from(seq).map_err(|_| StoreError::new("negative outbox seq"))?,
                    run_id: RunId::from_uuid(run_id),
                    run_seq: u64::try_from(run_seq)
                        .map_err(|_| StoreError::new("negative run seq"))?,
                    event: serde_json::from_value(body).map_err(json)?,
                })
            })
            .collect::<Result<_, StoreError>>()?;
        Ok(OutboxPage {
            entries,
            pruned_through: u64::try_from(pruned).unwrap_or(0),
            last: u64::try_from(last).unwrap_or(0),
        })
    }

    fn hints(&self) -> HintStream {
        let stream = self.hints.subscribe();
        let mut started = self
            .listening
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !*started {
            let (config, hints, live) =
                (self.listen.clone(), self.hints.sender(), self.hints.live());
            match std::thread::Builder::new()
                .name("gol-outbox-listener".to_string())
                .spawn(move || listen(config, hints, live))
            {
                Ok(_) => *started = true,
                // The next stream tries again; this one polls.
                Err(error) => eprintln!("gol: outbox listener: {error}"),
            }
        }
        stream
    }

    /// One statement: for each principal, the last number stored before
    /// `before`; its entries up to it deleted, and its counter row marked
    /// pruned through it (taking only that row's lock, never a run row's).
    fn prune_outbox(&self, before: Timestamp) -> Result<u64, StoreError> {
        let deleted: i64 = self.with_client(|client| {
            client
                .query_one(
                    "with cut as (
                         select owner_issuer, owner_subject, max(seq) as through
                         from outbox where stored_ms < $1
                         group by owner_issuer, owner_subject
                     ), gone as (
                         delete from outbox using cut
                         where outbox.owner_issuer = cut.owner_issuer
                           and outbox.owner_subject = cut.owner_subject
                           and outbox.seq <= cut.through
                         returning 1
                     ), marked as (
                         update outbox_counters set pruned = greatest(pruned, cut.through)
                         from cut
                         where outbox_counters.owner_issuer = cut.owner_issuer
                           and outbox_counters.owner_subject = cut.owner_subject
                         returning 1
                     )
                     select count(*) from gone",
                    &[&before.as_unix_millis()],
                )
                .map_err(sql)?
                .try_get(0)
                .map_err(sql)
        })?;
        Ok(u64::try_from(deleted).unwrap_or(0))
    }
}

/// Messages are rows keyed by id and unique per sending run and decision;
/// the message itself is the `body` column (Phase 2.1).
impl MessageStore for PostgresStore {
    fn put_message(&self, message: StoredMessage) -> Result<PutMessage, StoreError> {
        let body = serde_json::to_value(&message).map_err(json)?;
        let decision = i64::from(message.decision);
        let from_run = message.from_run.as_uuid();
        self.with_client(|client| {
            let inserted = client
                .execute(
                    "insert into messages (id, from_run, decision, task_run, expects_reply, deadline_ms, body)
                     values ($1, $2, $3, $4, $5, $6, $7)
                     on conflict (from_run, decision) do nothing",
                    &[
                        &message.id.as_uuid(),
                        &from_run,
                        &decision,
                        &message.task_run.map(RunId::as_uuid),
                        &message.expects_reply,
                        &message.deadline.map(Timestamp::as_unix_millis),
                        &body,
                    ],
                )
                .map_err(sql)?;
            if inserted == 1 {
                return Ok(PutMessage::Stored);
            }
            let row = client
                .query_one(
                    "select body from messages where from_run = $1 and decision = $2",
                    &[&from_run, &decision],
                )
                .map_err(sql)?;
            Ok(PutMessage::Existed(Box::new(message_of(&row)?)))
        })
    }

    fn message(&self, id: MessageId) -> Result<Option<StoredMessage>, StoreError> {
        self.with_client(|client| {
            client
                .query_opt("select body from messages where id = $1", &[&id.as_uuid()])
                .map_err(sql)?
                .map(|row| message_of(&row))
                .transpose()
        })
    }

    fn ask_of_task(&self, task_run: RunId) -> Result<Option<StoredMessage>, StoreError> {
        self.with_client(|client| {
            client
                .query_opt(
                    "select body from messages where task_run = $1 and expects_reply and not answered",
                    &[&task_run.as_uuid()],
                )
                .map_err(sql)?
                .map(|row| message_of(&row))
                .transpose()
        })
    }

    fn answer(&self, ask: MessageId) -> Result<bool, StoreError> {
        self.with_client(|client| {
            let changed = client
                .execute(
                    "update messages set answered = true where id = $1 and not answered",
                    &[&ask.as_uuid()],
                )
                .map_err(sql)?;
            Ok(changed == 1)
        })
    }

    fn open_asks_due(&self, now: Timestamp) -> Result<Vec<StoredMessage>, StoreError> {
        self.with_client(|client| {
            client
                .query(
                    "select body from messages
                     where not answered and expects_reply and deadline_ms <= $1
                     order by id",
                    &[&now.as_unix_millis()],
                )
                .map_err(sql)?
                .iter()
                .map(message_of)
                .collect()
        })
    }
}

/// A `messages` row read as `body`.
fn message_of(row: &postgres::Row) -> Result<StoredMessage, StoreError> {
    serde_json::from_value(row.try_get::<_, Value>(0).map_err(sql)?).map_err(json)
}

/// An `agents` row read as `manifest, owner_issuer, owner_subject, owner_tenant`.
fn stored_agent(row: &postgres::Row) -> Result<StoredAgent, StoreError> {
    Ok(StoredAgent {
        manifest: serde_json::from_value(row.try_get::<_, Value>(0).map_err(sql)?).map_err(json)?,
        owner: Owner::new(
            row.try_get::<_, String>(1).map_err(sql)?,
            row.try_get::<_, String>(2).map_err(sql)?,
            row.try_get::<_, String>(3).map_err(sql)?,
        ),
    })
}

impl RunStore for PostgresStore {
    /// One statement: insert, or replace the row only when the same
    /// principal (issuer and subject) owns it. No row changed means another
    /// principal owns the id.
    fn put_agent(&self, agent: StoredAgent) -> Result<PutAgent, StoreError> {
        let manifest = serde_json::to_value(&agent.manifest).map_err(json)?;
        let changed = self.with_client(|client| {
            client
                .execute(
                    "insert into agents (id, manifest, owner_issuer, owner_subject, owner_tenant)
                 values ($1, $2, $3, $4, $5)
                 on conflict (id) do update
                 set manifest = excluded.manifest, owner_tenant = excluded.owner_tenant
                 where agents.owner_issuer = excluded.owner_issuer
                   and agents.owner_subject = excluded.owner_subject",
                    &[
                        &agent.manifest.id.as_uuid(),
                        &manifest,
                        &agent.owner.issuer,
                        &agent.owner.subject,
                        &agent.owner.tenant,
                    ],
                )
                .map_err(sql)
        })?;
        Ok(if changed == 1 {
            PutAgent::Stored
        } else {
            PutAgent::OwnedByOther
        })
    }

    fn agent(&self, id: AgentId) -> Result<Option<StoredAgent>, StoreError> {
        let Some(row) = self.with_client(|client| {
            client
                .query_opt(
                    "select manifest, owner_issuer, owner_subject, owner_tenant from agents where id = $1",
                    &[&id.as_uuid()],
                )
                .map_err(sql)
        })?
        else {
            return Ok(None);
        };
        stored_agent(&row).map(Some)
    }

    fn agents_of(&self, owner: &Owner) -> Result<Vec<StoredAgent>, StoreError> {
        let rows = self.with_client(|client| {
            client
                .query(
                    "select manifest, owner_issuer, owner_subject, owner_tenant from agents
                     where owner_issuer = $1 and owner_subject = $2 order by id",
                    &[&owner.issuer, &owner.subject],
                )
                .map_err(sql)
        })?;
        rows.iter().map(stored_agent).collect()
    }

    /// One transaction: the run row, and its events only when the row is
    /// new, so a run already stored keeps its spec and events.
    fn put_run(&self, run: StoredRun) -> Result<PutRun, StoreError> {
        let spec = serde_json::to_value(&run.spec).map_err(json)?;
        let rows = EventRows::new(&run.events)?;
        let id = run.spec.run_id.as_uuid();
        let principal = (
            run.spec.owner.issuer.clone(),
            run.spec.owner.subject.clone(),
        );
        let thread = thread_of(&run.spec).map(str::to_string);
        let parent = run.spec.lineage.parent.map(RunId::as_uuid);
        // A run stored with no events is dated now, as the backfill dates one.
        let created = run
            .events
            .first()
            .map_or_else(|| Timestamp::now().as_unix_millis(), |_| created_ms(&run));
        self.with_client(|client| {
            let mut tx = read_committed(client)?;
            let inserted = tx
                .execute(
                    "insert into runs
                         (id, spec, owner_issuer, owner_subject, thread_id, parent_run, created_ms)
                     values ($1, $2, $3, $4, $5, $6, $7)
                     on conflict (id) do nothing",
                    &[
                        &id,
                        &spec,
                        &principal.0,
                        &principal.1,
                        &thread,
                        &parent,
                        &created,
                    ],
                )
                .map_err(sql)?;
            if inserted == 1 {
                rows.insert(&mut tx, id, 0, &principal)?;
            }
            tx.commit().map_err(sql)?;
            if inserted == 1 && !run.events.is_empty() {
                self.notify(client, &principal);
            }
            Ok(if inserted == 1 {
                PutRun::Stored
            } else {
                PutRun::Existed
            })
        })
    }

    /// One transaction. The run row is locked first, so appends to one run
    /// take turns: each sees every event committed before it, checks for a
    /// terminal one, and only then inserts.
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
        self.run_page(id, 0, usize::MAX)
    }

    fn run_page(
        &self,
        id: RunId,
        after: usize,
        limit: usize,
    ) -> Result<Option<StoredRun>, StoreError> {
        let after = i64::try_from(after).unwrap_or(i64::MAX);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let id = id.as_uuid();
        let Some((spec, rows)) = self.with_client(|client| {
            // One snapshot, so the spec and the events agree.
            let mut tx = client
                .build_transaction()
                .read_only(true)
                .isolation_level(IsolationLevel::RepeatableRead)
                .start()
                .map_err(sql)?;
            let Some(spec) = tx
                .query_opt("select spec from runs where id = $1", &[&id])
                .map_err(sql)?
            else {
                return Ok(None);
            };
            let rows = tx
                .query(
                    "select body from run_events where run_id = $1 and seq > $2
                     order by seq limit $3",
                    &[&id, &after, &limit],
                )
                .map_err(sql)?;
            tx.commit().map_err(sql)?;
            Ok(Some((spec, rows)))
        })?
        else {
            return Ok(None);
        };
        let spec =
            serde_json::from_value(spec.try_get::<_, Value>(0).map_err(sql)?).map_err(json)?;
        let events = rows
            .iter()
            .map(|row| {
                serde_json::from_value(row.try_get::<_, Value>(0).map_err(sql)?).map_err(json)
            })
            .collect::<Result<_, _>>()?;
        Ok(Some(StoredRun { spec, events }))
    }

    fn put_artifact(&self, artifact: StoredArtifact) -> Result<(), StoreError> {
        self.with_client(|client| {
            client
                .execute(
                    "insert into artifacts (id, run_id, name, body) values ($1, $2, $3, $4)
                 on conflict (id) do update set run_id = excluded.run_id, name = excluded.name, body = excluded.body",
                    &[
                        &artifact.id.as_uuid(),
                        &artifact.run_id.as_uuid(),
                        &artifact.name,
                        &artifact.body,
                    ],
                )
                .map_err(sql)
        })?;
        Ok(())
    }

    fn artifact(&self, id: ArtifactId) -> Result<Option<StoredArtifact>, StoreError> {
        let Some(row) = self.with_client(|client| {
            client
                .query_opt(
                    "select run_id, name, body from artifacts where id = $1",
                    &[&id.as_uuid()],
                )
                .map_err(sql)
        })?
        else {
            return Ok(None);
        };
        Ok(Some(StoredArtifact {
            id,
            run_id: RunId::from_uuid(row.try_get(0).map_err(sql)?),
            name: row.try_get(1).map_err(sql)?,
            body: row.try_get(2).map_err(sql)?,
        }))
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

    fn triggers(&self) -> Option<&dyn TriggerStore> {
        Some(self)
    }
}

/// The triggers of `rows` a server can read. A row it cannot (a kind a newer
/// server stored) is left out, with an error, so it does not stop the rest.
fn readable_triggers(rows: &[postgres::Row]) -> Vec<StoredTrigger> {
    rows.iter()
        .filter_map(|row| {
            trigger_row(row)
                .map_err(|error| eprintln!("gol: a trigger row cannot be read: {error}"))
                .ok()
        })
        .collect()
}

/// A trigger from its row: the stored JSON, with the row's `enabled` and
/// `next_fire_ms`, which the JSON's copies never override.
fn trigger_row(row: &postgres::Row) -> Result<StoredTrigger, StoreError> {
    let body: serde_json::Value = row.try_get("body").map_err(sql)?;
    let mut trigger: StoredTrigger = serde_json::from_value(body).map_err(json)?;
    trigger.enabled = row.try_get("enabled").map_err(sql)?;
    trigger.next_fire_ms = row.try_get("next_fire_ms").map_err(sql)?;
    Ok(trigger)
}

impl TriggerStore for PostgresStore {
    /// One transaction under the principal's advisory lock: the count and
    /// the insert, so concurrent creates keep the cap.
    fn put_trigger(&self, trigger: &StoredTrigger, most: usize) -> Result<bool, StoreError> {
        let body = serde_json::to_value(trigger).map_err(json)?;
        let most = i64::try_from(most).unwrap_or(i64::MAX);
        self.with_client(|client| {
            let mut tx = client.transaction().map_err(sql)?;
            tx.execute(
                "select pg_advisory_xact_lock(
                     hashtextextended('gol.triggers:' || $1 || chr(31) || $2, 0))",
                &[&trigger.owner.issuer, &trigger.owner.subject],
            )
            .map_err(sql)?;
            let kept: i64 = tx
                .query_one(
                    "select count(*) from triggers
                     where owner_issuer = $1 and owner_subject = $2",
                    &[&trigger.owner.issuer, &trigger.owner.subject],
                )
                .and_then(|row| row.try_get(0))
                .map_err(sql)?;
            if kept >= most {
                return Ok(false);
            }
            tx.execute(
                "insert into triggers
                     (id, owner_issuer, owner_subject, body, enabled, next_fire_ms, created_ms)
                 values ($1, $2, $3, $4, $5, $6, $7)",
                &[
                    &trigger.id.as_uuid(),
                    &trigger.owner.issuer,
                    &trigger.owner.subject,
                    &body,
                    &trigger.enabled,
                    &trigger.next_fire_ms,
                    &trigger.created_ms,
                ],
            )
            .map_err(sql)?;
            tx.commit().map_err(sql)?;
            Ok(true)
        })
    }

    fn triggers_of(&self, owner: &Owner) -> Result<Vec<StoredTrigger>, StoreError> {
        let rows = self.with_client(|client| {
            client
                .query(
                    "select body, enabled, next_fire_ms from triggers
                     where owner_issuer = $1 and owner_subject = $2
                     order by created_ms, id",
                    &[&owner.issuer, &owner.subject],
                )
                .map_err(sql)
        })?;
        rows.iter().map(trigger_row).collect()
    }

    fn trigger(&self, owner: &Owner, id: TriggerId) -> Result<Option<StoredTrigger>, StoreError> {
        let row = self.with_client(|client| {
            client
                .query_opt(
                    "select body, enabled, next_fire_ms from triggers
                     where id = $1 and owner_issuer = $2 and owner_subject = $3",
                    &[&id.as_uuid(), &owner.issuer, &owner.subject],
                )
                .map_err(sql)
        })?;
        row.as_ref().map(trigger_row).transpose()
    }

    fn set_enabled(
        &self,
        owner: &Owner,
        id: TriggerId,
        enabled: bool,
    ) -> Result<Option<StoredTrigger>, StoreError> {
        let row = self.with_client(|client| {
            client
                .query_opt(
                    "update triggers set enabled = $4
                     where id = $1 and owner_issuer = $2 and owner_subject = $3
                     returning body, enabled, next_fire_ms",
                    &[&id.as_uuid(), &owner.issuer, &owner.subject, &enabled],
                )
                .map_err(sql)
        })?;
        row.as_ref().map(trigger_row).transpose()
    }

    /// One statement: the rotation read and bumped under the row's lock.
    fn rotate_webhook(
        &self,
        owner: &Owner,
        id: TriggerId,
    ) -> Result<Option<StoredTrigger>, StoreError> {
        let row = self.with_client(|client| {
            client
                .query_opt(
                    "update triggers
                     set body = jsonb_set(body, '{kind,webhook,rotation}',
                         to_jsonb((body->'kind'->'webhook'->>'rotation')::bigint + 1))
                     where id = $1 and owner_issuer = $2 and owner_subject = $3
                       and body->'kind' ? 'webhook'
                       and (body->'kind'->'webhook'->>'rotation')::bigint < 4294967295
                     returning body, enabled, next_fire_ms",
                    &[&id.as_uuid(), &owner.issuer, &owner.subject],
                )
                .map_err(sql)
        })?;
        row.as_ref().map(trigger_row).transpose()
    }

    fn delete_trigger(&self, owner: &Owner, id: TriggerId) -> Result<bool, StoreError> {
        self.with_client(|client| {
            client
                .execute(
                    "delete from triggers
                     where id = $1 and owner_issuer = $2 and owner_subject = $3",
                    &[&id.as_uuid(), &owner.issuer, &owner.subject],
                )
                .map(|deleted| deleted > 0)
                .map_err(sql)
        })
    }

    fn resume_trigger(
        &self,
        owner: &Owner,
        id: TriggerId,
        next_fire_ms: Option<i64>,
    ) -> Result<Option<StoredTrigger>, StoreError> {
        // One statement resumes it only while it is paused, with a bumped
        // generation; a running trigger is read as it is.
        let row = self.with_client(|client| {
            client
                .query_opt(
                    "update triggers set enabled = true, next_fire_ms = $4,
                         body = jsonb_set(body, '{generation}',
                             to_jsonb(coalesce((body->>'generation')::bigint, 0) + 1))
                     where id = $1 and owner_issuer = $2 and owner_subject = $3
                       and not enabled
                     returning body, enabled, next_fire_ms",
                    &[&id.as_uuid(), &owner.issuer, &owner.subject, &next_fire_ms],
                )
                .map_err(sql)
        })?;
        match row {
            Some(row) => trigger_row(&row).map(Some),
            None => self.trigger(owner, id),
        }
    }

    fn due_triggers(&self, now_ms: i64, limit: usize) -> Result<Vec<StoredTrigger>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = self.with_client(|client| {
            client
                .query(
                    "select body, enabled, next_fire_ms from triggers
                     where enabled and next_fire_ms <= $1
                     order by next_fire_ms, id
                     limit $2",
                    &[&now_ms, &limit],
                )
                .map_err(sql)
        })?;
        Ok(readable_triggers(&rows))
    }

    fn unscheduled_triggers(&self, limit: usize) -> Result<Vec<StoredTrigger>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = self.with_client(|client| {
            client
                .query(
                    "select body, enabled, next_fire_ms from triggers
                     where enabled and next_fire_ms is null and body->'kind' ? 'schedule'
                     limit $1",
                    &[&limit],
                )
                .map_err(sql)
        })?;
        Ok(readable_triggers(&rows))
    }

    /// One statement: the tick moves only from the one this scheduler fired.
    fn advance_trigger(
        &self,
        id: TriggerId,
        due_ms: Option<i64>,
        next_ms: Option<i64>,
    ) -> Result<bool, StoreError> {
        self.with_client(|client| {
            client
                .execute(
                    "update triggers set next_fire_ms = $3
                     where id = $1 and next_fire_ms is not distinct from $2",
                    &[&id.as_uuid(), &due_ms, &next_ms],
                )
                .map(|moved| moved == 1)
                .map_err(sql)
        })
    }

    fn pause_triggers(&self, owner: &Owner) -> Result<usize, StoreError> {
        self.with_client(|client| {
            client
                .execute(
                    "update triggers set enabled = false
                     where owner_issuer = $1 and owner_subject = $2 and enabled",
                    &[&owner.issuer, &owner.subject],
                )
                .map(|paused| usize::try_from(paused).unwrap_or(usize::MAX))
                .map_err(sql)
        })
    }
}

fn read_committed(client: &mut postgres::Client) -> Result<postgres::Transaction<'_>, StoreError> {
    client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .map_err(sql)
}

/// Events as `run_events` rows: each body, and whether it ends the run.
struct EventRows {
    bodies: Vec<Value>,
    terminal: Vec<bool>,
}

impl EventRows {
    fn new(events: &[Event]) -> Result<Self, StoreError> {
        check_one_terminal(events)?;
        Ok(Self {
            bodies: events
                .iter()
                .map(serde_json::to_value)
                .collect::<Result<_, _>>()
                .map_err(json)?,
            terminal: events
                .iter()
                .map(|event| is_terminal(&event.payload))
                .collect(),
        })
    }

    /// Inserts the rows numbered after `last`, in order, in one statement,
    /// then numbers them in `principal`'s outbox in another. The caller
    /// holds the run row (locked or just inserted), so the counter row is
    /// always taken after it; its lock is held to commit, so an owner's
    /// numbers commit in order (`formal/outbox`).
    fn insert(
        &self,
        tx: &mut postgres::Transaction<'_>,
        run_id: uuid::Uuid,
        last: i64,
        principal: &(String, String),
    ) -> Result<(), StoreError> {
        tx.execute(
            "insert into run_events (run_id, seq, body, terminal)
             select $1, $2 + row.ord, row.body, row.terminal
             from unnest($3::jsonb[], $4::bool[]) with ordinality as row (body, terminal, ord)",
            &[&run_id, &last, &self.bodies, &self.terminal],
        )
        .map_err(sql)?;
        let count = i64::try_from(self.bodies.len()).unwrap_or(i64::MAX);
        if count == 0 {
            return Ok(());
        }
        tx.execute(
            "with counter as (
                 insert into outbox_counters (owner_issuer, owner_subject, last)
                 values ($1, $2, $3)
                 on conflict (owner_issuer, owner_subject)
                 do update set last = outbox_counters.last + excluded.last
                 returning last
             )
             insert into outbox (owner_issuer, owner_subject, seq, run_id, run_seq, stored_ms)
             select $1, $2, counter.last - $3 + n, $4, $5 + n, $6
             from counter, generate_series(1::bigint, $3) as n",
            &[
                &principal.0,
                &principal.1,
                &count,
                &run_id,
                &last,
                &Timestamp::now().as_unix_millis(),
            ],
        )
        .map_err(sql)?;
        Ok(())
    }
}
