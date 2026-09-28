use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::Duration;

use postgres::{IsolationLevel, NoTls};
use protocol::{AgentId, ArtifactId, Event, Owner, RunId};
use serde_json::Value;

use harness::StoreError;

use crate::store::{
    check_one_terminal, is_terminal, Append, PutAgent, RunStore, StoredAgent, StoredArtifact,
    StoredRun,
};

/// The run store in Postgres over a pool of connections (C2, owner decision
/// 1A). The pool checks a connection before lending it, so one whose backend
/// died is replaced before a call uses it. A call that fails mid-statement
/// reports a StoreError and is not retried, since a write may have committed
/// (C1 decision 2A).
pub struct PostgresStore {
    pool: r2d2::Pool<Connections>,
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
create table if not exists runs (
    id uuid primary key,
    spec jsonb not null
);
create table if not exists run_events (
    run_id uuid not null references runs (id),
    seq bigint not null,
    body jsonb not null,
    terminal boolean not null,
    primary key (run_id, seq)
);

create table if not exists artifacts (
    id uuid primary key,
    run_id uuid not null,
    name text not null,
    body bytea not null
);
";

fn ensure_schema(client: &mut postgres::Client) -> Result<(), StoreError> {
    let mut tx = client.transaction().map_err(sql)?;
    tx.query_one("select pg_advisory_xact_lock(872346)", &[])
        .map_err(sql)?;
    tx.batch_execute(SCHEMA).map_err(sql)?;
    // `create index if not exists` locks the table even when the index is
    // there, and would wait behind any writer stalled mid-append.
    // Looked up on the table itself, not by name on the search path.
    let index = tx
        .query_opt(
            "select 1 from pg_index join pg_class on pg_class.oid = pg_index.indexrelid
             where pg_index.indrelid = 'run_events'::regclass
               and pg_class.relname = 'run_events_one_terminal'",
            &[],
        )
        .map_err(sql)?;
    if index.is_none() {
        tx.batch_execute(
            "create unique index run_events_one_terminal on run_events (run_id) where terminal",
        )
        .map_err(sql)?;
    }
    // `create table if not exists` leaves a table from an older schema as it
    // was. Fail here, at connect, rather than on the first write; dropping
    // the transaction rolls back anything created above.
    tx.batch_execute("select owner_issuer, owner_subject, owner_tenant from agents limit 0")
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
            .build_unchecked(Connections { config });
        let store = Self { pool };
        store.with_client(ensure_schema)?;
        Ok(store)
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
        Ok(Some(StoredAgent {
            manifest: serde_json::from_value(row.try_get::<_, Value>(0).map_err(sql)?)
                .map_err(json)?,
            owner: Owner::new(
                row.try_get::<_, String>(1).map_err(sql)?,
                row.try_get::<_, String>(2).map_err(sql)?,
                row.try_get::<_, String>(3).map_err(sql)?,
            ),
        }))
    }

    /// One transaction: the run row, and its events only when the row is
    /// new, so a run already stored keeps its spec and events.
    fn put_run(&self, run: StoredRun) -> Result<(), StoreError> {
        let spec = serde_json::to_value(&run.spec).map_err(json)?;
        let rows = EventRows::new(&run.events)?;
        let id = run.spec.run_id.as_uuid();
        self.with_client(|client| {
            let mut tx = read_committed(client)?;
            let inserted = tx
                .execute(
                    "insert into runs (id, spec) values ($1, $2) on conflict (id) do nothing",
                    &[&id, &spec],
                )
                .map_err(sql)?;
            if inserted == 1 {
                rows.insert(&mut tx, id, 0)?;
            }
            tx.commit().map_err(sql)
        })
    }

    /// One transaction. The run row is locked first, so appends to one run
    /// take turns: each sees every event committed before it, checks for a
    /// terminal one, and only then inserts.
    fn append_events(&self, id: RunId, events: Vec<Event>) -> Result<Append, StoreError> {
        let rows = EventRows::new(&events)?;
        let id = id.as_uuid();
        self.with_client(|client| {
            // Read committed whatever the session default: each statement
            // after the row lock sees what the lock's last holder committed.
            let mut tx = read_committed(client)?;
            let locked = tx
                .query_opt("select 1 from runs where id = $1 for update", &[&id])
                .map_err(sql)?;
            if locked.is_none() {
                return Ok(Append::Missing);
            }
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
            let last = match last {
                Some(row) => row.try_get(0).map_err(sql)?,
                None => 0,
            };
            rows.insert(&mut tx, id, last)?;
            tx.commit().map_err(sql)?;
            Ok(Append::Appended)
        })
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

    /// Inserts the rows numbered after `last`, in order, in one statement.
    fn insert(
        &self,
        tx: &mut postgres::Transaction<'_>,
        run_id: uuid::Uuid,
        last: i64,
    ) -> Result<(), StoreError> {
        tx.execute(
            "insert into run_events (run_id, seq, body, terminal)
             select $1, $2 + row.ord, row.body, row.terminal
             from unnest($3::jsonb[], $4::bool[]) with ordinality as row (body, terminal, ord)",
            &[&run_id, &last, &self.bodies, &self.terminal],
        )
        .map_err(sql)?;
        Ok(())
    }
}
