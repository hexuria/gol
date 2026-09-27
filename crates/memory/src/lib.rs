#![forbid(unsafe_code)]
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Mutex;
use std::time::Duration;

use harness::{Memory, StoreError};
use postgres::NoTls;
use protocol::MemoryScope;

/// Memory in Postgres over one connection. A connection that dies is
/// replaced on the next call (owner decision 1A for C1); the call that saw it
/// die reports a StoreError and is not retried (2A).
pub struct PostgresMemory {
    url: String,
    client: Mutex<Option<postgres::Client>>,
}

const SCHEMA: &str = "
create table if not exists memories (
    scope text not null,
    key text not null,
    value text not null,
    primary key (scope, key)
);
";

fn ensure_schema(client: &mut postgres::Client) -> Result<(), postgres::Error> {
    client.batch_execute("begin")?;
    if let Err(err) = client.query_one("select pg_advisory_xact_lock(872347)", &[]) {
        let _ = client.batch_execute("rollback");
        return Err(err);
    }
    if let Err(err) = client.batch_execute(SCHEMA) {
        let _ = client.batch_execute("rollback");
        return Err(err);
    }
    client.batch_execute("commit")
}

impl PostgresMemory {
    pub fn connect(url: &str) -> Result<Self, postgres::Error> {
        let client = open(url)?;
        Ok(Self {
            url: url.to_string(),
            client: Mutex::new(Some(client)),
        })
    }

    /// Runs `op` on the connection, opening a new one first when there is
    /// none. Any failed call drops the connection, so the next call
    /// reconnects: a backend killed mid-statement reports a database error,
    /// not a closed connection, and its client still looks open.
    fn with_client<T>(
        &self,
        op: impl FnOnce(&mut postgres::Client) -> Result<T, postgres::Error>,
    ) -> Result<T, StoreError> {
        // A panic while the lock was held leaves the connection in an unknown
        // state. Drop it and keep serving, rather than refuse every later call.
        let mut slot = self.client.lock().unwrap_or_else(|poisoned| {
            self.client.clear_poison();
            let mut slot = poisoned.into_inner();
            *slot = None;
            slot
        });
        if slot.as_ref().is_none_or(postgres::Client::is_closed) {
            *slot = None;
            *slot = Some(reconnect(&self.url)?);
        }
        let Some(client) = slot.as_mut() else {
            return Err(StoreError::new("memory connection missing"));
        };
        let result = op(client);
        if result.is_err() {
            *slot = None;
        }
        result.map_err(sql)
    }
}

/// How long a connect may take when the URL sets no `connect_timeout`.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

fn open(url: &str) -> Result<postgres::Client, postgres::Error> {
    let mut config: postgres::Config = url.parse()?;
    if config.get_connect_timeout().is_none() {
        config.connect_timeout(CONNECT_TIMEOUT);
    }
    let mut client = config.connect(NoTls)?;
    ensure_schema(&mut client)?;
    Ok(client)
}

/// `open` for a running store. `postgres::Config::connect` unwraps building
/// its runtime, which panics when the process is out of file descriptors;
/// that is a StoreError here, not a panic in the caller.
fn reconnect(url: &str) -> Result<postgres::Client, StoreError> {
    catch_unwind(AssertUnwindSafe(|| open(url)))
        .map_err(|_| StoreError::new("memory connect panicked"))?
        .map_err(sql)
}

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

fn scope_name(scope: MemoryScope) -> Result<String, StoreError> {
    serde_json::to_string(&scope).map_err(|error| StoreError::new(error.to_string()))
}

impl Memory for PostgresMemory {
    fn read(&self, scope: MemoryScope, key: &str) -> Result<Option<String>, StoreError> {
        let scope = scope_name(scope)?;
        let row = self.with_client(|client| {
            client.query_opt(
                "select value from memories where scope = $1 and key = $2",
                &[&scope, &key],
            )
        })?;
        row.map(|row| row.try_get(0).map_err(sql)).transpose()
    }

    fn write(&mut self, scope: MemoryScope, key: &str, value: &str) -> Result<(), StoreError> {
        let scope = scope_name(scope)?;
        self.with_client(|client| {
            client.execute(
                "insert into memories (scope, key, value) values ($1, $2, $3)
                 on conflict (scope, key) do update set value = excluded.value",
                &[&scope, &key, &value],
            )
        })
        .map(|_| ())
    }
}
