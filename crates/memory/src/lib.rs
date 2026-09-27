#![forbid(unsafe_code)]
use std::sync::Mutex;

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

    /// Runs `op` on the connection, opening a new one first when the last
    /// died. A poisoned lock or a failed statement is a StoreError, and a
    /// closed connection is dropped so the next call reconnects.
    fn with_client<T>(
        &self,
        op: impl FnOnce(&mut postgres::Client) -> Result<T, postgres::Error>,
    ) -> Result<T, StoreError> {
        let mut slot = self
            .client
            .lock()
            .map_err(|_| StoreError::new("memory lock poisoned"))?;
        if slot.as_ref().is_none_or(postgres::Client::is_closed) {
            *slot = None;
            *slot = Some(open(&self.url).map_err(|error| StoreError::new(error.to_string()))?);
        }
        let Some(client) = slot.as_mut() else {
            return Err(StoreError::new("memory connection missing"));
        };
        let result = op(client);
        if client.is_closed() || result.as_ref().is_err_and(|error| error.is_closed()) {
            *slot = None;
        }
        result.map_err(|error| StoreError::new(error.to_string()))
    }
}

fn open(url: &str) -> Result<postgres::Client, postgres::Error> {
    let mut client = postgres::Client::connect(url, NoTls)?;
    ensure_schema(&mut client)?;
    Ok(client)
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
        Ok(row.map(|row| row.get(0)))
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
