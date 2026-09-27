use std::sync::Mutex;

use postgres::NoTls;
use protocol::{AgentId, ArtifactId, Event, Owner, RunId};
use serde_json::Value;

use harness::StoreError;

use crate::store::{Append, PutAgent, RunStore, StoredAgent, StoredArtifact, StoredRun};

/// The run store in Postgres over one connection. A connection that dies is
/// replaced on the next call (owner decision 1A for C1); the call that saw it
/// die reports a StoreError and is not retried, since a write may have
/// committed (2A).
pub struct PostgresStore {
    url: String,
    client: Mutex<Option<postgres::Client>>,
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
    spec jsonb not null,
    events jsonb not null
);
create table if not exists artifacts (
    id uuid primary key,
    run_id uuid not null,
    name text not null,
    body bytea not null
);
";

fn ensure_schema(client: &mut postgres::Client) -> Result<(), postgres::Error> {
    client.batch_execute("begin")?;
    if let Err(err) = client.query_one("select pg_advisory_xact_lock(872346)", &[]) {
        let _ = client.batch_execute("rollback");
        return Err(err);
    }
    if let Err(err) = client.batch_execute(SCHEMA) {
        let _ = client.batch_execute("rollback");
        return Err(err);
    }
    // `create table if not exists` leaves a table from an older schema as it
    // was. Fail here, at connect, rather than on the first write, where the
    // panic would poison the store for every later request.
    if let Err(err) =
        client.batch_execute("select owner_issuer, owner_subject, owner_tenant from agents limit 0")
    {
        let _ = client.batch_execute("rollback");
        return Err(err);
    }
    client.batch_execute("commit")
}

impl PostgresStore {
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
        op: impl FnOnce(&mut postgres::Client) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let mut slot = self
            .client
            .lock()
            .map_err(|_| StoreError::new("postgres store lock poisoned"))?;
        if slot.as_ref().is_none_or(postgres::Client::is_closed) {
            *slot = None;
            *slot = Some(open(&self.url).map_err(sql)?);
        }
        let Some(client) = slot.as_mut() else {
            return Err(StoreError::new("postgres connection missing"));
        };
        let result = op(client);
        if client.is_closed() {
            *slot = None;
        }
        result
    }
}

fn open(url: &str) -> Result<postgres::Client, postgres::Error> {
    let mut client = postgres::Client::connect(url, NoTls)?;
    ensure_schema(&mut client)?;
    Ok(client)
}

fn sql(error: postgres::Error) -> StoreError {
    StoreError::new(error.to_string())
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
            manifest: serde_json::from_value(row.get::<_, Value>(0)).map_err(json)?,
            owner: Owner::new(
                row.get::<_, String>(1),
                row.get::<_, String>(2),
                row.get::<_, String>(3),
            ),
        }))
    }

    fn put_run(&self, run: StoredRun) -> Result<(), StoreError> {
        let spec = serde_json::to_value(&run.spec).map_err(json)?;
        let events = serde_json::to_value(&run.events).map_err(json)?;
        self.with_client(|client| {
            client
                .execute(
                    "insert into runs (id, spec, events) values ($1, $2, $3)
                 on conflict (id) do nothing",
                    &[&run.spec.run_id.as_uuid(), &spec, &events],
                )
                .map_err(sql)
        })?;
        Ok(())
    }

    fn append_events(&self, id: RunId, events: Vec<Event>) -> Result<Append, StoreError> {
        let events = serde_json::to_value(&events).map_err(json)?;
        self.with_client(|client| {
            // One statement: the terminal check and the append see the same row.
            // The payloads of store::is_terminal. Unit variants serialize as a string
            // ("RunCancelled"), the others as {"RunCompleted": ..}; `?|` matches both.
            let appended = client
                .execute(
                    "update runs set events = events || $2::jsonb
                 where id = $1 and not exists (
                     select 1 from jsonb_array_elements(events) as event
                     where event->'payload' ?| array['RunCompleted', 'RunFailed', 'RunCancelled', 'RunExpired'])",
                    &[&id.as_uuid(), &events],
                )
                .map_err(sql)?;
            if appended == 1 {
                return Ok(Append::Appended);
            }
            let exists = client
                .query_opt("select 1 from runs where id = $1", &[&id.as_uuid()])
                .map_err(sql)?
                .is_some();
            Ok(if exists {
                Append::Terminal
            } else {
                Append::Missing
            })
        })
    }

    fn run(&self, id: RunId) -> Result<Option<StoredRun>, StoreError> {
        let Some(row) = self.with_client(|client| {
            client
                .query_opt(
                    "select spec, events from runs where id = $1",
                    &[&id.as_uuid()],
                )
                .map_err(sql)
        })?
        else {
            return Ok(None);
        };
        let spec = serde_json::from_value(row.get::<_, Value>(0)).map_err(json)?;
        let events = serde_json::from_value(row.get::<_, Value>(1)).map_err(json)?;
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
            run_id: RunId::from_uuid(row.get(0)),
            name: row.get(1),
            body: row.get(2),
        }))
    }
}
