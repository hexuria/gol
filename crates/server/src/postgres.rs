use std::sync::Mutex;

use postgres::NoTls;
use protocol::{AgentId, ArtifactId, RunId};
use serde_json::Value;

use crate::store::{AgentManifest, RunStore, StoredArtifact, StoredRun};

pub struct PostgresStore {
    client: Mutex<postgres::Client>,
}

const SCHEMA: &str = "
create table if not exists agents (
    id uuid primary key,
    manifest jsonb not null
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
    client.batch_execute("commit")
}

impl PostgresStore {
    pub fn connect(url: &str) -> Result<Self, postgres::Error> {
        let mut client = postgres::Client::connect(url, NoTls)?;
        ensure_schema(&mut client)?;
        Ok(Self {
            client: Mutex::new(client),
        })
    }
}

impl RunStore for PostgresStore {
    fn put_agent(&self, agent: AgentManifest) {
        let manifest = serde_json::to_value(&agent).expect("agent json");
        self.client
            .lock()
            .expect("postgres")
            .execute(
                "insert into agents (id, manifest) values ($1, $2)
                 on conflict (id) do update set manifest = excluded.manifest",
                &[&agent.id.as_uuid(), &manifest],
            )
            .expect("insert agent");
    }

    fn put_run(&self, run: StoredRun) {
        let spec = serde_json::to_value(&run.spec).expect("spec json");
        let events = serde_json::to_value(&run.events).expect("events json");
        self.client
            .lock()
            .expect("postgres")
            .execute(
                "insert into runs (id, spec, events) values ($1, $2, $3)
                 on conflict (id) do update set spec = excluded.spec, events = excluded.events",
                &[&run.spec.run_id.as_uuid(), &spec, &events],
            )
            .expect("insert run");
    }

    fn run(&self, id: RunId) -> Option<StoredRun> {
        let row = self
            .client
            .lock()
            .expect("postgres")
            .query_opt(
                "select spec, events from runs where id = $1",
                &[&id.as_uuid()],
            )
            .expect("select run")?;
        let spec = serde_json::from_value(row.get::<_, Value>(0)).expect("spec");
        let events = serde_json::from_value(row.get::<_, Value>(1)).expect("events");
        Some(StoredRun { spec, events })
    }

    fn put_artifact(&self, artifact: StoredArtifact) {
        self.client
            .lock()
            .expect("postgres")
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
            .expect("insert artifact");
    }

    fn artifact(&self, id: ArtifactId) -> Option<StoredArtifact> {
        let row = self
            .client
            .lock()
            .expect("postgres")
            .query_opt(
                "select run_id, name, body from artifacts where id = $1",
                &[&id.as_uuid()],
            )
            .expect("select artifact")?;
        Some(StoredArtifact {
            id,
            run_id: RunId::from_uuid(row.get(0)),
            name: row.get(1),
            body: row.get(2),
        })
    }
}

impl PostgresStore {
    pub fn agent(&self, id: AgentId) -> Option<AgentManifest> {
        let row = self
            .client
            .lock()
            .expect("postgres")
            .query_opt(
                "select manifest from agents where id = $1",
                &[&id.as_uuid()],
            )
            .expect("select agent")?;
        Some(serde_json::from_value(row.get::<_, Value>(0)).expect("agent"))
    }
}
