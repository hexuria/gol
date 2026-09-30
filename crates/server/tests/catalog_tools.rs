//! The catalog in runs (item 8a, decision 90A; the D2 remainder): with a
//! catalog directory (`GOL_CATALOG_DIR`), a run gets, beside `echo`, the
//! catalog tools its agent's manifest names, and calls them through their
//! MCP server. A catalog tool the manifest does not name is not offered.
//! On both stores; needs Postgres, Redis and `python3`, as `pg_redis.rs`
//! does.
mod common;

use std::fs;
use std::path::PathBuf;

use common::queued::{blocking, fresh_user, jev, serve, stores, Store};
use protocol::{AgentId, Capability, EventPayload, RunId};
use server::{AgentManifest, StoredAgent};

/// A catalog directory with one MCP server, `local`, whose `ping` tool
/// answers `pong:<input>`.
fn catalog() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("gol-catalog-{}", RunId::new()));
    fs::create_dir_all(&dir).expect("dir");
    let script = dir.join("server.py");
    fs::write(
        &script,
        r#"import json, sys
def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()
for line in sys.stdin:
    msg = json.loads(line)
    if "id" not in msg:
        continue
    method = msg.get("method")
    if method == "initialize":
        send({"jsonrpc":"2.0","id":msg["id"],"result":{"protocolVersion":"2024-11-05","capabilities":{},"serverInfo":{"name":"fake","version":"0"}}})
    elif method == "tools/list":
        send({"jsonrpc":"2.0","id":msg["id"],"result":{"tools":[{"name":"ping","description":"p","inputSchema":{"type":"object"}}]}})
    elif method == "tools/call":
        text = msg["params"]["arguments"].get("input", "")
        send({"jsonrpc":"2.0","id":msg["id"],"result":{"content":[{"type":"text","text":"pong:" + text}]}})
    else:
        send({"jsonrpc":"2.0","id":msg["id"],"error":{"code":-32601,"message":"no"}})
"#,
    )
    .expect("script");
    fs::write(
        dir.join("harness.toml"),
        format!(
            "[[mcp]]\nname = \"local\"\ncommand = \"python3\"\nargs = [\"{}\"]\n\n[[mcp.tools]]\nname = \"ping\"\ndescription = \"p\"\n",
            script.display()
        ),
    )
    .expect("catalog");
    dir
}

/// Stores an agent of `user`'s whose manifest names `tools`, with the
/// capability of the catalog's `ping`.
async fn agent(store: &Store, user: &str, tools: &[&str]) -> AgentId {
    let (store, user) = (store.clone(), user.to_string());
    let tools: Vec<String> = tools.iter().map(|tool| (*tool).to_string()).collect();
    let id = AgentId::new();
    blocking(move || {
        store
            .put_agent(StoredAgent {
                manifest: AgentManifest {
                    id,
                    version: "1".to_string(),
                    name: "pinger".to_string(),
                    description: "Pings.".to_string(),
                    instructions: "Ping.".to_string(),
                    tools,
                    required_capabilities: vec![Capability::new("mcp.local.ping")],
                },
                owner: protocol::Owner::new(common::ISSUER, user, "tenant-1"),
            })
            .expect("put agent");
    })
    .await;
    id
}

async fn payloads(store: &Store, run: &str) -> Vec<EventPayload> {
    let (store, run): (Store, RunId) = (store.clone(), run.parse().expect("id"));
    blocking(move || {
        store
            .run(run)
            .expect("read")
            .expect("stored")
            .events
            .into_iter()
            .map(|event| event.payload)
            .collect()
    })
    .await
}

// A run whose agent names the catalog's `ping` calls it through its MCP
// server, and completes.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_calls_a_catalog_tool_its_manifest_names() {
    for store in stores() {
        let jev = jev(&["ping", "complete"]).await;
        let server = serve(store.clone(), &jev, 1).await.with_catalog(&catalog());
        let user = fresh_user();
        let agent = agent(&store, &user, &["ping"]).await;
        let (_, run) = server.start(&user, agent, "hi").await;
        assert!(server.work().await.is_some());
        let log = payloads(&store, &run).await;
        assert!(
            log.iter().any(|payload| matches!(
                payload,
                EventPayload::ToolResult { output, .. } if output == "pong:hi"
            )),
            "{log:?}"
        );
        assert!(
            matches!(log.last(), Some(EventPayload::RunCompleted { .. })),
            "{log:?}"
        );
    }
}

// A catalog tool the manifest does not name is not offered: a decider that
// asks for it anyway fails the run, and nothing is called.
#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_tool_outside_the_manifest_is_not_offered() {
    for store in stores() {
        let jev = jev(&["ping"]).await;
        let server = serve(store.clone(), &jev, 2).await.with_catalog(&catalog());
        let user = fresh_user();
        let agent = agent(&store, &user, &[]).await;
        let (_, run) = server.start(&user, agent, "hi").await;
        assert!(server.work().await.is_some());
        let log = payloads(&store, &run).await;
        assert!(
            !log.iter()
                .any(|payload| matches!(payload, EventPayload::ToolResult { .. })),
            "{log:?}"
        );
        assert!(
            matches!(log.last(), Some(EventPayload::RunFailed { message, .. }) if message.contains("unknown effect choice: ping")),
            "{log:?}"
        );
    }
}

// A catalog that does not load (it was removed after the server started)
// leaves a run that names a catalog tool open: its delivery ends in an
// error, so the run comes back once its lease expires, not failed for good.
#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_that_stops_loading_leaves_the_run_open() {
    for store in stores() {
        let jev = jev(&["ping", "complete"]).await;
        let dir = catalog();
        let server = serve(store.clone(), &jev, 3).await.with_catalog(&dir);
        fs::remove_dir_all(&dir).expect("remove the catalog");
        let user = fresh_user();
        let agent = agent(&store, &user, &["ping"]).await;
        let (_, run) = server.start(&user, agent, "hi").await;
        let worker = server.worker.clone();
        let worked = blocking(move || worker.work_one()).await;
        assert!(worked.is_err(), "{worked:?}");
        let log = payloads(&store, &run).await;
        assert!(
            !log.iter().any(|payload| matches!(
                payload,
                EventPayload::RunFailed { .. } | EventPayload::RunCompleted { .. }
            )),
            "{log:?}"
        );
    }
}

/// An in-memory store whose reads of an agent fail once armed: the run
/// store blipped while a worker looked up the run's tools.
struct AgentReadFails {
    inner: server::InMemoryStore,
    failing: std::sync::Mutex<bool>,
}

impl server::RunStore for AgentReadFails {
    fn put_agent(&self, agent: StoredAgent) -> Result<server::PutAgent, server::StoreError> {
        self.inner.put_agent(agent)
    }
    fn agent(&self, id: AgentId) -> Result<Option<StoredAgent>, server::StoreError> {
        if *self.failing.lock().expect("lock") {
            return Err(server::StoreError::new("the store is unreachable"));
        }
        self.inner.agent(id)
    }
    fn agents_of(&self, owner: &protocol::Owner) -> Result<Vec<StoredAgent>, server::StoreError> {
        self.inner.agents_of(owner)
    }
    fn put_run(&self, run: server::StoredRun) -> Result<server::PutRun, server::StoreError> {
        self.inner.put_run(run)
    }
    fn append_events(
        &self,
        id: RunId,
        events: Vec<protocol::Event>,
    ) -> Result<server::Append, server::StoreError> {
        self.inner.append_events(id, events)
    }
    fn append_events_after(
        &self,
        id: RunId,
        seen: usize,
        events: Vec<protocol::Event>,
    ) -> Result<server::Append, server::StoreError> {
        self.inner.append_events_after(id, seen, events)
    }
    fn run(&self, id: RunId) -> Result<Option<server::StoredRun>, server::StoreError> {
        self.inner.run(id)
    }
    fn put_artifact(&self, artifact: server::StoredArtifact) -> Result<(), server::StoreError> {
        self.inner.put_artifact(artifact)
    }
    fn artifact(
        &self,
        id: protocol::ArtifactId,
    ) -> Result<Option<server::StoredArtifact>, server::StoreError> {
        self.inner.artifact(id)
    }
    fn threads(&self) -> Option<&dyn server::ThreadStore> {
        self.inner.threads()
    }
    fn stops(&self) -> Option<&dyn server::StopStore> {
        self.inner.stops()
    }
    fn triggers(&self) -> Option<&dyn server::TriggerStore> {
        self.inner.triggers()
    }
}

// A store that cannot answer for the run's agent when its worker looks up
// its tools leaves the run open, to be tried again, rather than running it
// without the tools its manifest names.
#[tokio::test(flavor = "multi_thread")]
async fn a_store_blip_on_the_agent_leaves_the_run_open() {
    let blip = std::sync::Arc::new(AgentReadFails {
        inner: server::InMemoryStore::default(),
        failing: std::sync::Mutex::new(false),
    });
    let store: Store = blip.clone();
    let jev = jev(&["ping", "complete"]).await;
    let server = serve(store.clone(), &jev, 4).await.with_catalog(&catalog());
    let user = fresh_user();
    let agent = agent(&store, &user, &["ping"]).await;
    let (_, run) = server.start(&user, agent, "hi").await;
    *blip.failing.lock().expect("lock") = true;
    let worker = server.worker.clone();
    let worked = blocking(move || worker.work_one()).await;
    assert!(worked.is_err(), "{worked:?}");
    let log = payloads(&store, &run).await;
    assert!(
        !log.iter().any(|payload| matches!(
            payload,
            EventPayload::RunFailed { .. } | EventPayload::RunCompleted { .. }
        )),
        "{log:?}"
    );
}
