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
