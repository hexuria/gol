use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::time::{SystemTime, UNIX_EPOCH};

use harness::{
    load_catalog, Decider, DeciderError, DecisionView, Driver, InMemory, ScriptedDecider,
    UnavailableModel,
};
use protocol::{
    AgentId, Capability, CredentialSource, Effect, EventPayload, ExecutionPlacement, HarnessState,
    InvocationId, Limits, ModelProvider, RunSpec, WorkModel,
};

fn spec(capabilities: Vec<&str>) -> RunSpec {
    RunSpec::builder()
        .agent(AgentId::new(), "1")
        .input("hello")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .capabilities(capabilities.into_iter().map(Capability::new).collect())
        .limits(Limits {
            max_steps: 8,
            max_model_calls: 4,
        })
        .build()
}

fn scratch() -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("gol-catalog-{nanos}"));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn tool_names(events: &[protocol::Event]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match &event.payload {
            EventPayload::ToolResult { name, output, .. } => Some(format!("{name}:{output}")),
            _ => None,
        })
        .collect()
}

#[test]
fn declared_echo_runs_and_undeclared_tool_is_denied() {
    let dir = scratch();
    fs::write(dir.join("harness.toml"), "tools = [\"echo\"]\n").unwrap();
    let catalog = load_catalog(&dir).unwrap();
    let mut driver = Driver::boot_with_catalog(spec(vec!["tool.echo"]), catalog).unwrap();
    let mut decider = ScriptedDecider::new([
        Effect::ToolCall {
            name: "echo".into(),
            input: "hello".into(),
            invocation: InvocationId::new(),
        },
        Effect::ToolCall {
            name: "other".into(),
            input: "nope".into(),
            invocation: InvocationId::new(),
        },
        Effect::Complete {
            outcome: "done".into(),
        },
    ]);
    driver
        .run_loaded(&mut decider, &UnavailableModel, &mut InMemory::default())
        .unwrap();
    assert_eq!(tool_names(driver.events()), vec!["echo:hello".to_string()]);
    assert!(driver.events().iter().any(|event| matches!(
        &event.payload,
        EventPayload::EffectDenied { reason, .. } if reason.contains("unknown tool")
    )));
    assert!(matches!(
        driver.state().harness,
        HarnessState::Completed { .. }
    ));
}

#[test]
fn mcp_tool_is_loaded_and_called() {
    let dir = scratch();
    let script = dir.join("mcp.py");
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
    .unwrap();
    let toml = format!(
        "[[mcp]]\nname = \"local\"\ncommand = \"python3\"\nargs = [\"{}\"]\n\n[[mcp.tools]]\nname = \"ping\"\ndescription = \"p\"\n",
        script.display()
    );
    fs::write(dir.join("harness.toml"), toml).unwrap();
    let catalog = load_catalog(&dir).unwrap();
    assert_eq!(catalog.descriptors()[0].name, "ping");
    let mut driver = Driver::boot_with_catalog(spec(vec!["mcp.local.ping"]), catalog).unwrap();
    let mut decider = ScriptedDecider::new([
        Effect::ToolCall {
            name: "ping".into(),
            input: "hi".into(),
            invocation: InvocationId::new(),
        },
        Effect::Complete {
            outcome: "done".into(),
        },
    ]);
    driver
        .run_loaded(&mut decider, &UnavailableModel, &mut InMemory::default())
        .unwrap();
    assert_eq!(
        tool_names(driver.events()),
        vec!["ping:pong:hi".to_string()]
    );
}

struct SkillSeen {
    body: String,
}

impl Decider for SkillSeen {
    fn decide(&mut self, view: &DecisionView<'_>) -> Result<protocol::Effect, DeciderError> {
        self.body = view
            .skills
            .iter()
            .map(|skill| format!("{}:{}", skill.name, skill.body))
            .collect();
        Ok(Effect::Complete {
            outcome: "done".into(),
        })
    }
}

#[test]
fn reading_the_catalog_does_not_spawn_the_configured_command() {
    let dir = scratch();
    let marker = dir.join("spawned");
    let command = dir.join("spawn-marker");
    fs::write(
        &command,
        format!("#!/bin/sh\ntouch '{}'\n", marker.display()),
    )
    .unwrap();
    fs::set_permissions(&command, fs::Permissions::from_mode(0o755)).unwrap();
    let toml = format!(
        "[[mcp]]\nname = \"local\"\ncommand = \"{}\"\n\n[[mcp.tools]]\nname = \"ping\"\ndescription = \"p\"\n",
        command.display()
    );
    fs::write(dir.join("harness.toml"), toml).unwrap();

    let loaded = load_catalog(&dir);
    assert!(
        !marker.exists(),
        "reading the catalog spawned the configured command"
    );
    let catalog = loaded.expect("reading the catalog must not execute the mcp command");
    assert_eq!(catalog.descriptors()[0].name, "ping");
    assert!(!marker.exists());
    drop(catalog);
    assert!(!marker.exists());
}

#[test]
fn skill_body_is_on_the_decision_view() {
    let dir = scratch();
    fs::create_dir(dir.join("skills")).unwrap();
    fs::write(dir.join("skills/note.md"), "remember the rust").unwrap();
    fs::write(dir.join("harness.toml"), "skills = [\"skills/note.md\"]\n").unwrap();
    let catalog = load_catalog(&dir).unwrap();
    let mut driver = Driver::boot_with_catalog(spec(vec![]), catalog).unwrap();
    let mut decider = SkillSeen {
        body: String::new(),
    };
    driver
        .run_loaded(&mut decider, &UnavailableModel, &mut InMemory::default())
        .unwrap();
    assert_eq!(decider.body, "note:remember the rust");
}
