use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use harness::{
    load_catalog, Decider, DeciderError, DecisionView, Driver, EchoTool, InMemory, LoadError,
    LoadedCatalog, ScriptedDecider, Tool, UnavailableModel,
};
use protocol::{
    AgentId, Capability, CredentialSource, Effect, EventPayload, ExecutionPlacement, FailureClass,
    HarnessState, InvocationId, Limits, ModelProvider, RunSpec, WorkModel,
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

fn command_is_running(command: &Path) -> bool {
    let needle = command.as_os_str().as_bytes();
    let entries = fs::read_dir("/proc").expect("catalog spawn check requires /proc");
    for entry in entries.flatten() {
        let name = entry.file_name();
        if !name.as_bytes().iter().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        let Ok(cmdline) = fs::read(entry.path().join("cmdline")) else {
            continue;
        };
        if cmdline.split(|byte| *byte == 0).any(|arg| arg == needle) {
            return true;
        }
    }
    false
}

/// `Command::spawn` returns before the shell reaches `touch`, and dropping a
/// session kills that child. Watch the process itself for `window` before
/// accepting that the command never started.
fn assert_command_not_started(command: &Path, marker: &Path, window: Duration) {
    let deadline = Instant::now() + window;
    loop {
        let running = command_is_running(command);
        let marked = marker.exists();
        if running || marked {
            panic!(
                "reading the catalog spawned the configured command (running={running}, marker={marked})"
            );
        }
        if Instant::now() >= deadline {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
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
        format!(
            "#!/bin/sh\ntouch '{}'\nprintf '\\n'\nread ignored\n",
            marker.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&command, fs::Permissions::from_mode(0o755)).unwrap();
    let toml = format!(
        "[[mcp]]\nname = \"local\"\ncommand = \"{}\"\n\n[[mcp.tools]]\nname = \"ping\"\ndescription = \"p\"\n",
        command.display()
    );
    fs::write(dir.join("harness.toml"), toml).unwrap();

    let loaded = load_catalog(&dir);
    let catalog = loaded.expect("reading the catalog must not execute the mcp command");
    assert_eq!(catalog.descriptors()[0].name, "ping");
    assert_command_not_started(&command, &marker, Duration::from_millis(200));
    drop(catalog);
    assert!(
        !command_is_running(&command),
        "reading the catalog spawned the configured command"
    );
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

fn run_call(catalog: LoadedCatalog, capability: &str, name: &str, input: &str) -> Driver {
    let mut driver = Driver::boot_with_catalog(spec(vec![capability]), catalog).unwrap();
    let mut decider = ScriptedDecider::new([
        Effect::ToolCall {
            name: name.into(),
            input: input.into(),
            invocation: InvocationId::new(),
        },
        Effect::Complete {
            outcome: "done".into(),
        },
    ]);
    driver
        .run_loaded(&mut decider, &UnavailableModel, &mut InMemory::default())
        .unwrap();
    driver
}

fn assert_tool_failure(driver: &Driver) {
    assert!(
        driver
            .events()
            .iter()
            .all(|event| !matches!(event.payload, EventPayload::ToolResult { .. })),
        "tool error was recorded as a tool result"
    );
    assert!(
        matches!(
            driver.state().harness,
            HarnessState::Failed {
                class: FailureClass::Tool,
                ..
            }
        ),
        "expected a tool failure, got {:?}",
        driver.state().harness
    );
    assert!(!matches!(
        driver.state().harness,
        HarnessState::Completed { .. }
    ));
}

fn mcp_script(call_arm: &str) -> String {
    format!(
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
        send({{"jsonrpc":"2.0","id":msg["id"],"result":{{"protocolVersion":"2024-11-05","capabilities":{{}},"serverInfo":{{"name":"fake","version":"0"}}}}}})
    elif method == "tools/list":
        send({{"jsonrpc":"2.0","id":msg["id"],"result":{{"tools":[{{"name":"ping","description":"p","inputSchema":{{"type":"object"}}}}]}}}})
    elif method == "tools/call":
        {call_arm}
    else:
        send({{"jsonrpc":"2.0","id":msg["id"],"error":{{"code":-32601,"message":"no"}}}})
"#
    )
}

fn write_mcp_catalog(dir: &Path, command: &str, args: &[&str], tools: &[(&str, &str)]) {
    let mut toml = format!("[[mcp]]\nname = \"local\"\ncommand = \"{command}\"\n");
    if !args.is_empty() {
        let listed = args
            .iter()
            .map(|arg| format!("\"{arg}\""))
            .collect::<Vec<_>>()
            .join(", ");
        toml.push_str(&format!("args = [{listed}]\n"));
    }
    for (name, description) in tools {
        toml.push_str(&format!(
            "\n[[mcp.tools]]\nname = \"{name}\"\ndescription = \"{description}\"\n"
        ));
    }
    fs::write(dir.join("harness.toml"), toml).unwrap();
}

#[test]
fn mcp_jsonrpc_error_is_not_a_tool_result() {
    let dir = scratch();
    let script = dir.join("mcp.py");
    fs::write(
        &script,
        mcp_script(
            "send({\"jsonrpc\":\"2.0\",\"id\":msg[\"id\"],\"error\":{\"code\":-32000,\"message\":\"boom\"}})",
        ),
    )
    .unwrap();
    write_mcp_catalog(
        &dir,
        "python3",
        &[script.to_str().unwrap()],
        &[("ping", "p")],
    );
    let catalog = load_catalog(&dir).unwrap();
    let driver = run_call(catalog, "mcp.local.ping", "ping", "hi");
    assert_tool_failure(&driver);
}

fn failure_message(driver: &Driver) -> String {
    match &driver.state().harness {
        HarnessState::Failed { message, .. } => message.clone(),
        other => panic!("expected a failure, got {other:?}"),
    }
}

#[test]
fn mcp_jsonrpc_error_names_the_server_and_tool() {
    let dir = scratch();
    let script = dir.join("mcp.py");
    fs::write(
        &script,
        mcp_script(
            "send({\"jsonrpc\":\"2.0\",\"id\":msg[\"id\"],\"error\":{\"code\":-32602,\"message\":\"bad input\"}})",
        ),
    )
    .unwrap();
    write_mcp_catalog(
        &dir,
        "python3",
        &[script.to_str().unwrap()],
        &[("ping", "p")],
    );
    let catalog = load_catalog(&dir).unwrap();
    let driver = run_call(catalog, "mcp.local.ping", "ping", "hi");
    assert_tool_failure(&driver);
    assert_eq!(
        failure_message(&driver),
        "mcp local.ping: bad input (code -32602)"
    );
}

#[test]
fn mcp_is_error_result_is_not_a_tool_result() {
    let dir = scratch();
    let script = dir.join("mcp.py");
    fs::write(
        &script,
        mcp_script(
            "send({\"jsonrpc\":\"2.0\",\"id\":msg[\"id\"],\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"boom\"}],\"isError\":True}})",
        ),
    )
    .unwrap();
    write_mcp_catalog(
        &dir,
        "python3",
        &[script.to_str().unwrap()],
        &[("ping", "p")],
    );
    let catalog = load_catalog(&dir).unwrap();
    let driver = run_call(catalog, "mcp.local.ping", "ping", "hi");
    assert_tool_failure(&driver);
    assert_eq!(failure_message(&driver), "mcp local.ping: boom");
}

#[test]
fn two_mcp_servers_cannot_declare_one_tool_name() {
    let dir = scratch();
    fs::write(
        dir.join("harness.toml"),
        "[[mcp]]\nname = \"a\"\ncommand = \"true\"\n\n[[mcp.tools]]\nname = \"ping\"\n\n\
         [[mcp]]\nname = \"b\"\ncommand = \"true\"\n\n[[mcp.tools]]\nname = \"ping\"\n",
    )
    .unwrap();
    match load_catalog(&dir) {
        Err(LoadError::DuplicateTool(name)) => assert_eq!(name, "ping"),
        Err(other) => panic!("expected DuplicateTool, got {other:?}"),
        Ok(_) => panic!("two tools named ping were loaded"),
    }
}

#[test]
fn mcp_spawn_failure_is_not_a_tool_result() {
    let dir = scratch();
    let missing = dir.join("missing-mcp-command");
    write_mcp_catalog(&dir, missing.to_str().unwrap(), &[], &[("ping", "p")]);
    let catalog = load_catalog(&dir).unwrap();
    let driver = run_call(catalog, "mcp.local.ping", "ping", "hi");
    assert_tool_failure(&driver);
}

#[test]
fn echo_mcp_error_text_is_a_tool_result() {
    let dir = scratch();
    fs::write(dir.join("harness.toml"), "tools = [\"echo\"]\n").unwrap();
    let catalog = load_catalog(&dir).unwrap();
    let input = "mcp error: Mcp(\"x\")";
    let driver = run_call(catalog, "tool.echo", "echo", input);
    assert_eq!(tool_names(driver.events()), vec![format!("echo:{input}")]);
    assert!(matches!(
        driver.state().harness,
        HarnessState::Completed { .. }
    ));
}

fn sibling(dir: &Path, suffix: &str) -> std::path::PathBuf {
    let name = dir.file_name().unwrap().to_str().unwrap();
    dir.with_file_name(format!("{name}-{suffix}"))
}

#[test]
fn skill_dotdot_outside_is_rejected() {
    let dir = scratch();
    let leaked = sibling(&dir, "leaked.md");
    fs::write(&leaked, "leaked").unwrap();
    let rel = format!("../{}", leaked.file_name().unwrap().to_str().unwrap());
    fs::write(dir.join("harness.toml"), format!("skills = [\"{rel}\"]\n")).unwrap();
    assert!(load_catalog(&dir).is_err());
}

#[test]
fn skill_symlink_outside_is_rejected() {
    let dir = scratch();
    fs::create_dir(dir.join("skills")).unwrap();
    let outside = sibling(&dir, "outside");
    fs::create_dir(&outside).unwrap();
    let secret = outside.join("secret.md");
    fs::write(&secret, "outside secret body").unwrap();
    std::os::unix::fs::symlink(&secret, dir.join("skills/link.md")).unwrap();
    fs::write(dir.join("harness.toml"), "skills = [\"skills/link.md\"]\n").unwrap();
    match load_catalog(&dir) {
        Err(_) => {}
        Ok(catalog) => {
            assert!(
                catalog
                    .skills
                    .iter()
                    .all(|skill| skill.body != "outside secret body"),
                "loaded the outside file"
            );
            panic!("symlink skill was accepted");
        }
    }
}

#[test]
fn skill_absolute_is_rejected() {
    let dir = scratch();
    fs::create_dir(dir.join("skills")).unwrap();
    let inside = dir.join("skills/note.md");
    fs::write(&inside, "remember the rust").unwrap();
    let outside = sibling(&dir, "other.md");
    fs::write(&outside, "nope").unwrap();
    for absolute in [inside, outside] {
        fs::write(
            dir.join("harness.toml"),
            format!("skills = [\"{}\"]\n", absolute.display()),
        )
        .unwrap();
        assert!(
            load_catalog(&dir).is_err(),
            "accepted absolute skill path {}",
            absolute.display()
        );
    }
}

#[test]
fn skill_dotdot_back_inside_is_rejected() {
    let dir = scratch();
    fs::create_dir(dir.join("skills")).unwrap();
    fs::write(dir.join("skills/note.md"), "remember the rust").unwrap();
    fs::write(
        dir.join("harness.toml"),
        "skills = [\"skills/../skills/note.md\"]\n",
    )
    .unwrap();
    assert!(load_catalog(&dir).is_err());
}

#[test]
fn echo_descriptor_id_is_stable() {
    let first = EchoTool::descriptor();
    let second = EchoTool::descriptor();
    assert_eq!(first.id, second.id);
    assert_eq!(Tool::descriptor(&EchoTool).id, first.id);
}

#[test]
fn mcp_descriptor_id_is_stable() {
    let dir = scratch();
    write_mcp_catalog(&dir, "python3", &[], &[("ping", "p"), ("pong", "q")]);
    let catalog = load_catalog(&dir).unwrap();
    let first = catalog.descriptors();
    let second = catalog.descriptors();
    assert_eq!(first[0].id, second[0].id);
    assert_eq!(first[1].id, second[1].id);
    assert_ne!(first[0].id, first[1].id);
}

/// A fake MCP server that lists `listed` and answers `tools/call` with
/// `call_arm` (Python, run with `msg`, `send`, `answer` and `MARK` in scope).
fn fake_server(dir: &Path, listed: &[&str], call_arm: &str) -> std::path::PathBuf {
    let tools = listed
        .iter()
        .map(|name| format!("{{\"name\":\"{name}\",\"inputSchema\":{{\"type\":\"object\"}}}}"))
        .collect::<Vec<_>>()
        .join(",");
    fake_server_with(
        dir,
        &format!(
            "send({{\"jsonrpc\":\"2.0\",\"id\":msg[\"id\"],\"result\":{{\"tools\":[{tools}]}}}})"
        ),
        call_arm,
    )
}

/// A fake MCP server that answers `tools/list` with `list_arm` and
/// `tools/call` with `call_arm`. Each start appends a line to `starts`.
fn fake_server_with(dir: &Path, list_arm: &str, call_arm: &str) -> std::path::PathBuf {
    let script = dir.join("mcp.py");
    fs::write(
        &script,
        format!(
            r#"import json, os, sys, time
MARK = {mark:?}
with open({starts:?}, "a") as starts:
    starts.write("start\n")
def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()
def answer(msg):
    text = msg["params"]["arguments"].get("input", "")
    send({{"jsonrpc":"2.0","id":msg["id"],"result":{{"content":[{{"type":"text","text":"pong:" + text}}]}}}})
for line in sys.stdin:
    msg = json.loads(line)
    if "id" not in msg:
        continue
    method = msg.get("method")
    if method == "initialize":
        send({{"jsonrpc":"2.0","id":msg["id"],"result":{{"protocolVersion":"2024-11-05","capabilities":{{}},"serverInfo":{{"name":"fake","version":"0"}}}}}})
    elif method == "tools/list":
        {list_arm}
    elif method == "tools/call":
        {call_arm}
    else:
        send({{"jsonrpc":"2.0","id":msg["id"],"error":{{"code":-32601,"message":"no"}}}})
"#,
            mark = dir.join("called").display().to_string(),
            starts = dir.join("starts").display().to_string(),
        ),
    )
    .unwrap();
    script
}

fn starts(dir: &Path) -> usize {
    fs::read_to_string(dir.join("starts"))
        .map(|text| text.lines().count())
        .unwrap_or(0)
}

/// Writes a catalog with one `local` server run by `python3 script`, the given
/// extra server lines (such as `timeout_ms = 200`) and the given tool tables.
fn write_server_catalog(dir: &Path, script: &Path, server_extra: &str, tools: &str) {
    fs::write(
        dir.join("harness.toml"),
        format!(
            "[[mcp]]\nname = \"local\"\ncommand = \"python3\"\nargs = [\"{}\"]\n{server_extra}\n{tools}",
            script.display()
        ),
    )
    .unwrap();
}

const PING: &str = "[[mcp.tools]]\nname = \"ping\"\ndescription = \"p\"\n";

/// Calls tool `name` of a freshly loaded catalog.
fn call(catalog: &LoadedCatalog, name: &str, input: &str) -> Result<String, String> {
    let tools = catalog.tools();
    let tool = tools
        .iter()
        .find(|tool| tool.descriptor().name == name)
        .expect("declared tool");
    tool.call(input)
}

/// Aborts the test process if `work` runs longer than `limit`, so a hung MCP
/// read fails the test instead of hanging CI.
fn within<T>(limit: Duration, work: impl FnOnce() -> T) -> T {
    let (done, finished) = std::sync::mpsc::channel::<()>();
    let watchdog = thread::spawn(move || {
        if finished.recv_timeout(limit).is_err() {
            eprintln!("watchdog: MCP call ran longer than {limit:?}");
            std::process::abort();
        }
    });
    let value = work();
    let _ = done.send(());
    watchdog.join().unwrap();
    value
}

#[test]
fn notification_before_response_is_skipped() {
    let dir = scratch();
    let script = fake_server(
        &dir,
        &["ping"],
        r#"send({"jsonrpc":"2.0","method":"notifications/progress","params":{}})
        answer(msg)"#,
    );
    write_server_catalog(&dir, &script, "", PING);
    let catalog = load_catalog(&dir).unwrap();
    let output = within(Duration::from_secs(10), || call(&catalog, "ping", "hi"));
    assert_eq!(output, Ok("pong:hi".to_string()));
}

#[test]
fn a_response_with_another_id_is_skipped() {
    let dir = scratch();
    let script = fake_server(
        &dir,
        &["ping"],
        r#"send({"jsonrpc":"2.0","id":999,"result":{"content":[{"type":"text","text":"stale"}]}})
        answer(msg)"#,
    );
    write_server_catalog(&dir, &script, "", PING);
    let catalog = load_catalog(&dir).unwrap();
    let output = within(Duration::from_secs(10), || call(&catalog, "ping", "hi"));
    assert_eq!(output, Ok("pong:hi".to_string()));
}

#[test]
fn silent_server_times_out() {
    let dir = scratch();
    let script = fake_server(&dir, &["ping"], "pass");
    write_server_catalog(&dir, &script, "timeout_ms = 200", PING);
    let catalog = load_catalog(&dir).unwrap();
    let start = Instant::now();
    let output = within(Duration::from_secs(10), || call(&catalog, "ping", "hi"));
    assert_eq!(
        output,
        Err("mcp local.ping: timed out after 200 ms".to_string())
    );
    assert!(start.elapsed() < Duration::from_secs(5));
}

#[test]
fn a_silent_server_fails_the_run_as_a_tool_failure() {
    let dir = scratch();
    let script = fake_server(&dir, &["ping"], "pass");
    write_server_catalog(&dir, &script, "timeout_ms = 200", PING);
    let catalog = load_catalog(&dir).unwrap();
    let driver = within(Duration::from_secs(10), || {
        run_call(catalog, "mcp.local.ping", "ping", "hi")
    });
    assert_tool_failure(&driver);
    assert_eq!(
        failure_message(&driver),
        "mcp local.ping: timed out after 200 ms"
    );
}

#[test]
fn oversized_line_is_error() {
    let dir = scratch();
    let script = fake_server(
        &dir,
        &["ping"],
        r#"sys.stdout.write("x" * (2 * 1024 * 1024))
        sys.stdout.flush()
        time.sleep(30)"#,
    );
    write_server_catalog(&dir, &script, "", PING);
    let catalog = load_catalog(&dir).unwrap();
    let output = within(Duration::from_secs(10), || call(&catalog, "ping", "hi"));
    assert_eq!(output, Err("mcp local.ping: line over 1 MiB".to_string()));
}

// The first server hangs for good. After its call times out the session is
// dropped, so the second call starts a new server instead of waiting on (or
// reading a late answer from) the old one.
#[test]
fn a_failed_session_is_restarted() {
    let dir = scratch();
    let script = fake_server(
        &dir,
        &["ping"],
        r#"if not os.path.exists(MARK):
            open(MARK, "w").close()
            time.sleep(3600)
        answer(msg)"#,
    );
    write_server_catalog(&dir, &script, "timeout_ms = 300", PING);
    let catalog = load_catalog(&dir).unwrap();
    let (first, second) = within(Duration::from_secs(10), || {
        (call(&catalog, "ping", "one"), call(&catalog, "ping", "two"))
    });
    assert_eq!(
        first,
        Err("mcp local.ping: timed out after 300 ms".to_string())
    );
    assert_eq!(second, Ok("pong:two".to_string()));
}

#[test]
fn declared_schema_is_on_the_descriptor() {
    let dir = scratch();
    let script = fake_server(&dir, &["ping", "plain"], "answer(msg)");
    write_server_catalog(
        &dir,
        &script,
        "",
        "[[mcp.tools]]\nname = \"ping\"\ndescription = \"p\"\ninput_schema = '{\"type\":\"string\"}'\n\n[[mcp.tools]]\nname = \"plain\"\n",
    );
    let catalog = load_catalog(&dir).unwrap();
    let schemas: Vec<(String, String)> = catalog
        .descriptors()
        .into_iter()
        .map(|descriptor| (descriptor.name, descriptor.input_schema))
        .collect();
    assert_eq!(
        schemas,
        [
            ("ping".to_string(), "{\"type\":\"string\"}".to_string()),
            ("plain".to_string(), "{\"type\":\"object\"}".to_string()),
        ]
    );
}

#[test]
fn a_tool_the_server_does_not_list_fails_clearly() {
    let dir = scratch();
    let script = fake_server(&dir, &["ping"], "answer(msg)");
    write_server_catalog(
        &dir,
        &script,
        "",
        "[[mcp.tools]]\nname = \"ping\"\n\n[[mcp.tools]]\nname = \"pong\"\n",
    );
    let catalog = load_catalog(&dir).unwrap();
    let (listed, unlisted) = within(Duration::from_secs(10), || {
        (call(&catalog, "ping", "hi"), call(&catalog, "pong", "hi"))
    });
    assert_eq!(listed, Ok("pong:hi".to_string()));
    assert_eq!(
        unlisted,
        Err("mcp local.pong: not listed by the server".to_string())
    );
}

// MCP lets a server send its own requests (a ping, say) at any time, numbered
// in its own id space. One that happens to carry our id is not our answer.
#[test]
fn a_server_request_with_our_id_is_skipped() {
    let dir = scratch();
    let script = fake_server(
        &dir,
        &["ping"],
        r#"send({"jsonrpc":"2.0","id":msg["id"],"method":"ping"})
        answer(msg)"#,
    );
    write_server_catalog(&dir, &script, "", PING);
    let catalog = load_catalog(&dir).unwrap();
    let output = within(Duration::from_secs(10), || call(&catalog, "ping", "hi"));
    assert_eq!(output, Ok("pong:hi".to_string()));
}

// The timeout bounds the whole request: notifications arriving more often than
// the timeout do not keep a call that is never answered alive.
#[test]
fn notifications_do_not_extend_the_timeout() {
    let dir = scratch();
    let script = fake_server(
        &dir,
        &["ping"],
        r#"while True:
            send({"jsonrpc":"2.0","method":"notifications/progress","params":{}})
            time.sleep(0.1)"#,
    );
    write_server_catalog(&dir, &script, "timeout_ms = 300", PING);
    let catalog = load_catalog(&dir).unwrap();
    let start = Instant::now();
    let output = within(Duration::from_secs(10), || call(&catalog, "ping", "hi"));
    assert_eq!(
        output,
        Err("mcp local.ping: timed out after 300 ms".to_string())
    );
    assert!(start.elapsed() < Duration::from_secs(5));
}

// The server answered, with an error: the session is sound, so the next call
// reuses it instead of starting the server again.
#[test]
fn an_answered_error_keeps_the_session() {
    let dir = scratch();
    let script = fake_server(
        &dir,
        &["ping"],
        r#"if msg["params"]["arguments"].get("input") == "bad":
            send({"jsonrpc":"2.0","id":msg["id"],"error":{"code":-32000,"message":"bad"}})
        else:
            answer(msg)"#,
    );
    write_server_catalog(&dir, &script, "", PING);
    let catalog = load_catalog(&dir).unwrap();
    let (first, second) = within(Duration::from_secs(10), || {
        (call(&catalog, "ping", "bad"), call(&catalog, "ping", "ok"))
    });
    assert_eq!(first, Err("mcp local.ping: bad (code -32000)".to_string()));
    assert_eq!(second, Ok("pong:ok".to_string()));
    assert_eq!(starts(&dir), 1);
}

#[test]
fn a_tools_list_error_names_tools_list() {
    let dir = scratch();
    let script = fake_server_with(
        &dir,
        r#"send({"jsonrpc":"2.0","id":msg["id"],"error":{"code":-32601,"message":"no tools"}})"#,
        "answer(msg)",
    );
    write_server_catalog(&dir, &script, "", PING);
    let catalog = load_catalog(&dir).unwrap();
    let output = within(Duration::from_secs(10), || call(&catalog, "ping", "hi"));
    assert_eq!(
        output,
        Err("mcp local.ping: tools/list: no tools (code -32601)".to_string())
    );
}

#[test]
fn a_malformed_tools_list_is_an_error() {
    let dir = scratch();
    let script = fake_server_with(
        &dir,
        r#"send({"jsonrpc":"2.0","id":msg["id"],"result":{"tools":"ping"}})"#,
        "answer(msg)",
    );
    write_server_catalog(&dir, &script, "", PING);
    let catalog = load_catalog(&dir).unwrap();
    let output = within(Duration::from_secs(10), || call(&catalog, "ping", "hi"));
    assert_eq!(
        output,
        Err("mcp local.ping: tools/list: malformed result".to_string())
    );
}

#[test]
fn tools_list_pages_are_followed() {
    let dir = scratch();
    let script = fake_server_with(
        &dir,
        r#"if msg.get("params", {}).get("cursor") is None:
            send({"jsonrpc":"2.0","id":msg["id"],"result":{"tools":[{"name":"ping"}],"nextCursor":"2"}})
        else:
            send({"jsonrpc":"2.0","id":msg["id"],"result":{"tools":[{"name":"pong"}]}})"#,
        "answer(msg)",
    );
    write_server_catalog(
        &dir,
        &script,
        "",
        "[[mcp.tools]]\nname = \"ping\"\n\n[[mcp.tools]]\nname = \"pong\"\n",
    );
    let catalog = load_catalog(&dir).unwrap();
    let output = within(Duration::from_secs(10), || call(&catalog, "pong", "hi"));
    assert_eq!(output, Ok("pong:hi".to_string()));
}

#[test]
fn a_zero_timeout_is_rejected_at_load() {
    let dir = scratch();
    let script = fake_server(&dir, &["ping"], "answer(msg)");
    write_server_catalog(&dir, &script, "timeout_ms = 0", PING);
    let error = load_catalog(&dir).err().expect("a zero timeout is refused");
    assert_eq!(error.to_string(), "local: timeout_ms must be at least 1");
    assert_eq!(starts(&dir), 0);
}
