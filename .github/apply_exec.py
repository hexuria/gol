from pathlib import Path
import subprocess


def sub(path, old, new):
    file = Path(path)
    text = file.read_text()
    if old not in text:
        raise SystemExit(f"missing pattern in {path}")
    file.write_text(text.replace(old, new, 1))


sub(
    "README.md",
    "`Reverse` and `Box` are valid placements on the spec. The server responds `422` and does not run the loop.",
    "`Reverse` and `Box` run the same loop as `Local`. The execution crate runs each placement on a worker thread.",
)
sub(
    "docs/architecture.md",
    "The execution plane is local and in-process. `Reverse` and `Box` type-check and stop at the execution boundary. One echo tool is registered.",
    "The execution plane runs `Local`, `Reverse`, and `Box`. Reverse and box run on a worker thread with the echo tool.",
)
sub(
    "docs/harness-runtime.md",
    "`ExecutionPlacement::Reverse` and `ExecutionPlacement::Box` type-check on `RunSpec`. The execution boundary returns `ExecuteError::UnsupportedPlacement` and does not run the loop.",
    "`ExecutionPlacement::Reverse` and `ExecutionPlacement::Box` boot the same harness loop as `Local`. The execution crate runs each on its own worker thread.",
)
sub(
    "crates/harness/src/driver.rs",
    "            ExecutionPlacement::Local => {}\n            placement => return Err(BootError::UnsupportedPlacement(placement)),",
    "            ExecutionPlacement::Local | ExecutionPlacement::Reverse | ExecutionPlacement::Box => {}",
)
sub(
    "crates/harness/src/driver.rs",
    """    fn reverse_and_box_do_not_boot() {
        let reverse = spec_with(ExecutionPlacement::Reverse, Vec::new());
        let boxed = spec_with(ExecutionPlacement::Box, Vec::new());
        assert_eq!(
            Driver::boot(reverse).err(),
            Some(BootError::UnsupportedPlacement(ExecutionPlacement::Reverse))
        );
        assert_eq!(
            Driver::boot(boxed).err(),
            Some(BootError::UnsupportedPlacement(ExecutionPlacement::Box))
        );
    }""",
    """    fn reverse_and_box_boot_into_running() {
        for placement in [ExecutionPlacement::Reverse, ExecutionPlacement::Box] {
            let driver = Driver::boot(spec_with(placement, Vec::new())).unwrap();
            assert!(matches!(
                driver.state().harness,
                HarnessState::Running { .. }
            ));
        }
    }""",
)
sub(
    "crates/server/tests/http_run.rs",
    "use protocol::{AgentId, Capability, EventPayload, HarnessState, Limits};",
    "use protocol::{AgentId, Capability, EventPayload, HarnessState};",
)
http = Path("crates/server/tests/http_run.rs")
text = http.read_text()
start = text.index("async fn reverse_placement_does_not_run()")
end = text.index("#[tokio::test]\nasync fn run_calls_jev_system_one()")
replacement = """async fn reverse_and_box_placements_complete() {
    for placement in ["Reverse", "Box"] {
        let jev = jev_mock().await;
        let app = router(Arc::new(InMemoryStore::default()), jev.uri());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        let client = reqwest::Client::new();
        let response = post_when_up(
            &client,
            &format!("http://{addr}/v1/runs"),
            &serde_json::json!({
                "agent_id": AgentId::new(),
                "agent_version": "1",
                "input": "hello",
                "placement": placement,
                "work_model": {
                    "provider": "OpenAI",
                    "model_name": "gpt-test",
                    "credential": "PlatformGateway"
                },
                "capabilities": ["tool.echo"],
                "limits": { "max_steps": 8, "max_model_calls": 4 }
            }),
        )
        .await;
        assert!(response.status().is_success(), "{placement}");
        let created: protocol::RunState = response.json().await.expect("run json");
        assert_eq!(
            created.harness,
            HarnessState::Completed {
                outcome: "done".to_string()
            }
        );
    }
}

"""
http.write_text(text[:start] + replacement + text[end:])
lock = Path("Cargo.lock")
lock_text = lock.read_text()
needle = '[[package]]\nname = "fallible-iterator"\n'
insert = '[[package]]\nname = "execution"\nversion = "0.1.0"\ndependencies = [\n "harness",\n "protocol",\n]\n\n'
if 'name = "execution"' not in lock_text:
    if needle not in lock_text:
        raise SystemExit("lock anchor missing")
    lock.write_text(lock_text.replace(needle, insert + needle, 1))

expected = {
    "crates/harness/src/driver.rs": "7c96c112498ac4f03c7dedec417d208b19301eb1",
    "crates/server/tests/http_run.rs": "bb69c24ca759108e237d728678aedd034ea160ab",
    "README.md": "42fbd9a226d44bab3eee8675fabf148a3ff9e1c3",
    "docs/architecture.md": "1cd4d18f2c412c5304ff10136bb99ce6e820a2c4",
    "docs/harness-runtime.md": "bfe72427dfc8e438b6659af909565252d3e2c775",
    "Cargo.lock": "895f8e7c29457557928dc4015d841d029596bd4a",
}
for path, sha in expected.items():
    actual = subprocess.check_output(["git", "hash-object", path], text=True).strip()
    print(path, actual)
    if actual != sha:
        raise SystemExit(f"hash mismatch {path}")
