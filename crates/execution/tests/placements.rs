use std::thread;

use execution::{run_box, run_reverse};
use harness::ScriptedDecider;
use protocol::{
    Capability, CredentialSource, Effect, EventPayload, ExecutionPlacement, HarnessState, Limits,
    ModelProvider, RunSpec, WorkModel,
};

fn spec(placement: ExecutionPlacement) -> RunSpec {
    RunSpec::builder()
        .agent(protocol::AgentId::new(), "1")
        .input("hello")
        .placement(placement)
        .work_model(WorkModel {
            provider: ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .capabilities(vec![Capability::new("tool.echo")])
        .limits(Limits {
            max_steps: 8,
            max_model_calls: 4,
        })
        .build()
}

fn script() -> ScriptedDecider {
    ScriptedDecider::new([
        Effect::ToolCall {
            name: "other".to_string(),
            input: "nope".to_string(),
        },
        Effect::ToolCall {
            name: "echo".to_string(),
            input: "hello".to_string(),
        },
        Effect::Complete {
            outcome: "done".to_string(),
        },
    ])
}

#[test]
fn reverse_worker_runs_echo_off_the_caller_thread() {
    let caller = thread::current().id();
    let spec = spec(ExecutionPlacement::Reverse);
    let run = run_reverse(spec.clone(), script()).expect("reverse");
    assert_ne!(run.worker_thread, caller);
    assert!(run.events.iter().any(|event| matches!(
        &event.payload,
        EventPayload::EffectDenied { effect, .. } if matches!(effect, Effect::ToolCall { name, .. } if name == "other")
    )));
    assert!(run.events.iter().any(|event| matches!(
        &event.payload,
        EventPayload::ToolResult { name, output } if name == "echo" && output == "hello"
    )));
    let folded = protocol::fold(&spec, &run.events);
    assert!(matches!(folded.harness, HarnessState::Completed { .. }));
}

#[test]
fn box_worker_runs_only_the_boxed_echo_tool() {
    let caller = thread::current().id();
    let spec = spec(ExecutionPlacement::Box);
    let run = run_box(spec.clone(), script()).expect("box");
    assert_ne!(run.worker_thread, caller);
    assert!(run.events.iter().any(|event| matches!(
        &event.payload,
        EventPayload::ToolResult { name, output } if name == "echo" && output == "hello"
    )));
    assert!(!run.events.iter().any(|event| matches!(
        &event.payload,
        EventPayload::ToolResult { name, .. } if name == "other"
    )));
    assert_eq!(
        protocol::fold(&spec, &run.events).harness,
        HarnessState::Completed {
            outcome: "done".to_string()
        }
    );
}
