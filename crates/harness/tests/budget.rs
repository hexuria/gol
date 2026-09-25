//! The run loop ends through the budget. This is the Rust owner of the
//! "every run eventually finishes" property: a decider that never completes is
//! stopped by `limits.max_steps`, and the run fails with `FailureClass::Budget`.

use harness::{
    run_to_completion, Driver, EchoTool, InMemory, ScriptedDecider, Tool, UnavailableModel,
};
use protocol::{
    AgentId, Capability, CredentialSource, Effect, EventPayload, ExecutionPlacement, FailureClass,
    HarnessState, InvocationId, Limits, ModelProvider, RunSpec, WorkModel,
};

fn run(max_steps: u32, effects: Vec<Effect>) -> Driver {
    let spec = RunSpec::builder()
        .agent(AgentId::new(), "1")
        .input("hi")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::Anthropic,
            model_name: "m".into(),
            credential: CredentialSource::PlatformGateway,
        })
        .capabilities(vec![Capability::new("tool.echo")])
        .limits(Limits {
            max_steps,
            max_model_calls: 4,
        })
        .build();
    let mut driver = Driver::boot(spec).unwrap();
    let mut decider = ScriptedDecider::new(effects);
    let echo = EchoTool;
    let tools: [&dyn Tool; 1] = [&echo];
    run_to_completion(
        &mut driver,
        &mut decider,
        &tools,
        &UnavailableModel,
        &mut InMemory::default(),
    )
    .unwrap();
    driver
}

fn echo() -> Effect {
    Effect::ToolCall {
        name: "echo".into(),
        input: "x".into(),
        invocation: InvocationId::new(),
    }
}

fn budget_failures(driver: &Driver) -> usize {
    driver
        .events()
        .iter()
        .filter(|event| {
            matches!(
                event.payload,
                EventPayload::RunFailed {
                    class: FailureClass::Budget,
                    ..
                }
            )
        })
        .count()
}

#[test]
fn a_decider_that_never_completes_is_stopped_by_the_step_budget() {
    let driver = run(3, (0..50).map(|_| echo()).collect());
    assert!(matches!(
        driver.state().harness,
        HarnessState::Failed {
            class: FailureClass::Budget,
            ..
        }
    ));
    assert_eq!(budget_failures(&driver), 1);
    assert_eq!(driver.state().steps, 3);
}

#[test]
fn a_run_inside_its_budget_completes() {
    let driver = run(
        2,
        vec![
            echo(),
            Effect::Complete {
                outcome: "done".into(),
            },
        ],
    );
    assert_eq!(
        driver.state().harness,
        HarnessState::Completed {
            outcome: "done".into()
        }
    );
    assert_eq!(budget_failures(&driver), 0);
}

#[test]
fn a_run_one_decision_over_its_budget_fails() {
    let driver = run(
        1,
        vec![
            echo(),
            Effect::Complete {
                outcome: "done".into(),
            },
        ],
    );
    assert!(matches!(
        driver.state().harness,
        HarnessState::Failed {
            class: FailureClass::Budget,
            ..
        }
    ));
}
