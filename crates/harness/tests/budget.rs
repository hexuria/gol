//! The run loop ends through the budget. This is the Rust owner of the
//! "every run eventually finishes" property: a decider that never completes is
//! stopped by `limits.max_steps`, and the run fails with `FailureClass::Budget`.
//!
//! A `Complete` that finishes the run is always allowed and never counts
//! against either budget, so a run that has spent its budget can still finish.
//! Every other decision counts, including a `Complete` while a tool call is
//! outstanding (it cannot finish the run), which is what stops a decider that
//! never completes.

use harness::{
    run_to_completion, Decider, DeciderError, DecisionView, Driver, EchoTool, InMemory,
    ModelCompletion, ScriptedDecider, Tool, UnavailableModel,
};
use protocol::{
    AgentId, Capability, CredentialSource, Effect, EventPayload, ExecutionPlacement, FailureClass,
    HarnessState, InvocationId, Limits, MessageRole, ModelMessage, ModelProvider, ModelRequest,
    RunSpec, WorkModel,
};

/// A model that always answers.
struct Answering;

impl ModelCompletion for Answering {
    fn complete(&self, _request: &ModelRequest) -> Result<ModelMessage, String> {
        Ok(ModelMessage {
            role: MessageRole::Assistant,
            text: "ok".into(),
        })
    }
}

fn run(max_steps: u32, effects: Vec<Effect>) -> Driver {
    run_with(
        Limits {
            max_steps,
            max_model_calls: 4,
        },
        effects,
        &UnavailableModel,
    )
}

fn run_with(limits: Limits, effects: Vec<Effect>, models: &dyn ModelCompletion) -> Driver {
    let spec = RunSpec::builder()
        .agent(AgentId::new(), "1")
        .input("hi")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::Anthropic,
            model_name: "m".into(),
            credential: CredentialSource::PlatformGateway,
        })
        .capabilities(vec![
            Capability::new("tool.echo"),
            Capability::new("model.call"),
        ])
        .limits(limits)
        .build();
    let mut driver = Driver::boot(spec).unwrap();
    let mut decider = ScriptedDecider::new(effects);
    let echo = EchoTool;
    let tools: [&dyn Tool; 1] = [&echo];
    run_to_completion(
        &mut driver,
        &mut decider,
        &tools,
        models,
        &mut InMemory::default(),
    )
    .unwrap();
    driver
}

fn complete() -> Effect {
    Effect::Complete {
        outcome: "done".into(),
    }
}

fn model() -> Effect {
    Effect::ModelCall { prompt: "p".into() }
}

fn completed(driver: &Driver) -> bool {
    driver.state().harness
        == HarnessState::Completed {
            outcome: "done".into(),
        }
}

fn failed_on_budget(driver: &Driver) -> bool {
    matches!(
        driver.state().harness,
        HarnessState::Failed {
            class: FailureClass::Budget,
            ..
        }
    )
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

// Owner decision: Complete is always allowed. This replaces
// a_run_one_decision_over_its_budget_fails, which asserted the opposite.
#[test]
fn a_run_at_its_step_limit_may_still_complete() {
    let driver = run(1, vec![echo(), complete()]);
    assert!(completed(&driver), "{:?}", driver.state().harness);
    assert_eq!(budget_failures(&driver), 0);
    assert_eq!(driver.state().steps, 1);
}

#[test]
fn a_run_with_no_steps_may_still_complete() {
    let driver = run(0, vec![complete()]);
    assert!(completed(&driver), "{:?}", driver.state().harness);
    assert_eq!(driver.state().steps, 0);
}

#[test]
fn a_second_non_complete_decision_over_the_limit_fails() {
    let driver = run(1, vec![echo(), echo(), complete()]);
    assert!(failed_on_budget(&driver), "{:?}", driver.state().harness);
    assert_eq!(budget_failures(&driver), 1);
    assert_eq!(driver.state().steps, 1);
}

#[test]
fn model_then_complete_within_one_model_call() {
    let limits = Limits {
        max_steps: 8,
        max_model_calls: 1,
    };
    let driver = run_with(limits, vec![model(), complete()], &Answering);
    assert!(completed(&driver), "{:?}", driver.state().harness);
    assert_eq!(driver.state().model_calls, 1);
}

#[test]
fn a_model_call_past_the_model_limit_fails() {
    let limits = Limits {
        max_steps: 8,
        max_model_calls: 1,
    };
    let driver = run_with(limits, vec![model(), model(), complete()], &Answering);
    assert!(failed_on_budget(&driver), "{:?}", driver.state().harness);
    assert_eq!(budget_failures(&driver), 1);
    assert_eq!(driver.state().model_calls, 1);
}

// A tool call is not a model call: the model budget does not stop it.
#[test]
fn a_tool_call_after_the_model_limit_is_allowed() {
    let limits = Limits {
        max_steps: 8,
        max_model_calls: 1,
    };
    let driver = run_with(limits, vec![model(), echo(), complete()], &Answering);
    assert!(completed(&driver), "{:?}", driver.state().harness);
}

/// Always decides Complete, and gives up after `limit` calls so a run that
/// never ends fails the test instead of hanging it.
struct AlwaysComplete {
    calls: usize,
    limit: usize,
}

impl Decider for AlwaysComplete {
    fn decide(&mut self, _view: &DecisionView<'_>) -> Result<Effect, DeciderError> {
        self.calls += 1;
        if self.calls > self.limit {
            return Err(DeciderError {
                message: "the run did not end".into(),
            });
        }
        Ok(complete())
    }
}

// A Complete is free only when it finishes the run. While a tool call is
// outstanding it cannot, so it costs a step like any other decision, and the
// step budget still ends a decider that only ever says Complete.
#[test]
fn a_complete_that_cannot_finish_the_run_costs_a_step() {
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
            max_steps: 2,
            max_model_calls: 4,
        })
        .build();
    let mut driver = Driver::boot(spec).unwrap();
    let echo_tool = EchoTool;
    let descriptors = [echo_tool.descriptor()];
    // Decide a tool call and leave it unanswered: the run waits for the tool.
    driver
        .decide(&mut ScriptedDecider::new(vec![echo()]), &descriptors)
        .unwrap();
    assert!(matches!(
        driver.state().harness,
        HarnessState::WaitingForTool { .. }
    ));

    let mut decider = AlwaysComplete {
        calls: 0,
        limit: 100,
    };
    let tools: [&dyn Tool; 1] = [&echo_tool];
    let ended = run_to_completion(
        &mut driver,
        &mut decider,
        &tools,
        &UnavailableModel,
        &mut InMemory::default(),
    );
    assert!(ended.is_ok(), "{ended:?}");
    assert!(failed_on_budget(&driver), "{:?}", driver.state().harness);
    assert_eq!(driver.state().steps, 2);
    assert_eq!(decider.calls, 2);
}

/// Records the budget flags of each view it is shown.
struct Watching {
    script: ScriptedDecider,
    seen: Vec<(bool, bool)>,
}

impl Decider for Watching {
    fn decide(&mut self, view: &DecisionView<'_>) -> Result<Effect, DeciderError> {
        self.seen
            .push((view.steps_exhausted, view.model_calls_exhausted));
        self.script.decide(view)
    }
}

// A decider is told which budget is spent: after the last model call, tool
// calls are still allowed, so the step budget is not reported as spent.
#[test]
fn a_decider_sees_which_budget_is_spent() {
    let spec = RunSpec::builder()
        .agent(AgentId::new(), "1")
        .input("hi")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::Anthropic,
            model_name: "m".into(),
            credential: CredentialSource::PlatformGateway,
        })
        .capabilities(vec![
            Capability::new("tool.echo"),
            Capability::new("model.call"),
        ])
        .limits(Limits {
            max_steps: 2,
            max_model_calls: 1,
        })
        .build();
    let mut driver = Driver::boot(spec).unwrap();
    let mut decider = Watching {
        script: ScriptedDecider::new(vec![model(), echo(), complete()]),
        seen: Vec::new(),
    };
    let echo_tool = EchoTool;
    let tools: [&dyn Tool; 1] = [&echo_tool];
    run_to_completion(
        &mut driver,
        &mut decider,
        &tools,
        &Answering,
        &mut InMemory::default(),
    )
    .unwrap();
    assert!(completed(&driver), "{:?}", driver.state().harness);
    assert_eq!(decider.seen, [(false, false), (false, true), (true, true)]);
}
