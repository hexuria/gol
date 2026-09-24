use protocol::{
    authorize, fold, Actor, Effect, Event, EventPayload, ExecutionPlacement, FailureClass,
    HarnessState, PolicyDecision, RunSpec, RunState, Timestamp, ToolDescriptor,
};

use crate::{Decider, DeciderError, DecisionView, Memory, ModelCompletion, Tool};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootError {
    UnsupportedPlacement(ExecutionPlacement),
}

pub struct Driver {
    spec: RunSpec,
    events: Vec<Event>,
}

impl Driver {
    pub fn boot(spec: RunSpec) -> Result<Self, BootError> {
        match spec.placement {
            ExecutionPlacement::Local => {}
            placement => return Err(BootError::UnsupportedPlacement(placement)),
        }
        let mut driver = Self {
            spec,
            events: Vec::new(),
        };
        driver.push(EventPayload::RunStarted, Actor::System);
        Ok(driver)
    }

    pub fn events(&self) -> &[Event] {
        &self.events
    }

    pub fn state(&self) -> RunState {
        fold(&self.spec, &self.events)
    }

    pub fn cancel(&mut self) {
        self.push(EventPayload::RunCancelled, Actor::System);
    }

    pub fn retry(&mut self) {
        self.push(EventPayload::StepRetried, Actor::System);
    }

    pub fn deliver_tool_result(
        &mut self,
        name: impl Into<String>,
        output: impl Into<String>,
    ) -> bool {
        if !matches!(self.state().harness, HarnessState::WaitingForTool { .. }) {
            return false;
        }
        self.push(
            EventPayload::ToolResult {
                name: name.into(),
                output: output.into(),
            },
            Actor::Tool,
        );
        true
    }

    pub fn decide(
        &mut self,
        decider: &mut dyn Decider,
        tools: &[ToolDescriptor],
    ) -> Result<Vec<Effect>, DeciderError> {
        let state = self.state();
        if state.harness.is_terminal() {
            return Ok(Vec::new());
        }
        if state.steps >= self.spec.limits.max_steps
            || state.model_calls >= self.spec.limits.max_model_calls
        {
            self.push(
                EventPayload::RunFailed {
                    class: FailureClass::Budget,
                    message: "limit hit".to_string(),
                },
                Actor::System,
            );
            return Ok(Vec::new());
        }

        let effect = {
            let view = DecisionView {
                spec: &self.spec,
                state: &state,
                events: &self.events,
                tools,
            };
            decider.decide(&view)?
        };
        self.push(
            EventPayload::EffectDecided {
                effect: effect.clone(),
            },
            Actor::Agent,
        );

        match authorize(&self.spec, &effect, tools) {
            PolicyDecision::Allow => {
                self.push(
                    EventPayload::EffectAuthorized {
                        effect: effect.clone(),
                    },
                    Actor::Policy,
                );
                if let Effect::Complete { outcome } = &effect {
                    if matches!(self.state().harness, HarnessState::Completed { .. }) {
                        self.push(
                            EventPayload::RunCompleted {
                                outcome: outcome.clone(),
                            },
                            Actor::System,
                        );
                    }
                }
                Ok(effects_of_last(&self.spec, &self.events))
            }
            PolicyDecision::Deny { reason } => {
                self.push(EventPayload::EffectDenied { effect, reason }, Actor::Policy);
                Ok(Vec::new())
            }
            PolicyDecision::RequireApproval
            | PolicyDecision::Modify
            | PolicyDecision::Limit
            | PolicyDecision::Redirect => {
                self.push(
                    EventPayload::EffectDenied {
                        effect,
                        reason: "effect is not implemented in this slice".to_string(),
                    },
                    Actor::Policy,
                );
                Ok(Vec::new())
            }
        }
    }

    pub fn perform(
        &mut self,
        effects: &[Effect],
        tools: &[&dyn Tool],
        models: &dyn ModelCompletion,
        memory: &mut dyn Memory,
    ) {
        for effect in effects {
            match effect {
                Effect::ToolCall { name, input } => {
                    if !matches!(self.state().harness, HarnessState::WaitingForTool { .. }) {
                        continue;
                    }
                    let Some(tool) = tools.iter().find(|tool| tool.descriptor().name == *name)
                    else {
                        self.push(
                            EventPayload::RunFailed {
                                class: FailureClass::Tool,
                                message: format!("unknown tool: {name}"),
                            },
                            Actor::System,
                        );
                        return;
                    };
                    let output = tool.call(input);
                    self.deliver_tool_result(name, output);
                }
                Effect::ModelCall { prompt } => {
                    let request = protocol::ModelRequest {
                        provider: self.spec.work_model.provider,
                        model_name: self.spec.work_model.model_name.clone(),
                        prompt: prompt.clone(),
                    };
                    match models.complete(&request) {
                        Ok(message) => {
                            self.push(EventPayload::ModelResponded { message }, Actor::Gateway)
                        }
                        Err(message) => self.push(
                            EventPayload::RunFailed {
                                class: FailureClass::Dependency,
                                message,
                            },
                            Actor::Gateway,
                        ),
                    }
                }
                Effect::MemoryRead { scope, key } => {
                    let value = memory.read(*scope, key);
                    self.push(
                        EventPayload::MemoryRead {
                            scope: *scope,
                            key: key.clone(),
                            value,
                        },
                        Actor::System,
                    );
                }
                Effect::MemoryWrite { scope, key, value } => {
                    memory.write(*scope, key, value);
                    self.push(
                        EventPayload::MemoryWritten {
                            scope: *scope,
                            key: key.clone(),
                            value: value.clone(),
                        },
                        Actor::System,
                    );
                }
                Effect::Complete { .. }
                | Effect::Execute { .. }
                | Effect::Delegate { .. }
                | Effect::AskUser { .. }
                | Effect::RequestApproval { .. }
                | Effect::Wait { .. }
                | Effect::PublishArtifact { .. } => {}
            }
        }
    }

    fn push(&mut self, payload: EventPayload, actor: Actor) {
        self.events.push(Event::record(
            self.spec.run_id,
            self.spec.agent_id,
            &self.spec.agent_version,
            None,
            actor,
            None,
            Timestamp::now(),
            payload,
        ));
    }
}

pub fn run_to_completion(
    driver: &mut Driver,
    decider: &mut dyn Decider,
    tools: &[&dyn Tool],
    models: &dyn ModelCompletion,
    memory: &mut dyn Memory,
) -> Result<(), DeciderError> {
    let descriptors: Vec<ToolDescriptor> = tools.iter().map(|tool| tool.descriptor()).collect();
    loop {
        if driver.state().harness.is_terminal() {
            return Ok(());
        }
        let effects = driver.decide(decider, &descriptors)?;
        if effects.is_empty() {
            if driver.state().harness.is_terminal() {
                return Ok(());
            }
            continue;
        }
        driver.perform(&effects, tools, models, memory);
    }
}

fn effects_of_last(spec: &RunSpec, events: &[Event]) -> Vec<Effect> {
    let Some(last) = events.last() else {
        return Vec::new();
    };
    let prior = fold(spec, &events[..events.len() - 1]);
    reduce_effects(prior.harness, last)
}

fn reduce_effects(state: HarnessState, event: &Event) -> Vec<Effect> {
    protocol::reduce(state, event).1
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EchoTool, InMemory, ScriptedDecider, Tool, UnavailableModel};
    use protocol::{
        AgentId, Capability, CredentialSource, DispatchPhase, ExecutionPlacement, Limits,
        ModelProvider, RunSpec, WorkModel,
    };

    fn spec_with(placement: ExecutionPlacement, capabilities: Vec<Capability>) -> RunSpec {
        RunSpec::builder()
            .agent(AgentId::new(), "1")
            .input("hello")
            .placement(placement)
            .work_model(WorkModel {
                provider: ModelProvider::OpenAI,
                model_name: "gpt-test".to_string(),
                credential: CredentialSource::PlatformGateway,
            })
            .capabilities(capabilities)
            .limits(Limits {
                max_steps: 8,
                max_model_calls: 4,
            })
            .build()
    }

    fn local_echo() -> RunSpec {
        spec_with(
            ExecutionPlacement::Local,
            vec![Capability::new("tool.echo")],
        )
    }

    fn tool_call() -> Effect {
        Effect::ToolCall {
            name: "echo".to_string(),
            input: "hello".to_string(),
        }
    }

    fn complete() -> Effect {
        Effect::Complete {
            outcome: "done".to_string(),
        }
    }

    fn tool_results(events: &[Event]) -> Vec<&Event> {
        events
            .iter()
            .filter(|event| matches!(event.payload, EventPayload::ToolResult { .. }))
            .collect()
    }

    struct PanicTool;

    impl Tool for PanicTool {
        fn descriptor(&self) -> ToolDescriptor {
            EchoTool::descriptor()
        }

        fn call(&self, _input: &str) -> String {
            panic!("tool executed");
        }
    }

    #[test]
    fn echo_then_complete_finishes_completed() {
        let mut driver = Driver::boot(local_echo()).unwrap();
        let mut decider = ScriptedDecider::new([tool_call(), complete()]);
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
        let state = driver.state();
        assert_eq!(
            state.harness,
            HarnessState::Completed {
                outcome: "done".to_string()
            }
        );
        assert_eq!(
            state.dispatch,
            DispatchPhase::Completed {
                outcome: "done".to_string()
            }
        );
        assert_eq!(tool_results(driver.events()).len(), 1);
        match &tool_results(driver.events())[0].payload {
            EventPayload::ToolResult { name, output } => {
                assert_eq!(name, "echo");
                assert_eq!(output, "hello");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn missing_capability_denies_and_does_not_execute() {
        let spec = spec_with(ExecutionPlacement::Local, Vec::new());
        let mut driver = Driver::boot(spec).unwrap();
        let mut decider = ScriptedDecider::new([tool_call(), complete()]);
        let tool = PanicTool;
        let tools: [&dyn Tool; 1] = [&tool];
        run_to_completion(
            &mut driver,
            &mut decider,
            &tools,
            &UnavailableModel,
            &mut InMemory::default(),
        )
        .unwrap();
        assert!(driver.events().iter().any(|event| {
            matches!(
                &event.payload,
                EventPayload::EffectDenied { reason, .. } if reason.contains("missing capability")
            )
        }));
        assert!(tool_results(driver.events()).is_empty());
        assert_eq!(
            driver.state().harness,
            HarnessState::Completed {
                outcome: "done".to_string()
            }
        );
    }

    #[test]
    fn duplicate_tool_result_is_not_appended() {
        let mut driver = Driver::boot(local_echo()).unwrap();
        let mut decider = ScriptedDecider::new([tool_call()]);
        let echo = EchoTool;
        let descriptors = [echo.descriptor()];
        let effects = driver.decide(&mut decider, &descriptors).unwrap();
        driver.perform(
            &effects,
            &[&echo],
            &UnavailableModel,
            &mut InMemory::default(),
        );
        assert_eq!(tool_results(driver.events()).len(), 1);
        assert!(!driver.deliver_tool_result("echo", "again"));
        assert_eq!(tool_results(driver.events()).len(), 1);
        assert_eq!(
            driver.state().harness,
            HarnessState::Running {
                step: 1,
                attempt: 0,
                answered: true
            }
        );
    }

    #[test]
    fn cancel_then_late_tool_result_stays_cancelled() {
        let mut driver = Driver::boot(local_echo()).unwrap();
        let mut decider = ScriptedDecider::new([tool_call()]);
        let echo = EchoTool;
        let effects = driver.decide(&mut decider, &[echo.descriptor()]).unwrap();
        assert!(matches!(
            driver.state().harness,
            HarnessState::WaitingForTool { .. }
        ));
        driver.cancel();
        assert!(!driver.deliver_tool_result("echo", "late"));
        driver.perform(
            &effects,
            &[&echo],
            &UnavailableModel,
            &mut InMemory::default(),
        );
        assert_eq!(driver.state().harness, HarnessState::Cancelled);
        assert!(tool_results(driver.events()).is_empty());
    }

    #[test]
    fn retry_after_cancel_stays_cancelled() {
        let mut driver = Driver::boot(local_echo()).unwrap();
        driver.cancel();
        driver.retry();
        assert_eq!(driver.state().harness, HarnessState::Cancelled);
        assert_eq!(driver.state().dispatch, DispatchPhase::Cancelled);
    }

    #[test]
    fn reverse_and_box_do_not_boot() {
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
    }
}
