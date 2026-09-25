use protocol::{
    authorize, fold, Actor, Effect, Event, EventPayload, ExecutionPlacement, FailureClass,
    HarnessState, InvocationId, PolicyDecision, RunSpec, RunState, Timestamp, ToolDescriptor,
};

use crate::{
    Decider, DeciderError, DecisionView, LoadedCatalog, Memory, ModelCompletion, Skill, Tool,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootError {
    UnsupportedPlacement(ExecutionPlacement),
}

pub struct Driver {
    spec: RunSpec,
    events: Vec<Event>,
    skills: Vec<Skill>,
    loaded: Option<Vec<Box<dyn Tool>>>,
}

impl Driver {
    pub fn boot(spec: RunSpec) -> Result<Self, BootError> {
        match spec.placement {
            ExecutionPlacement::Local | ExecutionPlacement::Reverse | ExecutionPlacement::Box => {}
        }
        let mut driver = Self {
            spec,
            events: Vec::new(),
            skills: Vec::new(),
            loaded: None,
        };
        driver.push(EventPayload::RunStarted, Actor::System);
        Ok(driver)
    }

    pub fn boot_with_catalog(spec: RunSpec, catalog: LoadedCatalog) -> Result<Self, BootError> {
        let mut driver = Self::boot(spec)?;
        let (tools, skills) = catalog.into_parts();
        driver.loaded = Some(tools);
        driver.skills = skills;
        Ok(driver)
    }

    pub fn run_loaded(
        &mut self,
        decider: &mut dyn Decider,
        models: &dyn ModelCompletion,
        memory: &mut dyn Memory,
    ) -> Result<(), DeciderError> {
        let tools = self.loaded.take().unwrap_or_default();
        let result = {
            let refs: Vec<&dyn Tool> = tools.iter().map(|tool| tool.as_ref()).collect();
            run_to_completion(self, decider, &refs, models, memory)
        };
        self.loaded = Some(tools);
        result
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
        invocation: InvocationId,
        output: impl Into<String>,
    ) -> bool {
        let name = name.into();
        let HarnessState::WaitingForTool {
            step,
            attempt,
            name: expected_name,
            invocation: expected_invocation,
        } = self.state().harness
        else {
            return false;
        };
        if name != expected_name || invocation != expected_invocation {
            return false;
        }
        self.push(
            EventPayload::ToolResult {
                name,
                invocation,
                step,
                attempt,
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
        let skills = self.skills.clone();
        self.decide_with_skills(decider, tools, &skills)
    }

    pub fn decide_with_skills(
        &mut self,
        decider: &mut dyn Decider,
        tools: &[ToolDescriptor],
        skills: &[Skill],
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
                skills,
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
                Effect::ToolCall {
                    name,
                    input,
                    invocation,
                } => {
                    let HarnessState::WaitingForTool {
                        name: waiting_name,
                        invocation: waiting_invocation,
                        ..
                    } = self.state().harness
                    else {
                        continue;
                    };
                    if waiting_name != *name || waiting_invocation != *invocation {
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
                    self.deliver_tool_result(name, *invocation, output);
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

    fn advance_answered_step(&mut self) {
        let HarnessState::Running {
            step,
            answered: true,
            ..
        } = self.state().harness
        else {
            return;
        };
        if step >= protocol::MAX_STEPS {
            return;
        }
        self.push(EventPayload::StepAdvanced, Actor::System);
    }

    fn push(&mut self, payload: EventPayload, actor: Actor) {
        self.events.push(Event::record(
            protocol::EventSource::new(
                self.spec.run_id,
                self.spec.agent_id,
                &self.spec.agent_version,
                actor,
                Timestamp::now(),
            ),
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
        driver.advance_answered_step();
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
            invocation: InvocationId::from_uuid(uuid::Uuid::from_u128(1)),
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
    fn two_authorized_tool_steps_both_execute() {
        let mut driver = Driver::boot(local_echo()).unwrap();
        let first = Effect::ToolCall {
            name: "echo".to_string(),
            input: "one".to_string(),
            invocation: InvocationId::from_uuid(uuid::Uuid::from_u128(1)),
        };
        let second = Effect::ToolCall {
            name: "echo".to_string(),
            input: "two".to_string(),
            invocation: InvocationId::from_uuid(uuid::Uuid::from_u128(2)),
        };
        let mut decider = ScriptedDecider::new([first, second.clone(), complete()]);
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

        let results = tool_results(driver.events());
        assert_eq!(results.len(), 2);
        match (&results[0].payload, &results[1].payload) {
            (
                EventPayload::ToolResult {
                    name: first_name,
                    output: first_output,
                    ..
                },
                EventPayload::ToolResult {
                    name: second_name,
                    output: second_output,
                    ..
                },
            ) => {
                assert_eq!(first_name, "echo");
                assert_eq!(first_output, "one");
                assert_eq!(second_name, "echo");
                assert_eq!(second_output, "two");
            }
            _ => unreachable!(),
        }

        let events = driver.events();
        let first_result = events
            .iter()
            .position(|event| matches!(event.payload, EventPayload::ToolResult { .. }))
            .unwrap();
        assert!(matches!(
            events[first_result + 1].payload,
            EventPayload::StepAdvanced
        ));
        let advanced = protocol::fold(&driver.spec, &events[..=first_result + 1]);
        assert_eq!(
            advanced.harness,
            HarnessState::Running {
                step: 2,
                attempt: 0,
                answered: false
            }
        );
        let second_authorized = events
            .iter()
            .rposition(|event| {
                matches!(
                    &event.payload,
                    EventPayload::EffectAuthorized {
                        effect: Effect::ToolCall { .. }
                    }
                )
            })
            .unwrap();
        let waiting = protocol::fold(&driver.spec, &events[..=second_authorized]);
        assert_eq!(
            waiting.harness,
            HarnessState::WaitingForTool {
                step: 2,
                attempt: 0,
                name: "echo".to_string(),
                invocation: InvocationId::from_uuid(uuid::Uuid::from_u128(2)),
            }
        );
        assert_eq!(
            driver.state().harness,
            HarnessState::Completed {
                outcome: "done".to_string()
            }
        );
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
            EventPayload::ToolResult { name, output, .. } => {
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
    fn mismatched_tool_result_leaves_the_original_call_outstanding() {
        let invocation = InvocationId::from_uuid(uuid::Uuid::from_u128(1));
        let call = Effect::ToolCall {
            name: "echo".to_string(),
            input: "hello".to_string(),
            invocation,
        };
        let mut driver = Driver::boot(local_echo()).unwrap();
        let mut decider = ScriptedDecider::new([call.clone()]);
        let echo = EchoTool;
        let effects = driver.decide(&mut decider, &[echo.descriptor()]).unwrap();
        let outstanding = HarnessState::WaitingForTool {
            step: 1,
            attempt: 0,
            name: "echo".to_string(),
            invocation,
        };
        assert_eq!(effects, vec![call]);
        assert_eq!(driver.state().harness, outstanding);
        assert!(!driver.deliver_tool_result(
            "other",
            InvocationId::from_uuid(uuid::Uuid::from_u128(2)),
            "nope",
        ));
        assert_eq!(driver.state().harness, outstanding);
        assert!(tool_results(driver.events()).is_empty());
        assert!(!driver.deliver_tool_result(
            "echo",
            InvocationId::from_uuid(uuid::Uuid::from_u128(2)),
            "nope",
        ));
        assert_eq!(driver.state().harness, outstanding);
        assert!(tool_results(driver.events()).is_empty());
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
        assert!(!driver.deliver_tool_result(
            "echo",
            InvocationId::from_uuid(uuid::Uuid::from_u128(1)),
            "again"
        ));
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
        assert!(!driver.deliver_tool_result(
            "echo",
            InvocationId::from_uuid(uuid::Uuid::from_u128(1)),
            "late"
        ));
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
    fn reverse_and_box_boot_into_running() {
        for placement in [ExecutionPlacement::Reverse, ExecutionPlacement::Box] {
            let driver = Driver::boot(spec_with(placement, Vec::new())).unwrap();
            assert!(matches!(
                driver.state().harness,
                HarnessState::Running { .. }
            ));
        }
    }
}
