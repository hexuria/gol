use std::sync::Arc;

use protocol::{
    applicable, authorize, fold, Actor, AgentId, DispatchPhase, Effect, Event, EventPayload,
    ExecutionPlacement, FailureClass, HarnessState, InvocationId, Limits, MemoryScope,
    PolicyDecision, RunSpec, RunState, Timestamp, ToolDescriptor, MAX_CHILDREN,
};

use crate::{
    AgentSpawner, ChildRequest, Decider, DeciderError, DecisionView, DelegateTarget, LoadedCatalog,
    Memory, MemoryKey, ModelCompletion, Skill, StoreError, Tool,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootError {
    UnsupportedPlacement(ExecutionPlacement),
}

/// Why `Driver::resume` would not rebuild a run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResumeError {
    UnsupportedPlacement(ExecutionPlacement),
    /// A log from before the harness started, whose dispatch is not one that
    /// `RunStarted` starts (`Created` or `Starting`). A queued run is
    /// scheduled first, as a worker records it.
    NotStartable(DispatchPhase),
}

pub struct Driver {
    spec: RunSpec,
    events: Vec<Event>,
    skills: Vec<Skill>,
    loaded: Option<Vec<Box<dyn Tool>>>,
    spawner: Option<Arc<dyn AgentSpawner>>,
    targets: Vec<DelegateTarget>,
    /// Effects the log authorized and holds no result for, performed first
    /// by `run_until`: the tail of a step a resumed run was cut in.
    pending: Vec<Effect>,
    /// A decision the log recorded and never authorized, with the harness
    /// state it was made in: `run_until` authorizes it first.
    undecided: Option<(HarnessState, Effect)>,
    /// Rebuilt by `resume` and not yet run: `run_until` first finishes the
    /// step the log was cut in.
    resumed: bool,
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
            spawner: None,
            targets: Vec::new(),
            pending: Vec::new(),
            undecided: None,
            resumed: false,
        };
        driver.push(EventPayload::RunStarted, Actor::System);
        Ok(driver)
    }

    /// Rebuilds a driver from a stored log and picks up where it stopped.
    ///
    /// - A log that already holds a terminal event (`is_run_end`) is left as
    ///   it is: nothing is appended and `run_until` decides nothing, even
    ///   when the harness fold never saw the end (a run failed or cancelled
    ///   before it started, or cancelled after its `Complete` was
    ///   authorized). A second terminal event would be refused.
    /// - A log from before the harness started gets its `RunStarted` when
    ///   its dispatch is `Created` or `Starting`; a queued log that no worker
    ///   scheduled is refused with `NotStartable`.
    /// - A log cut after a decision and before its authorization is
    ///   authorized by `run_until`; one cut after an effect was authorized
    ///   and before its result performs that effect again, a tool with the
    ///   same invocation (decision 1.5a-3A).
    /// - A harness that completed with no terminal event in the log gets its
    ///   `RunCompleted`, as `decide` would have recorded it.
    pub fn resume(spec: RunSpec, events: Vec<Event>) -> Result<Self, ResumeError> {
        match spec.placement {
            ExecutionPlacement::Local | ExecutionPlacement::Reverse | ExecutionPlacement::Box => {}
        }
        let started = events
            .iter()
            .any(|event| matches!(event.payload, EventPayload::RunStarted));
        let ended = events.iter().any(|event| is_run_end(&event.payload));
        let mut driver = Self {
            spec,
            events,
            skills: Vec::new(),
            loaded: None,
            spawner: None,
            targets: Vec::new(),
            pending: Vec::new(),
            undecided: None,
            resumed: false,
        };
        if ended {
            return Ok(driver);
        }
        let state = driver.state();
        if !started {
            return match state.dispatch {
                DispatchPhase::Created | DispatchPhase::Starting => {
                    driver.push(EventPayload::RunStarted, Actor::System);
                    Ok(driver)
                }
                other => Err(ResumeError::NotStartable(other)),
            };
        }
        driver.resumed = true;
        match state.harness {
            HarnessState::Completed { outcome } => {
                driver.push(EventPayload::RunCompleted { outcome }, Actor::System);
            }
            harness if !harness.is_terminal() => match driver.events.last() {
                Some(Event {
                    payload: EventPayload::EffectDecided { effect },
                    ..
                }) => {
                    let before = &driver.events[..driver.events.len() - 1];
                    let prior = fold(&driver.spec, before).harness;
                    driver.undecided = Some((prior, effect.clone()));
                }
                _ => driver.pending = effects_of_last(&driver.spec, &driver.events),
            },
            _ => {}
        }
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
        memory: &dyn Memory,
    ) -> Result<(), DeciderError> {
        let tools = self.loaded.take().unwrap_or_default();
        let result = {
            let refs: Vec<&dyn Tool> = tools.iter().map(|tool| tool.as_ref()).collect();
            run_to_completion(self, decider, &refs, models, memory)
        };
        self.loaded = Some(tools);
        result
    }

    /// Delegations this run is allowed to make start their children through
    /// `spawner`. Without one, they are refused. `targets` are the agents the
    /// decider is offered.
    pub fn with_spawner(
        mut self,
        spawner: Arc<dyn AgentSpawner>,
        targets: Vec<DelegateTarget>,
    ) -> Self {
        self.spawner = Some(spawner);
        self.targets = targets;
        self
    }

    /// Whether the log holds a terminal event.
    fn log_ended(&self) -> bool {
        self.events.iter().any(|event| is_run_end(&event.payload))
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
        let limits = &self.spec.limits;
        // What this run gave its children is spent as if it had taken it.
        let steps_spent = state.steps.saturating_add(state.given_steps) >= limits.max_steps;
        let model_calls_spent =
            state.model_calls.saturating_add(state.given_model_calls) >= limits.max_model_calls;

        let effect = {
            let view = DecisionView {
                spec: &self.spec,
                state: &state,
                events: &self.events,
                tools,
                skills,
                agents: &self.targets,
                steps_exhausted: steps_spent,
                model_calls_exhausted: model_calls_spent,
            };
            decider.decide(&view)?
        };
        // A Complete that finishes the run is always allowed and costs
        // nothing. Only a running harness completes on it; anywhere else it
        // is a step like any other decision (see `fold`). Any other effect
        // needs a step left, and a model call also needs a model call left.
        let over_budget = match effect {
            Effect::Complete { .. } if matches!(state.harness, HarnessState::Running { .. }) => {
                false
            }
            Effect::ModelCall { .. } => steps_spent || model_calls_spent,
            _ => steps_spent,
        };
        if over_budget {
            self.push(
                EventPayload::RunFailed {
                    class: FailureClass::Budget,
                    message: "limit hit".to_string(),
                },
                Actor::System,
            );
            return Ok(Vec::new());
        }
        self.push(
            EventPayload::EffectDecided {
                effect: effect.clone(),
            },
            Actor::Agent,
        );

        Ok(self.authorize_decided(&state.harness, effect, tools))
    }

    /// Authorizes the effect the log just recorded as decided, from the
    /// harness state before that decision, and returns what to perform.
    fn authorize_decided(
        &mut self,
        harness: &HarnessState,
        effect: Effect,
        tools: &[ToolDescriptor],
    ) -> Vec<Effect> {
        match authorize(&self.spec, &effect, tools) {
            // Allowed, but the harness would not act on it now: deny it with
            // the reason rather than authorize an effect `reduce` drops. The
            // decision above already cost a step, so the budget still ends a
            // decider that keeps proposing such effects.
            PolicyDecision::Allow if !applicable(harness, &effect, &self.spec) => {
                // The policy allowed it; the harness state is what refuses it.
                let reason = format!("not applicable while {}", describe(harness));
                self.push(EventPayload::EffectDenied { effect, reason }, Actor::System);
                Vec::new()
            }
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
                effects_of_last(&self.spec, &self.events)
            }
            PolicyDecision::Deny { reason } => {
                self.push(EventPayload::EffectDenied { effect, reason }, Actor::Policy);
                Vec::new()
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
                Vec::new()
            }
        }
    }

    pub fn perform(
        &mut self,
        effects: &[Effect],
        tools: &[&dyn Tool],
        models: &dyn ModelCompletion,
        memory: &dyn Memory,
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
                    match tool.call(input) {
                        Ok(output) => {
                            self.deliver_tool_result(name, *invocation, output);
                        }
                        Err(message) => {
                            self.push(
                                EventPayload::RunFailed {
                                    class: FailureClass::Tool,
                                    message,
                                },
                                Actor::System,
                            );
                            return;
                        }
                    }
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
                // A memory store that cannot answer ends the run: the effect
                // is not recorded as done, and nothing retries a write whose
                // outcome is unknown. The run log, which clients read, gets a
                // fixed message; the store's detail goes to stderr only.
                Effect::MemoryRead { scope, key } => {
                    let Some(owner) = self.memory_key(*scope) else {
                        self.fail_unowned(*scope);
                        return;
                    };
                    match memory.read(&owner, key) {
                        Ok(value) => self.push(
                            EventPayload::MemoryRead {
                                scope: *scope,
                                key: key.clone(),
                                value,
                            },
                            Actor::System,
                        ),
                        Err(error) => {
                            self.fail_memory("read", &error);
                            return;
                        }
                    }
                }
                Effect::MemoryWrite { scope, key, value } => {
                    let Some(owner) = self.memory_key(*scope) else {
                        self.fail_unowned(*scope);
                        return;
                    };
                    match memory.write(&owner, key, value) {
                        Ok(()) => self.push(
                            EventPayload::MemoryWritten {
                                scope: *scope,
                                key: key.clone(),
                                value: value.clone(),
                            },
                            Actor::System,
                        ),
                        Err(error) => {
                            self.fail_memory("write", &error);
                            return;
                        }
                    }
                }
                Effect::Delegate { agent_id, input } => self.delegate(*agent_id, input),
                Effect::Complete { .. }
                | Effect::Execute { .. }
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
        if step >= self.spec.limits.max_steps {
            return;
        }
        self.push(EventPayload::StepAdvanced, Actor::System);
    }

    /// Whose memory `scope` is for this run at its current harness step
    /// (`Running.step`, which advances once a tool result answers the step).
    fn memory_key(&self, scope: MemoryScope) -> Option<MemoryKey> {
        let step = match self.state().harness {
            HarnessState::Running { step, .. } | HarnessState::WaitingForTool { step, .. } => step,
            _ => 0,
        };
        protocol::memory_owner_id(&self.spec, scope, step)
            .map(|owner_id| MemoryKey { scope, owner_id })
    }

    /// A session or workspace effect whose id the spec does not name. The
    /// authorizer denies those (owner decision 2B for C3), so only a caller
    /// of `perform` that skipped it gets here.
    fn fail_unowned(&mut self, scope: MemoryScope) {
        self.push(
            EventPayload::RunFailed {
                class: FailureClass::Policy,
                message: format!("memory {scope:?}: the run names no owner for it"),
            },
            Actor::System,
        );
    }

    fn fail_memory(&mut self, operation: &str, error: &StoreError) {
        eprintln!(
            "gol: run {}: memory {operation} failed: {error}",
            self.spec.run_id
        );
        self.push(
            EventPayload::RunFailed {
                class: FailureClass::Infrastructure,
                message: format!("memory {operation}: store unavailable"),
            },
            Actor::System,
        );
    }

    /// Starts a child through the spawner, with half of what this run has
    /// left, and records what happened. The run goes on either way.
    fn delegate(&mut self, agent_id: AgentId, input: &str) {
        let state = self.state();
        let limits = self.spec.limits;
        let left = |max: u32, used: u32, given: u32| max.saturating_sub(used.saturating_add(given));
        let steps_left = left(limits.max_steps, state.steps, state.given_steps);
        let model_calls_left = left(
            limits.max_model_calls,
            state.model_calls,
            state.given_model_calls,
        );
        let refused = if !matches!(state.harness, HarnessState::Running { .. }) {
            Err("the run is not running".to_string())
        } else if state.children >= MAX_CHILDREN {
            Err(format!("already started {MAX_CHILDREN} children"))
        } else if steps_left < 2 || model_calls_left < 2 {
            Err("not enough budget left to give a child".to_string())
        } else {
            Ok(Limits {
                max_steps: steps_left / 2,
                max_model_calls: model_calls_left / 2,
            })
        };
        let step = match state.harness {
            HarnessState::Running { step, .. } => step,
            _ => 0,
        };
        let started = refused.and_then(|given| {
            let spawner = self
                .spawner
                .clone()
                .ok_or_else(|| "no agent spawner is configured".to_string())?;
            let started = spawner.start(ChildRequest {
                parent: &self.spec,
                step,
                agent_id,
                input,
                limits: given,
            })?;
            // The stored child's limits, which are what this run has given.
            Ok((started.run_id, started.limits))
        });
        let payload = match started {
            Ok((run_id, limits)) => EventPayload::ChildStarted {
                run_id,
                agent_id,
                limits,
            },
            Err(reason) => EventPayload::DelegateRefused { agent_id, reason },
        };
        self.push(payload, Actor::System);
    }

    fn push(&mut self, payload: EventPayload, actor: Actor) {
        self.events.push(Event::record(
            protocol::EventSource::for_spec(&self.spec, actor, Timestamp::now()),
            payload,
        ));
    }
}

pub fn run_to_completion(
    driver: &mut Driver,
    decider: &mut dyn Decider,
    tools: &[&dyn Tool],
    models: &dyn ModelCompletion,
    memory: &dyn Memory,
) -> Result<(), DeciderError> {
    run_until(driver, decider, tools, models, memory, &|| false)
}

/// Runs step by step until the run ends. `should_stop` is read at each step
/// boundary, before the next decision; when it answers yes the run is
/// cancelled there (decision 1.5a-2A). Effects a resumed log left pending are
/// performed first.
pub fn run_until(
    driver: &mut Driver,
    decider: &mut dyn Decider,
    tools: &[&dyn Tool],
    models: &dyn ModelCompletion,
    memory: &dyn Memory,
    should_stop: &dyn Fn() -> bool,
) -> Result<(), DeciderError> {
    let descriptors: Vec<ToolDescriptor> = tools.iter().map(|tool| tool.descriptor()).collect();
    let mut pending = std::mem::take(&mut driver.pending);
    if let Some((harness, effect)) = driver.undecided.take() {
        pending = driver.authorize_decided(&harness, effect, &descriptors);
    }
    if std::mem::take(&mut driver.resumed) {
        driver.perform(&pending, tools, models, memory);
        driver.advance_answered_step();
    }
    loop {
        // A log another writer ended stays ended, whatever the harness fold
        // says (see `Driver::resume`).
        if driver.state().harness.is_terminal() || driver.log_ended() {
            return Ok(());
        }
        if should_stop() {
            driver.cancel();
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

/// The harness state in words, for the reason of a denied effect.
fn describe(state: &HarnessState) -> &'static str {
    match state {
        HarnessState::Idle => "idle",
        HarnessState::Running { answered: true, .. } => "running, answered",
        HarnessState::Running { .. } => "running",
        HarnessState::WaitingForTool { .. } => "waiting for a tool",
        HarnessState::Completed { .. } | HarnessState::Failed { .. } | HarnessState::Cancelled => {
            "finished"
        }
    }
}

/// Whether `payload` ends the run's log.
fn is_run_end(payload: &EventPayload) -> bool {
    matches!(
        payload,
        EventPayload::RunCompleted { .. }
            | EventPayload::RunFailed { .. }
            | EventPayload::RunCancelled
            | EventPayload::RunExpired
    )
}

fn effects_of_last(spec: &RunSpec, events: &[Event]) -> Vec<Effect> {
    let Some(last) = events.last() else {
        return Vec::new();
    };
    let prior = fold(spec, &events[..events.len() - 1]);
    reduce_effects(prior.harness, last, spec)
}

fn reduce_effects(state: HarnessState, event: &Event, spec: &RunSpec) -> Vec<Effect> {
    protocol::reduce(state, event, spec).1
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
            .owner(protocol::Owner::new(
                "https://issuer.test",
                "user-1",
                "tenant-1",
            ))
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

        fn call(&self, _input: &str) -> Result<String, String> {
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
            &InMemory::default(),
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
            &InMemory::default(),
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
            &InMemory::default(),
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
        driver.perform(&effects, &[&echo], &UnavailableModel, &InMemory::default());
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
        driver.perform(&effects, &[&echo], &UnavailableModel, &InMemory::default());
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
