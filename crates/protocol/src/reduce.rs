use crate::{
    DispatchPhase, Effect, Event, EventPayload, FailureClass, HarnessState, RunSpec, MAX_RETRIES,
};

pub type Effects = Vec<Effect>;

pub fn reduce_dispatch(state: DispatchPhase, event: &Event) -> DispatchPhase {
    if state.is_terminal() {
        return state;
    }
    match (&state, &event.payload) {
        (DispatchPhase::Created, EventPayload::RunQueued) => DispatchPhase::Queued,
        (DispatchPhase::Queued, EventPayload::RunScheduled) => DispatchPhase::Scheduled,
        (DispatchPhase::Scheduled, EventPayload::RunProvisioning) => DispatchPhase::Provisioning,
        (DispatchPhase::Provisioning, EventPayload::RunStarting) => DispatchPhase::Starting,
        (DispatchPhase::Starting | DispatchPhase::Created, EventPayload::RunStarted) => {
            DispatchPhase::Running
        }
        (DispatchPhase::Running, EventPayload::RunWaiting { reason }) => DispatchPhase::Waiting {
            reason: reason.clone(),
        },
        (DispatchPhase::Running, EventPayload::RunAwaitingApproval { approval_id }) => {
            DispatchPhase::AwaitingApproval {
                approval_id: *approval_id,
            }
        }
        (DispatchPhase::Running, EventPayload::RunPaused) => DispatchPhase::Paused,
        (DispatchPhase::Running, EventPayload::RunRecovering) => DispatchPhase::Recovering,
        (
            DispatchPhase::Waiting { .. }
            | DispatchPhase::AwaitingApproval { .. }
            | DispatchPhase::Paused
            | DispatchPhase::Recovering,
            EventPayload::RunResumed,
        ) => DispatchPhase::Running,
        (
            DispatchPhase::Running
            | DispatchPhase::Waiting { .. }
            | DispatchPhase::AwaitingApproval { .. }
            | DispatchPhase::Paused
            | DispatchPhase::Recovering,
            EventPayload::RunCompleted { outcome },
        ) => DispatchPhase::Completed {
            outcome: outcome.clone(),
        },
        (_, EventPayload::RunFailed { class, message }) => DispatchPhase::Failed {
            class: *class,
            message: message.clone(),
        },
        (_, EventPayload::RunCancelled) => DispatchPhase::Cancelled,
        (_, EventPayload::RunExpired) => DispatchPhase::Expired,
        _ => state,
    }
}

pub fn reduce(state: HarnessState, event: &Event, spec: &RunSpec) -> (HarnessState, Effects) {
    if state.is_terminal() {
        return (state, Vec::new());
    }

    match &event.payload {
        EventPayload::RunStarted => match state {
            HarnessState::Idle => (
                HarnessState::Running {
                    step: 1,
                    attempt: 0,
                    answered: false,
                },
                Vec::new(),
            ),
            other => (other, Vec::new()),
        },
        EventPayload::EffectAuthorized { effect } => on_authorized(state, effect, spec),
        EventPayload::ToolResult {
            name,
            invocation,
            step,
            attempt,
            ..
        } => match &state {
            HarnessState::WaitingForTool {
                step: waiting_step,
                attempt: waiting_attempt,
                name: waiting_name,
                invocation: waiting_invocation,
            } if *step == *waiting_step
                && *attempt == *waiting_attempt
                && name == waiting_name
                && invocation == waiting_invocation =>
            {
                (
                    HarnessState::Running {
                        step: *waiting_step,
                        attempt: *waiting_attempt,
                        answered: true,
                    },
                    Vec::new(),
                )
            }
            _ => (state, Vec::new()),
        },
        EventPayload::StepRetried => match state {
            HarnessState::Running {
                step,
                attempt,
                answered: true,
            } if attempt < MAX_RETRIES => (
                HarnessState::Running {
                    step,
                    attempt: attempt + 1,
                    answered: false,
                },
                Vec::new(),
            ),
            other => (other, Vec::new()),
        },
        EventPayload::StepAdvanced => match state {
            HarnessState::Running {
                step,
                answered: true,
                ..
            } if step < spec.limits.max_steps => (
                HarnessState::Running {
                    step: step + 1,
                    attempt: 0,
                    answered: false,
                },
                Vec::new(),
            ),
            other => (other, Vec::new()),
        },
        EventPayload::RunCompleted { outcome } => match state {
            HarnessState::Running { .. } => (
                HarnessState::Completed {
                    outcome: outcome.clone(),
                },
                Vec::new(),
            ),
            other => (other, Vec::new()),
        },
        EventPayload::RunFailed { class, message } => match state {
            HarnessState::Running { .. } | HarnessState::WaitingForTool { .. } => (
                HarnessState::Failed {
                    class: *class,
                    message: message.clone(),
                },
                Vec::new(),
            ),
            other => (other, Vec::new()),
        },
        EventPayload::RunCancelled => match state {
            HarnessState::Running { .. } | HarnessState::WaitingForTool { .. } => {
                (HarnessState::Cancelled, Vec::new())
            }
            other => (other, Vec::new()),
        },
        EventPayload::RunExpired => match state {
            HarnessState::Running { .. } | HarnessState::WaitingForTool { .. } => (
                HarnessState::Failed {
                    class: FailureClass::Timeout,
                    message: "run expired".to_string(),
                },
                Vec::new(),
            ),
            other => (other, Vec::new()),
        },
        EventPayload::RunCreated
        | EventPayload::RunQueued
        | EventPayload::RunScheduled
        | EventPayload::RunProvisioning
        | EventPayload::RunStarting
        | EventPayload::RunWaiting { .. }
        | EventPayload::RunAwaitingApproval { .. }
        | EventPayload::RunPaused
        | EventPayload::RunRecovering
        | EventPayload::RunResumed
        | EventPayload::EffectDecided { .. }
        | EventPayload::EffectDenied { .. }
        | EventPayload::ModelResponded { .. }
        | EventPayload::UserMessage { .. }
        | EventPayload::MemoryRead { .. }
        | EventPayload::MemoryWritten { .. } => (state, Vec::new()),
    }
}

fn on_authorized(state: HarnessState, effect: &Effect, spec: &RunSpec) -> (HarnessState, Effects) {
    let HarnessState::Running {
        step,
        attempt,
        answered,
    } = state
    else {
        return (state, Vec::new());
    };
    let running = HarnessState::Running {
        step,
        attempt,
        answered,
    };

    match effect {
        Effect::ToolCall {
            name, invocation, ..
        } if !answered && (1..=spec.limits.max_steps).contains(&step) => (
            HarnessState::WaitingForTool {
                step,
                attempt,
                name: name.clone(),
                invocation: *invocation,
            },
            vec![effect.clone()],
        ),
        Effect::Complete { outcome } => (
            HarnessState::Completed {
                outcome: outcome.clone(),
            },
            Vec::new(),
        ),
        Effect::ModelCall { .. } | Effect::MemoryRead { .. } | Effect::MemoryWrite { .. } => {
            (running, vec![effect.clone()])
        }
        _ => (running, Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::sample_spec;
    use crate::{
        Actor, AgentId, Capability, CredentialSource, Event, EventSource, ExecutionPlacement,
        FailureClass, InvocationId, Limits, ModelProvider, RunSpec, Timestamp, WorkModel,
    };
    use proptest::prelude::*;
    use uuid::Uuid;

    fn invocation() -> InvocationId {
        InvocationId::from_uuid(Uuid::from_u128(1))
    }

    fn other_invocation() -> InvocationId {
        InvocationId::from_uuid(Uuid::from_u128(2))
    }

    fn echo_call(input: &str) -> Effect {
        Effect::ToolCall {
            name: "echo".to_string(),
            input: input.to_string(),
            invocation: invocation(),
        }
    }

    fn echo_wait(step: u32, attempt: u32) -> HarnessState {
        HarnessState::WaitingForTool {
            step,
            attempt,
            name: "echo".to_string(),
            invocation: invocation(),
        }
    }

    fn ev(payload: EventPayload) -> Event {
        let spec = sample_spec();
        Event::record(
            EventSource::new(
                spec.run_id,
                spec.agent_id,
                &spec.agent_version,
                Actor::System,
                Timestamp::unix_millis(0),
            ),
            payload,
        )
    }

    fn tool_result_for(
        name: &str,
        invocation: InvocationId,
        step: u32,
        attempt: u32,
        output: &str,
    ) -> Event {
        ev(EventPayload::ToolResult {
            name: name.to_string(),
            invocation,
            step,
            attempt,
            output: output.to_string(),
        })
    }

    fn tool_result() -> Event {
        tool_result_for("echo", invocation(), 1, 0, "hello")
    }

    #[test]
    fn authorized_tool_call_waits_and_emits_the_call() {
        let running = HarnessState::Running {
            step: 1,
            attempt: 0,
            answered: false,
        };
        let effect = echo_call("hello");
        let (next, effects) = reduce(
            running,
            &ev(EventPayload::EffectAuthorized {
                effect: effect.clone(),
            }),
            &sample_spec(),
        );
        assert_eq!(next, echo_wait(1, 0));
        assert_eq!(effects, vec![effect]);
    }

    #[test]
    fn mismatched_tool_result_leaves_the_wait() {
        let waiting = echo_wait(1, 0);
        let mismatches = [
            tool_result_for("other", invocation(), 1, 0, "nope"),
            tool_result_for("echo", other_invocation(), 1, 0, "nope"),
            tool_result_for("echo", invocation(), 2, 0, "nope"),
            tool_result_for("echo", invocation(), 1, 1, "nope"),
        ];
        for event in mismatches {
            let (next, effects) = reduce(waiting.clone(), &event, &sample_spec());
            assert_eq!(next, waiting);
            assert!(effects.is_empty());
        }
    }

    #[test]
    fn duplicate_tool_result_stays_answered() {
        let waiting = echo_wait(1, 0);
        let (answered, effects) = reduce(waiting, &tool_result(), &sample_spec());
        assert!(effects.is_empty());
        let (again, effects) = reduce(answered.clone(), &tool_result(), &sample_spec());
        assert_eq!(again, answered);
        assert!(effects.is_empty());
        assert_eq!(
            again,
            HarnessState::Running {
                step: 1,
                attempt: 0,
                answered: true
            }
        );
    }

    #[test]
    fn late_tool_result_after_cancel_stays_cancelled() {
        let waiting = echo_wait(1, 0);
        let (cancelled, _) = reduce(waiting, &ev(EventPayload::RunCancelled), &sample_spec());
        assert_eq!(cancelled, HarnessState::Cancelled);
        let (next, effects) = reduce(cancelled, &tool_result(), &sample_spec());
        assert_eq!(next, HarnessState::Cancelled);
        assert!(effects.is_empty());
    }

    #[test]
    fn retry_after_cancel_stays_cancelled() {
        let (next, effects) = reduce(
            HarnessState::Cancelled,
            &ev(EventPayload::StepRetried),
            &sample_spec(),
        );
        assert_eq!(next, HarnessState::Cancelled);
        assert!(effects.is_empty());
    }

    #[test]
    fn fail_while_waiting_clears_the_wait() {
        let waiting = echo_wait(1, 0);
        let (next, effects) = reduce(
            waiting,
            &ev(EventPayload::RunFailed {
                class: FailureClass::Tool,
                message: "worker gone".to_string(),
            }),
            &sample_spec(),
        );
        assert_eq!(
            next,
            HarnessState::Failed {
                class: FailureClass::Tool,
                message: "worker gone".to_string()
            }
        );
        assert!(effects.is_empty());
    }

    #[test]
    fn tool_result_then_step_advanced_opens_the_next_tool_call() {
        let running = HarnessState::Running {
            step: 1,
            attempt: 0,
            answered: false,
        };
        let first = echo_call("one");
        let (waiting, effects) = reduce(
            running,
            &ev(EventPayload::EffectAuthorized {
                effect: first.clone(),
            }),
            &sample_spec(),
        );
        assert_eq!(waiting, echo_wait(1, 0));
        assert_eq!(effects, vec![first]);

        let (answered, effects) = reduce(waiting, &tool_result(), &sample_spec());
        assert!(effects.is_empty());
        assert_eq!(
            answered,
            HarnessState::Running {
                step: 1,
                attempt: 0,
                answered: true
            }
        );

        let (next, effects) = reduce(answered, &ev(EventPayload::StepAdvanced), &sample_spec());
        assert!(effects.is_empty());
        assert_eq!(
            next,
            HarnessState::Running {
                step: 2,
                attempt: 0,
                answered: false
            }
        );

        let second = Effect::ToolCall {
            name: "echo".to_string(),
            input: "two".to_string(),
            invocation: other_invocation(),
        };
        let (waiting, effects) = reduce(
            next,
            &ev(EventPayload::EffectAuthorized {
                effect: second.clone(),
            }),
            &sample_spec(),
        );
        assert_eq!(effects, vec![second]);
        assert_eq!(
            waiting,
            HarnessState::WaitingForTool {
                step: 2,
                attempt: 0,
                name: "echo".to_string(),
                invocation: other_invocation(),
            }
        );
    }

    #[test]
    fn retry_and_advance_stay_on_running() {
        let answered = HarnessState::Running {
            step: 1,
            attempt: 0,
            answered: true,
        };
        let (retried, _) = reduce(answered, &ev(EventPayload::StepRetried), &sample_spec());
        assert_eq!(
            retried,
            HarnessState::Running {
                step: 1,
                attempt: 1,
                answered: false
            }
        );
        let answered = HarnessState::Running {
            step: 1,
            attempt: 1,
            answered: true,
        };
        let (advanced, _) = reduce(answered, &ev(EventPayload::StepAdvanced), &sample_spec());
        assert_eq!(
            advanced,
            HarnessState::Running {
                step: 2,
                attempt: 0,
                answered: false
            }
        );
    }

    #[test]
    fn step_advanced_stops_at_the_spec_limit() {
        let spec = RunSpec::builder()
            .agent(AgentId::new(), "3")
            .input("ship")
            .placement(ExecutionPlacement::Box)
            .work_model(WorkModel {
                provider: ModelProvider::SystemOne,
                model_name: "jev-latest".to_string(),
                credential: CredentialSource::BringYourOwn {
                    secret_ref: "jev".to_string(),
                },
            })
            .capabilities(vec![Capability::new("tool.echo")])
            .limits(Limits {
                max_steps: 2,
                max_model_calls: 1,
            })
            .build();

        let at_limit = HarnessState::Running {
            step: 2,
            attempt: 1,
            answered: true,
        };
        let (stopped, effects) = reduce(at_limit.clone(), &ev(EventPayload::StepAdvanced), &spec);
        assert_eq!(stopped, at_limit);
        assert!(effects.is_empty());

        let answered = HarnessState::Running {
            step: 1,
            attempt: 1,
            answered: true,
        };
        let (advanced, effects) = reduce(answered, &ev(EventPayload::StepAdvanced), &spec);
        assert!(effects.is_empty());
        assert_eq!(
            advanced,
            HarnessState::Running {
                step: 2,
                attempt: 0,
                answered: false
            }
        );

        let effect = echo_call("hello");
        let (waiting, effects) = reduce(
            HarnessState::Running {
                step: 2,
                attempt: 0,
                answered: false,
            },
            &ev(EventPayload::EffectAuthorized {
                effect: effect.clone(),
            }),
            &spec,
        );
        assert_eq!(effects, vec![effect.clone()]);
        assert_eq!(waiting, echo_wait(2, 0));

        let past = HarnessState::Running {
            step: 3,
            attempt: 0,
            answered: false,
        };
        let (stays, effects) = reduce(
            past.clone(),
            &ev(EventPayload::EffectAuthorized { effect }),
            &spec,
        );
        assert_eq!(stays, past);
        assert!(effects.is_empty());
    }

    fn failure_class() -> impl Strategy<Value = FailureClass> {
        prop_oneof![
            Just(FailureClass::Agent),
            Just(FailureClass::Model),
            Just(FailureClass::Tool),
            Just(FailureClass::Policy),
            Just(FailureClass::Environment),
            Just(FailureClass::Infrastructure),
            Just(FailureClass::Timeout),
            Just(FailureClass::Budget),
            Just(FailureClass::Dependency),
            Just(FailureClass::UserCancellation),
        ]
    }

    fn terminal_state() -> impl Strategy<Value = HarnessState> {
        prop_oneof![
            any::<String>().prop_map(|outcome| HarnessState::Completed { outcome }),
            (failure_class(), any::<String>())
                .prop_map(|(class, message)| HarnessState::Failed { class, message }),
            Just(HarnessState::Cancelled),
        ]
    }

    fn payload() -> impl Strategy<Value = EventPayload> {
        prop_oneof![
            Just(EventPayload::RunCreated),
            Just(EventPayload::RunQueued),
            Just(EventPayload::RunScheduled),
            Just(EventPayload::RunStarted),
            Just(EventPayload::RunCancelled),
            Just(EventPayload::RunExpired),
            Just(EventPayload::StepRetried),
            Just(EventPayload::StepAdvanced),
            any::<String>().prop_map(|outcome| EventPayload::RunCompleted { outcome }),
            (failure_class(), any::<String>())
                .prop_map(|(class, message)| { EventPayload::RunFailed { class, message } }),
            (
                any::<String>(),
                any::<u128>(),
                any::<u32>(),
                any::<u32>(),
                any::<String>(),
            )
                .prop_map(|(name, bits, step, attempt, output)| {
                    EventPayload::ToolResult {
                        name,
                        invocation: InvocationId::from_uuid(Uuid::from_u128(bits)),
                        step,
                        attempt,
                        output,
                    }
                }),
            any::<String>().prop_map(|outcome| EventPayload::EffectAuthorized {
                effect: Effect::Complete { outcome },
            }),
        ]
    }

    proptest! {
        #[test]
        fn terminal_state_is_stuck(state in terminal_state(), payload in payload()) {
            let (next, effects) = reduce(state.clone(), &ev(payload), &sample_spec());
            prop_assert_eq!(next, state);
            prop_assert!(effects.is_empty());
        }
    }
}
