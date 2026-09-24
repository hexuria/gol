use crate::{
    DispatchPhase, Effect, Event, EventPayload, FailureClass, HarnessState, MAX_RETRIES, MAX_STEPS,
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

pub fn reduce(state: HarnessState, event: &Event) -> (HarnessState, Effects) {
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
        EventPayload::EffectAuthorized { effect } => on_authorized(state, effect),
        EventPayload::ToolResult { .. } => match state {
            HarnessState::WaitingForTool { step, attempt } => (
                HarnessState::Running {
                    step,
                    attempt,
                    answered: true,
                },
                Vec::new(),
            ),
            other => (other, Vec::new()),
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
                attempt,
                answered: true,
            } if step < MAX_STEPS => (
                HarnessState::Running {
                    step: step + 1,
                    attempt,
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
        | EventPayload::EffectDecided { .. }
        | EventPayload::EffectDenied { .. }
        | EventPayload::ModelResponded { .. }
        | EventPayload::UserMessage { .. }
        | EventPayload::MemoryRead { .. }
        | EventPayload::MemoryWritten { .. } => (state, Vec::new()),
    }
}

fn on_authorized(state: HarnessState, effect: &Effect) -> (HarnessState, Effects) {
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
        Effect::ToolCall { .. } if !answered && (1..=MAX_STEPS).contains(&step) => (
            HarnessState::WaitingForTool { step, attempt },
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
    use crate::{Actor, Event, FailureClass, Timestamp};
    use proptest::prelude::*;

    fn ev(payload: EventPayload) -> Event {
        let spec = sample_spec();
        Event::record(
            spec.run_id,
            spec.agent_id,
            &spec.agent_version,
            None,
            Actor::System,
            None,
            Timestamp::unix_millis(0),
            payload,
        )
    }

    fn tool_result() -> Event {
        ev(EventPayload::ToolResult {
            name: "echo".to_string(),
            output: "hello".to_string(),
        })
    }

    #[test]
    fn authorized_tool_call_waits_and_emits_the_call() {
        let running = HarnessState::Running {
            step: 1,
            attempt: 0,
            answered: false,
        };
        let effect = Effect::ToolCall {
            name: "echo".to_string(),
            input: "hello".to_string(),
        };
        let (next, effects) = reduce(
            running,
            &ev(EventPayload::EffectAuthorized {
                effect: effect.clone(),
            }),
        );
        assert_eq!(
            next,
            HarnessState::WaitingForTool {
                step: 1,
                attempt: 0
            }
        );
        assert_eq!(effects, vec![effect]);
    }

    #[test]
    fn duplicate_tool_result_stays_answered() {
        let waiting = HarnessState::WaitingForTool {
            step: 1,
            attempt: 0,
        };
        let (answered, effects) = reduce(waiting, &tool_result());
        assert!(effects.is_empty());
        let (again, effects) = reduce(answered.clone(), &tool_result());
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
        let waiting = HarnessState::WaitingForTool {
            step: 1,
            attempt: 0,
        };
        let (cancelled, _) = reduce(waiting, &ev(EventPayload::RunCancelled));
        assert_eq!(cancelled, HarnessState::Cancelled);
        let (next, effects) = reduce(cancelled, &tool_result());
        assert_eq!(next, HarnessState::Cancelled);
        assert!(effects.is_empty());
    }

    #[test]
    fn retry_after_cancel_stays_cancelled() {
        let (next, effects) = reduce(HarnessState::Cancelled, &ev(EventPayload::StepRetried));
        assert_eq!(next, HarnessState::Cancelled);
        assert!(effects.is_empty());
    }

    #[test]
    fn fail_while_waiting_clears_the_wait() {
        let waiting = HarnessState::WaitingForTool {
            step: 1,
            attempt: 0,
        };
        let (next, effects) = reduce(
            waiting,
            &ev(EventPayload::RunFailed {
                class: FailureClass::Tool,
                message: "worker gone".to_string(),
            }),
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
        let first = Effect::ToolCall {
            name: "echo".to_string(),
            input: "one".to_string(),
        };
        let (waiting, effects) = reduce(
            running,
            &ev(EventPayload::EffectAuthorized {
                effect: first.clone(),
            }),
        );
        assert_eq!(
            waiting,
            HarnessState::WaitingForTool {
                step: 1,
                attempt: 0
            }
        );
        assert_eq!(effects, vec![first]);

        let (answered, effects) = reduce(waiting, &tool_result());
        assert!(effects.is_empty());
        assert_eq!(
            answered,
            HarnessState::Running {
                step: 1,
                attempt: 0,
                answered: true
            }
        );

        let (next, effects) = reduce(answered, &ev(EventPayload::StepAdvanced));
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
        };
        let (waiting, effects) = reduce(
            next,
            &ev(EventPayload::EffectAuthorized {
                effect: second.clone(),
            }),
        );
        assert_eq!(effects, vec![second]);
        assert_eq!(
            waiting,
            HarnessState::WaitingForTool {
                step: 2,
                attempt: 0
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
        let (retried, _) = reduce(answered, &ev(EventPayload::StepRetried));
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
        let (advanced, _) = reduce(answered, &ev(EventPayload::StepAdvanced));
        assert_eq!(
            advanced,
            HarnessState::Running {
                step: 2,
                attempt: 1,
                answered: false
            }
        );
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
            (any::<String>(), any::<String>())
                .prop_map(|(name, output)| EventPayload::ToolResult { name, output }),
            any::<String>().prop_map(|outcome| EventPayload::EffectAuthorized {
                effect: Effect::Complete { outcome },
            }),
        ]
    }

    proptest! {
        #[test]
        fn terminal_state_is_stuck(state in terminal_state(), payload in payload()) {
            let (next, effects) = reduce(state.clone(), &ev(payload));
            prop_assert_eq!(next, state);
            prop_assert!(effects.is_empty());
        }
    }
}
