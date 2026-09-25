use serde::{Deserialize, Serialize};

use crate::{
    reduce, reduce_dispatch, DispatchPhase, Effect, Event, EventPayload, HarnessState, RunSpec,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunState {
    pub run_id: crate::RunId,
    pub harness: HarnessState,
    pub dispatch: DispatchPhase,
    pub steps: u32,
    pub model_calls: u32,
}

pub fn fold(spec: &RunSpec, events: &[Event]) -> RunState {
    let mut harness = HarnessState::Idle;
    let mut dispatch = DispatchPhase::Created;
    let mut steps = 0;
    let mut model_calls = 0;

    for event in events {
        match &event.payload {
            EventPayload::EffectDecided { .. } => steps += 1,
            EventPayload::EffectAuthorized {
                effect: Effect::ModelCall { .. },
            } => {
                model_calls += 1;
            }
            _ => {}
        }
        dispatch = reduce_dispatch(dispatch, event);
        let (next, _) = reduce(harness, event);
        harness = next;
    }

    RunState {
        run_id: spec.run_id,
        dispatch,
        harness,
        steps,
        model_calls,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::sample_spec;
    use crate::{Actor, ApprovalId, Event, EventSource, FailureClass, Timestamp};

    fn ev(spec: &RunSpec, payload: EventPayload) -> Event {
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

    fn started(spec: &RunSpec, terminal: EventPayload) -> Vec<Event> {
        vec![ev(spec, EventPayload::RunStarted), ev(spec, terminal)]
    }

    #[test]
    fn empty_log_is_idle_and_created() {
        let spec = sample_spec();
        let state = fold(&spec, &[]);
        assert_eq!(state.harness, HarnessState::Idle);
        assert_eq!(state.dispatch, DispatchPhase::Created);
    }

    #[test]
    fn fold_completed() {
        let spec = sample_spec();
        let events = started(
            &spec,
            EventPayload::RunCompleted {
                outcome: "shipped".to_string(),
            },
        );
        let state = fold(&spec, &events);
        assert_eq!(
            state.harness,
            HarnessState::Completed {
                outcome: "shipped".to_string()
            }
        );
        assert_eq!(
            state.dispatch,
            DispatchPhase::Completed {
                outcome: "shipped".to_string()
            }
        );
    }

    #[test]
    fn fold_failed() {
        let spec = sample_spec();
        let events = started(
            &spec,
            EventPayload::RunFailed {
                class: FailureClass::Timeout,
                message: "clock exceeded".to_string(),
            },
        );
        let state = fold(&spec, &events);
        assert_eq!(
            state.harness,
            HarnessState::Failed {
                class: FailureClass::Timeout,
                message: "clock exceeded".to_string()
            }
        );
        assert_eq!(
            state.dispatch,
            DispatchPhase::Failed {
                class: FailureClass::Timeout,
                message: "clock exceeded".to_string()
            }
        );
    }

    #[test]
    fn fold_cancelled() {
        let spec = sample_spec();
        let events = started(&spec, EventPayload::RunCancelled);
        let state = fold(&spec, &events);
        assert_eq!(state.harness, HarnessState::Cancelled);
        assert_eq!(state.dispatch, DispatchPhase::Cancelled);
    }

    #[test]
    fn fold_expired_is_failed_timeout() {
        let spec = sample_spec();
        let events = started(&spec, EventPayload::RunExpired);
        let state = fold(&spec, &events);
        assert_eq!(
            state.harness,
            HarnessState::Failed {
                class: FailureClass::Timeout,
                message: "run expired".to_string()
            }
        );
        assert_eq!(state.dispatch, DispatchPhase::Expired);
    }

    #[test]
    fn terminal_phase_sticks() {
        let spec = sample_spec();
        let mut events = started(
            &spec,
            EventPayload::RunCompleted {
                outcome: "shipped".to_string(),
            },
        );
        events.push(ev(
            &spec,
            EventPayload::RunFailed {
                class: FailureClass::Agent,
                message: "late".to_string(),
            },
        ));
        assert_eq!(
            fold(&spec, &events).harness,
            HarnessState::Completed {
                outcome: "shipped".to_string()
            }
        );
    }

    #[test]
    fn scheduling_events_do_not_move_the_harness() {
        let spec = sample_spec();
        let events = vec![
            ev(&spec, EventPayload::RunCreated),
            ev(&spec, EventPayload::RunQueued),
            ev(&spec, EventPayload::RunScheduled),
            ev(&spec, EventPayload::RunProvisioning),
            ev(&spec, EventPayload::RunStarting),
            ev(&spec, EventPayload::RunRecovering),
        ];
        let state = fold(&spec, &events);
        assert_eq!(state.harness, HarnessState::Idle);
        assert_eq!(state.dispatch, DispatchPhase::Starting);
    }

    #[test]
    fn scheduling_ladder_sets_literal_dispatch_phases() {
        let spec = sample_spec();
        let queued = fold(&spec, &[ev(&spec, EventPayload::RunQueued)]);
        assert_eq!(queued.harness, HarnessState::Idle);
        assert_eq!(queued.dispatch, DispatchPhase::Queued);

        let scheduled = fold(
            &spec,
            &[
                ev(&spec, EventPayload::RunQueued),
                ev(&spec, EventPayload::RunScheduled),
            ],
        );
        assert_eq!(scheduled.harness, HarnessState::Idle);
        assert_eq!(scheduled.dispatch, DispatchPhase::Scheduled);

        let provisioning = fold(
            &spec,
            &[
                ev(&spec, EventPayload::RunQueued),
                ev(&spec, EventPayload::RunScheduled),
                ev(&spec, EventPayload::RunProvisioning),
            ],
        );
        assert_eq!(provisioning.harness, HarnessState::Idle);
        assert_eq!(provisioning.dispatch, DispatchPhase::Provisioning);

        let ladder = fold(
            &spec,
            &[
                ev(&spec, EventPayload::RunQueued),
                ev(&spec, EventPayload::RunScheduled),
                ev(&spec, EventPayload::RunProvisioning),
                ev(&spec, EventPayload::RunStarting),
            ],
        );
        assert_eq!(ladder.harness, HarnessState::Idle);
        assert_eq!(ladder.dispatch, DispatchPhase::Starting);
    }

    #[test]
    fn scheduling_event_does_not_move_harness_state() {
        let spec = sample_spec();
        let started = fold(&spec, &[ev(&spec, EventPayload::RunStarted)]);
        assert_eq!(
            started.harness,
            HarnessState::Running {
                step: 1,
                attempt: 0,
                answered: false
            }
        );
        assert_eq!(started.dispatch, DispatchPhase::Running);

        let mut events = vec![ev(&spec, EventPayload::RunStarted)];
        events.push(ev(&spec, EventPayload::RunScheduled));
        events.push(ev(
            &spec,
            EventPayload::RunWaiting {
                reason: "tool".to_string(),
            },
        ));
        events.push(ev(
            &spec,
            EventPayload::RunAwaitingApproval {
                approval_id: ApprovalId::new(),
            },
        ));
        events.push(ev(&spec, EventPayload::RunPaused));
        events.push(ev(&spec, EventPayload::RunRecovering));
        let state = fold(&spec, &events);
        assert_eq!(state.harness, started.harness);
        assert_eq!(
            state.dispatch,
            DispatchPhase::Waiting {
                reason: "tool".to_string()
            }
        );

        let approval = ApprovalId::new();
        let awaiting = fold(
            &spec,
            &[
                ev(&spec, EventPayload::RunStarted),
                ev(
                    &spec,
                    EventPayload::RunAwaitingApproval {
                        approval_id: approval,
                    },
                ),
            ],
        );
        assert_eq!(awaiting.harness, started.harness);
        assert_eq!(
            awaiting.dispatch,
            DispatchPhase::AwaitingApproval {
                approval_id: approval
            }
        );

        let paused = fold(
            &spec,
            &[
                ev(&spec, EventPayload::RunStarted),
                ev(&spec, EventPayload::RunPaused),
            ],
        );
        assert_eq!(paused.harness, started.harness);
        assert_eq!(paused.dispatch, DispatchPhase::Paused);

        let recovering = fold(
            &spec,
            &[
                ev(&spec, EventPayload::RunStarted),
                ev(&spec, EventPayload::RunRecovering),
            ],
        );
        assert_eq!(recovering.harness, started.harness);
        assert_eq!(recovering.dispatch, DispatchPhase::Recovering);
    }
}
