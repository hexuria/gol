//! Every bounded (state, event) pair of the production reducers.
//!
//! This owns what the retired Lean copy (`Harness.lean`) decided over a copy of the reducer:
//! validity is preserved, a lexicographic rank strictly drops on every change,
//! terminal states absorb every event, and a pair outside the transition table
//! leaves the state unchanged. Here the subject is `protocol::reduce` and
//! `protocol::reduce_dispatch` themselves. Each reducer is checked against a
//! total table: the exact next state and, for the harness, the exact effects
//! of every pair.
//!
//! The alphabets come from matches with no `_` arm (`payload_index`,
//! `effect_index`, `phase_index`, `class_index`), so adding a variant to
//! `EventPayload`, `Effect`, `DispatchPhase` or `FailureClass` stops this file
//! from compiling until the variant is enumerated here.

use protocol::{
    reduce, reduce_dispatch, Actor, AgentId, ApprovalId, ArtifactId, CredentialSource,
    DispatchPhase, Effect, Event, EventPayload, EventSource, ExecutionPlacement, FailureClass,
    HarnessState, InvocationId, Limits, MemoryScope, MessageRole, ModelMessage, ModelProvider,
    RunId, RunSpec, Timestamp, WorkModel, MAX_RETRIES,
};
use uuid::Uuid;

const TOOL: &str = "echo";
const OTHER_TOOL: &str = "other";
const MAX_STEPS: [u32; 5] = [0, 1, 2, 3, 9];

fn invocation() -> InvocationId {
    InvocationId::from_uuid(Uuid::from_u128(1))
}

fn other_invocation() -> InvocationId {
    InvocationId::from_uuid(Uuid::from_u128(2))
}

fn spec(max_steps: u32) -> RunSpec {
    RunSpec::builder()
        .agent(AgentId::from_uuid(Uuid::from_u128(3)), "1")
        .input("hello")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .limits(Limits {
            max_steps,
            max_model_calls: 4,
        })
        .build()
}

fn event(payload: EventPayload) -> Event {
    Event::record(
        EventSource::new(
            RunId::from_uuid(Uuid::from_u128(4)),
            AgentId::from_uuid(Uuid::from_u128(3)),
            "1",
            Actor::System,
            Timestamp::unix_millis(0),
        ),
        payload,
    )
}

fn classes() -> Vec<FailureClass> {
    let all = vec![
        FailureClass::Agent,
        FailureClass::Model,
        FailureClass::Tool,
        FailureClass::Policy,
        FailureClass::Environment,
        FailureClass::Infrastructure,
        FailureClass::Timeout,
        FailureClass::Budget,
        FailureClass::Dependency,
        FailureClass::UserCancellation,
    ];
    assert_covers(all.iter().map(class_index), 10);
    all
}

fn class_index(class: &FailureClass) -> usize {
    match class {
        FailureClass::Agent => 0,
        FailureClass::Model => 1,
        FailureClass::Tool => 2,
        FailureClass::Policy => 3,
        FailureClass::Environment => 4,
        FailureClass::Infrastructure => 5,
        FailureClass::Timeout => 6,
        FailureClass::Budget => 7,
        FailureClass::Dependency => 8,
        FailureClass::UserCancellation => 9,
    }
}

fn tool_call(name: &str, invocation: InvocationId) -> Effect {
    Effect::ToolCall {
        name: name.to_string(),
        input: "x".to_string(),
        invocation,
    }
}

fn effects() -> Vec<Effect> {
    let all = vec![
        Effect::ModelCall {
            prompt: "p".to_string(),
        },
        tool_call(TOOL, invocation()),
        tool_call(OTHER_TOOL, other_invocation()),
        Effect::MemoryRead {
            scope: MemoryScope::Run,
            key: "k".to_string(),
        },
        Effect::MemoryWrite {
            scope: MemoryScope::Run,
            key: "k".to_string(),
            value: "v".to_string(),
        },
        Effect::Execute {
            command: "true".to_string(),
        },
        Effect::Delegate {
            agent_id: AgentId::from_uuid(Uuid::from_u128(5)),
            input: "x".to_string(),
        },
        Effect::AskUser {
            prompt: "?".to_string(),
        },
        Effect::RequestApproval {
            approval_id: ApprovalId::from_uuid(Uuid::from_u128(6)),
            reason: "r".to_string(),
        },
        Effect::Wait {
            reason: "r".to_string(),
        },
        Effect::PublishArtifact {
            artifact_id: ArtifactId::from_uuid(Uuid::from_u128(7)),
            name: "a".to_string(),
            body: "b".to_string(),
        },
        Effect::Complete {
            outcome: "done".to_string(),
        },
    ];
    assert_covers(all.iter().map(effect_index), 11);
    all
}

fn effect_index(effect: &Effect) -> usize {
    match effect {
        Effect::ModelCall { .. } => 0,
        Effect::ToolCall { .. } => 1,
        Effect::MemoryRead { .. } => 2,
        Effect::MemoryWrite { .. } => 3,
        Effect::Execute { .. } => 4,
        Effect::Delegate { .. } => 5,
        Effect::AskUser { .. } => 6,
        Effect::RequestApproval { .. } => 7,
        Effect::Wait { .. } => 8,
        Effect::PublishArtifact { .. } => 9,
        Effect::Complete { .. } => 10,
    }
}

fn tool_result(name: &str, invocation: InvocationId, step: u32, attempt: u32) -> EventPayload {
    EventPayload::ToolResult {
        name: name.to_string(),
        invocation,
        step,
        attempt,
        output: "out".to_string(),
    }
}

/// Every payload kind, with a matching tool result and one mismatch per field
/// for each (step, attempt) the harness can wait on.
fn payloads(max_steps: u32) -> Vec<EventPayload> {
    let mut all = vec![
        EventPayload::RunCreated,
        EventPayload::RunQueued,
        EventPayload::RunScheduled,
        EventPayload::RunProvisioning,
        EventPayload::RunStarting,
        EventPayload::RunStarted,
        EventPayload::RunWaiting {
            reason: "r".to_string(),
        },
        EventPayload::RunAwaitingApproval {
            approval_id: ApprovalId::from_uuid(Uuid::from_u128(6)),
        },
        EventPayload::RunPaused,
        EventPayload::RunRecovering,
        EventPayload::RunResumed,
        EventPayload::RunCompleted {
            outcome: "done".to_string(),
        },
        EventPayload::RunCancelled,
        EventPayload::RunExpired,
        EventPayload::UserMessage {
            text: "hi".to_string(),
        },
        EventPayload::StepRetried,
        EventPayload::StepAdvanced,
        EventPayload::ModelResponded {
            message: ModelMessage {
                role: MessageRole::Assistant,
                text: "t".to_string(),
            },
        },
        EventPayload::MemoryRead {
            scope: MemoryScope::Run,
            key: "k".to_string(),
            value: None,
        },
        EventPayload::MemoryWritten {
            scope: MemoryScope::Run,
            key: "k".to_string(),
            value: "v".to_string(),
        },
    ];
    for class in classes() {
        all.push(EventPayload::RunFailed {
            class,
            message: "m".to_string(),
        });
    }
    for effect in effects() {
        all.push(EventPayload::EffectDecided {
            effect: effect.clone(),
        });
        all.push(EventPayload::EffectAuthorized {
            effect: effect.clone(),
        });
        all.push(EventPayload::EffectDenied {
            effect,
            reason: "no".to_string(),
        });
    }
    for step in 1..=max_steps.max(1) {
        for attempt in 0..=MAX_RETRIES {
            all.push(tool_result(TOOL, invocation(), step, attempt));
            all.push(tool_result(OTHER_TOOL, invocation(), step, attempt));
            all.push(tool_result(TOOL, other_invocation(), step, attempt));
            all.push(tool_result(TOOL, invocation(), step + 1, attempt));
            all.push(tool_result(TOOL, invocation(), step, attempt + 1));
        }
    }
    assert_covers(all.iter().map(payload_index), 25);
    all
}

fn payload_index(payload: &EventPayload) -> usize {
    match payload {
        EventPayload::RunCreated => 0,
        EventPayload::RunQueued => 1,
        EventPayload::RunScheduled => 2,
        EventPayload::RunProvisioning => 3,
        EventPayload::RunStarting => 4,
        EventPayload::RunStarted => 5,
        EventPayload::RunWaiting { .. } => 6,
        EventPayload::RunAwaitingApproval { .. } => 7,
        EventPayload::RunPaused => 8,
        EventPayload::RunRecovering => 9,
        EventPayload::RunResumed => 10,
        EventPayload::RunCompleted { .. } => 11,
        EventPayload::RunFailed { .. } => 12,
        EventPayload::RunCancelled => 13,
        EventPayload::RunExpired => 14,
        EventPayload::UserMessage { .. } => 15,
        EventPayload::StepRetried => 16,
        EventPayload::StepAdvanced => 17,
        EventPayload::EffectDecided { .. } => 18,
        EventPayload::EffectAuthorized { .. } => 19,
        EventPayload::EffectDenied { .. } => 20,
        EventPayload::ToolResult { .. } => 21,
        EventPayload::ModelResponded { .. } => 22,
        EventPayload::MemoryRead { .. } => 23,
        EventPayload::MemoryWritten { .. } => 24,
    }
}

fn assert_covers(indices: impl Iterator<Item = usize>, count: usize) {
    let mut seen = vec![false; count];
    for index in indices {
        seen[index] = true;
    }
    assert!(
        seen.iter().all(|hit| *hit),
        "an enum variant is not enumerated"
    );
}

/// Every valid harness state at this bound, plus one of each terminal kind.
fn harness_states(max_steps: u32) -> Vec<HarnessState> {
    let mut all = vec![HarnessState::Idle, HarnessState::Cancelled];
    all.push(HarnessState::Completed {
        outcome: "done".to_string(),
    });
    for class in classes() {
        all.push(HarnessState::Failed {
            class,
            message: "m".to_string(),
        });
    }
    for step in 1..=max_steps.max(1) {
        for attempt in 0..=MAX_RETRIES {
            for answered in [false, true] {
                all.push(HarnessState::Running {
                    step,
                    attempt,
                    answered,
                });
            }
        }
    }
    for step in 1..=max_steps {
        for attempt in 0..=MAX_RETRIES {
            all.push(HarnessState::WaitingForTool {
                step,
                attempt,
                name: TOOL.to_string(),
                invocation: invocation(),
            });
        }
    }
    all
}

/// The validity the Lean model called `Valid`, over the Rust state.
fn valid(state: &HarnessState, max_steps: u32) -> bool {
    match state {
        HarnessState::Running { step, attempt, .. } => {
            (1..=max_steps.max(1)).contains(step) && *attempt <= MAX_RETRIES
        }
        HarnessState::WaitingForTool { step, attempt, .. } => {
            (1..=max_steps).contains(step) && *attempt <= MAX_RETRIES
        }
        HarnessState::Idle
        | HarnessState::Completed { .. }
        | HarnessState::Failed { .. }
        | HarnessState::Cancelled => true,
    }
}

/// Lexicographic: terminal < running or waiting < idle. Among active states,
/// fewer steps left, then fewer retries left, then answered < waiting <
/// unanswered.
fn rank(state: &HarnessState, max_steps: u32) -> (u8, i64, i64, u8) {
    let left = |step: u32, attempt: u32| {
        (
            i64::from(max_steps) - i64::from(step),
            i64::from(MAX_RETRIES) - i64::from(attempt),
        )
    };
    match state {
        HarnessState::Completed { .. } | HarnessState::Failed { .. } | HarnessState::Cancelled => {
            (0, 0, 0, 0)
        }
        HarnessState::Idle => (2, 0, 0, 0),
        HarnessState::Running {
            step,
            attempt,
            answered,
        } => {
            let (steps, retries) = left(*step, *attempt);
            (1, steps, retries, if *answered { 1 } else { 3 })
        }
        HarnessState::WaitingForTool { step, attempt, .. } => {
            let (steps, retries) = left(*step, *attempt);
            (1, steps, retries, 2)
        }
    }
}

/// The transition table: when (state, payload) may change the harness state.
/// Every other pair must be the identity.
fn may_change(state: &HarnessState, payload: &EventPayload, max_steps: u32) -> bool {
    use EventPayload as P;
    use HarnessState as H;
    match (state, payload) {
        (H::Idle, P::RunStarted) => true,
        (
            H::Running {
                answered: false,
                step,
                ..
            },
            P::EffectAuthorized { effect },
        ) => match effect {
            Effect::ToolCall { .. } => (1..=max_steps).contains(step),
            Effect::Complete { .. } => true,
            _ => false,
        },
        (H::Running { .. }, P::EffectAuthorized { effect }) => {
            matches!(effect, Effect::Complete { .. })
        }
        (
            H::WaitingForTool {
                step,
                attempt,
                name,
                invocation,
            },
            P::ToolResult {
                name: result_name,
                invocation: result_invocation,
                step: result_step,
                attempt: result_attempt,
                ..
            },
        ) => {
            name == result_name
                && invocation == result_invocation
                && step == result_step
                && attempt == result_attempt
        }
        (
            H::Running {
                answered: true,
                attempt,
                ..
            },
            P::StepRetried,
        ) => *attempt < MAX_RETRIES,
        (
            H::Running {
                answered: true,
                step,
                ..
            },
            P::StepAdvanced,
        ) => *step < max_steps,
        (H::Running { .. }, P::RunCompleted { .. }) => true,
        (
            H::Running { .. } | H::WaitingForTool { .. },
            P::RunFailed { .. } | P::RunCancelled | P::RunExpired,
        ) => true,
        _ => false,
    }
}

/// The exact next state for every change `may_change` allows.
fn expected_next(state: &HarnessState, payload: &EventPayload) -> HarnessState {
    use EventPayload as P;
    use HarnessState as H;
    match (state, payload) {
        (H::Idle, P::RunStarted) => H::Running {
            step: 1,
            attempt: 0,
            answered: false,
        },
        (H::Running { step, attempt, .. }, P::EffectAuthorized { effect }) => match effect {
            Effect::ToolCall {
                name, invocation, ..
            } => H::WaitingForTool {
                step: *step,
                attempt: *attempt,
                name: name.clone(),
                invocation: *invocation,
            },
            Effect::Complete { outcome } => H::Completed {
                outcome: outcome.clone(),
            },
            other => panic!("no change expected for {other:?}"),
        },
        (H::WaitingForTool { step, attempt, .. }, P::ToolResult { .. }) => H::Running {
            step: *step,
            attempt: *attempt,
            answered: true,
        },
        (H::Running { step, attempt, .. }, P::StepRetried) => H::Running {
            step: *step,
            attempt: attempt + 1,
            answered: false,
        },
        (H::Running { step, .. }, P::StepAdvanced) => H::Running {
            step: step + 1,
            attempt: 0,
            answered: false,
        },
        (_, P::RunCompleted { outcome }) => H::Completed {
            outcome: outcome.clone(),
        },
        (_, P::RunFailed { class, message }) => H::Failed {
            class: *class,
            message: message.clone(),
        },
        (_, P::RunCancelled) => H::Cancelled,
        (_, P::RunExpired) => H::Failed {
            class: FailureClass::Timeout,
            message: "run expired".to_string(),
        },
        (state, payload) => panic!("no change expected for {state:?} + {payload:?}"),
    }
}

/// The exact effects of every pair. An authorized model call or memory
/// access passes through from any running step; an authorized tool call only
/// from an unanswered step inside the budget. Nothing else emits.
fn expected_effects(state: &HarnessState, payload: &EventPayload, max_steps: u32) -> Vec<Effect> {
    let (HarnessState::Running { step, answered, .. }, EventPayload::EffectAuthorized { effect }) =
        (state, payload)
    else {
        return Vec::new();
    };
    let emits = match effect {
        Effect::ToolCall { .. } => !answered && (1..=max_steps).contains(step),
        Effect::ModelCall { .. } | Effect::MemoryRead { .. } | Effect::MemoryWrite { .. } => true,
        Effect::Execute { .. }
        | Effect::Delegate { .. }
        | Effect::AskUser { .. }
        | Effect::RequestApproval { .. }
        | Effect::Wait { .. }
        | Effect::PublishArtifact { .. }
        | Effect::Complete { .. } => false,
    };
    if emits {
        vec![effect.clone()]
    } else {
        Vec::new()
    }
}

#[test]
fn harness_reduce_keeps_validity_rank_and_table_on_every_bounded_pair() {
    let mut checked = 0usize;
    for max_steps in MAX_STEPS {
        let spec = spec(max_steps);
        let payloads = payloads(max_steps);
        for state in harness_states(max_steps) {
            assert!(valid(&state, max_steps), "enumerated an invalid state");
            for payload in &payloads {
                let (next, effects) = reduce(state.clone(), &event(payload.clone()), &spec);
                let at = || format!("max_steps={max_steps} {state:?} + {payload:?} -> {next:?}");
                checked += 1;

                assert!(valid(&next, max_steps), "validity lost: {}", at());

                let changed = next != state;
                assert_eq!(
                    changed,
                    may_change(&state, payload, max_steps),
                    "outside the transition table: {}",
                    at()
                );
                if changed {
                    assert_eq!(
                        next,
                        expected_next(&state, payload),
                        "wrong next state: {}",
                        at()
                    );
                    assert!(
                        rank(&next, max_steps) < rank(&state, max_steps),
                        "rank did not drop: {}",
                        at()
                    );
                }

                if state.is_terminal() {
                    assert_eq!(next, state, "terminal state moved: {}", at());
                    assert!(effects.is_empty(), "terminal state emitted: {}", at());
                }

                assert_eq!(
                    effects,
                    expected_effects(&state, payload, max_steps),
                    "wrong effects: {}",
                    at()
                );
                if effects
                    .iter()
                    .any(|effect| matches!(effect, Effect::ToolCall { .. }))
                {
                    assert!(
                        matches!(
                            (&state, &next),
                            (
                                HarnessState::Running {
                                    answered: false,
                                    ..
                                },
                                HarnessState::WaitingForTool { .. }
                            )
                        ),
                        "tool call outside an unanswered running step: {}",
                        at()
                    );
                }
            }
        }
    }
    assert!(checked > 10_000, "only {checked} pairs");
}

fn dispatch_phases() -> Vec<DispatchPhase> {
    let all = vec![
        DispatchPhase::Created,
        DispatchPhase::Queued,
        DispatchPhase::Scheduled,
        DispatchPhase::Provisioning,
        DispatchPhase::Starting,
        DispatchPhase::Running,
        DispatchPhase::Waiting {
            reason: "r".to_string(),
        },
        DispatchPhase::AwaitingApproval {
            approval_id: ApprovalId::from_uuid(Uuid::from_u128(6)),
        },
        DispatchPhase::Paused,
        DispatchPhase::Recovering,
        DispatchPhase::Completed {
            outcome: "done".to_string(),
        },
        DispatchPhase::Failed {
            class: FailureClass::Tool,
            message: "m".to_string(),
        },
        DispatchPhase::Cancelled,
        DispatchPhase::Expired,
    ];
    assert_covers(all.iter().map(phase_index), 14);
    all
}

fn phase_index(phase: &DispatchPhase) -> usize {
    match phase {
        DispatchPhase::Created => 0,
        DispatchPhase::Queued => 1,
        DispatchPhase::Scheduled => 2,
        DispatchPhase::Provisioning => 3,
        DispatchPhase::Starting => 4,
        DispatchPhase::Running => 5,
        DispatchPhase::Waiting { .. } => 6,
        DispatchPhase::AwaitingApproval { .. } => 7,
        DispatchPhase::Paused => 8,
        DispatchPhase::Recovering => 9,
        DispatchPhase::Completed { .. } => 10,
        DispatchPhase::Failed { .. } => 11,
        DispatchPhase::Cancelled => 12,
        DispatchPhase::Expired => 13,
    }
}

/// The rank `formal/harness/Dispatch.tla` uses. Terminal phases are 0.
fn dispatch_rank(phase: &DispatchPhase) -> u32 {
    match phase {
        DispatchPhase::Created => 140,
        DispatchPhase::Queued => 130,
        DispatchPhase::Scheduled => 120,
        DispatchPhase::Provisioning => 110,
        DispatchPhase::Starting => 100,
        DispatchPhase::Running => 90,
        DispatchPhase::Waiting { .. } => 80,
        DispatchPhase::AwaitingApproval { .. } => 70,
        DispatchPhase::Paused => 60,
        DispatchPhase::Recovering => 50,
        DispatchPhase::Completed { .. }
        | DispatchPhase::Failed { .. }
        | DispatchPhase::Cancelled
        | DispatchPhase::Expired => 0,
    }
}

/// The transition table of `formal/harness/Dispatch.tla`, one arm per action,
/// as a total function: every pair outside an action is the identity.
fn dispatch_expected(phase: &DispatchPhase, payload: &EventPayload) -> DispatchPhase {
    use DispatchPhase as D;
    use EventPayload as P;
    let live = matches!(
        phase,
        D::Running | D::Waiting { .. } | D::AwaitingApproval { .. } | D::Paused | D::Recovering
    );
    match (phase, payload) {
        _ if phase.is_terminal() => phase.clone(),
        // DispatchQueue, DispatchSchedule, DispatchProvision, DispatchPrepare
        (D::Created, P::RunQueued) => D::Queued,
        (D::Queued, P::RunScheduled) => D::Scheduled,
        (D::Scheduled, P::RunProvisioning) => D::Provisioning,
        (D::Provisioning, P::RunStarting) => D::Starting,
        // DispatchRun, DispatchLocalRun
        (D::Starting | D::Created, P::RunStarted) => D::Running,
        // DispatchWait, DispatchApproval, DispatchPause, DispatchRecover
        (D::Running, P::RunWaiting { reason }) => D::Waiting {
            reason: reason.clone(),
        },
        (D::Running, P::RunAwaitingApproval { approval_id }) => D::AwaitingApproval {
            approval_id: *approval_id,
        },
        (D::Running, P::RunPaused) => D::Paused,
        (D::Running, P::RunRecovering) => D::Recovering,
        // DispatchResume
        (
            D::Waiting { .. } | D::AwaitingApproval { .. } | D::Paused | D::Recovering,
            P::RunResumed,
        ) => D::Running,
        // DispatchComplete: live phases only
        (_, P::RunCompleted { outcome }) if live => D::Completed {
            outcome: outcome.clone(),
        },
        // DispatchFail, DispatchCancel, DispatchExpire: every open phase
        (_, P::RunFailed { class, message }) => D::Failed {
            class: *class,
            message: message.clone(),
        },
        (_, P::RunCancelled) => D::Cancelled,
        (_, P::RunExpired) => D::Expired,
        _ => phase.clone(),
    }
}

/// `DispatchResume`: a paused live phase returns to running. It is the only
/// change that may raise the rank (`RankDecreases` in `Dispatch.tla`).
fn is_resume(phase: &DispatchPhase, next: &DispatchPhase) -> bool {
    matches!(
        phase,
        DispatchPhase::Waiting { .. }
            | DispatchPhase::AwaitingApproval { .. }
            | DispatchPhase::Paused
            | DispatchPhase::Recovering
    ) && *next == DispatchPhase::Running
}

/// The properties `formal/harness/Dispatch.cfg` checks, on production
/// `reduce_dispatch`: the exact next phase of every pair, terminal phases stay
/// put, every change except `DispatchResume` lowers the rank, and every open
/// phase has an exit.
#[test]
fn dispatch_reduce_matches_the_table_on_every_pair() {
    let payloads = payloads(3);
    for phase in dispatch_phases() {
        let mut exits = 0;
        for payload in &payloads {
            let next = reduce_dispatch(phase.clone(), &event(payload.clone()));
            let at = || format!("{phase:?} + {payload:?} -> {next:?}");
            assert_eq!(
                next,
                dispatch_expected(&phase, payload),
                "wrong next phase: {}",
                at()
            );
            if phase.is_terminal() {
                assert_eq!(next, phase, "terminal phase moved: {}", at());
                continue;
            }
            if next != phase {
                exits += 1;
                if !is_resume(&phase, &next) {
                    assert!(
                        dispatch_rank(&next) < dispatch_rank(&phase),
                        "rank did not drop: {}",
                        at()
                    );
                }
            }
        }
        if !phase.is_terminal() {
            assert!(exits > 0, "open phase with no exit: {phase:?}");
        }
    }
}
