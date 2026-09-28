use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::{
    AgentId, ApprovalId, Effect, EventId, FailureClass, InvocationId, Limits, MemoryScope,
    ModelMessage, RunId, RunSpec, StepId,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(i64);

impl Timestamp {
    pub fn unix_millis(millis: i64) -> Self {
        Self(millis)
    }

    pub fn now() -> Self {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        Self(millis)
    }

    pub fn as_unix_millis(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Actor {
    System,
    Agent,
    Policy,
    Tool,
    Gateway,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: EventId,
    pub event_type: String,
    pub run_id: RunId,
    pub step_id: Option<StepId>,
    pub parent_run_id: Option<RunId>,
    pub agent_id: AgentId,
    pub agent_version: String,
    pub at: Timestamp,
    pub actor: Actor,
    pub caused_by: Option<EventId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventPayload {
    RunCreated,
    RunQueued,
    RunScheduled,
    RunProvisioning,
    RunStarting,
    RunStarted,
    RunWaiting {
        reason: String,
    },
    RunAwaitingApproval {
        approval_id: ApprovalId,
    },
    RunPaused,
    RunRecovering,
    RunResumed,
    RunCompleted {
        outcome: String,
    },
    RunFailed {
        class: FailureClass,
        message: String,
    },
    RunCancelled,
    RunExpired,
    UserMessage {
        text: String,
    },
    StepRetried,
    StepAdvanced,
    EffectDecided {
        effect: Effect,
    },
    EffectAuthorized {
        effect: Effect,
    },
    EffectDenied {
        effect: Effect,
        reason: String,
    },
    ToolResult {
        name: String,
        invocation: InvocationId,
        step: u32,
        attempt: u32,
        output: String,
    },
    ModelResponded {
        message: ModelMessage,
    },
    MemoryRead {
        scope: MemoryScope,
        key: String,
        value: Option<String>,
    },
    MemoryWritten {
        scope: MemoryScope,
        key: String,
        value: String,
    },
    /// An authorized delegation started a child run, and gave it `limits`
    /// from this run's budget.
    ChildStarted {
        run_id: RunId,
        agent_id: AgentId,
        limits: Limits,
    },
    /// An authorized delegation started nothing, for `reason`.
    DelegateRefused {
        agent_id: AgentId,
        reason: String,
    },
}

impl EventPayload {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::RunCreated => "run.created",
            Self::RunQueued => "run.queued",
            Self::RunScheduled => "run.scheduled",
            Self::RunProvisioning => "run.provisioning",
            Self::RunStarting => "run.starting",
            Self::RunStarted => "run.started",
            Self::RunWaiting { .. } => "run.waiting",
            Self::RunAwaitingApproval { .. } => "run.awaiting_approval",
            Self::RunPaused => "run.paused",
            Self::RunRecovering => "run.recovering",
            Self::RunResumed => "run.resumed",
            Self::RunCompleted { .. } => "run.completed",
            Self::RunFailed { .. } => "run.failed",
            Self::RunCancelled => "run.cancelled",
            Self::RunExpired => "run.expired",
            Self::UserMessage { .. } => "message.user",
            Self::StepRetried => "step.retried",
            Self::StepAdvanced => "step.advanced",
            Self::EffectDecided { .. } => "effect.decided",
            Self::EffectAuthorized { .. } => "effect.authorized",
            Self::EffectDenied { .. } => "effect.denied",
            Self::ToolResult { .. } => "tool.result",
            Self::ModelResponded { .. } => "model.responded",
            Self::MemoryRead { .. } => "memory.read",
            Self::MemoryWritten { .. } => "memory.written",
            Self::ChildStarted { .. } => "child.started",
            Self::DelegateRefused { .. } => "delegate.refused",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub envelope: EventEnvelope,
    pub payload: EventPayload,
}

pub struct EventSource<'a> {
    pub run_id: RunId,
    pub parent_run_id: Option<RunId>,
    pub agent_id: AgentId,
    pub agent_version: &'a str,
    pub step_id: Option<StepId>,
    pub actor: Actor,
    pub caused_by: Option<EventId>,
    pub at: Timestamp,
}

impl<'a> EventSource<'a> {
    pub fn new(
        run_id: RunId,
        agent_id: AgentId,
        agent_version: &'a str,
        actor: Actor,
        at: Timestamp,
    ) -> Self {
        Self {
            run_id,
            parent_run_id: None,
            agent_id,
            agent_version,
            step_id: None,
            actor,
            caused_by: None,
            at,
        }
    }

    /// A source for an event of `spec`'s run, naming its parent if it has
    /// one.
    pub fn for_spec(spec: &'a RunSpec, actor: Actor, at: Timestamp) -> Self {
        Self {
            parent_run_id: spec.lineage.parent,
            ..Self::new(spec.run_id, spec.agent_id, &spec.agent_version, actor, at)
        }
    }
}

impl Event {
    pub fn record(source: EventSource<'_>, payload: EventPayload) -> Self {
        Self {
            envelope: EventEnvelope {
                event_id: EventId::new(),
                event_type: payload.event_type().to_string(),
                run_id: source.run_id,
                step_id: source.step_id,
                parent_run_id: source.parent_run_id,
                agent_id: source.agent_id,
                agent_version: source.agent_version.to_string(),
                at: source.at,
                actor: source.actor,
                caused_by: source.caused_by,
            },
            payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::sample_spec;
    use crate::RunSpec;

    // Every event of a child run names its parent; a top-level run's names
    // none.
    #[test]
    fn an_event_of_a_child_names_its_parent() {
        let parent = sample_spec();
        let child = RunSpec::builder()
            .owner(parent.owner.clone())
            .agent(AgentId::new(), "1")
            .input("draft")
            .placement(parent.placement)
            .work_model(parent.work_model.clone())
            .child_of(&parent, 1)
            .build();
        let at = Timestamp::unix_millis(0);
        let of = |spec: &RunSpec| {
            Event::record(
                EventSource::for_spec(spec, Actor::System, at),
                EventPayload::RunStarted,
            )
        };
        assert_eq!(of(&parent).envelope.parent_run_id, None);
        assert_eq!(of(&child).envelope.parent_run_id, Some(parent.run_id));
        assert_eq!(of(&child).envelope.run_id, child.run_id);
    }
}
