use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::{
    AgentId, ApprovalId, Effect, EventId, FailureClass, InvocationId, MemoryScope, ModelMessage,
    RunId, StepId,
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
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub envelope: EventEnvelope,
    pub payload: EventPayload,
}

impl Event {
    pub fn record(
        run_id: RunId,
        agent_id: AgentId,
        agent_version: &str,
        step_id: Option<StepId>,
        actor: Actor,
        caused_by: Option<EventId>,
        at: Timestamp,
        payload: EventPayload,
    ) -> Self {
        Self {
            envelope: EventEnvelope {
                event_id: EventId::new(),
                event_type: payload.event_type().to_string(),
                run_id,
                step_id,
                parent_run_id: None,
                agent_id,
                agent_version: agent_version.to_string(),
                at,
                actor,
                caused_by,
            },
            payload,
        }
    }
}
