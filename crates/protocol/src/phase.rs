use serde::{Deserialize, Serialize};

use crate::{ApprovalId, InvocationId, MessageId};

pub const MAX_RETRIES: u32 = 2;

/// How deep a chain of delegations may go: a run this many hops from its
/// root may not start another.
pub const MAX_DELEGATION_HOPS: u32 = 8;

/// How many child runs one run may start.
pub const MAX_CHILDREN: u32 = 10;

/// The largest message body, in bytes (decision 30A).
pub const MAX_MESSAGE_BYTES: usize = 32 * 1024;

/// The longest an ask may wait for its reply, in seconds (decision 28A).
pub const MAX_ASK_TIMEOUT_SECS: u32 = 24 * 60 * 60;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HarnessState {
    Idle,
    Running {
        step: u32,
        attempt: u32,
        answered: bool,
    },
    WaitingForTool {
        step: u32,
        attempt: u32,
        name: String,
        invocation: InvocationId,
    },
    /// An ask was sent from this step, which waits for its reply (or its
    /// timeout) before the run goes on (Phase 2).
    WaitingForMessage {
        step: u32,
        attempt: u32,
        message_id: MessageId,
    },
    Completed {
        outcome: String,
    },
    Failed {
        class: FailureClass,
        message: String,
    },
    Cancelled,
}

impl HarnessState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. } | Self::Failed { .. } | Self::Cancelled
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DispatchPhase {
    Created,
    Queued,
    Scheduled,
    Provisioning,
    Starting,
    Running,
    Waiting {
        reason: String,
    },
    AwaitingApproval {
        approval_id: ApprovalId,
    },
    Paused,
    Recovering,
    Completed {
        outcome: String,
    },
    Failed {
        class: FailureClass,
        message: String,
    },
    Cancelled,
    Expired,
}

impl DispatchPhase {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. } | Self::Failed { .. } | Self::Cancelled | Self::Expired
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailureClass {
    Agent,
    Model,
    Tool,
    Policy,
    Environment,
    Infrastructure,
    Timeout,
    Budget,
    Dependency,
    UserCancellation,
}
