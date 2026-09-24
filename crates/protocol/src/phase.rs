use serde::{Deserialize, Serialize};

use crate::ApprovalId;

pub const MAX_STEPS: u32 = 3;
pub const MAX_RETRIES: u32 = 2;

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
