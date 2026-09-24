use serde::{Deserialize, Serialize};

use crate::{AgentId, ApprovalId, ArtifactId, InvocationId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum MemoryScope {
    Step,
    Run,
    Session,
    Agent,
    Workspace,
    User,
    Organization,
    Global,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Effect {
    ModelCall {
        prompt: String,
    },
    ToolCall {
        name: String,
        input: String,
        invocation: InvocationId,
    },
    MemoryRead {
        scope: MemoryScope,
        key: String,
    },
    MemoryWrite {
        scope: MemoryScope,
        key: String,
        value: String,
    },
    Execute {
        command: String,
    },
    Delegate {
        agent_id: AgentId,
        input: String,
    },
    AskUser {
        prompt: String,
    },
    RequestApproval {
        approval_id: ApprovalId,
        reason: String,
    },
    Wait {
        reason: String,
    },
    PublishArtifact {
        artifact_id: ArtifactId,
        name: String,
        body: String,
    },
    Complete {
        outcome: String,
    },
}
