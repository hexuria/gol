use serde::{Deserialize, Serialize};

use crate::{AgentId, ApprovalId, ArtifactId, InvocationId, RunSpec};

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

/// The metadata key that names a run's session, for session memory.
pub const SESSION_ID: &str = "session_id";
/// The metadata key that names a run's workspace, for workspace memory.
pub const WORKSPACE_ID: &str = "workspace_id";

/// Whose memory `scope` is, for a run of `spec` at step `step`: the id that,
/// with the scope, keys the memory (owner decisions 1A and 3A for C3).
///
/// A user is the issuer and subject, as `Owner::is` compares them; an
/// organization is the issuer and tenant. Session and workspace ids come
/// from the caller's metadata, so a session belongs to its user and a
/// workspace to its organization. Parts are length-prefixed, so no two
/// different owners share an id. `None` when the metadata names no session or
/// workspace (the authorizer denies those effects, decision 2B).
pub fn memory_owner_id(spec: &RunSpec, scope: MemoryScope, step: u32) -> Option<String> {
    let owner = &spec.owner;
    let user = || joined(&[&owner.issuer, &owner.subject]);
    let organization = || joined(&[&owner.issuer, &owner.tenant]);
    let named = |key: &str| {
        spec.metadata
            .get(key)
            .filter(|id| !id.is_empty())
            .map(|id| joined(&[id]))
    };
    Some(match scope {
        MemoryScope::Step => format!("{}/{step}", spec.run_id),
        MemoryScope::Run => spec.run_id.to_string(),
        MemoryScope::Agent => spec.agent_id.to_string(),
        MemoryScope::User => user(),
        MemoryScope::Organization => organization(),
        MemoryScope::Session => user() + &named(SESSION_ID)?,
        MemoryScope::Workspace => organization() + &named(WORKSPACE_ID)?,
        MemoryScope::Global => String::new(),
    })
}

/// Each part as `<byte length>:<part>`, one after another.
fn joined(parts: &[&str]) -> String {
    parts
        .iter()
        .map(|part| format!("{}:{part}", part.len()))
        .collect()
}

#[cfg(test)]
mod memory_owner_tests {
    use super::*;
    use crate::spec::sample_spec;

    fn with_metadata(pairs: &[(&str, &str)]) -> RunSpec {
        let mut spec = sample_spec();
        for (key, value) in pairs {
            spec.metadata.insert(key.to_string(), value.to_string());
        }
        spec
    }

    #[test]
    fn each_scope_names_its_owner() {
        let spec = with_metadata(&[(SESSION_ID, "s-1"), (WORKSPACE_ID, "w-1")]);
        let owner = |scope| memory_owner_id(&spec, scope, 3);
        let user = "19:https://issuer.test6:user-1";
        let organization = "19:https://issuer.test8:tenant-1";
        assert_eq!(owner(MemoryScope::Run), Some(spec.run_id.to_string()));
        assert_eq!(owner(MemoryScope::Step), Some(format!("{}/3", spec.run_id)));
        assert_eq!(owner(MemoryScope::Agent), Some(spec.agent_id.to_string()));
        assert_eq!(owner(MemoryScope::User), Some(user.to_string()));
        assert_eq!(
            owner(MemoryScope::Organization),
            Some(organization.to_string())
        );
        assert_eq!(owner(MemoryScope::Session), Some(format!("{user}3:s-1")));
        assert_eq!(
            owner(MemoryScope::Workspace),
            Some(format!("{organization}3:w-1"))
        );
        assert_eq!(owner(MemoryScope::Global), Some(String::new()));
    }

    // Session and workspace ids come from the caller's metadata, so they are
    // kept apart per user and per organization: naming another user's session
    // id reaches a different owner.
    #[test]
    fn a_session_id_is_scoped_to_its_user() {
        let alice = with_metadata(&[(SESSION_ID, "shared")]);
        let mut bob = with_metadata(&[(SESSION_ID, "shared")]);
        bob.owner.subject = "user-2".to_string();
        assert_ne!(
            memory_owner_id(&alice, MemoryScope::Session, 0),
            memory_owner_id(&bob, MemoryScope::Session, 0)
        );
        let mut other_tenant = with_metadata(&[(WORKSPACE_ID, "w")]);
        other_tenant.owner.tenant = "tenant-2".to_string();
        assert_ne!(
            memory_owner_id(
                &with_metadata(&[(WORKSPACE_ID, "w")]),
                MemoryScope::Workspace,
                0
            ),
            memory_owner_id(&other_tenant, MemoryScope::Workspace, 0)
        );
    }

    // Length prefixes keep the parts apart: moving a character from the
    // issuer into the subject changes the owner.
    #[test]
    fn owner_parts_cannot_run_together() {
        let mut first = sample_spec();
        first.owner.issuer = "ab".to_string();
        first.owner.subject = "c".to_string();
        let mut second = sample_spec();
        second.owner.issuer = "a".to_string();
        second.owner.subject = "bc".to_string();
        assert_ne!(
            memory_owner_id(&first, MemoryScope::User, 0),
            memory_owner_id(&second, MemoryScope::User, 0)
        );
    }

    #[test]
    fn a_missing_or_empty_session_or_workspace_id_names_no_owner() {
        let spec = with_metadata(&[(WORKSPACE_ID, "")]);
        assert_eq!(memory_owner_id(&spec, MemoryScope::Session, 0), None);
        assert_eq!(memory_owner_id(&spec, MemoryScope::Workspace, 0), None);
    }
}
