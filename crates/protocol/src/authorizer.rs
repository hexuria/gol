use crate::policy::PolicyDecision;
use crate::{
    memory_owner_id, Capability, Effect, MemoryScope, RunSpec, ToolDescriptor, MAX_DELEGATION_HOPS,
};

const MODEL_CALL: &str = "model.call";
const MEMORY_READ: &str = "memory.read";
const MEMORY_WRITE: &str = "memory.write";
const AGENT_DELEGATE: &str = "agent.delegate";

pub fn authorize(spec: &RunSpec, effect: &Effect, tools: &[ToolDescriptor]) -> PolicyDecision {
    match effect {
        // Always allowed. The budget rule depends on it: a Complete decided
        // while the harness is Running is not a step because it always ends
        // the run (fold, Driver::decide_with_skills). A Complete that could be
        // denied would be free and never end. Pinned by
        // complete_is_allowed_without_capabilities.
        Effect::Complete { .. } => PolicyDecision::Allow,
        Effect::ModelCall { .. } => allow_capability(spec, MODEL_CALL),
        Effect::MemoryRead { scope, .. } => allow_memory(spec, MEMORY_READ, *scope),
        Effect::MemoryWrite { scope, .. } => allow_memory(spec, MEMORY_WRITE, *scope),
        Effect::ToolCall { name, .. } => match tools.iter().find(|tool| tool.name == *name) {
            Some(tool) => allow_capability(spec, tool.required_capability.as_str()),
            None => PolicyDecision::Deny {
                reason: format!("unknown tool: {name}"),
            },
        },
        // The capability first, as for memory; then the depth.
        Effect::Delegate { .. } => match allow_capability(spec, AGENT_DELEGATE) {
            PolicyDecision::Allow if spec.lineage.hop >= MAX_DELEGATION_HOPS => {
                PolicyDecision::Deny {
                    reason: format!("delegation is already {MAX_DELEGATION_HOPS} hops deep"),
                }
            }
            decision => decision,
        },
        Effect::Execute { .. }
        | Effect::AskUser { .. }
        | Effect::RequestApproval { .. }
        | Effect::Wait { .. }
        | Effect::PublishArtifact { .. } => PolicyDecision::Deny {
            reason: "effect is not implemented in this slice".to_string(),
        },
    }
}

/// The capability, and for session and workspace memory the id that names
/// whose memory it is (owner decision 2B for C3). Global memory is denied:
/// every tenant would share it, and a manifest grants its own capabilities.
fn allow_memory(spec: &RunSpec, capability: &str, scope: MemoryScope) -> PolicyDecision {
    let decision = allow_capability(spec, capability);
    if decision != PolicyDecision::Allow {
        return decision;
    }
    if scope == MemoryScope::Global {
        return PolicyDecision::Deny {
            reason: "global memory is shared by every tenant and is not offered".to_string(),
        };
    }
    if memory_owner_id(spec, scope, 0).is_some() {
        return decision;
    }
    let key = match scope {
        MemoryScope::Workspace => crate::WORKSPACE_ID,
        _ => crate::SESSION_ID,
    };
    PolicyDecision::Deny {
        reason: format!("no {key} in the run's metadata"),
    }
}

fn allow_capability(spec: &RunSpec, name: &str) -> PolicyDecision {
    let capability = Capability::new(name);
    if spec.capabilities.iter().any(|held| held == &capability) {
        PolicyDecision::Allow
    } else {
        PolicyDecision::Deny {
            reason: format!("missing capability: {name}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::sample_spec;
    use crate::{Capability, Effect, InvocationId, ToolDescriptor, ToolId};

    fn echo() -> ToolDescriptor {
        ToolDescriptor {
            id: ToolId::new(),
            name: "echo".to_string(),
            description: "Returns the input text.".to_string(),
            input_schema: "{\"type\":\"string\"}".to_string(),
            output_schema: "{\"type\":\"string\"}".to_string(),
            required_capability: Capability::new("tool.echo"),
        }
    }

    #[test]
    fn allows_listed_tool_and_denies_a_missing_capability() {
        let mut spec = sample_spec();
        let tools = [echo()];
        let effect = Effect::ToolCall {
            name: "echo".to_string(),
            input: "ping".to_string(),
            invocation: InvocationId::new(),
        };

        assert_eq!(
            authorize(&spec, &effect, &tools),
            PolicyDecision::Deny {
                reason: "missing capability: tool.echo".to_string()
            }
        );

        spec.capabilities.push(Capability::new("tool.echo"));
        assert_eq!(authorize(&spec, &effect, &tools), PolicyDecision::Allow);
    }

    fn delegate() -> Effect {
        Effect::Delegate {
            agent_id: crate::AgentId::new(),
            input: "draft".to_string(),
        }
    }

    #[test]
    fn delegate_needs_its_capability() {
        let mut spec = sample_spec();
        assert_eq!(
            authorize(&spec, &delegate(), &[]),
            PolicyDecision::Deny {
                reason: "missing capability: agent.delegate".to_string()
            }
        );
        spec.capabilities.push(Capability::new("agent.delegate"));
        assert_eq!(authorize(&spec, &delegate(), &[]), PolicyDecision::Allow);
    }

    // A run eight hops from its root may not start a ninth.
    #[test]
    fn delegation_stops_after_eight_hops() {
        let mut spec = sample_spec();
        spec.capabilities.push(Capability::new("agent.delegate"));
        spec.lineage.hop = crate::MAX_DELEGATION_HOPS - 1;
        assert_eq!(authorize(&spec, &delegate(), &[]), PolicyDecision::Allow);
        spec.lineage.hop = crate::MAX_DELEGATION_HOPS;
        assert_eq!(
            authorize(&spec, &delegate(), &[]),
            PolicyDecision::Deny {
                reason: "delegation is already 8 hops deep".to_string()
            }
        );
        // Without the capability, that is the reason at any depth.
        spec.capabilities.clear();
        assert_eq!(
            authorize(&spec, &delegate(), &[]),
            PolicyDecision::Deny {
                reason: "missing capability: agent.delegate".to_string()
            }
        );
    }

    #[test]
    fn complete_is_allowed_without_capabilities() {
        let spec = sample_spec();
        let effect = Effect::Complete {
            outcome: "done".to_string(),
        };
        assert_eq!(authorize(&spec, &effect, &[]), PolicyDecision::Allow);
    }

    fn memory_spec(metadata: &[(&str, &str)]) -> crate::RunSpec {
        let mut spec = sample_spec();
        spec.capabilities = vec![
            Capability::new("memory.read"),
            Capability::new("memory.write"),
        ];
        for (key, value) in metadata {
            spec.metadata.insert(key.to_string(), value.to_string());
        }
        spec
    }

    fn read(scope: crate::MemoryScope) -> Effect {
        Effect::MemoryRead {
            scope,
            key: "k".to_string(),
        }
    }

    fn write(scope: crate::MemoryScope) -> Effect {
        Effect::MemoryWrite {
            scope,
            key: "k".to_string(),
            value: "v".to_string(),
        }
    }

    // Owner decision 2B for C3: session and workspace memory need their id in
    // the spec's metadata; without it the effect is denied, and the run goes on.
    #[test]
    fn session_memory_needs_a_session_id() {
        use crate::MemoryScope::Session;
        let denied = PolicyDecision::Deny {
            reason: "no session_id in the run's metadata".to_string(),
        };
        let without = memory_spec(&[]);
        let with = memory_spec(&[("session_id", "s-1")]);
        for effect in [read(Session), write(Session)] {
            assert_eq!(authorize(&without, &effect, &[]), denied, "{effect:?}");
            assert_eq!(
                authorize(&with, &effect, &[]),
                PolicyDecision::Allow,
                "{effect:?}"
            );
        }
    }

    #[test]
    fn workspace_memory_needs_a_workspace_id() {
        use crate::MemoryScope::Workspace;
        let denied = PolicyDecision::Deny {
            reason: "no workspace_id in the run's metadata".to_string(),
        };
        let without = memory_spec(&[("workspace_id", "")]);
        let with = memory_spec(&[("workspace_id", "w-1")]);
        for effect in [read(Workspace), write(Workspace)] {
            assert_eq!(authorize(&without, &effect, &[]), denied, "{effect:?}");
            assert_eq!(
                authorize(&with, &effect, &[]),
                PolicyDecision::Allow,
                "{effect:?}"
            );
        }
    }

    // Global memory would be shared by every tenant, so it is denied even
    // with both capabilities.
    #[test]
    fn global_memory_is_denied() {
        let spec = memory_spec(&[]);
        let denied = PolicyDecision::Deny {
            reason: "global memory is shared by every tenant and is not offered".to_string(),
        };
        for effect in [
            read(crate::MemoryScope::Global),
            write(crate::MemoryScope::Global),
        ] {
            assert_eq!(authorize(&spec, &effect, &[]), denied, "{effect:?}");
        }
    }

    // The capability check still comes first.
    #[test]
    fn a_memory_effect_without_its_capability_is_denied_first() {
        let mut spec = memory_spec(&[("session_id", "s-1")]);
        spec.capabilities.clear();
        assert_eq!(
            authorize(&spec, &read(crate::MemoryScope::Session), &[]),
            PolicyDecision::Deny {
                reason: "missing capability: memory.read".to_string()
            }
        );
    }
}
