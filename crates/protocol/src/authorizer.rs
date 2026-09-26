use crate::policy::PolicyDecision;
use crate::{Capability, Effect, RunSpec, ToolDescriptor};

const MODEL_CALL: &str = "model.call";
const MEMORY_READ: &str = "memory.read";
const MEMORY_WRITE: &str = "memory.write";

pub fn authorize(spec: &RunSpec, effect: &Effect, tools: &[ToolDescriptor]) -> PolicyDecision {
    match effect {
        // Always allowed. The budget rule depends on it: a Complete decided
        // while the harness is Running is not a step because it always ends
        // the run (fold, Driver::decide_with_skills). A Complete that could be
        // denied would be free and never end. Pinned by
        // complete_is_allowed_without_capabilities.
        Effect::Complete { .. } => PolicyDecision::Allow,
        Effect::ModelCall { .. } => allow_capability(spec, MODEL_CALL),
        Effect::MemoryRead { .. } => allow_capability(spec, MEMORY_READ),
        Effect::MemoryWrite { .. } => allow_capability(spec, MEMORY_WRITE),
        Effect::ToolCall { name, .. } => match tools.iter().find(|tool| tool.name == *name) {
            Some(tool) => allow_capability(spec, tool.required_capability.as_str()),
            None => PolicyDecision::Deny {
                reason: format!("unknown tool: {name}"),
            },
        },
        Effect::Execute { .. }
        | Effect::Delegate { .. }
        | Effect::AskUser { .. }
        | Effect::RequestApproval { .. }
        | Effect::Wait { .. }
        | Effect::PublishArtifact { .. } => PolicyDecision::Deny {
            reason: "effect is not implemented in this slice".to_string(),
        },
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

    #[test]
    fn complete_is_allowed_without_capabilities() {
        let spec = sample_spec();
        let effect = Effect::Complete {
            outcome: "done".to_string(),
        };
        assert_eq!(authorize(&spec, &effect, &[]), PolicyDecision::Allow);
    }
}
