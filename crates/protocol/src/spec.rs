use std::collections::BTreeMap;
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::{AgentId, RunId};

#[derive(Clone, Copy, Debug, Default)]
pub struct Missing;

#[derive(Clone, Copy, Debug, Default)]
pub struct Set;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Capability(String);

impl Capability {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExecutionPlacement {
    Local,
    Reverse,
    Box,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CredentialSource {
    BringYourOwn { secret_ref: String },
    PlatformGateway,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ModelProvider {
    OpenAI,
    Anthropic,
    Gemini,
    SystemOne,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkModel {
    pub provider: ModelProvider,
    pub model_name: String,
    pub credential: CredentialSource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    pub max_steps: u32,
    pub max_model_calls: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSpec {
    pub run_id: RunId,
    pub agent_id: AgentId,
    pub agent_version: String,
    pub input: String,
    pub placement: ExecutionPlacement,
    pub work_model: WorkModel,
    pub capabilities: Vec<Capability>,
    pub limits: Limits,
    pub metadata: BTreeMap<String, String>,
}

struct SpecDraft {
    agent_id: Option<AgentId>,
    agent_version: Option<String>,
    input: Option<String>,
    placement: Option<ExecutionPlacement>,
    work_model: Option<WorkModel>,
    capabilities: Vec<Capability>,
    limits: Limits,
    metadata: BTreeMap<String, String>,
}

pub struct RunSpecBuilder<A, I, P, W> {
    draft: SpecDraft,
    _state: PhantomData<(A, I, P, W)>,
}

impl RunSpec {
    pub fn builder() -> RunSpecBuilder<Missing, Missing, Missing, Missing> {
        RunSpecBuilder::new()
    }
}

impl Default for RunSpecBuilder<Missing, Missing, Missing, Missing> {
    fn default() -> Self {
        Self::new()
    }
}

impl RunSpecBuilder<Missing, Missing, Missing, Missing> {
    pub fn new() -> Self {
        Self {
            draft: SpecDraft {
                agent_id: None,
                agent_version: None,
                input: None,
                placement: None,
                work_model: None,
                capabilities: Vec::new(),
                limits: Limits {
                    max_steps: 8,
                    max_model_calls: 4,
                },
                metadata: BTreeMap::new(),
            },
            _state: PhantomData,
        }
    }
}

impl<A, I, P, W> RunSpecBuilder<A, I, P, W> {
    fn retag<A2, I2, P2, W2>(self) -> RunSpecBuilder<A2, I2, P2, W2> {
        RunSpecBuilder {
            draft: self.draft,
            _state: PhantomData,
        }
    }
}

impl<I, P, W> RunSpecBuilder<Missing, I, P, W> {
    pub fn agent(
        mut self,
        agent_id: AgentId,
        agent_version: impl Into<String>,
    ) -> RunSpecBuilder<Set, I, P, W> {
        self.draft.agent_id = Some(agent_id);
        self.draft.agent_version = Some(agent_version.into());
        self.retag()
    }
}

impl<A, P, W> RunSpecBuilder<A, Missing, P, W> {
    pub fn input(mut self, input: impl Into<String>) -> RunSpecBuilder<A, Set, P, W> {
        self.draft.input = Some(input.into());
        self.retag()
    }
}

impl<A, I, W> RunSpecBuilder<A, I, Missing, W> {
    pub fn placement(mut self, placement: ExecutionPlacement) -> RunSpecBuilder<A, I, Set, W> {
        self.draft.placement = Some(placement);
        self.retag()
    }
}

impl<A, I, P> RunSpecBuilder<A, I, P, Missing> {
    pub fn work_model(mut self, work_model: WorkModel) -> RunSpecBuilder<A, I, P, Set> {
        self.draft.work_model = Some(work_model);
        self.retag()
    }
}

impl RunSpecBuilder<Set, Set, Set, Set> {
    pub fn capabilities(mut self, capabilities: Vec<Capability>) -> Self {
        self.draft.capabilities = capabilities;
        self
    }

    pub fn limits(mut self, limits: Limits) -> Self {
        self.draft.limits = limits;
        self
    }

    pub fn metadata(mut self, metadata: BTreeMap<String, String>) -> Self {
        self.draft.metadata = metadata;
        self
    }

    pub fn build(self) -> RunSpec {
        let draft = self.draft;
        RunSpec {
            run_id: RunId::new(),
            agent_id: draft.agent_id.expect("typestate recorded the agent"),
            agent_version: draft.agent_version.expect("typestate recorded the agent"),
            input: draft.input.expect("typestate recorded the input"),
            placement: draft.placement.expect("typestate recorded the placement"),
            work_model: draft.work_model.expect("typestate recorded the work model"),
            capabilities: draft.capabilities,
            limits: draft.limits,
            metadata: draft.metadata,
        }
    }
}

#[cfg(test)]
pub fn sample_spec() -> RunSpec {
    RunSpec::builder()
        .agent(AgentId::new(), "1")
        .input("hello")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_keeps_the_required_fields() {
        let spec = RunSpec::builder()
            .agent(AgentId::new(), "3")
            .input("ship")
            .placement(ExecutionPlacement::Box)
            .work_model(WorkModel {
                provider: ModelProvider::SystemOne,
                model_name: "jev-latest".to_string(),
                credential: CredentialSource::BringYourOwn {
                    secret_ref: "jev".to_string(),
                },
            })
            .capabilities(vec![Capability::new("tool.echo")])
            .limits(Limits {
                max_steps: 2,
                max_model_calls: 1,
            })
            .build();
        assert_eq!(spec.agent_version, "3");
        assert_eq!(spec.input, "ship");
        assert_eq!(spec.placement, ExecutionPlacement::Box);
        assert_eq!(spec.capabilities, vec![Capability::new("tool.echo")]);
        assert_eq!(spec.limits.max_steps, 2);
        assert_eq!(
            spec.work_model.credential,
            CredentialSource::BringYourOwn {
                secret_ref: "jev".to_string()
            }
        );
    }
}
