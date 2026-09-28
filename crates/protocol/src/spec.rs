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

/// The principal a run belongs to: the token issuer and subject that created
/// it, and the tenant it was created in. Only the owner may read or finish
/// the run.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Owner {
    pub issuer: String,
    pub subject: String,
    pub tenant: String,
}

impl Owner {
    pub fn new(
        issuer: impl Into<String>,
        subject: impl Into<String>,
        tenant: impl Into<String>,
    ) -> Self {
        Self {
            issuer: issuer.into(),
            subject: subject.into(),
            tenant: tenant.into(),
        }
    }

    /// Whether `other` is the same principal: the same issuer and subject.
    /// The tenant is recorded for scoping but does not change who the caller
    /// is (owner decision for B2).
    pub fn is(&self, other: &Owner) -> bool {
        self.issuer == other.issuer && self.subject == other.subject
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    pub max_steps: u32,
    pub max_model_calls: u32,
}

/// Where a run came from. A top-level run has no parent, is its own root
/// and is at hop 0; a run started by another run names its parent and the
/// root of the chain, one hop further on.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lineage {
    pub parent: Option<RunId>,
    pub root: Option<RunId>,
    pub hop: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSpec {
    pub run_id: RunId,
    pub owner: Owner,
    pub agent_id: AgentId,
    pub agent_version: String,
    pub input: String,
    pub placement: ExecutionPlacement,
    pub work_model: WorkModel,
    pub capabilities: Vec<Capability>,
    pub limits: Limits,
    pub metadata: BTreeMap<String, String>,
    /// Absent from specs stored before lineage existed: those are top level.
    #[serde(default)]
    pub lineage: Lineage,
}

impl RunSpec {
    /// The first run of this run's chain: its own id for a top-level run.
    pub fn root(&self) -> RunId {
        self.lineage.root.unwrap_or(self.run_id)
    }
}

/// The request a child run is started for: its parent, and the parent's
/// step that asked for it.
struct ChildOf {
    parent: RunId,
    root: RunId,
    hop: u32,
    step: u32,
}

/// The namespace of child run ids (UUID v5 over the request).
const CHILD_RUN_NAMESPACE: uuid::Uuid =
    uuid::Uuid::from_u128(0x6f8b_1c2e_4a7d_4e5b_9c3f_2d1a_0b8e_7c6d);

struct SpecDraft {
    owner: Option<Owner>,
    agent_id: Option<AgentId>,
    agent_version: Option<String>,
    input: Option<String>,
    placement: Option<ExecutionPlacement>,
    work_model: Option<WorkModel>,
    capabilities: Vec<Capability>,
    limits: Limits,
    metadata: BTreeMap<String, String>,
    child_of: Option<ChildOf>,
}

pub struct RunSpecBuilder<A, I, P, W, O> {
    draft: SpecDraft,
    _state: PhantomData<(A, I, P, W, O)>,
}

impl RunSpec {
    pub fn builder() -> RunSpecBuilder<Missing, Missing, Missing, Missing, Missing> {
        RunSpecBuilder::new()
    }
}

impl Default for RunSpecBuilder<Missing, Missing, Missing, Missing, Missing> {
    fn default() -> Self {
        Self::new()
    }
}

impl RunSpecBuilder<Missing, Missing, Missing, Missing, Missing> {
    pub fn new() -> Self {
        Self {
            draft: SpecDraft {
                owner: None,
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
                child_of: None,
            },
            _state: PhantomData,
        }
    }
}

impl<A, I, P, W, O> RunSpecBuilder<A, I, P, W, O> {
    fn retag<A2, I2, P2, W2, O2>(self) -> RunSpecBuilder<A2, I2, P2, W2, O2> {
        RunSpecBuilder {
            draft: self.draft,
            _state: PhantomData,
        }
    }
}

impl<A, I, P, W> RunSpecBuilder<A, I, P, W, Missing> {
    /// The principal the run belongs to.
    pub fn owner(mut self, owner: Owner) -> RunSpecBuilder<A, I, P, W, Set> {
        self.draft.owner = Some(owner);
        self.retag()
    }
}

impl<I, P, W, O> RunSpecBuilder<Missing, I, P, W, O> {
    pub fn agent(
        mut self,
        agent_id: AgentId,
        agent_version: impl Into<String>,
    ) -> RunSpecBuilder<Set, I, P, W, O> {
        self.draft.agent_id = Some(agent_id);
        self.draft.agent_version = Some(agent_version.into());
        self.retag()
    }
}

impl<A, P, W, O> RunSpecBuilder<A, Missing, P, W, O> {
    pub fn input(mut self, input: impl Into<String>) -> RunSpecBuilder<A, Set, P, W, O> {
        self.draft.input = Some(input.into());
        self.retag()
    }
}

impl<A, I, W, O> RunSpecBuilder<A, I, Missing, W, O> {
    pub fn placement(mut self, placement: ExecutionPlacement) -> RunSpecBuilder<A, I, Set, W, O> {
        self.draft.placement = Some(placement);
        self.retag()
    }
}

impl<A, I, P, O> RunSpecBuilder<A, I, P, Missing, O> {
    pub fn work_model(mut self, work_model: WorkModel) -> RunSpecBuilder<A, I, P, Set, O> {
        self.draft.work_model = Some(work_model);
        self.retag()
    }
}

impl RunSpecBuilder<Set, Set, Set, Set, Set> {
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

    /// Makes this a child of `parent`, started by the parent's `step`. The
    /// child's id is derived from its root, parent, step, agent and input
    /// alone: the same five name the same run (a parent that is run again
    /// does not start a second child), and a change in any of them names
    /// another. Nothing else in the spec takes part, so the caller must set
    /// the rest from the parent and the target agent, the same way every
    /// time. Two identical delegations in one step name one child; a fan-out
    /// gives each child its own input.
    pub fn child_of(mut self, parent: &RunSpec, step: u32) -> Self {
        self.draft.child_of = Some(ChildOf {
            parent: parent.run_id,
            root: parent.root(),
            hop: parent.lineage.hop.saturating_add(1),
            step,
        });
        self
    }

    pub fn build(self) -> RunSpec {
        let draft = self.draft;
        let agent_id = draft.agent_id.expect("typestate recorded the agent");
        let input = draft.input.expect("typestate recorded the input");
        let (run_id, lineage) = match &draft.child_of {
            None => (RunId::new(), Lineage::default()),
            Some(child) => {
                let mut name = Vec::with_capacity(52 + input.len());
                name.extend_from_slice(child.root.as_uuid().as_bytes());
                name.extend_from_slice(child.parent.as_uuid().as_bytes());
                name.extend_from_slice(&child.step.to_le_bytes());
                name.extend_from_slice(agent_id.as_uuid().as_bytes());
                name.extend_from_slice(input.as_bytes());
                (
                    RunId::from_uuid(uuid::Uuid::new_v5(&CHILD_RUN_NAMESPACE, &name)),
                    Lineage {
                        parent: Some(child.parent),
                        root: Some(child.root),
                        hop: child.hop,
                    },
                )
            }
        };
        RunSpec {
            run_id,
            owner: draft.owner.expect("typestate recorded the owner"),
            agent_id,
            agent_version: draft.agent_version.expect("typestate recorded the agent"),
            input,
            placement: draft.placement.expect("typestate recorded the placement"),
            work_model: draft.work_model.expect("typestate recorded the work model"),
            capabilities: draft.capabilities,
            limits: draft.limits,
            metadata: draft.metadata,
            lineage,
        }
    }
}

#[cfg(test)]
pub fn sample_spec() -> RunSpec {
    RunSpec::builder()
        .owner(Owner::new("https://issuer.test", "user-1", "tenant-1"))
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
            .owner(Owner::new("https://issuer.test", "user-1", "tenant-1"))
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
        assert_eq!(
            spec.owner,
            Owner::new("https://issuer.test", "user-1", "tenant-1")
        );
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

#[cfg(test)]
mod owner_tests {
    use super::Owner;

    // The same principal is the same issuer and subject; the tenant does not
    // change who the caller is.
    #[test]
    fn an_owner_is_its_issuer_and_subject() {
        let owner = Owner::new("iss", "sub", "tenant-a");
        assert!(owner.is(&Owner::new("iss", "sub", "tenant-a")));
        assert!(owner.is(&Owner::new("iss", "sub", "tenant-b")));
        assert!(!owner.is(&Owner::new("iss", "other", "tenant-a")));
        assert!(!owner.is(&Owner::new("other", "sub", "tenant-a")));
    }

    // The owner is part of the stored spec, so it survives a round trip.
    #[test]
    fn the_owner_round_trips_through_json() {
        let spec = super::sample_spec();
        let json = serde_json::to_value(&spec).unwrap();
        assert_eq!(json["owner"]["subject"], "user-1");
        let back: super::RunSpec = serde_json::from_value(json).unwrap();
        assert_eq!(back.owner, spec.owner);
    }
}

#[cfg(test)]
mod lineage_tests {
    use super::*;

    fn child(parent: &RunSpec, step: u32, agent: AgentId, input: &str) -> RunSpec {
        RunSpec::builder()
            .owner(parent.owner.clone())
            .agent(agent, "1")
            .input(input)
            .placement(parent.placement)
            .work_model(parent.work_model.clone())
            .child_of(parent, step)
            .build()
    }

    // A top-level run is its own root at hop 0; a child names its parent and
    // its root and is one hop further.
    #[test]
    fn a_child_is_traced_to_its_parent_and_root() {
        let root = sample_spec();
        assert_eq!(root.lineage, Lineage::default());
        assert_eq!(root.root(), root.run_id);
        let agent = AgentId::new();
        let first = child(&root, 1, agent, "a");
        assert_eq!(
            first.lineage,
            Lineage {
                parent: Some(root.run_id),
                root: Some(root.run_id),
                hop: 1,
            }
        );
        let second = child(&first, 2, agent, "b");
        assert_eq!(
            second.lineage,
            Lineage {
                parent: Some(first.run_id),
                root: Some(root.run_id),
                hop: 2,
            }
        );
        assert_eq!(second.root(), root.run_id);
    }

    // A child's id comes from what was asked: the same request gives the same
    // id, so a redelivered parent starts no second child, and a different
    // step, agent, input or parent gives another.
    #[test]
    fn a_child_id_is_derived_from_its_request() {
        let root = sample_spec();
        let agent = AgentId::new();
        let id = child(&root, 3, agent, "draft").run_id;
        assert_eq!(child(&root, 3, agent, "draft").run_id, id);
        assert_ne!(child(&root, 4, agent, "draft").run_id, id);
        assert_ne!(child(&root, 3, AgentId::new(), "draft").run_id, id);
        assert_ne!(child(&root, 3, agent, "draft!").run_id, id);
        assert_ne!(child(&sample_spec(), 3, agent, "draft").run_id, id);
        assert_ne!(sample_spec().run_id, sample_spec().run_id);
    }

    // The id's layout is fixed: once child ids are stored, a change to the
    // name bytes or the namespace would rename every child. A second-level
    // child, whose root and parent differ, shows the root counts on its own.
    #[test]
    fn a_child_id_is_fixed_by_its_layout() {
        let id = |n: u128| RunId::from_uuid(uuid::Uuid::from_u128(n));
        let mut parent = sample_spec();
        parent.run_id = id(1);
        parent.lineage = Lineage {
            parent: Some(id(2)),
            root: Some(id(3)),
            hop: 1,
        };
        let agent = AgentId::from_uuid(uuid::Uuid::from_u128(4));
        let named = child(&parent, 5, agent, "draft").run_id;
        assert_eq!(named.to_string(), "4f6b81d8-6ace-5267-afe7-182617feebc3");
        let mut other_root = parent.clone();
        other_root.lineage.root = Some(id(9));
        assert_ne!(child(&other_root, 5, agent, "draft").run_id, named);
    }

    // A corrupt hop at the top of its range stays there: it does not wrap
    // to a child that looks like a root.
    #[test]
    fn a_hop_at_its_limit_does_not_wrap() {
        let mut parent = sample_spec();
        parent.lineage.hop = u32::MAX;
        assert_eq!(child(&parent, 1, AgentId::new(), "x").lineage.hop, u32::MAX);
    }

    // A spec stored before lineage existed still loads, as a top-level run.
    #[test]
    fn a_spec_stored_without_lineage_loads_as_top_level() {
        let spec = sample_spec();
        let mut json = serde_json::to_value(&spec).unwrap();
        json.as_object_mut().unwrap().remove("lineage");
        let back: RunSpec = serde_json::from_value(json).unwrap();
        assert_eq!(back.lineage, Lineage::default());
        assert_eq!(back.root(), back.run_id);
    }
}
