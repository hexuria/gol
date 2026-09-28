//! Delegation in the driver. An authorized `Delegate` is performed by the
//! driver like a memory access: it records what happened and the run goes on.
//! Until a spawner is wired in, what happens is a refusal. Every event of a
//! child run names its parent.

use harness::{run_to_completion, Driver, InMemory, ScriptedDecider, UnavailableModel};
use protocol::{
    AgentId, Capability, CredentialSource, Effect, EventPayload, ExecutionPlacement, HarnessState,
    ModelProvider, Owner, RunSpec, WorkModel,
};

fn spec(capabilities: &[&str]) -> RunSpec {
    RunSpec::builder()
        .owner(Owner::new("https://issuer.test", "user-1", "tenant-1"))
        .agent(AgentId::new(), "1")
        .input("hi")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::Anthropic,
            model_name: "m".into(),
            credential: CredentialSource::PlatformGateway,
        })
        .capabilities(
            capabilities
                .iter()
                .map(|name| Capability::new(*name))
                .collect(),
        )
        .build()
}

fn run(spec: RunSpec, effects: Vec<Effect>) -> Driver {
    let mut driver = Driver::boot(spec).unwrap();
    let mut decider = ScriptedDecider::new(effects);
    run_to_completion(
        &mut driver,
        &mut decider,
        &[],
        &UnavailableModel,
        &InMemory::default(),
    )
    .unwrap();
    driver
}

fn delegate(agent_id: AgentId) -> Effect {
    Effect::Delegate {
        agent_id,
        input: "draft".to_string(),
    }
}

fn complete() -> Effect {
    Effect::Complete {
        outcome: "done".to_string(),
    }
}

#[test]
fn a_delegate_with_no_spawner_is_refused_and_the_run_goes_on() {
    let agent = AgentId::new();
    let driver = run(spec(&["agent.delegate"]), vec![delegate(agent), complete()]);
    let payloads: Vec<&EventPayload> = driver.events().iter().map(|e| &e.payload).collect();
    assert!(
        payloads.contains(&&EventPayload::EffectAuthorized {
            effect: delegate(agent)
        }),
        "{payloads:?}"
    );
    assert!(
        payloads.contains(&&EventPayload::DelegateRefused {
            agent_id: agent,
            reason: "no agent spawner is configured".to_string(),
        }),
        "{payloads:?}"
    );
    assert_eq!(
        driver.state().harness,
        HarnessState::Completed {
            outcome: "done".to_string()
        }
    );
    assert_eq!(driver.state().children, 0);
}

#[test]
fn a_delegate_without_its_capability_is_denied() {
    let agent = AgentId::new();
    let driver = run(spec(&[]), vec![delegate(agent), complete()]);
    assert!(driver.events().iter().any(|event| event.payload
        == EventPayload::EffectDenied {
            effect: delegate(agent),
            reason: "missing capability: agent.delegate".to_string(),
        }));
    assert!(!driver
        .events()
        .iter()
        .any(|event| matches!(event.payload, EventPayload::DelegateRefused { .. })));
}

#[test]
fn every_event_of_a_child_run_names_its_parent() {
    let parent = spec(&[]);
    let child = RunSpec::builder()
        .owner(parent.owner.clone())
        .agent(AgentId::new(), "1")
        .input("draft")
        .placement(parent.placement)
        .work_model(parent.work_model.clone())
        .child_of(&parent, 1)
        .build();
    let driver = run(child, vec![complete()]);
    assert!(!driver.events().is_empty());
    assert!(driver
        .events()
        .iter()
        .all(|event| event.envelope.parent_run_id == Some(parent.run_id)));
    let top = run(parent, vec![complete()]);
    assert!(top
        .events()
        .iter()
        .all(|event| event.envelope.parent_run_id.is_none()));
}
