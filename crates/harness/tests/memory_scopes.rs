//! Memory is keyed by whose it is (owner decision 10 for C3): the shared
//! scenarios against the in-memory store. `crates/memory/tests/recall.rs`
//! runs the same scenarios against Postgres.
use harness::{memory_scenarios, InMemory};

#[test]
fn run_memory_isolated_between_runs() {
    memory_scenarios::run_memory_isolated_between_runs(&InMemory::default());
}

#[test]
fn step_memory_isolated_between_steps() {
    memory_scenarios::step_memory_isolated_between_steps(&InMemory::default());
}

#[test]
fn agent_memory_survives_across_runs() {
    memory_scenarios::agent_memory_survives_across_runs(&InMemory::default());
}

#[test]
fn user_memory_isolated_between_tenants() {
    memory_scenarios::user_memory_isolated_between_tenants(&InMemory::default());
}

#[test]
fn session_memory_belongs_to_its_user() {
    memory_scenarios::session_memory_belongs_to_its_user(&InMemory::default());
}

// The Rust test of `NoCrossScopeRead` in formal/memory/Memory.tla.
#[test]
fn no_cross_scope_read() {
    memory_scenarios::no_cross_scope_read(&InMemory::default());
}

// A session effect performed for a run whose metadata names no session (the
// authorizer denies those, so only a caller of `perform` that skipped it gets
// here) fails the run, and reaches no memory.
#[test]
fn a_session_effect_without_a_session_id_fails_the_run() {
    use harness::{Driver, UnavailableModel};
    use protocol::{
        AgentId, CredentialSource, Effect, EventPayload, ExecutionPlacement, FailureClass,
        MemoryScope, ModelProvider, Owner, RunSpec, WorkModel,
    };
    let spec = RunSpec::builder()
        .owner(Owner::new("https://issuer.test", "alice", "tenant-1"))
        .agent(AgentId::new(), "1")
        .input("remember")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .build();
    let mut driver = Driver::boot(spec).expect("boot");
    let memory = InMemory::default();
    driver.perform(
        &[Effect::MemoryWrite {
            scope: MemoryScope::Session,
            key: "topic".to_string(),
            value: "talk".to_string(),
        }],
        &[],
        &UnavailableModel,
        &memory,
    );
    assert!(matches!(
        driver.events().last().map(|event| &event.payload),
        Some(EventPayload::RunFailed {
            class: FailureClass::Policy,
            ..
        })
    ));
    assert!(!driver
        .events()
        .iter()
        .any(|event| matches!(event.payload, EventPayload::MemoryWritten { .. })));
}
