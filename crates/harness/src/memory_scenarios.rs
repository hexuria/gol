//! Memory scoping scenarios, run through the driver against any `Memory`:
//! the shared contract that `InMemory` and `memory::PostgresMemory` both
//! pass (owner decision 10 for C3). Each scenario panics when a scope leaks
//! or loses what it should keep.
use std::collections::BTreeMap;

use protocol::{
    AgentId, Capability, CredentialSource, Effect, EventPayload, ExecutionPlacement, MemoryScope,
    ModelProvider, Owner, RunSpec, WorkModel, SESSION_ID,
};

use crate::{run_to_completion, Driver, Memory, ScriptedDecider, UnavailableModel};

const ISSUER: &str = "https://issuer.test";

fn spec(subject: &str, tenant: &str, agent: AgentId, session: Option<&str>) -> RunSpec {
    let mut metadata = BTreeMap::new();
    if let Some(session) = session {
        metadata.insert(SESSION_ID.to_string(), session.to_string());
    }
    RunSpec::builder()
        .owner(Owner::new(ISSUER, subject, tenant))
        .agent(agent, "1")
        .input("remember")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .capabilities(vec![
            Capability::new("memory.read"),
            Capability::new("memory.write"),
        ])
        .metadata(metadata)
        .build()
}

fn write(scope: MemoryScope, value: &str) -> Effect {
    Effect::MemoryWrite {
        scope,
        key: "topic".to_string(),
        value: value.to_string(),
    }
}

fn read(scope: MemoryScope) -> Effect {
    Effect::MemoryRead {
        scope,
        key: "topic".to_string(),
    }
}

/// Runs `effects`, then completes, and returns what each memory read found.
fn reads(spec: RunSpec, effects: Vec<Effect>, memory: &dyn Memory) -> Vec<Option<String>> {
    let mut driver = Driver::boot(spec).expect("boot");
    let mut script = effects;
    script.push(Effect::Complete {
        outcome: "done".to_string(),
    });
    let mut decider = ScriptedDecider::new(script);
    run_to_completion(&mut driver, &mut decider, &[], &UnavailableModel, memory).expect("run");
    let found: Vec<_> = driver
        .events()
        .iter()
        .filter_map(|event| match &event.payload {
            EventPayload::MemoryRead { value, .. } => Some(value.clone()),
            _ => None,
        })
        .collect();
    let failed = driver.events().iter().any(|event| {
        matches!(
            event.payload,
            EventPayload::RunFailed { .. } | EventPayload::EffectDenied { .. }
        )
    });
    assert!(
        !failed,
        "the run failed or was denied: {:?}",
        driver.events()
    );
    found
}

fn value(text: &str) -> Option<String> {
    Some(text.to_string())
}

/// Run memory belongs to one run: another run of the same agent and user
/// does not see it.
pub fn run_memory_isolated_between_runs(memory: &dyn Memory) {
    let agent = AgentId::new();
    let first = spec("alice", "tenant-1", agent, None);
    assert_eq!(
        reads(
            first,
            vec![write(MemoryScope::Run, "a"), read(MemoryScope::Run)],
            memory
        ),
        [value("a")]
    );
    let second = spec("alice", "tenant-1", agent, None);
    assert_eq!(reads(second, vec![read(MemoryScope::Run)], memory), [None]);
}

/// Step memory belongs to one step of one run.
pub fn step_memory_isolated_between_steps(memory: &dyn Memory) {
    let run = spec("alice", "tenant-1", AgentId::new(), None);
    assert_eq!(
        reads(
            run,
            vec![write(MemoryScope::Step, "s"), read(MemoryScope::Step)],
            memory
        ),
        [None]
    );
}

/// Agent memory outlives a run: the next run of the same agent reads it,
/// and another agent's run does not.
pub fn agent_memory_survives_across_runs(memory: &dyn Memory) {
    let agent = AgentId::new();
    let first = spec("alice", "tenant-1", agent, None);
    reads(first, vec![write(MemoryScope::Agent, "kept")], memory);
    let again = spec("alice", "tenant-1", agent, None);
    assert_eq!(
        reads(again, vec![read(MemoryScope::Agent)], memory),
        [value("kept")]
    );
    let other = spec("alice", "tenant-1", AgentId::new(), None);
    assert_eq!(reads(other, vec![read(MemoryScope::Agent)], memory), [None]);
}

/// User and organization memory: a user in another tenant sees neither
/// what alice wrote for herself nor what she wrote for her organization; a
/// colleague in her tenant sees the organization's; alice sees her own from
/// another tenant too, since a user is the issuer and subject.
pub fn user_memory_isolated_between_tenants(memory: &dyn Memory) {
    let subject = format!("alice-{}", AgentId::new());
    let tenant = format!("tenant-{}", AgentId::new());
    let alice = spec(&subject, &tenant, AgentId::new(), None);
    reads(
        alice,
        vec![
            write(MemoryScope::User, "mine"),
            write(MemoryScope::Organization, "ours"),
        ],
        memory,
    );
    let stranger = spec("bob", "tenant-elsewhere", AgentId::new(), None);
    assert_eq!(
        reads(
            stranger,
            vec![read(MemoryScope::User), read(MemoryScope::Organization)],
            memory
        ),
        [None, None]
    );
    let colleague = spec("carol", &tenant, AgentId::new(), None);
    assert_eq!(
        reads(
            colleague,
            vec![read(MemoryScope::User), read(MemoryScope::Organization)],
            memory
        ),
        [None, value("ours")]
    );
    let alice_elsewhere = spec(&subject, "tenant-elsewhere", AgentId::new(), None);
    assert_eq!(
        reads(alice_elsewhere, vec![read(MemoryScope::User)], memory),
        [value("mine")]
    );
}

/// A session is its user's: alice's runs share it, and bob naming the same
/// session id does not reach it.
pub fn session_memory_belongs_to_its_user(memory: &dyn Memory) {
    let session = format!("session-{}", AgentId::new());
    let first = spec("alice", "tenant-1", AgentId::new(), Some(&session));
    reads(first, vec![write(MemoryScope::Session, "talk")], memory);
    let again = spec("alice", "tenant-1", AgentId::new(), Some(&session));
    assert_eq!(
        reads(again, vec![read(MemoryScope::Session)], memory),
        [value("talk")]
    );
    let bob = spec("bob", "tenant-1", AgentId::new(), Some(&session));
    assert_eq!(reads(bob, vec![read(MemoryScope::Session)], memory), [None]);
}

/// `NoCrossScopeRead` in `formal/memory/Memory.tla`: runs of two tenants
/// write and read their organization's memory at the same time, and each
/// reads back only its own tenant's value.
pub fn no_cross_scope_read(memory: &dyn Memory) {
    let tenants: Vec<String> = (0..2)
        .map(|_| format!("tenant-{}", AgentId::new()))
        .collect();
    std::thread::scope(|scope| {
        for round in 0..8 {
            for tenant in &tenants {
                scope.spawn(move || {
                    let mine = format!("{tenant}/{round}");
                    let run = spec("alice", tenant, AgentId::new(), None);
                    let found = reads(
                        run,
                        vec![
                            write(MemoryScope::Organization, &mine),
                            read(MemoryScope::Organization),
                        ],
                        memory,
                    );
                    let read_tenant = found[0]
                        .as_deref()
                        .and_then(|value| value.split_once('/'))
                        .map(|(tenant, _)| tenant.to_string());
                    assert_eq!(read_tenant.as_deref(), Some(tenant.as_str()));
                });
            }
        }
    });
}

/// Every scenario, in order.
pub fn all(memory: &dyn Memory) {
    run_memory_isolated_between_runs(memory);
    step_memory_isolated_between_steps(memory);
    agent_memory_survives_across_runs(memory);
    user_memory_isolated_between_tenants(memory);
    session_memory_belongs_to_its_user(memory);
    no_cross_scope_read(memory);
}
