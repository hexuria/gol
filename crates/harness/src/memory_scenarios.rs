//! Memory scoping scenarios, run through the driver against any `Memory`:
//! the shared contract that `InMemory` and `memory::PostgresMemory` both
//! pass (owner decision 10 for C3). Each scenario panics when a scope leaks
//! or loses what it should keep. Every principal and tenant is new, so a
//! persistent store's earlier rows cannot answer for them.
use std::collections::BTreeMap;

use protocol::{
    AgentId, Capability, CredentialSource, Effect, Event, EventPayload, ExecutionPlacement,
    InvocationId, MemoryScope, ModelProvider, Owner, RunSpec, WorkModel, SESSION_ID, WORKSPACE_ID,
};

use crate::{run_to_completion, Driver, EchoTool, Memory, ScriptedDecider, UnavailableModel};

const ISSUER: &str = "https://issuer.test";

/// A name no earlier run used.
fn fresh(what: &str) -> String {
    format!("{what}-{}", AgentId::new())
}

struct Who<'a> {
    subject: &'a str,
    tenant: &'a str,
    agent: AgentId,
    metadata: &'a [(&'a str, &'a str)],
}

fn spec(who: &Who<'_>) -> RunSpec {
    let metadata: BTreeMap<String, String> = who
        .metadata
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    RunSpec::builder()
        .owner(Owner::new(ISSUER, who.subject, who.tenant))
        .agent(who.agent, "1")
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
            Capability::new("tool.echo"),
        ])
        .metadata(metadata)
        .build()
}

fn who<'a>(subject: &'a str, tenant: &'a str) -> Who<'a> {
    Who {
        subject,
        tenant,
        agent: AgentId::new(),
        metadata: &[],
    }
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

/// An echo call: its result answers the step, so the next effect is in the
/// next harness step.
fn echo() -> Effect {
    Effect::ToolCall {
        name: "echo".to_string(),
        input: "next".to_string(),
        invocation: InvocationId::new(),
    }
}

/// Runs `effects`, then completes, and returns the run's events.
fn run(spec: RunSpec, effects: Vec<Effect>, memory: &dyn Memory) -> Vec<Event> {
    let mut driver = Driver::boot(spec).expect("boot");
    let mut script = effects;
    script.push(Effect::Complete {
        outcome: "done".to_string(),
    });
    let mut decider = ScriptedDecider::new(script);
    run_to_completion(
        &mut driver,
        &mut decider,
        &[&EchoTool],
        &UnavailableModel,
        memory,
    )
    .expect("run");
    driver.events().to_vec()
}

fn found(events: &[Event]) -> Vec<Option<String>> {
    events
        .iter()
        .filter_map(|event| match &event.payload {
            EventPayload::MemoryRead { value, .. } => Some(value.clone()),
            _ => None,
        })
        .collect()
}

/// What each memory read found, in a run that must not fail or be denied.
fn reads(spec: RunSpec, effects: Vec<Effect>, memory: &dyn Memory) -> Vec<Option<String>> {
    let events = run(spec, effects, memory);
    let failed = events.iter().any(|event| {
        matches!(
            event.payload,
            EventPayload::RunFailed { .. } | EventPayload::EffectDenied { .. }
        )
    });
    assert!(!failed, "the run failed or was denied: {events:?}");
    found(&events)
}

fn value(text: &str) -> Option<String> {
    Some(text.to_string())
}

/// Run memory belongs to one run: another run of the same agent and user
/// does not see it.
pub fn run_memory_isolated_between_runs(memory: &dyn Memory) {
    let (alice, tenant) = (fresh("alice"), fresh("tenant"));
    let agent = AgentId::new();
    let first = Who {
        agent,
        ..who(&alice, &tenant)
    };
    assert_eq!(
        reads(
            spec(&first),
            vec![write(MemoryScope::Run, "a"), read(MemoryScope::Run)],
            memory
        ),
        [value("a")]
    );
    let second = Who {
        agent,
        ..who(&alice, &tenant)
    };
    assert_eq!(
        reads(spec(&second), vec![read(MemoryScope::Run)], memory),
        [None]
    );
}

/// Step memory belongs to one harness step of one run: a read in the same
/// step finds it, and a read after the step advances does not.
pub fn step_memory_isolated_between_steps(memory: &dyn Memory) {
    let (alice, tenant) = (fresh("alice"), fresh("tenant"));
    assert_eq!(
        reads(
            spec(&who(&alice, &tenant)),
            vec![
                write(MemoryScope::Step, "s"),
                read(MemoryScope::Step),
                echo(),
                read(MemoryScope::Step),
            ],
            memory
        ),
        [value("s"), None]
    );
}

/// Agent memory outlives a run: the next run of the same agent by the same
/// principal reads it; another agent, or another principal naming the same
/// agent id, does not.
pub fn agent_memory_survives_across_runs(memory: &dyn Memory) {
    let (alice, mallory, tenant) = (fresh("alice"), fresh("mallory"), fresh("tenant"));
    let agent = AgentId::new();
    let first = Who {
        agent,
        ..who(&alice, &tenant)
    };
    reads(
        spec(&first),
        vec![write(MemoryScope::Agent, "kept")],
        memory,
    );
    let again = Who {
        agent,
        ..who(&alice, &tenant)
    };
    assert_eq!(
        reads(spec(&again), vec![read(MemoryScope::Agent)], memory),
        [value("kept")]
    );
    let other = who(&alice, &tenant);
    assert_eq!(
        reads(spec(&other), vec![read(MemoryScope::Agent)], memory),
        [None]
    );
    let borrowed = Who {
        agent,
        ..who(&mallory, &tenant)
    };
    assert_eq!(
        reads(spec(&borrowed), vec![read(MemoryScope::Agent)], memory),
        [None]
    );
}

/// User and organization memory: a user in another tenant sees neither
/// what alice wrote for herself nor what she wrote for her organization; a
/// colleague in her tenant sees the organization's; alice sees her own from
/// another tenant too, since a user is the issuer and subject.
pub fn user_memory_isolated_between_tenants(memory: &dyn Memory) {
    let (alice, bob, carol) = (fresh("alice"), fresh("bob"), fresh("carol"));
    let (tenant, elsewhere) = (fresh("tenant"), fresh("tenant"));
    reads(
        spec(&who(&alice, &tenant)),
        vec![
            write(MemoryScope::User, "mine"),
            write(MemoryScope::Organization, "ours"),
        ],
        memory,
    );
    let both = || vec![read(MemoryScope::User), read(MemoryScope::Organization)];
    assert_eq!(
        reads(spec(&who(&bob, &elsewhere)), both(), memory),
        [None, None]
    );
    assert_eq!(
        reads(spec(&who(&carol, &tenant)), both(), memory),
        [None, value("ours")]
    );
    assert_eq!(
        reads(
            spec(&who(&alice, &elsewhere)),
            vec![read(MemoryScope::User)],
            memory
        ),
        [value("mine")]
    );
}

/// A session is its user's, in its tenant: alice's runs there share it; bob
/// naming the same session id, or alice in another tenant, do not reach it.
pub fn session_memory_belongs_to_its_user(memory: &dyn Memory) {
    let (alice, bob) = (fresh("alice"), fresh("bob"));
    let (tenant, elsewhere) = (fresh("tenant"), fresh("tenant"));
    let session = fresh("session");
    let metadata = [(SESSION_ID, session.as_str())];
    let at = |subject, tenant| Who {
        metadata: &metadata,
        ..who(subject, tenant)
    };
    reads(
        spec(&at(&alice, &tenant)),
        vec![write(MemoryScope::Session, "talk")],
        memory,
    );
    let session_read = || vec![read(MemoryScope::Session)];
    assert_eq!(
        reads(spec(&at(&alice, &tenant)), session_read(), memory),
        [value("talk")]
    );
    assert_eq!(
        reads(spec(&at(&bob, &tenant)), session_read(), memory),
        [None]
    );
    assert_eq!(
        reads(spec(&at(&alice, &elsewhere)), session_read(), memory),
        [None]
    );
}

/// A workspace is its organization's: colleagues in the tenant share it,
/// and another tenant naming the same workspace id does not reach it.
pub fn workspace_memory_belongs_to_its_organization(memory: &dyn Memory) {
    let (alice, carol, dave) = (fresh("alice"), fresh("carol"), fresh("dave"));
    let (tenant, elsewhere) = (fresh("tenant"), fresh("tenant"));
    let workspace = fresh("workspace");
    let metadata = [(WORKSPACE_ID, workspace.as_str())];
    let at = |subject, tenant| Who {
        metadata: &metadata,
        ..who(subject, tenant)
    };
    reads(
        spec(&at(&alice, &tenant)),
        vec![write(MemoryScope::Workspace, "plan")],
        memory,
    );
    let workspace_read = || vec![read(MemoryScope::Workspace)];
    assert_eq!(
        reads(spec(&at(&carol, &tenant)), workspace_read(), memory),
        [value("plan")]
    );
    assert_eq!(
        reads(spec(&at(&dave, &elsewhere)), workspace_read(), memory),
        [None]
    );
}

/// Global memory is denied: the run records the denial and writes nothing.
pub fn global_memory_is_denied(memory: &dyn Memory) {
    let (alice, tenant) = (fresh("alice"), fresh("tenant"));
    let events = run(
        spec(&who(&alice, &tenant)),
        vec![write(MemoryScope::Global, "all")],
        memory,
    );
    assert!(events
        .iter()
        .any(|event| matches!(event.payload, EventPayload::EffectDenied { .. })));
    assert!(!events
        .iter()
        .any(|event| matches!(event.payload, EventPayload::MemoryWritten { .. })));
}

/// `NoCrossScopeRead` in `formal/memory/Memory.tla`: runs of two tenants
/// write and read their organization's memory at the same time, and each
/// reads back only its own tenant's value.
pub fn no_cross_scope_read(memory: &dyn Memory) {
    let tenants: Vec<String> = (0..2).map(|_| fresh("tenant")).collect();
    std::thread::scope(|scope| {
        for round in 0..8 {
            for tenant in &tenants {
                scope.spawn(move || {
                    let mine = format!("{tenant}/{round}");
                    let alice = fresh("alice");
                    let found = reads(
                        spec(&who(&alice, tenant)),
                        vec![
                            write(MemoryScope::Organization, &mine),
                            read(MemoryScope::Organization),
                        ],
                        memory,
                    );
                    let read_tenant = found[0]
                        .as_deref()
                        .and_then(|value| value.rsplit_once('/'))
                        .map(|(tenant, _)| tenant.to_string());
                    assert_eq!(read_tenant.as_deref(), Some(tenant.as_str()));
                });
            }
        }
    });
}

/// The counterexample `formal/memory` finds for the unscoped design, forced:
/// run 1 writes, run 2 of another tenant writes, then run 1 reads. Run 1
/// finds its own value.
pub fn no_cross_scope_read_in_the_model_trace(memory: &dyn Memory) {
    let (alice, bob) = (fresh("alice"), fresh("bob"));
    let (first, second) = (fresh("tenant"), fresh("tenant"));
    let mut one = Driver::boot(spec(&who(&alice, &first))).expect("boot");
    let mut two = Driver::boot(spec(&who(&bob, &second))).expect("boot");
    let perform = |driver: &mut Driver, effect: Effect| {
        driver.perform(&[effect], &[], &UnavailableModel, memory);
    };
    perform(&mut one, write(MemoryScope::Organization, "A"));
    perform(&mut two, write(MemoryScope::Organization, "B"));
    perform(&mut one, read(MemoryScope::Organization));
    assert_eq!(found(one.events()), [value("A")]);
}

/// Every scenario, in order.
pub fn all(memory: &dyn Memory) {
    run_memory_isolated_between_runs(memory);
    step_memory_isolated_between_steps(memory);
    agent_memory_survives_across_runs(memory);
    user_memory_isolated_between_tenants(memory);
    session_memory_belongs_to_its_user(memory);
    workspace_memory_belongs_to_its_organization(memory);
    global_memory_is_denied(memory);
    no_cross_scope_read(memory);
    no_cross_scope_read_in_the_model_trace(memory);
}
