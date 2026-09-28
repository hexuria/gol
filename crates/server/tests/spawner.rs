//! The owned spawner (Phase 1.2a): a delegation starts a queued child run of
//! an agent the parent's owner holds, with the capabilities both allow, the
//! parent's placement, work model and session, and the budget the driver
//! carved. A request asked twice starts one child. Needs Postgres and Redis,
//! as `queue_worker.rs` does.
use std::collections::BTreeMap;
use std::sync::Arc;

use harness::{AgentSpawner, ChildRequest};
use protocol::{
    AgentId, Capability, CredentialSource, EventPayload, ExecutionPlacement, Limits, Lineage,
    ModelProvider, Owner, RunId, RunSpec, WorkModel, SESSION_ID,
};
use server::{
    AgentManifest, InMemoryStore, OwnedSpawner, PostgresStore, RedisRunQueue, RunStore, StoredAgent,
};

const POSTGRES_URL: &str = "postgres://gol:gol@127.0.0.1/gol";
const REDIS_URL: &str = "redis://127.0.0.1/";

fn owner() -> Owner {
    Owner::new("https://issuer.test", "user-1", "tenant-1")
}

fn parent() -> RunSpec {
    RunSpec::builder()
        .owner(owner())
        .agent(AgentId::new(), "1")
        .input("plan the trip")
        .placement(ExecutionPlacement::Box)
        .work_model(WorkModel {
            provider: ModelProvider::Anthropic,
            model_name: "m".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .capabilities(vec![
            Capability::new("agent.delegate"),
            Capability::new("tool.echo"),
        ])
        .metadata(BTreeMap::from([(SESSION_ID.to_string(), "s1".to_string())]))
        .build()
}

/// An agent held by `holder`, asking for echo and memory reads.
fn agent_of(store: &dyn RunStore, holder: Owner) -> AgentId {
    let id = AgentId::new();
    store
        .put_agent(StoredAgent {
            manifest: AgentManifest {
                id,
                version: "7".to_string(),
                instructions: "Write it up.".to_string(),
                tools: vec!["echo".to_string()],
                required_capabilities: vec![
                    Capability::new("tool.echo"),
                    Capability::new("memory.read"),
                ],
            },
            owner: holder,
        })
        .expect("put agent");
    id
}

fn queue() -> Arc<RedisRunQueue> {
    Arc::new(RedisRunQueue::with_key(
        REDIS_URL,
        format!("gol:test:{}", RunId::new()),
    ))
}

fn given() -> Limits {
    Limits {
        max_steps: 3,
        max_model_calls: 2,
    }
}

fn request(parent: &RunSpec, agent_id: AgentId) -> ChildRequest<'_> {
    ChildRequest {
        parent,
        step: 2,
        agent_id,
        input: "draft",
        limits: given(),
    }
}

fn stores() -> Vec<Arc<dyn RunStore>> {
    vec![
        Arc::new(InMemoryStore::default()),
        Arc::new(PostgresStore::connect(POSTGRES_URL).expect("connect")),
    ]
}

#[test]
fn a_delegate_starts_an_owned_child_run() {
    for store in stores() {
        let queue = queue();
        let agent = agent_of(store.as_ref(), owner());
        let parent = parent();
        let spawner = OwnedSpawner::new(store.clone(), Some(queue.clone()));
        let child_id = spawner.start(request(&parent, agent)).expect("started");

        let child = store.run(child_id).expect("read").expect("stored").spec;
        let expected_id = RunSpec::builder()
            .owner(owner())
            .agent(agent, "7")
            .input("draft")
            .placement(parent.placement)
            .work_model(parent.work_model.clone())
            .child_of(&parent, 2)
            .build()
            .run_id;
        assert_eq!(child_id, expected_id);
        assert!(child.owner.is(&parent.owner));
        assert_eq!((child.agent_id, child.agent_version.as_str()), (agent, "7"));
        assert_eq!(child.input, "draft");
        assert_eq!(child.placement, parent.placement);
        assert_eq!(child.work_model, parent.work_model);
        assert_eq!(child.capabilities, vec![Capability::new("tool.echo")]);
        assert_eq!(child.limits, given());
        assert_eq!(
            child.metadata.get(SESSION_ID).map(String::as_str),
            Some("s1")
        );
        assert_eq!(
            child.lineage,
            Lineage {
                parent: Some(parent.run_id),
                root: Some(parent.run_id),
                hop: 1,
            }
        );
        let events = store.run(child_id).expect("read").expect("stored").events;
        assert!(matches!(
            events.iter().map(|e| &e.payload).collect::<Vec<_>>().as_slice(),
            [EventPayload::RunCreated, EventPayload::RunQueued, EventPayload::UserMessage { text }]
                if text == "draft"
        ));
        assert!(events
            .iter()
            .all(|event| event.envelope.parent_run_id == Some(parent.run_id)));
        assert_eq!(queue.queued().expect("queued"), [child_id]);
        assert_eq!(queue.pending().expect("pending"), []);
    }
}

// Decision 7A: only an agent the parent's owner holds.
#[test]
fn a_delegate_to_another_owners_agent_is_refused() {
    for store in stores() {
        let queue = queue();
        let agent = agent_of(
            store.as_ref(),
            Owner::new("https://issuer.test", "user-2", "tenant-1"),
        );
        let parent = parent();
        let spawner = OwnedSpawner::new(store.clone(), Some(queue.clone()));
        assert_eq!(
            spawner.start(request(&parent, agent)),
            Err("agent belongs to another owner".to_string())
        );
        assert_eq!(queue.queued().expect("queued"), []);
        assert_eq!(queue.pending().expect("pending"), []);
    }
}

#[test]
fn a_delegate_to_an_unknown_agent_is_refused() {
    for store in stores() {
        let queue = queue();
        let parent = parent();
        let spawner = OwnedSpawner::new(store.clone(), Some(queue.clone()));
        assert_eq!(
            spawner.start(request(&parent, AgentId::new())),
            Err("no such agent".to_string())
        );
        assert_eq!(queue.queued().expect("queued"), []);
    }
}

// A parent run again asks for the same child: one run, queued once.
#[test]
fn a_redelivered_parent_starts_one_child() {
    for store in stores() {
        let queue = queue();
        let agent = agent_of(store.as_ref(), owner());
        let parent = parent();
        let spawner = OwnedSpawner::new(store.clone(), Some(queue.clone()));
        let first = spawner.start(request(&parent, agent)).expect("first");
        let second = spawner.start(request(&parent, agent)).expect("second");
        assert_eq!(first, second);
        assert_eq!(queue.queued().expect("queued"), [first]);
    }
}

// Without the run queue a child could not run at all.
#[test]
fn delegate_without_the_queue_is_refused() {
    let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::default());
    let agent = agent_of(store.as_ref(), owner());
    let parent = parent();
    let spawner = OwnedSpawner::new(store.clone(), None);
    assert_eq!(
        spawner.start(request(&parent, agent)),
        Err("delegation needs the run queue".to_string())
    );
}
