//! The owned spawner (Phase 1.2a): a delegation starts a queued child run of
//! an agent the parent's owner holds, with the capabilities both allow, the
//! parent's placement, work model and session, and the budget the driver
//! carved. A request asked twice starts one child. Needs Postgres and Redis,
//! as `queue_worker.rs` does.
mod common;

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
                name: String::new(),
                description: String::new(),
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
        let started = spawner.start(request(&parent, agent)).expect("started");
        assert_eq!(started.limits, given());
        let child_id = started.run_id;

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
        assert_eq!(queue.queued().expect("queued"), [first.run_id]);
        // Not left pending, where a sweep would push it a second time.
        assert_eq!(queue.pending().expect("pending"), []);
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

// A child that was stored but could not be queued was ended before it ever
// ran. Asking for it again does not report it as started.
#[test]
fn a_child_that_could_not_be_queued_is_not_started_again() {
    for store in stores() {
        let queue = queue();
        let agent = agent_of(store.as_ref(), owner());
        let parent = parent();
        let child = RunSpec::builder()
            .owner(owner())
            .agent(agent, "7")
            .input("draft")
            .placement(parent.placement)
            .work_model(parent.work_model.clone())
            .child_of(&parent, 2)
            .build();
        let mut events = server::queued_events(&child);
        events.push(server::run_failed_event(
            &child,
            protocol::FailureClass::Infrastructure,
            "queue push failed: down".to_string(),
        ));
        store
            .put_run(server::StoredRun {
                spec: child.clone(),
                events,
            })
            .expect("put ended child");
        let spawner = OwnedSpawner::new(store.clone(), Some(queue.clone()));
        assert_eq!(
            spawner.start(request(&parent, agent)),
            Err("the child could not be started".to_string())
        );
        assert_eq!(queue.queued().expect("queued"), []);
        assert_eq!(queue.pending().expect("pending"), []);
    }
}

// A retry reports the limits the stored child actually has, not the carve it
// asked for this time (the parent may have spent more since).
#[test]
fn a_retry_reports_the_stored_childs_limits() {
    for store in stores() {
        let queue = queue();
        let agent = agent_of(store.as_ref(), owner());
        let parent = parent();
        let spawner = OwnedSpawner::new(store.clone(), Some(queue.clone()));
        let first = spawner.start(request(&parent, agent)).expect("first");
        let mut smaller = request(&parent, agent);
        smaller.limits = Limits {
            max_steps: 1,
            max_model_calls: 1,
        };
        let second = spawner.start(smaller).expect("second");
        assert_eq!(second.run_id, first.run_id);
        assert_eq!(second.limits, given());
    }
}

// A push that fails leaves the child stored and pending for the sweep; it is
// not ended, since its id is fixed and a retry could never start it again.
#[test]
fn a_child_whose_push_failed_is_left_to_the_sweep() {
    let proxy = common::redis_proxy::RedisProxy::start();
    let key = format!("gol:test:{}", RunId::new());
    let direct = RedisRunQueue::with_key(REDIS_URL, &key);
    let store: Arc<dyn RunStore> = Arc::new(GoesDownAfterPut {
        inner: InMemoryStore::default(),
        proxy: proxy.clone(),
    });
    let agent = agent_of(store.as_ref(), owner());
    let parent = parent();
    let through_proxy = Arc::new(RedisRunQueue::with_key(proxy.url(0), &key));
    let spawner = OwnedSpawner::new(store.clone(), Some(through_proxy));
    let started = spawner
        .start(request(&parent, agent))
        .expect("left to the sweep");
    let child = store.run(started.run_id).expect("read").expect("stored");
    assert!(child
        .events
        .iter()
        .all(|event| !server::is_terminal(&event.payload)));
    assert_eq!(direct.pending().expect("pending"), [started.run_id]);
    assert_eq!(direct.queued().expect("queued"), []);
    assert_eq!(
        server::sweep(
            &direct,
            store.as_ref(),
            std::time::Duration::ZERO,
            std::time::Duration::ZERO
        ),
        Ok(vec![started.run_id])
    );
}

/// An in-memory store whose Redis goes away as soon as a run is put.
struct GoesDownAfterPut {
    inner: InMemoryStore,
    proxy: common::redis_proxy::RedisProxy,
}

impl RunStore for GoesDownAfterPut {
    fn put_agent(&self, agent: StoredAgent) -> Result<server::PutAgent, server::StoreError> {
        self.inner.put_agent(agent)
    }
    fn agent(&self, id: AgentId) -> Result<Option<StoredAgent>, server::StoreError> {
        self.inner.agent(id)
    }
    fn agents_of(&self, owner: &Owner) -> Result<Vec<StoredAgent>, server::StoreError> {
        self.inner.agents_of(owner)
    }
    fn put_run(&self, run: server::StoredRun) -> Result<server::PutRun, server::StoreError> {
        let put = self.inner.put_run(run)?;
        self.proxy.go_down();
        Ok(put)
    }
    fn append_events(
        &self,
        id: RunId,
        events: Vec<protocol::Event>,
    ) -> Result<server::Append, server::StoreError> {
        self.inner.append_events(id, events)
    }
    fn run(&self, id: RunId) -> Result<Option<server::StoredRun>, server::StoreError> {
        self.inner.run(id)
    }
    fn put_artifact(&self, artifact: server::StoredArtifact) -> Result<(), server::StoreError> {
        self.inner.put_artifact(artifact)
    }
    fn artifact(
        &self,
        id: protocol::ArtifactId,
    ) -> Result<Option<server::StoredArtifact>, server::StoreError> {
        self.inner.artifact(id)
    }
}
