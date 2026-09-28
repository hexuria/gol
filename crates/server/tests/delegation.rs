//! Delegation from a real run (Phase 1.2b): the worker offers Jev the
//! owner's other agents as `delegate:<name>`, a chosen one starts as a queued
//! child run, and a worker runs that child next. Needs Postgres and Redis, as
//! `queue_worker.rs` does.
use std::sync::Arc;

use harness::InMemory;
use protocol::{
    AgentId, Capability, CredentialSource, EventPayload, ExecutionPlacement, Limits, ModelProvider,
    Owner, RunId, RunSpec, WorkModel,
};
use serde_json::{json, Value};
use server::{
    is_terminal, queued_events, AgentManifest, InMemoryStore, PostgresStore, RedisRunQueue,
    RunStore, StoredAgent, StoredRun, Worker,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const POSTGRES_URL: &str = "postgres://gol:gol@127.0.0.1/gol";
const REDIS_URL: &str = "redis://127.0.0.1/";

fn stores() -> Vec<Arc<dyn RunStore>> {
    vec![
        Arc::new(InMemoryStore::default()),
        Arc::new(PostgresStore::connect(POSTGRES_URL).expect("connect")),
    ]
}

/// An owner of its own, so agents other tests store are not listed.
fn fresh_owner() -> Owner {
    Owner::new(
        "https://issuer.test",
        format!("user-{}", RunId::new()),
        "tenant-1",
    )
}

fn put_agent(store: &dyn RunStore, owner: &Owner, name: &str, capabilities: &[&str]) -> AgentId {
    let id = AgentId::new();
    store
        .put_agent(StoredAgent {
            manifest: AgentManifest {
                id,
                version: "1".to_string(),
                name: name.to_string(),
                description: format!("The {name}."),
                instructions: "Do it.".to_string(),
                tools: vec!["echo".to_string()],
                required_capabilities: capabilities
                    .iter()
                    .map(|name| Capability::new(*name))
                    .collect(),
            },
            owner: owner.clone(),
        })
        .expect("put agent");
    id
}

/// Runs `work` on a plain thread: the stores and the queue block, which the
/// Postgres client refuses on a Tokio worker.
fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::spawn(work).join().expect("thread")
}

#[test]
fn agents_of_lists_only_the_owners_agents() {
    for store in stores() {
        let owner = fresh_owner();
        let first = put_agent(store.as_ref(), &owner, "writer", &[]);
        let second = put_agent(store.as_ref(), &owner, "reader", &[]);
        put_agent(store.as_ref(), &fresh_owner(), "stranger", &[]);
        // The same principal in another tenant is the same owner (`Owner::is`).
        let elsewhere = Owner::new("https://issuer.test", owner.subject.clone(), "tenant-2");
        let third = put_agent(store.as_ref(), &elsewhere, "copier", &[]);

        let mut listed: Vec<AgentId> = store
            .agents_of(&owner)
            .expect("agents")
            .into_iter()
            .map(|agent| agent.manifest.id)
            .collect();
        listed.sort_by_key(|id| id.as_uuid());
        let mut expected = vec![first, second, third];
        expected.sort_by_key(|id| id.as_uuid());
        assert_eq!(listed, expected);
        let names: Vec<String> = store
            .agents_of(&owner)
            .expect("agents")
            .into_iter()
            .filter(|agent| agent.manifest.id == first)
            .map(|agent| agent.manifest.name)
            .collect();
        assert_eq!(names, ["writer"]);
    }
}

// Manifests stored before 1.2b have no name or description.
#[test]
fn a_manifest_without_a_name_loads() {
    let id = AgentId::new();
    let manifest: AgentManifest = serde_json::from_value(json!({
        "id": id,
        "version": "1",
        "instructions": "Do it.",
        "tools": [],
        "required_capabilities": [],
    }))
    .expect("an old manifest loads");
    assert_eq!(manifest.id, id);
    assert_eq!(
        (manifest.name.as_str(), manifest.description.as_str()),
        ("", "")
    );
}

/// Jev answers each label once, in order, and repeats the last one.
async fn jev(labels: &[&str]) -> MockServer {
    let server = MockServer::start().await;
    let answer = |label: &str| {
        json!({
            "model": "jev-latest",
            "usage": {"input_tokens": 1, "output_tokens": 1},
            "answers": {"effect": {"type": "choice", "choice": label,
                "confidence": 1.0, "probabilities": {label: 1.0}}}
        })
    };
    let (last, first) = labels.split_last().expect("at least one answer");
    for label in first {
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .respond_with(ResponseTemplate::new(200).set_body_json(answer(label)))
            .up_to_n_times(1)
            .mount(&server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(answer(last)))
        .mount(&server)
        .await;
    server
}

// A parent run's Jev picks `delegate:writer`. The worker starts the writer as
// a queued child run with the parent's lineage, ends the parent, and then runs
// the child. Only the owner's other agents are offered.
#[tokio::test(flavor = "multi_thread")]
async fn a_worker_runs_a_delegating_parent_and_then_its_child() {
    for which in 0..2 {
        let server = jev(&["delegate:writer", "complete"]).await;
        let uri = server.uri();
        let (parent_id, child_id) = blocking(move || {
            // Built here: the Postgres client cannot start on a Tokio worker.
            let store = stores().swap_remove(which);
            let owner = fresh_owner();
            let planner = put_agent(store.as_ref(), &owner, "planner", &["agent.delegate"]);
            let writer = put_agent(store.as_ref(), &owner, "writer", &["tool.echo"]);
            put_agent(store.as_ref(), &fresh_owner(), "stranger", &[]);
            let parent = RunSpec::builder()
                .owner(owner)
                .agent(planner, "1")
                .input("plan the trip")
                .placement(ExecutionPlacement::Local)
                .work_model(WorkModel {
                    provider: ModelProvider::OpenAI,
                    model_name: "gpt-test".to_string(),
                    credential: CredentialSource::PlatformGateway,
                })
                .capabilities(vec![
                    Capability::new("agent.delegate"),
                    Capability::new("tool.echo"),
                ])
                .limits(Limits {
                    max_steps: 8,
                    max_model_calls: 4,
                })
                .build();
            let key = format!("gol:test:{}", RunId::new());
            let queue = RedisRunQueue::with_key(REDIS_URL, &key);
            store
                .put_run(StoredRun {
                    events: queued_events(&parent),
                    spec: parent.clone(),
                })
                .expect("put run");
            queue.push(parent.run_id).expect("push");
            let worker = Worker::builder()
                .queue(RedisRunQueue::with_key(REDIS_URL, &key))
                .store(store.clone())
                .memory(Arc::new(InMemory::default()))
                .jev(uri)
                .build();

            assert_eq!(worker.work_one().expect("work"), Some(parent.run_id));
            let events = store.run(parent.run_id).expect("read").expect("run").events;
            let children: Vec<RunId> = events
                .iter()
                .filter_map(|event| match &event.payload {
                    EventPayload::ChildStarted {
                        run_id, agent_id, ..
                    } if *agent_id == writer => Some(*run_id),
                    _ => None,
                })
                .collect();
            let [child_id] = children.as_slice() else {
                panic!("one child started: {events:?}")
            };
            assert!(matches!(
                events.last().map(|event| &event.payload),
                Some(EventPayload::RunCompleted { .. })
            ));
            assert_eq!(queue.queued().expect("queued"), [*child_id]);

            assert_eq!(worker.work_one().expect("work"), Some(*child_id));
            let child = store.run(*child_id).expect("read").expect("child");
            assert_eq!(child.spec.agent_id, writer);
            assert_eq!(child.spec.lineage.parent, Some(parent.run_id));
            assert!(child
                .events
                .iter()
                .all(|event| event.envelope.parent_run_id == Some(parent.run_id)));
            assert!(matches!(
                child
                    .events
                    .iter()
                    .filter(|event| is_terminal(&event.payload))
                    .map(|event| &event.payload)
                    .collect::<Vec<_>>()
                    .as_slice(),
                [EventPayload::RunCompleted { .. }]
            ));
            assert_eq!(queue.queued().expect("queued"), []);
            (parent.run_id, *child_id)
        });
        assert_ne!(parent_id, child_id);

        let sent: Vec<Value> = server
            .received_requests()
            .await
            .expect("requests")
            .iter()
            .map(|request| serde_json::from_slice(&request.body).expect("json"))
            .collect();
        let offered = sent[0]["questions"]["effect"]["criteria"].to_string();
        assert!(offered.contains("delegate:writer"), "{offered}");
        assert!(!offered.contains("stranger"), "{offered}");
        assert!(!offered.contains("delegate:planner"), "{offered}");
        // The child holds no `agent.delegate`: it is offered no delegation.
        let last = sent.last().expect("the child asked Jev")["questions"]["effect"]["criteria"]
            .to_string();
        assert!(!last.contains("delegate:"), "{last}");
    }
}
