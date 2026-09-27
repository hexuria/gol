mod common;

use std::sync::Arc;

use protocol::{
    Actor, AgentId, ArtifactId, Capability, CredentialSource, DispatchPhase, Event, EventPayload,
    EventSource, ExecutionPlacement, FailureClass, HarnessState, Limits, MessageRole, ModelMessage,
    ModelProvider, RunId, RunSpec, Timestamp, WorkModel,
};
use server::{
    accept_subscription_completion, box_container_name, fail_turn, open_turn, router_with_queue,
    AgentManifest, Append, GatewayCall, GatewayPoster, MemorySandbox, PostgresStore, RedisRunQueue,
    RunStore, SandboxError, SandboxHost, StoredArtifact, StoredRun, TurnError,
};

const POSTGRES_URL: &str = "postgres://gol:gol@127.0.0.1/gol";
const REDIS_URL: &str = "redis://127.0.0.1/";

fn spec() -> RunSpec {
    RunSpec::builder()
        .owner(protocol::Owner::new(
            "https://issuer.test",
            "user-1",
            "tenant-1",
        ))
        .agent(AgentId::new(), "1")
        .input("hello")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .limits(Limits {
            max_steps: 4,
            max_model_calls: 1,
        })
        .build()
}

#[test]
fn postgres_round_trips_event_and_artifact_on_a_new_connection() {
    let run = spec();
    let run_id = run.run_id;
    let artifact_id = ArtifactId::new();
    {
        let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
        store.put_agent(server::StoredAgent {
            manifest: AgentManifest {
                id: run.agent_id,
                version: "1".to_string(),
                instructions: "store".to_string(),
                tools: vec!["echo".to_string()],
                required_capabilities: vec![Capability::new("tool.echo")],
            },
            owner: run.owner.clone(),
        });
        let event = Event::record(
            protocol::EventSource::new(
                run_id,
                run.agent_id,
                "1",
                Actor::System,
                Timestamp::unix_millis(1),
            ),
            EventPayload::RunCreated,
        );
        store.put_run(StoredRun {
            spec: run.clone(),
            events: vec![event.clone()],
        });
        store.put_artifact(StoredArtifact {
            id: artifact_id,
            run_id,
            name: "note.txt".to_string(),
            body: b"saved".to_vec(),
        });
    }
    let store = PostgresStore::connect(POSTGRES_URL).expect("reconnect");
    let loaded = store.run(run_id).expect("run");
    assert_eq!(loaded.spec.input, "hello");
    assert!(matches!(
        loaded.events.as_slice(),
        [Event {
            payload: EventPayload::RunCreated,
            ..
        }]
    ));
    let artifact = store.artifact(artifact_id).expect("artifact");
    assert_eq!(artifact.body, b"saved");
    assert_eq!(artifact.name, "note.txt");
}

#[test]
fn redis_queue_pops_the_pushed_run_id() {
    let key = format!("gol:test:{}", RunId::new());
    let queue = RedisRunQueue::with_key(REDIS_URL, &key);
    let id = RunId::new();
    queue.push(id).expect("push");
    let popped = queue.pop().expect("pop").expect("queued id");
    assert_eq!(popped, id);
    assert!(queue.pop().expect("second pop").is_none());
}

#[tokio::test]
async fn create_run_writes_postgres_and_enqueues_redis() {
    let queue = RedisRunQueue::open(REDIS_URL);
    while queue.pop().expect("drain").is_some() {}

    let jev = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/v1/systemone"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(choice("echo")))
        .up_to_n_times(1)
        .mount(&jev)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/v1/systemone"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(choice("complete")))
        .mount(&jev)
        .await;

    let store =
        tokio::task::spawn_blocking(|| PostgresStore::connect(POSTGRES_URL).expect("connect"))
            .await
            .expect("connect thread");
    let app = router_with_queue(
        Arc::new(store),
        jev.uri(),
        Some(REDIS_URL.to_string()),
        common::authenticator(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let client = reqwest::Client::new();
    let agent_id = AgentId::new();
    let agent = client
        .post(format!("http://{addr}/v1/agents"))
        .header("authorization", common::bearer())
        .json(&AgentManifest {
            id: agent_id,
            version: "1".to_string(),
            instructions: "Echo the input, then finish.".to_string(),
            tools: vec!["echo".to_string()],
            required_capabilities: vec![Capability::new("tool.echo")],
        })
        .send()
        .await
        .expect("agent");
    assert!(agent.status().is_success());

    let created = client
        .post(format!("http://{addr}/v1/runs"))
        .header("authorization", common::bearer())
        .json(&serde_json::json!({
            "agent_id": agent_id,
            "agent_version": "1",
            "input": "hello",
            "placement": "Local",
            "work_model": {
                "provider": "OpenAI",
                "model_name": "gpt-test",
                "credential": "PlatformGateway"
            },
            "limits": { "max_steps": 8, "max_model_calls": 4 }
        }))
        .send()
        .await
        .expect("run");
    assert_eq!(created.status(), reqwest::StatusCode::OK);
    let created = created
        .json::<protocol::RunState>()
        .await
        .expect("run json");
    assert_eq!(created.harness, HarnessState::Idle);
    assert_eq!(created.dispatch, DispatchPhase::Created);
    assert_eq!(created.steps, 0);
    assert_eq!(created.model_calls, 0);

    let run_id = created.run_id;
    let stored = tokio::task::spawn_blocking(move || {
        PostgresStore::connect(POSTGRES_URL)
            .expect("reconnect")
            .run(run_id)
    })
    .await
    .expect("reconnect thread")
    .expect("stored run");
    assert!(matches!(
        stored.events.as_slice(),
        [Event {
            payload: EventPayload::UserMessage { text },
            ..
        }] if text == "hello"
    ));
    assert!(!stored
        .events
        .iter()
        .any(|event| matches!(event.payload, EventPayload::RunCompleted { .. })));

    let queued = queue.pop().expect("queue").expect("run id");
    assert_eq!(queued, created.run_id);

    let requests = jev.received_requests().await.expect("requests");
    assert!(
        !requests
            .iter()
            .any(|request| request.url.path() == "/v1/systemone"),
        "queued create_run called Jev"
    );
}

fn record(spec: &RunSpec, actor: Actor, at: i64, payload: EventPayload) -> Event {
    Event::record(
        EventSource::new(
            spec.run_id,
            spec.agent_id,
            &spec.agent_version,
            actor,
            Timestamp::unix_millis(at),
        ),
        payload,
    )
}

fn reconnect(id: RunId) -> StoredRun {
    PostgresStore::connect(POSTGRES_URL)
        .expect("reconnect")
        .run(id)
        .expect("run")
}

fn user_message(spec: &RunSpec, at: i64) -> Event {
    record(
        spec,
        Actor::System,
        at,
        EventPayload::UserMessage {
            text: "hello".to_string(),
        },
    )
}

fn completion_pair(spec: &RunSpec, text: &str, at: i64) -> (Event, Event) {
    (
        record(
            spec,
            Actor::Gateway,
            at,
            EventPayload::ModelResponded {
                message: ModelMessage {
                    role: MessageRole::Assistant,
                    text: text.to_string(),
                },
            },
        ),
        record(
            spec,
            Actor::System,
            at + 1,
            EventPayload::RunCompleted {
                outcome: text.to_string(),
            },
        ),
    )
}

#[test]
fn a_second_put_keeps_the_first_run() {
    let spec = spec();
    let run_id = spec.run_id;
    let user = user_message(&spec, 1);
    let started = record(&spec, Actor::System, 2, EventPayload::RunStarted);
    {
        let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
        store.put_run(StoredRun {
            spec: spec.clone(),
            events: vec![user.clone()],
        });
        store.put_run(StoredRun {
            spec,
            events: vec![user.clone(), started],
        });
    }
    assert_eq!(reconnect(run_id).events, vec![user]);
}

#[test]
fn append_extends_the_stored_log() {
    let spec = spec();
    let run_id = spec.run_id;
    let created = record(&spec, Actor::System, 1, EventPayload::RunCreated);
    let started = record(&spec, Actor::System, 2, EventPayload::RunStarted);
    let user = user_message(&spec, 3);
    let (responded, completed) = completion_pair(&spec, "gateway text", 4);
    let first = vec![created, started, user];
    let appended = {
        let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
        store.put_run(StoredRun {
            spec,
            events: first.clone(),
        });
        store.append_events(run_id, vec![responded.clone(), completed.clone()])
    };
    assert_eq!(appended, Append::Appended);
    let mut expected = first;
    expected.push(responded);
    expected.push(completed);
    assert_eq!(reconnect(run_id).events, expected);
}

#[test]
fn a_terminal_log_refuses_every_append() {
    let spec = spec();
    let run_id = spec.run_id;
    let user = user_message(&spec, 1);
    let (responded, completed) = completion_pair(&spec, "first", 2);
    let (again_responded, again_completed) = completion_pair(&spec, "second", 4);
    let late = user_message(&spec, 6);
    let outcomes = {
        let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
        store.put_run(StoredRun {
            spec,
            events: vec![user.clone()],
        });
        [
            store.append_events(run_id, vec![responded.clone(), completed.clone()]),
            store.append_events(run_id, vec![again_responded, again_completed]),
            store.append_events(run_id, vec![late]),
        ]
    };
    assert_eq!(
        outcomes,
        [Append::Appended, Append::Terminal, Append::Terminal]
    );
    assert_eq!(reconnect(run_id).events, vec![user, responded, completed]);
}

#[test]
fn every_terminal_payload_closes_the_log() {
    for terminal in [
        EventPayload::RunCancelled,
        EventPayload::RunExpired,
        EventPayload::RunFailed {
            class: FailureClass::Tool,
            message: "boom".to_string(),
        },
    ] {
        let spec = spec();
        let run_id = spec.run_id;
        let ended = record(&spec, Actor::System, 1, terminal);
        let late = user_message(&spec, 2);
        let outcome = {
            let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
            store.put_run(StoredRun {
                spec,
                events: vec![ended.clone()],
            });
            store.append_events(run_id, vec![late])
        };
        assert_eq!(outcome, Append::Terminal, "{:?}", ended.payload);
        assert_eq!(reconnect(run_id).events, vec![ended]);
    }
}

#[test]
fn append_to_a_missing_run_is_refused() {
    let spec = spec();
    let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
    let outcome = store.append_events(spec.run_id, vec![user_message(&spec, 1)]);
    assert_eq!(outcome, Append::Missing);
    assert!(store.run(spec.run_id).is_none());
}

fn choice(effect: &str) -> serde_json::Value {
    let mut probabilities = serde_json::json!({"echo": 0.0, "model": 0.0, "complete": 0.0});
    probabilities[effect] = serde_json::json!(1.0);
    serde_json::json!({
        "model": "jev-latest",
        "usage": {"input_tokens": 1, "output_tokens": 1},
        "answers": {
            "effect": {
                "type": "choice",
                "choice": effect,
                "confidence": 1.0,
                "probabilities": probabilities
            }
        }
    })
}

/// Postgres, recording the id of every run the server stores.
struct WatchedPostgres {
    inner: PostgresStore,
    ids: std::sync::Mutex<Vec<RunId>>,
}

impl RunStore for WatchedPostgres {
    fn put_agent(&self, agent: server::StoredAgent) -> server::PutAgent {
        self.inner.put_agent(agent)
    }

    fn agent(&self, id: protocol::AgentId) -> Option<server::StoredAgent> {
        self.inner.agent(id)
    }

    fn put_run(&self, run: StoredRun) {
        self.ids.lock().expect("ids").push(run.spec.run_id);
        self.inner.put_run(run);
    }

    fn append_events(&self, id: RunId, events: Vec<Event>) -> Append {
        self.inner.append_events(id, events)
    }

    fn run(&self, id: RunId) -> Option<StoredRun> {
        self.inner.run(id)
    }

    fn put_artifact(&self, artifact: StoredArtifact) {
        self.inner.put_artifact(artifact);
    }

    fn artifact(&self, id: ArtifactId) -> Option<StoredArtifact> {
        self.inner.artifact(id)
    }
}

/// Posts one run to a server on `store` and returns the status and the id of
/// the stored run, read back on a new connection.
async fn post_run(
    store: Arc<WatchedPostgres>,
    jev: &wiremock::MockServer,
    redis: Option<&str>,
) -> (u16, StoredRun) {
    let app = router_with_queue(
        store.clone(),
        jev.uri(),
        redis.map(str::to_string),
        common::authenticator(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    let agent_id = AgentId::new();
    let agent = reqwest::Client::new()
        .post(format!("http://{addr}/v1/agents"))
        .header("authorization", common::bearer())
        .json(&AgentManifest {
            id: agent_id,
            version: "1".to_string(),
            instructions: "Echo the input, then finish.".to_string(),
            tools: vec!["echo".to_string()],
            required_capabilities: vec![Capability::new("tool.echo")],
        })
        .send()
        .await
        .expect("agent");
    assert!(agent.status().is_success());
    let response = reqwest::Client::new()
        .post(format!("http://{addr}/v1/runs"))
        .header("authorization", common::bearer())
        .json(&serde_json::json!({
            "agent_id": agent_id,
            "agent_version": "1",
            "input": "hello",
            "placement": "Local",
            "work_model": {
                "provider": "OpenAI",
                "model_name": "gpt-test",
                "credential": "PlatformGateway"
            },
            "limits": { "max_steps": 8, "max_model_calls": 4 }
        }))
        .send()
        .await
        .expect("post run");
    let ids = store.ids.lock().expect("ids").clone();
    assert_eq!(ids.len(), 1, "the run was not stored");
    let id = ids[0];
    let stored = tokio::task::spawn_blocking(move || reconnect(id))
        .await
        .expect("reconnect thread");
    (response.status().as_u16(), stored)
}

async fn watched_postgres() -> Arc<WatchedPostgres> {
    let inner =
        tokio::task::spawn_blocking(|| PostgresStore::connect(POSTGRES_URL).expect("connect"))
            .await
            .expect("connect thread");
    Arc::new(WatchedPostgres {
        inner,
        ids: std::sync::Mutex::new(Vec::new()),
    })
}

#[tokio::test]
async fn jev_error_leaves_run_failed_in_postgres() {
    let jev = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/v1/systemone"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(choice("echo")))
        .up_to_n_times(1)
        .mount(&jev)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/v1/systemone"))
        .respond_with(wiremock::ResponseTemplate::new(500).set_body_string("jev is down"))
        .mount(&jev)
        .await;

    let (status, stored) = post_run(watched_postgres().await, &jev, None).await;
    assert_eq!(status, 502);
    let payloads: Vec<&EventPayload> = stored.events.iter().map(|event| &event.payload).collect();
    assert!(
        payloads
            .iter()
            .any(|payload| matches!(payload, EventPayload::ToolResult { output, .. } if output == "hello")),
        "{payloads:?}"
    );
    assert!(
        matches!(
            payloads.last(),
            Some(EventPayload::RunFailed {
                class: FailureClass::Dependency,
                message,
            }) if message.starts_with("decider: ")
        ),
        "{payloads:?}"
    );
}

#[tokio::test]
async fn redis_push_failure_leaves_run_failed_in_postgres() {
    let jev = wiremock::MockServer::start().await;
    let (status, stored) = post_run(
        watched_postgres().await,
        &jev,
        Some("redis://127.0.0.1:6390"),
    )
    .await;
    assert_eq!(status, 502);
    assert!(
        matches!(
            stored.events.as_slice(),
            [
                Event {
                    payload: EventPayload::UserMessage { .. },
                    ..
                },
                Event {
                    payload: EventPayload::RunFailed {
                        class: FailureClass::Infrastructure,
                        message,
                    },
                    ..
                },
            ] if message.starts_with("queue push failed: ")
        ),
        "{:?}",
        stored.events
    );
}

/// A subscription turn never posts to the gateway.
struct NoPost;

impl GatewayPoster for NoPost {
    fn complete(&self, _call: &GatewayCall) -> Result<String, String> {
        Err("subscription must not post".to_string())
    }
}

/// A sandbox host whose provision always fails.
struct NoProvision;

impl SandboxHost for NoProvision {
    fn provision(&self, name: &str) -> Result<(), SandboxError> {
        Err(SandboxError::Host(format!("cannot start {name}")))
    }

    fn destroy(&self, _name: &str) -> Result<(), SandboxError> {
        Ok(())
    }

    fn exists(&self, _name: &str) -> bool {
        false
    }

    fn absent(&self, _name: &str) -> Result<bool, SandboxError> {
        Ok(true)
    }

    fn launches_docker(&self) -> bool {
        false
    }
}

fn subscription_turn(placement: ExecutionPlacement) -> RunSpec {
    RunSpec::builder()
        .owner(protocol::Owner::new(
            "https://issuer.test",
            "user-1",
            "tenant-1",
        ))
        .agent(AgentId::new(), "1")
        .input("hello from the desktop")
        .placement(placement)
        .work_model(WorkModel {
            provider: ModelProvider::Anthropic,
            model_name: "claude-fixture".to_string(),
            credential: CredentialSource::BringYourOwn {
                secret_ref: "desktop-subscription".to_string(),
            },
        })
        .build()
}

fn failures(events: &[Event]) -> Vec<(FailureClass, String)> {
    events
        .iter()
        .filter_map(|event| match &event.payload {
            EventPayload::RunFailed { class, message } => Some((*class, message.clone())),
            _ => None,
        })
        .collect()
}

#[test]
fn provision_failure_run_failed_in_postgres() {
    let spec = subscription_turn(ExecutionPlacement::Box);
    let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
    open_turn(&store, spec.clone(), &NoPost, &NoProvision).expect_err("provision fails");
    let stored = reconnect(spec.run_id);
    assert_eq!(
        failures(&stored.events),
        vec![(
            FailureClass::Environment,
            format!(
                "provision: cannot start {}",
                box_container_name(spec.run_id)
            )
        )]
    );
}

#[test]
fn fail_turn_is_terminal_in_postgres() {
    let spec = subscription_turn(ExecutionPlacement::Box);
    let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
    let sandbox = MemorySandbox::default();
    open_turn(&store, spec.clone(), &NoPost, &sandbox).expect("open");

    fail_turn(&store, spec.run_id, "proxy said 529", &sandbox).expect("fail");
    assert!(!sandbox.exists(&box_container_name(spec.run_id)));

    let again = fail_turn(&store, spec.run_id, "again", &sandbox).expect_err("ended");
    assert!(
        matches!(again, TurnError::Conflict("turn already completed")),
        "{again:?}"
    );
    let completion =
        accept_subscription_completion(&store, spec.run_id, "late", &sandbox).expect_err("ended");
    assert!(
        matches!(completion, TurnError::Conflict("turn already completed")),
        "{completion:?}"
    );
    let stored = reconnect(spec.run_id);
    assert_eq!(
        failures(&stored.events),
        vec![(FailureClass::Dependency, "proxy said 529".to_string())]
    );
    assert!(!stored
        .events
        .iter()
        .any(|event| matches!(event.payload, EventPayload::RunCompleted { .. })));
}

// formal/agentowner FirstOwnerKeeps and OnlyOwnerStores on Postgres: the
// conditional upsert is one statement, so principals racing on their own
// connections leave the agent with the first to store it.
#[test]
fn first_owner_keeps_the_agent_in_postgres() {
    use server::{PutAgent, StoredAgent};
    let id = AgentId::new();
    let threads: Vec<_> = ["alice", "bob", "carol"]
        .into_iter()
        .map(|subject| {
            std::thread::spawn(move || {
                let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
                let owner = protocol::Owner::new("https://issuer.test", subject, "tenant-1");
                (0..20)
                    .map(|version| {
                        let put = store.put_agent(StoredAgent {
                            manifest: AgentManifest {
                                id,
                                version: version.to_string(),
                                instructions: "race".to_string(),
                                tools: Vec::new(),
                                required_capabilities: Vec::new(),
                            },
                            owner: owner.clone(),
                        });
                        (subject, put)
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let puts: Vec<_> = threads
        .into_iter()
        .flat_map(|t| t.join().unwrap())
        .collect();
    let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
    let owner = store.agent(id).expect("stored").owner.subject;
    for (subject, put) in puts {
        let expected = if subject == owner {
            PutAgent::Stored
        } else {
            PutAgent::OwnedByOther
        };
        assert_eq!(put, expected, "{subject} with owner {owner}");
    }
}

fn agent_for(id: AgentId, subject: &str, tenant: &str, version: &str) -> server::StoredAgent {
    server::StoredAgent {
        manifest: AgentManifest {
            id,
            version: version.to_string(),
            instructions: "owner".to_string(),
            tools: Vec::new(),
            required_capabilities: Vec::new(),
        },
        owner: protocol::Owner::new("https://issuer.test", subject, tenant),
    }
}

// The owner is issuer and subject in Postgres too: the same subject from
// another tenant replaces the manifest, and no one else can.
#[test]
fn the_owner_replaces_from_any_tenant_in_postgres() {
    use server::PutAgent;
    let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
    let id = AgentId::new();
    assert_eq!(
        store.put_agent(agent_for(id, "alice", "tenant-1", "1")),
        PutAgent::Stored
    );
    assert_eq!(
        store.put_agent(agent_for(id, "bob", "tenant-1", "9")),
        PutAgent::OwnedByOther
    );
    assert_eq!(store.agent(id).expect("stored").manifest.version, "1");
    assert_eq!(
        store.put_agent(agent_for(id, "alice", "tenant-2", "2")),
        PutAgent::Stored
    );
    let stored = PostgresStore::connect(POSTGRES_URL)
        .expect("connect")
        .agent(id)
        .expect("stored");
    assert_eq!(stored.manifest.version, "2");
    assert_eq!(stored.owner.tenant, "tenant-2");
    assert_eq!(stored.owner.subject, "alice");
}

// The model's one environment assumption, forced: while alice's insert of a
// new id is uncommitted, bob's put waits on it, and once alice commits bob is
// told the agent is hers.
#[test]
fn a_put_waits_for_a_concurrent_insert_of_the_same_id() {
    use server::PutAgent;
    let id = AgentId::new();
    PostgresStore::connect(POSTGRES_URL).expect("schema");
    let mut alice = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("connect");
    let mut tx = alice.transaction().expect("begin");
    let manifest = serde_json::to_value(agent_for(id, "alice", "tenant-1", "1").manifest).unwrap();
    tx.execute(
        "insert into agents (id, manifest, owner_issuer, owner_subject, owner_tenant)
         values ($1, $2, 'https://issuer.test', 'alice', 'tenant-1')",
        &[&id.as_uuid(), &manifest],
    )
    .expect("insert");
    let (sender, receiver) = std::sync::mpsc::channel();
    let bob = std::thread::spawn(move || {
        let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
        sender
            .send(store.put_agent(agent_for(id, "bob", "tenant-1", "9")))
            .unwrap();
    });
    assert!(
        receiver
            .recv_timeout(std::time::Duration::from_millis(500))
            .is_err(),
        "bob's put finished while alice's insert was uncommitted"
    );
    tx.commit().expect("commit");
    assert_eq!(receiver.recv().unwrap(), PutAgent::OwnedByOther);
    bob.join().unwrap();
    let stored = PostgresStore::connect(POSTGRES_URL)
        .expect("connect")
        .agent(id)
        .expect("stored");
    assert_eq!(stored.owner.subject, "alice");
}

// A database whose agents table predates owners is refused at connect,
// instead of failing on the first write.
#[test]
fn an_old_agents_table_is_refused_at_connect() {
    let mut admin = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("connect");
    let schema = format!("b2_old_{}", std::process::id());
    admin
        .batch_execute(&format!(
            "drop schema if exists {schema} cascade;
             create schema {schema};
             create table {schema}.agents (id uuid primary key, manifest jsonb not null);"
        ))
        .expect("old schema");
    let url = format!("{POSTGRES_URL}?options=-csearch_path%3D{schema}");
    let connected = PostgresStore::connect(&url).map(|_| ());
    // Drop the schema before asserting, so a failure leaves nothing behind.
    admin
        .batch_execute(&format!("drop schema {schema} cascade"))
        .expect("drop");
    let error = connected.expect_err("connect must fail");
    assert!(format!("{error:?}").contains("owner_issuer"), "{error:?}");
}
