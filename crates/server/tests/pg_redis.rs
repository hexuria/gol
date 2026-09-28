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
        store
            .put_agent(server::StoredAgent {
                manifest: AgentManifest {
                    id: run.agent_id,
                    version: "1".to_string(),
                    instructions: "store".to_string(),
                    tools: vec!["echo".to_string()],
                    required_capabilities: vec![Capability::new("tool.echo")],
                },
                owner: run.owner.clone(),
            })
            .expect("put agent");
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
        store
            .put_run(StoredRun {
                spec: run.clone(),
                events: vec![event.clone()],
            })
            .expect("put run");
        store
            .put_artifact(StoredArtifact {
                id: artifact_id,
                run_id,
                name: "note.txt".to_string(),
                body: b"saved".to_vec(),
            })
            .expect("put artifact");
    }
    let store = PostgresStore::connect(POSTGRES_URL).expect("reconnect");
    let loaded = store.run(run_id).expect("store").expect("run");
    assert_eq!(loaded.spec.input, "hello");
    assert!(matches!(
        loaded.events.as_slice(),
        [Event {
            payload: EventPayload::RunCreated,
            ..
        }]
    ));
    let artifact = store
        .artifact(artifact_id)
        .expect("store")
        .expect("artifact");
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
    assert_eq!(created.dispatch, DispatchPhase::Queued);
    assert_eq!(created.steps, 0);
    assert_eq!(created.model_calls, 0);

    let run_id = created.run_id;
    let stored = tokio::task::spawn_blocking(move || {
        PostgresStore::connect(POSTGRES_URL)
            .expect("reconnect")
            .run(run_id)
            .expect("store")
    })
    .await
    .expect("reconnect thread")
    .expect("stored run");
    // redis_run_records_created_and_queued: the run is on the record as
    // created and queued, with its message, before the push.
    assert!(matches!(
        stored.events.as_slice(),
        [
            Event { payload: EventPayload::RunCreated, .. },
            Event { payload: EventPayload::RunQueued, .. },
            Event { payload: EventPayload::UserMessage { text }, .. },
        ] if text == "hello"
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
        .expect("store")
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
        store
            .put_run(StoredRun {
                spec: spec.clone(),
                events: vec![user.clone()],
            })
            .expect("put run");
        store
            .put_run(StoredRun {
                spec,
                events: vec![user.clone(), started],
            })
            .expect("put run");
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
        store
            .put_run(StoredRun {
                spec,
                events: first.clone(),
            })
            .expect("put run");
        store.append_events(run_id, vec![responded.clone(), completed.clone()])
    };
    assert_eq!(appended, Ok(Append::Appended));
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
        store
            .put_run(StoredRun {
                spec,
                events: vec![user.clone()],
            })
            .expect("put run");
        [
            store.append_events(run_id, vec![responded.clone(), completed.clone()]),
            store.append_events(run_id, vec![again_responded, again_completed]),
            store.append_events(run_id, vec![late]),
        ]
    };
    assert_eq!(
        outcomes,
        [
            Ok(Append::Appended),
            Ok(Append::Terminal),
            Ok(Append::Terminal)
        ]
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
            store
                .put_run(StoredRun {
                    spec,
                    events: vec![ended.clone()],
                })
                .expect("put run");
            store.append_events(run_id, vec![late])
        };
        assert_eq!(outcome, Ok(Append::Terminal), "{:?}", ended.payload);
        assert_eq!(reconnect(run_id).events, vec![ended]);
    }
}

#[test]
fn append_to_a_missing_run_is_refused() {
    let spec = spec();
    let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
    let outcome = store.append_events(spec.run_id, vec![user_message(&spec, 1)]);
    assert_eq!(outcome, Ok(Append::Missing));
    assert!(store.run(spec.run_id).expect("store").is_none());
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
    fn put_agent(
        &self,
        agent: server::StoredAgent,
    ) -> Result<server::PutAgent, server::StoreError> {
        self.inner.put_agent(agent)
    }

    fn agent(
        &self,
        id: protocol::AgentId,
    ) -> Result<Option<server::StoredAgent>, server::StoreError> {
        self.inner.agent(id)
    }

    fn put_run(&self, run: StoredRun) -> Result<(), server::StoreError> {
        self.ids.lock().expect("ids").push(run.spec.run_id);
        self.inner.put_run(run)
    }

    fn append_events(&self, id: RunId, events: Vec<Event>) -> Result<Append, server::StoreError> {
        self.inner.append_events(id, events)
    }

    fn run(&self, id: RunId) -> Result<Option<StoredRun>, server::StoreError> {
        self.inner.run(id)
    }

    fn put_artifact(&self, artifact: StoredArtifact) -> Result<(), server::StoreError> {
        self.inner.put_artifact(artifact)
    }

    fn artifact(&self, id: ArtifactId) -> Result<Option<StoredArtifact>, server::StoreError> {
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
                    payload: EventPayload::RunCreated,
                    ..
                },
                Event {
                    payload: EventPayload::RunQueued,
                    ..
                },
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
    let owner = store
        .agent(id)
        .expect("store")
        .expect("stored")
        .owner
        .subject;
    for (subject, put) in puts {
        let expected = if subject == owner {
            PutAgent::Stored
        } else {
            PutAgent::OwnedByOther
        };
        assert_eq!(put, Ok(expected), "{subject} with owner {owner}");
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
        Ok(PutAgent::Stored)
    );
    assert_eq!(
        store.put_agent(agent_for(id, "bob", "tenant-1", "9")),
        Ok(PutAgent::OwnedByOther)
    );
    assert_eq!(
        store
            .agent(id)
            .expect("store")
            .expect("stored")
            .manifest
            .version,
        "1"
    );
    assert_eq!(
        store.put_agent(agent_for(id, "alice", "tenant-2", "2")),
        Ok(PutAgent::Stored)
    );
    let stored = PostgresStore::connect(POSTGRES_URL)
        .expect("connect")
        .agent(id)
        .expect("store")
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
    assert_eq!(receiver.recv().unwrap(), Ok(PutAgent::OwnedByOther));
    bob.join().unwrap();
    let stored = PostgresStore::connect(POSTGRES_URL)
        .expect("connect")
        .agent(id)
        .expect("store")
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

/// Terminates every backend whose `application_name` is `application` and
/// waits until Postgres no longer lists them. Returns how many it terminated.
fn terminate_backends(application: &str) -> i64 {
    let mut admin = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("admin");
    let terminated: i64 = admin
        .query_one(
            "select count(pg_terminate_backend(pid)) from pg_stat_activity
             where application_name = $1",
            &[&application],
        )
        .expect("terminate")
        .get(0);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let alive: i64 = admin
            .query_one(
                "select count(*) from pg_stat_activity where application_name = $1",
                &[&application],
            )
            .expect("activity")
            .get(0);
        if alive == 0 {
            return terminated;
        }
        assert!(std::time::Instant::now() < deadline, "backend still alive");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

// An idle connection whose backend was killed is found dead when the pool
// checks it out and replaced before the request uses it (C2; C1 answered one
// 503 first). The server process never restarts.
#[tokio::test]
async fn a_killed_idle_backend_is_replaced_before_the_next_request() {
    let application = format!("gol_c1_{}", uuid::Uuid::new_v4().simple());
    let url = format!("{POSTGRES_URL}?application_name={application}");
    let spec = spec();
    let run_id = spec.run_id;
    let store = tokio::task::spawn_blocking(move || {
        let store = PostgresStore::connect(&url).expect("connect");
        let events = vec![user_message(&spec, 1)];
        store.put_run(StoredRun { spec, events }).expect("put run");
        store
    })
    .await
    .expect("connect thread");
    let app = server::router(
        Arc::new(store),
        "http://127.0.0.1:9",
        common::authenticator(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    let get = || async {
        let response = reqwest::Client::new()
            .get(format!("http://{addr}/v1/runs/{run_id}"))
            .header("authorization", common::bearer())
            .send()
            .await
            .expect("get");
        (
            response.status().as_u16(),
            response.text().await.expect("body"),
        )
    };

    assert_eq!(get().await.0, 200, "before the kill");
    let killed = tokio::task::spawn_blocking(move || terminate_backends(&application))
        .await
        .expect("terminate thread");
    assert_eq!(killed, 1);
    assert_eq!(get().await.0, 200, "after the kill");
    assert_eq!(get().await.0, 200, "and after that");
}

/// Waits until the backend of `application` is waiting on a lock.
fn wait_for_lock_wait(application: &str) {
    wait_for_lock_waits(application, 1);
}

/// Waits until `count` backends of `application` are waiting on a lock.
fn wait_for_lock_waits(application: &str, count: i64) {
    let mut admin = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("admin");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let waiting: i64 = admin
            .query_one(
                "select count(*) from pg_stat_activity
                 where application_name = $1 and wait_event_type = 'Lock'",
                &[&application],
            )
            .expect("activity")
            .get(0);
        if waiting == count {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "put never waited");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

// A backend killed while a statement runs reports a database error, not a
// closed connection. The call that saw it fails, and the next call still
// reconnects (owner decision 1A).
#[test]
fn a_backend_killed_mid_statement_is_replaced_on_the_next_call() {
    let application = format!("gol_c1_{}", uuid::Uuid::new_v4().simple());
    let url = format!("{POSTGRES_URL}?application_name={application}");
    let store = Arc::new(PostgresStore::connect(&url).expect("connect"));
    let id = AgentId::new();
    let mut alice = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("connect");
    let mut tx = alice.transaction().expect("begin");
    let manifest = serde_json::to_value(agent_for(id, "alice", "tenant-1", "1").manifest).unwrap();
    tx.execute(
        "insert into agents (id, manifest, owner_issuer, owner_subject, owner_tenant)
         values ($1, $2, 'https://issuer.test', 'alice', 'tenant-1')",
        &[&id.as_uuid(), &manifest],
    )
    .expect("insert");
    let blocked = {
        let store = store.clone();
        std::thread::spawn(move || store.put_agent(agent_for(id, "bob", "tenant-1", "9")))
    };
    wait_for_lock_wait(&application);
    assert_eq!(terminate_backends(&application), 1);
    assert!(blocked.join().unwrap().is_err(), "the killed put");
    tx.rollback().expect("rollback");
    assert_eq!(store.run(RunId::new()).map(|run| run.is_none()), Ok(true));
}

fn completed(spec: &RunSpec, at: i64, outcome: String) -> Event {
    record(
        spec,
        Actor::System,
        at,
        EventPayload::RunCompleted { outcome },
    )
}

// Sixteen writers on sixteen pooled connections race a terminal append on one
// run: the run row lock admits one, and the rest see its terminal event.
#[test]
fn sixteen_racing_terminal_appends_one_wins() {
    let application = format!("gol_c2_{}", uuid::Uuid::new_v4().simple());
    let url = format!("{POSTGRES_URL}?application_name={application}");
    let store = Arc::new(PostgresStore::connect_with_pool_size(&url, 16).expect("connect"));
    // Open all sixteen connections first: the pool opens them one at a time,
    // which would otherwise line the racers up. Sixteen puts wait on a row
    // alice holds, each on its own connection, until she rolls back.
    let held = AgentId::new();
    let mut alice = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("connect");
    let mut tx = alice.transaction().expect("begin");
    let manifest =
        serde_json::to_value(agent_for(held, "alice", "tenant-1", "1").manifest).unwrap();
    tx.execute(
        "insert into agents (id, manifest, owner_issuer, owner_subject, owner_tenant)
         values ($1, $2, 'https://issuer.test', 'alice', 'tenant-1')",
        &[&held.as_uuid(), &manifest],
    )
    .expect("insert");
    let warmers: Vec<_> = (0..16)
        .map(|_| {
            let store = store.clone();
            std::thread::spawn(move || store.put_agent(agent_for(held, "bob", "tenant-1", "9")))
        })
        .collect();
    wait_for_lock_waits(&application, 16);
    tx.rollback().expect("rollback");
    for warmer in warmers {
        warmer.join().unwrap().expect("warm put");
    }

    let spec = spec();
    let run_id = spec.run_id;
    store
        .put_run(StoredRun {
            spec: spec.clone(),
            events: vec![user_message(&spec, 1)],
        })
        .expect("put run");
    let start = Arc::new(std::sync::Barrier::new(16));
    let racers: Vec<_> = (0..16)
        .map(|n| {
            let (store, start, spec) = (store.clone(), start.clone(), spec.clone());
            std::thread::spawn(move || {
                start.wait();
                store.append_events(run_id, vec![completed(&spec, 2, n.to_string())])
            })
        })
        .collect();
    let outcomes: Vec<Append> = racers
        .into_iter()
        .map(|racer| racer.join().unwrap().expect("append"))
        .collect();
    let won = outcomes.iter().filter(|o| **o == Append::Appended).count();
    let refused = outcomes.iter().filter(|o| **o == Append::Terminal).count();
    assert_eq!((won, refused), (1, 15));
    let events = reconnect(run_id).events;
    assert_eq!(events.len(), 2);
    assert_eq!(
        events
            .iter()
            .filter(|event| server::is_terminal(&event.payload))
            .count(),
        1
    );
}

// Postgres keeps one row per event, numbered from 1, and pages by count.
#[test]
fn postgres_pages_events_after_seq() {
    let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
    let spec = spec();
    let run_id = spec.run_id;
    let first: Vec<Event> = (1..=3).map(|at| user_message(&spec, at)).collect();
    let more: Vec<Event> = (4..=5).map(|at| user_message(&spec, at)).collect();
    store
        .put_run(StoredRun {
            spec: spec.clone(),
            events: first.clone(),
        })
        .expect("put run");
    assert_eq!(
        store.append_events(run_id, more.clone()),
        Ok(Append::Appended)
    );
    let all: Vec<Event> = first.into_iter().chain(more).collect();
    let page = |after, limit| {
        store
            .run_page(run_id, after, limit)
            .expect("store")
            .expect("run")
            .events
    };
    assert_eq!(page(0, 2), all[0..2]);
    assert_eq!(page(2, 2), all[2..4]);
    assert_eq!(page(4, 2), all[4..5]);
    assert!(page(5, 2).is_empty());
    assert_eq!(page(0, 500), all);
    assert_eq!(reconnect(run_id).events, all);
    assert!(store.run_page(RunId::new(), 0, 2).expect("store").is_none());
}

// One call waiting on a lock no longer holds up the others: each takes its
// own pooled connection (C1 serialized every call on one).
#[test]
fn pool_serves_concurrent_requests() {
    let application = format!("gol_c2_{}", uuid::Uuid::new_v4().simple());
    let url = format!("{POSTGRES_URL}?application_name={application}");
    let store = Arc::new(PostgresStore::connect(&url).expect("connect"));
    let id = AgentId::new();
    let mut alice = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("connect");
    let mut tx = alice.transaction().expect("begin");
    let manifest = serde_json::to_value(agent_for(id, "alice", "tenant-1", "1").manifest).unwrap();
    tx.execute(
        "insert into agents (id, manifest, owner_issuer, owner_subject, owner_tenant)
         values ($1, $2, 'https://issuer.test', 'alice', 'tenant-1')",
        &[&id.as_uuid(), &manifest],
    )
    .expect("insert");
    let blocked = {
        let store = store.clone();
        std::thread::spawn(move || store.put_agent(agent_for(id, "bob", "tenant-1", "9")))
    };
    wait_for_lock_wait(&application);
    let started = std::time::Instant::now();
    assert_eq!(store.run(RunId::new()).map(|run| run.is_none()), Ok(true));
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "a read waited behind the blocked put"
    );
    tx.rollback().expect("rollback");
    assert_eq!(blocked.join().unwrap(), Ok(server::PutAgent::Stored));
}

// A database whose runs table still holds the events as one jsonb column is
// refused at connect (owner decision 2A for C2), before anything is written.
#[test]
fn an_old_runs_table_is_refused_at_connect() {
    let mut admin = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("connect");
    let schema = format!("c2_old_{}", std::process::id());
    admin
        .batch_execute(&format!(
            "drop schema if exists {schema} cascade;
             create schema {schema};
             create table {schema}.runs (id uuid primary key, spec jsonb not null, events jsonb not null);"
        ))
        .expect("old schema");
    let url = format!("{POSTGRES_URL}?options=-csearch_path%3D{schema}");
    let connected = PostgresStore::connect(&url).map(|_| ());
    let created: i64 = admin
        .query_one(
            "select count(*) from information_schema.tables where table_schema = $1",
            &[&schema],
        )
        .expect("tables")
        .get(0);
    admin
        .batch_execute(&format!("drop schema {schema} cascade"))
        .expect("drop");
    let error = connected.expect_err("connect must fail");
    assert!(
        format!("{error}").contains("drop tables runs and run_events"),
        "{error}"
    );
    assert_eq!(created, 1, "connect created tables before refusing");
}

/// While an outside session holds the run row with an uncommitted event at
/// seq 2, the store appends; the session then commits. Returns the append's
/// outcome and how many events the run holds after.
fn forced_append_after_a_holder(
    extra: &str,
    held_terminal: bool,
) -> (Result<Append, server::StoreError>, usize) {
    let application = format!("gol_c2_{}", uuid::Uuid::new_v4().simple());
    let url = format!("{POSTGRES_URL}?application_name={application}{extra}");
    let store = Arc::new(PostgresStore::connect(&url).expect("connect"));
    let spec = spec();
    store
        .put_run(StoredRun {
            spec: spec.clone(),
            events: vec![user_message(&spec, 1)],
        })
        .expect("put run");
    let mut holder = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("holder");
    let mut tx = holder.transaction().expect("begin");
    tx.query_one(
        "select 1 from runs where id = $1 for update",
        &[&spec.run_id.as_uuid()],
    )
    .expect("lock");
    let held = if held_terminal {
        completed(&spec, 2, "held".to_string())
    } else {
        user_message(&spec, 2)
    };
    tx.execute(
        "insert into run_events (run_id, seq, body, terminal) values ($1, 2, $2, $3)",
        &[
            &spec.run_id.as_uuid(),
            &serde_json::to_value(&held).unwrap(),
            &held_terminal,
        ],
    )
    .expect("held event");
    let waiter = {
        let (store, spec) = (store.clone(), spec.clone());
        std::thread::spawn(move || store.append_events(spec.run_id, vec![user_message(&spec, 3)]))
    };
    wait_for_lock_wait(&application);
    tx.commit().expect("commit");
    let outcome = waiter.join().unwrap();
    let len = reconnect(spec.run_id).events.len();
    (outcome, len)
}

// Forced interleavings of the run row lock (T2). The waiting append sees what
// the holder committed, whatever the session's default isolation: it is
// refused after a terminal event and numbered after a plain one.
#[test]
fn a_waiting_append_sees_what_the_lock_holder_committed() {
    let serializable = "&options=-cdefault_transaction_isolation%3Dserializable";
    let mut session = postgres::Client::connect(
        &format!("{POSTGRES_URL}?application_name=gol_c2_isolation{serializable}"),
        postgres::NoTls,
    )
    .expect("session");
    let isolation: String = session
        .query_one("show default_transaction_isolation", &[])
        .expect("show")
        .get(0);
    assert_eq!(isolation, "serializable", "the option took effect");
    for extra in ["", serializable] {
        assert_eq!(
            forced_append_after_a_holder(extra, true),
            (Ok(Append::Terminal), 2),
            "terminal held{extra}"
        );
        assert_eq!(
            forced_append_after_a_holder(extra, false),
            (Ok(Append::Appended), 3),
            "message held{extra}"
        );
    }
}

// Connecting does not wait on a writer stalled mid-append: an existing schema
// takes no lock that an uncommitted insert into run_events holds up.
#[test]
fn a_connect_does_not_wait_on_a_stalled_append() {
    let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
    let spec = spec();
    store
        .put_run(StoredRun {
            spec: spec.clone(),
            events: vec![user_message(&spec, 1)],
        })
        .expect("put run");
    let mut stalled = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("stalled");
    let mut tx = stalled.transaction().expect("begin");
    tx.execute(
        "insert into run_events (run_id, seq, body, terminal) values ($1, 2, $2, false)",
        &[
            &spec.run_id.as_uuid(),
            &serde_json::to_value(user_message(&spec, 2)).unwrap(),
        ],
    )
    .expect("stalled insert");
    let started = std::time::Instant::now();
    let connected = std::thread::spawn(|| PostgresStore::connect(POSTGRES_URL).map(|_| ()));
    while !connected.is_finished() && started.elapsed() < std::time::Duration::from_secs(3) {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let finished = connected.is_finished();
    tx.rollback().expect("rollback");
    assert!(finished, "connect waited on the stalled insert");
    assert_eq!(connected.join().unwrap(), Ok(()));
}

#[test]
fn a_pool_of_no_connections_is_refused() {
    assert!(PostgresStore::connect_with_pool_size(POSTGRES_URL, 0).is_err());
}

// A connect that cannot reach its database says why, not only the kind of
// error.
#[test]
fn a_failed_connect_names_its_cause() {
    let url = POSTGRES_URL.replace("127.0.0.1/gol", "127.0.0.1/gol_no_such_database");
    assert_ne!(url, POSTGRES_URL);
    let error = PostgresStore::connect(&url)
        .map(|_| ())
        .expect_err("no database");
    assert!(error.to_string().contains("does not exist"), "{error}");
}

// A batch may hold one terminal event, in either store: the log ends at it.
#[test]
fn a_batch_with_two_terminal_events_is_refused_in_postgres() {
    let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
    let spec = spec();
    let two = vec![
        completed(&spec, 2, "a".to_string()),
        completed(&spec, 3, "b".to_string()),
    ];
    assert!(store
        .put_run(StoredRun {
            spec: spec.clone(),
            events: two.clone(),
        })
        .is_err());
    store
        .put_run(StoredRun {
            spec: spec.clone(),
            events: vec![user_message(&spec, 1)],
        })
        .expect("put run");
    assert!(store.append_events(spec.run_id, two).is_err());
    assert_eq!(reconnect(spec.run_id).events.len(), 1);
}

// The terminal index is created in the store's own schema, even when another
// schema on the search path has an index of the same name.
#[test]
fn the_terminal_index_is_created_in_the_store_schema() {
    let mut admin = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("connect");
    let (first, second) = (
        format!("c2_index_{}", std::process::id()),
        format!("c2_other_{}", std::process::id()),
    );
    admin
        .batch_execute(&format!(
            "drop schema if exists {first} cascade;
             drop schema if exists {second} cascade;
             create schema {first};
             create schema {second};
             create table {second}.other (id int);
             create index run_events_one_terminal on {second}.other (id);"
        ))
        .expect("schemas");
    let url = format!("{POSTGRES_URL}?options=-csearch_path%3D{first},{second}");
    let connected = PostgresStore::connect(&url).map(|_| ());
    let created: bool = admin
        .query_one(
            "select to_regclass($1) is not null",
            &[&format!("{first}.run_events_one_terminal")],
        )
        .expect("index")
        .get(0);
    admin
        .batch_execute(&format!(
            "drop schema {first} cascade; drop schema {second} cascade"
        ))
        .expect("drop");
    assert_eq!(connected, Ok(()));
    assert!(created, "no terminal index on {first}.run_events");
}

// A connect_timeout past what an Instant can add is still a store.
#[test]
fn a_huge_connect_timeout_is_not_a_panic() {
    let url = format!("{POSTGRES_URL}?connect_timeout=9223372036854775807");
    let store = PostgresStore::connect(&url).expect("connect");
    assert_eq!(store.run(RunId::new()).map(|run| run.is_none()), Ok(true));
}

const FD_CHILD: &str = "GOL_TEST_FD_EXHAUSTION_CHILD";

// A connect that panics on a pool worker (the postgres crate unwraps building
// its runtime, which fails when the process is out of file descriptors) does
// not cost the pool a connection for good. The scenario runs in a child
// process under `ulimit -n 256`, so only the child runs out of descriptors.
#[test]
fn a_connect_that_panics_does_not_shrink_the_pool() {
    if std::env::var_os(FD_CHILD).is_some() {
        return connect_panic_in_this_process();
    }
    let exe = std::env::current_exe().expect("test binary");
    let output = std::process::Command::new("sh")
        .arg("-c")
        .arg(r#"ulimit -n 256 && exec "$0" --exact "$1" --test-threads 1 --nocapture"#)
        .arg(exe)
        .arg("a_connect_that_panics_does_not_shrink_the_pool")
        .env(FD_CHILD, "1")
        .output()
        .expect("child");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("1 passed"),
        "child failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn connect_panic_in_this_process() {
    let application = format!("gol_c2_{}", uuid::Uuid::new_v4().simple());
    let url = format!("{POSTGRES_URL}?application_name={application}");
    let store = Arc::new(PostgresStore::connect_with_pool_size(&url, 2).expect("connect"));
    // One pooled connection stays busy: a put that waits on alice's row.
    let id = AgentId::new();
    let mut alice = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("connect");
    let mut tx = alice.transaction().expect("begin");
    tx.execute(
        "insert into agents (id, manifest, owner_issuer, owner_subject, owner_tenant)
         values ($1, '{}'::jsonb, 'https://issuer.test', 'alice', 'tenant-1')",
        &[&id.as_uuid()],
    )
    .expect("insert");
    let blocked = {
        let store = store.clone();
        std::thread::spawn(move || store.put_agent(agent_for(id, "bob", "tenant-1", "9")))
    };
    wait_for_lock_wait(&application);
    // Out of descriptors, the pool's connect for a second caller panics.
    // The child runs under `ulimit -n 256`: far fewer opens than this cap
    // exhaust it. Hitting the cap means the limit was not lowered, and the
    // test stops before it starves anything else.
    let mut hog = Vec::new();
    while let Ok(file) = std::fs::File::open("/dev/null") {
        hog.push(file);
        assert!(hog.len() < 4096, "descriptor limit not lowered");
    }
    let during = store.run(RunId::new());
    drop(hog);
    let error = during.expect_err("no connection could open");
    assert!(
        error.to_string().contains("postgres connect panicked"),
        "{error}"
    );
    // With descriptors free again, the second slot still opens.
    assert_eq!(
        store.run(RunId::new()).map(|run| run.is_none()),
        Ok(true),
        "the pool lost the slot of the panicked connect"
    );
    tx.rollback().expect("rollback");
    blocked.join().unwrap().expect("put");
}

// libpq reads connect_timeout=0 as "wait indefinitely": the store treats it
// as unset instead of handing r2d2 a zero wait, which it refuses by panicking.
#[test]
fn a_zero_connect_timeout_is_not_a_panic() {
    let url = format!("{POSTGRES_URL}?connect_timeout=0");
    let store = PostgresStore::connect(&url).expect("connect");
    assert_eq!(store.run(RunId::new()).map(|run| run.is_none()), Ok(true));
}

// A batch ends at its terminal event: one after it is refused, in Postgres.
#[test]
fn an_event_after_a_terminal_in_one_batch_is_refused_in_postgres() {
    let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
    let spec = spec();
    let batch = vec![
        completed(&spec, 2, "done".to_string()),
        user_message(&spec, 3),
    ];
    assert!(store
        .put_run(StoredRun {
            spec: spec.clone(),
            events: batch.clone(),
        })
        .is_err());
    store
        .put_run(StoredRun {
            spec: spec.clone(),
            events: vec![user_message(&spec, 1)],
        })
        .expect("put run");
    assert!(store.append_events(spec.run_id, batch).is_err());
    assert_eq!(reconnect(spec.run_id).events.len(), 1);
}

// The events route pages a Postgres log by count, as it does in memory.
#[tokio::test]
async fn the_events_route_pages_a_postgres_log() {
    let spec = spec();
    let run_id = spec.run_id;
    let events: Vec<Event> = (1..=5).map(|at| user_message(&spec, at)).collect();
    let expected = events.clone();
    let store = tokio::task::spawn_blocking(move || {
        let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
        store.put_run(StoredRun { spec, events }).expect("put run");
        store
    })
    .await
    .expect("connect thread");
    let app = server::router(
        Arc::new(store),
        "http://127.0.0.1:9",
        common::authenticator(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    let page = |query: &'static str| async move {
        reqwest::Client::new()
            .get(format!("http://{addr}/v1/runs/{run_id}/events{query}"))
            .header("authorization", common::bearer())
            .send()
            .await
            .expect("get")
            .json::<Vec<Event>>()
            .await
            .expect("events")
    };
    assert_eq!(page("?after=1&limit=2").await, expected[1..3]);
    assert_eq!(page("?after=3").await, expected[3..5]);
    assert_eq!(page("").await, expected);
}

fn env(pairs: &[(&str, &str)]) -> std::collections::BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

// GOL_DATABASE_URL puts the server's runs and memory in Postgres: what it
// stores, a fresh connection finds.
#[test]
fn the_database_url_selects_postgres() {
    use harness::{Memory, MemoryKey};
    let stores = server::stores_from_env(&env(&[
        ("GOL_DATABASE_URL", POSTGRES_URL),
        ("GOL_DATABASE_POOL_SIZE", "2"),
    ]))
    .expect("stores");
    let spec = spec();
    let run_id = spec.run_id;
    stores
        .runs
        .put_run(StoredRun {
            spec: spec.clone(),
            events: vec![user_message(&spec, 1)],
        })
        .expect("put run");
    assert_eq!(reconnect(run_id).events.len(), 1);
    let owner = MemoryKey {
        scope: protocol::MemoryScope::Run,
        owner_id: run_id.to_string(),
    };
    stores.memory.write(&owner, "topic", "kept").expect("write");
    let fresh = memory::PostgresMemory::connect(POSTGRES_URL).expect("connect");
    assert_eq!(fresh.read(&owner, "topic"), Ok(Some("kept".to_string())));
    // An empty pool size, like an empty URL, counts as unset.
    assert!(server::stores_from_env(&env(&[
        ("GOL_DATABASE_URL", POSTGRES_URL),
        ("GOL_DATABASE_POOL_SIZE", ""),
    ]))
    .is_ok());
}

// A pool size that is not a positive count, or a database the server cannot
// reach, refuses to start, saying why.
#[test]
fn a_bad_database_setting_refuses_to_start() {
    for size in ["0", "x", "-1"] {
        let error = server::stores_from_env(&env(&[
            ("GOL_DATABASE_URL", POSTGRES_URL),
            ("GOL_DATABASE_POOL_SIZE", size),
        ]))
        .map(|_| ())
        .expect_err(size);
        assert!(error.contains("GOL_DATABASE_POOL_SIZE"), "{error}");
    }
    let missing = POSTGRES_URL.replace("127.0.0.1/gol", "127.0.0.1/gol_no_such_database");
    let error = server::stores_from_env(&env(&[("GOL_DATABASE_URL", &missing)]))
        .map(|_| ())
        .expect_err("no database");
    assert!(error.contains("does not exist"), "{error}");
}

// Without GOL_DATABASE_URL, what the server stores stays in the process: a
// Postgres connection does not find it.
#[test]
fn no_database_url_keeps_runs_out_of_postgres() {
    let stores = server::stores_from_env(&env(&[("GOL_DATABASE_URL", "")])).expect("stores");
    let spec = spec();
    let run_id = spec.run_id;
    stores
        .runs
        .put_run(StoredRun {
            spec: spec.clone(),
            events: vec![user_message(&spec, 1)],
        })
        .expect("put run");
    assert!(stores.runs.run(run_id).expect("store").is_some());
    let postgres = PostgresStore::connect(POSTGRES_URL).expect("connect");
    assert!(postgres.run(run_id).expect("store").is_none());
}
