use std::sync::Arc;

use protocol::{
    Actor, AgentId, ArtifactId, Capability, CredentialSource, DispatchPhase, Event, EventPayload,
    EventSource, ExecutionPlacement, FailureClass, HarnessState, Limits, MessageRole, ModelMessage,
    ModelProvider, RunId, RunSpec, Timestamp, WorkModel,
};
use server::{
    router_with_queue, AgentManifest, Append, PostgresStore, RedisRunQueue, RunStore,
    StoredArtifact, StoredRun,
};

const POSTGRES_URL: &str = "postgres://gol:gol@127.0.0.1/gol";
const REDIS_URL: &str = "redis://127.0.0.1/";

fn spec() -> RunSpec {
    RunSpec::builder()
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
        store.put_agent(AgentManifest {
            id: run.agent_id,
            version: "1".to_string(),
            instructions: "store".to_string(),
            tools: vec!["echo".to_string()],
            required_capabilities: vec![Capability::new("tool.echo")],
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
    let app = router_with_queue(Arc::new(store), jev.uri(), Some(REDIS_URL.to_string()));
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
        .header("authorization", "Bearer gol-gateway-local")
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
        .header("authorization", "Bearer gol-gateway-local")
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
            "capabilities": ["tool.echo"],
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
