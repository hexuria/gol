use std::sync::Arc;

use protocol::{
    Actor, AgentId, ArtifactId, Capability, CredentialSource, DispatchPhase, Event, EventPayload,
    ExecutionPlacement, HarnessState, Limits, ModelProvider, RunId, RunSpec, Timestamp, WorkModel,
};
use server::{
    router_with_queue, AgentManifest, PostgresStore, RedisRunQueue, RunStore, StoredArtifact,
    StoredRun,
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
