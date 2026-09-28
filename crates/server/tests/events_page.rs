//! `GET /v1/runs/{id}/events?after=&limit=` pages the log: `after` is how
//! many events the caller has seen, and a page holds at most 500 events
//! (owner decision 4A for C2).
mod common;

use std::sync::Arc;

use protocol::{
    Actor, AgentId, CredentialSource, Event, EventPayload, EventSource, ExecutionPlacement, Limits,
    ModelProvider, RunId, RunSpec, Timestamp, WorkModel,
};
use server::{router, InMemoryStore, RunStore, StoredRun};

fn spec(subject: &str) -> RunSpec {
    RunSpec::builder()
        .owner(protocol::Owner::new(common::ISSUER, subject, "tenant-1"))
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

/// `count` user messages, numbered from 1 in their text.
fn messages(spec: &RunSpec, count: usize) -> Vec<Event> {
    (1..=count)
        .map(|n| {
            Event::record(
                EventSource::new(
                    spec.run_id,
                    spec.agent_id,
                    &spec.agent_version,
                    Actor::System,
                    Timestamp::unix_millis(n as i64),
                ),
                EventPayload::UserMessage {
                    text: n.to_string(),
                },
            )
        })
        .collect()
}

fn texts(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .map(|event| match &event.payload {
            EventPayload::UserMessage { text } => text.clone(),
            other => panic!("unexpected {other:?}"),
        })
        .collect()
}

fn stored(count: usize) -> (InMemoryStore, RunSpec) {
    let store = InMemoryStore::default();
    let spec = spec("user-1");
    store
        .put_run(StoredRun {
            spec: spec.clone(),
            events: messages(&spec, count),
        })
        .unwrap();
    (store, spec)
}

#[test]
fn run_page_returns_the_events_after_a_count() {
    let (store, spec) = stored(5);
    let page = |after, limit| {
        texts(
            &store
                .run_page(spec.run_id, after, limit)
                .unwrap()
                .unwrap()
                .events,
        )
    };
    assert_eq!(page(0, 2), ["1", "2"]);
    assert_eq!(page(2, 2), ["3", "4"]);
    assert_eq!(page(4, 2), ["5"]);
    assert_eq!(page(5, 2), Vec::<String>::new());
    assert_eq!(page(9, 2), Vec::<String>::new());
    assert!(store.run_page(RunId::new(), 0, 2).unwrap().is_none());
}

async fn serve(store: InMemoryStore) -> String {
    let app = router(
        Arc::new(store),
        "http://127.0.0.1:9",
        common::authenticator(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn get(url: String, subject: &str) -> (u16, serde_json::Value) {
    let response = reqwest::Client::new()
        .get(url)
        .header("authorization", common::bearer_for(subject))
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = response.text().await.unwrap();
    (
        status,
        serde_json::from_str(&body).unwrap_or(serde_json::Value::String(body)),
    )
}

fn page_texts(body: &serde_json::Value) -> Vec<String> {
    texts(&serde_json::from_value::<Vec<Event>>(body.clone()).unwrap())
}

#[tokio::test]
async fn events_paginate_after_seq() {
    let (store, spec) = stored(5);
    let base = serve(store).await;
    let url = |query: &str| format!("{base}/v1/runs/{}/events{query}", spec.run_id);

    let (status, body) = get(url("?after=1&limit=2"), "user-1").await;
    assert_eq!(
        (status, page_texts(&body)),
        (200, vec!["2".into(), "3".into()])
    );
    let (status, body) = get(url("?after=3"), "user-1").await;
    assert_eq!(
        (status, page_texts(&body)),
        (200, vec!["4".into(), "5".into()])
    );
    let (status, body) = get(url(""), "user-1").await;
    assert_eq!((status, page_texts(&body).len()), (200, 5));
    let (status, body) = get(url("?after=5&limit=500"), "user-1").await;
    assert_eq!((status, page_texts(&body).len()), (200, 0));

    for bad in ["?limit=0", "?limit=501", "?after=-1", "?limit=x"] {
        assert_eq!(get(url(bad), "user-1").await.0, 400, "{bad}");
    }
    assert_eq!(get(url("?after=0&limit=2"), "bob").await.0, 404);
}

#[tokio::test]
async fn a_page_holds_at_most_500_events_by_default() {
    let (store, spec) = stored(501);
    let base = serve(store).await;
    let url = format!("{base}/v1/runs/{}/events", spec.run_id);
    let (status, body) = get(url.clone(), "user-1").await;
    let first = page_texts(&body);
    assert_eq!((status, first.len()), (200, 500));
    assert_eq!(first.last().map(String::as_str), Some("500"));
    let (_, body) = get(format!("{url}?after=500"), "user-1").await;
    assert_eq!(page_texts(&body), ["501"]);
}
