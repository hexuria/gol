//! The live stream (Phase 3.2, decisions 40A-45A): `GET /v1/stream` sends
//! the caller's outbox, and `GET /v1/runs/{id}/stream` one owned run, as
//! server-sent events that resume after `Last-Event-ID`. A hint wakes a
//! waiting stream to re-read; the hint carries no state, so a lost one costs
//! a read, not an event. On both stores where a store is involved; needs
//! Postgres, as `pg_redis.rs` does.
mod common;

use std::sync::Arc;
use std::time::Duration;

use protocol::{
    Actor, AgentId, CredentialSource, Event, EventPayload, EventSource, ExecutionPlacement, Limits,
    ModelProvider, Owner, RunId, RunSpec, Timestamp, WorkModel,
};
use server::{router, InMemoryStore, OutboxStore, PostgresStore, RunStore, StoredRun};

const POSTGRES_URL: &str = "postgres://gol:gol@127.0.0.1/gol";

/// A principal of its own, so other tests' events are not in its outbox.
fn fresh_user() -> String {
    format!("user-{}", RunId::new())
}

fn spec(user: &str) -> RunSpec {
    RunSpec::builder()
        .owner(Owner::new(common::ISSUER, user, "tenant-1"))
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

fn message(spec: &RunSpec, text: &str) -> Event {
    Event::record(
        EventSource::for_spec(spec, Actor::System, Timestamp::now()),
        EventPayload::UserMessage {
            text: text.to_string(),
        },
    )
}

fn messages(spec: &RunSpec, from: usize, count: usize) -> Vec<Event> {
    (from..from + count)
        .map(|n| message(spec, &n.to_string()))
        .collect()
}

type Store = Arc<dyn RunStore>;

/// A Postgres store kept for the whole binary: built on a plain thread (the
/// Postgres client refuses to start on a Tokio worker) and never dropped, so
/// no Tokio worker closes its connections either.
fn postgres(slot: &'static std::sync::OnceLock<Store>) -> Store {
    slot.get_or_init(|| {
        std::thread::spawn(|| {
            Arc::new(PostgresStore::connect(POSTGRES_URL).expect("connect")) as Store
        })
        .join()
        .expect("thread")
    })
    .clone()
}

static POSTGRES: std::sync::OnceLock<Store> = std::sync::OnceLock::new();
static OTHER_POSTGRES: std::sync::OnceLock<Store> = std::sync::OnceLock::new();

/// The memory store, then Postgres.
fn stores() -> Vec<Store> {
    vec![
        Arc::new(InMemoryStore::default()) as Store,
        postgres(&POSTGRES),
    ]
}

/// Runs a store call on a plain thread (Postgres blocks).
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(work).await.expect("blocking")
}

async fn put(store: &Store, spec: &RunSpec, events: Vec<Event>) {
    let (store, spec) = (store.clone(), spec.clone());
    blocking(move || {
        store.put_run(StoredRun { spec, events }).expect("put run");
    })
    .await;
}

async fn append(store: &Store, spec: &RunSpec, events: Vec<Event>) {
    let (store, id) = (store.clone(), spec.run_id);
    blocking(move || {
        store.append_events(id, events).expect("append");
    })
    .await;
}

async fn serve(store: Store) -> String {
    let app = router(store, "http://127.0.0.1:9", common::authenticator());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn open(base: &str, path: &str, user: &str, last: Option<u64>) -> reqwest::Response {
    let mut request = reqwest::Client::new()
        .get(format!("{base}{path}"))
        .header("authorization", common::bearer_for(user));
    if let Some(last) = last {
        request = request.header("last-event-id", last.to_string());
    }
    request.send().await.expect("send")
}

/// One server-sent event: its id, name and JSON data.
#[derive(Debug)]
struct Sse {
    id: Option<u64>,
    name: String,
    data: serde_json::Value,
}

/// Reads events off `response` until `count` have come, the stream ends, or
/// `wait` passes with nothing new; comments (heartbeats) are skipped.
struct Reader {
    response: reqwest::Response,
    buffer: String,
    ended: bool,
}

impl Reader {
    fn new(response: reqwest::Response) -> Self {
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.headers()["content-type"].to_str().unwrap(),
            "text/event-stream"
        );
        Self {
            response,
            buffer: String::new(),
            ended: false,
        }
    }

    async fn take(&mut self, count: usize, wait: Duration) -> Vec<Sse> {
        let mut events = Vec::new();
        while events.len() < count {
            while let Some(at) = self.buffer.find("\n\n") {
                let block: String = self.buffer.drain(..at + 2).collect();
                if let Some(event) = parse(&block) {
                    events.push(event);
                }
            }
            if events.len() >= count || self.ended {
                break;
            }
            match tokio::time::timeout(wait, self.response.chunk()).await {
                Ok(Ok(Some(bytes))) => self.buffer.push_str(&String::from_utf8_lossy(&bytes)),
                Ok(Ok(None)) => self.ended = true,
                Ok(Err(error)) => panic!("stream: {error}"),
                Err(_) => break,
            }
        }
        events
    }
}

fn parse(block: &str) -> Option<Sse> {
    let (mut id, mut name, mut data) = (None, "message".to_string(), String::new());
    for line in block.lines() {
        if let Some(value) = line.strip_prefix("id:") {
            id = Some(value.trim().parse().expect("numeric id"));
        } else if let Some(value) = line.strip_prefix("event:") {
            name = value.trim().to_string();
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push_str(value.trim());
        }
    }
    if data.is_empty() {
        return None;
    }
    Some(Sse {
        id,
        name,
        data: serde_json::from_str(&data).expect("json data"),
    })
}

fn ids(events: &[Sse]) -> Vec<u64> {
    events.iter().map(|event| event.id.expect("id")).collect()
}

// A client that saw up to number 1 gets the rest of what is stored, then
// what is stored after it opened, in order, each with its run, place and
// event; the event's kind names it.
#[tokio::test(flavor = "multi_thread")]
async fn the_stream_resumes_after_last_event_id() {
    for store in stores() {
        let base = serve(store.clone()).await;
        let user = fresh_user();
        let spec = spec(&user);
        put(&store, &spec, messages(&spec, 1, 3)).await;
        let mut stream = Reader::new(open(&base, "/v1/stream", &user, Some(1)).await);
        let first = stream.take(2, Duration::from_secs(5)).await;
        assert_eq!(ids(&first), [2, 3]);
        assert_eq!(first[0].name, "message.user");
        assert_eq!(first[0].data["run_id"], spec.run_id.to_string());
        assert_eq!(first[0].data["run_seq"], 2);
        assert_eq!(
            first[0].data["event"]["payload"]["UserMessage"]["text"],
            "2"
        );

        append(&store, &spec, messages(&spec, 4, 2)).await;
        let later = stream.take(2, Duration::from_secs(5)).await;
        assert_eq!(ids(&later), [4, 5]);
        assert_eq!(
            later[1].data["event"]["payload"]["UserMessage"]["text"],
            "5"
        );
    }
}

// Another principal's work never shows, and another owner's run is not
// found.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_shows_only_the_callers_work() {
    for store in stores() {
        let base = serve(store.clone()).await;
        let (alice, bob) = (fresh_user(), fresh_user());
        let hers = spec(&alice);
        let his = spec(&bob);
        put(&store, &hers, messages(&hers, 1, 2)).await;
        put(&store, &his, messages(&his, 1, 2)).await;
        let mut stream = Reader::new(open(&base, "/v1/stream?after=0", &alice, None).await);
        let mut events = stream.take(2, Duration::from_secs(5)).await;
        events.extend(stream.take(1, Duration::from_millis(500)).await);
        assert_eq!(ids(&events), [1, 2]);
        assert!(events
            .iter()
            .all(|event| event.data["run_id"] == hers.run_id.to_string()));
        let path = format!("/v1/runs/{}/stream", his.run_id);
        assert_eq!(open(&base, &path, &alice, None).await.status(), 404);
    }
}

// More than two pages are waiting: the stream reads on after a full page
// rather than waiting for a hint (none comes), well inside the 5 s poll.
// Then more arrives while the client reads: every number comes, once, in
// order.
#[tokio::test(flavor = "multi_thread")]
async fn a_lagging_stream_catches_up_from_the_outbox() {
    for store in stores() {
        let base = serve(store.clone()).await;
        let user = fresh_user();
        let spec = spec(&user);
        put(&store, &spec, messages(&spec, 1, 1200)).await;
        let mut stream = Reader::new(open(&base, "/v1/stream", &user, Some(0)).await);
        let backlog = stream.take(1200, Duration::from_secs(2)).await;
        assert_eq!(ids(&backlog), (1..=1200).collect::<Vec<u64>>());
        let writer = {
            let (store, spec) = (store.clone(), spec.clone());
            tokio::spawn(async move {
                for n in 0..20 {
                    append(&store, &spec, messages(&spec, 1201 + n * 2, 2)).await;
                }
            })
        };
        let later = stream.take(40, Duration::from_secs(10)).await;
        writer.await.expect("writer");
        assert_eq!(ids(&later), (1201..=1240).collect::<Vec<u64>>());
    }
}

// Decision 41A: a client whose cursor is below what was pruned is told to
// reset, with the number to reload from, and the stream ends.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_cursor_gets_a_reset() {
    let memory = Arc::new(InMemoryStore::default());
    let base = serve(memory.clone()).await;
    let user = fresh_user();
    let spec = spec(&user);
    put(&(memory.clone() as Store), &spec, messages(&spec, 1, 3)).await;
    let cutoff = Timestamp::unix_millis(Timestamp::now().as_unix_millis() + 1);
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(memory.prune_outbox(cutoff), Ok(3));
    let mut stream = Reader::new(open(&base, "/v1/stream", &user, Some(1)).await);
    let events = stream.take(2, Duration::from_secs(5)).await;
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0].name, "reset");
    assert_eq!(events[0].data["pruned_through"], 3);
    // Its id is where to resume once reloaded: a browser's EventSource
    // reconnects from it rather than from the stale number.
    assert_eq!(events[0].id, Some(3));
    assert!(stream.ended, "the stream ends after the reset");
    let mut resumed = Reader::new(open(&base, "/v1/stream", &user, Some(3)).await);
    append(&(memory.clone() as Store), &spec, messages(&spec, 4, 1)).await;
    assert_eq!(ids(&resumed.take(1, Duration::from_secs(5)).await), [4]);
}

// The same on Postgres. The pruned state is made by hand for this principal
// alone, so no store-wide prune reaches another test's rows.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_cursor_gets_a_reset_on_postgres() {
    let store = postgres(&POSTGRES);
    let base = serve(store.clone()).await;
    let user = fresh_user();
    let spec = spec(&user);
    put(&store, &spec, messages(&spec, 1, 3)).await;
    let owner = spec.owner.clone();
    blocking(move || {
        let mut admin = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("admin");
        admin
            .execute(
                "delete from outbox where owner_issuer = $1 and owner_subject = $2 and seq <= 2",
                &[&owner.issuer, &owner.subject],
            )
            .expect("prune by hand");
        admin
            .execute(
                "update outbox_counters set pruned = 2
                 where owner_issuer = $1 and owner_subject = $2",
                &[&owner.issuer, &owner.subject],
            )
            .expect("mark pruned");
    })
    .await;
    let mut stream = Reader::new(open(&base, "/v1/stream", &user, Some(1)).await);
    let events = stream.take(2, Duration::from_secs(5)).await;
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0].name, "reset");
    assert_eq!(events[0].id, Some(2));
    assert_eq!(events[0].data["pruned_through"], 2);
    assert!(stream.ended);
}

// A client with no cursor starts at the end: it gets what is stored from
// now on, not the retained history, which `?after=0` asks for.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_without_a_cursor_starts_at_the_end() {
    for store in stores() {
        let base = serve(store.clone()).await;
        let user = fresh_user();
        let spec = spec(&user);
        put(&store, &spec, messages(&spec, 1, 3)).await;
        let mut stream = Reader::new(open(&base, "/v1/stream", &user, None).await);
        assert_eq!(
            ids(&stream.take(1, Duration::from_millis(500)).await),
            Vec::<u64>::new()
        );
        append(&store, &spec, messages(&spec, 4, 1)).await;
        assert_eq!(ids(&stream.take(1, Duration::from_secs(5)).await), [4]);
        let mut history = Reader::new(open(&base, "/v1/stream?after=2", &user, None).await);
        assert_eq!(ids(&history.take(2, Duration::from_secs(5)).await), [3, 4]);
    }
}

// Decision 40A: a write through another connection (another server, here
// another store) reaches a waiting stream by LISTEN/NOTIFY, well inside the
// 5 s poll floor.
#[tokio::test(flavor = "multi_thread")]
async fn a_notify_wakes_a_waiting_stream() {
    let (serving, writing) = (postgres(&POSTGRES), postgres(&OTHER_POSTGRES));
    let base = serve(serving).await;
    let user = fresh_user();
    let spec = spec(&user);
    put(&writing, &spec, messages(&spec, 1, 1)).await;
    let mut stream = Reader::new(open(&base, "/v1/stream", &user, Some(0)).await);
    assert_eq!(ids(&stream.take(1, Duration::from_secs(5)).await), [1]);
    // A probe: once it arrives, the listener is up (its first wake-up comes
    // right after LISTEN), so the next write can only arrive by NOTIFY
    // inside the poll floor.
    append(&writing, &spec, messages(&spec, 2, 1)).await;
    assert_eq!(ids(&stream.take(1, Duration::from_secs(10)).await), [2]);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let wrote = std::time::Instant::now();
    append(&writing, &spec, messages(&spec, 3, 1)).await;
    let woken = stream.take(1, Duration::from_millis(1500)).await;
    assert_eq!(ids(&woken), [3]);
    assert!(
        wrote.elapsed() < Duration::from_secs(2),
        "{:?}",
        wrote.elapsed()
    );
}

// The run stream resumes after its place in the run, and ends after the
// run's terminal event.
#[tokio::test(flavor = "multi_thread")]
async fn the_run_stream_ends_after_the_terminal_event() {
    for store in stores() {
        let base = serve(store.clone()).await;
        let user = fresh_user();
        let spec = spec(&user);
        put(&store, &spec, messages(&spec, 1, 2)).await;
        let path = format!("/v1/runs/{}/stream", spec.run_id);
        let mut stream = Reader::new(open(&base, &path, &user, Some(1)).await);
        let first = stream.take(1, Duration::from_secs(5)).await;
        assert_eq!(ids(&first), [2]);
        assert_eq!(first[0].data["run_seq"], 2);
        let ended = Event::record(
            EventSource::for_spec(&spec, Actor::System, Timestamp::now()),
            EventPayload::RunCompleted {
                outcome: "done".to_string(),
            },
        );
        append(&store, &spec, vec![message(&spec, "3"), ended]).await;
        let rest = stream.take(3, Duration::from_secs(5)).await;
        assert_eq!(ids(&rest), [3, 4]);
        assert_eq!(rest[1].name, "run.completed");
        assert!(stream.ended, "the stream ends after the terminal event");
        // A browser reconnects from the terminal event: nothing more comes,
        // and 204 tells it to stop. A cursor past the log is refused.
        assert_eq!(open(&base, &path, &user, Some(4)).await.status(), 204);
        assert_eq!(open(&base, &path, &user, Some(9)).await.status(), 400);
    }
}

// Decision 45A: at most 16 open streams per principal; another principal
// is not held back by them.
#[tokio::test(flavor = "multi_thread")]
async fn a_seventeenth_stream_is_refused() {
    let store: Store = Arc::new(InMemoryStore::default());
    let base = serve(store).await;
    let user = fresh_user();
    let mut open_streams = Vec::new();
    for _ in 0..16 {
        let response = open(&base, "/v1/stream", &user, None).await;
        assert_eq!(response.status(), 200);
        open_streams.push(response);
    }
    assert_eq!(open(&base, "/v1/stream", &user, None).await.status(), 429);
    assert_eq!(
        open(&base, "/v1/stream", &fresh_user(), None)
            .await
            .status(),
        200
    );
    // A client that goes away gives its slot back.
    drop(open_streams.pop());
    let mut status = 0;
    for _ in 0..50 {
        status = open(&base, "/v1/stream", &user, None)
            .await
            .status()
            .as_u16();
        if status == 200 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(status, 200);
}
