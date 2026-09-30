//! Webhooks (Phase 4.3, decisions 73A-81A): a webhook trigger fires on a
//! signed request to `POST /hooks/{id}`. The signature is HMAC-SHA256 of
//! "event.timestamp.body" under the trigger's secret (derived from the
//! server's key, 73A; the event id signed, 79A), sent as
//! `X-Gol-Signature: v1=<hex>` with `X-Gol-Timestamp` (Unix seconds, within
//! 5 minutes) and `X-Gol-Event` (the sender's event id). A fire's run id
//! comes from the trigger and the event id, so a replay fires nothing new,
//! and is answered from the event's run (80A). On both stores; needs
//! Postgres and Redis, as `pg_redis.rs` does.
mod common;

use common::queued::{blocking, fresh_user, jev, serve_with_webhooks, stores, Server, Store};
use protocol::{EventPayload, RunId};
use serde_json::{json, Value};
use server::RedisRunQueue;

const KEY: &[u8] = b"a server key for webhook tests, 32 bytes or more";

fn webhook(agent: impl serde::Serialize) -> Value {
    json!({
        "agent_id": agent,
        "kind": "webhook",
        "input": "triage the incoming issue",
        "placement": "Local",
        "work_model": {"provider": "OpenAI", "model_name": "gpt-test",
            "credential": "PlatformGateway"},
    })
}

/// A server, a user, and their webhook trigger: (server, user, id, secret).
async fn setup(store: &Store, db: u8) -> (Server, String, String, String) {
    let jev = jev(&["complete"]).await;
    let server = serve_with_webhooks(store.clone(), &jev, db, Some(KEY.to_vec())).await;
    std::mem::forget(jev);
    let user = fresh_user();
    let agent = server.agent(&user, "triager", &[]).await;
    let (status, created) = server.post("/v1/triggers", &user, webhook(agent)).await;
    assert_eq!(status, 201, "{created}");
    (
        server,
        user,
        created["id"].as_str().expect("id").to_string(),
        created["secret"].as_str().expect("secret").to_string(),
    )
}

fn now_s() -> i64 {
    protocol::Timestamp::now().as_unix_millis() / 1000
}

/// The `X-Gol-Signature` of event `event`'s `body` at `timestamp` under
/// `secret`.
fn sign(secret: &str, event: &str, timestamp: i64, body: &[u8]) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    let mut message = format!("{event}.{timestamp}.").into_bytes();
    message.extend_from_slice(body);
    let tag = ring::hmac::sign(&key, &message);
    let hex: String = tag
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("v1={hex}")
}

/// Posts `body` to trigger `id`'s hook with the given headers (any left out
/// when `None`).
async fn hook(
    server: &Server,
    id: &str,
    body: &[u8],
    event: Option<&str>,
    timestamp: Option<i64>,
    signature: Option<String>,
) -> (u16, Value) {
    let mut request = reqwest::Client::new()
        .post(format!("{}/hooks/{id}", server.base))
        .body(body.to_vec());
    if let Some(event) = event {
        request = request.header("x-gol-event", event);
    }
    if let Some(timestamp) = timestamp {
        request = request.header("x-gol-timestamp", timestamp.to_string());
    }
    if let Some(signature) = signature {
        request = request.header("x-gol-signature", signature);
    }
    let response = request.send().await.expect("send");
    let status = response.status().as_u16();
    (status, response.json().await.unwrap_or(Value::Null))
}

/// A correctly signed post of `body` as event `event`, now.
async fn signed(server: &Server, id: &str, secret: &str, event: &str, body: &[u8]) -> (u16, Value) {
    let at = now_s();
    hook(
        server,
        id,
        body,
        Some(event),
        Some(at),
        Some(sign(secret, event, at, body)),
    )
    .await
}

async fn run_of(store: &Store, run: &str) -> server::StoredRun {
    let (store, run): (Store, RunId) = (store.clone(), run.parse().expect("id"));
    blocking(move || store.run(run).expect("read").expect("stored")).await
}

// A signed request starts a task: 202 at once, an ordinary queued run of the
// trigger's agent whose input is the trigger's input, a blank line and the
// body; the worker runs it to its end.
#[tokio::test(flavor = "multi_thread")]
async fn a_signed_webhook_starts_a_task() {
    for store in stores() {
        let (server, _, id, secret) = setup(&store, 1).await;
        let body = br#"{"issue": 42}"#;
        let (status, answer) = signed(&server, &id, &secret, "evt-1", body).await;
        assert_eq!(status, 202, "{answer}");
        let run = answer["run_id"].as_str().expect("run").to_string();
        let stored = run_of(&store, &run).await;
        assert_eq!(
            stored.spec.input,
            "triage the incoming issue\n\n{\"issue\": 42}"
        );
        assert_eq!(stored.spec.metadata.get("gol.trigger"), Some(&id));
        assert_eq!(
            stored.spec.metadata.get("gol.event").map(String::as_str),
            Some("evt-1")
        );
        assert_eq!(
            server.work().await.map(|run| run.to_string()),
            Some(run.clone())
        );
        assert!(run_of(&store, &run)
            .await
            .events
            .iter()
            .any(|event| matches!(event.payload, EventPayload::RunCompleted { .. })));
    }
}

// A replay (the same event id, signed again) fires nothing new: 200 with the
// run the first fired, and the run is queued once.
#[tokio::test(flavor = "multi_thread")]
async fn a_replayed_event_is_dropped() {
    for store in stores() {
        let (server, _, id, secret) = setup(&store, 2).await;
        let (status, first) = signed(&server, &id, &secret, "evt-1", b"{}").await;
        assert_eq!(status, 202, "{first}");
        let (status, again) = signed(&server, &id, &secret, "evt-1", b"{}").await;
        assert_eq!(status, 200, "{again}");
        assert_eq!(again["run_id"], first["run_id"]);
        assert_eq!(again["duplicate"], true);
        let url = server.redis.clone();
        let queued = blocking(move || RedisRunQueue::open(url).queued().expect("queued")).await;
        let run: RunId = first["run_id"].as_str().expect("run").parse().expect("id");
        assert_eq!(queued.iter().filter(|queued| **queued == run).count(), 1);
        // Another event is another run.
        let (status, other) = signed(&server, &id, &secret, "evt-2", b"{}").await;
        assert_eq!(status, 202, "{other}");
        assert_ne!(other["run_id"], first["run_id"]);
    }
}

/// The runs on this server's queue.
async fn queued(server: &Server) -> Vec<RunId> {
    let url = server.redis.clone();
    blocking(move || RedisRunQueue::open(url).queued().expect("queued")).await
}

async fn post(server: &Server, user: &str, path: &str) {
    let (status, answer) = server.post(path, user, json!({})).await;
    assert_eq!(status, 200, "{path}: {answer}");
}

// A replay is answered from the event's run (80A), never fired anew: one
// that ran is a duplicate even while paused; one stored but never pushed
// (its delivery failed between the store and the push) is pushed by the
// replay, or held if the trigger was paused since, and a held run is
// refused again after a resume. A delivery refused while paused stores no
// run, so its retry after a resume fires.
#[tokio::test(flavor = "multi_thread")]
async fn a_replay_is_answered_from_the_events_run() {
    for store in stores() {
        let (server, user, id, secret) = setup(&store, 5).await;
        let (pause, resume) = (
            format!("/v1/triggers/{id}/pause"),
            format!("/v1/triggers/{id}/resume"),
        );
        // Refused while paused, nothing stored: retried after a resume, it
        // fires.
        post(&server, &user, &pause).await;
        let (status, answer) = signed(&server, &id, &secret, "retried", b"{}").await;
        assert_eq!(status, 409, "{answer}");
        assert_eq!(queued(&server).await, []);
        post(&server, &user, &resume).await;
        let (status, answer) = signed(&server, &id, &secret, "retried", b"{}").await;
        assert_eq!(status, 202, "{answer}");
        assert!(server.work().await.is_some());
        // Ran, then replayed while paused: a duplicate.
        let (status, ran) = signed(&server, &id, &secret, "ran", b"{}").await;
        assert_eq!(status, 202, "{ran}");
        assert!(server.work().await.is_some());
        post(&server, &user, &pause).await;
        let (status, again) = signed(&server, &id, &secret, "ran", b"{}").await;
        assert_eq!(
            (status, &again["duplicate"]),
            (200, &json!(true)),
            "{again}"
        );
        assert_eq!(again["run_id"], ran["run_id"]);
        post(&server, &user, &resume).await;
        // Stored, its push lost: the replay pushes it, once.
        let (status, lost) = signed(&server, &id, &secret, "lost", b"{}").await;
        assert_eq!(status, 202, "{lost}");
        let run: RunId = lost["run_id"].as_str().expect("run").parse().expect("id");
        let url = server.redis.clone();
        let popped = blocking(move || RedisRunQueue::open(url).pop().expect("pop")).await;
        assert_eq!(popped, Some(run));
        let (status, again) = signed(&server, &id, &secret, "lost", b"{}").await;
        assert_eq!(status, 202, "{again}");
        assert_eq!(again["run_id"], lost["run_id"]);
        assert_eq!(queued(&server).await, [run]);
        let (status, again) = signed(&server, &id, &secret, "lost", b"{}").await;
        assert_eq!(
            (status, &again["duplicate"]),
            (200, &json!(true)),
            "{again}"
        );
        assert_eq!(queued(&server).await, [run]);
        assert_eq!(server.work().await, Some(run));
        // Stored, its push lost, then paused: the replay holds it.
        let (status, held) = signed(&server, &id, &secret, "held", b"{}").await;
        assert_eq!(status, 202, "{held}");
        let url = server.redis.clone();
        blocking(move || RedisRunQueue::open(url).pop().expect("pop")).await;
        post(&server, &user, &pause).await;
        let (status, answer) = signed(&server, &id, &secret, "held", b"{}").await;
        assert_eq!(status, 409, "{answer}");
        let run = held["run_id"].as_str().expect("run");
        assert!(run_of(&store, run)
            .await
            .events
            .iter()
            .any(|event| matches!(event.payload, EventPayload::RunCancelled)));
        assert_eq!(queued(&server).await, []);
        // Held, then replayed after a resume: refused again, never run.
        post(&server, &user, &resume).await;
        let (status, answer) = signed(&server, &id, &secret, "held", b"{}").await;
        assert_eq!(status, 409, "{answer}");
        assert_eq!(queued(&server).await, []);
    }
}

/// An in-memory store whose next read of a run, once armed, misses it: as
/// when another delivery of the same event stores its run between this
/// delivery's read and its own store.
struct MissNextRead {
    inner: server::InMemoryStore,
    /// Armed by the test thread before a request, taken by the request's
    /// one read; the swap needs no order with other memory.
    armed: std::sync::atomic::AtomicBool,
}

impl MissNextRead {
    fn arm(&self) {
        self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

impl server::RunStore for MissNextRead {
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
    fn agents_of(
        &self,
        owner: &protocol::Owner,
    ) -> Result<Vec<server::StoredAgent>, server::StoreError> {
        self.inner.agents_of(owner)
    }
    fn put_run(&self, run: server::StoredRun) -> Result<server::PutRun, server::StoreError> {
        self.inner.put_run(run)
    }
    fn append_events(
        &self,
        id: RunId,
        events: Vec<protocol::Event>,
    ) -> Result<server::Append, server::StoreError> {
        self.inner.append_events(id, events)
    }
    fn append_events_after(
        &self,
        id: RunId,
        seen: usize,
        events: Vec<protocol::Event>,
    ) -> Result<server::Append, server::StoreError> {
        self.inner.append_events_after(id, seen, events)
    }
    fn run(&self, id: RunId) -> Result<Option<server::StoredRun>, server::StoreError> {
        if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
            return Ok(None);
        }
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
    fn threads(&self) -> Option<&dyn server::ThreadStore> {
        self.inner.threads()
    }
    fn stops(&self) -> Option<&dyn server::StopStore> {
        self.inner.stops()
    }
    fn triggers(&self) -> Option<&dyn server::TriggerStore> {
        self.inner.triggers()
    }
}

// Overlapping deliveries of one event (80A): one that reads no run, then
// finds another delivery's run stored before its own, is answered from
// that run's log, as a replay is: held is 409, done is 200 duplicate, and
// still queued is 202, queued once.
#[tokio::test(flavor = "multi_thread")]
async fn an_overlapping_delivery_is_answered_from_the_runs_log() {
    let miss = std::sync::Arc::new(MissNextRead {
        inner: server::InMemoryStore::default(),
        armed: std::sync::atomic::AtomicBool::new(false),
    });
    let store: Store = miss.clone();
    let (server, user, id, secret) = setup(&store, 6).await;
    let (pause, resume) = (
        format!("/v1/triggers/{id}/pause"),
        format!("/v1/triggers/{id}/resume"),
    );
    // Held by the other delivery.
    let (status, held) = signed(&server, &id, &secret, "held", b"{}").await;
    assert_eq!(status, 202, "{held}");
    let url = server.redis.clone();
    blocking(move || RedisRunQueue::open(url).pop().expect("pop")).await;
    post(&server, &user, &pause).await;
    let (status, answer) = signed(&server, &id, &secret, "held", b"{}").await;
    assert_eq!(status, 409, "{answer}");
    post(&server, &user, &resume).await;
    miss.arm();
    let (status, answer) = signed(&server, &id, &secret, "held", b"{}").await;
    assert_eq!(status, 409, "{answer}");
    // Done by the other delivery.
    let (status, done) = signed(&server, &id, &secret, "done", b"{}").await;
    assert_eq!(status, 202, "{done}");
    assert!(server.work().await.is_some());
    miss.arm();
    let (status, answer) = signed(&server, &id, &secret, "done", b"{}").await;
    assert_eq!(
        (status, &answer["duplicate"]),
        (200, &json!(true)),
        "{answer}"
    );
    assert_eq!(answer["run_id"], done["run_id"]);
    // Still queued by the other delivery.
    let (status, waiting) = signed(&server, &id, &secret, "waiting", b"{}").await;
    assert_eq!(status, 202, "{waiting}");
    miss.arm();
    let (status, answer) = signed(&server, &id, &secret, "waiting", b"{}").await;
    assert_eq!(status, 202, "{answer}");
    assert_eq!(answer["run_id"], waiting["run_id"]);
    let run: RunId = waiting["run_id"]
        .as_str()
        .expect("run")
        .parse()
        .expect("id");
    assert_eq!(queued(&server).await, [run]);
}

// What is refused, and fires nothing: 401 for a tampered body, a wrong or
// missing signature, one for another event (79A), a timestamp over 5
// minutes off, a missing event id or one that is not 1 to 128 visible
// ASCII characters other than a dot, an unknown trigger or one that is not
// a webhook (so ids cannot be probed); 409 for a paused trigger; 413 for a
// body that makes the run's input over 32 KiB (81A); 400 for one that is
// not UTF-8 or has a NUL. A rotated secret stops the old one, and a
// captured request cannot be split again as another event.
#[tokio::test(flavor = "multi_thread")]
async fn bad_requests_are_refused() {
    for store in stores() {
        let (server, user, id, secret) = setup(&store, 3).await;
        let at = now_s();
        let body = b"{}";
        let good = sign(&secret, "e", at, body);
        let cases: Vec<(&str, u16, (u16, Value))> = vec![
            (
                "tampered",
                401,
                hook(
                    &server,
                    &id,
                    b"{ }",
                    Some("e"),
                    Some(at),
                    Some(good.clone()),
                )
                .await,
            ),
            (
                "wrong key",
                401,
                hook(
                    &server,
                    &id,
                    body,
                    Some("e"),
                    Some(at),
                    Some(sign("wrong", "e", at, body)),
                )
                .await,
            ),
            (
                "no signature",
                401,
                hook(&server, &id, body, Some("e"), Some(at), None).await,
            ),
            (
                "garbled",
                401,
                hook(
                    &server,
                    &id,
                    body,
                    Some("e"),
                    Some(at),
                    Some("v1=zz".to_string()),
                )
                .await,
            ),
            (
                "stale",
                401,
                hook(
                    &server,
                    &id,
                    body,
                    Some("e"),
                    Some(at - 400),
                    Some(sign(&secret, "e", at - 400, body)),
                )
                .await,
            ),
            (
                "future",
                401,
                hook(
                    &server,
                    &id,
                    body,
                    Some("e"),
                    Some(at + 400),
                    Some(sign(&secret, "e", at + 400, body)),
                )
                .await,
            ),
            (
                "no timestamp",
                401,
                hook(&server, &id, body, Some("e"), None, Some(good.clone())).await,
            ),
            (
                "no event",
                401,
                hook(&server, &id, body, None, Some(at), Some(good.clone())).await,
            ),
            (
                "event id with a space",
                401,
                hook(
                    &server,
                    &id,
                    body,
                    Some("evt 1"),
                    Some(at),
                    Some(sign(&secret, "evt 1", at, body)),
                )
                .await,
            ),
            (
                "event id too long",
                401,
                hook(
                    &server,
                    &id,
                    body,
                    Some(&"e".repeat(129)),
                    Some(at),
                    Some(sign(&secret, &"e".repeat(129), at, body)),
                )
                .await,
            ),
            (
                "unknown",
                401,
                hook(
                    &server,
                    &server::TriggerId::new().to_string(),
                    body,
                    Some("e"),
                    Some(at),
                    Some(good.clone()),
                )
                .await,
            ),
            ("too long", 413, {
                let long = vec![b'x'; protocol::MAX_MESSAGE_BYTES + 1];
                hook(
                    &server,
                    &id,
                    &long,
                    Some("e"),
                    Some(at),
                    Some(sign(&secret, "e", at, &long)),
                )
                .await
            }),
            // 81A: the body after the trigger's input and a blank line.
            ("too long with the input", 413, {
                let input = "triage the incoming issue".len();
                let long = vec![b'x'; protocol::MAX_MESSAGE_BYTES - input - 1];
                hook(
                    &server,
                    &id,
                    &long,
                    Some("e"),
                    Some(at),
                    Some(sign(&secret, "e", at, &long)),
                )
                .await
            }),
            ("a NUL", 400, {
                let bytes = b"{\"a\": \"\0\"}";
                hook(
                    &server,
                    &id,
                    bytes,
                    Some("e"),
                    Some(at),
                    Some(sign(&secret, "e", at, bytes)),
                )
                .await
            }),
            // 79A: a captured request cannot be sent as another event.
            (
                "signed for another event",
                401,
                hook(&server, &id, body, Some("f"), Some(at), Some(good.clone())).await,
            ),
            ("not utf-8", 400, {
                let bytes = [0xff, 0xfe];
                hook(
                    &server,
                    &id,
                    &bytes,
                    Some("e"),
                    Some(at),
                    Some(sign(&secret, "e", at, &bytes)),
                )
                .await
            }),
        ];
        for (name, expected, (status, answer)) in cases {
            assert_eq!(status, expected, "{name}: {answer}");
        }
        // None of them fired: this server's queue has nothing.
        assert_eq!(queued(&server).await, []);
        // A schedule trigger has no hook.
        let agent = server.agent(&user, "digest", &[]).await;
        let (_, scheduled) = server
            .post(
                "/v1/triggers",
                &user,
                json!({"agent_id": agent, "kind": {"schedule": {"cron": "0 9 * * *", "time_zone": "UTC"}},
                    "input": "x", "placement": "Local",
                    "work_model": {"provider": "OpenAI", "model_name": "gpt-test", "credential": "PlatformGateway"}}),
            )
            .await;
        let schedule_id = scheduled["id"].as_str().expect("id");
        let (status, _) = hook(
            &server,
            schedule_id,
            body,
            Some("e"),
            Some(at),
            Some(good.clone()),
        )
        .await;
        assert_eq!(status, 401);
        // Paused: 409; resumed, it fires.
        server
            .post(&format!("/v1/triggers/{id}/pause"), &user, json!({}))
            .await;
        let (status, answer) = signed(&server, &id, &secret, "evt-p", body).await;
        assert_eq!(status, 409, "{answer}");
        server
            .post(&format!("/v1/triggers/{id}/resume"), &user, json!({}))
            .await;
        // Rotated: the old secret is refused, the new one fires.
        let (_, rotated) = server
            .post(&format!("/v1/triggers/{id}/rotate"), &user, json!({}))
            .await;
        let fresh = rotated["secret"].as_str().expect("secret").to_string();
        let (status, _) = signed(&server, &id, &secret, "evt-r", body).await;
        assert_eq!(status, 401, "the old secret");
        let (status, answer) = signed(&server, &id, &fresh, "evt-r", body).await;
        assert_eq!(status, 202, "{answer}");
        assert!(server.work().await.is_some());
        // 79A: a captured request whose body begins with a fresh timestamp
        // and a dot has the same signed bytes as another split, with the
        // first timestamp in the event id; that split is refused, since an
        // event id has no dot.
        let (at, later) = (now_s(), now_s() + 100);
        let captured = format!("{later}.payload").into_bytes();
        let signature = sign(&fresh, "evt", at, &captured);
        let (status, answer) = hook(
            &server,
            &id,
            &captured,
            Some("evt"),
            Some(at),
            Some(signature.clone()),
        )
        .await;
        assert_eq!(status, 202, "{answer}");
        let (status, answer) = hook(
            &server,
            &id,
            b"payload",
            Some(&format!("evt.{at}")),
            Some(later),
            Some(signature),
        )
        .await;
        assert_eq!(status, 401, "{answer}");
    }
}

/// Schedule `id` of `user` as a scheduler pass reads it.
async fn read(server: &Server, user: &str, id: &str) -> server::StoredTrigger {
    let store = server.store.clone();
    let owner = protocol::Owner::new(common::ISSUER, user.to_string(), "tenant-1");
    let id: server::TriggerId = id.parse().expect("id");
    blocking(move || {
        store
            .triggers()
            .expect("triggers")
            .trigger(&owner, id)
            .expect("read")
            .expect("stored")
    })
    .await
}

/// A scheduler's step over `trigger`, as its pass read it, at `now`: fired
/// for its tick and moved on. The run it fired, if any. (A whole pass fires
/// every principal's due triggers, a batch at a time, on the shared
/// Postgres store; its batching is `scheduler.rs`'s to test.)
async fn step(server: &Server, trigger: &server::StoredTrigger, now: i64) -> Option<RunId> {
    let (store, url, trigger) = (server.store.clone(), server.redis.clone(), trigger.clone());
    blocking(move || {
        server::fire_due_trigger(store.as_ref(), &RedisRunQueue::open(url), &trigger, now)
            .expect("fire")
    })
    .await
}

// The Case 8 exit gate (78A), on both stores: with two schedulers up, a
// schedule's tick is one task; a signed webhook starts a task and its
// replay is dropped; the owner's stop pauses both triggers, after which
// neither fires.
#[tokio::test(flavor = "multi_thread")]
async fn case_8_schedules_and_webhooks_end_to_end() {
    for store in stores() {
        let (server, user, hook_id, secret) = setup(&store, 4).await;
        let agent = server.agent(&user, "digest", &[]).await;
        let (status, scheduled) = server
            .post(
                "/v1/triggers",
                &user,
                json!({"agent_id": agent, "kind": {"schedule": {"cron": "* * * * *", "time_zone": "UTC"}},
                    "input": "summarize the inbox", "placement": "Local",
                    "work_model": {"provider": "OpenAI", "model_name": "gpt-test", "credential": "PlatformGateway"}}),
            )
            .await;
        assert_eq!(status, 201, "{scheduled}");
        let schedule_id = scheduled["id"].as_str().expect("id").to_string();
        let tick = scheduled["next_fire_at"].as_i64().expect("next tick");
        // Two schedulers read it due, just after the tick, and take it at
        // once: one task.
        let due = read(&server, &user, &schedule_id).await;
        assert_eq!(due.next_fire_ms, Some(tick));
        let (one, two) = tokio::join!(
            step(&server, &due, tick + 1_000),
            step(&server, &due, tick + 1_000)
        );
        let mut runs: Vec<RunId> = one.into_iter().chain(two).collect();
        runs.dedup();
        assert_eq!(runs.len(), 1, "one task per tick");
        let url = server.redis.clone();
        let queued = blocking(move || RedisRunQueue::open(url).queued().expect("queued")).await;
        assert_eq!(
            queued.iter().filter(|queued| **queued == runs[0]).count(),
            1
        );
        // A signed webhook starts a task; its replay is dropped.
        let (status, first) = signed(&server, &hook_id, &secret, "case-8", b"{}").await;
        assert_eq!(status, 202, "{first}");
        let (status, again) = signed(&server, &hook_id, &secret, "case-8", b"{}").await;
        assert_eq!((status, &again["run_id"]), (200, &first["run_id"]));
        // The owner's stop pauses both.
        let (status, stopped) = server.post("/v1/stop", &user, json!({})).await;
        assert_eq!(status, 200, "{stopped}");
        assert_eq!(stopped["paused_triggers"], 2);
        let (_, listed) = server.get("/v1/triggers", &user).await;
        let enabled: Vec<bool> = listed["triggers"]
            .as_array()
            .expect("triggers")
            .iter()
            .map(|trigger| trigger["enabled"].as_bool().expect("enabled"))
            .collect();
        assert_eq!(enabled, [false, false]);
        let (status, answer) = signed(&server, &hook_id, &secret, "case-8b", b"{}").await;
        assert_eq!(status, 409, "{answer}");
        let later = tick + 10 * 60_000;
        let paused = read(&server, &user, &schedule_id).await;
        assert_eq!(step(&server, &paused, later).await, None);
    }
}
