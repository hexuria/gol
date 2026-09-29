//! Triggers (Phase 4.1, decisions 68A-72A): a principal's schedules (and,
//! later, webhooks) that start runs while they are away. A trigger is
//! created, listed, paused, resumed and deleted by its owner alone. A fire
//! starts an ordinary queued run of the trigger's agent with the trigger's
//! input, marked by the server (`gol.trigger`), in the trigger's own thread,
//! so it is a card on that board. A paused trigger does not fire, and the
//! owner's stop button pauses every trigger they have. On both stores;
//! needs Postgres and Redis, as `pg_redis.rs` does.
mod common;

use common::queued::{
    blocking, fresh_user, jev, serve, serve_with_webhooks, stores, Server, Store,
};
use protocol::{EventPayload, Owner, RunId};
use serde_json::{json, Value};
use server::{fire_trigger, Fired, RedisRunQueue, TriggerId};

fn schedule(agent: impl serde::Serialize) -> Value {
    json!({
        "agent_id": agent,
        "kind": {"schedule": {"cron": "0 9 * * 1-5", "time_zone": "Asia/Manila"}},
        "input": "summarize the inbox",
        "placement": "Local",
        "work_model": {"provider": "OpenAI", "model_name": "gpt-test",
            "credential": "PlatformGateway"},
    })
}

async fn create(server: &Server, user: &str, body: Value) -> (u16, Value) {
    server.post("/v1/triggers", user, body).await
}

async fn list(server: &Server, user: &str) -> Vec<Value> {
    let (status, body) = server.get("/v1/triggers", user).await;
    assert_eq!(status, 200, "{body}");
    body["triggers"].as_array().expect("triggers").clone()
}

async fn fire(server: &Server, user: &str, id: &str) -> Fired {
    let (store, url) = (server.store.clone(), server.redis.clone());
    let owner = Owner::new(common::ISSUER, user, "tenant-1");
    let id: TriggerId = id.parse().expect("trigger id");
    blocking(move || {
        fire_trigger(store.as_ref(), &RedisRunQueue::open(url), &owner, id).expect("fire")
    })
    .await
}

// A trigger is its owner's to create, list, pause, resume and delete.
#[tokio::test(flavor = "multi_thread")]
async fn a_trigger_is_created_listed_paused_resumed_and_deleted() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store.clone(), &jev, 1).await;
        let user = fresh_user();
        let agent = server.agent(&user, "digest", &[]).await;
        let (status, created) = create(&server, &user, schedule(agent)).await;
        assert_eq!(status, 201, "{created}");
        let id = created["id"].as_str().expect("id").to_string();
        assert_eq!(created["enabled"], true);
        assert_eq!(created["agent_id"], json!(agent));
        assert_eq!(
            created["kind"],
            json!({"schedule": {"cron": "0 9 * * 1-5", "time_zone": "Asia/Manila"}})
        );
        assert_eq!(created["missed"], "run_once_late", "70A's default");
        assert_eq!(created["thread_id"], format!("trigger-{id}"));
        assert_eq!(list(&server, &user).await, vec![created.clone()]);

        let (status, paused) = server
            .post(&format!("/v1/triggers/{id}/pause"), &user, json!({}))
            .await;
        assert_eq!(status, 200, "{paused}");
        assert_eq!(paused["enabled"], false);
        assert_eq!(list(&server, &user).await[0]["enabled"], false);
        let (status, resumed) = server
            .post(&format!("/v1/triggers/{id}/resume"), &user, json!({}))
            .await;
        assert_eq!(status, 200, "{resumed}");
        assert_eq!(resumed["enabled"], true);

        let (status, _) = server
            .send(
                reqwest::Method::DELETE,
                &format!("/v1/triggers/{id}"),
                &user,
                None,
            )
            .await;
        assert_eq!(status, 204);
        assert_eq!(list(&server, &user).await, Vec::<Value>::new());
        let (status, _) = server
            .post(&format!("/v1/triggers/{id}/pause"), &user, json!({}))
            .await;
        assert_eq!(status, 404, "deleted");
    }
}

// Another principal's trigger is not found, and they cannot trigger an agent
// that is not theirs.
#[tokio::test(flavor = "multi_thread")]
async fn another_principals_trigger_is_not_found() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store.clone(), &jev, 2).await;
        let (alice, bob) = (fresh_user(), fresh_user());
        let hers = server.agent(&alice, "digest", &[]).await;
        let (_, created) = create(&server, &alice, schedule(hers)).await;
        let id = created["id"].as_str().expect("id").to_string();
        assert_eq!(list(&server, &bob).await, Vec::<Value>::new());
        for action in ["pause", "resume"] {
            let (status, _) = server
                .post(&format!("/v1/triggers/{id}/{action}"), &bob, json!({}))
                .await;
            assert_eq!(status, 404, "{action}");
        }
        let (status, _) = server
            .send(
                reqwest::Method::DELETE,
                &format!("/v1/triggers/{id}"),
                &bob,
                None,
            )
            .await;
        assert_eq!(status, 404);
        let hers_now = list(&server, &alice).await;
        assert_eq!(hers_now.len(), 1, "still hers");
        assert_eq!(hers_now[0]["enabled"], true, "Bob's pause did nothing");
        let (status, body) = create(&server, &bob, schedule(hers)).await;
        assert_eq!(status, 404, "{body}");
        assert_eq!(body["error"], "agent not found");
        // Bob's fire of her trigger finds nothing.
        assert_eq!(fire(&server, &bob, &id).await, Fired::NotFound);
    }
}

// A fire starts an ordinary queued run of the trigger's agent with its
// input, marked `gol.trigger` by the server (68A), as a card on the
// trigger's own board; the worker runs it to its end.
#[tokio::test(flavor = "multi_thread")]
async fn a_fire_is_a_run_on_the_board() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store.clone(), &jev, 3).await;
        let user = fresh_user();
        let agent = server.agent(&user, "digest", &[]).await;
        let (_, created) = create(&server, &user, schedule(agent)).await;
        let id = created["id"].as_str().expect("id").to_string();
        // The version its owner keeps now, not the one kept at creation.
        assert!(server.put_version(&user, agent, "digest", "2").await);
        let Fired::Run(run) = fire(&server, &user, &id).await else {
            panic!("a run");
        };
        let runs: Store = store.clone();
        let stored = blocking(move || runs.run(run).expect("read").expect("stored")).await;
        assert_eq!(stored.spec.agent_id, agent);
        assert_eq!(stored.spec.agent_version, "2");
        assert_eq!(stored.spec.input, "summarize the inbox");
        assert_eq!(stored.spec.metadata.get("gol.trigger"), Some(&id));
        let (status, board) = server
            .get(&format!("/v1/threads/trigger-{id}/board"), &user)
            .await;
        assert_eq!(status, 200, "{board}");
        assert_eq!(board["cards"][0]["run_id"], json!(run));
        assert_eq!(board["cards"][0]["state"], "queued");
        // A follow-up in the trigger's thread is a run the caller asked for,
        // not refused for the server's marker.
        let (status, body) = server
            .post(
                &format!("/v1/threads/trigger-{id}/messages"),
                &user,
                json!({"input": "and the calendar"}),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(server.work().await, Some(run));
        assert!(stored_payloads(&store, run)
            .await
            .iter()
            .any(|payload| matches!(payload, EventPayload::RunCompleted { .. })));
    }
}

async fn stored_payloads(store: &Store, run: RunId) -> Vec<EventPayload> {
    let store = store.clone();
    blocking(move || {
        store
            .run(run)
            .expect("read")
            .expect("stored")
            .events
            .into_iter()
            .map(|event| event.payload)
            .collect()
    })
    .await
}

// A paused trigger does not fire, and a resumed one does again.
#[tokio::test(flavor = "multi_thread")]
async fn a_paused_trigger_does_not_fire() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store.clone(), &jev, 4).await;
        let user = fresh_user();
        let agent = server.agent(&user, "digest", &[]).await;
        let (_, created) = create(&server, &user, schedule(agent)).await;
        let id = created["id"].as_str().expect("id").to_string();
        server
            .post(&format!("/v1/triggers/{id}/pause"), &user, json!({}))
            .await;
        assert_eq!(fire(&server, &user, &id).await, Fired::Paused);
        assert_eq!(server.work().await, None, "nothing was queued");
        server
            .post(&format!("/v1/triggers/{id}/resume"), &user, json!({}))
            .await;
        assert!(matches!(fire(&server, &user, &id).await, Fired::Run(_)));
    }
}

// The owner's stop button pauses every trigger they have, and nobody
// else's; a thread's or a run's stop pauses none.
#[tokio::test(flavor = "multi_thread")]
async fn stop_everything_pauses_triggers() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store.clone(), &jev, 5).await;
        let (alice, bob) = (fresh_user(), fresh_user());
        let hers = server.agent(&alice, "digest", &[]).await;
        let his = server.agent(&bob, "digest", &[]).await;
        for _ in 0..2 {
            create(&server, &alice, schedule(hers)).await;
        }
        let (_, his_trigger) = create(&server, &bob, schedule(his)).await;
        // A run's stop pauses no trigger, even that run's.
        let first = list(&server, &alice).await[0]["id"]
            .as_str()
            .expect("id")
            .to_string();
        let Fired::Run(run) = fire(&server, &alice, &first).await else {
            panic!("a run");
        };
        let (status, _) = server
            .post(&format!("/v1/runs/{run}/stop"), &alice, json!({}))
            .await;
        assert_eq!(status, 200);
        assert!(list(&server, &alice)
            .await
            .iter()
            .all(|trigger| trigger["enabled"] == true));
        let (status, body) = server.post("/v1/stop", &alice, json!({})).await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["paused_triggers"], 2);
        assert!(list(&server, &alice)
            .await
            .iter()
            .all(|trigger| trigger["enabled"] == false));
        assert_eq!(list(&server, &bob).await, vec![his_trigger]);
    }
}

// What a trigger refuses: an unknown kind or an empty schedule, limits out
// of range, an unknown field, a NUL character, an input over 32 KiB; and
// the server's marker (`gol.trigger`) on a run a caller posts. (InMemoryStore:
// the checks come before the store.)
#[tokio::test(flavor = "multi_thread")]
async fn a_bad_trigger_is_refused() {
    let jev = jev(&["complete"]).await;
    let store: Store = stores().remove(0);
    let server = serve(store, &jev, 6).await;
    let user = fresh_user();
    let agent = server.agent(&user, "digest", &[]).await;
    let mut empty = schedule(agent);
    empty["kind"] = json!({"schedule": {"cron": " ", "time_zone": "Asia/Manila"}});
    let mut limits = schedule(agent);
    limits["limits"] = json!({"max_steps": 0, "max_model_calls": 1});
    let mut unknown = schedule(agent);
    unknown["priority"] = json!(1);
    let mut kind = schedule(agent);
    kind["kind"] = json!("hourly");
    for (name, body) in [
        ("empty schedule", empty),
        ("limits", limits),
        ("unknown field", unknown),
        ("kind", kind),
    ] {
        let (status, body) = create(&server, &user, body).await;
        assert_eq!(status, 400, "{name}: {body}");
    }
    let mut nul = schedule(agent);
    nul["input"] = json!("a\u{0}b");
    let (status, body) = create(&server, &user, nul).await;
    assert_eq!(status, 400, "{body}");
    let mut long = schedule(agent);
    long["input"] = json!("x".repeat(protocol::MAX_MESSAGE_BYTES + 1));
    let (status, body) = create(&server, &user, long).await;
    assert_eq!(status, 413, "{body}");
    let (status, body) = server
        .post(
            "/v1/runs",
            &user,
            json!({
                "agent_id": agent, "agent_version": "1", "input": "x", "placement": "Local",
                "work_model": {"provider": "OpenAI", "model_name": "gpt-test",
                    "credential": "PlatformGateway"},
                "metadata": {"gol.trigger": "1"},
            }),
        )
        .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["error"], "gol.trigger is set by the server");
}

const WEBHOOK_KEY: &[u8] = b"a server key for tests, 32 bytes or more";

/// What decision 73A derives for trigger `id` at `rotation`.
fn derived(id: &str, rotation: u32) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, WEBHOOK_KEY);
    let tag = ring::hmac::sign(&key, format!("gol-webhook-v1:{id}:{rotation}").as_bytes());
    tag.as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn webhook(agent: impl serde::Serialize) -> Value {
    let mut body = schedule(agent);
    body["kind"] = json!("webhook");
    body
}

// A webhook trigger's secret is derived from the server's key, the trigger
// and its rotation (73A): shown when it is made and when it is rotated, and
// never listed. A rotation gives a new secret; only a webhook trigger has
// one, and only its owner rotates it.
#[tokio::test(flavor = "multi_thread")]
async fn a_webhook_secret_is_shown_once_and_rotates() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve_with_webhooks(store.clone(), &jev, 7, Some(WEBHOOK_KEY.to_vec())).await;
        let (user, other) = (fresh_user(), fresh_user());
        let agent = server.agent(&user, "digest", &[]).await;
        let (status, created) = create(&server, &user, webhook(agent)).await;
        assert_eq!(status, 201, "{created}");
        let id = created["id"].as_str().expect("id").to_string();
        assert_eq!(created["kind"], json!({"webhook": {"rotation": 0}}));
        assert_eq!(created["secret"], derived(&id, 0));
        let listed = list(&server, &user).await;
        assert_eq!(listed.len(), 1);
        assert!(listed[0].get("secret").is_none(), "{}", listed[0]);
        for action in ["pause", "resume"] {
            let (_, answer) = server
                .post(&format!("/v1/triggers/{id}/{action}"), &user, json!({}))
                .await;
            assert!(answer.get("secret").is_none(), "{action}: {answer}");
        }

        let (status, rotated) = server
            .post(&format!("/v1/triggers/{id}/rotate"), &user, json!({}))
            .await;
        assert_eq!(status, 200, "{rotated}");
        assert_eq!(rotated["kind"], json!({"webhook": {"rotation": 1}}));
        assert_eq!(rotated["secret"], derived(&id, 1));
        assert_ne!(rotated["secret"], created["secret"]);

        let (status, _) = server
            .post(&format!("/v1/triggers/{id}/rotate"), &other, json!({}))
            .await;
        assert_eq!(status, 404, "not theirs");
        let (_, scheduled) = create(&server, &user, schedule(agent)).await;
        let schedule_id = scheduled["id"].as_str().expect("id");
        assert!(scheduled.get("secret").is_none());
        let (status, body) = server
            .post(
                &format!("/v1/triggers/{schedule_id}/rotate"),
                &user,
                json!({}),
            )
            .await;
        assert_eq!(status, 409, "{body}");
    }
}

// Without the server's webhook key there are no webhook triggers (73A).
#[tokio::test(flavor = "multi_thread")]
async fn webhooks_need_the_servers_key() {
    let jev = jev(&["complete"]).await;
    let server = serve(stores().remove(0), &jev, 8).await;
    let user = fresh_user();
    let agent = server.agent(&user, "digest", &[]).await;
    let (status, body) = create(&server, &user, webhook(agent)).await;
    assert_eq!(status, 503, "{body}");
    assert_eq!(body["error"], "webhooks are not available");
    let (_, created) = create(&server, &user, schedule(agent)).await;
    let id = created["id"].as_str().expect("id");
    let (status, _) = server
        .post(&format!("/v1/triggers/{id}/rotate"), &user, json!({}))
        .await;
    assert_eq!(status, 503);
}

// A fire checks the trigger's agent is still its owner's: a trigger that
// names another principal's agent (stored directly, as the API never
// would) starts nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_fire_of_another_principals_agent_starts_nothing() {
    for store in stores() {
        let jev = jev(&["complete"]).await;
        let server = serve(store.clone(), &jev, 9).await;
        let (alice, bob) = (fresh_user(), fresh_user());
        let his = server.agent(&bob, "digest", &[]).await;
        let id = TriggerId::new();
        let trigger = server::StoredTrigger {
            id,
            owner: Owner::new(common::ISSUER, alice.as_str(), "tenant-1"),
            agent_id: his,
            kind: server::TriggerKind::Schedule {
                cron: "0 9 * * *".to_string(),
                time_zone: "UTC".to_string(),
            },
            input: "x".to_string(),
            placement: protocol::ExecutionPlacement::Local,
            work_model: protocol::WorkModel {
                provider: protocol::ModelProvider::OpenAI,
                model_name: "gpt-test".to_string(),
                credential: protocol::CredentialSource::PlatformGateway,
            },
            limits: None,
            missed: server::Missed::RunOnceLate,
            enabled: true,
            next_fire_ms: None,
            created_ms: 1,
        };
        let runs = store.clone();
        blocking(move || {
            runs.triggers()
                .expect("triggers")
                .put_trigger(&trigger, server::MAX_TRIGGERS)
                .expect("put")
        })
        .await;
        assert_eq!(
            fire(&server, &alice, &id.to_string()).await,
            Fired::AgentNotFound
        );
        assert_eq!(server.work().await, None);
    }
}

/// What a `Forced` store does.
#[derive(Clone, Copy)]
enum Forced {
    /// Pausing the owner's triggers fails.
    FailPause,
    /// Right after a run is stored, the owner's triggers are paused: a stop
    /// landing between a fire's read of its trigger and its run.
    PauseAfterPut,
    /// Right after a run is stored, the owner's triggers are deleted.
    DeleteAfterPut,
}

/// An in-memory store with one forced behavior.
struct ForcedStore {
    inner: server::InMemoryStore,
    forced: Forced,
}

impl server::RunStore for ForcedStore {
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
    fn agents_of(&self, owner: &Owner) -> Result<Vec<server::StoredAgent>, server::StoreError> {
        self.inner.agents_of(owner)
    }
    fn put_run(&self, run: server::StoredRun) -> Result<server::PutRun, server::StoreError> {
        let owner = run.spec.owner.clone();
        let put = self.inner.put_run(run)?;
        use server::TriggerStore;
        match self.forced {
            Forced::PauseAfterPut => {
                self.inner.pause_triggers(&owner)?;
            }
            Forced::DeleteAfterPut => {
                for trigger in self.inner.triggers_of(&owner)? {
                    self.inner.delete_trigger(&owner, trigger.id)?;
                }
            }
            Forced::FailPause => {}
        }
        Ok(put)
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
        Some(self)
    }
}

impl server::TriggerStore for ForcedStore {
    fn put_trigger(
        &self,
        trigger: &server::StoredTrigger,
        most: usize,
    ) -> Result<bool, server::StoreError> {
        self.inner.put_trigger(trigger, most)
    }
    fn triggers_of(&self, owner: &Owner) -> Result<Vec<server::StoredTrigger>, server::StoreError> {
        self.inner.triggers_of(owner)
    }
    fn trigger(
        &self,
        owner: &Owner,
        id: TriggerId,
    ) -> Result<Option<server::StoredTrigger>, server::StoreError> {
        self.inner.trigger(owner, id)
    }
    fn set_enabled(
        &self,
        owner: &Owner,
        id: TriggerId,
        enabled: bool,
    ) -> Result<Option<server::StoredTrigger>, server::StoreError> {
        self.inner.set_enabled(owner, id, enabled)
    }
    fn rotate_webhook(
        &self,
        owner: &Owner,
        id: TriggerId,
    ) -> Result<Option<server::StoredTrigger>, server::StoreError> {
        self.inner.rotate_webhook(owner, id)
    }
    fn delete_trigger(&self, owner: &Owner, id: TriggerId) -> Result<bool, server::StoreError> {
        self.inner.delete_trigger(owner, id)
    }
    fn resume_trigger(
        &self,
        owner: &Owner,
        id: TriggerId,
        next_fire_ms: Option<i64>,
    ) -> Result<Option<server::StoredTrigger>, server::StoreError> {
        self.inner.resume_trigger(owner, id, next_fire_ms)
    }
    fn due_triggers(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> Result<Vec<server::StoredTrigger>, server::StoreError> {
        self.inner.due_triggers(now_ms, limit)
    }
    fn advance_trigger(
        &self,
        id: TriggerId,
        due_ms: i64,
        next_ms: i64,
    ) -> Result<bool, server::StoreError> {
        self.inner.advance_trigger(id, due_ms, next_ms)
    }
    fn pause_triggers(&self, owner: &Owner) -> Result<usize, server::StoreError> {
        match self.forced {
            Forced::FailPause => Err(server::StoreError::new("the store is unreachable")),
            Forced::PauseAfterPut | Forced::DeleteAfterPut => self.inner.pause_triggers(owner),
        }
    }
}

fn forced(forced: Forced) -> Store {
    std::sync::Arc::new(ForcedStore {
        inner: server::InMemoryStore::default(),
        forced,
    })
}

// A pause that fails does not keep the owner's stop from going on: their
// queued runs are still cancelled, and the stop answers 503 once done.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_pause_does_not_stop_the_stop() {
    let jev = jev(&["complete"]).await;
    let server = serve(forced(Forced::FailPause), &jev, 10).await;
    let user = fresh_user();
    let agent = server.agent(&user, "digest", &[]).await;
    let (_, run) = server.start(&user, agent, "queued work").await;
    let (status, body) = server.post("/v1/stop", &user, json!({})).await;
    assert_eq!(status, 503, "{body}");
    let payloads = stored_payloads(&server.store, run.parse().expect("id")).await;
    assert!(
        payloads.contains(&EventPayload::RunCancelled),
        "the stop went on"
    );
}

// A fire whose trigger is paused between its read and its run's store (as
// the owner's stop pauses before it records itself) holds that run: it is
// cancelled before any worker can see it and never pushed, so no run
// escapes the stop (a run stored after a stop is not covered by it).
#[tokio::test(flavor = "multi_thread")]
async fn a_fire_racing_a_pause_holds_its_run() {
    let jev = jev(&["complete"]).await;
    let server = serve(forced(Forced::PauseAfterPut), &jev, 11).await;
    let user = fresh_user();
    let agent = server.agent(&user, "digest", &[]).await;
    let (_, created) = create(&server, &user, schedule(agent)).await;
    let id = created["id"].as_str().expect("id").to_string();
    assert_eq!(fire(&server, &user, &id).await, Fired::Paused);
    assert_eq!(server.work().await, None, "never pushed");
    let (status, board) = server
        .get(&format!("/v1/threads/trigger-{id}/board"), &user)
        .await;
    assert_eq!(status, 200, "{board}");
    assert_eq!(board["cards"][0]["state"], "cancelled", "{board}");
    assert_eq!(board["cards"][0]["started_at"], Value::Null);
}

// A fire whose trigger is deleted between its read and its run's store holds
// its run the same way.
#[tokio::test(flavor = "multi_thread")]
async fn a_fire_racing_a_delete_holds_its_run() {
    let jev = jev(&["complete"]).await;
    let server = serve(forced(Forced::DeleteAfterPut), &jev, 14).await;
    let user = fresh_user();
    let agent = server.agent(&user, "digest", &[]).await;
    let (_, created) = create(&server, &user, schedule(agent)).await;
    let id = created["id"].as_str().expect("id").to_string();
    assert_eq!(fire(&server, &user, &id).await, Fired::NotFound);
    assert_eq!(server.work().await, None, "never pushed");
    let (_, board) = server
        .get(&format!("/v1/threads/trigger-{id}/board"), &user)
        .await;
    assert_eq!(board["cards"][0]["state"], "cancelled", "{board}");
}

// The cap and the insert are one step on each store: sixteen writers racing
// for the last five places store exactly five. (A race, not a forced order:
// each store holds one lock across the count and the insert, so there is no
// point between them to force.)
#[test]
fn racing_creates_keep_the_cap() {
    for store in stores() {
        let owner = Owner::new(common::ISSUER, fresh_user(), "tenant-1");
        let trigger = |n: i64| server::StoredTrigger {
            id: TriggerId::new(),
            owner: owner.clone(),
            agent_id: protocol::AgentId::new(),
            kind: server::TriggerKind::Schedule {
                cron: "0 9 * * *".to_string(),
                time_zone: "UTC".to_string(),
            },
            input: "x".to_string(),
            placement: protocol::ExecutionPlacement::Local,
            work_model: protocol::WorkModel {
                provider: protocol::ModelProvider::OpenAI,
                model_name: "gpt-test".to_string(),
                credential: protocol::CredentialSource::PlatformGateway,
            },
            limits: None,
            missed: server::Missed::RunOnceLate,
            enabled: true,
            next_fire_ms: None,
            created_ms: n,
        };
        let most = server::MAX_TRIGGERS;
        for n in 0..most - 5 {
            let stored = store
                .triggers()
                .expect("triggers")
                .put_trigger(&trigger(n as i64), most)
                .expect("put");
            assert!(stored);
        }
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(16));
        let racers: Vec<_> = (0..16)
            .map(|n| {
                let (store, barrier, trigger) = (store.clone(), barrier.clone(), trigger(1000 + n));
                std::thread::spawn(move || {
                    barrier.wait();
                    store
                        .triggers()
                        .expect("triggers")
                        .put_trigger(&trigger, most)
                        .expect("put")
                })
            })
            .collect();
        let stored = racers
            .into_iter()
            .map(|racer| racer.join().expect("racer"))
            .filter(|stored| *stored)
            .count();
        assert_eq!(stored, 5);
        let kept = store
            .triggers()
            .expect("triggers")
            .triggers_of(&owner)
            .expect("list");
        assert_eq!(kept.len(), most);
    }
}

// Where a trigger cannot be made: a server without the run queue (its fires
// have nowhere to go) is 503, and a webhook key shorter than 32 bytes is no
// key, so webhook triggers are 503 too. A principal keeps at most 100
// triggers. (InMemoryStore.)
#[tokio::test(flavor = "multi_thread")]
async fn triggers_are_refused_where_they_cannot_work() {
    let jev = jev(&["complete"]).await;
    let store: Store = std::sync::Arc::new(server::InMemoryStore::default());
    let app = server::router(store.clone(), jev.uri(), common::authenticator());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let user = fresh_user();
    let server = serve(store.clone(), &jev, 12).await;
    let agent = server.agent(&user, "digest", &[]).await;
    let response = reqwest::Client::new()
        .post(format!("http://{addr}/v1/triggers"))
        .header("authorization", common::bearer_for(&user))
        .json(&schedule(agent))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status().as_u16(), 503);

    let short = serve_with_webhooks(store.clone(), &jev, 13, Some(b"short".to_vec())).await;
    let (status, body) = create(&short, &user, webhook(agent)).await;
    assert_eq!(status, 503, "{body}");

    for _ in 0..server::MAX_TRIGGERS {
        let (status, _) = create(&server, &user, schedule(agent)).await;
        assert_eq!(status, 201);
    }
    let listed = list(&server, &user).await;
    assert_eq!(listed.len(), server::MAX_TRIGGERS);
    // Oldest first.
    let created: Vec<i64> = listed
        .iter()
        .map(|trigger| trigger["created_at"].as_i64().expect("created"))
        .collect();
    assert!(created.windows(2).all(|pair| pair[0] <= pair[1]));
    let (status, body) = create(&server, &user, schedule(agent)).await;
    assert_eq!(status, 409, "{body}");
}
