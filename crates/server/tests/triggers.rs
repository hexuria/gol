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
        assert_eq!(list(&server, &alice).await.len(), 1, "still hers");
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
        let Fired::Run(run) = fire(&server, &user, &id).await else {
            panic!("a run");
        };
        let runs: Store = store.clone();
        let stored = blocking(move || runs.run(run).expect("read").expect("stored")).await;
        assert_eq!(stored.spec.agent_id, agent);
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
// of range, an unknown field; and the server's marker (`gol.trigger`) on a
// run a caller posts.
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
        assert!(status == 400 || status == 422, "{name}: {status} {body}");
    }
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

const WEBHOOK_KEY: &[u8] = b"a server key for tests";

/// What decision 73A derives for trigger `id` at `rotation`.
fn derived(id: &str, rotation: u32) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, WEBHOOK_KEY);
    let tag = ring::hmac::sign(&key, format!("{id}:{rotation}").as_bytes());
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
                .put_trigger(&trigger)
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
