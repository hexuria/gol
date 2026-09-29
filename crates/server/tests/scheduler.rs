//! The scheduler (Phase 4.2, decisions 69A, 70A, 72A). A schedule trigger
//! carries the time of its next tick; each pass fires the triggers that are
//! due and moves each to its next tick with a conditional update (69A), so
//! two schedulers fire a tick once. A fire's run id comes from the trigger
//! and the tick, so a tick fired twice (two schedulers, or one that died
//! between the fire and the update) is one run. Ticks missed while no server
//! ran follow the trigger's rule (70A). On both stores; needs Postgres and
//! Redis, as `pg_redis.rs` does.
mod common;

use common::queued::{blocking, fresh_user, jev, serve, stores, Server, Store};
use protocol::{Owner, RunId};
use serde_json::{json, Value};
use server::{
    fire_due_trigger, next_tick, schedule_due, Missed, RedisRunQueue, StoredTrigger, TriggerId,
    TriggerKind,
};

const HOUR: i64 = 3_600_000;

fn trigger(
    owner: &Owner,
    agent: protocol::AgentId,
    cron: &str,
    next: i64,
    missed: Missed,
) -> StoredTrigger {
    StoredTrigger {
        id: TriggerId::new(),
        owner: owner.clone(),
        agent_id: agent,
        kind: TriggerKind::Schedule {
            cron: cron.to_string(),
            time_zone: "UTC".to_string(),
        },
        input: "summarize the inbox".to_string(),
        placement: protocol::ExecutionPlacement::Local,
        work_model: protocol::WorkModel {
            provider: protocol::ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: protocol::CredentialSource::PlatformGateway,
        },
        limits: None,
        missed,
        enabled: true,
        next_fire_ms: Some(next),
        created_ms: 1,
    }
}

/// A server, a user and one of their agents.
async fn setup(store: &Store, db: u8) -> (Server, String, Owner, protocol::AgentId) {
    let jev = jev(&["complete"]).await;
    let server = serve(store.clone(), &jev, db).await;
    std::mem::forget(jev);
    let user = fresh_user();
    let agent = server.agent(&user, "digest", &[]).await;
    let owner = Owner::new(common::ISSUER, user.clone(), "tenant-1");
    (server, user, owner, agent)
}

async fn put(store: &Store, trigger: &StoredTrigger) {
    let (store, trigger) = (store.clone(), trigger.clone());
    blocking(move || {
        assert!(store
            .triggers()
            .expect("triggers")
            .put_trigger(&trigger, server::MAX_TRIGGERS)
            .expect("put"));
    })
    .await;
}

async fn stored(store: &Store, owner: &Owner, id: TriggerId) -> StoredTrigger {
    let (store, owner) = (store.clone(), owner.clone());
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

/// A scheduler pass at `now`, and the runs it fired for `own` triggers. A
/// pass fires every principal's due triggers, and the Postgres store is
/// shared, so a test counts only its own.
async fn pass(server: &Server, now: i64, own: &[TriggerId]) -> Vec<RunId> {
    let (store, url) = (server.store.clone(), server.redis.clone());
    let own: Vec<String> = own.iter().map(TriggerId::to_string).collect();
    blocking(move || {
        schedule_due(store.as_ref(), &RedisRunQueue::open(url), now)
            .expect("pass")
            .into_iter()
            .filter(|run| {
                let spec = store.run(*run).expect("read").expect("stored").spec;
                spec.metadata
                    .get(server::TRIGGER_KEY)
                    .is_some_and(|id| own.contains(id))
            })
            .collect()
    })
    .await
}

/// This binary's tests take turns: a pass in one would move another's
/// triggers on the shared Postgres store.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The runs a trigger's board shows.
async fn fired(server: &Server, user: &str, id: TriggerId) -> Vec<Value> {
    let (status, board) = server
        .get(&format!("/v1/threads/trigger-{id}/board"), user)
        .await;
    if status == 404 {
        return Vec::new();
    }
    assert_eq!(status, 200, "{board}");
    board["cards"].as_array().expect("cards").clone()
}

// A schedule's next tick is set when it is made, from its cron expression
// in its time zone; one that is not a cron expression, or names no known
// time zone, is refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_schedule_gets_its_next_tick_when_made() {
    let _turn = SERIAL.lock().await;
    let store = stores().remove(0);
    let (server, user, _, agent) = setup(&store, 1).await;
    let body = |cron: &str, zone: &str| {
        json!({
            "agent_id": agent,
            "kind": {"schedule": {"cron": cron, "time_zone": zone}},
            "input": "x", "placement": "Local",
            "work_model": {"provider": "OpenAI", "model_name": "gpt-test",
                "credential": "PlatformGateway"},
        })
    };
    let before = protocol::Timestamp::now().as_unix_millis();
    let (status, created) = server
        .post("/v1/triggers", &user, body("* * * * *", "UTC"))
        .await;
    assert_eq!(status, 201, "{created}");
    let next = created["next_fire_at"].as_i64().expect("next");
    assert_eq!(next % 60_000, 0, "on a minute");
    assert!(
        next > before && next <= before + 60_000,
        "{next} after {before}"
    );
    for (cron, zone) in [("every morning", "UTC"), ("0 9 * * *", "Mars/Olympus")] {
        let (status, body) = server.post("/v1/triggers", &user, body(cron, zone)).await;
        assert_eq!(status, 400, "{cron} {zone}: {body}");
        assert_eq!(
            body["error"],
            "a schedule needs a cron expression and a known time zone"
        );
    }
}

// A due trigger fires once for its tick and moves to its next tick; the
// next pass at the same time fires nothing. The run's id comes from the
// trigger and the tick.
#[tokio::test(flavor = "multi_thread")]
async fn a_due_trigger_fires_once_per_tick() {
    let _turn = SERIAL.lock().await;
    for store in stores() {
        let (server, user, owner, agent) = setup(&store, 2).await;
        let now = 10 * HOUR + 10_000;
        let due = trigger(&owner, agent, "0 * * * *", 10 * HOUR, Missed::RunOnceLate);
        put(&store, &due).await;
        let first = pass(&server, now, &[due.id]).await;
        assert_eq!(first.len(), 1);
        assert_eq!(pass(&server, now, &[due.id]).await, Vec::<RunId>::new());
        assert_eq!(
            stored(&store, &owner, due.id).await.next_fire_ms,
            Some(11 * HOUR)
        );
        assert_eq!(fired(&server, &user, due.id).await.len(), 1);
        let url = server.redis.clone();
        let queued = blocking(move || RedisRunQueue::open(url).queued().expect("queued")).await;
        assert_eq!(
            queued.iter().filter(|queued| **queued == first[0]).count(),
            1
        );
    }
}

// Two schedulers on one tick (forced: one read the due triggers, then the
// other fired and moved them, then the first went on with what it read):
// one run, and the stale scheduler's update does not move the next tick
// again.
#[tokio::test(flavor = "multi_thread")]
async fn one_fire_per_tick_with_two_schedulers() {
    let _turn = SERIAL.lock().await;
    for store in stores() {
        let (server, user, owner, agent) = setup(&store, 3).await;
        let now = 10 * HOUR + 10_000;
        let due = trigger(&owner, agent, "0 * * * *", 10 * HOUR, Missed::RunOnceLate);
        put(&store, &due).await;
        let stale = stored(&store, &owner, due.id).await;
        let other = pass(&server, now, &[due.id]).await;
        assert_eq!(other.len(), 1);
        // The other scheduler goes on to the next tick too.
        let later = pass(&server, 11 * HOUR + 10_000, &[due.id]).await;
        assert_eq!(later.len(), 1);
        let (runs, url) = (store.clone(), server.redis.clone());
        let late = blocking(move || {
            fire_due_trigger(runs.as_ref(), &RedisRunQueue::open(url), &stale, now).expect("fire")
        })
        .await;
        assert_eq!(late, Some(other[0]), "the same tick is the same run");
        assert_eq!(
            fired(&server, &user, due.id).await.len(),
            2,
            "one run per tick"
        );
        assert_eq!(
            stored(&store, &owner, due.id).await.next_fire_ms,
            Some(12 * HOUR),
            "the stale scheduler does not move it back"
        );
        // Each run is on the queue once. (A pass fires every principal's
        // due triggers onto this queue, so only these two are counted.)
        let url = server.redis.clone();
        let queued = blocking(move || RedisRunQueue::open(url).queued().expect("queued")).await;
        for run in [other[0], later[0]] {
            assert_eq!(queued.iter().filter(|queued| **queued == run).count(), 1);
        }
    }
}

// Ticks missed while no server ran (70A): run_once_late fires once, for the
// first tick it missed, and skip fires none; both move to the first tick
// after now.
#[tokio::test(flavor = "multi_thread")]
async fn missed_ticks_follow_their_rule() {
    let _turn = SERIAL.lock().await;
    for store in stores() {
        let (server, user, owner, agent) = setup(&store, 4).await;
        let now = 13 * HOUR + 10_000;
        let late = trigger(&owner, agent, "0 * * * *", 10 * HOUR, Missed::RunOnceLate);
        let skips = trigger(&owner, agent, "0 * * * *", 10 * HOUR, Missed::Skip);
        put(&store, &late).await;
        put(&store, &skips).await;
        let runs = pass(&server, now, &[late.id, skips.id]).await;
        assert_eq!(runs.len(), 1, "one late run for three missed ticks");
        assert_eq!(fired(&server, &user, late.id).await.len(), 1);
        assert_eq!(fired(&server, &user, skips.id).await.len(), 0);
        for id in [late.id, skips.id] {
            assert_eq!(
                stored(&store, &owner, id).await.next_fire_ms,
                Some(14 * HOUR)
            );
        }
    }
}

// A paused trigger is not fired, however due; resumed, it moves to the
// first tick after now, and owes nothing for the ticks it was paused.
#[tokio::test(flavor = "multi_thread")]
async fn a_paused_schedule_is_not_owed_its_ticks() {
    let _turn = SERIAL.lock().await;
    for store in stores() {
        let (server, user, owner, agent) = setup(&store, 5).await;
        let due = trigger(&owner, agent, "* * * * *", 60_000, Missed::RunOnceLate);
        put(&store, &due).await;
        let (status, _) = server
            .post(&format!("/v1/triggers/{}/pause", due.id), &user, json!({}))
            .await;
        assert_eq!(status, 200);
        assert_eq!(
            pass(&server, 10 * HOUR, &[due.id]).await,
            Vec::<RunId>::new()
        );
        assert_eq!(
            stored(&store, &owner, due.id).await.next_fire_ms,
            Some(60_000),
            "a paused trigger is not due: the scheduler leaves it alone"
        );
        let before = protocol::Timestamp::now().as_unix_millis();
        let (status, resumed) = server
            .post(&format!("/v1/triggers/{}/resume", due.id), &user, json!({}))
            .await;
        assert_eq!(status, 200, "{resumed}");
        let next = resumed["next_fire_at"].as_i64().expect("next");
        assert!(
            next > before,
            "the first tick after now, not the one it missed"
        );
        assert_eq!(pass(&server, before, &[due.id]).await, Vec::<RunId>::new());
    }
}

// Daylight saving time, by the trigger's time zone. On the day New York's
// clocks go back, 01:30 happens twice and a daily 01:30 fires once; on the
// day they go forward, 02:30 does not happen and the job runs at its next
// occurrence.
#[test]
fn ticks_follow_the_time_zone_through_daylight_saving() {
    // 2026-11-01 01:30 EDT is 05:30 UTC; the next 01:30 is 2026-11-02 EST.
    let first = next_tick("30 1 * * *", "America/New_York", 1_793_511_000_000 - 1).expect("tick");
    assert_eq!(first, 1_793_511_000_000, "2026-11-01T05:30Z");
    let second = next_tick("30 1 * * *", "America/New_York", first).expect("tick");
    assert_eq!(
        second, 1_793_601_000_000,
        "2026-11-02T06:30Z, not the repeated 01:30"
    );
    // 2026-03-08 02:30 does not exist in New York.
    let gap = next_tick("30 2 * * *", "America/New_York", 1_772_946_000_000).expect("tick");
    assert!(gap > 1_772_946_000_000);
    assert!(next_tick("0 9 * * *", "Mars/Olympus", 0).is_err());
    assert!(next_tick("every morning", "UTC", 0).is_err());
}
