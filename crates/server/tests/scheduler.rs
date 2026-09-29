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
    fire_due_trigger, fire_trigger_at, next_tick, schedule_due, Fired, Missed, RedisRunQueue,
    StoredTrigger, TriggerId, TriggerKind,
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
        generation: 0,
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
    let after = protocol::Timestamp::now().as_unix_millis();
    let next = created["next_fire_at"].as_i64().expect("next");
    assert_eq!(next % 60_000, 0, "on a minute");
    assert!(
        next > before && next <= after + 60_000,
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
        forget(&store, &owner, &[due.id]).await;
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
        assert_eq!(late, None, "a moved tick is not fired again");
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
        forget(&store, &owner, &[late.id, skips.id]).await;
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

// Daylight saving time, by the trigger's time zone, with exact times:
// - a wall-clock time the clocks skip runs when they resume (New York's
//   02:30 on 2026-03-08 runs at 03:00 EDT), and a daily time outside the gap
//   runs once that day (London's midnight on 2026-03-29, New York's 01:30);
// - a wall-clock time the clocks repeat runs at its first occurrence only
//   (New York's 01:30 on 2026-11-01, even for `30 1,2 * * *`).
// A schedule has minutes and no seconds or years.
#[test]
fn ticks_follow_the_time_zone_through_daylight_saving() {
    let tick = |cron: &str, zone: &str, after: i64| next_tick(cron, zone, after).expect("tick");
    // Spring forward.
    assert_eq!(
        tick("0 0 * * *", "Europe/London", 1_774_742_400_000),
        1_774_825_200_000
    );
    assert_eq!(
        tick("30 1 * * *", "America/New_York", 1_772_951_400_000),
        1_773_034_200_000
    );
    assert_eq!(
        tick("30 2 * * *", "America/New_York", 1_772_946_000_000),
        1_772_953_200_000
    );
    // A list or a step inside the gap runs once, when the clocks resume, and
    // the next day as usual (New York 02:00-03:00; Lord Howe 02:00-02:30).
    for cron in ["0,30 2 * * *", "*/30 2 * * *"] {
        assert_eq!(
            tick(cron, "America/New_York", 1_772_946_000_000),
            1_772_953_200_000,
            "{cron}"
        );
        assert_eq!(
            tick(cron, "America/New_York", 1_772_953_200_000),
            1_773_036_000_000,
            "{cron}"
        );
    }
    assert_eq!(
        tick("0,15 2 * * *", "Australia/Lord_Howe", 1_791_034_200_000),
        1_791_041_400_000
    );
    assert_eq!(
        tick("0,15 2 * * *", "Australia/Lord_Howe", 1_791_041_400_000),
        1_791_126_000_000
    );
    // Fall back: 01:30 EDT (05:30Z) runs, 01:30 EST (06:30Z) does not.
    assert_eq!(
        tick("30 1 * * *", "America/New_York", 1_793_511_000_000 - 1),
        1_793_511_000_000
    );
    assert_eq!(
        tick("30 1 * * *", "America/New_York", 1_793_511_000_000),
        1_793_601_000_000
    );
    assert_eq!(
        tick("30 1,2 * * *", "America/New_York", 1_793_511_000_000),
        1_793_518_200_000
    );
    for bad in [
        "every morning",
        "0 0 9 * * *",
        "0 9 1 1 * 2027",
        "0 0 9 1 1 * 2027",
    ] {
        assert!(next_tick(bad, "UTC", 0).is_err(), "{bad}");
    }
    assert!(next_tick("0 9 * * *", "Mars/Olympus", 0).is_err());
}

/// Deletes `ids`, so no later pass on the shared Postgres fires them.
async fn forget(store: &Store, owner: &Owner, ids: &[TriggerId]) {
    let (store, owner, ids) = (store.clone(), owner.clone(), ids.to_vec());
    blocking(move || {
        for id in ids {
            store
                .triggers()
                .expect("triggers")
                .delete_trigger(&owner, id)
                .expect("delete");
        }
    })
    .await;
}

// A tick fired twice (a scheduler that died between its fire and moving the
// trigger on, then the next pass) is one run, pushed once: the second fire
// finds it queued and leaves it, and the queue sweep does not push it again.
#[tokio::test(flavor = "multi_thread")]
async fn a_tick_fired_twice_is_one_run_pushed_once() {
    let _turn = SERIAL.lock().await;
    for store in stores() {
        let (server, _, owner, agent) = setup(&store, 6).await;
        let due = trigger(&owner, agent, "0 * * * *", 20 * HOUR, Missed::RunOnceLate);
        put(&store, &due).await;
        let (runs, url, id, who) = (store.clone(), server.redis.clone(), due.id, owner.clone());
        let (first, second, pending) = blocking(move || {
            let queue = RedisRunQueue::open(url);
            let first = fire_trigger_at(runs.as_ref(), &queue, &who, id, 20 * HOUR).expect("fire");
            let second = fire_trigger_at(runs.as_ref(), &queue, &who, id, 20 * HOUR).expect("fire");
            let pushed = server::sweep(
                &queue,
                runs.as_ref(),
                std::time::Duration::ZERO,
                std::time::Duration::from_secs(3600),
            )
            .expect("sweep");
            assert!(pushed.is_empty(), "{pushed:?}");
            (first, second, queue.pending().expect("pending"))
        })
        .await;
        let Fired::Run(run) = first else {
            panic!("{first:?}")
        };
        assert_eq!(second, Fired::Run(run));
        assert!(!pending.contains(&run), "off the pending set");
        let url = server.redis.clone();
        let queued = blocking(move || RedisRunQueue::open(url).queued().expect("queued")).await;
        assert_eq!(queued.iter().filter(|queued| **queued == run).count(), 1);
        // Once run, a third fire of the tick leaves it: not queued again.
        while server.work().await.is_some() {}
        let (runs, url, id, who) = (store.clone(), server.redis.clone(), due.id, owner.clone());
        let queued = blocking(move || {
            let queue = RedisRunQueue::open(url);
            fire_trigger_at(runs.as_ref(), &queue, &who, id, 20 * HOUR).expect("fire");
            queue.queued().expect("queued")
        })
        .await;
        assert!(!queued.contains(&run), "an ended run is not queued again");
        forget(&store, &owner, &[due.id]).await;
    }
}

/// An in-memory store whose one-trigger reads fail once, when armed: the
/// `n`-th read after `arm(n)`.
#[derive(Default)]
struct FailingRead {
    inner: server::InMemoryStore,
    /// A countdown, not a lock: the test arms it, then one pass on one
    /// thread reads it in turn. Its load and store need no order with any
    /// other memory, and SeqCst is used only for plainness.
    fail_in: std::sync::atomic::AtomicUsize,
}

impl FailingRead {
    fn arm(&self, n: usize) {
        self.fail_in.store(n, std::sync::atomic::Ordering::SeqCst);
    }
}

impl server::RunStore for FailingRead {
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

impl server::TriggerStore for FailingRead {
    fn put_trigger(
        &self,
        trigger: &StoredTrigger,
        most: usize,
    ) -> Result<bool, server::StoreError> {
        self.inner.put_trigger(trigger, most)
    }
    fn triggers_of(&self, owner: &Owner) -> Result<Vec<StoredTrigger>, server::StoreError> {
        self.inner.triggers_of(owner)
    }
    fn trigger(
        &self,
        owner: &Owner,
        id: TriggerId,
    ) -> Result<Option<StoredTrigger>, server::StoreError> {
        use std::sync::atomic::Ordering;
        let left = self.fail_in.load(Ordering::SeqCst);
        if left > 0 {
            self.fail_in.store(left - 1, Ordering::SeqCst);
            if left == 1 {
                return Err(server::StoreError::new("the store is unreachable"));
            }
        }
        self.inner.trigger(owner, id)
    }
    fn set_enabled(
        &self,
        owner: &Owner,
        id: TriggerId,
        enabled: bool,
    ) -> Result<Option<StoredTrigger>, server::StoreError> {
        self.inner.set_enabled(owner, id, enabled)
    }
    fn rotate_webhook(
        &self,
        owner: &Owner,
        id: TriggerId,
    ) -> Result<Option<StoredTrigger>, server::StoreError> {
        self.inner.rotate_webhook(owner, id)
    }
    fn delete_trigger(&self, owner: &Owner, id: TriggerId) -> Result<bool, server::StoreError> {
        self.inner.delete_trigger(owner, id)
    }
    fn pause_triggers(&self, owner: &Owner) -> Result<usize, server::StoreError> {
        self.inner.pause_triggers(owner)
    }
    fn resume_trigger(
        &self,
        owner: &Owner,
        id: TriggerId,
        next_fire_ms: Option<i64>,
    ) -> Result<Option<StoredTrigger>, server::StoreError> {
        self.inner.resume_trigger(owner, id, next_fire_ms)
    }
    fn due_triggers(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> Result<Vec<StoredTrigger>, server::StoreError> {
        self.inner.due_triggers(now_ms, limit)
    }
    fn advance_trigger(
        &self,
        id: TriggerId,
        due_ms: Option<i64>,
        next_ms: Option<i64>,
    ) -> Result<bool, server::StoreError> {
        self.inner.advance_trigger(id, due_ms, next_ms)
    }
    fn unscheduled_triggers(&self, limit: usize) -> Result<Vec<StoredTrigger>, server::StoreError> {
        self.inner.unscheduled_triggers(limit)
    }
}

// A fire whose second read of its trigger fails leaves its run stored,
// unpushed and pending, and the tick unmoved. The queue sweep settles it
// through the same gate and pushes it; the next pass fires the tick again,
// finds it queued, and leaves it: the tick runs, once.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_second_read_is_finished_by_the_next_pass() {
    let _turn = SERIAL.lock().await;
    let failing = std::sync::Arc::new(FailingRead::default());
    let store: Store = failing.clone();
    let (server, user, owner, agent) = setup(&store, 7).await;
    let due = trigger(&owner, agent, "0 * * * *", 10 * HOUR, Missed::RunOnceLate);
    put(&store, &due).await;
    // The pass reads the due triggers, then the fire reads it (1) and reads
    // it again at its gate (2): that read fails.
    failing.arm(2);
    let now = 10 * HOUR + 10_000;
    assert_eq!(pass(&server, now, &[due.id]).await, Vec::<RunId>::new());
    assert_eq!(
        stored(&store, &owner, due.id).await.next_fire_ms,
        Some(10 * HOUR)
    );
    // Kept pending, and both the queue sweep and the next pass settle it:
    // pushed once.
    let (runs, url) = (store.clone(), server.redis.clone());
    let swept = blocking(move || {
        server::sweep(
            &RedisRunQueue::open(url),
            runs.as_ref(),
            std::time::Duration::ZERO,
            std::time::Duration::from_secs(3600),
        )
        .expect("sweep")
    })
    .await;
    assert_eq!(swept.len(), 1, "the sweep pushed it, past its gate");
    let again = pass(&server, now, &[due.id]).await;
    assert_eq!(again, swept, "the same run");
    let url = server.redis.clone();
    let queued = blocking(move || RedisRunQueue::open(url).queued().expect("queued")).await;
    assert_eq!(
        queued.iter().filter(|queued| **queued == swept[0]).count(),
        1,
        "queued once"
    );
    assert_eq!(server.work().await, Some(swept[0]));
    let cards = fired(&server, &user, due.id).await;
    assert_eq!(cards.len(), 1);
    assert_eq!(cards[0]["state"], "completed");
}

// A schedule made before the scheduler (no tick yet) gets its first tick
// from the next pass; one whose schedule has no tick after now (a date that
// never comes again) fires its last tick and is then left with none.
#[tokio::test(flavor = "multi_thread")]
async fn schedules_without_a_tick_get_one_or_none() {
    let _turn = SERIAL.lock().await;
    for store in stores() {
        let (server, _, owner, agent) = setup(&store, 8).await;
        let mut old = trigger(&owner, agent, "0 * * * *", 0, Missed::RunOnceLate);
        old.next_fire_ms = None;
        put(&store, &old).await;
        let now = 30 * HOUR + 10_000;
        // A pass ticks at most 100 such schedules, and the shared Postgres
        // may hold others'.
        for _ in 0..50 {
            pass(&server, now, &[old.id]).await;
            if stored(&store, &owner, old.id).await.next_fire_ms.is_some() {
                break;
            }
        }
        assert_eq!(
            stored(&store, &owner, old.id).await.next_fire_ms,
            Some(31 * HOUR)
        );
        // February 30th never comes: its one tick (stored by hand) fires,
        // and then there is no next.
        let never = trigger(&owner, agent, "0 0 30 2 *", 30 * HOUR, Missed::RunOnceLate);
        put(&store, &never).await;
        assert_eq!(pass(&server, now, &[never.id]).await.len(), 1);
        assert_eq!(stored(&store, &owner, never.id).await.next_fire_ms, None);
        assert_eq!(pass(&server, now, &[never.id]).await, Vec::<RunId>::new());
        forget(&store, &owner, &[old.id, never.id]).await;
    }
}

// Resuming a trigger that is running keeps its tick: a due tick is not
// dropped.
#[tokio::test(flavor = "multi_thread")]
async fn resuming_a_running_trigger_keeps_its_tick() {
    let _turn = SERIAL.lock().await;
    let store = stores().remove(0);
    let (server, user, owner, agent) = setup(&store, 9).await;
    let due = trigger(&owner, agent, "* * * * *", 60_000, Missed::RunOnceLate);
    put(&store, &due).await;
    let (status, resumed) = server
        .post(&format!("/v1/triggers/{}/resume", due.id), &user, json!({}))
        .await;
    assert_eq!(status, 200, "{resumed}");
    assert_eq!(resumed["next_fire_at"], 60_000);
    assert_eq!(
        stored(&store, &owner, due.id).await.next_fire_ms,
        Some(60_000)
    );
}

// A resume of a trigger that is already running (a second, late resume)
// changes nothing: its tick and generation stay. A 4.1 schedule the scheduler
// cannot read (no tick) is paused, so it neither fires nor holds up the
// others.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_resume_changes_nothing_and_a_bad_schedule_is_paused() {
    let _turn = SERIAL.lock().await;
    for store in stores() {
        let (server, user, owner, agent) = setup(&store, 10).await;
        let running = trigger(&owner, agent, "0 * * * *", 40 * HOUR, Missed::RunOnceLate);
        put(&store, &running).await;
        server
            .post(
                &format!("/v1/triggers/{}/pause", running.id),
                &user,
                json!({}),
            )
            .await;
        let (_, first) = server
            .post(
                &format!("/v1/triggers/{}/resume", running.id),
                &user,
                json!({}),
            )
            .await;
        let (_, second) = server
            .post(
                &format!("/v1/triggers/{}/resume", running.id),
                &user,
                json!({}),
            )
            .await;
        assert_eq!(second["next_fire_at"], first["next_fire_at"]);
        assert_eq!(stored(&store, &owner, running.id).await.generation, 1);
        // The store's resume leaves a running trigger as it is, whoever calls
        // it (two resumes that both read it paused).
        let (runs, who, id) = (store.clone(), owner.clone(), running.id);
        let again = blocking(move || {
            runs.triggers()
                .expect("triggers")
                .resume_trigger(&who, id, Some(1))
                .expect("resume")
                .expect("found")
        })
        .await;
        assert_eq!(again.generation, 1);
        assert_eq!(
            again.next_fire_ms.map(serde_json::Value::from),
            Some(first["next_fire_at"].clone())
        );

        let mut bad = trigger(&owner, agent, "every morning", 0, Missed::RunOnceLate);
        bad.next_fire_ms = None;
        put(&store, &bad).await;
        for _ in 0..50 {
            pass(&server, 40 * HOUR, &[bad.id]).await;
            if !stored(&store, &owner, bad.id).await.enabled {
                break;
            }
        }
        let bad_now = stored(&store, &owner, bad.id).await;
        assert!(!bad_now.enabled, "paused");
        assert_eq!(bad_now.next_fire_ms, None);
        forget(&store, &owner, &[running.id, bad.id]).await;
    }
}
