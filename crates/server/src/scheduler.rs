//! The scheduler (Phase 4.2, decisions 69A, 70A, 72A): fires schedule
//! triggers at their ticks.
use std::time::Duration;

use protocol::{RunId, Timestamp};

use crate::queue::RedisRunQueue;
use crate::store::{Missed, RunStore, StoredTrigger, TriggerKind};
use crate::triggers::{fire_trigger_at, Fired};

/// How late a tick may be fired and still count as on time. A later one is
/// a missed tick, which follows its trigger's rule (decision 70A).
pub const MISSED_AFTER_MS: i64 = 60_000;

/// The most triggers one pass fires.
const PASS_LIMIT: usize = 100;

/// The first tick of `cron` in `time_zone` strictly after `after_ms`, in
/// Unix milliseconds. Daylight saving follows croner: a wall-clock time the
/// clocks skip runs at its next occurrence, and one they repeat runs once.
pub fn next_tick(cron: &str, time_zone: &str, after_ms: i64) -> Result<i64, String> {
    use chrono::TimeZone;
    let cron: croner::Cron = cron
        .parse()
        .map_err(|error| format!("not a cron expression: {error}"))?;
    let zone: chrono_tz::Tz = time_zone
        .parse()
        .map_err(|_| format!("not a known time zone: {time_zone}"))?;
    let after = zone
        .timestamp_millis_opt(after_ms)
        .single()
        .ok_or_else(|| "a time out of range".to_string())?;
    cron.find_next_occurrence(&after, false)
        .map(|next| next.timestamp_millis())
        .map_err(|error| format!("no next tick: {error}"))
}

/// One scheduler pass at `now_ms`: every due trigger fired for its tick (or
/// not, by its missed-tick rule) and moved to its next tick. The runs it
/// fired. A trigger whose fire failed keeps its tick, and the next pass
/// fires it again: the same run, since its id comes from the tick.
pub fn schedule_due(
    store: &dyn RunStore,
    queue: &RedisRunQueue,
    now_ms: i64,
) -> Result<Vec<RunId>, String> {
    let triggers = store
        .triggers()
        .ok_or_else(|| "the store keeps no triggers".to_string())?;
    let due = triggers
        .due_triggers(now_ms, PASS_LIMIT)
        .map_err(|error| error.to_string())?;
    let mut fired = Vec::new();
    for trigger in &due {
        match fire_due_trigger(store, queue, trigger, now_ms) {
            Ok(Some(run)) => fired.push(run),
            Ok(None) => {}
            Err(error) => eprintln!("gol: scheduler: trigger {}: {error}", trigger.id),
        }
    }
    Ok(fired)
}

/// Fires due `trigger` for its tick, as read, unless its tick was missed
/// and it skips missed ticks, then moves it to the first tick after
/// `now_ms` if its tick is still the one read (decision 69A). The run it
/// fired, or found fired for that tick.
pub fn fire_due_trigger(
    store: &dyn RunStore,
    queue: &RedisRunQueue,
    trigger: &StoredTrigger,
    now_ms: i64,
) -> Result<Option<RunId>, String> {
    let (TriggerKind::Schedule { cron, time_zone }, Some(due)) =
        (&trigger.kind, trigger.next_fire_ms)
    else {
        return Ok(None);
    };
    let next = next_tick(cron, time_zone, now_ms)?;
    let missed = now_ms - due > MISSED_AFTER_MS;
    let mut run = None;
    if !(missed && trigger.missed == Missed::Skip) {
        match fire_trigger_at(store, queue, &trigger.owner, trigger.id, due)
            .map_err(|error| format!("{error:?}"))?
        {
            Fired::Run(fired) => run = Some(fired),
            Fired::Paused | Fired::NotFound | Fired::AgentNotFound => {}
        }
    }
    let triggers = store
        .triggers()
        .ok_or_else(|| "the store keeps no triggers".to_string())?;
    triggers
        .advance_trigger(trigger.id, due, next)
        .map_err(|error| error.to_string())?;
    Ok(run)
}

/// Runs a scheduler pass every `every`, for good (decision 72A: every
/// server runs one; exactly one fire per tick across servers is claimed on
/// Postgres, where their passes share the triggers table).
pub fn schedule_forever(
    store: std::sync::Arc<dyn RunStore>,
    queue: std::sync::Arc<RedisRunQueue>,
    every: Duration,
) {
    loop {
        if let Err(error) = schedule_due(store.as_ref(), &queue, Timestamp::now().as_unix_millis())
        {
            eprintln!("gol: scheduler: {error}");
        }
        std::thread::sleep(every);
    }
}
