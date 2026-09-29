//! The scheduler (Phase 4.2, decisions 69A, 70A, 72A): fires schedule
//! triggers at their ticks.
use std::time::Duration;

use protocol::RunId;

use crate::queue::RedisRunQueue;
use crate::store::{Missed, RunStore, StoredTrigger, TriggerKind};
use crate::triggers::{fire_trigger_at, Fired};

/// How late a tick may be fired and still count as on time. A later one is
/// a missed tick, which follows its trigger's rule (decision 70A).
pub const MISSED_AFTER_MS: i64 = 60_000;

/// The most triggers one pass fires.
const PASS_LIMIT: usize = 100;

/// The most candidates `next_tick` looks through for one tick: croner's,
/// each checked against the clock changes.
const MAX_CANDIDATES: usize = 1000;

/// The parser of a schedule: five fields, minute to day of week. No seconds
/// (a schedule fires at most once a minute) and no year (a schedule never
/// runs out of ticks).
fn parser() -> croner::parser::CronParser {
    croner::parser::CronParser::builder()
        .seconds(croner::parser::Seconds::Disallowed)
        .year(croner::parser::Year::Disallowed)
        .build()
}

/// The first tick of `cron` in `time_zone` strictly after `after_ms`, in
/// Unix milliseconds. Through daylight-saving changes:
/// - a wall-clock time the clocks skip runs when they resume, once, and a
///   time outside the gap is not moved;
/// - a wall-clock time the clocks repeat runs at its first occurrence only.
pub fn next_tick(cron: &str, time_zone: &str, after_ms: i64) -> Result<i64, String> {
    use chrono::TimeZone;
    let cron = parser()
        .parse(cron)
        .map_err(|error| format!("not a cron expression: {error}"))?;
    let zone: chrono_tz::Tz = time_zone
        .parse()
        .map_err(|_| format!("not a known time zone: {time_zone}"))?;
    let mut after = zone
        .timestamp_millis_opt(after_ms)
        .single()
        .ok_or_else(|| "a time out of range".to_string())?;
    for _ in 0..MAX_CANDIDATES {
        let candidate = cron
            .find_next_occurrence(&after, false)
            .map_err(|error| format!("no next tick: {error}"))?;
        if is_tick(&cron, zone, &candidate)? {
            // croner steps over a gap whose skipped times a list or a step
            // names: such a gap's end comes first.
            let tick = candidate.timestamp_millis();
            return Ok(gap_tick(&cron, zone, after_ms, tick)?.unwrap_or(tick));
        }
        after = candidate;
    }
    Err("no next tick".to_string())
}

/// The first instant strictly between `after_ms` and `before_ms` where the
/// clocks jump forward over a wall-clock time the pattern names: a skipped
/// time runs once, when the clocks resume.
fn gap_tick(
    cron: &croner::Cron,
    zone: chrono_tz::Tz,
    after_ms: i64,
    before_ms: i64,
) -> Result<Option<i64>, String> {
    use chrono::{Offset, TimeZone};
    let offset = |ms: i64| -> i64 {
        let utc = chrono::DateTime::from_timestamp_millis(ms)
            .unwrap_or_default()
            .naive_utc();
        i64::from(zone.offset_from_utc_datetime(&utc).fix().local_minus_utc()) * 1000
    };
    const MINUTE: i64 = 60_000;
    const STEP: i64 = 60 * MINUTE;
    let mut from = after_ms;
    while from < before_ms {
        let to = (from + STEP).min(before_ms);
        let (old, new) = (offset(from), offset(to));
        if new > old {
            // The clocks go forward in (from, to]: the first minute on the
            // new offset is where they resume.
            let (mut low, mut high) = (from, to);
            while high - low > MINUTE {
                let middle = low + (high - low) / 2;
                if offset(middle) > old {
                    high = middle;
                } else {
                    low = middle;
                }
            }
            let resume = high - high.rem_euclid(MINUTE);
            let resume = if offset(resume) > old {
                resume
            } else {
                resume + MINUTE
            };
            if resume > after_ms && resume < before_ms {
                // The wall-clock minutes skipped: from `resume` on the old
                // offset up to it on the new one.
                let mut skipped = resume + old;
                while skipped < resume + new {
                    let wall = chrono::DateTime::from_timestamp_millis(skipped)
                        .unwrap_or_default()
                        .naive_utc();
                    if cron
                        .is_time_matching(&chrono::Utc.from_utc_datetime(&wall))
                        .map_err(|error| error.to_string())?
                    {
                        return Ok(Some(resume));
                    }
                    skipped += MINUTE;
                }
            }
        }
        from = to;
    }
    Ok(None)
}

/// Whether croner's `candidate` is a tick: its wall-clock time matches the
/// pattern and is not the repeat of a time the clocks went back over; or it
/// is where the clocks resume after a gap that skipped a time the pattern
/// names (croner gives the gap's end for a time inside it, and at times for
/// one that is not).
fn is_tick(
    cron: &croner::Cron,
    zone: chrono_tz::Tz,
    candidate: &chrono::DateTime<chrono_tz::Tz>,
) -> Result<bool, String> {
    use chrono::TimeZone;
    let wall = candidate.naive_local();
    if let chrono::LocalResult::Ambiguous(first, _) = zone.from_local_datetime(&wall) {
        if *candidate != first {
            return Ok(false);
        }
    }
    let matches = |wall: chrono::NaiveDateTime| {
        cron.is_time_matching(&chrono::Utc.from_utc_datetime(&wall))
            .map_err(|error| error.to_string())
    };
    if matches(wall)? {
        return Ok(true);
    }
    // The wall-clock minutes just before the candidate that never happened.
    let minute = chrono::Duration::minutes(1);
    let mut skipped = wall - minute;
    for _ in 0..(3 * 60) {
        if !matches!(
            zone.from_local_datetime(&skipped),
            chrono::LocalResult::None
        ) {
            break;
        }
        if matches(skipped)? {
            return Ok(true);
        }
        skipped -= minute;
    }
    Ok(false)
}

/// One scheduler pass at `now_ms`: every schedule with no tick yet gets
/// its first, and every due trigger is fired for its tick (or not, by its
/// missed-tick rule) and moved to its next tick. The runs it fired. A
/// trigger whose fire failed keeps its tick, and the next pass fires it
/// again: the same run, since its id comes from the tick.
pub fn schedule_due(
    store: &dyn RunStore,
    queue: &RedisRunQueue,
    now_ms: i64,
) -> Result<Vec<RunId>, String> {
    pass(store, queue, now_ms).map(|(fired, _)| fired)
}

/// `schedule_due`, and whether the pass read as many due triggers as it may
/// (more may be waiting).
fn pass(
    store: &dyn RunStore,
    queue: &RedisRunQueue,
    now_ms: i64,
) -> Result<(Vec<RunId>, bool), String> {
    let triggers = store
        .triggers()
        .ok_or_else(|| "the store keeps no triggers".to_string())?;
    // Schedules made before the scheduler (Phase 4.1) get their first tick.
    for trigger in triggers
        .unscheduled_triggers(PASS_LIMIT)
        .map_err(|error| error.to_string())?
    {
        if let TriggerKind::Schedule { cron, time_zone } = &trigger.kind {
            match next_tick(cron, time_zone, now_ms) {
                Ok(first) => {
                    triggers
                        .advance_trigger(trigger.id, None, Some(first))
                        .map_err(|error| error.to_string())?;
                }
                // A schedule Phase 4.1 took without parsing it, that has no
                // tick: paused, so it neither fires nor fills this batch
                // again. Its owner's resume says why (400).
                Err(error) => {
                    eprintln!("gol: scheduler: trigger {} paused: {error}", trigger.id);
                    triggers
                        .set_enabled(&trigger.owner, trigger.id, false)
                        .map_err(|error| error.to_string())?;
                }
            }
        }
    }
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
    Ok((fired, due.len() == PASS_LIMIT))
}

/// Fires due `trigger` for its tick, as read, unless its tick was missed
/// and it skips missed ticks, then moves it to the first tick after
/// `now_ms` if its tick is still the one read (decision 69A); a schedule
/// with no tick after now is left with none. The run it fired, or found
/// fired for that tick; none when another scheduler had already moved it.
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
    let missed = now_ms - due > MISSED_AFTER_MS;
    let mut run = None;
    if !(missed && trigger.missed == Missed::Skip) {
        match fire_trigger_at(store, queue, &trigger.owner, trigger.id, due)
            .map_err(|error| format!("{error:?}"))?
        {
            Fired::Run(fired) => run = Some(fired),
            Fired::Paused | Fired::NotFound | Fired::AgentNotFound | Fired::Moved => {}
        }
    }
    let next = next_tick(cron, time_zone, now_ms)
        .map_err(|error| eprintln!("gol: scheduler: trigger {}: {error}", trigger.id))
        .ok();
    let triggers = store
        .triggers()
        .ok_or_else(|| "the store keeps no triggers".to_string())?;
    triggers
        .advance_trigger(trigger.id, Some(due), next)
        .map_err(|error| error.to_string())?;
    Ok(run)
}

/// The most passes a scheduler runs back to back while each finds as many
/// due triggers as it may.
const DRAIN_PASSES: usize = 10;

/// Runs a scheduler pass every `every`, for good (decision 72A: every
/// server runs one; exactly one fire per tick across servers is claimed on
/// Postgres, where their passes share the triggers table). The time is
/// Redis's, which every server shares, so a server whose clock is off fires
/// nothing early. A full pass is followed at once by another, up to
/// `DRAIN_PASSES`. A pass that panics is reported, and the next one runs.
pub fn schedule_forever(
    store: std::sync::Arc<dyn RunStore>,
    queue: std::sync::Arc<RedisRunQueue>,
    every: Duration,
) {
    loop {
        for _ in 0..DRAIN_PASSES {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let now = queue.now_ms()?;
                pass(store.as_ref(), &queue, now)
            }));
            match outcome {
                Ok(Ok((_, true))) => continue,
                Ok(Ok((_, false))) => {}
                Ok(Err(error)) => eprintln!("gol: scheduler: {error}"),
                Err(_) => eprintln!("gol: scheduler: a pass panicked"),
            }
            break;
        }
        std::thread::sleep(every);
    }
}
