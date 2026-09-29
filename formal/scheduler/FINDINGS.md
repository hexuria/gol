# Scheduler findings

Checked on 2026-09-30 for Phase 4.2. `Scheduler.tla` is one schedule trigger fired by several servers' schedulers and settled by the queue sweep (decisions 69A-72A), against its owner's stop and resume (Phase 4.1).

## Model

**Resource:** one `triggers` row (its `next_fire_ms`, `enabled` and `generation`), the runs its fires store, and the Redis queue they are pushed to. The row and runs live in `crates/server/src/postgres.rs` and `InMemoryStore` (`crates/server/src/store.rs`); the queue is `crates/server/src/queue.rs`.

**Writers:**
- **Each server's scheduler:** `schedule_due` / `fire_due_trigger` (`crates/server/src/scheduler.rs`).
  - It reads the due trigger with its tick and generation, then fires the tick through `fire_trigger_at` (`crates/server/src/triggers.rs`).
  - The fire stores the run under an id of the trigger and the tick, pending first (`spawner::enqueue_fire`); `put_run` stores a run once.
  - It then settles the run (`spawner::settle_fire`). A run still waiting is gated: the trigger is read again, and must be running at the generation the fire read. It is then pushed once (`RedisRunQueue::push_once`, a script that queues the run unless it is queued or claimed), or held (cancelled, never pushed).
  - A settle that fails leaves the run stored and pending, and ends the pass without moving the trigger.
  - Otherwise the trigger moves to its next tick with `advance_trigger`, only if its tick is still the one read (69A).
- **The queue sweep:** `worker::sweep` settles a trigger's run it finds pending the same way (`triggers::settle_fired_run`), by the generation the run recorded (`gol.generation`).
- **The owner:**
  - Its stop (`stop_owner`, `crates/server/src/http.rs`) pauses the triggers (`pause_triggers`), then records the stop.
  - A resume (`resume_trigger`) sets a paused trigger running again, at a new generation and with its next tick after now, in one statement. A trigger already running is left as it is.

**Steps and bounds:**
- A scheduler takes one step per atomic step of the code:
  - `Read`: the pass's read and the fire's first read, which see one row.
  - `Store`: the put.
  - `Gate`: `settle_fire`'s second read, then its push-once script or its cancel, merged into one step. In the code they are two steps: the read, then one Redis script.
    - A stop between them is covered: the run was stored before the stop's record, so the stop covers it and its worker cancels it at its claim (Phase 3.4).
    - A plain pause and resume between them, with no stop, is not covered: the run of the tick read is pushed after the resume. This window is one Redis round trip.
  Neither case is in the model.
  - `Fail`.
  - `Advance`.
- The sweep settles in one step: `Sweep`.
- The owner: `Pause`, `Record`, `Resume`, each at most once.
- Two schedulers, MaxTick = 2 ticks in the window.

**Designs:**
- `Design = "derived"` is the code.
- `"random"` gives each fire a fresh id.
- `"nogen"` gates on `enabled` alone, as #85's first version did.
- `"pushtwice"` pushes without checking the queue.
- `"blind"` moves the trigger to its tick + 1 without the condition.
- `"nofire"` stores nothing and still moves on.
- `"nosweep"` has no sweep.

**Not modelled:**
- delete, which the gate treats as a pause (`Fired::NotFound`, tested);
- the missed-tick rules;
- the first read's `Moved` answer, which only takes behaviours away.

## Properties

- `OneRunPerTick`: one run per tick, however many schedulers fired it.
- `PushedOnce`: a run is queued at most once.
- `NoPushAcrossPause`: no run whose fire read the trigger before a pause is pushed after it. The stop covers the runs stored before its record, and a resumed trigger owes nothing for the ticks it was paused.
- `HeldOnlyAcrossPause`: a run is held only because a pause came after its fire's read.
- `PassedTicksFired`: every tick the trigger has moved past has a run, unless a resume skipped it.
- `TicksAdvance`: the next tick never moves back.
- `StoredRunsSettle`: every stored run is settled, pushed or held.
- `EveryTickFires`: while the trigger runs, the schedule gets through its ticks.

The last two hold under weak fairness on each scheduler's successful steps (its thread loops) and on the sweep (the reaper loops). A settle may fail any number of times. Deadlock is checked, and `Done` is the only stuttering step.

## TLA+ Findings

```text
java -XX:+UseParallelGC -jar ~/.local/tla/tla2tools.jar -workers auto -lncheck final -config Scheduler.cfg Scheduler.tla
```

TLC2 Version 2.19 of 08 August 2024, from tla2tools v1.7.4, which `scripts/install-tla.sh` pins. There is no `-deadlock`.
- `Scheduler.cfg` (`Design = "derived"`, two schedulers, `MaxTick = 2`): 3,621 states generated, 1,404 distinct, depth 16. No error, no deadlock.
- Confirmed at larger bounds, with no error:
  - `MaxTick = 3`: 10,563 generated, 4,046 distinct, depth 20.
  - Three schedulers: 42,072 generated, 12,794 distinct, depth 19.

Negative controls, each on a copy of `Scheduler.cfg` with `-workers 1`:
- `Design = "random"` violates `OneRunPerTick` in a 5-state trace: s1 reads tick 1 and stores its run, then s2 reads tick 1 and stores another.
- `Design = "nogen"` violates `NoPushAcrossPause` in a 7-state trace: s1 reads tick 1, the owner pauses, records and resumes, then s1 stores its run and pushes it.
- `Design = "pushtwice"` violates `PushedOnce` in a 5-state trace: s1 stores and pushes tick 1's run, and s2, settling the same run, pushes it again.
- `Design = "blind"` violates `TicksAdvance` in a 12-state trace: s2 reads tick 1; s1 fires tick 1 and moves the trigger to 2, fires tick 2 and moves it to 3; then s2, going on with tick 1, moves it back to 2.
- `Design = "nofire"` violates `PassedTicksFired` in a 5-state trace: s1 reads tick 1, stores nothing, and moves the trigger to 2.
- `Design = "nosweep"` violates `StoredRunsSettle`, and `EveryTickFires` with it, in a 17-state lasso: a settle fails and leaves its run stored, and a scheduler whose settles keep failing never settles it.

## Mapping

In `crates/server/tests/scheduler.rs` and `crates/server/tests/triggers.rs`:
- **`OneRunPerTick`:**
  - `one_fire_per_tick_with_two_schedulers` (forced; both stores): one scheduler's read, another's two passes, then the first's fire fires nothing.
  - `a_due_trigger_fires_once_per_tick`.
  - `a_tick_fired_twice_is_one_run_pushed_once`.
- **`PushedOnce`:**
  - `a_tick_fired_twice_is_one_run_pushed_once`: a second fire finds the run queued.
  - `a_failed_second_read_is_finished_by_the_next_pass`: the sweep and the next pass both settle one run, which is queued once.
- **`NoPushAcrossPause`:**
  - `a_fire_across_a_pause_and_resume_holds_its_run` (a new generation at the gate).
  - `a_fire_racing_a_pause_holds_its_run`.
  - `a_fire_racing_a_delete_holds_its_run`.
- **`HeldOnlyAcrossPause`:** `a_failed_second_read_is_finished_by_the_next_pass`. A gate that cannot tell does not hold; the run is pushed later.
- **`StoredRunsSettle`:**
  - `a_failed_hold_is_settled_by_the_sweep`: a cancel fails, the run stays pending, and the sweep holds it.
  - `a_failed_second_read_is_finished_by_the_next_pass`.
- **`TicksAdvance`:** `one_fire_per_tick_with_two_schedulers`. The other scheduler moves the trigger two ticks on, and the stale one leaves it there; its hand mutant fails that test on both stores. `advance_trigger` is one conditional update on Postgres and one lock scope in memory.
- **`PassedTicksFired`:** `a_due_trigger_fires_once_per_tick`, `missed_ticks_follow_their_rule`.
- **A resume:**
  - `a_paused_schedule_is_not_owed_its_ticks`: not due while paused; resumed to the first tick after now.
  - `a_late_resume_changes_nothing_and_a_bad_schedule_is_paused`: a second resume keeps the tick and generation.
- **`EveryTickFires`:** unlinked.
- **The model's assumption that `put_run` stores a run once:** RunLog's `Reput`.
- **The model's assumption that `push_once` is one step:** it is one Redis script.

Retire this model if fires move to a table that numbers ticks itself (a unique `(trigger, tick)` row written with the advance), with its own model.
