# Scheduler findings

Checked on 2026-09-30 for Phase 4.2. `Scheduler.tla` is one schedule trigger fired by several servers' schedulers (decisions 69A-72A), against its owner's stop (Phase 4.1).

## Model

**Resource:** one `triggers` row (its `next_fire_ms` and `enabled`) and the runs its fires store, in `crates/server/src/postgres.rs` and `InMemoryStore` (`crates/server/src/store.rs`).

**Writers:**
- Each server's scheduler: `schedule_due` / `fire_due_trigger` (`crates/server/src/scheduler.rs`). It reads the due trigger (`due_triggers`), fires its tick through `fire_trigger_at` (`crates/server/src/triggers.rs`), then moves it to its next tick with `advance_trigger`, only if its tick is still the one read (69A).
  - The fire stores the run (`spawner::enqueue_unless`: pend, `put_run`), reads the trigger again, then pushes the run, or holds it (cancels it, never pushed) if the trigger is paused or gone by then.
  - The run's id is a UUID v5 of the trigger and the tick, and `put_run` stores a run once.
- The owner's stop: `stop_owner` (`crates/server/src/http.rs`) pauses the owner's triggers (`pause_triggers`), then records the stop (`put_stop`). The stop covers the runs stored before its record.

**Steps and bounds:**
- Each scheduler takes four steps, one per atomic step of the code: `Read`, `Store`, `Gate` (the second read and the push or the hold), and `Advance`. The stop takes two: `Pause` and `Record`.
- Schedulers = {s1, s2}, MaxTick = 2 ticks in the window.

**Designs:**
- `Design = "derived"` is the code.
- `"random"` gives each fire a fresh run id.
- `"latecheck"` pushes the run before the second read, as #84's first version did.
- `"blind"` moves the trigger to its tick + 1 without the condition.

## Properties

- `OneRunPerTick`: one run for each tick, however many schedulers fired it.
- `NoRunEscapesStop`: a run stored after the stop's record is never pushed.
- `TicksAdvance`: the next tick never moves back.
- `EveryTickFires`: while the trigger runs, the schedule gets through its ticks. This holds under weak fairness on each scheduler's steps (its pass keeps running) and on the stop's record (a stop begun finishes). The stop itself need not come.
- Deadlock is checked. `Done` is the only stuttering step.

## TLA+ Findings

```text
java -XX:+UseParallelGC -jar ~/.local/tla/tla2tools.jar -workers auto -lncheck final -config Scheduler.cfg Scheduler.tla
```

TLC2 Version 2.19 of 08 August 2024, from tla2tools v1.7.4, which `scripts/install-tla.sh` pins. There is no `-deadlock`.
- `Scheduler.cfg` (`Design = "derived"`, two schedulers, `MaxTick = 2`): 1,398 states generated, 702 distinct, depth 15. No error, no deadlock.
- Confirmed at larger bounds, with no error:
  - `MaxTick = 3`: 3,222 generated, 1,605 distinct, depth 19.
  - Three schedulers: 27,681 generated, 10,725 distinct, depth 19.
- Three schedulers with `MaxTick = 3`: TLC explores the whole space (104,997 generated, 39,888 distinct, none left on the queue) and reports no violation. It then throws `java.lang.ArithmeticException: Division by zero` in its own end-of-run code. The same happens with only the invariants, one worker, another fingerprint and another seed, and the spec divides nothing. That run is not counted as a pass.

Negative controls, each on a copy of `Scheduler.cfg` with `-workers 1`:
- `Design = "random"` violates `OneRunPerTick` in a 5-state trace: s1 reads tick 1 and stores its run, then s2 reads tick 1 and stores another.
- `Design = "latecheck"` violates `NoRunEscapesStop` in a 5-state trace: s1 reads tick 1, the stop pauses the trigger and records itself, then s1 stores and pushes its run, which the stop does not cover.
- `Design = "blind"` violates `TicksAdvance` in a 10-state trace: s1 fires tick 1; s2 reads tick 1; s1 moves the trigger to 2, fires tick 2 and moves it to 3; then s2, going on with tick 1, moves it back to 2.

## Mapping

- `OneRunPerTick`: `one_fire_per_tick_with_two_schedulers` (forced: one scheduler's read, another's whole pass, then the first's fire of what it read gives the same run, pushed once) and `a_due_trigger_fires_once_per_tick`, in `crates/server/tests/scheduler.rs`, on both stores.
- `NoRunEscapesStop`: `a_fire_racing_a_pause_holds_its_run` and `a_fire_racing_a_delete_holds_its_run` (forced: the pause lands between the fire's store and its second read), in `crates/server/tests/triggers.rs`.
- `TicksAdvance`: `one_fire_per_tick_with_two_schedulers`. The other scheduler fires two ticks and moves the trigger to the third, and the stale scheduler's update then leaves it there, where an unconditional one would move it back (its hand mutant fails that test on both stores). `advance_trigger` is one conditional update on Postgres and one lock scope in memory.
- A paused trigger is not due: `a_paused_schedule_is_not_owed_its_ticks` (a pass leaves its tick alone, and a resume moves it to the first tick after now).
- `EveryTickFires`: unlinked.
- The model's assumption that `put_run` stores a run once: RunLog's `Reput`, and its Rust tests.

Retire this model if fires move to a table that numbers ticks itself (a unique `(trigger, tick)` row written with the advance), with its own model.
