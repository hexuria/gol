# Outbox findings

Checked on 2026-09-29 for Phase 3.1. `Outbox.tla` is the per-owner outbox: every event a run store keeps is numbered in its owner's outbox in the same transaction (decisions 16A and 36A–39A).

## Model

**Resource:** the owner's counter row (`outbox_counters`) and the `outbox` rows, in `crates/server/src/postgres.rs`, plus the `Outbox` behind `InMemoryStore`'s lock in `crates/server/src/store.rs`.

**Writers:** every stored event goes through `EventRows::insert` (`postgres.rs`), from two callers:
- `put_run`: inserts the new run's row, then its events.
- `append_checked`: backs `append_events` and `append_events_after`. It locks the run row (`select ... for update`), checks it, then inserts.

Either way the run row is held when the counter row is taken, by an upsert that locks it and adds the count (`insert ... on conflict ... do update ... returning last`). The event rows and the outbox rows are written in the same transaction, and the commit releases both locks. `InMemoryStore` numbers inside `put_run` and `append_checked` while holding the runs lock, taking the outbox lock inside it.

**Pruner:** `OutboxStore::prune_outbox`, from the queue's reaper, with a cutoff 7 days back (38A). In one statement, for each principal, it finds the last number stored before the cutoff, deletes that principal's entries up to that number, and marks the counter row `pruned` through it. So a prefix goes even when writers on hosts with different clocks stored a later number with an earlier time. It takes only the counter row's lock, never a run row.

**Reader:** `OutboxStore::outbox_after` resumes from the last number it saw, as `GET /v1/stream` will with `Last-Event-ID` (Phase 3.2). One repeatable-read snapshot gives the page and `pruned_through`, so a reader below `pruned_through` knows it lost entries.

**Steps and bounds:**
- Each writer takes three steps, one per atomic step of the code: lock the run row, take the number(s), then insert and commit.
- One `Prune`, of committed rows, bounded to one to keep the state space small.
- `NWriters = 3`: writers 1 and 2 append to one run (a worker and a late message), and writer 3 appends to another. `EventsPer = 2` events per append.

**Designs:**
- `Design = "new"` is 16A.
- `"noLock"` reads the counter without its row lock.
- `"sequence"` takes numbers from a global sequence that commits in any order (16B).
- `"inverted"` has one write path take the counter before the run row.
- `"clock"` prunes one row by its stored time alone.

## Properties

- `UniqueSeqs`: no two rows share a number.
- `Suffix`: the kept numbers are always exactly `pruned+1 .. pruned+n`, with no hole.
- `RunOrder`: a run's events keep their order in the outbox.
- `NoSkip`: a reader never moves its cursor past a row it has not delivered. This is 16A's "no skipped events on resume".
- `EveryWriterCommits` and `ReaderCatchesUp`, under weak fairness on each writer's steps (the store's calls return) and on the reader (it keeps polling while its stream is open).
- Deadlock is checked. `Done` is the only stuttering step.

## TLA+ Findings

```text
java -XX:+UseParallelGC -jar ~/.local/tla/tla2tools.jar -workers auto -lncheck final -config Outbox.cfg Outbox.tla
```

TLC2 Version 2.19 of 08 August 2024, from tla2tools v1.7.4, which `scripts/install-tla.sh` pins. There is no `-deadlock`. Every invariant and property passed, and no state is a deadlock.
- `Outbox.cfg` (`Design = "new"`, `NWriters = 3`, `EventsPer = 2`): 1,524 states generated, 772 distinct, depth 13.
- Confirmed once at larger bounds, with no error:
  - `NWriters = 4`: 15,599 generated, 7,118 distinct, depth 16.
  - `EventsPer = 3`: 2,271 generated, 1,147 distinct, depth 13.
- Before the pruner was added, the same config gave 229 generated, 133 distinct, depth 11.

Negative controls, each on a copy of `Outbox.cfg` with `-workers 1`:
- `Design = "noLock"` violates `UniqueSeqs` in a 7-state trace. Writers 1 and 3 each lock their own run and read the counter at 0. Both commit rows numbered 1 and 2.
- `Design = "sequence"` violates `Suffix` in a 6-state trace: writer 3 draws 3–4 after writer 1 drew 1–2, and commits first. With `Suffix` left out, it violates `NoSkip` in an 8-state trace: the reader reads 3–4 and moves its cursor to 4, then 1–2 commit behind it, and a resume from 4 never sees them.
- `Design = "inverted"` deadlocks in a 4-state trace:
  1. Writer 1 takes the counter.
  2. Writer 2 locks run 1 and waits for the counter.
  3. Writer 1 waits for run 1.
  4. Writer 3 locks run 2 and waits for the counter.

  This is why every write path takes the run row first: `EventRows::insert` is the only place that numbers, and both of its callers hold the run row before calling it.
- `Design = "clock"` violates `Suffix` in a 5-state trace: writer 1 commits 1–2, and the prune removes 2 alone, keeping 1. A reader holding 1 would never learn that 2 existed.

## Mapping

Tests in `crates/server/tests/outbox.rs`, on `InMemoryStore` and `PostgresStore`:
- `UniqueSeqs`, `Suffix` (with nothing pruned) and `RunOrder`: `racing_appends_for_one_owner_get_consecutive_numbers`. Four threads, two per run, append 10 pairs each; the numbers are 1..n, each run's `run_seq` rises with them, and each pair takes consecutive numbers. `every_stored_event_leaves_one_outbox_entry` covers a put and an append, and `a_refused_append_leaves_no_entry` covers `Moved`, `Terminal` and `Missing`.
- `NoSkip`: `the_outbox_lists_in_order_after_a_number` (a resume after a number, in pages).
- The model's assumption that a waiting writer numbers after what the counter's holder committed: `a_waiting_append_numbers_after_the_counter_holder` (`crates/server/tests/pg_redis.rs`). An outside session holds the counter with one uncommitted entry, the store's append waits on it, and after the commit the numbers are 1, 2, 3. It is checked under both the default and a serializable session default. That an owner's numbers commit in order rests on that lock being held until commit.
- 36A: `each_principal_has_its_own_sequence`.
- 38A and `Suffix` under pruning: `pruning_removes_old_entries_and_keeps_the_count`, which also runs `pruning_takes_a_prefix_across_clock_skew` (Postgres, with stored times skewed by hand), in `crates/server/tests/outbox_prune.rs`. That file holds one test, in a binary of its own, since a prune is store-wide. `InMemoryStore` prunes by the same prefix rule; with one process's clock, no test can skew it.
- `EveryWriterCommits` and `ReaderCatchesUp`: unlinked.
- The lock order has no Rust test that can force the inverted interleaving. It rests on `EventRows::insert` being the only numbering site and on its callers' order.

Retire this model if the outbox moves to a log whose order the database gives directly (logical decoding), with its own model.
