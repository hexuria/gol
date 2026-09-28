# Run queue findings

Checked on 2026-09-28, and again for C6 the same day. `RunQueue.tla` is the Redis run queue, its producer, its workers, its reaper and sweep, and the run log they end.

## Model

Resources: the Redis runs list, the processing list, the pending set, the per-run lease keys, and the run log.

Writers:
- **The producer:** `create_run` (`crates/server/src/http.rs`), in one blocking task, so a client going away cannot separate its steps:
  - marks the run pending: one script adds it to the pending set, scored by the Redis clock (`:350`; `PEND`, `crates/server/src/queue.rs:80`; `pend`, `:215`). With Redis down it fails here, and nothing is stored;
  - stores the run as created and queued (`:352`);
  - pushes it: one script queues it and takes it off pending (`:369`; `PUSH`, `queue.rs:87`; `push`, `:226`).
- **Workers** (`Worker::work_one`, `crates/server/src/worker.rs:186`), `GOL_WORKERS` of them in the server process. Each one:
  - claims a run: one script moves it to processing and leases it with `SET NX PX` (`CLAIM`, `queue.rs:103`; `claim`, `:303`). A run already leased (queued again while held) is dropped from the list instead;
  - loads the run (`Claim::prepare`, `worker.rs:243`). An open run counts a start (`START`, `queue.rs:114`). A claim that cannot load or start its run releases it: one script, which acts only while the claim still holds the lease, moves it off processing, onto the back of the runs list, and deletes the lease (`RELEASE`, `queue.rs:122`);
  - runs the harness (`Open::execute`, `worker.rs:310`), renewing the lease every heartbeat (`:371`; `RENEW`, `queue.rs:132`);
  - stores the run as it goes (Phase 1.5b, `Worker::store_as_it_goes` and `run_from` in `worker.rs`): the scheduling ladder, then each step's events at its boundary, then the tail that ends the run, each with `RunStore::append_events_after`, which the store refuses once the log is terminal or no longer as long as the worker saw. The run log's side of this is `formal/runlog` (`WAppend`); to this model the steps before the tail are no write, and the tail is `Record`;
  - when other writers keep moving the log (more than three reloads), stores no end and releases the run instead of acknowledging it (`Done::ack` on `Append::Moved`): the `Release` action, taken from `ran`;
  - acknowledges: one script removes the run from processing, clears its start count and releases the lease if it still holds it (`ACK`, `queue.rs:141`; `Done::ack`, `worker.rs:360`).
  - Only a `Done` can acknowledge, and only `record`, or a `prepare` that finds nothing to run, makes one. So the ack of a stored run comes after its terminal event by construction. A run that is not stored is acknowledged without one, and dropped with an error.
- **The reaper** (`reap_forever`, `worker.rs:394`): one script moves every run in processing whose lease is gone back to the front of the runs list (`REAP`, `queue.rs:152`).
- **The sweep** (`sweep`, `worker.rs:419`), in the reaper's loop: for each run pending for at least `sweep_after` (60 s; `PENDING_FOR`, `queue.rs:94`), it loads the run. A run still created and queued is pushed with the producer's script; a run that has started or ended is taken off pending (`unpend`, `queue.rs:270`). A run not in the store yet stays pending, since its put may still be in flight, until it has been pending for `forget_after` (24 h), when it is dropped.

Each Redis command, each script and each append is one atomic step. The producer's pend, store and push are three steps, and the server process may die between them (`ProducerDies`, at most `MaxProducerDeaths` times).

A worker may crash at any point after its claim; its lease stays until it expires. A lease also expires under a live worker that misses its heartbeats. That is bounded by `MaxSlow`, and it is the case where two workers run one run.

A worker's load may fail, or (Phase 1.5b) its run may keep moving under it; either way it releases its claim, at most `MaxReleases` times in all.

`Design = "new"` is C4. The negative controls:
- `Design = "rpop"` pops the run, with no processing list and no lease: the control for `NoOrphan`.
- `Design = "early"` acknowledges before recording: the control for `AckAfterTerminal`.
- `Design = "loose"` releases without holding the lease, as the release script of 9511c0e did: a second control for `NoOrphan`.
- `Design = "c4"` is the producer before C6: it stores and then pushes, with no pend and no sweep. The control for `NoStrandedRun`.
- `Design = "noSweep"` pends but has no sweep: the control for `EveryRunEnds` under a producer death.

In `Design = "new"` no queued run holds a lease (`WaitingUnleased`, checked by `RunQueue.cfg`; it fails in `loose`), so the `SET NX` branch of `Claim` is only taken in `loose`. The code needs it for an id queued twice, which a model of lists as sets cannot express; `a_run_queued_twice_is_claimed_once` tests it.

`RunQueue.cfg` checks `Design = "new"` with `Workers = {"w1", "w2"}`, `Runs = {"r1"}`, `MaxCrashes = 1`, `MaxSlow = 1`, `MaxReleases = 1` and `MaxProducerDeaths = 0`. `RunQueueCrash.cfg` (with `RunQueueCrash.tla`, which only extends `RunQueue`, so `verify-tla.sh` pairs them) checks the same with `MaxProducerDeaths = 1`. Keeping deaths at 0 in `RunQueue.cfg` keeps its state count near the one recorded before C6.

## Assumptions

The model's claims rest on these; each is outside what it checks.
- **Push:**
  - The grace: the sweep takes a pending run only after its producer has pushed it or died. `sweep_after` is 60 s. The pend and the push give up within 5 s (`REDIS_TIMEOUT`); the put can wait 30 s for a pooled connection and has no statement timeout, which is why a run not yet in the store is kept rather than dropped at the grace. A producer whose put is visible but whose push is later than that is not modelled; the sweep then pushes a run its producer pushes too. An id queued twice is claimed once, and a claim of an ended run acknowledges it without running it (`a_sweep_between_store_and_push_runs_the_run_once`). A put not yet visible is not taken for dead at the grace: the sweep keeps the entry (`a_pending_run_not_yet_stored_is_kept_until_forgotten`). `Sweep` drops a never-stored run only once its producer is dead, which the code takes to be true after `forget_after` (24 h): a put still in flight after 24 h is outside this model.
  - Cancellation cannot separate the two steps: they are one blocking task.
  - A second producer for the same run (the owned spawner retrying a child whose id it derives from the request, Phase 1.2a) pends an id that already exists. `PEND` adds with `NX`, so an existing entry keeps its score. After such a put finds the run already stored, the spawner takes the id off pending if it is queued, claimed or past waiting (`settle_existing`, `crates/server/src/spawner.rs`), so no sweep pushes it again. The model's lists are sets, so it cannot express the duplicate this prevents; `a_redelivered_parent_starts_one_child` tests it. A child's failed push leaves the run pending for the sweep instead of ending it (`a_child_whose_push_failed_is_left_to_the_sweep`).
  - A store whose put fails with an unknown outcome gets a best-effort `RunFailed` append, so a put that did commit is not left queued and unpushed.
- **Store:** every queued run is in the store the workers read. The queue refuses to start without `GOL_DATABASE_URL`, and one Redis serves one deployment. A worker drops a run it cannot find, with an error.
- **Record:** a record eventually succeeds, or the run is failed.
  - A run started more than `max_deliveries` times (5, owner decision of 2026-09-28) without ending is recorded `RunFailed` and acknowledged.
  - Only a claim that opens the run counts. A claim that cannot load the run hands it back to the back of the queue and does not count, so a store outage uses no starts.
  - A run that can never be loaded is retried for as long as that lasts, with backoff and an error each time. Released to the back, it does not hold up the runs queued behind it.
  - The model bounds crashes instead.
- **Workers:** a crashed worker comes back. `work_forever` catches a panic, lets that claim expire, backs off and goes on. A process that dies is restarted by its supervisor.
- **Redis:** Redis keeps what it is given: standalone, `noeviction`, and AOF persistence. Redis answers or times out: a connection's handshake, and every read and write, has a 5 s deadline.
- **AtMostOneTerminal** is `formal/runlog`'s property, and that model owns it. `Record` here takes the store's refusal as given, so this model links to it and does not check it again.

## Properties

- `NoOrphan`: a pushed run that has not ended is on the runs list, in processing, or in a live worker's hands. No crash loses it.
- `AckAfterTerminal`: no run leaves processing before its log is terminal. In Design "new" it holds by construction, as it does in the code's types; the "early" control shows the model can see it fail.
- `AtMostOneTerminal`: the store keeps one terminal event per run, even when two workers run it. This is a link to `formal/runlog`, not a second owner.
- `NoStrandedRun`: a stored run that has not ended is pending, on the runs list, in processing, or in a worker's hands. It holds because the pend comes before the store; `c4` shows it fail without the pend.
- `EveryRunEnds`: every stored run ends, and every producer finishes or dies. This assumes weak fairness on the producer's steps, each worker's own steps, the reaper, the sweep, and the expiry of a dead worker's lease. There is no fairness on crashes, producer deaths or slow expiries.

## TLA+ Findings

`Design = "new"`, from `formal/runqueue`, as `scripts/verify-tla.sh` runs it:

```text
java -XX:+UseParallelGC -jar ~/.local/tla/tla2tools.jar -workers auto -lncheck final -config RunQueue.cfg RunQueue.tla
```

TLC2 Version 2.19 of 08 August 2024, from tla2tools v1.7.4, which `scripts/install-tla.sh` pins. The command has no `-deadlock`, so TLC checked deadlock. Every invariant and property passed, and no state is a deadlock. `Done` is the only stuttering step.
- `RunQueue.cfg`: 863 states generated, 339 distinct, depth 19. Before Phase 1.5b let `Release` act after `Run` too: 795 generated, 339 distinct, depth 19; before C6: 793, 337, depth 17.
- `RunQueueCrash.cfg`: 1,727 states generated, 678 distinct, depth 20 (before Phase 1.5b: 1,591 generated, 678 distinct).

Checked again on 2026-09-29 for Phase 1.5b. The only change is that `Release` is enabled from `ran` as well as `claimed`, so no new state is reachable, only new transitions between the same states. Every invariant and property still passes. Load-failure releases and hand-backs share `MaxReleases`, so at the PR constants a release after `Run` is explored within the same budget of two.

Larger bounds; the first two rows are `RunQueueCrash.cfg` (`MaxProducerDeaths = 1`) with the constants changed:

| Constants | Generated | Distinct | Depth | Error |
|---|---|---|---|---|
| `Runs = {"r1", "r2"}`, `MaxCrashes = 2`, `MaxReleases = 2` | 198,606 | 55,677 | 33 | none |
| `Workers = {"w1", "w2", "w3"}` | 3,423 | 1,308 | 20 | none |
| three workers, two runs, `MaxCrashes = 2`, `MaxSlow = 2`, `MaxReleases = 2`, `MaxProducerDeaths = 0` (nightly; 205,110 distinct before C6) | 1,137,499 | 227,738 | 37 | none |
| the same with `MaxProducerDeaths = 1` (nightly, a second step) | 3,424,133 | 683,214 | 38 | none |

About the nightly rows:
- At those constants, TLC 2.19's default fingerprint set throws a division by zero in `AbstractChecker.reportSuccess`. That is its collision estimate, after the check has finished. This happens with any worker count.
- With `-Dtlc2.tool.fp.FPSet.impl=tlc2.tool.fp.OffHeapDiskFPSet` it completes: 6 of 6 runs here. The nightly job (`.github/workflows/nightly.yml`, `tlc-large`) uses that flag.

Negative controls, each on a copy of the config with `-workers 1`:

Each trace below starts with the producer's pend, store and push, except in `c4` and `noSweep`.
- `Design = "c4"` (on `RunQueueCrash.cfg`) violates `NoStrandedRun` in a 2-state trace: the run is stored, and it is in no set anyone acts on until its push. A death there strands it for good.
- `Design = "noSweep"` (on `RunQueueCrash.cfg`) deadlocks in a 3-state trace: the run is pended and its producer dies. Nothing takes it off pending, so `Done` is never enabled.
- `Design = "rpop"` violates `NoOrphan` in a 6-state trace: the run is pushed, a worker pops it, and the worker crashes. The run is on no list and in no hands.
- `Design = "early"` violates `AckAfterTerminal` in a 6-state trace: the run is pushed, claimed, and acknowledged before its record.
- `Design = "loose"` violates `WaitingUnleased` in a 9-state trace, and, with that invariant left out, `NoOrphan` in an 11-state trace:
  1. Run 1 is pushed, and w1 claims it.
  2. w1's lease expires while w1 is slow, the reaper hands the run back, and w2 claims it.
  3. w1's load fails, and its release takes w2's entry off processing and queues the run again.
  4. A claim finds w2's lease and drops the entry.
  5. w2 crashes. The run is on no list, holds no lease, and never ended.
- On `Design = "new"`, `OneWorkerPerRun` (a control that no two workers hold one run) is violated in a 6-state trace. The run is claimed, its holder's lease expires while it is slow, the reaper hands the run back, and a second worker claims it. So the model reaches the case `AtMostOneTerminal` guards.

## Mapping

Tests in `crates/server/tests/queue_worker.rs`, against Redis and Postgres (the first two also against `InMemoryStore`):
- **`NoOrphan`:**
  - `worker_crash_after_claim_redelivered`: a worker claims and dies, its lease runs out, the reaper hands the run back, and another worker ends it with one `RunCompleted`.
  - `a_failed_record_is_not_acknowledged`: a worker whose record fails leaves the run in processing under its lease, and the reaper hands it back.
  - `a_late_release_leaves_the_new_holders_claim`: the `loose` trace forced. A's load outlasts its lease, B claims and opens the run, and A's load then fails. A's release leaves B's claim alone, and when B dies the reaper hands the run back.
- **`AtMostOneTerminal`**, the `OneWorkerPerRun` trace forced: `two_workers_one_terminal`.
  - A's lease runs out after A loads the run.
  - B claims, runs and acknowledges.
  - A runs Jev too, so there are two Jev calls.
  - A's record is refused (`Append::Terminal`), one terminal event stays, and both acknowledgements leave the queue empty.
- **`AckAfterTerminal`:** the claim's types (`Done` alone acknowledges) and `a_failed_record_is_not_acknowledged`.
  - `an_ended_run_is_acknowledged_without_jev` covers the found-ended path (owner decision 3A).
- **The start cap:** `a_run_started_too_often_is_failed`, on both stores. It checks the boundary (with a cap of 2, the second start still runs, and the third is failed) and that the ack clears the count. A store outage uses no starts: `a_load_error_releases_the_claim_without_counting`. A run that can never load does not hold up the queue: `an_unloadable_run_does_not_hold_up_the_queue`.
- **The claim takes the lease with the run:** `the_reaper_leaves_a_live_lease` shows a claimed run is leased. That the two happen in one step rests on reading `CLAIM`: no test can interleave inside a script. A run queued twice is claimed once: `a_run_queued_twice_is_claimed_once`.
- **The heartbeat:** `the_heartbeat_keeps_a_slow_run_leased`. The reaper, run all through a 1.5 s Jev call with a 400 ms lease, hands nothing back.
- **Malformed entries:** `a_malformed_entry_is_dropped`.
- **The producer's records:** `redis_run_records_created_and_queued` is the assertion in `create_run_writes_postgres_and_enqueues_redis` (`crates/server/tests/pg_redis.rs`).
- **`NoStrandedRun`** and the sweep:
  - `a_producer_that_dies_after_the_store_leaves_the_run_to_the_sweep` (`pg_redis.rs`): the real `create_run` dies after its store (the put panics once the run is in). The run is pending and off the queue, and the sweep queues it.
  - `a_run_stored_but_not_pushed_is_queued_by_the_sweep` (`queue_worker.rs`): the sweep pushes such a run, and a worker ends it.
  - `redis_down_stores_no_run` (`pg_redis.rs`): the pend comes before the store, so with Redis down nothing is stored.
  - The sweep's other cases: `a_pending_run_not_yet_stored_is_kept_until_forgotten`, `a_redis_error_on_one_run_does_not_stop_the_sweep`, `a_pending_run_that_was_never_stored_is_dropped`, `a_pending_run_that_moved_on_is_not_pushed` (started, or ended) and `a_pending_run_inside_its_grace_is_left`; the push's `a_push_clears_the_pending_entry`; and a push that fails after the store, `a_push_that_fails_after_the_store_ends_the_run` (`pg_redis.rs`) and `redis_push_failure_does_not_run_the_harness` (`http_run.rs`).
  - Beyond the grace assumption: `a_sweep_between_store_and_push_runs_the_run_once`, the sweep forced into the producer's gap. The run is pushed twice and runs once.
- **`EveryRunEnds`:** unlinked.

Retire this model if the queue moves to a broker whose API gives leased delivery and acknowledgement directly, such as Redis streams consumer groups with `XAUTOCLAIM`, with its own model.
