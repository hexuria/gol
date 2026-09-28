# Run queue findings

Checked on 2026-09-28. `RunQueue.tla` is the Redis run queue, its workers and its reaper, and the run log they end.

## Model

Resources: the Redis runs list, the processing list, the per-run lease keys, and the run log.

Writers:
- **The producer:** `create_run` (`crates/server/src/http.rs:341`) stores the run as created and queued, then pushes it (`:364`, `RedisRunQueue::push`, `crates/server/src/queue.rs:167`). Both happen in one blocking task, so a client going away cannot separate them.
- **Workers** (`Worker::work_one`, `crates/server/src/worker.rs:186`), `GOL_WORKERS` of them in the server process. Each one:
  - claims a run: one script moves it to processing and leases it with `SET NX PX` (`CLAIM`, `queue.rs:62`; `claim`, `:195`). A run already leased (queued again while held) is dropped from the list instead;
  - loads the run (`Claim::prepare`, `worker.rs:243`). An open run counts a start (`START`, `queue.rs:73`). A claim that cannot load or start its run releases it: one script, which acts only while the claim still holds the lease, moves it off processing, onto the back of the runs list, and deletes the lease (`RELEASE`, `queue.rs:81`);
  - runs the harness (`Open::execute`, `worker.rs:310`), renewing the lease every heartbeat (`:371`; `RENEW`, `queue.rs:91`);
  - records the harness's events in one append (`Executed::record`, `worker.rs:338`; `RunStore::append_events`), which the store refuses once the log is terminal;
  - acknowledges: one script removes the run from processing, clears its start count and releases the lease if it still holds it (`ACK`, `queue.rs:100`; `Done::ack`, `worker.rs:360`).
  - Only a `Done` can acknowledge, and only `record`, or a `prepare` that finds nothing to run, makes one. So the ack of a stored run comes after its terminal event by construction. A run that is not stored is acknowledged without one, and dropped with an error.
- **The reaper** (`reap_forever`, `worker.rs:394`): one script moves every run in processing whose lease is gone back to the front of the runs list (`REAP`, `queue.rs:111`).

Each Redis command, each script and each append is one atomic step. `Push` is one step: the store and the push run in one blocking task.

A worker may crash at any point after its claim; its lease stays until it expires. A lease also expires under a live worker that misses its heartbeats. That is bounded by `MaxSlow`, and it is the case where two workers run one run.

A worker's load may fail, at most `MaxReleases` times; it then releases its claim.

`Design = "new"` is C4. The negative controls:
- `Design = "rpop"` pops the run, with no processing list and no lease: the control for `NoOrphan`.
- `Design = "early"` acknowledges before recording: the control for `AckAfterTerminal`.
- `Design = "loose"` releases without holding the lease, as the release script of 9511c0e did: a second control for `NoOrphan`.

In `Design = "new"` no queued run holds a lease (`WaitingUnleased`, checked by `RunQueue.cfg`; it fails in `loose`), so the `SET NX` branch of `Claim` is only taken in `loose`. The code needs it for an id queued twice, which a model of lists as sets cannot express; `a_run_queued_twice_is_claimed_once` tests it.

`RunQueue.cfg` checks `Design = "new"` with `Workers = {"w1", "w2"}`, `Runs = {"r1"}`, `MaxCrashes = 1`, `MaxSlow = 1` and `MaxReleases = 1`.

## Assumptions

The model's claims rest on these; each is outside what it checks.
- **Push:**
  - The server process does not die between storing a run and pushing it. Such a crash leaves a stored, queued run that no worker sees (not modelled; see Recommended in the PR).
  - Cancellation cannot separate the two steps: they are one blocking task.
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
- `EveryRunEnds`: every run ends. This assumes weak fairness on the producer, each worker's own steps, the reaper, and the expiry of a dead worker's lease. There is no fairness on crashes or slow expiries.

## TLA+ Findings

`Design = "new"`, from `formal/runqueue`, as `scripts/verify-tla.sh` runs it:

```text
java -XX:+UseParallelGC -jar ~/.local/tla/tla2tools.jar -workers auto -lncheck final -config RunQueue.cfg RunQueue.tla
```

TLC2 Version 2.19 of 08 August 2024, from tla2tools v1.7.4, which `scripts/install-tla.sh` pins. The command has no `-deadlock`, so TLC checked deadlock. Every invariant and property passed, and no state is a deadlock: 793 states generated, 337 distinct, depth 17. `Done` is the only stuttering step.

Larger bounds:

| Constants | Generated | Distinct | Depth | Error |
|---|---|---|---|---|
| `Runs = {"r1", "r2"}`, `MaxCrashes = 2`, `MaxReleases = 2` | 53,680 | 15,223 | 28 | none |
| `Workers = {"w1", "w2", "w3"}` | 1,709 | 652 | 17 | none |
| three workers, two runs, `MaxCrashes = 2`, `MaxSlow = 2`, `MaxReleases = 2` (nightly) | 1,026,263 | 205,110 | 33 | none |

About the nightly row:
- At those constants, TLC 2.19's default fingerprint set throws a division by zero in `AbstractChecker.reportSuccess`. That is its collision estimate, after the check has finished. This happens with any worker count.
- With `-Dtlc2.tool.fp.FPSet.impl=tlc2.tool.fp.OffHeapDiskFPSet` it completes: 6 of 6 runs here. The nightly job (`.github/workflows/nightly.yml`, `tlc-large`) uses that flag.

Negative controls, each on a copy of the config with `-workers 1`:

- `Design = "rpop"` violates `NoOrphan` in a 4-state trace: the run is pushed, a worker pops it, and the worker crashes. The run is on no list and in no hands.
- `Design = "early"` violates `AckAfterTerminal` in a 4-state trace: the run is pushed, claimed, and acknowledged before its record.
- `Design = "loose"` violates `NoOrphan` in a 9-state trace:
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
- **`EveryRunEnds`:** unlinked.

Retire this model if the queue moves to a broker whose API gives leased delivery and acknowledgement directly, such as Redis streams consumer groups with `XAUTOCLAIM`, with its own model.
