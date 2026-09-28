# Run queue findings

Checked on 2026-09-28. `RunQueue.tla` is the Redis run queue, its workers and its reaper, and the run log they end.

## Model

Resources: the Redis runs list, the processing list, the per-run lease keys, and the run log.

Writers:
- **The producer:** `create_run` (`crates/server/src/http.rs:337`) stores the run as created and queued, then pushes it (`:354`, `RedisRunQueue::push`, `crates/server/src/queue.rs:102`).
- **Workers** (`Worker::work_one`, `crates/server/src/worker.rs:67`), `GOL_WORKERS` of them in the server process. Each one:
  - claims a run: one script moves it to processing and leases it (`CLAIM`, `queue.rs:41`; `claim`, `:130`);
  - runs the harness (`Claim::prepare`, `worker.rs:101`, and `Claim::execute`, `:114`), renewing the lease every heartbeat (`:145`, `RENEW`, `queue.rs:49`);
  - records the harness's events in one append (`Claim::record`, `worker.rs:130`, `RunStore::append_events`), which the store refuses once the log is terminal (`formal/runlog`);
  - acknowledges: one script removes the run from processing and releases the lease if it still holds it (`ACK`, `queue.rs:57`; `Claim::ack`, `worker.rs:140`).
- **The reaper** (`reap_forever`, `worker.rs:165`): one script moves every run in processing whose lease is gone back onto the runs list (`REAP`, `queue.rs:66`).

Each Redis command, each script and each append is one atomic step. A worker may crash at any point after its claim; its lease stays until it expires. A lease also expires under a live worker that misses its heartbeats. That is bounded by `MaxSlow`, and it is the case where two workers run one run.

`Design = "new"` is C4. `Design = "rpop"` is the queue before C4: a worker pops the run, with no processing list and no lease. `Design = "early"` acknowledges before recording.

`RunQueue.cfg` checks `Design = "new"` with `Workers = {"w1", "w2"}`, `Runs = {"r1"}`, `MaxCrashes = 1` and `MaxSlow = 1`.

## Properties

- `NoOrphan`: a pushed run that has not ended is on the runs list, in processing, or in a live worker's hands. No crash loses it.
- `AckAfterTerminal`: no run leaves processing before its log is terminal.
- `AtMostOneTerminal`: the store keeps one terminal event per run, even when two workers run it.
- `EveryRunEnds`: every run ends. This assumes weak fairness on the producer, each worker's own steps, the reaper, and the expiry of a dead worker's lease. There is no fairness on crashes or slow expiries.

## TLA+ Findings

`Design = "new"`, from `formal/runqueue`, as `scripts/verify-tla.sh` runs it:

```text
java -XX:+UseParallelGC -jar ~/.local/tla/tla2tools.jar -workers auto -lncheck final -config RunQueue.cfg RunQueue.tla
```

TLC2 Version 2.19 of 08 August 2024, from tla2tools v1.7.4, which `scripts/install-tla.sh` pins. The command has no `-deadlock`, so TLC checked deadlock. Every invariant and property passed, and no state is a deadlock: 357 states generated, 165 distinct, depth 15. `Done` is the only stuttering step.

Larger bounds:

| Constants | Generated | Distinct | Depth | Error |
|---|---|---|---|---|
| `Runs = {"r1", "r2"}`, `MaxCrashes = 2` | 16,030 | 5,019 | 24 | none |
| `Workers = {"w1", "w2", "w3"}` | 768 | 321 | 15 | none |
| three workers, two runs, `MaxCrashes = 2`, `MaxSlow = 2` (nightly, `-workers 1`) | 302,051 | 67,427 | 29 | none |

For the nightly row, TLC 2.19 finishes the check with more than one worker, then throws a division by zero in its closing statistics. With one worker it completes cleanly.

Negative controls, each on a copy of the config with `-workers 1`:

- `Design = "rpop"` violates `NoOrphan` in a 4-state trace: the run is pushed, a worker pops it, and the worker crashes. The run is on no list and in no hands.
- `Design = "early"` violates `AckAfterTerminal` in a 4-state trace: the run is pushed, claimed, and acknowledged before its record.
- On `Design = "new"`, `OneWorkerPerRun` (a control that no two workers hold one run) is violated in a 6-state trace. The run is claimed, its holder's lease expires while it is slow, the reaper hands the run back, and a second worker claims it. So the model reaches the case `AtMostOneTerminal` guards.

## Mapping

Tests in `crates/server/tests/queue_worker.rs`, against Redis and Postgres:
- `NoOrphan`: `worker_crash_after_claim_redelivered`. A worker claims and dies, its lease runs out, the reaper hands the run back, and another worker ends it with one `RunCompleted`.
- `AtMostOneTerminal`: `two_workers_one_terminal`, the `OneWorkerPerRun` trace forced. A's lease runs out after A loads the run; B claims, runs and acknowledges; A runs Jev too (two Jev calls). A's record is refused (`Append::Terminal`), one terminal event stays, and both acknowledgements leave the queue empty. The store's refusal is `formal/runlog`'s `Appends`.
- `AckAfterTerminal`: `Worker::work_one` acknowledges only after `Claim::record` returns, or when `Claim::prepare` finds the log already ended. `an_ended_run_is_acknowledged_without_jev` is the second case (owner decision 3A): no Jev call. The first case rests on reading the code: no test can crash a worker between record and ack.
- The claim is one step: `the_reaper_leaves_a_live_lease`.
- The heartbeat keeps a live worker's lease: `the_heartbeat_keeps_a_slow_run_leased`. The reaper, run all through a 1.5 s Jev call with a 400 ms lease, hands nothing back.
- `redis_run_records_created_and_queued`: the assertion in `create_run_writes_postgres_and_enqueues_redis` (`crates/server/tests/pg_redis.rs`) that the run is stored as created and queued before the push.
- `EveryRunEnds`: unlinked.

Retire this model if the queue moves to a broker whose API gives leased delivery and acknowledgement directly, such as Redis streams consumer groups with `XAUTOCLAIM`, with its own model.
