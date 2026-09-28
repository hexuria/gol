# Run log findings

Checked on 2026-09-25; the failer was added and checked on 2026-09-26. `RunLog.tla` is one stored run and the writers that race on its event log.

## Model

Writers: the Jev driver in `create_run`, completers (`accept_subscription_completion`, or `open_turn` with a gateway completion), failers (`fail_turn`), a user message stored by another writer, and a redelivered `put_run`. The Box sandbox is `up` or `gone`, and `destroy` can fail.

`Design = "old"` is the store before this change:

- `put_run` overwrote the in-memory row.
- `create_run` finished with `replace_run(<<message>> \o jev)`.
- A completer appended its completion, then destroyed the sandbox, and on a failed destroy wrote its pre-completion snapshot back with `replace_run`.

`Design = "new"`:

- `put_run` inserts once.
- `append_events` is the only write after that. In one atomic step it refuses when the stored log already holds a terminal event (`Append::Terminal`).
- A completer destroys the sandbox first and appends only after that succeeds. A failed destroy writes nothing.

`RunLog.cfg` checks `Design = "new"` with `Completers = {"c1", "c2"}` and `Failers = {}`, so its state space is the one recorded before the failer was added.

A failer (`FailCheck`, `FailDestroy`, `FailAppend`) ends a turn with `RunFailed`, in the completer's order: it checks the log is not terminal, destroys the sandbox (nothing to do when it is already gone; a failed destroy writes nothing and leaves the turn open), then appends, and the store refuses the append once the log is terminal. Unlike a completion it does not require the sandbox to be up. Its destroy can fail even when the sandbox is gone: `fail_turn` asks the host whether the sandbox is absent, then destroys, and a completer can remove the sandbox in between. A host that cannot say whether it is absent (an unreachable Docker daemon) is the same failed branch: `fail_turn` returns an error and writes nothing. `RunLogFail.tla` extends `RunLog` so that `RunLogFail.cfg` can check `Completers = {"c1"}` with `Failers = {"f1"}`.

## Properties

- `AckedDurable`: a write the caller was told succeeded is in the log.
- `AtMostOneTerminal`: one terminal event per run.
- `NothingAfterTerminal`: the terminal event is last.
- `CompletedHasNoSandbox`: a Box turn that a completer or a failer ended has no sandbox.
- `LogGrows`: `[][IsPrefix(log, log')]_log`. The log only grows, so a terminal run stays terminal.
- `WritersFinish`: under weak fairness on each writer, every started writer finishes.

## TLA+ Findings

Each invariant was checked on its own against `Design = "old"`. Each one fails. Shortest traces:

- `AckedDurable`: Jev stores its events, then a redelivered `put_run` overwrites the row. The same thing happens when `replace_run` runs after another writer's append, or when the rollback runs after it.
- `AtMostOneTerminal`: Jev's events end in a terminal event. A completer that read the log earlier then appends a second one.
- `NothingAfterTerminal`: a late user message is appended after Jev's terminal event.
- `CompletedHasNoSandbox`: the completion is appended while the sandbox is still up. A reader can see it, and it can then be taken back.

`Design = "new"`, from `formal/runlog`, as `scripts/verify-tla.sh` runs it:

```text
java -XX:+UseParallelGC -jar ~/.local/tla/tla2tools.jar -workers auto -lncheck final -config RunLog.cfg RunLog.tla
```

TLC2 Version 2.19 of 08 August 2024, from tla2tools v1.7.4, which `scripts/install-tla.sh` pins. The command has no `-deadlock`, so TLC checked deadlock. Every invariant and property passed, and no state is a deadlock: 1,461 states generated, 616 distinct, depth 10. With `Completers = {"c1", "c2", "c3"}`: 15,136 generated, 5,316 distinct, depth 13, no error. `Done` is the only stuttering step, so deadlock checking is not masked.

The checks are not vacuous: an invariant saying `c1` never lands in the log is violated, and so is one saying a late message is never refused.

`RunLogFail.cfg`, same command with `-config RunLogFail.cfg RunLogFail.tla`: every invariant and property passed, no deadlock, 1,049 states generated, 434 distinct, depth 10. With `Completers = {"c1", "c2"}` and `Failers = {"f1"}`: 9,982 generated, 3,474 distinct, depth 13, no error. `RunLog.cfg` with `Failers = {}` is unchanged: 1,461 generated, 616 distinct, depth 10.

Negative controls for the failer, each on a copy of the model with `RunLogFail.cfg` and `-workers 1` (the counterexample's length varies with more workers):

- A failer that appends before it destroys: `CompletedHasNoSandbox` fails in a 3-state trace. The failer checks, appends `RunFailed`, and the log is terminal while the sandbox is still up.
- A failer whose append the store does not refuse: `AtMostOneTerminal` fails in a 5-state trace. The failer checks an open turn, Jev's events land with their terminal event, then the failer destroys and appends a second terminal event.
- An invariant saying `f1` never lands is violated in a 4-state trace, so the failer's checks are not vacuous.

## Simplification

- `replace_run` is gone. It was the one way to shrink the log.
- The rollback on a failed destroy is gone. Destroy-then-append leaves nothing to take back.
- The duplicate-completion guard is the store's refusal, not a read-then-append in the caller. The caller's read still gives an early `Conflict`, but correctness does not depend on it.

## Implementation Mapping

| Model | Rust |
| --- | --- |
| `Appends(id)` | `RunStore::append_events -> Append`. `InMemoryStore` does it under its lock. `PostgresStore` (C2) does it in one transaction on a pooled connection: `select ... for update` on the `runs` row, then the terminal check over `run_events`, then the insert. The row lock makes concurrent appends to one run take turns: the transaction is read committed whatever the session default, so each statement after the lock sees what the lock's last holder committed. A partial unique index keeps one terminal row per run. Tests: `sixteen_racing_terminal_appends_one_wins` and `a_waiting_append_sees_what_the_lock_holder_committed` (forced: the lock holder commits a terminal or a plain event while the append waits; also under a serializable session default) in `crates/server/tests/pg_redis.rs`. |
| `Terminal` | `store::is_terminal`: `RunCompleted`, `RunFailed`, `RunCancelled`, `RunExpired`, the terminal `DispatchPhase`s |
| `Reput` | `put_run`, insert-once in both stores (in Postgres, the `runs` row and its first `run_events` rows in one transaction, written only when the row is new) |
| `JevReturns` | `create_run` appends the driver's events, then folds the stored log. A queue worker appends `RunFailed` alone for a run started too often (`Executed::record`, `crates/server/src/worker.rs`). |
| `WLoad`, `WAppend` | A queue worker storing a run step by step (Phase 1.5b): `Claim::prepare` loads the log. `Worker::run_from` appends the scheduling ladder, each step's events at its boundary (`run_until`'s `Boundary` callback), and the tail, each with `append_events_after`. `Append::Moved` makes `Worker::store_as_it_goes` reload and resume (`Driver::resume`), at most `RELOADS` (3) times, after which `Done::ack` releases the run. The model's `MaxReloads` is 1, to reach the give-up branch. `append_events_after` does its length check in the same atomic step as the terminal check: under the `InMemoryStore` lock, and in Postgres under the `runs` row lock, where the last `seq` is the length because rows are numbered from 1 with no gaps. |
| `NewDestroy`, `NewAppend` | `open_turn` and `accept_subscription_completion`: `sandbox.destroy`, then `record_completion` |
| `FailCheck`, `FailDestroy`, `FailAppend` | `fail_turn` (`POST /v1/coworker/turns/{id}/fail`): `store.run` and `check_open_turn`, `sandbox.absent`, `sandbox.destroy` unless the host confirmed it absent, then `record_completion` of `RunFailed` |
| (not modelled) | `open_turn`'s own failure paths append `RunFailed` without a check: after a failed provision only when the host confirms no sandbox is left (`SandboxHost::absent` answers `Ok(true)`), and after a failed gateway proxy call only once `sandbox.destroy` succeeded. They run before `open_turn` returns the run id, so no completer or failer can race them; only a late message or a redelivered `put_run` can, and the store's refusal covers those. Unit tests: `provision_failure_run_failed`, `a_provision_failure_that_leaves_a_sandbox_keeps_the_turn_open`, `a_provision_whose_container_is_gone_ends_the_turn`, `an_unreachable_docker_does_not_end_a_failed_provision`, `proxy_failure_run_failed`, `a_proxy_failure_with_a_failed_destroy_leaves_the_turn_open`. |
| `JevReturns`, error path | `create_run` appends the driver's events and `RunFailed` after a decider error, and `RunFailed` alone after a failed queue push: the request's own writer, refused once the log is terminal |

Regression tests from the traces are in `crates/server/tests/inference.rs` (`a_failed_destroy_keeps_a_message_stored_during_the_turn`, `a_completion_racing_another_completion_is_a_conflict`, `the_store_log_grows_and_ends_at_the_first_terminal_event`) and `crates/server/tests/pg_redis.rs`.

The failer's traces are tests in `crates/server/tests/inference.rs`: `fail_races_completion_exactly_one_terminal` (the `AtMostOneTerminal` trace, forced with `WriteAfterSnapshot` in both orders), `fail_turn_is_terminal_and_destroys_sandbox` and `a_failed_destroy_keeps_a_failed_turn_open` and `an_unreachable_docker_does_not_end_a_failed_turn` (`CompletedHasNoSandbox`), `fail_on_a_finished_or_gateway_turn_is_a_conflict`, `fail_on_a_run_that_is_not_an_open_turn_is_a_conflict` (`FailCheck`), `provision_failure_run_failed`, `proxy_failure_run_failed` and `fail_endpoint_is_terminal_and_destroys_sandbox`; on Postgres, `fail_turn_is_terminal_in_postgres` and `provision_failure_run_failed_in_postgres` in `crates/server/tests/pg_redis.rs`.

## Phase 1.5b: workers that store a run step by step

Checked on 2026-09-29. `RunLog.tla` gains `Workers`: queue workers that append a run's steps one at a time with a conditional append (`WAppend`). The append is refused once the log is terminal, and refused as `Moved` unless the log is as long as the worker last saw. On `Moved` the worker reloads, at most `MaxReloads` times, then gives the run back (`gaveup`). `Steps` is the number of appends a run takes, the last of which ends it. `create_run`'s Jev writer and the workers never share a run, so `jevPc` starts `absent` when `Workers` is not empty.

New invariant `StepsContiguous`: the workers' steps land once each and in order, so no step is appended twice and none by a worker that had not seen the step before it.

`RunLogWorkers.cfg` checks `Design = "new"` with `Workers = {"w1", "w2"}`, `Steps = 2`, `MaxReloads = 1` and the late-message writer, against `TypeOK`, `AckedDurable`, `AtMostOneTerminal`, `NothingAfterTerminal`, `StepsContiguous`, `LogGrows` and `WritersFinish`. Same command, `-config RunLogWorkers.cfg RunLogWorkers.tla`: no error, no deadlock, 724 states generated, 402 distinct, depth 10. With `MaxReloads = 2`: 798 generated, 446 distinct, depth 11, no error. `RunLog.cfg` and `RunLogFail.cfg` take `Workers = {}` and are unchanged: 616 and 434 distinct.

Negative control, on `RunLogWorkers.cfg` with `-workers 1`: `Design = "blind"`, the same workers with an append that does not check the length, violates `StepsContiguous` in a 5-state trace. Both workers load the log holding only the message, `w1` appends `s1`, and `w2` appends `s1` again. Witnesses: an invariant that no worker ever reloads is violated, and so is one that no worker ever gives up (at `MaxReloads = 1`; at 2 no worker gives up), so neither branch is vacuous.

Mapping: `StepsContiguous` and the blind trace are `racing_conditional_appends_one_lands` (sixteen appends that read the same log: one lands, fifteen are `Moved`, on both stores) and `a_late_message_mid_run_is_absorbed` (a forced write before the worker's step append: the append is `Moved`, the worker reloads, and the step lands once), in `crates/server/tests/store_as_you_go.rs`. The terminal refusal of a stale holder is `a_stale_holders_append_is_refused` and `two_workers_one_terminal` (`queue_worker.rs`). Resuming without repeating a stored step is `a_redelivered_run_does_not_repeat_recorded_steps`.

Scope of the Phase 1.5b model:
- Each worker in `Workers` runs the run once. `WritersFinish` says each of them finishes, but not that a run that keeps being handed back ends under unbounded redelivery. The queue side of that is `formal/runqueue`'s `EveryRunEnds`, with releases bounded by `MaxReleases`, and `max_deliveries` in the code, which is not modelled.
- A worker whose lease ran out stores nothing more. `run_from` renews the lease before every write: the ladder, each boundary, both failure writes, and the tail that ends the run. It also renews before `run_until`, which can perform an effect the log left pending before any boundary. When a renewal answers that another claim holds the lease, the worker stops, and its release does nothing. A renewal that fails with a Redis error does not stop it, as with the heartbeat. Tests: `a_worker_that_lost_its_lease_stops_at_the_next_boundary`, `a_worker_that_lost_its_lease_during_the_last_step_does_not_end_the_run` (lease lost during the Jev call whose completion or error would end the run), and `a_worker_without_its_lease_does_not_redo_a_pending_effect` (a delegation cut after its authorization is not started). The model does not have leases (`formal/runqueue` does), so it lets a stale worker keep appending. That is the weaker case, and the conditional append still keeps `StepsContiguous` in it.
- The forced tests inject another writer's append at a chosen point of one worker. Two live workers running one run are covered at the store level by `racing_conditional_appends_one_lands`, not end to end.
