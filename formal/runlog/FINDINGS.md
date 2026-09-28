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
| `Appends(id)` | `RunStore::append_events -> Append`. `InMemoryStore` does it under its lock. `PostgresStore` (C2) does it in one transaction on a pooled connection: `select ... for update` on the `runs` row, then the terminal check over `run_events`, then the insert. The row lock makes concurrent appends to one run take turns: the transaction is read committed whatever the session default, so each statement after the lock sees what the lock's last holder committed. A partial unique index keeps one terminal row per run. Tests: `sixteen_racing_terminal_appends_one_wins` and `a_waiting_append_sees_what_the_lock_holder_committed` (both orders forced, also under a serializable default) in `crates/server/tests/pg_redis.rs`. |
| `Terminal` | `store::is_terminal`: `RunCompleted`, `RunFailed`, `RunCancelled`, `RunExpired`, the terminal `DispatchPhase`s |
| `Reput` | `put_run`, insert-once in both stores (in Postgres, the `runs` row and its first `run_events` rows in one transaction, written only when the row is new) |
| `JevReturns` | `create_run` appends the driver's events, then folds the stored log |
| `NewDestroy`, `NewAppend` | `open_turn` and `accept_subscription_completion`: `sandbox.destroy`, then `record_completion` |
| `FailCheck`, `FailDestroy`, `FailAppend` | `fail_turn` (`POST /v1/coworker/turns/{id}/fail`): `store.run` and `check_open_turn`, `sandbox.absent`, `sandbox.destroy` unless the host confirmed it absent, then `record_completion` of `RunFailed` |
| (not modelled) | `open_turn`'s own failure paths append `RunFailed` without a check: after a failed provision only when the host confirms no sandbox is left (`SandboxHost::absent` answers `Ok(true)`), and after a failed gateway proxy call only once `sandbox.destroy` succeeded. They run before `open_turn` returns the run id, so no completer or failer can race them; only a late message or a redelivered `put_run` can, and the store's refusal covers those. Unit tests: `provision_failure_run_failed`, `a_provision_failure_that_leaves_a_sandbox_keeps_the_turn_open`, `a_provision_whose_container_is_gone_ends_the_turn`, `an_unreachable_docker_does_not_end_a_failed_provision`, `proxy_failure_run_failed`, `a_proxy_failure_with_a_failed_destroy_leaves_the_turn_open`. |
| `JevReturns`, error path | `create_run` appends the driver's events and `RunFailed` after a decider error, and `RunFailed` alone after a failed queue push: the request's own writer, refused once the log is terminal |

Regression tests from the traces are in `crates/server/tests/inference.rs` (`a_failed_destroy_keeps_a_message_stored_during_the_turn`, `a_completion_racing_another_completion_is_a_conflict`, `the_store_log_grows_and_ends_at_the_first_terminal_event`) and `crates/server/tests/pg_redis.rs`.

The failer's traces are tests in `crates/server/tests/inference.rs`: `fail_races_completion_exactly_one_terminal` (the `AtMostOneTerminal` trace, forced with `WriteAfterSnapshot` in both orders), `fail_turn_is_terminal_and_destroys_sandbox` and `a_failed_destroy_keeps_a_failed_turn_open` and `an_unreachable_docker_does_not_end_a_failed_turn` (`CompletedHasNoSandbox`), `fail_on_a_finished_or_gateway_turn_is_a_conflict`, `fail_on_a_run_that_is_not_an_open_turn_is_a_conflict` (`FailCheck`), `provision_failure_run_failed`, `proxy_failure_run_failed` and `fail_endpoint_is_terminal_and_destroys_sandbox`; on Postgres, `fail_turn_is_terminal_in_postgres` and `provision_failure_run_failed_in_postgres` in `crates/server/tests/pg_redis.rs`.
