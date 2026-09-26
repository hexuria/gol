# Run log findings

Checked on 2026-09-25; the failer was added and checked on 2026-09-26. `RunLog.tla` is one stored run and the writers that race on its event log.

## Model

Writers: the Jev driver in `create_run`, completers (`accept_subscription_completion`, or `open_turn` with a gateway completion), failers (`fail_turn`, and `open_turn` when the proxy fails), a user message stored by another writer, and a redelivered `put_run`. The Box sandbox is `up` or `gone`, and `destroy` can fail.

`Design = "old"` is the store before this change:

- `put_run` overwrote the in-memory row.
- `create_run` finished with `replace_run(<<message>> \o jev)`.
- A completer appended its completion, then destroyed the sandbox, and on a failed destroy wrote its pre-completion snapshot back with `replace_run`.

`Design = "new"`:

- `put_run` inserts once.
- `append_events` is the only write after that. In one atomic step it refuses when the stored log already holds a terminal event (`Append::Terminal`).
- A completer destroys the sandbox first and appends only after that succeeds. A failed destroy writes nothing.

`RunLog.cfg` checks `Design = "new"` with `Completers = {"c1", "c2"}` and `Failers = {}`, so its state space is the one recorded before the failer was added.

A failer (`FailCheck`, `FailDestroy`, `FailAppend`) ends a turn with `RunFailed`, in the completer's order: it checks the log is not terminal, destroys the sandbox (nothing to do when it is already gone; a failed destroy writes nothing and leaves the turn open), then appends, and the store refuses the append once the log is terminal. Unlike a completion it does not require the sandbox to be up. `RunLogFail.tla` extends `RunLog` so that `RunLogFail.cfg` can check `Completers = {"c1"}` with `Failers = {"f1"}`.

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

`RunLogFail.cfg`, same command with `-config RunLogFail.cfg RunLogFail.tla`: every invariant and property passed, no deadlock, 1,011 states generated, 434 distinct, depth 10. With `Completers = {"c1", "c2"}` and `Failers = {"f1"}`: 9,520 generated, 3,474 distinct, depth 13, no error. `RunLog.cfg` with `Failers = {}` is unchanged: 1,461 generated, 616 distinct, depth 10.

Negative controls for the failer, each on a copy of the model:

- A failer that appends before it destroys: `CompletedHasNoSandbox` fails at depth 4. The failer checks, appends `RunFailed`, and the log is terminal while the sandbox is still up.
- A failer whose append the store does not refuse: `AtMostOneTerminal` fails at depth 7. The completer and the failer both check an open turn, the completer appends, then the failer appends a second terminal event.
- An invariant saying `f1` never lands is violated at depth 5, so the failer's checks are not vacuous.

## Simplification

- `replace_run` is gone. It was the one way to shrink the log.
- The rollback on a failed destroy is gone. Destroy-then-append leaves nothing to take back.
- The duplicate-completion guard is the store's refusal, not a read-then-append in the caller. The caller's read still gives an early `Conflict`, but correctness does not depend on it.

## Implementation Mapping

| Model | Rust |
| --- | --- |
| `Appends(id)` | `RunStore::append_events -> Append` (`InMemoryStore` under its lock, `PostgresStore` in one `update ... where not exists` statement) |
| `Terminal` | `store::is_terminal`: `RunCompleted`, `RunFailed`, `RunCancelled`, `RunExpired`, the terminal `DispatchPhase`s |
| `Reput` | `put_run`, insert-once in both stores |
| `JevReturns` | `create_run` appends the driver's events, then folds the stored log |
| `NewDestroy`, `NewAppend` | `open_turn` and `accept_subscription_completion`: `sandbox.destroy`, then `record_completion` |
| `FailCheck`, `FailDestroy`, `FailAppend` | `fail_turn` (`POST /v1/coworker/turns/{id}/fail`): `check_open_turn`, `sandbox.destroy` when the sandbox exists, then `record_completion` of `RunFailed`; `open_turn` when the gateway proxy fails: `sandbox.destroy`, then `RunFailed` only if it succeeded. A failed provision is a failer with no sandbox to destroy. |
| `JevReturns`, error path | `create_run` appends the driver's events and `RunFailed` after a decider error, and `RunFailed` alone after a failed queue push: the request's own writer, refused once the log is terminal |

Regression tests from the traces are in `crates/server/tests/inference.rs` (`a_failed_destroy_keeps_a_message_stored_during_the_turn`, `a_completion_racing_another_completion_is_a_conflict`, `the_store_log_grows_and_ends_at_the_first_terminal_event`) and `crates/server/tests/pg_redis.rs`.

The failer's traces are tests in `crates/server/tests/inference.rs`: `fail_races_completion_exactly_one_terminal` (the `AtMostOneTerminal` trace, forced with `WriteAfterSnapshot` in both orders), `fail_turn_is_terminal_and_destroys_sandbox` and `a_proxy_failure_with_a_failed_destroy_leaves_the_turn_open` (`CompletedHasNoSandbox`), `fail_on_a_finished_or_gateway_turn_is_a_conflict`, `provision_failure_run_failed`, `proxy_failure_run_failed` and `fail_endpoint_is_terminal_and_destroys_sandbox`; on Postgres, `fail_turn_is_terminal_in_postgres` and `provision_failure_run_failed_in_postgres` in `crates/server/tests/pg_redis.rs`.
