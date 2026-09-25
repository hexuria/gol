# Run log findings

Checked on 2026-09-25. `RunLog.tla` is one stored run and the writers that race on its event log.

## Model

Writers: the Jev driver in `create_run`, completers (`accept_subscription_completion`, or `open_turn` with a gateway completion), a user message stored by another writer, and a redelivered `put_run`. The Box sandbox is `up` or `gone`, and `destroy` can fail.

`Design = "old"` is the store before this change:

- `put_run` overwrote the in-memory row.
- `create_run` finished with `replace_run(<<message>> \o jev)`.
- A completer appended its completion, then destroyed the sandbox, and on a failed destroy wrote its pre-completion snapshot back with `replace_run`.

`Design = "new"`:

- `put_run` inserts once.
- `append_events` is the only write after that. In one atomic step it refuses when the stored log already holds a terminal event (`Append::Terminal`).
- A completer destroys the sandbox first and appends only after that succeeds. A failed destroy writes nothing.

`RunLog.cfg` checks `Design = "new"` with `Completers = {"c1", "c2"}`.

## Properties

- `AckedDurable`: a write the caller was told succeeded is in the log.
- `AtMostOneTerminal`: one terminal event per run.
- `NothingAfterTerminal`: the terminal event is last.
- `CompletedHasNoSandbox`: a completed Box turn has no sandbox.
- `LogGrows`: `[][IsPrefix(log, log')]_log`. The log only grows, so a terminal run stays terminal.
- `WritersFinish`: under weak fairness on each writer, every started writer finishes.

## TLA+ Findings

Each invariant was checked on its own against `Design = "old"`. Each one fails. Shortest traces:

- `AckedDurable`: Jev stores its events, then a redelivered `put_run` overwrites the row. The same thing happens when `replace_run` runs after another writer's append, or when the rollback runs after it.
- `AtMostOneTerminal`: Jev's events end in a terminal event. A completer that read the log earlier then appends a second one.
- `NothingAfterTerminal`: a late user message is appended after Jev's terminal event.
- `CompletedHasNoSandbox`: the completion is appended while the sandbox is still up. A reader can see it, and it can then be taken back.

`Design = "new"`, from `formal/runlog`:

```text
java -XX:+UseParallelGC -jar tla2tools.jar -workers auto -lncheck final -config RunLog.cfg -deadlock RunLog.tla
```

TLC2 2026.09.25. Every invariant and property passed: 1,461 states generated, 616 distinct. With `Completers = {"c1", "c2", "c3"}`: 15,136 generated, 5,316 distinct, no error.

The checks are not vacuous: an invariant saying `c1` never lands in the log is violated, and so is one saying a late message is never refused.

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

Regression tests from the traces are in `crates/server/tests/inference.rs` (`a_failed_destroy_keeps_a_message_stored_during_the_turn`, `a_completion_racing_another_completion_is_a_conflict`, `the_store_log_grows_and_ends_at_the_first_terminal_event`) and `crates/server/tests/pg_redis.rs`.
