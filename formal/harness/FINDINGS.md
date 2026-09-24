# Harness findings

Checked on 2026-09-23. The harness phase and the control-plane dispatch phase are separate variables. This file does not call the machine minimal. It records no Lean lower bound on the number of phases.

## Model

`Harness.tla` is one session. Constants in `Harness.cfg` are `MaxTurns = 2`, `MaxSteps = 3`, `MaxWorkers = 2`, `MaxRetries = 2`, `MaxTools = 2`.

Variables are `session`, `dispatch`, `turnPhase`, `turnStep`, `owner`, `attempt`, `pending`, `answered`, `live`, and `issued`.

`session` is `idle`, `active`, or `done`. `turnPhase` is `idle`, `running`, `waiting`, `completed`, `failed`, or `cancelled`. `dispatch` is its own control-plane variable. Its phases are `created`, `queued`, `scheduled`, `provisioning`, `starting`, `running`, `waiting`, `awaiting_approval`, `paused`, `recovering`, `completed`, `failed`, `cancelled`, and `expired`. `pending = 0` means no outstanding tool. `owner = 0` means no worker. `issued` stores `<<step, attempt>>` pairs already sent.

The numeric bounds are unchanged. `MaxTurns = 2`, `MaxSteps = 3`, `MaxWorkers = 2`, `MaxRetries = 2`, `MaxTools = 2`. The dispatch phase set is the full control-plane enum. No numeric bound was raised. Waiting reasons and approval ids are payload data on those variants. TLC stores the phase name.

`formal/lean/Harness.lean` is the single-turn relation. It has the same six phases and the same tool, retry, advance, cancel, complete, and fail steps. It does not model a second turn, workers, or dispatch. Those interleavings stay in TLA+.

## Properties

TLC invariants:

- `TypeOK`
- `SingleOwner`. An active turn names one live worker. Any other phase has `owner = 0`.
- `WaitingIffPending`. `waiting` is exactly an outstanding tool id.
- `NoActiveWhenDone`. A done session has no active turn.
- `AttemptBound`

`DispatchMatches` is gone. Dispatch is not a projection of the harness.

TLC properties:

- `TerminalStuck`. A terminal harness phase stays that phase.
- `DispatchTerminalStuck`. `completed`, `failed`, `cancelled`, and `expired` stay put.
- `SchedulingPreserves`. Queue, schedule, provision, prepare, wait, approval, pause, and recover leave the harness variables unchanged.
- `RankDecreases`. Every state-changing step lowers `Rank`.
- `EventuallyDone`, under `WF_vars(Next)`.

Lean theorems:

- `Valid s` means the attempt is at most 2, the step is at most 3, `waiting` lines up with `pending`, a terminal phase has no pending tool, `idle` is the zero record, and `running` or `waiting` has step at least 1.
- `step_preserves_validity`. `Valid s` and `allowed s e` imply `Valid (step s e)`.
- `rank_decreases` on every allowed event except `duplicateResult`.
- `terminal_stuck`, `late_tool_result`, `retry_after_cancel`, `duplicate_is_identity`, and `disallowed_is_identity`.

## TLA+ Findings

Command, from `formal/harness`:

```text
java -XX:+UseParallelGC -jar /tmp/tla/tla2tools.jar -workers 2 -deadlock Harness.tla
```

TLC2 Version 2026.09.23.154203. Deadlock checking was on. Result:

Uriah chose the full dispatch lifecycle. TLC was re-run on that machine before the Rust reducer changed. Deadlock checking was on. Result at 2026-09-23 23:26:24:

```text
Model checking completed. No error has been found.
  Estimates of the probability that TLC did not check all reachable states
  because two distinct states had the same fingerprint:
  calculated (optimistic):  val = 6.7E-6
  based on the actual fingerprints:  val = 2.9E-7
35583194 states generated, 3924158 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 41.
The average outdegree of the complete state graph is 1 (minimum is 0, the maximum 12 and the 95th percentile is 5).
Finished in 06min 25s at (2026-09-23 23:26:24)
```

The earlier three-value dispatch run, at the same numeric bounds, found 280,297 distinct states, depth 37, and finished in 15s. This run replaces that dispatch variable. It is not a minimality comparison.

Answers at those bounds:

- Cancel and a tool result can both be enabled only while the turn is `waiting`. `Cancel` clears `pending` and sets `cancelled`. `ReceiveResult` is enabled only from `waiting`. `IgnoreStale` leaves a terminal turn unchanged.
- Retry after cancel is disabled. `Retry` requires `running`.
- Two results do not satisfy one call. `RequestTool` records `<<step, attempt>>` in `issued` and refuses that pair a second time. `ReceiveResult` requires `answered = FALSE`. A later result is `IgnoreStale`.
- Complete and cancel can both be enabled from `running`. The first one to occur wins. `TerminalStuck` keeps that phase.
- `running`, `waiting`, and `running` cannot cycle. Unanswered `running` ranks `base + 30`, `waiting` ranks `base + 20`, and answered `running` ranks `base + 10`. `Retry` and `Advance` drop `base` by raising attempt or step. TLC accepted `RankDecreases` and `EventuallyDone`.
- A worker cannot disappear while the owner still holds the step. `WorkerCrash` clears `live`, sets `owner` to 0, fails that worker's active turns, and clears `pending` in one action.
- The same `<<step, attempt>>` is not issued twice. The same step index can run again only after `Retry`, which uses the next attempt. `Advance` is the only action that increments the step, and only from answered `running` with `step < MaxSteps`.
- A session cannot complete while a turn is active. `CompleteSession` requires every turn to be outside `Active`. TLC accepted `NoActiveWhenDone`.

Three rejected variants were checked with the same constants. Each one changed the model by deleting the bad transition.

Retry from `cancelled` that writes `running` again violates `TerminalStuck`. Trace: `StartSession`, `StartTurn(1)`, `RequestTool(1)`, `ReceiveResult(1)`, `Cancel(1)`, `Retry(1)` returns to `running` at attempt 1. 701 states generated, depth 7, 2026-09-23 22:41:10. The checked `Retry` stays on `running` and does not assign `turnPhase`.

`ReceiveResult` from `cancelled` that writes `running` violates `TerminalStuck`. Trace: `StartSession`, `StartTurn(1)`, `Cancel(1)`, `ReceiveResult(1)` returns to `running`. 107 states generated, depth 5, 2026-09-23 22:49:43. The checked `ReceiveResult` requires `waiting`.

`WorkerCrash` that only clears `live` violates `SingleOwner`. Trace: `StartSession`, `StartTurn(1)` with `owner = 1`, `WorkerCrash(1)` leaves phase `running` and `owner = 1` while `live[1] = FALSE`. 23 states generated, depth 4, 2026-09-23 22:42:06. The checked action releases the owner and fails the turn together.

## Lean Findings

Command, from `formal/lean`, toolchain `leanprover/lean4:v4.34.0`:

```text
lake build
```

```text
✔ [2/3] Built Harness (499ms)
Build completed successfully (3 jobs).
```

`scanOk` and `scanRank` evaluate `okStep` and `rankOk` on every phase, every attempt in `0..2`, every step in `0..3`, both booleans, and all nine events. `native_decide` accepts both scans. The theorems lift that rectangle through `Valid`, which already forces the attempt and step bounds. This is a bounded check of preservation and rank. It is not a proof that fewer phases would fail.

`duplicateResult` is the stutter Lean excludes from the rank theorem. `apply` returns the same state for that event.

## Simplification

The diagram in slice 1 started with `Idle`, `Planning`, `Running`, `WaitingForTool`, `Retrying`, `Cancelled`, `Failed`, and `Completed`, plus `ToolResult` drawn as a return edge.

Deleted as phases:

- `Planning`. The first active phase is `running` at step 1. No checked event is enabled only in a planning phase.
- `Retrying`. `Retry` stays in `running` and increments `attempt`.
- `ToolResult` as a phase. `ReceiveResult` is an event from `waiting` back to answered `running`.
- Scheduling names as harness phases. They are dispatch phases. `RunExpired` still sends the harness to `Failed` with `Timeout`. Dispatch records `expired` on its own reducer.

No second TLC run measured a model that still had `Planning` and `Retrying`. This file therefore records no state-count delta, no transition-count delta, and no branch-count delta against that larger diagram. The checked graph's own outdegree is the TLC line above. Mutable variables are the ten names in the Model section.

## Implementation mapping

Rust `HarnessState` keeps the six checked phases.

- `Idle`
- `Running { step, attempt, answered }`
- `WaitingForTool { step, attempt }` with answered false by construction
- `Completed { outcome }`
- `Failed { class, message }`
- `Cancelled`

`DispatchPhase` is not a projection of `HarnessState`. `reduce_dispatch` is its own pure function. `fold` applies both reducers to each event. TLC interleaves the two machines. The safety properties above hold for that interleaving.

Dispatch variants:

- `Created`. The empty log.
- `Queued`, `Scheduled`, `Provisioning`, `Starting`, in that order.
- `Running`. `RunStarted` from `Created` or from `Starting`. The `Created` edge is the local shortcut, so a local run does not have to visit every variant.
- `Waiting { reason }`, `AwaitingApproval { approval_id }`, `Paused`, and `Recovering`, each only from `Running`.
- `Completed { outcome }` from `Running` or one of those four side phases.
- `Failed { class, message }`, `Cancelled`, and `Expired` from any nonterminal dispatch phase.

Those four terminals stay terminal. A scheduling event leaves `HarnessState` unchanged.

Reducer guards match the checked relation. Bounds stay `maxSteps = 3` and `maxRetries = 2` until TLC and Lean are re-run. `RunSpec.limits` is the separate budget counter. A budget hit appends `RunFailed` with `Budget`. `RunExpired` reduces to `Failed` with `Timeout`.

Events:

- `RunStarted` from `Idle` enters `Running { step: 1, attempt: 0, answered: false }`.
- `EffectAuthorized` of `ToolCall` from unanswered `Running` enters `WaitingForTool` and emits that `ToolCall`.
- `ToolResult` from `WaitingForTool` enters answered `Running` and emits nothing. A second `ToolResult`, or a `ToolResult` after a terminal phase, leaves the state in place.
- `StepRetried` from answered `Running` with `attempt < 2` increments `attempt` and clears `answered`. From `Cancelled` it leaves `Cancelled`.
- `StepAdvanced` from answered `Running` with `step < 3` increments `step` and clears `answered`.
- `EffectAuthorized` of `Complete`, and `RunCompleted`, from `Running` enter `Completed`.
- `RunFailed` and `RunCancelled` from `Running` or `WaitingForTool` enter `Failed` and `Cancelled`. `RunFailed` from `WaitingForTool` clears the wait in that same step.
- `RunQueued`, `RunScheduled`, `RunProvisioning`, `RunStarting`, `RunWaiting`, `RunRecovering`, `RunPaused`, and `RunAwaitingApproval` leave `HarnessState` unchanged and move only `DispatchPhase`, and only when the dispatch edge above is enabled.
- `RunExpired` leaves a terminal harness state unchanged. From `Running` or `WaitingForTool` it enters harness `Failed` with `Timeout`. Dispatch enters `Expired` from any nonterminal dispatch phase.
- A disallowed event leaves the state unchanged. The driver performs an effect only when `reduce` emits it.

Regression tests for the three TLC counterexamples are retry-after-cancel, late tool result after cancel, and fail-while-waiting ending in `Failed` rather than `WaitingForTool`.
