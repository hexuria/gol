# Harness findings

Checked on 2026-09-23. The harness phase and the control-plane dispatch phase are separate variables. This file does not call the machine minimal. It records no Lean lower bound on the number of phases.

## Model

`Harness.tla` is one session. It is the interleaving of two modules. `HarnessCore.tla` holds the harness variables and actions. `Dispatch.tla` holds `dispatch` and its actions. Constants in `HarnessCore.cfg` are `MaxTurns = 2`, `MaxSteps = 3`, `MaxWorkers = 2`, `MaxRetries = 2`, `MaxTools = 2`. The Decomposition section below says what each config checks.

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
- `SchedulingPreserves`. Queue, schedule, provision, prepare, wait, approval, pause, recover, and resume leave the harness variables unchanged.
- `RankDecreases`. A state-changing step lowers `Rank`, except `DispatchResume`, which returns dispatch to `running` and is exempt.
- `EventuallyDone`. `FairSpec` is `Spec /\ WF_vars(Next) /\ WF_vars(DispatchComplete)`. The 2026-09-23 run recorded below used the earlier `FairSpec` and does not cover this formula. The 2026-09-25 run does.

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

TLC2 Version 2026.09.23.154203. `-deadlock` turns deadlock checking off, so these runs did not check deadlock. Result:

Uriah chose the full dispatch lifecycle. TLC was re-run on that machine before the Rust reducer changed, with the same command. Result at 2026-09-23 23:26:24:

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
- `running`, `waiting`, and `running` cannot cycle. Unanswered `running` ranks `base + 30`, `waiting` ranks `base + 20`, and answered `running` ranks `base + 10`. `Retry` drops `base` by raising attempt. `Advance` raises the step and sets `attempt` to 0. TLC accepted the earlier strict `RankDecreases` and `EventuallyDone` on 2026-09-23. The current `RankDecreases` exempts `DispatchResume`. The 2026-09-23 run did not re-check that formula. The 2026-09-25 run does.
- A worker cannot disappear while the owner still holds the step. `WorkerCrash` clears `live`, sets `owner` to 0, fails that worker's active turns, and clears `pending` in one action.
- The same `<<step, attempt>>` is not issued twice. The same step index can run again only after `Retry`, which uses the next attempt. `Advance` is the only action that increments the step, and only from answered `running` with `step < MaxSteps`.
- A session cannot complete while a turn is active. `CompleteSession` requires every turn to be outside `Active`. TLC accepted `NoActiveWhenDone`.

Three rejected variants were checked with the same constants. Each one changed the model by deleting the bad transition.

Retry from `cancelled` that writes `running` again violates `TerminalStuck`. Trace: `StartSession`, `StartTurn(1)`, `RequestTool(1)`, `ReceiveResult(1)`, `Cancel(1)`, `Retry(1)` returns to `running` at attempt 1. 701 states generated, depth 7, 2026-09-23 22:41:10. The checked `Retry` stays on `running` and does not assign `turnPhase`.

`ReceiveResult` from `cancelled` that writes `running` violates `TerminalStuck`. Trace: `StartSession`, `StartTurn(1)`, `Cancel(1)`, `ReceiveResult(1)` returns to `running`. 107 states generated, depth 5, 2026-09-23 22:49:43. The checked `ReceiveResult` requires `waiting`.

`WorkerCrash` that only clears `live` violates `SingleOwner`. Trace: `StartSession`, `StartTurn(1)` with `owner = 1`, `WorkerCrash(1)` leaves phase `running` and `owner = 1` while `live[1] = FALSE`. 23 states generated, depth 4, 2026-09-23 22:42:06. The checked action releases the owner and fails the turn together.

Re-run on 2026-09-25 with deadlock checking on. Command, from `formal/harness`, as `scripts/verify-tla.sh` runs it:

```text
java -XX:+UseParallelGC -jar ~/.local/tla/tla2tools.jar -workers auto -lncheck final -config Harness.cfg Harness.tla
```

TLC2 Version 2.19 of 08 August 2024, from tla2tools v1.7.4, which `scripts/install-tla.sh` pins. 4 workers. The command has no `-deadlock`, so TLC checked deadlock. `-lncheck final` checks `EventuallyDone` once, on the complete graph. The run covers the current `FairSpec`, `RankDecreases` with the `DispatchResume` exemption, and every invariant and property in `Harness.cfg`. Result:

```text
Model checking completed. No error has been found.
  Estimates of the probability that TLC did not check all reachable states
  because two distinct states had the same fingerprint:
  calculated (optimistic):  val = 1.2E-4
  based on the actual fingerprints:  val = 7.0E-6
153963902 states generated, 16441278 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 65.
The average outdegree of the complete state graph is 1 (minimum is 0, the maximum 12 and the 95th percentile is 5).
Finished in 17min 04s at (2026-09-25 20:02:55)
```

The nightly TLC2 Version 2026.09.25.020137 found the same 16,441,278 distinct states and depth 65 with `-deadlock`. `formal/workflow/Replay.cfg` also passes with deadlock checking on: 9 states generated, 5 distinct states found, depth 3. No state in either model is a deadlock. `Done`, `IgnoreStale`, and `Stop` are the stutter steps at the ends.

## Decomposition

Split on 2026-09-25. Before the split, TLC explored the product of 14 dispatch phases and every harness state. The two machines share no variable that either one reads. So the product had exactly 1,174,377 × 14 = 16,441,278 distinct states. The numeric bounds are unchanged. No property was removed.

Modules:

- `HarnessCore.tla` has the harness variables and actions from the old `Harness.tla`, without `dispatch`. `FairSpec` is `Spec /\ WF_vars(Next)`. `HarnessCore` cannot name `dispatch`, so no harness action can read it.
- `Dispatch.tla` has `dispatch` and its fifteen actions. `Step` is those fifteen. `Next` is `Step \/ DispatchDone`. `DispatchDone` is a stutter at a terminal dispatch phase, like `Done` in the harness. `FairSpec` is `Spec /\ WF_vars(DispatchComplete)`.
- `Harness.tla` composes them. `Next` is `H!Next /\ UNCHANGED dispatch` or `D!Step /\ UNCHANGED harnessVars`. `FairSpec`, `Rank`, `RankDecreases`, `EventuallyDone`, and `SchedulingPreserves` keep their old definitions. `DispatchDone` is not in the product `Next`, so the product relation is the old one.

Which variables each property reads:

| Property | Reads | Checked in |
|---|---|---|
| `TypeOK` | both, as two conjuncts | `HarnessCore.cfg` and `Dispatch.cfg`, one conjunct each |
| `SingleOwner`, `WaitingIffPending`, `NoActiveWhenDone`, `AttemptBound` | harness | `HarnessCore.cfg` |
| `TerminalStuck` | harness | `HarnessCore.cfg` |
| `DispatchTerminalStuck` | `dispatch` | `Dispatch.cfg` |
| `RankDecreases` | both | `HarnessCore.cfg` and `Dispatch.cfg`, one rank each |
| `EventuallyDone` under `FairSpec` | `session`, with fairness over both | `HarnessCore.cfg` with `Settles` in `Dispatch.cfg` |
| `SchedulingPreserves` | both | holds by construction |
| deadlock | both | `HarnessCore.cfg` and `Dispatch.cfg` |

`Harness.cfg` checks all of them on the composed product at `MaxSteps = 1`, `MaxRetries = 1`, `MaxTools = 1`. That run is a cross-check of the argument below. It does not replace the full-bounds runs.

Why the product properties follow:

- Every product step changes only harness variables, only `dispatch`, or nothing. A harness step leaves `dispatch` unchanged, and a dispatch step leaves the harness unchanged. Each component's reachable set does not depend on the other. So the product's reachable states are the Cartesian product, and a state predicate over one side holds on the product exactly when it holds on that component.
- `RankDecreases`. `Rank` is `HarnessRank + DispatchRank`. A harness step leaves `DispatchRank` fixed and cannot be `DispatchResume`. A dispatch step leaves the harness rank fixed. The product formula holds exactly when `HarnessCore`'s `Rank' < Rank` and `Dispatch`'s `DispatchResume \/ DispatchRank' < DispatchRank` both hold.
- `SchedulingPreserves`. A `D!Scheduling` step changes `dispatch`. The only product disjunct that changes `dispatch` is `D!Step /\ UNCHANGED harnessVars`.
- Deadlock. A product state is deadlocked only when both sides have no step. Terminal dispatch phases are reachable and have no dispatch step. So the product is deadlock free exactly when `HarnessCore` is. `Dispatch.cfg` checks more than the product did. In the product, a harness stutter was always enabled, so a nonterminal dispatch phase with no exit would not have been reported. `DispatchDone` is enabled only at a terminal phase, so `Dispatch.cfg` reports such a phase.
- `EventuallyDone`. `HarnessCore`'s `RankDecreases` bounds the number of harness steps in any behavior. `Settles` is `<>[][UNCHANGED dispatch]_dispatch` under `WF(DispatchComplete)`. It bounds the number of dispatch steps in any fair behavior. An infinite run of dispatch steps needs infinitely many `DispatchResume` steps. Those keep `dispatch` in the live phases, and `DispatchComplete` is enabled in every live phase. The projection of a product behavior onto `dispatch` satisfies `WF(DispatchComplete)`, because that action reads and writes only `dispatch`. So every behavior of the product `FairSpec` ends at a pair where neither side takes another step. `WF_vars(Next)` then requires that no harness step is enabled at that pair. `HarnessCore`'s `EventuallyDone` under `WF_vars(Next)` holds exactly when every reachable harness state with no enabled step has `session = "done"`. That is the product property.

`Settles` is the one new property. It is the lemma the `EventuallyDone` argument needs.

Checks that the split is sound, from `formal/harness` scratch copies:

- The old `Harness.tla` and the composed `Harness.tla` refine each other at `MaxSteps = 1`, `MaxRetries = 1`, `MaxTools = 1`. `New!FairSpec` checked `Old!FairSpec` as a property, and `Old!FairSpec` checked `New!FairSpec`. Both passed: 397,790 states generated, 43,806 distinct states found.
- The composed `Harness.tla` at the full bounds, once, with the product `TypeOK`, `RankDecreases`, `EventuallyDone`, and `SchedulingPreserves`: no error, 153,963,902 states generated, 16,441,278 distinct states found, depth 65, 21min 30s. Those are the old counts. That run is not in CI.
- `Dispatch.tla` without `WF(DispatchComplete)` violates `Settles`. Trace: `DispatchLocalRun`, `DispatchPause`, then `DispatchResume` back to state 2. That loop is why `FairSpec` has the conjunct.
- `Dispatch.tla` without `DispatchDone` reports a deadlock at `failed`.
- `FailAbandoned` removed from `Next` deadlocks both `HarnessCore` and the old product. Trace: `StartSession`, `WorkerCrash`, `WorkerCrash`. The product trace adds one `DispatchFail` step to bring dispatch to a terminal phase first.

TLC 2.19 throws `the identifier session is either undefined or not an operator` on `WF_vars(D!DispatchComplete)` in `Harness.tla`. The composed module therefore defines `DispatchComplete == D!DispatchComplete /\ UNCHANGED harnessVars`, which is the old definition, and uses `WF_vars(DispatchComplete)`.

Command, from the repository root:

```text
./scripts/verify-tla.sh
```

That runs `java -XX:+UseParallelGC -jar ~/.local/tla/tla2tools.jar -workers auto -lncheck final -config <name>.cfg <name>.tla` for each config. TLC2 Version 2.19 of 08 August 2024, from tla2tools v1.7.4. 4 workers on 4 cores. No `-deadlock`, so TLC checked deadlock. Results on 2026-09-25:

| Config | Bounds | Generated | Distinct | Depth | Time |
|---|---|---|---|---|---|
| old `Harness.cfg`, before the split | 2, 3, 2, 2, 2 | 153,963,902 | 16,441,278 | 65 | 17min 55s |
| `HarnessCore.cfg` | 2, 3, 2, 2, 2 | 6,887,103 | 1,174,377 | 61 | 42s |
| `Dispatch.cfg` | none | 54 | 14 | 5 | 00s |
| `Harness.cfg`, product cross-check | 2, 1, 2, 1, 1 | 397,790 | 43,806 | 23 | 04s |
| `formal/workflow/Replay.cfg` | none | 9 | 5 | 3 | 00s |

Bounds are `MaxTurns`, `MaxSteps`, `MaxWorkers`, `MaxRetries`, `MaxTools`. The whole `verify-tla.sh` took 49s against 17min 55s before, on the same machine. The old `Harness.cfg` took about 28 minutes on the 4-vCPU GitHub runner.

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

`DispatchPhase` is not a projection of `HarnessState`. `reduce_dispatch` is its own pure function. `fold` applies both reducers to each event. TLC interleaves the two machines. The 2026-09-23 run checked the earlier formulas. `DispatchResume` and the current `FairSpec` were not part of that run. The 2026-09-25 run covers both.

Dispatch variants:

- `Created`. The empty log.
- `Queued`, `Scheduled`, `Provisioning`, `Starting`, in that order.
- `Running`. `RunStarted` from `Created` or from `Starting`. The `Created` edge is the local shortcut, so a local run does not have to visit every variant.
- `Waiting { reason }`, `AwaitingApproval { approval_id }`, `Paused`, and `Recovering` are entered from `Running`. `DispatchResume` returns those four phases to `running` and leaves the harness variables unchanged. From `Running` the four side phases can be entered again.
- `Completed { outcome }` from `Running` or one of those four side phases.
- `Failed { class, message }`, `Cancelled`, and `Expired` from any nonterminal dispatch phase.

Those four terminals stay terminal. A scheduling event leaves `HarnessState` unchanged.

Reducer guards match the checked relation. `Harness.cfg` checks one `MaxSteps = 3`, the TLC instance of `Limits.max_steps`. Rust reads `Limits.max_steps` for the harness ceiling. The driver still compares the `EffectDecided` count to that field. `maxRetries` stays 2. A budget hit appends `RunFailed` with `Budget`. `RunExpired` reduces to `Failed` with `Timeout`.

Events:

- `RunStarted` from `Idle` enters `Running { step: 1, attempt: 0, answered: false }`.
- `EffectAuthorized` of `ToolCall` from unanswered `Running` enters `WaitingForTool` and emits that `ToolCall`.
- `ToolResult` from `WaitingForTool` enters answered `Running` and emits nothing. A second `ToolResult`, or a `ToolResult` after a terminal phase, leaves the state in place.
- `StepRetried` from answered `Running` with `attempt < 2` increments `attempt` and clears `answered`. From `Cancelled` it leaves `Cancelled`.
- `StepAdvanced` from answered `Running` with `step` below `Limits.max_steps` increments `step`, sets `attempt` to 0, and clears `answered`. `Advance` sets `attempt' = 0` under that same step guard with `MaxSteps`.
- `EffectAuthorized` of `Complete`, and `RunCompleted`, from `Running` enter `Completed`.
- `RunFailed` and `RunCancelled` from `Running` or `WaitingForTool` enter `Failed` and `Cancelled`. `RunFailed` from `WaitingForTool` clears the wait in that same step.
- `RunQueued`, `RunScheduled`, `RunProvisioning`, `RunStarting`, `RunWaiting`, `RunRecovering`, `RunPaused`, `RunAwaitingApproval`, and `RunResumed` leave `HarnessState` unchanged. The scheduling and side-phase payloads move `DispatchPhase` only when the dispatch edge above is enabled. `RunResumed` returns `Waiting`, `AwaitingApproval`, `Paused`, and `Recovering` to `Running`.
- `RunExpired` leaves a terminal harness state unchanged. From `Running` or `WaitingForTool` it enters harness `Failed` with `Timeout`. Dispatch enters `Expired` from any nonterminal dispatch phase.
- A disallowed event leaves the state unchanged. The driver performs an effect only when `reduce` emits it.

Regression tests for the three TLC counterexamples are retry-after-cancel, late tool result after cancel, and fail-while-waiting ending in `Failed` rather than `WaitingForTool`.
