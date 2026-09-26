# Retired verifiers

A verifier is retired once another check owns its properties on production Rust and that check is green (AGENTS.md Principle 2). Each section says what the retired check established, and what owns each property now. The deleted files stay in git history: `git log --diff-filter=D --stat -- formal/ crates/protocol/tests/loom_cancel.rs`.

## `formal/harness` (TLA+)

`HarnessCore.tla`, `Dispatch.tla` and `Harness.tla` were retired together.

- `Dispatch.tla` restated `reduce_dispatch`: one machine with no concurrent writer (AGENTS.md Principle 4).
- `Harness.tla` ran `HarnessCore` and `Dispatch` side by side with no shared variable. It was the cross-check for the split in hexuria/gol#39 ("join independent machines in one spec" is on the Do-not list).
- `HarnessCore.tla` sketched multi-worker sessions: workers owning turns, leases and abandonment. That code does not exist; `execution` runs a run on one worker thread and the Redis queue pops without an ack. When leases or acks are built, T3 fires and a new model of the real writers is written, as `formal/runlog` was.

Last results, TLC 2.19 (tla2tools v1.7.4), deadlock checked, 2026-09-25:

| Config | Bounds (`MaxTurns`, `MaxSteps`, `MaxWorkers`, `MaxRetries`, `MaxTools`) | Distinct states | Depth | Time |
|---|---|---|---|---|
| `HarnessCore.cfg` | 2, 3, 2, 2, 2 | 1,174,377 | 61 | 42s |
| `Dispatch.cfg` | none | 14 | 5 | 00s |
| `Harness.cfg`, product cross-check | 2, 1, 2, 1, 1 | 43,806 | 23 | 04s |

| Property | Owner now |
|---|---|
| `TypeOK` | the Rust types (`HarnessState`, `DispatchPhase`) |
| `WaitingIffPending` | type-enforced: `HarnessState::WaitingForTool` carries the pending tool |
| `AttemptBound` | `reduce_bounded`: validity (attempt ≤ `MAX_RETRIES`) is preserved on every pair |
| `TerminalStuck` | `reduce_bounded`: terminal states absorb every event; proptest `terminal_state_is_stuck` |
| `DispatchTerminalStuck`, dispatch `RankDecreases` | `reduce_bounded`: `dispatch_reduce_matches_the_table_on_every_pair` |
| `SchedulingPreserves` | `reduce_bounded` (scheduling events are outside the harness table); `fold::tests::scheduling_event_does_not_move_harness_state` |
| harness `RankDecreases` | `reduce_bounded`: a lexicographic rank drops on every change |
| `EventuallyDone` for one run | `crates/harness/tests/budget.rs`: a run that never completes fails with `Budget` |
| `Settles` (`Dispatch.cfg`: dispatch eventually stops changing, under `WF(DispatchComplete)`) | no owner for the liveness itself. `reduce_bounded` checks the parts it rested on: every open phase has an exit, and every change except a resume lowers the rank |
| `SingleOwner`, `NoActiveWhenDone`, session `EventuallyDone` | no owner: they describe multi-worker sessions, which are not built (T3 when they are) |

The dispatch transition table that `reduce_bounded` checks (`dispatch_expected`) was transcribed from `Dispatch.tla`'s actions, one arm per action.

### Design record

The harness model shaped `HarnessState`. These notes are kept because they explain it.

### Simplification

The diagram in slice 1 started with `Idle`, `Planning`, `Running`, `WaitingForTool`, `Retrying`, `Cancelled`, `Failed`, and `Completed`, plus `ToolResult` drawn as a return edge.

Deleted as phases:

- `Planning`. The first active phase is `running` at step 1. No checked event is enabled only in a planning phase.
- `Retrying`. `Retry` stays in `running` and increments `attempt`.
- `ToolResult` as a phase. `ReceiveResult` is an event from `waiting` back to answered `running`.
- Scheduling names as harness phases. They are dispatch phases. `RunExpired` still sends the harness to `Failed` with `Timeout`. Dispatch records `expired` on its own reducer.

No second TLC run measured a model that still had `Planning` and `Retrying`. This file therefore records no state-count delta, no transition-count delta, and no branch-count delta against that larger diagram. `HarnessCore` had ten mutable variables: `session`, `dispatch`, `turnPhase`, `turnStep`, `owner`, `attempt`, `pending`, `answered`, `live` and `issued`.

### Implementation mapping

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

Reducer guards match the checked relation. `HarnessCore.cfg` checked `MaxSteps = 3`, the TLC instance of `Limits.max_steps`. Rust reads `Limits.max_steps` for the harness ceiling. The driver still compares the `EffectDecided` count to that field. `maxRetries` stays 2. A budget hit appends `RunFailed` with `Budget`. `RunExpired` reduces to `Failed` with `Timeout`.

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

## `formal/workflow/Replay.tla` (TLA+)

It restated the counter journal in `crates/runtime-tokio` (AGENTS.md Principle 4: do not restate the journal). One process writes the journal, so no concurrent writer needed a model, and `crates/runtime-tokio/tests/replay_proof.rs` checks the crash property on the real binary with SIGKILL.

The model had three actions: `Perform` held a result in memory, `Commit` was the only action that wrote the journal, and `Crash` dropped what was held.

### Last run

2026-09-26, `./scripts/verify-tla.sh`, which runs `java -XX:+UseParallelGC -jar ~/.local/tla/tla2tools.jar -workers auto -lncheck final -config Replay.cfg Replay.tla`. TLC2 Version 2.19 of 08 August 2024, from tla2tools v1.7.4. No constants. Deadlock checked.

| Config | Generated | Distinct | Depth | Result |
|---|---|---|---|---|
| `Replay.cfg` | 9 | 5 | 3 | no error |

### Negative control

Adding `EmptyStaysEmpty == [][(journal = "empty") => UNCHANGED journal]_vars` as a property fails: TLC reports it violated, because `Commit` writes the empty journal. The model can therefore reach a commit, and `HitSticks` is not vacuous.

### Owner now

| Invariant or property | Rust owner |
|---|---|
| `JournaledResultForcesBranch` | `branch_on_recorded_counter` in `crates/workflow-core/src/program.rs` |
| `HeldIsNotAHit` | `kill_before_commit_leaves_the_next_unstarted`: a result held but not committed is not replayed, so the rerun performs the effect again; `held_bytes_are_not_a_hit_before_ok` in `crates/runtime-tokio/src/journal.rs` checks the log bytes only |
| `JournaledIdNotReexecuted`, `HitSticks` | `kill_after_commit_skips_the_counter` in `crates/runtime-tokio/tests/replay_proof.rs`: after the rerun the effect count stays 1 |
| a crash before commit performs again | `kill_before_commit_leaves_the_next_unstarted`: the effect count becomes 2 |

## Loom: `crates/protocol/tests/loom_cancel.rs`

It ran a late tool result against `RunCancelled` on two Loom threads around a `loom::sync::Mutex<HarnessState>` and asserted the run ends `Cancelled`. `reduce` is pure, so Loom explored the two orders of two calls and checked no gol synchronization code ("Run Loom around a pure function" is on the Do-not list).

| Property | Owner now |
|---|---|
| cancel, then a late tool result: `Cancelled` | `late_tool_result_after_cancel_stays_cancelled` in `reduce.rs` |
| a tool result, then cancel: `Cancelled` | `tool_result_then_cancel_ends_cancelled` in `reduce.rs` |
| every other order of every event pair | `reduce_bounded` |

Loom stays in the gol-verify charter for T8: a Loom test of gol's own atomics or locks when correctness depends on memory ordering.

## Lean

`formal/lean/Harness.lean` was a hand copy of the single-turn reducer. Its `step_preserves_validity` and `rank_decreases` were decided by `native_decide` over a scan of 2,592 (state, event) pairs of the copy, with the attempt at most 2 and the step at most 3. It was removed after `crates/protocol/tests/reduce_bounded.rs` checked the same properties on production `reduce`:

| Lean theorem | Rust owner |
|---|---|
| `step_preserves_validity` | `reduce_bounded`: validity is preserved on every pair |
| `rank_decreases` | `reduce_bounded`: a lexicographic rank drops on every change |
| `terminal_stuck` | `reduce_bounded`: terminal states absorb every event; proptest `terminal_state_is_stuck` |
| `late_tool_result` | `late_tool_result_after_cancel_stays_cancelled` in `reduce.rs` |
| `retry_after_cancel`, `no_retry_after_cancel` | `retry_after_cancel_stays_cancelled` in `reduce.rs` |
| `duplicate_is_identity` | `duplicate_tool_result_stays_answered` in `reduce.rs`; `reduce_bounded` table |
| `disallowed_is_identity` | `reduce_bounded`: a pair outside the transition table leaves the state unchanged |

The Rust test covers more than the copy did: `max_steps` ∈ {0, 1, 2, 3, 9}, invocation matching, `RunExpired`, and every effect kind.

`formal/replay/Replay.lean` restated `formal/workflow/Replay.tla` (itself retired above) for the counter journal. The Rust owners are:

| Lean theorem | Rust owner |
|---|---|
| `journaled_result_forces_branch` | `branch_on_recorded_counter` in `crates/workflow-core/src/program.rs`: no record executes the counter, 0 completes, nonzero fails, no wait |
| `branch_changes_id` | `branch_on_recorded_counter`: each recorded path gets its own effect id at sequence 0 |
| `held_is_not_a_hit` | `held_bytes_are_not_a_hit_before_ok` in `crates/runtime-tokio/src/journal.rs` |
| `journaled_id_not_executed_again`, `hit_sticks`, `hit_disables_perform` | `kill_after_commit_skips_the_counter` in `crates/runtime-tokio/tests/replay_proof.rs`: after the rerun the effect count stays 1 and the log keeps the committed zero |
| `crash_reenables_perform` | `kill_before_commit_leaves_the_next_unstarted`: a kill before commit leaves no record, so the rerun performs the effect again |
