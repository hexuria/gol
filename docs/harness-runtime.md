# Harness runtime

A run is an immutable `RunSpec` plus an append-only event log. `fold` replays `reduce` over that log and derives `RunState`. Neither function does I/O.

`HarnessState` and `DispatchPhase` are separate reducers. The checked harness phases are `Idle`, `Running`, `WaitingForTool`, `Completed`, `Failed`, and `Cancelled`. Dispatch is the control-plane lifecycle: `Created`, `Queued`, `Scheduled`, `Provisioning`, `Starting`, `Running`, `Waiting { reason }`, `AwaitingApproval { approval_id }`, `Paused`, `Recovering`, `Completed { outcome }`, `Failed { class, message }`, `Cancelled`, and `Expired`. A local run ends `Completed`. It does not have to visit every dispatch variant.

`Running` stores `step`, `attempt`, and `answered`. `WaitingForTool` stores `step` and `attempt`. A tool result is an event. Retry increments `attempt` and stays in `Running`. The harness ceiling is `spec.limits.max_steps`, and `MAX_RETRIES` stays 2.

The decider chooses the next effect. The work model is a separate gateway call. The harness makes that call only while it executes an emitted `Effect::ModelCall`. Jev is the production decider. This slice ships the `Decider` trait and a scripted decider. It does not call System One during `cargo test`.

## Loop

A local run does the following.

1. Append `RunStarted`. `reduce` moves `Idle` to `Running { step: 1, attempt: 0, answered: false }`. `reduce_dispatch` moves `Created` to `Running`.
2. While the harness state is nonterminal, build a `DecisionView` from the spec, the folded state, the event log, and the tool catalog.
3. Ask the decider for one effect. Append `EffectDecided`. Authorize that effect.
4. Append `EffectAuthorized` or `EffectDenied`, then `reduce`. Perform an effect only when `reduce` returns it.
5. Stop on `Completed`, `Failed`, or `Cancelled`. A `Complete` that finishes the run is always allowed and is not a step. Any other decision (including a `Complete` while a tool call is outstanding, which cannot finish the run) once `limits.max_steps` is spent, or a model call once `limits.max_model_calls` is spent, appends `RunFailed` with `FailureClass::Budget`.

`POST /v1/runs` and `POST /v1/coworker/turns` accept each limit from 1 to 64 and answer 400 otherwise; the defaults are 8 steps and 4 model calls. `limits.max_steps` counts `EffectDecided` events, except a `Complete` decided while the harness is `Running`. `reduce` and `advance_answered_step` compare harness `step` to that same field. `limits.max_model_calls` counts authorized model calls. `FailureClass::Timeout` is the fold of `RunExpired`. This slice has no clock.

`WaitingForTool` does not spin. A `Wait` effect is denied. The deny event is appended, `reduce` leaves the state in place, and the loop continues until the tool result arrives or the step budget runs out; a `Complete` while waiting cannot finish the run and costs a step.

Cancellation is terminal. A later `ToolResult` stays `Cancelled`. A later `StepRetried` stays `Cancelled`. A second `ToolResult` for a call that is already answered does not move the state and does not emit another effect.

`RunFailed` from `WaitingForTool` enters `Failed` in that same step. The wait does not survive the failure.

`ExecutionPlacement::Reverse` and `ExecutionPlacement::Box` boot the same harness loop as `Local`. The execution crate runs each on its own worker thread.

`RunQueued`, `RunScheduled`, `RunProvisioning`, `RunStarting`, `RunWaiting`, `RunRecovering`, `RunPaused`, and `RunAwaitingApproval` stay in the log and do not change `HarnessState`. `reduce_dispatch` is the only function that moves `DispatchPhase`.

## Effects

Each variant carries only the fields that variant needs.

| Effect | This slice |
| --- | --- |
| `ModelCall` | Allowed when the spec lists `model.call`. From `Running`, `reduce` emits the call and stays in `Running`. |
| `ToolCall` | Allowed when the named tool is in the catalog and the spec lists that tool's capability. From unanswered `Running`, `reduce` enters `WaitingForTool` and emits the call. |
| `MemoryRead` | Allowed when the spec lists `memory.read`. From `Running`, `reduce` emits the read. |
| `MemoryWrite` | Allowed when the spec lists `memory.write`. From `Running`, `reduce` emits the write. |
| `Complete` | Always allowed. From `Running`, `reduce` enters `Completed`. |
| `Execute`, `Delegate`, `AskUser`, `RequestApproval`, `Wait`, `PublishArtifact` | Denied. The loop continues. |

`PolicyDecision` is `Allow`, `Deny { reason }`, `RequireApproval`, `Modify`, `Limit`, or `Redirect`. The authorizer in this slice returns `Allow` or `Deny` only.

A denied effect becomes `EffectDenied`. The tool, the gateway, and memory are not called.

## Event envelope

Every event has one envelope and one payload.

Envelope fields are `event_id`, `event_type`, `run_id`, `step_id`, `parent_run_id`, `agent_id`, `agent_version`, `at`, `actor`, and `caused_by`. `at` is Unix milliseconds. `event_type` is copied from the payload when the event is recorded, so the label cannot drift from the variant.

Lifecycle payloads run from `RunCreated` through `RunCancelled`, and include `RunExpired`. `StepRetried` and `StepAdvanced` are the retry and advance transitions. Decision payloads are `EffectDecided`, `EffectAuthorized`, and `EffectDenied`. Execution payloads are `ToolResult`, `ModelResponded`, `MemoryRead`, and `MemoryWritten`.

The runtime does not copy secret material into events or into the decision view. `CredentialSource::BringYourOwn` stores a `secret_ref` name. It does not store the secret.

## Fold

`fold(spec, events)` starts at `HarnessState::Idle` and `DispatchPhase::Created`. Each event is passed to `reduce` and to `reduce_dispatch`. After a terminal harness state, later events stay in the log and leave that harness state in place. Dispatch terminals are `Completed`, `Failed`, `Cancelled`, and `Expired`.

`RunState` carries `run_id`, `harness`, `dispatch`, `steps`, and `model_calls`.
