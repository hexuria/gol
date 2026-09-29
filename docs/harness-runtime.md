# Harness runtime

A run is an immutable `RunSpec` plus an append-only event log. `fold` replays `reduce` over that log and derives `RunState`. Neither function does I/O.

`HarnessState` and `DispatchPhase` are separate reducers. The checked harness phases are `Idle`, `Running`, `WaitingForTool`, `WaitingForMessage`, `Completed`, `Failed`, and `Cancelled`. Dispatch is the control-plane lifecycle: `Created`, `Queued`, `Scheduled`, `Provisioning`, `Starting`, `Running`, `Waiting { reason }`, `AwaitingApproval { approval_id }`, `Paused`, `Recovering`, `Completed { outcome }`, `Failed { class, message }`, `Cancelled`, and `Expired`. A local run ends `Completed`. It does not have to visit every dispatch variant.

`Running` stores `step`, `attempt`, and `answered`. `WaitingForTool` stores `step` and `attempt`. `WaitingForMessage` stores `step`, `attempt`, and the `message_id` of the ask it waits on; the reply to that ask (`MessageReceived` with that `reply_to`), its `AskTimedOut`, or for a question to the user its `UserAnswered` returns it to `Running` with the step answered, and `run_until` returns while it waits. A tool result is an event. Retry increments `attempt` and stays in `Running`. The harness ceiling is `spec.limits.max_steps`, and `MAX_RETRIES` stays 2.

The decider chooses the next effect. The work model is a separate gateway call. The harness makes that call only while it executes an emitted `Effect::ModelCall`. Jev is the production decider. This slice ships the `Decider` trait and a scripted decider. It does not call System One during `cargo test`.

## Loop

A local run does the following.

1. Append `RunStarted`. `reduce` moves `Idle` to `Running { step: 1, attempt: 0, answered: false }`. `reduce_dispatch` moves `Created` to `Running`.
2. While the harness state is nonterminal, build a `DecisionView` from the spec, the folded state, the event log, and the tool catalog.
3. Ask the decider for one effect. Append `EffectDecided`. Authorize that effect.
4. Append `EffectAuthorized` or `EffectDenied`, then `reduce`. Perform an effect only when `reduce` returns it. An effect the policy allows is authorized only when `protocol::applicable` holds, that is when `reduce` would change the state or return the effect; otherwise it is denied with `not applicable while <state>`, so no authorized effect is dropped.
5. Stop on `Completed`, `Failed`, or `Cancelled`. A `Complete` that finishes the run is always allowed and is not a step. Any other decision (including a `Complete` while a tool call is outstanding, which cannot finish the run) once `limits.max_steps` is spent, or a model call once `limits.max_model_calls` is spent, appends `RunFailed` with `FailureClass::Budget`.

`POST /v1/runs` and `POST /v1/coworker/turns` accept each limit from 1 to 64 and answer 400 otherwise (`POST /v1/runs` first answers 404 for an agent the caller does not own, and 409 for an `agent_version` that is not the stored manifest's); the defaults are 8 steps and 4 model calls. `limits.max_steps` counts `EffectDecided` events, except a `Complete` decided while the harness is `Running`. `reduce` and `advance_answered_step` compare harness `step` to that same field. `limits.max_model_calls` counts authorized model calls. `FailureClass::Timeout` is the fold of `RunExpired`. This slice has no clock.

`WaitingForTool` does not spin. A `Wait` effect is denied. The deny event is appended, `reduce` leaves the state in place, and the loop continues until the tool result arrives or the step budget runs out; a `Complete` or model call while waiting is denied as not applicable, and still costs a step.

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
| `MemoryRead` | Allowed when the spec lists `memory.read`, and for session or workspace scope when its metadata names `session_id` or `workspace_id`. Global scope is denied. From `Running`, `reduce` emits the read. |
| `MemoryWrite` | Allowed when the spec lists `memory.write`, with the same rules for session, workspace and global scope. From `Running`, `reduce` emits the write. |
| `Complete` | Always allowed. From `Running`, `reduce` enters `Completed`. |
| `Delegate` | Allowed when the spec lists `agent.delegate` and the run is fewer than `MAX_DELEGATION_HOPS` (8) hops from its root. From `Running`, `reduce` emits it. The driver starts the child through its `AgentSpawner` with half of the steps and model calls the run has left, and records `ChildStarted` (with those limits, which then count as spent for the parent) or `DelegateRefused`. It refuses the eleventh child (`MAX_CHILDREN`), a run with fewer than 2 steps or model calls left, and any delegation when no spawner is configured. The server's `OwnedSpawner` queues the child as a run of an agent the parent's owner holds, with the capabilities both allow and the parent's placement, work model and session. |
| `SendMessage` | Allowed when the spec lists `agent.message`. A tell or a new ask starts a task one hop further, so it is denied `MAX_DELEGATION_HOPS` (8) hops from the root. A reply (`reply_to`, not asking back) adds no hop; a reply that asks back opens a new ask and obeys the cap. The exemption rests on the effect's fields alone, so the deliverer (Phase 2.3) must refuse a `reply_to` that is not an ask this run received. The body is at most `MAX_MESSAGE_BYTES` (32 KiB); only an ask has a timeout, from 1 second to `MAX_ASK_TIMEOUT_SECS` (24 h). From `Running`, `reduce` emits a tell, and an ask only from an unanswered step. The driver hands it to its `MessageDeliverer` and records `MessageSent` (an ask then enters `WaitingForMessage`) or `MessageRefused`; without a deliverer every message is refused. A tell or a new ask carves its task's budget as a delegation does and records it as `ChildStarted` first. The server's `OwnedDeliverer` (queue workers only) refuses a tell or new ask from a run already `MAX_DELEGATION_HOPS` deep, as the authorizer does, stores the message (with its sender's hop) once per run and decision, refusing a resend with other content, and then starts the task through the owned spawner; it accepts a reply only to the agent that asked, answering an open ask whose task is the sending run. An answer is appended only while its ask is open in the asker's log; an explicit reply that cannot land yet is refused (`MessageRefused`), and the same reply at the same decision retries it. A resend at the same decision must match what was sent, timeout included. An ask's task answers it when it ends, or earlier with an explicit reply. The end's answer is the task's last non-empty assistant response if it completed after one, else its outcome (or that it failed, was cancelled or expired), cut to 32 KiB at a character boundary. The worker parks a run that waits on an ask instead of acknowledging it, rereads the log and wakes it if the answer is already there; the replier appends the answer, then wakes the asker. The reaper's ask sweep wakes a parked run whose answer landed, delivers an ended task's answer nobody delivered, and answers an ask past its deadline with `AskTimedOut`. Jev is offered `tell:<agent>` and `ask:<agent>` under the delegate choices' filters; its asks wait `JEV_ASK_TIMEOUT_SECS` (1 h). `formal/runqueue` (`RunQueueMail.cfg`) models the park and wake. |
| `AskUser` | Allowed when the spec lists `user.ask`; a blank question, or one over `MAX_MESSAGE_BYTES` (32 KiB), is denied. From `Running`, `reduce` emits it only from an unanswered step. A driver built `with_user_questions()` (the queue worker, which can park the run) records `UserAsked` with a fresh id, and the harness enters `WaitingForMessage`; any other driver records `UserAskRefused` and the loop continues. The worker parks the run on the question's id; `POST /v1/runs/{id}/reply` or a thread message appends `UserAnswered` onto the log it read and wakes it. A question has no timeout: the ask sweep leaves it parked unless its run is stopped, and wakes a stopped one so its worker cancels it. Jev is offered `ask_user` when the run holds `user.ask`, can wait and its step is unanswered; it asks the run's last non-empty assistant text, else the input, cut to 32 KiB at a character boundary. |
| `Execute`, `RequestApproval`, `Wait`, `PublishArtifact` | Denied. The loop continues. |

`PolicyDecision` is `Allow`, `Deny { reason }`, `RequireApproval`, `Modify`, `Limit`, or `Redirect`. The authorizer in this slice returns `Allow` or `Deny` only.

A denied effect becomes `EffectDenied`. The tool, the gateway, and memory are not called.

Memory is keyed by its scope and its owner, which `protocol::memory_owner_id` names from the spec:

| Scope | Owner |
|---|---|
| `Run` | the run id |
| `Step` | the run id and the harness step (`Running.step`) |
| `Agent` | the principal running it (issuer and subject) and the agent id |
| `User` | the issuer and subject |
| `Organization` | the issuer and tenant |
| `Session` | the user, the tenant and the metadata's `session_id` |
| `Workspace` | the organization and the metadata's `workspace_id` |
| `Global` | denied: it would be shared by every tenant |

A server's runs share one memory, so agent, user, organization, session and workspace memory outlive a run; run and step memory stay with the run and go when it ends. A run resumed in a later call or another process starts its run and step memory empty. `formal/memory` checks that no run reads another tenant's organization memory.

A memory store that cannot answer ends the run `Failed { class: Infrastructure }` with the message `memory read: store unavailable` or `memory write: store unavailable`. No `MemoryRead` or `MemoryWritten` is recorded, and the store's detail goes to stderr.

## Event envelope

Every event has one envelope and one payload.

Envelope fields are `event_id`, `event_type`, `run_id`, `step_id`, `parent_run_id`, `agent_id`, `agent_version`, `at`, `actor`, and `caused_by`. `parent_run_id` names the run that started this one (from the spec's `lineage`), and is absent for a top-level run. `at` is Unix milliseconds. `event_type` is copied from the payload when the event is recorded, so the label cannot drift from the variant.

Lifecycle payloads run from `RunCreated` through `RunCancelled`, and include `RunExpired`. `StepRetried` and `StepAdvanced` are the retry and advance transitions. Decision payloads are `EffectDecided`, `EffectAuthorized`, and `EffectDenied`. Execution payloads are `ToolResult`, `ModelResponded`, `MemoryRead`, `MemoryWritten`, `ChildStarted`, and `DelegateRefused`. Message payloads are `MessageSent`, `MessageRefused`, `MessageReceived`, and `AskTimedOut`. `UserMessage` is a message from the run's user. Question payloads are `UserAsked`, `UserAskRefused`, and `UserAnswered`.

The runtime does not copy secret material into events or into the decision view. `CredentialSource::BringYourOwn` stores a `secret_ref` name. It does not store the secret.

## Fold

`fold(spec, events)` starts at `HarnessState::Idle` and `DispatchPhase::Created`. Each event is passed to `reduce` and to `reduce_dispatch`. After a terminal harness state, later events stay in the log and leave that harness state in place. Dispatch terminals are `Completed`, `Failed`, `Cancelled`, and `Expired`.

`RunState` carries `run_id`, `harness`, `dispatch`, `steps`, and `model_calls`.
