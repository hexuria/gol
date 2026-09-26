# Architecture

gol has three planes.

The control plane accepts a run and records dispatch. Dispatch has its own reducer. The phases are `Created`, `Queued`, `Scheduled`, `Provisioning`, `Starting`, `Running`, `Waiting`, `AwaitingApproval`, `Paused`, `Recovering`, `Completed`, `Failed`, `Cancelled`, and `Expired`. Those scheduling names are not harness states.

The harness plane owns the loop. `reduce` is pure. The driver performs an effect only after `reduce` emits it. The checked phases are `Idle`, `Running`, `WaitingForTool`, `Completed`, `Failed`, and `Cancelled`. `Running` stores the step, the attempt, and whether the current step has been answered. A tool result is an event.

The execution plane runs `Local`, `Reverse`, and `Box`. Reverse and box run on a worker thread with the echo tool. Memory is an in-memory map. The gateway turns one provider payload into `ModelMessage`. The harness does not read provider JSON.

The coworker desktop picks the computer and the credential. Subscription model HTTP is made by the desktop against the local proxy. Platform gateway model HTTP is made by the server. The message record stays on the server either way. The agent container does not make the model call. The desktop is the gpuix app in `coworker/`.

## Crates

| Crate | Owns |
| --- | --- |
| `protocol` | Ids, `RunSpec`, effects, events, `HarnessState`, `DispatchPhase`, `fold`, `reduce`, the authorizer |
| `harness` | `Decider`, the driver, the echo tool, in-memory `Memory` |
| `gateway` | `ModelGateway` and the OpenAI chat-completion adapter |
| `server` | Axum routes and `RunStore`. Gateway mode is the only path that posts to the fixture proxy. |
| `proxy` | Local fixture stand-in for Claude, Codex, Grok, and the platform gateway. No vendor HTTP. |

`RunSpec` is built with a typestate builder. Agent, input, placement, and work model are required. The gateway client cannot send until provider and credential source are set.

Jev is the production decider. It calls System One and is not used by `cargo test`; the harness tests mock System One with wiremock. `jev_state` shows Jev the input, a summary of the fold, the last 16 event payloads, the tool catalog and the skills. `jev_choices` offers each catalog tool by name, `model` while model calls remain, and `complete`; once the step budget is spent it offers only `complete`, the one decision that is still free. `POST /v1/runs` uses `JevDecider`. It stores one user message, runs the driver to the end inside one `spawn_blocking`, then replaces the stored event array from a second `spawn_blocking`. The work model stays on the spec and is not the decider.

## Later slices

PostgreSQL replaces `InMemoryStore`. Redis, gpui-kit, AG-UI, and json-render attach to these types. Approvals, delegation, sandboxes, checkpoint resume, and live provider calls stay out of this slice.
