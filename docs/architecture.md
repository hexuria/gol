# Architecture

gol has three planes.

The control plane accepts a run and records dispatch. Dispatch has its own reducer. The phases are `Created`, `Queued`, `Scheduled`, `Provisioning`, `Starting`, `Running`, `Waiting`, `AwaitingApproval`, `Paused`, `Recovering`, `Completed`, `Failed`, `Cancelled`, and `Expired`. Those scheduling names are not harness states.

The harness plane owns the loop. `reduce` is pure. The driver performs an effect only after `reduce` emits it. The checked phases are `Idle`, `Running`, `WaitingForTool`, `Completed`, `Failed`, and `Cancelled`. `Running` stores the step, the attempt, and whether the current step has been answered. A tool result is an event.

The execution plane runs `Local`, `Reverse`, and `Box`. Reverse and box run on a worker thread with the echo tool. Memory is an in-memory map. The gateway turns one provider payload into `ModelMessage`. The harness does not read provider JSON.

## Crates

| Crate | Owns |
| --- | --- |
| `protocol` | Ids, `RunSpec`, effects, events, `HarnessState`, `DispatchPhase`, `fold`, `reduce`, the authorizer |
| `harness` | `Decider`, the driver, the echo tool, in-memory `Memory` |
| `gateway` | `ModelGateway` and the OpenAI chat-completion adapter |
| `server` | Axum routes and `RunStore` |

`RunSpec` is built with a typestate builder. Agent, input, placement, and work model are required. The gateway client cannot send until provider and credential source are set.

Jev is the production decider. It calls System One and is not used by `cargo test`. This slice's server uses a local decider that echoes the run input and then completes. The work model stays on the spec and is not the decider.

## Later slices

PostgreSQL replaces `InMemoryStore`. Redis, gpui-kit, AG-UI, and json-render attach to these types. Approvals, delegation, sandboxes, checkpoint resume, and live provider calls stay out of this slice.
