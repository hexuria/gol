# gol

gol is a composable agent runtime. This slice runs one local agent loop. The harness state machine stays the six checked phases. Dispatch is a separate control-plane lifecycle and ends `Completed` on a local run.

## Build

```bash
cargo test
cargo run -p server
```

The toolchain is Rust 1.98.1.

The server listens on `http://127.0.0.1:43123`. Override the port with `GOL_PORT`.

`cargo test` does not call a live model and does not need an API key. The local server echoes the run input through one tool, then completes. A run records the user message before that loop.

## Inference proxy

`gol-proxy` answers from fixtures. It does not call Anthropic, OpenAI, xAI, or any other vendor.

```bash
cargo run -p proxy
```

It listens on `http://127.0.0.1:43124`. Override the port with `GOL_PROXY_PORT`.

- Claude: `POST /v1/messages` (`?beta=true` is allowed). One of `Authorization` or `x-api-key`. `anthropic-version`, `anthropic-beta`, and `x-claude-code-session-id` are echoed. `stream: true` is SSE.
- Codex subscription: a path ending in `/backend-api/codex/responses`. `Authorization: Bearer` and `ChatGPT-Account-ID`.
- Grok session: `POST /v1/responses`. `Authorization: Bearer` and `X-XAI-Token-Auth: xai-grok-cli`.
- Platform gateway: `POST /v1/gateway/complete`. The gol server is the caller. `Authorization: Bearer`.

Missing auth is 401.

## Coworker

The desktop is `coworker/chat.tsx`, a gpuix window started from gpuix `examples/chat.tsx`. The picker slot is Local or Box, and subscription or gateway. Box is the default computer. Local is opt-in Docker on the desktop host.

```bash
cd coworker
npm test
bun chat.tsx
```

`GOL_SERVER_URL` defaults to `http://127.0.0.1:43123`. `GOL_PROXY_URL` defaults to `http://127.0.0.1:43124`.

The desktop posts the user message to `POST /v1/coworker/turns` before any model call.

| Mode | Who calls the proxy | Who starts the computer |
| --- | --- | --- |
| Subscription, Local | Desktop, `POST /v1/messages` on the local proxy, then `POST /v1/coworker/turns/{id}/completion` | Desktop: `docker run --rm -d --name gol-agent-local -v gol-workspace-$RUN_ID:/workspace gol-agent:local` |
| Subscription, Box | Desktop, same proxy call | Server: unique `gol-box-$RUN_ID`, then remove it. `docker create --name gol-box-$RUN_ID -v gol-workspace-$RUN_ID:/workspace --entrypoint /bin/sh gol-agent:production -c true && docker start -a gol-box-$RUN_ID && docker rm -f gol-box-$RUN_ID` |
| Gateway, Local | Server, `POST /v1/gateway/complete`. The desktop does not call the proxy. | Desktop starts `gol-agent:local` |
| Gateway, Box | Server, same gateway path | Server starts `gol-agent:production` |

The server posts to the proxy only for gateway mode. Set `GOL_PROXY_URL` and `GOL_GATEWAY_TOKEN` (default `gol-gateway-local`, a local stand-in, not a vendor token). A Box turn provisions `gol-box-<run id>` before the model call. Gateway mode removes it only after the turn is completed. Subscription mode leaves it up until the desktop posts the completion, then removes it. A failed remove does not leave the turn completed. A turn that fails is ended, never left open: a failed provision ends it with `RunFailed`, and so does a gateway proxy failure once the sandbox is removed. In subscription mode the desktop checks the proxy is not a vendor host before it opens the turn, and when its proxy call fails it posts `POST /v1/coworker/turns/{id}/fail` with the error; the server removes the sandbox, then records `RunFailed`. The turn ends once, whether it is completed or failed first. `GOL_START_BOX=1` makes that sandbox a Docker container. Otherwise the server records the command and tracks the sandbox in process. Each run mounts its own ephemeral volume `gol-workspace-<run id>`. The shared `gol-workspace` volume is not mounted.

## Agent images

`images/local/Dockerfile` and `images/production/Dockerfile` share `images/agent-entrypoint.sh`. Both install a minimal XFCE. `docker build --build-arg AGENT=rust` also installs the Rust toolchain. The container has no vendor tokens and the entrypoint does not call the proxy.

```bash
docker build -f images/local/Dockerfile -t gol-agent:local images
docker build -f images/production/Dockerfile -t gol-agent:production images
```

The desktop chooses Local (start the local image with Docker) or Box (the server starts the production image).

## HTTP

- `POST /v1/agents` registers a manifest.
- `POST /v1/runs` accepts a run spec and executes `Local` placement to a terminal harness state.
- `GET /v1/runs/{id}` returns the folded state.
- `GET /v1/runs/{id}/events` returns the event log.

`Reverse` and `Box` run the same loop as `Local`. The execution crate runs each placement on a worker thread.

## Bend

Bend 2.0.28 is the pure counter workflow. Rust still owns effects. Install the pinned release, then verify syntax, types, laws, proofs, and the Rust adapter:

```bash
./scripts/install-bend.sh
export PATH="$HOME/.bend/bin:$PATH"
export BEND_NO_TELEMETRY=1
./scripts/verify-bend.sh
```

`scripts/install-bend.sh` downloads the 2.0.28 release archive, checks its pinned sha256, and installs it under `~/.bend`. The upstream `bend-lang.com/install.sh` installs the newest release, which fails the 2.0.28 pin. `docs/bend.md` is the boundary model. `experiments/bend/LAWS.bend` states the counter laws. `experiments/bend/PROOF.bend` proves them. `cargo test -p workflow-bend` compiles that program into `WorkflowProgram`. `cargo bench -p workflow-bend --bench boundary` measures the process boundary against `counter_program`.

## Formal model

`formal/runlog/RunLog.tla` models the writers of a run's event log. Its findings are in `formal/runlog/FINDINGS.md`. `formal/RETIRED.md` records retired checks and what owns their properties now. The reducers themselves are checked in Rust: `crates/protocol/tests/reduce_bounded.rs` enumerates every bounded (state, event) pair of production `reduce` and `reduce_dispatch`.

`./scripts/install-tla.sh` installs the pinned TLA+ tools, v1.7.4 (TLC 2.19), at `~/.local/tla/tla2tools.jar` and checks its sha256. `./scripts/verify-tla.sh` runs TLC on every `formal/**/*.cfg` with `-workers auto -lncheck final`. TLC checks deadlock on every config; AGENTS.md forbids turning it off. The script reads the jar from `TLA_JAR`, then `~/.local/tla/tla2tools.jar`, then `/usr/share/java/tla2tools.jar`.

```bash
./scripts/install-tla.sh
./scripts/verify-tla.sh
```

`./scripts/verify-formal.sh` runs `verify-bend.sh`, then `verify-tla.sh`.

## Layout

- `crates/protocol` holds the types, `fold`, and `reduce`.
- `crates/harness` is the local driver, the echo tool, and the decider trait.
- `crates/gateway` maps provider HTTP into one message type.
- `crates/server` is the Axum process and the in-memory store.
