# gol

gol is a composable agent runtime. This slice runs one local agent loop. The harness state machine stays the six checked phases. Dispatch is a separate control-plane lifecycle and ends `Completed` on a local run.

## Build

```bash
cargo test
cargo run -p server
cargo build -p desktop
```

`cargo build -p desktop` produces `gol-desktop` with gpui-kit. On Linux it needs `pkg-config`, `libxkbcommon-dev`, `libxkbcommon-x11-dev`, `libwayland-dev`, `libfontconfig1-dev`, `libvulkan-dev`, and `g++`. The toolchain is Rust 1.98.1.

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
| Subscription, Local | Desktop, `POST /v1/messages` on the local proxy, then `POST /v1/coworker/turns/{id}/completion` | Desktop: `docker run --rm -d --name gol-agent-local -v gol-workspace:/workspace gol-agent:local` |
| Subscription, Box | Desktop, same proxy call | Server: `docker run --rm -d --name gol-agent-box -v gol-workspace:/workspace gol-agent:production` |
| Gateway, Local | Server, `POST /v1/gateway/complete`. The desktop does not call the proxy. | Desktop starts `gol-agent:local` |
| Gateway, Box | Server, same gateway path | Server starts `gol-agent:production` |

The server posts to the proxy only for gateway mode. Set `GOL_PROXY_URL` and `GOL_GATEWAY_TOKEN` (default `gol-gateway-local`, a local stand-in, not a vendor token). `GOL_START_BOX=1` makes a Box turn run the production `docker run`. Otherwise the server records the command and does not start a container.

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

## Formal model

`formal/harness/Harness.tla` is the interleaving model. `formal/lean` is the single-turn proof. Findings are in `formal/harness/FINDINGS.md`.

From `formal/harness`, with `Harness.cfg` in that directory:

```bash
java -jar /path/to/tla2tools.jar -workers 2 -deadlock Harness.tla
```

```bash
cd formal/lean && lake build
```

## Layout

- `crates/protocol` holds the types, `fold`, and `reduce`.
- `crates/harness` is the local driver, the echo tool, and the decider trait.
- `crates/gateway` maps provider HTTP into one message type.
- `crates/server` is the Axum process and the in-memory store.
