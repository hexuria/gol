# gol

gol is a composable agent runtime. This slice runs one local agent loop. The harness state machine stays the six checked phases. Dispatch is a separate control-plane lifecycle and ends `Completed` on a local run.

## Build

```bash
cargo test
GOL_AUTH=local-dev cargo run -p server
```

The toolchain is Rust 1.98.1.

The server listens on `http://127.0.0.1:43123`. Override the port with `GOL_PORT`.

Runs and memory are kept in the process unless `GOL_DATABASE_URL` names a Postgres database, for example `postgres://gol:gol@127.0.0.1/gol`. The server then keeps runs, and every memory scope but run and step memory, there. Run and step memory stay in the process with their run.
- It uses a pool of `GOL_DATABASE_POOL_SIZE` connections (default 8) for runs, keeps one idle, and opens one more connection for memory.
- It refuses to start, saying why, when it cannot reach the database, the pool size is not a positive count, or a table predates the current schema (drop it).
- It connects without TLS, so keep the database on a private network.
- Memory bounds a lock wait at 5 s and a statement at 10 s unless the URL, role or database sets its own. Behind a transaction-mode pooler, set them on the role.

`GOL_REDIS_URL` queues runs instead of running them inside `POST /v1/runs`:
- The server stores a run as created and queued and pushes it onto `{gol:runs}`, and `GOL_WORKERS` worker threads (default 2) in the same process run it. The processing list, start counts and leases are `{gol:runs}:processing`, `{gol:runs}:deliveries` and `{gol:runs}:lease:<run>`.
- A worker claims a run and its 30 s lease in one step, renews the lease every 10 s, records the run's events, and only then acknowledges it.
- A reaper puts a run whose lease ran out back at the front of the queue, every 15 s, so a crashed worker's run is redelivered.
- A redelivered run that already ended is acknowledged without running again, and one started more than five times without ending is failed. A run the worker cannot load (the database is down) is queued again at once, behind the other runs, and does not count as a start.
- Delivery is at least once. A worker stores a run step by step, so a redelivered run resumes from its stored log; a step its worker had not stored is decided again.
- The queue needs `GOL_DATABASE_URL`, since queued run ids outlive the process. One Redis serves one deployment: a standalone Redis (no cluster), with `noeviction` and AOF persistence. A Redis that does not answer a connection within 5 s counts as down.
- The server refuses to start if Redis does not answer, and starts the workers only once its port is bound.

## Model providers

A run's model calls go to its work model's provider with the platform's key for that provider:

- `GOL_OPENAI_API_KEY`, `GOL_ANTHROPIC_API_KEY`, `GOL_GEMINI_API_KEY`, `GOL_SYSTEMONE_API_KEY`: a provider without one fails the call, and the run, with `RunFailed { Dependency }`.
- `GOL_<PROVIDER>_BASE_URL` overrides the provider's host: `https://`, or `http://` to a local host. System One has no public host, so it needs one. Values are trimmed.
- `GOL_MODEL_TIMEOUT_SECS` bounds each call's connect and whole call (default 60, 1 to 600). A call is not retried.
- A bring-your-own credential is refused on the server; it is used from the desktop through the local proxy.
- Each `ModelResponded` records the tokens the call used, when the provider reports them.
- A failed call leaves a fixed reason in the run log (`model call to Anthropic failed`); the provider's error, which may repeat the request's key, goes to the server's stderr.

## Authentication

Every route needs `Authorization: Bearer <token>`. The server refuses to start unless one of these is configured:

- **OIDC:** set `GOL_OIDC_ISSUER`, `GOL_OIDC_AUDIENCE`, `GOL_OIDC_JWKS_URL` and `GOL_OIDC_TENANT_CLAIM`.
  - A token must be RS256, ES256 or EdDSA with a `kid` naming a signing key in the issuer's JWKS. Its `iss` must equal `GOL_OIDC_ISSUER` exactly (a trailing slash differs; an `iss` array containing it is also accepted), its `aud` must include `GOL_OIDC_AUDIENCE`, and it needs a current `exp`, a non-empty `sub`, and a non-empty string tenant claim named by `GOL_OIDC_TENANT_CLAIM`. `nbf` is optional and checked when present. Both have 60 s leeway. An invalid token is 401.
  - `GOL_OIDC_JWKS_URL` must be an https URL, or http on `127.0.0.1`, `localhost` or `[::1]`, without credentials. Redirects are not followed, and only a 200 response is read.
  - The JWKS is fetched on first use and refetched after 10 minutes, so a removed key stops verifying. An unknown `kid` refetches it at most once per 30 s. Keys the server cannot read, or marked for encryption, are skipped.
  - When the JWKS cannot be fetched within 3 s, the answer is 503, and the next fetch waits 30 s. A refetch that fails keeps using the keys already loaded, for as long as the issuer stays unreachable.
- **Local development only:** `GOL_AUTH=local-dev`, with no `GOL_OIDC_*` set, accepts exactly the desktop's static token `gol-gateway-local` and logs a warning on every request. Never use it in production.

Runs and agents belong to the caller that created them, by token issuer and subject:

- `POST /v1/agents` stores a manifest. The same caller may replace it, and anyone else gets 409.
- `POST /v1/runs` needs a stored manifest the caller owns (otherwise 404), with the same `agent_version` (otherwise 409). The run takes its capabilities from the manifest, and a body with any unknown field, `capabilities` included, is 400.
- A desktop turn (`POST /v1/coworker/turns`) needs no stored manifest and names its own capabilities.
- Reading a run, its events, `ag-ui` or `ui`, and completing or failing a turn, answer 404 unless the caller owns the run.
- Postgres keeps the owner with each agent, and each event in its own `run_events` row. Every stored event is also numbered in its owner's outbox (`outbox`, `outbox_counters`): one sequence per principal, from 1 with no gaps, written in the same transaction; the queue's reaper prunes entries older than 7 days. A database from before either change is refused at connect and needs its `agents`, `runs` and `run_events` tables dropped.
- A store that cannot answer gives 503 `{"error":"store unavailable"}`, with the detail on stderr only. A write that fails is not retried, since it may have committed. The Postgres store keeps a pool of connections (8 by default) and replaces one whose backend died before lending it out.

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

The server posts to the proxy only for gateway mode. Set `GOL_PROXY_URL` and `GOL_GATEWAY_TOKEN` (default `gol-gateway-local`, a local stand-in, not a vendor token). A Box turn provisions `gol-box-<run id>` before the model call. Gateway mode removes it only after the turn is completed. Subscription mode leaves it up until the desktop posts the completion, then removes it. A failed remove does not leave the turn completed. A turn that fails is ended with `RunFailed` once its sandbox is known to be gone: after a failed provision when the host confirms no sandbox was left, and after a gateway proxy failure once the sandbox is removed. A turn whose sandbox cannot be removed, or whose host cannot say whether it is gone, stays open. In subscription mode the desktop checks the proxy is not a vendor host before it opens the turn, and when its proxy call fails it posts `POST /v1/coworker/turns/{id}/fail` with the error; the server removes the sandbox, then records `RunFailed`. The turn ends once, whether it is completed or failed first. `GOL_START_BOX=1` makes that sandbox a Docker container. Otherwise the server records the command and tracks the sandbox in process. Each run mounts its own ephemeral volume `gol-workspace-<run id>`. The shared `gol-workspace` volume is not mounted.

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
- `GET /v1/runs/{id}/events?after=N&limit=L` returns a page of the event log: the events after the first `N` (default 0), at most `L` (1 to 500, default 500). A log longer than 500 events is read in pages.

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

`formal/runlog/RunLog.tla` models the writers of a run's event log. Its findings are in `formal/runlog/FINDINGS.md`. `formal/agentowner/AgentOwner.tla` models principals racing to store one agent manifest; its findings are in `formal/agentowner/FINDINGS.md`. `formal/memory/Memory.tla` models runs of different tenants writing and reading memory at the same time; its findings are in `formal/memory/FINDINGS.md`. `formal/runqueue/RunQueue.tla` models the Redis queue, its workers and reaper; its findings are in `formal/runqueue/FINDINGS.md`, and the nightly workflow checks it at larger constants. `formal/RETIRED.md` records retired checks and what owns their properties now. The reducers themselves are checked in Rust: `crates/protocol/tests/reduce_bounded.rs` enumerates every bounded (state, event) pair of production `reduce` and `reduce_dispatch`.

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
