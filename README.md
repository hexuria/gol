# gol

gol is a composable agent runtime. This slice runs one local agent loop. The harness state machine stays the six checked phases. Dispatch is a separate control-plane lifecycle and ends `Completed` on a local run.

## Build

```bash
cargo test
cargo run -p server
```

The server listens on `http://127.0.0.1:43123`. Override the port with `GOL_PORT`.

`cargo test` does not call a live model and does not need an API key. The local server echoes the run input through one tool, then completes.

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
