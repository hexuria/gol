---
name: gol-bend
description: >-
  Bend rules for the gol repo. Use for any change under experiments/bend/,
  crates/workflow-bend/, scripts/install-bend.sh, scripts/verify-bend.sh or docs/bend.md
  (AGENTS.md trigger T6), a Bend version bump, a new Bend law, proof or program, or any
  claim about what Bend checks or how fast it is.
---

# gol-bend

`docs/bend.md` is the record of the pinned Bend release and the Rust-to-Bend boundary. This skill is the rule set that goes with it. AGENTS.md "Verification" wins where they differ.

## Role

Bend is an optional compile-time frontend that emits a `WorkflowProgram`. It is frozen at the counter workflow. No crate depends on `workflow-bend`, and every steady-state decision is made by Rust `evaluate_program`. The sources stay in `experiments/`.

A new Bend program is proposed, never written, unless the owner asks. It is admitted only when all of these hold:

- the output is a finite first-order `workflow-core` value;
- the code is pure and total, uses no `@unsafe`, and runs at compile time;
- a named law covers an unbounded domain that Rust types and tests cannot close;
- the output fits a versioned token line;
- no other method already owns the property.

The counter fails the third condition. It stays only as the reference program for the boundary.

## Boundary (Rust owns it)

- Only `workflow_bend::compile` evaluates a Bend `main`.
- Every Bend process runs under `unshare -r -n` (fallback `-n`, never the host network) and `env -i BEND_NO_TELEMETRY=1`, in its own process group, with a 4 KiB stdout cap, a 16 KiB stderr cap and a 30 s limit, then SIGKILL to the group.
- Staging copies a fixed list of regular files of at most 64 KiB.
- The output is one ASCII line: `v<N>` plus tokens from a closed set. Any `Decision` change bumps `N`.

## Laws

- `LAWS.bend` states the laws. `PROOF.bend` imports and closes them. `?TODO` fails the gate.
- Each artifact has an agreement law against an independent spec function, plus an exact encoding law.
- Deleting a law, dropping a quantifier, narrowing a domain or editing a right-hand side is a spec change: stop and report. An approved spec change updates `LAWS.bend`, `decide`, `counter_program()`, `expected()`, the Rhai and JS sources and `docs/bend.md` in one PR.
- A failing proof means stop. Never weaken a law to make a proof pass.

## What "All terms check." establishes

- The laws hold for Bend's own `decide` and `eval_program`, the quantified ones for every sign and Nat.
- The emitted line is `v1 on_counter execute complete fail`.

It does not establish that Rust's `evaluate_program` agrees beyond the tested histories, that `i64` maps correctly to (sign, Nat), anything about the journal, crashes, effects, the sandbox, another Bend version or speed, or that the laws are the right spec. The trust base is the pinned checker, whose logic has `Type : Type` and no positivity check.

Claim wording: "Bend-checked (bend ‹pinned version›): the `LAWS.bend` laws hold under Bend's evaluator; Rust `counter_program()` equals the emitted program (unit-tested); Rust evaluation is differentially tested on 6 histories."

## Pin and upgrade

- `scripts/install-bend.sh` holds the version, archive URL and sha256. Never install with `curl … | sh`.
- The pinned binary's `bend guide` outranks upstream web docs.
- An upgrade PR bumps the pin everywhere it appears (`install-bend.sh`, `verify-bend.sh`, `BEND_VERSION` and its test, the CI step names, `docs/bend.md`), re-runs every probe recorded in `docs/bend.md`, re-checks the main-guard lexer, and leaves the laws untouched. If a law fails after the upgrade, stop.

## Speed

Cite numbers only from `crates/workflow-bend/benches/boundary.rs`, with the version, machine, sample count and interval. Report load cost and steady-state cost separately, each against Rust. Never write "Bend is faster", and never gate on it.

## Never use Bend for

Effects, IO, clocks, tools, persistence, cancellation or concurrency; the request path; interleavings or crashes; copies of `reduce` or of a model; authorization ("All terms check." grants nothing); `bend -o`, `--gpu`, the hub or `bend login`; the generated C, FFI or JSON; porting Rust to Bend "to verify it".
