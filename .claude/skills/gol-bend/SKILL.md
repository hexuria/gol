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

Bend is an optional compile-time frontend that emits a `WorkflowProgram`. Owner decision 8 has it express the full workflow IR in the v2 encoding: `workflow.bend` emits one reference program, and `evals.bend` prints Bend's evaluation of five programs for the Rust differential test. No crate depends on `workflow-bend` yet; the server's `bend` feature arrives with D4, where workflows are registered. Every steady-state decision is made by Rust `evaluate_program`. The sources stay in `experiments/`.

A new Bend program is proposed, never written, unless the owner asks. It is admitted only when all of these hold:

- the output is a finite first-order `workflow-core` value;
- the code is pure and total, uses no `@unsafe`, and runs at compile time;
- a named law covers an unbounded domain that Rust types and tests cannot close;
- the output fits a versioned token line;
- no other method already owns the property.

The v2 program is admitted by owner decision 8, not by these conditions: its laws cover little that Rust tests do not already cover.

## Boundary (Rust owns it)

- Only `workflow_bend::compile` evaluates a Bend `main`, plus the `bend_agrees_with_rust` test, which runs `evals.bend` in the same sandbox. `verify-bend.sh` also runs `workflow.bend` on the repo tree, without the main guard.
- Every Bend process runs under `unshare -r -n` (fallback `-n`, never the host network) and `env -i BEND_NO_TELEMETRY=1`, in its own process group, with a 64 KiB stdout cap, a 16 KiB stderr cap and a 30 s limit (120 s for the test-only `evals.bend` run), then SIGKILL to the group. `verify-bend.sh` sets no limit.
- Staging copies a fixed list of regular files of at most 64 KiB (`STAGED_FILES`) into a new private directory, and every file whose `main` runs (`RUN_FILES`) must have one `String` main.
- The output is one ASCII line: `v<N>` plus tokens from a fixed grammar (`docs/bend.md`). Any `Decision` change bumps `N`.

## Laws

- `LAWS.bend` states the laws. `PROOF.bend` imports and closes them. `?TODO` fails the gate.
- Each artifact has an agreement law against an independent spec function, plus an exact encoding law. The IR also has a round-trip law over every program.
- Deleting a law, dropping a quantifier, narrowing a domain or editing a right-hand side is a spec change: stop and report. An approved spec change updates `LAWS.bend`, `spec`, `search_program()` in `workflow.bend` and in the `workflow-bend` tests, the v2 line in `verify-bend.sh` and in `the_reference_line_parses_to_the_reference_program`, `evals.bend` with `eval_programs()` and `alphabet()` in the tests, the Rhai and JS `search` sources and `docs/bend.md` in one PR.
- A failing proof means stop. Never weaken a law to make a proof pass.

## What "All terms check." establishes

- `round_trip`: every program's postfix token list decodes back to it, under Bend's evaluator.
- `program_agrees`: Bend's `eval_program` on the reference program equals `spec` on every history drawn from the eight records `W.record` names.
- The emitted line is the v2 line in `docs/bend.md`.

It does not establish that Rust's `evaluate_program` agrees beyond the five programs and 585 histories `bend_agrees_with_rust` compares, that Rust's parser inverts Bend's text rendering (one reference line links them; Rust's own round-trip uses a test-only encoder), anything about the journal, crashes, effects, the sandbox, another Bend version or speed, or that the laws are the right spec. The trust base is the pinned checker, whose logic has `Type : Type` and no positivity check.

Claim wording: "Bend-checked (bend ‹pinned version›): the `LAWS.bend` laws hold under Bend's evaluator; Rust parses the emitted line to the reference program (unit-tested); Rust evaluation is differentially tested against Bend's on 5 programs × 585 histories."

## Pin and upgrade

- `scripts/install-bend.sh` holds the version, archive URL and sha256. Never install with `curl … | sh`.
- The pinned binary's `bend guide` outranks upstream web docs.
- An upgrade PR bumps the pin everywhere it appears (`install-bend.sh`, `verify-bend.sh`, `BEND_VERSION` and its test, the CI step names, `docs/bend.md`), re-runs every probe recorded in `docs/bend.md`, re-checks the main-guard lexer, and leaves the laws untouched. If a law fails after the upgrade, stop.

## Speed

Cite numbers only from `crates/workflow-bend/benches/boundary.rs`, with the version, machine, sample count and interval. Report load cost and steady-state cost separately, each against Rust. Never write "Bend is faster", and never gate on it.

## Never use Bend for

Effects, IO, clocks, tools, persistence, cancellation or concurrency; the request path; interleavings or crashes; copies of `reduce` or of a model; authorization ("All terms check." grants nothing); `bend -o`, `--gpu`, the hub or `bend login`; the generated C, FFI or JSON; porting Rust to Bend "to verify it".
