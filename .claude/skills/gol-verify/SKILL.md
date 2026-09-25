---
name: gol-verify
description: >-
  How to verify a change in the gol repo. Use whenever scripts/verify-plan.sh or the
  AGENTS.md "Verification" table fires any trigger other than T0; when touching formal/,
  a FINDINGS.md, the crates/protocol reducers, crates/harness/src/driver.rs, the run
  store (put_run, append_events), the runtime-tokio journal, unsafe code, atomics or a
  hand-written parser; when choosing or running proptest, reduce_bounded, cargo-mutants,
  fuzz, Miri, Loom, Kani, TLA+/TLC, Lean or Bend; or when writing a PR's Verification
  block or any verification claim. How-to only: it never adds a requirement.
---

# gol-verify

The "Required when" column cites AGENTS.md trigger IDs. Nothing here adds a requirement: AGENTS.md "Verification" decides what a change needs, and this skill says how. Where this skill and impeccable-rust differ, this skill wins.

## 1. Method charter

| Method | Question in gol | Required when | Not justified when | Files | Gate | Claim wording |
|---|---|---|---|---|---|---|
| Unit / integration | Does this input give this literal outcome? | every row | — | `mod tests`, `crates/*/tests/` | PR | unit-tested / integration-tested: ‹tests› (Postgres 16, Redis 7) |
| Bounded enumeration | Does every state × event within bounds keep the invariants on production code? | T1 | code that does I/O | `crates/protocol/tests/reduce_bounded.rs` | PR, ≤ 30 s | exhaustively enumerated on production `reduce`/`reduce_dispatch`: `max_steps` ∈ {0,1,2,3,9}, every valid state, every payload and effect kind |
| proptest | Does a property hold beyond the enumerable bounds? | T1 (incremental fold), T9 | domains the enumeration covers | inline; commit `proptest-regressions/` | PR, 256 cases | property-tested: ‹P›, ‹N› cases |
| Differential | Do two implementations agree? | T2 (both stores), T5, T6 | only one implementation | `workflow-bend/src/lib.rs`, `server/tests/` | PR | differentially tested: ‹A› vs ‹B› on ‹n› cases (never "equivalent") |
| Crash | Is state right after a kill at point P? | T4 | pure code | `runtime-tokio/tests/replay_proof.rs` | PR | crash-tested: SIGKILL at ‹P›; no fsync, so not power loss |
| cargo-mutants | Would the tests catch a wrong guard? | recommended for T1 until a CI job exists | glue, I/O | — | — | mutation-tested: ‹k›/‹n› viable caught in ‹F› |
| Fuzz | Can outside bytes crash or get past a parser? | T9 when network-reachable | serde; repo-only input | `crates/<c>/fuzz/` | nightly (no job yet) | fuzz-tested: ‹target›, ‹execs› |
| Criterion | How fast is it on machine M? | T11 | as a gate | `crates/*/benches/` | compiled only | measured: ‹statistic›, n, ‹machine›; not gated |
| Miri | Is there UB on the executed paths? | T7 when Miri can run it | syscall FFI (`kill_group`), sockets, spawn | `.github/workflows/nightly.yml` | nightly | Miri-checked: executed paths of ‹tests› |
| ASan / TSan | Memory errors on paths Miri cannot run? | T7 with pointers crossing FFI | scalar-only FFI | — | — | sanitizer-checked: ‹tests› |
| Loom | Is every interleaving of custom sync code correct? | T8 | locks or channels around pure code | `tests/loom_*.rs` | nightly | Loom-checked: ‹model›, preemption bound ‹p›; partial memory model |
| Kani | Does it hold for every bounded input? | recommended under T7 | serde, `String`, lookups | `#[cfg(kani)]` | — | bounded model-checked (Kani): ‹harness›, unwind ‹k› |
| TLA+ / TLC | Can concurrent writers break a shared-state invariant or stall? | T2, T3, T10 | one actor; restating a reducer; code that does not exist | `formal/<m>/{M.tla,M.cfg,FINDINGS.md}` | PR when `formal/` changes; always on main | model-checked (TLC 2.19, tla2tools v1.7.4): ‹M› at ‹constants›, ‹N› distinct states, deadlock checked; Rust link: ‹tests› or "design model (unlinked)" |
| Lean, Apalache, TLAPS, shuttle, turmoil | Unbounded theorem; symbolic depth; Rust schedules of a modeled protocol | recommend only | mirrors of Rust; `native_decide` scans | — | — | theorem-proven (Lean ‹v›, kernel-checked, axioms ‹list›); a `native_decide` result is "decided by evaluation over ‹D›" |
| Bend | Do the counter laws hold in Bend? | T6 | anything Rust owns | `experiments/bend/` | bend job | see gol-bend |

## 2. Ownership map

One owner per failure class. A new check on a property that already has an owner either retires the old owner in the same PR, or is that owner's link or negative control. Update a row when its action lands.

| Failure class | Code | Owner | Status |
|---|---|---|---|
| Wrong harness transition, lost validity, no progress | `protocol/src/{reduce,phase}.rs` | unit tests in `reduce.rs`; `reduce_bounded` | owned in Rust; the Lean copy was deleted once `reduce_bounded` covered its theorems |
| Dispatch lifecycle | `reduce_dispatch` | `reduce_bounded` (terminal stuck, rank, every open phase has an exit) | `formal/harness/Dispatch.tla` states the same properties; this test is its Rust link |
| Late tool result vs cancel | `reduce.rs` | unit tests of both orders | `protocol/tests/loom_cancel.rs` runs Loom around the pure reducer and checks no gol sync code |
| Run never ends | `harness/src/driver.rs` budget | `harness/tests/budget.rs` | covers `EventuallyDone` for real runs |
| Effect without authorization | `authorizer.rs`, `driver.rs` | unit tests | thin: 2 authorizer tests |
| Concurrent run-log writers | `server/src/{store,postgres,inference,http}.rs` | `formal/runlog` + `server/tests/{inference,pg_redis}.rs` | linked: each counterexample is a Rust test |
| Stores disagree | `store.rs` vs `postgres.rs` | tests on both stores | no shared contract suite yet |
| Duplicate effect after a crash | `runtime-tokio/src/{journal,host}.rs` | `replay_proof.rs`; `formal/workflow/Replay.tla` for the design | the Lean restatement was deleted; its extra theorems map to `replay_proof.rs` and `program.rs` |
| Frontends disagree | `workflow-*` | `histories_agree_across_rust_rhai_js_and_bend` | runs in more than one CI job |
| Bend laws and encoding | `experiments/bend` | `verify-bend.sh`, workflow-bend tests | see gol-bend |
| gol `unsafe` | `workflow-bend/src/boundary.rs` `kill_group` | `forbid(unsafe_code)` in every other crate; `timeout_kills_the_process_group` | compiler-enforced |
| Dependency UB | rhai, smartstring | nightly Miri on `workflow-rhai` | boa_engine excluded after Miri found UB |
| Known-vulnerable dependencies | `Cargo.lock` | `cargo deny` | `cargo audit` repeats its vulnerability ignores |
| Crate layering | crate graph | `check-architecture.sh` | keep |
| Worker ownership, leases, queue ack | not built (`execution/src/lib.rs` joins one thread; `queue.rs` pops without ack) | T3 when built | `formal/harness/HarnessCore.tla` sketches workers: design model (unlinked) |

## 3. Vocabulary

- Every claim gives a term, a subject (a production function or a named model), bounds and assumptions.
- Terms: type-enforced, compiler-enforced, the charter's wordings, "design model (unlinked)" for a model with no Rust test of its properties, "decided by evaluation" for `native_decide`.
- "Proof", "proven", "verified", "guaranteed" and "correct" appear only inside "theorem-proven".
- Never write "equivalent", "deadlock-free" unless TLC checked deadlock and `Done` is the only stuttering step, or "durable" beyond SIGKILL. Never write "faster" without a Criterion run.

## 4. Conformance rule

A model result is evidence about Rust only through a Rust test that runs in CI.

- FINDINGS has a Mapping: for each invariant, the Rust tests that check the same property on production code, or "unlinked".
- Each counterexample becomes a deterministic Rust test before the fix: gate each call and release them in trace order (the `WriteAfterSnapshot` pattern in `crates/server/tests/inference.rs`).
- Each environment assumption the model makes has a test on the real system. For RunLog, the terminal-refusing append is a single SQL statement; the Postgres tests in `pg_redis.rs` exercise it.
- A negative control shows the model is not vacuous: an invariant the old design violates, or a witness invariant that must fail (`formal/runlog/FINDINGS.md` records both).

## 5. TLA+ how-to

- **New model** (T3 answered yes): name the resource and its two or more writers with file:line, state the invariants in words, include a negative control, keep it at most 250 lines with a PR config under 2 minutes, link each invariant to a Rust test or say "unlinked", and say what retires it. Start from `formal/runlog`.
- **Structure:** a header names the Rust it abstracts; `CONSTANTS` hold the bounds; one action per atomic step of the real code (one SQL statement, one lock scope); `Done == <all writers finished> /\ UNCHANGED vars` is the only stuttering disjunct; weak fairness only on processes that really keep running, with a comment saying why.
- **Bounds:** the smallest that show the bug class (two writers); confirm at three once. Record distinct states and depth.
- **Deadlock:** always checked (`verify-tla.sh` passes no `-deadlock`). A reported deadlock is a finding.
- **FINDINGS:** date, command, pinned tla2tools version, constants, distinct states, depth, the negative control, the shortest counterexample traces in words, and the Mapping. No pasted logs.

## 6. `reduce_bounded`

`crates/protocol/tests/reduce_bounded.rs` enumerates, for `max_steps` ∈ {0, 1, 2, 3, 9}, every valid `HarnessState`, all 25 `EventPayload` kinds (every `FailureClass`, every `Effect` inside `EffectDecided`/`EffectAuthorized`/`EffectDenied`, and matching and mismatching tool results), and all 14 `DispatchPhase`s. The alphabets come from matches with no `_` arm, so a new variant stops the file from compiling until it is enumerated.

It asserts: validity is preserved; a change happens exactly where the transition table allows it, to exactly the expected next state; a lexicographic rank strictly drops on every change; terminal states absorb every event and emit nothing; the only effect emitted is the authorized one, and a tool call only from an unanswered running step; dispatch stays terminal, lowers its rank on every change except `RunResumed`, and every open phase has an exit.

`max_steps = 0` is enumerated with today's behavior: `RunStarted` enters `Running { step: 1 }`, no tool call is accepted, and the driver fails the run with `Budget` before its first decision.
