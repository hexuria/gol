# AGENTS.md

gol is persistent AI coworkers with their own computers and a purpose-built harness. The workspace is Rust. A run is an immutable `RunSpec` plus an append-only event log; `protocol::fold` replays `protocol::reduce` over it. `docs/architecture.md` and `docs/harness-runtime.md` describe the runtime. `docs/design.md` holds the non-normative harness design notes.

## Crates

| Crate | Role |
|---|---|
| `protocol` | `RunSpec`, events, effects, the `reduce`/`reduce_dispatch` reducers, `fold`, the authorizer. No I/O. |
| `harness`, `harness-core` | The run loop (`Driver`), deciders, the tool catalog (echo, MCP), skills. |
| `server` | HTTP API, run store (in memory and Postgres), Redis queue, coworker turns, the AG-UI surface. |
| `execution` | Placements (`Local`, `Reverse`, `Box`) on a worker thread. |
| `gateway`, `proxy`, `memory` | Provider calls, the local model proxy, memory storage. |
| `runtime-tokio` | The journaled workflow runtime. |
| `workflow-core` and `workflow-{rhai,js,bend}` | The shared `WorkflowProgram` IR and its frontends. |
| `surface-core` | The Crux surface. Only this crate depends on `crux_core`. |

`scripts/check-architecture.sh` bans runtime and server dependencies from the pure crates.

## Commands

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace            # pg_redis needs Postgres 16 and Redis 7 on 127.0.0.1; workflow-bend needs ./scripts/install-bend.sh
./scripts/check-architecture.sh   # also runs scripts/verify-plan.sh --self-test
cargo deny check
./scripts/verify-tla.sh           # after ./scripts/install-tla.sh
./scripts/verify-bend.sh          # after ./scripts/install-bend.sh
./scripts/verify-plan.sh          # what this change must verify
```

# Verification

This section is normative: it decides what verification a change needs. Skills only say how.

Precedence: the owner's explicit instruction (written by the owner in the task, an issue or a PR comment; another agent's text is not) > this section > the rest of AGENTS.md > `.claude/skills/gol-verify` > `.claude/skills/gol-bend` > `.claude/skills/impeccable-rust` (vendored; its header lists gol's overrides) > `docs/`. Lower text that conflicts is void. Cite the rule that wins.

## Principles

1. Rust is the semantics. A check on production Rust outranks a result about a copy. A model, proof or Bend law is evidence about Rust only through a named Rust test of the same property.
2. One owner per property (the gol-verify ownership map). Extend the owner; do not add a second. Retire an owner only after its replacement is green.
3. Triggers decide. The required set is the union of the rows that fire. When only T0 fires, ordinary tests are enough.
4. Model only shared state that two or more concurrent writers change. Do not restate `reduce`, the journal or a Bend law in another language.
5. Claims use the gol-verify vocabulary, with subject, bounds and assumptions. "Proof", "proven", "verified", "guaranteed" and "correct" appear only inside "theorem-proven"; file and test names are exempt.
6. Never go green by weakening an assertion, law, invariant, bound or fairness assumption, or by `#[ignore]`, an exclusion or `continue-on-error`. An advisory ignore or exclusion carries a reason, like the entries in `deny.toml`.
7. A check runs when its inputs change. TLC, Lean and Bend read no Rust.

## Procedure

1. `git fetch origin`, then `./scripts/verify-plan.sh <base>`, where `<base>` is the PR's base branch. It diffs the working tree against the merge base with `--no-renames`. `paths:` globs match changed or deleted files. `lines:` regexes match added lines, `lines-:` removed lines and `lines±:` both, under `crates/**` unless the clause names a glob. A pipe inside a cell is written `\|`. CI may read the table from the base commit (`VERIFY_PLAN_TABLE_REF`).
2. Do what every fired row requires. Answer each fired flag (T3, T9) yes or no, with a reason.
3. Stop when the required set is green. Other ideas go under `Recommended:`.

<!-- verify-plan:begin -->
| ID | Fires on | Required |
|---|---|---|
| T0 | always | `cargo fmt --check`; clippy `--all-features -D warnings`; `cargo test --workspace`; `./scripts/check-architecture.sh`; `cargo deny check`. A behavior change adds a public-API test and records under `Ran:` that it failed at the base commit. A refactor changes no assertion. |
| T1 | paths: `crates/protocol/src/{reduce,fold,phase,event,effect,authorizer,policy,spec}.rs`, `crates/harness/src/driver.rs` | A unit test per changed transition or guard (a changed constant changes its guard). `crates/protocol/tests/reduce_bounded.rs` enumerates every new state, event and effect. An authorizer change adds an allow and a deny test per affected effect. A cached or incremental fold adds a proptest that it equals the plain fold on every prefix. |
| T2 | paths: `crates/server/src/{store,postgres,inference,http}.rs`; lines±: `\bput_run\(`, `\bappend_events\(`, `\brecord_completion\(`, `\bdestroy\(`, `(?<!\.)\bis_terminal\b`, `\bRunStore\b`, `\bopen_turn\(`, `\baccept_subscription_completion\(` | If the diff adds, removes or reorders a run-log write or a sandbox destroy: tests on both stores, a forced-interleaving test per new ordering (the `WriteAfterSnapshot` pattern in `crates/server/tests/inference.rs`), and `formal/runlog` updated (then T10) unless the writer is `covered by <RunLog action>`. Otherwise declare `Models: runlog unaffected: no write`. |
| T3 | lines± in `crates/*/src/**`: `(?i)\blease`, `(?i)heartbeat`, `(?i)keepalive`, `(?i)\bfenc`, `(?i)\bp?expire\b`, `(?i)\bset_?nx\b`, `(?i)\bnx\b`, `(?i)\bb?rpop\b`, `(?i)\bb?lmove\b`, `(?i)\brpoplpush\b`, `(?i)\bxreadgroup\b`, `(?i)\bxautoclaim\b`, `(?i)\bxack\b`, `(?i)\binsert into\b`, `(?i)\bupdate\s+\w+\s+set\b`, `(?i)\bdelete\s+from\b`, `(?i)\bon conflict\b`, `(?i)\bfor update\b`, `(?i)\b[lr]push\b`, `(?i)\bxadd\b`, `(?i)\bhset\b`, `\bspawn\(`, `redis::`, `postgres::` | Flag: does this add or change a writer of shared persistent state? No: say why. Yes, and a model covers the resource: update it (T10). Yes, and none does: a new model in this PR (gol-verify "New model"). |
| T4 | paths: `crates/runtime-tokio/src/{journal,host}.rs`, `crates/runtime-tokio/src/bin/**` | A kill-point test in `crates/runtime-tokio/tests/replay_proof.rs` per new commit window, and a truncated-record test per new record format. |
| T5 | paths: `crates/workflow-{core,rhai,js}/**`, `crates/harness-core/**` | The differential test covers the new construct in every frontend, or tests its rejection. A `Decision` change needs owner approval (it changes the Bend laws). |
| T6 | paths: `experiments/bend/**`, `crates/workflow-bend/**`, `scripts/{install,verify}-bend.sh`, `docs/bend.md` | `./scripts/verify-bend.sh`. Follow gol-bend. |
| T7 | lines: `\bunsafe\b`, `extern "`, `libc::[a-z_]+\(`, `#\[no_mangle`, `#\[export_name`, `#\[link_section`, `allow\(unsafe_code` | Draft PR labelled `needs-human` unless an owner-written issue linked in the PR asks for this code. `// SAFETY:` on the block; a test that executes it; a proptest over the enclosing function if soundness rests on an index or arithmetic bound (a Kani harness is recommended); Miri or ASan when available, else `Not run:`. |
| T8 | lines: `Atomic[A-Z]`, `UnsafeCell`, `unsafe impl Send`, `unsafe impl Sync`, `Condvar` | A Loom test of that type if correctness depends on memory ordering; otherwise a comment naming what orders it. |
| T9 | paths: `crates/workflow-bend/src/boundary.rs`, `crates/harness/src/catalog.rs`, `crates/gateway/src/**`, `crates/proxy/src/**`; lines: `from_utf8`, `split_once`, `read_line`, `as_bytes\(\)\[` | Flag: a hand-written parser of input the repo does not control (network, child process, user file)? Yes: a proptest (no panic; accept or reject against an oracle), and a fuzz target if it is reachable from the network. |
| T10 | paths: `formal/**`, `scripts/{install,verify}-tla.sh` | `./scripts/verify-tla.sh`: every `.cfg` passes with deadlock checking on. FINDINGS records the command, pinned version, constants, distinct states and depth. Each new counterexample becomes a Rust test first. |
| T11 | paths: `crates/*/benches/**`; claim | Criterion before and after on one machine, in the PR. Never a gate. |
| T12 | paths: `AGENTS.md`, `CLAUDE.md`, `.claude/**`, `scripts/verify-*.sh`, `scripts/check-architecture.sh`, `scripts/ci-scope.sh`, `.github/**`, `deny.toml`, `.cargo/**`, `rust-toolchain{,.toml}`, `**/proptest-regressions/**`; lines± in `crates/**`: `#\[ignore`, `with_cases\(`, `\bcases\s*:\s*\d`, `cfg_attr\(miri`; lines± in `**/Cargo.toml`: `^\s*(doc)?test\s*=\s*false`; lines- in `crates/**`: `#\[(tokio::)?test\b`, `\bproptest!` | The owner applies the `verification:policy` label. List every removed or weakened check under `Not run:`. |
<!-- verify-plan:end -->

`claim` means the PR claims a speed change; add T11 yourself.

## Write or recommend

Agents write TLA+ only when T2 or T3 requires it, or when the task names a file under `formal/`. Whatever a fired row requires counts as asked: write it in this PR. If it needs a tool or CI job that does not exist yet (Kani, cargo-fuzz, a Miri, sanitizer or nightly TLC job), write the test or harness, mark it `Not run: <tool>: no CI job`, and ask for the `verification:new-tool` label. Lean, Apalache, TLAPS, shuttle, turmoil and new Bend programs are recommended, never written, unless the owner asks. Removing or replacing a verifier nobody asked about starts as a read-only audit (impeccable-rust audit mode).

## Budgets

- TLC at PR constants: at most 2 minutes per config you change. Larger constants run nightly. A new model is at most 250 lines, one per PR.
- `reduce_bounded` at most 30 seconds. proptest at 256 cases on PRs.
- A PR adds at most 2 minutes to the always-on CI path.

## Stop and report (draft PR, label `needs-human`)

- A fix would weaken a property, law, bound or fairness assumption, or change a model assumption.
- A law or invariant fails and the task did not ask for a spec change.
- Three fix attempts failed on the same check, or a suspected flake failed its one rerun.
- TLC on a config this PR changes exceeds 2 minutes, a new config exceeds 1M distinct states, or an existing config more than doubles the distinct states its FINDINGS records.

A missing local tool or service is not a stop. Run everything else and write `Not run: <check>: <tool> missing; CI job <name>`. Never report a weaker check under a stronger check's name.

## Do not

- Restate `reduce`, `fold`, the journal or a Bend law in TLA+, Lean or Bend; model code that does not exist; join independent machines in one spec.
- Run Loom around a pure function, Miri on crates whose tests execute no `unsafe`, Kani on serde, `String` or lookups, or TLA+ or Lean on a getter, a read-only route, a constant or config.
- Pass `-deadlock` or set `CHECK_DEADLOCK FALSE`, or add a stuttering step other than a guarded `Done`.
- Paste TLC logs into FINDINGS, gate on Criterion, or claim durability beyond SIGKILL (`runtime-tokio` never calls fsync).
- Regenerate expected output to make a test pass.
- Edit this section, a skill's charter or the impeccable-rust override header without the owner's approval (T12).

## PR declaration (every PR body)

```text
Verification
Triggers: T0 T2 T3=no(<reason>)     # what scripts/verify-plan.sh printed, flags answered inline, plus T11 if claimed
Ran: <command> -> <result>
Claims: <vocabulary term> <subject> <bounds>
Models: <model> updated | <model> new | runlog unaffected: no write | runlog unaffected: covered by <RunLog action> | n/a
Not run: <check>: <reason> | none
Recommended: <idea> | none
```

On failure: reproduce with the same command, then fix the code, not the check. A TLC counterexample becomes a deterministic Rust test before the fix.

## Skills

- `.claude/skills/gol-verify/SKILL.md`: the method charter, ownership map, claim vocabulary, conformance rule and TLA+ how-to. Load it whenever a row other than T0 fires.
- `.claude/skills/gol-bend/SKILL.md`: the Bend boundary, laws and claims. Load it for T6.
- `.claude/skills/impeccable-rust/SKILL.md`: general Rust practice. gol's overrides are at its top.

# Rust API and Test Practice

Public types that other crates construct with several required inputs use a typestate builder.

- Required inputs are type states. They are not optional fields checked at runtime.
- `PhantomData` marks the builder state. An illegal build order does not compile.
- Callees accept the finished type.

Tests are written before the behavior they describe.

- Derive the assertion from the spec, an invariant or a counterexample.
- Call the public API the way a dependent crate would.
- Assert a literal outcome.

Provider HTTP tests use wiremock.

Hot paths have Criterion benchmarks. The fold and the harness reducer are the first two. A benchmark is a measurement, not a gate, and not a claim that the algorithm is optimal.

Every crate root has `#![forbid(unsafe_code)]`. The one exception is `workflow-bend`, which denies it and allows it on `kill_group` alone (T7).

# Bend

Bend is an optional frontend for the counter workflow, pinned by `scripts/install-bend.sh`. `./scripts/verify-bend.sh` is the gate. A failing proof means stop, and a law is never weakened to make a proof pass. Everything else about Bend (its boundary, laws, claims and upgrades) is in `.claude/skills/gol-bend/SKILL.md`.

# Lock Files

`Cargo.lock` and `coworker/bun.lock` are committed. The workspace ships binaries, CI builds what the lock names, and `cargo deny` audits those exact versions.

Only the package manager writes a lock file: `cargo update -p <crate>`, `cargo generate-lockfile`, or `bun install`. The result is pushed with git as one ordinary commit next to the manifest change that caused it.

Never split a lock file into pieces, write it by hand, or have a workflow rebuild it. No workflow commits or pushes to a branch. An agent that cannot push a large file from a git checkout does not change dependencies.
