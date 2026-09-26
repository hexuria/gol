# Replay findings

`Replay.tla` is the design model of one counter journal in `crates/runtime-tokio`: `Perform` holds a result in memory, `Commit` is the only action that writes the journal, and `Crash` drops what was held. The Rust owner of the crash property is `crates/runtime-tokio/tests/replay_proof.rs`.

## Run

2026-09-26, `./scripts/verify-tla.sh`, which runs `java -XX:+UseParallelGC -jar ~/.local/tla/tla2tools.jar -workers auto -lncheck final -config Replay.cfg Replay.tla`. TLC2 Version 2.19 of 08 August 2024, from tla2tools v1.7.4. No constants. Deadlock checked.

| Config | Generated | Distinct | Depth | Result |
|---|---|---|---|---|
| `Replay.cfg` | 9 | 5 | 3 | no error |

## Negative control

Adding `EmptyStaysEmpty == [][(journal = "empty") => UNCHANGED journal]_vars` as a property fails: TLC reports it violated, because `Commit` writes the empty journal. The model can therefore reach a commit, and `HitSticks` is not vacuous.

## Mapping

| Invariant or property | Rust test of the same property |
|---|---|
| `JournaledResultForcesBranch` | `branch_on_recorded_counter` in `crates/workflow-core/src/program.rs` |
| `HeldIsNotAHit` | `held_bytes_are_not_a_hit_before_ok` in `crates/runtime-tokio/src/journal.rs` (it checks the log bytes, not a journal lookup) |
| `JournaledIdNotReexecuted`, `HitSticks` | `kill_after_commit_skips_the_counter` in `crates/runtime-tokio/tests/replay_proof.rs`: after the rerun the effect count stays 1 |
| a crash before commit performs again | `kill_before_commit_leaves_the_next_unstarted`: the effect count becomes 2 |
