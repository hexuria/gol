# Memory findings

Checked on 2026-09-28. `Memory.tla` is the memories table and the runs that write and read it at the same time.

## Model

Resource: the memories table.
- `memory::PostgresMemory` (`crates/memory/src/lib.rs:155`): one upsert per write (`:167`) and one select per read (`:156`), each on the store's one connection behind its lock (`with_client`, `:71`).
- `harness::InMemory` (`crates/harness/src/memory.rs:56`): one lock per call.

Writers: runs of different owners. Each has a driver that performs `MemoryWrite` (`crates/harness/src/driver.rs:318`) and `MemoryRead` (`:298`) under the key `memory_key` (`:366`) builds from `protocol::memory_owner_id` (`crates/protocol/src/effect.rs:81`). The server gives every run the shared memory, whether `POST /v1/runs` runs it (`crates/server/src/http.rs:664`) or a queue worker does (`Open::execute`, `crates/server/src/worker.rs`). Today no server run writes memory, because Jev offers no memory effect; the drivers in the tests do. Each call is one atomic step.

`Design = "old"` keys an entry by its scope alone, as before C3. `Design = "new"` also keys it by its owner.

The model checks the organization scope, owned by the run's tenant, so `NoCrossScopeRead` means no run reads another tenant's organization memory. The other scopes are not all tenant-bound:
- User and agent memory follow their principal (issuer and subject) across that principal's tenants, by design. Session memory is keyed by the tenant too, so it does not.
- Global memory is denied by the authorizer.
- Each scope's key is checked in Rust (Mapping).

Runs are numbered `1..NRuns`, and run `r` belongs to tenant `r % NTenants`. Each run writes its own value, then reads its key back.

`Memory.cfg` checks `Design = "new"` with `NRuns = 2` and `NTenants = 2`: two runs of different tenants.

## Properties

- `NoCrossScopeRead`: a run reads only a value written by a run of its own tenant.
- `ReadsFindAValue`: a run's read finds a value, since its own write comes first.
- `EveryRunReads`: under weak fairness on each run, every run writes and reads.

## TLA+ Findings

`Design = "new"`, from `formal/memory`, as `scripts/verify-tla.sh` runs it:

```text
java -XX:+UseParallelGC -jar ~/.local/tla/tla2tools.jar -workers auto -lncheck final -config Memory.cfg Memory.tla
```

TLC2 Version 2.19 of 08 August 2024, from tla2tools v1.7.4, which `scripts/install-tla.sh` pins. The command has no `-deadlock`, so TLC checked deadlock. Every invariant and property passed, and no state is a deadlock: 14 states generated, 9 distinct, depth 5. `Done` is the only stuttering step.

Larger bounds, where runs 1 and 3 share a tenant: `NRuns = 3`, `NTenants = 2` gives 93 generated, 51 distinct, depth 7; `NRuns = 4`, `NTenants = 2` gives 629 generated, 289 distinct, depth 9. Neither has an error.

Negative controls, each on a copy of the config with `-workers 1`:

- `Design = "old"` violates `NoCrossScopeRead` in a 4-state trace: run 1 writes, run 2 of the other tenant writes the same key, and run 1 reads run 2's value.
- `OnlyOwnValue` (a run reads only its own value), with `NRuns = 3`, is violated in a 4-state trace: run 1 writes, run 3 of the same tenant writes, and run 1 reads run 3's value. So the model does share memory within a tenant, and `NoCrossScopeRead` does not hold merely because runs never meet.

## Mapping

- `NoCrossScopeRead`: `no_cross_scope_read` (`harness::memory_scenarios`). Sixteen runs of two tenants write and read organization memory on concurrent threads, against `InMemory` (`crates/harness/tests/memory_scopes.rs`) and `PostgresMemory` (`crates/memory/tests/recall.rs`). Racing threads are evidence, not a forcing test.
- The other scopes' keys are tested through the same scenarios on both memories, each run with its own `RunMemory` as on the server:
  - `run_memory_isolated_between_runs`
  - `step_memory_isolated_between_steps`
  - `agent_memory_survives_across_runs`
  - `user_memory_isolated_between_tenants`
  - `session_memory_belongs_to_its_user`
  - `workspace_memory_belongs_to_its_organization`
  - `global_memory_is_denied`
- The key itself is tested by the `memory_owner_tests` unit tests in `crates/protocol/src/effect.rs`, which build each scope's id and check that no two owners' ids run together.
- The counterexample trace for the unscoped design, forced: `no_cross_scope_read_in_the_model_trace` (run 1 writes, run 2 of another tenant writes, run 1 reads its own value), on both memories.
- The model's environment assumption is that each call is atomic. In Postgres, each call is one SQL statement. In memory, it is one lock scope.
- `EveryRunReads` assumes each call returns. In Postgres, a lock wait is bounded at 5 s and a statement at 10 s, unless the URL, role or database sets a non-zero bound. `a_write_waiting_on_a_lock_gives_up` (`crates/memory/tests/recall.rs`) checks the lock timeout (SQLSTATE 55P03).
- `ReadsFindAValue`: unlinked.
- Not modelled: a crash between a Postgres write and the run's `MemoryWritten` event leaves a row that no run log records.

Retire this model if memory moves to a store whose API takes the owner as a required, typed part of every call, so that an unscoped read cannot be written.
