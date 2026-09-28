# Memory findings

Checked on 2026-09-28. `Memory.tla` is the memories table and the runs that write and read it at the same time.

## Model

Resource: the memories table. That is `memory::PostgresMemory` (`crates/memory/src/lib.rs`: one upsert per write, one select per read) and `harness::InMemory` (`crates/harness/src/memory.rs`: one lock per call).

Writers: runs of different owners, each with a driver that performs `MemoryWrite` and `MemoryRead` effects (`Driver::perform` in `crates/harness/src/driver.rs`). On the server, every run started by `POST /v1/runs` shares one memory (`AppState::memory`, `crates/server/src/http.rs`). Each call is one atomic step.

`Design = "old"` keys an entry by its scope alone, as before C3. `Design = "new"` also keys it by its owner, the id that `protocol::memory_owner_id` gives the run. The model checks the organization scope, owned by the run's tenant; the other scopes differ only in which id names the owner.

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
- The other scopes' keys are tested through the same scenarios on both memories:
  - `run_memory_isolated_between_runs`
  - `step_memory_isolated_between_steps`
  - `agent_memory_survives_across_runs`
  - `user_memory_isolated_between_tenants`
  - `session_memory_belongs_to_its_user`
- The key itself is tested by the `memory_owner_tests` unit tests in `crates/protocol/src/effect.rs`, which build each scope's id and check that no two owners' ids run together.
- The model's environment assumption is that each call is atomic. In Postgres, each call is one statement on a single-connection store behind a lock. In memory, it is one lock scope.
- `ReadsFindAValue` and `EveryRunReads`: unlinked. Each call is one statement or lock scope that returns.

Retire this model if memory moves to a store whose API takes the owner as a required, typed part of every call, so that an unscoped read cannot be written.
