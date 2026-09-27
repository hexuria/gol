# Agent owner findings

Checked on 2026-09-27. `AgentOwner.tla` is one stored agent manifest and the principals that race to put it.

## Model

Writers: `put_agent` in `PostgresStore` (`crates/server/src/postgres.rs`) and `InMemoryStore` (`crates/server/src/store.rs`), called by `POST /v1/agents` (`create_agent` in `crates/server/src/http.rs`), one per principal. Each put is one atomic step. In Postgres it is one statement: an insert that, on a conflicting id, replaces the row only when the stored owner has the same issuer and subject. In memory it is one lock.

`Design = "old"` is the unconditional upsert before B2: any principal's put replaced the row. `Design = "new"` is the conditional put.

`AgentOwner.cfg` checks `Design = "new"` with `Principals = {"alice", "bob"}` and `Puts = 2`.

## Properties

- `FirstOwnerKeeps`: once stored, the agent keeps the principal that first stored it.
- `OnlyOwnerStores`: only that principal is ever told `PutAgent::Stored`.
- `WritersFinish`: under weak fairness on each principal's put, every principal finishes its puts.

## TLA+ Findings

`Design = "new"`, from `formal/agentowner`, as `scripts/verify-tla.sh` runs it:

```text
java -XX:+UseParallelGC -jar ~/.local/tla/tla2tools.jar -workers auto -lncheck final -config AgentOwner.cfg AgentOwner.tla
```

TLC2 Version 2.19 of 08 August 2024, from tla2tools v1.7.4, which `scripts/install-tla.sh` pins. The command has no `-deadlock`, so TLC checked deadlock. Every invariant and property passed, and no state is a deadlock: 19 states generated, 13 distinct, depth 5. With `Principals = {"alice", "bob", "carol"}` and `Puts = 3`: 319 generated, 145 distinct, depth 10, no error. `Done` is the only stuttering step.

Negative controls, each on a copy of the config with `-workers 1`:

- `Design = "old"` violates `FirstOwnerKeeps` in a 3-state trace: alice puts, then bob's put replaces the row.
- `Design = "old"` violates `OnlyOwnerStores` in the same 3-state trace: bob is told his put was stored.
- On `Design = "new"`, an invariant saying bob is never told `Stored` is violated in a 2-state trace (bob puts first), so the checks are not vacuous.

## Mapping

- `FirstOwnerKeeps` and `OnlyOwnerStores`: `first_owner_keeps_the_agent` (`crates/server/tests/ownership.rs`, in-memory store, three racing threads, 20 rounds) and `first_owner_keeps_the_agent_in_postgres` (`crates/server/tests/pg_redis.rs`, three connections racing). Racing threads only sometimes hit a bad interleaving, so they are evidence, not a forcing test.
- The model's one environment assumption is that each put is atomic. In Postgres it is forced by `a_put_waits_for_a_concurrent_insert_of_the_same_id`: bob's put blocks on alice's uncommitted insert of the same id, then is refused. In memory it rests on reading the code: the check and the insert are one lock scope in `InMemoryStore::put_agent`.
- Replacement by the owner, from any tenant: `the_owner_replaces_from_any_tenant_and_no_one_else_does` and `the_owner_replaces_from_any_tenant_in_postgres`.
- The HTTP behaviour, a 409 for another principal and replacement for the owner: `only_the_owner_replaces_a_manifest`.
- `WritersFinish`: unlinked. Each put is one call that returns.

Retire this model if agent manifests move to a store whose API makes ownership immutable by construction.
