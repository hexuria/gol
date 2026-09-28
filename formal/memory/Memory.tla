---- MODULE Memory ----
\* The memories table and the runs that write and read it at the same time:
\* Memory::write and Memory::read in memory::PostgresMemory
\* (crates/memory/src/lib.rs) and harness::InMemory
\* (crates/harness/src/memory.rs), called by the driver for MemoryWrite and
\* MemoryRead effects (crates/harness/src/driver.rs). Each call is one atomic
\* step: one SQL statement, or one lock.
\*
\* Design "old" keys an entry by its scope alone, as before C3. Design "new"
\* also keys it by its owner: here the organization scope, owned by the run's
\* tenant (protocol::memory_owner_id).
EXTENDS Integers

CONSTANTS Design, NRuns, NTenants

\* Runs are numbered, and each belongs to a tenant by its number.
Runs == 1..NRuns
TenantOf(r) == r % NTenants

None == 0
Unread == -1

VARIABLES store, pc, found

vars == <<store, pc, found>>

\* The key a run's organization memory is stored under.
Key(r) == IF Design = "old" THEN <<"organization", 0>> ELSE <<"organization", TenantOf(r)>>

Keys == {Key(r) : r \in Runs}

TypeOK ==
  /\ store \in [Keys -> Runs \cup {None}]
  /\ pc \in [Runs -> {"write", "read", "done"}]
  /\ found \in [Runs -> Runs \cup {None, Unread}]

Init ==
  /\ store = [k \in Keys |-> None]
  /\ pc = [r \in Runs |-> "write"]
  /\ found = [r \in Runs |-> Unread]

\* Run r writes its own value (its name) under its key: one upsert.
Write(r) ==
  /\ pc[r] = "write"
  /\ store' = [store EXCEPT ![Key(r)] = r]
  /\ pc' = [pc EXCEPT ![r] = "read"]
  /\ UNCHANGED found

\* Run r reads its key back: one select.
Read(r) ==
  /\ pc[r] = "read"
  /\ found' = [found EXCEPT ![r] = store[Key(r)]]
  /\ pc' = [pc EXCEPT ![r] = "done"]
  /\ UNCHANGED store

\* Every run has written and read. The only stuttering step.
Done ==
  /\ \A r \in Runs : pc[r] = "done"
  /\ UNCHANGED vars

Next == (\E r \in Runs : Write(r) \/ Read(r)) \/ Done

\* Fairness makes EveryRunReads a check that no run is stuck; each call in
\* the real code returns.
Spec == Init /\ [][Next]_vars /\ \A r \in Runs : WF_vars(Write(r) \/ Read(r))

\* A run reads only a value written by a run of its own tenant.
NoCrossScopeRead ==
  \A r \in Runs : found[r] \in Runs => TenantOf(found[r]) = TenantOf(r)

\* What a run reads was written: its own write comes first, so a read of its
\* key finds a value.
ReadsFindAValue == \A r \in Runs : found[r] # None

EveryRunReads == <>(\A r \in Runs : pc[r] = "done")

\* A control, not checked by Memory.cfg: a run reads only its own value. It
\* fails once two runs share a tenant, so the model does share memory within
\* a tenant, and NoCrossScopeRead is not met by keeping every run apart.
OnlyOwnValue == \A r \in Runs : found[r] \in Runs => found[r] = r
====
