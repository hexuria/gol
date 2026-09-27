---- MODULE AgentOwner ----
\* One stored agent manifest and the principals that race to put it: put_agent
\* in PostgresStore and InMemoryStore (crates/server/src/{postgres,store}.rs),
\* called by POST /v1/agents (create_agent in http.rs). Design "old" is the
\* unconditional upsert before B2. Design "new" inserts, or replaces the row
\* only when the same principal owns it, in one atomic step (one SQL
\* statement, or one lock).
EXTENDS Naturals

CONSTANTS Design, Principals, Puts

None == "none"

VARIABLES owner, first, done, stored

vars == <<owner, first, done, stored>>

TypeOK ==
  /\ owner \in Principals \cup {None}
  /\ first \in Principals \cup {None}
  /\ done \in [Principals -> 0..Puts]
  /\ stored \subseteq Principals

Init ==
  /\ owner = None
  /\ first = None
  /\ done = [p \in Principals |-> 0]
  /\ stored = {}

\* One put_agent by p, as one atomic step. `stored` records the principals
\* that were told PutAgent::Stored.
Put(p) ==
  /\ done[p] < Puts
  /\ done' = [done EXCEPT ![p] = @ + 1]
  /\ first' = IF first = None THEN p ELSE first
  /\ IF Design = "old" \/ owner = None \/ owner = p
       THEN owner' = p /\ stored' = stored \cup {p}
       ELSE UNCHANGED <<owner, stored>>

\* Every principal has made all its puts. The only stuttering step.
Done ==
  /\ \A p \in Principals : done[p] = Puts
  /\ UNCHANGED vars

Next == (\E p \in Principals : Put(p)) \/ Done

\* Fairness makes WritersFinish a check that no put is ever refused forever
\* by a stuck state; it is not a claim that clients keep calling.
Spec == Init /\ [][Next]_vars /\ \A p \in Principals : WF_vars(Put(p))

\* The principal that first stored the agent owns it for good.
FirstOwnerKeeps == first # None => owner = first

\* Only that principal is ever told its put was stored.
OnlyOwnerStores == stored \subseteq {first}

WritersFinish == <>(\A p \in Principals : done[p] = Puts)
====
