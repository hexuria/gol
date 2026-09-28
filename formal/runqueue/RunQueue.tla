---- MODULE RunQueue ----
\* The Redis run queue, its workers and its reaper, and the run log they end:
\* RedisRunQueue (crates/server/src/queue.rs: one Lua script per claim, renew,
\* ack and reap; one LPUSH per push), Worker and Claim
\* (crates/server/src/worker.rs), the producer in create_run
\* (crates/server/src/http.rs), and RunStore::append_events for the record,
\* which refuses a terminal log (formal/runlog).
\*
\* Design "new": a claim moves the run to processing and leases it in one
\* step (SET NX: a run already leased is dropped instead); a worker that
\* cannot load its run releases it only while it holds the lease; a worker
\* acknowledges only after its record. Design "rpop" is the queue before C4:
\* a worker pops the run with no processing list or lease. Design "early"
\* acknowledges before recording. Design "loose" releases without holding
\* the lease.
EXTENDS Naturals, FiniteSets

CONSTANTS Design, Workers, Runs, MaxCrashes, MaxSlow, MaxReleases

None == "none"

VARIABLES produced, waiting, processing, lease, job, pc, ended, terminals,
          ackedOpen, crashes, slow, releases

vars == <<produced, waiting, processing, lease, job, pc, ended, terminals,
          ackedOpen, crashes, slow, releases>>

TypeOK ==
  /\ produced \subseteq Runs
  /\ waiting \subseteq Runs
  /\ processing \subseteq Runs
  /\ lease \in [Runs -> Workers \cup {None}]
  /\ job \in [Workers -> Runs \cup {None}]
  /\ pc \in [Workers -> {"idle", "claimed", "ran", "recorded"}]
  /\ ended \subseteq Runs
  /\ terminals \in [Runs -> 0..Cardinality(Workers) + MaxCrashes + MaxSlow]
  /\ ackedOpen \in BOOLEAN
  /\ crashes \in 0..MaxCrashes
  /\ slow \in 0..MaxSlow
  /\ releases \in 0..MaxReleases

Init ==
  /\ produced = {}
  /\ waiting = {}
  /\ processing = {}
  /\ lease = [r \in Runs |-> None]
  /\ job = [w \in Workers |-> None]
  /\ pc = [w \in Workers |-> "idle"]
  /\ ended = {}
  /\ terminals = [r \in Runs |-> 0]
  /\ ackedOpen = FALSE
  /\ crashes = 0
  /\ slow = 0
  /\ releases = 0

\* create_run stores the run as created and queued, then pushes it.
Push(r) ==
  /\ r \notin produced
  /\ produced' = produced \cup {r}
  /\ waiting' = waiting \cup {r}
  /\ UNCHANGED <<processing, lease, job, pc, ended, terminals, ackedOpen, crashes, slow, releases>>

\* One claim script: off the runs list, onto processing, leased to w.
Claim(w, r) ==
  /\ pc[w] = "idle"
  /\ r \in waiting
  /\ waiting' = waiting \ {r}
  /\ IF Design = "rpop"
       THEN /\ UNCHANGED <<processing, lease>>
            /\ job' = [job EXCEPT ![w] = r]
            /\ pc' = [pc EXCEPT ![w] = "claimed"]
       ELSE IF lease[r] # None
       \* SET NX finds the run leased (it was queued again while held): the
       \* entry is dropped, and the holder keeps the run.
       THEN UNCHANGED <<processing, lease, job, pc>>
       ELSE /\ processing' = processing \cup {r}
            /\ lease' = [lease EXCEPT ![r] = w]
            /\ job' = [job EXCEPT ![w] = r]
            /\ pc' = [pc EXCEPT ![w] = IF Design = "early" THEN "recorded" ELSE "claimed"]
  /\ UNCHANGED <<produced, ended, terminals, ackedOpen, crashes, slow, releases>>

\* The harness runs with Jev (Claim::prepare and execute): no shared write.
Run(w) ==
  /\ pc[w] = "claimed"
  /\ pc' = [pc EXCEPT ![w] = "ran"]
  /\ UNCHANGED <<produced, waiting, processing, lease, job, ended, terminals,
                 ackedOpen, crashes, slow, releases>>

\* One append of the harness's events, which end the run; the store refuses
\* it once the log is terminal.
Record(w) ==
  /\ pc[w] = "ran"
  /\ LET r == job[w] IN
       /\ ended' = ended \cup {r}
       /\ terminals' = [terminals EXCEPT ![r] = IF r \in ended THEN @ ELSE @ + 1]
  /\ pc' = [pc EXCEPT ![w] = IF Design = "early" THEN "idle" ELSE "recorded"]
  /\ job' = IF Design = "early" THEN [job EXCEPT ![w] = None] ELSE job
  /\ UNCHANGED <<produced, waiting, processing, lease, ackedOpen, crashes, slow, releases>>

\* One ack script: off processing, and the lease released if w holds it.
Ack(w) ==
  /\ pc[w] = "recorded"
  /\ LET r == job[w] IN
       /\ processing' = processing \ {r}
       /\ lease' = IF lease[r] = w THEN [lease EXCEPT ![r] = None] ELSE lease
       /\ ackedOpen' = (ackedOpen \/ r \notin ended)
  /\ pc' = [pc EXCEPT ![w] = IF Design = "early" THEN "ran" ELSE "idle"]
  /\ job' = IF Design = "early" THEN job ELSE [job EXCEPT ![w] = None]
  /\ UNCHANGED <<produced, waiting, ended, terminals, crashes, slow, releases>>

\* A worker dies holding a run, and restarts with nothing. Its lease stays
\* until it expires.
Crash(w) ==
  /\ pc[w] # "idle"
  /\ crashes < MaxCrashes
  /\ crashes' = crashes + 1
  /\ pc' = [pc EXCEPT ![w] = "idle"]
  /\ job' = [job EXCEPT ![w] = None]
  /\ UNCHANGED <<produced, waiting, processing, lease, ended, terminals, ackedOpen, slow, releases>>

\* A lease runs out: its holder died, or (at most MaxSlow times) its holder
\* is alive but missed its heartbeats.
Expire(r) ==
  /\ lease[r] # None
  /\ \/ job[lease[r]] # r /\ UNCHANGED slow
     \/ job[lease[r]] = r /\ slow < MaxSlow /\ slow' = slow + 1
  /\ lease' = [lease EXCEPT ![r] = None]
  /\ UNCHANGED <<produced, waiting, processing, job, pc, ended, terminals, ackedOpen, crashes, releases>>

\* One release script, for a claim that could not load or start its run: back
\* on the runs list, off processing, the lease gone, all only while w holds
\* the lease. In Design "loose" it acts whoever holds the lease. At most
\* MaxReleases load failures.
Release(w) ==
  /\ pc[w] = "claimed"
  /\ Design # "rpop"
  /\ releases < MaxReleases
  /\ releases' = releases + 1
  /\ LET r == job[w] IN
       IF Design = "loose" \/ lease[r] = w
         THEN /\ processing' = processing \ {r}
              /\ waiting' = waiting \cup {r}
              /\ lease' = IF lease[r] = w THEN [lease EXCEPT ![r] = None] ELSE lease
         ELSE UNCHANGED <<processing, waiting, lease>>
  /\ pc' = [pc EXCEPT ![w] = "idle"]
  /\ job' = [job EXCEPT ![w] = None]
  /\ UNCHANGED <<produced, ended, terminals, ackedOpen, crashes, slow>>

\* One reap script: a run in processing without a lease goes back on the list.
Reap(r) ==
  /\ r \in processing
  /\ lease[r] = None
  /\ processing' = processing \ {r}
  /\ waiting' = waiting \cup {r}
  /\ UNCHANGED <<produced, lease, job, pc, ended, terminals, ackedOpen, crashes, slow, releases>>

\* Every run is pushed and ended, and every worker is idle. The only
\* stuttering step.
Done ==
  /\ produced = Runs
  /\ ended = Runs
  /\ \A w \in Workers : pc[w] = "idle"
  /\ UNCHANGED vars

Next ==
  \/ \E r \in Runs : Push(r) \/ Expire(r) \/ Reap(r)
  \/ \E w \in Workers : Run(w) \/ Record(w) \/ Ack(w) \/ Crash(w) \/ Release(w)
                        \/ \E r \in Runs : Claim(w, r)
  \/ Done

\* Fairness for the producer, each worker's own steps and the reaper, which
\* keep running; none for Crash, and Expire only for a dead holder's lease,
\* which Redis always expires.
Spec ==
  /\ Init /\ [][Next]_vars
  /\ \A r \in Runs : WF_vars(Push(r)) /\ WF_vars(Reap(r))
  /\ \A r \in Runs : WF_vars(lease[r] # None /\ job[lease[r]] # r /\ Expire(r))
  /\ \A w \in Workers : WF_vars(Run(w) \/ Record(w) \/ Ack(w) \/ \E r \in Runs : Claim(w, r))

\* A pushed run that has not ended is on the list, in processing, or in a
\* live worker's hands: no crash loses it.
NoOrphan ==
  \A r \in produced :
    r \in ended \/ r \in waiting \/ r \in processing \/ \E w \in Workers : job[w] = r

\* No run leaves processing before its log is terminal.
AckAfterTerminal == ~ackedOpen

\* The store keeps one terminal event per run.
AtMostOneTerminal == \A r \in Runs : terminals[r] <= 1

EveryRunEnds == <>(ended = Runs)

\* A control, not checked by RunQueue.cfg: no two workers ever hold one run.
\* It fails once a slow worker's lease expires, so the model does reach the
\* two-workers case that AtMostOneTerminal guards.
OneWorkerPerRun ==
  \A r \in Runs : Cardinality({w \in Workers : job[w] = r}) <= 1
====
