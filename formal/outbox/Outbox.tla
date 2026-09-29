---- MODULE Outbox ----
\* The per-owner outbox (Phase 3.1, decisions 16A and 36A-39A): every event a
\* run store keeps also gets an outbox row numbered by the owner's counter, in
\* the same transaction. The writers are the store's appends for runs of one
\* owner: RunStore::put_run, append_events and append_events_after in
\* PostgresStore (crates/server/src/postgres.rs) and InMemoryStore
\* (crates/server/src/store.rs). A reader resumes from the last sequence
\* number it saw, as GET /v1/stream will with Last-Event-ID (Phase 3.2).
\*
\* One append is one transaction: lock the run's row (select ... for update),
\* take the next sequence number, then write the event and outbox rows and
\* commit, which releases the locks and makes the rows visible at once.
\* put_run is a writer of a new run's row, with the same steps.
\*
\* Design "new" takes the owner's counter row under its own row lock, after
\* the run row (16A). The negative controls:
\* - "noLock" reads the counter without locking it: two appends to different
\*   runs take the same numbers (UniqueSeqs).
\* - "sequence" takes numbers from a global sequence that is not rolled back
\*   and commits in any order (16B): a reader passes a number that commits
\*   later (NoSkip), and an aborted-looking hole stays (Contiguous).
\* - "inverted" has one write path (writer 1) take the counter before the
\*   run row while the others keep run then counter: two appends to one run
\*   deadlock.
EXTENDS Integers, FiniteSets

CONSTANTS Design, NWriters, EventsPer

\* Writers are numbered: the first two append to run 1 (a worker and a late
\* message on one run), and each later one to a run of its own.
Writers == 1..NWriters
RunOf(w) == IF w = 1 THEN 1 ELSE w - 1
Runs == {RunOf(w) : w \in Writers}
Free == 0

\* Whether writer w takes the counter first (design "inverted", writer 1).
First(w) == Design = "inverted" /\ w = 1

VARIABLES
  pc,       \* each writer: "start", "run", "taken", "done"
  runLock,  \* each run's row lock holder, or Free
  ctrLock,  \* the owner's counter row lock holder, or Free
  counter,  \* the counter row's committed value
  seqNext,  \* design "sequence": the global sequence's next value
  taken,    \* each writer's first number, once taken (0 before)
  log,      \* each run's committed length
  outbox,   \* committed rows: [seq, run, runSeq]
  cursor,   \* the reader's last sequence number seen
  seen      \* the rows the reader has delivered

vars == <<pc, runLock, ctrLock, counter, seqNext, taken, log, outbox, cursor, seen>>

Row == [seq : Nat, run : Runs, runSeq : Nat]

TypeOK ==
  /\ pc \in [Writers -> {"start", "run", "taken", "done"}]
  /\ runLock \in [Runs -> Writers \cup {Free}]
  /\ ctrLock \in Writers \cup {Free}
  /\ counter \in Nat /\ seqNext \in Nat
  /\ taken \in [Writers -> Nat]
  /\ log \in [Runs -> Nat]
  /\ outbox \subseteq Row
  /\ cursor \in Nat /\ seen \subseteq Row

Init ==
  /\ pc = [w \in Writers |-> "start"]
  /\ runLock = [r \in Runs |-> Free]
  /\ ctrLock = Free
  /\ counter = 0 /\ seqNext = 1
  /\ taken = [w \in Writers |-> 0]
  /\ log = [r \in Runs |-> 0]
  /\ outbox = {} /\ cursor = 0 /\ seen = {}

\* select 1 from runs where id = $1 for update: waits while another holds it.
LockRun(w) ==
  /\ runLock[RunOf(w)] = Free
  /\ \/ ~First(w) /\ pc[w] = "start" /\ pc' = [pc EXCEPT ![w] = "run"]
     \/ First(w) /\ pc[w] = "taken" /\ UNCHANGED pc
  /\ runLock' = [runLock EXCEPT ![RunOf(w)] = w]
  /\ UNCHANGED <<ctrLock, counter, seqNext, taken, log, outbox, cursor, seen>>

\* The next sequence number: the counter row under its lock ("new",
\* "inverted"), the counter read without a lock ("noLock"), or nextval
\* ("sequence", which takes EventsPer numbers at once and never rolls back).
Take(w) ==
  /\ \/ ~First(w) /\ pc[w] = "run"
     \/ First(w) /\ pc[w] = "start"
  /\ pc' = [pc EXCEPT ![w] = "taken"]
  /\ CASE Design \in {"new", "inverted"} ->
            /\ ctrLock = Free
            /\ ctrLock' = w
            /\ taken' = [taken EXCEPT ![w] = counter + 1]
            /\ UNCHANGED seqNext
       [] Design = "noLock" ->
            /\ taken' = [taken EXCEPT ![w] = counter + 1]
            /\ UNCHANGED <<ctrLock, seqNext>>
       [] Design = "sequence" ->
            /\ taken' = [taken EXCEPT ![w] = seqNext]
            /\ seqNext' = seqNext + EventsPer
            /\ UNCHANGED ctrLock
  /\ UNCHANGED <<runLock, counter, log, outbox, cursor, seen>>

\* Insert the events and their outbox rows, update the counter, commit:
\* visible at once, and every lock released.
Commit(w) ==
  /\ pc[w] = "taken"
  /\ runLock[RunOf(w)] = w
  /\ LET r == RunOf(w)
         rows == {[seq |-> taken[w] + i, run |-> r, runSeq |-> log[r] + i + 1] :
                    i \in 0..(EventsPer - 1)}
     IN /\ outbox' = outbox \cup rows
        /\ log' = [log EXCEPT ![r] = log[r] + EventsPer]
        /\ runLock' = [runLock EXCEPT ![r] = Free]
  /\ counter' = IF Design = "sequence" THEN counter
                ELSE taken[w] + EventsPer - 1
  /\ ctrLock' = IF ctrLock = w THEN Free ELSE ctrLock
  /\ pc' = [pc EXCEPT ![w] = "done"]
  /\ UNCHANGED <<seqNext, taken, cursor, seen>>

\* The reader takes every committed row after its cursor, in order, and moves
\* its cursor to the last one (one select ... where seq > $cursor).
Read ==
  LET next == {e \in outbox : e.seq > cursor}
  IN /\ next # {}
     /\ seen' = seen \cup next
     /\ cursor' = CHOOSE s \in {e.seq : e \in next} :
                    \A t \in {e.seq : e \in next} : t <= s
     /\ UNCHANGED <<pc, runLock, ctrLock, counter, seqNext, taken, log, outbox>>

\* Every writer committed and the reader has read everything. The only
\* stuttering step.
Done ==
  /\ \A w \in Writers : pc[w] = "done"
  /\ \A e \in outbox : e.seq <= cursor
  /\ UNCHANGED vars

Next == (\E w \in Writers : LockRun(w) \/ Take(w) \/ Commit(w)) \/ Read \/ Done

\* A writer's statements return (the store's calls are bounded), and the
\* reader keeps polling while its stream is open.
Spec == Init /\ [][Next]_vars
          /\ \A w \in Writers : WF_vars(LockRun(w) \/ Take(w) \/ Commit(w))
          /\ WF_vars(Read)

\* No two rows share a sequence number.
UniqueSeqs == \A e, f \in outbox : e.seq = f.seq => e = f

\* The committed numbers are 1..n with no hole.
Contiguous == {e.seq : e \in outbox} = 1..Cardinality(outbox)

\* A run's events keep their order in the outbox.
RunOrder ==
  \A e, f \in outbox : e.run = f.run /\ e.runSeq < f.runSeq => e.seq < f.seq

\* A reader never passes a row it has not delivered: resuming from its
\* cursor loses nothing (16A: "no skipped events on resume").
NoSkip == \A e \in outbox : e.seq <= cursor => e \in seen

EveryWriterCommits == <>(\A w \in Writers : pc[w] = "done")
ReaderCatchesUp == <>[](\A e \in outbox : e \in seen)
====
