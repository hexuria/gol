---- MODULE Scheduler ----
\* The scheduler (Phase 4.2, decisions 69A-72A) against one schedule trigger,
\* with its owner's stop and resume (Phase 4.1). Writers of the trigger row,
\* of the runs it fires and of the queue:
\* - each server's scheduler: `schedule_due` / `fire_due_trigger`
\*   (crates/server/src/scheduler.rs) reads the due trigger (its tick and
\*   generation), fires the tick through `fire_trigger_at`
\*   (crates/server/src/triggers.rs), then moves the trigger on with
\*   `TriggerStore::advance_trigger`, only if its tick is still the one read.
\*   The fire stores the run under an id of the trigger and the tick
\*   (`spawner::enqueue_fire`: pend, `put_run`, which stores a run once), then
\*   settles it (`spawner::settle_fire`): a run still waiting is gated (the
\*   trigger read again: running at the generation the fire read) and pushed
\*   once (`RedisRunQueue::push_once`: not if queued or claimed), or held
\*   (cancelled, never pushed). A settle that fails leaves the run stored and
\*   pending, and the tick unmoved.
\* - the queue sweep (`worker::sweep`) settles a trigger's run it finds
\*   pending the same way (`triggers::settle_fired_run`), by the generation
\*   the run recorded.
\* - the owner's stop (`stop_owner`, crates/server/src/http.rs) pauses the
\*   triggers, then records the stop; a resume (`resume_trigger`) sets the
\*   trigger running again at a new generation and its next tick after now.
\*
\* Design "derived" is the code. The negative controls:
\* - "random": a fire makes a fresh id: two runs for one tick (OneRunPerTick).
\* - "nogen": the gate checks only that the trigger runs: a fire across a
\*   pause and a resume pushes (NoPushAcrossPause).
\* - "pushtwice": the push does not check the queue: two settlers queue one
\*   run twice (PushedOnce).
\* - "blind": the advance lacks its condition: the tick moves back
\*   (TicksAdvance).
\* - "nofire": a fire stores nothing and the trigger moves on
\*   (PassedTicksFired).
\* - "nosweep": no sweep, so a failed settle leaves its run stored for good
\*   (StoredRunsSettle).
\*
\* Not modelled: delete, the missed-tick rules, and the first read's `Moved`
\* answer (a fire that finds its tick moved fires nothing), which only take
\* behaviours away.
EXTENDS Integers, FiniteSets

CONSTANTS Design, Schedulers, MaxTick

Ticks == 1..MaxTick
Ids == IF Design = "random" THEN Schedulers \X Ticks ELSE Ticks
Id(s, t) == IF Design = "random" THEN <<s, t>> ELSE t
TickOf(id) == IF Design = "random" THEN id[2] ELSE id

VARIABLES
  next,      \* the trigger's next tick (MaxTick + 1: none left in the model)
  enabled,   \* the trigger runs
  gen,       \* the trigger's generation (a resume bumps it)
  stop,      \* "none", "paused", "recorded", "resumed"
  pauses,    \* how many pauses so far
  pc,        \* each scheduler: "idle", "read", "stored", "settled"
  seen,      \* each scheduler's tick, as it read it
  seenGen,   \* each scheduler's generation, as it read it
  seenAt,    \* each scheduler's pauses, when it read
  status,    \* each run id: "none", "stored", "pushed", "held"
  runGen,    \* each run id: the generation its fire read
  readAt,    \* each run id: the pauses when its fire read
  pushes,    \* each run id: how many times it was queued
  crossed    \* each run id: pushed after a pause since its fire's read

vars == <<next, enabled, gen, stop, pauses, pc, seen, seenGen, seenAt, status,
          runGen, readAt, pushes, crossed>>

TypeOK ==
  /\ next \in 1..(MaxTick + 2)
  /\ enabled \in BOOLEAN
  /\ gen \in 0..1
  /\ stop \in {"none", "paused", "recorded", "resumed"}
  /\ pauses \in 0..1
  /\ pc \in [Schedulers -> {"idle", "read", "stored", "settled"}]
  /\ seen \in [Schedulers -> 0..MaxTick]
  /\ seenGen \in [Schedulers -> 0..1]
  /\ seenAt \in [Schedulers -> 0..1]
  /\ status \in [Ids -> {"none", "stored", "pushed", "held"}]
  /\ runGen \in [Ids -> 0..1]
  /\ readAt \in [Ids -> 0..1]
  /\ pushes \in [Ids -> 0..2]
  /\ crossed \in [Ids -> BOOLEAN]

Init ==
  /\ next = 1 /\ enabled = TRUE /\ gen = 0 /\ stop = "none" /\ pauses = 0
  /\ pc = [s \in Schedulers |-> "idle"]
  /\ seen = [s \in Schedulers |-> 0]
  /\ seenGen = [s \in Schedulers |-> 0]
  /\ seenAt = [s \in Schedulers |-> 0]
  /\ status = [id \in Ids |-> "none"]
  /\ runGen = [id \in Ids |-> 0]
  /\ readAt = [id \in Ids |-> 0]
  /\ pushes = [id \in Ids |-> 0]
  /\ crossed = [id \in Ids |-> FALSE]

Row == <<next, enabled, gen, stop, pauses>>
Runs == <<status, runGen, readAt, pushes, crossed>>

\* due_triggers and the fire's first read.
Read(s) ==
  /\ pc[s] = "idle" /\ enabled /\ next <= MaxTick
  /\ seen' = [seen EXCEPT ![s] = next]
  /\ seenGen' = [seenGen EXCEPT ![s] = gen]
  /\ seenAt' = [seenAt EXCEPT ![s] = pauses]
  /\ pc' = [pc EXCEPT ![s] = "read"]
  /\ UNCHANGED <<Row, Runs>>

\* enqueue_fire's put_run: stores the run once, recording what it read.
Store(s) ==
  LET id == Id(s, seen[s]) IN
  /\ pc[s] = "read"
  /\ IF status[id] = "none" /\ Design # "nofire"
       THEN /\ status' = [status EXCEPT ![id] = "stored"]
            /\ runGen' = [runGen EXCEPT ![id] = seenGen[s]]
            /\ readAt' = [readAt EXCEPT ![id] = seenAt[s]]
       ELSE UNCHANGED <<status, runGen, readAt>>
  /\ pc' = [pc EXCEPT ![s] = "stored"]
  /\ UNCHANGED <<Row, seen, seenGen, seenAt, pushes, crossed>>

\* settle_fire: a waiting run gated, then pushed once or held; one gate
\* that reads the trigger, one push script.
Settle(id, g) ==
  /\ status[id] = "stored"
  /\ IF enabled /\ (Design = "nogen" \/ gen = g)
       THEN /\ status' = [status EXCEPT ![id] = "pushed"]
            /\ pushes' = [pushes EXCEPT ![id] = @ + 1]
            /\ crossed' = [crossed EXCEPT ![id] = pauses # readAt[id]]
       ELSE /\ status' = [status EXCEPT ![id] = "held"]
            /\ UNCHANGED <<pushes, crossed>>
  /\ UNCHANGED <<runGen, readAt>>

\* A settler that finds the run already pushed ("pushtwice": pushes again).
Found(id) ==
  /\ status[id] = "pushed"
  /\ IF Design = "pushtwice" /\ pushes[id] < 2
       THEN pushes' = [pushes EXCEPT ![id] = @ + 1]
       ELSE UNCHANGED pushes
  /\ UNCHANGED <<status, runGen, readAt, crossed>>

Gate(s) ==
  LET id == Id(s, seen[s]) IN
  /\ pc[s] = "stored"
  /\ \/ Settle(id, runGen[id])
     \/ Found(id)
     \/ status[id] \in {"none", "held"} /\ UNCHANGED Runs
  /\ pc' = [pc EXCEPT ![s] = "settled"]
  /\ UNCHANGED <<Row, seen, seenGen, seenAt>>

\* A settle that fails: the run stays stored, the tick stays, the pass ends.
Fail(s) ==
  /\ pc[s] = "stored"
  /\ status[Id(s, seen[s])] = "stored"
  /\ pc' = [pc EXCEPT ![s] = "idle"]
  /\ UNCHANGED <<Row, seen, seenGen, seenAt, Runs>>

\* advance_trigger (69A): from the tick read, only if it is still next.
Advance(s) ==
  /\ pc[s] = "settled"
  /\ next' = IF Design = "blind" THEN seen[s] + 1
             ELSE IF next = seen[s] THEN next + 1 ELSE next
  /\ pc' = [pc EXCEPT ![s] = "idle"]
  /\ UNCHANGED <<enabled, gen, stop, pauses, seen, seenGen, seenAt, Runs>>

\* The queue sweep settles a stored run by its recorded generation.
Sweep(id) ==
  /\ Design # "nosweep"
  /\ Settle(id, runGen[id])
  /\ UNCHANGED <<Row, pc, seen, seenGen, seenAt>>

Pause ==
  /\ stop = "none"
  /\ enabled' = FALSE /\ stop' = "paused" /\ pauses' = pauses + 1
  /\ UNCHANGED <<next, gen, pc, seen, seenGen, seenAt, Runs>>

Record ==
  /\ stop = "paused" /\ stop' = "recorded"
  /\ UNCHANGED <<next, enabled, gen, pauses, pc, seen, seenGen, seenAt, Runs>>

\* resume_trigger: running again at a new generation, from the next tick
\* after now (it owes nothing for the ticks it was paused).
Resume ==
  /\ stop = "recorded"
  /\ enabled' = TRUE /\ gen' = gen + 1 /\ stop' = "resumed"
  /\ next' = IF next <= MaxTick THEN next + 1 ELSE next
  /\ UNCHANGED <<pauses, pc, seen, seenGen, seenAt, Runs>>

Step(s) == Read(s) \/ Store(s) \/ Gate(s) \/ Fail(s) \/ Advance(s)

Done ==
  /\ \A s \in Schedulers : pc[s] = "idle"
  /\ (next > MaxTick \/ ~enabled)
  /\ \A id \in Ids : status[id] # "stored" \/ Design = "nosweep"
  /\ UNCHANGED vars

Next ==
  \/ \E s \in Schedulers : Step(s)
  \/ \E id \in Ids : Sweep(id)
  \/ Pause \/ Record \/ Resume \/ Done

\* Each scheduler's pass keeps running, and so does the sweep. A pass that
\* fails may fail again: only the gate's success is fair once it is taken.
Spec ==
  /\ Init /\ [][Next]_vars
  /\ \A s \in Schedulers : WF_vars(Read(s) \/ Store(s) \/ Gate(s) \/ Advance(s))
  /\ \A id \in Ids : WF_vars(Sweep(id))

OneRunPerTick ==
  \A t \in Ticks : Cardinality({id \in Ids : TickOf(id) = t /\ status[id] # "none"}) <= 1

\* A run is queued at most once.
PushedOnce == \A id \in Ids : pushes[id] <= 1

\* No run whose fire read the trigger before a pause is pushed after it:
\* the stop covers the runs stored before its record, and a resumed trigger
\* owes nothing for the ticks it was paused.
NoPushAcrossPause == \A id \in Ids : ~crossed[id]

\* A run is held only because a pause came after its fire's read.
HeldOnlyAcrossPause == \A id \in Ids : status[id] = "held" => readAt[id] < pauses

\* Every tick the trigger has moved past has a run, unless a resume skipped
\* it.
PassedTicksFired ==
  \A t \in Ticks : (t < next /\ stop # "resumed") =>
    \E id \in Ids : TickOf(id) = t /\ status[id] # "none"

TicksAdvance == [][next' >= next]_next

\* Every stored run is settled: pushed or held.
StoredRunsSettle == <>[](\A id \in Ids : status[id] # "stored")

\* While the trigger runs, the schedule gets through its ticks.
EveryTickFires == <>[](next > MaxTick \/ ~enabled)
====
