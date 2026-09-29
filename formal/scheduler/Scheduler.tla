---- MODULE Scheduler ----
\* The scheduler (Phase 4.2, decisions 69A-72A) against one schedule trigger,
\* with its owner's stop (Phase 4.1). Writers of the trigger row and of the
\* runs it fires:
\* - each server's scheduler: `schedule_due` / `fire_due_trigger`
\*   (crates/server/src/scheduler.rs) reads the due trigger, fires its tick
\*   through `fire_trigger_at` (crates/server/src/triggers.rs), then moves it
\*   on with `TriggerStore::advance_trigger`. The fire stores the run
\*   (`spawner::enqueue_unless`: pend, put_run), reads the trigger again,
\*   and pushes the run, or holds it (cancels it, never pushed) if the
\*   trigger is paused by then.
\* - the owner's stop: `stop_owner` (crates/server/src/http.rs) pauses the
\*   owner's triggers, then records the stop (`put_stop`), which covers the
\*   runs stored before it.
\*
\* A run's id comes from the trigger and the tick (Design "derived"), and a
\* run stored under an id already stored is the one already there (put_run
\* stores once). The negative controls:
\* - "random": a fire makes a fresh id, so two schedulers on one tick make
\*   two runs (OneRunPerTick).
\* - "latecheck": the fire pushes before it reads the trigger again, so a run
\*   stored after the stop's record is pushed (NoRunEscapesStop).
\* - "blind": the scheduler moves the trigger to its tick + 1 without the
\*   condition that its tick is still the one read (69A), so a stale
\*   scheduler moves it back (TicksAdvance).
EXTENDS Integers, FiniteSets

CONSTANTS Design, Schedulers, MaxTick

Ticks == 1..MaxTick
Ids == IF Design = "random" THEN Schedulers \X Ticks ELSE Ticks
Id(s, t) == IF Design = "random" THEN <<s, t>> ELSE t
TickOf(id) == IF Design = "random" THEN id[2] ELSE id

VARIABLES
  next,      \* the trigger row's next tick (MaxTick + 1: none left in the model)
  enabled,   \* the trigger row's flag
  stop,      \* the owner's stop: "none", "paused" (its pause), "recorded"
  pc,        \* each scheduler: "idle", "read", "stored", "gated"
  seen,      \* each scheduler's tick, as it read it
  fresh,     \* whether this scheduler's store stored the run (not found it)
  status,    \* each run id: "none", "stored", "pushed", "held"
  afterStop  \* each run id: stored after the stop's record

vars == <<next, enabled, stop, pc, seen, fresh, status, afterStop>>

TypeOK ==
  /\ next \in 1..(MaxTick + 1)
  /\ enabled \in BOOLEAN
  /\ stop \in {"none", "paused", "recorded"}
  /\ pc \in [Schedulers -> {"idle", "read", "stored", "gated"}]
  /\ seen \in [Schedulers -> 0..MaxTick]
  /\ fresh \in [Schedulers -> BOOLEAN]
  /\ status \in [Ids -> {"none", "stored", "pushed", "held"}]
  /\ afterStop \in [Ids -> BOOLEAN]

Init ==
  /\ next = 1
  /\ enabled = TRUE
  /\ stop = "none"
  /\ pc = [s \in Schedulers |-> "idle"]
  /\ seen = [s \in Schedulers |-> 0]
  /\ fresh = [s \in Schedulers |-> FALSE]
  /\ status = [id \in Ids |-> "none"]
  /\ afterStop = [id \in Ids |-> FALSE]

\* due_triggers: the trigger is running and its tick is due.
Read(s) ==
  /\ pc[s] = "idle"
  /\ enabled
  /\ next <= MaxTick
  /\ seen' = [seen EXCEPT ![s] = next]
  /\ pc' = [pc EXCEPT ![s] = "read"]
  /\ UNCHANGED <<next, enabled, stop, fresh, status, afterStop>>

\* enqueue_unless's put_run: store once. "latecheck" pushes it here.
Store(s) ==
  LET id == Id(s, seen[s]) IN
  /\ pc[s] = "read"
  /\ IF status[id] = "none"
       THEN /\ status' = [status EXCEPT ![id] =
                            IF Design = "latecheck" THEN "pushed" ELSE "stored"]
            /\ afterStop' = [afterStop EXCEPT ![id] = (stop = "recorded")]
            /\ fresh' = [fresh EXCEPT ![s] = TRUE]
       ELSE /\ fresh' = [fresh EXCEPT ![s] = FALSE]
            /\ UNCHANGED <<status, afterStop>>
  /\ pc' = [pc EXCEPT ![s] = "stored"]
  /\ UNCHANGED <<next, enabled, stop, seen>>

\* The trigger read again: a run this scheduler stored is pushed if the
\* trigger runs, held otherwise ("latecheck": cancelled after its push).
Gate(s) ==
  LET id == Id(s, seen[s]) IN
  /\ pc[s] = "stored"
  /\ status' = IF ~fresh[s] THEN status
               ELSE IF Design = "latecheck"
                 THEN (IF enabled THEN status ELSE [status EXCEPT ![id] = "held"])
                 ELSE [status EXCEPT ![id] = IF enabled THEN "pushed" ELSE "held"]
  /\ pc' = [pc EXCEPT ![s] = "gated"]
  /\ UNCHANGED <<next, enabled, stop, seen, fresh, afterStop>>

\* advance_trigger (69A): from the tick read, only if it is still next.
Advance(s) ==
  /\ pc[s] = "gated"
  /\ next' = IF Design = "blind" THEN seen[s] + 1
             ELSE IF next = seen[s] THEN next + 1 ELSE next
  /\ pc' = [pc EXCEPT ![s] = "idle"]
  /\ UNCHANGED <<enabled, stop, seen, fresh, status, afterStop>>

\* The owner's stop: its pause, then its record.
Pause ==
  /\ stop = "none"
  /\ enabled' = FALSE
  /\ stop' = "paused"
  /\ UNCHANGED <<next, pc, seen, fresh, status, afterStop>>

Record ==
  /\ stop = "paused"
  /\ stop' = "recorded"
  /\ UNCHANGED <<next, enabled, pc, seen, fresh, status, afterStop>>

Step(s) == Read(s) \/ Store(s) \/ Gate(s) \/ Advance(s)

\* Every scheduler is idle and nothing is due, and a stop begun is recorded.
Done ==
  /\ \A s \in Schedulers : pc[s] = "idle"
  /\ (next > MaxTick \/ ~enabled)
  /\ stop # "paused"
  /\ UNCHANGED vars

Next == (\E s \in Schedulers : Step(s)) \/ Pause \/ Record \/ Done

\* Each scheduler's pass keeps running, and a stop begun finishes; the stop
\* itself may never come.
Spec ==
  /\ Init /\ [][Next]_vars
  /\ \A s \in Schedulers : WF_vars(Step(s))
  /\ WF_vars(Record)

\* One run for each tick, however many schedulers fired it.
OneRunPerTick ==
  \A t \in Ticks :
    Cardinality({id \in Ids : TickOf(id) = t /\ status[id] # "none"}) <= 1

\* A run stored after the stop's record (which covers only earlier runs) is
\* never pushed: no worker ever sees it.
NoRunEscapesStop == \A id \in Ids : afterStop[id] => status[id] # "pushed"

\* The trigger's next tick never moves back.
TicksAdvance == [][next' >= next]_next

\* While the trigger runs, every tick is fired: the schedule gets through.
EveryTickFires == <>[](next > MaxTick \/ ~enabled)
====
