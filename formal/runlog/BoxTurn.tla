---- MODULE BoxTurn ----
\* A Box background coworker turn (the 67A follow-up, decisions 82A-86A): queue
\* workers that each run one delivery (an attempt) of the turn in a sandbox of
\* their own, the owner's stop, and the reaper. It abstracts:
\* - Claim::prepare (crates/server/src/worker.rs): a claim counts a start and
\*   takes the lease; past max_deliveries it only cleans up, then fails the turn;
\* - Worker::run_turn: RunStarted, the stop check and a lease check, the
\*   provision of gol-box-<run>-<n>, a lease check, the gateway call, a lease
\*   check, then the sandboxes of attempts 1 to its own destroyed, each confirmed
\*   gone, a lease check, and the terminal event appended;
\* - stop() (crates/server/src/http.rs): it records the stop, and cancels only a
\*   turn no worker has started;
\* - the reaper: a lease that ran out puts the run back on the queue, and it
\*   removes every gol-box sandbox the host lists whose turn has ended.
\* Sandbox calls are one step each; destroying a name not up succeeds. The log
\* is "open" or "ended"; the store refuses a second end (RunLog).
\*
\* Design "new" is the code. The negative controls:
\*   "shared"    one sandbox name for every attempt (the #83 design);
\*   "nofence"   no lease check after the provision or the call: it holds every
\*               property; those checks save a call and a cleanup, not a sandbox;
\*   "cleanlatest" no lease checks, and a cleanup of attempts up to the
\*               latest start instead of the worker's own;
\*   "stopall"   the stop cancels a started turn itself;
\*   "nocleanup" past max_deliveries the turn is failed without cleaning up;
\*   "nosweep"   the reaper does not clean up after an ended turn;
\*   "set"       the reaper removes only what a Redis set names: a worker adds
\*               its name before it provisions, and a removal confirmed gone
\*               takes the name off (the first 86A).
EXTENDS Naturals, FiniteSets

CONSTANTS Design, Workers, MaxDeliveries, MaxFails, MaxCrashes, MaxLapses

Cap == MaxDeliveries + 1
Attempts == 1..Cap
\* An attempt's sandbox name: gol-box-<run>-<n> (82A), or one name for all.
N(a) == IF Design = "shared" THEN 1 ELSE a
NamesUpTo(n) == {N(a) : a \in 1..n}

Active == {"claimed", "recorded", "provisioned", "calling", "called", "dropping"}

\* History: names a cleanup confirmed gone (cleared), and those provisioned
\* after that by a worker that had lost its lease (late).
VARIABLES log, started, stopReq, starts, queued, holder, up, named,
          pc, att, why, todo, fails, crashes, lapses, cleared, late

vars == <<log, started, stopReq, starts, queued, holder, up, named,
          pc, att, why, todo, fails, crashes, lapses, cleared, late>>

TypeOK ==
  /\ log \in {"open", "ended"}
  /\ started \in BOOLEAN /\ stopReq \in BOOLEAN
  /\ starts \in 0..Cap /\ queued \in BOOLEAN /\ holder \in Workers \cup {"none"}
  /\ up \subseteq Attempts /\ named \subseteq Attempts
  /\ pc \in [Workers -> {"idle", "gated", "cleaning", "ending", "appending", "dead"} \cup Active]
  /\ att \in [Workers -> 0..Cap]
  /\ why \in [Workers -> {"none", "done", "stop", "max"}]
  /\ todo \in [Workers -> SUBSET Attempts]
  /\ fails \in 0..MaxFails /\ crashes \in 0..MaxCrashes /\ lapses \in 0..MaxLapses
  /\ cleared \subseteq Attempts /\ late \subseteq Attempts

\* The turn is stored queued and pushed (the 202).
Init ==
  /\ log = "open" /\ started = FALSE /\ stopReq = FALSE
  /\ starts = 0 /\ queued = TRUE /\ holder = "none"
  /\ up = {} /\ named = {}
  /\ pc = [w \in Workers |-> "idle"] /\ att = [w \in Workers |-> 0]
  /\ why = [w \in Workers |-> "none"] /\ todo = [w \in Workers |-> {}]
  /\ fails = 0 /\ crashes = 0 /\ lapses = 0
  /\ cleared = {} /\ late = {}

\* The lease is the claim's: one worker runs one claim at a time.
Holds(w) == holder = w
Set(f, w, v) == [f EXCEPT ![w] = v]
\* Going on to clean up, for a reason: the sandboxes of attempts 1 to n.
ToClean(w, r, n) == /\ pc' = Set(pc, w, "cleaning") /\ why' = Set(why, w, r)
                    /\ todo' = Set(todo, w, NamesUpTo(n))

\* Claim and prepare: an ended turn is acknowledged; otherwise a start is
\* counted and the lease taken. Past max_deliveries: cleanup only (84A).
Claim(w) ==
  /\ pc[w] = "idle" /\ queued
  /\ queued' = FALSE
  /\ IF log = "ended"
       THEN UNCHANGED <<starts, holder, pc, att, why, todo>>
       ELSE LET n == IF starts < Cap THEN starts + 1 ELSE Cap IN
            /\ starts' = n /\ holder' = w /\ att' = Set(att, w, n)
            /\ IF starts + 1 > MaxDeliveries
                 THEN ToClean(w, "max", n)
                 ELSE pc' = Set(pc, w, "gated") /\ UNCHANGED <<why, todo>>
  /\ UNCHANGED <<log, started, stopReq, up, named, fails, crashes, lapses, cleared, late>>

\* RunStarted (refused once ended), the stop check and the lease check; then
\* the provision ("set": the name goes into the set first).
Begin(w) ==
  /\ pc[w] = "gated"
  /\ IF log = "ended" \/ ~Holds(w)
       THEN /\ pc' = Set(pc, w, "idle")
            /\ UNCHANGED <<started, why, todo, up, named, late>>
       ELSE /\ started' = TRUE
            /\ IF stopReq
                 THEN ToClean(w, "stop", att[w]) /\ UNCHANGED <<up, named, late>>
                 ELSE /\ pc' = Set(pc, w, "claimed") /\ UNCHANGED <<why, todo, up, named, late>>
  /\ UNCHANGED <<log, stopReq, starts, queued, holder, att, fails, crashes, lapses, cleared>>

\* "set": the name goes into the set, one step before the provision.
Record(w) ==
  /\ Design = "set" /\ pc[w] = "claimed"
  /\ named' = named \cup {N(att[w])} /\ pc' = Set(pc, w, "recorded")
  /\ UNCHANGED <<log, started, stopReq, starts, queued, holder, up, att, why, todo,
                 fails, crashes, lapses, cleared, late>>

Provision(w) ==
  /\ pc[w] = IF Design = "set" THEN "recorded" ELSE "claimed"
  /\ up' = up \cup {N(att[w])} /\ pc' = Set(pc, w, "provisioned")
  /\ late' = IF N(att[w]) \in cleared THEN late \cup {N(att[w])} ELSE late
  /\ UNCHANGED <<log, started, stopReq, starts, queued, holder, named, att, why, todo,
                 fails, crashes, lapses, cleared>>

\* The lease checks after the provision, after the gateway call, and before
\* the terminal event (85A): a worker that lost its lease removes its own
\* sandbox and stops, or, its cleanup done, stops without ending the turn.
Fence(w) ==
  /\ pc[w] \in {"provisioned", "called", "ending"}
  /\ IF Holds(w) \/ Design = "cleanlatest" \/ (Design = "nofence" /\ pc[w] # "ending")
       THEN CASE pc[w] = "provisioned" -> pc' = Set(pc, w, "calling") /\ UNCHANGED <<why, todo>>
              [] pc[w] = "called" ->
                   ToClean(w, "done", IF Design = "cleanlatest" THEN starts ELSE att[w])
              [] OTHER -> pc' = Set(pc, w, "appending") /\ UNCHANGED <<why, todo>>
       ELSE pc' = Set(pc, w, IF pc[w] = "ending" THEN "idle" ELSE "dropping")
            /\ UNCHANGED <<why, todo>>
  /\ UNCHANGED <<log, started, stopReq, starts, queued, holder, up, named, att,
                 fails, crashes, lapses, cleared, late>>

\* The gateway call returns. A call that started finishes, stop or not (66A).
Call(w) ==
  /\ pc[w] = "calling"
  /\ pc' = Set(pc, w, "called")
  /\ UNCHANGED <<log, started, stopReq, starts, queued, holder, up, named, att, why, todo,
                 fails, crashes, lapses, cleared, late>>

\* Its own sandbox, removed; a removal that fails leaves it for the reaper.
Drop(w) ==
  /\ pc[w] = "dropping"
  /\ pc' = Set(pc, w, "idle")
  /\ \/ up' = up \ {N(att[w])} /\ named' = named \ {N(att[w])} /\ UNCHANGED fails
     \/ fails < MaxFails /\ fails' = fails + 1 /\ UNCHANGED <<up, named>>
  /\ UNCHANGED <<log, started, stopReq, starts, queued, holder, att, why, todo,
                 crashes, lapses, cleared, late>>

\* Destroy each attempt's sandbox, confirmed gone, one at a time; then on to
\* the terminal event (84A). A removal that fails leaves the turn open, and
\* the claim is released for another try.
Clean(w) ==
  /\ pc[w] = "cleaning"
  /\ IF todo[w] = {} \/ (Design = "nocleanup" /\ why[w] = "max")
       THEN /\ pc' = Set(pc, w, "ending")
            /\ UNCHANGED <<log, queued, holder, up, named, todo, fails, cleared>>
       ELSE \E n \in todo[w] :
              \/ /\ up' = up \ {n} /\ named' = named \ {n}
                 /\ todo' = Set(todo, w, todo[w] \ {n}) /\ cleared' = cleared \cup {n}
                 /\ UNCHANGED <<log, queued, holder, pc, fails>>
              \/ /\ fails < MaxFails /\ fails' = fails + 1
                 /\ pc' = Set(pc, w, "idle")
                 /\ IF Holds(w) THEN queued' = TRUE /\ holder' = "none"
                                ELSE UNCHANGED <<queued, holder>>
                 /\ UNCHANGED <<log, up, named, todo, cleared>>
  /\ UNCHANGED <<started, stopReq, starts, att, why, crashes, lapses, late>>

\* The terminal event (refused by the store once the log is terminal).
Append(w) ==
  /\ pc[w] = "appending"
  /\ log' = "ended" /\ pc' = Set(pc, w, "idle")
  /\ UNCHANGED <<started, stopReq, starts, queued, holder, up, named, att, why, todo,
                 fails, crashes, lapses, cleared, late>>

\* A worker dies anywhere past its claim; its lease runs out later.
Crash(w) ==
  /\ crashes < MaxCrashes /\ pc[w] \notin {"idle", "dead"}
  /\ pc' = Set(pc, w, "dead") /\ crashes' = crashes + 1
  /\ UNCHANGED <<log, started, stopReq, starts, queued, holder, up, named, att, why, todo,
                 fails, lapses, cleared, late>>

\* A lease runs out and the reaper puts the run back: a live worker's (it
\* stalled, bounded by MaxLapses), or a dead one's (always, in time).
Lapse ==
  /\ log = "open" /\ holder # "none" /\ lapses < MaxLapses
  /\ holder' = "none" /\ queued' = TRUE /\ lapses' = lapses + 1
  /\ UNCHANGED <<log, started, stopReq, starts, up, named, pc, att, why, todo,
                 fails, crashes, cleared, late>>
LapseDead ==
  /\ log = "open" /\ holder # "none" /\ pc[holder] = "dead"
  /\ holder' = "none" /\ queued' = TRUE
  /\ UNCHANGED <<log, started, stopReq, starts, up, named, pc, att, why, todo,
                 fails, crashes, lapses, cleared, late>>

\* The owner's stop: recorded; a turn no worker started is cancelled (83A).
Stop ==
  /\ ~stopReq /\ stopReq' = TRUE
  /\ log' = IF log = "open" /\ (~started \/ Design = "stopall") THEN "ended" ELSE log
  /\ UNCHANGED <<started, starts, queued, holder, up, named, pc, att, why, todo,
                 fails, crashes, lapses, cleared, late>>

\* The reaper, for an ended turn: remove one sandbox the host lists ("set":
\* one the set names).
Sweep ==
  /\ Design # "nosweep" /\ log = "ended"
  /\ \E n \in (IF Design = "set" THEN named ELSE up) :
       \/ up' = up \ {n} /\ named' = named \ {n} /\ UNCHANGED fails
       \/ fails < MaxFails /\ fails' = fails + 1 /\ UNCHANGED <<up, named>>
  /\ UNCHANGED <<log, started, stopReq, starts, queued, holder, pc, att, why, todo,
                 crashes, lapses, cleared, late>>

Done ==
  /\ log = "ended" /\ ~queued /\ ~ENABLED Sweep
  /\ \A w \in Workers : pc[w] \in {"idle", "dead"}
  /\ UNCHANGED vars

Step(w) == Claim(w) \/ Begin(w) \/ Record(w) \/ Provision(w) \/ Fence(w)
           \/ Call(w) \/ Drop(w) \/ Clean(w) \/ Append(w)

Next == \E w \in Workers : Step(w) \/ Crash(w)
        \/ Lapse \/ LapseDead \/ Stop \/ Sweep \/ Done

\* Workers and the reaper keep running; a crash, a stall and a stop may not come.
Spec == Init /\ [][Next]_vars /\ \A w \in Workers : WF_vars(Step(w))
          /\ WF_vars(LapseDead) /\ WF_vars(Sweep)

\* While the turn is open, the worker holding its lease has its sandbox up
\* through the gateway call: no other worker removed it.
LiveSandbox ==
  log = "open" => \A w \in Workers :
    (pc[w] = "calling" /\ Holds(w)) => N(att[w]) \in up

\* A turn ends only with no sandbox up but those of workers still under way
\* (each removes its own), those provisioned after a cleanup confirmed their
\* name gone, and, when a worker ends it, those of attempts after its own: a
\* worker whose lease ran out between its last check and its append (84A).
\* The reaper removes what those workers leave (EventuallyClean).
Owned == {N(att[w]) : w \in {v \in Workers : pc[v] \in Active \cup {"cleaning"}}}
Excused(e) == Owned' \cup late' \cup {N(a) : a \in {b \in Attempts : b > e}}
EndClean ==
  [][(log = "open" /\ log' = "ended") =>
       /\ stopReq' # stopReq => up' \subseteq Excused(Cap)
       /\ \A w \in Workers : pc[w] = "appending" /\ pc'[w] = "idle" => up' \subseteq Excused(att[w])]_vars

\* The turn ends, and then no sandbox is left.
TurnEnds == <>(log = "ended")
EventuallyClean == <>[](log = "ended" /\ up = {})
====
