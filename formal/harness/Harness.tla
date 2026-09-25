---- MODULE Harness ----
EXTENDS Integers, FiniteSets, TLC

CONSTANTS MaxTurns, MaxSteps, MaxWorkers, MaxRetries, MaxTools

Turns == 1..MaxTurns
Steps == 1..MaxSteps
Workers == 1..MaxWorkers
Tools == 1..MaxTools
Attempts == 0..MaxRetries

VARIABLES session, dispatch, turnPhase, turnStep, owner, attempt, pending, answered, live, issued

vars == <<session, dispatch, turnPhase, turnStep, owner, attempt, pending, answered, live, issued>>

Terminal == {"completed", "failed", "cancelled"}
Active == {"running", "waiting"}
Phases == {"idle", "running", "waiting", "completed", "failed", "cancelled"}

DispatchPhases == {
  "created", "queued", "scheduled", "provisioning", "starting",
  "running", "waiting", "awaiting_approval", "paused", "recovering",
  "completed", "failed", "cancelled", "expired"
}
DispatchTerminal == {"completed", "failed", "cancelled", "expired"}
DispatchOpen == DispatchPhases \ DispatchTerminal
DispatchLive == {"running", "waiting", "awaiting_approval", "paused", "recovering"}

harnessVars == <<session, turnPhase, turnStep, owner, attempt, pending, answered, live, issued>>

Init ==
  /\ session = "idle"
  /\ dispatch = "created"
  /\ turnPhase = [t \in Turns |-> "idle"]
  /\ turnStep = [t \in Turns |-> 0]
  /\ owner = [t \in Turns |-> 0]
  /\ attempt = [t \in Turns |-> 0]
  /\ pending = [t \in Turns |-> 0]
  /\ answered = [t \in Turns |-> FALSE]
  /\ live = [w \in Workers |-> TRUE]
  /\ issued = [t \in Turns |-> {}]

StartSession ==
  /\ session = "idle"
  /\ session' = "active"
  /\ UNCHANGED <<dispatch, turnPhase, turnStep, owner, attempt, pending, answered, live, issued>>

StartTurn(t) ==
  /\ session = "active"
  /\ turnPhase[t] = "idle"
  /\ \A u \in Turns : turnPhase[u] \notin Active
  /\ \E w \in Workers :
       /\ live[w]
       /\ owner' = [owner EXCEPT ![t] = w]
       /\ turnPhase' = [turnPhase EXCEPT ![t] = "running"]
       /\ turnStep' = [turnStep EXCEPT ![t] = 1]
       /\ UNCHANGED <<session, dispatch, attempt, pending, answered, live, issued>>

RequestTool(t) ==
  /\ turnPhase[t] = "running"
  /\ answered[t] = FALSE
  /\ pending[t] = 0
  /\ owner[t] \in Workers
  /\ live[owner[t]]
  /\ turnStep[t] \in Steps
  /\ <<turnStep[t], attempt[t]>> \notin issued[t]
  /\ \E tool \in Tools :
       /\ pending' = [pending EXCEPT ![t] = tool]
       /\ turnPhase' = [turnPhase EXCEPT ![t] = "waiting"]
       /\ issued' = [issued EXCEPT ![t] = issued[t] \union {<<turnStep[t], attempt[t]>>}]
       /\ UNCHANGED <<session, dispatch, turnStep, owner, attempt, answered, live>>

ReceiveResult(t) ==
  /\ turnPhase[t] = "waiting"
  /\ pending[t] \in Tools
  /\ answered[t] = FALSE
  /\ turnPhase' = [turnPhase EXCEPT ![t] = "running"]
  /\ pending' = [pending EXCEPT ![t] = 0]
  /\ answered' = [answered EXCEPT ![t] = TRUE]
  /\ UNCHANGED <<session, dispatch, turnStep, owner, attempt, live, issued>>

Advance(t) ==
  /\ turnPhase[t] = "running"
  /\ answered[t] = TRUE
  /\ turnStep[t] < MaxSteps
  /\ turnStep' = [turnStep EXCEPT ![t] = turnStep[t] + 1]
  /\ answered' = [answered EXCEPT ![t] = FALSE]
  /\ attempt' = [attempt EXCEPT ![t] = 0]
  /\ UNCHANGED <<session, dispatch, turnPhase, owner, pending, live, issued>>

Retry(t) ==
  /\ turnPhase[t] = "running"
  /\ answered[t] = TRUE
  /\ attempt[t] < MaxRetries
  /\ owner[t] \in Workers
  /\ live[owner[t]]
  /\ attempt' = [attempt EXCEPT ![t] = attempt[t] + 1]
  /\ answered' = [answered EXCEPT ![t] = FALSE]
  /\ UNCHANGED <<session, dispatch, turnPhase, turnStep, owner, pending, live, issued>>

Complete(t) ==
  /\ turnPhase[t] = "running"
  /\ turnPhase' = [turnPhase EXCEPT ![t] = "completed"]
  /\ owner' = [owner EXCEPT ![t] = 0]
  /\ pending' = [pending EXCEPT ![t] = 0]
  /\ UNCHANGED <<session, dispatch, turnStep, attempt, answered, live, issued>>

Fail(t) ==
  /\ turnPhase[t] \in Active
  /\ turnPhase' = [turnPhase EXCEPT ![t] = "failed"]
  /\ owner' = [owner EXCEPT ![t] = 0]
  /\ pending' = [pending EXCEPT ![t] = 0]
  /\ UNCHANGED <<session, dispatch, turnStep, attempt, answered, live, issued>>

Cancel(t) ==
  /\ turnPhase[t] \in Active
  /\ turnPhase' = [turnPhase EXCEPT ![t] = "cancelled"]
  /\ owner' = [owner EXCEPT ![t] = 0]
  /\ pending' = [pending EXCEPT ![t] = 0]
  /\ UNCHANGED <<session, dispatch, turnStep, attempt, answered, live, issued>>

WorkerCrash(w) ==
  /\ live[w]
  /\ live' = [live EXCEPT ![w] = FALSE]
  /\ owner' = [t \in Turns |-> IF owner[t] = w THEN 0 ELSE owner[t]]
  /\ turnPhase' = [t \in Turns |-> IF owner[t] = w /\ turnPhase[t] \in Active THEN "failed" ELSE turnPhase[t]]
  /\ pending' = [t \in Turns |-> IF owner[t] = w THEN 0 ELSE pending[t]]
  /\ UNCHANGED <<session, dispatch, turnStep, attempt, answered, issued>>

CompleteSession ==
  /\ session = "active"
  /\ \A t \in Turns : turnPhase[t] \notin Active
  /\ \E t \in Turns : turnPhase[t] \in Terminal
  /\ session' = "done"
  /\ UNCHANGED <<dispatch, turnPhase, turnStep, owner, attempt, pending, answered, live, issued>>

FailAbandoned ==
  /\ session = "active"
  /\ \A w \in Workers : live[w] = FALSE
  /\ \A t \in Turns : turnPhase[t] \notin Active
  /\ session' = "done"
  /\ UNCHANGED <<dispatch, turnPhase, turnStep, owner, attempt, pending, answered, live, issued>>

DispatchQueue ==
  /\ dispatch = "created"
  /\ dispatch' = "queued"
  /\ UNCHANGED harnessVars

DispatchSchedule ==
  /\ dispatch = "queued"
  /\ dispatch' = "scheduled"
  /\ UNCHANGED harnessVars

DispatchProvision ==
  /\ dispatch = "scheduled"
  /\ dispatch' = "provisioning"
  /\ UNCHANGED harnessVars

DispatchPrepare ==
  /\ dispatch = "provisioning"
  /\ dispatch' = "starting"
  /\ UNCHANGED harnessVars

DispatchRun ==
  /\ dispatch = "starting"
  /\ dispatch' = "running"
  /\ UNCHANGED harnessVars

DispatchLocalRun ==
  /\ dispatch = "created"
  /\ dispatch' = "running"
  /\ UNCHANGED harnessVars

DispatchWait ==
  /\ dispatch = "running"
  /\ dispatch' = "waiting"
  /\ UNCHANGED harnessVars

DispatchApproval ==
  /\ dispatch = "running"
  /\ dispatch' = "awaiting_approval"
  /\ UNCHANGED harnessVars

DispatchPause ==
  /\ dispatch = "running"
  /\ dispatch' = "paused"
  /\ UNCHANGED harnessVars

DispatchRecover ==
  /\ dispatch = "running"
  /\ dispatch' = "recovering"
  /\ UNCHANGED harnessVars

DispatchResume ==
  /\ dispatch \in {"waiting", "awaiting_approval", "paused", "recovering"}
  /\ dispatch' = "running"
  /\ UNCHANGED harnessVars

DispatchComplete ==
  /\ dispatch \in DispatchLive
  /\ dispatch' = "completed"
  /\ UNCHANGED harnessVars

DispatchFail ==
  /\ dispatch \in DispatchOpen
  /\ dispatch' = "failed"
  /\ UNCHANGED harnessVars

DispatchCancel ==
  /\ dispatch \in DispatchOpen
  /\ dispatch' = "cancelled"
  /\ UNCHANGED harnessVars

DispatchExpire ==
  /\ dispatch \in DispatchOpen
  /\ dispatch' = "expired"
  /\ UNCHANGED harnessVars

Scheduling ==
  \/ DispatchQueue
  \/ DispatchSchedule
  \/ DispatchProvision
  \/ DispatchPrepare
  \/ DispatchWait
  \/ DispatchApproval
  \/ DispatchPause
  \/ DispatchRecover
  \/ DispatchResume

Done ==
  /\ session = "done"
  /\ UNCHANGED vars

IgnoreStale(t) ==
  /\ \/ turnPhase[t] \in Terminal
     \/ answered[t] = TRUE
  /\ UNCHANGED vars

Next ==
  \/ StartSession
  \/ \E t \in Turns : \/ StartTurn(t)
                    \/ RequestTool(t)
                    \/ ReceiveResult(t)
                    \/ Advance(t)
                    \/ Retry(t)
                    \/ Complete(t)
                    \/ Fail(t)
                    \/ Cancel(t)
                    \/ IgnoreStale(t)
  \/ \E w \in Workers : WorkerCrash(w)
  \/ CompleteSession
  \/ FailAbandoned
  \/ DispatchQueue
  \/ DispatchSchedule
  \/ DispatchProvision
  \/ DispatchPrepare
  \/ DispatchRun
  \/ DispatchLocalRun
  \/ DispatchWait
  \/ DispatchApproval
  \/ DispatchPause
  \/ DispatchRecover
  \/ DispatchResume
  \/ DispatchComplete
  \/ DispatchFail
  \/ DispatchCancel
  \/ DispatchExpire
  \/ Done

Spec == Init /\ [][Next]_vars
FairSpec == Spec /\ WF_vars(Next) /\ WF_vars(DispatchComplete)

TypeOK ==
  /\ session \in {"idle", "active", "done"}
  /\ dispatch \in DispatchPhases
  /\ turnPhase \in [Turns -> Phases]
  /\ turnStep \in [Turns -> 0..MaxSteps]
  /\ owner \in [Turns -> 0..MaxWorkers]
  /\ attempt \in [Turns -> Attempts]
  /\ pending \in [Turns -> 0..MaxTools]
  /\ answered \in [Turns -> BOOLEAN]
  /\ live \in [Workers -> BOOLEAN]
  /\ issued \in [Turns -> SUBSET (Steps \X Attempts)]

SingleOwner ==
  \A t \in Turns :
    /\ (turnPhase[t] \in Active) => (owner[t] \in Workers /\ live[owner[t]])
    /\ (turnPhase[t] \notin Active) => owner[t] = 0

WaitingIffPending ==
  \A t \in Turns : (turnPhase[t] = "waiting") <=> (pending[t] \in Tools)

NoActiveWhenDone ==
  session = "done" => \A t \in Turns : turnPhase[t] \notin Active

AttemptBound ==
  \A t \in Turns : attempt[t] <= MaxRetries

PhaseRank(t) ==
  LET p == turnPhase[t]
      base == (MaxSteps - turnStep[t]) * 1000 + (MaxRetries - attempt[t]) * 40
  IN IF p \in Terminal THEN 0
     ELSE IF p = "idle" THEN 8000
     ELSE IF p = "running" /\ answered[t] = FALSE THEN base + 30
     ELSE IF p = "waiting" THEN base + 20
     ELSE IF p = "running" /\ answered[t] = TRUE THEN base + 10
     ELSE 0

RECURSIVE SumPhase(_)
SumPhase(S) ==
  IF S = {} THEN 0
  ELSE LET t == CHOOSE x \in S : \A y \in S : x <= y
       IN PhaseRank(t) + SumPhase(S \ {t})

LiveCount == Cardinality({w \in Workers : live[w]})

SessionRank ==
  IF session = "idle" THEN 5
  ELSE IF session = "active" THEN 3
  ELSE 0

DispatchRank ==
  IF dispatch = "created" THEN 140
  ELSE IF dispatch = "queued" THEN 130
  ELSE IF dispatch = "scheduled" THEN 120
  ELSE IF dispatch = "provisioning" THEN 110
  ELSE IF dispatch = "starting" THEN 100
  ELSE IF dispatch = "running" THEN 90
  ELSE IF dispatch = "waiting" THEN 80
  ELSE IF dispatch = "awaiting_approval" THEN 70
  ELSE IF dispatch = "paused" THEN 60
  ELSE IF dispatch = "recovering" THEN 50
  ELSE 0

Rank == SessionRank + DispatchRank + LiveCount + SumPhase(Turns)

RankDecreases == [][DispatchResume \/ Rank' < Rank]_vars

TerminalStuck ==
  [][\A t \in Turns : turnPhase[t] \in Terminal => turnPhase'[t] = turnPhase[t]]_vars

EventuallyDone == <>(session = "done")

DispatchTerminalStuck ==
  [][(dispatch \in DispatchTerminal) => UNCHANGED <<dispatch>>]_vars

SchedulingPreserves ==
  [][Scheduling => UNCHANGED harnessVars]_vars
====
