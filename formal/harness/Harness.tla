---- MODULE Harness ----
EXTENDS Integers

CONSTANTS MaxTurns, MaxSteps, MaxWorkers, MaxRetries, MaxTools

VARIABLES session, dispatch, turnPhase, turnStep, owner, attempt, pending, answered, live, issued

vars == <<session, dispatch, turnPhase, turnStep, owner, attempt, pending, answered, live, issued>>

harnessVars == <<session, turnPhase, turnStep, owner, attempt, pending, answered, live, issued>>

H == INSTANCE HarnessCore
D == INSTANCE Dispatch

Init == H!Init /\ D!Init

DispatchComplete == D!DispatchComplete /\ UNCHANGED harnessVars

Next ==
  \/ H!Next /\ UNCHANGED dispatch
  \/ D!Step /\ UNCHANGED harnessVars

Spec == Init /\ [][Next]_vars
FairSpec == Spec /\ WF_vars(Next) /\ WF_vars(DispatchComplete)

TypeOK == H!TypeOK /\ D!TypeOK
SingleOwner == H!SingleOwner
WaitingIffPending == H!WaitingIffPending
NoActiveWhenDone == H!NoActiveWhenDone
AttemptBound == H!AttemptBound

Rank == H!Rank + D!DispatchRank

RankDecreases == [][D!DispatchResume \/ Rank' < Rank]_vars

TerminalStuck == H!TerminalStuck

EventuallyDone == <>(session = "done")

DispatchTerminalStuck == D!DispatchTerminalStuck

SchedulingPreserves ==
  [][D!Scheduling => UNCHANGED harnessVars]_vars
====
