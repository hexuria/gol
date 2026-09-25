---- MODULE Dispatch ----
EXTENDS Naturals

VARIABLE dispatch

vars == <<dispatch>>

DispatchPhases == {
  "created", "queued", "scheduled", "provisioning", "starting",
  "running", "waiting", "awaiting_approval", "paused", "recovering",
  "completed", "failed", "cancelled", "expired"
}
DispatchTerminal == {"completed", "failed", "cancelled", "expired"}
DispatchOpen == DispatchPhases \ DispatchTerminal
DispatchLive == {"running", "waiting", "awaiting_approval", "paused", "recovering"}

Init == dispatch = "created"

DispatchQueue ==
  /\ dispatch = "created"
  /\ dispatch' = "queued"

DispatchSchedule ==
  /\ dispatch = "queued"
  /\ dispatch' = "scheduled"

DispatchProvision ==
  /\ dispatch = "scheduled"
  /\ dispatch' = "provisioning"

DispatchPrepare ==
  /\ dispatch = "provisioning"
  /\ dispatch' = "starting"

DispatchRun ==
  /\ dispatch = "starting"
  /\ dispatch' = "running"

DispatchLocalRun ==
  /\ dispatch = "created"
  /\ dispatch' = "running"

DispatchWait ==
  /\ dispatch = "running"
  /\ dispatch' = "waiting"

DispatchApproval ==
  /\ dispatch = "running"
  /\ dispatch' = "awaiting_approval"

DispatchPause ==
  /\ dispatch = "running"
  /\ dispatch' = "paused"

DispatchRecover ==
  /\ dispatch = "running"
  /\ dispatch' = "recovering"

DispatchResume ==
  /\ dispatch \in {"waiting", "awaiting_approval", "paused", "recovering"}
  /\ dispatch' = "running"

DispatchComplete ==
  /\ dispatch \in DispatchLive
  /\ dispatch' = "completed"

DispatchFail ==
  /\ dispatch \in DispatchOpen
  /\ dispatch' = "failed"

DispatchCancel ==
  /\ dispatch \in DispatchOpen
  /\ dispatch' = "cancelled"

DispatchExpire ==
  /\ dispatch \in DispatchOpen
  /\ dispatch' = "expired"

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

DispatchDone ==
  /\ dispatch \in DispatchTerminal
  /\ UNCHANGED vars

Step ==
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

Next == Step \/ DispatchDone

Spec == Init /\ [][Next]_vars
FairSpec == Spec /\ WF_vars(DispatchComplete)

TypeOK == dispatch \in DispatchPhases

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

RankDecreases == [][DispatchResume \/ DispatchRank' < DispatchRank]_vars

DispatchTerminalStuck ==
  [][(dispatch \in DispatchTerminal) => UNCHANGED <<dispatch>>]_vars

Settles == <>[][UNCHANGED vars]_vars
====
