---- MODULE RunLog ----
\* One stored run and the writers that race on its event log: the Jev driver in
\* create_run, subscription or gateway completers, failers (fail_turn), a late
\* user message, and a redelivered put_run. Design "old" is the store before this change (replace_run,
\* rollback on a failed sandbox destroy, put_run that overwrites). Design "new" is
\* insert-once put_run, append-only writes that the store refuses after a terminal
\* event, and destroy before the completion is appended.
\*
\* Workers (Phase 1.5b) are queue workers that store a run step by step: each
\* appends with append_events_after, refused as Moved unless the log is as long
\* as the worker last saw, reloads on Moved (at most MaxReloads times) and then
\* hands the run back. Design "blind" is the same workers with an append that
\* does not check the length: the negative control for StepsContiguous.
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Design, Completers, Failers, Workers, Steps, MaxReloads

Jev == "jev"
Late == "late"
Msg == "msg"
\* A worker's appends, in order: the last one ends the run.
StepId(k) == CASE k = 1 -> "s1" [] k = 2 -> "s2" [] k = 3 -> "s3"
StepIds == IF Workers = {} THEN {} ELSE {StepId(k) : k \in 1..Steps}
LastStep == IF Workers = {} THEN {} ELSE {StepId(Steps)}
Terminal == {Jev} \cup Completers \cup Failers \cup LastStep
Ids == {Msg, Late} \cup Terminal \cup StepIds

VARIABLES log, sandbox, jevPc, cPc, snap, latePc, putPc, acked, fPc, wPc, wSeen, wNext, wReloads

vars == <<log, sandbox, jevPc, cPc, snap, latePc, putPc, acked, fPc, wPc, wSeen, wNext, wReloads>>
workerVars == <<wPc, wSeen, wNext, wReloads>>

Range(s) == {s[i] : i \in DOMAIN s}
HasTerminal(s) == Range(s) \cap Terminal # {}
IsPrefix(s, t) == Len(s) <= Len(t) /\ SubSeq(t, 1, Len(s)) = s

StepsIn(s) == Len(SelectSeq(s, LAMBDA x : x \in StepIds))

\* The store's append. "old" always appends. "new" refuses once the log is terminal
\* and reports whether it appended, in one step (one lock: a transaction holding the run row).
Appends(id) == Design = "old" \/ ~HasTerminal(log)

TypeOK ==
  /\ log \in Seq(Ids)
  /\ sandbox \in {"up", "gone"}
  /\ jevPc \in {"running", "ok", "refused", "absent"}
  /\ cPc \in [Completers -> {"idle", "checked", "destroyed", "appended", "ok", "refused", "failed"}]
  /\ latePc \in {"idle", "ok", "refused"}
  /\ putPc \in {"idle", "done"}
  /\ acked \subseteq Ids
  /\ fPc \in [Failers -> {"idle", "checked", "destroyed", "ok", "refused", "failed"}]
  /\ wPc \in [Workers -> {"idle", "loaded", "done", "gaveup"}]
  /\ wSeen \in [Workers -> Nat]
  /\ wNext \in [Workers -> 1..(Steps + 1)]
  /\ wReloads \in [Workers -> 0..MaxReloads]

\* put_run stored the user message, and the Box sandbox is up.
Init ==
  /\ log = <<Msg>>
  /\ sandbox = "up"
  \* create_run's Jev writer and queue workers never share a run.
  /\ jevPc = IF Workers = {} THEN "running" ELSE "absent"
  /\ cPc = [c \in Completers |-> "idle"]
  /\ snap = [c \in Completers |-> <<>>]
  /\ latePc = "idle"
  /\ putPc = "idle"
  /\ acked = {Msg}
  /\ fPc = [f \in Failers |-> "idle"]
  /\ wPc = [w \in Workers |-> "idle"]
  /\ wSeen = [w \in Workers |-> 0]
  /\ wNext = [w \in Workers |-> 1]
  /\ wReloads = [w \in Workers |-> 0]

\* run_with_jev returns. old: replace_run(<<Msg>> \o jev). new: append_events(jev).
JevReturns ==
  /\ jevPc = "running"
  /\ IF Design = "old"
       THEN /\ log' = <<Msg, Jev>>
            /\ jevPc' = "ok"
            /\ acked' = acked \cup {Jev}
       ELSE IF Appends(Jev)
         THEN /\ log' = Append(log, Jev)
              /\ jevPc' = "ok"
              /\ acked' = acked \cup {Jev}
         ELSE /\ UNCHANGED <<log, acked>>
              /\ jevPc' = "refused"
  /\ UNCHANGED <<sandbox, cPc, snap, latePc, putPc, fPc>>
  /\ UNCHANGED workerVars

\* accept_subscription_completion reads the run: not terminal, sandbox still there.
Check(c) ==
  /\ cPc[c] = "idle"
  /\ ~HasTerminal(log)
  /\ sandbox = "up"
  /\ cPc' = [cPc EXCEPT ![c] = "checked"]
  /\ snap' = [snap EXCEPT ![c] = log]
  /\ UNCHANGED <<log, sandbox, jevPc, latePc, putPc, acked, fPc>>
  /\ UNCHANGED workerVars

\* old: append the completion first.
OldAppend(c) ==
  /\ Design = "old"
  /\ cPc[c] = "checked"
  /\ log' = Append(log, c)
  /\ cPc' = [cPc EXCEPT ![c] = "appended"]
  /\ UNCHANGED <<sandbox, jevPc, snap, latePc, putPc, acked, fPc>>
  /\ UNCHANGED workerVars

\* old: destroy the sandbox. On failure, replace_run(prior snapshot).
OldDestroy(c) ==
  /\ Design = "old"
  /\ cPc[c] = "appended"
  /\ \/ /\ sandbox' = "gone"
        /\ cPc' = [cPc EXCEPT ![c] = "ok"]
        /\ acked' = acked \cup {c}
        /\ UNCHANGED log
     \/ /\ log' = snap[c]
        /\ cPc' = [cPc EXCEPT ![c] = "failed"]
        /\ UNCHANGED <<sandbox, acked>>
  /\ UNCHANGED <<jevPc, snap, latePc, putPc, fPc>>
  /\ UNCHANGED workerVars

\* new: destroy first. A failed destroy leaves the log untouched.
NewDestroy(c) ==
  /\ Design = "new"
  /\ cPc[c] = "checked"
  /\ \/ /\ sandbox' = "gone"
        /\ cPc' = [cPc EXCEPT ![c] = "destroyed"]
     \/ /\ cPc' = [cPc EXCEPT ![c] = "failed"]
        /\ UNCHANGED sandbox
  /\ UNCHANGED <<log, jevPc, snap, latePc, putPc, acked, fPc>>
  /\ UNCHANGED workerVars

\* new: append the completion. A refusal answers the caller with Conflict.
NewAppend(c) ==
  /\ Design = "new"
  /\ cPc[c] = "destroyed"
  /\ IF Appends(c)
       THEN /\ log' = Append(log, c)
            /\ cPc' = [cPc EXCEPT ![c] = "ok"]
            /\ acked' = acked \cup {c}
       ELSE /\ cPc' = [cPc EXCEPT ![c] = "refused"]
            /\ UNCHANGED <<log, acked>>
  /\ UNCHANGED <<sandbox, jevPc, snap, latePc, putPc, fPc>>
  /\ UNCHANGED workerVars

\* A user message stored by another writer while the turn runs.
LateMessage ==
  /\ latePc = "idle"
  /\ IF Appends(Late)
       THEN /\ log' = Append(log, Late)
            /\ latePc' = "ok"
            /\ acked' = acked \cup {Late}
       ELSE /\ latePc' = "refused"
            /\ UNCHANGED <<log, acked>>
  /\ UNCHANGED <<sandbox, jevPc, cPc, snap, putPc, fPc>>
  /\ UNCHANGED workerVars

\* put_run delivered again for the same run. old (in memory) overwrites; new inserts once.
Reput ==
  /\ putPc = "idle"
  /\ putPc' = "done"
  /\ IF Design = "old" THEN log' = <<Msg>> ELSE UNCHANGED log
  /\ UNCHANGED <<sandbox, jevPc, cPc, snap, latePc, acked, fPc>>
  /\ UNCHANGED workerVars

\* fail_turn reads the run: not terminal. Unlike a completion it does not need
\* the sandbox; a turn whose sandbox is already gone can still be failed.
FailCheck(f) ==
  /\ Design = "new"
  /\ fPc[f] = "idle"
  /\ ~HasTerminal(log)
  /\ fPc' = [fPc EXCEPT ![f] = "checked"]
  /\ UNCHANGED <<log, sandbox, jevPc, cPc, snap, latePc, putPc, acked>>
  /\ UNCHANGED workerVars

\* Destroy first, as a completer does; a sandbox already gone needs nothing. A
\* failed destroy leaves the log untouched and the turn open. It can fail even
\* with the sandbox gone: fail_turn asks whether it is absent, then destroys,
\* and a completer can remove the sandbox in between. A host that cannot say
\* whether it is absent also takes the failed branch.
FailDestroy(f) ==
  /\ fPc[f] = "checked"
  /\ \/ /\ sandbox' = "gone"
        /\ fPc' = [fPc EXCEPT ![f] = "destroyed"]
     \/ /\ fPc' = [fPc EXCEPT ![f] = "failed"]
        /\ UNCHANGED sandbox
  /\ UNCHANGED <<log, jevPc, cPc, snap, latePc, putPc, acked>>
  /\ UNCHANGED workerVars

\* Append RunFailed. A refusal answers the caller with Conflict.
FailAppend(f) ==
  /\ fPc[f] = "destroyed"
  /\ IF Appends(f)
       THEN /\ log' = Append(log, f)
            /\ fPc' = [fPc EXCEPT ![f] = "ok"]
            /\ acked' = acked \cup {f}
       ELSE /\ fPc' = [fPc EXCEPT ![f] = "refused"]
            /\ UNCHANGED <<log, acked>>
  /\ UNCHANGED <<sandbox, jevPc, cPc, snap, latePc, putPc>>
  /\ UNCHANGED workerVars

\* A worker loads the run (Claim::prepare's store.run): nothing to do once it
\* ended; otherwise it resumes after the steps the log holds.
WLoad(w) ==
  /\ wPc[w] = "idle"
  /\ IF HasTerminal(log)
       THEN /\ wPc' = [wPc EXCEPT ![w] = "done"]
            /\ UNCHANGED <<wSeen, wNext>>
       ELSE /\ wPc' = [wPc EXCEPT ![w] = "loaded"]
            /\ wSeen' = [wSeen EXCEPT ![w] = Len(log)]
            /\ wNext' = [wNext EXCEPT ![w] = StepsIn(log) + 1]
  /\ UNCHANGED <<log, sandbox, jevPc, cPc, snap, latePc, putPc, acked, fPc, wReloads>>

\* One conditional append of the worker's next step (append_events_after at a
\* step boundary, or the tail). Refused once the log is terminal; refused as
\* Moved when the log is not as long as the worker saw ("blind" does not look),
\* after which the worker reloads, or gives the run back to the queue.
WAppend(w) ==
  /\ wPc[w] = "loaded"
  /\ IF HasTerminal(log)
       THEN /\ wPc' = [wPc EXCEPT ![w] = "done"]
            /\ UNCHANGED <<log, wSeen, wNext, wReloads>>
       ELSE IF Design = "blind" \/ Len(log) = wSeen[w]
         THEN /\ log' = Append(log, StepId(wNext[w]))
              /\ wSeen' = [wSeen EXCEPT ![w] = Len(log) + 1]
              /\ wNext' = [wNext EXCEPT ![w] = @ + 1]
              /\ wPc' = [wPc EXCEPT ![w] = IF wNext[w] = Steps THEN "done" ELSE "loaded"]
              /\ UNCHANGED wReloads
         ELSE IF wReloads[w] < MaxReloads
           THEN /\ wSeen' = [wSeen EXCEPT ![w] = Len(log)]
                /\ wNext' = [wNext EXCEPT ![w] = StepsIn(log) + 1]
                /\ wReloads' = [wReloads EXCEPT ![w] = @ + 1]
                /\ UNCHANGED <<log, wPc>>
           ELSE /\ wPc' = [wPc EXCEPT ![w] = "gaveup"]
                /\ UNCHANGED <<log, wSeen, wNext, wReloads>>
  /\ UNCHANGED <<sandbox, jevPc, cPc, snap, latePc, putPc, acked, fPc>>

Done ==
  /\ jevPc # "running"
  /\ \A c \in Completers : cPc[c] \in {"idle", "ok", "refused", "failed"}
  /\ \A f \in Failers : fPc[f] \in {"idle", "ok", "refused", "failed"}
  /\ latePc # "idle"
  /\ putPc = "done"
  /\ \A w \in Workers : wPc[w] \in {"done", "gaveup"}
  /\ UNCHANGED vars

Next ==
  \/ JevReturns
  \/ \E c \in Completers :
       Check(c) \/ OldAppend(c) \/ OldDestroy(c) \/ NewDestroy(c) \/ NewAppend(c)
  \/ \E f \in Failers : FailCheck(f) \/ FailDestroy(f) \/ FailAppend(f)
  \/ LateMessage
  \/ Reput
  \/ \E w \in Workers : WLoad(w) \/ WAppend(w)
  \/ Done

\* Writers are processes that keep running; weak fairness says each enabled one moves.
Spec == Init /\ [][Next]_vars /\ WF_vars(JevReturns) /\ WF_vars(LateMessage) /\ WF_vars(Reput)
          /\ \A c \in Completers : WF_vars(OldAppend(c) \/ OldDestroy(c) \/ NewDestroy(c) \/ NewAppend(c))
          /\ \A f \in Failers : WF_vars(FailDestroy(f) \/ FailAppend(f))
          /\ \A w \in Workers : WF_vars(WLoad(w) \/ WAppend(w))

\* A write the caller was told succeeded is in the log.
AckedDurable == \A id \in acked : id \in Range(log)

\* One logical completion per run.
AtMostOneTerminal == Cardinality(Range(log) \cap Terminal) <= 1 /\
  \A i, j \in DOMAIN log : (i # j /\ log[i] \in Terminal) => log[j] # log[i]

\* Nothing is recorded after the terminal event.
NothingAfterTerminal == \A i \in DOMAIN log : log[i] \in Terminal => i = Len(log)

\* A Box turn that a completer or a failer ended has no sandbox.
CompletedHasNoSandbox == (Range(log) \cap (Completers \cup Failers) # {}) => sandbox = "gone"

\* The workers' steps land once each and in order: no step is appended twice,
\* and none is appended by a worker that had not seen the one before it.
StepsContiguous ==
  LET steps == SelectSeq(log, LAMBDA x : x \in StepIds)
  IN \A i \in 1..Len(steps) : steps[i] = StepId(i)

\* The log only grows, so a terminal run stays terminal.
LogGrows == [][IsPrefix(log, log')]_log

\* Every started writer finishes.
WritersFinish == <>[](jevPc # "running" /\ latePc # "idle" /\ putPc = "done"
                     /\ \A c \in Completers : cPc[c] \in {"idle", "ok", "refused", "failed"}
                     /\ \A f \in Failers : fPc[f] \in {"idle", "ok", "refused", "failed"}
                     /\ \A w \in Workers : wPc[w] \in {"done", "gaveup"})
====
