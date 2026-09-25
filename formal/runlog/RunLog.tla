---- MODULE RunLog ----
\* One stored run and the writers that race on its event log: the Jev driver in
\* create_run, subscription or gateway completers, a late user message, and a
\* redelivered put_run. Design "old" is the store before this change (replace_run,
\* rollback on a failed sandbox destroy, put_run that overwrites). Design "new" is
\* insert-once put_run, append-only writes that the store refuses after a terminal
\* event, and destroy before the completion is appended.
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Design, Completers

Jev == "jev"
Late == "late"
Msg == "msg"
Terminal == {Jev} \cup Completers
Ids == {Msg, Late} \cup Terminal

VARIABLES log, sandbox, jevPc, cPc, snap, latePc, putPc, acked

vars == <<log, sandbox, jevPc, cPc, snap, latePc, putPc, acked>>

Range(s) == {s[i] : i \in DOMAIN s}
HasTerminal(s) == Range(s) \cap Terminal # {}
IsPrefix(s, t) == Len(s) <= Len(t) /\ SubSeq(t, 1, Len(s)) = s

\* The store's append. "old" always appends. "new" refuses once the log is terminal
\* and reports whether it appended, in one step (one lock, one SQL statement).
Appends(id) == Design = "old" \/ ~HasTerminal(log)

TypeOK ==
  /\ log \in Seq(Ids)
  /\ sandbox \in {"up", "gone"}
  /\ jevPc \in {"running", "ok", "refused"}
  /\ cPc \in [Completers -> {"idle", "checked", "destroyed", "appended", "ok", "refused", "failed"}]
  /\ latePc \in {"idle", "ok", "refused"}
  /\ putPc \in {"idle", "done"}
  /\ acked \subseteq Ids

\* put_run stored the user message, and the Box sandbox is up.
Init ==
  /\ log = <<Msg>>
  /\ sandbox = "up"
  /\ jevPc = "running"
  /\ cPc = [c \in Completers |-> "idle"]
  /\ snap = [c \in Completers |-> <<>>]
  /\ latePc = "idle"
  /\ putPc = "idle"
  /\ acked = {Msg}

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
  /\ UNCHANGED <<sandbox, cPc, snap, latePc, putPc>>

\* accept_subscription_completion reads the run: not terminal, sandbox still there.
Check(c) ==
  /\ cPc[c] = "idle"
  /\ ~HasTerminal(log)
  /\ sandbox = "up"
  /\ cPc' = [cPc EXCEPT ![c] = "checked"]
  /\ snap' = [snap EXCEPT ![c] = log]
  /\ UNCHANGED <<log, sandbox, jevPc, latePc, putPc, acked>>

\* old: append the completion first.
OldAppend(c) ==
  /\ Design = "old"
  /\ cPc[c] = "checked"
  /\ log' = Append(log, c)
  /\ cPc' = [cPc EXCEPT ![c] = "appended"]
  /\ UNCHANGED <<sandbox, jevPc, snap, latePc, putPc, acked>>

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
  /\ UNCHANGED <<jevPc, snap, latePc, putPc>>

\* new: destroy first. A failed destroy leaves the log untouched.
NewDestroy(c) ==
  /\ Design = "new"
  /\ cPc[c] = "checked"
  /\ \/ /\ sandbox' = "gone"
        /\ cPc' = [cPc EXCEPT ![c] = "destroyed"]
     \/ /\ cPc' = [cPc EXCEPT ![c] = "failed"]
        /\ UNCHANGED sandbox
  /\ UNCHANGED <<log, jevPc, snap, latePc, putPc, acked>>

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
  /\ UNCHANGED <<sandbox, jevPc, snap, latePc, putPc>>

\* A user message stored by another writer while the turn runs.
LateMessage ==
  /\ latePc = "idle"
  /\ IF Appends(Late)
       THEN /\ log' = Append(log, Late)
            /\ latePc' = "ok"
            /\ acked' = acked \cup {Late}
       ELSE /\ latePc' = "refused"
            /\ UNCHANGED <<log, acked>>
  /\ UNCHANGED <<sandbox, jevPc, cPc, snap, putPc>>

\* put_run delivered again for the same run. old (in memory) overwrites; new inserts once.
Reput ==
  /\ putPc = "idle"
  /\ putPc' = "done"
  /\ IF Design = "old" THEN log' = <<Msg>> ELSE UNCHANGED log
  /\ UNCHANGED <<sandbox, jevPc, cPc, snap, latePc, acked>>

Done ==
  /\ jevPc # "running"
  /\ \A c \in Completers : cPc[c] \in {"idle", "ok", "refused", "failed"}
  /\ latePc # "idle"
  /\ putPc = "done"
  /\ UNCHANGED vars

Next ==
  \/ JevReturns
  \/ \E c \in Completers :
       Check(c) \/ OldAppend(c) \/ OldDestroy(c) \/ NewDestroy(c) \/ NewAppend(c)
  \/ LateMessage
  \/ Reput
  \/ Done

\* Writers are processes that keep running; weak fairness says each enabled one moves.
Spec == Init /\ [][Next]_vars /\ WF_vars(JevReturns) /\ WF_vars(LateMessage) /\ WF_vars(Reput)
          /\ \A c \in Completers : WF_vars(OldAppend(c) \/ OldDestroy(c) \/ NewDestroy(c) \/ NewAppend(c))

\* A write the caller was told succeeded is in the log.
AckedDurable == \A id \in acked : id \in Range(log)

\* One logical completion per run.
AtMostOneTerminal == Cardinality(Range(log) \cap Terminal) <= 1 /\
  \A i, j \in DOMAIN log : (i # j /\ log[i] \in Terminal) => log[j] # log[i]

\* Nothing is recorded after the terminal event.
NothingAfterTerminal == \A i \in DOMAIN log : log[i] \in Terminal => i = Len(log)

\* A completed Box turn has no sandbox.
CompletedHasNoSandbox == (Range(log) \cap Completers # {}) => sandbox = "gone"

\* The log only grows, so a terminal run stays terminal.
LogGrows == [][IsPrefix(log, log')]_log

\* Every started writer finishes.
WritersFinish == <>[](jevPc # "running" /\ latePc # "idle" /\ putPc = "done"
                     /\ \A c \in Completers : cPc[c] \in {"idle", "ok", "refused", "failed"})
====
