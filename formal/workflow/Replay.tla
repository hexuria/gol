---- MODULE Replay ----
\* this spec is one counter journal; the kernel line is pull request 7 at 1291bc64c78e71b8f5a3f4745184e1472decc679, shown by cargo test -p runtime-tokio --test replay_proof; this spec has no variable for that line.

VARIABLES journal, held, command, path, run, wait

vars == <<journal, held, command, path, run, wait>>

\* The journal variable is the arm. It has no id field.
\* The row operators take that arm and do not mention held.

CommandOf(j) ==
  CASE j = "empty" -> "execute"
    [] j = "zero" -> "complete"
    [] j = "nonzero" -> "fail"

PathOf(j) ==
  CASE j = "empty" -> "unrecorded"
    [] j = "zero" -> "zero"
    [] j = "nonzero" -> "nonzero"

RunOf(j) ==
  CASE j = "empty" -> "open"
    [] j = "zero" -> "completed"
    [] j = "nonzero" -> "failed"

Init ==
  /\ held = "none"
  /\ journal \in {"empty", "zero", "nonzero"}
  /\ command = CommandOf(journal)
  /\ path = PathOf(journal)
  /\ run = RunOf(journal)
  /\ wait = "none"

\* Stand-in. It does not append.
Perform ==
  /\ journal = "empty"
  /\ held = "none"
  /\ command = "execute"
  /\ held' = "zero"
  /\ UNCHANGED <<journal, command, path, run, wait>>

\* The only action that changes journal. It writes only from empty to zero.
Commit ==
  /\ journal = "empty"
  /\ held = "zero"
  /\ journal' = "zero"
  /\ command' = CommandOf(journal')
  /\ path' = PathOf(journal')
  /\ run' = RunOf(journal')
  /\ UNCHANGED <<held, wait>>

\* One process death. The journal value is what differs.
Crash ==
  /\ held = "zero"
  /\ held' = "none"
  /\ UNCHANGED <<journal, command, path, run, wait>>

\* Stutter on a hit with nothing held.
Stop ==
  /\ journal \in {"zero", "nonzero"}
  /\ held = "none"
  /\ UNCHANGED vars

Next == Perform \/ Commit \/ Crash \/ Stop

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ journal \in {"empty", "zero", "nonzero"}
  /\ held \in {"none", "zero"}
  /\ command \in {"execute", "complete", "fail"}
  /\ path \in {"unrecorded", "zero", "nonzero"}
  /\ run \in {"open", "completed", "failed"}
  /\ wait = "none"

\* command, path, run, and wait are the row of journal.
\* The derived id is <<"counter-branch", path, 0>>, computed from the arm.
\* Some(0) completes. Any other Some fails. The three ids differ.
JournaledResultForcesBranch ==
  /\ command = CommandOf(journal)
  /\ path = PathOf(journal)
  /\ run = RunOf(journal)
  /\ wait = "none"
  /\ <<"counter-branch", path, 0>> = <<"counter-branch", PathOf(journal), 0>>
  /\ <<"counter-branch", "unrecorded", 0>> /= <<"counter-branch", "zero", 0>>
  /\ <<"counter-branch", "zero", 0>> /= <<"counter-branch", "nonzero", 0>>
  /\ <<"counter-branch", "unrecorded", 0>> /= <<"counter-branch", "nonzero", 0>>

\* The return held in memory is not the branch.
HeldIsNotAHit ==
  (journal = "empty" /\ held = "zero") =>
    (command = "execute" /\ path = "unrecorded" /\ run = "open")

\* Every step that sets command' = execute has journal = empty.
JournaledIdNotReexecuted ==
  [][(command' = "execute") => (journal = "empty")]_vars

\* A step whose journal is already not empty leaves journal unchanged.
HitSticks ==
  [][(journal /= "empty") => UNCHANGED journal]_vars

====
