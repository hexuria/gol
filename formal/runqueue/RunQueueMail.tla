---- MODULE RunQueueMail ----
\* RunQueue with Mail: a run that asks another agent parks until its reply
\* (Phase 2.3). A module of its own so scripts/verify-tla.sh pairs
\* RunQueueMail.cfg with it.
EXTENDS RunQueue
====
