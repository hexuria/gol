---- MODULE RunLogWorkers ----
\* RunLog with two queue workers storing one run step by step (Phase 1.5b),
\* racing each other and a late user message. A module of its own so
\* scripts/verify-tla.sh pairs RunLogWorkers.cfg with it.
EXTENDS RunLog
====
