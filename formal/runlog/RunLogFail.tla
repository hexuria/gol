---- MODULE RunLogFail ----
\* RunLog with a failer (fail_turn) racing a completer. A module of its own so
\* scripts/verify-tla.sh pairs RunLogFail.cfg with it; RunLog.cfg keeps
\* Failers = {} and its recorded state count.
EXTENDS RunLog
====
