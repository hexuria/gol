---- MODULE RunQueueCrash ----
\* RunQueue with a producer that may die between its pend and its push, and
\* the sweep that recovers its run. A module of its own so
\* scripts/verify-tla.sh pairs RunQueueCrash.cfg with it; RunQueue.cfg keeps
\* MaxProducerDeaths = 0 and its recorded state count.
EXTENDS RunQueue
====
