------------------------------ MODULE queue_claim ------------------------------
EXTENDS Naturals, Sequences

\* Sketch only.
\* Intended variables:
\* - jobs
\* - brokerEpoch
\* - currentClaimToken
\* Safety goal: stale claim tokens cannot complete or heartbeat a stolen job.

================================================================================
