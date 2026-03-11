------------------------------ MODULE retention_gc ------------------------------
EXTENDS Naturals, Sequences

\* Sketch only.
\* Intended variables:
\* - retentionFloorSeq
\* - snapshotSeq
\* - gcPlans
\* Safety goal: no live object is deleted by an eligible-but-stale plan.

================================================================================
