------------------------------ MODULE namespace_head ------------------------------
EXTENDS Naturals, Sequences

\* Sketch only.
\* Intended variables:
\* - headSeq
\* - fenceToken
\* - walCommits
\* - writers
\* Safety goal: a stale writer cannot advance the head.

================================================================================
