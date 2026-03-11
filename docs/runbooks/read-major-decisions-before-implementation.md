# Runbook: read the locked decisions before implementing a large feature

Use this checklist before starting work in `loon-objectstore`, `loon-core`, `loon-queue`, or `loon-client`.

1. Read `docs/specs/090-major-implementation-decisions.md`.
2. Read ADRs 0009 through 0015.
3. Identify which decision your feature depends on.
4. If your feature appears to contradict one of those decisions, stop and write an ADR update before coding.
5. Add a readable scenario fixture before implementing the production path.

This runbook exists to keep large correctness decisions from being silently re-opened in code review.
