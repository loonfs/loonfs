# ADR 0012: one user commit request maps to one visible namespace seq in v1

Status: accepted

In v1, unrelated user requests do not share a visible namespace commit. A single request may contain multiple operations, but it publishes as one seq if it succeeds.

Consequences:
- request idempotency remains simple
- commit traces stay human-readable
- the authoritative write path is not turned into a batching queue prematurely
