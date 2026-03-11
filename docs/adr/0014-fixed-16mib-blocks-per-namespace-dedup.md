# ADR 0014: content uses fixed 16 MiB blocks, SHA-256, and per-namespace dedup in v1

Status: accepted

The first content system will use fixed 16 MiB blocks, canonical SHA-256 digests of plaintext block bytes, and dedup only within a namespace.

Consequences:
- large transfers are parallelizable and easy to reason about
- GC and retention stay namespace-local
- content-defined chunking and cross-namespace dedup are deferred
