# ADR 0001: object storage is the only durable system of record

Status: accepted

We will design LoonDB so durable truth lives only in object storage.

Consequences:
- no external database is required for correctness
- control-plane objects must stay small and CAS-friendly
- higher write latency is acceptable for remote durability
- client-local durability remains important for UX and recovery
