# ADR 0015: core planners are serialized state machines and the client uses SQLite as local truth

Status: accepted

Core namespace planning, queue transitions, and client sync planning will stay in serialized state-machine code. The client will use SQLite as its only durable local truth.

Consequences:
- deterministic simulation and replay stay tractable
- transfer concurrency is isolated from correctness logic
- mirror mode and later File Provider mode share one local truth model
