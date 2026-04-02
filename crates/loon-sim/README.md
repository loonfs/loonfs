# loon-sim

Deterministic simulator runtime for fault injection, actor stepping, delivery ordering,
restart events, and replay traces.

This crate provides a controlled execution environment for testing LoonDB's concurrent and
failure-sensitive paths with reproducible seeds. Every test run with the same seed produces
identical behavior, making failures debuggable and regressions catchable.

`#![forbid(unsafe_code)]`

## Key exports

- `SimRuntime` — deterministic scheduler with controlled clock and delivery ordering
- `SimTraceEvent` — structured trace events for replay and debugging
- `SimActorId` — typed actor identifiers for multi-party simulation
- `SimDelivery` — delivery-ordering control for message and I/O simulation

## Modules

| Module | Purpose |
|--------|---------|
| `runtime` | Core deterministic scheduler and clock |
| `faults` | Fault injection definitions (crashes, delays, reordering) |
| `trace` | Structured trace recording and replay |
