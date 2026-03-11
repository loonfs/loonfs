# ADR 0007: testability-first architecture

Status: accepted

We will shape the product architecture so deterministic model tests and simulator tests are straightforward to build.

Consequences:
- avoid hidden threads in core logic
- inject time, randomness, and I/O boundaries
- prefer strict protocols that reduce invalid state space
