# loon-testkit

Readable scenario fixture types, rendering helpers, and test infrastructure for LoonDB.

Scenario fixtures are YAML files under `tests/scenarios/` that describe expected protocol behavior
in human-readable form. They are treated as product artifacts — not just test internals — and are
reviewed alongside specs and ADRs.

`#![forbid(unsafe_code)]`

## Key modules

| Module | Purpose |
|--------|---------|
| `fixtures` | YAML fixture parsing and typed scenario definitions |
| `scenario` | Scenario execution against the reference model |
| `render` | Human-readable rendering of scenario outcomes |
| `replay` | Deterministic replay of recorded traces |
| `seed` | Reproducible seed generation for randomized testing |
| `minimize` | Failure case minimization for debugging |
| `invariants` | Cross-scenario invariant checking |
| `explore` | Exploratory state-space traversal |
| `snapshots` | Expected-output snapshot management |
| `client` | Client-specific test helpers |
| `tempdir` | Temporary directory management for tests |
