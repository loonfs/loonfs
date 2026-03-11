# Runbook: add a scenario fixture

## Goal

Add a test case that is readable outside the implementation.

## Steps

1. Copy a nearby fixture from `tests/scenarios/`.
2. Keep the initial state small.
3. Use stable names for inodes, revisions, and namespaces.
4. Name the invariant directly in `expect.invariants`.
5. Render the fixture with `cargo run -p xtask -- render-case <path>`.
6. Save a snapshot output if the harness already supports it.
7. Link the fixture in the PR description.

## Good fixture qualities

- small
- explicit
- names the race or rule being tested
- can be discussed in design review without reading Rust
