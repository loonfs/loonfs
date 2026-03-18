# Runbook: add a scenario fixture

## Goal

Add a test case that is readable outside the implementation.

## Steps

1. Copy a nearby fixture from `tests/scenarios/`.
2. Set the required top-level metadata:
   - `schema_version: 1`
   - `scenario_kind: client | model | native | sim`
   - `scenario_kind` must match the fixture family directory under `tests/scenarios/`
3. Keep the initial state small.
4. Use stable names for inodes, revisions, and namespaces.
5. Name the invariant directly in `expect.invariants`.
6. Render the fixture with `cargo run -p xtask -- render-case <path> [--snapshot]`.
   You can pass either a real file path or a fixture key like `client/foo.yaml`.
7. Validate one family or the whole fixture tree with
   `cargo run -p xtask -- validate-fixtures [client|model|native|sim]`.
8. Batch-render one family with
   `cargo run -p xtask -- render-kind <client|model|native|sim> [--snapshot]`.
9. For replay fixtures, rerun them with
   `cargo run -p xtask -- replay-seed replay <path> [--seed <u64>] [--snapshot]`.
   You can also pass a fixture key like `native/wal_tail_replay_advances_head.yaml`.
10. If a replay fixture fails, minimize it with
   `cargo run -p xtask -- minimize-case replay <path> [--seed <u64>] [--snapshot] [--write <path>]`.
   Default snapshots land under `tests/snapshots/<command>/<family>/<fixture>.txt`.
11. Save a snapshot output if the harness already supports it.
12. Link the fixture in the PR description.

## Good fixture qualities

- small
- explicit
- names the race or rule being tested
- can be discussed in design review without reading Rust
