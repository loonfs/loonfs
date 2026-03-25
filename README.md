# LoonDB

LoonDB is a Dropbox/GDrive-like sync engine with an object-storage-only durable backend.
For now, this repository is a **development bootstrap**, not a finished product. It is structured so we can start implementing in small, testable steps without losing the design intent we already established.

## What this repo is trying to optimize for

- correctness before feature count
- deterministic behavior before raw throughput
- small changes with high reviewability
- object-storage portability earned through conformance tests
- a test suite that is understandable by both engineers and product-minded reviewers

## Intended product shape

- Rust server
- macOS client
- local-server mode and remote-server mode
- object storage as the only durable system of record
- inode-keyed metadata
- multiple namespaces per account
- deterministic background work built on rebuildable state

## Current implemented surfaces

- `loon-core`, `loon-model`, and `loon-objectstore` hold the active protocol and storage logic
- `loon-server::mutation` is the current authoritative server-side execution surface
- `loon-client` contains the active SQLite/planner/executor work
- `loon-ops` owns the shared operability command/config/rendering contract
- `xtask ops ...` and `loon ops ...` are the active local operability frontends
- `loon-cli` also owns built-in help, manpage generation, config inspection, and `doctor`
- `xtask rc-local` remains the canonical repo release-candidate path
- `loon-testkit` remains the active review and debugging toolkit
- the `loond` binary shell and `loon-macos` are still reserved later-phase delivery surfaces

## Start here

1. Read `AGENTS.md`.
2. Read `docs/specs/000-overview.md`.
3. Read `docs/specs/060-testing-strategy.md`.
4. Read `docs/specs/090-major-implementation-decisions.md`.
5. Read `docs/adr/` in numerical order.
6. Read `docs/roadmap/020-semantic-core-reset.md` for the current execution order.
7. Read `docs/roadmap/000-bootstrap.md` and `docs/roadmap/010-foundation-workstreams.md` only for historical context.

## Repository map

```text
docs/
  adr/              locked architectural decisions
  context/          source context and original prompt
  references/       public research links and notes
  roadmap/          implementation phases
  runbooks/         operational/debugging checklists
  specs/            readable design specs

spec/tla/           protocol-level model-checking seeds

crates/
  loon-types/       shared IDs and wire/domain types
  loon-objectstore/ object-store trait, provider profiles, conformance surface
  loon-core/        canonical metadata rules and commit planning
  loon-model/       pure reference model for state-machine testing
  loon-queue/       durable background-work coordination
  loon-testkit/     scenario fixtures, rendering, helpers
  loon-sim/         deterministic simulator scaffolding
  loon-server/      authoritative mutation surface; binary/http shell quarantined for now
  loon-cli/         active thin CLI frontend over loon-ops
  loon-client/      active client state, planner, and executor implementation
  loon-macos/       reserved macOS integration placeholder for later File Provider work

tests/
  scenarios/        readable test cases
  conformance/      storage-provider contract tests
  snapshots/        expected rendered outputs

xtask/              repository automation entrypoints
```

## Recommended first implementation order

1. `loon-objectstore`: local adapter + conformance harness
2. `loon-core`: head/lease/commit rules
3. `loon-model`: pure namespace model + state-machine tests
4. `loon-queue`: one sharded queue class, probably `BuildSnapshot`
5. `loon-sim`: deterministic clock, scheduler, mock object store
6. `loon-server`: authoritative mutation surface before widening binary or HTTP shells
7. `loon-client`: durable local truth, planner, and executor paths
8. `loon-macos`: File Provider bridge after mirror semantics are stable

## Commands to wire up first

```bash
# Format all Rust code in the workspace (rustfmt)
cargo fmt --all

# Lint every crate and target; all warnings are errors
cargo clippy --workspace --all-targets -- -D warnings

# Run the built-in test runner across all crates
cargo test --workspace

# Run the canonical local release-candidate path
cargo run -p xtask -- rc-local --config ./loondb-demo.toml --namespace demo

# Run the active thin CLI frontend over the same loon-ops contract
cargo run -p loon-cli -- ops smoke --config ./loondb-demo.toml --namespace demo

# Discover the active CLI surface and validate a local config
cargo run -p loon-cli -- help ops bootstrap-namespace
cargo run -p loon-cli -- config validate --config ./loondb-demo.toml
cargo run -p loon-cli -- doctor --config ./loondb-demo.toml

# Run the same tests with nextest (optional, if installed)
cargo nextest run

# Render a YAML scenario fixture into human-readable form
cargo run -p xtask -- render-case tests/scenarios/model/delete_then_stale_local_edit.yaml
```

## Definition of “done” for an early feature

A feature is not done when the code compiles. A feature is done when all of the following are true:

- the relevant spec section exists or was updated
- an ADR exists if the decision is architectural
- the reference model has the behavior
- at least one readable scenario fixture covers the behavior
- invariants are named explicitly
- the implementation and tests agree on the same vocabulary

## CLI manual

The current operator-facing CLI manual is:

- [docs/runbooks/loon-cli.md](/Users/conormccarter/Code/loondb/docs/runbooks/loon-cli.md)
