# LoonDB

[![CI](https://github.com/prequel-co/loonfs/actions/workflows/ci.yml/badge.svg)](https://github.com/prequel-co/loonfs/actions/workflows/ci.yml)

LoonDB is a file sync engine whose only durable backend is object storage (S3, R2, local
filesystem). There is no traditional database server — all authoritative state lives as immutable
objects and a small set of mutable control objects in the object store. The project is under active
development and not yet production-ready.

## Design principles

- Correctness before feature count
- Deterministic behavior before raw throughput
- Small changes with high reviewability
- Object-storage portability earned through conformance tests
- A test suite understandable by both engineers and product-minded reviewers

## Product shape

- Rust server and macOS client
- Local-server mode and remote-server mode
- Object storage as the only durable system of record
- Inode-keyed metadata (paths are derived views, never canonical identity)
- Multiple namespaces per account
- Deterministic background work built on rebuildable state

## Crate map

```text
crates/
  loon-types/       shared IDs and wire/domain types
  loon-objectstore/ object-store trait, provider profiles, conformance surface
  loon-core/        canonical metadata rules and commit planning
  loon-model/       pure reference model for state-machine testing
  loon-queue/       durable background-work coordination
  loon-client/      client-side sync: SQLite state, planner, executor
  loon-server/      authoritative mutation surface (binary/HTTP shell quarantined)
  loon-ops/         shared operability command/config/rendering contract
  loon-cli/         thin CLI frontend over loon-ops
  loon-testkit/     scenario fixtures, rendering, helpers
  loon-sim/         deterministic simulator scaffolding
  loon-macos/       macOS File Provider bridge (experimental spike)

tests/
  scenarios/        readable test cases (YAML fixtures)
  conformance/      storage-provider contract tests
  snapshots/        expected rendered outputs

xtask/              repository automation entrypoints
```

## Getting started

```bash
# Build all crates
cargo build --workspace

# Run the full test suite
cargo test --workspace

# Lint
cargo clippy --workspace --all-targets -- -D warnings

# Explore the CLI
cargo run -p loon-cli -- help

# Validate a local config
cargo run -p loon-cli -- config validate --config ./loondb-demo.toml
cargo run -p loon-cli -- doctor --config ./loondb-demo.toml

# Run the canonical local release-candidate path
cargo run -p xtask -- rc-local --config ./loondb-demo.toml --namespace demo

# Render a YAML scenario fixture into human-readable form
cargo run -p xtask -- render-case tests/scenarios/model/delete_then_stale_local_edit.yaml
```

## Configuration

Example configuration templates are in [`configs/`](configs/):

- [`loondb-demo.local-fs.example.toml`](configs/loondb-demo.local-fs.example.toml) — local filesystem backend
- [`loondb-demo.aws-s3.example.toml`](configs/loondb-demo.aws-s3.example.toml) — AWS S3 backend
- [`loondb-demo.cloudflare-r2.example.toml`](configs/loondb-demo.cloudflare-r2.example.toml) — Cloudflare R2 backend

Copy one of these to `./loondb-demo.toml` and edit to match your setup.

## Architecture

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for a comprehensive system overview covering the data
model, mutation flow, object-store layer, commit protocol, client architecture, and testing
strategy.

## Documentation

| Path | Content |
|------|---------|
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | System overview and orientation |
| [`AGENTS.md`](AGENTS.md) | Non-negotiable rules and development workflow |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Branch/PR guidance, formatting, documentation style |
| [`docs/specs/`](docs/specs/) | Readable design specifications |
| [`docs/adr/`](docs/adr/) | Architectural decision records |
| [`docs/roadmap/`](docs/roadmap/) | Implementation phases |
| [`docs/runbooks/`](docs/runbooks/) | Operational and debugging guides |

### Recommended reading order

1. [`AGENTS.md`](AGENTS.md)
2. [`docs/specs/000-overview.md`](docs/specs/000-overview.md)
3. [`docs/specs/060-testing-strategy.md`](docs/specs/060-testing-strategy.md)
4. [`docs/specs/090-major-implementation-decisions.md`](docs/specs/090-major-implementation-decisions.md)
5. [`docs/adr/`](docs/adr/) in numerical order

## CLI manual

The operator-facing CLI manual is at [`docs/runbooks/loon-cli.md`](docs/runbooks/loon-cli.md).
