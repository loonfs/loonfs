# xtask

Build automation entrypoints for the LoonDB workspace.

This crate is not intended for external use. It provides repository-level automation commands
following the [cargo-xtask](https://github.com/matklad/cargo-xtask) convention.

## Commands

- `rc-local` — canonical local release-candidate path (fmt, clippy, tests, conformance, smoke)
- `render-case` — render a YAML scenario fixture into human-readable form
- `replay-seed` — replay a deterministic simulation seed
- `ops` — run operability commands (parallel to `loon-cli ops`)
