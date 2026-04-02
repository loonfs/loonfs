# loon-cli

Active thin CLI frontend over `crates/loon-ops`.

## Quick start

```bash
# See available commands
cargo run -p loon-cli -- help

# Validate a local configuration file
cargo run -p loon-cli -- config validate --config ./loondb-demo.toml

# Run diagnostics
cargo run -p loon-cli -- doctor --config ./loondb-demo.toml

# Run operability commands (delegates to loon-ops)
cargo run -p loon-cli -- ops --help
```

## Current scope

- `loon ops ...` reuses the shared `loon-ops` subcommand grammar and output semantics unchanged
- `loon config ...` and `loon doctor` are CLI-only discovery/diagnostic affordances over the same
  existing ops TOML config shape
- `loon completion ...`, `loon manpages ...`, and `loon version` are CLI-only affordances
- `xtask ops ...` remains supported in parallel for repo/dev workflows
- `xtask rc-local` remains repo automation and does not move into `loon-cli`

## Deliberate non-goals in the current slice

- no profiles or token auth
- no separate `~/.config/loon/config.toml` model
- no endpoint or HTTP transport abstraction
- no public `namespace`, `ls`, `cat`, `put`, or `get` families yet

## Local filesystem semantics

The local filesystem semantics underneath the `ops` commands continue to live in `crates/loon-client`:

- path-based `observe-local` / `observe-delete` / `observe-move` / `observe-subtree` routing
- generic filesystem event normalization for future watcher adapters

Broader CLI work should continue to reuse those existing layers rather than re-implementing them in
`loon-cli`.

## Manual

The checked-in manual for the active surface is at
[docs/runbooks/loon-cli.md](../../docs/runbooks/loon-cli.md).
