# loon-cli

Active thin CLI frontend over `crates/loon-ops`.

Current scope:

- `loon ops ...` reuses the shared `loon-ops` subcommand grammar and output semantics unchanged
- `loon completion ...`, `loon manpages ...`, and `loon version` are CLI-only affordances
- `xtask ops ...` remains supported in parallel for repo/dev workflows
- `xtask rc-local` remains repo automation and does not move into `loon-cli`

Deliberate non-goals in the current slice:

- no profiles or token auth
- no separate `~/.config/loon/config.toml` model
- no endpoint or HTTP transport abstraction
- no public `namespace`, `ls`, `cat`, `put`, `get`, or `doctor` families yet

The local filesystem semantics underneath the `ops` commands continue to live in `crates/loon-client`:

- path-based `observe-local` / `observe-delete` / `observe-move` / `observe-subtree` routing
- generic filesystem event normalization for future watcher adapters

Broader CLI work should continue to reuse those existing layers rather than re-implementing them in
`loon-cli`.
