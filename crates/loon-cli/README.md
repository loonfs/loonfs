# loon-cli

Reserved future operator/debugging CLI surface.

This crate remains intentionally quarantined.

The current active operator frontend is `xtask ops ...`, and the shared command/config/rendering
contract now lives in `crates/loon-ops`.

When `loon-cli` is activated, it should reuse that `loon-ops` layer with the same subcommand
grammar and output semantics instead of re-implementing config loading or shell logic. That
includes `import-remote-observations`, `observe-local`, `observe-delete`, `observe-move`,
`observe-subtree`, `sync-once`, and `sync-until-idle`, which should move over unchanged rather
than being re-specified in `loon-cli`. In particular, subtree move pairing semantics should come
from `loon-ops` unchanged: unique digest-equal file pairs and unique exact-subtree directory pairs
may infer a move, while non-exact directory refactors still require explicit `observe-move`.
