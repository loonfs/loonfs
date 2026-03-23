# loon-cli

Reserved future operator/debugging CLI surface.

This crate remains intentionally quarantined.

The current active operator frontend is `xtask ops ...`, and the shared command/config/rendering
contract now lives in `crates/loon-ops`.

When `loon-cli` is activated, it should reuse that `loon-ops` layer with the same subcommand
grammar and output semantics instead of re-implementing config loading or shell logic.
