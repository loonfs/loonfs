# loon-ops

Shared operability command, config, and rendering contract for LoonDB.

This crate implements the core operations (import, observe, sync) consumed by both `loon-cli` and
`xtask`. It is the integration point that composes `loon-client`, `loon-server`, and
`loon-objectstore` into user-facing workflows.

`#![forbid(unsafe_code)]`

## Public API

- **Config types** — `OpsConfig`, `OpsObjectStoreSpec`, and related TOML-driven configuration
- **Import** — `import_authoritative_remote_observations` for bootstrapping client state
- **Observe** — `observe_local_path`, `observe_delete_path`, `observe_move_path`,
  `observe_subtree_path` for feeding filesystem events into the sync pipeline
- **Sync** — `sync_once` and `sync_until_idle` for running the upload/download loop
