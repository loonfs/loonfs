# Snapshots

Rendered scenario traces and summaries live here, grouped by the xtask command that produced them:

- `render-case/`
- `replay-seed/`
- `minimize-case/`
- `conflict-list/`
- `conflict-show/`
- `conflict-restore/`
- `conflict-archive/`
- `conflict-unarchive/`
- `ops-bootstrap-namespace/`
- `ops-show-namespace-state/`
- `ops-show-client-state/`
- `ops-import-remote-observations/`
- `ops-observe-local/`
- `ops-sync-once/`
- `ops-smoke/`
- `sim-interleavings/`
- `sim-explore/`

`sim-explore/` contains deterministic exploration summaries and minimized plain sim repros across
the scheduler-backed harnesses.

The intention is that reviewers can diff scenario output without reading Rust code.
