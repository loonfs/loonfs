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
- `ops-observe-delete/`
- `ops-observe-move/`
- `ops-observe-subtree/`
- `ops-sync-once/`
- `ops-sync-until-idle/`
- `ops-smoke/`
- `rc-local/`
- `sim-interleavings/`
- `sim-explore/`

`sim-explore/` contains deterministic exploration summaries and minimized plain sim repros across
the scheduler-backed harnesses.

The intention is that reviewers can diff scenario output without reading Rust code.

`ops-observe-subtree/` also covers the strict inferred file-move path: unique digest-equal file
pairs may render as inferred rename work, while ambiguous candidates remain explicit failure
cases.
