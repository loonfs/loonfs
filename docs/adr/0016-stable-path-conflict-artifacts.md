# ADR 0016: stable-path conflict artifacts

Status: accepted

## Decision

LoonDB v1 splits file conflict handling into explicit classes:

- `same_inode_stale_base_edit`
- `path_binding_collision`
- `delete_vs_edit`
- `rename_vs_edit`

The default v1 policy is `stable_paths`.

Rules:

- the authoritative winner keeps the canonical path
- the loser is preserved as a durable conflict artifact object
- visible suffixed conflict siblings are presentation-only and are not the canonical storage model
- authoritative namespace commit stays on strict CAS for stale-base writes in v1

## Consequences

- `create_conflict_copy` stops being the single file-level defer bucket
- clients can resolve file conflicts without renaming the canonical winner
- loser content stays recoverable through immutable content plus a deterministic conflict artifact
- directory and subtree conflict-artifact execution can remain deferred without blocking the file
  taxonomy cleanup
