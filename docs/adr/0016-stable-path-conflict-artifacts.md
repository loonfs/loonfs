# ADR 0016: stable-path conflict artifacts

Status: accepted

## Decision

LoonDB v1 splits stable-path conflict handling into explicit classes.

File classes:

- `same_inode_stale_base_edit`
- `path_binding_collision`
- `delete_vs_edit`
- `rename_vs_edit`

Subtree classes:

- `subtree_delete_vs_local_changes`
- `subtree_rename_vs_local_changes`

The default v1 policy is `stable_paths`.

Rules:

- the authoritative winner keeps the canonical path
- the loser is preserved as a durable conflict artifact object
- visible suffixed conflict siblings are presentation-only and are not the canonical storage model
- authoritative namespace commit stays on strict CAS for stale-base writes in v1

## Consequences

- `create_conflict_copy` stops being the single conflict defer bucket for supported file and
  subtree classes
- clients can resolve file conflicts without renaming the canonical winner
- clients can resolve supported subtree conflicts without materializing visible sibling trees
- loser content stays recoverable through immutable content plus a deterministic conflict artifact
- `create_conflict_copy` remains only for busy descendants, target-parent-unusable subtree rename,
  and still-unsupported future hierarchy classes
