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
- artifact discovery and restore are library-first and out of band
- restore always targets an explicit caller-supplied destination and never mutates sync planner
  state
- archive state is binary only in v1: implicit `active` by sidecar absence or `archived` by sidecar
  presence
- archive state lives in immutable sidecar objects under
  `namespaces/{namespace_id}/conflict-archives/{conflict_id}.json`
- the first operator-facing shell is `xtask` against explicit local DB/store paths with
  list/show/restore/archive/unarchive
- destructive delete/GC lifecycle controls remain deferred

## Consequences

- `create_conflict_copy` stops being the single conflict defer bucket for supported file and
  subtree classes
- clients can resolve file conflicts without renaming the canonical winner
- clients can resolve supported subtree conflicts without materializing visible sibling trees
- loser content stays recoverable through immutable content plus a deterministic conflict artifact
- conflict artifacts can be rediscovered from object storage and restored later without rebinding
  them into canonical sync state
- operators have a narrow `xtask` shell for list/show/restore before any server-admin or public
  CLI surface exists
- `create_conflict_copy` remains only for busy descendants, target-parent-unusable subtree rename,
  and still-unsupported future hierarchy classes
