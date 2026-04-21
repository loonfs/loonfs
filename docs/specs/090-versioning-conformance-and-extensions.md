# Versioning, Conformance, and Extensions

## 1. Versioning

A stable spec needs explicit versioning in three places.

| Layer | What is versioned |
| --- | --- |
| **Storage format** | Durable object envelopes and payload rules. |
| **Protocol binding** | HTTP or other transport shapes. |
| **Namespace naming rules** | `NamePolicy` and any future policy revisions. |

A new version should be introduced only when an old implementation could misread or misapply a new feature.

## 2. Server requirements

A conforming server must:

1. treat object storage as the authoritative durable foundation;
2. publish visible metadata only through logical commits stored in visible WAL segments plus a successful head update;
3. validate that referenced content is already durable before publish;
4. preserve `(namespace_id, inode_id)` as canonical identity;
5. implement tombstone-first delete;
6. serve replay from verified checkpoints plus the visible WAL segment chain, replayed as logical commits;
7. honor the namespace's `NamePolicy`;
8. keep control-plane sessions and any implementation-specific coordinators out of namespace history and the change feed; and
9. preserve per-request idempotency, ordering, and change-feed identity even when physically batching logical commits in a WAL segment.

## 3. Writer and client requirements

A conforming writer or client must:

1. treat paths as selectors, not as durable identity;
2. upload or otherwise stage content before asking the server to publish it;
3. use request ids or equivalent idempotency keys for safe retry;
4. tolerate commit rejection when preconditions no longer hold; and
5. re-bootstrap if its cursor falls behind the retention floor.

A sync client must also maintain durable local state for its cursor and reconciliation logic.

## 4. Optional commit metadata

A commit may carry optional human or product metadata such as:

- a commit message;
- annotations or tags attached to the commit envelope;
- actor information; or
- workflow-correlation fields such as `operation_id`, `operation_kind`, or `operation_part`.

This metadata belongs to the logical commit, not to the resource itself.

## 5. Optional resource properties

A resource may carry optional structured properties such as display hints, application tags, or a resource-type hint.

These properties belong to the resource, not to the commit. They should move with the inode when the path changes.

## 6. Timestamps

The semantic creation marker in the core model is the create commit in namespace history, not a wall-clock field.

An implementation may expose wall-clock timestamps such as `committed_at` or `created_at`, but these are optional and non-semantic.

## 7. Hooks and downstream processing

The preferred extension point is the committed change feed. Downstream systems such as indexers, notification services, preview builders, or policy engines should consume committed changes rather than becoming part of the core mutation path.
