# Filesystem and storage model

This document describes the logical filesystem model that LoonFS stores durably.

## Namespace model

A namespace is the unit of serialized metadata history. Each namespace has:

- one root inode
- one visible commit order, numbered by `seq`
- its own WAL, checkpoints, progress objects, and retention floor

The namespace root is ordinary canonical metadata. It is seeded as inode `1` at `seq = 0`.

## Identity, paths, and names

The canonical identity of an item is `(namespace_id, inode_id)`.

Paths are projections built from visible directory bindings. This gives LoonFS two important properties:

1. renames do not change identity
2. mutation requests do not have to guess which path name was current at the moment of commit

Name collisions are governed by a versioned `NamePolicy`. The first policy is `macos_ci_v1`:

- preserve `display_name` exactly for presentation
- derive `name_key` using Unicode NFC normalization and case folding
- reject sibling names whose `name_key` collides

## Supported inode kinds

| Kind | Meaning |
| --- | --- |
| `FILE` | A regular file whose content is described by file revisions and content manifests. |
| `DIR` | A directory that contains child bindings. |
| `SYMLINK` | A symbolic link. |
| `MOUNT` | An entry point into another namespace or another namespace subtree. |

## Logical metadata families

LoonFS reconstructs visible state from a small set of logical record families:

| Family | What it means |
| --- | --- |
| **Inodes** | Which identities exist, what kind each inode is, and when it first appeared. |
| **Direntries** | Which child inode is bound under which parent directory and name. |
| **Revisions** | Which immutable content manifest each file revision points to. |
| **Subtree tombstones** | Which directory roots have been recursively deleted. |

A path is obtained by walking visible direntries from a starting inode. A file’s content head is obtained by reading the highest visible revision for that inode.

## Mounts and cross-namespace access

A `MOUNT` inode points to:

- `target_namespace_id`
- `target_root_inode_id`

Most mounts target another namespace root. Some mounts target a subtree inside another namespace. Mount traversal must detect and reject mount loops.

Cross-namespace moves are not atomic in the core model. They are modeled as copy plus delete.

## File content model

A file revision points to one immutable content manifest. That manifest names the file’s size, whole-file digest, block size, and ordered list of content blocks.

Rules:

- a revision is immutable once committed
- a new visible file state always means a new revision number
- restore does not rewrite history; it creates a new revision that reuses older content
- metadata may reference content only after the referenced blocks and manifest are durable

## Replay model

Authoritative state is reconstructed like this:

```text
head.json
   -> latest verified checkpoint at or before head.seq
   -> WAL entries after that checkpoint
   -> visible logical state
```

If no checkpoint exists, replay starts from the namespace bootstrap state and uses the full WAL.

The durable replay rule is one of the core design choices in LoonFS:

- history is append-only
- checkpoints are immutable
- compaction means publishing a newer checkpoint and later advancing retention
- compaction does **not** mean rewriting history into a mutable log
