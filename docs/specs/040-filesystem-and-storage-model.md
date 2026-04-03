# Filesystem and Storage Model

## 1. Namespaces and identity

A namespace is the unit of visible metadata history.

Each namespace has:

- a current head
- an ordered WAL of commits
- zero or more checkpoints
- a retention policy

The head also carries the next monotonic inode id for that namespace. New inode ids are allocated from the head as part of commit publication.

The canonical identity of an item is `(namespace_id, inode_id)`.

Two consequences follow:

1. rename does not change identity;
2. path is a view, not the identity model.

In v1, the root inode is created as `inode_id = 1` at `seq = 0`.

## 2. Inode kinds

The core inode kinds are:

| Kind | Meaning |
| --- | --- |
| **DIR** | A directory that can own child bindings. |
| **FILE** | A file whose history is an ordered set of revisions. |
| **MOUNT** | A presentation point for another namespace or subtree. |

The spec does not require a larger type taxonomy in the core model. New resource types should normally be represented through file content or resource properties rather than by introducing new inode kinds.

## 3. Directories, names, and paths

Directories do not "contain bytes." They contain bindings from a name to a child inode.

A path is produced by walking visible directory bindings from the root inode. A path can change even when the underlying item has not.

### 3.1 NamePolicy

Sibling-name comparison is governed by a versioned `NamePolicy`. A namespace has exactly one active name policy.

The v1 policy is `macos_ci_v1`, which defines a macOS-friendly, case-insensitive collision rule. Future policies may exist, but all writers for a namespace must agree on the namespace's active policy.

## 4. Files and revisions

A file is represented by one inode and a sequence of immutable revisions.

Each revision points to exactly one immutable content manifest. The manifest, in turn, describes the ordered list of immutable content blocks that reconstruct the file bytes.

Blocks and manifests belong to the owning namespace. A file revision may reference only content that is durable under that namespace's content store.

This gives LoonFS a two-stage write model:

```text
make content durable  ->  then make metadata visible
```

That separation is one of the core design decisions of the system.

## 5. Tombstones and deletion

Deletion is logical first.

When an item is deleted, LoonFS records tombstone metadata that hides the file or subtree from visible lookups. The delete becomes visible as part of normal namespace history.

Physical reclamation is separate background work. It may happen only when retention and reference-safety rules allow it.

## 6. Mounts

A mount presents another namespace, or a subtree of another namespace, inside the current tree.

A mount carries:

- a target namespace id
- a target root inode id within that namespace

This allows a composed visible tree without inventing one global namespace history underneath.

Two important rules apply:

1. path resolution may cross a mount;
2. mount loops are invalid and must be rejected.

A share grants access to a subtree. A mount presents that accessible subtree at a path. The two concepts are related, but they are not the same.

## 7. Cross-namespace moves

Identity is namespace-local. A true inode-preserving rename is therefore namespace-local as well.

Across namespaces, a move is modeled as a copy plus a delete from the source namespace. Content may still be reused internally, but inode identity does not cross the namespace boundary.
