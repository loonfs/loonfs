# Filesystem and Storage Model

## 1. Namespaces and identity

A namespace is the unit of visible metadata history.

The `namespace_id` is a durable storage identity, not a reusable display name. It must not be reused after namespace destruction. Future aliases or user-facing names may be reused only if they map to a new `namespace_id`.

Each namespace has:

- a descriptor that records its namespace id and content-store id
- a current head
- an ordered WAL of logical commits stored in immutable segments
- zero or more checkpoints
- a retention policy

The head also carries the next monotonic inode id for that namespace. New inode ids are allocated from the head as part of commit publication.

The canonical identity of an item is `(namespace_id, inode_id)`.

Each namespace has exactly one immutable `content_store_id`. The content store is an immutable pool for file bytes and may be referenced by many namespaces. A new root namespace receives a new content store; a forked namespace reuses the source namespace's content store while starting an independent namespace metadata history.

Two consequences follow:

1. rename does not change identity;
2. path is a view, not the identity model.

If an item is deleted and a new item is later created at the same path, that new item receives a new inode identity.

An inode is the durable namespace-local identity record for one filesystem item.

An inode records:

- what item this is;
- what kind of item it is; and
- when it first entered namespace history.

An inode does not record:

- the item's current path;
- the parent directory that currently contains it; or
- the file bytes it currently references.

Those facts live in other metadata families:

- direntries say where an inode is currently bound in the tree;
- revisions say which immutable file version is current for a file inode; and
- paths are derived views produced by walking visible directory bindings from the root.

### 1.1 Example metadata shapes

The inode itself is only one part of the metadata model. A complete visible file usually involves multiple logical records.

Illustrative inode row:

```json
{
  "inode_id": 42,
  "inode_kind": "FILE",
  "created_seq": 17
}
```

Illustrative direntry row that binds that inode into the tree:

```json
{
  "parent_inode_id": 9,
  "name_key": "report.txt",
  "display_name": "Report.txt",
  "child_inode_id": 42,
  "bind_seq": 17
}
```

Illustrative revision row for the current file contents:

```json
{
  "inode_id": 42,
  "revision_no": 7,
  "committed_seq": 91,
  "content_ref": {
    "kind": "whole_file_v0",
    "digest": "sha256:42d...",
    "size_bytes": 19482
  }
}
```

Together, those three records mean:

- inode `42` is the durable identity of the file;
- the file is currently visible under parent directory inode `9` as `Report.txt`; and
- the current visible file bytes come from revision `7`.

If the file is renamed, the direntry changes but the inode stays `42`. If the file contents are replaced, the revision row changes but the inode stays `42`.

In v0, the root inode is created as `inode_id = 1` at `seq = 0`.

## 2. Inode kinds

The core inode kinds are:

| Kind | Meaning |
| --- | --- |
| **DIR** | A directory that can own child bindings. |
| **FILE** | A file whose history is an ordered set of revisions. |
| **MOUNT** | A presentation point for another namespace or subtree. |

The spec does not require a larger type taxonomy in the core model. New resource types should normally be represented through file content or resource properties rather than by introducing new inode kinds.

## 3. Directories, names, and paths

Directories contain bindings from a name to a child inode. They do not contain file bytes.

A path is produced by walking visible directory bindings from the root inode. A path can change even when the underlying item has not.

### 3.1 NamePolicy

Sibling-name comparison is governed by a versioned `NamePolicy`. A namespace has exactly one active name policy.

The v0 policy is `nfc_casefold_v0`, which defines sibling-name comparison by Unicode NFC normalization plus case folding. Future policies may exist, but all writers for a namespace must agree on the namespace's active policy.

## 4. Files and revisions

A file is represented by one inode and a sequence of immutable revisions.

Each revision stores exactly one immutable `content_ref`. In v0, that reference names one whole-file object containing the complete plaintext file bytes. Revisions do not store object-store paths or `content_store_id`; readers resolve those through the namespace descriptor when bytes are needed.

Content objects belong to the namespace's content store. A file revision may reference only content that is durable under the content store named by that namespace descriptor.

LoonFS therefore uses a two-stage write model:

```text
make content durable  ->  then make metadata visible
```

This separation is part of the core model.

### 4.1 Immutable content storage

The stable immutable content families are:

```text
content-stores/{content_store_id}/blobs/sha256/{hex[0..2]}/{hex[2..4]}/{hex}
```

The core rules are:

- `content_ref.kind` is `whole_file_v0` for the v0 content strategy;
- `content_ref.digest` uses `sha256:<64 lowercase hex>` over the complete plaintext file bytes;
- `content_ref.size_bytes` records the complete byte length;
- the object key leaf is the raw 64-character hex digest, while JSON keeps the full `sha256:<hex>` digest string;
- all content-object access resolves `namespace_id` through the namespace descriptor to its `content_store_id`;
- future content strategies must use a new `content_ref.kind` and name their durability and validation rules before revisions may reference them.

### 4.2 Upload-before-publish

Metadata may reference content only after that content is already durable.

This applies to:

- file create;
- file replace; and
- file restore, when the restore introduces a newly referenced content object.

## 5. Tombstones and deletion

Deletion is logical first. When an item is deleted, LoonFS records tombstone metadata that hides the file or subtree from visible lookups. The delete becomes visible as part of normal namespace history.

Physical reclamation is separate background work. It may happen only when retention and reference-safety rules allow it.

Namespace deletion does not imply content-store deletion. In v0, content-store deletion and destructive content garbage collection are unsupported operator-only work.

## 6. Forks

Forking a namespace creates a new namespace with independent metadata history and the same `content_store_id` as the source namespace. The fork point is the source namespace's current head. The implementation creates or reuses a verified source checkpoint at that head, writes the target head first to reserve the namespace, rewrites checkpoint artifacts under the new namespace id and object keys, writes the target lease, and writes the namespace descriptor last as the publish/list marker.

No durable parent/child relationship is part of v0 namespace state. After fork, the clone must remain readable even if the source namespace metadata is deleted or corrupted. Source writes after the fork do not affect the clone, and clone writes do not affect the source.

## 7. Mounts

A mount presents another namespace, or a subtree of another namespace, inside the current tree.

A mount carries:

- a target namespace id
- a target root inode id within that namespace

This allows a composed visible tree without inventing one global namespace history underneath.

Two rules apply:

1. path resolution may cross a mount;
2. mount loops are invalid and must be rejected.

A share grants access to a subtree. A mount presents that accessible subtree at a path. The two concepts are related, but they are not the same.

## 8. Cross-namespace moves

Identity is namespace-local. A true inode-preserving rename is therefore namespace-local as well.

Across namespaces, a move is modeled as a copy plus a delete from the source namespace. Same-content-store copies may reuse `content_ref`. Cross-content-store copies are not supported in v0 unless the content is first imported into the destination content store. Inode identity does not cross the namespace boundary.

## 9. Recovery basis

Readers reconstruct authoritative state from:

1. the current head;
2. the checkpoint named by the head, if any; and
3. the visible WAL segment chain after that checkpoint through `head.seq`, replayed as logical commits in ascending `seq` order.

The head summarizes the current visible boundary and replay hints, including at minimum:

- `seq`
- `next_inode_id`
- `checkpoint_hint_seq`
- `retention_floor_seq`
- `wal_tip_segment_id` or an equivalent visible tail pointer

A checkpoint is authoritative only when it has been verified against its durable objects and namespace summary. If verification fails, readers must not treat that checkpoint as authoritative.

The WAL preserves ordered history even when multiple logical commits are stored in one segment. Checkpoints keep replay bounded. Together they provide recovery from durable artifacts alone without requiring unbounded WAL replay as history grows.
