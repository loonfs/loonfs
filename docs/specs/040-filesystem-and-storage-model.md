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

The inode itself is only one part of the metadata model. A complete visible file usually involves
multiple logical records.

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
  "content_manifest_digest": "sha256:manifest..."
}
```

Together, those three records mean:

- inode `42` is the durable identity of the file;
- the file is currently visible under parent directory inode `9` as `Report.txt`; and
- the current visible file bytes come from revision `7`.

If the file is renamed, the direntry changes but the inode stays `42`. If the file contents are
replaced, the revision row changes but the inode stays `42`.

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

Each revision points to exactly one immutable content manifest. The manifest, in turn, describes the ordered list of immutable content blocks that reconstruct the file bytes.

Blocks and manifests belong to the owning namespace. A file revision may reference only content that is durable under that namespace's content store.

LoonFS therefore uses a two-stage write model:

```text
make content durable  ->  then make metadata visible
```

This separation is part of the core model.

### 4.1 Immutable content storage

The stable immutable content families are:

```text
namespaces/{namespace_id}/blobs/{block_digest_sha256}
namespaces/{namespace_id}/manifests/{content_manifest_digest}.json
```

The core rules are:

- block digests use `sha256:<hex>` over plaintext block bytes;
- blocks are fixed at `16 MiB`, except the final block may be shorter;
- the content manifest records `namespace_id`, `file_size_bytes`, `file_digest_sha256`, `block_size_bytes`, and the ordered block digests and block sizes; and
- `content_manifest_digest` is the digest of the canonical manifest bytes.

### 4.2 Upload-before-publish

Metadata may reference content only after that content is already durable.

This applies to:

- file create;
- file replace; and
- file restore, when the restore introduces a newly referenced manifest.

## 5. Tombstones and deletion

Deletion is logical first. When an item is deleted, LoonFS records tombstone metadata that hides the file or subtree from visible lookups. The delete becomes visible as part of normal namespace history.

Physical reclamation is separate background work. It may happen only when retention and reference-safety rules allow it.

## 6. Mounts

A mount presents another namespace, or a subtree of another namespace, inside the current tree.

A mount carries:

- a target namespace id
- a target root inode id within that namespace

This allows a composed visible tree without inventing one global namespace history underneath.

Two rules apply:

1. path resolution may cross a mount;
2. mount loops are invalid and must be rejected.

A share grants access to a subtree. A mount presents that accessible subtree at a path. The two concepts are related, but they are not the same.

## 7. Cross-namespace moves

Identity is namespace-local. A true inode-preserving rename is therefore namespace-local as well.

Across namespaces, a move is modeled as a copy plus a delete from the source namespace. Content may still be reused internally, but inode identity does not cross the namespace boundary.

## 8. Recovery basis

Readers reconstruct authoritative state from:

1. the current head;
2. the checkpoint named by the head, if any; and
3. the contiguous WAL tail after that checkpoint through `head.seq`.

The head summarizes the current visible boundary and replay hints, including at minimum:

- `seq`
- `next_inode_id`
- `snapshot_hint_seq`
- `retention_floor_seq`

A checkpoint is authoritative only when it has been verified against its durable objects and namespace summary. If verification fails, readers must not treat that checkpoint as authoritative.

The WAL preserves ordered history. Checkpoints keep replay bounded. Together they provide recovery from durable artifacts alone without requiring unbounded WAL replay as history grows.
