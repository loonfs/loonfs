# Write & Read Protocol

## 1. Write protocol

A write has four phases: durably stage content (if mutation contains content), reconstruct and validate, commit to the WAL, and advance the head. A metadata change becomes visible only after the head advances.

### 1.1 Content staging

Content must be durable before any metadata change can reference it.

1. Split the file into fixed **16 MiB blocks** (the final block may be shorter). Compute the `sha256` digest of each block.
2. Upload each block to `namespaces/{namespace_id}/blobs/{block_digest}` with create-if-absent semantics.
3. Build a content manifest listing `file_size_bytes`, `file_digest_sha256`, `block_size_bytes`, and the ordered block digests and sizes.
4. Upload the manifest to `namespaces/{namespace_id}/manifests/{content_manifest_digest}.json` with create-if-absent semantics.

Content staging is idempotent and has no effect on the visible tree. If the caller crashes mid-upload, orphaned blocks are harmless.

### 1.2 Basis reconstruction

Before evaluating a mutation, the server reconstructs the current metadata state using the same procedure described in §2.1: load the head, load the checkpoint (if any), and replay the WAL tail. The server never trusts caller-supplied metadata.

### 1.3 Validation

The server validates the mutation against the reconstructed state:

1. Resolve any operation-local references needed to identify referenced content.
2. Verify that all referenced content (manifests and blocks) is already durable in object storage, and that digests and sizes match.
3. Evaluate preconditions in order (see §6 for the precondition catalogue).
4. Resolve inode references and allocate new inode ids monotonically from the head's `next_inode_id`.

If a request contains multiple operations, they are evaluated sequentially against ephemeral state advanced by earlier operations in the same request.

### 1.4 WAL commit and head advance

This is the atomicity boundary.

1. Write one immutable **WAL entry** containing the validated operations, allocated inode ids, and preconditions. The WAL key includes the next `seq`.
2. **CAS-update** the namespace head to advance `seq` and `next_inode_id`.
3. If the CAS fails, the commit fails. The WAL entry is orphaned and harmless.

The change becomes visible only after step 2 succeeds.

## 2. Read protocol

A read reconstructs the visible filesystem state from durable artifacts on object storage. No server-side cache or local database is required for correctness; everything needed is in the object store.

### 2.1 Basis reconstruction

The reader builds an in-memory metadata state from two kinds of durable object:

1. Read the namespace **head** object to learn the current `seq` and `snapshot_hint_seq`.
2. If `snapshot_hint_seq` is set, load the **verified checkpoint** at that seq. The checkpoint materializes metadata state through that seq across four append-only tables: inodes, direntries, revisions, and subtree tombstones.
3. Load and replay every **WAL entry** contiguously after the checkpoint seq (or from genesis, if no checkpoint exists) through `head.seq`. Each WAL entry appends rows to the same four tables.

The result is a complete metadata state pinned to one `seq`.

### 2.2 Visibility rules

Given a metadata state at seq N:

- An **inode** is visible if `created_seq <= N` and no active subtree tombstone covers the inode or any of its ancestors.
- A **directory binding** is active if it is the latest `(parent_inode_id, name_key)` pair with `bind_seq <= N`.
- A **file revision** is the latest revision for an inode with `committed_seq <= N`.

### 2.3 Path resolution

To resolve an absolute path at seq N:

1. Start at the root inode (inode id 1).
2. For each path component, find the active directory binding whose normalized `name_key` matches the component under the namespace's `NamePolicy`.
3. Follow the binding to its `child_inode_id`. If the child is a mount, cross into the target namespace and continue resolution there.
4. If any component has no matching visible binding, the path does not exist.

### 2.4 File content retrieval

Given a visible file inode at seq N:

1. Look up the file's latest revision at N to obtain `content_manifest_digest`.
2. Fetch the content manifest from object storage at `namespaces/{namespace_id}/manifests/{content_manifest_digest}.json`.
3. The manifest lists the file's ordered block digests. Fetch each block from `namespaces/{namespace_id}/blobs/{block_digest}`.
4. Concatenate the blocks in manifest order to produce the file bytes.

### 2.5 Directory listing

Given a visible directory inode at seq N:

1. Collect all active directory bindings whose `parent_inode_id` matches the directory.
2. For each binding, resolve the child inode. If the child is a file, its latest revision provides size and content digest via the manifest.

## 3. One request, one visible sequence

A successful mutation request is published as one namespace `seq`.

A request may contain more than one operation, but:

- the operations are evaluated in request order;
- the request becomes visible as one committed step in namespace history.

Each request has a single visibility and replay point.

## 4. Server authority

The server is authoritative for mutation validation.

In particular, the server is responsible for:

- resolving any supplied paths against the current visible tree;
- allocating new inode ids;
- validating name collisions according to the namespace's `NamePolicy`;
- validating preconditions;
- verifying that referenced content is already durable; and
- publishing the final WAL entry and head update.

Clients may assist with planning, hashing, upload, or retry, but they are not the authority for visible state.

The server need not be centralized. The protocol is designed for multiple writers.

## 5. Standard mutation operations

The first standard lower-level mutation set includes:

- `create_dir(parent_inode_id, display_name)`
- `create_file(parent_inode_id, display_name, content_manifest_digest)`
- `replace_file(inode_id, base_revision_no, content_manifest_digest)`
- `rename(inode_id, new_parent_inode_id, new_display_name)`
- `delete_subtree(root_inode_id)`
- `restore_revision(inode_id, source_revision_no, base_revision_no)`

The path-oriented filesystem surface may compile higher-level operations into these lower-level
mutations.

## 6. Preconditions

A mutation may include explicit preconditions. Preconditions are how clients say, "apply this only if the namespace still looks like the state I planned against."

The core kinds of precondition are:

| Kind of check | Example use |
| --- | --- |
| **Head-based** | "Apply this only if I planned against the current head." |
| **Name-slot based** | "Create this child only if that name slot is still empty." |
| **Revision-based** | "Replace this file only if it is still at the revision I saw." |
| **Ancestor-visibility based** | "Apply this only if no ancestor was tombstoned." |

The exact wire shape of preconditions may vary by transport binding, but the semantics must match these checks.

## 7. Change feed and replay

A namespace exposes an ordered change feed. The feed answers the question:

> What committed metadata changes happened after `seq = N`?

This feed is the basis for sync engines, replication, and other incremental consumers.

## 8. Retention floor

A namespace may advance a retention floor to say:

> Incremental replay older than this point is no longer promised.

Clients older than the retention floor must re-bootstrap from a fresh snapshot instead of replaying from an obsolete cursor.

The retention floor may advance only after the system has enough verified material to keep replay safe at or after that point.

## 9. Long-running operations

Some operations are not well described by one request.

Examples include:

- recursive reads that need a pinned snapshot;
- large or resumable uploads that need a stable destination binding;
- same-service recursive copy jobs.

In those cases, the server may create control-plane objects such as read sessions, upload sessions, put intents, import jobs, or copy jobs.

Three rules apply:

1. these objects may be ephemeral when no durability guarantee is required; if an operation's correctness, restart safety, or promised resumability depends on them, they must be stored durably in object storage;
2. they do not advance namespace `seq`;
3. they do not appear in the namespace change feed.
