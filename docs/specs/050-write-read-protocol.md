# Write & Read Protocol

## 1. Write protocol

A write has four phases: durably stage content (if a mutation contains content), reconstruct and validate, publish one or more logical commits into the WAL, and advance the head. A commit request may be rejected immediately, or tentatively accepted and written to a WAL segment, but it is committed and successful only if the WAL segment is durably stored and the head advances to reference it. A metadata change becomes visible only after the head advances.

### 1.1 Content staging

Content must be durable before any metadata change can reference it.

1. Compute the `sha256` digest of the complete plaintext file bytes.
2. Resolve the namespace descriptor to its `content_store_id`.
3. Upload the complete byte sequence to `content-stores/{content_store_id}/blobs/sha256/{hex[0..2]}/{hex[2..4]}/{hex}` with create-if-absent semantics.
4. Build a content reference:

   ```json
   {
     "kind": "whole_file_v0",
     "digest": "sha256:<64hex>",
     "size_bytes": 123
   }
   ```

Content staging is idempotent and has no effect on the visible tree. If the caller crashes mid-upload, orphaned content objects are harmless.

### 1.2 Basis reconstruction

Before evaluating commit requests, the server reconstructs the current metadata state using the same procedure described in §2.1: load the head, load the checkpoint (if any), and replay the visible WAL segment chain. The server never trusts caller-supplied metadata.

### 1.3 Validation and logical commits

The server validates each commit request against the reconstructed state:

1. Resolve any operation-local references needed to identify referenced content.
2. Verify that all referenced content objects are already durable in object storage, and that `content_ref.kind`, digest, and size match. When provider-verified SHA-256 metadata is present, this validation may use metadata instead of downloading the whole object; otherwise it must read and hash the object bytes.
3. Evaluate preconditions in order (see §6 for the precondition catalogue).
4. Resolve inode references and allocate new inode ids monotonically from the head's `next_inode_id`.

If a request contains multiple operations, they are evaluated sequentially against ephemeral state advanced by earlier operations in the same request.

Passing validation does not by itself make the request committed or successful. If a client mutation request reaches the success boundary in §1.4, it becomes one logical commit. Distinct client commit requests remain distinct logical commits even when they are published in the same WAL segment.

Content reference validation fails before metadata preconditions are evaluated when:

- `content_ref.kind` is unsupported;
- `content_ref.digest` is not a valid `sha256:<64 lowercase hex>` digest;
- the referenced object is missing from the namespace's content store;
- the object size differs from `content_ref.size_bytes`; or
- provider-verified checksum metadata, when present, differs from `content_ref.digest`; or
- when checksum metadata is absent, the object bytes hash to a different digest than `content_ref.digest`.

### 1.4 WAL segment publication and head advance

This is the success boundary.

1. Collect one or more candidate commit requests.
2. Choose publication order and validate those requests against ephemeral state advanced by earlier tentatively accepted requests in the same batch.
3. Reject immediately any request whose preconditions fail or whose mutation is otherwise invalid.
4. Tentatively accept the remaining requests and assign contiguous `seq` values.
5. Write one immutable **WAL segment** containing logical commit records for the tentatively accepted requests and the segment metadata needed to identify the visible segment chain.
6. **CAS-update** the namespace head to advance `seq`, `next_inode_id`, and the visible WAL tip.
7. If step 5 or step 6 fails, the publication fails. A WAL segment written before a failed CAS is orphaned and harmless.

A tentatively accepted request is not yet committed or successful. A request becomes committed, successful, and visible only if step 5 durably stores the WAL segment and step 6 succeeds. A request rejected at step 3 receives no `seq` and creates no durable WAL record.

### 1.5 Failure semantics inside a publication batch

A publication batch is not an all-or-nothing multi-client transaction.

The server may:

- reject some candidate requests before publication; and
- tentatively accept other requests into the same batch and, if publication succeeds, publish them in the same WAL segment.

Each request still has its own success or failure outcome. Tentative acceptance inside a batch is not success.

## 2. Read protocol

A read reconstructs the visible filesystem state from durable artifacts on object storage. No server-side cache or local database is required for correctness; everything needed is in the object store.

### 2.1 Basis reconstruction

The reader builds an in-memory metadata state from two kinds of durable object:

1. Read the namespace descriptor and content-store descriptor to learn the namespace's immutable content-store relationship.
2. Read the namespace **head** object to learn the current `seq`, `checkpoint_hint_seq`, and visible WAL tip.
3. If `checkpoint_hint_seq` is set, load the **verified checkpoint** at that `seq`. The checkpoint manifest references materialized metadata tables through that `seq`: full base tables plus any delta-run tables containing WAL-derived rows after the base.
4. Use the visible WAL tip named by the head to identify the visible segment chain after the checkpoint `seq` (or from genesis, if no checkpoint exists), then replay the logical commit records in ascending `seq` order through `head.seq`. Each logical commit appends normalized rows to the same metadata tables.

The result is a complete metadata state pinned to one `seq`.

### 2.2 Visibility rules

Given a metadata state at seq N:

- An **inode** is visible if `created_seq <= N` and no active subtree tombstone covers the inode or any of its ancestors.
- A **directory binding** is active if it is the latest `(parent_inode_id, name_key)` pair with `bind_seq <= N`, is also the latest parent binding for the child inode, and has not been removed by a matching direntry unbind.
- A **file revision** is the latest revision for an inode with `committed_seq <= N`.

### 2.3 Path resolution

To resolve an absolute path at seq N:

1. Start at the root inode (inode id 1).
2. For each path component, find the active directory binding whose normalized `name_key` matches the component under the namespace's `NamePolicy`.
3. Follow the binding to its `child_inode_id`; v0 path resolution does not cross mounts, and mount traversal is reserved future work.
4. If any component has no matching visible binding, the path does not exist.

### 2.4 File content retrieval

Given a visible file inode at seq N:

1. Look up the file's latest revision at N to obtain `content_ref`.
2. Resolve the namespace descriptor to its `content_store_id`.
3. Verify that `content_ref.kind` is supported by the reader.
4. For `whole_file_v0`, fetch the object at `content-stores/{content_store_id}/blobs/sha256/{hex[0..2]}/{hex[2..4]}/{hex}`, where `hex` is the digest suffix from `content_ref.digest`.
5. Verify that the fetched bytes match `content_ref.size_bytes` and `content_ref.digest`.

A **file revision** is an immutable content state for one file inode, identified by that inode's
monotonic `revision_no`. A namespace commit `seq` is the global visibility order for committed
mutations; it is not a file revision number. Revision reads may target either the current path's
current inode or an inode id directly. Path-based revision reads first resolve the path at the
current head; inode-based revision reads use the retained revision rows for that inode.

### 2.5 Directory listing

Given a visible directory inode at seq N:

1. Collect all active directory bindings whose `parent_inode_id` matches the directory.
2. For each binding, resolve the child inode. If the child is a file, its latest revision provides size and content identity through `content_ref`.

## 3. Logical commits, sequence numbers, and visibility

A successful client mutation request is one logical commit.

A request may contain more than one operation, but:

- the operations are evaluated in request order; and
- the request becomes one ordered logical commit in namespace history.

Each successful logical commit receives exactly one namespace `seq`. A request that is rejected receives no `seq`. A request may be assigned a `seq` while tentatively accepted into a batch, but it is not committed or successful unless the WAL segment is durable and the head update succeeds.

One head update may publish one or more contiguous logical commits.

A logical commit becomes visible only when the head advances to a value at or beyond that commit's `seq` and the visible WAL chain includes that commit.

This gives each successful request one `seq` and one replay identity without requiring one object write or one head update per request.

## 4. Server authority

The server is authoritative for mutation validation.

In particular, the server is responsible for:

- resolving any supplied paths against the current visible tree;
- allocating new inode ids;
- validating name collisions according to the namespace's `NamePolicy`;
- validating preconditions;
- verifying that referenced content is already durable; and
- publishing successful logical commits by durably writing a WAL segment and advancing the head.

Clients may assist with planning, hashing, upload, or retry, but they are not the authority for visible state.

The server need not be centralized. The protocol is designed for multiple writers.

## 5. Standard mutation operations

The first standard lower-level mutation set includes:

- `create_dir(parent_inode_id, display_name)`
- `create_file(parent_inode_id, display_name, content_ref)`
- `replace_file(inode_id, base_revision_no, content_ref)`
- `rename(inode_id, new_parent_inode_id, new_display_name, mode = no_replace)`
- `delete_file(inode_id)`
- `delete_subtree(root_inode_id)`
- `restore_revision(inode_id, source_revision_no, base_revision_no)`

The path-oriented filesystem surface may compile higher-level operations into these lower-level mutations.

`rename` accepts the explicit mode shape for forward compatibility, but v0 implements only `no_replace`. Reserved modes such as `replace_existing` and `exchange` must fail with `unsupported_rename_mode`.

These are semantic commit operations. Durable WAL payloads store normalized metadata deltas derived from the semantic operations: `create_inode`, `bind_direntry`, `unbind_direntry`, `append_file_revision`, and `tombstone_subtree`. Raw bind/unbind/create-inode deltas are not standard client-facing commit operations.

## 6. Preconditions

A mutation may include explicit preconditions. Preconditions are how clients say, "apply this only if the namespace still looks like the state I planned against."

The core kinds of precondition are:

| Kind of check | Example use |
| --- | --- |
| **Name-slot based** | "Create this child only if that name slot is still empty." |
| **Name-binding based** | "Move or delete this item only if this name still points at the inode I saw." |
| **Revision-based** | "Replace this file only if it is still at the revision I saw." |
| **Ancestor-visibility based** | "Apply this only if no ancestor was tombstoned." |
| **Directory-contents based** | "Delete this directory non-recursively only if it is still empty." |

The exact wire shape of preconditions may vary by transport binding, but the semantics must match these checks.

The exact binding precondition is `binding_is(parent_inode_id, name_key, child_inode_id, bind_seq, bind_delta_index)`. It pins a source path to one specific prior binding, so a rename-away, delete, or same-name rebind cannot accidentally satisfy a stale move or delete.

## 7. Change feed and replay

A namespace exposes an ordered change feed. The feed answers the question:

> What committed metadata changes happened after `seq = N`?

This feed is the basis for sync engines, replication, and other incremental consumers.

The change feed is ordered by logical commit, not by physical WAL segment. A segment containing N logical commits produces N ordered change events.

Each change event exposes both layers of the committed mutation:

- semantic operation results, keyed by `op_index`, describe the client-facing operation outcome; and
- materialized WAL deltas, keyed by `semantic_op_index` and `delta_index`, describe the authoritative metadata facts that replay/projectors should apply.

## 8. Retention floor

A namespace may advance a retention floor to say:

> Incremental replay older than this point is no longer promised.

Clients older than the retention floor must re-bootstrap from a fresh checkpoint instead of replaying from an obsolete cursor.

The retention floor may advance only after the system has enough verified material to keep replay safe at or after that point.

## 9. Namespace forks

A fork creates a new namespace from the source namespace's current head. The request supplies only the new namespace id; the server supplies the mutation context.

The fork protocol is:

1. Check the target namespace initialization state. A complete target is rejected as existing, and a partial target is rejected as partially initialized.
2. Resolve and verify the source namespace descriptor, content-store descriptor, head, lease, checkpoint, and WAL basis.
3. Create or reuse a verified source checkpoint at the current source head.
4. Build the target head, lease, and descriptor using the source namespace's `content_store_id`.
5. Write the target `head.json` first to reserve the namespace.
6. Rebuild checkpoint artifacts under the new namespace id with fresh checksums and object keys.
7. Write the target `lease.json`.
8. Write the target `descriptor.json` last as the publish/list marker.
9. Start the new namespace WAL independently at `fork_seq + 1`.

The fork copies namespace-local checkpoint metadata only. It does not copy content-store blobs. If initialization fails after the target head exists but before the descriptor is published, the target is partial. A successful fork has independent namespace history from the fork point. It also does not create a durable parent/child relationship; provenance may be recorded later as audit metadata outside the core namespace model.

## 10. Long-running operations

Some operations are not well described by one request.

Examples include:

- recursive reads that need a pinned snapshot; and
- resumable uploads that need a stable destination binding.

In those cases, the server may create control-plane objects such as read sessions, upload sessions, or put intents.

Three rules apply:

1. these objects may be ephemeral when no durability guarantee is required; if an operation's correctness, restart safety, or promised resumability depends on them, they must be stored durably in object storage;
2. they do not advance namespace `seq`;
3. they do not appear in the namespace change feed.
