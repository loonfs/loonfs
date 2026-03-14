# Spec 040: namespace commit protocol

## Purpose

A namespace commit is the operation that makes metadata changes visible.

## Publish rule

A metadata change becomes visible only when the namespace head advances successfully.

Why it exists:
it gives one publish point for visibility.

Failure mode prevented:
readers seeing half-applied metadata.

## Plain-language write path

1. upload missing blocks
2. upload the content manifest
3. acquire or renew the namespace lease
4. validate preconditions against the latest head
5. write an immutable WAL commit object
6. CAS-update the head object
7. return success only after step 6 succeeds

## Preconditions

Mutations are never path-addressed. They are inode-addressed and explicit.

Example preconditions:

- planned head seq still matches
- target inode is still a file
- current revision is still `12`
- target child name is absent
- ancestors are not covered by a subtree tombstone

Why they exist:
they make races observable and reviewable.

Failure mode prevented:
silent last-writer-wins corruption.

## Create mutations

The initial mutation set adds two authoritative create operations:

- `create_dir(parent_inode, display_name)`
- `create_file(parent_inode, display_name, content_manifest_digest)`

Rules:

- the namespace head still owns inode allocation
- each create operation consumes exactly one inode id from `head.next_inode_id`
- `create_file` requires durable content before publish, just like `replace_file`
- `content_manifest_digest` must identify one immutable manifest object at
  `namespaces/{namespace_id}/manifests/{content_manifest_digest}.json`
- `create_dir` does not require a content manifest

The first create preconditions are:

- `HeadSeqIs(current_head.seq)`
- `ChildNameAbsent(parent_inode, name_key)`
- `AncestorsNotSubtreeDeleted(parent_inode)`

For the current create-mutation contract, `name_key` may equal `display_name` until the shared versioned
name-policy layer is wired into request construction.

Why these rules exist:

- create operations need canonical inode allocation, not client-guessed ids
- the initial create-mutation contract should still reject obvious lost-update races
- file creates must not publish metadata before their content is durable

Failure modes prevented:

- two writers silently creating different children with the same allocated inode id
- file metadata publishing without durable content
- a create under a deleted ancestor becoming visible

Failure modes named for the first implementation:

- `create_mutation_consumes_next_inode_id`
- `create_file_requires_durable_content`
- `create_parent_missing`
- `create_parent_not_directory`
- `create_child_name_collision`
- `create_under_subtree_tombstone`

## Replace-file mutation

The first inode-keyed edit mutation is:

- `replace_file(inode_id, base_revision_no, content_manifest_digest)`

Rules:

- `replace_file` updates the content revision of one existing file inode
- it does not rename the inode and it does not move the inode to a new parent
- `content_manifest_digest` must identify one immutable manifest object at
  `namespaces/{namespace_id}/manifests/{content_manifest_digest}.json`
- `replace_file` requires durable content before publish

The first replace-file preconditions are:

- `HeadSeqIs(current_head.seq)`
- `InodeRevisionIs(inode_id, base_revision_no)`
- `AncestorsNotSubtreeDeleted(inode_id)`

Why these rules exist:

- the first bound-file edit path should use canonical inode identity, not path mutation guesses
- the first client edit executor only covers content replacement, not rename or move
- file edits must still prove durable content before metadata may publish

Failure modes prevented:

- uploading new bytes into the wrong inode after a local rename
- publishing a file revision against a stale base revision
- publishing metadata that points at missing or corrupted content

Failure modes named for the first implementation:

- `replace_file_requires_durable_content`
- `replace_file_inode_missing`
- `replace_file_inode_not_file`
- `replace_file_base_revision_mismatch`
- `replace_file_under_subtree_tombstone`
- `replace_file_path_change_not_supported`

## Authoritative metadata precondition lookups

The semantic meaning of a commit precondition comes from the authoritative metadata state at:

- `base_seq = request.planned_head_seq`

Frame validation may happen first, but commit planning is not complete until metadata preconditions
are evaluated against the logical metadata families described in Spec 030.

### `HeadSeqIs(expected_seq)`

Lookup rule:

- compare `expected_seq` to the current durable `head.json.seq`

Why it exists:

- every deeper metadata lookup must be anchored to one explicit namespace history point

### `ChildNameAbsent(parent_inode_id, name_key)`

Lookup rule:

1. resolve `inode_at_seq(namespace_id, parent_inode_id, base_seq)`
2. require the parent inode to exist
3. require the parent inode kind to be `DIR`
4. resolve `bound_child_at_seq(namespace_id, parent_inode_id, name_key, base_seq)`
5. require that lookup to return no bound child binding

The current create contract may still use `display_name` as `name_key`, but the lookup rule itself
is in terms of canonical `name_key`.

Failure modes named for the first implementation:

- `create_parent_missing`
- `create_parent_not_directory`
- `create_child_name_collision`

### `InodeRevisionIs(inode_id, revision_no)`

Lookup rule:

1. resolve `inode_at_seq(namespace_id, inode_id, base_seq)`
2. require the inode to exist
3. require the inode kind to be `FILE` for the current `replace_file` mutation family
4. resolve `latest_revision_head_at_seq(namespace_id, inode_id, base_seq)`
5. require the resolved head revision to equal `revision_no`

Failure modes named for the first implementation:

- `replace_file_inode_missing`
- `replace_file_inode_not_file`
- `replace_file_base_revision_mismatch`

### `AncestorsNotSubtreeDeleted(inode_id)`

Lookup rule:

1. start from `inode_id`
2. walk the visible parent chain toward the namespace root at `base_seq`
3. at each visited inode, require that no `active_subtree_tombstone(namespace_id, visited_inode, base_seq)` exists

For create operations this check is anchored at the target parent inode, because the child inode does
not exist yet.

Failure modes named for the first implementation:

- `create_under_subtree_tombstone`
- `replace_file_under_subtree_tombstone`

## Metadata evaluation order

The first semantic-core commit planner should evaluate metadata preconditions in this order after
frame validation succeeds:

1. `HeadSeqIs`
2. inode existence / kind lookups needed by the request ops
3. `ChildNameAbsent` and `InodeRevisionIs`
4. `AncestorsNotSubtreeDeleted`

Why this order exists:

- it keeps the lookup anchor explicit before any deeper table scan
- it makes failure reporting deterministic when more than one metadata rule is wrong
- it avoids allocating inode ids or writing WAL objects for requests that are already invalid at the
  metadata layer

Failure modes prevented:

- a request reporting different failures depending on incidental lookup order
- spending inode allocation or WAL work on a request that already loses at metadata validation

## First authoritative metadata transition rules

After metadata preconditions succeed and the commit is accepted, the canonical metadata state must
advance by appending rows at:

- `commit_seq = plan.next_seq`

No existing inode, direntry, or revision row is rewritten in place.

### `create_dir(parent_inode, display_name)`

The committed transition must append:

- one inode row for the allocated inode id with:
  `inode_kind = DIR` and `created_seq = commit_seq`
- one direntry row under `(parent_inode, name_key)` with:
  `display_name`, `child_inode_id = allocated inode id`, and `bind_seq = commit_seq`

It must not append a revision row.

### `create_file(parent_inode, display_name, content_manifest_digest)`

The committed transition must append:

- one inode row for the allocated inode id with:
  `inode_kind = FILE` and `created_seq = commit_seq`
- one direntry row under `(parent_inode, name_key)` with:
  `display_name`, `child_inode_id = allocated inode id`, and `bind_seq = commit_seq`
- one revision row for the allocated inode id with:
  `revision_no = 1`, `committed_seq = commit_seq`, and the committed
  `content_manifest_digest`

### `replace_file(inode_id, base_revision_no, content_manifest_digest)`

The committed transition must append:

- one revision row for `inode_id` with:
  `revision_no = base_revision_no + 1`,
  `committed_seq = commit_seq`,
  and the committed `content_manifest_digest`

It must not append or rewrite an inode row.
It must not append or rewrite a direntry row.

### Allocation ordering rule

If one request contains multiple create operations, allocated inode ids are consumed in request-op
order from `head.next_inode_id`.

Why these rules exist:

- the logical metadata families only stay replayable if every successful mutation names the exact
  rows it appends
- create and replace must update revision history differently
- replay should not depend on implicit server-side side effects

Failure modes prevented:

- a successful create that advances `head.json` without a corresponding visible direntry
- a successful file create without an initial revision row
- a file replace that mutates path bindings or overwrites older revision history

Failure modes named for the first implementation:

- `create_dir_writes_inode_and_direntry_rows`
- `create_file_writes_inode_direntry_and_initial_revision`
- `replace_file_appends_new_revision_head`

## Authoritative durable content validation

Before the authoritative side may publish `create_file` or `replace_file`, it must validate the
referenced immutable content objects against object storage.

Validation steps:

1. load `namespaces/{namespace_id}/manifests/{content_manifest_digest}.json`
2. decode `ContentManifestEnvelope` and verify `payload_checksum_sha256`
3. recompute the manifest object's digest from the stored JSON bytes and require it to equal
   `content_manifest_digest`
4. require `payload.namespace_id` to match the request namespace
5. for every listed block, load `namespaces/{namespace_id}/blobs/{block_digest_sha256}`
6. require every loaded block's raw bytes to match its listed digest and size
7. require the ordered concatenation of those blocks to reproduce
   `payload.file_size_bytes` and `payload.file_digest_sha256`

Why these rules exist:

- `create_file_requires_durable_content` and `replace_file_requires_durable_content`
  should be enforced against real durable objects, not just against a non-empty digest string
- metadata should not publish if the referenced manifest or any referenced block is missing,
  corrupted, or namespace-crossed

Failure modes prevented:

- a create request pointing at a manifest object with tampered JSON
- a create request pointing at a manifest whose listed blocks do not exist
- a create request pointing at blocks whose bytes no longer match the manifest descriptors
- a create request pointing at a manifest whose whole-file digest does not match its block list

Failure modes named for the first implementation:

- `create_file_manifest_missing`
- `create_file_manifest_digest_mismatch`
- `create_file_manifest_namespace_mismatch`
- `create_file_block_missing`
- `create_file_block_descriptor_mismatch`
- `create_file_file_digest_mismatch`
- `replace_file_manifest_missing`
- `replace_file_manifest_digest_mismatch`
- `replace_file_manifest_namespace_mismatch`
- `replace_file_block_missing`
- `replace_file_block_descriptor_mismatch`
- `replace_file_file_digest_mismatch`

## Fencing

Lease ownership changes must change the active fencing token.

Why it exists:
an old writer may still be alive after a failover.

Example:
writer A reads head with fence token 41. writer B takes over and publishes token 42. A must not be able to publish later using its stale view.

## Restore revision rule

Restoring revision 3 while revision 7 is current creates revision 8 that points to revision 3’s content.

Why it exists:
history should be monotonic.

Failure mode prevented:
moving the head backward and rewriting history.

## Control objects used by commit validation

The namespace commit path depends on two small JSON control objects:

### `head.json`

The head object is the authoritative summary of the latest visible namespace state.

It must carry:

- `kind = "namespace_head"`
- `format_version = 1`
- `writer_version`
- `payload_checksum_sha256`
- `state.namespace_id`
- `state.seq`
- `state.active_fence_token`
- `state.next_inode_id`
- `state.snapshot_hint_seq`
- `state.retention_floor_seq`

Why these fields exist:

- `seq` gives one publish boundary
- `active_fence_token` fences stale writers
- `next_inode_id` keeps allocation inside the serialized head update
- `snapshot_hint_seq` tells readers where checkpoint replay may start
- `retention_floor_seq` tells readers whether incremental replay is still promised

Failure modes prevented:

- separate inode-id allocation side channels
- readers guessing the replay start point
- stale writers publishing after lease takeover

### `lease.json`

The lease object is the current writer claim for the namespace.

It must carry:

- `kind = "namespace_lease"`
- `format_version = 1`
- `writer_version`
- `payload_checksum_sha256`
- `state.namespace_id`
- `state.holder_id`
- `state.fence_token`
- `state.lease_expires_at_ms`

Why these fields exist:

- `holder_id` tells us who currently owns the write lease
- `fence_token` must match the active head token before publish
- `lease_expires_at_ms` makes expiration an explicit input to deterministic validation

Failure modes prevented:

- old writers publishing after a leadership handoff
- silent disagreement between head state and lease state
- validation logic depending on ambient wall-clock reads

## Frame validation before metadata evaluation

Before metadata lookups or WAL creation begin, the commit planner must at minimum validate:

1. the request namespace matches the current head and lease namespace
2. the planned head seq still matches the current head seq
3. the request carries the active fencing token
4. the lease holder still matches the requesting writer
5. the lease has not expired at the explicitly supplied validation time

These checks are necessary but not sufficient. A commit is not valid for publish until the metadata
preconditions above are also evaluated at the same `planned_head_seq`.

## WAL commit object

After validation succeeds, the writer must create one immutable WAL object before attempting the head CAS.

The object key is:

```text
namespaces/{namespace_id}/wal/{seq:020}-{commit_id}.cbor.zst
```

The body is:

1. a `WalCommitEnvelope`
2. encoded as CBOR
3. then compressed with zstd

The envelope must carry:

- `kind = "namespace_wal_commit"`
- `format_version = 1`
- `writer_version`
- `payload_checksum_sha256`
- `payload.namespace_id`
- `payload.seq`
- `payload.base_head_seq`
- `payload.commit_id`
- `payload.request_id`
- `payload.writer_id`
- `payload.writer_fence_token`
- `payload.ops`
- `payload.preconditions`

For the current skeleton, `commit_id` may equal `request_id`.

For create mutations, the WAL must carry the allocated inode id inside the durable op body so
replay can reproduce `next_inode_id` advancement from immutable history alone.

Why these fields exist:

- `seq` and `base_head_seq` make the visible ordering explicit
- `commit_id` makes the immutable object key stable and auditable
- `writer_id` and `writer_fence_token` preserve the publish authority that produced the WAL entry
- `ops` and `preconditions` preserve the semantic input that replay and debugging need
- `payload_checksum_sha256` makes corruption or codec drift observable

Failure modes prevented:

- head advancement that points at missing WAL history
- opaque commit history that cannot explain why metadata changed
- silent corruption of immutable WAL payload bytes

## Head publish rule for create mutations

After the WAL create-if-absent succeeds, the authoritative writer prepares the next `head.json`
summary for CAS.

For the current create mutations, head publication must:

1. advance `seq` to the validated next seq
2. preserve `active_fence_token`
3. advance `next_inode_id` by the number of create operations in the request
4. preserve `snapshot_hint_seq` and `retention_floor_seq`

Why this rule exists:

- inode allocation is part of authoritative metadata publish, not a side channel
- replay and live publish should agree on how create mutations consume head state

Failure modes prevented:

- allocating a new inode without advancing durable head state
- WAL replay and live publish disagreeing about the next free inode id

## Create success response

After the WAL write and head CAS both succeed for a client create mutation, the authoritative side
may return one committed create summary to the client.

For the current create-only client mutation contract, that summary must carry:

- `namespace_id`
- `client_request_id`
- `committed_seq`
- `created_inode.inode_id`
- `created_inode.inode_kind`
- `created_inode.revision_no`
- `created_inode.parent_inode_id`
- `created_inode.display_name`
- `created_inode.content_digest`

Rules:

- the response is only valid after the head CAS succeeds
- `committed_seq` must equal the newly published head seq
- the returned `created_inode.inode_id` must equal the inode id allocated in the WAL op
- the first create revision is `1`
- for files, `created_inode.content_digest` is the whole-file content digest from the validated
  manifest payload, not the manifest object digest

Why this rule exists:

- the client needs one authoritative post-publish observation it can bind immediately
- the success response should be derivable from committed namespace history, not an out-of-band guess

Failure modes prevented:

- returning success before metadata is durably visible
- the client binding a temporary local create to an inode id that does not match committed history

## Write ordering rule

The head CAS must not be attempted until the WAL object create-if-absent succeeds.

If the WAL object already exists with the same key, the write path may treat that as idempotent only after verifying that the stored envelope payload matches the commit it intended to publish.

Failure mode prevented:

- head state publishing a seq whose immutable WAL record was never durably created

## WAL replay skeleton

The first WAL replay skeleton starts from a known basis head and a sorted WAL tail.

For each WAL object, replay must verify:

1. the object decodes successfully and its payload checksum still matches
2. the object key still matches `payload.seq` and `payload.commit_id`
3. `payload.namespace_id` matches the namespace being replayed
4. `payload.base_head_seq` matches the current replay cursor
5. `payload.seq` is exactly one greater than the current replay cursor

If those checks pass, replay must:

- apply the WAL ops into the authoritative metadata families at `payload.seq`
- advance the head summary to:
  - `seq = payload.seq`
  - `active_fence_token = payload.writer_fence_token`
  - `next_inode_id = max(current_head.next_inode_id, allocated create inode ids + 1)`

The current replay path still preserves `snapshot_hint_seq` and `retention_floor_seq` from the
replay basis, but it no longer treats WAL as head-summary-only history.

Why this section exists:

- readers need one deterministic rule for whether a WAL tail is safe to apply
- checkpoint integration depends on the same continuity checks plus the same metadata application
  rules as live publish

Failure modes prevented:

- skipping WAL entries during replay
- applying WAL from the wrong namespace
- accepting corrupted or mismatched immutable history objects
- replaying head seq without replaying the metadata rows that seq made visible

## Checkpoint manifest skeleton

The first checkpoint manifest lives at:

```text
namespaces/{namespace_id}/snapshots/{checkpoint_seq:020}/manifest.json
```

The manifest is JSON and must carry:

- `kind = "namespace_checkpoint_manifest"`
- `format_version = 1`
- `writer_version`
- `payload_checksum_sha256`
- `payload.namespace_id`
- `payload.checkpoint_seq`
- `payload.active_fence_token`
- `payload.next_inode_id`
- `payload.retention_floor_seq`
- `payload.verified`
- `payload.tables`

Each table entry must name:

- `family`
- `segments`

Each segment entry must name:

- `object_key`
- `segment_index`
- `row_count`
- `min_key`
- `max_key`
- `payload_checksum_sha256`
- `page_checksums_sha256`

Replay may trust the manifest only after every referenced segment object is loaded and its typed
rows reconstruct one basis metadata state.

Why these fields exist:

- `checkpoint_seq` pins the replay basis boundary
- `active_fence_token` lets the restored head summary preserve writer generation
- `next_inode_id` preserves deterministic inode allocation after restore
- `retention_floor_seq` preserves replay promises made by the published head
- `verified` keeps partially built checkpoints out of the read path
- per-segment checksums and ranges make segment verification explicit instead of implied

Failure modes prevented:

- using a partially built checkpoint as authoritative state
- restoring a head summary that loses allocator or retention state
- silently drifting between manifest metadata and segment objects

## Checkpoint plus WAL tail replay

The first checkpoint-aware replay path is:

1. load a verified checkpoint manifest
2. load every checkpoint segment object referenced by the manifest
3. for each referenced segment:
   - decode the immutable segment envelope
   - verify the segment object key matches `(namespace_id, checkpoint_seq, family, segment_index)`
   - verify the manifest descriptor matches the decoded segment payload checksum, page checksums, row count, and min/max keys
4. only after all referenced segments verify, derive a basis head with:
   - `seq = checkpoint_seq`
   - `active_fence_token = manifest.active_fence_token`
   - `next_inode_id = manifest.next_inode_id`
   - `snapshot_hint_seq = Some(checkpoint_seq)`
   - `retention_floor_seq = manifest.retention_floor_seq`
5. replay the WAL tail after `checkpoint_seq` using the WAL replay checks above

The reconstructed checkpoint basis metadata must include the same canonical logical families as
live publish:

- inode rows
- direntry rows
- revision rows
- subtree tombstone rows

If the manifest namespace does not match, the manifest key does not match `checkpoint_seq`, `verified` is false, a referenced segment object is missing, or any descriptor does not match its decoded segment body, replay must fail before any WAL is applied.

Failure modes prevented:

- starting replay from the wrong checkpoint
- treating an unverified snapshot as a trusted basis
- trusting manifest metadata that does not match the durable segment bodies
- silently skipping a referenced checkpoint segment during restore
- diverging between checkpoint basis state and later WAL replay

## Checkpoint segment object skeleton

Each checkpoint segment lives at:

```text
namespaces/{namespace_id}/snapshots/{checkpoint_seq:020}/tables/{family}-{segment_index:05}.sst.zst
```

Each segment is an immutable `CheckpointSegmentEnvelope` encoded as CBOR and then compressed with zstd.

The envelope must carry:

- `kind = "namespace_checkpoint_segment"`
- `format_version = 1`
- `writer_version`
- `payload_checksum_sha256`
- `payload.namespace_id`
- `payload.checkpoint_seq`
- `payload.family`
- `payload.segment_index`
- `payload.row_count`
- `payload.min_key`
- `payload.max_key`
- `payload.pages`

Each page entry must name:

- `page_index`
- `min_key`
- `max_key`
- `row_keys`
- `rows`

Each row is typed and must match the segment family:

- `inodes` segments carry `inode(inode_id, inode_kind, created_seq)`
- `direntries` segments carry `direntry(parent_inode_id, name_key, display_name, child_inode_id, bind_seq)`
- `revisions` segments carry `revision(inode_id, revision_no, committed_seq, content_manifest_digest)`
- `tombstones` segments carry `tombstone(root_inode_id, tombstone_seq)`

The manifest stores the segment payload checksum plus one checksum per page. `row_keys`,
`min_key`, `max_key`, and `row_count` are redundant summary fields and must match the typed row
body exactly; replay must validate them before trusting the segment.

For the current writer skeleton, each table family emits one deterministic segment object at
`segment_index = 0`.

Why these fields exist:

- `family` and `segment_index` bind the object body to its durable object key
- segment `row_count`, `min_key`, and `max_key` make manifest-to-segment verification explicit
- page-level ordered keys and typed rows make checkpoint restore deterministic instead of trusting
  manifest summaries alone
- immutable CBOR+zstd keeps snapshot bulk data portable and append-only

Failure modes prevented:

- mutable or implementation-defined snapshot segment encodings
- manifest descriptors that cannot be checked against the segment body
- page-level corruption going undetected inside a durable checkpoint object

## Skeleton checkpoint writer

The first checkpoint writer path is:

1. take the current `HeadState` as the checkpoint basis
2. take the current canonical metadata families as the checkpoint basis rows
3. derive `checkpoint_seq = head.seq`
4. build immutable segment objects for each table family under that `checkpoint_seq`
5. copy each segment payload checksum and page checksum list into the manifest descriptors
6. write `manifest.json` with `verified = true` only after the segment objects are fully assembled

The current skeleton preserves:

- `active_fence_token`
- `next_inode_id`
- `retention_floor_seq`

Failure modes prevented:

- publishing a verified manifest before its segment objects exist
- losing allocator or retention state while building a checkpoint
- drifting between durable segment keys, typed segment rows, and manifest metadata

## Checkpoint publication via head CAS

After a checkpoint is fully verified, background work may CAS-update `head.json` to advertise it.

Checkpoint publication must:

1. read the latest `head.json`
2. require the checkpoint namespace to match the head namespace
3. require `checkpoint_seq <= head.seq`
4. preserve the current head values for:
   - `seq`
   - `active_fence_token`
   - `next_inode_id`
5. set `snapshot_hint_seq = max(current_snapshot_hint_seq, checkpoint_seq)`
6. optionally advance `retention_floor_seq` only when the requested floor:
   - is not below the current floor
   - is at or below `checkpoint_seq`
   - is covered by every required `progress.json` object loaded from `namespaces/{namespace_id}/derived/{work_class}/progress.json`
   - is covered by the retention-policy `progress.json` object
7. require every loaded progress object to:
   - decode as a `namespace_progress` control envelope
   - have a valid payload checksum
   - match the expected namespace and work class in both key and payload
8. skip the CAS if neither `snapshot_hint_seq` nor `retention_floor_seq` would change
9. encode the new head as a JSON `namespace_head` envelope and compare-and-swap `head.json`

Why these rules exist:

- checkpoint publication is summary maintenance, not an authoritative metadata commit
- `snapshot_hint_seq` must only move forward
- `retention_floor_seq` must move only when replay and derived-state promises still hold, as proven by durable progress objects

Failure modes prevented:

- background work rewriting the authoritative commit seq or writer generation
- regressing the published checkpoint hint
- advertising retention beyond what a verified checkpoint and durable progress objects can support
- unnecessary head CAS traffic that does not change any published promise
