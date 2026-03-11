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

## Validation skeleton

Before writing a WAL object, the commit planner must at minimum validate:

1. the request namespace matches the current head and lease namespace
2. the planned head seq still matches the current head seq
3. the request carries the active fencing token
4. the lease holder still matches the requesting writer
5. the lease has not expired at the explicitly supplied validation time

The planner may leave deeper inode and name checks to later metadata lookups, but the checks above are mandatory before publish is attempted.

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

## Write ordering rule

The head CAS must not be attempted until the WAL object create-if-absent succeeds.

If the WAL object already exists with the same key, the write path may treat that as idempotent only after verifying that the stored envelope payload matches the commit it intended to publish.

Failure mode prevented:

- head state publishing a seq whose immutable WAL record was never durably created
