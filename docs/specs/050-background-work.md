# Spec 050: background work

## What background work is for

Background work exists to build derived state after canonical metadata has already been committed.

It is for:

- snapshots
- change indices
- listing indices
- revision indices
- verification
- GC planning and sweep

It is not for:

- authoritative namespace commits
- required content uploads for the current request

## Queue design

We use sharded queue objects in object storage.

Each shard owns:

- broker lease state
- a set of queued jobs
- claim / heartbeat / timeout metadata

We do **not** use one global queue file.

Why not:
a single mutable object becomes a write hotspot.

## Job rule

Every job class must be idempotent.

Example:
`BuildSnapshot(namespace=abc, through_seq=420)` may run twice and still converge to the same durable outputs.

Failure mode prevented:
duplicate execution corrupting derived state.

## Lost enqueue rule

The queue is coordination, not truth.

If a post-commit enqueue is lost, repair logic must be able to recreate it by comparing namespace head seq to derived progress objects.

Failure mode prevented:
derived work permanently stalling because one enqueue was dropped.

## Progress object contract

Every derived work class publishes one small mutable `progress.json` object at:

```text
namespaces/{namespace_id}/derived/{work_class}/progress.json
```

The durable JSON envelope uses the same versioned control-object shape as `head.json` and
`lease.json`:

```json
{
  "kind": "namespace_progress",
  "format_version": 1,
  "writer_version": "loon-worker/0.1.0",
  "payload_checksum_sha256": "<sha256>",
  "state": {
    "namespace_id": "ns-1",
    "work_class": "BuildListingIndex",
    "through_seq": 420
  }
}
```

Rules:

- the object key must match `namespace_id` and `work_class`
- `through_seq` is monotonic and must only advance with CAS
- readers may trust derived outputs only when the corresponding `progress.json` proves coverage for the requested boundary

Why this shape exists:

- small control objects stay readable
- readers can validate progress objects against durable keys instead of ambient arguments
- checksum validation catches silent payload drift

Failure modes prevented:

- trusting derived outputs whose control object does not match the namespace or work class
- mutating progress state backward and silently regressing published coverage
- treating malformed progress JSON as authoritative

## Retention policy gate

Retention advancement also requires one durable progress object that represents the policy gate for the namespace.

The current skeleton stores that policy gate in the same `progress.json` family, under a dedicated
work class such as `RetentionPolicy`.

Rule:

- `retention_floor_seq` may advance only when the retention-policy progress object's `through_seq` is at or above the requested floor

Failure mode prevented:

- dropping incremental replay before policy has actually authorized it
