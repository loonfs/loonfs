# Spec 020: object-store contract

## Purpose

LoonDB is built on the idea that object storage is the only durable dependency.
That makes the object-store contract a first-class part of the product.

## Required primitives

### Create-if-absent

Plainly:
write an object only if it does not already exist.

Why it exists:
immutable content blocks and manifests should never overwrite each other.

Example:
upload `blobs/sha256/aa/bb/<digest>` with `If-None-Match: *`.

Failure mode prevented:
one writer silently replacing another writer’s supposedly immutable object.

### Compare-and-swap update

Plainly:
update a small mutable control object only if it still has the expected version or ETag.

Why it exists:
this is how we safely advance `head.json`, `lease.json`, and queue shards.

Example:
rewrite `namespaces/ns-1/head.json` with `If-Match: <old-etag>`.

Failure mode prevented:
lost updates from concurrent writers.

### Strong visibility after write and delete

Plainly:
if a write succeeds, later reads and lists must see it immediately.

Why it exists:
namespace publish logic assumes the head object becomes authoritative as soon as the CAS succeeds.

Failure mode prevented:
clients reading stale state after a successful publish.

## What we deliberately avoid

- multi-object transactions
- provider ETags as canonical content identity
- correctness assumptions based only on “S3 compatible” marketing

## S3 and R2 notes

Both AWS S3 and Cloudflare R2 expose strong consistency for reads, writes, deletes, and listings, and both expose S3-style conditional operations. We still treat those behaviors as **must verify by conformance**, because LoonDB depends on the details, not only on the headlines.

## Real-provider conformance rule

Provider expectations are provisional until the same conformance suite passes against:

- local FS
- AWS S3
- Cloudflare R2

Rule:

- real-provider credentials belong to the test harness, not production adapter code
- conformance tests may read explicit test-only environment variables
- production `ObjectStore` implementations must receive configuration explicitly, not through ambient process environment

Why this rule exists:

- test credentials and production configuration should not silently couple together
- provider validation should stay explicit and reviewable

Failure modes prevented:

- passing tests accidentally using a developer's ambient cloud credentials
- provider adapters gaining hidden runtime configuration paths that are hard to audit

## Control-plane rule

Head, lease, and queue objects must stay small enough for simple conditional writes.
Large immutable file content may use multipart upload.
Small mutable control objects should not.

## Initial durable key layout

The first object-store key builders should encode these stable families:

```text
namespaces/{namespace_id}/head.json
namespaces/{namespace_id}/lease.json
namespaces/{namespace_id}/wal/{seq:020}-{commit_id}.cbor.zst
namespaces/{namespace_id}/snapshots/{seq:020}/manifest.json
namespaces/{namespace_id}/snapshots/{seq:020}/tables/{family}-{segment_index:05}.sst.zst
namespaces/{namespace_id}/derived/{work_class}/progress.json
queue/shards/{shard_index:05}.json
```

Why this section exists:
engineers need one readable place that defines the object families the key builders are allowed to generate.

Failure mode prevented:
different crates silently inventing incompatible durable paths for the same logical object.
