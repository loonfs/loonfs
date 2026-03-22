# Spec 020: object-store contract

## Purpose

LoonDB is built on the idea that object storage is the only durable dependency.
That makes the object-store contract a first-class part of the product.

## Required primitives

The active v1 provider-facing surface is intentionally small. Higher layers may depend only on:

- `head`
- `get`
- `put`
- `delete`
- `list_prefix`
- `ObjectMetadata.etag` as an opaque compare token
- `ObjectStoreError` variants

Anything else is provider-specific detail and must terminate inside `loon-objectstore`.

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

### Additional active v1 behaviors

Higher layers now also depend on these trait-level behaviors:

- overwrite publishes the new bytes immediately
- `head` reflects the latest visible version after overwrite and returns `None` after delete
- deleting a missing key is idempotent
- compare-and-swap on a missing object fails as `PreconditionFailed`
- stale compare tokens stay rejected after overwrite
- `list_prefix` is returned sorted at the trait boundary
- invalid keys and traversal attempts are rejected consistently across `head`, `get`, `put`,
  `delete`, and `list_prefix`
- scoped providers must never leak keys outside their configured prefix

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
- real-provider conformance should be a required delivery gate for object-store contract changes

Failure modes prevented:

- passing tests accidentally using a developer's ambient cloud credentials
- provider adapters gaining hidden runtime configuration paths that are hard to audit
- merging object-store contract changes that were only proven against local FS

## Provider-profile rule

Provider profiles must separate:

- the active `ObjectStore` contract that higher layers may rely on today
- future provider capabilities that are informative but not yet correctness dependencies

Example:

- create-if-absent and compare-and-swap belong in the active contract profile
- multipart-upload support belongs in a future-capability profile until the trait and conformance
  suite make it part of the active surface

Failure mode prevented:
provider-profile metadata drifting into a misleading mix of “safe to depend on now” and “maybe
useful later” fields.

## Boundary isolation rule

Provider quirks must stop at the `ObjectStore` trait boundary.

Rules:

- higher layers may branch on `ObjectStoreError::PreconditionFailed` as the generic stale-write
  signal
- higher layers must not branch on provider-specific HTTP status codes, header names, SDK error
  strings, or transport text
- `ObjectMetadata.etag` is an opaque compare token for one object version; it is not canonical
  content identity, not a durable protocol field, and not valid for cross-object or cross-provider
  reasoning
- key prefixing, path-style quirks, conditional-header construction, and provider-specific retry
  details must stay inside `loon-objectstore`
- any new provider capability must first appear as an `ObjectStore`-level contract plus a
  conformance case before other crates may depend on it

Why these rules exist:

- the repo should have one place where cloud-provider behavior is interpreted
- higher layers should talk in terms of LoonDB concurrency and durability semantics, not S3 header
  mechanics
- opaque compare tokens are useful for CAS without becoming accidental durable identity

Failure modes prevented:

- `loon-core`, `loon-queue`, or `loon-client` growing hidden dependencies on one provider's HTTP
  behavior
- ETags being mistaken for canonical content digests or stable protocol fields
- future provider swaps requiring changes in semantic crates instead of only in `loon-objectstore`

## Control-plane rule

Head, lease, and queue objects must stay small enough for simple conditional writes.
Large immutable file content may use multipart upload.
Small mutable control objects should not.

Multipart upload is therefore not part of the active v1 `ObjectStore` trait surface. If large
immutable blob upload later requires explicit multipart semantics in this repository, that change
must update this spec, the trait surface, and the conformance suite together before higher layers
depend on it.

## Immutable content objects

File content is stored as immutable per-namespace block objects plus one immutable
content manifest object.

The durable object families are:

```text
namespaces/{namespace_id}/blobs/{block_digest_sha256}
namespaces/{namespace_id}/manifests/{content_manifest_digest}.json
```

Rules:

- block digests use `sha256:<hex>` over plaintext block bytes
- the v1 block size is fixed at `16 MiB`, except the final block may be shorter
- block object bodies are the raw plaintext block bytes
- the manifest body is deterministic JSON `ContentManifestEnvelope`
- `content_manifest_digest` is `sha256:<hex>` of the canonical manifest JSON bytes
- the manifest payload must carry `namespace_id`, `file_size_bytes`, `file_digest_sha256`,
  `block_size_bytes`, and the ordered list of block digests and block sizes
- immutable content writes use create-if-absent; if an object already exists, the writer must
  verify byte-for-byte equality before reusing it

Why these rules exist:

- `create_file` must be able to point at durable immutable content
- identical content must not depend on provider ETags or uploader identity
- future readers must be able to validate a manifest against the exact bytes it names

Failure modes prevented:

- metadata publishing before file bytes are durable
- two uploaders writing different bytes under the same content-addressed key
- content manifests becoming uploader-version-specific even when file content is identical

Milestone 8 executable-invariant rollout now evaluates the file-content object IDs for this family
directly in the harnesses:

- `content_manifest_checksum_matches_payload`
- `content_manifest_digest_matches_object`
- `content_manifest_namespace_matches_request`
- `content_manifest_blocks_match_descriptors`
- `content_manifest_file_digest_matches_blocks`

Milestone 8 slice 4 now evaluates the checkpoint immutable-object IDs for this family directly in
the harnesses:

- `checkpoint_segment_payload_checksum_matches_payload`
- `checkpoint_segment_key_matches_family_and_index`
- `verified_checkpoint_manifest_requires_durable_segments`
- `checkpoint_manifest_preserves_head_summary`
- `checkpoint_manifest_preserves_basis_metadata`

The first client-side invariant slice still remains later and file-transfer-only.

## Initial durable key layout

The first object-store key builders should encode these stable families:

```text
namespaces/{namespace_id}/head.json
namespaces/{namespace_id}/lease.json
namespaces/{namespace_id}/blobs/{block_digest_sha256}
namespaces/{namespace_id}/manifests/{content_manifest_digest}.json
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
