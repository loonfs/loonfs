# Object Store Contract

## 1. Purpose

LoonFS relies on object storage as its only required durable dependency. The object-store contract is therefore part of the core spec, not an implementation detail.

## 2. Required guarantees

A conforming object-store layer must provide the following behavior.

| Guarantee | Rationale |
| --- | --- |
| **Create-if-absent** for immutable objects | File content objects, WAL segments, and checkpoints must never be silently overwritten. |
| **Compare-and-swap update** for small mutable objects | The namespace head and similar control objects must be advanced safely in the presence of concurrent writers. |
| **Strong consistency** | A successful put/delete operation must become authoritative immediately after it succeeds. |
| **Prefix enumeration** | Checkpoint discovery, WAL segment discovery for repair and cleanup, and general namespace inspection need a reliable way to enumerate objects by prefix. |
| **Deterministic key scoping** | Providers must not allow objects outside the configured namespace or tenant prefix to leak into operations. |
| **Consistent error signaling for failed preconditions** | Higher layers need one generic way to detect stale writes and retry or fail safely. |

The spec deliberately avoids relying on multi-object transactions or provider-specific behavior that is not exposed through this contract.

## 3. Durable object families

The required durable object families and standard key patterns are:

| Family | Mutability | Purpose | Standard object key pattern |
| --- | --- | --- | --- |
| **Namespace descriptor** | Immutable | Record namespace identity and its immutable content-store relationship. | `namespaces/{namespace_id}/descriptor.json` |
| **Namespace head** | Mutable | Record the current visible boundary, replay hints, and visible WAL tip. | `namespaces/{namespace_id}/head.json` |
| **Namespace lease** | Mutable | Fence concurrent publishers when the deployment uses more than one possible writer. | `namespaces/{namespace_id}/lease.json` |
| **Content-store descriptor** | Immutable | Record content-store identity. | `content-stores/{content_store_id}/descriptor.json` |
| **Content objects** | Immutable | Store whole-file v0 bytes. | `content-stores/{content_store_id}/blobs/sha256/{hex[0..2]}/{hex[2..4]}/{hex}` |
| **WAL segments** | Immutable | Record one or more logical commits with a contiguous sequence range. | `namespaces/{namespace_id}/wal/{start_seq}-{end_seq}-{segment_id}.cbor.zst` |
| **Checkpoint manifest** | Immutable | Record the verified checkpoint summary and referenced materialization tables. | `namespaces/{namespace_id}/checkpoints/{checkpoint_seq}/manifest.json` |
| **Checkpoint base tables** | Immutable | Store a full verified metadata materialization through a base sequence. | `namespaces/{namespace_id}/checkpoints/{checkpoint_seq}/tables/{family}-{segment_index}.sst.zst` |
| **Checkpoint delta-run tables** | Immutable | Store WAL-derived metadata rows after a base materialization. | `namespaces/{namespace_id}/checkpoints/{checkpoint_seq}/delta-runs/{delta_run_id}/tables/{family}-{segment_index}.sst.zst` |

These key shapes are part of the interoperable storage contract. Implementations may add other control-plane objects.

## 4. Durable naming conventions

LoonFS uses distinct naming conventions for distinct surfaces:

- Fixed object-store path segments use lowercase words or lowercase-kebab, e.g. `content-stores`, `commit-receipts`, and `control/uploads`.
- Generated opaque IDs use underscore-prefixed tokens with 32 lowercase hex characters, e.g. `cs_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41`, `upl_4d8f2c91a7b34e0f9c6d1a2b3e5f708c`, and `seg_b7c14a0d9e6f42a38c5d21f0e8a739bc`.
- Durable work-class names use lowercase-kebab, e.g. `checkpoint-builder`.
- JSON enum values use snake_case.
- Namespace IDs are human/operator slugs. They use the durable slug grammar: 1-128 bytes, no leading or trailing whitespace, not `.` or `..`, first character lowercase ASCII letter or digit, and remaining characters lowercase ASCII letters, digits, `.`, `_`, or `-`. Prefer lowercase kebab-case within that grammar.

Namespace IDs are durable storage identities. A `namespace_id` must not be reused after namespace destruction; future user-facing display names or aliases may be reused only by mapping them to a new namespace ID.

Implementations must reject invalid namespace IDs before object-key construction. Durable objects that deserialize to invalid typed namespace IDs must be treated as invalid at load boundaries rather than coerced or normalized.

Generated runtime IDs must be unguessable, high-entropy, and never reused within the relevant namespace incarnation.

Underscores are reserved for generated opaque ID prefixes and JSON snake_case values. Fixed object-store path-family names should not use underscores.

## 5. WAL segment rules

The metadata log has five important rules.

1. A logical commit is the semantic record of one accepted client commit request.
2. A WAL segment stores one or more logical commits with contiguous `seq` values.
3. Distinct client commit requests remain distinct logical commits even when they are stored in the same WAL segment.
4. The visible WAL chain must be deterministically recoverable from the head plus referenced segment metadata. A head field such as `wal_tip_segment_id`, together with segment metadata such as `segment_id`, `start_seq`, `end_seq`, `base_head_seq`, and `prev_visible_segment_id`, is one conforming shape. Equivalent semantics are acceptable.
5. `segment_id` must be unique and never reused within a namespace incarnation. It should be generated from at least 128 bits of randomness or an equivalent collision-resistant source, not derived only from the sequence range.
6. Orphan WAL segments are permitted and harmless when a writer loses the head compare-and-swap.

## 6. Immutable content rules

The content model has four rules.

1. Content digests are content-derived, not provider-derived.
2. A `content_ref` describes one complete file revision.
3. Immutable content objects are written with create-if-absent semantics.
4. A metadata commit may reference a `content_ref` only after the referenced object is already durable.
5. When provider-verified full-object SHA-256 metadata is available, the object-store layer may expose it as `sha256:<64hex>`. If SHA-256 metadata is absent, readers and writers must fall back to reading and hashing bytes before treating a `content_ref` as verified.

In v0, file content is stored as one whole-file object whose `content_ref.kind` is `whole_file_v0`. The digest remains serialized as `sha256:<64hex>`, while the object key partitions the hex as `sha256/ab/cd/<hex>`.

ETags remain opaque compare tokens. They may be used for object freshness or compare-and-swap, but they are not content digests unless a provider-specific behavior is separately exposed and verified through this contract.

A reader or writer resolves content through the namespace descriptor: `namespace_id -> content_store_id -> content-stores/{content_store_id}/...`. File revisions and change-feed payloads store only `content_ref`; they do not store content-store ids or object-store paths.

## 7. Mutable control-object rules

Small mutable objects such as the namespace head or a lease must use compare-and-swap semantics. These objects must remain small enough that guarded rewrite is practical.

The live namespace lease object must not be physically deleted to represent ordinary expiry. Lease expiry is represented in the payload, and acquisition or transfer rewrites the existing `lease.json` object. In v0, the head `active_fence_token` and lease `fence_token` are the monotonic lease-epoch equivalent: each successful writer takeover advances the fence token, and a writer that observes a higher active fence token than its own must stop publishing.

Large immutable file data may use multipart upload or another provider-specific optimization. Small mutable control objects should not depend on those mechanisms.

## 8. Provider conformance

The spec standardizes the required behaviors, not a brand name such as "S3 compatible." A provider is conforming only when those behaviors are verified by conformance tests.

In practice:

- higher layers may depend on the LoonFS object-store contract;
- higher layers may not depend directly on provider headers, status codes, or SDK quirks.
