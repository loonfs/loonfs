# Object Store Contract

## 1. Purpose

LoonFS relies on object storage as its only required durable dependency. The object-store contract is therefore part of the core spec, not an implementation detail.

## 2. Required guarantees

A conforming object-store layer must provide the following behavior.

| Guarantee | Rationale |
| --- | --- |
| **Create-if-absent** for immutable objects | Content blocks, manifests, WAL segments, and checkpoints must never be silently overwritten. |
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
| **Namespace head** | Mutable | Record the current visible boundary, replay hints, and visible WAL tip. | `namespaces/{namespace_id}/head.json` |
| **Namespace lease** | Mutable | Fence concurrent publishers when the deployment uses more than one possible writer. | `namespaces/{namespace_id}/lease.json` |
| **Content blocks** | Immutable | Store file bytes. | `namespaces/{namespace_id}/blobs/{block_digest_sha256}` |
| **Content manifests** | Immutable | Describe file size, digest, block size, and the ordered block list. | `namespaces/{namespace_id}/manifests/{content_manifest_digest}.json` |
| **WAL segments** | Immutable | Record one or more logical commits with a contiguous sequence range. | `namespaces/{namespace_id}/wal/{start_seq}-{end_seq}-{segment_id}.cbor.zst` |
| **Checkpoint manifest** | Immutable | Record the verified checkpoint summary and referenced checkpoint data. | `namespaces/{namespace_id}/snapshots/{checkpoint_seq}/manifest.json` |
| **Checkpoint segments** | Immutable | Store verified checkpoint data. | `namespaces/{namespace_id}/snapshots/{checkpoint_seq}/tables/{family}-{segment_index}.sst.zst` |

These key shapes are part of the interoperable storage contract. Implementations may add other control-plane objects.

## 4. WAL segment rules

The metadata log has five important rules.

1. A logical commit is the semantic record of one accepted client commit request.
2. A WAL segment stores one or more logical commits with contiguous `seq` values.
3. Distinct client commit requests remain distinct logical commits even when they are stored in the same WAL segment.
4. The visible WAL chain must be deterministically recoverable from the head plus referenced segment metadata. A head field such as `wal_tip_segment_id`, together with segment metadata such as `segment_id`, `start_seq`, `end_seq`, `base_head_seq`, and `prev_visible_segment_id`, is one conforming shape. Equivalent semantics are acceptable.
5. Orphan WAL segments are permitted and harmless when a writer loses the head compare-and-swap.

## 5. Immutable content rules

The content model has four rules.

1. Block digests are content-derived, not provider-derived.
2. A content manifest describes one complete file revision.
3. Immutable content objects are written with create-if-absent semantics.
4. A metadata commit may reference a content manifest only after that manifest and all referenced blocks are already durable.

In v0, file content is stored as fixed-size 16 MiB blocks, except for the final block which may be smaller.

## 6. Mutable control-object rules

Small mutable objects such as the namespace head or a lease must use compare-and-swap semantics. These objects must remain small enough that guarded rewrite is practical.

Large immutable file data may use multipart upload or another provider-specific optimization. Small mutable control objects should not depend on those mechanisms.

## 7. Provider conformance

The spec standardizes the required behaviors, not a brand name such as "S3 compatible." A provider is conforming only when those behaviors are verified by conformance tests.

In practice:

- higher layers may depend on the LoonFS object-store contract;
- higher layers may not depend directly on provider headers, status codes, or SDK quirks.
