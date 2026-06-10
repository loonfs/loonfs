# Versioning, Conformance, and Extensions

## 1. Versioning

A stable spec needs explicit versioning in three places.

| Layer | What is versioned |
| --- | --- |
| **Storage format** | Durable object envelopes and payload rules. |
| **Protocol binding** | HTTP or other transport shapes. |
| **Namespace naming rules** | `NamePolicy` and any future policy revisions. |

A new version should be introduced only when an old implementation could misread or misapply a new feature.

For the protocol binding, spec 060 §3.9 is the registry of stable error codes and their HTTP statuses, and the rule that clients must ignore unknown JSON response fields and tolerate unknown error codes.

The durable namespace descriptor and content-store descriptor are storage-format objects. The namespace descriptor is authoritative for the namespace-to-content-store relationship; a future catalog may index descriptors but must not replace their meaning.

### 1.1 Durable envelope layout

Every durable LoonFS object is an envelope document with the same leading fields, followed by the payload as an opaque sub-document:

| Field | Meaning |
| --- | --- |
| `kind` | snake_case object kind string. |
| `format_version` | Per-family format version (see table below). |
| `writer_version` | Informational `crate/<version>` of the writer. Never used for decode decisions. |
| `payload_checksum` | `sha256:<64 lowercase hex>` digest of the exact payload bytes as stored. |
| `payload` | The payload: a raw JSON sub-document in JSON families, a CBOR byte string in CBOR families. |

Two rules make these envelopes evolvable:

1. **Checksums cover stored bytes, never a re-encoding.** Readers verify `payload_checksum` against the payload bytes exactly as stored, before decoding them. A checksum failure therefore always means corruption; version skew can never be misreported as corruption.
2. **Readers probe before they decode.** Readers first decode only `kind` and `format_version`, so an object written with an unknown kind or an unsupported format version fails with a precise, typed error rather than a generic decode error.

### 1.2 Format families and versions

| Family | `kind` | Encoding | Current version |
| --- | --- | --- | --- |
| WAL segment | `namespace_wal_segment` | CBOR envelope, zstd-compressed; CBOR payload | 1 |
| Metadata SST | `metadata_sst` | CBOR envelope, zstd-compressed; CBOR payload | 1 |
| Namespace manifest | `namespace_manifest` | JSON, uncompressed | 1 |
| Control objects (head, lease, descriptors, fork state, GC pin, derived progress, upload session) | per-kind snake_case names | JSON, uncompressed | 1 (tracked per kind) |

JSON families keep their payload inline as raw JSON so manifests and control objects stay directly readable with generic tooling; CBOR families carry the payload as a byte string. Control-object versions are tracked per kind so one kind's payload schema can change without invalidating the others.

### 1.3 Evolution rules

- **Additive within a version.** A writer may add new payload fields without bumping `format_version`. Readers must ignore unknown payload and envelope fields. This is the only same-version change allowed.
- **Everything else bumps the version.** Renaming, removing, retyping, or re-tagging any field — or changing the payload encoding — requires bumping the owning family's `format_version`. Readers reject versions they do not support with a typed unsupported-version error; there is no silent fallback.
- **Digest strings are self-describing.** Durable digest values carry their algorithm as a prefix (`sha256:<hex>`) so a future algorithm can be introduced without re-interpreting old values. Commit fingerprints additionally carry their canonicalization scheme (`v0:sha256:<hex>`, spec 050 section 3.1) because their preimage rules can evolve independently of the algorithm.
- **Unknown content-ref kinds round-trip.** A reader that does not understand a `content_ref.kind` must preserve the original string when relaying or rewriting rows; it must not create new references with kinds it does not understand (see spec 050 section 1.3).
- **Every encoding is pinned by golden-byte fixtures** (`crates/loon-api/tests/golden_formats.rs`). An encoder change that alters durable bytes fails those tests; the failure message demands either reverting the change or bumping the format version and regenerating the fixtures.

## 2. Server requirements

A conforming server must:

1. treat object storage as the authoritative durable foundation;
2. publish visible metadata only through logical commits stored in visible WAL segments plus a successful head update;
3. validate that referenced content is already durable before publish;
4. preserve `(namespace_id, inode_id)` as canonical identity;
5. resolve namespace content through the immutable `content_store_id` in the namespace descriptor;
6. implement tombstone-first delete;
7. serve replay from the current verified manifest named by `head.current_manifest_id`, plus the visible WAL segment chain, replayed as logical commits; checkpoints pin manifest versions for retention, stable reads, restore, and forks;
8. honor the namespace's `NamePolicy`;
9. keep control-plane sessions and any implementation-specific coordinators out of namespace history and the change feed; and
10. preserve per-commit idempotency, ordering, and change-feed identity even when physically batching logical commits in a WAL segment.

## 3. Writer and client requirements

A conforming writer or client must:

1. treat paths as selectors, not as durable identity;
2. upload or otherwise stage content before asking the server to publish it;
3. use commit ids or equivalent idempotency keys for safe retry;
4. tolerate commit rejection when preconditions no longer hold; and
5. re-bootstrap if its cursor falls behind the retention floor.

A sync client must also maintain durable local state for its cursor and reconciliation logic.

## 4. Optional commit metadata

A commit may carry optional human or product metadata such as:

- a commit message;
- annotations or tags attached to the commit envelope;
- actor information; or
- workflow-correlation fields such as `operation_id`, `operation_kind`, or `operation_part`.

This metadata belongs to the logical commit, not to the resource itself.

## 5. Optional resource properties

A resource may carry optional structured properties such as display hints, application tags, or a resource-type hint.

These properties belong to the resource, not to the commit. They should move with the inode when the path changes.

## 6. Timestamps

The semantic creation marker in the core model is the create commit in namespace history, not a wall-clock field.

An implementation may expose wall-clock timestamps such as `committed_at` or `created_at`, but these are optional and non-semantic.

## 7. Hooks and downstream processing

The preferred extension point is the committed change feed. Downstream systems such as indexers, notification services, preview builders, or policy engines should consume committed changes rather than becoming part of the core mutation path.
