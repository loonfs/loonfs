# Object-store contract

LoonFS assumes that object storage is the only durable dependency. That makes the storage contract a first-class part of the system.

## Required operations

A conforming object-store adapter must provide only a small set of primitives:

| Primitive | Purpose |
| --- | --- |
| `head` | Read object metadata, including an opaque compare token such as an ETag. |
| `get` | Read the full object body. |
| `put if absent` | Create an immutable object only when it does not already exist. |
| `put if match` | Compare-and-swap update for a small mutable control object. |
| `delete` | Remove an object. Deleting a missing key is idempotent. |
| `list_prefix` | List objects under a prefix. The adapter must return results in sorted order. |

Higher layers must not depend on provider-specific HTTP codes, headers, SDK error strings, or multipart-upload details.

## Required semantics

| Requirement | Meaning |
| --- | --- |
| **Create-if-absent** | Immutable objects such as blocks, manifests, WAL entries, and checkpoint segments must never be overwritten silently. |
| **Compare-and-swap** | Small mutable control objects such as `head.json`, `lease.json`, queue shards, and progress objects must reject stale compare tokens. |
| **Strong visibility** | If a write or delete succeeds, later reads and prefix listings must observe the latest visible state. |
| **Prefix isolation** | A scoped adapter must never leak keys outside its configured namespace or root prefix. |
| **Consistent key validation** | Invalid keys and traversal attempts must be rejected consistently across read, write, delete, and list operations. |

## Mutable and immutable object families

| Family | Mutability | Typical key |
| --- | --- | --- |
| Namespace head | Mutable | `namespaces/{ns}/head.json` |
| Namespace lease | Mutable | `namespaces/{ns}/lease.json` |
| Content blocks | Immutable | `namespaces/{ns}/blobs/{block_digest}` |
| Content manifests | Immutable | `namespaces/{ns}/manifests/{manifest_digest}.json` |
| WAL entries | Immutable | `namespaces/{ns}/wal/{seq}-{commit_id}.cbor.zst` |
| Checkpoint manifest | Immutable | `namespaces/{ns}/snapshots/{seq}/manifest.json` |
| Checkpoint segments | Immutable | `namespaces/{ns}/snapshots/{seq}/tables/{family}-{index}.sst.zst` |
| Derived progress | Mutable | `namespaces/{ns}/derived/{work_class}/progress.json` |
| Queue shards | Mutable | `queue/shards/{shard_id}.json` |
| Conflict artifacts | Immutable | `namespaces/{ns}/conflicts/{conflict_id}.json` |
| Conflict-archive sidecars | Mutable | `namespaces/{ns}/conflict-archives/{conflict_id}.json` |

## Content object rules

For v1, file content uses these fixed rules:

- blocks are fixed at `16 MiB` of plaintext, except the final block may be shorter
- canonical block identity is `sha256:<hex>` of the plaintext bytes
- the content manifest is deterministic JSON
- the manifest digest is `sha256:<hex>` of the canonical manifest bytes
- deduplication is namespace-scoped

If an immutable content object already exists, an implementation may reuse it only after verifying that the bytes are exactly the bytes implied by that object’s identity.

## Conformance rule

The object-store contract is not satisfied by a marketing claim such as “S3 compatible.”

A provider profile is considered safe only after the same conformance suite passes against at least:

- a local filesystem adapter
- AWS S3
- Cloudflare R2

Additional providers are welcome, but they must pass the same contract before higher layers rely on them.
