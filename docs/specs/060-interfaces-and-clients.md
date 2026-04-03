# Interfaces and Clients

## 1. Two public operation surfaces

LoonFS has two peer public surfaces.

| Surface | Purpose | Shape |
| --- | --- | --- |
| **Filesystem operations** | User-facing filesystem actions | Path-oriented commands such as `ls`, `stat`, `get`, `put`, `mkdir`, `mv`, `cp`, and `rm`. |
| **Upload, commit, and change feed** | Explicit staging, publication, and incremental consumption | Explicit content staging, explicit commit, and incremental change consumption. |

The filesystem surface exists because user intent is naturally path-based. The upload-and-commit surface exists because long-running and stateful clients need explicit control over staging, validation, and replay.

A single client may use or expose both surfaces.

Both surfaces share the same namespace, inode, content, and visibility rules.

## 2. Minimal upload, commit, and change-feed model

The lower-level writer surface has three stages:

1. make content durable
2. commit metadata visibility
3. observe ordered changes through the change feed

This split is deliberate:

- content durability is not visibility;
- WAL durability is not visibility;
- head advance is the visibility point.

### 2.1 Commit request envelope

A commit request carries the following logical fields:

| Field | Meaning |
| --- | --- |
| `request_id` | Client-generated stable idempotency key for this logical commit request. The same value must be reused for safe retries. |
| `planned_head_seq` | The history point the client planned against. Preconditions are evaluated against authoritative state at this boundary. |
| `preconditions` | Explicit checks such as `HeadSeqIs`, `InodeRevisionIs`, or ancestor-visibility checks that make races fail explicitly rather than silently merge. |
| `ops` | Ordered list of mutation operations. Operation order is preserved through validation, commit, and change-feed output. |
| `message` | Optional human-readable description of the mutation event. |
| `annotations` | Optional structured metadata attached to the commit request. |

The server validates that request against authoritative namespace state, writes one immutable WAL
entry, advances the head with compare-and-swap, and returns success only after the head update
succeeds.

The change feed returns ordered committed changes after an explicit cursor. If the requested cursor
is older than the retention floor, the caller must re-bootstrap instead of expecting older
incremental history to remain available.

The path-oriented filesystem surface may compile higher-level operations into this lower-level
model, but both surfaces preserve the same identity, content-durability, and visibility rules.

## 3. Representative HTTP binding

HTTP is one transport binding for these abstract operations. It is not the underlying semantics.

A representative v0 binding is shown below.

| Purpose | Representative HTTP shape |
| --- | --- |
| Stat a path | `GET /v0/namespaces/{ns}/filesystem/stat?path=/docs/report.txt` |
| List a path | `GET /v0/namespaces/{ns}/filesystem/list?path=/docs` |
| Read file content | `GET /v0/namespaces/{ns}/filesystem/content?path=/docs/report.txt` |
| Apply path-oriented operations | `POST /v0/namespaces/{ns}/filesystem/operations` |
| Begin or prepare upload | `POST /v0/namespaces/{ns}/uploads` |
| Publish an explicit commit | `POST /v0/namespaces/{ns}/commits` |
| Read committed changes | `GET /v0/namespaces/{ns}/changes?after_seq=123` |

Long-running transfers may additionally expose session or job resources. The exact endpoint set is less important than the semantic rule: once a long-running operation begins, the server-issued session or job id becomes the stable in-flight identifier of that operation.

A few representative requests and responses are shown below. These examples are illustrative, not exhaustive.

### 3.1 `GET /filesystem/stat`

```json
{
  "namespace_id": "demo",
  "absolute_path": "/docs/report.txt",
  "inode_id": 42,
  "inode_kind": "FILE",
  "head_seq": 418,
  "revision_no": 7,
  "size_bytes": 19482,
  "content_manifest_digest": "sha256:report-v7"
}
```

### 3.2 `GET /filesystem/list`

```json
{
  "namespace_id": "demo",
  "absolute_path": "/docs",
  "head_seq": 418,
  "entries": [
    {
      "display_name": "report.txt",
      "absolute_path": "/docs/report.txt",
      "inode_id": 42,
      "inode_kind": "FILE"
    },
    {
      "display_name": "slides",
      "absolute_path": "/docs/slides",
      "inode_id": 43,
      "inode_kind": "DIR"
    }
  ]
}
```

### 3.3 `GET /filesystem/content`

The response body is the authoritative file bytes. Metadata may be exposed in headers, but the
body itself is raw content rather than JSON.

### 3.4 `POST /filesystem/operations`

Representative request:

```json
{
  "request_id": "req_01J...",
  "message": "move report and publish new bytes",
  "annotations": {
    "source": "cli"
  },
  "ops": [
    {
      "op": "put",
      "path": "/docs/report.txt",
      "content_manifest_digest": "sha256:report-v8"
    },
    {
      "op": "mv",
      "source_path": "/docs/report.txt",
      "destination_path": "/reports/report.txt"
    }
  ]
}
```

Representative response:

```json
{
  "namespace_id": "demo",
  "committed_seq": 419,
  "results": [
    {
      "op_index": 0,
      "inode_id": 42,
      "revision_no": 8
    },
    {
      "op_index": 1,
      "inode_id": 42,
      "absolute_path": "/reports/report.txt"
    }
  ]
}
```

### 3.5 `POST /uploads`

The exact upload exchange may vary more than the other examples on this page. Implementations may
use delegated upload, service-proxied upload, or another equivalent staged-upload flow, as long as
the returned upload state is sufficient to make content durable before commit.

Representative response:

```json
{
  "upload_id": "upl_01J...",
  "block_size_bytes": 16777216,
  "mode": "delegated"
}
```

### 3.6 `POST /commits`

Representative request:

```json
{
  "request_id": "req_01J...",
  "planned_head_seq": 418,
  "message": "replace report bytes",
  "annotations": {
    "source": "sync"
  },
  "preconditions": [
    {
      "type": "HeadSeqIs",
      "expected_seq": 418
    },
    {
      "type": "InodeRevisionIs",
      "inode_id": 42,
      "revision_no": 7
    },
    {
      "type": "AncestorsNotSubtreeDeleted",
      "inode_id": 42
    }
  ],
  "ops": [
    {
      "op": "replace_file",
      "inode_id": 42,
      "base_revision_no": 7,
      "content_manifest_digest": "sha256:report-v8"
    }
  ]
}
```

Representative response:

```json
{
  "namespace_id": "demo",
  "commit_id": "c_01J...",
  "committed_seq": 419,
  "results": [
    {
      "op_index": 0,
      "inode_id": 42,
      "revision_no": 8
    }
  ]
}
```

### 3.7 `GET /changes`

```json
{
  "namespace_id": "demo",
  "from_exclusive_seq": 418,
  "through_seq": 420,
  "changes": [
    {
      "seq": 419,
      "commit_id": "c_01J...",
      "request_id": "req_01J...",
      "message": "replace report bytes",
      "ops": [
        {
          "op_index": 0,
          "op": "replace_file",
          "inode_id": 42,
          "revision_no": 8
        }
      ]
    }
  ]
}
```

## 4. Client profiles

These profiles are defined by the surface a client uses, not by whether the implementation is a
CLI, desktop app, web app, SDK, or service. A single client may implement more than one profile.

### 4.1 Path-oriented client

This client uses the path-oriented surface.

Typical behavior:

- `ls`, `stat`, `get`, `put`, `mv`, and `cp` use user-visible paths;
- the server remains authoritative for path resolution, canonical inode identity, and commit validation;
- small commands are often sessionless;
- large or recursive commands may use server-side sessions or jobs.

This client does not require a sync database or full local mirror to be a first-class client.
Implementations may still keep durable local state such as auth/session state, retry journals,
pinned snapshot ids, or inode context learned from prior responses when that improves usability,
restart safety, or resumability.

### 4.2 Sync client

This client maintains durable local state and consumes the change feed over time.

Typical behavior:

- maintains a durable cursor;
- projects remote state into local state;
- may upload content and publish explicit commits;
- preserves conflicts according to the client's conflict policy.

### 4.3 Explicit-commit client

This client uses the upload, commit, and change-feed surface more directly. It stages content and
publishes explicit commits, but it does not necessarily maintain a long-lived local mirror.

Typical behavior:

- content hashing and upload;
- explicit commit with preconditions and request ids;
- change-feed reads or cursors where incremental observation is needed.

### 4.4 Operator or admin tool

This client uses low-level recovery or inspection surfaces that are specific to an implementation or deployment.

## 5. Statefulness summary

The table below is intentionally short. It captures the core split without turning stateful transfers into the main story of the spec.
For more detailed command-oriented guidance, see [Appendix 095: Operation Statefulness Matrix](../appendices/095-operation-statefulness-matrix.md).

| Operation | Usual shape | Typical server-side state |
| --- | --- | --- |
| `get <file>` | One request | None after the request completes. |
| `get -r <dir>` | Multi-request snapshot read | A read session may pin a consistent snapshot. |
| `put <file>` | One request for small files; staged upload for large files | A put intent and upload session may bind the destination and upload. |
| `put -r <dir>` | Import-style operation | An import job may coordinate a large tree upload. |
| `cp <file>` on one service | One request | Usually none after the request completes. |
| `cp -r <dir>` on one service | Server-side job | A copy job may coordinate traversal and publication. |

## 6. Client and server responsibilities

| Concern | Server | Client |
| --- | --- | --- |
| Path resolution | Authoritative | Supplies user intent by path when using the filesystem surface. |
| Content hashing and upload | May accept direct bytes, proxy uploads, or issue upload capabilities, but must verify that any content referenced by a commit is already durable. | Usually responsible for reading local bytes, computing content hashes, and uploading missing content when originating new data. |
| Commit validation | Authoritative | Supplies preconditions and request ids where needed. |
| Namespace visibility | Authoritative | Observes committed results. |
| Long-running transfer progress | Authoritative for sessions or jobs that affect correctness | Responsible for local temp files, local progress, and retry behavior. |
