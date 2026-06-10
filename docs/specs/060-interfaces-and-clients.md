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
2. make metadata visible
3. observe ordered changes through the change feed

This split is deliberate:

- content durability is not visibility;
- WAL-segment durability is not visibility by itself; and
- head advance is the visibility point.

A commit request may therefore be rejected immediately, or tentatively accepted into a WAL batch, without yet being a committed or successful change.

### 2.1 Commit request envelope

A commit request carries the following logical fields:

| Field | Meaning |
| --- | --- |
| `commit_id` | Client-generated stable idempotency key for this logical commit request. The same value must be reused for safe retries. |
| `preconditions` | Explicit semantic checks such as `inode_revision_is`, `binding_is`, `child_name_absent`, `directory_empty`, or ancestor-visibility checks that make races fail explicitly rather than silently merge. |
| `ops` | Ordered list of mutation operations. Operation order is preserved through validation, logical commit creation, and change-feed output. |
| `message` | Optional human-readable description of the mutation event. |
| `annotations` | Optional structured metadata attached to the logical commit request. |

The server validates each request against authoritative namespace state. A request may be rejected immediately. If it is tentatively accepted into a publication batch, the server may assign it a `seq`, but the request is not yet committed or successful at that point. It becomes one committed logical commit only after its WAL segment is durably written and the head update succeeds. If the WAL segment is written but the head update fails, the segment is orphaned and the request is not committed.

The server may publish multiple committed logical commits in one WAL segment and one head update, but it must preserve per-commit idempotency, ordering, and change-feed identity.

Annotations may be used to correlate multiple logical commits that belong to one higher-level workflow, for example with fields such as `operation_id`, `operation_kind`, or `operation_part`.

The change feed returns ordered committed changes after an explicit cursor. If the requested cursor is older than the retention floor, the caller must re-bootstrap instead of expecting older incremental history to remain available.

The standard lower-level mutation set is defined in the mutation and visibility model. The path-oriented filesystem surface may compile higher-level operations into that lower-level model, but both surfaces preserve the same identity, content-durability, and visibility rules.

## 3. Representative HTTP binding

HTTP is one transport binding for these abstract operations. It is not the underlying semantics.

A representative v0 binding is shown below.

| Purpose | Representative HTTP shape |
| --- | --- |
| Create a namespace | `POST /v0/namespaces` |
| Read one namespace's status | `GET /v0/namespaces/{ns}` |
| Stat a path | `GET /v0/namespaces/{ns}/filesystem/stat?path=/docs/report.txt` |
| List a path | `GET /v0/namespaces/{ns}/filesystem/list?path=/docs` |
| List file revisions by path | `GET /v0/namespaces/{ns}/filesystem/revisions?path=/docs/report.txt` |
| Read file content | `GET /v0/namespaces/{ns}/filesystem/content?path=/docs/report.txt` |
| Read prior file content by path | `GET /v0/namespaces/{ns}/filesystem/content?path=/docs/report.txt&revision_no=3` |
| List file revisions by inode | `GET /v0/namespaces/{ns}/inodes/{inode_id}/revisions` |
| Read prior file content by inode | `GET /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{revision_no}/content` |
| Apply path-oriented operations | `POST /v0/namespaces/{ns}/filesystem/operations` |
| Begin or prepare upload | `POST /v0/namespaces/{ns}/uploads` |
| Upload full staged content | `PUT /v0/namespaces/{ns}/uploads/{upload_id}/content` |
| Complete staged upload | `POST /v0/namespaces/{ns}/uploads/{upload_id}/complete` |
| Submit an explicit commit request | `POST /v0/namespaces/{ns}/commits` |
| Read committed changes | `GET /v0/namespaces/{ns}/changes?after_seq=123` |
| Fork a namespace | `POST /v0/namespaces/{source_ns}/forks` |
| Create a checkpoint | `POST /v0/admin/namespaces/{ns}/checkpoint` |

Long-running transfers may additionally expose session resources. Implementations may also expose workflow helper resources, but those helpers are outside the core semantics. Once a multi-request interaction begins, the server-issued identifier is the stable in-flight identifier of that interaction.

Namespace creation uses the namespace id directly. v0 has no namespace aliases or separate display names:

```json
{
  "namespace_id": "demo"
}
```

The field name `namespace_id` is intentional API compatibility surface. Fork creation uses `new_namespace_id` for the target namespace. Route placeholders such as `{ns}`, `{source_ns}`, or an implementation-internal `:namespace` are only path parameter names for the same namespace id value; v0 does not accept or emit a namespace `name` alias.

A few representative requests and responses are shown below. These examples are illustrative, not exhaustive. Responses may gain fields within v0; clients must ignore JSON fields they do not recognize.

### 3.0 `GET /v0/namespaces/{ns}`

The namespace status read answers "does this namespace exist, and where is its head?" without listing every namespace. A missing namespace is `404` with code `namespace_not_found`.

```json
{
  "namespace_id": "demo",
  "head_seq": 418,
  "current_manifest_id": 410,
  "latest_checkpoint_id": "chk_f3d6b97a7d394ddf84b621ddf36e5071",
  "wal_tail_segments": 3,
  "retention_floor_seq": 120
}
```

### 3.1 `GET /filesystem/stat`

The response is one authoritative path entry. Enum values are snake_case per the durable naming rules (spec 030 section 4).

```json
{
  "namespace_id": "demo",
  "absolute_path": "/docs/report.txt",
  "inode_id": 42,
  "inode_kind": "file",
  "head_seq": 418,
  "parent_inode_id": 7,
  "display_name": "report.txt",
  "revision_no": 7,
  "size_bytes": 19482,
  "content_ref": {
    "kind": "whole_file_v0",
    "digest": "sha256:42d...",
    "size_bytes": 19482
  }
}
```

### 3.2 `GET /filesystem/list`

The envelope names the listed path and the head the listing was read from, so an empty directory still reports which state it observed and the response can grow (for example, pagination cursors) without reshaping `entries`. Entries are full path entries with the same shape as `stat` (directory entries leave the file-only fields out).

```json
{
  "namespace_id": "demo",
  "absolute_path": "/docs",
  "head_seq": 418,
  "entries": [
    {
      "namespace_id": "demo",
      "absolute_path": "/docs/report.txt",
      "inode_id": 42,
      "inode_kind": "file",
      "head_seq": 418,
      "parent_inode_id": 7,
      "display_name": "report.txt",
      "revision_no": 7,
      "size_bytes": 19482,
      "content_ref": {
        "kind": "whole_file_v0",
        "digest": "sha256:42d...",
        "size_bytes": 19482
      }
    },
    {
      "namespace_id": "demo",
      "absolute_path": "/docs/slides",
      "inode_id": 43,
      "inode_kind": "dir",
      "head_seq": 418,
      "parent_inode_id": 7,
      "display_name": "slides"
    }
  ]
}
```

### 3.3 `GET /filesystem/content`

The response body is the authoritative file bytes. Metadata may be exposed in headers, but the body itself is raw content rather than JSON.

### 3.4 `POST /filesystem/operations`

Representative request:

```json
{
  "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
  "operation": {
    "op": "move_path",
    "from_path": "/docs/report.txt",
    "to_path": "/reports/report.txt",
    "mode": "no_replace"
  }
}
```

A successful response is returned only after the underlying change is actually committed: the WAL segment is durable and the head has advanced.

Representative response:

```json
{
  "namespace_id": "demo",
  "committed_seq": 419
}
```

The same endpoint also accepts path directory creation:

```json
{
  "commit_id": "c_8b7d4ef098ec4c1fbde15edbe02f9a64",
  "operation": {
    "op": "create_dir",
    "path": "/docs"
  }
}
```

and path revision restore:

```json
{
  "commit_id": "c_8f9a1b2c3d4e4f50a6b7c8d9e0f12345",
  "operation": {
    "op": "restore_revision",
    "path": "/docs/report.txt",
    "source_revision_no": 3
  }
}
```

Inode-based restore is available when a caller already has stable inode identity and the expected
current base revision:

`POST /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{source_revision_no}/restore`

```json
{
  "commit_id": "c_271e8c2b45a04e5da6a7e8d9f0012345",
  "base_revision_no": 7
}
```

### 3.5 Upload transport

The upload transport standardizes staged content publication, not one specific byte path. In v0, uploads are whole-file uploads: the staged body is the complete file content, not a separate metadata document or multipart strategy.

The semantic rule is:

- `PUT /content` stores the immutable whole-file object and records the staged `content_ref`;
- `complete` finalizes the upload session only when the expected `content_ref` exactly matches the service-computed staged ref; and
- the returned `content_ref` is then safe to reference from a commit.

Repeating `PUT /content` with the same bytes for the same upload id is idempotent. Repeating it with different bytes is a conflict. Completing an upload fails if no content was staged or if the expected `content_ref` differs from the staged one. Generic metadata publication still applies the durable content validation rules from the write protocol before arbitrary `content_ref`s become visible.

Representative begin-upload response:

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_4d8f2c91a7b34e0f9c6d1a2b3e5f708c",
  "mode": "service_proxied"
}
```

Representative content-upload response:

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_4d8f2c91a7b34e0f9c6d1a2b3e5f708c",
  "content_ref": {
    "kind": "whole_file_v0",
    "digest": "sha256:7ab...",
    "size_bytes": 20591
  }
}
```

Representative complete-upload request:

```json
{
  "content_ref": {
    "kind": "whole_file_v0",
    "digest": "sha256:7ab...",
    "size_bytes": 20591
  }
}
```

Representative complete-upload response:

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_4d8f2c91a7b34e0f9c6d1a2b3e5f708c",
  "content_ref": {
    "kind": "whole_file_v0",
    "digest": "sha256:7ab...",
    "size_bytes": 20591
  }
}
```

### 3.6 `POST /commits`

Representative request:

```json
{
  "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
  "message": "replace report bytes",
  "annotations": {
    "source": "sync",
    "operation_id": "op_report_refresh_01"
  },
  "preconditions": [
    {
      "type": "inode_revision_is",
      "inode_id": 42,
      "revision_no": 7
    },
    {
      "type": "ancestors_not_subtree_deleted",
      "inode_id": 42
    }
  ],
  "ops": [
    {
      "op": "replace_file",
      "inode_id": 42,
      "base_revision_no": 7,
      "content_ref": {
        "kind": "whole_file_v0",
        "digest": "sha256:7ab...",
        "size_bytes": 20591
      }
    }
  ]
}
```

A request may be rejected immediately. A successful response is returned only after the request is actually committed: the WAL segment is durably stored and the head has been updated to reference it.

Representative response:

```json
{
  "namespace_id": "demo",
  "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
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
      "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
      "message": "replace report bytes",
      "ops": [
        {
          "op_index": 0,
          "op": "replace_file",
          "inode_id": 42,
          "revision_no": 8,
          "content_ref": {
            "kind": "whole_file_v0",
            "digest": "sha256:7ab...",
            "size_bytes": 20591
          }
        }
      ],
      "deltas": [
        {
          "semantic_op_index": 0,
          "delta_index": 0,
          "delta": "append_file_revision",
          "inode_id": 42,
          "revision_no": 8,
          "content_ref": {
            "kind": "whole_file_v0",
            "digest": "sha256:7ab...",
            "size_bytes": 20591
          }
        }
      ]
    }
  ]
}
```

### 3.8 `POST /forks`

Representative request:

```json
{
  "new_namespace_id": "demo-branch"
}
```

Representative response:

```json
{
  "namespace_id": "demo-branch"
}
```

The server forks from the source namespace's current head. The new namespace shares the source namespace's content store and starts with independent future namespace metadata. The fork records provenance and a source-side GC pin for the source checkpoint/manifest pair so source-owned immutable metadata files remain available while the target manifest references them.

### 3.9 Error responses

Every error response is a JSON body with two fields:

```json
{
  "code": "path_not_found",
  "message": "path not found `/docs/missing.txt`"
}
```

`code` is the stable machine contract; `message` is human-readable and may change between releases. Clients must branch on `code`, must tolerate codes they do not recognize, and must not parse `message`. The registry (`ErrorCode` in `loon-api`) is:

| Code | HTTP status | Meaning |
| --- | --- | --- |
| `invalid_path` | 400 | The supplied path is not a valid absolute path. |
| `invalid_namespace_id` | 400 | The namespace id violates the slug grammar (spec 030 section 4). |
| `invalid_commit_id` | 400 | The commit id violates the id grammar. |
| `invalid_upload_id` | 400 | The upload id violates the generated-id grammar. |
| `invalid_inode_id` | 400 | The inode id path parameter is not a valid integer id. |
| `invalid_revision_no` | 400 | The revision number parameter is not a valid integer. |
| `invalid_config` | 400 | The request or server configuration is invalid. |
| `unauthorized` | 401 | Missing or wrong credentials. |
| `namespace_not_found` | 404 | The namespace does not exist. |
| `path_not_found` | 404 | No visible entry at the path. |
| `revision_not_found` | 404 | The file has no such revision. |
| `upload_not_found` | 404 | No upload session with this id. |
| `namespace_exists` | 409 | The create target already exists. |
| `namespace_partial` | 409 | The namespace is partially initialized and unusable. |
| `path_conflict` | 409 | The destination path is already bound. |
| `directory_not_empty` | 409 | The directory has children and the operation is not recursive. |
| `stale_head` | 409 | The write raced a head advance; retry against fresh state. |
| `stale_revision` | 409 | A caller-supplied base revision is no longer current. |
| `tombstone_conflict` | 409 | The path is covered by a subtree tombstone. |
| `lease_conflict` | 409 | Another writer holds the namespace lease. |
| `would_cycle` | 409 | The rename would create a directory cycle. |
| `unsupported_rename_mode` | 400 | The requested rename mode is not supported. |
| `commit_id_reuse_conflict` | 409 | The commit id was reused with different content. |
| `upload_already_completed` | 409 | The upload session is already completed. |
| `upload_content_conflict` | 409 | Different bytes were staged under this upload id. |
| `invalid_upload_content` | 400 | The staged or referenced content is invalid. |
| `rebootstrap_required` | 409 | The change cursor is older than the retention floor. |
| `commit_queue_full` | 503 | The namespace write queue is full; back off and retry. |
| `checkpoint_unavailable` | 503 | Required checkpoint state is not yet available; retry. |
| `bootstrap_failed` | 500 | Namespace bootstrap failed internally. |
| `namespace_corrupt` | 500 | Durable namespace state failed validation. |
| `server_error` | 500 | Unclassified internal failure. |

Precondition failures surface as `409` resource-state conflicts (`stale_revision`, `stale_head`, `commit_id_reuse_conflict`) rather than `412`: v0 treats them as conflicts with current namespace state, not HTTP conditional-request failures.

## 4. Client profiles

These profiles are defined by the surface a client uses, not by whether the implementation is a CLI, desktop app, web app, SDK, or service. A single client may implement more than one profile.

### 4.1 Path-oriented client

This client uses the path-oriented surface.

Typical behavior:

- `ls`, `stat`, `get`, `put`, `mkdir`, `mv`, and `cp` use user-visible paths;
- the server remains authoritative for path resolution, canonical inode identity, and commit validation;
- small commands are often sessionless;
- large or recursive commands may be realized as sequences of ordinary logical commits.

This client does not require a sync database or full local mirror.
Implementations may still keep durable local state such as auth/session state, retry journals, pinned snapshot ids, or inode context learned from prior responses when that improves usability, restart safety, or resumability.

### 4.2 Sync client

This client maintains durable local state and consumes the change feed over time.

Typical behavior:

- maintains a durable cursor;
- projects remote state into local state;
- may upload content and publish explicit commits;
- preserves conflicts according to the client's conflict policy.

### 4.3 Explicit-commit client

This client uses the upload, commit, and change-feed surface more directly. It stages content and publishes explicit commits, but it does not necessarily maintain a long-lived local mirror.

Typical behavior:

- content hashing and upload;
- explicit commit with preconditions and commit ids;
- change-feed reads or cursors where incremental observation is needed.

### 4.4 Operator or admin tool

This client uses low-level recovery or inspection surfaces that are specific to an implementation or deployment.

## 5. Statefulness summary

The following table summarizes the core split. For more detailed command-oriented guidance, see [Appendix 095: Operation Statefulness Matrix](../appendices/095-operation-statefulness-matrix.md).

| Operation | Usual shape | Typical server-side state |
| --- | --- | --- |
| `get <file>` | One request | None after the request completes. |
| `get -r <dir>` | Multi-request snapshot read | A read session may pin a consistent snapshot. |
| `put <file>` | One request for small files; staged upload for large files | A put intent and upload session may bind the destination and upload. |
| `put -r <dir>` | Client- or coordinator-driven upload plus one or more commits | No core job is required. Implementation-specific helpers may exist outside the core model. |
| `cp <file>` on one service | One request | Usually none after the request completes. |
| `cp -r <dir>` on one service | Client- or coordinator-driven sequence of logical commits | No core job is required. Implementation-specific helpers may exist outside the core model. |

## 6. Client and server responsibilities

| Concern | Server | Client |
| --- | --- | --- |
| Path resolution | Authoritative | Supplies user intent by path when using the filesystem surface. |
| Content hashing and upload | May accept direct bytes, proxy uploads, or issue upload capabilities, but must verify that any content referenced by a commit is already durable. | Usually responsible for reading local bytes, computing content hashes, and uploading missing content when originating new data. |
| Commit validation | Authoritative | Supplies preconditions and commit ids where needed. |
| Namespace visibility | Authoritative | Observes committed results. |
| Long-running transfer progress | Authoritative for sessions that affect correctness | Responsible for local temp files, local progress, retry behavior, and any higher-level orchestration outside the core model. |
