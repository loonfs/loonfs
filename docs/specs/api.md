# LoonFS API Specification

This document is the normative specification of the LoonFS client-facing API. It covers API groups, capability discovery, the standard error contract, the SDK shape, the upload and commit model, and the v0 HTTP API.

The specification is normative for every part of the API that a deployment implements. A deployment chooses which optional API groups to expose, and every operation it exposes MUST match the shape defined here. The companion [format specification](format.md) defines the durable storage format, which is mandatory for every implementation.

The same client code works against an embedded engine and a hosted server. Both expose the same operations, advertise their capabilities in the same document, and report unsupported operations and features with the same errors.

The key words MUST, MUST NOT, SHOULD, and MAY in this document are to be interpreted as described in BCP 14 ([RFC 2119](https://www.rfc-editor.org/rfc/rfc2119), [RFC 8174](https://www.rfc-editor.org/rfc/rfc8174)) when, and only when, they appear in capitals.

## 1. API groups

An "API group" is a coherent set of endpoints with a shared area of responsibility (rather than a single resource type). Each API group is an all-or-nothing unit of conformance, so a deployment MUST NOT advertise an API group unless it implements every required operation in that group. Optional behavior inside an API group is advertised as feature keys in the capability document (see [Capability discovery](#2-capability-discovery)).

| API group | Operations | Status |
| --- | --- | --- |
| `filesystem/v0` | Path and inode reads, path and inode mutations, uploads, namespace lifecycle, snapshots, the change feed, namespace state, capability discovery, and standard errors. Inline commit content and direct transfers are optional features. | Mandatory for any conforming deployment |
| `maintenance/v0` | Namespace diagnostics, the snapshot inventory, one-shot metadata maintenance, retention-floor advancement, garbage collection, and grep index maintenance (as a feature). | Optional |
| `query/v0` | Content search over derived indexes, gated by the `query.grep` feature. | Optional |

API groups are client contracts and don't describe node roles or deployment topology:

- **Embedded engine and minimal servers.** An embedded engine implements `filesystem/v0` and `maintenance/v0` (where callers trigger maintenance manually). A minimal server that wraps the embedded engine over HTTP advertises the same two groups and is fully conformant.
- **Hosted server maintenance modes.** A hosted LoonFS server serves `maintenance/v0` when its `maintenance` setting is `serve_and_maintain` or `serve_only`. In `maintain_only` or `disabled` mode, every route under `/v0/maintenance/` returns `route_not_found`, just like any other unserved path. All four modes are fully conformant.
- **Independent versioning.** Each API group has its own version, so `filesystem/v0` can coexist with a future `maintenance/v1`. The group name (the segment before the slash) is the stable identifier that feature keys refer to.
- **Scheduling is out of scope.** `maintenance/v0` defines only the shapes for triggering work and reporting its status. Queueing and job scheduling are left to the implementation.

### 1.1 Where new behavior belongs

Three questions determine where new behavior belongs:

1. Does it change durable bytes or metadata that another implementation needs to interpret? If so, it belongs in the [format specification](format.md) and is mandatory.
2. Is it a client-visible operation whose shape should be uniform wherever it exists? If so, it belongs in this document, inside an API group or as a feature.
3. Is it about how work runs internally (queues, schedulers, or caches)? If so, it's an implementation choice and stays out of both specifications.

## 2. Capability discovery

### 2.1 The capability document

Every deployment describes what it supports in one capability document. A remote client fetches it from `GET /v0/capabilities` and caches it for the connection, while an embedded engine exposes the same document as a constant. Because both backends return the same structure, SDKs check capabilities identically across deployments.

The example below is the reference deployment's document with default server settings. It advertises the runtime's `filesystem/v0` and `maintenance/v0` groups, plus `query/v0`, which the server adds by composing the grep extension. A host without that extension omits `query/v0` and both grep feature keys (`query.grep` for searching an index, and `maintenance.grep.index` for administering one).

```json
{
  "protocol_version": "v0",
  "api_groups": ["filesystem/v0", "maintenance/v0", "query/v0"],
  "features": {
    "maintenance.grep.index": true,
    "filesystem.changes.split_commits": true,
    "filesystem.commits.inline_content": true,
    "filesystem.merges": true,
    "query.grep": true
  },
  "limits": {
    "access.max_principals_per_request": 64,
    "commit.max_content_tokens": 4096,
    "commit.max_inline_content_bytes_per_operation": 65536,
    "commit.max_message_bytes": 4096,
    "commit.max_operations": 4096,
    "commit.max_preconditions": 1024,
    "commit.max_request_body_bytes": 2097152,
    "download.service_proxied.max_concurrent_requests": 16,
    "download.service_proxied.max_content_bytes": 268435456,
    "pagination.default_limit": 1000,
    "pagination.max_limit": 1000,
    "pagination.namespaces_default_limit": 100,
    "query.grep.scan_budget_files": 4096,
    "query.grep.tail_budget_files": 512,
    "upload.complete.max_request_body_bytes": 8388608,
    "upload.service_proxied.max_concurrent_requests": 8,
    "upload.service_proxied.max_content_bytes": 268435456
  }
}
```

| Field | Meaning |
| --- | --- |
| `protocol_version` | The protocol version, `v0`. |
| `api_groups` | Advertised API groups, each formatted as `group/version`. |
| `features` | Feature keys mapped to booleans. An absent key means the feature isn't supported. |
| `limits` | Advisory numeric limits clients can use to validate requests before sending them. This object can be empty. |

Clients and servers follow four rules when reading or emitting this document:

- API groups are all-or-nothing for their required operations, so clients don't probe individual operations inside an advertised group.
- Every feature key is dotted (for example, `query.grep`), and its first segment MUST match the group name of an advertised API group. A feature key whose first segment doesn't match an advertised group is malformed, and clients MUST reject the document or ignore that key.
- Clients MUST ignore unknown feature keys and unknown document fields.
- Limits are advisory. The server's response to an actual request is always authoritative.

Registered limit keys:

| Limit key | Meaning |
| --- | --- |
| `pagination.default_limit` | Page size used when a paged request omits `limit`, except namespace listing, which uses `pagination.namespaces_default_limit` (see [Listing namespaces](#listing-namespaces)). An explicit `limit=0` returns 400 `invalid_request`. |
| `pagination.namespaces_default_limit` | Page size used when `GET /v0/namespaces` omits `limit` (100). |
| `pagination.max_limit` | Largest page size accepted for paged requests. A larger `limit` returns 400 `invalid_request`. |
| `upload.service_proxied.max_content_bytes` | Largest body accepted by one `PUT .../uploads/{upload_id}/content` request. This is a per-request proxy cap, not a maximum file size. Clients can use `direct_put` for larger uploads when `filesystem.uploads.direct_put` is advertised, and otherwise MUST stay within this limit. |
| `upload.direct_put.max_content_bytes` | Largest object the deployment's provider accepts in one presigned `direct_put` request, independent of the service-proxied limit. A size hint above this limit returns `content_too_large` at upload begin, and completion checks the actual stored size. Advertised only with `filesystem.uploads.direct_put`. |
| `upload.complete.max_request_body_bytes` | Largest JSON body accepted by `POST .../uploads/{upload_id}/complete`. Larger requests return `content_too_large`. |
| `download.service_proxied.max_content_bytes` | Maximum bytes streamed in one service-proxied read (`GET .../filesystem/content`, `GET .../inodes/{inode_id}/content`, or `GET .../inodes/{inode_id}/revisions/{revision_no}/content`). Checked against the full file without a `Range` header, or against the requested range length for a [ranged read](#ranged-reads). Larger reads return `content_too_large`. Clients can read in ranges or request a presigned URL when `filesystem.downloads.direct_get` is advertised (always true when direct writes are offered). |
| `upload.service_proxied.max_concurrent_requests` | Maximum service-proxied upload requests a serving process streams at once, shared across all callers. Excess requests return `server_busy`. |
| `download.service_proxied.max_concurrent_requests` | Maximum service-proxied content reads a serving process streams at once, shared across all callers. Excess requests return `server_busy`. A read occupies a slot until its response body finishes or is dropped. |
| `access.max_principals_per_request` | Maximum principal IDs one request can send in `Loonfs-Principals`. Excess principals return `invalid_request`. |
| `namespace.default_retention_window_ms` | Retention window, in milliseconds, assigned to a namespace created without `retention_window_ms`. Omitted when new namespaces get no default window. A `retention_window` maintenance job can change or clear the window later. |
| `commit.max_inline_content_bytes_per_operation` | Largest file size for one `inline_content` value, measured before base64 encoding. Advertised only with `filesystem.commits.inline_content`. Larger values return `content_too_large` before commit planning. `commit.max_request_body_bytes` caps the entire request. |
| `commit.max_request_body_bytes` | Largest JSON body a commit request accepts, including base64 inline content and all other fields. Larger bodies return `content_too_large`. Advertised by HTTP deployments. |
| `commit.max_operations` | Maximum path operations in one commit. Longer lists return `invalid_request` before planning on every transport. |
| `commit.max_preconditions` | Maximum precondition entries in one commit (counting entries, not distinct resources). Longer lists return `invalid_request` before planning on every transport. |
| `commit.max_content_tokens` | Maximum content tokens in one commit. Excess tokens return `invalid_request` before planning. |
| `commit.max_message_bytes` | Maximum commit `message` length in bytes. Longer messages return `invalid_request` before planning. |
| `query.grep.scan_budget_files` | Maximum files an `allow_scan` grep without an index plan scans before failing with `query_unindexable`. |
| `query.grep.tail_budget_files` | Maximum distinct files whose content can change after the index watermark before grep returns `index_lagging` (or skips the tail when `allow_stale` is set). |

### 2.2 Feature registry

The feature registry is stable within a protocol version, and new feature keys are added only through a specification change.

| Feature key | Gates | Notes |
| --- | --- | --- |
| `maintenance.grep.index` | Grep index administration: `GET /v0/maintenance/namespaces/{ns}/grep/index` and its `enable` and `disable` routes. | Independent of `query.grep`, because index maintenance and query serving can be deployed separately. Deployments without index maintenance return `not_supported` with this `feature` key. Every deployment's `gc` jobs still collect unreferenced grep index segments. |
| `filesystem.changes.split_commits` | The `split_commits` and `event_index` query parameters on `GET /v0/namespaces/{ns}/changes` and `GET /v0/namespaces/{ns}/diff`, and `next_event_index` in their responses. | Lets a page stop inside a single commit that exceeds the page budget and resume at an event index (see [`GET /changes`](#611-get-changes)). Without it, both routes return every commit whole. |
| `filesystem.commits.inline_content` | Sending file bytes inline in `put_file`, `create_file_by_inode`, or `put_file_revision_by_inode`. | Bounded per file by `commit.max_inline_content_bytes_per_operation`. Without this feature, clients upload content before committing. |
| `filesystem.merges` | Fork merges and rebases: `POST /v0/namespaces/{ns}/merges`, `GET /v0/namespaces/{ns}/merges`, `POST /v0/namespaces/{ns}/rebases`, and `POST /v0/namespaces/{ns}/replays`. | Plans a fork's net change since its previous merge into one commit (see [`POST /merges`](#614-post-merges)), or replays unmerged commits into a new fork while reporting conflicts (see [`POST /rebases`](#616-post-rebases) and [`POST /replays`](#617-post-replays)). Advertised only when merges are enabled (in the reference server, when `max_merge_bytes` is non-zero). Otherwise these four routes return `not_supported` with this `feature` key. |
| `filesystem.uploads.direct_put` | Presigned `direct_put` upload sessions (`POST /v0/namespaces/{ns}/uploads`). | Returns a short-lived, create-only presigned PUT URL for the exact content object, and requires a provider that reports a durable whole-object checksum. Advertised only on endpoints verified by the live conformance suite. Independent of `filesystem.uploads.direct_multipart`. Never exposes raw object keys. |
| `filesystem.uploads.direct_multipart` | Presigned `direct_multipart` upload sessions (`POST /v0/namespaces/{ns}/uploads`) and part signing (`POST /v0/namespaces/{ns}/uploads/{upload_id}/parts`). | Opens a provider multipart upload and returns short-lived, checksum-bound presigned URLs per part. Requires an S3-style multipart API, so providers without one omit this key while still advertising `direct_put` and `filesystem.downloads.direct_get`. |
| `filesystem.downloads.direct_get` | Presigned download URLs (`POST /v0/namespaces/{ns}/filesystem/downloads`, `POST /v0/namespaces/{ns}/inodes/{inode_id}/downloads`, and `POST /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{revision_no}/downloads`). | Returns short-lived presigned GET URLs for the selected content object without exposing raw object keys. Every deployment that offers direct writes also advertises this key so large files written directly can be read back directly. |
| `query.grep` | Content search (`GET /v0/namespaces/{ns}/grep`). | Data-dependent capability (see [Data-dependent features](#23-data-dependent-features)). Also requires an active grep index in the namespace's manifest, and namespaces without one return `not_supported` regardless of this key. |

`maintenance.grep.index` is the only optional feature in `maintenance/v0`. In `filesystem/v0`, the only optional features are splitting commits across pages, inline commit content, merges, and the three direct-transfer features. Every other operation in both groups is required, including namespace creation, forking, deletion, and snapshot routes.

Listing namespaces isn't required to discover a known namespace, and callers can always address namespaces directly by ID. (The `list_namespaces` route is described in [Listing namespaces](#listing-namespaces).)

### 2.3 Data-dependent features

The capability document describes what a deployment can serve, whereas whether a specific namespace has built an extension's derived index (such as the grep index) is recorded in that extension's entry in the namespace manifest (see [Extensions](format.md#124-extensions)).

A data-dependent operation succeeds only when both conditions are met:

- The deployment advertises the serving capability in its capability document.
- The namespace manifest records that the extension's index is ready, using the extension's own verified readiness marker.

## 3. Standard error contract

Every error response uses this JSON envelope:

```json
{
  "code": "writer_fenced",
  "message": "writer session fenced: epoch 3 was fenced by epoch 4 (writer `server-b`, acquired at 1739459200000 ms)",
  "request_id": "req_4k8m2v9qx7d3hn5tbwa6jc1rfe",
  "details": {
    "fenced_writer_epoch": 3,
    "active_writer_epoch": 4,
    "active_writer_id": "server-b",
    "active_acquired_at_ms": 1739459200000
  }
}
```

The top-level fields describe the error and the request:

- **`code`.** The stable machine-readable contract. Clients MUST branch on `code`, MUST tolerate unrecognized codes, and MUST NOT parse `message`.
- **`message`.** A human-readable description that can change between releases.
- **`request_id`.** The server-assigned correlation ID for the request. Every response (success or error) also returns it in the `x-request-id` header so client logs and server traces can be joined.
- **`feature`.** Present only on `not_supported` errors, identifying the capability-document key the client should reconcile against.
- **`param`.** Present when the error points to one invalid input (see the format table below).
- **`details`.** Optional machine-readable values for the failure (such as an inode ID or writer epoch), so callers never parse `message` to act.

Each error code corresponds to one client remedy, which lets generic retry layers (such as generated SDKs) decide how to handle an error from `code` alone without knowing the route or parsing `message`. Two conditions share a code only when clients handle them identically, and in those cases `details.cause` records which condition occurred for logging and metrics. For example, a server-side deadline cancellation and an unconfirmed object-storage write both return `outcome_unknown` (with `cause` set to `deadline` or `unconfirmed`), because both require the client to reconcile before retrying. Request parameters follow the same consistency rule: one concept uses one name on every route (for example, every route that identifies a fork to merge uses `source_namespace_id`).

When `param` is present, its format matches the input source:

| Input source | `param` value |
| --- | --- |
| JSON request body | JSON Pointer |
| Request header | Header name |
| Query parameter | Parameter name |
| Path parameter | Parameter name |
| CLI-local input | Flag or argument spelling |

Commit shape errors point to the offending field with `/operations/{i}/{field}` or `/preconditions/{i}/{field}`, where `i` is zero-based.

This error envelope also covers request-shape failures: an unparseable query string, path parameter, or JSON body returns `invalid_request` in this envelope, never a framework's plain-text error. Authorization runs before request parsing, so a malformed request without valid credentials returns `unauthorized`. Unrecognized query parameters return `invalid_request` with `param` set to the parameter name, just like unknown JSON body fields (see [Authentication and transport](#authentication-and-transport)).

Every field inside `details` is optional, and clients MUST tolerate absent or unknown fields just as they tolerate unknown codes. When a caller supplies one value and the server finds another, `details` pairs `expected_<field>` with `actual_<field>`.

The codes that populate it:

| Code | Detail fields |
| --- | --- |
| `namespace_not_found` | `namespace_id` identifies the missing namespace, including a fork referenced by a merge, rebase, replay, or next-merge-range request. |
| `namespace_deleted` | `namespace_id` identifies the deleted namespace, including a fork's source or target, or a fork referenced by a merge, rebase, replay, or next-merge-range request. |
| `writer_fenced` | `fenced_writer_epoch` and `active_writer_epoch`, plus `active_writer_id` and `active_acquired_at_ms` when the current manifest includes a writer block. Because writer IDs are process labels that two runs on one host can share, the acquisition timestamp distinguishes them. |
| `path_conflict` | `expected_inode_id` and `actual_inode_id` (absent when the path isn't bound), plus `precondition_index` for a failed request precondition. |
| `stale_revision` | `inode_id`, `expected_revision_no`, and `actual_revision_no` (absent when the inode has no current revision or isn't visible), plus `precondition_index` for a failed request precondition. |
| `stale_attributes` | `inode_id`, `expected_attributes_revision_no` (absent when the caller stated no expectation), and `actual_attributes_revision_no` (absent when the inode isn't visible), plus `precondition_index` for a failed request precondition. |
| `stale_access` | `inode_id`, `expected_access_revision_no` (absent when the caller stated no expectation), and `actual_access_revision_no` (absent when the inode isn't visible), plus `precondition_index` for a failed request precondition. |
| `binding_version_mismatch` | `inode_id`, `expected_binding_version` (the token supplied by the caller), and `actual_binding_version` (the current binding's token, absent when the inode isn't visible), plus `precondition_index` for a failed request precondition. Clients MUST NOT parse or order these tokens. |
| `commit_id_reuse_conflict` | `commit_id`, plus `committed_seq` and `committed_fingerprint` when the conflict was detected from a durable commit receipt. `committed_seq` is where `commit_id` was committed and `committed_fingerprint` is its semantic identity (see [Commit identity and preconditions](#51-commit-identity-and-preconditions)). Both come together from the receipt and retained commit row, and both are absent when two live requests claim an uncommitted ID at once. |
| `below_retention_floor` | `retention_floor_seq`, plus the sequence that fell below it: `after_seq` for a change feed, `from_seq` for a diff, or `fork_point_seq` when a merge's fork point is below the target namespace's floor. When a merge's range start is below the fork's own floor, `source_namespace_id` identifies the fork, `source_after_seq` is the range start, and `retention_floor_seq` is the fork's floor. For a read at an earlier sequence (such as a later grep page), only `retention_floor_seq` is included. |
| `merge_conflict` | `conflict_count` (total conflicting inodes) and `conflict` (the conflict entry for the conflicting inode with the smallest ID, see [POST /merges](#614-post-merges)). This is the only nested object in details. |
| `merge_too_large` | `merge_bound` (the bound exceeded, listed in the registry below), plus `split_at_seq` (a fork sequence where a shorter merge fits, when one exists, see [POST /merges](#614-post-merges)). |
| `merge_range_conflict` | `source_namespace_id` identifies the fork, `expected_after_seq` is where the merge had to start (after the previous merge, or at the fork point for a first merge), and `actual_after_seq` is where the merge started. |
| `stale_head` | `expected_head_seq` and `actual_head_seq` for a caller-supplied head precondition, plus `precondition_index` for a failed request precondition. |
| `snapshot_not_found` | `cause`, present when the snapshot record exists but its manifest is missing (value listed in the registry below). |
| `store_contention` | `cause`, present when a metadata publication hit its time budget before publishing (value listed in the registry below). |
| `outcome_unknown` | `cause`, explaining why the outcome wasn't observed (values listed in the registry below). |
| `forbidden` | `inode_id`. |
| `not_deleted` | `inode_id`, plus `expected_deletion_seq` and `actual_deletion_seq` when the inode has a trash deletion at a different sequence. |
| `range_not_satisfiable` | `size_bytes`, the byte size of the target file. |
| any failed commit | `commit_id`, echoed so failed and uncertain commits include the caller's reconciliation handle (see [Commit responses and safe retry](#52-commit-responses-and-safe-retry)). |
| any failed operation | `operation_index`, the zero-based position of the failing operation (0 for a single-operation request, see [Commit identity and preconditions](#51-commit-identity-and-preconditions)). |

`cause` is a string from a small enum, and clients MUST tolerate unrecognized values just as they tolerate unknown codes. Clients branch on `code` and use `cause` only for logging, reporting, or metrics.

One code exists specifically so that capability handling is uniform from the start:

- `not_supported` (HTTP 501): the deployment doesn't implement the requested operation or feature. Any operation can return it. A client maps the error to its `feature` key and disables or degrades that code path.

The full registry (`ErrorCode` in `loonfs-types`) lists every error code and its HTTP status:

| Code | HTTP status | Meaning |
| --- | --- | --- |
| `invalid_request` | 400 | Malformed request: a path, ID, cursor, parameter, staged content reference, configuration value, or commit limit failed validation. The message identifies the offending field or limit. |
| `unauthorized` | 401 | Missing or invalid bearer credentials. |
| `forbidden` | 403 | The subject has some rights on a checked inode but lacks a required right, lacks the authority needed for a move or access update, or lacks administrator rights on an admin-only operation. If the subject has no rights on a checked inode, LoonFS returns `path_not_found` or `inode_not_found` instead (see [Authorization](#authorization)). |
| `content_too_large` | 413 | A request body, proxied response, append, or inline content value exceeded its size limit. JSON bodies MUST fit within 2 MiB (except upload routes, which have their own limits), appends are capped at 256 KiB, inline values at `commit.max_inline_content_bytes_per_operation`, and multipart completions at `upload.complete.max_request_body_bytes`. Use `direct_put` for large uploads, and ranged reads or `filesystem.downloads.direct_get` for large downloads. |
| `merge_too_large` | 413 | A [merge](glossary.md#merge) exceeded the WAL object size (512 MiB), the server's merge memory allowance (`max_merge_bytes`, 512 MiB by default), or the touched-inode cap. `details.merge_bound` reports `wal_object`, `merge_allowance`, or `touched_inodes` (clients MUST tolerate other values), and `details.split_at_seq` provides a split point when a shorter range fits (see [`POST /merges`](#614-post-merges) and the sizing note below). |
| `commit_too_large` | 413 | A commit exceeds one WAL object (512 MiB) and can't be published. Nothing is committed, so split the operations into smaller commits. |
| `range_not_satisfiable` | 416 | A ranged read starts at or past the end of the file. `details.size_bytes` reports the file's size. |
| `route_not_found` | 404 | No route matches the request path. |
| `method_not_allowed` | 405 | The path exists but doesn't support this HTTP method. |
| `namespace_not_found` | 404 | The namespace has no manifest, or its newest manifest is `initializing`. Every request except create, fork, or delete treats an initializing namespace as missing. |
| `pool_not_found` | 404 | No object exists under the pool's prefix, so the pool doesn't exist and collection deletes nothing. |
| `namespace_deleted` | 410 | The namespace ID is deleted or [retired](format.md#95-retirement) and can't be created or forked into again. Ordinary operations fail, and `details.namespace_id` identifies the deleted namespace. |
| `snapshot_not_found` | 404 | No usable snapshot exists under this ID: it's missing, expired, deleted, or a fork snapshot used on a non-read route. Also returned when a fork folding the source's WAL tail exceeds its 5-minute temporary snapshot window or loses a source tail WAL object, or when snapshot inventory lookup misses. When the record exists but its manifest is gone, `details.cause` is `manifest_missing`. |
| `path_not_found` | 404 | No visible entry exists at the path, or the subject has no rights on the checked entry. |
| `inode_not_found` | 404 | No visible or retained inode exists with this ID, or the subject has no rights on the checked inode. |
| `revision_not_found` | 404 | The file has no such revision. |
| `upload_not_found` | 404 | No usable upload session exists with this ID. Returned for missing sessions, aborted sessions, and open sessions asked to stage, sign parts, or complete at or after `expires_at_ms`. |
| `namespace_exists` | 409 | The create or fork target ID already belongs to an active namespace, or the request options don't match manifest 1 of an initializing namespace with that ID (see [Creating and forking namespaces](#creating-and-forking-namespaces)). |
| `content_not_prepared` | 409 | A commit operation references external content without a valid admission proof, or its token was rejected. Prepare the content and retry with a valid token. |
| `path_conflict` | 409 | The destination path already exists, or a `path_binding`, `path_absence`, or `name_absence` precondition failed. |
| `wrong_kind` | 409 | A file was addressed where a directory is required, or a directory where a file is required (including a file used as a parent path component, or a commit operation on the wrong inode kind). |
| `directory_not_empty` | 409 | The directory has children and the operation isn't recursive. |
| `stale_head` | 409 | The write raced a head advance, a caller-supplied `expected_head_seq` didn't match, snapshot creation raced newer manifests on every attempt, or a fork's source WAL tail still had a commit too old to fork after every fold. Retry against fresh state (see the read-retry note below). |
| `stale_revision` | 409 | A caller-supplied base content revision is no longer current. |
| `stale_attributes` | 409 | The inode's attribute revision changed before the update committed, whether the caller supplied an expectation or relied on the update's own implicit revision check. Re-read attributes and retry. |
| `stale_access` | 409 | The inode's access revision changed before the update committed. Re-read the access row and retry. |
| `binding_version_mismatch` | 409 | The `expected_binding_version` on an inode move, delete, or binding precondition doesn't match the entry's current binding. Re-read the entry and retry. |
| `not_deleted` | 409 | The restore target isn't the root of an active trash deletion (above the retention floor and not yet revoked). |
| `writer_fenced` | 409 | Another writer superseded this writer session. The session is dead, this request fails, and a later request opens a new session whose first publish reclaims the namespace. Repeated fencing means two writers are receiving traffic for one namespace. |
| `would_cycle` | 409 | The move would create a directory cycle. |
| `commit_id_reuse_conflict` | 409 | The `commit_id` was reused with different operations, content, actor, or preconditions. |
| `merge_range_conflict` | 409 | A [merge](glossary.md#merge) range overlaps or skips past the fork's previous merge (or a first merge doesn't start at the fork point), usually because two merges of the same fork ran concurrently. Retry with a new `commit_id`. Clients don't retry this automatically. |
| `merge_conflict` | 409 | The fork's net change conflicts with the target namespace under the conflict rule ([Validation](format.md#62-validation)). Nothing is committed. `details` reports `conflict_count` and the first `conflict`. Clients don't retry this automatically. |
| `upload_already_completed` | 409 | The upload session is already completed, so it can't accept new content or be aborted. |
| `upload_content_conflict` | 409 | Different bytes were already staged under this upload ID. Open a new upload session (restaging identical bytes succeeds). |
| `query_unindexable` | 400 | The pattern lacks a run of at least 3 literal bytes for the trigram index. Rewrite the pattern or set `allow_scan` (capped by `query.grep.scan_budget_files`). |
| `below_retention_floor` | 409 | A history position required by the request (`after_seq`, `from_seq`, an earlier grep page's head, a merge's fork point, or a merge's range start) fell below a retention floor. Change-feed and diff clients re-bootstrap from a listing or snapshot, and unmergeable forks must be recreated from the current head (see the retention note below). |
| `not_supported` | 501 | The deployment doesn't implement the requested operation or feature. |
| `outcome_unknown` | 503 | A write's outcome wasn't observed, so it might have taken effect. `details.cause` is `unconfirmed` (store write outcome unobserved) or `deadline` (cancelled at `request_deadline_ms`). Retry commits with the same `commit_id` (see [Commit responses and safe retry](#52-commit-responses-and-safe-retry)). For other writes, read the resource before retrying. |
| `commit_queue_full` | 503 | The namespace's write queue is full. Back off and retry. |
| `writer_session_closed` | 503 | This node has no open writer session for the namespace, so the request wasn't admitted. Retry on the node assigned to the namespace. |
| `server_busy` | 503 | The server is at its concurrency cap for requests, proxied uploads, or proxied downloads, or concurrent merges left no room in the merge memory allowance. Back off and retry. |
| `shutting_down` | 503 | The server stopped admitting new requests during shutdown (already-admitted work still finishes). Retry against a live instance. |
| `store_contention` | 503 | The request stopped without publishing: a control-object write exhausted its compare-and-swap retries, a metadata publication hit its time budget (`details.cause` is `publication_budget`), or a read found its manifest replaced on every retry. Back off and retry. |
| `content_not_materialized` | 503 | The file is committed, but this deployment can't write its pending content pieces for a direct download. Use the proxied content route or retry after a fold. |
| `maintenance_required` | 503 | The WAL tail hit its write-stop threshold and the [write-stop wait](format.md#73-recovery-material-and-maintenance-policy) elapsed before a fold freed room. Nothing is committed. Run maintenance and retry. Replays of already-committed IDs still return their receipt. |
| `index_lagging` | 503 | The grep index is further behind the head than the tail-scan budget allows. Run maintenance or set `allow_stale`, then retry. |
| `storage_permission_denied` | 503 | Object storage rejected the deployment's credentials for this operation. Fix credentials or bucket policy, because unchanged retries won't succeed. |
| `index_corrupt` | 500 | The grep index's derived state failed validation, or uses a different grep format version. Disable and re-enable grep to rebuild (disabling another version requires a release that reads that version). Core filesystem state stays available. |
| `namespace_corrupt` | 500 | Durable namespace state failed validation. |
| `server_error` | 500 | An unclassified internal failure. |

A few error codes have extra sizing or retry behavior worth calling out:

- **`merge_too_large` sizing.** Instead of the normal commit request limits, merges are bounded by one WAL object (512 MiB), the touched-inode cap, and the server's merge memory allowance (`max_merge_bytes`, 512 MiB by default). The server estimates a merge's peak memory as the larger of three times its planned WAL bytes or its planning memory (touched inodes, paths, and one page of fork commits). In the reference implementation, the pre-planning upper bound per created file is about 4,850 WAL bytes, so one WAL object fits roughly 110,000 file creates and the default 512 MiB allowance admits roughly 36,000 file creates. When a merge is too large, clients split it into consecutive ranges using `split_at_seq` (for example, after `P` through `T1`, then after `T1` through `T2`). A single fork commit larger than one WAL object can't be split or merged, while one that exceeds only the merge allowance can be merged on a server with a higher allowance.
- **`stale_head` on reads.** Ordinary HTTP reads don't return `stale_head`. If garbage collection removes an older manifest's segments after a newer manifest replaces it, the server retries the read from the current manifest (and restarts searches from a new view) up to its attempt limit, after which it returns `store_contention`. By contrast, an embedded `ReadView` reads one manifest for its entire lifetime, so missing segments return `stale_head` and the caller captures a new view. A snapshot read that hits missing segments returns `snapshot_not_found` if the snapshot was deleted or expired during the read, or `namespace_corrupt` if the snapshot is still valid.
- **`below_retention_floor` on merges.** A merge fails with `below_retention_floor` when the fork point is below the target namespace's retention floor (because compaction might have removed the fork's previous merge from the [merge index](glossary.md#merge-index)), or when the merge range start is below the fork's own floor (because the fork's net change can't be computed from pruned history). Such a fork can't be merged, so the caller creates a new fork from the current head and replays the changes it wants to keep using ordinary reads and writes.

The reference server also enforces two transport-level limits:

- **In-flight requests (`max_in_flight_requests`, default 256).** Excess requests are rejected before authentication, body parsing, or store access. Each request occupies a slot until its response body completes or is dropped, and uploads and downloads count against both this cap and their transfer cap. `GET` and `HEAD` requests to `/health` and `/readiness` are exempt (regardless of query parameters). A `GET` or `HEAD` request to `/metrics` takes a slot of its own instead of one of these, so one scrape runs at a time and a second scrape that arrives while it runs is rejected with `server_busy`.
- **Accepted TCP connections (`max_connections`, default 1,024).** When all slots are full (including TLS handshakes and idle keep-alive connections), the server pauses accepting new TCP connections and leaves them in the OS backlog, where callers may wait or time out. This limit applies to every route, including probes.

Automatic retry is narrower than HTTP status codes alone suggest. Clients may retry raw transport failures, and among registered error codes, they automatically retry only `commit_queue_full`, `server_busy`, and `shutting_down`. Responses with those three codes include `Retry-After: 1`, and generated SDKs retry on the presence of `Retry-After` rather than on HTTP status alone.

Other 503 codes require specific action before retrying and include no `Retry-After` header:

- `writer_session_closed` requires routing to the node assigned to the namespace (or waiting briefly if that node is draining a fenced session before a later request opens a new one).
- `store_contention` requires caller-controlled backoff, because LoonFS already exhausted its internal CAS attempts, publication time budget, or manifest-reload retries.
- `maintenance_required` and `index_lagging` require running maintenance.
- `storage_permission_denied` requires fixing storage credentials or bucket policy.
- `outcome_unknown` requires checking whether a mutation took effect before retrying it.

Precondition failures surface as `409` resource-state conflicts (`stale_revision`, `stale_head`, `commit_id_reuse_conflict`) rather than `412`, because v0 treats a failed precondition as a conflict with current namespace state rather than an HTTP conditional-request failure.

## 4. SDK shape

The same SDK works with both the embedded runtime and a hosted server, so applications don't need separate code paths for different deployment modes. A generated client accepts a default actor at construction time, and individual requests can override it with a header.

Generated SDKs use OpenAPI schema names as public type names:

- A resource body uses the resource name, such as `SnapshotRecord` or `UploadSession`.
- The namespace body is `NamespaceMetadata`, avoiding a collision with the embedded `Namespace` handle.
- A response envelope uses `<Verb><Noun>Response`, such as `ListSnapshotRecordsResponse`.
- An operation's request and response schemas share its verb, such as `CreateDownloadRequest` for `create_download`.

Namespace-owned resources always include `namespace_id`.

Revision numbers, change sequences, attribute revisions, manifest numbers, writer epochs, and grep run numbers are JSON integers from 0 through 9007199254740991 (`2^53 - 1`). Implementations MUST reject larger input values and MUST NOT store a larger value.

Field-name suffixes each have one consistent meaning:

| Suffix | Meaning |
| --- | --- |
| `_seq` | Position in the namespace commit history. |
| `_no` | Monotonic counter scoped to one object (a file's revisions, an inode's attribute revisions, or a namespace's manifests). |
| `_index` | Zero-based position inside a container. |
| `_number` | One-based position defined by a provider or tool. |
| `_id` | Opaque identifier. |

The embedded runtime and the Rust HTTP client (`loonfs_client::Client`) use the same verbs for the same operations (such as `stat` and `list`). Because the HTTP client's receiver is the service rather than one namespace, its methods also take the namespace or path, and a few keep the wire operation's name (such as `get_health`).

In the embedded runtime:

- **`loonfs::LoonFs`.** The runtime handle, parameterized as `ReadOnly` or `Writable`. A writable runtime can also create and fork namespaces, and `LoonFs::maintenance` returns its maintenance interface.
- **`loonfs::Namespace`.** A handle bound to one namespace whose methods omit `namespace_id`. `LoonFs::namespace` returns a `Namespace<ReadOnly>` for reads, while `LoonFs::open_namespace` on a writable runtime returns a `Namespace<Writable>` for reads and writes.

The namespace handle's `metadata` method corresponds to the client's `get_namespace`. Both `LoonFs::capabilities()` (a constant) and `Client::get_capabilities()` (fetched from `GET /v0/capabilities` and cached) return the capability document from [The capability document](#21-the-capability-document).

Both Rust APIs stay aligned through three conventions:

- **Shared option structs.** Both re-export the same option types from `loonfs-types` (`PutFileOptions`, `CreateDirectoryOptions`, `DeleteOptions`) rather than hiding behind a transport trait. A host selects either the embedded runtime or the HTTP client directly.
- **Actor and optional settings.** Every commit, upload operation, namespace creation, and fork records an actor. The embedded runtime takes it as an argument on each of those calls. The HTTP client takes it at construction (`ClientConfig::actor`) and sends it in `Loonfs-Actor`; `Client::with_actor` clones the client with another actor, and a client without one can read but fails an operation that records an actor before sending it. Option structs contain only optional fields with defaults, so each mutation provides both a plain form and an options form.
- **Typed unsupported errors.** Individual operations return `not_supported` with their `feature` key, so capability checks and fallbacks work identically on both backends.

Generated SDKs group operations by resource rather than by API group: `capabilities`, `namespaces`, `snapshots`, `commits`, `changes`, `files`, `inodes`, `trash`, and `uploads` sit at the client root. The `maintenance` group keeps its nested prefix (such as `maintenance.snapshots.list`) because hosted deployments can disable the entire group. Method names follow two rules:

- Operations on a group's own resource use a bare verb (`create`, `retrieve`, `list`, `delete`) or action name (`fork`, `grep`).
- Operations on a sub-resource of a file or inode use a verb plus a noun on the parent (such as `files.listRevisions` and `inodes.listChildren`).

The `/health`, `/readiness`, and `/metrics` probes are HTTP-only and have no SDK methods.

In the embedded runtime, every public read acquires one slot from the runtime's `ExecutionBudget` before its first object-store request (`ExecutionBudgetBuilder::max_concurrent_reads` defaults to 64). Reads beyond the limit wait, read-only and writable runtimes sharing a budget share the cap, and dropping a waiting read removes it from the queue:

- Each pager page acquires and releases its own slot.
- Capturing a `ReadView` takes one slot and releases it immediately, and each read through the view takes its own slot.
- Nested snapshot stat and list calls reuse the outer read's slot.
- A buffered content read keeps its slot until it finishes.
- A streamed or ranged content read releases its slot after metadata and content lookup, before returning the body (transfer limits bound body consumption).
- Internal reads inside mutations and maintenance don't take a read slot.
- Grep queries and index steps acquire and release a slot per public read and never wait on a grep step permit while they have a read slot.

### 4.1 Public inode identity

Public APIs represent an inode ID as a string such as `ino_000000r0000013kje0d47gc8kw`. The value MUST start with lowercase `ino_` followed by 26 lowercase Crockford base 32 characters (with the first character between `0` and `7`, matching every generated ID). JSON numbers, uppercase letters, out-of-alphabet characters, and any other length are invalid. In every namespace, `ino_00000000000000000000000000` identifies the namespace root.

An inode ID is unique only within its namespace, so an inode's full identity is `(namespace_id, inode_id)`. (Inherited inodes in a fork keep their IDs from the source namespace.) Clients MUST treat inode IDs as opaque and MUST NOT construct them or infer anything from their characters, such as creation commit, relative age, or count. The storage format describes how LoonFS generates inode IDs ([Namespaces and identity](format.md#11-namespaces-and-identity)), but the API makes no layout guarantee.

### 4.2 Checksums

Every public checksum object has the same shape:

```json
{ "algorithm": "crc64nvme", "value": "<16 lowercase hex>" }
```

Two algorithms are allowed, and any other algorithm or malformed value is rejected:

- `crc64nvme`: exactly 16 lowercase hexadecimal characters.
- `crc32c`: exactly 8 lowercase hexadecimal characters.

The enclosing field defines what a checksum covers: checksums in `ContentRef` and `UploadContentClaim` cover the complete file content, while a part checksum covers one multipart upload part. A `checksum_algorithm` field selects an algorithm without supplying a digest.

Every content reference uses the object store's checksum algorithm. Every upload begin response reports that algorithm in `checksum_algorithm` (including for `service_proxied` uploads), and reads verify the stored checksum in the content reference.

## 5. Minimal upload, commit, and change-feed model

Writing a file is a two-step workflow: prepare the file's content, then commit the filesystem change. Committed changes appear in the change feed in sequence order.

Uploaded content is durable before the commit runs, though the file isn't visible yet. Inline content becomes durable and visible in the same write ([File contents and ownership](format.md#15-file-contents-and-ownership)). In both cases, a commit takes effect only when the next numbered WAL object is written with put-if-absent. Admitting a request into a writer's batch doesn't mean it has committed.

With an embedded `loonfs::Namespace<Writable>`, callers can separate preparation from publication:

- `prepare_content` accepts file bytes, keeps files at or below the inline threshold in memory, uploads larger files as content objects, and returns an opaque prepared value.
- `put_file_prepared` commits that prepared value. If WAL limits are reached, inline bytes may be uploaded before publication, while already-uploaded content requires no extra content-object I/O during publication.
- `complete_upload_prepared` returns both the upload completion response and a prepared value for an upload session. Over HTTP, commits supply either inline bytes or a `content_ref` paired with its `content_token`.

When preparation uploads an object, LoonFS records the upload session before writing the object and completes the session afterward. Prepared uploads have a deadline no later than the expiry of the last token the completed session can issue (matching remote upload token lifetimes in [Upload transport](#69-upload-transport) and [Upload-session cleanup](format.md#116-upload-session-cleanup)). Prepared inline bytes have no expiry.

A prepared upload or token is valid while the clock is strictly before its deadline and expires at the deadline. Checking the deadline prevents a commit from referencing content that cleanup could have reclaimed. LoonFS checks the deadline when the commit enters a batch and again right before the numbered WAL write, using the request clock plus the time elapsed since the publication attempt began. If the final check rejects one candidate in a batch, that candidate receives its own error, the other accepted candidates receive `stale_head` so the publisher re-plans them, and nothing is written.

Prepared content belongs to one namespace and can't be shared across namespaces. A commit that admits uploaded content records the committing namespace as the creator of that content's chain, and only the creator can append to a chain in place ([Operations and WAL deltas](format.md#66-operations-and-wal-deltas)). Sharing one upload across two namespaces would let both append to the same chain.

A prepared value referencing content from the destination's pool expires after 15 minutes. That value points to content referenced by a revision or layout in the source's view, and garbage collection keeps that view's manifests (and content) for longer than 15 minutes after the view was read (the same safety margin used for direct downloads), ensuring a commit within that window finds the content in storage.

### 5.1 Commit identity and preconditions

A commit request consists of:

- **`commit_id`.** A client-generated idempotency key. Clients MUST reuse it verbatim for safe retries.
- **Actor ID.** Sent in the `Loonfs-Actor` request header.
- **`message`.** Optional human-readable text (at most 4,096 bytes) that is part of the commit's identity. Longer messages return `invalid_request` before planning on every transport.
- **`preconditions`.** Optional ordered array of request-level checks.
- **`operations`.** Required ordered, non-empty list of mutations.

A single-operation request uses the exact same shape as a multi-operation batch, so a convenience helper and a one-element list produce the same commit and the same [semantic fingerprint](glossary.md#semantic-fingerprint). The fingerprint always includes `preconditions` in request order (even when empty), so changing or reordering preconditions changes commit identity. A commit whose estimated encoded log exceeds one WAL object returns `commit_too_large` before publication, referencing the [WAL document limit](format.md#a5-wal-records).

All operations in one request commit atomically, in order, as one logical commit. Operation `k` is planned against the authoritative namespace state plus the effects of operations `0..k`, so a single commit can create a directory and write a file into it, or delete a path and recreate it. If any operation fails, the entire request aborts, nothing becomes visible, and the error reports the failing operation's own error code plus its zero-based position in `details.operation_index` (there's no batch-specific error code).

Under the publish lock, the server plans each operation against authoritative state and generates the exact semantic checks that the operation implies (revision identity, binding identity, name absence, directory emptiness, and ancestor visibility). Generating these checks per operation makes concurrent races fail explicitly rather than silently combining conflicting writes, while evaluating each check at its operation's position lets later operations build on earlier ones.

Callers add cross-request preconditions wherever staleness matters:

- Puts can check the current inode and revision.
- Moves and copies can check the destination inode and revision.
- Deletes, trash restores, inode-addressed writes, and namespace deletion each have matching preconditions.

Each operation-level precondition evaluates against the state visible to that operation, including earlier operations in the same request. The server rejects unknown fields in a commit body with `invalid_request`, so a misspelled precondition (such as a typo in `expected_revision_no`) fails instead of silently running without the guard.

Request and event fields follow consistent naming rules:

- Moves and copies use `source_*` and `destination_*` for endpoints, matching their preconditions and `moved` events.
- `restore_deleted` takes its optional target path as `destination_path`.
- Fields identifying the primary addressed resource use the resource's own name (`inode_id`, `namespace_id`, `path`) with no directional prefix.
- Creation requests can use `new_*` for the created resource.
- `target` describes an action subject, persisted relationship, or goal (not a move/copy endpoint).
- `local` and `remote` refer to coordinate systems.

Every entry with a name has a `binding_version`, an opaque token identifying its current parent-and-name binding. Creating, moving, or restoring an entry generates a new token, while content and attribute writes leave it unchanged. Clients MUST NOT parse or order these tokens, and each token is valid only in the namespace that issued it. Inode-addressed moves and deletes require `expected_binding_version` (and the `inode_binding` request precondition checks the same token for any operation). A mismatched token returns `binding_version_mismatch`, while a malformed token or one from another namespace returns `invalid_request`. Both checks are part of the commit's identity. An operation's own `expected_binding_version` is evaluated after any earlier operations in the same request, while the `inode_binding` request precondition is evaluated against the state before the request's operations (see [Request-level preconditions](#request-level-preconditions)).

The server validates each commit against authoritative namespace state and can reject invalid commits immediately. A tentatively accepted commit becomes durable when put-if-absent of the next numbered WAL object succeeds. If put-if-absent loses a race, nothing is written, and the writer reads the new tip, re-plans, and retries. If the WAL put's outcome isn't observed (such as a transport failure mid-request), the server returns `outcome_unknown`, and the caller reconciles using [Commit responses and safe retry](#52-commit-responses-and-safe-retry). A server may physically batch several logical commits into one WAL object, provided it preserves each commit's idempotency, ordering, and change-feed identity.

Applications can use commit messages or annotations to correlate multiple logical commits in a higher-level workflow (for example, with fields such as `operation_id`, `operation_kind`, or `operation_part`).

The change feed returns committed changes in sequence order after an explicit `after_seq` cursor, bounded by `limit`. Truncated pages return `next_after_seq` for the next request. If a client's cursor falls behind the retention floor, the client MUST re-bootstrap rather than expecting pruned incremental history.

### Request-level preconditions

The optional `preconditions` array defaults to empty and can contain at most 1,024 entries (`commit.max_preconditions`). Longer arrays return `invalid_request` before planning.

Request-level preconditions are evaluated against the candidate's "pre-state": the namespace state including earlier admitted candidates in the same batch, before any of this candidate's own operations run. Its head sequence is the sequence of the last admitted commit, or the batch's base head sequence for the first candidate.

| Precondition | What it checks | Error on failure |
| --- | --- | --- |
| `namespace_head` | Namespace head sequence | `stale_head` |
| `file_revision` | Inode's current content revision | `stale_revision` |
| `path_binding` | Inode at a path, and optionally its binding version | `path_conflict` or `binding_version_mismatch` |
| `inode_binding` | Inode's binding version | `binding_version_mismatch` |
| `path_absence` | No visible entry resolves at a path | `path_conflict` |
| `name_absence` | Parent has no visible child with a name | `path_conflict` |
| `attributes_revision` | Inode's attribute revision | `stale_attributes` |
| `access_revision` | Inode's access revision | `stale_access` |

Exact rules for each precondition:

- **`namespace_head`.** Requires `expected_head_seq` to equal the pre-state head sequence. Any intervening commit (even unrelated) invalidates it and returns `stale_head` with `expected_head_seq` and `actual_head_seq`.
- **`file_revision`.** Requires a visible `inode_id` whose current content revision equals `expected_revision_no`. Any new revision (including a restore) or deletion invalidates it and returns `stale_revision` (with `actual_revision_no` absent if the inode isn't visible or has no current revision).
- **`path_binding`.** Requires an absolute `path` and `expected_inode_id` (missing or null `expected_inode_id` is a decode error). Passes only when `path` resolves to that inode. A different inode or unbound path returns `path_conflict` with `expected_inode_id` and `actual_inode_id` (absent when unbound). Without `expected_binding_version`, moving an inode away and back passes again, while supplying `expected_binding_version` catches that and fails with `binding_version_mismatch`. Checking `/` against `ino_00000000000000000000000000` passes without a binding version, while supplying one for the root returns `invalid_request`.
- **`inode_binding`.** Requires `inode_id` and `expected_binding_version`. Passes when the inode is visible at that binding version, regardless of its current path. A mismatch returns `binding_version_mismatch` (with `actual_binding_version` absent when not visible). A malformed token, a foreign-namespace token, or the root inode returns `invalid_request`.
- **`path_absence`.** Requires only an absolute `path` (rejecting inode or binding version fields). Passes when no visible entry resolves at `path`, including when an ancestor is missing or a parent component isn't a directory. A bound path returns `path_conflict` with `actual_inode_id` set and `expected_inode_id` absent (`/` fails with `ino_00000000000000000000000000`).
- **`name_absence`.** Requires `parent_inode_id` and `display_name`. Passes when the parent has no visible child matching the name key, including when the parent isn't visible or isn't a directory. A bound name returns `path_conflict` with `actual_inode_id` set and `expected_inode_id` absent.
- **`attributes_revision`.** Requires a visible `inode_id` whose attribute revision equals `expected_attributes_revision_no`. Attribute updates and deletions invalidate it, while content-only writes don't. Failure returns `stale_attributes` (`actual_attributes_revision_no` absent if not visible).
- **`access_revision`.** Requires a visible `inode_id` whose access revision equals `expected_access_revision_no`. Any access update invalidates it and returns `stale_access` (`actual_access_revision_no` absent if not visible).

The server evaluates a commit request in strict order:

1. Check the commit receipt for `commit_id`. If an identical request already committed, return the original commit even if its preconditions are now stale. If `commit_id` was used with different preconditions, return `commit_id_reuse_conflict`.
2. Evaluate `preconditions` in array order. The first failure returns that precondition's error with zero-based `precondition_index`.
3. Plan and run `operations` in order.

A failed precondition reserves no sequence or inode and leaves the planning view untouched. If a numbered WAL put loses a race, retries re-evaluate preconditions against the new tip. Preconditions are admission-time guards: they enter durable storage only through the commit fingerprint and are never re-evaluated during replay.

### Actor attribution

Every commit, upload operation, namespace creation, and namespace fork requires an actor ID in `Loonfs-Actor` (see [Identity headers](#identity-headers)). The application supplies a stable, opaque identifier of 1 to 256 visible ASCII characters (0x21 through 0x7E). LoonFS records it on commits, metadata created by those commits, upload sessions, and `created_by` on created or forked namespaces.

LoonFS doesn't authenticate actors or resolve user profiles. The application MUST authenticate users and authorize requests before calling LoonFS, and SHOULD pass a stable internal ID rather than an email address or display name.

`Loonfs-Actor` feeds into the commit fingerprint as `actor_id`, so reusing a `commit_id` with a different actor returns `commit_id_reuse_conflict`. The commit timestamp isn't part of the fingerprint.

Responses expose attribution through these fields:

| Field | Meaning |
| --- | --- |
| `created_by`, `created_at_ms` | Commit attribution for inode creation. |
| `created_by`, `created_at_ms` on a namespace | Actor and time for namespace creation or fork. |
| `revision_committed_by`, `revision_committed_at_ms` | Commit attribution for a file's current revision on stat and list entries (absent on directories). |
| `commit_id` | Commit identity on revision-history items and committed changes. |
| `committed_by`, `committed_at_ms` | Commit attribution on revision-history items and committed changes. |
| `attributes_updated_by`, `attributes_updated_at_ms` | Commit attribution for the latest attribute update (absent at initial empty revision 0). |
| `deleted_by`, `deleted_at_ms` | Commit attribution for an active trash entry. |

### Subject and principals

Every request that reads or mutates a namespace's contents MUST explicitly declare whose authority it acts under, using exactly one of two headers:

- **`Loonfs-Principals`.** Comma-separated principal IDs without whitespace (capped by `access.max_principals_per_request`). The request acts as a "subject" whose effective rights are the union of grants to those principals, checked against the namespace's access rows (see [Authorization](#authorization)).
- **`Loonfs-Context: service`.** Places the request in the "service context", acting on the application's own account with the full authority of its deployment token and bypassing per-inode access checks (for example, for indexers, migrations, or applications without per-user permissions).

Requiring an explicit authority header prevents privilege escalation bugs: because an application's backend shares one deployment token across all users, defaulting an omitted header to the service context would silently grant full access whenever a client forgot to pass principals. With two explicit headers, omitting both returns `invalid_request`.

Principal IDs never contain commas, so lists parse unambiguously. Like actor IDs, principal IDs are opaque and appear in access logs, so applications should use internal IDs rather than emails or display names.

Principals determine a request's rights, while the actor is its identity. LoonFS includes the actor in the commit fingerprint and records the actor as the upload session owner, whereas principals are part of neither. Reusing a `commit_id` from a different actor returns `commit_id_reuse_conflict`, while a retry from the same actor (while the receipt is retained) replays the original commit without re-checking the actor's current rights, because the write is already committed and reveals nothing the actor didn't author.

Embedded callers and Rust HTTP clients choose their authority context when building the runtime or client (either service context or a set of principals), with no default so user-scoped handles can't accidentally run in the service context. A multi-tenant host builds its runtime in the service context and clones it into each request's context. Every read and commit uses the handle's context. The embedded runtime's commit and upload methods take the actor as an explicit argument; the HTTP client takes it at construction and `Client::with_actor` clones the client with another. The CLI saves the actor in its upload journal alongside the request and refuses to resume under a different actor.

### Identity headers

Three headers supply identity and authority information, and any single request sends at most two of them: `Loonfs-Actor` (1 to 256 visible ASCII characters, 0x21 through 0x7E) plus either `Loonfs-Principals` or `Loonfs-Context`. All three are optional in the transport schema, and the server enforces per-operation requirements when handling requests:

| Operations | `Loonfs-Actor` | `Loonfs-Principals` or `Loonfs-Context` |
| --- | --- | --- |
| `create_commit`, `merge_fork`, `replay_fork` | Required. Recorded as `committed_by`. | Required. |
| `create_namespace` | Required. Recorded as `created_by`. | Not read. |
| `fork_namespace`, `fork_namespace_for_rebase` | Required. Recorded as `created_by`. | Required. A request with principals needs an administrator of the source. |
| `delete_namespace`, `create_snapshot`, `list_snapshots`, `get_snapshot`, `extend_snapshot`, `delete_snapshot`, `list_changes`, `list_diff`, `get_next_merge_range` | Not required. | Required, with the same administrator rule as `fork_namespace`. |
| `create_upload`, `put_upload_content`, `sign_upload_parts`, `complete_upload`, `get_upload`, `abort_upload` | Required. Recorded as the owner of the session. | Required. |
| Path and inode reads, downloads, `list_trash`, and `grep` | Not required. | Required. |
| `run_maintenance_job` | Not recorded, but a malformed value is rejected. | Not read. |
| `get_capabilities`, `get_namespace`, `list_namespaces`, the other maintenance operations, and the health, readiness, and metrics routes | Not read. | Not read. |

Validation rules for operations that read these headers:

- Sending neither `Loonfs-Principals` nor `Loonfs-Context` returns `invalid_request` with `param` set to `Loonfs-Principals` and message `send Loonfs-Principals or Loonfs-Context: service`.
- Sending both returns `invalid_request` with `param` set to `Loonfs-Context`.
- The only valid value for `Loonfs-Context` is lowercase `service`, and any other value returns `invalid_request` with `param` set to `Loonfs-Context`.
- Missing a required `Loonfs-Actor` returns `invalid_request` with `param` set to `Loonfs-Actor` and message `missing required header Loonfs-Actor`. A malformed actor returns `invalid_request` with the same `param` and the validator's reason. Authorization runs before header validation.

Header names are case-insensitive (HTTP/2 lowercases them). Retries and replayed requests MUST send the exact same header values, so saved requests MUST persist the actor and authority context alongside the body. Because headers appear in access logs, values MUST be opaque identifiers rather than email addresses or display names.

### 5.2 Commit responses and safe retry

Every successful commit returns a `Commit` object containing `namespace_id`, `commit_id`, `committed_seq`, `committed_by`, `committed_at_ms`, optional `message`, and `events`. These values match what the change feed returns, including newly created inode IDs.

When a commit is a [merge](glossary.md#merge) (applying a fork's commits to its parent namespace), the response also includes `merge` (absent on all other commits):

| Field | Meaning |
| --- | --- |
| `source_namespace_id` | The fork whose commits were applied. |
| `source_after_seq` | Exclusive lower bound in the fork's history (for a first merge, the fork's `fork_basis.fork_point_seq`). |
| `source_through_seq` | Inclusive upper bound in the fork's history. |

This range matches the change feed's cursor convention: reading the fork's change feed with `after_seq={source_after_seq}` through `source_through_seq` returns the exact commits applied by the merge.

To retry a commit safely (for example, after `outcome_unknown`, a transport error, or a process restart), the client resubmits the identical request with the same `commit_id`. There's no separate commit-status endpoint:

- If the original request committed, LoonFS replays the original response (with the original `committed_seq` and `events` from the retained commit record) without making a new commit.
- If the original request didn't commit, this attempt can commit normally.
- If `commit_id` was used for a different request (including changed preconditions, bytes, or actor), the retry returns `commit_id_reuse_conflict`, reporting the original `committed_seq` when resolved from a receipt.

LoonFS serves a retry from its receipt before checking commit limits. A previously committed request still replays if a later release lowers a limit below it, and an over-limit request reusing an existing ID returns `commit_id_reuse_conflict` rather than `invalid_request`.

A merge's identity is its fork and commit range rather than its generated operations (since LoonFS plans merge operations itself). Before reading the merge index, the merge route checks the receipt for `commit_id` and returns the original merge if one already committed (otherwise a retry after success would compute the next range and conflict). Reusing a merge's `commit_id` for a different range returns `commit_id_reuse_conflict`. A merge with a new `commit_id` must continue right after the fork's previous merge, and overlapping or gapped ranges return `merge_range_conflict`, ensuring each fork commit merges at most once.

These idempotency guarantees apply while the [commit receipt](glossary.md#commit-receipt) is retained. Once the retention floor passes the commit and compaction rebuilds the metadata runs, the receipt is removed and the `commit_id` can be used again. A very late retry past that window might re-apply the operation (for example, appending a duplicate revision), so callers retrying outside the retention window MUST inspect namespace state first.

File content is part of a commit's identity, so what a client must keep across retries depends on how the content was supplied:

| Content supplied with the commit | What to retain for a retry |
| --- | --- |
| Uploaded content | The original `content_ref`. Re-uploading the same bytes creates a new content object whose reference conflicts with the original `commit_id` (`commit_id_reuse_conflict`). |
| Inline content | The original bytes. Different bytes return `commit_id_reuse_conflict`, even if the size matches. |

Inline retry rules still apply even if the server writes inline bytes to a separate content object to stay within WAL limits. Switching between inline bytes and an uploaded `content_ref` changes commit identity, even for identical bytes. If the original commit succeeded and its receipt is still retained, retrying the exact request returns the original commit without re-uploading bytes, even after a restart or on another server.

For reliable retries across both inline and uploaded files, clients prepare content once, save the result, and pass it to the prepared-commit method with the same explicit `commit_id`, path, actor, and options on every attempt:

| Client | Prepare content | Publish retained content |
| --- | --- | --- |
| Rust HTTP and embedded runtime | `prepare_content()` / `prepare_content_stream()` | `put_file_prepared()` |
| Python synchronous client | `files.prepare()` | `files.upload_prepared()` |
| Go | `Files.Prepare()` | `Files.UploadPrepared()` |
| TypeScript | `files.prepare()` | `files.uploadPrepared()` |

Preparation chooses inline versus uploaded representation once:

- In the embedded runtime, preparing bytes at or below the inline threshold makes no storage request.
- HTTP clients prepare inline content when the server advertises `filesystem.commits.inline_content` and the bytes fit within `commit.max_inline_content_bytes_per_operation` (Go, Python, and TypeScript helpers cap inline preparation at 64 KiB even if the server advertises more). Streams use bounded lookahead of the effective limit plus 1 byte, falling back to upload while preserving that prefix if larger.
- The Rust HTTP client caches capabilities, so small files need only the commit request once cached. Generated SDK helpers still fetch capabilities during preparation, skipping upload creation, transfer, and completion for inline content.
- In Go, Python, and TypeScript, preparation returns a `PreparedFile` union of staged `PreparedContent` or `InlinePreparedContent`. Callers inspecting `content_ref` or token fields MUST narrow to the staged variant first, while callers passing the value straight to `upload_prepared`, `UploadPrepared`, or `uploadPrepared` need no transport branching. Inline values store immutable bytes (base64 in Go and TypeScript) with no uploaded reference before publication. Callers that explicitly need a completed upload can use the lower-level uploads API.
- Publication always uses the retained variant without re-checking capabilities or falling back to another representation. Prepared inline bytes never expire, while completed uploads keep their normal expiry (preparation doesn't extend it). Preparation alone doesn't make a file visible.

Whole-file convenience helpers (`files.upload`, `files.uploadStream`, `files.upload_stream`, `Files.Upload`, `Files.UploadStream`, `put_file()`, and `put_file_stream()`) prepare content on every call. Small inline files can replay if re-prepared identically, whereas larger files (or files prepared without inline writes) create new upload objects that conflict on `commit_id` reuse, and capability changes can switch representations between calls. Using the preparation methods and persisting the full commit request avoids those hazards (the remote CLI saves the request before submission so retries survive process restarts). Omitting `commit_id` in a helper generates a fresh ID on each call.

At the WAL write-stop threshold, new commits wait in the writer's queue for a fold to free space. If the write-stop wait expires first, the commit returns `maintenance_required` without committing. (The reference server sets the write-stop wait to half of `request_deadline_ms` so queued commits return `maintenance_required` cleanly instead of timing out with `outcome_unknown`.) Replays of already-committed IDs still return their receipts, subject to normal writer-session, availability, and corruption checks.

The Rust HTTP client automatically retries operations marked `idempotent` or `replayable` in [Operations and retry classes](#operations-and-retry-classes) and makes one attempt for `not_idempotent` operations, presigned direct PUTs, and streamed request or response bodies (since streams can't be replayed).

### 5.3 Writer topology and fencing

Each namespace has at most one active [writer session](glossary.md#writer-session). Multiple concurrent clients submit commits through that session, while independent readers scale separately. The reference server uses one service-level writer session per active namespace to coordinate all client writes.

Replacing an active writer requires no handshake or lease. A new writer acquires the next [writer epoch](glossary.md#writer-epoch) on its first publication, and epoch fencing stops the displaced session from corrupting state: the old session's next publish fails terminally with `writer_fenced`. The error's `details` identify the new epoch and writer so operators can distinguish a planned failover from two servers misconfigured to write the same namespace.

The host owns writer session lifetimes:

- Opening a namespace for writing creates a session without writing anything durable, and the session stays open until the host closes it, drops it, or another writer fences it. The runtime never opens or evicts sessions automatically and imposes no session count cap.
- To hand a namespace off cleanly, a node closes the session. Closing immediately rejects new work with `writer_session_closed`, drains already-admitted commits, and writes nothing durable. The next node acquires the next epoch on its first publish.

In the reference server, a namespace's writer session opens on the first write request to that namespace. The maintenance scheduler closes an idle session when all four conditions are met:

- Its last open is older than `idle_session_close_after_ms` (1,800,000 ms, or 30 minutes, by default).
- No in-flight request keeps a clone of the session handle.
- Metadata (and the grep index, if enabled) is caught up with the session's published sequence (a session that never published still waits for an enabled index to finish building).
- No collection step is due.

A server whose maintenance mode is `serve_only` or `disabled` runs no metadata, collection, or index step and no sweep, because folds, compactions, sweeps, and collections are another process's work. Its scheduler still checks its sessions at every maintenance interval and closes a session on the first two conditions alone, so a session that can't write anymore closes as soon as no request keeps its handle, and an idle session closes after `idle_session_close_after_ms` whether or not its metadata is caught up. The close writes nothing and sends no store requests. The session's unfolded WAL tail stays in the store for the maintaining process to fold.

While the scheduler is closing a session, a new open for the same namespace waits for the drain and then opens a fresh session (never failing with `writer_session_closed` on this path), while opens for other namespaces don't wait.

The server also drops a session when the namespace is deleted through that server, or on the next open after a session records that it was fenced or found its namespace deleted or missing. When a session is fenced:

- The request that hits the fence fails with `writer_fenced` and isn't resubmitted.
- The session (including all in-flight handle clones) can never publish again.
- While in-flight work on the fenced session drains, a new open can return `writer_session_closed` (503). Once that work finishes, a later request opens a fresh session whose first publish reclaims the namespace.
- Fencing one namespace doesn't affect sessions for other namespaces, and each dropped fenced session logs a warning with the namespace ID.

Sustained fencing of one namespace indicates split-brain routing and should be resolved by routing the namespace to a single writer. Neither a `retention` job nor the reference server's sweep fences an active writer, because both record the new floor in a "[floor object](glossary.md#floor-object)" (a WAL object containing no commits) without claiming a writer epoch (see [Retention jobs](#retention-jobs)).

[Operations and WAL deltas](format.md#66-operations-and-wal-deltas) defines the standard mutation operations, and [`POST /commits`](#68-post-commits) exposes them over HTTP with identical identity, durability, and visibility semantics.

## 6. The HTTP API

HTTP is one transport for the abstract operations defined earlier in this document and doesn't define separate semantics.

Route paths follow two conventions:

- `GET` routes identify resources with nouns (`entry`, `entries`, `content`, `revisions`, `trash`).
- `POST` routes end in a verb when invoking an action rather than creating a resource (`enable`, `disable`, `abort`, `complete`, `probe`).

`/v0/maintenance/` is the only API-group path prefix, covering every route in `maintenance/v0`. All other routes are grouped by resource, including `GET /v0/namespaces/{ns}/grep` (which belongs to `query/v0`) and all remaining routes in this section (which belong to `filesystem/v0`).

Operation IDs start with a verb: `get` reads one resource, `list` reads a page, `create` posts a new resource to a collection, and action operations use a direct verb (`grep`, `run_maintenance_job`, `extend_snapshot`). Operation IDs are wire-registry identifiers rather than SDK method names. Every operation MUST declare its SDK placement with OpenAPI extensions (`x-fern-sdk-group-name` and `x-fern-sdk-method-name`), or declare `x-fern-ignore` to omit it from generated SDKs.

Examples in this section are representative rather than exhaustive. Responses may add fields within v0, so clients MUST ignore unrecognized JSON fields. Absent optional response fields are omitted and never encoded as `null`.

### Authentication and transport

When a deployment configures an auth token, every request authenticates with an HTTP bearer credential:

```
Authorization: Bearer <auth_token>
```

After request admission, missing or invalid credentials return 401 `unauthorized`. The server authenticates before parsing headers, query parameters, or bodies so validation errors never leak information to unauthorized callers. As a result, a malformed request without valid credentials returns 401 rather than 400.

`GET /health` and `GET /readiness` are exempt from authentication so load balancers can probe them. Every other route (including `GET /v0/capabilities`) requires the token. In `openapi.json`, this is expressed as a global `bearer_auth` requirement overridden on those two probe operations.

The identity headers (`Loonfs-Actor`, `Loonfs-Principals`, and `Loonfs-Context`) are optional in the transport schema and enforced per operation as listed in [Identity headers](#identity-headers).

Request and response decoding follow asymmetric evolution rules:

- **Strict request decoding.** The server rejects unknown fields at any nesting depth in a JSON body with 400 `invalid_request`. Because many request fields are optional preconditions, ignoring an unrecognized field would silently run a mutation without a misspelled guard.
- **JSON media type.** Any request sending a JSON body MUST set `Content-Type` to `application/json` or another `+json` media type (otherwise 400 `invalid_request`). Requests with optional bodies omit the header when sending no body.
- **Strict query parameters.** Unknown query parameters return 400 `invalid_request` (for example, `DELETE /v0/namespaces/{ns}?expected_head_sq=418` fails instead of deleting without `expected_head_seq`). Routes declaring no query parameters reject all query parameters, except `GET /health`, `GET /readiness`, and `GET /metrics` (since probes and scrapers may append parameters).
- **Tolerant response decoding.** Clients MUST ignore unknown JSON fields in responses so older clients keep working against newer servers ([Writer and client requirements](#72-writer-and-client-requirements)).
- **Closed transfer shapes.** `ContentRef`, `Checksum`, and `ContentToken` are closed in both requests and responses (`additionalProperties: false`). New content strategies add a `kind` value, new checksum algorithms add an `algorithm` value, and `ContentToken` encodes new data inside its opaque `token` string. Unknown `kind` or `algorithm` values fail decoding ([Strict decoding and evolution](format.md#123-strict-decoding-and-evolution)).

The auth token and every presigned URL returned by upload or download routes act as bearer credentials (for example, anyone with a presigned direct-upload URL can write to the bucket). Deployments reachable beyond localhost MUST serve `https`, terminated either by LoonFS or by a proxy in front of it.

### Operations and retry classes

Every OpenAPI operation declares retry, header, and scope metadata for generated SDKs and deployment proxies:

- **`x-loonfs-retry`.** One of `idempotent` (safe to repeat), `replayable` (repeating returns the original result, such as commits by `commit_id` or upload completion), or `not_idempotent` (generated clients make one automatic attempt and leave recovery to the application).
- **`x-loonfs-actor` and `x-loonfs-authority`.** Set to `required` on operations requiring `Loonfs-Actor` or requiring `Loonfs-Principals` / `Loonfs-Context` (see [Identity headers](#identity-headers)). Omitted when not required.
- **`x-loonfs-scope`.** Classifies every operation except `get_health`, `get_readiness`, and `get_capabilities` (which are open to all credentials) by its effect on the store rather than its HTTP verb. Scopes don't imply one another, and the server doesn't enforce them itself (proxies or scoped-token wrappers read them from OpenAPI). `read` covers all `GET` routes except `get_upload`, `/metrics`, `/v0/maintenance/` routes, and the three unscoped routes, plus creating downloads (since downloads read file bytes). `write` covers namespace mutations, creating namespaces, upload sessions, forks, merges, rebases, and `get_upload` (since reading an open `direct_put` session mints a new presigned upload URL). `admin` covers deleting namespaces, creating, extending, or deleting snapshots (since snapshots prevent compaction across the store), `/v0/maintenance/` routes (including maintenance `GET` routes), and scraping `/metrics`.

The table below lists every v0 operation, its retry class, and its representative HTTP route:

| Purpose | Operation ID | Retry class | Representative HTTP shape |
| --- | --- | --- | --- |
| Check server health | `get_health` | `idempotent` | `GET /health` |
| Check server readiness | `get_readiness` | `idempotent` | `GET /readiness` |
| Read deployment capabilities | `get_capabilities` | `idempotent` | `GET /v0/capabilities` |
| Create a namespace | `create_namespace` | `not_idempotent` | `POST /v0/namespaces`. Requires `Loonfs-Actor`, optional `namespace_id` and `retention_window_ms`. Re-sending with matching options finishes an initializing namespace (see [Creating and forking namespaces](#creating-and-forking-namespaces)). |
| List namespaces | `list_namespaces` | `idempotent` | `GET /v0/namespaces?limit=100&cursor=...` (`limit` defaults to 100, see [Listing namespaces](#listing-namespaces)) |
| Read a namespace | `get_namespace` | `idempotent` | `GET /v0/namespaces/{ns}?consistency=strong` (`consistency` and `max_staleness_ms` are optional) |
| Read a path entry | `get_path_entry` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/entry?path=/docs/report.txt&include_attributes=true&snapshot_id=...` (`include_attributes` defaults to `false`, `snapshot_id` is optional) |
| Read an inode | `get_inode` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}?include_attributes=true&snapshot_id=...` (`include_attributes` defaults to `false`, `snapshot_id` is optional) |
| List path entries | `list_path_entries` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/entries?path=/docs&limit=100&cursor=...&include_attributes=true&recursive=true&snapshot_id=...` (`include_attributes` and `recursive` default to `false`, `snapshot_id` is optional) |
| List directory children by inode | `list_inode_children` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}/children?limit=100&cursor=...&include_attributes=true&recursive=true&snapshot_id=...` (`include_attributes` and `recursive` default to `false`, `snapshot_id` is optional) |
| List file revisions by path | `list_file_revisions` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/revisions?path=/docs/report.txt&limit=100&cursor=...` |
| List file revisions by inode | `list_file_revisions_by_inode` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}/revisions?limit=100&cursor=...` |
| Read current or prior file content by path | `get_file_content` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/content?path=/docs/report.txt&snapshot_id=...` (`revision_no` and `snapshot_id` are optional and mutually exclusive) |
| Read current file content by inode | `get_file_content_by_inode` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}/content?snapshot_id=...` (`snapshot_id` is optional) |
| Read prior file content by inode | `get_file_revision_content_by_inode` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{revision_no}/content` |
| Start a download by path | `create_download` | `idempotent` | `POST /v0/namespaces/{ns}/filesystem/downloads` with `path`, optional `revision_no`, optional `snapshot_id` (mutually exclusive with `revision_no`), and optional `start_offset` (default 0) |
| Start a current download by inode | `create_download_by_inode` | `idempotent` | `POST /v0/namespaces/{ns}/inodes/{inode_id}/downloads` with optional `snapshot_id` and `start_offset` (default 0) |
| Start a revision download by inode | `create_revision_download_by_inode` | `idempotent` | `POST /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{revision_no}/downloads` with optional `snapshot_id` and `start_offset` (default 0) |
| List the trash | `list_trash` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/trash?limit=100&cursor=...` |
| Create a commit | `create_commit` | `replayable` | `POST /v0/namespaces/{ns}/commits`. Requires `Loonfs-Actor`. |
| Create an upload session | `create_upload` | `not_idempotent` | `POST /v0/namespaces/{ns}/uploads`. Returns the open session. |
| Upload content through the server | `put_upload_content` | `idempotent` | `PUT /v0/namespaces/{ns}/uploads/{upload_id}/content`. Returns the open session with staged `content_ref`. |
| Create multipart upload URLs | `sign_upload_parts` | `idempotent` | `POST /v0/namespaces/{ns}/uploads/{upload_id}/parts` |
| Complete an upload | `complete_upload` | `replayable` | `POST /v0/namespaces/{ns}/uploads/{upload_id}/complete` |
| Read an upload session | `get_upload` | `idempotent` | `GET /v0/namespaces/{ns}/uploads/{upload_id}`. Completed sessions return a fresh `content_token`. |
| Abort an upload session | `abort_upload` | `idempotent` | `POST /v0/namespaces/{ns}/uploads/{upload_id}/abort` (terminal and repeatable, rejected on completed sessions) |
| Read committed changes | `list_changes` | `idempotent` | `GET /v0/namespaces/{ns}/changes?after_seq=123&limit=100&snapshot_id=...` (`snapshot_id` is optional) |
| Read the differences between two sequences | `list_diff` | `idempotent` | `GET /v0/namespaces/{ns}/diff?from_seq=418&to_seq=425&path=/docs&after_seq=418&limit=100` (`to_seq`, `path`, and `after_seq` are optional) |
| Create a snapshot | `create_snapshot` | `not_idempotent` | `POST /v0/namespaces/{ns}/snapshots`. Accepts optional `name` and `ttl_ms`. |
| List snapshots | `list_snapshots` | `idempotent` | `GET /v0/namespaces/{ns}/snapshots?limit=100&cursor=...` |
| Read a snapshot | `get_snapshot` | `idempotent` | `GET /v0/namespaces/{ns}/snapshots/{snapshot_id}` (missing, expired, or deleted returns `snapshot_not_found`, deleted namespace returns `namespace_deleted`) |
| Extend a snapshot | `extend_snapshot` | `idempotent` | `POST /v0/namespaces/{ns}/snapshots/{snapshot_id}/extend`. Requires `ttl_ms` and never shortens expiry. |
| Delete a snapshot | `delete_snapshot` | `idempotent` | `DELETE /v0/namespaces/{ns}/snapshots/{snapshot_id}` (missing IDs succeed) |
| Fork a namespace | `fork_namespace` | `not_idempotent` | `POST /v0/namespaces/{source_ns}/forks`. Requires `Loonfs-Actor`. |
| Merge a fork | `merge_fork` | `replayable` | `POST /v0/namespaces/{ns}/merges`. Requires `Loonfs-Actor`. |
| Read where a fork's next merge starts | `get_next_merge_range` | `idempotent` | `GET /v0/namespaces/{ns}/merges?source_namespace_id=demo-branch` |
| Create a rebase's new fork | `fork_namespace_for_rebase` | `not_idempotent` | `POST /v0/namespaces/{ns}/rebases`. Requires `Loonfs-Actor`. |
| Replay a fork into a rebase's new fork | `replay_fork` | `replayable` | `POST /v0/namespaces/{ns}/replays`. Requires `Loonfs-Actor`. |
| Delete a namespace | `delete_namespace` | `not_idempotent` | `DELETE /v0/namespaces/{ns}?expected_head_seq=418` (`expected_head_seq` is optional) |
| Read namespace diagnostics | `get_namespace_diagnostics` | `idempotent` | `GET /v0/maintenance/namespaces/{ns}/diagnostics` |
| List snapshot records | `list_snapshot_records` | `idempotent` | `GET /v0/maintenance/namespaces/{ns}/snapshots?limit=100&cursor=...` |
| Read a snapshot record | `get_snapshot_record` | `idempotent` | `GET /v0/maintenance/namespaces/{ns}/snapshots/{snapshot_id}` (returns any snapshot in the inventory, missing ID returns `snapshot_not_found`) |
| Run one maintenance job | `run_maintenance_job` | `not_idempotent` | `POST /v0/maintenance/namespaces/{ns}/jobs`. Body selects one job via `kind`. |
| Run one maintenance job on a pool | `run_pool_maintenance_job` | `not_idempotent` | `POST /v0/maintenance/pools/{pool_id}/jobs`. Body selects one job via `kind` (`gc` is the only pool job). |
| Probe the store contract | `probe_store` | `not_idempotent` | `POST /v0/maintenance/store/probe`. Body sets probe options (`{}` uses defaults). |
| Search file contents | `grep` | `idempotent` | `GET /v0/namespaces/{ns}/grep?pattern=needle&case_insensitive=false&path=%2Fsrc&allow_scan=false&allow_stale=false&limit=100&cursor=...`. Requires `query.grep` and an active index. |
| Read grep index status | `get_grep_index` | `idempotent` | `GET /v0/maintenance/namespaces/{ns}/grep/index` |
| Enable the grep index | `enable_grep_index` | `idempotent` | `POST /v0/maintenance/namespaces/{ns}/grep/index/enable` |
| Disable the grep index | `disable_grep_index` | `idempotent` | `POST /v0/maintenance/namespaces/{ns}/grep/index/disable` |
| Scrape metrics | `get_metrics` | `idempotent` | `GET /metrics` (Prometheus text exposition, requires bearer token, see [`GET /metrics`](#619-get-metrics)) |

### Maintenance jobs

#### Grep index

The grep index routes (`get_grep_index`, `enable_grep_index`, `disable_grep_index`) manage a namespace's derived grep index. Unreferenced grep index segments are collected by `gc` jobs (see [Garbage collection jobs](#garbage-collection-jobs)).

If a namespace's grep entry is corrupt, `disable_grep_index` replaces it with a disabled entry so the index can be rebuilt by re-enabling it. If the entry uses another version of the grep format, `disable_grep_index` returns `index_corrupt` and leaves the entry untouched.

All three routes return the same flat grep index object containing `namespace_id`, status-specific lifecycle fields, and `reorganize_pending` (indicating whether a partitioned segment reorganization is underway). No two lifecycle statuses share a sequence field:

| `status` | Fields | Means |
| --- | --- | --- |
| `disabled` | None | No index is maintained (also returned if grep was never enabled). |
| `backfilling` | `captured_seq`, `cursor_inode_id` | Initial walk over files at `captured_seq` is running. Nothing is searchable yet, so `built_through_seq` is omitted. |
| `active` | `built_through_seq`, `next_event_index` | Index follows the change feed. Commits through `built_through_seq` are searchable (when `next_event_index` is non-zero, the remainder of the commit at `built_through_seq` isn't indexed yet). `captured_seq` is omitted. |

Examples:

```json
{"namespace_id":"demo","status":"active","built_through_seq":12,"next_event_index":0,"reorganize_pending":false}
```

```json
{"namespace_id":"demo","status":"backfilling","captured_seq":12,"cursor_inode_id":"ino_000000j000000tz7yc2z8ypbmf","reorganize_pending":false}
```

Because these routes read the current manifest, they reflect the index's last publication (the reference server publishes progress roughly every maintenance interval, 5 seconds by default, and at the end of a run of steps). A client waiting for indexing to catch up should capture a target sequence first and stop once the index reaches it, rather than chasing a moving head.

#### Jobs and outcomes

`POST /v0/maintenance/namespaces/{ns}/jobs` runs one maintenance job selected by `kind`:

| `kind` | Fields | Result |
| --- | --- | --- |
| `metadata` | Optional `max_wal_tail_objects` | `outcome`, `compaction_units`, and optional `skipped` |
| `gc` | None | Collection summary |
| `retention` | Optional `to_seq` or `cutoff_at_ms` | `retention_floor_seq` |
| `retention_window` | Optional `retention_window_ms` | `retention_window_ms` when set (omitting it in the request clears the window) |

Every job runs as the namespace's [maintainer](glossary.md#maintainer) without opening a writer session. Responses echo `kind` and `namespace_id` alongside the job's result, and no job creates a snapshot:

```json
{"kind":"gc"}
```

A job that loses a race with another process or whose work is superseded reports that in its result rather than failing with an error. Deleted and [retired](format.md#95-retirement) namespaces accept only `gc` jobs (all other jobs return `namespace_deleted`).

Pools use a separate route, `POST /v0/maintenance/pools/{pool_id}/jobs`, which supports only `{"kind":"gc"}` (for example, `POST /v0/maintenance/pools/pool_3s0drmj8em84yb4mnetqkz2j0x/jobs`). A namespace `gc` collects objects under the namespace prefix, while a pool `gc` collects content objects, upload records, and temporary objects under the pool prefix (see [Garbage collection jobs](#garbage-collection-jobs)). The response echoes `kind` and `pool_id` plus the collection summary, and an empty or missing pool prefix returns `pool_not_found`. Pool jobs share the retry class and request-deadline exemption of namespace jobs.

A `metadata` job [folds](glossary.md#fold) the WAL tail when due and then [compacts](glossary.md#compaction) metadata in units:

- Each unit compacts a window of up to 8 runs in one [family group](glossary.md#family-group) selected by size tier ([Compaction windows](format.md#102-compaction-windows)) and publishes the next manifest. If the manifest put loses a race and the unit's input runs are unchanged, the unit retries over the winning manifest.
- A unit is abandoned if an input run changed, its elapsed-time bound expired, or all publication attempts lost (leaving any newly written segments unreferenced for later GC).
- The job stops early after publishing 16 units, after 3 consecutive abandoned units, or if another runtime claims the namespace's [compactor epoch](glossary.md#compactor-epoch).
- The server runs at most one fold and at most one compaction unit per namespace at a time. If one is already running (for example, started by the writer session), the job waits for it instead of duplicating work.

The `metadata` response reports `compaction_units` (units published by this call) and `outcome`:

- `caught_up`: nothing is due, and nothing can become due without a new commit.
- `due`: more work remains or couldn't be confirmed (for example, the job stopped early, or the WAL tail is below the fold threshold and hasn't gone idle yet). Callers run another `metadata` job later.

```json
{"kind":"metadata","namespace_id":"demo","outcome":"caught_up","compaction_units":2}
```

If any published unit hit a limit and skipped part of its work, the response includes `skipped` (each reason listed once in order of first encounter, omitted when empty, and clients MUST tolerate unknown values):

| Reason | What the job skipped | What the operator can do |
| --- | --- | --- |
| `expired_row_removal_walk_limit` | A base compaction skipped removing rows hidden by an expired deletion because walking kept inodes would have required more than 4,096 concurrent directory cursors ([Row retention during a base compaction](format.md#103-row-retention-during-a-base-compaction)). | Move part of the deeply nested tree closer to the root. The next base compaction of each family group removes the hidden rows once the walk fits. |

Because compaction units can take minutes, the reference server exempts maintenance jobs from its request deadline. Clients send `run_maintenance_job` once (it's `not_idempotent`) and schedule another call after a `due` outcome.

In a `metadata` job, `max_wal_tail_objects` overrides the WAL-object fold threshold while the WAL-byte threshold and idle fold still apply (0 or values above the write-rejection threshold return `invalid_request`). A tail below the count threshold still folds once its newest commit is at least as old as the server's idle period (15 minutes by default, measured on the server's clock, unless disabled, see [Recovery material and maintenance policy](format.md#73-recovery-material-and-maintenance-policy)). Metadata jobs never prune replay history, which requires `kind: "retention"`.

#### Retention jobs

A `retention` job advances the namespace's retention floor to the manifest's folded head unless one target is supplied (supplying both returns `invalid_request`):

- `to_seq`: advances to that sequence, clamped at the manifest's folded head.
- `cutoff_at_ms`: Unix timestamp in milliseconds. The server scans commits upward from the current floor and stops at the first commit with `committed_at_ms` later than the cutoff (because commit timestamps come from writer clocks and aren't guaranteed to be monotonic).

Regardless of target, the floor never advances past any commit younger than 24 hours on the server's clock (the [grace window](glossary.md#grace-window)). Because a client reading a file revision can keep reading after a newer revision is committed, keeping the floor 24 hours behind new commits leaves readers at least 24 hours to finish before base compaction removes the replaced revision and GC reclaims its content ([Content roots](format.md#119-content-roots)). The server enforces this as a second cutoff 24 hours before its clock (taking the earlier cutoff when `cutoff_at_ms` is also set), treating future-dated commits as younger than 24 hours and stopping at the first young commit even if a later commit is older. For example, if head is 40 and commits 38 to 40 are less than 24 hours old, a job with no target stops at 37. Targets at or below the current floor change nothing ([Advancing the retention floor](format.md#101-advancing-the-retention-floor)).

Advancing the retention floor governs both revision history and trash expiration:

- **Revision history.** Revisions above the floor are complete, while at or below the floor each file keeps only its newest revision. Base compaction removes older revisions (ending revision listings at that newest revision and making older reads return `revision_not_found`). Restoring a revision at or below the floor succeeds only for the file's newest revision at or below the floor (older ones return `revision_not_found` even before compaction runs). Deleted files keep their newest revision while in the trash so restore still works. GC reclaims removed revisions' content once unreferenced and past the grace window.
- **Trash expiration.** Once the floor reaches or passes a deletion's `deletion_seq`, that deletion expires and leaves the trash (`restore_deleted` returns `not_deleted`). Expiration produces no commit and no change-feed event. Later base compactions of each family group remove the hidden rows, allowing GC to reclaim their content.

To record a new floor, the job writes a [floor object](glossary.md#floor-object) (a WAL object with no commits) at the next WAL number. If the server hosts a writer session for the namespace, the floor object queues behind earlier admitted commits and future commits validate against the new floor. Otherwise, the job writes the floor object directly without opening a session. Because a floor object claims no [writer epoch](glossary.md#writer-epoch), it never fences a writer in another process (that writer simply collides on the WAL number and re-plans against the new floor). If no manifest records the new floor afterward, the job runs a fold so the resulting manifest records it. If the floor wouldn't move, the job writes nothing and reports the floor it planned from. Otherwise, `retention_floor_seq` in the response reports the namespace view's floor after the job (the manifest floor raised by any tail floor objects).

A `retention_window` job sets the namespace's [retention window](glossary.md#retention-window) to `retention_window_ms`, or clears it when the field is omitted. Values shorter than the 24-hour grace window (including 0) return `invalid_request` with `param` `/retention_window_ms`. The job publishes a manifest changing only the window (or publishes nothing if unchanged) and doesn't advance the floor itself. The reference server's sweep uses the new window on its next run and skips namespaces with no window (see [Deleting, retaining, and reclaiming](#deleting-retaining-and-reclaiming)).

Example 7-day window request and response (the response omits `retention_window_ms` when cleared):

```json
{"kind":"retention_window","retention_window_ms":604800000}
```

```json
{"kind":"retention_window","namespace_id":"demo","retention_window_ms":604800000}
```

#### Garbage collection jobs

A `gc` job runs one complete, stateless collection using a single clock reading and the standard [grace window](glossary.md#grace-window) (there are no request fields or cursors, and no progress is persisted between calls). Collection deletes unreferenced objects older than the grace window, while pool upload sessions keep their leases and completed-content grace ([Upload-session cleanup](format.md#116-upload-session-cleanup)).

Namespace GC and pool GC collect disjoint prefixes ([One complete collection](format.md#111-one-complete-collection)):

- **Namespace GC** collects manifests, data WAL objects, metadata segments, extension segments, and snapshots under the namespace prefix. It never touches the pool prefix or deletes content objects. It reads the current manifest and all snapshot-pinned manifests ([Reference roots](format.md#112-reference-roots)), aborting before listing or deleting anything if any manifest read fails. Before deleting manifests, it raises the [manifest boundary](glossary.md#manifest-boundary) to the highest deleted manifest number. If the newest manifest is `initializing` and more than 24 hours old, it publishes the namespace's tombstone ([Deleting a namespace](format.md#94-deleting-a-namespace)). When retiring an eligible deleted namespace ([Retiring an eligible tombstone](format.md#118-retiring-an-eligible-tombstone)), it deletes any fork snapshot on the source and sets the boundary to retired. Once retired, a namespace has no roots, reads no manifests, returns a normal GC summary (not `namespace_deleted`), and deletes all remaining manifests, data WAL objects, segments, and snapshot records past the grace window while keeping the boundary object, [fences](glossary.md#fence), and [floor objects](glossary.md#floor-object) (which no collection ever deletes).
- **Pool GC** collects content objects, upload records, and temporary objects under the pool prefix by listing upload sessions and [member entries](glossary.md#member-entry), then reading content roots from every member ([Content roots](format.md#119-content-roots)). It protects content matching rooted layout extent filters, deletes member entries belonging to retired, missing, or other-pool namespaces, and skips all content-object deletions if any member is unreadable or reading all members exceeds the pool root-reading budget. A pool with no remaining member entries deletes content, temporary objects, and upload records under the age rules.

Because concurrent GC calls can overlap deletion attempts, returned counts are operational summaries (every count field is always present, even when zero):

- **`listed_bytes`.** Total provider-reported bytes of every object listed during the call before deletions. For namespaces, this covers everything under the namespace prefix except the manifest boundary and non-segment extension objects. For pools, it covers member entries, upload records, content objects, and temporary objects (including a single listing of content objects for measurement even when content deletion was skipped). Applications metering storage combine pool values for content with namespace values for metadata.
- **`deleted` (namespace).** Counts deleted `wal_objects` (data WAL objects only), `metadata_segments`, `extension_segments` (such as unreferenced grep segments), and `manifests`. Paired with `deleted_snapshots_by_owner` (`user` and `fork`, counted once per actually present snapshot deleted).
- **`deleted` (pool).** Counts deleted `content_objects`, `upload_sessions`, and `temporary_objects` (omitting `deleted_snapshots_by_owner` and member-entry deletions).
- **`next_reclamation_at_ms`.** Optional earliest known future reclamation timestamp examined during the call. For namespaces: a deleted namespace's retirement deadline within the grace window, the 24-hour mark of an initializing namespace, or a retained snapshot's cleanup time. For pools: an upload session's lease+grace, abort-grace, completed-content-grace, or deleted-namespace retirement deadline. Fork snapshots and provider-timestamp age-outs don't contribute, so omitting this field doesn't mean nothing remains to collect.
- **`retires_at_ms` (namespace only).** Present whenever the current manifest is deleted (even while snapshots still block retirement), reporting the [retirement](format.md#95-retirement) deadline. Absent for active and retired namespaces.

Every GC response also includes `retained`, a closed set of counts explaining why examined candidates were kept (each collection counts what it examined, so consecutive runs recount kept objects). Namespace responses include the first five rows plus `fences_and_floor_objects`, while pool responses include the first four rows plus the two upload-session rows:

| Reason | Means |
| --- | --- |
| `referenced` | Protected by current roots (including extent-filter matches, even false positives, until the layout segment is unrooted) or needed for forward manifest discovery. |
| `within_grace_window` | Unreferenced, but younger than the grace window by provider last-modified time. |
| `no_provider_timestamp` | Unreferenced, and the provider reported no last-modified time (treated as young). |
| `unrecognized_key` | Listed key the collector doesn't recognize (never deleted regardless of age). |
| `snapshot_not_deletable` | Snapshot still protected by its owner or grace window. |
| `upload_session_window` | Upload session waiting on a timed window (matching `next_reclamation_at_ms`). |
| `upload_session_undecided` | Upload session kept due to a lost CAS, a vanished record during collection, or a content cleanup failure. |
| `fences_and_floor_objects` | A [fence](glossary.md#fence) or [floor object](glossary.md#floor-object), identified by listed size ([Candidate and age rules](format.md#113-candidate-and-age-rules)) and never deleted. |

Pool GC responses include `skipped` (omitted when empty, each reason listed once, and clients MUST tolerate unknown values) when content deletion couldn't run:

| Reason | What the collection skipped | What the operator can do |
| --- | --- | --- |
| `content_deletion_pool_budget` | Skipped content-object deletion because reading all pool members' roots exceeded the [Content roots](format.md#119-content-roots) time budget. | Run pool GC again later. If it persists, delete unused forks to reduce member count. |
| `content_deletion_unreadable_member` | Skipped content-object deletion because one pool member's roots couldn't be read. | Check the server warning log for the collection and repair or delete the reported member namespace. |

[Candidate and age rules](format.md#113-candidate-and-age-rules) defines exact deletion eligibility for every object family.

Example namespace GC result (3 WAL objects and 2 grep segments deleted):

```json
{"kind":"gc","namespace_id":"demo","deleted":{"wal_objects":3,"metadata_segments":0,"extension_segments":2,"manifests":0},"deleted_snapshots_by_owner":{"user":0,"fork":0},"retained":{"referenced":12,"within_grace_window":4,"no_provider_timestamp":0,"unrecognized_key":0,"snapshot_not_deletable":0,"fences_and_floor_objects":2},"listed_bytes":1048576}
```

Example pool GC result (2 content objects and 1 upload session deleted, 1 session waiting on completed-content grace):

```json
{"kind":"gc","pool_id":"pool_3s0drmj8em84yb4mnetqkz2j0x","deleted":{"content_objects":2,"upload_sessions":1,"temporary_objects":0},"retained":{"referenced":40,"within_grace_window":3,"no_provider_timestamp":0,"unrecognized_key":0,"upload_session_window":1,"upload_session_undecided":0},"listed_bytes":73400320,"next_reclamation_at_ms":1752626600000}
```

### Snapshots

A "snapshot" is a stored record that keeps one manifest of a namespace and its referenced segments readable ([Snapshots](format.md#8-snapshots)). Every snapshot has an owner: callers create and delete "user snapshots", while forks maintain "fork snapshots" on their source namespace until the fork retires. The snapshot routes under `/v0/namespaces/{ns}/snapshots` manage only user snapshots (for example, a sandbox can snapshot a namespace before an agent edits it, then read or fork that snapshot later). Each snapshot is identified by `snapshot_id` and records `captured_seq`.

Creating a snapshot (`POST /v0/namespaces/{ns}/snapshots`) first folds any WAL tail after the current manifest, then writes a snapshot record pinning the resulting manifest:

- **`name` (optional).** A label of 1 to 128 characters (empty strings return `invalid_request`). Names aren't unique keys, so multiple snapshots can share a name.
- **`ttl_ms` (optional).** Milliseconds until expiry measured from the server's clock (`0` returns `invalid_request`). Omitting `ttl_ms` creates a snapshot that doesn't expire. Sending `{}` creates an unnamed, non-expiring snapshot.

Create, list, retrieve, and extend all return the same snapshot object (`name` and `expires_at_ms` are omitted when unset):

```json
{"snapshot_id":"snp_0000000000004s63m8hsbwe8xc","namespace_id":"demo","name":"release","captured_seq":12,"created_at_ms":1752623000000,"expires_at_ms":1752626600000}
```

Lifecycle and listing rules:

- **Listing and retrieval.** `GET /v0/namespaces/{ns}/snapshots` returns active user snapshots in ascending `snapshot_id` order, omitting expired, deleted, and fork snapshots. `GET /v0/namespaces/{ns}/snapshots/{snapshot_id}` returns one active user snapshot and returns `snapshot_not_found` for missing, expired, deleted, or fork snapshot IDs. On a deleted namespace, both routes return `410 namespace_deleted` ([Deleting a namespace](format.md#94-deleting-a-namespace)).
- **Expiration.** Expired snapshots can't be read, forked, extended, or retrieved (all return `snapshot_not_found`).
- **Extension.** `POST .../snapshots/{snapshot_id}/extend` sets the expiry to `now + ttl_ms` on the server's clock, never moving expiry earlier (so extending a non-expiring snapshot returns it unchanged).
- **Deletion.** `DELETE .../snapshots/{snapshot_id}` records `deleted_at_ms` on the snapshot record while leaving `expires_at_ms` unchanged, and returns:

```json
{"namespace_id":"demo","snapshot_id":"snp_0000000000004s63m8hsbwe8xc"}
```

Recording deletion in `deleted_at_ms` instead of overwriting `expires_at_ms` avoids clock-skew races where a host with a lagging clock would keep treating the snapshot as valid. Every host refuses a snapshot with `deleted_at_ms` regardless of its local clock, returning `snapshot_not_found` for reads, forks, extensions, or retrievals, and omitting it from user listings.

Even after expiry or deletion, the snapshot record stays in storage until GC removes it after the grace window (`deleted_at_ms + grace` or `expires_at_ms + grace`, whichever comes first). Keeping the record through the grace window leaves operations that validated the snapshot right before deletion with the same completion margin as before expiry, and keeps the [snapshot inventory](#snapshot-inventory) accurate about what still pins old manifests.

Deleting a missing or already-deleted user snapshot returns the same success response without writing, making retries safe. Deleting a fork snapshot returns `snapshot_not_found` because only garbage collection deletes fork snapshots. In the service context, snapshot deletion stays available after the namespace itself is deleted ([`DELETE /v0/namespaces/{ns}`](#63-delete-v0namespacesns)).

Creating, retrieving, listing, extending, and deleting snapshots require an administrator or the service context. Reading through a snapshot requires `read` and `history` on the historical inode, evaluated at the current head.

Path stat, inode stat, path and inode directory listings, path and inode revision listings, file content reads, downloads (in the JSON body), and change-feed requests all accept an optional `snapshot_id`:

- On path content and path download requests, `snapshot_id` can't be combined with `revision_no` because the snapshot selects the revision.
- On inode revision content (`GET /inodes/{inode_id}/revisions/{revision_no}/content`) and inode revision downloads, `snapshot_id` looks up that revision in the snapshot's state, keeping it readable even after the retention floor passes it.
- A snapshot change feed stops at `captured_seq`, and `after_seq` can't exceed `captured_seq`.
- Read routes also accept any of the namespace's fork snapshots as `snapshot_id` (unlike snapshot management routes and `POST /forks`, which refuse them with `snapshot_not_found`). In particular, every fork's `fork_basis.fork_point_snapshot_id` pins the fork's state at the fork point (identical to the source at that sequence) and never expires, so clients can read historical revisions from the fork even after the source's retention floor advances past the fork point ([`POST /forks`](#613-post-forks)).
- Snapshot reads never fall back to live state. Missing, deleted, or expired snapshots always return `snapshot_not_found`.
- In the embedded runtime, setting `snapshot_id` opens a read view of that snapshot. A snapshot `ReadView` rejects options referencing any other snapshot ID with `invalid_request`, and directory cursors from a snapshot view are bound to that snapshot (resuming them on a live listing or another view returns `invalid_request`).

### Snapshot inventory

The maintenance snapshot inventory lists every snapshot pinning manifests in a namespace, including both user snapshots (`owner.kind: "user"`) and fork snapshots (`owner.kind: "fork"`). The inventory is read-only: user snapshots are managed through `/v0/namespaces/{ns}/snapshots` ([Snapshots](#snapshots)), and fork snapshots are deleted when their target fork retires.

A snapshot ID is `snp_` followed by 26 lowercase Crockford base 32 characters, newly generated for every snapshot. Each inventory `SnapshotRecord` contains `namespace_id`, `snapshot_id`, `owner` (including `name` for user snapshots with a name, or `target_namespace_id` for fork snapshots), `created_at_ms`, optional `expires_at_ms`, optional `deleted_at_ms`, `captured_seq`, and `manifest_no`.

`GET /v0/maintenance/namespaces/{ns}/snapshots?limit=100&cursor=...` returns all existing records in ascending `snapshot_id` order:

```json
{"namespace_id":"demo","snapshots":[{"namespace_id":"demo","snapshot_id":"snp_0000000000004s63m8hsbwe8xc","owner":{"kind":"user","name":"release"},"created_at_ms":1752623000000,"expires_at_ms":1752626600000,"captured_seq":12,"manifest_no":9},{"namespace_id":"demo","snapshot_id":"snp_0000000000004w2t8qv0d9a5rj","owner":{"kind":"fork","target_namespace_id":"demo-copy"},"created_at_ms":1752623100000,"captured_seq":12,"manifest_no":9}]}
```

`GET /v0/maintenance/namespaces/{ns}/snapshots/{snapshot_id}` returns one record in the same shape, including expired or deleted snapshots awaiting GC, and returns `snapshot_not_found` if absent.

Inventory pagination uses the advertised pagination limits and omits `next_cursor` on the final page. Cursors are opaque and scoped to the namespace and operation (replaying a cursor on another namespace returns `invalid_request`). Because the inventory is live rather than a point-in-time view, concurrent creates, deletes, or collections can affect later pages.

Expired snapshots stay in the inventory until GC removes them after `expires_at_ms + grace`. Deleted user snapshots stay listed (with `deleted_at_ms`) until GC removes them after `deleted_at_ms + grace` (or `expires_at_ms + grace`, if earlier). Non-expiring snapshots on live namespaces stay until explicitly deleted ([Records and owners](format.md#81-records-and-owners)).

### Deleting, retaining, and reclaiming

Deleting a namespace causes ordinary operations to return `namespace_deleted` (cached readers follow [Deleting a namespace](format.md#94-deleting-a-namespace)). Space reclamation is asynchronous, has no set completion deadline, and doesn't guarantee physical erasure.

Reclamation works across four interacting layers:

- **Namespace retirement.** Once a deleted namespace is eligible ([Retirement](format.md#95-retirement)), retirement raises its [manifest boundary](glossary.md#manifest-boundary) to retired ([Retiring an eligible tombstone](format.md#118-retiring-an-eligible-tombstone)). Later namespace collections delete the tombstone and every other object under the namespace prefix except the boundary object (kept as a record that the ID was used, whose purge is outside this API), [fences](glossary.md#fence), [floor objects](glossary.md#floor-object), and unrecognized keys. A deleted ancestor still keeps each fork's fork-snapshot manifest and segments until that fork retires.
- **Pool content reclamation.** Namespace retirement deletes no content. Pool GC deletes a content object once it's older than the grace window and unreferenced by every pool member's [content roots](format.md#119-content-roots) (revisions in current manifests, snapshot manifests, or unfolded WAL tails, plus upload session records). A deleted namespace's own content references stop protecting content at its retirement deadline (24 hours after deletion), though snapshots still protect their referenced content until deleted.
- **Trash and retention floor.** Deleting a file or directory doesn't immediately reclaim content because deleted files keep their newest revision while in the trash. Advancing the metadata retention floor (explicitly via a `retention` job or `loonfs maintenance retention advance`, or automatically via the server's sweep under the namespace's [retention window](glossary.md#retention-window)) expires deletions at or below the floor and lets base compaction drop superseded revisions (keeping each file's newest revision at or below the floor) and rows hidden by expired deletions, making their content eligible for pool GC.
- **Upload sessions and late writes.** Completed upload sessions stay recorded for the 8-day completed-content grace in active namespaces, or until the retirement deadline in deleted namespaces (after which they follow retired-namespace rules even if snapshots delay retirement, see [Upload-session cleanup](format.md#116-upload-session-cleanup)). Once deleted, LoonFS refuses commits, new presigned URLs, and upload completions, though already-issued presigned URLs aren't revoked. Because a runtime revalidates its cached namespace head at least every 20 minutes, it can grant an upload request up to 20 minutes after another process deletes the namespace. The 24-hour retirement grace window exceeds that 20-minute bound plus presigned URL lifetimes (15 minutes, or 1 hour for multipart parts), and running GC repeatedly catches any late writes that arrive after an earlier listing.

The reference server's maintenance scheduler runs on two schedules:

- **Session maintenance (every 5 seconds by default).** Runs due steps for every open namespace writer session. Collection is due on the collection interval (1 hour by default) once the session's published sequence advances, collecting only the namespace (not its pool).
- **Store sweep (at startup, then every 24 hours by default).** Lists all namespaces (including deleted and retired ones) and runs due steps including namespace GC, then lists all pools (every pool with any object under its prefix) and runs pool GC with the same concurrency. Failed pool collections are logged and retried on the next sweep (`pool_not_found` isn't an error if the pool emptied after listing). Idle namespaces (forks, deleted, retired, or unwritten namespaces) cost no requests between sweeps.

Each sweep also advances the retention floor of every active namespace that has a retention window, moving it to the last commit at or before `now - retention_window_ms` using the same cutoff rule as a `retention` job ([Advancing the retention floor](format.md#101-advancing-the-retention-floor)). New namespaces receive `retention_window_ms` from the create request (or from the server's `retention_window_ms` default, advertised as `namespace.default_retention_window_ms`), and namespaces with no window aren't advanced by sweeps. If a namespace step is already running when the sweep arrives, that step performs the advance afterward so the sweep doesn't block. Like `retention` jobs, sweeps write a [floor object](glossary.md#floor-object) without claiming a writer epoch (never fencing writers in other processes), fold if needed, and tolerate concurrent maintaining servers cleanly (the higher floor wins).

LoonFS doesn't trigger GC automatically on namespace deletion, upload expiry, or fork retirement. Hosts without the reference scheduler run GC themselves per namespace (`POST /v0/maintenance/namespaces/{ns}/jobs` with `{"kind":"gc"}`, `loonfs maintenance gc`, or `Maintenance::gc`) and per pool (`POST /v0/maintenance/pools/{pool_id}/jobs`, `loonfs maintenance pool gc`, or `Maintenance::gc_pool`), optionally using `next_reclamation_at_ms` to schedule the next run.

During server shutdown, the scheduler starts no new steps, running compactions stop at their next block (including writer-session compactions), and shutdown waits for already-started folds and compactions to finish.

Operators on providers with multipart uploads SHOULD keep a provider lifecycle rule to clean up incomplete multipart uploads (which can exist outside object listings), and note that key deletion doesn't erase physical versions kept by bucket versioning or retention locks. To diagnose blocked reclamation, inspect the `retained` counts in GC responses and list snapshots via `GET /v0/maintenance/namespaces/{ns}/snapshots`.

### Uploads

#### Service-proxied upload

`service_proxied` is the default upload mode and requires no special provider capabilities. The client sends file bytes via `PUT /uploads/{upload_id}/content`, and the server streams them straight to object storage without buffering the entire file, counting bytes and computing the store's checksum along the way. Bodies exceeding `upload.service_proxied.max_content_bytes` fail with `content_too_large`. The resulting `content_ref` records the computed checksum.

Retrying `PUT /content` with identical bytes for the same upload ID (even concurrently or after a lost response) succeeds and returns the same `content_ref`. Sending different bytes after content is staged returns `upload_content_conflict`.

#### Direct single-PUT upload

In `direct_put` mode, the server allocates the object identity and returns a short-lived presigned PUT URL plus the provider's stored checksum algorithm. The client counts and hashes the bytes as it uploads directly to the provider, then sends the size and checksum when completing the session:

```json
{
  "mode": "direct_put",
  "size_bytes": 1234
}
```

`size_bytes` is optional and advisory. When supplied, values above `upload.direct_put.max_content_bytes` fail immediately with `content_too_large` (the provider enforces the same limit on the PUT). Larger files use `direct_multipart` when available.

The response returns a short-lived presigned URL and signed headers (never raw credentials or caller-managed object keys) that the client MUST send unchanged. For example, on an S3-compatible provider:

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_...",
  "mode": "direct_put",
  "checksum_algorithm": "crc64nvme",
  "access": {
    "kind": "presigned_url",
    "method": "PUT",
    "url": "https://...",
    "headers": {
      "if-none-match": "*"
    },
    "expires_at_ms": 1780000000000
  }
}
```

Signed headers are bound into the URL signature (for example, `if-none-match: *` enforces create-only writes to the immutable object, and dropping or altering it invalidates the signature). S3-family issuers report `crc64nvme`, while GCS reports `crc32c`.

Provider qualification is strict because arbitrary S3-compatible gateways aren't guaranteed to enforce create-only preconditions or report stored checksums. Unqualified endpoints omit `filesystem.uploads.direct_put` and return `not_supported` (`feature = "filesystem.uploads.direct_put"`), leaving `service_proxied` available. In the reference server, `direct_put` is enabled only for adapters passing the live provider suite:

- AWS S3 and Cloudflare R2 (SigV4 on their native domains).
- Google Cloud Storage (native `GOOG4-RSA-SHA256` API, not its S3-interoperability API, which doesn't preserve preconditions).
- Custom S3-compatible endpoints, Azure Blob Storage, and local filesystems don't advertise `direct_put`, with no config override.

`direct_put` is independent of `direct_multipart` (for example, the reference server's GCS adapter supports `direct_put` and direct downloads without multipart signing). Clients fall back from `direct_multipart` to `direct_put` before falling back to `service_proxied`, and unknown-length streams can use `direct_put` and report their measured length at completion. Browser callers talking to presigned URLs are governed by the bucket's own CORS configuration.

After uploading to the presigned URL, the client completes the session with a content claim matching the session's `checksum_algorithm` (a mismatched algorithm returns `invalid_request`):

```json
{
  "mode": "direct_put",
  "content": {
    "size_bytes": 1234,
    "checksum": { "algorithm": "crc64nvme", "value": "<16 lowercase hex>" }
  }
}
```

To complete a `direct_put` session, the server:

1. Builds the candidate `content_ref` from the session's content ID and the client's claim.
2. Reads the stored object's size and checksum via a provider metadata request (without downloading the bytes) and compares them to the claim (without re-checking the provider's single-PUT size limit).

If the stored metadata disagrees with the claim, the server returns `invalid_request` and leaves the session open and object intact so the client can fix the claim or abort. If the metadata request fails, the server returns `server_error` leaving the session open for retry. Abandoned sessions are aborted by upload cleanup after their lease expires ([Upload-session cleanup](format.md#116-upload-session-cleanup)).

#### Direct multipart upload

In `direct_multipart` mode, the client uploads a large file in parallel parts directly to object storage while the server manages provider multipart initialization, part signing, and assembly without touching file bytes.

The begin request sets only the part size (no total length or whole-object checksum is needed upfront, supporting single-pass and unknown-length streams):

```json
{
  "mode": "direct_multipart",
  "part_size_bytes": 8388608
}
```

`part_size_bytes` defaults to 8 MiB and MUST be between 5 MiB and 5 GiB. A session allows at most 10,000 parts, so very large files need a larger part size. The begin response omits `content_ref` (since total size and checksum aren't known yet):

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_...",
  "mode": "direct_multipart",
  "part_size_bytes": 8388608,
  "checksum_algorithm": "crc64nvme"
}
```

To upload parts, the client posts a list of `{part_number, checksum}` items (using the session's `checksum_algorithm`) to `POST /uploads/{upload_id}/parts` and receives one presigned URL per part. Parts can be uploaded in parallel and retried by requesting a fresh URL (the provider keeps the latest upload for each part number). Because the server doesn't track part progress, the client MUST record each part's `part_number`, `etag`, and `checksum` until completion.

The completion request supplies the total size, whole-object checksum (CRC-64/NVME in v0), and every uploaded part once in ascending `part_number` order:

```json
{
  "mode": "direct_multipart",
  "content": {
    "size_bytes": 17301504,
    "checksum": { "algorithm": "crc64nvme", "value": "<16 lowercase hex>" }
  },
  "parts": [
    { "part_number": 1, "etag": "\"...\"", "checksum": { "algorithm": "crc64nvme", "value": "<16 lowercase hex>" } },
    { "part_number": 2, "etag": "\"...\"", "checksum": { "algorithm": "crc64nvme", "value": "<16 lowercase hex>" } },
    { "part_number": 3, "etag": "\"...\"", "checksum": { "algorithm": "crc64nvme", "value": "<16 lowercase hex>" } }
  ]
}
```

(By comparison, `service_proxied` completion sends `{"mode":"service_proxied"}`, and `direct_put` completion sends `mode` and `content`.)

To complete a `direct_multipart` session, the server:

1. Asks the provider to assemble the multipart object.
2. Reads the assembled object's stored size and checksum (required because providers vary in how they handle bad checksums during assembly, making the stored object the only authoritative proof).
3. Compares the stored values with the completion claim.

Mismatch returns `invalid_request` (leaving the session open and object intact), while assembly or metadata failures return `server_error` (leaving the session open for retry). If a completion response is lost, the client resends the identical completion request:

- A `completed` session replays its stored result (and mints a new `content_token` while its token window is open).
- An `open` session continues completion. If the provider multipart upload no longer exists (for example, after a lost earlier completion), the server checks the assembled object against the claim, completing the session on match or returning `invalid_request` otherwise.
- An `aborted` session is terminal, so the client stops retrying.

Callers that can't resend the completion request can call `GET /uploads/{upload_id}` instead, which returns the same stored result for a completed session. Because the session record stores the provider's multipart upload ID, aborting (by client or upload GC) also aborts the provider upload and cleans up partial data (aborting an already-assembled upload succeeds harmlessly on every supported provider without touching the assembled object).

### Content references and tokens

A `ContentRef` contains `kind`, `content_id`, `size_bytes`, and `checksum`, with no namespace ID. Because storage keys derive from `content_id` and the reading namespace's [pool](glossary.md#pool), the same `ContentRef` addresses the same bytes across every member of that pool ([File contents and ownership](format.md#15-file-contents-and-ownership)). Clients pass `ContentRef` values back unchanged.

Servers return a short-lived opaque `content_token` alongside a completed session's `content_ref`. Tokens are scoped strictly to the namespace where the upload completed and are rejected in every other namespace (including other members of the same pool). Reading a completed session issues a fresh token while its token-issuing window stays open, and `content_ref` stays readable afterward:

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_...",
  "content_ref": { "kind": "blob_v1", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "..." } }
}
```

In a commit, `put_file` (or inode file-write operations) references `content_ref` and includes the matching `content_token` verbatim in `content_tokens`. The server verifies tokens before admission and re-checks the resulting in-memory proof's binding and deadline at publication. A missing, malformed, or expired proof for a referenced `content_ref` returns `content_not_prepared` without reading the content object, while unused tokens are ignored:

```text
Loonfs-Actor: document-importer
Loonfs-Context: service
```

```json
{
  "commit_id": "commit-a",
  "content_tokens": [
    {
      "content_ref": { "kind": "blob_v1", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "..." } },
      "token": "opaque-server-token"
    }
  ],
  "operations": [
    {
      "kind": "put_file",
      "path": "/docs/report.pdf",
      "content_ref": { "kind": "blob_v1", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "..." } },
      "behavior": "no_replace"
    }
  ]
}
```

### Creating and forking namespaces

Namespace creation addresses namespaces directly by ID (v0 has no namespace aliases or separate `name` fields, and route placeholders such as `{ns}`, `{source_ns}`, and `:namespace` all accept the same namespace ID). A create request and its initial response look like this:

`Loonfs-Actor: usr_8f3c`

```json
{
  "namespace_id": "demo"
}
```

```json
{
  "namespace_id": "demo",
  "created_at_ms": 1752623000000,
  "created_by": "usr_8f3c",
  "pool_id": "pool_3s0drmj8em84yb4mnetqkz2j0x",
  "head_seq": 0,
  "retention_floor_seq": 0
}
```

Fork requests specify the target namespace ID in `new_namespace_id`. Both `namespace_id` (on create) and `new_namespace_id` (on fork) are optional: when omitted, LoonFS generates `ns_` followed by the standard 26-character random suffix and returns it as `namespace_id`. Because create and fork aren't idempotent, callers that need safe recovery from a lost response MUST supply their own ID (retrying without one would create a duplicate namespace).

Applications that want reusable human-readable names MUST maintain their own mapping to namespace IDs and allocate a fresh ID per lifetime ([Namespaces and identity](format.md#11-namespaces-and-identity)).

Both create and fork first check the target ID's newest manifest:

- An active target returns `409 namespace_exists` without writing.
- A deleted or retired target returns `410 namespace_deleted` without writing.
- Conditional put of manifest 1 paired with reading the [manifest boundary](glossary.md#manifest-boundary) determines whether the call created the namespace ([Namespace lifecycle and forks](format.md#9-namespace-lifecycle-and-forks)).

Creation and fork initialization proceed in steps:

- **Create.** Generates a new `pool_id`, writes manifest 1 (`initializing`) with put-if-absent, writes the pool [member entry](glossary.md#member-entry) (`pools/{pool_id}/members/{namespace_id}.json`), and writes manifest 2 (`active`, see [Creating a namespace](format.md#91-creating-a-namespace)).
- **Fork.** If no `snapshot_id` is supplied, reads the source head and creates a temporary unnamed user snapshot of the source's current manifest expiring in 5 minutes. Writes manifest 1 (`initializing`), folds any source WAL tail into manifest 2 (without publishing anything on the source), writes the fork point snapshot on the fork, the fork snapshot on the source, and the member entry in the source's pool, then writes the `active` manifest ([Forking a namespace](format.md#92-forking-a-namespace)).

If creation or forking fails between manifest 1 and the active manifest, the namespace stays `initializing`, and every route except create, fork, or delete returns `namespace_not_found`. A delete removes it, and namespace GC tombstones any initializing namespace whose manifest 1 is older than 24 hours ([Deleting a namespace](format.md#94-deleting-a-namespace)).

A retry with the same ID and matching options finishes an initializing namespace and returns success (or returns `namespace_exists` once active, unless a create explicitly allows an existing namespace). Matching rules:

- A create matches an initializing create with the same retention window (never an initializing fork).
- A fork matches an initializing fork with the same source, the same `snapshot_id` when supplied (omitting `snapshot_id` matches whichever snapshot the first call used), and the same rebased fork for a rebase fork (never an initializing create).
- Request timestamps and principals aren't compared. Mismatched options always return `namespace_exists`.
- Within the original call, after an unobserved transport error on manifest 1, LoonFS looks up the newest manifest and finishes initialization the same way ([Finishing an initializing namespace](format.md#finishing-an-initializing-namespace)).

### Listing namespaces

`GET /v0/namespaces?limit=100&cursor=...` lists active namespaces in object-key order, returning each item as the same `NamespaceMetadata` object returned by `GET /v0/namespaces/{ns}`, along with `next_cursor` when another page remains:

```json
{
  "namespaces": [
    {"namespace_id": "demo", "created_at_ms": 1752623000000, "created_by": "usr_8f3c", "pool_id": "pool_3s0drmj8em84yb4mnetqkz2j0x", "head_seq": 418, "retention_floor_seq": 0},
    {"namespace_id": "ns_01jcd8z6b4r8v7w4a6ygqz0d1k", "created_at_ms": 1752700000000, "created_by": "usr_8f3c", "pool_id": "pool_7h2m9kq4xw1c0bz5trnp8e3vfy", "head_seq": 0, "retention_floor_seq": 0}
  ]
}
```

Each page reads up to `limit` namespace prefixes from object storage and loads each namespace's newest manifest at the server's default consistency. Because object storage still retains tombstones for deleted namespaces and boundary objects for retired ones ([Retirement](format.md#95-retirement)), reading each manifest is required just to filter out non-live IDs, so returning full metadata adds no extra reads. That per-namespace manifest read is also why `limit` defaults to 100 here (rather than `pagination.default_limit`, though `pagination.max_limit` still caps it).

Deleted, retired, and initializing namespaces (and any stray non-namespace objects under `namespaces/`) are omitted from `namespaces` while still consuming slots in the object-storage page. As a result, a page can return fewer items than `limit` (even zero) and still include `next_cursor`. Callers listing all namespaces MUST follow `next_cursor` until it's absent.

Object-key order matches namespace ID order with one quirk: IDs extending a shorter ID with `-` or `.` sort before the shorter ID (for example, `a-b` and `a.b` precede `a`), because the prefix key `namespaces/a/` ends in `/`, which sorts after `-` and `.` (and before letters, digits, and `_`, so `a` still precedes `a0` and `ab`).

Cursors record the object-storage listing position, so they don't expire and concurrent namespace creates or deletes don't invalidate them (a namespace created before the cursor isn't visited in the current pass). Cursors from other routes return `invalid_request`.

### 6.1 `GET /v0/capabilities`

Returns the capability document defined in [The capability document](#21-the-capability-document).

### 6.2 `GET /v0/namespaces/{ns}`

Returns the metadata of one namespace without listing all namespaces. A missing or `initializing` namespace returns `404 namespace_not_found`, and a deleted or retired namespace returns `410 namespace_deleted`. The route accepts `consistency` and `max_staleness_ms` to control how recently the server confirms the head that `head_seq` and `retention_floor_seq` come from (see [Read consistency](#read-consistency)), using the server's default when omitted.

```json
{
  "namespace_id": "demo",
  "created_at_ms": 1752623000000,
  "created_by": "usr_8f3c",
  "pool_id": "pool_3s0drmj8em84yb4mnetqkz2j0x",
  "head_seq": 418,
  "retention_floor_seq": 120
}
```

`NamespaceMetadata` contains these fields:

| Field | Meaning |
| --- | --- |
| `namespace_id` | Durable namespace ID. |
| `created_at_ms` | Creation time in Unix milliseconds. |
| `created_by` | Actor that created or forked the namespace. |
| `fork_basis` | Present only on forks. Records `source_namespace_id`, `fork_point_seq` (the source sequence captured), `fork_point_snapshot_id` (keeps the fork's state at the fork point readable), and `rebased_from` on rebase forks ([`POST /forks`](#613-post-forks)). |
| `pool_id` | The namespace's [pool](glossary.md#pool), under whose prefix content objects and upload records are stored ([File contents and ownership](format.md#15-file-contents-and-ownership)). Generated on create, inherited on fork. |
| `head_seq` | Current visible commit sequence. |
| `retention_floor_seq` | Current retention floor. Change-feed requests with `after_seq >= retention_floor_seq` succeed. |
| `retention_window_ms` | Present only when set. The namespace's [retention window](glossary.md#retention-window) in milliseconds (at least the 24-hour [grace window](glossary.md#grace-window)). |

A newly created namespace starts with no access rows, so requests with principals have no rights until the application installs initial administrators via `update_access` on `/` in the service context ([`POST /commits`](#68-post-commits)). Create requests can pass `retention_window_ms` (values under 24 hours, including 0, return `invalid_request` with `param` `/retention_window_ms`). When omitted, the reference server applies its configured `retention_window_ms` default if set. Later `retention_window` jobs can update or clear it ([Retention jobs](#retention-jobs)), and forks copy their source's window.

How LoonFS resolves a namespace's live sequence depends on the runtime's state:

- **Cold read.** Reads the [manifest boundary](glossary.md#manifest-boundary) and lists manifests above it ([Resolving the metadata basis](format.md#41-resolving-the-metadata-basis)). If neither a boundary nor any manifest exists, the namespace is treated as absent.
- **Cached view without a writer session.** A `strong` read ([Read consistency](#read-consistency)) probes the next numbered WAL object with `GET`, replays any new WAL objects until hitting 404, and sees commits before a fold publishes them into a manifest.
- **Open writer session.** Reads start from the session's [writer position](glossary.md#writer-position), which advances before each commit is acknowledged, so local reads see local commits without probing the WAL. Another writer must publish a claim manifest to take over, which the successor-manifest check detects (causing `eventual` reads to see the new writer's commits within their staleness bound, `strong` reads to see them immediately, and the old session's next commit to fail with `writer_fenced`). A [floor object](glossary.md#floor-object) written by another process changes only the retention floor and reaches the session's reads on its next discovery or once a fold manifest covers it.

`RuntimeConfig::manifest_revalidation_interval_ms` (default 1,000 ms, or 0 to check on every read) sets the minimum monotonic interval between checks for a successor manifest on a cached head. After the interval elapses, a read sends a `HEAD` for the successor manifest and a conditional read of the manifest boundary (a `strong` read also probes the next WAL number first, so a warm unchanged read costs 3 requests past the interval and 1 request within it). If a successor exists or the cached manifest is at or below the boundary, the reader reloads the namespace, observing deletions and new manifests on that interval. A `strong` read on a cached view also discovers foreign floor objects via its WAL probe. Handle builders accept an injected monotonic timer for deterministic testing.

The maintenance endpoint `GET /v0/maintenance/namespaces/{ns}/diagnostics` returns namespace metadata plus storage diagnostics for maintenance:

| Field | Meaning |
| --- | --- |
| `namespace_id` | Durable namespace ID. |
| `created_at_ms` | Creation time in Unix milliseconds. |
| `created_by` | Actor that created or forked the namespace. |
| `fork_basis` | Present only on forks (`source_namespace_id`, `fork_point_seq`, `fork_point_snapshot_id`, and optional `rebased_from`). |
| `head_seq` | Current visible commit sequence. |
| `retention_floor_seq` | Current retention floor. |
| `current_manifest_no` | Current manifest number, present from namespace creation. |
| `wal_tail_objects` | WAL tip minus the current manifest's folded WAL number, including fences. |
| `live_snapshots` | Count of snapshots unexpired when the diagnostics read began (including non-expiring snapshots). |

```json
{
  "namespace_id": "demo",
  "created_at_ms": 1752623000000,
  "created_by": "usr_8f3c",
  "head_seq": 418,
  "retention_floor_seq": 120,
  "current_manifest_no": 410,
  "wal_tail_objects": 3,
  "live_snapshots": 2
}
```

#### Read consistency

In a runtime without the namespace's writer session, confirming the cached head costs a WAL probe. Because reads following writes in the same runtime already see those writes (and cold opens already load fresh state via discovery), live reads default to `eventual` consistency so callers pay for head confirmation only when needed. Each live read supports two `consistency` modes:

- **`eventual` (default unless the host sets `strong`).** Reuses the reader's last confirmed head without sending a confirmation request while that confirmation is younger than the read's maximum staleness, and confirms the head like `strong` once stale. An `eventual` read sees every commit acknowledged before the last confirmation, may miss newer commits, and never goes backward on the same runtime because the confirmed head is monotonic. For example, with 1,000 ms staleness, a reader confirmed at sequence 40 serves `eventual` reads at 40 for up to 1 second even if sequence 41 commits, while a `strong` read during that second advances the confirmed head to 41 for all later `eventual` reads.
- **`strong`.** Confirms the head before reading, guaranteeing visibility of every commit acknowledged before the read started. Use `strong` when reading after a write or permission change made through a different process or server.

Configuration and overrides:

- **Defaults.** `RuntimeConfig::consistency` (`ReadConsistency::Eventual` or `ReadConsistency::Strong`) and the reference server's `consistency` setting control the default for requests that omit `consistency`. `RuntimeConfig::max_staleness_ms` (and the server's `max_staleness_ms`) default to 1,000 ms, with `0` confirming on every read. In Rust, read option structs default consistency to `None` (or `ReadAt::Live(None)`), and option-less methods such as `Namespace::read_file_revision` always use the runtime default.
- **Per-request staleness.** Callers can override staleness with `Consistency::Eventual { max_staleness_ms }` in Rust or `max_staleness_ms` in HTTP (accepted only together with `consistency=eventual`).
- **Upper bound (`READ_REVALIDATION_BOUND_MS`).** Neither `max_staleness_ms` nor `manifest_revalidation_interval_ms` can exceed `READ_REVALIDATION_BOUND_MS` (1,200,000 ms, or 20 minutes). A larger setting fails runtime construction or server startup, and a read that asks for a larger `max_staleness_ms`, in Rust or over HTTP, returns `invalid_request` with `param` `max_staleness_ms` (`/max_staleness_ms` on the download routes) before the read sends a request to object storage. Any cached head whose last manifest check is older than 20 minutes is always revalidated regardless of setting, ensuring the 24-hour grace window always covers the revalidation bound plus the 15-minute presigned download URL lifetime and clock allowance ([Content roots](format.md#119-content-roots)).

In a runtime with the namespace's writer session, reads start from the [writer position](glossary.md#writer-position) and confirm it by checking the next manifest number (`HEAD`) and reading the manifest boundary. An `eventual` read skips the check within its staleness window and waits for it afterward, while a `strong` read always waits for a check that started after the read began so it sees another writer's claim. Because the session runs one check at a time and concurrent arrivals share the next check, a wave of concurrent `strong` reads costs at most two checks.

Pinned reads (reads with `snapshot_id` or through a `ReadView`) have no consistency setting because the snapshot or view pins the state. Passing `consistency` with `snapshot_id` (or on a `ReadView` read in Rust) returns `invalid_request` with `param` `consistency`. In Rust, snapshot-capable options use `at` (`ReadAt::Live` or `ReadAt::Snapshot`) so both can't be set together. Looking up a snapshot at the live head uses `strong` confirmation, and capturing a `ReadView` uses the requested or default live consistency.

When an `eventual` request supplies a sequence learned from a fresher read (a listing cursor's head, a change feed's `after_seq`, or a diff's `to_seq`) that is ahead of the reader's cached head, LoonFS automatically confirms the head as a `strong` read and serves the request at the confirmed head (avoiding an extra client error-and-retry round trip). If the sequence is still ahead of the confirmed head, LoonFS returns `invalid_request`. Requests with sequences at or below the cached head send no confirmation request.

#### Namespace statistics

In the embedded runtime, `NamespaceStore::load_statistics` and `NamespaceStore::load_snapshot_statistics` return `NamespaceStatistics` as of a manifest's folded head (excluding newer WAL commits). The observation includes the manifest reference, creation time, lifecycle status, folded WAL number, activity counters (starting at 0, integers from 0 through 9007199254740991, comparable only within the same namespace), referenced inode and metadata totals, and any fork basis ([Statistics](format.md#74-statistics)).

### 6.3 `DELETE /v0/namespaces/{ns}`

Deletes a namespace. Requires an administrator or the service context.

Deletion runs in the namespace's writer session as a fenced manifest publication ([Deleting a namespace](format.md#94-deleting-a-namespace)), except for an `initializing` namespace, whose tombstone is published without opening a writer session. Deletion linearizes at the manifest put, so commits acknowledged beforehand stay committed. Once observed, reads, user snapshot listings and retrievals, commits, forks from the ID, status queries, repeat deletions, and creating or forking into the same ID all return `410 namespace_deleted` (and continue to do so after [retirement](format.md#95-retirement)).

The snapshot inventory remains available after namespace deletion until retirement. The service context can still delete user snapshots during this period. A deletion request with principals returns `410 namespace_deleted` ([Snapshot cleanup](format.md#117-snapshot-cleanup)). Deleting a fork snapshot ID through the user snapshot route still returns `snapshot_not_found`.

Deletion itself reclaims no storage. Space is reclaimed asynchronously by namespace retirement ([Retirement](format.md#95-retirement), [Retiring an eligible tombstone](format.md#118-retiring-an-eligible-tombstone)) and pool GC ([Garbage collection jobs](#garbage-collection-jobs)). Deployments should run GC repeatedly to catch late writes, keep provider lifecycle rules for incomplete multipart uploads, and account for bucket versioning or retention locks.

The optional `expected_head_seq` query parameter gates deletion on the current head sequence, returning `stale_head` with `expected_head_seq` and `actual_head_seq` in `details` on mismatch:

```json
{
  "namespace_id": "demo",
  "head_seq": 418
}
```

The reference server resolves its write queue in admission order:

- Commits admitted before the delete publish first (a commit whose response was lost isn't retroactively undone by a later delete).
- Requests admitted after the delete fail with `namespace_deleted`.
- If the delete fails its precondition, no other request is rejected because of it.

### 6.4 `GET /filesystem/entry` and `GET /inodes/{inode_id}`

Returns one authoritative path entry, addressed by path or by inode ID. Enum values follow the durable naming rules in [Field conventions](format.md#121-field-conventions).

```json
{
  "namespace_id": "demo",
  "path": "/docs/report.txt",
  "inode_id": "ino_000000r0000013kje0d47gc8kw",
  "created_by": "usr_8f3c",
  "created_at_ms": 1752623000000,
  "inode_kind": "file",
  "head_seq": 418,
  "parent_inode_id": "ino_000000r000000tn10r0b9wf3yy",
  "display_name": "report.txt",
  "binding_version": "opaque-token",
  "revision_no": 7,
  "revision_committed_by": "render-worker",
  "size_bytes": 19482,
  "content_ref": {
    "kind": "blob_v1",
    "content_id": "con_4z59p0wjvx9a8b2frdhhf6mat1",
    "size_bytes": 19482,
    "checksum": { "algorithm": "crc64nvme", "value": "42d..." }
  },
  "revision_committed_at_ms": 1752624000000,
  "attributes_revision_no": 3,
  "attributes_updated_by": "metadata-editor",
  "attributes_updated_at_ms": 1752623500000,
  "attributes": { "owner": "platform" }
}
```

Entry fields and parameters follow these rules:

- **Attribution and content reference.** Every entry includes `created_by` and `created_at_ms`. File entries add `revision_committed_by`, `revision_committed_at_ms`, and `content_ref` (which directory listings also include, omitting it for files the subject can't read, see [`GET /filesystem/entries`](#65-get-filesystementries-and-get-inodesinode_idchildren)). Timestamps are informational (sequence numbers define order). Directories have no modification time in v0, and moves or renames leave creation attribution unchanged.
- **Forward compatibility for `inode_kind`.** Clients MUST tolerate unrecognized `inode_kind` values by treating such entries as opaque: reading universal fields (`display_name`, `inode_id`, `path`, `binding_version`) and ignoring kind-specific fields.
- **Attributes projection (`include_attributes`).** Accepts `true` or `false` (default `false`, and other values return `invalid_request`). Attribute fields are serialized as prefixed siblings rather than a nested object so callers can read `attributes_revision_no` directly for `expected_attributes_revision_no` on their next write. `attributes_revision_no` and `attributes` are always both present or both absent, with `attributes_updated_by` and `attributes_updated_at_ms` added when a stored attribute revision exists. Never-written attributes read as `{}` at revision 0 (with no updater fields), whereas omitting `include_attributes` drops all four fields so absence never means empty.
- **Root and non-root bindings.** The root `/` omits `parent_inode_id`, `display_name`, and `binding_version`. Every other entry includes a validated non-empty `display_name` and its current `binding_version` ([Commit identity and preconditions](#51-commit-identity-and-preconditions)).
- **Consistency.** Accepts `consistency` (`eventual` or `strong`, default from server) and `max_staleness_ms` (0 to 1,200,000, only with `consistency=eventual`). Neither can be combined with `snapshot_id` ([Read consistency](#read-consistency)).
- **Inode route (`GET /inodes/{inode_id}`).** Accepts `snapshot_id`, `include_attributes`, `consistency`, and `max_staleness_ms`, returning the same entry shape with the inode's current `path` (or `/` for the root). Request path limits ([Names and paths](format.md#14-names-and-paths)) don't block returning a longer derived `path`. Moves change `path` and `display_name` without changing `inode_id` or metadata. Unknown or hidden inodes return `inode_not_found`.

### 6.5 `GET /filesystem/entries` and `GET /inodes/{inode_id}/children`

Lists a directory's immediate children or, with `recursive=true`, its entire subtree, addressed by path or inode ID. The envelope reports the target (`path` on the path route, `parent_inode_id` on the inode route) and `head_seq`, so empty directories still report the head they observed. Each entry matches `GET /filesystem/entry` (directories omit file-only fields).

Listings populate each file's `revision_no`, `size_bytes`, `content_ref`, `revision_committed_by`, and `revision_committed_at_ms` directly from its directory binding without reading file revision rows. Including `content_ref` lets sync clients bootstrapping from a listing (or recovering after `below_retention_floor`, or running without admin access to the change feed) compare local files or files across pool members by `(content_id, size_bytes)` and `checksum` without an extra entry read per file (since inode ID and revision number alone aren't unique across forks or recreated paths).

Ordering, addressing, and parameters:

- **Ordering.** Non-recursive listings return children in canonical `name_key` order, while recursive listings walk depth first (see below). Concatenating pages in cursor order yields the complete ordered listing, and clients MUST NOT re-sort pages. The target (`path` or `inode_id`) MUST be repeated on every page. `next_cursor` is included only when more pages exist.
- **Inode route.** Accepts `snapshot_id` and stays attached to the same directory across concurrent renames or moves (entry paths reflect the parent's location at each page's head). Unknown or deleted inodes return `inode_not_found` (including mid-pagination if deleted), and file targets return `wrong_kind` (unlike the path route, which returns a single-entry listing for a file target).
- **`include_attributes` and consistency.** `include_attributes` defaults to `false` with the same sibling-field rules as entry reads (choose page sizes carefully since each attribute map can reach 64 KiB and `pagination.max_limit` applies). `consistency` and `max_staleness_ms` apply per page.

Recursive listings (`recursive=true`, default `false`, and other values return `invalid_request`):

- **Depth-first traversal.** Walks the subtree depth first so cursors store only one `name_key` per depth level rather than an unbounded breadth-first directory queue. Each directory appears before its descendants, with each directory's children in `name_key` order (for example, `/docs/a`, `/docs/a/x.txt`, `/docs/b.txt`). Every entry includes its full `path` and `parent_inode_id` so callers can reconstruct the tree. The listed directory itself isn't returned (on the path route, a file target returns just that file).
- **Cursor resumption and limits.** Recursive cursors encode the walk stack from the target down to the last returned entry and can't be mixed with non-recursive requests or different targets (`invalid_request`). On resume, if a stacked directory was deleted, moved, replaced, or became unreadable, the walk continues in its parent after that directory's name. Each page scans at most `limit` directories beyond the target (or cursor stack). Traversal stops at 128 levels below the target (deeper subtrees can only arise from moves, since request paths cap at 128 components, see [Names and paths](format.md#14-names-and-paths)), returning directories at depth 128 without their children so callers can list them by inode.
- **Authorization.** Requires `read` on the target. An unreadable child directory is still returned (without its `attributes` projection), and the walk doesn't descend into it (matching what separate per-directory listings would reveal, see [Authorization](#authorization)). Subjects with `read` on a deeper directory can list it directly.

Cursor semantics across paged routes:

- Directory, trash, revision, grep, and change-feed cursors are opaque resume positions rather than snapshots (except snapshot-bound directory and revision cursors). They tolerate forward head movement: each page reads at the current head, continuing strictly after the last position. Because multi-page live listings can span different heads, concurrent creates behind the cursor can be missed, deletes behind it may already have been returned, and renames can duplicate or omit an entry. Callers needing a consistent cut re-run if `head_seq` changes. (The CLI reports the last page's `head_seq` and adds `head_drift` with first and last heads when they differ.)
- Cursors reject backward head drift. If a cursor's head is ahead of an `eventual` page's cached head, LoonFS confirms the head first ([Read consistency](#read-consistency)). A cursor still ahead of the confirmed head, a malformed or unrecognized-version cursor, or a cursor replayed on a different target returns `invalid_request`.
- A directory cursor created with `snapshot_id` is pinned to that snapshot's immutable view. Every later page MUST pass the same `snapshot_id` (omitting it or passing another returns `invalid_request`).

```json
{
  "namespace_id": "demo",
  "path": "/docs",
  "head_seq": 418,
  "entries": [
    {
      "namespace_id": "demo",
      "path": "/docs/report.txt",
      "inode_id": "ino_000000r0000013kje0d47gc8kw",
      "created_by": "usr_8f3c",
      "created_at_ms": 1752623000000,
      "inode_kind": "file",
      "head_seq": 418,
      "parent_inode_id": "ino_000000r000000tn10r0b9wf3yy",
      "display_name": "report.txt",
      "binding_version": "opaque-token",
      "revision_no": 7,
      "revision_committed_by": "render-worker",
      "revision_committed_at_ms": 1752624000000,
      "size_bytes": 19482,
      "content_ref": {
        "kind": "blob_v1",
        "content_id": "con_4z59p0wjvx9a8b2frdhhf6mat1",
        "size_bytes": 19482,
        "checksum": { "algorithm": "crc64nvme", "value": "42d..." }
      }
    },
    {
      "namespace_id": "demo",
      "path": "/docs/slides",
      "inode_id": "ino_000000w000000kewn0stth7erh",
      "created_by": "usr_8f3c",
      "created_at_ms": 1752623000000,
      "inode_kind": "directory",
      "head_seq": 418,
      "parent_inode_id": "ino_000000r000000tn10r0b9wf3yy",
      "display_name": "slides",
      "binding_version": "opaque-token"
    }
  ],
  "next_cursor": "7b2e2e2e7d"
}
```

### 6.6 `GET /filesystem/trash`

Lists active trash entries oldest first, in ascending `(deletion_seq, inode_id)` order, using standard `limit` and `cursor` pagination plus per-page `consistency` and `max_staleness_ms` ([Read consistency](#read-consistency)). For requests with principals, permission filtering can produce a page shorter than `limit` that still includes `next_cursor`.

Trash listing scans the derived `trash` family ([File and subtree deletion](format.md#16-file-and-subtree-deletion)) starting strictly above the retention floor, so expired deletions never appear and page cost is independent of older deletion history. Cursors created before a floor advance resume above the new floor.

Because a fork's retention floor starts at its fork point ([Advancing the retention floor](format.md#101-advancing-the-retention-floor)), the scan starts above the fork point and excludes inherited source deletions without having to scan and filter them. For example, if a namespace has a deletion at sequence 80 and is forked at sequence 100, the fork's trash omits it while the source's trash keeps showing it until the source's floor reaches 80.

Each entry includes `inode_id`, `deletion_seq` (the pair required by `restore_deleted`), `inode_kind` (clients MUST tolerate unknown kinds), `deleted_by`, `deleted_at_ms`, and `deleted_binding`. Nested deletions appear as separate entries, and restoring an outer directory doesn't remove an inner deletion from the trash:

```json
{
  "namespace_id": "demo",
  "head_seq": 418,
  "entries": [
    {
      "inode_id": "ino_000000r0000013kje0d47gc8kw",
      "inode_kind": "file",
      "deletion_seq": 417,
      "deleted_at_ms": 1752625000000,
      "deleted_by": "usr_8f3c",
      "deleted_binding": {
        "parent_inode_id": "ino_000000r000000tn10r0b9wf3yy",
        "name_key": "report.txt",
        "display_name": "report.txt"
      }
    }
  ]
}
```

### 6.7 `GET /filesystem/content` and `GET /inodes/{inode_id}/content`

Returns raw authoritative file bytes addressed by path or inode ID (metadata can appear in headers, while the body is raw file bytes rather than JSON).

The server streams and verifies content in bounded chunks. For files exceeding `download.service_proxied.max_content_bytes`, the server fails fast with `content_too_large` before reading bytes (use [ranged reads](#ranged-reads) or `filesystem.downloads.direct_get` instead).

For reads without a `Range` header, the server intentionally omits `Content-Length` so receiving `N` bytes can't signal success before full length and checksum verification completes. An I/O or checksum failure after headers are sent aborts the stream rather than writing a JSON error. Clients MUST verify clean stream completion before publishing a downloaded file (streaming directly to stdout may expose unverified prefix bytes before an abort).

Revision listings (`GET /filesystem/revisions` and `GET /inodes/{inode_id}/revisions`) return revisions newest first with standard `limit`, `cursor`, `snapshot_id`, `consistency`, and `max_staleness_ms` parameters (neither response includes paths). With `snapshot_id`, both listings read the snapshot's state (the path listing resolves the path in that state), so they start at the revision that the snapshot captured, and every page returns the snapshot's `captured_seq` as `head_seq`. A cursor from a listing with `snapshot_id` is bound to that snapshot, and every later page MUST pass the same `snapshot_id` (omitting it or passing another returns `invalid_request`). Above the retention floor, revision history is complete. At or below the floor, each file keeps its newest revision, so paging reaches revision 1 only until base compaction drops older ones (cursors resume strictly after the last returned revision, so concurrent compaction neither skips nor duplicates retained revisions):

```json
{
  "namespace_id": "demo",
  "inode_id": "ino_000000r0000013kje0d47gc8kw",
  "head_seq": 418,
  "revisions": [
    {
      "inode_id": "ino_000000r0000013kje0d47gc8kw",
      "revision_no": 7,
      "committed_seq": 418,
      "committed_at_ms": 1752624000000,
      "committed_by": "render-worker",
      "content_ref": {
        "kind": "blob_v1",
        "content_id": "con_4z59p0wjvx9a8b2frdhhf6mat1",
        "size_bytes": 19482,
        "checksum": { "algorithm": "crc64nvme", "value": "42d..." }
      }
    }
  ],
  "next_cursor": "7b2e2e2e7d"
}
```

Inode-addressed content routes:

- **`GET /inodes/{inode_id}/content`.** Streams and verifies the current revision of a visible file inode regardless of renames (requiring `read`, while deleted or unknown inodes return `inode_not_found` and directories return `wrong_kind`). With `snapshot_id`, reads the snapshot's revision and also requires `history`.
- **`GET /inodes/{inode_id}/revisions/{revision_no}/content`.** Reads a specific revision without path resolution, keeping trashed files readable while their deletion is above the retention floor. Once the retention floor reaches the deletion, both this route and inode revision listing return `inode_not_found` for every hidden inode regardless of rights ([Paths, listings, and revision reads](format.md#44-paths-listings-and-revision-reads)). Directories return `wrong_kind`, missing inodes return `inode_not_found`, and missing or pruned revisions return `revision_not_found`. With `snapshot_id` (user or fork snapshot, see [Snapshots](#snapshots)), looks up the revision in the snapshot manifest (keeping it readable past the floor, rejecting post-snapshot revisions with `revision_not_found`, and requiring `history`).

All three content routes accept `consistency` and `max_staleness_ms` for live reads (neither can be combined with `snapshot_id`).

In the embedded runtime, by-reference reads require administrator access to the reading namespace and a matching content publication in that namespace's read view (including inherited fork publications). Unreferenced content returns `path_not_found` without reading bytes, even if uploaded to that namespace, and content published after a snapshot isn't readable through that snapshot.

#### Ranged reads

Any of the three content routes accepts an HTTP `Range` header to return a single byte slice of a file (alongside `revision_no`, `snapshot_id`, `consistency`, and `max_staleness_ms`). Two forms are supported:

- `bytes=start-end`: inclusive byte offsets (`bytes=0-99` returns the first 100 bytes, and an `end` past EOF clamps to the last byte, so `bytes=950-1999` on a 1,000-byte file returns the last 50 bytes).
- `bytes=start-`: bytes from `start` through EOF.

Any other `Range` header (suffix ranges like `bytes=-500`, multi-ranges, `end < start`, non-`bytes` units, or multiple `Range` headers) returns `400 invalid_request` with `param` set to `Range`, rather than silently falling back to `200` with the full file (which could trigger `content_too_large` or mislead callers that don't check the status code). A range starting at or past EOF returns `416 range_not_satisfiable` with `details.size_bytes` and `Content-Range: bytes */size` (a zero-byte file satisfies no range).

A satisfiable range returns `206` with `Content-Range: bytes start-end/size` and `Content-Length` equal to the returned slice length. Both `200` and `206` responses include `Accept-Ranges: bytes`. Because ranged reads aren't verified against the whole-file checksum, `Content-Length` lets the client's HTTP library verify slice length (unlike whole-file reads, which omit `Content-Length` until checksum verification finishes).

Verification and transfer rules for ranged reads:

- **Partial vs whole-object checks.** The server checks that each ranged GET to object storage returns the exact requested byte count. When a GET reads a complete [span object](glossary.md#span-object), the server also verifies that object's stored layout checksum (aborting on mismatch like a whole-file read). Sub-ranges inside an object aren't checksum-verified ([Content verification](format.md#45-content-verification)), so clients needing end-to-end verification read the whole file or verify the checksum after reading all ranges.
- **Limits.** `download.service_proxied.max_content_bytes` applies to the returned range length rather than total file size (so a 100 MiB slice of a 1 GiB file succeeds under a 256 MiB cap, while a 300 MiB slice fails with `content_too_large`). Ranged reads still count against `download.service_proxied.max_concurrent_requests` until the body finishes or drops.
- **No materialization writes.** The server serves object bytes via ranged GETs and pending WAL pieces directly from the in-memory WAL tail. Unlike direct download requests (which must materialize pending pieces into content objects to sign presigned URLs), ranged reads MUST NOT write content objects.

### 6.8 `POST /commits`

Commits an ordered list of operations to a namespace, implementing the commit model in [Commit identity and preconditions](#51-commit-identity-and-preconditions). The `Loonfs-Actor` header is required. The JSON body contains `commit_id`, optional `message`, optional `preconditions`, and a required non-empty `operations` array (an empty array returns `invalid_request`).

The root path `/` is readable, and `update_access` is the only mutation that can target it. Any other operation targeting `/` (as its primary path or as either endpoint of a move or copy) returns `invalid_request` as an operation error (reporting `operation_index` inside a batch). Namespace existence is checked first, so a root mutation on an unknown namespace returns `namespace_not_found`.

Representative single-operation commit:

```text
Loonfs-Actor: usr_8f3c
Loonfs-Context: service
```

```json
{
  "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
  "preconditions": [
    { "kind": "namespace_head", "expected_head_seq": 42 },
    { "kind": "file_revision", "inode_id": "ino_000000r0000013kje0d47gc8kw", "expected_revision_no": 3 }
  ],
  "operations": [
    {
      "kind": "move_path",
      "source_path": "/docs/report.txt",
      "destination_path": "/reports/report.txt",
      "behavior": "replace"
    }
  ]
}
```

A single-operation request is just a one-element batch, so convenience calls and explicit batches generate the exact same commit and fingerprint and replay against each other.

Operations in a request commit atomically in order, and operation `k` sees the effects of operations `0..k`. For example, one commit can create a directory, write a file into it, and remove the source file:

```text
Loonfs-Actor: usr_8f3c
Loonfs-Context: service
```

```json
{
  "commit_id": "c_2a41d0c6b9f34e7d8a1b5c9e0f234567",
  "message": "import the January report",
  "content_tokens": [
    {
      "content_ref": { "kind": "blob_v1", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "..." } },
      "token": "opaque-server-token"
    }
  ],
  "operations": [
    { "kind": "create_directory", "path": "/reports/2026" },
    {
      "kind": "put_file",
      "path": "/reports/2026/january.pdf",
      "content_ref": { "kind": "blob_v1", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "..." } },
      "behavior": "no_replace"
    },
    {
      "kind": "delete_path",
      "path": "/inbox/january.pdf"
    }
  ]
}
```

If any operation fails, the whole commit aborts with nothing visible and the error reports `operation_index` (for example, if operation 1 raced another writer):

```json
{
  "code": "path_conflict",
  "message": "destination `/reports/2026/january.pdf` already exists",
  "request_id": "req_4k8m2v9qx7d3hn5tbwa6jc1rfe",
  "details": {
    "commit_id": "c_2a41d0c6b9f34e7d8a1b5c9e0f234567",
    "operation_index": 1
  }
}
```

Three operations write whole-file content, each requiring exactly one of `content_ref` (uploaded content) or `inline_content` (supplying both or neither returns `invalid_request` before planning, and unknown fields are rejected):

| Operation | Other fields (`?` means optional) |
| --- | --- |
| `put_file` | `path`, `behavior?`, `expected_inode_id?`, `expected_revision_no?` |
| `create_file_by_inode` | `parent_inode_id`, `display_name` |
| `put_file_revision_by_inode` | `inode_id`, `expected_revision_no` |

For uploaded content, the client includes its proof in `content_tokens` (one token covers all operations using its `content_ref`, and unused tokens are ignored). For inline content, the client encodes the full file as standard padded base64 with no content token (an empty string writes an empty file, and the server assigns the content ID and computes size and checksum):

```text
Loonfs-Actor: usr_8f3c
Loonfs-Context: service
```

```json
{
  "commit_id": "c_6d2a8f013e9b4c57a0f6d3b8217c95e4",
  "operations": [
    {
      "kind": "put_file",
      "path": "/hello.txt",
      "inline_content": "aGVsbG8K"
    }
  ]
}
```

Inline writes are enabled by default up to 64 KiB, advertised via `filesystem.commits.inline_content` and `commit.max_inline_content_bytes_per_operation` (when disabled, both keys are omitted and inline requests return `not_supported` with `feature` `filesystem.commits.inline_content`). The per-operation limit applies before base64 encoding (larger values fail before any storage write), and the entire JSON request body MUST fit within `commit.max_request_body_bytes` (2 MiB by default). Even if the server stores some inline files as separate content objects to stay within WAL limits, all operations commit atomically and retries still send the same inline bytes and `commit_id` ([Commit responses and safe retry](#52-commit-responses-and-safe-retry)).

`append_file` and `append_file_by_inode` add bytes to an existing file as its next revision:

| Operation | Other fields (`?` means optional) |
| --- | --- |
| `append_file` | `path`, `inline_content`, `expected_inode_id?`, `expected_revision_no?` |
| `append_file_by_inode` | `inode_id`, `inline_content`, `expected_revision_no?` |

```text
Loonfs-Actor: usr_8f3c
Loonfs-Context: service
```

```json
{
  "commit_id": "c_3e1f0a9b8c7d4e2f9a1b0c8d7e6f5a43",
  "operations": [
    {
      "kind": "append_file",
      "path": "/logs/app.log",
      "inline_content": "c2Vjb25kIGxpbmUK"
    }
  ]
}
```

Append rules:

- **Payload size.** `inline_content` is standard padded base64 encoding 1 byte to 256 KiB (262,144 bytes) before encoding. Bytes are stored in the WAL object, so appends need no upload, no content token, and don't depend on `filesystem.commits.inline_content`. Empty values return `invalid_request` (with `param`), and larger values return `content_too_large`.
- **Target and preconditions.** The target MUST be a visible file (directories return `wrong_kind`, missing targets return `path_not_found` or `inode_not_found`, and appends never create files). `expected_inode_id` and `expected_revision_no` behave like a replacing `put_file` (`path_conflict` or `stale_revision` on mismatch, where on a path `expected_revision_no` requires `expected_inode_id`, while by inode it stands alone). Without preconditions, concurrent appends compose in commit order without losing bytes, and multiple appends (or a `put_file` followed by an append) inside one commit compose in operation order.
- **Content chain behavior.** Given the current revision's content ID `C` and size `S`, if this namespace created `C`'s chain ([File contents and ownership](format.md#15-file-contents-and-ownership)), `S > 0`, and the current revision is the chain's head ([Operations and WAL deltas](format.md#66-operations-and-wal-deltas)), the new revision reuses `C` starting at offset `S` with no layout delta and no content-object write. Otherwise (empty base file, restored older revision, appended copy, or chain created by another pool member such as a fork's source), the new revision allocates a new content ID created by this namespace whose layout shares the base's objects (materializing any unfolded base bytes as an extent first). Either way, the `content_changed` event includes `append_offset` set to `S` ([`GET /changes`](#611-get-changes)).
- **Checksums and retries.** Earlier revisions stay readable. The writer combines the base CRC with the appended bytes' CRC without reading prior bytes (a mismatched checksum algorithm returns `namespace_corrupt`). Retrying with the same `commit_id` and bytes replays the commit, while different bytes return `commit_id_reuse_conflict`.

Move and copy accept the same `behavior` values as `put_file`:

- `no_replace` (default): fails with `path_conflict` if the destination is occupied.
- `replace`: replaces an occupied file destination (directories can't be replaced, and a path never replaces itself). A replacing move deletes the destination file and rebinds the source in one commit, while a replacing copy appends a new revision to the destination inode (preserving its identity and revision history).

Path-addressed replacing operations accept content-level preconditions (`expected_inode_id` and optional `expected_revision_no` on `put_file`, or `expected_destination_inode_id` and optional `expected_destination_revision_no` on move and copy):

- These preconditions require `behavior: "replace"`, and a revision precondition MUST be paired with its matching inode precondition (a revision-only precondition returns `invalid_request`).
- An inode mismatch returns `path_conflict`, a revision mismatch returns `stale_revision`, and a missing destination returns `path_not_found`.
- These preconditions check content revisions rather than attribute revisions (so an attribute-only update doesn't block a replacing put or copy, and callers use `expected_attributes_revision_no` for attribute concurrency). A replacing move deletes the destination inode and its attributes.

Ten operations address targets by inode ID so mutations remain valid even if the entry moved concurrently (unknown or hidden inodes return `inode_not_found`):

- **`create_directory_by_inode` and `create_file_by_inode`.** Create-only under an existing parent directory (`path_conflict` if the name is taken).
- **`put_file_revision_by_inode`.** Appends a revision to a file regardless of its path, requiring `expected_revision_no` (`stale_revision` on mismatch).
- **`append_file_by_inode`.** Appends bytes to a file by inode (described above).
- **`move_by_inode` and `delete_by_inode`.** Require `expected_binding_version` ([Commit identity and preconditions](#51-commit-identity-and-preconditions)) and match the destination, replacement, and recursive-delete rules of `move_path` and `delete_path`. The root can't be moved or deleted.
- **`copy_by_inode`.** Copies a file inode's current revision to `destination_display_name` under `destination_parent_inode_id`, matching `copy_path` for `behavior`, preconditions, and attributes.
- **`restore_revision_by_inode`, `update_attributes_by_inode`, and `update_access_by_inode`.** Match their path counterparts while targeting `inode_id` directly, so `expected_attributes_revision_no` and `expected_access_revision_no` stand alone without `expected_inode_id`. Targeting the root inode returns `invalid_request` except on `update_access_by_inode`.

Example inode-addressed batch and its `Commit` response (returned only after the numbered WAL put succeeds, see [Commit responses and safe retry](#52-commit-responses-and-safe-retry)):

```text
Loonfs-Actor: usr_8f3c
Loonfs-Context: service
```

```json
{
  "commit_id": "c_1b2c3d4e5f60718293a4b5c6d7e8f901",
  "operations": [
    {
      "kind": "create_directory_by_inode",
      "parent_inode_id": "ino_00000000000000000000000000",
      "display_name": "archive"
    },
    {
      "kind": "move_by_inode",
      "inode_id": "ino_000000r0000013kje0d47gc8kw",
      "expected_binding_version": "opaque-token",
      "destination_parent_inode_id": "ino_00000s4000000qnxsdtx1c5z9t",
      "destination_display_name": "january.pdf",
      "behavior": "replace"
    }
  ]
}
```

```json
{
  "namespace_id": "demo",
  "commit_id": "c_1b2c3d4e5f60718293a4b5c6d7e8f901",
  "committed_seq": 419,
  "committed_by": "usr_8f3c",
  "committed_at_ms": 1752624000000,
  "events": [
    {
      "kind": "directory_created",
      "inode_id": "ino_00000t6000000tw2yg4n7xytth",
      "parent_inode_id": "ino_00000000000000000000000000",
      "display_name": "archive",
      "binding_version": "opaque-token"
    },
    {
      "kind": "moved",
      "inode_id": "ino_000000r0000013kje0d47gc8kw",
      "source_parent_inode_id": "ino_000000r000000tn10r0b9wf3yy",
      "source_display_name": "report.txt",
      "destination_parent_inode_id": "ino_00000s4000000qnxsdtx1c5z9t",
      "destination_display_name": "january.pdf",
      "binding_version": "opaque-token"
    }
  ]
}
```

Directory creation and revision restore by path:

```text
Loonfs-Actor: usr_8f3c
Loonfs-Context: service
```

```json
{
  "commit_id": "c_8b7d4ef098ec4c1fbde15edbe02f9a64",
  "operations": [{ "kind": "create_directory", "path": "/docs" }]
}
```

```text
Loonfs-Actor: usr_8f3c
Loonfs-Context: service
```

```json
{
  "commit_id": "c_8f9a1b2c3d4e4f50a6b7c8d9e0f12345",
  "operations": [
    {
      "kind": "restore_revision",
      "path": "/docs/report.txt",
      "source_revision_no": 3
    }
  ]
}
```

`restore_deleted` restores a deleted file or subtree by rebinding the deletion's root inode (preserving its identity and retained revision history), identified by `(inode_id, deletion_seq)`:

```text
Loonfs-Actor: usr_8f3c
Loonfs-Context: service
```

```json
{
  "commit_id": "c_5d6e7f8091a2b3c4d5e6f70812345678",
  "operations": [
    {
      "kind": "restore_deleted",
      "inode_id": "ino_000000r0000013kje0d47gc8kw",
      "deletion_seq": 17,
      "destination_path": "/docs/report.txt"
    }
  ]
}
```

Destination and matching rules for `restore_deleted`:

- If `destination_path` is provided, its parent MUST be a visible directory and the name MUST be free.
- If `destination_parent_inode_id` and `destination_display_name` are provided (required together, and mutually exclusive with `destination_path`, otherwise `invalid_request` with `param`), that parent MUST be a visible directory and the name MUST be free.
- If neither destination form is provided, the entry restores in place under its recorded parent inode (working even if ancestor directories were renamed) and original name, subject to the same parent-visibility and free-name checks.
- Only the root of an active trash deletion can be restored, and `deletion_seq` MUST match the trash entry (`not_deleted` with expected and actual sequences on mismatch, or `not_deleted` if the deletion expired at or below the retention floor, see [Retention jobs](#retention-jobs)).

`update_attributes` sets and removes keys on any visible file or directory:

```text
Loonfs-Actor: usr_8f3c
Loonfs-Context: service
```

```json
{
  "commit_id": "c_6e7f8091a2b3c4d5e6f7081234567890",
  "operations": [
    {
      "kind": "update_attributes",
      "path": "/docs/report.txt",
      "set": {
        "owner": "ada",
        "tags": "draft,review"
      },
      "remove": ["stage"],
      "expected_inode_id": "ino_000000r0000013kje0d47gc8kw",
      "expected_attributes_revision_no": 3
    }
  ]
}
```

An attribute map maps validated keys to plain UTF-8 strings (empty string is a stored value, so removing a key requires listing it in `remove`, and callers needing list values choose their own string encoding). Limits are measured in logical UTF-8 bytes without framing ([Attributes and attribution](format.md#17-attributes-and-attribution)):

| Constant | Value | Bound |
| --- | --- | --- |
| `MAX_ATTRIBUTE_KEY_BYTES` | 128 | Longest attribute key. |
| `MAX_ATTRIBUTE_VALUE_BYTES` | 4,096 | Longest attribute value. |
| `MAX_ATTRIBUTE_ENTRIES` | 100 | Most entries in one map. |
| `MAX_ATTRIBUTES_TOTAL_BYTES` | 65,536 | Largest map (sum of all key and value bytes). |

Attribute update rules:

- `set` upserts listed keys while leaving unlisted keys intact, and `remove` deletes listed keys (both default to empty). A request returns `invalid_request` if both are empty, a key appears in both, a key repeats in `remove`, any key uses the reserved `loonfs.` prefix, or the resulting map exceeds any limit above.
- Every accepted update increments `attributes_revision_no` (even if the map didn't change), stores the full resulting map, and emits an `attributes_changed` event (replaying the same `commit_id` returns the original commit without incrementing again).
- Attributes stay with the inode across moves, renames, new file revisions, revision restores, and delete/restore cycles. Only a copy to a vacant destination copies attributes (starting the new inode at attribute revision 1 with its own event), while a replacing copy changes only destination content.
- Optional `expected_inode_id` and `expected_attributes_revision_no` are part of commit identity. `expected_attributes_revision_no` requires `expected_inode_id` (otherwise `invalid_request` before path resolution). A wrong inode returns `path_conflict`, and a stale revision returns `stale_attributes`. Even when `expected_attributes_revision_no` is omitted, the update implicitly preconditions on the revision it read so concurrent updates return `stale_attributes`.

`update_access` replaces the complete access row of any visible inode, including `/` (where namespace administrators are configured):

```text
Loonfs-Actor: usr_8f3c
Loonfs-Context: service
```

```json
{
  "commit_id": "c_7f8091a2b3c4d5e6f70812345678901a",
  "operations": [
    {
      "kind": "update_access",
      "path": "/docs/secret",
      "boundary": true,
      "grants": {
        "prn_finance": ["read", "history"],
        "prn_ada": ["read", "history", "write", "create", "remove", "share", "manage"]
      },
      "expected_inode_id": "ino_000000y000000vk0224rf3qytb",
      "expected_access_revision_no": 2
    }
  ]
}
```

Both `boundary` and `grants` are required and replace the whole row (an empty `grants` map clears all direct grants, and `boundary: false` resumes inheritance). `grants` maps each principal ID to a non-empty list of distinct right names ([Access rows](format.md#18-access-rows)). Duplicate principals or rights, empty grant lists, `admin` on any non-root inode, or `boundary: true` on a file all return `invalid_request`. In the service context, `update_access` can configure any row (including initial root administrators). Every accepted update advances `access_revision_no` and emits an `access_changed` event with the full new row. `expected_inode_id` and `expected_access_revision_no` follow the exact same rules as attribute preconditions (including `path_conflict`, `stale_access`, and implicit revision checking).

#### Authorization

Requests in the service context skip all access checks and read no access rows. For requests with `Loonfs-Principals`, LoonFS checks the subject's effective rights on touched inodes before conflict checks:

- **No rights on a checked inode:** returns `path_not_found` or `inode_not_found` (matching how the request addressed it) so unauthorized callers can't probe existence.
- **Some rights, missing a required right:** returns `forbidden` with the checked `inode_id` in `details`.

Required rights for mutations:

| Operation | Required rights |
| --- | --- |
| create_directory, new put_file | `create` on the parent, or on the deepest existing directory when missing parents are created. |
| create_directory_by_inode, create_file_by_inode | `create` on the parent. |
| Replacing put_file, put_file_revision_by_inode, append_file, append_file_by_inode | `write` on the existing file. |
| put_file with no_replace at an occupied name | `create` on the parent, before the conflict is reported. |
| delete_path, delete_by_inode | `remove` on the source parent. |
| move_path, move_by_inode | `remove` on the source parent, `create` on the destination parent, and `read` on the moved inode. Replacing an occupant also requires `remove` on the destination parent. |
| copy_path, copy_by_inode | `read` on the source file, plus either `create` on the destination parent (for a vacant name or a name conflict) or `write` on the occupied file being replaced. |
| restore_revision, restore_revision_by_inode | `write` and `history` on the file. |
| update_attributes, update_attributes_by_inode | `write` on the target inode. |
| restore_deleted | `remove` on the saved parent and `create` on the destination parent, including when restoring in place. A restore to a destination other than the saved parent also requires `read` on the restored inode. |
| update_access, update_access_by_inode | Authority for the changes, as described below. |

Additional mutation authority rules:

- **Privilege-widening moves and relocating restores.** If moving (or relocating on restore) an inode grants any non-admin principal new effective rights on it, the mover MUST have `share` plus every gained right, or `manage`, or administrator rights. Narrowing access, same-directory renames, and moving a `boundary` directory (whose inherited rights don't change) need only the table rights.
- **`update_access`.** Changing root administrator principals requires an administrator. Otherwise, `manage` allows any access update, while a subject without `manage` MUST have `share`, keep `boundary` unchanged, and have every right added to or removed from any principal's direct grant.
- **Preconditions.** Inode preconditions require `read` on the referenced inode, `path_absence` requires `read` on the deepest existing parent, `name_absence` requires `read` on the parent, and `namespace_head` needs no inode right.

The same not-found versus `forbidden` rule applies to reads:

| Read operation | Required rights |
| --- | --- |
| Stat, directory listing, current content, or current download, by path or inode | `read` on the target inode. |
| Recursive directory listing, by path or inode | `read` on the target inode. The listing descends only into the directories below it where the subject also has `read`. |
| Older content revision or historical download, by path or inode | `read` and `history` on the target inode. |
| Revision listing, by path or inode | `read` and `history` on the target inode. |
| Snapshot stat, listing, content, or download | `read` and `history` on the historical inode, evaluated at the current head. Administrator, when an expired deletion (one at or below the current head's retention floor) hides the inode at the current head, or the inode's ancestry there is disconnected. |
| Trash listing | `read` on each deletion's saved original parent. |
| Batch file resolution and grep candidates | `read` on the candidate inode. Snapshot resolution also requires `history`. |
| Change feed, diff, a fork's next merge range, or bare content reference read | Administrator, and for a fork's next merge range, administrator in the fork too. |

Special read-filtering and snapshot authorization rules:

- **Directory listings.** Having `read` on a directory reveals child names, while any child the subject can't read omits `attributes` and (for files) `content_ref` (preventing checksum-based content guessing). Because boundaries apply only to directories and grants are additive, a listed file can only be unreadable in a snapshot listing if it later moved under an unreadable directory. Recursive listings return unreadable child directories without descending into them.
- **Snapshot reads and expired subtrees.** Snapshot rights are evaluated at the current head on the historical inode (never on whatever inode currently occupies that path) and require `history`. Once an expired deletion hides that inode at the current head (or leaves its ancestry disconnected, see [Paths, listings, and revision reads](format.md#44-paths-listings-and-revision-reads)), base compactions remove rows one family group at a time, which could temporarily drop a `boundary` row while an ancestor grant remains and widen non-admin rights. To prevent that, reading an expired subtree through a snapshot requires an administrator (all other subjects receive `path_not_found` or `inode_not_found`).
- **Trash, grep, and admin routes.** Trash listings filter out deletions whose saved parent the subject can't read (so pages may be short with `next_cursor`). Grep filters unreadable candidates before reading file bytes, though filtered candidates still count against the page candidate budget. Change feeds, diffs, next-merge-range queries, and bare content-reference reads require an administrator when using principals (or service context). Snapshot inventory and maintenance routes ignore both identity headers.

### 6.9 Upload transport

Upload routes stage a file's complete content in an upload session so a later commit can reference it (every upload in v0 is a whole-file upload).

An upload is the only operation that spans multiple HTTP requests, using the upload session as server-side state between steps. All other operations complete in one request (paged listings and change feeds keep their cursor on the client, and recursive CLI commands such as `put -r`, `cp -r`, and `get -r` drive a sequence of independent requests with no server-side batch state).

Every upload operation requires `Loonfs-Actor`, and the session belongs to the actor that opened it (whether opened with principals or in the service context, see [Identity headers](#identity-headers)). Any request from a different actor returns `upload_not_found`, even with the same principals.

Each upload step has a single clear effect:

- `PUT /content` writes the immutable whole-file object and records the staged `content_ref`.
- `complete` verifies the uploaded bytes and records the final `content_ref` (plus an opaque `content_token` for remote commit admission).
- Once completed, commits can reference `content_ref` using that `content_token`.

`CreateUploadBody` selects the transport via `mode`, with mode-specific fields as siblings of `mode`:

```json
{ "mode": "service_proxied" }
{ "mode": "direct_put", "size_bytes": 1234 }
{ "mode": "direct_multipart", "part_size_bytes": 8388608 }
```

- `service_proxied`: no extra fields.
- `direct_put`: optional advisory `size_bytes`.
- `direct_multipart`: optional `part_size_bytes` (defaults to 8 MiB).

`CompleteUploadBody` uses the same `mode` values (and MUST match the session's mode):

```json
{ "mode": "service_proxied" }
{ "mode": "direct_put", "content": { "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "<16 lowercase hex>" } } }
{ "mode": "direct_multipart", "content": { "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "<16 lowercase hex>" } }, "parts": [] }
```

`direct_put` completion requires `content`, `direct_multipart` requires `content` and `parts`, and `service_proxied` accepts no extra fields. Unknown fields, missing required fields, and mode mismatches return `invalid_request`.

Every upload route (`create_upload`, `put_upload_content`, `get_upload`, `complete_upload`, `abort_upload`) returns a flat `UploadSession` object with status- and mode-specific fields as siblings (omitting inapplicable fields, and response readers accept unknown fields). An `open` session includes `mode`, `status`, `expires_at_ms`, and `checksum_algorithm`, plus:

- `part_size_bytes` for `direct_multipart`.
- `transfer` (a freshly minted presigned PUT URL on creation and every `GET`) for `direct_put`.
- `content_ref` (once staged via `PUT .../content`) for `service_proxied`.

Each session allocates its own content object at creation (content is never deduplicated across sessions). Repeating `PUT /content` with identical bytes for the same upload ID is idempotent, while different bytes return `upload_content_conflict`. Completing a `service_proxied` upload before staging fails, and commits never download external objects to make up for a missing proof.

Upload sessions follow a simple three-state machine where `completed` and `aborted` are terminal ([Upload sessions](format.md#51-upload-sessions)):

```text
+------+   complete   +-----------+
| open | -----------> | completed |
+------+              +-----------+
   |
   | abort, or cleanup after expiry
   v
+---------+
| aborted |
+---------+
```

Representative flat session objects for each status:

```json
{ "namespace_id": "demo", "upload_id": "upl_...", "mode": "direct_multipart", "status": "open", "expires_at_ms": 1730000000000, "checksum_algorithm": "crc64nvme", "part_size_bytes": 8388608 }
{ "namespace_id": "demo", "upload_id": "upl_...", "mode": "direct_put", "status": "completed", "completed_at_ms": 1730000001000, "content_ref": { "kind": "blob_v1", "content_id": "con_...", "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "<16 hex>" } }, "content_token": { "content_ref": { "kind": "blob_v1", "content_id": "con_...", "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "<16 hex>" } }, "token": "<opaque>" } }
{ "namespace_id": "demo", "upload_id": "upl_...", "mode": "service_proxied", "status": "aborted", "aborted_at_ms": 1730000002000 }
```

State transition rules:

- **`open` expiry.** `PUT /content`, part signing, and `complete` work strictly before `expires_at_ms`. At or after `expires_at_ms`, all three return `upload_not_found` until upload cleanup aborts the session (`GET` still shows `open` and `abort` still works).
- **`completed`.** Completion records and returns `completed_at_ms` (idempotent replays return the same timestamp). `GET` on a completed session returns a fresh `content_token` while its token-issuing window is open, omitting the field afterward. Aborting a completed session fails with `upload_already_completed`.
- **`aborted`.** `POST /uploads/{upload_id}/abort` ends an open session and deletes its staged object (repeating it succeeds with the original `aborted_at_ms`). `PUT /content`, part signing, and `complete` all return `upload_not_found` on an aborted session, matching the response after cleanup removes the record (if completion wins the CAS race against cleanup, the completed session stays).

Clients can stream large payloads in a single pass with bounded memory:

1. Begin a `direct_multipart` session (no upfront size or digest required, and the response sets `part_size_bytes`).
2. Read one part at a time, computing each part's `crc64nvme` while feeding the same bytes into a running whole-object `crc64nvme`.
3. Request part URLs in waves (up to 1,000 parts per sign request), upload each wave, and record `{part_number, etag, crc64nvme}` locally (retrying any failed part by requesting a fresh URL).
4. Keep only a bounded window of in-flight parts in memory, independent of total file size.
5. Complete the upload with the part list and measured `{size_bytes, crc64nvme}` (aborting the session if streaming fails partway so incomplete data is cleaned up).

Providers allow at most 10,000 parts per session (max size `part_size_bytes * 10,000`), refusing part signing past part 10,000 so very large uploads must pick a larger `part_size_bytes` upfront.

SDK file helpers select upload transports in priority order after checking whether the payload fits inline ([Commit responses and safe retry](#52-commit-responses-and-safe-retry)) or under one part via `PUT /content` (if within the proxy limit):

1. `direct_multipart` when advertised (per-part retries and no upfront length needed).
2. `direct_put` when advertised and `size_bytes <= upload.direct_put.max_content_bytes` (single-pass direct write for providers without multipart signing).
3. Streaming `PUT /content` when `size_bytes <= upload.service_proxied.max_content_bytes` (using chunked transfer encoding if length is unknown, enforced as bytes arrive).

Clients MUST check advertised capability limits rather than hardcoded sizes, refusing payloads that exceed all available transports locally instead of sending them to fail at the proxy. Because declared lengths are advisory hints that can go stale, clients don't reject uploads from hints alone (both direct modes measure exact bytes during transfer).

A `content_token` is a short-lived upload receipt scoped to `{namespace, content_ref}`, issued only for already-completed sessions. Because commits normally follow uploads promptly and reading the completed session mints a fresh token without re-uploading any bytes, short token lifetimes add safety without complicating retries. Once the token-issuing window closes, `GET` omits `content_token`, and unreferenced staged content becomes eligible for cleanup ([Upload-session cleanup](format.md#116-upload-session-cleanup)).

Representative begin-upload response:

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_2dhwp939xk9r7srv8t5cz5yw4c",
  "mode": "service_proxied",
  "status": "open",
  "expires_at_ms": 1730000000000,
  "checksum_algorithm": "crc64nvme"
}
```

Representative content-upload response:

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_2dhwp939xk9r7srv8t5cz5yw4c",
  "mode": "service_proxied",
  "status": "open",
  "expires_at_ms": 1730000000000,
  "checksum_algorithm": "crc64nvme",
  "content_ref": {
    "kind": "blob_v1",
    "content_id": "con_4z59p0wjvx9a8b2frdhhf6mat1",
    "size_bytes": 20591,
    "checksum": { "algorithm": "crc64nvme", "value": "7ab..." }
  }
}
```

Representative complete-upload request:

```json
{ "mode": "service_proxied" }
```

Representative complete-upload response:

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_2dhwp939xk9r7srv8t5cz5yw4c",
  "mode": "service_proxied",
  "status": "completed",
  "completed_at_ms": 1730000001000,
  "content_ref": {
    "kind": "blob_v1",
    "content_id": "con_4z59p0wjvx9a8b2frdhhf6mat1",
    "size_bytes": 20591,
    "checksum": { "algorithm": "crc64nvme", "value": "7ab..." }
  },
  "content_token": {
    "content_ref": {
      "kind": "blob_v1",
      "content_id": "con_4z59p0wjvx9a8b2frdhhf6mat1",
      "size_bytes": 20591,
      "checksum": { "algorithm": "crc64nvme", "value": "7ab..." }
    },
    "token": "opaque-server-token"
  }
}
```

### 6.10 Download transport

Download routes issue short-lived presigned GET URLs so clients can stream file bytes directly from object storage. Any deployment advertising a direct write feature MUST also advertise `filesystem.downloads.direct_get`, ensuring objects written directly beyond `download.service_proxied.max_content_bytes` can be read back directly without routing every byte through the server.

A direct download returns one presigned range per immutable extent. If some bytes of the revision are still pending WAL pieces, LoonFS materializes them into a new span object using the same writer as a fold (never rewriting existing objects, so concurrent downloads or a later fold each write their own span object, and zero-byte references materialize an empty whole object if missing). If the deployment can't materialize pending pieces, the route returns `content_not_materialized`.

Path download (`POST /v0/namespaces/{ns}/filesystem/downloads`) takes `path`, optional `revision_no` or `snapshot_id` (mutually exclusive), optional `start_offset` (default 0), and optional live-read `consistency` and `max_staleness_ms` (mutually exclusive with `snapshot_id`, with invalid values reporting JSON Pointer `param` such as `/consistency`, see [Read consistency](#read-consistency)):

```json
{ "path": "/docs/report.txt", "revision_no": 3, "start_offset": 1048576 }
```

```json
{
  "namespace_id": "demo",
  "path": "/docs/report.txt",
  "revision_no": 3,
  "content_ref": {
    "kind": "blob_v1",
    "content_id": "con_4z59p0wjvx9a8b2frdhhf6mat1",
    "size_bytes": 314572800,
    "checksum": { "algorithm": "crc64nvme", "value": "42d..." }
  },
  "ranges": [{
    "start_offset": 1048576,
    "length": 313524224,
    "transfer": {
      "kind": "presigned_url",
      "method": "GET",
      "url": "https://bucket.s3.us-east-1.amazonaws.com/...&X-Amz-Signature=...",
      "headers": { "range": "bytes=1048576-314572799" },
      "expires_at_ms": 1780000000000
    }
  }]
}
```

Inode-addressed download routes use the same provider checks and response shape (with `inode_id` instead of `path`):

- **`POST /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{revision_no}/downloads`.** Accepts an optional body with `snapshot_id`, `start_offset`, `consistency`, and `max_staleness_ms`. Survives renames and trash deletions while above the retention floor (returning `inode_not_found` once the floor reaches the deletion, see [Paths, listings, and revision reads](format.md#44-paths-listings-and-revision-reads)). With `snapshot_id`, looks up the revision in the snapshot manifest (rejecting post-snapshot revisions with `revision_not_found`).
- **`POST /v0/namespaces/{ns}/inodes/{inode_id}/downloads`.** Accepts the same optional body for the current revision of a visible file inode (or the snapshot's revision when `snapshot_id` is set), following renames and returning `inode_not_found` once deleted.

```json
{
  "namespace_id": "demo",
  "inode_id": "ino_000000r0000013kje0d47gc8kw",
  "revision_no": 3,
  "content_ref": {
    "kind": "blob_v1",
    "content_id": "con_4z59p0wjvx9a8b2frdhhf6mat1",
    "size_bytes": 314572800,
    "checksum": { "algorithm": "crc64nvme", "value": "42d..." }
  },
  "ranges": [{
    "start_offset": 0,
    "length": 314572800,
    "transfer": {
      "kind": "presigned_url",
      "method": "GET",
      "url": "https://bucket.s3.us-east-1.amazonaws.com/...&X-Amz-Signature=...",
      "headers": { "range": "bytes=0-314572799" },
      "expires_at_ms": 1780000000000
    }
  }]
}
```

Clients can rely on five invariants in every download response:

- **Ordered ranges.** Ranges are contiguous and ascending over `[start_offset, size_bytes)` with lengths summing to `size_bytes - start_offset`. Every range points to immutable object bytes unaffected by later commits or appends.
- **Signed headers.** Clients MUST send each range's `transfer.headers` unchanged without adding a `Range` header (object offsets may differ from revision offsets) and verify each HTTP response length matches `length`. A zero-byte revision returns one range (`start_offset: 0`, `length: 0`, no `Range` header) requiring no object fetch.
- **Resuming.** If short-lived URLs expire mid-transfer after `n` bytes, request fresh URLs with `start_offset: n` (reading no byte twice). Any `start_offset >= size_bytes` returns `invalid_request` with `param` `/start_offset`, except `0` on a zero-byte file.
- **Verification.** Clients stream ranges in order and verify total byte count and `content_ref.checksum` (CRC-64/NVME or CRC-32C) across the whole file (including any previously downloaded prefix on resume). A mismatch fails the download.
- **No object keys.** Raw object keys are never exposed.

Deployments that can't presign downloads return `501 not_supported` (`feature = "filesystem.downloads.direct_get"`). On such deployments, files that grow past `download.service_proxied.max_content_bytes` (for example, through appends) can be read via [ranged reads](#ranged-reads) and verified locally against `content_ref.checksum`.

### 6.11 `GET /changes`

Returns a page of commits after `after_seq`. Requires an administrator or the service context.

The route accepts `consistency` and `max_staleness_ms` (mutually exclusive with `snapshot_id`, see [Read consistency](#read-consistency)). An `eventual` poll within its staleness window may miss very recent commits until the next confirmation, while passing an `after_seq` ahead of the cached head triggers strong head confirmation (returning `invalid_request` if still above the confirmed head).

Each entry in `changes` is a `Commit` matching `POST /commits`, containing `committed_seq`, `commit_id`, `committed_by`, observational `committed_at_ms`, optional `message`, optional `merge` (for [merges](glossary.md#merge), which emit standard filesystem events rather than a separate merge event kind, see [Commit responses and safe retry](#52-commit-responses-and-safe-retry)), and ordered `events`. A single request operation can produce multiple events in order (for example, `put_file` creating missing parent directories, a replacing move deleting an occupant, or a copy applying copied attributes).

```json
{
  "namespace_id": "demo",
  "after_seq": 418,
  "head_seq": 419,
  "changes": [
    {
      "namespace_id": "demo",
      "committed_seq": 419,
      "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
      "committed_by": "usr_8f3c",
      "committed_at_ms": 1752624000000,
      "message": "replace report bytes",
      "events": [
        {
          "kind": "content_changed",
          "inode_id": "ino_000000r0000013kje0d47gc8kw",
          "revision_no": 8,
          "content_ref": {
            "kind": "blob_v1",
            "content_id": "con_4z59p0wjvx9a8b2frdhhf6mat1",
            "size_bytes": 20591,
            "checksum": { "algorithm": "crc64nvme", "value": "7ab..." }
          }
        }
      ]
    }
  ]
}
```

Event kinds (clients MUST ignore unrecognized kinds and fields):

| Kind | Meaning | Fields |
| --- | --- | --- |
| `directory_created` | Directory created. | `inode_id`, `parent_inode_id`, `display_name`, `binding_version`. |
| `file_created` | File created with its first revision. | `inode_id`, `parent_inode_id`, `display_name`, `binding_version`, `revision_no`, `content_ref`. |
| `content_changed` | File received a new current revision (replacing put, replacing copy, append, or revision restore). | `inode_id`, `revision_no`, `content_ref`, plus `append_offset` when produced by an append. |
| `moved` | Entry moved to a new parent or name (`source_*` is the removed binding, `destination_*` is the new binding). | `inode_id`, `source_parent_inode_id`, `source_display_name`, `destination_parent_inode_id`, `destination_display_name`, `binding_version`. |
| `deleted` | File or directory subtree deleted (use the enclosing `committed_seq` as `deletion_seq` for `restore_deleted`). | `inode_id`, plus `deleted_binding` (`parent_inode_id`, `name_key`, `display_name`). |
| `restored` | Deleted inode restored and bound to a name. | `inode_id`, `parent_inode_id`, `display_name`, `binding_version`. |
| `attributes_changed` | Attributes updated (`attributes` is the complete flat map after the update, where `{}` means cleared). | `inode_id`, `attributes_revision_no`, `attributes`. |
| `access_changed` | Access row replaced (`grants` is the complete direct grant map after the update). | `inode_id`, `access_revision_no`, `boundary`, `grants`. |

Key event rules:

- **No event for trash expiration.** Advancing the retention floor creates no commit, so consumers track expired deletions by reading `retention_floor_seq` from namespace metadata.
- **Inodes and bindings, not paths.** Events report inode IDs and parent bindings so consumers maintain a local binding projection or stat inodes as needed. Only binding-creating events (`directory_created`, `file_created`, `moved`, `restored`) include `binding_version`.
- **Distinct directory and file creation shapes:**

```json
{
  "kind": "directory_created",
  "inode_id": "ino_000000r000000tn10r0b9wf3yy",
  "parent_inode_id": "ino_00000000000000000000000000",
  "display_name": "docs",
  "binding_version": "opaque-token"
}

{
  "kind": "file_created",
  "inode_id": "ino_000000r0000013kje0d47gc8kw",
  "parent_inode_id": "ino_000000r000000tn10r0b9wf3yy",
  "display_name": "report.txt",
  "binding_version": "opaque-token",
  "revision_no": 1,
  "content_ref": {
    "kind": "blob_v1",
    "content_id": "con_4z59p0wjvx9a8b2frdhhf6mat1",
    "size_bytes": 20591,
    "checksum": { "algorithm": "crc64nvme", "value": "7ab..." }
  }
}
```

- **Incremental sync via `append_offset`.** Every append emits a `content_changed` event with `append_offset` equal to the base revision's `size_bytes` (including appends to empty files where it's `0`, appends starting a new content ID such as in a fork or after a restore, and each individual append when one commit contains several). Because an append can start a new content ID even though it extends the prior bytes, `append_offset` lets sync consumers fetch only `[append_offset, size_bytes)` (for example, via a [download request](#610-download-transport) with `start_offset: append_offset`) instead of treating a new content ID as a full replacement. Puts, copies, and revision restores omit `append_offset`:

```json
{
  "kind": "content_changed",
  "inode_id": "ino_00000ct000000sne6s8ae5c5ka",
  "revision_no": 3,
  "content_ref": {
    "kind": "blob_v1",
    "content_id": "con_2bfn591c9z1p65wthb86fjmv0e",
    "size_bytes": 1036,
    "checksum": { "algorithm": "crc64nvme", "value": "3c1..." }
  },
  "append_offset": 1024
}
```

Pagination and commit splitting:

- **Page bounds.** Pages are capped by both `limit` (commits) and an event-heap "page budget" (4 MiB in the reference implementation, counting vector capacity and owned event data). Without `split_commits=true`, commits are never split across pages, so a single commit exceeding 4 MiB is returned whole by itself. Whenever either bound truncates before `head_seq`, the response includes `next_after_seq` (the last returned `committed_seq`). Clients MUST check `next_after_seq` rather than commit count to detect more pages.
- **Cursor bounds.** `after_seq == head_seq` returns an empty page, `after_seq > head_seq` returns `invalid_request`, and `after_seq < retention_floor_seq` returns `below_retention_floor` (prompting the client to re-bootstrap from a listing or snapshot).
- **Splitting large commits (`split_commits=true`, gated by `filesystem.changes.split_commits`).** Defaults to `false`. When `true`, any single commit exceeding the page budget splits at a zero-based "event index", returning as many events as fit (at least one), setting `next_after_seq` to that commit's `committed_seq`, and setting `next_event_index` to the next unreturned event index (omitted at commit boundaries). The client passes both back as `after_seq` and `event_index` (default `0`). With `event_index > 0`, the page resumes that commit from `event_index` and continues into later commits (repeating the commit's envelope fields on each chunk so clients concatenate `events` in page order, while `limit` counts a partial commit as one commit). Passing `event_index > 0` without `split_commits=true` or out of bounds returns `invalid_request` (`param` `event_index`), and `after_seq == retention_floor_seq` with `event_index > 0` returns `below_retention_floor` because it reads the commit at `after_seq`.

For example, when commit 419 is a 30,000-file merge, `GET /changes?after_seq=418&split_commits=true` returns a partial page with `"next_after_seq": 419` and `"next_event_index": 13107`. Following both values until `next_event_index` disappears finishes commit 419 and continues into commit 420:

```json
{
  "namespace_id": "demo",
  "after_seq": 419,
  "head_seq": 420,
  "changes": [
    {
      "namespace_id": "demo",
      "committed_seq": 419,
      "commit_id": "c_0b7e4d2a9f1c4e8b8a6d3f5c2e1b9a70",
      "committed_by": "usr_8f3c",
      "committed_at_ms": 1752624000000,
      "message": "import the archive",
      "merge": {
        "source_namespace_id": "demo-import",
        "source_after_seq": 400,
        "source_through_seq": 412
      },
      "events": [
        {
          "kind": "file_created",
          "inode_id": "ino_000000r0000013kje0d47gc8kw",
          "parent_inode_id": "ino_000000r000000tn10r0b9wf3yy",
          "display_name": "part-29999.csv",
          "binding_version": "opaque-token",
          "revision_no": 1,
          "content_ref": {
            "kind": "blob_v1",
            "content_id": "con_4z59p0wjvx9a8b2frdhhf6mat1",
            "size_bytes": 20591,
            "checksum": { "algorithm": "crc64nvme", "value": "7ab..." }
          }
        }
      ]
    },
    {
      "namespace_id": "demo",
      "committed_seq": 420,
      "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
      "committed_by": "usr_8f3c",
      "committed_at_ms": 1752624060000,
      "events": [
        {
          "kind": "attributes_changed",
          "inode_id": "ino_000000r000000tn10r0b9wf3yy",
          "attributes_revision_no": 2,
          "attributes": { "owner": "imports" }
        }
      ]
    }
  ]
}
```

### 6.12 `GET /diff`

Returns a page of differences between two sequences of a namespace: every inode whose visible state at `to_seq` differs from its visible state at `from_seq`. LoonFS finds touched inodes by scanning commits between `from_seq` and `to_seq` and comparing their states at the two endpoints, rather than listing the subtree at either sequence. Requires an administrator or the service context.

Query parameters:

| Parameter | Meaning |
| --- | --- |
| `from_seq` | Required. Exclusive start sequence whose state is compared with `to_seq`. |
| `to_seq` | Inclusive end sequence (defaults to the head read by the first page). |
| `path` | Absolute directory path scoping the diff (defaults to `/`). |
| `after_seq` | `next_after_seq` from the previous page (defaults to `from_seq`). |
| `split_commits` | Allows a page to end inside a single large commit, as on the change feed (default `false`). |
| `event_index` | `next_event_index` from the previous page, resuming the commit at `after_seq` (default `0`, requires `split_commits=true`). |
| `limit` | Maximum commits read by this page (same default and max as `GET /changes`). |
| `consistency`, `max_staleness_ms` | Head confirmation settings ([Read consistency](#read-consistency)). |

Sequence bounds MUST satisfy `retention_floor_seq <= from_seq <= after_seq <= to_seq <= head`:

- `from_seq < retention_floor_seq` returns `below_retention_floor` (with `from_seq` and `retention_floor_seq` in `details`).
- `from_seq` or `to_seq` ahead of an `eventual` reader's cached head triggers strong head confirmation (returning `invalid_request` if still above the confirmed head).
- `from_seq > to_seq`, `after_seq` outside `[from_seq, to_seq]`, or a positive `event_index` when `after_seq == from_seq`, without `split_commits=true`, or past the end of the commit's events returns `invalid_request` with `param`.
- `from_seq == to_seq` returns an empty page.

Representative response:

```json
{
  "namespace_id": "demo",
  "from_seq": 418,
  "to_seq": 425,
  "after_seq": 418,
  "next_after_seq": 421,
  "retention_floor_seq": 400,
  "entries": [
    {
      "inode_id": "ino_000000r0000013kje0d47gc8kw",
      "inode_kind": "file",
      "kind": "moved",
      "path": "/docs/archive/report.txt",
      "previous_path": "/docs/report.txt",
      "binding_version": "opaque-token",
      "revision_no": 9
    },
    {
      "inode_id": "ino_00000ct000000sne6s8ae5c5ka",
      "inode_kind": "file",
      "kind": "created",
      "path": "/docs/summary.txt",
      "binding_version": "opaque-token",
      "revision_no": 1
    }
  ]
}
```

An inode's visible state at a sequence records existence, deletion visibility ([Paths, listings, and revision reads](format.md#44-paths-listings-and-revision-reads)), and (when visible) its parent, name, content revision (for files), attribute revision, and access revision. Each differing inode yields one entry whose `kind` classifies the transition (clients MUST skip unrecognized `kind` values):

| Kind | At `from_seq` | At `to_seq` | Fields |
| --- | --- | --- | --- |
| `created` | Didn't exist. | Visible. | `path`, `binding_version`, and `revision_no` (for files). |
| `restored` | Hidden by deletion. | Visible. | `path`, `binding_version`, and `revision_no` (for files). |
| `deleted` | Visible. | Hidden by deletion. | `previous_path`. |
| `moved` | Visible at one parent/name. | Visible at another parent or name. | `path`, `previous_path`, `binding_version`, plus each revision field that changed. |
| `changed` | Visible at same parent/name. | Visible at same parent/name with different content, attribute, or access revision. | `path`, `binding_version`, plus each revision field that changed. |

Every entry includes `inode_id`, `inode_kind`, and `kind`. `path` is the path at `to_seq`, `previous_path` is the path at `from_seq`, and revision fields (`revision_no`, `attributes_revision_no`, `access_revision_no`) report values at `to_seq` (included on `moved` or `changed` only when they actually changed, so a move plus rewrite is `moved` with `revision_no`).

Subtree, scoping, and reconciliation rules:

- **Subtree entries aren't expanded.** A `deleted`, `moved`, or `restored` directory applies to its whole subtree without listing untouched descendants (keeping diff cost independent of subtree size, so clients restoring a directory list it to populate descendants, while a `created` directory needs no listing because every child inside it at `to_seq` has its own entry). Descendants that were explicitly touched by a commit in the range still appear as entries (for example, a file rewritten before its parent was deleted). An inode with identical visible state at both endpoints produces no entry even if touched in between (such as created and deleted, or deleted and restored unchanged).
- **Path scoping (`path`).** Matches entries whose `path` or `previous_path` is at or below `path` (using canonical name-key comparison from [Names and paths](format.md#14-names-and-paths)), plus any `deleted`, `moved`, or `restored` directory above `path` at either sequence (since that change affects everything below it). Ancestor attribute or access changes aren't included because an inode's state tracks only its own revisions.
- **Reconciliation by inode ID.** Because entries describe endpoint states rather than replay steps (for example, path swaps or moves into newly created directories), no single sequential apply order works in every case. Sync clients reconcile by `inode_id`.

Pagination and performance:

- Each page reads up to `limit` commits after `after_seq` within the change feed's byte budget and an examined-inode cap (16,384 inode touches in the reference implementation). Because LoonFS can check from an inode's rows at `to_seq` whether a commit is its last touch in the range, each inode is emitted once at its last touching commit without server-side state (so a page can have zero entries or more than `limit` entries and still return `next_after_seq`). Callers repeat `from_seq`, `to_seq`, and `after_seq = next_after_seq` on every page (each page re-checks `from_seq` against the current retention floor).
- With `split_commits=true`, any single commit exceeding the examined-inode cap runs on its own page and stops at `next_after_seq` + `next_event_index` (resumed with `after_seq` and `event_index`). In this mode only, an inode touched on multiple pages of a split commit appears on each of those pages with identical endpoint fields, so reconciling by `inode_id` produces the same result.
- To diff a fork against its fork point, call `GET /diff` on the fork with `from_seq` set to `fork_basis.fork_point_seq`. To test whether anything under a directory changed, read pages until one returns an entry or omits `next_after_seq`.
- Cost scales with the number of commits in `(from_seq, to_seq]` and emitted entries (not subtree size), so frequent callers should keep `from_seq` recent.

### 6.13 `POST /forks`

Creates a new namespace starting as a copy of a source namespace at the source's current head or at an unexpired user snapshot. Requires an administrator or the service context.

Representative request and response (`new_namespace_id` is optional, generated automatically when omitted, see [Creating and forking namespaces](#creating-and-forking-namespaces)):

```text
Loonfs-Actor: usr_8f3c
Loonfs-Context: service
```

```json
{
  "new_namespace_id": "demo-branch"
}
```

```json
{
  "namespace_id": "demo-branch",
  "created_at_ms": 1752625000000,
  "created_by": "usr_8f3c",
  "fork_basis": {
    "source_namespace_id": "demo",
    "fork_point_seq": 418,
    "fork_point_snapshot_id": "snp_0000000000001bk81xx02a36fb"
  },
  "pool_id": "pool_3s0drmj8em84yb4mnetqkz2j0x",
  "head_seq": 418,
  "retention_floor_seq": 418
}
```

`fork_basis` identifies the captured source state:

| Field | Meaning |
| --- | --- |
| `source_namespace_id` | Source namespace captured by the fork. |
| `fork_point_seq` | Source sequence captured as the fork point. |
| `fork_point_snapshot_id` | Fork snapshot on this namespace pinning its state at `fork_point_seq` (readable via `snapshot_id`, see [Snapshots](#snapshots)). |
| `rebased_from` | Present only on rebase forks created by [`POST /rebases`](#616-post-rebases) (never by this route), recording the rebased fork (`source_namespace_id`) and starting sequence (`resume_after_seq`). |

The optional `snapshot_id` selects an unexpired user snapshot of the source (omitting it captures the source's current head). The source namespace must stay live and the chosen snapshot (or temporary snapshot) must stay unexpired through verification. Forking doesn't extend the source snapshot, later deletion of the source snapshot doesn't affect the fork, `Loonfs-Actor` is recorded as `created_by`, and forking emits no change-feed event.

The fork shares its source's pool and existing content and metadata objects without copying files. To create the fork, the server:

1. If `snapshot_id` is omitted, reads the source's head (manifest plus WAL tail). If any commit in the tail is older than 11 hours and 45 minutes, folds the source first and re-reads the head, then creates a temporary unnamed user snapshot of the current manifest (expiring in 5 minutes). The fork point is the captured head or snapshot sequence.
2. Writes the fork's manifest 1 (`initializing`) with put-if-absent, recording the fork point and the IDs of the fork snapshot and fork point snapshot.
3. If the source had WAL tail commits, folds them into the fork's manifest 2 (still `initializing`), materializing pending pieces with the source as chain creator and writing the fork's own metadata segments (the source publishes nothing).
4. Writes the fork point snapshot on the fork's newest manifest so the fork point stays readable for the fork's lifetime.
5. Writes and verifies the fork snapshot on the source (protecting shared source metadata segments until the fork retires) and writes the fork's [member entry](glossary.md#member-entry) in the pool.
6. Writes the fork's `active` manifest, setting `head_seq` and `retention_floor_seq` to `fork_point_seq` (the fork's first commit is `fork_point_seq + 1`, and after its first base compaction, inherited files keep only their newest revision at or below the fork point plus any revisions added in the fork).

Error conditions:

- An active target ID returns `namespace_exists`, and a deleted target ID returns `namespace_deleted` (before writing). An `initializing` fork with matching manifest 1 is finished from the next step, while a mismatch returns `namespace_exists` ([Creating and forking namespaces](#creating-and-forking-namespaces)).
- Missing, expired, deleted, foreign, or fork snapshots return `snapshot_not_found`. A tail-folding fork also returns `snapshot_not_found` if it doesn't become active within 5 minutes of the first call (extending the temporary snapshot doesn't lengthen this window) or if a source tail WAL object was already GC'd while finishing an initializing fork.
- If the source is deleted during verification, the call returns `namespace_deleted`. Any verification failure after manifest 1 leaves the target `initializing` until deleted or collected, and its ID can't be reused.
- If the source's head moves before temporary snapshot verification on every attempt, or the tail still has a commit older than 11 hours and 45 minutes after every fold, the call returns `stale_head` before writing any manifest.

### 6.14 `POST /merges`

Merges a fork's net change over a commit range into its parent namespace as one commit. For each inode touched by the fork's commits in the range, the "net change" is the difference between its state at the range end (`through_seq`, or the fork's head) and its state at the range start (after the fork's previous merge into this namespace, or after `fork_point_seq` for the first merge). The server determines the range start automatically from the merge index (callers can inspect it with [`GET /merges`](#615-get-merges)).

Representative request and response:

```text
Loonfs-Actor: usr_8f3c
Loonfs-Context: service
```

```json
{
  "source_namespace_id": "demo-branch",
  "commit_id": "c_merge_demo_branch_1",
  "message": "Merge demo-branch"
}
```

```json
{
  "namespace_id": "demo",
  "commit_id": "c_merge_demo_branch_1",
  "committed_seq": 431,
  "committed_by": "usr_8f3c",
  "committed_at_ms": 1752625100000,
  "message": "Merge demo-branch",
  "merge": {
    "source_namespace_id": "demo-branch",
    "source_after_seq": 418,
    "source_through_seq": 425
  },
  "events": [
    {
      "kind": "content_changed",
      "inode_id": "ino_000000r0000013kje0d47gc8kw",
      "revision_no": 9,
      "content_ref": {
        "kind": "blob_v1",
        "content_id": "con_4z59p0wjvx9a8b2frdhhf6mat1",
        "size_bytes": 20591,
        "checksum": { "algorithm": "crc64nvme", "value": "7ab..." }
      }
    }
  ]
}
```

| Request field | Meaning |
| --- | --- |
| `source_namespace_id` | The fork (must be forked directly from this namespace, so a fork of a fork merges into its immediate parent). |
| `through_seq` | Optional inclusive end sequence in the fork (must be above range start and at or below fork head, defaulting to fork head). |
| `commit_id` | Idempotency key for the merge. |
| `message` | Optional annotation included in the merge's identity. |

The response is a standard `Commit` with its `merge` record (an empty net change emits no `events` while still advancing the merge index). A merge publishes as the sole commit in its WAL object and counts toward normal fold triggers in the target namespace.

To merge a fork, the server executes nine steps (reading one view of each namespace across steps 1 to 8, reloading once from the current manifest if GC removed a superseded manifest's segments, see [Reference roots](format.md#112-reference-roots)):

1. Checks the commit receipt for `commit_id` and returns the original merge immediately if already committed.
2. Reserves 8 MiB of the server's merge memory allowance (or the full allowance if smaller), returning `server_busy` immediately if full. At step 7, the reservation grows to the larger of `3 * planned_wal_bytes` or planning memory (touched inodes, paths, and one commit page), again failing immediately with `server_busy` if room is unavailable. The reservation is released once the commit settles. (A concurrent retry with an in-flight `commit_id` also plans, and whichever commits second returns the first commit.)
3. Verifies the fork was created from this namespace and reads `fork_point_seq`.
4. Rejects a fork point below this namespace's retention floor with `below_retention_floor`.
5. Reads this namespace's [merge index](glossary.md#merge-index) for the fork's latest merge to determine the range start, rejecting a start below the fork's own floor with `below_retention_floor`.
6. Reads the fork's manifest and WAL tail and creates an unnamed temporary user snapshot of the fork's manifest (without folding unless a tail commit is older than 11 hours and 45 minutes, see [Cross-namespace copies and moves](format.md#97-cross-namespace-copies-and-moves)). The server deletes the snapshot after step 9 (with a 5-minute expiry fallback).
7. Computes the fork's net change and planned commit size before conflict checks, returning `merge_too_large` (with a split hint when a shorter range fits) if the touched-inode cap, WAL object limit, or merge allowance is exceeded.
8. Classifies every changed inode against this namespace's head under the conflict rule ([Validation](format.md#62-validation)) and materializes any pending WAL tail pieces needed by merge content references. Returns `merge_conflict` on any conflict, or `merge_too_large` if adding layouts exceeds one WAL object (leaving materialized content objects for GC).
9. Commits the merge as its own WAL object. If concurrent writes to this namespace fail a precondition (table below), the server keeps its reservation and restarts from step 1 up to 3 times before returning the error (where a conflicting concurrent write becomes `merge_conflict` and a concurrent merge of the same fork becomes `merge_range_conflict`).

When `merge_too_large` occurs, `details.split_at_seq` provides a "split hint" if any shorter range fits:

- For `touched_inodes`, `split_at_seq` is the last fork commit before the cap was crossed.
- For `wal_object` or `merge_allowance`, the server measures up to 4 shorter ranges from the fork alone: 3 bisections across the commit sequence range, then (if none fit) the first unmerged commit alone. Because the fork-only estimate omits rows matching the target (as well as temporary moves and previously skipped replay inodes), merging through `split_at_seq` can occasionally return `merge_too_large` again with a smaller hint. If even the first unmerged commit doesn't fit, `split_at_seq` is omitted. Callers pass `split_at_seq` as `through_seq` and then merge the remaining commits in follow-up calls.

Planning, ordering, and concurrency details:

- **Preconditions and retries.** The merge generates inode-addressed operations (preserving fork inode IDs and reusing fork content references without reading file bytes) with preconditions matching this namespace's current values (file deletes also check file revision, except hidden files being restored). Whenever the merge deletes or moves a directory, or moves an inode through a temporary name, it also preconditions on `namespace_head` so concurrent child creations or ancestor renames fail cleanly with `stale_head`. Concurrent-change codes that trigger step 9 re-planning:

| Code | Change since the plan |
| --- | --- |
| `stale_head` | The head moved while the merge deletes or moves a directory, or uses a temporary name. |
| `stale_revision` | Another writer modified a file's content that the merge updates or deletes. |
| `stale_attributes` | Another writer updated attributes on an inode the merge modifies. |
| `stale_access` | Another writer updated access on an inode the merge modifies. |
| `binding_version_mismatch` | Another writer moved an inode the merge moves or deletes. |
| `path_conflict` | Another inode took a name needed by a create, move, or restore. |
| `inode_not_found` | A deletion hid an inode the merge modifies or its destination directory. |
| `not_deleted` | Another writer restored an inode the merge restores. |

- **Operation ordering and cycles.** Operations run in dependency order: name-freeing deletes and moves before name-taking creates and moves, directory creates before moves/creates into them, moves out of a directory before deleting it or moving it below that item, and revision/attribute/access updates after binding changes. Recursive directory deletes omit separately deleting unchanged descendants. Name-swap cycles break by moving one inode first to a temporary name (its `inode_id`, with a numeric suffix if occupied, placed in the parent of a deleted directory when vacating it, or otherwise in the destination or current directory). Both moves appear in the change feed, though no reader sees the temporary name because the commit is atomic. Unorderable plans return `invalid_request`.
- **Conflict rule.** A row in this namespace is "the fork's own" if written at or below `fork_point_seq` or by an earlier merge of this fork. An inode change conflicts when:
  - the target row it would overwrite isn't the fork's own (`content`, `binding`, `attributes`, or `access`),
  - a name needed by a create or move is bound to another inode that the merge isn't deleting or moving away (`name`, noting that items under a deleted directory still occupy their names),
  - anything under a directory the fork deleted or moved changed after `fork_point_seq` (`subtree`), or
  - a deletion that the merge doesn't restore hides the inode, its current parent, or its destination parent (`visibility`). As an exception, deleting an inode already hidden by such a deletion is skipped (unless the merge restores that ancestor directory, in which case the merge restores the directory and then applies all fork changes below it). Undoing a deletion committed by an earlier merge of this fork succeeds while the deletion is still in trash, and becomes a `visibility` conflict once expired.
- **Conflict entry (`details.conflict`).** Describes the conflicting inode with the smallest ID (with `details.conflict_count` reporting the total). Each version (`base`, `ours`, `theirs`) includes `path`, plus `revision_no` and `content_ref` for files (`base` can be read from this namespace by inode and revision while the fork stays mergeable):

| Field | Meaning |
| --- | --- |
| `inode_id` | Conflicting inode ID (identical in both namespaces). |
| `kind` | One of `content`, `binding`, `attributes`, `access`, `visibility`, `name`, or `subtree` (clients MUST tolerate other values). |
| `base` | Target state at `fork_point_seq` (absent for inodes created in the fork). |
| `ours` | Fork state at range end (absent for inodes deleted in the fork). |
| `theirs` | Current target state (absent when deleted/hidden here, or the occupying inode for a `name` conflict). |

- **Authorization.** Checked against the net change (not intermediate temporary-name moves) in this namespace's access rows during planning and commit. Subjects also need `read` on every changed fork inode so they can't merge unreadable fork changes.

Merge-specific errors beyond ordinary commit errors:

| Code | Status | Meaning |
| --- | --- | --- |
| `merge_conflict` | 409 | Fork changes conflict with this namespace (`details.conflict_count` and `details.conflict`). |
| `merge_range_conflict` | 409 | Another merge of the fork committed after this merge planned its range. |
| `merge_too_large` | 413 | Exceeded touched-inode cap, WAL object size, or merge allowance (`details.merge_bound`, plus optional `details.split_at_seq`). |
| `below_retention_floor` | 409 | Fork point is below this namespace's floor, or range start is below the fork's floor (`details` specifies which). |
| `stale_head`, `stale_revision`, `stale_attributes`, `stale_access`, `binding_version_mismatch`, `path_conflict`, `not_deleted` | 409 | Target namespace changed between plan and commit four times in a row. Retry the merge. |
| `inode_not_found` | 404 | Target changed four times in a row, last failing because a deletion hid an affected inode or destination directory. Retry the merge. |
| `server_busy` | 503 | Concurrent merges exhausted the server's merge memory allowance (step 2). Retry later. |
| `namespace_not_found`, `namespace_deleted` | 404, 410 | Target or fork is missing or deleted (`details.namespace_id` identifies which). |
| `invalid_request` | 400 | Fork wasn't created from this namespace (`param`: `/source_namespace_id`), `through_seq` is out of range, or operations couldn't be ordered (`param`: `/through_seq`). |
| `not_supported` | 501 | Merges are disabled (`max_merge_bytes == 0` in the reference server). Sets `feature` to `filesystem.merges` without writing. |

### 6.15 `GET /merges`

Returns where a fork's next merge into this namespace starts without planning the merge. Clients don't need this route to call `POST /merges`, and instead use it to resume a merge loop after a crash or inspect unmerged commits (by reading the fork's change feed after `next_after_seq`). Requires an administrator in both namespaces, or the service context.

Representative request and response:

`GET /v0/namespaces/demo/merges?source_namespace_id=demo-branch`

```json
{
  "namespace_id": "demo",
  "source_namespace_id": "demo-branch",
  "fork_point_seq": 418,
  "source_head_seq": 440,
  "outcome": "open",
  "merge": {
    "source_namespace_id": "demo-branch",
    "source_after_seq": 418,
    "source_through_seq": 425
  },
  "next_after_seq": 425
}
```

| Query parameter | Meaning |
| --- | --- |
| `source_namespace_id` | Required. The fork (must be forked from this namespace, or identified by `fork_basis.rebased_from` when querying a rebase fork). |
| `consistency`, `max_staleness_ms` | Optional head confirmation settings applied to both namespaces ([Read consistency](#read-consistency)). |

| Response field | Meaning |
| --- | --- |
| `namespace_id` | Namespace that the fork merges into (or the rebase fork being queried). |
| `source_namespace_id` | Fork whose next merge range was queried. |
| `fork_point_seq` | Fork point sequence in the parent namespace. |
| `source_head_seq` | Fork's head sequence when read (default end of a merge without `through_seq`). |
| `outcome` | Floor status: `open`, `range_start_below_floor`, or `fork_point_below_floor` (clients MUST tolerate other values). |

Rather than returning `below_retention_floor` when steps 3 to 5 of [`POST /merges`](#614-post-merges) fail a floor check, `GET /merges` reports the status in `outcome` with outcome-specific sibling fields:

| `outcome` | Fields | Means |
| --- | --- | --- |
| `open` | `merge`, `next_after_seq` | Floors allow the next merge. `merge` is the latest merge record (absent before the first merge), and `next_after_seq` is its `source_through_seq` (or `fork_point_seq` before the first merge). |
| `range_start_below_floor` | `merge`, `next_after_seq`, `retention_floor_seq` | Range start is below the fork's own floor (`retention_floor_seq`), so `POST /merges` will return `below_retention_floor`. |
| `fork_point_below_floor` | `retention_floor_seq` | Fork point is below this namespace's floor (`retention_floor_seq`), so `POST /merges` will return `below_retention_floor` (and `merge` / `next_after_seq` are omitted because compaction may have pruned the [merge index](glossary.md#merge-index)). |

Example when the parent's floor has passed the fork point:

```json
{"namespace_id":"demo","source_namespace_id":"demo-branch","fork_point_seq":418,"source_head_seq":440,"outcome":"fork_point_below_floor","retention_floor_seq":450}
```

When queried on a rebase fork with `source_namespace_id` set to its `fork_basis.rebased_from.source_namespace_id`, the route inspects replay progress for [`POST /replays`](#617-post-replays): `merge` is the newest replay commit in the rebase fork (ignoring inherited merges from the parent namespace), `next_after_seq` starts at `resume_after_seq`, `fork_point_seq` is the source fork's fork point in the parent namespace, and `outcome` is `fork_point_below_floor` if the rebase fork's floor passed its own fork point.

Errors beyond standard read errors:

| Code | Status | Meaning |
| --- | --- | --- |
| `namespace_not_found`, `namespace_deleted` | 404, 410 | Target or fork is missing or deleted (`details.namespace_id` identifies which). |
| `invalid_request` | 400 | Missing `source_namespace_id` or fork doesn't match this namespace (`param`: `source_namespace_id`). |
| `forbidden` | 403 | Subject isn't an administrator of both namespaces. |
| `not_supported` | 501 | Merges are disabled (`max_merge_bytes == 0`). Sets `feature` to `filesystem.merges` without reading. |

### 6.16 `POST /rebases`

Starts a [rebase](glossary.md#rebase) by creating a new "rebase fork" from this parent namespace's current head, ready to receive unmerged commits from an existing "source fork" (`source_namespace_id`). After creating the rebase fork here, the client calls [`POST /replays`](#617-post-replays) one or more times to apply non-conflicting changes and collect a conflict report, resolves reported conflicts in the rebase fork using normal commits, and finally merges the rebase fork back into the parent namespace with [`POST /merges`](#614-post-merges).

Requires administrator rights on the parent namespace (the right to fork it), or the service context.

Representative request and response:

```text
Loonfs-Actor: usr_8f3c
Loonfs-Context: service
```

```json
{
  "source_namespace_id": "demo-branch",
  "new_namespace_id": "demo-branch-2"
}
```

```json
{
  "namespace_id": "demo-branch-2",
  "created_at_ms": 1752625200000,
  "created_by": "usr_8f3c",
  "fork_basis": {
    "source_namespace_id": "demo",
    "fork_point_seq": 431,
    "fork_point_snapshot_id": "snp_0000000000001b82r3jhza68dy",
    "rebased_from": {"source_namespace_id": "demo-branch", "resume_after_seq": 418}
  },
  "pool_id": "pool_3s0drmj8em84yb4mnetqkz2j0x",
  "head_seq": 431,
  "retention_floor_seq": 431
}
```

| Request field | Meaning |
| --- | --- |
| `source_namespace_id` | The source fork (must be forked directly from this parent namespace). |
| `new_namespace_id` | Optional ID for the rebase fork (generated automatically if omitted, see [Creating and forking namespaces](#creating-and-forking-namespaces)). |

Like `POST /forks`, this route isn't idempotent, so callers needing safe recovery from a lost response MUST supply `new_namespace_id`. Re-sending the same request finishes an initializing rebase fork or returns `namespace_exists` once active, and in either case the caller proceeds to [`POST /replays`](#617-post-replays).

The response returns the rebase fork's `NamespaceMetadata`. Its `fork_basis.rebased_from` records the source fork (`source_namespace_id`) and the sequence after which the source fork's next merge into the parent namespace would have started (`resume_after_seq`).

To create the rebase fork, the server:

1. Checks `new_namespace_id` (returning `namespace_exists` if active, `namespace_deleted` if deleted, or finishing an initializing rebase fork whose manifest 1 matches, as in [`POST /forks`](#613-post-forks)).
2. Verifies the source fork was forked from this parent namespace and reads its fork point.
3. Rejects with `below_retention_floor` if the source fork's fork point is below the parent's floor or if the source fork's next merge start is below the source fork's own floor.
4. Forks this parent namespace at its current head (as in [`POST /forks`](#613-post-forks)) and records `rebased_from` in `fork_basis`.

Errors:

| Code | Status | Meaning |
| --- | --- | --- |
| `namespace_exists` | 409 | `new_namespace_id` is already an active namespace. |
| `below_retention_floor` | 409 | Source fork point is below parent's floor, or next merge start is below source fork's floor (`details` specifies which). |
| `snapshot_not_found` | 404 | Tail-folding rebase fork exceeded its 5-minute temporary snapshot window or lost a parent tail WAL object while finishing, as in [`POST /forks`](#613-post-forks). |
| `namespace_not_found`, `namespace_deleted` | 404, 410 | Parent namespace, source fork, or `new_namespace_id` is missing or deleted (`details.namespace_id` identifies which). |
| `invalid_request` | 400 | Source fork wasn't created from this parent namespace (`param`: `/source_namespace_id`). |
| `not_supported` | 501 | Merges are disabled (`max_merge_bytes == 0`). Sets `feature` to `filesystem.merges` without reading or writing. |

Replay commits created by [`POST /replays`](#617-post-replays) follow the merge rules in [`POST /merges`](#614-post-merges) with four rebase-specific adaptations:

- **Skipping conflicts and cascading dependencies.** A replay commit applies non-conflicting changes and skips conflicting ones (recording only the merge record and no operations if everything conflicts, so later replays advance past the range). Changes that depend on a skipped change are also skipped with `kind: "dependency"` (ordered after the entry they depend on): creating, restoring, moving into, or modifying below a directory whose create or restore was skipped, or deleting a directory that still contains an inode whose move out was skipped. Skipping a change that would have freed a name also skips the create or move taking that name with `kind: "name"`.
- **Conflict ownership on the rebase fork.** Conflict classification runs against the rebase fork's current head. A row at or below the rebase fork's fork point is "the source fork's own" if written in the parent namespace at or below the source fork's fork point or by an earlier merge of the source fork. A row above the rebase fork's fork point is "the source fork's own" only if written by a replay commit (ordinary commits on the rebase fork, such as conflict-resolution edits, count as foreign). Undoing a deletion that an earlier merge already applied is a `visibility` conflict because inherited deletions start expired in the rebase fork.
- **Preconditions during concurrent edits.** Because the rebase fork accepts ordinary commits while rebasing runs, replay commits use the same inode-addressed and head preconditions as merges and re-plan on race errors (for example, a concurrent create inside a directory the replay wanted to delete triggers `stale_head`, and the next plan reports a `subtree` conflict while preserving the new file).
- **Conflict entries and base reads.** Each conflict entry uses the `merge_conflict` entry shape plus `kind: "dependency"` (for `dependency`, `theirs` is the source fork's version of the skipped dependency inode, whose path matches that inode's `ours` path). Clients read `base` revisions from the rebase fork using `GET /inodes/{inode_id}/revisions/{revision_no}/content` with `snapshot_id` set to the rebase fork's `fork_basis.fork_point_snapshot_id`, which stays readable for the rebase fork's lifetime even after the parent's retention floor advances.

### 6.17 `POST /replays`

Replays a range of a source fork's unmerged commits into this rebase fork (created by [`POST /rebases`](#616-post-rebases)) as one replay commit, applying non-conflicting changes and returning any conflicts in `conflicts`.

Representative request and response:

```text
Loonfs-Actor: usr_8f3c
Loonfs-Context: service
```

```json
{
  "source_namespace_id": "demo-branch",
  "commit_id": "c_rebase_demo_branch_1"
}
```

```json
{
  "namespace": {
    "namespace_id": "demo-branch-2",
    "created_at_ms": 1752625200000,
    "created_by": "usr_8f3c",
    "fork_basis": {
      "source_namespace_id": "demo",
      "fork_point_seq": 431,
      "fork_point_snapshot_id": "snp_0000000000001b82r3jhza68dy",
      "rebased_from": {"source_namespace_id": "demo-branch", "resume_after_seq": 418}
    },
    "pool_id": "pool_3s0drmj8em84yb4mnetqkz2j0x",
    "head_seq": 432,
    "retention_floor_seq": 431
  },
  "commit": {
    "namespace_id": "demo-branch-2",
    "commit_id": "c_rebase_demo_branch_1",
    "committed_seq": 432,
    "committed_at_ms": 1752625201000,
    "committed_by": "usr_8f3c",
    "merge": {
      "source_namespace_id": "demo-branch",
      "source_after_seq": 418,
      "source_through_seq": 425
    },
    "events": []
  },
  "conflicts": [
    {
      "inode_id": "ino_000000r0000013kje0d47gc8kw",
      "kind": "content",
      "base": {"path": "/docs/report.txt", "revision_no": 7, "content_ref": {"kind": "blob_v1", "content_id": "con_4z59p0wjvx9a8b2frdhhf6mat1", "size_bytes": 19482, "checksum": {"algorithm": "crc64nvme", "value": "42d..."}}},
      "ours": {"path": "/docs/report.txt", "revision_no": 8, "content_ref": {"kind": "blob_v1", "content_id": "con_1p2q9w3e4r5t6y7v8j9k0a1s2d", "size_bytes": 20117, "checksum": {"algorithm": "crc64nvme", "value": "9c1..."}}},
      "theirs": {"path": "/docs/report.txt", "revision_no": 8, "content_ref": {"kind": "blob_v1", "content_id": "con_4z59p0wjvx9a8b2frdhhf6mat2", "size_bytes": 20591, "checksum": {"algorithm": "crc64nvme", "value": "7ab..."}}}
    }
  ]
}
```

| Request field | Meaning |
| --- | --- |
| `source_namespace_id` | Source fork matching this rebase fork's `fork_basis.rebased_from.source_namespace_id`. |
| `through_seq` | Optional inclusive end sequence in the source fork (same bounds and default as a merge's `through_seq`). |
| `commit_id` | Idempotency key for the replay commit. |
| `message` | Optional annotation included in the replay commit's identity. |

The response returns the updated rebase fork (`namespace`, matching [`GET /v0/namespaces/{ns}`](#62-get-v0namespacesns)), the replay commit (`commit`, matching [`POST /merges`](#614-post-merges)), and its conflict report (`conflicts`, empty when nothing conflicted).

To execute a replay commit, the server (reading one view of each namespace across steps 1 to 5, reloading once if GC removes a replaced manifest's segments):

1. Checks the commit receipt for `commit_id` in the rebase fork, returning the rebase fork, the replay commit, and the persisted conflict report immediately if already committed.
2. Reserves merge memory allowance as in [`POST /merges`](#614-post-merges).
3. Verifies `rebased_from` matches `source_namespace_id` and determines the range start (after the previous replay commit, or after `resume_after_seq` for the first replay). Returns `below_retention_floor` if the range start is below the source fork's floor or if the rebase fork's floor passed its own fork point.
4. Snapshots the source fork's manifest (folding only when a merge would), computes the net change and planned size (including the serialized conflict report), and returns `merge_too_large` with `split_at_seq` if it doesn't fit.
5. Classifies conflicts against the rebase fork's head (reading parent history up to the rebase fork's fork point through `fork_point_snapshot_id`) and materializes any needed source WAL tail pieces.
6. Commits non-conflicting operations into the rebase fork (storing the conflict report in the merge record, see [WAL records](format.md#a5-wal-records)) and returns `namespace`, `commit`, and `conflicts`. On concurrent-change precondition failures, keeps the reservation and restarts from step 1 up to 3 times (reporting intervening foreign changes in `conflicts`).

A failed replay leaves the rebase fork intact for follow-up calls. Because the conflict report is persisted in the merge record, retrying with the same `commit_id` always replays the exact same conflict report. Large ranges split across multiple replay calls using `split_at_seq` and `through_seq`, just like merges. Authorization checks the net change against the rebase fork's access rows (inherited from the parent namespace) and requires `read` on touched source-fork inodes.

This route never returns `merge_conflict` because conflicts are returned in `conflicts` rather than failing the request. Other replay-specific errors:

| Code | Status | Meaning |
| --- | --- | --- |
| `merge_range_conflict` | 409 | Another replay commit into this rebase fork committed after planning started. |
| `merge_too_large` | 413 | Replay commit (including its conflict report) exceeded merge bounds (`details.split_at_seq` reports a split point when one fits). |
| `below_retention_floor` | 409 | Range start is below the source fork's floor, or the rebase fork's floor passed its own fork point (`details` specifies which). |
| `stale_head`, `stale_revision`, `stale_attributes`, `stale_access`, `binding_version_mismatch`, `path_conflict`, `not_deleted` | 409 | Rebase fork changed between plan and commit four times in a row. Retry the replay. |
| `inode_not_found` | 404 | Rebase fork changed four times in a row, last failing because a deletion hid an affected inode or destination directory. Retry the replay. |
| `server_busy` | 503 | Concurrent merges exhausted the merge memory allowance. Retry later. |
| `namespace_not_found`, `namespace_deleted` | 404, 410 | Rebase fork or source fork is missing or deleted (`details.namespace_id` identifies which). |
| `invalid_request` | 400 | `rebased_from` doesn't match `source_namespace_id` (`param`: `/source_namespace_id`), `through_seq` is invalid, or operations couldn't be ordered (`param`: `/through_seq`). |
| `not_supported` | 501 | Merges are disabled (`max_merge_bytes == 0`). Sets `feature` to `filesystem.merges` without reading or writing. |

### 6.18 `GET /grep`

Searches file contents in a namespace for lines matching a regex pattern and returns a page of matches. Requires the `query.grep` feature and an active grep index on the namespace (otherwise returns `not_supported` with `feature` set to `query.grep`, see [Data-dependent features](#23-data-dependent-features)).

Representative request and response:

```http
GET /grep?pattern=fn%20%28grep%7Csearch%29&case_insensitive=false&path=%2Fsrc&limit=100
```

```json
{
  "namespace_id": "demo",
  "head_seq": 418,
  "built_through_seq": 410,
  "tail_scanned": true,
  "matches": [
    {
      "path": "/src/search.rs",
      "inode_id": "ino_00000ej000000snh6a5re18dhf",
      "revision_no": 3,
      "line_number": 17,
      "byte_offset": 512,
      "line": "fn grep(&self) {",
      "line_truncated": false
    }
  ],
  "next_cursor": "..."
}
```

Patterns use the Rust `regex` crate dialect (no backreferences or lookaround), compiled line-anchored so `^` and `$` match line boundaries. To execute a query, the server:

1. Extracts required trigrams from the pattern.
2. Intersects the namespace's grep index for those trigrams ([Grep extension format](format.md#appendix-d-grep-extension-format)).
3. Scans every file revision committed after `built_through_seq`.
4. Verifies every candidate against the full regex (so index lag slows queries without affecting accuracy).

Search ordering, scoping, and pagination rules:

- **Content values and ordering.** The index tracks "content values" identified by `(content_id, size_bytes)` so files sharing identical bytes share one posting entry. Matches are emitted once per matching line for each visible file, ordered by content value, then `inode_id`, then `byte_offset` (clients wanting inode order sort locally).
- **Budgets.** Every page is bounded by both `limit` (match count, using `pagination.default_limit` / `pagination.max_limit`) and a per-page candidate budget (counting verified content values and checked files). As a result, a page can return fewer than `limit` matches with a `next_cursor` and can pause partway through files sharing one content value.
- **Consistent multi-page cut.** Page 1 reads at the live head (respecting `consistency` and `max_staleness_ms`, see [Read consistency](#read-consistency)) and records `head_seq` in the cursor. Every later page reads at that same `head_seq` (triggering strong confirmation if the cursor's head is ahead of an `eventual` reader) and reports paths as of `head_seq`. Unchanged files use current `read` rights, while files whose revision or path changed since page 1 also require `history` (subjects without `history` don't see those changed files on later pages). Cursors bind to the query criteria and the grep index build: changing parameters, supplying a head beyond the confirmed head, or continuing after an index rebuild/disable-reenable returns `invalid_request`, and the retention floor passing `head_seq` returns `below_retention_floor` (prompting a fresh search from page 1).
- **Unindexable patterns and lagging indexes.** Patterns without a 3-byte literal run return `query_unindexable` unless `allow_scan=true` (capped by `query.grep.scan_budget_files`). Tails exceeding `query.grep.tail_budget_files` return `index_lagging` unless `allow_stale=true` (reported as `tail_scanned: false`). Stale searches exclude files whose current revision is newer than the last fully indexed commit (and still exclude files deleted afterward), keeping that same cutoff recorded in the cursor across all pages even if the index catches up mid-pagination.
- **Deleted files and authorization.** The index covers all retained revisions, and each query filters visibility at its read sequence (so deletes and restores require no index rebuild). Unreadable candidates are filtered before reading bytes (still counting against the candidate budget), and unindexed `allow_scan` walks skip unreadable directories.
- **Subtree scoping (`path` or `inode_id`).** `path` takes a complete canonical absolute path (resolved by name key, returning `path_not_found` if missing, or empty matches if empty). `inode_id` scopes to a visible inode's descendants regardless of renames (`inode_not_found` if missing or hidden). Supplying both returns `invalid_request` with `param` `inode_id`.

### 6.19 `GET /metrics`

Returns per-process Prometheus metrics in text exposition format 0.0.4 (`Content-Type: text/plain; version=0.0.4`). Alongside the liveness routes (`GET /health` returning `ok`, and `GET /readiness` returning `ready` while admitting work), `/metrics` is an operational route. Unlike `/health` and `/readiness` (which expose only liveness for load balancers), `/metrics` exposes traffic volume, workload shapes, and error rates, so it requires the deployment's `Authorization: Bearer` token (returning `401 unauthorized` in the standard error envelope if missing or invalid).

Metric names follow `loonfs_<subsystem>_<metric>` covering object-store calls, maintenance steps and sweeps, batched publications, GC reclamation, and served HTTP requests. Labels draw strictly from closed vocabularies (for example, route templates like `/v0/namespaces/{namespace_id}`, never concrete namespace, upload, commit, or snapshot IDs). Counters are per process and reset on restart. The metric catalog isn't part of the v0 wire contract, so series may be added or renamed and clients MUST NOT depend on any specific series.

## 7. Conformance requirements

### 7.1 Server requirements

A conforming server MUST:

1. Treat object storage as the authoritative durable foundation.
2. Publish visible metadata only through logical commits stored in visible numbered WAL objects.
3. Make referenced content durable no later than the commit that references it ([File contents and ownership](format.md#15-file-contents-and-ownership)).
4. Preserve `(namespace_id, inode_id)` as the canonical identity of an item, and preserve the namespace lifetime in [Namespaces and identity](format.md#11-namespaces-and-identity).
5. Resolve content through the reading namespace's pool and the reference's content ID.
6. Implement tombstone-first delete.
7. Serve replay from the newest verified manifest above the manifest boundary (`boundary.json`) plus the numbered WAL objects after its folded boundary, replayed as logical commits, with snapshots pinning manifests for retention, stable reads, restore, and forks.
8. Map sibling names to name keys exactly as specified in [Names and paths](format.md#14-names-and-paths).
9. Keep upload sessions out of namespace history and the change feed.
10. Preserve per-commit idempotency, ordering, and change-feed identity even when batching multiple logical commits into one WAL object.
11. Advertise API groups and features truthfully in the capability document, never advertising an API group whose required operations aren't implemented.
12. Return `not_supported` (with its `feature` key) for unimplemented or disabled operations, never undefined behavior.

### 7.2 Writer and client requirements

A conforming writer or client MUST:

1. Treat paths as selectors rather than durable identity.
2. Upload content before committing it unless sending inline bytes.
3. Use `commit_id` (or equivalent idempotency keys) for safe retries.
4. Tolerate commit rejections when preconditions are no longer met.
5. Re-bootstrap when a cursor falls behind the retention floor.
6. Gate optional APIs on the capability document rather than probing, treating `not_supported` as authoritative if the two disagree.
7. Ignore unknown JSON response fields, unknown error codes, and unknown feature keys.

A sync client MUST also persist local state for its cursor and reconciliation logic.

## 8. Extension points

The preferred extension point is the committed change feed: downstream systems (indexers, notification pipelines, preview generators, policy engines) should consume committed changes asynchronously rather than inserting themselves into the core commit path.

New client-visible operations belong in this specification (as API group operations or features in the [Feature registry](#22-feature-registry)). New durable storage state belongs in `format.md`. Internal scheduling and queueing belong to each implementation. Cross-store discovery (global naming, cross-store search, ownership, quotas) is outside this specification.

### SDK streaming downloads

Handwritten transfer helpers expose verified streaming downloads in every generated SDK:

| SDK | Open a stream | Cancel or release it |
| --- | --- | --- |
| Go | `client.Files.DownloadStream(ctx, input)` | Close `Content` or cancel `ctx` |
| Python (`LoonFS` and `AsyncLoonFS`) | `client.files.download_stream(namespace_id, path=path, request_options=options)` (await with `AsyncLoonFS`) | Use a `with` block or call `close()` (`async with` or `await aclose()` for async). |
| TypeScript | `client.files.downloadStream(input, requestOptions)` | Cancel the `content` reader or abort `requestOptions.abortSignal` |

Buffered download helpers are built on top of these streams. Streams verify total byte length and checksum incrementally, reporting any mismatch on the same reader/iterator delivering the bytes. Only reading a stream completely verifies the file (early close or cancellation skips verification). Consumers MUST handle late errors after partial bytes have been delivered, and failed bodies are never automatically replayed.

Transport controls apply across both direct and proxied downloads:

- **Go.** Uses the caller's context deadline (defaulting to a 60-second operation deadline when unset).
- **TypeScript.** Applies `timeoutInSeconds` (client default or 60 seconds) across both metadata and body requests, plus `abortSignal`.
- **Python.** Uses the request or client HTTPX timeout for I/O waits on both transports, closing the response on exit from `with` / `async with`.

API auth headers and cookies are never forwarded to presigned object-store URLs. Each direct request sends `transfer.headers` unchanged with no extra `Range` header, verifies per-range byte length, and verifies one checksum across all ranges (zero-byte files make no object request).

### SDK streaming uploads

Streaming preparation accepts single-pass sources across SDKs:

- **Go:** caller-owned `io.Reader`.
- **Python (sync):** binary file object.
- **Python (async):** `AsyncIterator[bytes]` or binary file object.
- **TypeScript:** `Blob`, `ReadableStream<Uint8Array>`, or `AsyncIterable<Uint8Array>`.

Streaming helpers return the same prepared content type as the byte helpers without committing:

| SDK | Prepare once | Prepare and publish once |
| --- | --- | --- |
| Go | `Files.PrepareStream(ctx, namespaceID, reader, sizeBytes)` | `Files.UploadStream(ctx, files.StreamUploadInput{...})` |
| Python (`LoonFS` and `AsyncLoonFS`) | `files.prepare_stream(namespace_id, content=reader, size_bytes=size)` (await with `AsyncLoonFS`) | `files.upload_stream(namespace_id, content=reader, ...)` (await with `AsyncLoonFS`) |
| TypeScript | `files.prepareStream({ namespace_id, content, size_bytes }, options)` | `files.uploadStream(input, options)` |

The optional size hint is verified against actual bytes read (inferred automatically for a TypeScript `Blob`). Small streams (including unknown-length streams) stay inline when supported. Beyond the inline lookahead, unknown-length streams use `direct_multipart` if advertised and `service_proxied` otherwise.

Memory and transport rules:

- **Bounded memory.** SDKs read in up to 64 KiB chunks, and multipart uploads keep one part buffer plus up to 10,000 part descriptors in memory (async Python and TypeScript also keep the caller's current chunk, so callers should yield bounded chunks). TypeScript sends known-size payloads up to 8 MiB as bounded bodies of known length for browser compatibility (larger or unknown-length single-request uploads require runtime streaming request support, whereas multipart parts always have known length). Byte helpers share these same code paths.
- **Deadlines and cancellation.** Go and TypeScript deadlines span the entire `UploadStream` / `uploadStream` call, while Python timeouts bound HTTP I/O waits (async Python runs blocking file reads in a worker thread). Callers own and close Go/Python sources and MUST interrupt blocking reads on cancellation, while TypeScript cancels or returns its source on completion or failure.
- **No automatic replay and cleanup.** Payloads are read once and never automatically replayed. Source errors, size mismatches, failed part/body transfers, and async Python cancellation during staging prevent completion and trigger a best-effort abort with a 5-second cleanup timeout (an I/O timeout in sync Python). Ambiguous completion errors leave the session open for inspection.
- **Retrying publication.** After preparation succeeds, callers retry `UploadPrepared` / `upload_prepared` / `uploadPrepared` with the retained prepared value and identical commit inputs rather than re-reading the stream or starting a new upload.

### SDK appends

Handwritten helpers append bytes to an existing file via one `create_commit` request containing an `append_file` operation:

| SDK | Append bytes |
| --- | --- |
| Go | `client.Files.Append(ctx, files.AppendInput{...})` |
| Python (`LoonFS` and `AsyncLoonFS`) | `client.files.append(namespace_id, path=path, content=data, ...)` (await with `AsyncLoonFS`) |
| TypeScript | `client.files.append(input, requestOptions)` |

Bytes are sent as `inline_content` with no upload. Helpers validate locally before sending, rejecting empty payloads and payloads over 256 KiB with the same error type used by upload helpers (`error` in Go, `ValueError` in Python, `Error` in TypeScript). `commit_id`, `message`, `expected_inode_id`, and `expected_revision_no` are optional (pass an explicit `commit_id` for safe idempotent retries). To use `append_file_by_inode`, call `commits.create` directly.
