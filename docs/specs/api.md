# LoonFS API Specification

This document is the normative specification of the LoonFS client-facing API:
API groups, capability discovery, the standard error contract, operation
statefulness, and the representative v0 HTTP binding. It is **normative where implemented** — a
deployment chooses which optional API groups to expose, but every op it does
expose must have the shape specified here.

The companion document is `format.md` — the durable format, mandatory for
every implementation.

The same client codebase works against an embedded engine and a hosted
server: both expose the same operations, advertise their capabilities the
same way, and report unsupported surface area with the same errors. The only
difference a client observes is *which* capabilities a deployment advertises.

## 1. API groups

API groups are coherent areas of responsibility with their own endpoint sets,
not resource types. They are **all-or-nothing** conformance units: a deployment MUST NOT advertise an
API group unless every required op in it is implemented. Optional behavior
within an API group is expressed as **named features** (section 2).

| API group | Ops | Status |
| --- | --- | --- |
| `filesystem/v0` | Path and inode reads, path mutations, uploads, the change feed, namespace state, capability discovery, and standard errors. Namespace lifecycle, snapshots, attributes, inode child listing, and direct transfers are features. | **Mandatory** for any conforming deployment |
| `maintenance/v0` | Namespace diagnostics, checkpoints, one-shot metadata maintenance, retention-floor advancement, garbage collection, and grep index maintenance as a feature. | Optional |
| `query/v0` | Content search over derived indexes as the `query.grep` feature. | Optional |

API groups are client contracts. They do not describe node roles or deployment topology.

Notes:

- An embedded engine is `filesystem/v0` (with the namespace-management features enabled) plus
  `maintenance/v0` (maintenance is manually triggerable). A minimal server that
  wraps the embedded engine over HTTP advertises the same two API groups and is
  fully conformant.
- A hosted LoonFS server serves `maintenance/v0` when `maintenance` is
  `serve_and_maintain` or `serve_only`. In `maintain_only` or `disabled` mode,
  every route under `/v0/maintenance/` answers `route_not_found`, as any path
  outside the served surface does; all four choices are fully conformant.
- API groups version independently (`filesystem/v0` could coexist with a future
  `maintenance/v1`). The group name — the segment before the slash — is the stable
  identity that feature keys reference.
- No queue or job-scheduling semantics exist in this document. `maintenance/v0`
  exposes *trigger* and *status* shapes only; how work is scheduled is
  implementation freedom.

### 1.1 Where new behavior belongs

When new behavior arrives, three questions decide where it belongs:

1. Does it change bytes or metadata another implementation must interpret?
   It belongs in `format.md`, and it is mandatory.
2. Is it a client-visible operation whose shape should be uniform wherever it
   exists? It belongs here, inside an API group or as a named feature.
3. Is it about *how* work gets done — queues, schedulers, caches? That is
   implementation freedom and belongs in no specification document.

## 2. Capability discovery

### 2.1 The capability document

Every deployment describes itself with one capability document. A remote
client fetches it from `GET /v0/capabilities` and caches it for the connection; an
embedded engine exposes the same document as a constant. SDK gating logic is
therefore identical for both backends.

The example below is the reference deployment: the runtime's own `filesystem/v0`
and `maintenance/v0` API groups plus the `query/v0` API group the server composes from the
grep extension. A host that composes no extension advertises neither that
API group nor the two grep feature keys: `query.grep` for searching an index
and `maintenance.grep.index` for administering one. Clients gate on the document
either way.

```json
{
  "protocol_version": "v0",
  "api_groups": ["filesystem/v0", "maintenance/v0", "query/v0"],
  "features": {
    "maintenance.grep.index": true,
    "filesystem.namespaces.create": true,
    "filesystem.namespaces.fork": true,
    "filesystem.namespaces.delete": true,
    "filesystem.snapshots": true,
    "filesystem.commits.inline_content": true,
    "query.grep": true
  },
  "limits": {
    "access.max_principals_per_request": 64,
    "commit.max_content_tokens": 4096,
    "commit.max_external_content_refs": 4096,
    "commit.max_inline_content_bytes_per_operation": 65536,
    "commit.max_message_bytes": 4096,
    "commit.max_operations": 4096,
    "commit.max_preconditions": 1024,
    "maintenance.gc.min_grace_window_ms": 1335000,
    "pagination.default_limit": 1000,
    "pagination.max_limit": 1000,
    "query.grep.default_limit": 1000,
    "query.grep.max_limit": 1000,
    "query.grep.scan_budget_files": 4096,
    "query.grep.tail_budget_files": 512
  }
}
```

| Field | Meaning |
| --- | --- |
| `protocol_version` | The protocol version, `v0`. |
| `api_groups` | The advertised API groups. Each entry is `group/version`. |
| `features` | Named features and whether this deployment supports them. An absent key means unsupported. |
| `limits` | Advisory numeric limits clients may use to pre-validate requests. May be empty. |

Rules:

- API groups are all-or-nothing for their required ops; clients never probe
  op-by-op inside an advertised API group.
- **Feature-key rule (normative):** every feature key is dotted and its first
  segment MUST be the group name of an advertised API group. A feature whose
  first segment does not match an advertised API group is malformed; clients
  MUST reject the document or ignore that key.
- Clients must ignore unknown feature keys and unknown document fields.
- Limits are advisory; the authoritative outcome is the server's response to
  the actual request.

Registered limit keys:

| Limit key | Meaning |
| --- | --- |
| `pagination.default_limit` | Default page size applied when a paged request omits `limit`. An explicit `limit=0` is rejected with 400 `invalid_request`. |
| `pagination.max_limit` | Largest accepted page size for paged requests. A `limit` greater than this value is rejected with 400 `invalid_request`. |
| `upload.service_proxied.max_content_bytes` | Largest request body accepted by one service-proxied upload content request (`PUT .../uploads/{upload_id}/content`). This is not a maximum file size. Clients may use `direct_put` for larger content only when `filesystem.uploads.direct_put` is advertised; otherwise they must stay within this limit. |
| `upload.direct_put.max_content_bytes` | Largest object this deployment's provider accepts in one presigned `direct_put` request. Unrelated to `upload.service_proxied.max_content_bytes`, which bounds service-proxied uploads. A size hint above this limit returns `content_too_large` at begin, and completion checks the actual stored size. Advertised only alongside `filesystem.uploads.direct_put`. |
| `upload.complete.max_request_body_bytes` | Largest JSON body accepted by `POST .../uploads/{upload_id}/complete`. Larger requests return `content_too_large`. |
| `download.service_proxied.max_content_bytes` | Largest file content a service-proxied read (`GET .../filesystem/content` or `GET .../inodes/{inode_id}/revisions/{revision_no}/content`) will stream and return in one response. Over-limit reads answer `content_too_large`; proxied reads use bounded chunks but do not support range reads. A file past this limit is read through the corresponding path or inode download grant when `filesystem.downloads.direct_get` is advertised — which it is on exactly the deployments that could have let a client create such a file. The check is against the whole file. The proxied read has no ranged form. |
| `upload.service_proxied.max_concurrent_requests` | How many service-proxied upload requests a serving process streams at once. The cap is shared by all callers and is not a per-caller allowance. Requests past it answer `server_busy`. |
| `download.service_proxied.max_concurrent_requests` | How many service-proxied content reads a serving process streams at once. The cap is shared by all callers and is not a per-caller allowance. Requests past it answer `server_busy`. A read holds its place until its body finishes or is dropped. |
| `access.max_principals_per_request` | Most principal ids one request may act as. Over-limit headers answer `invalid_request`. |
| `commit.max_inline_content_bytes_per_operation` | Maximum file size for one `inline_content` value, measured before base64 encoding. Advertised only with `filesystem.commits.inline_content`. Larger values return `invalid_request` before commit planning. This bounds each value. `commit.max_request_body_bytes` bounds the whole request. |
| `commit.max_request_body_bytes` | Largest JSON body accepted by a commit request, with base64 inline content and every other field included. A larger body answers `content_too_large`. Advertised by HTTP deployments. |
| `commit.max_operations` | Most path operations one commit may carry. A longer list answers `invalid_request` before planning, on every transport. |
| `commit.max_preconditions` | Most precondition entries one commit may carry, counting entries rather than resources. A longer list answers `invalid_request` before planning, on every transport. |
| `commit.max_content_tokens` | Most content tokens one commit may carry. Over-limit requests answer `invalid_request` before planning. |
| `commit.max_external_content_refs` | Most distinct external content refs one commit's operations may name. Over-limit requests answer `invalid_request` before planning. |
| `commit.max_message_bytes` | Largest accepted commit `message`, in bytes; a longer one answers `invalid_request` before planning. |
| `maintenance.gc.min_grace_window_ms` | Smallest accepted `grace_window_ms` on a `gc` request; smaller values answer `invalid_request`. Derived from the publication budgets, not tuned. |
| `snapshot.max_ttl_ms` | Largest `ttl_ms` accepted by snapshot create and extend requests. A larger value returns `invalid_request`. |
| `snapshot.max_lifetime_ms` | Largest snapshot lifetime measured from the record's creation time. Extension never moves the expiry past this ceiling. |
| `snapshot.max_live_per_namespace` | Most live, unexpired snapshots one namespace may hold. Creation past this limit returns `snapshot_quota_exceeded`. |
| `query.grep.default_limit` | Matches per grep page when the request omits `limit`. |
| `query.grep.max_limit` | Largest accepted grep page limit; invalid limits are rejected as `invalid_request`. The query keys identify the operation's contract even though grep now shares the standard pagination values. |
| `query.grep.scan_budget_files` | Files a plan-less `allow_scan` grep will scan before refusing with `query_unindexable`. |
| `query.grep.tail_budget_files` | Unindexed-tail revisions one grep scans exhaustively before failing with `index_lagging`. |

### 2.2 Feature registry

Every feature key is defined here, alongside the ops it gates. This registry
is frozen per protocol version: new keys arrive with a spec change, not ad
hoc.

| Feature key | Gates | Notes |
| --- | --- | --- |
| `maintenance.grep.index` | Maintaining a namespace's grep index: `GET /v0/maintenance/namespaces/{ns}/grep/index` and its `enable` and `disable` routes, and the `grep_gc` run kind. | The maintenance half of the grep capability, and independent of `query.grep`: searching an index and keeping one built are separately deployable, so a deployment may advertise either key alone. A deployment that maintains no index answers these routes and the run kind `not_supported` with this key. |
| `filesystem.namespaces.create` | Creating namespaces (`POST /v0/namespaces`). | |
| `filesystem.namespaces.fork` | Forking namespaces (`POST /v0/namespaces/{ns}/forks`). | |
| `filesystem.namespaces.delete` | Deleting namespaces (`DELETE /v0/namespaces/{ns}`). | Deletion is terminal. Metadata and the namespace's own content become conditionally reclaimable through maintenance runs with `kind` set to `gc` (section 6.3). A deployment may still advertise `false` and answer `not_supported`. |
| `filesystem.snapshots` | Creating, listing, extending, and releasing snapshots under `/v0/namespaces/{ns}/snapshots`. | |
| `filesystem.commits.inline_content` | Sending file content in a `put_file`, `create_file_by_inode`, or `put_file_revision_by_inode` commit operation. | Advertised when inline writes are enabled. The per-file limit is `commit.max_inline_content_bytes_per_operation`. Without this feature, upload content before committing. |
| `filesystem.uploads.direct_put` | Starting presigned `direct_put` upload sessions (`POST /v0/namespaces/{ns}/uploads`). | The server returns a short-lived, create-only presigned PUT capability for the exact content object. The provider must report a durable whole-object checksum after the write. The key is present only on an endpoint the live conformance suite has run against. Independent of `filesystem.uploads.direct_multipart`: a provider may offer this and no multipart API at all. Raw object keys and caller-managed object-store writes are not part of this feature. |
| `filesystem.uploads.direct_multipart` | Starting presigned `direct_multipart` upload sessions (`POST /v0/namespaces/{ns}/uploads`) and signing their parts (`POST /v0/namespaces/{ns}/uploads/{upload_id}/parts`). | The server opens the provider's multipart upload and returns one short-lived, checksum-bound capability per part. It needs an S3-style multipart API on top of the signing the other keys need, so a provider without one advertises this key alone as absent. |
| `filesystem.downloads.direct_get` | Taking path or inode download grants (`POST /v0/namespaces/{ns}/filesystem/downloads` and `POST /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{revision_no}/downloads`). | The server returns a short-lived presigned GET capability for the selected content object. Any deployment that offers a direct write advertises this too, because one that lets a client create an object larger than `download.service_proxied.max_content_bytes` must be able to hand that object back. Raw object keys are not part of this feature. |
| `query.grep` | Content search (`GET /v0/namespaces/{ns}/grep`). | The serving half of a data-dependent capability: the request also requires a materialized active grep manifest, and a namespace without one answers `not_supported` whatever this key advertises. |

`maintenance/v0`'s only feature key is `maintenance.grep.index`; the rest of that API group
is required ops.

Namespace listing is intentionally not supported in v0. Callers must address
namespaces by id until LoonFS has a scalable namespace catalog/index design.

### 2.3 Data-dependent features

The capability document describes a *deployment*. What is materialized on
*data* — for example, whether a derived index is ready for a namespace — lives
in the owning extension's keyspace ([format: extensions](format.md#124-extensions)), not in the namespace manifest.

A successful data-dependent operation requires both halves: the deployment
advertises the serving capability here, and the namespace's metadata shows
the capability materialized through the extension's own verified readiness
marker.

## 3. Standard error contract

Every error response is a JSON body:

```json
{
  "code": "writer_fenced",
  "message": "writer session fenced: epoch 3 was fenced by epoch 4 (writer `server-b`, acquired at 1739459200000 ms)",
  "request_id": "req_9c2f4a1b7d8e4f21a0b3c4d5e6f70819",
  "details": {
    "fenced_writer_epoch": 3,
    "active_writer_epoch": 4,
    "active_writer_id": "server-b",
    "active_acquired_at_ms": 1739459200000
  }
}
```

`param` is present when the error identifies one invalid input. Its format
depends on where the input came from:

| Input source | `param` value |
| --- | --- |
| JSON request body | JSON Pointer |
| Request header | Header name |
| Query parameter | Parameter name |
| Path parameter | Parameter name |
| CLI-local input | Flag or argument spelling |

Commit shape errors identify the offending expectation field as `/operations/{i}/{field}` or `/preconditions/{i}/{field}`, where `i` is its zero-based position.

`code` is the stable machine contract; `message` is human-readable and may
change between releases; `feature` is present only on `not_supported` errors
and names the capability-document key the client should reconcile against.
Clients must branch on `code`, must tolerate codes they do not recognize, and
must not parse `message`.

This contract covers request-shape failures too: a query string, path
parameter, or JSON body the server cannot parse answers `invalid_request`
inside this envelope, never a framework plain-text rejection — and
authorization is checked first, so a malformed request without valid credentials answers `unauthorized`. An unrecognized query parameter returns `invalid_request` with `param` set to that parameter's name, just like an unknown request-body field (section 6).

`request_id` is the correlation id the server assigned to the request; every
response — success or error — also carries it as the `x-request-id` header,
so a caller's log line and the server's trace can be joined.

`details` is present when the failure carries machine-usable identity, so a
caller never has to parse `message` to act. Every field is optional and
clients must tolerate absent fields exactly as they tolerate unknown codes.

The top-level fields (`code`, `message`, `param`, `feature`, and `request_id`) describe the failed request and error. `details` contains the machine-readable values involved. When a caller supplied one value and the server found another, the fields are named `expected_<field>` and `actual_<field>`.

The codes that populate it:

| Code | Detail fields |
| --- | --- |
| `namespace_deleted` | `namespace_id` identifies the deleted namespace, including a fork's source or target |
| `writer_fenced` | `fenced_writer_epoch`, `active_writer_epoch`, plus `active_writer_id` and `active_acquired_at_ms` when the current manifest records a writer block. Writer ids are process labels, so two runs on one machine can share one; the acquisition stamp is what tells them apart |
| `writer_capacity_exceeded` | `max_writer_sessions` |
| `path_conflict` | `expected_inode_id`, `actual_inode_id` (absent when unbound); `precondition_index` for a failed request precondition |
| `stale_revision` | `inode_id`, `expected_revision_no`, `actual_revision_no` (absent when the inode has no current revision or is not visible); `precondition_index` for a failed request precondition |
| `stale_attributes` | `inode_id`, `expected_attributes_revision_no` (absent when the caller stated no expectation), `actual_attributes_revision_no` (absent when the inode is not visible); `precondition_index` for a failed request precondition |
| `stale_access` | `inode_id`, `expected_access_revision_no` (absent when the caller stated no expectation), `actual_access_revision_no` (absent when the inode is not visible); `precondition_index` for a failed request precondition |
| `binding_version_mismatch` | `inode_id`, `expected_binding_version` (the request's token as supplied), `actual_binding_version` (the current binding's token, absent for the root); `precondition_index` for a failed request precondition. Clients must not parse or order the tokens |
| `commit_id_reuse_conflict` | `commit_id`, plus `committed_seq` and `committed_fingerprint` when the conflict was decided against a durable commit receipt: the sequence that `commit_id` already landed at, and the semantic identity of what landed there (section 5.1). The sequence comes from the receipt and the fingerprint comes from the retained commit row at that sequence, so both are present or neither is; both are absent when nothing has committed under the id yet and two live requests are claiming it at once |
| `rebootstrap_required` | `after_seq`, `retention_floor_seq` |
| `stale_head` | `expected_head_seq`, `actual_head_seq` for a caller-supplied head precondition; `precondition_index` identifies a failed request precondition. |
| `forbidden` | `inode_id` |
| `not_deleted` | `inode_id`, plus `expected_deletion_seq` and `actual_deletion_seq` when a live deletion exists at a different sequence |
| any failed commit | `commit_id` — the idempotency key the request committed under, echoed so failed and uncertain outcomes carry the caller's reconciliation handle (section 5.2) |
| any failed operation | `operation_index` — the zero-based position of the operation that stopped the request, 0 for a one-operation request (section 5.1) |

One code exists specifically so capability handling is uniform from day one:

- `not_supported` (HTTP 501): the deployment does not implement the requested
  op or feature. Any op may return it; a client maps the error to its
  `feature` key and disables or degrades that code path.

The full registry (`ErrorCode` in `loonfs-api`):

| Code | HTTP status | Meaning |
| --- | --- | --- |
| `invalid_request` | 400 | The request is malformed: a path, id, cursor, parameter, staged content reference, configuration value, or commit request limit fails validation. The message names the offending field or limit. |
| `unauthorized` | 401 | Missing or wrong credentials. |
| `forbidden` | 403 | The subject lacks authority the operation needs: some but not all required rights on a checked inode, the authority a move or access update requires, administrator rights on an administrator-only surface, or the namespace's principal scope. A checked inode on which the subject holds no right answers `path_not_found` or `inode_not_found` instead ([authorization](#authorization-in-acl-namespaces)). |
| `content_too_large` | 413 | A request or proxied response exceeds its advertised size limit. Send smaller proxied uploads or use `direct_put` when available. Multipart completions must fit within `upload.complete.max_request_body_bytes`. For large reads, request a download grant when `filesystem.downloads.direct_get` is available. |
| `route_not_found` | 404 | No route matches the request path. |
| `method_not_allowed` | 405 | The path exists but does not serve this HTTP method. |
| `namespace_not_found` | 404 | The namespace has no installed manifest, so it does not exist. |
| `namespace_deleted` | 410 | The namespace id is permanently deleted and can never be created or forked into again. Ordinary operations fail. The response's details identify the deleted namespace. |
| `checkpoint_not_found` | 404 | The checkpoint id names no existing pin. |
| `snapshot_not_found` | 404 | The snapshot id names no pin. Refresh state or choose another snapshot. |
| `snapshot_gone` | 410 | The snapshot has expired, or was deleted while a fork was verifying its selected snapshot. |
| `path_not_found` | 404 | No visible entry at the path. In an ACL namespace, a checked entry on which the subject holds no right also answers this code. |
| `inode_not_found` | 404 | The requested visible or retained inode does not exist. In an ACL namespace, a checked inode on which the subject holds no right also answers this code. |
| `revision_not_found` | 404 | The file has no such revision. |
| `upload_not_found` | 404 | No upload session with this id, or one that was aborted: an aborted session will never select content, so it reports the absence that its deletion will. |
| `namespace_exists` | 409 | The create or fork target already exists: another namespace holds the id. |
| `snapshot_quota_exceeded` | 409 | Creating the snapshot would pass the namespace's live-snapshot limit. Delete a snapshot or wait for a snapshot to expire. |
| `content_not_prepared` | 409 | A path put or explicit create/replace operation references external content without a matching admission, or carries a rejected relevant token. Prepare the content and retry with its proof. |
| `path_conflict` | 409 | The destination path is already bound. |
| `directory_not_empty` | 409 | The directory has children and the operation is not recursive. |
| `stale_head` | 409 | The write raced a head advance, or a caller-supplied `expected_head_seq` does not match the head; retry against fresh state. A read also returns this code when its manifest's segments were collected and a newer manifest exists; read again against current state. A read at a snapshot or checkpoint returns `namespace_corrupt` instead, because collection keeps the segments that a snapshot or checkpoint needs. |
| `stale_revision` | 409 | A caller-supplied base revision is no longer current. |
| `stale_attributes` | 409 | The inode's attribute revision moved while the update was being decided. Two things raise it: a caller-supplied expected attribute revision that is no longer current, and the revision precondition every attribute update carries even when the caller states no expectation. Re-read the attributes and retry. |
| `stale_access` | 409 | The inode's access revision moved while the update was being decided. Re-read the access row and retry. |
| `namespace_unrestricted` | 409 | The namespace's access mode is unrestricted, so it holds no access rows. |
| `binding_version_mismatch` | 409 | The binding version supplied for an inode move or delete is not the entry's current binding version. Re-read the entry before retrying. |
| `not_deleted` | 409 | The undelete target is not the root of a live deletion; nothing to recover. |
| `writer_fenced` | 409 | The writer epoch was superseded by another session. |
| `would_cycle` | 409 | The rename would create a directory cycle. |
| `commit_id_reuse_conflict` | 409 | The commit id was reused with different content. |
| `upload_already_completed` | 409 | The upload session is already completed, so it cannot select other content and cannot be aborted. |
| `upload_content_conflict` | 409 | Different bytes were staged under this upload id. |
| `query_unindexable` | 400 | The pattern has no run of at least 3 literal bytes, so the trigram index cannot narrow candidates; rewrite the pattern, or set `allow_scan` (capped by `query.grep.scan_budget_files`). |
| `rebootstrap_required` | 409 | The resume position is unanswerable — a change cursor below the retention floor, or a listing cursor minted ahead of the serving head; restart from a fresh listing or checkpoint. |
| `not_supported` | 501 | The deployment does not implement the requested op or feature. |
| `commit_outcome_unknown` | 503 | The publish outcome was not observed; the commit may or may not be visible. Retry with the same commit id or reconcile. |
| `commit_queue_full` | 503 | The namespace write queue is full; back off and retry. |
| `writer_session_closed` | 503 | This node holds no open writer session for the namespace. The request was not admitted; retry on the node the namespace is assigned to. |
| `writer_capacity_exceeded` | 503 | This node holds its maximum number of writer sessions. The request was not admitted; retry on another node, or wait for a session to close in a single-node deployment. |
| `server_busy` | 503 | The server is at its configured concurrency limit for this kind of work (proxied upload bodies or proxied content reads); back off and retry. |
| `shutting_down` | 503 | The serving process closed admission for shutdown; work admitted earlier still settles. Retry against a live instance. |
| `deadline_exceeded` | 503 | The server cancelled a bounded request at its configured `request_deadline_ms`. A commit may still land after this response; reconcile it by commit id before retrying. |
| `content_not_materialized` | 503 | The file is committed, but a direct download requires a content object that this deployment cannot create. Read through the proxied content route, or retry after a fold writes the object. |
| `checkpoint_unavailable` | 503 | Required checkpoint state is unavailable: not yet published, deleted during the operation, or referenced material is missing. Retry after maintenance. |
| `maintenance_required` | 503 | Namespace metadata requires maintenance before the request can be served; run maintenance and retry. The WAL write-stop threshold refuses new commits. A commit id the namespace already knows is still answered from its receipt. |
| `index_lagging` | 503 | The grep index trails the head past the exhaustive-scan budget; let the grep worker catch up (or set `allow_stale`) and retry. |
| `storage_permission_denied` | 503 | The backing object store rejected the deployment's storage credentials for this operation. Fix the storage credentials or bucket policy; an unchanged retry will not succeed. |
| `index_corrupt` | 500 | The grep index's derived state failed validation. Disable and re-enable grep on the namespace to rebuild it; core filesystem state remains available. |
| `namespace_corrupt` | 500 | Durable namespace state failed validation. |
| `server_error` | 500 | Unclassified internal failure. |

Automated retry is narrower than the HTTP status. Raw transport failures may
be retried. Of the registered error codes, only `commit_queue_full`,
`server_busy`, and `shutting_down` can clear without caller or operator action.
`writer_session_closed` and `writer_capacity_exceeded` are resolved by routing
the request to another node, not by waiting, and carry no `Retry-After` header.
`checkpoint_unavailable`, `maintenance_required`, and `index_lagging` require
maintenance. `storage_permission_denied` requires the operator to fix the
storage credentials or bucket policy. `commit_outcome_unknown` and
`deadline_exceeded` require the caller to determine whether a mutation
completed before retrying it.
Responses carrying one of the three immediately retryable codes include
`Retry-After: 1`. Generated SDKs retry a response that carries `Retry-After` and do
not retry on status alone.

Precondition failures surface as `409` resource-state conflicts
(`stale_revision`, `stale_head`, `commit_id_reuse_conflict`) rather than
`412`: v0 treats them as conflicts with current namespace state, not HTTP
conditional-request failures.

## 4. SDK shape

One SDK serves both backends; deployment mode never forks the client
codebase.

Generated clients accept a default actor at construction and a per-request header override.

Generated SDKs use schema names as public type names. A resource body uses the resource name, such as `Checkpoint` or `UploadSession`. A response envelope uses `<Verb><Noun>Response`, such as `ListCheckpointsResponse`. The request and response schemas of an operation share its verb, as in `CreateDownloadRequest` for `create_download`. Namespace-owned resources include `namespace_id`.

Revision numbers, change sequences, attribute revisions, manifest numbers,
writer epochs, and grep run numbers are JSON integers from 0 through
9007199254740991 (`2^53 - 1`). Implementations MUST reject larger input values
and MUST NOT store a larger value. `inode_id` is not an ordinal and may use the
full `u64` range.

Field-name suffixes each mean one thing. `_seq` is a position in the namespace
commit history. `_no` is a monotonic counter scoped to one object: a file's
revisions, an inode's attribute revisions, or a namespace's manifests.
`_index` is a 0-based position inside a container. `_number` is a 1-based
position defined by a provider or a tool. `_id` is an opaque identity.

### 4.1 Public inode identity

Public APIs represent an inode ID as a string such as `ino_27`. The value must
start with the lowercase prefix `ino_`, followed by a nonzero `u64` with no
leading zeroes. Numbers, numeric strings such as `"27"`, zero, and uppercase
prefixes are invalid. `ino_1` identifies the namespace root.

An inode ID is only unique within its namespace. Use `namespace_id` and
`inode_id` together when identifying an inode. Clients MUST treat the ID as an
opaque value and MUST NOT create IDs or infer ordering from the numeric suffix.

- The embedded handles (`loonfs::FsWriter`, `loonfs::FsReader`) and the
  remote client (`loonfs_client::Client`) expose the same operations under the
  same names, including the `get_capabilities()` accessor that returns the
  capability document of section 2.1. For the remote client the document is
  fetched from `GET /v0/capabilities` and cached; for the embedded handles it
  is a constant.
- The two surfaces stay aligned by sharing one definition of every option
  struct they both take (`PutFileOptions`, `CreateDirectoryOptions`,
  `DeleteOptions` live in `loonfs-api` and are re-exported by both), not by a
  trait either one implements. There is no transport abstraction to program
  against: a host picks the embedded runtime or the HTTP client directly.
- Unsupported surface area is typed: individual ops return the
  `not_supported` error with its `feature` name, so gating logic — check the
  capability document, fall back on `not_supported` — is identical against
  either backend.
- Generated SDKs group operations by resource, not by API group. `capabilities`,
  `namespaces`, `snapshots`, `commits`, `changes`, `files`, `inodes`, `trash`,
  and `uploads` sit at the client root. The `maintenance` API group keeps its prefix as
  a nested group, as in `maintenance.checkpoints.create`, because a hosted
  deployment may hide the whole API group. A method on a group's own resource is
  a bare verb: `create`, `retrieve`, `list`, `delete`, or the action, as in
  `fork` and `grep`. A sub-resource of one file or inode is verb plus noun on
  the parent, as in `files.listRevisions` and `inodes.listChildren`. The
  `/health`, `/readiness`, and `/metrics` probes are HTTP-only and have no
  SDK method.

### 4.2 Checksums

Every public checksum value uses one shape:

```json
{ "algorithm": "sha256", "value": "<64 lowercase hex>" }
```

The allowed algorithms are `sha256`, `crc64nvme`, and `crc32c`. Their values
contain exactly 64, 16, and 8 lowercase hexadecimal characters respectively.
Other algorithms and invalid values are rejected.

The surrounding field defines what the checksum covers. Checksums in
`ContentRef` and `UploadContentClaim` cover the complete content; a part
checksum covers one multipart upload part. A `checksum_algorithm` field selects
an algorithm but does not contain a checksum value.

Service-proxied uploads use `sha256`. Direct PUT and direct multipart use the
`checksum_algorithm` returned when the session begins. Direct multipart
currently uses `crc64nvme`. Reads verify the algorithm stored in the content
reference.

## 5. Minimal upload, commit, and change-feed model

To write a file, prepare its content and then commit the change. Committed
changes are available through the change feed in sequence order.

Uploaded content is durable before the commit, but the file is not yet visible.
Inline content becomes durable and visible in the same write
([format section 1.5](format.md#15-file-contents-and-ownership)). In both cases,
the commit takes effect when the next numbered WAL segment is written with
put-if-absent. Accepting a request into a batch does not mean it has committed.

With the embedded `loonfs::FsWriter`, you can prepare content separately from
committing it. Call `prepare_file_bytes` with the file's bytes. Files at or below
the enabled inline threshold stay in memory; other files are uploaded as
content objects. To import existing content, call `prepare_content_ref`. The bytes
are verified and copied to a new object owned by the destination namespace.
Both calls return an opaque prepared value.

Use `put_file_prepared` to commit that value. Inline bytes may need to be
uploaded first if WAL limits are reached. Already uploaded content requires no
further content-object I/O during publication. For an upload session,
`complete_upload_prepared` returns both the completion response and a prepared
value. Over HTTP, include either inline bytes or a content reference with its
content token in the commit request.

When preparation requires an upload, the upload session is recorded before the
content object is written and completed afterward. Prepared uploads have a
deadline no later than the expiry of the last token the completed session could
issue. The deadline is checked when the commit enters a batch and again just
before the numbered WAL write. Both checks use the request clock plus the time
elapsed since the publication attempt began. This matches the lifetime of
remote upload tokens ([upload transport](#69-upload-transport); [upload cleanup](format.md#116-upload-session-cleanup))
and prevents publication from referring to content that may have been reclaimed.
Prepared inline bytes have no expiry.

If the final deadline check rejects a candidate, that candidate receives its own error. Other accepted candidates receive `stale_head` so the publisher plans them again. The rejected batch writes nothing.

Prepared content belongs to one namespace. Two namespaces cannot share a
prepared value: uploads and garbage collection are tracked separately for each
namespace. Use
`prepare_content_ref` to import content into another namespace. The result
refers to the new copy owned by that namespace. A subject must be an
administrator of the reference's owner namespace to import it.

An import checks authorization against the owner’s current head. A reference that matches the owner’s resident content may read those bytes. Otherwise, the reference’s owner namespace and content ID determine its object key. A missing object returns `namespace_deleted` for a deleted owner and `namespace_corrupt` for an active owner.

### 5.1 Commit identity and preconditions

A commit is one request: a `commit_id` — a client-generated stable
idempotency key that must be reused verbatim for safe retries — an actor id
carried in the `Loonfs-Actor` request header, an optional `message` (a human-readable annotation that is part of the commit's
identity), an optional ordered `preconditions` array, and an ordered, non-empty list of path operations. A request with
one operation is the same shape as a request with many, so a convenience
call and a one-element list are the same commit and fingerprint alike.
A `message` is at most 4096 bytes; a longer one is rejected with
`invalid_request` before planning, on every transport.
A commit whose estimated encoded log would exceed one WAL segment is rejected with `content_too_large` before publication; the message names the [WAL document limit](format.md#a5-wal-records).
The semantic fingerprint includes preconditions in request order. Changing a
precondition or its position changes identity. The fingerprint input always
includes the precondition list, including an empty list.

The operations of one request commit together, in order, as one logical
commit. Operation `k` is planned against authoritative namespace state plus
everything operations `0..k` do, so a request can create a directory and
write into it, or delete a path and recreate it. Either every operation
commits or none does: the first operation that fails aborts the whole
request, nothing it or its predecessors would have written becomes visible,
and the error names the
position that stopped it in `details.operation_index`. The error code stays
the failing operation's own; no code is specific to batching.

The server plans each operation against authoritative namespace state under
the publish lock, synthesizing the exact semantic checks the operation
implies (revision identity, binding identity, name absence, directory
emptiness, ancestor visibility) so races fail explicitly rather than
silently merge. Those checks are evaluated where their operation runs, which
is what lets a later operation depend on an earlier one. Callers add their
own cross-request preconditions on operations where staleness matters. Puts can
check the current inode and revision. Moves and copies can check the
destination inode and revision. Deletes, undeletes, inode-addressed writes,
and namespace deletion have preconditions for their corresponding state. Each precondition
checks the state visible to its operation, including changes made by earlier
operations in the same request.

Move and copy operations name their endpoints `source_*` and
`destination_*`. Their preconditions and the `moved` event use the same
words. Undelete's optional restore location is `destination_path`. A field
that names the addressed resource keeps the resource's own name, such as
`inode_id`, `namespace_id`, or `path`; no directional prefix is added for
symmetry. A creation request may say `new_*` for the resource it creates.
`target` describes an action subject, a persisted relationship, or a goal,
and is not an endpoint name in the move and copy family. `local` and
`remote` name coordinate systems.

Commit bodies reject unknown fields so a misspelled precondition cannot be ignored. For example, dropping a letter from `expected_revision_no` returns `invalid_request` instead of applying a write without that precondition.

Every named entry includes a `binding_version`, an opaque token identifying its current parent/name binding. Creating, moving, or undeleting an entry produces a new token; content and attribute writes do not. Clients must not parse or order these tokens. A token is valid only for the namespace that issued it.

Inode-addressed moves and deletes require the token as `expected_binding_version`. A valid token that does not match the entry's current binding returns `binding_version_mismatch`; a malformed token or one from another namespace returns `invalid_request`. The precondition is part of the commit's identity and is evaluated after any earlier operations in the same request.

The server validates each request against authoritative namespace state and
may reject it immediately. A tentatively accepted request becomes one
committed logical commit when put-if-absent of the next WAL number succeeds.
A failed precondition writes nothing; the loser discovers the new tip,
re-plans, and retries.

If the WAL put's outcome was never observed — a transport failure after
the put was sent — the server reports `commit_outcome_unknown`: the commit
may already be visible. Section 5.2 defines how the caller resolves it.

The server may publish multiple committed logical commits in one WAL segment
put, but it must preserve per-commit idempotency, ordering,
and change-feed identity.

Annotations may be used to correlate multiple logical commits that belong to
one higher-level workflow, for example with fields such as `operation_id`,
`operation_kind`, or `operation_part`.

The change feed returns ordered committed changes after an explicit cursor.
Callers may bound a response with `limit`; a truncated page returns
`next_after_seq`, which should be used as the next request's `after_seq`. If
the requested cursor is older than the retention floor, the caller must
re-bootstrap instead of expecting older incremental history to remain
available.

### Request-level preconditions

`preconditions` is optional and defaults to an empty list, with at most 1024 entries as advertised by `commit.max_preconditions`.
More entries return `invalid_request` before planning.
The pre-state includes earlier admitted candidates in the batch and none of this candidate's operations.
Its head sequence is the last admitted commit's sequence, or the batch's base head sequence for the first admission.

`namespace_head` requires `expected_head_seq` to equal the pre-state head sequence.
Any intervening namespace commit invalidates it, including unrelated writes.
Failure returns `stale_head` with `expected_head_seq` and `actual_head_seq`.

`file_revision` requires a visible `inode_id` whose current content revision equals `expected_revision_no`.
Any new revision of that inode, including restore, invalidates it, as does deletion.
Failure returns `stale_revision`; `actual_revision_no` is absent when the inode is not visible or has no current revision.

`path_binding` requires an absolute `path` and `expected_inode_id`.
A missing or null `expected_inode_id` is a decode error.
The path must resolve to that inode. A different inode or an unbound path returns `path_conflict` with `expected_inode_id` and `actual_inode_id`; the latter is absent when unbound.
Without a binding version, returning to the same inode satisfies the precondition again.
Optional `expected_binding_version` also detects moves away and back.
A binding version mismatch returns `binding_version_mismatch`.
Binding the root to `ino_1` passes without a binding version. The root has no binding version, so supplying one returns `binding_version_mismatch`.

`path_absence` requires only an absolute `path` and rejects inode or binding version fields.
It passes when no visible entry resolves at the full path, including when an ancestor is missing or an intermediate component is not a directory.
A bound path returns `path_conflict` with `actual_inode_id` set and `expected_inode_id` absent.
Absence of `/` fails with actual inode `ino_1`.

`attributes_revision` requires a visible `inode_id` whose attribute revision equals `expected_attributes_revision_no`.
Any attribute update invalidates it, as does deletion. Content-only rewrites do not.
Failure returns `stale_attributes`; `actual_attributes_revision_no` is absent when the inode is not visible.

`access_revision` requires a visible `inode_id` whose access revision
equals `expected_access_revision_no`. Any access update invalidates it.
Failure returns `stale_access`; `actual_access_revision_no` is absent when
the inode is not visible.

Receipt resolution comes first: an identical landed request returns its original commit even when its precondition is stale.
Reusing that commit ID with different preconditions returns `commit_id_reuse_conflict`.
Preconditions run in order before operations; the first failure returns its kind's error with zero-based `precondition_index`.
A failed precondition reserves no sequence or inode and leaves the planning view unchanged.
A retry after a lost numbered WAL put evaluates preconditions again against the new basis.
Preconditions are admission conditions, stored only through the fingerprint in WAL records and commit rows, and never evaluated during replay.

### Actor attribution

Every commit, namespace creation, namespace fork, and administrator recovery
requires an actor in the `Loonfs-Actor` header ([identity headers](#identity-headers)).
The application supplies a stable opaque identifier
of 1 to 256 visible ASCII characters (0x21 through 0x7E), with the identity scope it needs. LoonFS preserves
it on the commit and the metadata created by that commit. Namespace creation
and forking record it as `created_by` on the namespace. LoonFS does not
authenticate the actor or resolve profile information. The application must
authenticate the user and authorize the operation before sending the request.
Use a stable internal ID, not an email address or display name.

The header's value is the semantic commit fingerprint's `actor_id`. Reusing a
`commit_id` with a different actor fails with
`commit_id_reuse_conflict`. The commit timestamp is not part of the
fingerprint.

Responses expose attribution through these fields:

| Field | Meaning |
| --- | --- |
| `created_by`, `created_at_ms` | Commit attribution for inode creation. |
| `created_by`, `created_at_ms` on a namespace | Who created or forked the namespace, and when. |
| `revision_committed_by`, `revision_committed_at_ms` | Commit attribution for the current file revision on stat and list entries; absent on directories. |
| `commit_id` | Owning commit identity on revision-history items and committed changes. |
| `committed_by`, `committed_at_ms` | Commit attribution on revision-history items and committed changes. |
| `attributes_updated_by`, `attributes_updated_at_ms` | Commit attribution for the latest stored attribute update; absent for the initial empty attributes at revision 0. |
| `deleted_by`, `deleted_at_ms` | Commit attribution for an active trash entry. |

### Subject and principals

`Loonfs-Principal-Scope` identifies the domain that issued the subject's
principal ids. `Loonfs-Principals` carries comma-separated principal ids without whitespace.
Principal, subject, and scope ids never contain a comma, so the list is
unambiguous.
`Loonfs-Subject` identifies the subject the request acts as. The principal
count is capped by the advertised `access.max_principals_per_request` limit.
Without a subject context, a request acts with the token holder's service
authority, except on the operations that require a subject context in an ACL
namespace. The [identity header table](#identity-headers) lists those
operations and how the subject id falls back to `Loonfs-Actor`. A subject
scope that differs from the namespace's `principal_scope` answers `forbidden`,
naming the expected and actual scopes, before any grant is read.
These ids are opaque and reach access logs like the actor id; use internal ids,
never email addresses or display names.

The subject id is part of a commit's semantic identity. Retrying a commit id
from another subject answers `commit_id_reuse_conflict`. The scope is not part
of the fingerprint or upload ownership: a namespace has one scope and refuses
subjects from every other scope.

Embedded callers set the subject once, on the handle they use. Every read,
commit, and upload through that handle acts as that subject. A commit does not
carry its own subject. The CLI records the handle's subject in its upload
journal beside the request, and it refuses to resume the upload under a
different subject.

### Identity headers

Four request headers carry identity. `Loonfs-Actor` is attribution: an actor
id of 1 to 256 visible ASCII characters (0x21 through 0x7E). `Loonfs-Subject`,
`Loonfs-Principal-Scope`, and `Loonfs-Principals` form the subject context that
an ACL namespace authorizes against ([subject and principals](#subject-and-principals)).
All four are optional in the transport schema. Each operation enforces its own
requirements while handling the request:

| Operations | `Loonfs-Actor` | Subject context |
| --- | --- | --- |
| `create_commit` | Required. Recorded as `committed_by`. | Required in an ACL namespace. |
| `create_namespace` | Required. Recorded as `created_by`. | Not read. |
| `fork_namespace` | Required. Recorded as `created_by`. | Optional. When present, an ACL source requires an administrator subject. When absent, the request acts as the token holder. |
| `delete_namespace`, `create_snapshot`, `list_snapshots`, `extend_snapshot`, `delete_snapshot`, `list_changes` | Not required. | Optional, with the same administrator rule as `fork_namespace`. |
| Path and inode reads, downloads, `list_trash`, `grep`, and upload-session operations | Not required. | Required in an ACL namespace. |
| `run_maintenance` | Required by `recover_administrator`, which records it as `committed_by` on the recovery commit. Other jobs do not record it, but still reject a malformed value. | Not read. |
| `get_capabilities`, `get_namespace`, the other maintenance operations, and the health, readiness, and metrics routes | Not read. | Not read. |

Where an operation reads the subject context:

- `Loonfs-Principals` and `Loonfs-Principal-Scope` are sent together.
  `Loonfs-Subject` or the scope without principals answers `invalid_request`
  with `param` `Loonfs-Principals`. Principals without the scope answer
  `invalid_request` with `param` `Loonfs-Principal-Scope`.
- The subject id is `Loonfs-Subject` when present, and otherwise
  `Loonfs-Actor`. With neither, the request answers `invalid_request` with
  `param` `Loonfs-Subject`. An actor used as the subject id must also be a
  valid subject id, which cannot contain a comma; otherwise the `param` is
  `Loonfs-Actor`.
- An operation that does not require an actor accepts a complete explicit
  subject (`Loonfs-Subject`, scope, and principals) without one.
- An unrestricted namespace checks no rights, but it still rejects an
  incomplete or malformed subject context. A commit's subject id joins its
  fingerprint in either access mode.
- In an ACL namespace, an operation that requires the subject context answers
  `invalid_request` with `param` `Loonfs-Principals` when it is absent. A
  subject scope that differs from the namespace's `principal_scope` answers
  `forbidden`.

A missing required `Loonfs-Actor` answers `invalid_request` with `param`
`Loonfs-Actor` and message `missing required header Loonfs-Actor`. A malformed
actor answers `invalid_request` with the same `param` and the validator's
reason. Authorization is checked first.
Header names are case-insensitive; HTTP/2 sends them lowercase. Retries and
replayed requests must carry the same header values, so saved requests must
keep the actor and subject context beside the body. Request headers reach
access logs, so every value must be an opaque identifier, never an email or a
display name.

### 5.2 Commit responses and safe retry

Every successful commit returns a `Commit` with its `namespace_id`,
`commit_id`, and `committed_seq`. The response also includes `committed_by`,
`committed_at_ms`, the optional `message`, and `events`. These match the change
feed, including values such as newly created inode IDs. If the caller omits an
optional `commit_id`, the response includes a generated ID.

To retry a commit, resubmit the same request with the same `commit_id`:

- If the original request committed, the response includes the original
  `committed_seq`. No new commit is made.
- If it did not commit, the request can commit on this attempt.
- If the ID was used for a different request, the retry returns
  `commit_id_reuse_conflict`. Changing a write precondition counts as a
  different request, even if the operations are unchanged.

A new `commit_id` means a new commit. After `commit_outcome_unknown`, a
transport failure, or a process restart, retry with the original ID and
request. There is no separate commit-status lookup.

**Retention limits.** These retry rules apply while the commit receipt is
retained. Once the retention floor passes the commit and metadata runs are
rebuilt, the receipt is removed. The same ID can then be used for a new commit,
so a late retry may apply the operation again. For example, a replacing put
may append another revision with the same content. Applications that retry
beyond this window must check whether the original operation took effect.

A replay returns the original response, including `events`, from the retained
commit record. A reuse conflict also reports the original `committed_seq` when
it was resolved from a receipt.

**File content.** A commit can refer to an upload or include the file's bytes
directly (inline content). Content is part of the request's identity, and the
retry rules depend on how it was supplied:

| Content supplied with the commit | What to retain for a retry |
| --- | --- |
| Uploaded content | The original `content_ref`. Uploading the same bytes again creates a new object; using that object's reference with the original commit ID returns `commit_id_reuse_conflict`. |
| Inline content | The original bytes. Different bytes return `commit_id_reuse_conflict`, even if the file size is unchanged. |

Inline retry rules still apply if the bytes are written to a separate content
object to stay within WAL limits. Switching between inline bytes and an uploaded
reference changes the request's identity, even for identical content.

If the original commit succeeded and its receipt is still available, retrying
the same request returns the original result without uploading the file again.
This also works after a restart or on another server. Changed bytes or a
different subject still return `commit_id_reuse_conflict`.

**Prepared content.** Prepare the content once, retain the result, and reuse
it with the same explicit commit ID, path, actor, and options on each attempt.
This works for both inline content and completed uploads.

| Client | Prepare content | Publish retained content |
| --- | --- | --- |
| Rust HTTP and embedded runtime | `prepare_file_bytes()` / `prepare_file_stream()` | `put_file_prepared()` |
| Python synchronous client | `files.prepare()` | `files.upload_prepared()` |
| Go | `Files.Prepare()` | `Files.UploadPrepared()` |
| TypeScript server and browser clients | `files.prepare()` | `files.uploadPrepared()` |

In the embedded runtime, preparing content at or below the enabled inline
threshold makes no storage request. The HTTP clients prepare inline content
when the server advertises `filesystem.commits.inline_content` and the actual
file bytes fit `commit.max_inline_content_bytes_per_operation`. The Go, Python, and TypeScript
helpers retain at most 64 KiB inline, even if the server advertises a larger
limit. Streams use bounded lookahead (the effective limit plus one byte); larger
sources continue through the existing upload path with that prefix preserved.
The Rust HTTP client caches capabilities; once populated, writing a small file
requires only the commit HTTP request. The generated SDK helpers still retrieve
capabilities during preparation, but skip upload creation, payload transfer, and
completion for inline content.

In Go, Python, and TypeScript, preparation returns `PreparedFile`, either the
existing staged `PreparedContent` or an `InlinePreparedContent`. The staged type's
constructor and reference/token fields are unchanged. Callers that inspect those
fields must first narrow to the staged variant; callers that retain the result
and pass it to `upload_prepared` / `UploadPrepared` / `uploadPrepared` need no
transport-specific logic. Inline values retain immutable bytes (base64 in Go and
TypeScript) and have no uploaded reference before publication. Applications that
explicitly need a completed upload can still use the lower-level uploads API.
Publication uses the retained variant without checking capabilities again or
falling back to another representation after an error. A lost response must be
retried with the original prepared value and commit ID.

Prepared inline bytes have no expiry. Completed uploads retain their normal
expiry; preparing content does not extend it. Preparation does not make a file
visible. A commit is still required.

The whole-file convenience methods (`files.upload`, `files.uploadStream`,
`files.upload_stream`, `Files.Upload`, `Files.UploadStream`,
`put_file_bytes()`, and `put_file_stream()`) prepare content on every call.
Small files can be prepared inline, so another call with the same bytes,
commit ID, and options can replay the original commit if preparation still
selects inline content. Larger files and files prepared with inline writes
disabled create new objects. When a new object is created, reusing the original
commit ID conflicts, even for identical bytes. A capability change can also
change the representation; retaining the prepared value avoids that risk. An
unused upload can be reclaimed after its grace period.

For reliable retries, use the preparation methods above and retain the complete
commit request. The remote CLI saves this request before submission so it can
resend it after a process restart. If a helper call omits `commit_id`, each
call generates a new one. The actor can be configured once on the client.

At the WAL write-stop threshold, new commits return `maintenance_required`.
Retries of retained commits can still return their original commits, including after
`commit_outcome_unknown` or `deadline_exceeded`. Writer-session, availability,
and corruption checks still apply.

The blocking Rust client automatically retries operations labeled `idempotent`
or `replayable`. It makes one attempt for operations labeled `not_idempotent`:
namespace create, fork, and delete; upload-session begin; checkpoint create;
maintenance; grep index collection; and store probe. Presigned direct PUT also
receives one attempt.

### 5.3 Writer topology and fencing

Each namespace has one active writer session. Many concurrent clients may
submit commits through that session — the reference server is exactly this
shape: one service-level writer session coordinating every client request —
and independent readers scale separately.

Replacing the active writer is not an approval flow. Opening a writer and
publishing acquires the next writer epoch, and epoch fencing — not liveness,
not a lease — is what keeps the displaced session from corrupting anything:
its next publish fails with `writer_fenced`, terminally for that session.
The error's `details` name the epoch and writer that displaced it, so an
operator can tell a planned failover from two writers misconfigured against
one namespace.

A node closes a session to hand a namespace off. Closing writes nothing durable.
The next node's first publish acquires the next writer epoch. A request sent to
a node without the namespace's open session fails with `writer_session_closed`.
A node holds a bounded number of sessions and refuses to open one beyond that
limit with `writer_capacity_exceeded`.

The standard mutation operations are defined in [the format specification](format.md#66-operations-and-wal-deltas). `POST /commits` (section 6.8) exposes those operations
over HTTP. The same identity, durability, and visibility rules apply to every
API that implements them.

## 6. Representative HTTP binding

HTTP is one transport binding for these abstract operations. It is not the
underlying semantics.

GET routes name resources, so they use nouns such as `entry`, `entries`, `content`, `revisions`, and `trash`. A POST route ends in a verb when it invokes an action rather than creating a resource, as in `enable`, `disable`, `abort`, `complete`, and `probe`. `/v0/maintenance/` is the only API group prefix. Other routes are grouped by resource, including `GET /v0/namespaces/{ns}/grep`.

Operation IDs start with a verb. `get` reads one resource, `list` reads a page, and `create` posts a new resource to a collection. Other verbs describe the operation directly, as in `grep`, `run_maintenance`, and `delete_checkpoint`. Operation IDs are the wire registry only. Generated SDK group and method names come from the SDK naming table in the OpenAPI postprocessor, which every operation must appear in or be explicitly excluded from.

### Authentication and transport

A deployment that sets a token authenticates every request with an HTTP
bearer credential:

```
Authorization: Bearer <auth_token>
```

A request without it, or with the wrong value, answers 401 `unauthorized`.
Authorization is checked before the request is otherwise parsed, so a
malformed body from an unauthenticated caller still answers 401 rather than
400. Two routes are exempt and always answer unauthenticated, because they
are what a load balancer probes: `GET /health` and `GET /readiness`.
Everything else, `GET /v0/capabilities` included, requires the token. The
generated `openapi.json` states this as a global `bearer_auth` requirement
with those two operations overriding it.

The identity headers `Loonfs-Actor`, `Loonfs-Subject`, `Loonfs-Principal-Scope`,
and `Loonfs-Principals` are optional in the transport schema. The
[identity header table](#identity-headers) lists what each operation requires.

Request bodies reject unknown fields, at every level of nesting, with 400
`invalid_request`. Most request fields are optional and several of those are
preconditions, so a field the server does not recognize cannot be ignored: a
misspelled precondition would decode to its default and the server would carry out
a different request than the caller asked for. Response bodies are the other
way round, because a client must keep working against a server newer than
itself and so must tolerate fields it does not know (section 7.2).
`ContentRef` and `Checksum` are closed shapes on both sides: a
response never adds a field to one of them, and new content strategies or
checksum algorithms arrive as new `kind` and `algorithm` values, which is why
those two schemas are `additionalProperties: false` in responses too.
Unknown content kinds fail to decode, just like unknown checksum algorithms.
The encoding conventions in `format.md` state the same rules and extend them to
durable shapes.

Query strings reject unknown parameters. For example, `DELETE /v0/namespaces/{ns}?expected_head_sq=418` returns 400 `invalid_request` rather than deleting the namespace without the intended precondition. Routes that declare no query parameters reject all query parameters. `GET /health`, `GET /readiness`, and `GET /metrics` are exceptions because probes and scrapers may append their own parameters.

The token is a bearer credential and so is everything the upload routes hand
back: a presigned direct-upload URL is a capability to write to the
deployment's bucket, carried in an ordinary response body. Both are readable
by anyone who can read the connection. Serve `https` for any deployment
reachable beyond localhost — either terminated by the server itself or by a
proxy in front of it.

Every operation includes an `x-loonfs-retry` value for generated SDKs:

- `idempotent`: clients may repeat the request safely.
- `replayable`: repeating the request returns the original result. Commits replay by `commit_id`, and upload completion returns the first result.
- `not_idempotent`: generated clients make one automatic attempt and leave recovery to the application.

The table below lists the retry class for every v0 operation.

| Purpose | Operation ID | Retry class | Representative HTTP shape |
| --- | --- | --- | --- |
| Check server health | `get_health` | `idempotent` | `GET /health` |
| Check server readiness | `get_readiness` | `idempotent` | `GET /readiness` |
| Read deployment capabilities | `get_capabilities` | `idempotent` | `GET /v0/capabilities` |
| Create a namespace | `create_namespace` | `not_idempotent` | `POST /v0/namespaces`; requires the `Loonfs-Actor` header and accepts the optional `access` body field |
| Read a namespace | `get_namespace` | `idempotent` | `GET /v0/namespaces/{ns}` |
| Read a path entry | `get_path_entry` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/entry?path=/docs/report.txt&include_attributes=false&snapshot_id=...` (`include_attributes` is optional and defaults to `true`; `snapshot_id` is optional) |
| Read an inode | `get_inode` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}?include_attributes=false&snapshot_id=...` (`include_attributes` is optional and defaults to `true`; `snapshot_id` is optional) |
| List path entries | `list_path_entries` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/entries?path=/docs&limit=100&cursor=...&include_attributes=true&snapshot_id=...` (`include_attributes` is optional and defaults to `false`; `snapshot_id` is optional) |
| List directory children by inode | `list_inode_children` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}/children?limit=100&cursor=...&include_attributes=true&snapshot_id=...` (`include_attributes` is optional and defaults to `false`; `snapshot_id` is optional) |
| List file revisions by path | `list_file_revisions` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/revisions?path=/docs/report.txt&limit=100&cursor=...` |
| List file revisions by inode | `list_file_revisions_by_inode` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}/revisions?limit=100&cursor=...` |
| Read current or prior file content by path | `get_file_bytes` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/content?path=/docs/report.txt&snapshot_id=...` (`revision_no` and `snapshot_id` are optional and mutually exclusive) |
| Read prior file content by inode | `get_file_revision_bytes_by_inode` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{revision_no}/content` |
| Start a download by path | `create_download` | `idempotent` | `POST /v0/namespaces/{ns}/filesystem/downloads` with body `path`, optional `revision_no`, and optional `snapshot_id` (`snapshot_id` cannot be combined with `revision_no`) |
| Start a download by inode | `create_download_by_inode` | `idempotent` | `POST /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{revision_no}/downloads` with no body |
| List recoverable deletions | `list_trash` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/trash?limit=100&cursor=...` |
| Create a commit | `create_commit` | `replayable` | `POST /v0/namespaces/{ns}/commits`; requires the `Loonfs-Actor` header |
| Create an upload session | `create_upload` | `not_idempotent` | `POST /v0/namespaces/{ns}/uploads`; returns the open session |
| Upload content through the server | `put_upload_content` | `idempotent` | `PUT /v0/namespaces/{ns}/uploads/{upload_id}/content`; returns the open session with the staged `content_ref` |
| Create multipart upload URLs | `sign_upload_parts` | `idempotent` | `POST /v0/namespaces/{ns}/uploads/{upload_id}/parts` |
| Complete an upload | `complete_upload` | `replayable` | `POST /v0/namespaces/{ns}/uploads/{upload_id}/complete` |
| Read an upload session | `get_upload` | `idempotent` | `GET /v0/namespaces/{ns}/uploads/{upload_id}`; completed sessions return a fresh `content_token` |
| Abort an upload session | `abort_upload` | `idempotent` | `POST /v0/namespaces/{ns}/uploads/{upload_id}/abort` (terminal and repeatable; a completed session is refused) |
| Read committed changes | `list_changes` | `idempotent` | `GET /v0/namespaces/{ns}/changes?after_seq=123&limit=100&snapshot_id=...` (`snapshot_id` is optional) |
| Create a snapshot | `create_snapshot` | `not_idempotent` | `POST /v0/namespaces/{ns}/snapshots`; requires `name` and `ttl_ms`. |
| List snapshots | `list_snapshots` | `idempotent` | `GET /v0/namespaces/{ns}/snapshots?limit=100&cursor=...`. |
| Extend a snapshot | `extend_snapshot` | `idempotent` | `POST /v0/namespaces/{ns}/snapshots/{snapshot_id}/extend`; requires `ttl_ms` and clamps to the lifetime ceiling. |
| Delete a snapshot | `delete_snapshot` | `not_idempotent` | `DELETE /v0/namespaces/{ns}/snapshots/{snapshot_id}` (deletes the pin; a missing id returns `snapshot_not_found`) |
| Fork a namespace | `fork_namespace` | `not_idempotent` | `POST /v0/namespaces/{source_ns}/forks`; requires the `Loonfs-Actor` header |
| Delete a namespace | `delete_namespace` | `not_idempotent` | `DELETE /v0/namespaces/{ns}?expected_head_seq=418` (feature `filesystem.namespaces.delete`; the precondition is optional) |
| Read namespace diagnostics | `get_namespace_diagnostics` | `idempotent` | `GET /v0/maintenance/namespaces/{ns}/diagnostics` |
| Create a checkpoint | `create_checkpoint` | `not_idempotent` | `POST /v0/maintenance/namespaces/{ns}/checkpoints`; requires `name` and accepts `ttl_ms` |
| List checkpoints | `list_checkpoints` | `idempotent` | `GET /v0/maintenance/namespaces/{ns}/checkpoints?limit=100&cursor=...` |
| Delete a checkpoint | `delete_checkpoint` | `not_idempotent` | `DELETE /v0/maintenance/namespaces/{ns}/checkpoints/{checkpoint_id}` (deletes the pin; a missing id returns `checkpoint_not_found`; other owners are rejected) |
| Run one maintenance job | `run_maintenance` | `not_idempotent` | `POST /v0/maintenance/namespaces/{ns}/runs`; the body names one job with `kind` |
| Search file contents | `grep` | `idempotent` | `GET /v0/namespaces/{ns}/grep?pattern=needle&case_insensitive=false&path_prefix=%2Fsrc&allow_scan=false&allow_stale=false&limit=100&cursor=...`; requires the `query.grep` feature and an active index |
| Read grep index status | `get_grep_index` | `idempotent` | `GET /v0/maintenance/namespaces/{ns}/grep/index` |
| Enable the grep index | `enable_grep_index` | `idempotent` | `POST /v0/maintenance/namespaces/{ns}/grep/index/enable`; idempotent |
| Disable the grep index | `disable_grep_index` | `idempotent` | `POST /v0/maintenance/namespaces/{ns}/grep/index/disable`; idempotent |
| Test object storage | `probe_store` | `not_idempotent` | `POST /v0/maintenance/store/probe` with body `{}` |
| Scrape metrics | `get_metrics` | `idempotent` | `GET /metrics` (Prometheus text exposition; authorized, unlike the liveness routes — see below) |

Grep index GC reads durable roots on each call and completes one pass over
the namespace's grep manifests and segments. It runs through `run_maintenance`
with request body `{"kind":"grep_gc"}`.
Its response carries `namespace_id`, `deleted_segments`,
`deleted_other_objects`, `namespace_reaped`, and `retained_candidates`.
Unreadable or invalid roots fail before deletion. A tombstoned or absent
namespace has its aged grep prefix reaped. The retention and age rules are
in [the grep format](format.md#appendix-d-grep-extension-format).

The status, enable, and disable routes all return one flat grep index object:
`namespace_id`, lifecycle fields tagged by `status`, `next_run_no`, and
`reorganize_pending`. Lifecycle statuses never share a sequence field:

| `status` | Carries | Means |
| --- | --- | --- |
| `disabled` | — | No index is maintained here. Also the answer for a namespace that never enabled one. |
| `backfilling` | `captured_seq`, `cursor_inode_id`, `checkpoint_id` | The initial walk over a pinned checkpoint is running. `captured_seq` is the namespace sequence that checkpoint captured; reaching it completes the backfill. Nothing is searchable yet, and no watermark exists to report. |
| `active` | `built_through_seq`, `next_event_index` | The index follows the change feed. Commits at or below `built_through_seq` are searchable, except that a non-zero `next_event_index` leaves the rest of that one commit unindexed. |

For example:

```json
{"namespace_id":"demo","status":"active","built_through_seq":12,"next_run_no":3,"reorganize_pending":false}
```

```json
{"namespace_id":"demo","status":"backfilling","captured_seq":12,"cursor_inode_id":"ino_4","checkpoint_id":"pin_00000000000000000009-0000000000000009","next_run_no":1,"reorganize_pending":false}
```

A backfill therefore never reports a `built_through_seq`, and an active index
never reports a `captured_seq`. `next_run_no` is the run number the index
allocates next, while `reorganize_pending` reports whether a partitioned
segment reorganization is in progress. A client waiting for the index to catch up
captures one sequence before it starts waiting and stops there, rather than
chasing a head that keeps moving.

A maintenance run body names exactly one job with `kind`:

| `kind` | Fields | Result |
| --- | --- | --- |
| `metadata` | Optional `max_wal_tail_segments` | `wal_flush` and `reorganize` outcomes |
| `metadata_compaction` | None | `compaction`, tagged by `outcome`; a published outcome includes the manifest number and row, byte, and segment counts |
| `gc` | Optional `grace_window_ms` | The collection result |
| `grep_gc` | None | `deleted_segments`, `deleted_other_objects`, `namespace_reaped`, and `retained_candidates` |
| `retention` | None | `retention_floor_seq` |
| `recover_administrator` | `principal_id` | `commit_id`, `committed_seq`, and the root row's new `access_revision_no`. Grants `admin` on the root row to the principal and keeps every other root grant, through a commit that checks no subject and is attributed to `Loonfs-Actor`. Use it when an ACL namespace has lost every administrator. |

The response carries the same `kind`, the addressed `namespace_id`, and that
job's result. None of the jobs creates a pin.

```json
{"kind":"gc"}
```

Races and supersessions are outcomes, not errors.

A deleted namespace accepts only a run with `kind` set to `gc` or `grep_gc`; other jobs return `namespace_deleted`. Retirement follows [format section 9.5](format.md#95-retirement).

`wal_flush.outcome` has four values. `not_needed` means the WAL tail was below the threshold. `flushed` means this step published the next current manifest. `already_published` means the current manifest already covered the captured WAL tail, so this step published no manifest. `retries_exhausted` means concurrent updates prevented every attempt from publishing; nothing was flushed, and a later step can try again.

`reorganize.outcome` has five values. `not_needed` means no bounded merge is
due. `unit_published` means this run published one bounded merge.
`compaction_required` means a family group needs streaming compaction; run the
`metadata_compaction` job. `manifest_advanced` means another publisher changed the
current manifest first. Segments this run wrote remain unreferenced, and a
later GC pass can delete them. `fenced` means a newer runtime holds the
compactor epoch.

A `metadata_compaction` run compacts one unit: a bounded merge when the
selected window fits one step, or otherwise one streaming compaction of a
family group. Repeat the run while it publishes to compact every eligible
group.

`compaction.outcome` has six values. `not_needed` means no family group has
eligible input. `bounded_merge_published` means the planner selected and
published a bounded merge. `published` reports the manifest number and row,
byte, and segment counts. `cancelled` means the caller cancelled the job.
`abandoned` means an input run changed, the elapsed-time bound was exceeded,
or all publication attempts lost. `fenced` means another process advanced
the manifest's compactor epoch. These last three outcomes publish no manifest.

For `metadata`, `max_wal_tail_segments` overrides the flush threshold. Zero and values above the write-rejection threshold return `invalid_request`. Replay history is retained unless the request uses `kind: "retention"`. For `gc`, `grace_window_ms` overrides the grace window. A grace window below the derived safety floor returns `invalid_request`. Upload sessions keep their leases and completed content keeps its derived reclamation grace ([format: upload cleanup](format.md#116-upload-session-cleanup)).

Responses contain counts for that call. Concurrent calls can overlap deletion
attempts, so these counts are operational summaries. No collection state is
saved between calls.

Each GC request runs one complete stateless pass.
`GcRequest` accepts `grace_window_ms`. It has no cursor.
Nothing sweeps unless `gc` is present.

The retention floor bounds incremental replay only. File revision history
is never pruned: a revisions listing is always complete, however far the
floor has advanced.

#### Checkpoint inventory

A checkpoint name is a label, not a key. Every create call generates a new record, so the same name may identify multiple checkpoints. Creation first folds any WAL tail after the current manifest, then pins the resulting manifest. Create and list use one checkpoint object with `namespace_id`, `checkpoint_id`, `owner`, `created_at_ms`, optional `expires_at_ms`, `captured_seq`, and `manifest_no`. Create returns this object directly. For API-created checkpoints, `owner` is `user` with the requested `name`, and `created_at_ms` is the durable record timestamp.

The id is `pin_{manifest_no:020}-{16 lowercase hex}`. It identifies the
manifest used by checkpoint and snapshot reads. Every pin has a fresh id.

For example, a create response is:

```json
{"namespace_id":"demo","checkpoint_id":"pin_00000000000000000009-0000000000000009","owner":{"kind":"user","name":"release"},"created_at_ms":1752623000000,"expires_at_ms":1752626600000,"captured_seq":12,"manifest_no":9}
```

`GET /v0/maintenance/namespaces/{ns}/checkpoints?limit=100&cursor=...` returns existing
checkpoints in ascending `checkpoint_id` order. Each entry is the same
checkpoint object returned by create. User checkpoints can be deleted by
id. Fork checkpoints retain their `fork` owner and remain while their target
namespace still reads through them.

```json
{"namespace_id":"demo","checkpoints":[{"namespace_id":"demo","checkpoint_id":"pin_00000000000000000009-0000000000000009","owner":{"kind":"user","name":"release"},"created_at_ms":1752623000000,"expires_at_ms":1752626600000,"captured_seq":12,"manifest_no":9}]}
```

Deletion removes the pin and returns the addressed namespace and checkpoint.
The success response is:

```json
{"namespace_id":"demo","checkpoint_id":"pin_00000000000000000009-0000000000000009"}
```

A missing id, including one already deleted, returns `checkpoint_not_found`.

`limit` follows the advertised pagination limits. `next_cursor` is omitted
after the final page. Cursors are opaque and tied to this namespace and
operation; clients should only return them unchanged. A namespace-scoped cursor from another namespace returns `invalid_request`.

This is a live listing, not a snapshot. Checkpoints created, deleted, or
collected while a client is paging can affect later pages.

Deletion removes the record. An expired user pin remains listed and readable
until GC deletes it after expiry plus grace. A permanent user pin on a live
namespace requires explicit deletion. [Format section 8.1](format.md#81-records-and-owners)
defines reads and removal for each pin owner.

#### Snapshots

A snapshot is a time-bounded view of a namespace. Its pin id is
its `snapshot_id`. The `captured_seq` is the namespace sequence the snapshot captured.
Creation requires `name` and `ttl_ms`. The ttl cannot exceed
`snapshot.max_ttl_ms` or `snapshot.max_lifetime_ms`.

An extension measures its requested ttl from the server's current time. It
never moves the expiry past `snapshot.max_lifetime_ms` from the record's
`created_at_ms`. A namespace may hold at most
`snapshot.max_live_per_namespace` live snapshots.

Snapshot listing returns only snapshot-owned pins whose lifetimes have not
expired. The maintenance checkpoint listing keeps expired records visible until
collection deletes them. Snapshot deletion removes the pin. A second delete returns
`snapshot_not_found`.

In an ACL namespace, creating, listing, extending, and deleting snapshots requires an administrator subject; a request with no subject context acts as the token holder. Snapshot reads require `read` and `history` on the historical inode, evaluated at the current head.

These operations manage the snapshot lifetime. Path stat, inode stat, path directory listing,
inode children listing, file content, download, and change-feed requests accept an optional
`snapshot_id`; the download request carries it in its body. File content and download requests cannot combine `snapshot_id`
with `revision_no`; the snapshot selects the revision. A snapshot change feed
ends at the captured sequence, and `after_seq` cannot exceed that sequence.

When an embedded read sets `snapshot_id` in its options, the read pins that
snapshot, as the HTTP request does. A reader that is already pinned at a
snapshot accepts options that name that snapshot and rejects any other
snapshot id as `invalid_request`.

Snapshot reads require a live snapshot. Missing snapshots return
`snapshot_not_found`, including after deletion. Expired snapshots return
`snapshot_gone` while their pins still exist. Neither case falls back to the current namespace state.

#### Store contract probe

The store probe proves the configured object store honours the provider
contract this format depends on — create-if-absent, compare-and-swap,
read-after-write visibility, prefix listing, ranged reads — and reports what
it found check by check. It runs only when an operator asks: a probe writes
and deletes objects, so nothing runs one at startup or on a schedule.

Every object a run writes lives under `probe-runs/{run_id}/`, which is not a
durable object family, so garbage collection never enumerates it and no
namespace state can be reached from it. The run's last check deletes those
objects and proves the prefix empty, so a probe that completes leaves
nothing behind; one that dies partway leaves orphans under a prefix nothing
consults.

The response carries the `run_id` the server minted and one entry per check,
in the order the checks ran. Each entry names the check and its `outcome`:

| `outcome` | Means |
| --- | --- |
| `passed` | The store behaved as the contract requires. |
| `unsupported` | The store declares it cannot do this at all. Only the optional capabilities answer this way, and a deployment that needs neither is unaffected. |
| `failed` | The store did something the contract forbids, or the operation failed outright. The entry's `message` says what was expected and what happened instead. |

A failed check is reported in the body, not as an error: a probe that ran to
completion answers 200 whatever it found, because the operator asked a
question and the answer is that the store is wrong. Only an unauthorized
request or a malformed body answers in the error envelope. One check ending
does not end the run, so one probe answers the whole question rather than
the first thing that went wrong.

A probe never decides whether a deployment may serve presigned direct
uploads. That trust comes from the endpoint allowlist behind the
`uploads.direct_put` capability, because a probe exercises the server's own
request path and never a presigned capability handed to a client.

Routes under `/v0/maintenance/` belong to the `maintenance/v0` API group. `GET /v0/namespaces/{ns}/grep` belongs to `query/v0`. Everything else shown belongs to `filesystem/v0`.

A GC response includes `next_reclamation_at_ms` when a deleted namespace is inside its retirement grace, a retained user or snapshot pin has a future deletion time, or an upload session has a future cleanup time. It is the earliest of those future times examined by the pass. Fork pins carry no cleanup time. Upload cleanup times include lease plus grace, abort grace, and completed-content grace. Candidates that age out by provider timestamps carry no time here. Absence does not mean that nothing remains to collect.

`reclaimable_at_ms` is the deadline defined in [format section 9.5](format.md#95-retirement) when the current manifest is deleted, including when pins still block reclamation. It is absent for an active namespace.

Every call reads the current manifest and uses one fixed clock. It keeps its live set in memory and writes no collection progress. Every family lists from the beginning and sweeps to the end. Collection roots follow [format section 11.2](format.md#112-reference-roots). Manifest read failures fail the call before sweeping.

A GC response groups related counts. `deleted` contains `wal_segments`,
`metadata_segments`, `manifests`, `upload_sessions`, `content_objects`,
and `retired_content_objects`. `deleted_checkpoints_by_owner` contains `user`, `snapshot`, and `fork` counts for pins deleted in the pass.
Their sum is the total number of pins deleted. Each deletion
is counted once. A target's deletion of its source pin contributes to `fork`
when the pin was present before deletion. Repeating that deletion on an
absent pin adds no count. Every count field is present, including zero values.

`content_objects` counts reclamation through completed upload sessions.
`retired_content_objects` counts keys listed under the deleted namespace’s
content prefix and successfully deleted. With no new objects, a repeat content
sweep makes one empty LIST, no DELETE, and reports zero. This count contributes
to maintenance progress.

Every core GC response carries `retained`, the candidates the pass kept, split by
the decision that spared each one. The reasons are a closed
set, so every field is always present and a zero means nothing was kept for
that reason, and the fields sum to the total:

| Reason | Means |
| --- | --- |
| `referenced` | Protected by current roots or needed for forward manifest discovery. |
| `within_grace_window` | Unreachable, but younger than `grace_window_ms` by the object's own provider timestamp. |
| `no_provider_timestamp` | Unreachable, and the provider reported no last-modified time, so the object's age is unknown and it is treated as young. |
| `unrecognized_key` | A key under a swept family that this collector does not recognize as one of its own. Never deleted, whatever its age. |
| `checkpoint_not_deletable` | A pin retained by its owner or its grace window. |
| `upload_session_window` | An upload session waiting out a window a clock resolves — the same waits `next_reclamation_at_ms` reports. |
| `upload_session_undecided` | An upload session held for a reason no clock resolves: a lost compare-and-swap, a record that vanished mid-pass, or a content cleanup failure. |

Retention is counted per candidate examined, not per object in the
namespace, so one object two passes both examine is counted by each.

Candidate and age rules follow [format section 11.3](format.md#113-candidate-and-age-rules).

#### Deleting, retaining, and reclaiming

An ordinary operation that observes namespace deletion returns `namespace_deleted`; cached readers follow [format section 9.4](format.md#94-deleting-a-namespace). Collection is asynchronous, with no fixed completion time or guarantee of physical erasure.

Retirement eligibility and content reclamation follow [format sections 9.5](format.md#95-retirement) and [11.8](format.md#118-sweeping-a-retired-owners-content). Purging the tombstone and hint is outside this API.

Retention is coarse: a deleted ancestor keeps every object it
published while a live descendant still depends on it. GC does not select
individual published objects within a namespace. Deleting a file or tree in an active
namespace also does not reclaim its published content, because LoonFS retains
every file revision.

The metadata retention floor is separate. It limits WAL replay history and
makes older WAL segments eligible for GC. Advance it explicitly with
`POST .../runs` and body `{"kind":"retention"}`, or
`loonfs maintenance retention advance`. It does not remove file revisions.
Completed upload sessions in active namespaces use the derived content
reclamation grace, slightly longer than seven days, before GC can reclaim
staged content that no retained revision references.

Run GC repeatedly, including after a pass finds nothing left to delete. An
already-issued upload capability can write an object after deletion, and a
late write missed by one listing is found by the next run. Continued late
writes, grace windows, dependent forks, pin cleanup, and maintenance
not running can all delay complete reclamation. A deleted manifest refuses commits, upload capabilities, and upload completion.
It does not revoke capabilities already issued. The retirement grace exceeds
the presigned URL lifetime.

A successful delete schedules GC in the attached in-process runner for the
deletion time plus the retirement grace and GC safety margin. The runtime also
schedules future work from `next_reclamation_at_ms`. Deleted namespaces need
not stay assigned to an operator loop forever. LoonFS does not enumerate
namespaces for maintenance, and hints do not survive restart. Use
`loonfs maintenance loop --namespaces <id>` for inactive namespaces and as a
backstop after restart or a missed hint. The command runs until stopped, or
performs one bounded pass with `--drain`. Retirement also prompts the runner
to schedule GC for a fork's source. A missed prompt delays reclamation.

A maintenance job that cannot start because the process is shutting down releases its claim without running. Shutdown waits for every job that did start.

Keep the provider's lifecycle rule for incomplete multipart uploads. Provider
upload state can exist outside object listings, so deleting a namespace's
content objects does not replace session abort and provider cleanup. Deleting a key does not erase
physical versions retained by bucket versioning or retention locks.

Use `retained` to understand what a pass kept. Inspect checkpoint blockers
through `GET /v0/maintenance/namespaces/{ns}/checkpoints`.

#### Service-proxied upload

`service_proxied` is the default and needs no capability from the provider:
the client `PUT`s its bytes to `/uploads/{upload_id}/content` and the server
writes them to object storage.

The server streams the body to object storage without buffering the complete
file. While streaming, it counts the bytes and computes SHA-256. A body larger
than `upload.service_proxied.max_content_bytes` fails with `content_too_large`. The resulting
content reference stores the server-computed SHA-256 in `checksum`.

#### Direct single-PUT upload

For `direct_put`, the server chooses the object identity and returns a
presigned upload capability with the provider's stored checksum algorithm.
The client counts and hashes the bytes while sending them, then supplies the
content facts at completion.

```json
{
  "mode": "direct_put",
  "size_bytes": 1234
}
```

`size_bytes` is optional and advisory. When present, the server compares it
with `upload.direct_put.max_content_bytes`, the provider's single-request
ceiling, and answers `content_too_large` before issuing a capability when it
is too large. The provider enforces the same limit when accepting the PUT.
Larger content uses `direct_multipart` when available.

The response includes only a short-lived transfer capability, never raw object-store credentials or a caller-managed object key. Required headers are provider-issued and must be echoed by the client; for example, an S3-compatible deployment may return:

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

The signed headers are part of the transfer capability. In the S3-compatible
example, `if-none-match: *` keeps the immutable object create-only. A client
cannot drop or edit that requirement without invalidating the capability.
Arbitrary S3-compatible gateways are unproven because HMAC interoperability
does not prove that the gateway enforces create-only requests or reports the
stored checksum. The feature key is absent on unproven endpoints, and
beginning `direct_put` answers `not_supported` with
`feature = "filesystem.uploads.direct_put"`. The server-mediated upload path remains
available and is the default.

The reference server offers `direct_put` only where its adapter can presign a
create-only request, read back the stored checksum, and has passed the live
provider suite. AWS S3 and Cloudflare R2 qualify through SigV4 on their own
domain families. Google Cloud Storage qualifies through its native
`GOOG4-RSA-SHA256` API, not through the S3-interoperability surface that did
not preserve preconditions. Custom S3-compatible endpoints, Azure Blob
Storage, and the local filesystem are not offered `direct_put`, and there is
no configuration override. Other implementations may use different headers
or decline `direct_put` support.

The response names the provider's stored checksum algorithm. S3-family
issuers return `crc64nvme`; the GCS issuer returns `crc32c`. The client
calculates that checksum while sending the bytes.

`direct_put` and `direct_multipart` are separate offers, and a deployment may
advertise the first and not the second — the reference server's Google Cloud
Storage adapter is built that way, signing whole-object writes and reads while
implementing no multipart signing. A client's transport ladder falls from
parts to one whole-object write before it falls back to the proxy. A source
whose length is unknown can take the whole-object write and report its exact
size after the one-pass transfer.

A browser calling a presigned URL is talking to the provider, not to LoonFS,
so cross-origin access is governed by the bucket's or container's own CORS
configuration rather than by anything this API sets.

After uploading to the presigned URL, the client completes the session with
the same content-claim grammar used by direct multipart:

```json
{
  "mode": "direct_put",
  "content": {
    "size_bytes": 1234,
    "checksum": { "algorithm": "crc64nvme", "value": "<16 lowercase hex>" }
  }
}
```

The checksum algorithm must match the session's `checksum_algorithm`; a
difference answers `invalid_request`. The server builds the final content
reference from the session's namespace id and content id, plus the completion
claim. The session's namespace is the owner. It
then compares the claimed size and checksum with the object in storage. A
mismatch makes the session unusable, and the server deletes the unpublished
object. Completion verifies the stored content against the client's claim; it
does not reapply the provider's upload limit.
If the provider metadata request fails, the server returns `server_error`
without changing the object or session, so the client can retry. The server
does not download the object during this check.

#### Direct multipart upload

`direct_multipart` uploads large objects directly to object storage in
parallel. The server opens the provider upload, signs each part, and completes
the upload without receiving the file bytes.

A begin request sets only the part size. It does not include the total size or
complete-object checksum:

```json
{
  "mode": "direct_multipart",
  "part_size_bytes": 8388608
}
```

`part_size_bytes` defaults to 8 MiB and must be between 5 MiB and 5 GiB. A
session supports at most 10,000 parts, so larger objects require larger parts.
A client that does not know the total size can request parts until its stream
ends.

Direct PUT and multipart uploads provide the complete checksum at completion.
Multipart also provides one checksum per part before each part is signed.
Both transports support one-pass uploads and streams whose total size is
initially unknown.

The response records the part size and checksum algorithm for the session:

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_...",
  "mode": "direct_multipart",
  "part_size_bytes": 8388608,
  "checksum_algorithm": "crc64nvme"
}
```

The response has no content reference because the complete size and checksum
are not known yet. A client that knows its size can calculate the part count
from `part_size_bytes`.

**Parts.** `POST /uploads/{upload_id}/parts` accepts a list of
`{part_number, checksum}` values and returns one presigned capability for each
part. Every checksum must use the session's `checksum_algorithm`. Parts may be
uploaded in parallel. To retry a part, request another capability and upload
that part again; the provider keeps the latest upload.

The server does not store part progress. The client must keep each part number,
etag, and checksum until completion.

**Completion.** The completion request includes the complete size and checksum
plus every uploaded part. In v0, multipart sessions use CRC-64/NVME. The
resulting content reference stores that complete-object checksum.

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

The request lists every part once in ascending order. The server returns the
content reference after completion. Service-proxied completion contains only
`{"mode":"service_proxied"}`. Direct PUT completion includes the final size
and checksum, as shown in the previous section.

The server asks the provider to assemble the object, then reads its stored size
and checksum and compares them with the completion request. This read is
required because providers do not handle an incorrect assembled checksum in
the same way.

If the stored values do not match, the server aborts the session, deletes the
object, and returns failure. The client must start a new session. If assembly
or the metadata read fails before a comparison can be made, the server returns
`server_error` and keeps the session open so completion can be retried.

If completion fails without a clear response, resend the same completion
request:

- a completed session returns its stored result and a fresh `content_token`
  while the minting window remains open;
- an open session continues completion;
- an aborted session is terminal; do not retry.

A caller that cannot resend the same request reads the upload status instead;
a completed status returns the same stored result.

When an `open` multipart upload no longer exists at the provider, the server
checks whether the completed object matches the request. A match completes the
session. If the upload and a matching object are both missing, the server
aborts the session and returns an error.

**Cleanup.** The session record carries the provider's upload id, so a
session that is aborted — by the client, by a failed verification, or by
upload garbage collection after its lease passes — abandons the provider's
upload along with the object it was writing. Aborting an upload that already
assembled its object is safe on every supported provider: it succeeds and
leaves the object alone.

A content reference contains `kind`, `owner_namespace_id`, `content_id`, `size_bytes`, and `checksum`. The owner names the namespace that originally wrote the bytes. Clients echo the complete reference unchanged. The owner does not change on a fork or restore.

A server may return a short-lived `content_token` for completed content.
Clients treat the token as opaque and can copy it directly into a commit
request. Reading a completed session returns a fresh token while its minting
window remains open. The separate `content_ref` remains available afterward.

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_...",
  "content_ref": { "kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "sha256", "value": "..." } }
}
```

Path-oriented `put_file` operations then reference the completed `content_ref`.
The client includes the matching `content_token` in `content_tokens` unchanged;
the server verifies it before admission and publication checks the resulting
in-memory proof's binding and deadline. A missing or expired proof answers
`content_not_prepared` without reading the content object. A malformed or
expired token that names the put's ref also answers `content_not_prepared`;
tokens naming other refs are ignored.

`Loonfs-Actor: document-importer`

```json
{
  "commit_id": "commit-a",
  "content_tokens": [
    {
      "content_ref": { "kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "sha256", "value": "..." } },
      "token": "opaque-server-token"
    }
  ],
  "operations": [
    {
      "kind": "put_file",
      "path": "/docs/report.pdf",
      "content_ref": { "kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "sha256", "value": "..." } },
      "behavior": "no_replace"
    }
  ]
}
```

Long-running transfers may additionally expose session resources.
Implementations may also expose workflow helper resources, but those helpers
are outside the core semantics. Once a multi-request interaction begins, the
server-issued identifier is the stable in-flight identifier of that
interaction.

Namespace creation uses the namespace id directly. v0 has no namespace aliases
or separate display names. Representative request:

`Loonfs-Actor: usr_8f3c`

```json
{
  "namespace_id": "demo",
  "access": {"kind":"unrestricted"}
}
```

The response contains the new namespace's initial state. A new namespace
starts at sequence 0 with a retention floor of 0:

```json
{
  "namespace_id": "demo",
  "created_at_ms": 1752623000000,
  "created_by": "usr_8f3c",
  "access": {"kind":"unrestricted"},
  "head_seq": 0,
  "retention_floor_seq": 0
}
```

Fork creation uses `new_namespace_id` for the target namespace. Route placeholders
such as `{ns}`, `{source_ns}`, or an implementation-internal `:namespace` are
only path parameter names for the same namespace id value; v0 does not accept
or emit a namespace `name` alias.

Create and fork install hint and manifest 1 in order. The conditional put of manifest 1 decides existence ([format: namespace lifecycle](format.md#9-namespace-lifecycle-and-forks)). A create or fork that loses that write to another active namespace answers `namespace_exists` (409). A create or fork into a deleted id answers `namespace_deleted` (410) before writing anything. There is no partially created namespace.

Namespace lifetime follows [format section 1.1](format.md#11-namespaces-and-identity). Applications that reuse a human name must keep their own name-to-id map and mint a fresh namespace id for each lifetime.

A new request after a lost creation acknowledgement returns
`namespace_exists`, unless it explicitly allows an existing namespace.
Within the original attempt, an exact manifest read-back after an unknown
transport outcome confirms a fork because its source pin is unique. For a
plain create, matching bytes answer `namespace_exists`, or the
existing summary when the caller allows an existing namespace, because they
do not prove which caller created the namespace.

The examples below are representative, not exhaustive. Responses may gain
fields within v0; clients must ignore JSON fields they do not recognize.
Optional response fields are omitted when absent, never encoded as `null`.

### 6.1 `GET /v0/capabilities`

The capability document of section 2.1.

### 6.2 `GET /v0/namespaces/{ns}`

This operation returns one namespace without listing every namespace. A
missing namespace returns `404` with `namespace_not_found`. A deleted
namespace returns `410` with `namespace_deleted`.

```json
{
  "namespace_id": "demo",
  "access": {"kind": "unrestricted"},
  "created_at_ms": 1752623000000,
  "created_by": "usr_8f3c",
  "head_seq": 418,
  "retention_floor_seq": 120
}
```

The `Namespace` object has exactly these fields:

| Field | Meaning |
| --- | --- |
| `namespace_id` | Durable namespace id. |
| `access` | Access mode: `{"kind": "unrestricted"}` or `{"kind": "acl", "principal_scope": "..."}`. |
| `created_at_ms` | Time the namespace was created, in Unix milliseconds. |
| `created_by` | Actor that created or forked the namespace, as supplied by the application. |
| `fork_basis` | Present only for a fork. Contains `source_namespace_id` and the captured `source_head_seq`. |
| `head_seq` | Current visible namespace sequence. |
| `retention_floor_seq` | Oldest sequence still promised for incremental replay. |

The create request carries `access` with the same shape plus `root_grants` for the `acl` kind, defaulting to unrestricted, and an ACL namespace needs at least one administrator in `root_grants`.

Namespace status derives the live sequence from the manifest and numbered
WAL tip. A cold read follows a lagging hint by probing forward. A missing
hint reads as an absent namespace. A cached runtime probes the next WAL
number with GET, applies any new segments, and continues until 404. Commits
are visible on the next read even when the hint has not been raised. The
acknowledging runtime seeds its read caches from the publication. Its next
read probes the next WAL number like any warm read. If the manifest and WAL
tip are unchanged and the seeded projection is retained, it replays nothing
and does not reload the manifest.

`RuntimeCacheConfig::manifest_revalidation_interval_ms` sets the minimum
monotonic interval between checks for a successor to the cached manifest.
It also paces the writer's hint raise after publication. It defaults to 1000 milliseconds; `0` checks on every read. A read after the
interval probes the successor manifest with HEAD as well as the next WAL
number, so an unchanged warm head costs two requests; within the interval
it costs one. A present successor reloads the namespace. Warm readers
observe deletion, retention floor advances, and new manifests on this
interval. Handle builders accept an injected monotonic timer for
deterministic runtime timing.

The maintenance endpoint `GET /v0/maintenance/namespaces/{ns}/diagnostics` returns the
namespace state plus storage details used by maintenance:

| Field | Meaning |
| --- | --- |
| `namespace_id` | Durable namespace id. |
| `created_at_ms` | Time the namespace was created, in Unix milliseconds. |
| `created_by` | Actor that created or forked the namespace, as supplied by the application. |
| `fork_basis` | Present only for a fork. Contains `source_namespace_id` and the captured `source_head_seq`. |
| `head_seq` | Current visible namespace sequence. |
| `retention_floor_seq` | Oldest sequence still promised for incremental replay. |
| `current_manifest_no` | Current manifest number, present from namespace creation. |
| `wal_tail_segments` | WAL tip minus the current manifest's folded number, including fences. |
| `live_snapshots` | Number of snapshots that had not expired when diagnostics began. |
| `live_checkpoints` | Number of active user checkpoints, including expired records awaiting collection. |

```json
{
  "namespace_id": "demo",
  "created_at_ms": 1752623000000,
  "created_by": "usr_8f3c",
  "head_seq": 418,
  "retention_floor_seq": 120,
  "current_manifest_no": 410,
  "wal_tail_segments": 3,
  "live_snapshots": 2,
  "live_checkpoints": 4
}
```

#### Namespace statistics

The embedded `load_namespace_statistics` and `load_checkpoint_statistics` loaders return `NamespaceStatistics` through one manifest’s folded head. Newer WAL commits are excluded. The observation includes the manifest reference, creation time, lifecycle status, folded WAL number, activity counters, referenced inode and metadata totals, and any fork basis. Counters start at zero and comparisons hold within the same namespace. Activity counters are integers from zero through 9007199254740991. Their definitions and the footprint calculations are in [format section 7.4](format.md#74-statistics).

### 6.3 `DELETE /v0/namespaces/{ns}`

In an ACL namespace this operation requires an administrator subject; a request
with no subject headers acts as the token holder.

Deletion is a fenced manifest publication ([format: namespace deletion](format.md#94-deleting-a-namespace)). It linearizes at the manifest put: commits acknowledged before it stay committed. Once they observe deletion, reads, commits, forks from the id, status, another deletion, and create or fork into the id fail with `namespace_deleted` (410). The tombstone remains the current manifest after content reclamation.

Checkpoint listing and user-checkpoint deletion are explicit exceptions. They
remain available because permanent user pins must stay discoverable and
releasable after deletion. Releasing a fork-owned checkpoint remains rejected.

Deletion itself reclaims nothing. Retirement eligibility follows [format section 9.5](format.md#95-retirement), retained roots follow [section 11.2](format.md#112-reference-roots), and the content sweep follows [section 11.8](format.md#118-sweeping-a-retired-owners-content).

Run GC repeatedly to catch late writes and keep the provider's incomplete
multipart-upload lifecycle rule. Deleting an object key does not erase
physical versions under bucket versioning or retention locks.

The optional `expected_head_seq` query parameter deletes only if the head is
still at that sequence, failing with `stale_head` otherwise — the same
precondition pattern used for file mutations. That rejection
reports both sequences, in the message and as `expected_head_seq` and
`actual_head_seq` details, so a caller that still means to delete can retry
against the sequence it found.

```json
{
  "namespace_id": "demo",
  "head_seq": 418
}
```

A commit can succeed without its acknowledgement reaching the caller. Deletion does not retroactively change that outcome. The reference server resolves
its queue in admission order — requests admitted before the delete publish first, requests admitted
after it fail with `namespace_deleted`, and nothing is rejected for a delete
that ends up failing its precondition.

### 6.4 `GET /filesystem/entry` and `GET /inodes/{inode_id}`

The response is one authoritative path entry. Enum values are snake_case per
the durable naming rules ([format: field conventions](format.md#121-field-conventions)).

```json
{
  "namespace_id": "demo",
  "path": "/docs/report.txt",
  "inode_id": "ino_42",
  "created_by": "usr_8f3c",
  "created_at_ms": 1752623000000,
  "inode_kind": "file",
  "head_seq": 418,
  "parent_inode_id": "ino_7",
  "display_name": "report.txt",
  "binding_version": "opaque-token",
  "revision_no": 7,
  "revision_committed_by": "render-worker",
  "size_bytes": 19482,
  "content_ref": {
    "kind": "blob_v1",
    "owner_namespace_id": "demo",
    "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
    "size_bytes": 19482,
    "checksum": { "algorithm": "sha256", "value": "42d..." }
  },
  "revision_committed_at_ms": 1752624000000,
  "attributes_revision_no": 3,
  "attributes_updated_by": "metadata-editor",
  "attributes_updated_at_ms": 1752623500000,
  "attributes": { "owner": "platform" }
}
```

Every entry carries `created_by` and `created_at_ms` from its inode row. File
entries additionally carry `revision_committed_by` and `revision_committed_at_ms` from their
current revision row. These stamps are observational — sequences are the
order, and no validity rule reads them. Directories have creation time but no
modified time in v0; rename and move change neither attribution nor time.

`include_attributes` selects whether the response includes the inode's attributes. It accepts `true` or `false`; anything else is `invalid_request`. The entry route defaults to `true` because it returns one bounded attribute map of at most 64 KiB.

The projection serializes as prefixed siblings. `attributes_revision_no` and
the complete `attributes` map are present together or absent together;
`attributes_updated_by` and `attributes_updated_at_ms` are also present when a
persisted attributes revision exists. The revision number is read
independently — clients feed it to `expected_attributes_revision_no` on the
next write without touching the values — which is the prefixed-sibling case,
not the consumed-as-a-unit case used by the entry-kind enum. An empty map is a
real projected answer at its current revision, and an inode that has never had
attributes written reads as `{}` at revision 0 with no updater or update time.
A read that did not include attributes omits all four siblings, so an absent
projection never means "no attributes".

The namespace root is nameless, so its entry omits `parent_inode_id`, `display_name`, and `binding_version`. Every other entry includes a validated `display_name` and a `binding_version` for its current parent/name binding (section 5.1). The empty string is not a valid name for the root or any named path component.

The inode route accepts `snapshot_id` and returns the same entry shape, including
the `path` at the selected sequence.
Renaming an entry changes its path and name but not its inode id or metadata.
An unknown or hidden inode returns `inode_not_found`. The root inode returns
`/`. `include_attributes` behaves the same as it does for the path entry route.

### 6.5 `GET /filesystem/entries` and `GET /inodes/{inode_id}/children`

The envelope names the listing target and the head the listing was read from, so an empty directory still reports which state it observed and the response can grow without reshaping `entries`. The path route names its target as `path`; the inode route names it as `parent_inode_id`. Entries are full path entries with the same shape returned by `GET /filesystem/entry` (directory entries omit file-only fields).

Directory listing advances in canonical `name_key` order. Concatenating pages
in cursor order yields the complete listing in that same order; clients must
not re-sort aggregated pages. The listing target is required on every page —
the `path` query parameter on the path route, the `inode_id` route segment on
the inode route; the cursor carries the resume position, but the request
target remains the authority for what is being listed. Responses include
`next_cursor` only when another page is available.

The inode route accepts `snapshot_id` and addresses the directory by its stable
identity instead of a name, so a listing and its resumption stay on the same directory across
concurrent renames or moves of the parent; entry paths reflect the parent's
location at each page's head. An unknown or hidden target inode answers `inode_not_found`, and a
file target answers `path_conflict`: an inode-addressed caller asked for
children, never for the entry itself, so there is no single-entry file
listing on this route. A directory deleted mid-listing answers
`inode_not_found` on the resumed page.

`include_attributes` works exactly as it does on the entry route and obeys the same required-siblings-together projection rule, but it defaults to `false`. This keeps the default response bounded because a page may contain up to `pagination.max_limit` entries and each attribute map may be 64 KiB. Clients that request attributes should choose a suitable page size.

A cursor is normally an opaque ordering resume, not a snapshot pin. Directory
listing, revision listing, grep, and change-feed cursors tolerate forward head
drift: commits landing mid-listing never retire them, and the resumed page
evaluates at the then-current head, continuing strictly after the last returned
position. Each page is internally consistent at its own head, but a multi-page
listing spans whatever heads its pages ran at: an entry created behind the
resume position is missed, an entry deleted behind it was already returned,
and a rename can surface as a duplicate or a miss. A client that needs one
consistent cut re-issues the listing when `head_seq` changes between pages.
Only a cursor minted ahead of the serving head answers `rebootstrap_required`
— drift tolerance runs forward, never backward. (A malformed cursor, or one
replayed against a different target, stays `invalid_request`.)

A directory cursor minted with `snapshot_id` is the exception: it binds to
that snapshot's immutable view. The client must repeat the same `snapshot_id`
on every page. Omitting it or supplying another snapshot returns
`invalid_request` instead of combining rows from different views.
The CLI reports the last page's head as `head_seq` and, for multi-page and
recursive reads, reports the first and last observed heads as `head_drift`
when they differ; a caller that needs a settled tree re-lists until the
observed heads stop moving.
An unrecognized cursor version is also rejected as `invalid_request`.

```json
{
  "namespace_id": "demo",
  "path": "/docs",
  "head_seq": 418,
  "entries": [
    {
      "namespace_id": "demo",
      "path": "/docs/report.txt",
      "inode_id": "ino_42",
      "created_by": "usr_8f3c",
      "created_at_ms": 1752623000000,
      "inode_kind": "file",
      "head_seq": 418,
      "parent_inode_id": "ino_7",
      "display_name": "report.txt",
      "revision_no": 7,
      "revision_committed_by": "render-worker",
      "revision_committed_at_ms": 1752624000000,
      "size_bytes": 19482,
      "content_ref": {
        "kind": "blob_v1",
        "owner_namespace_id": "demo",
        "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
        "size_bytes": 19482,
        "checksum": { "algorithm": "sha256", "value": "42d..." }
      }
    },
    {
      "namespace_id": "demo",
      "path": "/docs/slides",
      "inode_id": "ino_43",
      "created_by": "usr_8f3c",
      "created_at_ms": 1752623000000,
      "inode_kind": "dir",
      "head_seq": 418,
      "parent_inode_id": "ino_7",
      "display_name": "slides"
    }
  ],
  "next_cursor": "7b2e2e2e7d"
}
```

### 6.6 `GET /filesystem/trash`

In an ACL namespace a page may return fewer entries than `limit` and still carry `next_cursor`.

Lists the namespace's recoverable deletions, oldest deletion first — ascending
by `(deletion_seq, inode_id)` — paged with the standard `limit`/`cursor`
pattern (the cursor is an ordering resume like every other). The listing is a
range scan over the derived active-deletions family ([format: file deletion](format.md#16-file-and-subtree-deletion)), so a page costs the page rather than the namespace's deletion history.
Those rows represent current state and are not removed when the retention floor advances. Each entry includes the inode id and deletion sequence required by `undelete`, plus `inode_kind`, `deleted_by`, `deleted_at_ms`, and the removed `deleted_binding`. Nested deletions remain separate entries, and recovering an outer deletion does not remove an inner deletion from the list.

```json
{
  "namespace_id": "demo",
  "head_seq": 418,
  "entries": [
    {
      "inode_id": "ino_42",
      "inode_kind": "file",
      "deletion_seq": 417,
      "deleted_at_ms": 1752625000000,
      "deleted_by": "usr_8f3c",
      "deleted_binding": {
        "parent_inode_id": "ino_7",
        "name_key": "report.txt",
        "display_name": "report.txt"
      }
    }
  ]
}
```

### 6.7 `GET /filesystem/content`

The response body is the authoritative file bytes. Metadata may be exposed in
headers, but the body itself is raw content rather than JSON.

The server streams and verifies content in bounded chunks. A file past
`download.service_proxied.max_content_bytes` answers `content_too_large` before content bytes
are fetched; clients may use a download grant when direct GET is advertised.
The limit is a transfer policy, not an allocation size.

A successful end of the response body means length and checksum verification
completed. The server omits `Content-Length` so receiving the expected byte
count alone cannot signal success before verification. An I/O or verification
failure after headers aborts the body instead of returning a JSON error.
Consumers must require successful stream completion; file downloads should
publish their temporary output only after that completion. Streaming to stdout
can expose a partial or unverified prefix before a later error.

Revision listings return newest revisions first and use the standard
`limit`/`cursor` pattern. The path route resolves the current inode first; the
inode route addresses it directly. Both return the same path-free response.
`next_cursor` is included only when another page is available. Revision
history is not pruned, so paging to the end reaches revision 1.

The inode content route reads and verifies a revision without resolving a
current path. Deleted files remain readable while their revision rows are
retained. A directory returns `path_conflict`, an unknown inode returns
`inode_not_found`, and an unknown revision returns `revision_not_found`.

Embedded by-reference reads require administrator access to the reading
namespace. The namespace's pinned view must contain a publication for the
content, including publications inherited through a fork. A reference absent
from that view returns `path_not_found` without reading content bytes, even
when the reading namespace owns it. Content published after a snapshot is
not readable through that snapshot.

```json
{
  "namespace_id": "demo",
  "inode_id": "ino_42",
  "head_seq": 418,
  "revisions": [
    {
      "inode_id": "ino_42",
      "revision_no": 7,
      "committed_seq": 418,
      "committed_at_ms": 1752624000000,
      "committed_by": "render-worker",
      "content_ref": {
        "kind": "blob_v1",
        "owner_namespace_id": "demo",
        "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
        "size_bytes": 19482,
        "checksum": { "algorithm": "sha256", "value": "42d..." }
      }
    }
  ],
  "next_cursor": "7b2e2e2e7d"
}
```

### 6.8 `POST /commits`

This is the binding for the commit model in section 5.1. The `Loonfs-Actor`
header is required. The body contains one `commit_id`, an optional `message`,
optional `preconditions`, and `operations` — an ordered,
non-empty array of path operations. An empty array is `invalid_request`.

The root path `/` is readable but, with one exception, never a mutation
target: `update_access` replaces its access row. Any other operation that
names it — as its own path, or as either end of a move or copy — is
`invalid_request`. The rejection belongs to the operation rather than to the
request, so it is attributed like every other failure: inside a batch it
names its position, and it is decided against a namespace that exists, so a
root mutation sent to an unknown namespace answers `namespace_not_found`
first.

Representative request:

`Loonfs-Actor: usr_8f3c`

```json
{
  "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
  "preconditions": [
    { "kind": "namespace_head", "expected_head_seq": 42 },
    { "kind": "file_revision", "inode_id": "ino_7", "expected_revision_no": 3 }
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

A one-operation request is the one-element case of this shape, not a
different request: a convenience call and a batch produce the same commit
and the same fingerprint, so a commit id used by either replays against the
other.

The operations commit together, in order, as one logical commit: either
every operation commits or none does. Operation `k` sees authoritative
namespace state plus everything operations `0..k` do, so one request can
create a directory and write into it:

`Loonfs-Actor: usr_8f3c`

```json
{
  "commit_id": "c_2a41d0c6b9f34e7d8a1b5c9e0f234567",
  "message": "import the January report",
  "content_tokens": [
    {
      "content_ref": { "kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "sha256", "value": "..." } },
      "token": "opaque-server-token"
    }
  ],
  "operations": [
    { "kind": "create_directory", "path": "/reports/2026" },
    {
      "kind": "put_file",
      "path": "/reports/2026/january.pdf",
      "content_ref": { "kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "sha256", "value": "..." } },
      "behavior": "no_replace"
    },
    {
      "kind": "delete_path",
      "path": "/inbox/january.pdf"
    }
  ]
}
```

The first operation that fails aborts the whole request — nothing it or its
predecessors would have written becomes visible — and the error names the
position that stopped it. Had the put above raced another writer:

```json
{
  "code": "path_conflict",
  "message": "destination `/reports/2026/january.pdf` already exists",
  "request_id": "req_9c2f4a1b7d8e4f21a0b3c4d5e6f70819",
  "details": {
    "commit_id": "c_2a41d0c6b9f34e7d8a1b5c9e0f234567",
    "operation_index": 1
  }
}
```

**File content.** These three operations require either `content_ref` for
uploaded content or `inline_content` for bytes included in the commit request:

| Operation | Other fields (`?` means optional) |
| --- | --- |
| `put_file` | `path`, `behavior?`, `expected_inode_id?`, `expected_revision_no?` |
| `create_file_by_inode` | `parent_inode_id`, `display_name` |
| `put_file_revision_by_inode` | `inode_id`, `expected_revision_no` |

Supplying both content fields, or neither, returns `invalid_request` before
commit planning. Unknown fields are also rejected.

For uploaded content, include the proof in `content_tokens`. One token covers
every operation that uses its `content_ref`; tokens for unused references are
ignored.

For inline content, encode the complete file as a standard padded base64 JSON
string. No content token is needed. For example, this request writes
`hello\n` to `/hello.txt`:

`Loonfs-Actor: usr_8f3c`

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

An empty string writes an empty file. The content ID is assigned by the server,
and the size and SHA-256 checksum are computed from the decoded bytes.

Inline writes are enabled by default for files up to 64 KiB. The configured
limit is advertised as `commit.max_inline_content_bytes_per_operation`, alongside
`filesystem.commits.inline_content`. If inline writes are disabled, both keys
are absent. An inline request then returns `not_supported` with `feature` set
to `filesystem.commits.inline_content`.

The size limit applies to each file before base64 encoding. A larger value
returns `invalid_request` before any storage write. The complete JSON body,
including base64 content and all other fields, must also fit
`commit.max_request_body_bytes`, 2 MiB by default.

Some inline files may be uploaded as content objects before the commit to
meet the configured WAL limits. All operations still commit together. Retry
the same request with the same inline bytes and commit ID, even if those bytes
were stored separately. The retry rules in section 5.2 apply.

Move and copy accept the same `behavior` choice as put: `no_replace` (the
default) fails when the destination is occupied, and `replace` replaces a
file destination. A replacing move deletes the destination file and rebinds
the source in one commit; a replacing copy appends a revision to the
destination inode, keeping its identity and revision history. Only a file
destination can be replaced, and a path never replaces itself.

Replacing puts can include `expected_inode_id` alone or pair it with
`expected_revision_no`. Replacing moves and copies use
`expected_destination_inode_id` alone or pair it with
`expected_destination_revision_no`. A destination or path revision precondition
requires its matching inode precondition because a revision identifies a version
within one inode, not the inode occupying a path. A revision-only precondition
returns `invalid_request`.

Preconditions require `replace` behavior. An inode mismatch returns `path_conflict`,
a revision mismatch returns `stale_revision`, and a missing destination
returns `path_not_found`.

These preconditions check content revisions only. A put with preconditions or replacing copy
proceeds after an attribute-only update when the inode and content revision
still match. A replacing move deletes the destination inode, including its
attributes. Attribute-level concurrency uses
`expected_attributes_revision_no` separately.

Five operations use inode IDs instead of paths. They let clients act on an entry they previously read even if its path has changed. An unknown or hidden inode returns `inode_not_found`.

`create_directory_by_inode` and `create_file_by_inode` create an entry under an existing parent directory. Both are create-only and return `path_conflict` when the name is already in use.

`put_file_revision_by_inode` appends a revision to a file wherever it is currently located. It requires `expected_revision_no` and returns `stale_revision` when the file has changed.

`move_by_inode` and `delete_by_inode` require `expected_binding_version` (section 5.1). Their destination, replacement, and recursive-delete behavior matches `move_path` and `delete_path`. The namespace root cannot be moved or deleted.

`Loonfs-Actor: usr_8f3c`

```json
{
  "commit_id": "c_1b2c3d4e5f60718293a4b5c6d7e8f901",
  "operations": [
    {
      "kind": "create_directory_by_inode",
      "parent_inode_id": "ino_1",
      "display_name": "reports"
    },
    {
      "kind": "move_by_inode",
      "inode_id": "ino_42",
      "expected_binding_version": "opaque-token",
      "destination_parent_inode_id": "ino_12",
      "destination_display_name": "january.pdf",
      "behavior": "replace"
    }
  ]
}
```

A successful response is returned only after the underlying change is actually
committed: the numbered WAL put succeeded. Every
commit returns the same `Commit` object (section 5.2).

Representative response:

```json
{
  "namespace_id": "demo",
  "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
  "committed_seq": 419,
  "committed_by": "usr_8f3c",
  "committed_at_ms": 1752624000000,
  "message": "import the January report",
  "events": [
    {
      "kind": "file_created",
      "inode_id": "ino_43",
      "parent_inode_id": "ino_12",
      "display_name": "january.pdf",
      "revision_no": 1,
      "content_ref": { "kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "sha256", "value": "..." } }
    }
  ]
}
```

The same endpoint also accepts path directory creation:

`Loonfs-Actor: usr_8f3c`

```json
{
  "commit_id": "c_8b7d4ef098ec4c1fbde15edbe02f9a64",
  "operations": [{ "kind": "create_directory", "path": "/docs" }]
}
```

and path revision restore:

`Loonfs-Actor: usr_8f3c`

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

and undelete, which recovers a deleted file or subtree by re-binding the
deletion's root inode — the inode's identity and retained revision history
come back with it. The request names both halves of the recovery handle the
delete reported (and the change feed carries): the inode id and the
deletion's committed sequence.

`Loonfs-Actor: usr_8f3c`

```json
{
  "commit_id": "c_5d6e7f8091a2b3c4d5e6f70812345678",
  "operations": [
    {
      "kind": "undelete",
      "inode_id": "ino_42",
      "deletion_seq": 17,
      "destination_path": "/docs/report.txt"
    }
  ]
}
```

`destination_path` is optional. When present, it is the destination: its parent must
exist and be visible, and its name must be free. When absent, the entry
restores in place — it re-binds under the parent inode and name its
deletion recorded, anchored on the parent's identity rather than a
remembered spelling, so recovery lands correctly even when the enclosing
directories were renamed after the delete. The in-place parent and name obey
the same rules a path would: the parent must not be deleted, and the name must
be free, each answering its usual code otherwise.

Only the root of a deletion can be undeleted, and `deletion_seq` must match the active deletion sequence. A mismatch returns `not_deleted` with the expected and actual sequences, preventing a stale recovery request from cancelling a later deletion.

and `update_attributes`, which writes and removes attributes on the inode a
path resolves to:

`Loonfs-Actor: usr_8f3c`

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
      "expected_inode_id": "ino_7",
      "expected_attributes_revision_no": 3
    }
  ]
}
```

An attribute map is a map from validated keys to plain UTF-8 strings. Values
have no kind envelope or built-in list shape; a caller that needs a list
chooses its own string encoding. The empty string is a stored value, not a
tombstone. Only naming a key in `remove` deletes it.

Every size is counted in logical UTF-8 bytes, without JSON or durable-encoding
framing:

| Constant | Value | Bound |
| --- | --- | --- |
| `MAX_ATTRIBUTE_KEY_BYTES` | 128 | Longest attribute key. |
| `MAX_ATTRIBUTE_VALUE_BYTES` | 4,096 | Longest attribute value. |
| `MAX_ATTRIBUTE_ENTRIES` | 100 | Most entries in one map. |
| `MAX_ATTRIBUTES_TOTAL_BYTES` | 65,536 | Largest map, counting every key's bytes plus every value's bytes. |

`set` writes each key over whatever the inode currently holds under it, and
leaves keys it does not name alone. `remove` drops each key it names. Both
default to empty, but a request that names neither answers `invalid_request`:
there is nothing to apply. So do a key that appears in both, a key repeated
in `remove`, and any key under the reserved `loonfs.` prefix, which is
system-owned and not a caller's to write.

An accepted update advances the attribute revision even when the resulting
map is unchanged. The revision counts accepted updates, matching puts of
identical content. The update writes the complete resulting map and produces
an attributes event in the change feed. Replaying the same request with the
same `commit_id` returns its original commit without advancing the revision
again; a new `commit_id` is a new update.

The resulting map is checked against every limit in the format spec,
so a small write that pushes an already-large map over a cap is rejected for
the map it would produce.

The target is any visible file or directory: attributes belong to the
resource. They travel with inode identity, so a move, a rename, a new file
revision, and a revision restore all leave them unchanged, a delete keeps them
while it hides the inode, and an undelete gives back the same map at the same
revision. A copy to a vacant destination is the one operation that carries
them: the new inode starts with the source's map at attribute revision 1, as
its own event in the change feed. A copy over an existing file changes that
file's content and nothing else.

`expected_inode_id` and `expected_attributes_revision_no` are both optional
preconditions, and both are part of the commit's semantic identity for commit-id
reuse. An attribute revision precondition requires its matching inode precondition
because a revision identifies a version of one inode; a revision-only precondition
returns `invalid_request` before resolving the path. A wrong `expected_inode_id`
answers `path_conflict`, like the delete precondition it mirrors, so a raced
rebinding cannot land attributes on the wrong inode. A stale
`expected_attributes_revision_no` answers `stale_attributes`. Omitting the
revision precondition does not make the write a merge: every update carries the
revision it read as its own precondition, so a concurrent update still answers
`stale_attributes` with the expected and actual revisions in the details.

`update_access` replaces the access row of the inode a path resolves to.
The root path is a valid target; its row is where administrators are named.

`Loonfs-Actor: usr_8f3c`

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
      "expected_inode_id": "ino_9",
      "expected_access_revision_no": 2
    }
  ]
}
```

`boundary` and `grants` are both required and replace the row whole: an
update that names no principals clears every direct grant, and one with
`boundary` false resumes inheritance. Grants are a map from principal id to
a list of distinct right names; the format specification gives the rights,
the size limits, and the rule that no entry has an empty list. A repeated
principal key or right name answers `invalid_request`. Two rules
answer `invalid_request`: `admin` is valid only on the root inode, and a
boundary applies only to a directory.

An unrestricted namespace holds no access rows and answers
`namespace_unrestricted`. Every accepted update advances the inode's access
revision and produces an `access_changed` event carrying the complete new
state.

`expected_inode_id` and `expected_access_revision_no` follow the attribute
rules exactly: both are part of the commit's semantic identity, the revision
precondition requires its inode precondition, a wrong inode answers
`path_conflict`, and a stale revision answers `stale_access` whether the
caller stated the revision or the update's own precondition observed it.

#### Authorization in ACL namespaces

Each operation checks the subject's effective rights on the inodes it touches
before its conflict checks. If the subject holds no right on a checked inode,
the response is `path_not_found` or `inode_not_found`, according to how the
request named it. If it holds some right but lacks a required right, the
response is `forbidden` with the checked `inode_id` in the error details.

| Operation | Required rights |
| --- | --- |
| create_directory, new put_file | `create` on the parent, or the deepest existing directory when allocating parents. |
| create_directory_by_inode, create_file_by_inode | `create` on the parent. |
| Replacing put_file, put_file_revision_by_inode | `write` on the existing file. |
| put_file with no_replace at an occupied name | `create` on the parent before reporting the conflict. |
| delete_path, delete_by_inode | `remove` on the source parent. |
| move_path, move_by_inode | `remove` on the source parent and `create` on the destination parent; replacing an occupant also requires `remove` on the destination parent. |
| copy_path | `read` on the source file; `create` on the destination parent for a vacant name or a name conflict, or `write` on an occupied file being replaced. |
| restore_revision | `write` and `history` on the file. |
| update_attributes | `write` on the target inode. |
| undelete | `remove` on the saved parent and `create` on the recovery parent, including when recovering in place. |
| update_access | Authority for the changes, as described below. |

A move that changes any principal's effective rights on the moved inode is
authorized as if the mover had granted the gained rights. The mover must hold
`share` and every gained right, or `manage`, or be an administrator. Only gains
count; removing access needs no extra authority. Entry rights suffice when
nothing is conferred. A boundary folder can move anywhere with entry rights,
because its inherited rights do not change. Root administrator principals do
not contribute gains. Relocating an undelete follows the same rule.

For `update_access`, changing the root's set of administrator principals
requires an administrator. Otherwise `manage` permits any update. A subject
without `manage` must hold `share`, leave the boundary unchanged, and hold every
right it adds or removes from any principal's direct grant.

Preconditions require `read` on the inode they name, or the existing parent for
path absence; a namespace-head precondition needs no inode right.

The not-found-versus-forbidden rule also applies to reads: a subject with no
right on the target receives `path_not_found` or `inode_not_found`; a subject
with some right but without a required right receives `forbidden`.

| Read operation | Required rights |
| --- | --- |
| Path or inode stat, directory listing, current content, current download | `read` on the target inode. |
| Older content revision or historical download, by path or inode | `read` and `history` on the target inode. |
| Revision listing, by path or inode | `read` and `history` on the target inode. |
| Snapshot stat, listing, content, or download | `read` and `history` on the historical inode, evaluated at the current head. |
| Trash listing | `read` on each deletion's saved original parent. |
| Batch file resolution and grep candidates | `read` on the candidate inode; snapshot resolution also requires `history`. |
| Change feed or bare content reference read | Administrator. |
| Bare content reference import | Administrator on the reference's owner namespace. |

Names belong to the directory. A subject with `read` on a directory sees every
child in a listing. A child the subject cannot read keeps its entry fields but
omits its `attributes` projection, even when attributes were requested.
Snapshot reads evaluate rights at the current head on the historical inode
resolved by the snapshot, never on today's occupant of the path. Every
snapshot read of an inode also requires `history`.

The trash listing filters entries by whether the subject can read their saved
original parent. A filtered page may be short and still carry `next_cursor`.
Grep filters candidates the subject cannot read before reading their content;
those candidates still count against the page's candidate budget.

The change feed and bare content reference reads and imports require an
administrator when a subject is supplied. An import checks the reference's
owner namespace, including when the owner is also the destination or has been
deleted. With no subject headers, these surfaces act as the token holder; every
per-subject read surface instead answers `invalid_request` naming
`Loonfs-Principals`. Checkpoints and maintenance continue to read as the
service.

### 6.9 Upload transport

The upload transport standardizes staged content publication, not one specific
byte path. In v0, uploads are whole-file uploads: the staged body is the
complete file content, not a separate metadata document or multipart strategy.

A session in an ACL namespace belongs to the subject that opened it. Every
upload operation requires the namespace's scope and principals; any other
subject receives `upload_not_found`. An unrestricted namespace records no
subject ownership.

The semantic rule is:

- `PUT /content` stores the immutable whole-file object and records the staged
  `content_ref`;
- `complete` verifies the upload and records its final `content_ref`; and
- the returned completed session's `content_ref` is then safe to reference
  from a commit. Remote servers may also return an opaque `content_token` that
  remote create/replace mutations carry back as their content-preparation
  proof.

Begin requests (`CreateUploadBody`) use `mode` to select the upload transport. A request may include
only the fields for that mode:

```json
{ "mode": "service_proxied" }
{ "mode": "direct_put", "size_bytes": 1234 }
{ "mode": "direct_multipart", "part_size_bytes": 8388608 }
```

Mode-specific fields are placed beside `mode`. `service_proxied` has no additional fields. `direct_put` accepts an optional size hint. `direct_multipart` accepts an optional `part_size_bytes` and uses the default when omitted.

Completion requests (`CompleteUploadBody`) use the same `mode` values as begin requests:

```json
{ "mode": "service_proxied" }
{ "mode": "direct_put", "content": { "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "<16 lowercase hex>" } } }
{ "mode": "direct_multipart", "content": { "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "<16 lowercase hex>" } }, "parts": [] }
```

The request mode must match the upload session. Direct PUT requires `content`.
Multipart requires `content` and `parts`. Service-proxied completion has no
additional fields. Unknown fields, missing fields, and mode mismatches return
`invalid_request`.

Every upload step (`create_upload`, `put_upload_content`, `get_upload`,
`complete_upload`, and `abort_upload`) returns the session object. An open
session carries its mode's fields beside `mode`, `status`, and `expires_at_ms`.
Both `direct_put` and `direct_multipart` carry `checksum_algorithm`.
`direct_multipart` also carries `part_size_bytes`. `direct_put` carries `access`,
a write capability minted fresh at creation and on every read of the open
session. A `service_proxied` session carries `content_ref` after bytes have
been staged. `PUT .../content` returns that open session with the staged
`content_ref`. Fields that do not apply are omitted. Response readers accept
unknown fields.

An upload session allocates its content object when it begins, so repeating
`PUT /content` with the same bytes for the same upload id writes the same
object and is idempotent. Repeating it with different bytes is a conflict.
Two *different* sessions carrying identical bytes get their own objects:
content is never shared across uploads, so retry idempotency belongs to the
session and nothing else. Completing a service-proxied upload fails if no
content was staged. Publication never downloads an arbitrary external ref to
rescue a missing proof.

A session is `open`, then `completed` or `aborted`, and both of those are
final ([format: upload sessions](format.md#51-upload-sessions)). What that means at the API:

- `GET /uploads/{upload_id}`, `POST /uploads/{upload_id}/complete`, and
  `POST /uploads/{upload_id}/abort` all return one flat upload-session object.
  Its `mode` is the transport chosen when the session began, and `status` is
  `open`, `completed`, or `aborted`. The status-specific fields are siblings
  of that tag rather than a nested object:

  ```json
  { "namespace_id": "demo", "upload_id": "upl_...", "mode": "direct_multipart", "status": "open", "expires_at_ms": 1730000000000, "checksum_algorithm": "crc64nvme", "part_size_bytes": 8388608 }
  { "namespace_id": "demo", "upload_id": "upl_...", "mode": "direct_put", "status": "completed", "completed_at_ms": 1730000001000, "content_ref": { "kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_...", "size_bytes": 1234, "checksum": { "algorithm": "sha256", "value": "<64 hex>" } }, "content_token": { "content_ref": { "kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_...", "size_bytes": 1234, "checksum": { "algorithm": "sha256", "value": "<64 hex>" } }, "token": "<opaque>" } }
  { "namespace_id": "demo", "upload_id": "upl_...", "mode": "service_proxied", "status": "aborted", "aborted_at_ms": 1730000002000 }
  ```

  A completed status read supplies a **freshly minted** `content_token` while
  its minting window remains open. The field is absent after that window.
  Completion returns the `completed_at_ms` stored by the terminal session
  transition; an idempotent replay returns that same stored timestamp.
- `POST /uploads/{upload_id}/abort` ends an open session and deletes the
  object it was writing. Repeating it succeeds and reports the abort that
  stands, including the original stored `aborted_at_ms`. A completed session
  is refused with `upload_already_completed`, because its content may already
  be published.
- An aborted session reports `upload_not_found` from `PUT /content` and from
  `complete` — the same stable surface as the physical absence that follows
  it. This is also what a completion sees when server-side cleanup aborted
  the session first; if the completion lands first instead, the cleanup's
  conditional write fails and the completed session is retained.

**The one-pass client obligation.** A client uploads a large payload by
reading its source once, forward, and never holding it whole. Nothing in the
transport requires more than that, and every part of the flow is arranged so
it does not have to:

1. Begin a `direct_multipart` session. It declares no length and no digest,
   so a source that cannot state its length starts uploading immediately.
   The response settles the part geometry and nothing else.
2. Read the source part by part. For each part, compute its `crc64nvme` for
   the URL the server signs, and fold the same bytes into a running
   `crc64nvme` over the whole object. One pass produces both.
3. Ask for part URLs in waves rather than all at once — a signing request
   names at most 1,000 parts — upload the wave's parts, and record each
   part's `{part_number, etag, crc64nvme}`. Part bookkeeping is the
   client's, exactly as it is in the provider's own multipart API.
4. Hold no more than a bounded window of parts at a time. The window is the
   memory the upload costs, and it does not depend on the payload's length.
   A part that fails is retried by re-asking for its URL: a repeated part is
   last-write-wins at the provider.
5. Complete with the part list plus the `{size_bytes, crc64nvme}` this pass
   discovered. The claim arrives here because this is the first moment a
   one-pass uploader can produce it.

A session whose upload fails partway is aborted so its incomplete object can
be deleted.

Two bounds are worth planning for. A provider assembles at most 10,000
parts, so a session carries at most `part_size_bytes × 10_000` bytes and a
longer payload is refused when it asks to authorize the part past that
ceiling — a client that knows its payload is very large asks for a larger
part size at begin.

**Choosing a transport.** The file helpers first retain small content inline
when supported, as described in section 5.2. Content that requires an upload
and is smaller than one part goes to `PUT /content` if it fits the proxy limit.
Above that, a client works down the transports its deployment advertises:

1. `direct_multipart`, where advertised. Parts win because each is retried on
   its own and nothing has to know the payload's length in advance.
2. `direct_put`, where advertised and `size_bytes` is at most
   `upload.direct_put.max_content_bytes`. This is the rung a provider that
   can sign a write but has no multipart API to open offers. It is the one
   transport that sends one whole object directly. The client counts and
   hashes the bytes while sending them, then reports both values at completion.
   The source is read once and does not have to be held in memory.
3. `PUT /content` as a streaming request body, where `size_bytes` is at most
   `upload.service_proxied.max_content_bytes`. The server hashes the payload as it forwards
   it on. A body whose length is unknown is sent with chunked transfer
   encoding, and the server's incremental accounting is what bounds it.

Every rung with a known size is judged against the advertised limits, never
against an assumed one: a payload under one part is not thereby known to fit
`upload.service_proxied.max_content_bytes`, since a deployment may set that cap anywhere. A
payload that none of the three can carry should be refused by the client,
naming the limits it passed, rather than sent into the proxy to be refused
there.

**A declared length is only a hint.** File metadata and other size hints may be
stale, so the client does not reject an upload based only on a hint. Direct PUT
counts the bytes while sending them and reports the measured size at
completion. The server compares that size with the provider limit and the
stored object. A source with no size hint can use direct multipart or direct
PUT; both determine the final size while uploading.

**Receipt expiry and re-minting.** The `content_token` is the
upload's receipt: it is minted only from a session the store already says is
completed, it names one `{namespace, content_ref}` pair, and
it is short-lived — a commit is expected to follow the upload promptly, and a
rejected receipt is not an error the client has to plan around. Durability
lives in the session, not in the receipt: reading the session mints another
one for bytes that never move again, so **a lost commit response costs one
request, never a retransfer**. Re-minting stops a fixed window after
completion, after which the status read reports the session without a token;
by then the content is either referenced by committed metadata, which
protects it on its own, or reclaimable ([format: upload cleanup](format.md#116-upload-session-cleanup)). A client that receives an expired or otherwise rejected receipt
re-reads the session and commits again with the fresh one.

Representative begin-upload response:

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_4d8f2c91a7b34e0f9c6d1a2b3e5f708c",
  "mode": "service_proxied",
  "status": "open",
  "expires_at_ms": 1730000000000
}
```

Representative content-upload response:

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_4d8f2c91a7b34e0f9c6d1a2b3e5f708c",
  "mode": "service_proxied",
  "status": "open",
  "expires_at_ms": 1730000000000,
  "content_ref": {
    "kind": "blob_v1",
    "owner_namespace_id": "demo",
    "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
    "size_bytes": 20591,
    "checksum": { "algorithm": "sha256", "value": "7ab..." }
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
  "upload_id": "upl_4d8f2c91a7b34e0f9c6d1a2b3e5f708c",
  "mode": "service_proxied",
  "status": "completed",
  "completed_at_ms": 1730000001000,
  "content_ref": {
    "kind": "blob_v1",
    "owner_namespace_id": "demo",
    "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
    "size_bytes": 20591,
    "checksum": { "algorithm": "sha256", "value": "7ab..." }
  },
  "content_token": {
    "content_ref": {
      "kind": "blob_v1",
      "owner_namespace_id": "demo",
      "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
      "size_bytes": 20591,
      "checksum": { "algorithm": "sha256", "value": "7ab..." }
    },
    "token": "opaque-server-token"
  }
}
```

### 6.10 Download transport

A deployment must be able to serve back whatever it let a client create.
That is the whole rule, and it is why this exists: `direct_put` and
`direct_multipart` let a client write an object of any size, while a proxied
read enforces the transfer limit `download.service_proxied.max_content_bytes`. Direct reads
bypass that service limit and keep object traffic off the server. So
`filesystem.downloads.direct_get` is advertised by every deployment that offers
any direct write — the read is not a separate decision, and a deployment
that offers none of them cannot have created such a file in the first place.

A direct download requires a content object. If a file's bytes are still in the
WAL and no object exists yet, the server writes one before returning a download
URL. If the deployment cannot write that object, the request returns
`content_not_materialized`. Read through the proxied content route, which can
serve the WAL bytes directly, or retry after a fold writes the object.

`POST /v0/namespaces/{ns}/filesystem/downloads` takes a path and, optionally,
the revision to read in its JSON body:

```json
{ "path": "/docs/report.txt", "revision_no": 3 }
```

The body may instead include `snapshot_id` to read a snapshot; it cannot be combined with `revision_no`.

The response is a short-lived read capability plus everything the reader
checks the arriving bytes against:

```json
{
  "namespace_id": "demo",
  "path": "/docs/report.txt",
  "revision_no": 3,
  "content_ref": {
    "kind": "blob_v1",
    "owner_namespace_id": "demo",
    "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
    "size_bytes": 314572800,
    "checksum": { "algorithm": "sha256", "value": "42d..." }
  },
  "access": {
    "kind": "presigned_url",
    "method": "GET",
    "url": "https://bucket.s3.us-east-1.amazonaws.com/...&X-Amz-Signature=...",
    "expires_at_ms": 1780000000000
  }
}
```

The inode form is
`POST /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{revision_no}/downloads`.
The request has no body and its response does not include a path:

```json
{
  "namespace_id": "demo",
  "inode_id": "ino_42",
  "revision_no": 3,
  "content_ref": {
    "kind": "blob_v1",
    "owner_namespace_id": "demo",
    "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
    "size_bytes": 314572800,
    "checksum": { "algorithm": "sha256", "value": "42d..." }
  },
  "access": {
    "kind": "presigned_url",
    "method": "GET",
    "url": "https://bucket.s3.us-east-1.amazonaws.com/...&X-Amz-Signature=...",
    "expires_at_ms": 1780000000000
  }
}
```

Both download routes use the same provider support check and access format.
The inode route remains available after a rename or deletion while the
revision is retained.

Four properties follow from the shape, and clients may rely on all of them.

**The grant names one immutable object.** A commit that replaces the file
writes a new content object and leaves this one alone, so an issued
capability does not go stale when the path moves on, and it reads the
revision it was issued for rather than whatever is current when it is used.

**No headers are required, and `Range` is free.** The signed header set is
the host and nothing else, so a client may range, resume after a broken
connection, or fetch windows in parallel on the one URL without another round
trip to the server. A server implementation must not sign a `Range` header
into the capability; doing so would bind it to a single window.

**The client verifies the complete file.** It checks the byte length and
recomputes the algorithm in `content_ref.checksum`. This applies to SHA-256,
CRC-64/NVME, and CRC-32C, including downloads assembled from ranged or resumed
requests. A mismatch fails the download.

**The raw object key is never exposed.** A client learns a URL that expires,
the same way a `direct_put` client does.

The capability is short-lived — a transfer's worth of time, not a session's.
A reader that runs out of time asks for another grant, which costs one small
request and no retransfer. A deployment that cannot presign reads answers 501
`not_supported` with `feature = "filesystem.downloads.direct_get"`, and its proxied
read stays available under its own limit; because such a deployment cannot
presign writes either, no file it holds can be larger than it will proxy.

### 6.11 `GET /changes`

In an ACL namespace the change feed requires an administrator when subject headers are supplied; without them it reads as the token holder.

Each change is one commit carrying its identity (`committed_seq`, `commit_id`,
`committed_by`, observational `committed_at_ms`, optional `message`) and `events`:
the semantic filesystem operations the commit
applied, in the order it applied them. Each change is a `Commit`, the same object `POST /commits` returns. One request operation may apply
several — a put creates each missing parent directory, a replacing move
deletes the file it moves over, a copy carries the source's attributes onto
the inode it just created — so a request with three operations may report
more than three events. The events stay in request order.

```json
{
  "namespace_id": "demo",
  "after_seq": 418,
  "through_seq": 419,
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
          "inode_id": "ino_42",
          "revision_no": 8,
          "content_ref": {
            "kind": "blob_v1",
            "owner_namespace_id": "demo",
            "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
            "size_bytes": 20591,
            "checksum": { "algorithm": "sha256", "value": "7ab..." }
          }
        }
      ]
    }
  ]
}
```

Event kinds:

| Kind | Meaning | Fields |
| --- | --- | --- |
| `directory_created` | A directory was created. | `inode_id`, `parent_inode_id`, `display_name`, `binding_version`. |
| `file_created` | A file was created with its first revision. | `inode_id`, `parent_inode_id`, `display_name`, `binding_version`, `revision_no`, `content_ref`. |
| `content_changed` | A file received a new current revision — a replacing put or a revision restore (one durable fact for both). | `inode_id`, `revision_no`, `content_ref`. |
| `moved` | An entry moved to a new parent directory or name. | `inode_id`, `source_parent_inode_id`, `source_display_name`, `destination_parent_inode_id`, `destination_display_name`, `binding_version`. |
| `deleted` | A file or directory subtree was deleted. Use the enclosing `committed_seq` as `deletion_seq` when restoring it. | `inode_id`, plus `deleted_binding` containing `parent_inode_id`, `name_key`, and `display_name`. |
| `undeleted` | A deleted inode was recovered and re-bound. | `inode_id`, `parent_inode_id`, `display_name`, `binding_version`. |
| `attributes_changed` | An inode's attributes changed. `attributes` is the complete flat string map after the update, so a consumer projects it without reading anything back; an empty map is the cleared state. | `inode_id`, `attributes_revision_no`, `attributes`. |
| `access_changed` | An inode's access row was replaced. `grants` is the complete direct grant map after the update. | `inode_id`, `access_revision_no`, `boundary`, `grants`. |

Directory and file creation use separate event shapes. A file creation always
includes its first revision and content reference:

```json
{
  "kind": "directory_created",
  "inode_id": "ino_42",
  "parent_inode_id": "ino_1",
  "display_name": "docs",
  "binding_version": "opaque-token"
}

{
  "kind": "file_created",
  "inode_id": "ino_43",
  "parent_inode_id": "ino_1",
  "display_name": "report.txt",
  "binding_version": "opaque-token",
  "revision_no": 1,
  "content_ref": {
    "kind": "blob_v1",
    "owner_namespace_id": "demo",
    "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
    "size_bytes": 20591,
    "checksum": { "algorithm": "sha256", "value": "7ab..." }
  }
}
```

Events name inodes and their parent-directory bindings rather than full
paths; a consumer that needs paths can stat the inode or maintain its own
binding projection from this feed. Clients must ignore unknown event kinds
and unknown fields.

In a `moved` event, the source fields describe the removed binding and the
destination fields describe the new binding.

`directory_created`, `file_created`, `moved`, and `undeleted` include the `binding_version` they created. It matches later reads of the same binding. Other events do not create bindings and omit the field.

If `limit` truncates the page before the namespace head, the response includes
`next_after_seq` set to the last returned change's `committed_seq`. The client
resumes with `after_seq={next_after_seq}`.

`after_seq` may equal the current namespace head, which returns an empty page.

### 6.12 `POST /forks`

In an ACL namespace this operation requires an administrator subject; a request
with no subject headers acts as the token holder.

Representative request:

`Loonfs-Actor: usr_8f3c`

```json
{
  "new_namespace_id": "demo-branch"
}
```

Representative response:

```json
{
  "namespace_id": "demo-branch",
  "created_at_ms": 1752625000000,
  "created_by": "usr_8f3c",
  "access": {"kind":"unrestricted"},
  "fork_basis": {
    "source_namespace_id": "demo",
    "source_head_seq": 418
  },
  "head_seq": 418,
  "retention_floor_seq": 418
}
```

The `fork_basis` object identifies the captured source:

| Field | Meaning |
| --- | --- |
| `source_namespace_id` | Namespace the fork captured. |
| `source_head_seq` | Captured source sequence. |

The optional `snapshot_id` request field selects a live user snapshot of the
source namespace. Without it, the server captures the current head. The
snapshot must remain live through verification after the fork-owned checkpoint is written. Missing
snapshots and ids owned by another namespace or checkpoint kind return
`snapshot_not_found`. Expired snapshots and snapshots deleted during fork
verification return `snapshot_gone`.
Forking does not extend the snapshot, and later deletion does not affect the fork.
The fork records the `Loonfs-Actor` header as `created_by`, independently of the source's creator.
Namespace creation and forking produce no change-feed event.

The new namespace reads inherited content at each reference's owner key and starts its own
metadata history. The fork shares the source's existing content and
metadata objects without copying the filesystem. Forking the current head may first flush the
source's outstanding WAL tail, which writes that tail's inline content to content objects.
The fork creates a fork-owned source checkpoint so the
source-owned immutable metadata segments stay available for as long as the
target may still read them. It verifies the checkpoint, then installs the target namespace with a conditional manifest write. The manifest records the source checkpoint for the target's lifetime.

The response contains the new namespace's initial state. Its head sequence and
retention floor equal the captured basis sequence. The target's first data commit is one sequence above that head.

If the target ID is active, the server returns `namespace_exists` before writing a source pin. If it is deleted, the server returns `namespace_deleted` before writing a source pin. If the source checkpoint cannot be verified, the server returns `checkpoint_unavailable` and no target namespace is installed.

### 6.13 `GET /grep`

Grep filters candidates the subject cannot read before reading their content, and those candidates still count against the page budget.
A scan without the index skips every directory the subject cannot read; files under it are reachable only through the index.

Representative request:

```http
GET /grep?pattern=fn%20%28grep%7Csearch%29&case_insensitive=false&path_prefix=%2Fsrc&limit=100
```

Representative response:

```json
{
  "namespace_id": "demo",
  "head_seq": 418,
  "built_through_seq": 410,
  "tail_scanned": true,
  "matches": [
    {
      "path": "/src/search.rs",
      "inode_id": "ino_42",
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

The pattern uses the Rust `regex` crate's dialect (no backreferences or
lookaround), compiled line-anchored: `^` and `$` match line boundaries. The server plans required grams from the pattern, intersects
the namespace's grep index ([grep format](format.md#appendix-d-grep-extension-format)), scans
revisions committed after `built_through_seq` exhaustively, and verifies
every candidate against the real pattern, so index staleness affects cost,
never answers. Matches order by `(inode_id, byte_offset)` and one match is
reported per line. Two budgets bound a page: the match limit, and a
per-page verified-candidate budget, so a page may return fewer matches
than its limit and still carry a cursor. Each page is evaluated against
the namespace head at page time and reports it in `head_seq`; the cursor
resumes strictly after the last candidate the issuing page finished
scanning and is bound to that request — replaying it with different
criteria is rejected as `invalid_request`.

An active index remains enabled when its watermark is ahead of the reader's pinned head. The query pins the head again once and continues with the loaded index.

Grep cursors tolerate head drift with the same forward-only rule as every
other cursor in the API (section 6.5). A grep cursor minted at an older
head is accepted, and the resumed page evaluates at the then-current head
and reports it: each page is internally consistent at its own head, but a
multi-page search spans whatever heads its pages ran at, and candidates
the cursor has passed are not revisited even if later commits changed
them. A search is a bounded sampling read over content, not an
enumeration contract; a client that needs one consistent cut across pages
re-issues the search when `head_seq` changes between pages. A cursor from
a head newer than the serving view's is still rejected
(`rebootstrap_required`) — drift tolerance runs forward, never backward.

A pattern with no required
literal bytes is rejected with `query_unindexable` unless `allow_scan`
opts into a capped exhaustive scan. A tail past the scan budget is
rejected with `index_lagging` unless `allow_stale` accepts indexed-only
results (reported via `tail_scanned: false`); stale results are a
consistent cut at the index watermark — files whose newest revision
postdates it are omitted entirely rather than mixed in.

An undelete after the index watermark also returns `index_lagging` for an
exact query: the restored entry may be a directory whose descendants were
hidden from the checkpoint backfill, and the change event names only that
root. With `allow_stale`, the query serves indexed-only results and reports
`tail_scanned: false`. The worker starts a fresh checkpoint backfill
before advancing its watermark past the undelete, so a later exact query
includes the restored subtree.

The `path_prefix` value is a complete absolute path, not a partial textual
segment prefix. The server resolves it using the name-key folding rule
([format: names and paths](format.md#14-names-and-paths)), then limits results to descendants of that
inode. It must therefore
use the same canonical spelling as any other path. A scope that does not exist
answers `path_not_found`; an
empty existing scope answers successfully with no matches. A missing data half answers `not_supported` with the
`feature` field naming `query.grep`, the same key capability discovery
advertises the serving half under.

### 6.14 `GET /metrics`

An operational route, alongside the two liveness routes: `GET /health`
answers `ok` while the process is up, `GET /readiness` answers `ready`
while it still admits work, and this one reports what the process has been
doing. It answers Prometheus text exposition format 0.0.4 with
`Content-Type: text/plain; version=0.0.4`.

Unlike `/health` and `/readiness`, it requires the deployment's bearer
token. Those two say only whether the process is alive, which a load
balancer needs and nobody can misuse; this one describes a deployment's
traffic, its namespaces' shape of work, and its failure rates, which is
not public. A scrape sends the same `Authorization: Bearer` header a
client does, and an unauthorized one answers `401 unauthorized` inside the
standard error envelope.

Metric names are `loonfs_<subsystem>_<metric>`, covering the object-store
calls the process made, the maintenance passes it settled, the publications
it batched, what garbage collection reclaimed, and the requests this
server served. Label values come from closed vocabularies only: a request
is labeled by the route template it matched (`/v0/namespaces/{namespace_id}`),
never by its own path, so no namespace, upload, commit, or checkpoint id
ever appears in a label. Values are per process and reset when it
restarts, which is the ordinary contract for a counter a scraper reads.

The metric set is not part of the v0 wire contract: names may be added or
adjusted as the runtime's instrumentation grows, and clients must not
depend on any particular series existing.

## 7. Conformance requirements

### 7.1 Server requirements

A conforming server must:

1. treat object storage as the authoritative durable foundation;
2. publish visible metadata only through logical commits stored in visible
   numbered WAL objects;
3. make referenced content durable no later than the commit that references it ([format section 1.5](format.md#15-file-contents-and-ownership));
4. preserve `(namespace_id, inode_id)` as canonical item identity and the namespace lifetime in [format section 1.1](format.md#11-namespaces-and-identity);
5. resolve content through the reference's owner namespace and content ID;
6. implement tombstone-first delete;
7. serve replay from the highest numbered verified manifest found through
   `hint.json`, plus the numbered WAL objects after the folded boundary, replayed
   as logical commits; checkpoints pin manifest versions for retention, stable
   reads, restore, and forks;
8. fold sibling names into name keys by the v0 rule ([format: names and paths](format.md#14-names-and-paths));
9. keep control-plane sessions and any implementation-specific coordinators
   out of namespace history and the change feed;
10. preserve per-commit idempotency, ordering, and change-feed identity even
    when physically batching logical commits in a WAL segment;
11. advertise its API groups and features truthfully through the capability
    document, never advertising an API group whose required ops are not
    implemented; and
12. answer unimplemented or disabled surface area with `not_supported` (and
    its `feature` name), never with undefined behavior.

### 7.2 Writer and client requirements

A conforming writer or client must:

1. treat paths as selectors, not as durable identity;
2. upload content before asking the server to publish it, unless the commit carries the bytes inline;
3. use commit ids or equivalent idempotency keys for safe retry;
4. tolerate commit rejection when preconditions no longer hold;
5. re-bootstrap if its cursor falls behind the retention floor;
6. gate optional surface area on the capability document rather than probing,
   and treat `not_supported` as authoritative when the two disagree; and
7. ignore unknown JSON response fields, unknown error codes, and unknown
   feature keys.

A sync client must also maintain durable local state for its cursor and
reconciliation logic.

## 8. Client patterns

These patterns are defined by the surface a client uses, not by whether the
implementation is a CLI, desktop app, web app, SDK, or service. A single
client may implement more than one pattern. (These are usage patterns, not
the conformance API groups of section 1.)

### 8.1 Path-oriented client

This client uses the path-oriented surface.

Typical behavior:

- `ls`, `stat`, `get`, `put`, `mkdir`, `mv`, and `cp` use user-visible paths;
- the server remains authoritative for path resolution, canonical inode
  identity, and commit validation;
- small commands are often sessionless;
- large or recursive commands may be realized as sequences of ordinary
  logical commits.

This client does not require a sync database or full local mirror.
Implementations may still keep durable local state such as auth/session
state, retry journals, pinned snapshot ids, or inode context learned from
prior responses when that improves usability, restart safety, or
resumability.

The reference CLI loads defaults only when reading its config metadata returns a not-found error. Any other metadata error returns an invalid-config error naming the path and leaves the file unchanged.

### 8.2 Sync client

This client maintains durable local state and consumes the change feed over
time.

Typical behavior:

- maintains a durable cursor;
- projects remote state into local state;
- may upload content and publish commits of its own;
- preserves conflicts according to the client's conflict policy.

### 8.3 Operator or admin tool

This client uses low-level recovery or inspection surfaces that are specific
to an implementation or deployment.

## 9. Operation statefulness

This section defines when an operation is a single request, when it uses a
control object, and how responsibility splits between client and server for
the common filesystem commands.

One-shot operations are fully described by a single request and normally do
not require durable control-plane state. Long-running operations may span
multiple requests, may require a stable snapshot or destination binding, and
may need resumability across client or server restarts.

For the operations in this section, v0 defines upload sessions but not read
sessions.

### 9.1 Definitions

| Term | Meaning |
| --- | --- |
| **Single-request operation** | An operation that is fully described by a single request and does not require a server-side control object after the request completes. |
| **Control object** | Server-side state for an operation that spans multiple requests. A large or resumable `put <file>` may use one upload session. |
| **Implementation-specific coordinator** | A helper resource or service that correlates multiple logical commits for one higher-level workflow. Coordinators are outside the core interoperable model and do not define namespace history. |
| **Authoritative operation state** | The state that determines the correctness, visibility, and resumability of an operation. |
| **Transfer progress** | Non-authoritative progress information such as completed bytes, completed files, local temporary outputs, or user-interface counters. |

### 9.2 Normative rules

1. A LoonFS operation **MAY** be a single-request operation only when all of
   the following are true:
   - the operation is fully described by one request;
   - the operation completes synchronously;
   - no pinned read snapshot is required after the request returns;
   - no stable destination binding is required after the request returns; and
   - the request can be retried by replaying the full request.

2. A LoonFS operation **MAY** use a server-side control object when any of
   the following are true:
   - the client will continue the operation across multiple requests;
   - the operation requires a pinned snapshot for consistent reads;
   - the operation requires a stable destination binding across time;
   - the operation requires resumable multi-part upload; or
   - loss of server restart state would change correctness, retention
     safety, or promised resumability guarantees.

   A large or resumable `put <file>` that needs server-side state typically
   uses one upload session.

3. The core specification does **NOT** require a server-side job object for
   recursive `put` or recursive `cp`. Those workflows may be realized as one
   or more logical commits, optionally coordinated by implementation-specific
   helpers outside the core model.

4. When a server-side control-plane object is used, the authoritative
   identity of the in-flight interaction **MUST** become the server-issued
   object identifier. After that point, the original path string is entry
   input only and **MUST NOT** remain the sole identifier of the in-flight
   interaction.

5. Server-side control-plane objects **MUST NOT** advance namespace `seq`,
   **MUST NOT** appear as filesystem-visible resources, and **MUST NOT**
   appear in the namespace change feed.

6. Implementation-specific coordinators **MAY** exist, but they **MUST NOT**
   redefine logical commit boundaries or change-feed semantics.

### 9.3 Statefulness by operation

| Operation | Typical execution shape | Typical server-side control-plane object | Long-lived server state required? | Server is authoritative for | Client is authoritative for |
| --- | --- | --- | --- | --- | --- |
| `get <file>` | Single-request read | none | No | path resolution, access check, selected file revision, content serving or delegated download | local download progress, temporary file, client retries |
| `get -r <dir>` | Client-driven multi-request read | none in v0 | No | each request's path resolution, access check, and selected file revision | traversal progress, local outputs, client retries |
| `put <file>` (small, one-shot convenience) | Single request | none | No | destination resolution, validation, metadata commit | request payload, client retries |
| `put <file>` (large or resumable) | Begin, upload, commit | upload session, if used | Only if server-side resumability or stable binding is promised | stable destination binding, expected slot or revision, upload session validity, final publish | file reading, hashing, content upload progress, retry tokens |
| `put -r <dir>` | Client- or coordinator-driven uploads plus one or more logical commits | none required by the core model | No | each commit's validation and publication | orchestration across files, progress, retries |
| `cp <file>` (same server) | Single-request server-side copy | none | No | source resolution, destination resolution, metadata publication, content reference reuse | request retry |
| `cp -r <dir>` (same server) | Client- or coordinator-driven sequence of logical commits | none required by the core model | No | each commit's validation and publication | orchestration across entries, retries |
| `cp remote -> local` | Alias for `get` or `get -r` | same as `get` | same as `get` | same as `get` | same as `get` |
| `cp local -> remote` | Alias for `put` or `put -r` | same as `put` | same as `put` | same as `put` | same as `put` |

### 9.4 Client and server split for common commands

The following table is the normative split of responsibility for the primary
filesystem commands.

| Command | Server responsibilities | Client responsibilities |
| --- | --- | --- |
| `get <file>` | resolve the requested path or handle; authorize the read; select the file revision to read; serve bytes or delegated download targets | receive bytes; write local output; maintain local retry and resume state |
| `put <file>` (one-shot) | resolve the destination; validate preconditions; publish the metadata change | supply bytes or content reference; retry the request if needed |
| `put <file>` (resumable) | if an upload session is used, create or validate it; bind the destination; validate durable content and commit the final publish | read the local file; hash it; upload content; track upload progress; submit the final commit request |
| `cp <file>` (same server) | resolve source and destination; authorize both sides; create the copied resource; publish the metadata change | submit the request; retry if appropriate |

### 9.5 When raw paths cease to identify the operation

LoonFS accepts path-oriented input because user intent is naturally expressed
by path. However, long-running operations require a more stable identity than
a raw path string.

| Operation | Raw path is used for | Stable in-flight identity after start |
| --- | --- | --- |
| `get <file>` | the request itself | none required |
| `put <file>` (resumable) | `create_upload` only | server-issued upload session, if used |

### 9.6 Control-plane durability guidance

1. A control object **MUST** be durably recorded if losing it on restart
   would change correctness, visibility, retention safety, or promised
   resumability.

2. An upload session must be durable when it provides a stable destination
   across requests or must survive a server restart.

3. Implementation-specific coordinators **MAY** also be stored durably, but
   they are not required by this specification.

4. Control objects and any implementation-specific coordinators **MUST**
   remain outside namespace-visible metadata. Their existence is
   authoritative for orchestration, not for filesystem history.

### 9.7 Recommended defaults

A conforming implementation SHOULD use the following defaults unless a
stronger mode is explicitly requested:

- `get <file>` is a single-request operation;
- `cp <file>` on the same service is a single-request operation;
- if large or resumable `put <file>` uses a control object, it uses a single
  upload session.

These defaults preserve a simple model for single-request commands while
allowing multi-request correctness and resumability where they actually
matter.

## 10. Client and server responsibilities

| Concern | Server | Client |
| --- | --- | --- |
| Path resolution | Authoritative | Supplies user intent by path when using the filesystem surface. |
| Content hashing and upload | May accept inline bytes, proxy uploads, or issue upload capabilities. Must verify that uploaded content referenced by a commit is durable before publishing the commit. A server may issue short-lived content admission tokens after validation to avoid repeating expensive checks. | Usually responsible for reading local bytes, computing content hashes, and uploading missing content when originating new data. Clients may forward admission tokens when provided, but must tolerate slow-path validation. |
| Commit validation | Authoritative | Supplies preconditions and commit ids where needed. |
| Namespace visibility | Authoritative | Observes committed sequence receipts and change-feed deltas. |
| Long-running transfer progress | Authoritative for sessions that affect correctness | Responsible for local temp files, local progress, retry behavior, and any higher-level orchestration outside the core model. |
| Capability truthfulness | Advertises only implemented API groups and features. | Gates on the capability document; reconciles via `not_supported`. |

## 11. Extension points

The preferred extension point is the committed change feed. Downstream systems
such as indexers, notification services, preview builders, or policy engines
should consume committed changes rather than becoming part of the core
mutation path.

New client-visible operations arrive as API group ops or named features here;
new durable state arrives in `format.md`; new scheduling machinery is
implementation freedom. Cross-store discovery — naming authority, search,
ownership, quotas — is out of scope for this specification.

### SDK streaming downloads

The handwritten transfer helpers expose verified download streams in every SDK:

| SDK | Open a stream | Cancel or release it |
| --- | --- | --- |
| Go | `client.Files.DownloadStream(ctx, input)` | Close `Content` or cancel `ctx` |
| Python (`LoonFS` and `AsyncLoonFS`) | `client.files.download_stream(namespace_id, path=path, request_options=options)`; await with `AsyncLoonFS` | Use a `with` block or call `close()`; async uses `async with` or `await aclose()` |
| TypeScript server/browser | `client.files.downloadStream(input, requestOptions)` | Cancel the `content` reader or abort `requestOptions.abortSignal` |

The existing buffered download helpers collect these streams. Streams verify size
and checksum incrementally and report a verification error on the same iterator,
reader, or stream that carries the bytes. Only successful exhaustion verifies the
file; cancellation and early close do not. Consumers must handle late errors even
after some bytes have been delivered. A failed body is never automatically replayed.

Both direct and proxied downloads propagate the caller's transport controls.
Go respects a caller context deadline, with a 60-second operation deadline when
none is supplied. TypeScript uses `timeoutInSeconds` (client default, otherwise
60 seconds) across metadata and the body, plus `abortSignal`. Python uses the
request/client HTTPX timeout for I/O waits on both transports; its synchronous
iterator closes the response on early exit from a `with` block. The async iterator
uses an `async with` block. API authorization, cookies and API headers are not
copied into presigned object-store requests.

### SDK streaming uploads

Streaming preparation accepts a caller-owned `io.Reader` in Go, a binary file
object in synchronous Python, an `AsyncIterator[bytes]` or binary file object in
async Python, and a `Blob`, `ReadableStream<Uint8Array>`, or
`AsyncIterable<Uint8Array>` in TypeScript. It returns the same prepared content
used by the byte helpers, without publishing a filesystem entry:

| SDK | Prepare once | Prepare and publish once |
| --- | --- | --- |
| Go | `Files.PrepareStream(ctx, namespaceID, reader, sizeBytes)` | `Files.UploadStream(ctx, files.StreamUploadInput{...})` |
| Python (`LoonFS` and `AsyncLoonFS`) | `files.prepare_stream(namespace_id, content=reader, size_bytes=size)`; await with `AsyncLoonFS` | `files.upload_stream(namespace_id, content=reader, ...)`; await with `AsyncLoonFS` |
| TypeScript server/browser | `files.prepareStream({ ...namespace, content, size_bytes }, options)` | `files.uploadStream(input, options)` |

The optional size is checked against the consumed bytes; TypeScript infers it
for a Blob. Small streams become inline prepared content when supported, including
unknown-size streams. After inline lookahead, larger unknown-size sources use
multipart when advertised, otherwise service-proxied uploads. SDK reads are
limited to 64 KiB, and multipart uploads
retain a provider-sized part plus at most 10,000 part descriptors. Async Python
and TypeScript retain the caller's current chunk, so callers should also produce
bounded chunks. Known TypeScript sources up to 8 MiB use a bounded fixed request body to
preserve browser compatibility; larger or unknown single-request uploads require
runtime support for streaming request bodies. Multipart sends fixed part bodies
and does not require that support. Existing byte helpers use these same paths.

Upload preparation and publication use the same caller transport controls as
downloads. Go and TypeScript deadlines cover the combined `UploadStream` /
`uploadStream` operation. Python's timeout bounds HTTP I/O waits. Async Python
runs blocking file reads in a worker thread to keep the event loop running.
A blocking caller-owned Go or Python source read must be interrupted by its owner;
network cancellation cannot interrupt arbitrary source code. Callers close Go/Python
sources; TypeScript cancels or returns its consumed source on completion or error.

Payloads are consumed once and never automatically replayed. Source errors,
size mismatches, failed payload requests, and async Python cancellation during
staging prevent completion and trigger a best-effort abort with a separate
five-second cleanup timeout (an I/O timeout in synchronous Python). An ambiguous
completion failure leaves the session available for inspection. After successful
preparation, retain the result and retry `UploadPrepared` / `upload_prepared` /
`uploadPrepared` with identical publication inputs; do not reread a stream or
start a new upload to retry publication.
