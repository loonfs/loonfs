# LoonFS API Specification

This document is the normative specification of the LoonFS client-facing API. It covers API groups, capability discovery, the standard error contract, operation statefulness, and the representative v0 HTTP binding.

The specification is normative for the parts of the API that a deployment implements. A deployment chooses which optional API groups to expose, but every operation it does expose MUST have the shape specified here.

The [format specification](format.md) is the companion document. It defines the durable format, which is mandatory for every implementation.

The same client code works with an embedded engine and with a hosted server. Both expose the same operations, advertise their capabilities the same way, and report unsupported operations and features with the same errors. The only difference a client sees is which capabilities a deployment advertises.

The key words MUST, MUST NOT, SHOULD, and MAY in this document are to be interpreted as described in BCP 14 ([RFC 2119](https://www.rfc-editor.org/rfc/rfc2119), [RFC 8174](https://www.rfc-editor.org/rfc/rfc8174)) when, and only when, they appear in capitals.

## 1. API groups

This section defines the API groups and describes where new behavior belongs.

An "API group" is a coherent area of responsibility with its own set of endpoints (not a resource type). Each API group is an all-or-nothing unit of conformance, so a deployment MUST NOT advertise an API group unless it implements every required operation in it. Optional behavior within an API group is expressed as features, and each feature has a key in the capability document (see [Capability discovery](#2-capability-discovery)).

| API group | Operations | Status |
| --- | --- | --- |
| `filesystem/v0` | Path and inode reads, path and inode mutations, uploads, the change feed, namespace state, capability discovery, and standard errors. Namespace lifecycle, snapshots, attributes, inode child listing, and direct transfers are features. | Mandatory for any conforming deployment |
| `maintenance/v0` | Namespace diagnostics, checkpoints, one-shot metadata maintenance, retention-floor advancement, garbage collection, and grep index maintenance (as a feature). | Optional |
| `query/v0` | Content search over derived indexes, as the `query.grep` feature. | Optional |

API groups are client contracts, and they don't describe node roles or deployment topology.

Notes:

- An embedded engine implements `filesystem/v0` (with the namespace-management features enabled) and `maintenance/v0` (maintenance can be triggered manually). A minimal server that wraps the embedded engine over HTTP advertises the same two API groups and is fully conformant.
- A hosted LoonFS server serves `maintenance/v0` when its `maintenance` setting is `serve_and_maintain` or `serve_only`. In `maintain_only` or `disabled` mode, every route under `/v0/maintenance/` returns `route_not_found`, as any path outside the served routes does. All four modes are fully conformant.
- Each API group has its own version, so `filesystem/v0` could coexist with a future `maintenance/v1`. The group name (the segment before the slash) is the stable identity that feature keys refer to.
- This document doesn't define any queue or job-scheduling semantics. `maintenance/v0` exposes only the shapes for triggering work and reporting its status, and how work is scheduled is up to the implementation.

### 1.1 Where new behavior belongs

Where new behavior belongs depends on three questions:

1. Does it change bytes or metadata that another implementation needs to interpret? If so, it belongs in the [format specification](format.md), and it's mandatory.
2. Is it a client-visible operation whose shape should be uniform wherever it exists? If so, it belongs in this document, inside an API group or as a feature.
3. Is it about how work gets done (queues, schedulers, or caches)? If so, it's up to the implementation, and it doesn't belong in any specification document.

## 2. Capability discovery

This section describes the capability document, which lists what a deployment supports, and the registry of feature keys.

### 2.1 The capability document

Every deployment describes itself with one capability document. A remote client fetches it from `GET /v0/capabilities` and caches it for the connection, and an embedded engine exposes the same document as a constant. As a result, the SDK checks capabilities the same way for both backends.

The example below is the reference deployment's document. It lists the runtime's own `filesystem/v0` and `maintenance/v0` API groups, plus the `query/v0` API group, which the server adds by composing the grep extension. A host that composes no extension advertises neither that API group nor the two grep feature keys (`query.grep` for searching an index, and `maintenance.grep.index` for administering one). Either way, clients decide what to call based on the document.

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
    "maintenance.gc.min_grace_window_ms": 2490000,
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
| `features` | Feature keys, and whether this deployment supports each one. An absent key means the feature isn't supported. |
| `limits` | Advisory numeric limits that clients can use to check requests before sending them. The object can be empty. |

Rules:

- API groups are all-or-nothing for their required operations, so clients never probe operation by operation inside an advertised API group.
- Every feature key is dotted (for example, `query.grep`), and its first segment MUST be the group name of an advertised API group. A feature key whose first segment doesn't match an advertised API group is malformed, and clients MUST reject the document or ignore that key.
- Clients MUST ignore unknown feature keys and unknown document fields.
- Limits are advisory. The server's response to the actual request is the authoritative outcome.

Registered limit keys:

| Limit key | Meaning |
| --- | --- |
| `pagination.default_limit` | The page size used when a paged request omits `limit`. An explicit `limit=0` is rejected with 400 `invalid_request`. |
| `pagination.max_limit` | The largest page size accepted for paged requests. A `limit` greater than this value is rejected with 400 `invalid_request`. |
| `upload.service_proxied.max_content_bytes` | The largest request body that one service-proxied upload content request (`PUT .../uploads/{upload_id}/content`) accepts. It isn't a maximum file size. Clients can use `direct_put` for larger content only when `filesystem.uploads.direct_put` is advertised. Otherwise, they MUST stay within this limit. |
| `upload.direct_put.max_content_bytes` | The largest object that this deployment's provider accepts in one presigned `direct_put` request. It's unrelated to `upload.service_proxied.max_content_bytes`, which limits service-proxied uploads. A size hint above this limit returns `content_too_large` when the upload begins, and completing the upload checks the actual stored size. This limit is advertised only with `filesystem.uploads.direct_put`. |
| `upload.complete.max_request_body_bytes` | The largest JSON body that `POST .../uploads/{upload_id}/complete` accepts. A larger request returns `content_too_large`. |
| `download.service_proxied.max_content_bytes` | The largest file content that a service-proxied read (`GET .../filesystem/content`, `GET .../inodes/{inode_id}/content`, or `GET .../inodes/{inode_id}/revisions/{revision_no}/content`) streams and returns in one response. A read over the limit returns `content_too_large`. The limit is checked against the whole file. Proxied reads use bounded chunks, but they have no ranged form. When `filesystem.downloads.direct_get` is advertised, a file over this limit is read through the matching path or inode download grant. (That feature is advertised on exactly the deployments that could have let a client create such a file.) |
| `upload.service_proxied.max_concurrent_requests` | The number of service-proxied upload requests that a serving process streams at once. All callers share this cap (it isn't a per-caller allowance). A request over the cap returns `server_busy`. |
| `download.service_proxied.max_concurrent_requests` | The number of service-proxied content reads that a serving process streams at once. All callers share this cap (it isn't a per-caller allowance). A request over the cap returns `server_busy`. A read counts against the cap until its body finishes or is dropped. |
| `access.max_principals_per_request` | The largest number of principal IDs that one request can act as. Headers over the limit return `invalid_request`. |
| `commit.max_inline_content_bytes_per_operation` | The largest file size for one `inline_content` value, measured before base64 encoding. This limit is advertised only with `filesystem.commits.inline_content`. A larger value returns `invalid_request` before commit planning. This limit applies to each value, and `commit.max_request_body_bytes` limits the whole request. |
| `commit.max_request_body_bytes` | The largest JSON body that a commit request accepts, including base64 inline content and every other field. A larger body returns `content_too_large`. HTTP deployments advertise this limit. |
| `commit.max_operations` | The largest number of path operations that one commit can include. A longer list returns `invalid_request` before planning, on every transport. |
| `commit.max_preconditions` | The largest number of precondition entries that one commit can include. It counts entries, not resources. A longer list returns `invalid_request` before planning, on every transport. |
| `commit.max_content_tokens` | The largest number of content tokens that one commit can include. A request over the limit returns `invalid_request` before planning. |
| `commit.max_external_content_refs` | The largest number of distinct external content references that one commit's operations can refer to. A request over the limit returns `invalid_request` before planning. |
| `commit.max_message_bytes` | The largest commit `message` accepted, in bytes. A longer message returns `invalid_request` before planning. |
| `maintenance.gc.min_grace_window_ms` | The smallest `grace_window_ms` accepted on a `gc` request. A smaller value returns `invalid_request`. The value is derived from the publication budgets and the direct transfer capability lifetime, rather than tuned. |
| `snapshot.max_ttl_ms` | The largest `ttl_ms` that snapshot create and extend requests accept. A larger value returns `invalid_request`. |
| `snapshot.max_lifetime_ms` | The longest snapshot lifetime, measured from the time the snapshot record was created. Extending a snapshot never moves its expiry past this limit. |
| `snapshot.max_live_per_namespace` | The largest number of live, unexpired snapshots that one namespace can have. Creating a snapshot past this limit returns `snapshot_quota_exceeded`. |
| `query.grep.default_limit` | The number of matches per grep page when the request omits `limit`. |
| `query.grep.max_limit` | The largest grep page limit accepted. An invalid limit is rejected with `invalid_request`. These query keys identify the operation's contract, even though grep uses the same values as standard pagination. |
| `query.grep.scan_budget_files` | The number of files that an `allow_scan` grep without a plan scans before the request fails with `query_unindexable`. |
| `query.grep.tail_budget_files` | The number of revisions in the unindexed tail that one grep scans exhaustively before it fails with `index_lagging`. |

### 2.2 Feature registry

This section defines every feature key and the operations it gates. The registry doesn't change within a protocol version, and new keys are added only with a spec change.

| Feature key | Gates | Notes |
| --- | --- | --- |
| `maintenance.grep.index` | Maintaining a namespace's grep index: `GET /v0/maintenance/namespaces/{ns}/grep/index` and its `enable` and `disable` routes, and the `grep_gc` run kind. | The maintenance half of the grep capability. It's independent of `query.grep`. Searching an index and keeping one built can be deployed separately, so a deployment can advertise either key alone. A deployment that doesn't maintain an index returns `not_supported`, with this key in its `feature` field, for these routes and for the run kind. |
| `filesystem.namespaces.create` | Creating namespaces (`POST /v0/namespaces`). | |
| `filesystem.namespaces.fork` | Forking namespaces (`POST /v0/namespaces/{ns}/forks`). | |
| `filesystem.namespaces.delete` | Deleting namespaces (`DELETE /v0/namespaces/{ns}`). | Deletion is terminal. After deletion, maintenance runs with `kind` set to `gc` can reclaim the metadata and the namespace's own content, subject to the conditions in [`DELETE /v0/namespaces/{ns}`](#63-delete-v0namespacesns). A deployment can still advertise `false` and return `not_supported`. |
| `filesystem.snapshots` | Creating, listing, extending, and releasing snapshots under `/v0/namespaces/{ns}/snapshots`. | |
| `filesystem.commits.inline_content` | Sending file content in a `put_file`, `create_file_by_inode`, or `put_file_revision_by_inode` commit operation. | A deployment advertises this key when inline writes are enabled. The per-file limit is `commit.max_inline_content_bytes_per_operation`. Without this feature, a client uploads the content before committing. |
| `filesystem.uploads.direct_put` | Starting presigned `direct_put` upload sessions (`POST /v0/namespaces/{ns}/uploads`). | The server returns a short-lived, create-only presigned PUT capability for the exact content object. The provider MUST report a durable whole-object checksum after the write. A deployment advertises the key only on an endpoint that the live conformance suite has run against. The key is independent of `filesystem.uploads.direct_multipart`, so a provider can offer this feature and no multipart API at all. Raw object keys and caller-managed object-store writes aren't part of this feature. |
| `filesystem.uploads.direct_multipart` | Starting presigned `direct_multipart` upload sessions (`POST /v0/namespaces/{ns}/uploads`) and signing their parts (`POST /v0/namespaces/{ns}/uploads/{upload_id}/parts`). | The server opens the provider's multipart upload and returns one short-lived, checksum-bound capability for each part. This feature needs an S3-style multipart API in addition to the signing that the other keys need, so a deployment whose provider has no multipart API leaves out this key and can still advertise the others. |
| `filesystem.downloads.direct_get` | Requesting path or inode download grants (`POST /v0/namespaces/{ns}/filesystem/downloads`, `POST /v0/namespaces/{ns}/inodes/{inode_id}/downloads`, and `POST /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{revision_no}/downloads`). | The server returns a short-lived presigned GET capability for the selected content object. Any deployment that offers a direct write also advertises this key, because a deployment that lets a client create an object larger than `download.service_proxied.max_content_bytes` has to be able to return that object. Raw object keys aren't part of this feature. |
| `query.grep` | Content search (`GET /v0/namespaces/{ns}/grep`). | The serving half of a data-dependent capability (see [Data-dependent features](#23-data-dependent-features)). A grep request also requires a materialized, active grep manifest, and a namespace without one returns `not_supported` whatever this key's value is. |

`maintenance.grep.index` is the only feature key in `maintenance/v0`. Every other operation in that API group is required.

Listing namespaces is intentionally not supported in v0. Callers MUST address namespaces by ID until LoonFS has a scalable design for a namespace catalog or index.

### 2.3 Data-dependent features

The capability document describes a deployment. Whether something is materialized for a namespace's data (for example, whether a derived index is ready for a namespace) is recorded in the owning extension's keyspace (see [Extensions](format.md#124-extensions)), not in the namespace manifest.

A data-dependent operation succeeds only when both of these are true:

- The deployment advertises the serving capability in its capability document.
- The namespace's metadata shows that the capability is materialized, through the extension's own verified readiness marker.

## 3. Standard error contract

This section defines the body of every error response, the registry of error codes, the reference server's request and connection limits, and which errors clients retry automatically.

Every error response has a JSON body in this form:

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

The top-level fields (`code`, `message`, `param`, `feature`, and `request_id`) describe the failed request and the error. `details` contains the machine-readable values involved.

`code` is the stable machine contract. `message` is human-readable, and it can change between releases. `feature` is present only on `not_supported` errors, and it identifies the capability-document key that the client should reconcile against. Clients MUST branch on `code`, MUST tolerate codes they don't recognize, and MUST NOT parse `message`.

`request_id` is the correlation ID that the server assigned to the request. Every response (success or error) also includes it in the `x-request-id` header, so a caller's log line and the server's trace can be joined.

`param` is present when the error identifies one invalid input. Its format depends on where the input came from:

| Input source | `param` value |
| --- | --- |
| JSON request body | JSON Pointer |
| Request header | Header name |
| Query parameter | Parameter name |
| Path parameter | Parameter name |
| CLI-local input | Flag or argument spelling |

Commit shape errors identify the offending expectation field as `/operations/{i}/{field}` or `/preconditions/{i}/{field}`, where `i` is its zero-based position.

This contract also covers request-shape failures. A query string, path parameter, or JSON body that the server can't parse returns `invalid_request` inside this envelope, never a framework's plain-text rejection. Authorization is checked first, so a malformed request without valid credentials returns `unauthorized`. An unrecognized query parameter returns `invalid_request` with `param` set to that parameter's name, just like an unknown request-body field (see [Authentication and transport](#authentication-and-transport)).

`details` is present when the failure involves machine-usable identity (such as an inode ID or a writer epoch), so a caller never has to parse `message` to act. Every field in `details` is optional, and clients MUST tolerate absent fields exactly as they tolerate unknown codes. When a caller supplied one value and the server found another, `details` uses the field names `expected_<field>` and `actual_<field>`.

The codes that populate it:

| Code | Detail fields |
| --- | --- |
| `namespace_deleted` | `namespace_id` identifies the deleted namespace, including a fork's source or target. |
| `writer_fenced` | `fenced_writer_epoch` and `active_writer_epoch`, plus `active_writer_id` and `active_acquired_at_ms` when the current manifest records a writer block. Writer IDs are process labels, so two runs on one machine can share one. The acquisition timestamp distinguishes them. |
| `path_conflict` | `expected_inode_id` and `actual_inode_id` (absent when the path isn't bound), plus `precondition_index` for a failed request precondition. |
| `stale_revision` | `inode_id`, `expected_revision_no`, and `actual_revision_no` (absent when the inode has no current revision or isn't visible), plus `precondition_index` for a failed request precondition. |
| `stale_attributes` | `inode_id`, `expected_attributes_revision_no` (absent when the caller stated no expectation), and `actual_attributes_revision_no` (absent when the inode isn't visible), plus `precondition_index` for a failed request precondition. |
| `stale_access` | `inode_id`, `expected_access_revision_no` (absent when the caller stated no expectation), and `actual_access_revision_no` (absent when the inode isn't visible), plus `precondition_index` for a failed request precondition. |
| `binding_version_mismatch` | `inode_id`, `expected_binding_version` (the request's token as supplied), and `actual_binding_version` (the current binding's token, absent when the inode isn't visible), plus `precondition_index` for a failed request precondition. Clients MUST NOT parse or order the tokens. |
| `commit_id_reuse_conflict` | `commit_id`, plus `committed_seq` and `committed_fingerprint` when the server detected the conflict from a durable commit receipt. `committed_seq` is the sequence that `commit_id` was already committed at, and `committed_fingerprint` is the semantic identity of what was committed there (see [Commit identity and preconditions](#51-commit-identity-and-preconditions)). The sequence comes from the receipt, and the fingerprint comes from the retained commit row at that sequence, so both are present or neither is. Both are absent when nothing has been committed under the ID yet and two live requests are claiming it at once. |
| `rebootstrap_required` | `after_seq` and `retention_floor_seq`. |
| `stale_head` | `expected_head_seq` and `actual_head_seq` for a caller-supplied head precondition, plus `precondition_index` for a failed request precondition. |
| `forbidden` | `inode_id`. |
| `not_deleted` | `inode_id`, plus `expected_deletion_seq` and `actual_deletion_seq` when a live deletion exists at a different sequence. |
| any failed commit | `commit_id`, the idempotency key that the request tried to commit under. The server echoes it so that failed and uncertain outcomes include the caller's reconciliation handle (see [Commit responses and safe retry](#52-commit-responses-and-safe-retry)). |
| any failed operation | `operation_index`, the zero-based position of the operation that stopped the request (0 for a one-operation request). See [Commit identity and preconditions](#51-commit-identity-and-preconditions). |

One code exists specifically so that capability handling is uniform from the start:

- `not_supported` (HTTP 501): the deployment doesn't implement the requested operation or feature. Any operation can return it. A client maps the error to its `feature` key, and it disables or degrades that code path.

The full registry (`ErrorCode` in `loonfs-types`) lists every error code and its HTTP status:

| Code | HTTP status | Meaning |
| --- | --- | --- |
| `invalid_request` | 400 | The request is malformed. A path, ID, cursor, parameter, staged content reference, configuration value, or commit request limit fails validation. The message identifies the offending field or limit. |
| `unauthorized` | 401 | The credentials are missing or wrong. |
| `forbidden` | 403 | The subject lacks authority that the operation needs: some but not all of the required rights on a checked inode, the authority that a move or access update requires, administrator rights on an administrator-only operation, or the namespace's principal scope. If the subject has no rights on a checked inode, the request returns `path_not_found` or `inode_not_found` instead (see [Authorization in ACL namespaces](#authorization-in-acl-namespaces)). |
| `content_too_large` | 413 | A request or proxied response exceeds its size limit. Every JSON request body MUST fit within 2 MiB, except on the upload operations, which have their own limits. One append contains at most 256 KiB. Send smaller proxied uploads, or use `direct_put` when it's available. Multipart completions MUST fit within `upload.complete.max_request_body_bytes`. For large reads, request a download grant when `filesystem.downloads.direct_get` is available. |
| `route_not_found` | 404 | No route matches the request path. |
| `method_not_allowed` | 405 | The path exists, but it doesn't serve this HTTP method. |
| `namespace_not_found` | 404 | The namespace has no installed manifest, so it doesn't exist. |
| `namespace_deleted` | 410 | The namespace ID is deleted, and it can never be created or forked into again. Ordinary operations fail. The response's details identify the deleted namespace. |
| `checkpoint_not_found` | 404 | The checkpoint ID doesn't identify a user checkpoint. Either no pin exists under it, or the pin belongs to a snapshot or a fork. |
| `snapshot_not_found` | 404 | The snapshot ID doesn't identify a snapshot. Either no pin exists under it, or the pin belongs to a user checkpoint or a fork. Refresh state or choose another snapshot. |
| `snapshot_gone` | 410 | The snapshot exists and has expired. |
| `path_not_found` | 404 | No visible entry exists at the path. In an ACL namespace, the server also returns this code for a checked entry on which the subject has no rights. |
| `inode_not_found` | 404 | The requested visible or retained inode doesn't exist. In an ACL namespace, the server also returns this code for a checked inode on which the subject has no rights. |
| `revision_not_found` | 404 | The file has no such revision. |
| `upload_not_found` | 404 | No upload session exists with this ID. The server also returns this code for an aborted session, and for an open session asked to stage, sign parts, or complete at or after its `expires_at_ms`. Neither session will ever select content, so the server reports each one as missing, as it will once the session is deleted. |
| `namespace_exists` | 409 | The create or fork target already exists (another namespace has the ID). |
| `snapshot_quota_exceeded` | 409 | Creating the snapshot would exceed the namespace's live-snapshot limit. Delete a snapshot or wait for one to expire. |
| `content_not_prepared` | 409 | A path put or an explicit create or replace operation references external content without a matching admission, or it includes a relevant token that was rejected. Prepare the content and retry with its proof. |
| `path_conflict` | 409 | The destination path is already bound. |
| `directory_not_empty` | 409 | The directory has children, and the operation isn't recursive. |
| `stale_head` | 409 | The write raced a head advance, or a caller-supplied `expected_head_seq` doesn't match the head. Retry against fresh state. A read also returns this code when its manifest's segments were garbage collected and a newer manifest exists. Read again against current state. A read at a snapshot or checkpoint that finds a segment missing returns what a new read of its pin would return: `snapshot_not_found` or `checkpoint_not_found` when the pin was deleted during the read, `snapshot_gone` when it expired, and `namespace_corrupt` when the pin still exists. |
| `stale_revision` | 409 | A caller-supplied base revision is no longer current. |
| `stale_attributes` | 409 | The inode's attribute revision changed while the update was being decided. Two cases return this code: a caller-supplied expected attribute revision that's no longer current, and the revision precondition that every attribute update includes even when the caller states no expectation. Re-read the attributes and retry. |
| `stale_access` | 409 | The inode's access revision changed while the update was being decided. Re-read the access row and retry. |
| `namespace_unrestricted` | 409 | The namespace's access mode is unrestricted, so it has no access rows. |
| `binding_version_mismatch` | 409 | The binding version supplied for an inode move or delete, or by a binding precondition, isn't the entry's current binding version. Re-read the entry before retrying. |
| `not_deleted` | 409 | The undelete target isn't the root of a live deletion, so there's nothing to recover. |
| `writer_fenced` | 409 | Another writer superseded this session. The session is dead, and this request fails. The host drops the session, and a later request opens a new session whose first publication takes the namespace back. Repeated fencing of one namespace means that two writers are receiving its traffic. |
| `would_cycle` | 409 | The rename would create a directory cycle. |
| `commit_id_reuse_conflict` | 409 | The commit ID was reused with different content. |
| `upload_already_completed` | 409 | The upload session is already completed, so it can't select other content and can't be aborted. |
| `upload_content_conflict` | 409 | Different bytes were staged under this upload ID, or another staging request has the session's staging claim (one still in flight, one that was cancelled, or one whose write might still take effect). Open a new session. |
| `query_unindexable` | 400 | The pattern has no run of at least 3 literal bytes, so the trigram index can't narrow the candidates. Rewrite the pattern, or set `allow_scan` (capped by `query.grep.scan_budget_files`). |
| `rebootstrap_required` | 409 | A change feed's `after_seq` is below the retention floor, so the feed no longer contains the changes after it. Rebuild state from a fresh listing or checkpoint. |
| `not_supported` | 501 | The deployment doesn't implement the requested operation or feature. |
| `commit_outcome_unknown` | 503 | The outcome of the publication wasn't observed, so the commit might or might not be visible. Retry with the same commit ID, or reconcile. |
| `outcome_unknown` | 503 | The outcome of a write other than a commit wasn't observed, so the operation might or might not have taken effect. Read the resource before retrying. |
| `commit_queue_full` | 503 | The namespace's write queue is full. Back off and retry. |
| `writer_session_closed` | 503 | This node has no open writer session for the namespace. The request wasn't admitted. Retry on the node that the namespace is assigned to. |
| `server_busy` | 503 | The server is at its configured concurrency limit for requests, proxied upload bodies, or proxied content reads. Back off and retry. |
| `shutting_down` | 503 | The serving process stopped admitting requests because it's shutting down. Work that was admitted earlier still finishes. Retry against a live instance. |
| `deadline_exceeded` | 503 | The server cancelled a bounded request at its configured `request_deadline_ms`. A commit might still take effect after this response, so reconcile it by commit ID before retrying. |
| `content_not_materialized` | 503 | The file is committed, but this deployment can't write its pending content pieces for a direct download. Read through the proxied content route, or retry after a fold writes the pieces. |
| `checkpoint_unavailable` | 503 | Required checkpoint state is unavailable: the pin exists but its manifest is gone, pin creation couldn't verify its basis, or a metadata publication ran past its budget before publishing. Retry after maintenance. |
| `maintenance_required` | 503 | The namespace's metadata needs maintenance before the request can be served. Run maintenance and retry. New commits are refused at the WAL write-stop threshold. A request with a commit ID that the namespace already knows still returns the result recorded in its receipt. |
| `index_lagging` | 503 | The grep index is further behind the head than the exhaustive-scan budget allows. Run maintenance or set `allow_stale`, then retry. |
| `storage_permission_denied` | 503 | The object store rejected the deployment's storage credentials for this operation. Fix the storage credentials or bucket policy, because an unchanged retry won't succeed. |
| `index_corrupt` | 500 | The grep index's derived state failed validation. Disable and re-enable grep on the namespace to rebuild it. Core filesystem state stays available. |
| `namespace_corrupt` | 500 | Durable namespace state failed validation. |
| `server_error` | 500 | An unclassified internal failure. |

The reference server admits at most `max_in_flight_requests` requests at once (256 by default). It refuses excess requests before authentication, body extraction, or store access. A request keeps its slot until the response body finishes or is dropped. Uploads and downloads count against both this cap and their transfer cap. `GET` and `HEAD` requests to `/health` and `/readiness` are exempt, and probe query parameters don't change the exemption. `/metrics` is subject to the cap.

The reference server also limits accepted connections with `max_connections` (1,024 by default). When all connection slots are in use, it waits before accepting more TCP connections. Pending connections stay in the OS backlog. A client can connect and then receive no HTTP response until a slot is available, or it can time out or fail if the backlog is full. TLS handshakes and idle keep-alive connections occupy slots. The connection limit applies to every route, including probes.

Automatic retry is narrower than the HTTP status suggests. Raw transport failures can be retried. Of the registered error codes, clients automatically retry only `commit_queue_full`, `server_busy`, and `shutting_down`, and responses with one of these three codes include `Retry-After: 1`. Generated SDKs retry a response that includes `Retry-After`, and they don't retry on status alone.

These codes need another action before a retry:

- `writer_session_closed` requires routing to the node that the namespace is assigned to. It can also occur on that node while a fenced session finishes its work, and a later request can open a new session after that work ends. It has no `Retry-After` header, and clients don't retry it automatically.
- `checkpoint_unavailable`, `maintenance_required`, and `index_lagging` require maintenance.
- `storage_permission_denied` requires the operator to fix the storage credentials or bucket policy.
- `commit_outcome_unknown`, `outcome_unknown`, and `deadline_exceeded` require the caller to determine whether a mutation completed before retrying it.

Precondition failures surface as `409` resource-state conflicts (`stale_revision`, `stale_head`, `commit_id_reuse_conflict`) rather than `412`.

Why `409` and not `412`? v0 treats a precondition failure as a conflict with current namespace state, not as an HTTP conditional-request failure.

## 4. SDK shape

This section describes the shape of the SDKs and the embedded runtime that clients program against. The same SDK works with both backends, so clients don't need different code for each deployment mode.

A generated client accepts a default actor when it's constructed, and a per-request header can override it.

Generated SDKs use schema names as public type names:

- A resource body uses the resource name, such as `Checkpoint` or `UploadSession`.
- The namespace body is `NamespaceMetadata`, because `Namespace` is the name of the embedded handle.
- A response envelope uses `<Verb><Noun>Response`, such as `ListCheckpointsResponse`.
- An operation's request and response schemas share its verb, as in `CreateDownloadRequest` for `create_download`.

Namespace-owned resources include `namespace_id`.

Revision numbers, change sequences, attribute revisions, manifest numbers, writer epochs, and grep run numbers are JSON integers from 0 through 9007199254740991 (`2^53 - 1`). Implementations MUST reject larger input values and MUST NOT store a larger value. `inode_id` isn't an ordinal, and it can use the full `u64` range.

Each field-name suffix has one meaning:

| Suffix | Meaning |
| --- | --- |
| `_seq` | A position in the namespace commit history. |
| `_no` | A monotonic counter scoped to one object: a file's revisions, an inode's attribute revisions, or a namespace's manifests. |
| `_index` | A 0-based position inside a container. |
| `_number` | A 1-based position defined by a provider or a tool. |
| `_id` | An opaque identity. |

The embedded runtime and the remote client (`loonfs_client::Client`) use the same verbs for the same operations, such as `stat` and `list`. The client's receiver is the service, so its methods also take the namespace or path, and a few keep the wire operation's name, such as `get_health`.

In the embedded runtime:

- `loonfs::LoonFs` is the runtime, in a `ReadOnly` or a `Writable` mode. A writable runtime also creates and forks namespaces, and `LoonFs::maintenance` returns its maintenance operations.
- A `loonfs::Namespace` handle acts on one namespace, and its methods take no namespace ID. `LoonFs::namespace` returns a `Namespace<ReadOnly>`, which provides that namespace's reads. On a writable runtime, `LoonFs::open_namespace` returns a `Namespace<Writable>`, which provides the same reads and the namespace's writes.

The namespace handle's `metadata` corresponds to the client's `get_namespace`. The runtime's `capabilities()` and the client's `get_capabilities()` return the capability document described in [The capability document](#21-the-capability-document). The remote client fetches the document from `GET /v0/capabilities` and caches it, and for the embedded runtime the document is a constant.

The runtime and the client also follow these rules:

- They stay aligned by sharing one definition of every option struct they both take, not by a trait that either one implements. (`PutFileOptions`, `CreateDirectoryOptions`, and `DeleteOptions` are defined in `loonfs-types` and re-exported by both.) There's no transport abstraction to program against, so a host picks the embedded runtime or the HTTP client directly.
- A mutation takes the actor as an argument, and the remote client sends it in the `Loonfs-Actor` header. An option struct contains only optional settings and has a default, so each mutation has a plain form and a form that takes options.
- Unsupported operations and features are typed. An individual operation returns the `not_supported` error with its `feature` name, so the gating logic (check the capability document, and fall back on `not_supported`) is the same with either backend.

Generated SDKs group operations by resource, not by API group. `capabilities`, `namespaces`, `snapshots`, `commits`, `changes`, `files`, `inodes`, `trash`, and `uploads` are at the client root. The `maintenance` API group keeps its prefix as a nested group, as in `maintenance.checkpoints.create`, because a hosted deployment can hide the whole API group. Methods follow these naming rules:

- A method on a group's own resource is a bare verb: `create`, `retrieve`, `list`, `delete`, or the action, as in `fork` and `grep`.
- A method for a sub-resource of one file or inode is a verb plus a noun on the parent, as in `files.listRevisions` and `inodes.listChildren`.

The `/health`, `/readiness`, and `/metrics` probes are HTTP-only and have no SDK method.

Each public embedded read takes one slot from the runtime's `ExecutionBudget` before its first store request. `ExecutionBudgetBuilder::max_concurrent_reads` is 64 by default, and reads beyond the limit wait. Read-only and writable runtimes that share a budget share this limit. Dropping a waiting read removes it from the queue.

Reads take and release slots as follows:

- Each pager page takes and releases its own slot.
- Capturing a `ReadView` takes one slot. The view keeps no slot between calls, and each read through it takes one.
- Nested snapshot stat and list calls reuse the outer read's slot.
- A buffered content read keeps its slot until it completes.
- A streamed or ranged content read releases its slot after the metadata and content lookup, before it returns the body. The existing transfer limits bound how the body is consumed.
- Core reads within mutations and maintenance don't take a read slot.
- Grep queries and index steps acquire and release a slot for each public read. Nothing waits for a grep step permit while it has a read slot.

### 4.1 Public inode identity

Public APIs represent an inode ID as a string, such as `ino_27`. The value MUST start with the lowercase prefix `ino_`, followed by a nonzero `u64` with no leading zeroes. Numbers, numeric strings such as `"27"`, zero, and uppercase prefixes are invalid. `ino_1` identifies the namespace root.

An inode ID is unique only within its namespace, so an inode is identified by `namespace_id` and `inode_id` together. Clients MUST treat the ID as an opaque value, and they MUST NOT create IDs or infer ordering from the numeric suffix.

### 4.2 Checksums

Every public checksum value has the same shape:

```json
{ "algorithm": "crc64nvme", "value": "<16 lowercase hex>" }
```

The allowed algorithms are:

- `crc64nvme`, whose value is exactly 16 lowercase hexadecimal characters.
- `crc32c`, whose value is exactly 8 lowercase hexadecimal characters.

Other algorithms and invalid values are rejected.

The field that contains a checksum defines what it covers. Checksums in `ContentRef` and `UploadContentClaim` cover the complete content, and a part checksum covers one multipart upload part. A `checksum_algorithm` field selects an algorithm, but it doesn't contain a checksum value.

Every content reference uses the store's checksum algorithm. Every response to an upload begin request reports that algorithm in `checksum_algorithm`, including for `service_proxied` uploads. Reads verify the checksum stored in the content reference.

## 5. Minimal upload, commit, and change-feed model

This section describes how a client prepares content and commits changes, how commits are identified, attributed, and retried, and how a namespace's writer session publishes them.

To write a file, a client prepares the file's content and then commits the change. Committed changes are available through the change feed in sequence order.

Uploaded content is durable before the commit, but the file isn't visible yet. Inline content becomes durable and visible in the same write (see [File contents and ownership](format.md#15-file-contents-and-ownership)). In both cases, the commit takes effect when the next numbered WAL object is written with put-if-absent. Accepting a request into a batch doesn't mean that the request has committed.

A caller with an embedded `loonfs::Namespace<Writable>` can prepare content separately from committing it. `prepare_content` takes the file's bytes. Files at or below the enabled inline threshold stay in memory, and larger files are uploaded as content objects. To import existing content, the caller uses `prepare_content_ref`, which verifies the bytes and copies them to a new object owned by the destination namespace. Both calls return an opaque prepared value.

`put_file_prepared` commits a prepared value. If WAL limits are reached, inline bytes might need to be uploaded first. Content that's already uploaded needs no more content-object I/O during publication. For an upload session, `complete_upload_prepared` returns both the completion response and a prepared value. Over HTTP, the commit request includes either inline bytes, or a content reference with its content token.

When preparation needs an upload, LoonFS records the upload session before it writes the content object, and it completes the session afterward. A prepared upload has a deadline, which is no later than the expiry of the last token that the completed session could issue. A prepared upload or token is valid while the clock reads before its deadline, and it has expired at the deadline. The deadline is checked when the commit enters a batch, and again just before the numbered WAL write. Both checks use the request clock plus the time elapsed since the publication attempt began. Prepared inline bytes have no expiry.

If the final deadline check rejects a candidate, that candidate receives its own error, and the other accepted candidates receive `stale_head` so that the publisher plans them again. The rejected batch writes nothing.

Prepared content belongs to one namespace, and two namespaces can't share a prepared value. To import content into another namespace, a caller uses `prepare_content_ref`, and the result refers to the new copy owned by that namespace. To import a reference, the subject MUST be an administrator of the reference's owner namespace.

LoonFS checks an import's authorization against the owner namespace's current head. If the reference matches the owner's resident content, the import can read those bytes. Otherwise, the reference's owner namespace and content ID determine its object key. If that object is missing, the import returns `namespace_deleted` when the owner is deleted, or `namespace_corrupt` when the owner is active.

Why do prepared uploads have a deadline? The deadline matches the lifetime of remote upload tokens (see [Upload transport](#69-upload-transport) and [Upload-session cleanup](format.md#116-upload-session-cleanup)), and checking it prevents publication from referring to content that might have been reclaimed.

Why can't two namespaces share a prepared value? Uploads and garbage collection are tracked separately for each namespace.

### 5.1 Commit identity and preconditions

A commit is one request. It contains:

- A commit ID (`commit_id`), which is a stable idempotency key that the client generates. The commit ID MUST be reused verbatim for safe retries.
- An actor ID, sent in the `Loonfs-Actor` request header.
- An optional `message`, which is a human-readable annotation that's part of the commit's identity.
- An optional, ordered `preconditions` array.
- An ordered, non-empty list of path operations.

A request with one operation has the same shape as a request with many, so a convenience call and a one-element list are the same commit, and they have the same fingerprint.

A `message` is at most 4096 bytes. On every transport, a longer message is rejected with `invalid_request` before planning. A commit whose estimated encoded log would exceed one WAL object is rejected with `content_too_large` before publication, and the error message refers to the [WAL document limit](format.md#a5-wal-records).

The [semantic fingerprint](glossary.md#semantic-fingerprint) includes the preconditions in request order, so changing a precondition or its position changes the commit's identity. The fingerprint input always includes the precondition list, even when the list is empty.

The operations of one request commit together, in order, as one logical commit. Operation `k` is planned against the authoritative namespace state plus everything that operations `0..k` do, so a request can create a directory and write into it, or delete a path and recreate it.

Either every operation commits or none does. The first operation that fails aborts the whole request, and nothing that it or the operations before it would have written becomes visible. The error reports the position of the operation that stopped the request in `details.operation_index`. The error code is the failing operation's own code, and no error code is specific to batching.

The server plans each operation against the authoritative namespace state under the publish lock. For each operation, it generates the exact semantic checks that the operation implies (revision identity, binding identity, name absence, directory emptiness, and ancestor visibility). Each check is evaluated where its operation runs, which is what lets a later operation depend on an earlier one.

Callers add their own cross-request preconditions to the operations where staleness matters:

- A put can check the current inode and revision.
- A move or copy can check the destination inode and revision.
- Deletes, undeletes, inode-addressed writes, and namespace deletion have preconditions for their corresponding state.

Each precondition checks the state visible to its operation, including changes made by earlier operations in the same request.

Fields follow these naming conventions:

- Move and copy operations use `source_*` and `destination_*` for their endpoint fields. Their preconditions and the `moved` event use the same words.
- Undelete's optional restore location is `destination_path`.
- A field that identifies the addressed resource uses the resource's own name, such as `inode_id`, `namespace_id`, or `path`. No directional prefix is added for symmetry.
- A creation request can use `new_*` for the resource it creates.
- `target` describes an action subject, a persisted relationship, or a goal. It isn't an endpoint field in the move and copy family.
- `local` and `remote` refer to coordinate systems.

The server rejects unknown fields in a commit body, so a misspelled precondition can't be ignored. For example, a request that drops a letter from `expected_revision_no` returns `invalid_request` instead of applying the write without that precondition.

Every entry with a name includes a `binding_version`, an opaque token that identifies its current parent/name binding. Creating, moving, or undeleting an entry produces a new token, but content and attribute writes don't. Clients MUST NOT parse or order these tokens. A token is valid only for the namespace that issued it.

Inode-addressed moves and deletes require the token as `expected_binding_version`. A valid token that doesn't match the entry's current binding returns `binding_version_mismatch`. A malformed token, or a token from another namespace, returns `invalid_request`. The precondition is part of the commit's identity, and it's evaluated after any earlier operations in the same request. The `inode_binding` request precondition checks the same token for any operation.

The server validates each request against the authoritative namespace state, and it can reject the request immediately. A tentatively accepted request becomes one committed logical commit when the put-if-absent of the next WAL number succeeds. A failed put-if-absent precondition writes nothing, and the writer that lost discovers the new tip, plans again, and retries.

If the WAL put's outcome was never observed (a transport failure after the put was sent), the server reports `commit_outcome_unknown`, which means that the commit might already be visible. [Commit responses and safe retry](#52-commit-responses-and-safe-retry) defines how the caller resolves it.

The server can publish several logical commits in one WAL object put, but it MUST preserve each commit's idempotency, ordering, and change-feed identity.

Annotations can be used to correlate several logical commits that belong to one higher-level workflow, for example with fields such as `operation_id`, `operation_kind`, or `operation_part`.

The change feed returns committed changes, in order, after an explicit cursor. A caller can bound a response with `limit`. A truncated page returns `next_after_seq`, which the caller should use as the next request's `after_seq`. If the requested cursor is older than the retention floor, the caller MUST re-bootstrap instead of expecting older incremental history to remain available.

Why does the server generate semantic checks for every operation? The checks make races fail explicitly instead of silently merging.

### Request-level preconditions

The `preconditions` array is optional and defaults to an empty list. It can have at most 1024 entries, as advertised by `commit.max_preconditions`, and a request with more entries returns `invalid_request` before planning.

Request-level preconditions are checked against the candidate's "pre-state". The pre-state includes the earlier admitted candidates in the batch, and none of this candidate's operations. Its head sequence is the sequence of the last admitted commit, or the batch's base head sequence for the first admission.

| Precondition | What it checks | Error on failure |
| --- | --- | --- |
| `namespace_head` | The namespace's head sequence | `stale_head` |
| `file_revision` | An inode's current content revision | `stale_revision` |
| `path_binding` | The inode that a path resolves to, and optionally its binding version | `path_conflict` or `binding_version_mismatch` |
| `inode_binding` | An inode's binding version | `binding_version_mismatch` |
| `path_absence` | That no visible entry resolves at a path | `path_conflict` |
| `name_absence` | That a parent has no visible child with a name | `path_conflict` |
| `attributes_revision` | An inode's attribute revision | `stale_attributes` |
| `access_revision` | An inode's access revision | `stale_access` |

`namespace_head` requires `expected_head_seq` to equal the pre-state head sequence. Any intervening commit to the namespace invalidates it, including unrelated writes. A failure returns `stale_head` with `expected_head_seq` and `actual_head_seq`.

`file_revision` requires a visible `inode_id` whose current content revision equals `expected_revision_no`. Any new revision of that inode (including a restore) invalidates it, and so does deletion. A failure returns `stale_revision`, and its `actual_revision_no` is absent when the inode isn't visible or has no current revision.

`path_binding` requires an absolute `path` and `expected_inode_id`. A missing or null `expected_inode_id` is a decode error. The precondition passes only when the path resolves to that inode. A different inode, or an unbound path, returns `path_conflict` with `expected_inode_id` and `actual_inode_id`, and `actual_inode_id` is absent when the path is unbound. Without a binding version, the precondition passes again when the path returns to the same inode. The optional `expected_binding_version` also detects moves away and back, and a binding version mismatch returns `binding_version_mismatch`. Checking that the root is bound to `ino_1` passes without a binding version. The root has no binding version, so supplying one returns `invalid_request`.

`inode_binding` requires `inode_id` and `expected_binding_version`. It passes when the inode is visible and still bound at that version, wherever its path now leads. A mismatch returns `binding_version_mismatch`, and its `actual_binding_version` is absent when the inode isn't visible. A malformed token, a token from another namespace, or the root inode (which has no binding version) returns `invalid_request`.

`path_absence` requires only an absolute `path`, and it rejects inode or binding version fields. It passes when no visible entry resolves at the full path, including when an ancestor is missing or an intermediate component isn't a directory. A bound path returns `path_conflict` with `actual_inode_id` set and `expected_inode_id` absent. A `path_absence` for `/` fails with an actual inode of `ino_1`.

`name_absence` requires `parent_inode_id` and `display_name`. It passes when the parent has no visible child whose name key matches the name, including when the parent isn't visible or isn't a directory. A bound name returns `path_conflict` with `actual_inode_id` set and `expected_inode_id` absent.

`attributes_revision` requires a visible `inode_id` whose attribute revision equals `expected_attributes_revision_no`. Any attribute update invalidates it, and so does deletion, but content-only rewrites don't. A failure returns `stale_attributes`, and its `actual_attributes_revision_no` is absent when the inode isn't visible.

`access_revision` requires a visible `inode_id` whose access revision equals `expected_access_revision_no`. Any access update invalidates it. A failure returns `stale_access`, and its `actual_access_revision_no` is absent when the inode isn't visible.

The server checks a request in this order:

1. It resolves the commit receipt. If an identical request has already committed, the server returns its original commit, even when its precondition is stale. Reusing that commit ID with different preconditions returns `commit_id_reuse_conflict`.
2. It runs the preconditions in order. The first failure returns its kind's error, with the zero-based `precondition_index`.
3. It runs the operations.

A failed precondition reserves no sequence or inode, and it leaves the planning view unchanged. A retry after a lost numbered WAL put evaluates the preconditions again against the new basis. Preconditions are admission conditions. They're stored only through the fingerprint in WAL records and commit rows, and they're never evaluated during replay.

### Actor attribution

Every commit, namespace creation, namespace fork, and administrator recovery requires an actor in the `Loonfs-Actor` header (see [Identity headers](#identity-headers)). The application supplies the actor as a stable, opaque identifier of 1 to 256 visible ASCII characters (0x21 through 0x7E), with the identity scope that it needs. LoonFS records the actor on the commit and on the metadata created by that commit. Namespace creation and forking record it as `created_by` on the namespace.

LoonFS doesn't authenticate the actor or resolve profile information. The application MUST authenticate the user and authorize the operation before it sends the request. The application should use a stable internal ID, not an email address or display name.

The header's value is the semantic commit fingerprint's `actor_id`. Reusing a `commit_id` with a different actor fails with `commit_id_reuse_conflict`. The commit timestamp isn't part of the fingerprint.

Responses report attribution in these fields:

| Field | Meaning |
| --- | --- |
| `created_by`, `created_at_ms` | Commit attribution for inode creation. |
| `created_by`, `created_at_ms` on a namespace | Who created or forked the namespace, and when. |
| `revision_committed_by`, `revision_committed_at_ms` | Commit attribution for the current file revision, on stat and list entries. Absent on directories. |
| `commit_id` | The identity of the commit that an item belongs to, on revision-history items and committed changes. |
| `committed_by`, `committed_at_ms` | Commit attribution on revision-history items and committed changes. |
| `attributes_updated_by`, `attributes_updated_at_ms` | Commit attribution for the latest stored attribute update. Absent for the initial empty attributes at revision 0. |
| `deleted_by`, `deleted_at_ms` | Commit attribution for an active trash entry. |

### Subject and principals

A request can act as a subject. Three headers form the "subject context" that describes the subject:

- `Loonfs-Subject` identifies the subject that the request acts as.
- `Loonfs-Principal-Scope` identifies the domain that issued the subject's principal IDs.
- `Loonfs-Principals` contains comma-separated principal IDs, without whitespace. The advertised `access.max_principals_per_request` limit caps the number of principals.

Principal, subject, and scope IDs never contain a comma, so the list is unambiguous.

Without a subject context, a request acts with the token holder's service authority, except on the operations that require a subject context in an ACL namespace. The [identity header table](#identity-headers) lists those operations, and it describes how the subject ID falls back to `Loonfs-Actor`. If the subject scope differs from the namespace's `principal_scope`, the request returns `forbidden` before any grant is read, and the error reports the expected and actual scopes.

These IDs are opaque, and like the actor ID, they appear in access logs. The application should use internal IDs, never email addresses or display names.

The subject ID is part of a commit's semantic identity. A retry of a commit ID from another subject returns `commit_id_reuse_conflict`. The scope isn't part of the fingerprint or of upload ownership.

An embedded caller sets the subject once, on the handle that it uses. Every read, commit, and upload through that handle acts as that subject, and the commit and upload methods don't take a separate subject. The CLI records the handle's subject in its upload journal next to the request, and it refuses to resume the upload under a different subject.

Why isn't the scope part of the fingerprint or of upload ownership? A namespace has one scope, and LoonFS refuses subjects from every other scope.

### Identity headers

Four request headers contain identity information. `Loonfs-Actor` is for attribution, and its value is an actor ID of 1 to 256 visible ASCII characters (0x21 through 0x7E). `Loonfs-Subject`, `Loonfs-Principal-Scope`, and `Loonfs-Principals` form the subject context that an ACL namespace authorizes against (see [Subject and principals](#subject-and-principals)). All four headers are optional in the transport schema. The server enforces each operation's own requirements while it handles the request:

| Operations | `Loonfs-Actor` | Subject context |
| --- | --- | --- |
| `create_commit` | Required. Recorded as `committed_by`. | Required in an ACL namespace. |
| `create_namespace` | Required. Recorded as `created_by`. | Not read. |
| `fork_namespace` | Required. Recorded as `created_by`. | Optional. When it's present, an ACL source requires an administrator subject. When it's absent, the request acts as the token holder. |
| `delete_namespace`, `create_snapshot`, `list_snapshots`, `extend_snapshot`, `delete_snapshot`, `list_changes` | Not required. | Optional, with the same administrator rule as `fork_namespace`. |
| Path and inode reads, downloads, `list_trash`, `grep`, and upload-session operations | Not required. | Required in an ACL namespace. |
| `run_maintenance` | Required by `recover_administrator`, which records it as `committed_by` on the recovery commit. Other jobs don't record it, but they still reject a malformed value. | Not read. |
| `get_capabilities`, `get_namespace`, the other maintenance operations, and the health, readiness, and metrics routes | Not read. | Not read. |

Where an operation reads the subject context:

- `Loonfs-Principals` and `Loonfs-Principal-Scope` are sent together. A request with `Loonfs-Subject` or the scope, but without principals, returns `invalid_request` with `param` `Loonfs-Principals`. A request with principals but without the scope returns `invalid_request` with `param` `Loonfs-Principal-Scope`.
- The subject ID is `Loonfs-Subject` when it's present, and `Loonfs-Actor` otherwise. A request with neither returns `invalid_request` with `param` `Loonfs-Subject`. An actor used as the subject ID MUST also be a valid subject ID, which can't contain a comma. If it isn't, the `param` is `Loonfs-Actor`.
- An operation that doesn't require an actor accepts a complete explicit subject (`Loonfs-Subject`, scope, and principals) without an actor.
- In an unrestricted namespace, the server checks no rights, but it still rejects an incomplete or malformed subject context. A commit's subject ID is part of its fingerprint in either access mode.
- In an ACL namespace, an operation that requires the subject context returns `invalid_request` with `param` `Loonfs-Principals` when the subject context is absent. A subject scope that differs from the namespace's `principal_scope` returns `forbidden`.

A missing required `Loonfs-Actor` returns `invalid_request` with `param` `Loonfs-Actor` and the message `missing required header Loonfs-Actor`. A malformed actor returns `invalid_request` with the same `param` and the validator's reason. Authorization is checked first.

Header names are case-insensitive, and HTTP/2 sends them in lowercase. Retries and replayed requests MUST include the same header values, so saved requests MUST keep the actor and subject context next to the body. Request headers appear in access logs, so every value MUST be an opaque identifier, never an email address or a display name.

### 5.2 Commit responses and safe retry

Every successful commit returns a `Commit` with its `namespace_id`, `commit_id`, and `committed_seq`. The response also includes `committed_by`, `committed_at_ms`, the optional `message`, and `events`. These values match the change feed, including values such as newly created inode IDs. If the caller omits an optional `commit_id`, the response includes a generated ID.

To retry a commit, a client resubmits the same request with the same `commit_id`:

- If the original request committed, the response includes the original `committed_seq`, and no new commit is made.
- If the original request didn't commit, the request can commit on this attempt.
- If the ID was used for a different request, the retry returns `commit_id_reuse_conflict`. Changing a write precondition counts as a different request, even if the operations are unchanged.

A new `commit_id` means a new commit. After `commit_outcome_unknown`, a transport failure, or a process restart, the client retries with the original ID and request. There's no separate commit-status lookup.

These retry rules apply while the [commit receipt](glossary.md#commit-receipt) is kept. Once the retention floor passes the commit and the metadata runs are rebuilt, the receipt is removed. The same ID can then be used for a new commit, so a late retry might apply the operation again. For example, a replacing put might append another revision with the same content. An application that retries beyond this window MUST check whether the original operation took effect.

A replay returns the original response, including `events`, from the retained commit record. A reuse conflict also reports the original `committed_seq` when the conflict was resolved from a receipt.

A commit can refer to an upload, or it can include the file's bytes directly (inline content). Content is part of the request's identity, and the retry rules depend on how the content was supplied:

| Content supplied with the commit | What to retain for a retry |
| --- | --- |
| Uploaded content | The original `content_ref`. Uploading the same bytes again creates a new object, and using that object's reference with the original commit ID returns `commit_id_reuse_conflict`. |
| Inline content | The original bytes. Different bytes return `commit_id_reuse_conflict`, even if the file size is unchanged. |

The inline retry rules still apply if the bytes are written to a separate content object to stay within WAL limits. Switching between inline bytes and an uploaded reference changes the request's identity, even for identical content.

If the original commit succeeded and its receipt is still available, retrying the same request returns the original result without uploading the file again. This also works after a restart or on another server. Changed bytes or a different subject still return `commit_id_reuse_conflict`.

With prepared content, a client prepares the content once, keeps the result, and reuses it with the same explicit commit ID, path, actor, and options on each attempt. This works for both inline content and completed uploads.

| Client | Prepare content | Publish retained content |
| --- | --- | --- |
| Rust HTTP and embedded runtime | `prepare_content()` / `prepare_content_stream()` | `put_file_prepared()` |
| Python synchronous client | `files.prepare()` | `files.upload_prepared()` |
| Go | `Files.Prepare()` | `Files.UploadPrepared()` |
| TypeScript server and browser clients | `files.prepare()` | `files.uploadPrepared()` |

In the embedded runtime, preparing content at or below the enabled inline threshold makes no storage request. The HTTP clients prepare inline content when the server advertises `filesystem.commits.inline_content` and the actual file bytes fit within `commit.max_inline_content_bytes_per_operation`. The Go, Python, and TypeScript helpers keep at most 64 KiB inline, even if the server advertises a larger limit. Streams use bounded lookahead (the effective limit plus one byte), and larger sources continue through the existing upload path with that prefix preserved.

The Rust HTTP client caches capabilities, so once its cache is populated, writing a small file needs only the commit HTTP request. The generated SDK helpers still retrieve capabilities during preparation, but they skip upload creation, payload transfer, and completion for inline content.

In Go, Python, and TypeScript, preparation returns a `PreparedFile`, which is either the existing staged `PreparedContent` or an `InlinePreparedContent`. The staged type's constructor and its reference and token fields are unchanged. A caller that inspects those fields MUST first narrow the value to the staged variant. A caller that keeps the result and passes it to `upload_prepared`, `UploadPrepared`, or `uploadPrepared` needs no transport-specific logic. Inline values contain immutable bytes (base64 in Go and TypeScript), and they have no uploaded reference before publication. An application that explicitly needs a completed upload can still use the lower-level uploads API.

Publication uses the retained variant. It doesn't check capabilities again, and it doesn't fall back to another representation after an error. A lost response MUST be retried with the original prepared value and commit ID.

Prepared inline bytes have no expiry. Completed uploads keep their normal expiry, and preparing content doesn't extend it. Preparation doesn't make a file visible, so a commit is still required.

The whole-file convenience methods (`files.upload`, `files.uploadStream`, `files.upload_stream`, `Files.Upload`, `Files.UploadStream`, `put_file()`, and `put_file_stream()`) prepare content on every call. Small files can be prepared inline, so another call with the same bytes, commit ID, and options can replay the original commit if the content is still prepared inline on that call. Larger files, and files prepared with inline writes disabled, create new objects. When a new object is created, reusing the original commit ID conflicts, even for identical bytes. A capability change can also change the representation, and keeping the prepared value avoids that risk. An unused upload can be reclaimed after its grace period.

For reliable retries, a client uses the preparation methods above and keeps the complete commit request. The remote CLI saves this request before it submits it, so that it can resend the request after a process restart. If a helper call omits `commit_id`, each call generates a new one. The actor can be configured once on the client.

At the WAL write-stop threshold, new commits return `maintenance_required`. Retries of retained commits can still return their original commits, including after `commit_outcome_unknown` or `deadline_exceeded`. Writer-session, availability, and corruption checks still apply.

The blocking Rust client automatically retries operations labeled `idempotent` or `replayable`. It makes one attempt for operations labeled `not_idempotent`:

- namespace create, fork, and delete
- upload-session begin
- checkpoint create
- maintenance
- grep index collection
- store probe

The client also makes one attempt for a presigned direct PUT.

### 5.3 Writer topology and fencing

Each namespace has one active [writer session](glossary.md#writer-session). Many concurrent clients can submit commits through that session, and independent readers scale separately. The reference server works this way, with one service-level writer session that coordinates every client request.

Replacing the active writer isn't an approval flow. A writer that opens and publishes acquires the next [writer epoch](glossary.md#writer-epoch). Epoch fencing (not liveness, and not a lease) is what keeps the displaced session from corrupting anything, because the displaced session's next publish fails with `writer_fenced`, and the failure is terminal for that session. The error's `details` identify the epoch and the writer that displaced the session, so an operator can tell a planned failover apart from two writers that are misconfigured against one namespace.

The host controls the lifetime of each writer session. A session opens when the host opens a namespace for writing, and it stays open until the host closes it, the host drops it, or another writer fences it. The runtime never opens a session on its own, and it never closes one to make room for another. It doesn't cap how many sessions exist. Opening a session writes nothing durable, and the session's first publish acquires the next writer epoch.

A node closes a session to hand a namespace off. Closing refuses new work at once, lets admitted work publish, and writes nothing durable. The next node's first publish acquires the next writer epoch. A request that reaches a session after its close began fails with `writer_session_closed`.

The reference server opens a namespace's session on the first request that writes to the namespace. Its maintenance sweep closes a session when all of these conditions are true:

- The session's last open is older than `idle_session_timeout_ms`, which is 1800000 ms (30 minutes) by default.
- No request has a clone of the session's handle.
- The session is caught up. Metadata, and the grep index where it's maintained, are caught up with the session's published sequence. A session that never published through this process closes under the same idle rule. It has no published sequence to catch up with, but an enabled index has to finish building first.
- Collection isn't due.

While the sweep closes a session:

- An open of the same namespace waits for the drain, and then opens a fresh session. The new session's first publish acquires a new writer epoch.
- An open that's waiting doesn't fail with `writer_session_closed` from this path.
- Opens of other namespaces don't wait for this close.

The server also drops a session when the namespace is deleted, when a request finds that the namespace doesn't exist, or when the session is fenced. When a session is fenced:

- The request that finds the fence fails with `writer_fenced`, and the server doesn't resubmit it.
- The session is dead, including every clone of it that a request still in flight is using.
- A later request opens a new session, and that session's first publish takes the namespace back. Until the old session's work ends, an open can return `writer_session_closed` (503). A later request can open a session after that work ends.
- A fence on one namespace doesn't affect the sessions for other namespaces.
- Each dropped fenced session produces a warning with the namespace, and with the winning writer ID when the error includes it. The host table keeps a cumulative count of these drops. Typed hosts call `Namespaces::forget_if_fenced` after a failure, and they read `Namespaces::fenced_sessions_dropped` for the count.

Sustained fencing of one namespace means that two writers are being sent its traffic, and the namespace should be routed to one writer.

[Operations and WAL deltas](format.md#66-operations-and-wal-deltas) defines the standard mutation operations, and [`POST /commits`](#68-post-commits) exposes those operations over HTTP. The same identity, durability, and visibility rules apply to every API that implements them.

## 6. Representative HTTP binding

This section describes the representative v0 HTTP binding: the naming conventions for routes and operation IDs, how requests are authenticated and decoded, how each operation can be retried, and the shape of each operation's requests and responses. HTTP is one transport binding for the abstract operations defined earlier in this document, and it doesn't define their semantics.

GET routes identify resources, so their paths use nouns such as `entry`, `entries`, `content`, `revisions`, and `trash`. A POST route ends in a verb when it invokes an action instead of creating a resource, as in `enable`, `disable`, `abort`, `complete`, and `probe`.

`/v0/maintenance/` is the only API group prefix, and routes under it belong to the `maintenance/v0` API group. Other routes are grouped by resource, including `GET /v0/namespaces/{ns}/grep`, which belongs to the `query/v0` group. Every other route shown in this section belongs to `filesystem/v0`.

Operation IDs start with a verb. `get` reads one resource, `list` reads a page, and `create` posts a new resource to a collection. Other verbs describe the operation directly, as in `grep`, `run_maintenance`, and `delete_checkpoint`. Operation IDs are identifiers in the wire registry, and they don't determine SDK names. Generated SDK group and method names come from the SDK naming table in the OpenAPI postprocessor, and every operation MUST appear in that table or be explicitly excluded from it.

The examples in the following sections are representative, not exhaustive. Responses might gain fields within v0, and clients MUST ignore JSON fields they don't recognize. An optional response field that has no value is left out, and it's never encoded as `null`.

### Authentication and transport

If a deployment sets a token, the server authenticates every request with an HTTP bearer credential:

```
Authorization: Bearer <auth_token>
```

After request admission, a request without the token, or with the wrong value, receives 401 `unauthorized`. The server checks authorization before it parses the rest of the request, so a malformed body from an unauthenticated caller still receives 401, not 400.

Why check authorization first? A validation error could reveal information to a caller that isn't authorized, so the server reports `unauthorized` before it reports anything about the request itself.

Two routes, `GET /health` and `GET /readiness`, don't require the token and always respond to unauthenticated requests, because load balancers probe them. Every other route requires the token, including `GET /v0/capabilities`. The generated `openapi.json` describes this as a global `bearer_auth` requirement, which those two operations override.

The identity headers `Loonfs-Actor`, `Loonfs-Subject`, `Loonfs-Principal-Scope`, and `Loonfs-Principals` are optional in the transport schema. The [identity header table](#identity-headers) lists which headers each operation requires.

The server rejects a request body that contains an unknown field, at any level of nesting, with 400 `invalid_request`. Most request fields are optional, and several of them are preconditions, so the server can't ignore a field it doesn't recognize. If it did, a misspelled precondition would decode to its default, and the server would perform a different request than the one the caller asked for.

Response bodies follow the opposite rule. A client MUST keep working against a server newer than itself, so it MUST tolerate response fields it doesn't recognize (see [Writer and client requirements](#72-writer-and-client-requirements)).

The exception is `ContentRef` and `Checksum`, which are closed shapes in both requests and responses. A response never adds a field to either shape. A new content strategy is added as a new `kind` value, and a new checksum algorithm as a new `algorithm` value, so both schemas are `additionalProperties: false` in responses too. An unknown content kind fails to decode, and so does an unknown checksum algorithm. [Strict decoding and evolution](format.md#123-strict-decoding-and-evolution) states the same rules and extends them to durable shapes.

The server also rejects unknown query parameters. For example, `DELETE /v0/namespaces/{ns}?expected_head_sq=418` (a misspelling of `expected_head_seq`) returns 400 `invalid_request`, instead of deleting the namespace without the intended precondition. A route that declares no query parameters rejects every query parameter. `GET /health`, `GET /readiness`, and `GET /metrics` are exceptions, because probes and scrapers might append their own parameters.

The token is a bearer credential, and so is everything the upload routes return. For example, a presigned direct-upload URL is a capability to write to the deployment's bucket, and it's returned in an ordinary response body. Anyone who can read the connection can read both. So a deployment that's reachable beyond localhost needs to serve `https`, terminated either by the server itself or by a proxy in front of it.

### Operations and retry classes

Every operation includes an `x-loonfs-retry` value for generated SDKs. The value is one of three retry classes:

- `idempotent`: a client can safely repeat the request.
- `replayable`: repeating the request returns the original result. A repeated commit is replayed by its `commit_id`, and a repeated upload completion returns the first completion's result.
- `not_idempotent`: a generated client makes one automatic attempt and leaves recovery to the application.

The following table lists every v0 operation, with its retry class and representative HTTP shape.

| Purpose | Operation ID | Retry class | Representative HTTP shape |
| --- | --- | --- | --- |
| Check server health | `get_health` | `idempotent` | `GET /health` |
| Check server readiness | `get_readiness` | `idempotent` | `GET /readiness` |
| Read deployment capabilities | `get_capabilities` | `idempotent` | `GET /v0/capabilities` |
| Create a namespace | `create_namespace` | `not_idempotent` | `POST /v0/namespaces`. Requires the `Loonfs-Actor` header, and accepts the optional `access` and `naming` body fields. |
| Read a namespace | `get_namespace` | `idempotent` | `GET /v0/namespaces/{ns}` |
| Read a path entry | `get_path_entry` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/entry?path=/docs/report.txt&include_attributes=false&snapshot_id=...` (`include_attributes` is optional and defaults to `true`, and `snapshot_id` is optional) |
| Read an inode | `get_inode` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}?include_attributes=false&snapshot_id=...` (`include_attributes` is optional and defaults to `true`, and `snapshot_id` is optional) |
| List path entries | `list_path_entries` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/entries?path=/docs&limit=100&cursor=...&include_attributes=true&snapshot_id=...` (`include_attributes` is optional and defaults to `false`, and `snapshot_id` is optional) |
| List directory children by inode | `list_inode_children` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}/children?limit=100&cursor=...&include_attributes=true&snapshot_id=...` (`include_attributes` is optional and defaults to `false`, and `snapshot_id` is optional) |
| List file revisions by path | `list_file_revisions` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/revisions?path=/docs/report.txt&limit=100&cursor=...` |
| List file revisions by inode | `list_file_revisions_by_inode` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}/revisions?limit=100&cursor=...` |
| Read current or prior file content by path | `get_file_bytes` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/content?path=/docs/report.txt&snapshot_id=...` (`revision_no` and `snapshot_id` are optional and mutually exclusive) |
| Read current file content by inode | `get_file_bytes_by_inode` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}/content?snapshot_id=...` (`snapshot_id` is optional) |
| Read prior file content by inode | `get_file_revision_bytes_by_inode` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{revision_no}/content` |
| Start a download by path | `create_download` | `idempotent` | `POST /v0/namespaces/{ns}/filesystem/downloads` with body fields `path`, optional `revision_no`, optional `snapshot_id`, and optional `start_offset` (`snapshot_id` can't be combined with `revision_no`, and `start_offset` defaults to 0) |
| Start a current download by inode | `create_download_by_inode` | `idempotent` | `POST /v0/namespaces/{ns}/inodes/{inode_id}/downloads?snapshot_id=...&start_offset=...` with no body (`snapshot_id` and `start_offset` are optional) |
| Start a revision download by inode | `create_revision_download_by_inode` | `idempotent` | `POST /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{revision_no}/downloads?start_offset=...` with no body (`start_offset` is optional) |
| List recoverable deletions | `list_trash` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/trash?limit=100&cursor=...` |
| Create a commit | `create_commit` | `replayable` | `POST /v0/namespaces/{ns}/commits`. Requires the `Loonfs-Actor` header. |
| Create an upload session | `create_upload` | `not_idempotent` | `POST /v0/namespaces/{ns}/uploads`. Returns the open session. |
| Upload content through the server | `put_upload_content` | `idempotent` | `PUT /v0/namespaces/{ns}/uploads/{upload_id}/content`. Returns the open session with the staged `content_ref`. |
| Create multipart upload URLs | `sign_upload_parts` | `idempotent` | `POST /v0/namespaces/{ns}/uploads/{upload_id}/parts` |
| Complete an upload | `complete_upload` | `replayable` | `POST /v0/namespaces/{ns}/uploads/{upload_id}/complete` |
| Read an upload session | `get_upload` | `idempotent` | `GET /v0/namespaces/{ns}/uploads/{upload_id}`. A completed session returns a new `content_token`. |
| Abort an upload session | `abort_upload` | `idempotent` | `POST /v0/namespaces/{ns}/uploads/{upload_id}/abort` (terminal and repeatable, and a completed session can't be aborted) |
| Read committed changes | `list_changes` | `idempotent` | `GET /v0/namespaces/{ns}/changes?after_seq=123&limit=100&snapshot_id=...` (`snapshot_id` is optional) |
| Create a snapshot | `create_snapshot` | `not_idempotent` | `POST /v0/namespaces/{ns}/snapshots`. Requires `name` and `ttl_ms`. |
| List snapshots | `list_snapshots` | `idempotent` | `GET /v0/namespaces/{ns}/snapshots?limit=100&cursor=...` |
| Extend a snapshot | `extend_snapshot` | `idempotent` | `POST /v0/namespaces/{ns}/snapshots/{snapshot_id}/extend`. Requires `ttl_ms`, and clamps the expiry to the lifetime ceiling. |
| Delete a snapshot | `delete_snapshot` | `not_idempotent` | `DELETE /v0/namespaces/{ns}/snapshots/{snapshot_id}` (deletes the pin, and a missing ID returns `snapshot_not_found`) |
| Fork a namespace | `fork_namespace` | `not_idempotent` | `POST /v0/namespaces/{source_ns}/forks`. Requires the `Loonfs-Actor` header. |
| Delete a namespace | `delete_namespace` | `not_idempotent` | `DELETE /v0/namespaces/{ns}?expected_head_seq=418` (requires the `filesystem.namespaces.delete` feature, and the precondition is optional) |
| Read namespace diagnostics | `get_namespace_diagnostics` | `idempotent` | `GET /v0/maintenance/namespaces/{ns}/diagnostics` |
| Create a checkpoint | `create_checkpoint` | `not_idempotent` | `POST /v0/maintenance/namespaces/{ns}/checkpoints`. Requires `name`, and accepts `ttl_ms`. |
| List checkpoints | `list_checkpoints` | `idempotent` | `GET /v0/maintenance/namespaces/{ns}/checkpoints?limit=100&cursor=...` |
| Delete a checkpoint | `delete_checkpoint` | `not_idempotent` | `DELETE /v0/maintenance/namespaces/{ns}/checkpoints/{checkpoint_id}` (deletes the pin, and a missing ID, or the ID of a snapshot or fork pin, returns `checkpoint_not_found`) |
| Run one maintenance job | `run_maintenance` | `not_idempotent` | `POST /v0/maintenance/namespaces/{ns}/runs`. The body identifies one job with `kind`. |
| Search file contents | `grep` | `idempotent` | `GET /v0/namespaces/{ns}/grep?pattern=needle&case_insensitive=false&path_prefix=%2Fsrc&allow_scan=false&allow_stale=false&limit=100&cursor=...`. Requires the `query.grep` feature and an active index. |
| Read grep index status | `get_grep_index` | `idempotent` | `GET /v0/maintenance/namespaces/{ns}/grep/index` |
| Enable the grep index | `enable_grep_index` | `idempotent` | `POST /v0/maintenance/namespaces/{ns}/grep/index/enable` (idempotent) |
| Disable the grep index | `disable_grep_index` | `idempotent` | `POST /v0/maintenance/namespaces/{ns}/grep/index/disable` (idempotent) |
| Test object storage | `probe_store` | `not_idempotent` | `POST /v0/maintenance/store/probe` with body `{}` |
| Scrape metrics | `get_metrics` | `idempotent` | `GET /metrics` (Prometheus text exposition. Unlike the liveness routes, it requires the token, as described in [`GET /metrics`](#614-get-metrics).) |

### Maintenance runs

This section describes the grep index routes, and the jobs that `run_maintenance` runs.

#### Grep index

Grep index garbage collection runs through `run_maintenance`, with the request body `{"kind":"grep_gc"}`. Each call reads the durable roots and completes one pass over the namespace's grep manifests and segments. The response includes `namespace_id`, `deleted_segments`, `deleted_other_objects`, `namespace_reaped`, and `retained_candidates`. If a root is unreadable or invalid, the call fails before it deletes anything. For a tombstoned or absent namespace, the pass reaps the namespace's aged grep prefix. [Grep extension format](format.md#appendix-d-grep-extension-format) defines the retention and age rules.

The status, enable, and disable routes all return the same flat grep index object. It contains `namespace_id`, the lifecycle fields for its `status`, `next_run_no`, and `reorganize_pending`. No two lifecycle statuses share a sequence field:

| `status` | Fields | Means |
| --- | --- | --- |
| `disabled` | None | No index is maintained for the namespace. This is also the status of a namespace that has never enabled an index. |
| `backfilling` | `captured_seq`, `cursor_inode_id`, `checkpoint_id` | The initial walk over a pinned checkpoint is running. `captured_seq` is the namespace sequence that the checkpoint captured, and the backfill is complete when the walk reaches it. Nothing is searchable yet, and there's no watermark to report. |
| `active` | `built_through_seq`, `next_event_index` | The index follows the change feed. Commits at or below `built_through_seq` are searchable, except when `next_event_index` is non-zero. In that case, the rest of the commit at `built_through_seq` isn't indexed yet. |

For example:

```json
{"namespace_id":"demo","status":"active","built_through_seq":12,"next_run_no":3,"reorganize_pending":false}
```

```json
{"namespace_id":"demo","status":"backfilling","captured_seq":12,"cursor_inode_id":"ino_4","checkpoint_id":"pin_00000000000000000009-0000000000000009","next_run_no":1,"reorganize_pending":false}
```

So a backfilling index never reports a `built_through_seq`, and an active index never reports a `captured_seq`. `next_run_no` is the run number that the index allocates next, and `reorganize_pending` reports whether a partitioned segment reorganization is in progress. A client that waits for the index to catch up captures one sequence before it starts waiting, and it stops waiting when the index reaches that sequence, instead of chasing a head that keeps moving.

#### Jobs and outcomes

A maintenance run request body identifies exactly one job with `kind`:

| `kind` | Fields | Result |
| --- | --- | --- |
| `metadata` | Optional `max_wal_tail_objects` | `wal_fold` and `compaction` outcomes |
| `metadata_compaction` | None | `compaction`, tagged by `outcome`. A published outcome includes the manifest number and the row, byte, and segment counts. |
| `gc` | Optional `grace_window_ms` | The collection result |
| `grep_gc` | None | `deleted_segments`, `deleted_other_objects`, `namespace_reaped`, and `retained_candidates` |
| `retention` | Optional `to_seq` or `cutoff_at_ms` | `retention_floor_seq` |
| `recover_administrator` | `principal_id` | `commit_id`, `committed_seq`, and the root row's new `access_revision_no`. The job grants `admin` on the root row to the principal and keeps every other root grant. It makes the grant through a commit that doesn't check a subject and is attributed to the request's `Loonfs-Actor`. The job is meant for an ACL namespace that has lost every administrator. |

The response includes the same `kind`, the addressed `namespace_id`, and the job's result. None of the jobs creates a pin. For example, this request body runs one GC pass:

```json
{"kind":"gc"}
```

A run that loses a race with another process, or whose work is superseded, reports that as an outcome in its result, not as an error.

A deleted namespace accepts only `gc` and `grep_gc` runs, and other jobs return `namespace_deleted`. [Retirement](format.md#95-retirement) defines how a deleted namespace is retired.

`wal_fold.outcome` reports the result of the WAL fold. It has four values:

- `not_needed`: the WAL tail was below the threshold and hadn't gone idle (see the `metadata` options below).
- `folded`: this step published the next current manifest.
- `already_published`: the current manifest already covered the captured WAL tail, so this step published no manifest.
- `retries_exhausted`: concurrent updates prevented every attempt from publishing. Nothing was folded, and a later step can try again.

In a `metadata` result, `compaction.outcome` reports the bounded compaction step. It has five values:

| `outcome` | Means |
| --- | --- |
| `not_needed` | No bounded merge is due. |
| `unit_published` | This run published one bounded merge. |
| `metadata_compaction_required` | A family group's window is too large for one bounded merge. The caller runs the `metadata_compaction` job instead, which compacts the group by streaming. |
| `manifest_advanced` | Another publisher changed the current manifest first. The segments this run wrote stay unreferenced, and a later GC pass can delete them. |
| `fenced` | A newer runtime has the compactor epoch. |

The bounded step in a `metadata` run never runs a streaming compaction. Only the `metadata_compaction` job does.

A `metadata_compaction` run compacts one unit. If the selected window fits in one step, the unit is a bounded merge, and otherwise it's one streaming compaction of a family group. To compact every eligible group, a caller repeats the run for as long as it publishes.

In a `metadata_compaction` result, `compaction.outcome` has six values:

| `outcome` | Means |
| --- | --- |
| `not_needed` | No family group has eligible input. |
| `bounded_merge_published` | The planner selected and published a bounded merge. |
| `published` | The outcome includes the published manifest number and the row, byte, and segment counts. |
| `cancelled` | The caller cancelled the job. |
| `abandoned` | An input run changed, the elapsed-time bound was exceeded, or every publication attempt lost. |
| `fenced` | Another process advanced the manifest's compactor epoch. |

The `cancelled`, `abandoned`, and `fenced` outcomes publish no manifest.

In a `metadata` run, `max_wal_tail_objects` overrides the fold threshold. A value of zero, or a value above the write-rejection threshold, returns `invalid_request`. A WAL tail below the threshold is still folded once it goes idle, which means that its newest commit is as old as the server's configured idle period (15 minutes by default), measured on the server's clock (see [Recovery material and maintenance policy](format.md#73-recovery-material-and-maintenance-policy)). A server can turn this rule off. Replay history is kept unless the request uses `kind: "retention"`.

#### Retention runs

In a `retention` run, the floor moves to the folded manifest head, unless the request includes one target:

- `to_seq` moves the floor to that sequence, or to the folded manifest head if the sequence is above it.
- `cutoff_at_ms` is a Unix time in milliseconds, and it moves the floor to the last commit committed at or before that time. The server reads commits upward from the current floor, and it stops at the first commit whose `committed_at_ms` is later than the cutoff, because commit times come from writers' clocks and aren't guaranteed to increase with sequence.

A request that includes both targets returns `invalid_request`. A target at or below the current floor changes nothing, and the response reports the current floor (see [Advancing the retention floor](format.md#101-advancing-the-retention-floor)).

The retention floor limits incremental replay and file revision history. Above the floor, a file's revision history is complete. At or below the floor, each file keeps its newest revision. Once the floor advances, a base compaction removes the older revisions, so a revision listing ends at that newest revision, and reading or restoring a removed revision returns `revision_not_found`. A deleted file keeps its newest revision, so an undelete still restores its content. Recoverable deletions in the trash are never pruned. They stay until the namespace is deleted and retired. GC reclaims the content object of a removed revision once no retained view references it and it's older than the grace window (see [Content roots](format.md#119-content-roots)).

#### Garbage collection runs

In a `gc` run, `grace_window_ms` overrides the grace window. A grace window below the derived safety floor returns `invalid_request`. Upload sessions still keep their leases, and completed content still keeps its derived reclamation grace (see [Upload-session cleanup](format.md#116-upload-session-cleanup)).

A GC response contains counts for that call. Concurrent calls can overlap their deletion attempts, so these counts are operational summaries. No collection state is saved between calls.

Each GC request runs one complete, stateless pass. `GcRequest` accepts `grace_window_ms`, and it has no cursor. No sweep runs unless `gc` is present.

Every GC call reads the current manifest and uses one clock reading for the whole call. It keeps its live set in memory, and it doesn't write any collection progress. For each object family it sweeps, the call lists the family from the beginning and sweeps it to the end. The call skips the active namespace's content sweep if the shared base filter would exceed its byte cap. [Reference roots](format.md#112-reference-roots) defines the collection roots, and [Content roots](format.md#119-content-roots) defines the content roots. If a manifest read fails, the call fails before it sweeps anything.

A GC response includes `next_reclamation_at_ms` when the pass finds any of these future times:

- a deleted namespace that's still inside its retirement grace,
- a retained user or snapshot pin with a future deletion time, or
- an upload session with a future cleanup time.

Upload cleanup times include the lease plus grace, the abort grace, and the completed-content grace. The value is the earliest of those future times that the pass examined. Fork pins have no cleanup time, and candidates that age out by provider timestamps don't contribute a time. If the field is absent, that doesn't mean nothing remains to collect.

A GC response also includes `reclaimable_at_ms` when the current manifest is deleted. It's the deadline defined in [Retirement](format.md#95-retirement), and it's included even when pins still block reclamation. It's absent for an active namespace.

A GC response groups related counts:

- `deleted` contains `wal_objects`, `metadata_segments`, `manifests`, `upload_sessions`, `content_objects`, `retired_content_objects`, and `temporary_objects`.
- `deleted_checkpoints_by_owner` contains `user`, `snapshot`, and `fork` counts for the pins deleted in the pass. Their sum is the total number of pins deleted.

Each deletion is counted once. When a fork's target namespace deletes its source pin, the deletion counts toward `fork` if the pin was present before the deletion, and repeating that deletion on an absent pin adds nothing. Every count field is present, including fields whose value is zero.

`content_objects` counts the content objects in an active namespace that no retained view references, which the pass deleted once they were older than the grace window. `retired_content_objects` counts the keys that the pass listed under the deleted namespace's content prefix and successfully deleted. If there are no new objects, a repeated content sweep makes one empty LIST request and no DELETE requests, and it reports zero. The `retired_content_objects` count contributes to maintenance progress. `temporary_objects` counts the store's temporary objects that the pass deleted once they were older than the grace window.

Every core GC response also includes `retained`, which counts the candidates the pass kept, grouped by the reason each one was kept. The reasons are a closed set, so every field is always present, a zero means nothing was kept for that reason, and the fields sum to the total:

| Reason | Means |
| --- | --- |
| `referenced` | Protected by current roots or the shared base filter, or needed for forward manifest discovery. |
| `within_grace_window` | Unreachable, but younger than `grace_window_ms` by the object's own provider timestamp. |
| `no_provider_timestamp` | Unreachable, and the provider reported no last-modified time, so the object's age is unknown and it's treated as young. |
| `unrecognized_key` | A key under a swept family that the collector doesn't recognize as one of its own. The collector never deletes it, whatever its age. |
| `checkpoint_not_deletable` | A pin that's kept by its owner or by its grace window. |
| `upload_session_window` | An upload session that's waiting for a window that ends at a known time. These are the same waits that `next_reclamation_at_ms` reports. |
| `upload_session_undecided` | An upload session that's kept for a reason that doesn't end at a known time: a lost compare-and-swap, a record that disappeared during the pass, or a content cleanup failure. |

Each pass counts the candidates that it examined and kept, not the objects in the namespace, so an object that two passes both examine is counted by each pass.

[Candidate and age rules](format.md#113-candidate-and-age-rules) defines which objects are candidates for deletion, and when each one is old enough to delete.

### Checkpoint inventory

A checkpoint's name is a label, not a key. Every create call writes a new record, so several checkpoints can have the same name. Creating a checkpoint first folds any WAL tail after the current manifest, and then pins the resulting manifest.

Create and list use the same checkpoint object, which contains `namespace_id`, `checkpoint_id`, `owner`, `created_at_ms`, an optional `expires_at_ms`, `captured_seq`, and `manifest_no`. Create returns this object directly. For a checkpoint created through the API, `owner` is `user` with the requested `name`, and `created_at_ms` is the durable record's timestamp.

A checkpoint ID has the form `pin_{manifest_no:020}-{16 lowercase hex}`. It identifies the manifest that checkpoint and snapshot reads use, and every pin is assigned a new ID.

For example, a create response looks like this:

```json
{"namespace_id":"demo","checkpoint_id":"pin_00000000000000000009-0000000000000009","owner":{"kind":"user","name":"release"},"created_at_ms":1752623000000,"expires_at_ms":1752626600000,"captured_seq":12,"manifest_no":9}
```

`GET /v0/maintenance/namespaces/{ns}/checkpoints?limit=100&cursor=...` returns existing checkpoints in ascending `checkpoint_id` order, and each entry is the same checkpoint object that create returns. User checkpoints can be deleted by ID. A fork checkpoint keeps its `fork` owner, and it stays while its target namespace still reads through it.

```json
{"namespace_id":"demo","checkpoints":[{"namespace_id":"demo","checkpoint_id":"pin_00000000000000000009-0000000000000009","owner":{"kind":"user","name":"release"},"created_at_ms":1752623000000,"expires_at_ms":1752626600000,"captured_seq":12,"manifest_no":9}]}
```

Deleting a checkpoint removes its pin record and returns the addressed namespace and checkpoint. The success response is:

```json
{"namespace_id":"demo","checkpoint_id":"pin_00000000000000000009-0000000000000009"}
```

A missing ID, including the ID of a checkpoint that was already deleted, returns `checkpoint_not_found`.

`limit` follows the advertised pagination limits, and the response leaves out `next_cursor` after the final page. Cursors are opaque and tied to this namespace and operation, so clients should pass them back unchanged. A namespace-scoped cursor from another namespace returns `invalid_request`.

The checkpoint listing is live, not a point-in-time view. Checkpoints that are created, deleted, or collected while a client is paging can change later pages.

An expired user pin stays listed and readable until GC deletes it, after its expiry plus the grace window. A user pin without an expiry, on a live namespace, stays until it's explicitly deleted. [Records and owners](format.md#81-records-and-owners) defines reads and removal for each pin owner.

### Snapshots

A snapshot is a time-bounded view of a namespace. Its `snapshot_id` is its pin ID, and its `captured_seq` is the namespace sequence it captured. A create request requires `name` and `ttl_ms`, and the TTL can't exceed `snapshot.max_ttl_ms` or `snapshot.max_lifetime_ms`.

An extension measures its requested TTL from the server's current time, and it never moves the expiry more than `snapshot.max_lifetime_ms` past the record's `created_at_ms`. A namespace can have at most `snapshot.max_live_per_namespace` live snapshots.

To enforce that limit when it creates a snapshot, the server:

1. Counts the live, unexpired snapshots. If the request passes the limit at this count, the server returns `snapshot_quota_exceeded` and writes nothing.
2. Writes the new pin.
3. Counts the live, unexpired snapshots again. If the request passes the limit at this count, the server deletes the pin it wrote and returns `snapshot_quota_exceeded`. If that delete fails, the pin stays until it expires, and it counts toward the limit until then.

Two requests that race at the limit can both be refused.

The snapshot listing returns snapshot-owned pins whose lifetimes haven't expired, and it leaves out expired snapshots and pins with other owners. (The maintenance checkpoint listing, by contrast, shows expired records until collection deletes them.) Deleting a snapshot removes its pin, and a second delete returns `snapshot_not_found`.

In an ACL namespace, creating, listing, extending, and deleting snapshots requires an administrator subject. A request with no subject context acts as the token holder. A snapshot read requires `read` and `history` on the historical inode, evaluated at the current head.

The operations above manage a snapshot's lifetime. Path stat, inode stat, path directory listing, inode children listing, file content, download, and change-feed requests accept an optional `snapshot_id`. The path download request includes it in its body, and the inode download request includes it as a query parameter. File content and download requests can't combine `snapshot_id` with `revision_no`, because the snapshot selects the revision. The inode revision routes identify their revision in the route, so they reject `snapshot_id` with the same `invalid_request` error. A snapshot change feed ends at the captured sequence, and `after_seq` can't exceed that sequence.

When an embedded read sets `snapshot_id` in its options, the read opens a read view of that snapshot, as the HTTP request does. A read view of a snapshot accepts options that refer to the same snapshot, and it rejects any other snapshot ID with `invalid_request`.

An embedded caller can also open a read view of a checkpoint. A directory cursor from a read view of a checkpoint or snapshot identifies that pin. A read view of the same checkpoint or snapshot can resume the cursor, and a live listing or any other read view rejects it with `invalid_request`.

A snapshot read requires a live snapshot. A read of a missing snapshot, including one that was deleted, returns `snapshot_not_found`. A read of an expired snapshot returns `snapshot_gone` while its pin still exists. In neither case does the read fall back to the current namespace state.

### Store contract probe

The store probe tests whether the configured object store meets the provider contract that the storage format depends on (see [Object-storage requirements](format.md#3-object-storage-requirements)), and it reports the result of each check. The contract covers create-if-absent, compare-and-swap, read-after-write visibility, prefix listing, and ranged reads. A probe writes and deletes objects, so LoonFS doesn't run one at startup or on a schedule. A probe runs only when an operator requests one.

Every object a probe run writes is stored under `probe-runs/{run_id}/`. That prefix isn't a durable object family, so garbage collection never lists it, and no namespace state can be reached from it. The run's last check deletes those objects and confirms that the prefix is empty, so a probe that completes leaves nothing behind. A probe that doesn't finish leaves orphaned objects under that prefix, which nothing reads.

The response includes the `run_id` that the server generated, and one entry for each check, in the order the checks ran. Each entry identifies the check and its `outcome`:

| `outcome` | Means |
| --- | --- |
| `passed` | The store behaved as the contract requires. |
| `unsupported` | The store reports that it can't perform this operation at all. Only checks of the optional capabilities report this outcome, and a deployment that needs neither optional capability is unaffected. |
| `failed` | The store did something the contract forbids, or the operation failed outright. The entry's `message` says what was expected and what happened instead. |

A failed check is reported in the response body, not as an error. A probe that runs to completion returns 200, whatever it found. The server returns the error envelope only for an unauthorized request or a malformed body. A failed check doesn't end the run, so one probe reports the result of every check, not just the first problem it found.

A probe's result doesn't determine whether a deployment is trusted to serve presigned direct uploads. That trust comes from the endpoint allowlist behind the `uploads.direct_put` capability.

Why does a probe with failed checks return 200? The operator asked whether the store meets the contract, and the failed checks are the answer to that question (the store is wrong), not a failure of the request.

Why doesn't a passing probe make a deployment trusted for direct uploads? A probe exercises the server's own request path, and never a presigned capability that's issued to a client.

### Deleting, retaining, and reclaiming

An ordinary operation that finds that its namespace was deleted returns `namespace_deleted`. Cached readers follow the rules in [Deleting a namespace](format.md#94-deleting-a-namespace). Collection is asynchronous. It has no set completion time, and it doesn't guarantee physical erasure.

[Retirement](format.md#95-retirement) defines when a deleted namespace is eligible for retirement, and [Sweeping a retired owner's content](format.md#118-sweeping-a-retired-owners-content) defines how its content is reclaimed. Purging the namespace's tombstone and hint is outside the scope of this API.

Retention is coarse. A deleted ancestor namespace keeps every object it published while a live descendant still depends on it.

In an active namespace, GC reclaims a content object once it's older than the grace window and nothing references it. A content object is referenced by:

- a revision in the current manifest, in a pinned manifest, in a manifest whose successor is younger than the grace window, or in the unfolded WAL tail, or
- an upload session record, which references its own content.

[Content roots](format.md#119-content-roots) defines these rules. Deleting a file or a tree doesn't reclaim anything, because a deleted file keeps its newest revision. A compaction that removes old revisions at or below the retention floor makes their content collectable.

The metadata retention floor is a separate mechanism. It limits WAL replay history and file revision history, and it makes older WAL objects eligible for GC. A caller advances it explicitly, with `POST .../runs` and the body `{"kind":"retention"}`, or with `loonfs maintenance retention advance`. After the floor advances, a base compaction keeps every revision above the floor, and each file's newest revision at or below it.

Completed upload sessions in active namespaces keep their records for the derived content reclamation grace, which is slightly longer than seven days. A session record protects its content from collection, so GC reclaims staged content that no commit published on a pass after the record is removed.

GC needs to run repeatedly, including after a pass finds nothing left to delete. An upload capability that was issued before the deletion can still write an object after it, and a late write that one listing misses is found by the next run. Complete reclamation can be delayed by continued late writes, grace windows, dependent forks, pin cleanup, or maintenance that isn't running.

Once the current manifest is deleted, LoonFS refuses commits, requests for new upload capabilities, and upload completion, but it doesn't revoke capabilities that were already issued. The retirement grace is longer than the presigned URL lifetime.

The reference server runs a maintenance sweep with two parts:

- On the collection interval, the sweep visits the namespace sessions that the server keeps open and whose published sequence has moved, and it collects their garbage.
- A full pass lists every namespace in the store, including deleted ones, and runs GC for each one. The full pass runs when the sweep starts, and then on the full sweep interval (24 hours by default).

A namespace that nobody writes to through this server process (including forks and deleted namespaces) waits for the full pass. The full pass also covers objects left behind by a crashed process, and garbage that's still inside a grace window. An idle namespace costs no requests between full passes.

LoonFS doesn't schedule GC when a namespace is deleted, when an upload expires, or when a fork retires and its source has more to collect. A host that doesn't run the sweep runs GC itself, with `POST .../runs` and the body `{"kind":"gc"}`, with `loonfs maintenance gc`, or with `Maintenance::gc`. It can use `next_reclamation_at_ms` to decide when to run GC again.

When the server shuts down, its sweep doesn't start any new visits, and a streaming compaction in a visit stops at its next block. A writer session's metadata compaction stops the same way. Shutdown waits for the folds and compactions that already started.

A deployment needs to keep the provider's lifecycle rule for incomplete multipart uploads. Provider upload state can exist outside object listings, so deleting a namespace's content objects doesn't replace session abort and provider cleanup. Deleting a key also doesn't erase physical versions that bucket versioning or retention locks keep.

The `retained` counts in a GC response show what a pass kept. To find the checkpoints that block reclamation, an operator lists them with `GET /v0/maintenance/namespaces/{ns}/checkpoints`.

### Uploads

This section describes the three upload modes.

#### Service-proxied upload

`service_proxied` is the default mode, and it doesn't need any capability from the provider. The client sends its bytes with a `PUT` to `/uploads/{upload_id}/content`, and the server writes them to object storage.

The server streams the body to object storage without buffering the whole file. While it streams, it counts the bytes and computes the store's checksum. A body larger than `upload.service_proxied.max_content_bytes` fails with `content_too_large`. The resulting content reference stores that checksum in its `checksum` field.

#### Direct single-PUT upload

In `direct_put` mode, the server chooses the object identity and returns a presigned upload capability, along with the provider's stored checksum algorithm. The client counts and hashes the bytes while it sends them, and then supplies the size and checksum when it completes the upload. The begin request looks like this:

```json
{
  "mode": "direct_put",
  "size_bytes": 1234
}
```

`size_bytes` is optional and advisory. If it's present, the server compares it with `upload.direct_put.max_content_bytes` (the provider's single-request ceiling), and if it's too large, the server returns `content_too_large` before it issues a capability. The provider enforces the same limit when it accepts the PUT. Larger content uses `direct_multipart`, when it's available.

The response includes a short-lived transfer capability, and never raw object-store credentials or a caller-managed object key. The provider issues the required headers, and the client MUST send them unchanged. For example, an S3-compatible deployment might return:

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

The signed headers are part of the transfer capability. In the S3-compatible example, `if-none-match: *` keeps the write to the immutable object create-only. A client can't drop or change that requirement without invalidating the capability.

Arbitrary S3-compatible gateways are unproven, because HMAC interoperability doesn't show that a gateway enforces create-only requests or reports the stored checksum. On an unproven endpoint, the feature key is absent, and a request to begin a `direct_put` upload returns `not_supported` with `feature = "filesystem.uploads.direct_put"`. The service-proxied upload path is still available, and it's the default.

The reference server offers `direct_put` for a provider when its adapter can presign a create-only request and read back the stored checksum, and the adapter has passed the live provider suite:

- AWS S3 and Cloudflare R2 qualify through SigV4 on their own domain families.
- Google Cloud Storage qualifies through its native `GOOG4-RSA-SHA256` API, not through its S3-interoperability API, which doesn't preserve preconditions.
- Custom S3-compatible endpoints, Azure Blob Storage, and the local filesystem aren't offered `direct_put`, and no configuration setting overrides this.

Other implementations might use different headers, or not support `direct_put` at all.

The response identifies the provider's stored checksum algorithm. S3-family issuers return `crc64nvme`, and the GCS issuer returns `crc32c`. The client calculates that checksum while it sends the bytes.

`direct_put` and `direct_multipart` are separate offers, and a deployment can advertise the first without the second. (The reference server's Google Cloud Storage adapter works this way. It signs whole-object writes and reads, and it doesn't implement multipart signing.) A client falls back from a multipart upload to one whole-object write before it falls back to the service-proxied path. A source whose length is unknown can use the whole-object write, and report its exact size after the one-pass transfer.

A browser that calls a presigned URL is talking to the provider, not to LoonFS, so the bucket's or container's own CORS configuration controls cross-origin access, not anything this API sets.

After it uploads to the presigned URL, the client completes the session with the same content claim that direct multipart uses:

```json
{
  "mode": "direct_put",
  "content": {
    "size_bytes": 1234,
    "checksum": { "algorithm": "crc64nvme", "value": "<16 lowercase hex>" }
  }
}
```

The checksum algorithm MUST match the session's `checksum_algorithm`, and a different algorithm returns `invalid_request`. To complete the session, the server:

1. Builds the final content reference from the session's namespace ID and content ID, plus the completion claim. The session's namespace is the owner.
2. Compares the claimed size and checksum with the object in storage, using a provider metadata request. The server doesn't download the object during this check.

If the stored object doesn't match the claim, the server returns `invalid_request` and changes nothing. The session stays open, nothing is deleted, and the client can complete again with a claim that matches the object, or abort. If the provider metadata request fails, the server returns `server_error` without changing the object or the session, so the client can retry.

Completion checks the stored content against the client's claim, and it doesn't apply the provider's upload limit again. A session that never completes ends when upload collection aborts it after its lease passes (see [Upload-session cleanup](format.md#116-upload-session-cleanup)).

#### Direct multipart upload

In `direct_multipart` mode, the client uploads a large object directly to object storage, in parallel parts. The server opens the provider upload, signs each part, and completes the upload, without receiving the file bytes.

A begin request sets the part size, and it doesn't include the total size or the complete-object checksum:

```json
{
  "mode": "direct_multipart",
  "part_size_bytes": 8388608
}
```

`part_size_bytes` defaults to 8 MiB, and it MUST be between 5 MiB and 5 GiB. A session supports at most 10,000 parts, so larger objects need larger parts. A client that doesn't know the total size can request parts until its stream ends.

Both direct PUT and multipart uploads provide the complete checksum at completion, and multipart also provides one checksum for each part before the part is signed. Both transports support one-pass uploads and streams whose total size isn't known at the start.

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

The response has no content reference, because the complete size and checksum aren't known yet. A client that knows its size can calculate the part count from `part_size_bytes`.

To upload parts, the client sends `POST /uploads/{upload_id}/parts` with a list of `{part_number, checksum}` values, and the server returns one presigned capability for each part. Every checksum MUST use the session's `checksum_algorithm`. Parts can be uploaded in parallel. To retry a part, the client requests another capability and uploads that part again, and the provider keeps the latest upload.

The server doesn't store part progress, so the client MUST keep each part's number, etag, and checksum until completion.

The completion request includes the complete size and checksum, plus every uploaded part. In v0, multipart sessions use CRC-64/NVME, and the resulting content reference stores that complete-object checksum:

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

The request lists every part once, in ascending order, and the server returns the content reference after completion. (For comparison, a service-proxied completion contains just `{"mode":"service_proxied"}`, and a direct PUT completion includes the final size and checksum, as shown in [Direct single-PUT upload](#direct-single-put-upload).)

To complete the session, the server:

1. Asks the provider to assemble the object.
2. Reads the assembled object's stored size and checksum.
3. Compares them with the completion request.

The read in step 2 is required because providers don't all handle an incorrect assembled checksum in the same way. After the provider call, the object at the key is the only evidence of the result, whatever the provider's response was.

If the stored values don't match, the server returns `invalid_request` and changes nothing, as for direct PUT. The session stays open, nothing is deleted, and the client can complete again with a claim that matches the object, or abort. If assembly or the metadata read fails before the server can compare the values, the server returns `server_error` and keeps the session open, so the client can retry completion.

If completion fails without a clear response, the client resends the same completion request. The result depends on the session's state:

- A completed session returns its stored result, and a new `content_token` while its token-issuing window is still open.
- An open session continues completion.
- An aborted session is terminal, so the client stops retrying.

A caller that can't resend the same request reads the upload status instead. A completed session's status includes the same stored result.

If the provider upload of an `open` session no longer exists (for example, because the response to an earlier completion was lost), the server checks the object at the key against the request. If the object matches, the server completes the session. If the object is missing or different, the server returns `invalid_request` and leaves the session open.

The session record includes the provider's upload ID. So when a session is aborted, either by the client or by upload garbage collection after its lease passes, the abort also abandons the provider's upload and the object it was writing. Aborting an upload that has already assembled its object is safe on every supported provider. The abort succeeds and leaves the object alone.

### Content references and tokens

A content reference contains `kind`, `owner_namespace_id`, `content_id`, `size_bytes`, and `checksum`. The owner is the namespace that originally wrote the bytes, and it doesn't change when the namespace is forked or the revision is restored (see [File contents and ownership](format.md#15-file-contents-and-ownership)). Clients send the complete reference back unchanged.

A server can return a short-lived `content_token` for completed content. Clients treat the token as opaque, and they can copy it directly into a commit request. Reading a completed session returns a new token while its token-issuing window is still open, and the session's `content_ref` stays available after that.

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_...",
  "content_ref": { "kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "..." } }
}
```

A path-oriented `put_file` operation then references the completed `content_ref`, and the client includes the matching `content_token`, unchanged, in `content_tokens`. The server verifies the token before admission, and when the writer publishes the commit, it checks the binding and deadline of the resulting in-memory proof. A missing or expired proof returns `content_not_prepared`, without reading the content object. A malformed or expired token that refers to the put's reference also returns `content_not_prepared`, and the server ignores tokens that refer to other references.

For example, this commit request puts the uploaded content at `/docs/report.pdf`:

`Loonfs-Actor: document-importer`

```json
{
  "commit_id": "commit-a",
  "content_tokens": [
    {
      "content_ref": { "kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "..." } },
      "token": "opaque-server-token"
    }
  ],
  "operations": [
    {
      "kind": "put_file",
      "path": "/docs/report.pdf",
      "content_ref": { "kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "..." } },
      "behavior": "no_replace"
    }
  ]
}
```

Long-running transfers can also expose session resources. Implementations can expose workflow helper resources too, but those helpers aren't part of the core semantics. Once a multi-request interaction begins, the identifier that the server issued identifies that interaction for as long as it's in progress.

### Creating and forking namespaces

Creating a namespace uses the namespace ID directly. In v0, there are no namespace aliases or separate display names. A representative create request looks like this:

`Loonfs-Actor: usr_8f3c`

```json
{
  "namespace_id": "demo",
  "access": {"kind":"unrestricted"},
  "naming": "case_insensitive"
}
```

The response contains the new namespace's initial state. A new namespace starts at sequence 0, with a retention floor of 0:

```json
{
  "namespace_id": "demo",
  "created_at_ms": 1752623000000,
  "created_by": "usr_8f3c",
  "access": {"kind":"unrestricted"},
  "naming": "case_insensitive",
  "head_seq": 0,
  "retention_floor_seq": 0
}
```

A fork request identifies the target namespace with `new_namespace_id`. Route placeholders such as `{ns}`, `{source_ns}`, and an implementation-internal `:namespace` are path parameter names, and each one takes the same namespace ID value. v0 doesn't accept or return a namespace `name` alias.

Create and fork install the hint and then manifest 1. The conditional put of manifest 1 determines whether the namespace exists (see [Namespace lifecycle and forks](format.md#9-namespace-lifecycle-and-forks)). A create or fork that loses that write to another active namespace returns `namespace_exists` (409). A create or fork into a deleted ID returns `namespace_deleted` (410) before it writes anything. A namespace is never left partially created.

[Namespaces and identity](format.md#11-namespaces-and-identity) defines a namespace's lifetime. An application that reuses a human-readable name MUST keep its own map from names to namespace IDs, and generate a new namespace ID for each lifetime.

If a creation acknowledgement is lost, a new request returns `namespace_exists`, unless the request explicitly allows an existing namespace. Within the original attempt, after an unknown transport outcome, an exact read-back of the manifest confirms a fork, because the fork's source pin is unique. For a plain create, matching bytes don't prove which caller created the namespace, so the result is `namespace_exists`, or the existing summary if the caller allows an existing namespace.

### 6.1 `GET /v0/capabilities`

Returns the capability document described in [The capability document](#21-the-capability-document).

### 6.2 `GET /v0/namespaces/{ns}`

Returns the metadata of one namespace, without listing every namespace. A missing namespace returns `404` with `namespace_not_found`, and a deleted namespace returns `410` with `namespace_deleted`.

```json
{
  "namespace_id": "demo",
  "access": {"kind": "unrestricted"},
  "naming": "case_insensitive",
  "created_at_ms": 1752623000000,
  "created_by": "usr_8f3c",
  "head_seq": 418,
  "retention_floor_seq": 120
}
```

The `NamespaceMetadata` object has exactly these fields:

| Field | Meaning |
| --- | --- |
| `namespace_id` | The durable namespace ID. |
| `access` | The access mode, either `{"kind": "unrestricted"}` or `{"kind": "acl", "principal_scope": "..."}`. |
| `naming` | The naming mode, either `"case_insensitive"` or `"case_sensitive"`. It controls how sibling names are compared (see [Names and paths](format.md#14-names-and-paths)). |
| `created_at_ms` | The time the namespace was created, in Unix milliseconds. |
| `created_by` | The actor that created or forked the namespace, as supplied by the application. |
| `fork_basis` | Present only for a fork. It contains `source_namespace_id` and the captured `source_head_seq`. |
| `head_seq` | The namespace's current visible sequence. |
| `retention_floor_seq` | The oldest position a change feed can resume after. The feed returns changes above it. |

The create request includes `access`, in the same shape, plus `root_grants` for the `acl` kind. `access` defaults to unrestricted, and an ACL namespace needs at least one administrator in `root_grants`. The create request also includes `naming`, which defaults to `case_insensitive`. Both modes are set when the namespace is created and can't be changed, and a fork uses its source's modes.

LoonFS derives the namespace's live sequence from the manifest and the numbered WAL tip. A cold read starts at the hint, and if the hint lags, the read probes forward. If the hint is missing, the read treats the namespace as absent. A cached runtime probes the next WAL number with GET, applies any new WAL objects, and continues until it gets a 404. Commits are visible on the next read even when the hint hasn't been raised.

The runtime that acknowledged a commit seeds its read caches from the publication. Its next read probes the next WAL number like any warm read. If the manifest and the WAL tip haven't changed and the seeded projection is still cached, that read replays nothing and doesn't reload the manifest.

`LoonFsBuilder::manifest_revalidation_interval_ms` sets the minimum monotonic interval between checks for a successor to the cached manifest. It defaults to 1000 milliseconds, and a value of `0` checks on every read. A read after the interval probes for the successor manifest with HEAD, as well as probing the next WAL number, so a warm read of an unchanged head costs two requests. Within the interval, it costs one. If a successor exists, the reader reloads the namespace. Warm readers observe deletions, retention floor advances, and new manifests on this interval. Handle builders accept an injected monotonic timer for deterministic runtime timing.

The maintenance endpoint `GET /v0/maintenance/namespaces/{ns}/diagnostics` returns the namespace state plus the storage details that maintenance uses:

| Field | Meaning |
| --- | --- |
| `namespace_id` | The durable namespace ID. |
| `created_at_ms` | The time the namespace was created, in Unix milliseconds. |
| `created_by` | The actor that created or forked the namespace, as supplied by the application. |
| `fork_basis` | Present only for a fork. It contains `source_namespace_id` and the captured `source_head_seq`. |
| `head_seq` | The namespace's current visible sequence. |
| `retention_floor_seq` | The oldest position a change feed can resume after. The feed returns changes above it. |
| `current_manifest_no` | The current manifest number, present from namespace creation. |
| `wal_tail_objects` | The WAL tip minus the current manifest's folded WAL number, including fences. |
| `live_snapshots` | The number of snapshots that hadn't expired when the diagnostics read began. |
| `live_checkpoints` | The number of user checkpoints, including expired records that are waiting to be collected. |

```json
{
  "namespace_id": "demo",
  "created_at_ms": 1752623000000,
  "created_by": "usr_8f3c",
  "head_seq": 418,
  "retention_floor_seq": 120,
  "current_manifest_no": 410,
  "wal_tail_objects": 3,
  "live_snapshots": 2,
  "live_checkpoints": 4
}
```

#### Namespace statistics

The embedded `load_namespace_statistics` and `load_checkpoint_statistics` loaders return `NamespaceStatistics` as of one manifest's folded head, so newer WAL commits aren't included. The observation includes the manifest reference, the creation time, the lifecycle status, the folded WAL number, the activity counters, the referenced inode and metadata totals, and any fork basis. Counters start at zero, and comparing two observations is valid only within the same namespace. Activity counters are integers from zero through 9007199254740991. [Statistics](format.md#74-statistics) defines the counters and the footprint calculations.

### 6.3 `DELETE /v0/namespaces/{ns}`

Deletes a namespace. In an ACL namespace, this operation requires an administrator subject. A request with no subject headers acts as the token holder.

Deletion is a fenced manifest publication (see [Deleting a namespace](format.md#94-deleting-a-namespace)). The deletion is linearized at the manifest put, so commits acknowledged before that put stay committed. These requests fail with `namespace_deleted` (410) once they observe the deletion: reads, commits, forks from the namespace ID, status requests, another deletion, and creating or forking a namespace into that ID. The namespace tombstone stays the current manifest after the namespace's content is reclaimed.

Checkpoint listing and user-checkpoint deletion are explicit exceptions, and they stay available after deletion. Releasing a fork-owned checkpoint is still rejected.

Deletion itself doesn't reclaim anything. [Retirement](format.md#95-retirement) defines when a deleted namespace is eligible for retirement, [Reference roots](format.md#112-reference-roots) defines the roots that garbage collection keeps, and [Sweeping a retired owner's content](format.md#118-sweeping-a-retired-owners-content) defines the content sweep.

A deployment should run garbage collection repeatedly to catch late writes, and it should keep the provider's lifecycle rule for incomplete multipart uploads. Deleting an object key doesn't erase the physical versions kept by bucket versioning or retention locks.

With the optional `expected_head_seq` query parameter, the server deletes the namespace only if the head is still at that sequence, and otherwise fails with `stale_head`. This is the same precondition pattern that file mutations use. The rejection reports both sequences, in the message and as the `expected_head_seq` and `actual_head_seq` details, so a caller that still wants to delete the namespace can retry against the sequence it found.

```json
{
  "namespace_id": "demo",
  "head_seq": 418
}
```

A commit can succeed without its acknowledgement reaching the caller, and deletion doesn't retroactively change that outcome. The reference server resolves its queue in admission order:

- Requests admitted before the delete are published first.
- Requests admitted after the delete fail with `namespace_deleted`.
- If the delete ends up failing its precondition, no request is rejected because of it.

Why do checkpoint listing and user-checkpoint deletion still work after deletion? User pins with no expiry time have to stay discoverable and releasable after the namespace is deleted.

### 6.4 `GET /filesystem/entry` and `GET /inodes/{inode_id}`

Returns one authoritative path entry, addressed by path or by inode ID. Enum values are snake_case, following the durable naming rules in [Field conventions](format.md#121-field-conventions).

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
    "checksum": { "algorithm": "crc64nvme", "value": "42d..." }
  },
  "revision_committed_at_ms": 1752624000000,
  "attributes_revision_no": 3,
  "attributes_updated_by": "metadata-editor",
  "attributes_updated_at_ms": 1752623500000,
  "attributes": { "owner": "platform" }
}
```

Every entry includes `created_by` and `created_at_ms` from its inode row. File entries also include `revision_committed_by` and `revision_committed_at_ms` from their current revision row. These stamps are informational. Sequence numbers determine order, and no validity rule reads the stamps. In v0, directories have a creation time but no modification time, and a rename or move changes neither the attribution nor the time.

A newer server might report an `inode_kind` that this document doesn't define. A client MUST tolerate an `inode_kind` it doesn't know. It treats such an entry as opaque, so it reads the fields that every entry includes (such as `display_name`, `inode_id`, `path`, and `binding_version`) and ignores any field that belongs to that kind.

The `include_attributes` parameter selects whether the response includes the inode's attributes. It accepts `true` or `false`, and any other value returns `invalid_request`. The entry route defaults to `true`, because it returns one attribute map, which is limited to 64 KiB.

The attributes projection is serialized as prefixed sibling fields of the entry. `attributes_revision_no` and the complete `attributes` map are either both present or both absent. `attributes_updated_by` and `attributes_updated_at_ms` are also present when a stored attributes revision exists.

An empty map is a real projected value at its current revision. An inode whose attributes have never been written reads as `{}` at revision 0, with no updater or update time. A read that didn't include attributes leaves out all four fields, so an absent projection never means "no attributes".

The namespace root has no name, so its entry omits `parent_inode_id`, `display_name`, and `binding_version`. Every other entry includes a validated `display_name`, and a `binding_version` for its current parent and name binding (see [Commit identity and preconditions](#51-commit-identity-and-preconditions)). The empty string isn't a valid name for the root or for any path component that has a name.

The inode route accepts `snapshot_id` and returns the same entry shape, including the `path` at the selected sequence. The path limits in [Names and paths](format.md#14-names-and-paths) apply to request paths. A derived `path` can exceed them, and inode-addressed reads still return it. Renaming an entry changes its path and name, but not its inode ID or metadata. An unknown or hidden inode returns `inode_not_found`, and for the root inode, the route returns the path `/`. `include_attributes` behaves the same way as it does on the path entry route.

Why are the attribute fields prefixed siblings? Clients read the revision number on its own, and pass it as `expected_attributes_revision_no` on their next write without touching the values. That's the prefixed-sibling case, not the consumed-as-a-unit case that the entry-kind enum uses.

### 6.5 `GET /filesystem/entries` and `GET /inodes/{inode_id}/children`

Lists the children of a directory, addressed by path or by inode ID. The response envelope identifies the listing target and the head the listing was read from, so an empty directory still reports which state it observed, and the response can grow without changing the shape of `entries`. The path route identifies its target as `path`, and the inode route identifies it as `parent_inode_id`. Each entry is a full path entry, in the same shape that `GET /filesystem/entry` returns (directory entries omit file-only fields).

A directory listing returns entries in canonical `name_key` order. Concatenating the pages in cursor order produces the complete listing in that same order, and clients MUST NOT re-sort the combined pages. The listing target is required on every page (the `path` query parameter on the path route, or the `inode_id` route segment on the inode route). The cursor records the resume position, but the request target determines what's being listed. Responses include `next_cursor` only when another page is available.

The inode route accepts `snapshot_id`, and it addresses the directory by its stable identity instead of by name. So a listing and its resumed pages stay on the same directory even if the parent is renamed or moved concurrently. Entry paths reflect the parent's location at each page's head. An unknown or hidden target inode returns `inode_not_found`, and a file target returns `path_conflict`. (An inode-addressed caller asks for a directory's children, never for the entry itself, so this route has no single-entry listing for a file.) If the directory is deleted partway through a listing, the resumed page returns `inode_not_found`.

`include_attributes` works exactly as it does on the entry route, and it follows the same rule about which attribute fields are present together, but on this route it defaults to `false`. That keeps the default response bounded, because a page can contain up to `pagination.max_limit` entries, and each attribute map can be up to 64 KiB. Clients that request attributes should choose a suitable page size.

A cursor is normally an opaque resume position in the listing order, not a snapshot pin. Directory listing, trash listing, revision listing, grep, and change-feed cursors tolerate forward head drift. A commit made while a client is paging doesn't invalidate these cursors, and the resumed page is evaluated at the head that's current when it's read, continuing strictly after the last returned position.

Each page is internally consistent at its own head, but a multi-page listing spans whatever heads its pages were read at. So an entry created behind the resume position is missed, an entry deleted behind it was already returned, and a rename can appear as a duplicate or be missed. A client that needs one consistent cut re-issues the listing when `head_seq` changes between pages.

Drift tolerance runs forward, never backward. Every read confirms its head when it starts, so a cursor issued ahead of the serving head wasn't issued for this view, and it returns `invalid_request`. A malformed cursor, or one replayed against a different target, also returns `invalid_request`.

A directory cursor issued with `snapshot_id` is the exception. It's bound to that snapshot's immutable view, and the client MUST repeat the same `snapshot_id` on every page. If a page request omits it or supplies another snapshot, the server returns `invalid_request` instead of combining rows from different views. An unrecognized cursor version is also rejected with `invalid_request`.

The CLI reports the last page's head as `head_seq`. For multi-page and recursive reads, it also reports the first and last observed heads as `head_drift` when they differ. A caller that needs a settled tree lists it again until the observed heads stop changing.

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
        "checksum": { "algorithm": "crc64nvme", "value": "42d..." }
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

Lists the namespace's recoverable deletions, oldest deletion first (in ascending `(deletion_seq, inode_id)` order). The listing is paged with the standard `limit` and `cursor` pattern, and its cursor is an ordering resume like every other cursor. In an ACL namespace, a page can return fewer entries than `limit` and still include `next_cursor`.

The listing is a range scan over the derived active-deletions family (see [File and subtree deletion](format.md#16-file-and-subtree-deletion)), so the cost of a page depends on the page, not on the namespace's deletion history. Those rows represent current state, and they aren't removed when the retention floor advances.

Each entry includes the inode ID and the deletion sequence that `undelete` requires, plus `inode_kind`, `deleted_by`, `deleted_at_ms`, and the removed binding (`deleted_binding`). As with path entries, a client MUST tolerate an `inode_kind` it doesn't know. Nested deletions stay separate entries, and recovering an outer deletion doesn't remove an inner deletion from the list.

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

### 6.7 `GET /filesystem/content` and `GET /inodes/{inode_id}/content`

Returns a file's content, addressed by path or by inode ID. The response body is the authoritative file bytes. Metadata can be exposed in headers, but the body itself is raw content, not JSON.

The server streams and verifies content in bounded chunks. For a file larger than `download.service_proxied.max_content_bytes`, the server returns `content_too_large` before it fetches any content bytes, and clients can use a download grant when direct GET is advertised. The limit is a transfer policy, not an allocation size.

If the response body ends successfully, length and checksum verification completed. The server omits `Content-Length`, so receiving the expected number of bytes can't signal success before verification. An I/O or verification failure after the headers are sent aborts the body instead of returning a JSON error. Consumers MUST require successful stream completion, and a file download should publish its temporary output only after the stream completes. Streaming to stdout can expose a partial or unverified prefix before a later error.

Revision listings return the newest revisions first, and they use the standard `limit` and `cursor` pattern. The path route resolves the current inode first, and the inode route addresses the inode directly. Both routes return the same response, which doesn't include paths. `next_cursor` is included only when another page is available.

File history above the retention floor is complete. At or below the floor, each file keeps its newest revision, so paging to the end reaches revision 1 only while no base compaction has removed it. The cursor resumes after the last revision returned, so a compaction between pages neither repeats nor skips a retained revision.

For example, a revision listing returns:

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
        "checksum": { "algorithm": "crc64nvme", "value": "42d..." }
      }
    }
  ],
  "next_cursor": "7b2e2e2e7d"
}
```

The inode revision route, `GET /inodes/{inode_id}/revisions/{revision_no}/content`, reads and verifies a revision without resolving a current path. Deleted files stay readable while their revision rows are kept. A directory returns `path_conflict`, an unknown inode returns `inode_not_found`, and an unknown or removed revision returns `revision_not_found`.

`GET /inodes/{inode_id}/content` reads the current revision of a visible file inode, wherever the inode is bound, and streams and verifies it like the path route. Like the path route without `revision_no`, it requires only the `read` right. A deleted or unknown inode returns `inode_not_found`, and a directory returns `path_conflict`. With `snapshot_id`, the route reads the revision that the snapshot captured, and like every snapshot read, it also requires `history`.

Embedded by-reference reads require administrator access to the reading namespace. The namespace's read view MUST contain a publication for the content, including publications inherited through a fork. A reference that's absent from that view returns `path_not_found` without reading any content bytes, even when the reading namespace owns the content. Content published after a snapshot isn't readable through that snapshot.

### 6.8 `POST /commits`

Commits an ordered list of operations to a namespace. This endpoint is the HTTP binding for the commit model in [Commit identity and preconditions](#51-commit-identity-and-preconditions). The `Loonfs-Actor` header is required. The body contains one `commit_id`, an optional `message`, optional `preconditions`, and `operations`, which is an ordered, non-empty array of path operations. An empty array returns `invalid_request`.

The root path `/` is readable, but with one exception it can't be the target of a mutation. The exception is `update_access`, which replaces the root's access row. Any other operation that refers to `/` (as its own path, or as either end of a move or copy) returns `invalid_request`. The rejection belongs to the operation, not to the request, so it's reported like every other operation failure. Inside a batch, the error identifies the operation's position. The check runs against a namespace that exists, so a root mutation sent to an unknown namespace returns `namespace_not_found` first.

A representative request:

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

A request with one operation is the one-element case of this shape, not a different kind of request. A convenience call and a batch produce the same commit and the same fingerprint, so a commit ID used by either one replays against the other.

The operations commit together, in order, as one logical commit, so either every operation commits or none does. Operation `k` sees the authoritative namespace state plus the effects of operations `0..k`, so one request can create a directory and write into it:

`Loonfs-Actor: usr_8f3c`

```json
{
  "commit_id": "c_2a41d0c6b9f34e7d8a1b5c9e0f234567",
  "message": "import the January report",
  "content_tokens": [
    {
      "content_ref": { "kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "..." } },
      "token": "opaque-server-token"
    }
  ],
  "operations": [
    { "kind": "create_directory", "path": "/reports/2026" },
    {
      "kind": "put_file",
      "path": "/reports/2026/january.pdf",
      "content_ref": { "kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "..." } },
      "behavior": "no_replace"
    },
    {
      "kind": "delete_path",
      "path": "/inbox/january.pdf"
    }
  ]
}
```

The first operation that fails aborts the whole request, and nothing that it or the operations before it would have written becomes visible. The error identifies the position of the operation that failed. For example, if the put above had raced another writer, the response would be:

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

Three operations write file content. Each one requires either `content_ref` for uploaded content or `inline_content` for bytes included in the commit request:

| Operation | Other fields (`?` means optional) |
| --- | --- |
| `put_file` | `path`, `behavior?`, `expected_inode_id?`, `expected_revision_no?` |
| `create_file_by_inode` | `parent_inode_id`, `display_name` |
| `put_file_revision_by_inode` | `inode_id`, `expected_revision_no` |

Supplying both content fields, or neither, returns `invalid_request` before commit planning. Unknown fields are also rejected.

For uploaded content, the client includes the proof in `content_tokens`. One token covers every operation that uses its `content_ref`, and tokens for unused references are ignored.

For inline content, the client encodes the complete file as a standard padded base64 JSON string, and no content token is needed. For example, this request writes `hello\n` to `/hello.txt`:

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

An empty string writes an empty file. The content ID is assigned by the server, and the size and content checksum are computed from the decoded bytes.

Inline writes are enabled by default for files up to 64 KiB. The configured limit is advertised as `commit.max_inline_content_bytes_per_operation`, alongside `filesystem.commits.inline_content`. If inline writes are disabled, both keys are absent, and an inline request returns `not_supported` with `feature` set to `filesystem.commits.inline_content`.

The size limit applies to each file before base64 encoding. A larger value returns `invalid_request` before any storage write. The complete JSON body, including the base64 content and all other fields, MUST also fit within `commit.max_request_body_bytes` (2 MiB by default).

To stay within the configured WAL limits, some inline files might be uploaded as content objects before the commit. All the operations still commit together. To retry, a client sends the same request with the same inline bytes and commit ID, even if those bytes were stored separately. The retry rules in [Commit responses and safe retry](#52-commit-responses-and-safe-retry) apply.

`append_file` and `append_file_by_inode` add bytes to the end of an existing file as its next revision:

| Operation | Other fields (`?` means optional) |
| --- | --- |
| `append_file` | `path`, `inline_content`, `expected_inode_id?`, `expected_revision_no?` |
| `append_file_by_inode` | `inode_id`, `inline_content`, `expected_revision_no?` |

For example, this request adds `second line\n` to `/logs/app.log`:

`Loonfs-Actor: usr_8f3c`

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

`inline_content` contains the bytes to add, as standard padded base64, from 1 byte to 256 KiB (262,144 bytes) before encoding. The commit stores those bytes in its WAL object, so an append needs no upload and no content token, and it doesn't depend on `filesystem.commits.inline_content`. An empty value returns `invalid_request`, with `param` identifying the field. A larger value returns `content_too_large`.

The target MUST be a visible file. A directory returns `path_conflict`, and a missing path returns `path_not_found` (or `inode_not_found` when the target is addressed by inode). An append never creates a file. `expected_inode_id` and `expected_revision_no` behave as they do on a replacing `put_file`. An inode mismatch returns `path_conflict`, and a revision mismatch returns `stale_revision`. On a path, the revision precondition requires the inode precondition, but by inode, `expected_revision_no` stands alone.

Without a precondition, appends are applied in commit order, each to the result of the one before it, so concurrent appends don't lose any bytes. Within one commit, appends to the same file are applied in operation order, and an append after a `put_file` of the same file adds to the bytes that the put writes.

The new revision has an ordinary content reference whose size and checksum cover the whole file. The new reference either continues the current chain or starts a new one, depending on the current revision's content ID `C` and size `S`:

- When this namespace owns `C`, `S` is positive, and no revision or layout refers to more than `S` bytes of `C`, the new reference keeps `C`. Its pieces start at offset `S`. Its delta omits the layout, and validation writes no content object.
- Otherwise, the new reference uses a new content ID owned by this namespace, and the first `S` bytes of the new chain are the bytes of `C`. This happens when another revision has already extended `C` (for example, after a restore of an older revision, or after an append to a copy of the file), and in a fork whose file still refers to the source's content. The new chain shares the base's objects through the layout that its delta includes. Before it admits the reference, validation writes the base's unfolded bytes as an extent of the base chain. The commit includes only the appended bytes. An append to an empty file also starts a new content ID.

Earlier revisions keep their references, and they still read their own bytes. The new checksum combines the base reference's CRC with the CRC of the appended bytes, and the writer doesn't read earlier bytes to compute it. If the base's checksum algorithm differs from the store's, the append returns `namespace_corrupt`.

A retry with the same commit ID and the same bytes replays the original commit. A retry with different bytes returns `commit_id_reuse_conflict`.

Move and copy accept the same `behavior` values as put:

- `no_replace` (the default) fails when the destination is occupied.
- `replace` replaces a file destination. A replacing move deletes the destination file and rebinds the source in one commit. A replacing copy appends a revision to the destination inode, which keeps its identity and revision history.

Only a file destination can be replaced, and a path never replaces itself.

A replacing put can include `expected_inode_id` alone, or pair it with `expected_revision_no`. A replacing move or copy can use `expected_destination_inode_id` alone, or pair it with `expected_destination_revision_no`. A destination or path revision precondition requires its matching inode precondition, because a revision identifies a version within one inode, not the inode that occupies a path. A revision-only precondition returns `invalid_request`.

These preconditions require `replace` behavior. An inode mismatch returns `path_conflict`, a revision mismatch returns `stale_revision`, and a missing destination returns `path_not_found`.

These preconditions check content revisions, not attribute revisions. A put with preconditions, or a replacing copy, still proceeds after an attribute-only update when the inode and the content revision still match. A replacing move deletes the destination inode, including its attributes. For attribute-level concurrency, a request uses `expected_attributes_revision_no` separately.

Ten operations use inode IDs instead of paths, so a client can act on an entry it read earlier even if the entry's path has changed. An unknown or hidden inode returns `inode_not_found`.

`create_directory_by_inode` and `create_file_by_inode` create an entry under an existing parent directory. Both are create-only, and they return `path_conflict` when the name is already in use.

`put_file_revision_by_inode` appends a revision to a file, wherever the file is currently located. It requires `expected_revision_no`, and it returns `stale_revision` when the file has changed.

`move_by_inode` and `delete_by_inode` require `expected_binding_version` (see [Commit identity and preconditions](#51-commit-identity-and-preconditions)). Their destination, replacement, and recursive-delete behavior matches `move_path` and `delete_path`. The namespace root can't be moved or deleted.

`copy_by_inode` copies the current revision of a file inode to `destination_display_name` under `destination_parent_inode_id`. Its `behavior`, destination preconditions, and attribute rule match `copy_path`.

`restore_revision_by_inode`, `update_attributes_by_inode`, and `update_access_by_inode` behave like `restore_revision`, `update_attributes`, and `update_access`, except that `inode_id` identifies the target. They take no `expected_inode_id`, because the inode is the target, so `expected_attributes_revision_no` and `expected_access_revision_no` stand alone. `copy_by_inode`, `restore_revision_by_inode`, and `update_attributes_by_inode` reject the root inode with `invalid_request`, just as their path forms reject `/`. `update_access_by_inode` accepts the root inode.

For example:

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

The server returns a successful response only after the change is actually committed, which means that the numbered WAL put succeeded. Every commit returns the same `Commit` object (see [Commit responses and safe retry](#52-commit-responses-and-safe-retry)).

A representative response:

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
      "content_ref": { "kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "..." } }
    }
  ]
}
```

The same endpoint also accepts directory creation by path:

`Loonfs-Actor: usr_8f3c`

```json
{
  "commit_id": "c_8b7d4ef098ec4c1fbde15edbe02f9a64",
  "operations": [{ "kind": "create_directory", "path": "/docs" }]
}
```

It also accepts a revision restore by path:

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

It also accepts `undelete`, which recovers a deleted file or subtree by binding the deletion's root inode again. The inode's identity and its retained revision history come back with it. The request identifies both parts of the recovery handle that the delete reported (and that the change feed includes): the inode ID and the deletion's committed sequence.

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

`destination_path` is optional. When it's present, it's the destination. Its parent MUST exist and be visible, and its name MUST be free.

When `destination_path` is absent, the entry is restored in place. It's bound again under the parent inode and name that its deletion recorded. The parent is identified by its inode, not by a remembered spelling of its path, so recovery restores the entry under the right parent even when the enclosing directories were renamed after the delete. The in-place parent and name follow the same rules that a path would. The parent MUST NOT be deleted, and the name MUST be free. If either rule is broken, the request returns that rule's usual error code.

The destination can instead be identified by a parent inode, with `destination_parent_inode_id` and `destination_display_name`, which are required together. The parent MUST be a visible directory, and the name MUST be free. A request that supplies `destination_path` together with either of them, or only one of them, returns `invalid_request`, with `param` identifying the field at fault.

Only the root of a deletion can be undeleted, and `deletion_seq` MUST match the active deletion sequence. A mismatch returns `not_deleted` with the expected and actual sequences, so a stale recovery request can't cancel a later deletion.

The endpoint also accepts `update_attributes`, which writes and removes attributes on the inode that a path resolves to:

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

An attribute map is a map from validated keys to plain UTF-8 strings. Values have no kind envelope or built-in list shape, so a caller that needs a list chooses its own string encoding. The empty string is a stored value, not a tombstone. The only way to delete a key is to list it in `remove`.

Every size is counted in logical UTF-8 bytes, without JSON or durable-encoding framing:

| Constant | Value | Bound |
| --- | --- | --- |
| `MAX_ATTRIBUTE_KEY_BYTES` | 128 | The longest attribute key. |
| `MAX_ATTRIBUTE_VALUE_BYTES` | 4,096 | The longest attribute value. |
| `MAX_ATTRIBUTE_ENTRIES` | 100 | The most entries in one map. |
| `MAX_ATTRIBUTES_TOTAL_BYTES` | 65,536 | The largest map, counting every key's bytes plus every value's bytes. |

`set` writes each of its keys, replacing whatever value the inode currently has for that key, and it leaves the keys it doesn't list unchanged. `remove` deletes each key it lists. Both default to empty. A request returns `invalid_request` if:

- it includes neither `set` nor `remove` (there's nothing to apply),
- a key appears in both,
- a key is repeated in `remove`, or
- it includes any key under the reserved `loonfs.` prefix (those keys belong to the system, and callers can't write them).

An accepted update advances the attribute revision even when the resulting map is unchanged. The revision counts accepted updates, which matches how puts of identical content are counted. The update writes the complete resulting map, and it produces an attributes event in the change feed. Replaying the same request with the same `commit_id` returns its original commit without advancing the revision again, and a request with a new `commit_id` is a new update.

The resulting map is checked against every limit in [Attributes and attribution](format.md#17-attributes-and-attribution), so a small write that pushes an already-large map over a limit is rejected because of the map it would produce.

The target can be any visible file or directory, because attributes belong to the resource. Attributes stay with the inode's identity:

- A move, a rename, a new file revision, and a revision restore leave them unchanged.
- A delete keeps them while it hides the inode, and an undelete restores the same map at the same revision.
- A copy to a vacant destination is the one operation that copies them. The new inode starts with the source's map at attribute revision 1, as its own event in the change feed.
- A copy over an existing file changes that file's content and nothing else.

`expected_inode_id` and `expected_attributes_revision_no` are both optional preconditions, and both are part of the commit's semantic identity when a commit ID is reused. An attribute revision precondition requires its matching inode precondition, because a revision identifies a version of one inode. A revision-only precondition returns `invalid_request` before the path is resolved.

A wrong `expected_inode_id` returns `path_conflict`, like the delete precondition it mirrors, so if the path is rebound in a race, the attributes can't be written to the wrong inode. A stale `expected_attributes_revision_no` returns `stale_attributes`. Omitting the revision precondition doesn't make the write a merge. Every update includes the revision it read as its own precondition, so a concurrent update still returns `stale_attributes`, with the expected and actual revisions in the details.

`update_access` replaces the access row of the inode that a path resolves to. The root path is a valid target, and the root's row is where administrators are listed.

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

`boundary` and `grants` are both required, and they replace the whole row. So an update that lists no principals clears every direct grant, and an update with `boundary` set to false resumes inheritance. Grants are a map from principal ID to a list of distinct right names. [Access rows](format.md#18-access-rows) defines the rights, the size limits, and the rule that no entry has an empty list. A repeated principal key or right name returns `invalid_request`. Two more rules also return `invalid_request`: `admin` is valid only on the root inode, and a boundary applies only to a directory.

An unrestricted namespace has no access rows, and an access update in an unrestricted namespace returns `namespace_unrestricted`. Every accepted update advances the inode's access revision, and it produces an `access_changed` event that includes the complete new state.

`expected_inode_id` and `expected_access_revision_no` follow exactly the same rules as the attribute preconditions:

- Both are part of the commit's semantic identity.
- The revision precondition requires its inode precondition.
- A wrong inode returns `path_conflict`.
- A stale revision returns `stale_access`, whether the caller stated the revision or the update's own precondition observed it.

#### Authorization in ACL namespaces

Before an operation's conflict checks, LoonFS checks the subject's effective rights on the inodes the operation touches. If the subject has no right on a checked inode, the response is `path_not_found` or `inode_not_found`, depending on how the request identified that inode. If the subject has some right but lacks a required right, the response is `forbidden`, with the checked `inode_id` in the error details.

| Operation | Required rights |
| --- | --- |
| create_directory, new put_file | `create` on the parent, or on the deepest existing directory when missing parents are created. |
| create_directory_by_inode, create_file_by_inode | `create` on the parent. |
| Replacing put_file, put_file_revision_by_inode, append_file, append_file_by_inode | `write` on the existing file. |
| put_file with no_replace at an occupied name | `create` on the parent, before the conflict is reported. |
| delete_path, delete_by_inode | `remove` on the source parent. |
| move_path, move_by_inode | `remove` on the source parent and `create` on the destination parent. Replacing an occupant also requires `remove` on the destination parent. |
| copy_path, copy_by_inode | `read` on the source file, plus either `create` on the destination parent (for a vacant name or a name conflict) or `write` on the occupied file being replaced. |
| restore_revision, restore_revision_by_inode | `write` and `history` on the file. |
| update_attributes, update_attributes_by_inode | `write` on the target inode. |
| undelete | `remove` on the saved parent and `create` on the recovery parent, including when recovering in place. |
| update_access, update_access_by_inode | Authority for the changes, as described below. |

A move that changes any principal's effective rights on the moved inode is authorized as if the mover had granted the rights that are gained. The mover MUST have `share` and every gained right, or have `manage`, or be an administrator. Only gains count, so removing access needs no extra authority. Entry rights are enough when the move confers nothing. A boundary folder can move anywhere with entry rights, because its inherited rights don't change. Root administrator principals don't contribute gains. An undelete that relocates the entry follows the same rule.

For `update_access`, changing the root's set of administrator principals requires an administrator. Otherwise, `manage` permits any update. A subject without `manage` MUST have `share`, leave the boundary unchanged, and have every right that it adds to or removes from any principal's direct grant.

A precondition requires `read` on the inode it refers to. A path-absence precondition requires `read` on the existing parent instead, and a name-absence precondition requires `read` on the parent. A namespace-head precondition needs no inode right.

The same not-found and forbidden rule applies to reads. A subject with no right on the target receives `path_not_found` or `inode_not_found`, and a subject with some right but without a required right receives `forbidden`.

| Read operation | Required rights |
| --- | --- |
| Stat, directory listing, current content, or current download, by path or inode | `read` on the target inode. |
| Older content revision or historical download, by path or inode | `read` and `history` on the target inode. |
| Revision listing, by path or inode | `read` and `history` on the target inode. |
| Snapshot stat, listing, content, or download | `read` and `history` on the historical inode, evaluated at the current head. |
| Trash listing | `read` on each deletion's saved original parent. |
| Batch file resolution and grep candidates | `read` on the candidate inode. Snapshot resolution also requires `history`. |
| Change feed or bare content reference read | Administrator. |
| Bare content reference import | Administrator on the reference's owner namespace. |

Names belong to the directory, so a subject with `read` on a directory sees every child in a listing. A child that the subject can't read keeps its entry fields, but its `attributes` projection is left out, even when attributes were requested. For a snapshot read, LoonFS evaluates rights at the current head, on the historical inode that the snapshot resolves, never on the inode that occupies the path today. Every snapshot read of an inode also requires `history`.

The trash listing filters out entries whose saved original parent the subject can't read. A filtered page can be short and still include `next_cursor`. Grep filters out candidates that the subject can't read before it reads their content, but those candidates still count against the page's candidate budget.

When a subject is supplied, change feed reads, bare content reference reads, and bare content reference imports require an administrator. An import checks the reference's owner namespace, including when the owner is also the destination or has been deleted. With no subject headers, these operations act as the token holder, but every per-subject read operation returns `invalid_request` and identifies `Loonfs-Principals` in the error. Checkpoint and maintenance operations still read as the service.

### 6.9 Upload transport

The upload routes stage a file's content in an upload session, so that a commit can reference it. This section defines what each upload step means, not one specific path for the bytes. In v0, every upload is a whole-file upload. The staged body is the file's complete content, not a separate metadata document or a multipart strategy.

In an ACL namespace, an upload session belongs to the subject that opened it. Every upload operation requires the namespace's scope and principals (see [Subject and principals](#subject-and-principals)), and a request from any other subject returns `upload_not_found`. An unrestricted namespace doesn't record which subject owns a session.

Each upload step has a defined effect:

- `PUT /content` stores the immutable whole-file object and records the staged `content_ref`.
- `complete` verifies the upload and records its final `content_ref`.
- After that, a commit can safely reference the `content_ref` of the completed session that the server returns. Remote servers can also return an opaque `content_token`, which remote create and replace operations send back as their proof that the content was prepared.

A begin request (`CreateUploadBody`) selects the upload transport with `mode`, and it can include only the fields for that mode:

```json
{ "mode": "service_proxied" }
{ "mode": "direct_put", "size_bytes": 1234 }
{ "mode": "direct_multipart", "part_size_bytes": 8388608 }
```

Mode-specific fields are siblings of `mode`:

- `service_proxied` has no other fields.
- `direct_put` accepts an optional size hint.
- `direct_multipart` accepts an optional `part_size_bytes`, and the server uses the default when it's omitted.

A completion request (`CompleteUploadBody`) uses the same `mode` values as a begin request:

```json
{ "mode": "service_proxied" }
{ "mode": "direct_put", "content": { "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "<16 lowercase hex>" } } }
{ "mode": "direct_multipart", "content": { "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "<16 lowercase hex>" } }, "parts": [] }
```

The request's mode MUST match the upload session. A direct PUT completion requires `content`, a multipart completion requires `content` and `parts`, and a service-proxied completion has no other fields. Unknown fields, missing fields, and mode mismatches return `invalid_request`.

Every upload step (`create_upload`, `put_upload_content`, `get_upload`, `complete_upload`, and `abort_upload`) returns the session object. An open session includes `mode`, `status`, `expires_at_ms`, and `checksum_algorithm`, and its mode's fields are siblings of those:

- A `direct_multipart` session also includes `part_size_bytes`.
- A `direct_put` session includes `access`, a write capability. The server issues a new one when it creates the session and on every read of the open session.
- A `service_proxied` session includes `content_ref` after bytes have been staged. `PUT .../content` returns the open session with the staged `content_ref`.

Fields that don't apply are omitted. Response readers accept unknown fields.

An upload session allocates its content object when it begins. Repeating `PUT /content` with the same bytes for the same upload ID writes the same object, so the repeat is idempotent. Repeating it with different bytes is a conflict. Two different sessions with identical bytes each get their own object, because content is never shared across uploads. Retries are idempotent only within one session. Completing a service-proxied upload fails if no content was staged. Publication never downloads an arbitrary external reference to make up for a missing proof.

A session starts `open` and then becomes `completed` or `aborted`, and both of those states are final ([Upload sessions](format.md#51-upload-sessions)):

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

`GET /uploads/{upload_id}`, `POST /uploads/{upload_id}/complete`, and `POST /uploads/{upload_id}/abort` all return one flat upload-session object. Its `mode` is the transport chosen when the session began, and its `status` is `open`, `completed`, or `aborted`. The fields for each status are siblings of `status`, not a nested object:

```json
{ "namespace_id": "demo", "upload_id": "upl_...", "mode": "direct_multipart", "status": "open", "expires_at_ms": 1730000000000, "checksum_algorithm": "crc64nvme", "part_size_bytes": 8388608 }
{ "namespace_id": "demo", "upload_id": "upl_...", "mode": "direct_put", "status": "completed", "completed_at_ms": 1730000001000, "content_ref": { "kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_...", "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "<16 hex>" } }, "content_token": { "content_ref": { "kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_...", "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "<16 hex>" } }, "token": "<opaque>" } }
{ "namespace_id": "demo", "upload_id": "upl_...", "mode": "service_proxied", "status": "aborted", "aborted_at_ms": 1730000002000 }
```

A status read of a completed session returns a newly issued `content_token` while the window for issuing tokens is open, and the field is absent after that window. Completion returns the `completed_at_ms` that the session's terminal transition stored, and an idempotent replay returns the same stored timestamp.

Each state also has these rules:

- `POST /uploads/{upload_id}/abort` ends an open session and deletes the object it was writing. Repeating it succeeds and reports the existing abort, including the original stored `aborted_at_ms`. The server refuses to abort a completed session with `upload_already_completed`, because its content might already be published.
- An open session accepts `PUT /content`, part signing, and `complete` while the server clock reads before its `expires_at_ms`. From that instant on, each of them returns `upload_not_found`, as for an aborted session, and upload collection aborts the session later. `GET` still reports the session as `open`, and `abort` still ends it.
- An aborted session returns `upload_not_found` from `PUT /content`, from part signing, and from `complete`. That's the same response the server returns after the session is later physically removed, so the response doesn't change when cleanup runs. It's also what a completion receives when server-side cleanup aborted the session first. If the completion is recorded first instead, the cleanup's conditional write fails, and the completed session is kept.

A client uploads a large payload in one pass. It reads its source once, in order, and never keeps the whole payload in memory. The transport never requires more than that, and each step of the flow is designed so that the client doesn't need to do more. To upload in one pass, a client:

1. Begins a `direct_multipart` session. The begin request declares no length and no digest, so a source that can't report its length can start uploading immediately. The response settles how the payload is split into parts, and nothing else.
2. Reads the source one part at a time. For each part, it computes the part's `crc64nvme` for the URL the server signs, and it adds the same bytes to a running `crc64nvme` of the whole object, so one pass produces both checksums.
3. Asks for part URLs in waves instead of all at once (a signing request can list at most 1,000 parts), uploads each wave's parts, and records each part's `{part_number, etag, crc64nvme}`. The client keeps track of its own parts, exactly as it would with the provider's own multipart API.
4. Keeps no more than a bounded window of parts in memory at a time. That window is all the memory the upload uses, and its size doesn't depend on the payload's length. To retry a failed part, the client asks for its URL again. If a part is uploaded more than once, the provider keeps the last upload.
5. Completes the upload with the part list and the `{size_bytes, crc64nvme}` that the pass measured. The size and checksum are sent at completion because that's the first point at which a one-pass uploader knows them.

A session whose upload fails partway is aborted, so that its incomplete object can be deleted.

A provider assembles at most 10,000 parts, so a session can store at most `part_size_bytes` times 10,000 bytes. A longer payload is refused when the client asks the server to sign the part past that limit, so a client that knows its payload is very large asks for a larger part size when it begins the session.

To choose a transport, the file helpers first keep small content inline when the server supports it, as described in [Commit responses and safe retry](#52-commit-responses-and-safe-retry). Content that needs an upload and is smaller than one part is sent with `PUT /content` if it fits the proxy limit. For larger content, a client tries the transports that its deployment advertises, in this order:

1. `direct_multipart`, if the deployment advertises it. Multipart comes first because each part is retried on its own, and nothing needs to know the payload's length in advance.
2. `direct_put`, if the deployment advertises it and `size_bytes` is at most `upload.direct_put.max_content_bytes`. A provider offers this option when it can sign a write but has no multipart API. It's the only transport that sends one whole object directly to the provider. The client counts and hashes the bytes while it sends them, and then reports both values at completion. The source is read once, and it doesn't have to be kept in memory.
3. `PUT /content` with a streaming request body, if `size_bytes` is at most `upload.service_proxied.max_content_bytes`. The server hashes the payload as it forwards it. A body of unknown length is sent with chunked transfer encoding, and the server enforces the limit by counting the bytes as they arrive.

When the size is known, the client checks each option against the limits that the deployment advertises, never against an assumed limit. For example, a payload smaller than one part doesn't necessarily fit `upload.service_proxied.max_content_bytes`, because a deployment can set that limit to any value. If a payload is too large for all three transports, the client should refuse it and report the limits it exceeds, instead of sending it to the proxy to be refused there.

A declared length is only a hint. File metadata and other size hints can be out of date, so the client doesn't reject an upload based only on a hint. With direct PUT, the client counts the bytes while it sends them and reports the measured size at completion. The server compares that size with the provider limit and with the stored object. A source with no size hint can use direct multipart or direct PUT, because both measure the final size during the upload.

The `content_token` is the upload's receipt. The server issues a token only for a session that the store already records as completed. Each token identifies one `{namespace, content_ref}` pair, and tokens are short-lived.

If a commit response is lost, the client doesn't need to upload the bytes again. The completed session keeps the content durable, and those bytes don't move after completion, so reading the session returns a new token for the same bytes. The server stops issuing new tokens a set period after completion, and after that, a status read returns the session without a token. By then, the content is either referenced by committed metadata, which protects it without a token, or it can be reclaimed ([Upload-session cleanup](format.md#116-upload-session-cleanup)). A client that receives an expired or otherwise rejected token reads the session again and commits again with the new token.

Representative begin-upload response:

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_4d8f2c91a7b34e0f9c6d1a2b3e5f708c",
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
  "upload_id": "upl_4d8f2c91a7b34e0f9c6d1a2b3e5f708c",
  "mode": "service_proxied",
  "status": "open",
  "expires_at_ms": 1730000000000,
  "checksum_algorithm": "crc64nvme",
  "content_ref": {
    "kind": "blob_v1",
    "owner_namespace_id": "demo",
    "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
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
  "upload_id": "upl_4d8f2c91a7b34e0f9c6d1a2b3e5f708c",
  "mode": "service_proxied",
  "status": "completed",
  "completed_at_ms": 1730000001000,
  "content_ref": {
    "kind": "blob_v1",
    "owner_namespace_id": "demo",
    "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
    "size_bytes": 20591,
    "checksum": { "algorithm": "crc64nvme", "value": "7ab..." }
  },
  "content_token": {
    "content_ref": {
      "kind": "blob_v1",
      "owner_namespace_id": "demo",
      "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
      "size_bytes": 20591,
      "checksum": { "algorithm": "crc64nvme", "value": "7ab..." }
    },
    "token": "opaque-server-token"
  }
}
```

Why are content tokens short-lived? A commit is expected to follow its upload promptly. A rejected token also isn't an error the client has to plan around, because reading the completed session returns a new one.

### 6.10 Download transport

The download routes return short-lived signed URLs that let a client read a file revision's bytes directly from object storage.

A deployment MUST be able to serve back any file it let a client create. So every deployment that offers any direct write advertises `filesystem.downloads.direct_get`, and direct reads aren't a separate choice for the deployment. Direct reads also keep object traffic off the server.

A direct download grants one signed range for each immutable extent. If some of the revision's bytes are still pending WAL pieces, they're written as the chain's next extent, with the same writer that a fold uses. Existing objects are never rewritten. `content_not_materialized` means only that the deployment can't write the pending pieces. A chain that starts in the WAL tail reuses its whole object when that object covers the reference. For a zero-byte reference, the empty whole object is written if it doesn't exist.

`POST /v0/namespaces/{ns}/filesystem/downloads` takes a JSON body with a path and, optionally, the revision to read and the first byte to read:

```json
{ "path": "/docs/report.txt", "revision_no": 3, "start_offset": 1048576 }
```

Instead of `revision_no`, the body can include `snapshot_id` to read a snapshot, but it can't include both. `start_offset` defaults to 0.

The response contains a short-lived read capability, plus everything the reader needs to check the bytes it receives:

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
    "checksum": { "algorithm": "crc64nvme", "value": "42d..." }
  },
  "ranges": [{
    "start_offset": 1048576,
    "length": 313524224,
    "access": {
      "kind": "presigned_url",
      "method": "GET",
      "url": "https://bucket.s3.us-east-1.amazonaws.com/...&X-Amz-Signature=...",
      "headers": { "range": "bytes=1048576-314572799" },
      "expires_at_ms": 1780000000000
    }
  }]
}
```

The inode revision form is `POST /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{revision_no}/downloads`. It has no request body, it takes `start_offset` as an optional query parameter, and its response doesn't include a path:

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
    "checksum": { "algorithm": "crc64nvme", "value": "42d..." }
  },
  "ranges": [{
    "start_offset": 0,
    "length": 314572800,
    "access": {
      "kind": "presigned_url",
      "method": "GET",
      "url": "https://bucket.s3.us-east-1.amazonaws.com/...&X-Amz-Signature=...",
      "headers": { "range": "bytes=0-314572799" },
      "expires_at_ms": 1780000000000
    }
  }]
}
```

`POST /v0/namespaces/{ns}/inodes/{inode_id}/downloads` grants the current revision of a visible file inode, and it returns the same response. It has no body. Its optional `snapshot_id` query parameter grants the revision that the snapshot captured instead, and its optional `start_offset` query parameter is the first byte to read.

The download routes use the same provider support check and the same access format. The inode revision route stays available after a rename or a deletion, as long as the revision is retained. The current inode route follows the inode across renames, and it returns `inode_not_found` once the inode is deleted.

These five properties follow from the response's shape, and clients can rely on all of them:

- **Ordered ranges.** The grant covers exactly `[start_offset, size_bytes)`, as ordered ranges. The ranges are contiguous and ascending, and their lengths add up to `size_bytes - start_offset`. Each range refers to bytes in an immutable object, so later commits and appends don't change those bytes.
- **Signed headers.** A client sends each range's headers as given. Each range's `access.headers` includes the signed `Range` for its object. The object offset can differ from `start_offset`, which is an offset in the revision. The client sends the headers unchanged and adds no range of its own, and it checks each response's length against that range's `length`. A zero-byte revision has one range, at offset 0 with length 0, whose access signs no `Range` header, and it needs no object request.
- **Resuming.** To resume, a client asks for a new grant. A client that has the first `n` bytes asks for a grant with `start_offset` set to `n`. An offset at or past `size_bytes` returns `invalid_request` with `param` set to `start_offset`, except offset 0 for a zero-byte revision. A client that already has every byte doesn't ask for a grant.
- **Verification.** The client verifies the complete file across ranges. It reads the ranges in order, and it checks the complete byte length and `content_ref.checksum`. This applies to CRC-64/NVME and CRC-32C. A resumed download includes the prefix the client already has in the same checksum. A mismatch fails the download.
- **No object keys.** The raw object keys are never exposed. Each range includes a URL that expires, as a direct upload does.

The capability is short-lived. It lasts long enough for one transfer, not for a session. A reader that runs out of time asks for another grant starting at the byte it reached, which costs one small request and doesn't transfer any bytes again. A deployment that can't presign reads returns 501 `not_supported` with `feature = "filesystem.downloads.direct_get"`, and its proxied read stays available under its own limit. Such a deployment can't presign writes either, so no file it stores can be larger than it will proxy.

Why does every deployment with a direct write offer direct reads? `direct_put` and `direct_multipart` let a client write an object of any size, but a proxied read enforces the transfer limit `download.service_proxied.max_content_bytes`. Direct reads bypass that limit, so the deployment can serve back every file it let a client create. A deployment that offers no direct write can't have created such a file in the first place.

### 6.11 `GET /changes`

Returns a page of the namespace's change feed, which lists the commits after `after_seq`.

In an ACL namespace, the change feed requires an administrator when the request includes subject headers. Without subject headers, the request reads as the token holder.

Each change is one commit, represented as a `Commit` (the same object `POST /commits` returns). A change includes the commit's identity (`committed_seq`, `commit_id`, `committed_by`, the observational `committed_at_ms`, and the optional `message`) and its `events`, which are the semantic filesystem operations the commit applied, in the order it applied them.

One request operation can apply several semantic operations. For example, a put creates each missing parent directory, a replacing move deletes the file it moves over, and a copy applies the source's attributes to the inode it just created. So a request with three operations can report more than three events. The events stay in request order.

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
            "checksum": { "algorithm": "crc64nvme", "value": "7ab..." }
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
| `content_changed` | A file received a new current revision, from a replacing put or a revision restore (both are recorded as the same durable event). | `inode_id`, `revision_no`, `content_ref`. |
| `moved` | An entry moved to a new parent directory or name. | `inode_id`, `source_parent_inode_id`, `source_display_name`, `destination_parent_inode_id`, `destination_display_name`, `binding_version`. |
| `deleted` | A file or directory subtree was deleted. To restore it, use the enclosing `committed_seq` as `deletion_seq`. | `inode_id`, plus `deleted_binding` containing `parent_inode_id`, `name_key`, and `display_name`. |
| `undeleted` | A deleted inode was recovered and bound to a name again. | `inode_id`, `parent_inode_id`, `display_name`, `binding_version`. |
| `attributes_changed` | An inode's attributes changed. `attributes` is the complete flat string map after the update, so a consumer can update its own copy without reading anything back. An empty map means the attributes were cleared. | `inode_id`, `attributes_revision_no`, `attributes`. |
| `access_changed` | An inode's access row was replaced. `grants` is the complete direct grant map after the update. | `inode_id`, `access_revision_no`, `boundary`, `grants`. |

Directory and file creation use separate event shapes. A file creation always includes its first revision and content reference:

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
    "checksum": { "algorithm": "crc64nvme", "value": "7ab..." }
  }
}
```

Events identify inodes and their parent-directory bindings, not full paths. A consumer that needs paths can stat the inode, or maintain its own projection of the bindings from this feed. Clients MUST ignore unknown event kinds and unknown fields.

In a `moved` event, the source fields describe the removed binding, and the destination fields describe the new binding.

`directory_created`, `file_created`, `moved`, and `undeleted` include the `binding_version` they created, and it matches later reads of the same binding. Other events don't create bindings, and they omit the field.

The commit `limit` is an upper bound, and a page can contain fewer commits when their events are large. The reference implementation budgets 4 MiB of event heap per page, including vector capacity and owned event data. A commit is never split across pages, and a single commit larger than the budget is returned on a page by itself.

If either bound ends the page before the namespace head, the response includes `next_after_seq`, set to the last returned change's `committed_seq`. The client resumes with `after_seq={next_after_seq}`. Clients MUST follow `next_after_seq`, instead of counting commits, to decide whether another page remains.

`after_seq` can equal the current namespace head, which returns an empty page. An `after_seq` above the head that the request reads returns `invalid_request`. The lowest `after_seq` the feed accepts is `retention_floor_seq`, and a lower `after_seq` returns `rebootstrap_required`. The client then rebuilds its state from a fresh listing or checkpoint, and follows the feed from that state's head.

### 6.12 `POST /forks`

Creates a new namespace that starts as a copy of a source namespace, at the source's current head or at a snapshot.

In an ACL namespace, this operation requires an administrator subject. A request with no subject headers acts as the token holder.

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
  "naming": "case_insensitive",
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
| `source_namespace_id` | The namespace that the fork captured. |
| `source_head_seq` | The source sequence that the fork captured. |

The optional `snapshot_id` request field selects an unexpired snapshot of the source namespace. Without it, the server captures the current head.

The snapshot and the source namespace have to stay live while the server verifies the fork-owned checkpoint, which it does after it writes that checkpoint. The server returns these errors:

- A missing snapshot (including one deleted during fork verification), or an ID owned by another namespace or another checkpoint kind, returns `snapshot_not_found`.
- An expired snapshot returns `snapshot_gone`.
- A source deleted during fork verification returns `namespace_deleted`.

Forking doesn't extend the snapshot, and a later deletion of the snapshot doesn't affect the fork. The fork records the `Loonfs-Actor` header as `created_by`, independently of the source's creator. Namespace creation and forking produce no change-feed event.

The new namespace reads inherited content at each reference's owner key, and it starts its own metadata history. The fork shares the source's existing content and metadata objects instead of copying the filesystem. To create the fork, the server:

1. Might first fold the source's outstanding WAL tail, if the fork captures the current head. The fold writes that tail's inline content to content objects.
2. Creates a fork-owned checkpoint of the source, so the source-owned immutable metadata segments stay available for as long as the target might still read them.
3. Verifies the checkpoint.
4. Installs the target namespace with a conditional manifest write. The manifest records the source checkpoint for the target's lifetime.

The response contains the new namespace's initial state. Its head sequence and retention floor equal the captured basis sequence, and the target's first data commit is one sequence above that head. Inherited file history follows the floor rule from the start. After the target's first base compaction, each inherited file keeps its newest revision at or below the fork point, plus the revisions the target adds.

If the target ID is active, the server returns `namespace_exists` before it writes a source pin. If the target ID is deleted, the server returns `namespace_deleted` before it writes a source pin. If the source checkpoint can't be verified, the server returns `checkpoint_unavailable`, and no target namespace is installed.

### 6.13 `GET /grep`

Searches the content of the namespace's files for lines that match a pattern, and returns a page of matches.

The server filters out candidates that the subject can't read before it reads their content, and those candidates still count against the page budget. A scan without the index skips every directory the subject can't read, so files under such a directory can be found only through the index.

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

The pattern uses the Rust `regex` crate's dialect (no backreferences or lookaround), and it's compiled line-anchored, so `^` and `$` match line boundaries. To run a query, the server:

1. Plans the required grams from the pattern.
2. Intersects the namespace's grep index for those grams ([Grep extension format](format.md#appendix-d-grep-extension-format)).
3. Scans every revision committed after `built_through_seq`.
4. Verifies every candidate against the real pattern.

Because every candidate is verified, index staleness affects cost, never results. Matches are ordered by `(inode_id, byte_offset)`, and one match is reported per line.

Two budgets bound a page: the match limit, and a per-page budget of verified candidates. So a page can return fewer matches than its limit and still include a cursor. Each page is evaluated against the namespace head at page time, and it reports that head in `head_seq`. The cursor resumes strictly after the last candidate that the issuing page finished scanning, and it's bound to that request, so replaying it with different criteria is rejected as `invalid_request`.

An active index stays enabled when its watermark is ahead of the head of the query's read view. The query takes a new read view once, and continues with the index it loaded.

Grep cursors tolerate head drift under the same forward-only rule as every other cursor in the API (see [`GET /filesystem/entries` and `GET /inodes/{inode_id}/children`](#65-get-filesystementries-and-get-inodesinode_idchildren)). The server accepts a grep cursor issued at an older head, and the resumed page is evaluated at the current head and reports it. Each page is internally consistent at its own head, but a multi-page search spans whatever heads its pages ran at. Candidates the cursor has passed aren't revisited, even if later commits changed them.

A search is a bounded sampling read over content, not a promise to enumerate every match. A client that needs one consistent cut across pages issues the search again when `head_seq` changes between pages. A cursor from a head newer than the serving view's head returns `invalid_request`, because drift is tolerated forward but not backward.

A pattern with no required literal bytes is rejected with `query_unindexable`, unless the request opts into a capped exhaustive scan with `allow_scan`. A tail longer than the scan budget is rejected with `index_lagging`, unless the request accepts results from the index alone with `allow_stale` (reported as `tail_scanned: false`). Stale results are a consistent cut at the index watermark, so files whose newest revision is newer than the watermark are left out entirely instead of mixed in.

The index covers every retained file, including deleted files and files under a deleted directory, and each query checks the current visibility of every candidate. So a delete or an undelete changes no postings, and an exact query after an undelete includes the restored files without a rebuild.

The `path_prefix` value is a complete absolute path, not a partial prefix of a path segment. The server resolves it using the namespace's naming mode ([Names and paths](format.md#14-names-and-paths)), and then limits results to descendants of that inode. So `path_prefix` MUST use the same canonical spelling as any other path. A scope that doesn't exist returns `path_not_found`, and an existing scope that's empty returns a successful response with no matches.

`inode_id` selects the scope by inode instead, and limits results to the inode's descendants wherever the inode is bound. A request can include `path_prefix` or `inode_id`, but not both. A request with both returns `invalid_request` with `param` set to `inode_id`. An inode that isn't visible returns `inode_not_found`.

If the data half is missing, the request returns `not_supported` with the `feature` field set to `query.grep`, the same key that capability discovery advertises the serving half under (see [Data-dependent features](#23-data-dependent-features)).

### 6.14 `GET /metrics`

Returns metrics about what the process has been doing, in Prometheus text exposition format 0.0.4, with `Content-Type: text/plain; version=0.0.4`.

`GET /metrics` is an operational route, like the two liveness routes. `GET /health` returns `ok` while the process is up, and `GET /readiness` returns `ready` while the process still admits work.

Unlike `/health` and `/readiness`, `/metrics` requires the deployment's bearer token. A scrape sends the same `Authorization: Bearer` header that a client does, and an unauthorized scrape returns `401 unauthorized` in the standard error envelope.

Metric names have the form `loonfs_<subsystem>_<metric>`. They cover the object-store calls the process made, the maintenance passes it finished, the publications it batched, what garbage collection reclaimed, and the requests this server served. Label values come only from closed vocabularies. For example, a request is labeled by the route template it matched (`/v0/namespaces/{namespace_id}`), never by its own path, so no namespace, upload, commit, or checkpoint ID ever appears in a label. Values are per process, and they reset when the process restarts, which is the usual contract for a counter that a scraper reads.

The metric set isn't part of the v0 wire contract. Metric names might be added or changed as the runtime's instrumentation grows, and clients MUST NOT depend on any particular series existing.

Why does `/metrics` require the bearer token when the liveness routes don't? The liveness routes report only whether the process is alive, which a load balancer needs and nobody can misuse. The metrics describe a deployment's traffic, the kind of work its namespaces do, and its failure rates, and that information isn't public.

## 7. Conformance requirements

This section lists the requirements for a conforming server, and for a conforming writer or client.

### 7.1 Server requirements

A conforming server MUST:

1. Treat object storage as the authoritative durable foundation.
2. Publish visible metadata only through logical commits stored in visible numbered WAL objects.
3. Make referenced content durable no later than the commit that references it (see [File contents and ownership](format.md#15-file-contents-and-ownership)).
4. Preserve `(namespace_id, inode_id)` as the canonical identity of an item, and preserve the namespace lifetime described in [Namespaces and identity](format.md#11-namespaces-and-identity).
5. Resolve content through the reference's owner namespace and content ID.
6. Implement tombstone-first delete.
7. Serve replay from the highest-numbered verified manifest found through `hint.json`, plus the numbered WAL objects after the folded boundary, replayed as logical commits. Checkpoints pin manifest versions for retention, stable reads, restore, and forks.
8. Map sibling names to name keys by each namespace's naming mode, and apply each mode's rule exactly (see [Names and paths](format.md#14-names-and-paths)).
9. Keep control-plane sessions and any implementation-specific coordinators out of namespace history and the change feed.
10. Preserve per-commit idempotency, ordering, and change-feed identity, even when it physically batches logical commits in one WAL object.
11. Advertise its API groups and features truthfully in the capability document, and never advertise an API group whose required ops aren't implemented.
12. Respond to unimplemented or disabled parts of the API with `not_supported` (and its `feature` name), never with undefined behavior.

### 7.2 Writer and client requirements

A conforming writer or client MUST:

1. Treat paths as selectors, not as durable identity.
2. Upload content before it asks the server to publish it, unless the commit includes the bytes inline.
3. Use commit IDs, or equivalent idempotency keys, for safe retry.
4. Tolerate commit rejection when preconditions are no longer met.
5. Re-bootstrap if its cursor falls behind the retention floor.
6. Gate optional parts of the API on the capability document instead of probing, and treat `not_supported` as authoritative when the two disagree.
7. Ignore unknown JSON response fields, unknown error codes, and unknown feature keys.

A sync client MUST also maintain durable local state for its cursor and reconciliation logic.

## 8. Client patterns

This section describes common client patterns. A pattern is defined by the part of the API a client uses, not by whether the client is a CLI, desktop app, web app, SDK, or service, and a single client can follow more than one pattern. (These are usage patterns, not the conformance groups defined in [API groups](#1-api-groups).)

### 8.1 Path-oriented client

A path-oriented client uses the path-oriented surface of the API.

Typical behavior:

- `ls`, `stat`, `get`, `put`, `mkdir`, `mv`, and `cp` use user-visible paths.
- The server stays authoritative for path resolution, canonical inode identity, and commit validation.
- Small commands are often sessionless.
- Large or recursive commands can be implemented as sequences of ordinary logical commits.

This client doesn't need a sync database or a full local mirror. An implementation can still keep durable local state when that improves usability, restart safety, or resumability. Examples include auth and session state, retry journals, pinned snapshot IDs, and inode context learned from earlier responses.

The reference CLI loads defaults only when reading its config metadata returns a not-found error. Any other metadata error returns an invalid-config error that includes the path, and the CLI leaves the file unchanged.

### 8.2 Sync client

A sync client maintains durable local state, and it reads the change feed over time.

Typical behavior:

- It maintains a durable cursor.
- It projects remote state into local state.
- It can upload content and publish commits of its own.
- It preserves conflicts according to the client's conflict policy.

### 8.3 Operator or admin tool

An operator or admin tool uses low-level recovery or inspection APIs that are specific to an implementation or a deployment.

## 9. Operation statefulness

This section defines when an operation is a single request, when it uses a control object, and how responsibility is split between the client and the server for the common filesystem commands.

A one-shot operation is fully described by a single request, and it normally doesn't require durable control-plane state. A long-running operation can span multiple requests, can require a stable snapshot or destination binding, and might need to resume after a client or server restart.

For the operations in this section, v0 defines upload sessions but not read sessions.

### 9.1 Definitions

| Term | Meaning |
| --- | --- |
| Single-request operation | An operation that's fully described by a single request, and that doesn't require a server-side control object after the request completes. |
| Control object | Server-side state for an operation that spans multiple requests. A large or resumable `put <file>` can use one upload session. |
| Implementation-specific coordinator | A helper resource or service that correlates multiple logical commits for one higher-level workflow. Coordinators are outside the core interoperable model, and they don't define namespace history. |
| Authoritative operation state | The state that determines the correctness, visibility, and resumability of an operation. |
| Transfer progress | Non-authoritative progress information, such as completed bytes, completed files, local temporary outputs, or user-interface counters. |

### 9.2 Normative rules

1. A LoonFS operation MAY be a single-request operation only when all of the following are true:
   - The operation is fully described by one request.
   - The operation completes synchronously.
   - No retained read view is required after the request returns.
   - No stable destination binding is required after the request returns.
   - The request can be retried by replaying the full request.

2. A LoonFS operation MAY use a server-side control object when any of the following are true:
   - The client will continue the operation across multiple requests.
   - The operation requires a pinned snapshot for consistent reads.
   - The operation requires a stable destination binding across time.
   - The operation requires resumable multi-part upload.
   - Losing the server's state on a restart would change correctness, retention safety, or promised resumability guarantees.

   A large or resumable `put <file>` that needs server-side state typically uses one upload session.

3. The core specification doesn't require a server-side job object for recursive `put` or recursive `cp`. Those workflows can be implemented as one or more logical commits, optionally coordinated by implementation-specific helpers outside the core model.

4. When a server-side control-plane object is used, the authoritative identity of the in-flight interaction MUST become the server-issued object identifier. After that point, the original path string is only the input that started the interaction, and it MUST NOT remain the sole identifier of the in-flight interaction.

5. Server-side control-plane objects MUST NOT advance the namespace `seq`, MUST NOT appear as filesystem-visible resources, and MUST NOT appear in the namespace change feed.

6. Implementation-specific coordinators MAY exist, but they MUST NOT redefine logical commit boundaries or change-feed semantics.

### 9.3 Statefulness by operation

| Operation | Typical execution shape | Typical server-side control-plane object | Long-lived server state required? | Server is authoritative for | Client is authoritative for |
| --- | --- | --- | --- | --- | --- |
| `get <file>` | Single-request read | none | No | path resolution, access check, selected file revision, content serving or delegated download | local download progress, temporary file, client retries |
| `get -r <dir>` | Client-driven multi-request read | none in v0 | No | each request's path resolution, access check, and selected file revision | traversal progress, local outputs, client retries |
| `put <file>` (small, one-shot convenience) | Single request | none | No | destination resolution, validation, metadata commit | request payload, client retries |
| `put <file>` (large or resumable) | Begin, upload, commit | upload session, if used | Only if server-side resumability or a stable binding is promised | stable destination binding, expected slot or revision, upload session validity, final publish | file reading, hashing, content upload progress, retry tokens |
| `put -r <dir>` | Client- or coordinator-driven uploads plus one or more logical commits | none required by the core model | No | each commit's validation and publication | orchestration across files, progress, retries |
| `cp <file>` (same server) | Single-request server-side copy | none | No | source resolution, destination resolution, metadata publication, content reference reuse | request retry |
| `cp -r <dir>` (same server) | Client- or coordinator-driven sequence of logical commits | none required by the core model | No | each commit's validation and publication | orchestration across entries, retries |
| `cp remote -> local` | Alias for `get` or `get -r` | same as `get` | same as `get` | same as `get` | same as `get` |
| `cp local -> remote` | Alias for `put` or `put -r` | same as `put` | same as `put` | same as `put` | same as `put` |

### 9.4 Client and server split for common commands

This table defines the normative split of responsibility for the primary filesystem commands:

| Command | Server responsibilities | Client responsibilities |
| --- | --- | --- |
| `get <file>` | Resolve the requested path or handle, authorize the read, select the file revision to read, and serve bytes or delegated download targets. | Receive bytes, write local output, and maintain local retry and resume state. |
| `put <file>` (one-shot) | Resolve the destination, validate preconditions, and publish the metadata change. | Supply bytes or a content reference, and retry the request if needed. |
| `put <file>` (resumable) | If an upload session is used, create or validate it. Bind the destination, validate durable content, and commit the final publish. | Read the local file, hash it, upload content, track upload progress, and submit the final commit request. |
| `cp <file>` (same server) | Resolve the source and destination, authorize both sides, create the copied resource, and publish the metadata change. | Submit the request, and retry if appropriate. |

### 9.5 When raw paths cease to identify the operation

LoonFS accepts path-oriented input because users naturally express what they want as paths. However, a long-running operation requires a more stable identity than a raw path string.

| Operation | Raw path is used for | Stable in-flight identity after start |
| --- | --- | --- |
| `get <file>` | the request itself | none required |
| `put <file>` (resumable) | `create_upload` only | server-issued upload session, if used |

### 9.6 Control-plane durability guidance

1. A control object MUST be durably recorded if losing it on restart would change correctness, visibility, retention safety, or promised resumability.

2. An upload session MUST be durable when it provides a stable destination across requests, or when it has to survive a server restart.

3. Implementation-specific coordinators MAY also be stored durably, but this specification doesn't require it.

4. Control objects and any implementation-specific coordinators MUST remain outside namespace-visible metadata. Their existence is authoritative for orchestration, not for filesystem history.

### 9.7 Recommended defaults

A conforming implementation SHOULD use the following defaults, unless a stronger mode is explicitly requested:

- `get <file>` is a single-request operation.
- `cp <file>` on the same service is a single-request operation.
- If a large or resumable `put <file>` uses a control object, it uses a single upload session.

These defaults keep a simple model for single-request commands, and still allow multi-request correctness and resumability where they matter.

## 10. Client and server responsibilities

This section summarizes which concerns the server is responsible for and which the client is responsible for.

| Concern | Server | Client |
| --- | --- | --- |
| Path resolution | Authoritative | Supplies user intent by path when it uses the filesystem surface. |
| Content hashing and upload | Can accept inline bytes, proxy uploads, or issue upload capabilities. MUST verify that uploaded content referenced by a commit is durable before it publishes the commit. Can issue short-lived content admission tokens after validation, to avoid repeating expensive checks. | Usually responsible for reading local bytes, computing content hashes, and uploading missing content when it creates new data. Can forward admission tokens when they're provided, but MUST tolerate slow-path validation. |
| Commit validation | Authoritative | Supplies preconditions and commit IDs where needed. |
| Namespace visibility | Authoritative | Observes committed sequence receipts and change-feed deltas. |
| Long-running transfer progress | Authoritative for sessions that affect correctness | Responsible for local temp files, local progress, retry behavior, and any higher-level orchestration outside the core model. |
| Capability truthfulness | Advertises only implemented API groups and features. | Gates on the capability document, and reconciles through `not_supported`. |

## 11. Extension points

This section describes where LoonFS can be extended.

The preferred extension point is the committed change feed. Downstream systems (such as indexers, notification services, preview builders, or policy engines) should consume committed changes instead of becoming part of the core mutation path.

New client-visible operations are added to this document as API group ops or as features in the [Feature registry](#22-feature-registry). New durable state is added to `format.md`. New scheduling machinery is up to each implementation. Cross-store discovery (naming authority, search, ownership, and quotas) is out of scope for this specification.

### SDK streaming downloads

The handwritten transfer helpers provide verified download streams in every SDK:

| SDK | Open a stream | Cancel or release it |
| --- | --- | --- |
| Go | `client.Files.DownloadStream(ctx, input)` | Close `Content` or cancel `ctx` |
| Python (`LoonFS` and `AsyncLoonFS`) | `client.files.download_stream(namespace_id, path=path, request_options=options)` (await it with `AsyncLoonFS`) | Use a `with` block or call `close()`. With async, use `async with` or `await aclose()`. |
| TypeScript server/browser | `client.files.downloadStream(input, requestOptions)` | Cancel the `content` reader or abort `requestOptions.abortSignal` |

The existing buffered download helpers collect these streams. Streams verify the size and checksum incrementally, and they report a verification error on the same iterator, reader, or stream that delivers the bytes. Only a stream that's successfully read to the end verifies the file. Cancelling a stream or closing it early doesn't verify the file. Consumers MUST handle late errors, even after some bytes have been delivered. A failed body is never automatically replayed.

Both direct and proxied downloads pass the caller's transport controls through:

- Go respects the caller's context deadline, and it uses a 60-second operation deadline when the caller doesn't supply one.
- TypeScript applies `timeoutInSeconds` (the client default, otherwise 60 seconds) across the metadata request and the body, and it also respects `abortSignal`.
- Python uses the request or client HTTPX timeout for I/O waits on both transports. Its synchronous iterator closes the response when the caller exits a `with` block early, and the async iterator uses an `async with` block.

API authorization, cookies, and API headers aren't copied into presigned object-store requests. Each direct request sends its range's `access.headers` unchanged, and it adds no `Range`. The helpers check each response's length, and one checksum across all ranges. A grant of zero bytes makes no request.

### SDK streaming uploads

Streaming preparation accepts these sources:

- In Go, a caller-owned `io.Reader`.
- In synchronous Python, a binary file object.
- In async Python, an `AsyncIterator[bytes]` or a binary file object.
- In TypeScript, a `Blob`, a `ReadableStream<Uint8Array>`, or an `AsyncIterable<Uint8Array>`.

It returns the same prepared content that the byte helpers use, without publishing a filesystem entry:

| SDK | Prepare once | Prepare and publish once |
| --- | --- | --- |
| Go | `Files.PrepareStream(ctx, namespaceID, reader, sizeBytes)` | `Files.UploadStream(ctx, files.StreamUploadInput{...})` |
| Python (`LoonFS` and `AsyncLoonFS`) | `files.prepare_stream(namespace_id, content=reader, size_bytes=size)` (await it with `AsyncLoonFS`) | `files.upload_stream(namespace_id, content=reader, ...)` (await it with `AsyncLoonFS`) |
| TypeScript server/browser | `files.prepareStream({ ...namespace, content, size_bytes }, options)` | `files.uploadStream(input, options)` |

The optional size is checked against the bytes consumed, and TypeScript infers it for a `Blob`. Small streams, including streams of unknown size, become inline prepared content when the server supports it. After the inline lookahead, a larger source of unknown size uses multipart when the deployment advertises it, and a service-proxied upload otherwise.

SDK reads are limited to 64 KiB, and a multipart upload keeps one provider-sized part plus at most 10,000 part descriptors in memory. Async Python and TypeScript also keep the caller's current chunk, so callers should produce bounded chunks too. A TypeScript source of known size up to 8 MiB is sent as a bounded request body of known length, for browser compatibility. Larger single-request uploads, and single-request uploads of unknown size, require runtime support for streaming request bodies. Multipart sends each part as a body of known length, so it doesn't require that support. The existing byte helpers use these same paths.

Upload preparation and publication use the same caller transport controls as downloads. Go and TypeScript deadlines cover the combined `UploadStream` or `uploadStream` operation, and Python's timeout bounds HTTP I/O waits. Async Python runs blocking file reads in a worker thread, so the event loop keeps running. The owner of a caller-owned Go or Python source MUST interrupt a blocking read from it, because network cancellation can't interrupt arbitrary source code. Callers close Go and Python sources. TypeScript cancels or returns the source it consumed when the upload completes or fails.

Payloads are consumed once and never automatically replayed. Source errors, size mismatches, failed payload requests, and async Python cancellation during staging prevent completion. Each of them starts a best-effort abort, with its own five-second cleanup timeout (an I/O timeout in synchronous Python). An ambiguous completion failure leaves the session available for inspection.

After a successful preparation, a caller that retries publication keeps the prepared result and retries `UploadPrepared`, `upload_prepared`, or `uploadPrepared` with identical publication inputs. It doesn't read the stream again or start a new upload to retry publication.

### SDK appends

The handwritten helpers add bytes to the end of an existing file with one `create_commit` request that includes an `append_file` operation:

| SDK | Append bytes |
| --- | --- |
| Go | `client.Files.Append(ctx, files.AppendInput{...})` |
| Python (`LoonFS` and `AsyncLoonFS`) | `client.files.append(namespace_id, path=path, content=data, ...)` (await it with `AsyncLoonFS`) |
| TypeScript server/browser | `client.files.append(input, requestOptions)` |

The request includes the bytes as `inline_content`, so an append needs no upload. Before it sends anything, each helper refuses empty content and content larger than 256 KiB. It raises the same error that its upload helpers raise for a size no transport can handle: an `error` in Go, a `ValueError` in Python, and an `Error` in TypeScript. The other inputs are optional: the commit ID, the commit message, `expected_inode_id`, and `expected_revision_no`. A caller that might retry passes the commit ID explicitly, because a retry with the same bytes and commit ID replays the original commit. No helper sends `append_file_by_inode`, so a caller sends that operation with `commits.create`.
