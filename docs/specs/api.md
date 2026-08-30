# LoonFS API Specification

This document is the normative specification of the LoonFS client-facing API:
profiles, capability discovery, the standard error contract, operation
statefulness, and the representative v0 HTTP binding. It is **normative where implemented** — a
deployment chooses which optional profiles to expose, but every op it does
expose must have the shape specified here.

The companion document is `format.md` — the durable format, mandatory for
every implementation.

The same client codebase works against an embedded engine and a hosted
server: both expose the same operations, advertise their capabilities the
same way, and report unsupported surface area with the same errors. The only
difference a client observes is *which* capabilities a deployment advertises.

## 1. Profiles are functional planes

**A profile is a functional plane** — a coherent area of responsibility with
its own endpoint set — not a resource type. Profiles
are **all-or-nothing** conformance units: a deployment MUST NOT advertise a
profile unless every required op in it is implemented. Optional behavior
within a plane is expressed as **named features** (section 2).

| Profile | Plane | Ops | Status |
| --- | --- | --- | --- |
| `core/v0` | Data plane | Path and inode reads (stat, list, content, revisions), path mutations, staged uploads, the change feed, namespace state by id, `GET /v0/capabilities`, and the standard error contract. Namespace `create`, `fork`, and `delete` are **features** within this profile, as is inode child listing (`core.inodes.list_children`). | **Mandatory** for any conforming deployment |
| `admin/v0` | Maintenance plane | Read namespace storage diagnostics; create and release checkpoints; run one-shot maintenance steps that select any of metadata upkeep (WAL flush plus reorganization), retention-floor advancement, and garbage collection. Maintenance triggers for derived indexes arrive as features in this plane: grep-index administration is the `admin.grep.index` **feature**. | Optional |
| `query/v0` | Query plane | Content search over derived indexes (`GET /v0/namespaces/{namespace_id}/grep`). Grep-index search is the `query.grep` **feature** within this profile; using it also requires a materialized active grep root for the namespace. | Optional |
| `acl/v0` | Authorization plane | — | **Reserved name only.** Do not specify ops yet. Clients must tolerate unknown error codes, so authorization errors can land with this plane without breaking anyone. |

Notes:

- An embedded engine is `core/v0` (with the namespace-management features enabled) plus
  `admin/v0` (maintenance is manually triggerable). A minimal server that
  wraps the embedded engine over HTTP advertises the same two profiles and is
  fully conformant.
- A hosted server may disable individual features per tenant and typically
  hides `admin/v0` because maintenance runs automatically. Both choices are
  fully conformant.
- Profiles version independently (`core/v0` could coexist with a future
  `admin/v1`). The plane name — the segment before the slash — is the stable
  identity that feature keys reference.
- No queue or job-scheduling semantics exist in this document. `admin/v0`
  exposes *trigger* and *status* shapes only; how work is scheduled is
  implementation freedom.

### 1.1 Where new behavior belongs

When new behavior arrives, three questions decide where it belongs:

1. Does it change bytes or metadata another implementation must interpret?
   It belongs in `format.md`, and it is mandatory.
2. Is it a client-visible operation whose shape should be uniform wherever it
   exists? It belongs here, inside a profile or as a named feature.
3. Is it about *how* work gets done — queues, schedulers, caches? That is
   implementation freedom and belongs in no specification document.

## 2. Capability discovery

### 2.1 The capability document

Every deployment describes itself with one capability document. A remote
client fetches it from `GET /v0/capabilities` and caches it for the connection; an
embedded engine exposes the same document as a constant. SDK gating logic is
therefore identical for both backends.

The example below is the reference deployment: the runtime's own `core/v0`
and `admin/v0` planes plus the `query/v0` plane the server composes from the
grep extension. A host that composes no extension advertises neither that
profile nor the two grep feature keys: `query.grep` for searching an index
and `admin.grep.index` for administering one. Clients gate on the document
either way.

```json
{
  "protocol_version": "v0",
  "profiles": ["core/v0", "admin/v0", "query/v0"],
  "features": {
    "admin.grep.index": true,
    "core.namespaces.create": true,
    "core.namespaces.fork": true,
    "core.namespaces.delete": true,
    "core.snapshots": true,
    "core.attributes": true,
    "core.inodes.list_children": true,
    "core.write_guards": true,
    "query.grep": true
  },
  "limits": {
    "commit.max_content_tokens": 4096,
    "commit.max_external_content_refs": 4096,
    "commit.max_message_bytes": 4096,
    "commit.max_operations": 4096,
    "maintenance.gc.min_grace_window_ms": 1230000,
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
| `protocol_version` | The protocol generation, currently `v0`. |
| `profiles` | The advertised profiles. Each entry is `plane/version`. |
| `features` | Named features and whether this deployment supports them. An absent key means unsupported. |
| `limits` | Advisory numeric limits clients may use to pre-validate requests. May be empty. |

Rules:

- Profiles are all-or-nothing for their required ops; clients never probe
  op-by-op inside an advertised profile.
- **Feature-key rule (normative):** every feature key is dotted and its first
  segment MUST be the plane name of an advertised profile. A feature whose
  first segment does not match an advertised profile is malformed; clients
  MUST reject the document or ignore that key.
- Clients must ignore unknown feature keys and unknown document fields.
- Limits are advisory; the authoritative outcome is the server's response to
  the actual request.

Registered limit keys:

| Limit key | Meaning |
| --- | --- |
| `pagination.default_limit` | Default page size applied when a paged request omits `limit`. An explicit `limit=0` is rejected with 400 `invalid_request`. |
| `pagination.max_limit` | Largest accepted page size for paged requests. A `limit` greater than this value is rejected with 400 `invalid_request`. |
| `upload.max_content_bytes` | Largest request body accepted for service-proxied upload content (`PUT .../uploads/{upload_id}/content`). Clients may use `direct_put` for larger content only when `core.uploads.direct_put` is advertised; otherwise they must stay within this limit. |
| `upload.direct_put_max_content_bytes` | Largest object this deployment's provider accepts in one presigned `direct_put` request. Unrelated to `upload.max_content_bytes`, which bounds service-proxied uploads. A size hint above this limit returns `content_too_large` at begin, and completion checks the actual stored size. Advertised only alongside `core.uploads.direct_put`. |
| `upload.completion_max_body_bytes` | Largest JSON body accepted by `POST .../uploads/{upload_id}/complete`. Larger requests return `content_too_large`. |
| `download.max_content_bytes` | Largest file content a service-proxied read (`GET .../filesystem/content` or `GET .../inodes/{inode_id}/revisions/{revision_no}/content`) will buffer and return in one response. Over-limit reads answer `content_too_large`; v0 has no proxied streaming or range reads. A file past this limit is read through the corresponding path or inode download grant when `core.downloads.direct_get` is advertised — which it is on exactly the deployments that could have let a client create such a file. |
| `upload.max_concurrent` | How many service-proxied upload bodies the deployment buffers at once; requests past the cap answer `server_busy`. |
| `download.max_concurrent` | How many service-proxied content reads the deployment materializes at once; requests past the cap answer `server_busy`. |
| `commit.max_operations` | Most path operations one commit may carry. A longer list answers `invalid_request` before planning, on every transport. |
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
| `admin.grep.index` | Administering a namespace's grep index: `GET /v0/admin/namespaces/{ns}/grep/index` and its `enable`, `disable`, and `gc` routes. | The maintenance half of the grep capability, and independent of `query.grep`: searching an index and keeping one built are separately deployable, so a deployment may advertise either key alone. A deployment that maintains no index answers all four routes `not_supported` with this key. |
| `core.namespaces.create` | Creating namespaces (`POST /v0/namespaces`). | |
| `core.namespaces.fork` | Forking namespaces (`POST /v0/namespaces/{ns}/forks`). | |
| `core.namespaces.delete` | Deleting namespaces (`DELETE /v0/namespaces/{ns}`). | Terminal, and the id is permanently retired. Derived state becomes reclaimable through a maintenance step that selects `gc` alone (section 6.3), which also reclaims the content of any upload session that completed, aged past the derived reclamation grace, and is referenced by nothing the namespace can reach. A deployment may still advertise `false` and answer `not_supported`. |
| `core.snapshots` | Creating, listing, extending, and releasing snapshots under `/v0/namespaces/{ns}/snapshots`. | |
| `core.attributes` | Writing inode attributes (`update_attributes`) and projecting them onto `GET /filesystem/entry` and `GET /filesystem/entries`. | Implemented by the core runtime rather than composed by a host, so a deployment serving `core/v0` advertises it. |
| `core.inodes.list_children` | Listing a directory's children by parent inode ID (`GET /v0/namespaces/{ns}/inodes/{inode_id}/children`). | Implemented by the core runtime rather than composed by a host, so a deployment serving `core/v0` advertises it. The key exists so inode-driven sync clients can gate on deployments built before the route existed. |
| `core.write_guards` | Replacing puts, moves, and copies can require the inode or revision that the client read. | Implemented by the core runtime, so `core/v0` advertises it. |
| `core.uploads.direct_put` | Starting presigned `direct_put` upload sessions (`POST /v0/namespaces/{ns}/uploads`). | The server returns a short-lived, create-only presigned PUT capability for the exact content object. The provider must report a durable whole-object checksum after the write. The key is present only on an endpoint the live conformance suite has run against. Independent of `core.uploads.direct_multipart`: a provider may offer this and no multipart API at all. Raw object keys and caller-managed object-store writes are not part of this feature. |
| `core.uploads.direct_multipart` | Starting presigned `direct_multipart` upload sessions (`POST /v0/namespaces/{ns}/uploads`) and signing their parts (`POST /v0/namespaces/{ns}/uploads/{upload_id}/parts`). | The server opens the provider's multipart upload and returns one short-lived, checksum-bound capability per part. It needs an S3-style multipart API on top of the signing the other keys need, so a provider without one advertises this key alone as absent. |
| `core.downloads.direct_get` | Taking path or inode download grants (`POST /v0/namespaces/{ns}/filesystem/downloads` and `POST /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{revision_no}/downloads`). | The server returns a short-lived presigned GET capability for the selected content object. Any deployment that offers a direct write advertises this too, because one that lets a client create an object larger than `download.max_content_bytes` must be able to hand that object back. Raw object keys are not part of this feature. |
| `query.grep` | Content search (`GET /v0/namespaces/{ns}/grep`). | The serving half of a data-dependent capability: the request also requires a materialized active grep root, and a namespace without one answers `not_supported` whatever this key advertises. |

`admin/v0`'s only feature key is `admin.grep.index`; the rest of that plane
is required ops. `acl.*` keys are unregistered until that plane
materializes.

Namespace listing is intentionally not supported in v0. Callers must address
namespaces by id until LoonFS has a scalable namespace catalog/index design.

### 2.3 Data-dependent features

The capability document describes a *deployment*. What is materialized on
*data* — for example, whether a derived index is ready for a namespace — lives
in the owning extension's keyspace (`format.md`, "Extension-owned
materialization"), not in the namespace manifest.

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
    "active_writer": "server-b",
    "active_acquired_at_ms": 1739459200000
  }
}
```

`param` is present when the error identifies one invalid input. Its format
depends on where the input came from:

| Input source | `param` value |
| --- | --- |
| JSON request body | JSON Pointer |
| Query parameter | Parameter name |
| Path parameter | Parameter name |
| CLI-local input | Flag or argument spelling |

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
| `writer_fenced` | `fenced_writer_epoch`, `active_writer_epoch`, plus `active_writer` and `active_acquired_at_ms` when the head recorded a writer block. Writer ids are process labels, so two runs on one machine can share one; the acquisition stamp is what tells them apart |
| `stale_revision` | `inode_id`, `expected_revision_no`, `actual_revision_no` (absent when the inode has no current revision) |
| `stale_attributes` | `inode_id`, `expected_attributes_revision_no` (absent when the caller stated no expectation), `actual_attributes_revision_no` |
| `commit_id_reuse_conflict` | `commit_id`, plus `committed_seq` and `committed_fingerprint` when the conflict was decided against a durable commit receipt — the sequence that `commit_id` already landed at, and the semantic identity of what landed there (section 5.1). Both come from the receipt, so both are present or neither is; both are absent when nothing has committed under the id yet and two live requests are claiming it at once |
| `rebootstrap_required` | `after_seq`, `retention_floor_seq` |
| `stale_head` | `expected_head_seq`, `actual_head_seq`, when the failure was a caller-supplied `expected_head_seq` precondition rather than a raced head advance. A caller that still means to delete retries against the sequence it found |
| `not_deleted` | `inode_id`, plus `expected_deletion_seq` and `actual_deletion_seq` when a live deletion exists at a different generation |
| any failed commit | `commit_id` — the idempotency key the request committed under, echoed so failed and uncertain outcomes carry the caller's reconciliation handle (section 5.2) |
| any commit carrying more than one operation | `operation_index` — the position of the operation that stopped the request (section 5.1) |

One code exists specifically so capability handling is uniform from day one:

- `not_supported` (HTTP 501): the deployment does not implement the requested
  op or feature. Any op may return it; a client maps the error to its
  `feature` key and disables or degrades that code path.

The full registry (`ErrorCode` in `loonfs-api`):

| Code | HTTP status | Meaning |
| --- | --- | --- |
| `invalid_request` | 400 | The request is malformed: a path, id, cursor, parameter, staged content reference, configuration value, or commit request limit fails validation. The message names the offending field or limit. |
| `unauthorized` | 401 | Missing or wrong credentials. |
| `content_too_large` | 413 | A request or proxied response exceeds its advertised size limit. Send smaller proxied uploads or use `direct_put` when available. Multipart completions must fit within `upload.completion_max_body_bytes`. For large reads, request a download grant when `core.downloads.direct_get` is available. |
| `route_not_found` | 404 | No route matches the request path. |
| `method_not_allowed` | 405 | The path exists but does not serve this HTTP method. |
| `namespace_not_found` | 404 | The namespace has no head, so it does not exist. |
| `namespace_deleted` | 410 | The namespace's head records the terminal deleted status. The id is permanently retired, so a create or fork against it fails here rather than as a conflict. |
| `snapshot_not_found` | 404 | The snapshot id names no checkpoint record. Refresh state or choose another snapshot. |
| `snapshot_gone` | 410 | The snapshot record exists but is released or expired. The message names which terminal condition applies. |
| `path_not_found` | 404 | No visible entry at the path. |
| `inode_not_found` | 404 | The requested visible or retained inode does not exist. |
| `revision_not_found` | 404 | The file has no such revision. |
| `upload_not_found` | 404 | No upload session with this id, or one that was aborted: an aborted session will never select content, so it reports the absence that its deletion will. |
| `namespace_exists` | 409 | The create or fork target already exists: another namespace holds the id. |
| `snapshot_quota_exceeded` | 409 | Creating the snapshot would pass the namespace's live-snapshot limit. Release a snapshot or wait for a lease to expire. |
| `content_not_prepared` | 409 | A path put or explicit create/replace operation references external content without a matching admission, or carries a rejected relevant token. Prepare the content and retry with its proof. |
| `path_conflict` | 409 | The destination path is already bound. |
| `directory_not_empty` | 409 | The directory has children and the operation is not recursive. |
| `stale_head` | 409 | The write raced a head advance, or a caller-supplied `expected_head_seq` no longer matches the head; retry against fresh state. |
| `stale_revision` | 409 | A caller-supplied base revision is no longer current. |
| `stale_attributes` | 409 | The inode's attribute revision moved while the update was being decided. Two things raise it: a caller-supplied expected attribute revision that is no longer current, and the revision guard every attribute update carries even when the caller states no expectation. Re-read the attributes and retry. |
| `binding_generation_mismatch` | 409 | The binding generation supplied for an inode move or delete is no longer current. Re-read the entry before retrying. |
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
| `server_busy` | 503 | The server is at its configured concurrency limit for this kind of work (proxied upload bodies or proxied content reads); back off and retry. |
| `shutting_down` | 503 | The serving process closed admission for shutdown; work admitted earlier still settles. Retry against a live instance. |
| `deadline_exceeded` | 503 | The server cancelled a bounded request at its configured `request_deadline_ms`. A commit may still land after this response; reconcile it by commit id before retrying. |
| `checkpoint_unavailable` | 503 | Required checkpoint state is unavailable: not yet published, released during the operation, or referenced material is missing. Retry after maintenance. |
| `maintenance_required` | 503 | Namespace metadata requires maintenance before the request can be served; run maintenance and retry. |
| `index_lagging` | 503 | The grep index trails the head past the exhaustive-scan budget; let the grep worker catch up (or set `allow_stale`) and retry. |
| `storage_permission_denied` | 503 | The backing object store rejected the deployment's storage credentials for this operation. Fix the storage credentials or bucket policy; an unchanged retry will not succeed. |
| `index_corrupt` | 500 | The grep index's derived state failed validation. Disable and re-enable grep on the namespace to rebuild it; core filesystem state remains available. |
| `namespace_corrupt` | 500 | Durable namespace state failed validation. |
| `server_error` | 500 | Unclassified internal failure. |

Automated retry is narrower than the HTTP status. Raw transport failures may
be retried. Of the registered error codes, only `commit_queue_full`,
`server_busy`, and `shutting_down` can clear without caller or operator action.
`checkpoint_unavailable`, `maintenance_required`, and `index_lagging` require
maintenance. `storage_permission_denied` requires the operator to fix the
storage credentials or bucket policy. `commit_outcome_unknown` and
`deadline_exceeded` require the caller to determine whether a mutation
completed before retrying it.
Responses carrying one of the three immediately retryable codes include
`Retry-After: 1`.

Precondition failures surface as `409` resource-state conflicts
(`stale_revision`, `stale_head`, `commit_id_reuse_conflict`) rather than
`412`: v0 treats them as conflicts with current namespace state, not HTTP
conditional-request failures.

## 4. SDK shape

One SDK serves both backends; deployment mode never forks the client
codebase.

Generated SDKs use schema names as public type names. A resource body uses the resource name, such as `Checkpoint` or `UploadSession`. A response envelope uses `<Verb><Noun>Response`, such as `ListCheckpointsResponse`. Namespace-owned resources include `namespace_id`.

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
- As optional planes gain ops, SDKs should group them the way the planes are
  grouped (`core`, `admin`, and later), so the surface a deployment does not
  support is visibly absent instead of failing call by call.

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

The writer surface has three stages:

1. make content durable
2. make metadata visible
3. observe ordered changes through the change feed

This split is deliberate:

- content durability is not visibility;
- WAL-segment durability is not visibility by itself; and
- head advance is the visibility point.

A commit request may therefore be rejected immediately, or tentatively
accepted into a WAL batch, without yet being a committed or successful change.

The embedded `loonfs::FsWriter` makes the first two stages independently
drivable. `prepare_file_bytes` stages bytes, while `prepare_content_ref`
fully validates an existing reference and re-homes its bytes under a fresh
content identity owned by the target namespace; both return opaque prepared
content. `put_file_prepared` consumes that evidence without content-store I/O
during publication. `complete_upload_prepared` returns the ordinary
completion response together with the same evidence. These are embedded
conveniences, not HTTP operations; hosted clients continue to carry validated
content tokens on the existing wire requests.

Staging is staging wherever it happens, so `prepare_file_bytes` opens an
upload session for the object it writes, exactly as a remote upload does: the
session record lands before the bytes and completes after them. The opaque
prepared value carries an admission deadline no later than the expiry of the
last token the completed session could issue, and publication checks it when the
batch is admitted. This is the same horizon that bounds remote upload tokens
(section 6.3; format spec, "Garbage collection", rule 11), so content cannot
be reclaimed and then admitted through an older in-process proof.

Prepared evidence is bound to both the namespace and its content store. Two
namespaces sharing a content store therefore cannot exchange prepared values:
their collectors have different metadata roots and upload-session records.
The explicit `prepare_content_ref` import is the safe handoff when only a ref
is available; the returned prepared value names the fresh target-owned ref,
not the input ref.

### 5.1 Commit identity and race guards

A commit is one request: a `commit_id` — a client-generated stable
idempotency key that must be reused verbatim for safe retries — a required
application-asserted `actor` with a `kind` (`user`, `service`, or `system`)
and opaque `id`, an optional `message` (a human-readable annotation that is part of the commit's
identity), and an ordered, non-empty list of path operations. A request with
one operation is the same shape as a request with many, so a convenience
call and a one-element list are the same commit and fingerprint alike.
A `message` is at most 4096 bytes; a longer one is rejected with
`invalid_request` before planning, on every transport.

The operations of one request commit together, in order, as one logical
commit. Operation `k` is planned against authoritative namespace state plus
everything operations `0..k` do, so a request can create a directory and
write into it, or delete a path and recreate it. Either every operation
commits or none does: the first operation that fails aborts the whole
request, nothing it or its predecessors would have written becomes visible,
and — when the request carried more than one operation — the error names the
position that stopped it in `details.operation_index`. The error code stays
the failing operation's own; no code is specific to batching.

The server plans each operation against authoritative namespace state under
the publish lock, synthesizing the exact semantic checks the operation
implies (revision identity, binding identity, name absence, directory
emptiness, ancestor visibility) so races fail explicitly rather than
silently merge. Those checks are evaluated where their operation runs, which
is what lets a later operation depend on an earlier one. Callers add their
own cross-request guards on operations where staleness matters. Puts can
check the current inode and revision. Moves and copies can check the
destination inode and revision. Deletes, undeletes, inode-addressed writes,
and namespace deletion have guards for their corresponding state. Each guard
checks the state visible to its operation, including changes made by earlier
operations in the same request.

Commit bodies reject unknown fields so a misspelled guard cannot be ignored. For example, dropping a letter from `expected_revision_no` returns `invalid_request` instead of applying an unguarded write.

Every named entry includes a `binding_generation`, an opaque token identifying its current parent/name binding. Creating, moving, or undeleting an entry produces a new token; content and attribute writes do not. Clients must not parse or order these tokens.

Inode-addressed moves and deletes require the token as `expected_binding_generation`. A valid token that no longer matches returns `binding_generation_mismatch`; a malformed token or one from another namespace returns `invalid_request`. The guard is part of the commit's identity and is evaluated after any earlier operations in the same request.

The server validates each request against authoritative namespace state and
may reject it immediately. A tentatively accepted request becomes one
committed logical commit only after its WAL segment is durably written and the
head update succeeds; a segment written before a failed head update is
orphaned, and the request is not committed.

If the head update's outcome was never observed — a transport failure after
the update was sent — the server reports `commit_outcome_unknown`: the commit
may already be visible. Section 5.2 defines how the caller resolves it.

The server may publish multiple committed logical commits in one WAL segment
and one head update, but it must preserve per-commit idempotency, ordering,
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

### Actor attribution

Every commit includes an actor with a `kind` and `id`. The application chooses
this value, and LoonFS stores it on the commit and the metadata created by that
commit. LoonFS does not authenticate the actor. The application must
authenticate the user and authorize the operation before sending the request.
Use a stable internal ID, not an email address or display name.

Actor kind and actor id are part of the semantic commit fingerprint. Reusing a
`commit_id` with a different actor id or kind fails with
`commit_id_reuse_conflict`. The commit timestamp is not part of the
fingerprint.

Responses expose attribution through these fields:

| Field | Meaning |
| --- | --- |
| `created_by`, `created_at_ms` | Commit attribution for inode creation. |
| `revision_committed_by`, `revision_committed_at_ms` | Commit attribution for the current file revision on stat and list entries; absent on directories. |
| `commit_id` | Owning commit identity on revision-history items and committed changes. |
| `committed_by`, `committed_at_ms` | Commit attribution on revision-history items and committed changes. |
| `attributes_updated_by`, `attributes_updated_at_ms` | Commit attribution for the latest stored attribute update; absent for the initial empty attributes at revision 0. |
| `deleted_by`, `deleted_at_ms` | Commit attribution for an active trash entry. |

### 5.2 Commit responses and safe retry

Every commit returns the same response envelope: the `namespace_id` that changed, the
`commit_id` it committed under, and the `committed_seq` where it
became visible. When the caller did not supply a commit id, the surface that
accepted the request generates one and returns it, so every caller holds the
identity it needs to reconcile an uncertain outcome.

The response also includes `committed_by`, `committed_at_ms`, the optional `message`, and `events`. These match the change feed, so the caller immediately receives values such as newly created inode IDs.

A replay reads these fields from the retained WAL record. If that record has been retired but its commit receipt has not yet been removed, the response omits `events`. The receipt still supplies the commit ID, sequence, actor, timestamp, and message.

The retry rule has three cases:

- Resubmitting the semantically identical commit with the same `commit_id`
  is safe: if the original committed, the response replays the original
  `committed_seq` without committing again; if it never committed, the
  resubmission completes it.
- Reusing a `commit_id` for a different commit fails with
  `commit_id_reuse_conflict`.
- Retrying with a new `commit_id` is a new logical commit.

**The replay guarantee has a horizon.** A commit's receipt lives exactly as
long as retained history: when the retention floor passes a commit and
metadata runs are rebuilt, its receipt is dropped, and from then on the id
is indistinguishable from one never used. A retry past that horizon does
not replay and does not conflict — **it commits again as a new mutation.**
The effects are bounded and mostly self-announcing: creates fail
`destination_exists`, moves and deletes fail against the already-moved
state, and a put lays down a duplicate revision of identical content
(the read surface is unchanged; revision history gains one entry). Retries
should therefore happen promptly — within the retention window, they are
fully guaranteed — and a caller that must reuse ids beyond the window is
responsible for its own reconciliation. LoonFS chooses this documented
horizon over an unbounded receipt index deliberately: detecting a dropped
id would require remembering every id forever.

Write guards are part of the commit's identity. Reusing a `commit_id` after
changing a guard fails with `commit_id_reuse_conflict`.

A put's content is part of that identity too, and identity means *which
content object*, not what bytes it holds. So:

- **Retrying a commit means resending the same `content_ref`.** That is a
  semantically identical commit and it replays.
- **Re-uploading the bytes and then reusing the `commit_id` conflicts at the
  server.** A fresh upload mints a fresh content object, which is a
  different commit, answered with `commit_id_reuse_conflict`.

Keep the `content_ref` a completed upload returned and reuse it across
commit retries; that is the cheapest retry and the one the server can
answer on its own.

**Client-side retry reconciliation.** Rerunning a whole upload-then-commit
sequence under one `commit_id` — the shape of a command rerun, where no
`content_ref` survived the first attempt — is nonetheless safe when the
request is identical. A client that composes upload and commit must, on
`commit_id_reuse_conflict`, resolve it as follows before surfacing it.

The proof is the commit's whole semantic identity, not a selection of its
parts. The conflict reports `details.committed_fingerprint`, the fingerprint
the server's receipt holds for what landed. The client recomputes the
fingerprint its own retry would have had if the only difference were the
fresh content object, and the two values must be equal. Comparing the whole
value is what makes the check complete: a path, a `behavior`, an
`expected_revision_no`, a `message`, or an operation count that differs is
a different mutation, and every field a future request gains is covered the
day it joins the preimage.

1. Find what that `commit_id` committed. There is no read keyed by commit
   id, but the error names where it landed: read the change feed once at
   `details.committed_seq` — cursor `after_seq = committed_seq - 1`, limit 1
   — and take the row whose `commit_id` matches. If `details.committed_seq`
   or `details.committed_fingerprint` is absent, or the feed answers
   `rebootstrap_required` because retention has moved past that sequence, or
   the row there names some other commit, there is nothing to compare: go
   straight to rule 5.
2. Take the committed content reference from that row. The row must carry
   exactly one content-bearing event — a `file_created` or a
   `content_changed`. A single put produces exactly one, however many
   parent directories it also had to create, because directory creations
   carry no content ref. None, or more than one, means the commit was not
   this put: go to rule 5.
3. Recompute the fingerprint of the request just made, with that committed
   `content_ref` substituted for the freshly uploaded one, and compare it
   against `details.committed_fingerprint`. Any difference means the two
   requests are not the same mutation: go to rule 5. The preimage is the
   one defined in section 5.1, so annotations are compared exactly as the
   server fingerprints them — an absent message and an empty one are
   different commits, and a client must not fold both into "no message".
4. Compare the uploaded bytes with the committed content reference's
   `checksum`. If the client still has the bytes, it recomputes the required
   checksum. If it streamed the bytes, it uses the checksum calculated during
   that stream. The algorithms must match; the client must not compare or
   convert different algorithms.

   If both the fingerprint and checksum match, report the original commit and
   its `committed_seq`.
5. Otherwise, report failure. Preserve `commit_id_reuse_conflict` when the
   fingerprints or content differ. Also fail when the original commit cannot
   be found or the client cannot compute the required checksum. An incomplete
   comparison is never treated as success.

The duplicate content object the rerun uploaded is then referenced by
nothing. That is by design: it is a completed upload whose content no
metadata names, and content garbage collection reclaims it once its grace
passes (format spec, "Garbage collection", rule 11).

This reconciliation is a client obligation, not a server behaviour: the
server's answer to a reused id with different content is always
`commit_id_reuse_conflict`.

Identical resubmission is the reconciliation mechanism. There is no separate
commit-status lookup: after `commit_outcome_unknown`, a transport failure, or
a process restart, resubmit the same request with the same `commit_id` and
read the definitive answer from the response.

The blocking Rust client retries operations labeled `idempotent` or `replayable`. It makes one attempt for operations labeled `not_idempotent`: namespace create, fork, and delete; upload-session begin; checkpoint create; maintenance; grep-index collection; and store probe. Presigned direct PUT also receives one attempt.

Commits record a durable receipt binding the `commit_id` to its
`committed_seq`; replay reads that receipt, and a reuse conflict reports the
`committed_seq` it read. The replay window is the namespace's retention
floor: metadata reorganization drops receipts whose `committed_seq` has
fallen below the floor, so resubmitting from below the window is a new
logical commit rather than a replay of the original.

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

The standard mutation operations are defined in `format.md` ("Standard
mutation operations"). `POST /commits` (section 6.8) exposes those operations
over HTTP. The same identity, durability, and visibility rules apply to every
API that implements them.

## 6. Representative HTTP binding

HTTP is one transport binding for these abstract operations. It is not the
underlying semantics.

GET routes name resources, so they use nouns such as `entry`, `entries`, `content`, `revisions`, and `trash`. A POST route ends in a verb when it invokes an action rather than creating a resource, as in `run`, `release`, `enable`, `disable`, `gc`, `abort`, `complete`, and `probe`. `/v0/admin/` is the only plane prefix. Other routes are grouped by resource, including `GET /v0/namespaces/{ns}/grep`.

Operation IDs start with a verb. `get` reads one resource, `list` reads a page, and `create` posts a new resource to a collection. Other verbs describe the operation directly, as in `grep`, `run_maintenance`, and `release_checkpoint`. Generated SDK method names come from these IDs, so changing an ID also changes the generated method name.

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

Request bodies reject unknown fields, at every level of nesting, with 400
`invalid_request`. Most request fields are optional and several of those are
preconditions, so a field the server does not recognize cannot be ignored: a
misspelled guard would decode to its default and the server would carry out
a different request than the caller asked for. Response bodies are the other
way round, because a client must keep working against a server newer than
itself and so must tolerate fields it does not know (section 7.2).
`ContentRef`, `Checksum`, and `ActorRef` are closed shapes on both sides: a
response never adds a field to one of them, and new content strategies or
checksum algorithms arrive as new `kind` and `algorithm` values, which is why
those three schemas are `additionalProperties: false` in responses too. The
encoding conventions in `format.md` state the same rules and extend them to
durable shapes.

Query strings reject unknown parameters. For example, `DELETE /v0/namespaces/{ns}?expected_head_sq=418` returns 400 `invalid_request` rather than deleting the namespace without the intended guard. Routes that declare no query parameters reject all query parameters. `GET /health`, `GET /readiness`, and `GET /metrics` are exceptions because probes and scrapers may append their own parameters.

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
| Create a namespace | `create_namespace` | `not_idempotent` | `POST /v0/namespaces` |
| Read a namespace | `get_namespace` | `idempotent` | `GET /v0/namespaces/{ns}` |
| Read a path entry | `get_path_entry` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/entry?path=/docs/report.txt&include_attributes=false&snapshot_id=...` (`include_attributes` is optional and defaults to `true`; `snapshot_id` is optional) |
| Read an inode | `get_inode` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}?include_attributes=false` (the parameter is optional and defaults to `true`) |
| List path entries | `list_path_entries` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/entries?path=/docs&limit=100&cursor=...&include_attributes=true&snapshot_id=...` (`include_attributes` is optional and defaults to `false`; `snapshot_id` is optional) |
| List directory children by inode | `list_inode_children` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}/children?limit=100&cursor=...&include_attributes=true` (the parameter is optional and defaults to `false`) |
| List file revisions by path | `list_file_revisions` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/revisions?path=/docs/report.txt&limit=100&cursor=...` |
| List file revisions by inode | `list_file_revisions_by_inode` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}/revisions?limit=100&cursor=...` |
| Read current or prior file content by path | `get_file_bytes` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/content?path=/docs/report.txt&snapshot_id=...` (`revision_no` and `snapshot_id` are optional and mutually exclusive) |
| Read prior file content by inode | `get_file_revision_bytes_by_inode` | `idempotent` | `GET /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{revision_no}/content` |
| Start a download by path | `create_download` | `idempotent` | `POST /v0/namespaces/{ns}/filesystem/downloads?snapshot_id=...` (`snapshot_id` is optional and cannot be combined with the body's `revision_no`) |
| Start a download by inode | `create_download_by_inode` | `idempotent` | `POST /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{revision_no}/downloads` with body `{}` |
| List recoverable deletions | `list_trash` | `idempotent` | `GET /v0/namespaces/{ns}/filesystem/trash?limit=100&cursor=...` |
| Create a commit | `create_commit` | `replayable` | `POST /v0/namespaces/{ns}/commits` |
| Create an upload session | `create_upload` | `not_idempotent` | `POST /v0/namespaces/{ns}/uploads` |
| Upload content through the server | `put_upload_content` | `idempotent` | `PUT /v0/namespaces/{ns}/uploads/{upload_id}/content` |
| Create multipart upload URLs | `sign_upload_parts` | `idempotent` | `POST /v0/namespaces/{ns}/uploads/{upload_id}/parts` |
| Complete an upload | `complete_upload` | `replayable` | `POST /v0/namespaces/{ns}/uploads/{upload_id}/complete` |
| Read an upload session | `get_upload` | `idempotent` | `GET /v0/namespaces/{ns}/uploads/{upload_id}`; completed sessions return a fresh `content_token` |
| Abort an upload session | `abort_upload` | `idempotent` | `POST /v0/namespaces/{ns}/uploads/{upload_id}/abort` (terminal and repeatable; a completed session is refused) |
| Read committed changes | `list_changes` | `idempotent` | `GET /v0/namespaces/{ns}/changes?after_seq=123&limit=100&snapshot_id=...` (`snapshot_id` is optional) |
| Create a snapshot | `create_snapshot` | `not_idempotent` | `POST /v0/namespaces/{ns}/snapshots`; requires `name` and `ttl_ms` |
| List snapshots | `list_snapshots` | `idempotent` | `GET /v0/namespaces/{ns}/snapshots?limit=100&cursor=...` |
| Extend a snapshot | `extend_snapshot` | `idempotent` | `POST /v0/namespaces/{ns}/snapshots/{snapshot_id}/extend`; requires `ttl_ms` and clamps to the lifetime ceiling |
| Release a snapshot | `release_snapshot` | `idempotent` | `POST /v0/namespaces/{ns}/snapshots/{snapshot_id}/release` (idempotent and one-way) |
| Fork a namespace | `fork_namespace` | `not_idempotent` | `POST /v0/namespaces/{source_ns}/forks` |
| Delete a namespace | `delete_namespace` | `not_idempotent` | `DELETE /v0/namespaces/{ns}?expected_head_seq=418` (feature `core.namespaces.delete`; the precondition is optional) |
| Read namespace diagnostics | `get_namespace_diagnostics` | `idempotent` | `GET /v0/admin/namespaces/{ns}/diagnostics` |
| Create a checkpoint | `create_checkpoint` | `not_idempotent` | `POST /v0/admin/namespaces/{ns}/checkpoints`; requires `name` and accepts `ttl_ms` |
| List checkpoints | `list_checkpoints` | `idempotent` | `GET /v0/admin/namespaces/{ns}/checkpoints?limit=100&cursor=...` |
| Release a checkpoint | `release_checkpoint` | `idempotent` | `POST /v0/admin/namespaces/{ns}/checkpoints/{checkpoint_id}/release` (idempotent and one-way; records owned by another operation are rejected) |
| Run maintenance | `run_maintenance` | `not_idempotent` | `POST /v0/admin/namespaces/{ns}/maintenance/run` |
| Search file contents | `grep` | `idempotent` | `GET /v0/namespaces/{ns}/grep?pattern=needle&case_insensitive=false&path_prefix=%2Fsrc&allow_scan=false&allow_stale=false&limit=100&cursor=...`; requires the `query.grep` feature and an active index |
| Read grep index status | `get_grep_index` | `idempotent` | `GET /v0/admin/namespaces/{ns}/grep/index` |
| Enable the grep index | `enable_grep_index` | `idempotent` | `POST /v0/admin/namespaces/{ns}/grep/index/enable`; idempotent |
| Disable the grep index | `disable_grep_index` | `idempotent` | `POST /v0/admin/namespaces/{ns}/grep/index/disable`; idempotent |
| Collect grep index garbage | `gc_grep_index` | `not_idempotent` | `POST /v0/admin/namespaces/{ns}/grep/index/gc`; supports `max_objects` and `next_cursor` |
| Test object storage | `probe_store` | `not_idempotent` | `POST /v0/admin/store/probe` with body `{}` |
| Scrape metrics | `get_metrics` | `idempotent` | `GET /metrics` (Prometheus text exposition; authorized, unlike the liveness routes — see below) |

The status, enable, and disable routes all return one flat grep-index object:
`namespace_id`, lifecycle fields tagged by `status`, `next_run_no`, and
`reorganize_pending`. Lifecycle statuses never share a sequence field:

| `status` | Carries | Means |
| --- | --- | --- |
| `disabled` | — | No index is maintained here. Also the answer for a namespace that never enabled one. |
| `backfilling` | `target_seq`, `cursor_inode_id`, `checkpoint_id` | The initial walk over a pinned checkpoint is running. `target_seq` is the namespace sequence that checkpoint captured; reaching it completes the backfill. Nothing is searchable yet, and no watermark exists to report. |
| `active` | `built_through_seq`, `next_event_index` | The index follows the change feed. Commits at or below `built_through_seq` are searchable, except that a non-zero `next_event_index` leaves the rest of that one commit unindexed. |

For example:

```json
{"namespace_id":"demo","status":"active","built_through_seq":12,"next_run_no":3,"reorganize_pending":false}
```

```json
{"namespace_id":"demo","status":"backfilling","target_seq":12,"cursor_inode_id":"ino_4","checkpoint_id":"chk_00000000000000000000000000000009","next_run_no":1,"reorganize_pending":false}
```

A backfill therefore never reports a `built_through_seq`, and an active index
never reports a `target_seq`. `next_run_no` is the run number the index
allocates next, while `reorganize_pending` reports whether a partitioned
segment reorganization is in progress. A client waiting for the index to catch up
captures one sequence before it starts waiting and stops there, rather than
chasing a head that keeps moving.

A maintenance step selects its actions by naming them, and runs the ones it
named in a fixed order:

| Field | Action |
| --- | --- |
| `metadata_maintenance` | Folds the visible WAL tail into metadata segments and advances the metadata root once the tail reaches `max_wal_tail_segments`, then merges one bounded metadata reorganization unit. The two are one action: folding a tail is what creates the delta runs a merge consumes. |
| `retention` | Advances the retention floor to the flushed manifest head. |
| `gc` | Runs one bounded garbage-collection pass. |

Every field above is an options object. Include a field to select that action; an empty object uses the server defaults. `retention` has no options yet, so its value must be an empty object.

A body that names no action is rejected as `invalid_request`, which includes
sending no body at all. None of the actions creates a checkpoint record.

This request selects all three actions:

```json
{"metadata_maintenance":{"max_wal_tail_segments":8},"retention":{},"gc":{"max_objects":1024}}
```

Each selected action reports under the same field that selected it. `metadata_maintenance` contains `wal_flush` and `reorganize`, `retention` contains `retention_floor_seq`, and `gc` contains the collection result. An absent field means the action was not selected. Compare `retention.retention_floor_seq` with `status_before.retention_floor_seq` to see whether the floor moved. Races and supersessions are outcomes, not errors.

Outcome names describe what the step observed. The same name has the same meaning in every maintenance response.

A deleted namespace accepts a step that names `gc` alone, which is how its
reclaimable state is collected; naming anything else is refused with
`namespace_deleted`, because a tombstone has nothing to flush, reorganize,
or retain.

`wal_flush.outcome` has four values. `not_needed` means the WAL tail was below the threshold. `flushed` means this step published a manifest and updated the root. `already_published` means the root already referenced a different manifest, so this step did not update it. `retries_exhausted` means concurrent updates prevented every attempt from publishing; nothing was flushed, and a later step can try again.

Four reorganize outcomes describe work the step did not do itself. A family
group that has outgrown one step is rebuilt by a background streaming
compaction, and the step publishes nothing in that case: the job publishes
once, when it finishes. `reorganize.outcome` says what became of that job.

`compaction_started` means this step started one. `compaction_at_capacity`
means this step's job claimed the namespace but is waiting for a process
compaction permit, because the process is already running its limit of them;
it starts when one frees. `compaction_running` means a job was already
running for this namespace, which runs one at a time, so this step started
none and a later step plans the group again. `compaction_required` means the
group needs a job and the handle serving the request has no background work
behind it at all, so nothing will run one until an operator does; the
self-hosting guide names the call.

`root_advanced` means another publisher updated the metadata root first. The manifest written by this step remains unreferenced, and a later GC pass can delete it. A later maintenance step retries the reorganization.

Inside `metadata_maintenance`, `max_wal_tail_segments` overrides the flush threshold. Zero and values above the write-rejection threshold return `invalid_request`. Replay history is retained unless the request includes `retention`. Inside `gc`, `grace_window_ms` overrides the grace window, `max_objects` limits one pass, and `cursor` resumes a previous pass. A grace window below the derived safety floor or a zero budget returns `invalid_request`. Upload sessions and staged content have additional protections beyond `grace_window_ms`: each session has a lease, and the protection period for completed-session content is derived rather than configured (format spec, "Garbage collection", rule 11).
`max_objects` bounds the whole pass, from its first read to its last, and
not only the candidates it enumerates. Building the live root set spends it
too: the head and metadata root together, the retention floor, each
checkpoint record, each live manifest, each manifest the pass ages to find
its reference manifest, and each retained WAL segment, read
once, while marking. A WAL segment request the store fails is charged like
one it serves, so a flaky store spends the budget faster than the retained
chain is long. Deciding whether a completed session's content is still
referenced then spends it again on each live manifest and the revision rows
inside it. A pass that runs out partway through that scan skips
completed-content reclamation for the rest of the invocation: the session is
retained, `content_reclamation_deferred` is set, and the sweep goes on
through every other candidate under the usual budget. Deletion only ever
follows a complete collection, so a partial one decides nothing. A budget
too small for the roots themselves does nothing at all. It reports
`budget_exhausted` alongside `content_reclamation_deferred`, deletes
nothing, and returns the cursor it was given byte for byte. A budget that
covers the roots and no more behaves the same way, except that it did
finish marking, so it does not set `content_reclamation_deferred`. Any pass
that stops without deciding a candidate returns its submitted cursor
unchanged, and a caller that loops on `next_cursor` should stop when the
token it gets back is the token it sent. A budget above the roots but below
the reference scan on top of them keeps completed content rather than
reclaiming it, and a pass with room for the whole scan collects it later.
Step-driven GC defaults `max_objects` to 1024 and returns any `next_cursor`
for a later step rather than looping internally. Nothing sweeps unless `gc`
is present.

The retention floor bounds incremental replay only. File revision history
is never pruned: a revisions listing is always complete, however far the
floor has advanced.

#### Checkpoint inventory

A checkpoint name is a label, not a key. Every create call generates a new record, so the same name may identify multiple checkpoints. Create and list use one checkpoint object with `namespace_id`, `checkpoint_id`, `owner`, `created_at_ms`, optional `expires_at_ms`, `checkpoint_seq`, and `manifest_no`. Create returns this object directly. For API-created checkpoints, `owner` is `user` with the requested `name`, and `created_at_ms` is the durable record timestamp.

For example, a create response is:

```json
{"namespace_id":"demo","checkpoint_id":"chk_00000000000000000000000000000009","owner":{"kind":"user","name":"release"},"created_at_ms":1752623000000,"expires_at_ms":1752626600000,"checkpoint_seq":12,"manifest_no":9}
```

`GET /v0/admin/namespaces/{ns}/checkpoints?limit=100&cursor=...` returns active
checkpoints in ascending `checkpoint_id` order. Each entry is the same
checkpoint object returned by create. User checkpoints can be released by
id. Fork checkpoints retain their `fork` owner and remain while their target
namespace still reads through them.

```json
{"namespace_id":"demo","checkpoints":[{"namespace_id":"demo","checkpoint_id":"chk_00000000000000000000000000000009","owner":{"kind":"user","name":"release"},"created_at_ms":1752623000000,"expires_at_ms":1752626600000,"checkpoint_seq":12,"manifest_no":9}]}
```

Release is idempotent and returns only the addressed namespace and
checkpoint. The response is identical whether this call released an active
record or the record was already released or reaped:

```json
{"namespace_id":"demo","checkpoint_id":"chk_00000000000000000000000000000009"}
```

`limit` follows the advertised pagination limits. `next_cursor` is omitted
after the final page. Cursors are opaque and tied to this namespace and
operation; clients should only return them unchanged.

This is a live listing, not a snapshot. Checkpoints created, released, or
collected while a client is paging can affect later pages.

Released records are absent, because a release is what stops a record
pinning anything. A record whose `expires_at_ms` has passed is still
present, with that instant in the entry: expiry is not release — garbage
collection is what turns a passed expiry into one — so until a pass reaches
it the record is still a root, and reads still serve from it. Listing it is
the honest answer to the question the route is asked.

#### Snapshots

A snapshot is a time-bounded view of a namespace. Its checkpoint record id is
its `snapshot_id`. Creation requires `name` and `ttl_ms`. The ttl cannot exceed
`snapshot.max_ttl_ms` or `snapshot.max_lifetime_ms`.

An extension measures its requested ttl from the server's current time. It
never moves the expiry past `snapshot.max_lifetime_ms` from the record's
`created_at_ms`. A namespace may hold at most
`snapshot.max_live_per_namespace` live snapshots.

Snapshot listing returns only snapshot-owned records whose leases have not
expired. The admin checkpoint listing keeps expired records visible until
collection releases them. Snapshot release is idempotent and one-way. A
second release succeeds, including after the record is reaped.

These operations manage the snapshot lifetime. Path stat, directory listing,
file content, download, and change-feed requests accept an optional
`snapshot_id`. File content and download requests cannot combine `snapshot_id`
with `revision_no`; the snapshot selects the revision. A snapshot change feed
ends at the captured sequence, and `after_seq` cannot exceed that sequence.

Snapshot reads require a live snapshot. Missing snapshots return
`snapshot_not_found`, while released or expired snapshots return
`snapshot_gone`. Neither case falls back to the current namespace state.

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

Routes under `/v0/admin/` belong to the `admin/v0` profile. `GET /v0/namespaces/{ns}/grep` belongs to `query/v0`. Everything else shown belongs to `core/v0`.

When a GC pass sees a future reclamation deadline, its response includes
`next_reclamation_at_ms`, the soonest time still ahead of the pass at which
something it retained becomes reclaimable: an open upload session's lease
plus the grace window, an aborted session's grace, or a completed session's
derived content-reclamation grace. A scheduler reads it to decide when to
run the namespace again rather than tracking upload deadlines itself. It
describes only what this pass examined — a pass that stopped on `next_cursor`
saw part of the keyspace, and candidates that age out on their object
timestamps carry no time here — so its absence is not a claim that nothing
is owed.

GC responses carry `next_cursor` only when more candidate enumeration remains.
The token is opaque, tolerant of additive fields when decoded, and valid only
against the namespace that issued it. It encodes the last examined key and
object family, not a live set or retention proof: every resumed invocation
reloads the current roots, WAL floor, and checkpoint protections before any
deletion. If the namespace advances or keys disappear between calls, a stale
cursor may re-examine work or defer a newly inserted key that sorts before its
position until the next full pass; it can never make a newly live object
deletable.

A GC response groups related counts. `deleted` contains counts for `wal_segments`, `metadata_segments`, `manifests`, `checkpoint_records`, `upload_sessions`, and `content_objects`. `released_checkpoints` contains `fork`, `expired`, and `missing_basis` counts. Every count field is present, including zero values.

Every GC response also carries `retained`, which is `retained_candidates`
split by the decision that spared each candidate. The reasons are a closed
set, so every field is always present and a zero means nothing was kept for
that reason, and the fields sum to the total:

| Reason | Means |
| --- | --- |
| `referenced` | Selected as unreachable, then found reachable by the re-verification that runs immediately before every deletion. A candidate the pass already knew was reachable is never examined, so this counts the namespace moving underneath the pass, not the size of its live set. |
| `within_grace_window` | Unreachable, but younger than `grace_window_ms` by the object's own provider timestamp. |
| `no_provider_timestamp` | Unreachable, and the provider reported no last-modified time, so the object's age is unknown and it is treated as young. |
| `no_reference_manifest` | Unreachable and aged, but the namespace has published no manifest old enough to say what it referenced when the grace window opened. A reader that pinned its anchor inside the window may still be reading the object, so the pass keeps it until a manifest ages past the window. |
| `degraded_roots` | Root resolution failed somewhere in the pass, so manifest and segment deletion was suppressed wholesale. `retention_degraded` is set too. |
| `unrecognized_key` | A key under a swept family that this collector does not recognize as one of its own. Never deleted, whatever its age. |
| `checkpoint_not_releasable` | A checkpoint record the pass could not advance: a lost compare-and-swap, an unreadable record, a fork record its target may still reach, a released record still inside its grace, or an active pin doing its job. |
| `upload_session_window` | An upload session waiting out a window a clock resolves — the same waits `next_reclamation_at_ms` reports. |
| `upload_session_undecided` | An upload session held for a reason no clock resolves: a lost compare-and-swap, a record that vanished mid-pass, or a reference set the pass could not establish. |
| `content_scan_deferred` | A completed session whose content reclamation was skipped because the reference scan did not fit in `max_objects`. `content_reclamation_deferred` is set too. |

Retention is counted per candidate examined, not per object in the
namespace, so one object two passes both examine is counted by each.

An object that something once referenced is not collectable the moment it
stops being referenced. Reads pin a head and the manifest under it and go on
reading through that pair, so `grace_window_ms` runs from the unreferencing
as well as from the write: a segment a reorganization folds away today is
collected a grace window from now, not on the next pass. A namespace too
young to have any manifest older than the window has nothing that dates its
unreferencing yet, and a pass over one collects nothing and reports every
candidate under `no_reference_manifest`.

#### Deleting, retaining, and reclaiming

Two horizons decide when a namespace actually gets smaller, and they are
independent.

The first is the **metadata retention floor**. It limits how far back clients
can replay the WAL. Advancing the floor makes older WAL segments eligible for
garbage collection. It advances only through an explicit request:
`POST .../maintenance/run` with a body naming `retention`, or
`loonfs admin retention advance`. It does not remove file revisions.

The second is the **content reclamation grace**, which is slightly longer than
seven days and is derived rather than configured. Upload sessions stage
content before a commit references it. LoonFS keeps unreferenced staged
content until no valid upload receipt can still publish it. During this
period, garbage collection reports the object under `upload_session_window`.

Deleting a file does not reclaim its content because LoonFS retains every file
revision. Garbage collection removes staged content that no commit published,
such as data from an abandoned upload. Deleting a large tree therefore does
not make the object-store bucket smaller.

The runtime schedules another garbage-collection pass at
`next_reclamation_at_ms`, when the oldest retained object becomes eligible.
Active namespaces do not need a separate cron job for this cleanup.

LoonFS does not enumerate namespaces for maintenance. Use
`loonfs admin maintenance run --namespaces <id>` to maintain inactive
namespaces explicitly. The command runs until stopped, or performs one
bounded pass with `--drain`. An inactive namespace receives no maintenance
unless a process is assigned to it.

When a pass keeps more than it deletes, `retained` above says why. The one
answer that is an operator decision rather than a wait is
`checkpoint_not_releasable`: a pin holds its basis for as long as it exists,
so `GET /v0/admin/namespaces/{ns}/checkpoints` is where to look next.

#### Service-proxied upload

`service_proxied` is the default and needs no capability from the provider:
the client `PUT`s its bytes to `/uploads/{upload_id}/content` and the server
writes them to object storage.

The server streams the body to object storage without buffering the complete
file. While streaming, it counts the bytes and computes SHA-256. A body larger
than `upload.max_content_bytes` fails with `content_too_large`. The resulting
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
with `upload.direct_put_max_content_bytes`, the provider's single-request
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
`feature = "core.uploads.direct_put"`. The server-mediated upload path remains
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
reference from the session's content identity and the completion claim. It
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

A server may return a short-lived `content_token` for completed content.
Clients treat the token as opaque and can copy it directly into a commit
request. Reading a completed session returns a fresh token while its minting
window remains open. The separate `content_ref` remains available afterward.

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_...",
  "content_ref": { "kind": "blob_v1", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "sha256", "value": "..." } }
}
```

Path-oriented `put_file` operations then reference the completed `content_ref`.
The client includes the matching `content_token` in `content_tokens` unchanged;
the server verifies it before admission and publication checks the resulting
in-memory proof's binding and deadline. A missing or expired proof answers
`content_not_prepared` without reading the content object. A malformed or
expired token that names the put's ref also answers `content_not_prepared`;
tokens naming other refs are ignored.

```json
{
  "commit_id": "commit-a",
  "actor": { "kind": "service", "id": "document-importer" },
  "content_tokens": [
    {
      "content_ref": { "kind": "blob_v1", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "sha256", "value": "..." } },
      "token": "opaque-server-token"
    }
  ],
  "operations": [
    {
      "kind": "put_file",
      "path": "/docs/report.pdf",
      "content_ref": { "kind": "blob_v1", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "sha256", "value": "..." } },
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

```json
{
  "namespace_id": "demo"
}
```

The response contains the new namespace's initial state. A new namespace
starts at sequence 0 with a retention floor of 0:

```json
{
  "namespace_id": "demo",
  "head_seq": 0,
  "retention_floor_seq": 0
}
```

Fork creation uses `new_namespace_id` for the target namespace. Route placeholders
such as `{ns}`, `{source_ns}`, or an implementation-internal `:namespace` are
only path parameter names for the same namespace id value; v0 does not accept
or emit a namespace `name` alias.

Create and fork both install one object — the namespace head — with a single
conditional write (`format.md`, "Namespace creation and forks"), so they
answer conflicts the same way. A create or fork that loses that write to
another namespace answers `namespace_exists` (409). A create or fork against
a deleted id answers `namespace_deleted` (410): the id is retired and never
comes back. There is no partially created namespace, so there is no third
answer and nothing to repair.

A create whose first acknowledgment was lost answers `namespace_exists` on
the retry, like any other lost race. Nothing durable can tell the two apart:
a server publishes every caller's work under one writer label, so the
head's writer block cannot prove which request wrote it, and answering
success would tell two callers they each created the same namespace. The
409 is still actionable, which is the point — the namespace it names is
complete and usable, so a caller that does not care who created it reads it
and proceeds. The previous protocol answered this case with a namespace that
existed, could not be used, and needed an explicit repair.

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
  "head_seq": 418,
  "retention_floor_seq": 120
}
```

The `Namespace` object has exactly these fields:

| Field | Meaning |
| --- | --- |
| `namespace_id` | Durable namespace id. |
| `head_seq` | Current visible namespace sequence. |
| `retention_floor_seq` | Oldest sequence still promised for incremental replay. |

The admin endpoint `GET /v0/admin/namespaces/{ns}/diagnostics` returns the
namespace state plus storage details used by maintenance:

| Field | Meaning |
| --- | --- |
| `namespace_id` | Durable namespace id. |
| `head_seq` | Current visible namespace sequence. |
| `retention_floor_seq` | Oldest sequence still promised for incremental replay. |
| `current_manifest_no` | Current manifest number; omitted until the namespace has a manifest. |
| `wal_tail_segments` | Number of visible WAL segments after the current manifest. |
| `live_snapshots` | Number of snapshots that had not expired when diagnostics began. |
| `live_checkpoints` | Number of active user checkpoints, including expired records awaiting collection. |

```json
{
  "namespace_id": "demo",
  "head_seq": 418,
  "retention_floor_seq": 120,
  "current_manifest_no": 410,
  "wal_tail_segments": 3,
  "live_snapshots": 2,
  "live_checkpoints": 4
}
```

### 6.3 `DELETE /v0/namespaces/{ns}`

Deletion is a fenced, terminal head transition (`format.md`, "Tombstones and
deletion"). It linearizes at the head swap: commits acknowledged before it
stay committed; everything that observes the deleted namespace afterwards —
reads, commits, forks, status, re-creation of the id — fails with
`namespace_deleted` (410). Deleting an already-deleted namespace is also
`namespace_deleted`.

Checkpoint listing and user-checkpoint release are explicit exceptions. They
remain available because permanent user pins must stay discoverable and
releasable after deletion. Releasing a fork-owned checkpoint remains rejected.

Deletion itself reclaims nothing, but a deleted namespace's derived state —
WAL segments, metadata segments and manifests, and checkpoint records that
protect nothing live — becomes garbage once the tombstone is in place. A
maintenance step that selects `gc` alone runs against the tombstone and ages
that state out under the normal grace rules; the head survives as the
tombstone so the id stays retired. Content blobs live in a shared content
store outside the namespace prefix, and the same pass reclaims each one
still held by an upload-session record; a blob whose session record was
already swept has nothing left pointing at it and is not reclaimed.

The optional `expected_head_seq` query parameter deletes only if the head is
still at that sequence, failing with `stale_head` otherwise — the same
race-explicit pattern preconditions give file mutations. That rejection
reports both sequences, in the message and as `expected_head_seq` and
`actual_head_seq` details, so a caller that still means to delete can retry
against the sequence it found.

```json
{
  "namespace_id": "demo",
  "head_seq": 418
}
```

An unacknowledged request was never committed. The reference server resolves
its queue in admission order — requests admitted before the delete publish first, requests admitted
after it fail with `namespace_deleted`, and nothing is rejected for a delete
that ends up failing its precondition.

### 6.4 `GET /filesystem/entry` and `GET /inodes/{inode_id}`

The response is one authoritative path entry. Enum values are snake_case per
the durable naming rules (`format.md`, "Durable naming conventions").

```json
{
  "namespace_id": "demo",
  "path": "/docs/report.txt",
  "inode_id": "ino_42",
  "created_by": { "kind": "user", "id": "usr_8f3c" },
  "created_at_ms": 1752623000000,
  "inode_kind": "file",
  "head_seq": 418,
  "parent_inode_id": "ino_7",
  "display_name": "report.txt",
  "binding_generation": "opaque-token",
  "revision_no": 7,
  "revision_committed_by": { "kind": "service", "id": "render-worker" },
  "size_bytes": 19482,
  "content_ref": {
    "kind": "blob_v1",
    "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
    "size_bytes": 19482,
    "checksum": { "algorithm": "sha256", "value": "42d..." }
  },
  "revision_committed_at_ms": 1752624000000,
  "attributes_revision_no": 3,
  "attributes_updated_by": { "kind": "user", "id": "metadata-editor" },
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

The namespace root is nameless, so its entry omits `parent_inode_id`, `display_name`, and `binding_generation`. Every other entry includes a validated `display_name` and a `binding_generation` for its current parent/name binding (section 5.1). The empty string is not a valid name for the root or any named path component.

The inode route returns the same entry shape, including the current `path`.
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

The inode route addresses the directory by its stable identity instead of a
name, so a listing and its resumption stay on the same directory across
concurrent renames or moves of the parent; entry paths reflect the parent's
location at each page's head. It is gated by the `core.inodes.list_children`
feature. An unknown or hidden target inode answers `inode_not_found`, and a
file target answers `path_conflict`: an inode-addressed caller asked for
children, never for the entry itself, so there is no single-entry file
listing on this route. A directory deleted mid-listing answers
`inode_not_found` on the resumed page.

`include_attributes` works exactly as it does on the entry route and obeys the same required-siblings-together projection rule, but it defaults to `false`. This keeps the default response bounded because a page may contain up to `pagination.max_limit` entries and each attribute map may be 64 KiB. Clients that request attributes should choose a suitable page size.

A cursor is an opaque ordering resume, not a snapshot pin. Every cursor in the API
— directory listing, revision listing, grep, and the change feed alike —
tolerates forward head drift: commits landing mid-listing never retire it,
and the resumed page evaluates at the then-current head, continuing strictly
after the last returned position. Each page is internally consistent at its
own head, but a multi-page listing spans whatever heads its pages ran at: an
entry created behind the resume position is missed, an entry deleted behind
it was already returned, and a rename can surface as a duplicate or a miss.
A client that needs one consistent cut re-issues the listing when `head_seq`
changes between pages. Only a cursor minted ahead of the serving head
answers `rebootstrap_required` — drift tolerance runs forward, never
backward. (A malformed cursor, or one replayed against a different target,
stays `invalid_request`.)
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
      "created_by": { "kind": "user", "id": "usr_8f3c" },
      "created_at_ms": 1752623000000,
      "inode_kind": "file",
      "head_seq": 418,
      "parent_inode_id": "ino_7",
      "display_name": "report.txt",
      "revision_no": 7,
      "revision_committed_by": { "kind": "service", "id": "render-worker" },
      "revision_committed_at_ms": 1752624000000,
      "size_bytes": 19482,
      "content_ref": {
        "kind": "blob_v1",
        "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
        "size_bytes": 19482,
        "checksum": { "algorithm": "sha256", "value": "42d..." }
      }
    },
    {
      "namespace_id": "demo",
      "path": "/docs/slides",
      "inode_id": "ino_43",
      "created_by": { "kind": "user", "id": "usr_8f3c" },
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

Lists the namespace's recoverable deletions, oldest deletion first — ascending
by `(deletion_seq, inode_id)` — paged with the standard `limit`/`cursor`
pattern (the cursor is an ordering resume like every other). The listing is a
range scan over the derived active-deletions family (format spec, section
2.5), so a page costs the page rather than the namespace's deletion history.
Those rows represent current state and are not removed when the retention floor advances. Each entry includes the inode id and deletion sequence required by `undelete`, plus `deleted_by` and `deleted_at_ms` from that deletion. When available, `deleted_binding` contains the directory binding that was removed. Entries without this binding still contain everything needed for recovery. Nested deletions remain separate entries, and recovering an outer deletion does not remove an inner deletion from the list.

```json
{
  "namespace_id": "demo",
  "head_seq": 418,
  "entries": [
    {
      "inode_id": "ino_42",
      "deletion_seq": 417,
      "deleted_at_ms": 1752625000000,
      "deleted_by": { "kind": "user", "id": "usr_8f3c" },
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

The server buffers the whole file for one response, so a file past
`download.max_content_bytes` answers `content_too_large` here. That is not the
end of the road: the download transport in section 6.10 reads the same bytes
without the server holding them, and every deployment that could have let a
client create such a file offers it.

Revision listings return newest revisions first and use the standard
`limit`/`cursor` pattern. The path route resolves the current inode first; the
inode route addresses it directly. Both return the same path-free response.
`next_cursor` is included only when another page is available. Revision
history is not pruned, so paging to the end reaches revision 1.

The inode content route reads and verifies a revision without resolving a
current path. Deleted files remain readable while their revision rows are
retained. A directory returns `path_conflict`, an unknown inode returns
`inode_not_found`, and an unknown revision returns `revision_not_found`.

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
      "committed_by": { "kind": "service", "id": "render-worker" },
      "content_ref": {
        "kind": "blob_v1",
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

This is the binding for the commit model in section 5.1: one `commit_id`, one
required `actor`, an optional `message`, and `operations` — an ordered,
non-empty array of path operations. An empty array is `invalid_request`.

The root path `/` is readable but never a mutation target. An operation that
names it — as its own path, or as either end of a move or copy — is
`invalid_request`. The rejection belongs to the operation rather than to the
request, so it is attributed like every other failure: inside a batch it
names its position, and it is decided against a namespace that exists, so a
root mutation sent to an unknown namespace answers `namespace_not_found`
first.

Representative request:

```json
{
  "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
  "actor": { "kind": "user", "id": "usr_8f3c" },
  "operations": [
    {
      "kind": "move_path",
      "from_path": "/docs/report.txt",
      "to_path": "/reports/report.txt",
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

```json
{
  "commit_id": "c_2a41d0c6b9f34e7d8a1b5c9e0f234567",
  "actor": { "kind": "user", "id": "usr_8f3c" },
  "message": "import the January report",
  "content_tokens": [
    {
      "content_ref": { "kind": "blob_v1", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "sha256", "value": "..." } },
      "token": "opaque-server-token"
    }
  ],
  "operations": [
    { "kind": "create_directory", "path": "/reports/2026" },
    {
      "kind": "put_file",
      "path": "/reports/2026/january.pdf",
      "content_ref": { "kind": "blob_v1", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "sha256", "value": "..." } },
      "behavior": "no_replace"
    },
    {
      "kind": "delete_path",
      "path": "/inbox/january.pdf"
    }
  ]
}
```

One `content_tokens` proof covers every operation that names its
`content_ref`; tokens naming a ref no operation puts are ignored.

The first operation that fails aborts the whole request — nothing it or its
predecessors would have written becomes visible — and the error names the
position that stopped it. Had the put above raced another writer:

```json
{
  "code": "path_conflict",
  "message": "operation 1: destination `/reports/2026/january.pdf` already exists",
  "request_id": "req_9c2f4a1b7d8e4f21a0b3c4d5e6f70819",
  "details": {
    "commit_id": "c_2a41d0c6b9f34e7d8a1b5c9e0f234567",
    "operation_index": 1
  }
}
```

Move and copy accept the same `behavior` choice as put: `no_replace` (the
default) fails when the destination is occupied, and `replace` replaces a
file destination. A replacing move deletes the destination file and rebinds
the source in one commit; a replacing copy appends a revision to the
destination inode, keeping its identity and revision history. Only a file
destination can be replaced, and a path never replaces itself.

Replacing puts can include `expected_inode_id`, `expected_revision_no`, or
both. Replacing moves and copies use `destination_expected_inode_id` and
`destination_expected_revision_no`. These fields let a client require the
same file and revision that it previously read.

Guards require `replace` behavior. An inode mismatch returns `path_conflict`,
a revision mismatch returns `stale_revision`, and a missing destination
returns `path_not_found`.

Five operations use inode IDs instead of paths. They let clients act on an entry they previously read even if its path has changed. An unknown or hidden inode returns `inode_not_found`.

`create_directory_by_inode` and `put_file_by_inode` create an entry under an existing parent directory. Both are create-only and return `path_conflict` when the name is already in use.

`put_file_revision_by_inode` appends a revision to a file wherever it is currently located. It requires `expected_revision_no` and returns `stale_revision` when the file has changed.

`move_by_inode` and `delete_by_inode` require `expected_binding_generation` (section 5.1). Their destination, replacement, and recursive-delete behavior matches `move_path` and `delete_path`. The namespace root cannot be moved or deleted.

```json
{
  "commit_id": "c_1b2c3d4e5f60718293a4b5c6d7e8f901",
  "actor": { "kind": "user", "id": "usr_8f3c" },
  "operations": [
    {
      "kind": "create_directory_by_inode",
      "parent_inode_id": "ino_1",
      "display_name": "reports"
    },
    {
      "kind": "move_by_inode",
      "inode_id": "ino_42",
      "expected_binding_generation": "opaque-token",
      "to_parent_inode_id": "ino_12",
      "to_display_name": "january.pdf",
      "behavior": "replace"
    }
  ]
}
```

A successful response is returned only after the underlying change is actually
committed: the WAL segment is durable and the head has advanced. Every
commit returns the same envelope (section 5.2).

Representative response:

```json
{
  "namespace_id": "demo",
  "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
  "committed_seq": 419,
  "committed_by": { "kind": "user", "id": "usr_8f3c" },
  "committed_at_ms": 1752624000000,
  "message": "import the January report",
  "events": [
    {
      "kind": "file_created",
      "inode_id": "ino_43",
      "parent_inode_id": "ino_12",
      "display_name": "january.pdf",
      "revision_no": 1,
      "content_ref": { "kind": "blob_v1", "content_id": "con_9f2a...", "size_bytes": 1234, "checksum": { "algorithm": "sha256", "value": "..." } }
    }
  ]
}
```

The same endpoint also accepts path directory creation:

```json
{
  "commit_id": "c_8b7d4ef098ec4c1fbde15edbe02f9a64",
  "actor": { "kind": "user", "id": "usr_8f3c" },
  "operations": [{ "kind": "create_directory", "path": "/docs" }]
}
```

and path revision restore:

```json
{
  "commit_id": "c_8f9a1b2c3d4e4f50a6b7c8d9e0f12345",
  "actor": { "kind": "user", "id": "usr_8f3c" },
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

```json
{
  "commit_id": "c_5d6e7f8091a2b3c4d5e6f70812345678",
  "actor": { "kind": "user", "id": "usr_8f3c" },
  "operations": [
    {
      "kind": "undelete",
      "inode_id": "ino_42",
      "deletion_seq": 17,
      "path": "/docs/report.txt"
    }
  ]
}
```

`path` is optional. When present, it is the destination: its parent must
exist and be visible, and its name must be free. When absent, the entry
restores in place — it re-binds under the parent inode and name its
deletion recorded, anchored on the parent's identity rather than a
remembered spelling, so recovery lands correctly even when the enclosing
directories were renamed after the delete. A deletion that recorded no
binding (early tombstones carry none) answers `invalid_request` and needs
the explicit path. The in-place parent and name obey the same rules a path
would: the parent must not be deleted, and the name must be free, each
answering its usual code otherwise.

Only the root of a deletion can be undeleted, and `deletion_seq` must match the active deletion generation. A mismatch returns `not_deleted` with the expected and actual generations, preventing a stale recovery request from cancelling a later deletion.

and `update_attributes`, which writes and removes attributes on the inode a
path resolves to:

```json
{
  "commit_id": "c_6e7f8091a2b3c4d5e6f7081234567890",
  "actor": { "kind": "user", "id": "usr_8f3c" },
  "operations": [
    {
      "kind": "update_attributes",
      "path": "/docs/report.txt",
      "set": {
        "owner": "ada",
        "tags": "draft,review"
      },
      "remove": ["stage"],
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

An update that leaves the map exactly as it was also answers
`invalid_request`. Attributes are current state with no history, so a
revision that restates the same map has nothing behind it — unlike a put of
identical bytes, which appends a revision because a file's revisions *are* its
history. The resulting map is checked against every limit in the format spec,
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
guards, and both are part of the commit's semantic identity for commit-id
reuse. A wrong `expected_inode_id` answers `path_conflict`, like the delete
guard it mirrors, so a raced rebinding cannot land attributes on the wrong
inode. A stale
`expected_attributes_revision_no` answers `stale_attributes`. Omitting the
revision guard does not make the write a merge: every update carries the
revision it read as its own guard, so a concurrent update still answers
`stale_attributes` with the expected and actual revisions in the details.

### 6.9 Upload transport

The upload transport standardizes staged content publication, not one specific
byte path. In v0, uploads are whole-file uploads: the staged body is the
complete file content, not a separate metadata document or multipart strategy.

The semantic rule is:

- `PUT /content` stores the immutable whole-file object and records the staged
  `content_ref`;
- `complete` verifies the upload and records its final `content_ref`; and
- the returned completed session's `content_ref` is then safe to reference
  from a commit. Remote servers may also return an opaque `content_token` that
  remote create/replace mutations carry back as their content-preparation
  proof.

Begin requests use `mode` to select the upload transport. A request may include
only the fields for that mode:

```json
{ "mode": "service_proxied" }
{ "mode": "direct_put", "size_bytes": 1234 }
{ "mode": "direct_multipart", "part_size_bytes": 8388608 }
```

Mode-specific fields are placed beside `mode`. `service_proxied` has no additional fields. `direct_put` accepts an optional size hint. `direct_multipart` accepts an optional `part_size_bytes` and uses the default when omitted.

Completion requests use the same `mode` values as begin requests:

```json
{ "mode": "service_proxied" }
{ "mode": "direct_put", "content": { "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "<16 lowercase hex>" } } }
{ "mode": "direct_multipart", "content": { "size_bytes": 1234, "checksum": { "algorithm": "crc64nvme", "value": "<16 lowercase hex>" } }, "parts": [] }
```

The request mode must match the upload session. Direct PUT requires `content`.
Multipart requires `content` and `parts`. Service-proxied completion has no
additional fields. Unknown fields, missing fields, and mode mismatches return
`invalid_request`.

The begin-upload response uses the same `mode` tag. `service_proxied` adds no fields beyond `namespace_id` and `upload_id`. `direct_put` adds `checksum_algorithm` and `access`. `direct_multipart` adds `part_size_bytes` and `checksum_algorithm`. Response readers accept unknown fields so newer servers can add fields without breaking older clients.

An upload session allocates its content object when it begins, so repeating
`PUT /content` with the same bytes for the same upload id writes the same
object and is idempotent. Repeating it with different bytes is a conflict.
Two *different* sessions carrying identical bytes get their own objects:
content is never shared across uploads, so retry idempotency belongs to the
session and nothing else. Completing a service-proxied upload fails if no
content was staged. Publication never downloads an arbitrary external ref to
rescue a missing proof.

A session is `open`, then `completed` or `aborted`, and both of those are
final (format spec, section 3.10). What that means at the API:

- `GET /uploads/{upload_id}`, `POST /uploads/{upload_id}/complete`, and
  `POST /uploads/{upload_id}/abort` all return one flat upload-session object.
  Its `mode` is the transport chosen when the session began, and `status` is
  `open`, `completed`, or `aborted`. The status-specific fields are siblings
  of that tag rather than a nested object:

  ```json
  { "namespace_id": "demo", "upload_id": "upl_...", "mode": "direct_multipart", "status": "open", "expires_at_ms": 1730000000000 }
  { "namespace_id": "demo", "upload_id": "upl_...", "mode": "direct_put", "status": "completed", "completed_at_ms": 1730000001000, "content_ref": { "kind": "blob_v1", "content_id": "con_...", "size_bytes": 1234, "checksum": { "algorithm": "sha256", "value": "<64 hex>" } }, "content_token": { "content_ref": { "kind": "blob_v1", "content_id": "con_...", "size_bytes": 1234, "checksum": { "algorithm": "sha256", "value": "<64 hex>" } }, "token": "<opaque>" } }
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

**Choosing a transport.** A payload smaller than one part gains nothing from
any direct transport and goes to `PUT /content`. Above that, a client works
down the transports its deployment advertises:

1. `direct_multipart`, where advertised. Parts win because each is retried on
   its own and nothing has to know the payload's length in advance.
2. `direct_put`, where advertised and `size_bytes` is at most
   `upload.direct_put_max_content_bytes`. This is the rung a provider that
   can sign a write but has no multipart API to open offers. It is the one
   transport that sends one whole object directly. The client counts and
   hashes the bytes while sending them, then reports both values at completion.
   The source is read once and does not have to be held in memory.
3. `PUT /content` as a streaming request body, where `size_bytes` is at most
   `upload.max_content_bytes`. The server hashes the payload as it forwards
   it on. A body whose length is unknown is sent with chunked transfer
   encoding, and the server's incremental accounting is what bounds it.

Every rung with a known size is judged against the advertised limits, never
against an assumed one: a payload under one part is not thereby known to fit
`upload.max_content_bytes`, since a deployment may set that cap anywhere. A
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
completed, it names one `{namespace, content store, content_ref}` triple, and
it is short-lived — a commit is expected to follow the upload promptly, and a
rejected receipt is not an error the client has to plan around. Durability
lives in the session, not in the receipt: reading the session mints another
one for bytes that never move again, so **a lost commit response costs one
request, never a retransfer**. Re-minting stops a fixed window after
completion, after which the status read reports the session without a token;
by then the content is either referenced by committed metadata, which
protects it on its own, or reclaimable (format spec, "Garbage collection",
rule 11). A client that receives an expired or otherwise rejected receipt
re-reads the session and commits again with the fresh one.

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
    "kind": "blob_v1",
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
  "content_ref": {
    "kind": "blob_v1",
    "content_id": "con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41",
    "size_bytes": 20591,
    "checksum": { "algorithm": "sha256", "value": "7ab..." }
  },
  "content_token": {
    "content_ref": {
      "kind": "blob_v1",
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
read buffers the file for one response and refuses anything past
`download.max_content_bytes`. Without a read that does not buffer, a
deployment could hold a file it had no way to return. So
`core.downloads.direct_get` is advertised by every deployment that offers
any direct write — the read is not a separate decision, and a deployment
that offers none of them cannot have created such a file in the first place.

`POST /v0/namespaces/{ns}/filesystem/downloads` takes a path and, optionally,
the revision to read — the same two things the proxied read takes:

```json
{ "path": "/docs/report.txt", "revision_no": 3 }
```

The response is a short-lived read capability plus everything the reader
checks the arriving bytes against:

```json
{
  "namespace_id": "demo",
  "path": "/docs/report.txt",
  "revision_no": 3,
  "content_ref": {
    "kind": "blob_v1",
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
Its body is `{}` and its response does not include a path:

```json
{
  "namespace_id": "demo",
  "inode_id": "ino_42",
  "revision_no": 3,
  "content_ref": {
    "kind": "blob_v1",
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
`not_supported` with `feature = "core.downloads.direct_get"`, and its proxied
read stays available under its own limit; because such a deployment cannot
presign writes either, no file it holds can be larger than it will proxy.

### 6.11 `GET /changes`

Each change is one commit carrying its identity (`committed_seq`, `commit_id`,
`committed_by`, observational `committed_at_ms`, optional `message`) and `events`:
the semantic filesystem operations the commit
applied, in the order it applied them. One request operation may apply
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
      "committed_seq": 419,
      "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
      "committed_by": { "kind": "user", "id": "usr_8f3c" },
      "committed_at_ms": 1752624000000,
      "message": "replace report bytes",
      "events": [
        {
          "kind": "content_changed",
          "inode_id": "ino_42",
          "revision_no": 8,
          "content_ref": {
            "kind": "blob_v1",
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
| `directory_created` | A directory was created. | `inode_id`, `parent_inode_id`, `display_name`, `binding_generation`. |
| `file_created` | A file was created with its first revision. | `inode_id`, `parent_inode_id`, `display_name`, `binding_generation`, `revision_no`, `content_ref`. |
| `content_changed` | A file received a new current revision — a replacing put or a revision restore (one durable fact for both). | `inode_id`, `revision_no`, `content_ref`. |
| `moved` | An entry moved to a new parent directory or name. | `inode_id`, `from_parent_inode_id`, `from_display_name`, `to_parent_inode_id`, `to_display_name`, `binding_generation`. |
| `deleted` | A file or directory subtree was deleted. Use the enclosing `committed_seq` as `deletion_seq` when restoring it. | `inode_id`, plus optional `deleted_binding` containing `parent_inode_id`, `name_key`, and `display_name`. |
| `undeleted` | A deleted inode was recovered and re-bound. | `inode_id`, `parent_inode_id`, `display_name`, `binding_generation`. |
| `attributes_changed` | An inode's attributes changed. `attributes` is the complete flat string map after the update, so a consumer projects it without reading anything back; an empty map is the cleared state. | `inode_id`, `attributes_revision_no`, `attributes`. |

Directory and file creation use separate event shapes. A file creation always
includes its first revision and content reference:

```json
{
  "kind": "directory_created",
  "inode_id": "ino_42",
  "parent_inode_id": "ino_1",
  "display_name": "docs",
  "binding_generation": "opaque-token"
}

{
  "kind": "file_created",
  "inode_id": "ino_43",
  "parent_inode_id": "ino_1",
  "display_name": "report.txt",
  "binding_generation": "opaque-token",
  "revision_no": 1,
  "content_ref": {
    "kind": "blob_v1",
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

`directory_created`, `file_created`, `moved`, and `undeleted` include the `binding_generation` they created. It matches later reads of the same binding. Other events do not create bindings and omit the field.

If `limit` truncates the page before the namespace head, the response includes
`next_after_seq` set to the last returned change's `committed_seq`. The client
resumes with `after_seq={next_after_seq}`.

`after_seq` may equal the current namespace head, which returns an empty page.
A value above the head is invalid: accepting an unpublished sequence would let
a consumer silently skip commits as the namespace catches up.

### 6.12 `POST /forks`

Representative request:

```json
{
  "new_namespace_id": "demo-branch"
}
```

Representative response:

```json
{
  "namespace_id": "demo-branch",
  "head_seq": 418,
  "retention_floor_seq": 418
}
```

The server forks from the source namespace's current head. The new namespace
shares the source namespace's content store and starts with independent future
namespace metadata. The fork creates a fork-owned source checkpoint so the
source-owned immutable metadata segments stay available for as long as the
target may still read them. It renews the checkpoint with compare-and-swap,
then installs the target namespace's head in one conditional write. That head
records the source checkpoint for the target's lifetime.

The response contains the new namespace's initial state. Its head
sequence and retention floor are set to the source namespace's sequence at
the fork point.

If the target ID already exists or has been deleted, the server returns the
same `namespace_exists` or `namespace_deleted` error as namespace creation.
If the source checkpoint cannot be renewed, the server returns
`checkpoint_unavailable` and no target namespace is installed.

### 6.13 `GET /grep`

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
the namespace's grep index (format spec, "Gram index segments"), scans
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
(format spec, section 2.3.1), then limits results to descendants of that
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
calls the process made, the maintenance steps it settled, the publications
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
   WAL segments plus a successful head update;
3. validate that referenced content is already durable before publish;
4. preserve `(namespace_id, inode_id)` as canonical identity;
5. resolve namespace content through the immutable `content_store_id` in the
   namespace head;
6. implement tombstone-first delete;
7. serve replay from the current verified manifest named by
   `metadata/root.json`, plus the visible WAL segment chain, replayed as
   logical commits; checkpoints pin manifest versions for retention, stable
   reads, restore, and forks;
8. fold sibling names into name keys by the v0 rule (`format.md`, section
   2.3.1);
9. keep control-plane sessions and any implementation-specific coordinators
   out of namespace history and the change feed;
10. preserve per-commit idempotency, ordering, and change-feed identity even
    when physically batching logical commits in a WAL segment;
11. advertise its profiles and features truthfully through the capability
    document, never advertising a profile whose required ops are not
    implemented; and
12. answer unimplemented or disabled surface area with `not_supported` (and
    its `feature` name), never with undefined behavior.

### 7.2 Writer and client requirements

A conforming writer or client must:

1. treat paths as selectors, not as durable identity;
2. upload or otherwise stage content before asking the server to publish it;
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
the conformance profiles of section 1.)

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
| Content hashing and upload | May accept direct bytes, proxy uploads, or issue upload capabilities, but must verify that any content referenced by a commit is already durable. A server may issue short-lived content admission tokens after validation to avoid repeating expensive checks. | Usually responsible for reading local bytes, computing content hashes, and uploading missing content when originating new data. Clients may forward admission tokens when provided, but must tolerate slow-path validation. |
| Commit validation | Authoritative | Supplies preconditions and commit ids where needed. |
| Namespace visibility | Authoritative | Observes committed sequence receipts and change-feed deltas. |
| Long-running transfer progress | Authoritative for sessions that affect correctness | Responsible for local temp files, local progress, retry behavior, and any higher-level orchestration outside the core model. |
| Capability truthfulness | Advertises only implemented profiles and features. | Gates on the capability document; reconciles via `not_supported`. |

## 11. Extension points

The preferred extension point is the committed change feed. Downstream systems
such as indexers, notification services, preview builders, or policy engines
should consume committed changes rather than becoming part of the core
mutation path.

New client-visible operations arrive as profile ops or named features here;
new durable state arrives in `format.md`; new scheduling machinery is
implementation freedom. Cross-store discovery — naming authority, search,
ownership, quotas — is out of scope for this specification.
