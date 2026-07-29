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
| `core/v0` | Data plane | Path reads and mutations (stat, list, content, revisions, path operations), staged uploads, the change feed, namespace status by id, `GET /v0/capabilities`, and the standard error contract. Namespace `list`, `create`, `fork`, and `delete` are **features** within this profile. | **Mandatory** for any conforming deployment |
| `admin/v0` | Maintenance plane | Create and release checkpoints; run one-shot maintenance steps (WAL flush, metadata reorganization, retention-floor advancement, and garbage collection, together or one at a time). Future maintenance triggers (index builds) arrive as features in this plane. | Optional |
| `query/v0` | Query plane | Content search over derived indexes (`POST /v0/namespaces/{namespace}/query/grep`). Grep-index search is the `query.grep` **feature** within this profile; using it also requires a materialized steady-state grep root for the namespace. | Optional |
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

```json
{
  "protocol_version": "v0",
  "profiles": ["core/v0", "admin/v0", "query/v0"],
  "features": {
    "core.namespaces.create": true,
    "core.namespaces.fork": true,
    "core.namespaces.delete": true,
    "core.uploads.direct_put": false,
    "query.grep": true
  },
  "limits": {
    "maintenance.gc.min_grace_window_ms": 1230000,
    "pagination.default_limit": 1000,
    "pagination.max_limit": 1000,
    "query.grep.default_limit": 100,
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
| `pagination.default_limit` | Default page size applied when a paged request omits `limit`. |
| `pagination.max_limit` | Largest accepted page size for paged requests. |
| `upload.max_content_bytes` | Largest request body accepted for service-proxied upload content (`PUT .../uploads/{upload_id}/content`). Clients may use `direct_put` for larger content only when `core.uploads.direct_put` is advertised; otherwise they must stay within this limit. |
| `download.max_content_bytes` | Largest file content a service-proxied read (`GET .../filesystem/content`, inode revision content) will buffer and return in one response. Over-limit reads answer `content_too_large`; v0 has no proxied streaming or range reads. |
| `upload.max_concurrent` | How many service-proxied upload bodies the deployment buffers at once; requests past the cap answer `server_busy`. |
| `download.max_concurrent` | How many service-proxied content reads the deployment materializes at once; requests past the cap answer `server_busy`. |
| `maintenance.gc.min_grace_window_ms` | Smallest accepted `grace_window_ms` on a `gc` request; smaller values answer `invalid_request`. Derived from the publication budgets, not tuned. |
| `query.grep.default_limit` | Matches per grep page when the request omits `limit`. |
| `query.grep.max_limit` | Largest accepted grep page limit; invalid limits are rejected as `invalid_request`. Distinct from the pagination keys because a grep item costs a verified file read, not a row. |
| `query.grep.scan_budget_files` | Files a plan-less `allow_scan` grep will scan before refusing with `query_unindexable`. |
| `query.grep.tail_budget_files` | Unindexed-tail revisions one grep scans exhaustively before failing with `index_lagging`. |

### 2.2 Feature registry

Every feature key is defined here, alongside the ops it gates. This registry
is frozen per protocol version: new keys arrive with a spec change, not ad
hoc.

| Feature key | Gates | Notes |
| --- | --- | --- |
| `core.namespaces.create` | Creating namespaces (`POST /v0/namespaces`). | |
| `core.namespaces.fork` | Forking namespaces (`POST /v0/namespaces/{ns}/forks`). | |
| `core.namespaces.delete` | Deleting namespaces (`DELETE /v0/namespaces/{ns}`). | Terminal, and the id is permanently retired. Derived state becomes reclaimable through a `gc`-restricted maintenance step (section 6.3); content blobs are not reclaimed in v0. A deployment may still advertise `false` and answer `not_supported`. |
| `core.uploads.direct_put` | Starting presigned `direct_put` upload sessions (`POST /v0/namespaces/{ns}/uploads`). | The server returns a short-lived presigned PUT capability for the exact content object. The key is present only when the selected provider profile proves the signed preconditions or the deployment explicitly opts into an unproven endpoint. Raw object keys and caller-managed object-store writes are not part of this feature. |
| `query.grep` | Content search (`POST /v0/namespaces/{ns}/query/grep`). | The serving half of a data-dependent capability: the request also requires a materialized steady-state grep root, and a namespace without one answers `not_supported` whatever this key advertises. |

`admin/v0` currently has required ops only and no feature keys. `acl.*` keys
are unregistered until that plane materializes.

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
  "message": "writer session fenced: epoch 3 was fenced by epoch 4 (writer `server-b`)",
  "request_id": "req_9c2f4a1b7d8e4f21a0b3c4d5e6f70819",
  "details": {
    "fenced_epoch": 3,
    "active_writer_epoch": 4,
    "active_writer": "server-b"
  }
}
```

`code` is the stable machine contract; `message` is human-readable and may
change between releases; `feature` is present only on `not_supported` errors
and names the capability-document key the client should reconcile against.
Clients must branch on `code`, must tolerate codes they do not recognize, and
must not parse `message`.

This contract covers request-shape failures too: a query string, path
parameter, or JSON body the server cannot parse answers `invalid_request`
inside this envelope, never a framework plain-text rejection — and
authorization is checked first, so a malformed request without valid
credentials answers `unauthorized`.

`request_id` is the correlation id the server assigned to the request; every
response — success or error — also carries it as the `x-request-id` header,
so a caller's log line and the server's trace can be joined.

`details` is present when the failure carries machine-usable identity, so a
caller never has to parse `message` to act. Every field is optional and
clients must tolerate absent fields exactly as they tolerate unknown codes.
The codes that populate it:

| Code | Detail fields |
| --- | --- |
| `writer_fenced` | `fenced_epoch`, `active_writer_epoch`, `active_writer` (when the head recorded the winner's writer id) |
| `stale_revision` | `inode_id`, `expected_revision`, `actual_revision` (absent when the inode has no current revision) |
| `commit_id_reuse_conflict` | `commit_id` |
| `rebootstrap_required` | `after_seq`, `retention_floor_seq` |
| `not_deleted` | `inode_id`, plus `requested_deletion_seq` and `active_deletion_seq` when a live deletion exists at a different generation |
| any failed mutation | `commit_id` — the idempotency key the request committed under, echoed so failed and uncertain outcomes carry the caller's reconciliation handle (section 5.2) |

One code exists specifically so capability handling is uniform from day one:

- `not_supported` (HTTP 501): the deployment does not implement the requested
  op or feature. Any op may return it; a client maps the error to its
  `feature` key and disables or degrades that code path.

The full registry (`ErrorCode` in `loonfs-api`):

| Code | HTTP status | Meaning |
| --- | --- | --- |
| `invalid_request` | 400 | The request is malformed: a path, id, cursor, parameter, staged content reference, configuration value, or commit request limit fails validation. The message names the offending field or limit. |
| `unauthorized` | 401 | Missing or wrong credentials. |
| `permission_denied` | 403 | The backing object store rejected the deployment's storage credentials for this operation. Fix the storage credentials or bucket policy; retrying unchanged will not succeed. |
| `content_too_large` | 413 | The request body exceeds the deployment's limit: `upload.max_content_bytes` for proxied uploads. Served file content past `download.max_content_bytes` reports it too. For uploads, send a smaller payload or use `direct_put` when that feature is advertised; for reads, the deployment limit must be raised. |
| `route_not_found` | 404 | No route matches the request path. |
| `method_not_allowed` | 405 | The path exists but does not serve this HTTP method. |
| `namespace_not_found` | 404 | The namespace has no head, so it does not exist. |
| `namespace_deleted` | 410 | The namespace's head records the terminal deleted state. The id is permanently retired, so a create or fork against it fails here rather than as a conflict. |
| `path_not_found` | 404 | No visible entry at the path. |
| `revision_not_found` | 404 | The file has no such revision. |
| `upload_not_found` | 404 | No upload session with this id. |
| `namespace_exists` | 409 | The create or fork target already exists: another namespace holds the id. |
| `content_not_prepared` | 409 | A path put or explicit create/replace operation references external content without a matching admission, or carries a rejected relevant token. Prepare the content and retry with its proof. |
| `path_conflict` | 409 | The destination path is already bound. |
| `directory_not_empty` | 409 | The directory has children and the operation is not recursive. |
| `stale_head` | 409 | The write raced a head advance; retry against fresh state. |
| `stale_revision` | 409 | A caller-supplied base revision is no longer current. |
| `tombstone_conflict` | 409 | The path is covered by a subtree tombstone. |
| `not_deleted` | 409 | The undelete target is not the root of a live deletion; nothing to recover. |
| `writer_fenced` | 409 | The writer epoch was superseded by another session. |
| `would_cycle` | 409 | The rename would create a directory cycle. |
| `commit_id_reuse_conflict` | 409 | The commit id was reused with different content. |
| `upload_already_completed` | 409 | The upload session is already completed. |
| `upload_content_conflict` | 409 | Different bytes were staged under this upload id. |
| `query_unindexable` | 400 | The pattern has no run of at least 3 literal bytes, so the trigram index cannot narrow candidates; rewrite the pattern, or set `allow_scan` (capped by `query.grep.scan_budget_files`). |
| `rebootstrap_required` | 409 | The resume position is unanswerable — a change cursor below the retention floor, or a listing cursor minted ahead of the serving head; restart from a fresh listing or checkpoint. |
| `not_supported` | 501 | The deployment does not implement the requested op or feature. |
| `commit_outcome_unknown` | 503 | The publish outcome was not observed; the commit may or may not be visible. Retry with the same commit id or reconcile. |
| `commit_queue_full` | 503 | The namespace write queue is full; back off and retry. |
| `server_busy` | 503 | The server is at its configured concurrency limit for this kind of work (proxied upload bodies or proxied content reads); back off and retry. |
| `shutting_down` | 503 | The serving process closed admission for shutdown; work admitted earlier still settles. Retry against a live instance. |
| `checkpoint_unavailable` | 503 | Required checkpoint state is unavailable: not yet published, changed during the operation, condemned pending GC deletion, or referenced material is missing. Retry after maintenance. |
| `maintenance_required` | 503 | Namespace metadata requires maintenance before the request can be served; run maintenance and retry. |
| `index_lagging` | 503 | The grep index trails the head past the exhaustive-scan budget; let the grep worker catch up (or set `allow_stale`) and retry. |
| `index_corrupt` | 500 | The grep index's derived state failed validation. Disable and re-enable grep on the namespace to rebuild it; core filesystem state remains available. |
| `namespace_corrupt` | 500 | Durable namespace state failed validation. |
| `server_error` | 500 | Unclassified internal failure. |

Precondition failures surface as `409` resource-state conflicts
(`stale_revision`, `stale_head`, `commit_id_reuse_conflict`) rather than
`412`: v0 treats them as conflicts with current namespace state, not HTTP
conditional-request failures.

## 4. SDK shape

One SDK serves both backends; deployment mode never forks the client
codebase.

- The embedded handles (`loonfs::FsWriter`, `loonfs::FsReader`) and the
  remote client (`loonfs_client::Client`) expose the same operations and the
  same `capabilities()` accessor returning the capability document of
  section 2.1. For the remote client the document is fetched from
  `GET /v0/capabilities` and cached; for the embedded handles it is a constant.
- Unsupported surface area is typed: individual ops return the
  `not_supported` error with its `feature` name, so gating logic — check the
  capability document, fall back on `not_supported` — is identical against
  either backend.
- As optional planes gain ops, SDKs should group them the way the planes are
  grouped (`core`, `admin`, and later), so the surface a deployment does not
  support is visibly absent instead of failing call by call.

## 5. Minimal upload, mutation, and change-feed model

The writer surface has three stages:

1. make content durable
2. make metadata visible
3. observe ordered changes through the change feed

This split is deliberate:

- content durability is not visibility;
- WAL-segment durability is not visibility by itself; and
- head advance is the visibility point.

A mutation request may therefore be rejected immediately, or tentatively
accepted into a WAL batch, without yet being a committed or successful change.

The embedded `loonfs::FsWriter` makes the first two stages independently
drivable. `prepare_file_bytes` stages bytes, while `prepare_content_ref`
fully validates an existing reference; both return opaque prepared content.
`put_file_prepared` consumes that evidence without content-store I/O during
publication. `complete_upload_prepared` returns the ordinary completion
response together with the same evidence. These are embedded conveniences,
not HTTP operations; hosted clients continue to carry validated content
tokens on the existing wire requests.

### 5.1 Mutation identity and race guards

Every mutation carries a `commit_id` — a client-generated stable idempotency
key that must be reused verbatim for safe retries — and an optional
`message`, a human-readable annotation that is part of the mutation's
identity.

The server plans each path operation against authoritative namespace state
under the publish lock, synthesizing the exact semantic checks the operation
implies (revision identity, binding identity, name absence, directory
emptiness, ancestor visibility) so races fail explicitly rather than
silently merge. Callers add their own cross-request guards on the operation
itself where staleness matters: `expected_revision_no` on a replacing put,
`expected_inode_id` on a delete, `deleted_at_seq` on an undelete, and
`expected_head_seq` on a namespace delete.

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

### 5.2 Mutation responses and safe retry

Every committed mutation returns the same response envelope: the `namespace_id` that changed, the
`commit_id` the mutation committed under, and the `committed_seq` where it
became visible. When the caller did not supply a commit id, the surface that
accepted the request generates one and returns it, so every caller holds the
identity it needs to reconcile an uncertain outcome.

The retry rule has three cases:

- Resubmitting the semantically identical mutation with the same `commit_id`
  is safe: if the original committed, the response replays the original
  `committed_seq` without committing again; if it never committed, the
  resubmission completes it.
- Reusing a `commit_id` for a different mutation fails with
  `commit_id_reuse_conflict`.
- Retrying with a new `commit_id` is a new logical mutation.

Caller-supplied race guards (`expected_inode_id` on delete,
`expected_revision_no` on put) are part of the mutation's semantic identity,
so changing, adding, or removing a guard while reusing a `commit_id` fails
with `commit_id_reuse_conflict`.

Identical resubmission is the reconciliation mechanism. There is no separate
commit-status lookup: after `commit_outcome_unknown`, a transport failure, or
a process restart, resubmit the same request with the same `commit_id` and
read the definitive answer from the response.

The blocking Rust client automatically retries reads, commit-id mutations,
replay-safe upload stages, and idempotent maintenance calls, but makes one
attempt for namespace create, fork, and delete, upload-session begin, and
presigned direct PUT.

Committed mutations record a durable receipt binding the `commit_id` to its
`committed_seq`; replay reads that receipt. Receipts are currently retained
for the life of the namespace. When receipt retention becomes bounded, this
contract will state the replay window explicitly, and resubmission after the
window must fail loudly rather than silently committing again.

### 5.3 Writer topology and fencing

Each namespace has one active writer session. Many concurrent clients may
submit mutations through that session — the reference server is exactly this
shape: one service-level writer session coordinating every client request —
and independent readers scale separately.

Replacing the active writer is not an approval flow. Opening a writer and
publishing acquires the next writer epoch, and epoch fencing — not liveness,
not a lease — is what keeps the displaced session from corrupting anything:
its next publish fails with `writer_fenced`, terminally for that session.
The error's `details` name the epoch and writer that displaced it, so an
operator can tell a planned failover from two writers misconfigured against
one namespace.

The standard lower-level mutation set is defined in `format.md` ("Standard
mutation operations"). The path-oriented filesystem surface compiles into
that lower-level model server-side, preserving the same identity,
content-durability, and visibility rules.

## 6. Representative HTTP binding

HTTP is one transport binding for these abstract operations. It is not the
underlying semantics.

A representative v0 binding is shown below.

| Purpose | Representative HTTP shape |
| --- | --- |
| Read deployment capabilities | `GET /v0/capabilities` |
| Create a namespace | `POST /v0/namespaces` |
| Read one namespace's status | `GET /v0/namespaces/{ns}` |
| Stat a path | `GET /v0/namespaces/{ns}/filesystem/stat?path=/docs/report.txt` |
| List a path | `GET /v0/namespaces/{ns}/filesystem/list?path=/docs&limit=100&cursor=...` |
| List file revisions by path | `GET /v0/namespaces/{ns}/filesystem/revisions?path=/docs/report.txt&limit=100&cursor=...` |
| Read file content | `GET /v0/namespaces/{ns}/filesystem/content?path=/docs/report.txt` |
| Read prior file content by path | `GET /v0/namespaces/{ns}/filesystem/content?path=/docs/report.txt&revision_no=3` |
| Apply path-oriented operations | `POST /v0/namespaces/{ns}/filesystem/operations` |
| Begin or prepare upload | `POST /v0/namespaces/{ns}/uploads` |
| Upload full staged content | `PUT /v0/namespaces/{ns}/uploads/{upload_id}/content` |
| Complete staged upload | `POST /v0/namespaces/{ns}/uploads/{upload_id}/complete` |
| Read committed changes | `GET /v0/namespaces/{ns}/changes?after_seq=123&limit=100` |
| Fork a namespace | `POST /v0/namespaces/{source_ns}/forks` |
| Delete a namespace | `DELETE /v0/namespaces/{ns}?expected_head_seq=418` (feature `core.namespaces.delete`; the precondition is optional) |
| Create a checkpoint | `POST /v0/admin/namespaces/{ns}/checkpoints` (body carries the required `name` and optional `ttl_ms`; the record is user-owned and a GC root until released or expired) |
| Release a checkpoint | `POST /v0/admin/namespaces/{ns}/checkpoints/{checkpoint_id}/release` (idempotent; fork-owned records are rejected) |
| Run a maintenance step | `POST /v0/admin/namespaces/{ns}/maintenance/step` (the one maintenance entry point; see below) |
| Content search | `POST /v0/namespaces/{ns}/query/grep` (feature `query.grep`; requires a materialized steady-state grep root) |
| Enable the grep index | `POST /v0/admin/namespaces/{ns}/grep/index/enable` (CAS-publishes the independent grep root into checkpointed backfill; embedded mode starts that namespace's event-driven driver; idempotent) |
| Disable the grep index | `POST /v0/admin/namespaces/{ns}/grep/index/disable` (CAS-publishes the grep root as disabled; grep-owned garbage collection later reclaims unreferenced segments; idempotent) |
| Collect grep-index garbage | `POST /v0/admin/namespaces/{ns}/grep/index/gc` (one explicit pass over only that namespace's grep extension; also reaps aged state for an absent or tombstoned namespace) |

One maintenance step does four things in order: folds the visible WAL tail
into metadata tables and advances the metadata root once the tail reaches
`max_wal_tail_segments`, merges one bounded metadata reorganization unit,
and — each strictly opt-in — advances the retention floor (only when the
body carries `retention: true`) and runs one bounded garbage-collection
pass (only when the body carries `gc`). None of it creates a checkpoint
record.

Each part reports separately in the response: `wal_flush`, `reorganize`,
`retention_floor_seq`, and `gc`. Races and supersessions are outcomes, not
errors. Compare `retention_floor_seq` with `status_before.retention_floor_seq`
to see whether the floor moved.

The body is optional. `max_wal_tail_segments` overrides the flush threshold,
and a value above the write-rejection threshold is rejected as
`invalid_request`. `retention: true` opts into advancing the retention
floor to the flushed manifest head; nothing surrenders replay history
unless it is present. `gc` opts into sweeping and overrides
`grace_window_ms`/`reap_window_ms`, bounds one invocation with `max_objects`,
and resumes with `cursor`; a grace window below the derived safety floor or a
zero budget is rejected as `invalid_request`. Step-driven GC defaults
`max_objects` to 1024 and returns any `next_cursor` for a later step rather
than looping internally. Nothing sweeps unless `gc` is present.

`only` restricts the step to one sub-step — `wal_flush`, `reorganize`,
`retention`, or `gc` — for operators who want exactly one of them; naming
`retention` or `gc` this way is itself the opt-in. An unrestricted step
runs the two opted-in sub-steps after flush and reorganization.

The retention floor bounds incremental replay only. File revision history
is never pruned: a revisions listing is always complete, however far the
floor has advanced.

Routes under `/v0/admin/` belong to the `admin/v0` profile and routes under
`/v0/namespaces/{ns}/query/` to `query/v0`; everything else shown belongs to
`core/v0`.

GC responses carry `next_cursor` only when more candidate enumeration remains.
The token is opaque, tolerant of additive fields when decoded, and valid only
against the namespace that issued it. It encodes the last examined key and
object family, not a live set or retention proof: every resumed invocation
reloads the current roots, WAL floor, and checkpoint protections before any
deletion. If the namespace advances or keys disappear between calls, a stale
cursor may re-examine work or defer a newly inserted key that sorts before its
position until the next full pass; it can never make a newly live object
deletable.

For `direct_put`, the client requests a presigned upload capability:

```json
{
  "mode": "direct_put",
  "content_ref": {
    "kind": "whole_file_v0",
    "digest": "sha256:...",
    "size_bytes": 1234
  }
}
```

The response includes only a short-lived transfer capability, never raw object-store credentials or a caller-managed object key. Required headers are provider-issued and must be echoed by the client; for example, an S3-compatible deployment may return:

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_...",
  "mode": "direct_put",
  "direct_put": {
    "content_ref": { "kind": "whole_file_v0", "digest": "sha256:...", "size_bytes": 1234 },
    "access": {
      "kind": "presigned_url",
      "method": "PUT",
      "url": "https://...",
      "headers": {
        "if-none-match": "*",
        "x-amz-checksum-sha256": "..."
      },
      "expires_at_ms": 1780000000000
    }
  }
}
```

The signed headers are part of the transfer capability. In the S3-compatible
example, `if-none-match: *` keeps the immutable object create-only, and
`x-amz-checksum-sha256` binds the object-store write to the SHA-256 digest in
`content_ref`. Because both requirements ride the signature, the provider —
not the server — proves digest integrity at write time; a deployment only
advertises `core.uploads.direct_put` when its provider profile proves this or
the operator explicitly accepts an unproven endpoint. Arbitrary
S3-compatible gateways are unproven because HMAC interoperability does not
prove that the gateway enforces signed checksum and create-only
preconditions; gateways have been observed silently ignoring preconditions.
Without an explicit opt-in, the feature key is absent and beginning
`direct_put` answers `not_supported` with
`feature = "core.uploads.direct_put"`. The server-mediated upload path remains
available and is the default.

The reference server offers `direct_put` only where its adapter can presign the
required create-only, checksum-bound request *and* the configured endpoint is a
first-party provider domain family the live conformance suite has run against.
Custom S3-compatible endpoints, Google Cloud Storage, Azure Blob Storage, and
the local filesystem are not offered `direct_put`, and there is no
configuration override. Other implementations may use different headers or
decline `direct_put` support.

After the client uploads bytes to the presigned URL, it calls complete with the
same `content_ref`. Completion proves the durable object exists and carries the
declared size from object metadata alone; the digest needs no re-proof because
the provider enforced it during the upload, so completion never reads the
content back through the server. A server may return a short-lived
`validated_content_token` for the completed content ref; the token is opaque to
clients.

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_...",
  "content_ref": { "kind": "whole_file_v0", "digest": "sha256:...", "size_bytes": 1234 }
}
```

Path-oriented `put_file` operations then reference the completed `content_ref`.
The client includes the matching `validated_content_token` in `content_tokens`;
the server verifies it before admission and publication checks only the
resulting in-memory proof. A missing proof answers `content_not_prepared`
without reading the content object. A malformed or expired token that names
the put's ref also answers `content_not_prepared`; tokens naming other refs are
ignored.

```json
{
  "commit_id": "commit-a",
  "content_tokens": [
    {
      "content_ref": { "kind": "whole_file_v0", "digest": "sha256:...", "size_bytes": 1234 },
      "token": "opaque-server-token"
    }
  ],
  "operation": {
    "kind": "put_file",
    "path": "/docs/report.pdf",
    "content_ref": { "kind": "whole_file_v0", "digest": "sha256:...", "size_bytes": 1234 },
    "behavior": "no_replace"
  }
}
```

Long-running transfers may additionally expose session resources.
Implementations may also expose workflow helper resources, but those helpers
are outside the core semantics. Once a multi-request interaction begins, the
server-issued identifier is the stable in-flight identifier of that
interaction.

Namespace creation uses the namespace id directly. v0 has no namespace aliases
or separate display names:

```json
{
  "namespace_id": "demo"
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
a server holds one writer session across every caller it serves, so the
head's writer block cannot prove which request wrote it, and answering
success would tell two callers they each created the same namespace. The
409 is still actionable, which is the point — the namespace it names is
complete and usable, so a caller that does not care who created it reads it
and proceeds. The previous protocol answered this case with a namespace that
existed, could not be used, and needed an explicit repair.

The examples below are representative, not exhaustive. Responses may gain
fields within v0; clients must ignore JSON fields they do not recognize.

### 6.1 `GET /v0/capabilities`

The capability document of section 2.1.

### 6.2 `GET /v0/namespaces/{ns}`

The namespace status read answers "does this namespace exist, and where is
its head?" without listing every namespace. Existence is exactly the head
object: a namespace with no head is `404` with code `namespace_not_found`,
and a namespace whose head records the terminal deleted state is `410` with
code `namespace_deleted`.

```json
{
  "namespace_id": "demo",
  "head_seq": 418,
  "current_manifest_id": 410,
  "wal_tail_segments": 3,
  "retention_floor_seq": 120
}
```

### 6.3 `DELETE /v0/namespaces/{ns}`

Deletion is a fenced, terminal head transition (`format.md`, "Tombstones and
deletion"). It linearizes at the head swap: commits acknowledged before it
stay committed; everything that observes the deleted namespace afterwards —
reads, commits, forks, status, re-creation of the id — fails with
`namespace_deleted` (410). Deleting an already-deleted namespace is also
`namespace_deleted`.

Deletion itself reclaims nothing, but a deleted namespace's derived state —
WAL segments, metadata tables and manifests, and checkpoint records that
protect nothing live — becomes garbage once the tombstone is in place. A
maintenance step restricted to `gc` runs against the tombstone and ages
that state out under the normal grace rules; the head survives as the
tombstone so the id stays retired. Content blobs live in a shared
content store outside the namespace prefix and are not reclaimed in v0.

The optional `expected_head_seq` query parameter deletes only if the head is
still at that sequence, failing with `stale_head` otherwise — the same
race-explicit pattern preconditions give file mutations.

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

### 6.4 `GET /filesystem/stat`

The response is one authoritative path entry. Enum values are snake_case per
the durable naming rules (`format.md`, "Durable naming conventions").

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
  },
  "committed_at_ms": 1752624000000
}
```

File entries carry `committed_at_ms`: the wall-clock stamp of the commit that
created the current revision, in Unix milliseconds. It is observational —
sequences are the order, and no validity rule reads it. Directory entries
carry no modification time in v0.

The namespace root is nameless: its entry has `parent_inode_id: null` and
omits `display_name` entirely. Every non-root entry carries a validated
`display_name`; the empty string is not a spelling for the root or for any
named path component.

### 6.5 `GET /filesystem/list`

The envelope names the listed path and the head the listing was read from, so
an empty directory still reports which state it observed and the response can
grow without reshaping `entries`. Entries are full path entries with the same
shape as `stat` (directory entries leave the file-only fields out).

Directory listing advances in canonical `name_key` order. Concatenating pages
in cursor order yields the complete listing in that same order; clients must
not re-sort aggregated pages. The `path` query parameter is
required on every page; the cursor carries the resume position, but the
request path remains the authority for what is being listed. Responses
include `next_cursor` only when another page is available.

A cursor is an ordering resume, not a snapshot pin. Every cursor in the API
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
  ],
  "next_cursor": "7b2e2e2e7d"
}
```

### 6.6 `GET /filesystem/trash`

Lists the namespace's recoverable deletions: every active subtree tombstone,
ascending by deleted root inode, paged with the standard `limit`/`cursor`
pattern (the cursor is an ordering resume like every other). Tombstone rows
are retained forever, so entries never age out of this listing, however far
the retention floor advances. Each entry carries the inode id and deletion
sequence that `undelete` requires, the deletion's wall-clock stamp, and the
deleted binding's name when the delete recorded one; entries written before
names were recorded still carry a complete recovery handle.

### 6.7 `GET /filesystem/content`

The response body is the authoritative file bytes. Metadata may be exposed in
headers, but the body itself is raw content rather than JSON.

Revision listing returns newest revisions first and uses the same
`limit` / `cursor` pattern as directory listing, resolving the current path
to its current inode. Responses include
`next_cursor` only when another page is available. Revision history is never
pruned — paging to the end always reaches revision 1, regardless of how far
the retention floor has advanced.

```json
{
  "namespace_id": "demo",
  "inode_id": 42,
  "head_seq": 418,
  "revisions": [
    {
      "inode_id": 42,
      "revision_no": 7,
      "committed_seq": 418,
      "committed_at_ms": 1752624000000,
      "content_ref": {
        "kind": "whole_file_v0",
        "digest": "sha256:42d...",
        "size_bytes": 19482
      }
    }
  ],
  "next_cursor": "7b2e2e2e7d"
}
```

### 6.8 `POST /filesystem/operations`

Representative request:

```json
{
  "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
  "operation": {
    "kind": "move_path",
    "from_path": "/docs/report.txt",
    "to_path": "/reports/report.txt",
    "behavior": "replace"
  }
}
```

Move and copy accept the same `behavior` choice as put: `no_replace` (the
default) fails when the destination is occupied, and `replace` replaces a
file destination. A replacing move deletes the destination file and rebinds
the source in one commit; a replacing copy appends a revision to the
destination inode, keeping its identity and revision history. Only a file
destination can be replaced, and a path never replaces itself.

A replacing `put_file` may also carry `expected_revision_no`: the put then
applies only while the file's current revision is still that one, so a raced
write fails with the revision conflict's expected/actual details instead of
silently stacking a revision on state the caller never saw. The guard
asserts an existing file — an absent path answers `path_not_found`, and
combining it with `no_replace` is `invalid_request`. Like the delete guard,
it is part of the mutation's semantic identity for commit-id reuse.

A successful response is returned only after the underlying change is actually
committed: the WAL segment is durable and the head has advanced. Every
mutation returns the same envelope (section 5.2).

Representative response:

```json
{
  "namespace_id": "demo",
  "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
  "committed_seq": 419
}
```

The same endpoint also accepts path directory creation:

```json
{
  "commit_id": "c_8b7d4ef098ec4c1fbde15edbe02f9a64",
  "operation": {
    "kind": "create_directory",
    "path": "/docs"
  }
}
```

and path revision restore:

```json
{
  "commit_id": "c_8f9a1b2c3d4e4f50a6b7c8d9e0f12345",
  "operation": {
    "kind": "restore_revision",
    "path": "/docs/report.txt",
    "source_revision_no": 3
  }
}
```

and undelete, which recovers a deleted file or subtree by re-binding the
deletion's root inode at a destination path — the inode's identity and
retained revision history come back with it. The request names both halves
of the recovery handle the delete reported (and the change feed carries):
the inode id and the deletion's committed sequence.

```json
{
  "commit_id": "c_5d6e7f8091a2b3c4d5e6f70812345678",
  "operation": {
    "kind": "undelete",
    "inode_id": 42,
    "deleted_at_seq": 17,
    "path": "/docs/report.txt"
  }
}
```

Only the root of a deletion can be undeleted, and only the exact deletion
generation named by `deleted_at_seq` — anything else answers `not_deleted`
with the requested and active generations in the details, so a stale
recovery request can never cancel a later deletion. The destination parent
must exist and be visible, and the destination name must be free.

### 6.9 Upload transport

The upload transport standardizes staged content publication, not one specific
byte path. In v0, uploads are whole-file uploads: the staged body is the
complete file content, not a separate metadata document or multipart strategy.

The semantic rule is:

- `PUT /content` stores the immutable whole-file object and records the staged
  `content_ref`;
- `complete` finalizes the upload session only when the expected `content_ref`
  exactly matches the service-computed staged ref; and
- the returned `content_ref` is then safe to reference from a commit. Remote
  servers may also return an opaque `validated_content_token` that remote
  create/replace mutations carry back as their content-preparation proof.

Repeating `PUT /content` with the same bytes for the same upload id is
idempotent. Repeating it with different bytes is a conflict. Completing an
upload fails if no content was staged or if the expected `content_ref` differs
from the staged one. An aged abandoned session is conditionally condemned
before GC deletes it. If condemnation wins a completion race, completion
reports `upload_not_found`, the same stable surface as the subsequent physical
absence; if completion wins, GC's conditional write fails and retains the
completed session. Publication never downloads an arbitrary external ref to
rescue a missing proof.

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
  },
  "validated_content_token": "opaque-server-token"
}
```

### 6.10 `GET /changes`

Each change is one committed mutation carrying its identity (`seq`,
`commit_id`, observational `committed_at_ms`, writer provenance, optional
`message`) and `events`: semantic filesystem events, exactly one per
committed operation, in request-operation order.

```json
{
  "namespace_id": "demo",
  "after_seq": 418,
  "through_seq": 419,
  "changes": [
    {
      "seq": 419,
      "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
      "committed_at_ms": 1752624000000,
      "message": "replace report bytes",
      "events": [
        {
          "kind": "content_changed",
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

Event kinds:

| Kind | Meaning | Fields |
| --- | --- | --- |
| `created` | A file or directory was created. | `inode_id`, `inode_kind`, `parent_inode_id`, `name`; file creations also carry `revision_no` and `content_ref`. |
| `content_changed` | A file received a new current revision — a replacing put or a revision restore (one durable fact for both). | `inode_id`, `revision_no`, `content_ref`. |
| `moved` | An entry moved to a new parent directory or name. | `inode_id`, `from_parent_inode_id`, `from_name`, `to_parent_inode_id`, `to_name`. |
| `deleted` | A file or directory subtree was deleted. The enclosing change's `seq` is the `deleted_at_seq` an undelete passes. | `inode_id`, plus `parent_inode_id` and `name` when the delete recorded them. |
| `undeleted` | A deleted inode was recovered and re-bound. | `inode_id`, `parent_inode_id`, `name`. |

Events name inodes and their parent-directory bindings rather than full
paths; a consumer that needs paths can stat the inode or maintain its own
binding projection from this feed. Clients must ignore unknown event kinds
and unknown fields.

If `limit` truncates the page before the namespace head, the response includes
`next_after_seq` set to the last returned change seq. The client resumes with
`after_seq={next_after_seq}`.

### 6.11 `POST /forks`

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

The server forks from the source namespace's current head. The new namespace
shares the source namespace's content store and starts with independent future
namespace metadata. The fork creates a fork-owned source checkpoint so the
source-owned immutable metadata files stay available for as long as the
target may still read them, then installs the target namespace's head in one
conditional write. That head carries the fork provenance for the target's
whole life. A fork answers `namespace_exists` and `namespace_deleted` on the
target id exactly as create does.

### 6.12 `POST /query/grep`

Representative request:

```json
{
  "pattern": "fn (grep|search)",
  "case_insensitive": false,
  "path_prefix": "/src",
  "limit": 100
}
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
      "absolute_path": "/src/search.rs",
      "inode_id": 42,
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
(`snapshot_unavailable`) — drift tolerance runs forward, never backward.

A pattern with no required
literal bytes is rejected with `query_unindexable` unless `allow_scan`
opts into a capped exhaustive scan. A tail past the scan budget is
rejected with `index_lagging` unless `allow_stale` accepts indexed-only
results (reported via `tail_scanned: false`); stale results are a
consistent cut at the index watermark — files whose newest revision
postdates it are omitted entirely rather than mixed in. The `path_prefix`
value is a complete absolute path, not a partial textual segment prefix. Its
scope resolves to an inode under the namespace's name policy and filters by
ancestry, so it requires the same canonical spelling and validation as every
other path read. A missing data half answers `not_supported` with the
`feature` field naming `grep.index`.

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
8. honor the namespace's `NamePolicy`;
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
- may upload content and publish mutations of its own;
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

### 9.1 Definitions

| Term | Meaning |
| --- | --- |
| **Single-request operation** | An operation that is fully described by a single request and does not require a server-side control object after the request completes. |
| **Control object** | A server-side control-plane object used when the client continues driving an operation across multiple requests. For large or resumable `put <file>`, an implementation may use a single `UploadHandle`. |
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

   For large or resumable `put <file>`, an implementation that uses a
   server-side control object typically uses a single `UploadHandle`.

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
| `get -r <dir>` | Multi-request snapshot read | optional read session pinning a consistent snapshot | Only if a consistent snapshot is promised across requests | snapshot selection, per-request reads as for `get` | traversal progress, local outputs, client retries |
| `put <file>` (small, one-shot convenience) | Single request | none | No | destination resolution, validation, metadata commit | request payload, client retries |
| `put <file>` (large or resumable) | Begin, upload, commit | `UploadHandle`, if used | Only if server-side resumability or stable binding is promised | stable destination binding, expected slot or revision, upload handle validity, final publish | file reading, hashing, content upload progress, retry tokens |
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
| `put <file>` (resumable) | if an `UploadHandle` is used, create or validate it; bind the destination; validate durable content and commit the final publish | read the local file; hash it; upload content; track upload progress; submit the final commit request |
| `cp <file>` (same server) | resolve source and destination; authorize both sides; create the copied resource; publish the metadata change | submit the request; retry if appropriate |

### 9.5 When raw paths cease to identify the operation

LoonFS accepts path-oriented input because user intent is naturally expressed
by path. However, long-running operations require a more stable identity than
a raw path string.

| Operation | Raw path is used for | Stable in-flight identity after start |
| --- | --- | --- |
| `get <file>` | the request itself | none required |
| `put <file>` (resumable) | `begin_put` only | server-issued `UploadHandle`, if used |

### 9.6 Control-plane durability guidance

1. A control object **MUST** be durably recorded if losing it on restart
   would change correctness, visibility, retention safety, or promised
   resumability.

2. At minimum, an `UploadHandle` is normally durable when an implementation
   uses one for stable destination binding across time or restart-safe
   resumability.

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
  `UploadHandle`.

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
