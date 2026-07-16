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
| `core/v0` | Data plane | Path reads and mutations (stat, list, content, revisions, path operations), staged uploads, explicit commits, the change feed, namespace status by id, `GET /v0/config`, and the standard error contract. Namespace `list`, `create`, `fork`, and `delete` are **features** within this profile. | **Mandatory** for any conforming deployment |
| `admin/v0` | Maintenance plane | Trigger WAL flushes; create and release checkpoints; trigger retention-floor advancement; run one-shot maintenance ticks; run garbage collection. Future maintenance triggers (compaction, index builds) arrive as features in this plane. | Optional |
| `query/v0` | Query plane | Content search over derived indexes (`POST /v0/namespaces/{namespace}/query/grep`). Gram-index search is the `query.grep` **feature** within this profile; using it also requires the namespace's `index.grams` feature entry (format spec, "Namespace features map"). | Optional |
| `acl/v0` | Authorization plane | — | **Reserved name only.** Do not specify ops yet. Clients must tolerate unknown error codes, so authorization errors can land with this plane without breaking anyone. |

Notes:

- An embedded engine is `core/v0` (with the namespace features enabled) plus
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
client fetches it from `GET /v0/config` and caches it for the connection; an
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
| `upload.max_content_bytes` | Largest request body accepted for service-proxied upload content (`PUT .../uploads/{upload_id}/content`). Larger content should use `direct_put` uploads. |
| `download.max_content_bytes` | Largest file content a service-proxied read (`GET .../filesystem/content`, inode revision content) will buffer and return in one response. Over-limit reads answer `content_too_large`; v0 has no proxied streaming or range reads. |
| `upload.max_concurrent` | How many service-proxied upload bodies the deployment buffers at once; requests past the cap answer `server_busy`. |
| `download.max_concurrent` | How many service-proxied content reads the deployment materializes at once; requests past the cap answer `server_busy`. |
| `commit.max_body_bytes` | Largest JSON body accepted by `POST .../commits`. Commit bodies are metadata only — file bytes ride uploads — so over-limit commits should be split into smaller batches, not routed through `direct_put`. |
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
| `core.namespaces.delete` | Deleting namespaces (`DELETE /v0/namespaces/{ns}`). | Terminal, and the id is permanently retired. Deletion does not reclaim storage in v0. A deployment may still advertise `false` and answer `not_supported`. |
| `core.uploads.direct_put` | Starting presigned `direct_put` upload sessions (`POST /v0/namespaces/{ns}/uploads`). | The server returns a short-lived presigned PUT capability for the exact content object. Raw object keys and caller-managed object-store writes are not part of this feature. |
| `query.grep` | Content search (`POST /v0/namespaces/{ns}/query/grep`). | The serving half of a data-dependent capability: the request also requires the namespace's `index.grams` feature entry, and a namespace without it answers `not_supported` whatever this key advertises. |

`admin/v0` currently has required ops only and no feature keys. `acl.*` keys
are unregistered until that plane materializes.

Namespace listing is intentionally not supported in v0. Callers must address
namespaces by id until LoonFS has a scalable namespace catalog/index design.

### 2.3 Namespace-level features

The capability document describes a *deployment*. What is materialized on
*data* — for example, which derived indexes exist for a namespace — lives in
the namespace manifest's `features` map (`format.md`, "Namespace features
map"), whose keys are deliberately **not** profile-prefixed because they
describe data, not endpoints.

A successful data-dependent operation requires both halves: the deployment
advertises the serving capability here, and the namespace's metadata shows
the capability materialized on the data.

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
| any failed mutation | `commit_id` — the idempotency key the request committed under, echoed so failed and uncertain outcomes carry the caller's reconciliation handle (section 5.2) |

One code exists specifically so capability handling is uniform from day one:

- `not_supported` (HTTP 501): the deployment does not implement the requested
  op or feature. Any op may return it; a client maps the error to its
  `feature` key and disables or degrades that code path.

The full registry (`ErrorCode` in `loonfs-api`):

| Code | HTTP status | Meaning |
| --- | --- | --- |
| `invalid_request` | 400 | The request is malformed: a path, id, cursor, parameter, staged content reference, or configuration value fails validation. The message names the offending field. |
| `unauthorized` | 401 | Missing or wrong credentials. |
| `content_too_large` | 413 | The request body exceeds the deployment's limit: `upload.max_content_bytes` for proxied uploads, `commit.max_body_bytes` for commit bodies. Served file content past `download.max_content_bytes` reports it too. For uploads, send a smaller payload or use `direct_put`; for commits, split the batch; for reads, the deployment limit must be raised. |
| `route_not_found` | 404 | No route matches the request path. |
| `method_not_allowed` | 405 | The path exists but does not serve this HTTP method. |
| `namespace_not_found` | 404 | The namespace does not exist. |
| `namespace_deleted` | 410 | The namespace existed and was deleted. The id is permanently retired. |
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
| `not_deleted` | 409 | The undelete target is not the root of a live deletion; nothing to recover. |
| `writer_fenced` | 409 | The writer epoch was superseded by another session. |
| `would_cycle` | 409 | The rename would create a directory cycle. |
| `commit_id_reuse_conflict` | 409 | The commit id was reused with different content. |
| `upload_already_completed` | 409 | The upload session is already completed. |
| `upload_content_conflict` | 409 | Different bytes were staged under this upload id. |
| `query_unindexable` | 400 | The pattern has no run of at least 3 literal bytes, so the trigram index cannot narrow candidates; rewrite the pattern, or set `allow_scan` (capped by `query.grep.scan_budget_files`). |
| `rebootstrap_required` | 409 | The resume position — a change cursor or listing snapshot — is no longer available; restart from a fresh listing or checkpoint. |
| `not_supported` | 501 | The deployment does not implement the requested op or feature. |
| `commit_outcome_unknown` | 503 | The publish outcome was not observed; the commit may or may not be visible. Retry with the same commit id or reconcile. |
| `commit_queue_full` | 503 | The namespace write queue is full; back off and retry. |
| `server_busy` | 503 | The server is at its configured concurrency limit for this kind of work (proxied upload bodies or proxied content reads); back off and retry. |
| `shutting_down` | 503 | The serving process closed admission for shutdown; work admitted earlier still settles. Retry against a live instance. |
| `checkpoint_unavailable` | 503 | Required checkpoint state is unavailable: not yet published, changed during the operation, or referenced material is missing. Retry after maintenance. |
| `maintenance_required` | 503 | Namespace metadata requires maintenance before the request can be served; run maintenance and retry. |
| `index_lagging` | 503 | The gram index trails the head past the exhaustive-scan budget; run maintenance (or set `allow_stale`) and retry. |
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
  `GET /v0/config` and cached; for the embedded handles it is a constant.
- Unsupported surface area is typed: individual ops return the
  `not_supported` error with its `feature` name, so gating logic — check the
  capability document, fall back on `not_supported` — is identical against
  either backend.
- As optional planes gain ops, SDKs should group them the way the planes are
  grouped (`core`, `admin`, and later), so the surface a deployment does not
  support is visibly absent instead of failing call by call.

## 5. Minimal upload, commit, and change-feed model

The lower-level writer surface has three stages:

1. make content durable
2. make metadata visible
3. observe ordered changes through the change feed

This split is deliberate:

- content durability is not visibility;
- WAL-segment durability is not visibility by itself; and
- head advance is the visibility point.

A commit request may therefore be rejected immediately, or tentatively
accepted into a WAL batch, without yet being a committed or successful change.

### 5.1 Commit request envelope

A commit request carries the following logical fields:

| Field | Meaning |
| --- | --- |
| `commit_id` | Client-generated stable idempotency key for this logical commit request. The same value must be reused for safe retries. |
| `preconditions` | Explicit semantic checks such as `inode_revision_is`, `binding_is`, `child_name_absent`, `directory_empty`, or ancestor-visibility checks that make races fail explicitly rather than silently merge. |
| `ops` | Ordered list of mutation operations. Operation order is preserved through validation, logical commit creation, and change-feed output. |
| `message` | Optional human-readable description of the mutation event. |

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

Every committed mutation — an explicit commit or a path-oriented operation —
returns the same response envelope: the `namespace_id` that changed, the
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

Identical resubmission is the reconciliation mechanism. There is no separate
commit-status lookup: after `commit_outcome_unknown`, a transport failure, or
a process restart, resubmit the same request with the same `commit_id` and
read the definitive answer from the response.

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
mutation operations"). The path-oriented filesystem surface may compile
higher-level operations into that lower-level model, but both surfaces
preserve the same identity, content-durability, and visibility rules.

## 6. Representative HTTP binding

HTTP is one transport binding for these abstract operations. It is not the
underlying semantics.

A representative v0 binding is shown below.

| Purpose | Representative HTTP shape |
| --- | --- |
| Read deployment capabilities | `GET /v0/config` |
| Create a namespace | `POST /v0/namespaces` |
| Read one namespace's status | `GET /v0/namespaces/{ns}` |
| Stat a path | `GET /v0/namespaces/{ns}/filesystem/stat?path=/docs/report.txt` |
| List a path | `GET /v0/namespaces/{ns}/filesystem/list?path=/docs&limit=100&cursor=...` |
| List file revisions by path | `GET /v0/namespaces/{ns}/filesystem/revisions?path=/docs/report.txt&limit=100&cursor=...` |
| Read file content | `GET /v0/namespaces/{ns}/filesystem/content?path=/docs/report.txt` |
| Read prior file content by path | `GET /v0/namespaces/{ns}/filesystem/content?path=/docs/report.txt&revision_no=3` |
| List file revisions by inode | `GET /v0/namespaces/{ns}/inodes/{inode_id}/revisions?limit=100&cursor=...` |
| Read prior file content by inode | `GET /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{revision_no}/content` |
| Apply path-oriented operations | `POST /v0/namespaces/{ns}/filesystem/operations` |
| Begin or prepare upload | `POST /v0/namespaces/{ns}/uploads` |
| Upload full staged content | `PUT /v0/namespaces/{ns}/uploads/{upload_id}/content` |
| Complete staged upload | `POST /v0/namespaces/{ns}/uploads/{upload_id}/complete` |
| Submit an explicit commit request | `POST /v0/namespaces/{ns}/commits` |
| Read committed changes | `GET /v0/namespaces/{ns}/changes?after_seq=123&limit=100` |
| Fork a namespace | `POST /v0/namespaces/{source_ns}/forks` |
| Delete a namespace | `DELETE /v0/namespaces/{ns}?expected_head_seq=418` (feature `core.namespaces.delete`; the precondition is optional) |
| Create a checkpoint | `POST /v0/admin/namespaces/{ns}/checkpoints` (body carries the required `name` and optional `ttl_ms`; the record is user-owned and a GC root until released or expired) |
| Release a checkpoint | `POST /v0/admin/namespaces/{ns}/checkpoints/{checkpoint_id}/release` (idempotent; fork-owned records are rejected) |
| Flush the WAL tail | `POST /v0/admin/namespaces/{ns}/wal/flush` (folds the visible WAL tail into metadata tables and advances the metadata root; creates no checkpoint record) |
| Advance the retention floor | `POST /v0/admin/namespaces/{ns}/retention/advance` |
| Run a maintenance tick | `POST /v0/admin/namespaces/{ns}/maintenance/tick` (optional body overrides `max_wal_tail_segments` and opts into `gc`; flush races surface as outcomes, not errors) |
| Collect garbage | `POST /v0/admin/namespaces/{ns}/gc` (optional body overrides `grace_window_ms`/`reap_window_ms`; a grace window below the derived safety floor is rejected as `invalid_request`; nothing sweeps without an explicit call) |
| Content search | `POST /v0/namespaces/{ns}/query/grep` (feature `query.grep`; requires the namespace's `index.grams` feature entry) |
| Enable the gram index | `POST /v0/admin/namespaces/{ns}/index/grams/enable` (publishes the `index.grams` feature entry; backfill and upkeep run through maintenance ticks; idempotent) |
| Disable the gram index | `POST /v0/admin/namespaces/{ns}/index/grams/disable` (removes the feature entry and segment references; garbage collection reclaims the segments; idempotent) |

Routes under `/v0/admin/` belong to the `admin/v0` profile and routes under
`/v0/namespaces/{ns}/query/` to `query/v0`; everything else shown belongs to
`core/v0`.

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
`content_ref`. Other providers may use different headers or decline
`direct_put` support.

After the client uploads bytes to the presigned URL, it calls complete with the
same `content_ref`. Completion validates that the durable object exists and
matches. A server may return a short-lived `validated_content_token` for the
completed content ref; the token is opaque to clients.

```json
{
  "namespace_id": "demo",
  "upload_id": "upl_...",
  "content_ref": { "kind": "whole_file_v0", "digest": "sha256:...", "size_bytes": 1234 }
}
```

Path-oriented `put_file` operations then reference the completed `content_ref`.
Publishing validates the durable content reference before metadata becomes
visible. If the client has a matching `validated_content_token`, it may include
that token in `content_tokens` so the server can skip repeated durable-content
validation on the hot publish path. Missing, malformed, expired, or non-matching
tokens are ignored and the server falls back to normal durable-content
validation.

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

The examples below are representative, not exhaustive. Responses may gain
fields within v0; clients must ignore JSON fields they do not recognize.

### 6.1 `GET /v0/config`

The capability document of section 2.1.

### 6.2 `GET /v0/namespaces/{ns}`

The namespace status read answers "does this namespace exist, and where is
its head?" without listing every namespace. A missing namespace is `404` with
code `namespace_not_found`.

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
`namespace_deleted`. Storage is not reclaimed in v0.

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
  }
}
```

### 6.5 `GET /filesystem/list`

The envelope names the listed path and the head the listing was read from, so
an empty directory still reports which state it observed and the response can
grow without reshaping `entries`. Entries are full path entries with the same
shape as `stat` (directory entries leave the file-only fields out).

Directory listing advances in canonical `name_key` order. Concatenating pages
in cursor order yields the complete listing in that same order; clients must
not re-sort aggregated pages. The `path` query parameter is
required on every page; the cursor pins the snapshot and resume position, but
the request path remains the authority for what is being listed. Responses
include `next_cursor` only when another page is available.

A cursor pins the head sequence it was issued at, and v0 serves pages only
against the current head: once any commit advances the namespace, resuming an
older cursor answers `rebootstrap_required` — restart the listing from a
fresh first page. Directory listing and revision listing behave identically
here. (A malformed cursor, or one replayed against a different target, stays
`invalid_request`.)

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

### 6.6 `GET /filesystem/content`

The response body is the authoritative file bytes. Metadata may be exposed in
headers, but the body itself is raw content rather than JSON.

Revision listing endpoints return newest revisions first and use the same
`limit` / `cursor` pattern as directory listing. Path-based
revision listing resolves the current path to its current inode, while inode
revision listing is stable across later renames. Responses include
`next_cursor` only when another page is available.

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

### 6.7 `POST /filesystem/operations`

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

A successful response is returned only after the underlying change is actually
committed: the WAL segment is durable and the head has advanced. Path
operations return the same envelope as explicit commits (section 5.2).

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
deletion's root inode (the id the delete reported, also visible in the
change feed) at a destination path — the inode's identity and retained
revision history come back with it:

```json
{
  "commit_id": "c_5d6e7f8091a2b3c4d5e6f70812345678",
  "operation": {
    "kind": "undelete",
    "inode_id": 42,
    "path": "/docs/report.txt"
  }
}
```

Only the root of a deletion can be undeleted (`not_deleted` otherwise), the
destination parent must exist and be visible, and the destination name must
be free.

Inode-based restore is available when a caller already has stable inode
identity and the expected current base revision:

`POST /v0/namespaces/{ns}/inodes/{inode_id}/revisions/{source_revision_no}/restore`

```json
{
  "commit_id": "c_271e8c2b45a04e5da6a7e8d9f0012345",
  "base_revision_no": 7
}
```

### 6.8 Upload transport

The upload transport standardizes staged content publication, not one specific
byte path. In v0, uploads are whole-file uploads: the staged body is the
complete file content, not a separate metadata document or multipart strategy.

The semantic rule is:

- `PUT /content` stores the immutable whole-file object and records the staged
  `content_ref`;
- `complete` finalizes the upload session only when the expected `content_ref`
  exactly matches the service-computed staged ref; and
- the returned `content_ref` is then safe to reference from a commit. Remote
  servers may also return an opaque `validated_content_token` for hot-path
  admission; the token is an optimization hint, not a correctness requirement.

Repeating `PUT /content` with the same bytes for the same upload id is
idempotent. Repeating it with different bytes is a conflict. Completing an
upload fails if no content was staged or if the expected `content_ref` differs
from the staged one. Commits that reference arbitrary `content_ref`s still
pass the write protocol's durable-content validation.

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

### 6.9 `POST /commits`

Representative request:

```json
{
  "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
  "message": "replace report bytes",
  "preconditions": [
    {
      "kind": "inode_revision_is",
      "inode_id": 42,
      "revision_no": 7
    },
    {
      "kind": "ancestors_not_subtree_deleted",
      "inode_id": 42
    }
  ],
  "ops": [
    {
      "kind": "replace_file",
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

A request may be rejected immediately. A successful response is returned only
after the request is committed (section 5.1).

Representative response:

```json
{
  "namespace_id": "demo",
  "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
  "committed_seq": 419
}
```

### 6.10 `GET /changes`

```json
{
  "namespace_id": "demo",
  "after_seq": 418,
  "through_seq": 419,
  "changes": [
    {
      "seq": 419,
      "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
      "message": "replace report bytes",
      "deltas": [
        {
          "semantic_op_index": 0,
          "delta_index": 0,
          "kind": "append_file_revision",
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
namespace metadata. The fork records provenance and a fork-owned source
checkpoint so source-owned immutable metadata files remain available while
the target manifest references them.

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
the namespace's gram index (format spec, "Gram index segments"), scans
revisions committed after `built_through_seq` exhaustively, and verifies
every candidate against the real pattern, so index staleness affects cost,
never answers. Matches order by `(inode_id, byte_offset)` and one match is
reported per line. Two budgets bound a page: the match limit, and a
per-page verified-candidate budget, so a page may return fewer matches
than its limit and still carry a cursor. Each page is evaluated against
the namespace head at page time and reports it in `head_seq`; the cursor
resumes strictly after the last candidate the issuing page finished
scanning and is bound to that request — replaying it with different
criteria is rejected as `invalid_request`. A pattern with no required
literal bytes is rejected with `query_unindexable` unless `allow_scan`
opts into a capped exhaustive scan. A tail past the scan budget is
rejected with `index_lagging` unless `allow_stale` accepts indexed-only
results (reported via `tail_scanned: false`); stale results are a
consistent cut at the index watermark — files whose newest revision
postdates it are omitted entirely rather than mixed in. The `path_prefix`
scope resolves to an inode under the namespace's name policy and filters
by ancestry, so it follows the same normalization as every other path
read. A missing data half answers `not_supported` with the `feature`
field naming `index.grams`.

## 7. Conformance requirements

### 7.1 Server requirements

A conforming server must:

1. treat object storage as the authoritative durable foundation;
2. publish visible metadata only through logical commits stored in visible
   WAL segments plus a successful head update;
3. validate that referenced content is already durable before publish;
4. preserve `(namespace_id, inode_id)` as canonical identity;
5. resolve namespace content through the immutable `content_store_id` in the
   namespace descriptor;
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
- may upload content and publish explicit commits;
- preserves conflicts according to the client's conflict policy.

### 8.3 Explicit-commit client

This client uses the upload, commit, and change-feed surface more directly.
It stages content and publishes explicit commits, but it does not necessarily
maintain a long-lived local mirror.

Typical behavior:

- content hashing and upload;
- explicit commit with preconditions and commit ids;
- change-feed reads or cursors where incremental observation is needed.

### 8.4 Operator or admin tool

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
