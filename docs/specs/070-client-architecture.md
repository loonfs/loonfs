# Spec 070: client architecture

## First milestone

The first client milestone is a full live mirror, not File Provider hydration.

Why:
the mirror client exercises the core sync semantics with fewer platform-specific variables.

## Client shape

- Rust daemon
- CLI
- later macOS File Provider bridge
- local SQLite state is acceptable

## Local durable truth

The client uses SQLite as its only durable local truth.

The first client-state slice models three durable views explicitly:

- `remote_state`: what remote metadata the client has observed
- `local_state`: what local filesystem state the client has observed
- `sync_anchor`: the last fully reconciled state

Why this shape exists:

- conflict decisions should compare explicit durable views, not reconstructed guesses
- restart behavior should be deterministic
- later File Provider mode should reuse the same truth model

Failure modes prevented:

- planner behavior changing after restart because one view was only in memory
- conflict handling depending on callback ordering instead of durable observed state

## First SQLite schema

The first schema version is intentionally small but durable enough to support one hot-file planner loop.

It must contain at least these tables or equivalent structures:

- `remote_state`
- `local_state`
- `sync_anchor`
- `planned_actions`
- `transfer_ledger`
- `conflicts_and_errors`

The first schema uses SQLite `user_version = 1`.

For the first slice, rows for already-known mirrored files are keyed by:

```text
(namespace_id, inode_id)
```

Rules:

- `namespace_id` is the namespace-scoped durable identity root
- `inode_id` is the canonical file identity whenever the client already knows the remote inode
- `display_name` and `parent_inode_id` are observed views, not canonical identity
- local-only creations that do not yet have a server inode are deferred to a later schema extension

The first table contents are:

- `remote_state`: latest observed `seq`, `revision_no`, content digest, optional downloadable
  content reference, and current observed path view
- `local_state`: latest observed local content digest, current observed path view, `dirty`, and local observation time
- `sync_anchor`: last fully synced remote revision, content digest, and optional downloadable content reference
- `planned_actions`: the current planner output for one `(namespace_id, inode_id)` when work is needed
- `transfer_ledger`: resumable transfer progress keyed to namespace file identity
- `conflicts_and_errors`: durable user-visible explanations

Why the first schema is inode-keyed:

- canonical metadata is inode-keyed everywhere else in the system
- path-only local truth would reintroduce identity ambiguity during rename races

Failure modes prevented:

- the client forgetting whether two observed paths refer to the same canonical file
- local restart logic inventing sync identity from a mutable path string

## Schema v2: temporary local identities

The next schema version adds durable identity for local-only files that do not yet have a remote inode.

It adds:

- `client_metadata`: small durable counters and schema-owned allocator state
- `local_only_state`: durable local observations for files that exist only on the client so far
- `planned_local_only_actions`: durable planner output for those temporary local identities

The durable key for one local-only file is:

```text
client_file_id
```

Rules:

- `client_file_id` is generated inside SQLite, never guessed from a path
- the id must be stable across restart until the file is bound to a real remote inode
- `client_file_id` does not replace canonical inode identity; it is a temporary local bridge until authoritative remote identity exists
- the allocator must be monotonic so later debugging can tell whether two client-local ids were created in order

The first temporary id format is:

```text
tmp:{namespace_id}:{counter:020}
```

Why this rule exists:

- local-only creates still need durable identity before the server assigns an inode
- the client should not key unsynced files only by mutable paths

Failure modes prevented:

- restart losing track of which local-only file a queued upload refers to
- rename-before-upload causing the client to treat one unsynced file as two separate creations
- local planner output pointing at a path string that no longer identifies the same file

## Schema v3: kind-aware local truth

The next schema version adds explicit `inode_kind` to the durable client views that already exist:

- `remote_state`
- `local_state`
- `sync_anchor`
- `local_only_state`

Rules:

- `inode_kind` must use the same canonical values as namespace metadata: `file`, `dir`, `symlink`, `mount`
- planner decisions must not infer directory-vs-file from `content_digest = null`
- migration from the earlier client-only schema may default existing rows to `file`, because all v1/v2 client fixtures only modeled files

Why this rule exists:

- an empty file and a directory are different sync behaviors, even when both have no content digest
- later local-only bind and planner logic must preserve inode kind when moving from temporary identity to canonical inode identity

Failure modes prevented:

- treating an empty file like a directory create
- binding a temporary directory identity into file-keyed planner state

## Planner transaction boundary

One planner pass for one mirrored file must happen inside one SQLite transaction.

The first planner transaction does all of the following atomically:

1. read `remote_state`, `local_state`, and `sync_anchor` for one `(namespace_id, inode_id)`
2. derive one deterministic planner decision
3. replace or clear the current `planned_actions` row for that same file identity

The planner may still perform uploads and downloads outside SQLite, but the decision about what should happen next must not be split across callbacks or partially persisted writes.

Why this rule exists:

- the planner should never observe one durable view and persist a decision against another
- restart behavior should be replayable from SQLite alone

Failure modes prevented:

- crash windows where the planner read new local state but persisted an action against stale remote state
- in-memory planner branches that cannot be reconstructed after restart

## First hot-file decision skeleton

The first deterministic planner rule compares the three durable views for one known mirrored file:

- if local observed state differs from the sync anchor while remote still matches the sync anchor, plan `upload_local_edit`
- if remote observed state differs from the sync anchor while local still matches the sync anchor, plan `download_remote_edit`
- if both local and remote differ from the sync anchor, plan `create_conflict_copy`
- if both still match the sync anchor, clear any pending action and return `no_op`

This first rule is intentionally narrow. It is enough to prove that restart-safe planner state can drive one hot-file case before the client grows broader sync semantics.

## First local-only create rule

For one local-only file with no remote inode yet:

- if `local_only_state` says the file exists and is dirty, plan `upload_local_create`
- if it no longer exists on disk, clear any planned local-only action and return `no_op`

The planned row must reference `client_file_id`, not a guessed path identity.

This rule is intentionally small. It proves that local-only files can survive restart with a durable temporary identity before the client learns a canonical remote inode.

## First local-only directory create rule

For one local-only directory with no remote inode yet:

- if `local_only_state.inode_kind = dir` and `exists_on_disk = true`, plan `create_remote_dir`
- if it no longer exists on disk, clear any planned local-only action and return `no_op`

The planned row must still reference `client_file_id`, not a guessed path identity.

Why this rule exists:

- directory creates need restart-safe durable identity too, even though they do not upload content blocks

Failure modes prevented:

- creating the same local-only directory twice because restart forgot the temporary identity
- treating a directory create like a file upload just because both are unsynced local-only items

## First bound-directory child observation rule

After a directory is already bound to a canonical remote inode, the client may persist newly observed local-only children beneath that bound parent.

The first child-observation preconditions are intentionally strict:

- `parent_inode_id` must already exist in `remote_state`, `local_state`, and `sync_anchor`
- those three rows must all agree that the parent `inode_kind = dir`
- the remote parent row must not be deleted
- the local parent row must still exist on disk and must not be dirty
- the three parent rows must still agree on `inode_kind`, `parent_inode_id`, `display_name`, and `content_digest`

On success, one SQLite transaction must:

1. allocate a fresh `client_file_id`
2. persist one `local_only_state(client_file_id)` row for the child
3. store the canonical `parent_inode_id` of the already-bound directory on that child row

After that persistence step, normal local-only planning rules apply:

- a child `file` plans `upload_local_create`
- a child `dir` plans `create_remote_dir`

Why this rule exists:

- child creates under a known directory should immediately use canonical parent identity, not a mutable path string
- restart should not lose which bound parent a local-only child belongs under

Failure modes prevented:

- persisting a child under a file inode
- planning a child create against a parent directory whose local and remote views already diverged
- watcher code inventing child identity from `"/path/to/dir/name"` after restart instead of durable `(parent_inode_id, client_file_id)`

Failure modes named for the first implementation:

- `local_only_parent_missing`
- `local_only_parent_not_directory`
- `local_only_parent_not_bound`

## First durable content upload path

Before the client can publish `create_file`, it must turn one observed local file into
immutable durable content objects.

The first upload helper takes:

- `namespace_id`
- a caller-supplied local filesystem path

One successful upload must:

1. read the local file bytes
2. split them into fixed `16 MiB` plaintext blocks
3. write each block immutably at `namespaces/{namespace_id}/blobs/{block_digest_sha256}`
4. build deterministic JSON `ContentManifestEnvelope` bytes
5. write that manifest immutably at
   `namespaces/{namespace_id}/manifests/{content_manifest_digest}.json`
6. return `file_digest_sha256` plus `content_manifest_digest`

Rules:

- block digests and whole-file digests use `sha256:<hex>` over plaintext bytes
- the content manifest object must be content-addressed and uploader-independent
- if an immutable block or manifest object already exists, the client may reuse it only after
  verifying the existing bytes are identical
- the first slice may read the whole file from the supplied path and does not yet need resumable
  transfer state

Why this rule exists:

- file bytes must become durable before metadata publish
- the first create-file happy path should already use the same immutable content layout the rest
  of the system will read later

Failure modes prevented:

- publishing `create_file` with only a local digest string and no durable manifest object
- different uploaders writing different manifest bytes under the same manifest digest
- treating provider-side existence as proof that the immutable object body matches the local file

## Local-only upload ledger (schema v5)

The first restart-safe `create_file` path adds one narrow durable table:

- `local_only_uploads`

One row is keyed by:

```text
client_file_id
```

The row must durably map:

- `client_file_id`
- `namespace_id`
- `file_digest_sha256`
- `content_manifest_digest`
- `manifest_object_key`
- `file_size_bytes`
- `uploaded_at_ms`

Rules:

- the client may persist this row only for a local-only `file`
- persisting the row must validate that the uploaded namespace matches the local-only row
- persisting the row must validate that the uploaded `file_digest_sha256` still matches the
  current `local_only_state.content_digest`
- when building `create_file`, the executor must load `content_manifest_digest` from this durable
  row, not from an in-memory upload result
- if the local-only file later binds to a real inode, the temp upload row must be cleared in the
  same transaction that clears the temp identity

Why this rule exists:

- upload and publish are separate durable steps
- restart must not force the executor to rediscover upload state from a local filesystem path
- if the local file changed after upload, the executor must refuse to publish stale content

Failure modes prevented:

- losing the uploaded manifest digest across restart and rebuilding a request from ambient memory
- publishing `create_file` after the local-only file changed to a different digest
- leaving temp upload state behind after the temp identity is already bound to a real inode

## Client mutation contract

The first executor does not invent a separate long-lived sync protocol. It emits
one narrow request shape that the authoritative side can translate directly into one namespace
commit request:

```json
{
  "namespace_id": "ns-1",
  "client_request_id": "client-req-0001",
  "op": {
    "create_file": {
      "parent_inode_id": 902,
      "display_name": "note.txt",
      "content_manifest_digest": "sha256:child-note"
    }
  }
}
```

The initial client mutation contract supports only:

- `create_remote_dir` -> `ClientMutationOp::CreateDir`
- `upload_local_create` -> `ClientMutationOp::CreateFile`

The next bound-file edit extension adds:

- `upload_local_edit` -> `ClientMutationOp::ReplaceFile`

Rules:

- one planned local-only action maps to one client mutation request
- the request carries canonical `parent_inode_id`, never a parent path string
- `client_request_id` is stable for retries and becomes the authoritative `request_id`
- for `create_file`, the executor must load `content_manifest_digest` from the durable
  `local_only_uploads(client_file_id)` row, not from caller memory and not from the local observed
  file digest
- for `replace_file`, the executor must load `content_manifest_digest` from a durable inode-keyed
  upload row, not from caller memory and not from the local observed file digest
- `replace_file` must carry canonical `inode_id` plus the current bound `base_revision_no`, never
  a path string

Why this rule exists:

- we want one thin happy-path bridge from planner output to authoritative commit publish
- the initial contract should prove end-to-end request shaping before broader API design lands

Failure modes prevented:

- the executor guessing parent identity from mutable paths
- planner output being coupled to a transport shape that cannot become a namespace commit request
- retrying the same local-only action with a different authoritative request id

Failure modes named for the first implementation:

- `planned_local_only_action_missing`
- `uploaded_content_missing`
- `uploaded_content_requires_file`
- `uploaded_content_namespace_mismatch`
- `uploaded_content_digest_mismatch`
- `uploaded_content_local_digest_missing`

## Inode upload ledger (schema v7)

The first restart-safe inode-keyed file-edit path adds one narrow durable table:

- `inode_uploads`

One row is keyed by:

```text
(namespace_id, inode_id)
```

The row must durably map:

- `namespace_id`
- `inode_id`
- `file_digest_sha256`
- `content_manifest_digest`
- `manifest_object_key`
- `file_size_bytes`
- `uploaded_at_ms`

Rules:

- the client may persist this row only for a bound `file` inode
- persisting the row must validate that the uploaded namespace matches the bound inode row
- persisting the row must validate that the uploaded `file_digest_sha256` still matches the
  current `local_state.content_digest`
- when building `replace_file`, the executor must load `content_manifest_digest` from this durable
  row, not from an in-memory upload result
- successful edit publish may keep the row, because immutable content can be safely reused by a
  later retry or repeated content digest

Why this rule exists:

- bound-file edit should use the same durable upload-before-publish split as local-only create
- restart should not force the client to rediscover uploaded edit content from an ambient path
- if the local file changed after upload, the executor must refuse to publish stale content

Failure modes prevented:

- rebuilding `replace_file` from a local filesystem path after restart
- publishing `replace_file` after the local file changed to a different digest
- running a file-content executor against a directory inode

Failure modes named for the first implementation:

- `inode_upload_missing`
- `inode_upload_requires_file`
- `inode_upload_namespace_mismatch`
- `inode_upload_digest_mismatch`
- `inode_upload_local_digest_missing`

## Pending inode mutation request ledger (schema v7)

The first restart-safe inode-keyed mutation path adds:

- `pending_inode_mutations`

One row is keyed by:

```text
client_request_id
```

The row must durably map:

- `client_request_id`
- `namespace_id`
- `inode_id`
- `request_json`
- `created_at_ms`

Rules:

- the client must persist this row before it treats a bound-file edit request as in flight
- the mapping from `client_request_id` to `(namespace_id, inode_id)` must stay stable across
  restart
- `request_json` must be the exact `ClientMutationRequest` bytes that were dispatched
- if a pending row already exists for one `(namespace_id, inode_id)`, retry must reuse the stored
  `client_request_id` and stored `request_json` instead of allocating a new request id or
  rebuilding from current local state
- recording the same request twice is only valid if it still points at the same namespace, bound
  inode, and exact request body

Why this rule exists:

- a successful authoritative edit may be observed after restart
- retrying the same inode-keyed edit must not silently mutate the request body after the first
  attempt

Failure modes prevented:

- post-restart success that cannot be matched back to the correct bound inode
- retrying one bound-file edit with a newly generated request id
- local state drifting after the first dispatch attempt and silently changing the retried request

Failure modes named for the first implementation:

- `pending_inode_mutation_missing`
- `pending_inode_mutation_conflict`
- `pending_inode_mutation_inode_conflict`
- `pending_inode_mutation_namespace_mismatch`
- `pending_inode_mutation_request_missing`

## First bound-file edit executor

The first inode-keyed executor is intentionally narrow. It only handles:

- `upload_local_edit`

The executor takes:

- `namespace_id`
- `inode_id`
- an optional caller-supplied local filesystem path
- one upload timestamp
- one dispatch timestamp

Rules:

- if `pending_inode_mutations(namespace_id, inode_id)` already contains a row, the executor must
  reuse that durable `request_json` and must not rebuild the request from current local state
- if no pending row exists, the executor must load `planned_actions(namespace_id, inode_id)` and
  require `decision = upload_local_edit`
- the first implementation only supports content replacement, not rename or move, so the executor
  must require `local_state`, `remote_state`, and `sync_anchor` to exist and to agree on
  `inode_kind = file`, `parent_inode_id`, and `display_name`
- the executor must require `remote_state` to still match `sync_anchor` for the current bound
  revision before freezing the request
- if `inode_uploads(namespace_id, inode_id)` already matches the current local digest, the
  executor may reuse that upload row and skip rereading the local path
- otherwise the executor must upload the supplied local file path, then persist the resulting
  upload row before building the request
- the frozen request must carry:
  - canonical `inode_id`
  - `base_revision_no = sync_anchor.revision_no`
  - `content_manifest_digest` from the durable inode upload row

Why this rule exists:

- the first bound-file edit path should advance real file revisions, not only surface planned work
- content replacement should become restart-safe before the client grows broader inode-keyed
  download/conflict logic
- rename or move needs a stricter protocol than a content-only replace request

Failure modes prevented:

- rereading a local file path during retry and silently publishing different bytes under the same
  inode edit intent
- emitting `replace_file` from a planner state that already includes an unmodeled rename or move
- publishing a file edit against a stale bound revision

Failure modes named for the first implementation:

- `upload_local_edit_decision_missing`
- `upload_local_edit_state_missing`
- `upload_local_edit_requires_file`
- `upload_local_edit_path_change_not_supported`
- `upload_local_edit_remote_not_converged`
- `upload_local_edit_source_path_missing`

## Schema v8: bound remote content references

The next schema version adds one nullable field to the bound remote views for file revisions:

- `remote_state.content_manifest_digest`
- `sync_anchor.content_manifest_digest`

Rules:

- the field is only meaningful for `inode_kind = file`
- the field may stay `null` for older rows or for observations that do not yet carry a durable
  content reference
- when present, the field must be the immutable manifest object digest that can reproduce the
  authoritative file bytes for that revision
- local-only rows do not need this field, because uploads already use `local_only_uploads`
  or `inode_uploads`

Why this rule exists:

- `content_digest` alone is not enough to fetch bytes from object storage
- the first download executor needs a durable object-store locator for the remote file body

Failure modes prevented:

- planning `download_remote_edit` without any way to fetch the referenced remote bytes
- using a whole-file digest as though it were a manifest object key

Failure modes named for the first implementation:

- `download_remote_edit_manifest_missing`

## First bound-file download executor

The first inode-keyed download executor is also intentionally narrow. It only handles:

- `download_remote_edit`

The executor takes:

- `namespace_id`
- `inode_id`
- one caller-supplied local filesystem path
- one explicit local apply timestamp

Rules:

- the executor must load `planned_actions(namespace_id, inode_id)` and require
  `decision = download_remote_edit`
- the first implementation only supports content replacement, not rename or move, so the executor
  must require `remote_state`, `local_state`, and `sync_anchor` to exist and to agree on
  `inode_kind = file`, `parent_inode_id`, and `display_name`
- the executor must require `local_state` to still match `sync_anchor` before download
- the executor must require `remote_state.content_digest` to be present
- the executor must require `remote_state.content_manifest_digest` to be present
- the executor must fetch and verify the immutable manifest and blocks from object storage before
  writing the local path
- the executor must require the downloaded whole-file digest to match
  `remote_state.content_digest`
- after the local write succeeds, one SQLite transaction must:
  - update `local_state` to the remote digest
  - clear `dirty`
  - advance `sync_anchor` to the current `remote_state` revision and manifest reference
  - clear `planned_actions(namespace_id, inode_id)`

Why this rule exists:

- the first two-way bound-file sync demo needs a real downward content path, not only uploads
- the client should only claim convergence after it has both durable remote bytes and a durable
  local state transition

Failure modes prevented:

- overwriting a locally diverged file with remote bytes
- downloading remote bytes without a durable manifest reference
- applying a remote observation whose manifest bytes do not match the observed file digest
- claiming convergence after a partial or unverified local file write

Failure modes named for the first implementation:

- `download_remote_edit_decision_missing`
- `download_remote_edit_state_missing`
- `download_remote_edit_requires_file`
- `download_remote_edit_path_change_not_supported`
- `download_remote_edit_local_not_converged`
- `download_remote_edit_remote_digest_missing`
- `download_remote_edit_remote_digest_mismatch`
- `download_remote_edit_source_path_missing`

## Pending client mutation request ledger (schema v6)

The next schema version widens the durable bridge for in-flight authoritative creates:

- `pending_client_mutations`
- `client_metadata.next_client_request_id`

One row is keyed by:

```text
client_request_id
```

The row must durably map:

- `client_request_id`
- `namespace_id`
- `client_file_id`
- `request_json`
- `created_at_ms`

Rules:

- `client_metadata.next_client_request_id` allocates monotonically formatted request ids like
  `client-req-00000000000000000001`
- the client must persist this row before it treats a create request as in flight
- the mapping from `client_request_id` to `client_file_id` must stay stable across restart
- `request_json` must be the exact `ClientMutationRequest` bytes that were dispatched
- if a pending row already exists for a `client_file_id`, retry must reuse the stored
  `client_request_id` and stored `request_json` instead of allocating a new request id or
  rebuilding from current local state
- recording the same request twice is only valid if it still points at the same namespace,
  temporary local identity, and exact request body

Why this rule exists:

- a successful authoritative create may be observed after a client restart
- the bind step still needs to know which temporary local identity that success belongs to
- a failed or lost dispatch retry must not accidentally send a different request body under the
  same semantic create intent

Failure modes prevented:

- a post-restart success response that cannot be matched back to the correct `client_file_id`
- the client binding one authoritative inode to the wrong temporary local file
- retrying a failed dispatch with a newly generated request id
- local state drifting after the first dispatch attempt and silently changing the retried request

Failure modes named for the first implementation:

- `pending_client_mutation_missing`
- `pending_client_mutation_conflict`
- `pending_client_mutation_client_file_conflict`
- `pending_client_mutation_namespace_mismatch`
- `pending_client_mutation_request_missing`

## First small-file create executor

The first end-to-end file-create executor is intentionally narrow. It only handles one local-only
planner outcome:

- `upload_local_create`

The executor takes:

- `client_file_id`
- one caller-supplied local filesystem path
- upload and dispatch timestamps

Rules:

- if `pending_client_mutations` already contains a row for `client_file_id`, the executor must
  reuse that durable `request_json` and must not rebuild the request from current local state
- if no pending row exists, the executor must load `planned_local_only_actions(client_file_id)` and
  require `decision = upload_local_create`
- if `local_only_uploads(client_file_id)` already matches the current local-only digest, the
  executor may reuse that upload row and skip rereading the local path
- otherwise the executor must upload the supplied local file path, then persist the resulting
  upload row before building the request
- persisting the upload row must still validate that the uploaded file digest matches the current
  `local_only_state.content_digest`
- after upload is ensured, the executor must dispatch through the same pending-request flow and
  bind the authoritative success response in the same way as any other create

Why this rule exists:

- the first client happy path should be one executable action, not separate upload and dispatch
  choreography in the caller
- retries after a failed dispatch must stay stable even if the local file changes afterward
- a caller-supplied path is only for producing durable content, never for guessing metadata identity

Failure modes prevented:

- rereading a local file path during retry and silently publishing different bytes under the same
  create intent
- dispatching `create_file` without first freezing durable content into `local_only_uploads`
- running a file-content executor against a non-file local-only planner action

Failure modes named for the first implementation:

- `upload_local_create_decision_missing`
- `uploaded_content_digest_mismatch`

## First directory create executor

The matching directory executor is also intentionally narrow. It only handles one local-only
planner outcome:

- `create_remote_dir`

The executor takes:

- `client_file_id`
- one dispatch timestamp

Rules:

- if `pending_client_mutations` already contains a row for `client_file_id`, the executor must
  reuse that durable `request_json` and must not rebuild the request from current local state
- if no pending row exists, the executor must load `planned_local_only_actions(client_file_id)` and
  require `decision = create_remote_dir`
- after that check, the executor must dispatch through the same pending-request flow and bind the
  authoritative success response in the same way as any other create

Why this rule exists:

- directory create should use the same durable request/bind discipline as file create
- the client action surface should not force callers to know whether retries are rebuilding from
  state or reusing a prior pending request

Failure modes prevented:

- running a directory-create executor against a file upload planner action
- retrying a failed directory create with a newly rebuilt request id

Failure modes named for the first implementation:

- `create_remote_dir_decision_missing`

## Unified local-only create executor

The first higher-level local-only create entrypoint sits above the narrow file and directory
executors. It handles exactly two planner outcomes:

- `upload_local_create`
- `create_remote_dir`

The executor takes:

- `client_file_id`
- an optional caller-supplied local filesystem path
- one upload timestamp
- one dispatch timestamp

Rules:

- if `pending_client_mutations` already contains a row for `client_file_id`, the executor must
  choose its branch from the durable stored `request_json`, not from current planner state
- a stored pending `create_dir` request must retry without consulting `planned_local_only_actions`
- a stored pending `create_file` request must retry without rereading the caller-supplied path
- if no pending row exists, the executor must load `planned_local_only_actions(client_file_id)`
  and branch only on:
  - `decision = upload_local_create`
  - `decision = create_remote_dir`
- if the selected branch is `upload_local_create` and `local_only_uploads(client_file_id)` already
  matches the current local-only digest, the executor may dispatch without a caller-supplied local
  path
- if the selected branch is `upload_local_create` and no matching upload row exists yet, the
  caller-supplied local path is required so the executor can freeze durable content before dispatch
- if the selected branch is `create_remote_dir`, the caller-supplied local path is ignored

Why this rule exists:

- callers should ask for “execute this local-only create” once instead of choosing between separate
  file and directory paths
- restarts should still allow a file create to proceed after content is already durable, even if
  the original source path is no longer available
- the durable pending request and upload rows remain the source of truth for retries

Failure modes prevented:

- retrying a frozen file create by rereading a local path that has changed or disappeared
- forcing callers to know whether durable content is already available before picking an executor
- rebuilding a retry from current planner state after a request has already been frozen

Failure modes named for the first implementation:

- `local_only_create_source_path_missing`

## First local-only create scheduler loop

The first automatic client sync loop for local-only creates is intentionally small. It does not
scan the filesystem itself and it does not run multiple creates concurrently. It performs one step:

1. select one row from `planned_local_only_actions`
2. resolve an optional source path for that `client_file_id`
3. run the unified local-only create executor for that row

Rules:

- row selection must be deterministic:
  - lowest `created_at_ms` first
  - then lexicographically smallest `client_file_id` as the tie-breaker
- if there is no row in `planned_local_only_actions`, the loop must return `no_work`
- the loop must pass the selected `client_file_id` into the unified local-only create executor and
  must not rebuild separate file-vs-directory logic of its own
- the loop may ask a caller-supplied resolver for a source path only for the selected
  `client_file_id`
- if the selected row is a directory create, the resolver result is ignored
- if the selected row is a file create and durable uploaded content is already present, the loop
  may still succeed even when the resolver returns no path

Why this rule exists:

- one deterministic “next create” loop is enough to demonstrate automatic client progress
- the caller should provide path lookup, not orchestration policy
- tie-breaking must be explicit so tests and restart behavior stay reproducible

Failure modes prevented:

- two equivalent clients choosing different next local-only creates from the same SQLite state
- a scheduler accidentally reimplementing file-vs-directory branching differently from the
  underlying executor

## First mixed client tick

The first broader client tick arbitrates between:

- `planned_local_only_actions`
- `planned_actions`

It is still intentionally partial. In this first implementation it may:

- execute one selected local-only create immediately
- execute one selected inode-keyed `upload_local_edit`
- execute one selected inode-keyed `download_remote_edit`
- or surface one selected inode-keyed planned action as the next scheduled work item

Rules:

- the tick must load at most one candidate row from each table:
  - the deterministic next `planned_local_only_actions` row
  - the deterministic next `planned_actions` row
- cross-table arbitration must be deterministic:
  - lower `created_at_ms` first
  - if `created_at_ms` ties, `planned_local_only_actions` wins over `planned_actions`
- if the selected row comes from `planned_local_only_actions`, the tick must execute it through the
  existing local-only create loop
- if the selected row comes from `planned_actions` and `decision = upload_local_edit`, the tick
  must execute it through the bound-file edit executor
- if the selected row comes from `planned_actions` and `decision = download_remote_edit`, the tick
  must execute it through the bound-file download executor
- if the selected row comes from `planned_actions` and the decision is anything else, the tick must
  return that planned action as the next scheduled work item without trying to silently emulate
  download/conflict behavior yet
- if both tables are empty, the tick must return `no_work`

Why this rule exists:

- one scheduler surface is more valuable than separate “creates only” and “everything else later”
  entrypoints
- we should not fake support for inode-keyed conflict actions before their executors exist
- a deterministic cross-table tie-break keeps restart behavior and tests reproducible

Failure modes prevented:

- local-only creates and inode-keyed planned actions being scheduled by unrelated policies
- a partial implementation pretending it executed an inode-keyed action when it only selected it
- a bound-file edit staying permanently stuck in `planned_actions` after the upload and replace
  path already exists

## Bind-after-publish loop

After a successful `create_file` or `create_dir`, the authoritative side returns one committed
create summary:

- `namespace_id`
- `client_request_id`
- `committed_seq`
- `created_inode.inode_id`
- `created_inode.inode_kind`
- `created_inode.revision_no`
- `created_inode.parent_inode_id`
- `created_inode.display_name`
- `created_inode.content_digest`

For the current create-only contract, `created_inode.revision_no` is `1` and
`created_inode.content_digest` is `null` for directories. For files, it is the authoritative
whole-file digest from the validated manifest payload.

On receipt of that response, one SQLite transaction must:

1. load `pending_client_mutations(client_request_id)`
2. derive the authoritative `remote_state` row from the committed create summary
3. bind the stored `client_file_id` into inode-keyed `remote_state`, `local_state`, and `sync_anchor`
4. delete the matching `pending_client_mutations` row
5. delete the temporary `local_only_state` and `planned_local_only_actions` rows

## First apply-after-publish loop for bound-file edit

After a successful `replace_file`, the authoritative side returns one committed edit summary:

- `namespace_id`
- `client_request_id`
- `committed_seq`
- `replaced_file.inode_id`
- `replaced_file.inode_kind`
- `replaced_file.revision_no`
- `replaced_file.content_digest`

On receipt of that response, one SQLite transaction must:

1. load `pending_inode_mutations(client_request_id)`
2. load the current bound `remote_state`, `local_state`, and `sync_anchor` rows for that inode
3. derive the authoritative next `remote_state` row by updating `observed_seq`, `revision_no`,
   and `content_digest` while preserving the current bound `parent_inode_id` and `display_name`
4. update `local_state` to the same `content_digest` and clear `dirty`
5. update `sync_anchor` to the same `committed_seq`, `revision_no`, and `content_digest`
6. delete the matching `pending_inode_mutations` row
7. clear any `planned_actions(namespace_id, inode_id)` row because the file is converged again

Rules:

- the first implementation only accepts `replaced_file` when the bound inode rows still represent
  a plain file content edit, not a rename or move
- the client must reject a response that carries both `created_inode` and `replaced_file`, or
  neither of them

Why this rule exists:

- a restart-safe file edit needs the same durable response-apply step as local-only create
- the local client should become converged from the authoritative result, not from optimistic local
  memory

Failure modes prevented:

- losing which bound inode an acknowledged `replace_file` belonged to after restart
- clearing a dirty local edit without actually advancing the authoritative revision anchor

After that transaction commits, a restart and normal planner tick for the bound inode must return
`no_op` / `already_converged`.

Why this rule exists:

- a successful create should converge immediately instead of waiting for a later full directory crawl
- restart must not lose either the in-flight request mapping or the converged bound state

Failure modes prevented:

- create success returning an inode id that the client never persists durably
- restart after success re-planning the same local-only create because the bind was only in memory

## First local-only bind rule

After a local-only create is successfully published and the client later observes the authoritative remote inode, the client must bind the temporary `client_file_id` into the inode-keyed tables.

The first bind preconditions are intentionally strict:

- `local_only_state.namespace_id` must equal the observed remote namespace
- `local_only_state.inode_kind` must equal the observed remote inode kind
- the observed remote file must not be deleted
- `local_only_state.exists_on_disk` must still be true
- `content_digest`, `parent_inode_id`, and `display_name` must still match between the local-only row and the observed remote row

On success, one SQLite transaction must do all of the following:

1. upsert `remote_state(namespace_id, inode_id)` from the observed remote file
2. upsert `local_state(namespace_id, inode_id)` from the local-only row, but mark `dirty = false`
3. upsert `sync_anchor(namespace_id, inode_id)` from the observed remote file
4. clear any `planned_actions(namespace_id, inode_id)` row because the file is converged at bind time
5. delete the old `planned_local_only_actions(client_file_id)` row
6. delete the old `local_only_state(client_file_id)` row

Why this rule exists:

- the planner must not forget that the uploaded local-only create is now the synced anchor
- later planner passes must key the file by canonical inode identity, not by a temporary local id

Failure modes prevented:

- upload success followed by remote observation causing the client to upload the same create again
- deleting the temp identity before durable `sync_anchor` exists
- binding a temp identity to a different remote file after a local rename or edit changed the local observation

Failure modes named for the first implementation:

- `local_only_file_missing`
- `bind_kind_mismatch`
- `bind_namespace_mismatch`
- `bind_remote_deleted`
- `bind_observation_mismatch`

If any bind precondition fails, the transaction must abort without partially migrating rows.

## Local data model inspiration

The client should keep separate, individually consistent views of:

- remote observed state
- local observed state
- last fully synced state

Why it exists:
conflict reasoning and convergence are much easier when directionality is explicit.

## Client responsibilities

- watch local changes
- poll or subscribe to remote changes
- plan sync work deterministically
- persist enough local state to recover after restart
- avoid publishing partial local observations as canonical truth

## Later File Provider rule

Online-only placeholders must use the same canonical inode and revision semantics as full mirror mode.
The platform integration layer must not invent a different sync model.
