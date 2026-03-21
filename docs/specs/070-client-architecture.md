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
- `transfer_ledger`: bounded transfer progress keyed to namespace file identity
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
- before replacing the target path, the executor must write the downloaded bytes to one staging
  file in the same directory as the target path
- the executor must sync the staging file before it renames that staging file over the target path
- the target replacement must use one atomic rename in the same directory
- where the platform permits parent-directory sync, the executor must sync the parent directory
  after the rename before it claims convergence
- if a stale staging file from an earlier interrupted attempt already exists, the executor may
  overwrite it; the staging file is scratch state, not durable truth
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
- claiming convergence after a partial, truncated, or unverified local file write
- crash windows where the target path is rewritten in place before the replacement bytes are fully
  staged and synced

Failure modes named for the first implementation:

- `download_remote_edit_decision_missing`
- `download_remote_edit_state_missing`
- `download_remote_edit_requires_file`
- `download_remote_edit_path_change_not_supported`
- `download_remote_edit_local_not_converged`
- `download_remote_edit_remote_digest_missing`
- `download_remote_edit_remote_digest_mismatch`
- `download_remote_edit_source_path_missing`
- `download_remote_edit_local_apply_failed`

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

- the first mixed tick uses one explicit scheduler policy, `client_tick_scheduler_v1`
- `client_tick_scheduler_v1` has three priority buckets:
  - `local_only_create`:
    the deterministic next row from `planned_local_only_actions`
  - `executable_inode_action`:
    the deterministic next row from `planned_actions` where
    `decision in {upload_local_edit, download_remote_edit, materialize_remote_dir}`
  - `deferred_inode_action`:
    the deterministic next row from `planned_actions` where
    `decision not in {upload_local_edit, download_remote_edit, materialize_remote_dir}`
- the tick must load at most one candidate row from each scheduler bucket
- bucket priority is strict:
  - `local_only_create` always wins over `executable_inode_action`
  - `executable_inode_action` always wins over `deferred_inode_action`
  - `created_at_ms` is only a tie-break inside one bucket, not across different buckets
- deterministic row order inside each bucket must be:
  - for `planned_local_only_actions`: lower `created_at_ms`, then lexicographically smaller
    `client_file_id`
  - for `planned_actions`: lower `created_at_ms`, then lexicographically smaller `namespace_id`,
    then lower `inode_id`
- if the selected row comes from `planned_local_only_actions`, the tick must execute it through the
  existing local-only create loop
- if the selected row comes from `executable_inode_action` and `decision = upload_local_edit`,
  the tick must execute it through the bound-file edit executor
- if the selected row comes from `executable_inode_action` and
  `decision = download_remote_edit`, the tick must execute it through the bound-file download
  executor, which may either refresh an already bound file or materialize a discovered remote-only
  file
- if the selected row comes from `executable_inode_action` and
  `decision = materialize_remote_dir`, the tick must execute it through the remote-only directory
  materialization executor
- if the selected row comes from `deferred_inode_action`, the tick must return that planned action
  as the next scheduled work item without trying to silently emulate download/conflict behavior yet
- if both tables are empty, the tick must return `no_work`

Why this rule exists:

- one scheduler surface is more valuable than separate “creates only” and “everything else later”
  entrypoints
- executable inode actions should make progress even when an older deferred inode action is still
  waiting for a later implementation slice
- we should not fake support for inode-keyed conflict actions before their executors exist
- bucketed selection plus deterministic intra-bucket ordering keeps restart behavior and tests
  reproducible

Failure modes prevented:

- local-only creates and inode-keyed planned actions being scheduled by unrelated policies
- an older deferred inode action blocking a newer but executable bound-file edit or download
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

For `create_file` only, the apply step must also accept one idempotent late-response case:

- if an earlier authoritative observation already bound the temp identity and therefore removed the
  temporary `local_only_state` row, `apply_client_mutation_response` may still succeed
- that idempotent success is valid only when the current inode-keyed `remote_state`, `local_state`,
  and `sync_anchor` already match the committed file result on:
  - `namespace_id`
  - `inode_id`
  - `inode_kind`
  - `committed_seq`
  - `revision_no`
  - `content_digest`
  - `parent_inode_id`
  - `display_name`
  - `local.exists_on_disk = true`
  - `local.dirty = false`
- on that idempotent success, the client must still delete the matching
  `pending_client_mutations(client_request_id)` row
- otherwise the apply step must fail closed as `pending_client_mutation_missing` or
  `local_only_file_missing`

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

## First remote observation apply path

The first remote observation path is intentionally narrower than a full remote crawl. It accepts
one explicit observed inode shape:

- `namespace_id`
- `inode_id`
- `inode_kind`
- `observed_seq`
- `revision_no`
- `content_digest`
- `content_manifest_digest`
- `parent_inode_id`
- `display_name`
- `is_deleted`

Rules:

- if the client already has `remote_state(namespace_id, inode_id)` and the incoming
  `observed_seq` is not newer, the observation must be ignored as stale
- if the client already has a bound inode for `(namespace_id, inode_id)`, the client may upsert
  the newer `remote_state` row from the observation
- if that bound inode's `local_state` still matches the observed inode on:
  - `inode_kind`
  - `content_digest`
  - `parent_inode_id`
  - `display_name`
  - `exists_on_disk = true`
  then the client must treat the observation as authoritative convergence:
  - clear `dirty`
  - advance `sync_anchor`
  - clear `planned_actions(namespace_id, inode_id)`
  - delete any matching `pending_inode_mutations` row for that inode
- otherwise, for an already bound inode, the client must only update `remote_state` and leave the
  planner to decide between later download or conflict handling
- if there is no bound inode yet, the client may bind exactly one matching `local_only_state` row
  when all of the strict local-only bind preconditions still hold
- after a late bind succeeds, the client must retain any matching
  `pending_client_mutations(client_request_id)` row until the corresponding create response
  arrives
- if more than one `local_only_state` row matches the same observation, the apply step must:
  - avoid partial migration of `remote_state`, `local_state`, or `sync_anchor`
  - record or replace one durable `conflicts_and_errors` row with
    `kind = remote_observation_bind_ambiguous`
- if there is still no bound inode and no matching `local_only_state`, the first post-reset remote
  discovery slice may materialize one remote-only placeholder when all of the following hold:
  - `inode_kind in {file, dir}`
  - `is_deleted = false`
  - the client does not already have a bound `local_state` row for that inode
- remote-only discovery must:
  - upsert `remote_state(namespace_id, inode_id)` from the authoritative observation
  - upsert `local_state(namespace_id, inode_id)` as a placeholder with:
    - `inode_kind = observed.inode_kind`
    - `content_digest = null`
    - `parent_inode_id = observed.parent_inode_id`
    - `display_name = observed.display_name`
    - `exists_on_disk = false`
    - `dirty = false`
  - leave `sync_anchor(namespace_id, inode_id)` absent until local materialization succeeds
- if a later newer observation arrives for that same remote-only placeholder before local
  materialization, the client may advance `remote_state` and refresh the placeholder path view from
  the newer authoritative observation
- deleted inodes and unsupported kinds may still be ignored in this first slice

Why this rule exists:

- authoritative success should still converge the client when the immediate response is lost
- the first remote-observation path should repair known create/edit flows without pretending the
  client already has full remote tree ingestion
- remote-only file and directory discovery should become restart-safe durable state before full
  remote crawl and tree materialization exist
- temp identities should only bind through one deterministic matching rule

Failure modes prevented:

- a successful `replace_file` remaining dirty forever because the success response was dropped
- a successful local-only create being uploaded again after a later remote observation
- two temp identities both binding to the same observed remote inode
- dropping a remote-only authoritative file on the floor because no immediate local bind exists
- dropping a remote-only authoritative directory on the floor because no immediate local bind
  exists

Failure modes named for the first implementation:

- `remote_observation_bind_ambiguous`

## First remote-only file materialization path

The first remote-only materialization path is intentionally file-only and reuses
`download_remote_edit`.

Rules:

- `download_remote_edit` may run in either of two shapes:
  - bound-file refresh:
    `remote_state`, `local_state`, and `sync_anchor` all exist for the inode
  - discovered remote-only file materialization:
    `remote_state` exists, `local_state` exists as a non-dirty placeholder with
    `exists_on_disk = false`, and `sync_anchor` is still absent
- for the discovered remote-only file shape, the local placeholder must still match the remote
  path view on:
  - `inode_kind`
  - `parent_inode_id`
  - `display_name`
- after bytes are downloaded and atomically written locally, one SQLite transaction must:
  - update `local_state` to `exists_on_disk = true`, `dirty = false`, and
    `content_digest = remote_state.content_digest`
  - create `sync_anchor(namespace_id, inode_id)` from the authoritative remote row
  - clear `planned_actions(namespace_id, inode_id)`

Why this rule exists:

- remote-only discovery is not useful if the client cannot later materialize the discovered file
- the first remote-only path should reuse the existing durable download executor instead of growing
  a separate ad hoc materializer

Failure modes prevented:

- remote-only files surviving restart in SQLite but never becoming executable work
- downloading a remote-only file without ever establishing a durable synced anchor

## First remote-only directory materialization path

The first remote-only directory materialization path is separate from file download and uses one
explicit planner decision:

- `materialize_remote_dir`

Rules:

- `materialize_remote_dir` is valid only when:
  - `remote_state(namespace_id, inode_id)` exists
  - `remote_state.inode_kind = dir`
  - `remote_state.is_deleted = false`
  - `local_state(namespace_id, inode_id)` exists as a non-dirty placeholder with:
    - `inode_kind = dir`
    - `content_digest = null`
    - `exists_on_disk = false`
  - `sync_anchor(namespace_id, inode_id)` is still absent
- the local placeholder must still match the authoritative path view on:
  - `parent_inode_id`
  - `display_name`
- after the local directory is created durably, one SQLite transaction must:
  - update `local_state` to `exists_on_disk = true`, `dirty = false`
  - create `sync_anchor(namespace_id, inode_id)` from the authoritative remote row
  - clear `planned_actions(namespace_id, inode_id)`

Why this rule exists:

- remote-only directory discovery should become executable work, not durable dead state
- directory materialization should stay explicit instead of being smuggled through the file
  download path

Failure modes prevented:

- a discovered remote-only directory never becoming visible on disk because it has no content
  manifest to download
- reusing the file download executor for a directory inode and silently depending on file-only
  assumptions

## First durable discovery and reconciliation issue records

The first durable failure surfacing paths use two SQLite tables:

- `conflicts_and_errors`
- `local_only_conflicts_and_errors`

`local_only_conflicts_and_errors` is introduced in schema v10 so temp-identity upload failures can
survive restart before a real inode exists.

The inode-keyed row shape is:

- `namespace_id`
- `inode_id`
- `kind`
- `summary`
- `detail_json`
- `created_at_ms`

The temp-identity row shape is:

- `client_file_id`
- `namespace_id`
- `kind`
- `summary`
- `detail_json`
- `created_at_ms`

Rules:

- the inode-keyed implementation keeps at most one latest row per `(namespace_id, inode_id,
  kind)`
- the temp-identity implementation keeps at most one latest row per `(client_file_id, kind)`
- recording the same `kind` again for the same identity must replace the older row instead of
  appending unbounded duplicates
- successful recovery of the same action may clear the matching `kind`
- when a temp identity later binds to a real remote inode, the bind transaction must clear any
  leftover `local_only_conflicts_and_errors(client_file_id)` rows
- these rows are durable debug/user-facing explanations; they do not replace the authoritative
  `remote_state`, `local_state`, `sync_anchor`, or `local_only_state` views

The first issue kinds are:

- `remote_observation_bind_ambiguous`
  - recorded when one authoritative observation matches more than one `local_only_state` row
  - `detail_json` must include at least the observed inode identity plus `matches`
- `upload_local_edit_upload_failed`
  - recorded when inode-keyed `upload_local_edit` cannot prepare durable immutable content before
    dispatch
  - `detail_json` must include at least a failure class such as `source_path_missing`,
    `local_file_read`, `store_write`, or `local_file_changed_during_upload`, plus any available
    path, object-key, block, or digest context
- `upload_local_create_upload_failed`
  - recorded when temp-identity `upload_local_create` cannot prepare durable immutable content
    before dispatch
  - it uses `local_only_conflicts_and_errors(client_file_id)` because no authoritative inode
    exists yet
  - `detail_json` must include at least a failure class such as `source_path_missing`,
    `local_file_read`, `store_write`, or `local_file_changed_during_upload`, plus any available
    path, object-key, block, or digest context
- `download_remote_edit_remote_digest_mismatch`
  - recorded when downloaded durable content does not match the authoritative remote digest
- `download_remote_edit_local_apply_failed`
  - recorded when the file download executor cannot stage or atomically apply local bytes
- `download_remote_edit_transfer_reset`
  - recorded when inode-keyed download resume state is discarded and the executor restarts from
    block `0`
  - `detail_json.reason` must be one of:
    - `stage_size_mismatch`
    - `transfer_id_mismatch`
    - `object_key_mismatch`
    - `block_count_mismatch`
- `upload_local_edit_transfer_reset`
  - recorded when inode-keyed upload resume state is discarded and the executor restarts from
    block `0`
  - `detail_json.reason` must be one of:
    - `transfer_id_mismatch`
    - `object_key_mismatch`
    - `block_count_mismatch`
- `upload_local_create_transfer_reset`
  - recorded when temp-identity upload resume state is discarded and the executor restarts from
    block `0`
  - it uses `local_only_conflicts_and_errors(client_file_id)` because no authoritative inode
    exists yet
  - `detail_json.reason` must be one of:
    - `transfer_id_mismatch`
    - `object_key_mismatch`
    - `block_count_mismatch`
- `materialize_remote_dir_local_apply_failed`
  - recorded when the remote-only directory materializer cannot create the local directory durably

Why this rule exists:

- restart-safe remote discovery is much less useful if failures disappear into transient executor
  errors
- ambiguous bind failures need a durable breadcrumb even when the client intentionally refuses to
  guess
- a local-only create can fail repeatedly before remote inode allocation ever happens, so it needs
  a temp-identity durable issue path rather than only inode-keyed issue rows

Failure modes prevented:

- an inode-keyed local edit upload failing repeatedly with no durable explanation of why bytes are
  not reaching durable content storage
- a local-only file create upload failing repeatedly with no durable explanation before the temp
  identity binds to a real inode
- a remote observation bind ambiguity being lost after restart with no durable evidence
- a discovered remote-only inode failing local materialization repeatedly without any persisted
  explanation

## First durable download transfer-ledger path

The first transfer-ledger slice is for `download_remote_edit`.

It uses the existing SQLite table:

- `transfer_ledger`

The first active download row shape is:

- `namespace_id`
- `inode_id`
- `transfer_id`
- `direction = download`
- `object_key = manifest_object_key`
- `block_index = next block index to download`
- `block_count = total manifest block count`
- `state = staging`
- `updated_at_ms`

Rules:

- the first implementation keeps at most one active download row per `(namespace_id, inode_id)`
- the v1 transfer budget is one content block per executor tick
- `transfer_id` must be deterministic for one remote file revision:
  `download:{namespace_id}:{inode_id}:{content_manifest_digest}`
- before downloading blocks, the executor must load and verify the immutable manifest object
- the executor must stage bytes in the same directory as the target file, using the durable stage
  path derived from the target path
- one executor call may append at most one content block to the staged file
- after one block is written and `sync_all()` succeeds on the stage file, the client must advance
  `transfer_ledger.block_index` durably
- if blocks remain after that ledger advance, the executor must return `Progressed` and leave:
  - `planned_actions(namespace_id, inode_id)` intact
  - the active `transfer_ledger` row intact
  - the stage file intact
- after the final block is staged, the client must verify staged file size and file digest against
  the manifest before rename
- only after full verification may the client atomically rename the staged file into the target
  path, clear the active `transfer_ledger` row, and apply `local_state` / `sync_anchor`
- if the active ledger row and staged file length disagree on restart, the client must reset the
-  staged file and restart from `block_index = 0` rather than guessing partial recovery
- when the client discards an existing staged file or active download row and restarts from block
  `0`, it must record or replace one durable `download_remote_edit_transfer_reset` row
- if the authoritative manifest digest changed, the client may discard the older active download
  row and restart from block `0` for the newer manifest
- a successful terminal completion must clear:
  - `download_remote_edit_transfer_reset`
  - `download_remote_edit_remote_digest_mismatch`
  - `download_remote_edit_local_apply_failed`

Why this rule exists:

- remote-only discovery is only half-useful if larger downloads still depend on one in-memory
  blob fetch
- block-by-block durable progress is the smallest restart-safe transfer protocol that can grow into
  full upload/download resume later

Failure modes prevented:

- restarting a large remote download from byte `0` with no durable proof of prior staged progress
- trusting a staged file whose bytes no longer match the durable ledger cursor
- atomically publishing a local file before the fully staged bytes are verified against the remote
  manifest

## First durable upload transfer-ledger path

The first upload-side transfer-ledger slice is for inode-keyed `upload_local_edit`.

It reuses the same SQLite table:

- `transfer_ledger`

The first active upload row shape is:

- `namespace_id`
- `inode_id`
- `transfer_id`
- `direction = upload`
- `object_key = manifest_object_key`
- `block_index = next block index to upload`
- `block_count = total manifest block count`
- `state = uploading`
- `updated_at_ms`

Rules:

- the first implementation keeps at most one active upload row per `(namespace_id, inode_id)`
- this slice applies only to inode-keyed `upload_local_edit`
- local-only creates use the separate temp-identity keyed `local_only_transfer_ledger` path below
- before uploading blocks, the executor must scan the local source path and derive one deterministic
  content manifest plan for the current local bytes
- the v1 transfer budget is one content block per executor tick
- `transfer_id` must be deterministic for one planned upload:
  `upload:{namespace_id}:{inode_id}:{content_manifest_digest}`
- when an existing upload row matches the newly derived `transfer_id`, `object_key`, and
  `block_count`, the executor may resume from the recorded `block_index`
- when the existing row does not match the newly derived upload plan, the executor must restart
  from `block_index = 0`
- when the client discards an existing upload row and restarts from block `0`, it must record or
  replace one durable `upload_local_edit_transfer_reset` row
- one executor call may upload at most one content block
- after one content block is durably written to object storage, the executor must advance
  `transfer_ledger.block_index` durably
- if the process crashes after a block object write but before the ledger advance, the retry path
  may safely re-upload that block because immutable block keys are content-addressed
- if blocks remain after that ledger advance, the executor must return `Progressed` and leave:
  - `planned_actions(namespace_id, inode_id)` intact
  - the active upload `transfer_ledger` row intact
  - `inode_uploads(namespace_id, inode_id)` absent unless a matching durable upload row already
    existed before this step
- after all content blocks are durable, the executor may write the immutable manifest object
- only after the manifest object exists may the client record `inode_uploads(namespace_id, inode_id)`
  and clear the active upload `transfer_ledger` row
- dispatching `ClientMutationOp::ReplaceFile` remains a later step and may still fail independently
  after upload completion
- a successful terminal completion of the whole `upload_local_edit` path must clear:
  - `upload_local_edit_upload_failed`
  - `upload_local_edit_transfer_reset`

Why this rule exists:

- restart-safe download progress is only half of the transfer story if bound local edits still
  restart from block `0`
- block-by-block upload progress is the smallest durable protocol that can grow into broader
  resumable transfers later

Failure modes prevented:

- restarting a large local edit upload from byte `0` with no durable proof of prior block progress
- depending on one in-memory upload call for inode-keyed file edits
- losing a fully uploaded content manifest because upload completion and manifest publication were
  not named as separate durable steps

## Temp-identity upload transfer ledger (schema v9)

The next upload-side transfer-ledger slice is for local-only file creates that still have only a
temporary `client_file_id`.

It uses one new SQLite table:

- `local_only_transfer_ledger`

The first active temp upload row shape is:

- `client_file_id`
- `namespace_id`
- `transfer_id`
- `direction = upload`
- `object_key = manifest_object_key`
- `block_index = next block index to upload`
- `block_count = total manifest block count`
- `state = uploading`
- `updated_at_ms`

Rules:

- the first implementation keeps at most one active temp upload row per `(client_file_id,
  direction)`
- this slice applies only to local-only `upload_local_create`
- before uploading blocks, the executor must scan the local source path and derive one deterministic
  content manifest plan for the current local bytes
- the v1 transfer budget is one content block per executor tick
- `transfer_id` must be deterministic for one planned temp upload:
  `upload-local-only:{client_file_id}:{content_manifest_digest}`
- when an existing temp upload row matches the newly derived `transfer_id`, `object_key`, and
  `block_count`, the executor may resume from the recorded `block_index`
- when the existing row does not match the newly derived upload plan, the executor must restart
  from `block_index = 0`
- when the client discards an existing temp upload row and restarts from block `0`, it must record
  or replace one durable `upload_local_create_transfer_reset` row
- one executor call may upload at most one content block
- after one content block is durably written to object storage, the executor must advance
  `local_only_transfer_ledger.block_index` durably
- if the process crashes after a block object write but before the ledger advance, the retry path
  may safely re-upload that block because immutable block keys are content-addressed
- if blocks remain after that ledger advance, the executor must return `Progressed` and leave:
  - `planned_local_only_actions(client_file_id)` intact
  - the active `local_only_transfer_ledger` row intact
  - `local_only_uploads(client_file_id)` absent unless a matching durable upload row already
    existed before this step
- after all content blocks are durable, the executor may write the immutable manifest object
- only after the manifest object exists may the client record `local_only_uploads(client_file_id)`
  and clear the active temp upload ledger row
- if a matching `local_only_uploads(client_file_id)` row already exists for the current local
  digest, the executor may reuse it and clear any stale temp upload ledger row
- if the temp identity later binds to a real inode, the bind transaction must clear any leftover
  `local_only_transfer_ledger` row for that `client_file_id`
- a successful terminal completion of the whole `upload_local_create` path must clear:
  - `upload_local_create_upload_failed`
  - `upload_local_create_transfer_reset`

## Executable invariant surface for Milestone 8 slice 5a

The first client-side executable invariant slice is file-transfer-only.

Runtime `checked_invariants` strings remain unchanged for compatibility. The executable proof
surface lives in harness-side reports first.

The file-transfer invariant IDs for this slice are:

- download flow:
  - `download_transfer_block_index_advances_monotonically`
  - `download_transfer_reset_records_durable_issue`
  - `download_completion_clears_transfer_ledger`
  - `download_materialization_updates_local_state_and_sync_anchor`
- inode-keyed upload flow:
  - `inode_upload_block_index_advances_monotonically`
  - `inode_upload_dispatch_waits_for_terminal_block`
  - `inode_upload_retry_reuses_pending_inode_mutation`
  - `inode_upload_completion_clears_transfer_ledger`
  - `inode_upload_transfer_reset_records_durable_issue`
- temp-identity upload flow:
  - `local_only_upload_block_index_advances_monotonically`
  - `local_only_upload_dispatch_waits_for_terminal_block`
  - `local_only_upload_retry_reuses_pending_client_mutation`
  - `local_only_upload_completion_clears_temp_transfer_ledger`
  - `local_only_upload_bind_clears_temp_issue_and_transfer_ledger`
  - `local_only_upload_transfer_reset_records_durable_issue`

Remote rename/delete reconciliation remains out of scope for this first client slice.

## Executable invariant surface for Milestone 8 slice 5b

The next client executable-invariant slice broadens from file transfers into observation-driven
reconciliation, still without widening to remote rename/delete.

Runtime `checked_invariants` strings remain unchanged for compatibility. The proof surface stays in
harness-side reports first.

The client reconciliation invariant IDs for this slice are:

- bound-file convergence:
  - `remote_observation_convergence_clears_dirty_and_planned_action`
  - `remote_observation_convergence_clears_pending_inode_mutation`
  - `remote_observation_convergence_advances_sync_anchor`
- late local-only bind:
  - `remote_observation_late_bind_establishes_remote_local_and_anchor`
  - `remote_observation_late_bind_clears_temp_local_state`
  - `remote_observation_late_bind_clears_temp_transfer_and_issue_rows`
  - `remote_observation_late_bind_retains_pending_client_mutation_until_response`
- ambiguous bind:
  - `remote_observation_ambiguous_bind_records_durable_issue`
  - `remote_observation_ambiguous_bind_avoids_partial_migration`
- late observations while file transfers are active:
  - `remote_observation_active_upload_preserves_transfer_and_pending_inode_mutation`
  - `remote_observation_active_download_preserves_transfer_ledger`
- remote-only discovery/materialization:
  - `remote_only_file_discovery_creates_placeholder_without_anchor`
  - `remote_only_directory_discovery_creates_placeholder_without_anchor`
  - `remote_only_directory_materialization_updates_local_state_and_sync_anchor`
  - `remote_only_directory_materialization_clears_planned_action`
  - `remote_only_directory_materialization_failure_records_durable_issue`

Remote rename/delete reconciliation remained out of scope for Milestone 8.

## First bound-file remote rename observation path

The first remote hierarchy reconciliation slice is bound-file-only.

Rules:

- `apply_remote_observation` remains a durable metadata update; it must not move local files
  directly
- a later planner pass may schedule one new inode-keyed decision:
  - `apply_remote_rename`
- `apply_remote_rename` is valid only when:
  - `remote_state(namespace_id, inode_id)` exists
  - `local_state(namespace_id, inode_id)` exists
  - `sync_anchor(namespace_id, inode_id)` exists
  - all three rows have `inode_kind = file`
  - `local_state` still matches `sync_anchor` on:
    - `content_digest`
    - `parent_inode_id`
    - `display_name`
    - `exists_on_disk = true`
    - `dirty = false`
  - `remote_state` still matches `sync_anchor` on content identity:
    - `revision_no`
    - `content_digest`
    - `content_manifest_digest`
  - `remote_state.is_deleted = false`
  - `remote_state` differs from `sync_anchor` only on:
    - `parent_inode_id`
    - `display_name`
- if `remote_state` differs from `sync_anchor` on both path view and content identity while
  `local_state` still matches the anchor, the planner must fall back to deferred
  `create_conflict_copy` with reason `remote_path_and_content_differ_from_anchor`
- the first slice covers:
  - same-parent rename
  - moves between already-bound directories
- the first slice still excludes:
  - remote directory rename reconciliation
  - remote delete reconciliation
  - compound remote path + content application in one executor call

Planner surface additions:

- planner decision:
  - `apply_remote_rename`
- planner reasons:
  - `remote_path_differs_from_anchor`
  - `remote_path_and_content_differ_from_anchor`

The rename executor takes:

- `namespace_id`
- `inode_id`
- one caller-supplied resolver for the current bound local path
- one caller-supplied resolver for the desired target path view
- one explicit local apply timestamp

Rules:

- the mixed tick must treat `apply_remote_rename` as an executable inode action
- the executor must load `planned_actions(namespace_id, inode_id)` and require
  `decision = apply_remote_rename`
- the executor must require the bound-file rename preconditions above at execution time, not only
  at planning time
- the executor must require the current local path to resolve successfully
- the executor must require the desired target path to resolve successfully from:
  - `namespace_id`
  - `inode_id`
  - current authoritative `remote_state.parent_inode_id`
  - current authoritative `remote_state.display_name`
- if the target path already exists locally, the executor must fail closed and record one durable
  issue row with `kind = apply_remote_rename_local_apply_failed`
- after a successful durable local rename, one SQLite transaction must:
  - update `local_state.parent_inode_id` and `local_state.display_name` from `remote_state`
  - preserve `local_state.content_digest`
  - preserve `exists_on_disk = true`
  - preserve `dirty = false`
  - advance `sync_anchor` to the current authoritative remote seq/revision/path view
  - clear `planned_actions(namespace_id, inode_id)`
  - clear any matching `apply_remote_rename_local_apply_failed` row

The local apply step must:

- move the file with one filesystem rename from current path to target path
- sync the destination parent directory
- when the source and destination parents differ, sync the source parent directory too

The first issue kind for this path is:

- `apply_remote_rename_local_apply_failed`
  - recorded for destination collisions and path-resolution/local-apply failures
  - `detail_json.failure` must be one of:
    - `current_path_missing`
    - `target_path_missing`
    - `destination_occupied`
    - `rename_io`

Why this rule exists:

- authoritative remote path changes are real state changes, not generic content drift
- the client already stores path view durably on inode-keyed rows, so the first hierarchy slice
  should reconcile those path changes explicitly instead of deferring forever

Failure modes prevented:

- downloading or uploading bytes only to leave a bound file permanently at the wrong local path
- silently overwriting an occupied local destination slot during authoritative rename apply
- treating a remote rename-plus-content change as if it were safe to apply through a rename-only
  executor

Executable invariant IDs for this slice:

- `remote_path_change_plans_apply_remote_rename`
- `apply_remote_rename_updates_local_state_and_sync_anchor`
- `apply_remote_rename_clears_planned_action`
- `apply_remote_rename_failure_records_durable_issue`

## First bound-file remote delete observation path

The next remote hierarchy reconciliation slice is also bound-file-only.

Rules:

- `apply_remote_observation` remains a durable metadata update; it must not unlink local files
  directly
- a later planner pass may schedule one new inode-keyed decision:
  - `apply_remote_delete`
- `apply_remote_delete` is valid only when:
  - `remote_state(namespace_id, inode_id)` exists
  - `local_state(namespace_id, inode_id)` exists
  - `sync_anchor(namespace_id, inode_id)` exists
  - all three rows have `inode_kind = file`
  - `remote_state.is_deleted = true`
  - `local_state` still matches `sync_anchor` on:
    - `content_digest`
    - `parent_inode_id`
    - `display_name`
    - `exists_on_disk = true`
    - `dirty = false`
- if `remote_state.is_deleted = true` while `local_state` no longer matches the anchor, the
  planner must fall back to deferred `create_conflict_copy` with reason
  `remote_deleted_while_local_differs_from_anchor`
- if the durable state is a tombstoned remote row without local/anchor, the planner must return
  `no_op` with reason `remote_deleted_without_anchor`
- while upload/download work is still in flight for the inode, replanning must preserve the current
  executable `upload_local_edit` or `download_remote_edit` action instead of replacing it with
  delete reconciliation
- the first slice still excludes:
  - remote directory delete reconciliation
  - remote subtree delete reconciliation

Planner surface additions:

- planner decision:
  - `apply_remote_delete`
- planner reasons:
  - `remote_deleted_from_anchor`
  - `remote_deleted_while_local_differs_from_anchor`
  - `remote_deleted_without_anchor`

The delete executor takes:

- `namespace_id`
- `inode_id`
- one caller-supplied resolver for the current bound local path
- one explicit local apply timestamp

Rules:

- the mixed tick must treat `apply_remote_delete` as an executable inode action
- the executor must load `planned_actions(namespace_id, inode_id)` and require
  `decision = apply_remote_delete`
- the executor must require the bound-file delete preconditions above at execution time, not only
  at planning time
- the executor must require the current local path to resolve successfully
- if the current path is missing locally, the executor must fail closed and record one durable
  issue row with `kind = apply_remote_delete_local_apply_failed`
- after a successful durable local unlink, one SQLite transaction must:
  - preserve `remote_state(namespace_id, inode_id)` with `is_deleted = true`
  - delete `local_state(namespace_id, inode_id)`
  - delete `sync_anchor(namespace_id, inode_id)`
  - clear `planned_actions(namespace_id, inode_id)`
  - clear any matching `apply_remote_delete_local_apply_failed` row

The local apply step must:

- unlink the current file path
- sync the parent directory after the unlink

The first issue kind for this path is:

- `apply_remote_delete_local_apply_failed`
  - recorded for current-path resolution failures and local unlink failures
  - `detail_json.failure` must be one of:
    - `current_path_missing`
    - `unlink_io`

Why this rule exists:

- a tombstoned authoritative file observation is a real hierarchy change, not just content drift
- the client already stores bound file existence and path view durably, so delete should become a
  first-class reconciliation path before broader subtree work begins
- keeping the remote tombstone while clearing local/anchor rows lets later planning distinguish
  “successfully applied delete” from “never observed this inode”

Failure modes prevented:

- preserving a locally present file after the remote inode is authoritatively tombstoned
- replanning a successfully deleted file as a fresh download because only the tombstone remained
- collapsing local unlink failures into generic executor errors without durable explanation

Executable invariant IDs for this slice:

- `remote_delete_plans_apply_remote_delete`
- `apply_remote_delete_preserves_remote_tombstone`
- `apply_remote_delete_clears_local_state_and_sync_anchor`
- `apply_remote_delete_clears_planned_action`
- `apply_remote_delete_failure_records_durable_issue`

## First bound-directory remote subtree delete observation path

The next remote hierarchy reconciliation slice is bound-directory-only.

Rules:

- `apply_remote_observation` remains a durable metadata update; it must not remove local
  directories directly
- a later planner pass may schedule one new inode-keyed decision:
  - `apply_remote_subtree_delete`
- `apply_remote_subtree_delete` is valid only when:
  - `remote_state(namespace_id, inode_id)` exists
  - `local_state(namespace_id, inode_id)` exists
  - `sync_anchor(namespace_id, inode_id)` exists
  - the root rows all have `inode_kind = dir`
  - `remote_state.is_deleted = true`
  - the full rooted durable local subtree is already bound and locally converged
- the first subtree-delete slice defines "locally converged" strictly for every rooted descendant
  that still exists in durable client state:
  - one matching `local_state(namespace_id, inode_id)` row exists
  - one matching `sync_anchor(namespace_id, inode_id)` row exists
  - `exists_on_disk = true`
  - `dirty = false`
  - `inode_kind`, `parent_inode_id`, and `display_name` still match the anchor
  - for files, `content_digest` still matches the anchor
- if the root is tombstoned but the durable state is `(remote tombstone, no local, no anchor)`,
  the planner must return `no_op` with reason `remote_subtree_deleted_without_anchor`
- if any rooted descendant is dirty, missing its anchor, is only a remote-only placeholder, or is
  a temp/local-only inode, the planner must fall back to deferred `create_conflict_copy` with
  reason `remote_subtree_deleted_while_descendants_differ_from_anchor`
- if any rooted descendant has transfer-ledger or pending-mutation state, the planner must fall
  back to deferred `create_conflict_copy` with reason
  `remote_subtree_deleted_while_descendants_busy`
- if the root itself no longer matches the anchor while the remote row is tombstoned, the planner
  must fall back to deferred `create_conflict_copy` with reason
  `remote_subtree_deleted_while_local_differs_from_anchor`
- the first slice still excludes:
  - remote directory rename reconciliation
  - mixed subtree delete with descendant uploads/downloads still in flight

Planner surface additions:

- planner decision:
  - `apply_remote_subtree_delete`
- planner reasons:
  - `remote_subtree_deleted_from_anchor`
  - `remote_subtree_deleted_while_local_differs_from_anchor`
  - `remote_subtree_deleted_while_descendants_differ_from_anchor`
  - `remote_subtree_deleted_while_descendants_busy`
  - `remote_subtree_deleted_without_anchor`

The subtree-delete executor takes:

- `namespace_id`
- `inode_id`
- one caller-supplied resolver for the current bound local root path
- one explicit local apply timestamp

Rules:

- the mixed tick must treat `apply_remote_subtree_delete` as an executable inode action
- the executor must load `planned_actions(namespace_id, inode_id)` and require
  `decision = apply_remote_subtree_delete`
- the executor must require the bound-directory subtree-delete preconditions above at execution
  time, not only at planning time
- the executor must resolve the current root local path successfully
- if the current root path is missing locally, the executor must fail closed and record one
  durable issue row with `kind = apply_remote_subtree_delete_local_apply_failed`
- after a successful durable local recursive remove, one SQLite transaction must:
  - preserve `remote_state(namespace_id, inode_id)` for the root with `is_deleted = true`
  - delete descendant `remote_state` rows in the rooted subtree
  - delete `local_state` rows for the full rooted subtree, including the root
  - delete `sync_anchor` rows for the full rooted subtree, including the root
  - clear `planned_actions` rows for the full rooted subtree
  - clear any matching `apply_remote_subtree_delete_local_apply_failed` row on the root

The local apply step must:

- recursively remove the root directory tree
- sync the root parent directory after the removal

The first issue kind for this path is:

- `apply_remote_subtree_delete_local_apply_failed`
  - recorded for root-path resolution failures and recursive local-remove failures
  - `detail_json.failure` must be one of:
    - `current_path_missing`
    - `recursive_remove_io`

Why this rule exists:

- authoritative remote directory tombstones are real hierarchy changes, not generic content drift
- subtree delete is simpler than directory rename because it does not require descendant path
  rewrites, but it still needs an explicit durable state shape
- preserving only the root tombstone while clearing descendant rows keeps later planning simpler
  than synthesizing one tombstone per removed descendant

Failure modes prevented:

- leaving a bound local subtree present after the authoritative remote directory was deleted
- inventing descendant tombstones that were never actually observed remotely
- silently canceling descendant transfer or pending-mutation work instead of surfacing the
  conflict/defer decision durably

Executable invariant IDs for this slice:

- `remote_subtree_delete_plans_apply_remote_subtree_delete`
- `apply_remote_subtree_delete_preserves_root_remote_tombstone`
- `apply_remote_subtree_delete_clears_descendant_remote_rows`
- `apply_remote_subtree_delete_clears_local_state_and_sync_anchor_for_subtree`
- `apply_remote_subtree_delete_clears_subtree_planned_actions`
- `apply_remote_subtree_delete_failure_records_durable_issue`

## SQLite hardening boundary (schema v11)

The next schema version does not add new client-truth tables. It hardens the existing durable
shape into a stricter correctness boundary.

Rules:

- stable enum-like `TEXT` columns use SQLite `CHECK` constraints for:
  - `inode_kind`
  - planner `decision`
  - planner `reason`
  - transfer `direction`
  - transfer `state`
  - repo-defined issue `kind`
- inode-keyed adjunct tables reference `local_state(namespace_id, inode_id)`
- temp-identity adjunct tables reference `local_only_state(client_file_id)`
- explicit read-path indexes exist for planned actions, transfer ledgers, and issue tables
- migration coverage must prove every historical schema version upgrades cleanly to the latest one

Why it exists:

- the client database is durable protocol state, not a permissive cache

Failure modes prevented:

- invalid planner or transfer rows being inserted silently and failing later in Rust decode paths
- orphaned ledger or issue rows surviving after their owning local state has been removed
- schema upgrades only being tested from empty databases instead of real historical versions

## File-focused late authoritative observations during transfer work

The first late-authoritative hardening slice stays file-focused.

Rules:

- later authoritative file observations may always advance `remote_state`
- later authoritative file observations must not silently delete:
  - `transfer_ledger(namespace_id, inode_id)`
  - `local_only_transfer_ledger(client_file_id)`
  - `pending_inode_mutations(client_request_id)`
  - `pending_client_mutations(client_request_id)`
- if an inode-keyed transfer is still active for `(namespace_id, inode_id)`, the apply step may
  update `remote_state` but must leave `local_state`, `sync_anchor`, `planned_actions`, and the
  active transfer row for the next executor tick to interpret
- if a local-only temp identity already matches a later authoritative create observation, the
  client may bind that temp identity into inode-keyed state before the immediate success response
  arrives, but it must not silently delete the frozen `pending_client_mutations` row for that
  request
- if a bound inode already converges to a later authoritative observation while a matching
  `pending_inode_mutations` row still exists, the client must clear that matching pending row as
  part of authoritative convergence
- if a later `replace_file` success response arrives after that authoritative convergence already
  cleared the pending inode mutation, response application must accept it as an idempotent no-op
  only when `remote_state`, `local_state`, and `sync_anchor` already match the committed
  replacement state; otherwise missing-pending remains an error
- late local-only bind intentionally does not clear `pending_client_mutations` in this slice;
  create-response cleanup remains deferred until the create-response API is widened

Why this rule exists:

- transfer or request execution can outlive the exact observation that later proves convergence
- a late authoritative observation should improve the durable remote view without erasing the
  client's evidence of in-flight work

Failure modes prevented:

- a later authoritative observation silently discarding an active staged download
- a later authoritative observation silently discarding an in-flight upload request with no durable
  response application
- a temp-id create binding to a real inode and losing the durable frozen request that still needs
  a response or reconciliation path

Why this rule exists:

- restart-safe inode uploads are only half of the transfer story if local-only file creates still
  restart from block `0`
- temp identities need their own durable transfer namespace before a remote inode exists
- upload completion for local-only creates should stay durable before request dispatch, just like
  inode-keyed uploads

Failure modes prevented:

- restarting a large local-only file create upload from byte `0` with no durable proof of prior
  block progress
- coupling temp-identity upload resume to inode-keyed tables that do not exist yet
- leaving stale temp upload progress behind after a temp identity is already bound or a matching
  durable upload row already exists

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
