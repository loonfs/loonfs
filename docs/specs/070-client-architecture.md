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

- `remote_state`: latest observed `seq`, `revision_no`, content digest, and current observed path view
- `local_state`: latest observed local content digest, current observed path view, `dirty`, and local observation time
- `sync_anchor`: last fully synced remote revision and content digest
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
