# Spec 030: namespace metadata model

## Namespace

A namespace is the unit of serialized metadata history.

Why it exists:
we want a clean boundary for ordering, caching, and background work.

Example:
an account may have a home namespace, a shared project namespace, and several agent namespaces.

Failure mode prevented:
forcing unrelated trees into one giant global transaction log.

## Canonical identity

The canonical identity of an item is `(namespace_id, inode_id)`.

Why it exists:
paths change; identity should not.

Example:
if `/Projects/A/logo.png` is renamed to `/Brand/logo.png`, it keeps the same inode.

Failure mode prevented:
rename being mis-modeled as delete+add.

## Inode kinds

Supported kinds:

- `FILE`
- `DIR`
- `SYMLINK`
- `MOUNT`

## Authoritative metadata records

- inode record
- direntry record
- revision record
- content manifest reference
- subtree tombstone record

The important split is:

- inode identity and history are durable metadata
- directory entries are name bindings
- paths are rebuilt by walking bindings

## Logical metadata families

The canonical metadata engine must be able to reconstruct four logical families at any committed
`head.seq`.

Physical WAL bodies and checkpoint rows may encode these families differently. That is an
implementation detail. Replay must still answer the same logical queries.

### Inode family

Logical key:

- `(namespace_id, inode_id)`

Minimum semantics carried by this family:

- the inode exists in the namespace history
- the inode kind is fixed for that inode id
- the inode has a creation seq
- kind-specific payload may exist later for `SYMLINK` or `MOUNT`

Why it exists:

- path bindings and revision history should not have to guess whether an inode exists or what kind
  it is

Failure mode prevented:

- evaluating a rename, replace, or delete against an inode that only exists narratively, not in
  canonical metadata state

### Revision family

Logical key:

- `(namespace_id, inode_id, revision_no)`

Minimum semantics carried by this family:

- each committed file revision is immutable
- `revision_no` is monotonic per inode
- each revision records the commit seq that made it visible
- file revisions carry the authoritative `content_manifest_digest`
- restore semantics may point back at an earlier revision's content while still creating a new
  `revision_no`

Why it exists:

- file history and restore behavior must be reconstructible from durable metadata alone

Failure mode prevented:

- a visible file head that cannot explain which immutable content manifest it represents

### Direntry family

Logical key:

- `(namespace_id, parent_inode_id, name_key, bind_seq)`

Minimum semantics carried by this family:

- a direntry binds one `child_inode_id` under one parent/name pair
- `display_name` is preserved exactly for presentation
- `name_key` is compared by the shared versioned `NamePolicy`
- later rows under the same `(parent_inode_id, name_key)` may supersede older bindings
- replay can determine which binding is visible at any committed seq

Why it exists:

- rename and sibling-collision semantics are name-binding rules, not inode-identity rules

Failure mode prevented:

- treating sibling-name checks as ambient path lookups instead of as canonical metadata queries

### Subtree tombstone family

Logical key:

- `(namespace_id, root_inode_id, tombstone_seq)`

Minimum semantics carried by this family:

- a subtree tombstone covers one deleted directory root and every descendant inode reachable from it
- the tombstone becomes active at `tombstone_seq`
- later restore semantics may make the root visible again, but the delete event remains in history
- replay can determine whether a given inode is covered by an active tombstone at a chosen seq

Why it exists:

- recursive delete should be cheap without destroying history needed for restore and audit

Failure mode prevented:

- deleting a subtree by eagerly erasing descendants and making restore or history replay ambiguous

## Derived visibility queries

The first semantic-core implementation does not need to lock one final checkpoint row encoding, but
it must answer these derived queries deterministically at a chosen `base_seq`.

It must also answer the corresponding raw history lookups that do not hide rows behind subtree
tombstones. Commit-precondition evaluation uses those raw lookups first, then evaluates tombstone
coverage explicitly.

### `inode_at_seq(namespace_id, inode_id, base_seq)`

Returns the inode row only if:

- the inode exists in the inode family
- the inode's creation seq is at or before `base_seq`

This is the raw existence/kind lookup. It does not hide the inode because of a subtree tombstone.

### `visible_inode(namespace_id, inode_id, base_seq)`

Returns the inode row only if:

- the inode exists in the inode family
- the inode's creation seq is at or before `base_seq`
- the inode is not covered by an active subtree tombstone rooted at itself or any visible ancestor

### `current_revision_head(namespace_id, inode_id, base_seq)`

Returns the visible revision head only if:

- `visible_inode(...)` exists
- at least one revision row for that inode has `committed_seq <= base_seq`

The visible head is the highest `revision_no` satisfying those rules.

### `latest_revision_head_at_seq(namespace_id, inode_id, base_seq)`

Returns the highest `revision_no` with `committed_seq <= base_seq`, without hiding the result
because of subtree-tombstone coverage.

### `visible_child(namespace_id, parent_inode_id, name_key, base_seq)`

Returns the active direntry binding only if:

- `visible_inode(namespace_id, parent_inode_id, base_seq)` exists
- the parent inode kind is `DIR`
- at least one direntry row for `(parent_inode_id, name_key)` is bound at or before `base_seq`
- no later binding for the same `(parent_inode_id, name_key)` supersedes it by `base_seq`
- the bound child inode is still visible at `base_seq`

### `bound_child_at_seq(namespace_id, parent_inode_id, name_key, base_seq)`

Returns the latest direntry binding for `(parent_inode_id, name_key)` at or before `base_seq`,
without hiding the result because the parent or child is later covered by a subtree tombstone.

### `active_subtree_tombstone(namespace_id, root_inode_id, base_seq)`

Returns the latest active tombstone rooted at `root_inode_id` whose `tombstone_seq <= base_seq`.

The exact clear/restore encoding may evolve, but replay must still answer the yes/no coverage
question deterministically.

## First metadata lookups the semantic core must support

Before the commit path can claim correctness for inode-keyed mutations, the metadata engine must be
able to answer at minimum:

- does inode `X` exist and what kind is it at `base_seq`?
- what is inode `X`'s current visible revision at `base_seq`?
- is `(parent_inode_id, name_key)` already occupied at `base_seq`?
- is inode `X`, or any ancestor on its visible parent chain, covered by an active subtree tombstone
  at `base_seq`?

For commit-precondition evaluation specifically, the engine must also be able to answer the raw
lookups before tombstone coverage is applied:

- `inode_at_seq(...)`
- `latest_revision_head_at_seq(...)`
- `bound_child_at_seq(...)`

These lookups are the semantic center for:

- `ChildNameAbsent`
- `InodeRevisionIs`
- `AncestorsNotSubtreeDeleted`

Until those lookups are backed by canonical metadata state, commit validation is still only a frame
validator rather than a metadata engine.

## Multiple namespaces per account

Accounts can own more than one namespace.

The home namespace exposes other namespaces through `MOUNT` inodes.

This lets us support:

- user workspaces
- shared project spaces
- agent sandboxes
- selective sharing of a subtree instead of an entire namespace

## Cross-namespace move rule

Cross-namespace moves are not atomic by default.
They are modeled as copy + delete.

Why the rule exists:
it keeps the namespace ordering model simple.
