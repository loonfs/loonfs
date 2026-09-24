# Directory bindings

A directory slot is a name within a parent, identified by `(parent_inode_id, name_key)`. Each change stores a bound or unbound value in two indexes. Readers select the newest visible value. Compaction processes each index independently under one retention rule.

The [storage format](../specs/format.md#13-directory-bindings) defines the records, keys, and visibility rules. The [API](../specs/api.md#51-commit-identity-and-preconditions) defines binding tokens and preconditions.

## Records and indexes

A `direntry_binding` row contains `parent_inode_id`, `name_key`, `child_inode_id`, `committed_seq`, `delta_index`, and `state`. A bound state is `{"kind":"bound","display_name":...}`. An unbound state is `{"kind":"unbound"}`. `DeltaPosition` groups a sequence and delta index for comparisons and references to another event.

| Family | Order | Filter key |
| --- | --- | --- |
| `direntry_binds` | Parent, name, committed sequence, delta index | Parent and name |
| `direntry_child_binds` | Child, committed sequence, delta index, parent, name | Child |

The two indexes contain identical records. Positions sort oldest first within a slot or child. Each bind and unbind writes one record to each index. The unbind WAL delta identifies the exact bound event it retires. Commit validation checks that reference before materialization, so the unbound row needs only its own position.

## Reads

At sequence `N`, a slot's value is its greatest `(committed_seq, delta_index)` at or below `N`. A bound value identifies its child. An unbound value, or no value, makes the name available. Parent lookup follows the same rule in the child index. Within one commit the last delta wins.

Commit validation enforces one child per slot and one parent per child. A move unbinds the old slot before binding the new slot. This keeps the indexes consistent at every readable sequence.

A path lookup reads the slot index once per component. A listing scans the parent's binding prefix, selects one visible value per name, and includes bound entries. Inode visibility, covering subtree tombstones, and permissions still apply. Neither operation joins a binding with a separate removal family. Snapshots and checkpoints use their pinned manifests and captured sequences.

## Retention

An unbound value is a tombstone for older values of its slot or child. It remains while those older values can still be read from an excluded run. A rebuild that excludes the group's oldest run keeps every row.

A bottom-anchored rebuild includes the oldest run. It retains all versions above the floor and the newest version at or below it. If that floor value is unbound, it removes the value together with every older version it hides. The sorted stream requires at most one buffered floor value per slot or child.

For example, inode 7 moves from `/a` to `/b` at sequence 20. With the floor at 20, a bottom-anchored rebuild produces:

| Index entry | Value at the floor | Retained rows |
| --- | --- | --- |
| Slot `/a` | Unbound | None |
| Slot `/b` | Bound to inode 7 | The bound value |
| Child 7 | Bound at `/b` | The same bound value |

Every event above the floor remains in both indexes. At the floor, a bound value is current in both indexes or neither, because replacing a child or moving it requires an unbind. Both indexes retain the same event set. The existing row-count and digest checks verify that agreement.

Attribute and access revisions retain a cleared floor value because its revision number is needed by the next update. A binding position comes from the publishing event, so a future bind does not depend on a retained unbound value.

## Deletion, forks, and API behavior

Subtree deletion unbinds the deleted root and records a subtree tombstone. Descendants remain bound beneath that root and are hidden by the covering-tombstone rule. Undelete binds the root using the tombstone's saved name or the caller's destination. Reusing a name writes a newer bound value.

Fork rebuilds apply the same retention rule to inherited and local runs. Inherited rows retain their owners. Pins continue to protect their captured manifests and runs.

WAL deltas, semantic fingerprints, and change-feed events keep their existing shapes. The API's `binding_generation` token represents the bound event's position. Creating, moving, and undeleting an entry changes that token. Content and attribute writes do not. Binding preconditions compare the same positions as reads return.

## Costs and alternatives

An unbind writes two rows. A bottom-anchored rebuild can remove those values and their hidden history. In return, reads need no removal join, and both bounded and streaming compaction process one index at a time without collecting removals or making point reads for them.

| Alternative | Cost |
| --- | --- |
| Collect removal identities before compacting the child index | Keeps joins in path reads, listings, and retention, and memory grows with the input. |
| Keep unbound floor values during bottom-anchored rebuilds | Retains abandoned slots and leaves different event sets in the indexes after a move. |
| Repeat the retired position on an unbound value | Repeats a reference already checked during commit validation. |
| Remove the child index | Parent lookup, deletion checks, and inode-addressed moves require slot scans. |

The durable format version is 1. Stores using a different binding encoding must be recreated; decoding has no fallback.
