# Directory bindings

A directory slot is a name within a parent, identified by `(parent_inode_id, name_key)`. Each change to a slot writes a bound or unbound value to two indexes. A read selects the newest visible value. Compaction processes each index on its own, under one retention rule.

The [storage format](../specs/format.md#13-directory-bindings) defines the records, keys, and visibility rules. The [API](../specs/api.md#51-commit-identity-and-preconditions) defines binding tokens and preconditions.

## Records and indexes

A `direntry_binding` row contains `parent_inode_id`, `name_key`, `child_inode_id`, `committed_seq`, `delta_index`, and `state`. A bound state is `{"kind":"bound","display_name":...}`. An unbound state is `{"kind":"unbound"}`. A `DeltaPosition` pairs a sequence with a delta index. Position comparisons and references to another event use this pair.

| Family | Order | Filter key |
| --- | --- | --- |
| `direntry_binds` | Parent, name, committed sequence, delta index | Parent and name |
| `direntry_child_binds` | Child, committed sequence, delta index, parent, name | Child |

The two indexes contain the same records. Within a slot or child, positions sort oldest first. Each bind and each unbind writes one record to each index. The unbind delta in the WAL names the exact bind it removes. Commit validation checks that reference before materialization, so the unbound row needs to store only its own position.

## Reads

At sequence `N`, a slot's value is the version with the greatest `(committed_seq, delta_index)` at or below `N`. A bound value names its child. If the value is unbound, or the slot has no value, the name is available. Parent lookup follows the same rule in the child index. Within one commit, the last delta wins.

Commit validation allows at most one child per slot and at most one parent per child. A move unbinds the old slot before it binds the new slot. The two indexes therefore agree at every readable sequence.

A path lookup reads the slot index once per component. A listing scans the parent's binding prefix, selects one visible value for each name, and includes the bound entries. Inode visibility and covering subtree tombstones also apply. Neither operation joins bindings with a separate removal family. Snapshots and checkpoints read through their pinned manifests at their captured sequences.

## Retention

An unbound value is a tombstone for older values of its slot or child. It must remain while an excluded run may still hold those older values. A rebuild that excludes the group's oldest run keeps every row.

A bottom-anchored rebuild includes the oldest run. It keeps every version above the floor and the newest version at or below the floor. If that floor value is unbound, the rebuild removes it together with every older version it hides. Because the input is sorted, the rebuild buffers at most one floor value per slot or child.

For example, suppose inode 7 moves from `/a` to `/b` at sequence 20 and the floor is at 20. A bottom-anchored rebuild produces:

| Index entry | Value at the floor | Retained rows |
| --- | --- | --- |
| Slot `/a` | Unbound | None |
| Slot `/b` | Bound to inode 7 | The bound value |
| Child 7 | Bound at `/b` | The same bound value |

Every event above the floor stays in both indexes. At the floor, a bound value is current in both indexes or in neither, because replacing a child or moving it requires an unbind. Both indexes therefore keep the same set of events. The row-count and digest checks verify that agreement.

Attribute and access revisions keep a cleared floor value, because the next update needs its revision number. A binding position comes from the event that published it, so a later bind does not depend on a retained unbound value.

## Deletion, forks, and API behavior

Deleting a subtree unbinds its root and records a subtree tombstone. The descendants stay bound beneath that root, and the covering-tombstone rule hides them. Undelete binds the root again, at the name saved in the tombstone or at the caller's destination. Reusing a name writes a newer bound value.

A rebuild in a fork applies the same retention rule to inherited and local runs. Inherited segments and content references keep their owner namespace. Pins still protect their captured manifests and runs.

WAL deltas, semantic fingerprints, and change-feed events do not depend on the binding row encoding. The API's `binding_version` token represents the position of the bound event. Creating, moving, or undeleting an entry changes the token. Content and attribute writes do not. Binding preconditions compare the same positions that reads return.

## Costs and alternatives

An unbind writes two rows. A bottom-anchored rebuild can remove those rows and the history they hide. Reads need no join against removals. Bounded and streaming compaction both process one index at a time, and neither collects removals or makes point reads for them.

| Alternative | Cost |
| --- | --- |
| Collect removal identities before compacting the child index | Keeps joins in path reads, listings, and retention, and memory grows with the input. |
| Keep unbound floor values during bottom-anchored rebuilds | Retains abandoned slots and leaves different event sets in the indexes after a move. |
| Repeat the retired position on an unbound value | Repeats a reference already checked during commit validation. |
| Remove the child index | Parent lookup, deletion checks, and inode-addressed moves require slot scans. |

The durable format version is 1. A store written with a different binding encoding must be recreated, because decoding has no fallback.
