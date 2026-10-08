# Directory bindings

A directory slot is a name within a parent, identified by `(parent_inode_id, name_key)`. Each change to a slot writes a bound or unbound value to two indexes. A read selects the newest visible value. Compaction processes each index on its own, under one retention rule.

The [storage format](../specs/format.md#13-directory-bindings) defines the records, keys, and visibility rules. The [API](../specs/api.md#51-commit-identity-and-preconditions) defines binding tokens and preconditions.

## Records and indexes

A `direntry_binding` row contains `parent_inode_id`, `name_key`, `child_inode_id`, `committed_seq`, `delta_index`, and `state`. A bound state is `{"kind":"bound","display_name":...}`. An unbound state is `{"kind":"unbound"}`. A `DeltaPosition` pairs a sequence with a delta index. Position comparisons and references to another event use this pair.

| Family | Order | Filter key |
| --- | --- | --- |
| `direntry_binds` | Parent, name, child, committed sequence, delta index | Parent and name |
| `direntry_child_binds` | Child, parent, name, committed sequence, delta index | Child |

The two indexes contain the same records: the same edge events in two orders. An edge is a parent, a name, and a child. Within one edge, positions sort oldest first in both indexes. Each bind and each unbind writes one record to each index. The unbind delta in the WAL names the exact bind it removes. Commit validation checks that reference before materialization, so the unbound row needs to store only its own position.

## Reads

At sequence `N`, a slot's value is the version with the greatest `(committed_seq, delta_index)` at or below `N`. A bound value names its child. If the value is unbound, or the slot has no value, the name is available. Parent lookup follows the same rule in the child index. Within one commit, the last delta wins. Key order is not position order: a slot's rows sort by child first, and a child's rows by parent and name first. A read therefore compares positions and never takes the last row in key order.

One child per slot and one parent per child are validated rules. Commit validation refuses a bind into a slot that holds another child, and a bind of a child that already has a binding, unless the same operation unbinds that binding first. A move unbinds the old slot before it binds the new slot, and an undelete binds a deletion root, which has no binding. The two indexes therefore agree at every readable sequence.

A path lookup reads the slot index once per component. A listing scans the parent's binding prefix, selects one visible value for each name, and includes the bound entries. Inode visibility and covering subtree tombstones also apply. Neither operation joins bindings with a separate removal family. Snapshots and checkpoints read through their pinned manifests at their captured sequences.

## Retention

An unbound value is a tombstone for older values of its edge. It must remain while an excluded run may still hold those older values. A compaction that excludes the group's oldest run keeps every row.

A bottom-anchored compaction includes the oldest run. It groups both indexes by edge. For each edge it keeps every version above the floor and the newest version at or below the floor. If that floor value is unbound, the compaction removes it together with every older version of the edge. A child that left a slot by the floor has an unbind as its newest floor value there, so the compaction removes all of that edge's rows at or below the floor. Because the input is sorted, the compaction buffers at most one floor value per edge. It also carries the last edge bound at the floor: across the edges of one slot in the slot index, and across the edges of one child in the child index. It refuses a second child bound in one slot at the floor, or a second parent bound for one child at the floor, as corruption.

For example, suppose inode 7 moves from `/a` to `/b` at sequence 20 and the floor is at 20. A bottom-anchored compaction produces:

| Edge | Value at the floor | Retained rows in each index |
| --- | --- | --- |
| `/a` to inode 7 | Unbound | None |
| `/b` to inode 7 | Bound | The bound value |

Every event above the floor stays in both indexes. Both indexes group by edge, so each edge's rows get the same decision in each index, and both indexes keep the same set of events. The row-count and digest checks verify that agreement.

Attribute and access revisions keep a cleared floor value, because the next update needs its revision number. A binding position comes from the event that published it, so a later bind does not depend on a retained unbound value.

## Deletion, forks, and API behavior

Deleting a subtree unbinds its root and records a subtree tombstone. The descendants stay bound beneath that root, and the covering-tombstone rule hides them. Undelete binds the root again, at the name saved in the tombstone or at the caller's destination. Reusing a name writes a newer bound value.

A compaction in a fork applies the same retention rule to inherited and local runs. Inherited segments and content references keep their owner namespace. Pins still protect their captured manifests and runs.

WAL deltas, semantic fingerprints, and change-feed events do not depend on the binding row encoding. The API's `binding_version` token represents the position of the bound event. Creating, moving, or undeleting an entry changes the token. Content and attribute writes do not. Binding preconditions compare the same positions that reads return.

## Costs and alternatives

An unbind writes two rows. A bottom-anchored compaction can remove those rows and the history they hide. Reads need no join against removals. Bounded and streaming compaction both process one index at a time, and neither collects removals or makes point reads for them.

| Alternative | Cost |
| --- | --- |
| Collect removal identities before compacting the child index | Keeps joins in path reads, listings, and retention, and memory grows with the input. |
| Keep unbound floor values during bottom-anchored compactions | Retains abandoned slots and leaves different event sets in the indexes after a move. |
| Repeat the retired position on an unbound value | Repeats a reference already checked during commit validation. |
| Remove the child index | Parent lookup, deletion checks, and inode-addressed moves require slot scans. |

The durable format version is 1. A store written with a different binding encoding must be recreated, because decoding has no fallback.
