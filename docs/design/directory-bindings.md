# Directory bindings

A "directory slot" is a name within a parent directory, identified by `(parent_inode_id, name_key)`. Each change to a slot writes a bound or unbound value to two indexes. A read selects the newest visible value. Compaction processes each index separately, under one retention rule.

[Directory bindings](../specs/format.md#13-directory-bindings) in the storage format defines the records, keys, and visibility rules, and [Commit identity and preconditions](../specs/api.md#51-commit-identity-and-preconditions) in the API specification defines binding tokens and preconditions.

## Records and indexes

A directory binding row (`direntry_binding`) contains the parent's inode ID (`parent_inode_id`), the name key (`name_key`), the child's inode ID (`child_inode_id`), and the row's state (`state`). It also records its position in the namespace history: the sequence number of its commit (`committed_seq`) and its position within that commit (`delta_index`). A bound state is `{"kind":"bound","display_name":...}`, and an unbound state is `{"kind":"unbound"}`. A `DeltaPosition` pairs a sequence with a delta index, and LoonFS uses this pair to compare positions and to refer to another event.

| Family | Order | Filter key |
| --- | --- | --- |
| `direntry_binds` | Parent, name, child, committed sequence, delta index | Parent and name |
| `direntry_child_binds` | Child, parent, name, committed sequence, delta index | Child |
| `direntry_revisions` | Parent, name, child, committed sequence, delta index | Parent and name |

The `direntry_revisions` family isn't an index of binding rows. It stores a copy of each bound file's current revision under the file's edge (a [directory entry revision](../specs/glossary.md#directory-entry-revision)), in the slot index's order, so a listing reads its children's revisions with one scan of the parent. The two indexes contain the same records, which are the same edge events in two different orders. An "edge" is a parent, a name, and a child. Within one edge, positions sort oldest first in both indexes. Each bind and each unbind writes one record to each index. The unbind delta in the WAL identifies the exact bind it removes. LoonFS checks that reference when it validates the commit, before materialization, so the unbound row needs to store just its own position.

## Reads

This section describes how a read finds the current value of a slot or the parent of a child.

At sequence `N`, a slot's value is the version with the greatest `(committed_seq, delta_index)` at or below `N`. A bound value identifies its child. If the value is unbound, or the slot has no value, the name is available. A parent lookup follows the same rule in the child index. Within one commit, the last delta wins.

Key order isn't position order, because a slot's rows sort by child first, and a child's rows sort by parent and name first. So a read compares positions, and it never takes the last row in key order.

LoonFS checks two rules when it validates a commit: a slot has at most one child, and a child has at most one parent. A commit fails validation if it binds into a slot that already has another child, or binds a child that already has a binding, unless the same operation unbinds that binding first. A move unbinds the old slot before it binds the new one, and an undelete binds a deletion root, which has no binding. So the two indexes agree at every readable sequence.

A path lookup reads the slot index once for each path component. A listing scans the parent's binding prefix, selects one visible value for each name, and includes the entries that are bound. Inode visibility and covering subtree tombstones also apply. Neither operation joins bindings with a separate family of removals. Snapshots read through their pinned manifests, at their captured sequences.

## Retention

This section describes which binding rows compaction keeps and removes.

An unbound value is a tombstone for the older values of its edge. It must stay while an excluded run might still contain those older values. A compaction that excludes the group's oldest run keeps every row.

A "bottom-anchored" compaction includes the oldest run. It groups the rows of both indexes by edge, and for each edge, it keeps every version above the floor and the newest version at or below the floor. If that floor value is unbound, the compaction removes it, together with every older version of the edge. A child that left a slot by the floor has an unbind as its newest floor value there, so the compaction removes all of that edge's rows at or below the floor. The compaction reads an edge's `direntry_revisions` rows right after the edge's slot-index rows. It keeps every revision above the floor, and it keeps the newest revision at or below the floor only while the edge's floor value is bound, so an edge that loses its binding rows loses its revision rows in the same compaction.

Because the input is sorted, the compaction buffers at most one floor value for each edge. It also tracks the last edge that's bound at the floor, across the edges of one slot in the slot index, and across the edges of one child in the child index. It refuses a second child bound in one slot at the floor, or a second parent bound for one child at the floor, as corruption.

For example, suppose inode 7 moves from `/a` to `/b` at sequence 20, and the floor is at 20. A bottom-anchored compaction produces:

| Edge | Value at the floor | Retained rows in each index | Retained `direntry_revisions` rows, if inode 7 is a file |
| --- | --- | --- | --- |
| `/a` to inode 7 | Unbound | None | None |
| `/b` to inode 7 | Bound | The bound value | The newest revision at or below the floor |

Every event above the floor stays in both indexes. Both indexes group their rows by edge, so compaction keeps or removes each edge's rows the same way in both indexes, and both indexes keep the same set of events. The row-count and digest checks verify that the two indexes agree.

Why can compaction remove an unbound floor value, when attribute and access revisions keep a cleared floor value? The next attribute or access update needs the cleared value's revision number. A binding position comes from the event that published it, so a later bind doesn't depend on a kept unbound value.

## Deletion, forks, and API behavior

This section describes how bindings work with subtree deletion, forks, and the API.

Deleting a subtree unbinds its root and records a subtree tombstone. The descendants stay bound beneath that root, and the covering-tombstone rule hides them. An undelete binds the root again, either at the name saved in the tombstone or at the caller's destination. Reusing a name writes a newer bound value.

In a fork, compaction applies the same retention rule to inherited runs and local runs. Inherited segments and content references keep their owner namespace, and pins still protect their captured manifests and runs.

WAL deltas, semantic fingerprints, and change-feed events don't depend on how binding rows are encoded. The API's `binding_version` token represents the position of the bound event. Creating, moving, or undeleting an entry changes the token, but writing its content or attributes doesn't. Binding preconditions compare the same positions that reads return.

## Costs and alternatives

This section describes what this design costs, and the alternatives to it.

An unbind writes two rows, and a bottom-anchored compaction can remove those rows along with the history they hide. Reads don't need a join against removals. Compaction processes one index at a time, and it doesn't collect removals or make point reads to find them.

| Alternative | Cost |
| --- | --- |
| Collect removal identities before compacting the child index | Path reads, listings, and retention still need joins, and memory grows with the input. |
| Keep unbound floor values during bottom-anchored compactions | Abandoned slots are kept, and after a move, the two indexes contain different sets of events. |
| Repeat the retired position on an unbound value | Repeats a reference that LoonFS already checked during commit validation. |
| Remove the child index | Parent lookups, deletion checks, and inode-addressed moves need slot scans. |

The durable format version is 1. Decoding has no fallback, so a store written with a different binding encoding has to be recreated.
