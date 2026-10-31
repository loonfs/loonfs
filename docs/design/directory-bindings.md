# Directory bindings

A "directory slot" is a name within a parent directory, identified by `(parent_inode_id, name_key)`. Each change to a slot writes a bound or unbound value to two indexes, and each revision of a bound file writes the file's new revision summary to one of them. A read takes each edge's newest value at its sequence, which is the edge's first row. Compaction processes each index separately, under one retention rule.

[Directory bindings](../specs/format.md#13-directory-bindings) in the storage format defines the records, keys, and visibility rules, and [Commit identity and preconditions](../specs/api.md#51-commit-identity-and-preconditions) in the API specification defines binding tokens and preconditions.

## Records and indexes

A directory binding row (`direntry_binding`) contains the parent's inode ID (`parent_inode_id`), the name key (`name_key`), the child's inode ID (`child_inode_id`), the child's kind and creation attribution, and the row's state (`state`). It records its position in the namespace history (`committed_seq` and `delta_index`), its commit's ID, actor, and time, and the position of the bind that established the binding (`bind_position`). A bound state is `{"kind":"bound","display_name":...}`, and a bound file row's state also includes the file's revision summary (`revision`): the revision number, the size, and the actor and time of the revision's commit. An unbound state is `{"kind":"unbound"}`.

| Family | Order | Filter keys |
| --- | --- | --- |
| `direntry_binds` | Parent, name, child, position newest first | Parent and name, and parent |
| `direntry_child_binds` | Child, parent, name, position newest first | Child |

An "edge" is a parent, a name, and a child. A bind writes a bound row to both indexes, and an unbind writes an unbound row to both. These rows are the binding events, and both indexes store the same ones. A revision of a file writes one more bound row, a "summary row", to the slot index alone, under the file's current edge. The summary row copies the edge's name and attribution, keeps its `bind_position`, and stores the new revision's summary, so the slot index has the file's current revision next to its name. The unbind delta in the WAL identifies the exact bind it removes, and an unbound row's `bind_position` is that bind's position.

## Reads

This section describes how a read finds the value of a slot, the parent of a child, or the entries of a directory.

At sequence `N`, an edge's value is its first row in key order whose `committed_seq` is at or below `N`, because each edge's rows sort newest first. A slot's value is the newest of its edges' values, and a parent lookup applies the same rule to a child's edges in the child index. A bound value identifies its child, and an unbound value, or no value, means the name is available. A read takes an edge's first row at or below its sequence and then moves past the edge, so a file with thousands of summary rows costs one row.

LoonFS checks two rules when it validates a commit: a slot has at most one child, and a child has at most one parent. A commit fails validation if it binds into a slot that already has another child, or binds a child that already has a binding, unless the same operation unbinds that binding first. A move unbinds the old slot before it binds the new one, and an undelete binds a deletion root, which has no binding. So the two indexes agree at every readable sequence.

A path lookup reads the slot index once for each path component. A listing scans the parent's prefix of the slot index once, takes each edge's value, and includes the bound children that no tombstone hides. A file's entry reports its revision number, size, and revision attribution from the summary in its row, so the listing reads no revision row and no inode row, and it doesn't report a content reference. The parent probe in each `direntry_binds` filter lets the listing skip a run that has no row under the parent. Snapshots read through their pinned manifests, at their captured sequences, and the first row at or below the captured sequence has the summary as of that sequence.

## Retention

This section describes which binding rows compaction keeps and removes.

An unbound value is a tombstone for the older values of its edge. It must stay while an excluded run might still contain those older values. A compaction that excludes the group's oldest run keeps every row.

A "bottom-anchored" compaction includes the oldest run. It groups the rows of both indexes by edge. For each edge and each kind of row (binding events and summary rows), it keeps every row above the floor and the newest row at or below the floor. If the edge's newest binding event at or below the floor is unbound, it removes the edge's rows of both kinds at or below the floor. The rows arrive newest first, so the operator decides each row when it arrives. It also tracks the last edge that's bound at the floor, across the edges of one slot in the slot index, and across the edges of one child in the child index, and it refuses a second child bound in one slot at the floor, or a second parent bound for one child at the floor, as corruption.

For example, suppose inode 7 is bound at `/a` at sequence 10, written at sequence 15, and moved to `/b` at sequence 20, and the floor is at 20. A bottom-anchored compaction produces:

| Edge | Value at the floor | Retained rows in the slot index | Retained rows in the child index |
| --- | --- | --- | --- |
| `/a` to inode 7 | Unbound | None | None |
| `/b` to inode 7 | Bound | The bind at 20 | The bind at 20 |

If the file is instead written at sequence 15 and not moved, the slot index keeps the summary row at 15 and the bind at 10, and the child index keeps the bind at 10.

Every row above the floor stays. The child index stores binding events alone, and the rule judges binding events apart from summary rows, so both indexes keep the same binding events. The merge's digests of the two indexes' binding events, and the manifest's check that each run's slot index has at least as many rows as its child index, verify that the indexes agree.

Why can compaction remove an unbound floor value, when attribute and access revisions keep a cleared floor value? The next attribute or access update needs the cleared value's revision number. A binding position comes from the event that published it, so a later bind doesn't depend on a kept unbound value.

## Deletion, forks, and API behavior

This section describes how bindings work with subtree deletion, forks, and the API.

Deleting a subtree unbinds its root and records a subtree tombstone. The descendants stay bound beneath that root, and the covering-tombstone rule hides them. An undelete binds the root again, either at the name saved in the tombstone or at the caller's destination. Reusing a name writes a newer bound value.

In a fork, compaction applies the same retention rule to inherited runs and local runs. Inherited segments and content references keep their owner namespace, and pins still protect their captured manifests and runs.

WAL deltas, semantic fingerprints, and change-feed events don't depend on how binding rows are encoded. The API's `binding_version` token represents a binding's `bind_position`, which a summary row keeps. Creating, moving, or undeleting an entry changes the token, but writing its content or attributes doesn't. Binding preconditions compare the same positions that reads return.

## Costs and alternatives

This section describes what this design costs, and the alternatives to it.

An unbind writes two rows, and a bottom-anchored compaction can remove those rows along with the history they hide. A revision writes one summary row, which compaction removes once a newer summary row is at or below the floor. Reads don't need a join against removals or against revisions. Compaction processes one index at a time, and it doesn't collect removals or make point reads to find them.

| Alternative | Cost |
| --- | --- |
| Collect removal identities before compacting the child index | Path reads, listings, and retention still need joins, and memory grows with the input. |
| Keep unbound floor values during bottom-anchored compactions | Abandoned slots are kept, and after a move, the two indexes contain different sets of events. |
| Read each file's current revision from `revisions` during a listing | A listing makes one lookup for each file, keyed by the file's inode ID, in rows stored far from the directory's bindings. |
| Store the revision summaries in a family of their own, ordered like the slot index | A listing scans two families and joins them by edge, and each family needs its own retention rule. |
| Remove the child index | Parent lookups, deletion checks, and inode-addressed moves need slot scans. |

The durable format version is 1. Decoding has no fallback, so a store written with a different binding encoding has to be recreated.
