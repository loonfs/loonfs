# Versioned directory bindings

Status: proposed.

A directory binding associates a name with an inode. Today, LoonFS stores a bind record and a separate unbind record when that binding is removed. Reads and compaction have to match the two to determine whether the binding still exists.

This proposal stores each change as a versioned value: the name is either bound to a child or unbound. Readers select the latest version visible at their sequence. Compaction uses the same retention rule as attributes and access rows, with one addition for unbound values.

## Why change it

Bindings currently occupy three metadata families: `direntry_binds`, ordered by parent and name; `direntry_child_binds`, ordered by child; and `direntry_unbinds`.

This adds work to both reads and compaction:

- A path lookup finds a bind, checks whether it was unbound, and checks the child's binding in the reverse index.
- A directory listing scans binds and unbinds for the same range of names, then matches them to exclude removed entries.
- Compaction pairs binds with unbinds to decide what to retain. For the child index, it either collects the unbinds in memory or reads them individually from its own input.

Reads and compaction can then use versions from one index at a time. The separate unbind family and the lookups needed to join it are removed.

## Stored records

A slot is a name within a parent directory, identified by `(parent_inode_id, name_key)`. Each change produces one `direntry_binding` record, stored in both the slot index and the child index.

| Field | Meaning |
| --- | --- |
| `parent_inode_id`, `name_key` | The slot being changed. |
| `child_inode_id` | The child being bound or unbound. |
| `generation` | The event's `{seq, delta_index}`. An unbound version identifies the unbind event. |
| `state` | `{"kind":"bound","display_name":...}` or `{"kind":"unbound"}`. |

Both indexes contain the same records in different orders:

| Family | Row key |
| --- | --- |
| `direntry_binds` | `direntry-bind-{parent_inode_id:020}-{name_key_hex}-{u64::MAX - seq:020}-{u32::MAX - delta_index:010}` |
| `direntry_child_binds` | `direntry-child-bind-{child_inode_id:020}-{u64::MAX - seq:020}-{u32::MAX - delta_index:010}` |

Versions sort newest first within each slot or child. The filter keys remain `direntry-bind-{parent}-{name}` and `direntry-child-bind-{child}`. The `direntry_unbinds` family, record, and key format are removed.

A bind writes a bound version to both indexes. An unbind writes an unbound version with the same parent, name, and child as its WAL delta. Commit validation already requires an unbind to refer to the current binding, so the stored row does not need to repeat which generation it removed. The WAL delta retains that information.

## Reads

At sequence `N`, a slot's value is its newest version at or below `N`. A bound version identifies the child at that name. An unbound version, or no version, means the name is available. Looking up a child's parent follows the same rule in the child index. Within a commit, the delta index determines the order, so the last change to a slot or child wins.

Path resolution needs one lookup in the slot index for each component. Commit validation enforces one child per slot and one parent per child. Moving a child requires unbinding it from its previous slot, so the two indexes agree at every read sequence.

A directory listing scans the parent's binding prefix in name order, selects the newest visible version of each name, and includes bound entries. It still checks the inode, any covering deletion tombstone, and the caller's permissions. Snapshots and checkpoints use the same lookup rules through their pinned manifests at their captured sequences.

## Retention

The existing `WholeState` rule retains all versions above the retention floor and the newest version at or below it. Bindings use that rule with one addition: if the newest version at or below the floor is unbound, remove it too.

This removal is safe only during a rebuild that includes the oldest run. Runs cover separate sequence ranges, so that rebuild contains every older version the unbound row could hide. Removing them together preserves the state at every retained sequence, including any later changes above the floor. A compaction that excludes the oldest run keeps every row.

Attributes retain a cleared map at the floor because its revision number is needed to validate the next update. Binding generations use the event's sequence and delta index. A later bind does not depend on the unbound row, so that row can be removed.

### A move from `/a` to `/b`

Suppose inode 7 is bound at `/a`, then moved to `/b` at sequence 20. The move unbinds `/a` and binds `/b`. With the retention floor at 20, a rebuild retains:

| Index entry | Latest state at the floor | Rebuild result |
| --- | --- | --- |
| Slot `/a` | Unbound | Remove the unbound version and the older bind. |
| Slot `/b` | Bound to inode 7 | Keep the bound version. |
| Child 7 | Bound at `/b` | Keep that version and remove its earlier versions. |

Each index retains one record: inode 7 bound at `/b`. Keeping the unbound version for `/a` would leave two records in the slot index and one in the child index.

Above the floor, both indexes retain every event. At the floor, a bound version is current in both indexes or neither: replacing a slot's child requires an unbind, and moving a child requires unbinding its old slot. Removing unbound versions at or below the floor therefore leaves the same records in both indexes. The existing row-count and digest checks continue to verify that agreement.

## Deletes, forks, and API behavior

Subtree deletion still unbinds the deleted directory and records a tombstone. Descendants remain bound beneath it and are hidden by the existing covering-tombstone check. Undelete binds the directory again using the tombstone's saved name. Reusing a name writes a newer bound version to that slot.

A fork's base rebuild applies the same retention rule to inherited and local runs. Inherited rows keep their owners.

WAL deltas, semantic fingerprints, and change-feed events remain unchanged. The API's `binding_generation` is still the bound event's `(seq, delta_index)`, so `expected_binding_generation` accepts or rejects the same requests as before. Other metadata families are unchanged.

## Implementation and tradeoffs

An unbind writes two rows instead of one. A base rebuild can later remove the unbound versions and the history they hide.

The implementation must update WAL materialization, tail and manifest indexes, reads, and both compaction paths together. This affects every path lookup and directory listing. The storage format changes in sections 1.3, 4.3, 7.1, 10.3, and Appendix A.6; the bind, child-bind, and mixed-family block fixtures must be regenerated. No compatibility path is proposed.

## Alternatives

| Alternative | Why it is not proposed |
| --- | --- |
| Collect unbinds before compacting the child index | Removes point reads during compaction, but leaves the joins in path lookups, listings, and retention. |
| Keep the unbound version at the floor | Retains abandoned names and leaves different records in the two indexes, as the move example shows. |
| Store the retired generation on each unbound version | Repeats information already checked during commit validation. Reads need only the latest state. |
| Remove the child index | Parent lookups, deletion checks, and moves by inode would require scanning slots. |

## Verification

- Check path lookups and listings before and after binding, unbinding, and reusing a name, including reads through pinned manifests.
- Check that several changes to one slot or child within a commit resolve to the last delta.
- Rebuild through the retention floor after a move. Verify that both indexes retain the same records and pass the row-count and digest checks.
- Verify that compaction above the base retains every row.
- Confirm that a listing reads one binding family, with no separate unbind scan.
- Check subtree deletion, undelete, fork rebuilds over inherited runs, and binding-generation preconditions against their current behavior.
