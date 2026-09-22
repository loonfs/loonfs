# Versioned directory bindings

**Status: proposed.**

A directory binding is stored today as events: a bind row that names its generation, and later an unbind row that names the exact generation it retires. The reader reconstructs the state of a name by joining the two, and the compactor decides what to keep by pairing them. This note stores the state of a name instead. Each parent-and-name slot, and each child, is a versioned key whose newest version at the read sequence is the answer. Retention becomes the rule attributes and access rows already use, with ordinary tombstone handling.

## The problem

The `bindings` group has three families: `direntry_binds` by parent and name, `direntry_child_binds` by child, and `direntry_unbinds`. Every question about a name is a join across them:

- Resolving one path component finds the latest bind under the name, asks the unbind family whether that generation was retired, then asks the child index whether the child's latest binding is this one and whether that was retired (`active_child_binding` in `metadata/visibility.rs`).
- A directory listing pages binds by name and joins a range scan over unbinds for the same names before it can drop retired entries (`direntry_unbinds_for_parent_name_range` in `metadata/view.rs`).
- A base rebuild holds each bind until the unbinds of its generation arrive, checks a slot invariant across generations, and applies a separate survival rule (`BindingRetention` in `checkpoint/compaction_retention.rs`, `checkpoint/frozen_floor.rs`).
- The child index cannot be grouped with the unbinds that retire it, so the compactor either collects every unbind of the input in memory first or point-reads the unbind family for each reverse row, out of a cache it keeps for that purpose (`reverse_bind_survives` in `checkpoint/streaming_compaction.rs`).

A compactor that looks things up in its own input is the sign that the row model is not merge-shaped. SlateDB's compaction is a k-way merge over versioned keys: the newest version wins, and retention keeps the newest version at or below the horizon and everything above it. LoonFS uses that rule for attributes and access rows. Bindings are the one family that does not.

## The row

One record, `direntry_binding`, written under two keys:

| Field | Meaning |
| --- | --- |
| `parent_inode_id`, `name_key` | The slot. |
| `child_inode_id` | The child the event bound or unbound. |
| `generation` | `{seq, delta_index}` of the event, as the tombstone row spells it. |
| `state` | `{"kind":"bound","display_name":...}` or `{"kind":"unbound"}`. |

| Family | Row key |
| --- | --- |
| `direntry_binds` | `direntry-bind-{parent_inode_id:020}-{name_key_hex}-{u64::MAX - seq:020}-{u32::MAX - delta_index:010}` |
| `direntry_child_binds` | `direntry-child-bind-{child_inode_id:020}-{u64::MAX - seq:020}-{u32::MAX - delta_index:010}` |

The filter keys are unchanged: `direntry-bind-{parent}-{name}` and `direntry-child-bind-{child}`. Keys within a slot or a child sort newest first, like attributes. The `direntry_unbinds` family, its record, and its key grammar are removed. The `bindings` group has two families, and both hold the same records in different orders, which is what the format already says of them.

A bind delta writes a bound version in both orders. An unbind delta writes an unbound version in both orders, naming the same parent, name, and child the delta names. The row does not record which generation an unbind retired. Commit validation already guarantees an unbind lands only against the slot's current binding, so the newest version of the slot is the retired one by construction. The WAL deltas keep their fields; only their materialization changes.

## Reading

The state of a slot at sequence `N` is its newest version at or below `N`. Bound means the child is bound there; unbound or no version means the name is free. The current parent of a child is the child's newest version at or below `N`, read the same way. Several events on one slot inside one commit sort by delta index, so the last one in the commit wins.

Path resolution reads one prefix per component. The three-leg check goes away: if a slot's newest version binds child `C`, then `C`'s newest version names that slot, because any later event on `C` would have unbound it from the slot and become the slot's newest version instead. The invariant is the one the writer already keeps, that a slot holds one child and a child has one parent, and the digest check between the two orders keeps guarding it.

A listing scans the parent's prefix once. Names arrive in order with each name's versions newest first; the reader takes the first version at or below `N` per name and emits it when bound. That is the binding lookup only. A listing still checks each entry's inode, its covering tombstone, and the caller's rights, as it does today.

A snapshot or checkpoint reads the same rows through its pinned manifest at its captured sequence.

## Retention and compaction

The rule is `WholeState`, as for attributes and access rows, with one addition. For each slot and each child, a bottom-anchored rebuild keeps every version above the floor and the newest version at or below it. When that newest version is unbound, the rebuild removes it as well: every older version it could hide is in the rebuild's input, because runs partition the sequence axis and the window includes the oldest run. A rebuild above the base keeps every row, as today.

Attributes keep a cleared map at the floor and bindings drop an unbound state at the floor. The families differ because an attribute revision number is a per-inode counter that the next update validates against, so the cleared row must stay to hold the count. A binding generation is the sequence of its own event. Nothing counts from an unbound version, and a slot with no version reads exactly as one whose newest version is unbound.

The two orders keep the same records after a rebuild. Above the floor both keep every event. At or below it, a bound version survives in the forward order when no later event at or below the floor touched its slot, and in the child order when no later event at or below the floor touched its child. Those conditions coincide: an event that touches a child's binding unbinds it from the slot it occupies, and an event that touches an occupied slot unbinds its child. So the same bound versions survive in both orders, and unbound versions are removed from both. Per-run row counts stay equal and the digest comparison stays true.

The move of inode 7 from `/a` to `/b` with the floor past the move illustrates the rule. Forward: `/a` has an unbound newest version, removed with the bind it hid; `/b` keeps its bound version. Child: 7 keeps its bound version for `/b`; the unbound version from `/a` was older and is removed. One row in each order.

Subtree deletion unbinds the root and records a tombstone; descendants keep their bound versions under the deleted directory and stay hidden by the covering-tombstone walk, which follows current parent bindings upward and reaches the root's tombstone. Undelete binds the root again from the tombstone's saved name. A name reused after an unbind is a newer bound version on the same slot. A fork's base rebuild merges inherited and own runs under the same rule; inherited rows keep their owners.

## What stays the same

The WAL delta kinds and fields, the semantic fingerprint, and the change feed's events do not change. `binding_generation` on the wire is the generation of the slot's newest bound version, which is the same `(seq, delta_index)` it is today, so `expected_binding_generation` preconditions and the feed's `moved`, `directory_created`, `file_created`, and `undeleted` events are unchanged. Tombstones, active deletions, inodes, revisions, and the other families are untouched.

## Costs

- An unbind writes two rows instead of one. A base rebuild removes more rows than today, since it drops unbound versions and everything they hide.
- Materialization, the in-memory tail indexes, the visibility rules, the view, the manifest index, both compaction drivers, and their tests change together. This is a rewrite of the path the reads hit most.
- The format changes in sections 1.3, 4.3, 7.1, 10.3, and Appendix A.6, and the bind, child-bind, and mixed-family block fixtures regenerate. There is no compatibility path.

## Alternatives considered

**Keep the event rows and fix only the compactor.** Collecting every unbind of the input before the reverse pass removes the point reads, and the compactor already does this for merges that fit one step. It leaves the three-leg lookup, the listing join, the generation pairing, and the slot invariant check in place.

**Pure `WholeState` with the unbound version kept.** Simplest to state, but it keeps a row for every name ever abandoned, and it breaks parity: after the move above, the forward order keeps two rows and the child order one. The tombstone rule is what restores both.

**Store the retired generation on the unbound version.** It would let a reader verify which bind an unbind retired. Validation already established that at commit time, and the row would tie the durable shape to the exact-generation vocabulary the API keeps only for preconditions.

**One order only.** Dropping the child index would make parent lookup, the tombstone walk, and moves by inode scan every slot. Two orders of one record is the right shape; the change is what the record is.

## Verification

- Resolving and listing at sequences before, between, and after a bind, an unbind, and a rebind of one name return the state at each sequence, on the live view and through a pinned manifest.
- Several changes to one slot in one commit resolve to the last one in delta order.
- A base rebuild past a move removes the unbound version and the bind it hid, keeps the rebind, and leaves both orders with the same records; the parity and digest checks pass on the output.
- A rebuild above the base keeps every row.
- A listing page reads one binding family and no unbind rows; the request-counting store shows no second family read for names.
- Subtree delete, undelete, and a fork's base rebuild over inherited runs produce the state each did before.
- A precondition with an expected binding generation accepts the current version and rejects a stale one exactly as today.
