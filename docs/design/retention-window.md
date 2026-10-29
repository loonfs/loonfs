# Retention window

This note describes how the [retention floor](../specs/glossary.md#retention-floor) expires deletions, how the writer's log acknowledges each floor so that a base compaction can remove the rows that an expired deletion hides, what that compaction removes, and how the reference server advances and acknowledges the floor on a schedule.

## History that grows without bound

This section describes why LoonFS needs to bound how long a namespace keeps deleted items and old revisions.

An undelete restores a deleted item with its revisions, its attributes, and everything below it, so LoonFS can't reclaim any of those rows or their content while the deletion can be undone. A deleted file keeps its newest revision for this reason, and each recoverable deletion has an entry in the namespace's "trash" (the `active_deletions` family).

Two gaps would let this history grow without bound. If the [retention floor](../specs/glossary.md#retention-floor) didn't cover deletions, advancing it wouldn't let LoonFS reclaim anything that a deletion hides, so a namespace that deletes files every day would keep every byte it ever deleted. If no process advanced the floor on a schedule, revision history, trash entries, and the content behind them would grow until an operator ran the `retention` job by hand.

## Recoverable and expired deletions

This section describes how long a deletion stays recoverable.

LoonFS can't reclaim what a deletion hides while the deletion can still be undone, so each deletion needs a point in the namespace's history where it stops being recoverable.

The [retention floor](../specs/glossary.md#retention-floor) bounds every kind of history that a namespace keeps: incremental replay, superseded bindings, file revisions, attribute and access states, and commit receipts (see [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor)). The floor bounds recoverable deletions in the same way. Each deletion has a deletion sequence (`deletion_seq`), which is the sequence number of the commit that deleted the item. A deletion has "expired" in a [read view](../specs/glossary.md#read-view) when its deletion sequence is at or below the retention floor of the view's manifest, and it's "recoverable" when its tombstone's latest event is `set` and its deletion sequence is above that floor. Nothing is committed when a deletion expires. A retention advance moves the floor, and every view on the advance's manifest, or on a later one, treats the deletions that the floor passed as expired.

An expired deletion can't be undeleted, and the trash listing leaves it out. No read of the namespace's current state can reach the inodes that the deletion hides, and LoonFS can reclaim their rows and content once the writer's log has acknowledged the floor, as described in [Acknowledging the floor](#acknowledging-the-floor). (A snapshot from before the advance still shows them, as described in [Reads of an expired subtree](#reads-of-an-expired-subtree).) Until the floor reaches a deletion, an undelete can restore the deleted item as described in [File and subtree deletion](../specs/format.md#16-file-and-subtree-deletion). Deletions don't have a floor of their own, and expiry doesn't depend on a clock.

For example, suppose a file is deleted at sequence 40 while the floor is at 35. The file is in the trash, and an undelete can restore the file. A retention advance then moves the floor to 50. In every view on the advance's manifest or a later one, the deletion has expired, so the trash listing leaves it out and an undelete of it returns `not_deleted`.

Undelete validation compares the deletion's sequence with the floor of the writer's basis, which is the manifest that the [writer session](../specs/glossary.md#writer-session) plans against. The trash listing scans `active_deletions` from above the floor of the view's manifest. The family's keys begin with the deletion sequence, so the floor is where the scan starts, not a filter on its results, and a page costs the same whatever the floor is.

Deletions can be nested. If `/old/draft.txt` is deleted on its own and `/old` is deleted later, each deletion has its own tombstone and its own trash entry, and undeleting `/old` doesn't restore `draft.txt`. An item can't be deleted while a deletion above it hides it, so an inner deletion is always older than the deletion around it, and the floor reaches the inner deletion first, or in the same advance. So no recoverable deletion is ever inside an expired one.

A [fork](../specs/glossary.md#fork)'s floor starts at its fork point, so every deletion that a fork inherits is at or below the fork's floor, and it has expired in the fork from the fork's creation. The fork's trash listing starts its scan above the fork point, an undelete of an inherited deletion returns `not_deleted`, and no writer has to run in the fork for either. The fork's first manifest also acknowledges the fork point (see [Acknowledging the floor](#acknowledging-the-floor)), so a base compaction in the fork can remove the rows that an inherited deletion hides. The source namespace keeps its own trash under its own floor, so an inherited deletion can still be recovered in the source.

For example, suppose a namespace's trash lists a deletion at sequence 80, and the namespace is forked at sequence 100. The fork's floor starts at 100, so the fork's trash listing starts its scan above 100 and doesn't list the deletion, and an undelete of the deletion in the fork returns `not_deleted`. In the source, the deletion stays recoverable under the source's own floor.

Why don't deletions have a floor of their own? The floor is the namespace's one bound on how far back it keeps history, and deletions are part of that history. With one floor, a client can rely on one number for how far back it can recover anything, whether that's an old revision or a deleted file.

Why does a fork lose its inherited trash? A fork's history starts at its fork point, so the fork's floor can't be lower than the fork point. Keeping inherited deletions recoverable in the fork would take a second floor (see [Alternatives considered](#alternatives-considered)).

## Acknowledging the floor

This section describes how a base compaction learns that it can remove the rows that an expired deletion hides, and why the retention floor alone doesn't tell it.

A retention advance is a maintainer publication and an undelete is a writer commit, so LoonFS can't order a floor move against an undelete. A writer validates each commit against its basis, and it keeps that basis until its next successor check (see [Publication and group commit](../specs/format.md#63-publication-and-group-commit)). So a writer can validate an undelete against a floor that a maintainer has already moved past the deletion. For example, an undelete of a deletion at sequence 10 can be validated while the writer's basis has a floor of 5 and committed at sequence 22, while the maintainer moves the floor to 20 in between. If a base compaction compared deletions with the floor alone, it could remove the subtree's rows before the fold of commit 22, and that fold would bind an item whose rows are gone.

LoonFS closes this gap by recording, in the writer's log, which floor each WAL object was validated against:

- Each WAL object records its "basis floor" (`basis_floor_seq`), the retention floor of the manifest that the writer validated the object's commits against. A [fence](../specs/glossary.md#fence) records the floor of the manifest that the acquiring session discovered before it wrote the fence.
- Each manifest records an "acknowledged floor" (`acknowledged_floor_seq`), the highest basis floor among the WAL objects folded into it. A fold raises it to the highest basis floor of the objects it folds, and every other publication copies it. It never decreases, and it's never above the manifest's retention floor. A new namespace's first manifest has an acknowledged floor of 0, and a fork's first manifest has the fork point.
- A base compaction treats a deletion as expired when the deletion's latest tombstone event in the compaction's basis is `set` and its deletion sequence is at or below the basis manifest's acknowledged floor. Reads, undelete validation, and the trash listing use the retention floor itself, as described in [Recoverable and expired deletions](#recoverable-and-expired-deletions).

For example, suppose the floor is 5 and the acknowledged floor is 5. A retention advance moves the floor to 20. The writer's next commit, at sequence 23, is validated against the advance's manifest, so WAL object 12 records a basis floor of 20. The fold of WAL object 12 publishes a manifest with an acknowledged floor of 20, and a base compaction over that manifest removes the rows of every deletion at or below 20 whose latest event is `set`. Between the advance and that fold, a view on the advance's manifest already treats those deletions as expired, but a compaction keeps their rows.

A base compaction can trust the acknowledged floor for these reasons:

1. Basis floors never decrease along the WAL. Within one writer session, the basis moves only forward, and a successor manifest never lowers the floor. When a new session takes over, its claim manifest is a successor of every manifest the old session used (a session never adopts a manifest with another writer epoch), its fence and later objects are planned against the claim or its successors, and every object of the old session comes before the fence.
2. The WAL object of an accepted undelete has a basis floor below the deletion, because validation returns `not_deleted` for a deletion at or below the floor.
3. The acknowledged floor is the basis floor of some folded WAL object, call it `w`. An undelete of a deletion at or below the acknowledged floor is in an object with a lower basis floor, which comes before `w` by the first reason, so that object is folded too, and the deletion's latest event in the basis is `revoke`. So if the latest event is `set`, no undelete of the deletion is in the log, and none can be committed later, because every object after `w` has a basis floor at or above the acknowledged floor.

A fork's first manifest acknowledges the fork point without a WAL object, because the fork's log is empty, and every commit in it is validated against a fork manifest, whose floor is at least the fork point. So the fork's log never accepts an undelete of an inherited deletion.

A namespace whose writer commits nothing after an advance never acknowledges the new floor on its own, so its hidden rows stay until a WAL object is written. Any process can acknowledge the floor for it by acquiring the writer epoch and writing a fence, the same way a new writer session starts (see [Writer ownership](../specs/format.md#61-writer-ownership)), and then folding. The reference server does this in its full pass, as described in [Advancing the floor on a schedule](#advancing-the-floor-on-a-schedule).

Why does a base compaction compare a deletion with the acknowledged floor, and not with the retention floor? The retention floor says that the deletion has expired, but it doesn't say that every undelete validated against an older floor has been folded. The acknowledged floor says both, because a WAL object that records the new floor comes after every object validated against an older one.

Why is the acknowledgment a WAL object, and not a time recorded in the manifest? A writer trusts a basis for at most the revalidation bound, so after that bound plus a clock allowance, no writer validates against the old floor. But a fold has no time bound, so an undelete validated against the old floor can stay unfolded in the WAL for any length of time, and a compaction would also have to know that every such object has been folded. With a basis floor in each WAL object, the order of the log shows which objects were validated against the old floor, and row retention keeps depending on sequence numbers and conditional writes alone.

Why does a fence acknowledge the floor? A fence is planned against a discovered manifest like any WAL object, and once it's written, no commit of an older session can follow it, so the fence's basis floor bounds the basis floor of every later object.

Why is the acknowledged floor never above the retention floor? A WAL object's basis was the current manifest before the object was written, and a fold publishes its manifest as a successor of a manifest that was current after the fold read the object. So the fold's predecessor comes at or after the basis in the manifest chain, and its floor is at least the basis floor. A manifest whose acknowledged floor is above its retention floor is corrupt.

## Reads of an expired subtree

This section describes what a read returns for an inode that an expired deletion hides.

Base compactions of different [family groups](../specs/glossary.md#family-group) run at different times, so for a while some groups have removed an expired subtree's rows and others haven't. A read by inode inherits access grants from the inode's ancestors, as described in [Access rows](../specs/format.md#18-access-rows). If the `access` group removed a hidden directory's access row, and with it the boundary flag, before the `revisions` and `inodes` groups removed their rows, a read by inode would inherit the grants above the boundary. Principals that the boundary excluded could then read the rows that those groups still store.

From the moment a view's floor reaches a deletion, no read of the namespace's current state through that view can reach an inode that the deletion hides. A read by path doesn't resolve the inode, as with any [tombstone](../specs/glossary.md#tombstone). Before LoonFS serves a read by inode ID, whether of the inode, its revisions, its attributes, or its content, it checks the inode's ancestry. This ancestry walk starts at the inode, follows the current bound bindings up through the inode's ancestors, and stops at the first tombstone that hides the inode or one of those ancestors:

- If that tombstone's deletion is at or below the view's floor, the read returns `inode_not_found`, the error for an unknown inode.
- If its deletion is above the floor, the read is allowed, and the subject's rights are evaluated through the deletion's saved parent, as for any recoverable deletion.
- If the walk reaches the root inode without meeting a tombstone, the read is allowed.
- If the walk reaches an inode with no bound binding before it meets a tombstone, the chain to the root is broken, and the read returns `inode_not_found`.

A tombstone whose latest event is `revoke` hides nothing, so the walk passes it and continues through the item's current binding. LoonFS makes this check before it evaluates any access grant, so every principal gets the same result, an administrator included.

For example, suppose the directory `/team` grants a team `read`, and its subdirectory `/team/hr` has its boundary flag set and grants `read` to one person. `/team/hr` contains the file `salaries.csv`. Someone deletes `/team/hr` at sequence 40, and a retention advance moves the floor to 50. From that advance on, a read by inode of `salaries.csv`, or of any of its revisions, returns `inode_not_found` for every principal. So if the `access` group's base compaction removes the access row for `/team/hr` before the `revisions` group's base compaction removes the file's revisions, no team member can read those revisions through the grant on `/team`.

A snapshot from before the advance still shows the subtree, because a read through the snapshot uses the pinned manifest, whose floor is below the deletion and whose segments still store the subtree's rows. In an ACL namespace (one whose access mode is `acl`, see [Access rows](../specs/format.md#18-access-rows)), LoonFS evaluates a snapshot read's rights on the access rows at the current head. So once a deletion has expired at the head and hides the inode there (or the inode's chain to the root is broken there), a read of the inode through the snapshot requires an administrator, and any other subject gets `inode_not_found` or `path_not_found`, as a read of the current state does. In an `unrestricted` namespace, a snapshot read of the inode works as it did before the advance.

Why does the subtree become unreadable when the floor reaches the deletion, before any compaction runs? The check closes the window between the family groups' compactions whatever order the groups compact in, and keeping the tombstone alone doesn't.

Why does a read compare the deletion with the floor of its own view, when another view can have a newer floor? Each view reads the segments that its own manifest lists, and a compaction removes a deletion's rows only in a manifest whose acknowledged floor, and so whose retention floor, is at or above the deletion. So in every view, a deletion whose rows a compaction may have removed is at or below the view's floor, and the view already hides it.

Why does LoonFS refuse a read when the chain is broken? Every live inode has bound bindings up to the root inode, and every deleted inode has them up to its deletion's root, which has a tombstone. A chain that breaks before it reaches either one belongs to an inode that the namespace has no way to show. Base compactions remove an expired subtree's rows one family group at a time, so the bindings that lead up to the expired root can be gone while the revisions below it are still stored. A walk from one of those revisions' inodes can then no longer reach the tombstone, so LoonFS refuses the read because the chain is broken.

Why does a snapshot read of an expired subtree require an administrator? LoonFS evaluates a snapshot read's rights on the access rows at the current head, and once a deletion expires, base compactions remove the head's rows under it one family group at a time. So at the head, a boundary row can be gone while a grant above it is still stored, and a subject that the boundary excluded could then inherit that grant. An administrator's rights don't depend on those rows.

## Removing an expired deletion's rows

This section describes the rows that a base compaction removes for an expired deletion.

A deletion expires when the floor reaches it, but the rows that the deletion hides stay in the namespace's metadata until a compaction removes them.

A "base compaction" merges a window of runs that includes a [family group](../specs/glossary.md#family-group)'s oldest run, so it can drop rows under the retention rules (see [Compaction windows](../specs/format.md#102-compaction-windows)). A deletion has expired in the compaction's [metadata basis](../specs/glossary.md#metadata-basis) when the deletion's latest tombstone event in that basis is `set` and its deletion sequence is at or below the basis manifest's acknowledged floor (see [Acknowledging the floor](#acknowledging-the-floor)). For each expired deletion, the compaction removes the rows of every inode that the deletion hides, which are the deletion's root and every descendant of the root in the basis. For example, if the directory `/old` (inode 7) contains `a.txt` (inode 8), and the deletion of `/old` has expired, the deletion hides inodes 7 and 8.

| Family | Rows a base compaction removes for an expired deletion |
| --- | --- |
| `active_deletions` | The root's `listed` row. |
| `inodes`, `revisions`, `attributes`, `access` | Every row of a hidden inode. |
| `direntry_binds`, `direntry_child_binds` | Every edge whose parent is a hidden inode. The binding rule in [Row retention during a base compaction](../specs/format.md#103-row-retention-during-a-base-compaction) covers the root's own edge. The newest version of that edge at or below the floor is unbound, so the compaction drops that version together with every older version it hides. |
| `tombstones` | None. Every `set` and `revoke` event stays. |
| `content_layouts` | The chains of the removed revisions. The basis revision filter drops them as it does for any removed revision. |

A delta-only compaction (one whose window doesn't include the group's oldest run) removes none of these rows, because it keeps every history row (see [Compaction windows](../specs/format.md#102-compaction-windows)).

Reclaiming a deletion's content takes four steps:

1. A retention advance moves the floor past the deletion, and the deletion expires.
2. A WAL object validated against the new floor is written (the writer's next commit, or a fence), and a fold raises the manifest's acknowledged floor to it.
3. A base compaction of each family group removes the rows that the deletion hides, and a base compaction of `content_layouts` that runs after the revisions are gone drops their chains.
4. [Garbage collection](../specs/glossary.md#garbage-collection-gc) deletes the content objects once no current, pinned, or recently replaced manifest refers to them and they're older than the [reclamation grace](../specs/glossary.md#reclamation-grace) (see [Content roots](../specs/format.md#119-content-roots)). Content that a kept revision also refers to stays.

Why can a base compaction remove every row of a hidden inode, and not just the older rows that the usual retention rules drop? The deletion is at or below the acknowledged floor, and so at or below the retention floor, and the tombstone hides those inodes at every sequence from the deletion on, so they're hidden in every view that compaction has to preserve. Every row of a hidden inode is also at or below the floor, because LoonFS commits no change to a hidden inode. (If an inode was undeleted and then deleted again, its latest deletion is the one that expired, so its rows are older than that deletion too.)

Why keep the tombstone events? Family groups are compacted at different times, so for a while some groups have removed a hidden inode's rows and others haven't. The ancestry walk in [Reads of an expired subtree](#reads-of-an-expired-subtree) stops at the tombstone, so the tombstone keeps those inodes unreadable in between. Once the bindings group has removed the edges that lead up to the tombstone, the broken chain keeps them unreadable instead, so a read by inode ID can't find a partly removed subtree. The cost is one small tombstone row for each deletion and each undelete.

## The kept inode filter

This section describes how a base compaction finds the inodes that an expired deletion hides.

A compaction tests one row at a time, so it needs a way to check whether each row's inode is hidden, and a hidden subtree can have more inodes than fit in memory. Before a base compaction merges the `inodes`, `revisions`, `attributes`, or `access` group, or the bindings group (`direntry_binds` and `direntry_child_binds`), it builds a "kept inode filter". That filter is a Bloom filter that contains every inode that's visible or under a recoverable deletion in the basis. A bounded compaction step and a streaming compaction job both build the filter before they merge.

To build the filter, the compaction:

1. Walks the basis's directory tree at the basis's head, starting at the root inode, depth first and with one cursor for each level, the way a recursive listing does. It reads each directory's children from the slot index (`direntry_binds`), follows each bound edge, and inserts every inode it reaches.
2. Walks the subtree of every recoverable deletion the same way, starting at the deletion's root. It reads those roots from the basis's `listed` rows in `active_deletions` that have no `removed` row and whose deletion sequence is above the basis manifest's acknowledged floor.

Deleting an item unbinds it, so the walk from the root inode doesn't reach a deleted subtree. An expired deletion isn't a starting point for the second walk, and its root is unbound, so neither walk inserts the inodes that the expired deletion hides. An inner deletion is older than the deletion around it (see [Recoverable and expired deletions](#recoverable-and-expired-deletions)), so no recoverable deletion is inside an expired one, and the two walks reach every inode that a view can show, including every ancestor through which LoonFS evaluates the rights on an item under a recoverable deletion.

The merge removes a row whose inode isn't in the filter, and applies the usual retention rules to a row whose inode is in the filter. The merge tests a binding row by its parent inode. The two binding indexes are merged in one compaction against the same filter, so the compaction keeps or removes each binding event the same way in both indexes.

A Bloom filter can report an inode that was never inserted (a false positive), but it never misses an inode that was inserted. A false positive keeps a hidden inode's rows for one compaction, and a later compaction uses a different seed, so the later compaction removes those rows. The layout filter in [Row retention during a base compaction](../specs/format.md#103-row-retention-during-a-base-compaction) errs the same way, and the kept inode filter follows the layout filter's rules: xxh64 double hashing, a seed from the output run number, and ten bits per expected entry, up to `MAX_BLOOM_FILTER_BYTES` (64 MiB). The expected number of entries is the number of rows in the basis's `inodes` family (the sum of its segments' `row_count`), which shrinks as base compactions remove hidden inodes. Ten bits per entry fit about 53 million entries in 64 MiB. Above that, the filter stays at 64 MiB, so it has fewer bits per entry and uses fewer hash positions, and it reports more false positives. For example, a filter for 100 million entries has about five bits per entry and keeps about one hidden row in thirteen, and the compactions after it remove a share of the rest each time.

If the walk would go more than `MAX_KEPT_INODE_WALK_DEPTH` (4,096) levels deep, the compaction keeps every row it would have tested, removes no hidden rows in that step, and logs that it reached the limit. The depth limit applies to the cursors that the walk keeps at once, one for each level. A move can nest a tree deeper than the longest path that a request accepts. Each level of the walk keeps a directory's resume key of up to about 1.6 KiB, so 4,096 levels take at most about 7 MiB.

The walk reads the slot-index rows of every directory it reaches, so each base compaction of these groups costs one more pass over the bindings of the visible and recoverable tree.

A base compaction over a manifest whose acknowledged floor is still 0 doesn't build the filter. It keeps every row and doesn't walk the tree, so the base compactions of a namespace whose floor has never been acknowledged skip that extra pass.

Why does the compaction build a filter from a walk, and not an exact set of hidden inodes? A hidden subtree can have more inodes than fit in memory, and so can the visible tree. A filter's size is set before the walk starts, and a filter with no false negatives errs toward keeping a row, so the compaction stays inside its memory limit without removing a row that a view needs.

Why can a base compaction over an acknowledged floor of 0 skip the walk? A deletion's sequence is at least 1, so a deletion has expired in the compaction's basis only when the acknowledged floor is at least 1, and the acknowledged floor never decreases from one manifest to the next. So a manifest whose acknowledged floor is 0 contains no deletion that the compaction may treat as expired, and every inode in its basis is visible or under a recoverable deletion.

Why doesn't the compaction skip the walk when the trash lists no expired deletion? The `active_deletions` group compacts on its own schedule, and its base compaction removes an expired deletion's `listed` row whether or not the other groups have removed the rows that the deletion hides. So the trash can list no expired deletion while the `revisions` group still stores the revisions of one, and a compaction that trusted the trash would keep those rows for good.

Why does the filter stay at 64 MiB, instead of keeping every row above that size? The expected count includes the hidden rows that the compaction would remove, so a compaction that kept every row above the limit would never remove them, and no later compaction could bring the count under the limit. A filter with fewer bits per entry keeps more hidden rows, but it removes some, and each later compaction removes a share of the rest.

Why does the compaction find hidden inodes by reachability, and not by a flag on each row? A tombstone on a directory is one row that hides the whole subtree, so no row below the directory records that the row is hidden. A flag on every row would turn one delete into a write for every descendant, which is what a subtree tombstone avoids.

## Advancing the floor on a schedule

This section describes how the reference server advances each namespace's floor on a schedule, and how it acknowledges a floor that the namespace's writer hasn't.

A retention advance moves the [retention floor](../specs/glossary.md#retention-floor). Without a schedule, an advance runs only when an operator starts one, and a namespace's history keeps growing in between.

A namespace's [retention window](../specs/glossary.md#retention-window) is how long, in milliseconds, the reference server keeps the namespace's history before the server advances the floor past that history. A namespace can have a window of its own, which its manifest stores (`retention_window_ms`). The window is set when the namespace is created, a `retention_window` maintenance run can set, change, or clear it (see [Retention runs](../specs/api.md#retention-runs)), and a fork starts with its source's window. The server has a window too, the top-level `retention_window_ms` setting in its config file (like `full_sweep_interval_ms`), and it applies to every namespace without a window of its own. Each window must be positive when it's set.

The full pass of the maintenance sweep (the pass that lists every namespace, once a day by default) advances the floor of each namespace that isn't deleted, with a cutoff time of the current time minus the namespace's window, or minus the server's window for a namespace that has no window of its own. A namespace with neither window keeps its floor. The full pass uses the same retention advance as the `retention` job, so the cutoff rule in [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor) applies. The advance reads the namespace's window from the current manifest, which it loads anyway to find the cutoff commit, so a host that advances floors on a schedule needs no extra request to read each namespace's window. Every 5 seconds by default, the sweep also runs a "tick", which visits each writer session that the server keeps open (see [Maintenance hosting](maintenance-hosting.md#tick-and-daily-pass)). The ticks don't advance the floor on their own schedule. If a tick is visiting a namespace when the full pass reaches that namespace, the pass doesn't wait for the tick, and the tick makes the pass's advance once its own step finishes. An operator can also advance a floor by hand, with or without a window, through the `retention` job (see [Retention runs](../specs/api.md#retention-runs)) or `loonfs maintenance retention advance`. A server whose `maintenance` mode doesn't run the sweep doesn't advance any floor.

The deletions that an advance passes expire at once, in every view on the new manifest. Their rows can be removed once the writer's log acknowledges the new floor (see [Acknowledging the floor](#acknowledging-the-floor)). When this server has a writer session open for the namespace, the full pass advances the floor in the same process, so the session's position moves onto the advance's manifest, and the session's next WAL object records the new floor. A fold then raises the manifest's acknowledged floor, and the next base compaction of each family group removes the hidden rows.

A namespace that nobody writes to after an advance never acknowledges the new floor on its own, so the full pass acknowledges it. Before a visit's own advance, the pass compares the current manifest's acknowledged floor with its retention floor. If the acknowledged floor is lower, the pass folds the WAL tail, which raises the acknowledged floor if the writer has committed since the advance. If the acknowledged floor is still lower after the fold, the pass acquires the namespace's writer epoch, writes a fence, and folds again, so the fence's basis floor becomes the acknowledged floor. The pass checks the lag before its own advance, so the floor it acknowledges is the one from the previous pass, and a namespace whose writer committed anything in the day between has already acknowledged that floor. So the pass fences only a namespace that nobody has written to since the previous pass. The check doesn't prevent a race: a request that reaches the namespace's writer session while the pass fences it fails with `writer_fenced`, as the first request after the fence does, and the runtime opens a new session for the request after it, as after any takeover (see [Writer ownership](../specs/format.md#61-writer-ownership)).

If an advance fails, the server keeps no retry state for it, even for a writer session that the server keeps open, and the next full pass tries the advance again. So a failed advance delays the floor move, and the expiry of the deletions that the floor would have reached, until the next full pass (a day later, by default). A failed fence or fold is tried again the same way.

For example, with a 30-day window (`retention_window_ms = 2592000000`) and the default daily pass, a deletion stays recoverable for at least 30 days. The first full pass after the deletion is 30 days old moves the floor past the deletion, so the deletion expires between 30 and 31 days after the commit that deleted it. If the namespace is being written to, the writer's next commit acknowledges the new floor, and the next fold and base compaction remove the hidden rows. If nobody writes to the namespace, the following pass, a day later, fences and folds it, and the acknowledged floor follows the floor by about a day. The deleted bytes are reclaimed after the next base compaction and garbage collection pass. (A base compaction runs once enough new metadata builds up above a family group's oldest run, so a namespace that stops writing can keep an expired deletion's rows until the namespace is written again or an operator compacts it.)

These times are measured by commit timestamps, which come from writers' clocks. The cutoff scan stops at the first commit whose timestamp is later than the cutoff, so one commit with a late timestamp keeps the floor below that commit, and the deletions committed after it stay recoverable for longer.

If two servers that maintain one store set different windows, the shorter server window takes effect for each namespace without a window of its own, because the floor never moves back. A namespace's own window is the same for every server, because each server reads it from the namespace's manifest.

Why don't the ticks advance the floor on their own schedule? An advance checks that the manifest's segments exist and then publishes a manifest. A window measured in days doesn't need a finer schedule than the daily pass, and the full pass publishes nothing for a namespace whose floor has already reached its cutoff. For the same reason, the ticks don't retry a failed advance.

Why does the full pass leave its advance to a tick that's visiting the namespace? The tick might be running a long compaction, so the pass can't wait for it. If the pass skipped the advance instead, the window would be a day longer for the busiest namespaces.

Why does the pass check the acknowledged floor before its own advance, and not after? Right after an advance, every namespace with new history has an acknowledged floor below its retention floor, because no WAL object has been written against the new floor yet. Fencing every such namespace would fence every active writer once a day. Checking the previous pass's floor instead leaves a day for the writer's own commits to acknowledge it, so the pass fences only namespaces that were idle for a day, and a request that arrives during the fence fails once, as after any takeover.

## Expired deletions in the API

This section describes how an expired deletion appears to clients.

| Request | What a client sees for an expired deletion |
| --- | --- |
| The trash listing ([`GET /filesystem/trash`](../specs/api.md#66-get-filesystemtrash)) | The listing leaves out every deletion at or below the floor, because its scan starts above the floor. |
| Undelete | An undelete of a deletion at or below the floor returns `not_deleted`. |
| The [change feed](../specs/api.md#611-get-changes) | The feed has no event for an expiry, because nothing is committed when a deletion expires. A client that follows the feed reads the floor from the namespace status, and a deletion at or below `retention_floor_seq` has expired. |
| Reads by path and by inode | A read by path doesn't resolve an inode that an expired deletion hides. A read by inode of any such inode, whether of the inode, its revisions, its attributes, or its content, returns `inode_not_found` from the advance on, as it does for any unknown inode, whatever rights the subject has. |
| Snapshot reads | A snapshot from before the advance still shows the subtree. In an ACL namespace, a read of a hidden inode through the snapshot requires an administrator, and any other subject gets the not-found error that a read of the current state returns. |
| Namespace status | The status reports the floor in `retention_floor_seq`. Every deletion at or below it has expired. |
| Namespace diagnostics | The maintenance diagnostics report `acknowledged_floor_seq` beside `retention_floor_seq`, so an operator can see whether a base compaction can remove the rows that the expired deletions hide. |

Expiry has no route or error code of its own.

Why isn't there an error code for an expired deletion? A client can't do anything different for an expired deletion than for a deletion that was already restored, because in both cases there's nothing to recover. A client that needs to tell the two cases apart compares the deletion's sequence with the floor in the namespace status.

## Spec and guide sections

These sections of the specs and guides describe this design.

| Section | What it describes |
| --- | --- |
| Format, [File and subtree deletion](../specs/format.md#16-file-and-subtree-deletion) | The `set` and `revoke` events. A deletion has expired when it's at or below the floor, and it's recoverable when its latest event is `set` and it's above the floor. The trash listing starts above the floor, and undelete validation returns `not_deleted` at or below it. |
| Format, [Access rows](../specs/format.md#18-access-rows) | The access walk stops at the root of an expired deletion, and the rights on an item under a recoverable deletion are evaluated through the deletion's saved parent. |
| Format, [Paths, listings, and revision reads](../specs/format.md#44-paths-listings-and-revision-reads) | The ancestry walk before a read by inode ID, and the `inode_not_found` error for an inode that an expired deletion hides or whose chain to the root is broken. |
| Format, [Visible metadata](../specs/format.md#43-visible-metadata) | A tombstone whose latest event is `set` hides an inode. |
| Format, [Writer ownership](../specs/format.md#61-writer-ownership) and [Publication and group commit](../specs/format.md#63-publication-and-group-commit) | A fence and a WAL object record the basis floor, and a maintainer can acquire the writer epoch to write a fence. |
| Format, [Publishing a materialized file set](../specs/format.md#72-publishing-a-materialized-file-set) | A fold raises the acknowledged floor, a successor never lowers it, and a fork's first manifest acknowledges the fork point. |
| Format, [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor) | The floor expires deletions, the writer's log acknowledges it, and a maintainer acknowledges it for an idle namespace. |
| Format, [Row retention during a base compaction](../specs/format.md#103-row-retention-during-a-base-compaction) | The family rows in the table above, and the kept inode filter beside the layout filter, with its limits. A base compaction removes the rows that a deletion hides only once the deletion is at or below the acknowledged floor. |
| Format, [Control and manifest payloads](../specs/format.md#a4-control-and-manifest-payloads) and [WAL records](../specs/format.md#a5-wal-records) | The `acknowledged_floor_seq` and `basis_floor_seq` fields. |
| The glossary's [tombstone](../specs/glossary.md#tombstone), [retention floor](../specs/glossary.md#retention-floor), [acknowledged floor](../specs/glossary.md#acknowledged-floor), and [fence](../specs/glossary.md#fence) entries | Expired deletions and the acknowledgment. |
| API, [Retention runs](../specs/api.md#retention-runs) | The floor bounds deletions, and a deletion leaves the trash when the floor reaches it. |
| API, [`GET /filesystem/trash`](../specs/api.md#66-get-filesystemtrash) | The listing starts its scan above the floor. |
| API, [`GET /filesystem/content` and `GET /inodes/{inode_id}/content`](../specs/api.md#67-get-filesystemcontent-and-get-inodesinode_idcontent) and [Download transport](../specs/api.md#610-download-transport) | The inode-addressed read and download routes return `inode_not_found` for an inode that an expired deletion hides, even while its revision rows are kept. |
| API, [Authorization in ACL namespaces](../specs/api.md#authorization-in-acl-namespaces) | A snapshot read of an inode that an expired deletion hides at the head requires an administrator. |
| API, [Deleting, retaining, and reclaiming](../specs/api.md#deleting-retaining-and-reclaiming) | The reference server's sweep advances and acknowledges floors under `retention_window_ms`, and deleting a tree reclaims its content once the deletion expires. |
| The [self-hosting guide](../../crates/loonfs-server/docs/self-hosting.md#background-maintenance) and the example configs | The `retention_window_ms` setting, and how the full pass advances floors and tries a failed advance again. |
| [Maintenance hosting](maintenance-hosting.md#tick-and-daily-pass) | The full pass's retention advance and acknowledgment, and the visit that makes the advance when the pass finds the namespace busy. |

## Alternatives considered

Keep a second floor for deletions, with its own advance and its own server setting. A fork could then keep its inherited trash, and an operator could keep deletions longer than revisions. But the second floor is a second mechanism that every compaction rule, every status report, and every advance has to cover, and an operator has to work out how the two settings interact.

Let a base compaction, too, compare a deletion with the retention floor, with no acknowledgment. The floor moves in a maintainer publication, but an undelete is a writer commit, and LoonFS can't order the two against each other. For example, an undelete of a deletion at sequence 10 can be validated while the writer's basis has a floor of 5 and committed at sequence 22. The maintainer can move the floor to 20 in between, and a base compaction can then remove the subtree's rows, so the fold of commit 22 binds an item whose rows are gone.

Expire each deletion with an event that the writer commits. After each advance, the writer scans the trash for the deletions at or below the new floor and commits an `expire` event for each one, and a base compaction removes a deletion's rows once its latest event is `expire`. The WAL orders each expiry against every undelete, so this design needs no acknowledged floor. But the writer commits one event for each deletion, and a fork's first writer session has to expire every inherited deletion before it admits its first request, which costs one commit for each 1,000 inherited deletions. The scan loop needs its own failure handling and backoff, the change feed reports every expiry, the `active_deletions` family stores a `removed` row for each one, and a namespace reclaims nothing until a writer runs in it. A basis floor in each WAL object acknowledges every deletion at or below the floor at once, with one field.

Record the time of each advance in the manifest, and let a base compaction remove a deletion's rows once the floor has been above the deletion for longer than a writer can trust a basis, plus a clock allowance. After that time, no writer validates an undelete against the old floor. But an undelete validated against the old floor can stay unfolded in the WAL for any length of time, so the compaction also has to know that every WAL object from before that time has been folded, which the manifest's time alone doesn't tell it. The rule also makes row retention depend on the relative clock error between the maintainer and every writer, where it otherwise depends only on sequence numbers and conditional writes.

Have the writer advance the floor itself, by folding its tail and setting the floor in one manifest. That closes the race between a floor move and an undelete within one process. But it moves the retention advance to the writer role, the writer has to validate undeletes against the floor that it's about to publish, and no process advances the floor of a namespace whose writer isn't running.

Expire a deletion inside compaction once its deletion time (`deleted_at_ms`) is older than a configured age. Compaction then reads a wall clock and the server's configuration, two compactions of the same input can keep different rows, and the floor stops being the one bound on how much history a namespace keeps.

Advance the floor on every sweep visit as well as on the full pass. Each advance checks the manifest's segments and publishes a manifest, so every active namespace costs a publication and segment checks every few minutes. With a window of days, deletions don't expire any sooner in a way that a client could notice.
