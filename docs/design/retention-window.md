# Retention window

This note describes how the [retention floor](../specs/glossary.md#retention-floor) expires deletions, why only the holder of the namespace's writer epoch advances the floor, what a base compaction removes for an expired deletion, and how the reference server advances the floor on a schedule.

## History that grows without bound

This section describes why LoonFS needs to bound how long a namespace keeps deleted items and old revisions.

A restore brings back a deleted item with its revisions, its attributes, and everything below it, so LoonFS can't reclaim any of those rows or their content while the deletion can be undone. A deleted file keeps its newest revision for this reason, and each recoverable deletion has an entry in the namespace's "trash" (the `active_deletions` family).

Two gaps would let this history grow without bound. If the [retention floor](../specs/glossary.md#retention-floor) didn't cover deletions, advancing it wouldn't let LoonFS reclaim anything that a deletion hides, so a namespace that deletes files every day would keep every byte it ever deleted. If no process advanced the floor on a schedule, revision history, trash entries, and the content behind them would grow until an operator ran the `retention` job by hand.

## Recoverable and expired deletions

This section describes how long a deletion stays recoverable.

LoonFS can't reclaim what a deletion hides while the deletion can still be restored, so each deletion needs a point in the namespace's history where it stops being recoverable.

The [retention floor](../specs/glossary.md#retention-floor) bounds every kind of history that a namespace keeps: incremental replay, superseded bindings, file revisions, attribute and access states, and commit receipts (see [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor)). The floor bounds recoverable deletions in the same way. Each deletion has a deletion sequence (`deletion_seq`), which is the sequence number of the commit that deleted the item. A deletion has "expired" in a [read view](../specs/glossary.md#read-view) when its deletion sequence is at or below the retention floor of the view's manifest, and it's "recoverable" when its tombstone's latest event is `set` and its deletion sequence is above that floor. Nothing is committed when a deletion expires. A retention advance moves the floor, and every view on the advance's manifest, or on a later one, treats the deletions that the floor passed as expired.

An expired deletion can't be restored, and the trash listing leaves it out. No read of the namespace's current state can reach the inodes that the deletion hides, and the next base compaction of each family group removes their rows, so LoonFS can reclaim their content, as described in [Removing a deletion's rows](#removing-a-deletions-rows). That's safe because only the holder of the writer epoch advances the floor, as described in [Who advances the floor](#who-advances-the-floor). (A snapshot from before the advance still shows them, as described in [Reads of an expired subtree](#reads-of-an-expired-subtree).) Until the floor reaches a deletion, a restore can bring back the deleted item as described in [File and subtree deletion](../specs/format.md#16-file-and-subtree-deletion). Deletions don't have a floor of their own, and expiry doesn't depend on a clock.

For example, suppose a file is deleted at sequence 40 while the floor is at 35. The file is in the trash, and a restore can bring the file back. A retention advance then moves the floor to 50. In every view on the advance's manifest or a later one, the deletion has expired, so the trash listing leaves it out and a restore of it returns `not_deleted`.

Restore validation compares the deletion's sequence with the floor of the writer's basis, which is the manifest that the [writer session](../specs/glossary.md#writer-session) plans against. The trash listing scans `active_deletions` from above the floor of the view's manifest. The family's keys begin with the deletion sequence, so the floor is where the scan starts, not a filter on its results, and a page costs the same whatever the floor is.

Deletions can be nested. If `/old/draft.txt` is deleted on its own and `/old` is deleted later, each deletion has its own tombstone and its own trash entry, and restoring `/old` doesn't bring back `draft.txt`. An item can't be deleted while a deletion above it hides it, so an inner deletion is always older than the deletion around it, and the floor reaches the inner deletion first, or in the same advance. So no recoverable deletion is ever inside an expired one.

A [fork](../specs/glossary.md#fork)'s floor starts at its fork point, so every deletion that a fork inherits is at or below the fork's floor, and it has expired in the fork from the fork's creation. The fork's trash listing starts its scan above the fork point, a restore of an inherited deletion returns `not_deleted`, and no writer has to run in the fork for either. Every commit in the fork is validated against a fork manifest, whose floor is at least the fork point, so no restore of an inherited deletion can be committed in the fork, and a base compaction in the fork can remove the rows that an inherited deletion hides. The source namespace keeps its own trash under its own floor, so an inherited deletion can still be restored in the source.

For example, suppose a namespace's trash lists a deletion at sequence 80, and the namespace is forked at sequence 100. The fork's floor starts at 100, so the fork's trash listing starts its scan above 100 and doesn't list the deletion, and a restore of the deletion in the fork returns `not_deleted`. In the source, the deletion stays recoverable under the source's own floor.

Why don't deletions have a floor of their own? The floor is the namespace's one bound on how far back it keeps history, and deletions are part of that history. With one floor, a client can rely on one number for how far back it can restore anything, whether that's an old revision or a deleted file.

Why does a fork lose its inherited trash? A fork's history starts at its fork point, so the fork's floor can't be lower than the fork point. Keeping inherited deletions recoverable in the fork would take a second floor (see [Alternatives considered](#alternatives-considered)).

## Who advances the floor

This section describes which process may advance the retention floor, and why that rule lets a base compaction remove an expired deletion's rows as soon as the floor passes the deletion.

A retention advance publishes a manifest, and a restore is a commit in a WAL object. If any process could publish an advance at any time, nothing would order the two. A writer validates each commit against its basis, the manifest it plans against, and it keeps that basis until its next successor check (see [Publication and group commit](../specs/format.md#63-publication-and-group-commit)). For example, a writer could validate a restore of a deletion at sequence 10 while its basis has a floor of 5, and commit the restore at sequence 22, while another process moves the floor to 20 in between. A base compaction over the advance's manifest would find a deletion at or below the floor whose latest event is `set` and remove the subtree's rows, and the fold of commit 22 would then bind an item whose rows are gone.

So LoonFS lets only the holder of the namespace's [writer epoch](../specs/glossary.md#writer-epoch) advance the floor. The holder is the namespace's [writer session](../specs/glossary.md#writer-session), or a session that a process acquires for the advance. The holder's tip is the [WAL tip](../specs/glossary.md#wal-tip) as the holder knows it, the highest WAL number that it has written or found. To advance the floor, the holder:

1. Acquires the writer epoch, unless it has the epoch already. It publishes a claim manifest and writes a [fence](../specs/glossary.md#fence), so every WAL object of an older epoch has a lower number than the fence.
2. Folds its WAL tail through its tip, which is at or above its fence and every WAL object it wrote.
3. Publishes the new floor under its writer epoch, over the fold's manifest. If another manifest takes the number first, the holder loads that manifest. A manifest with another writer epoch means that another writer has taken the namespace over, so the holder publishes nothing and returns `writer_fenced`. A manifest with the holder's epoch is a fold, a compaction, a retention window change, or an extension's publication, which folds at least as much of the WAL, so the holder publishes the floor over it.

A writer session runs the advance as one item of its publication queue, after the commits that it admitted before the advance and before the ones it admits after. A commit admitted before the advance is published before step 1, so step 2 folds it. A commit admitted after the advance is validated against the new floor, because the session validates each batch when it publishes the batch. So an undelete queued before an advance restores its deletion even when the advance expires the deletion, and an undelete queued after the advance returns `not_deleted`.

After these steps, the advance's manifest keeps an invariant. No WAL object after the manifest's `folded_wal_no` contains a restore of a deletion that the manifest's floor expired and whose latest event in the manifest is still `set`, and no WAL object written later will. A restore of such a deletion was validated against the old floor, and one of three writers wrote it:

- The holder wrote it before the advance, so step 2 folded it, and the deletion's latest event in the manifest is the restore's `revoke`. A restore that the holder publishes after step 3 is validated against the new floor and refused.
- A writer of an older epoch wrote it, below the holder's fence, so step 2 folded it too.
- A writer of a newer epoch wrote it. If that writer claimed the namespace before the floor's put, the put collides, and the holder publishes nothing. If it claimed the namespace after the put, it validates against the new floor and refuses the restore.

Every later manifest keeps the floor or raises it, and it folds at least as much of the WAL. So no commit, folded or not, can restore a deletion that has expired in a manifest, and a base compaction over that manifest removes the rows that the deletion hides, as described in [Removing a deletion's rows](#removing-a-deletions-rows). The same argument covers a restore of an old file revision by reference. A restore validated against the old floor is folded before the advance's manifest, so the restore's own revision row refers to the content chain when a base compaction drops the revision that the restore copied. (The writer still refuses to restore a revision at or below its basis's floor unless the revision is the file's newest at or below that floor.)

For example, suppose the floor is 5, a writer session has written WAL objects through 30, and its queue contains an undelete of a deletion at sequence 10, then an advance to 20, then a second undelete of the same deletion. The session commits the first undelete in WAL object 31, folds through 31, and publishes floor 20. The deletion's latest event is now `revoke`, so a base compaction keeps the restored item's rows. The second undelete is validated against floor 20 and returns `not_deleted`.

The fence has to come before the fold, because a writer of an older epoch can commit until the holder's fence exists. If the holder folded first and wrote its fence after, an older writer's restore could be written between the fold and the fence, above the fold's `folded_wal_no` and validated against the old floor, and a base compaction over the advance's manifest would then remove the rows of the item that the restore brought back.

A process that advances the floor without a writer session of its own, such as the reference server for a namespace that it has no session for, takes the writer epoch in step 1, the same way a new writer session starts (see [Writer ownership](../specs/format.md#61-writer-ownership)), and commits nothing after its fence. The namespace's writer session in any other process is then fenced, and it returns `writer_fenced` to its next request, as after any takeover. An advance whose floor wouldn't move publishes nothing and takes no epoch.

Why does the advance need the writer epoch, and not just a fold before the floor? A fold covers the WAL up to the tip that it reads, but without the epoch, a writer could commit a restore validated against the old floor right after the fold and before the floor's put. The holder of the epoch is the one process whose own commits come after its advance in its queue, and whose fence stops every older writer.

Why does that cost a process without a session a takeover? To take the epoch, a process claims it, so a writer session in another process has to start again with another claim and fence (about 200 ms), and the request that finds the fence fails with `writer_fenced`. The reference server advances each namespace's floor once for each sweep, in the process that has the namespace's writer session when the server has one, so a writer pays this cost only when another process advances its namespace's floor, at most once for each sweep that moves the floor (once a day with the default sweep interval).

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

Why does a read compare the deletion with the floor of its own view, when another view can have a newer floor? Each view reads the segments that its own manifest lists, and a compaction removes a deletion's rows only in a manifest whose retention floor is at or above the deletion. So in every view, a deletion whose rows a compaction may have removed is at or below the view's floor, and the view already hides it.

Why does LoonFS refuse a read when the chain is broken? Every live inode has bound bindings up to the root inode, and every deleted inode has them up to its deletion's root, which has a tombstone. A chain that breaks before it reaches either one belongs to an inode that the namespace has no way to show. Base compactions remove an expired subtree's rows one family group at a time, so the bindings that lead up to the expired root can be gone while the revisions below it are still stored. A walk from one of those revisions' inodes can then no longer reach the tombstone, so LoonFS refuses the read because the chain is broken.

Why does a snapshot read of an expired subtree require an administrator? LoonFS evaluates a snapshot read's rights on the access rows at the current head, and once a deletion expires, base compactions remove the head's rows under it one family group at a time. So at the head, a boundary row can be gone while a grant above it is still stored, and a subject that the boundary excluded could then inherit that grant. An administrator's rights don't depend on those rows.

## Removing a deletion's rows

This section describes the rows that a base compaction removes for an expired deletion.

A deletion expires when the floor reaches it, but the rows that the deletion hides stay in the namespace's metadata until a compaction removes them.

A "base compaction" rewrites a window of runs that includes a [family group](../specs/glossary.md#family-group)'s oldest run, so it can drop rows under the retention rules (see [Compaction windows](../specs/format.md#102-compaction-windows)). A deletion has expired in the compaction's [metadata basis](../specs/glossary.md#metadata-basis) when its deletion sequence is at or below the basis manifest's retention floor. No commit can restore it then (see [Who advances the floor](#who-advances-the-floor)). For each expired deletion whose latest tombstone event in the basis is `set`, the compaction removes the rows of every inode that the deletion hides, which are the deletion's root and every descendant of the root in the basis. For example, if the directory `/old` (inode 7) contains `a.txt` (inode 8), and the deletion of `/old` has expired, the deletion hides inodes 7 and 8.

| Family | Rows a base compaction removes for an expired deletion |
| --- | --- |
| `active_deletions` | The root's `listed` row. |
| `inodes`, `revisions`, `attributes`, `access` | Every row of a hidden inode. |
| `direntry_binds`, `direntry_child_binds` | Every edge whose parent is a hidden inode. The binding rule in [Row retention during a base compaction](../specs/format.md#103-row-retention-during-a-base-compaction) covers the root's own edge. The newest version of that edge at or below the floor is unbound, so the compaction drops that version together with every older version it hides. |
| `tombstones` | None. Every `set` and `revoke` event stays. |
| `content_layouts` | The chains of the removed revisions. The basis revision filter drops them as it does for any removed revision. |

A delta-only compaction (one whose window doesn't include the group's oldest run) removes none of these rows, because it keeps every history row (see [Compaction windows](../specs/format.md#102-compaction-windows)).

Reclaiming a deletion's content takes three steps:

1. A retention advance moves the floor past the deletion, and the deletion expires.
2. A base compaction of each family group removes the rows that the deletion hides, and a base compaction of `content_layouts` that runs after the revisions are gone drops their chains.
3. [Garbage collection](../specs/glossary.md#garbage-collection-gc) deletes the content objects once neither the current manifest nor a pinned manifest refers to them and they're older than the [grace window](../specs/glossary.md#grace-window) (see [Content roots](../specs/format.md#119-content-roots)). Content that a kept revision also refers to stays.

Why can a base compaction remove every row of a hidden inode, and not just the older rows that the usual retention rules drop? The deletion is at or below the retention floor, and the tombstone hides those inodes at every sequence from the deletion on, so they're hidden in every view that compaction has to preserve. Every row of a hidden inode is also at or below the floor, because LoonFS commits no change to a hidden inode. (If an inode was restored and then deleted again, its latest deletion is the expired one, so its rows are older than that deletion too.)

Why keep the tombstone events? Family groups are compacted at different times, so for a while some groups have removed a hidden inode's rows and others haven't. The ancestry walk in [Reads of an expired subtree](#reads-of-an-expired-subtree) stops at the tombstone, so the tombstone keeps those inodes unreadable in between. Once the bindings group has removed the edges that lead up to the tombstone, the broken chain keeps them unreadable instead, so a read by inode ID can't find a partly removed subtree. The cost is one small tombstone row for each deletion and each restore.

## The kept inode filter

This section describes how a base compaction finds the inodes that an expired deletion hides.

A compaction tests one row at a time, so it needs a way to check whether each row's inode is hidden, and a hidden subtree can have more inodes than fit in memory. Before a base compaction rewrites the `inodes`, `revisions`, `attributes`, or `access` group, or the bindings group (`direntry_binds` and `direntry_child_binds`), it builds a "kept inode filter". That filter is a Bloom filter that contains every inode that's visible or under a recoverable deletion in the basis. Every base compaction of these groups builds the filter before it rewrites its window.

To build the filter, the compaction:

1. Walks the basis's directory tree at the basis's head, starting at the root inode, depth first and with one cursor for each level, the way a recursive listing does. It reads each directory's children from the slot index (`direntry_binds`), follows each bound edge, and inserts every inode it reaches.
2. Walks the subtree of every recoverable deletion the same way, starting at the deletion's root. It reads those roots from the basis's `listed` rows in `active_deletions` that have no `removed` row and whose deletion sequence is above the basis manifest's retention floor.

Deleting an item unbinds it, so the walk from the root inode doesn't reach a deleted subtree. An expired deletion isn't a starting point for the second walk, and its root is unbound, so neither walk inserts the inodes that the expired deletion hides. An inner deletion is older than the deletion around it (see [Recoverable and expired deletions](#recoverable-and-expired-deletions)), so no recoverable deletion is inside an expired one, and the two walks reach every inode that a view can show, including every ancestor through which LoonFS evaluates the rights on an item under a recoverable deletion.

The compaction removes a row whose inode isn't in the filter, and applies the usual retention rules to a row whose inode is in the filter. The compaction tests a binding row by its parent inode. The two binding indexes are rewritten in one compaction against the same filter, so the compaction keeps or removes each binding event the same way in both indexes.

A Bloom filter can report an inode that was never inserted (a false positive), but it never misses an inode that was inserted. A false positive keeps a hidden inode's rows for one compaction, and a later compaction uses a different seed, so the later compaction removes those rows. The layout filter in [Row retention during a base compaction](../specs/format.md#103-row-retention-during-a-base-compaction) errs the same way, and the kept inode filter follows the layout filter's rules: xxh64 double hashing, a seed from the output run number, and ten bits per expected entry, up to `MAX_BLOOM_FILTER_BYTES` (64 MiB). The expected number of entries is the number of rows in the basis's `inodes` family (the sum of its segments' `row_count`), which shrinks as base compactions remove hidden inodes. Ten bits per entry fit about 53 million entries in 64 MiB. Above that, the filter stays at 64 MiB, so it has fewer bits per entry and uses fewer hash positions, and it reports more false positives. For example, a filter for 100 million entries has about five bits per entry and keeps about one hidden row in thirteen, and the compactions after it remove a share of the rest each time.

If the walk would go more than `MAX_KEPT_INODE_WALK_DEPTH` (4,096) levels deep, the compaction keeps every row it would have tested, removes no hidden rows in that step, and logs that it reached the limit. The depth limit applies to the cursors that the walk keeps at once, one for each level. A move can nest a tree deeper than the longest path that a request accepts. Each level of the walk keeps a directory's resume key of up to about 1.6 KiB, so 4,096 levels take at most about 7 MiB.

The walk reads the slot-index rows of every directory it reaches, so each base compaction of these groups costs one more read of the bindings of the visible and recoverable tree.

A base compaction over a manifest whose retention floor is still 0 doesn't build the filter. It keeps every row and doesn't walk the tree, so the base compactions of a namespace whose floor has never moved skip that extra read.

Why does the compaction build a filter from a walk, and not an exact set of hidden inodes? A hidden subtree can have more inodes than fit in memory, and so can the visible tree. A filter's size is set before the walk starts, and a filter with no false negatives errs toward keeping a row, so the compaction stays inside its memory limit without removing a row that a view needs.

Why can a base compaction over a floor of 0 skip the walk? A deletion's sequence is at least 1, so a deletion has expired in the compaction's basis only when the floor is at least 1, and the floor never decreases from one manifest to the next. So a manifest whose floor is 0 contains no expired deletion, and every inode in its basis is visible or under a recoverable deletion.

Why doesn't the compaction skip the walk when the trash stores no expired deletion? The `active_deletions` group compacts on its own schedule, and its base compaction removes an expired deletion's `listed` row whether or not the other groups have removed the rows that the deletion hides. So the trash can store no expired deletion while the `revisions` group still stores the revisions of one, and a compaction that trusted the trash would keep those rows for good.

Why does the filter stay at 64 MiB, instead of keeping every row above that size? The expected count includes the hidden rows that the compaction would remove, so a compaction that kept every row above the limit would never remove them, and no later compaction could bring the count under the limit. A filter with fewer bits per entry keeps more hidden rows, but it removes some, and each later compaction removes a share of the rest.

Why does the compaction find hidden inodes by reachability, and not by a flag on each row? A tombstone on a directory is one row that hides the whole subtree, so no row below the directory records that the row is hidden. A flag on every row would turn one delete into a write for every descendant, which is what a subtree tombstone avoids.

## Advancing the floor on a schedule

This section describes how the reference server advances each namespace's floor on a schedule.

A retention advance moves the [retention floor](../specs/glossary.md#retention-floor). Without a schedule, an advance runs only when an operator starts one, and a namespace's history keeps growing in between.

A namespace's [retention window](../specs/glossary.md#retention-window) is how long, in milliseconds, the reference server keeps the namespace's history before the server advances the floor past that history. The namespace's manifest stores the window (`retention_window_ms`), and the server doesn't read the window from anywhere else. The window is set when the namespace is created, a `retention_window` maintenance run can set, change, or clear it (see [Retention jobs](../specs/api.md#retention-jobs)), and a fork starts with its source's window. The top-level `retention_window_ms` setting in the server's config file (like `sweep_interval_ms`) is a default window. When a create request doesn't include a window, the server records the setting in the namespace's first manifest. Changing the setting later doesn't change the window of a namespace that already exists. For example, if the setting is 30 days when a namespace is created without a window, the namespace's window stays 30 days after the setting changes to 7 days. Each window must be positive when it's set.

A [sweep](../specs/glossary.md#sweep) (the scheduler's listing of every namespace, once a day by default) advances the floor of each namespace that isn't deleted, with a cutoff time of the current time minus the namespace's window. The sweep doesn't advance the floor of a namespace without a window, even when the server has a `retention_window_ms` setting. The sweep uses the same retention advance as the `retention` job, so the cutoff rule in [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor) applies. The advance reads the namespace's window from the current manifest, which it loads anyway to find the cutoff commit, so a host that advances floors on a schedule needs no extra request to read each namespace's window. Every maintenance interval (5 seconds by default), the scheduler also runs the due steps of each writer session that the server keeps open (see [Maintenance hosting](maintenance-hosting.md)). Those steps don't advance the floor on their own schedule. If a step of a namespace is running when the sweep reaches that namespace, the sweep doesn't wait for the step, and the step makes the sweep's advance once its own work finishes. An operator can also advance a floor by hand, with or without a window, through the `retention` job (see [Retention jobs](../specs/api.md#retention-jobs)) or `loonfs maintenance retention advance`. A server whose `maintenance` mode doesn't run the scheduler doesn't advance any floor.

The deletions that an advance passes expire at once, in every view on the new manifest, and the next base compaction of each family group removes the rows that they hide. The sweep's advance runs in the namespace's writer session in this server (see [Who advances the floor](#who-advances-the-floor)). When the server has a session for the namespace, that session folds its tail and publishes the floor under its own epoch, and its position moves onto the advance's manifest. When the server has no session for the namespace, the advance opens one, which claims the writer epoch, writes a fence, folds, and publishes the floor, and the session ends when the advance returns. A writer session for the namespace in another process is then fenced, as after any takeover. The advance plans the new floor before it opens a session, so a namespace whose floor wouldn't move costs one load of its current manifest, and nothing else.

If an advance fails, the server keeps no retry state for it, even for a writer session that the server keeps open, and the next sweep tries the advance again. So a failed advance delays the floor move, and the expiry of the deletions that the floor would have reached, until the next sweep (a day later, by default). A failed claim, fence, or fold is part of the advance, so it's tried again the same way.

For example, with a 30-day window (`retention_window_ms = 2592000000`) and the default sweep interval of a day, a deletion stays recoverable for at least 30 days. The first sweep after the deletion is 30 days old moves the floor past the deletion, so the deletion expires between 30 and 31 days after the commit that deleted it. The advance folds the namespace's tail before it moves the floor, so the next base compaction of each family group removes the hidden rows, and the deleted bytes are reclaimed after that compaction and the next collection. (A base compaction runs once enough new metadata builds up above a family group's oldest run, so a namespace that stops writing can keep an expired deletion's rows until the namespace is written again or an operator compacts it.)

These times are measured by commit timestamps, which come from writers' clocks. The cutoff scan stops at the first commit whose timestamp is later than the cutoff, so one commit with a late timestamp keeps the floor below that commit, and the deletions committed after it stay recoverable for longer.

A store has one maintaining server. An advance takes the writer epoch of a namespace that its server has no session for, so if two servers swept one store, each server's sweep would take the writer epoch of each namespace from the other once a day, and each server's writer sessions would be fenced daily. A namespace's window is the same for every process that advances its floor, because each process reads the window from the namespace's manifest.

Why don't the steps between sweeps advance the floor on their own schedule? An advance checks that the manifest's segments exist and then publishes a manifest. A window measured in days doesn't need a finer schedule than the daily sweep, and the sweep publishes nothing for a namespace whose floor has already reached its cutoff. For the same reason, the steps between sweeps don't retry a failed advance.

Why does the sweep leave its advance to a step that's running for the namespace? The step might be running a long compaction, so the sweep can't wait for it. If the sweep skipped the advance instead, the window would be a day longer for the busiest namespaces.

## Expired deletions in the API

This section describes how an expired deletion appears to clients.

| Request | What a client sees for an expired deletion |
| --- | --- |
| The trash listing ([`GET /filesystem/trash`](../specs/api.md#66-get-filesystemtrash)) | The listing leaves out every deletion at or below the floor, because its scan starts above the floor. |
| Restore (`undelete`) | A restore of a deletion at or below the floor returns `not_deleted`. |
| The [change feed](../specs/api.md#611-get-changes) | The feed has no event for an expiry, because nothing is committed when a deletion expires. A client that follows the feed reads the floor from the namespace status, and a deletion at or below `retention_floor_seq` has expired. |
| Reads by path and by inode | A read by path doesn't resolve an inode that an expired deletion hides. A read by inode of any such inode, whether of the inode, its revisions, its attributes, or its content, returns `inode_not_found` from the advance on, as it does for any unknown inode, whatever rights the subject has. |
| Snapshot reads | A snapshot from before the advance still shows the subtree. In an ACL namespace, a read of a hidden inode through the snapshot requires an administrator, and any other subject gets the not-found error that a read of the current state returns. |
| Namespace status | The status reports the floor in `retention_floor_seq`. Every deletion at or below it has expired. |

Expiry has no route or error code of its own.

Why isn't there an error code for an expired deletion? A client can't do anything different for an expired deletion than for a deletion that was already restored, because in both cases there's nothing to restore. A client that needs to tell the two cases apart compares the deletion's sequence with the floor in the namespace status.

## Spec and guide sections

These sections of the specs and guides describe this design.

| Section | What it describes |
| --- | --- |
| Format, [File and subtree deletion](../specs/format.md#16-file-and-subtree-deletion) | The `set` and `revoke` events. A deletion has expired when it's at or below the floor, and it's recoverable when its latest event is `set` and it's above the floor. The trash listing starts above the floor, and restore validation returns `not_deleted` at or below it. |
| Format, [Access rows](../specs/format.md#18-access-rows) | The access walk stops at the root of an expired deletion, and the rights on an item under a recoverable deletion are evaluated through the deletion's saved parent. |
| Format, [Paths, listings, and revision reads](../specs/format.md#44-paths-listings-and-revision-reads) | The ancestry walk before a read by inode ID, and the `inode_not_found` error for an inode that an expired deletion hides or whose chain to the root is broken. |
| Format, [Visible metadata](../specs/format.md#43-visible-metadata) | A tombstone whose latest event is `set` hides an inode. |
| Format, [Writer ownership](../specs/format.md#61-writer-ownership) | A process that advances the floor without a writer session acquires the writer epoch with a claim and a fence. |
| Format, [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor) | The floor expires deletions, only the holder of the writer epoch advances it, the three steps of an advance, and the invariant that lets a compaction remove an expired deletion's rows. |
| Format, [Row retention during a base compaction](../specs/format.md#103-row-retention-during-a-base-compaction) | The family rows in the table above, and the kept inode filter beside the layout filter, with its limits. |
| The glossary's [tombstone](../specs/glossary.md#tombstone), [retention floor](../specs/glossary.md#retention-floor), and [maintainer](../specs/glossary.md#maintainer) entries | Expired deletions, and the writer epoch that an advance needs. |
| API, [Retention jobs](../specs/api.md#retention-jobs) | The floor bounds deletions, and a deletion leaves the trash when the floor reaches it. |
| API, [`GET /filesystem/trash`](../specs/api.md#66-get-filesystemtrash) | The listing starts its scan above the floor. |
| API, [`GET /filesystem/content` and `GET /inodes/{inode_id}/content`](../specs/api.md#67-get-filesystemcontent-and-get-inodesinode_idcontent) and [Download transport](../specs/api.md#610-download-transport) | The inode-addressed read and download routes return `inode_not_found` for an inode that an expired deletion hides, even while its revision rows are kept. |
| API, [Authorization in ACL namespaces](../specs/api.md#authorization-in-acl-namespaces) | A snapshot read of an inode that an expired deletion hides at the head requires an administrator. |
| API, [Deleting, retaining, and reclaiming](../specs/api.md#deleting-retaining-and-reclaiming) | The reference server's sweep advances each namespace's floor under the namespace's window, the server records its `retention_window_ms` setting as the window of a namespace created without a window, and deleting a tree reclaims its content once the deletion expires. |
| The [self-hosting guide](../../crates/loonfs-server/docs/self-hosting.md#background-maintenance) and the example configs | The `retention_window_ms` setting, which the server records as the window of each namespace created without a window, and how the sweep advances floors and tries a failed advance again. |
| [Maintenance hosting](maintenance-hosting.md) | The sweep's retention advance through the namespace's writer session, and the step that makes the advance when the sweep finds the namespace busy. |

## Alternatives considered

Keep a second floor for deletions, with its own advance and its own server setting. A fork could then keep its inherited trash, and an operator could keep deletions longer than revisions. But the second floor is a second mechanism that every compaction rule, every status report, and every advance has to cover, and an operator has to work out how the two settings interact.

Let any maintainer advance the floor at any time, and let a base compaction compare a deletion with the retention floor. The floor would then move in a publication that nothing orders against a writer's restore. For example, a restore of a deletion at sequence 10 can be validated while the writer's basis has a floor of 5 and committed at sequence 22. The maintainer can move the floor to 20 in between, and a base compaction can then remove the subtree's rows, so the fold of commit 22 binds an item whose rows are gone.

Let any maintainer advance the floor without the writer epoch, and record which floor each commit was validated against. Each WAL object would record the retention floor of the manifest that its writer validated it against, each manifest would record the highest of those floors among the WAL objects folded into it, and a base compaction would remove a deletion's rows only at or below that highest floor. But it costs two durable fields, a second floor in every pruning rule (revisions as well as deletions), and an acknowledgment step, because a namespace that nobody writes to never folds a WAL object validated against the new floor, so a maintainer has to fence its quiet writer once a day anyway. Its correctness also rests on the recorded floors never decreasing along the log, a property of every writer and every takeover that each change to either has to keep.

Expire each deletion with an event that the writer commits. After each advance, the writer scans the trash for the deletions at or below the new floor and commits an `expire` event for each one, and a base compaction removes a deletion's rows once its latest event is `expire`. The WAL orders each expiry against every restore. But the writer commits one event for each deletion, and a fork's first writer session has to expire every inherited deletion before it admits its first request, which costs one commit for each 1,000 inherited deletions. The scan loop needs its own failure handling and backoff, the change feed reports every expiry, the `active_deletions` family stores a `removed` row for each one, and a namespace reclaims nothing until a writer runs in it. An advance by the holder of the writer epoch, after a fold, expires every deletion at or below the floor in one manifest.

Record the time of each advance in the manifest, and let a base compaction remove a deletion's rows once the floor has been above the deletion for longer than a writer can trust a basis, plus a clock allowance. After that time, no writer validates a restore against the old floor. But a restore validated against the old floor can stay unfolded in the WAL for any length of time, so the compaction also has to know that every WAL object from before that time has been folded, which the manifest's time alone doesn't tell it. The rule also makes row retention depend on the relative clock error between the maintainer and every writer, where it otherwise depends only on sequence numbers and conditional writes.

Expire a deletion inside compaction once its deletion time (`deleted_at_ms`) is older than a configured age. Compaction then reads a wall clock and the server's configuration, two compactions of the same input can keep different rows, and the floor stops being the one bound on how much history a namespace keeps.

Advance the floor in every step that the scheduler runs, as well as in the sweep. Each advance checks the manifest's segments and publishes a manifest, so every active namespace costs a publication and segment checks every few minutes. With a window of days, deletions don't expire any sooner in a way that a client could notice.

Let a maintainer that advances floors on a schedule have a window of its own, and use that window for each namespace whose manifest has no window. A namespace's window then comes from two places (the manifest and the maintainer's settings), and how long a namespace keeps its history depends on which process sweeps the store. If two servers with different settings advance one namespace's floor, the namespace keeps the history of the shorter window, because a floor never moves down. For example, if one server's setting is 30 days and the other server's setting is 7 days, a namespace without a window keeps 7 days of history.
