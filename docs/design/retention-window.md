# Retention window

This note describes how a namespace's writer expires deletions once the [retention floor](../specs/glossary.md#retention-floor) reaches them, what a base compaction then removes, and how the reference server advances the floor on a schedule.

## History that grows without bound

This section describes why LoonFS needs to bound how long a namespace keeps deleted items and old revisions.

An undelete restores a deleted item with its revisions, its attributes, and everything below it, so LoonFS can't reclaim any of those rows or their content while the deletion can be undone. A deleted file keeps its newest revision for this reason, and each recoverable deletion has an entry in the namespace's "trash" (the `active_deletions` family).

Two gaps would let this history grow without bound. If the [retention floor](../specs/glossary.md#retention-floor) didn't cover deletions, advancing it wouldn't let LoonFS reclaim anything that a deletion hides, so a namespace that deletes files every day would keep every byte it ever deleted. If no process advanced the floor on a schedule, revision history, trash entries, and the content behind them would grow until an operator ran the `retention` job by hand.

## Recoverable and expired deletions

This section describes how long a deletion stays recoverable.

LoonFS can't reclaim what a deletion hides while the deletion can still be undone, so each deletion needs a point in the namespace's history where it stops being recoverable.

The [retention floor](../specs/glossary.md#retention-floor) bounds every kind of history that a namespace keeps: incremental replay, superseded bindings, file revisions, attribute and access states, and commit receipts (see [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor)). The floor bounds recoverable deletions in the same way. Each deletion has a deletion sequence (`deletion_seq`), which is the sequence number of the commit that deleted the item. Once the floor reaches a deletion's sequence, the namespace's writer commits an "expire" event for the deletion, and the deletion has "expired".

An expired deletion can't be undeleted, and the trash listing leaves it out. No read of the namespace's current state can reach the inodes that the deletion hides, and LoonFS can reclaim their rows and content. (A snapshot from before the expiry still shows them, as described in [Reads of an expired subtree](#reads-of-an-expired-subtree).) A deletion stays recoverable until its expire event is committed, and until then an undelete can restore the deleted item as described in [File and subtree deletion](../specs/format.md#16-file-and-subtree-deletion). Deletions don't have a floor of their own, and expiry doesn't depend on a clock.

For example, suppose a file is deleted at sequence 40 while the floor is at 35. The file is in the trash, and an undelete can restore the file. A retention advance then moves the floor to 50, and the writer commits an expire event for the deletion at sequence 61. From sequence 61 on, the deletion has expired.

Deletions can be nested. If `/old/draft.txt` is deleted on its own and `/old` is deleted later, each deletion has its own tombstone and its own trash entry, and undeleting `/old` doesn't restore `draft.txt`. An item can't be deleted while a deletion above it hides it, so an inner deletion is always older than the deletion around it. The writer expires deletions in deletion sequence order, so it expires the inner deletion in the same commit as the outer one, or in an earlier commit. The format doesn't require that order, because each expire event cancels one deletion. So the reads in [Reads of an expired subtree](#reads-of-an-expired-subtree) and the compaction in [The kept inode filter](#the-kept-inode-filter) also cover a recoverable deletion inside an expired one. The items under such a deletion stay readable by inode ID, and their rows stay in place.

A [fork](../specs/glossary.md#fork)'s floor starts at its fork point, so every deletion that a fork inherits is at or below the fork's floor. In a fork, the trash listing starts its scan above the fork point (the fork's initial floor). The keys of `active_deletions` begin with the deletion sequence, so the fork point is where the scan starts, not a filter on its results, and an inherited deletion never appears in the fork's trash.

An undelete of an inherited deletion returns `not_deleted`. An undelete runs in a [writer session](../specs/glossary.md#writer-session), and in a fork, a new writer session expires every deletion at or below the fork point before it admits its first request, so the outcome of the fork's first request doesn't depend on timing. The session makes one expire commit for every 1,000 inherited deletions, and a retention window on the source keeps that number small. If one of those expire commits fails, the requests that the session was about to commit fail with the same error, and the session tries the expiry again before it commits its next requests.

The rows that an inherited deletion hides stay until the fork's first writer session commits the expire events and a [base compaction](#removing-an-expired-deletions-rows) runs, so a fork that nobody writes to keeps those rows. The source namespace keeps its own trash under its own floor, so an inherited deletion can still be recovered in the source.

For example, suppose a namespace's trash lists a deletion at sequence 80, and the namespace is forked at sequence 100. The fork's floor starts at 100, so the fork's trash listing starts its scan above 100 and doesn't list the deletion, even before a writer session opens in the fork. The fork's first writer session expires the deletion before it admits a request, so an undelete of the deletion in the fork returns `not_deleted`. In the source, the deletion stays recoverable under the source's own floor.

Why don't deletions have a floor of their own? The floor is the namespace's one bound on how far back it keeps history, and deletions are part of that history. With one floor, a client can rely on one number for how far back it can recover anything, whether that's an old revision or a deleted file.

Why does a fork lose its inherited trash? A fork's history starts at its fork point, so the fork's floor can't be lower than the fork point. Keeping inherited deletions recoverable in the fork would take a second floor (see [Alternatives considered](#alternatives-considered)).

Why does a fork's trash listing start above the fork point, instead of relying on the expire events? A read-only handle never opens a writer session, so without the listing rule, a fresh fork would show inherited deletions until a client wrote to it.

## Expiring a deletion

This section describes the event that expires a deletion, and why the namespace's writer commits it.

A retention advance is a maintainer publication and an undelete is a writer commit, so LoonFS can't order a floor move against an undelete. If undelete validation compared a deletion's sequence with the floor, an undelete could restore an item whose rows a base compaction had already removed, as the example in [Alternatives considered](#alternatives-considered) shows. LoonFS avoids that race by recording the end of a deletion's recoverability as a commit in the writer's log.

A [tombstone](../specs/glossary.md#tombstone) has three kinds of event. A `set` event starts a deletion, a `revoke` event (from an undelete) cancels it, and an `expire` event ends the deletion's recoverability. Like a `revoke`, an `expire` identifies the `set` event that it cancels by that event's position and contains no deleted binding. Committing an `expire` adds a `removed` row to `active_deletions` for the same `(deletion_seq, root_inode_id)`. So the trash listing leaves out an expired deletion through the usual removed-row rule, and an undelete returns `not_deleted` because the deletion isn't the active one. Neither the trash listing nor undelete validation compares a sequence with the floor.

An `expire` differs from a `revoke` in what it restores. An undelete commits a `revoke` together with a new binding for the item, so the subtree reappears. An `expire` restores nothing, so the tombstone keeps hiding the subtree. At each sequence, a tombstone hides its subtree when the tombstone's latest event is `set` or `expire`, and the deletion is recoverable when that latest event is `set`. An `expire` also makes the subtree unreadable, as described in [Reads of an expired subtree](#reads-of-an-expired-subtree).

Each writer session keeps the last floor whose deletions it has finished expiring. When the session's [position](../specs/glossary.md#writer-position) moves onto a manifest with a higher [retention floor](../specs/glossary.md#retention-floor) (`retention_floor_seq`), the session repeats one step of its commit loop until no `listed` row at or below the new floor remains. In each step, the session:

1. Scans its view of `active_deletions` for the next 1,000 `listed` rows at or below the new floor. The family's keys start with the deletion sequence, so this is a range scan that ends at the new floor.
2. Commits an `expire` event for each deletion that the scan found, in one commit. The commit is attributed to the actor `loonfs` (the actor that the root inode is attributed to), and it isn't checked against a subject.

The first step starts at that last finished floor (or at the beginning of the family, in a new session that hasn't finished expiring under any floor), and each later step starts after the last deletion that the step before it expired.

LoonFS validates each `expire` the way it validates an undelete, so the `set` event that the `expire` cancels has to be the deletion's latest event. LoonFS also rejects an `expire` whose deletion is above the floor of the manifest that the commit is validated against. The session admits no other commit between a scan and the commit of that scan's batch, so every expire event in the batch passes both checks. The session admits user commits between batches, so it expires a large trash one batch at a time without delaying the namespace's other writes. For example, 2,500 deletions at or below the floor take three steps, with 1,000, 1,000, and 500 expire events, and the session can admit user commits between those steps. (A new session in a fork is the exception, because it expires every inherited deletion before it admits its first request, as described in [Recoverable and expired deletions](#recoverable-and-expired-deletions).)

A step can fail, for example if the WAL put of its commit fails. After a failed step, the session waits for one more publication of its own (a WAL object that it writes for its requests) before it runs another step, and each failure in a row doubles that wait, up to 64 publications. For example, if the store refuses every expiry put, the session tries a step again after 1, 2, 4, 8, 16, 32, and then every 64 of its own publications. A step that commits its expire events, or finds nothing left to expire, resets the doubling, so a later failed step waits for one publication again. So a session that publishes nothing after a failed step doesn't try again until it publishes, or until a new writer session opens. (The 64 is an implementation limit, `MAX_EXPIRY_RETRY_PUBLICATIONS`, not a format requirement.)

Why does the writer commit the expiry? A namespace has one writer, and the writer's commits are totally ordered. An undelete that the writer validated before an expire event comes before the expire event in the WAL, so the undelete has already revoked the deletion, and the writer's scan doesn't find the deletion. An undelete attempted after the expire event fails validation. So no process can undelete a deletion between the floor move and the deletion's expiry, and compaction can treat an `expire` event as final. The retention advance is the maintainer's publication, as described in [Maintenance roles](maintenance-roles.md), and the writer's part consists of ordinary commits.

Why does a failed step wait for the session's own publications, and not for a time? With a wait measured in publications, the session tries the step again at its next write after a failure that doesn't last, such as one failed put. After a failure that keeps happening, such as a store that refuses every expiry put, the session doesn't try again on every commit.

## Reads of an expired subtree

This section describes what a read returns for an inode that an expired deletion hides.

Base compactions of different [family groups](../specs/glossary.md#family-group) run at different times, so for a while some groups have removed an expired subtree's rows and others haven't. A read by inode inherits access grants from the inode's ancestors, as described in [Access rows](../specs/format.md#18-access-rows). If the `access` group removed a hidden directory's access row, and with it the boundary flag, before the `revisions` and `inodes` groups removed their rows, a read by inode would inherit the grants above the boundary. Principals that the boundary excluded could then read the rows that those groups still store.

From the moment an expire event is committed, no read of the namespace's current state can reach an inode that the expired deletion hides. A read by path doesn't resolve the inode, as with any [tombstone](../specs/glossary.md#tombstone). Before LoonFS serves a read by inode ID, whether of the inode, its revisions, its attributes, or its content, it checks the inode's ancestry. This ancestry walk starts at the inode, follows the current bound bindings up through the inode's ancestors, and stops at the first tombstone that hides the inode or one of those ancestors:

- If that tombstone's latest event is `expire`, the read returns `inode_not_found`, the error for an unknown inode.
- If its latest event is `set`, the read is allowed, and the subject's rights are evaluated through the deletion's saved parent, as for any recoverable deletion.
- If the walk reaches the root inode without meeting a tombstone, the read is allowed.
- If the walk reaches an inode with no bound binding before it meets a tombstone, the chain to the root is broken, and the read returns `inode_not_found`.

LoonFS makes this check before it evaluates any access grant, so every principal gets the same result, an administrator included.

For example, suppose the directory `/team` grants a team `read`, and its subdirectory `/team/hr` has its boundary flag set and grants `read` to one person. `/team/hr` contains the file `salaries.csv`. Someone deletes `/team/hr`, and the deletion expires. From the expire event on, a read by inode of `salaries.csv`, or of any of its revisions, returns `inode_not_found` for every principal. So if the `access` group's base compaction removes the access row for `/team/hr` before the `revisions` group's base compaction removes the file's revisions, no team member can read those revisions through the grant on `/team`.

The walk stops at the first tombstone it meets, so for an item under a recoverable deletion that's inside an expired one, the walk finds the inner deletion's tombstone. For example, suppose `/old/draft.txt` was deleted on its own before `/old` was deleted, and the deletion of `/old` has expired while the deletion of `draft.txt` is still recoverable. For a read by inode ID of the revisions of `draft.txt`, the walk meets the file's own tombstone first and finds that its latest event is `set`, so the read is allowed. For the same read of a file that was still in `/old` when `/old` was deleted, the walk meets the tombstone of `/old`, so the read returns `inode_not_found`.

A snapshot from before the expiry still shows the subtree, because a read through the snapshot uses the pinned manifest, where the subtree's rows are still stored. In an ACL namespace (one whose access mode is `acl`, see [Access rows](../specs/format.md#18-access-rows)), LoonFS evaluates a snapshot read's rights on the access rows at the current head. So once an expired deletion hides the inode at the head (or the inode's chain to the root is broken there), a read of the inode through the snapshot requires an administrator, and any other subject gets `inode_not_found` or `path_not_found`, as a read of the current state does. In an `unrestricted` namespace, a snapshot read of the inode works as it did before the expiry.

Why does the subtree become unreadable at the expire event? The check closes the window between the family groups' compactions whatever order the groups compact in, and keeping the tombstone alone doesn't.

Why does LoonFS refuse a read when the chain is broken? Every live inode has bound bindings up to the root inode, and every deleted inode has them up to its deletion's root, which has a tombstone. A chain that breaks before it reaches either one belongs to an inode that the namespace has no way to show. Base compactions remove an expired subtree's rows one family group at a time, so the bindings that lead up to the expired root can be gone while the revisions below it are still stored. A walk from one of those revisions' inodes can then no longer reach the `expire` event, so LoonFS refuses the read because the chain is broken.

Why does a snapshot read of an expired subtree require an administrator? LoonFS evaluates a snapshot read's rights on the access rows at the current head, and once a deletion expires, base compactions remove the head's rows under it one family group at a time. So at the head, a boundary row can be gone while a grant above it is still stored, and a subject that the boundary excluded could then inherit that grant. An administrator's rights don't depend on those rows.

## Removing an expired deletion's rows

This section describes the rows that a base compaction removes for an expired deletion.

An expire event ends a deletion's recoverability, but the rows that the deletion hides stay in the namespace's metadata until a compaction removes them.

A "base compaction" merges a window of runs that includes a [family group](../specs/glossary.md#family-group)'s oldest run, so it can drop rows under the retention rules (see [Compaction windows](../specs/format.md#102-compaction-windows)). A deletion is expired in the compaction's [metadata basis](../specs/glossary.md#metadata-basis) when the deletion's latest tombstone event in that basis is `expire`. For each expired deletion, the compaction removes the rows of every inode that the deletion hides, which are the deletion's root and every descendant of the root in the basis. For example, if the directory `/old` (inode 7) contains `a.txt` (inode 8), and the deletion of `/old` has expired, the deletion hides inodes 7 and 8.

| Family | Rows a base compaction removes for an expired deletion |
| --- | --- |
| `active_deletions` | The root's `listed` row and the `removed` row of its expire event. A base compaction removes the two rows together, like any cancelled pair. |
| `inodes`, `revisions`, `attributes`, `access` | Every row of a hidden inode. |
| `direntry_binds`, `direntry_child_binds` | Every edge whose parent is a hidden inode. The binding rule in [Row retention during a base compaction](../specs/format.md#103-row-retention-during-a-base-compaction) covers the root's own edge. The newest version of that edge at or below the floor is unbound, so the compaction drops that version together with every older version it hides. |
| `tombstones` | None. Every `set`, `revoke`, and `expire` event stays. |
| `content_layouts` | The chains of the removed revisions. The basis revision filter drops them as it does for any removed revision. |

A hidden inode that's an ancestor of a recoverable deletion keeps its rows in every family while that deletion is recoverable, as described in [The kept inode filter](#the-kept-inode-filter).

A delta-only compaction (one whose window doesn't include the group's oldest run) removes none of these rows, because it keeps every history row (see [Compaction windows](../specs/format.md#102-compaction-windows)).

Reclaiming a deletion's content takes four steps:

1. A retention advance moves the floor past the deletion.
2. The writer's position moves onto the manifest with the new floor, and the writer commits the deletion's expire event.
3. A base compaction of each family group removes the rows that the deletion hides, and a base compaction of `content_layouts` that runs after the revisions are gone drops their chains.
4. [Garbage collection](../specs/glossary.md#garbage-collection-gc) deletes the content objects once no current, pinned, or recently replaced manifest refers to them and they're older than the [reclamation grace](../specs/glossary.md#reclamation-grace) (see [Content roots](../specs/format.md#119-content-roots)). Content that a kept revision also refers to stays.

Why can a base compaction remove every row of a hidden inode, and not just the older rows that the usual retention rules drop? The floor is at or above the deletion, and the tombstone hides those inodes at every sequence from the deletion on, so they're hidden in every view that compaction has to preserve. Every row of a hidden inode is also at or below the floor, because LoonFS commits no change to a hidden inode, and the writer doesn't expire a deletion until the floor reaches the deletion. (If an inode was undeleted and then deleted again, its latest deletion is the one that expired, so its rows are older than that deletion too.)

Why keep the tombstone events? Family groups are compacted at different times, so for a while some groups have removed a hidden inode's rows and others haven't. The ancestry walk in [Reads of an expired subtree](#reads-of-an-expired-subtree) stops at the tombstone, so the tombstone keeps those inodes unreadable in between. Once the bindings group has removed the edges that lead up to the tombstone, the broken chain keeps them unreadable instead, so a read by inode ID can't find a partly removed subtree. The cost is one small tombstone row for each deletion, each undelete, and each expiry.

## The kept inode filter

This section describes how a base compaction finds the inodes that an expired deletion hides.

A compaction tests one row at a time, so it needs a way to check whether each row's inode is hidden, and a hidden subtree can have more inodes than fit in memory. Before a base compaction merges the `inodes`, `revisions`, `attributes`, or `access` group, or the bindings group (`direntry_binds` and `direntry_child_binds`), it builds a "kept inode filter". That filter is a Bloom filter that contains every inode that's visible or under a recoverable deletion, and every ancestor of a recoverable deletion. A bounded compaction step and a streaming compaction job both build the filter before they merge.

To build the filter, the compaction:

1. Walks the basis's directory tree at the basis's head, starting at the root inode, depth first and with one cursor for each level, the way a recursive listing does. It reads each directory's children from the slot index (`direntry_binds`), follows each bound edge, and inserts every inode it reaches.
2. Walks the subtree of every recoverable deletion (one whose latest event is `set`, on either side of the floor) the same way, starting at the deletion's root. It reads those roots from the basis's `listed` rows in `active_deletions` that have no `removed` row.
3. Inserts the ancestors of every recoverable deletion. It starts at the parent saved in the deletion's tombstone, and then moves to each inode's current bound parent (or, at the root of another deletion, that deletion's saved parent), up to the root inode.

Deleting an item unbinds it, so the walk from the root inode doesn't reach a deleted subtree. An expired deletion isn't a starting point for the second walk, and its root is unbound, so neither walk inserts the inodes that the expired deletion hides. Step 3 inserts the hidden inodes that are ancestors of a recoverable deletion inside the expired one.

The merge removes a row whose inode isn't in the filter, and applies the usual retention rules to a row whose inode is in the filter. The merge tests a binding row by its parent inode. The two binding indexes are merged in one compaction against the same filter, so the compaction keeps or removes each binding event the same way in both indexes.

A Bloom filter can report an inode that was never inserted (a false positive), but it never misses an inode that was inserted. A false positive keeps a hidden inode's rows for one compaction, and a later compaction uses a different seed, so the later compaction removes those rows. The layout filter in [Row retention during a base compaction](../specs/format.md#103-row-retention-during-a-base-compaction) errs the same way, and the kept inode filter follows the layout filter's rules: xxh64 double hashing, a seed from the output run number, and ten bits per expected entry. The expected number of entries is the number of rows in the basis's `inodes` family (the sum of its segments' `row_count`), which shrinks as base compactions remove hidden inodes.

If the filter would be larger than `MAX_BLOOM_FILTER_BYTES` (64 MiB), or the walk would go more than `MAX_KEPT_INODE_WALK_DEPTH` (4,096) levels deep, the compaction keeps every row it would have tested, removes no hidden rows in that step, and logs which limit it reached. At ten bits per entry, the byte limit covers about 53 million inode rows. The depth limit applies both to the cursors that the walk keeps at once (one for each level) and to the ancestors that step 3 follows up from one deletion. A move can nest a tree deeper than the longest path that a request accepts. Each level of the walk keeps a directory's resume key of up to about 1.6 KiB, so 4,096 levels take at most about 7 MiB.

The walk reads the slot-index rows of every directory it reaches, so each base compaction of these groups costs one more pass over the bindings of the visible and recoverable tree.

A base compaction over a manifest whose `retention_floor_seq` is still 0 doesn't build the filter. It keeps every row and doesn't walk the tree, so the base compactions of a namespace whose floor has never advanced skip that extra pass.

Why does the compaction build a filter from a walk, and not an exact set of hidden inodes? A hidden subtree can have more inodes than fit in memory, and so can the visible tree. A filter's size is set before the walk starts, and a filter with no false negatives errs toward keeping a row, so the compaction stays inside its memory limit without removing a row that a view needs.

Why does the filter keep a recoverable deletion's ancestors? LoonFS evaluates a subject's rights on an item under a recoverable deletion through the deletion's saved ancestry (see [Access rows](../specs/format.md#18-access-rows)), so the ancestors' access rows have to stay while base compactions remove the rest of the expired subtree. For example, suppose `/team` grants a team `read`, `/team/hr` has its boundary flag set and grants one person `read`, and `/team/hr/salaries.csv` is deleted on its own before `/team` is deleted. If the deletion of `/team` expires while the deletion of `salaries.csv` is still recoverable, the rights for a read of the deleted file are still evaluated through `/team/hr`. If a compaction removed the access row of `/team/hr`, the evaluation could reach the team's grant on `/team` in a run that no compaction has merged yet, and a team member could read the file.

Why can a base compaction over a floor of 0 skip the walk? A deletion expires only through an `expire` event, and validation rejects an `expire` whose deletion is above the floor of the manifest that the commit is validated against (see [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor)). A deletion's sequence is at least 1, so an `expire` needs a floor of at least 1, and the floor never decreases from one manifest to the next. So a manifest whose floor is 0 holds no expired deletion, and every inode in its basis is visible or under a recoverable deletion.

Why does the compaction find hidden inodes by reachability, and not by a flag on each row? A tombstone on a directory is one row that hides the whole subtree, so no row below the directory records that the row is hidden. A flag on every row would turn one delete into a write for every descendant, which is what a subtree tombstone avoids.

## Advancing the floor on a schedule

This section describes how the reference server advances each namespace's floor on a schedule, and when the deletions that the floor passes expire.

A retention advance moves the [retention floor](../specs/glossary.md#retention-floor). Without a schedule, an advance runs only when an operator starts one, and a namespace's history keeps growing in between.

A namespace's [retention window](../specs/glossary.md#retention-window) is how long, in milliseconds, the reference server keeps the namespace's history before the server advances the floor past that history. A namespace can have a window of its own, which its manifest stores (`retention_window_ms`). The window is set when the namespace is created, a `retention_window` maintenance run can set, change, or clear it (see [Retention runs](../specs/api.md#retention-runs)), and a fork starts with its source's window. The server has a window too, the top-level `retention_window_ms` setting in its config file (like `full_sweep_interval_ms`), and it applies to every namespace without a window of its own. Each window must be positive when it's set.

The full pass of the maintenance sweep (the pass that lists every namespace, once a day by default) advances the floor of each namespace that isn't deleted, with a cutoff time of the current time minus the namespace's window, or minus the server's window for a namespace that has no window of its own. A namespace with neither window keeps its floor. The full pass uses the same retention advance as the `retention` job, so the cutoff rule in [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor) applies. The advance reads the namespace's window from the current manifest, which it loads anyway to find the cutoff commit, so a host that advances floors on a schedule needs no extra request to read each namespace's window. Every 5 seconds by default, the sweep also runs a "tick", which visits each writer session that the server keeps open (see [Maintenance hosting](maintenance-hosting.md#tick-and-daily-pass)). The ticks don't advance the floor on their own schedule. If a tick is visiting a namespace when the full pass reaches that namespace, the pass doesn't wait for the tick, and the tick makes the pass's advance once its own step finishes. An operator can also advance a floor by hand, with or without a window, through the `retention` job (see [Retention runs](../specs/api.md#retention-runs)) or `loonfs maintenance retention advance`. A server whose `maintenance` mode doesn't run the sweep doesn't advance any floor.

A deletion expires when the namespace's writer finds the new floor, which happens at the writer's next commit, fold, or position move after the advance. When this server has a writer session open for the namespace, the full pass advances the floor in the same process, so the session's position moves onto the advance's manifest, and the session starts its expiry steps right away. If a namespace has no open writer session, no process expires its deletions, so the deletions of a namespace that nobody writes to stay recoverable, and their rows stay, until a writer session opens.

If an advance fails, the server keeps no retry state for it, even for a writer session that the server keeps open, and the next full pass tries the advance again. So a failed advance delays the floor move, and the expiry of the deletions that the floor would have reached, until the next full pass (a day later, by default).

For example, with a 30-day window (`retention_window_ms = 2592000000`) and the default daily pass, a deletion stays recoverable for at least 30 days. The first full pass after the deletion is 30 days old moves the floor past the deletion, so with a writer session open, the deletion expires between 30 and 31 days after the commit that deleted it. The deleted bytes are reclaimed after the next base compaction and garbage collection pass.

These times are measured by commit timestamps, which come from writers' clocks. The cutoff scan stops at the first commit whose timestamp is later than the cutoff, so one commit with a late timestamp keeps the floor below that commit, and the deletions committed after it stay recoverable for longer. (A base compaction runs once enough new metadata builds up above a family group's oldest run, so a namespace that stops writing can keep an expired deletion's rows until the namespace is written again.)

If two servers that maintain one store set different windows, the shorter server window takes effect for each namespace without a window of its own, because the floor never moves back. A namespace's own window is the same for every server, because each server reads it from the namespace's manifest.

Why don't the ticks advance the floor on their own schedule? An advance checks that the manifest's segments exist and then publishes a manifest. A window measured in days doesn't need a finer schedule than the daily pass, and the full pass publishes nothing for a namespace whose floor has already reached its cutoff. For the same reason, the ticks don't retry a failed advance.

Why does the full pass leave its advance to a tick that's visiting the namespace? The tick might be running a long compaction, so the pass can't wait for it. If the pass skipped the advance instead, the window would be a day longer for the busiest namespaces.

## Expired deletions in the API

This section describes how an expired deletion appears to clients.

| Request | What a client sees for an expired deletion |
| --- | --- |
| The trash listing ([`GET /filesystem/trash`](../specs/api.md#66-get-filesystemtrash)) | The listing leaves out expired deletions, because each expire event adds a `removed` row. |
| Undelete | An undelete of an expired deletion returns `not_deleted`. |
| The [change feed](../specs/api.md#611-get-changes) | Each expire commit appears in the feed, attributed to `loonfs`, with a `deletion_expired` event (`inode_id` and `deletion_seq`) for each deletion that the commit expires. A client that doesn't recognize the event kind ignores the event. |
| Reads by path and by inode | A read by path doesn't resolve an inode that an expired deletion hides. A read by inode of any such inode, whether of the inode, its revisions, its attributes, or its content, returns `inode_not_found` from the moment the expire event is committed, as it does for any unknown inode, whatever rights the subject has. |
| Snapshot reads | A snapshot from before the expiry still shows the subtree. In an ACL namespace, a read of a hidden inode through the snapshot requires an administrator, and any other subject gets the not-found error that a read of the current state returns. |
| Namespace status | The status reports the floor in `retention_floor_seq`. A deletion at or below the floor stays recoverable until the writer expires it. |

Expiry has no route or error code of its own.

Why isn't there an error code for an expired deletion? A client can't do anything different for an expired deletion than for a deletion that was already restored, because in both cases there's nothing to recover. The change feed records each expiry, so a client that follows the feed can tell the two cases apart.

## Spec and guide sections

These sections of the specs and guides describe this design.

| Section | What it describes |
| --- | --- |
| Format, [File and subtree deletion](../specs/format.md#16-file-and-subtree-deletion) | The `expire` event and its `removed` row. A tombstone hides its subtree when its latest event is `set` or `expire`, and the deletion is recoverable when its latest event is `set`. |
| Format, [Access rows](../specs/format.md#18-access-rows) | The access walk stops at the root of an expired deletion, and the rights on an item under a recoverable deletion are evaluated through the deletion's saved parent. |
| Format, [Paths, listings, and revision reads](../specs/format.md#44-paths-listings-and-revision-reads) | The ancestry walk before a read by inode ID, and the `inode_not_found` error for an inode that an expired deletion hides or whose chain to the root is broken. |
| Format, [Visible metadata](../specs/format.md#43-visible-metadata) | A tombstone whose latest event is `set` or `expire` hides an inode. |
| Format, [Operations and WAL deltas](../specs/format.md#66-operations-and-wal-deltas), [WAL records](../specs/format.md#a5-wal-records), and [Metadata rows and row keys](../specs/format.md#a6-metadata-rows-and-row-keys) | The `expire_subtree_tombstone` delta that the writer commits, and the `expire` tombstone action. |
| Format, [Semantic commit fingerprints](../specs/format.md#appendix-b-semantic-commit-fingerprints) | The writer's expire operation, which `POST /commits` doesn't accept. |
| Format, [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor) | The floor also bounds recoverable deletions, and the writer expires the deletions that the floor reaches. |
| Format, [Row retention during a base compaction](../specs/format.md#103-row-retention-during-a-base-compaction) | The family rows in the table above, and the kept inode filter beside the layout filter, with its ancestors step and its limits. A base compaction doesn't remove the rows that a deletion hides until the deletion expires. |
| The glossary's [tombstone](../specs/glossary.md#tombstone) and [retention floor](../specs/glossary.md#retention-floor) entries | Expired deletions. |
| API, [Retention runs](../specs/api.md#retention-runs) | The floor bounds deletions, and a deletion leaves the trash once the writer expires it. |
| API, [`GET /filesystem/trash`](../specs/api.md#66-get-filesystemtrash) | The listing leaves out a deletion once the writer expires it. In a fork, the listing starts its scan above the fork point. |
| API, [`GET /filesystem/content` and `GET /inodes/{inode_id}/content`](../specs/api.md#67-get-filesystemcontent-and-get-inodesinode_idcontent) and [Download transport](../specs/api.md#610-download-transport) | The inode-addressed read and download routes return `inode_not_found` for an inode that an expired deletion hides, even while its revision rows are kept. |
| API, [Authorization in ACL namespaces](../specs/api.md#authorization-in-acl-namespaces) | A snapshot read of an inode that an expired deletion hides at the head requires an administrator. |
| API, [`GET /changes`](../specs/api.md#611-get-changes) | The `deletion_expired` event. |
| API, [Deleting, retaining, and reclaiming](../specs/api.md#deleting-retaining-and-reclaiming) | The reference server's sweep advances floors under `retention_window_ms`, and deleting a tree reclaims its content once the deletion expires. |
| The [self-hosting guide](../../crates/loonfs-server/docs/self-hosting.md#background-maintenance) and the example configs | The `retention_window_ms` setting, and how the full pass advances floors and tries a failed advance again. |
| [Maintenance hosting](maintenance-hosting.md#tick-and-daily-pass) | The full pass's retention advance, and the visit that makes the advance when the pass finds the namespace busy. |

## Alternatives considered

Keep a second floor for deletions, with its own advance and its own server setting. A fork could then keep its inherited trash, and an operator could keep deletions longer than revisions. But the second floor is a second mechanism that every compaction rule, every status report, and every advance has to cover, and an operator has to work out how the two settings interact.

Treat a deletion as expired as soon as the floor reaches it, with no event, and have reads, undelete validation, and compaction compare the deletion sequence (`deletion_seq`) with the floor. The floor moves in a maintainer publication, but an undelete is a writer commit, and LoonFS can't order the two against each other. For example, an undelete of a deletion at sequence 10 can be validated while the floor is 5 and committed at sequence 22. The maintainer can then move the floor to the manifest's head at 20, and a base compaction can remove the subtree's rows, so the fold of commit 22 binds an item whose rows are gone.

Treat a deletion at or below a fork's fork point as expired from the fork's creation, with no event. The fork point never moves, so comparing a deletion with the fork point is safe, and the fork's writer commits nothing for its inherited deletions. But that's a second way for a deletion to expire, so undelete validation, compaction, and the kept inode walk each have to check a second rule beside the event. (The trash listing starts its scan above the fork point in either design.)

Have the writer advance the floor itself, by folding its tail and setting the floor in one manifest. That closes the race between a floor move and an undelete within one process. But it moves the retention advance to the writer role, the writer has to validate undeletes against the floor that it's about to publish, and no process advances the floor of a namespace whose writer isn't running.

Expire a deletion inside compaction once its deletion time (`deleted_at_ms`) is older than a configured age. Compaction then reads a wall clock and the server's configuration, two compactions of the same input can keep different rows, and the floor stops being the one bound on how much history a namespace keeps.

Advance the floor on every sweep visit as well as on the full pass. Each advance checks the manifest's segments and publishes a manifest, so every active namespace costs a publication and segment checks every few minutes. With a window of days, deletions don't expire any sooner in a way that a client could notice.
