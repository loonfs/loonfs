# Publishing a large merge through the manifest

This note proposes a second way to publish a [merge](../specs/glossary.md#merge). Instead of writing the merge's commit into one WAL object, LoonFS would build the merge's final rows as metadata segments, off to the side and in bounded memory, and main's writer would publish them with one manifest put that folds main's [WAL tail](../specs/glossary.md#wal-tail) and adds one [run](../specs/glossary.md#run) at the next sequence. A large merge would then be bounded by its plan budget instead of by one WAL object and the merge allowance, and a large merge or rebase would no longer be split into pieces. Nothing in this note is implemented, and [Merging a fork](merge.md) and the [format spec](../specs/format.md) describe today's design.

In this note, "main" is the namespace that a merge commits into, "the fork" is the namespace whose commits the merge applies, and "the plan" is the work that turns the fork's net change into the merge's rows.

## The problem

This section describes the limits of a merge today and what a merge at the largest allowed size does to main.

Today a merge is one commit in one WAL object. The planner turns the fork's net change into an ordered list of operations, and main's writer plans every operation again when it publishes. It assigns the sequence, each delta's `delta_index`, the binding positions and versions, and the revision numbers, and it checks each operation's preconditions against main's current rows. Four limits follow.

The first limit is size. A merge has to fit in one WAL object (512 MiB) and in the "merge allowance" (`max_merge_bytes`, 512 MiB by default), which charges five times the merge's planned WAL bytes, because publication builds the deltas and the WAL object while the operations are still in memory. A file create with a one-extent layout plans 5,418 bytes, so the default allowance admits about 19,800 file creates, and one WAL object fits about 99,100. The planner also refuses a range whose commits touched more than about 290,000 inodes. Each bound returns `merge_too_large` with a split hint, and the caller merges the fork in pieces.

The second limit is the writer. A merge is alone in its batch, and one worker per namespace publishes one batch at a time, so main's other commits wait while the writer plans the merge again. A merge with a subject goes through the writer's planning twice, because the plan runs it once to check rights.

The third limit is the commit row. After a fold, a `commits` row stores the whole commit with every delta, so every reader that needs one field of a merge commit (the planner's ownership check, a retry's receipt lookup, a retention advance) decodes all of it.

The fourth limit is the tail. Until main folds, every reader of main replays the merge's WAL object, and the fold then keeps the whole tail in memory with a sorted copy of each family, which no allowance charges.

Only the first limit has an error. A user meets the other three as latency, memory, and read cost. For example, a merge of 99,000 file creates on a server whose allowance was raised to 2.5 GiB has a WAL object whose planned bound is about 536 MB, which main's other commits wait behind and every reader without that tail cached replays until the next fold. A fork of 200,000 files can't merge as one commit, and its rebase replays in pieces, which [Merging a fork](merge.md#rebasing) says can report conflicts on names that the fork passed through.

## The design

This section describes the proposal for one merge, from the request to the manifest that makes it visible.

A commit goes through the log. A fold or a compaction builds immutable segments off to the side, with bounded memory, and makes them visible with one manifest publication, and a segment that never gets published is collected once it's old enough. A large merge is the same kind of work, so this design builds the merge's rows as segments and publishes them the same way.

Merges below a size threshold keep today's planner and publication unchanged (see [Small merges](#small-merges)). To merge a fork above it, LoonFS:

1. Starts the merge as a job in the process that received the request, looks up the receipt for the request's commit ID, and checks the range and both retention floors, as today. The plan needs no writer epoch.
2. Folds the fork, takes snapshots of the fork's manifest and of main's manifest that last the plan's budget, and reads main at its head, the "basis" `P`.
3. Streams the fork's net change in inode order, classifies each inode against main at `P`, and writes the inode's final rows into segment writers. A merge that finds a conflict stops and publishes nothing.
4. Checks what main committed after `P` against what the plan touched, first without any writer epoch, and then, after claiming main's writer epoch, writing its fence, and pausing its own queue, for the last few commits up to the WAL tip, whose head is `H`.
5. Puts the next manifest, which folds main's tail into a run at `H`, adds one more run at `H + 1` that lists the merge's segments, and sets the head to `H + 1`.
6. Resumes the queue. The writer's next WAL object starts at `H + 2`.

If step 4 finds an intersection, LoonFS plans the merge again.

### Planning

The fork is folded first, so the fork snapshot's manifest contains every commit in the range, the plan reads no WAL tail of the fork, and the merge writes no pending pieces. Main's snapshot is the temporary snapshot kind that a merge takes of its fork today, with a lifetime of the plan's budget instead of 5 minutes, so a compaction and a collection of main can't remove the segments that the plan reads. The plan reads main's tail above that manifest once, at the start.

The plan keeps one structure that grows with the merge and writes everything else out as it goes:

- **The touched inodes.** The planner reads the fork's commits in pages, as today, and collects the inodes they touched, at an estimated 32 bytes each (about 32 MB per million inodes). Above a bound, it writes the set as sorted chunks and merges them.
- **Classification.** Classification runs in inode order and writes rows into one segment writer per family, the writers that the fold and compaction use, with at most 32 puts and 32 MiB in flight. The families keyed by inode come out in key order.
- **Families out of order.** The slot index (`direntry_binds`), `content_layouts`, and `trash` aren't keyed by inode, so the planner writes them as sorted chunks under segment keys, which the age rule for unlisted segments collects, and merges the chunks with the compaction's sorted iterators.
- **Checks across inodes.** Names that inodes of the same merge take or free, the subtree check, and a replay's skip of dependent changes keep sets that grow with the binding changes or the conflicts.

Classification applies the conflict rule first, and then checks the subject's rights for each change that passes it, with the rules in [Merging a fork](merge.md#merging). The rights walk reads main's access rows at `P`, except for a directory that the merge creates, where it reads the access row that the merge writes for that directory and then continues with main's rows from the first ancestor that exists at `P`, as today's writer sees the rows of earlier operations in the same commit. So a merge with a conflict returns `merge_conflict` and no rights error, as today, and a replay checks rights on the parts it applies.

A re-plan is a new plan, with a new basis, new snapshots, and a new budget. LoonFS makes at most four plans, as today, and then returns `store_contention` with the cause `lost_race`. A plan that doesn't finish within its budget publishes nothing and returns `merge_too_large` with a new bound, `plan_budget`, and no split hint, and the caller can still merge a shorter range of the fork.

### Rows that take the commit's sequence and time

The planner writes final rows, not operations. Three kinds of field depend on the merge's commit:

- **The sequence.** Every row except a `trash` row records the sequence of its commit (`committed_seq`), and most keys contain it, inverted, so that newer rows sort first. A merge run's rows store a marker in its place, and the run entry records the sequence. The block decoder replaces the marker with the run's `run_seq`, and a reader that seeks to a key in a merge run, or probes its filter, replaces the sequence in that key with the marker.
- **The time.** History rows record `committed_at_ms`, and a `trash` row `deleted_at_ms`. These store a time marker, and the run entry records the commit's time, which the publication sets. A row dated at the plan's start would let a retention advance pass the merge up to a budget early.
- **Positions and numbers.** Each delta's `delta_index` is assigned by the planner, so a binding's position and binding version are `{merge sequence, delta_index}`. Revision, attributes revision, and access revision numbers continue main's from its rows at `P`.

The marker is per field, because some sequences in a merge's rows are main's: a summary row's `bind_position` can be main's existing binding, an unbind or a revoke points at main's event, and a restore's `trash` row is keyed by the deletion's original sequence.

| Families | The merge's sequence in the key | In a merge run |
| --- | --- | --- |
| `inodes` | Not in the key | Marker in `committed_seq` |
| `direntry_binds`, `direntry_child_binds`, `revisions`, `tombstones`, `attributes`, `access`, `content_layouts` | Inverted, after the entity's ID | Marker in every row, so the stored order is the decoded order |
| `trash` | First, as the deletion sequence | Marker for the merge's own deletions. It sorts above every real sequence, and every other `trash` row in the run removes a deletion at or below `P`, so the stored order is the decoded order. |
| `commit_deltas`, `merge_conflicts` (new) | First | Marker in every row |
| `commits`, `commit_receipts`, `merges` | First or last | Written by the publication with the real sequence |

So the publication writes the header row, the receipt, and one merge index row per covered range, and nothing proportional to the merge happens under the writer's pause. The run is the final state of the touched inodes, so it needs no ordering of operations, no temporary names, and no head precondition. A compaction that rewrites a merge run writes the real sequence and time into its output, because the output's `run_seq` is the window's position, and a fork's first manifest copies the run entry.

### A commit's deltas as rows

For a merge run, the `commits` row stores the commit's header and merge record, and the deltas are rows of a new family, `commit_deltas`, keyed by sequence and `delta_index`. A replay commit stores its conflict report the same way, in `merge_conflicts`. Both join the `commits` [family group](../specs/glossary.md#family-group). The ownership check, a receipt lookup, and a retention advance read the small header, and the change feed, diff, and grep read the deltas in pages, resuming inside a commit by seeking to the next `delta_index`. The merge route returns the header with a count of events. An ordinary commit, or a small merge, keeps its whole record in its `commits` row.

### Checking what main committed during the plan

The run is valid if no commit in `(P, H]` changed a row that the plan read or replaces, or a rule that the plan applied. The plan records, beside its segments:

- the "touched set", a sorted list of every inode whose rows the merge replaces or reads to classify, and every directory slot (parent and name) that it binds into or vacates,
- the "path set", every directory on the path to a parent that the merge binds into or to an inode that it changes, which grows with the distinct directories on those paths, and
- the smallest deletion sequence among the deletions that the plan relies on being in main's trash (one it restores, or one that already hides an inode it would delete).

For the commits in an interval, the check reads the inodes and slots that each commit touched, and finds an intersection if:

1. a commit wrote any row of an inode in the touched set,
2. a binding change used a slot in the touched set (a name that main took after `P`),
3. a commit wrote a path set directory's own binding, tombstone, or access row (a change to the directory's children doesn't count, so a create under `/` doesn't intersect),
4. a commit wrote any row of an inode that has, among its ancestors in main at `H` (or, for an unbind, at its old parent), a directory that the merge deletes or moves, which is the conflict rule's subtree check applied to the commits after `P`, or
5. at the end of the interval, the range rule or the merge floor rule fails, or main's retention floor is at or above the smallest deletion sequence that the plan relies on.

Condition 5 covers a floor advance during the plan. For example, the plan restores a deletion at sequence 980, a maintainer's floor object raises main's floor to 990, a maintainer's fold records it, and a base compaction removes the rows under the expired deletion. The merge's put then loses to that fold's manifest and rebuilds over it, and the check against that basis finds 990 at or above 980, so LoonFS plans again and reports the restore as a conflict. The check runs against every basis that the publication builds on. (The rows that the merge's unbinds and revokes point at don't need a check of their own. They're current at `H` by conditions 1 to 3, and a base compaction keeps every current row except one hidden by an expired deletion, which condition 5 covers.)

After hours, the commits in `(P, H]` can number in the millions, mostly folded, so the check runs in two stages:

1. Without a writer epoch, the process reads main's head `P'` and checks `(P, P']`, reading `commits` rows and the tail like any reader. If main committed more than a few hundred commits meanwhile, it checks again from `P'` to the head it reads then, for at most three rounds (a choice, not a measurement). This stage can run on a host that isn't main's writer.
2. The process claims main's writer epoch, writes its fence ([Writer ownership](../specs/format.md#61-writer-ownership)), and pauses its own publication queue for main. It reads the WAL above its fence to find the tip, including a floor object that another process wrote, and checks the rest, from the last head that stage 1 checked through `H`, whatever its size. These commits are in the tail that it's about to fold.

An intervening merge run is read through its own segments. Its inode-keyed families and its slot index list what it changed in sorted order, so conditions 1 to 3 compare two sorted lists, reading that merge's keys once. Condition 4 walks the ancestors of each inode that it changed, but only when this merge deletes or moves a directory. Both happen in the first stage, because no other merge can publish after the fence, so a merge run doesn't need a separate list of what it touched.

### The publication

The plan runs in the process that received the request and has no writer epoch, so it doesn't fence main's writer for hours. That process becomes main's writer only for the publication: it claims the epoch and writes its fence after the first stage of the check and before the second. The fence keeps another host's writer from putting a WAL object between the tip that the publication reads and its manifest put, and the pause keeps this process's own queue from doing so. The publication then runs in main's fold path, under the fold lock (see [Folding and compacting a namespace](maintenance-hosting.md#folding-and-compacting-a-namespace)).

The new manifest folds main's tail through the WAL tip into a delta run at `run_seq = H`, adds the "merge run" at `run_seq = H + 1`, which lists the merge's segments and the publication's small segment, and sets `head_seq` to `H + 1`. The merge is ordered after every commit acknowledged before the claim, it's never in the WAL tail, so no read path that combines the manifest and the tail changes, and no run is above the head.

The publication keeps three rules that a fold doesn't:

- **The writer's epoch.** A fold rebuilds over any winning manifest, even another writer's claim. A merge publication publishes only while the basis's `writer_epoch` is the session's, and returns `writer_fenced` to the merge otherwise. That happens when a commit on another host claims main again between the merge's claim and its put, an interval of the second stage, one fold, and one put. The merge then claims again and repeats both stages of the check from where they stopped, without planning again, because a claim changes no row. Each attempt counts toward the four plans, after which the merge returns `store_contention` with the cause `lost_race`. A put lost to a maintainer's fold or a compaction rebuilds over the winner and runs the second stage of the check against it.
- **No WAL put in between.** The queue stays paused from the read of the tip until the publication learns the put's outcome.
- **An unknown outcome.** The session releases its writer epoch just before the manifest put, as it does before a commit's WAL put, and after an unknown outcome it claims the namespace again before it publishes anything. The claim takes the next manifest number with put-if-absent ([Writer ownership](../specs/format.md#61-writer-ownership)). If that number has the merge's manifest (the same publication ID), the claim collides and claims the number after it, and the merge committed. If the number is free, the claim takes it, and a delayed put of the merge's manifest can't be applied. The session then reads the receipt from its new basis, and a retry with the same commit ID returns the commit or plans again.

### Budgets and collection

A namespace's [collection](../specs/glossary.md#collection) deletes an unlisted segment once it's 24 hours old (the [grace window](../specs/glossary.md#grace-window)). A compaction unit bounds this exposure with a 12-hour budget plus the completion bound. A merge's plan gets a budget too, checked before the manifest put, and it has to be shorter, because the fork's content has to stay rooted for the whole plan, and the pool's collection relies on this inequality ([Content roots](../specs/format.md#119-content-roots)):

```text
POOL_ROOTS_READ_BUDGET_MS + GC_SAFETY_MARGIN_MS
    + OPERATION_SNAPSHOT_TTL_MS + PROVIDER_PUBLICATION_REQUEST_BOUND_MS
    < GC_GRACE_WINDOW_MS
```

With a plan budget `B` in place of the 5-minute snapshot lifetime, the left side is 12 hours + 5 minutes + `B` + 5 minutes, so the inequality is true while `B` is shorter than 11 hours and 50 minutes, and a compaction's 12 hours and 25 minutes doesn't fit. This design sets `B` to 6 hours (a choice, not a measurement). The same terms set `MAX_TAIL_COMMIT_AGE_MS`, which would fall to 5 hours and 50 minutes, but the plan reads no fork tail.

### The request

A plan over millions of inodes could take minutes to hours (not measured), so it can't run inside one HTTP request. The merge route starts the plan in the process that received the request and returns at once. The same request sent again with the same commit ID returns the commit once its receipt exists, `merge_conflict` while the process still has that outcome, or a response that says the merge is running, and after a restart it plans again. This is a compaction unit's shape, with a budget and no saved progress, which fits [Maintenance hosting](maintenance-hosting.md). Each plan uses one of a small number of plan permits, as a compaction unit uses a compaction permit.

### Small merges

Merges below a threshold keep today's planner and publication, unchanged, because one WAL put is cheaper than a fold, a run, and a pause. This design sets the threshold at the fold trigger, 8 MiB of planned bytes (about 1,500 file creates, estimated from the planned size of a create), so a merge in the WAL doesn't force a fold by its own size. The route runs today's planner first. That planner counts its planned bytes as it goes and, before it plans, the range's touched inodes, and it stops as soon as either could pass the threshold (more than about 4,500 touched inodes, at the smallest planned operation). The merge is then planned as a run from the start, so a large merge pays for at most a small plan first.

A small merge plans at most 8 MiB, so the WAL-object and touched-inode bounds can't refuse it, and its estimate of about 40 MiB fits the default allowance. It can still return `merge_too_large` on a server whose allowance is set below its estimate, and `server_busy` when other merges have reserved the allowance.

### A rebase

A replay commit is one publication in the new fork, planned on the fork's whole unmerged range, with the same rule for skipping the conflicting part of an inode's change. With no pieces, a replay doesn't report a conflict on a name that the fork passed through, doesn't apply a state that the fork passed through, and judges a directory's delete together with a skipped move of a file out of it. The new fork is private unless a resolver starts early, so the check usually finds no commits after `P`.

## A worked example

This section follows one merge, first with a commit to main that doesn't overlap it, and then with one that does.

Main's manifest 40 has `folded_wal_no` 300 and a folded head of 1,000, and WAL objects 301 to 305 contain commits 1,001 to 1,010. The fork was forked at 950, and its commits change `/docs/a.txt` (inode `A`), create `/docs/b.txt` (inode `B`), and delete the directory `/tmp/old` (inode `C`).

1. The route starts the merge. No receipt exists, and main's merge index has no row for the fork, so the range starts at the fork point.
2. The writer folds the fork, takes 6-hour snapshots of the fork's manifest and of manifest 40, and reads main at `P` = 1,010.
3. `A`'s revision 3 on main was committed at 900, at or below the fork point, so it's the fork's own, and the planner writes revision 4 (delta 0). `B` doesn't exist on main and its slot is free, so the planner writes `B`'s create, binding, and revision 1 (deltas 1 to 3). `C`'s binding is the fork's own and main's diff below `/tmp/old` is empty, so the planner writes `C`'s unbind and tombstone (deltas 4 and 5). The touched set is `A`, `B`, `C` and their three slots, and the path set is `/`, `/docs`, and `/tmp`.
4. A client commits 1,011, which creates `/src/main.rs` (inode `D`), in WAL object 306. The first stage finds no intersection: `D` and its slot aren't in the touched set, the create doesn't change `/`'s own rows, `D`'s ancestors aren't below `/tmp/old`, and the floor and merge index haven't changed. The process then claims main's writer epoch, which publishes manifest 41 with the next epoch, writes its fence at WAL object 307, and pauses its queue. It finds nothing above 307, so `H` = 1,011.
5. The process puts manifest 42, which folds WAL objects 301 to 307 into a run at 1,011, adds the merge run at 1,012, and sets the head to 1,012. `B`'s binding row is stored with the marker and decodes to the position `{1012, 2}`, which is `B`'s binding version.
6. The process resumes its queue, and its next commit, 1,013, is WAL object 308.

A reader with manifest 41 and the tail through 307 cached finds WAL object 308 starting two sequences above its head, runs discovery under the rule in [The sequence gap](#the-sequence-gap), and loads manifest 42.

If commit 1,011 had written a new revision of `/docs/a.txt` instead, the check would find `A` in the touched set, and the writer would plan again from 1,011. `A`'s current revision would then be foreign, so the plan would return `merge_conflict`, and collection would delete the first plan's segments after the grace window.

## What changes in the format and the log protocol

This section states each rule that changes and why the change is safe.

### The sequence gap

[Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal) requires every data WAL object to continue the preceding head. After a merge publication, a reader on an older manifest finds the next WAL object starting above its head. Today, discovery treats it as invalid, reloads the current manifest, and starts again from it when it's newer, so discovery recovers. A warm probe of a cached view treats it as corruption, so one read returns `namespace_corrupt` before the runtime drops the view.

The rule becomes: a data WAL object whose prior head is above the replaying head means that a newer manifest published a merge. Discovery reloads the current manifest and reports corruption only if it's the same one, and a warm probe returns "needs discovery", as it does today for an object of another writer epoch. Every replay validates objects through the same check, so discovery, warm probes, a fork's read of its source's tail, the writer's discovery after a collision, a retention advance, and the pool collection's read of a tail all get the rule. Only main's writer, at its epoch with its queue paused, raises the head without a WAL object, and it folds through the tip it found. So the first data WAL object above the new manifest's `folded_wal_no` continues the new head (a floor object can still record the older head, as the next section describes), and a gap above the current manifest's head is still corruption.

### Floor objects

Any process can write a [floor object](../specs/glossary.md#floor-object) at `tip + 1` without the writer epoch, with the head that its discovery found, and replay requires a floor object's head to equal the replaying head. The second stage of the check folds every floor object written before it reads the tip, but one put between that read and the manifest put keeps the older head. For example, another process writes a floor object at 308 with head 1,011 while the writer publishes manifest 42 with head 1,012 and `folded_wal_no` 307. Both puts succeed, the floor object's check after its put finds it live, and every reader from manifest 42 finds a floor object below its head.

The rule becomes: a floor object's head is at or below the replaying head, and replay keeps the replaying head. Every head computation changes with it, because replay today sets the head from each WAL object. The floor still moves as the floor object says, and no rows are lost, because a base compaction uses a floor only after a fold records it, and that fold builds over manifest 42, where a deletion that the merge restored is revoked.

The rule loses two things. It loses a check that would catch a floor object written with a wrong head for another reason. And a reader can see an expired deletion come back: if the merge restores a deletion at 1,005 and the floor object at 308 raises the floor to 1,008, a reader that replayed 308 on manifest 41 sees the deletion expired, and after it loads manifest 42, it sees the deletion restored, which [Expiring deletions and creating floor objects](../specs/format.md#expiring-deletions-and-creating-floor-objects) says can't happen. The process that advanced the floor also reported success for a floor that briefly hid a deletion that's now restored. The window lasts from the floor object's put to the merge's manifest put, so it's never longer than the writer's pause, and it needs a floor advance past a deletion that the merge restores. The designs that avoid it are in [Alternatives considered](#alternatives-considered).

### The guarantees of the log protocol

[A summary of the log protocol](../specs/format.md#26-a-summary-of-the-log-protocol) lists eight guarantees:

| Guarantee | Change | Reason |
| --- | --- | --- |
| 1. The visible log is unbroken above `folded_wal_no` | Changes: it continues the manifest's head, and an object above the replaying head means a newer manifest | The merge is committed by its manifest put, and the gap rule sends readers to that manifest. |
| 2. Every acknowledged commit is in the visible log or the current manifest | Changes: or in a merge run of it | The writer acknowledges a merge only after its put succeeds and the boundary check after it passes. |
| 3. Writer epochs never decrease, and a fence blocks older writers | Unchanged | The publication writes no WAL object, and a merge's manifest put loses at a number that a newer claim took, so a merge can't commit at an older epoch. |
| 4. Fresh discovery observes every acknowledged commit | Unchanged | Discovery loads the newest manifest, which lists the merge run. |
| 5. Cached views and writer positions never move backward | Unchanged | A view moves to the merge's manifest through discovery, and replay keeps the higher head over a floor object's. |
| 6. Pieces stay readable until folded | Unchanged | The publication folds main's tail as a fold does, and the merge has no pieces. |
| 7. Identical retries don't commit twice | Unchanged, with a new argument | After an unknown outcome the session claims before it publishes, so a delayed put is the committed merge or can't be applied. The range rule stops a second process from publishing the same range under the same commit ID. |
| 8. Current and snapshot manifests keep their segments | Unchanged | The merge's segments are protected by their age until the publication, and main's and the fork's by the plan's snapshots. |

### Changes to the specs

In the storage format, [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal) gains the gap rule and the floor object rule, and the log protocol summary changes guarantees 1 and 2. [Manifests, runs, and segments](../specs/format.md#71-manifests-runs-and-segments) gains the merge run, whose entry records the commit's sequence and time, and the basis checks for a manifest publication gain the merge publication and its epoch rule. [Row families](../specs/format.md#19-row-families) gains a third exception to "every row is derived from one delta", the two new families, and the markers. [Expiring deletions and creating floor objects](../specs/format.md#expiring-deletions-and-creating-floor-objects) says that under the floor object rule a reader can see an expired deletion as restored, for no longer than a merge publication's pause. [Merges](../specs/format.md#merges) gains the five conditions for a merge through a run, and [Content roots](../specs/format.md#119-content-roots) uses the plan's budget in its inequality. In the API, the merge route becomes a job, its response becomes the header with a count of events, and `merge_too_large` gains the `plan_budget` bound.

## What goes and what stays

Because small merges keep today's path, little code goes. For merges through a run, the split hint and its probes go, the WAL-object and touched-inode bounds no longer apply (above the threshold a merge goes to a run), and so do the writer's second planning and paging inside a merge commit by loading it again. In the merge session, the loops that follow a split hint, the pending requests pinned to a split point, the replay pieces, and the per-piece reports go. The `rebasing` state stays, because creating the new fork and sending the replay are still two steps. `merge_too_large` gains the `plan_budget` bound for merges through a run.

For small merges everything stays: the merge allowance (each reserves at most about 40 MiB at the threshold), the ordering of operations, the temporary names, the head precondition, and the writer's planning.

## Costs

This section lists what a merge through a run costs, each marked as measured, estimated, or not measured.

- **A publication and a run per merge.** One fold of main and one more run, which compaction absorbs with the next tier. A merge a minute adds a run a minute, within what folds already add on a steady writer (estimated from a measured build that published 62 folds in 15.6 minutes, in [A window for replaced manifests](replaced-manifest-window.md#costs)). What one more run costs a read hasn't been measured.
- **The writer's pause.** One fold of the tail (bounded by the 64 MiB write stop), the second stage of the check, and one manifest put. Not measured.
- **The check.** The first stage reads every commit after `P`, which grows with main's commit rate and the plan's length, and an intervening merge run's keys. When main commits faster than three rounds can follow, the rest moves into the pause. Not measured.
- **Planning time.** Reads of the fork and of main for each changed inode. Not measured, for one million or ten million inodes.
- **Memory.** About 32 MB per million touched inodes before the set spills (estimated from the planner's own figure), 32 MiB of puts in flight per family, and 64 MiB of decoded input in the sort. Not measured.
- **Storage.** The slot index, layouts, and `trash` rows are written twice (estimated). Main's snapshot keeps main's replaced segments for up to the budget plus the grace window, about 30 hours, which is up to one extra copy of main's metadata if base compaction rewrites it daily (estimated). A merge with `N` bytes of rows that plans twice stores about `2N` for up to a day (estimated). A file costs about 227 bytes of metadata across four families, from a single local run, and the deltas family adds a copy of each delta, as the `commits` row does today.
- **Fenced publications.** Where requests for main reach more than one host, a commit on another host between the merge's claim and its put fences the publication, and the merge claims again and repeats the check, within its four attempts. The plan itself has no epoch, so it isn't lost.
- **The size of the change.** The planner, about 4,300 lines today, is mostly rewritten, and the segment reader, the fold, the change feed, diff, grep, and the retry path change, about ten thousand to twenty thousand lines including tests (estimated).

## Alternatives considered

- **Today's design, with its limits stated as product limits.** A small merge is one WAL put, the size limits are explicit errors with split hints, and each piece is a correct merge. But the writer's pause, the `commits` row, and the tail grow with the merge without any error, and a rebase in pieces depends on where the pieces fall.
- **The log as the commit point.** The merge's commit is a small WAL object that refers to the staged segments, so order, unknown outcomes, epochs, and floor objects follow today's rules with no protocol change, and a floor object can't race the merge. A reader that replays to that object stops there and reads main as of the commit before it, and the writer's next step is a fold that lists the segments. The merge isn't acknowledged until that fold, so reads don't wait, but the writer's own commits do, and after a crash between the two puts they wait for a maintainer's fold. It needs a new WAL record kind that refers to segments, and its rows still need the sequence marker, because they're written before the WAL put. And the merge is committed while no manifest lists its segments, so the namespace collection, which reads no WAL objects, would need a new root, or it would delete a committed merge's rows once they're a day old. That last point is why this design makes the manifest the commit point.
- **Retention advances only through the writer.** No floor object could race the publication, so the floor object rule wouldn't change. Other processes would need a way to reach the writer, and a namespace with no writer would still need today's path.
- **Reserving the next WAL number in the merge's manifest.** A floor object that races into `tip + 1` would be dead by the existing rule, but one whose check ran before the manifest put has reported success, and readers that replayed it would see the floor go down.
- **A commit across several WAL objects, closed by a marker.** Each part is still bounded by a WAL object, the tail and the fold still keep the whole merge in memory, and every reader needs a rule for an unclosed commit.
- **Listing the fork's own runs in main.** The fork's rows record the fork's sequences, which overlap main's above the fork point, and segments would need shared ownership across namespaces.
- **Stamping the sequence at publication.** The writer would rewrite the whole merge under its pause, which brings back the second limit.
- **Steps that the client drives.** Each step would continue the previous step's plan, so the server would need a durable record of the plan's progress.
- **An incremental re-plan.** Planning only the intersecting inodes again needs a way to remove the first plan's rows for them, because a second set of segments at the same sequence adds rows and hides none.
- **One planner for every merge.** A small merge's rows would be written as deltas in a WAL commit and admitted after the same check. The ordering of operations, the temporary names, and the merge allowance would then go too.
