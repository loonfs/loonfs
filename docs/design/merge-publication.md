# Publishing a merge through the manifest

This note proposes a new way to plan and publish every [merge](../specs/glossary.md#merge) and every replay commit of a [rebase](../specs/glossary.md#rebase), whatever its size. One planner turns the fork's net change into the merge's deltas (the same kinds of delta that a WAL commit record contains), with placeholders for the merge's sequence and time. It derives the merge's rows from those deltas with the function that the fold uses, and writes them as metadata segments, off to the side and in bounded memory. Main's writer then publishes the segments with one manifest put that folds main's [WAL tail](../specs/glossary.md#wal-tail) and adds one [run](../specs/glossary.md#run) at the next sequence. The WAL never contains a merge. A merge is then bounded by the time that its plan is allowed to take, not by one WAL object and a memory allowance, and no merge or rebase is split into pieces. Nothing in this note is implemented. [Merging a fork](merge.md) and the [format spec](../specs/format.md) describe today's design.

In this note, "main" is the namespace that a merge commits into, "the fork" is the namespace whose commits the merge applies, and "the plan" is the work that turns the fork's net change into the merge's rows.

## The problem

This section describes the limits of a merge today and what a merge at the largest allowed size does to main.

Today a merge is one commit in one WAL object. The planner turns the fork's net change into an ordered list of operations, and main's writer plans every operation again when it publishes. The writer assigns the sequence, each delta's `delta_index`, the binding positions and versions, and the revision numbers, and it checks each operation's preconditions against main's current rows. Four limits follow.

The first limit is size. A merge has to fit in one WAL object (512 MiB) and in the "merge allowance" (`max_merge_bytes`, 512 MiB by default). The allowance charges five times the merge's planned WAL bytes, because publication builds the deltas and the WAL object while the operations are still in memory. A file create with a one-extent layout plans 5,418 bytes, so the allowance charges about 27 KB for each create, the default allowance admits about 19,800 file creates, and one WAL object fits about 99,100. The planner also refuses a range whose commits touched more than about 290,000 inodes. Each bound returns `merge_too_large` with a split hint, and the caller merges the fork in pieces.

The second limit is the writer. A merge is alone in its batch, and one worker per namespace publishes one batch at a time, so main's other commits wait while the writer plans the merge again. A merge with a subject goes through the writer's planning twice, because the planner runs the writer's planning once to check rights.

The third limit is the commit row. After a fold, a `commits` row stores the whole commit with every delta, so every reader that needs one field of a merge commit (the planner's ownership check, a retry's receipt lookup, a retention advance) decodes all of it.

The fourth limit is the tail. Until main folds, every reader of main replays the merge's WAL object. The fold then keeps the whole tail in memory with a sorted copy of each family, and no allowance charges for it.

Only the first limit returns an error. A user meets the other three as latency, memory, and read cost. For example, a merge of 99,000 file creates on a server whose allowance was raised to 2.5 GiB has a WAL object whose planned bound is about 536 MB. Main's other commits wait behind it, and every reader without that tail cached replays it until the next fold. A fork of 200,000 files can't merge as one commit, and its rebase replays in pieces, which [Merging a fork](merge.md#rebasing) says can report conflicts on names that the fork passed through.

## The design

This section describes one merge, from the request to the manifest that makes it visible.

A commit goes through the log. A fold or a compaction builds immutable segments off to the side, with bounded memory, and makes them visible with one manifest publication, and a segment that never gets published is collected once it's old enough. A merge is the same kind of work, so this design builds every merge's rows as segments and publishes them the same way. Every merge takes this path, whatever its size, so LoonFS has one planner, one publication, and one form of merge commit record. The price is that a merge of a few files costs more requests than one WAL put, as [Costs](#costs) estimates.

To merge a fork, LoonFS:

1. Starts the merge as a job in the process that received the request, looks up the receipt for the request's commit ID, and checks the range and both retention floors, as today. The plan needs no writer epoch.
2. Folds the fork, takes a snapshot of the fork's manifest that lasts the plan's budget, and reads main at its head, the "basis" `P`.
3. Classifies the fork's net change in inode order against main at `P`, and produces each inode's deltas. A merge that finds a conflict stops and publishes nothing.
4. Orders the merge's name changes, assigns each delta its position, derives the rows, and writes them into segments.
5. Checks what main committed after `P` against what the plan touched, first without pausing anything. It then folds main's tail, pauses main's queue, and checks the commits that are left, up to the WAL tip. `H` is the head at that tip.
6. Puts the next manifest, which folds what's left of main's tail, adds one run at `H + 1` that lists the merge's segments, and sets the head to `H + 1`.
7. Resumes the queue. The writer's next WAL object starts at `H + 2`.

If step 5 finds a commit that changed what the plan read or replaces, LoonFS plans the merge again.

### The plan's deltas and markers

[Row families](../specs/format.md#19-row-families) says that every row is derived from a single WAL delta or commit record without reading other rows. The fold derives a delta's rows in one function (`apply_committed_wal_delta_mut` in `loonfs-core`), whose inputs are the delta and its commit's sequence, commit ID, actor, and time. So the planner's output is deltas, the eight kinds in [WAL delta kinds](../specs/format.md#wal-delta-kinds), and the merge's rows are what that function produces from them. The commit ID and the actor come from the request. Revision, attributes revision, and access revision numbers continue main's from its rows at `P`. Unbinds, revokes, and summary rows point at main's existing events where they undo or extend them.

The merge's sequence and time aren't known while the plan runs, so the plan writes a "marker" in their place. The sequence enters the rows as every row's `committed_seq`, as a bind row's `bind_position`, and as a `trash` row's `deletion_seq` for the merge's own deletions and `removed_seq` for its restores. The time enters as every history row's `committed_at_ms` and a `trash` row's `deleted_at_ms`. Some delta fields contain the merge's own sequence or time as well. A file that the merge creates, or moves and also changes, has a revision delta whose `bind_position` is the merge's own bind, and the second leg of a move through a temporary name unbinds the merge's own temporary binding. A create's `child_created_at_ms` and its revision summary's `revision_committed_at_ms` are the commit's time. The plan writes the markers in those fields too.

The marker is per field, because some sequences in a merge's rows are main's. A summary row's `bind_position` can be main's existing binding, an unbind or a revoke points at main's event, and a restore's `trash` row is keyed by the deletion's original sequence.

| Families | The merge's sequence in the key | In a merge run |
| --- | --- | --- |
| `inodes` | Not in the key | Marker in `committed_seq` |
| `direntry_binds`, `direntry_child_binds`, `revisions`, `tombstones`, `attributes`, `access`, `content_layouts` | Inverted, after the entity's ID | Marker in every row, so the stored order is the decoded order |
| `trash` | First, as the deletion sequence | Marker for the merge's own deletions. It sorts above every real sequence, and every other `trash` row in the run removes a deletion at or below `P`, so the stored order is the decoded order. |
| `commit_deltas`, `merge_conflicts` (new) | First | Marker in every row |
| `commits`, `commit_receipts`, `merges` | First or last | Written by the publication with the real sequence |

The manifest's activity counters for the merge come from the deltas, as a fold's come from each commit record.

### Planning

The fork is folded first, so the fork snapshot's manifest contains every commit in the range, the plan reads no WAL tail of the fork, and the merge writes no pending pieces. The fork's snapshot is the temporary snapshot that a merge takes of its fork today, with a lifetime of the plan's budget (see [Budgets and collection](#budgets-and-collection)) instead of 5 minutes.

The plan takes no snapshot of main. It reads main as of `P`, the way any reader reads at a sequence: through whatever manifest of main is current, which it checks again as a cached view does. Every row that the plan reads at `P` stays in main's later manifests for the whole plan. A base compaction keeps the state at the retention floor and everything after it, and a retention advance can't pass a commit that is younger than the grace window of 24 hours ([Targets and the grace-window bound](../specs/format.md#targets-and-the-grace-window-bound)), so the floor can't pass a commit that main made during a plan of at most 6 hours, and the state at `P` stays readable.

The plan has two steps.

Step one classifies. The planner reads the fork's commits in pages, as today, and collects the "touched inodes", which are the inodes that the range's commits touched. It keeps them at an estimated 32 bytes each (about 32 MB per million inodes), and above a bound it writes the set as sorted chunks and merges them. Classification then runs in inode order and produces each inode's deltas, grouped by event. A "group" is the deltas of one change that the [change feed](../specs/format.md#67-change-feed) reports as one event: a create (create, bind, and first revision), a content change, a move (unbind and bind), a delete (unbind and tombstone), a restore (revoke and bind), an attributes change, or an access change. A group has no position yet. Step one keeps the groups in memory up to 16 MiB of deltas and writes the rest to temporary chunks, objects under segment keys that the age rule for unlisted segments collects. A merge whose groups fit in that buffer writes no chunk, and a merge whose groups don't fit writes them once more than a single pass would. (16 MiB is a choice, not a measurement.)

For each inode with a name change (a create, a move, a delete, or a restore), step one also keeps an "ordering entry" in memory. The entry contains the change's parent and name, the binding version that a move or a delete replaces, the inode's kind, main's edge of the inode and of the parents that the change uses, and the inode's place in the ordering's maps (which inode has the name on main, which directories that the merge creates the change needs, and whether the change still waits). An entry takes about 300 bytes plus about twice the name's length, so about 330 bytes with 13-byte names and 800 bytes with 255-byte names (estimated from the field sizes). Main's edges of the ancestors add one entry per distinct directory.

Step two orders and writes. It orders the name changes with the rules that the planner uses today ([Merging a fork](merge.md#merging)):

- A create, restore, or move into a directory comes after the create or restore of that directory.
- A change that takes a name comes after the change that frees it on main.
- A directory's delete comes after the moves of the inodes that leave it.
- A directory never moves under its own descendant.
- A delete below a directory that the merge deletes, with no moved inode between them, is left out, because the directory's recursive delete covers it.
- When moves wait on each other in a cycle, one of them first moves to a temporary name.

Step two chooses the temporary name by reading main at `P` (the inode's ID, with a numeric suffix if the name is taken), and adds the temporary name's slot to the touched set (see [Checking what main committed during the plan](#checking-what-main-committed-during-the-plan)).

Step two then assigns positions. The groups of the name changes come first, in that order, and every other group (content, attributes, and access) follows in inode order. Each delta gets its `delta_index` from its place in that sequence. Each group gets one `semantic_operation_index`, numbered 0, 1, 2, and so on, and the change feed, diff, and the activity counters group a commit's deltas into events by this index. Two orders matter here. A slot's state is the newest row across its edges ([Directory bindings](../specs/format.md#13-directory-bindings)), so a delta that frees a name has to come before the delta that takes it. And a consumer of the change feed applies a commit's events in order, so each event has to be valid after the ones before it. The ordering keeps both.

Step two reads the groups back in inode order (from memory, and from the chunks if step one wrote any), derives each group's rows, and writes them into one segment writer per family. These are the writers that the fold and compaction use, with at most 32 puts and 32 MiB in flight. The families keyed by inode come out in key order. The slot index (`direntry_binds`), `content_layouts`, `trash`, and `commit_deltas` aren't keyed by inode, so the planner writes them as sorted chunks under segment keys and merges the chunks with the compaction's sorted iterators.

Three more parts of the plan keep sets in memory, which grow with the number of name changes or conflicts: the names that the merge's own inodes take or free, the subtree check, and a replay's skip of dependent changes.

Classification applies the conflict rule first, and then checks the subject's rights for each change that passes it, with the rules in [Merging a fork](merge.md#merging). The rights walk reads main's access rows at `P`, except for a directory that the merge creates. For that directory, it reads the access row that the merge writes, and then continues with main's rows from the first ancestor that exists at `P`, as today's writer sees the rows of earlier operations in the same commit. So a merge with a conflict returns `merge_conflict` and no rights error, as today, and a replay checks rights on the parts it applies.

A re-plan is a new plan, with a new basis, a new snapshot of the fork, and a new budget. LoonFS makes at most four plans, as today, and then returns `store_contention` with the cause `lost_race`. A plan that doesn't finish within its budget publishes nothing and returns `merge_too_large` with a new bound, `plan_budget`, and no split hint. The caller can still merge a shorter range of the fork.

### Reading a merge run

A segment of a merge run is opened only through its run entry, which records the commit's sequence and time. One place in the block decoder replaces each marker with the run entry's sequence or time. A cached decoded block belongs to the run entry it was decoded under, and a reader that finds a marker without a run entry returns `namespace_corrupt` instead of the marker. A reader that seeks to a key in a merge run, or probes its filter, replaces the sequence in that key with the marker.

The design keeps one property that every reader can be tested against. A merge run's rows, read through any reader, are the rows that the fold's derivation produces from the same deltas at the run's sequence and time. A test checks it by folding the same deltas as an ordinary commit record at the same sequence in a second store, and comparing the two namespaces through every reader: seeks, scans in both directions, filter probes, compaction, the change feed, diff, and grep.

A compaction that rewrites a merge run writes the real sequence and time into its output, because the output's `run_seq` is the window's position. A fork's first manifest copies the run entry.

### A merge's commit record as rows

A merge's commit record has one form. The `commits` row stores the commit's header and merge record, and the deltas are rows of a new family, `commit_deltas`. Each `commit_deltas` row stores one group and is keyed by the sequence and the group's `semantic_operation_index`. A replay commit stores its conflict report the same way, in `merge_conflicts`. Both families join the `commits` [family group](../specs/glossary.md#family-group). The ownership check, a receipt lookup, and a retention advance read the small header. The change feed, diff, and grep read the deltas in pages, and they resume inside a commit by seeking to the next group. An ordinary commit keeps its whole record in its `commits` row.

### Checking what main committed during the plan

The run is valid if no commit in `(P, H]` changed a row that the plan read or replaces, or a rule that the plan applied. The plan records, beside its segments:

- the "touched set", a sorted list of every inode whose rows the merge replaces or reads to classify, and every directory slot (parent and name) that it binds into or vacates, including the slot of a temporary name,
- the "path set", every directory on the path to a parent that the merge binds into or to an inode that it changes, which grows with the distinct directories on those paths, and
- the smallest deletion sequence among the deletions that the plan relies on being in main's trash (one it restores, or one that already hides an inode it would delete).

For the commits in an interval, the check reads the inodes and slots that each commit touched, and finds an intersection if:

1. a commit wrote any row of an inode in the touched set,
2. a binding change used a slot in the touched set (a name that main took after `P`),
3. a commit wrote a path set directory's own binding, tombstone, or access row (a change to the directory's children doesn't count, so a create under `/` doesn't intersect),
4. a commit wrote any row of an inode that has, among its ancestors in main at `H` (or, for an unbind, at its old parent), a directory that the merge deletes or moves, which is the conflict rule's subtree check applied to the commits after `P`, or
5. at the end of the interval, the range rule or the merge floor rule fails, or main's retention floor is at or above the smallest deletion sequence that the plan relies on.

Condition 5 covers a floor advance during the plan. Main's writer can advance the floor while the plan runs, because the publication pauses main's queue only in the second stage of the check. For example, the plan restores a deletion at sequence 980, main's writer puts a floor object from its queue that raises main's floor to 990, a maintainer's fold records it, and a base compaction removes the rows under the expired deletion. The check finds 990 at or above 980, so LoonFS plans again and reports the restore as a conflict. The check runs against every basis that the publication builds on. So if another host claims main after the pause starts and advances the floor, the merge's put loses or its session is fenced, and the check against the new basis finds the new floor in the same way. (The rows that the merge's unbinds and revokes point at don't need a check of their own. They're current at `H` by conditions 1 to 3, and a base compaction keeps every current row except one hidden by an expired deletion, which condition 5 covers.)

After a plan of several hours, `(P, H]` can contain millions of commits, most of them folded, so the check runs in two stages:

1. Without pausing anything, the process reads main's head `P'` and checks `(P, P']`, reading `commits` rows and the tail like any reader. It repeats from the head that it checked until at most 256 commits are left to check, or the plan's budget ends. (256 is a choice, not a measurement.) This stage can run on a host that isn't main's writer.
2. The process folds main's tail with an ordinary fold, so that the fold under the pause covers only the WAL objects that arrive in between. It then pauses its own publication queue for main. If its writer session for main doesn't have the writer epoch, it first claims the epoch and writes its fence ([Writer ownership](../specs/format.md#61-writer-ownership)). It reads the WAL above the tip that its fold reached, or above its fence, including any floor object that main's writer put before the pause, and checks the commits from the last head that stage 1 checked through `H`, which are at most 256 plus what arrived since.

An intervening merge run is read through its own segments. Its inode-keyed families and its slot index list what it changed in sorted order, so a merge run doesn't need a separate list of what it touched. Conditions 1 to 3 compare two sorted lists, reading the intervening merge's keys once. Condition 4 walks the ancestors of each inode that the intervening merge changed, but only when this merge deletes or moves a directory. Both checks run in the first stage, because no other merge can publish while this process's queue is paused and its session has the writer epoch.

### The publication

The plan runs in the process that received the request and has no writer epoch, so it doesn't fence main's writer for hours. The publication runs in that process's writer session for main, which claims the epoch only if it doesn't have it. The epoch keeps another host's writer from putting a WAL object between the tip that the publication reads and its manifest put, and the pause keeps this process's own queue from doing so. The publication then runs in main's fold path, under the fold lock (see [Folding and compacting a namespace](maintenance-hosting.md#folding-and-compacting-a-namespace)).

The new manifest folds main's tail through the WAL tip, adds the "merge run" at `run_seq = H + 1`, which lists the merge's segments and the publication's segments for `commits`, `commit_receipts`, and `merges`, and sets `head_seq` to `H + 1`. If the tail under the pause has only fences and floor objects, as it does when no commit arrived after the fold in stage 2, the fold adds no run of its own ([Fold protocol](../specs/format.md#fold-protocol)), and the merge run is the run at the head. Otherwise the fold's run is at `H`. The merge is ordered after every commit acknowledged before the pause, it's never in the WAL tail, and no run is above the head.

The publication writes the header row, the receipt, and one merge index row per covered range, so nothing proportional to the merge happens during the pause. The pause lasts for the fold of a short tail, a check of at most 256 commits plus what arrived since, and one manifest put. It never waits for the compaction lock. A compaction unit keeps that lock for as long as the unit runs, so a publication that waited for it would stop main's commits for that long. A compaction on the same host, or any publisher on another host, can therefore win the manifest number, and the rules below handle that.

The publication keeps four rules that a fold doesn't:

- **The writer's epoch.** A fold rebuilds over any winning manifest, even another writer's claim. A merge publication publishes only while the basis's `writer_epoch` is the session's. Otherwise the session is fenced, and the publication doesn't put its manifest. That happens when a commit on another host claims main between the start of the pause and the put.
- **One put per pause.** If the put loses to another publisher's manifest (a compaction, a maintainer's fold, or another host's claim), or the session is fenced, the queue resumes at once. The publication goes back to the first stage of the check, over the winning manifest, for the commits that it hasn't checked, without planning again, because a claim or a compaction changes no row that the plan read. A lost or fenced publication doesn't count as a plan. An intersection is what makes a new plan. The publication repeats until the plan's budget ends, and then the merge returns `store_contention` with the cause `lost_race`. Each lost put costs main's writers one more short pause. A measurement on S3 saw a manifest put lose every attempt it was allowed while compaction units published about four manifests a second (the lab record `analysis/box-campaign-20261125.md` of the lab repository), which is why a pause covers one put and not a series of retries.
- **No WAL put in between.** The queue stays paused from the read of the tip until the publication learns the put's outcome.
- **An unknown outcome.** The session releases its writer epoch just before the manifest put, as it does before a commit's WAL put, and after an unknown outcome it claims the namespace again before it publishes anything. The claim takes the next manifest number with put-if-absent ([Writer ownership](../specs/format.md#61-writer-ownership)). If that number contains the merge's manifest (the same publication ID), the claim collides and claims the number after it, and the merge committed. If the number is free, the claim takes it, and a delayed put of the merge's manifest can't be applied. The session then reads the receipt from its new basis, and a retry with the same commit ID returns the commit or plans again.

On the publishing host, the writer position and every cached view move to the merge's head only from the manifest that the put made durable, after the boundary check, never from the value that the publication planned to write.

### Budgets and collection

A namespace's [collection](../specs/glossary.md#collection) deletes an unlisted segment once it's 24 hours old (the [grace window](../specs/glossary.md#grace-window)). A compaction unit bounds this exposure with a 12-hour budget plus the completion bound. A merge's plan gets a budget too, checked before the manifest put, and it has to be shorter, because the fork's content has to stay rooted for the whole plan, and the pool's collection relies on this inequality ([Content roots](../specs/format.md#119-content-roots)):

```text
POOL_ROOTS_READ_BUDGET_MS + GC_SAFETY_MARGIN_MS
    + OPERATION_SNAPSHOT_TTL_MS + PROVIDER_PUBLICATION_REQUEST_BOUND_MS
    < GC_GRACE_WINDOW_MS
```

With a plan budget `B` in place of the 5-minute snapshot lifetime, the left side is 12 hours + 5 minutes + `B` + 5 minutes, so the inequality is true while `B` is shorter than 11 hours and 50 minutes, and a compaction's 12 hours and 25 minutes doesn't fit. This design sets `B` to 6 hours (a choice, not a measurement). The same terms also set `MAX_TAIL_COMMIT_AGE_MS`, which would fall to 5 hours and 50 minutes with `B` in place of the snapshot lifetime. The plan reads no fork tail, so that constant doesn't change.

### The request

A plan over millions of inodes could take minutes to hours (not measured), so it can't run inside one HTTP request. The merge route starts the plan in the process that received the request and waits for it for up to 10 seconds (a choice, not a measurement). If the merge commits within that time, the route returns the commit's header with a count of events. Otherwise it returns a response that says the merge is running. The same request sent again with the same commit ID returns the commit once its receipt exists, `merge_conflict` while the process still has that outcome, or the running response again, and after a restart the process plans again. A merge that commits within the wait returns its result to the first request. The plan runs as a compaction unit does, with a budget and no saved progress, so it fits [Maintenance hosting](maintenance-hosting.md).

Each plan uses one of a small number of plan permits, as a compaction unit uses a compaction permit. The number is a setting (4 by default, a choice, not a measurement). A merge that waits for a permit waits inside its job, and its request returns the running response when the 10 seconds pass.

### A rebase

A replay commit is one publication in the new fork, planned on the fork's whole unmerged range, with the same rule for skipping the conflicting part of an inode's change. Because the replay isn't split into pieces, it doesn't report a conflict on a name that the fork passed through, doesn't apply a state that the fork passed through, and judges a directory's delete together with a skipped move of a file out of it. The new fork is private unless a resolver (the program or person that resolves the conflicts, as in [Merging a fork](merge.md#rebasing)) starts early, so the check usually finds no commits after `P`.

## A worked example

This section follows one merge, first with a commit to main that doesn't overlap it, and then with one that does.

Main's manifest 40 has `folded_wal_no` 300 and a folded head of 1,000, and WAL objects 301 to 305 contain commits 1,001 to 1,010. Another host has main's writer epoch. The fork was forked at 950, and its commits change `/docs/a.txt` (inode `A`), create `/docs/b.txt` (inode `B`), and delete the directory `/tmp/old` (inode `C`).

1. The route starts the merge. No receipt exists, and main's merge index has no row for the fork, so the range starts at the fork point.
2. The process folds the fork, takes a 6-hour snapshot of the fork's manifest, and reads main at `P` = 1,010.
3. Step one classifies. `A`'s revision 3 on main was committed at 900, at or below the fork point, so it's the fork's own, and the planner produces a content group with revision 4. `B` doesn't exist on main and its slot is free, so the planner produces a create group (`B`'s create, binding, and revision 1). `C`'s binding is the fork's own and main's diff below `/tmp/old` is empty, so the planner produces a delete group (`C`'s unbind and tombstone). `B` and `C` have ordering entries. The touched set is `A`, `B`, `C` and their three slots, and the path set is `/`, `/docs`, and `/tmp`.
4. Step two finds no dependency between `B` and `C`, so their groups come first in inode order, then `A`'s. `B`'s group is deltas 0 to 2, `C`'s is 3 and 4, and `A`'s is 5, with operation indexes 0, 1, and 2. Step two derives the rows and writes them into small segments, one for each family that has a row.
5. A client commits 1,011, which creates `/src/main.rs` (inode `D`), in WAL object 306. The first stage finds no intersection, because `D` and its slot aren't in the touched set, the create doesn't change `/`'s own rows, `D`'s ancestors aren't below `/tmp/old`, and the floor and merge index haven't changed. Fewer than 256 commits are left to check, so the process folds WAL objects 301 to 306 into manifest 41. It then claims main's writer epoch, which publishes manifest 42 with the next epoch, writes its fence at WAL object 307, and pauses its queue. It finds nothing above 307, so `H` = 1,011.
6. The process puts manifest 43, which folds the fence at 307 without a run, adds the merge run at 1,012, and sets the head to 1,012. `B`'s binding row is stored with the marker and decodes to the position `{1012, 1}`, which is `B`'s binding version.
7. The process resumes its queue, and its next commit, 1,013, is WAL object 308.

A reader with manifest 42 and the tail through 307 cached finds WAL object 308 starting two sequences above its head, runs discovery under the rule in [The sequence gap](#the-sequence-gap), and loads manifest 43.

If commit 1,011 had written a new revision of `/docs/a.txt` instead, the check would find `A` in the touched set, and LoonFS would plan again from 1,011. `A`'s current revision would then be foreign, so the plan would return `merge_conflict`, and collection would delete the first plan's segments after the grace window.

On a single server whose process already has main's writer epoch, a merge of one file into a namespace whose tail is folded takes the same steps without the claim and the fence. The process pauses its queue, finds the tail empty, and puts one manifest that adds the merge run and no other run. (A session that sent a WAL put without learning its outcome has no epoch, so it claims and writes a fence first.)

## What changes in the format and the log protocol

This section states each rule that changes and why the change is safe.

### The sequence gap

[Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal) requires every data WAL object to continue the preceding head, and every fence and floor object to record it. After a merge publication, a reader on an older manifest finds the next WAL object above its head. That can be the writer's next data WAL object, which starts above the reader's head, or the writer's next fence or floor object, which records the merge's head. (Neither case needs a race. A reader whose view is cached on the manifest before the merge's finds that WAL object on its next probe.) Today, discovery treats such an object as invalid, reloads the current manifest, and starts again from it when it's newer, so discovery recovers. A warm probe of a cached view treats it as corruption, so one read returns `namespace_corrupt` before the runtime drops the view.

The new rule is that a WAL object whose head is above the replaying head (for a data WAL object, its prior head) means that a newer manifest published a merge. Discovery reloads the current manifest and reports corruption only if it's the same one, and a warm probe returns "needs discovery", as it does today for an object of another writer epoch. Every replay validates objects through the same check, so discovery, warm probes, a fork's read of its source's tail, the writer's discovery after a collision or a claim (which is how a retention advance reads the log, because it's planned and put from a writer session's position), and the pool collection's read of a tail all follow the rule. No reader may treat an object above its head as the end of the log. Main's writer is the one process that raises the head without a WAL object, at its epoch with its queue paused, and it folds through the tip it found. So the first WAL object above the new manifest's `folded_wal_no` continues the new head, or records it if that WAL object is a fence or a floor object, and a gap above the current manifest's head is still corruption.

The change feed has no hole at a merge's sequence. It reads the commits above the manifest's folded head from the tail and the commits at or below it from rows, and a merge's commit is always at or below its manifest's head, with its deltas in `commit_deltas`.

### Floor objects

A merge's restore and a floor advance have to be ordered against each other, because a restore of a deletion that has already expired could bring back an inode whose rows a base compaction removed ([The expired-deletion invariant](../specs/format.md#the-expired-deletion-invariant)). A restore in a WAL commit is ordered against a [floor object](../specs/glossary.md#floor-object) by its WAL number. A merge's restore is in a manifest, so the writer epoch orders the two instead. Only a namespace's writer writes floor objects. A floor object is one item of a writer session's publication queue, put at the session's writer epoch. A process that has no writer session for the namespace opens a writer session, which claims the epoch as it would for a commit, before it queues the advance ([Retention advance in the WAL](retention-advance-in-the-wal.md)).

So no floor object can be written between the publication's read of the tip and its manifest put. A floor object comes from one of three places, and none of them can put a floor object in that window:

- The publishing session's own queue, which stays paused from the read of the tip until the publication learns the put's outcome. A WAL put that the queue sent before the pause, whether a commit or a floor object, can still be in flight if the session never learned its outcome. A writer session releases its writer epoch immediately before it sends any WAL put and takes the epoch back only when it learns the outcome ([Failed and unknown outcomes](../specs/format.md#64-failed-and-unknown-outcomes), and for a floor object [Retention advance in the WAL](retention-advance-in-the-wal.md)). So after an unknown outcome the pause claims the epoch and writes a fence, and the put in flight is either found by the fence's discovery and folded by the merge's manifest, or it collides, or it's written below the fence where no reader replays it (the argument in [Publication and group commit](../specs/format.md#63-publication-and-group-commit) for a session's own put whose outcome is unknown).
- A session in another process that claims main's writer epoch after the publishing session. The claim publishes a manifest, so the merge's put loses at that number, or the basis that the publication reads has a `writer_epoch` that isn't the session's, and the publication doesn't put its manifest (see guarantee 3 in [The guarantees of the log protocol](#the-guarantees-of-the-log-protocol)). The publication then goes back to the first stage of the check, and its next pause claims main again and reads that session's floor objects in the tail.
- A session whose position still has an older epoch. Its position's tip is below the publishing session's fence, so its floor object's put collides with an existing WAL object, or it's written at a number that a fold already covered, where no reader replays it ([Writer ownership](../specs/format.md#61-writer-ownership)).

That leaves two orders, and both are safe.

An advance that main's writer planned before the publication, and that its queue puts after the publication, is put from the session's position on the merge's manifest. So the floor object records the merge's head, and replay checks it against that head like any other floor object. The advance's target was planned from an older manifest, so it's at or below `H`, and it can pass a deletion that the merge restored. That does no harm, because the restore is ordered first. For example, suppose that the merge in [A worked example](#a-worked-example) also restores a deletion at 1,005. It publishes manifest 43 with head 1,012, and an advance to 1,008 that was planned from manifest 42 and queued during the pause is put at WAL object 308 with head 1,012. Every fold that records the floor of 1,008 covers WAL object 308, so it builds on manifest 43 or a later manifest, where the deletion's latest event is the merge's revoke. The deletion isn't in the trash at any head from 1,012 on, and a base compaction removes rows only under a deletion whose latest event is `set`, so no row of the restored inode is removed. This is the same order as a restore that a writer commits in the WAL ahead of a queued advance.

A floor object that main's writer put before the publication read the tip is in main's tail at that tip, whether it came from this session's queue before the pause or from another host's session before the claim. The second stage of the check reads it, condition 5 compares its floor with the deletions that the plan relies on, and the merge's manifest folds it and records its floor.

So replay keeps the requirement that a floor object records the replaying head. (The lab repository's model `verification/tla/merge_publication` checks floor objects that only the writer puts, together with the gap rule, and finds no counterexample within its bounds, which include a floor object that the writer's queue puts after a merge that restored a deletion at or below the new floor.)

### The guarantees of the log protocol

[A summary of the log protocol](../specs/format.md#26-a-summary-of-the-log-protocol) lists eight guarantees:

| Guarantee | Change | Reason |
| --- | --- | --- |
| 1. The visible log is unbroken above `folded_wal_no` | Changes: it continues the manifest's head, and a WAL object whose head is above the replaying head (for a data WAL object, its prior head) means a newer manifest | The merge is committed by its manifest put, and the gap rule sends readers to that manifest. |
| 2. Every acknowledged commit is in the visible log or the current manifest | Changes: or in a merge run of it | The writer acknowledges a merge only after its put succeeds and the boundary check after it passes. |
| 3. Writer epochs never decrease, and a fence blocks older writers | Unchanged | The publication writes no WAL object, and a merge's manifest put loses at a number that a newer claim took, so a merge can't commit at an older epoch. |
| 4. Fresh discovery observes every acknowledged commit | Unchanged | Discovery loads the newest manifest, which lists the merge run. |
| 5. Cached views and writer positions never move backward | Unchanged | A view moves to the merge's manifest through discovery, or on the publishing host from the manifest that the put made durable. |
| 6. Pieces stay readable until folded | Unchanged | The publication folds main's tail as a fold does, and the merge has no pieces. |
| 7. Identical retries don't commit twice | Unchanged, with a new argument | After an unknown outcome the session claims before it publishes, so a delayed put is the committed merge or can't be applied. The range rule stops a second process from publishing the same range under the same commit ID. |
| 8. Current and snapshot manifests keep their segments | Unchanged | The merge's segments are protected by their age until the publication, and the fork's by the plan's snapshot. The plan reads main through its current manifest. |

### Two details of the commit record

A merge's deltas need a `semantic_operation_index`, which the change feed, diff, and the activity counters read to group a commit's deltas into events. The change feed maps each group to one event and reports any other shape as corruption. So a merge assigns one index per group, in the order of its deltas, and never one per inode. An inode that the merge moves and whose content it changes has two groups, a move and a content change.

A merge's fingerprint ([Merge commits](../specs/format.md#b5-merge-commits)) hashes the namespace, the actor, the fork, the merge's own range, and the message, and not the operations, so this design doesn't change it. A retry is identified by its commit ID and that fingerprint, as today.

### Changes to the specs

In the storage format:

- [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal) gains the gap rule for every WAL object whose head is above the replaying head, and the log protocol summary changes guarantees 1 and 2.
- [Manifests, runs, and segments](../specs/format.md#71-manifests-runs-and-segments) gains the merge run, whose entry records the commit's sequence and time, and the basis checks for a manifest publication gain the merge publication and its epoch rule.
- [Row families](../specs/format.md#19-row-families) gains the two new families and the markers. Every row of a merge run is still derived from one delta or commit record, so the section gains no exception.
- [WAL records](../specs/format.md#a5-wal-records) loses the merge record of a WAL commit record, the conflict report's count toward the WAL object's size, and the rule that a merge is alone in its WAL object, because the WAL never contains a merge. The merge record moves to the `commits` row in [Metadata rows and row keys](../specs/format.md#a6-metadata-rows-and-row-keys), where a merge's `commit` row doesn't match any WAL commit record.
- [The expired-deletion invariant](../specs/format.md#the-expired-deletion-invariant) covers a restore in a merge run as well as one in a WAL object. It states that no manifest restores a deletion that an earlier manifest has at or below its floor with its latest event `set`, because condition 5 refuses that restore and the publication's epoch and pause order it against every floor object.
- [Merges](../specs/format.md#merges) gains the five conditions of the check, in place of the writer's preconditions, and [Content roots](../specs/format.md#119-content-roots) uses the plan's budget in its inequality.

In the API, the merge route becomes a job. Its response becomes the commit's header with a count of events, or a running response, in place of the `Commit`. `merge_too_large` keeps the `plan_budget` bound and loses the other three bounds and `split_at_seq`, and a merge doesn't return `server_busy`, because the merge allowance goes. The route's description loses the rule that a merge is the sole commit in its WAL object.

## What goes and what stays

This section lists the code that goes, as counted at main `929504525` (non-test lines unless marked).

The operation form of a merge goes. The writer's planning of a merge's operations goes, and so does the operation form that the planner builds for it (`merge/operations.rs`, 352 lines, which step one's groups replace). The `namespace_head` precondition goes, because the check of main's commits after `P` replaces it for a directory's delete or move, and the second leg of a temporary move no longer goes by path. The merge allowance and its estimate go (`merge/memory.rs`, 125 lines, and its reservation in the publisher, which the next paragraph counts). The WAL-object and touched-inode bounds, the split hint, and its probes go (`merge/size.rs`, 274 lines and 37 test lines). Merges and replays in pieces go, with the loops in `loonfs-merge` that follow a split hint, the pending requests pinned to a split point, and the per-piece reports, and with the tests of split replays (1,185 test lines).

Because the WAL never contains a merge, the writer's code for merges goes too, about 550 lines (estimated from the spans of the functions). This covers the merge's candidate and its temporary moves (about 95), the merge's sizing in WAL bytes (85), the merge's admission in the commit session and its fork inode IDs (about 125), the temporary move's exemption from the rights checks (about 30), the merge-alone batch (about 40), the allowance's reservation in the publisher (about 140), and the WAL decoder's checks of a merge record (31), which move to the `commits` row. Replay, the commit session's overlay, and the publish batch then handle ordinary commits and floor objects. Replay never meets a merge record, the overlay never applies a fork's inode ID or a temporary move, and the batch never has to keep a merge alone. About 1,850 test lines of the writer's merge path go or move to tests of the check.

The ordering and the temporary names stay (`merge/order.rs`, 595 lines), in step two, and so do classification, the net change, the state reads, the target checks, and the replay's skip rule (about 2,240 lines of `loonfs-core/src/merge`). The `rebasing` state stays, because creating the new fork and sending the replay are still two steps.

The new code is the chunks of step one, the positions of step two, the merge run and its marker decoding, the publication with its pause and two-stage check, the two families with paging over them, the gap rule, and the route's job and permits. That's about 3,000 to 4,000 lines and two to three times that in tests (estimated from the sizes of the fold, compaction, and route code that it resembles, not measured).

## Costs

This section lists what the design costs, each marked as measured, estimated, or not measured. Every merge takes the same path, so a merge of one file pays these costs too, and its requests are listed first.

- **Requests for a one-file merge.** A merge that creates one file writes nine segments (`inodes`, both binding families, `revisions`, `content_layouts`, `commit_deltas`, `commits`, `commit_receipts`, `merges`), one snapshot record, and one manifest. It also writes the fork's fold (about seven segments and a manifest) when the fork has a tail, main's fold before the pause (about seven segments and a manifest) when main has a tail, and a claim manifest and a fence when this process isn't main's writer. That's about 11 puts at the least and about 29 at the most, plus about 20 to 30 reads for the receipt, the ranges, the snapshot's check, the boundary reads, and the tip. Today a one-file merge is two puts (one WAL object and one snapshot record) and its reads (estimated from the steps, not measured).
- **Runs.** Each merge adds one merge run, and the fold before the pause adds a run when main's tail has commits. The fold under the pause adds a run when a commit arrived after the fold before it, and no run otherwise. A steady stream of merges adds runs faster than a writer's folds do (a steady writer folds about 62 times in 15.6 minutes, estimated in [A window for replaced manifests](replaced-manifest-window.md#costs) from a measured build), and compaction absorbs them with the next tier. The merge rate at which runs outpace compaction hasn't been measured, and neither has what one more run costs a read.
- **The writer's pause.** Every merge pauses main's queue for one fold of a short tail, a check of at most 256 commits plus what arrived since, and one manifest put (not measured). Today a merge is alone in its batch, and the writer plans the whole merge again inside its queue. The lab measured a clean merge of 1,000 changes on S3 at 1.3 to 2.0 seconds and 1,007 to 1,273 store requests (the lab records `analysis/battery-20261118.md` and `analysis/box-campaign-20261125.md`), and main's other commits wait for the writer's share of that time.
- **The fold before the pause.** Folding main's tail before the pause costs one more manifest per merge when main's tail has commits. When commits also arrive between the two folds, it costs one more run, because the fold under the pause then adds a run of its own (estimated).
- **Lost puts.** Each manifest put that loses to a compaction or another publisher costs main's writers one more short pause, and the publication goes back to the first stage of the check for the commits that it hasn't checked (not measured).
- **Readers.** Every reader with a cached view on an older manifest runs discovery at each merge, as it does after each fold (estimated).
- **The fork's snapshot.** Every plan takes a temporary snapshot of the fork's manifest, as a merge does today, which expires after the plan's budget instead of after 5 minutes. A snapshot's record stays, and keeps its manifest and every segment that the manifest lists, until `T` (24 hours) after its expiry ([Snapshot cleanup](../specs/format.md#117-snapshot-cleanup)). So a fork keeps the segments of each manifest that a merge read for about 30 hours after the plan starts, where today it keeps them for about 24 (estimated).
- **Memory.** A merge's memory grows with its name changes (creates, moves, deletes, and restores) at about 300 bytes plus twice the name's length for each, about 330 MB per million creates with short names (estimated from the field sizes). Today the allowance charges about 27 KB for each create. On top of that, a plan uses the 16 MiB buffer of step one, 32 MiB of puts in flight, 64 MiB of decoded input in the sorts, and about 32 MB per million touched inodes before the set spills (estimated). The plan's budget is what bounds the ordering entries, so an operator sizes the plan permits for the largest merges that the server takes (not measured).
- **Plan permits.** A short merge can wait for a permit behind long plans in the same process, and its request returns the running response until it gets one (not measured).
- **The check.** The first stage reads every commit after `P`, which grows with main's commit rate and the plan's length, and an intervening merge run's keys (not measured).
- **Planning time.** Reads of the fork and of main for each changed inode (not measured, for one million or ten million inodes).
- **Storage.** The slot index, layouts, `trash`, and `commit_deltas` rows are written twice, and the groups of a merge that doesn't fit in step one's buffer are written once more as chunks (estimated). A merge with `N` bytes of rows that plans twice stores about `2N` for up to a day (estimated). The deltas family adds a copy of each delta, as the `commits` row does today.
- **Fenced publications.** Where requests for main reach more than one host, a commit on another host between the publication's claim and its put fences the publication. The queue resumes, and the publication goes back to the first stage of the check and claims main again when it pauses. The plan itself has no epoch, so it isn't lost (not measured).

## Alternatives considered

- **Today's design, with its limits stated as product limits.** A small merge is one WAL put, the size limits are explicit errors with split hints, and each piece is a correct merge. But the writer's pause, the `commits` row, and the tail grow with the merge without any error, and a rebase in pieces depends on where the pieces fall.
- **Today's planner below a threshold.** Merges below 8 MiB of planned bytes would keep today's planner and publication, and larger ones would take this design. Two planners would then apply different rules to the same merge, and the limits that no error reports (the writer's second planning and the merge-alone batch) would stay for the largest merges below the threshold.
- **A WAL commit for a small merge.** The same planner would publish a merge below a threshold as one WAL commit, with main's writer replacing the markers in the deltas. That needs a threshold, markers replaced in the writer, merge records in the WAL format, two forms of commit record, and a large publication that most merges never exercise. The gain is the requests and the run that a small merge saves: about 9 to 27 puts and a few reads, one manifest, and one run per merge (estimated in [Costs](#costs)).
- **The bound edge in place of the newest edge.** If a slot's state were the edge that's bound, whatever its position, a merge's name changes wouldn't need an order across inodes, and the temporary names would go. But the rule changes every reader of a slot for every commit. A consumer that applies a commit's events in order would see two files under one name for a moment. And the change removes the slot part of the ordering and nothing else, because a bind still has to follow the create of its parent directory, and a directory's delete still has to follow the moves out of it.
- **Ordering just the changes that another change depends on.** A create into a directory that the merge doesn't change, at a name that no other change of the merge touches, depends on nothing, so it could be streamed without an ordering entry, which would save memory. But finding the names that are reused needs a pass over every name change sorted by slot, and a mistake in what counts as independent corrupts a slot's state.
- **Checking by the newest row of each touched inode.** The check could look up the newest row of each inode and slot in the touched set instead of reading the commits after `P`, so its cost would follow the merge and not main's commits. But conditions 3 to 5 need the commits anyway, so it would be a second check beside the first.
- **Storing every commit's deltas as rows.** Every commit, not just a merge, would store its deltas in `commit_deltas`, so LoonFS would have one form of commit record. That costs a second row for every small commit.
- **The log as the commit point.** The merge's commit is a small WAL object that refers to the staged segments, so order, unknown outcomes, epochs, and floor objects follow today's rules with no protocol change. A reader that replays to that object stops there and reads main as of the commit before it, and the writer's next step is a fold that lists the segments. The merge isn't acknowledged until that fold, so reads don't wait, but the writer's own commits do, and after a crash between the two puts they wait for a maintainer's fold. It needs a new WAL record kind that refers to segments. And the merge is committed while no manifest lists its segments, so the namespace collection, which reads no WAL objects, would need a new root, or it would delete a committed merge's rows once they're a day old. That last point is why this design makes the manifest the commit point.
- **Accepting a floor object with an older head.** Any process could write a floor object without the writer epoch, and replay would accept a floor object whose head is at or below the replaying head, apply its floor, and keep the replaying head. A floor object whose writer's discovery ran before the merge's manifest put records the older head, and its put can come after the manifest put. A reader can then see a deletion expire and come back, which [Expiring deletions and creating floor objects](../specs/format.md#expiring-deletions-and-creating-floor-objects) says can't happen. For example, if the merge restores a deletion at 1,005 and such a floor object raises the floor to 1,008, a reader that replays the floor object on the manifest before the merge's sees the deletion expired, and after it loads the merge's manifest, it sees the deletion restored. The rule also loosens the head check on floor objects for every replay, so a floor object written with a wrong head for another reason is accepted without an error. And every merge takes this path, so every merge exposes readers to the race. (The lab repository's model `verification/tla/merge_publication` finds the order in which a reader sees the deletion expired and then restored.)
- **Making a floor object with an older head dead.** Any process could write a floor object without the writer epoch, and a floor object that records the head from before the merge would be dead. Either the merge's manifest reserves the next WAL number, so a floor object that races into `tip + 1` is dead by the existing rule, or replay skips a floor object whose head is below the replaying head, and its writer's check after the put retries. Both break the same way. The check after the floor object's put can run before the merge's manifest exists, so the advance reports a floor that the merge's manifest then drops, and a reader that replayed the floor object sees the floor go down (the lab repository's model `verification/tla/merge_publication` finds this order for the second form).
- **A commit across several WAL objects, closed by a marker.** Each part is still bounded by a WAL object, the tail and the fold still keep the whole merge in memory, and every reader needs a rule for an unclosed commit.
- **Listing the fork's own runs in main.** The fork's rows record the fork's sequences, which overlap main's above the fork point, and segments would need shared ownership across namespaces.
- **Stamping the sequence at publication.** The writer would rewrite the whole merge during its pause, which brings back the second limit.
- **Steps that the client drives.** Each step would continue the previous step's plan, so the server would need a durable record of the plan's progress.
- **An incremental re-plan.** Planning only the intersecting inodes again needs a way to remove the first plan's rows for them, because a second set of segments at the same sequence adds rows and hides none.
