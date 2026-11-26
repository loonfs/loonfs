# Retention advance in the WAL

This note describes how a process advances a namespace's [retention floor](../specs/glossary.md#retention-floor) without the writer epoch. The process writes the advance to the WAL as a "floor object", which is a WAL object with no commits that records the new floor. Every floor object is written the same way, by discovery and a put at the next WAL number, whether or not the advancing process has the namespace's writer session. The WAL orders the advance against every restore, so the writer validates commits against the new floor from the floor object's number onward, and no writer session is fenced. Readers see the new floor when the fold that follows the advance records it in a manifest.

## The problem

This section describes why a retention advance has to be ordered against restores, and what that order costs if the writer epoch provides it.

A retention advance moves the floor past deletions, and a base compaction over a manifest whose floor reaches a deletion removes the rows that the deletion hides. A restore is a commit in a WAL object, and a writer validates the restore against the floor of the state that it planned from. If an advance published a manifest with the new floor, nothing would order that manifest against a restore that the writer had already validated. For example, a writer could validate a restore of a deletion at sequence 10 while its manifest has floor 5, another process could publish a manifest with floor 20, and the writer could then commit the restore at sequence 22. A base compaction over the manifest with floor 20 would remove the deleted subtree's rows, and the fold of commit 22 would bind an item whose rows are gone.

The writer epoch could provide the order. A process would claim the epoch (a claim manifest and a [fence](../specs/glossary.md#fence)), fold the tail, and publish the floor in a manifest, so no restore validated against the old floor could be committed after the new floor. But a process that advances a namespace whose [writer session](../specs/glossary.md#writer-session) is in another process would then fence that session, whose next request would return `writer_fenced`, and the application would pay another claim and fence (about 200 ms) to continue. A sweep that advanced floors would fence the writer sessions of every other server once a day, so a store could have only one maintaining server, a deployment rule that the format can't enforce. The [Maintenance roles](maintenance-roles.md) note rejects a fold under the writer epoch for the same cost.

## The design

This section describes the floor object, how every process creates one, the two floors that the design uses, how the new floor reaches a manifest, what a writer does when it meets a floor object, and why the expired-deletion invariant stays true. The design adds no constant, no setting, and no object family.

### The floor object

A floor object is a WAL object with no commit records whose payload also sets `retention_floor_seq` to the new floor. Like a fence, a floor object is created at the next WAL number and doesn't advance `head_seq`. It adds no commit, no sequence number, and no change feed entry.

A floor object's writer epoch is the epoch of the manifest that the creating attempt's [discovery](../specs/format.md#26-a-summary-of-the-log-protocol) loaded before the put (discovery loads the current manifest and the WAL objects after it). The creating process claims nothing. Each attempt copies the epoch again from its own discovery, so a floor object is never written above a fence with a higher epoch, and the writer epochs of WAL objects never decrease as the WAL number increases.

For example, if the current manifest is manifest 7, with writer epoch 3, and the [WAL tip](../specs/glossary.md#wal-tip) is WAL object 30 at sequence 140, a process that advances the floor to 20 creates WAL object 31 with writer epoch 3, `head_seq` 140, no records, and `retention_floor_seq` 20.

Replay accepts a floor object only when its `head_seq` equals the head that replay reached just before it, as it does for a fence.

Why does a WAL object order the advance when a manifest can't? Commits are ordered by WAL number, and a writer can't create a WAL object above a number without first reading the WAL object at that number, or a manifest that folded that WAL object. A writer can validate a restore against an older manifest and then commit the restore after another process has published a manifest that moves the floor. But the writer can't skip a floor object. In this design, nothing else orders the advance against commits (no publication queue, writer epoch, or clock), so the floor object can come from any process, and the order is the same.

Why may a process without the writer epoch create a WAL object? The writer epoch exists to fence commits from an older writer. A floor object contains no commits, and it can't branch the history, because put-if-absent at the next WAL number orders the floor object like any other WAL object.

### Creating a floor object

Every floor object is created with the same steps. To advance the floor to `T`, a process:

1. Plans the new floor with the rules in [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor): the three targets, the cutoff walk over the `commits` family, and the rule that the floor never passes a commit whose `committed_at_ms` is later than the advancing host's clock minus the [grace window](../specs/glossary.md#grace-window) (24 hours). The process plans from its runtime's writer position, after a check of the position like a strong read's, or from the current manifest when the runtime has no position. If the new floor isn't above the floor that the process planned from, the process writes nothing.
2. Checks that the segments of the manifest that the process planned from exist.
3. Runs discovery to find the WAL tip. If the discovered view's floor, which includes the floor objects in the WAL tail, is already at or above `T`, the process writes nothing. Otherwise, it creates the floor object at `tip + 1` with put-if-absent, with the writer epoch of the manifest that this discovery loaded.
4. After the put succeeds, checks the discovery's manifest for a newer one, and compares the floor object's number with the newest manifest's `folded_wal_no`. If the number is at or below it, the floor object is dead, because the put may have used a number that a collection emptied (see [The check after a fence or floor object put](kept-fences-and-floors.md#the-check-after-a-fence-or-floor-object-put)), and the process starts the next attempt.
5. If the put collides, runs discovery again, copies the writer epoch from the manifest that the new discovery loaded, and tries the next WAL number. The process stops after 1,024 attempts or when `METADATA_PUBLICATION_BUDGET_MS` (15 minutes) runs out, and returns `store_contention`.
6. If the process has a writer session for the namespace, drops that session's [writer position](../specs/glossary.md#writer-position), so the session's next publication and the fold in the next step read the floor object from the store.
7. If no manifest records the floor of the view after the put, folds the namespace, so the fold's manifest records the new floor (see [Publishing the floor in a manifest](#publishing-the-floor-in-a-manifest)).

If the process can't tell whether the put succeeded, it reports the outcome as unknown. If the caller then retries, the retry's discovery finds the floor object when it was written, and writes nothing. A second floor object with the same floor would do no harm.

A writer session never writes a floor object from its publication queue. A runtime that has the namespace's writer session uses the steps above, and its session learns of the floor object as a session in another process does: its next put at the floor object's number collides and it plans again, or, after step 6, its next publication runs discovery and finds the floor object. The same steps serve every caller because a session's put at `tip + 1` and the advance's put at `tip + 1` are ordered by put-if-absent whichever process sends them, so nothing about the order depends on where the session is.

For example, suppose a runtime has the writer session of a namespace with epoch 3, tip 30, head 140, and a position on manifest 7 (`folded_wal_no` 25, floor 5). The sweep advances the floor to 20. Its discovery finds tip 30 and floor 5, it puts WAL object 31 (epoch 3, head 140, floor 20), and its check finds no manifest 8, so the floor object is live. The runtime drops the session's position. If the session had a batch planned at 31 when the put arrived, that batch's put collides, and the session runs discovery, replays WAL object 31, plans the batch again against floor 20, and commits it at 32. The advance's fold then publishes manifest 8, with `folded_wal_no` 31 or 32 and floor 20.

### Two floors

A floor object limits the writer from the moment it's written, and readers see the new floor from the manifest that a fold publishes. So the design uses two floors:

- The "validation floor" is the manifest's `retention_floor_seq` raised by the `retention_floor_seq` of every floor object in the unfolded WAL tail. Replay computes it. Every floor that the writer reads while it plans and validates a commit is this one: the restore check (of a deleted item and of a file revision), the merge floor rule, and the trash lookup and the ancestry and rights walks that the commit's plan runs. So a restore of a deletion between the two floors fails exactly as a restore of an expired deletion does. An advance's test that the floor already reaches its target uses it too. For example, if a writer position's manifest has floor 5 and its tail contains floor object 31 with floor 20, the position's validation floor is 20.
- A reader's floor is the `retention_floor_seq` of the manifest that its view is on. In the same example, a read from that position takes floor 5 until the position moves onto a manifest that records floor 20.

| Rule | Floor it reads |
| --- | --- |
| Restore validation, for a deleted item and for a file revision | The writer position's validation floor |
| The trash lookup, ancestry walk, and rights walk in a commit's plan | The writer position's validation floor |
| The merge floor rule (a fork point at or above the namespace's floor) | The writer position's validation floor |
| An advance's test, before its put, that the floor already reaches the target | The discovered view's validation floor |
| The trash listing's scan start | The reader's manifest's floor |
| An expired deletion in a read (the ancestry walk, and `inode_not_found` by inode ID) | The reader's manifest's floor |
| `below_retention_floor` for the change feed, a diff, a read at a sequence, and a later grep page | The reader's manifest's floor |
| A merge planner's floor checks, and a rebase fork's check of its fork point | The manifest's floor of the view that the planner reads |
| The grep index's pruning | The manifest's floor |
| `retention_floor_seq` in `GET /v0/namespaces/{ns}` | The reader's manifest's floor |
| `retention_floor_seq` in `GET /v0/maintenance/namespaces/{ns}/diagnostics` | The manifest's floor of the view that the diagnostics read |
| A base compaction | Its basis manifest's floor |
| A snapshot read | The snapshot manifest's floor, and for its rights check at the head, the head's manifest's floor |

Why is the manifest's floor safe for a read? A base compaction removes a row only when its basis manifest's floor allows the removal, and a reader on manifest `M` reads `M`'s segments, which no compaction over a later manifest changes. So a floor below the validation floor keeps rows visible that `M` still stores, and nothing else: a deletion stays in the trash listing, its inodes stay readable by inode ID, and the change feed returns commits whose rows are still there. A read hides a row only when a base compaction over the reader's manifest may remove it.

Why does the writer need the validation floor? The invariant in [The invariant](#the-invariant) needs the writer to refuse a restore at the higher floor from the floor object's WAL number onward, before any manifest records the floor. The advance's test needs it too, or each advance would write another floor object until a fold.

Why don't readers use the validation floor too? With the validation floor, readers would learn of a new floor at three different points. A `strong` read in a process without a writer session would find the floor object at its next WAL probe. A read from a writer position in another process would keep the earlier floor until the session's next put collides or its successor probe finds the fold's manifest, because a position sends no WAL probe. A read from the position in the advancing runtime would see the floor once the position replays the floor object. With the manifest's floor, one rule covers every reader. A read shows the floor of its manifest, a new floor reaches every reader with the manifest that records it, which the successor probe finds, and a read from a writer position and a `strong` read on the same manifest report the same floor. So the format needs no rule for how each kind of reader learns of a floor object.

The reason for the rule is this consistency, not the guarantee in [No reader sees an expired deletion come back](#no-reader-sees-an-expired-deletion-come-back). That guarantee is true under either rule, because a view that replays a floor object has also replayed every WAL object below it, and the writer refuses every restore above it. The rule's cost is the time between a floor object's put and the fold, in which the trash lists a deletion that a restore refuses, as the next section describes.

#### What a user sees between the put and the fold

Between a floor object's put and the fold that records it, the trash listing shows a deletion that a restore refuses. For example, suppose deletion 10 is in the trash, and floor object 31 (floor 20) is written at 03:00:00 by the sweep. A trash listing in that time still lists deletion 10, because its manifest's floor is 5, and a read of a file under the deletion by inode ID still succeeds. A restore of deletion 10 is validated at floor 20 and fails as a restore of any expired deletion does (`not_deleted` for the service context, see [Expiring deletions and creating floor objects](../specs/format.md#expiring-deletions-and-creating-floor-objects)). Once the fold's manifest 8 records floor 20, the listing leaves the deletion out, and the read returns `inode_not_found`. In the same way, when a fork's fork point is between the two floors, `GET /merges` reports the fork's next range while `POST /merges` refuses the merge with `below_retention_floor`.

The advance folds right after its put, so the disagreement lasts one fold: a few store round trips when the tail contains only the floor object, plus the time to write segments when the tail has commits. (This is an estimate and hasn't been measured.)

If that fold fails, the advance returns the fold's error, and the floor object stays in the tail. The writer still refuses restores at floor 20, and readers still use floor 5, until the next fold of the namespace. That fold is one of these:

- a fold at the fold thresholds, which a maintaining process runs (the floor object counts toward the unfolded WAL object count),
- the idle fold, once the newest commit in the tail is 15 minutes old (the reference server's default), or
- the next advance of the namespace (by default the next sweep, a day later). Its discovery finds the floor object, and it folds because no manifest records the validation floor. It writes another floor object first only when its own plan passes floor 20.

The idle fold rule never folds a tail that contains no commits, so after a failed fold in a namespace that nobody writes, the disagreement lasts until the next sweep.

A server whose maintenance mode is `serve_only` or `disabled` runs no sweep, so it doesn't advance floors on a schedule. The maintaining process's sweep advances the namespaces that such a server writes, and runs the fold right after the put. So the disagreement lasts longer than one fold only when that fold fails, and then it lasts until the maintaining process's next fold of the namespace. A `retention` job sent to a `serve_only` server runs the same steps, its fold included.

#### What the `retention` job reports

The `retention` job, and an embedded `Maintenance::advance_retention_floor` call, report the validation floor that the advance reached. They return that floor only after a manifest records it, because the job folds whenever the floor it reached is above the floor of the manifest below its view, and returns the fold's error if the fold fails. So a read of the namespace that starts after the job returns, and that observes the newest manifest (a `strong` read, or any read in the runtime that ran the fold, whose cached view the fold's manifest replaces), reports that floor or a higher one. When the plan leaves the floor where it is, the job reports the floor that it planned from (a manifest's floor or a writer position's floor), and it folds in the same way when that floor is above the floor of the position's manifest. A read in another process with `eventual` consistency can still report an older floor within its staleness bound, as it can report an older head.

### Publishing the floor in a manifest

When a fold covers a floor object, the manifest that the fold publishes has a `retention_floor_seq` equal to the higher of the previous manifest's floor and the floors of the floor objects that it covers. LoonFS publishes no separate manifest for the floor. A compaction reads the floor from its basis manifest, so a base compaction removes rows only at or below a floor that a manifest has published.

Why does a fold publish the floor, instead of a separate manifest from the advancing process? The fold already publishes a manifest that covers the floor object. The invariant in [The invariant](#the-invariant) requires a manifest to record a floor only after every WAL object below the floor object is folded, and a fold that covers the floor object has folded each of those WAL objects.

### What the writer does

A writer handles a collision with a floor object as it handles any collision. A [writer session](../specs/glossary.md#writer-session) whose put collides with a floor object at `tip + 1` creates no object, drops its writer position, and runs discovery. Discovery finds no higher writer epoch, so the session isn't fenced. Discovery replays the floor object into the session's position, and the session plans its batch again against the new validation floor. The session's callers see no error from the collision.

A collision costs the session one failed put and one discovery: the successor probe and the boundary read, one GET for each unfolded WAL object, and one GET that finds the next number empty. Then the session puts the batch again. Right after an advance, whose fold leaves a short tail, that's about five requests. (This is an estimate and hasn't been measured.) A session pays at most one collision per advance, because its discovery replays the floor object. A session in the advancing runtime that had no batch planned at the floor object's number pays no collision, only the discovery that its dropped position causes.

A restore that the session admitted before the advance can fail. If the restore's put loses `tip + 1` to the floor object, the session plans the restore again against the new floor and refuses it. If the restore's put wins, the restore is committed, the floor object is written at the next number, and the fold that records the new floor also folds the restore. Both outcomes keep the invariant, and the caller can't tell in advance which one it gets.

Why doesn't the floor object fence the writer? A fence stops a writer because the writer's discovery finds a higher writer epoch in the manifest. A floor object doesn't change the writer epoch.

A floor object is a WAL object that a process creates without a claim manifest, so two more rules apply.

The first rule is that a session never keeps a writer position whose manifest folds past the position's tip (that is, a manifest whose `folded_wal_no` is above the tip). When a session's successor probe finds a manifest that has the session's writer epoch and folds past the position's tip, the session drops its position and runs discovery, instead of moving the position onto that manifest. A manifest folds past the position's tip only when its fold covered a floor object that the session hasn't read. For example, a session has tip 30. Another process creates a floor object at 31 and publishes manifest 8, whose fold covers 31. The session's probe finds manifest 8, whose `folded_wal_no` is 31, so the session drops its position, and its discovery starts from manifest 8 with tip 31. Without the rule, the position would move onto manifest 8 with tip 30, below the manifest's `folded_wal_no`, which the position's WAL tail and its count of unfolded WAL objects assume never happens (see [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal)).

The second rule is that no process deletes a floor object (see [Keeping fences and floor objects](kept-fences-and-floors.md)). A session's put at `tip + 1` then collides with any floor object at that number, however long the session was idle, and the session runs discovery. For example, a session idle at tip 30 for three days puts its next batch at 31 after floor object 31 was folded and the folded WAL objects below it were collected. The put collides with the floor object, which stays, so the session can't commit at a number that a manifest has already folded. The argument is in [Why the put can't reuse an emptied number](kept-fences-and-floors.md#why-the-put-cant-reuse-an-emptied-number).

### The invariant

The design keeps the invariant stated in [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor). For every manifest `M` and every deletion `D` whose deletion sequence is at or below `M`'s retention floor and whose latest event in `M`'s rows is `set`, no WAL object above `M`'s `folded_wal_no` contains an accepted restore of `D`, and no WAL object written later will.

`M`'s floor comes from a floor object at some WAL number `k` at or below `M`'s `folded_wal_no`, or from an earlier manifest, for which the same argument applies. Suppose that a WAL object `W` contains an accepted restore `R` of `D`.

- If `W` is below `k`, the fold that published `M`, or an earlier fold, folded `W`. So `D`'s latest event in `M` is the restore's `revoke`, not `set`.
- If `W` is above `k`, the writer that created `W` created it at the writer's tip plus one, so the writer's tip was at or above `k`. A writer's tip passes a WAL number in one of two ways: the writer created the object at that number itself, or the writer ran discovery, which replays every WAL object above the manifest's `folded_wal_no`. No writer creates a floor object, and a position doesn't move onto a manifest that folds past the position's tip, so a manifest from another process never raises the tip. So before the writer created `W`, the writer ran a discovery that either replayed the floor object at `k`, or started from a manifest that had already folded `k` and so records a floor at or above the floor object's. In both cases, the writer validated `R` against a validation floor at or above `D`'s deletion sequence, so the writer refused `R`, and `W` doesn't contain `R`.

The second case applies to a writer of any epoch, so the argument needs no separate case for older or newer writers. Every later manifest copies or raises `M`'s floor and folds at least as much of the WAL, so the invariant is true for every later manifest. The same argument covers a restore of a file revision by reference.

### No reader sees an expired deletion come back

A reader's floor is its manifest's floor, and readers move from manifest to manifest along one chain, in which each manifest is built on the one before it and no floor goes down. The first manifest that records a floor is a fold that covers the floor object, and that fold is built on every manifest before it, so it includes every restore committed below the floor object. So a reader sees a deletion `D` expire only on a manifest whose floor passes `D` while `D`'s latest event is `set`, and the invariant says that no later manifest restores `D`.

For example, a reader is on manifest 7, with floor 5 and a tail through WAL object 30, and deletion 10 is in the trash. Floor object 31 (floor 20) is written. The reader keeps manifest 7's floor of 5, so its trash still lists deletion 10. If a restore of deletion 10 was committed at 30 or below, the fold that publishes manifest 8 with floor 20 folds the restore's `revoke`, so no reader ever sees deletion 10 expired. If not, manifest 8 records floor 20 with deletion 10's latest event `set`, the writer refuses every restore of it after WAL object 31, and the deletion stays expired in every later manifest.

### Examples

In the first example, the advancing runtime has the namespace's writer session. Suppose the floor is 5, the session has written WAL objects through 30, and its queue contains a restore of a deletion at sequence 10. If the restore's put at 31 comes first, the restore is committed at 31, the advance's put at 31 collides, and its next discovery finds tip 31 and puts the floor object at 32. The fold publishes floor 20, and the deletion's latest event in that manifest is `revoke`, so a base compaction keeps the restored item's rows. If the advance's put at 31 comes first, the restore's put collides, the session replays the floor object, and the restore fails because the deletion has expired.

In the second example, a server with no session for the namespace advances the floor. It discovers tip 30 and creates the floor object at 31 with floor 20. A writer session in another process then tries to commit a restore of the deletion at sequence 10 at WAL number 31. The session's put collides, so the session runs discovery, replays the floor object, and validates the restore against floor 20. The restore fails, and the session keeps its writer epoch.

In the third example, a writer commits without a pause while the advance tries to write its floor object. The writer's commits take 31, 32, and 33 while the advance's attempts collide at each number. On its fourth attempt, the advance's discovery finds tip 33, and its put at 34 arrives before the writer's next put, so the floor object is at 34 and the writer's batch collides and goes to 35.

### What a hosted service gets

A maintaining process advances the floor of every namespace in the store with the same steps, and fences no writer:

| Where the namespace's writer is | Who advances the floor | What the writer pays |
| --- | --- | --- |
| A session in the maintaining process (`serve_and_maintain`) | The process's sweep | One discovery, and one failed put if a batch was planned at the floor object's number |
| A session in another server, in any maintenance mode | The maintaining process's sweep | One failed put and one discovery at its next commit |
| An embedded writer session in an application | The maintaining process's sweep | The same |
| No session | The maintaining process's sweep | Nothing |

There's no setting for any of this, no delay before a maintaining process takes over a namespace, and no rule about how many processes maintain a store. If two processes advance one namespace's floor, each can create a floor object, and the higher floor applies.

### How the design follows the floor rules

The design follows the five rules for a namespace's writer and its background work, which the [Maintenance roles](maintenance-roles.md) note states ahead of its roles table.

1. A namespace has one writer, and only it adds commits. A floor object adds no commit, and a restore or a merge that the floor limits is a write in the writer's queue, validated against the validation floor.
2. The writer releases its epoch before every write that it sends under the epoch. Every WAL put that a session sends is a commit's put, which releases the epoch, because no session sends a floor object.
3. Background work never claims the epoch. An advance limits commits, so it puts an object with no commits at the next WAL number, as a fence does, and it copies the epoch from its discovery. The roles table in [Maintenance roles](maintenance-roles.md#two-roles) assigns the advance to the maintainer.
4. A reader sees its manifest plus the commits after it. A floor object limits the writer from the moment it's written, and readers see its floor when a fold records it in a manifest.
5. Deleting stored objects is ordered by age and by what manifests and snapshots still list, never by an epoch or a WAL number. A collection recognizes floor objects by their size and keeps them, and it deletes data objects under the age rules.

## Costs

This section lists what the design costs.

- An advance contends with a busy writer for the next WAL number. From the discovery's read that finds `tip + 1` empty to the put's arrival is about three round trips, and a writer that commits without a pause sends its next put about one round trip after its last put returns, so the writer takes `tip + 1` inside that time on most attempts. The estimate is that an advance against such a writer needs tens of attempts, each with another discovery of the tail, and can reach 1,024 attempts or the 15-minute budget and return `store_contention`. A writer that commits once a second leaves `tip + 1` free most of the time, so the advance needs one or two attempts. None of this has been measured.
- A writer session pays one collision per advance, about five requests right after the advance's fold, and its callers see no error. A session in the advancing runtime also loses its position, and its next publication runs discovery.
- A restore admitted before an advance can fail, because the race at `tip + 1` decides its order, not the session's queue.
- The design needs every rule for a WAL object written without the epoch: the epoch copied from discovery, the check after each floor object put (a HEAD request and a read of the manifest boundary), kept floor objects (one for each advance that moves the floor, a few hundred bytes each), and the rule that a position drops at a manifest that folds past its tip.
- Between a floor object's put and the fold that records it, the trash lists a deletion that a restore refuses, and `GET /merges` can report a range that `POST /merges` refuses. That lasts one fold, and until the next fold of the namespace when that fold fails (a day later by default, for a namespace that nobody writes).
- A view has two floors: the manifest's floor, which reads use, and the validation floor, which the writer and the advance's test use.
- When the tail contains nothing but the floor object, the fold after the advance publishes one more manifest, because the idle fold rule never folds a tail without commits. When the tail has commits, that fold is one that the namespace would need anyway.
- A floor object counts toward the unfolded WAL object thresholds, as a fence does.
- LoonFS creates no floor object in a deleted namespace. A floor object that is written between a namespace deletion's fold and the namespace tombstone stays unfolded and has no effect, and like every floor object, it stays in the store.

## Code and tests that the design removes

The session's own way of writing a floor object goes, with everything that exists only for it:

- `NamespaceCommitEngine::publish_floor_object` and `PositionState::publish_floor_object_against`, which put a floor object at the position's tip plus one under the session's epoch, kept the epoch through the put, and fell back to the general steps when the session had no epoch, was fenced, or found the number taken. `NamespaceStore::publish_floor_object_at_tip`, which exists for that fallback, goes back into `NamespaceStore::publish_floor_object`.
- The queue item `WorkItem::RetentionAdvance` and `PendingRetentionAdvance`, the joining of waiting advances in `NamespacePublisher::submit_retention_advance`, the worker's arm for the item, the delete barrier's drain of queued advances, `NamespacePublisher::publish_floor_object`, and `PublisherRegistry::submit_retention_advance`.
- The branch in `Maintenance::publish_retention_floor` between a session and no session. Every advance calls `NamespaceStore::publish_floor_object`, then `PublisherRegistry::drop_session_position`.
- The tests of the session's path: `a_sessions_floor_object_orders_its_restores_around_the_floor`, `a_sessions_floor_object_has_no_check_after_its_put`, `an_advance_orders_the_restores_queued_around_it`, `a_restore_at_the_floor_objects_number_is_folded_before_the_floor`, and `an_unknown_floor_object_put_leaves_the_session_acquired` are deleted. `a_retention_advance_from_a_live_session_checks_the_position_and_folds_on_its_basis` and `an_advance_moves_the_sessions_position_onto_its_manifest` are rewritten for an advance that drops the position.

`NamespaceStore::publish_floor_object`, the shared loop for fence and floor object puts, the check after the put, `FloorObjectReport`, the position check before the plan, and the fold after the advance stay. A view's floor splits into the manifest's floor for reads and the validation floor that the commit engine and the advance's test read.

## Changes to the specs and glossary

This section lists the sections of the storage format, the API spec, the design notes, and the glossary that the design changes.

The storage format changes in these sections:

- [File and subtree deletion](../specs/format.md#16-file-and-subtree-deletion), [Visible metadata](../specs/format.md#43-visible-metadata), and [Paths, listings, and revision reads](../specs/format.md#44-paths-listings-and-revision-reads) say that a deletion has expired in a view when its deletion sequence is at or below the view's manifest's floor, that the trash listing starts above that floor, and that restore validation uses the writer position's validation floor.
- [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal) says that replay computes the validation floor and that a reader's floor is its manifest's. Replay's check of a floor object's head doesn't change. In "Warm reads from a local writer position", the position is no longer updated "on local floor objects", and the parenthetical about an external floor object says instead that a floor object changes no read until a fold's manifest, which the successor probe finds, records its floor.
- [Writer ownership](../specs/format.md#61-writer-ownership) says that the check after the put applies to every floor object, in place of "a floor object that a process without the writer epoch creates".
- [Publication and group commit](../specs/format.md#63-publication-and-group-commit) says "fences and floor objects" in place of "fences and non-writer floor objects", because no writer creates a floor object.
- [Change feed](../specs/format.md#67-change-feed) says that a cursor below the reader's manifest's floor re-bootstraps.
- [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor) says that a floor object raises the validation floor and that readers' floor moves when a fold records it, defines the two floors in place of the paragraph on a view's floor, and drops the sentence that one definition makes restore validation refuse every deletion that the trash hides. In its place, the validation floor is at or above every reader's floor, so a restore refuses every deletion that a trash listing leaves out, and a trash listing can list a deletion that a restore refuses until the fold. "Expiring deletions and creating floor objects" says that a deletion has expired in a view when its deletion sequence is at or below the view's manifest's floor, deletes the paragraph about a writer session that already owns the writer epoch, says that every floor object is created with the general steps, adds the step that drops the runtime's writer position, and says that the session in the advancing runtime learns of the floor object as a session in another process does. The invariant's proof drops the case of a writer that created the floor object itself, and the worked example replaces the queue order with the race at `tip + 1`.
- [Clock and operation assumptions](../specs/format.md#114-clock-and-operation-assumptions) says "a floor object put" in place of "non-writer floor put".

The API changes in these sections:

- [Retention jobs](../specs/api.md#retention-jobs) removes the sentences about a floor object queued in the server's writer session and about a job without a session. It says that every job writes the floor object the same way and folds, and that `retention_floor_seq` in the response is the floor that the advance reached, which the job returns only after a manifest records it. A job whose fold fails returns the error while its floor object already limits restores.
- [`GET /v0/namespaces/{ns}`](../specs/api.md#62-get-v0namespacesns) says that `retention_floor_seq` is the floor of the manifest that the read's view is on. In the "Open writer session" bullet, the sentence about a floor object written by another process says that a floor object reaches the session's reads, as it reaches every read, once a fold's manifest records its floor.
- [`GET /filesystem/trash`](../specs/api.md#66-get-filesystemtrash) and the restore operation in [`POST /commits`](../specs/api.md#68-post-commits) say that a restore of a listed deletion can fail between an advance's floor object and the fold that records it.
- [`GET /merges`](../specs/api.md#615-get-merges) says that its floor outcomes compare with the floor of the reader's manifest, so `POST /merges` can refuse a fork point that the response doesn't report until the fold.

The glossary's "Retention floor" entry defines the validation floor and says that reads use the manifest's floor. "Read view" says that its floor is its manifest's. "Writer position" says that the position's validation floor includes the floor objects in its tail. "Floor object" says that every floor object is created with the same steps and that readers see its floor when a fold records it. "Fence", "Writer epoch", "Maintainer", and "WAL" don't change.

In the design notes and guides:

- [Retention window](retention-window.md) changes "The trash and expired deletions" and "Who advances the floor" for the two floors, removes the sentences about a session that runs the advance as an item of its queue and its example, and says that the deletions an advance passes expire for readers when the fold's manifest records the floor. "Advancing the floor on a schedule" makes the same change, and "Reads of an expired subtree" compares a deletion with the floor of the reader's manifest.
- [Maintenance hosting](maintenance-hosting.md) says that the sweep's advance creates the floor object itself whether or not the host has a writer session, and drops the session's position.
- [Keeping fences and floor objects](kept-fences-and-floors.md) removes the sentence about a floor object that a writer session creates from its own queue, and says "a floor object put" in place of "a floor object put by a process without the writer epoch".
- [Read freshness](read-freshness.md) no longer moves the position onto a floor object that the session creates.
- [Forks and merges without another namespace's fold](forks-and-merges-from-the-tail.md#a-fork-folds-the-sources-tail-into-itself) says that a floor object in the source's tail raises the source's validation floor, in place of "the view's floor", in its reason why manifest 2 keeps manifest 1's floor.
- The self-hosting guide's [Background maintenance](../../crates/loonfs-server/docs/self-hosting.md#background-maintenance) removes the sentence about a session that creates the floor object as one item of its queue, and its request costs include the check after the put for every advance.
- [Maintenance roles](maintenance-roles.md) states the five rules in [How the design follows the floor rules](#how-the-design-follows-the-floor-rules) ahead of its table in "Two roles", and keeps the table as it is.

## Alternatives considered

- **The session's own path beside the general one.** A runtime that has the namespace's writer session puts the floor object from the session's queue, and other processes use the general steps. The queue orders the advance against the restores admitted around it, and the session saves one collision and the check after the put. But the general steps have to exist anyway for a namespace with no session in the advancing runtime, so the format has two ways to write the same object, and the session's put is the one WAL put of a session that doesn't release the epoch. No invariant needs the queue's order, because the WAL number orders the advance whichever process wins `tip + 1`.
- **Only the writer writes floor objects.** The writer session puts every floor object from its queue under its epoch, and a host advances its sessions' floors on a timer. A sweep advances a namespace whose floor falls behind after a delay, by opening a session and claiming the epoch. That removes the copied epoch and the check after the put, but it needs two timing settings, a rule for when a fold is due, an advance path in the `serve_only` and `disabled` schedulers, and a session opened by the sweep. Maintenance still fences a writer when a host is late by more than the delay, when a session opens while the sweep claims, for a `retention` job or an embedded CLI run against another server's writer, and for an embedded writer whose floor falls behind. Asking the process that has the writer session to make the advance is a form of this design that also needs a way to find and call that process.
- **The floor published in a manifest with no floor object.** The advance folds the tail and publishes the new floor in a manifest. Only the holder of the writer epoch can publish that manifest safely, because a restore validated against an older manifest isn't ordered against the new floor otherwise. So a process without the namespace's writer session claims the epoch and fences the session, and a store is limited to one maintaining server (see [The problem](#the-problem)).
- **Readers take the floor from the log.** Every view's floor is its manifest's floor raised by the floor objects in its tail, so the trash, restores, and reads agree from the floor object's put. But a reader then learns of a new floor at different points (a `strong` read at its WAL probe, a read from a writer position in another process at its next collision or at the fold), so the format needs a rule for each kind of reader.
- **Recording the floor that each WAL object was validated against.** Each WAL object records the floor of the manifest that its writer validated the WAL object against, and a base compaction prunes only below the highest recorded floor that a fold has folded. That costs two durable fields, a second floor in every pruning rule, and an acknowledgment step for a namespace that nobody writes to (see [Alternatives considered](retention-window.md#alternatives-considered) in the retention window note).
- **An `expire` event for each deletion.** The writer commits an `expire` event for each deletion that the floor passes. That costs one event for each deletion, a scan loop with its own failure handling, and no reclamation until a writer runs in the namespace. A floor object is one object for each advance.
- **A floor object that the fold ignores, with a separate manifest for the floor.** The advancing process creates the floor object and then publishes the floor in a manifest of its own. The fold already publishes a manifest that covers the floor object, so the second publication adds a write, and a failure between the two steps leaves the floor in the WAL and not in a manifest.
- **Keeping a floor object until a writer has written past the floor object.** The collector would have to tell floor objects from a writer's objects and know when each was passed. After another writer takes the namespace over, a floor object below the new writer's fence could be collected, and the old writer's put at that number would succeed. LoonFS keeps every floor object instead (see [Keeping fences and floor objects](kept-fences-and-floors.md)).
- **Checking for a manifest successor after the put and before the acknowledgment.** This sends the same number of requests, but a successful put of the next WAL object wouldn't be the commit.
