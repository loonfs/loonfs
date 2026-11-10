# Retention advance in the WAL

This note describes a retention advance that any process can make without the writer epoch. The advance is written to the WAL as a "floor object", a WAL object with no commits that records the new [retention floor](../specs/glossary.md#retention-floor). The floor object takes the next WAL number, so the WAL orders the advance against every restore, the namespace's writer session isn't fenced, and the fold that covers the floor object publishes the new floor in a manifest.

## The problem

This section describes how the floor is advanced under the writer epoch, and what that costs.

A retention advance is a manifest publication, and a restore is a commit in a WAL object, so nothing orders the two unless one process makes both. For that reason, only the holder of the namespace's [writer epoch](../specs/glossary.md#writer-epoch) may publish a manifest that raises the floor. To advance the floor, the holder acquires the writer epoch (a claim manifest and a [fence](../specs/glossary.md#fence)), folds its WAL tail, and publishes the new floor (see [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor)).

A process that has no writer session for the namespace takes the epoch for the advance. The namespace's [writer session](../specs/glossary.md#writer-session) in any other process is then fenced, so its next request returns `writer_fenced`, and the application pays a claim and a fence (about 200 ms) to continue with a new session. A sweep that advances floors would fence the writer sessions of every other server once per sweep, so a store may have one maintaining server. That's a deployment rule, and the format can't enforce it.

[Maintenance roles](maintenance-roles.md) rejects a fold under the writer epoch for the same cost, because the claim fences a session that might be idle rather than dead, and it lists the retention advance and namespace deletion as the two exceptions. An advance by a process without a session also sends four publications: a claim manifest, a fence, a fold, and the floor's manifest.

## The design

This section describes the floor object, how a process creates one, how a view's floor is computed, how the floor reaches a manifest, and what a writer does when its commit collides with a floor object. The design adds no constant and no object family.

### The floor object

A retention advance to a new floor `T` creates a floor object, which is a WAL object with no commit records whose payload also sets `retention_floor_seq` to `T`. Like a fence, a floor object takes the next WAL number and keeps `head_seq` unchanged, and it produces no commit, no sequence, and no change feed entry. Any process can create one, with no claim on the writer epoch or the compactor epoch. The floor object's `writer_epoch` is copied, without a claim, from the manifest that the put's discovery loaded, in the same way that a fold publishes under the current compactor epoch without claiming it. Each attempt copies the epoch again from its own discovery, so a floor object is never written above a fence with a higher epoch, and epochs don't decrease along the log.

For example, if the current manifest has writer epoch 3 and the [WAL tip](../specs/glossary.md#wal-tip) is WAL object 30 at sequence 140, an advance to floor 20 creates WAL object 31 with writer epoch 3, `head_seq` 140, no records, and `retention_floor_seq` 20.

Why does a WAL object order the advance when a manifest can't? Commits are ordered by WAL number, and a writer can't create a WAL object above a number without having read the object at that number, or a manifest that folded that object. A writer can validate a restore against an older manifest and commit the restore after a new manifest moved the floor, but it can't skip a floor object.

Why may a process without the writer epoch create a WAL object? The writer epoch exists to fence commits. A floor object contains no commits, and it can't branch the history, because put-if-absent at the next number orders it like any other WAL object.

### Creating a floor object

To advance the floor to `T`, a process:

1. Plans the new floor with the rules in [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor): the three targets, the cutoff walk over the `commits` family, and the rule that the floor never passes a commit whose `committed_at_ms` is later than the advancing host's clock minus the default grace window. If the new floor isn't above the floor of the view it planned from (the manifest and its tail), the process writes nothing.
2. Checks that the segments of the manifest it planned from exist.
3. Discovers the WAL tip and creates the floor object with put-if-absent at `tip + 1`, with the writer epoch of the manifest that this discovery loaded, under `WAL_PUBLISH_BUDGET_MS` from its tip observation, like any numbered put.
4. After a collision, discovers the tip again, copies the writer epoch from the manifest that the new discovery loaded, and tries the next number, up to a bounded number of attempts.

A process that has the namespace's writer session creates the floor object as one item of the session's publication queue, so the floor object never collides with the session's own commits. A put whose outcome is unknown is reported as unknown. If the caller retries, a second floor object with the same floor does no harm.

### The floor of a view

The retention floor of a [read view](../specs/glossary.md#read-view), or of a [writer position](../specs/glossary.md#writer-position), is the higher of the manifest's `retention_floor_seq` and the `retention_floor_seq` of every floor object in the view's unfolded WAL tail. Replay applies a floor object by raising the floor to the object's value when that value is higher. A floor object whose `retention_floor_seq` is above its own `head_seq` is invalid.

Every rule that reads the floor of a view reads this value: restore validation (an undelete, and a restore of a file revision), the trash listing's scan start, the change feed's lower bound, the merge floor rule, and the reads that return `rebootstrap_required`.

Why does the floor take effect in a view before a manifest records it? Restore validation has to use the new floor from the floor object's WAL number onward, and one definition of a view's floor keeps the trash listing, the change feed, and restore validation in agreement. So a deletion that the trash listing leaves out is one that a restore refuses.

### The fold publishes the floor

A fold that covers a floor object publishes a manifest whose `retention_floor_seq` is the higher of its predecessor's floor and the floor object's. No separate manifest publishes the floor. A compaction reads the floor from its basis manifest, so a base compaction removes rows only at or below a floor that a manifest has published. The reference server's sweep folds the namespace after it creates a floor object, so the manifest's floor follows the floor object by one fold.

Why does the fold publish the floor, instead of a manifest that the advancing process publishes? The fold already publishes a manifest that covers the floor object. The invariant below needs a manifest to record a floor only after every WAL object below the floor object is folded, and a fold that covers the floor object has folded each of those WAL objects.

### What the writer does

A writer handles a collision with a floor object as it handles any collision. A writer session whose put collides with a floor object at `tip + 1` creates no object, drops its position, and runs discovery. Discovery finds no higher writer epoch, so it replays the floor object into the session's position, and the session plans its batch again against the new floor. The session isn't fenced, and its callers see no error from the collision.

Why doesn't the floor object fence the writer? A fence stops a writer because the writer's discovery finds a higher writer epoch in the manifest. A floor object changes no epoch.

A writer session in another process doesn't learn about a floor object when the object is created, because the session checks its position with the manifest successor probe, not with a WAL probe (see [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal)). Until the session's next commit or tip confirmation finds the floor object, or the session's probe finds the manifest of the fold that covers the floor object, reads in that process use the earlier floor. A read under the earlier floor returns rows that still exist, because compaction removes nothing until a manifest records the new floor, and no restore can be committed under the earlier floor, because the session's next put collides with the floor object. The sweep's fold after the floor object keeps this interval to about the session's manifest probe interval. A reader without the namespace's writer session sees a floor object at its next WAL probe, as it sees a commit.

A floor object is a WAL object that a process creates without a claim manifest, so the writer follows two more rules.

The first rule is that a writer position never sits on a manifest whose `folded_wal_no` is above the position's tip. When a session's successor probe finds a manifest with the session's writer epoch that folds past the position's tip, the session drops its position and runs discovery, instead of moving the position onto that manifest. The rule applies to every move of a position, including a move onto a manifest that the session's own runtime publishes. Such a manifest is a fold by another process that covered a floor object the session hasn't read. For example, a session has tip 30, and another process creates a floor object at 31 and publishes manifest 7, whose fold covers 31. The session's probe finds manifest 7, whose `folded_wal_no` is 31, so the session drops its position, and its discovery starts from manifest 7 with tip 31.

The second rule is that a tip confirmation is a discovery of one WAL object. A confirmation observes the tip the way discovery does, WAL first and manifest after. The writer sends a HEAD request for the next WAL number, and after that answer arrives, it sends the HEAD request for the next manifest number. If an object exists at the next WAL number, or the manifest has a successor, the writer runs discovery. For example, a session with tip 30 that confirms its tip before a commit finds the floor object at 31, and it runs discovery before it plans the commit.

Why does a tip confirmation check the next WAL number? Without that check, a put at `tip + 1` could write a number that garbage collection has already emptied. [Publication and group commit](../specs/format.md#63-publication-and-group-commit) argues that it can't, because another writer creates a WAL object only after its claim manifest, so if the manifest probe found no claim, the other writer's WAL object was created after the probe. A floor object has no claim manifest, and it can be older than the session's last confirmation. For example, a session has tip 30 and has been idle. Another process created a floor object at 31 thirty minutes ago, and nothing has folded it. The session confirms its tip with the manifest HEAD request alone and finds no successor. A minute later, a fold covers 31, and a collection deletes WAL object 31, which is folded and older than the grace window. The session then commits at 31 inside its publication budget, with no further probe. The put succeeds below the manifest's `folded_wal_no`, so no reader ever replays it, and an acknowledged commit is lost. With the second rule, the confirmation finds the object at 31, and the session runs discovery.

Why does the confirmation wait for the WAL answer before it checks the manifest? Two requests sent together have no order between their answers, and that loses a commit in this sequence:

1. The writer remembers tip 30. Floor object 31 exists and is older than the grace window.
2. The manifest HEAD request returns that the basis has no successor.
3. Another process folds object 31 into a new manifest, and a collection deletes object 31.
4. The WAL HEAD request returns that nothing exists at 31.
5. Both checks passed, so the writer creates a commit at 31, below the new manifest's `folded_wal_no`, and no reader replays the commit.

The first rule can't help, because the writer never saw the new manifest. With the order, if the WAL answer finds nothing at `tip + 1`, then either no object was created at that number, or an object was created there, folded, and collected. A collection deletes a WAL object only after a manifest folds the object, so in the second case that manifest was published before the collection, and so before the WAL answer. The manifest check starts after the WAL answer, so it finds that manifest, and the first rule then makes the session drop its position. In the first case, any object that is created at `tip + 1` after the WAL answer is too young for a collection to delete before the put runs. The reverse order (manifest first, WAL second) is the sequence above again.

Why does a position drop instead of moving onto a manifest that folds past its tip? A manifest that another process publishes with the session's writer epoch folds only WAL objects that the session wrote, unless the folded tail contained another process's floor object. In the first case, the position can move onto that manifest. In the second case, moving onto the manifest would leave the position's tip below the manifest's `folded_wal_no`. For example, a session has tip 30. Another process creates a floor object at 31 and folds it. If the session's probe moved the position onto the fold's manifest while the position's tip stayed at 30, a collection could delete the floor object once the object is older than the grace window. The session's next confirmation would then find no manifest successor and no object at 31, and its put at 31 would succeed below `folded_wal_no`. With the first rule, the session drops its position when the probe finds the manifest, and discovery starts from a tip at or above 31.

With both rules, the argument that a put at `tip + 1` can't write an emptied number no longer depends on a claim manifest. When a session plans a put at `tip + 1`, its tip observation is at most `WAL_PUBLISH_BUDGET_MS` old, and at that observation no object existed at `tip + 1` and no manifest folded past the tip. Discovery replayed the tail through its highest object, or the session created the object at the tip itself, or a confirmation found no object at `tip + 1` and then no successor manifest. In the last case, an object that was created at `tip + 1` and collected before the WAL answer was folded by a manifest that the later manifest check would have found, so the confirmation wouldn't have passed. So any object now at `tip + 1` was created after the observation. Garbage collection can't delete an object that young before the put runs (see [Clock and operation assumptions](../specs/format.md#114-clock-and-operation-assumptions)), so the put collides with the object. The position never sits on a manifest that folds past its tip, so `tip + 1` is never at or below a manifest's `folded_wal_no`.

### The invariant

The design keeps the invariant that [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor) states. For every manifest `M` and every deletion `D` whose deletion sequence is at or below `M`'s retention floor and whose latest event in `M`'s rows is `set`, no WAL object above `M`'s `folded_wal_no` contains an accepted restore of `D`, and no WAL object written later will.

`M`'s floor comes from a floor object at some WAL number `k` at or below `M`'s `folded_wal_no`, or from an earlier manifest, for which the same argument applies. Suppose that a WAL object `W` contains an accepted restore `R` of `D`.

- If `W` is below `k`, the fold that published `M`, or an earlier fold, folded `W`. So `D`'s latest event in `M` is the restore's `revoke`, not `set`.
- If `W` is above `k`, the writer that created `W` created it at its tip plus one, so the writer's tip was at or above `k`. A writer learns that the tip passed a WAL number in one of two ways: it created the object at that number itself, or it ran discovery, which replays every WAL object above the manifest's `folded_wal_no`. A position doesn't move onto a manifest that folds past its tip (see [What the writer does](#what-the-writer-does)), so a manifest from another process never raises the tip. The writer didn't create the object at `k`. So before it created `W`, it ran a discovery that either replayed the floor object at `k`, or started from a manifest that had already folded `k` and so records a floor at or above the floor object's. In both cases, the writer validated `R` against a floor at or above `D`'s deletion sequence, so the writer refused `R`, and `W` doesn't contain `R`.

The second case holds for a writer of any epoch, so the argument needs no case for older or newer writers. Every later manifest copies or raises `M`'s floor and folds at least as much of the WAL, so the invariant holds for every later manifest. The same argument covers a restore of a file revision by reference.

### Examples

In the first example, the advance is in the session's own queue. Suppose the floor is 5, a writer session has written WAL objects through 30, and its queue contains an undelete of a deletion at sequence 10, then an advance to 20, then a second undelete of the same deletion. The session commits the first undelete in WAL object 31 and creates the floor object at 32. It validates the second undelete against floor 20, so the second undelete returns `not_deleted`. The next fold publishes floor 20, and the deletion's latest event in that manifest is `revoke`, so a base compaction keeps the restored item's rows.

In the second example, another process advances the floor. A server with no session for the namespace discovers tip 30 and creates the floor object at 31 with floor 20. A writer session in another process then tries to commit an undelete of the deletion at sequence 10 at WAL number 31. Its put collides, so the session runs discovery, replays the floor object, and validates the undelete against floor 20, and the undelete returns `not_deleted`. The session keeps its epoch.

In the third example, the same race goes the other way, and the writer's undelete is written at 31 first. The advancing process's put at 31 collides, so the process discovers tip 31 and creates the floor object at 32. A fold folds 31 before 32, so the manifest that records floor 20 also has the undelete's `revoke` event.

## Costs

This section lists what the design costs, compared with the advance under the writer epoch.

- An advance is one WAL put, where an advance by a process without a session is a claim manifest, a fence, a fold, and a floor manifest. When the namespace's tail has commits, the sweep's fold after the floor object is the maintenance fold that the namespace would get anyway. When the tail contains nothing but the floor object, the fold is one more manifest publication, because the idle fold rule never folds a tail without commits.
- A writer session in another process pays one refused put and one discovery for each floor object that another process creates, and its callers see no error. The session doesn't pay a claim and a fence, and no request returns `writer_fenced` because of a sweep.
- A store can have more than one maintaining server. If two servers advance the same namespace, each one creates a floor object, and the higher floor applies.
- The WAL has a second kind of object with no commit records, so a collision at `tip + 1` no longer means that another writer took the namespace over. The writer's handling of a collision doesn't change, but the format's explanation of a collision does (see [Publication and group commit](../specs/format.md#63-publication-and-group-commit)).
- A view's floor is computed from the manifest and the tail, instead of read from one manifest field.
- A writer session in another process learns of a new floor at its next commit or from the fold's manifest (see [What the writer does](#what-the-writer-does)).
- A tip confirmation is two round trips (the WAL HEAD request, then the manifest HEAD request), where the confirmation under the writer epoch is one. A confirmation runs on a commit after a quiet period longer than the publication budget.
- A session whose probe finds another process's fold over a floor object drops its position and runs discovery once.
- A floor object counts toward the unfolded WAL object thresholds, as a fence does.
- LoonFS creates no floor object in a deleted namespace. A floor object that is written between a deletion's fold and its tombstone stays unfolded, has no effect, and is collected under the WAL rule for a deleted namespace.

## Changes to the specs and glossary

This section lists the sections of the storage format, the API spec, the design notes, and the glossary that the design changes.

The storage format changes in these sections:

- [A summary of the log protocol](../specs/format.md#26-a-summary-of-the-log-protocol) says that a WAL object is a data object, a fence, or a floor object, and drops the clause that says a WAL object is stamped with the retention floor of the manifest that the session validated its commits against.
- [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal) says that replay raises the view's floor at a floor object. In the paragraph about reads from a writer position, a floor object created by another process reaches the position at the session's next discovery, and the sentence "A read from the position sends no WAL probe, because another writer has to publish a claim manifest before it can write a WAL object" gets a new reason: a floor object changes only the floor, and a read under the earlier floor is safe. The sentence about moving the position onto manifests with the position's writer epoch adds the first writer rule, so the session drops its position at a manifest that folds past the position's tip.
- [Paths, listings, and revision reads](../specs/format.md#44-paths-listings-and-revision-reads) and [File and subtree deletion](../specs/format.md#16-file-and-subtree-deletion) say that a deletion has expired in a view when its deletion sequence is at or below the view's floor.
- [Writer ownership](../specs/format.md#61-writer-ownership) removes the paragraph about a process that advances the floor without a writer session.
- [Validation](../specs/format.md#62-validation) says that the merge floor rule and restore validation read the position's floor.
- [Publication and group commit](../specs/format.md#63-publication-and-group-commit) says that a collision at the next WAL number can be a floor object, after which the writer runs discovery and plans again. The observations table and the confirmation attempt follow the second writer rule, so the attempt sends the HEAD request for the next WAL number and then the HEAD request for the next manifest number. The attempt's start, before the WAL HEAD request, stays the tip observation and the basis confirmation, and the timing rules on the manifest answer don't change. The paragraphs "Why check for a manifest successor at all?" and "Why doesn't the writer also check the next WAL number?" are rewritten around the argument at the end of [What the writer does](#what-the-writer-does).
- [Change feed](../specs/format.md#67-change-feed) says that the feed's lower bound is the view's floor.
- [Publishing a manifest](../specs/format.md#72-publishing-a-manifest) says that a fold sets `retention_floor_seq` to the higher of its predecessor's floor and the floors of the floor objects it covers.
- [Recovery material and maintenance policy](../specs/format.md#73-recovery-material-and-maintenance-policy) counts a floor object toward the write-stop object threshold.
- [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor) replaces the three steps, the holder rules, and the three-writer proof with the floor object, the view's floor, and the proof in [The invariant](#the-invariant). It keeps the targets, the cutoff walk, the default grace rule, the retention window, and the segment check.
- [Row retention during a base compaction](../specs/format.md#103-row-retention-during-a-base-compaction) doesn't change its rule, which reads the basis manifest's floor.
- [WAL records](../specs/format.md#a5-wal-records) adds the optional `retention_floor_seq` to the WAL payload, valid only with an empty record list and at most `head_seq`.
- [Appendix C](../specs/format.md#appendix-c-timing-and-size-reference) adds no constant.

The API changes in these sections:

- [Deleting, retaining, and reclaiming](../specs/api.md#deleting-retaining-and-reclaiming) and [Maintenance jobs](../specs/api.md#maintenance-jobs) say that a `retention` job and the sweep create a floor object, and remove the sentences about opening a writer session, about `writer_fenced` in another process, and about one maintaining server per store.
- [Writer topology and fencing](../specs/api.md#53-writer-topology-and-fencing) says that a sweep doesn't fence a writer.
- [Retention jobs](../specs/api.md#retention-jobs) says that the `retention` job's result reports the view's floor.
- [`GET /v0/namespaces/{ns}`](../specs/api.md#62-get-v0namespacesns) changes two sentences. In the paragraph about reads from the writer session, "Another writer can't commit until it publishes a claim manifest" says that a floor object from another process reaches the session's reads at the session's next discovery or at the fold's manifest. The sentence "Readers observe deletions, retention floor advances, and new manifests on this interval" says the same for a `strong` read in the session's process.

In the design notes and guides, [Who advances the floor](retention-window.md#who-advances-the-floor) and its alternatives change, and so do the description of the sweep in [Maintenance hosting](maintenance-hosting.md) and the self-hosting guide's [Background maintenance](../../crates/loonfs-server/docs/self-hosting.md#background-maintenance). In [Maintenance roles](maintenance-roles.md), the paragraph that lists the advance as an exception changes, and the roles table row for the retention advance becomes: role maintainer, the current writer epoch copied without a claim, and the numbered create as what fences a rival.

In the glossary, a new entry defines "floor object", and the "Retention floor", "Fence", "Writer epoch", and "Read view" entries describe the floor object and the view's floor. "WAL" says that a WAL object contains commits, a fence, or a floor object. "Maintainer" moves the retention advance from the writer to the maintainer. "Writer position" adds the first writer rule and the reason that a read from the position sends no WAL probe.

## Alternatives considered

- **The advance under the writer epoch.** The holder of the writer epoch fences, folds, and publishes the floor. The advance is ordered against every restore, but each advance by a process without a session takes the namespace over from its writer session, and a store can have one maintaining server.
- **Recording the floor that each WAL object was validated against.** Each WAL object records the floor of the manifest that its writer validated it against, and a base compaction prunes only below the highest recorded floor that a fold has folded. That costs two durable fields, a second floor in every pruning rule, and an acknowledgment step for a namespace that nobody writes to (see [Alternatives considered](retention-window.md#alternatives-considered) in the retention window note).
- **An `expire` event for each deletion.** The writer commits an `expire` event for each deletion that the floor passes. That costs one event for each deletion, a scan loop with its own failure handling, and no reclamation until a writer runs in the namespace. A floor object is one object for each advance, and any process can create one.
- **Asking the namespace's writer session to advance the floor.** The process that wants the advance sends the request to whichever process has the namespace's writer session. That needs a way to find and call that process, and a namespace with no open session still needs a session opened for the advance.
- **A floor object that the fold ignores, with a separate manifest for the floor.** The advancing process creates the floor object and then publishes the floor in a manifest of its own. The fold already publishes a manifest that covers the floor object, so the second publication adds a write, and a failure between the two steps leaves the floor in the WAL and not in a manifest.
- **Sending the two HEAD requests of a confirmation together.** A confirmation costs one round trip, but the two answers have no order, and the sequence in [What the writer does](#what-the-writer-does) loses a commit.
- **Keeping a floor object until a writer has written past it.** The writer would need no WAL check. But the collector would have to tell floor objects from a writer's objects, which needs a manifest field. After another writer takes the namespace over, a floor object below the new writer's fence could be collected while the fence is still young, and the old writer's put at that number would succeed.
- **Checking for a manifest successor after the put and before the acknowledgment.** This sends the same number of requests, but a successful put of the next WAL object would no longer be the commit.
