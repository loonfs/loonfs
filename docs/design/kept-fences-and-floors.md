# Keeping fences and floor objects

This note describes how a WAL writer puts its next WAL object with no limit on how old its view of the log is. The design has three rules. No process deletes a fence or a floor object, in any namespace, a retired one included. A writer puts at the next WAL number with no clock, and the put either creates the object or collides with the fence or floor object that took the number, however long the writer waited. And after a fence put, or a floor object put by a process without the writer epoch, succeeds, the process checks the put's number against the newest manifest's `folded_wal_no`, a number that only rises. The WAL publication budget and the tip confirmation are removed.

## The problem

This section describes the time rule that protects a WAL put, why the rule exists, and what it costs.

WAL objects are numbered in order, and a writer commits a batch by creating WAL object `tip + 1` with put-if-absent, where the "tip" is the highest WAL number that the writer has seen (see [A summary of the log protocol](../specs/format.md#26-a-summary-of-the-log-protocol)). Three kinds of object share these numbers. A data object contains commits. A "fence" is a WAL object with no commits that a new [writer session](../specs/glossary.md#writer-session) creates after its claim manifest, so that an older writer's next put collides with the fence. A "floor object" is a WAL object with no commits that records a new retention floor, and any process can create one (see [Retention advance in the WAL](retention-advance-in-the-wal.md)).

A namespace's [collection](../specs/glossary.md#collection) deletes every WAL object at or below the current manifest's `folded_wal_no` (every WAL object, in a deleted namespace) once the object is older than the [grace window](../specs/glossary.md#grace-window) (24 hours), whatever kind of object it is (see [Candidate and age rules](../specs/format.md#113-candidate-and-age-rules)). So a WAL number that was taken can be empty again, and a put at that number would succeed at or below `folded_wal_no`, where no reader replays the new object.

For example, the writer session of `main` sees tip 30 at 10:00 on Monday, and its process then pauses (a virtual machine is suspended) just before it puts its next batch at 31. At 03:00 on Tuesday the sweep creates floor object 31 and folds the namespace, so the current manifest's `folded_wal_no` is 31. At 03:00 on Wednesday a collection deletes WAL object 31, which is folded and 24 hours old. If the writer resumed at 09:00 on Wednesday and put its batch at 31, the put would succeed, the writer would acknowledge the batch, and no reader would ever replay the batch.

LoonFS prevents this with a clock. A writer starts each WAL put within `WAL_PUBLISH_BUDGET_MS` (5 minutes) of the "tip observation" that it planned against, which is the discovery, the earlier put, or the check that last confirmed its tip. After a quiet period longer than that, the writer runs a "tip confirmation": a HEAD request for the next WAL number, and after that response, a HEAD request for the next manifest number. A WAL put that returns after the budget is reported as `outcome_unknown`, even if it succeeded (see [Publication and group commit](../specs/format.md#63-publication-and-group-commit)).

That clock costs these pieces of the format:

- `WAL_PUBLISH_BUDGET_MS`, and its term in `PUBLICATION_COMPLETION_BOUND_MS`.
- The tip confirmation, its two round trips, and the argument for its order.
- The rule that a WAL put that returns after the budget has an unknown outcome.
- The age recorded with every tip observation, and a correctness rule on the monotonic timer.

Why isn't the put itself enough? Put-if-absent checks that the number is empty now, not that the number was never used. The budget makes sure that any object created after the writer's observation is too young to have been collected when the put is applied.

## The design

This section describes the objects that stay, the WAL put and why it's safe, the check after a fence or floor object put, how cleanup recognizes the objects that stay, and the time rules that remain.

### Fences and floor objects stay

No process deletes a fence or a floor object. A namespace's collection keeps them in an active namespace and in a deleted one, and the cleanup of a retired namespace keeps them too. A retired namespace's prefix keeps its [manifest boundary](../specs/glossary.md#manifest-boundary) object, its fences, and its floor objects, and nothing else (see [Retiring a namespace](manifest-boundary.md#retiring-a-namespace)).

A collection still deletes data objects under the existing age rules, and the keys don't change (see [Object keys](../specs/format.md#a8-object-keys)).

### The WAL put

To publish a batch, a writer:

1. Plans the batch against its [writer position](../specs/glossary.md#writer-position) and builds one WAL object at `tip + 1`.
2. Checks the content-admission evidence, and puts the object with put-if-absent, however old its tip observation is.
3. If the put succeeds, the batch is committed. The writer moves its position and acknowledges the requests, with no check of the elapsed time.
4. If the put collides, runs discovery, as after any collision. A higher writer epoch means that the writer is fenced. A floor object means that the writer plans the batch again against the new floor, at the next number.
5. If the put's outcome is unknown, drops its position, as in [Failed and unknown outcomes](../specs/format.md#64-failed-and-unknown-outcomes). The next publication runs discovery, which reads the number back.

The session's successor probe still runs every second by default, so the session learns of claims and folds between commits (see [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal)), but the put doesn't wait for the probe.

In the example in [The problem](#the-problem), the writer resumes on Wednesday and puts its batch at 31. The put collides with floor object 31, which is still there, so the writer runs discovery, plans the batch again against the new floor, and commits it at 32.

Why is there no check after a successful put? The put succeeds only at a number where no object was ever created (see [Why the put can't reuse an emptied number](#why-the-put-cant-reuse-an-emptied-number)). A fold covers only WAL objects that exist, so that number is above every manifest's `folded_wal_no`, and every reader replays the batch.

### Why the put can't reuse an emptied number

This section shows that a put by an acquired writer session at `tip + 1` succeeds only if no object was ever created at `tip + 1`.

A writer session `W` with writer epoch 5 has tip 30. When `W` observed tip 30 (by discovery, or by creating WAL object 30), nothing was at 31. A collection deletes only data objects, so if 31 is empty when the put is applied, either nothing was ever there, or a data object was created at 31 after the observation and later deleted. A writer learns that the tip passed a number in one of two ways: it created the object at that number itself, or it ran discovery, which replays every WAL object above the manifest's `folded_wal_no`. So the data object at 31 would come from one of these writers:

- **`W` itself.** A session publishes one WAL object at a time, and the batch that's waiting is the one that would write 31. After a put whose outcome is unknown, `W` drops its position and runs discovery.
- **A writer with an older epoch.** `W` is acquired, so its own fence (say at 25) is at or below 30. To commit at 31, the older writer's tip would have to pass 25. It can't create WAL object 25 itself, because `W`'s fence is there, so it would have run a discovery that replays the fence or loads a manifest with epoch 5, and it would be fenced.
- **A writer with a newer epoch.** That writer commits only above its own live fence (see [The check after a fence or floor object put](#the-check-after-a-fence-or-floor-object-put)). If its fence were at 30 or below, `W` would have passed it in the same way, by discovery (and been fenced) or by a put at that number (which would have collided). So the fence is at 31 or above, and that writer's first object at 31 or above is its fence, not a commit.

So the objects that can take 31 after `W`'s observation are a fence and a floor object, and no process deletes either one, so `W`'s put collides. The argument is true whenever the provider applies the put.

The same argument covers a deleted namespace. Deletion runs under an acquired writer epoch, so the deleting session wrote a fence. A stale writer's put collides, its discovery finds the tombstone (or the retired boundary), and it returns `namespace_deleted`.

A fence put, and a floor object put by a process without the writer epoch, aren't covered, because a writer may legally commit at the numbers that those puts use. [The check after a fence or floor object put](#the-check-after-a-fence-or-floor-object-put) covers them. A writer session that creates a floor object from its own publication queue puts it at its position's tip plus one, like a commit, and the argument above covers that put.

Why keep floor objects and not just fences? A process that isn't the writer creates a floor object at the writer's next number with no claim manifest first, so a writer can find a floor object at `tip + 1` that no manifest announced. Why keep them in a retired namespace? A namespace ID can't be reused, so a stale writer's put there has to collide too, or the writer would acknowledge a commit that no reader replays.

### The check after a fence or floor object put

A fence put, and a floor object put by a process without the writer epoch, are the two WAL puts that race with a writer that may legally commit at the same numbers. Between a claim and its fence, the previous writer can still commit (see [Writer ownership](../specs/format.md#61-writer-ownership)), and a process without the writer epoch creates a floor object while the writer commits. A collection deletes those commits once they're folded and older than the grace window, so either put could succeed at an emptied number. So after such a put succeeds, the process:

1. Checks the manifest that it has for a newer one, with the successor probe and the boundary read (see [Checking a manifest that a process already has](manifest-boundary.md#checking-a-manifest-that-a-process-already-has)).
2. Compares the put's WAL number with the newest manifest's `folded_wal_no`. If the number is above `folded_wal_no`, the fence or floor object is live.
3. Otherwise, treats the object as dead. The object stays, but nothing replays it. The process runs discovery again and puts again at the new tip.

For example, a new writer session for `main` claims epoch 6, discovers tip 30 on Monday, and pauses before its fence put. The previous writer, with epoch 5, commits 31 to 40, and a fold covers them. On Wednesday a collection deletes 31 to 40. The new writer's fence put at 31 succeeds, but its check finds `folded_wal_no` 40, so the writer discovers again and puts its fence at 41.

The claim manifest's publication budget stays as it is, but nothing in this design depends on that budget. A floor object that a paused process creates at an emptied number is dead, and the process puts the floor object again, so the advance isn't lost.

Why is the check enough? A fold covers only WAL objects that exist, so a number that a collection emptied was folded before the put, and the newest manifest at the check has a `folded_wal_no` at or above that number. After a passing check, the fence's number was never used. The previous writer writes consecutive numbers, so none of its objects is at or above the fence's number, and its next put collides with the fence.

### Recognizing fences and floor objects

A key doesn't show the kind of a WAL object. But the payload of a fence or a floor object has only the namespace ID, the WAL number, the writer epoch, the head sequence, an empty record list, and for a floor object, the floor. So its stored size is at or below a constant bound that the WAL encoding sets. A WAL object with commits is always larger than that bound, because the writer pads a commit object that would otherwise be at or below the bound, with padding that survives compression. The [WAL records](../specs/format.md#a5-wal-records) section of the format defines where the padding goes.

A collection then tells the kinds apart from the listing alone. A candidate at or below the bound is a fence or a floor object, and it stays. A candidate above the bound is a commit object, and the collection deletes it by key, under the age rules in [Candidate and age rules](../specs/format.md#113-candidate-and-age-rules).

For example, suppose the bound is 512 bytes, and `main`'s WAL objects 1 to 30 are folded and old enough. The listing shows floor object 12 and fence 25 at 300 bytes or less, and 28 commit objects above 512 bytes. The collection keeps 12 and 25 and deletes the other 28, with no GET.

Why pad commit objects instead of reading small candidates? A read would cost one GET for every small commit object deleted, and every collection would read every fence and floor object again, a cost that grows as long as the store exists.

### What stays

These parts of the format don't change:

- `METADATA_PUBLICATION_BUDGET_MS` and `METADATA_COMPACTION_BUDGET_MS`, which keep a publication's unreferenced outputs younger than the age rules, and stop a publication that runs too long with an error.
- `PUBLICATION_COMPLETION_BOUND_MS`, which becomes `METADATA_PUBLICATION_BUDGET_MS + PROVIDER_PUBLICATION_REQUEST_BOUND_MS + GC_SAFETY_MARGIN_MS`. The value stays 25 minutes, because the metadata budget was already the larger term.
- The rule that a writer drops a position whose manifest folds past the position's tip. That rule compares numbers, and the retention floor's invariant depends on it (see [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor)).
- The handling of an unknown outcome, the provider request bound, and the clock allowance.

### Time rules

No rule depends on how long ago any process observed the WAL tip. These time rules remain on the write path:

- The output-age budgets of a manifest publication and a compaction.
- The expiry of content-admission evidence.
- The lifetime of an operation snapshot and of the content proofs of the operation's plan (`OPERATION_SNAPSHOT_TTL_MS`).

## Costs

- Every fence and every floor object is stored as long as the store exists, and each is a few hundred bytes. A namespace gets one fence for each writer session that commits, and one floor object for each retention advance (a sweep makes at most one advance of each namespace, and sweeps run once a day by default). A namespace with one long-lived writer session keeps about 365 objects a year. A namespace written once an hour, by a server that closes a session after 30 idle minutes, starts 24 sessions a day and keeps about 9,100 objects a year, or about 3 MB.
- Every listing of a namespace's WAL returns its fences and floor objects. After a year, the second namespace above adds about 9 listing pages of 1,000 keys to every collection.
- How often writer sessions start sets the number of fences, so the server's idle-close setting is the one to change if the count matters.
- A commit object that would be at or below the bound is padded, by at most the bound.
- A fence put, or a floor object put by a process without the writer epoch, costs one check after the put: a HEAD request and a conditional read of the boundary.
- A collision with a floor object costs the writer one discovery and one new plan of the batch, as before.

A commit after a quiet period longer than five minutes no longer waits for a tip confirmation's two round trips.

## Changes to the specs and glossary

The storage format changes in these sections:

- [A summary of the log protocol](../specs/format.md#26-a-summary-of-the-log-protocol) says that garbage collection keeps fences and floor objects, and drops the time budget of every numbered put as the reason for the second guarantee.
- [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal) removes the tip confirmation from how a position learns of a floor object and from a new position's previous probe.
- [Writer ownership](../specs/format.md#61-writer-ownership) removes the budget from the fence put and adds the check after it.
- [Publication and group commit](../specs/format.md#63-publication-and-group-commit) removes the budget check, the observations table, the tip confirmation, and the "why" paragraphs about the confirmation, and adds the argument in [Why the put can't reuse an emptied number](#why-the-put-cant-reuse-an-emptied-number).
- [Commit identity and retries](../specs/format.md#65-commit-identity-and-retries) refreshes the receipt projection after a collision or a manifest successor, not after a budget.
- [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor) removes the budget and the tip confirmation from the floor object's put and the writer rules, and adds the check after a put by a process without the writer epoch.
- [Retirement](../specs/format.md#95-retirement), [One complete collection](../specs/format.md#111-one-complete-collection), [Candidate and age rules](../specs/format.md#113-candidate-and-age-rules), and [Retiring an eligible tombstone](../specs/format.md#118-retiring-an-eligible-tombstone) keep fences and floor objects, in a retired prefix too, and recognize them by their listed size.
- [Clock and operation assumptions](../specs/format.md#114-clock-and-operation-assumptions), [Publication during collection](../specs/format.md#115-publication-during-collection), and [Alternatives considered](../specs/format.md#13-alternatives-considered) replace the budget argument for WAL puts.
- [Required operations](../specs/format.md#31-required-operations) says that a listing returns each object's size.
- [WAL records](../specs/format.md#a5-wal-records) states the size bound and the padding of a commit object, and [Publication and collection timing](../specs/format.md#c1-publication-and-collection-timing) removes `WAL_PUBLISH_BUDGET_MS` and its term in the bound formulas.

[Failed and unknown outcomes](../specs/format.md#64-failed-and-unknown-outcomes), [Recovery material and maintenance policy](../specs/format.md#73-recovery-material-and-maintenance-policy), [Object keys](../specs/format.md#a8-object-keys), and the API spec don't change.

The design notes [Retention advance in the WAL](retention-advance-in-the-wal.md), [Retention window](retention-window.md#who-advances-the-floor), [Read freshness](read-freshness.md#what-the-format-and-api-specs-say), [The WAL implementation](wal-boundary.md#responsibilities-outside-the-wal), and [The manifest boundary](manifest-boundary.md) (its retirement, [time rules](manifest-boundary.md#time-rules), and [costs](manifest-boundary.md#costs)) change. In the glossary, "Fence" and "Floor object" say that no process deletes them, "Publication budget" covers manifest publications alone, and "Collection" and "Garbage collection" exclude fences and floor objects.

## Alternatives considered

- **The publication budget and the tip confirmation.** This works, but a correctness rule then depends on a clock and a timer, where an object that stays does the same job with no clock.
- **A manifest field that lists the numbers of fences and floor objects.** The collection would skip the listed numbers without reading them. Every fold would have to keep the field in step with the WAL, for information that the objects already contain.
- **A kind in the key, such as `.fence`.** A fence at a different key doesn't collide with a put at the WAL number, so it fences nothing.
- **Empty fences that a listing recognizes by size.** A fence contains the writer epoch and a floor object contains its floor, so neither can be empty.
- **Reading each candidate at or below the bound.** Each small commit object costs one GET before its deletion, and every collection reads every fence and floor object again, a cost that grows as long as the store exists.
- **Keeping every small WAL object without a read.** A namespace whose commits are all small would never reclaim its WAL.
- **Deleting fences and floor objects after a very long age.** An age longer than any writer runs is a clock rule again, with a larger number.
- **Making floor objects through the writer session when one exists.** That doesn't remove the fence case, and a namespace with no session still needs a floor object.
