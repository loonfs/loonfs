# Maintenance hosting

A writable session maintains its own namespace as it publishes. The host's scheduler also runs the due maintenance steps of the sessions it has open at a regular interval, and a daily sweep over a listing of the store for everything else. All scheduling information is temporary, and durable state determines what each maintenance call can do.

## Folding and compacting a namespace

This section describes how one runtime folds and compacts a namespace, whatever part of LoonFS asks it to.

A [fold](../specs/glossary.md#fold) writes the commits in a namespace's WAL tail into metadata segments and publishes a manifest that lists them. A compaction unit combines one window of metadata runs and publishes the result in one manifest, as [Metadata streaming compaction](metadata-streaming-compaction.md) describes. Many parts of LoonFS fold or compact a namespace:

- the namespace's writer session, which folds its own tail and then compacts,
- the host's scheduler, described in [Session steps and sweeps](#session-steps-and-sweeps),
- a `metadata` maintenance request and an explicit fold request,
- a merge or a rebase, which folds the fork first,
- a snapshot, or a fork created from the current head, which folds the tail first,
- a retention advance, which folds the tail after it creates a "floor object" (a WAL object with no commits that records the new floor, as the [glossary](../specs/glossary.md#floor-object) describes), so a manifest records the new floor, and
- a namespace deletion.

If two of them folded the same tail at once, both would write every content object in the tail, and one of the two manifest puts would fail. If two compaction units planned from the same manifest, one of them would do its work for nothing.

A writer session's [writer position](../specs/glossary.md#writer-position) is the session's in-memory record of the manifest that the session last published or found current, and of the unfolded WAL tail above that manifest. For example, after the session publishes manifest 5 and then commits WAL objects 41 to 43, the position records manifest 5 and a tail of three WAL objects. A session has no position before its first publish, and it drops its position after a fork of its namespace, another writer's claim, a change that the session can't follow, or a check that finds the position's manifest at or below the namespace's [manifest boundary](../specs/glossary.md#manifest-boundary). The position's age doesn't drop it. The session's next publish sets a new position.

So each runtime has one "fold path" and one "compaction path" for each namespace, and every part of LoonFS that folds or compacts the namespace goes through them. The fold path is the one place where the runtime folds the namespace, and it runs one fold at a time, so a fold that starts during another fold waits for that fold to finish. The compaction path does the same for compaction units.

Every fold first takes the namespace's "fold lock", an in-memory read-write lock. A fold takes the fold lock in exclusive mode, so one fold of the namespace has the lock at a time. (A snapshot takes the lock in shared mode, as described below.) LoonFS checks whether the fold is due (defined below) after the fold has the lock, so if a fold waited for another fold, the check finds the tail already folded and the second fold does nothing. With a position, LoonFS also checks the position before the fold takes the lock, and if no fold is due, the fold returns without a store request and without taking the lock.

Some requests need a folded manifest: a merge's fold of the fork, a snapshot, a fork, an explicit fold, and a retention advance. Any fold of the namespace in the same runtime serves such a request if the fold took its input after the request arrived, because that fold covers every commit that was acknowledged before the request. The request folds only when no fold like that has finished, so the requests that wait for one fold share the next fold. For example, if three snapshots arrive while a fold is running, the first of the three snapshots to get the fold lock folds the tail, and the other two use the manifest that the first snapshot's fold published.

A fold that's due then:

1. Waits for a fold permit from the runtime's execution budget.
2. Writes the tail into metadata segments and publishes a manifest that lists them. With a position, the fold takes the tail from the position without reading the WAL. Without one, it reads the current manifest and the tail from the store.
3. Moves the session's position onto that manifest.
4. Wakes the commits that are waiting at the [write stop](write-stop-wait.md).

Creating a snapshot, a fork, or a merge's plan writes a [snapshot](../specs/glossary.md#snapshot) that keeps the manifest of the fold that served the request, and LoonFS verifies each snapshot after it writes the snapshot. A snapshot keeps the fold lock in shared mode from the moment it has the fold's manifest until its verification. Any number of snapshots can have the lock in shared mode at once. So a fold in the same runtime waits for the snapshots in progress, and a snapshot waits for the fold in progress.

A request that folds for its snapshot releases the fold lock, which it had in exclusive mode, before it takes the lock in shared mode, so the requests that waited for its fold can take their snapshots at the same time. Once a request has the fold lock in shared mode, it takes a snapshot of the manifest of the newest fold that it finds. A compaction unit, an extension's publication, or another process can still replace the manifest before the verification. When that happens, the request folds and takes a snapshot again, up to eight attempts in all.

Before each compaction unit chooses its window, it takes the namespace's "compaction lock", an in-memory lock that one unit of the namespace can have at a time, so two units of the namespace never run at once in one runtime. The compaction path runs units one after another while a unit is due, up to 16 units for each call, and stops early if the caller cancels the call, the namespace's writer session closes, or the namespace is deleted. A unit plans from the position's manifest, which it reads through the segment cache, or from the current manifest when the session has no position. Either manifest may have been read before the unit took the lock, and the position doesn't show manifests that another process published, such as another process's claim of the compactor epoch. So before a due unit plans, the compaction path sends one HEAD request for the manifest numbered after the unit's manifest (for a position, the same check that a strong read makes), and then reads the [manifest boundary](../specs/glossary.md#manifest-boundary). If newer manifests exist, the unit plans from the newest one, and for a position, the path moves the position onto the ones that have the session's writer epoch. If the manifest that the unit would plan from is at or below the boundary, the unit finds the namespace's newest manifest from the start and plans from that manifest instead.

Commits don't take the fold lock or the compaction lock, and a fold releases the fold lock before any compaction unit starts, so neither a commit nor a fold waits for compaction.

Whether a fold is due depends on what asks for it. For the writer session's own folds and for a scheduled metadata step, a fold is "due" once the tail reaches a fold threshold (256 WAL objects, or 8 MiB of WAL bytes), or once the tail's newest commit reaches the "idle fold age" (`idle_fold_after_ms`, 15 minutes by default), so that a tail that stops growing below the thresholds is still folded. With a position, the age is measured from the session's last publish. Without a writer session in this runtime, the check uses the object count and the WAL bytes of the tail that it loads, and the idle fold age. A merge's fold of the fork, a snapshot, a fork, an explicit fold, and a retention advance fold the tail whatever its size or age, unless a fold that took its input after such a request arrived has already finished. In that case, the request uses the finished fold's manifest. A deletion folds the tail whatever its size or age. A compaction unit is due when the manifest has a window that the compaction policy selects. The writer session runs compaction units after each fold of its own that publishes. A fold that a maintenance call or a scheduled metadata step makes doesn't start compaction. The call or the step then runs compaction units on its own whenever a unit is due, whether or not its fold published.

Why does one fold serve every request that waited for that fold? If each waiting request folded the tail that it found when it got the fold lock, ten snapshots that waited for one fold would fold ten times on a namespace that takes commits continuously, because each snapshot would find new commits in the tail. Each fold costs segment puts and a manifest put. A fold that took its input after a request arrived already includes every commit that the request has to see, so one fold serves all ten snapshots.

Why does a snapshot keep the fold lock until its verification? If a fold in the same runtime published a manifest between a snapshot's write and its verification, the verification would find the snapshot's manifest replaced, and the request would fold and take a snapshot again. Concurrent snapshots and forks of a busy namespace would then replace each other's manifests and use up their attempts. Snapshots take the fold lock in shared mode, so a snapshot doesn't wait for another snapshot.

## Session state

This section describes what the host keeps for each session that it has open.

Each entry in `Namespaces` for an open session stores:

- the handle and the last time it was opened,
- the indexed sequence number and an index-dirty flag,
- the sequence number and time of the last collection, and
- for each call (metadata, index, and collection), a retry time and the number of times in a row that the call has failed.

Dropping the entry drops this information. A run of a session's steps keeps the entry it started with, so its result can't update a replacement entry after a close. The ordered table releases its lock between entries. The scheduler has no per-namespace progress maps. The HTTP handlers that enable and disable the index keep a session open and mark its entry dirty. The grep worker doesn't keep a set of lifecycle changes for the host.

## Session steps and sweeps

The scheduler runs maintenance in "steps". A step is one bounded unit of maintenance work on one namespace: the metadata step, the collection step (one garbage collection), the index step (grep), or the close of an idle session. The scheduler runs steps on two schedules. Every `maintenance_interval_ms` (5000 ms by default), it runs each open session's due steps. A "sweep" lists every namespace in the store and runs each listed namespace's due steps, at startup and then every `sweep_interval_ms` (86400000 ms by default).

`Scheduler::run_due_steps` walks the IDs of the open sessions without cloning their handles. It runs the steps of every open session, including one that's still committing, so a namespace that's written to steadily still gets its index steps and its collection. For each session, the scheduler runs the metadata step, and then the first of the collection, index, and close steps that's due:

1. **Metadata.** The step runs when the session has a writer position and the entry's retry time for the step has come. It calls `maintain_metadata_while_due`, which folds the tail through the fold path when a fold is due, and then runs compaction units through the compaction path while a unit is due, at most 16. When the session's position shows that no fold and no compaction unit is due, the step makes no store request. Because that check is free, the step sets no retry time, and the step runs again at the next interval. If the step leaves work unfinished, the step sets the entry's retry time `metadata_retry_interval_ms` (300000 ms by default) later. If the only work left is a tail below the fold thresholds and younger than the idle fold age, the call reports when the tail reaches that age, and the step sets the retry time to that age or `metadata_retry_interval_ms` later, whichever comes first. A failed step waits for the longer of that interval and its failure backoff (described after this list). The scheduler skips the metadata step for a session without a position (one that hasn't published since it opened, or whose position was dropped), because that session isn't writing, and the sweep maintains that namespace's metadata, as the sweep does for a namespace with no open session. The one exception is the session's idle fold: when the tail of a session without a position reaches the idle fold age that the session's last step reported, the scheduler runs the step once, reading the tail from the store, and that step's own report replaces the due time. A session that has committed since its last metadata step, or whose commits no step has seen, gets that step at its retry time too, because the scheduler can't date its tail without one. If that step fails, the sweep retries it. A session that keeps its position gets the metadata step from the position at every retry time, so an idle session's step costs no store requests until work is due, and the step at the idle fold age folds the tail from the position.
2. **Collection.** This step runs core garbage collection of the namespace, which also collects the grep index's unreferenced segments. The step collects the namespace and nothing else, so it doesn't collect the content objects, upload records, or temporary objects of the namespace's [pool](../specs/glossary.md#pool). It runs when the published sequence number has moved since the last successful collection, and `gc_interval_ms` (3600000 ms by default) has elapsed. For a newly opened entry, this clock starts when the session is opened. After a failed collection, the scheduler waits for the collection call's failure backoff before it collects again. Collection comes before the index step because the scheduler runs only the first due step of the three. A session with a commit at every interval has index work at every interval, so if the index step came first, that session would never collect.
3. **Index.** This step runs when the host maintains grep, and the entry is dirty or its indexed sequence number is older than its published sequence number. The index step runs up to 16 grep steps, and each one is a reorganization step when a reorganization is due, and a build step otherwise (see [Reorganization](content-search-index.md#reorganization)). The grep steps' changes to grep's entry are published once a maintenance interval has passed since the first of them, and again when the index step ends, so an index step publishes one or a few manifests. An unfinished, failed, or cancelled build leaves the entry dirty, even without a published sequence number. An index change during a build also leaves the entry dirty. After a failed build, the scheduler waits for the index call's failure backoff before the next index step.
4. **Close.** This step runs when the session's last open was more than `idle_session_close_after_ms` (1800000 ms by default) ago, no caller has its handle, and the session's metadata needs no more steps (the metadata step in the same run found the metadata caught up, or the session has no position). A session that's written to steadily is opened for each write, so it doesn't reach this age. The step doesn't wait for the idle age when the session can't write anymore: when its caller already closed it, when another writer fenced it, when the session deleted its own namespace, or when the metadata, collection, or index step finds the namespace deleted or missing. Such a session closes as soon as no caller has its handle. The sequence number and exclusive ownership are checked together under the table lock. An open that arrives during the close waits for the drain and gets a fresh session. It doesn't receive `writer_session_closed` from this close path. A session whose collection or index call keeps failing stays open, so that the scheduler keeps retrying the call, until the call's failure backoff reaches its cap. Then the session can close, and the next sweep retries the call. A metadata step that keeps failing follows the same rule, so the session can close once the step's failure backoff reaches its cap, and the sweep retries the metadata step.

When a call fails, the entry records the failure, and the scheduler waits for the call's failure backoff before it makes that call again. The backoff is one maintenance interval after the first failure, and each failure in a row doubles it, up to one hour. A success ends the backoff, and a call that panics counts as a failure.

A closing entry stores a shared close future. Only opens of that namespace wait for its drain, and opens of other namespaces continue. Table reads and updates take a short synchronous lock, which is released before the drain. A cancelled close leaves the shared future in place, so the next open can finish it. When the close completes, it removes only its original entry. A later session's first publish acquires a new writer epoch.

Each open session keeps its publisher, engine state, head position, and allocated queue capacities. Closing the session releases them after admitted work ends. Shared caches keep their own limits.

`Scheduler::run_sweep` runs one sweep. It runs every listed namespace's steps: metadata and index maintenance, the retention floor advance under the namespace's [retention window](../specs/glossary.md#retention-window), and garbage collection. When the metadata call returns `namespace_deleted` or `namespace_not_found`, the index call and the retention advance could only return the same code, so the sweep skips both and runs the collection. A sweep runs the metadata step for every namespace through the same fold and compaction paths as the session steps. With a position, the step checks the position, and otherwise it reads the current manifest and tail from the store. The sweep then lists every pool in the store (under `pools/`) and runs a collection of each listed pool, with the same `max_concurrent_maintenance` concurrency, so a pool's content objects, upload records, and temporary objects are collected by sweeps. A sweep makes its calls without waiting for an entry's retry times. A namespace without an open session has no retry state, because the next sweep retries any call that failed. Sweeps cover forks, deleted namespaces, work left behind by a crashed process, and garbage that's still inside its grace window. A namespace that nobody writes to through this host gets steps only from sweeps.

For each namespace, a sweep advances the retention floor under the cutoff rule in [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor), with the current time minus the window as the cutoff. The sweep reads the window from the namespace's manifest, and it doesn't advance the floor of a namespace whose manifest has no window. The sweep makes the advance after the namespace's metadata step, because the advance reads only folded commits and the metadata step can fold an idle tail. The session steps don't advance the floor on their own schedule or retry a failed advance, so the entry keeps no retry state for the advance, and the next sweep tries a failed advance again. The advance plans the new floor from the namespace's position, or from the current manifest and the WAL tail after that manifest. A floor that moves is recorded in a [floor object](../specs/glossary.md#floor-object) at the next WAL number. When the host has a writer session for the namespace, the floor object is one item of the session's queue, and otherwise the advance creates the floor object itself and opens no session. When no manifest records the floor of the namespace's view, the advance then folds the namespace, so the fold's manifest records the floor. The advance claims no writer epoch, so a writer session for the namespace in another process isn't fenced. [Who advances the floor](retention-window.md#who-advances-the-floor) in the retention window note describes why a floor object orders the advance against every restore.

Sweeps and session steps share `max_concurrent_maintenance` slots, and the scheduler keeps a claim on each namespace whose steps are running, so it never runs the steps of one namespace twice at once. If a namespace is busy, the sweep leaves it to the steps that are running, and those steps also make the sweep's retention advance once they finish. A sweep lists up to 1000 IDs per page, and it requests the next page when one of its slots is free and no listed ID is waiting. After a listing failure, the sweep starts the steps of no more namespaces, and the running steps finish before the sweep returns the error. A sweep whose namespace listing fails collects no pools, and a failure of the pool listing starts no more pool collections in the same way. The next sweep lists the store again.

A pool has no entry and no retry state. The scheduler records a failed collection of a pool under the call name `pool_gc` and logs it, and the next sweep tries the collection again. A listed pool whose collection returns `pool_not_found` isn't a failure, because a create call can delete its pool record between the listing and the collection.

Sweeps and session steps run in separate loops. A long compaction uses one slot until it finishes, and the other free slots can keep working. The scheduler starts a session's steps without waiting for the steps that other sessions started at the interval before, so a session's long metadata step delays only that session: at each interval, the scheduler starts no steps for a session whose earlier steps are still pending or running, and starts the steps of every other session. All maintenance from one runtime shares that runtime's compactor claim and execution budget. Shutdown cancels both loops and stops new steps, and a running compaction stops at its next block, within the server's shutdown deadline.

## Shared read working memory

This section describes the shared budget that limits read working memory.

The budget is `max_read_working_bytes` in `ExecutionBudget`, which defaults to 256 MiB. All read-only and writable runtimes that are given clones of a budget share this read working memory. A runtime without an explicit budget creates a private one. Core callers without a runtime budget use an unshared 64 MiB budget. Metadata caches have their own limits, which are managed separately.

| Limit | Owner | Default | At the limit |
| --- | --- | --- | --- |
| `max_read_working_bytes` | `ExecutionBudget` | 256 MiB | A memo evicts its own entries, and continues without keeping blocks that don't fit. A scan's refill loads fewer blocks, and always takes its first block, which the budget records even above the limit, so the read working memory can exceed the limit by one block for each segment that a scan has open until the scan consumes the block. Nothing waits. |

Read, publication, fold, and snapshot-fold memos reserve memory from this budget, and so does each refill of a scan, for the decoded bytes of its blocks until the scan has consumed them. A memo's reservation covers:

- decoded data blocks,
- decoded indexes and filters,
- keys and reference counts, and
- allocated memo container capacity.

Evicting or dropping an entry releases its reservation. A block that a caller already has stays valid after eviction. Cache entries are charged to their own limits, even when they share an allocation with a memo. Allocator bookkeeping, temporary decoding, returned rows, and blocks borrowed after eviction aren't counted in this budget. The budget doesn't limit read concurrency.

`ExecutionBudget::stats().read_working_bytes` reports the retained bytes. The budget's recorder reports two gauges:

- `loonfs.execution_budget.read_working_bytes`
- `loonfs.execution_budget.read_working_reservation_failures`, which counts failed attempts to reserve, including retries after eviction.

The reference server sets the budget with its `max_read_working_bytes` setting, and a value of zero keeps no memo, so each scan keeps one block for each segment that it has open.

## Store request costs

This section lists how many store requests maintenance makes in common cases.

A session whose writer position shows nothing due makes no store requests across maintenance intervals, and an idle close makes none either. A session without a position makes no store requests at maintenance intervals, because the scheduler skips its metadata step, except for the one step that folds its tail at the idle fold age, and one step that dates the tail when the session committed and lost its position before a step saw the position. When the session's position moves onto a manifest that isn't in the segment cache yet (for example, the manifest that a fold or a grep build published), the next metadata step reads the manifest boundary and then that manifest with one GET, and keeps the manifest in the segment cache. A metadata step that finds another process has claimed the compactor makes 7 requests per metadata retry interval, not per maintenance interval: the HEAD that finds the manifests after the position's manifest, the reads of the walk over those manifests, a read of the manifest boundary, and a second read of the boundary before the read of the newest manifest into the segment cache. The step checks a tail that's waiting for its idle fold age once per retry interval, and again when the tail reaches that age. With a position, both checks read the position, so they make no store request.

A daily sweep costs one namespace listing per page, one pool listing per page, and these requests for each namespace that has nothing to fold, compact, or collect and has a pool of its own. The requests include the collection of the namespace's pool:

| Grep not maintained | Grep maintained, index not enabled | Grep index enabled |
| --- | --- | --- |
| 25 GET or HEAD and 9 LIST | 29 GET or HEAD and 9 LIST | 30 GET or HEAD and 9 LIST |

Objects that are still inside their grace windows, work that's due, and the roots of each fork in a pool add more requests. Deleted namespaces are still listed, because their tombstone manifests are still stored. A retired namespace is listed too, because its manifest boundary object stays under its prefix after later collections delete everything else, and each retired namespace costs every sweep 3 requests under its prefix: the metadata call reads the boundary and returns `namespace_deleted`, and the namespace's collection reads the boundary and lists the prefix once. A retired namespace that is its pool's first member costs 4, because the pool's collection reads the boundary of the pool's first member. That cost grows with the number of retired namespaces in the store.

## Recovery after a restart

After a restart, each kind of work starts again from durable state:

| Work | Durable basis after restart |
| --- | --- |
| WAL fold | The current manifest and the numbered WAL tail. |
| Metadata compaction | The current manifest, the selected input descriptors, and a new runtime epoch claim. |
| Core garbage collection | For a namespace, fresh discovery of the current manifest and a complete listing of snapshots. For a pool, the pool record, a complete listing of upload sessions, and fresh discovery of each member's current manifest. |
| Grep indexing | Grep's entry in the current manifest, with its build or reorganization cursor. |

A grep build records its progress durably in grep's entry, and steps whose changes hadn't been published when the process stopped run again. A core compaction or garbage collection has no equivalent durable cursor, so the next attempt repeats it from the start.

## Compaction after a worker stops

Compaction publishes new segment objects only after it has combined the selected runs and the result passes validation. If the process stops before then, readers keep using the previous manifest. A replacement runtime plans from the current manifest's runs, claims a newer compactor epoch when eligible work requires it, and repeats the compaction instead of resuming partially written segments. Unreferenced output is collected under the ordinary segment age rule. [Publication and restart](metadata-streaming-compaction.md#publication-and-restart) in the streaming compaction note describes the epoch, the process-local concurrency limit, and restarts in more detail.

## Garbage collection after a worker stops

Every core garbage collection call loads fresh roots, builds its live set in memory, and lists each candidate family completely, from the beginning. It doesn't write a durable run object, mark pages, or a progress cursor. An interrupted collection might have deleted some eligible objects already, and another call safely repeats the listings and the remaining cleanup.

Each call uses one clock value for all its age and retirement checks. Snapshot keys identify the manifests that must be kept for that collection. Upload records keep the evidence needed for cleanup until the content and the provider state have been handled. A retirement tombstone keeps the fork snapshot's identity, so a failed dependency release can be retried.

## Scheduling and correctness

This section describes what a maintenance schedule can and can't affect.

A sweep recovers missed opportunities to run work. It doesn't replace reference validation, publication time bounds, epoch checks, or conditional writes. No filesystem state is rebuilt from a schedule, so losing a sweep can delay work, but it can't change committed filesystem state.

Writer assignment and handoff are separate from maintenance. A host must not treat running maintenance on a namespace as permission to silently reacquire a fenced semantic writer.

[Metadata streaming compaction](metadata-streaming-compaction.md) describes execution and resource bounds. In the storage format, [Publishing a commit](../specs/format.md#6-publishing-a-commit), [Garbage collection](../specs/format.md#11-garbage-collection), and [Retirement](../specs/format.md#95-retirement) define the publication, collection, and retirement rules.
