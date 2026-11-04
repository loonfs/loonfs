# Maintenance hosting

A writable session maintains its own namespace as it publishes. The host also runs a regular tick over the sessions it has open, and a daily pass over a listing of the store for everything else. All scheduling information is temporary, and durable state determines what each maintenance call can do.

## Folding and compacting a namespace

This section describes how one runtime folds and compacts a namespace, whatever part of LoonFS asks it to.

A [fold](../specs/glossary.md#fold) writes the commits in a namespace's WAL tail into metadata segments and publishes a manifest that lists them. A compaction unit merges one window of metadata runs and publishes the result in one manifest, as [Metadata streaming compaction](metadata-streaming-compaction.md) describes. Many parts of LoonFS fold or compact a namespace:

- the namespace's writer session, which folds its own tail and then compacts,
- the host's tick and daily pass, described in [Tick and daily pass](#tick-and-daily-pass),
- a `metadata` maintenance request and an explicit fold request,
- a merge or a rebase, which folds the fork first,
- a snapshot, or a fork created from the current head, which folds the tail first,
- a retention floor acknowledgment, and
- a namespace deletion.

If two of them folded the same tail at once, both would write every content object in the tail, and one of the two manifest puts would fail. If two compaction units planned from the same manifest, one of them would do its work for nothing.

A writer session's [writer position](../specs/glossary.md#writer-position) is the session's in-memory record of the manifest that the session last published or found current, and of the unfolded WAL tail above that manifest. For example, after the session publishes manifest 5 and then commits WAL objects 41 to 43, the position records manifest 5 and a tail of three WAL objects. The position is "current" for 19.25 minutes (the [revalidation bound](../specs/glossary.md#revalidation-bound)) after the session last checked it against the store. After that, another writer may have committed, and the position wouldn't show that commit. A session has no position before its first publish, and it drops its position after a fork of its namespace or another writer's claim. The session's next publish sets a new position.

So each runtime has one "fold path" and one "compaction path" for each namespace, and every part of LoonFS that folds or compacts the namespace goes through them. The fold path is the one place where the runtime folds the namespace, and it runs one fold at a time, so a fold that starts during another fold waits for that fold to finish. The compaction path does the same for compaction units.

Every fold first takes the namespace's "fold lock", an in-memory lock that one fold of the namespace can have at a time. LoonFS checks whether the fold is due (defined below) after the fold has the lock, so if a fold waited for another fold, the check finds the tail already folded and the second fold does nothing. With a current position, LoonFS also checks the position before the fold takes the lock, and if no fold is due, the fold returns without a store request and without the lock. A fold that's due then:

1. Waits for a fold permit from the runtime's execution budget.
2. Writes the tail into metadata segments and publishes a manifest that lists them. With a current position, the fold takes the tail from the position without reading the WAL. Without one, it reads the current manifest and the tail from the store.
3. Moves the session's position onto that manifest.
4. Raises the namespace's hint to the new manifest number.
5. Wakes the commits that are waiting at the [write stop](write-stop-wait.md).

Before each compaction unit chooses its window, it takes the namespace's "compaction lock", an in-memory lock that one unit of the namespace can have at a time, so two units of the namespace never run at once in one runtime. The compaction path runs units one after another while a unit is due, up to 16 units for each call, and stops early if the caller cancels the call, the namespace's writer session closes, or the namespace is deleted. A unit plans from the position's manifest, which it reads through the segment cache, or from the current manifest when the session has no current position. Either manifest may have been read before the unit took the lock, and the position doesn't show manifests that another process published, such as another process's claim of the compactor epoch. So before a due unit plans, the compaction path sends one HEAD request for the manifest numbered after the unit's manifest (for a position, the same check that a strong read makes). If newer manifests exist, the unit plans from the newest one, and for a position, the path moves the position onto the ones that have the session's writer epoch.

Commits don't take either lock, and a fold releases the fold lock before any compaction unit starts, so neither a commit nor a fold waits for compaction.

Whether a fold is due depends on what asks for it. For the writer session's own folds and for a tick's metadata step, a fold is "due" once the tail reaches a fold threshold (32 WAL objects, or 2 MiB of inline content by default), or once the tail's newest commit reaches the "idle fold age" (`idle_fold_after_ms`, 15 minutes by default), so that a tail that stops growing below the thresholds is still folded. With a current position, the age is measured from the session's last publish. Without a writer session in this runtime, the check uses the object count and the idle fold age. A merge, a snapshot, a fork, an explicit fold, a retention floor acknowledgment, and a deletion fold the tail whatever its size or age. A compaction unit is due when the manifest has a window that the compaction policy selects. The writer session runs compaction units after each fold of its own that publishes. A fold that a maintenance call or a tick's metadata step makes doesn't start compaction. The call or the step then runs compaction units on its own whenever a unit is due, whether or not its fold published.

## Session state

This section describes what the host keeps for each session that it has open.

Each entry in `Namespaces` for an open session stores:

- the handle and the last time it was opened,
- the indexed sequence number and an index-dirty flag,
- the sequence number and time of the last collection, and
- for each call (metadata, index, and collection), a retry time and the number of times in a row that the call has failed.

Dropping the entry drops this information. A visit keeps the entry it started with, so its result can't update a replacement entry after a close. The ordered table releases its lock between entries. The sweep has no per-namespace progress maps. The HTTP handlers that enable and disable the index keep a session open and mark its entry dirty. The grep worker doesn't keep a set of lifecycle changes for the host.

## Tick and daily pass

The host runs maintenance on two schedules. A "tick" is one round of maintenance over the sessions that the host has open, and it runs every `tick_interval_ms` (5000 ms by default). The "daily pass" lists every namespace in the store and maintains each one.

`Sweep::tick` walks the IDs of the open sessions without cloning their handles. The tick visits every open session, including one that's still committing, so a namespace that's written to steadily still gets its index steps and its collection. For each session, the tick runs the metadata step, and then the first of the collection, index, and close steps that's due:

1. **Metadata.** The step runs when the session has a current writer position and the entry's retry time for the step has come. It calls `maintain_metadata_while_due`, which folds the tail through the fold path when a fold is due, and then runs compaction units through the compaction path while a unit is due, at most 16. When the session's position shows that no fold and no compaction unit is due, the step makes no store request. Because that check is free, the step sets no retry time, and the next tick checks again. If the step leaves work unfinished, the step sets the entry's retry time `maintenance_interval_ms` (300000 ms by default) later. If the only work left is a tail below the fold thresholds and younger than the idle fold age, the call reports when the tail reaches that age, and the step sets the retry time to that age or `maintenance_interval_ms` later, whichever comes first. A failed step waits for the longer of that interval and its failure backoff (described after this list). The tick skips the metadata step for a session without a current position, because that session isn't writing, and the daily pass maintains that namespace's metadata, as the pass does for a namespace with no open session. The one exception is the session's idle fold: when the tail of a session without a current position reaches the idle fold age that the session's last step reported, the next tick runs the step once, reading the tail from the store, and that step's own report replaces the due time. A session that has committed since its last metadata step, or whose commits no step has seen, gets that step at its retry time too, because the sweep can't date its tail without one; if that step fails, the daily pass retries it. (A slow writer's position can stop being current before its tail reaches the idle fold age, because a publish checks the position only every few minutes.)
2. **Collection.** This step runs core garbage collection, which also collects the grep index's unreferenced segments. It runs when the published sequence number has moved since the last successful collection, and `gc_interval_ms` (3600000 ms by default) has elapsed. For a newly opened entry, this clock starts when the session is opened. After a failed collection, the ticks wait for the collection call's failure backoff before they collect again. Collection comes before the index step because a tick runs only the first due step of the three. A session with a commit on every tick has index work on every tick, so if the index step came first, that session would never collect.
3. **Index.** This step runs when the host maintains grep, and the entry is dirty or its indexed sequence number is older than its published sequence number. A visit runs up to 16 steps, and each one is a reorganization step when a reorganization is due, and a build step otherwise (see [Reorganization](content-search-index.md#reorganization)). The steps' changes to grep's entry are published once a tick interval has passed since the first of them, and again when the visit ends, so a visit publishes one or a few manifests. An unfinished, failed, or cancelled build leaves the entry dirty, even without a published sequence number. An index change during a build also leaves the entry dirty. After a failed build, the ticks wait for the index call's failure backoff before the next index step.
4. **Close.** This step runs when the session's last open was more than `idle_session_close_after_ms` (1800000 ms by default) ago, no caller has its handle, and the session's metadata needs no more ticks (this tick's metadata step found the metadata caught up, or the session has no current position). A session that's written to steadily is opened for each write, so it doesn't reach this age. The step doesn't wait for the idle age when the session can't write anymore: when its caller already closed it, when another writer fenced it, when the session deleted its own namespace, or when the metadata, collection, or index step finds the namespace deleted or missing. Such a session closes as soon as no caller has its handle. The sequence number and exclusive ownership are checked together under the table lock. An open that arrives during the close waits for the drain and gets a fresh session. It doesn't receive `writer_session_closed` from this close path. A session whose collection or index call keeps failing stays open, so that the ticks keep retrying the call, until the call's failure backoff reaches its cap. Then the session can close, and the next daily pass retries the call. A failed metadata step doesn't check the position against the store. So if the step keeps failing, the session's position stops being current 19.25 minutes after its last check, the session can close after that, and the daily pass retries the metadata step.

When a call fails, the entry records the failure, and the ticks wait for the call's failure backoff before they make that call again. The backoff is one tick interval after the first failure, and each failure in a row doubles it, up to one hour. A success ends the backoff, and a call that panics counts as a failure.

A closing entry stores a shared close future. Only opens of that namespace wait for its drain, and opens of other namespaces continue. Table reads and updates take a short synchronous lock, which is released before the drain. A cancelled close leaves the shared future in place, so the next open can finish it. When the close completes, it removes only its original entry. A later session's first publish acquires a new writer epoch.

Each open session keeps its publisher, engine state, head position, and allocated queue capacities. Closing the session releases them after admitted work ends. Shared caches keep their own limits.

`Sweep::run_pass` runs the listing pass. It runs at startup, and then every `full_sweep_interval_ms` (86400000 ms by default), alongside the ticks. It visits every listed namespace, runs metadata and index maintenance, advances the retention floor under the namespace's [retention window](../specs/glossary.md#retention-window), and collects garbage. The pass runs the metadata step for every namespace through the same fold and compaction paths as a tick. With a current position, the step checks the position, and otherwise it reads the current manifest and tail from the store. The pass makes its calls without waiting for an entry's retry times. A namespace without an open session has no retry state, because the next pass retries any call that failed. This pass covers forks, deleted namespaces, work left behind by a crashed process, and garbage that's still inside its grace window. A namespace that nobody writes to through this host gets no tick visits.

Each visit of the pass advances the namespace's retention floor under the cutoff rule in [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor), with the current time minus the window as the cutoff. The pass uses the window in the namespace's manifest, or the server's `retention_window_ms` if the manifest has no window. A namespace with neither window keeps its floor. The visit makes the advance after its metadata step, because the advance reads only folded commits and the metadata step can fold an idle tail. The ticks don't advance the floor on their own schedule or retry a failed advance, so the entry keeps no retry state for the advance, and the next pass tries a failed advance again. Before the advance, the visit checks whether the writer's log has acknowledged the floor that the previous pass set, and if it hasn't, the visit folds the tail and, if the acknowledged floor still lags, acquires the writer epoch and writes a fence before it folds again. [Advancing the floor on a schedule](retention-window.md#advancing-the-floor-on-a-schedule) in the retention window note describes the acknowledgment, and why a base compaction waits for it before it removes the rows of the deletions that the floor passed.

The tick and the pass share `max_concurrent_maintenance` visit slots and a per-namespace set of visits in progress, so they never visit the same namespace at the same time. If a namespace is busy, the pass leaves it to its current visit, and that visit also makes the pass's retention advance once its own step finishes. A full pass lists up to 1000 IDs per page, and it requests the next page when a visit slot in that pass is free and no listed ID is waiting. After a listing failure, the pass starts no more visits, and the running visits finish before the pass returns the error. The next daily pass lists the store again.

The tick and the daily pass run in separate loops. A long compaction uses one visit slot until it finishes, and the other free slots can keep working. A tick ends only when its last visit ends, so a session's long metadata step delays the next tick for every session, for as long as that step's compaction units (up to 16) take to run. All maintenance from one runtime shares that runtime's compactor claim and execution budget. Shutdown cancels both loops and stops new visits, and a running compaction stops at its next block, within the server's shutdown deadline.

## Shared read working memory

This section describes the shared pool that limits read working memory.

The pool's size is `max_read_working_bytes` in `ExecutionBudget`, which defaults to 256 MiB. All read-only and writable runtimes that are given clones of a budget share this pool. A runtime without an explicit budget creates a private one. Core callers without a runtime budget use an unshared 64 MiB pool. Metadata caches have their own limits, which are managed separately.

| Limit | Owner | Default | At the limit |
| --- | --- | --- | --- |
| `max_read_working_bytes` | `ExecutionBudget` | 256 MiB | A memo evicts its own entries, and continues without keeping blocks that don't fit. Nothing waits. |

Read, publication, fold, and pin-fold memos reserve memory from this pool. A reservation covers:

- decoded data blocks,
- stored read-ahead bytes,
- decoded indexes and filters,
- keys and reference counts, and
- allocated memo container capacity.

Evicting or dropping an entry releases its reservation. A block that a caller already has stays valid after eviction. Cache entries are charged to their own limits, even when they share an allocation with a memo. Allocator bookkeeping, temporary decoding, returned rows, and blocks borrowed after eviction aren't counted in this pool. The pool doesn't limit read concurrency.

`ExecutionBudget::stats().read_working_bytes` reports the retained bytes. The budget's recorder reports two gauges:

- `loonfs.execution_budget.read_working_bytes`
- `loonfs.execution_budget.read_working_reservation_failures`, which counts failed attempts to reserve, including retries after eviction.

The reference server sets the pool's size with its `max_read_working_bytes` setting, and a value of zero turns off memo retention.

## Store request costs

This section lists how many store requests maintenance makes in common cases.

A session whose writer position shows nothing due makes no store requests across ticks, and an idle close makes none either. A session without a current position makes no store requests on ticks, because the ticks skip its metadata step, except for the one step that folds its tail at the idle fold age, and one step that dates the tail when the session committed and no tick saw its position while it was current. When the session's position moves onto a manifest that isn't in the segment cache yet (for example, the manifest that a fold or a grep build published), the next metadata step reads that manifest with one GET and keeps it in the segment cache. A metadata step that finds another process has claimed the compactor makes 6 requests per metadata retry interval, not per tick: a read of the position's manifest, the HEAD that finds the manifests after it, the reads of those manifests and the HEAD that finds where they end, and a read of the newest one into the segment cache. The step checks a tail that's waiting for its idle fold age once per retry interval, and again when the tail reaches that age. With a current position, both checks read the position, so they make no store request.

A daily pass costs one namespace listing per page, plus these requests for each namespace that has nothing to fold, compact, or collect:

| Pass | Grep not maintained | Grep maintained, index not enabled | Grep index enabled |
| --- | --- | --- | --- |
| Does not collect | 14 GET or HEAD | 18 GET or HEAD | 19 GET or HEAD |
| Collects | 20 GET or HEAD and 11 LIST | 24 GET or HEAD and 11 LIST | 26 GET or HEAD and 11 LIST |

Objects that are still inside their grace windows, and work that's due, add more requests. Deleted namespaces are still listed, because their tombstone manifests are still stored.

## Recovery after a restart

After a restart, each kind of work starts again from durable state:

| Work | Durable basis after restart |
| --- | --- |
| WAL fold | The current manifest and the numbered WAL tail. |
| Metadata compaction | The current manifest, the selected input descriptors, and a new runtime epoch claim. |
| Core garbage collection | Fresh discovery of the current manifest, and complete listings of upload sessions and pins. |
| Grep indexing | Grep's entry in the current manifest, with its build or reorganization cursor. |

A grep build records its progress durably in grep's entry, and steps whose changes hadn't been published when the process stopped run again. A core compaction or garbage collection pass has no equivalent durable cursor, so the next attempt repeats it from the start.

## Compaction after a worker stops

Compaction publishes new segment objects only after the selected merge completes and passes validation. If the process stops before then, readers keep using the previous manifest. A replacement runtime plans from the current file set, claims a newer compactor epoch when eligible work requires it, and repeats the merge instead of resuming partially written segments. Unreferenced output is collected under the ordinary segment age rule. [Publication and restart](metadata-streaming-compaction.md#publication-and-restart) in the streaming compaction note describes the epoch, the process-local concurrency limit, and restarts in more detail.

## Garbage collection after a worker stops

Every core garbage collection call loads fresh roots, builds its live set in memory, and lists each candidate family completely, from the beginning. It doesn't write a durable run object, mark pages, or a progress cursor. An interrupted pass might have deleted some eligible objects already, and another call safely repeats the listings and the remaining cleanup.

Each call uses one clock value for all its age and retirement checks. Pin keys identify the manifests that must be kept for that pass. Upload records keep the evidence needed for cleanup until the content and the provider state have been handled. A retirement tombstone keeps the source pin's identity, so a failed dependency release can be retried.

Grep garbage collection also completes one explicit pass without a cursor. It works only on its own extension prefix, and core garbage collection doesn't collect those objects.

## Scheduling and correctness

This section describes what a maintenance schedule can and can't affect.

A pass recovers missed opportunities to run work. It doesn't replace reference validation, publication time bounds, epoch checks, or conditional writes. No filesystem state is rebuilt from a schedule, so losing a pass can delay work, but it can't change committed filesystem state.

Writer assignment and handoff are separate from maintenance. A host must not treat running maintenance on a namespace as permission to silently reacquire a fenced semantic writer.

[Metadata streaming compaction](metadata-streaming-compaction.md) describes execution and resource bounds. In the storage format, [Publishing a commit](../specs/format.md#6-publishing-a-commit), [Garbage collection](../specs/format.md#11-garbage-collection), and [Retirement](../specs/format.md#95-retirement) define the publication, collection, and retirement rules.
