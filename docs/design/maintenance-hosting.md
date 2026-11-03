# Maintenance hosting

A writable session maintains its own namespace as it publishes. The host also runs a regular tick over the sessions it has open, and a daily pass over a listing of the store for everything else. All scheduling information is temporary, and durable state determines what each maintenance call can do.

## Session state

This section describes what a session and the host keep to schedule the session's maintenance.

A session folds its WAL tail at the fold thresholds and compacts after each fold that it publishes, so most of a busy namespace's metadata work happens without the host. The host covers the rest with a "tick", which runs every 5 seconds by default and visits every open session. The ticks handle a tail that stopped growing below the fold thresholds, compaction that the session left unfinished, the grep index, collection, and closing an idle session. The host doesn't track whether a session is receiving commits. On each tick, the host asks the session for its metadata work, and the session checks its own state to see whether a fold or a compaction is due.

Metadata work is "due" when the session would run it if it were asked now. To find the work that's due, the session checks its [writer position](../specs/glossary.md#writer-position), which records the unfolded tail above the manifest that the session last published or found current:

- A fold is due when the tail reaches the fold thresholds, or when the session's last publish reaches the "idle fold age" (`idle_fold_after_ms`, 15 minutes by default). The idle fold age lets a tail that stops growing below the thresholds be folded too.
- Compaction is due when the position's manifest has a compaction unit due under the compaction policy. (A compaction unit merges one window of metadata runs and publishes the result in one manifest, as [Metadata streaming compaction](metadata-streaming-compaction.md) describes.) The session reads that manifest through the in-memory segment cache.
- A session without a current position can't trust what it knows about its tail, so it reads the tail and the current manifest from the store. A position is "current" while its last check is within the [revalidation bound](../specs/glossary.md#revalidation-bound). After that, another writer may have committed without the session seeing it. A session has no position before its first publish. A fork of its namespace drops the position, and so does a read that finds the position's last check at the revalidation bound.

Metadata is "caught up" when neither a fold nor a compaction is due. When a metadata call that read the store finds metadata caught up, nothing can become due without another commit, so the session keeps that answer in memory. The kept answer applies until the first of these:

- The session attempts a batch. A batch whose WAL put is written but whose outcome is unknown leaves a tail without moving the session's published sequence number.
- The session's position is dropped. A fork's fold, for example, can add a run that makes compaction due.
- The revalidation bound passes after the call that found the answer, so the session reads the store again at least that often, and a tail that another writer left is folded within the bound.

A later metadata call from a session without a current position returns the kept answer while it applies, so such a session reads the store about once per revalidation bound rather than on every tick.

The runtime records the session's last published sequence number and the monotonic time of that publish, which the idle fold age is measured from. Before each compaction unit, the publisher's compaction checks the session's writer position for an unfolded tail and then reads only the current manifest, so it doesn't read back the WAL objects that the session wrote. (A fold publishes its manifest before it moves the position, so a tail that the position shows folded is in the manifest that the compaction reads next.)

Each entry in `Namespaces` for an open session stores:

- the handle and the last time it was opened,
- the indexed sequence number and an index-dirty flag,
- the sequence number and time of the last collection, and
- for each call (metadata, index, and collection), a retry time and the number of times in a row that the call has failed.

Dropping the entry drops this information. A visit keeps the entry it started with, so its result can't update a replacement entry after a close. The ordered table releases its lock between entries. The sweep has no per-namespace progress maps. The HTTP handlers that enable and disable the index keep a session open and mark its entry dirty. The grep worker doesn't keep a set of lifecycle changes for the host.

## Tick and daily pass

The host runs maintenance on two schedules, a frequent tick over open sessions and a daily pass over the whole store.

Every `tick_interval_ms` (5000 ms by default), `Sweep::tick` walks the IDs of the open sessions without cloning their handles. A tick visits every open session, whether or not the session is receiving commits, so a namespace that's written to steadily still gets its index steps and its collection. For each session, the tick makes the metadata call, and then runs the first of the collection, index, and close steps that's due:

1. **Metadata.** The tick makes one metadata call through the session (`Namespace::maintain_metadata_while_due`), unless the entry's retry time for the call is still in the future. The session folds the tail if a fold is due, and then compacts. It starts the fold only after any fold that it's already running, and the compaction only after any compaction that it's already running, so the call never repeats a fold or a compaction that the session is running. A call publishes at most 16 compaction units. A call that finds metadata caught up sets no retry time, so the next tick asks again. The tick doesn't ask a fenced session, and when the call returns `writer_fenced`, `namespace_deleted`, or `namespace_not_found`, the tick closes the session (step 4) instead of retrying. If the call leaves work due (including a tail that's waiting for its idle fold age), the entry waits `maintenance_interval_ms` (300000 ms by default) before another metadata call, and if the call fails, the entry waits for the longer of that interval and the call's failure backoff (described after this list). When the tail reaches the idle fold age, one earlier call is allowed unless the call is failing, and an unsuccessful call after that age uses the retry delay.
2. **Collection.** This step runs core garbage collection, which also collects the grep index's unreferenced segments. It runs when the published sequence number has moved since the last successful collection, and `gc_interval_ms` (3600000 ms by default) has elapsed. For a newly opened entry, this clock starts when the session is opened. After a failed collection, the ticks wait for the collection call's failure backoff before they collect again. Collection comes before the index step because it's due at most once per interval, and a session with a commit on every tick has index work on every tick.
3. **Index.** This step runs when the host maintains grep, and the entry is dirty or its indexed sequence number is older than its published sequence number. It runs at most once per tick for each session, and `[grep].max_concurrent_steps` limits how many build and reorganization steps have data in memory at once across all namespaces. A visit runs up to 16 build steps, and then a reorganization step. The steps' changes to grep's entry are published once a tick interval has passed since the first of them, and again when the visit ends, so a visit publishes one or a few manifests. An unfinished, failed, or cancelled build leaves the entry dirty, even without a published sequence number. An index change during a build also leaves the entry dirty. After a failed build, the ticks wait for the index call's failure backoff before the next index step.
4. **Close.** This step runs when the session's metadata call on this tick found metadata caught up, the session was last opened more than `idle_session_close_after_ms` (1800000 ms by default) ago, and no caller has the session's handle. A session that's written to steadily is opened for each write, so it doesn't reach this age. A fenced session, or the session of a namespace that's gone, closes without the idle age, as soon as no caller has its handle. The sequence number and exclusive ownership are checked together under the table lock. An open that arrives during the close waits for the drain and gets a fresh session. It doesn't receive `writer_session_closed` from this close path. A session with a failing call stays open, so that the ticks keep retrying the call, until the call's failure backoff reaches its cap. Then the session can close, and the next daily pass retries the call.

When a call fails, the entry records the failure, and the ticks wait for the call's failure backoff before they make that call again. The backoff is one tick interval after the first failure, and each failure in a row doubles it, up to one hour. A success ends the backoff, and a call that panics counts as a failure.

A closing entry stores a shared close future. Only opens of that namespace wait for its drain, and opens of other namespaces continue. Table reads and updates take a short synchronous lock, which is released before the drain. A cancelled close leaves the shared future in place, so the next open can finish it. When the close completes, it removes only its original entry. A later session's first publish acquires a new writer epoch.

Each open session keeps its publisher, engine state, head position, and allocated queue capacities. Closing the session releases them after admitted work ends. Shared caches keep their own limits.

`Sweep::run_pass` runs the listing pass. It runs at startup, and then every `full_sweep_interval_ms` (86400000 ms by default), alongside the ticks. It visits every listed namespace, runs metadata maintenance and index maintenance, advances the retention floor under the namespace's [retention window](../specs/glossary.md#retention-window), and collects garbage. For a namespace that has an open session on this host, the pass makes no metadata call, because the ticks make that call through the session whenever its retry time has come: on every tick after a caught-up answer, and after `maintenance_interval_ms` or the failure backoff after any other answer. A session without a current position reads the store at least once per revalidation bound, so a tail that another writer left on the namespace is folded within that bound without the pass. The pass still makes the other calls on that namespace, because the ticks index and collect only after the session's own commits, and they don't advance the retention floor. The pass makes its calls without waiting for an entry's retry times. A namespace without an open session has no retry state, because the next pass retries any call that failed. This pass covers forks, deleted namespaces, work left behind by a crashed process, and garbage that's still inside its grace window. A namespace that nobody writes to through this host gets no tick visits.

Each visit of the pass advances the namespace's retention floor under the cutoff rule in [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor), with the current time minus the window as the cutoff. The pass uses the window in the namespace's manifest, or the server's `retention_window_ms` if the manifest has no window. A namespace with neither window keeps its floor. The visit makes the advance after its metadata call, because the advance reads only folded commits and the metadata call can fold an idle tail. (On a namespace with an open session, the tick's metadata call folds the idle tail.) The ticks don't advance the floor on their own schedule or retry a failed advance, so the entry keeps no retry state for the advance, and the next pass tries a failed advance again. Before the advance, the visit checks whether the writer's log has acknowledged the floor that the previous pass set, and if it hasn't, the visit folds the tail and, if the acknowledged floor still lags, acquires the writer epoch and writes a fence before it folds again. [Advancing the floor on a schedule](retention-window.md#advancing-the-floor-on-a-schedule) in the retention window note describes the acknowledgment, and why a base compaction waits for it before it removes the rows of the deletions that the floor passed.

The tick and the pass share `max_concurrent_maintenance` visit slots and a per-namespace set of visits in progress, so they never visit the same namespace at the same time. If a namespace is busy, the pass leaves it to its current visit, and that visit also makes the pass's retention advance once its own step finishes. A full pass lists up to 1000 IDs per page, and it requests the next page when a visit slot in that pass is free and no listed ID is waiting. After a listing failure, the pass starts no more visits, and the running visits finish before the pass returns the error. The next daily pass lists the store again.

The tick and the daily pass run in separate loops. A long compaction uses one visit slot until it finishes, and the other free slots can keep working. A tick ends only when its last visit ends, so a session's long metadata call delays the next tick for every session, by up to the call's 16 compaction units. All maintenance from one runtime shares that runtime's compactor claim and execution budget. Shutdown cancels both loops and stops new visits, and a running compaction stops at its next block, within the server's shutdown deadline.

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

A session with a current [writer position](../specs/glossary.md#writer-position) checks the position instead of the store when a tick asks for its metadata work, so while nothing is due, the session makes no store requests across ticks. An idle close makes no requests either. A session without a current position reads the store on its first metadata call (10 requests, for a namespace that has nothing to fold or compact and whose hint is at its current manifest). It then returns its kept caught-up answer until its next batch, a drop of its position, or the revalidation bound, so while nothing is due it reads the store about once per revalidation bound (19 minutes and 15 seconds). A fenced session costs no requests, because the ticks don't ask it and close it once no caller has its handle. After a grep step publishes a manifest, the session's position moves onto it, and the next metadata call reads that manifest into the segment cache with one GET. When a session's tail is waiting for its idle fold age, a metadata call reads the tail from the session's position, so the call makes no store requests while the position is within its revalidation bound. The tick asks about a waiting tail again on the metadata retry interval, and when the tail first reaches the idle fold age. When a compactor in another process has claimed the namespace, the session's position doesn't show the claim, so a metadata call runs one compaction unit, reading its inputs and writing its outputs, before the unit's manifest put fails because of the claim. The session pays this once for each claim by another process, because its next attempt claims the compactor again, and its own compaction after a fold pays the same cost. The ticks repeat the call only after the metadata retry interval, not on every tick.

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
