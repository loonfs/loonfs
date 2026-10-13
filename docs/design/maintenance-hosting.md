# Maintenance hosting

A writable session maintains its own namespace as it publishes. The host also runs a regular tick over the sessions it has open, and a daily pass over a listing of the store for everything else. All scheduling information is temporary, and durable state determines what each maintenance call can do.

## Session state

Each session that the host has open is in one of four maintenance states:

| State | Meaning | Leaves when |
| --- | --- | --- |
| Active | Commits are arriving. The publisher folds at the fold thresholds and compacts after a fold. | A tick passes with no commit. |
| Settling | Metadata, index work, or collection is still due. | A tick finds nothing due. |
| Quiet | Metadata is caught up, the maintained index is current, and collection isn't due. Ticks cost no store requests. | A commit arrives, an index lifecycle changes, or collection becomes due. |
| Closed | A Quiet session passed its idle threshold, and no caller had its handle. | A later open creates a fresh session. |

The runtime records the last published sequence number and the monotonic time of that publish. A publish clears the "metadata caught up" flag. After its fold, the publisher sets the flag again from the maintenance result, but only if the published sequence number still matches. If the publisher's own maintenance leaves a tail that needs a later idle fold, the flag stays cleared. The host records metadata results with the same sequence check, so a concurrent publish stays due.

Each entry in `Namespaces` for an open session stores:

- the handle and the last time it was opened,
- the indexed sequence number and an index-dirty flag,
- the sequence number and time of the last collection, and
- the metadata retry time.

Dropping the entry drops this information. A visit keeps the entry it started with, so its result can't update a replacement entry after a close. The ordered table releases its lock between entries. The sweep has no per-namespace progress maps. The HTTP handlers that enable and disable the index keep a session open and mark its entry dirty. The grep worker doesn't keep a set of lifecycle changes for the host.

## Tick and daily pass

The host runs maintenance on two schedules, a frequent tick over open sessions and a daily pass over the whole store.

Every `tick_interval_ms` (5000 ms by default), `Sweep::tick` walks the IDs of the open sessions without cloning their handles. Once a session has gone one tick without a commit, the tick runs the first step that's due for it:

1. **Metadata.** While metadata isn't caught up (including when a tail is waiting for its idle fold age), the tick makes one `maintain_metadata_while_due` call. A call publishes at most 16 compaction units. If the call leaves work unfinished or fails, the entry waits `maintenance_interval_ms` (300000 ms by default) before another metadata call. When the tail reaches the idle fold age, one earlier attempt is allowed, and an unsuccessful attempt after that age uses the retry delay.
2. **Index.** This step runs when the host maintains grep, and the entry is dirty or its indexed sequence number is older than its published sequence number. A visit runs up to 16 build steps, and then a reorganization step. An unfinished, failed, or cancelled build leaves the entry dirty, even without a published sequence number. An index change during a build also leaves the entry dirty.
3. **Collection.** This step runs core garbage collection, and grep garbage collection when the host maintains grep. It runs when the published sequence number has moved since the last successful collection, and `gc_interval_ms` (3600000 ms by default) has elapsed. For a newly opened entry, this clock starts when the session is opened.
4. **Close.** This step runs when the entry is Quiet, its last open was more than `idle_session_close_after_ms` (1800000 ms by default) ago, and no caller has its handle. The sequence number and exclusive ownership are checked together under the table lock. An open that arrives during the close waits for the drain and gets a fresh session. It doesn't receive `writer_session_closed` from this close path.

A closing entry stores a shared close future. Only opens of that namespace wait for its drain, and opens of other namespaces continue. Table reads and updates take a short synchronous lock, which is released before the drain. A cancelled close leaves the shared future in place, so the next open can finish it. When the close completes, it removes only its original entry. A later session's first publish acquires a new writer epoch.

Each open session keeps its publisher, engine state, head position, and allocated queue capacities. Closing the session releases them after admitted work ends. Shared caches keep their own limits.

`Sweep::run_pass` runs the listing pass. It runs at startup, and then every `full_sweep_interval_ms` (86400000 ms by default), alongside the ticks. It visits every listed namespace, runs metadata and index maintenance, and collects garbage. This pass covers forks, deleted namespaces, work left behind by a crashed process, and garbage that's still inside its grace window. A namespace that nobody writes to through this host gets no tick visits.

The tick and the pass share `max_concurrent_maintenance` visit slots and a per-namespace set of visits in progress, so they never visit the same namespace at the same time. If a namespace is busy, it's left to its current visit. A full pass lists up to 1000 IDs per page, and it requests the next page when a visit slot in that pass is free and no listed ID is waiting. After a listing failure, the pass starts no more visits, and the running visits finish before the pass returns the error. The next daily pass lists the store again.

The tick and the daily pass run in separate loops. A streaming compaction uses one visit slot until it finishes, and the other free slots can keep working. All maintenance from one runtime shares that runtime's compactor claim and execution budget. Shutdown cancels both loops and stops new visits, and a streaming compaction stops at its next block, within the server's shutdown deadline.

## Shared read working memory

This section describes the shared pool that limits read working memory.

The pool's size is `max_read_working_bytes` in `ExecutionBudget`, which defaults to 256 MiB. All read-only and writable runtimes that are given clones of a budget share this pool. A runtime without an explicit budget creates a private one. Core callers without a runtime budget use an unshared 64 MiB pool. Metadata caches have their own limits, which are managed separately.

| Limit | Owner | Default | At the limit |
| --- | --- | --- | --- |
| `max_read_working_bytes` | `ExecutionBudget` | 256 MiB | A memo evicts its own entries, and continues without keeping blocks that don't fit. Nothing waits. |

Read, publication, fold, pin-fold, and bounded compaction memos reserve memory from this pool. A reservation covers:

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

A Quiet session makes no store requests across ticks, and an idle close makes none either. An unfinished metadata visit that finds another process has claimed the compactor makes 6 requests per metadata retry interval, not per tick. A waiting tail is retried on that interval, and when it first reaches the idle fold age.

A daily pass costs one namespace listing per page, plus these requests for each namespace that has nothing to fold, compact, or collect:

| Pass | Grep not maintained | Grep maintained, index not enabled | Grep index enabled |
| --- | --- | --- | --- |
| Does not collect | 6 GET or HEAD | 7 GET or HEAD | 26 GET or HEAD |
| Collects | 12 GET or HEAD and 10 LIST | 20 GET or HEAD and 12 LIST | 38 GET or HEAD and 12 LIST |

Objects that are still inside their grace windows, and work that's due, add more requests. Deleted namespaces are still listed, because their tombstone manifests are still stored.

## Recovery after a restart

After a restart, each kind of work starts again from durable state:

| Work | Durable basis after restart |
| --- | --- |
| WAL fold | The current manifest and the numbered WAL tail. |
| Metadata compaction | The current manifest, the selected input descriptors, and a new runtime epoch claim. |
| Core garbage collection | Fresh discovery of the current manifest, and complete listings of upload sessions and pins. |
| Grep indexing | The current grep manifest and its build or reorganization cursor. |
| Grep garbage collection | Fresh discovery of the grep manifest, and complete candidate listings. |

A grep build records its progress durably in its manifest. A core compaction or garbage collection pass has no equivalent durable cursor, so the next attempt repeats it from the start.

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
