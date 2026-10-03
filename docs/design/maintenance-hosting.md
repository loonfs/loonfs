# Maintenance hosting

A writable session maintains its own namespace as it publishes. The host ticks the sessions it holds and runs a daily listing pass for the rest of the store. All scheduling facts are temporary. Durable state decides what each maintenance call can do.

## Session state

| State | Meaning | Leaves when |
| --- | --- | --- |
| Active | Commits are arriving. The publisher folds at the fold thresholds and compacts after a fold. | No commit for one tick. |
| Settling | Metadata, index work, or collection remains due. | A tick finds nothing due. |
| Quiet | Metadata is caught up, the maintained index is current, and collection is not due. Ticks cost no store requests. | A commit arrives, an index lifecycle changes, or collection becomes due. |
| Closed | A Quiet session passed its idle threshold and no caller held its handle. | A later open creates a fresh session. |

The runtime records the last published seq and its monotonic time. A publish clears the metadata caught-up flag. The publisher sets it from the maintenance result after its fold, only if the published seq still matches. Its self-maintenance leaves a tail requiring a later idle fold unsettled. The host records metadata results with the same seq check, so a concurrent publish remains due.

Each held entry in `Namespaces` keeps the handle, last opened time, indexed seq, index-dirty flag, collected seq and time, and metadata retry time. Dropping the entry drops these facts. Visits retain the entry they started with, so a result cannot update a replacement after close. The ordered table releases its lock between entries. The sweep has no namespace progress maps. The HTTP enable and disable handlers hold a session and mark its entry dirty. The grep worker keeps no host lifecycle-change set.

## Tick and daily pass

Every `tick_interval_ms`, 5000 ms by default, `Sweep::tick` walks held ids without cloning handles. Once a session has gone one tick without a commit, it runs the first due step:

1. Metadata: one `maintain_metadata_while_due` call while metadata is not caught up, including a tail waiting for its idle fold age. A call publishes at most 16 compaction units. If it remains unfinished or fails, the entry waits `maintenance_interval_ms`, 300000 ms by default, before another metadata call. Reaching the idle fold age permits one earlier attempt; an unsuccessful attempt after that age uses the retry delay.
2. Index: when grep is maintained and the entry is dirty or its indexed seq is older than its published seq. A visit runs up to 16 build steps, then a reorganization step. Unfinished, failed, and cancelled builds leave the entry dirty, even without a published seq. An index change during a build also remains dirty.
3. Collection: core GC and, when maintained, grep GC, when the published seq moved since the last successful collection and `gc_interval_ms`, 3600000 ms by default, has elapsed. A newly held entry starts this clock at open.
4. Close: when the entry is Quiet, its last open is more than `idle_session_close_after_ms`, 1800000 ms by default, ago, and no caller holds its handle. Seq and exclusive ownership are checked together under the table lock. Opens wait through the drain and get a fresh session. They do not receive `writer_session_closed` from this close path.

The closing entry holds a shared close future. Only opens of that namespace wait for its drain. Opens of other namespaces continue. Table reads and updates keep a short synchronous lock, released before the drain. A cancelled close leaves the shared future held so the next open can finish it. Close completion removes only its original entry. A later session's first publish acquires a new writer epoch. Each held session retains its publisher, engine state, head position, and allocated queue capacities. Closing releases these after admitted work ends; shared caches keep their own limits.

`Sweep::run_pass` keeps the listing pass. It runs at start and every `full_sweep_interval_ms`, 86400000 ms by default, alongside ticks. It visits every listed namespace, runs metadata and index maintenance, and collects garbage. This covers forks, deleted namespaces, work left by a crashed process, and garbage still inside its grace window. A namespace nobody writes to through this host receives no tick visits.

Both paths share `max_concurrent_maintenance` visit slots and a per-namespace visiting set. They never visit the same namespace at the same time. A busy namespace is left to its current visit. A full pass lists up to 1000 ids per page and requests the next page when a visit slot in that pass is free and no listed id is waiting. A listing failure starts no more visits; running visits finish before the pass returns the error. The next daily pass lists again.

The tick and daily pass have independent loops. Streaming compaction holds one visit slot until it finishes. Other free slots can continue work. All maintenance from one runtime shares its compactor claim and execution budget. Shutdown cancels both loops, stops new visits, and lets streaming compaction stop at its next block, bounded by the server's shutdown deadline.

## Shared read working memory

`ExecutionBudget` owns `max_read_working_bytes`, defaulting to 256 MiB. All
read-only and writable runtimes given clones of a budget share this pool.
A runtime without an explicit budget creates a private one. Core callers
without a runtime budget use an unshared 64 MiB pool. Metadata cache limits
are unchanged and have a separate owner.

| Limit | Owner | Default | At the limit |
| --- | --- | --- | --- |
| `max_read_working_bytes` | `ExecutionBudget` | 256 MiB | A memo evicts its own entries and continues without retaining blocks that do not fit. Nothing waits. |

Read, publication, fold, pin-fold, and bounded compaction memos reserve from
this pool. Reservations include decoded data blocks, stored read-ahead
bytes, decoded indexes and filters, keys, reference counts, and allocated
memo container capacity. Eviction and drop release the reservation. A
caller that already holds a block keeps it valid after eviction. Cache
entries are charged to their own limits even when they share an allocation
with a memo. Allocator bookkeeping, temporary decoding, returned rows, and
blocks borrowed after eviction are outside this pool. The pool does not
limit read concurrency.

`ExecutionBudget::stats().read_working_bytes` reports retained bytes.
The budget's recorder exposes `loonfs.execution_budget.read_working_bytes`
and `loonfs.execution_budget.read_working_reservation_failures`. The second
gauge counts failed attempts to reserve, including retries after eviction.
The reference server sets the pool through `max_read_working_bytes`; zero
disables memo retention.

## Store request costs

A Quiet session costs zero store requests across ticks. An idle close also costs zero. An unfinished metadata visit that finds another process holding the compactor costs 6 requests per metadata retry interval, rather than per tick. A waiting tail is retried on that interval and when it first reaches the idle fold age.

A daily pass costs one namespace listing per page plus the following requests for a namespace with nothing to fold, compact, or collect:

| Pass | Grep not maintained | Grep maintained, index not enabled | Grep index enabled |
| --- | --- | --- | --- |
| Does not collect | 6 GET or HEAD | 7 GET or HEAD | 26 GET or HEAD |
| Collects | 12 GET or HEAD and 7 LIST | 20 GET or HEAD and 9 LIST | 38 GET or HEAD and 9 LIST |

Objects still inside their grace windows and due work add requests. Deleted namespaces stay listed because their tombstone manifests remain durable.

## Recovery after a restart

Each kind of work starts again from durable state:

| Work | Durable basis after restart |
| --- | --- |
| WAL fold | Current manifest and the numbered WAL tail. |
| Metadata compaction | Current manifest, selected input descriptors, and a new runtime epoch claim. |
| Core garbage collection | Fresh current-manifest discovery and a complete pin listing. |
| Grep indexing | Current grep manifest and its build or reorganization cursor. |
| Grep garbage collection | Fresh grep-manifest discovery and complete candidate listings. |

A grep build has durable progress in its manifest. A core compaction or GC pass does not have an equivalent durable cursor, so the next attempt repeats it from the start.

## Compaction after a worker stops

Compaction publishes new segment objects only after the selected merge completes and validates. If the process stops first, readers continue using the previous manifest. A replacement runtime plans from the current file set, claims a newer compactor epoch when eligible work requires it, and repeats the merge rather than resuming partially written segments. Unreferenced output follows the ordinary segment age rule. [Streaming compaction](metadata-streaming-compaction.md#publication-and-restart) describes the epoch, the process-local concurrency limit, and restart in more detail.

## Garbage collection after a worker stops

Every core GC call loads fresh roots, builds its live set in memory, and completes each candidate-family listing from the beginning. It writes no durable run object, mark pages, or progress cursor. An interrupted pass may have deleted some eligible objects; another call repeats the listings and remaining cleanup safely.

Each call uses one fixed clock for age and retirement decisions. Pin keys identify the manifests that must be retained for that pass. Upload records preserve cleanup evidence until content and provider state have been handled. A retirement tombstone preserves the source-pin identity so a failed dependency release can be retried.

Grep GC likewise completes one explicit pass without a cursor. It owns only its extension prefix; core GC does not collect those objects.

## Scheduling and correctness

A pass recovers opportunities to run work. It does not replace reference validation, publication time bounds, epoch checks, or conditional writes. No filesystem state is reconstructed from a schedule, and losing a pass can delay work but cannot change committed filesystem state.

Writer assignment and handoff are separate from maintenance. A host must not treat running maintenance on a namespace as permission to silently reacquire a fenced semantic writer.

See [streaming compaction](metadata-streaming-compaction.md) for execution and resource bounds, and the [storage format](../specs/format.md) for publication, collection, and retirement rules.
