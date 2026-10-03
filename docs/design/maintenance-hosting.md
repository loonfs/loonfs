# Maintenance hosting and recovery

A writer session folds its own namespace's WAL tail and compacts its metadata. The host sweep finishes work for sessions this process wrote to and periodically visits every namespace in the store. Published seqs decide which sessions to visit. Each visit reads durable state to decide what work is due. A replacement host starts with a full pass.

## The writer

A writable session folds its WAL tail when the tail reaches the fold thresholds, or when a publish is refused at the write stop. After each fold it publishes, it runs `Maintenance::maintain_metadata_while_due` over its namespace: bounded compaction steps while compaction is due, and a streaming compaction when a step requires one. Its folds take fold permits and its merges take compaction permits from the runtime's execution budget. Closing the session or shutting the runtime down cancels the compaction, which stops at its next block.

One call of `maintain_metadata_while_due` publishes at most 16 compaction units, bounded or streaming, and then returns. Both forms return `Result<bool>`. `true` means nothing is due now and nothing becomes due without another commit under the supplied options. The result comes from reads the call already made. The call returns `false` when it stops at the unit or lost-race limit, is fenced or cancelled, leaves a streaming compaction unfinished, or leaves a tail waiting for the idle fold age. The session's next fold starts compaction again. The sweep continues unfinished work even without another commit.

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

## The sweep

The reference server runs the sweep when its `maintenance` mode maintains. One loop serializes session and full passes:

- Every `maintenance_interval_ms`, 300000 ms by default, a session pass walks `Namespaces::held()`. It skips sessions that published nothing. It visits a session when its last published seq differs from the seq recorded at its last caught-up visit.
- Every `gc_interval_ms`, 3600000 ms by default, the session pass also collects garbage for sessions whose seq moved since their last successful collection. Collection has its own recorded seq, so finishing metadata does not suppress collection.
- At start and every `full_sweep_interval_ms`, 86400000 ms (24 hours) by default, a full pass visits every namespace in the store and collects garbage. This covers forks, deleted namespaces, leftovers of a crashed process, and garbage still inside a grace window. A namespace nobody writes to through this process waits for the full pass.

A visit:

1. calls `maintain_metadata_while_due`, which folds a tail whose newest commit is past the idle fold age and compacts while compaction is due, at most 16 units;
2. runs grep build steps while each one publishes, at most 16, and then one reorganize step once the index is up to date, when the server maintains the grep index;
3. on a collection pass, calls `gc` and then grep garbage collection.

The sweep records the seq read before the visit only when metadata and, where maintained, the grep build are caught up. A commit during the visit therefore remains eligible for the next session pass. It records collection separately when core and grep collection succeed. It drops recorded seqs for sessions no longer held. A full pass can record progress for held sessions too.

A session pass closes a caught-up session when its last open is older than `idle_session_close_after_ms`, 1800000 ms (30 minutes) by default, and no request holds a clone. The setting must be positive. A session with unfinished metadata or index work stays open for that pass. Under the handle table lock, the close checks the elapsed monotonic time, the shared session's reference count, and the caught-up published seq. The sweep drops its own handle clones before that check. Closing drops the session's maintenance, collection, and index progress records. A later write opens a fresh session whose first publish acquires a new writer epoch.

Each held session keeps its publisher, commit engine and writer epoch, head position, and the allocated capacities of its publication queue and in-flight map. These remain allocated while the session is idle. Runtime caches and execution limits are shared across sessions. Closing releases the session's retained memory after admitted work ends; it leaves the shared caches under their own limits.

Session and full passes share `max_concurrent_maintenance` visit slots. A full pass lists namespace ids with `loonfs_objectstore::layout::list_namespace_ids`, one page of up to 1,000 ids per list request. The sweep lists the next page when a slot is free and no listed id is waiting, so a slow visit does not delay later pages. A failed call is logged with the namespace id and call name, counted, and left for a later pass. A failed listing starts no new visit. Running visits finish, and the pass ends with the listing error. The next full pass lists again.

Hosts can drive the passes directly through `run_pass`, `run_session_pass`, and `run_index_pass`. Public session and full pass calls are serialized with the scheduled maintenance passes. A full pass delays session passes until it ends. It does not delay the index pass.

A streaming compaction of a very large namespace is a single unit and can run for a long time. It holds one visit slot while it runs, the pass waits for it before it ends, and the next maintenance pass starts late. Other namespaces in the same pass are not held up.

Every value from one runtime shares one compactor claim, so the writer's sessions, the sweep, and explicit maintenance requests never fence one another. The sweep's own limit bounds only how many namespaces it visits at once.

When the server maintains the grep index, a separate loop runs an index pass every five seconds over the writer sessions the server holds. It compares each session's last published seq with the seq it last indexed through. The grep worker also records successful enable and disable changes in a shared set. Each index pass drains that set, forgets those namespaces' recorded seqs, and selects them for a build. Enabling an index through this process therefore starts building it on the next index pass, with or without a prior commit or a held session. After a build catches up, the pass records the held session's seq if it has one. Without a published seq, the namespace is not selected again until it publishes or its lifecycle changes again. A held session with no new commit or lifecycle change costs no store request, including when its index is disabled. The index pass can run while a session or full pass is running. The per-namespace grep claim prevents concurrent builds of the same index. Its build limits and failure handling are unchanged.

## What one pass costs

An idle namespace costs no request between full passes. A session still waiting for an idle fold or unfinished work is visited again. A full pass costs one list request per page of namespaces and a fixed number of requests for each namespace. The reference server's request-counting test pins the numbers for an idle namespace, one with nothing to fold, compact, or collect:

| Pass | Grep not maintained | Grep maintained, index not enabled | Grep index enabled |
| --- | --- | --- | --- |
| Does not collect | 6 GET or HEAD | 7 GET or HEAD | 26 GET or HEAD |
| Collects | 12 GET or HEAD and 7 LIST | 20 GET or HEAD and 9 LIST | 38 GET or HEAD and 9 LIST |

The core metadata visit remains 6 requests, and core collection remains 13 additional requests, for 19 total. At the former five-minute listing cadence, metadata alone cost 1,728 requests per idle namespace per day. Session passes now cost zero for a caught-up idle namespace. Closing an idle session also costs zero store requests; close only drains work already admitted. Each unreferenced object still inside its grace adds a request on a collection pass, and work that is due adds its own requests. A deleted namespace stays listed because its tombstone manifest is never collected; it costs a visit on each full pass.

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
