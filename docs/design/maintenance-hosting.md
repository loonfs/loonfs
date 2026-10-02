# Maintenance hosting and recovery

Background work has two actors, and both read durable state to decide what is due. A writer session folds its own namespace's WAL tail and compacts its metadata. A host sweep visits every namespace on a cadence and does the rest. Nothing is scheduled by hints, and no queue or per-namespace schedule lives in memory, so a replacement host recovers by running its next pass.

## The writer

A writable session folds its WAL tail when the tail reaches the fold thresholds, or when a publish is refused at the write stop. After each fold it publishes, it runs `Maintenance::maintain_metadata_while_due` over its namespace: bounded compaction steps while compaction is due, and a streaming compaction when a step requires one. Its folds take fold permits and its merges take compaction permits from the runtime's execution budget. Closing the session or shutting the runtime down cancels the compaction, which stops at its next block.

One call of `maintain_metadata_while_due` publishes at most 16 compaction units, bounded or streaming, and then returns. The session's next fold starts it again. A session that stops folding while compaction is still due leaves the rest to the sweep: its next pass visits the namespace and runs the same call.

## The sweep

The reference server runs the sweep when its `maintenance` mode maintains. A pass lists the namespace ids in the store with `loonfs_objectstore::layout::list_namespace_ids`, one page of up to 1,000 ids per list request, and visits a bounded number of namespaces at once. The ids of every page share one set of visit slots. The sweep lists the next page when a slot is free and no listed id is waiting, so a slow visit holds one slot and does not delay the namespaces on later pages. A visit:

1. calls `maintain_metadata_while_due`, which folds a tail whose newest commit is past the idle fold age and compacts while compaction is due, at most 16 units;
2. runs grep build steps while each one publishes, at most 16, and then one reorganize step once the index is up to date, when the server maintains the grep index;
3. on a collection pass, calls `gc` and then grep garbage collection.

A pass starts on a fixed interval, and a pass collects garbage when the collection interval has passed since the last collection pass that listed every namespace. A failed call is logged with the namespace id and the call name, counted, and left for the next pass. A failed listing starts no new visit. The visits already running finish, and then the pass ends with the listing error. The sweep keeps no state about a namespace between passes and has no backoff.

A streaming compaction of a very large namespace is a single unit and can run for a long time. It holds one visit slot while it runs, the pass waits for it before it ends, and the next pass starts late. Other namespaces in the same pass are not held up.

Every value from one runtime shares one compactor claim, so the writer's sessions, the sweep, and explicit maintenance requests never fence one another. The sweep's own limit bounds only how many namespaces it visits at once.

When the server maintains the grep index, a second pass runs every five seconds over the writer sessions the server holds. It compares each session's last published seq with the seq it last indexed through and runs build steps only when they differ, so a held session that has not committed costs no store request. The sweep and this pass share one set of namespaces being indexed, so they never build one namespace's index at the same time.

## What one pass costs

A pass costs one list request per page of namespaces and a fixed number of requests for each namespace. The reference server's request-counting test pins the numbers for an idle namespace, one with nothing to fold, compact, or collect:

| Pass | Grep not maintained | Grep maintained, index not enabled | Grep index enabled |
| --- | --- | --- | --- |
| Does not collect | 6 GET or HEAD | 7 GET or HEAD | 26 GET or HEAD |
| Collects | 12 GET or HEAD and 7 LIST | 20 GET or HEAD and 9 LIST | 38 GET or HEAD and 9 LIST |

Each unreferenced object still inside its grace adds a request on a collection pass, and work that is due adds its own requests. A deleted namespace stays listed, because its tombstone manifest is never collected, so it costs a visit on every pass. The cadence is the knob: a deployment with many namespaces raises the pass interval and the collection interval.

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
