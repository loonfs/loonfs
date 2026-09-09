# Maintenance hosting and recovery

Namespace assignment belongs to the hosting layer. The maintenance runner schedules assigned work, and each job reads durable state to determine what remains. A replacement host can repeat its assignments without recovering the previous process's queue or scheduling hints.

## Assigning work

Hosts assign namespace/job pairs explicitly. The runner can deduplicate pending work, limit concurrency, back off failed jobs, and check assignments periodically. It does not discover every namespace by listing storage.

Metadata probes inspect the WAL and manifest descriptors using the same thresholds and compaction policy as execution. An assigned namespace can be checked again after a restart. Losing an in-memory scheduling hint can delay work, but cannot change committed filesystem state.

| Work | Durable basis after restart |
| --- | --- |
| WAL fold | Current manifest and the numbered WAL tail. |
| Metadata compaction | Current manifest, selected input descriptors, and a new runtime epoch claim. |
| Core garbage collection | Fresh current-manifest discovery and a complete pin listing. |
| Grep indexing | Current grep manifest and its build or reorganization cursor. |
| Grep garbage collection | Fresh grep-manifest discovery and complete candidate listings. |

A grep build has durable progress in its manifest. A core compaction or GC pass does not have an equivalent durable cursor. Scheduling should preserve this distinction rather than assume every job resumes where it stopped.

## Compaction after a worker stops

Compaction writes new segment objects and publishes them only after the selected merge completes and validates. If the process stops first, readers continue using the previous manifest.

A replacement runtime plans from the current file set and claims a newer compactor epoch when eligible work requires it. That claim fences publications from earlier runtimes. The replacement repeats the merge rather than resuming partially written segments.

Unreferenced segments remain protected until their provider age is strictly greater than 24 hours. Streaming publication must begin within the shorter bound that reserves the minimum GC grace inside that day. Cancelled and abandoned output follows the same collection rule.

The built-in `metadata_compaction` job defaults to two concurrent compactions. An embedding application can change this through `MetadataCompactionJob::max_concurrent`. This process-local limit controls resource use; the manifest epoch controls publication authority. Concurrent family groups in one runtime share its epoch claim.

## Garbage collection after a worker stops

Every core GC call loads fresh roots, builds its live set in memory, and completes each candidate-family listing from the beginning. It writes no durable run object, mark pages, or progress cursor. An interrupted pass may have deleted some eligible objects; another call repeats the listings and remaining cleanup safely.

Each call uses one fixed clock for age and retirement decisions. Pin keys identify the manifests that must be retained for that pass. Upload records preserve cleanup evidence until content and provider state have been handled. A retirement tombstone preserves the source-pin identity so a failed dependency release can be retried.

Grep GC likewise completes one explicit pass without a cursor. It owns only its extension prefix; core GC does not collect those objects.

## Scheduling and correctness

Repeated assignments recover opportunities to run work. They do not replace reference validation, publication time bounds, epoch checks, or conditional writes. Conversely, no filesystem state is reconstructed from the runner's queue.

Writer assignment and handoff are separate from maintenance scheduling. A host must not treat maintenance assignment as permission to silently reacquire a fenced semantic writer.

See [streaming compaction](metadata-streaming-compaction.md) for execution and resource bounds, and the [storage format](../specs/format.md) for publication, collection, and retirement rules.
