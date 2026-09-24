# Metadata streaming compaction

LoonFS stores metadata in immutable runs. WAL flushes add delta runs, and compaction merges selected runs into a more efficient layout. An ordinary maintenance step limits its input by run count, decoded rows, and decoded bytes. A selected window that exceeds those limits runs as a streaming job instead.

Both paths use the same merge and retention rules. A streaming job processes the selected window incrementally, writes segments as they fill, and publishes the completed output in one numbered manifest. Readers continue using the earlier file set until that publication succeeds.

The [storage format](../specs/format.md#10-retention-and-compaction) defines window placement, row retention, the compactor epoch, and the publication checks. This note explains planning, resource use, and the execution tradeoffs.

## Selecting a window

Compaction works on family groups ([format Appendix A.6](../specs/format.md#a6-metadata-rows-and-row-keys) lists them). The planner ranks the groups that have delta rows by their delta row count and picks the first group with an eligible window. A group whose only run is a base is not selected; it is rewritten when it next receives a delta run.

Within a group, the planner considers the base first and then delta runs from oldest to newest. It selects a contiguous window of at most eight runs and never steps over an unselected run in the middle of that window.

Under the automatic size-tiered policy, a window is eligible when its oldest run is at most 8 MiB, or when the newer runs in that window total at least one quarter of the oldest run's stored bytes. The rule applies to both bases and large deltas. Stored bytes come from the manifest's block handles; decoded bytes separately bound execution within an ordinary step.

If an eligible prefix fits the step budgets, the bounded path executes it. Otherwise a streaming job processes the selected window.

| Policy or bound | Behavior |
| --- | --- |
| `SizeTiered` | Wait for enough newer data before rewriting a large oldest run. |
| `CompactImmediately` | Bypass the size threshold for explicit compaction. |
| Eight-run limit | Applies to bounded and streaming execution, including explicit requests. |
| Ordinary step budgets | At most 131,072 rows and 64 MiB of decoded data-block input. |

A large backlog can require several publications. Explicit compaction performs one window per call, so a caller repeats it while it publishes. Automatic size-tiering reduces repeated base rewrites, but obsolete metadata can remain until enough newer data accumulates.

A merge that starts at the group's oldest run produces a base run and can remove rows under the format's retention rules. A merge that starts above it produces a delta run and keeps every row, because an excluded older run may contain versions hidden by a tombstone in the selected window. A lone oldest delta can be promoted to establish a base.

## Reading and writing incrementally

The merge reads sorted iterators over the selected runs and produces rows in family key order. Each iterator advances through one run's segments sequentially. Input fan-in is bounded by eight runs rather than by total history.

The output writer closes data blocks near 64 KiB decoded, and segments near 8 MiB decoded or 65,536 rows. A final row can exceed a byte target, and one oversized row is never split. Filters and indexes also consume memory, so these targets are not a fixed-memory guarantee independent of row size.

Both paths bound decoded input buffering, concurrent fetches, cached blocks, and output buffers. Family-specific retention operators hold a fixed number of fields and at most one complete row. One inode's long attribute history, or one name's many binding versions, therefore need not be held in memory as a complete group.

```text
selected immutable runs
          |
          v
sorted iterators -> family retention -> completed segment objects
                                                |
                                                v
                                validate inputs, epoch, and elapsed time
                                                |
                                                v
                                    publish next numbered manifest
```

Output uses fresh IDs under `namespaces/{namespace_id}/segments/`. Published descriptors reference those objects in place. There is no copy from a staging prefix.

## Binding retention

The slot and child indexes each contain bound and unbound versions. A rebuild that includes the oldest run groups rows by slot or child and retains all versions above the floor. It also retains the newest value at or below the floor when that value is bound. An unbound value is a tombstone. Only this rebuild can remove it together with the older values it hides. A rebuild above the oldest run keeps every row.

Binding keys order positions oldest first. The operator holds at most one floor value until the group ends or a row above the floor arrives. Both execution paths read each index as a sorted stream and apply the same rule independently.

## Validating a merge

Every metadata row key identifies one logical row. Input keys must be strictly increasing within the merged family stream. A duplicate is rejected even if retention would otherwise remove it, including a duplicate split across segments or runs.

The parent-and-name binding output and the child-binding output must contain the same binding rows. The merge compares order-independent digests before publication. This check is separate from duplicate detection: the same duplicate in both families could leave their digests equal.

The published manifest must also pass manifest validation ([format section 7.1](../specs/format.md#71-manifests-runs-and-segments)).

## Publication and restart

A runtime claims the compactor epoch before its first eligible compaction, and a newer claim fences it ([format section 10.4](../specs/format.md#104-streaming-compaction)). Clones of one maintenance handle and concurrent family groups share that claim. A fenced runtime does not claim the role again on its own.

Before every output publication, the job reloads the current manifest and checks its epoch, its selected inputs, and its elapsed time. A streaming job makes up to four publication attempts while those checks still pass, and then returns `abandoned`. Bounded merges use the ordinary 15-minute metadata publication budget and do not retry a lost put. Unreferenced output follows the same collection age rule whether the job failed, was cancelled, or was abandoned.

A bounded pass reports `compaction_required` when streaming execution is needed. The built-in `metadata_compaction` job defaults to two concurrent runs; an embedding application can change the process-local limit through `MetadataCompactionJob::max_concurrent`.

Shutdown cancels a streaming job between rows and at finalization, so an in-flight fetch or write finishes first. A replacement process plans from the current manifest rather than resuming partial output. The repeated I/O is the cost of keeping compaction progress out of the durable format.
