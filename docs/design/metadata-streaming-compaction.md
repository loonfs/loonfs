# Metadata streaming compaction

LoonFS stores metadata in immutable runs. WAL folds add delta runs; compaction merges selected runs into a more efficient layout. An ordinary maintenance pass limits its input by run count, decoded rows, and decoded bytes. Once an eligible window exceeds those limits, it needs streaming execution.

Both paths use the same merge and retention rules. A streaming job processes the selected window incrementally, writes segments as they fill, and publishes the completed output in one numbered manifest. Readers continue using the earlier file set until that publication succeeds.

The [storage format](../specs/format.md#10-retention-and-compaction) defines the durable requirements. This document explains planning, resource use, and the execution tradeoffs.

## Selecting a window

Compaction works on family groups. Directory binds, the child-binding index, and unbinds form one group because their retention decisions must remain consistent. Other groups contain one family.

For each group, the planner considers the base first and then delta runs from oldest to newest. It selects a contiguous window of at most eight runs. It never steps over an unselected run in the middle of that window.

Under the automatic size-tiered policy, a window is eligible when its oldest run is at most 8 MiB, or the newer runs in that window total at least one quarter of the oldest run's stored bytes. The rule applies to both bases and large deltas. Stored bytes come from the manifest's block handles; decoded bytes separately bound execution within an ordinary step.

If an eligible prefix fits the step budgets, the bounded path executes it. Otherwise a streaming job processes the selected window. An ineligible group does not prevent another group from being selected.

| Policy or bound | Behavior |
| --- | --- |
| `SizeTiered` | Wait for enough newer data before rewriting a large oldest run. |
| `CompactImmediately` | Bypass the size threshold for explicit compaction. |
| Eight-run limit | Applies to bounded and streaming execution, including explicit requests. |
| Ordinary step budgets | At most 131,072 decoded rows and 64 MiB of decoded data-block input. |

A large backlog can require several publications. Explicit compaction performs one window per call. Automatic size-tiering reduces repeated base rewrites, but obsolete metadata can remain until enough newer data accumulates or an operator requests compaction.

## Placement and retention

A merge's location determines both its output tier and whether it can remove rows.

| Selected window | Output | Retention |
| --- | --- | --- |
| Starts at the group's oldest run | Base run stamped at the captured manifest head sequence | May remove rows under the family retention rules. |
| Starts above the oldest run | Delta run stamped at its newest input sequence | Retains all input rows. |

A group has at most one base run. A merge starting at the oldest run replaces the existing base when present. A higher window cannot safely remove rows because an excluded older run may contain the other half of a binding or removal pair.

```text
Before                         After merging the newest two runs

delta 3 ──┐                    new delta (same placement as delta 3)
delta 2 ──┘                    delta 1
delta 1                        base
base
```

A delta-only window needs at least two runs to reduce run count. A lone oldest delta can be promoted to establish a base. Runs outside the selected window remain referenced without being rewritten.

The captured retention floor applies throughout the merge. Revisions, content-publication rows, inodes, and tombstones are retained. Receipts below the floor can be removed. Attributes retain the newest state at or below the floor and all newer states. Cancelled active-deletion pairs can be removed together. Bindings and their child index must apply the same generation-retention decisions. The full rules are in the format specification.

## Reading and writing incrementally

The merge reads sorted iterators over the selected runs and produces rows in family key order. Each iterator advances through one run's segments sequentially. Input fan-in is bounded by eight runs rather than total history.

The output writer closes data blocks near 64 KiB decoded and segments near 8 MiB decoded or 65,536 rows. A final row can exceed a byte target, and one oversized row is never split. Filters and indexes also consume memory, so these targets are not a universal fixed-memory guarantee independent of row size.

Both paths bound decoded input buffering, concurrent fetches, cached blocks, and output buffers. Family-specific retention operators hold a fixed number of fields and at most one complete row. One inode's long attribute history or one name's many binding generations therefore need not be retained as a complete group in memory.

```text
selected immutable runs
          │
          v
sorted iterators → family retention → completed segment objects
                                              │
                                              v
                              validate inputs, epoch, and elapsed time
                                              │
                                              v
                                  publish next numbered manifest
```

Output uses fresh IDs under `namespaces/{namespace_id}/segments/`. Published descriptors reference those objects in place. There is no copy from a staging prefix.

## Resolving the child index

A child-binding row is ordered by child inode, while the unbind retiring that generation is ordered by parent and name. They do not arrive together in one sorted stream. Both execution paths use the same retention rule, but prepare the required evidence differently.

A bounded merge collects below-floor unbound generation identities while scanning the forward binding and unbind rows. The child-index pass consults that set. Its size is bounded by the selected window's row and byte budgets, and it avoids additional object reads for each child row.

A streaming job cannot retain a set that grows with an arbitrarily large window. It instead uses Bloom-filtered point lookups against the captured input, with a bounded decoded-block cache. This can cost additional reads, especially when the relevant unbind blocks exceed the cache and child order differs substantially from parent order.

The distinction preserves each path's resource contract: the ordinary step reads its bounded window without an extra lookup per reverse row, while streaming execution can process a larger window without retaining every generation identity.

## Validating a merge

Every metadata row key identifies one logical row. Input keys must be strictly increasing within the merged family stream. A duplicate is rejected even if retention would otherwise remove it, including a duplicate split across segments or runs.

The parent-and-name binding output and child-binding output must contain equivalent bind rows. The merge compares order-independent digests before publication. This check is separate from duplicate detection: the same duplicate in both families could leave their digests equal.

Manifest validation also requires dense zero-based segment indexes, ordered non-overlapping key ranges within each run's family, unique run numbers below `next_run_no`, and at most one base per group.

## Publication authority and collection

Before its first eligible compaction, a runtime publishes a manifest with an incremented `compactor_epoch` and otherwise unchanged state. Clones and concurrent family groups share that claim. A newer runtime claim fences earlier compactors; a fenced runtime does not automatically claim the role again.

Before every output publication, the job reloads the current manifest and checks:

- Its epoch still matches the manifest.
- Every selected input descriptor is present and unchanged.
- Its monotonic publication budget has not expired.
- Merge and index validation succeeded.

The new manifest replaces only the selected inputs and preserves newer or unrelated runs. Put-if-absent at the next number decides publication. If another publisher wins, the job reloads and retries while its inputs, epoch, and time bound still permit it.

Unreferenced segments are collectable only when their provider age is strictly greater than 24 hours. A streaming job must initiate publication within 23 hours, 39 minutes, and 30 seconds, reserving the minimum GC grace inside that day. The remaining interval covers provider operations, clock error, and scheduling allowance. Bounded merges use the ordinary 15-minute metadata publication budget.

A changed epoch returns `fenced`. Changed inputs or an exceeded streaming bound return `abandoned`. None of these outcomes publishes a partial replacement. Unreferenced output follows the same age rule whether the job failed, was cancelled, or was abandoned.

## Scheduling and restart

A bounded pass reports `compaction_required` when streaming execution is needed. The built-in `metadata_compaction` job defaults to two concurrent runs; an embedding application can change the process-local limit through `MetadataCompactionJob::max_concurrent`.

Shutdown cancels jobs during reads, retention, writing, and finalization. A replacement process plans from the current manifest rather than resuming partial output. The repeated I/O is the cost of keeping core compaction progress out of the durable format.

Retaining progress across process restarts would require a separate resume protocol. It is not implied by the immutable segments left by an interrupted job.
