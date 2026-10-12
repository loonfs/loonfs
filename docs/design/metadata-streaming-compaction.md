# Metadata streaming compaction

LoonFS stores metadata in immutable runs. WAL folds add delta runs, and compaction merges selected runs into a more efficient layout. An ordinary maintenance step limits its input by run count, decoded rows, and decoded bytes. If a selected window exceeds those limits, it runs as a streaming job instead.

Both paths use the same merge and retention rules. A streaming job processes the selected window incrementally, writes each segment as it fills, and publishes the completed output in one numbered manifest. Readers keep using the earlier file set until that publication succeeds.

[Retention and compaction](../specs/format.md#10-retention-and-compaction) in the storage format defines window placement, row retention, the compactor epoch, and the publication checks. This note explains planning, resource use, and the tradeoffs in how a job runs.

## Selecting a window

This section describes how the planner chooses which runs to merge.

Compaction works on family groups, which [Metadata rows and row keys](../specs/format.md#a6-metadata-rows-and-row-keys) lists. The planner ranks the groups that have delta rows by their delta row count, and it picks the first group that has an eligible window. A group whose only run is a base isn't selected. It's rewritten the next time it receives a delta run.

Within a group, the planner considers the base first, and then the delta runs from oldest to newest. It selects a contiguous window of at most eight runs, and it never leaves out a run in the middle of that window.

Under the automatic size-tiered policy, a window is eligible when its oldest run is at most 8 MiB, or when the newer runs in that window total at least a quarter of the oldest run's stored bytes. The rule applies to both bases and large deltas. Stored bytes come from the manifest's block handles. Decoded bytes are a separate limit on how much an ordinary step processes.

If an eligible prefix fits within the step budgets, LoonFS runs it on the bounded path. Otherwise, a streaming job processes the selected window.

| Policy or bound | Behavior |
| --- | --- |
| `SizeTiered` | Waits for enough newer data before it rewrites a large oldest run. |
| `CompactImmediately` | Bypasses the size threshold for explicit compaction. |
| Eight-run limit | Applies to bounded and streaming execution, including explicit requests. |
| Ordinary step budgets | At most 131,072 rows and 64 MiB of decoded data-block input. |

A large backlog can need several publications. Explicit compaction merges one window per call, so a caller repeats the call for as long as it publishes. Automatic size tiering reduces repeated rewrites of the base, but obsolete metadata can stay until enough newer data builds up.

A merge that starts at the group's oldest run produces a base run, and it can remove rows under the format's retention rules. A merge that starts above the oldest run produces a delta run, and it keeps every row. A lone oldest delta can be promoted to become the group's base.

Why can't a merge above the oldest run remove rows? An excluded older run might contain rows that a row in the window supersedes, such as an older binding version below an unbound one, or a listed deletion whose removal row is in the window.

## Reading and writing incrementally

This section describes how a merge reads its input and writes its output incrementally.

The merge reads the selected runs through sorted iterators, and it produces rows in family key order. Each iterator reads one run's segments in order. The input fan-in is limited to eight runs, so it doesn't grow with the total history.

The output writer closes a data block at about 64 KiB decoded, and a segment at about 8 MiB decoded or 65,536 rows. A final row can exceed a byte target, and an oversized row is never split. Filters and indexes also use memory, so these targets don't guarantee a memory limit that's independent of row size.

Both paths limit decoded input buffering, concurrent fetches, cached blocks, and output buffers. Each family's retention operator keeps a constant number of fields and at most one complete row in memory. As a result, one inode's long attribute history, or one name's many binding versions, doesn't have to be kept in memory as a complete group.

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

Output segments are written with new IDs under `namespaces/{namespace_id}/segments/`, and the published descriptors reference those objects where they are. Nothing is copied from a staging prefix.

## Binding retention

This section describes which binding rows a compaction keeps.

The slot index and the child index contain the same edge events (both bound and unbound) in two different orders. An edge is a parent, a name, and a child. A compaction that includes the oldest run groups the rows of both indexes by edge. For each edge, it keeps every version above the floor, and it keeps the newest version at or below the floor if that version is bound.

An unbound version is a tombstone. A compaction that includes the oldest run is the only kind that can remove it, and it removes the older versions that the tombstone hides at the same time. A compaction above the oldest run keeps every row.

Within an edge, binding keys sort by position, oldest first. The retention operator keeps at most one floor version (the newest version at or below the floor) until its edge ends or a row above the floor arrives. It also tracks the last edge it kept bound at the floor. In the slot index, it refuses a second child bound in one slot at the floor, and in the child index, it refuses a second parent bound for one child at the floor. Both execution paths read each index as a separate sorted stream, and they apply the same rule to each one.

## Validating a merge

This section describes the checks a merge runs before it publishes.

Every metadata row key identifies one logical row. Input keys must be strictly increasing within the merged stream of each family. A merge rejects a duplicate even if retention would otherwise remove it, and even if the duplicate is split across segments or runs.

The binding output keyed by parent and name, and the binding output keyed by child, must contain the same binding rows. Before publication, the merge compares order-independent digests of the two outputs. This check is separate from duplicate detection, because the same duplicate in both families could leave their digests equal.

The published manifest must also pass the manifest validation in [Manifests, runs, and segments](../specs/format.md#71-manifests-runs-and-segments).

## Publication and restart

This section describes how a compaction publishes its output, and what happens when a job is fenced, abandoned, or stopped.

A runtime claims the compactor epoch before its first eligible compaction, and a newer claim fences it (see [Streaming compaction](../specs/format.md#104-streaming-compaction)). Every writer session and `Maintenance` value in one runtime shares that claim, and so do concurrent family groups. A fenced runtime forgets its claim, and its next compaction claims the role again. A session's next compaction runs after its next fold, and a host sweep's runs on its next visit.

Before every publication of its output, a job reloads the current manifest and checks the manifest's epoch, the job's selected inputs, and the job's elapsed time. While those checks still pass, a streaming job makes up to four publication attempts, and then it returns `abandoned`. Two deadlines limit those attempts:

- **Job deadline.** The job starts its last attempt within `METADATA_COMPACTION_BUDGET_MS` of its own start. A job past that bound returns `abandoned`.
- **Publication deadline.** Every attempt of the job's finalization runs under one 15-minute metadata publication budget, which starts when finalization starts. An attempt past that budget fails with the publication budget error.

Bounded merges use the ordinary 15-minute metadata publication budget, and they don't retry a lost put. Unreferenced output follows the same collection age rule whether the job failed, was cancelled, or was abandoned.

When streaming execution is needed, a bounded pass reports `metadata_compaction_required`, and `Maintenance::maintain_metadata_while_due` then runs the streaming job. Every merge, bounded or streaming, uses one of the compaction permits in the runtime's execution budget (two by default) while it runs. An embedding application sets the limit with `ExecutionBudgetBuilder::max_concurrent_compactions`, and it can share one budget across runtimes.

Shutdown cancels a streaming job between rows and at finalization, so a fetch or write that's in progress finishes first. A replacement process plans again from the current manifest, instead of resuming the partial output.

Why doesn't a replacement process resume the partial output? Resuming would need a record of compaction progress in the durable format. Planning again keeps that progress out of the format, and the cost is the repeated I/O.
