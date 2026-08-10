# Streaming Compaction for Large Metadata Family Groups

Status: accepted

## Summary

LoonFS stores namespace metadata in immutable runs. The oldest data is stored in a base run, and checkpoints add newer delta runs. Routine maintenance merges complete runs within a fixed per-step budget, but a family group can eventually become larger than that budget. Once this happens, ordinary maintenance can reduce the number of delta runs but cannot rebuild the base run or apply retention across the complete group.

This design adds a background streaming compaction for oversized metadata family groups. The job reads a fixed snapshot of the selected runs and captures a retention floor, which is the sequence number below which obsolete history may be removed. It applies that floor throughout the job, writes output segments to a staging prefix, and publishes the result with one manifest update. Readers continue using the existing manifest until that final update succeeds.

## Problem

Metadata reorganization works on family groups because some families must remain consistent with related indexes. For example, bindings must be processed with the child-binding index, and revisions must be processed with the revisions-by-inode index.

Each maintenance step limits the number of runs, decoded rows, and decoded bytes that it may process. The decoded-row limit is currently 131,072 rows. A bottom-anchored merge cannot run when its selected family group exceeds these limits.

Merges above the base can continue reducing delta-run count, but they cannot apply retention safely. Retention decisions may depend on rows stored in the base, so rows can only be removed by a bottom-anchored merge, meaning a merge whose selected input begins with the oldest run in the group. Without another compaction path, the base remains unchanged and obsolete history continues to accumulate.

## Goals

- Compact a metadata family group even when its complete input exceeds a normal maintenance-step budget.
- Keep intermediate output invisible to readers.
- Apply one retention floor consistently across the complete job.
- Preserve runs created after the job starts.
- Bound input buffering, fetch concurrency, decoded-block caching, output buffering, and retention working memory.
- Allow cancellation without publishing a partial result.
- Prevent garbage collection from deleting output that belongs to an active job.
- Reclaim unreferenced output after failed, cancelled, or superseded jobs.

## Non-goals

- Durable resume after a process restart.
- More than one active metadata compaction per namespace.
- A fixed wall-clock limit for the complete job.
- Changes to the metadata segment format.

## Compaction flow

Normal bounded merges remain the preferred path. Streaming compaction is selected only when a family group needs bottom-anchored work and no valid bounded merge can make progress.

The complete operation has five stages:

1. Capture an immutable compaction specification containing the family group, selected input runs, output identity, and retention floor.
2. Open sorted iterators over the selected runs and merge their rows in key order.
3. Apply the retention rules and write completed output segments under the metadata compaction staging prefix.
4. Reload the current manifest and confirm that every selected input is still present and unchanged.
5. Publish one manifest update that removes the selected inputs and adds the completed output.

The manifest remains unchanged during stages 1 through 4. A crash, cancellation, or validation failure leaves the existing metadata state intact. A later maintenance pass may start the job again.

```text
fixed input runs -> sorted iterators -> retention -> staged output segments

current manifest + verified inputs + verified output -> one manifest publication
```

## Merge placement

Output placement depends on the position of the selected merge window:

- A window that begins with the oldest run produces a base-tier run. Retention may remove rows because the input contains the complete retained history for the group.
- A window above the oldest run produces a delta-tier run at the sequence of its newest input. Every input row is preserved because the base may contain related history.

Both cases are represented by `MergePlacement`, which provides the output level, output sequence, and retention eligibility together. Keeping these values in one type prevents invalid combinations.

Manifest validation enforces the resulting layout:

- A family group has at most one base-tier run.
- Segment indexes for one family and run start at zero and contain no gaps or duplicates.
- Segment key ranges are strictly ordered and do not overlap.

## Planning and coordination

Planning produces either a bounded merge or a `MetadataCompactionSpec`. The compaction specification records the family group as an enum value, along with the exact input descriptors, output identity, and retention floor. These values do not change during the job.

The maintenance runner keeps one active compaction slot per namespace. While a job is active, its family group is excluded from bounded merges. Maintenance may continue processing unrelated groups in the same namespace.

Runs published after the snapshot was captured are not part of the compaction input. Final publication preserves those runs.

## Streaming executor

Each family is read through a sorted iterator over its selected runs. A k-way merge selects the next row in family key order. Completed output segments are written as soon as they reach the normal segment target size.

The following resources have explicit limits:

- Decoded input blocks held by each iterator.
- Concurrent object-store fetches.
- The decoded-block cache used by reverse-index point lookups.
- Buffered output rows for each family.
- In-memory rows held for one retention locality.

A retention locality is the set of rows that must be considered together before any of them can be removed. One inode may have a long attribute history, and one parent-and-name slot may be reused many times. The implementation must not collect an unlimited number of locality rows in memory. An operator that needs the complete locality must either process it incrementally or spill after reaching a configured byte limit.

This requirement bounds memory independently of the total family-group size and of the number of revisions associated with one logical key.

## Retention and index consistency

Retention uses the floor captured in the compaction specification. The same floor applies to every input row, even when the namespace advances while the job is running.

Rows are processed according to the locality required by each rule:

- Revisions pass through without removal.
- Receipt rows are evaluated independently.
- Attribute rows are grouped by inode.
- Active-deletion rows are grouped by deletion identity.
- Bind and unbind rows are grouped by parent and name slot so related rows are removed together.
- Reverse child-binding rows use bloom-filtered point lookups against the snapshot's unbind rows because their key order differs from the forward binding table.

Bindings and revisions have secondary indexes that must remain equivalent to their canonical families. The executor computes order-independent digests for the canonical and index rows selected for output. A mismatch fails the job before publication.

## Staging and visibility

Output segments are written under a metadata compaction staging prefix. Their object keys are not referenced by the manifest while the job is running, so readers cannot observe them.

Published descriptors may continue to reference objects under the staging prefix. Metadata loading and descriptor validation must therefore accept valid staged object keys after publication. Moving or copying the objects is not required.

The output must remain protected from garbage collection until finalization succeeds or the job is abandoned. This protection cannot depend only on an estimated job duration because the design does not impose a wall-clock limit. Garbage collection needs an active-job lease or another durable root that covers every staged output object through publication.

After publication, the manifest is the durable root for the output. After cancellation, failure, or supersession, the active-job protection is released and the ordinary staging grace period may be used before deleting the unreferenced objects.

## Finalization

Finalization reloads the current root and manifest. Publication is allowed only when all of the following conditions hold:

- Every selected input descriptor is still present and unchanged.
- Every output object exists and remains protected from garbage collection.
- Canonical-family and secondary-index validation succeeded.
- The retention floor in the result matches the captured specification.

The new manifest removes exactly the selected input descriptors, adds the output descriptors, and preserves every newer or unrelated run.

Publication uses the normal manifest compare-and-swap. If an unrelated publication wins the race, finalization reloads the manifest and retries. If any selected input changed, the compaction result is abandoned because it no longer represents the current snapshot.

## Cancellation and recovery

The executor checks a cancellation token during input processing, object-store work, retention processing, and output writing. Graceful shutdown requests cancellation before waiting for background tasks to drain.

Cancellation never publishes a partial result. A process restart does not resume completed segments; maintenance starts a new compaction from the current manifest. This repeats work but does not require durable progress state in the namespace manifest.

## Maintenance results and observability

The maintenance API reports `compaction_started` when a step launches a background job. It reports `compaction_pending` when the group requires streaming compaction but the current step cannot start one, such as when the namespace already has an active job.

Lifecycle logging covers job selection, start, progress, publication, cancellation, abandonment, supersession, and failure. Progress records include the namespace, family group, input-run count, rows processed, output-segment count, elapsed time, and final outcome.

## Validation

The implementation is validated with the following tests:

- Compare streaming output with a whole-group fold over the same snapshot.
- Confirm that new runs published during execution survive finalization.
- Confirm that changed input causes abandonment without publication.
- Exercise compare-and-swap retries after unrelated manifest updates.
- Cancel jobs during reading, retention, writing, and finalization and verify that readers continue using the original manifest.
- Keep active staged output live beyond the normal garbage-collection grace period.
- Reclaim staged output after failed and cancelled jobs release their protection.
- Force one retention locality beyond the in-memory limit and verify incremental processing or spill behavior.
- Reject canonical-family and secondary-index mismatches before publication.
- Load a published manifest whose descriptors still use staging-prefix object keys.

## Deferred work

Durable resume may be added later if measured restart cost justifies the additional state. Resume state would be owned by the compactor and would record completed segment boundaries without changing the reader-visible namespace manifest.

Support for multiple concurrent compactions in one namespace is also deferred. It would require explicit scheduling and resource-sharing rules beyond the single active-job model described here.
