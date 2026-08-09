# Streaming Compaction for Large Metadata Family Groups

Status: accepted

## Summary

LoonFS stores namespace metadata in immutable runs. The oldest data is stored in a base run, and checkpoints add newer delta runs. Routine maintenance merges complete runs within a fixed per-step budget, but a family group can eventually become larger than that budget. Once this happens, ordinary maintenance can reduce the number of delta runs but cannot rebuild the base run or apply retention across the complete group.

This design adds a background streaming compaction for oversized metadata family groups. The job reads a fixed snapshot of the selected runs and captures a retention floor, which is the sequence number below which obsolete history may be removed. It applies that floor throughout the job, writes output segments under a job-specific prefix, and publishes the result with one manifest update. Readers continue using the existing manifest until that final update succeeds.

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
- Ensure that a group starts compaction even when new delta runs arrive continuously.
- Avoid rebuilding a large base after every small batch of delta runs.
- Allow cancellation without publishing a partial result.
- Prevent garbage collection from deleting output that belongs to an active job.
- Reclaim unreferenced output after failed, cancelled, or superseded jobs.

## Non-goals

- Durable resume after a process restart.
- More than one active metadata compaction per namespace.
- A fixed wall-clock limit for the complete job.
- Changes to the metadata segment format.
- A process-wide limit on the number of namespaces compacting at once.

## Compaction flow

Normal bounded merges remain the preferred path. Streaming compaction is selected when a family group has repeatedly required work while its bottom-anchored merge cannot fit within one maintenance step.

The complete operation has five stages:

1. Capture an immutable compaction specification containing a generated job ID, the family group, selected input runs, output identity, and retention floor.
2. Create the job lease, open sorted iterators over the selected runs, and merge their rows in key order.
3. Apply the retention rules and write completed output segments under `metadata/compactions/{job_id}/tables/`.
4. Reload the current manifest and confirm that every selected input is still present and unchanged.
5. Publish one manifest update that removes the selected inputs and adds the completed output.

The manifest remains unchanged during stages 1 through 4. A crash, cancellation, or validation failure leaves the existing metadata state intact. A later maintenance pass may start the job again.

```text
fixed input runs -> sorted iterators -> retention -> staged output segments

current manifest + verified inputs + staged output -> one manifest publication
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

## Planning and scheduling

Planning produces either a bounded merge or a `MetadataCompactionSpec`. The compaction specification records the job ID, family group, exact input descriptors, output identity, and retention floor. These values do not change during the job.

A group with an oversized base may still have delta runs that fit within a bounded merge. Always choosing that merge can prevent full compaction when writes continuously create more delta runs. Starting full compaction as soon as the base exceeds the budget would cause the opposite problem: the complete base would be reread for every small batch of delta runs.

The maintenance runner balances these cases by recording how many maintenance engagements planned work for a family group while its bottom-anchored merge was blocked. The first two engagements may use bounded delta merges. After two such engagements, planning selects full compaction even when another delta merge is available.

The count is stored in process memory for each namespace and family group. It is cleared after successful full compaction or after a bottom-anchored merge becomes possible. A restart loses the count, so at most two more engagements are required before full compaction is selected again.

The maintenance runner also keeps one active compaction slot per namespace. While a job is active, its family group is excluded from bounded merges. Maintenance may continue processing unrelated groups in the same namespace.

Runs published after the snapshot was captured are not part of the compaction input. Final publication preserves those runs.

## Streaming executor

Each family is read through a sorted iterator over its selected runs. A k-way merge selects the next row in family key order. Completed output segments are written as soon as they reach the normal segment target size.

The following resources have explicit limits:

- Decoded input blocks held by each iterator.
- Concurrent object-store fetches.
- The decoded-block cache used by reverse-index point lookups.
- Buffered output rows for each family.
- State held by each retention operator.

Retention is implemented by family-specific streaming operators. Each operator keeps a fixed number of fields and at most one complete row. Memory use therefore does not increase with the total family-group size, one inode's attribute history, or the number of binding generations associated with one name.

## Retention and index consistency

Retention uses the floor captured in the compaction specification. The same floor applies to every input row, even when the namespace advances while the job is running.

Rows are processed as follows:

- Revisions, inodes, and tombstones pass through without removal.
- Receipt rows are evaluated independently against the retention floor.
- Attribute rows arrive newest first for each inode. The operator tracks whether it has retained the newest row at or below the floor and detects repeated revision numbers without retaining the complete history.
- Active-deletion rows are ordered so a removal marker arrives before the row it removes. One flag is enough to remove the pair together.
- Forward binding rows are grouped by binding generation. The operator holds at most one bind row until the matching unbind rows arrive, and it retains one generation identity to validate the parent-and-name slot.
- Reverse child-binding rows use bloom-filtered point lookups against the snapshot's unbind rows because their key order differs from the forward binding table.

Bindings and revisions have secondary indexes that must remain equivalent to their canonical families. The executor computes order-independent digests for the canonical and index rows selected for output. A mismatch fails the job before publication.

The executor reports its peak retained-row count. Resource tests use heavily reused inodes and binding slots to verify that this value remains constant.

## Job leases and garbage collection

Each job owns `metadata/compactions/{job_id}/`. Output segments are stored in its `tables/` subdirectory, and lifecycle ownership is recorded in `lease.json` beside that directory.

The lease records the job ID, namespace ID, owner ID, start time, and most recent heartbeat. It does not contain a cursor, output descriptors, offsets, or progress, so it cannot be used to resume a failed job.

The lease is written before the first output segment and refreshed every five minutes while the job runs. It is also refreshed at the start of every finalization attempt. A lease remains valid for 25 minutes after its last heartbeat, which covers missed heartbeats and the complete manifest-publication budget.

Garbage collection reads at most one lease per job prefix during a pass. A fresh lease keeps the complete prefix regardless of object age. A stale, missing, or invalid lease makes unreferenced objects eligible for collection after a staging grace period derived from the lease expiry and the normal publication grace. Unrecognized keys under the compaction prefix are retained because ownership cannot be established safely.

After publication, the manifest is the durable reference for the output segments. Lease deletion is best effort because an undeleted lease expires without affecting the published data. After cancellation, failure, or supersession, the lease stops receiving heartbeats and the unreferenced output is eventually collected.

## Finalization

Finalization refreshes the lease, then reloads the current root and manifest. Publication is allowed only when every selected input descriptor is still present and unchanged and canonical-family and secondary-index validation succeeded.

The new manifest removes exactly the selected input descriptors, adds the output descriptors, and preserves every newer or unrelated run.

Publication uses the normal manifest compare-and-swap. If an unrelated publication wins the race, finalization refreshes the lease, reloads the manifest, and retries. If any selected input changed, the compaction result is abandoned because it no longer represents the current snapshot.

## Cancellation and recovery

The executor checks a cancellation token during input processing, object-store work, retention processing, and output writing. Graceful shutdown requests cancellation before waiting for background tasks to drain.

Cancellation never publishes a partial result. A process restart does not resume completed segments; maintenance starts a new compaction from the current manifest. This repeats work but does not require durable progress state in the namespace manifest or lease.

## Maintenance results and observability

The maintenance API reports `compaction_started` when a step launches a background job. It reports `compaction_pending` when the group requires streaming compaction but the current step cannot start one, such as when the namespace already has an active job.

Lifecycle logging covers job selection, start, progress, publication, cancellation, abandonment, supersession, and failure. Progress records include the namespace, family group, input-run count, rows processed, output-segment count, peak retention rows, elapsed time, and final outcome.

## Validation

The implementation is validated with the following tests:

- Compare streaming output with a whole-group fold over the same snapshot.
- Confirm that new runs published during execution survive finalization.
- Confirm that changed input causes abandonment without publication.
- Exercise compare-and-swap retries after unrelated manifest updates.
- Cancel jobs during reading, retention, writing, and finalization and verify that readers continue using the original manifest.
- Keep all objects under a fresh job lease live beyond the normal garbage-collection grace period.
- Reclaim staged output after a lease expires or is missing.
- Reject malformed and mismatched leases as job ownership records.
- Exercise continuous delta creation and verify that full compaction starts after the bounded engagement count.
- Process large attribute histories and heavily reused binding slots while holding at most one row in retention state.
- Reject canonical-family and secondary-index mismatches before publication.
- Load a published manifest whose descriptors still use compaction-prefix object keys.

## Deferred work

Durable resume may be added later if measured restart cost justifies the additional state. Resume state would be owned by the compactor and would record completed segment boundaries without changing the reader-visible namespace manifest. The job lease is not resume state and never records progress.

Support for multiple concurrent compactions in one namespace and a process-wide concurrency cap are also deferred. Both require scheduling and resource-sharing rules beyond the current single-job-per-namespace model.
