# Streaming Compaction for Large Metadata Family Groups

Status: accepted

## Summary

LoonFS stores namespace metadata in immutable runs. The oldest data is stored in a base run, and flushes add newer delta runs. Routine maintenance merges complete runs within a fixed per-step budget, but a family group can eventually become larger than that budget. Once this happens, ordinary maintenance can reduce the number of delta runs but cannot rebuild the base run or apply retention across the complete group.

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
- Configurable process-wide concurrency. The maintenance runner uses a fixed limit of two concurrent jobs.

## One engine, two orchestrations

Both reorganization paths merge with the same code. A maintenance step runs it synchronously over the window its budgets selected, and a background job runs it over every run a group holds. The merge itself does not know which one is driving it: it reads sorted iterators, applies the retention operators, writes segments, and reports what it wrote. Rows are dropped when, and only when, the merge placement is base-tier, which is the same rule that decides the output level.

The two orchestrations exist because the work has two shapes. A step-contained merge is the frequent small case. It is bounded by the step's input budgets, it publishes inside the step that ran it, and paying for a lease, a staging prefix, a registry entry, and an admission permit on every one of them would be pure overhead. It also preserves the step contract that `ManualOnly` deployments drive: one call does one unit of work and either publishes it or reports that there was nothing to do.

The background job exists because work of unbounded duration cannot be done that way. A job may run for minutes or hours, so its output has to be staged where a lease can speak for it, its concurrency has to be admitted, and its publication has to revalidate the input it read. Those costs buy nothing for a merge that finishes inside its own step.

One thing inside the merge follows the same split, and only one: how a reverse bind row is resolved. The two resource contracts genuinely differ there, and the section on reverse-index resolution below says how. Everything else — iteration, retention, segment writing, index parity, and what the merge reports — is the same code for both.

## Compaction flow

Normal bounded merges remain the preferred path. Streaming compaction is selected when a family group has repeatedly required work while its bottom-anchored merge cannot fit within one maintenance step.

The complete operation has five stages:

1. Capture an immutable compaction specification containing a generated job ID, the family group, selected input runs, output identity, and retention floor.
2. Create the job lease, open sorted iterators over the selected runs, and merge their rows in key order.
3. Apply the retention rules and write completed output segments under `metadata/compactions/{job_id}/segments/`.
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

The maintenance runner balances these cases by recording how many delta merges it has published above each frozen base. It may publish two bounded delta merges for a group. After the second merge, planning selects full compaction even when another delta merge is available.

The count is stored in process memory for each namespace and `MetadataFamilyGroup`. It is cleared after successful full compaction or after a bottom-anchored merge becomes possible. A restart loses the count, so at most two more delta merges are published before full compaction is selected again.

Callers provide a `FrozenBasePolicy` when planning. Writer-owned background maintenance uses `Amortized` with the per-group counters. `FsAdmin::compact_metadata` uses `CompactImmediately` because the caller explicitly requested full compaction. A bounded maintenance step without a background runner also uses `CompactImmediately`, allowing it to report that compaction is required instead of repeatedly publishing delta merges that cannot rebuild the base.

The maintenance runner keeps one active compaction slot per namespace and allows at most two compaction jobs to run in the process. A job reserves its namespace before waiting for a process permit, so bounded maintenance excludes the selected family group while the job is queued or running. Maintenance may continue processing unrelated groups in the same namespace. Shutdown cancels both queued and running jobs.

Runs published after the snapshot was captured are not part of the compaction input. Final publication preserves those runs.

## Streaming executor

This is the engine both paths run. Each family is read through a sorted iterator over its selected runs. A k-way merge selects the next row in family key order. Completed output segments are written as soon as they reach the normal segment target size. A background job writes them under its own prefix; a step-contained merge writes them at ordinary segment keys, because the publication that names them lands in the same step and the ordinary write-time grace already covers them.

The following resources have explicit limits, and they bound both paths:

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
- Reverse child-binding rows are resolved against the same below-floor unbound generations, reached by one of two routes because their key order differs from the forward binding family. The next section says which route and why.

Bindings and revisions have secondary indexes that must remain equivalent to their canonical families. The executor computes order-independent digests for the canonical and index rows selected for output, and a mismatch fails the merge before publication. This is the only index-parity check either path makes. It covers the rows a merge wrote rather than the rows it read, so it states that the two families dropped in lockstep and not only that their inputs matched.

Every metadata row key identifies one row, and nothing downstream re-establishes that: reads concatenate runs rather than deduplicating them, and the segment writer rejects a descending key but not a repeated one. The executor therefore holds the last input row key it saw for each family and requires the next one to be strictly greater. An equal key is namespace corruption and names the family and the key; a smaller key is an internal error against the merge itself. The check runs over the merge's input, so it sees a duplicate that retention would have dropped, one split across two runs, and one split across two segments. It is separate from the digests and catches a different fault: a duplicate present in both families of an index pair leaves both multisets equal, so the digests pass it.

The executor reports its peak retained-row count. Resource tests use heavily reused inodes and binding slots to verify that this value remains constant.

## Reverse-index resolution

A reverse child-binding row is keyed by child while the unbind that retires its binding is keyed by parent, so no grouping of the merged stream holds the two together. Both paths decide such a row with the same rule against the same set of below-floor unbound generations. They build that set differently, because their resource contracts differ.

A background job reads the unbinds of one binding out of its snapshot, one bloom-filtered point lookup per reverse row at or below the floor, behind a bounded decoded-block cache. A job has no bound on the group it rebuilds, so it must not hold a set that grows with that group.

A merge inside a maintenance step collects the below-floor unbound generations while the forward binding cluster streams the unbind family, and the reverse cluster consults that set. The set holds one generation identity per below-floor unbind row in the window, so it is capped by the same row and decoded-byte budgets that capped the window, and it costs no reads at all.

The step does not use point lookups because their cost is not bounded by those budgets. The lookups are one per reverse row, and each becomes a separate round trip once the cache can no longer hold the window's unbind family. The step's row budget admits about 43,000 unbind rows in a bindings window, so that family reaches the 16 MiB cache at roughly 380 bytes a row, which is an ordinary name length; the decoded-byte budget is four times the cache and allows more still.

Measured on a window whose unbind family just fills the cache, with the reverse index walking it out of order, 65,536 reverse rows cost 27,427 data-block reads and 309 MB transferred against 16 MB of priced input. Doubling the family to twice the cache doubled it again: 131,072 reverse rows, 54,819 data-block reads, 707 MB. The cost per reverse row does not settle, because each row is decided on its own.

The same 65,536-row window resolved from the collected set costs 386 data-block reads and 3.7 MB, which is the window read once. The collected set is also the smaller resident structure: one generation identity per below-floor unbind is well under the 16 MiB cache it replaces.

Step budgets therefore price the selected logical input, and a step-contained merge reads exactly that: each selected segment's index once and its data once. Reverse resolution adds no store work to it.

## Job leases and garbage collection

Each job owns `metadata/compactions/{job_id}/`. Output segments are stored in its `segments/` subdirectory, and lifecycle ownership is recorded in `lease.json` beside that directory.

The lease records the job ID, namespace ID, owner ID, status, start time, and most recent heartbeat. It does not contain a cursor, output descriptors, offsets, or progress, so it cannot be used to resume a failed job.

The job creates the lease with `active` status and create-if-absent semantics before writing the first output segment. Every refresh uses compare-and-swap with the ETag returned by the preceding lease write. Refreshes occur every five minutes while the job runs and at the start of every finalization attempt. An active lease remains valid for 25 minutes after its last heartbeat, which covers missed heartbeats and the complete manifest-publication budget.

Garbage collection reads at most one lease per job prefix during a pass. A fresh active lease keeps the complete prefix regardless of object age. For an expired active lease, the collector uses compare-and-swap to change the status to `reaping`. If a concurrent heartbeat wins, the collector retains the prefix. If the collector wins, the job's next heartbeat fails, the job returns a fenced outcome without publishing, and unreferenced objects in the prefix become eligible for collection. The `reaping` status is terminal, so a later pass can continue an interrupted cleanup without repeating the ownership decision.

A missing, invalid, or mismatched lease provides no ownership claim. Unreferenced objects in that prefix become eligible after a staging grace period derived from the lease expiry and the normal publication grace. Unrecognized keys under the compaction prefix are retained because ownership cannot be established safely.

After publication, the job stops heartbeating but leaves its final active lease in place. This protects the output from a collection pass that captured its live references before the manifest update. After the lease expires, a later pass reads the updated manifest, retains the referenced output segments, claims the lease, and deletes the lease after processing the rest of the prefix. Failed, cancelled, abandoned, superseded, and fenced jobs leave unreferenced output that is eventually collected.

## Finalization

Finalization refreshes the lease with compare-and-swap, then reloads the current root and manifest. A failed lease refresh means garbage collection has claimed the prefix, so finalization returns a fenced outcome without publishing. Publication is otherwise allowed only when every selected input descriptor is still present and unchanged and canonical-family and secondary-index validation succeeded.

The new manifest removes exactly the selected input descriptors, adds the output descriptors, and preserves every newer or unrelated run.

Publication uses the normal manifest compare-and-swap. If an unrelated publication wins the race, finalization refreshes the lease, reloads the manifest, and retries. If any selected input changed, the compaction result is abandoned because it no longer represents the current snapshot.

## Cancellation and recovery

The executor checks a cancellation token during input processing, object-store work, retention processing, and output writing. Graceful shutdown requests cancellation before waiting for background tasks to drain.

Cancellation and lease fencing never publish a partial result. A process restart does not resume completed segments; maintenance starts a new compaction from the current manifest. This repeats work but does not require durable progress state in the namespace manifest or lease.

## Maintenance results and observability

The maintenance API reports `compaction_started` when a step launches a background job. `compaction_at_capacity` means the job is queued for a process permit, `compaction_running` means the namespace already has a queued or running job, and `compaction_required` means the current handle has no background runner and an operator must call `FsAdmin::compact_metadata`.

An explicit `FsAdmin::compact_metadata` call reports `NoWork`, `BoundedMergePublished`, `AlreadyRunning`, or `Ran`. The separate no-work and bounded-merge outcomes tell callers whether the method changed the manifest without starting a full compaction.

Lifecycle logging covers job selection, start, progress, publication, cancellation, abandonment, supersession, and failure. Progress records include the namespace, family group, input-run count, rows processed, output-segment count, peak retention rows, elapsed time, and final outcome.

## Validation

The implementation is validated with the following tests:

- Compare a background job's output with a synchronous merge in a maintenance step over the same snapshot. Both run the same engine, so this test guards the orchestration split rather than two merge implementations.
- Confirm that a step-contained merge holds the same bounded input blocks, fetch width, and retention state the background job holds.
- Confirm that a step-contained merge reads each selected segment's index once and its data once, and makes no reverse-index lookup, while a background job over the same window makes one lookup per reverse row the floor covers.
- Reject a row key repeated in both families of a secondary-index pair, on both paths, without publishing.
- Confirm that new runs published during execution survive finalization.
- Confirm that changed input causes abandonment without publication.
- Exercise compare-and-swap retries after unrelated manifest updates.
- Cancel jobs during reading, retention, writing, and finalization and verify that readers continue using the original manifest.
- Keep all objects under a fresh job lease live beyond the normal garbage-collection grace period.
- Race a job heartbeat against garbage collection's expired-lease claim and verify that exactly one side owns the prefix.
- Fence a job after garbage collection claims its prefix and verify that the job publishes nothing.
- Leave the final lease after publication and verify that an older collection pass cannot remove the published output.
- Reclaim staged output after a lease expires, is already marked `reaping`, or is missing.
- Reject malformed and mismatched leases as job ownership records.
- Exercise continuous delta creation and verify that writer maintenance starts full compaction after two published delta merges.
- Verify that explicit compaction and maintenance without a background runner select full compaction immediately for a frozen base.
- Process large attribute histories and heavily reused binding slots while holding at most one row in retention state.
- Reject canonical-family and secondary-index mismatches before publication.
- Reject a metadata family whose merge input repeats a row key.
- Load a published manifest whose descriptors still use compaction-prefix object keys.

## Deferred work

Durable resume may be added later if measured restart cost justifies the additional state. Resume state would be owned by the compactor and would record completed segment boundaries without changing the reader-visible namespace manifest. The job lease is not resume state and never records progress.

Support for multiple concurrent compactions in one namespace remains deferred. The process-wide concurrency limit is fixed at two rather than exposed as configuration.
