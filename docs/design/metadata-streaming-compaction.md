# Streaming compaction for large metadata family groups

Status: approved for implementation. This replaces the partial-fold design
(PRs 543 through 548, closed unmerged). Those PRs proved the correctness
semantics and found two real defects in live soaks; a second-opinion review
then showed the durable-progress architecture cost more than it bought, and
we agreed. This document keeps everything the earlier work proved and
changes where progress lives and how input is bounded.

## The problem, unchanged

Reorganization folds a family group as one unit, and one maintenance step
may decode at most `max_decoded_input_rows_per_step` (131,072) rows. A group
whose bottom-anchored window exceeds that can never fold again: its base is
frozen, run count drifts up, and retention for the group stops, because rows
are only dropped by a fold whose window starts at the group's oldest run.
The revisions group crosses at roughly 65,000 revisions namespace-wide.

## What the abandoned design taught us

The partial-fold stack recorded fold progress in the namespace manifest and
published every bounded slice. That made every manifest producer carry the
state, made GC root hidden outputs, made loading validate a cursor grammar,
created a global one-fold slot with fairness rules, and republished a
growing output-descriptor list every slice — quadratic control-plane work
for the mechanism meant to remove a size ceiling. The root misjudgment:
LoonFS is a single-writer system, so compaction coordination never needed to
be durable, and reader truth never needed to carry maintenance bookkeeping.

Two live-soak findings survive as invariants regardless of architecture:
non-bottom-anchored merge outputs must be delta-tier (base fragments
otherwise accumulate and consolidation becomes untriggerable), and merge
outputs must not be stamped at the manifest head (consecutive maintenance
steps share a head on a quiet namespace, so head-stamped outputs collide).

## The design in one paragraph

A group whose bottom-anchored window fits one step folds exactly as today.
A group whose window does not fit is compacted by one streaming background
job: open iterators over a fixed snapshot of the group's runs, merge them in
scan order, apply the retention rules as streaming operators against a floor
frozen at job start, write output segments to a staging area as they fill,
and publish one manifest transition at the end that replaces the snapshot
runs with the completed output run. Nothing durable records progress. A
crash or cancellation loses work, never correctness: the old manifest stays
valid, the staged outputs stay invisible, and a later maintenance pass runs
the job again. Durable resume can be added later, outside the manifest, at
completed-segment boundaries, if measurements ever show restart waste
matters.

## Semantics preserved from the earlier work (verbatim requirements)

- Readers see the snapshot runs until the single final publication; no
  intermediate state is ever visible or loadable.
- The retention floor is frozen at job start; every row in the job is
  judged against it.
- The snapshot is fixed; runs that arrive during the job stay outside it
  and survive the final publication.
- Retention drops happen only in bottom-anchored work. An unbind and the
  bind it cancels leave together; a removal marker and the listed row it
  repeats leave together; the forward bind table and the child-keyed
  reverse index drop in lockstep, because the loader rejects a run whose
  two counts disagree.
- Canonical families and their secondary indexes stay equivalent, checked
  during the job (in memory; nothing durable).
- A publication race never overwrites a winner; a losing job's outputs stay
  unreferenced and are reclaimed.

## Part 1: the placement invariant (lands first, independent)

A merge output is base-tier if and only if its window starts at the group's
oldest run. A non-bottom-anchored merge emits a delta run at the sequence of
its newest input, standing where its window stood. The planner expresses
this as one type the level and retention eligibility are both derived from:

```
enum MergePlacement {
    Base  { output_seq },   // window bottom-anchored; retention may drop
    Delta { output_seq },   // output_seq = newest input's; no drops
}
```

The loader enforces what the rule makes true: at most one base-tier run per
family group (groups may share one base run identity — they fold at the same
head, and usually do); segments of one family in one run numbered densely
from zero; ordered, non-overlapping key ranges. These reject the exact
states the soak observed.

## Part 2: the streaming executor

The planner returns one of two plans: a bounded merge (the existing path,
window fits the step) or a full compaction spec — the group (a closed enum,
not a vector compared against a table), the snapshot run ids, the output
identity, and the frozen floor. The spec is immutable for the job's life.

The executor streams: a k-way merge over the snapshot's segment iterators in
scan order, feeding retention operators, feeding segment builders that roll
at the existing segment size. Memory is bounded by open blocks, the merge
heap, one group's in-flight retention state, and writer buffers — never by
group size. Rows are never collected into whole-slice vectors, and planning
never materializes remaining-boundary sets.

Retention operators keep the per-group locality the partition work proved,
as stream groupings rather than durable grammar: revisions pass through
(never dropped); receipts decide per row; attributes group by inode and keep
what the rule keeps for that inode; deletions group by their sequence;
forward binds group by parent and slot with their unbinds. The child-keyed
reverse index is the one non-local family: each reverse row at or below the
floor is decided by a bloom-filtered point lookup into the snapshot's unbind
family — zero or one row per binding. Parity between canonical families and
indexes is two in-memory order-independent digests compared before
publication; a mismatch fails the job loudly and publishes nothing.

## Part 3: what the review missed, folded in

**Garbage collection must not eat a running job's outputs.** A job outlives
the GC grace window, and its staged segments are unreferenced objects aged
past grace by write time — the exact class the collector reaps. Staged
outputs therefore live under a distinct staging prefix that the sweep treats
as its own candidate family with a long, derived grace (a bound on job
duration, not the general window). A completed publication moves nothing:
the final manifest references the staged keys, and referenced objects are
live wherever they sit. Orphans from failed jobs age out under the staging
grace. The staging prefix is the only durable addition this design makes,
and nothing loads or validates it.

**Input exclusion moves in-process.** The runner keeps an in-memory registry
of the active job's snapshot runs and never schedules a bounded merge over
them; without this, routine delta merges would invalidate the snapshot and
waste whole jobs at finalization. Single-writer makes this a lookup. One job
runs at a time per namespace — a process-level fact, not a manifest field.

**Cancellation rides the drain.** Graceful shutdown cancels the job before
the 600-second drain expires; the job checks a cancellation token between
blocks. Restarts then waste work only on kill -9 and OOM.

## Part 4: finalization

At completion the job reloads the current root and manifest, verifies every
snapshot run is still present and unchanged, replaces exactly those
descriptors with the output run, preserves everything newer and unrelated,
and publishes with the existing compare-and-swap. An unrelated publication
winning the race means reload and retry; a snapshot run changing means
abandon, and staging reclaims the outputs. The publication budget applies to
this one publication, not to the job.

## Observability

The job reports lifecycle, not slices: selected, running (with rows and
bytes processed so far, surfaced through the existing maintenance logging),
published, superseded, failed, cancelled. The runbook explains that a
restart after a crash re-runs the job and that this is expected.

## Implementation plan

1. `MergePlacement` and the loader invariants, against main, harvesting the
   closed stack's tests (the soak-repro rejection tests port intact).
2. The streaming executor, test-driven, not yet scheduled: the plan split,
   the operators (harvested from the hardening PR's drop-rule split), the
   staging writer, parity, and the equivalence oracle ported from the old
   stack — the walk's outputs and the whole fold's outputs were proven
   row-identical; the streaming job must pass the same oracle.
3. Runner integration: the job task, the input registry, cancellation,
   finalization, the staging family in GC, lifecycle reporting, runbook.
   The end-to-end proof is the soak rig's forced mode, re-run.

## Explicitly deferred

Durable resume (compactor-owned control object, completed-segment
boundaries, only if measured restart waste justifies it); any budget for
total job size (memory is bounded; wall clock is allowed to be long);
multi-job concurrency per namespace.
