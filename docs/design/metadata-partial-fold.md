# Partial folds: reorganizing a family group larger than one step

Status: approved for implementation. Follow-up to the liveness fix in #528,
which made an over-budget group survivable and loud. This design makes it
foldable again.

## The problem

Reorganization folds a family group as one unit: a fold reads every row of
every input run and writes the merged result back as one base run.
`max_decoded_input_rows_per_step` (131,072 rows) bounds what one step may
decode, and nothing splits a group, so a group whose base run alone exceeds
the budget can never fold again. After #528 such a group keeps merging its
delta runs among themselves and logs a warning, but three things stay
degraded permanently: the base is frozen, run count drifts up, and retention
for the group stops entirely, because rows are only dropped by a fold whose
input starts at the group's oldest run.

The ceiling is low. The revisions group carries two rows per revision and
never drops them, so it crosses at roughly 65,000 revisions namespace-wide.
The bindings group crosses at roughly 65,000 live files. Raising the budget
buys a single-digit multiple and costs step memory; it is a lever, not a fix.

## The design in one paragraph

A fold that cannot read its input whole becomes a walk. The step that starts
it records, durably in the manifest, the exact input run set (the snapshot),
the identity of the output run, the retention floor frozen at that moment,
and a cursor. Each step merges one bounded slice of the snapshot's keyspace
into fresh output segments and publishes the manifest with the outputs
accumulated so far and the cursor advanced. Readers never consult any of it:
the snapshot runs stay in `runs` and serve reads exactly as before, and the
outputs stay inside the progress state until the completing step atomically
replaces the snapshot runs with the finished output run and clears the
progress. A walk interrupted anywhere resumes from the cursor in the last
published manifest.

The grep index already reorganizes this way (see
`docs/design/content-search-index.md`, "partitioned segment reorganize", and
`GrepReorganizeState`). This design borrows its shape with one deliberate
difference: grep serves snapshot and outputs together, because postings are
add-only and readers that union them see duplicates, never gaps. Metadata
rows must appear exactly once — scans concatenate runs with no
deduplication — so here the outputs are invisible until the swap. That
keeps the read path byte-for-byte unchanged, which is where most of the
risk in the earlier sketch lived.

## Durable state

`NamespaceManifestPayload` gains one optional field, skipped when absent, so
every existing manifest encodes byte-identically and no golden fixture
changes:

```
reorganize: Option<MetadataReorganizeProgress>

MetadataReorganizeProgress {
    families: Vec<MetadataTableFamily>,   // must equal one REORGANIZE_FAMILY_GROUPS entry
    input_runs: Vec<MetadataRunId>,       // named {run_seq, level} pairs; every entry must exist in `runs`
    output_run_seq: ChangeSeq,            // fixed at walk start: the head seq of the starting step
    output_level: u32,                    // the base level
    frozen_floor_seq: ChangeSeq,          // the retention floor at walk start
    cursor: String,                       // the next unprocessed partition key (see below)
    output_segments: Vec<MetadataFileRef>,  // outputs so far; each descriptor carries its family
}

The field spellings follow the format layer as built: `MetadataFileRef` is
the durable descriptor type (the doc originally said the core-side grouping
view, which has no durable encoding), input runs are named structs because
tuples encode as bare JSON arrays, and durable ChangeSeq fields carry the
`_seq` suffix.
```

At most one walk exists per manifest. One walk per group is the invariant a
single field gives us for free; if two groups ever need concurrent walks the
field becomes a list, and nothing else in this design changes.

## The cursor is a partition key, not a row key

Each group names a partition key that every one of its families' row keys is
prefixed by, and the walk advances in whole partitions. A step never splits
a partition. This is what keeps the retention drop rules local:

- Revisions group: partition = inode id. `Revisions` and
  `RevisionsByInodeDesc` both prefix on the inode. Revision rows are never
  dropped, so this walk is a pure rewrite.
- Bindings group: partition = parent inode id for `DirentryBinds` and
  `DirentryUnbinds`, whose cancellation pairs share a (parent, name). The
  reverse index `DirentryChildBinds` is keyed by child and does not align;
  its handling is defined below.
- `Inodes`, `Tombstones`, `ActiveDeletions`, `CommitReceipts`,
  `Attributes`: single-family groups. Partition = the family's own leading
  key component (inode id, root inode id, deletion sequence, receipt id,
  inode id). `ActiveDeletions` pairs (a removal marker and the listed row it
  cancels) live in one family by construction — the group comment says so —
  and the partition must keep each pair together; the marker repeats its
  target's deletion sequence, which is the family's leading component, so
  partitioning on that sequence does keep them together. (An earlier draft
  said the partition was the deletion root; the root is the family's
  *second* component, so it is not a prefix of the row keys.) `Attributes`
  supersession is per-inode. `CommitReceipts` drop by horizon, a per-row
  rule that needs no neighbors.

Each family maps a partition boundary to a key bound with a small pure
function per group. The implementation must state, as a tested invariant per
group, that every cancellation-coupled row pair maps to the same partition.

Metadata row keys are globally unique (established during #528), so any
boundary between two partitions is a legal cursor. The cursor stores the
next unprocessed partition key; a step plans its slice by reading index
sections (per-block key ranges and row counts are already durable) and takes
as many whole partitions as fit the row and byte budgets, always at least
one partition even if that partition alone exceeds them — a walk must never
park the way the old selector did, and a single partition is bounded by what
one inode or one directory can accumulate, not by group size.

## Retention during the walk

Drops stay legal because the walk's input is the whole group from its
oldest run — only the output arrives in pieces. Rules are evaluated against
the floor frozen in the progress state, so every step and every resume
decides identically.

Two rules need care:

1. The bind drop consults the set of unbindings at or below the floor.
   Today that set is built from the whole merged row set in memory. The walk
   builds it once at walk start by scanning the snapshot's `DirentryUnbinds`
   family and keeping the at-or-below-floor entries; the scan is charged
   against the starting step's budgets, and the set is re-derived
   identically on resume because the floor is frozen and the snapshot is
   immutable. If the set exceeds a size bound, the walk falls back to
   no-drop mode for this group and says so in its outcome — a pure rewrite
   still unfreezes the base, and the next walk drops with a smaller set.
2. The reverse-index row (`DirentryChildBinds`) for a dropped binding lives
   in a different partition than the forward row, so the slice holding it
   holds neither the parent's other binds nor the parent's unbinds. It is
   decided against the same frozen unbind set rule 1 builds, which is
   already in memory whenever the walk drops at all: a reverse row is a
   bind row, carrying its parent, name, sequence, and delta index, which is
   exactly what the set is keyed by. No extra read is needed, and the two
   families drop in lockstep by construction.

   The forward rule keeps a bind at or below the floor when it is both the
   latest in its slot and not unbound; the set alone settles the reverse
   row because a bind is only ever superseded by an operation that also
   unbinds it, an invariant the drop pass refuses to compact without.

   An earlier draft answered this with point lookups into the snapshot and
   gave them a share of the step's budget, with a fallback that retained
   the *dangling* reverse rows on their own — on the grounds that a reverse
   row whose forward binding is gone is invisible, since visibility
   requires the forward row, the reverse row, and the inode to agree. Two
   things were wrong with it. The read argument holds but the run would not
   publish: the format gives every bind row exactly one reverse row, and
   manifest load rejects a run whose two counts disagree
   (`validate_manifest_table_descriptors`). And the lookups bought nothing
   the frozen set did not already answer, so they are gone along with their
   budget share.

## Interaction rules

- While a group has a walk in progress, that group performs only walk
  steps. No delta merges for it, no second fold. Other groups reorganize
  normally. Delta merges for the group resume after the swap.
- Runs that arrive during the walk stay out of the snapshot and survive it
  untouched, exactly as in grep.
- The output run's seq is fixed at walk start and its level is the base
  level, so at swap time it sits below every run that arrived during the
  walk, in the same position the snapshot occupied.
- The swap is one manifest publication: remove the snapshot runs from
  `runs`, insert the completed output run, clear `reorganize`. Readers go
  from seeing the snapshot to seeing the output in one step; no manifest
  ever shows both to a scan.

## Garbage collection

Progress output segments are referenced only by the progress state, so the
function that enumerates a manifest's referenced objects must include them —
otherwise GC reaps a walk's outputs mid-walk. This covers the current live
set and the reference-anchor manifest from #529 through the same
enumeration. The snapshot runs are still in `runs` and need nothing new. A
walk abandoned by crash resumes rather than leaking: every output segment is
named by the manifest that survives the crash.

## Validation at load

The #525 rule: an invariant a builder maintains is an invariant the loader
checks. A manifest carrying progress is rejected unless:

- `families` equals one `REORGANIZE_FAMILY_GROUPS` entry exactly;
- every `input_runs` entry names a run present in `runs`;
- every output segment carries a non-empty ascending key range (the #525
  check applies as-is), ranges within a family are disjoint and ascending,
  and every range lies strictly below the cursor's bound for that family;
- output segments carry the declared `output_run_seq` and `output_level`;
- `frozen_floor` is at or below the manifest's current floor;
- the cursor parses as a partition key for the group.

## Triggering

The #528 selector already detects the condition: the bottom-anchored window
cannot fit the budgets. Where today it warns and falls back to delta
merges, it will: advance the walk if one exists for the group; start one if
the group has delta-run pressure or an over-budget base; otherwise behave
as today. The #528 warning keeps firing until a walk starts, then the walk
reports progress (partitions done, rows written) in the step outcome, and
the runbook gains a paragraph on reading it.

## Sizing

At the default budget a step handles up to 131,072 rows, so a 10-million-row
group completes in roughly 80 steps — 80 manifest publications on the
ordinary maintenance cadence. Each step's memory is bounded by the budgets
plus the unbind set, which has its own bound and fallback.

## Implementation plan

Three stacked PRs, in order:

1. Format: the progress struct, decode validation, golden fixtures for the
   in-progress shape (existing goldens unchanged), the GC enumeration
   change, and a format.md section.
2. The walk executor: start, step, complete, resume — driven entirely by
   tests, not yet reachable from the selector. This PR carries the
   partition-mapping functions per group, the frozen-floor drop pass, the
   unbind-set derivation with its fallback, and the crash-resume tests.
3. The trigger: selector integration, group exclusivity, observability,
   runbook, and the end-to-end test — a group past the budget completes a
   walk, its base folds, retention resumes, and the #528 warning stops.
