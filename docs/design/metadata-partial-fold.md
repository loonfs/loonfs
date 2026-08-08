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
    partition_offset: Option<String>,     // how far into the cursor's partition, when it is being folded in pieces
    canonical_rows_digest: String,        // index-parity digest over the canonical family's written rows
    index_rows_digest: String,            // the same over the secondary index's written rows
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
field becomes a list, and nothing else in this design changes. A second group
that needs a walk while one is in flight waits: it keeps merging its delta
runs the way it did before partial folds existed, warning each time, and its
walk starts once the field is free. Starting a second walk is refused rather
than allowed to replace the state in flight, which would strand every segment
that walk had written and send it back to the front.

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
as many whole partitions as fit the row and byte budgets.

## One partition larger than one step

A partition is not a bound. One directory can hold millions of entries, and
one inode's revision history has no limit either, so a walk that accepted its
first partition whatever it cost would have unbounded step memory, would
overrun its publication window, and would retry the same partition forever.

When planning finds that the cursor's own partition does not fit either
budget, the step folds a bounded piece of it instead. `partition_offset`
records the position inside the partition: the last row key the fold has
written, and by the family that row key belongs to, which families of the
group are already done. The families are folded in their declared order, one
per piece; the piece takes as many whole data blocks of that family as fit
the budgets and always at least one. When no family has a row left in the
partition, the offset clears and the cursor moves on, in the same step.

A piece is decided by rules that read no neighbours. Some of the frozen
floor's rules do read neighbours inside a partition — the active-deletion
rule needs the marker that cancels a listed row, the attribute rule needs the
other revisions of the inode, and the bind rule's writer-invariant check
needs the other binds in the slot — and those do not run over a piece. The
step says so in its outcome. The two groups that realistically grow a huge
partition lose nothing to this: revision rows are never dropped at all, and
for the bindings group the drop rule itself is decided per row by the point
read described below, so a directory too large to fold whole still sheds its
retired bindings.

## Retention during the walk

Drops stay legal because the walk's input is the whole group from its
oldest run — only the output arrives in pieces. Rules are evaluated against
the floor frozen in the progress state, so every step and every resume
decides identically.

The bind drop is the rule that needs care, because the rows it reads do not
always share a slice with the rows it decides:

1. A bind at or below the floor survives exactly when nothing retired it.
   The forward rule also asks whether the bind is the latest in its
   (parent, name) slot, but under the writer invariant — a bind is only ever
   superseded by an operation that also unbinds it — the two questions have
   one answer, and the drop pass refuses to compact state that breaks the
   invariant. So one predicate decides a bind row, and it needs the
   unbindings of that one binding.
2. A slice of whole partitions holds them. Binds and the unbinds that retire
   them share a partition by construction, so the slice's own unbind rows
   are exactly the set its forward binds are decided against — the same
   derivation a whole-group fold does, over the slice's rows.
3. The reverse-index row (`DirentryChildBinds`) is keyed by child, so it
   never shares a partition with the binds it indexes, and neither does a
   piece of a partition being folded one family at a time. Those rows are
   decided by a point read into the snapshot's `DirentryUnbinds` family. A
   reverse row is a bind row carrying the full binding identity, and the
   unbind key grammar leads with exactly that identity, so the read is a
   prefix lookup returning the unbinds of one binding. Only rows at or below
   the frozen floor cost a read — a bind above the floor survives whatever
   retired it later — so a slice makes at most one read per such row, and the
   plan charges the family's rows twice to keep the step inside its row
   budget. The segment bloom filters are keyed by parent and name, so a
   binding no operation ever retired misses them outright.

Both routes run the same two functions over unbind rows from the same
immutable snapshot against the same frozen floor, so the two bind families
drop in lockstep. They must: the format gives every bind row exactly one
reverse row, and manifest load rejects a run whose two counts disagree
(`validate_manifest_table_descriptors`).

An earlier draft built one set of every unbinding at or below the floor,
once, and held it for the whole walk. It is gone. Deriving it rescanned the
whole `DirentryUnbinds` family on every step, including on a young family
where no row enters the set at all; and its over-size fallback could never
recover, because a walk that rewrites without dropping retains every unbind,
so the next walk faces the same set plus whatever arrived meanwhile.

## Index parity during the walk

A whole-group fold verifies on every unit that a secondary index holds the
same rows as its canonical family. A walk cannot do that outright for the
bind pair: no slice ever holds a bind row and the reverse row that indexes
it. Two mechanisms cover the two pairs:

- The revisions pair is co-partitioned, so a slice of whole partitions holds
  both sides and is checked outright, by the same function the whole-group
  fold calls.
- Both pairs additionally carry a running digest each, in
  `canonical_rows_digest` and `index_rows_digest`. Every step folds the rows
  it wrote into them with an order-independent combiner, so the two agree at
  the end exactly when the walk wrote the two families the same rows. The
  completing step requires that before the swap and fails the walk with a
  corruption error naming the group otherwise. A group with no secondary
  index leaves both at the zero digest.

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
- `input_runs` is not empty and names no run twice;
- every output segment passes the checks a run's descriptors pass: object key
  agreeing with its owner and table id, the filter block directly preceding
  the index block, an inline filter whose length matches its handle, and a
  non-empty ascending key range;
- output segments of one family are numbered from zero in the order they were
  written, and their ranges are disjoint and ascending;
- every output range lies below where the fold has written that family, which
  is the cursor's bound normally and, when the fold stands inside a partition,
  the offset for the family it names, the partition's end for the families
  before it, and the partition's start for the families after it;
- output segments carry the declared `output_run_seq` and `output_level`;
- `frozen_floor` is at or below the manifest's current floor;
- the cursor parses as a partition key for the group;
- `partition_offset`, when present, parses as a row key of the cursor's
  partition for one of the group's families;
- both digest fields parse.

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
ordinary maintenance cadence. A step's memory is the budgets and nothing
else: the walk holds no derived set, and the only reads beyond the slice are
the point reads the plan reserves budget for.

## Implementation plan

Four stacked PRs, in order:

1. Format: the progress struct, decode validation, golden fixtures for the
   in-progress shape (existing goldens unchanged), the GC enumeration
   change, and a format.md section.
2. The walk executor: start, step, complete, resume — driven entirely by
   tests, not yet reachable from the selector. This PR carries the
   partition-mapping functions per group, the frozen-floor drop pass, and
   the crash-resume tests.
3. The trigger: selector integration, group exclusivity, observability,
   runbook, and the end-to-end test — a group past the budget completes a
   walk, its base folds, retention resumes, and the #528 warning stops.
4. Hardening: pieces for a partition larger than one step, the point read
   that replaced the frozen unbind set, the index-parity digests, and the
   full descriptor validation for the state's outputs.
