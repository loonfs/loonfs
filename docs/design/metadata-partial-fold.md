# Partial metadata folds for large family groups

Status: approved for implementation.

Current maintenance behavior prevents an oversized metadata family group from
blocking all reorganization work, but it cannot reorganize the oversized
group itself. This design divides that work across multiple maintenance
steps.

## Summary

A metadata fold currently reads and rewrites a complete family group in one
maintenance step. The default step limit is 131,072 decoded rows. If the
group's base run exceeds that limit, no later fold can include the base run.

A partial fold divides that work by partition. The manifest records the input
run set, completed output segments, retention floor, and next partition. Each
maintenance step processes one or more complete partitions. The final step
replaces the selected input descriptors with the completed output descriptors
in one manifest publication.

The read path continues to use the original input descriptors until the
partial fold completes. In-progress output descriptors are stored only in the
progress record and are not visible to metadata reads.

## Current limitation

Reorganization operates on a family group because some metadata families must
be compacted together. A normal fold:

1. selects an oldest-first set of complete runs;
2. reads every row for the selected family group;
3. applies retention rules;
4. writes replacement base-level segments; and
5. publishes a manifest that replaces the selected input descriptors.

`max_decoded_input_rows_per_step` limits the decoded input to 131,072 rows by
default. The selection cannot split a run. Once the base run for a family
group exceeds the limit, every selection that includes that base also exceeds
the limit.

With the current fallback behavior, maintenance can still merge newer delta
runs for that group and can reorganize other groups. However, the oversized
base remains unchanged, the number of runs can continue to increase, and
retention rules are not applied to that group. Retention requires a fold that
begins with the oldest run.

This limit is reachable in normal use:

- The revisions group stores two rows per revision and does not discard them.
  It reaches the default limit at about 65,000 revisions in one namespace.
- The bindings group can reach the limit at about 65,000 live files.

Increasing the step limit only delays the same failure and increases memory
usage. It does not remove the dependency on total group size.

## Requirements

The partial-fold implementation must:

- make progress when a family group is larger than one maintenance step;
- preserve the existing metadata read path;
- keep in-progress output invisible until completion;
- resume from the last published manifest after a restart;
- apply retention against one fixed floor for the entire operation;
- preserve metadata created after the operation starts; and
- keep all referenced output objects reachable by garbage collection.

Only one partial fold may be active in a namespace manifest. Concurrent
partial folds are outside the scope of this design.

## Terms

- **Family group:** the set of metadata families that reorganization processes
  together.
- **Input run set:** the run identities selected when the partial fold starts.
  Only descriptors for the selected family group are replaced.
- **Partition:** rows that must be processed in the same step because their
  retention decisions depend on one another.
- **Cursor:** the next partition that has not been processed.
- **In-progress output:** replacement segments already written by completed
  steps but not yet available to metadata reads.

## Operation

### Start

The first step performs the following work:

1. Select a family group and an oldest-first input run set.
2. Record the current retention floor as `frozen_floor_seq`.
3. Assign the output run identity. Its sequence is the manifest head sequence
   at the start of the operation, and its level is the base level.
4. Set the phase to `primary_families` and the cursor to that phase's first
   partition.
5. Process the first bounded range of complete partitions.
6. Publish the progress record and any output segments from this step.

The selected input descriptors remain in `metadata_files`. This keeps them
available to the read path and to later partial-fold steps.

### Advance

Each later step:

1. loads the progress record from the current manifest;
2. selects complete partitions beginning at the cursor;
3. merges the selected rows from the recorded input run set;
4. applies retention using `frozen_floor_seq`;
5. writes new output segments;
6. appends their descriptors to `output_segments`; and
7. publishes a manifest with the updated cursor.

Most family groups have only the `primary_families` phase. The bindings group
uses a second phase because its reverse index has a different key order. After
the parent-keyed binding families are complete, publish a phase transition to
`binding_reverse_index` and reset the cursor to the first child-keyed
partition. The output from both phases remains invisible until the second
phase completes.

A publication conflict is handled by the existing manifest compare-and-swap
rules. Output written by a step that loses the compare-and-swap is
unreferenced and can be reclaimed by garbage collection. A retry loads the
new current manifest before doing more work.

### Complete

After the final partition, one manifest publication:

1. removes descriptors that belong to both the selected family group and the
   recorded input run set;
2. adds all descriptors from `output_segments` to `metadata_files`;
3. retains descriptors for other family groups, including descriptors that
   share an input run identity;
4. retains runs published after the partial fold started; and
5. clears the progress record.

This publication is the visibility point. A metadata read uses either the
complete input set or the complete output set. No published manifest exposes
both sets for the selected family group.

## Durable manifest state

`NamespaceManifestPayload` gains an optional `reorganize` field. The field is
omitted when no partial fold is active, so existing manifests keep their
current encoded form.

```text
reorganize: Option<MetadataReorganizeProgress>

MetadataRunId {
    run_seq: ChangeSeq,
    level: u32,
}

MetadataReorganizePhase {
    PrimaryFamilies,
    BindingReverseIndex,
}

MetadataReorganizeProgress {
    families: Vec<MetadataTableFamily>,
    input_runs: Vec<MetadataRunId>,
    output_run_seq: ChangeSeq,
    output_level: u32,
    frozen_floor_seq: ChangeSeq,
    phase: MetadataReorganizePhase,
    cursor: String,
    output_segments: Vec<MetadataFileRef>,
}
```

The fields have the following meanings:

- `families` must exactly match one entry in
  `REORGANIZE_FAMILY_GROUPS`, including order.
- `input_runs` identifies the selected runs by the durable `run_seq` and
  `level` fields. Named fields are used instead of JSON arrays.
- `output_run_seq` and `output_level` are fixed when the operation starts.
- `frozen_floor_seq` is the only retention floor used by the operation.
- `phase` selects the key order currently being processed. The reverse-index
  phase is valid only for the bindings group.
- `cursor` encodes the next partition key in the current phase.
- `output_segments` contains the durable `MetadataFileRef` descriptors written
  by completed steps. Each descriptor includes its family, run sequence, and
  level.

`MetadataFileRef` is used here because it is the durable descriptor type. The
grouped table and run types in `loonfs-core` are derived in-memory views and do
not have durable encodings.

## Partition boundaries

The cursor identifies a partition in the current phase, not an individual
row. A step never splits a partition. This ensures that rows needed for one
retention decision are available in the same step whenever their key order
permits it.

| Phase and families | Partition key | Reason |
| --- | --- | --- |
| Primary: `Revisions`, `RevisionsByInodeDesc` | inode ID | Both indexes use the inode ID as their leading key. Revision rows are rewritten but not discarded. |
| Primary: `DirentryBinds`, `DirentryUnbinds` | parent inode ID | A binding and its matching unbinding share the parent-and-name prefix. |
| Binding reverse index: `DirentryChildBinds` | child inode ID | The reverse index uses a different leading key and is processed after the forward output is complete. |
| Primary: `Inodes` | inode ID | Each inode row is independent for retention. |
| Primary: `Tombstones` | root inode ID | Rows for one tombstone root stay together. |
| Primary: `ActiveDeletions` | deletion root | A listed deletion row and its removal marker stay together. |
| Primary: `CommitReceipts` | receipt ID | Expiration is evaluated per row. |
| Primary: `Attributes` | inode ID | Attribute supersession is evaluated per inode. |

Each phase needs a pure mapping from its partition key to the corresponding
row-key bound for every family in that phase. Tests must prove that every pair
of rows used by a cancellation rule maps to the same primary partition. The
reverse-index phase uses the completed forward output as described below.

Metadata row keys are globally unique. The cursor therefore can use the
boundary between two partitions without identifying a specific row.

### Step planning and limits

Segment index entries provide block key ranges, row counts, and decoded byte
counts. Planning uses this metadata to select as many complete partitions as
fit within the row and byte limits.

At least one complete partition must be selected. If one partition exceeds a
limit by itself, the step processes that partition anyway. The row and byte
limits are therefore soft limits at a partition boundary, not strict memory
limits. This design removes the limit on total family-group size, but it does
not place a strict bound on the size of one inode or directory partition.
Strictly bounding an oversized partition would require a separate design for
splitting retention dependencies across steps.

After a step, every output key range must be before the new cursor bound for
its family. This makes the cursor sufficient to determine which partitions
have completed.

## Retention

The input run set begins with the group's oldest run, so the same retention
rules used by a normal fold remain valid. Every step uses
`frozen_floor_seq`, even if the namespace retention floor advances while the
partial fold is active. Using an older floor is conservative: it can retain
extra rows but cannot remove rows that are still observable.

Most retention rules are local to the partitions in the table above. The
bindings group requires two additional rules.

### Binding and unbinding rows

Dropping a binding requires the set of matching unbindings at or below the
retention floor. A normal fold builds this set from all merged rows in memory.
For a partial fold, derive the set from the snapshot's
`DirentryUnbinds` descriptors and `frozen_floor_seq`.

The scan and the set both count toward configured limits. Use index metadata
to avoid starting a scan that is already known to exceed those limits. Stop
the derivation if the runtime limit is reached. In either case, process the
step in conservative no-drop mode for this family group. No-drop mode still
rewrites the selected partitions and reduces run count; it only postpones
retention for those rows.

The set may be cached during one process lifetime, but it is not part of the
durable state. After a restart, recompute it from the immutable input run set
and the fixed retention floor. A successful recomputation produces the same
set. A no-drop result remains correct because retaining an otherwise
discardable row is safe.

### Reverse binding index

`DirentryChildBinds` is ordered by child inode ID, while its corresponding
forward binding is ordered by parent inode ID. The two rows may therefore be
processed in different steps. After the primary phase completes, the
`binding_reverse_index` phase scans `DirentryChildBinds` by child inode ID.

For each reverse-index row, use a point lookup against the completed
`DirentryBinds` output to check whether the corresponding forward row was
retained. Bloom filters reduce the number of segment reads. Charge these
lookups to the step limits.

If the lookup cost is too high, retain the reverse-index row. A reverse row
without a matching visible forward binding is ignored by visibility checks,
so retaining it consumes storage but does not change query results. A later
fold can remove it.

## Concurrent metadata changes

While progress exists:

- the selected family group performs partial-fold steps only;
- no delta merge or second fold runs for that group;
- other family groups continue to reorganize normally; and
- checkpoint runs published after the start are excluded from `input_runs`.

The output sequence remains fixed at the head sequence recorded at the start,
and the output level remains the base level. Newer runs therefore keep their
existing order above the replacement base descriptors after completion.

## Read behavior

The grep index has a related durable progress type, `GrepReorganizeState`.
Grep queries can read both input and output segments during reorganization
because duplicate postings do not change the result.

Metadata scans concatenate rows and do not deduplicate them. Exposing both
input and output segments would return duplicate metadata rows. For this
reason, metadata reads use only `metadata_files`; they do not inspect
`reorganize.output_segments`. No read-path change is required.

## Crash recovery and garbage collection

Every output descriptor in `reorganize.output_segments` is a live object
reference. The common manifest reference enumerator must include these
descriptors for both the current manifest and the manifest used as the
garbage-collection reference anchor.

The input descriptors remain in `metadata_files` and require no additional
garbage-collection handling.

After a crash:

- output written before a failed manifest publication is unreferenced and can
  be collected; and
- output included in the last successful publication is referenced by the
  progress record, and the next maintenance step resumes at its cursor.

## Manifest validation

Decode-time validation must reject a progress record unless all
construction-time invariants hold:

- `families` exactly matches one `REORGANIZE_FAMILY_GROUPS` entry;
- every `input_runs` entry identifies a run represented in `metadata_files`;
- every output segment has a non-empty, ascending key range;
- output ranges for one family are ordered and do not overlap;
- every output range for the current phase is strictly before that phase's
  cursor bound;
- every output segment has the declared `output_run_seq` and `output_level`;
- `frozen_floor_seq` is less than or equal to the manifest's current
  `retention_floor_seq`;
- `phase` is valid for the selected family group; and
- `cursor` parses as a partition key for the current phase.

The existing validation for segment descriptors still applies.

## Triggering and observability

Maintenance already reports `BudgetExhausted` when no oldest-first input
selection fits within the step limits. The behavior changes as follows:

1. If a progress record exists, advance that partial fold.
2. Otherwise, start a partial fold when the selected group has delta-run
   pressure or an oversized base and a normal fold cannot fit.
3. If neither condition applies, keep the existing selection behavior.

The budget warning continues until a partial fold starts. After that, each
maintenance result reports the completed partition count and number of rows
written. The operator runbook must explain these fields and how to identify
stalled progress.

## Expected number of steps

At the default row limit, 10 million rows require at least 77 steps
(`ceil(10,000,000 / 131,072)`). Partition boundaries, byte limits, retention
work, and the bindings reverse-index phase can increase that number. Each
successful step publishes a new manifest.

Normal step memory remains within the row and byte targets plus the bounded
unbinding set. As described above, one oversized partition may exceed those
targets.

## Implementation plan

Implement this design in three stages:

1. **Format:** add the progress types and decode validation, add a golden
   fixture for an in-progress manifest, include progress outputs in manifest
   reference enumeration, and document the format in `docs/specs/format.md`.
   Existing manifest goldens remain unchanged because the absent field is
   omitted.
2. **Executor:** implement start, advance, completion, and restart behavior
   behind tests but without selector integration. Include partition mapping,
   the bindings phase transition, frozen-floor retention, bounded
   unbinding-set derivation, conservative fallbacks, and crash-recovery tests.
3. **Trigger:** integrate the executor with selection, enforce one active
   operation per group, add maintenance results and runbook guidance, and add
   an end-to-end test. The test must show that an oversized group completes a
   partial fold, replaces its base descriptors, resumes retention, and stops
   producing the budget warning.
