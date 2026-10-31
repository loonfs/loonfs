# Family backfill

A "backfill" is a base compaction that computes the rows of one row family, the "target", from the rows of other families, the "sources". A backfill lets LoonFS add a family, or change a family's key or rows, without recreating a namespace. LoonFS doesn't implement backfills, and this note records the design so that the format keeps what a backfill depends on.

## Why a family needs a backfill

Every row is computed from one WAL delta or one commit record (rule 1 in [Row families](../specs/format.md#19-row-families)), and garbage collection deletes each WAL object after it's folded (see [Candidate and age rules](../specs/format.md#113-candidate-and-age-rules)). The `commits` family keeps the deltas of the commits from the [retention floor](../specs/glossary.md#retention-floor) on, but a family's rows at the floor usually came from older commits, such as a file's newest revision at or below the floor. So a family that didn't exist, or had another key, when those commits were folded has no source for its rows at the floor except the rows that other families kept.

## Computing the target

A backfill plans from a manifest, its [metadata basis](../specs/glossary.md#metadata-basis), and reads each source's state at the floor and every history row above the floor. It reads no WAL object, so the same basis always produces the same target rows.

The target belongs to the [family group](../specs/glossary.md#family-group) that rule 8 in [Row families](../specs/format.md#19-row-families) assigns. A backfill is a bottom-anchored compaction of the target's group whose window is every run of the group in the basis (see [Compaction windows](../specs/format.md#102-compaction-windows)). Its base run contains the target's rows, computed from the sources, and the rows of the group's other families, merged under their usual rules (see [Row retention during a base compaction](../specs/format.md#103-row-retention-during-a-base-compaction)). The backfill publishes the run in a successor manifest under the [compactor epoch](../specs/glossary.md#compactor-epoch), and the window's runs leave the manifest, like the inputs of any compaction (see [Streaming compaction](../specs/format.md#104-streaming-compaction)).

A target row is computable when every field it needs is in a source row at the same or an earlier position that the basis keeps. A field copied from another entity also has to meet rule 5. These kinds of change are computable:

- A new query index. For example, commits keyed by actor (`committed_by`) and then sequence are computed from the `commits` rows, which record both fields.
- The same rows under a re-spelled key. For example, `revisions` with its counters spelled in hexadecimal is computed from `revisions`, because every key component is a field of the row.
- A copy added to a family's rows. For example, binding rows that also copy the ID of the commit that created the child are computed by joining the binding families with `inodes` on the child's inode ID. That commit ID can't change, and `inodes` keeps an inode's row for as long as the inode exists (rule 6).

In the last two kinds, the target replaces a source. A backfill can't compute history below the floor that no family kept, such as a file's revisions older than its newest revision at or below the floor. It also can't compute a fact that the planner checked at commit time but no row recorded, such as a request's preconditions (see [Preconditions](../specs/format.md#b3-preconditions)).

Why is a target a new family, even when it replaces a source? Until the backfill publishes, the writer writes both shapes, and within one run, a family's segments are in one key order with no repeated key (see [Manifests, runs, and segments](../specs/format.md#71-manifests-runs-and-segments)). A re-keyed shape sorts in another order, and a shape with a copy added repeats the old shape's keys, so neither shape can share the source's family.

## Ordering the writer and the backfill

A release that adds a target adds two manifest versions:

- Under the first version, the target is incomplete, and readers read the sources.
- Under the second version, the target is complete, and readers read the target. A backfill publishes the first manifest of this version, and every successor of that manifest keeps the second version.

Under both versions, every fold and every replay of the WAL tail computes the target's rows from deltas. The release's binaries publish no manifest of an older version, because an older version can't record that folds compute the target. An older binary can't load a manifest of the first version (see [Strict decoding and evolution](../specs/format.md#123-strict-decoding-and-evolution)), so once one manifest has that version, every later fold computes the target.

If the target copies a field that older deltas don't include, such as the commit ID above, the release adds the field to the delta, as rule 1 requires. A fold can't compute target rows from an older binary's WAL objects, and an older writer session writes them until a fence ends the session. So the backfill first acquires the writer epoch, writes a fence, and folds, as a maintainer does to acknowledge the floor (see [Writer ownership](../specs/format.md#61-writer-ownership)). No older binary can claim the epoch after the backfill's claim.

A backfill then:

1. Plans from a basis of the first version. If the release added a delta field, the basis is the manifest that folds the backfill's own fence.
2. Computes every target row at or below the basis head, including the rows that folds already wrote.
3. Writes the group's new base run, with the computed rows in place of the target rows in the window.
4. Publishes the run in a successor manifest of the second version. Folds after the basis computed every target row above the basis head.

Why does the writer start first? In the other order, neither the backfill nor a fold would compute target rows for the commits between the backfill's basis and the writer's start.

Why does the backfill recompute rows that folds already wrote? No manifest records where folds began to compute the target, so the basis head is the one boundary that the backfill knows. Those rows can be in any run of the group, and a key in both the new base run and an excluded run would be a duplicate, so the window includes every run. (A group with more than eight runs first merges its newer runs with delta-only compactions.) A fold and the backfill compute each row from the same facts, so a difference is corruption and fails the backfill.

## Reading the target

A reader checks a manifest's version before it decodes the manifest (see [Envelopes and checksums](../specs/format.md#122-envelopes-and-checksums)), and it reads the sources under the first version and the target under the second. The version records whether the target is complete, because the segment descriptors can't. LoonFS doesn't patch a segment or a manifest in place, so every earlier manifest still lists its own segments, and [pins](../specs/glossary.md#pin) and [forks](../specs/glossary.md#fork) from before the backfill keep reading the sources (see [Reference roots](../specs/format.md#112-reference-roots)).

## Failures

A backfill is abandoned under the same input checks and deadline as any compaction (see [Streaming compaction](../specs/format.md#104-streaming-compaction)). A failed backfill publishes no manifest, and garbage collection deletes its output segments (see [Candidate and age rules](../specs/format.md#113-candidate-and-age-rules)). It keeps no durable progress, so any process can run it again from a basis that step 1 accepts.

A backfill doesn't delete a source's rows. Once the current manifest has the second version, a replaced source leaves the current manifest through an ordinary base compaction, under a manifest version that no longer defines the source. That compaction doesn't wait for pins, because a pinned manifest keeps its own segments.

## Requirements on the format

A backfill depends on four properties of the current format. Every row is computed from one delta or one commit record (rule 1), so a backfill and a fold compute the same rows. Each segment descriptor records its family and encoding (see [Control and manifest payloads](../specs/format.md#a4-control-and-manifest-payloads)), so a manifest lists the families that have rows, and the target can use a row kind of its own. A reader rejects a manifest version or a segment encoding that the reader doesn't support (see [Strict decoding and evolution](../specs/format.md#123-strict-decoding-and-evolution)). Compaction already publishes successor manifests under the compactor epoch. No rule in the current format blocks a backfill.

## Costs

A backfill costs about one base compaction of the target's group, plus one read of each source outside the group, and a sort when the target sorts in another order than its sources. From the first version on, every fold writes the target, and while a target replaces a source, every fold writes both families. Adding a delta field costs one writer takeover in each namespace. Readers pay no extra cost until the backfill publishes, and while a pin keeps a manifest from before the backfill, the namespace stores both shapes.

## Alternatives considered

Recreate the store. A copy of each namespace's current state loses the old store's pins and forks, and the history above the floor, such as earlier revisions, commit receipts, and the trash. The copy also assigns new sequences, which changes every inode ID (see [Namespaces and identity](../specs/format.md#11-namespaces-and-identity)).

Compute the target from the WAL. Keeping WAL objects until the floor passes them would store a second copy of the deltas in `commits`, and it still wouldn't reach the rows at the floor.

Patch rows in place. A segment and its rows are immutable, and a pinned manifest reads the segments that it lists, so a patched segment would change what a read through a pin returns.

Keep both shapes. Readers could use the target for commits after the writer started and the sources for earlier commits. But every read of that fact would need two rules and the commit at which the writer started, which no manifest records.
