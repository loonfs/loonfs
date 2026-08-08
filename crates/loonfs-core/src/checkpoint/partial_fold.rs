//! Folding one family group a slice at a time.
//!
//! A whole-group fold reads every row of every input run in one step. A
//! group whose oldest run alone busts the per-step budget can never do that
//! again, so its base is frozen and its retention stops (#528 made that
//! survivable and loud; this makes it foldable). Such a group is folded by a
//! **walk**: the step that starts it records the input runs, the identity of
//! the run it is building, the retention floor it froze, and a cursor into
//! the group's partition keyspace ([`super::partition`]). Each step merges
//! one bounded slice into fresh segments and publishes a manifest carrying
//! the outputs so far and the cursor advanced. The completing step swaps the
//! input runs for the finished run and clears the state, in one publication.
//!
//! Readers never see any of it. The input runs stay in `metadata_files` and
//! serve reads unchanged; the outputs live in the progress state, which no
//! scan consults. Metadata rows must appear exactly once — scans concatenate
//! runs without deduplicating — so a manifest showing both would show every
//! rebuilt row twice.
//!
//! Nothing a step holds is unbounded. A slice covers whole partitions when
//! they fit the budgets, and one bounded piece of a single partition when
//! that partition alone does not — a directory has no size limit, so a
//! partition is not a bound. Drops are decided from the slice's own rows
//! wherever the rows that cancel each other share a partition, and by a point
//! read into the snapshot where they do not (the reverse bind index is keyed
//! by child). No walk keeps a set whose size follows the namespace.
//!
//! The reorganization step reaches this ([`super::reorganize`]): it starts a
//! walk when a group's oldest run no longer fits one step, and advances the
//! walk a manifest carries before it selects anything else for that group.

use super::block_fetch::load_segment_index_for_reorganization;
use super::build::{
    build_manifest_tables_from_rows, debug_assert_manifest_table_segments_do_not_overlap,
    MetadataTableSegmentation,
};
use super::error::ManifestLoadError;
use super::flush::{ensure_metadata_publication_budget, next_manifest_id_after};
use super::load::validate_revision_by_inode_desc_index;
use super::partition::{GroupPartitioning, PartitionCursor, PartitionKey, PartitionOffset};
use super::publish::{
    manifest_write_failure, publish_metadata_root, write_namespace_manifest,
    ManifestPublicationOutcome,
};
use super::reorganize::{
    bind_survives_frozen_floor, drop_rows_below_frozen_floor, unbindings_at_or_below_floor,
    FrozenFloorScope,
};
use super::runs::{
    MetadataLsmPolicy, MetadataRunManifest, CHECKPOINT_BASE_RUN_LEVEL, CHECKPOINT_L0_RUN_LEVEL,
    PARTIAL_FOLD_PROBE_CONCURRENCY, REORGANIZE_FAMILY_GROUPS,
};
use super::scan::VerifiedMetadataTables;
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::timing::MonotonicTimer;
use futures::future::try_join_all;
use loonfs_api::wire::manifest::{
    lookup_keys, MetadataFileRef, MetadataReorganizeProgress, MetadataRow, MetadataRunId,
    MetadataTableFamily, NamespaceManifestEnvelope, NamespaceManifestPayload,
};
use loonfs_api::wire::sst_blocks::{index_blocks_for_key_range, SegmentIndexEntry};
use loonfs_api::{ChangeSeq, ManifestId, ManifestObjectId, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use xxhash_rust::xxh64::xxh64;

/// What one call to [`MetadataFoldWalk::advance`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum MetadataFoldWalkOutcome {
    /// One slice merged, its segments written, and the manifest published
    /// with the cursor advanced.
    SlicePublished(MetadataFoldSliceReport),
    /// The cursor passed the last partition, so this publication removed the
    /// input runs, inserted the finished run, and cleared the state.
    Completed {
        manifest_id: ManifestId,
        output_segments: usize,
        output_rows: u64,
    },
    /// A concurrent publication moved the root while this step ran. Its
    /// segments are unreferenced, garbage collection reclaims them, and the
    /// next step resumes from the manifest that won.
    Superseded,
}

/// What one slice of a walk cost and dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MetadataFoldSliceReport {
    pub(super) manifest_id: ManifestId,
    /// Partitions the slice covered, counted over the rows it decoded. A
    /// slice folding one piece of an oversized partition reports that one
    /// partition.
    pub(super) partitions: u64,
    pub(super) decoded_input_rows: u64,
    pub(super) decoded_input_bytes: u64,
    /// Point reads the slice made into the snapshot's unbind family, one per
    /// bind row at or below the frozen floor whose unbinds sit outside the
    /// slice.
    pub(super) unbind_probes: u64,
    pub(super) output_rows: u64,
    pub(super) drops: MetadataFoldSliceDrops,
}

/// Which of the frozen floor's rules one slice could run.
///
/// Public because a reorganization step reports it: an operator watching a
/// walk needs to tell a step that ran every rule from one that could only
/// run the rules a row answers on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataFoldSliceDrops {
    /// The slice held whole partitions, so every rule the frozen floor
    /// defines ran over it.
    Applied,
    /// The slice held one bounded piece of a partition larger than one step
    /// may read. Rules that decide a row from its neighbours inside its own
    /// partition did not run over it; the rules that decide a row on its own
    /// did.
    PartitionPiece,
}

/// One partial fold in flight, rebuilt from the manifest that carries it.
///
/// Everything here is either durable in [`MetadataReorganizeProgress`] or
/// derived from it and the snapshot the state names. Nothing survives in
/// memory that a resumption cannot rebuild, which is what makes a crash at
/// any step boundary a resume rather than a restart.
pub(super) struct MetadataFoldWalk {
    group: &'static [MetadataTableFamily],
    partitioning: GroupPartitioning,
    progress: MetadataReorganizeProgress,
    /// The snapshot's runs, resolved against the manifest the walk is
    /// standing on. Re-resolved on every resumption from `input_runs`.
    snapshot: Vec<MetadataRunManifest>,
}

impl MetadataFoldWalk {
    /// Starts a walk over `group`, snapshotting the runs it will merge.
    ///
    /// The input must be the group's runs from the oldest one: dropping rows
    /// is only visibility-preserving over a merge that starts at the bottom,
    /// because the row that cancels another is always the newer of the two
    /// (see `ReorganizationInput::starts_at_group_bottom`). A walk builds
    /// its whole output from that input, so the requirement is the same one
    /// and it is checked here rather than assumed.
    pub(super) fn start<S: ObjectStore + ?Sized>(
        tables: &VerifiedMetadataTables<'_, S>,
        group: &'static [MetadataTableFamily],
        snapshot: Vec<MetadataRunManifest>,
        frozen_floor_seq: ChangeSeq,
    ) -> Result<Self> {
        let partitioning = group_partitioning(group)?;
        let payload = &tables.manifest().payload;
        // A manifest carries one walk at a time, so a walk may only start
        // against a manifest carrying none. Starting here would replace the
        // state in flight, and the walk it replaced would lose every segment
        // it had written and start again from the front. The reorganization
        // step waits instead of reaching this; the refusal is what keeps that
        // true for any other caller.
        if let Some(in_flight) = &payload.reorganize {
            return Err(CoreError::Internal(format!(
                "a partial fold of {group:?} was started while the manifest carries a partial \
                 fold of {:?}; a manifest carries one at a time",
                in_flight.families
            )));
        }
        if !snapshot_is_group_bottom_anchored(tables.scan_runs.as_ref(), group, &snapshot) {
            return Err(CoreError::Internal(format!(
                "a partial fold of {group:?} was started on a run subset that is not the group's \
                 oldest runs; its drops would not be visibility-preserving"
            )));
        }
        let progress = MetadataReorganizeProgress {
            families: group.to_vec(),
            input_runs: snapshot
                .iter()
                .map(|run| MetadataRunId {
                    run_seq: run.run_seq,
                    level: run.level,
                })
                .collect(),
            // The output run's identity is fixed here: stamped at the
            // manifest head and at the base level, it lands where the input
            // runs sat, below every run that arrives while the walk runs.
            output_run_seq: payload.head_seq,
            output_level: CHECKPOINT_BASE_RUN_LEVEL,
            frozen_floor_seq,
            cursor: partitioning.spell_cursor(&partitioning.first_cursor()),
            partition_offset: None,
            canonical_rows_digest: spell_row_digest(0),
            index_rows_digest: spell_row_digest(0),
            output_segments: Vec::new(),
        };
        Ok(Self {
            group,
            partitioning,
            progress,
            snapshot,
        })
    }

    /// Rebuilds the walk a manifest carries, or `None` when it carries none.
    pub(super) fn resume_from_manifest<S: ObjectStore + ?Sized>(
        tables: &VerifiedMetadataTables<'_, S>,
    ) -> Result<Option<Self>> {
        let payload = &tables.manifest().payload;
        let Some(progress) = payload.reorganize.clone() else {
            return Ok(None);
        };
        // Load validation already refused a state whose group is not a
        // reorganization family group, so this only turns the stored list
        // back into the static entry the fold is written against.
        let group = REORGANIZE_FAMILY_GROUPS
            .into_iter()
            .find(|candidate| *candidate == progress.families.as_slice())
            .ok_or_else(|| {
                CoreError::NamespaceCorrupt(format!(
                    "a partial fold names families {:?}, which is not a reorganization family group",
                    progress.families
                ))
            })?;
        let partitioning = group_partitioning(group)?;
        let snapshot = resolve_snapshot_runs(tables, &progress)?;
        if !snapshot_is_group_bottom_anchored(tables.scan_runs.as_ref(), group, &snapshot) {
            return Err(CoreError::NamespaceCorrupt(format!(
                "a partial fold of {group:?} is merging a run subset that is no longer the \
                 group's oldest runs"
            )));
        }
        let walk = Self {
            group,
            partitioning,
            progress,
            snapshot,
        };
        // The cursor and the offset are the walk's whole position; a state
        // that cannot say where it stands is refused rather than restarted
        // from the front, which would rewrite partitions the state's outputs
        // already hold.
        let cursor = walk.cursor()?;
        walk.partition_offset(&cursor)?;
        walk.digests()?;
        Ok(Some(walk))
    }

    pub(super) fn group(&self) -> &'static [MetadataTableFamily] {
        self.group
    }

    pub(super) fn progress(&self) -> &MetadataReorganizeProgress {
        &self.progress
    }

    /// The state this walk will publish next, for a test that needs to see a
    /// step decide against a value no honest step would produce.
    #[cfg(test)]
    pub(super) fn progress_mut(&mut self) -> &mut MetadataReorganizeProgress {
        &mut self.progress
    }

    /// Runs one step: either the next slice, or the publication that
    /// finishes the walk.
    ///
    /// `tables` must be the manifest this walk is standing on: the step
    /// publishes its successor, and a root that moved off it in the
    /// meantime supersedes this step rather than overwriting the winner.
    pub(super) async fn advance<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        tables: &VerifiedMetadataTables<'_, S>,
        policy: MetadataLsmPolicy,
        context: &MutationContext,
        timer: &dyn MonotonicTimer,
    ) -> Result<MetadataFoldWalkOutcome> {
        let publication_started_ms = timer.monotonic_now_ms();
        let mut cursor = self.cursor()?;

        // A walk standing inside an oversized partition folds the next piece
        // of it. When nothing of that partition is left, the cursor moves
        // past it here and the step goes on to plan whole partitions from
        // there, so a step is never spent on bookkeeping alone.
        if let Some(offset) = self.partition_offset(&cursor)? {
            let partition = partition_of_cursor(&cursor)?;
            match Box::pin(self.read_partition_piece(tables, &partition, Some(&offset), policy))
                .await?
            {
                Some(slice) => {
                    return Box::pin(self.publish_slice(
                        store,
                        namespace_id,
                        tables,
                        policy,
                        context,
                        timer,
                        publication_started_ms,
                        slice,
                    ))
                    .await;
                }
                None => cursor = self.partitioning.cursor_after(&partition),
            }
        }

        let Some(plan) = Box::pin(self.plan_whole_partitions(tables, &cursor, policy)).await?
        else {
            return Box::pin(self.complete(
                store,
                namespace_id,
                tables,
                context,
                timer,
                publication_started_ms,
            ))
            .await;
        };
        let slice = match plan {
            WholePartitionsPlan::Slice {
                next_cursor,
                planned_bytes,
            } => {
                Box::pin(self.read_whole_partitions(tables, &cursor, next_cursor, planned_bytes))
                    .await?
            }
            WholePartitionsPlan::FirstPartitionTooLarge => {
                let partition = partition_of_cursor(&cursor)?;
                match Box::pin(self.read_partition_piece(tables, &partition, None, policy)).await? {
                    Some(slice) => slice,
                    // The partition the planner priced holds no row a scan
                    // returns. Stepping past it writes nothing and leaves the
                    // walk one partition further on, which is progress.
                    None => SliceRead::empty(self.partitioning.cursor_after(&partition)),
                }
            }
        };
        Box::pin(self.publish_slice(
            store,
            namespace_id,
            tables,
            policy,
            context,
            timer,
            publication_started_ms,
            slice,
        ))
        .await
    }

    /// Where the walk stands, read back from the state it published.
    ///
    /// Both constructors parse the cursor before handing the walk out, so
    /// this only ever fails on a spelling that changed underneath. It fails
    /// rather than starting over: a walk that restarted from the front would
    /// rewrite partitions its own outputs already hold, and every rewritten
    /// row would land in the finished run twice.
    fn cursor(&self) -> Result<PartitionCursor> {
        self.partitioning
            .parse_cursor(&self.progress.cursor)
            .map_err(|why| {
                CoreError::NamespaceCorrupt(format!(
                    "a partial fold carries an unreadable cursor: {why}"
                ))
            })
    }

    /// How far into the cursor's partition the walk stands, when it stands
    /// inside one at all.
    fn partition_offset(&self, cursor: &PartitionCursor) -> Result<Option<PartitionOffset>> {
        let Some(spelled) = &self.progress.partition_offset else {
            return Ok(None);
        };
        self.partitioning
            .parse_partition_offset(self.group, cursor, spelled)
            .map(Some)
            .map_err(|why| {
                CoreError::NamespaceCorrupt(format!(
                    "a partial fold carries an unreadable partition offset: {why}"
                ))
            })
    }

    /// The digests the walk has accumulated over the two families of its
    /// index pair.
    fn digests(&self) -> Result<(u128, u128)> {
        let read = |field: &str, spelled: &str| {
            parse_row_digest(spelled).ok_or_else(|| {
                CoreError::NamespaceCorrupt(format!(
                    "a partial fold carries an unreadable {field} `{spelled}`"
                ))
            })
        };
        Ok((
            read("canonical row digest", &self.progress.canonical_rows_digest)?,
            read("index row digest", &self.progress.index_rows_digest)?,
        ))
    }

    /// Chooses the next slice of whole partitions by reading the snapshot's
    /// index sections.
    ///
    /// A segment's index publishes the last key of every data block and that
    /// block's decoded length, so a plan can be priced without decoding a
    /// row. Per-block row counts are not durable — only the segment's
    /// total — so the row side of the plan is the segment's average spread
    /// over its blocks, and the step charges the rows it actually decodes.
    ///
    /// `None` means no rows of the group remain at or above the cursor, so
    /// the walk is finished and the next publication completes it.
    async fn plan_whole_partitions<S: ObjectStore + ?Sized>(
        &self,
        tables: &VerifiedMetadataTables<'_, S>,
        cursor: &PartitionCursor,
        policy: MetadataLsmPolicy,
    ) -> Result<Option<WholePartitionsPlan>> {
        let mut segments = Vec::new();
        let mut boundaries = BTreeSet::<PartitionKey>::new();
        for family in self.group {
            let lower_bound = self.partitioning.family_lower_bound(*family, cursor);
            for descriptor in self.snapshot_descriptors(*family) {
                let index = self.segment_index(tables, descriptor).await?;
                if index.is_empty() {
                    continue;
                }
                let first = index.partition_point(|entry| entry.last_key < lower_bound);
                if first == index.len() {
                    continue;
                }
                let rows_per_block = descriptor.row_count.div_ceil(index.len() as u64);
                for entry in index[first..].iter() {
                    if let Some(partition) = self
                        .partitioning
                        .partition_of_row_key(*family, &entry.last_key)
                    {
                        boundaries.insert(partition);
                    }
                }
                segments.push(PlannedSegment {
                    family: *family,
                    index,
                    min_key: descriptor.min_key.clone(),
                    lower_bound: lower_bound.clone(),
                    consumed: first,
                    rows_per_block,
                });
            }
        }
        if boundaries.is_empty() {
            return Ok(None);
        }
        // The cursor's own partition is always a candidate stopping place,
        // whether or not a data block happens to end inside it. Without it a
        // slice could only stop where a block ends, and a partition that
        // shares a block with the partitions above it would be priced
        // together with them.
        if let PartitionCursor::At(partition) = cursor {
            boundaries.insert(partition.clone());
        }

        let row_budget =
            u64::try_from(policy.max_decoded_input_rows_per_step.get()).unwrap_or(u64::MAX);
        let byte_budget =
            u64::try_from(policy.max_decoded_input_bytes_per_step.get()).unwrap_or(u64::MAX);
        let mut planned_rows = 0u64;
        let mut planned_bytes = 0u64;
        let mut through = None;
        for boundary in boundaries {
            let next = self.partitioning.cursor_after(&boundary);
            let (added_rows, added_bytes) = segments
                .iter_mut()
                .map(|segment| {
                    let upper_bound = self.partitioning.family_lower_bound(segment.family, &next);
                    let (rows, bytes) = segment.extend_through(&upper_bound);
                    self.charge_with_point_reads(segment.family, rows, bytes)
                })
                .fold((0u64, 0u64), |(rows, bytes), (added_rows, added_bytes)| {
                    (
                        rows.saturating_add(added_rows),
                        bytes.saturating_add(added_bytes),
                    )
                });
            let candidate_rows = planned_rows.saturating_add(added_rows);
            let candidate_bytes = planned_bytes.saturating_add(added_bytes);
            if candidate_rows > row_budget || candidate_bytes > byte_budget {
                // The cursor's own partition busting the budgets is the case
                // a whole-partition slice has no answer for: one directory
                // can hold millions of entries. The caller folds that
                // partition in pieces instead.
                if through.is_none() {
                    return Ok(Some(WholePartitionsPlan::FirstPartitionTooLarge));
                }
                if planned_bytes == 0 {
                    // Nothing at all lies between the cursor and this
                    // partition, so the walk steps straight to it rather than
                    // publishing one empty partition at a time.
                    return Ok(Some(WholePartitionsPlan::Slice {
                        next_cursor: PartitionCursor::At(boundary),
                        planned_bytes: 0,
                    }));
                }
                break;
            }
            planned_rows = candidate_rows;
            planned_bytes = candidate_bytes;
            through = Some(boundary);
        }
        let through = through.expect("a partition over the budgets returns before this point");
        Ok(Some(WholePartitionsPlan::Slice {
            next_cursor: self.partitioning.cursor_after(&through),
            planned_bytes,
        }))
    }

    /// Reads the rows of `[cursor, next_cursor)` out of the snapshot.
    async fn read_whole_partitions<S: ObjectStore + ?Sized>(
        &self,
        tables: &VerifiedMetadataTables<'_, S>,
        cursor: &PartitionCursor,
        next_cursor: PartitionCursor,
        planned_bytes: u64,
    ) -> Result<SliceRead> {
        let mut rows_by_family = BTreeMap::<MetadataTableFamily, Vec<MetadataRow>>::new();
        for family in self.group {
            let lower_bound = self.partitioning.family_lower_bound(*family, cursor);
            let upper_bound = self.partitioning.family_lower_bound(*family, &next_cursor);
            let rows = tables
                .scan_key_range_page_in_runs(
                    &self.snapshot,
                    *family,
                    &lower_bound,
                    Some(&upper_bound),
                    usize::MAX,
                )
                .await
                .map_err(manifest_load_failure)?
                .into_iter()
                .map(|(_, row)| row)
                .collect::<Vec<_>>();
            rows_by_family.insert(*family, rows);
        }
        let decoded_input_rows = rows_by_family
            .values()
            .map(|rows| rows.len() as u64)
            .sum::<u64>();
        let partitions = self.partitions_covered(&rows_by_family);
        Ok(SliceRead {
            rows_by_family,
            decoded_input_rows,
            decoded_input_bytes: planned_bytes,
            partitions,
            next_cursor,
            next_offset: None,
            scope: FrozenFloorScope::WholePartitions,
        })
    }

    /// Reads one bounded piece of `partition`, resuming after `offset` when
    /// the walk already stands inside it.
    ///
    /// The families of the group are folded in their declared order, one per
    /// piece: a piece holds one family's rows over one key range, and the
    /// offset it publishes says both how far that family has got and, by the
    /// family its row key belongs to, which families are already done.
    /// Nothing outside the piece is consulted, which is why the caller marks
    /// the slice [`FrozenFloorScope::PartitionPiece`].
    ///
    /// `None` means no family of the group has a row left in this partition.
    async fn read_partition_piece<S: ObjectStore + ?Sized>(
        &self,
        tables: &VerifiedMetadataTables<'_, S>,
        partition: &PartitionKey,
        offset: Option<&PartitionOffset>,
        policy: MetadataLsmPolicy,
    ) -> Result<Option<SliceRead>> {
        let resumed_family = offset.map(|offset| offset.family);
        let mut reached_resumed_family = resumed_family.is_none();
        for family in self.group {
            if !reached_resumed_family {
                reached_resumed_family = Some(*family) == resumed_family;
                if !reached_resumed_family {
                    continue;
                }
            }
            let lower_bound = match offset {
                Some(offset) if offset.family == *family => offset.resume_bound(),
                _ => self
                    .partitioning
                    .family_lower_bound(*family, &PartitionCursor::At(partition.clone())),
            };
            let partition_end = self.partitioning.family_partition_end(*family, partition);
            let Some(piece) = self
                .plan_partition_piece(tables, *family, &lower_bound, &partition_end, policy)
                .await?
            else {
                continue;
            };
            let rows = tables
                .scan_key_range_page_in_runs(
                    &self.snapshot,
                    *family,
                    &lower_bound,
                    Some(&piece.upper_bound),
                    usize::MAX,
                )
                .await
                .map_err(manifest_load_failure)?;
            let Some((last_key, _)) = rows.last() else {
                continue;
            };
            let next_offset = last_key.clone();
            let rows: Vec<MetadataRow> = rows.into_iter().map(|(_, row)| row).collect();
            let decoded_input_rows = rows.len() as u64;
            return Ok(Some(SliceRead {
                rows_by_family: BTreeMap::from([(*family, rows)]),
                decoded_input_rows,
                decoded_input_bytes: piece.planned_bytes,
                partitions: 1,
                next_cursor: PartitionCursor::At(partition.clone()),
                next_offset: Some(next_offset),
                scope: FrozenFloorScope::PartitionPiece,
            }));
        }
        Ok(None)
    }

    /// Prices the data blocks of one family between `lower_bound` and the
    /// partition's end, and stops at the last block boundary that fits the
    /// budgets.
    ///
    /// A piece always takes at least one block, so a partition no budget can
    /// hold still gets folded. `None` means the family has no block in the
    /// range at all.
    async fn plan_partition_piece<S: ObjectStore + ?Sized>(
        &self,
        tables: &VerifiedMetadataTables<'_, S>,
        family: MetadataTableFamily,
        lower_bound: &str,
        partition_end: &str,
        policy: MetadataLsmPolicy,
    ) -> Result<Option<PartitionPiecePlan>> {
        let mut segments = Vec::new();
        // The bounds a piece may stop at: one past the last key of every data
        // block that ends inside the range, plus the end of the partition.
        // Every one of them is a place where whole blocks have been read.
        let mut candidates = BTreeSet::<String>::new();
        for descriptor in self.snapshot_descriptors(family) {
            let index = self.segment_index(tables, descriptor).await?;
            if index.is_empty() {
                continue;
            }
            let first = index.partition_point(|entry| entry.last_key.as_str() < lower_bound);
            if first == index.len() {
                continue;
            }
            for entry in index[first..].iter() {
                if entry.last_key.as_str() >= partition_end {
                    break;
                }
                candidates.insert(format!("{}\0", entry.last_key));
            }
            let rows_per_block = descriptor.row_count.div_ceil(index.len() as u64);
            segments.push(PlannedSegment {
                family,
                index,
                min_key: descriptor.min_key.clone(),
                lower_bound: lower_bound.to_owned(),
                consumed: first,
                rows_per_block,
            });
        }
        if segments.is_empty() {
            return Ok(None);
        }
        candidates.insert(partition_end.to_owned());

        let row_budget =
            u64::try_from(policy.max_decoded_input_rows_per_step.get()).unwrap_or(u64::MAX);
        let byte_budget =
            u64::try_from(policy.max_decoded_input_bytes_per_step.get()).unwrap_or(u64::MAX);
        let mut planned_rows = 0u64;
        let mut planned_bytes = 0u64;
        let mut upper_bound = None;
        for candidate in candidates {
            let (added_rows, added_bytes) = segments
                .iter_mut()
                .map(|segment| {
                    let (rows, bytes) = segment.extend_through(&candidate);
                    self.charge_with_point_reads(family, rows, bytes)
                })
                .fold((0u64, 0u64), |(rows, bytes), (added_rows, added_bytes)| {
                    (
                        rows.saturating_add(added_rows),
                        bytes.saturating_add(added_bytes),
                    )
                });
            let candidate_rows = planned_rows.saturating_add(added_rows);
            let candidate_bytes = planned_bytes.saturating_add(added_bytes);
            // The first candidate goes in whatever it costs: it is one block
            // set, which is the smallest piece the layout can express, and a
            // piece that took nothing would leave the walk where it stood.
            if upper_bound.is_some()
                && (candidate_rows > row_budget || candidate_bytes > byte_budget)
            {
                break;
            }
            planned_rows = candidate_rows;
            planned_bytes = candidate_bytes;
            upper_bound = Some(candidate);
        }
        Ok(upper_bound.map(|upper_bound| PartitionPiecePlan {
            upper_bound,
            planned_bytes,
        }))
    }

    /// What one family's blocks cost a step, including the point reads the
    /// rows in them will make.
    ///
    /// A bind row at or below the frozen floor whose unbinds sit outside the
    /// slice is decided by a point read into the snapshot's unbind family,
    /// and each such read returns the unbinds of one binding. So a slice
    /// holding N such rows decodes at most N rows more than its own, and
    /// charging the family's rows twice is what keeps the step inside the row
    /// budget it was given. It is also what bounds how many point reads one
    /// step may make, since a slice makes at most one per row.
    fn charge_with_point_reads(
        &self,
        family: MetadataTableFamily,
        rows: u64,
        bytes: u64,
    ) -> (u64, u64) {
        if self.point_reads_decide(family) {
            (rows.saturating_mul(2), bytes)
        } else {
            (rows, bytes)
        }
    }

    /// Whether rows of `family` in a slice of whole partitions are decided by
    /// a point read rather than from the slice.
    fn point_reads_decide(&self, family: MetadataTableFamily) -> bool {
        family == MetadataTableFamily::DirentryChildBinds
            && self.group.contains(&MetadataTableFamily::DirentryUnbinds)
    }

    async fn segment_index<S: ObjectStore + ?Sized>(
        &self,
        tables: &VerifiedMetadataTables<'_, S>,
        descriptor: &MetadataFileRef,
    ) -> Result<Arc<Vec<SegmentIndexEntry>>> {
        load_segment_index_for_reorganization(
            tables.store,
            tables.table_cache,
            &tables.block_memo,
            descriptor,
        )
        .await
        .map_err(manifest_load_failure)
    }

    fn snapshot_descriptors(
        &self,
        family: MetadataTableFamily,
    ) -> impl Iterator<Item = &MetadataFileRef> {
        self.snapshot
            .iter()
            .flat_map(|run| run.tables.iter())
            .filter(move |table| table.family == family)
            .flat_map(|table| table.segments.iter())
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_slice<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        tables: &VerifiedMetadataTables<'_, S>,
        policy: MetadataLsmPolicy,
        context: &MutationContext,
        timer: &dyn MonotonicTimer,
        publication_started_ms: u64,
        slice: SliceRead,
    ) -> Result<MetadataFoldWalkOutcome> {
        let SliceRead {
            mut rows_by_family,
            decoded_input_rows,
            decoded_input_bytes,
            partitions,
            next_cursor,
            next_offset,
            scope,
        } = slice;

        // A group carrying a secondary index must write the index the same
        // rows as its canonical family. Where the two share a partition the
        // slice holds both sides and can be checked outright, exactly as a
        // whole-group fold checks a unit.
        if scope == FrozenFloorScope::WholePartitions
            && self.group.contains(&MetadataTableFamily::Revisions)
        {
            validate_revision_by_inode_desc_index(
                tables.manifest().payload.manifest_object_id.as_ref(),
                rows_by_family
                    .get(&MetadataTableFamily::Revisions)
                    .map_or(&[], Vec::as_slice),
                rows_by_family
                    .get(&MetadataTableFamily::RevisionsByInodeDesc)
                    .map_or(&[], Vec::as_slice),
            )
            .map_err(manifest_load_failure)?;
        }

        let unbind_probes =
            Box::pin(self.apply_frozen_floor(tables, &mut rows_by_family, scope)).await?;
        let output_rows = rows_by_family
            .values()
            .map(|rows| rows.len() as u64)
            .sum::<u64>();

        let mut progress = self.progress.clone();
        if let Some((canonical, index)) = index_pair(self.group) {
            let (mut canonical_digest, mut index_digest) = self.digests()?;
            canonical_digest =
                fold_rows_into_digest(canonical_digest, rows_by_family.get(&canonical))?;
            index_digest = fold_rows_into_digest(index_digest, rows_by_family.get(&index))?;
            progress.canonical_rows_digest = spell_row_digest(canonical_digest);
            progress.index_rows_digest = spell_row_digest(index_digest);
        }

        let run_tables = build_manifest_tables_from_rows(
            store,
            namespace_id,
            self.progress.output_run_seq,
            self.progress.output_level,
            |family| rows_by_family.remove(&family).unwrap_or_default(),
            MetadataTableSegmentation::Base {
                max_rows_per_segment: policy.max_rows_per_segment,
            },
        )
        .await?;
        debug_assert_manifest_table_segments_do_not_overlap(&run_tables);

        for table in run_tables {
            // Segment indexes number a family's segments within one run, and
            // this walk keeps adding to the same run, so each slice's
            // segments continue where the previous slice's left off.
            let already_written = progress
                .output_segments
                .iter()
                .filter(|segment| segment.family == table.family)
                .count() as u32;
            for (offset, mut segment) in table.segments.into_iter().enumerate() {
                segment.segment_index = already_written + offset as u32;
                progress.output_segments.push(segment);
            }
        }
        progress.cursor = self.partitioning.spell_cursor(&next_cursor);
        progress.partition_offset = next_offset;

        let mut payload = successor_payload(namespace_id, &tables.manifest().payload)?;
        // The floor the walk decides drops against must never sit above the
        // floor the manifest promises, so a walk that froze a newer floor
        // carries the manifest up to it.
        if payload.retention_floor_seq < progress.frozen_floor_seq {
            payload.retention_floor_seq = progress.frozen_floor_seq;
        }
        payload.reorganize = Some(progress.clone());
        let manifest_id = payload.manifest_id;

        match self
            .publish_manifest(
                store,
                namespace_id,
                payload,
                &tables.manifest().payload.manifest_object_id,
                context,
                timer,
                publication_started_ms,
            )
            .await?
        {
            Some(()) => {
                self.progress = progress;
                Ok(MetadataFoldWalkOutcome::SlicePublished(
                    MetadataFoldSliceReport {
                        manifest_id,
                        partitions,
                        decoded_input_rows,
                        decoded_input_bytes,
                        unbind_probes,
                        output_rows,
                        drops: match scope {
                            FrozenFloorScope::WholePartitions => MetadataFoldSliceDrops::Applied,
                            FrozenFloorScope::PartitionPiece => {
                                MetadataFoldSliceDrops::PartitionPiece
                            }
                        },
                    },
                ))
            }
            None => Ok(MetadataFoldWalkOutcome::Superseded),
        }
    }

    /// Drops the rows of one slice the frozen floor lets go, and returns how
    /// many point reads that took.
    ///
    /// Bind rows reach one rule by two routes. A slice of whole partitions
    /// holds every unbind that can retire the forward binds it holds, so
    /// those are decided from the slice's own rows. The reverse index is
    /// keyed by child and never shares a partition with the binds it
    /// indexes, and a piece of an oversized partition holds one family only,
    /// so both of those are decided by a point read into the snapshot's
    /// unbind family. Either way the decision runs
    /// [`unbindings_at_or_below_floor`] and [`bind_survives_frozen_floor`]
    /// over unbind rows from the same immutable snapshot against the same
    /// frozen floor, so the two families drop in lockstep — which they must,
    /// because the format gives every bind row exactly one reverse row and a
    /// run whose two counts disagree does not load.
    async fn apply_frozen_floor<S: ObjectStore + ?Sized>(
        &self,
        tables: &VerifiedMetadataTables<'_, S>,
        rows_by_family: &mut BTreeMap<MetadataTableFamily, Vec<MetadataRow>>,
        scope: FrozenFloorScope,
    ) -> Result<u64> {
        let point_read_families: &[MetadataTableFamily] = match scope {
            FrozenFloorScope::WholePartitions => &[MetadataTableFamily::DirentryChildBinds],
            FrozenFloorScope::PartitionPiece => &[
                MetadataTableFamily::DirentryBinds,
                MetadataTableFamily::DirentryChildBinds,
            ],
        };
        let mut point_read_rows = Vec::new();
        let mut unbind_probes = 0;
        for family in point_read_families {
            let Some(rows) = rows_by_family.remove(family) else {
                continue;
            };
            let (kept, probes) = self.retain_binds_by_point_read(tables, rows).await?;
            unbind_probes += probes;
            point_read_rows.push((*family, kept));
        }

        let unbound_at_floor = unbindings_at_or_below_floor(
            rows_by_family
                .get(&MetadataTableFamily::DirentryUnbinds)
                .map_or(&[], Vec::as_slice),
            self.progress.frozen_floor_seq,
        );
        drop_rows_below_frozen_floor(
            rows_by_family,
            self.progress.frozen_floor_seq,
            &unbound_at_floor,
            scope,
        )?;
        rows_by_family.extend(point_read_rows);
        Ok(unbind_probes)
    }

    /// Keeps the bind rows the frozen floor still covers, deciding each one
    /// by reading the snapshot for the unbinds of its own binding.
    ///
    /// Only rows at or below the frozen floor cost a read: a bind above the
    /// floor is retained whatever retired it later, so the reads a slice
    /// makes are bounded by the rows it holds at or below the floor and are
    /// exactly one per such row.
    async fn retain_binds_by_point_read<S: ObjectStore + ?Sized>(
        &self,
        tables: &VerifiedMetadataTables<'_, S>,
        rows: Vec<MetadataRow>,
    ) -> Result<(Vec<MetadataRow>, u64)> {
        let probed: Vec<usize> = rows
            .iter()
            .enumerate()
            .filter(|(_, row)| self.bind_needs_point_read(row))
            .map(|(index, _)| index)
            .collect();
        let mut verdicts = BTreeMap::new();
        for chunk in probed.chunks(PARTIAL_FOLD_PROBE_CONCURRENCY) {
            // Boxed so the step's future does not nest one scan's state
            // inside another's: a slice's drops sit under the whole
            // maintenance call, and the compiler lays that stack out whole.
            let retained = try_join_all(
                chunk
                    .iter()
                    .map(|index| Box::pin(self.bind_survives_point_read(tables, &rows[*index]))),
            )
            .await?;
            for (index, retained) in chunk.iter().zip(retained) {
                verdicts.insert(*index, retained);
            }
        }
        let probes = verdicts.len() as u64;
        let kept = rows
            .into_iter()
            .enumerate()
            .filter(|(index, _)| verdicts.get(index).copied().unwrap_or(true))
            .map(|(_, row)| row)
            .collect();
        Ok((kept, probes))
    }

    fn bind_needs_point_read(&self, row: &MetadataRow) -> bool {
        matches!(row, MetadataRow::DirentryBind { bind_seq, .. } if *bind_seq <= self.progress.frozen_floor_seq)
    }

    async fn bind_survives_point_read<S: ObjectStore + ?Sized>(
        &self,
        tables: &VerifiedMetadataTables<'_, S>,
        row: &MetadataRow,
    ) -> Result<bool> {
        let MetadataRow::DirentryBind {
            parent_inode_id,
            name_key,
            bind_seq,
            bind_delta_index,
            ..
        } = row
        else {
            return Ok(true);
        };
        // The unbind key grammar leads with the binding this row names, so
        // the read is a prefix lookup that returns the unbinds of this one
        // binding and nothing else. The bloom filter is keyed by parent and
        // name, so a binding no operation ever retired misses it outright.
        let unbind_rows = tables
            .scan_prefix_in_runs_for_lookup(
                &self.snapshot,
                MetadataTableFamily::DirentryUnbinds,
                &lookup_keys::direntry_unbind_binding_prefix(
                    *parent_inode_id,
                    name_key.as_str(),
                    *bind_seq,
                    *bind_delta_index,
                ),
                &lookup_keys::direntry_unbind_probe(*parent_inode_id, name_key.as_str()),
            )
            .await
            .map_err(manifest_load_failure)?;
        let unbound_at_floor =
            unbindings_at_or_below_floor(&unbind_rows, self.progress.frozen_floor_seq);
        Ok(bind_survives_frozen_floor(
            row,
            self.progress.frozen_floor_seq,
            &unbound_at_floor,
        ))
    }

    fn partitions_covered(
        &self,
        rows_by_family: &BTreeMap<MetadataTableFamily, Vec<MetadataRow>>,
    ) -> u64 {
        let mut partitions = BTreeSet::new();
        for (family, rows) in rows_by_family {
            for row in rows {
                if let Some(partition) = self.partitioning.partition_of_row(*family, row) {
                    partitions.insert(partition);
                }
            }
        }
        partitions.len() as u64
    }

    /// The publication that finishes the walk: the input runs leave
    /// `metadata_files`, the run the walk built takes their place, and the
    /// state clears. Readers go from seeing the inputs to seeing the output
    /// in one step, so no manifest ever shows both to a scan.
    async fn complete<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        tables: &VerifiedMetadataTables<'_, S>,
        context: &MutationContext,
        timer: &dyn MonotonicTimer,
        publication_started_ms: u64,
    ) -> Result<MetadataFoldWalkOutcome> {
        self.refuse_a_run_whose_index_disagrees()?;
        let previous = &tables.manifest().payload;
        let snapshot_runs: BTreeSet<(ChangeSeq, u32)> = self
            .progress
            .input_runs
            .iter()
            .map(|run| (run.run_seq, run.level))
            .collect();
        let mut metadata_files: Vec<MetadataFileRef> = previous
            .metadata_files
            .iter()
            .filter(|descriptor| {
                !self.group.contains(&descriptor.family)
                    || !snapshot_runs.contains(&(descriptor.run_seq, descriptor.level))
            })
            .cloned()
            .collect();
        metadata_files.extend(self.progress.output_segments.iter().cloned());
        let output_rows = self
            .progress
            .output_segments
            .iter()
            .map(|segment| segment.row_count)
            .sum();
        let base_seq = metadata_files
            .iter()
            .map(|descriptor| descriptor.run_seq)
            .min()
            .unwrap_or(previous.base_seq);

        let mut payload = successor_payload(namespace_id, previous)?;
        payload.metadata_files = metadata_files;
        payload.base_seq = base_seq;
        payload.reorganize = None;
        let manifest_id = payload.manifest_id;
        let output_segments = self.progress.output_segments.len();

        match self
            .publish_manifest(
                store,
                namespace_id,
                payload,
                &tables.manifest().payload.manifest_object_id,
                context,
                timer,
                publication_started_ms,
            )
            .await?
        {
            Some(()) => Ok(MetadataFoldWalkOutcome::Completed {
                manifest_id,
                output_segments,
                output_rows,
            }),
            None => Ok(MetadataFoldWalkOutcome::Superseded),
        }
    }

    /// Refuses to swap in a run whose secondary index does not hold the same
    /// rows as its canonical family.
    ///
    /// A whole-group fold checks that outright, over rows it holds all of. A
    /// walk cannot: the reverse bind index is keyed by child, so no slice
    /// ever holds a bind row and the reverse row that indexes it. The two
    /// digests are what stands in — each step folds the rows it wrote into
    /// them, order does not matter, and the two agree at the end exactly when
    /// the walk wrote the two families the same rows.
    fn refuse_a_run_whose_index_disagrees(&self) -> Result<()> {
        if index_pair(self.group).is_none() {
            return Ok(());
        }
        let (canonical_digest, index_digest) = self.digests()?;
        if canonical_digest != index_digest {
            return Err(CoreError::NamespaceCorrupt(format!(
                "a partial fold of {:?} wrote canonical rows digesting to `{}` and index rows \
                 digesting to `{}`; the two families must hold the same rows, so the run it built \
                 is not publishable",
                self.group,
                spell_row_digest(canonical_digest),
                spell_row_digest(index_digest),
            )));
        }
        Ok(())
    }

    /// Writes the manifest and advances the root, through the one write per
    /// publication every other manifest producer uses. `None` means a
    /// concurrent publication won.
    #[allow(clippy::too_many_arguments)]
    async fn publish_manifest<S: ObjectStore + ?Sized>(
        &self,
        store: &S,
        namespace_id: &NamespaceId,
        payload: NamespaceManifestPayload,
        expected_predecessor: &ManifestObjectId,
        context: &MutationContext,
        timer: &dyn MonotonicTimer,
        publication_started_ms: u64,
    ) -> Result<Option<()>> {
        let manifest = NamespaceManifestEnvelope::from_payload(payload).map_err(|err| {
            CoreError::Internal(format!("failed to build a partial fold's manifest: {err}"))
        })?;
        write_namespace_manifest(store, &manifest)
            .await
            .map_err(manifest_write_failure)?;
        ensure_metadata_publication_budget(timer, publication_started_ms, namespace_id)?;
        match publish_metadata_root(
            store,
            namespace_id,
            &manifest,
            Some(expected_predecessor.clone()),
            context.now_ms,
        )
        .await?
        {
            ManifestPublicationOutcome::Published(_) => Ok(Some(())),
            ManifestPublicationOutcome::Superseded(_)
            | ManifestPublicationOutcome::RootCasRaceLost => Ok(None),
        }
    }
}

/// The rows one step will write, and where the step leaves the walk.
struct SliceRead {
    rows_by_family: BTreeMap<MetadataTableFamily, Vec<MetadataRow>>,
    decoded_input_rows: u64,
    /// Decoded data-block bytes the slice reads, exact from the index
    /// sections the plan already fetched.
    decoded_input_bytes: u64,
    partitions: u64,
    next_cursor: PartitionCursor,
    next_offset: Option<String>,
    scope: FrozenFloorScope,
}

impl SliceRead {
    /// A step that writes nothing and only moves the cursor on.
    fn empty(next_cursor: PartitionCursor) -> Self {
        Self {
            rows_by_family: BTreeMap::new(),
            decoded_input_rows: 0,
            decoded_input_bytes: 0,
            partitions: 0,
            next_cursor,
            next_offset: None,
            scope: FrozenFloorScope::WholePartitions,
        }
    }
}

/// What a slice of whole partitions may take.
enum WholePartitionsPlan {
    /// Every partition from the cursor through to `next_cursor`.
    Slice {
        next_cursor: PartitionCursor,
        planned_bytes: u64,
    },
    /// The cursor's own partition is larger than one step may read, so no
    /// slice of whole partitions can start here.
    FirstPartitionTooLarge,
}

/// One bounded piece of a partition that does not fit one step.
struct PartitionPiecePlan {
    /// The key the piece stops below.
    upper_bound: String,
    planned_bytes: u64,
}

/// One snapshot segment as slice planning walks it: how far into its index
/// the plan has reached, and what one more block costs.
struct PlannedSegment {
    family: MetadataTableFamily,
    index: Arc<Vec<SegmentIndexEntry>>,
    /// The segment's first row key, which is how the plan prunes a segment
    /// that starts above the slice — the same pruning the scan does.
    min_key: String,
    lower_bound: String,
    consumed: usize,
    /// The segment's rows spread evenly over its blocks. Per-block row
    /// counts are not durable, so this is the plan's estimate; the step
    /// charges what it actually decodes.
    rows_per_block: u64,
}

impl PlannedSegment {
    /// Adds the blocks this segment contributes to a slice that now runs up
    /// to `upper_bound`, and returns what they cost.
    fn extend_through(&mut self, upper_bound: &str) -> (u64, u64) {
        // A segment whose first row sorts at or above the bound holds nothing
        // this slice reads. Saying so here matters: the block range below
        // always admits one block past the bound, because a block whose last
        // key sits above it can still hold keys below it, and without this a
        // segment far above the slice would be priced at one block anyway.
        if self.min_key.as_str() >= upper_bound {
            return (0, 0);
        }
        let end = index_blocks_for_key_range(&self.index, &self.lower_bound, Some(upper_bound)).end;
        let mut rows = 0u64;
        let mut bytes = 0u64;
        while self.consumed < end {
            bytes = bytes.saturating_add(u64::from(self.index[self.consumed].block.decoded_len));
            rows = rows.saturating_add(self.rows_per_block);
            self.consumed += 1;
        }
        (rows, bytes)
    }
}

/// The canonical family and secondary index of a group that carries one.
fn index_pair(group: &[MetadataTableFamily]) -> Option<(MetadataTableFamily, MetadataTableFamily)> {
    if group.contains(&MetadataTableFamily::DirentryBinds) {
        Some((
            MetadataTableFamily::DirentryBinds,
            MetadataTableFamily::DirentryChildBinds,
        ))
    } else if group.contains(&MetadataTableFamily::Revisions) {
        Some((
            MetadataTableFamily::Revisions,
            MetadataTableFamily::RevisionsByInodeDesc,
        ))
    } else {
        None
    }
}

/// Seeds for the two hash passes a row digest is built from, borrowed from
/// the way segment bloom filters widen one hash into two.
const ROW_DIGEST_SEED_LOW: u64 = 0x9e37_79b9_7f4a_7c15;
const ROW_DIGEST_SEED_HIGH: u64 = 0xc2b2_ae3d_27d4_eb4f;

/// The digest of one row: two hash passes over its canonical encoding, read
/// as one 128-bit value.
///
/// This is corruption detection, not a signature. What it has to catch is a
/// walk that wrote a secondary index rows its canonical family does not hold,
/// including a row that differs in one field.
pub(super) fn row_digest(row: &MetadataRow) -> Result<u128> {
    let encoded = serde_json::to_vec(row).map_err(|err| {
        CoreError::Internal(format!(
            "failed to encode a metadata row for digesting: {err}"
        ))
    })?;
    let low = xxh64(&encoded, ROW_DIGEST_SEED_LOW);
    let high = xxh64(&encoded, ROW_DIGEST_SEED_HIGH);
    Ok((u128::from(high) << 64) | u128::from(low))
}

/// Folds `rows` into a running digest.
///
/// The combiner is wrapping addition, so the digest depends on the multiset
/// of rows and not on the order a walk happened to write them in — which is
/// the whole point, because the two families of an index pair are written in
/// different orders and, for the bind pair, in different steps.
pub(super) fn fold_rows_into_digest(digest: u128, rows: Option<&Vec<MetadataRow>>) -> Result<u128> {
    let mut digest = digest;
    for row in rows.into_iter().flatten() {
        digest = digest.wrapping_add(row_digest(row)?);
    }
    Ok(digest)
}

/// The durable spelling of a digest: thirty-two lowercase hex characters.
pub(super) fn spell_row_digest(digest: u128) -> String {
    format!("{digest:032x}")
}

/// Reads back a digest spelled by [`spell_row_digest`], or `None` when the
/// spelling is not one.
pub(super) fn parse_row_digest(spelled: &str) -> Option<u128> {
    if spelled.len() != 32
        || !spelled
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return None;
    }
    u128::from_str_radix(spelled, 16).ok()
}

fn group_partitioning(group: &[MetadataTableFamily]) -> Result<GroupPartitioning> {
    GroupPartitioning::for_group(group).ok_or_else(|| {
        CoreError::Internal(format!(
            "family group {group:?} has no partition grammar, so it cannot be folded in slices"
        ))
    })
}

fn partition_of_cursor(cursor: &PartitionCursor) -> Result<PartitionKey> {
    match cursor {
        PartitionCursor::At(partition) => Ok(partition.clone()),
        PartitionCursor::End => Err(CoreError::NamespaceCorrupt(
            "a partial fold stands inside a partition while its cursor has passed every partition"
                .to_owned(),
        )),
    }
}

fn manifest_load_failure(error: ManifestLoadError) -> CoreError {
    CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
}

/// The manifest a step publishes: the previous one with a fresh identity.
/// Everything a fold does not touch travels unchanged, so a slice moves only
/// the fold's own state.
fn successor_payload(
    namespace_id: &NamespaceId,
    previous: &NamespaceManifestPayload,
) -> Result<NamespaceManifestPayload> {
    let manifest_id = next_manifest_id_after(previous.manifest_id)?;
    Ok(NamespaceManifestPayload {
        namespace_id: namespace_id.clone(),
        manifest_id,
        // One generated object id, one write. The generated id ends in 16
        // random hex characters, so the key is this step's alone.
        manifest_object_id: ManifestObjectId::generate(manifest_id),
        head_seq: previous.head_seq,
        head_commit_id: previous.head_commit_id.clone(),
        base_seq: previous.base_seq,
        writer_epoch: previous.writer_epoch,
        next_inode_id: previous.next_inode_id,
        retention_floor_seq: previous.retention_floor_seq,
        metadata_files: previous.metadata_files.clone(),
        reorganize: previous.reorganize.clone(),
    })
}

/// Turns the run ids a progress state names back into the manifest's runs.
fn resolve_snapshot_runs<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    progress: &MetadataReorganizeProgress,
) -> Result<Vec<MetadataRunManifest>> {
    progress
        .input_runs
        .iter()
        .map(|input| {
            tables
                .scan_runs
                .iter()
                .find(|run| run.run_seq == input.run_seq && run.level == input.level)
                .cloned()
                .ok_or_else(|| {
                    CoreError::NamespaceCorrupt(format!(
                        "a partial fold names input run seq `{}` level {}, which the manifest \
                         does not reference",
                        input.run_seq, input.level
                    ))
                })
        })
        .collect()
}

/// Whether `snapshot` holds every run of the group that sorts at or below
/// its own oldest run — the property that makes dropping rows
/// visibility-preserving.
///
/// The order is the selector's: base-tier runs sit under every delta run
/// whatever their sequence says, because a bounded fold stamps its output at
/// the manifest head.
fn snapshot_is_group_bottom_anchored(
    runs: &[MetadataRunManifest],
    group: &[MetadataTableFamily],
    snapshot: &[MetadataRunManifest],
) -> bool {
    // Base-tier runs hold rows an earlier fold already absorbed, so they sit
    // under every delta run whatever their sequence says.
    let rank = |run: &MetadataRunManifest| (run.level == CHECKPOINT_L0_RUN_LEVEL, run.run_seq);
    let Some(highest_in_snapshot) = snapshot.iter().map(rank).max() else {
        return false;
    };
    let holds_group_rows = |run: &MetadataRunManifest| {
        run.tables
            .iter()
            .any(|table| group.contains(&table.family) && !table.segments.is_empty())
    };
    // Nothing this group owns may sit below the snapshot's top without being
    // in it: a row the snapshot cannot see is a row its drops cannot account
    // for.
    runs.iter()
        .filter(|run| holds_group_rows(run) && rank(run) <= highest_in_snapshot)
        .all(|run| {
            snapshot
                .iter()
                .any(|input| input.run_seq == run.run_seq && input.level == run.level)
        })
}
