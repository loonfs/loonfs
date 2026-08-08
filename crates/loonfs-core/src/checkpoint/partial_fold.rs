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
use super::partition::{GroupPartitioning, PartitionCursor, PartitionKey};
use super::publish::{
    manifest_write_failure, publish_metadata_root, write_namespace_manifest,
    ManifestPublicationOutcome,
};
use super::reorganize::{
    drop_rows_below_frozen_floor, unbindings_at_or_below_floor, BindingGeneration,
};
use super::runs::{
    MetadataLsmPolicy, MetadataRunManifest, CHECKPOINT_BASE_RUN_LEVEL, CHECKPOINT_L0_RUN_LEVEL,
    PARTIAL_FOLD_UNBIND_SCAN_PAGE_ROWS, REORGANIZE_FAMILY_GROUPS,
};
use super::scan::VerifiedMetadataTables;
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::timing::MonotonicTimer;
use loonfs_api::wire::manifest::{
    MetadataFileRef, MetadataReorganizeProgress, MetadataRow, MetadataRunId, MetadataTableFamily,
    NamespaceManifestEnvelope, NamespaceManifestPayload,
};
use loonfs_api::wire::sst_blocks::{index_blocks_for_key_range, SegmentIndexEntry};
use loonfs_api::{ChangeSeq, ManifestId, ManifestObjectId, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

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
    /// Partitions the slice covered, counted over the rows it decoded.
    pub(super) partitions: u64,
    pub(super) decoded_input_rows: u64,
    pub(super) decoded_input_bytes: u64,
    pub(super) output_rows: u64,
    pub(super) drops: MetadataFoldSliceDrops,
}

/// Whether a slice dropped what the frozen floor allows, and why not when
/// it did not.
///
/// Public because a reorganization step reports it: an operator watching a
/// walk needs to know whether it is reclaiming rows or only rebuilding the
/// run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataFoldSliceDrops {
    /// The slice dropped every row the frozen floor allows.
    Applied,
    /// The unbind set the bind drop reads did not fit its bound, so the
    /// whole walk is a pure rewrite. That still unfreezes the base, and the
    /// next walk drops with a smaller set.
    UnbindSetOverBound,
}

/// The frozen unbind set a walk decides bind drops against, or the record
/// that it did not fit.
#[derive(Debug, Clone, PartialEq, Eq)]
enum FrozenUnbindSet {
    /// Every unbinding at or below the frozen floor, read from the snapshot
    /// at walk start and re-read identically on every resumption.
    Complete(BTreeSet<BindingGeneration>),
    /// The set was larger than its bound. The walk still runs, as a pure
    /// rewrite that unfreezes the base; the next walk drops with a smaller
    /// set (design doc, "Retention during the walk").
    OverBound,
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
    unbind_set: FrozenUnbindSet,
    /// Rows the start-up unbind scan decoded, charged against the starting
    /// step's budgets and zero on every step after it.
    start_scan_rows: u64,
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
    pub(super) async fn start<S: ObjectStore + ?Sized>(
        tables: &VerifiedMetadataTables<'_, S>,
        group: &'static [MetadataTableFamily],
        snapshot: Vec<MetadataRunManifest>,
        frozen_floor_seq: ChangeSeq,
        policy: MetadataLsmPolicy,
    ) -> Result<Self> {
        let partitioning = group_partitioning(group)?;
        let payload = &tables.manifest().payload;
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
            output_segments: Vec::new(),
        };
        let (unbind_set, start_scan_rows) =
            derive_unbind_set(tables, group, &snapshot, frozen_floor_seq, policy).await?;
        Ok(Self {
            group,
            partitioning,
            progress,
            snapshot,
            unbind_set,
            start_scan_rows,
        })
    }

    /// Rebuilds the walk a manifest carries, or `None` when it carries none.
    ///
    /// The unbind set is re-derived here rather than carried: the floor is
    /// frozen and the snapshot is immutable, so the scan that built it
    /// answers identically however many times it runs.
    pub(super) async fn resume_from_manifest<S: ObjectStore + ?Sized>(
        tables: &VerifiedMetadataTables<'_, S>,
        policy: MetadataLsmPolicy,
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
        // The cursor is the walk's whole position; a state that cannot say
        // where it stands is refused rather than restarted from the front,
        // which would rewrite partitions the state's outputs already hold.
        partitioning.parse_cursor(&progress.cursor).map_err(|why| {
            CoreError::NamespaceCorrupt(format!(
                "a partial fold carries an unreadable cursor: {why}"
            ))
        })?;
        let snapshot = resolve_snapshot_runs(tables, &progress)?;
        if !snapshot_is_group_bottom_anchored(tables.scan_runs.as_ref(), group, &snapshot) {
            return Err(CoreError::NamespaceCorrupt(format!(
                "a partial fold of {group:?} is merging a run subset that is no longer the \
                 group's oldest runs"
            )));
        }
        let (unbind_set, start_scan_rows) =
            derive_unbind_set(tables, group, &snapshot, progress.frozen_floor_seq, policy).await?;
        Ok(Some(Self {
            group,
            partitioning,
            progress,
            snapshot,
            unbind_set,
            start_scan_rows,
        }))
    }

    pub(super) fn group(&self) -> &'static [MetadataTableFamily] {
        self.group
    }

    pub(super) fn progress(&self) -> &MetadataReorganizeProgress {
        &self.progress
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
        let cursor = self.cursor()?;
        let plan = self.plan_slice(tables, &cursor, policy).await?;
        let Some(plan) = plan else {
            return self
                .complete(
                    store,
                    namespace_id,
                    tables,
                    context,
                    timer,
                    publication_started_ms,
                )
                .await;
        };
        self.publish_slice(
            store,
            namespace_id,
            tables,
            policy,
            context,
            timer,
            publication_started_ms,
            &cursor,
            plan,
        )
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

    /// Chooses the next slice by reading the snapshot's index sections.
    ///
    /// A segment's index publishes the last key of every data block and that
    /// block's decoded length, so a plan can be priced without decoding a
    /// row. Per-block row counts are not durable — only the segment's
    /// total — so the row side of the plan is the segment's average spread
    /// over its blocks, and the step charges the rows it actually decodes.
    ///
    /// `None` means no rows of the group remain at or above the cursor, so
    /// the walk is finished and the next publication completes it.
    async fn plan_slice<S: ObjectStore + ?Sized>(
        &self,
        tables: &VerifiedMetadataTables<'_, S>,
        cursor: &PartitionCursor,
        policy: MetadataLsmPolicy,
    ) -> Result<Option<SlicePlan>> {
        let mut segments = Vec::new();
        let mut boundaries = BTreeSet::<PartitionKey>::new();
        for family in self.group {
            let lower_bound = self.partitioning.family_lower_bound(*family, cursor);
            for descriptor in self.snapshot_descriptors(*family) {
                let index = load_segment_index_for_reorganization(
                    tables.store,
                    tables.table_cache,
                    &tables.block_memo,
                    descriptor,
                )
                .await
                .map_err(manifest_load_failure)?;
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
                    lower_bound: lower_bound.clone(),
                    consumed: first,
                    rows_per_block,
                });
            }
        }
        if boundaries.is_empty() {
            return Ok(None);
        }

        let row_budget = u64::try_from(policy.max_decoded_input_rows_per_step.get())
            .unwrap_or(u64::MAX)
            .saturating_sub(self.start_scan_rows);
        let byte_budget =
            u64::try_from(policy.max_decoded_input_bytes_per_step.get()).unwrap_or(u64::MAX);
        let mut planned_rows = 0u64;
        let mut planned_bytes = 0u64;
        let mut through = None;
        for boundary in boundaries {
            let next = self.partitioning.cursor_after(&boundary);
            let (added_rows, added_bytes) = segments
                .iter_mut()
                .map(|segment| segment.extend_through(self.partitioning, &next))
                .fold((0u64, 0u64), |(rows, bytes), (added_rows, added_bytes)| {
                    (
                        rows.saturating_add(added_rows),
                        bytes.saturating_add(added_bytes),
                    )
                });
            let candidate_rows = planned_rows.saturating_add(added_rows);
            let candidate_bytes = planned_bytes.saturating_add(added_bytes);
            // The first partition goes in whatever it costs. A walk that
            // parked on an oversized partition would be the very stall this
            // design exists to remove, and one partition is bounded by what
            // one inode or one directory can accumulate, not by group size.
            if through.is_some() && (candidate_rows > row_budget || candidate_bytes > byte_budget) {
                break;
            }
            planned_rows = candidate_rows;
            planned_bytes = candidate_bytes;
            through = Some(boundary);
        }
        let through = through.expect("a non-empty boundary set always admits its first partition");
        Ok(Some(SlicePlan {
            next_cursor: self.partitioning.cursor_after(&through),
            planned_bytes,
        }))
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
        cursor: &PartitionCursor,
        plan: SlicePlan,
    ) -> Result<MetadataFoldWalkOutcome> {
        let mut rows_by_family = BTreeMap::<MetadataTableFamily, Vec<MetadataRow>>::new();
        for family in self.group {
            let lower_bound = self.partitioning.family_lower_bound(*family, cursor);
            let upper_bound = self
                .partitioning
                .family_lower_bound(*family, &plan.next_cursor);
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

        let mut drops = MetadataFoldSliceDrops::Applied;
        match &self.unbind_set {
            FrozenUnbindSet::OverBound => drops = MetadataFoldSliceDrops::UnbindSetOverBound,
            FrozenUnbindSet::Complete(unbound_at_floor) => {
                // The reverse index leaves the shared pass: its rows are
                // keyed by child, so the slice holding one does not hold the
                // forward binds that pass reads. It is decided against the
                // frozen set instead, just below.
                let reverse_rows = rows_by_family.remove(&MetadataTableFamily::DirentryChildBinds);
                drop_rows_below_frozen_floor(
                    &mut rows_by_family,
                    self.progress.frozen_floor_seq,
                    unbound_at_floor,
                )?;
                if let Some(mut reverse_rows) = reverse_rows {
                    reverse_rows.retain(|row| {
                        retain_reverse_bind_row(
                            row,
                            self.progress.frozen_floor_seq,
                            unbound_at_floor,
                        )
                    });
                    rows_by_family.insert(MetadataTableFamily::DirentryChildBinds, reverse_rows);
                }
            }
        }
        let output_rows = rows_by_family
            .values()
            .map(|rows| rows.len() as u64)
            .sum::<u64>();

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

        let mut progress = self.progress.clone();
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
        progress.cursor = self.partitioning.spell_cursor(&plan.next_cursor);

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
                self.start_scan_rows = 0;
                Ok(MetadataFoldWalkOutcome::SlicePublished(
                    MetadataFoldSliceReport {
                        manifest_id,
                        partitions,
                        decoded_input_rows,
                        decoded_input_bytes: plan.planned_bytes,
                        output_rows,
                        drops,
                    },
                ))
            }
            None => Ok(MetadataFoldWalkOutcome::Superseded),
        }
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

/// Whether one reverse bind row survives the frozen floor.
///
/// The forward rule keeps a bind at or below the floor when it is both the
/// latest in its (parent, name) slot and not unbound. A slice of the reverse
/// index is keyed by child, so it holds neither the parent's other binds nor
/// the parent's unbinds and can check neither directly. The frozen set
/// settles it on its own: a bind is only ever superseded by an operation
/// that also unbinds it — the invariant `drop_rows_below_frozen_floor`
/// refuses to compact without — so a bind that is not unbound at the floor
/// is the latest in its slot, and the two-part forward rule collapses to
/// this membership test. That the two agree row for row on real histories is
/// what the equivalence oracle checks.
///
/// The row carries everything the set is keyed by: a reverse row is a bind
/// row, holding its parent, name, sequence, and delta index whichever family
/// stores it.
fn retain_reverse_bind_row(
    row: &MetadataRow,
    frozen_floor_seq: ChangeSeq,
    unbound_at_floor: &BTreeSet<BindingGeneration>,
) -> bool {
    let MetadataRow::DirentryBind {
        parent_inode_id,
        name_key,
        bind_seq,
        bind_delta_index,
        ..
    } = row
    else {
        return true;
    };
    *bind_seq > frozen_floor_seq
        || !unbound_at_floor.contains(&(
            *parent_inode_id,
            name_key.clone(),
            *bind_seq,
            *bind_delta_index,
        ))
}

/// The slice one step will merge.
struct SlicePlan {
    /// The cursor the manifest carries once this slice is published.
    next_cursor: PartitionCursor,
    /// Decoded data-block bytes the slice reads, exact from the index
    /// sections the plan already fetched.
    planned_bytes: u64,
}

/// One snapshot segment as slice planning walks it: how far into its index
/// the plan has reached, and what one more block costs.
struct PlannedSegment {
    family: MetadataTableFamily,
    index: Arc<Vec<SegmentIndexEntry>>,
    lower_bound: String,
    consumed: usize,
    /// The segment's rows spread evenly over its blocks. Per-block row
    /// counts are not durable, so this is the plan's estimate; the step
    /// charges what it actually decodes.
    rows_per_block: u64,
}

impl PlannedSegment {
    /// Adds the blocks this segment contributes to a slice that now runs up
    /// to `next`, and returns what they cost.
    fn extend_through(
        &mut self,
        partitioning: GroupPartitioning,
        next: &PartitionCursor,
    ) -> (u64, u64) {
        let upper_bound = partitioning.family_lower_bound(self.family, next);
        let end =
            index_blocks_for_key_range(&self.index, &self.lower_bound, Some(&upper_bound)).end;
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

fn group_partitioning(group: &[MetadataTableFamily]) -> Result<GroupPartitioning> {
    GroupPartitioning::for_group(group).ok_or_else(|| {
        CoreError::Internal(format!(
            "family group {group:?} has no partition grammar, so it cannot be folded in slices"
        ))
    })
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

/// Reads the unbindings at or below the frozen floor out of the snapshot.
///
/// This is the one derived thing a step needs that a slice cannot see:
/// the bind drop asks whether a binding was retired, and the unbind that
/// retired it may sit in any partition the walk has not reached. The scan
/// runs at the start and again on every resumption, and answers identically
/// both times because the floor is frozen and the snapshot is immutable.
///
/// Groups other than the bindings group have no such rule, so they get an
/// empty set and no scan.
async fn derive_unbind_set<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    group: &[MetadataTableFamily],
    snapshot: &[MetadataRunManifest],
    frozen_floor_seq: ChangeSeq,
    policy: MetadataLsmPolicy,
) -> Result<(FrozenUnbindSet, u64)> {
    if !group.contains(&MetadataTableFamily::DirentryUnbinds) {
        return Ok((FrozenUnbindSet::Complete(BTreeSet::new()), 0));
    }
    // The set is held for the whole walk, so it is bounded by what one step
    // may hold in decoded rows anyway. Over the bound the walk keeps going
    // as a pure rewrite: that still unfreezes the base, and the next walk
    // drops with a smaller set.
    let byte_bound = policy.max_decoded_input_bytes_per_step.get() as u64;
    let mut estimated_bytes = 0u64;
    let mut scanned_rows = 0u64;
    let mut unbound = BTreeSet::new();
    let mut lower_bound = MetadataTableFamily::DirentryUnbinds
        .row_key_prefix()
        .to_owned();
    loop {
        let page = tables
            .scan_key_range_page_in_runs(
                snapshot,
                MetadataTableFamily::DirentryUnbinds,
                &lower_bound,
                None,
                PARTIAL_FOLD_UNBIND_SCAN_PAGE_ROWS,
            )
            .await
            .map_err(manifest_load_failure)?;
        let Some((last_key, _)) = page.last() else {
            break;
        };
        // Row keys are globally unique, so resuming strictly past the last
        // one returned skips exactly that row.
        lower_bound = format!("{last_key}\0");
        scanned_rows = scanned_rows.saturating_add(page.len() as u64);
        let rows: Vec<MetadataRow> = page.into_iter().map(|(_, row)| row).collect();
        for generation in unbindings_at_or_below_floor(&rows, frozen_floor_seq) {
            estimated_bytes = estimated_bytes.saturating_add(unbind_entry_bytes(&generation));
            if estimated_bytes > byte_bound {
                return Ok((FrozenUnbindSet::OverBound, scanned_rows));
            }
            unbound.insert(generation);
        }
    }
    Ok((FrozenUnbindSet::Complete(unbound), scanned_rows))
}

fn unbind_entry_bytes(generation: &BindingGeneration) -> u64 {
    (std::mem::size_of::<BindingGeneration>() + generation.1.as_str().len()) as u64
}
