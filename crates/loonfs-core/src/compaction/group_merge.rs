//! The merge engine of one metadata compaction unit.
//!
//! [`GroupMerge`] merges runs in row-key order, applies retention rules, and
//! writes bounded output segments. It buffers only input blocks, retention
//! state, one segment builder per family, and a bounded wave of encoded outputs.
//! Only base merges may remove rows below the retention floor, and only base
//! merges remove the rows of inodes that an expired deletion hides. Every
//! merge drops superseded layout rows.

use super::kept_inodes::{load_kept_inode_filter, KeptInodeFilter};
use super::merge_iterator::{
    locality_of, refill_iterators, select_next_iterator, MetadataSegmentBlockLoader,
    MetadataSegmentRowIterator,
};
use super::plan::MetadataCompactionSpec;
use super::plan::{group_run_descriptors, MergePlacement};
use super::retention::retention_clusters;
use super::retention::RetentionCluster;
use super::retention::{KeptRow, RetentionRule};
use super::step::CompactionUnitOutcome;
use super::step::MetadataCompactionCancellation;
use crate::bloom_filter::BloomFilter;
use crate::error::{CoreError, Result};
use crate::segment::build::{MetadataSegmentPuts, MetadataSegmentWriter};
use crate::segment::cache::MetadataSegmentCache;
use crate::segment::index_block_load::{segment_object_bytes, SegmentWriter};
use crate::segment::runs::{MetadataFamilyGroup, MetadataRunManifest, MetadataRunPolicy};
use crate::segment::scan::VerifiedMetadataSegments;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{MetadataRow, MetadataRowFamily, MetadataSegmentRef};
use loonfs_types::{ChangeSeq, NamespaceId, RunNo};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Rows between two progress lines. A unit that reaches one reads more than a
/// million rows, so a long unit logs a handful of lines rather than one per
/// segment.
const PROGRESS_ROW_INTERVAL: u64 = 1_000_000;

/// What one finished merge built, kept in memory until the caller publishes
/// it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MetadataMergeReport {
    /// The output run's segment descriptors, naming the object keys the merge
    /// wrote them to.
    pub(crate) output_segments: Vec<MetadataSegmentRef>,
    pub(crate) rows_read: u64,
    pub(crate) rows_written: u64,
    pub(super) input_bytes: u64,
    pub(super) output_bytes: u64,
    /// The most decoded input blocks the merge's iterators kept at once,
    /// which bounds the merge's memory, so tests assert it doesn't follow the
    /// size of the group. A retention operator decides each row as it
    /// arrives and keeps none.
    pub(crate) peak_resident_blocks: usize,
}

/// Rebuilds the family group and runs selected by `spec`. `segments` must come
/// from the manifest used to create the specification.
pub(crate) async fn run_metadata_compaction<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    output_cache: &MetadataSegmentCache,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    run_policy: MetadataRunPolicy,
    cancellation: &MetadataCompactionCancellation,
) -> Result<std::result::Result<MetadataMergeReport, CompactionUnitOutcome>> {
    let input_runs = resolve_input_runs(segments, spec)?;
    let kept_inodes =
        match load_kept_inode_filter(segments, namespace_id, spec, cancellation).await? {
            Ok(kept_inodes) => kept_inodes,
            Err(stopped) => return Ok(Err(stopped)),
        };
    let merge = GroupMerge::new(
        segments,
        output_cache,
        namespace_id,
        spec,
        run_policy,
        input_runs,
        kept_inodes,
    );
    merge.run(&MergeControl { cancellation }).await
}

/// Merging one family group: the shared engine, whatever is driving it.
struct GroupMerge<'a, S: ObjectStore + ?Sized> {
    store: &'a S,
    /// The cache that receives the segments the merge writes. The merge
    /// reads its input without any cache.
    output_cache: &'a MetadataSegmentCache,
    namespace_id: &'a NamespaceId,
    group: MetadataFamilyGroup,
    /// Where the output stands in the group and whether rows may be dropped.
    placement: MergePlacement,
    frozen_floor_seq: ChangeSeq,
    /// The basis manifest's acknowledged floor, at or below which a listed
    /// deletion that no undelete cancelled has expired.
    acknowledged_floor_seq: ChangeSeq,
    run_policy: MetadataRunPolicy,
    input_runs: Vec<MetadataRunManifest>,
    basis_runs: &'a [MetadataRunManifest],
    output_run_no: RunNo,
    chain_filter: ChainFilter,
    kept_inodes: KeptInodeFilter,
    result: MetadataMergeReport,
    canonical_digest: RowDigest,
    index_digest: RowDigest,
    /// The last input row key seen in each family, which is what lets the merge
    /// refuse a family that contains one row key twice. One string per family.
    last_input_key_by_family: BTreeMap<MetadataRowFamily, String>,
    /// Progress state for an unbounded background merge. `None` for a merge
    /// short enough to have nothing to report.
    progress: ProgressReporter,
}

/// Whether a layout merge drops the layout rows of chains no revision names.
enum ChainFilter {
    /// The merge isn't a layout merge that may drop rows below the floor.
    NotNeeded,
    /// The content IDs the revisions name, which a kept layout row matches.
    Loaded(BloomFilter),
}

struct MergeControl<'a> {
    cancellation: &'a MetadataCompactionCancellation,
}

impl MergeControl<'_> {
    fn cancellation(&self) -> Option<CompactionUnitOutcome> {
        self.cancellation
            .is_cancelled()
            .then_some(CompactionUnitOutcome::Cancelled)
    }
}

struct ProgressReporter {
    input_rows: u64,
    next_rows: u64,
}

impl ProgressReporter {
    fn new(input_rows: u64) -> Self {
        Self {
            input_rows,
            next_rows: PROGRESS_ROW_INTERVAL,
        }
    }
}

impl<'a, S: ObjectStore + ?Sized> GroupMerge<'a, S> {
    fn new(
        basis: &'a VerifiedMetadataSegments<'_, S>,
        output_cache: &'a MetadataSegmentCache,
        namespace_id: &'a NamespaceId,
        spec: &MetadataCompactionSpec,
        run_policy: MetadataRunPolicy,
        input_runs: Vec<MetadataRunManifest>,
        kept_inodes: KeptInodeFilter,
    ) -> Self {
        let group = spec.group();
        let input_bytes = input_runs
            .iter()
            .flat_map(|run| group_run_descriptors(run, group))
            .map(segment_object_bytes)
            .sum();
        Self {
            store: basis.store,
            basis_runs: &basis.scan_runs,
            output_run_no: basis.manifest().payload().next_run_no,
            acknowledged_floor_seq: basis.manifest().payload().acknowledged_floor_seq,
            chain_filter: ChainFilter::NotNeeded,
            kept_inodes,
            output_cache,
            namespace_id,
            group,
            placement: spec.placement(),
            frozen_floor_seq: spec.frozen_floor_seq(),
            run_policy,
            input_runs,
            result: MetadataMergeReport {
                input_bytes,
                ..MetadataMergeReport::default()
            },
            canonical_digest: RowDigest::default(),
            index_digest: RowDigest::default(),
            last_input_key_by_family: BTreeMap::new(),
            progress: ProgressReporter::new(spec.input_rows()),
        }
    }

    /// Merges every cluster of the group, in order.
    async fn run(
        mut self,
        control: &MergeControl<'_>,
    ) -> Result<std::result::Result<MetadataMergeReport, CompactionUnitOutcome>> {
        if let Some(stop) = self.load_chain_filter(control).await? {
            return Ok(Err(stop));
        }
        let mut puts =
            MetadataSegmentPuts::new(self.store, self.output_cache, SegmentWriter::Compaction);
        for cluster in retention_clusters(self.group) {
            if let Some(stopped) = self.run_cluster(cluster, control, &mut puts).await? {
                return Ok(Err(stopped));
            }
        }
        self.refuse_a_run_whose_index_disagrees()?;
        puts.finish().await?;
        // A token set after the final cluster still owns the outcome. The
        // caller must not re-derive cancellation from a stale Completed value.
        if let Some(stop) = control.cancellation() {
            return Ok(Err(stop));
        }
        Ok(Ok(self.result))
    }

    async fn load_chain_filter(
        &mut self,
        control: &MergeControl<'_>,
    ) -> Result<Option<CompactionUnitOutcome>> {
        if self.group != MetadataFamilyGroup::ContentLayouts
            || !self.placement.may_drop_rows_below_the_retention_floor()
        {
            return Ok(None);
        }
        let expected_ids = self
            .basis_runs
            .iter()
            .flat_map(|run| group_run_descriptors(run, MetadataFamilyGroup::Revisions))
            .fold(0_u64, |count, segment| {
                count.saturating_add(segment.row_count)
            });
        let mut filter = BloomFilter::new(expected_ids, self.output_run_no.0);
        let loader = MetadataSegmentBlockLoader::new(self.store, None);
        for run in self.basis_runs {
            let segments = group_run_descriptors(run, MetadataFamilyGroup::Revisions)
                .cloned()
                .collect();
            let mut iterator = [MetadataSegmentRowIterator::metadata(
                MetadataRowFamily::Revisions,
                run.run_seq,
                segments,
            )];
            loop {
                if let Some(stop) = control.cancellation() {
                    return Ok(Some(stop));
                }
                refill_iterators(
                    &loader,
                    &mut iterator,
                    self.run_policy.max_decoded_input_bytes_per_step.get(),
                )
                .await?;
                if iterator[0].head().is_none() {
                    break;
                }
                if let MetadataRow::FileRevision(row) = iterator[0].take_head() {
                    // The filter is sized for the basis's revision rows, so a
                    // refused insert means the segments contain more revision
                    // rows than their descriptors declare.
                    if filter
                        .insert(row.content_ref.content_id.as_str().as_bytes())
                        .is_none()
                    {
                        return Err(CoreError::NamespaceCorrupt(format!(
                            "revision segments of {} hold more rows than their descriptors declare",
                            self.namespace_id
                        )));
                    }
                }
            }
        }
        self.chain_filter = ChainFilter::Loaded(filter);
        Ok(None)
    }

    /// Rejects duplicate or out-of-order input keys within a family.
    ///
    /// This checks input before retention can drop a duplicate. Output parity
    /// digests cannot detect duplicates present in both paired families.
    fn refuse_a_repeated_input_key(
        &mut self,
        family: MetadataRowFamily,
        row_key: &str,
    ) -> Result<()> {
        if let Some(previous) = self.last_input_key_by_family.get(&family) {
            // A segment's decode checks key order within each block and the
            // block index's last keys, but not one block's first key against
            // the previous block's last, so stored bytes can go backwards here.
            match row_key.cmp(previous.as_str()) {
                std::cmp::Ordering::Greater => {}
                std::cmp::Ordering::Equal => {
                    return Err(CoreError::NamespaceCorrupt(format!(
                        "metadata family `{family:?}` contains duplicate row key `{row_key}`; \
                         refusing to merge it into a run"
                    )));
                }
                std::cmp::Ordering::Less => {
                    return Err(CoreError::NamespaceCorrupt(format!(
                        "metadata family `{family:?}` row key went backwards: `{row_key}` \
                         follows `{previous}`; refusing to merge it into a run"
                    )));
                }
            }
        }
        self.last_input_key_by_family
            .insert(family, row_key.to_owned());
        Ok(())
    }

    fn rule_for(&self, cluster: &RetentionCluster) -> RetentionRule {
        if self.placement.may_drop_rows_below_the_retention_floor()
            || cluster.rule == RetentionRule::FirstPerGroup
        {
            cluster.rule
        } else {
            RetentionRule::KeepEveryRow
        }
    }

    /// Merges one cluster end to end.
    async fn run_cluster(
        &mut self,
        cluster: &RetentionCluster,
        control: &MergeControl<'_>,
        puts: &mut MetadataSegmentPuts<'_, S>,
    ) -> Result<Option<CompactionUnitOutcome>> {
        // One iterator per input run and family. The planner caps run fan-in;
        // each iterator advances through that run's segments sequentially.
        let mut iterators = Vec::new();
        for run in self.input_runs.iter() {
            for family in cluster.families {
                let segments: Vec<MetadataSegmentRef> = group_run_descriptors(run, self.group)
                    .filter(|descriptor| descriptor.family == *family)
                    .cloned()
                    .collect();
                if !segments.is_empty() {
                    iterators.push(MetadataSegmentRowIterator::metadata(
                        *family,
                        run.run_seq,
                        segments,
                    ));
                }
            }
        }
        let mut writers: BTreeMap<MetadataRowFamily, MetadataSegmentWriter> = cluster
            .families
            .iter()
            .map(|family| {
                (
                    *family,
                    MetadataSegmentWriter::new(*family, self.namespace_id),
                )
            })
            .collect();

        let floor_seq = self.frozen_floor_seq;
        let rule = self.rule_for(cluster);
        let mut operator = rule.operator(self.acknowledged_floor_seq);
        let mut locality: Option<String> = None;
        loop {
            if let Some(stop) = control.cancellation() {
                return Ok(Some(stop));
            }
            puts.run(self.refill(&mut iterators)).await?;
            let Some(next) = select_next_iterator(&iterators, |family, row_key| {
                locality_of(*family, row_key, cluster.locality)
            }) else {
                break;
            };
            // The locality is a slice of the iterator's own key, so it is
            // only copied when it changes — once per group rather than once
            // per row.
            let opened = {
                let iterator = &iterators[next];
                let (row_key, _) = iterator
                    .head()
                    .expect("the selected iterator should have a row");
                let family = *iterator.sort_key();
                self.refuse_a_repeated_input_key(family, row_key)?;
                let row_locality = locality_of(family, row_key, cluster.locality);
                (locality.as_deref() != Some(row_locality)).then(|| row_locality.to_owned())
            };
            if opened.is_some() {
                operator.close_group();
                locality = opened;
            }
            let family = *iterators[next].sort_key();
            let row = iterators[next].take_head();
            self.result.rows_read += 1;
            self.report_progress();
            if let (ChainFilter::Loaded(filter), MetadataRow::ContentLayout(layout)) =
                (&self.chain_filter, &row)
            {
                if !filter.may_contain(layout.content_id.as_str().as_bytes()) {
                    continue;
                }
            }
            if !self.kept_inodes.keeps(&row) {
                continue;
            }
            if let Some(kept) = operator.push(family, row, floor_seq)? {
                self.write_row(kept, &mut writers, puts).await?;
            }
        }

        for (_, writer) in writers {
            let segments = writer.finish(puts).await?;
            self.result.output_bytes = self
                .result
                .output_bytes
                .saturating_add(segments.iter().map(segment_object_bytes).sum());
            self.result.output_segments.extend(segments);
        }
        Ok(None)
    }

    /// Reports how far a long unit has got, at [`PROGRESS_ROW_INTERVAL`].
    ///
    /// A unit publishes nothing until it's finished, so without this an
    /// operator watching a big namespace would see nothing until the unit
    /// publishes. The counters are the merge's own, so nothing extra is
    /// measured.
    fn report_progress(&mut self) {
        let progress = &mut self.progress;
        if self.result.rows_read < progress.next_rows {
            return;
        }
        progress.next_rows = self.result.rows_read.saturating_add(PROGRESS_ROW_INTERVAL);
        let input_rows = progress.input_rows;
        tracing::info!(
            namespace_id = self.namespace_id.as_str(),
            families = ?self.group.families(),
            rows_read = self.result.rows_read,
            rows_written = self.result.rows_written,
            input_rows,
            output_segments = self.result.output_segments.len(),
            "metadata compaction unit progress"
        );
    }

    /// Fills every iterator that has run out of rows and records what the merge
    /// then contains.
    async fn refill(&mut self, iterators: &mut [MetadataSegmentRowIterator]) -> Result<()> {
        let resident = refill_iterators(
            &MetadataSegmentBlockLoader::new(self.store, None),
            iterators,
            self.run_policy.max_decoded_input_bytes_per_step.get(),
        )
        .await?;
        self.result.peak_resident_blocks = self.result.peak_resident_blocks.max(resident);
        Ok(())
    }

    /// Writes one row a retention operator kept, rolling a segment when its
    /// family's builder fills.
    async fn write_row(
        &mut self,
        (family, row): KeptRow,
        writers: &mut BTreeMap<MetadataRowFamily, MetadataSegmentWriter<'_>>,
        puts: &mut MetadataSegmentPuts<'_, S>,
    ) -> Result<()> {
        self.result.rows_written += 1;
        let writer = writers
            .get_mut(&family)
            .expect("a cluster should write only the families it merges");
        // The slot index's summary and subtree rows have no copy in the child
        // index, so the parity digest covers binding events alone.
        let summary_row = matches!(
            &row,
            MetadataRow::DirentryBinding(binding) if !binding.is_binding_event()
        );
        match index_pair(self.group) {
            Some((canonical, _)) if family == canonical && !summary_row => {
                writer.push(row, &mut |encoded| self.canonical_digest.fold(encoded));
            }
            Some((_, index)) if family == index => {
                writer.push(row, &mut |encoded| self.index_digest.fold(encoded));
            }
            _ => writer.push(row, &mut |_| {}),
        }
        writer.roll_full_segments(puts, self.run_policy).await
    }

    /// Refuses to hand back a run whose secondary index does not contain the same
    /// rows as its canonical family, apart from the canonical family's rows
    /// that the index doesn't store (the slot index's summary and subtree
    /// rows).
    ///
    /// This is the only index-parity check a merge makes, whichever path is
    /// driving it. Comparing the two families outright is not available to a
    /// merge that never keeps them together: the reverse bind index is keyed by child,
    /// so no grouping ever contains a bind row and the reverse row that indexes
    /// it, and the two are judged in different passes. The digests stand in —
    /// each pass folds the rows it wrote into one, order does not matter, and
    /// the two agree at the end exactly when the merge wrote the two families
    /// the same rows.
    ///
    /// The check covers what the merge wrote rather than what it read, which is
    /// the stronger claim: it says the two families dropped in lockstep, not
    /// only that their inputs matched.
    fn refuse_a_run_whose_index_disagrees(&self) -> Result<()> {
        let Some((canonical, index)) = index_pair(self.group) else {
            return Ok(());
        };
        if self.canonical_digest == self.index_digest {
            return Ok(());
        }
        Err(CoreError::NamespaceCorrupt(format!(
            "a metadata merge of {:?} wrote {} `{canonical:?}` rows digesting to `{}` and \
             {} `{index:?}` rows digesting to `{}`; the two families must hold the same rows, so \
             the run it built is not publishable",
            self.group.families(),
            self.canonical_digest.rows,
            self.canonical_digest.spell(),
            self.index_digest.rows,
            self.index_digest.spell(),
        )))
    }
}

/// An order-independent digest of the rows one family was written.
///
/// The combiner is wrapping addition, so the digest depends on the multiset of
/// rows and not on the order they were written in — which is the point,
/// because the two families of an index pair are written in different orders
/// and, for the bind pair, in different passes. This is corruption detection,
/// not a signature: what it has to catch is a merge that wrote a secondary
/// index rows its canonical family does not contain, including a row differing in one
/// field. Nothing durable stores it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RowDigest {
    value: u128,
    rows: u64,
}

impl RowDigest {
    fn fold(&mut self, encoded_row: &[u8]) {
        let digest = Sha256::digest(encoded_row);
        let mut head = [0u8; 16];
        head.copy_from_slice(&digest[..16]);
        self.value = self.value.wrapping_add(u128::from_be_bytes(head));
        self.rows += 1;
    }

    fn spell(&self) -> String {
        format!("{:032x}", self.value)
    }
}

/// The canonical family and secondary index of a group that has one.
fn index_pair(group: MetadataFamilyGroup) -> Option<(MetadataRowFamily, MetadataRowFamily)> {
    match group {
        MetadataFamilyGroup::Bindings => Some((
            MetadataRowFamily::DirentryBinds,
            MetadataRowFamily::DirentryChildBinds,
        )),
        MetadataFamilyGroup::Revisions
        | MetadataFamilyGroup::Inodes
        | MetadataFamilyGroup::Tombstones
        | MetadataFamilyGroup::ActiveDeletions
        | MetadataFamilyGroup::Commits
        | MetadataFamilyGroup::ContentLayouts
        | MetadataFamilyGroup::Attributes
        | MetadataFamilyGroup::Access => None,
    }
}

/// Turns the run ids a spec names back into the manifest's runs.
fn resolve_input_runs<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    spec: &MetadataCompactionSpec,
) -> Result<Vec<MetadataRunManifest>> {
    spec.inputs
        .iter()
        .map(|run_no| {
            segments
                .scan_runs
                .iter()
                .find(|run| run.run_no == *run_no)
                .cloned()
                .ok_or_else(|| {
                    CoreError::NamespaceCorrupt(format!(
                        "a compaction unit names input run `{run_no}`, which the manifest \
                         does not reference"
                    ))
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::RowDigest;
    use loonfs_types::format::manifest::MetadataRow;
    use loonfs_types::{ChangeSeq, DisplayName, InodeId, NameKey};

    fn bind(parent: u64, name: &str, bind_seq: u64) -> MetadataRow {
        MetadataRow::DirentryBinding(crate::metadata::DirentryBindingRecord {
            parent_inode_id: InodeId(u128::from(parent)),
            name_key: NameKey::parse(name).expect("name key"),
            state: loonfs_types::format::manifest::DirentryBindingState::Bound {
                display_name: DisplayName::parse(name).expect("display name"),
                revision: Some(crate::test_support::revision_summary()),
                subtree_seq: None,
            },
            child_inode_id: InodeId(42),
            child_kind: loonfs_types::InodeKind::File,
            child_created_by: loonfs_types::ActorId::loonfs(),
            child_created_at_ms: 4_200,
            committed_seq: ChangeSeq(bind_seq),
            delta_index: 0,
            commit_id: crate::test_support::binding_commit_id(),
            committed_by: loonfs_types::ActorId::loonfs(),
            committed_at_ms: 0,
            bind_position: loonfs_types::format::manifest::DeltaPosition {
                seq: ChangeSeq(bind_seq),
                delta_index: 0,
            },
        })
    }

    fn unbind(parent: u64, name: &str, bind_seq: u64) -> MetadataRow {
        MetadataRow::DirentryBinding(crate::metadata::DirentryBindingRecord {
            parent_inode_id: InodeId(u128::from(parent)),
            name_key: NameKey::parse(name).expect("name key"),
            child_inode_id: InodeId(42),
            child_kind: loonfs_types::InodeKind::File,
            child_created_by: loonfs_types::ActorId::loonfs(),
            child_created_at_ms: 4_200,
            committed_seq: ChangeSeq(bind_seq + 1),
            delta_index: 0,
            commit_id: crate::test_support::binding_commit_id(),
            committed_by: loonfs_types::ActorId::loonfs(),
            committed_at_ms: 0,
            bind_position: loonfs_types::format::manifest::DeltaPosition {
                seq: ChangeSeq(bind_seq + 1),
                delta_index: 0,
            },
            state: loonfs_types::format::manifest::DirentryBindingState::Unbound,
        })
    }

    fn fold_row(digest: &mut RowDigest, row: &MetadataRow) {
        digest.fold(&serde_json::to_vec(row).expect("encode row"));
    }

    #[test]
    fn the_row_digest_ignores_order_and_notices_one_changed_field() {
        let rows = [
            bind(7, "a.txt", 11),
            bind(7, "b.txt", 12),
            unbind(7, "a.txt", 11),
        ];
        let mut forward = RowDigest::default();
        let mut backward = RowDigest::default();
        for row in &rows {
            fold_row(&mut forward, row);
        }
        for row in rows.iter().rev() {
            fold_row(&mut backward, row);
        }
        assert_eq!(forward, backward);

        let mut changed = RowDigest::default();
        for row in [
            bind(7, "a.txt", 11),
            bind(7, "b.txt", 13),
            unbind(7, "a.txt", 11),
        ]
        .iter()
        {
            fold_row(&mut changed, row);
        }
        assert_ne!(forward, changed);

        let mut short = RowDigest::default();
        fold_row(&mut short, &rows[0]);
        assert_ne!(forward, short);
    }
}
