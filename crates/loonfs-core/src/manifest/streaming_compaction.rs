//! The merge engine and the publication of one metadata compaction unit.
//!
//! [`GroupMerge`] merges runs in row-key order, applies retention rules, and
//! writes bounded output segments. It buffers only input blocks, retention
//! state, one segment builder per family, and a bounded wave of encoded outputs.
//! [`finalize_metadata_compaction`] publishes the merged run with epoch
//! fencing. Only base merges may remove rows below the retention floor, and
//! only base merges remove the rows of inodes that an expired deletion hides.
//! Every merge drops superseded layout rows.

use super::build::{MetadataSegmentPuts, MetadataSegmentWriter};
use super::cache::MetadataSegmentCache;
use super::compaction_merge::{
    locality_of, refill_iterators, select_next_iterator, LocalityGrouping,
    MetadataSegmentBlockLoader, MetadataSegmentRowIterator,
};
use super::compaction_retention::{KeptRow, RetentionRule};
use super::compaction_step::{
    build_replacement_manifest, group_run_descriptors, CompactionUnitOutcome, MergePlacement,
    ReplacementOutput,
};
use super::index_block_load::{segment_object_bytes, SegmentWriter};
use super::kept_inodes::{load_kept_inode_filter, KeptInodeFilter};
use super::publish::{update_manifest, ManifestChange};
use super::published::PublishedManifest;
use super::runs::{runs_newest_first, MetadataFamilyGroup, MetadataRunManifest, MetadataRunPolicy};
use super::scan::VerifiedMetadataSegments;
use crate::bloom_filter::BloomFilter;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use crate::limits::METADATA_COMPACTION_BUDGET_MS;
use crate::namespace::control::LoadedManifest;
use crate::namespace::PublicationBasis;
use crate::time::Deadline;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{MetadataRow, MetadataRowFamily, MetadataSegmentRef};
use loonfs_types::CompactorEpoch;
use loonfs_types::{ChangeSeq, MetadataCompactionId, NamespaceId, RunNo};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Rows between two progress lines. A unit that reaches one reads more than a
/// million rows, so a long unit logs a handful of lines rather than one per
/// segment.
const PROGRESS_ROW_INTERVAL: u64 = 1_000_000;

/// Publication attempts one finalization makes before giving up.
///
/// Only an unrelated publication between the load and the manifest
/// put-if-absent costs an attempt, and the next attempt loads the manifest
/// that won. A namespace publishing fast enough to win four in a row is one
/// where running the unit again later is better than spinning
/// here, and running it again is always safe.
const MAX_FINALIZATION_ATTEMPTS: usize = 4;

/// The immutable plan of one compaction unit.
///
/// Everything the unit decides is set here before it starts: which group it
/// rebuilds, which runs it reads, where its output stands, and the retention
/// floor every row is judged against. A unit run again from the same spec
/// against the same durable state produces the same rows, which is what makes
/// a cancelled attempt free to throw away.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataCompactionSpec {
    job_id: MetadataCompactionId,
    group: MetadataFamilyGroup,
    /// A contiguous window of at most eight runs, in oldest-first order.
    inputs: Vec<RunNo>,
    /// Rows those runs list for the group, from their descriptors. What the
    /// unit reports it is about to read. The rows it writes are fewer by
    /// whatever the floor lets go.
    input_rows: u64,
    placement: MergePlacement,
    frozen_floor_seq: ChangeSeq,
}

impl MetadataCompactionSpec {
    pub(super) fn new(
        group: MetadataFamilyGroup,
        inputs: Vec<RunNo>,
        input_rows: u64,
        placement: MergePlacement,
        frozen_floor_seq: ChangeSeq,
    ) -> Self {
        assert!(
            !inputs.is_empty() && inputs.len() <= crate::limits::MAX_COMPACTION_INPUT_RUNS,
            "a compaction plan must have bounded, nonempty input"
        );
        Self {
            job_id: MetadataCompactionId::generate(),
            group,
            inputs,
            input_rows,
            placement,
            frozen_floor_seq,
        }
    }

    /// The family group this unit rebuilds.
    pub(crate) fn group(&self) -> MetadataFamilyGroup {
        self.group
    }

    pub(crate) fn job_id(&self) -> &MetadataCompactionId {
        &self.job_id
    }

    /// The families this unit rebuilds, for a caller reporting what it started.
    pub(crate) fn families(&self) -> &'static [MetadataRowFamily] {
        self.group.families()
    }

    /// How many runs the unit reads.
    pub(crate) fn input_runs(&self) -> usize {
        self.inputs.len()
    }

    /// How many rows those runs hold.
    pub(crate) fn input_rows(&self) -> u64 {
        self.input_rows
    }

    pub(super) fn inputs(&self) -> &[RunNo] {
        &self.inputs
    }

    pub(super) fn frozen_floor_seq(&self) -> ChangeSeq {
        self.frozen_floor_seq
    }

    #[cfg(test)]
    pub(super) fn placement(&self) -> MergePlacement {
        self.placement
    }

    #[cfg(test)]
    pub(super) fn with_frozen_floor_seq(&self, frozen_floor_seq: ChangeSeq) -> Self {
        Self {
            frozen_floor_seq,
            ..self.clone()
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MetadataCompactionCancellation(Arc<Cancellation>);

#[derive(Debug, Default)]
struct Cancellation {
    cancelled: AtomicBool,
    /// Wakes whoever is waiting rather than reading rows. A unit queued behind
    /// its runtime's admission has no block fetch to check the flag between,
    /// so a shutdown would otherwise have to wait for a permit it is trying
    /// to stop needing.
    woken: Notify,
}

impl MetadataCompactionCancellation {
    pub fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::SeqCst);
        self.0.woken.notify_waiters();
    }

    /// Whether the token has been set. What a unit reads between block
    /// fetches, and what a caller about to start work reads first.
    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::SeqCst)
    }

    /// Resolves once the token is set, and stays pending while it is not.
    ///
    /// For a caller waiting on something else — a permit, a queue — that has
    /// to stop waiting when the unit is cancelled.
    pub async fn cancelled(&self) {
        let woken = self.0.woken.notified();
        tokio::pin!(woken);
        // Registered before the flag is read, which is what closes the race
        // with a `cancel` landing between the read and the await: the notify
        // then wakes this waiter rather than passing it by.
        woken.as_mut().enable();
        if self.is_cancelled() {
            return;
        }
        woken.await;
    }
}

/// What one finished merge built, held in memory until the caller publishes
/// it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct MetadataMergeReport {
    /// The output run's segment descriptors, naming the object keys the merge
    /// wrote them to.
    pub(super) output_segments: Vec<MetadataSegmentRef>,
    pub(super) rows_read: u64,
    pub(super) rows_written: u64,
    pub(super) input_bytes: u64,
    pub(super) output_bytes: u64,
    /// The most decoded input blocks the merge's iterators held at once, and
    /// the most rows one retention operator held. These are what bound the
    /// merge's memory, so tests assert they do not follow the size of the
    /// group, of one inode's history, or of one slot's versions.
    pub(super) peak_resident_blocks: usize,
    pub(super) peak_operator_rows: usize,
}

/// Rebuilds the family group and runs selected by `spec`. `segments` must come
/// from the manifest used to create the specification.
pub(super) async fn run_metadata_compaction<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    output_cache: Option<&MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    policy: MetadataRunPolicy,
    cancellation: &MetadataCompactionCancellation,
) -> Result<std::result::Result<MetadataMergeReport, CompactionUnitOutcome>> {
    let input_runs = resolve_input_runs(segments, spec)?;
    let kept_inodes = match load_kept_inode_filter(
        segments,
        namespace_id,
        spec.group,
        spec.placement,
        cancellation,
    )
    .await?
    {
        Ok(kept_inodes) => kept_inodes,
        Err(stopped) => return Ok(Err(stopped)),
    };
    let merge = GroupMerge::new(
        segments,
        output_cache,
        namespace_id,
        spec.group,
        spec.placement,
        spec.frozen_floor_seq,
        policy,
        input_runs,
        kept_inodes,
        ProgressReporter::new(spec.input_rows()),
    );
    merge.run(&MergeControl { cancellation }).await
}

pub(super) struct CompactionPublication {
    pub(super) compactor_epoch: CompactorEpoch,
    pub(super) compaction: Deadline,
    pub(super) publication: Deadline,
}

impl CompactionPublication {
    fn expired(&self) -> bool {
        self.compaction.elapsed_ms() > METADATA_COMPACTION_BUDGET_MS
    }
}

/// Swaps the rebuilt run in for the input it replaces.
///
/// The publication starts from `basis`, the manifest that the unit planned
/// from, or loads the current manifest when `basis` is `None`. Each attempt
/// checks the current manifest that `update_manifest` found.
/// That manifest must still contain the segments the unit merged, and the
/// replacement keeps every run published meanwhile. A lost manifest put is
/// retried over the manifest that won, while changes to the unit's input
/// abandon the output.
///
/// The publication budget starts during finalization rather than at the start
/// of the potentially long rebuild.
#[allow(
    clippy::too_many_arguments,
    reason = "finalization takes the unit's basis, plan, inputs, output, and publication clock"
)]
pub(super) async fn finalize_metadata_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    basis: Option<PublicationBasis>,
    spec: &MetadataCompactionSpec,
    input_keys: &BTreeSet<String>,
    result: MetadataMergeReport,
    cancellation: &MetadataCompactionCancellation,
    publication: &CompactionPublication,
) -> Result<CompactionUnitOutcome> {
    let output_segments = &result.output_segments;
    // `None` stands for the publication, and `Some` for every other outcome.
    let finalized = update_manifest(
        store,
        namespace_id,
        basis,
        &publication.publication,
        MAX_FINALIZATION_ATTEMPTS,
        |current: LoadedManifest| async move {
            let settled = |outcome| Ok(ManifestChange::Finished(Some(outcome)));
            if publication.expired() {
                return settled(CompactionUnitOutcome::Abandoned);
            }
            if cancellation.is_cancelled() {
                return settled(CompactionUnitOutcome::Cancelled);
            }
            let previous = current.state.envelope.payload();
            if previous.status.is_deleted() {
                return settled(CompactionUnitOutcome::Abandoned);
            }
            if previous.compactor_epoch != publication.compactor_epoch {
                return settled(CompactionUnitOutcome::Fenced);
            }
            if input_segment_keys(&runs_newest_first(previous), spec).as_ref() != Some(input_keys) {
                tracing::info!(
                    namespace_id = namespace_id.as_str(),
                    families = ?spec.families(),
                    "metadata compaction unit abandoned: its input runs moved while it ran"
                );
                return settled(CompactionUnitOutcome::Abandoned);
            }
            let surviving = previous
                .runs
                .iter()
                .filter_map(|run| {
                    let mut run = run.clone();
                    run.segments.retain(|descriptor| {
                        !input_keys.contains(&metadata_segment_object_key(descriptor))
                    });
                    (!run.segments.is_empty()).then_some(run)
                })
                .collect();
            let manifest = build_replacement_manifest(
                namespace_id,
                previous,
                surviving,
                ReplacementOutput {
                    segments: output_segments.clone(),
                    placement: spec.placement,
                },
                spec.frozen_floor_seq(),
            )?;
            // The last check before the swap that makes this output reader
            // truth. Everything above it is reads and objects nothing references.
            if cancellation.is_cancelled() {
                return settled(CompactionUnitOutcome::Cancelled);
            }
            if publication.expired() {
                return settled(CompactionUnitOutcome::Abandoned);
            }
            Ok(ManifestChange::Next(Box::new(manifest), None))
        },
    )
    .await;
    match finalized {
        Ok((Some(outcome), _, _)) => Ok(outcome),
        Ok((None, published, _)) => Ok(CompactionUnitOutcome::Published {
            group: spec.group(),
            manifest: PublishedManifest::new(
                &published.state.envelope,
                Some(publication.publication.origin()),
            ),
            input_runs: spec.input_runs(),
            rows_read: result.rows_read,
            rows_written: result.rows_written,
            input_bytes: result.input_bytes,
            output_bytes: result.output_bytes,
            output_segments: result.output_segments.len(),
        }),
        // Every attempt lost its manifest number to another publication.
        Err(CoreError::StoreContention { .. }) => {
            tracing::info!(
                namespace_id = namespace_id.as_str(),
                families = ?spec.families(),
                attempts = MAX_FINALIZATION_ATTEMPTS,
                "metadata compaction unit lost every publication attempt; a later unit plans it again"
            );
            Ok(CompactionUnitOutcome::Abandoned)
        }
        // The namespace's objects were removed while the unit ran.
        Err(CoreError::ControlObjectLoad(ControlObjectLoadError::MissingObject { .. })) => {
            Ok(CompactionUnitOutcome::Abandoned)
        }
        Err(error) => Err(error),
    }
}

/// The object keys the spec's runs hold for its group, or `None` when the
/// manifest no longer references one of those runs at all.
///
/// Segments are immutable and their keys are generated, so two manifests
/// agreeing on this set agree on every row the unit read. Runs the manifest
/// gained meanwhile are not in it (the unit never read them), and they
/// survive the publication untouched.
pub(super) fn input_segment_keys(
    runs: &[MetadataRunManifest],
    spec: &MetadataCompactionSpec,
) -> Option<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    for run_no in spec.inputs() {
        let run = runs.iter().find(|run| run.run_no == *run_no)?;
        keys.extend(group_run_descriptors(run, spec.group()).map(metadata_segment_object_key));
    }
    Some(keys)
}

/// One set of families the engine merges and judges together, and how it
/// groups their rows while doing it.
pub(super) struct RetentionCluster {
    pub(super) families: &'static [MetadataRowFamily],
    pub(super) locality: LocalityGrouping,
    pub(super) rule: RetentionRule,
}

// Both indexes put an edge's parent, name, and child ahead of its position, so
// positions run oldest first only within one edge.
const BINDINGS_CLUSTERS: [RetentionCluster; 2] = [
    RetentionCluster {
        families: &[MetadataRowFamily::DirentryBinds],
        locality: LocalityGrouping::LeadingKeyComponents(3),
        rule: RetentionRule::Bindings,
    },
    RetentionCluster {
        families: &[MetadataRowFamily::DirentryChildBinds],
        locality: LocalityGrouping::LeadingKeyComponents(3),
        rule: RetentionRule::Bindings,
    },
];

/// A family no rule ever drops a row from, rewritten in key order.
const fn row_cluster(families: &'static [MetadataRowFamily]) -> RetentionCluster {
    RetentionCluster {
        families,
        locality: LocalityGrouping::Row,
        rule: RetentionRule::KeepEveryRow,
    }
}

const LAYOUT_CLUSTERS: [RetentionCluster; 1] = [RetentionCluster {
    families: &[MetadataRowFamily::ContentLayouts],
    locality: LocalityGrouping::LeadingKeyComponents(1),
    rule: RetentionRule::FirstPerGroup,
}];
const INODE_CLUSTERS: [RetentionCluster; 1] = [row_cluster(&[MetadataRowFamily::Inodes])];
const TOMBSTONE_CLUSTERS: [RetentionCluster; 1] = [row_cluster(&[MetadataRowFamily::Tombstones])];
const COMMIT_CLUSTERS: [RetentionCluster; 2] = [
    RetentionCluster {
        families: &[MetadataRowFamily::Commits],
        locality: LocalityGrouping::Row,
        rule: RetentionRule::CommitHistory,
    },
    RetentionCluster {
        families: &[MetadataRowFamily::CommitReceipts],
        locality: LocalityGrouping::Row,
        rule: RetentionRule::CommitHistory,
    },
];
const ACTIVE_DELETION_CLUSTERS: [RetentionCluster; 1] = [RetentionCluster {
    families: &[MetadataRowFamily::ActiveDeletions],
    locality: LocalityGrouping::LeadingKeyComponents(2),
    rule: RetentionRule::ActiveDeletions,
}];
const REVISION_CLUSTERS: [RetentionCluster; 1] = [RetentionCluster {
    families: &[MetadataRowFamily::Revisions],
    locality: LocalityGrouping::LeadingKeyComponents(1),
    rule: RetentionRule::WholeState,
}];
const ATTRIBUTE_CLUSTERS: [RetentionCluster; 1] = [RetentionCluster {
    families: &[MetadataRowFamily::Attributes],
    locality: LocalityGrouping::LeadingKeyComponents(1),
    rule: RetentionRule::WholeState,
}];
const ACCESS_CLUSTERS: [RetentionCluster; 1] = [RetentionCluster {
    families: &[MetadataRowFamily::Access],
    locality: LocalityGrouping::LeadingKeyComponents(1),
    rule: RetentionRule::WholeState,
}];

pub(super) fn retention_clusters(group: MetadataFamilyGroup) -> &'static [RetentionCluster] {
    match group {
        MetadataFamilyGroup::Bindings => &BINDINGS_CLUSTERS,
        MetadataFamilyGroup::Revisions => &REVISION_CLUSTERS,
        MetadataFamilyGroup::Inodes => &INODE_CLUSTERS,
        MetadataFamilyGroup::Tombstones => &TOMBSTONE_CLUSTERS,
        MetadataFamilyGroup::ActiveDeletions => &ACTIVE_DELETION_CLUSTERS,
        MetadataFamilyGroup::Commits => &COMMIT_CLUSTERS,
        MetadataFamilyGroup::ContentLayouts => &LAYOUT_CLUSTERS,
        MetadataFamilyGroup::Attributes => &ATTRIBUTE_CLUSTERS,
        MetadataFamilyGroup::Access => &ACCESS_CLUSTERS,
    }
}

/// Merging one family group: the shared engine, whatever is driving it.
struct GroupMerge<'a, S: ObjectStore + ?Sized> {
    store: &'a S,
    /// The cache that receives the segments the merge writes. The merge
    /// reads its input without any cache.
    output_cache: Option<&'a MetadataSegmentCache>,
    namespace_id: &'a NamespaceId,
    group: MetadataFamilyGroup,
    /// Where the output stands in the group and whether rows may be dropped.
    placement: MergePlacement,
    frozen_floor_seq: ChangeSeq,
    /// The basis manifest's acknowledged floor, at or below which a listed
    /// deletion that no undelete cancelled has expired.
    acknowledged_floor_seq: ChangeSeq,
    policy: MetadataRunPolicy,
    input_runs: Vec<MetadataRunManifest>,
    basis_runs: &'a [MetadataRunManifest],
    output_run_no: Option<RunNo>,
    chain_filter: ChainFilter,
    kept_inodes: KeptInodeFilter,
    result: MetadataMergeReport,
    canonical_digest: RowDigest,
    index_digest: RowDigest,
    /// The last input row key seen in each family, which is what lets the merge
    /// refuse a family that holds one row key twice. One string per family.
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
    #[allow(
        clippy::too_many_arguments,
        reason = "construction keeps one merge's coordinated state explicit"
    )]
    fn new(
        basis: &'a VerifiedMetadataSegments<'_, S>,
        output_cache: Option<&'a MetadataSegmentCache>,
        namespace_id: &'a NamespaceId,
        group: MetadataFamilyGroup,
        placement: MergePlacement,
        frozen_floor_seq: ChangeSeq,
        policy: MetadataRunPolicy,
        input_runs: Vec<MetadataRunManifest>,
        kept_inodes: KeptInodeFilter,
        progress: ProgressReporter,
    ) -> Self {
        let input_bytes = input_runs
            .iter()
            .flat_map(|run| group_run_descriptors(run, group))
            .map(segment_object_bytes)
            .sum();
        Self {
            store: basis.store,
            basis_runs: &basis.scan_runs,
            output_run_no: basis
                .manifest
                .as_ref()
                .map(|manifest| manifest.payload().next_run_no),
            acknowledged_floor_seq: basis.manifest.as_ref().map_or(ChangeSeq(0), |manifest| {
                manifest.payload().acknowledged_floor_seq
            }),
            chain_filter: ChainFilter::NotNeeded,
            kept_inodes,
            output_cache,
            namespace_id,
            group,
            placement,
            frozen_floor_seq,
            policy,
            input_runs,
            result: MetadataMergeReport {
                input_bytes,
                ..MetadataMergeReport::default()
            },
            canonical_digest: RowDigest::default(),
            index_digest: RowDigest::default(),
            last_input_key_by_family: BTreeMap::new(),
            progress,
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
        let mut filter = BloomFilter::new(
            expected_ids,
            self.output_run_no
                .expect("a layout compaction should have a manifest basis")
                .0,
        );
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
                    self.policy.max_decoded_input_bytes_per_step.get(),
                )
                .await?;
                if iterator[0].head().is_none() {
                    break;
                }
                if let MetadataRow::FileRevision(row) = iterator[0].take_head() {
                    // The filter is sized for the basis's revision rows, so a
                    // refused insert means the segments hold more revision
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
                if let Some(kept) = operator.close_group(floor_seq)? {
                    self.write_row(kept, &mut writers, puts).await?;
                }
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
            if let Some(kept) = operator.take_floor_value_before(&row, floor_seq)? {
                self.write_row(kept, &mut writers, puts).await?;
            }
            let kept = operator.push(family, row, floor_seq)?;
            self.result.peak_operator_rows =
                self.result.peak_operator_rows.max(operator.held_rows());
            if let Some(kept) = kept {
                self.write_row(kept, &mut writers, puts).await?;
            }
        }
        if let Some(kept) = operator.close_group(floor_seq)? {
            self.write_row(kept, &mut writers, puts).await?;
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
    /// then holds.
    async fn refill(&mut self, iterators: &mut [MetadataSegmentRowIterator]) -> Result<()> {
        let resident = refill_iterators(
            &MetadataSegmentBlockLoader::new(self.store, None),
            iterators,
            self.policy.max_decoded_input_bytes_per_step.get(),
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
        match index_pair(self.group) {
            Some((canonical, _)) if family == canonical => {
                writer.push(row, &mut |encoded| self.canonical_digest.fold(encoded));
            }
            Some((_, index)) if family == index => {
                writer.push(row, &mut |encoded| self.index_digest.fold(encoded));
            }
            _ => writer.push(row, &mut |_| {}),
        }
        writer.roll_full_segments(puts, self.policy).await
    }

    /// Refuses to hand back a run whose secondary index does not hold the same
    /// rows as its canonical family.
    ///
    /// This is the only index-parity check a merge makes, whichever path is
    /// driving it. Comparing the two families outright is not available to a
    /// merge that never holds them: the reverse bind index is keyed by child,
    /// so no grouping ever holds a bind row and the reverse row that indexes
    /// it, and the two are decided in different passes. The digests stand in —
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
/// index rows its canonical family does not hold, including a row differing in one
/// field. Nothing durable carries it.
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

/// The canonical family and secondary index of a group that carries one.
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
            parent_inode_id: InodeId(parent),
            name_key: NameKey::parse(name).expect("name key"),
            state: loonfs_types::format::manifest::DirentryBindingState::Bound {
                display_name: DisplayName::parse(name).expect("display name"),
            },
            child_inode_id: InodeId(42),
            child_kind: loonfs_types::InodeKind::File,
            child_created_by: loonfs_types::ActorId::loonfs(),
            child_created_at_ms: 4_200,
            committed_seq: ChangeSeq(bind_seq),
            delta_index: 0,
        })
    }

    fn unbind(parent: u64, name: &str, bind_seq: u64) -> MetadataRow {
        MetadataRow::DirentryBinding(crate::metadata::DirentryBindingRecord {
            parent_inode_id: InodeId(parent),
            name_key: NameKey::parse(name).expect("name key"),
            child_inode_id: InodeId(42),
            child_kind: loonfs_types::InodeKind::File,
            child_created_by: loonfs_types::ActorId::loonfs(),
            child_created_at_ms: 4_200,
            committed_seq: ChangeSeq(bind_seq + 1),
            delta_index: 0,
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
