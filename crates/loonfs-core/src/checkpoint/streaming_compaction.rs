//! Streaming metadata merge engine.
//!
//! [`GroupMerge`] merges runs in row-key order, applies retention rules, and
//! writes bounded output segments. It buffers only input blocks, retention
//! state, and one segment builder per family.
//!
//! [`merge_group_in_step`] runs within a bounded maintenance pass.
//! [`run_metadata_compaction_job`] writes direct output with epoch fencing.
//! Only base merges may remove rows below the
//! retention floor; delta merges preserve every row.

use super::block_fetch::segment_object_len;
use super::block_load::SessionBlockMemo;
use super::build::MetadataSegmentWriter;
use super::compaction_merge::{
    locality_of, refill_iterators, select_next_iterator, LocalityGrouping,
    MetadataSegmentBlockLoader, MetadataSegmentRowIterator,
};
use super::compaction_retention::{KeptRow, RetentionRule};
use super::error::ManifestLoadError;
use super::load::load_manifest_segments;
use super::publish::{publish_manifest, ManifestPublicationOutcome};
use super::reorganize::{
    build_replacement_manifest, group_run_descriptors, MergePlacement, ReplacementOutput,
};
use super::runs::{MetadataFamilyGroup, MetadataLsmPolicy, MetadataRunManifest};
use super::scan::VerifiedMetadataSegments;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::limits::METADATA_COMPACTION_BUDGET_MS;
use crate::namespace::control::load_current_manifest_if_present;
use crate::time::{Deadline, StdMonotonicTimer};
use loonfs_api::wire::manifest::{MetadataRowFamily, MetadataSegmentRef};
use loonfs_api::{ChangeSeq, ManifestNo, MetadataCompactionId, NamespaceId, RunNo};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Rows between two progress lines. A job that needs one at all is bigger
/// than a step's whole row budget, so this is coarse on purpose: a handful of
/// lines over a long job rather than one per segment.
const PROGRESS_ROW_INTERVAL: u64 = 1_000_000;

/// Publication attempts one finalization makes before giving up.
///
/// Only an unrelated publication landing between the reload and the manifest
/// put-if-absent costs an attempt, and the reload is what the next attempt
/// takes the race against. A namespace publishing fast enough to win four in
/// a row is one where re-running the job later is the better answer than
/// spinning here, and re-running is always safe.
const MAX_FINALIZATION_ATTEMPTS: usize = 4;

/// The immutable plan of one streaming compaction.
///
/// Everything the job decides is fixed here before it starts: which group it
/// rebuilds, which runs it reads, where its output stands, and the retention
/// floor every row is judged against. A job re-run from the same spec against
/// the same durable state produces the same rows, which is what makes a
/// cancelled attempt free to throw away.
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCompactionSpec {
    job_id: MetadataCompactionId,
    group: MetadataFamilyGroup,
    /// A contiguous window of at most eight runs, in oldest-first order.
    inputs: Vec<RunNo>,
    /// Rows those runs hold for the group, from their descriptors. What the
    /// job reports it is about to read; the rows it writes are fewer by
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
            !inputs.is_empty() && inputs.len() <= super::reorganize::MAX_COMPACTION_INPUT_RUNS,
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

    /// The family group this job rebuilds.
    pub fn group(&self) -> MetadataFamilyGroup {
        self.group
    }

    pub fn job_id(&self) -> &MetadataCompactionId {
        &self.job_id
    }

    /// The families this job rebuilds, for a caller reporting what it started.
    pub fn families(&self) -> &'static [MetadataRowFamily] {
        self.group.families()
    }

    /// How many runs the job reads.
    pub fn input_runs(&self) -> usize {
        self.inputs.len()
    }

    /// How many rows those runs hold.
    pub fn input_rows(&self) -> u64 {
        self.input_rows
    }

    pub(super) fn inputs(&self) -> &[RunNo] {
        &self.inputs
    }

    pub(super) fn frozen_floor_seq(&self) -> ChangeSeq {
        self.frozen_floor_seq
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
    /// Wakes whoever is waiting rather than reading rows. A job queued behind
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

    /// Whether the token has been set. What a job reads between block
    /// fetches, and what a caller about to start work reads first.
    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::SeqCst)
    }

    /// Resolves once the token is set, and stays pending while it is not.
    ///
    /// For a caller waiting on something else — a permit, a queue — that has
    /// to stop waiting when the job is cancelled.
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
pub(super) struct MetadataMergeResult {
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

#[allow(
    clippy::too_many_arguments,
    reason = "one merge's inputs, named rather than grouped into a second shape"
)]
pub(super) async fn merge_group_in_step<S: ObjectStore + ?Sized>(
    store: &S,
    index_memo: Option<&SessionBlockMemo>,
    namespace_id: &NamespaceId,
    group: MetadataFamilyGroup,
    runs: &[MetadataRunManifest],
    placement: MergePlacement,
    frozen_floor_seq: ChangeSeq,
    policy: MetadataLsmPolicy,
) -> Result<MetadataMergeResult> {
    let merge = GroupMerge::new(
        store,
        index_memo,
        namespace_id,
        group,
        placement,
        frozen_floor_seq,
        policy,
        runs.to_vec(),
        // A step-contained merge is bounded by the step's input budgets and
        // ends in the step's own publication, so it has nothing to report
        // progress about.
        None,
    );
    let mut control = MergeControl { cancellation: None };
    Ok(merge
        .run(&mut control)
        .await?
        .expect("a step merge should have no cancellation token"))
}

/// Rebuilds the family group and runs selected by `spec`. `segments` must come
/// from the manifest used to create the specification.
pub(super) async fn run_metadata_compaction<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    policy: MetadataLsmPolicy,
    cancellation: &MetadataCompactionCancellation,
) -> Result<std::result::Result<MetadataMergeResult, MetadataCompactionJobOutcome>> {
    let merge = GroupMerge::new(
        segments.store,
        None,
        namespace_id,
        spec.group,
        spec.placement,
        spec.frozen_floor_seq,
        policy,
        resolve_input_runs(segments, spec)?,
        Some(ProgressReporter::new(spec.input_rows())),
    );
    let mut control = MergeControl {
        cancellation: Some(cancellation),
    };
    merge.run(&mut control).await
}

/// How one background compaction job ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataCompactionJobOutcome {
    /// The rebuilt group replaced its input in a published manifest.
    Published {
        manifest_no: ManifestNo,
        rows_read: u64,
        rows_written: u64,
        input_bytes: u64,
        output_bytes: u64,
        output_segments: usize,
    },
    /// The caller stopped the job before publication.
    Cancelled,
    /// Inputs changed, the time bound passed, or publication retries were exhausted.
    Abandoned,
    /// Another process claimed the namespace compactor role.
    Fenced,
}

pub(crate) async fn run_metadata_compaction_job<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    compactor_epoch: u64,
    spec: &MetadataCompactionSpec,
    policy: MetadataLsmPolicy,
    cancellation: &MetadataCompactionCancellation,
) -> Result<MetadataCompactionJobOutcome> {
    let timer = Arc::new(StdMonotonicTimer::default());
    let compaction = Deadline::start(timer.clone());
    let Some(segments) = load_current_manifest_segments(store, namespace_id).await? else {
        return Ok(MetadataCompactionJobOutcome::Abandoned);
    };
    // What the job is about to read, recorded before it reads anything.
    // Finalization compares the manifest against this, so the run it publishes
    // stands in for exactly the segments it merged.
    let Some(input_keys) = input_segment_keys(&segments, spec) else {
        return Ok(MetadataCompactionJobOutcome::Abandoned);
    };
    tracing::info!(
        namespace_id = namespace_id.as_str(),
        job_id = spec.job_id().as_str(),
        families = ?spec.families(),
        input_runs = spec.input_runs(),
        input_rows = spec.input_rows(),
        frozen_floor_seq = spec.frozen_floor_seq().0,
        "streaming metadata compaction started"
    );

    let outcome =
        match run_metadata_compaction(&segments, namespace_id, spec, policy, cancellation).await? {
            Ok(result) => {
                drop(segments);
                finalize_metadata_compaction(
                    store,
                    namespace_id,
                    spec,
                    &input_keys,
                    result,
                    cancellation,
                    &CompactionPublication {
                        compactor_epoch,
                        compaction,
                        publication: Deadline::start(timer),
                    },
                )
                .await?
            }
            Err(stopped) => stopped,
        };
    log_metadata_compaction_outcome(namespace_id, spec, &outcome);
    Ok(outcome)
}

fn log_metadata_compaction_outcome(
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    outcome: &MetadataCompactionJobOutcome,
) {
    match outcome {
        MetadataCompactionJobOutcome::Published {
            manifest_no,
            rows_read,
            rows_written,
            input_bytes,
            output_bytes,
            output_segments,
        } => tracing::info!(
            namespace_id = namespace_id.as_str(),
            job_id = spec.job_id().as_str(),
            families = ?spec.families(),
            rows_read,
            rows_written,
            input_bytes,
            output_bytes,
            output_segments,
            manifest_no = manifest_no.0,
            "streaming metadata compaction published"
        ),
        MetadataCompactionJobOutcome::Cancelled => tracing::info!(
            namespace_id = namespace_id.as_str(),
            job_id = spec.job_id().as_str(),
            families = ?spec.families(),
            "streaming metadata compaction cancelled"
        ),
        MetadataCompactionJobOutcome::Abandoned => tracing::info!(
            namespace_id = namespace_id.as_str(),
            job_id = spec.job_id().as_str(),
            families = ?spec.families(),
            "streaming metadata compaction abandoned"
        ),
        MetadataCompactionJobOutcome::Fenced => tracing::warn!(
            namespace_id = namespace_id.as_str(),
            job_id = spec.job_id().as_str(),
            families = ?spec.families(),
            "streaming metadata compaction fenced"
        ),
    }
}

pub(super) struct CompactionPublication {
    pub(super) compactor_epoch: u64,
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
/// The current manifest must still contain the segments used by the job. New
/// runs are preserved. Unrelated manifest publication conflicts are retried, while
/// changes to the job's input abandon the output.
///
/// The publication budget starts during finalization rather than at the start
/// of the potentially long rebuild. Each attempt reloads the current manifest.
pub(super) async fn finalize_metadata_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    input_keys: &BTreeSet<String>,
    result: MetadataMergeResult,
    cancellation: &MetadataCompactionCancellation,
    publication: &CompactionPublication,
) -> Result<MetadataCompactionJobOutcome> {
    let rows_read = result.rows_read;
    let rows_written = result.rows_written;
    let input_bytes = result.input_bytes;
    let output_bytes = result.output_bytes;
    let output_segments = result.output_segments.len();
    for attempt in 1..=MAX_FINALIZATION_ATTEMPTS {
        if publication.expired() {
            return Ok(MetadataCompactionJobOutcome::Abandoned);
        }
        if cancellation.is_cancelled() {
            return Ok(MetadataCompactionJobOutcome::Cancelled);
        }
        let Some(current_manifest) = load_current_manifest_if_present(store, namespace_id)
            .await
            .map_err(CoreError::ControlObjectLoad)?
            .filter(|loaded| !loaded.state.envelope.payload().status.is_deleted())
            .map(|loaded| loaded.state)
        else {
            return Ok(MetadataCompactionJobOutcome::Abandoned);
        };
        if current_manifest.compactor_epoch() != publication.compactor_epoch {
            return Ok(MetadataCompactionJobOutcome::Fenced);
        }
        let segments = load_manifest_segments(store, None, &current_manifest.manifest()).await?;
        if input_segment_keys(&segments, spec).as_ref() != Some(input_keys) {
            tracing::info!(
                namespace_id = namespace_id.as_str(),
                families = ?spec.families(),
                "streaming metadata compaction abandoned: its input runs moved while it ran"
            );
            return Ok(MetadataCompactionJobOutcome::Abandoned);
        }

        let previous = segments.manifest();
        let surviving = previous
            .payload()
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
                segments: result.output_segments.clone(),
                placement: spec.placement,
            },
            spec.frozen_floor_seq(),
        )?;

        // The last check before the swap that makes this output reader
        // truth. Everything above it is reads and objects nothing references.
        if cancellation.is_cancelled() {
            return Ok(MetadataCompactionJobOutcome::Cancelled);
        }
        if publication.expired() {
            return Ok(MetadataCompactionJobOutcome::Abandoned);
        }
        publication
            .publication
            .ensure_metadata_publication_budget(namespace_id)?;
        let manifest_no = manifest.envelope().payload().manifest_no;
        let published = publish_manifest(store, manifest, &publication.publication).await?;
        drop(segments);
        let lost_to = match published {
            ManifestPublicationOutcome::Published(_) => {
                return Ok(MetadataCompactionJobOutcome::Published {
                    manifest_no,
                    rows_read,
                    rows_written,
                    input_bytes,
                    output_bytes,
                    output_segments,
                })
            }
            ManifestPublicationOutcome::CoveredByCurrent(current)
            | ManifestPublicationOutcome::PredecessorChanged(current)
                if current.compactor_epoch() != publication.compactor_epoch =>
            {
                return Ok(MetadataCompactionJobOutcome::Fenced);
            }
            ManifestPublicationOutcome::CoveredByCurrent(_) => "covered_by_current",
            ManifestPublicationOutcome::PredecessorChanged(_) => "predecessor_changed",
        };
        tracing::debug!(
            namespace_id = namespace_id.as_str(),
            families = ?spec.families(),
            attempt,
            attempts = MAX_FINALIZATION_ATTEMPTS,
            lost_to,
            "a streaming metadata compaction lost its finalizing publication; reloading"
        );
    }
    tracing::info!(
        namespace_id = namespace_id.as_str(),
        families = ?spec.families(),
        attempts = MAX_FINALIZATION_ATTEMPTS,
        "streaming metadata compaction lost every publication attempt; a later step plans it \
         again"
    );
    Ok(MetadataCompactionJobOutcome::Abandoned)
}

/// Loads the current manifest segments, or `None` if the namespace is absent.
async fn load_current_manifest_segments<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
) -> Result<Option<VerifiedMetadataSegments<'a, S>>> {
    let Some(current_manifest) = load_current_manifest_if_present(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?
        .map(|loaded| loaded.state)
    else {
        return Ok(None);
    };
    load_manifest_segments(store, None, &current_manifest.manifest())
        .await
        .map(Some)
}

/// The object keys the spec's runs hold for its group, or `None` when the
/// manifest no longer references one of those runs at all.
///
/// Segments are immutable and their keys are generated, so two manifests
/// agreeing on this set agree on every row the job read. Runs the manifest
/// gained meanwhile are not in it — the job never read them, and they survive
/// the publication untouched.
pub(super) fn input_segment_keys<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    spec: &MetadataCompactionSpec,
) -> Option<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    for run_no in spec.inputs() {
        let run = segments
            .scan_runs
            .iter()
            .find(|run| run.run_no == *run_no)?;
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

const BINDINGS_CLUSTERS: [RetentionCluster; 2] = [
    RetentionCluster {
        families: &[MetadataRowFamily::DirentryBinds],
        locality: LocalityGrouping::LeadingKeyComponents(2),
        rule: RetentionRule::Bindings,
    },
    RetentionCluster {
        families: &[MetadataRowFamily::DirentryChildBinds],
        locality: LocalityGrouping::LeadingKeyComponents(1),
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

/// Revision rows are never dropped, so they are rewritten in key order.
const REVISION_CLUSTERS: [RetentionCluster; 1] = [row_cluster(&[MetadataRowFamily::Revisions])];
const PUBLICATION_CLUSTERS: [RetentionCluster; 1] =
    [row_cluster(&[MetadataRowFamily::ContentPublications])];
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
        MetadataFamilyGroup::ContentPublications => &PUBLICATION_CLUSTERS,
        MetadataFamilyGroup::Attributes => &ATTRIBUTE_CLUSTERS,
        MetadataFamilyGroup::Access => &ACCESS_CLUSTERS,
    }
}

/// Merging one family group: the shared engine, whatever is driving it.
struct GroupMerge<'a, S: ObjectStore + ?Sized> {
    store: &'a S,
    index_memo: Option<&'a SessionBlockMemo>,
    namespace_id: &'a NamespaceId,
    group: MetadataFamilyGroup,
    /// Where the output stands in the group and whether rows may be dropped.
    placement: MergePlacement,
    frozen_floor_seq: ChangeSeq,
    policy: MetadataLsmPolicy,
    input_runs: Vec<MetadataRunManifest>,
    result: MetadataMergeResult,
    canonical_digest: RowDigest,
    index_digest: RowDigest,
    /// The last input row key seen in each family, which is what lets the merge
    /// refuse a family that holds one row key twice. One string per family.
    last_input_key_by_family: BTreeMap<MetadataRowFamily, String>,
    /// Progress state for an unbounded background merge. `None` for a merge
    /// short enough to have nothing to report.
    progress: Option<ProgressReporter>,
}

struct MergeControl<'a> {
    cancellation: Option<&'a MetadataCompactionCancellation>,
}

impl MergeControl<'_> {
    fn cancellation(&self) -> Option<MetadataCompactionJobOutcome> {
        self.cancellation
            .is_some_and(|cancellation| cancellation.is_cancelled())
            .then_some(MetadataCompactionJobOutcome::Cancelled)
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
        store: &'a S,
        index_memo: Option<&'a SessionBlockMemo>,
        namespace_id: &'a NamespaceId,
        group: MetadataFamilyGroup,
        placement: MergePlacement,
        frozen_floor_seq: ChangeSeq,
        policy: MetadataLsmPolicy,
        input_runs: Vec<MetadataRunManifest>,
        progress: Option<ProgressReporter>,
    ) -> Self {
        let input_bytes = input_runs
            .iter()
            .flat_map(|run| group_run_descriptors(run, group))
            .map(segment_object_len)
            .sum();
        Self {
            store,
            index_memo,
            namespace_id,
            group,
            placement,
            frozen_floor_seq,
            policy,
            input_runs,
            result: MetadataMergeResult {
                input_bytes,
                ..MetadataMergeResult::default()
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
        control: &mut MergeControl<'_>,
    ) -> Result<std::result::Result<MetadataMergeResult, MetadataCompactionJobOutcome>> {
        for cluster in retention_clusters(self.group) {
            if let Some(stopped) = self.run_cluster(cluster, control).await? {
                return Ok(Err(stopped));
            }
        }
        self.refuse_a_run_whose_index_disagrees()?;
        // A token set after the final cluster still owns the outcome. The
        // caller must not re-derive cancellation from a stale Completed value.
        if let Some(stop) = control.cancellation() {
            return Ok(Err(stop));
        }
        Ok(Ok(self.result))
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
            match row_key.cmp(previous.as_str()) {
                std::cmp::Ordering::Greater => {}
                std::cmp::Ordering::Equal => {
                    return Err(CoreError::NamespaceCorrupt(format!(
                        "metadata family `{family:?}` contains duplicate row key `{row_key}`; \
                         refusing to merge it into a run"
                    )));
                }
                std::cmp::Ordering::Less => {
                    return Err(CoreError::Internal(format!(
                        "metadata merge read row key `{row_key}` after `{previous}` in family \
                         `{family:?}`"
                    )));
                }
            }
        }
        self.last_input_key_by_family
            .insert(family, row_key.to_owned());
        Ok(())
    }

    /// Which rule decides this cluster's rows.
    ///
    /// Dropping is only visibility-preserving over a window that starts at the
    /// group's oldest run, and the placement is what records that
    /// ([`MergePlacement`]). A merge above the base merges its window exactly
    /// as it stands, so every cluster keeps every row.
    fn rule_for(&self, cluster: &RetentionCluster) -> RetentionRule {
        if self.placement.may_drop_rows_below_the_retention_floor() {
            cluster.rule
        } else {
            RetentionRule::KeepEveryRow
        }
    }

    /// Merges one cluster end to end.
    async fn run_cluster(
        &mut self,
        cluster: &RetentionCluster,
        control: &mut MergeControl<'_>,
    ) -> Result<Option<MetadataCompactionJobOutcome>> {
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
        let mut operator = rule.operator();
        let mut locality: Option<String> = None;
        loop {
            if let Some(stop) = control.cancellation() {
                return Ok(Some(stop));
            }
            self.refill(&mut iterators).await?;
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
                let (row_key, _) = iterator.head().expect("the selected iterator has a row");
                let family = *iterator.sort_key();
                self.refuse_a_repeated_input_key(family, row_key)?;
                let row_locality = locality_of(family, row_key, cluster.locality);
                (locality.as_deref() != Some(row_locality)).then(|| row_locality.to_owned())
            };
            if opened.is_some() {
                if let Some(kept) = operator.close_group(floor_seq)? {
                    self.write_row(kept, &mut writers).await?;
                }
                locality = opened;
            }
            let family = *iterators[next].sort_key();
            let row = iterators[next].take_head();
            self.result.rows_read += 1;
            self.report_progress();
            if let Some(kept) = operator.take_floor_value_before(&row, floor_seq) {
                self.write_row(kept, &mut writers).await?;
            }
            let kept = operator.push(family, row, floor_seq)?;
            self.result.peak_operator_rows =
                self.result.peak_operator_rows.max(operator.held_rows());
            if let Some(kept) = kept {
                self.write_row(kept, &mut writers).await?;
            }
        }
        if let Some(kept) = operator.close_group(floor_seq)? {
            self.write_row(kept, &mut writers).await?;
        }

        for (_, writer) in writers {
            let segments = writer.finish(self.store).await?;
            self.result.output_bytes = self
                .result
                .output_bytes
                .saturating_add(segments.iter().map(segment_object_len).sum());
            self.result.output_segments.extend(segments);
        }
        Ok(None)
    }

    /// Says where a long job has got to, at [`PROGRESS_ROW_INTERVAL`].
    ///
    /// A job has no bound on how long it runs, and it publishes nothing until
    /// it is finished, so without this an operator watching a big namespace
    /// sees one line at the start and nothing until it lands. A merge that runs
    /// inside a maintenance pass reports nothing: its input is capped by the
    /// step's budgets and the step publishes it. The counters are the merge's
    /// own; nothing is measured for this.
    fn report_progress(&mut self) {
        let Some(progress) = &mut self.progress else {
            return;
        };
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
            "streaming metadata compaction progress"
        );
    }

    /// Fills every iterator that has run out of rows and records what the merge
    /// then holds.
    async fn refill(&mut self, iterators: &mut [MetadataSegmentRowIterator]) -> Result<()> {
        let resident = refill_iterators(
            &MetadataSegmentBlockLoader::new(self.store, self.index_memo),
            iterators,
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
    ) -> Result<()> {
        self.result.rows_written += 1;
        let writer = writers
            .get_mut(&family)
            .expect("a cluster writes only the families it merges");
        match index_pair(self.group) {
            Some((canonical, _)) if family == canonical => {
                writer.push(row, &mut |encoded| self.canonical_digest.fold(encoded))?;
            }
            Some((_, index)) if family == index => {
                writer.push(row, &mut |encoded| self.index_digest.fold(encoded))?;
            }
            _ => writer.push(row, &mut |_| {})?,
        }
        writer.roll_full_segments(self.store, self.policy).await
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
        | MetadataFamilyGroup::ContentPublications
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
                        "a streaming compaction names input run `{run_no}`, which the manifest \
                         does not reference"
                    ))
                })
        })
        .collect()
}

/// The one mapper the compaction modules share: a manifest load failure is a
/// metadata projection failure wherever it is read.
pub(super) fn manifest_load_failure(error: ManifestLoadError) -> CoreError {
    CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
}

#[cfg(test)]
mod tests {
    use super::RowDigest;
    use loonfs_api::wire::manifest::MetadataRow;
    use loonfs_api::{ChangeSeq, DisplayName, InodeId, NameKey};

    fn bind(parent: u64, name: &str, bind_seq: u64) -> MetadataRow {
        MetadataRow::DirentryBinding(crate::metadata::DirentryBindingRecord {
            parent_inode_id: InodeId(parent),
            name_key: NameKey::parse(name).expect("name key"),
            state: loonfs_api::wire::manifest::DirentryBindingState::Bound {
                display_name: DisplayName::parse(name).expect("display name"),
            },
            child_inode_id: InodeId(42),
            committed_seq: ChangeSeq(bind_seq),
            delta_index: 0,
        })
    }

    fn unbind(parent: u64, name: &str, bind_seq: u64) -> MetadataRow {
        MetadataRow::DirentryBinding(crate::metadata::DirentryBindingRecord {
            parent_inode_id: InodeId(parent),
            name_key: NameKey::parse(name).expect("name key"),
            child_inode_id: InodeId(42),
            committed_seq: ChangeSeq(bind_seq + 1),
            delta_index: 0,
            state: loonfs_api::wire::manifest::DirentryBindingState::Unbound,
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
