//! Rebuilding one family group in a single streaming job.
//!
//! A bounded merge reads every row of its window into memory, so a group
//! whose bottom-anchored window no longer fits one step's budgets can never
//! fold from its bottom again: its base is frozen and its retention stops
//! ([`super::reorganize`] reports that, loudly). Such a group is rebuilt
//! here instead, by one job that reads every run it holds and is paced by no
//! budget at all.
//!
//! Nothing about the job follows the size of the group. It opens one iterator
//! per run per family, merges them in row-key order, feeds the merged rows
//! through the retention rules a locality group at a time, and writes each
//! output segment as it fills. What it holds at any instant is a fixed number
//! of decoded input blocks per iterator, the rows of one locality group, and
//! one segment builder per family of the group. No family, no partition, and
//! no directory is ever collected whole.
//!
//! Output segments go to the staging directory rather than to
//! `metadata/tables/` (format spec, "Compaction"): a job outlives the
//! collector's grace window, so its output would otherwise look exactly like
//! the unreferenced aged objects the collector reaps. The executor publishes
//! nothing itself. It returns the descriptors it wrote, and
//! [`run_metadata_compaction_job`] swaps them in with one manifest
//! publication; a cancelled or crashed job leaves staged objects nothing
//! references and the old manifest still valid.
//!
//! The job runs as a background task the maintenance runner owns. The step
//! that plans it starts it and returns; the runner cancels it on shutdown and
//! joins it with the rest of its background work.

use super::block_fetch::{load_segment_filter, load_segment_index_for_reorganization};
use super::block_load::SessionBlockMemo;
use super::build::{write_manifest_segment, MetadataSstWriteRequest};
use super::cache::{MetadataTableCache, MetadataTableCacheConfig};
use super::data_block_load::load_segment_data_block_span;
use super::error::ManifestLoadError;
use super::flush::ensure_metadata_publication_budget;
use super::load::{
    load_manifest_segment_rows_in_key_range_with_cache, load_verified_manifest_tables,
};
use super::publish::{publish_metadata_root, ManifestPublicationOutcome};
use super::reorganize::{
    bind_survives_frozen_floor, drop_rows_below_frozen_floor, group_run_descriptors,
    unbindings_at_or_below_floor, write_reorganized_manifest, MergePlacement,
};
use super::runs::{MetadataFamilyGroup, MetadataLsmPolicy, MetadataRunManifest};
use super::scan::{descriptor_may_intersect_range, Readahead, VerifiedMetadataTables};
use super::validate::validate_manifest_row_seq_range;
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::control::read_metadata_root_object_if_present;
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::manifest::{lookup_keys, MetadataFileRef, MetadataRow, MetadataTableFamily};
use loonfs_api::wire::sst_blocks::{
    string_prefix_upper_bound, DecodedDataBlock, SegmentIndexEntry,
};
use loonfs_api::{ChangeSeq, ManifestId, MetadataTableId, NamespaceId};
use loonfs_objectstore::keys::metadata_compaction_staging_table;
use loonfs_objectstore::ObjectStore;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Decoded data blocks one iterator holds at a time. An iterator refills only
/// when it has none left, so this is also the most it ever holds, and the
/// job's input residency is this times the number of open iterators.
const BLOCKS_PER_ITERATOR_FETCH: usize = 2;

/// Iterators refilled in one wave. Refills are the job's only bulk reads, so
/// this is the width of its fan-out at the store.
const ITERATOR_FETCH_CONCURRENCY: usize = 8;

/// Decoded-byte budget for the cache the reverse-index point reads share.
///
/// Those reads are keyed by child while the unbind family is keyed by parent,
/// so they land all over the family and each one would otherwise re-fetch the
/// filter and index sections the read before it just used. The cache is an
/// LRU with a byte budget, so what it holds is this number and not a function
/// of how many reads the job makes.
const PROBE_CACHE_DECODED_BYTES: usize = 16 * 1024 * 1024;

/// Rows between two progress lines. A job that needs one at all is bigger
/// than a step's whole row budget, so this is coarse on purpose: a handful of
/// lines over a long job rather than one per segment.
const PROGRESS_ROW_INTERVAL: u64 = 1_000_000;

/// Publication attempts one finalization makes before giving up.
///
/// Only an unrelated publication landing between the reload and the root
/// compare-and-swap costs an attempt, and the reload is what the next attempt
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
/// The runtime holds one of these per running job and hands it back to
/// [`super::reorganize_metadata_step`], which is how a step knows not to
/// merge the group a job is rebuilding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCompactionSpec {
    group: MetadataFamilyGroup,
    /// Every run the group held when the plan was made, by sequence and
    /// level. That is bottom-anchored by construction — a run the group holds
    /// cannot sort below the whole of itself — which is what makes the job's
    /// drops visibility-preserving.
    inputs: Vec<(ChangeSeq, u32)>,
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
        inputs: Vec<(ChangeSeq, u32)>,
        input_rows: u64,
        placement: MergePlacement,
        frozen_floor_seq: ChangeSeq,
    ) -> Self {
        Self {
            group,
            inputs,
            input_rows,
            placement,
            frozen_floor_seq,
        }
    }

    pub(super) fn group(&self) -> MetadataFamilyGroup {
        self.group
    }

    /// The families this job rebuilds, for a caller reporting what it started.
    pub fn families(&self) -> &'static [MetadataTableFamily] {
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

    pub(super) fn inputs(&self) -> &[(ChangeSeq, u32)] {
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

/// Stops a running job between block fetches.
///
/// Cancelling costs the work done so far and nothing else: the staged
/// segments are unreferenced, the manifest never moved, and a later job runs
/// the same spec again.
#[derive(Debug, Clone, Default)]
pub struct MetadataCompactionCancellation(Arc<AtomicBool>);

impl MetadataCompactionCancellation {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// How a job ended.
#[derive(Debug)]
pub(super) enum MetadataCompactionOutcome {
    Completed(MetadataCompactionResult),
    /// The cancellation token was set. Whatever the job had written stays
    /// staged and unreferenced.
    Cancelled,
}

/// What one finished job built, held in memory until the caller publishes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MetadataCompactionResult {
    /// The output run's segment descriptors, naming their staged object keys.
    pub(super) output_segments: Vec<MetadataFileRef>,
    pub(super) rows_read: u64,
    pub(super) rows_written: u64,
    pub(super) rows_written_by_family: BTreeMap<MetadataTableFamily, u64>,
    /// Point reads into the snapshot's unbind family, one per reverse bind
    /// row at or below the frozen floor.
    pub(super) unbind_probes: u64,
    /// The most decoded input blocks the job's iterators held at once, and
    /// the most rows one locality group held. These are what bound the job's
    /// memory, so tests assert they do not follow the size of the group.
    pub(super) peak_resident_blocks: usize,
    pub(super) peak_locality_rows: usize,
}

/// Rebuilds `spec`'s group from `spec`'s runs.
///
/// `tables` must name the manifest the spec was planned against: the job
/// resolves its snapshot out of that manifest, so the runs it reads are the
/// runs the plan chose and nothing else.
pub(super) async fn run_metadata_compaction<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    policy: MetadataLsmPolicy,
    cancellation: &MetadataCompactionCancellation,
) -> Result<MetadataCompactionOutcome> {
    let snapshot = resolve_snapshot_runs(tables, spec)?;
    let mut job = CompactionJob {
        store: tables.store,
        namespace_id,
        spec,
        policy,
        cancellation,
        snapshot,
        probe_cache: MetadataTableCache::new(MetadataTableCacheConfig {
            max_decoded_bytes: PROBE_CACHE_DECODED_BYTES,
        }),
        result: MetadataCompactionResult {
            output_segments: Vec::new(),
            rows_read: 0,
            rows_written: 0,
            rows_written_by_family: BTreeMap::new(),
            unbind_probes: 0,
            peak_resident_blocks: 0,
            peak_locality_rows: 0,
        },
        canonical_digest: RowDigest::default(),
        index_digest: RowDigest::default(),
        next_progress_rows: PROGRESS_ROW_INTERVAL,
    };

    for cluster in retention_clusters(spec.group) {
        if !job.run_cluster(cluster).await? {
            return Ok(MetadataCompactionOutcome::Cancelled);
        }
    }
    job.refuse_a_run_whose_index_disagrees()?;
    Ok(MetadataCompactionOutcome::Completed(job.result))
}

/// How one background compaction job ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataCompactionJobOutcome {
    /// The rebuilt group replaced its snapshot in a published manifest.
    Published {
        manifest_id: ManifestId,
        rows_read: u64,
        rows_written: u64,
        output_segments: usize,
    },
    /// The cancellation token was set. The manifest never moved and the
    /// segments the job had written stay staged and unreferenced.
    Cancelled,
    /// A run the job read is no longer in the manifest, or no longer holds
    /// what it held, so this output cannot stand in for it. Nothing is
    /// published and the staged segments are orphans; a later step plans the
    /// group again from what the manifest now holds.
    Abandoned,
    /// Every publication attempt lost the root race. The job is thrown away
    /// and a later step plans it again.
    Superseded,
}

/// What one finalization attempt sequence decided.
#[derive(Debug)]
pub(super) enum Finalization {
    Published(ManifestId),
    Abandoned,
    Superseded,
}

/// Runs one streaming compaction end to end: rebuild the group, then swap the
/// rebuilt run in with one manifest publication.
///
/// This is what the maintenance runner spawns from the spec a step planned.
/// Nothing durable records that the job is running, so every way it can end
/// short of publishing — cancellation, a snapshot that moved, a lost race, an
/// error — costs the work it did and nothing else: the old manifest stays
/// valid, the staged segments stay invisible, and a later step plans the group
/// again.
pub(crate) async fn run_metadata_compaction_job<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
    spec: &MetadataCompactionSpec,
    cancellation: &MetadataCompactionCancellation,
) -> Result<MetadataCompactionJobOutcome> {
    let timer = StdMonotonicTimer::default();
    let Some(tables) = load_current_manifest_tables(store, namespace_id).await? else {
        return Ok(MetadataCompactionJobOutcome::Abandoned);
    };
    // What the job is about to read, recorded before it reads anything.
    // Finalization compares the manifest against this, so the run it publishes
    // stands in for exactly the segments it merged.
    let Some(snapshot_keys) = snapshot_segment_keys(&tables, spec) else {
        return Ok(MetadataCompactionJobOutcome::Abandoned);
    };
    tracing::info!(
        namespace_id = namespace_id.as_str(),
        families = ?spec.families(),
        input_runs = spec.input_runs(),
        input_rows = spec.input_rows(),
        frozen_floor_seq = spec.frozen_floor_seq().0,
        "streaming metadata compaction started"
    );

    let result =
        match run_metadata_compaction(&tables, namespace_id, spec, policy, cancellation).await? {
            MetadataCompactionOutcome::Completed(result) => result,
            MetadataCompactionOutcome::Cancelled => {
                // The executor stops between block fetches, so what it wrote is
                // whatever segments had already filled. They are staged and named
                // by nothing.
                tracing::info!(
                    namespace_id = namespace_id.as_str(),
                    families = ?spec.families(),
                    "streaming metadata compaction cancelled"
                );
                return Ok(MetadataCompactionJobOutcome::Cancelled);
            }
        };
    drop(tables);

    let rows_read = result.rows_read;
    let rows_written = result.rows_written;
    let output_segments = result.output_segments.len();
    match finalize_metadata_compaction(
        store,
        namespace_id,
        context,
        spec,
        &snapshot_keys,
        result,
        &timer,
    )
    .await?
    {
        Finalization::Published(manifest_id) => {
            tracing::info!(
                namespace_id = namespace_id.as_str(),
                families = ?spec.families(),
                rows_read,
                rows_written,
                output_segments,
                manifest_id = manifest_id.0,
                "streaming metadata compaction published"
            );
            Ok(MetadataCompactionJobOutcome::Published {
                manifest_id,
                rows_read,
                rows_written,
                output_segments,
            })
        }
        Finalization::Abandoned => Ok(MetadataCompactionJobOutcome::Abandoned),
        Finalization::Superseded => Ok(MetadataCompactionJobOutcome::Superseded),
    }
}

/// Swaps the rebuilt run in for the snapshot it replaces.
///
/// Reload the root and manifest, check that every segment the job read is
/// still exactly what the manifest holds for the group in those runs, replace
/// those descriptors with the output run's, keep everything else — including
/// the runs that arrived while the job ran — and publish through the ordinary
/// compare-and-swap. An unrelated publication winning that race is a reload
/// and another attempt; a snapshot that moved is an abandon, because this
/// output no longer stands in for what the manifest holds.
///
/// The publication budget covers this publication and not the job: what it
/// protects against is a root compare-and-swap landing after the objects it
/// names could have aged into the collector's window, which is a property of
/// the last few seconds and not of however long the rebuild took.
pub(super) async fn finalize_metadata_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    spec: &MetadataCompactionSpec,
    snapshot_keys: &BTreeSet<String>,
    result: MetadataCompactionResult,
    timer: &dyn MonotonicTimer,
) -> Result<Finalization> {
    for attempt in 1..=MAX_FINALIZATION_ATTEMPTS {
        let publication_started_ms = timer.monotonic_now_ms();
        let Some(root) = read_metadata_root_object_if_present(store, namespace_id)
            .await
            .map_err(CoreError::load_head)?
            .map(|loaded| loaded.envelope.state)
        else {
            return Ok(Finalization::Abandoned);
        };
        let tables = load_verified_manifest_tables(store, namespace_id, &root.manifest_object_id)
            .await
            .map_err(manifest_load_failure)?;
        if snapshot_segment_keys(&tables, spec).as_ref() != Some(snapshot_keys) {
            tracing::info!(
                namespace_id = namespace_id.as_str(),
                families = ?spec.families(),
                "streaming metadata compaction abandoned: its input runs moved while it ran"
            );
            return Ok(Finalization::Abandoned);
        }

        let previous = tables.manifest();
        let mut metadata_files: Vec<MetadataFileRef> = previous
            .payload
            .metadata_files
            .iter()
            .filter(|descriptor| !snapshot_keys.contains(&descriptor.object_key))
            .cloned()
            .collect();
        metadata_files.extend(result.output_segments.iter().cloned());
        let base_seq = metadata_files
            .iter()
            .map(|descriptor| descriptor.run_seq)
            .min()
            .unwrap_or(previous.payload.base_seq);
        let manifest = write_reorganized_manifest(
            store,
            namespace_id,
            previous,
            metadata_files,
            base_seq,
            previous
                .payload
                .retention_floor_seq
                .max(spec.frozen_floor_seq()),
        )
        .await?;

        ensure_metadata_publication_budget(timer, publication_started_ms, namespace_id)?;
        let published = publish_metadata_root(
            store,
            namespace_id,
            &manifest,
            Some(root.manifest_object_id.clone()),
            context.now_ms,
        )
        .await?;
        drop(tables);
        match published {
            ManifestPublicationOutcome::Published(_) => {
                return Ok(Finalization::Published(manifest.payload.manifest_id))
            }
            ManifestPublicationOutcome::Superseded(_)
            | ManifestPublicationOutcome::RootCasRaceLost => {
                tracing::debug!(
                    namespace_id = namespace_id.as_str(),
                    families = ?spec.families(),
                    attempt,
                    attempts = MAX_FINALIZATION_ATTEMPTS,
                    "a publication landed while a streaming metadata compaction was finalizing; \
                     reloading"
                );
            }
        }
    }
    tracing::info!(
        namespace_id = namespace_id.as_str(),
        families = ?spec.families(),
        attempts = MAX_FINALIZATION_ATTEMPTS,
        "streaming metadata compaction superseded at every publication attempt; a later step \
         plans it again"
    );
    Ok(Finalization::Superseded)
}

/// The tables of whatever manifest the namespace's root names, or `None` when
/// it names none.
async fn load_current_manifest_tables<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
) -> Result<Option<VerifiedMetadataTables<'a, S>>> {
    let Some(root) = read_metadata_root_object_if_present(store, namespace_id)
        .await
        .map_err(CoreError::load_head)?
        .map(|loaded| loaded.envelope.state)
    else {
        return Ok(None);
    };
    load_verified_manifest_tables(store, namespace_id, &root.manifest_object_id)
        .await
        .map(Some)
        .map_err(manifest_load_failure)
}

/// The object keys the spec's runs hold for its group, or `None` when the
/// manifest no longer references one of those runs at all.
///
/// Segments are immutable and their keys are generated, so two manifests
/// agreeing on this set agree on every row the job read. Runs the manifest
/// gained meanwhile are not in it — the job never read them, and they survive
/// the publication untouched.
pub(super) fn snapshot_segment_keys<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    spec: &MetadataCompactionSpec,
) -> Option<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    for (run_seq, level) in spec.inputs() {
        let run = tables
            .scan_runs
            .iter()
            .find(|run| run.run_seq == *run_seq && run.level == *level)?;
        keys.extend(
            group_run_descriptors(run, spec.group())
                .map(|descriptor| descriptor.object_key.clone()),
        );
    }
    Some(keys)
}

/// One set of families the job merges and judges together, and how it groups
/// their rows while doing it.
struct RetentionCluster {
    families: &'static [MetadataTableFamily],
    locality: LocalityGrouping,
    operator: RetentionOperator,
}

/// How many rows the retention rules need to see at once.
///
/// The rules that drop a row read its neighbours, and every one of them reads
/// neighbours that share a row-key prefix: an unbind cancels the bind under
/// the same parent and name, a removal marker repeats its deletion's sequence
/// and root, and an attribute revision supersedes revisions of its own inode.
/// So the grouping is read straight off the row-key grammar in
/// `MetadataRow::row_key_for_family`, and it is the shortest prefix each rule
/// needs rather than the whole partition the rows happen to share:
///
/// | families | grouped by | key components after the family prefix |
/// | --- | --- | --- |
/// | binds, unbinds | one name slot | `{parent}-{name}` |
/// | active deletions | one deletion | `{deleted_at_seq}-{root}` |
/// | attributes | one inode | `{inode}` |
/// | everything else | one row | — |
///
/// A slot rather than a directory is what keeps the bindings group bounded:
/// a directory has no size limit, and a slot holds one binding's generations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalityGrouping {
    /// Every row is judged on its own.
    Row,
    /// Rows sharing the first `n` hyphen-separated components after their
    /// family's row-key prefix are judged together. Every component of the
    /// grammar is digits or lowercase hex, and a hyphen sorts below both, so
    /// rows of one grouping are contiguous in row-key order and grouping
    /// order is row-key order.
    LeadingKeyComponents(usize),
}

/// Which rule decides the rows of a cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetentionOperator {
    /// The frozen floor's rules over one locality group's rows, which is the
    /// same function a whole-group fold runs over a whole family.
    FrozenFloor,
    /// Reverse bind rows, each decided by a point read into the snapshot for
    /// the unbinds of its own binding.
    ReverseBindProbe,
}

/// The bindings group merges its two forward families together, because an
/// unbind retires the bind in its slot. The reverse index is keyed by child,
/// so it shares no slot with the rows it indexes and streams on its own.
const BINDINGS_CLUSTERS: [RetentionCluster; 2] = [
    RetentionCluster {
        families: &[
            MetadataTableFamily::DirentryBinds,
            MetadataTableFamily::DirentryUnbinds,
        ],
        locality: LocalityGrouping::LeadingKeyComponents(2),
        operator: RetentionOperator::FrozenFloor,
    },
    RetentionCluster {
        families: &[MetadataTableFamily::DirentryChildBinds],
        locality: LocalityGrouping::Row,
        operator: RetentionOperator::ReverseBindProbe,
    },
];

/// A family whose rows are each decided on their own — either because no rule
/// drops them at all, or because the rule reads nothing but the row.
const fn row_cluster(families: &'static [MetadataTableFamily]) -> RetentionCluster {
    RetentionCluster {
        families,
        locality: LocalityGrouping::Row,
        operator: RetentionOperator::FrozenFloor,
    }
}

/// Revision rows are never dropped and their index travels with them, so both
/// families are a straight rewrite in key order.
const REVISION_CLUSTERS: [RetentionCluster; 2] = [
    row_cluster(&[MetadataTableFamily::Revisions]),
    row_cluster(&[MetadataTableFamily::RevisionsByInodeDesc]),
];
const INODE_CLUSTERS: [RetentionCluster; 1] = [row_cluster(&[MetadataTableFamily::Inodes])];
const TOMBSTONE_CLUSTERS: [RetentionCluster; 1] = [row_cluster(&[MetadataTableFamily::Tombstones])];
/// A receipt is kept or dropped by its own sequence against the floor.
const RECEIPT_CLUSTERS: [RetentionCluster; 1] =
    [row_cluster(&[MetadataTableFamily::CommitReceipts])];
const ACTIVE_DELETION_CLUSTERS: [RetentionCluster; 1] = [RetentionCluster {
    families: &[MetadataTableFamily::ActiveDeletions],
    locality: LocalityGrouping::LeadingKeyComponents(2),
    operator: RetentionOperator::FrozenFloor,
}];
const ATTRIBUTE_CLUSTERS: [RetentionCluster; 1] = [RetentionCluster {
    families: &[MetadataTableFamily::Attributes],
    locality: LocalityGrouping::LeadingKeyComponents(1),
    operator: RetentionOperator::FrozenFloor,
}];

fn retention_clusters(group: MetadataFamilyGroup) -> &'static [RetentionCluster] {
    match group {
        MetadataFamilyGroup::Bindings => &BINDINGS_CLUSTERS,
        MetadataFamilyGroup::Revisions => &REVISION_CLUSTERS,
        MetadataFamilyGroup::Inodes => &INODE_CLUSTERS,
        MetadataFamilyGroup::Tombstones => &TOMBSTONE_CLUSTERS,
        MetadataFamilyGroup::ActiveDeletions => &ACTIVE_DELETION_CLUSTERS,
        MetadataFamilyGroup::CommitReceipts => &RECEIPT_CLUSTERS,
        MetadataFamilyGroup::Attributes => &ATTRIBUTE_CLUSTERS,
    }
}

/// The locality group a row key belongs to, as a slice of the key itself.
///
/// The family's own prefix is stripped, so two families of one cluster name
/// the same group with the same string: a bind's `direntry-{parent}-{name}`
/// and its unbind's `direntry-unbind-{parent}-{name}` both read as
/// `{parent}-{name}`.
fn locality_of(family: MetadataTableFamily, row_key: &str, locality: LocalityGrouping) -> &str {
    let prefix_len = family.row_key_prefix().len();
    let tail = row_key.get(prefix_len..).unwrap_or(row_key);
    let LocalityGrouping::LeadingKeyComponents(components) = locality else {
        return tail;
    };
    let mut end = 0;
    for component in 0..components {
        if component > 0 {
            end += 1;
        }
        match tail[end..].find('-') {
            Some(offset) => end += offset,
            None => return tail,
        }
    }
    &tail[..end]
}

struct CompactionJob<'a, S: ObjectStore + ?Sized> {
    store: &'a S,
    namespace_id: &'a NamespaceId,
    spec: &'a MetadataCompactionSpec,
    policy: MetadataLsmPolicy,
    cancellation: &'a MetadataCompactionCancellation,
    snapshot: Vec<MetadataRunManifest>,
    probe_cache: MetadataTableCache,
    result: MetadataCompactionResult,
    canonical_digest: RowDigest,
    index_digest: RowDigest,
    /// The read count the next progress line is owed at.
    next_progress_rows: u64,
}

impl<S: ObjectStore + ?Sized> CompactionJob<'_, S> {
    /// Merges one cluster end to end. `false` means the job was cancelled.
    async fn run_cluster(&mut self, cluster: &RetentionCluster) -> Result<bool> {
        // The descriptors are cloned out of the snapshot rather than borrowed
        // from it: an iterator outlives every other borrow the job takes, and
        // a cluster opens one per run per family, which is a handful.
        let mut iterators = Vec::new();
        for run in &self.snapshot {
            for family in cluster.families {
                let segments: Vec<MetadataFileRef> = group_run_descriptors(run, self.spec.group)
                    .filter(|descriptor| descriptor.family == *family)
                    .cloned()
                    .collect();
                if !segments.is_empty() {
                    iterators.push(SegmentRowIterator::new(*family, segments));
                }
            }
        }
        let mut writers: BTreeMap<MetadataTableFamily, StagedSegmentWriter> = cluster
            .families
            .iter()
            .map(|family| (*family, StagedSegmentWriter::new(*family)))
            .collect();

        let mut locality: Option<String> = None;
        let mut group_rows: BTreeMap<MetadataTableFamily, Vec<MetadataRow>> = BTreeMap::new();
        loop {
            if self.cancellation.is_cancelled() {
                return Ok(false);
            }
            self.refill(&mut iterators).await?;
            let Some(next) = select_next_iterator(&iterators, cluster.locality) else {
                break;
            };
            // The locality is a slice of the iterator's own key, so it is
            // only copied when it changes — once per group rather than once
            // per row.
            let opened = {
                let iterator = &iterators[next];
                let (row_key, _) = iterator.head().expect("the selected iterator has a row");
                let row_locality = locality_of(iterator.family, row_key, cluster.locality);
                (locality.as_deref() != Some(row_locality)).then(|| row_locality.to_owned())
            };
            if let Some(opened) = opened {
                self.flush_locality(cluster, &mut group_rows, &mut writers)
                    .await?;
                locality = Some(opened);
            }
            let family = iterators[next].family;
            group_rows
                .entry(family)
                .or_default()
                .push(iterators[next].take_head());
            self.result.rows_read += 1;
            self.report_progress();
        }
        self.flush_locality(cluster, &mut group_rows, &mut writers)
            .await?;

        for writer in writers.into_values() {
            let segments = writer
                .finish(self.store, self.namespace_id, self.spec)
                .await?;
            self.result.output_segments.extend(segments);
        }
        Ok(true)
    }

    /// Says where a long job has got to, at [`PROGRESS_ROW_INTERVAL`].
    ///
    /// A job has no bound on how long it runs, and it publishes nothing until
    /// it is finished, so without this an operator watching a big namespace
    /// sees one line at the start and nothing until it lands. The counters are
    /// the job's own; nothing is measured for this.
    fn report_progress(&mut self) {
        if self.result.rows_read < self.next_progress_rows {
            return;
        }
        self.next_progress_rows = self.result.rows_read.saturating_add(PROGRESS_ROW_INTERVAL);
        tracing::info!(
            namespace_id = self.namespace_id.as_str(),
            families = ?self.spec.families(),
            rows_read = self.result.rows_read,
            rows_written = self.result.rows_written,
            input_rows = self.spec.input_rows(),
            output_segments = self.result.output_segments.len(),
            "streaming metadata compaction progress"
        );
    }

    /// Fills every iterator that has run out of rows, a bounded wave at a
    /// time, and records what the job then holds.
    async fn refill(&mut self, iterators: &mut [SegmentRowIterator]) -> Result<()> {
        let mut hungry: Vec<&mut SegmentRowIterator> = iterators
            .iter_mut()
            .filter(|iterator| iterator.needs_fill())
            .collect();
        for wave in hungry.chunks_mut(ITERATOR_FETCH_CONCURRENCY) {
            futures::future::try_join_all(
                wave.iter_mut()
                    .map(|iterator| Box::pin(iterator.fill(self.store))),
            )
            .await?;
        }
        let resident = iterators
            .iter()
            .map(SegmentRowIterator::resident_blocks)
            .sum();
        self.result.peak_resident_blocks = self.result.peak_resident_blocks.max(resident);
        Ok(())
    }

    /// Judges one locality group and writes what survives.
    async fn flush_locality(
        &mut self,
        cluster: &RetentionCluster,
        group_rows: &mut BTreeMap<MetadataTableFamily, Vec<MetadataRow>>,
        writers: &mut BTreeMap<MetadataTableFamily, StagedSegmentWriter>,
    ) -> Result<()> {
        let held: usize = group_rows.values().map(Vec::len).sum();
        if held == 0 {
            return Ok(());
        }
        self.result.peak_locality_rows = self.result.peak_locality_rows.max(held);

        match cluster.operator {
            RetentionOperator::FrozenFloor => {
                // A locality group holds every unbind that can retire the
                // binds it holds, because a bind and its unbind share a slot.
                let unbound_at_floor = unbindings_at_or_below_floor(
                    group_rows
                        .get(&MetadataTableFamily::DirentryUnbinds)
                        .map_or(&[], Vec::as_slice),
                    self.spec.frozen_floor_seq,
                );
                drop_rows_below_frozen_floor(
                    group_rows,
                    self.spec.frozen_floor_seq,
                    &unbound_at_floor,
                )?;
            }
            RetentionOperator::ReverseBindProbe => {
                self.retain_reverse_binds(group_rows).await?;
            }
        }

        for (family, rows) in std::mem::take(group_rows) {
            let writer = writers
                .get_mut(&family)
                .expect("a cluster writes only the families it merges");
            for row in rows {
                self.fold_into_index_digests(family, &row)?;
                *self
                    .result
                    .rows_written_by_family
                    .entry(family)
                    .or_default() += 1;
                self.result.rows_written += 1;
                writer.push(row);
            }
            writer
                .roll_full_segments(self.store, self.namespace_id, self.spec, self.policy)
                .await?;
        }
        Ok(())
    }

    /// Keeps the reverse bind rows the frozen floor still covers, deciding
    /// each one by reading the snapshot for the unbinds of its own binding.
    ///
    /// The reverse index is keyed by child and the unbind family by parent, so
    /// no grouping of the merged stream can hold a reverse row together with
    /// the unbind that retires it. The point read closes that: it runs
    /// [`unbindings_at_or_below_floor`] and [`bind_survives_frozen_floor`]
    /// over unbind rows from the same immutable snapshot against the same
    /// frozen floor as the forward pass, so the two bind families drop in
    /// lockstep — which they must, because the format gives every bind row
    /// exactly one reverse row and a run whose two counts disagree does not
    /// load.
    ///
    /// Only rows at or below the floor cost a read: a bind above the floor
    /// survives whatever retired it later. Each read returns the unbinds of
    /// one binding generation, which is zero rows or one.
    async fn retain_reverse_binds(
        &mut self,
        group_rows: &mut BTreeMap<MetadataTableFamily, Vec<MetadataRow>>,
    ) -> Result<()> {
        let Some(rows) = group_rows.get_mut(&MetadataTableFamily::DirentryChildBinds) else {
            return Ok(());
        };
        let mut kept = Vec::with_capacity(rows.len());
        for row in std::mem::take(rows) {
            let MetadataRow::DirentryBind {
                parent_inode_id,
                name_key,
                bind_seq,
                bind_delta_index,
                ..
            } = &row
            else {
                kept.push(row);
                continue;
            };
            if *bind_seq > self.spec.frozen_floor_seq {
                kept.push(row);
                continue;
            }
            let unbind_rows = self
                .read_unbinds_of_binding(
                    &lookup_keys::direntry_unbind_binding_prefix(
                        *parent_inode_id,
                        name_key.as_str(),
                        *bind_seq,
                        *bind_delta_index,
                    ),
                    &lookup_keys::direntry_unbind_probe(*parent_inode_id, name_key.as_str()),
                )
                .await?;
            self.result.unbind_probes += 1;
            let unbound_at_floor =
                unbindings_at_or_below_floor(&unbind_rows, self.spec.frozen_floor_seq);
            if bind_survives_frozen_floor(&row, self.spec.frozen_floor_seq, &unbound_at_floor) {
                kept.push(row);
            }
        }
        *rows = kept;
        Ok(())
    }

    /// One point read into the snapshot's unbind family.
    ///
    /// The unbind key grammar leads with the binding a row names, so the
    /// prefix selects the unbinds of that one binding and nothing else. The
    /// bloom filter is keyed by parent and name, so a binding no operation
    /// ever retired misses it outright and costs no index or data fetch.
    /// Decoded sections land in the job's own bounded cache, and the per-read
    /// memo is dropped with the read, so what these reads hold is the cache's
    /// byte budget however many of them the job makes.
    async fn read_unbinds_of_binding(
        &self,
        prefix: &str,
        filter_probe: &str,
    ) -> Result<Vec<MetadataRow>> {
        let upper_bound = string_prefix_upper_bound(prefix);
        let mut rows = Vec::new();
        for run in &self.snapshot {
            for descriptor in group_run_descriptors(run, self.spec.group)
                .filter(|descriptor| descriptor.family == MetadataTableFamily::DirentryUnbinds)
            {
                if !descriptor_may_intersect_range(descriptor, prefix, upper_bound.as_deref()) {
                    continue;
                }
                let memo = SessionBlockMemo::default();
                let filter =
                    load_segment_filter(self.store, Some(&self.probe_cache), &memo, descriptor)
                        .await
                        .map_err(manifest_load_failure)?;
                if !filter.may_contain(filter_probe) {
                    continue;
                }
                let blocks = load_manifest_segment_rows_in_key_range_with_cache(
                    self.store,
                    Some(&self.probe_cache),
                    &memo,
                    descriptor,
                    prefix,
                    upper_bound.as_deref(),
                    Readahead::Disabled,
                )
                .await
                .map_err(manifest_load_failure)?;
                rows.extend(
                    blocks
                        .rows_in_key_range(prefix, upper_bound.as_deref())
                        .map(|(_, row)| row.clone()),
                );
            }
        }
        Ok(rows)
    }

    /// Folds a written row into the digest of its side of the group's index
    /// pair, when the group has one.
    fn fold_into_index_digests(
        &mut self,
        family: MetadataTableFamily,
        row: &MetadataRow,
    ) -> Result<()> {
        let Some((canonical, index)) = index_pair(self.spec.group) else {
            return Ok(());
        };
        if family == canonical {
            self.canonical_digest.fold(row)?;
        } else if family == index {
            self.index_digest.fold(row)?;
        }
        Ok(())
    }

    /// Refuses to hand back a run whose secondary index does not hold the same
    /// rows as its canonical family.
    ///
    /// A whole-group fold compares the two families outright, over rows it
    /// holds all of. A streaming job cannot: the reverse bind index is keyed
    /// by child, so no grouping ever holds a bind row and the reverse row that
    /// indexes it, and the two are decided in different passes. The digests
    /// stand in — each pass folds the rows it wrote into one, order does not
    /// matter, and the two agree at the end exactly when the job wrote the two
    /// families the same rows.
    fn refuse_a_run_whose_index_disagrees(&self) -> Result<()> {
        let Some((canonical, index)) = index_pair(self.spec.group) else {
            return Ok(());
        };
        if self.canonical_digest == self.index_digest {
            return Ok(());
        }
        Err(CoreError::NamespaceCorrupt(format!(
            "a streaming compaction of {:?} wrote {} `{canonical:?}` rows digesting to `{}` and \
             {} `{index:?}` rows digesting to `{}`; the two families must hold the same rows, so \
             the run it built is not publishable",
            self.spec.group.families(),
            self.canonical_digest.rows,
            self.canonical_digest.spell(),
            self.index_digest.rows,
            self.index_digest.spell(),
        )))
    }
}

/// Reads one family's rows out of one run, one bounded span of data blocks at
/// a time.
///
/// A run's segments for one family are ascending and non-overlapping (manifest
/// load enforces it), so walking them in index order walks the run's rows in
/// row-key order. Only the current segment's index is held, and only the
/// blocks not yet consumed.
struct SegmentRowIterator {
    family: MetadataTableFamily,
    segments: Vec<MetadataFileRef>,
    next_segment: usize,
    index: Option<Arc<Vec<SegmentIndexEntry>>>,
    next_block: usize,
    blocks: VecDeque<Arc<DecodedDataBlock>>,
    row: usize,
}

impl SegmentRowIterator {
    fn new(family: MetadataTableFamily, mut segments: Vec<MetadataFileRef>) -> Self {
        segments.sort_by_key(|descriptor| descriptor.segment_index);
        Self {
            family,
            segments,
            next_segment: 0,
            index: None,
            next_block: 0,
            blocks: VecDeque::new(),
            row: 0,
        }
    }

    fn head(&self) -> Option<(&str, &MetadataRow)> {
        let block = self.blocks.front()?;
        Some((block.row_keys[self.row].as_str(), &block.rows[self.row]))
    }

    fn take_head(&mut self) -> MetadataRow {
        let row = self.blocks.front().expect("a head to take").rows[self.row].clone();
        self.row += 1;
        while self
            .blocks
            .front()
            .is_some_and(|block| self.row >= block.rows.len())
        {
            self.blocks.pop_front();
            self.row = 0;
        }
        row
    }

    fn needs_fill(&self) -> bool {
        self.blocks.is_empty() && !self.is_exhausted()
    }

    fn is_exhausted(&self) -> bool {
        self.blocks.is_empty()
            && self.next_segment >= self.segments.len()
            && self
                .index
                .as_ref()
                .is_none_or(|index| self.next_block >= index.len())
    }

    fn resident_blocks(&self) -> usize {
        self.blocks.len()
    }

    /// Fetches the next span of data blocks, opening the next segment first
    /// when the current one is spent. A segment with no rows left is closed
    /// and its index dropped before the next one is read, so one iterator
    /// holds one index at a time.
    async fn fill<S: ObjectStore + ?Sized>(&mut self, store: &S) -> Result<()> {
        while self.blocks.is_empty() {
            let index = match &self.index {
                Some(index) if self.next_block < index.len() => Arc::clone(index),
                _ => {
                    self.index = None;
                    let Some(descriptor) = self.segments.get(self.next_segment) else {
                        return Ok(());
                    };
                    self.next_segment += 1;
                    self.next_block = 0;
                    let index = load_segment_index_for_reorganization(
                        store,
                        None,
                        &SessionBlockMemo::default(),
                        descriptor,
                    )
                    .await
                    .map_err(manifest_load_failure)?;
                    self.index = Some(Arc::clone(&index));
                    index
                }
            };
            let descriptor = &self.segments[self.next_segment - 1];
            let end = (self.next_block + BLOCKS_PER_ITERATOR_FETCH).min(index.len());
            // Blocks are decoded into this iterator alone: no table cache and
            // a memo that dies with the call, so nothing the job reads is
            // retained anywhere but here.
            let blocks = load_segment_data_block_span(
                store,
                None,
                &SessionBlockMemo::default(),
                descriptor,
                &index[self.next_block..end],
            )
            .await
            .map_err(manifest_load_failure)?;
            self.next_block = end;
            validate_manifest_row_seq_range(
                &descriptor.object_key,
                blocks.iter().flat_map(|block| block.rows.iter()),
                descriptor.run_seq,
            )
            .map_err(manifest_load_failure)?;
            self.row = 0;
            self.blocks
                .extend(blocks.into_iter().filter(|block| !block.rows.is_empty()));
        }
        Ok(())
    }
}

/// The iterator whose next row sorts first, or `None` when every iterator is
/// spent.
///
/// A linear scan rather than a heap: an iterator is opened per run per family
/// of one cluster, which is a handful, and the scan compares borrowed key
/// slices while a heap would have to own them.
fn select_next_iterator(
    iterators: &[SegmentRowIterator],
    locality: LocalityGrouping,
) -> Option<usize> {
    let mut best: Option<(usize, &str, &str)> = None;
    for (position, iterator) in iterators.iter().enumerate() {
        let Some((row_key, _)) = iterator.head() else {
            continue;
        };
        let candidate = locality_of(iterator.family, row_key, locality);
        let take = match best {
            // Locality first so a group's rows arrive together whatever
            // family they come from, then family, then key: within one
            // family that is row-key order, which is the order a segment
            // builder demands.
            Some((best_position, best_locality, best_key)) => {
                (candidate, iterators[position].family, row_key)
                    < (best_locality, iterators[best_position].family, best_key)
            }
            None => true,
        };
        if take {
            best = Some((position, candidate, row_key));
        }
    }
    best.map(|(position, _, _)| position)
}

/// Accumulates one family's surviving rows and uploads a segment every time
/// the segment row budget fills.
struct StagedSegmentWriter {
    family: MetadataTableFamily,
    rows: Vec<MetadataRow>,
    next_segment_index: u32,
    segments: Vec<MetadataFileRef>,
}

impl StagedSegmentWriter {
    fn new(family: MetadataTableFamily) -> Self {
        Self {
            family,
            rows: Vec::new(),
            next_segment_index: 0,
            segments: Vec::new(),
        }
    }

    fn push(&mut self, row: MetadataRow) {
        self.rows.push(row);
    }

    async fn roll_full_segments<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        spec: &MetadataCompactionSpec,
        policy: MetadataLsmPolicy,
    ) -> Result<()> {
        let max_rows = policy.max_rows_per_segment.get();
        while self.rows.len() >= max_rows {
            let rest = self.rows.split_off(max_rows);
            let full = std::mem::replace(&mut self.rows, rest);
            self.write_segment(store, namespace_id, spec, full).await?;
        }
        Ok(())
    }

    async fn finish<S: ObjectStore + ?Sized>(
        mut self,
        store: &S,
        namespace_id: &NamespaceId,
        spec: &MetadataCompactionSpec,
    ) -> Result<Vec<MetadataFileRef>> {
        let rest = std::mem::take(&mut self.rows);
        if !rest.is_empty() {
            self.write_segment(store, namespace_id, spec, rest).await?;
        }
        Ok(self.segments)
    }

    async fn write_segment<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        spec: &MetadataCompactionSpec,
        rows: Vec<MetadataRow>,
    ) -> Result<()> {
        let table_id = MetadataTableId::generate();
        let object_key =
            metadata_compaction_staging_table(namespace_id.as_str(), table_id.as_str());
        let descriptor = write_manifest_segment(
            store,
            MetadataSstWriteRequest::new(
                namespace_id,
                table_id,
                object_key,
                spec.placement.output_seq(),
                spec.placement.output_level(),
                self.family,
                self.next_segment_index,
                rows,
            ),
        )
        .await?;
        self.next_segment_index += 1;
        self.segments.push(descriptor);
        Ok(())
    }
}

/// An order-independent digest of the rows one family was written.
///
/// The combiner is wrapping addition, so the digest depends on the multiset of
/// rows and not on the order they were written in — which is the point,
/// because the two families of an index pair are written in different orders
/// and, for the bind pair, in different passes. This is corruption detection,
/// not a signature: what it has to catch is a job that wrote a secondary index
/// rows its canonical family does not hold, including a row differing in one
/// field. Nothing durable carries it, so the input is the row's serde
/// encoding.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RowDigest {
    value: u128,
    rows: u64,
}

impl RowDigest {
    fn fold(&mut self, row: &MetadataRow) -> Result<()> {
        let encoded = serde_json::to_vec(row).map_err(|err| {
            CoreError::Internal(format!(
                "failed to encode a metadata row for digesting: {err}"
            ))
        })?;
        let digest = Sha256::digest(&encoded);
        let mut head = [0u8; 16];
        head.copy_from_slice(&digest[..16]);
        self.value = self.value.wrapping_add(u128::from_be_bytes(head));
        self.rows += 1;
        Ok(())
    }

    fn spell(&self) -> String {
        format!("{:032x}", self.value)
    }
}

/// The canonical family and secondary index of a group that carries one.
fn index_pair(group: MetadataFamilyGroup) -> Option<(MetadataTableFamily, MetadataTableFamily)> {
    match group {
        MetadataFamilyGroup::Bindings => Some((
            MetadataTableFamily::DirentryBinds,
            MetadataTableFamily::DirentryChildBinds,
        )),
        MetadataFamilyGroup::Revisions => Some((
            MetadataTableFamily::Revisions,
            MetadataTableFamily::RevisionsByInodeDesc,
        )),
        _ => None,
    }
}

/// Turns the run ids a spec names back into the manifest's runs.
fn resolve_snapshot_runs<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    spec: &MetadataCompactionSpec,
) -> Result<Vec<MetadataRunManifest>> {
    spec.inputs
        .iter()
        .map(|(run_seq, level)| {
            tables
                .scan_runs
                .iter()
                .find(|run| run.run_seq == *run_seq && run.level == *level)
                .cloned()
                .ok_or_else(|| {
                    CoreError::NamespaceCorrupt(format!(
                        "a streaming compaction names input run seq `{run_seq}` level {level}, \
                         which the manifest does not reference"
                    ))
                })
        })
        .collect()
}

fn manifest_load_failure(error: ManifestLoadError) -> CoreError {
    CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
}

#[cfg(test)]
mod tests {
    use super::{locality_of, LocalityGrouping, RowDigest};
    use loonfs_api::wire::manifest::{MetadataRow, MetadataTableFamily};
    use loonfs_api::{ChangeSeq, DisplayName, InodeId, NameKey};

    fn bind(parent: u64, name: &str, bind_seq: u64) -> MetadataRow {
        MetadataRow::DirentryBind {
            parent_inode_id: InodeId(parent),
            name_key: NameKey::parse(name).expect("name key"),
            display_name: DisplayName::parse(name).expect("display name"),
            child_inode_id: InodeId(42),
            bind_seq: ChangeSeq(bind_seq),
            bind_delta_index: 0,
        }
    }

    fn unbind(parent: u64, name: &str, bind_seq: u64) -> MetadataRow {
        MetadataRow::DirentryUnbind {
            parent_inode_id: InodeId(parent),
            name_key: NameKey::parse(name).expect("name key"),
            display_name: DisplayName::parse(name).expect("display name"),
            child_inode_id: InodeId(42),
            bind_seq: ChangeSeq(bind_seq),
            bind_delta_index: 0,
            unbind_seq: ChangeSeq(bind_seq + 1),
            unbind_delta_index: 0,
        }
    }

    /// A bind and the unbind that retires it must name the same locality
    /// group, because the drop rule reads one from the other. They live in
    /// different families with different row-key prefixes, so this holds only
    /// because the grouping is read behind the prefix.
    #[test]
    fn a_bind_and_its_unbind_name_one_locality_group() {
        let slot = LocalityGrouping::LeadingKeyComponents(2);
        let bound = bind(7, "report.txt", 11);
        let retired = unbind(7, "report.txt", 11);
        let bind_key = bound.row_key_for_family(MetadataTableFamily::DirentryBinds);
        let unbind_key = retired.row_key_for_family(MetadataTableFamily::DirentryUnbinds);

        assert_eq!(
            locality_of(MetadataTableFamily::DirentryBinds, &bind_key, slot),
            locality_of(MetadataTableFamily::DirentryUnbinds, &unbind_key, slot),
        );
        // Another name under the same parent is a different group: the rules
        // read one slot, not one directory.
        let other = bind(7, "other.txt", 11).row_key_for_family(MetadataTableFamily::DirentryBinds);
        assert_ne!(
            locality_of(MetadataTableFamily::DirentryBinds, &bind_key, slot),
            locality_of(MetadataTableFamily::DirentryBinds, &other, slot),
        );
        // And so is the same name under another parent.
        let elsewhere =
            bind(8, "report.txt", 11).row_key_for_family(MetadataTableFamily::DirentryBinds);
        assert_ne!(
            locality_of(MetadataTableFamily::DirentryBinds, &bind_key, slot),
            locality_of(MetadataTableFamily::DirentryBinds, &elsewhere, slot),
        );
    }

    /// Rows of one locality group are contiguous in row-key order, and
    /// grouping order is row-key order. Without both, a merge would reopen a
    /// group it had already judged and written.
    #[test]
    fn locality_order_follows_row_key_order() {
        let slot = LocalityGrouping::LeadingKeyComponents(2);
        let mut keys: Vec<String> = ["a", "ab", "b", "a-b"]
            .into_iter()
            .flat_map(|name| {
                [11u64, 12].map(|seq| {
                    bind(7, name, seq).row_key_for_family(MetadataTableFamily::DirentryBinds)
                })
            })
            .collect();
        keys.sort();

        let localities: Vec<&str> = keys
            .iter()
            .map(|key| locality_of(MetadataTableFamily::DirentryBinds, key, slot))
            .collect();
        let mut runs = localities.clone();
        runs.dedup();
        let mut distinct = runs.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(
            runs.len(),
            distinct.len(),
            "a group must not reappear after another group began: {localities:?}"
        );
        assert!(
            runs.windows(2).all(|pair| pair[0] < pair[1]),
            "grouping order must be row-key order: {runs:?}"
        );
    }

    /// The digest compares two families written in different orders and, for
    /// the bind pair, in different passes. So it must ignore order and still
    /// notice one row differing in one field.
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
            forward.fold(row).expect("digest");
        }
        for row in rows.iter().rev() {
            backward.fold(row).expect("digest");
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
            changed.fold(row).expect("digest");
        }
        assert_ne!(forward, changed);

        let mut short = RowDigest::default();
        short.fold(&rows[0]).expect("digest");
        assert_ne!(forward, short);
    }
}
