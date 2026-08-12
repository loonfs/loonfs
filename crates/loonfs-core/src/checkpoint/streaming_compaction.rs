//! The merge engine both reorganization paths run, and the background job
//! that is one of them.
//!
//! [`GroupMerge`] merges one family group. It opens one iterator per run per
//! family and merges them in row-key order ([`super::compaction_merge`]),
//! feeds the merged rows through streaming retention operators
//! ([`super::compaction_retention`]), and writes each output segment as it
//! fills ([`super::compaction_output`]). What it holds at any instant is a
//! fixed number of decoded input blocks per iterator, the fixed state of one
//! retention operator, and one segment builder per family of the group. No
//! family, no partition, no directory, no inode's history, and no name slot's
//! generations are ever collected whole, so nothing it holds follows the size
//! of what it merges.
//!
//! Rows are dropped when, and only when, the merge's [`MergePlacement`] is
//! `Base`. That is the placement rule already: a window that starts at the
//! group's oldest run holds every row a drop could need to read, and a window
//! above the base does not, so a merge above the base is a pure rewrite.
//!
//! Two callers drive the engine, and they differ in orchestration rather than
//! in merging.
//!
//! [`merge_group_in_step`] runs it synchronously inside one maintenance step.
//! The step's budgets chose the window and the step publishes the result, so
//! the segments go to `metadata/tables/` and there is no lease, no job, no
//! registry, and no admission.
//!
//! [`run_metadata_compaction_job`] runs it as a background task the
//! maintenance runner owns, for a group whose bottom-anchored window no longer
//! fits one step's budgets — a group whose base is frozen and whose retention
//! has stopped ([`super::reorganize`] reports that, loudly). Nothing paces that
//! work, so it needs everything the step-contained merge does not: admission, a
//! staging prefix, and a lease over what it writes there. Its segments go to
//! the job's own prefix (format spec, "Compaction") because a job outlives the
//! collector's grace window, so its output would otherwise look exactly like
//! the unreferenced aged objects the collector reaps; the lease beside them
//! ([`super::compaction_lease`]) is what tells the collector the difference. It
//! publishes nothing until it is finished, and a cancelled or crashed job
//! leaves staged objects nothing references and the old manifest still valid.
//! The step that plans it starts it and returns; the runner cancels it on
//! shutdown and joins it with the rest of its background work.

use super::block_fetch::{load_segment_filter, segment_object_len};
use super::block_load::SessionBlockMemo;
use super::build::MetadataTableDestination;
use super::cache::{MetadataTableCache, MetadataTableCacheConfig};
use super::compaction_lease::{CompactionLease, LeaseHold};
use super::compaction_merge::{
    locality_of, refill_iterators, select_next_iterator, LocalityGrouping, SegmentRowIterator,
};
use super::compaction_output::MergeSegmentWriter;
use super::compaction_retention::{KeptRow, RetentionRule};
use super::error::ManifestLoadError;
use super::flush::ensure_metadata_publication_budget;
use super::frozen_floor::{
    bind_survives_frozen_floor, unbinding_at_or_below_floor, unbindings_at_or_below_floor,
    BindingGeneration,
};
use super::load::{
    load_manifest_segment_rows_in_key_range_with_cache, load_verified_manifest_tables,
};
use super::publish::{publish_metadata_root, ManifestPublicationOutcome};
use super::reorganize::{group_run_descriptors, write_reorganized_manifest, MergePlacement};
use super::runs::{MetadataFamilyGroup, MetadataLsmPolicy, MetadataRunManifest};
use super::scan::{descriptor_may_intersect_range, Readahead, VerifiedMetadataTables};
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::control::read_metadata_root_object_if_present;
use crate::time::current_time_ms;
use crate::timing::StdMonotonicTimer;
use loonfs_api::wire::manifest::{lookup_keys, MetadataFileRef, MetadataRow, MetadataTableFamily};
use loonfs_api::wire::sst_blocks::string_prefix_upper_bound;
use loonfs_api::{ChangeSeq, ManifestId, MetadataCompactionId, NamespaceId};
use loonfs_objectstore::ObjectStore;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

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
    /// This job's identity, and the prefix its output and its lease live
    /// under. Generated with the plan, so two plans for one group never write
    /// into each other's prefix.
    job_id: MetadataCompactionId,
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
            job_id: MetadataCompactionId::generate(),
            group,
            inputs,
            input_rows,
            placement,
            frozen_floor_seq,
        }
    }

    /// The family group this job rebuilds, which is what a runtime keys its
    /// per-group bookkeeping by.
    pub fn group(&self) -> MetadataFamilyGroup {
        self.group
    }

    /// The job's identity, which names the prefix its output sits under.
    pub fn job_id(&self) -> &MetadataCompactionId {
        &self.job_id
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

    /// A plan over no runs at all, for tests that drive how a runtime admits
    /// jobs rather than what a job reads.
    ///
    /// Running one rebuilds nothing, which is the point: the tests that build
    /// these assert on slots, permits, and cancellation, and a plan naming
    /// runs would make them seed a namespace to say so.
    #[cfg(any(test, feature = "test-support"))]
    pub fn planned_over_no_runs() -> Self {
        Self::new(
            MetadataFamilyGroup::Bindings,
            Vec::new(),
            0,
            MergePlacement::Base {
                output_seq: ChangeSeq(0),
            },
            ChangeSeq(0),
        )
    }
}

/// Stops a job between block fetches, between finalization attempts, and
/// while it waits for whatever admission its runtime puts in front of it.
///
/// Cancelling costs the work done so far and nothing else: the staged
/// segments are unreferenced, the manifest never moved, and a later job runs
/// the same spec again.
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

/// How a job ended.
#[derive(Debug)]
pub(super) enum MetadataCompactionOutcome {
    Completed(MetadataMergeResult),
    /// The cancellation token was set. Whatever the job had written stays
    /// staged and unreferenced.
    Cancelled,
    /// A heartbeat lost its compare-and-swap, so garbage collection owns this
    /// job's prefix now. Whatever the job had written is being reaped, and
    /// the job must not publish descriptors naming it.
    Fenced,
}

/// What one finished merge built, held in memory until the caller publishes
/// it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct MetadataMergeResult {
    /// The output run's segment descriptors, naming the object keys the merge
    /// wrote them to.
    pub(super) output_segments: Vec<MetadataFileRef>,
    pub(super) rows_read: u64,
    pub(super) rows_written: u64,
    pub(super) input_bytes: u64,
    pub(super) output_bytes: u64,
    pub(super) rows_written_by_family: BTreeMap<MetadataTableFamily, u64>,
    /// Point reads into the snapshot's unbind family, one per reverse bind
    /// row at or below the frozen floor. Zero for a merge that resolves the
    /// reverse index from what it streamed
    /// ([`ReverseBindResolution::CollectedUnbinds`]).
    pub(super) unbind_probes: u64,
    /// Generation identities such a merge collected, which is what its reverse
    /// resolution holds instead of making those reads. Bounded by the below-
    /// floor unbind rows in the window, and therefore by the step's budgets.
    pub(super) collected_unbind_generations: usize,
    /// The most decoded input blocks the merge's iterators held at once, and
    /// the most rows one retention operator held. These are what bound the
    /// merge's memory, so tests assert they do not follow the size of the
    /// group, of one inode's history, or of one name slot's generations.
    pub(super) peak_resident_blocks: usize,
    pub(super) peak_operator_rows: usize,
}

/// Merges one window of complete runs inside the maintenance step that
/// selected it.
///
/// The engine is the job's engine. What this leaves out is everything the job
/// needs because it outlives its step: there is no lease, no staging prefix,
/// no registry entry, and no admission, and the segments are written at
/// ordinary table keys because the step publishes them itself a moment later.
///
/// `runs` is the window the step's budgets chose, and `placement` is where its
/// output stands in the group — which is also what decides whether rows may be
/// dropped.
pub(super) async fn merge_group_in_step<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    group: MetadataFamilyGroup,
    runs: &[MetadataRunManifest],
    placement: MergePlacement,
    frozen_floor_seq: ChangeSeq,
    policy: MetadataLsmPolicy,
) -> Result<MetadataMergeResult> {
    // A step-contained merge has nothing to cancel it: it holds the step, and
    // a shutdown waits for the step it is in.
    let cancellation = MetadataCompactionCancellation::default();
    let merge = GroupMerge::new(
        store,
        namespace_id,
        group,
        placement,
        frozen_floor_seq,
        MetadataTableDestination::Published { namespace_id },
        policy,
        &cancellation,
        runs.to_vec(),
        // The window is capped by the step's budgets, so the set of below-floor
        // unbound generations is capped with it and costs no store reads.
        ReverseBindResolution::CollectedUnbinds(BTreeSet::new()),
        // A step-contained merge is bounded by the step's input budgets and
        // ends in the step's own publication, so it has nothing to report
        // progress about.
        None,
    );
    match merge.run(None).await? {
        MetadataCompactionOutcome::Completed(result) => Ok(result),
        // Neither ending is reachable without a token to set and a lease to
        // lose, and this merge has neither.
        MetadataCompactionOutcome::Cancelled | MetadataCompactionOutcome::Fenced => {
            Err(CoreError::Internal(
                "a step-contained metadata merge cannot be cancelled or fenced".to_owned(),
            ))
        }
    }
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
    lease: &mut CompactionLease<'_>,
) -> Result<MetadataCompactionOutcome> {
    let merge = GroupMerge::new(
        tables.store,
        namespace_id,
        spec.group,
        spec.placement,
        spec.frozen_floor_seq,
        MetadataTableDestination::CompactionStaging {
            namespace_id,
            job_id: spec.job_id(),
        },
        policy,
        cancellation,
        resolve_snapshot_runs(tables, spec)?,
        // A job has no bound on the group it rebuilds, so it reads the
        // snapshot per reverse row rather than holding a set that would follow
        // the group's size.
        ReverseBindResolution::PointProbeSnapshot,
        Some(spec.input_rows()),
    );
    merge.run(Some(lease)).await
}

/// How one cluster's merge ended.
enum ClusterEnd {
    /// Every row of the cluster was merged and its segments written.
    Merged,
    Cancelled,
    Fenced,
}

/// How one background compaction job ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataCompactionJobOutcome {
    /// The rebuilt group replaced its snapshot in a published manifest.
    Published {
        manifest_id: ManifestId,
        rows_read: u64,
        rows_written: u64,
        input_bytes: u64,
        output_bytes: u64,
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
    /// The job lost its lease: it stopped heartbeating for longer than the
    /// lease expiry and garbage collection claimed its prefix. Ownership does
    /// not come back, so the job publishes nothing and the collector reclaims
    /// what it wrote. A later step plans the group again.
    Fenced,
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
    /// The cancellation token was set before this finalization published.
    /// Nothing was swapped in, so the ending is the executor's: staged output
    /// nothing references, and the manifest where it was.
    Cancelled,
    /// The heartbeat at the top of an attempt lost its compare-and-swap, so
    /// this job no longer owns the segments it was about to name.
    Fenced,
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
    // The lease is written before the first output object, so no object under
    // the job's prefix is ever unclaimed.
    let mut lease = CompactionLease::new(
        namespace_id,
        spec.job_id(),
        &context.writer_id,
        context.now_ms,
        &timer,
    );
    lease.create(store).await?;
    tracing::info!(
        namespace_id = namespace_id.as_str(),
        job_id = spec.job_id().as_str(),
        families = ?spec.families(),
        input_runs = spec.input_runs(),
        input_rows = spec.input_rows(),
        frozen_floor_seq = spec.frozen_floor_seq().0,
        "streaming metadata compaction started"
    );

    let result = match run_metadata_compaction(
        &tables,
        namespace_id,
        spec,
        policy,
        cancellation,
        &mut lease,
    )
    .await?
    {
        MetadataCompactionOutcome::Completed(result) if !cancellation.is_cancelled() => result,
        // The prefix belongs to garbage collection now, and the segments the
        // job wrote are being reaped. Publishing descriptors naming them is
        // exactly what the fence exists to stop.
        MetadataCompactionOutcome::Fenced => {
            tracing::warn!(
                namespace_id = namespace_id.as_str(),
                job_id = spec.job_id().as_str(),
                families = ?spec.families(),
                "streaming metadata compaction fenced: garbage collection claimed its prefix \
                 while it ran, so it publishes nothing and a later step plans it again"
            );
            return Ok(MetadataCompactionJobOutcome::Fenced);
        }
        // A token set after the last row still costs the job. Checked here so
        // a shutdown does not spend the drain building a manifest and taking
        // races for a publication it has already decided against.
        MetadataCompactionOutcome::Completed(_) | MetadataCompactionOutcome::Cancelled => {
            // The executor stops between block fetches, so what it wrote is
            // whatever segments had already filled. They are staged and named
            // by nothing. The lease is left where it is: it expires on its
            // own, and a pass that arrives before it does keeps a dead job's
            // orphans a while longer, which costs storage and nothing else.
            tracing::info!(
                namespace_id = namespace_id.as_str(),
                job_id = spec.job_id().as_str(),
                families = ?spec.families(),
                "streaming metadata compaction cancelled"
            );
            return Ok(MetadataCompactionJobOutcome::Cancelled);
        }
    };
    drop(tables);

    let rows_read = result.rows_read;
    let rows_written = result.rows_written;
    let input_bytes = result.input_bytes;
    let output_bytes = result.output_bytes;
    let output_segments = result.output_segments.len();
    match finalize_metadata_compaction(
        store,
        namespace_id,
        spec,
        &snapshot_keys,
        result,
        cancellation,
        &mut lease,
    )
    .await?
    {
        Finalization::Published(manifest_id) => {
            // The job stops heartbeating here and leaves its final lease
            // where it is. Deleting it would break the handoff: a collection
            // pass that captured its live set before this publication would
            // find segments hours older than any grace window and no lease
            // saying who owns them. The fresh lease covers that pass, and the
            // next one reads a root that already names the segments and
            // removes only the expired lease.
            tracing::info!(
                namespace_id = namespace_id.as_str(),
                job_id = spec.job_id().as_str(),
                families = ?spec.families(),
                rows_read,
                rows_written,
                input_bytes,
                output_bytes,
                output_segments,
                manifest_id = manifest_id.0,
                "streaming metadata compaction published"
            );
            Ok(MetadataCompactionJobOutcome::Published {
                manifest_id,
                rows_read,
                rows_written,
                input_bytes,
                output_bytes,
                output_segments,
            })
        }
        Finalization::Abandoned => Ok(MetadataCompactionJobOutcome::Abandoned),
        Finalization::Superseded => Ok(MetadataCompactionJobOutcome::Superseded),
        Finalization::Fenced => {
            tracing::warn!(
                namespace_id = namespace_id.as_str(),
                job_id = spec.job_id().as_str(),
                families = ?spec.families(),
                "streaming metadata compaction fenced while finalizing: garbage collection owns \
                 its prefix, so nothing was published"
            );
            Ok(MetadataCompactionJobOutcome::Fenced)
        }
        Finalization::Cancelled => {
            tracing::info!(
                namespace_id = namespace_id.as_str(),
                job_id = spec.job_id().as_str(),
                families = ?spec.families(),
                "streaming metadata compaction cancelled while finalizing"
            );
            Ok(MetadataCompactionJobOutcome::Cancelled)
        }
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
/// the last few seconds and not of however long the rebuild took. That is the
/// same span the lease has to cover, so both are measured off the lease's own
/// clock.
///
/// The root is stamped with the wall clock each attempt reads rather than the
/// one the job started under. A job that ran for hours would otherwise stamp
/// the root with its start time, and a job rebasing over a newer flush would
/// move the root's `updated_at_ms` backwards. The job's identity is the other
/// half of what a publication carries, and that stays frozen: the writer this
/// job runs as is the writer that planned it.
pub(super) async fn finalize_metadata_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    snapshot_keys: &BTreeSet<String>,
    result: MetadataMergeResult,
    cancellation: &MetadataCompactionCancellation,
    lease: &mut CompactionLease<'_>,
) -> Result<Finalization> {
    let timer = lease.timer();
    for attempt in 1..=MAX_FINALIZATION_ATTEMPTS {
        // Checked at the top of every attempt, not only the first: the
        // attempts after a lost race are exactly the wait a shutdown must not
        // sit through.
        if cancellation.is_cancelled() {
            return Ok(Finalization::Cancelled);
        }
        // The claim is refreshed at the top of every attempt, and losing that
        // compare-and-swap ends the job here. This is the check that makes
        // the fence complete: the span from here to the root compare-and-swap
        // below is one publication budget, which is shorter than the lease
        // expiry, so a job that gets past this line cannot have its prefix
        // claimed before the swap that makes its output referenced
        // (`limits::METADATA_COMPACTION_LEASE_EXPIRY_MS`).
        if lease.heartbeat(store).await? == LeaseHold::Fenced {
            return Ok(Finalization::Fenced);
        }
        let publication_started_ms = timer.monotonic_now_ms();
        let Some(root) = read_metadata_root_object_if_present(store, namespace_id)
            .await
            .map_err(CoreError::load_head)?
            .map(|loaded| loaded.state)
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

        // The last check before the swap that makes this output reader
        // truth. Everything above it is reads and objects nothing references.
        if cancellation.is_cancelled() {
            return Ok(Finalization::Cancelled);
        }
        ensure_metadata_publication_budget(timer, publication_started_ms, namespace_id)?;
        let published = publish_metadata_root(
            store,
            namespace_id,
            &manifest,
            Some(root.manifest_object_id.clone()),
            current_time_ms()?,
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
        .map(|loaded| loaded.state)
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

/// How a merge decides whether a reverse bind row survives the frozen floor.
///
/// The reverse index is keyed by child while the unbind that retires a bind is
/// keyed by parent, so no grouping of the merged stream holds a reverse row
/// together with its unbind. Both answers below come from the same rule
/// ([`bind_survives_frozen_floor`]) against the same set
/// ([`unbinding_at_or_below_floor`]); they differ only in where the set comes
/// from, because the two orchestrations have different resource contracts.
///
/// This parameterizes the lookup and nothing else. Iteration, retention,
/// segment writing, parity, and publication are the same code either way.
enum ReverseBindResolution {
    /// Read the unbinds of one binding out of the snapshot, one probe per
    /// reverse row at or below the floor.
    ///
    /// What a background job uses. A job has no bound on the group it
    /// rebuilds, so it must not hold a set that follows the group's size; a
    /// bounded cache in front of the reads is the most it can keep.
    PointProbeSnapshot,
    /// Consult the below-floor unbound generations the forward cluster already
    /// streamed past.
    ///
    /// What a merge inside a maintenance step uses. Its input is capped by the
    /// step's row and decoded-byte budgets, so this set is capped by the same
    /// budgets — it holds one generation identity per below-floor unbind in
    /// the window, and never more than the window itself.
    ///
    /// The alternative costs one store round trip per reverse row once the
    /// probe cache can no longer hold the window's unbind family. The row
    /// budget admits about 43,000 unbind rows, which reaches the 16 MiB cache
    /// at an ordinary name length, and the decoded-byte budget allows four
    /// times that. Measured on a window whose unbind family just fills the
    /// cache, with the reverse index walking it out of order: 65,536 reverse
    /// rows cost 27,427 data reads and 309 MB transferred for 16 MB of priced
    /// input, and doubling the family doubled both. This set is also the
    /// smaller resident structure — one identity per below-floor unbind, well
    /// under the cache it replaces.
    ///
    /// This is filled by the forward cluster, which
    /// [`BINDINGS_CLUSTERS`] runs first for exactly that reason.
    CollectedUnbinds(BTreeSet<BindingGeneration>),
}

/// One set of families the engine merges and judges together, and how it
/// groups their rows while doing it.
pub(super) struct RetentionCluster {
    pub(super) families: &'static [MetadataTableFamily],
    pub(super) locality: LocalityGrouping,
    pub(super) rule: RetentionRule,
}

/// The bindings group merges its two forward families together, because an
/// unbind retires the bind of its generation. The reverse index is keyed by
/// child, so it shares no group with the rows it indexes and streams on its
/// own.
///
/// The forward cluster is first, and must stay first: a merge resolving
/// reverse rows from [`ReverseBindResolution::CollectedUnbinds`] fills that set
/// while streaming the unbind family here.
const BINDINGS_CLUSTERS: [RetentionCluster; 2] = [
    RetentionCluster {
        families: &[
            MetadataTableFamily::DirentryBinds,
            MetadataTableFamily::DirentryUnbinds,
        ],
        locality: LocalityGrouping::LeadingKeyComponents(4),
        rule: RetentionRule::ForwardBindings,
    },
    RetentionCluster {
        families: &[MetadataTableFamily::DirentryChildBinds],
        locality: LocalityGrouping::Row,
        rule: RetentionRule::ReverseBindProbe,
    },
];

/// A family no rule ever drops a row from, rewritten in key order.
const fn row_cluster(families: &'static [MetadataTableFamily]) -> RetentionCluster {
    RetentionCluster {
        families,
        locality: LocalityGrouping::Row,
        rule: RetentionRule::KeepEveryRow,
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
const RECEIPT_CLUSTERS: [RetentionCluster; 1] = [RetentionCluster {
    families: &[MetadataTableFamily::CommitReceipts],
    locality: LocalityGrouping::Row,
    rule: RetentionRule::Receipts,
}];
const ACTIVE_DELETION_CLUSTERS: [RetentionCluster; 1] = [RetentionCluster {
    families: &[MetadataTableFamily::ActiveDeletions],
    locality: LocalityGrouping::LeadingKeyComponents(2),
    rule: RetentionRule::ActiveDeletions,
}];
const ATTRIBUTE_CLUSTERS: [RetentionCluster; 1] = [RetentionCluster {
    families: &[MetadataTableFamily::Attributes],
    locality: LocalityGrouping::LeadingKeyComponents(1),
    rule: RetentionRule::Attributes,
}];

pub(super) fn retention_clusters(group: MetadataFamilyGroup) -> &'static [RetentionCluster] {
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

/// Merging one family group: the shared engine, whatever is driving it.
struct GroupMerge<'a, S: ObjectStore + ?Sized> {
    store: &'a S,
    namespace_id: &'a NamespaceId,
    group: MetadataFamilyGroup,
    /// Where the output stands in the group, which decides the level and
    /// sequence every segment carries and whether rows may be dropped at all.
    placement: MergePlacement,
    frozen_floor_seq: ChangeSeq,
    destination: MetadataTableDestination<'a>,
    policy: MetadataLsmPolicy,
    cancellation: &'a MetadataCompactionCancellation,
    snapshot: Vec<MetadataRunManifest>,
    reverse_binds: ReverseBindResolution,
    probe_cache: MetadataTableCache,
    result: MetadataMergeResult,
    canonical_digest: RowDigest,
    index_digest: RowDigest,
    /// The last input row key seen in each family, which is what lets the merge
    /// refuse a family that holds one row key twice. One string per family.
    last_input_key_by_family: BTreeMap<MetadataTableFamily, String>,
    /// The rows the input runs hold, and the read count the next progress line
    /// is owed at. `None` for a merge short enough to have nothing to report.
    input_rows: Option<u64>,
    next_progress_rows: Option<u64>,
}

impl<'a, S: ObjectStore + ?Sized> GroupMerge<'a, S> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        store: &'a S,
        namespace_id: &'a NamespaceId,
        group: MetadataFamilyGroup,
        placement: MergePlacement,
        frozen_floor_seq: ChangeSeq,
        destination: MetadataTableDestination<'a>,
        policy: MetadataLsmPolicy,
        cancellation: &'a MetadataCompactionCancellation,
        snapshot: Vec<MetadataRunManifest>,
        reverse_binds: ReverseBindResolution,
        input_rows: Option<u64>,
    ) -> Self {
        let input_bytes = snapshot
            .iter()
            .flat_map(|run| group_run_descriptors(run, group))
            .map(segment_object_len)
            .sum();
        Self {
            store,
            namespace_id,
            group,
            placement,
            frozen_floor_seq,
            destination,
            policy,
            cancellation,
            snapshot,
            reverse_binds,
            probe_cache: MetadataTableCache::new(MetadataTableCacheConfig {
                max_decoded_bytes: PROBE_CACHE_DECODED_BYTES,
            }),
            result: MetadataMergeResult {
                input_bytes,
                ..MetadataMergeResult::default()
            },
            canonical_digest: RowDigest::default(),
            index_digest: RowDigest::default(),
            last_input_key_by_family: BTreeMap::new(),
            input_rows,
            next_progress_rows: input_rows.map(|_| PROGRESS_ROW_INTERVAL),
        }
    }

    /// Merges every cluster of the group, in order.
    ///
    /// `lease` is the claim a caller that stages its output holds over it. A
    /// merge that publishes inside its own step has none, and passes `None`.
    async fn run(
        mut self,
        mut lease: Option<&mut CompactionLease<'_>>,
    ) -> Result<MetadataCompactionOutcome> {
        for cluster in retention_clusters(self.group) {
            match self.run_cluster(cluster, lease.as_deref_mut()).await? {
                ClusterEnd::Merged => {}
                ClusterEnd::Cancelled => return Ok(MetadataCompactionOutcome::Cancelled),
                ClusterEnd::Fenced => return Ok(MetadataCompactionOutcome::Fenced),
            }
        }
        self.refuse_a_run_whose_index_disagrees()?;
        Ok(MetadataCompactionOutcome::Completed(self.result))
    }

    /// Refuses a family that hands the merge one row key twice, and refuses a
    /// merge that hands itself a family out of order.
    ///
    /// A metadata row key identifies one row. Nothing downstream re-checks
    /// that: reads concatenate runs rather than deduplicating them, and the
    /// segment builder rejects only a descending key, so two equal adjacent
    /// keys travel into a published run and make the same logical event
    /// answerable twice.
    ///
    /// This runs over the merge's input rather than its output, which is what
    /// makes it see a duplicate retention would otherwise drop, a duplicate
    /// split across two runs, and a duplicate split across two segments.
    ///
    /// It stands beside the output digests
    /// ([`Self::refuse_a_run_whose_index_disagrees`]) because the two prove
    /// different things. The digests say the two families of an index pair hold
    /// the same rows as each other; a duplicate present on both sides passes
    /// them, because both multisets still match. This says no family holds one
    /// row key twice, whatever the other family holds.
    ///
    /// A key that goes backwards is not corruption in the store — the k-way
    /// merge emits one family in row-key order — so that case is an internal
    /// error against the merge itself.
    fn refuse_a_repeated_input_key(
        &mut self,
        family: MetadataTableFamily,
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
        mut lease: Option<&mut CompactionLease<'_>>,
    ) -> Result<ClusterEnd> {
        // The descriptors are cloned out of the snapshot rather than borrowed
        // from it: an iterator outlives every other borrow the merge takes, and
        // a cluster opens one per run per family, which is a handful.
        let mut iterators = Vec::new();
        for run in &self.snapshot {
            for family in cluster.families {
                let segments: Vec<MetadataFileRef> = group_run_descriptors(run, self.group)
                    .filter(|descriptor| descriptor.family == *family)
                    .cloned()
                    .collect();
                if !segments.is_empty() {
                    iterators.push(SegmentRowIterator::new(*family, segments));
                }
            }
        }
        let mut writers: BTreeMap<MetadataTableFamily, MergeSegmentWriter> = cluster
            .families
            .iter()
            .map(|family| {
                (
                    *family,
                    MergeSegmentWriter::new(*family, self.destination, self.placement),
                )
            })
            .collect();

        let floor_seq = self.frozen_floor_seq;
        let rule = self.rule_for(cluster);
        let mut operator = rule.operator();
        let mut locality: Option<String> = None;
        loop {
            if self.cancellation.is_cancelled() {
                return Ok(ClusterEnd::Cancelled);
            }
            // The claim on a staged merge's prefix is refreshed where it checks
            // whether it should stop, which is the one place it is guaranteed
            // to reach however long a merge runs. Losing it is one more way the
            // merge has to stop: the segments it has written belong to the
            // collector now.
            if let Some(lease) = lease.as_deref_mut() {
                if lease.heartbeat_if_due(self.store).await? == LeaseHold::Fenced {
                    return Ok(ClusterEnd::Fenced);
                }
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
                let family = iterator.family;
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
            let family = iterators[next].family;
            let row = iterators[next].take_head();
            self.result.rows_read += 1;
            self.report_progress();
            self.collect_unbinding(&row);
            let kept = match rule {
                // The reverse index is the one family no grouping can decide,
                // so the merge reads the snapshot for it rather than holding
                // state.
                RetentionRule::ReverseBindProbe => self
                    .reverse_bind_survives(&row)
                    .await?
                    .then_some((family, row)),
                _ => operator.push(family, row, floor_seq)?,
            };
            self.result.peak_operator_rows =
                self.result.peak_operator_rows.max(operator.held_rows());
            if let Some(kept) = kept {
                self.write_row(kept, &mut writers).await?;
            }
        }
        if let Some(kept) = operator.close_group(floor_seq)? {
            self.write_row(kept, &mut writers).await?;
        }

        for writer in writers.into_values() {
            let segments = writer.finish(self.store).await?;
            self.result.output_bytes = self
                .result
                .output_bytes
                .saturating_add(segments.iter().map(segment_object_len).sum());
            self.result.output_segments.extend(segments);
        }
        Ok(ClusterEnd::Merged)
    }

    /// Says where a long job has got to, at [`PROGRESS_ROW_INTERVAL`].
    ///
    /// A job has no bound on how long it runs, and it publishes nothing until
    /// it is finished, so without this an operator watching a big namespace
    /// sees one line at the start and nothing until it lands. A merge that runs
    /// inside a maintenance step reports nothing: its input is capped by the
    /// step's budgets and the step publishes it. The counters are the merge's
    /// own; nothing is measured for this.
    fn report_progress(&mut self) {
        let (Some(next_progress_rows), Some(input_rows)) =
            (self.next_progress_rows, self.input_rows)
        else {
            return;
        };
        if self.result.rows_read < next_progress_rows {
            return;
        }
        self.next_progress_rows = Some(self.result.rows_read.saturating_add(PROGRESS_ROW_INTERVAL));
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
    async fn refill(&mut self, iterators: &mut [SegmentRowIterator]) -> Result<()> {
        let resident = refill_iterators(self.store, iterators).await?;
        self.result.peak_resident_blocks = self.result.peak_resident_blocks.max(resident);
        Ok(())
    }

    /// Writes one row a retention operator kept, rolling a segment when its
    /// family's builder fills.
    async fn write_row(
        &mut self,
        (family, row): KeptRow,
        writers: &mut BTreeMap<MetadataTableFamily, MergeSegmentWriter<'_>>,
    ) -> Result<()> {
        self.fold_into_index_digests(family, &row)?;
        *self
            .result
            .rows_written_by_family
            .entry(family)
            .or_default() += 1;
        self.result.rows_written += 1;
        let writer = writers
            .get_mut(&family)
            .expect("a cluster writes only the families it merges");
        writer.push(row);
        writer.roll_full_segments(self.store, self.policy).await
    }

    /// Remembers a below-floor unbind for the reverse pass, when the merge is
    /// resolving reverse rows from what it streamed rather than from the store.
    ///
    /// Every unbind row of the snapshot goes through here, because the forward
    /// cluster streams the whole unbind family before the reverse cluster
    /// starts. That is what makes the collected set answer exactly what a
    /// point read into the same snapshot would answer.
    fn collect_unbinding(&mut self, row: &MetadataRow) {
        let ReverseBindResolution::CollectedUnbinds(unbound_at_floor) = &mut self.reverse_binds
        else {
            return;
        };
        if let Some(generation) = unbinding_at_or_below_floor(row, self.frozen_floor_seq) {
            unbound_at_floor.insert(generation);
            self.result.collected_unbind_generations = unbound_at_floor.len();
        }
    }

    /// Whether one reverse bind row survives the frozen floor.
    ///
    /// The reverse index is keyed by child and the unbind family by parent, so
    /// no grouping of the merged stream can hold a reverse row together with
    /// the unbind that retires it. Both resolutions close that the same way:
    /// they run [`bind_survives_frozen_floor`] against the generations
    /// [`unbinding_at_or_below_floor`] retired, out of the same immutable
    /// snapshot and against the same frozen floor as the forward pass. So the
    /// two bind families drop in lockstep — which they must, because the format
    /// gives every bind row exactly one reverse row and a run whose two counts
    /// disagree does not load.
    ///
    /// They differ only in where the set comes from
    /// ([`ReverseBindResolution`]). A collected set answers with no read at
    /// all. A probe reads the unbinds of one binding, and only rows at or
    /// below the floor cost one: a bind above the floor survives whatever
    /// retired it later.
    async fn reverse_bind_survives(&mut self, row: &MetadataRow) -> Result<bool> {
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
        if *bind_seq > self.frozen_floor_seq {
            return Ok(true);
        }
        if let ReverseBindResolution::CollectedUnbinds(unbound_at_floor) = &self.reverse_binds {
            return Ok(bind_survives_frozen_floor(
                row,
                self.frozen_floor_seq,
                unbound_at_floor,
            ));
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
        let unbound_at_floor = unbindings_at_or_below_floor(&unbind_rows, self.frozen_floor_seq);
        Ok(bind_survives_frozen_floor(
            row,
            self.frozen_floor_seq,
            &unbound_at_floor,
        ))
    }

    /// One point read into the snapshot's unbind family.
    ///
    /// The unbind key grammar leads with the binding a row names, so the
    /// prefix selects the unbinds of that one binding and nothing else. The
    /// bloom filter is keyed by parent and name, so a binding no operation
    /// ever retired misses it outright and costs no index or data fetch.
    /// Decoded sections land in the merge's own bounded cache, and the per-read
    /// memo is dropped with the read, so what these reads hold is the cache's
    /// byte budget however many of them the merge makes.
    async fn read_unbinds_of_binding(
        &self,
        prefix: &str,
        filter_probe: &str,
    ) -> Result<Vec<MetadataRow>> {
        let upper_bound = string_prefix_upper_bound(prefix);
        let mut rows = Vec::new();
        for run in &self.snapshot {
            for descriptor in group_run_descriptors(run, self.group)
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
        let Some((canonical, index)) = index_pair(self.group) else {
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
