//! Runs one metadata compaction unit.
//!
//! A unit selects one family group and a window of at most
//! `max_input_runs_per_step` complete runs from the current manifest's
//! descriptors (see [`super::plan`]), rewrites the whole window into one run
//! with [`super::group_compaction`] whatever the number of rows its runs list,
//! and the unit's turn in the writer session's publication lane publishes the
//! replacement manifest through [`super::publication`]. A base compaction may apply retention,
//! and a delta compaction drops only superseded layouts. The namespace's
//! [`MetadataRunPolicy`](crate::segment::MetadataRunPolicy) sets which runs a
//! group compacts, and when: a tier of similar-size runs once it has
//! `min_compaction_runs` runs, and a group's newest runs once it has
//! `max_delta_runs` delta runs.

use super::group_compaction::{run_metadata_compaction, GroupCompactionReport};
use super::plan::{plan_compaction, select_family_group, MetadataCompactionSpec};
use super::publication::input_segment_keys;
use crate::commit::publish_view::WriterPosition;
use crate::engine::{NamespaceEngine, Writable};
use crate::error::Result;
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Held;
use crate::phase::Phase;
use crate::segment::runs::{runs_newest_first, MetadataFamilyGroup};
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{MetadataSkippedWork, NamespaceId};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// How one compaction unit ended.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(
    clippy::large_enum_variant,
    reason = "one value per compaction unit, moved once: indirection would buy an allocation and save nothing"
)]
pub enum CompactionUnitOutcome {
    /// No family group has eligible input.
    NotNeeded,
    /// The unit compacted one family group's window and published the next
    /// manifest. The new run is the group's base when the window started at
    /// the group's oldest run, and a bigger delta run otherwise.
    Published {
        /// The group this unit compacted. Its families are
        /// [`MetadataFamilyGroup::families`].
        group: MetadataFamilyGroup,
        /// The published manifest.
        manifest: Held<LoadedManifest>,
        input_runs: usize,
        rows_read: u64,
        rows_written: u64,
        input_bytes: u64,
        output_bytes: u64,
        output_segments: usize,
        /// The work that the unit skipped at a limit, each reason once.
        skipped: Vec<MetadataSkippedWork>,
    },
    /// The caller cancelled the unit before it published.
    Cancelled,
    /// The unit's inputs changed or its time bound passed.
    Abandoned,
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Checks `manifest`'s descriptors for a compaction unit that is due
    /// under the namespace's run policy, without reading the store.
    pub fn metadata_compaction_due(&self, manifest: &LoadedManifest) -> bool {
        let payload = manifest.envelope.payload();
        select_family_group(
            &runs_newest_first(payload),
            payload,
            self.namespace.run_policy,
        )
        .is_some()
    }
}

/// How the writes of a compaction unit before its turn in the session's
/// publication lane ended (see
/// [`NamespaceEngine::write_metadata_compaction_unit`]).
#[derive(Debug)]
pub enum CompactionWriteOutcome {
    /// The unit ended without output to publish: nothing was due, or the
    /// unit was cancelled or abandoned while it wrote.
    Ended(CompactionUnitOutcome),
    /// The unit wrote its output segments.
    Written(CompactionWrites),
}

/// The output segments that a compaction unit wrote from the runs of a
/// writer position's manifest, which the unit's publication lists in its
/// turn (see [`NamespaceStore::publish_compaction_in_turn`](crate::NamespaceStore::publish_compaction_in_turn)).
#[derive(Debug)]
pub struct CompactionWrites {
    pub(crate) spec: MetadataCompactionSpec,
    /// The segments of the input runs, which the manifest that the unit
    /// publishes over must still list.
    pub(crate) input_keys: BTreeSet<String>,
    pub(crate) report: GroupCompactionReport,
    /// Started before the unit planned, for the unit's time bound.
    pub(crate) compaction: OperationClock,
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Plans one metadata compaction unit of a writer session from the
    /// manifest of `position`, the session's position, and writes its output
    /// segments, before the unit takes its turn in the session's publication
    /// lane.
    ///
    /// A position whose manifest is missing from the store and at or below
    /// the manifest boundary returns `head_moved`, and a missing input
    /// segment of a manifest that a newer manifest replaced returns
    /// `manifest_replaced` (see
    /// [`SegmentKeeper::load_error`](crate::segment::SegmentKeeper::load_error)),
    /// as a fold's writes do. Either way the session drops its position and
    /// loads it again under its writer epoch, which returns `writer_fenced`
    /// when another writer claimed the namespace. The unit reads its input runs
    /// without the cache. The walk that builds a base compaction's kept inode
    /// filter reads through the cache. The unit compacts the whole window
    /// that the namespace's run policy selects, whatever the number of rows
    /// its runs list, and the compaction checks `cancellation` between
    /// blocks. The unit's time bound starts at the start of the call (see
    /// `METADATA_COMPACTION_BUDGET_MS`).
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.phase",
        err(level = "warn"),
        skip_all,
        fields(phase = Phase::MetadataCompactionStep.as_str(), key_class = "manifest")
    )]
    pub async fn write_metadata_compaction_unit(
        &self,
        position: &WriterPosition,
        cancellation: &MaintenanceCancellation,
    ) -> Result<CompactionWriteOutcome> {
        let compaction = self.namespace.start_clock();
        let basis = self.namespace.load_publication_basis(position).await?;
        let current = basis.manifest();
        let payload = current.payload();
        // The unit reads its input runs without the cache, and the compaction inserts
        // its output into the cache. The walk that builds the kept inode filter
        // scans one directory at a time. A scan keeps the data blocks it fetches
        // in the cache, not in its own memo, so the walk reads through the
        // cache, and a block that contains the rows of many directories is
        // fetched once.
        let segments = self.namespace.current_segments(current).segments;
        let Some(spec) = plan_compaction(payload, &segments.scan_runs, self.namespace.run_policy)
        else {
            return Ok(CompactionWriteOutcome::Ended(
                CompactionUnitOutcome::NotNeeded,
            ));
        };
        // What the unit is about to read, recorded before it reads anything.
        // The publication compares the manifest against this, so the run it
        // publishes stands in for exactly the segments it compacted.
        let input_keys = input_segment_keys(&segments.scan_runs, &spec)
            .expect("a plan should list runs of the manifest it was made from");
        tracing::debug!(
            namespace_id = self.namespace.namespace_id.as_str(),
            job_id = spec.job_id().as_str(),
            families = ?spec.families(),
            input_runs = spec.input_runs(),
            input_rows = spec.input_rows(),
            frozen_floor_seq = spec.frozen_floor_seq().0,
            "metadata compaction unit started"
        );
        let ran = run_metadata_compaction(
            &segments,
            &self.namespace.segment_cache,
            &self.namespace.namespace_id,
            &spec,
            self.namespace.run_policy,
            cancellation,
        )
        .await;
        let stopped = match ran {
            Ok(Ok(report)) => {
                return Ok(CompactionWriteOutcome::Written(CompactionWrites {
                    spec,
                    input_keys,
                    report,
                    compaction,
                }))
            }
            Ok(Err(stopped)) => stopped,
            Err(error) => return Err(error),
        };
        log_unit_outcome(&self.namespace.namespace_id, &spec, &stopped);
        Ok(CompactionWriteOutcome::Ended(stopped))
    }
}

pub(super) fn log_unit_outcome(
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    outcome: &CompactionUnitOutcome,
) {
    match outcome {
        CompactionUnitOutcome::Published {
            manifest,
            input_runs,
            rows_read,
            rows_written,
            input_bytes,
            output_bytes,
            output_segments,
            skipped,
            ..
        } => tracing::info!(
            namespace_id = namespace_id.as_str(),
            job_id = spec.job_id().as_str(),
            families = ?spec.families(),
            input_runs,
            rows_read,
            rows_written,
            input_bytes,
            output_bytes,
            output_segments,
            ?skipped,
            manifest_no = manifest.manifest_no().0,
            "metadata compaction unit published"
        ),
        CompactionUnitOutcome::Cancelled => tracing::info!(
            namespace_id = namespace_id.as_str(),
            job_id = spec.job_id().as_str(),
            families = ?spec.families(),
            "metadata compaction unit cancelled"
        ),
        CompactionUnitOutcome::Abandoned => tracing::info!(
            namespace_id = namespace_id.as_str(),
            job_id = spec.job_id().as_str(),
            families = ?spec.families(),
            "metadata compaction unit abandoned"
        ),
        CompactionUnitOutcome::NotNeeded => {}
    }
}

/// A token that stops the maintenance that takes it.
///
/// It stops a compaction unit at its next block, and it ends the waits of a
/// unit, a fold or a snapshot for a lock or a permit. A host's call has one,
/// and so does a writer session, whose stop the session's close, its fence
/// and the runtime's shutdown set. Clones share one state.
#[derive(Debug, Clone, Default)]
pub struct MaintenanceCancellation {
    /// The state that [`Self::cancel`] sets.
    own: Arc<Cancellation>,
    /// The states of the tokens that this token joins (see [`Self::joined`]),
    /// any of which cancels this token too.
    joined: Vec<Arc<Cancellation>>,
}

#[derive(Debug, Default)]
struct Cancellation {
    cancelled: AtomicBool,
    /// Wakes whoever is waiting rather than reading rows. A unit queued behind
    /// its runtime's admission has no block fetch to check the flag between,
    /// so a shutdown would otherwise have to wait for a permit it is trying
    /// to stop needing.
    woken: Notify,
}

impl MaintenanceCancellation {
    /// Creates an uncancelled token.
    pub fn new() -> Self {
        Self::default()
    }

    /// Cancels the token.
    pub fn cancel(&self) {
        self.own.cancelled.store(true, Ordering::SeqCst);
        self.own.woken.notify_waiters();
    }

    /// Returns a token that is cancelled once this token or `other` is, such
    /// as the token of a unit that a host runs through a writer session,
    /// which stops for the host's cancellation and for the session's stop.
    /// Cancelling the returned token cancels neither of the two.
    pub fn joined(&self, other: &Self) -> Self {
        Self {
            own: Arc::default(),
            joined: [self, other]
                .into_iter()
                .flat_map(|token| std::iter::once(&token.own).chain(&token.joined))
                .cloned()
                .collect(),
        }
    }

    /// Whether the token has been set. What a unit reads between block
    /// fetches, and what a caller about to start work reads first.
    pub fn is_cancelled(&self) -> bool {
        self.states()
            .any(|state| state.cancelled.load(Ordering::SeqCst))
    }

    /// Resolves once the token is set, and stays pending while it is not.
    ///
    /// For a caller waiting on something else (such as a permit or a queue)
    /// that has to stop waiting when the unit is cancelled.
    pub async fn cancelled(&self) {
        let mut woken: Vec<_> = self
            .states()
            .map(|state| Box::pin(state.woken.notified()))
            .collect();
        // Registered before the flags are read, which is what closes the race
        // with a `cancel` arriving between the read and the await: the notify
        // then wakes this waiter rather than passing it by.
        for woken in &mut woken {
            woken.as_mut().enable();
        }
        if self.is_cancelled() {
            return;
        }
        futures::future::select_all(woken).await;
    }

    /// Returns this token's own state and the states that it joins.
    fn states(&self) -> impl Iterator<Item = &Cancellation> {
        std::iter::once(&*self.own).chain(self.joined.iter().map(|state| &**state))
    }
}
