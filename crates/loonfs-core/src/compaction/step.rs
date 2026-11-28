//! Runs one metadata compaction unit.
//!
//! A unit selects one family group and a window of at most
//! `max_input_runs_per_step` complete runs from the current manifest's
//! descriptors (see [`super::plan`]), rewrites the whole window into one run
//! with [`super::group_compaction`] whatever the number of rows its runs list,
//! and publishes the replacement
//! manifest through [`super::publication`]. A base compaction may apply retention,
//! and a delta compaction drops only superseded layouts. The namespace's
//! [`MetadataRunPolicy`](crate::segment::MetadataRunPolicy) sets which runs a
//! group compacts, and when: a tier of similar-size runs once it has
//! `min_compaction_runs` runs, and a group's newest runs once it has
//! `max_delta_runs` delta runs.

use super::group_compaction::run_metadata_compaction;
use super::plan::{plan_compaction, select_family_group, MetadataCompactionSpec};
use super::publication::{input_segment_keys, CompactionPublication};
use crate::commit::publication_lane::PublicationLane;
use crate::commit::publish_view::WriterPosition;
use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::namespace::control::{ensure_namespace_live, LoadedManifest};
use crate::namespace::held::Held;
use crate::phase::Phase;
use crate::segment::runs::{runs_newest_first, MetadataFamilyGroup};
use loonfs_objectstore::ObjectStore;
use loonfs_types::{MetadataSkippedWork, NamespaceId};
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
    /// The unit's inputs changed, its time bound passed, or every publication
    /// attempt lost.
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

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Runs one metadata compaction unit of a writer session.
    ///
    /// The unit plans from the manifest of `position`, the session's
    /// position, so only a writer session compacts (see
    /// [`NamespaceCommitEngine::load_position`](crate::publish::NamespaceCommitEngine::load_position)).
    /// A position whose manifest is missing from the store and at or below
    /// the manifest boundary returns `head_moved`, and so does a unit that a
    /// manifest that won its put abandons, and the session plans again from
    /// a new position (see `ManifestUpdate::published_or_unchanged`). A
    /// cancelled unit, or one past its budget, returns its outcome. The unit
    /// reads its input runs without the cache. The walk that builds a base
    /// compaction's kept inode filter reads through the cache. The unit
    /// compacts the whole window that the namespace's run policy selects,
    /// whatever the number of rows its runs list, and the compaction checks
    /// `cancellation` between blocks. The unit then publishes as one item of
    /// `lane`, the session's publication lane, which it keeps only for its
    /// publication: it builds on the newer of the manifest it planned from
    /// and the newest manifest that the session published (see
    /// [`LaneGuard::basis_over`](crate::publish::LaneGuard::basis_over)), so
    /// it never puts at the number of the session's own fold, and records
    /// the manifest it published before it drops the lane. Finalization
    /// retries a lost put over the manifest that won while the unit's input
    /// segments are unchanged. The unit's time bound starts at the start of
    /// the call (see `METADATA_COMPACTION_BUDGET_MS`).
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.phase",
        err(level = "warn"),
        skip_all,
        fields(phase = Phase::MetadataCompactionStep.as_str(), key_class = "manifest")
    )]
    pub async fn compact_metadata_unit_from(
        &self,
        position: &WriterPosition,
        lane: &PublicationLane,
        cancellation: &MaintenanceCancellation,
    ) -> Result<CompactionUnitOutcome> {
        let compaction = self.namespace.start_clock();
        let basis = self.namespace.load_publication_basis(position).await?;
        let current = basis.manifest();
        let payload = current.payload();
        ensure_namespace_live(&payload.namespace_id, payload.status)?;
        // The unit reads its input runs without the cache, and the compaction inserts
        // its output into the cache. The walk that builds the kept inode filter
        // scans one directory at a time. A scan keeps the data blocks it fetches
        // in the cache, not in its own memo, so the walk reads through the
        // cache, and a block that contains the rows of many directories is
        // fetched once.
        let segments = self.namespace.current_segments(current).segments;
        let Some(spec) = plan_compaction(payload, &segments.scan_runs, self.namespace.run_policy)
        else {
            return Ok(CompactionUnitOutcome::NotNeeded);
        };
        // What the unit is about to read, recorded before it reads anything.
        // Finalization compares the manifest against this, so the run it
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
        let ran = match ran {
            // A collection can delete the input segments of a basis that a
            // newer manifest replaced while the unit reads them, and the
            // unit's read then returns `ManifestReplaced`. The unit is
            // abandoned, as it is when its put finds the basis replaced.
            Err(CoreError::ManifestReplaced { .. }) => {
                let outcome = CompactionUnitOutcome::Abandoned;
                log_unit_outcome(&self.namespace.namespace_id, &spec, &outcome);
                return Ok(outcome);
            }
            ran => ran?,
        };
        let outcome = match ran {
            Ok(compacted) => {
                drop(segments);
                let lane = lane.lock().await;
                let outcome = self
                    .namespace
                    .finalize_metadata_compaction(
                        lane.basis_over(basis),
                        &spec,
                        &input_keys,
                        compacted,
                        cancellation,
                        &CompactionPublication {
                            compaction,
                            publication: self.namespace.start_clock(),
                        },
                    )
                    .await?;
                if let CompactionUnitOutcome::Published { manifest, .. } = &outcome {
                    lane.record(manifest);
                }
                outcome
            }
            Err(stopped) => stopped,
        };
        log_unit_outcome(&self.namespace.namespace_id, &spec, &outcome);
        Ok(outcome)
    }
}

fn log_unit_outcome(
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
