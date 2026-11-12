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
use crate::engine::{NamespaceEngine, Writable};
use crate::error::Result;
use crate::manifest::load::metadata_basis_from_manifest;
use crate::manifest::published::PublishedManifest;
use crate::namespace::control::{ensure_namespace_live, LoadedManifest};
use crate::namespace::PublicationBasis;
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
        manifest: PublishedManifest,
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

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
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

    /// Runs one metadata compaction unit.
    ///
    /// The unit plans from `basis`, a manifest that the caller already has,
    /// or from the current manifest that it loads when `basis` is `None`, and
    /// reads its input runs without the cache. The unit compacts the whole
    /// window that the namespace's run policy selects, whatever the number of rows
    /// its runs list, and the compaction checks `cancellation` between
    /// blocks. The unit publishes through
    /// `finalize_metadata_compaction`, starting from the
    /// manifest it planned from, and finalization retries a lost put over the
    /// manifest that won while the unit's input segments are unchanged. The
    /// unit's time bound starts at the start of the call, before any load (see
    /// `METADATA_COMPACTION_BUDGET_MS`). A call without a basis reloads durable
    /// state, so callers may repeat it safely across process restarts.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.phase",
        err(level = "warn"),
        skip_all,
        fields(phase = Phase::MetadataCompactionStep.as_str(), key_class = "manifest")
    )]
    pub async fn compact_metadata_unit(
        &self,
        basis: Option<PublicationBasis>,
        cancellation: &MetadataCompactionCancellation,
    ) -> Result<CompactionUnitOutcome> {
        let compaction = self.namespace.start_clock();
        let basis = match basis {
            Some(basis) => basis,
            None => {
                let checked = compaction.observe();
                PublicationBasis {
                    manifest: self.namespace.load_current_manifest().await?,
                    checked,
                }
            }
        };
        let current = &basis.manifest;
        let payload = current.payload();
        ensure_namespace_live(&payload.namespace_id, payload.status)?;
        // The unit reads its input runs without the cache, and the compaction inserts
        // its output into the cache.
        let segments = metadata_basis_from_manifest(&self.namespace.store, None, current).segments;
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
        let outcome = match run_metadata_compaction(
            &segments,
            &self.namespace.segment_cache,
            &self.namespace.namespace_id,
            &spec,
            self.namespace.run_policy,
            cancellation,
        )
        .await?
        {
            Ok(compacted) => {
                drop(segments);
                self.namespace
                    .finalize_metadata_compaction(
                        Some(basis),
                        &spec,
                        &input_keys,
                        compacted,
                        cancellation,
                        &CompactionPublication {
                            compaction,
                            publication: self.namespace.start_clock(),
                        },
                    )
                    .await?
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
            manifest_no = manifest.manifest.manifest_no.0,
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
        // with a `cancel` arriving between the read and the await: the notify
        // then wakes this waiter rather than passing it by.
        woken.as_mut().enable();
        if self.is_cancelled() {
            return;
        }
        woken.await;
    }
}
