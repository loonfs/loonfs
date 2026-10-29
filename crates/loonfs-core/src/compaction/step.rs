//! Runs one metadata compaction unit.
//!
//! A unit selects one family group and a window of at most eight complete
//! runs from the current manifest's descriptors (see [`super::plan`]), merges
//! the window with [`super::group_merge`], and publishes the replacement
//! manifest through [`super::publication`]. A base merge may apply retention,
//! and a delta merge drops only superseded layouts. [`MetadataCompactionPolicy`]
//! sets which runs a group merges, and when. A unit publishes under the
//! compactor epoch that [`NamespaceEngine::claim_compactor`] claims.

use super::group_merge::run_metadata_compaction;
use super::plan::plan_compaction;
use super::plan::MetadataCompactionSpec;
use super::publication::{input_segment_keys, CompactionPublication};
use crate::engine::{NamespaceEngine, Writable};
use crate::error::Result;
use crate::manifest::load::metadata_basis_from_manifest;
use crate::manifest::publish::ManifestChange;
use crate::manifest::published::{PublicationReport, PublishedManifest};
use crate::namespace::control::ensure_namespace_live;
use crate::namespace::PublicationBasis;
use crate::phase::Phase;
use crate::segment::runs::MetadataFamilyGroup;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{CompactorEpoch, NamespaceId};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Which runs of a family group a unit merges, and when.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MetadataCompactionPolicy {
    /// Merge a tier of similar-size runs once it has
    /// [`MetadataRunPolicy::min_compaction_runs`](crate::segment::MetadataRunPolicy::min_compaction_runs) runs, and a group's newest
    /// runs once it has [`MetadataRunPolicy::max_delta_runs`](crate::segment::MetadataRunPolicy::max_delta_runs) delta runs.
    #[default]
    SizeTiered,
    /// Merge a group's runs whatever their sizes, for an explicit compaction
    /// request. Input fan-in stays bounded.
    CompactImmediately,
}

/// Which windows one compaction call may merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionAdmission {
    /// Any window, whatever the number of rows its runs list.
    AnySize,
    /// A window whose runs list at most `max_decoded_input_rows_per_step`
    /// rows, so the call never runs a long compaction. A larger window
    /// returns [`CompactionUnitOutcome::TooLarge`].
    WithinRowBudget,
}

/// How one compaction unit ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionUnitOutcome {
    /// No family group has eligible input.
    NotNeeded,
    /// The selected window lists more rows than the call admits, so the unit
    /// merged nothing.
    TooLarge {
        /// The group the unit selected.
        group: MetadataFamilyGroup,
    },
    /// The unit merged one family group's window and published the next
    /// manifest. The new run is the group's base when the window started at
    /// the group's oldest run, and a bigger delta run otherwise.
    Published {
        /// The group this unit merged. Its families are
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
    },
    /// The caller cancelled the unit before it published.
    Cancelled,
    /// The unit's inputs changed, its time bound passed, or every publication
    /// attempt lost.
    Abandoned,
    /// Another process claimed the namespace compactor role.
    Fenced,
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Runs one metadata compaction unit under the claimed compactor epoch.
    ///
    /// The unit plans from `basis`, a manifest that the caller already has,
    /// or from the current manifest that it loads when `basis` is `None`, and
    /// reads its input runs without the cache. `admission` determines whether
    /// the unit may merge a window over the run policy's row budget, and the
    /// merge checks `cancellation` between blocks. The unit publishes through
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
        compaction_policy: MetadataCompactionPolicy,
        admission: CompactionAdmission,
        compactor_epoch: CompactorEpoch,
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
        if payload.compactor_epoch != compactor_epoch {
            return Ok(CompactionUnitOutcome::Fenced);
        }
        ensure_namespace_live(&payload.namespace_id, payload.status)?;
        // The unit reads its input runs without the cache, and the merge inserts
        // its output into the cache.
        let segments = metadata_basis_from_manifest(&self.namespace.store, None, current).segments;
        let Some(spec) = plan_compaction(
            payload,
            &segments.scan_runs,
            self.namespace.run_policy,
            compaction_policy,
        ) else {
            return Ok(CompactionUnitOutcome::NotNeeded);
        };
        if admission == CompactionAdmission::WithinRowBudget
            && spec.input_rows()
                > self
                    .namespace
                    .run_policy
                    .max_decoded_input_rows_per_step
                    .get() as u64
        {
            return Ok(CompactionUnitOutcome::TooLarge {
                group: spec.group(),
            });
        }
        // What the unit is about to read, recorded before it reads anything.
        // Finalization compares the manifest against this, so the run it
        // publishes stands in for exactly the segments it merged.
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
            Ok(merged) => {
                drop(segments);
                self.namespace
                    .finalize_metadata_compaction(
                        Some(basis),
                        &spec,
                        &input_keys,
                        merged,
                        cancellation,
                        &CompactionPublication {
                            compactor_epoch,
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
        CompactionUnitOutcome::Fenced => tracing::warn!(
            namespace_id = namespace_id.as_str(),
            job_id = spec.job_id().as_str(),
            families = ?spec.families(),
            "metadata compaction unit fenced"
        ),
        CompactionUnitOutcome::NotNeeded | CompactionUnitOutcome::TooLarge { .. } => {}
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

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Claims the namespace compactor epoch for a maintenance runtime. The
    /// report includes the manifest that records the claim.
    pub async fn claim_compactor(&self) -> Result<PublicationReport<CompactorEpoch>> {
        let clock = self.namespace.start_clock();
        let (epoch, claimed, _) = self
            .namespace
            .update_live_manifest(
                None,
                &clock,
                |mut payload: NamespaceManifestPayload| async move {
                    payload.compactor_epoch =
                        CompactorEpoch(payload.compactor_epoch.0.checked_add(1).expect(
                            "compactor epoch should be bounded by the manifest number write stop",
                        ));
                    let epoch = payload.compactor_epoch;
                    Ok(ManifestChange::Next(Box::new(payload), epoch))
                },
            )
            .await?;
        Ok(PublicationReport {
            response: epoch,
            published: Some(PublishedManifest::new(
                &claimed.envelope,
                Some(clock.origin()),
            )),
        })
    }
}
