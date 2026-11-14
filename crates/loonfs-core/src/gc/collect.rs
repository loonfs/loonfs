//! One collection of a namespace's own objects, with a fixed clock and no
//! durable progress.

use super::charged_set::{ChargedQueue, CollectionMemory};
use super::deletes::{apply_decisions, CollectionReport, Decision};
use super::families::CandidateFamily;
use super::live_set::{LiveSet, RetirementStatus};
use super::sweep::{retired_candidate, Sweep};
use crate::error::{CoreError, Result};
use crate::namespace::store::NamespaceStore;
use crate::time::OperationClock;
use futures::{future, stream, Stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::{boundary, namespace_prefix};
use loonfs_objectstore::layout::manifest_no_of;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{GcResponse, ManifestNo};
use std::sync::atomic::AtomicU64;

impl<S: ObjectStore> NamespaceStore<S> {
    /// Collects the namespace's manifests, WAL objects, metadata segments,
    /// extension segments, and snapshots, and retires the namespace when its
    /// tombstone is eligible.
    ///
    /// A retired namespace has no roots, so the collection reads no manifest
    /// and makes one listing of the namespace's prefix. It deletes every
    /// manifest, WAL object, segment, and snapshot record there once the
    /// object is old enough, and keeps the boundary object.
    ///
    /// The collection lists nothing under its pool's prefix. The pool's own
    /// collection collects the content objects, upload records, and temporary
    /// objects there (see [`crate::PoolStore::gc`]).
    pub async fn gc(&self, clock: &OperationClock) -> Result<GcResponse> {
        let mut report = GcResponse::empty(self.namespace_id.clone());
        let Some(mut live) = LiveSet::load(self, clock).await? else {
            let prefix = namespace_prefix(&self.namespace_id);
            let boundary_key = boundary(&self.namespace_id);
            let mut listed_bytes = 0;
            let decisions = self
                .store
                .list_entries_from_stream(&prefix, None)
                .map_err(|error| CoreError::store(&prefix, &error))
                // The boundary object is not a candidate. The listings of a
                // namespace with roots don't return it either.
                .try_filter(|entry| future::ready(entry.key != boundary_key))
                .map_ok(|entry| retired_candidate(entry, clock.request_at_ms(), &mut listed_bytes))
                .boxed();
            apply_decisions(&self.store, decisions, &mut report).await?;
            report.listed_bytes += listed_bytes;
            return Ok(report);
        };
        report.retires_at_ms = live
            .current_tombstone
            .as_ref()
            .map(|tombstone| live.deadline(tombstone));
        report.next_reclamation_at_ms = match live.retirement_status() {
            RetirementStatus::Retained { retires_at_ms } => retires_at_ms,
            _ => live.tombstone_at_ms,
        };
        let sweep = Sweep {
            clock,
            live: &live,
            listed_bytes: AtomicU64::new(0),
        };
        for family in CandidateFamily::ALL {
            let prefix = family.prefix(&self.namespace_id);
            // Unboxed, rustc cannot prove the collection future `Send` through this stream.
            let decisions = self
                .store
                .list_entries_from_stream(&prefix, None)
                .map_err(|error| CoreError::store(&prefix, &error))
                .map_ok(|entry| sweep.candidate(family, entry))
                .boxed();
            if family == CandidateFamily::Manifests {
                self.collect_manifests(decisions, &live.memory, &mut report)
                    .await?;
            } else {
                apply_decisions(&self.store, decisions, &mut report).await?;
            }
        }
        report.listed_bytes += sweep.listed_bytes.into_inner() + live.snapshot_listed_bytes;
        // The snapshots go after the objects that they protect, from the listing
        // that loaded their manifests.
        let snapshots = std::iter::from_fn(|| live.snapshot_decisions.pop_front()).map(Ok);
        apply_decisions(&self.store, stream::iter(snapshots), &mut report).await?;
        self.retire_namespace(&live, &mut report).await?;
        Ok(report)
    }

    /// Records the decisions that keep a manifest, and deletes the manifests
    /// that the other decisions choose in batches that fit the collection's
    /// memory. The listing is in number order, so each batch raises the
    /// manifest boundary to the batch's highest manifest number, at or above
    /// the raise before, and then deletes exactly the batch's manifests.
    async fn collect_manifests(
        &self,
        mut decisions: impl Stream<Item = Result<Decision<GcResponse>>> + Unpin,
        memory: &CollectionMemory,
        report: &mut GcResponse,
    ) -> Result<()> {
        let mut batch = ChargedQueue::new(memory);
        while let Some(decision) = decisions.try_next().await? {
            match decision {
                Decision::Retain {
                    reason,
                    reclaimable_at_ms,
                } => report.record_retained(reason, reclaimable_at_ms),
                Decision::Gone => {}
                Decision::Delete { .. } => {
                    if let Err(decision) = batch.try_push_back(decision) {
                        self.delete_manifests(&mut batch, report).await?;
                        batch.push_back(decision)?;
                    }
                }
            }
        }
        self.delete_manifests(&mut batch, report).await
    }

    /// Raises the manifest boundary to the highest manifest number in
    /// `batch`, and then deletes the batch's manifests.
    async fn delete_manifests(
        &self,
        batch: &mut ChargedQueue<Decision<GcResponse>>,
        report: &mut GcResponse,
    ) -> Result<()> {
        if let Some(manifest_no) = raise_target(batch.iter()) {
            self.raise_boundary(manifest_no).await?;
        }
        let deletes = std::iter::from_fn(|| batch.pop_front()).map(Ok);
        apply_decisions(&self.store, stream::iter(deletes), report).await
    }
}

/// Returns the highest manifest number that `decisions` delete, or `None`
/// when they delete no manifest.
fn raise_target<'a>(
    decisions: impl Iterator<Item = &'a Decision<GcResponse>>,
) -> Option<ManifestNo> {
    decisions
        .filter_map(|decision| match decision {
            Decision::Delete { keys, .. } => {
                keys.iter().filter_map(|key| manifest_no_of(key)).max()
            }
            Decision::Retain { .. } | Decision::Gone => None,
        })
        .max()
}
