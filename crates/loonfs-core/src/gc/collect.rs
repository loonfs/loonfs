//! One collection of a namespace's own objects, with a fixed clock and no
//! durable progress.

use super::charged_set::{ChargedQueue, CollectionMemory};
use super::deletes::{apply_decisions, Decision};
use super::families::CandidateFamily;
use super::live_set::{LiveSet, RetirementStatus};
use super::sweep::Sweep;
use crate::error::{CoreError, Result};
use crate::namespace::store::NamespaceStore;
use crate::time::OperationClock;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use loonfs_objectstore::layout::manifest_no_of;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{GcResponse, ManifestNo};
use std::sync::atomic::AtomicU64;

impl<S: ObjectStore> NamespaceStore<S> {
    /// Collects the namespace's manifests, WAL objects, metadata segments,
    /// extension segments, and snapshots, and retires the namespace when its
    /// tombstone is eligible.
    ///
    /// The collection lists nothing under its pool's prefix. The pool's own
    /// collection collects the content objects, upload records, and temporary
    /// objects there (see [`crate::PoolStore::gc`]).
    pub async fn gc(&self, clock: &OperationClock) -> Result<GcResponse> {
        let mut report = GcResponse::empty(self.namespace_id.clone());
        let mut live = LiveSet::load(self, clock).await?;
        report.retires_at_ms = live
            .current_tombstone
            .as_ref()
            .map(|tombstone| live.deadline(tombstone));
        report.next_reclamation_at_ms = match live.retirement_status() {
            RetirementStatus::Retained { retires_at_ms } => retires_at_ms,
            _ => None,
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

    /// Keeps the collection's decision about every listed manifest, raises
    /// the manifest boundary to the highest manifest number that the decisions
    /// delete, and then deletes exactly those manifests.
    async fn collect_manifests(
        &self,
        mut decisions: impl Stream<Item = Result<Decision<GcResponse>>> + Unpin,
        memory: &CollectionMemory,
        report: &mut GcResponse,
    ) -> Result<()> {
        let mut kept = ChargedQueue::new(memory);
        while let Some(decision) = decisions.try_next().await? {
            kept.push_back(decision)?;
        }
        if let Some(manifest_no) = raise_target(kept.iter()) {
            self.raise_boundary(manifest_no).await?;
        }
        let kept = std::iter::from_fn(|| kept.pop_front()).map(Ok);
        apply_decisions(&self.store, stream::iter(kept), report).await
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
