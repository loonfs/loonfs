//! One visit to a held namespace: the claim on the namespace, and the
//! metadata, retention, garbage collection, and index calls the visit makes.

use crate::sweep::contain;
use crate::sweep::lock;
use crate::sweep::retries::CallFailure;
use crate::sweep::retries::SweepCall;
use crate::sweep::GarbageCollection;
use crate::sweep::GrepIndexing;
use crate::sweep::Sweep;
use crate::sweep::SweepInner;
use crate::sweep::SweepState;
use crate::sweep::MAX_GREP_BUILD_STEPS_PER_VISIT;
use futures::FutureExt as _;
use loonfs::{
    AdvanceRetentionFloorOptions, ChangeSeq, GcOptions, MetadataMaintenanceOutcome, NamespaceId,
    RetentionTarget,
};
use loonfs_grep::{GrepBuildOutcome, GrepError, GrepReorganizeOutcome};
use loonfs_http::HeldNamespace;

use std::collections::hash_map::Entry;
use std::sync::Mutex;
use tokio::sync::SemaphorePermit;

impl Sweep {
    pub(super) async fn visit(&self, namespace_id: NamespaceId, garbage: GarbageCollection) {
        let inner = &self.inner;
        let Some((visit, _permit)) = self
            .claim(&namespace_id, Overlap::LeaveRetentionAdvance)
            .await
        else {
            return;
        };
        let entry = inner.namespaces.entry(&namespace_id);
        let entry = entry.as_deref();
        let seq = entry.and_then(|entry| lock(entry).handle.last_published_seq());
        self.maintain_metadata(&namespace_id, entry, seq).await;
        if let Some(grep) = &inner.grep {
            self.maintain_index(grep, &namespace_id, entry, seq).await;
        }
        // The advance reads only folded commits, so it runs after the
        // metadata call, which can fold an idle tail.
        self.advance_retention(&namespace_id, entry).await;
        if garbage == GarbageCollection::Collect {
            self.gc(&namespace_id, entry, seq).await;
        }
        self.finish_visit(visit).await;
    }

    /// Makes one metadata call. Unfinished work, or a failure, leaves the
    /// call due again after `maintenance_interval_ms`.
    pub(super) async fn maintain_metadata(
        &self,
        namespace_id: &NamespaceId,
        entry: Option<&Mutex<HeldNamespace<SweepState>>>,
        seq: Option<ChangeSeq>,
    ) {
        let inner = &self.inner;
        let result = contain(
            inner
                .maintenance
                .maintain_metadata_while_due(namespace_id, &inner.stop)
                .map(|result| result.map_err(|error| CallFailure::error(error.code(), error))),
        )
        .await;
        let outcome = result
            .as_ref()
            .map_or(MetadataMaintenanceOutcome::Due, |response| response.outcome);
        if let Some(entry) = entry {
            lock(entry).handle.record_metadata_maintenance(seq, outcome);
        }
        let work_wait_ms = if outcome == MetadataMaintenanceOutcome::CaughtUp {
            0
        } else {
            inner.config.maintenance_interval_ms
        };
        self.record_call(
            namespace_id,
            entry,
            SweepCall::Metadata,
            result,
            work_wait_ms,
        );
    }

    pub(super) async fn maintain_index(
        &self,
        grep: &GrepIndexing,
        namespace_id: &NamespaceId,
        entry: Option<&Mutex<HeldNamespace<SweepState>>>,
        seq: Option<ChangeSeq>,
    ) {
        if let Some(entry) = entry {
            lock(entry).index_dirty = false;
        }
        let mut build = IndexBuild {
            entry,
            caught_up: false,
        };
        let result = contain(
            self.build_index(grep, namespace_id)
                .map(|result| result.map_err(|error| CallFailure::error(error.code(), error))),
        )
        .await;
        build.caught_up = self
            .record_call(namespace_id, entry, SweepCall::GrepIndex, result, 0)
            .unwrap_or(false);
        if build.caught_up {
            if let Some(entry) = entry {
                lock(entry).host_state.indexed_seq = seq;
            }
        }
    }

    /// Claims the namespace for one visit and waits for a visit slot.
    /// Returns `None` when another visit has the namespace, which `overlap`
    /// may leave the retention advance to, or when the sweep stops first.
    pub(super) async fn claim<'a>(
        &'a self,
        namespace_id: &NamespaceId,
        overlap: Overlap,
    ) -> Option<(Visiting<'a>, SemaphorePermit<'a>)> {
        let inner = &self.inner;
        match lock(&inner.visiting).entry(namespace_id.clone()) {
            Entry::Occupied(mut visit) => {
                if overlap == Overlap::LeaveRetentionAdvance {
                    visit.insert(VisitClaim::RetentionAdvanceLeft);
                }
                return None;
            }
            Entry::Vacant(free) => {
                free.insert(VisitClaim::Claimed);
            }
        }
        let visit = Visiting {
            inner,
            namespace_id: namespace_id.clone(),
            released: false,
        };
        let permit = tokio::select! {
            biased;
            () = inner.stop.cancelled() => return None,
            permit = inner.slots.acquire() => permit.ok()?,
        };
        Some((visit, permit))
    }

    /// Makes the retention advance that full passes left to this visit, then
    /// releases the namespace. The check and the release share the visiting
    /// lock with a pass's claim, so a pass either leaves its advance before
    /// the release or claims the namespace itself after it.
    pub(super) async fn finish_visit(&self, mut visit: Visiting<'_>) {
        // Each turn makes the advance that one pass left while this visit
        // had the namespace, so the loop turns once for each full pass that
        // overlaps the visit.
        while visit.release_unless_retention_due() {
            let entry = self.inner.namespaces.entry(&visit.namespace_id);
            self.advance_retention(&visit.namespace_id, entry.as_deref())
                .await;
        }
    }

    pub(super) async fn gc(
        &self,
        namespace_id: &NamespaceId,
        entry: Option<&Mutex<HeldNamespace<SweepState>>>,
        seq: Option<ChangeSeq>,
    ) {
        let inner = &self.inner;
        if inner.stop.is_cancelled() {
            return;
        }
        let options = GcOptions::default();
        let result = contain(
            loonfs_http::gc_namespace(
                &inner.maintenance,
                inner.grep.as_ref().map(|grep| &grep.worker),
                namespace_id,
                &options,
            )
            .map(|result| result.map_err(|error| CallFailure::error(error.code(), error))),
        )
        .await;
        let collected = self.record_call(namespace_id, entry, SweepCall::Gc, result, 0);
        if let (Some(_), Some(entry)) = (collected, entry) {
            let mut held = lock(entry);
            held.host_state.collected_seq = seq;
            held.host_state.collected_at_ms = inner.timer.monotonic_now_ms();
        }
    }

    /// Acknowledges the namespace's current retention floor, and then
    /// advances the floor to the last commit at or before the current time
    /// minus the namespace's retention window, or minus `retention_window_ms`
    /// for a namespace that has no window of its own, with the same advance
    /// as the `retention` job. A namespace with neither window keeps its
    /// floor. A deleted namespace returns `namespace_deleted`, which isn't a
    /// failure. A failed acknowledgment doesn't stop the advance.
    ///
    /// The acknowledgment comes first, so the floor it acknowledges is the
    /// one that the previous full pass set, and a namespace whose writer
    /// committed since then has acknowledged that floor already. Only a
    /// namespace that nobody wrote to since the previous pass gets a fence.
    /// For when the deletions below the new floor expire, and when their
    /// rows are removed, see [Advancing the floor on a
    /// schedule](https://github.com/loonfs/loonfs/blob/main/docs/design/retention-window.md#advancing-the-floor-on-a-schedule).
    async fn advance_retention(
        &self,
        namespace_id: &NamespaceId,
        entry: Option<&Mutex<HeldNamespace<SweepState>>>,
    ) {
        let inner = &self.inner;
        if inner.stop.is_cancelled() {
            return;
        }
        // Boxed so the spawned sweep's `Send` check does not recurse through
        // every engine future the acknowledgment and the advance await.
        let acknowledged = contain(
            inner
                .maintenance
                .acknowledge_retention_floor(namespace_id)
                .boxed()
                .map(|result| result.map_err(|error| CallFailure::error(error.code(), error))),
        )
        .await;
        self.record_call(
            namespace_id,
            entry,
            SweepCall::RetentionAcknowledgment,
            acknowledged,
            0,
        );
        if inner.stop.is_cancelled() {
            return;
        }
        let options = AdvanceRetentionFloorOptions {
            target: RetentionTarget::Window {
                default_window_ms: inner.config.retention_window_ms,
            },
        };
        let result = contain(
            inner
                .maintenance
                .advance_retention_floor_with_options(namespace_id, &options)
                .boxed()
                .map(|result| result.map_err(|error| CallFailure::error(error.code(), error))),
        )
        .await;
        self.record_call(namespace_id, entry, SweepCall::Retention, result, 0);
    }

    /// Runs build steps while each one publishes, at most
    /// [`MAX_GREP_BUILD_STEPS_PER_VISIT`], then one reorganize step once the
    /// index is up to date. Returns whether the index has nothing left to
    /// build for now.
    async fn build_index(
        &self,
        grep: &GrepIndexing,
        namespace_id: &NamespaceId,
    ) -> Result<bool, GrepError> {
        for _ in 0..MAX_GREP_BUILD_STEPS_PER_VISIT {
            if self.inner.stop.is_cancelled() {
                return Ok(false);
            }
            match grep.worker.build_step(namespace_id, grep.policy).await? {
                GrepBuildOutcome::Published { .. } | GrepBuildOutcome::BackfillRestarted { .. } => {
                    continue
                }
                GrepBuildOutcome::UpToDate { .. } => {
                    return Ok(matches!(
                        grep.worker
                            .reorganize_step(namespace_id, grep.policy)
                            .await?,
                        GrepReorganizeOutcome::NotNeeded { .. } | GrepReorganizeOutcome::NotEnabled
                    ));
                }
                GrepBuildOutcome::NotEnabled | GrepBuildOutcome::Superseded => return Ok(true),
            }
        }
        Ok(false)
    }
}

struct IndexBuild<'visit> {
    entry: Option<&'visit Mutex<HeldNamespace<SweepState>>>,
    caught_up: bool,
}

impl Drop for IndexBuild<'_> {
    fn drop(&mut self) {
        if !self.caught_up {
            if let Some(entry) = self.entry {
                lock(entry).index_dirty = true;
            }
        }
    }
}

/// How a visit claims a namespace in the sweep's visiting map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VisitClaim {
    /// A visit has the namespace.
    Claimed,
    /// A visit has the namespace, and a full pass that reached it left its
    /// retention advance to the visit.
    RetentionAdvanceLeft,
}

/// What a claim does when another visit has the namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Overlap {
    /// Skip the namespace.
    Skip,
    /// Skip the namespace, and let the visit in progress make the retention
    /// advance.
    LeaveRetentionAdvance,
}

/// One visit's claim on a namespace. Dropping it releases the namespace, and
/// [`Sweep::finish_visit`] releases it after the advances a pass left.
pub(super) struct Visiting<'sweep> {
    inner: &'sweep SweepInner,
    namespace_id: NamespaceId,
    released: bool,
}

impl Visiting<'_> {
    /// Releases the namespace, unless a full pass left its retention advance
    /// to this visit. Then clears that mark, keeps the namespace, and returns
    /// `true`.
    fn release_unless_retention_due(&mut self) -> bool {
        let mut visiting = lock(&self.inner.visiting);
        let claim = visiting
            .get_mut(&self.namespace_id)
            .expect("a claimed namespace should stay in the visiting map until it's released");
        if std::mem::replace(claim, VisitClaim::Claimed) == VisitClaim::RetentionAdvanceLeft {
            return true;
        }
        visiting.remove(&self.namespace_id);
        self.released = true;
        false
    }
}

impl Drop for Visiting<'_> {
    fn drop(&mut self) {
        if !self.released {
            lock(&self.inner.visiting).remove(&self.namespace_id);
        }
    }
}
