//! The scheduler's claim on a namespace, and the metadata, retention,
//! garbage collection, and index calls that a namespace's steps make.

use crate::scheduler::contain;
use crate::scheduler::lock;
use crate::scheduler::retries::CallFailure;
use crate::scheduler::retries::MaintenanceCall;
use crate::scheduler::GarbageCollection;
use crate::scheduler::GrepIndexing;
use crate::scheduler::Scheduler;
use crate::scheduler::SchedulerInner;
use crate::scheduler::SchedulerState;
use crate::scheduler::MAX_GREP_STEPS_PER_INDEX_STEP;
use futures::FutureExt as _;
use loonfs::SharedObjectStore;
use loonfs::{
    AdvanceRetentionFloorOptions, ChangeSeq, ErrorCode, GcOptions, MetadataMaintenanceOutcome,
    NamespaceId, RetentionTarget,
};
use loonfs_grep::{
    GrepBuildOutcome, GrepError, GrepProgress, GrepPublishOutcome, GrepReorganizeOutcome, GrepSteps,
};
use loonfs_http::HeldNamespace;

use std::collections::hash_map::Entry;
use std::sync::Mutex;
use tokio::sync::SemaphorePermit;

impl Scheduler {
    /// Runs every step of one namespace for a sweep.
    pub(super) async fn sweep_namespace(
        &self,
        namespace_id: NamespaceId,
        garbage: GarbageCollection,
    ) {
        let inner = &self.inner;
        let Some((claim, _permit)) = self
            .claim(&namespace_id, Overlap::LeaveRetentionAdvance)
            .await
        else {
            return;
        };
        let entry = inner.namespaces.entry(&namespace_id);
        let entry = entry.as_deref();
        let seq = entry.and_then(|entry| lock(entry).handle.last_published_seq());
        // The call records its own failure, and the sweep makes the
        // namespace's other calls whatever it returns.
        let _ = self.maintain_metadata(&namespace_id, entry).await;
        if let Some(grep) = &inner.grep {
            self.maintain_index(grep, &namespace_id, entry, seq).await;
        }
        // The advance reads only folded commits, so it runs after the
        // metadata call, which can fold an idle tail.
        self.advance_retention(&namespace_id, entry).await;
        if garbage == GarbageCollection::Collect {
            self.gc(&namespace_id, entry, seq).await;
        }
        self.finish_claim(claim).await;
    }

    /// Makes one metadata call, and returns its outcome, or the code that it
    /// failed with. Unfinished work, or a failure, leaves the call due again
    /// after `metadata_retry_interval_ms`.
    pub(super) async fn maintain_metadata(
        &self,
        namespace_id: &NamespaceId,
        entry: Option<&Mutex<HeldNamespace<SchedulerState>>>,
    ) -> Result<MetadataMaintenanceOutcome, ErrorCode> {
        let inner = &self.inner;
        // The seq before the step, so a commit during the step counts as one
        // the step didn't see, and the next maintenance interval runs the
        // step again.
        let seq = entry.map(|entry| lock(entry).handle.last_published_seq());
        let result = contain(
            inner
                .maintenance
                .maintain_metadata_while_due(namespace_id, &inner.stop)
                .map(|result| result.map_err(|error| CallFailure::error(error.code(), error))),
        )
        .await;
        let outcome = match &result {
            Ok(report) => Ok(report.response.outcome),
            Err(failure) => Err(failure.code()),
        };
        let next_due_at_ms = result
            .as_ref()
            .ok()
            .and_then(|report| report.next_due_at_ms);
        // A caught-up session whose position shows nothing due costs nothing
        // to ask again. A step without a current position read the store, so
        // it waits for the retry interval. A tail that waits for its idle
        // fold age is asked again at that age if it comes first.
        let positioned = entry.is_some_and(|entry| {
            let mut held = lock(entry);
            held.host_state.last_step_seq = seq.flatten();
            if outcome.is_ok() {
                held.host_state.idle_fold_due_at_ms = next_due_at_ms;
            }
            held.handle.has_current_writer_position()
        });
        let interval_ms = inner.config.metadata_retry_interval_ms;
        let work_wait_ms = match outcome {
            Ok(MetadataMaintenanceOutcome::CaughtUp) if positioned => 0,
            _ => next_due_at_ms.map_or(interval_ms, |due_at_ms| {
                due_at_ms
                    .saturating_sub(inner.timer.monotonic_now_ms())
                    .min(interval_ms)
            }),
        };
        self.record_call(
            namespace_id,
            entry,
            MaintenanceCall::Metadata,
            result,
            work_wait_ms,
        );
        outcome
    }

    /// Makes one index call, and returns whether it found the namespace
    /// gone.
    pub(super) async fn maintain_index(
        &self,
        grep: &GrepIndexing,
        namespace_id: &NamespaceId,
        entry: Option<&Mutex<HeldNamespace<SchedulerState>>>,
        seq: Option<ChangeSeq>,
    ) -> bool {
        if let Some(entry) = entry {
            lock(entry).index_dirty = false;
        }
        let mut build = IndexBuild {
            entry,
            caught_up: false,
        };
        // Boxed so the spawned scheduler's `Send` check does not recurse
        // through every engine future the index steps await.
        let result = contain(
            self.build_index(grep, namespace_id)
                .boxed()
                .map(|result| result.map_err(|error| CallFailure::error(error.code(), error))),
        )
        .await;
        let gone = result
            .as_ref()
            .err()
            .is_some_and(CallFailure::namespace_gone);
        build.caught_up = self
            .record_call(namespace_id, entry, MaintenanceCall::GrepIndex, result, 0)
            .unwrap_or(false);
        if build.caught_up {
            if let Some(entry) = entry {
                lock(entry).host_state.indexed_seq = seq;
            }
        }
        gone
    }

    /// Claims the namespace for one run of its steps and waits for a slot.
    /// Returns `None` when other steps have the namespace, which `overlap`
    /// may leave the retention advance to, or when the scheduler stops
    /// first.
    pub(super) async fn claim<'a>(
        &'a self,
        namespace_id: &NamespaceId,
        overlap: Overlap,
    ) -> Option<(ClaimedNamespace<'a>, SemaphorePermit<'a>)> {
        let inner = &self.inner;
        match lock(&inner.claims).entry(namespace_id.clone()) {
            Entry::Occupied(mut claim) => {
                if overlap == Overlap::LeaveRetentionAdvance {
                    claim.insert(Claim::RetentionAdvanceLeft);
                }
                return None;
            }
            Entry::Vacant(free) => {
                free.insert(Claim::Claimed);
            }
        }
        let claimed = ClaimedNamespace {
            inner,
            namespace_id: namespace_id.clone(),
            released: false,
        };
        let permit = tokio::select! {
            biased;
            () = inner.stop.cancelled() => return None,
            permit = inner.slots.acquire() => permit.ok()?,
        };
        Some((claimed, permit))
    }

    /// Makes the retention advance that sweeps left to the claim's steps,
    /// then releases the namespace. The check and the release share the
    /// claims lock with a sweep's claim, so a sweep either leaves its advance
    /// before the release or claims the namespace itself after it.
    pub(super) async fn finish_claim(&self, mut claimed: ClaimedNamespace<'_>) {
        // Each turn makes the advance that one sweep left while these steps
        // had the namespace, so the loop turns once for each sweep that
        // overlaps the steps.
        while claimed.release_unless_retention_due() {
            let entry = self.inner.namespaces.entry(&claimed.namespace_id);
            self.advance_retention(&claimed.namespace_id, entry.as_deref())
                .await;
        }
    }

    /// Makes one collection call, and returns whether it found the
    /// namespace gone.
    pub(super) async fn gc(
        &self,
        namespace_id: &NamespaceId,
        entry: Option<&Mutex<HeldNamespace<SchedulerState>>>,
        seq: Option<ChangeSeq>,
    ) -> bool {
        let inner = &self.inner;
        if inner.stop.is_cancelled() {
            return false;
        }
        let options = GcOptions::default();
        let result = contain(
            inner
                .maintenance
                .gc_with_options(namespace_id, &options)
                .map(|result| result.map_err(|error| CallFailure::error(error.code(), error))),
        )
        .await;
        let gone = result
            .as_ref()
            .err()
            .is_some_and(CallFailure::namespace_gone);
        let collected = self.record_call(namespace_id, entry, MaintenanceCall::Gc, result, 0);
        if let (Some(_), Some(entry)) = (collected, entry) {
            let mut held = lock(entry);
            held.host_state.collected_seq = seq;
            held.host_state.collected_at_ms = inner.timer.monotonic_now_ms();
        }
        gone
    }

    /// Advances the namespace's retention floor to the last commit at or
    /// before the current time minus the namespace's retention window, or
    /// minus `retention_window_ms` for a namespace that has no window of its
    /// own, with the same advance as the `retention` job. A namespace with
    /// neither window keeps its floor. A deleted namespace returns
    /// `namespace_deleted`, which isn't a failure.
    ///
    /// A floor that moves is advanced through the namespace's writer session
    /// in this server, which the advance opens when the server has none. For
    /// when the deletions below the new floor expire, and when their rows are
    /// removed, see [Advancing the floor on a
    /// schedule](https://github.com/loonfs/loonfs/blob/main/docs/design/retention-window.md#advancing-the-floor-on-a-schedule).
    async fn advance_retention(
        &self,
        namespace_id: &NamespaceId,
        entry: Option<&Mutex<HeldNamespace<SchedulerState>>>,
    ) {
        let inner = &self.inner;
        if inner.stop.is_cancelled() {
            return;
        }
        let options = AdvanceRetentionFloorOptions {
            target: RetentionTarget::Window {
                default_window_ms: inner.config.retention_window_ms,
            },
        };
        // Boxed so the spawned scheduler's `Send` check does not recurse
        // through every engine future the advance awaits.
        let result = contain(
            inner
                .maintenance
                .advance_retention_floor_with_options(namespace_id, &options)
                .boxed()
                .map(|result| result.map_err(|error| CallFailure::error(error.code(), error))),
        )
        .await;
        self.record_call(namespace_id, entry, MaintenanceCall::Retention, result, 0);
    }

    /// Runs grep steps while each one does some work, at most
    /// [`MAX_GREP_STEPS_PER_INDEX_STEP`], and publishes what the grep steps
    /// left pending. Returns whether the index has nothing left to build or
    /// reorganize for now.
    ///
    /// Each grep step is a reorganize step when a reorganization is due, and
    /// a build step otherwise (see [`GrepSteps::step`]).
    ///
    /// The grep steps' changes are published at most once per maintenance
    /// interval, and once more when the index step ends, so an index step's
    /// sixteen grep steps are published in one or a few manifests instead of
    /// sixteen. A stopping scheduler publishes what's pending too.
    async fn build_index(
        &self,
        grep: &GrepIndexing,
        namespace_id: &NamespaceId,
    ) -> Result<bool, GrepError> {
        let mut steps = grep
            .worker
            .steps(namespace_id, self.inner.config.maintenance_interval_ms);
        // A grep step changes the pending entry only once it succeeds, so the
        // index step publishes the grep steps before a failed one, and then
        // reports the failure.
        let caught_up = self.run_index_steps(grep, &mut steps).await;
        let published = steps.publish().await;
        Ok(caught_up? && published? != GrepPublishOutcome::Superseded)
    }

    async fn run_index_steps(
        &self,
        grep: &GrepIndexing,
        steps: &mut GrepSteps<'_, SharedObjectStore>,
    ) -> Result<bool, GrepError> {
        for _ in 0..MAX_GREP_STEPS_PER_INDEX_STEP {
            if self.inner.stop.is_cancelled() {
                return Ok(false);
            }
            match steps.step(grep.policy).await? {
                GrepProgress::Built(
                    GrepBuildOutcome::Indexed { .. } | GrepBuildOutcome::BackfillRestarted { .. },
                )
                | GrepProgress::Reorganized(
                    GrepReorganizeOutcome::UnitWritten { .. } | GrepReorganizeOutcome::Abandoned,
                ) => continue,
                GrepProgress::Built(
                    GrepBuildOutcome::UpToDate { .. } | GrepBuildOutcome::NotEnabled,
                )
                | GrepProgress::Reorganized(
                    GrepReorganizeOutcome::NotNeeded { .. } | GrepReorganizeOutcome::NotEnabled,
                ) => return Ok(true),
                GrepProgress::Built(GrepBuildOutcome::Superseded)
                | GrepProgress::Reorganized(GrepReorganizeOutcome::Superseded) => return Ok(false),
            }
        }
        Ok(false)
    }
}

struct IndexBuild<'claim> {
    entry: Option<&'claim Mutex<HeldNamespace<SchedulerState>>>,
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

/// How the scheduler's claims map records a namespace whose steps are
/// running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Claim {
    /// Steps have the namespace.
    Claimed,
    /// Steps have the namespace, and a sweep that reached it left its
    /// retention advance to those steps.
    RetentionAdvanceLeft,
}

/// What a claim does when other steps have the namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Overlap {
    /// Skip the namespace.
    Skip,
    /// Skip the namespace, and let the steps in progress make the retention
    /// advance.
    LeaveRetentionAdvance,
}

/// One claim on a namespace. Dropping it releases the namespace, and
/// [`Scheduler::finish_claim`] releases it after the advances that sweeps
/// left.
pub(super) struct ClaimedNamespace<'scheduler> {
    inner: &'scheduler SchedulerInner,
    namespace_id: NamespaceId,
    released: bool,
}

impl ClaimedNamespace<'_> {
    /// Releases the namespace, unless a sweep left its retention advance to
    /// these steps. Then clears that mark, keeps the namespace, and returns
    /// `true`.
    fn release_unless_retention_due(&mut self) -> bool {
        let mut claims = lock(&self.inner.claims);
        let claim = claims
            .get_mut(&self.namespace_id)
            .expect("a claimed namespace should stay in the claims map until it's released");
        if std::mem::replace(claim, Claim::Claimed) == Claim::RetentionAdvanceLeft {
            return true;
        }
        claims.remove(&self.namespace_id);
        self.released = true;
        false
    }
}

impl Drop for ClaimedNamespace<'_> {
    fn drop(&mut self) {
        if !self.released {
            lock(&self.inner.claims).remove(&self.namespace_id);
        }
    }
}
