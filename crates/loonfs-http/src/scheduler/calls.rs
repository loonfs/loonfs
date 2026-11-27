//! The scheduler's claim on a namespace, the metadata, retention, garbage
//! collection, and index calls that a namespace's steps make, and the
//! collection call that a sweep makes on a pool.

use crate::scheduler::contain;
use crate::scheduler::lock;
use crate::scheduler::retries::CallFailure;
use crate::scheduler::retries::MaintenanceCall;
use crate::scheduler::Collected;
use crate::scheduler::GrepIndexing;
use crate::scheduler::Scheduler;
use crate::scheduler::SchedulerInner;
use crate::scheduler::SchedulerState;
use crate::scheduler::MAX_GREP_STEPS_PER_INDEX_STEP;
use crate::{HeldNamespace, WriterSessions};
use futures::FutureExt as _;
use loonfs::SharedObjectStore;
use loonfs::{
    AdvanceRetentionFloorOptions, CommitSeq, ErrorCode, Maintenance, MetadataMaintenanceOutcome,
    Namespace, NamespaceId, PoolId, RetentionTarget, Writable,
};
use loonfs_grep::{
    GrepBuildOutcome, GrepError, GrepProgress, GrepPublishOutcome, GrepReorganizeOutcome, GrepSteps,
};

use std::collections::hash_map::Entry;
use std::sync::Mutex;
use tokio::sync::SemaphorePermit;

impl Scheduler {
    /// Runs the steps of one namespace for a sweep, and returns the result of
    /// its collection, or `None` when the collection failed or didn't run.
    ///
    /// The metadata, index, and retention steps run on the namespace's
    /// writable handle: the held one, or for a namespace without a held
    /// session, one that the sweep opens only when reads show maintenance
    /// due (see [`Self::sweep_writer`]). A namespace that the check or the
    /// metadata call finds deleted or missing gets only its collection.
    pub(super) async fn sweep_namespace(
        &self,
        maintenance: &Maintenance,
        namespace_id: NamespaceId,
    ) -> Option<Collected> {
        let inner = &self.inner;
        let (claim, _permit) = self
            .claim(&namespace_id, Overlap::LeaveRetentionAdvance)
            .await?;
        let writer = self.sweep_writer(maintenance, &namespace_id).await;
        let entry = inner.namespaces.entry(&namespace_id);
        let entry = entry.as_deref();
        let seq = entry.and_then(|entry| lock(entry).handle.last_published_seq());
        // The call records its own failure. The index call and the advance
        // of a namespace that the call finds gone can only find it gone too.
        if let Ok(Some(writer)) = &writer {
            let gone = self
                .maintain_metadata(writer, &namespace_id, entry)
                .await
                .is_err_and(|code| code.is_namespace_gone());
            if !gone {
                if let Some(grep) = &inner.grep {
                    self.maintain_index(grep, writer, &namespace_id, entry, seq)
                        .await;
                }
                // The advance reads only folded commits, so it runs after the
                // metadata call, which can fold a tail at its fold age.
                self.advance_retention(writer, &namespace_id, entry).await;
            }
        }
        drop(writer);
        let collected = match self.gc(maintenance, &namespace_id, entry, seq).await {
            Ok(collected) => Some(collected),
            Err(code) => code.is_namespace_gone().then(Collected::default),
        };
        self.finish_claim(claim).await;
        collected
    }

    /// Returns the writable handle that a sweep runs a namespace's steps on:
    /// the held session's, or for a namespace without a held session, a
    /// session that the table opens once
    /// [`Maintenance::due_maintenance`] shows maintenance due, or the grep
    /// worker shows index work (see [`GrepWorker::index_due`](loonfs_grep::GrepWorker::index_due)). Returns
    /// `None` for a namespace with nothing due, so the sweep writes nothing
    /// to it, and the code that the check or the open failed with
    /// otherwise, after it records the failure as a metadata call's.
    async fn sweep_writer(
        &self,
        maintenance: &Maintenance,
        namespace_id: &NamespaceId,
    ) -> Result<Option<Namespace<Writable>>, ErrorCode> {
        let inner = &self.inner;
        if let Some(entry) = inner.namespaces.entry(namespace_id) {
            return Ok(Some(lock(&entry).handle.clone()));
        }
        let failure = |error: loonfs::Error| CallFailure::error(error.code(), error);
        let opened = async {
            let due = maintenance
                .due_maintenance(namespace_id)
                .await
                .map_err(failure)?
                .any();
            let index_due = match &inner.grep {
                Some(grep) if !due => grep
                    .worker
                    .index_due(namespace_id, grep.policy)
                    .await
                    .map_err(|error| CallFailure::error(error.code(), error))?,
                _ => false,
            };
            if !due && !index_due {
                return Ok(None);
            }
            inner
                .namespaces
                .open(namespace_id)
                .await
                .map(Some)
                .map_err(failure)
        };
        let result = contain(opened).await;
        let code = result.as_ref().err().map(CallFailure::code);
        match self.record_call(namespace_id, None, MaintenanceCall::Metadata, result, 0) {
            Some(writer) => Ok(writer),
            None => Err(code.expect("a call without output failed")),
        }
    }

    /// Makes one metadata call on `writer`, the namespace's writable handle,
    /// and returns its outcome, or the code that it failed with. Unfinished
    /// work, or a failure, leaves the call due again after
    /// `metadata_retry_interval_ms`.
    pub(super) async fn maintain_metadata(
        &self,
        writer: &Namespace<Writable>,
        namespace_id: &NamespaceId,
        entry: Option<&Mutex<HeldNamespace<SchedulerState>>>,
    ) -> Result<MetadataMaintenanceOutcome, ErrorCode> {
        let inner = &self.inner;
        // The seq before the step, so a commit during the step counts as one
        // the step didn't see, and the next maintenance interval runs the
        // step again.
        let seq = entry.map(|entry| lock(entry).handle.last_published_seq());
        let result = contain(
            writer
                .maintain_metadata_while_due(&inner.stop)
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
        // to ask again. A step without a position read the store, so
        // it waits for the retry interval. A tail that waits for its fold
        // age is asked again at that age if it comes first.
        let positioned = entry.is_some_and(|entry| {
            let mut held = lock(entry);
            held.host_state.last_step_seq = seq.flatten();
            if outcome.is_ok() {
                held.host_state.fold_due_at_ms = next_due_at_ms;
            }
            held.handle.has_current_writer_position()
        });
        let interval_ms = inner.config.metadata_retry_interval_ms.get();
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

    /// Makes one index call on `writer`, the namespace's writable handle,
    /// and returns whether it found the namespace gone.
    pub(super) async fn maintain_index(
        &self,
        grep: &GrepIndexing,
        writer: &Namespace<Writable>,
        namespace_id: &NamespaceId,
        entry: Option<&Mutex<HeldNamespace<SchedulerState>>>,
        seq: Option<CommitSeq>,
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
            self.build_index(grep, writer)
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
        // overlaps the steps. Only the steps of a held session overlap a
        // sweep, so the advance runs on the held session's handle.
        while claimed.release_unless_retention_due() {
            let Some(entry) = self.inner.namespaces.entry(&claimed.namespace_id) else {
                continue;
            };
            let writer = lock(&entry).handle.clone();
            self.advance_retention(&writer, &claimed.namespace_id, Some(&entry))
                .await;
        }
    }

    /// Makes one collection call, and returns what it listed, or the code
    /// that it failed with.
    pub(super) async fn gc(
        &self,
        maintenance: &Maintenance,
        namespace_id: &NamespaceId,
        entry: Option<&Mutex<HeldNamespace<SchedulerState>>>,
        seq: Option<CommitSeq>,
    ) -> Result<Collected, ErrorCode> {
        let inner = &self.inner;
        if inner.stop.is_cancelled() {
            return Err(ErrorCode::ShuttingDown);
        }
        let result = contain(
            maintenance
                .gc(namespace_id)
                .map(|result| result.map_err(|error| CallFailure::error(error.code(), error))),
        )
        .await;
        let collected = match &result {
            Ok(response) => Ok(Collected {
                listed_bytes: response.listed_bytes,
                next_reclamation_at_ms: response.next_reclamation_at_ms,
            }),
            Err(failure) => Err(failure.code()),
        };
        self.record_call(namespace_id, entry, MaintenanceCall::Gc, result, 0);
        if let (Ok(_), Some(entry)) = (&collected, entry) {
            let mut held = lock(entry);
            held.host_state.collected_seq = seq;
            held.host_state.collected_at_ms = inner.timer.monotonic_now_ms();
        }
        collected
    }

    /// Makes one collection call on a pool, and returns its result, or
    /// `None` when the collection failed or didn't run.
    ///
    /// A pool whose prefix emptied after the listing found it is skipped
    /// without a failure.
    pub(super) async fn gc_pool(
        &self,
        maintenance: &Maintenance,
        pool_id: &PoolId,
    ) -> Option<Collected> {
        let inner = &self.inner;
        if inner.stop.is_cancelled() {
            return None;
        }
        let result = contain(
            maintenance
                .gc_pool(pool_id)
                .map(|result| result.map_err(|error| CallFailure::error(error.code(), error))),
        )
        .await;
        match result {
            Ok(response) => Some(Collected {
                listed_bytes: response.listed_bytes,
                next_reclamation_at_ms: response.next_reclamation_at_ms,
            }),
            Err(failure) if failure.code() == ErrorCode::PoolNotFound => Some(Collected::default()),
            Err(failure) if failure.code() == ErrorCode::ShuttingDown => None,
            Err(failure) => {
                self.record_pool_failure(pool_id, &failure);
                None
            }
        }
    }

    /// Advances the namespace's retention floor to the last commit at or
    /// before the current time minus the namespace's retention window, with
    /// the same advance as the `retention` job, on `writer`, the namespace's
    /// writable handle. A namespace without a window keeps its floor. A
    /// deleted namespace returns `namespace_deleted`, which isn't a failure.
    ///
    /// A floor that moves is recorded in a floor object that the writer
    /// session puts in the WAL, and the advance then folds the namespace, so
    /// a manifest records the floor. For when the deletions below the new
    /// floor expire, and when their rows are removed, see [Advancing the
    /// floor on a
    /// schedule](https://github.com/loonfs/loonfs/blob/main/docs/design/retention-window.md#advancing-the-floor-on-a-schedule).
    async fn advance_retention(
        &self,
        writer: &Namespace<Writable>,
        namespace_id: &NamespaceId,
        entry: Option<&Mutex<HeldNamespace<SchedulerState>>>,
    ) {
        let inner = &self.inner;
        if inner.stop.is_cancelled() {
            return;
        }
        let options = AdvanceRetentionFloorOptions {
            target: RetentionTarget::Window,
        };
        // Boxed so the spawned scheduler's `Send` check does not recurse
        // through every engine future the advance awaits.
        let result = contain(
            writer
                .advance_retention_floor_with_options(&options)
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
        writer: &Namespace<Writable>,
    ) -> Result<bool, GrepError> {
        let mut steps = grep
            .worker
            .steps(writer, self.inner.config.maintenance_interval_ms.get());
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
