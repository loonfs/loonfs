//! Session ticks and the daily listing pass share namespace visits.

use futures::{FutureExt as _, StreamExt as _};
use loonfs::metrics::{
    CounterHandle, GaugeHandle, HistogramHandle, MetricsRecorder, RESULT_ERROR, RESULT_OK,
};
use loonfs::{
    AdvanceRetentionFloorOptions, ChangeSeq, ErrorCode, GcOptions, Maintenance,
    MaintenanceCancellation, MetadataMaintenanceOutcome, NamespaceId, RetentionTarget,
    SharedObjectStore,
};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepError, GrepReorganizeOutcome, GrepWorker,
};
use loonfs_http::{panic_message, HeldNamespace, HostState, Namespaces};
use loonfs_objectstore::layout::{list_namespace_ids, MAX_NAMESPACE_IDS_PER_PAGE};
use loonfs_objectstore::ObjectStoreError;
use loonfs_types::{EffectiveLimit, MonotonicTimer};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::Future;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::task::JoinHandle;
use tokio::time::Sleep;

mod passes;

/// Grep build steps one visit runs while each step publishes.
const MAX_GREP_BUILD_STEPS_PER_VISIT: usize = 16;

/// Longest failure backoff for a call that keeps failing on a held
/// namespace. The backoff after the first failure is one tick interval, and
/// each later failure in a row doubles it up to this bound.
const MAX_SWEEP_BACKOFF_MS: u64 = 60 * 60 * 1000;

const SWEEP_PASS_SECONDS_BOUNDARIES: &[f64] =
    &[1.0, 10.0, 60.0, 300.0, 900.0, 3_600.0, 14_400.0, 86_400.0];

/// The sweep's current values for one held session: the seqs that it last
/// indexed and collected, and the retry state of each call that ticks
/// retry.
pub(crate) struct SweepState {
    indexed_seq: Option<ChangeSeq>,
    collected_seq: Option<ChangeSeq>,
    collected_at_ms: u64,
    metadata_retry: SweepRetry,
    index_retry: SweepRetry,
    gc_retry: SweepRetry,
}

impl HostState for SweepState {
    /// Returns the state of a new session, whose collection interval starts
    /// when the session opens.
    fn new(opened_at_ms: u64) -> Self {
        Self {
            indexed_seq: None,
            collected_seq: None,
            collected_at_ms: opened_at_ms,
            metadata_retry: SweepRetry::default(),
            index_retry: SweepRetry::default(),
            gc_retry: SweepRetry::default(),
        }
    }
}

impl SweepState {
    /// Whether garbage collection is due for a session at `seq`: the seq
    /// moved since the last collection, and at least `interval_ms` passed
    /// since that collection.
    fn collection_pending(&self, seq: Option<ChangeSeq>, now_ms: u64, interval_ms: u64) -> bool {
        seq != self.collected_seq && now_ms.saturating_sub(self.collected_at_ms) >= interval_ms
    }
}

/// Whether the grep index has work for a session at `seq`: the index is
/// marked dirty, or the sweep last brought the index up to date at another
/// seq.
fn index_pending(held: &HeldNamespace<SweepState>, seq: Option<ChangeSeq>) -> bool {
    held.index_dirty || held.host_state.indexed_seq != seq
}

/// When a sweep next makes one call on a held namespace, and how many times
/// in a row that call has failed there.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SweepRetry {
    /// The earliest time the sweep's ticks make the call again, on the
    /// sweep's monotonic timer.
    pub(crate) retry_at_ms: u64,
    /// The call's failures in a row, zero after an attempt that didn't fail.
    pub(crate) attempts: u32,
}

/// Whether a sweep pass also collects each namespace's garbage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GarbageCollection {
    /// Collect each namespace's garbage and its grep garbage.
    Collect,
    /// Make the visit's other calls without collecting garbage.
    Skip,
}

/// Ticks held sessions every `tick_interval_ms`.
///
/// Active sessions publish and maintain themselves. Settling sessions run the
/// first due step: metadata, index, collection, or close. Quiet sessions cost
/// no store requests. Metadata that remains unfinished waits
/// `maintenance_interval_ms` before retrying. Collection requires a moved seq
/// and `gc_interval_ms` elapsed. Each held entry keeps one retry time per
/// call, which ticks wait for. A call that fails waits its failure backoff:
/// one tick interval, doubled for each failure in a row up to
/// [`MAX_SWEEP_BACKOFF_MS`]. A failed metadata call waits the later of its
/// backoff and `maintenance_interval_ms`, because its work remains. A success
/// clears the backoff. A call that panics counts as a `server_error` failure.
/// The panic stops neither the sweep nor the visits to other namespaces.
/// Quiet sessions close after `idle_session_close_after_ms` without an open,
/// when no caller holds a handle. An open during close waits for its drain. A
/// failing call keeps an idle session held, so ticks retry it, until its
/// backoff reaches [`MAX_SWEEP_BACKOFF_MS`].
///
/// The daily listing pass runs at start and every `full_sweep_interval_ms`,
/// alongside ticks. It covers namespaces without held sessions and garbage
/// still inside its grace window. It makes every call on every namespace it
/// lists without waiting for a held entry's retry times, and a namespace
/// without a held entry keeps no retry state, because the next pass is its
/// retry.
///
/// The pass also advances each active namespace's retention floor to the
/// last commit at or before the current time minus the namespace's retention
/// window, or minus `retention_window_ms` for a namespace that has no window
/// of its own. The
/// advance reads the namespace's window from the manifest that it loads to
/// find the floor, so a namespace with neither window costs that one load
/// and publishes nothing. Ticks don't advance the floor on their own
/// schedule, because an advance checks that the manifest's segments exist and
/// publishes a manifest, and a window measured in days needs no finer
/// schedule than the daily pass. A namespace whose floor already reached its
/// cutoff publishes nothing. When the pass reaches a namespace that a tick is
/// visiting, it leaves the advance to that visit, which makes it after its
/// own step. The pass can't wait for a visit that may be running a long
/// compaction, and skipping the day's advance would make the window a day
/// longer for exactly the namespaces that are busiest.
///
/// Before each advance, the visit acknowledges the floor that the previous
/// pass set, with [`Maintenance::acknowledge_retention_floor`]. A namespace
/// whose writer committed since the previous pass has acknowledged that
/// floor, or acknowledges it when the visit folds its tail, and a namespace
/// that nobody wrote to gets a fence. A writer session that the fence fences
/// returns `writer_fenced` to its next request, like after any takeover. A
/// failed acknowledgment keeps no retry state either, and the next pass
/// tries it again.
///
/// The pass and the ticks share `max_concurrent_maintenance` visit slots and
/// never visit the same namespace concurrently. Listing failures stop new
/// visits; already started visits finish before returning. Hosts can call
/// [`Self::run_pass`] and [`Self::tick`] directly.
#[derive(Clone)]
pub struct Sweep {
    inner: Arc<SweepInner>,
}

/// The cadence and limits of a sweep, under the names of the server config
/// settings.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SweepConfig {
    pub(crate) max_concurrent_maintenance: NonZeroUsize,
    pub(crate) tick_interval_ms: u64,
    pub(crate) maintenance_interval_ms: u64,
    pub(crate) gc_interval_ms: u64,
    pub(crate) full_sweep_interval_ms: u64,
    /// The server's retention window, which applies to each namespace
    /// without a window of its own.
    pub(crate) retention_window_ms: Option<NonZeroU64>,
    pub(crate) idle_session_close_after_ms: u64,
}

struct SweepInner {
    store: SharedObjectStore,
    maintenance: Maintenance,
    grep: Option<GrepIndexing>,
    namespaces: Arc<Namespaces<SweepState>>,
    timer: Arc<dyn MonotonicTimer>,
    config: SweepConfig,
    /// The namespaces being visited.
    visiting: Mutex<HashMap<NamespaceId, VisitClaim>>,
    slots: Semaphore,
    page_limit: EffectiveLimit,
    stop: MaintenanceCancellation,
    metrics: SweepMetrics,
}

/// How one call failed on one namespace.
enum CallFailure {
    /// The call returned an error.
    Error { code: ErrorCode, error: String },
    /// The call panicked, which counts as a `server_error` failure.
    Panic { panic: String },
}

impl CallFailure {
    fn error(code: ErrorCode, error: impl std::fmt::Display) -> Self {
        Self::Error {
            code,
            error: error.to_string(),
        }
    }

    fn code(&self) -> ErrorCode {
        match self {
            Self::Error { code, .. } => *code,
            Self::Panic { .. } => ErrorCode::ServerError,
        }
    }

    /// Whether the failure counts against the call. A namespace that is
    /// gone, or a server that is shutting down, doesn't.
    fn counts(&self) -> bool {
        !matches!(
            self.code(),
            ErrorCode::NamespaceNotFound | ErrorCode::NamespaceDeleted | ErrorCode::ShuttingDown
        )
    }
}

/// Where a call stands on a held namespace after one attempt.
#[derive(Debug, Clone, Copy)]
struct Retry {
    /// Failures in a row before the attempt.
    failures_before: u32,
    /// Failures in a row after it, zero when it didn't fail.
    attempts: u32,
    /// How long the sweep waits before it makes the call again.
    wait_ms: u64,
}

/// The grep worker of a server that maintains the grep index, and the build
/// policy of its steps.
pub(crate) struct GrepIndexing {
    worker: GrepWorker<SharedObjectStore>,
    policy: GramIndexBuildPolicy,
}

impl GrepIndexing {
    /// Builds the indexing of `worker`, whose steps run under `policy`.
    pub(crate) fn new(worker: GrepWorker<SharedObjectStore>, policy: GramIndexBuildPolicy) -> Self {
        Self { worker, policy }
    }
}

/// The calls one visit makes. Failures are counted by these names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SweepCall {
    Metadata,
    GrepIndex,
    /// Garbage collection, which runs the core collector and then the grep index's collector
    /// through [`loonfs_http::gc_namespace`].
    Gc,
    /// The retention advance, which the full pass makes, or leaves to a
    /// visit that has the namespace when the pass reaches it.
    Retention,
    /// The acknowledgment of the retention floor, which the visit that
    /// makes the retention advance makes first.
    RetentionAcknowledgment,
}

impl SweepCall {
    const ALL: [Self; 5] = [
        Self::Metadata,
        Self::GrepIndex,
        Self::Gc,
        Self::Retention,
        Self::RetentionAcknowledgment,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::GrepIndex => "grep_index",
            Self::Gc => "gc",
            Self::Retention => "retention",
            Self::RetentionAcknowledgment => "retention_acknowledgment",
        }
    }

    /// The call's retry state on a held entry. The retention advance and
    /// the acknowledgment keep none, because ticks never retry them, so a
    /// failed one waits for the next full pass like a call on a namespace
    /// without a held entry.
    fn retry(self, state: &SweepState) -> Option<SweepRetry> {
        match self {
            Self::Metadata => Some(state.metadata_retry),
            Self::GrepIndex => Some(state.index_retry),
            Self::Gc => Some(state.gc_retry),
            Self::Retention | Self::RetentionAcknowledgment => None,
        }
    }

    fn retry_mut(self, state: &mut SweepState) -> Option<&mut SweepRetry> {
        match self {
            Self::Metadata => Some(&mut state.metadata_retry),
            Self::GrepIndex => Some(&mut state.index_retry),
            Self::Gc => Some(&mut state.gc_retry),
            Self::Retention | Self::RetentionAcknowledgment => None,
        }
    }
}

impl Sweep {
    /// Builds a sweep over `store` with the cadence and limits in `config`,
    /// which schedules its calls on `timer`. `grep` is the indexing of a
    /// server that maintains the grep index, and `None` otherwise.
    pub(crate) fn new(
        config: SweepConfig,
        store: SharedObjectStore,
        maintenance: Maintenance,
        namespaces: Arc<Namespaces<SweepState>>,
        timer: Arc<dyn MonotonicTimer>,
        grep: Option<GrepIndexing>,
        recorder: &dyn MetricsRecorder,
    ) -> Self {
        Self {
            inner: Arc::new(SweepInner {
                store,
                maintenance,
                grep,
                namespaces,
                timer,
                config,
                visiting: Mutex::default(),
                slots: Semaphore::new(config.max_concurrent_maintenance.get()),
                page_limit: EffectiveLimit::new(
                    NonZeroU32::new(MAX_NAMESPACE_IDS_PER_PAGE)
                        .expect("the namespace page limit should be nonzero"),
                ),
                stop: MaintenanceCancellation::new(),
                metrics: SweepMetrics::register(recorder),
            }),
        }
    }

    /// Lists `page_limit` namespace ids per store request instead of the
    /// most a provider returns, so a test can cross a page boundary.
    #[cfg(test)]
    pub(crate) fn page_limit(mut self, page_limit: EffectiveLimit) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("a new sweep should have one owner")
            .page_limit = page_limit;
        self
    }

    /// Visits every namespace the store lists once. With
    /// [`GarbageCollection::Collect`], each visit also collects the
    /// namespace's garbage and its grep garbage. Each visit also acknowledges
    /// the namespace's retention floor and then advances it under its
    /// retention window.
    ///
    /// Returns how many namespaces the pass listed, or the listing failure
    /// that ended it once the visits it had started returned. A failed call
    /// on one namespace does not fail the pass.
    /// After [`Sweep`] is stopped, no new visit starts and a running
    /// compaction unit stops at its next block.
    pub async fn run_pass(&self, garbage: GarbageCollection) -> Result<usize, ObjectStoreError> {
        let inner = &self.inner;
        let started_at_ms = inner.timer.monotonic_now_ms();
        let collect_garbage = garbage == GarbageCollection::Collect;
        let listed = self.visit_every_namespace(garbage).await;
        let elapsed_ms = inner.timer.monotonic_now_ms().saturating_sub(started_at_ms);
        inner
            .metrics
            .pass_seconds
            .record(Duration::from_millis(elapsed_ms).as_secs_f64());
        match &listed {
            Ok(namespaces) => {
                inner.metrics.passes_ok.increment(1);
                inner
                    .metrics
                    .namespaces
                    .set(i64::try_from(*namespaces).unwrap_or(i64::MAX));
                tracing::info!(
                    namespaces,
                    collect_garbage,
                    elapsed_ms,
                    "maintenance sweep pass finished"
                );
            }
            Err(error) => {
                inner.metrics.passes_failed.increment(1);
                tracing::warn!(
                    error = %error,
                    collect_garbage,
                    elapsed_ms,
                    "namespace listing failed; the next sweep pass lists again"
                );
            }
        }
        listed
    }

    async fn visit_every_namespace(
        &self,
        garbage: GarbageCollection,
    ) -> Result<usize, ObjectStoreError> {
        let inner = &self.inner;
        // Boxed for the same reason as a visit: the spawned sweep's `Send`
        // check fails on the unboxed listing future.
        let pages = futures::stream::try_unfold(Some(None), move |start_after| async move {
            let Some(start_after) = start_after else {
                return Ok(None);
            };
            let page =
                list_namespace_ids(&*inner.store, start_after.as_ref(), inner.page_limit).await?;
            Ok(Some((page.namespace_ids, page.next_cursor.map(Some))))
        })
        .boxed();
        let mut listed = 0;
        let mut listing = Ok(());
        pages
            .filter_map(|page| {
                let namespace_ids = match page {
                    Ok(namespace_ids) => {
                        listed += namespace_ids.len();
                        Some(futures::stream::iter(namespace_ids))
                    }
                    Err(error) => {
                        listing = Err(error);
                        None
                    }
                };
                futures::future::ready(namespace_ids)
            })
            .flatten()
            .take_until(inner.stop.cancelled())
            .for_each_concurrent(
                inner.config.max_concurrent_maintenance.get(),
                |namespace_id| {
                    // Boxed so the spawned sweep's `Send` check does not recurse
                    // through every engine future a visit awaits.
                    self.visit(namespace_id, garbage).map(drop).boxed()
                },
            )
            .await;
        listing.map(|()| listed)
    }

    async fn visit(&self, namespace_id: NamespaceId, garbage: GarbageCollection) {
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
    async fn maintain_metadata(
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

    async fn maintain_index(
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
    async fn claim<'a>(
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
    async fn finish_visit(&self, mut visit: Visiting<'_>) {
        // Each turn makes the advance that one pass left while this visit
        // had the namespace, so the loop turns once for each full pass that
        // overlaps the visit.
        while visit.release_unless_retention_due() {
            let entry = self.inner.namespaces.entry(&visit.namespace_id);
            self.advance_retention(&visit.namespace_id, entry.as_deref())
                .await;
        }
    }

    /// Whether `retry` is a call that keeps failing on a held namespace and
    /// whose backoff has reached [`MAX_SWEEP_BACKOFF_MS`]. An idle session
    /// doesn't wait for such a call before it closes. Below the cap, the
    /// session stays held so that ticks retry the call on its backoff,
    /// instead of the next full pass.
    fn backoff_at_cap(&self, retry: SweepRetry) -> bool {
        retry.attempts > 0
            && backoff_ms(self.first_backoff_ms(), retry.attempts) >= MAX_SWEEP_BACKOFF_MS
    }

    /// The backoff after a call's first failure: one tick interval.
    fn first_backoff_ms(&self) -> u64 {
        self.inner.config.tick_interval_ms
    }

    async fn gc(
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
    /// index is up to date. Answers whether the index has nothing left to
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

    /// Records how one call ended on a namespace, and returns the call's
    /// output when it succeeded. A failure counts in `sweep_visit_failures`,
    /// and no code ends the sweep's maintenance of a namespace.
    fn record_call<T>(
        &self,
        namespace_id: &NamespaceId,
        entry: Option<&Mutex<HeldNamespace<SweepState>>>,
        call: SweepCall,
        result: Result<T, CallFailure>,
        work_wait_ms: u64,
    ) -> Option<T> {
        let (output, failure) = match result {
            Ok(output) => (Some(output), None),
            Err(failure) => (None, Some(failure).filter(CallFailure::counts)),
        };
        let retry =
            entry.and_then(|entry| self.schedule(entry, call, failure.is_some(), work_wait_ms));
        match (&failure, retry) {
            (Some(failure), retry) => self.record_failure(namespace_id, call, failure, retry),
            (None, Some(retry)) if output.is_some() && retry.failures_before > 0 => {
                tracing::info!(
                    namespace_id = %namespace_id,
                    call = call.as_str(),
                    attempts = retry.failures_before,
                    "maintenance sweep call succeeded after failing; its backoff is cleared"
                );
            }
            (None, _) => {}
        }
        output
    }

    /// Sets when the sweep next makes `call` on a held namespace, the one
    /// place that computes it. Returns `None` for a call that keeps no retry
    /// state.
    ///
    /// The call waits `work_wait_ms`, the wait its own work asks for. After
    /// a failure, it waits the later of that and its failure backoff
    /// ([`backoff_ms`]), so a failed metadata call, whose work remains, waits
    /// at least `maintenance_interval_ms`.
    fn schedule(
        &self,
        entry: &Mutex<HeldNamespace<SweepState>>,
        call: SweepCall,
        failed: bool,
        work_wait_ms: u64,
    ) -> Option<Retry> {
        let now_ms = self.inner.timer.monotonic_now_ms();
        let first_backoff_ms = self.first_backoff_ms();
        let mut held = lock(entry);
        let state = call.retry_mut(&mut held.host_state)?;
        let failures_before = state.attempts;
        let (attempts, wait_ms) = if failed {
            let attempts = failures_before.saturating_add(1);
            let backoff_ms = backoff_ms(first_backoff_ms, attempts);
            (attempts, work_wait_ms.max(backoff_ms))
        } else {
            (0, work_wait_ms)
        };
        *state = SweepRetry {
            retry_at_ms: now_ms.saturating_add(wait_ms),
            attempts,
        };
        Some(Retry {
            failures_before,
            attempts,
            wait_ms,
        })
    }

    /// Counts a failed call, and logs it with where the call stands on a
    /// held namespace.
    ///
    /// On a held namespace, the first failure in a row logs at error level
    /// and later ones at warning level, and the doubling backoff limits how
    /// often they repeat. A namespace that no host holds keeps no retry
    /// state, and neither do the retention advance and its acknowledgment,
    /// because the next full pass is their retry. Each full pass that finds
    /// one failing logs one error line, so the namespace logs once per
    /// `full_sweep_interval_ms` (a day by default). The rule of logging a
    /// repeating failure once allows one line per interval, so a failure
    /// that's retried once a day can log on every retry. Every panic logs
    /// at error level.
    fn record_failure(
        &self,
        namespace_id: &NamespaceId,
        call: SweepCall,
        failure: &CallFailure,
        retry: Option<Retry>,
    ) {
        let metrics = &self.inner.metrics;
        metrics.visit_failures[call as usize].increment(1);
        let code = failure.code();
        let attempts = retry.map(|retry| retry.attempts);
        let retry_after_ms = retry.map(|retry| retry.wait_ms);
        match failure {
            CallFailure::Panic { panic } => {
                metrics.panics.increment(1);
                tracing::error!(
                    namespace_id = %namespace_id,
                    call = call.as_str(),
                    code = %code,
                    attempts,
                    retry_after_ms,
                    panic = panic.as_str(),
                    "maintenance sweep call panicked; the sweep retries it"
                );
            }
            CallFailure::Error { error, .. } if attempts.is_none_or(|attempts| attempts == 1) => {
                tracing::error!(
                    namespace_id = %namespace_id,
                    call = call.as_str(),
                    code = %code,
                    attempts,
                    retry_after_ms,
                    error = error.as_str(),
                    "maintenance sweep call failed; the sweep retries it"
                );
            }
            CallFailure::Error { error, .. } => tracing::warn!(
                namespace_id = %namespace_id,
                call = call.as_str(),
                code = %code,
                attempts,
                retry_after_ms,
                error = error.as_str(),
                "maintenance sweep call failed again; the sweep retries it"
            ),
        }
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

/// How a visit holds a namespace in the sweep's visiting map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VisitClaim {
    /// A visit has the namespace.
    Claimed,
    /// A visit has the namespace, and a full pass that reached it left its
    /// retention advance to the visit.
    RetentionAdvanceLeft,
}

/// What a claim does when another visit has the namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Overlap {
    /// Skip the namespace.
    Skip,
    /// Skip the namespace, and let the visit in progress make the retention
    /// advance.
    LeaveRetentionAdvance,
}

/// One visit's claim on a namespace. Dropping it releases the namespace, and
/// [`Sweep::finish_visit`] releases it after the advances a pass left.
struct Visiting<'sweep> {
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

/// The failure backoff after the `attempts`th failure in a row:
/// `first_backoff_ms`, doubled for each failure after the first, at most
/// [`MAX_SWEEP_BACKOFF_MS`].
fn backoff_ms(first_backoff_ms: u64, attempts: u32) -> u64 {
    let doublings = attempts.saturating_sub(1);
    1_u64
        .checked_shl(doublings)
        .map_or(u64::MAX, |factor| first_backoff_ms.saturating_mul(factor))
        .min(MAX_SWEEP_BACKOFF_MS)
}

/// Runs one call on a namespace, and contains a panic in it as a
/// `server_error` failure, so the call backs off and is retried like any
/// failed call, and the sweep goes on with its other calls and namespaces.
async fn contain<T>(work: impl Future<Output = Result<T, CallFailure>>) -> Result<T, CallFailure> {
    AssertUnwindSafe(work)
        .catch_unwind()
        .await
        .unwrap_or_else(|payload| {
            Err(CallFailure::Panic {
                panic: panic_message(payload.as_ref()).to_owned(),
            })
        })
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("a maintenance sweep lock should not be poisoned")
}

/// A started sweep. Stop it before the runtime shuts down.
pub(crate) struct RunningSweep {
    stop: MaintenanceCancellation,
    task: JoinHandle<()>,
}

impl RunningSweep {
    /// Starts no new visit and stops a running compaction unit at its next
    /// block. Returns at once.
    pub(crate) fn cancel(&self) {
        self.stop.cancel();
    }

    /// Cancels the sweep and waits for the calls it is running to return,
    /// until `deadline`. When the deadline passes first, aborts the sweep,
    /// which drops the visits still running, and logs a warning. A dropped
    /// visit leaves what a crash leaves. Like an expired request drain, that
    /// is not an error.
    pub(crate) async fn stop(
        self,
        deadline: Pin<&mut Sleep>,
        shutdown_deadline_ms: u64,
    ) -> loonfs::Result<()> {
        self.stop.cancel();
        let mut task = self.task;
        tokio::select! {
            biased;
            joined = &mut task => joined.map_err(|error| {
                loonfs::Error::RuntimeTask(format!("maintenance sweep failed: {error}"))
            }),
            () = deadline => {
                task.abort();
                // The dropped visits are gone before the runtime shuts down.
                let _ = task.await;
                tracing::warn!(
                    shutdown_deadline_ms,
                    "shutdown deadline passed before the maintenance sweep stopped; \
                     its running visits are abandoned"
                );
                Ok(())
            }
        }
    }
}

/// The instruments one sweep reports, registered when it is built.
struct SweepMetrics {
    passes_ok: Arc<dyn CounterHandle>,
    passes_failed: Arc<dyn CounterHandle>,
    pass_seconds: Arc<dyn HistogramHandle>,
    visit_failures: [Arc<dyn CounterHandle>; SweepCall::ALL.len()],
    panics: Arc<dyn CounterHandle>,
    namespaces: Arc<dyn GaugeHandle>,
}

impl SweepMetrics {
    fn register(recorder: &dyn MetricsRecorder) -> Self {
        let passes = |result: &'static str| {
            recorder.register_counter(
                "loonfs.maintenance.sweep_passes",
                "Maintenance sweep passes, by whether the namespace listing finished",
                &[("result", result)],
            )
        };
        Self {
            passes_ok: passes(RESULT_OK),
            passes_failed: passes(RESULT_ERROR),
            pass_seconds: recorder.register_histogram(
                "loonfs.maintenance.sweep_pass_seconds",
                "Maintenance sweep pass duration in seconds",
                &[],
                SWEEP_PASS_SECONDS_BOUNDARIES,
            ),
            visit_failures: SweepCall::ALL.map(|call| {
                recorder.register_counter(
                    "loonfs.maintenance.sweep_visit_failures",
                    "Maintenance sweep calls that failed on one namespace, by call",
                    &[("call", call.as_str())],
                )
            }),
            panics: recorder.register_counter(
                "loonfs.maintenance.sweep_panics",
                "Maintenance sweep calls and idle-session closes that panicked",
                &[],
            ),
            namespaces: recorder.register_gauge(
                "loonfs.maintenance.sweep_namespaces",
                "Namespaces the last finished maintenance sweep pass listed",
                &[],
            ),
        }
    }
}

#[cfg(test)]
mod tests;
