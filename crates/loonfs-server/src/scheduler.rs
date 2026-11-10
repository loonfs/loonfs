//! The maintenance scheduler: the due steps of each open session, and the
//! sweeps that list every namespace in the store.

use crate::scheduler::calls::Claim;
use crate::scheduler::metrics::SchedulerMetrics;
use crate::scheduler::retries::CallFailure;
use futures::{FutureExt as _, StreamExt as _};
use loonfs::metrics::MetricsRecorder;
use loonfs::{ChangeSeq, Maintenance, MaintenanceCancellation, NamespaceId, SharedObjectStore};
use loonfs_grep::{GramIndexBuildPolicy, GrepWorker};
use loonfs_http::{panic_message, HeldNamespace, HostState, Namespaces};
use loonfs_objectstore::layout::{list_namespace_ids, MAX_NAMESPACE_IDS_PER_PAGE};
use loonfs_objectstore::ObjectStoreError;
use loonfs_types::{EffectiveLimit, MonotonicTimer};
use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::Sleep;

mod calls;
mod metrics;
mod retries;
mod steps;

/// The largest number of grep steps, build or reorganize, that one index
/// step runs while each grep step does some work.
const MAX_GREP_STEPS_PER_INDEX_STEP: usize = 16;

/// Longest failure backoff for a step that keeps failing on a held
/// namespace. The backoff after the first failure is one maintenance
/// interval, and each later failure in a row doubles it up to this bound.
const MAX_STEP_BACKOFF_MS: u64 = 60 * 60 * 1000;

/// The scheduler's current values for one held session: the seqs that it
/// last indexed and collected, what the last metadata step reported about
/// the tail, and the retry state of each step that the scheduler retries.
pub(crate) struct SchedulerState {
    indexed_seq: Option<ChangeSeq>,
    collected_seq: Option<ChangeSeq>,
    collected_at_ms: u64,
    /// When the tail's idle fold comes due, on the scheduler's monotonic
    /// timer, as the last metadata step that succeeded reported it (see
    /// [`MetadataMaintenanceReport::next_due_at_ms`](loonfs::MetadataMaintenanceReport::next_due_at_ms)).
    idle_fold_due_at_ms: Option<u64>,
    /// The highest seq the session had committed when its last metadata
    /// step started, whether or not that step succeeded, and `None` before
    /// the first step. A commit since then can leave work in the tail that
    /// no step has dated.
    last_step_seq: Option<ChangeSeq>,
    metadata_retry: StepRetry,
    index_retry: StepRetry,
    gc_retry: StepRetry,
}

impl HostState for SchedulerState {
    /// Returns the state of a new session, whose collection interval starts
    /// when the session opens.
    fn new(opened_at_ms: u64) -> Self {
        Self {
            indexed_seq: None,
            collected_seq: None,
            collected_at_ms: opened_at_ms,
            idle_fold_due_at_ms: None,
            last_step_seq: None,
            metadata_retry: StepRetry::default(),
            index_retry: StepRetry::default(),
            gc_retry: StepRetry::default(),
        }
    }
}

impl SchedulerState {
    /// Whether garbage collection is due for a session at `seq`: the seq
    /// moved since the last collection, and at least `interval_ms` passed
    /// since that collection.
    fn collection_pending(&self, seq: Option<ChangeSeq>, now_ms: u64, interval_ms: u64) -> bool {
        seq != self.collected_seq && now_ms.saturating_sub(self.collected_at_ms) >= interval_ms
    }
}

/// Whether the grep index has work for a session at `seq`: the index is
/// marked dirty, or the scheduler last brought the index up to date at
/// another seq.
fn index_pending(held: &HeldNamespace<SchedulerState>, seq: Option<ChangeSeq>) -> bool {
    held.index_dirty || held.host_state.indexed_seq != seq
}

/// When the scheduler next makes one call on a held namespace, and how many
/// times in a row that call has failed there.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct StepRetry {
    /// The earliest time the scheduler makes the call again, on the
    /// scheduler's monotonic timer.
    pub(crate) retry_at_ms: u64,
    /// The call's failures in a row, zero after an attempt that didn't fail.
    pub(crate) attempts: u32,
}

/// Whether a sweep also collects each namespace's garbage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GarbageCollection {
    /// Collect each namespace's garbage.
    Collect,
    /// Run each namespace's other steps without collecting garbage.
    Skip,
}

/// Runs each open session's due steps every `maintenance_interval_ms`, and
/// sweeps every namespace in the store every `sweep_interval_ms`.
///
/// Every maintenance interval, a session that has a current writer position
/// gets a metadata step when the step's retry time has come: a fold of the
/// WAL tail if one is due, then compaction units if one is due, through the
/// namespace's one fold path and one compaction path, which the session's
/// own fold and compaction also use. A step that the session's position
/// shows to be not due costs no store requests. The scheduler then runs the
/// first due step of collection, index, and close. A session without a
/// current position gets no metadata step, because it isn't writing and the
/// sweep maintains its namespace's metadata. The one exception is a step
/// from the store once the session's tail reaches the idle fold age that its
/// last step reported, or once it has committed since that step, so that the
/// tail of a session whose position lapsed before the scheduler saw the
/// position is dated too. A metadata step that leaves work unfinished runs
/// again `metadata_retry_interval_ms` later, or sooner when the step
/// reported an earlier idle fold. A collection runs once the session's
/// published seq has moved and `gc_interval_ms` has elapsed, and it comes
/// before the index step, so a session with index work at every interval
/// still collects.
///
/// Each held entry keeps one retry time per step, which the scheduler waits
/// for. A step that fails waits its failure backoff: one maintenance
/// interval, doubled for each failure in a row up to
/// [`MAX_STEP_BACKOFF_MS`]. A failed metadata step waits the later of its
/// backoff and `metadata_retry_interval_ms`, because its work remains. A
/// success clears the backoff. A step that panics counts as a
/// `server_error` failure. The panic stops neither the scheduler nor the
/// steps of other namespaces.
///
/// A session closes after `idle_session_close_after_ms` without an open,
/// when nothing else is due, its metadata step in the same run found its
/// metadata caught up or it has no current position, and no caller has its
/// handle. An open during close waits for its drain. A failing step keeps an
/// idle session held, so the scheduler retries the step, until its backoff
/// reaches [`MAX_STEP_BACKOFF_MS`]. A session that was closed or fenced, or
/// whose namespace is gone, closes once no caller has its handle.
///
/// A sweep runs at start and then every `sweep_interval_ms`, beside the
/// session steps. It covers namespaces without held sessions and garbage
/// still inside its grace window. It runs every step of every namespace that
/// it lists, and it doesn't wait for a held entry's retry times. A namespace
/// without a held entry keeps no retry state, because the next sweep is its
/// retry.
///
/// A sweep also advances each active namespace's retention floor to the last
/// commit at or before the current time minus the namespace's retention
/// window. The advance reads the namespace's window from the manifest that
/// it loads to find the floor, so a namespace without a window costs that
/// one load and publishes nothing. The session steps don't advance the
/// floor on their own schedule, because an advance checks that the
/// manifest's segments exist and publishes a manifest, and a window measured
/// in days needs no finer schedule than the sweep. A namespace whose floor
/// already reached its cutoff publishes nothing. When a sweep reaches a
/// namespace whose session steps are running, it leaves the advance to those
/// steps, which make the advance after they finish. The sweep can't wait for
/// steps that may be running a long compaction, and skipping the day's
/// advance would make the window a day longer for exactly the namespaces
/// that are busiest.
///
/// An advance whose floor moves runs in the namespace's writer session in
/// this server, which folds the tail and publishes the floor under its
/// writer epoch. For a namespace that the server holds no session for, the
/// advance opens one, which acquires the writer epoch, and the session ends
/// when the advance returns. A writer session of the namespace in another
/// process returns `writer_fenced` to its next request, like after any
/// takeover. A failed advance keeps no retry state either, and the next
/// sweep tries it again.
///
/// The scheduler starts a session's steps without waiting for the steps
/// that other sessions started at the interval before, so a long step on one
/// session delays no other session's steps. Sweeps and session steps share
/// `max_concurrent_maintenance` slots, and the scheduler never runs the
/// steps of one namespace twice at once. A listing failure stops a sweep
/// from starting more namespaces, and the namespaces it already started
/// finish before it returns. Hosts can call [`Self::run_sweep`] and
/// [`Self::run_due_steps`] directly.
#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<SchedulerInner>,
}

/// The cadence and limits of the scheduler, under the names of the server
/// config settings.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SchedulerConfig {
    pub(crate) max_concurrent_maintenance: NonZeroUsize,
    pub(crate) maintenance_interval_ms: u64,
    pub(crate) metadata_retry_interval_ms: u64,
    pub(crate) gc_interval_ms: u64,
    pub(crate) sweep_interval_ms: u64,
    pub(crate) idle_session_close_after_ms: u64,
}

struct SchedulerInner {
    store: SharedObjectStore,
    maintenance: Maintenance,
    grep: Option<GrepIndexing>,
    namespaces: Arc<Namespaces<SchedulerState>>,
    timer: Arc<dyn MonotonicTimer>,
    config: SchedulerConfig,
    /// The namespaces whose steps are running.
    claims: Mutex<HashMap<NamespaceId, Claim>>,
    slots: Semaphore,
    page_limit: EffectiveLimit,
    stop: MaintenanceCancellation,
    metrics: SchedulerMetrics,
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

impl Scheduler {
    /// Builds a scheduler over `store` with the cadence and limits in
    /// `config`, which schedules its calls on `timer`. `grep` is the indexing
    /// of a server that maintains the grep index, and `None` otherwise.
    pub(crate) fn new(
        config: SchedulerConfig,
        store: SharedObjectStore,
        maintenance: Maintenance,
        namespaces: Arc<Namespaces<SchedulerState>>,
        timer: Arc<dyn MonotonicTimer>,
        grep: Option<GrepIndexing>,
        recorder: &dyn MetricsRecorder,
    ) -> Self {
        Self {
            inner: Arc::new(SchedulerInner {
                store,
                maintenance,
                grep,
                namespaces,
                timer,
                config,
                claims: Mutex::default(),
                slots: Semaphore::new(config.max_concurrent_maintenance.get()),
                page_limit: EffectiveLimit::new(MAX_NAMESPACE_IDS_PER_PAGE),
                stop: MaintenanceCancellation::new(),
                metrics: SchedulerMetrics::register(recorder),
            }),
        }
    }

    /// Lists `page_limit` namespace ids per store request instead of the
    /// most a provider returns, so a test can cross a page boundary.
    #[cfg(test)]
    pub(crate) fn page_limit(mut self, page_limit: EffectiveLimit) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("a new scheduler should have one owner")
            .page_limit = page_limit;
        self
    }

    /// Runs one sweep: the steps of every namespace that the store lists.
    /// With [`GarbageCollection::Collect`], each namespace also gets a
    /// collection. Each namespace's retention floor is advanced under its
    /// retention window.
    ///
    /// Returns how many namespaces the sweep listed, or the listing failure
    /// that ended it once the namespaces it had started returned. A failed
    /// call on one namespace does not fail the sweep. After the [`Scheduler`]
    /// is stopped, no namespace starts its steps, and a running compaction
    /// unit stops at its next block.
    pub async fn run_sweep(&self, garbage: GarbageCollection) -> Result<usize, ObjectStoreError> {
        let inner = &self.inner;
        let started_at_ms = inner.timer.monotonic_now_ms();
        let collect_garbage = garbage == GarbageCollection::Collect;
        let listed = self.sweep_every_namespace(garbage).await;
        let elapsed_ms = inner.timer.monotonic_now_ms().saturating_sub(started_at_ms);
        inner
            .metrics
            .sweep_seconds
            .record(Duration::from_millis(elapsed_ms).as_secs_f64());
        match &listed {
            Ok(namespaces) => {
                inner.metrics.sweeps_ok.increment(1);
                inner
                    .metrics
                    .namespaces
                    .set(i64::try_from(*namespaces).unwrap_or(i64::MAX));
                tracing::info!(
                    namespaces,
                    collect_garbage,
                    elapsed_ms,
                    "maintenance sweep finished"
                );
            }
            Err(error) => {
                inner.metrics.sweeps_failed.increment(1);
                tracing::warn!(
                    error = %error,
                    collect_garbage,
                    elapsed_ms,
                    "namespace listing failed; the next sweep lists again"
                );
            }
        }
        listed
    }

    async fn sweep_every_namespace(
        &self,
        garbage: GarbageCollection,
    ) -> Result<usize, ObjectStoreError> {
        let inner = &self.inner;
        // Boxed for the same reason as a namespace's steps: the spawned
        // scheduler's `Send` check fails on the unboxed listing future.
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
                    // Boxed so the spawned scheduler's `Send` check does not
                    // recurse through every engine future a namespace's steps
                    // await.
                    self.sweep_namespace(namespace_id, garbage)
                        .map(drop)
                        .boxed()
                },
            )
            .await;
        listing.map(|()| listed)
    }
}

/// Runs one call on a namespace, and contains a panic in it as a
/// `server_error` failure, so the call backs off and is retried like any
/// failed call, and the scheduler goes on with its other calls and
/// namespaces.
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
        .expect("a maintenance scheduler lock should not be poisoned")
}

/// A started scheduler. Stop it before the runtime shuts down.
pub(crate) struct RunningScheduler {
    stop: MaintenanceCancellation,
    task: JoinHandle<()>,
}

impl RunningScheduler {
    /// Starts no new steps and stops a running compaction unit at its next
    /// block. Returns at once.
    pub(crate) fn cancel(&self) {
        self.stop.cancel();
    }

    /// Cancels the scheduler and waits for the calls it is running to
    /// return, until `deadline`. When the deadline ends first, aborts the
    /// scheduler, which drops the steps still running, and logs a warning. A
    /// dropped step leaves what a crash leaves. Like an expired request
    /// drain, that is not an error.
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
                loonfs::Error::RuntimeTask(format!("maintenance scheduler failed: {error}"))
            }),
            () = deadline => {
                task.abort();
                // The dropped steps are gone before the runtime shuts down.
                let _ = task.await;
                tracing::warn!(
                    shutdown_deadline_ms,
                    "shutdown deadline passed before the maintenance scheduler stopped; \
                     its running steps are abandoned"
                );
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests;
