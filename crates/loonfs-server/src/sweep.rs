//! Session ticks and the daily listing pass share namespace visits.

use crate::sweep::metrics::SweepMetrics;
use crate::sweep::retries::CallFailure;
use crate::sweep::visit::VisitClaim;
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
use std::num::{NonZeroU64, NonZeroUsize};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::Sleep;

mod metrics;
mod passes;
mod retries;
mod visit;

/// Grep build steps one visit runs while each step publishes.
const MAX_GREP_BUILD_STEPS_PER_VISIT: usize = 16;

/// Longest failure backoff for a call that keeps failing on a held
/// namespace. The backoff after the first failure is one tick interval, and
/// each later failure in a row doubles it up to this bound.
const MAX_SWEEP_BACKOFF_MS: u64 = 60 * 60 * 1000;

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
/// when no caller has a handle. An open during close waits for its drain. A
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
                page_limit: EffectiveLimit::new(MAX_NAMESPACE_IDS_PER_PAGE),
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

#[cfg(test)]
mod tests;
