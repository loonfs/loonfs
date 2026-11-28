//! The maintenance scheduler: the due steps of each open session, and the
//! sweeps that list every namespace and every pool in the store.

use crate::scheduler::calls::Claim;
use crate::scheduler::metrics::SchedulerMetrics;
use crate::scheduler::retries::CallFailure;
use crate::{HostState, Namespaces};
use futures::{FutureExt as _, StreamExt as _};
use loonfs::metrics::MetricsRecorder;
use loonfs::{CommitSeq, Maintenance, MaintenanceCancellation, NamespaceId, SharedObjectStore};
use loonfs_grep::{GramIndexBuildPolicy, GrepWorker};
use loonfs_objectstore::layout::{list_namespace_ids, list_pool_ids, MAX_NAMESPACE_IDS_PER_PAGE};
use loonfs_objectstore::ObjectStoreError;
use loonfs_types::{panic_message, EffectiveLimit, MonotonicTimer};
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

/// The state that the scheduler keeps beside each session of its
/// [`Namespaces`] table, so a host builds the table as
/// `Namespaces<SchedulerState>`.
///
/// It records the seqs that the scheduler last indexed and collected, what
/// the last metadata step reported about the tail, and the retry state of
/// each step that the scheduler retries.
pub struct SchedulerState {
    indexed_seq: Option<CommitSeq>,
    collected_seq: Option<CommitSeq>,
    collected_at_ms: u64,
    /// When the tail's oldest commit reaches the fold age, on the
    /// scheduler's monotonic timer, as the last metadata step that succeeded
    /// reported it (see
    /// [`MetadataMaintenanceReport::next_due_at_ms`](loonfs::MetadataMaintenanceReport::next_due_at_ms)).
    fold_due_at_ms: Option<u64>,
    /// The highest seq the session had committed when its last metadata
    /// step started, whether or not that step succeeded, and `None` before
    /// the first step. A commit since then can leave work in the tail that
    /// no step has dated.
    last_step_seq: Option<CommitSeq>,
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
            fold_due_at_ms: None,
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
    fn collection_pending(&self, seq: Option<CommitSeq>, now_ms: u64, interval_ms: u64) -> bool {
        seq != self.collected_seq && now_ms.saturating_sub(self.collected_at_ms) >= interval_ms
    }
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

/// Runs each open session's due steps every `maintenance_interval_ms`, and
/// sweeps every namespace in the store every `sweep_interval_ms`.
///
/// Every step that publishes a manifest or writes a WAL object runs on the
/// namespace's writable handle, as the namespace's writer session, which
/// claims the namespace first when it has no writer epoch (format spec,
/// "Writer ownership"). So a step that has work due on a namespace that
/// another server writes displaces that server.
///
/// Every maintenance interval, a session that has a writer position
/// gets a metadata step when the step's retry time has come: a fold of the
/// WAL tail if one is due, then compaction units if one is due, through the
/// namespace's one fold path and one compaction path, which the session's
/// own fold and compaction also use. A step that the session's position
/// shows to be not due costs no store requests. The scheduler then runs the
/// first due step of collection, index, and close. A session without a
/// position gets no metadata step, because it isn't writing and the
/// sweep maintains its namespace's metadata. The one exception is a step
/// that reads the store. It runs when the tail's oldest commit reaches the
/// fold age, at the time that the session's last step reported, or once the
/// session has committed since that step, so that the tail of a session
/// whose position was dropped before the scheduler saw the position is dated
/// too. A metadata step that leaves work unfinished runs again
/// `metadata_retry_interval_ms` later, or sooner when the tail's oldest
/// commit reaches the fold age first. A collection runs once the session's
/// published seq has moved and `gc_interval_ms` has elapsed, and it comes
/// before the index step, so a session with index work at every interval
/// still collects.
///
/// Each held entry keeps one retry time per step, which the scheduler waits
/// for. A step that fails waits its failure backoff: one maintenance
/// interval, doubled for each failure in a row up to one hour. A failed
/// metadata step waits the later of its backoff and
/// `metadata_retry_interval_ms`, because its work remains. A success clears
/// the backoff. A step that panics counts as a `server_error` failure. The
/// panic stops neither the scheduler nor the steps of other namespaces.
///
/// A session closes after `idle_session_close_after_ms` without an open,
/// when nothing else is due, its metadata step in the same run found its
/// metadata caught up or it has no position, and no caller has its
/// handle. An open during close waits for its drain. A failing step keeps an
/// idle session held, so the scheduler retries the step, until its backoff
/// reaches one hour. A session that was closed or fenced, or whose namespace
/// is gone, closes once no caller has its handle.
///
/// A scheduler whose config doesn't set `sweep_store` belongs to a host that
/// leaves the store's sweeps and collections to another process. It still
/// runs the metadata and index steps of the sessions that it holds, because
/// those are the work of each namespace's writer, and it runs no collection
/// step and sweeps nothing.
///
/// A sweep runs at start and then every `sweep_interval_ms`, beside the
/// session steps. It covers namespaces without held sessions and garbage
/// still inside its grace window. For a namespace without a held session,
/// the sweep first reads what maintenance is due (see
/// [`Maintenance::due_maintenance`], and on a server with grep,
/// [`GrepWorker::index_due`](loonfs_grep::GrepWorker::index_due)), and it
/// opens a held session only for a namespace with a fold, a compaction
/// unit, a retention advance, or index work due. The session claims the
/// namespace at its first publication, so a sweep over a namespace with
/// nothing due writes nothing to it. It runs the metadata, index, and retention steps of
/// every namespace that it holds a session for or opened one for, and it
/// doesn't wait for a held entry's retry times. A namespace that the check or
/// the metadata call finds deleted or missing gets only its collection. A
/// namespace without a held entry keeps no retry state, because the next
/// sweep is its retry.
///
/// A sweep also advances each active namespace's retention floor to the last
/// commit at or before the current time minus the namespace's retention
/// window. The session steps don't advance the floor on their own schedule,
/// because an advance checks that the manifest's segments exist and
/// publishes a manifest, and a window measured in days needs no finer
/// schedule than the sweep. A namespace whose floor already reached its
/// cutoff publishes nothing. When a sweep reaches a namespace whose session
/// steps are running, it leaves the advance to those steps, which make the
/// advance after they finish. The sweep can't wait for steps that may be
/// running a long compaction, and skipping the day's advance would make the
/// window a day longer for exactly the namespaces that are busiest.
///
/// An advance whose floor moves creates a floor object in the WAL through
/// the namespace's writer session, as one item of the session's queue, and
/// then folds the namespace, so a manifest records the new floor. A failed
/// advance keeps no retry state either, and the next sweep tries it again.
///
/// The scheduler starts a session's steps without waiting for the steps
/// that other sessions started at the interval before, so a long step on one
/// session delays no other session's steps. Sweeps and session steps share
/// `max_concurrent_maintenance` slots, and the scheduler never runs the
/// steps of one namespace twice at once. A listing failure stops a sweep
/// from starting more namespaces or pools, and the namespaces and pools it
/// already started finish before it returns a [`SweepReport`] that isn't
/// complete.
///
/// The scheduler runs nothing on its own until [`Self::start`] starts its
/// two loops. A host that decides when maintenance runs calls
/// [`Self::run_due_steps`] and [`Self::run_sweep`] instead, and
/// [`Self::cancel`] stops either way of running it.
#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<SchedulerInner>,
}

/// The result of one sweep of the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SweepReport {
    /// The number of namespaces that the sweep listed, including those listed
    /// before a listing failed.
    pub namespaces: usize,
    /// The number of pools that the sweep listed, including those listed
    /// before a listing failed.
    pub pools: usize,
    /// The sum of `listed_bytes` over the responses of the sweep's
    /// collections (see [`GcResponse::listed_bytes`](loonfs::GcResponse::listed_bytes)).
    pub listed_bytes: u64,
    /// Whether the sweep covered the whole store: both listings reached their
    /// last page, and each listed namespace and pool has a collection result.
    /// A listing that the scheduler's stop cut short didn't reach its last
    /// page. A collection that returned a response, or that found its
    /// namespace or pool gone, is a result. A collection that failed or
    /// didn't run isn't.
    pub complete: bool,
    /// The earliest `next_reclamation_at_ms` in the responses of the sweep's
    /// collections, or `None` when no response includes a reclamation time.
    pub next_reclamation_at_ms: Option<u64>,
}

/// The result of the collection of one namespace or pool, or the sum of
/// several. A namespace or pool that the collection found gone adds zero
/// bytes.
#[derive(Debug, Clone, Copy, Default)]
struct Collected {
    listed_bytes: u64,
    next_reclamation_at_ms: Option<u64>,
}

impl Collected {
    /// Returns the sum of the listed bytes and the earlier reclamation time.
    fn add(self, other: Self) -> Self {
        Self {
            listed_bytes: self.listed_bytes.saturating_add(other.listed_bytes),
            next_reclamation_at_ms: self
                .next_reclamation_at_ms
                .into_iter()
                .chain(other.next_reclamation_at_ms)
                .min(),
        }
    }
}

/// What one listing of a sweep covered: whether the listing reached its last
/// page, the number of IDs that it listed, how many of them have a collection
/// result, and the sum of those results. A listing that failed or that the
/// scheduler's stop cut short didn't reach its last page.
#[derive(Debug, Default)]
struct Covered {
    finished: bool,
    listed: usize,
    collected: usize,
    sum: Collected,
}

/// The default of [`SchedulerConfig::max_concurrent_maintenance`], which the
/// reference server's config uses.
pub const DEFAULT_MAX_CONCURRENT_MAINTENANCE: NonZeroUsize = NonZeroUsize::new(8).unwrap();
/// The default of [`SchedulerConfig::maintenance_interval_ms`], which the
/// reference server's config uses.
pub const DEFAULT_MAINTENANCE_INTERVAL_MS: NonZeroU64 = NonZeroU64::new(5_000).unwrap();
/// The default of [`SchedulerConfig::metadata_retry_interval_ms`], which the
/// reference server's config uses.
pub const DEFAULT_METADATA_RETRY_INTERVAL_MS: NonZeroU64 = NonZeroU64::new(300_000).unwrap();
/// The default of [`SchedulerConfig::gc_interval_ms`], which the reference
/// server's config uses.
pub const DEFAULT_GC_INTERVAL_MS: NonZeroU64 = NonZeroU64::new(3_600_000).unwrap();
/// The default of [`SchedulerConfig::sweep_interval_ms`], which the
/// reference server's config uses.
pub const DEFAULT_SWEEP_INTERVAL_MS: NonZeroU64 = NonZeroU64::new(86_400_000).unwrap();
/// The default of [`SchedulerConfig::idle_session_close_after_ms`], which the
/// reference server's config uses.
pub const DEFAULT_IDLE_SESSION_CLOSE_AFTER_MS: NonZeroU64 = NonZeroU64::new(1_800_000).unwrap();

/// The cadence and limits of the scheduler, under the names of the
/// reference server's config settings.
#[derive(Debug, Clone, Copy)]
pub struct SchedulerConfig {
    /// How many namespaces the scheduler runs steps on at once, shared by
    /// sweeps and session steps.
    pub max_concurrent_maintenance: NonZeroUsize,
    /// Milliseconds between the scheduler's runs of each held session's due
    /// steps, and the backoff after a step's first failure.
    pub maintenance_interval_ms: NonZeroU64,
    /// Milliseconds before a metadata step that left work runs again.
    pub metadata_retry_interval_ms: NonZeroU64,
    /// The minimum time, in milliseconds, between two collections of a held
    /// session whose seq moved.
    pub gc_interval_ms: NonZeroU64,
    /// Milliseconds between sweeps, after the sweep at start.
    pub sweep_interval_ms: NonZeroU64,
    /// How long, in milliseconds, after a held session's last open the
    /// scheduler waits before it closes the session, when the session has
    /// nothing due and no caller.
    pub idle_session_close_after_ms: NonZeroU64,
    /// Whether the scheduler sweeps the store and collects the held
    /// sessions' namespaces. A host that leaves sweeps and collections to
    /// another process sets this to `false`.
    pub sweep_store: bool,
}

struct SchedulerInner {
    store: SharedObjectStore,
    /// The maintenance of the runtime that `namespaces` was built from, which
    /// reads what maintenance is due and collects garbage.
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

/// The grep worker of a host that builds the grep index of the namespaces
/// that it writes, and the build policy of its steps.
pub struct GrepIndexing {
    worker: GrepWorker<SharedObjectStore>,
    policy: GramIndexBuildPolicy,
}

impl GrepIndexing {
    /// Builds the indexing of `worker`, whose steps run under `policy`.
    pub fn new(worker: GrepWorker<SharedObjectStore>, policy: GramIndexBuildPolicy) -> Self {
        Self { worker, policy }
    }
}

impl Scheduler {
    /// Builds a scheduler over the sessions of `namespaces`, with the cadence
    /// and limits in `config`, and starts no loop. The scheduler reads the
    /// store and the monotonic timer of the runtime that `namespaces` was
    /// built from, and it runs maintenance through that runtime too, on the
    /// writable handles of the table's sessions. `grep` is the indexing of a
    /// host that builds the grep index of the namespaces that it writes, and
    /// `None` otherwise. The scheduler registers its metrics with
    /// `recorder`.
    pub fn new(
        config: SchedulerConfig,
        namespaces: Arc<Namespaces<SchedulerState>>,
        grep: Option<GrepIndexing>,
        recorder: &dyn MetricsRecorder,
    ) -> Self {
        let runtime = namespaces.runtime();
        Self {
            inner: Arc::new(SchedulerInner {
                store: runtime.object_store(),
                timer: runtime.monotonic_timer(),
                maintenance: runtime.maintenance(),
                grep,
                namespaces,
                config,
                claims: Mutex::default(),
                slots: Semaphore::new(config.max_concurrent_maintenance.get()),
                page_limit: EffectiveLimit::new(MAX_NAMESPACE_IDS_PER_PAGE),
                stop: MaintenanceCancellation::new(),
                metrics: SchedulerMetrics::register(recorder),
            }),
        }
    }

    /// Stops the scheduler from starting new steps, and stops a running
    /// compaction unit at its next block. Returns at once.
    pub fn cancel(&self) {
        self.inner.stop.cancel();
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

    /// Runs one sweep: the steps of every namespace that the store lists,
    /// including a collection, and then a collection of every pool that the
    /// store lists.
    /// Each namespace's retention floor is advanced under its retention window.
    ///
    /// Returns what the sweep listed and collected, once the steps and
    /// collections it had started finished. A failed call on one namespace or
    /// pool doesn't stop the sweep, and a listing failure stops it from
    /// starting more namespaces or pools. The report isn't complete after a
    /// failed listing, a listing that the stop cut short, a failed
    /// collection, or a namespace that the sweep left to its running steps.
    /// After the [`Scheduler`] is stopped, no namespace starts its steps, no
    /// pool starts its collection, and a running compaction unit stops at its
    /// next block.
    /// A scheduler whose config doesn't set `sweep_store` lists nothing and
    /// returns a report of zero namespaces and pools that isn't complete.
    pub async fn run_sweep(&self) -> SweepReport {
        let inner = &self.inner;
        let maintenance = &inner.maintenance;
        if !inner.config.sweep_store {
            return SweepReport {
                namespaces: 0,
                pools: 0,
                listed_bytes: 0,
                complete: false,
                next_reclamation_at_ms: None,
            };
        }
        let started_at_ms = inner.timer.monotonic_now_ms();
        let (namespaces, mut listing) = self.sweep_every_namespace(maintenance).await;
        let mut pools = Covered::default();
        if listing.is_ok() {
            (pools, listing) = self.collect_every_pool(maintenance).await;
        }
        let sum = namespaces.sum.add(pools.sum);
        let report = SweepReport {
            namespaces: namespaces.listed,
            pools: pools.listed,
            listed_bytes: sum.listed_bytes,
            complete: [&namespaces, &pools]
                .iter()
                .all(|covered| covered.finished && covered.collected == covered.listed),
            next_reclamation_at_ms: sum.next_reclamation_at_ms,
        };
        let elapsed_ms = inner.timer.monotonic_now_ms().saturating_sub(started_at_ms);
        inner
            .metrics
            .sweep_seconds
            .record(Duration::from_millis(elapsed_ms).as_secs_f64());
        match listing {
            Ok(()) => {
                inner.metrics.sweeps_ok.increment(1);
                inner
                    .metrics
                    .namespaces
                    .set(i64::try_from(report.namespaces).unwrap_or(i64::MAX));
                tracing::info!(
                    namespaces = report.namespaces,
                    pools = report.pools,
                    listed_bytes = report.listed_bytes,
                    complete = report.complete,
                    elapsed_ms,
                    "maintenance sweep finished"
                );
            }
            Err(error) => {
                inner.metrics.sweeps_failed.increment(1);
                tracing::warn!(
                    error = %error,
                    elapsed_ms,
                    "namespace or pool listing failed; the next sweep lists again"
                );
            }
        }
        report
    }

    /// Collects every pool that the store lists, with the concurrency of the
    /// namespace sweep, and returns what it covered and whether its listing
    /// failed, once the collections it had started returned.
    async fn collect_every_pool(
        &self,
        maintenance: &Maintenance,
    ) -> (Covered, Result<(), ObjectStoreError>) {
        let inner = &self.inner;
        self.sweep_listed_ids(
            |start_after| async move {
                let page =
                    list_pool_ids(&*inner.store, start_after.as_ref(), inner.page_limit).await?;
                Ok((page.pool_ids, page.next_cursor))
            },
            |pool_id| async move { self.gc_pool(maintenance, &pool_id).await },
        )
        .await
    }

    async fn sweep_every_namespace(
        &self,
        maintenance: &Maintenance,
    ) -> (Covered, Result<(), ObjectStoreError>) {
        let inner = &self.inner;
        self.sweep_listed_ids(
            |start_after| async move {
                let page =
                    list_namespace_ids(&*inner.store, start_after.as_ref(), inner.page_limit)
                        .await?;
                Ok((page.namespace_ids, page.next_cursor))
            },
            |namespace_id| self.sweep_namespace(maintenance, namespace_id),
        )
        .await
    }

    /// Runs `work` on every ID that `list_page` lists, with
    /// `max_concurrent_maintenance` at once, and returns what it covered and
    /// whether the listing failed, once the work it had started returned.
    /// `work` returns the ID's collection result, or `None` when the
    /// collection failed or didn't run.
    ///
    /// `list_page` lists the page after a cursor, or the first page for
    /// `None`, and returns the page's IDs and the next page's cursor. The
    /// sweep lists a page when a slot is free, and starts no more work after
    /// a listing failure or after the [`Scheduler`] is stopped.
    async fn sweep_listed_ids<Id, Cursor, Page, Work>(
        &self,
        list_page: impl Fn(Option<Cursor>) -> Page + Send + Sync,
        work: impl Fn(Id) -> Work + Send + Sync,
    ) -> (Covered, Result<(), ObjectStoreError>)
    where
        Id: Send,
        Cursor: Send,
        Page: Future<Output = Result<(Vec<Id>, Option<Cursor>), ObjectStoreError>> + Send,
        Work: Future<Output = Option<Collected>> + Send,
    {
        let inner = &self.inner;
        let list_page = &list_page;
        // Boxed for the same reason as the work on each ID: the spawned
        // scheduler's `Send` check fails on the unboxed listing future.
        let pages = futures::stream::try_unfold(Some(None), move |start_after| async move {
            let Some(start_after) = start_after else {
                return Ok(None);
            };
            let (ids, next_cursor) = list_page(start_after).await?;
            Ok(Some(((ids, next_cursor.is_none()), next_cursor.map(Some))))
        })
        .boxed();
        let mut finished = false;
        let mut listed = 0;
        let mut listing = Ok(());
        let (collected, sum) = pages
            .filter_map(|page| {
                let ids = match page {
                    Ok((ids, last)) => {
                        finished = last;
                        listed += ids.len();
                        Some(futures::stream::iter(ids))
                    }
                    Err(error) => {
                        listing = Err(error);
                        None
                    }
                };
                futures::future::ready(ids)
            })
            .flatten()
            .take_until(inner.stop.cancelled())
            // Boxed so the spawned scheduler's `Send` check does not recurse
            // through every engine future that the work awaits.
            .map(|id| work(id).boxed())
            .buffer_unordered(inner.config.max_concurrent_maintenance.get())
            .filter_map(futures::future::ready)
            .fold((0, Collected::default()), |(collected, sum), id_result| {
                futures::future::ready((collected + 1, sum.add(id_result)))
            })
            .await;
        (
            Covered {
                finished,
                listed,
                collected,
                sum,
            },
            listing,
        )
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

/// The loops of a scheduler that [`Scheduler::start`] started. Stop them
/// before the runtime shuts down.
pub struct RunningScheduler {
    scheduler: Scheduler,
    task: JoinHandle<()>,
}

impl RunningScheduler {
    /// Cancels the scheduler and waits for the calls it is running to
    /// return, until `deadline`. When the deadline ends first, aborts the
    /// scheduler, which drops the steps still running, and logs a warning
    /// that includes `shutdown_deadline_ms`. A dropped step leaves what a
    /// crash leaves. Like an expired request drain, that is not an error.
    pub async fn stop(
        self,
        deadline: Pin<&mut Sleep>,
        shutdown_deadline_ms: u64,
    ) -> loonfs::Result<()> {
        self.scheduler.cancel();
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
                    "shutdown deadline passed before the scheduler stopped; its running steps \
                     are abandoned"
                );
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests;
