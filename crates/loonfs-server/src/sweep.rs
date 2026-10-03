//! Session ticks and the daily listing pass share namespace visits.

use crate::config::{ServerConfig, ServerConfigError};
use futures::{FutureExt as _, StreamExt as _};
use loonfs::metrics::{
    CounterHandle, GaugeHandle, HistogramHandle, MetricsRecorder, RESULT_ERROR, RESULT_OK,
};
use loonfs::{
    ChangeSeq, ErrorCode, Maintenance, MaintenanceCancellation, MetadataMaintenanceOptions,
    NamespaceId, SharedObjectStore,
};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepError, GrepReorganizeOutcome, GrepWorker,
};
use loonfs_http::{HeldNamespace, Namespaces};
use loonfs_objectstore::layout::{list_namespace_ids, MAX_NAMESPACE_IDS_PAGE_LIMIT};
use loonfs_objectstore::timing::{MonotonicTimer, StdMonotonicTimer};
use loonfs_objectstore::ObjectStoreError;
use loonfs_types::EffectiveLimit;
use std::collections::HashSet;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::task::JoinHandle;
use tokio::time::Sleep;

mod passes;

/// Grep build steps one visit runs while each step publishes.
const MAX_GREP_BUILD_STEPS_PER_VISIT: usize = 16;

const SWEEP_PASS_SECONDS_BOUNDARIES: &[f64] =
    &[1.0, 10.0, 60.0, 300.0, 900.0, 3_600.0, 14_400.0, 86_400.0];

/// Ticks held sessions every `tick_interval_ms`. Active sessions publish and
/// maintain themselves. Settling sessions run the first due step: metadata,
/// index, collection, or close. Quiet sessions cost no store requests.
/// Metadata that remains unfinished waits `maintenance_interval_ms` before
/// retrying. Collection requires a moved seq and `gc_interval_ms` elapsed.
/// Quiet sessions close after `idle_session_close_after_ms` without an open,
/// when no caller holds a handle. An open during close waits for its drain.
///
/// The daily listing pass runs at start and every `full_sweep_interval_ms`,
/// alongside ticks. It covers namespaces without held sessions and garbage
/// still inside its grace window. Both share `max_concurrent_maintenance`
/// visit slots and never visit the same namespace concurrently. Listing
/// failures stop new visits; already started visits finish before returning.
/// Hosts can call [`Self::run_pass`] and [`Self::tick`] directly.
#[derive(Clone)]
pub struct Sweep {
    inner: Arc<SweepInner>,
}

struct SweepInner {
    store: SharedObjectStore,
    maintenance: Maintenance,
    metadata: MetadataMaintenanceOptions,
    grep: Option<GrepIndexing>,
    namespaces: Arc<Namespaces>,
    max_concurrent_visits: usize,
    tick_interval: Duration,
    metadata_retry_ms: u64,
    collection_interval_ms: u64,
    full_interval: Duration,
    idle_session_close_after_ms: u64,
    visiting: Mutex<HashSet<NamespaceId>>,
    slots: Semaphore,
    page_limit: EffectiveLimit,
    stop: MaintenanceCancellation,
    timer: StdMonotonicTimer,
    metrics: SweepMetrics,
}

struct GrepIndexing {
    worker: GrepWorker<SharedObjectStore>,
    policy: GramIndexBuildPolicy,
}

/// The calls one visit makes. Failures are counted by these names.
#[derive(Debug, Clone, Copy)]
enum SweepCall {
    Metadata,
    GrepIndex,
    Gc,
    GrepGc,
}

impl SweepCall {
    const ALL: [Self; 4] = [Self::Metadata, Self::GrepIndex, Self::Gc, Self::GrepGc];

    fn as_str(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::GrepIndex => "grep_index",
            Self::Gc => "gc",
            Self::GrepGc => "grep_gc",
        }
    }
}

impl Sweep {
    /// Builds a sweep over `store` with the cadence and limits in `config`.
    /// `grep_worker` is the worker of a server that maintains the grep
    /// index, and `None` otherwise.
    pub(crate) fn new(
        config: &ServerConfig,
        store: SharedObjectStore,
        maintenance: Maintenance,
        namespaces: Arc<Namespaces>,
        grep_worker: Option<GrepWorker<SharedObjectStore>>,
        recorder: &dyn MetricsRecorder,
    ) -> Result<Self, ServerConfigError> {
        let grep = grep_worker
            .map(|worker| {
                let policy = config
                    .grep
                    .worker_config()
                    .build_policy()
                    .map_err(|error| ServerConfigError::InvalidField {
                        field: "grep",
                        reason: error.to_string(),
                    })?;
                Ok::<_, ServerConfigError>(GrepIndexing { worker, policy })
            })
            .transpose()?;
        Ok(Self {
            inner: Arc::new(SweepInner {
                store,
                maintenance,
                metadata: MetadataMaintenanceOptions {
                    idle_fold_after_ms: config.idle_fold_after_ms,
                    ..MetadataMaintenanceOptions::default()
                },
                grep,
                namespaces,
                max_concurrent_visits: config.max_concurrent_maintenance,
                tick_interval: Duration::from_millis(config.tick_interval_ms),
                metadata_retry_ms: config.maintenance_interval_ms,
                collection_interval_ms: config.gc_interval_ms,
                full_interval: Duration::from_millis(config.full_sweep_interval_ms),
                idle_session_close_after_ms: config.idle_session_close_after_ms,
                visiting: Mutex::default(),
                slots: Semaphore::new(config.max_concurrent_maintenance),
                page_limit: EffectiveLimit::new(
                    NonZeroU32::new(MAX_NAMESPACE_IDS_PAGE_LIMIT)
                        .expect("the namespace page limit should be nonzero"),
                ),
                stop: MaintenanceCancellation::new(),
                timer: StdMonotonicTimer::default(),
                metrics: SweepMetrics::register(recorder),
            }),
        })
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

    /// Visits every namespace the store lists once. With `collect_garbage`,
    /// each visit also collects the namespace's garbage and its grep
    /// garbage.
    ///
    /// Returns how many namespaces the pass listed, or the listing failure
    /// that ended it once the visits it had started returned. A failed call
    /// on one namespace does not fail the pass.
    /// After [`Sweep`] is stopped, no new visit starts and a running
    /// streaming compaction stops at its next block.
    pub async fn run_pass(&self, collect_garbage: bool) -> Result<usize, ObjectStoreError> {
        let inner = &self.inner;
        let started_ms = inner.timer.monotonic_now_ms();
        let listed = self.visit_every_namespace(collect_garbage).await;
        let elapsed_ms = inner.timer.monotonic_now_ms().saturating_sub(started_ms);
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
        collect_garbage: bool,
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
            .for_each_concurrent(inner.max_concurrent_visits, |namespace_id| {
                // Boxed so the spawned sweep's `Send` check does not recurse
                // through every engine future a visit awaits.
                self.visit(namespace_id, collect_garbage).map(drop).boxed()
            })
            .await;
        listing.map(|()| listed)
    }

    async fn visit(&self, namespace_id: NamespaceId, collect_garbage: bool) {
        let inner = &self.inner;
        let Some(_visit) = self.claim(&namespace_id).await else {
            return;
        };
        let entry = inner.namespaces.entry(&namespace_id);
        let seq = entry
            .as_ref()
            .and_then(|entry| lock(entry).handle.last_published_seq());
        let caught_up = self.maintain_metadata(&namespace_id).await;
        if let Some(entry) = &entry {
            self.record_metadata(entry, seq, caught_up);
        }
        if let Some(grep) = &inner.grep {
            self.maintain_index(grep, &namespace_id, entry.as_deref(), seq)
                .await;
        }
        if collect_garbage && self.collect(&namespace_id).await {
            if let Some(entry) = &entry {
                self.record_collection(entry, seq);
            }
        }
    }

    async fn maintain_metadata(&self, namespace_id: &NamespaceId) -> bool {
        let inner = &self.inner;
        match inner
            .maintenance
            .maintain_metadata_while_due_with_options(namespace_id, &inner.stop, &inner.metadata)
            .await
        {
            Ok(caught_up) => caught_up,
            Err(error) => {
                self.record_failure(namespace_id, SweepCall::Metadata, error.code(), &error);
                false
            }
        }
    }

    fn record_metadata(
        &self,
        entry: &Mutex<HeldNamespace>,
        seq: Option<ChangeSeq>,
        caught_up: bool,
    ) {
        let inner = &self.inner;
        let mut held = lock(entry);
        held.handle.record_metadata_maintenance(seq, caught_up);
        held.metadata_retry_after_ms = if caught_up {
            0
        } else {
            inner
                .namespaces
                .now_ms()
                .saturating_add(inner.metadata_retry_ms)
        };
    }

    fn record_collection(&self, entry: &Mutex<HeldNamespace>, seq: Option<ChangeSeq>) {
        let mut held = lock(entry);
        held.collected_seq = seq;
        held.collected_ms = self.inner.namespaces.now_ms();
    }

    async fn maintain_index(
        &self,
        grep: &GrepIndexing,
        namespace_id: &NamespaceId,
        entry: Option<&Mutex<HeldNamespace>>,
        seq: Option<ChangeSeq>,
    ) {
        if let Some(entry) = entry {
            lock(entry).index_dirty = false;
        }
        let mut build = IndexBuild {
            entry,
            caught_up: false,
        };
        build.caught_up = match self.build_index(grep, namespace_id).await {
            Ok(caught_up) => caught_up,
            Err(error) => {
                self.record_failure(namespace_id, SweepCall::GrepIndex, error.code(), &error);
                false
            }
        };
        if build.caught_up {
            if let Some(entry) = entry {
                lock(entry).indexed_seq = seq;
            }
        }
    }

    async fn claim<'a>(
        &'a self,
        namespace_id: &NamespaceId,
    ) -> Option<(Visiting<'a>, SemaphorePermit<'a>)> {
        let inner = &self.inner;
        if !lock(&inner.visiting).insert(namespace_id.clone()) {
            return None;
        }
        let visit = Visiting {
            inner,
            namespace_id: namespace_id.clone(),
        };
        let permit = tokio::select! {
            biased;
            () = inner.stop.cancelled() => return None,
            permit = inner.slots.acquire() => permit.ok()?,
        };
        Some((visit, permit))
    }

    async fn collect(&self, namespace_id: &NamespaceId) -> bool {
        let inner = &self.inner;
        if inner.stop.is_cancelled() {
            return false;
        }
        let mut collected = true;
        if let Err(error) = inner.maintenance.gc(namespace_id).await {
            self.record_failure(namespace_id, SweepCall::Gc, error.code(), &error);
            collected = false;
        }
        let Some(grep) = &inner.grep else {
            return collected;
        };
        if inner.stop.is_cancelled() {
            return false;
        }
        let result = match inner.maintenance.now_ms() {
            Ok(now_ms) => grep
                .worker
                .garbage_collect_namespace(namespace_id, now_ms)
                .await
                .map(drop),
            Err(error) => Err(GrepError::from(error)),
        };
        if let Err(error) = result {
            self.record_failure(namespace_id, SweepCall::GrepGc, error.code(), &error);
            collected = false;
        }
        collected
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

    fn record_failure(
        &self,
        namespace_id: &NamespaceId,
        call: SweepCall,
        code: ErrorCode,
        error: &dyn std::fmt::Display,
    ) {
        if matches!(
            code,
            ErrorCode::NamespaceNotFound | ErrorCode::NamespaceDeleted | ErrorCode::ShuttingDown
        ) {
            return;
        }
        self.inner.metrics.visit_failures[call as usize].increment(1);
        tracing::warn!(
            namespace_id = %namespace_id,
            call = call.as_str(),
            error = %error,
            "maintenance sweep call failed; the next pass tries it again"
        );
    }
}

struct IndexBuild<'visit> {
    entry: Option<&'visit Mutex<HeldNamespace>>,
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

struct Visiting<'sweep> {
    inner: &'sweep SweepInner,
    namespace_id: NamespaceId,
}

impl Drop for Visiting<'_> {
    fn drop(&mut self) {
        lock(&self.inner.visiting).remove(&self.namespace_id);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A started sweep. Stop it before the runtime shuts down.
pub(crate) struct RunningSweep {
    stop: MaintenanceCancellation,
    task: JoinHandle<()>,
}

impl RunningSweep {
    /// Starts no new visit and stops a running streaming compaction at its
    /// next block. Returns at once.
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
    visit_failures: [Arc<dyn CounterHandle>; 4],
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
