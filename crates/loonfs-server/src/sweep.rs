//! Session ticks and the daily listing pass share namespace visits.

use crate::config::{ServerConfig, ServerConfigError};
use futures::{FutureExt as _, StreamExt as _};
use loonfs::metrics::{
    CounterHandle, GaugeHandle, HistogramHandle, MetricsRecorder, RESULT_ERROR, RESULT_OK,
};
use loonfs::{
    ChangeSeq, ErrorCode, Maintenance, MaintenanceCancellation, MetadataMaintenanceOutcome,
    NamespaceId, SharedObjectStore,
};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepError, GrepReorganizeOutcome, GrepWorker,
};
use loonfs_http::{HeldNamespace, Namespaces};
use loonfs_objectstore::layout::{list_namespace_ids, MAX_NAMESPACE_IDS_PER_PAGE};
use loonfs_objectstore::ObjectStoreError;
use loonfs_types::config::ConfigFieldError;
use loonfs_types::EffectiveLimit;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::num::NonZeroU32;
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

/// Longest wait before the sweep retries a call that keeps failing on one
/// namespace. The first retry waits one tick interval, and each later failure
/// doubles the wait up to this bound.
const MAX_SWEEP_BACKOFF_MS: u64 = 60 * 60 * 1000;

/// Most `(namespace, call)` pairs the sweep backs off at once. At the bound,
/// a new failure evicts the entry whose retry is due first, so that call is
/// retried at its next chance.
const MAX_SWEEP_BACKOFF_ENTRIES: usize = 10_000;

const SWEEP_PASS_SECONDS_BOUNDARIES: &[f64] =
    &[1.0, 10.0, 60.0, 300.0, 900.0, 3_600.0, 14_400.0, 86_400.0];

/// Whether a sweep pass also collects each namespace's garbage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GarbageCollection {
    /// Collect each namespace's garbage and its grep garbage.
    Collect,
    /// Maintain metadata and the grep index only.
    Skip,
}

/// Ticks held sessions every `tick_interval_ms`.
///
/// Active sessions publish and maintain themselves. Settling sessions run the
/// first due step: metadata, index, collection, or close. Quiet sessions cost
/// no store requests. Metadata that remains unfinished waits
/// `maintenance_interval_ms` before retrying. Collection requires a moved seq
/// and `gc_interval_ms` elapsed. A call that fails on a namespace waits one
/// tick interval before its next try there, each failure in a row doubles the
/// wait up to [`MAX_SWEEP_BACKOFF_MS`], and a success clears it. A call that
/// panics counts as a `server_error` failure. The panic stops neither the sweep
/// nor the visits to other namespaces. Quiet sessions close after
/// `idle_session_close_after_ms` without an open, when no caller holds a
/// handle. An open during close waits for its drain. A failing call keeps an
/// idle session held, so ticks retry it, until its wait reaches
/// [`MAX_SWEEP_BACKOFF_MS`].
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
    grep: Option<GrepIndexing>,
    namespaces: Arc<Namespaces>,
    max_concurrent_visits: usize,
    tick_interval: Duration,
    metadata_retry_interval_ms: u64,
    collection_interval_ms: u64,
    full_interval: Duration,
    idle_session_close_after_ms: u64,
    visiting: Mutex<HashSet<NamespaceId>>,
    /// The calls that failed on a namespace and when the sweep retries each
    /// one. At most [`MAX_SWEEP_BACKOFF_ENTRIES`] entries; a success removes
    /// its entry.
    backoff: Mutex<HashMap<(NamespaceId, SweepCall), Backoff>>,
    slots: Semaphore,
    page_limit: EffectiveLimit,
    stop: MaintenanceCancellation,
    metrics: SweepMetrics,
}

/// One call that keeps failing on one namespace.
#[derive(Debug, Clone, Copy)]
struct Backoff {
    /// The code of the latest failure.
    code: ErrorCode,
    /// Failures in a row, counting the first.
    attempts: u32,
    /// When the sweep may make the call again, on the namespace table's clock.
    next_at_ms: u64,
}

/// Where a call that just failed stands in its backoff.
struct Retry {
    /// Failures in a row, counting this one.
    attempts: u32,
    /// How long the sweep waits before it makes the call again.
    wait_ms: u64,
}

struct GrepIndexing {
    worker: GrepWorker<SharedObjectStore>,
    policy: GramIndexBuildPolicy,
}

/// The calls one visit makes. Failures are counted by these names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
                    .map_err(|error| {
                        ServerConfigError::Field(ConfigFieldError::Invalid {
                            field: "grep",
                            reason: error.to_string(),
                        })
                    })?;
                Ok::<_, ServerConfigError>(GrepIndexing { worker, policy })
            })
            .transpose()?;
        Ok(Self {
            inner: Arc::new(SweepInner {
                store,
                maintenance,
                grep,
                namespaces,
                max_concurrent_visits: config.max_concurrent_maintenance.get(),
                tick_interval: Duration::from_millis(config.tick_interval_ms),
                metadata_retry_interval_ms: config.maintenance_interval_ms,
                collection_interval_ms: config.gc_interval_ms,
                full_interval: Duration::from_millis(config.full_sweep_interval_ms),
                idle_session_close_after_ms: config.idle_session_close_after_ms,
                visiting: Mutex::default(),
                backoff: Mutex::default(),
                slots: Semaphore::new(config.max_concurrent_maintenance.get()),
                page_limit: EffectiveLimit::new(
                    NonZeroU32::new(MAX_NAMESPACE_IDS_PER_PAGE)
                        .expect("the namespace page limit should be nonzero"),
                ),
                stop: MaintenanceCancellation::new(),
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

    /// Visits every namespace the store lists once. With
    /// [`GarbageCollection::Collect`], each visit also collects the
    /// namespace's garbage and its grep garbage.
    ///
    /// Returns how many namespaces the pass listed, or the listing failure
    /// that ended it once the visits it had started returned. A failed call
    /// on one namespace does not fail the pass.
    /// After [`Sweep`] is stopped, no new visit starts and a running
    /// compaction unit stops at its next block.
    pub async fn run_pass(&self, garbage: GarbageCollection) -> Result<usize, ObjectStoreError> {
        let inner = &self.inner;
        let started_at_ms = inner.namespaces.now_ms();
        let collect_garbage = garbage == GarbageCollection::Collect;
        let listed = self.visit_every_namespace(garbage).await;
        let elapsed_ms = inner.namespaces.now_ms().saturating_sub(started_at_ms);
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
            .for_each_concurrent(inner.max_concurrent_visits, |namespace_id| {
                // Boxed so the spawned sweep's `Send` check does not recurse
                // through every engine future a visit awaits.
                self.visit(namespace_id, garbage).map(drop).boxed()
            })
            .await;
        listing.map(|()| listed)
    }

    async fn visit(&self, namespace_id: NamespaceId, garbage: GarbageCollection) {
        let inner = &self.inner;
        let Some(_visit) = self.claim(&namespace_id).await else {
            return;
        };
        let entry = inner.namespaces.entry(&namespace_id);
        let seq = entry
            .as_ref()
            .and_then(|entry| lock(entry).handle.last_published_seq());
        if !self.backed_off(&namespace_id, SweepCall::Metadata) {
            let outcome = self.maintain_metadata(&namespace_id).await;
            if let Some(entry) = &entry {
                self.record_metadata(entry, seq, outcome);
            }
        }
        if let Some(grep) = &inner.grep {
            if !self.backed_off(&namespace_id, SweepCall::GrepIndex) {
                self.maintain_index(grep, &namespace_id, entry.as_deref(), seq)
                    .await;
            }
        }
        if garbage == GarbageCollection::Collect && self.gc(&namespace_id).await {
            if let Some(entry) = &entry {
                self.record_collection(entry, seq);
            }
        }
    }

    async fn maintain_metadata(&self, namespace_id: &NamespaceId) -> MetadataMaintenanceOutcome {
        let inner = &self.inner;
        let call = SweepCall::Metadata;
        let result = self.contain(
            namespace_id,
            call,
            inner
                .maintenance
                .maintain_metadata_while_due(namespace_id, &inner.stop),
        );
        match result.await {
            Some(Ok(response)) => {
                self.record_success(namespace_id, call);
                response.outcome
            }
            Some(Err(error)) => {
                self.record_failure(namespace_id, call, error.code(), &error);
                MetadataMaintenanceOutcome::Due
            }
            None => MetadataMaintenanceOutcome::Due,
        }
    }

    fn record_metadata(
        &self,
        entry: &Mutex<HeldNamespace>,
        seq: Option<ChangeSeq>,
        outcome: MetadataMaintenanceOutcome,
    ) {
        let inner = &self.inner;
        let mut held = lock(entry);
        held.handle.record_metadata_maintenance(seq, outcome);
        held.metadata_retry_at_ms = if outcome == MetadataMaintenanceOutcome::CaughtUp {
            0
        } else {
            inner
                .namespaces
                .now_ms()
                .saturating_add(inner.metadata_retry_interval_ms)
        };
    }

    fn record_collection(&self, entry: &Mutex<HeldNamespace>, seq: Option<ChangeSeq>) {
        let mut held = lock(entry);
        held.collected_seq = seq;
        held.collected_at_ms = self.inner.namespaces.now_ms();
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
        let call = SweepCall::GrepIndex;
        build.caught_up = match self
            .contain(namespace_id, call, self.build_index(grep, namespace_id))
            .await
        {
            Some(Ok(caught_up)) => {
                self.record_success(namespace_id, call);
                caught_up
            }
            Some(Err(error)) => {
                self.record_failure(namespace_id, call, error.code(), &error);
                false
            }
            None => false,
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

    /// Whether `call` failed on the namespace and its retry isn't due yet.
    fn backed_off(&self, namespace_id: &NamespaceId, call: SweepCall) -> bool {
        let now_ms = self.inner.namespaces.now_ms();
        lock(&self.inner.backoff)
            .get(&(namespace_id.clone(), call))
            .is_some_and(|backoff| now_ms < backoff.next_at_ms)
    }

    /// Whether `call` keeps failing on the namespace and its wait has reached
    /// [`MAX_SWEEP_BACKOFF_MS`]. An idle session doesn't wait for such a call
    /// before it closes. Below the cap, the session stays held so that ticks
    /// retry the call on its backoff, instead of the next full pass.
    fn backoff_at_cap(&self, namespace_id: &NamespaceId, call: SweepCall) -> bool {
        let first_backoff_ms = self.first_backoff_ms();
        lock(&self.inner.backoff)
            .get(&(namespace_id.clone(), call))
            .is_some_and(|backoff| {
                backoff_ms(first_backoff_ms, backoff.attempts) >= MAX_SWEEP_BACKOFF_MS
            })
    }

    /// The wait after a call's first failure: one tick interval.
    fn first_backoff_ms(&self) -> u64 {
        loonfs_types::saturating_duration_ms(self.inner.tick_interval)
    }

    /// Runs one call on a namespace, and contains a panic in it. The panic
    /// counts as a `server_error` failure of `call`, so the call backs off
    /// and is retried like any failed call, and the sweep goes on with its
    /// other calls and namespaces. Returns `None` after a panic.
    async fn contain<T>(
        &self,
        namespace_id: &NamespaceId,
        call: SweepCall,
        work: impl Future<Output = T>,
    ) -> Option<T> {
        let payload = match AssertUnwindSafe(work).catch_unwind().await {
            Ok(output) => return Some(output),
            Err(payload) => payload,
        };
        self.inner.metrics.panics.increment(1);
        let code = ErrorCode::ServerError;
        let backoff = self.back_off(namespace_id, call, code);
        tracing::error!(
            namespace_id = %namespace_id,
            call = call.as_str(),
            code = %code,
            attempts = backoff.attempts,
            backoff_ms = backoff.wait_ms,
            panic = panic_message(payload.as_ref()),
            "maintenance sweep call panicked; the sweep retries it after a backoff"
        );
        None
    }

    async fn gc(&self, namespace_id: &NamespaceId) -> bool {
        let inner = &self.inner;
        if inner.stop.is_cancelled() {
            return false;
        }
        let mut collected = true;
        if self.backed_off(namespace_id, SweepCall::Gc) {
            collected = false;
        } else {
            let call = SweepCall::Gc;
            match self
                .contain(namespace_id, call, inner.maintenance.gc(namespace_id))
                .await
            {
                Some(Ok(_)) => self.record_success(namespace_id, call),
                Some(Err(error)) => {
                    self.record_failure(namespace_id, call, error.code(), &error);
                    collected = false;
                }
                None => collected = false,
            }
        }
        let Some(grep) = &inner.grep else {
            return collected;
        };
        if inner.stop.is_cancelled() {
            return false;
        }
        if self.backed_off(namespace_id, SweepCall::GrepGc) {
            return false;
        }
        let call = SweepCall::GrepGc;
        let result = self.contain(
            namespace_id,
            call,
            grep.worker
                .gc_namespace(namespace_id, inner.maintenance.now_ms()),
        );
        match result.await {
            Some(Ok(_)) => self.record_success(namespace_id, call),
            Some(Err(error)) => {
                self.record_failure(namespace_id, call, error.code(), &error);
                collected = false;
            }
            None => collected = false,
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

    /// Counts a failed call and backs it off on that namespace. No code ends
    /// the sweep's maintenance of a namespace: every failure is retried once
    /// its backoff passes.
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
        let backoff = self.back_off(namespace_id, call, code);
        if backoff.attempts == 1 {
            tracing::error!(
                namespace_id = %namespace_id,
                call = call.as_str(),
                code = %code,
                attempts = backoff.attempts,
                backoff_ms = backoff.wait_ms,
                error = %error,
                "maintenance sweep call failed; the sweep retries it after a backoff"
            );
        } else {
            tracing::warn!(
                namespace_id = %namespace_id,
                call = call.as_str(),
                code = %code,
                attempts = backoff.attempts,
                backoff_ms = backoff.wait_ms,
                error = %error,
                "maintenance sweep call failed again; the sweep doubles its backoff"
            );
        }
    }

    /// Counts a failed call and backs it off on that namespace.
    fn back_off(&self, namespace_id: &NamespaceId, call: SweepCall, code: ErrorCode) -> Retry {
        let inner = &self.inner;
        inner.metrics.visit_failures[call as usize].increment(1);
        let now_ms = inner.namespaces.now_ms();
        let first_backoff_ms = self.first_backoff_ms();
        let (backoff, entries) = {
            let mut entries = lock(&inner.backoff);
            let key = (namespace_id.clone(), call);
            let attempts = entries
                .get(&key)
                .map_or(1, |backoff| backoff.attempts.saturating_add(1));
            if !entries.contains_key(&key) && entries.len() >= MAX_SWEEP_BACKOFF_ENTRIES {
                let oldest = entries
                    .iter()
                    .min_by_key(|(_, backoff)| backoff.next_at_ms)
                    .map(|(key, _)| key.clone());
                if let Some(oldest) = oldest {
                    entries.remove(&oldest);
                }
            }
            let backoff = Backoff {
                code,
                attempts,
                next_at_ms: now_ms.saturating_add(backoff_ms(first_backoff_ms, attempts)),
            };
            entries.insert(key, backoff);
            (backoff, entries.len())
        };
        inner
            .metrics
            .backoff_entries
            .set(i64::try_from(entries).unwrap_or(i64::MAX));
        Retry {
            attempts: backoff.attempts,
            wait_ms: backoff.next_at_ms.saturating_sub(now_ms),
        }
    }

    /// Clears the backoff of a call that succeeded, and logs the recovery.
    fn record_success(&self, namespace_id: &NamespaceId, call: SweepCall) {
        let inner = &self.inner;
        let (recovered, entries) = {
            let mut entries = lock(&inner.backoff);
            let recovered = entries.remove(&(namespace_id.clone(), call));
            (recovered, entries.len())
        };
        let Some(recovered) = recovered else {
            return;
        };
        inner
            .metrics
            .backoff_entries
            .set(i64::try_from(entries).unwrap_or(i64::MAX));
        tracing::info!(
            namespace_id = %namespace_id,
            call = call.as_str(),
            code = %recovered.code,
            attempts = recovered.attempts,
            "maintenance sweep call succeeded after failing; its backoff is cleared"
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

/// The wait after the `attempts`th failure in a row: `first_backoff_ms`,
/// doubled for each failure after the first, at most
/// [`MAX_SWEEP_BACKOFF_MS`].
fn backoff_ms(first_backoff_ms: u64, attempts: u32) -> u64 {
    let doublings = attempts.saturating_sub(1);
    1_u64
        .checked_shl(doublings)
        .map_or(u64::MAX, |factor| first_backoff_ms.saturating_mul(factor))
        .min(MAX_SWEEP_BACKOFF_MS)
}

/// The message a panic carried, when it carried a string.
fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("a panic payload that isn't a string")
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
    visit_failures: [Arc<dyn CounterHandle>; 4],
    panics: Arc<dyn CounterHandle>,
    backoff_entries: Arc<dyn GaugeHandle>,
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
            backoff_entries: recorder.register_gauge(
                "loonfs.maintenance.sweep_backoff_entries",
                "Namespace calls the maintenance sweep is backing off after a failure",
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
