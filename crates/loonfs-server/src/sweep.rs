//! The maintenance sweep: on a cadence, visits every namespace in the store
//! and runs the maintenance its durable state says is due.

use crate::config::{ServerConfig, ServerConfigError};
use futures::{FutureExt as _, StreamExt as _};
use loonfs::metrics::{
    CounterHandle, GaugeHandle, HistogramHandle, MetricsRecorder, RESULT_ERROR, RESULT_OK,
};
use loonfs::{
    ChangeSeq, ErrorCode, Maintenance, MaintenanceCancellation, MetadataMaintenanceOptions,
    NamespaceId, SharedObjectStore,
};
use loonfs_grep::{GramIndexBuildPolicy, GrepBuildOutcome, GrepError, GrepWorker};
use loonfs_http::Namespaces;
use loonfs_objectstore::layout::{list_namespace_ids, MAX_NAMESPACE_IDS_PAGE_LIMIT};
use loonfs_objectstore::timing::{MonotonicTimer, StdMonotonicTimer};
use loonfs_objectstore::ObjectStoreError;
use loonfs_types::EffectiveLimit;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, Sleep};

/// Grep build steps one visit runs while each step publishes.
const MAX_GREP_BUILD_STEPS_PER_VISIT: usize = 16;
/// Time between index passes over the writer sessions this process holds.
const INDEX_PASS_INTERVAL: Duration = Duration::from_secs(5);
const SWEEP_PASS_SECONDS_BOUNDARIES: &[f64] =
    &[1.0, 10.0, 60.0, 300.0, 900.0, 3_600.0, 14_400.0, 86_400.0];

/// Visits every namespace in the store on a cadence.
///
/// A pass lists the namespace ids in the store one page at a time and
/// visits up to `max_concurrent_maintenance` of them at once. A visit folds
/// an idle WAL tail and compacts metadata while compaction is due, and runs
/// grep build steps when this server maintains the grep index. Every
/// `gc_interval_ms`, a pass also collects garbage, grep's included. A failed
/// call is logged and counted, and the next pass tries it again. A failed
/// listing ends the pass, and the next pass lists again. The sweep keeps no
/// state about a namespace between passes.
///
/// When this server maintains the grep index, a second pass runs every five
/// seconds over the writer sessions this process holds. It indexes a
/// session only when the session's last published seq moved since that pass
/// last indexed it, so an idle session costs no store request.
///
/// [`app`](crate::app) builds the sweep without starting it, and
/// [`serve`](crate::serve) starts it. A host can run passes itself with
/// [`Self::run_pass`] and [`Self::run_index_pass`].
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
    interval: Duration,
    collection_interval: Duration,
    page_limit: EffectiveLimit,
    stop: MaintenanceCancellation,
    timer: StdMonotonicTimer,
    metrics: SweepMetrics,
}

struct GrepIndexing {
    worker: GrepWorker<SharedObjectStore>,
    policy: GramIndexBuildPolicy,
    /// Namespaces whose index a visit or an index pass is building now.
    building: Mutex<HashSet<NamespaceId>>,
    /// For each held session, the published seq an index pass last brought
    /// the index up to.
    indexed: Mutex<HashMap<NamespaceId, ChangeSeq>>,
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
                Ok::<_, ServerConfigError>(GrepIndexing {
                    worker,
                    policy,
                    building: Mutex::default(),
                    indexed: Mutex::default(),
                })
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
                interval: Duration::from_millis(config.maintenance_interval_ms),
                collection_interval: Duration::from_millis(config.gc_interval_ms),
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
    /// that ended it. A failed call on one namespace does not fail the pass.
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

    /// Builds the grep index of each writer session this process holds
    /// whose last published seq moved since this pass last indexed it, one
    /// session at a time.
    ///
    /// Does nothing on a server that does not maintain the grep index. A
    /// session that has not moved costs no store request. A namespace whose
    /// index a sweep visit is building is left to that visit.
    pub async fn run_index_pass(&self) {
        let inner = &self.inner;
        let Some(grep) = &inner.grep else {
            return;
        };
        let held: HashMap<NamespaceId, ChangeSeq> = inner
            .namespaces
            .held()
            .iter()
            .filter_map(|namespace| {
                let seq = namespace.last_published_seq()?;
                Some((namespace.id().clone(), seq))
            })
            .collect();
        let moved: Vec<(NamespaceId, ChangeSeq)> = {
            let mut indexed = lock(&grep.indexed);
            indexed.retain(|namespace_id, _| held.contains_key(namespace_id));
            held.into_iter()
                .filter(|(namespace_id, seq)| indexed.get(namespace_id) != Some(seq))
                .collect()
        };
        for (namespace_id, seq) in moved {
            if inner.stop.is_cancelled() {
                return;
            }
            let Some(_building) = grep.claim(&namespace_id) else {
                continue;
            };
            // Boxed for the same reason as a sweep visit.
            let caught_up = match self.build_index(grep, &namespace_id).boxed().await {
                Ok(caught_up) => caught_up,
                Err(error) => {
                    self.record_failure(&namespace_id, SweepCall::GrepIndex, error.code(), &error);
                    // A failed build waits for the session's next commit or the
                    // next sweep pass instead of retrying every few seconds.
                    true
                }
            };
            if caught_up {
                lock(&grep.indexed).insert(namespace_id, seq);
            }
        }
    }

    /// Starts the pass loop and, when this server maintains the grep index,
    /// the index pass loop. The first pass starts at once and collects
    /// garbage.
    pub(crate) fn start(&self) -> RunningSweep {
        let inner = &self.inner;
        tracing::info!(
            maintenance_interval_ms = u64::try_from(inner.interval.as_millis()).unwrap_or(u64::MAX),
            gc_interval_ms =
                u64::try_from(inner.collection_interval.as_millis()).unwrap_or(u64::MAX),
            max_concurrent_maintenance = inner.max_concurrent_visits,
            maintains_grep_index = inner.grep.is_some(),
            "maintenance sweep started"
        );
        let sweep = self.clone();
        RunningSweep {
            stop: inner.stop.clone(),
            task: tokio::spawn(async move {
                tokio::join!(sweep.run_passes(), sweep.run_index_passes());
            }),
        }
    }

    async fn run_passes(&self) {
        let inner = &self.inner;
        let mut ticks = tokio::time::interval(inner.interval);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_collection = None;
        loop {
            let started = tokio::select! {
                biased;
                () = inner.stop.cancelled() => return,
                started = ticks.tick() => started,
            };
            let collect_garbage = last_collection
                .is_none_or(|at| started.duration_since(at) >= inner.collection_interval);
            if self.run_pass(collect_garbage).await.is_ok() && collect_garbage {
                last_collection = Some(started);
            }
        }
    }

    async fn run_index_passes(&self) {
        let inner = &self.inner;
        if inner.grep.is_none() {
            return;
        }
        let mut ticks = tokio::time::interval(INDEX_PASS_INTERVAL);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = inner.stop.cancelled() => return,
                _ = ticks.tick() => {}
            }
            self.run_index_pass().await;
        }
    }

    async fn visit_every_namespace(
        &self,
        collect_garbage: bool,
    ) -> Result<usize, ObjectStoreError> {
        let inner = &self.inner;
        let mut start_after = None;
        let mut listed = 0;
        while !inner.stop.is_cancelled() {
            let page =
                list_namespace_ids(&*inner.store, start_after.as_ref(), inner.page_limit).await?;
            listed += page.namespace_ids.len();
            futures::stream::iter(page.namespace_ids)
                .take_until(inner.stop.cancelled())
                .for_each_concurrent(inner.max_concurrent_visits, |namespace_id| {
                    // Boxed so the spawned sweep's `Send` check does not
                    // recurse through every engine future a visit awaits.
                    self.visit(namespace_id, collect_garbage).boxed()
                })
                .await;
            match page.next_cursor {
                Some(next) => start_after = Some(next),
                None => break,
            }
        }
        Ok(listed)
    }

    async fn visit(&self, namespace_id: NamespaceId, collect_garbage: bool) {
        let inner = &self.inner;
        let stop = &inner.stop;
        if let Err(error) = inner
            .maintenance
            .maintain_metadata_while_due_with_options(&namespace_id, stop, &inner.metadata)
            .await
        {
            self.record_failure(&namespace_id, SweepCall::Metadata, error.code(), &error);
        }
        if let Some(grep) = &inner.grep {
            if let Some(_building) = grep.claim(&namespace_id) {
                if let Err(error) = self.build_index(grep, &namespace_id).await {
                    self.record_failure(&namespace_id, SweepCall::GrepIndex, error.code(), &error);
                }
            }
        }
        if !collect_garbage || stop.is_cancelled() {
            return;
        }
        if let Err(error) = inner.maintenance.gc(&namespace_id).await {
            self.record_failure(&namespace_id, SweepCall::Gc, error.code(), &error);
        }
        let Some(grep) = &inner.grep else {
            return;
        };
        if stop.is_cancelled() {
            return;
        }
        let collected = match inner.maintenance.now_ms() {
            Ok(now_ms) => grep
                .worker
                .garbage_collect_namespace(&namespace_id, now_ms)
                .await
                .map(drop),
            Err(error) => Err(GrepError::from(error)),
        };
        if let Err(error) = collected {
            self.record_failure(&namespace_id, SweepCall::GrepGc, error.code(), &error);
        }
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
                    grep.worker
                        .reorganize_step(namespace_id, grep.policy)
                        .await?;
                    return Ok(true);
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

impl GrepIndexing {
    fn claim(&self, namespace_id: &NamespaceId) -> Option<Building<'_>> {
        lock(&self.building)
            .insert(namespace_id.clone())
            .then(|| Building {
                grep: self,
                namespace_id: namespace_id.clone(),
            })
    }
}

/// A namespace whose index one visit or index pass is building. The claim
/// ends when this drops.
struct Building<'grep> {
    grep: &'grep GrepIndexing,
    namespace_id: NamespaceId,
}

impl Drop for Building<'_> {
    fn drop(&mut self) {
        lock(&self.grep.building).remove(&self.namespace_id);
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
