//! Fair all-namespaces scheduling for [`crate::GrepWorker`] steps and GC.

use crate::keyspace::{all_namespaces_prefix, parse_key, GrepKeyKind};
use crate::{GrepGcReport, GrepWorker, GrepWorkerConfig};
use loonfs_api::NamespaceId;
use loonfs_core::{Error as CoreError, Result, StoreFailureClass};
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::Notify;

const MAX_NAMESPACE_BACKOFF_SWEEPS: u64 = 64;

/// Counts from one complete build/fold sweep and optional GC pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GrepSweepReport {
    /// Exact grep roots discovered under the all-namespaces prefix.
    pub namespaces_seen: u64,
    /// Namespaces whose build and fold steps both completed.
    pub namespaces_completed: u64,
    /// Namespaces whose build or fold step failed.
    pub namespace_failures: u64,
    /// Persistently failing namespaces skipped until their retry sweep.
    pub namespaces_backed_off: u64,
    /// Grep garbage-collection counts when this sweep included GC.
    pub garbage_collection: Option<GrepGcReport>,
}

/// Failure that prevents a standalone one-shot from completing its sweep.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum GrepWorkerRunOnceError {
    /// The worker could not enumerate grep roots or run grep GC.
    #[error("grep worker sweep failed: {0}")]
    Sweep(#[source] CoreError),
    /// Individual namespaces failed, after the sweep still serviced peers.
    #[error("grep worker sweep completed with {failures} namespace failure(s)")]
    NamespaceFailures { failures: u64 },
}

#[derive(Debug)]
struct ShutdownState {
    requested: AtomicBool,
    notify: Notify,
}

/// Cloneable stop handle for a running [`GrepWorkerLoop`].
#[derive(Debug, Clone)]
pub struct GrepWorkerLoopShutdown {
    state: Arc<ShutdownState>,
}

impl GrepWorkerLoopShutdown {
    /// Requests a clean stop between bounded worker steps.
    pub fn request_shutdown(&self) {
        self.state.requested.store(true, Ordering::Release);
        self.state.notify.notify_waiters();
    }

    fn requested(&self) -> bool {
        self.state.requested.load(Ordering::Acquire)
    }

    async fn wait_or_timeout(&self, deadline: tokio::time::Instant) {
        let notified = self.state.notify.notified();
        if self.requested() {
            return;
        }
        tokio::select! {
            () = notified => {}
            () = tokio::time::sleep_until(deadline) => {}
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct NamespaceFailure {
    consecutive_failures: u32,
    retry_at_sweep: u64,
}

/// Shared grep worker driving loop used by embedded and standalone hosts.
#[derive(Debug)]
pub struct GrepWorkerLoop<S> {
    worker: GrepWorker<S>,
    store: S,
    config: GrepWorkerConfig,
    shutdown: GrepWorkerLoopShutdown,
    sweep_number: u64,
    namespace_failures: BTreeMap<NamespaceId, NamespaceFailure>,
}

impl<S: ObjectStore + Clone> GrepWorkerLoop<S> {
    /// Builds a loop around one worker and the same store it operates on.
    pub fn new(worker: GrepWorker<S>, store: S, config: GrepWorkerConfig) -> Self {
        Self {
            worker,
            store,
            config,
            shutdown: GrepWorkerLoopShutdown {
                state: Arc::new(ShutdownState {
                    requested: AtomicBool::new(false),
                    notify: Notify::new(),
                }),
            },
            sweep_number: 0,
            namespace_failures: BTreeMap::new(),
        }
    }

    /// Returns a handle that cleanly stops [`Self::run`] between steps.
    pub fn shutdown_handle(&self) -> GrepWorkerLoopShutdown {
        self.shutdown.clone()
    }

    /// Runs exactly one full build/fold sweep and one grep GC pass.
    pub async fn run_once(
        &mut self,
    ) -> std::result::Result<GrepSweepReport, GrepWorkerRunOnceError> {
        let mut report = self
            .sweep(false)
            .await
            .map_err(GrepWorkerRunOnceError::Sweep)?;
        report.garbage_collection = Some(
            self.worker
                .garbage_collect(current_time_ms().map_err(GrepWorkerRunOnceError::Sweep)?)
                .await
                .map_err(GrepWorkerRunOnceError::Sweep)?,
        );
        if report.namespace_failures > 0 {
            return Err(GrepWorkerRunOnceError::NamespaceFailures {
                failures: report.namespace_failures,
            });
        }
        Ok(report)
    }

    /// Runs periodic sweeps until the matching shutdown handle is signaled.
    ///
    /// Namespace and store errors are logged and retried; only a task panic
    /// can escape to the host's task join. A shutdown requested during IO is
    /// observed after that bounded build, fold, or GC step finishes.
    pub async fn run(mut self) {
        let step_interval = Duration::from_millis(self.config.step_interval_ms);
        let gc_interval = Duration::from_millis(self.config.gc_interval_ms);
        let mut next_gc = tokio::time::Instant::now();
        while !self.shutdown.requested() {
            if let Err(error) = self.sweep(true).await {
                tracing::warn!(
                    phase = "grep_worker_sweep",
                    result = "error",
                    error = %error,
                    "grep worker could not enumerate namespaces; retrying"
                );
            }
            if self.shutdown.requested() {
                break;
            }
            let now = tokio::time::Instant::now();
            if now >= next_gc {
                match current_time_ms() {
                    Ok(now_ms) => {
                        if let Err(error) = self.worker.garbage_collect(now_ms).await {
                            tracing::warn!(
                                phase = "grep_gc",
                                result = "error",
                                error = %error,
                                "grep garbage collection failed; retrying"
                            );
                        }
                    }
                    Err(error) => tracing::warn!(
                        phase = "grep_gc",
                        result = "error",
                        error = %error,
                        "grep garbage collection clock failed; retrying"
                    ),
                }
                next_gc = tokio::time::Instant::now() + gc_interval;
            }
            self.shutdown
                .wait_or_timeout(tokio::time::Instant::now() + step_interval)
                .await;
        }
    }

    async fn sweep(&mut self, honor_shutdown: bool) -> Result<GrepSweepReport> {
        self.sweep_number = self.sweep_number.saturating_add(1);
        let namespace_ids = self.namespace_ids().await?;
        let discovered: BTreeSet<NamespaceId> = namespace_ids.iter().cloned().collect();
        self.namespace_failures
            .retain(|namespace_id, _| discovered.contains(namespace_id));
        let mut report = GrepSweepReport {
            namespaces_seen: namespace_ids.len() as u64,
            ..GrepSweepReport::default()
        };
        for namespace_id in namespace_ids {
            if honor_shutdown && self.shutdown.requested() {
                break;
            }
            if self.namespace_is_backed_off(&namespace_id) {
                report.namespaces_backed_off += 1;
                continue;
            }
            if let Err(error) = self
                .worker
                .build_step(&namespace_id, self.config.build_policy())
                .await
            {
                report.namespace_failures += 1;
                self.record_namespace_failure(&namespace_id, "grep_build", &error);
                continue;
            }
            if honor_shutdown && self.shutdown.requested() {
                break;
            }
            if let Err(error) = self
                .worker
                .fold_step(&namespace_id, self.config.build_policy())
                .await
            {
                report.namespace_failures += 1;
                self.record_namespace_failure(&namespace_id, "grep_fold", &error);
                continue;
            }
            self.namespace_failures.remove(&namespace_id);
            report.namespaces_completed += 1;
        }
        Ok(report)
    }

    async fn namespace_ids(&self) -> Result<Vec<NamespaceId>> {
        let keys = self
            .store
            .list_prefix(all_namespaces_prefix())
            .await
            .map_err(|error| CoreError::Store {
                object_key: all_namespaces_prefix().to_owned(),
                message: error.message(),
                class: StoreFailureClass::of(&error),
            })?;
        Ok(keys
            .into_iter()
            .filter_map(|key| {
                let parsed = parse_key(&key)?;
                matches!(parsed.kind, GrepKeyKind::Root).then_some(parsed.namespace_id)
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }

    fn namespace_is_backed_off(&self, namespace_id: &NamespaceId) -> bool {
        self.namespace_failures
            .get(namespace_id)
            .is_some_and(|failure| self.sweep_number < failure.retry_at_sweep)
    }

    fn record_namespace_failure(
        &mut self,
        namespace_id: &NamespaceId,
        phase: &'static str,
        error: &CoreError,
    ) {
        let consecutive_failures = self
            .namespace_failures
            .get(namespace_id)
            .map_or(1, |failure| failure.consecutive_failures.saturating_add(1));
        let backoff_sweeps =
            (1u64 << consecutive_failures.min(6)).min(MAX_NAMESPACE_BACKOFF_SWEEPS);
        self.namespace_failures.insert(
            namespace_id.clone(),
            NamespaceFailure {
                consecutive_failures,
                retry_at_sweep: self.sweep_number.saturating_add(backoff_sweeps),
            },
        );
        tracing::warn!(
            namespace_id = %namespace_id,
            phase,
            result = "error",
            error = %error,
            consecutive_failures,
            backoff_sweeps,
            "grep worker namespace step failed; continuing the sweep"
        );
    }
}

#[allow(clippy::disallowed_methods)]
fn current_time_ms() -> Result<u64> {
    // Grep GC age checks enter wall time at the worker-loop boundary; durable replay stays deterministic.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .map_err(|error| CoreError::Internal(format!("system clock before unix epoch: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use loonfs_api::{AbsolutePath, CommitId, DestinationBehavior};
    use loonfs_core::content::{prepare_stored_content, store_bytes_as_content};
    use loonfs_core::publish::{NamespaceMutationCandidate, PathMutationIntent};
    use loonfs_core::{BootstrapOptions, NamespaceEngine};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;
    use tempfile::tempdir;

    #[tokio::test]
    async fn poisoned_namespace_backs_off_while_later_namespaces_keep_indexing() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let poisoned = NamespaceId::parse("a-poisoned").expect("namespace id");
        let healthy = NamespaceId::parse("z-healthy").expect("namespace id");
        let _poisoned_engine = bootstrap(store.clone(), &poisoned).await;
        let healthy_engine = bootstrap(store.clone(), &healthy).await;
        put_file(&store, &healthy_engine, &healthy, b"healthy needle").await;

        let worker = GrepWorker::new(
            store.clone(),
            "loop-test-worker",
            "loop-test-session",
            "loop-test/0.1",
        );
        worker.enable(&poisoned).await.expect("enable poisoned");
        worker.enable(&healthy).await.expect("enable healthy");
        store
            .put_overwrite(
                &crate::keyspace::root_key(&poisoned),
                Bytes::from_static(b"poison"),
            )
            .await
            .expect("poison root");

        let mut worker_loop =
            GrepWorkerLoop::new(worker, store.clone(), GrepWorkerConfig::default());
        assert!(matches!(
            worker_loop.run_once().await,
            Err(GrepWorkerRunOnceError::NamespaceFailures { failures: 1 })
        ));
        let healthy_root = crate::root::load_grep_root(&*store, &healthy)
            .await
            .expect("load healthy root")
            .expect("healthy root exists");
        assert_eq!(healthy_root.state().index().built_through_seq.0, 1);
        assert!(matches!(
            healthy_root.state().lifecycle(),
            crate::root::GrepLifecycle::Steady
        ));
        assert!(!healthy_root.state().segments().is_empty());

        let report = worker_loop
            .run_once()
            .await
            .expect("backed-off poison does not fail the next sweep");
        assert_eq!(report.namespaces_backed_off, 1);
        assert_eq!(report.namespaces_completed, 1);
    }

    async fn bootstrap(
        store: Arc<LocalFsStore>,
        namespace_id: &NamespaceId,
    ) -> NamespaceEngine<Arc<LocalFsStore>> {
        let engine = NamespaceEngine::builder(store)
            .namespace_id(namespace_id.clone())
            .writer_id(format!("seed-{namespace_id}"))
            .writer_session_id(format!("seed-{namespace_id}-session"))
            .writer_version("worker-loop-test/0.1")
            .build()
            .expect("engine");
        engine
            .bootstrap_namespace(BootstrapOptions::default())
            .await
            .expect("bootstrap namespace");
        engine
    }

    async fn put_file(
        store: &Arc<LocalFsStore>,
        engine: &NamespaceEngine<Arc<LocalFsStore>>,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) {
        let stored = store_bytes_as_content(&**store, namespace_id, bytes)
            .await
            .expect("store content");
        let content_ref = stored.content_ref.clone();
        let prepared = prepare_stored_content(namespace_id.clone(), stored);
        let result = engine
            .publish_namespace_mutations_batch(vec![NamespaceMutationCandidate::path_prepared(
                PathMutationIntent::PutFile {
                    commit_id: CommitId::parse("worker-loop-put").expect("commit id"),
                    absolute_path: AbsolutePath::parse("/healthy.txt").expect("path"),
                    content_ref,
                    behavior: DestinationBehavior::NoReplace,
                },
                vec![prepared],
            )])
            .await
            .pop()
            .expect("one result");
        result.expect("publish file");
    }
}
