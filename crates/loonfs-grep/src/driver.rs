//! Event-driven maintenance for one grep-enabled namespace.

use crate::{GramIndexBuildPolicy, GrepBuildOutcome, GrepError, GrepFoldOutcome, GrepWorker};
use loonfs_api::{ChangeSeq, ErrorCode, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinError, JoinHandle};

const ERROR_BACKOFF_BASE_MS: u64 = 10;
const ERROR_BACKOFF_CAP_MS: u64 = 1_000;

/// Shared cap on concurrently executing grep build or fold steps.
///
/// Tokio's semaphore admits queued drivers in FIFO order, so a namespace
/// waiting behind busy siblings cannot be starved by later arrivals.
#[derive(Debug, Clone)]
pub struct GrepStepLimiter {
    permits: Arc<Semaphore>,
}

impl GrepStepLimiter {
    /// Creates a shared limiter from a validated concurrency limit.
    pub fn new(max_concurrent_steps: NonZeroUsize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max_concurrent_steps.get())),
        }
    }

    async fn acquire(&self) -> OwnedSemaphorePermit {
        self.permits
            .clone()
            .acquire_owned()
            .await
            .expect("grep step semaphore should remain open")
    }
}

/// Why a per-namespace driver has no immediate work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrepDriverParked {
    /// The root is steady and caught up to this namespace sequence.
    CaughtUp { built_through_seq: ChangeSeq },
    /// The namespace has no active grep root.
    NotEnabled,
}

/// Observable state of one per-namespace driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrepDriverState {
    /// The driver is running bounded build and fold steps.
    Active,
    /// A failed step is waiting before its namespace-local retry.
    BackingOff {
        /// Consecutive failed attempts since the last successful step.
        consecutive_failures: u32,
        /// Current one-shot retry delay, capped at one second.
        delay_ms: u64,
    },
    /// The driver is waiting for a nudge.
    Parked(GrepDriverParked),
    /// Stop was requested and the last bounded step has finished.
    Stopped,
}

#[derive(Debug)]
struct DriverControl {
    stopping: AtomicBool,
    requested_runs: AtomicU64,
    settled_runs: AtomicU64,
    work: mpsc::Sender<()>,
    stop_notify: Notify,
    state: watch::Sender<GrepDriverState>,
}

/// Cloneable, non-blocking control handle for one grep driver.
#[derive(Debug, Clone)]
pub struct GrepDriverHandle {
    control: Arc<DriverControl>,
}

impl GrepDriverHandle {
    /// Requests a catch-up run. Multiple requests coalesce into one pending
    /// run, including requests received while the driver is active.
    pub fn nudge(&self) {
        self.control.requested_runs.fetch_add(1, Ordering::AcqRel);
        let _ = self.control.work.try_send(());
    }

    /// Requests a clean stop after the current bounded step.
    pub fn request_stop(&self) {
        self.control.stopping.store(true, Ordering::Release);
        self.control.stop_notify.notify_waiters();
    }

    /// Returns the driver's latest observable state.
    pub fn state(&self) -> GrepDriverState {
        *self.control.state.borrow()
    }

    /// Subscribes to state transitions without polling or sleeping.
    pub fn subscribe_state(&self) -> watch::Receiver<GrepDriverState> {
        self.control.state.subscribe()
    }

    /// Waits without polling until the driver parks or stops.
    pub async fn wait_for_quiescence(&self) -> Option<GrepDriverParked> {
        let requested_runs = self.control.requested_runs.load(Ordering::Acquire);
        let mut state = self.control.state.subscribe();
        loop {
            match *state.borrow_and_update() {
                GrepDriverState::Parked(parked)
                    if self.control.settled_runs.load(Ordering::Acquire) >= requested_runs =>
                {
                    return Some(parked);
                }
                GrepDriverState::Parked(_) => {}
                GrepDriverState::Stopped => return None,
                GrepDriverState::Active | GrepDriverState::BackingOff { .. } => {}
            }
            if state.changed().await.is_err() {
                return None;
            }
        }
    }
}

/// The task and control handle for a spawned per-namespace driver.
#[derive(Debug)]
pub struct GrepDriverTask {
    handle: GrepDriverHandle,
    task: JoinHandle<()>,
}

impl GrepDriverTask {
    /// Returns a non-blocking control handle.
    pub fn handle(&self) -> GrepDriverHandle {
        self.handle.clone()
    }

    /// Whether the driver task has already exited.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    /// Stops the driver between bounded steps and joins its task.
    pub async fn shutdown(self) -> Result<(), JoinError> {
        self.handle.request_stop();
        self.task.await
    }
}

/// Event-driven owner of one namespace's [`GrepWorker`] steps.
#[derive(Debug)]
pub struct GrepDriver<S> {
    worker: GrepWorker<S>,
    namespace_id: NamespaceId,
    policy: GramIndexBuildPolicy,
    step_limiter: GrepStepLimiter,
    handle: GrepDriverHandle,
    work: mpsc::Receiver<()>,
}

impl<S: ObjectStore + Clone + Send + Sync + 'static> GrepDriver<S> {
    /// Creates a driver ready to perform its initial catch-up run.
    pub fn new(
        worker: GrepWorker<S>,
        namespace_id: NamespaceId,
        policy: GramIndexBuildPolicy,
        step_limiter: GrepStepLimiter,
    ) -> Self {
        let (state, _) = watch::channel(GrepDriverState::Active);
        let (work_send, work) = mpsc::channel(1);
        work_send
            .try_send(())
            .expect("fresh driver work channel should accept its initial run");
        Self {
            worker,
            namespace_id,
            policy,
            step_limiter,
            handle: GrepDriverHandle {
                control: Arc::new(DriverControl {
                    stopping: AtomicBool::new(false),
                    requested_runs: AtomicU64::new(1),
                    settled_runs: AtomicU64::new(0),
                    work: work_send,
                    stop_notify: Notify::new(),
                    state,
                }),
            },
            work,
        }
    }

    /// Spawns the driver on its host's explicitly owned Tokio runtime.
    pub fn spawn_on(self, runtime: &tokio::runtime::Handle) -> GrepDriverTask {
        let handle = self.handle.clone();
        let task = runtime.spawn(self.run());
        GrepDriverTask { handle, task }
    }

    async fn run(mut self) {
        while self.wait_for_work().await {
            let requested_runs = self.handle.control.requested_runs.load(Ordering::Acquire);
            self.set_state(GrepDriverState::Active);
            let Some(parked) = self.catch_up().await else {
                break;
            };
            self.handle
                .control
                .settled_runs
                .store(requested_runs, Ordering::Release);
            self.set_state(GrepDriverState::Parked(parked));
        }
        self.set_state(GrepDriverState::Stopped);
    }

    async fn wait_for_work(&mut self) -> bool {
        if self.stopping() {
            return false;
        }
        tokio::select! {
            work = self.work.recv() => work.is_some() && !self.stopping(),
            () = self.handle.control.stop_notify.notified() => false,
        }
    }

    async fn catch_up(&self) -> Option<GrepDriverParked> {
        let mut consecutive_failures = 0_u32;
        loop {
            if self.stopping() {
                return None;
            }
            let step_permit = self.acquire_step_permit().await?;
            let build_result = self
                .worker
                .build_step(&self.namespace_id, self.policy)
                .await;
            drop(step_permit);
            let build = match build_result {
                Ok(report) => report.outcome,
                Err(error)
                    if matches!(
                        error.code(),
                        ErrorCode::NamespaceDeleted | ErrorCode::NamespaceNotFound
                    ) =>
                {
                    return Some(GrepDriverParked::NotEnabled);
                }
                Err(error) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if !self
                        .back_off(consecutive_failures, "grep_build", &error)
                        .await
                    {
                        return None;
                    }
                    continue;
                }
            };
            if matches!(build, GrepBuildOutcome::NotEnabled) {
                return Some(GrepDriverParked::NotEnabled);
            }

            let step_permit = self.acquire_step_permit().await?;
            let fold_result = self.worker.fold_step(&self.namespace_id, self.policy).await;
            drop(step_permit);
            let fold = match fold_result {
                Ok(report) => report.outcome,
                Err(error) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if !self
                        .back_off(consecutive_failures, "grep_fold", &error)
                        .await
                    {
                        return None;
                    }
                    continue;
                }
            };
            consecutive_failures = 0;

            if let GrepBuildOutcome::UpToDate { built_through_seq } = build {
                if matches!(fold, GrepFoldOutcome::NotNeeded { .. }) {
                    return Some(GrepDriverParked::CaughtUp { built_through_seq });
                }
            }
            if matches!(fold, GrepFoldOutcome::NotEnabled) {
                return Some(GrepDriverParked::NotEnabled);
            }
            tokio::task::yield_now().await;
        }
    }

    async fn back_off(
        &self,
        consecutive_failures: u32,
        phase: &'static str,
        error: &GrepError,
    ) -> bool {
        let shift = consecutive_failures.saturating_sub(1).min(16);
        let delay_ms = ERROR_BACKOFF_BASE_MS
            .saturating_mul(1_u64 << shift)
            .min(ERROR_BACKOFF_CAP_MS);
        self.set_state(GrepDriverState::BackingOff {
            consecutive_failures,
            delay_ms,
        });
        tracing::warn!(
            namespace_id = %self.namespace_id,
            phase,
            result = "error",
            error = %error,
            consecutive_failures,
            delay_ms,
            "grep namespace driver step failed; backing off"
        );
        self.wait_for_error_backoff(Duration::from_millis(delay_ms))
            .await
    }

    #[allow(clippy::disallowed_methods)]
    async fn wait_for_error_backoff(&self, delay: Duration) -> bool {
        // Namespace-local error backoff enters a one-shot timer at this driver
        // scheduling boundary so durable replay stays deterministic.
        tokio::select! {
            () = tokio::time::sleep(delay) => !self.stopping(),
            () = self.handle.control.stop_notify.notified() => false,
        }
    }

    fn stopping(&self) -> bool {
        self.handle.control.stopping.load(Ordering::Acquire)
    }

    async fn acquire_step_permit(&self) -> Option<OwnedSemaphorePermit> {
        if self.stopping() {
            return None;
        }
        tokio::select! {
            permit = self.step_limiter.acquire() => Some(permit),
            () = self.handle.control.stop_notify.notified() => None,
        }
    }

    fn set_state(&self, state: GrepDriverState) {
        self.handle.control.state.send_replace(state);
    }
}
