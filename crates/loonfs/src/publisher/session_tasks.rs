//! The tasks that a writer session runs beside its queue: the worker, the
//! WAL-tail fold, and the metadata compaction that a fold starts, and the
//! waits for them.

use super::{FoldWhen, NamespaceFold, NamespacePublisher, NamespacePublisherState};
use crate::trace::phase_event;
use crate::{Error, FoldWalOutcome, Result};
use futures::FutureExt;
use loonfs_core::Phase;
use loonfs_types::panic_message;
use std::panic::AssertUnwindSafe;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

/// The worker that drains a session's queue.
pub(super) struct WorkerHandle {
    _task: JoinHandle<()>,
    pub(super) liveness: watch::Receiver<bool>,
}

impl WorkerHandle {
    pub(super) fn is_running(&self) -> bool {
        !*self.liveness.borrow()
    }
}

/// A fold or a compaction that a session runs beside its queue.
pub(super) struct SessionTask {
    id: u64,
    task: JoinHandle<()>,
    pub(super) liveness: watch::Receiver<bool>,
}

impl SessionTask {
    pub(super) fn is_running(&self) -> bool {
        !*self.liveness.borrow()
    }
}

/// Marks a session task as exited when the task ends, even when it panics.
struct TaskExit(watch::Sender<bool>);
impl Drop for TaskExit {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

/// Waits until the task that `liveness` reports on exits.
pub(super) async fn wait_for_exit(mut liveness: watch::Receiver<bool>) {
    let _ = liveness.wait_for(|exited| *exited).await;
}

impl NamespacePublisher {
    /// Runs one unit of the session's work, and returns `None` if the unit
    /// panics.
    ///
    /// LoonFS records the panic (see [`Self::record_panic`]) and keeps the
    /// session open, because a panic is a bug that a new session might hit
    /// again. Then LoonFS drops the session's view of the namespace (see
    /// [`Self::drop_view_after_panic`]). For a unit that panicked with the
    /// engine locked, the engine's release already did that, so this call
    /// covers a fold or a compaction, which puts manifests without the
    /// engine locked. The worker runs each unit it takes from the queue
    /// inside this function, and [`Self::spawn_session_task`] runs each fold
    /// and compaction inside it. The worker, not this function, returns
    /// `outcome_unknown` to the callers of a unit that panicked.
    pub(super) async fn contain_panic<T>(
        &self,
        unit: &'static str,
        work: impl std::future::Future<Output = T>,
    ) -> Option<T> {
        match AssertUnwindSafe(work).catch_unwind().await {
            Ok(output) => Some(output),
            Err(payload) => {
                self.record_panic(unit, panic_message(payload.as_ref()));
                drop(payload);
                self.drop_view_after_panic(&mut *self.lock_engine().await);
                None
            }
        }
    }

    /// Starts a fold task after its triggering publication releases the engine.
    /// A panicked fold is retried by the next publish over the threshold. A
    /// fold that publishes starts the session's compaction after it releases
    /// its fold permit, and the task folds again while the tail that the fold
    /// left is at the trigger.
    pub(super) fn start_fold(&self) -> Option<oneshot::Sender<()>> {
        let mut state = self.lock_state();
        let _session = self.live_session().ok()?;
        if state
            .fold
            .as_ref()
            .is_some_and(|fold| !fold.task.is_finished())
        {
            return None;
        }
        let (start, started) = oneshot::channel();
        let publisher = self.clone();
        let fold = self.spawn_session_task(&mut state, "fold", async move {
            if started.await.is_err() {
                return;
            }
            // A fold frees the tail up to where it began, and a publish while
            // it runs starts no fold, so the task checks the trigger again
            // after each fold that published. Each fold after the first folds
            // commits published during the fold before it.
            while publisher.run_fold().await {
                publisher.start_compaction();
            }
        });
        state.fold = Some(fold);
        Some(start)
    }

    /// Starts the session's metadata compaction unless one is running or the
    /// session no longer admits work. A panicked compaction may run again
    /// after the next fold.
    pub(super) fn start_compaction(&self) {
        let mut state = self.lock_state();
        let Ok(_session) = self.admitted_session(&state) else {
            return;
        };
        if state
            .compaction
            .as_ref()
            .is_some_and(SessionTask::is_running)
        {
            return;
        }
        let publisher = self.clone();
        let compaction = self.spawn_session_task(&mut state, "compaction", async move {
            publisher.run_compaction().await;
        });
        state.compaction = Some(compaction);
    }

    /// Runs compaction units over this session's namespace while compaction
    /// is due, through the namespace's one compaction path (see
    /// [`Self::compact_namespace`]).
    async fn run_compaction(&self) {
        if self.context.writer.upgrade().is_none() {
            return;
        }
        let result = Box::pin(self.compact_namespace(None, &self.stop)).await;
        if let Err(error) = result {
            phase_event!(
                self.context.runtime_core,
                Phase::MetadataCompaction,
                self.namespace_id,
                tracing::Level::WARN,
                error = %error.public_message(),
                "session metadata compaction failed; the next fold starts it again"
            );
        }
    }

    /// Folds the tail through the namespace's one fold path (see
    /// [`Self::fold`]) when it's at the fold trigger. Returns whether the
    /// fold published a manifest.
    async fn run_fold(&self) -> bool {
        let Some(_writer) = self.context.writer.upgrade() else {
            return false;
        };
        let when = FoldWhen::Due {
            max_wal_tail_objects: self
                .context
                .runtime_core
                .session_fold_at_wal_objects()
                .get(),
        };
        // The session's own fold isn't stopped by its close: the commits
        // that the close drains can wait at the write stop for it.
        match self
            .fold(when, &loonfs_core::MaintenanceCancellation::default())
            .await
        {
            Ok(NamespaceFold::Folded(folded)) => {
                let published = folded.response.outcome == FoldWalOutcome::Published;
                if published {
                    self.context.runtime_core.instruments().publisher.wal_fold();
                }
                published
            }
            Ok(NamespaceFold::NotDue { .. }) => false,
            Err(error) => {
                phase_event!(
                    self.context.runtime_core,
                    Phase::WalFold,
                    self.namespace_id,
                    tracing::Level::WARN,
                    error = %error.public_message(),
                    "WAL-tail fold failed"
                );
                false
            }
        }
    }

    /// Spawns `work` as the session's worker, whose exit the drain and close
    /// can wait for.
    pub(super) fn spawn_worker(
        &self,
        work: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> WorkerHandle {
        let (task, liveness) = self.spawn_tracked(|exit| async move {
            let _exit = exit;
            work.await;
        });
        WorkerHandle {
            _task: task,
            liveness,
        }
    }

    /// Spawns `work` as a task whose exit the drain and close can wait for,
    /// and that removes the session's entry if it was the last thing running.
    /// The task contains a panic in `work` (see [`Self::contain_panic`]), and
    /// `unit` identifies the work in the record of the panic.
    pub(super) fn spawn_session_task(
        &self,
        state: &mut NamespacePublisherState,
        unit: &'static str,
        work: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> SessionTask {
        let id = state.next_task_id;
        state.next_task_id = state.next_task_id.wrapping_add(1);
        let publisher = self.clone();
        let (task, liveness) = self.spawn_tracked(|exit| async move {
            publisher.contain_panic(unit, work).await;
            drop(exit);
            publisher.forget_if_ended();
        });
        SessionTask { id, task, liveness }
    }

    /// Spawns the task that `work` builds around its exit marker, and returns
    /// the task's handle and its liveness.
    ///
    /// The marker exists before the spawn, so a task dropped before its first
    /// poll still reports that it exited.
    fn spawn_tracked<Task>(
        &self,
        work: impl FnOnce(TaskExit) -> Task,
    ) -> (JoinHandle<()>, watch::Receiver<bool>)
    where
        Task: std::future::Future<Output = ()> + Send + 'static,
    {
        let (exit, liveness) = watch::channel(false);
        let task = self.context.runtime.spawn(work(TaskExit(exit)));
        (task, liveness)
    }

    /// Waits for the running worker, if any.
    pub(super) async fn wait_for_worker(&self) {
        let liveness = self
            .lock_state()
            .worker
            .as_ref()
            .map(|worker| worker.liveness.clone());
        if let Some(liveness) = liveness {
            wait_for_exit(liveness).await;
        }
    }

    pub(super) async fn wait_for_fold(&self) -> Result<()> {
        self.wait_for_session_task(|state| &mut state.fold)
            .await
            .map_err(|error| Error::RuntimeTask(format!("WAL-tail fold task failed: {error}")))
    }

    pub(super) async fn wait_for_compaction(&self) -> Result<()> {
        self.wait_for_session_task(|state| &mut state.compaction)
            .await
            .map_err(|error| {
                Error::RuntimeTask(format!("metadata compaction task failed: {error}"))
            })
    }

    /// Waits for the task in `slot`, if any, and reaps it unless a newer
    /// task has replaced it.
    async fn wait_for_session_task(
        &self,
        slot: fn(&mut NamespacePublisherState) -> &mut Option<SessionTask>,
    ) -> std::result::Result<(), tokio::task::JoinError> {
        let task = slot(&mut self.lock_state())
            .as_ref()
            .map(|task| (task.id, task.liveness.clone(), task.task.is_finished()));
        let Some((id, liveness, finished)) = task else {
            return Ok(());
        };
        if !finished {
            wait_for_exit(liveness).await;
        }
        let task = {
            let mut state = self.lock_state();
            let slot = slot(&mut state);
            if slot.as_ref().is_some_and(|task| task.id == id) {
                slot.take().map(|task| task.task)
            } else {
                None
            }
        };
        if let Some(task) = task {
            task.await?;
        }
        Ok(())
    }
}
