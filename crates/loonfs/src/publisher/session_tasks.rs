//! The tasks that a writer session runs beside its queue: the worker, the
//! WAL-tail fold, and the metadata compaction that a fold starts, and the
//! waits for them.

use super::registry::PositionChange;
use super::PublisherRegistry;
use super::{FoldTrigger, NamespacePublisher, NamespacePublisherState};
use crate::maintenance::{Compactor, TailFold};
use crate::trace::{phase_event, phase_span};
use crate::MaintainMetadataOptions;
use crate::{Error, FoldWalOutcome, MetadataMaintenanceOutcome, Result};
use futures::FutureExt;
use loonfs_core::limits::FOLD_AT_WAL_OBJECTS;
use loonfs_core::publish::NamespaceCommitEngine;
use loonfs_core::time::OperationClock;
use loonfs_core::Phase;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tracing::Instrument;

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
    /// Starts a fold task after its triggering publication releases the engine.
    /// A panicked fold is retried by the next publish over the threshold. The
    /// drain reports the contained panic. A fold that publishes starts the
    /// session's compaction after it releases its fold permit, and the task
    /// folds again while the tail that the fold left is over the trigger.
    pub(super) fn start_fold(&self) -> Option<oneshot::Sender<()>> {
        let mut state = self.lock_state();
        let _session = self.unfenced_session().ok()?;
        if state
            .fold
            .as_ref()
            .is_some_and(|fold| !fold.task.is_finished())
        {
            return None;
        }
        let (start, started) = oneshot::channel();
        let publisher = self.clone();
        let fold = self.spawn_session_task(&mut state, async move {
            if started.await.is_err() {
                return;
            }
            // A fold frees the tail up to where it began, and a publish while
            // it runs starts no fold, so the task checks the trigger again
            // after each fold that published. Each pass after the first folds
            // commits published during the pass before it.
            loop {
                match AssertUnwindSafe(publisher.run_fold()).catch_unwind().await {
                    Ok(true) => {
                        publisher.fold_published.send_replace(());
                        publisher.start_compaction();
                    }
                    Ok(false) => return,
                    Err(_) => {
                        publisher.record_panic();
                        return;
                    }
                }
                if !publisher.position_fold_is_due().await {
                    return;
                }
            }
        });
        state.fold = Some(fold);
        Some(start)
    }

    /// Whether the engine's position shows its tail over the fold trigger.
    /// Without a position the tail is unknown, and the next publish reads
    /// the tail and starts the fold.
    async fn position_fold_is_due(&self) -> bool {
        let slot = self.lock_engine().await;
        slot.engine.as_ref().is_some_and(|engine| {
            engine
                .wal_tail_objects()
                .zip(engine.wal_tail_inline_bytes())
                .is_some_and(|(objects, inline_bytes)| {
                    FoldTrigger::new(self.context.runtime_core.config()).fold_is_due(
                        FOLD_AT_WAL_OBJECTS,
                        objects,
                        inline_bytes,
                    )
                })
        })
    }

    /// Starts the session's metadata compaction unless one is running or the
    /// session no longer admits work. A panicked compaction may run again
    /// after the next fold. The drain reports the contained panic.
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
        let compaction = self.spawn_session_task(&mut state, async move {
            if AssertUnwindSafe(publisher.run_compaction())
                .catch_unwind()
                .await
                .is_err()
            {
                publisher.record_panic();
            }
        });
        state.compaction = Some(compaction);
    }

    /// Runs the maintenance loop over this session's namespace without its
    /// fold step, so folding stays with [`Self::run_fold`].
    async fn run_compaction(&self) {
        let Some(compactor) = self.compactor() else {
            return;
        };
        let seq = self.lock_state().last_published.map(|last| last.seq);
        let result = compactor
            .metadata_while_due(
                &self.namespace_id,
                &self.compaction_cancellation,
                TailFold::Never,
                MaintainMetadataOptions::default().compaction_policy,
            )
            .await;
        let outcome = result
            .as_ref()
            .map_or(MetadataMaintenanceOutcome::Due, |response| response.outcome);
        self.record_metadata_maintenance(seq, outcome);
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

    /// The runtime's compaction loop, acting as this session's writer.
    /// `None` once the writer or the session table is gone.
    fn compactor(&self) -> Option<Compactor> {
        let writer = self.context.writer.upgrade()?;
        let publisher = PublisherRegistry {
            shared: self.shared.upgrade()?,
        };
        let writer_id = writer.writer_id.clone();
        Some(Compactor::new(
            self.context.runtime_core.clone(),
            publisher,
            writer,
            writer_id,
        ))
    }

    /// Folds the tail under a fold permit. Returns whether the fold published
    /// a manifest.
    async fn run_fold(&self) -> bool {
        let Some(_writer) = self.context.writer.upgrade() else {
            return false;
        };
        let _permit = self.context.runtime_core.fold_permit().await;
        let input = {
            let slot = self.lock_engine().await;
            if slot.engine.as_ref().is_some_and(|engine| {
                engine
                    .wal_tail_objects()
                    .zip(engine.wal_tail_inline_bytes())
                    .is_some_and(|(objects, inline_bytes)| {
                        !FoldTrigger::new(self.context.runtime_core.config()).fold_is_due(
                            FOLD_AT_WAL_OBJECTS,
                            objects,
                            inline_bytes,
                        )
                    })
            }) {
                return false;
            }
            slot.engine
                .as_ref()
                .and_then(NamespaceCommitEngine::writer_position)
        };
        let started_at_ms = self.context.runtime_core.timer().monotonic_now_ms();
        let result = self
            .context
            .runtime_core
            .namespace_store(&self.namespace_id)
            .fold_wal_tail(
                input,
                &OperationClock::start(
                    self.context.runtime_core.now_ms(),
                    Arc::clone(self.context.runtime_core.timer()),
                ),
            )
            .instrument(phase_span!(
                self.context.runtime_core,
                Phase::WalFold,
                self.namespace_id
            ))
            .await;
        self.context
            .runtime_core
            .instruments()
            .publisher
            .wal_fold_duration(self.elapsed_ms_since(started_at_ms));
        // The fold moves the position onto the manifest that covers its tail,
        // whether it published that manifest or found it current. A fold
        // that failed leaves the position where it was.
        if let Ok(folded) = &result {
            self.change_position(PositionChange::Move(folded.manifest.clone()))
                .await;
        }
        match result {
            Ok(folded) => {
                let published = folded.response.outcome == FoldWalOutcome::Published;
                if published {
                    self.context.runtime_core.instruments().publisher.wal_fold();
                    if let Some(shared) = self.shared.upgrade() {
                        shared
                            .raise_hint(&self.namespace_id, folded.response.manifest_no)
                            .await;
                    }
                }
                published
            }
            Err(error) => {
                let error = Error::from(error);
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
    /// and that forgets the session if it was the last thing running.
    pub(super) fn spawn_session_task(
        &self,
        state: &mut NamespacePublisherState,
        work: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> SessionTask {
        let id = state.next_task_id;
        state.next_task_id = state.next_task_id.wrapping_add(1);
        let publisher = self.clone();
        let (task, liveness) = self.spawn_tracked(|exit| async move {
            work.await;
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
