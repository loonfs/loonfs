//! The worker's steps of the writer session's expiry, which commit the
//! expire events for the deletions that a retention advance reaches
//! ([Retention window](https://github.com/loonfs/loonfs/blob/main/docs/design/retention-window.md#expiring-a-deletion)).
//!
//! The worker runs one step between queued items, so a large trash expires
//! one commit at a time while user commits keep publishing. A new session in
//! a fork expires its inherited deletions inside its first publication
//! instead, which the commit engine does before it plans that batch.

use super::{NamespacePublisher, WorkItem};
use crate::trace::phase_event;
use futures::FutureExt;
use loonfs_core::time::Deadline;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

/// When the worker runs the session's next expiry step, while the session
/// has deletions to expire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExpiryTurn {
    /// Before the next queued item.
    BeforeItem,
    /// After the next queued item, because the worker's last unit was a
    /// step. With nothing queued, the step runs at once.
    AfterItem,
    /// Not again in this worker's run, because a step failed. The next
    /// worker tries again.
    Paused,
}

impl ExpiryTurn {
    /// The turn after the worker publishes a queued item.
    pub(super) fn after_item(self) -> Self {
        match self {
            Self::Paused => Self::Paused,
            Self::BeforeItem | Self::AfterItem => Self::BeforeItem,
        }
    }
}

/// One unit of the worker's work.
pub(super) enum WorkerUnit {
    /// A queued batch or delete.
    Item(WorkItem),
    /// One step of the session's expiry.
    Expiry,
}

impl NamespacePublisher {
    /// Records whether the engine has deletions to expire, and starts a
    /// worker for them unless one is running or the session no longer
    /// admits work.
    ///
    /// The worker decides whether to exit under the same lock (see
    /// [`Self::take_next_unit`]), so it either sees the expiry that this
    /// records or has already released its slot and this starts a new one.
    pub(super) fn record_expiry_due(&self, expiry_due: bool) {
        let mut state = self.lock_state();
        state.expiry_due = expiry_due;
        if !expiry_due {
            return;
        }
        let Ok(_session) = self.admitted_session(&state) else {
            return;
        };
        self.spawn_worker_if_idle(&mut state);
    }

    /// Runs one step of the session's expiry while containing a panic from
    /// it, and returns when the worker runs the next step.
    ///
    /// The step commits nothing that a caller waits for, so a panic needs no
    /// delivery. It drops the position, because the panic can follow the WAL
    /// put, and the worker continues with its queue.
    pub(super) async fn publish_expiry_step(&self) -> ExpiryTurn {
        if let Ok(turn) = AssertUnwindSafe(self.publish_expiry_step_through_engine())
            .catch_unwind()
            .await
        {
            return turn;
        }
        self.record_panic();
        if let Some(engine) = self.lock_engine().await.engine.as_mut() {
            engine.invalidate_projection();
        }
        ExpiryTurn::Paused
    }

    async fn publish_expiry_step_through_engine(&self) -> ExpiryTurn {
        let Some(writer) = self.writer.upgrade() else {
            return ExpiryTurn::Paused;
        };
        let context = self.runtime_core.mutation_context(&writer.identity);
        let batch = Deadline::start(Arc::clone(&self.timer));
        let mut slot = self.lock_engine().await;
        let engine = self.engine_for(&mut slot);
        let store = self.runtime_core.store();
        // Boxed for the same type-recursion depth as the batch publish.
        let Some(publish) = Box::pin(engine.publish_expiry_step(&store, &context, &batch)).await
        else {
            return ExpiryTurn::BeforeItem;
        };
        let fold_start = self.record_publication(&publish);
        drop(slot);
        if let Some(start) = fold_start {
            let _ = start.send(());
        }
        let failure = publish.results.into_iter().find_map(Result::err);
        let Some(error) = failure else {
            return ExpiryTurn::AfterItem;
        };
        phase_event!(
            self.runtime_core,
            "deletion_expiry",
            self.namespace_id,
            tracing::Level::WARN,
            error = %crate::Error::Core(error).public_message(),
            "deletion expiry step failed; the next worker or position move tries again"
        );
        ExpiryTurn::Paused
    }
}
