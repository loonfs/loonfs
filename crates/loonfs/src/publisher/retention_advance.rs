//! The retention advance in a writer session's queue, which creates a floor
//! object at the session's tip so that the floor object never collides with
//! the session's own commits.

use super::queue::PendingRetentionAdvance;
use super::NamespacePublisher;
use crate::{CommitSeq, CoreError, Error, Result};
use futures::FutureExt;
use loonfs_core::control::LoadedManifest;
use loonfs_core::time::OperationClock;
use loonfs_core::FloorObjectReport;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

impl NamespacePublisher {
    /// Runs one retention advance and delivers its outcome to every caller
    /// that waits for it.
    pub(super) async fn execute_retention_advance(&self, pending: PendingRetentionAdvance) {
        let PendingRetentionAdvance {
            target,
            planned,
            waiters,
        } = pending;
        let outcome = match AssertUnwindSafe(self.publish_floor_object(&planned, target))
            .catch_unwind()
            .await
        {
            Ok(outcome) => outcome,
            Err(_) => {
                // A panic can follow the floor object's put, so the position
                // may omit the floor object. The callers get the error, and
                // the next sweep tries the advance again.
                self.record_panic();
                if let Some(engine) = self.lock_engine().await.engine.as_mut() {
                    engine.invalidate_projection();
                }
                Err(Error::RuntimeTask(
                    "retention advance task aborted mid-advance".to_owned(),
                ))
            }
        };
        for waiter in waiters {
            let _ = waiter.send(outcome.clone());
        }
    }

    /// Creates a floor object for `target` through the session's commit
    /// engine, which puts the floor object at the position's tip after the
    /// tip confirmation that a commit's put gets, and moves the position
    /// onto the floor object (see
    /// [`NamespaceCommitEngine::publish_floor_object`](loonfs_core::publish::NamespaceCommitEngine::publish_floor_object)).
    async fn publish_floor_object(
        &self,
        planned: &LoadedManifest,
        target: CommitSeq,
    ) -> Result<FloorObjectReport> {
        let Some(writer) = self.context.writer.upgrade() else {
            return Err(CoreError::ShuttingDown.into());
        };
        let core = &self.context.runtime_core;
        let mut slot = self.lock_engine().await;
        let engine = self.engine_for(&mut slot, &writer.writer_id);
        let clock = OperationClock::start(core.now_ms(), Arc::clone(core.timer()));
        engine
            .publish_floor_object(planned, target, &clock)
            .await
            .map_err(Error::from)
    }
}
