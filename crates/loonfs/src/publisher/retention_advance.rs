//! The retention advance in a writer session's queue, which creates a floor
//! object at the session's tip so that the floor object never collides with
//! the session's own commits.

use super::NamespacePublisher;
use crate::{CommitSeq, CoreError, Error, Result};
use loonfs_core::control::LoadedManifest;
use loonfs_core::time::OperationClock;
use loonfs_core::FloorObjectReport;
use std::sync::Arc;

impl NamespacePublisher {
    /// Creates a floor object for `target` through the session's commit
    /// engine, which puts the floor object after the position's tip as it
    /// puts a commit, and moves the position
    /// onto the floor object (see
    /// [`NamespaceCommitEngine::publish_floor_object`](loonfs_core::publish::NamespaceCommitEngine::publish_floor_object)).
    pub(super) async fn publish_floor_object(
        &self,
        planned: &LoadedManifest,
        target: CommitSeq,
    ) -> Result<FloorObjectReport> {
        let Some(_writer) = self.context.writer.upgrade() else {
            return Err(CoreError::ShuttingDown.into());
        };
        let core = &self.context.runtime_core;
        let mut engine = self.lock_engine().await;
        let clock = OperationClock::start(core.now_ms(), Arc::clone(core.timer()));
        engine
            .publish_floor_object(planned, target, &clock)
            .await
            .map_err(Error::from)
    }
}
