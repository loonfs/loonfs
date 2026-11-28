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
    /// puts a commit, and moves the position onto the floor object. The
    /// advance is a unit of the commit queue, which takes the session's
    /// publication lane only for a claim (see
    /// [`NamespacePublisher::lock_engine_for_unit`] and
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
        let (mut engine, start) = self.lock_engine_for_unit().await?;
        let clock = OperationClock::start(core.now_ms(), Arc::clone(core.timer()));
        engine
            .publish_floor_object(start, planned, target, &clock)
            .await
            .map_err(Error::from)
    }
}
