//! A namespace deletion through the namespace's writer session, which runs
//! in its caller's task as admitted maintenance.

use super::NamespacePublisher;
use crate::{CoreError, DeleteNamespaceOptions, DeleteNamespaceResponse, Error, Result};
use loonfs_core::Administrator;

impl NamespacePublisher {
    /// Deletes the namespace through the session's own commit engine, so
    /// the session epoch and fencing that govern this namespace's
    /// publications govern its tombstone too (format spec, "Deleting a
    /// namespace").
    ///
    /// The deletion folds the tail as an ordinary fold while commits
    /// continue, and then, with the session's publication lane and its commit
    /// queue kept, checks the expected head against the session's position,
    /// folds what arrived, and publishes the tombstone (see
    /// [`Self::fold_then_pause`] and
    /// [`NamespaceCommitEngine::delete_namespace`](loonfs_core::publish::NamespaceCommitEngine::delete_namespace)).
    /// So a commit acknowledged before the pause is covered by the
    /// tombstone, or, with an expected head, moves the head, so the deletion
    /// returns `stale_head`. A commit that is still queued at the pause
    /// starts after the tombstone and returns `namespace_deleted`.
    ///
    /// A deletion whose expected head doesn't match the session's kept
    /// position sends no put, so the session's check confirms the position
    /// before the deletion returns the refusal (see [`Self::run_confirmed`]),
    /// and a session that another writer displaced returns `writer_fenced`.
    ///
    /// The tombstone ends the session, whichever unit of the session
    /// publishes or finds it (see [`Self::record_session_end`]).
    pub(super) async fn delete_namespace(
        &self,
        administrator: &Administrator,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        let Some(_writer) = self.context.writer.upgrade() else {
            return Err(CoreError::ShuttingDown.into());
        };
        let stop = &self.stop;
        self.run_confirmed(move || async move {
            self.fold_then_pause(stop, async move |lane, engine| {
                engine.delete_namespace(lane, administrator, options).await
            })
            .await
        })
        .await
        .and_then(|deleted| deleted.map_err(Error::from))
    }
}
