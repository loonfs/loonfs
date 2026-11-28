//! A retention floor advance through the namespace's writer session, which
//! runs in its caller's task as admitted maintenance.

use super::NamespacePublisher;
use crate::{CommitSeq, CoreError, Error, Result};

impl NamespacePublisher {
    /// Raises the retention floor to `target` through the session's own
    /// commit engine, so the floor is recorded in a manifest of the session
    /// (format spec, "Advancing the retention floor").
    ///
    /// The advance folds the tail as an ordinary fold while commits
    /// continue, and then, with the session's publication lane and its
    /// commit queue kept, folds what arrived and publishes one manifest that
    /// includes that fold and whose floor is the higher of the old floor and
    /// `target` (see [`Self::fold_then_pause`] and
    /// [`NamespaceCommitEngine::raise_retention_floor`](loonfs_core::publish::NamespaceCommitEngine::raise_retention_floor)).
    /// So a commit acknowledged before the pause, such as a restore that was
    /// validated against the old floor, is in that manifest, and a commit
    /// that is still queued at the pause is validated against the new floor.
    /// Two advances take turns at the fold lock, so the second folds from
    /// the manifest that the first published.
    ///
    /// An advance that publishes nothing, because nothing arrived and the
    /// floor is already at or above `target`, returns the floor of the
    /// session's newest published manifest, which the session's check
    /// confirms first (see [`Self::run_confirmed`]).
    pub(super) async fn advance_retention_floor(&self, target: CommitSeq) -> Result<CommitSeq> {
        let Some(_writer) = self.context.writer.upgrade() else {
            return Err(CoreError::ShuttingDown.into());
        };
        let stop = &self.stop;
        self.run_confirmed(move || async move {
            self.fold_then_pause(stop, async move |lane, engine| {
                engine.raise_retention_floor(lane, target).await
            })
            .await
        })
        .await
        .and_then(|raised| raised.map_err(Error::from))
    }
}
