//! How a writer session keeps its own position correct: the one way that it
//! settles a position that something found replaced, and the one way that it
//! confirms a value that it computed from its position without a put.
//!
//! Only the session's queues (the commit queue and the publication lane)
//! change its position. A commit advances it, a publication of the lane
//! records its manifest and the position follows, and the writer's load (a
//! discovery of the namespace from the store under the session's writer
//! epoch, which returns `writer_fenced` after another writer's claim) loads
//! it again. A read never moves the position onto a manifest that the read
//! found and never drops it. A read whose check finds a newer manifest asks
//! the session to settle its position (see
//! [`NamespacePublisher::settle_replaced_position`]).

use super::NamespacePublisher;
use crate::runtime_core::CheckedPosition;
use crate::{CoreError, Error, Result};
use loonfs_core::publish::{UnitBasis, UnitReport, UnitStart, WriterPosition, WriterSessionStatus};
use loonfs_types::{ManifestNo, WriterEpoch};
use std::future::Future;

impl NamespacePublisher {
    /// Settles the session's position after something found its manifest,
    /// manifest `replaced`, replaced by a newer manifest: a read's check, or
    /// a fold's, a compaction unit's or a snapshot's use of the position.
    ///
    /// A newer manifest is the session's own publication between its put
    /// and its record, or another writer's claim. So the call first waits
    /// for the session's publication lane, and a publication in flight has
    /// recorded its manifest once the call has the lane. When the session's
    /// position then follows a manifest above `replaced`, the newer manifest
    /// was the session's own (a put that succeeds at the number after
    /// `replaced` shows that no other writer's manifest is there), and the
    /// call returns without the engine, so no commit waits for it. Otherwise,
    /// with the engine, and only while the session keeps its writer epoch and
    /// a position, the call drops the position and runs the writer's load,
    /// which returns `writer_fenced`, and marks the session fenced, when
    /// another writer claimed the namespace.
    ///
    /// The call never claims: a session without its epoch has no position to
    /// settle, and a read must never cause a claim. When it returns, the
    /// session's position isn't on `replaced`: it follows a newer manifest of
    /// the session, or it was loaded again, or the session has none.
    pub(crate) async fn settle_replaced_position(
        &self,
        replaced: ManifestNo,
    ) -> std::result::Result<(), CoreError> {
        let _lane = self.lock_lane().await;
        let follows_newer = self
            .current_position()
            .is_some_and(|position| position.basis().manifest_no() > replaced);
        if follows_newer {
            return Ok(());
        }
        let mut engine = self.lock_engine().await;
        if engine.writer_position().is_none() {
            return Ok(());
        }
        let Some(start @ UnitStart::Kept(_)) = engine.kept_start()? else {
            return Ok(());
        };
        engine.invalidate_projection();
        engine.load_position(start).await.map(drop)
    }

    /// Runs `run`, one unit of the session's commit queue or of its
    /// publication lane, or a load of the manifest that a retention advance
    /// or an extension's change plans from, and returns its value once the
    /// value is confirmed.
    ///
    /// A value that rests on the store needs no check: a put that succeeds
    /// at the next number shows that no other writer's fence or claim is at
    /// that number. A value that the unit computed from the session's own
    /// state without a put (see [`UnitBasis::Session`]), such as a batch
    /// whose commits were all refused, a deletion whose expected head doesn't
    /// match, or a window that the manifest already has, is confirmed by the
    /// session's check, the check that a strong read makes, which the reads
    /// and refusals in flight share and which settles a position that it
    /// finds replaced (see
    /// [`RuntimeCore::checked_writer_position`](crate::runtime_core::RuntimeCore::checked_writer_position)).
    /// A session that another writer displaced and that hasn't put since
    /// finds the other writer's claim here.
    ///
    /// The check confirms the position that the session has when the check
    /// runs, so the value is returned only when the confirmed position has
    /// the writer epoch that the session had before `run`. A session can
    /// lose its epoch to an unknown outcome on its other queue while `run`
    /// computes, and claim and load again before the check. Otherwise `run`
    /// runs once more, and its own start loads a position, claims, or
    /// returns `writer_fenced`, as every unit's start does, so its value
    /// needs no check.
    ///
    /// A unit that returns an error isn't confirmed. Every such error comes
    /// from the store or from the session's own status, not from the kept
    /// position: a fenced, closed or deleted session's refusal, a claim's or
    /// a load's error, a write of segments or content, a publication budget
    /// that ran out on the unit's own clock, a lane publication's
    /// `namespace_deleted` from the session's newest published manifest
    /// (a manifest of the session, so the session deleted the namespace), and
    /// a position found behind, which [`Self::run_from_position`] settles.
    /// A commit's refusals are values of its batch, not errors of the unit.
    ///
    /// The check costs one HEAD of the successor of the position's manifest
    /// and one conditional read of the manifest boundary, and a unit that
    /// puts costs nothing more.
    pub(super) async fn run_confirmed<T, E, Fut>(
        &self,
        mut run: impl FnMut() -> Fut,
    ) -> std::result::Result<T, E>
    where
        E: From<CoreError>,
        Fut: Future<Output = std::result::Result<UnitReport<T>, E>>,
    {
        let epoch = self.writer_epoch();
        let first = run().await?;
        if first.basis == UnitBasis::Store || self.confirms(epoch).await? {
            return Ok(first.value);
        }
        Ok(run().await?.value)
    }

    /// Returns the session's writer epoch, or `None` when the session
    /// doesn't have one.
    fn writer_epoch(&self) -> Option<WriterEpoch> {
        match &*self.session.lock() {
            WriterSessionStatus::Acquired { writer, .. } => Some(writer.writer_epoch),
            _ => None,
        }
    }

    /// Returns whether the session's check confirms a position with
    /// `epoch`, the writer epoch that the session had before a unit ran.
    async fn confirms(&self, epoch: Option<WriterEpoch>) -> std::result::Result<bool, CoreError> {
        let checked = self
            .context
            .runtime_core
            .checked_writer_position(&self.namespace_id, None)
            .await?;
        Ok(match checked {
            CheckedPosition::Confirmed(position) => {
                epoch == Some(position.read_basis().head().writer_epoch)
            }
            CheckedPosition::Absent | CheckedPosition::Unconfirmed => false,
        })
    }

    /// Runs `run`, the writes of a fold or a compaction unit before its put,
    /// or a load of the manifest that a retention advance or an extension's
    /// change plans from, from the session's position (see
    /// [`Self::writer_position`]), and runs it once more from the position
    /// after a settle when the first position turns out to be behind. The
    /// writes then take the session's publication lane for their put.
    ///
    /// A position is "behind" when `run` returns `head_moved`, because the
    /// position's manifest is missing from the store and at or below the
    /// manifest boundary (see
    /// [`NamespaceStore::load_publication_basis`](loonfs_core::NamespaceStore::load_publication_basis)),
    /// or `manifest_replaced`, because a segment of that manifest is missing
    /// and a newer manifest replaced it (see `SegmentKeeper::load_error` in
    /// core). Either way a newer manifest replaced the position's manifest,
    /// so the session settles its position (see
    /// [`Self::settle_replaced_position`]), which returns `writer_fenced`
    /// when another writer claimed the namespace, and takes its position
    /// again. When the second run finds its position behind too, the session
    /// settles once more and returns that settle's error, such as
    /// `writer_fenced`, or the second run's error.
    pub(super) async fn run_from_position<T, Fut>(
        &self,
        mut run: impl FnMut(WriterPosition) -> Fut,
    ) -> Result<T>
    where
        Fut: Future<Output = loonfs_core::Result<T>>,
    {
        let position = self.writer_position().await?;
        let replaced = position.basis().manifest_no();
        match run(position).await {
            Err(CoreError::HeadMoved(_) | CoreError::ManifestReplaced { .. }) => {
                self.settle_replaced_position(replaced).await?;
            }
            settled => return settled.map_err(Error::from),
        }
        let position = self.writer_position().await?;
        let replaced = position.basis().manifest_no();
        match run(position).await {
            Err(behind @ (CoreError::HeadMoved(_) | CoreError::ManifestReplaced { .. })) => {
                self.settle_replaced_position(replaced).await?;
                Err(behind.into())
            }
            settled => settled.map_err(Error::from),
        }
    }
}
