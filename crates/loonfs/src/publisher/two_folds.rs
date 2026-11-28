//! The two folds of an operation that ends with a manifest that has to
//! cover every commit written before it, a namespace deletion or a retention
//! advance: an ordinary fold while commits continue, then a short pause of
//! the commit queue for the fold of what arrived and the last manifest.

use super::{KeptLane, LockedEngine, NamespacePublisher};
use crate::trace::phase_span;
use crate::{CoreError, Error, MaintenanceCancellation, Result};
use loonfs_core::Phase;
use tracing::Instrument;

impl NamespacePublisher {
    /// Folds the tail as an ordinary fold, and then runs `paused` with the
    /// session's publication lane and its commit queue kept, the pause
    /// (format spec, "Deleting a namespace" and "Advancing the retention
    /// floor").
    ///
    /// The call takes the namespace's fold lock and a fold permit, and keeps
    /// both until `paused` returns, so no other fold of the namespace runs
    /// in between. The ordinary fold is a publication of the lane like every
    /// other fold (see [`Self::fold_under_turn`]), and commits continue
    /// while it runs. Then the call keeps the lane and locks the engine,
    /// which is the commit queue: the lock waits for the unit in flight,
    /// whose WAL put is then settled, and no commit is written until
    /// `paused` returns. `paused` is the operation's last step in the
    /// session's commit engine, which loads the session's position (exact,
    /// because the call keeps the lane and the commit queue) and checks what
    /// its caller requires, and which folds
    /// what arrived during the ordinary fold with core's
    /// `NamespaceStore::fold_while_paused`, which publishes that fold with
    /// the operation's change as one manifest. A deletion publishes its
    /// tombstone after that manifest, and an advance raises the floor in it.
    /// So commits pause only for that fold and the operation's last
    /// manifest.
    ///
    /// An ordinary fold that fails returns its error, and `paused` doesn't
    /// run. A namespace that the ordinary fold doesn't find
    /// (`namespace_not_found`, such as an initializing namespace) has no
    /// tail to fold, so `paused` runs and loads the namespace again with the
    /// lane and the commit queue kept. A session that loses its writer epoch
    /// after the ordinary fold's publication claims at the start of
    /// `paused`, which keeps the lane and the commit queue as a claim
    /// requires. `stop`, the session's stop, ends the
    /// waits for the fold lock and the fold permit.
    pub(super) async fn fold_then_pause<T>(
        &self,
        stop: &MaintenanceCancellation,
        paused: impl AsyncFnOnce(&KeptLane<'_>, &mut LockedEngine<'_>) -> T,
    ) -> Result<T> {
        let core = &self.context.runtime_core;
        let _turn = self
            .unless_stopped(
                stop,
                self.turns.fold.lock().instrument(phase_span!(
                    core,
                    Phase::WaitForFoldLock,
                    self.namespace_id
                )),
            )
            .await?;
        let _permit = self.fold_permit(stop).await?;
        match self.fold_under_turn().await {
            Ok(_) | Err(Error::Core(CoreError::NamespaceNotFound { .. })) => {}
            Err(error) => return Err(error),
        }
        let lane = self.lock_lane().await;
        let mut engine = self.lock_engine().await;
        let ended = paused(&lane, &mut engine).await;
        // Wakes the commits that wait at the write stop after the fold of what
        // arrived, whether or not it published, because a woken commit that
        // still finds the stop waits again.
        self.fold_published.send_replace(());
        Ok(ended)
    }
}
