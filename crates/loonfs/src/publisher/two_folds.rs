//! The two folds of an operation that ends with a manifest that has to
//! cover every commit written before it, such as a namespace deletion: an
//! ordinary fold while commits continue, then a short pause of the commit
//! queue for the fold of what arrived and the last manifest.

use super::{KeptLane, LockedEngine, NamespacePublisher};
use crate::trace::phase_span;
use crate::{CoreError, Error, MaintenanceCancellation, Result};
use loonfs_core::Phase;
use tracing::Instrument;

impl NamespacePublisher {
    /// Folds the tail as an ordinary fold, and then runs `paused` with the
    /// session's publication lane and its commit queue kept, the pause
    /// (format spec, "Deleting a namespace").
    ///
    /// The call takes the namespace's fold lock and a fold permit, and keeps
    /// both until `paused` returns, so no other fold of the namespace runs
    /// in between. The ordinary fold is a publication of the lane like every
    /// other fold (see [`Self::fold_under_turn`]), and commits continue
    /// while it runs. Then the call keeps the lane and locks the engine,
    /// which is the commit queue: the lock waits for the unit in flight,
    /// whose WAL put is then settled, and no commit is written until
    /// `paused` returns. `paused` loads the session's position, which is
    /// exact while the call keeps both, checks what its caller requires,
    /// runs the fold of what arrived during the ordinary fold (core's
    /// `NamespaceStore::fold_while_paused`), and publishes its last
    /// manifest. So commits pause only for that fold and that manifest.
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
        Ok(paused(&lane, &mut engine).await)
    }
}
