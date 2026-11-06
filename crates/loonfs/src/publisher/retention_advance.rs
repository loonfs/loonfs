//! The retention advance, which moves the floor under the session's writer
//! epoch after the session has folded every commit validated against the
//! old floor.

use super::folds::FoldWhen;
use super::queue::PendingRetentionAdvance;
use super::{NamespacePublisher, PublisherRegistry};
use crate::{AdvanceRetentionResponse, ChangeSeq, CoreError, Error, Result};
use futures::FutureExt;
use loonfs_core::time::OperationClock;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

impl NamespacePublisher {
    /// Runs one retention advance and delivers its outcome to every caller
    /// that waits for it.
    pub(super) async fn execute_retention_advance(&self, pending: PendingRetentionAdvance) {
        let PendingRetentionAdvance { target, waiters } = pending;
        let outcome = match AssertUnwindSafe(self.advance_retention_floor(target))
            .catch_unwind()
            .await
        {
            Ok(outcome) => outcome,
            Err(_) => {
                // The advance has no receipt to settle against, so its callers
                // get the error, and the next sweep tries the advance again.
                self.record_panic();
                Err(Error::RuntimeTask(
                    "retention advance task aborted mid-advance".to_owned(),
                ))
            }
        };
        // The acquisition may have published a claim manifest.
        self.raise_hint_to_engine_publication().await;
        for waiter in waiters {
            let _ = waiter.send(outcome.clone());
        }
    }

    /// Advances the retention floor to `target` in three steps, in this
    /// order (format spec, "Advancing the retention floor"):
    ///
    /// 1. Acquires the writer epoch for the session, unless the session
    ///    already has it, and a position at the namespace's tip. Every WAL
    ///    object of an older epoch is then below the session's fence.
    /// 2. Folds the tail through the namespace's one fold path, so the
    ///    manifest after the fold covers the session's tip.
    /// 3. Publishes the floor under the session's epoch, building on the
    ///    session's position after the fold. If the put finds a manifest of
    ///    another writer epoch, the session is fenced, and the advance
    ///    publishes nothing and returns `writer_fenced`.
    ///
    /// The fence comes before the fold because a writer of an older epoch can
    /// commit until the fence exists. If the fold came first, an older writer
    /// could commit a restore validated against the old floor after the fold,
    /// and that restore could bring back a deletion that the new floor expires.
    async fn advance_retention_floor(&self, target: ChangeSeq) -> Result<AdvanceRetentionResponse> {
        let (Some(writer), Some(shared)) = (self.context.writer.upgrade(), self.shared.upgrade())
        else {
            return Err(CoreError::ShuttingDown.into());
        };
        let registry = PublisherRegistry { shared };
        let core = &self.context.runtime_core;
        let acquired = {
            let mut slot = self.lock_engine().await;
            let engine = self.engine_for(&mut slot, &writer.writer_id);
            let clock = OperationClock::start(core.now_ms(), Arc::clone(core.timer()));
            engine.acquire_position(&clock).await?
        };
        registry
            .fold_namespace(&self.namespace_id, FoldWhen::Always)
            .await?;
        // The floor's publication builds on the session's position, which the
        // fold moved onto the manifest that covers the tip, so its put needs no
        // request first. Without a position within the revalidation bound, the
        // put finds the current manifest through the hint.
        let basis = match self.current_position() {
            Some(position) => {
                loonfs_core::control::load_position_basis(
                    core.store(),
                    &core.metadata_segment_cache(),
                    &position,
                )
                .await?
            }
            None => None,
        };
        let published = core
            .namespace_store(&self.namespace_id)
            .publish_retention_floor(basis, acquired.writer_epoch, target)
            .await;
        if let Err(CoreError::WriterFenced(fence)) = &published {
            let mut slot = self.lock_engine().await;
            if let Some(engine) = slot.engine.as_mut() {
                engine.record_fenced(fence.clone());
            }
        }
        let retention_floor_seq = registry
            .finish_publication(&self.namespace_id, published.map_err(Error::from))
            .await?;
        Ok(AdvanceRetentionResponse {
            namespace_id: self.namespace_id.clone(),
            retention_floor_seq,
        })
    }
}
