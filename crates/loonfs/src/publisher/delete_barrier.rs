//! The delete barrier: a namespace deletion that publishes after the work
//! admitted before it, and fails the work admitted after it.

use super::queue::{take_queued_waiters, PendingDelete};
use super::{NamespacePublisher, PublisherAdmissionStatus};
use crate::trace::phase_event;
use crate::{CoreError, DeleteNamespaceOptions, DeleteNamespaceResponse, Error, ErrorCode, Result};
use futures::FutureExt;
use loonfs_core::Phase;
use std::panic::AssertUnwindSafe;

impl NamespacePublisher {
    /// Runs the delete barrier. Returns true when the publisher is now
    /// terminal and its worker should exit.
    pub(super) async fn execute_delete(&self, pending: PendingDelete) -> bool {
        let PendingDelete { options, waiters } = pending;
        let outcome = match AssertUnwindSafe(self.delete_through_engine(options))
            .catch_unwind()
            .await
        {
            Ok(outcome) => outcome,
            Err(_) => {
                // Contain deletion panics so the worker can continue. Deletion has no commit
                // receipt for reconciliation, so report an internal error to its callers.
                self.record_panic();
                Err(CoreError::Internal("delete task aborted mid-delete".to_owned()).into())
            }
        };
        let deleted = outcome
            .as_ref()
            .err()
            .is_none_or(|error| error.code() == ErrorCode::NamespaceDeleted);
        // The raise runs while the session is in the table, so a drain that
        // finds the session waits for the raise.
        self.raise_hint_to_engine_publication().await;
        if deleted {
            // Tombstone first, then fail everything that queued behind
            // the delete; admissions from here on fail fast.
            let queued = {
                let mut state = self.lock_state();
                state.admission = PublisherAdmissionStatus::Deleted;
                take_queued_waiters(&mut state)
            };
            self.compaction_cancellation.cancel();
            // A deleted session has ended. It leaves the table before the
            // delete's callers hear the outcome, so an open after that
            // starts a fresh session whose publish fails on the durable
            // tombstone. Handles to this session fail fast on `Deleted`.
            // A session stays in the table while its compaction runs (the
            // drain finds running compactions through the table), so the
            // delete waits for the cancelled compaction before it removes
            // the session.
            if let Err(error) = self.wait_for_compaction().await {
                phase_event!(
                    self.context.runtime_core,
                    Phase::MetadataCompaction,
                    self.namespace_id,
                    tracing::Level::WARN,
                    error = %error.public_message(),
                    "metadata compaction failed while the namespace was deleted"
                );
            }
            self.forget_if_ended();
            for waiter in queued.commits {
                let _ = waiter.send(Err(self.namespace_deleted().into()));
            }
            for waiter in queued.deletes {
                let _ = waiter.send(Err(self.namespace_deleted().into()));
            }
        }
        for waiter in waiters {
            let _ = waiter.send(outcome.clone());
        }
        deleted
    }

    /// Deletes the namespace through the publisher's own commit engine, so
    /// the session epoch and fencing that govern this namespace's
    /// publications govern its tombstone swap too.
    async fn delete_through_engine(
        &self,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        self.wait_for_fold().await?;
        let Some(writer) = self.context.writer.upgrade() else {
            return Err(CoreError::ShuttingDown.into());
        };
        let _permit = self.context.runtime_core.fold_permit().await;
        let mut slot = self.lock_engine().await;
        let engine = self.engine_for(&mut slot, &writer.writer_id);
        let result = engine.delete_namespace(options).await.map_err(Error::from);
        // What follows a deletion depends on the namespace being deleted, not
        // on which call deleted it.
        if result
            .as_ref()
            .err()
            .is_none_or(|error| error.code() == ErrorCode::NamespaceDeleted)
        {
            self.context
                .runtime_core
                .invalidate_namespace_read_cache(&self.namespace_id);
        }
        result
    }
}
