//! The delete barrier: a namespace deletion that publishes after the work
//! admitted before it, and fails the work admitted after it.

use super::admission::AdmittedWaiter;
use super::queue::take_queued_waiters;
use super::{NamespacePublisher, PublisherAdmissionStatus};
use crate::trace::phase_event;
use crate::{CoreError, DeleteNamespaceOptions, DeleteNamespaceResponse, Error, ErrorCode, Result};
use loonfs_core::{Administrator, Phase};

impl NamespacePublisher {
    /// Ends the delete barrier with the deletion's `outcome`, and returns
    /// it to the deletion's `waiters`. Returns true when the publisher is now
    /// terminal and its worker should exit.
    pub(super) async fn finish_delete(
        &self,
        outcome: Result<DeleteNamespaceResponse>,
        waiters: Vec<AdmittedWaiter<Result<DeleteNamespaceResponse>>>,
    ) -> bool {
        let deleted = outcome
            .as_ref()
            .err()
            .is_none_or(|error| error.code() == ErrorCode::NamespaceDeleted);
        if deleted {
            // Tombstone first, then fail everything that queued behind
            // the delete; admissions from here on fail fast.
            let queued = {
                let mut state = self.lock_state();
                state.admission = PublisherAdmissionStatus::Deleted;
                let queued = take_queued_waiters(&mut state);
                self.count_queued(&mut state);
                queued
            };
            self.report_queue_depth();
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
            for waiter in queued.retention_advances {
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
    ///
    /// The deletion folds the tail before its tombstone, with the
    /// namespace's fold lock in exclusive mode, so it waits for any other
    /// fold of the namespace instead of folding the same tail.
    pub(super) async fn delete_through_engine(
        &self,
        administrator: &Administrator,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        self.wait_for_fold().await?;
        let Some(_writer) = self.context.writer.upgrade() else {
            return Err(CoreError::ShuttingDown.into());
        };
        let _turn = self.turns.fold.write().await;
        let _permit = self.context.runtime_core.fold_permit().await;
        let mut engine = self.lock_engine().await;
        let result = engine
            .delete_namespace(administrator, options)
            .await
            .map_err(Error::from);
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
