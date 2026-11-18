//! A [`Namespace`] handle's lifecycle: deletion, closing the writer session,
//! the administrator check, and administrator recovery.

use crate::pinned_read::PinnedRead;
use crate::runtime_core::HeadConfirmation;
use crate::trace::{self, operation_span};
use crate::{
    CloseNamespaceReport, DeleteNamespaceOptions, DeleteNamespaceResponse, ErrorCode, Namespace,
    Result, Writable,
};

impl<M: crate::Mode> Namespace<M> {
    /// Delete and snapshot management belong to the service context and to
    /// the namespace's administrators.
    pub(crate) async fn ensure_administrator(&self) -> Result<()> {
        if self.core.subject().is_none() {
            return Ok(());
        }
        self.administrator_read().await.map(drop)
    }

    /// Returns a pinned read of the namespace's head after the check that
    /// [`Self::ensure_administrator`] makes, for an operation that reads
    /// the head itself. A handle with a subject confirms the head as a
    /// `strong` read for the check. A handle without a subject makes no
    /// check and confirms the head as [`HeadConfirmation::View`] does.
    pub(crate) async fn administrator_read(&self) -> Result<PinnedRead> {
        let confirmation = if self.core.subject().is_some() {
            HeadConfirmation::from(crate::Consistency::Strong)
        } else {
            HeadConfirmation::View
        };
        let pinned = self
            .core
            .pinned_read(&self.namespace_id, confirmation)
            .await?;
        pinned.engine.ensure_administrator(&pinned.context).await?;
        Ok(pinned)
    }

    /// Checks the administrator of a deletion as [`Self::ensure_administrator`]
    /// does, and also of an initializing namespace, which every other route
    /// treats as missing: that check reads the access and the segments of the
    /// namespace's manifest 1.
    async fn ensure_deletion_administrator(&self) -> Result<()> {
        match self.ensure_administrator().await {
            Err(error) if error.code() == ErrorCode::NamespaceNotFound => {
                let Some(basis) = self
                    .core
                    .namespace_store(&self.namespace_id)
                    .find_initializing_read_basis()
                    .await?
                else {
                    return Err(error);
                };
                let context = self.core.read_context(basis, None);
                Ok(self
                    .core
                    .reader_engine(&self.namespace_id)
                    .ensure_administrator(&context)
                    .await?)
            }
            result => result,
        }
    }
}

impl Namespace<Writable> {
    /// Ends the namespace after folding its final WAL tail, whatever its
    /// head.
    pub async fn delete(&self) -> Result<DeleteNamespaceResponse> {
        self.delete_with_options(&DeleteNamespaceOptions::default())
            .await
    }

    /// Ends the namespace after folding its final WAL tail.
    /// See [namespace deletion](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#94-deleting-a-namespace).
    ///
    /// Sequenced as a barrier through the publication service: mutations
    /// admitted before the delete publish first, and mutations admitted
    /// after it fail once it succeeds.
    pub async fn delete_with_options(
        &self,
        options: &DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        trace::in_operation(
            operation_span!(self.core, "delete", self.namespace_id),
            async {
                self.ensure_deletion_administrator().await?;
                self.session().submit_delete(*options).await
            },
        )
        .await
    }

    /// Refuses new work, cancels the session's metadata compaction, drains
    /// admitted work, waits for the fold and the compaction, and ends the
    /// session.
    ///
    /// The compaction stops at once while it waits for a compaction permit,
    /// and at its next block while it runs a unit, so close never waits for
    /// another session's compaction. From the moment this is called, commits
    /// and deletes through every clone of this handle fail with
    /// `writer_session_closed`, and so does [`LoonFs::open_namespace`] for
    /// this namespace until the drain finishes. After this returns, the next
    /// open starts a new session, which acquires a new writer epoch. Closing
    /// a session that no longer admits work only waits for its drain, and the
    /// report says it was not open. Fails with `shutting_down` after shutdown
    /// begins.
    pub async fn close(self) -> Result<CloseNamespaceReport> {
        trace::in_operation(
            operation_span!(self.core, "close", self.namespace_id),
            async { Ok(self.session().close().await?) },
        )
        .await
    }
}
