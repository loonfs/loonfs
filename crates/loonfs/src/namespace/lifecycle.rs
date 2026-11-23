//! A [`Namespace`] handle's lifecycle: deletion, closing the writer session,
//! the administrator check, and administrator recovery.

use crate::pinned_read::PinnedRead;
use crate::runtime_core::HeadConfirmation;
use crate::trace::{self, operation_span};
use crate::{
    CloseNamespaceReport, DeleteNamespaceOptions, DeleteNamespaceResponse, ErrorCode, Namespace,
    Result, Writable,
};
use loonfs_core::Administrator;

/// A pinned read of a namespace's head, and the proof that the reader
/// administers the namespace, as [`Namespace::administrator_read`] returns
/// them.
pub(crate) struct AdministratorRead {
    pub(crate) pinned: PinnedRead,
    pub(crate) administrator: Administrator,
}

impl<M: crate::Mode> Namespace<M> {
    /// Returns the proof that this handle's reader administers the
    /// namespace, for deletion, snapshot management and forks. A handle
    /// whose engine needs no read for the proof, as in the service context,
    /// makes no read. Any other handle confirms the head as a `strong` read
    /// and checks there, because the check chooses between an error and the
    /// normal path.
    pub(crate) async fn administrator(&self) -> Result<Administrator> {
        if let Some(administrator) = self
            .core
            .reader_engine(&self.namespace_id)
            .find_administrator_without_read()
        {
            return Ok(administrator);
        }
        let pinned = self
            .core
            .pinned_read(&self.namespace_id, crate::Consistency::Strong)
            .await?;
        Ok(pinned.engine.verify_administrator(&pinned.context).await?)
    }

    /// Returns a pinned read of the namespace's head with the proof that
    /// [`Self::administrator`] returns, for a snapshot read that reads the
    /// head itself. A handle whose engine needs no read for the proof
    /// confirms the head as [`HeadConfirmation::View`] does. Any other
    /// handle confirms it as a `strong` read, and the check reads that head.
    pub(crate) async fn administrator_read(&self) -> Result<AdministratorRead> {
        let engine = self.core.reader_engine(&self.namespace_id);
        if let Some(administrator) = engine.find_administrator_without_read() {
            let pinned = self
                .core
                .pinned_read(&self.namespace_id, HeadConfirmation::View)
                .await?;
            return Ok(AdministratorRead {
                pinned,
                administrator,
            });
        }
        let pinned = self
            .core
            .pinned_read(&self.namespace_id, crate::Consistency::Strong)
            .await?;
        let administrator = pinned.engine.verify_administrator(&pinned.context).await?;
        Ok(AdministratorRead {
            pinned,
            administrator,
        })
    }

    /// Returns the proof for a deletion as [`Self::administrator`] does, and
    /// also for an initializing namespace, which every other route treats as
    /// missing. The check of an initializing namespace reads the access and
    /// the segments of the namespace's manifest 1.
    async fn deletion_administrator(&self) -> Result<Administrator> {
        match self.administrator().await {
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
                    .verify_administrator(&context)
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
                let administrator = self.deletion_administrator().await?;
                self.session().submit_delete(*options, administrator).await
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
