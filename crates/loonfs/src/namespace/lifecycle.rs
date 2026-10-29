//! A [`Namespace`] handle's lifecycle: deletion, closing the writer session,
//! the administrator check, and administrator recovery.

use crate::trace::{self, operation_span};
use crate::{
    CloseNamespaceReport, DeleteNamespaceOptions, DeleteNamespaceResponse, Error, Namespace,
    Result, Writable,
};

impl<M: crate::Mode> Namespace<M> {
    /// Delete and snapshot management belong to the token holder and to
    /// administrators of an ACL namespace.
    pub(crate) async fn ensure_administrator(&self) -> Result<()> {
        if self.core.subject().is_none() {
            return Ok(());
        }
        let crate::pinned_read::PinnedRead { engine, context } = self
            .core
            .pinned_read(&self.namespace_id, crate::Consistency::Strong)
            .await?;
        Ok(engine.ensure_administrator(&context).await?)
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
                self.ensure_administrator().await?;
                self.session().submit_delete(*options).await
            },
        )
        .await
    }

    /// Refuses new work, cancels the session's metadata compaction, drains
    /// admitted work, waits for the fold and the compaction, and ends the
    /// session.
    ///
    /// The compaction stops at once while it waits for a compaction permit
    /// or runs a `metadata` pass, and at its next block while it runs a unit
    /// of any size, so close never waits for another session's merge. From the moment this is called, commits and deletes through
    /// every clone of this handle fail with `writer_session_closed`, and so
    /// does [`LoonFs::open_namespace`] for this namespace until the drain
    /// finishes. After this returns, the next open starts a new session,
    /// which acquires a new writer epoch. Closing a session that no longer
    /// admits work only waits for its drain, and the report says it was not
    /// open. Fails with `shutting_down` after shutdown begins.
    pub async fn close(self) -> Result<CloseNamespaceReport> {
        trace::in_operation(
            operation_span!(self.core, "close", self.namespace_id),
            async { Ok(self.session().close().await?) },
        )
        .await
    }

    /// Grants `admin` on the root row to `principal_id`, keeping every other
    /// root grant, through a commit no subject check applies to.
    pub async fn recover_administrator(
        &self,
        principal_id: &loonfs_types::PrincipalId,
        actor: &loonfs_types::ActorId,
    ) -> Result<loonfs_types::RecoverAdministratorResponse> {
        trace::in_operation(
            operation_span!(self.core, "recover_administrator", self.namespace_id),
            async {
                use loonfs_types::FilesystemChange;
                use loonfs_types::{AccessGrants, AccessRight};

                let crate::pinned_read::PinnedRead { engine, context } = self
                    .core
                    .pinned_read(
                        &self.namespace_id,
                        crate::runtime_core::HeadConfirmation::View,
                    )
                    .await?;
                let root = engine.root_access(&context).await?;
                let grants = root.access.grants;
                let mut entries: std::collections::BTreeMap<_, _> = grants
                    .iter()
                    .map(|(principal, rights)| (principal.clone(), rights))
                    .collect();
                let rights = grants.get(principal_id).union(AccessRight::Admin.into());
                entries.insert(principal_id.clone(), rights);
                let grants = AccessGrants::new(entries).map_err(|error| {
                    Error::Core(crate::CoreError::InvalidCommitField {
                        field: "grants",
                        message: error.to_string(),
                    })
                })?;
                let request = crate::publish::CommitIntent::single(
                    loonfs_types::CommitId::generate(),
                    actor.clone(),
                    Some("administrator recovery".to_owned()),
                    crate::publish::FilesystemOperation::UpdateAccess {
                        path: loonfs_types::AbsolutePath::root(),
                        boundary: root.access.boundary,
                        grants,
                        expected_inode_id: Some(loonfs_types::ROOT_INODE_ID),
                        expected_access_revision_no: Some(root.access_revision_no),
                    },
                );
                let commit = self
                    .commit_candidate_inner(crate::publish::CommitCandidate::maintenance(request))
                    .await?;
                let access_revision_no = commit
                    .events
                    .iter()
                    .find_map(|event| match event {
                        FilesystemChange::AccessChanged {
                            access_revision_no, ..
                        } => Some(*access_revision_no),
                        _ => None,
                    })
                    .expect("administrator recovery should publish an access change");
                Ok(loonfs_types::RecoverAdministratorResponse {
                    namespace_id: commit.namespace_id,
                    commit_id: commit.commit_id,
                    committed_seq: commit.committed_seq,
                    access_revision_no,
                })
            },
        )
        .await
    }
}
