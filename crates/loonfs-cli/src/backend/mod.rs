//! The backend seam: one logical LoonFS API over two transports.
//!
//! [`ResolvedTarget`] is what every CLI command programs against. A resolved
//! profile decides which arm answers — [`EmbeddedBackend`] drives an
//! in-process `loonfs` runtime, the remote arm drives a `loonfs-client` over
//! HTTP — and the commands above this seam cannot tell which they got.
//!
//! The seam is private to this crate on purpose. It exists so `loon` can run
//! its features against either transport, not as an extension point: an
//! application embedding LoonFS programs against `loonfs` (runtime) or
//! `loonfs-client` (HTTP) directly, and neither needs a seam between.
//!
//! The methods are async so the CLI drives both transports from its own
//! runtime. [`loonfs_client::Client`] is itself async, so the remote arms are
//! direct calls that map the client's error type onto [`BackendError`].

mod embedded;

use crate::backend_error::BackendError;
use crate::resolve::ResolvedTarget;
use loonfs_api::{
    v0::{ChangesResponse, DisableGrepIndexResponse, EnableGrepIndexResponse},
    AuthoritativePathEntry, ChangeSeq, CheckpointId, CommitResponse, CreateCheckpointRequest,
    CreateCheckpointResponse, DeleteNamespaceResponse, DestinationBehavior, GrepRequest,
    GrepResponse, InodeId, ListFileRevisionsResponse, ListTrashResponse, MaintenanceStepRequest,
    MaintenanceStepResponse, NamespaceId, NamespaceStatusResponse, NamespaceSummary,
    ReleaseCheckpointResponse, RevisionNo,
};
use loonfs_client::{
    CreateDirectoryOptions, DeleteOptions, MutationOptions, NamespacePath, PutFileOptions,
};

pub(crate) use embedded::EmbeddedBackend;

/// One logical LoonFS API over two transports.
///
/// Every method below is an exhaustive `match self` with no catch-all arm,
/// and that exhaustiveness *is* the parity statement this seam used to make
/// with a trait and two named implementations: neither transport can quietly
/// go missing from a method, and both must report the same registry error
/// code for the same failure, so a command renders identical outcomes
/// regardless of which transport a profile selects (this crate's two-mode
/// parity tests hold that line).
impl ResolvedTarget {
    /// Creates a new empty namespace.
    pub(crate) async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceSummary, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.create_namespace(namespace_id).await,
            Self::Remote(target) => Ok(target.client.create_namespace(namespace_id).await?),
        }
    }

    /// Marks a namespace deleted; `expected_head_seq` guards against deleting
    /// a namespace that moved since the caller last observed it.
    pub(crate) async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
        expected_head_seq: Option<ChangeSeq>,
    ) -> Result<DeleteNamespaceResponse, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .delete_namespace(namespace_id, expected_head_seq)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .delete_namespace(namespace_id, expected_head_seq)
                .await?),
        }
    }

    /// Creates a new namespace as a fork of the source's durable view.
    pub(crate) async fn fork_namespace(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
    ) -> Result<NamespaceSummary, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .fork_namespace(source_namespace_id, new_namespace_id)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .fork_namespace(source_namespace_id, new_namespace_id)
                .await?),
        }
    }

    /// Summarizes a namespace's current head state.
    pub(crate) async fn namespace_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceStatusResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.namespace_status(namespace_id).await,
            Self::Remote(target) => Ok(target.client.namespace_status(namespace_id).await?),
        }
    }

    /// Lists the entries of a directory.
    pub(crate) async fn list_path_entries_all(
        &self,
        spec: &NamespacePath,
    ) -> Result<Vec<AuthoritativePathEntry>, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.list_path_entries_all(spec).await,
            Self::Remote(target) => Ok(target.client.list_path_entries_all(spec).await?.entries),
        }
    }

    /// Describes a single path entry.
    pub(crate) async fn stat_path(
        &self,
        spec: &NamespacePath,
    ) -> Result<AuthoritativePathEntry, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.stat_path(spec).await,
            Self::Remote(target) => Ok(target.client.stat_path(spec).await?),
        }
    }

    /// Reads a file's current content.
    pub(crate) async fn get_file_bytes(
        &self,
        spec: &NamespacePath,
    ) -> Result<Vec<u8>, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.get_file_bytes(spec).await,
            Self::Remote(target) => Ok(target.client.get_file_bytes(spec).await?),
        }
    }

    /// Content search over a namespace's grep index.
    pub(crate) async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
    ) -> Result<GrepResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.grep(namespace_id, request).await,
            Self::Remote(target) => Ok(target.client.grep(namespace_id, request).await?),
        }
    }

    /// Enables the grep index on a namespace (admin plane).
    pub(crate) async fn enable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<EnableGrepIndexResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.enable_grep_index(namespace_id).await,
            Self::Remote(target) => Ok(target.client.enable_grep_index(namespace_id).await?),
        }
    }

    /// Disables the grep index on a namespace (admin plane).
    pub(crate) async fn disable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<DisableGrepIndexResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.disable_grep_index(namespace_id).await,
            Self::Remote(target) => Ok(target.client.disable_grep_index(namespace_id).await?),
        }
    }

    /// Reads a retained file revision's content.
    pub(crate) async fn get_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .get_file_revision_bytes(spec, revision_no)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .get_file_revision_bytes(spec, revision_no)
                .await?),
        }
    }

    /// Lists one page of the namespace's recoverable deletions.
    pub(crate) async fn list_trash(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListTrashResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.list_trash(namespace_id, limit, cursor).await,
            Self::Remote(target) => Ok(target
                .client
                .list_trash_page(namespace_id, limit, cursor)
                .await?),
        }
    }

    /// Lists one page of a file's retained revisions.
    pub(crate) async fn list_file_revisions_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .list_file_revisions_page(spec, limit, cursor)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .list_file_revisions_page(spec, limit, cursor)
                .await?),
        }
    }

    /// Writes a file; `behavior` selects create-only or replace semantics.
    /// An explicit `commit_id` makes the call retryable by resubmission;
    /// absent, one is generated and returned in the response.
    pub(crate) async fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        options: &PutFileOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.put_file_bytes(spec, bytes, options).await,
            Self::Remote(target) => Ok(target.client.put_file_bytes(spec, bytes, options).await?),
        }
    }

    /// Creates a directory; `parents` also creates missing ancestors.
    pub(crate) async fn create_directory(
        &self,
        spec: &NamespacePath,
        options: &CreateDirectoryOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.create_directory(spec, options).await,
            Self::Remote(target) => Ok(target.client.create_directory(spec, options).await?),
        }
    }

    /// Deletes a file or empty directory. With `expected_inode_id`, the
    /// delete applies only while the path still resolves to that inode, so
    /// callers reporting a recovery handle never report a raced rebinding.
    pub(crate) async fn delete_path(
        &self,
        spec: &NamespacePath,
        options: &DeleteOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.delete_path(spec, options).await,
            Self::Remote(target) => Ok(target.client.delete_path(spec, options).await?),
        }
    }

    /// Moves a path within a namespace; `behavior` selects create-only or
    /// replace semantics for the destination.
    pub(crate) async fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        behavior: DestinationBehavior,
        options: &MutationOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.move_path(from, to, behavior, options).await,
            Self::Remote(target) => {
                Ok(target.client.move_path(from, to, behavior, options).await?)
            }
        }
    }

    /// Copies a file within a namespace; `behavior` selects create-only or
    /// replace semantics for the destination.
    pub(crate) async fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        behavior: DestinationBehavior,
        options: &MutationOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.copy_path(from, to, behavior, options).await,
            Self::Remote(target) => {
                Ok(target.client.copy_path(from, to, behavior, options).await?)
            }
        }
    }

    /// Restores a file to one of its retained revisions.
    pub(crate) async fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
        options: &MutationOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .restore_file_revision(spec, source_revision_no, options)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .restore_file_revision(spec, source_revision_no, options)
                .await?),
        }
    }

    /// Recovers a deleted file or subtree to the spec's path; `inode_id`
    /// and `deleted_at_seq` are the identity and committed sequence the
    /// delete reported.
    pub(crate) async fn undelete(
        &self,
        spec: &NamespacePath,
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        options: &MutationOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .undelete(spec, inode_id, deleted_at_seq, options)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .undelete(spec, inode_id, deleted_at_seq, options)
                .await?),
        }
    }

    // --- maintenance/admin plane (`admin/v0`) ---

    /// Creates or reuses a named, user-owned checkpoint pinning the
    /// namespace's current view.
    pub(crate) async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        request: CreateCheckpointRequest,
    ) -> Result<CreateCheckpointResponse, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .create_checkpoint(namespace_id, request)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .create_checkpoint(namespace_id, &request)
                .await?),
        }
    }

    /// Releases a user-owned checkpoint pin by id. Idempotent.
    pub(crate) async fn release_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<ReleaseCheckpointResponse, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .release_checkpoint(namespace_id, checkpoint_id)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .release_checkpoint(namespace_id, checkpoint_id)
                .await?),
        }
    }

    /// Runs one bounded maintenance step: WAL flush, metadata
    /// reorganization, retention advance, and — when `request.gc` opts in —
    /// one garbage-collection pass. `request.only` restricts it to a single
    /// sub-step.
    pub(crate) async fn maintenance_step(
        &self,
        namespace_id: &NamespaceId,
        request: MaintenanceStepRequest,
    ) -> Result<MaintenanceStepResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.maintenance_step(namespace_id, request).await,
            Self::Remote(target) => Ok(target
                .client
                .maintenance_step(namespace_id, &request)
                .await?),
        }
    }

    /// Reads the ordered change feed after the `after_seq` cursor.
    pub(crate) async fn list_changes(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        limit: Option<u32>,
    ) -> Result<ChangesResponse, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .list_changes(namespace_id, after_seq, limit)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .list_changes(namespace_id, after_seq, limit)
                .await?),
        }
    }
}
