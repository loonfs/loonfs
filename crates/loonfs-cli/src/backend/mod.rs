//! The backend seam: one logical LoonFS API over two transports.
//!
//! [`Backend`] is what every CLI command programs against. A resolved profile
//! decides which implementation answers — [`EmbeddedBackend`] drives an
//! in-process `loonfs` runtime, [`RemoteBackend`] drives a `loonfs-client`
//! over HTTP — and the commands above this seam cannot tell which they got.
//!
//! The seam is private to this crate on purpose. It exists so `loon` can run
//! its features against either transport, not as an extension point: an
//! application embedding LoonFS programs against `loonfs` (runtime) or
//! `loonfs-client` (HTTP) directly, and neither needs a trait between.
//!
//! The trait is async so the CLI drives both transports from its own runtime.
//! [`loonfs_client::Client`] is itself async, so [`RemoteBackend`] is a thin
//! adapter that maps the client's error type onto [`BackendError`].

mod embedded;
mod remote;

use crate::backend_error::BackendError;
use async_trait::async_trait;
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
pub(crate) use remote::RemoteBackend;

/// One logical LoonFS API over two transports.
///
/// Implementations must report the same registry error code for the same
/// failure, so a command renders identical outcomes regardless of which
/// transport a profile selects (this crate's two-mode parity tests hold this
/// line).
#[async_trait]
pub(crate) trait Backend {
    /// Creates a new empty namespace.
    async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceSummary, BackendError>;
    /// Marks a namespace deleted; `expected_head_seq` guards against deleting
    /// a namespace that moved since the caller last observed it.
    async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
        expected_head_seq: Option<ChangeSeq>,
    ) -> Result<DeleteNamespaceResponse, BackendError>;
    /// Creates a new namespace as a fork of the source's durable view.
    async fn fork_namespace(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
    ) -> Result<NamespaceSummary, BackendError>;
    /// Summarizes a namespace's current head state.
    async fn namespace_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceStatusResponse, BackendError>;
    /// Lists the entries of a directory.
    async fn list_path_entries_all(
        &self,
        spec: &NamespacePath,
    ) -> Result<Vec<AuthoritativePathEntry>, BackendError>;
    /// Describes a single path entry.
    async fn stat_path(&self, spec: &NamespacePath)
        -> Result<AuthoritativePathEntry, BackendError>;
    /// Reads a file's current content.
    async fn get_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, BackendError>;
    /// Content search over a namespace's grep index.
    async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
    ) -> Result<GrepResponse, BackendError>;
    /// Enables the grep index on a namespace (admin plane).
    async fn enable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<EnableGrepIndexResponse, BackendError>;
    /// Disables the grep index on a namespace (admin plane).
    async fn disable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<DisableGrepIndexResponse, BackendError>;
    /// Reads a retained file revision's content.
    async fn get_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, BackendError>;
    /// Lists one page of the namespace's recoverable deletions.
    async fn list_trash(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListTrashResponse, BackendError>;

    /// Lists one page of a file's retained revisions.
    async fn list_file_revisions_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse, BackendError>;
    /// Writes a file; `behavior` selects create-only or replace semantics.
    /// An explicit `commit_id` makes the call retryable by resubmission;
    /// absent, one is generated and returned in the response.
    async fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        options: &PutFileOptions,
    ) -> Result<CommitResponse, BackendError>;
    /// Creates a directory; `parents` also creates missing ancestors.
    async fn create_directory(
        &self,
        spec: &NamespacePath,
        options: &CreateDirectoryOptions,
    ) -> Result<CommitResponse, BackendError>;
    /// Deletes a file or empty directory. With `expected_inode_id`, the
    /// delete applies only while the path still resolves to that inode, so
    /// callers reporting a recovery handle never report a raced rebinding.
    async fn delete_path(
        &self,
        spec: &NamespacePath,
        options: &DeleteOptions,
    ) -> Result<CommitResponse, BackendError>;
    /// Moves a path within a namespace; `behavior` selects create-only or
    /// replace semantics for the destination.
    async fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        behavior: DestinationBehavior,
        options: &MutationOptions,
    ) -> Result<CommitResponse, BackendError>;
    /// Copies a file within a namespace; `behavior` selects create-only or
    /// replace semantics for the destination.
    async fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        behavior: DestinationBehavior,
        options: &MutationOptions,
    ) -> Result<CommitResponse, BackendError>;
    /// Restores a file to one of its retained revisions.
    async fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
        options: &MutationOptions,
    ) -> Result<CommitResponse, BackendError>;
    /// Recovers a deleted file or subtree to the spec's path; `inode_id`
    /// and `deleted_at_seq` are the identity and committed sequence the
    /// delete reported.
    async fn undelete(
        &self,
        spec: &NamespacePath,
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        options: &MutationOptions,
    ) -> Result<CommitResponse, BackendError>;

    // --- maintenance/admin plane (`admin/v0`) ---

    /// Creates or reuses a named, user-owned checkpoint pinning the
    /// namespace's current view.
    async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        request: CreateCheckpointRequest,
    ) -> Result<CreateCheckpointResponse, BackendError>;
    /// Releases a user-owned checkpoint pin by id. Idempotent.
    async fn release_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<ReleaseCheckpointResponse, BackendError>;
    /// Runs one bounded maintenance step: WAL flush, metadata
    /// reorganization, retention advance, and — when `request.gc` opts in —
    /// one garbage-collection pass. `request.only` restricts it to a single
    /// sub-step.
    async fn maintenance_step(
        &self,
        namespace_id: &NamespaceId,
        request: MaintenanceStepRequest,
    ) -> Result<MaintenanceStepResponse, BackendError>;
    /// Reads the ordered change feed after the `after_seq` cursor.
    async fn list_changes(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        limit: Option<u32>,
    ) -> Result<ChangesResponse, BackendError>;
}
