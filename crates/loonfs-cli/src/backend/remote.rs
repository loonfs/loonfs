//! HTTP implementation of the CLI's [`Backend`] seam.
//!
//! Every method forwards to [`Client`] and maps its error type onto
//! [`BackendError`]; the client already owns the wire protocol, retry policy,
//! and error envelope, so nothing here adds behavior.

use super::Backend;
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
    Client, CreateDirectoryOptions, DeleteOptions, MutationOptions, NamespacePath, PutFileOptions,
};

/// [`Backend`] over HTTP: wraps a [`Client`] pointed at a LoonFS server.
#[derive(Debug)]
pub(crate) struct RemoteBackend {
    client: Client,
}

impl RemoteBackend {
    /// Wraps a configured HTTP client.
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl Backend for RemoteBackend {
    async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceSummary, BackendError> {
        self.client
            .create_namespace(namespace_id)
            .await
            .map_err(BackendError::from)
    }

    async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
        expected_head_seq: Option<ChangeSeq>,
    ) -> Result<DeleteNamespaceResponse, BackendError> {
        self.client
            .delete_namespace(namespace_id, expected_head_seq)
            .await
            .map_err(BackendError::from)
    }

    async fn fork_namespace(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
    ) -> Result<NamespaceSummary, BackendError> {
        self.client
            .fork_namespace(source_namespace_id, new_namespace_id)
            .await
            .map_err(BackendError::from)
    }

    async fn namespace_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceStatusResponse, BackendError> {
        self.client
            .namespace_status(namespace_id)
            .await
            .map_err(BackendError::from)
    }

    async fn list_path_entries_all(
        &self,
        spec: &NamespacePath,
    ) -> Result<Vec<AuthoritativePathEntry>, BackendError> {
        Ok(self
            .client
            .list_path_entries_all(spec)
            .await
            .map_err(BackendError::from)?
            .entries)
    }

    async fn stat_path(
        &self,
        spec: &NamespacePath,
    ) -> Result<AuthoritativePathEntry, BackendError> {
        self.client
            .stat_path(spec)
            .await
            .map_err(BackendError::from)
    }

    async fn get_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, BackendError> {
        self.client
            .get_file_bytes(spec)
            .await
            .map_err(BackendError::from)
    }

    async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
    ) -> Result<GrepResponse, BackendError> {
        self.client
            .grep(namespace_id, request)
            .await
            .map_err(BackendError::from)
    }

    async fn enable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<EnableGrepIndexResponse, BackendError> {
        self.client
            .enable_grep_index(namespace_id)
            .await
            .map_err(BackendError::from)
    }

    async fn disable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<DisableGrepIndexResponse, BackendError> {
        self.client
            .disable_grep_index(namespace_id)
            .await
            .map_err(BackendError::from)
    }

    async fn get_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, BackendError> {
        self.client
            .get_file_revision_bytes(spec, revision_no)
            .await
            .map_err(BackendError::from)
    }

    async fn list_trash(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListTrashResponse, BackendError> {
        self.client
            .list_trash_page(namespace_id, limit, cursor)
            .await
            .map_err(BackendError::from)
    }

    async fn list_file_revisions_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse, BackendError> {
        let cursor = cursor.map(ToOwned::to_owned);
        self.client
            .list_file_revisions_page(spec, limit, cursor.as_deref())
            .await
            .map_err(BackendError::from)
    }

    async fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        options: &PutFileOptions,
    ) -> Result<CommitResponse, BackendError> {
        let bytes = bytes.to_vec();
        self.client
            .put_file_bytes(spec, &bytes, options)
            .await
            .map_err(BackendError::from)
    }

    async fn create_directory(
        &self,
        spec: &NamespacePath,
        options: &CreateDirectoryOptions,
    ) -> Result<CommitResponse, BackendError> {
        self.client
            .create_directory(spec, options)
            .await
            .map_err(BackendError::from)
    }

    async fn delete_path(
        &self,
        spec: &NamespacePath,
        options: &DeleteOptions,
    ) -> Result<CommitResponse, BackendError> {
        self.client
            .delete_path(spec, options)
            .await
            .map_err(BackendError::from)
    }

    async fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        behavior: DestinationBehavior,
        options: &MutationOptions,
    ) -> Result<CommitResponse, BackendError> {
        self.client
            .move_path(from, to, behavior, options)
            .await
            .map_err(BackendError::from)
    }

    async fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        behavior: DestinationBehavior,
        options: &MutationOptions,
    ) -> Result<CommitResponse, BackendError> {
        self.client
            .copy_path(from, to, behavior, options)
            .await
            .map_err(BackendError::from)
    }

    async fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
        options: &MutationOptions,
    ) -> Result<CommitResponse, BackendError> {
        self.client
            .restore_file_revision(spec, source_revision_no, options)
            .await
            .map_err(BackendError::from)
    }

    async fn undelete(
        &self,
        spec: &NamespacePath,
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        options: &MutationOptions,
    ) -> Result<CommitResponse, BackendError> {
        self.client
            .undelete(spec, inode_id, deleted_at_seq, options)
            .await
            .map_err(BackendError::from)
    }

    async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        request: CreateCheckpointRequest,
    ) -> Result<CreateCheckpointResponse, BackendError> {
        self.client
            .create_checkpoint(namespace_id, &request)
            .await
            .map_err(BackendError::from)
    }

    async fn release_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<ReleaseCheckpointResponse, BackendError> {
        self.client
            .release_checkpoint(namespace_id, checkpoint_id)
            .await
            .map_err(BackendError::from)
    }

    async fn maintenance_step(
        &self,
        namespace_id: &NamespaceId,
        request: MaintenanceStepRequest,
    ) -> Result<MaintenanceStepResponse, BackendError> {
        self.client
            .maintenance_step(namespace_id, &request)
            .await
            .map_err(BackendError::from)
    }

    async fn list_changes(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        limit: Option<u32>,
    ) -> Result<ChangesResponse, BackendError> {
        self.client
            .list_changes(namespace_id, after_seq, limit)
            .await
            .map_err(BackendError::from)
    }
}
