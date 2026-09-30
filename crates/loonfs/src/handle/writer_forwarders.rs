//! [`FsWriter`]'s deprecated methods that take a namespace id. Each one
//! forwards to the [`NamespaceWriter`] method of the same name.

use super::{FsWriter, NamespaceWriter};
use crate::content_tokens::{CompletedUpload, ContentToken, ContentTokenError};
use crate::publish::{CommitCandidate, CommitRequest, PreparedContent};
use crate::uploads::{
    BeginDirectMultipartUploadTargetResponse, BeginDirectPutUploadTargetResponse,
    MultipartPartTargets, ResolvedUploadCompletion, UploadSessionView,
};
use crate::{
    ActorId, ByteStream, ChangeSeq, Checkpoint, ChecksumAlgorithm, Commit, ContentRef, CopyOptions,
    CreateDirectoryOptions, CreateSnapshotOptions, DeleteNamespaceOptions, DeleteNamespaceResponse,
    DeleteOptions, DeleteSnapshotResponse, DirectMultipartUploadOptions, InodeId, MoveOptions,
    NamespaceId, PinId, PutFileOptions, RestoreRevisionOptions, Result, RevisionNo,
    SnapshotSummary, UndeleteOptions, UpdateAccessOptions, UpdateAttributesOptions, UploadId,
    UploadMode, UploadSession,
};
use loonfs_api::v0::UploadPartChecksumClaim;
use loonfs_api::{PrincipalId, RecoverAdministratorResponse};

impl FsWriter {
    /// Forwards to [`NamespaceWriter::wait_for_fold`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn wait_for_fold(&self, namespace_id: &NamespaceId) -> Result<()> {
        NamespaceWriter::new(self, namespace_id)
            .wait_for_fold()
            .await
    }

    /// Forwards to [`NamespaceWriter::delete_namespace`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        NamespaceWriter::new(self, namespace_id)
            .delete_namespace(options)
            .await
    }

    /// Forwards to [`NamespaceWriter::put_file_bytes`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn put_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        options: PutFileOptions,
    ) -> Result<Commit> {
        NamespaceWriter::new(self, namespace_id)
            .put_file_bytes(absolute_path, bytes, options)
            .await
    }

    /// Forwards to [`NamespaceWriter::put_file_stream`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn put_file_stream(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        body: ByteStream,
        options: PutFileOptions,
    ) -> Result<Commit> {
        NamespaceWriter::new(self, namespace_id)
            .put_file_stream(absolute_path, body, options)
            .await
    }

    /// Forwards to [`NamespaceWriter::prepare_file_bytes`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn prepare_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) -> Result<PreparedContent> {
        NamespaceWriter::new(self, namespace_id)
            .prepare_file_bytes(bytes)
            .await
    }

    /// Forwards to [`NamespaceWriter::prepare_file_stream`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn prepare_file_stream(
        &self,
        namespace_id: &NamespaceId,
        body: ByteStream,
    ) -> Result<PreparedContent> {
        NamespaceWriter::new(self, namespace_id)
            .prepare_file_stream(body)
            .await
    }

    /// Forwards to [`NamespaceWriter::put_file_prepared`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn put_file_prepared(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        prepared_content: PreparedContent,
        options: PutFileOptions,
    ) -> Result<Commit> {
        NamespaceWriter::new(self, namespace_id)
            .put_file_prepared(absolute_path, prepared_content, options)
            .await
    }

    /// Forwards to [`NamespaceWriter::put_file_content_ref`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn put_file_content_ref(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        content_ref: ContentRef,
        options: PutFileOptions,
    ) -> Result<Commit> {
        NamespaceWriter::new(self, namespace_id)
            .put_file_content_ref(absolute_path, content_ref, options)
            .await
    }

    /// Forwards to [`NamespaceWriter::prepare_content_ref`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn prepare_content_ref(
        &self,
        namespace_id: &NamespaceId,
        content_ref: ContentRef,
    ) -> Result<PreparedContent> {
        NamespaceWriter::new(self, namespace_id)
            .prepare_content_ref(content_ref)
            .await
    }

    /// Forwards to [`NamespaceWriter::prepare_content_token`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn prepare_content_token(
        &self,
        namespace_id: &NamespaceId,
        secret: &str,
        token: &ContentToken,
        now_ms: u64,
    ) -> Result<std::result::Result<PreparedContent, ContentTokenError>> {
        NamespaceWriter::new(self, namespace_id)
            .prepare_content_token(secret, token, now_ms)
            .await
    }

    /// Forwards to [`NamespaceWriter::create_directory`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn create_directory(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirectoryOptions,
    ) -> Result<Commit> {
        NamespaceWriter::new(self, namespace_id)
            .create_directory(absolute_path, options)
            .await
    }

    /// Forwards to [`NamespaceWriter::delete_path`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn delete_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: DeleteOptions,
    ) -> Result<Commit> {
        NamespaceWriter::new(self, namespace_id)
            .delete_path(absolute_path, options)
            .await
    }

    /// Forwards to [`NamespaceWriter::move_path`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn move_path(
        &self,
        namespace_id: &NamespaceId,
        source_path: &str,
        destination_path: &str,
        options: MoveOptions,
    ) -> Result<Commit> {
        NamespaceWriter::new(self, namespace_id)
            .move_path(source_path, destination_path, options)
            .await
    }

    /// Forwards to [`NamespaceWriter::copy_path`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn copy_path(
        &self,
        namespace_id: &NamespaceId,
        source_path: &str,
        destination_path: &str,
        options: CopyOptions,
    ) -> Result<Commit> {
        NamespaceWriter::new(self, namespace_id)
            .copy_path(source_path, destination_path, options)
            .await
    }

    /// Forwards to [`NamespaceWriter::restore_revision`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn restore_revision(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        source_revision_no: RevisionNo,
        options: RestoreRevisionOptions,
    ) -> Result<Commit> {
        NamespaceWriter::new(self, namespace_id)
            .restore_revision(absolute_path, source_revision_no, options)
            .await
    }

    /// Forwards to [`NamespaceWriter::update_attributes`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn update_attributes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: UpdateAttributesOptions,
    ) -> Result<Commit> {
        NamespaceWriter::new(self, namespace_id)
            .update_attributes(absolute_path, options)
            .await
    }

    /// Forwards to [`NamespaceWriter::update_access`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn update_access(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: UpdateAccessOptions,
    ) -> Result<Commit> {
        NamespaceWriter::new(self, namespace_id)
            .update_access(absolute_path, options)
            .await
    }

    /// Forwards to [`NamespaceWriter::undelete`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn undelete(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        deletion_seq: ChangeSeq,
        destination_path: Option<&str>,
        options: UndeleteOptions,
    ) -> Result<Commit> {
        NamespaceWriter::new(self, namespace_id)
            .undelete(inode_id, deletion_seq, destination_path, options)
            .await
    }

    /// Forwards to [`NamespaceWriter::create_commit`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn create_commit(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> Result<Commit> {
        NamespaceWriter::new(self, namespace_id)
            .create_commit(request)
            .await
    }

    /// Forwards to [`NamespaceWriter::commit_prepared`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn commit_prepared(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
        prepared_content: Vec<PreparedContent>,
    ) -> Result<Commit> {
        NamespaceWriter::new(self, namespace_id)
            .commit_prepared(request, prepared_content)
            .await
    }

    /// Forwards to [`NamespaceWriter::commit_candidate`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn commit_candidate(
        &self,
        namespace_id: &NamespaceId,
        candidate: CommitCandidate,
    ) -> Result<Commit> {
        NamespaceWriter::new(self, namespace_id)
            .commit_candidate(candidate)
            .await
    }

    /// Forwards to [`NamespaceWriter::create_upload`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn create_upload(&self, namespace_id: &NamespaceId) -> Result<UploadSession> {
        NamespaceWriter::new(self, namespace_id)
            .create_upload()
            .await
    }

    /// Forwards to [`NamespaceWriter::create_direct_put_upload_target`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn create_direct_put_upload_target(
        &self,
        namespace_id: &NamespaceId,
        checksum_algorithm: ChecksumAlgorithm,
    ) -> Result<BeginDirectPutUploadTargetResponse> {
        NamespaceWriter::new(self, namespace_id)
            .create_direct_put_upload_target(checksum_algorithm)
            .await
    }

    /// Forwards to [`NamespaceWriter::create_direct_multipart_upload_target`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn create_direct_multipart_upload_target(
        &self,
        namespace_id: &NamespaceId,
        options: DirectMultipartUploadOptions,
    ) -> Result<BeginDirectMultipartUploadTargetResponse> {
        NamespaceWriter::new(self, namespace_id)
            .create_direct_multipart_upload_target(options)
            .await
    }

    /// Forwards to [`NamespaceWriter::sign_upload_parts`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn sign_upload_parts(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        requested: &[UploadPartChecksumClaim],
    ) -> Result<MultipartPartTargets> {
        NamespaceWriter::new(self, namespace_id)
            .sign_upload_parts(upload_id, requested)
            .await
    }

    /// Forwards to [`NamespaceWriter::put_upload_content`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn put_upload_content(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> Result<UploadSession> {
        NamespaceWriter::new(self, namespace_id)
            .put_upload_content(upload_id, bytes)
            .await
    }

    /// Forwards to [`NamespaceWriter::put_upload_content_stream`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn put_upload_content_stream(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        body: ByteStream,
    ) -> Result<UploadSession> {
        NamespaceWriter::new(self, namespace_id)
            .put_upload_content_stream(upload_id, body)
            .await
    }

    /// Forwards to [`NamespaceWriter::complete_upload`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn complete_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        completion: ResolvedUploadCompletion,
    ) -> Result<CompletedUpload> {
        NamespaceWriter::new(self, namespace_id)
            .complete_upload(upload_id, completion)
            .await
    }

    /// Forwards to [`NamespaceWriter::complete_upload_for_mode`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn complete_upload_for_mode<F>(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        resolve: F,
    ) -> Result<CompletedUpload>
    where
        F: FnOnce(UploadMode) -> std::result::Result<ResolvedUploadCompletion, String>,
    {
        NamespaceWriter::new(self, namespace_id)
            .complete_upload_for_mode(upload_id, resolve)
            .await
    }

    /// Forwards to [`NamespaceWriter::abort_upload`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn abort_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> Result<UploadSession> {
        NamespaceWriter::new(self, namespace_id)
            .abort_upload(upload_id)
            .await
    }

    /// Forwards to [`NamespaceWriter::get_upload`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn get_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> Result<UploadSessionView> {
        NamespaceWriter::new(self, namespace_id)
            .get_upload(upload_id)
            .await
    }

    /// Forwards to [`NamespaceWriter::create_snapshot`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn create_snapshot(
        &self,
        namespace_id: &NamespaceId,
        options: CreateSnapshotOptions,
        max_live: usize,
    ) -> Result<Checkpoint> {
        NamespaceWriter::new(self, namespace_id)
            .create_snapshot(options, max_live)
            .await
    }

    /// Forwards to [`NamespaceWriter::extend_snapshot`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn extend_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &PinId,
        requested_expires_at_ms: u64,
        max_lifetime_ms: u64,
    ) -> Result<SnapshotSummary> {
        NamespaceWriter::new(self, namespace_id)
            .extend_snapshot(snapshot_id, requested_expires_at_ms, max_lifetime_ms)
            .await
    }

    /// Forwards to [`NamespaceWriter::delete_snapshot`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn delete_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &PinId,
    ) -> Result<DeleteSnapshotResponse> {
        NamespaceWriter::new(self, namespace_id)
            .delete_snapshot(snapshot_id)
            .await
    }

    /// Forwards to [`NamespaceWriter::recover_administrator`].
    #[deprecated(note = "open the namespace and call this on the NamespaceWriter")]
    pub async fn recover_administrator(
        &self,
        namespace_id: &NamespaceId,
        principal_id: &PrincipalId,
        actor_id: ActorId,
    ) -> Result<RecoverAdministratorResponse> {
        NamespaceWriter::new(self, namespace_id)
            .recover_administrator(principal_id, actor_id)
            .await
    }
}
