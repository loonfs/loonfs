//! Staged uploads and direct-put targets.
//!
//! Every session opened here creates a wall-clock obligation nothing else
//! would notice: a lease that will expire, or completed content whose
//! reclamation grace will pass. Each path plants that deadline on the
//! garbage-collection job as a not-before time, so the pass that reclaims
//! the session is admitted by the clock rather than by the next unrelated
//! write to the namespace.

use crate::content_tokens::{CompletedUpload, CompletedUploadReceipt};
use crate::maintenance_runner::{completed_upload_reclaim_at_ms, upload_session_reclaim_at_ms};
use crate::uploads::{
    BeginDirectMultipartUploadTargetResponse, BeginDirectPutUploadTargetResponse,
    MultipartPartTargets,
};
use crate::ByteStream;
use crate::FsWriter;
use crate::Result;
use crate::{
    BeginUploadRequest, BeginUploadResponse, CompleteUploadRequest, CompleteUploadResponse,
    MaintenanceJobId, NamespaceId, UploadContentClaim, UploadContentResponse,
};
use loonfs_api::v0::{
    AbortUploadResponse, DirectMultipartUploadOptions, UploadPartChecksumClaim,
    UploadStatusResponse,
};
use loonfs_api::UploadId;

impl FsWriter {
    /// Plants the deadline a durable upload session just created.
    ///
    /// The clock is read after the session is durable, so the scheduled time
    /// can only land after the collector's own predicate. Landing early is
    /// the one failure that would cost something: the pass would find the
    /// session retained, park, and have nothing left to bring it back.
    fn schedule_upload_session_reclamation(&self, namespace_id: &NamespaceId) {
        let handle = self.bits.maintenance.handle();
        let reclaim_at_ms = upload_session_reclaim_at_ms(handle.now_ms());
        handle.nudge_not_before(MaintenanceJobId::GC, namespace_id, reclaim_at_ms);
    }

    /// Plants the deadline a completed session's content just created. The
    /// session record itself outlives completion — only a collection pass
    /// removes it — so completion schedules its own pass rather than
    /// relying on the one the session's lease already asked for.
    fn schedule_completed_upload_reclamation(&self, namespace_id: &NamespaceId) {
        let handle = self.bits.maintenance.handle();
        let reclaim_at_ms = completed_upload_reclaim_at_ms(handle.now_ms());
        handle.nudge_not_before(MaintenanceJobId::GC, namespace_id, reclaim_at_ms);
    }

    /// Starts a durable upload session for a namespace.
    pub async fn begin_upload(
        &self,
        namespace_id: &NamespaceId,
        request: BeginUploadRequest,
    ) -> Result<BeginUploadResponse> {
        let response = self.engine(namespace_id).begin_upload(request).await?;
        self.schedule_upload_session_reclamation(namespace_id);
        Ok(response)
    }

    /// Mints the content object a direct upload will write to and returns
    /// the internal target for server-side signing.
    pub async fn begin_direct_put_upload_target(
        &self,
        namespace_id: &NamespaceId,
        claim: UploadContentClaim,
    ) -> Result<BeginDirectPutUploadTargetResponse> {
        let response = self
            .engine(namespace_id)
            .begin_direct_put_upload_target(claim)
            .await?;
        self.schedule_upload_session_reclamation(namespace_id);
        Ok(response)
    }

    /// Mints the content object a direct multipart upload assembles into,
    /// opens the provider upload behind it, and returns the internal target
    /// for server-side signing.
    pub async fn begin_direct_multipart_upload_target(
        &self,
        namespace_id: &NamespaceId,
        options: DirectMultipartUploadOptions,
    ) -> Result<BeginDirectMultipartUploadTargetResponse> {
        let response = self
            .engine(namespace_id)
            .begin_direct_multipart_upload_target(options)
            .await?;
        self.schedule_upload_session_reclamation(namespace_id);
        Ok(response)
    }

    /// Resolves one wave of multipart parts for server-side signing.
    pub async fn direct_multipart_part_targets(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        requested: &[UploadPartChecksumClaim],
    ) -> Result<MultipartPartTargets> {
        Ok(self
            .engine(namespace_id)
            .direct_multipart_part_targets(upload_id, requested)
            .await?)
    }

    /// Uploads whole-file content into an upload session.
    pub async fn upload_content(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> Result<UploadContentResponse> {
        Ok(self
            .engine(namespace_id)
            .upload_content(upload_id, bytes)
            .await?)
    }

    /// Uploads content that arrives as a stream into an upload session.
    ///
    /// The payload is hashed as it is forwarded to object storage rather
    /// than held, so memory follows the transfer's part size instead of the
    /// object's length. Everything after this point — the reference it
    /// produces, completion, publication — is identical to the buffered
    /// path's; callers that already hold their bytes should stay on
    /// [`Self::upload_content`].
    pub async fn upload_streamed_content(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        body: ByteStream,
    ) -> Result<UploadContentResponse> {
        Ok(self
            .engine(namespace_id)
            .upload_streamed_content(upload_id, body)
            .await?)
    }

    /// Completes an upload session when the expected content ref matches.
    pub async fn complete_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> Result<CompleteUploadResponse> {
        Ok(self
            .complete_upload_prepared(namespace_id, upload_id, request)
            .await?
            .response)
    }

    /// Completes an upload session and returns proof for later publication.
    ///
    /// Service-proxied completion performs no content-blob I/O. Direct-put
    /// completion performs one content-blob HEAD and no content-blob GET.
    pub async fn complete_upload_prepared(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> Result<CompletedUpload> {
        let catalog = self
            .load_namespace_catalog_for_content_preparation(namespace_id)
            .await?;
        let completed = self
            .engine(namespace_id)
            .complete_upload_prepared_with_catalog(&catalog, upload_id, request)
            .await?;
        self.schedule_completed_upload_reclamation(namespace_id);
        Ok(completed)
    }

    /// Aborts an upload session and deletes the content object it owned.
    pub async fn abort_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> Result<AbortUploadResponse> {
        Ok(self.engine(namespace_id).abort_upload(upload_id).await?)
    }

    /// Reads one upload session, with a fresh receipt when it is completed.
    pub async fn read_upload_status(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> Result<(UploadStatusResponse, Option<CompletedUploadReceipt>)> {
        Ok(self
            .engine(namespace_id)
            .read_upload_status(upload_id)
            .await?)
    }
}
