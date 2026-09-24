//! Staged uploads and direct-put targets.
//!
//! Every session opened here creates a wall-clock obligation nothing else
//! would notice: a lease that will expire, or completed content whose
//! reclamation grace will pass. Each path plants that deadline on the
//! garbage-collection job as a not-before time, so the pass that reclaims
//! the session is admitted by the clock rather than by the next unrelated
//! write to the namespace. An attached runner may admit the hinted deadline.

use crate::content_tokens::CompletedUpload;
use crate::maintenance::{completed_upload_reclaim_at_ms, upload_session_reclaim_at_ms};
use crate::uploads::{
    BeginDirectMultipartUploadTargetResponse, BeginDirectPutUploadTargetResponse,
    MultipartPartTargets, ResolvedUploadCompletion, UploadSessionView,
};
use crate::ByteStream;
use crate::FsWriter;
use crate::Result;
use crate::{
    ChecksumAlgorithm, MaintenanceHint, MaintenanceJobId, NamespaceId, UploadMode, UploadSession,
};
use loonfs_api::options::DirectMultipartUploadOptions;
use loonfs_api::v0::UploadPartChecksumClaim;
use loonfs_api::{Subject, UploadId};

impl FsWriter {
    /// A scoped handle acts as its own subject; only an unscoped handle acts as the caller's.
    fn scoped_subject<'a>(&'a self, subject: Option<&'a Subject>) -> Option<&'a Subject> {
        self.core.subject.as_ref().or(subject)
    }

    /// Plants the deadline a durable upload session just created.
    ///
    /// The clock is read after the session is durable, so the scheduled time
    /// can only land after the collector's own predicate. Landing early is
    /// the one failure that would cost something: the pass would find the
    /// session retained, park, and have nothing left to bring it back.
    fn schedule_upload_session_reclamation(&self, namespace_id: &NamespaceId) {
        if self.bits.maintenance_hint_observer.is_none() {
            return;
        }
        let Ok(now_ms) = loonfs_core::time::current_time_ms() else {
            return;
        };
        self.bits.send_maintenance_hint(
            namespace_id,
            MaintenanceHint::DueAt {
                namespace_id: namespace_id.clone(),
                job: MaintenanceJobId::GC,
                not_before_ms: upload_session_reclaim_at_ms(now_ms),
            },
        );
    }

    /// Plants the deadline a completed session's content just created. The
    /// session record itself outlives completion — only a collection pass
    /// removes it — so completion schedules its own pass rather than
    /// relying on the one the session's lease already asked for.
    fn schedule_completed_upload_reclamation(&self, namespace_id: &NamespaceId) {
        if self.bits.maintenance_hint_observer.is_none() {
            return;
        }
        let Ok(now_ms) = loonfs_core::time::current_time_ms() else {
            return;
        };
        self.bits.send_maintenance_hint(
            namespace_id,
            MaintenanceHint::DueAt {
                namespace_id: namespace_id.clone(),
                job: MaintenanceJobId::GC,
                not_before_ms: completed_upload_reclaim_at_ms(now_ms),
            },
        );
    }

    /// Starts a durable upload session for a namespace.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.begin_upload",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "begin_upload",
            method = "create_upload",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn create_upload(
        &self,
        namespace_id: &NamespaceId,
        subject: Option<&Subject>,
    ) -> Result<UploadSession> {
        let subject = self.scoped_subject(subject);
        self.core.record_trace_context(&tracing::Span::current());
        let response = self.engine(namespace_id).begin_upload(subject).await?;
        self.schedule_upload_session_reclamation(namespace_id);
        Ok(response)
    }

    /// Mints the content object a direct upload will write to and returns
    /// the internal target for server-side signing.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.begin_upload",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "begin_upload",
            method = "create_direct_put_upload_target",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn create_direct_put_upload_target(
        &self,
        namespace_id: &NamespaceId,
        subject: Option<&Subject>,
        checksum_algorithm: ChecksumAlgorithm,
    ) -> Result<BeginDirectPutUploadTargetResponse> {
        let subject = self.scoped_subject(subject);
        self.core.record_trace_context(&tracing::Span::current());
        let response = self
            .engine(namespace_id)
            .begin_direct_put_upload_target(subject, checksum_algorithm)
            .await?;
        self.schedule_upload_session_reclamation(namespace_id);
        Ok(response)
    }

    /// Mints the content object a direct multipart upload assembles into,
    /// opens the provider upload behind it, and returns the internal target
    /// for server-side signing.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.begin_upload",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "begin_upload",
            method = "create_direct_multipart_upload_target",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn create_direct_multipart_upload_target(
        &self,
        namespace_id: &NamespaceId,
        subject: Option<&Subject>,
        options: DirectMultipartUploadOptions,
    ) -> Result<BeginDirectMultipartUploadTargetResponse> {
        let subject = self.scoped_subject(subject);
        self.core.record_trace_context(&tracing::Span::current());
        let response = self
            .engine(namespace_id)
            .begin_direct_multipart_upload_target(subject, options)
            .await?;
        self.schedule_upload_session_reclamation(namespace_id);
        Ok(response)
    }

    /// Resolves one wave of multipart parts for server-side signing.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.sign_upload_parts",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "sign_upload_parts",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn sign_upload_parts(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        subject: Option<&Subject>,
        requested: &[UploadPartChecksumClaim],
    ) -> Result<MultipartPartTargets> {
        let subject = self.scoped_subject(subject);
        self.core.record_trace_context(&tracing::Span::current());
        Ok(self
            .engine(namespace_id)
            .direct_multipart_part_targets(upload_id, subject, requested)
            .await?)
    }

    /// Uploads whole-file content into an upload session.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.upload_content",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "upload_content",
            method = "put_upload_content",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    pub async fn put_upload_content(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        subject: Option<&Subject>,
        bytes: &[u8],
    ) -> Result<UploadSession> {
        let subject = self.scoped_subject(subject);
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        span.record("payload_class", crate::trace::payload_class(bytes.len()));
        Ok(self
            .engine(namespace_id)
            .upload_content(upload_id, subject, bytes)
            .await?)
    }

    /// Uploads content that arrives as a stream into an upload session.
    ///
    /// The payload is hashed as it is forwarded to object storage rather
    /// than held, so memory follows the transfer's part size instead of the
    /// object's length. Everything after this point — the reference it
    /// produces, completion, publication — is identical to the buffered
    /// path's; callers that already hold their bytes should stay on
    /// [`Self::put_upload_content`].
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.upload_content",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "upload_content",
            method = "put_upload_content_stream",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = "streamed",
        )
    )]
    pub async fn put_upload_content_stream(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        subject: Option<&Subject>,
        body: ByteStream,
    ) -> Result<UploadSession> {
        let subject = self.scoped_subject(subject);
        self.core.record_trace_context(&tracing::Span::current());
        Ok(self
            .engine(namespace_id)
            .upload_streamed_content(upload_id, subject, body)
            .await?)
    }

    /// Completes an upload session and returns proof for later publication.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.complete_upload",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "complete_upload",
            method = "complete_upload",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn complete_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        subject: Option<&Subject>,
        completion: ResolvedUploadCompletion,
    ) -> Result<CompletedUpload> {
        let subject = self.scoped_subject(subject);
        self.core.record_trace_context(&tracing::Span::current());
        let catalog = self
            .load_namespace_catalog_for_content_preparation(namespace_id)
            .await?;
        let completed = self
            .engine(namespace_id)
            .complete_upload(&catalog, upload_id, subject, completion)
            .await?;
        self.schedule_completed_upload_reclamation(namespace_id);
        Ok(completed)
    }

    /// Completes an upload after decoding its request for the stored mode.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.complete_upload",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "complete_upload",
            method = "complete_upload_for_mode",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn complete_upload_for_mode<F>(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        subject: Option<&Subject>,
        resolve: F,
    ) -> Result<CompletedUpload>
    where
        F: FnOnce(
            UploadMode,
        ) -> std::result::Result<crate::uploads::ResolvedUploadCompletion, String>,
    {
        let subject = self.scoped_subject(subject);
        self.core.record_trace_context(&tracing::Span::current());
        let catalog = self
            .load_namespace_catalog_for_content_preparation(namespace_id)
            .await?;
        let completed = self
            .engine(namespace_id)
            .complete_upload_for_mode(&catalog, upload_id, subject, resolve)
            .await?;
        self.schedule_completed_upload_reclamation(namespace_id);
        Ok(completed)
    }

    /// Aborts an upload session and deletes the content object it owned.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.abort_upload",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "abort_upload",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn abort_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        subject: Option<&Subject>,
    ) -> Result<UploadSession> {
        let subject = self.scoped_subject(subject);
        self.core.record_trace_context(&tracing::Span::current());
        Ok(self
            .engine(namespace_id)
            .abort_upload(upload_id, subject)
            .await?)
    }

    /// Returns an upload session and a new receipt when the upload is complete.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.get_upload_status",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "get_upload_status",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn get_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        subject: Option<&Subject>,
    ) -> Result<UploadSessionView> {
        let subject = self.scoped_subject(subject);
        self.core.record_trace_context(&tracing::Span::current());
        Ok(self
            .engine(namespace_id)
            .get_upload_status(upload_id, subject)
            .await?)
    }
}
