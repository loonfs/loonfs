//! Staged uploads and direct-put targets.

use crate::content_tokens::CompletedUpload;
use crate::trace::{self, operation_span};
use crate::uploads::{
    DirectMultipartUploadTarget, DirectPutUploadTarget, MultipartPartTargets, UploadSessionView,
};
use crate::ByteStream;
use crate::Result;
use crate::{CompleteUploadBody, UploadSession};
use crate::{Namespace, Writable};
use loonfs_types::options::DirectMultipartUploadOptions;
use loonfs_types::u64_from_usize;
use loonfs_types::UploadId;
use loonfs_types::UploadPartChecksumClaim;

impl Namespace<Writable> {
    /// Starts a durable upload session for a namespace.
    pub async fn create_upload(&self) -> Result<UploadSession> {
        trace::in_operation(
            operation_span!(self.core, "create_upload", self.namespace_id),
            async { Ok(self.engine().begin_upload().await?) },
        )
        .await
    }

    /// Mints the content object a direct upload will write to and returns
    /// the internal target for server-side signing.
    pub async fn create_direct_put_upload_target(&self) -> Result<DirectPutUploadTarget> {
        trace::in_operation(
            operation_span!(
                self.core,
                "create_direct_put_upload_target",
                self.namespace_id
            ),
            async { Ok(self.engine().begin_direct_put_upload_target().await?) },
        )
        .await
    }

    /// Runs [`Self::create_direct_multipart_upload_target_with_options`]
    /// with the default part size.
    pub async fn create_direct_multipart_upload_target(
        &self,
    ) -> Result<DirectMultipartUploadTarget> {
        self.create_direct_multipart_upload_target_with_options(
            &DirectMultipartUploadOptions::default(),
        )
        .await
    }

    /// Mints the content object a direct multipart upload assembles into,
    /// opens the provider upload behind it, and returns the internal target
    /// for server-side signing.
    pub async fn create_direct_multipart_upload_target_with_options(
        &self,
        options: &DirectMultipartUploadOptions,
    ) -> Result<DirectMultipartUploadTarget> {
        trace::in_operation(
            operation_span!(
                self.core,
                "create_direct_multipart_upload_target",
                self.namespace_id
            ),
            async {
                Ok(self
                    .engine()
                    .begin_direct_multipart_upload_target(*options)
                    .await?)
            },
        )
        .await
    }

    /// Resolves one wave of multipart parts for server-side signing.
    pub async fn sign_upload_parts(
        &self,
        upload_id: &UploadId,
        requested: &[UploadPartChecksumClaim],
    ) -> Result<MultipartPartTargets> {
        trace::in_operation(
            operation_span!(self.core, "sign_upload_parts", self.namespace_id),
            async {
                Ok(self
                    .engine()
                    .direct_multipart_part_targets(upload_id, requested)
                    .await?)
            },
        )
        .await
    }

    /// Uploads whole-file content into an upload session.
    pub async fn put_upload_content(
        &self,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> Result<UploadSession> {
        trace::in_operation(
            operation_span!(
                self.core,
                "put_upload_content",
                self.namespace_id,
                payload_class = trace::payload_class(u64_from_usize(bytes.len()))
            ),
            async { Ok(self.engine().upload_content(upload_id, bytes).await?) },
        )
        .await
    }

    /// Uploads content that arrives as a stream into an upload session.
    ///
    /// The payload is hashed as it is forwarded to object storage rather
    /// than kept in memory, so memory follows the transfer's part size instead of the
    /// object's length. Everything after this point — the reference it
    /// produces, completion, publication — is identical to the buffered
    /// path's; callers that already have their bytes should stay on
    /// [`Self::put_upload_content`].
    pub async fn put_upload_content_stream(
        &self,
        upload_id: &UploadId,
        body: ByteStream,
    ) -> Result<UploadSession> {
        trace::in_operation(
            operation_span!(
                self.core,
                "put_upload_content_stream",
                self.namespace_id,
                payload_class = "streamed"
            ),
            async { Ok(self.engine().upload_content_stream(upload_id, body).await?) },
        )
        .await
    }

    /// Completes an upload session and returns proof for later publication.
    pub async fn complete_upload(
        &self,
        upload_id: &UploadId,
        completion: CompleteUploadBody,
    ) -> Result<CompletedUpload> {
        trace::in_operation(
            operation_span!(self.core, "complete_upload", self.namespace_id),
            async {
                let catalog = self
                    .core
                    .load_namespace_catalog_cached(&self.namespace_id)
                    .await?;
                Ok(self
                    .engine()
                    .complete_upload(&catalog, upload_id, completion)
                    .await?)
            },
        )
        .await
    }

    /// Aborts an upload session and deletes the content object it owned.
    pub async fn abort_upload(&self, upload_id: &UploadId) -> Result<UploadSession> {
        trace::in_operation(
            operation_span!(self.core, "abort_upload", self.namespace_id),
            async { Ok(self.engine().abort_upload(upload_id).await?) },
        )
        .await
    }

    /// Returns an upload session and new evidence when the upload is complete.
    pub async fn get_upload(&self, upload_id: &UploadId) -> Result<UploadSessionView> {
        trace::in_operation(
            operation_span!(self.core, "get_upload", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                Ok(self.engine().get_upload(upload_id).await?)
            },
        )
        .await
    }
}
