//! Staged uploads and direct-put targets.

use crate::content_tokens::CompletedUpload;
use crate::runtime_core::HeadConfirmation;
use crate::trace::{self, operation_span};
use crate::uploads::{
    DirectMultipartUploadTarget, DirectPutUploadTarget, MultipartPartTargets, UploadSessionView,
};
use crate::ByteStream;
use crate::Result;
use crate::{CompleteUploadBody, UploadSession};
use crate::{Namespace, Writable};
use loonfs_core::RuntimeReadContext;
use loonfs_types::options::DirectMultipartUploadOptions;
use loonfs_types::u64_from_usize;
use loonfs_types::UploadPartChecksumClaim;
use loonfs_types::{ActorId, UploadId};

impl Namespace<Writable> {
    /// Returns the read context of this runtime's head of the namespace, as
    /// [`HeadConfirmation::View`] confirms it, and returns
    /// `namespace_not_found` or `namespace_deleted` when the head isn't
    /// active.
    async fn live_context(&self) -> Result<RuntimeReadContext> {
        self.core
            .current_read_context(&self.namespace_id, HeadConfirmation::View)
            .await
    }

    /// Starts a durable upload session for a namespace. The session belongs
    /// to `actor`, and every later step of the upload takes the same actor.
    pub async fn create_upload(&self, actor: &ActorId) -> Result<UploadSession> {
        trace::in_operation(
            operation_span!(self.core, "create_upload", self.namespace_id),
            async {
                let context = self.live_context().await?;
                Ok(self.engine().begin_upload(&context, actor).await?)
            },
        )
        .await
    }

    /// Mints the content object a direct upload will write to and returns
    /// the internal target for server-side signing.
    pub async fn create_direct_put_upload_target(
        &self,
        actor: &ActorId,
    ) -> Result<DirectPutUploadTarget> {
        trace::in_operation(
            operation_span!(
                self.core,
                "create_direct_put_upload_target",
                self.namespace_id
            ),
            async {
                let context = self.live_context().await?;
                Ok(self
                    .engine()
                    .begin_direct_put_upload_target(&context, actor)
                    .await?)
            },
        )
        .await
    }

    /// Runs [`Self::create_direct_multipart_upload_target_with_options`]
    /// with the default part size.
    pub async fn create_direct_multipart_upload_target(
        &self,
        actor: &ActorId,
    ) -> Result<DirectMultipartUploadTarget> {
        self.create_direct_multipart_upload_target_with_options(
            actor,
            &DirectMultipartUploadOptions::default(),
        )
        .await
    }

    /// Mints the content object a direct multipart upload assembles into,
    /// opens the provider upload behind it, and returns the internal target
    /// for server-side signing.
    pub async fn create_direct_multipart_upload_target_with_options(
        &self,
        actor: &ActorId,
        options: &DirectMultipartUploadOptions,
    ) -> Result<DirectMultipartUploadTarget> {
        trace::in_operation(
            operation_span!(
                self.core,
                "create_direct_multipart_upload_target",
                self.namespace_id
            ),
            async {
                let context = self.live_context().await?;
                Ok(self
                    .engine()
                    .begin_direct_multipart_upload_target(&context, actor, *options)
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
        actor: &ActorId,
    ) -> Result<MultipartPartTargets> {
        trace::in_operation(
            operation_span!(self.core, "sign_upload_parts", self.namespace_id),
            async {
                let context = self.live_context().await?;
                Ok(self
                    .engine()
                    .direct_multipart_part_targets(&context, upload_id, requested, actor)
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
        actor: &ActorId,
    ) -> Result<UploadSession> {
        trace::in_operation(
            operation_span!(
                self.core,
                "put_upload_content",
                self.namespace_id,
                payload_class = trace::payload_class(u64_from_usize(bytes.len()))
            ),
            async {
                let context = self.live_context().await?;
                Ok(self
                    .engine()
                    .upload_content(&context, upload_id, bytes, actor)
                    .await?)
            },
        )
        .await
    }

    /// Uploads content that arrives as a stream into an upload session.
    ///
    /// The payload is hashed as it is forwarded to object storage rather
    /// than kept in memory, so memory follows the transfer's part size instead of the
    /// object's length. Everything after this point (the reference it
    /// produces, completion, and publication) is identical to the buffered
    /// path, so callers that already have their bytes should stay on
    /// [`Self::put_upload_content`].
    pub async fn put_upload_content_stream(
        &self,
        upload_id: &UploadId,
        body: ByteStream,
        actor: &ActorId,
    ) -> Result<UploadSession> {
        trace::in_operation(
            operation_span!(
                self.core,
                "put_upload_content_stream",
                self.namespace_id,
                payload_class = "streamed"
            ),
            async {
                let context = self.live_context().await?;
                Ok(self
                    .engine()
                    .upload_content_stream(&context, upload_id, body, actor)
                    .await?)
            },
        )
        .await
    }

    /// Completes an upload session and returns proof for later publication.
    pub async fn complete_upload(
        &self,
        upload_id: &UploadId,
        completion: CompleteUploadBody,
        actor: &ActorId,
    ) -> Result<CompletedUpload> {
        trace::in_operation(
            operation_span!(self.core, "complete_upload", self.namespace_id),
            async {
                let context = self.live_context().await?;
                Ok(self
                    .engine()
                    .complete_upload(&context, upload_id, completion, actor)
                    .await?)
            },
        )
        .await
    }

    /// Aborts an upload session and deletes the content object it owned.
    pub async fn abort_upload(
        &self,
        upload_id: &UploadId,
        actor: &ActorId,
    ) -> Result<UploadSession> {
        trace::in_operation(
            operation_span!(self.core, "abort_upload", self.namespace_id),
            async {
                // LoonFS still aborts the sessions of a deleted namespace.
                let head = self
                    .core
                    .current_head_of_any_status(&self.namespace_id)
                    .await?;
                Ok(self.engine().abort_upload(&head, upload_id, actor).await?)
            },
        )
        .await
    }

    /// Returns an upload session and new evidence when the upload is complete.
    pub async fn get_upload(
        &self,
        upload_id: &UploadId,
        actor: &ActorId,
    ) -> Result<UploadSessionView> {
        trace::in_operation(
            operation_span!(self.core, "get_upload", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                let context = self.live_context().await?;
                Ok(self.engine().get_upload(&context, upload_id, actor).await?)
            },
        )
        .await
    }
}
