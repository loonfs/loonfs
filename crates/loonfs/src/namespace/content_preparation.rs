//! Content preparation on a writable [`Namespace`]: staging bytes or a stream,
//! and redeeming a content token, so that a commit can reference the content.

use crate::publish::PreparedContent;
use crate::runtime_core::HeadConfirmation;
use crate::trace::{self, operation_span};
use crate::ByteStream;
use crate::Result;
use crate::{Namespace, Writable};
use futures::StreamExt;
use loonfs_types::u64_from_usize;

impl Namespace<Writable> {
    /// Prepares content for publication.
    ///
    /// Content at or under the configured inline threshold needs no store
    /// request and has no expiry. Larger content is written as one content
    /// object with one put, and a commit admits it until 23 hours and 35
    /// minutes after the write started
    /// ([`loonfs_core::limits::OWNED_STAGING_ADMISSION_WINDOW_MS`]).
    pub async fn prepare_content(&self, bytes: &[u8]) -> Result<PreparedContent> {
        trace::in_operation(
            operation_span!(
                self.core,
                "prepare_content",
                self.namespace_id,
                payload_class = trace::payload_class(u64_from_usize(bytes.len()))
            ),
            self.prepare_content_inner(bytes),
        )
        .await
    }

    pub(super) async fn prepare_content_inner(&self, bytes: &[u8]) -> Result<PreparedContent> {
        if bytes.len()
            <= self
                .core
                .config()
                .inline_content
                .inline_content_threshold_bytes
        {
            return Ok(PreparedContent::inline(
                bytes::Bytes::copy_from_slice(bytes),
                self.core.store().checksum_algorithm(),
            ));
        }
        let context = self
            .core
            .current_read_context(&self.namespace_id, HeadConfirmation::View)
            .await?;
        Ok(self.engine().stage_owned_bytes(&context, bytes).await?)
    }

    /// Prepares a stream, buffering at most the inline threshold plus one byte
    /// before choosing inline content or forwarding the stream to storage.
    ///
    /// The prepared content expires as [`Self::prepare_content`]'s does.
    pub async fn prepare_content_stream(&self, body: ByteStream) -> Result<PreparedContent> {
        trace::in_operation(
            operation_span!(
                self.core,
                "prepare_content_stream",
                self.namespace_id,
                payload_class = "streamed"
            ),
            self.prepare_content_stream_inner(body),
        )
        .await
    }

    pub(super) async fn prepare_content_stream_inner(
        &self,
        mut body: ByteStream,
    ) -> Result<PreparedContent> {
        let threshold = self
            .core
            .config()
            .inline_content
            .inline_content_threshold_bytes;
        let mut buffered = bytes::BytesMut::with_capacity(threshold + 1);
        while buffered.len() <= threshold {
            let Some(chunk) = body.next().await else {
                return Ok(PreparedContent::inline(
                    buffered.freeze(),
                    self.core.store().checksum_algorithm(),
                ));
            };
            let mut chunk = chunk.map_err(|error| crate::CoreError::Store {
                object_key: "upload body".to_owned(),
                message: error.public_message().into_owned(),
                kind: error.kind(),
            })?;
            let take = chunk.len().min(threshold + 1 - buffered.len());
            buffered.extend_from_slice(&chunk.split_to(take));
            if buffered.len() > threshold {
                body = futures::stream::iter([Ok(buffered.freeze()), Ok(chunk)])
                    .chain(body)
                    .boxed();
                break;
            }
        }
        let context = self
            .core
            .current_read_context(&self.namespace_id, HeadConfirmation::View)
            .await?;
        Ok(self.engine().stage_owned_stream(&context, body).await?)
    }

    /// Verifies an authorized content token for this namespace. A namespace
    /// that is initializing or deleted returns `namespace_not_found` or
    /// `namespace_deleted`, as [`Self::prepare_content`] does.
    pub async fn prepare_content_token(
        &self,
        secret: &str,
        token: &crate::content_tokens::ContentToken,
    ) -> Result<std::result::Result<PreparedContent, loonfs_core::content::ContentTokenError>> {
        trace::in_operation(
            operation_span!(self.core, "prepare_content_token", self.namespace_id),
            async {
                let context = self
                    .core
                    .current_read_context(&self.namespace_id, HeadConfirmation::View)
                    .await?;
                Ok(loonfs_core::content::verify_content_token(
                    secret,
                    &context.read_basis().head().namespace_id,
                    token,
                    self.core.now_ms(),
                ))
            },
        )
        .await
    }
}
