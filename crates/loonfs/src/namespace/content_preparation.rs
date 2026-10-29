//! Content preparation on a writable [`Namespace`]: staging bytes or a stream,
//! checking an existing content reference, and redeeming a content token, so
//! that a commit can reference the content.

use crate::publish::PreparedContent;
use crate::trace::{self, operation_span};
use crate::ByteStream;
use crate::ContentRef;
use crate::Result;
use crate::{Namespace, Writable};
use futures::StreamExt;
use loonfs_types::u64_from_usize;

impl Namespace<Writable> {
    /// Prepares content for publication.
    ///
    /// Content at or under the configured inline threshold needs no store
    /// request and has no expiry. Larger content writes an upload session and
    /// object. Its proof expires at the completed upload's receipt horizon.
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
        if self
            .core
            .config()
            .inline_content
            .inline_content_threshold_bytes
            .is_some_and(|threshold| bytes.len() <= threshold)
        {
            return Ok(PreparedContent::inline(
                self.namespace_id.clone(),
                bytes::Bytes::copy_from_slice(bytes),
                self.core.store().checksum_algorithm(),
            ));
        }
        let catalog = self
            .core
            .load_namespace_catalog_cached(&self.namespace_id)
            .await?;
        Ok(self
            .engine()
            .stage_owned_bytes(
                &catalog,
                self.core.subject().map(|subject| &subject.subject_id),
                bytes,
            )
            .await?)
    }

    /// Prepares a stream, buffering at most the inline threshold plus one byte
    /// before choosing inline content or forwarding the stream to storage.
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
        if let Some(threshold) = self
            .core
            .config()
            .inline_content
            .inline_content_threshold_bytes
        {
            let mut buffered = bytes::BytesMut::with_capacity(threshold + 1);
            while buffered.len() <= threshold {
                let Some(chunk) = body.next().await else {
                    return Ok(PreparedContent::inline(
                        self.namespace_id.clone(),
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
        }
        let catalog = self
            .core
            .load_namespace_catalog_cached(&self.namespace_id)
            .await?;
        Ok(self
            .engine()
            .stage_owned_stream(
                &catalog,
                self.core.subject().map(|subject| &subject.subject_id),
                body,
            )
            .await?)
    }

    /// Imports an existing content reference for later publication.
    ///
    /// Preparation verifies the source bytes and stages them under a fresh
    /// identity owned by this namespace. Later publication performs no content
    /// I/O. A subject must be an administrator of the reference's owner namespace.
    /// See the API specification's content preparation contract.
    pub async fn prepare_content_ref(&self, content_ref: ContentRef) -> Result<PreparedContent> {
        trace::in_operation(
            operation_span!(
                self.core,
                "prepare_content_ref",
                self.namespace_id,
                payload_class = trace::payload_class(content_ref.size_bytes)
            ),
            self.prepare_content_ref_inner(content_ref),
        )
        .await
    }

    pub(super) async fn prepare_content_ref_inner(
        &self,
        content_ref: ContentRef,
    ) -> Result<PreparedContent> {
        let catalog = self
            .core
            .load_namespace_catalog_cached(&self.namespace_id)
            .await?;
        let engine = self.engine();
        let crate::pinned_read::PinnedRead {
            engine: owner,
            context,
        } = self
            .core
            .pinned_read(&content_ref.owner_namespace_id, crate::Consistency::Strong)
            .await?;
        owner.ensure_administrator(&context).await?;
        let owner_location = Some(
            owner
                .resolve_content_location(&content_ref, &context)
                .await?,
        );
        match engine
            .import_content_ref(
                &catalog,
                self.core.subject().map(|subject| &subject.subject_id),
                &content_ref,
                owner_location,
            )
            .await
        {
            Err(crate::CoreError::DurableContent(
                loonfs_core::content::DurableContentValidationError::MissingContentObject {
                    ..
                },
            )) if context.read_basis.head.status.is_deleted() => {
                Err(crate::CoreError::NamespaceDeleted {
                    namespace_id: content_ref.owner_namespace_id,
                }
                .into())
            }
            result => Ok(result?),
        }
    }

    /// Verifies an authorized content token for this namespace.
    pub async fn prepare_content_token(
        &self,
        secret: &str,
        token: &crate::content_tokens::ContentToken,
    ) -> Result<std::result::Result<PreparedContent, loonfs_core::content::ContentTokenError>> {
        trace::in_operation(
            operation_span!(self.core, "prepare_content_token", self.namespace_id),
            async {
                let catalog = self
                    .core
                    .load_namespace_catalog_cached(&self.namespace_id)
                    .await?;
                Ok(loonfs_core::content::verify_content_token(
                    secret,
                    &catalog,
                    token,
                    self.core.now_ms(),
                ))
            },
        )
        .await
    }
}
