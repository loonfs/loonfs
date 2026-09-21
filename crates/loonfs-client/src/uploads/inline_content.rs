//! Prepares small payloads using the deployment's advertised inline limit.

use super::staging::{PreparedContent, PreparedContentKind, UploadContinuity};
use crate::{Client, ClientError, NamespaceId, PayloadSource, Result};
use bytes::BytesMut;
use futures::StreamExt;
use loonfs_api::{
    FEATURE_COMMIT_INLINE_CONTENT, LIMIT_COMMIT_MAX_INLINE_CONTENT_BYTES_PER_OPERATION,
};

impl Client {
    pub(crate) async fn inline_content_limit(&self) -> Result<Option<usize>> {
        let capabilities = self.get_capabilities().await?;
        if !capabilities.supports(FEATURE_COMMIT_INLINE_CONTENT) {
            return Ok(None);
        }
        Ok(capabilities
            .limits
            .get(LIMIT_COMMIT_MAX_INLINE_CONTENT_BYTES_PER_OPERATION)
            .and_then(|limit| usize::try_from(*limit).ok())
            .filter(|limit| *limit < usize::MAX))
    }

    pub(crate) async fn prepare_file_source(
        &self,
        namespace_id: &NamespaceId,
        mut source: PayloadSource,
        continuity: UploadContinuity<'_>,
    ) -> Result<PreparedContent> {
        if let Some(limit) = self
            .inline_content_limit()
            .await?
            .filter(|_| continuity.resume.is_none())
        {
            let (mut stream, size_bytes) = source.into_stream();
            let mut buffered = BytesMut::new();
            while buffered.len() <= limit {
                let Some(chunk) = stream.next().await else {
                    return Ok(PreparedContent {
                        kind: PreparedContentKind::Inline(buffered.to_vec()),
                    });
                };
                let mut chunk = chunk
                    .map_err(|error| ClientError::Io(format!("reading upload content: {error}")))?;
                let take = chunk.len().min(limit + 1 - buffered.len());
                buffered.extend_from_slice(&chunk.split_to(take));
                if buffered.len() > limit {
                    stream = futures::stream::iter([Ok(buffered.freeze()), Ok(chunk)])
                        .chain(stream)
                        .boxed();
                    break;
                }
            }
            source = PayloadSource { stream, size_bytes };
        }
        self.stage_source_as_content_ref(namespace_id, source, continuity)
            .await
    }
}
