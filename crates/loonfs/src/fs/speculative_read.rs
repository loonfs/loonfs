//! Small buffered reads that overlap content I/O with current-path validation.

use crate::{FileBytes, FsReader, NamespaceId, Result};
use loonfs_core::ResolvedFileContent;

impl FsReader {
    async fn speculative_file_target(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Option<ResolvedFileContent> {
        let context = self.core.cached_read_context(namespace_id)?;
        self.core
            .reader_engine(namespace_id)
            .resolve_file_content(
                absolute_path,
                &context,
                self.core.inner.config.max_read_content_bytes,
            )
            .await
            .ok()
            .filter(ResolvedFileContent::supports_speculative_read)
    }

    pub(super) async fn get_current_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<FileBytes> {
        let max_bytes = self.core.inner.config.max_read_content_bytes;
        let Some(candidate) = self
            .speculative_file_target(namespace_id, absolute_path)
            .await
        else {
            let (engine, context) = self.core.pinned_metadata_read(namespace_id).await?;
            return Ok(engine.get_file(absolute_path, &context, max_bytes).await?);
        };

        let engine = self.core.reader_engine(namespace_id);
        let (context, target) = {
            let current = async {
                let (engine, context) = self.core.pinned_metadata_read(namespace_id).await?;
                let target = engine
                    .resolve_file_content(absolute_path, &context, max_bytes)
                    .await?;
                Result::Ok((context, target))
            };
            let content = engine.get_speculative_file_content(&candidate);
            tokio::pin!(current, content);
            // An old content error matters only if current metadata still names it.
            let (current_result, content_result) = tokio::select! {
                biased;
                result = &mut current => (result, None),
                result = &mut content => (current.await, Some(result)),
            };
            let (context, target) = current_result?;
            if target.has_same_content(&candidate) {
                let bytes = match content_result {
                    Some(result) => result?,
                    None => content.await?,
                };
                return Ok(FileBytes {
                    entry: target.entry,
                    bytes,
                });
            }
            (context, target)
        };

        // The obsolete future and any completed buffer are dropped before fallback.
        let bytes = engine
            .read_content_ref(&target.content_ref, max_bytes.unwrap_or(u64::MAX), &context)
            .await?;
        Ok(FileBytes {
            entry: target.entry,
            bytes,
        })
    }
}
