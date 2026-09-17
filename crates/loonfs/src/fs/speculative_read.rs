//! Small buffered reads that overlap content I/O with current-path validation.

use crate::{FileBytes, FsReader, NamespaceId, Result};
use loonfs_core::{ResolvedFileContent, RuntimeReadContext};

/// A path resolved in the cached view, before that view is validated.
struct CachedFileTarget {
    view: RuntimeReadContext,
    target: ResolvedFileContent,
}

impl FsReader {
    async fn cached_file_target(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Option<CachedFileTarget> {
        let view = self.core.cached_read_context(namespace_id)?;
        let target = self
            .core
            .reader_engine(namespace_id)
            .resolve_file_content(
                absolute_path,
                &view,
                self.core.inner.config.max_read_content_bytes,
            )
            .await
            .ok()?;
        Some(CachedFileTarget { view, target })
    }

    /// Validates the namespace, then resolves the path unless the cached view
    /// is still current: an unchanged view resolves it to the same target.
    async fn current_file_target(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        cached: &CachedFileTarget,
    ) -> Result<ResolvedFileContent> {
        let (engine, context) = self.core.pinned_metadata_read(namespace_id).await?;
        let target = if context.head == cached.view.head && context.basis == cached.view.basis {
            cached.target.clone()
        } else {
            engine
                .resolve_file_content(
                    absolute_path,
                    &context,
                    self.core.inner.config.max_read_content_bytes,
                )
                .await?
        };
        Ok(target)
    }

    pub(super) async fn get_current_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<FileBytes> {
        let max_bytes = self.core.inner.config.max_read_content_bytes;
        let Some(cached) = self.cached_file_target(namespace_id, absolute_path).await else {
            let (engine, context) = self.core.pinned_metadata_read(namespace_id).await?;
            return Ok(engine.get_file(absolute_path, &context, max_bytes).await?);
        };

        let engine = self.core.reader_engine(namespace_id);
        let current = self.current_file_target(namespace_id, absolute_path, &cached);
        let target = if cached.target.supports_speculative_read() {
            let content = engine.get_speculative_file_content(&cached.target);
            tokio::pin!(current, content);
            // An old content error matters only if current metadata still names it.
            let (current_result, content_result) = tokio::select! {
                biased;
                result = &mut current => (result, None),
                result = &mut content => (current.await, Some(result)),
            };
            let target = current_result?;
            if target.has_same_content(&cached.target) {
                let bytes = match content_result {
                    Some(result) => result?,
                    None => content.await?,
                };
                return Ok(FileBytes {
                    entry: target.entry,
                    bytes,
                });
            }
            target
        } else {
            current.await?
        };

        // The obsolete future and any completed buffer are dropped before this read.
        let bytes = engine.get_resolved_file_content(&target).await?;
        Ok(FileBytes {
            entry: target.entry,
            bytes,
        })
    }
}
