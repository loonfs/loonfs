//! Small buffered reads that overlap content I/O with current-path validation.

use crate::{FileBytes, Freshness, Namespace, Result};
use loonfs_core::{ResolvedFileContent, RuntimeReadContext};

/// A path resolved in the cached view, before that view is validated.
struct CachedFileTarget {
    view: RuntimeReadContext,
    target: ResolvedFileContent,
}

impl<M: crate::Mode> Namespace<M> {
    async fn cached_file_target(&self, absolute_path: &str) -> Option<CachedFileTarget> {
        let view = self.core.cached_read_context(&self.namespace_id)?;
        let target = self
            .core
            .reader_engine(&self.namespace_id)
            .resolve_file_content(
                absolute_path,
                &view,
                Some(self.core.inner.config.max_read_content_bytes),
            )
            .await
            .ok()?;
        Some(CachedFileTarget { view, target })
    }

    /// Validates the namespace, then resolves the path unless the cached view
    /// is still current: an unchanged view resolves it to the same target.
    async fn current_file_target(
        &self,
        absolute_path: &str,
        cached: &CachedFileTarget,
        freshness: Freshness,
    ) -> Result<ResolvedFileContent> {
        self.core
            .read(
                &self.namespace_id,
                freshness,
                |engine, context| async move {
                    let target =
                        if context.head == cached.view.head && context.basis == cached.view.basis {
                            cached.target.clone()
                        } else {
                            engine
                                .resolve_file_content(
                                    absolute_path,
                                    &context,
                                    Some(self.core.inner.config.max_read_content_bytes),
                                )
                                .await?
                        };
                    Ok(target)
                },
            )
            .await
    }

    pub(super) async fn get_current_file_bytes(
        &self,
        absolute_path: &str,
        freshness: Freshness,
    ) -> Result<FileBytes> {
        let max_bytes = Some(self.core.inner.config.max_read_content_bytes);
        let Some(cached) = self.cached_file_target(absolute_path).await else {
            return self
                .core
                .read(
                    &self.namespace_id,
                    freshness,
                    |engine, context| async move {
                        Ok(engine.read_file(absolute_path, &context, max_bytes).await?)
                    },
                )
                .await;
        };

        let engine = self.core.reader_engine(&self.namespace_id);
        let current = self.current_file_target(absolute_path, &cached, freshness);
        let target = if cached.target.supports_speculative_read() {
            let content =
                engine.read_speculative_file_content(&cached.target, &cached.view.object_cache);
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
        let bytes = engine
            .read_resolved_file_content(&target, &cached.view.object_cache)
            .await?;
        Ok(FileBytes {
            entry: target.entry,
            bytes,
        })
    }
}
