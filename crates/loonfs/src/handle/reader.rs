//! The read-only runtime handle.

use super::HandleBuilderCore;
use crate::background::{BackgroundWork, FsBackgroundWork};
use crate::config::default_writer_version;
use crate::fs::FsCore;
use crate::{
    AuthoritativeFileBytes, AuthoritativePathEntry, CapabilityDocument, ChangeSeq, ChangesResponse,
    DirectoryPageCursor, FileRevisionsPageCursor, GrepRequest, GrepResponse, InodeId,
    ListChangesOptions, ListFileRevisionsResponse, ListPathEntriesResponse, NamespaceId,
    ObjectStoreMetricsRecorder, PageRequest, Result, RevisionNo, RuntimeCacheConfig,
    RuntimeCacheStats, SharedObjectStore, StoreConfig, TraceMode, TraceStoreKind,
};
use std::sync::Arc;

/// Read-only handle for latest namespace views.
///
/// `FsReader` serves stat, list, read, revision, and change-feed queries. It
/// owns no writer session, publishes nothing, and never schedules
/// maintenance, so read-only workers cannot accidentally participate in
/// writer scheduling. Reads revalidate cached control state against durable
/// state, so a standalone reader stays consistent without any writer
/// coordination.
///
/// The handle is runtime-bound: open it with `build().await` inside the
/// Tokio runtime that will drive its reads. `FsReader` is cheap to clone.
#[derive(Clone)]
pub struct FsReader {
    core: FsCore,
}

impl FsReader {
    /// Starts a reader builder that constructs its object-store client from
    /// configuration inside this handle's runtime ownership domain.
    pub fn builder(store_config: StoreConfig) -> FsReaderBuilder {
        FsReaderBuilder::new(HandleBuilderCore::from_config(store_config))
    }

    /// Starts a reader builder over a caller-supplied store.
    ///
    /// For callers who know the store is safe in this handle's runtime
    /// ownership domain. Do not use it to share one provider client across
    /// unrelated runtimes; open another handle from [`StoreConfig`] instead.
    pub fn builder_with_store(store: SharedObjectStore) -> FsReaderBuilder {
        FsReaderBuilder::new(HandleBuilderCore::from_store(store))
    }

    /// Wraps a shared runtime core; used by [`FsWriter::reader`](crate::FsWriter::reader).
    pub(super) fn from_core(core: FsCore) -> Self {
        Self { core }
    }

    /// Returns the capability document for this embedded build (API spec,
    /// "Capability discovery").
    pub fn capabilities(&self) -> CapabilityDocument {
        self.core.capabilities()
    }

    /// Snapshots the runtime cache counters.
    pub fn runtime_cache_stats(&self) -> RuntimeCacheStats {
        self.core.runtime_cache_stats()
    }

    /// Resolves an absolute path to its authoritative entry at the current
    /// head.
    pub async fn stat_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativePathEntry> {
        self.core.stat_path(namespace_id, absolute_path).await
    }

    /// Lists the children of a directory path.
    ///
    /// Entries-only convenience over [`Self::list_path_entries`].
    pub async fn list_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<Vec<AuthoritativePathEntry>> {
        self.core.list_path(namespace_id, absolute_path).await
    }

    /// Lists a directory together with the head the listing was read from.
    ///
    /// The envelope and every entry come from one consistent head, so an
    /// empty directory still reports which state answered the question.
    /// Entries are returned in canonical name-key order, matching paged
    /// listings.
    pub async fn list_path_entries(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<ListPathEntriesResponse> {
        self.core
            .list_path_entries(namespace_id, absolute_path)
            .await
    }

    /// Lists one page of a directory together with the head the page was
    /// read from.
    pub async fn list_path_entries_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
    ) -> Result<ListPathEntriesResponse> {
        self.core
            .list_path_entries_page(namespace_id, absolute_path, request)
            .await
    }

    /// Reads a file's current content plus the metadata entry it came from.
    pub async fn read_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativeFileBytes> {
        self.core.read_file_bytes(namespace_id, absolute_path).await
    }

    /// Content search over the namespace's gram index: index-accelerated
    /// candidates plus an exhaustive scan of the unindexed tail, every
    /// candidate verified against the real pattern.
    pub async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
    ) -> Result<GrepResponse> {
        self.core.grep(namespace_id, request).await
    }

    /// Lists the revision history of a file path.
    pub async fn list_file_revisions(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<ListFileRevisionsResponse> {
        self.core
            .list_file_revisions(namespace_id, absolute_path)
            .await
    }

    /// Lists one page of a file path's revision history.
    pub async fn list_file_revisions_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
        self.core
            .list_file_revisions_page(namespace_id, absolute_path, request)
            .await
    }

    /// Lists a file's revision history by inode id, independent of its
    /// current path.
    pub async fn list_file_revisions_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<ListFileRevisionsResponse> {
        self.core
            .list_file_revisions_for_inode(namespace_id, inode_id)
            .await
    }

    /// Lists one page of a file inode's revision history.
    pub async fn list_file_revisions_for_inode_page(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
        self.core
            .list_file_revisions_for_inode_page(namespace_id, inode_id, request)
            .await
    }

    /// Reads the content of one historical file revision by path.
    pub async fn read_file_revision_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        revision_no: RevisionNo,
    ) -> Result<AuthoritativeFileBytes> {
        self.core
            .read_file_revision_bytes(namespace_id, absolute_path, revision_no)
            .await
    }

    /// Reads the content of one historical file revision by inode id.
    pub async fn read_file_revision_bytes_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        self.core
            .read_file_revision_bytes_for_inode(namespace_id, inode_id, revision_no)
            .await
    }

    /// Reads the ordered change feed after the `after_seq` cursor.
    pub async fn list_changes_after(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        options: ListChangesOptions,
    ) -> Result<ChangesResponse> {
        self.core
            .list_changes_after(namespace_id, after_seq, options)
            .await
    }

    /// Shuts down handle-owned background work. Readers own none, so this
    /// settles immediately; it exists so every handle shares one shutdown
    /// shape.
    pub async fn shutdown_background(&self) -> Result<()> {
        Ok(())
    }
}

/// Builder for [`FsReader`].
pub struct FsReaderBuilder {
    core: HandleBuilderCore,
}

impl FsReaderBuilder {
    fn new(core: HandleBuilderCore) -> Self {
        Self { core }
    }

    /// Sets runtime cache behavior.
    pub fn runtime_cache(mut self, runtime_cache: RuntimeCacheConfig) -> Self {
        self.core.runtime_cache = runtime_cache;
        self
    }

    /// Caps the file content size the buffered read APIs will materialize
    /// for one call, checked against resolved metadata before any content
    /// fetch; over-limit reads fail with `content_too_large`. Unset by
    /// default: embedded callers read files of any size.
    pub fn max_read_content_bytes(mut self, max_read_content_bytes: u64) -> Self {
        self.core.max_read_content_bytes = Some(max_read_content_bytes);
        self
    }

    /// Sets the tracing mode label.
    pub fn trace_mode(mut self, trace_mode: TraceMode) -> Self {
        self.core.trace_mode = trace_mode;
        self
    }

    /// Sets the object-store kind label used by tracing and metrics.
    ///
    /// Config-built stores derive this automatically; setting it overrides
    /// the derived label.
    pub fn trace_store_kind(mut self, trace_store_kind: TraceStoreKind) -> Self {
        self.core.trace_store_kind = Some(trace_store_kind);
        self
    }

    /// Installs object-store metrics collection for this handle.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn ObjectStoreMetricsRecorder>) -> Self {
        self.core.metrics_recorder = Some(recorder);
        self
    }

    /// Opens the reader inside the Tokio runtime that will drive its reads.
    pub async fn build(self) -> Result<FsReader> {
        // Readers never mutate, so the engine identity below is inert; it
        // exists because shared internals require a non-empty actor.
        let background = BackgroundWork::new(
            FsBackgroundWork::ManualOnly,
            None,
            crate::config::DEFAULT_MAX_CONCURRENT_MAINTENANCE,
        );
        Ok(FsReader {
            core: self.core.open(
                "loonfs-reader".to_owned(),
                default_writer_version(),
                background,
            )?,
        })
    }
}
