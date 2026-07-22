//! Read operations: stat, list, content, grep, and revision reads.

use super::core::{default_page_limit, file_revisions_page_response, FsCore};
use crate::Result;
use crate::{
    AuthoritativeFileBytes, AuthoritativePathEntry, CoreError, InodeId, ListFileRevisionsResponse,
    ListPathEntriesResponse, NamespaceId, RevisionNo,
};
use loonfs_api::{
    encode_directory_cursor, AbsolutePath, DirectoryPageCursor, FileRevisionsPageCursor,
    GrepRequest, GrepResponse, PageRequest,
};
use loonfs_grep::GrepIndexSnapshot;

impl FsCore {
    /// Resolves an absolute path to its authoritative entry at the current
    /// head.
    #[tracing::instrument(
        level = "info",
        name = "loon.stat",
        err,
        skip_all,
        fields(
            operation = "stat",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            cache_path = tracing::field::Empty,
        )
    )]
    pub(crate) async fn stat_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativePathEntry> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let entry = engine
            .resolve_path_with_runtime_context(absolute_path, &read_context)
            .await?;
        tracing::Span::current().record(
            "cache_path",
            crate::trace::CachePath::MaterializedTables.as_str(),
        );
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(entry)
    }

    /// Lists a directory by aggregating every page into one response.
    ///
    /// The envelope and every entry come from one consistent head, so an
    /// empty directory still reports which state answered the question. Entries
    /// are returned in canonical name-key order, matching paged listings.
    pub(crate) async fn list_path_entries_all(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<ListPathEntriesResponse> {
        let limit = default_page_limit();
        let mut cursor = None;
        let mut entries = Vec::new();
        let mut envelope = None;
        loop {
            let (page, next_cursor) = self
                .list_path_entries_page_typed(
                    namespace_id,
                    absolute_path,
                    PageRequest { limit, cursor },
                )
                .await?;
            let envelope_ref = envelope.get_or_insert_with(|| ListPathEntriesResponse {
                namespace_id: page.namespace_id.clone(),
                absolute_path: page.absolute_path.clone(),
                head_seq: page.head_seq,
                entries: Vec::new(),
                next_cursor: None,
            });
            entries.extend(page.entries);
            cursor = next_cursor;
            if cursor.is_none() {
                envelope_ref.entries = entries;
                return Ok(envelope.expect("first page initializes response envelope"));
            }
        }
    }

    /// Lists one page of a directory together with the head the page was read from.
    pub(crate) async fn list_path_entries_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
    ) -> Result<ListPathEntriesResponse> {
        let (mut response, next_cursor) = self
            .list_path_entries_page_typed(namespace_id, absolute_path, request)
            .await?;
        response.next_cursor = next_cursor
            .as_ref()
            .map(encode_directory_cursor)
            .transpose()
            .map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
        Ok(response)
    }

    async fn list_path_entries_page_typed(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
    ) -> Result<(ListPathEntriesResponse, Option<DirectoryPageCursor>)> {
        let listed_path = AbsolutePath::parse(absolute_path)
            .map_err(|error| CoreError::InvalidPath(error.to_string()))?;
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let request_head_seq = request.cursor.as_ref().map(|cursor| cursor.head_seq);
        let page = engine
            .list_path_page_with_runtime_context(listed_path.as_str(), request, &read_context)
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        let head_seq = page
            .items
            .first()
            .map(|entry| entry.head_seq)
            .or(request_head_seq)
            .unwrap_or(read_context.head.seq);
        let next_cursor = page.next_cursor;
        let response = ListPathEntriesResponse {
            namespace_id: namespace_id.clone(),
            absolute_path: listed_path.as_str().to_owned(),
            head_seq,
            entries: page.items,
            next_cursor: None,
        };
        Ok((response, next_cursor))
    }

    /// Reads a file's current content plus the metadata entry it came from.
    pub(crate) async fn read_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativeFileBytes> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let read = engine
            .read_file_with_runtime_context(
                absolute_path,
                &read_context,
                self.inner.config.max_read_content_bytes,
            )
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(read)
    }

    /// Content search over the namespace's gram index.
    pub(crate) async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
    ) -> Result<GrepResponse> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let view = engine
            .load_grep_view_with_runtime_context(&read_context)
            .await?;
        let root = loonfs_grep::root::load_grep_root(self.store(), namespace_id).await;
        let snapshot = match root {
            Ok(root) => GrepIndexSnapshot::from_grep_root(root.as_ref().map(|root| root.state())),
            Err(error) => GrepIndexSnapshot::from_core_parts(Err(CoreError::NamespaceCorrupt(
                format!("grep state for `{namespace_id}` is unreadable: {error}"),
            ))),
        };
        let response = self
            .inner
            .grep_service
            .query(request, &snapshot, &view, &self.inner.store)
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(response)
    }

    /// Lists one page of a file path's revision history.
    pub(crate) async fn list_file_revisions_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let fallback_inode_id = request.cursor.as_ref().map(|cursor| cursor.inode_id);
        let page = engine
            .list_file_revisions_page_with_runtime_context(absolute_path, request, &read_context)
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(file_revisions_page_response(
            namespace_id.clone(),
            read_context.head.seq,
            page,
            fallback_inode_id,
        )?)
    }

    /// Lists one page of a file inode's revision history.
    pub(crate) async fn list_file_revisions_for_inode_page(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let page = engine
            .list_file_revisions_for_inode_page_with_runtime_context(
                inode_id,
                request,
                &read_context,
            )
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(file_revisions_page_response(
            namespace_id.clone(),
            read_context.head.seq,
            page,
            Some(inode_id),
        )?)
    }

    /// Reads the content of one historical file revision by path.
    pub(crate) async fn read_file_revision_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        revision_no: RevisionNo,
    ) -> Result<AuthoritativeFileBytes> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let read = engine
            .read_file_revision_with_runtime_context(
                absolute_path,
                revision_no,
                &read_context,
                self.inner.config.max_read_content_bytes,
            )
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(read)
    }

    /// Reads the content of one historical file revision by inode id.
    pub(crate) async fn read_file_revision_bytes_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let read = engine
            .read_file_revision_for_inode_with_runtime_context(
                inode_id,
                revision_no,
                &read_context,
                self.inner.config.max_read_content_bytes,
            )
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(read)
    }
}
