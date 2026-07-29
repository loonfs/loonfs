//! [`FsReader`]'s read operations: stat, list, content, grep, revision
//! reads, the change feed, and the whole-namespace reads a consumer that
//! derives its own data from the filesystem walks.

use super::core::{default_page_limit, file_revisions_page_response};
use crate::FsReader;
use crate::Result;
use crate::{
    AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq, ChangesResponse,
    CheckpointFilesPage, CheckpointFilesPageCursor, CheckpointId, ContentRef, CoreError,
    CurrentFileState, InodeId, ListChangesOptions, ListFileRevisionsResponse,
    ListPathEntriesResponse, NamespaceId, RevisionNo, RuntimeError,
};
use loonfs_api::{
    encode_cursor, AbsolutePath, DirectoryPageCursor, FileRevisionsPageCursor, GrepRequest,
    GrepResponse, PageRequest, PaginationPolicy,
};
use loonfs_grep::GrepIndexSnapshot;

impl FsReader {
    /// Resolves an absolute path to its authoritative entry at the current
    /// head.
    #[tracing::instrument(
        level = "info",
        name = "loonfs.stat",
        err,
        skip_all,
        fields(
            operation = "stat",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            cache_path = tracing::field::Empty,
        )
    )]
    pub async fn stat_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativePathEntry> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        let (engine, read_context) = self.core.pinned_read(namespace_id).await?;
        let entry = engine.resolve_path(absolute_path, &read_context).await?;
        tracing::Span::current().record(
            "cache_path",
            crate::trace::CachePath::MaterializedTables.as_str(),
        );
        self.core
            .inner
            .cache_stats
            .record_latest_metadata_view_read();
        Ok(entry)
    }

    /// Lists a directory by aggregating every page into one response.
    ///
    /// Listing cursors tolerate commits landing mid-listing — each page
    /// resumes in name-key order against the current head — so the
    /// envelope's `head_seq` reports the newest head that served a page (an
    /// empty directory still reports which state answered the question).
    /// Entries are returned in canonical name-key order, matching paged
    /// listings.
    pub async fn list_path_entries_all(
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
            envelope_ref.head_seq = envelope_ref.head_seq.max(page.head_seq);
            entries.extend(page.entries);
            cursor = next_cursor;
            if cursor.is_none() {
                envelope_ref.entries = entries;
                return Ok(envelope.expect("first page should initialize response envelope"));
            }
        }
    }

    /// Lists one page of a directory together with the head the page was read from.
    pub async fn list_path_entries_page(
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
            .map(encode_cursor)
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
        let (engine, read_context) = self.core.pinned_read(namespace_id).await?;
        let request_head_seq = request.cursor.as_ref().map(|cursor| cursor.head_seq);
        let page = engine
            .list_path_page(listed_path.as_str(), request, &read_context)
            .await?;
        self.core
            .inner
            .cache_stats
            .record_latest_metadata_view_read();
        let head_seq = page
            .items
            .first()
            .map(|entry| entry.head_seq)
            .or(request_head_seq)
            .unwrap_or(read_context.head.seq);
        let next_cursor = page.next_cursor;
        let response = ListPathEntriesResponse {
            namespace_id: namespace_id.clone(),
            absolute_path: listed_path,
            head_seq,
            entries: page.items,
            next_cursor: None,
        };
        Ok((response, next_cursor))
    }

    /// Reads a file's current content plus the metadata entry it came from.
    pub async fn get_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativeFileBytes> {
        let (engine, read_context) = self.core.pinned_read(namespace_id).await?;
        let read = engine
            .read_file(
                absolute_path,
                &read_context,
                self.core.inner.config.max_read_content_bytes,
            )
            .await?;
        self.core
            .inner
            .cache_stats
            .record_latest_metadata_view_read();
        Ok(read)
    }

    /// Content search over the namespace's grep index.
    pub async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
    ) -> Result<GrepResponse> {
        let (engine, read_context) = self.core.pinned_read(namespace_id).await?;
        let view = engine.load_grep_view(&read_context).await?;
        let snapshot = GrepIndexSnapshot::from_grep_root(
            self.core.store(),
            namespace_id,
            &self.core.inner.grep_service,
        )
        .await;
        let response = self
            .core
            .inner
            .grep_service
            .query(request, &snapshot, &view, &self.core.inner.store)
            .await?;
        self.core
            .inner
            .cache_stats
            .record_latest_metadata_view_read();
        Ok(response)
    }

    /// Reads one page of the files visible in the state a checkpoint pins,
    /// in ascending inode-id order.
    ///
    /// The checkpoint's pinned manifest is the whole answer: no WAL is
    /// replayed over it, so a commit landing mid-enumeration changes
    /// nothing a consumer sees, and everything after
    /// [`CheckpointFilesPage::checkpoint_seq`] is what
    /// [`Self::list_changes`] reports. Directories are not returned.
    ///
    /// A checkpoint that was released, expired, or reaped answers
    /// `checkpoint_unavailable`; the enumeration never silently falls back
    /// to current state. A consumer that loses its checkpoint takes a new
    /// one and starts over.
    pub async fn list_checkpoint_files_page(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
        request: PageRequest<CheckpointFilesPageCursor>,
    ) -> Result<CheckpointFilesPage> {
        let (engine, read_context) = self.core.pinned_read(namespace_id).await?;
        Ok(engine
            .list_checkpoint_files_page(checkpoint_id, request, &read_context)
            .await?)
    }

    /// Answers, for each inode id, what it looks like right now: whether it
    /// is visible, its current revision, and its current path.
    ///
    /// One pinned read serves the batch, so every answer describes the same
    /// state, and answers come back in input order. Ids that name nothing
    /// answer `visible: false` instead of failing — a consumer holding ids
    /// from an earlier enumeration routinely holds stale ones. A directory
    /// id answers visible with a path and no revision.
    ///
    /// At most [`MAX_RESOLVE_CURRENT_FILES`](crate::MAX_RESOLVE_CURRENT_FILES)
    /// ids per call; a larger batch is refused with `invalid_request`
    /// before anything is read.
    pub async fn resolve_current_files(
        &self,
        namespace_id: &NamespaceId,
        inode_ids: &[InodeId],
    ) -> Result<Vec<CurrentFileState>> {
        let (engine, read_context) = self.core.pinned_read(namespace_id).await?;
        let states = engine
            .resolve_current_files(inode_ids, &read_context)
            .await?;
        self.core
            .inner
            .cache_stats
            .record_latest_metadata_view_read();
        Ok(states)
    }

    /// Reads one immutable content object by reference.
    ///
    /// `max_bytes` is this caller's own budget, checked against the
    /// reference's declared size before any fetch, and deliberately
    /// independent of the deployment's configured download limit — a
    /// consumer that streams work through a fixed buffer says so here. The
    /// fetched bytes are verified against the reference's size and digest,
    /// and a mismatch fails the read.
    pub async fn read_content_ref(
        &self,
        namespace_id: &NamespaceId,
        content_ref: &ContentRef,
        max_bytes: u64,
    ) -> Result<Vec<u8>> {
        let (engine, read_context) = self.core.pinned_read(namespace_id).await?;
        Ok(engine
            .read_content_ref(content_ref, max_bytes, &read_context)
            .await?)
    }

    /// Lists one page of the namespace's recoverable deletions, ascending
    /// by deleted root inode. Tombstone rows are immortal, so this answers
    /// however far the replay floor has advanced; entries carry the deleted
    /// name when the delete recorded one.
    pub async fn list_trash_page(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<loonfs_api::TrashPageCursor>,
    ) -> Result<loonfs_api::ListTrashResponse> {
        let (engine, read_context) = self.core.pinned_read(namespace_id).await?;
        let page = engine.list_trash_page(request, &read_context).await?;
        self.core
            .inner
            .cache_stats
            .record_latest_metadata_view_read();
        let next_cursor = page
            .next_cursor
            .as_ref()
            .map(encode_cursor)
            .transpose()
            .map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
        Ok(loonfs_api::ListTrashResponse {
            namespace_id: namespace_id.clone(),
            head_seq: read_context.head.seq,
            entries: page.items,
            next_cursor,
        })
    }

    /// Lists one page of a file path's revision history.
    pub async fn list_file_revisions_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
        let (engine, read_context) = self.core.pinned_read(namespace_id).await?;
        let fallback_inode_id = request.cursor.as_ref().map(|cursor| cursor.inode_id);
        let page = engine
            .list_file_revisions_page(absolute_path, request, &read_context)
            .await?;
        self.core
            .inner
            .cache_stats
            .record_latest_metadata_view_read();
        Ok(file_revisions_page_response(
            namespace_id.clone(),
            read_context.head.seq,
            page,
            fallback_inode_id,
        )?)
    }

    /// Reads the content of one historical file revision by path.
    pub async fn get_file_revision_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        revision_no: RevisionNo,
    ) -> Result<AuthoritativeFileBytes> {
        let (engine, read_context) = self.core.pinned_read(namespace_id).await?;
        let read = engine
            .read_file_revision(
                absolute_path,
                revision_no,
                &read_context,
                self.core.inner.config.max_read_content_bytes,
            )
            .await?;
        self.core
            .inner
            .cache_stats
            .record_latest_metadata_view_read();
        Ok(read)
    }

    /// Reads the ordered change feed after the `after_seq` cursor.
    pub async fn list_changes(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        options: ListChangesOptions,
    ) -> Result<ChangesResponse> {
        let limit = match options.limit {
            Some(limit) => limit,
            None => PaginationPolicy::default()
                .resolve_limit(None)
                .map_err(|error| RuntimeError::Config(error.to_string()))?,
        };
        Ok(self
            .core
            .reader_engine(namespace_id)
            .list_changes_after(after_seq, limit)
            .await?)
    }
}
