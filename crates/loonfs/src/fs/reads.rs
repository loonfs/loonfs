//! Read-only namespace and filesystem operations for [`FsReader`].

use super::core::{encode_next_cursor, file_revisions_page_response};
use crate::downloads::{DirectDownloadByInodeTarget, DirectDownloadTarget};
use crate::FsReader;
use crate::Result;
use crate::{
    AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq, ChangesResponse,
    CheckpointFilesPage, CheckpointFilesPageCursor, CheckpointId, CommittedChange, ContentRef,
    CoreError, CurrentFileState, FileContentStream, FileRevision, InodeId, ListChangesOptions,
    ListFileRevisionsResponse, ListPathEntriesOptions, ListPathEntriesResponse, Namespace,
    NamespaceId, ReadFileStreamOptions, RevisionNo, RuntimeError, SharedObjectStore,
    StatPathOptions, TrashEntry,
};
use loonfs_api::{
    AbsolutePath, DirectoryPageCursor, FileRevisionsPageCursor, PageCursor, PageRequest,
    PaginationPolicy, TrashPageCursor,
};

/// Fetches directory pages as needed.
///
/// [`Self::next`] returns one page with its metadata. [`Self::collect_up_to`]
/// returns at most the requested number of entries and saves unused entries
/// for later calls.
#[must_use]
pub struct PathEntriesPager {
    reader: FsReader,
    namespace_id: NamespaceId,
    absolute_path: String,
    request: PageRequest<DirectoryPageCursor>,
    options: ListPathEntriesOptions,
    pending: Option<ListPathEntriesResponse>,
    exhausted: bool,
}

impl PathEntriesPager {
    /// Returns the next directory page, or `None` after exhaustion.
    pub async fn next(&mut self) -> Option<Result<ListPathEntriesResponse>> {
        if let Some(page) = self.pending.take() {
            return Some(Ok(page));
        }
        if self.exhausted {
            return None;
        }
        let page = self
            .reader
            .list_path_entries_page(
                &self.namespace_id,
                &self.absolute_path,
                self.request.clone(),
                self.options.clone(),
            )
            .await;
        Some(page.and_then(|page| {
            advance_typed_cursor(&mut self.request, page.next_cursor.as_deref())?;
            self.exhausted = self.request.cursor.is_none();
            Ok(page)
        }))
    }

    /// Returns at most `max_items` entries.
    ///
    /// Unused entries from the last page remain available to later calls.
    pub async fn collect_up_to(&mut self, max_items: usize) -> Result<Vec<AuthoritativePathEntry>> {
        let mut entries = Vec::new();
        while entries.len() < max_items {
            let Some(page) = self.next().await else {
                break;
            };
            let mut page = page?;
            let take = (max_items - entries.len()).min(page.entries.len());
            if take < page.entries.len() {
                let remaining = page.entries.split_off(take);
                entries.extend(page.entries);
                page.entries = remaining;
                self.pending = Some(page);
                break;
            }
            entries.extend(page.entries);
        }
        Ok(entries)
    }
}

enum FileRevisionsTarget {
    Path(String),
    Inode(InodeId),
}

/// Fetches file-revision pages as needed for a path or inode.
#[must_use]
pub struct FileRevisionsPager {
    reader: FsReader,
    namespace_id: NamespaceId,
    target: FileRevisionsTarget,
    request: PageRequest<FileRevisionsPageCursor>,
    pending: Option<ListFileRevisionsResponse>,
    exhausted: bool,
}

impl FileRevisionsPager {
    /// Returns the next revision page, or `None` after exhaustion.
    pub async fn next(&mut self) -> Option<Result<ListFileRevisionsResponse>> {
        if let Some(page) = self.pending.take() {
            return Some(Ok(page));
        }
        if self.exhausted {
            return None;
        }
        let page = match &self.target {
            FileRevisionsTarget::Path(absolute_path) => {
                self.reader
                    .list_file_revisions_page(
                        &self.namespace_id,
                        absolute_path,
                        self.request.clone(),
                    )
                    .await
            }
            FileRevisionsTarget::Inode(inode_id) => {
                self.reader
                    .list_file_revisions_by_inode_page(
                        &self.namespace_id,
                        *inode_id,
                        self.request.clone(),
                    )
                    .await
            }
        };
        Some(page.and_then(|page| {
            advance_typed_cursor(&mut self.request, page.next_cursor.as_deref())?;
            self.exhausted = self.request.cursor.is_none();
            Ok(page)
        }))
    }

    /// Returns at most `max_items` revisions.
    ///
    /// Unused revisions from the last page remain available to later calls.
    pub async fn collect_up_to(&mut self, max_items: usize) -> Result<Vec<FileRevision>> {
        let mut revisions = Vec::new();
        while revisions.len() < max_items {
            let Some(page) = self.next().await else {
                break;
            };
            let mut page = page?;
            let take = (max_items - revisions.len()).min(page.revisions.len());
            if take < page.revisions.len() {
                let remaining = page.revisions.split_off(take);
                revisions.extend(page.revisions);
                page.revisions = remaining;
                self.pending = Some(page);
                break;
            }
            revisions.extend(page.revisions);
        }
        Ok(revisions)
    }
}

/// Fetches recoverable-deletion pages as needed.
#[must_use]
pub struct TrashPager {
    reader: FsReader,
    namespace_id: NamespaceId,
    request: PageRequest<TrashPageCursor>,
    pending: Option<loonfs_api::ListTrashResponse>,
    exhausted: bool,
}

impl TrashPager {
    /// Returns the next trash page, or `None` after exhaustion.
    pub async fn next(&mut self) -> Option<Result<loonfs_api::ListTrashResponse>> {
        if let Some(page) = self.pending.take() {
            return Some(Ok(page));
        }
        if self.exhausted {
            return None;
        }
        let page = self
            .reader
            .list_trash_page(&self.namespace_id, self.request.clone())
            .await;
        Some(page.and_then(|page| {
            advance_typed_cursor(&mut self.request, page.next_cursor.as_deref())?;
            self.exhausted = self.request.cursor.is_none();
            Ok(page)
        }))
    }

    /// Returns at most `max_items` deletions.
    ///
    /// Unused deletions from the last page remain available to later calls.
    pub async fn collect_up_to(&mut self, max_items: usize) -> Result<Vec<TrashEntry>> {
        let mut entries = Vec::new();
        while entries.len() < max_items {
            let Some(page) = self.next().await else {
                break;
            };
            let mut page = page?;
            let take = (max_items - entries.len()).min(page.entries.len());
            if take < page.entries.len() {
                let remaining = page.entries.split_off(take);
                entries.extend(page.entries);
                page.entries = remaining;
                self.pending = Some(page);
                break;
            }
            entries.extend(page.entries);
        }
        Ok(entries)
    }
}

/// Fetches change-feed pages as needed, using sequence numbers to resume.
#[must_use]
pub struct ChangesPager {
    reader: FsReader,
    namespace_id: NamespaceId,
    after_seq: ChangeSeq,
    options: ListChangesOptions,
    pending: Option<ChangesResponse>,
    exhausted: bool,
}

impl ChangesPager {
    /// Returns the next change page, or `None` after exhaustion.
    pub async fn next(&mut self) -> Option<Result<ChangesResponse>> {
        if let Some(page) = self.pending.take() {
            return Some(Ok(page));
        }
        if self.exhausted {
            return None;
        }
        let page = self
            .reader
            .list_changes(&self.namespace_id, self.after_seq, self.options.clone())
            .await;
        Some(page.inspect(|page| {
            self.exhausted = page.next_after_seq.is_none();
            if let Some(next_after_seq) = page.next_after_seq {
                self.after_seq = next_after_seq;
            }
        }))
    }

    /// Returns at most `max_items` changes.
    ///
    /// Unused changes from the last page remain available to later calls.
    pub async fn collect_up_to(&mut self, max_items: usize) -> Result<Vec<CommittedChange>> {
        let mut changes = Vec::new();
        while changes.len() < max_items {
            let Some(page) = self.next().await else {
                break;
            };
            let mut page = page?;
            let take = (max_items - changes.len()).min(page.changes.len());
            if take < page.changes.len() {
                let remaining = page.changes.split_off(take);
                changes.extend(page.changes);
                page.changes = remaining;
                self.pending = Some(page);
                break;
            }
            changes.extend(page.changes);
        }
        Ok(changes)
    }
}

fn advance_typed_cursor<C: PageCursor>(
    request: &mut PageRequest<C>,
    next_cursor: Option<&str>,
) -> Result<()> {
    request.cursor = next_cursor
        .map(loonfs_api::decode_cursor)
        .transpose()
        .map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
    Ok(())
}

impl FsReader {
    /// Returns a namespace's current state.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.get_namespace",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "get_namespace",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn get_namespace(&self, namespace_id: &NamespaceId) -> Result<Namespace> {
        self.core.record_trace_context(&tracing::Span::current());
        Ok(loonfs_core::cache::load_namespace(self.core.store(), namespace_id).await?)
    }

    /// Resolves an absolute path to its authoritative entry at the current
    /// head, projecting what `options` asks for.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.stat",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "stat",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            cache_path = tracing::field::Empty,
        )
    )]
    pub async fn stat_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: StatPathOptions,
    ) -> Result<AuthoritativePathEntry> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        let (engine, read_context) = self.core.pinned_metadata_read(namespace_id).await?;
        let entry = engine
            .resolve_path(absolute_path, options, &read_context)
            .await?;
        tracing::Span::current().record("cache_path", crate::trace::CACHE_MATERIALIZED_TABLES);
        Ok(entry)
    }

    /// Returns the current entry for a visible inode.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.stat_inode",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "stat_inode",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            cache_path = tracing::field::Empty,
        )
    )]
    pub async fn stat_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: StatPathOptions,
    ) -> Result<AuthoritativePathEntry> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        let (engine, read_context) = self.core.pinned_metadata_read(namespace_id).await?;
        let entry = engine.stat_inode(inode_id, options, &read_context).await?;
        tracing::Span::current().record("cache_path", crate::trace::CACHE_MATERIALIZED_TABLES);
        Ok(entry)
    }

    /// Creates a directory pager beginning at `request.cursor`.
    pub fn list_path_entries_pager(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
        options: ListPathEntriesOptions,
    ) -> PathEntriesPager {
        PathEntriesPager {
            reader: self.clone(),
            namespace_id: namespace_id.clone(),
            absolute_path: absolute_path.to_owned(),
            request,
            options,
            pending: None,
            exhausted: false,
        }
    }

    /// Lists one page of a directory, projecting what `options` asks for.
    ///
    /// Asking for attributes costs one lookup per entry and adds an unbounded
    /// number of bytes to the page, so a caller that turns the projection on
    /// should also size its page for the maps it expects back.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.list_path_entries",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "list_path_entries",
            method = "list_path_entries_page",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn list_path_entries_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
        options: ListPathEntriesOptions,
    ) -> Result<ListPathEntriesResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let (mut response, next_cursor) = self
            .list_path_entries_page_typed(namespace_id, absolute_path, request, options)
            .await?;
        response.next_cursor = encode_next_cursor(next_cursor.as_ref())?;
        Ok(response)
    }

    async fn list_path_entries_page_typed(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
        options: ListPathEntriesOptions,
    ) -> Result<(ListPathEntriesResponse, Option<DirectoryPageCursor>)> {
        let listed_path = AbsolutePath::parse(absolute_path)
            .map_err(|error| CoreError::InvalidPath(error.to_string()))?;
        let (engine, read_context) = self.core.pinned_metadata_read(namespace_id).await?;
        let request_head_seq = request.cursor.as_ref().map(|cursor| cursor.head_seq);
        let page = engine
            .list_path_page(listed_path.as_str(), request, options, &read_context)
            .await?;
        let head_seq = page
            .items
            .first()
            .map(|entry| entry.head_seq)
            .or(request_head_seq)
            .unwrap_or(read_context.head.seq);
        let next_cursor = page.next_cursor;
        let response = ListPathEntriesResponse {
            namespace_id: namespace_id.clone(),
            path: listed_path,
            head_seq,
            entries: page.items,
            next_cursor: None,
        };
        Ok((response, next_cursor))
    }

    /// Reads a file's current content plus the metadata entry it came from.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.get_file_bytes",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "get_file_bytes",
            method = "get_file_bytes",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn get_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativeFileBytes> {
        self.core.record_trace_context(&tracing::Span::current());
        let (engine, read_context) = self.core.pinned_metadata_read(namespace_id).await?;
        let read = engine
            .get_file(
                absolute_path,
                &read_context,
                self.core.inner.config.max_read_content_bytes,
            )
            .await?;
        Ok(read)
    }

    /// Reads a file's current content as bounded chunks instead of one buffer.
    ///
    /// This is [`Self::get_file_bytes`] for a caller that should not hold what
    /// it reads: the content arrives one ranged read at a time with the
    /// verifying digest folded over it, so a 50 GiB file costs one chunk of
    /// memory rather than 50 GiB. The verification is the buffered read's,
    /// unweakened — declared size plus the reference's full-object checksum,
    /// recomputed using the algorithm the reference names — and it lands on the final
    /// [`FileContentStream::next_chunk`] call. A caller that stops early
    /// stops with unverified bytes; a caller that wants the whole answer or
    /// none of it uses [`Self::get_file_bytes`].
    ///
    /// The deployment's buffered-read cap does not apply here. That cap
    /// bounds what one call materializes, and this call materializes a chunk.
    ///
    /// [`ReadFileStreamOptions::start_offset`] resumes a read the caller
    /// already began. Verification stays over the whole object, so a
    /// resumed stream refuses to fetch anything until it has been handed
    /// the bytes below its start through
    /// [`FileContentStream::fold_resumed_prefix`].
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.get_file_bytes",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "get_file_bytes",
            method = "read_file_stream",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn read_file_stream(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: ReadFileStreamOptions,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        self.core.record_trace_context(&tracing::Span::current());
        let (engine, read_context) = self.core.pinned_metadata_read(namespace_id).await?;
        let stream = engine
            .read_file_stream(
                absolute_path,
                &read_context,
                options.chunk_bytes,
                options.start_offset,
            )
            .await?;
        Ok(stream)
    }

    /// Resolves a path to the content object a direct read would fetch:
    /// the reference that names those bytes, and the object key that
    /// addresses them.
    ///
    /// Metadata only — no content is read, and the handle's
    /// `max_read_content_bytes` does not apply, because that limit bounds
    /// what this process buffers and nothing here buffers anything. A host
    /// signs a short-lived read of the returned key and hands the client
    /// the reference to check the arriving bytes against.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.begin_download",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "begin_download",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn direct_download_target(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        revision_no: Option<RevisionNo>,
    ) -> Result<DirectDownloadTarget> {
        self.core.record_trace_context(&tracing::Span::current());
        let (engine, read_context) = self.core.pinned_metadata_read(namespace_id).await?;
        let target = engine
            .direct_download_target(absolute_path, revision_no, &read_context)
            .await?;
        Ok(target)
    }

    /// Resolves retained inode content for a direct download without
    /// requiring a current path.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.begin_download_by_inode",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "begin_download_by_inode",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn direct_download_target_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<DirectDownloadByInodeTarget> {
        self.core.record_trace_context(&tracing::Span::current());
        let (engine, read_context) = self.core.pinned_metadata_read(namespace_id).await?;
        let target = engine
            .direct_download_target_by_inode(inode_id, revision_no, &read_context)
            .await?;
        Ok(target)
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
    /// A released, expired, or reaped checkpoint returns
    /// `checkpoint_unavailable`. The caller must create a new checkpoint and
    /// restart instead of silently reading current state.
    ///
    /// This does not increment the latest-metadata-view metric because it
    /// reads a checkpoint snapshot.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.list_checkpoint_files_page",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "list_checkpoint_files_page",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn list_checkpoint_files_page(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
        request: PageRequest<CheckpointFilesPageCursor>,
    ) -> Result<CheckpointFilesPage> {
        self.core.record_trace_context(&tracing::Span::current());
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
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.resolve_current_files",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "resolve_current_files",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn resolve_current_files(
        &self,
        namespace_id: &NamespaceId,
        inode_ids: &[InodeId],
    ) -> Result<Vec<CurrentFileState>> {
        self.core.record_trace_context(&tracing::Span::current());
        let (engine, read_context) = self.core.pinned_metadata_read(namespace_id).await?;
        let states = engine
            .resolve_current_files(inode_ids, &read_context)
            .await?;
        Ok(states)
    }

    /// Reads one immutable content object by reference.
    ///
    /// `max_bytes` is checked against the declared size before fetching. It is
    /// independent of the deployment's download limit so callers can apply a
    /// smaller memory budget. The read fails if the returned size or digest
    /// does not match the reference.
    ///
    /// This does not increment the latest-metadata-view metric because it
    /// reads immutable content directly.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.read_content_ref",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "read_content_ref",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn read_content_ref(
        &self,
        namespace_id: &NamespaceId,
        content_ref: &ContentRef,
        max_bytes: u64,
    ) -> Result<Vec<u8>> {
        self.core.record_trace_context(&tracing::Span::current());
        let (engine, read_context) = self.core.pinned_read(namespace_id).await?;
        Ok(engine
            .read_content_ref(content_ref, max_bytes, &read_context)
            .await?)
    }

    /// Lists one page of the namespace's recoverable deletions, ascending
    /// by deleted root inode. Tombstone rows are immortal, so this answers
    /// however far the replay floor has advanced; entries carry the deleted
    /// name when the delete recorded one.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.list_trash",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "list_trash",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn list_trash_page(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<loonfs_api::TrashPageCursor>,
    ) -> Result<loonfs_api::ListTrashResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let (engine, read_context) = self.core.pinned_metadata_read(namespace_id).await?;
        let page = engine.list_trash_page(request, &read_context).await?;
        let next_cursor = encode_next_cursor(page.next_cursor.as_ref())?;
        Ok(loonfs_api::ListTrashResponse {
            namespace_id: namespace_id.clone(),
            head_seq: read_context.head.seq,
            entries: page.items,
            next_cursor,
        })
    }

    /// Creates a trash pager beginning at `request.cursor`.
    pub fn list_trash_pager(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<TrashPageCursor>,
    ) -> TrashPager {
        TrashPager {
            reader: self.clone(),
            namespace_id: namespace_id.clone(),
            request,
            pending: None,
            exhausted: false,
        }
    }

    /// Lists one page of a file path's revision history.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.list_file_revisions",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "list_file_revisions",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn list_file_revisions_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let absolute_path = AbsolutePath::parse(absolute_path)
            .map_err(|error| CoreError::InvalidPath(error.to_string()))?;
        let (engine, read_context) = self.core.pinned_metadata_read(namespace_id).await?;
        let fallback_inode_id = request.cursor.as_ref().map(|cursor| cursor.inode_id);
        let page = engine
            .list_file_revisions_page(absolute_path.as_str(), request, &read_context)
            .await?;
        Ok(file_revisions_page_response(
            namespace_id.clone(),
            read_context.head.seq,
            page,
            fallback_inode_id,
        )?)
    }

    /// Creates a path-based revision pager beginning at `request.cursor`.
    pub fn list_file_revisions_pager(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> FileRevisionsPager {
        FileRevisionsPager {
            reader: self.clone(),
            namespace_id: namespace_id.clone(),
            target: FileRevisionsTarget::Path(absolute_path.to_owned()),
            request,
            pending: None,
            exhausted: false,
        }
    }

    /// Lists one page of retained revisions for a file inode.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.list_file_revisions_by_inode",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "list_file_revisions_by_inode",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn list_file_revisions_by_inode_page(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let (engine, read_context) = self.core.pinned_metadata_read(namespace_id).await?;
        let page = engine
            .list_file_revisions_for_inode_page(inode_id, request, &read_context)
            .await?;
        Ok(file_revisions_page_response(
            namespace_id.clone(),
            read_context.head.seq,
            page,
            Some(inode_id),
        )?)
    }

    /// Creates an inode-based revision pager beginning at `request.cursor`.
    pub fn list_file_revisions_by_inode_pager(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> FileRevisionsPager {
        FileRevisionsPager {
            reader: self.clone(),
            namespace_id: namespace_id.clone(),
            target: FileRevisionsTarget::Inode(inode_id),
            request,
            pending: None,
            exhausted: false,
        }
    }

    /// Reads the content of one historical file revision by path.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.get_file_bytes",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "get_file_bytes",
            method = "get_file_revision_bytes",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn get_file_revision_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        revision_no: RevisionNo,
    ) -> Result<AuthoritativeFileBytes> {
        self.core.record_trace_context(&tracing::Span::current());
        let (engine, read_context) = self.core.pinned_metadata_read(namespace_id).await?;
        let read = engine
            .get_file_revision(
                absolute_path,
                revision_no,
                &read_context,
                self.core.inner.config.max_read_content_bytes,
            )
            .await?;
        Ok(read)
    }

    /// Reads and verifies one retained file revision by inode identity.
    /// Current visibility and path are not required.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.get_file_revision_bytes_by_inode",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "get_file_revision_bytes_by_inode",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn get_file_revision_bytes_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        self.core.record_trace_context(&tracing::Span::current());
        let (engine, read_context) = self.core.pinned_metadata_read(namespace_id).await?;
        let bytes = engine
            .get_file_revision_for_inode(
                inode_id,
                revision_no,
                &read_context,
                self.core.inner.config.max_read_content_bytes,
            )
            .await?;
        Ok(bytes)
    }

    /// Reads the ordered change feed after the `after_seq` cursor.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.list_changes",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "list_changes",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn list_changes(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        options: ListChangesOptions,
    ) -> Result<ChangesResponse> {
        self.core.record_trace_context(&tracing::Span::current());
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

    /// Creates a change-feed pager beginning after `after_seq`.
    pub fn list_changes_pager(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        options: ListChangesOptions,
    ) -> ChangesPager {
        ChangesPager {
            reader: self.clone(),
            namespace_id: namespace_id.clone(),
            after_seq,
            options,
            pending: None,
            exhausted: false,
        }
    }
}
