//! [`FsReader`]'s read operations: stat, list, content, revision reads, the
//! change feed, and the whole-namespace reads a consumer that derives its
//! own data from the filesystem walks.

use super::core::{default_page_limit, encode_next_cursor, file_revisions_page_response};
use crate::downloads::{DirectDownloadByInodeTarget, DirectDownloadTarget};
use crate::FsReader;
use crate::Result;
use crate::{
    AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq, ChangesResponse,
    CheckpointFilesPage, CheckpointFilesPageCursor, CheckpointId, ContentRef, CoreError,
    CurrentFileState, FileContentStream, InodeId, ListChangesOptions, ListFileRevisionsResponse,
    ListPathEntriesOptions, ListPathEntriesResponse, NamespaceId, ReadFileStreamOptions,
    RevisionNo, RuntimeError, SharedObjectStore, StatPathOptions,
};
use loonfs_api::{
    AbsolutePath, DirectoryPageCursor, FileRevisionsPageCursor, PageRequest, PaginationPolicy,
};

impl FsReader {
    /// Resolves an absolute path to its authoritative entry at the current
    /// head, projecting what `options` asks for.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.stat",
        err(level = "debug"),
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
        let (first_page, mut next_cursor) = self
            .list_path_entries_page_typed(
                namespace_id,
                absolute_path,
                PageRequest {
                    limit,
                    cursor: None,
                },
                ListPathEntriesOptions::default(),
            )
            .await?;
        let mut envelope = first_page;
        while let Some(cursor) = next_cursor {
            let (page, page_next_cursor) = self
                .list_path_entries_page_typed(
                    namespace_id,
                    absolute_path,
                    PageRequest {
                        limit,
                        cursor: Some(cursor),
                    },
                    ListPathEntriesOptions::default(),
                )
                .await?;
            envelope.head_seq = envelope.head_seq.max(page.head_seq);
            envelope.entries.extend(page.entries);
            next_cursor = page_next_cursor;
        }
        Ok(envelope)
    }

    /// Lists one page of a directory, projecting what `options` asks for.
    ///
    /// Asking for attributes costs one lookup per entry and adds an unbounded
    /// number of bytes to the page, so a caller that turns the projection on
    /// should also size its page for the maps it expects back.
    pub async fn list_path_entries_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
        options: ListPathEntriesOptions,
    ) -> Result<ListPathEntriesResponse> {
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
    pub async fn get_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativeFileBytes> {
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
    pub async fn read_file_stream(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: ReadFileStreamOptions,
    ) -> Result<FileContentStream<SharedObjectStore>> {
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
    pub async fn direct_download_target(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        revision_no: Option<RevisionNo>,
    ) -> Result<DirectDownloadTarget> {
        let (engine, read_context) = self.core.pinned_metadata_read(namespace_id).await?;
        let target = engine
            .direct_download_target(absolute_path, revision_no, &read_context)
            .await?;
        Ok(target)
    }

    /// Resolves retained inode content for a direct download without
    /// requiring a current path.
    pub async fn direct_download_target_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<DirectDownloadByInodeTarget> {
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

    /// Lists one page of a file path's revision history.
    pub async fn list_file_revisions_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
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

    /// Lists one page of retained revisions for a file inode.
    pub async fn list_file_revisions_by_inode_page(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
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

    /// Reads the content of one historical file revision by path.
    pub async fn get_file_revision_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        revision_no: RevisionNo,
    ) -> Result<AuthoritativeFileBytes> {
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
    pub async fn get_file_revision_bytes_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
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
