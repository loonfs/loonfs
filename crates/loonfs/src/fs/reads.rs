//! Read-only namespace and filesystem operations for [`FsReader`].

use super::core::{encode_next_cursor, file_revisions_page_response};
use crate::downloads::{DirectDownloadByInodeTarget, DirectDownloadTarget};
use crate::FsReader;
use crate::Result;
use crate::{
    ChangeSeq, CheckpointFilesPage, CheckpointFilesPageCursor, CheckpointId, ContentRef, CoreError,
    CurrentFileState, FileBytes, FileContentStream, InodeId, ListChangesOptions,
    ListChangesResponse, ListFileRevisionsResponse, ListInodeChildrenOptions,
    ListInodeChildrenResponse, ListPathEntriesOptions, ListPathEntriesResponse, Namespace,
    NamespaceId, PathEntry, ReadFileStreamOptions, RevisionNo, RuntimeError, SharedObjectStore,
    StatPathOptions,
};
use loonfs_api::{
    AbsolutePath, DirectoryPageCursor, FileRevisionsPageCursor, PageCursor, PageRequest,
    PaginationPolicy, TrashPageCursor,
};
use loonfs_core::{NamespaceReaderEngine, RuntimeReadContext};

/// Runtime readers require callers to pin snapshots explicitly.
fn reject_snapshot_option(snapshot_id: &Option<CheckpointId>, reader: &str) -> Result<()> {
    if snapshot_id.is_some() {
        return Err(loonfs_core::Error::InvalidCheckpointRequest(format!(
            "snapshot_id is not supported by {reader}"
        ))
        .into());
    }
    Ok(())
}

fn validate_pinned_directory_cursor(
    cursor: Option<&DirectoryPageCursor>,
    pinned_head_seq: ChangeSeq,
    snapshot_id: Option<&CheckpointId>,
) -> Result<()> {
    let Some(cursor) = cursor else {
        return Ok(());
    };
    if cursor.head_seq != pinned_head_seq {
        return Err(CoreError::InvalidCursor(format!(
            "directory cursor head `{}` does not match pinned head `{pinned_head_seq}`",
            cursor.head_seq
        ))
        .into());
    }
    match (&cursor.snapshot_id, snapshot_id) {
        (Some(actual), Some(expected)) if actual == expected => Ok(()),
        (Some(actual), Some(expected)) => Err(CoreError::InvalidCursor(format!(
            "directory cursor snapshot `{actual}` does not match requested snapshot `{expected}`"
        ))
        .into()),
        (Some(actual), None) => Err(CoreError::InvalidCursor(format!(
            "directory cursor is bound to snapshot `{actual}`"
        ))
        .into()),
        (None, Some(expected)) => Err(CoreError::InvalidCursor(format!(
            "directory cursor is not bound to snapshot `{expected}`"
        ))
        .into()),
        (None, None) => Ok(()),
    }
}

fn reject_snapshot_bound_directory_cursor(cursor: Option<&DirectoryPageCursor>) -> Result<()> {
    let Some(snapshot_id) = cursor.and_then(|cursor| cursor.snapshot_id.as_ref()) else {
        return Ok(());
    };
    Err(CoreError::InvalidCursor(format!(
        "directory cursor is bound to snapshot `{snapshot_id}`; repeat snapshot_id on every page"
    ))
    .into())
}

/// A namespace metadata view pinned to one head sequence.
///
/// Create one from the current state, a checkpoint, or a live snapshot. All
/// reads through the value use the same state even if new commits arrive.
#[must_use]
pub struct FsReadSnapshot {
    engine: NamespaceReaderEngine<SharedObjectStore>,
    context: RuntimeReadContext,
    snapshot_id: Option<CheckpointId>,
    max_read_content_bytes: Option<u64>,
}

impl FsReadSnapshot {
    /// Returns the namespace this snapshot reads.
    pub fn namespace_id(&self) -> &NamespaceId {
        self.engine.namespace_id()
    }

    /// Returns the head sequence this snapshot is pinned to.
    pub fn head_seq(&self) -> ChangeSeq {
        self.context.head.seq
    }

    /// Resolves an absolute path against this snapshot.
    pub async fn get_path_entry(
        &self,
        absolute_path: &str,
        options: StatPathOptions,
    ) -> Result<PathEntry> {
        reject_snapshot_option(
            &options.snapshot_id,
            "FsReadSnapshot because it is already pinned",
        )?;
        Ok(self
            .engine
            .resolve_path(absolute_path, options, &self.context)
            .await?)
    }

    /// Lists one directory page against this snapshot.
    pub async fn list_path_entries_page(
        &self,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
        options: ListPathEntriesOptions,
    ) -> Result<ListPathEntriesResponse> {
        reject_snapshot_option(&options.snapshot_id, "here; this view is already pinned")?;
        validate_pinned_directory_cursor(
            request.cursor.as_ref(),
            self.head_seq(),
            self.snapshot_id.as_ref(),
        )?;
        let listed_path = AbsolutePath::parse(absolute_path)
            .map_err(|error| CoreError::InvalidPath(error.to_string()))?;
        let mut page = self
            .engine
            .list_path_page(listed_path.as_str(), request, options, &self.context)
            .await?;
        if let (Some(cursor), Some(snapshot_id)) =
            (page.next_cursor.as_mut(), self.snapshot_id.as_ref())
        {
            cursor.snapshot_id = Some(snapshot_id.clone());
        }
        Ok(ListPathEntriesResponse {
            namespace_id: self.namespace_id().clone(),
            path: listed_path,
            head_seq: self.head_seq(),
            entries: page.items,
            next_cursor: encode_next_cursor(page.next_cursor.as_ref())?,
        })
    }

    /// Resolves current visibility, revision, and path against this snapshot.
    pub async fn resolve_current_files(
        &self,
        inode_ids: &[InodeId],
    ) -> Result<Vec<CurrentFileState>> {
        Ok(self
            .engine
            .resolve_current_files(inode_ids, &self.context)
            .await?)
    }

    /// Reads and verifies immutable content selected from this snapshot.
    pub async fn read_content_ref(
        &self,
        content_ref: &ContentRef,
        max_bytes: u64,
    ) -> Result<Vec<u8>> {
        Ok(self
            .engine
            .read_content_ref(content_ref, max_bytes, &self.context)
            .await?)
    }

    /// Reads the file selected by this snapshot.
    pub async fn get_file_bytes(&self, absolute_path: &str) -> Result<FileBytes> {
        Ok(self
            .engine
            .get_file(absolute_path, &self.context, self.max_read_content_bytes)
            .await?)
    }

    /// Resolves the file selected by this snapshot for a direct download.
    pub async fn create_download(&self, absolute_path: &str) -> Result<DirectDownloadTarget> {
        Ok(self
            .engine
            .direct_download_target(absolute_path, None, &self.context)
            .await?)
    }
}

/// A pager over directory entries.
pub type PathEntriesPager = loonfs_api::Pager<ListPathEntriesResponse, RuntimeError>;
/// A pager over directory children addressed by inode.
pub type InodeChildrenPager = loonfs_api::Pager<ListInodeChildrenResponse, RuntimeError>;
/// A pager over retained file revisions.
pub type FileRevisionsPager = loonfs_api::Pager<ListFileRevisionsResponse, RuntimeError>;
/// A pager over recoverable deletions.
pub type TrashPager = loonfs_api::Pager<loonfs_api::ListTrashResponse, RuntimeError>;
/// A pager over committed changes.
pub type ChangesPager = loonfs_api::Pager<ListChangesResponse, RuntimeError>;

fn encoded_pager_cursor<C: PageCursor>(cursor: Option<&C>) -> Option<String> {
    cursor.map(|cursor| loonfs_api::encode_cursor(cursor).expect("typed page cursor should encode"))
}

fn pager_request<C: PageCursor>(
    limit: loonfs_api::EffectiveLimit,
    cursor: Option<String>,
) -> Result<PageRequest<C>> {
    let cursor = cursor
        .as_deref()
        .map(loonfs_api::decode_cursor)
        .transpose()
        .map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
    Ok(PageRequest { limit, cursor })
}

impl FsReader {
    /// Pins one namespace metadata view for a sequence of related reads.
    ///
    /// The returned snapshot keeps path lookup, directory listing, inode
    /// resolution, and content selection on the same head even if commits
    /// publish concurrently. It is intended to be short-lived for one
    /// request or unit of work.
    pub async fn pin_namespace(&self, namespace_id: &NamespaceId) -> Result<FsReadSnapshot> {
        self.core.record_trace_context(&tracing::Span::current());
        let (engine, context) = self.core.pinned_metadata_read(namespace_id).await?;
        Ok(FsReadSnapshot {
            engine,
            context,
            snapshot_id: None,
            max_read_content_bytes: self.core.inner.config.max_read_content_bytes,
        })
    }

    /// Pins the namespace state captured by a checkpoint.
    ///
    /// Missing or released checkpoints return `checkpoint_unavailable`.
    pub async fn pin_namespace_at_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<FsReadSnapshot> {
        self.core.record_trace_context(&tracing::Span::current());
        let (engine, context) = self
            .core
            .pinned_read_at_checkpoint(namespace_id, checkpoint_id)
            .await?;
        Ok(FsReadSnapshot {
            engine,
            context,
            snapshot_id: None,
            max_read_content_bytes: self.core.inner.config.max_read_content_bytes,
        })
    }

    /// Pins the namespace state captured by a live snapshot.
    pub async fn pin_namespace_at_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &CheckpointId,
    ) -> Result<FsReadSnapshot> {
        self.core.record_trace_context(&tracing::Span::current());
        let now_ms = loonfs_core::time::current_time_ms()?;
        let (engine, context) = self
            .core
            .pinned_read_at_snapshot(namespace_id, snapshot_id, now_ms)
            .await?;
        self.core.inner.cache_stats.record_snapshot_view_read();
        Ok(FsReadSnapshot {
            engine,
            context,
            snapshot_id: Some(snapshot_id.clone()),
            max_read_content_bytes: self.core.inner.config.max_read_content_bytes,
        })
    }

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
    pub async fn get_path_entry(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: StatPathOptions,
    ) -> Result<PathEntry> {
        reject_snapshot_option(
            &options.snapshot_id,
            "FsReader; call pin_namespace_at_snapshot first",
        )?;
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        let (engine, read_context) = self.core.pinned_metadata_read(namespace_id).await?;
        let entry = engine
            .resolve_path(absolute_path, options, &read_context)
            .await?;
        tracing::Span::current().record("cache_path", crate::trace::CACHE_MATERIALIZED_SEGMENTS);
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
    pub async fn get_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: StatPathOptions,
    ) -> Result<PathEntry> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        let (engine, read_context) = self.core.pinned_metadata_read(namespace_id).await?;
        let entry = engine.stat_inode(inode_id, options, &read_context).await?;
        tracing::Span::current().record("cache_path", crate::trace::CACHE_MATERIALIZED_SEGMENTS);
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
        let cursor = encoded_pager_cursor(request.cursor.as_ref());
        let limit = request.limit;
        let reader = self.clone();
        let namespace_id = namespace_id.clone();
        let absolute_path = absolute_path.to_owned();
        loonfs_api::Pager::new(cursor, move |cursor| {
            let reader = reader.clone();
            let namespace_id = namespace_id.clone();
            let absolute_path = absolute_path.clone();
            let options = options.clone();
            async move {
                reader
                    .list_path_entries_page(
                        &namespace_id,
                        &absolute_path,
                        pager_request(limit, cursor)?,
                        options,
                    )
                    .await
            }
        })
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
        reject_snapshot_option(
            &options.snapshot_id,
            "FsReader; call pin_namespace_at_snapshot first",
        )?;
        reject_snapshot_bound_directory_cursor(request.cursor.as_ref())?;
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
        let page = engine
            .list_path_page(listed_path.as_str(), request, options, &read_context)
            .await?;
        let head_seq = read_context.head.seq;
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

    /// Creates a children pager for one directory inode beginning at
    /// `request.cursor`.
    pub fn list_inode_children_pager(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        request: PageRequest<DirectoryPageCursor>,
        options: ListInodeChildrenOptions,
    ) -> InodeChildrenPager {
        let cursor = encoded_pager_cursor(request.cursor.as_ref());
        let limit = request.limit;
        let reader = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_api::Pager::new(cursor, move |cursor| {
            let reader = reader.clone();
            let namespace_id = namespace_id.clone();
            let options = options.clone();
            async move {
                reader
                    .list_inode_children_page(
                        &namespace_id,
                        inode_id,
                        pager_request(limit, cursor)?,
                        options,
                    )
                    .await
            }
        })
    }

    /// Lists one page of a directory's children by inode, projecting what
    /// `options` asks for.
    ///
    /// The parent is addressed by its stable inode identity, so a page and
    /// its resumption always describe the same directory even when the
    /// parent is concurrently renamed or moved.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.list_inode_children",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "list_inode_children",
            method = "list_inode_children_page",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn list_inode_children_page(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        request: PageRequest<DirectoryPageCursor>,
        options: ListInodeChildrenOptions,
    ) -> Result<ListInodeChildrenResponse> {
        reject_snapshot_bound_directory_cursor(request.cursor.as_ref())?;
        self.core.record_trace_context(&tracing::Span::current());
        let (engine, read_context) = self.core.pinned_metadata_read(namespace_id).await?;
        let page = engine
            .list_inode_children_page(inode_id, request, options, &read_context)
            .await?;
        let head_seq = read_context.head.seq;
        let next_cursor = encode_next_cursor(page.next_cursor.as_ref())?;
        Ok(ListInodeChildrenResponse {
            namespace_id: namespace_id.clone(),
            parent_inode_id: inode_id,
            head_seq,
            entries: page.items,
            next_cursor,
        })
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
    ) -> Result<FileBytes> {
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
    /// Each ranged read uses bounded memory. Size and checksum verification
    /// complete when [`FileContentStream::next_chunk`] returns `None`; stopping
    /// early leaves the content unverified. The buffered-read size limit does
    /// not apply.
    ///
    /// [`ReadFileStreamOptions::start_offset`] resumes a read. The caller must
    /// supply earlier bytes for whole-object verification through
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
    pub async fn create_download(
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
    pub async fn create_download_by_inode(
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

    /// Lists files visible at a checkpoint in ascending inode-ID order.
    ///
    /// The pinned manifest is read without replaying later WAL entries.
    /// Directories are omitted. An unavailable checkpoint returns
    /// `checkpoint_unavailable` rather than falling back to current state.
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

    /// Resolves the current state of each inode ID.
    ///
    /// Results use one pinned read and preserve input order. Unknown IDs return
    /// `visible: false`. Directories have a path but no revision.
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
        let cursor = encoded_pager_cursor(request.cursor.as_ref());
        let limit = request.limit;
        let reader = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_api::Pager::new(cursor, move |cursor| {
            let reader = reader.clone();
            let namespace_id = namespace_id.clone();
            async move {
                reader
                    .list_trash_page(&namespace_id, pager_request(limit, cursor)?)
                    .await
            }
        })
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
        let cursor = encoded_pager_cursor(request.cursor.as_ref());
        let limit = request.limit;
        let reader = self.clone();
        let namespace_id = namespace_id.clone();
        let absolute_path = absolute_path.to_owned();
        loonfs_api::Pager::new(cursor, move |cursor| {
            let reader = reader.clone();
            let namespace_id = namespace_id.clone();
            let absolute_path = absolute_path.clone();
            async move {
                reader
                    .list_file_revisions_page(
                        &namespace_id,
                        &absolute_path,
                        pager_request(limit, cursor)?,
                    )
                    .await
            }
        })
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
        let cursor = encoded_pager_cursor(request.cursor.as_ref());
        let limit = request.limit;
        let reader = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_api::Pager::new(cursor, move |cursor| {
            let reader = reader.clone();
            let namespace_id = namespace_id.clone();
            async move {
                reader
                    .list_file_revisions_by_inode_page(
                        &namespace_id,
                        inode_id,
                        pager_request(limit, cursor)?,
                    )
                    .await
            }
        })
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
    ) -> Result<FileBytes> {
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
    ) -> Result<ListChangesResponse> {
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
        let reader = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_api::Pager::new(Some(after_seq), move |after_seq| {
            let reader = reader.clone();
            let namespace_id = namespace_id.clone();
            let options = options.clone();
            async move {
                reader
                    .list_changes(
                        &namespace_id,
                        after_seq.expect("change pager should carry a sequence"),
                        options,
                    )
                    .await
            }
        })
    }
}
