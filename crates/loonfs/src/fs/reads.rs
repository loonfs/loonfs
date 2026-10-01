//! Namespace and filesystem reads on a [`Namespace`] handle in any mode.

use super::core::{decode_page_request, encode_next_cursor, file_revisions_page_response};
use crate::downloads::{DirectDownloadByInodeTarget, DirectDownloadTarget};
use crate::Result;
use crate::{
    ChangeSeq, CheckpointFilesPage, CheckpointFilesPageCursor, ContentRef, CoreError,
    CurrentFileState, Error, FileBytes, FileContentStream, InodeId, ListChangesResponse,
    ListCheckpointFilesOptions, ListFileRevisionsResponse, ListInodeChildrenResponse, ListOptions,
    ListPathEntriesResponse, Namespace, NamespaceId, PathEntry, PinId, ReadFileStreamOptions,
    RevisionNo, SharedObjectStore, StatOptions,
};
use loonfs_api::{
    AbsolutePath, DirectoryPageCursor, EffectiveLimit, FileRevisionsPageCursor, PageRequest,
    TrashPageCursor,
};
use loonfs_core::{NamespaceReaderEngine, RuntimeReadContext};
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// Rejects a directory cursor that another read minted. A checkpoint or
/// snapshot implies its head, so the head test does work only for a view of
/// the live head.
fn validate_view_directory_cursor(
    cursor: Option<&DirectoryPageCursor>,
    captured_seq: ChangeSeq,
    pin_id: Option<&PinId>,
) -> Result<()> {
    let Some(cursor) = cursor else {
        return Ok(());
    };
    if cursor.head_seq != captured_seq {
        return Err(CoreError::InvalidCursor(format!(
            "directory cursor head `{}` does not match the read view head `{captured_seq}`",
            cursor.head_seq
        ))
        .into());
    }
    match (&cursor.pin_id, pin_id) {
        (Some(actual), Some(expected)) if actual == expected => Ok(()),
        (Some(actual), Some(expected)) => Err(CoreError::InvalidCursor(format!(
            "directory cursor pin `{actual}` does not match requested pin `{expected}`"
        ))
        .into()),
        (Some(actual), None) => Err(CoreError::InvalidCursor(format!(
            "directory cursor is bound to pin `{actual}`"
        ))
        .into()),
        (None, Some(expected)) => Err(CoreError::InvalidCursor(format!(
            "directory cursor is not bound to pin `{expected}`"
        ))
        .into()),
        (None, None) => Ok(()),
    }
}

/// Reads one page of the change feed at the head `context` serves. The
/// feed's position is `after_seq`; it has no cursor parameter.
async fn change_feed_page(
    engine: &NamespaceReaderEngine<SharedObjectStore>,
    context: &RuntimeReadContext,
    after_seq: ChangeSeq,
    limit: EffectiveLimit,
) -> Result<ListChangesResponse> {
    engine.require_administrator(context).await?;
    engine
        .list_changes_after(after_seq, limit, context)
        .await
        .map_err(|error| match error {
            CoreError::InvalidCursor(message) => Error::InvalidRequest {
                message,
                param: "after_seq",
            },
            error => error.into(),
        })
}

fn reject_pinned_directory_cursor(cursor: Option<&DirectoryPageCursor>) -> Result<()> {
    let Some(pin_id) = cursor.and_then(|cursor| cursor.pin_id.as_ref()) else {
        return Ok(());
    };
    Err(CoreError::InvalidCursor(format!(
        "directory cursor is bound to pin `{pin_id}`; resume it from the same snapshot or checkpoint"
    ))
    .into())
}

/// A captured namespace state that related reads share.
///
/// A [`Namespace`] handle reads the current state on every call. A read view
/// keeps related reads on one captured state, even when new commits publish.
/// A durable snapshot preserves a state for later use. A read view retains
/// nothing in the store and is meant to live for one request or unit of
/// work. It can capture the current state, a checkpoint, or a live snapshot.
#[must_use]
#[derive(Clone)]
pub struct ReadView {
    engine: Arc<NamespaceReaderEngine<SharedObjectStore>>,
    core: super::RuntimeCore,
    context: RuntimeReadContext,
    source: ReadSource,
}

#[derive(Clone)]
pub(super) enum ReadSource {
    Head,
    Checkpoint(PinId),
    Snapshot(PinId),
}

impl ReadView {
    async fn read<T>(&self, read: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        super::read_result::classify_read_result(
            &self.core,
            &self.context,
            &self.source,
            read.await,
        )
        .await
    }

    fn snapshot_id(&self) -> Option<&PinId> {
        match &self.source {
            ReadSource::Snapshot(snapshot_id) => Some(snapshot_id),
            ReadSource::Head | ReadSource::Checkpoint(_) => None,
        }
    }

    fn pin_id(&self) -> Option<&PinId> {
        match &self.source {
            ReadSource::Checkpoint(pin_id) | ReadSource::Snapshot(pin_id) => Some(pin_id),
            ReadSource::Head => None,
        }
    }

    /// Returns the namespace this view reads.
    pub fn namespace_id(&self) -> &NamespaceId {
        self.engine.namespace_id()
    }

    /// Returns the head sequence this view captured.
    pub fn head_seq(&self) -> ChangeSeq {
        self.context.head.seq
    }

    /// Shared read options may name the snapshot this view reads; naming any
    /// other snapshot would silently read the wrong one.
    fn require_same_snapshot(&self, requested: Option<&PinId>) -> Result<()> {
        let Some(requested) = requested else {
            return Ok(());
        };
        if Some(requested) != self.snapshot_id() {
            return Err(Error::InvalidRequest {
                message: format!(
                    "snapshot_id `{requested}` names a different snapshot than this read view"
                ),
                param: "snapshot_id",
            });
        }
        Ok(())
    }

    /// Lists the change feed after `after_seq` through this view's captured
    /// head.
    pub fn list_changes(&self, after_seq: ChangeSeq) -> ChangesPager {
        let view = self.clone();
        loonfs_api::Pager::new(move |request: PageRequest<ChangeSeq>| {
            let view = view.clone();
            async move {
                view.read(change_feed_page(
                    &view.engine,
                    &view.context,
                    request.cursor.unwrap_or(after_seq),
                    request.limit,
                ))
                .await
            }
        })
    }

    /// Resolves an absolute path against this view.
    pub async fn stat(&self, absolute_path: &str) -> Result<PathEntry> {
        self.stat_with_options(absolute_path, &StatOptions::default())
            .await
    }

    /// Resolves an absolute path against this view, projecting what
    /// `options` asks for.
    pub async fn stat_with_options(
        &self,
        absolute_path: &str,
        options: &StatOptions,
    ) -> Result<PathEntry> {
        self.read(async {
            self.require_same_snapshot(options.snapshot_id.as_ref())?;
            Ok(self
                .engine
                .resolve_path(absolute_path, options.clone(), &self.context)
                .await?)
        })
        .await
    }

    /// Lists a directory against this view.
    pub fn list(&self, absolute_path: &str) -> PathEntriesPager {
        self.list_with_options(absolute_path, &ListOptions::default())
    }

    /// Lists a directory against this view, projecting what `options` asks
    /// for.
    pub fn list_with_options(
        &self,
        absolute_path: &str,
        options: &ListOptions,
    ) -> PathEntriesPager {
        let view = self.clone();
        let absolute_path = absolute_path.to_owned();
        let options = options.clone();
        loonfs_api::Pager::new(move |request| {
            let view = view.clone();
            let absolute_path = absolute_path.clone();
            let options = options.clone();
            async move {
                view.path_entries_page(&absolute_path, decode_page_request(request)?, options)
                    .await
            }
        })
    }

    async fn path_entries_page(
        &self,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
        options: ListOptions,
    ) -> Result<ListPathEntriesResponse> {
        self.read(async {
            self.require_same_snapshot(options.snapshot_id.as_ref())?;
            validate_view_directory_cursor(
                request.cursor.as_ref(),
                self.head_seq(),
                self.pin_id(),
            )?;
            let listed_path = AbsolutePath::parse(absolute_path)
                .map_err(|error| CoreError::InvalidPath(error.to_string()))?;
            let mut page = self
                .engine
                .list_path_page(listed_path.as_str(), request, options, &self.context)
                .await?;
            if let Some(cursor) = page.next_cursor.as_mut() {
                cursor.pin_id = self.pin_id().cloned();
            }
            Ok(ListPathEntriesResponse {
                namespace_id: self.namespace_id().clone(),
                path: listed_path,
                head_seq: self.head_seq(),
                entries: page.items,
                next_cursor: encode_next_cursor(page.next_cursor.as_ref())?,
            })
        })
        .await
    }

    /// Reads a visible inode against this view.
    pub async fn stat_by_inode(&self, inode_id: InodeId) -> Result<PathEntry> {
        self.stat_by_inode_with_options(inode_id, &StatOptions::default())
            .await
    }

    /// Reads a visible inode against this view, projecting what `options`
    /// asks for.
    pub async fn stat_by_inode_with_options(
        &self,
        inode_id: InodeId,
        options: &StatOptions,
    ) -> Result<PathEntry> {
        self.read(async {
            self.require_same_snapshot(options.snapshot_id.as_ref())?;
            Ok(self
                .engine
                .stat_inode(inode_id, options.clone(), &self.context)
                .await?)
        })
        .await
    }

    /// Lists a directory inode's children against this view.
    pub fn list_by_inode(&self, inode_id: InodeId) -> InodeChildrenPager {
        self.list_by_inode_with_options(inode_id, &ListOptions::default())
    }

    /// Lists a directory inode's children against this view, projecting what
    /// `options` asks for.
    pub fn list_by_inode_with_options(
        &self,
        inode_id: InodeId,
        options: &ListOptions,
    ) -> InodeChildrenPager {
        let view = self.clone();
        let options = options.clone();
        loonfs_api::Pager::new(move |request| {
            let view = view.clone();
            let options = options.clone();
            async move {
                view.inode_children_page(inode_id, decode_page_request(request)?, options)
                    .await
            }
        })
    }

    async fn inode_children_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<DirectoryPageCursor>,
        options: ListOptions,
    ) -> Result<ListInodeChildrenResponse> {
        self.read(async {
            self.require_same_snapshot(options.snapshot_id.as_ref())?;
            validate_view_directory_cursor(
                request.cursor.as_ref(),
                self.head_seq(),
                self.pin_id(),
            )?;
            let mut page = self
                .engine
                .list_inode_children_page(inode_id, request, options, &self.context)
                .await?;
            if let Some(cursor) = page.next_cursor.as_mut() {
                cursor.pin_id = self.pin_id().cloned();
            }
            Ok(ListInodeChildrenResponse {
                namespace_id: self.namespace_id().clone(),
                parent_inode_id: inode_id,
                head_seq: self.head_seq(),
                entries: page.items,
                next_cursor: encode_next_cursor(page.next_cursor.as_ref())?,
            })
        })
        .await
    }

    /// Resolves current visibility, revision, and path against this view.
    ///
    /// Unreadable inodes return `visible: false` with no path or revision.
    pub async fn resolve_current_files(
        &self,
        inode_ids: &[InodeId],
    ) -> Result<Vec<CurrentFileState>> {
        self.read(async {
            Ok(self
                .engine
                .resolve_current_files(inode_ids, &self.context)
                .await?)
        })
        .await
    }

    /// Reads and verifies immutable content selected from this view.
    ///
    /// Requires namespace administrator access. Content must be published in
    /// this view, including through a fork. Otherwise returns
    /// `path_not_found` without reading content bytes.
    pub async fn read_content(&self, content_ref: &ContentRef, max_bytes: u64) -> Result<Vec<u8>> {
        self.read(async {
            Ok(self
                .engine
                .read_content_ref(content_ref, max_bytes, &self.context)
                .await?)
        })
        .await
    }

    /// Refuses a reader with no subject on an ACL namespace, for callers
    /// that could otherwise answer before any per-subject read.
    pub fn require_subject(&self) -> Result<()> {
        Ok(self.engine.require_subject(&self.context)?)
    }

    /// Reads one inode revision through this view's authorization.
    pub async fn read_file_revision_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        max_bytes: u64,
    ) -> Result<Vec<u8>> {
        self.read(async {
            Ok(self
                .engine
                .get_file_revision_for_inode(inode_id, revision_no, &self.context, Some(max_bytes))
                .await?)
        })
        .await
    }

    /// Reads the file selected by this view.
    pub async fn read_file(&self, absolute_path: &str) -> Result<FileBytes> {
        self.read(async {
            Ok(self
                .engine
                .get_file(
                    absolute_path,
                    &self.context,
                    self.core.inner.config.max_read_content_bytes,
                )
                .await?)
        })
        .await
    }

    /// Streams the file selected by this view in bounded chunks.
    /// Complete verification requires consuming the stream to its end.
    pub async fn read_file_stream(
        &self,
        absolute_path: &str,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        self.read_file_stream_with_options(absolute_path, &ReadFileStreamOptions::default())
            .await
    }

    /// Streams the file selected by this view in bounded chunks, as
    /// `options` asks. A view reads one state, so `options` may not name a
    /// revision.
    pub async fn read_file_stream_with_options(
        &self,
        absolute_path: &str,
        options: &ReadFileStreamOptions,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        self.read(async {
            if options.revision_no.is_some() {
                return Err(CoreError::InvalidCheckpointRequest(
                    "revision_no cannot be combined with a snapshot read".to_owned(),
                )
                .into());
            }
            Ok(self
                .engine
                .read_file_stream(
                    absolute_path,
                    &self.context,
                    None,
                    options.chunk_bytes,
                    options.start_offset,
                )
                .await?)
        })
        .await
    }

    /// Resolves the file selected by this view for a direct download.
    pub async fn create_download(&self, absolute_path: &str) -> Result<DirectDownloadTarget> {
        self.read(async {
            Ok(self
                .engine
                .direct_download_target(absolute_path, None, &self.context)
                .await?)
        })
        .await
    }
}

/// A pager over directory entries.
pub type PathEntriesPager = loonfs_api::Pager<ListPathEntriesResponse, Error>;
/// A pager over directory children addressed by inode.
pub type InodeChildrenPager = loonfs_api::Pager<ListInodeChildrenResponse, Error>;
/// A pager over retained file revisions.
pub type FileRevisionsPager = loonfs_api::Pager<ListFileRevisionsResponse, Error>;
/// A pager over recoverable deletions.
pub type TrashPager = loonfs_api::Pager<loonfs_api::ListTrashResponse, Error>;
/// A pager over committed changes.
pub type ChangesPager = loonfs_api::Pager<ListChangesResponse, Error>;
/// A pager over the files a checkpoint pins.
pub type CheckpointFilesPager = loonfs_api::Pager<CheckpointFilesPage, Error>;

impl<M> Namespace<M> {
    fn read_view_from(
        &self,
        engine: NamespaceReaderEngine<SharedObjectStore>,
        context: RuntimeReadContext,
        source: ReadSource,
    ) -> ReadView {
        ReadView {
            engine: Arc::new(engine),
            core: self.core.clone(),
            context,
            source,
        }
    }

    /// Captures the current namespace state for a group of related reads.
    ///
    /// The returned view keeps path lookup, directory listing, inode
    /// resolution, and content selection on the same head even if commits
    /// publish concurrently. It retains nothing in the store and is meant to
    /// live for one request or unit of work.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.read_view",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "read_view",
            method = "read_view",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn read_view(&self) -> Result<ReadView> {
        self.core.record_trace_context(&tracing::Span::current());
        let (engine, context) = self.core.pinned_metadata_read(&self.namespace_id).await?;
        Ok(self.read_view_from(engine, context, ReadSource::Head))
    }

    /// Captures the namespace state a checkpoint preserves.
    ///
    /// An id that names no user checkpoint returns `checkpoint_not_found`.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.read_view_at_checkpoint",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "read_view_at_checkpoint",
            method = "read_view_at_checkpoint",
            namespace_id = %self.namespace_id,
            checkpoint_id = %checkpoint_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn read_view_at_checkpoint(&self, checkpoint_id: &PinId) -> Result<ReadView> {
        self.core.record_trace_context(&tracing::Span::current());
        let (engine, context) = self
            .core
            .pinned_read_at_checkpoint(&self.namespace_id, checkpoint_id)
            .await?;
        Ok(self.read_view_from(
            engine,
            context,
            ReadSource::Checkpoint(checkpoint_id.clone()),
        ))
    }

    /// Captures the namespace state a live snapshot preserves.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.read_view_at_snapshot",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "read_view_at_snapshot",
            method = "read_view_at_snapshot",
            namespace_id = %self.namespace_id,
            snapshot_id = %snapshot_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn read_view_at_snapshot(&self, snapshot_id: &PinId) -> Result<ReadView> {
        self.core.record_trace_context(&tracing::Span::current());
        let (engine, context) = self
            .core
            .pinned_read_at_snapshot(&self.namespace_id, snapshot_id)
            .await?;
        self.core.inner.instruments.snapshot_view_read();
        Ok(self.read_view_from(engine, context, ReadSource::Snapshot(snapshot_id.clone())))
    }

    /// Returns this namespace's metadata: its access mode, creation, fork
    /// basis, current head, and retention floor.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.metadata",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "metadata",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn metadata(&self) -> Result<crate::NamespaceMetadata> {
        self.core.record_trace_context(&tracing::Span::current());
        Ok(loonfs_core::cache::load_namespace(self.core.store(), &self.namespace_id).await?)
    }

    /// Resolves an absolute path to its authoritative entry at the current
    /// head.
    pub async fn stat(&self, absolute_path: &str) -> Result<PathEntry> {
        self.stat_with_options(absolute_path, &StatOptions::default())
            .await
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
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn stat_with_options(
        &self,
        absolute_path: &str,
        options: &StatOptions,
    ) -> Result<PathEntry> {
        if let Some(snapshot_id) = &options.snapshot_id {
            return self
                .read_view_at_snapshot(snapshot_id)
                .await?
                .stat_with_options(absolute_path, options)
                .await;
        }
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        self.core
            .read(&self.namespace_id, |engine, read_context| {
                let options = options.clone();
                async move {
                    let entry = engine
                        .resolve_path(absolute_path, options, &read_context)
                        .await?;
                    Ok(entry)
                }
            })
            .await
    }

    /// Returns the current entry for a visible inode.
    pub async fn stat_by_inode(&self, inode_id: InodeId) -> Result<PathEntry> {
        self.stat_by_inode_with_options(inode_id, &StatOptions::default())
            .await
    }

    /// Returns the current entry for a visible inode, projecting what
    /// `options` asks for.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.stat_by_inode",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "stat_by_inode",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn stat_by_inode_with_options(
        &self,
        inode_id: InodeId,
        options: &StatOptions,
    ) -> Result<PathEntry> {
        if let Some(snapshot_id) = &options.snapshot_id {
            return self
                .read_view_at_snapshot(snapshot_id)
                .await?
                .stat_by_inode_with_options(inode_id, options)
                .await;
        }
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        self.core
            .read(&self.namespace_id, |engine, read_context| {
                let options = options.clone();
                async move {
                    let entry = engine.stat_inode(inode_id, options, &read_context).await?;
                    Ok(entry)
                }
            })
            .await
    }

    /// Lists a directory. Each page reads the head that is current when the
    /// page is fetched.
    pub fn list(&self, absolute_path: &str) -> PathEntriesPager {
        self.list_with_options(absolute_path, &ListOptions::default())
    }

    /// Lists a directory, projecting what `options` asks for.
    ///
    /// Asking for attributes costs one lookup per entry and adds an unbounded
    /// number of bytes to each page, so a caller that turns the projection on
    /// should also size its pages for the maps it expects back.
    pub fn list_with_options(
        &self,
        absolute_path: &str,
        options: &ListOptions,
    ) -> PathEntriesPager {
        let reader = self.read_only();
        let absolute_path = absolute_path.to_owned();
        let options = options.clone();
        loonfs_api::Pager::new(move |request| {
            let reader = reader.clone();
            let absolute_path = absolute_path.clone();
            let options = options.clone();
            async move {
                reader
                    .path_entries_page(&absolute_path, decode_page_request(request)?, options)
                    .await
            }
        })
    }

    #[tracing::instrument(
        level = "debug",
        name = "loonfs.list",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "list",
            method = "list",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    async fn path_entries_page(
        &self,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
        options: ListOptions,
    ) -> Result<ListPathEntriesResponse> {
        if let Some(snapshot_id) = &options.snapshot_id {
            return self
                .read_view_at_snapshot(snapshot_id)
                .await?
                .path_entries_page(absolute_path, request, options)
                .await;
        }
        reject_pinned_directory_cursor(request.cursor.as_ref())?;
        self.core.record_trace_context(&tracing::Span::current());
        let listed_path = AbsolutePath::parse(absolute_path)
            .map_err(|error| CoreError::InvalidPath(error.to_string()))?;
        self.core
            .read(&self.namespace_id, |engine, read_context| {
                let request = request.clone();
                let options = options.clone();
                let listed_path = listed_path.clone();
                async move {
                    // Give awakened validation waiters a turn before synchronous page work.
                    // The head is already captured and the validation guard has been released.
                    tokio::task::yield_now().await;
                    let page = engine
                        .list_path_page(listed_path.as_str(), request, options, &read_context)
                        .await?;
                    Ok(ListPathEntriesResponse {
                        namespace_id: self.namespace_id.clone(),
                        path: listed_path,
                        head_seq: read_context.head.seq,
                        entries: page.items,
                        next_cursor: encode_next_cursor(page.next_cursor.as_ref())?,
                    })
                }
            })
            .await
    }

    /// Lists a directory's children by inode. Each page reads the head that
    /// is current when the page is fetched.
    ///
    /// The parent is addressed by its stable inode identity, so a page and
    /// its resumption always describe the same directory even when the
    /// parent is concurrently renamed or moved.
    pub fn list_by_inode(&self, inode_id: InodeId) -> InodeChildrenPager {
        self.list_by_inode_with_options(inode_id, &ListOptions::default())
    }

    /// Lists a directory's children by inode, projecting what `options` asks
    /// for.
    pub fn list_by_inode_with_options(
        &self,
        inode_id: InodeId,
        options: &ListOptions,
    ) -> InodeChildrenPager {
        let reader = self.read_only();
        let options = options.clone();
        loonfs_api::Pager::new(move |request| {
            let reader = reader.clone();
            let options = options.clone();
            async move {
                reader
                    .inode_children_page(inode_id, decode_page_request(request)?, options)
                    .await
            }
        })
    }

    #[tracing::instrument(
        level = "debug",
        name = "loonfs.list_by_inode",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "list_by_inode",
            method = "list_by_inode",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    async fn inode_children_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<DirectoryPageCursor>,
        options: ListOptions,
    ) -> Result<ListInodeChildrenResponse> {
        if let Some(snapshot_id) = &options.snapshot_id {
            return self
                .read_view_at_snapshot(snapshot_id)
                .await?
                .inode_children_page(inode_id, request, options)
                .await;
        }
        reject_pinned_directory_cursor(request.cursor.as_ref())?;
        self.core.record_trace_context(&tracing::Span::current());
        self.core
            .read(&self.namespace_id, |engine, read_context| {
                let request = request.clone();
                let options = options.clone();
                async move {
                    // Give awakened validation waiters a turn before synchronous page work.
                    // The head is already captured and the validation guard has been released.
                    tokio::task::yield_now().await;
                    let page = engine
                        .list_inode_children_page(inode_id, request, options, &read_context)
                        .await?;
                    let head_seq = read_context.head.seq;
                    let next_cursor = encode_next_cursor(page.next_cursor.as_ref())?;
                    Ok(ListInodeChildrenResponse {
                        namespace_id: self.namespace_id.clone(),
                        parent_inode_id: inode_id,
                        head_seq,
                        entries: page.items,
                        next_cursor,
                    })
                }
            })
            .await
    }

    /// Reads a file's current content plus the metadata entry it came from.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.read_file",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "read_file",
            method = "read_file",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn read_file(&self, absolute_path: &str) -> Result<FileBytes> {
        self.core.record_trace_context(&tracing::Span::current());
        self.get_current_file_bytes(absolute_path).await
    }

    /// Reads a file's current content as bounded chunks instead of one buffer.
    ///
    /// See [`Self::read_file_stream_with_options`] for how the stream is
    /// verified.
    pub async fn read_file_stream(
        &self,
        absolute_path: &str,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        self.read_file_stream_with_options(absolute_path, &ReadFileStreamOptions::default())
            .await
    }

    /// Reads a file as bounded chunks, as `options` asks.
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
        name = "loonfs.read_file_stream",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "read_file_stream",
            method = "read_file_stream",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn read_file_stream_with_options(
        &self,
        absolute_path: &str,
        options: &ReadFileStreamOptions,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        self.core.record_trace_context(&tracing::Span::current());
        let options = *options;
        self.core
            .read(&self.namespace_id, |engine, read_context| async move {
                let stream = engine
                    .read_file_stream(
                        absolute_path,
                        &read_context,
                        options.revision_no,
                        options.chunk_bytes,
                        options.start_offset,
                    )
                    .await?;
                Ok(stream)
            })
            .await
    }

    /// Prepares a content object for a direct download.
    ///
    /// See the API specification's download transport contract. The handle's
    /// `max_read_content_bytes` does not apply to direct downloads.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.create_download",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "create_download",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn create_download(
        &self,
        absolute_path: &str,
        revision_no: Option<RevisionNo>,
    ) -> Result<DirectDownloadTarget> {
        self.core.record_trace_context(&tracing::Span::current());
        self.core
            .read(&self.namespace_id, |engine, read_context| async move {
                let target = engine
                    .direct_download_target(absolute_path, revision_no, &read_context)
                    .await?;
                Ok(target)
            })
            .await
    }

    /// Resolves retained inode content for a direct download without
    /// requiring a current path.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.create_download_by_inode",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "create_download_by_inode",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn create_download_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<DirectDownloadByInodeTarget> {
        self.core.record_trace_context(&tracing::Span::current());
        self.core
            .read(&self.namespace_id, |engine, read_context| async move {
                let target = engine
                    .direct_download_target_by_inode(inode_id, revision_no, &read_context)
                    .await?;
                Ok(target)
            })
            .await
    }

    /// Lists files visible at a checkpoint in ascending inode-ID order.
    ///
    /// The pinned manifest is read without replaying later WAL entries.
    /// Directories are omitted. An id that names no user checkpoint returns
    /// `checkpoint_not_found` rather than falling back to current state.
    pub fn list_checkpoint_files(&self, checkpoint_id: &PinId) -> CheckpointFilesPager {
        self.list_checkpoint_files_with_options(
            checkpoint_id,
            &ListCheckpointFilesOptions::default(),
        )
    }

    /// Lists files at a checkpoint as `options` asks. With
    /// [`ListCheckpointFilesOptions::include_deleted`], the pages also list
    /// deleted files and files under a deleted directory.
    pub fn list_checkpoint_files_with_options(
        &self,
        checkpoint_id: &PinId,
        options: &ListCheckpointFilesOptions,
    ) -> CheckpointFilesPager {
        let reader = self.read_only();
        let checkpoint_id = checkpoint_id.clone();
        let options = *options;
        loonfs_api::Pager::new(move |request| {
            let reader = reader.clone();
            let checkpoint_id = checkpoint_id.clone();
            async move {
                reader
                    .checkpoint_files_page(&checkpoint_id, request, options)
                    .await
            }
        })
    }

    #[tracing::instrument(
        level = "debug",
        name = "loonfs.list_checkpoint_files",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "list_checkpoint_files",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    async fn checkpoint_files_page(
        &self,
        checkpoint_id: &PinId,
        request: PageRequest<CheckpointFilesPageCursor>,
        options: ListCheckpointFilesOptions,
    ) -> Result<CheckpointFilesPage> {
        self.core.record_trace_context(&tracing::Span::current());
        let (engine, read_context) = self.core.pinned_read(&self.namespace_id).await?;
        engine
            .list_checkpoint_files_page(checkpoint_id, request, options, &read_context)
            .await
            .map_err(crate::Error::from)
    }

    /// Resolves the current state of each inode ID.
    ///
    /// Results use one read view and preserve input order. Missing or unreadable
    /// inodes return `visible: false` with no path or revision. Readable directories
    /// have a path but no revision.
    ///
    /// A batch larger than [`MAX_RESOLVE_CURRENT_FILES`](crate::MAX_RESOLVE_CURRENT_FILES)
    /// returns `invalid_request` before reading metadata.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.resolve_current_files",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "resolve_current_files",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn resolve_current_files(
        &self,
        inode_ids: &[InodeId],
    ) -> Result<Vec<CurrentFileState>> {
        self.core.record_trace_context(&tracing::Span::current());
        self.core
            .read(&self.namespace_id, |engine, read_context| async move {
                let states = engine
                    .resolve_current_files(inode_ids, &read_context)
                    .await?;
                Ok(states)
            })
            .await
    }

    /// Reads one immutable content object by reference.
    ///
    /// Requires namespace administrator access. Content must be published in
    /// the namespace's read view, including through a fork. Otherwise returns
    /// `path_not_found` without reading content bytes.
    ///
    /// `max_bytes` is checked against the declared size before fetching. It is
    /// independent of the deployment's download limit so callers can apply a
    /// smaller memory budget. The read fails if the returned size or digest
    /// does not match the reference.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.read_content",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "read_content",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn read_content(&self, content_ref: &ContentRef, max_bytes: u64) -> Result<Vec<u8>> {
        self.core.record_trace_context(&tracing::Span::current());
        self.core
            .read(&self.namespace_id, |engine, read_context| async move {
                Ok(engine
                    .read_content_ref(content_ref, max_bytes, &read_context)
                    .await?)
            })
            .await
    }

    /// Lists the namespace's recoverable deletions, ascending by deleted root
    /// inode. Tombstone rows are immortal, so this answers however far the
    /// replay floor has advanced; entries carry the deleted name when the
    /// delete recorded one.
    pub fn list_trash(&self) -> TrashPager {
        let reader = self.read_only();
        loonfs_api::Pager::new(move |request| {
            let reader = reader.clone();
            async move { reader.trash_page(decode_page_request(request)?).await }
        })
    }

    #[tracing::instrument(
        level = "debug",
        name = "loonfs.list_trash",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "list_trash",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    async fn trash_page(
        &self,
        request: PageRequest<TrashPageCursor>,
    ) -> Result<loonfs_api::ListTrashResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        self.core
            .read(&self.namespace_id, |engine, read_context| {
                let request = request.clone();
                async move {
                    let page = engine.list_trash_page(request, &read_context).await?;
                    let next_cursor = encode_next_cursor(page.next_cursor.as_ref())?;
                    Ok(loonfs_api::ListTrashResponse {
                        namespace_id: self.namespace_id.clone(),
                        head_seq: read_context.head.seq,
                        entries: page.items,
                        next_cursor,
                    })
                }
            })
            .await
    }

    /// Lists a file path's revision history, newest first.
    pub fn list_file_revisions(&self, absolute_path: &str) -> FileRevisionsPager {
        let reader = self.read_only();
        let absolute_path = absolute_path.to_owned();
        loonfs_api::Pager::new(move |request| {
            let reader = reader.clone();
            let absolute_path = absolute_path.clone();
            async move {
                reader
                    .file_revisions_page(&absolute_path, decode_page_request(request)?)
                    .await
            }
        })
    }

    #[tracing::instrument(
        level = "debug",
        name = "loonfs.list_file_revisions",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "list_file_revisions",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    async fn file_revisions_page(
        &self,
        absolute_path: &str,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let absolute_path = AbsolutePath::parse(absolute_path)
            .map_err(|error| CoreError::InvalidPath(error.to_string()))?;
        self.core
            .read(&self.namespace_id, |engine, read_context| {
                let request = request.clone();
                let absolute_path = absolute_path.clone();
                async move {
                    let (inode_id, page) = engine
                        .list_file_revisions_page(absolute_path.as_str(), request, &read_context)
                        .await?;
                    Ok(file_revisions_page_response(
                        self.namespace_id.clone(),
                        read_context.head.seq,
                        page,
                        inode_id,
                    )?)
                }
            })
            .await
    }

    /// Lists the retained revisions of a file inode, newest first.
    pub fn list_file_revisions_by_inode(&self, inode_id: InodeId) -> FileRevisionsPager {
        let reader = self.read_only();
        loonfs_api::Pager::new(move |request| {
            let reader = reader.clone();
            async move {
                reader
                    .file_revisions_by_inode_page(inode_id, decode_page_request(request)?)
                    .await
            }
        })
    }

    #[tracing::instrument(
        level = "debug",
        name = "loonfs.list_file_revisions_by_inode",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "list_file_revisions_by_inode",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    async fn file_revisions_by_inode_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        self.core
            .read(&self.namespace_id, |engine, read_context| {
                let request = request.clone();
                async move {
                    let page = engine
                        .list_file_revisions_for_inode_page(inode_id, request, &read_context)
                        .await?;
                    Ok(file_revisions_page_response(
                        self.namespace_id.clone(),
                        read_context.head.seq,
                        page,
                        inode_id,
                    )?)
                }
            })
            .await
    }

    /// Reads the content of one historical file revision by path.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.read_file_revision",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "read_file_revision",
            method = "read_file_revision",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn read_file_revision(
        &self,
        absolute_path: &str,
        revision_no: RevisionNo,
    ) -> Result<FileBytes> {
        self.core.record_trace_context(&tracing::Span::current());
        self.core
            .read(&self.namespace_id, |engine, read_context| async move {
                let read = engine
                    .get_file_revision(
                        absolute_path,
                        revision_no,
                        &read_context,
                        self.core.inner.config.max_read_content_bytes,
                    )
                    .await?;
                Ok(read)
            })
            .await
    }

    /// Streams a retained inode revision, including content without a visible path.
    /// Complete verification requires consuming the stream to its end.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.read_file_revision_stream_by_inode",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "read_file_revision_stream_by_inode",
            method = "read_file_revision_stream_by_inode",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn read_file_revision_stream_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        self.core.record_trace_context(&tracing::Span::current());
        self.core
            .read(&self.namespace_id, |engine, context| async move {
                Ok(engine
                    .read_file_revision_stream_by_inode(inode_id, revision_no, &context)
                    .await?)
            })
            .await
    }

    /// Reads and verifies one retained file revision by inode identity.
    /// Current visibility and path are not required.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.read_file_revision_by_inode",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "read_file_revision_by_inode",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn read_file_revision_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        self.core.record_trace_context(&tracing::Span::current());
        self.core
            .read(&self.namespace_id, |engine, read_context| async move {
                let bytes = engine
                    .get_file_revision_for_inode(
                        inode_id,
                        revision_no,
                        &read_context,
                        self.core.inner.config.max_read_content_bytes,
                    )
                    .await?;
                Ok(bytes)
            })
            .await
    }

    /// Lists the ordered change feed after `after_seq`.
    pub fn list_changes(&self, after_seq: ChangeSeq) -> ChangesPager {
        let reader = self.read_only();
        loonfs_api::Pager::new(move |request: PageRequest<ChangeSeq>| {
            let reader = reader.clone();
            async move {
                reader
                    .changes_page(request.cursor.unwrap_or(after_seq), request.limit)
                    .await
            }
        })
    }

    #[tracing::instrument(
        level = "debug",
        name = "loonfs.list_changes",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "list_changes",
            method = "list_changes",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    async fn changes_page(
        &self,
        after_seq: ChangeSeq,
        limit: EffectiveLimit,
    ) -> Result<ListChangesResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        self.core
            .read(&self.namespace_id, |engine, context| async move {
                change_feed_page(&engine, &context, after_seq, limit).await
            })
            .await
    }
}
