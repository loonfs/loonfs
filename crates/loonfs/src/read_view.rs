//! [`ReadView`], a captured namespace state that related reads share, and
//! where a view's state comes from.

use crate::downloads::{DirectDownloadByInodeTarget, DirectDownloadTarget};
use crate::pagers::{
    decode_list_page_request, ChangesPager, InodeChildrenPager, PathEntriesPager,
    RetainedRevisionsPager, SnapshotFilesPager,
};
use crate::pinned_read::PinnedRead;
use crate::runtime_core::{classify_read_result, RuntimeCore};
use crate::{
    CommitSeq, ContentRef, CoreError, CurrentFileState, DownloadOptions, Error, FileBytes,
    FileContentStream, InodeId, ListInodeChildrenResponse, ListOptions, ListPathEntriesResponse,
    ListSnapshotFilesOptions, NamespaceId, PathEntry, ReadAt, ReadFileStreamOptions, Result,
    RevisionNo, SharedObjectStore, SnapshotId, StatOptions,
};
use loonfs_core::RuntimeReadContext;
use loonfs_types::format::manifest::ExtensionEntry;
use loonfs_types::options::CommitSplitting;
use loonfs_types::{AbsolutePath, ChangeFeedPosition, ExtensionName, ListPageCursor, PageRequest};
use std::sync::Arc;

/// A captured namespace state that related reads share.
///
/// A [`Namespace`](crate::Namespace) handle reads the current state on every
/// call. A read view keeps related reads on one captured state, even when
/// new commits publish. A durable snapshot preserves a state for later use.
/// A read view retains nothing in the store and is meant to live for one
/// request or unit of work. It can capture the current state or an
/// unexpired snapshot.
#[must_use]
#[derive(Clone)]
pub struct ReadView {
    pinned: Arc<PinnedRead>,
    core: RuntimeCore,
    source: ReadSource,
}

/// The state that a read view captured: the live head, or a snapshot.
#[derive(Clone)]
pub(crate) enum ReadSource {
    Head,
    Snapshot(SnapshotId),
}

impl ReadSource {
    /// Returns the snapshot this source reads, or `None` for the live head.
    pub(crate) fn snapshot_id(&self) -> Option<&SnapshotId> {
        match self {
            Self::Snapshot(snapshot_id) => Some(snapshot_id),
            Self::Head => None,
        }
    }
}

/// Rejects a directory cursor that another read minted. A snapshot implies
/// its head, so the head test does work only for a view of the live head.
pub(crate) fn validate_view_directory_cursor(
    cursor: Option<&ListPageCursor>,
    captured_seq: CommitSeq,
    snapshot_id: Option<&SnapshotId>,
) -> Result<()> {
    let Some(cursor) = cursor else {
        return Ok(());
    };
    if cursor.head_seq() != captured_seq {
        return Err(CoreError::InvalidCursor(format!(
            "directory cursor head `{}` does not match the read view head `{captured_seq}`",
            cursor.head_seq()
        ))
        .into());
    }
    match (cursor.snapshot_id(), snapshot_id) {
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

impl ReadView {
    /// Returns a view that reads through `pinned`, which captured `source`.
    pub(crate) fn new(core: RuntimeCore, pinned: PinnedRead, source: ReadSource) -> Self {
        Self {
            pinned: Arc::new(pinned),
            core,
            source,
        }
    }

    async fn read<T, E>(
        &self,
        read: impl std::future::Future<Output = std::result::Result<T, E>>,
    ) -> Result<T>
    where
        Error: From<E>,
    {
        let result = read.await.map_err(Error::from);
        classify_read_result(&self.core, &self.pinned.context, &self.source, result).await
    }

    fn snapshot_id(&self) -> Option<&SnapshotId> {
        self.source.snapshot_id()
    }

    /// Returns the namespace this view reads.
    pub fn namespace_id(&self) -> &NamespaceId {
        self.pinned.namespace_id()
    }

    /// Returns the head sequence this view captured, or the sequence that a
    /// view from [`Self::at_seq`] reads at.
    pub fn head_seq(&self) -> CommitSeq {
        self.pinned.head_seq()
    }

    /// Returns a view of this view's head at `seq`, a retained sequence at or
    /// below the head.
    ///
    /// Reads through the returned view see the commits at or below `seq`,
    /// and they check the subject's rights at this view's head, as reads of
    /// a snapshot do. A sequence above the head, or a view of a snapshot,
    /// returns `invalid_request`, and a sequence below the retention floor
    /// returns `below_retention_floor`.
    pub async fn at_seq(&self, seq: CommitSeq) -> Result<ReadView> {
        if let Some(snapshot_id) = self.snapshot_id() {
            return Err(Error::InvalidRequest {
                message: format!(
                    "a read view of snapshot {:?} reads only the snapshot's sequence",
                    snapshot_id.as_str()
                ),
                param: "seq",
            });
        }
        let head = RuntimeReadContext {
            visible_seq: None,
            ..self.pinned.context.clone()
        };
        let head_seq = head.read_basis.head.seq;
        if seq > head_seq {
            return Err(Error::InvalidRequest {
                message: format!("sequence `{seq}` is ahead of the read view's head `{head_seq}`"),
                param: "seq",
            });
        }
        let pinned = PinnedRead {
            engine: self
                .core
                .reader_engine(self.namespace_id())
                .with_authorization_head(head.clone()),
            context: RuntimeReadContext {
                visible_seq: Some(seq),
                ..head
            },
        };
        let _permit = self.core.read_permit().await;
        self.read(pinned.engine.ensure_retained(&pinned.context))
            .await?;
        Ok(ReadView::new(self.core.clone(), pinned, ReadSource::Head))
    }

    /// Returns a view of the same state whose reads check no subject's
    /// rights, for an extension that decides what its results contain before
    /// it applies a subject's rights, such as `loonfs-grep`.
    ///
    /// Requires what the change feed requires: a handle with no subject, or
    /// a subject that administers the namespace at its current head.
    /// Otherwise it returns `forbidden`.
    pub async fn as_administrator(&self) -> Result<ReadView> {
        let _permit = self.core.read_permit().await;
        self.read(
            self.pinned
                .engine
                .ensure_administrator(&self.pinned.context),
        )
        .await?;
        let pinned = PinnedRead {
            engine: self
                .core
                .reader_engine(self.namespace_id())
                .with_administrator_reads(),
            context: self.pinned.context.clone(),
        };
        Ok(ReadView::new(
            self.core.clone(),
            pinned,
            self.source.clone(),
        ))
    }

    /// Rejects read options that ask for a state other than the one this
    /// view captured. A view is pinned, so a read through it has no
    /// consistency setting, and the options can refer to the snapshot this
    /// view reads but to no other, because a read of another snapshot through
    /// this view would silently return the wrong state.
    fn ensure_read_at(&self, at: &ReadAt) -> Result<()> {
        match at {
            ReadAt::Live(None) => Ok(()),
            ReadAt::Live(Some(_)) => Err(Error::InvalidRequest {
                message: "a read through a read view has no consistency setting".to_owned(),
                param: "consistency",
            }),
            ReadAt::Snapshot(requested) if Some(requested) == self.snapshot_id() => Ok(()),
            ReadAt::Snapshot(requested) => Err(Error::InvalidRequest {
                message: format!(
                    "`snapshot_id` {:?} refers to a different snapshot than this read view",
                    requested.as_str()
                ),
                param: "snapshot_id",
            }),
        }
    }

    /// Lists the change feed after `after_seq` through this view's captured
    /// head.
    pub fn list_changes(&self, after_seq: CommitSeq) -> ChangesPager {
        loonfs_types::Pager::with_state(
            (self.clone(), after_seq),
            |(view, after_seq), request: PageRequest<ChangeFeedPosition>| async move {
                let _permit = view.core.read_permit().await;
                let position = request
                    .cursor
                    .unwrap_or(ChangeFeedPosition::after(after_seq));
                view.read(
                    view.pinned
                        .changes_page(position, request.limit, CommitSplitting::Whole),
                )
                .await
            },
        )
    }

    /// Lists every file revision this view's metadata retains, ordered by
    /// inode and newest first within each inode, including the revisions of
    /// deleted files. The listing requires namespace administrator access, as
    /// the change feed does.
    pub fn list_retained_revisions(&self) -> RetainedRevisionsPager {
        loonfs_types::Pager::with_state(self.clone(), |view, request| async move {
            let _permit = view.core.read_permit().await;
            view.read(
                view.pinned
                    .engine
                    .retained_revisions_page(request, &view.pinned.context),
            )
            .await
        })
    }

    /// Lists the files visible at this view's sequence in ascending inode-ID
    /// order, as [`Namespace::list_snapshot_files`](crate::Namespace::list_snapshot_files)
    /// lists a snapshot's files. The listing requires namespace administrator
    /// access, as the change feed does.
    pub fn list_files(&self) -> SnapshotFilesPager {
        self.list_files_with_options(&ListSnapshotFilesOptions::default())
    }

    /// Lists the files at this view's sequence as `options` asks.
    pub fn list_files_with_options(
        &self,
        options: &ListSnapshotFilesOptions,
    ) -> SnapshotFilesPager {
        loonfs_types::Pager::with_state(
            (self.clone(), *options),
            |(view, options), request| async move {
                let _permit = view.core.read_permit().await;
                view.read(view.pinned.engine.list_files_page(
                    request,
                    options,
                    &view.pinned.context,
                ))
                .await
            },
        )
    }

    /// Returns the entry of extension `name` in the manifest this view reads
    /// above, or `None` when that manifest has none.
    ///
    /// An extension's state is as current as the view's manifest, so the
    /// view's head can be ahead of the commits that the entry describes.
    pub async fn find_extension_entry(
        &self,
        name: &ExtensionName,
    ) -> Result<Option<ExtensionEntry>> {
        let _permit = self.core.read_permit().await;
        self.read(
            self.pinned
                .engine
                .find_extension_entry(&self.pinned.context, name),
        )
        .await
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
        let _permit = self.core.read_permit().await;
        self.ensure_read_at(&options.at)?;
        self.read(self.pinned.engine.resolve_path(
            absolute_path,
            options.clone(),
            &self.pinned.context,
        ))
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
        loonfs_types::Pager::with_state(
            (self.clone(), absolute_path.to_owned(), options.clone()),
            |(view, absolute_path, options), request| async move {
                let _permit = view.core.read_permit().await;
                let request = decode_list_page_request(request, &options)?;
                view.path_entries_page(&absolute_path, request, options)
                    .await
            },
        )
    }

    async fn path_entries_page(
        &self,
        absolute_path: &str,
        request: PageRequest<ListPageCursor>,
        options: ListOptions,
    ) -> Result<ListPathEntriesResponse> {
        self.read(async {
            self.ensure_read_at(&options.at)?;
            validate_view_directory_cursor(
                request.cursor.as_ref(),
                self.head_seq(),
                self.snapshot_id(),
            )?;
            let listed_path = AbsolutePath::parse(absolute_path).map_err(CoreError::from)?;
            self.pinned
                .path_entries_page(listed_path, request, options, self.snapshot_id())
                .await
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
        let _permit = self.core.read_permit().await;
        self.ensure_read_at(&options.at)?;
        self.read(
            self.pinned
                .engine
                .stat_inode(inode_id, options.clone(), &self.pinned.context),
        )
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
        loonfs_types::Pager::with_state(
            (self.clone(), inode_id, options.clone()),
            |(view, inode_id, options), request| async move {
                let _permit = view.core.read_permit().await;
                let request = decode_list_page_request(request, &options)?;
                view.inode_children_page(inode_id, request, options).await
            },
        )
    }

    async fn inode_children_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<ListPageCursor>,
        options: ListOptions,
    ) -> Result<ListInodeChildrenResponse> {
        self.read(async {
            self.ensure_read_at(&options.at)?;
            validate_view_directory_cursor(
                request.cursor.as_ref(),
                self.head_seq(),
                self.snapshot_id(),
            )?;
            self.pinned
                .inode_children_page(inode_id, request, options, self.snapshot_id())
                .await
        })
        .await
    }

    /// Resolves current visibility, revision, and path against this view.
    ///
    /// Unreadable inodes return [`CurrentFile::Hidden`](crate::CurrentFile::Hidden), with no
    /// path or revision.
    pub async fn stat_current_files_by_inode(
        &self,
        inode_ids: &[InodeId],
    ) -> Result<Vec<CurrentFileState>> {
        let _permit = self.core.read_permit().await;
        self.read(
            self.pinned
                .engine
                .stat_current_files_by_inode(inode_ids, &self.pinned.context),
        )
        .await
    }

    /// Reads and verifies immutable content selected from this view.
    ///
    /// Requires namespace administrator access. Content must be published in
    /// this view, including through a fork. Otherwise returns
    /// `path_not_found` without reading content bytes. A published reference
    /// whose object collection reclaimed also returns `path_not_found`.
    pub async fn read_content(&self, content_ref: &ContentRef, max_bytes: u64) -> Result<Vec<u8>> {
        let _permit = self.core.read_permit().await;
        self.read(
            self.pinned
                .engine
                .read_content_ref(content_ref, max_bytes, &self.pinned.context),
        )
        .await
    }

    /// Refuses a reader with no subject on an ACL namespace, for callers
    /// that could otherwise return before any per-subject read.
    ///
    /// This is a hook for an extension that serves reads from its own state
    /// before it reads through this view, such as `loonfs-grep`.
    pub fn ensure_subject(&self) -> Result<()> {
        Ok(self.pinned.engine.ensure_subject(&self.pinned.context)?)
    }

    /// Reads one inode revision through this view's authorization.
    pub async fn read_file_revision_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        max_bytes: u64,
    ) -> Result<Vec<u8>> {
        let _permit = self.core.read_permit().await;
        self.read(self.pinned.engine.read_file_revision_by_inode(
            inode_id,
            revision_no,
            &self.pinned.context,
            Some(max_bytes),
        ))
        .await
    }

    /// Reads the file selected by this view.
    pub async fn read_file(&self, absolute_path: &str) -> Result<FileBytes> {
        let _permit = self.core.read_permit().await;
        self.read(self.pinned.engine.read_file(
            absolute_path,
            &self.pinned.context,
            Some(self.core.max_read_content_bytes()),
        ))
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
    /// `options` asks.
    pub async fn read_file_stream_with_options(
        &self,
        absolute_path: &str,
        options: &ReadFileStreamOptions,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        let _permit = self.core.read_permit().await;
        self.ensure_read_at(&options.at)?;
        self.read(self.pinned.engine.read_file_stream(
            absolute_path,
            &self.pinned.context,
            None,
            options.chunk_bytes,
            options.range,
        ))
        .await
    }

    /// Resolves the file selected by this view for a direct download.
    pub async fn create_download(&self, absolute_path: &str) -> Result<DirectDownloadTarget> {
        self.create_download_with_options(absolute_path, &DownloadOptions::default())
            .await
    }

    /// Resolves the file selected by this view for a direct download, as
    /// `options` asks.
    pub async fn create_download_with_options(
        &self,
        absolute_path: &str,
        options: &DownloadOptions,
    ) -> Result<DirectDownloadTarget> {
        let _permit = self.core.read_permit().await;
        self.ensure_read_at(&options.at)?;
        self.read(self.pinned.engine.direct_download_target(
            absolute_path,
            None,
            options.start_offset,
            &self.pinned.context,
        ))
        .await
    }

    /// Reads the revision of a file inode that this view selects, wherever
    /// the view binds it, plus the entry it came from.
    pub async fn read_file_by_inode(&self, inode_id: InodeId) -> Result<FileBytes> {
        let _permit = self.core.read_permit().await;
        self.read(self.pinned.engine.read_file_by_inode(
            inode_id,
            &self.pinned.context,
            Some(self.core.max_read_content_bytes()),
        ))
        .await
    }

    /// Streams the revision of a file inode that this view selects in
    /// bounded chunks. Complete verification requires consuming the stream
    /// to its end.
    pub async fn read_file_stream_by_inode(
        &self,
        inode_id: InodeId,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        self.read_file_stream_by_inode_with_options(inode_id, &ReadFileStreamOptions::default())
            .await
    }

    /// Streams the revision of a file inode that this view selects in
    /// bounded chunks, as `options` asks. Complete verification requires
    /// consuming the stream to its end.
    pub async fn read_file_stream_by_inode_with_options(
        &self,
        inode_id: InodeId,
        options: &ReadFileStreamOptions,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        let _permit = self.core.read_permit().await;
        self.ensure_read_at(&options.at)?;
        self.read(self.pinned.engine.read_file_stream_by_inode(
            inode_id,
            None,
            &self.pinned.context,
            options.chunk_bytes,
            options.range,
        ))
        .await
    }

    /// Resolves the revision of a file inode that this view selects for a
    /// direct download.
    pub async fn create_download_by_inode(
        &self,
        inode_id: InodeId,
    ) -> Result<DirectDownloadByInodeTarget> {
        self.create_download_by_inode_with_options(inode_id, &DownloadOptions::default())
            .await
    }

    /// Resolves the revision of a file inode that this view selects for a
    /// direct download, as `options` asks.
    pub async fn create_download_by_inode_with_options(
        &self,
        inode_id: InodeId,
        options: &DownloadOptions,
    ) -> Result<DirectDownloadByInodeTarget> {
        let _permit = self.core.read_permit().await;
        self.ensure_read_at(&options.at)?;
        self.read(self.pinned.engine.direct_download_target_by_inode(
            inode_id,
            None,
            options.start_offset,
            &self.pinned.context,
        ))
        .await
    }
}
