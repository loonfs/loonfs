//! [`ReadView`], a captured namespace state that related reads share, and
//! where a view's state comes from.

use crate::downloads::{DirectDownloadByInodeTarget, DirectDownloadTarget};
use crate::execution_budget::BudgetPermit;
use crate::pagers::{decode_list_page_request, ChangesPager, InodeChildrenPager, PathEntriesPager};
use crate::pinned_read::PinnedRead;
use crate::runtime_core::{classify_read_result, RuntimeCore};
use crate::{
    ChangeSeq, ContentRef, CoreError, CurrentFileState, DownloadOptions, Error, FileBytes,
    FileContentStream, InodeId, ListChangesResponse, ListInodeChildrenResponse, ListOptions,
    ListPathEntriesResponse, NamespaceId, PathEntry, PinId, ReadAt, ReadFileStreamOptions, Result,
    RevisionNo, SharedObjectStore, StatOptions,
};
use loonfs_types::{AbsolutePath, EffectiveLimit, ListPageCursor, PageRequest};
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
    Snapshot(PinId),
}

/// Rejects a directory cursor that another read minted. A snapshot implies
/// its head, so the head test does work only for a view of the live head.
fn validate_view_directory_cursor(
    cursor: Option<&ListPageCursor>,
    captured_seq: ChangeSeq,
    pin_id: Option<&PinId>,
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
    match (cursor.pin_id(), pin_id) {
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

impl ReadView {
    /// Returns a view that reads through `pinned`, which captured `source`.
    pub(crate) fn new(core: RuntimeCore, pinned: PinnedRead, source: ReadSource) -> Self {
        Self {
            pinned: Arc::new(pinned),
            core,
            source,
        }
    }

    async fn read<T>(&self, read: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        classify_read_result(&self.core, &self.pinned.context, &self.source, read.await).await
    }

    fn snapshot_id(&self) -> Option<&PinId> {
        match &self.source {
            ReadSource::Snapshot(snapshot_id) => Some(snapshot_id),
            ReadSource::Head => None,
        }
    }

    /// Returns the namespace this view reads.
    pub fn namespace_id(&self) -> &NamespaceId {
        self.pinned.namespace_id()
    }

    /// Returns the head sequence this view captured.
    pub fn head_seq(&self) -> ChangeSeq {
        self.pinned.head_seq()
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
    pub fn list_changes(&self, after_seq: ChangeSeq) -> ChangesPager {
        loonfs_types::Pager::with_state(
            (self.clone(), after_seq),
            |(view, after_seq), request: PageRequest<ChangeSeq>| async move {
                let permit = view.core.read_permit().await;
                view.changes_page(request.cursor.unwrap_or(after_seq), request.limit, &permit)
                    .await
            },
        )
    }

    pub(crate) async fn changes_page(
        &self,
        after_seq: ChangeSeq,
        limit: EffectiveLimit,
        _permit: &BudgetPermit<'_>,
    ) -> Result<ListChangesResponse> {
        self.read(self.pinned.changes_page(after_seq, limit)).await
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
        let permit = self.core.read_permit().await;
        self.stat_with_permit(absolute_path, options, &permit).await
    }

    pub(crate) async fn stat_with_permit(
        &self,
        absolute_path: &str,
        options: &StatOptions,
        _permit: &BudgetPermit<'_>,
    ) -> Result<PathEntry> {
        self.ensure_read_at(&options.at)?;
        self.read(self.pinned.stat(absolute_path, options)).await
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
                let permit = view.core.read_permit().await;
                let request = decode_list_page_request(request, &options)?;
                view.path_entries_page(&absolute_path, request, options, &permit)
                    .await
            },
        )
    }

    pub(crate) async fn path_entries_page(
        &self,
        absolute_path: &str,
        request: PageRequest<ListPageCursor>,
        options: ListOptions,
        _permit: &BudgetPermit<'_>,
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
        let permit = self.core.read_permit().await;
        self.stat_by_inode_with_permit(inode_id, options, &permit)
            .await
    }

    pub(crate) async fn stat_by_inode_with_permit(
        &self,
        inode_id: InodeId,
        options: &StatOptions,
        _permit: &BudgetPermit<'_>,
    ) -> Result<PathEntry> {
        self.ensure_read_at(&options.at)?;
        self.read(self.pinned.stat_by_inode(inode_id, options))
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
                let permit = view.core.read_permit().await;
                let request = decode_list_page_request(request, &options)?;
                view.inode_children_page(inode_id, request, options, &permit)
                    .await
            },
        )
    }

    pub(crate) async fn inode_children_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<ListPageCursor>,
        options: ListOptions,
        _permit: &BudgetPermit<'_>,
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
        self.read(self.pinned.stat_current_files_by_inode(inode_ids))
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
        self.read(self.pinned.read_content(content_ref, max_bytes))
            .await
    }

    /// Refuses a reader with no subject on an ACL namespace, for callers
    /// that could otherwise answer before any per-subject read.
    ///
    /// This is a hook for an extension that answers from its own state
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
        self.read(
            self.pinned
                .read_file_revision_by_inode(inode_id, revision_no, max_bytes),
        )
        .await
    }

    /// Reads the file selected by this view.
    pub async fn read_file(&self, absolute_path: &str) -> Result<FileBytes> {
        let permit = self.core.read_permit().await;
        self.read_file_with_permit(absolute_path, &permit).await
    }

    pub(crate) async fn read_file_with_permit(
        &self,
        absolute_path: &str,
        _permit: &BudgetPermit<'_>,
    ) -> Result<FileBytes> {
        self.read(
            self.pinned
                .read_file(absolute_path, self.core.max_read_content_bytes()),
        )
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
        let permit = self.core.read_permit().await;
        self.read_file_stream_with_permit(absolute_path, options, &permit)
            .await
    }

    pub(crate) async fn read_file_stream_with_permit(
        &self,
        absolute_path: &str,
        options: &ReadFileStreamOptions,
        _permit: &BudgetPermit<'_>,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        self.ensure_read_at(&options.at)?;
        self.read(self.pinned.read_file_stream(absolute_path, None, options))
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
        let permit = self.core.read_permit().await;
        self.create_download_with_permit(absolute_path, options, &permit)
            .await
    }

    pub(crate) async fn create_download_with_permit(
        &self,
        absolute_path: &str,
        options: &DownloadOptions,
        _permit: &BudgetPermit<'_>,
    ) -> Result<DirectDownloadTarget> {
        self.ensure_read_at(&options.at)?;
        self.read(self.pinned.create_download(absolute_path, None, options))
            .await
    }

    /// Reads the revision of a file inode that this view selects, wherever
    /// the view binds it, plus the entry it came from.
    pub async fn read_file_by_inode(&self, inode_id: InodeId) -> Result<FileBytes> {
        let permit = self.core.read_permit().await;
        self.read_file_by_inode_with_permit(inode_id, &permit).await
    }

    pub(crate) async fn read_file_by_inode_with_permit(
        &self,
        inode_id: InodeId,
        _permit: &BudgetPermit<'_>,
    ) -> Result<FileBytes> {
        self.read(
            self.pinned
                .read_file_by_inode(inode_id, self.core.max_read_content_bytes()),
        )
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
        let permit = self.core.read_permit().await;
        self.read_file_stream_by_inode_with_permit(inode_id, options, &permit)
            .await
    }

    pub(crate) async fn read_file_stream_by_inode_with_permit(
        &self,
        inode_id: InodeId,
        options: &ReadFileStreamOptions,
        _permit: &BudgetPermit<'_>,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        self.ensure_read_at(&options.at)?;
        self.read(
            self.pinned
                .read_file_stream_by_inode(inode_id, None, options),
        )
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
        let permit = self.core.read_permit().await;
        self.create_download_by_inode_with_permit(inode_id, options, &permit)
            .await
    }

    pub(crate) async fn create_download_by_inode_with_permit(
        &self,
        inode_id: InodeId,
        options: &DownloadOptions,
        _permit: &BudgetPermit<'_>,
    ) -> Result<DirectDownloadByInodeTarget> {
        self.ensure_read_at(&options.at)?;
        self.read(
            self.pinned
                .create_download_by_inode(inode_id, None, options),
        )
        .await
    }
}
