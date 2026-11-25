//! [`ReadView`], a captured namespace state that related reads share, and
//! where a view's state comes from.

use crate::downloads::{DirectDownloadByInodeTarget, DirectDownloadTarget};
use crate::pagers::{
    decode_list_page_request, decode_page_request, ChangesPager, FileRevisionsPager,
    InodeChildrenPager, PathEntriesPager, RetainedRevisionsPager, SnapshotFilesPager,
};
use crate::pinned_read::PinnedRead;
use crate::runtime_core::RuntimeCore;
use crate::{
    CommitSeq, ContentRef, CoreError, CurrentFileState, DownloadOptions, Error, FileBytes,
    FileContentStream, InodeId, ListChangesOptions, ListFileRevisionsResponse,
    ListInodeChildrenResponse, ListOptions, ListPathEntriesResponse, ListSnapshotFilesOptions,
    NamespaceId, PathEntry, ReadFileStreamOptions, Result, RevisionNo, SharedObjectStore,
    SnapshotId, StatOptions,
};
use loonfs_core::{BudgetPermit, OwnedBudgetPermit};
use loonfs_types::format::manifest::ExtensionEntry;
use loonfs_types::{
    AbsolutePath, ChangeFeedPosition, ExtensionName, FileRevisionsPageCursor, ListPageCursor,
    PageRequest,
};
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

/// A captured namespace state that related reads share.
///
/// A [`Namespace`](crate::Namespace) handle reads the current state on every
/// call. A read view keeps related reads on one captured state, even when
/// new commits publish. A durable snapshot preserves a state for later use.
/// A read view retains nothing in the store and is meant to live for one
/// request or unit of work. It can capture the current state, at the
/// consistency of the handle that captured it, or an unexpired snapshot.
/// Every read through a view reads the state that the view captured. A
/// directory or revision listing refuses a cursor from another view's
/// listing with `invalid_request`. A change-feed cursor is a position in the
/// feed (a sequence and an event index), so a page through any view can
/// resume it.
///
/// A view from [`Namespace::read_view`](crate::Namespace::read_view) or
/// [`Namespace::read_view_at_snapshot`](crate::Namespace::read_view_at_snapshot)
/// takes a read slot for each read. A view that
/// [`Namespace::read_at_snapshot`](crate::Namespace::read_at_snapshot) passes
/// to its closure reads under that call's one slot instead, and one slot
/// admits one running read, so reads through that view and its copies run
/// one at a time.
#[must_use]
#[derive(Clone)]
pub struct ReadView {
    pinned: Arc<PinnedRead>,
    core: RuntimeCore,
    source: ReadSource,
    admission: ReadAdmission,
}

/// How the reads through a view are admitted to the runtime's read budget.
#[derive(Clone)]
pub(crate) enum ReadAdmission {
    /// Each read waits for a read slot of its own.
    EachRead,
    /// The reads run under a read slot that every copy of the view shares,
    /// and the slot returns when the last copy is dropped. One slot admits
    /// one running read, so a read takes the lock for its duration, and a
    /// second read through the view or a copy of it waits until the first
    /// ends.
    Shared(Arc<Mutex<OwnedBudgetPermit>>),
}

/// The admission that one read through a view keeps until it ends: a slot
/// of its own, or the lock of the slot that the view shares.
enum Admitted<'a> {
    Own {
        _slot: BudgetPermit<'a>,
    },
    Shared {
        _lock: MutexGuard<'a, OwnedBudgetPermit>,
    },
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

/// A page cursor that a read of a snapshot binds to that snapshot.
pub(crate) trait SnapshotBoundCursor {
    /// The cursor's name in an error message.
    const NAME: &'static str;

    /// Returns the head sequence the issuing page was evaluated at.
    fn head_seq(&self) -> CommitSeq;

    /// Returns the snapshot whose read issued the cursor, or `None` for a
    /// live read.
    fn snapshot_id(&self) -> Option<&SnapshotId>;
}

impl SnapshotBoundCursor for ListPageCursor {
    const NAME: &'static str = "directory";

    fn head_seq(&self) -> CommitSeq {
        ListPageCursor::head_seq(self)
    }

    fn snapshot_id(&self) -> Option<&SnapshotId> {
        ListPageCursor::snapshot_id(self)
    }
}

impl SnapshotBoundCursor for FileRevisionsPageCursor {
    const NAME: &'static str = "revision";

    fn head_seq(&self) -> CommitSeq {
        self.head_seq
    }

    fn snapshot_id(&self) -> Option<&SnapshotId> {
        self.snapshot_id.as_ref()
    }
}

/// Rejects a cursor that another read minted. A snapshot implies its head,
/// so the head test does work only for a view of the live head.
pub(crate) fn validate_view_cursor<C: SnapshotBoundCursor>(
    cursor: Option<&C>,
    captured_seq: CommitSeq,
    snapshot_id: Option<&SnapshotId>,
) -> Result<()> {
    let Some(cursor) = cursor else {
        return Ok(());
    };
    let name = C::NAME;
    if cursor.head_seq() != captured_seq {
        return Err(CoreError::InvalidCursor(format!(
            "{name} cursor head `{}` does not match the read view head `{captured_seq}`",
            cursor.head_seq()
        ))
        .into());
    }
    match (cursor.snapshot_id(), snapshot_id) {
        (Some(actual), Some(expected)) if actual == expected => Ok(()),
        (Some(actual), Some(expected)) => Err(CoreError::InvalidCursor(format!(
            "{name} cursor snapshot `{actual}` does not match requested snapshot `{expected}`"
        ))
        .into()),
        (Some(actual), None) => Err(CoreError::InvalidCursor(format!(
            "{name} cursor is bound to snapshot `{actual}`"
        ))
        .into()),
        (None, Some(expected)) => Err(CoreError::InvalidCursor(format!(
            "{name} cursor is not bound to snapshot `{expected}`"
        ))
        .into()),
        (None, None) => Ok(()),
    }
}

impl ReadView {
    /// Returns a view that reads through `pinned`, which captured `source`,
    /// with its reads admitted as `admission` says.
    pub(crate) fn new(
        core: RuntimeCore,
        pinned: PinnedRead,
        source: ReadSource,
        admission: ReadAdmission,
    ) -> Self {
        Self {
            pinned: Arc::new(pinned),
            core,
            source,
            admission,
        }
    }

    /// Admits one read through this view, and returns the admission that
    /// the read keeps until it ends: a read slot of its own, or the lock of
    /// the slot that the view shares, which admits one running read at a
    /// time.
    async fn admit(&self) -> Admitted<'_> {
        match &self.admission {
            ReadAdmission::EachRead => Admitted::Own {
                _slot: self.core.read_permit().await,
            },
            ReadAdmission::Shared(slot) => Admitted::Shared {
                _lock: slot.lock().await,
            },
        }
    }

    async fn read<T, E>(
        &self,
        read: impl std::future::Future<Output = std::result::Result<T, E>>,
    ) -> Result<T>
    where
        Error: From<E>,
    {
        read.await.map_err(Error::from)
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
        let Some(context) = self.pinned.context.at_seq(seq) else {
            let head_seq = self.pinned.context.read_basis().head().seq;
            return Err(Error::InvalidRequest {
                message: format!("sequence `{seq}` is ahead of the read view's head `{head_seq}`"),
                param: "seq",
            });
        };
        let pinned = PinnedRead {
            engine: self.core.reader_engine(self.namespace_id()),
            context,
        };
        let _permit = self.admit().await;
        self.read(pinned.engine.ensure_retained(&pinned.context))
            .await?;
        Ok(ReadView::new(
            self.core.clone(),
            pinned,
            ReadSource::Head,
            self.admission.clone(),
        ))
    }

    /// Returns a view of the same state whose reads check no subject's
    /// rights, for an extension that decides what its results contain before
    /// it applies a subject's rights, such as `loonfs-grep`.
    ///
    /// Requires what the change feed requires: a handle with no subject, or
    /// a subject that administers the namespace at the head that this view's
    /// reads are authorized at.
    /// Otherwise it returns `forbidden`.
    pub async fn as_administrator(&self) -> Result<ReadView> {
        let _permit = self.admit().await;
        let engine = self
            .read(
                self.pinned
                    .engine
                    .verify_administrator_reads(&self.pinned.context),
            )
            .await?;
        let pinned = PinnedRead {
            engine,
            context: self.pinned.context.clone(),
        };
        Ok(ReadView::new(
            self.core.clone(),
            pinned,
            self.source.clone(),
            self.admission.clone(),
        ))
    }

    /// Lists the change feed after `after_seq` through this view's captured
    /// head.
    pub fn list_changes(&self, after_seq: CommitSeq) -> ChangesPager {
        self.list_changes_with_options(after_seq, &ListChangesOptions::default())
    }

    /// Lists the change feed after `after_seq` through this view's captured
    /// head, as `options` asks. A view of a snapshot lists the feed through
    /// the sequence that the snapshot captured.
    pub fn list_changes_with_options(
        &self,
        after_seq: CommitSeq,
        options: &ListChangesOptions,
    ) -> ChangesPager {
        let start = ChangeFeedPosition {
            after_seq,
            event_index: options.event_index,
        };
        loonfs_types::Pager::with_state(
            (self.clone(), start, options.commit_splitting),
            |(view, start, splitting), request: PageRequest<ChangeFeedPosition>| async move {
                let _permit = view.admit().await;
                let position = request.cursor.unwrap_or(start);
                view.read(view.pinned.changes_page(position, request.limit, splitting))
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
            let _permit = view.admit().await;
            view.read(
                view.pinned
                    .engine
                    .retained_revisions_page(request, &view.pinned.context),
            )
            .await
        })
    }

    /// Lists the files visible at this view's sequence in ascending inode-ID
    /// order. Directories are omitted. The listing requires namespace
    /// administrator access, as the change feed does.
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
                let _permit = view.admit().await;
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
        let _permit = self.admit().await;
        self.read(
            self.pinned
                .engine
                .find_extension_entry(&self.pinned.context, name),
        )
        .await
    }

    /// Returns `manifest_replaced` when a newer manifest replaced the manifest
    /// that this view reads above, and `snapshot_not_found` when this view
    /// reads a snapshot that no longer admits a read, for an extension that
    /// finds one of its own segments missing from the store.
    pub async fn ensure_manifest_current(&self) -> Result<()> {
        Ok(self
            .core
            .namespace_store(self.namespace_id())
            .ensure_context_kept(&self.pinned.context)
            .await?)
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
        let _permit = self.admit().await;
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
                let _permit = view.admit().await;
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
            validate_view_cursor(request.cursor.as_ref(), self.head_seq(), self.snapshot_id())?;
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
        let _permit = self.admit().await;
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
                let _permit = view.admit().await;
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
            validate_view_cursor(request.cursor.as_ref(), self.head_seq(), self.snapshot_id())?;
            self.pinned
                .inode_children_page(inode_id, request, options, self.snapshot_id())
                .await
        })
        .await
    }

    /// Lists a file path's revision history against this view, newest first.
    /// A view of a snapshot resolves the path in the snapshot's state and
    /// lists the revisions that the snapshot's manifest keeps, newest first
    /// from the revision that the snapshot captured.
    pub fn list_file_revisions(&self, absolute_path: &str) -> FileRevisionsPager {
        loonfs_types::Pager::with_state(
            (self.clone(), absolute_path.to_owned()),
            |(view, absolute_path), request| async move {
                let _permit = view.admit().await;
                let request = decode_page_request(request)?;
                view.file_revisions_page(&absolute_path, request).await
            },
        )
    }

    async fn file_revisions_page(
        &self,
        absolute_path: &str,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
        validate_view_cursor(request.cursor.as_ref(), self.head_seq(), self.snapshot_id())?;
        let absolute_path = AbsolutePath::parse(absolute_path).map_err(CoreError::from)?;
        self.pinned
            .file_revisions_page(&absolute_path, request, self.snapshot_id())
            .await
    }

    /// Lists the retained revisions of a file inode against this view,
    /// newest first. A view of a snapshot lists the revisions that the
    /// snapshot's manifest keeps, newest first from the revision that the
    /// snapshot captured.
    pub fn list_file_revisions_by_inode(&self, inode_id: InodeId) -> FileRevisionsPager {
        loonfs_types::Pager::with_state(
            (self.clone(), inode_id),
            |(view, inode_id), request| async move {
                let _permit = view.admit().await;
                let request = decode_page_request(request)?;
                validate_view_cursor(request.cursor.as_ref(), view.head_seq(), view.snapshot_id())?;
                view.pinned
                    .file_revisions_by_inode_page(inode_id, request, view.snapshot_id())
                    .await
            },
        )
    }

    /// Resolves current visibility, revision, and path against this view.
    ///
    /// Unreadable inodes return [`CurrentFile::Hidden`](crate::CurrentFile::Hidden), with no
    /// path or revision.
    pub async fn stat_current_files_by_inode(
        &self,
        inode_ids: &[InodeId],
    ) -> Result<Vec<CurrentFileState>> {
        let _permit = self.admit().await;
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
        let _permit = self.admit().await;
        self.read(
            self.pinned
                .engine
                .read_content_ref(content_ref, max_bytes, &self.pinned.context),
        )
        .await
    }

    /// Reads one inode revision through this view's authorization.
    pub async fn read_file_revision_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        max_bytes: u64,
    ) -> Result<Vec<u8>> {
        let _permit = self.admit().await;
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
        let _permit = self.admit().await;
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
        let _permit = self.admit().await;
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
        let _permit = self.admit().await;
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
        let _permit = self.admit().await;
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
        let _permit = self.admit().await;
        self.read(self.pinned.engine.read_file_stream_by_inode(
            inode_id,
            None,
            &self.pinned.context,
            options.chunk_bytes,
            options.range,
        ))
        .await
    }

    /// Streams one retained revision of a file inode in bounded chunks, as
    /// `options` asks. A view of a snapshot reads the revision that the
    /// snapshot's manifest keeps, so a revision stays readable through the
    /// snapshot after the namespace's retention floor passes it. Complete
    /// verification requires consuming the stream to its end.
    pub async fn read_file_revision_stream_by_inode_with_options(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        options: &ReadFileStreamOptions,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        let _permit = self.admit().await;
        self.read(self.pinned.engine.read_file_stream_by_inode(
            inode_id,
            Some(revision_no),
            &self.pinned.context,
            options.chunk_bytes,
            options.range,
        ))
        .await
    }

    /// Resolves one retained revision of a file inode for a direct download,
    /// as `options` asks. A view of a snapshot finds the revision in the
    /// snapshot's manifest.
    pub async fn create_revision_download_by_inode_with_options(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        options: &DownloadOptions,
    ) -> Result<DirectDownloadByInodeTarget> {
        let _permit = self.admit().await;
        self.read(self.pinned.engine.direct_download_target_by_inode(
            inode_id,
            Some(revision_no),
            options.start_offset,
            &self.pinned.context,
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
        let _permit = self.admit().await;
        self.read(self.pinned.engine.direct_download_target_by_inode(
            inode_id,
            None,
            options.start_offset,
            &self.pinned.context,
        ))
        .await
    }
}
