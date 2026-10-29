//! Namespace and filesystem reads on a [`Namespace`] handle in any mode.

use crate::downloads::{DirectDownloadByInodeTarget, DirectDownloadTarget};
use crate::execution_budget::BudgetPermit;
use crate::pagers::{
    decode_list_page_request, decode_page_request, encode_next_cursor,
    file_revisions_page_response, ChangesPager, CheckpointFilesPager, FileRevisionsPager,
    InodeChildrenPager, PathEntriesPager, TrashPager,
};
use crate::pinned_read::PinnedRead;
use crate::read_view::{ReadSource, ReadView};
use crate::trace::{self, operation_span};
use crate::{
    ChangeSeq, CheckpointFilesPage, CheckpointFilesPageCursor, Consistency, ContentRef, CoreError,
    CurrentFileState, DownloadOptions, Error, FileBytes, FileContentStream, InodeId,
    ListChangesOptions, ListChangesResponse, ListCheckpointFilesOptions, ListFileRevisionsOptions,
    ListFileRevisionsResponse, ListInodeChildrenResponse, ListOptions, ListPathEntriesResponse,
    ListTrashOptions, Namespace, PathEntry, PinId, ReadAt, ReadFileOptions, ReadFileStreamOptions,
    Result, RevisionNo, SharedObjectStore, StatOptions,
};
use loonfs_types::{
    AbsolutePath, EffectiveLimit, FileRevisionsPageCursor, ListPageCursor, PageRequest,
    TrashPageCursor,
};
use std::future::Future;

#[cfg(test)]
mod tests;

/// Returns the error for a read of a numbered revision at a snapshot. A
/// snapshot captures one revision of each file, so a read of a numbered
/// revision reads the live head.
fn revision_at_snapshot() -> Error {
    Error::InvalidRequest {
        message: "revision_no cannot be combined with snapshot_id".to_owned(),
        param: "snapshot_id",
    }
}

/// Rejects a numbered revision read whose options read a snapshot.
fn ensure_revision_read_at(at: &ReadAt, revision_no: Option<RevisionNo>) -> Result<()> {
    match (at, revision_no) {
        (ReadAt::Snapshot(_), Some(_)) => Err(revision_at_snapshot()),
        _ => Ok(()),
    }
}

fn reject_pinned_directory_cursor(cursor: Option<&ListPageCursor>) -> Result<()> {
    let Some(pin_id) = cursor.and_then(ListPageCursor::pin_id) else {
        return Ok(());
    };
    Err(CoreError::InvalidCursor(format!(
        "directory cursor is bound to pin `{pin_id}`, so resume it from the same snapshot"
    ))
    .into())
}

impl<M: crate::Mode> Namespace<M> {
    /// Runs a read at the state that `at` selects: `live` at the live head,
    /// confirmed as `at` asks, or `snapshot` through a read view of the
    /// snapshot that `at` selects, under the caller's `permit`.
    async fn read_at<'p, 'b, T, Live, LiveFuture, Snapshot, SnapshotFuture>(
        &self,
        at: &ReadAt,
        permit: &'p BudgetPermit<'b>,
        live: Live,
        snapshot: Snapshot,
    ) -> Result<T>
    where
        Live: Fn(PinnedRead) -> LiveFuture,
        LiveFuture: Future<Output = Result<T>>,
        Snapshot: FnOnce(ReadView, &'p BudgetPermit<'b>) -> SnapshotFuture,
        SnapshotFuture: Future<Output = Result<T>>,
    {
        match at {
            ReadAt::Live(consistency) => {
                self.core.read(&self.namespace_id, *consistency, live).await
            }
            ReadAt::Snapshot(snapshot_id) => {
                let view = self
                    .read_view_at_snapshot_with_permit(snapshot_id, permit)
                    .await?;
                snapshot(view, permit).await
            }
        }
    }

    /// Captures the current namespace state for a group of related reads.
    ///
    /// The returned view keeps path lookup, directory listing, inode
    /// resolution, and content selection on the same head even if commits
    /// publish concurrently. It retains nothing in the store and is meant to
    /// live for one request or unit of work.
    pub async fn read_view(&self) -> Result<ReadView> {
        trace::in_operation(
            operation_span!(self.core, "read_view", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                let pinned = self
                    .core
                    .pinned_read(&self.namespace_id, Consistency::Strong)
                    .await?;
                Ok(ReadView::new(self.core.clone(), pinned, ReadSource::Head))
            },
        )
        .await
    }

    /// Captures the namespace state an unexpired snapshot preserves. A
    /// missing or expired snapshot returns `snapshot_not_found`.
    pub async fn read_view_at_snapshot(&self, snapshot_id: &PinId) -> Result<ReadView> {
        let span = operation_span!(
            self.core,
            "read_view_at_snapshot",
            self.namespace_id,
            snapshot_id = %snapshot_id
        );
        trace::in_operation(span, async {
            let permit = self.core.read_permit().await;
            self.read_view_at_snapshot_with_permit(snapshot_id, &permit)
                .await
        })
        .await
    }

    async fn read_view_at_snapshot_with_permit(
        &self,
        snapshot_id: &PinId,
        _permit: &BudgetPermit<'_>,
    ) -> Result<ReadView> {
        let pinned = self
            .core
            .pinned_read_at_snapshot(&self.namespace_id, snapshot_id)
            .await?;
        Ok(ReadView::new(
            self.core.clone(),
            pinned,
            ReadSource::Snapshot(snapshot_id.clone()),
        ))
    }

    /// Returns this namespace's metadata: its access mode, creation, fork
    /// basis, current head, and retention floor.
    pub async fn metadata(&self) -> Result<crate::NamespaceMetadata> {
        trace::in_operation(
            operation_span!(self.core, "metadata", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                Ok(self
                    .core
                    .namespace_store(&self.namespace_id)
                    .load_metadata()
                    .await?)
            },
        )
        .await
    }

    /// Resolves an absolute path to its authoritative entry at the current
    /// head.
    pub async fn stat(&self, absolute_path: &str) -> Result<PathEntry> {
        self.stat_with_options(absolute_path, &StatOptions::default())
            .await
    }

    /// Resolves an absolute path to its authoritative entry at the current
    /// head, projecting what `options` asks for.
    pub async fn stat_with_options(
        &self,
        absolute_path: &str,
        options: &StatOptions,
    ) -> Result<PathEntry> {
        trace::in_operation(
            operation_span!(self.core, "stat", self.namespace_id),
            async {
                let permit = self.core.read_permit().await;
                self.read_at(
                    &options.at,
                    &permit,
                    |pinned| async move { pinned.stat(absolute_path, options).await },
                    |view, permit| async move {
                        view.stat_with_permit(absolute_path, options, permit).await
                    },
                )
                .await
            },
        )
        .await
    }

    /// Returns the current entry for a visible inode.
    pub async fn stat_by_inode(&self, inode_id: InodeId) -> Result<PathEntry> {
        self.stat_by_inode_with_options(inode_id, &StatOptions::default())
            .await
    }

    /// Returns the current entry for a visible inode, projecting what
    /// `options` asks for.
    pub async fn stat_by_inode_with_options(
        &self,
        inode_id: InodeId,
        options: &StatOptions,
    ) -> Result<PathEntry> {
        trace::in_operation(
            operation_span!(self.core, "stat_by_inode", self.namespace_id),
            async {
                let permit = self.core.read_permit().await;
                self.read_at(
                    &options.at,
                    &permit,
                    |pinned| async move { pinned.stat_by_inode(inode_id, options).await },
                    |view, permit| async move {
                        view.stat_by_inode_with_permit(inode_id, options, permit)
                            .await
                    },
                )
                .await
            },
        )
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
        loonfs_types::Pager::with_state(
            (self.read_only(), absolute_path.to_owned(), options.clone()),
            |(reader, absolute_path, options), request| async move {
                let request = decode_list_page_request(request, &options)?;
                reader
                    .path_entries_page(&absolute_path, request, options)
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
        trace::in_operation(
            operation_span!(self.core, "list", self.namespace_id),
            async {
                let permit = self.core.read_permit().await;
                if matches!(options.at, ReadAt::Live(_)) {
                    reject_pinned_directory_cursor(request.cursor.as_ref())?;
                }
                let listed_path = AbsolutePath::parse(absolute_path).map_err(CoreError::from)?;
                self.read_at(
                    &options.at,
                    &permit,
                    |pinned| {
                        let request = request.clone();
                        let options = options.clone();
                        let listed_path = listed_path.clone();
                        async move {
                            // Give awakened validation waiters a turn before
                            // synchronous page work. The head is already
                            // captured and the validation guard has been
                            // released.
                            tokio::task::yield_now().await;
                            pinned
                                .path_entries_page(listed_path, request, options, None)
                                .await
                        }
                    },
                    |view, permit| {
                        let request = request.clone();
                        let options = options.clone();
                        async move {
                            view.path_entries_page(absolute_path, request, options, permit)
                                .await
                        }
                    },
                )
                .await
            },
        )
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
        loonfs_types::Pager::with_state(
            (self.read_only(), inode_id, options.clone()),
            |(reader, inode_id, options), request| async move {
                let request = decode_list_page_request(request, &options)?;
                reader.inode_children_page(inode_id, request, options).await
            },
        )
    }

    async fn inode_children_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<ListPageCursor>,
        options: ListOptions,
    ) -> Result<ListInodeChildrenResponse> {
        trace::in_operation(
            operation_span!(self.core, "list_by_inode", self.namespace_id),
            async {
                let permit = self.core.read_permit().await;
                if matches!(options.at, ReadAt::Live(_)) {
                    reject_pinned_directory_cursor(request.cursor.as_ref())?;
                }
                self.read_at(
                    &options.at,
                    &permit,
                    |pinned| {
                        let request = request.clone();
                        let options = options.clone();
                        async move {
                            // Give awakened validation waiters a turn before
                            // synchronous page work. The head is already
                            // captured and the validation guard has been
                            // released.
                            tokio::task::yield_now().await;
                            pinned
                                .inode_children_page(inode_id, request, options, None)
                                .await
                        }
                    },
                    |view, permit| {
                        let request = request.clone();
                        let options = options.clone();
                        async move {
                            view.inode_children_page(inode_id, request, options, permit)
                                .await
                        }
                    },
                )
                .await
            },
        )
        .await
    }

    /// Reads a file's current content plus the metadata entry it came from.
    pub async fn read_file(&self, absolute_path: &str) -> Result<FileBytes> {
        self.read_file_with_options(absolute_path, &ReadFileOptions::default())
            .await
    }

    /// Reads a file's current content plus the metadata entry it came from,
    /// as `options` asks.
    pub async fn read_file_with_options(
        &self,
        absolute_path: &str,
        options: &ReadFileOptions,
    ) -> Result<FileBytes> {
        trace::in_operation(
            operation_span!(self.core, "read_file", self.namespace_id),
            async {
                let permit = self.core.read_permit().await;
                let max_bytes = self.core.max_read_content_bytes();
                self.read_at(
                    &options.at,
                    &permit,
                    |pinned| async move { pinned.read_file(absolute_path, max_bytes).await },
                    |view, permit| async move {
                        view.read_file_with_permit(absolute_path, permit).await
                    },
                )
                .await
            },
        )
        .await
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
    /// The stream reads one chunk at a time, so it uses bounded memory. Size
    /// and checksum verification complete when [`FileContentStream::next_chunk`]
    /// returns `None`. Stopping early leaves the content unverified. The
    /// buffered-read size limit does not apply.
    ///
    /// [`ReadFileStreamOptions::range`] can resume a read, in which case the
    /// caller must supply earlier bytes for whole-object verification through
    /// [`FileContentStream::fold_resumed_prefix`]. It can also select one
    /// byte range of the file, which the stream returns without verification.
    pub async fn read_file_stream_with_options(
        &self,
        absolute_path: &str,
        options: &ReadFileStreamOptions,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        trace::in_operation(
            operation_span!(self.core, "read_file_stream", self.namespace_id),
            self.file_stream(absolute_path, None, options),
        )
        .await
    }

    async fn file_stream(
        &self,
        absolute_path: &str,
        revision_no: Option<RevisionNo>,
        options: &ReadFileStreamOptions,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        let permit = self.core.read_permit().await;
        ensure_revision_read_at(&options.at, revision_no)?;
        self.read_at(
            &options.at,
            &permit,
            |pinned| async move {
                pinned
                    .read_file_stream(absolute_path, revision_no, options)
                    .await
            },
            |view, permit| async move {
                view.read_file_stream_with_permit(absolute_path, options, permit)
                    .await
            },
        )
        .await
    }

    /// Prepares the content object of a file's current revision for a direct
    /// download.
    ///
    /// See the API specification's download transport contract. The handle's
    /// `max_read_content_bytes` does not apply to direct downloads.
    pub async fn create_download(&self, absolute_path: &str) -> Result<DirectDownloadTarget> {
        self.create_download_with_options(absolute_path, &DownloadOptions::default())
            .await
    }

    /// Prepares the content object of a file's current revision for a direct
    /// download, as `options` asks. See [`Self::create_download`].
    pub async fn create_download_with_options(
        &self,
        absolute_path: &str,
        options: &DownloadOptions,
    ) -> Result<DirectDownloadTarget> {
        trace::in_operation(
            operation_span!(self.core, "create_download", self.namespace_id),
            self.direct_download(absolute_path, None, options),
        )
        .await
    }

    /// Prepares the content object of one retained file revision for a
    /// direct download. See [`Self::create_download`].
    pub async fn create_revision_download(
        &self,
        absolute_path: &str,
        revision_no: RevisionNo,
    ) -> Result<DirectDownloadTarget> {
        self.create_revision_download_with_options(
            absolute_path,
            revision_no,
            &DownloadOptions::default(),
        )
        .await
    }

    /// Prepares the content object of one retained file revision for a
    /// direct download, as `options` asks. See [`Self::create_download`].
    ///
    /// A snapshot captures one revision of each file, so options that read a
    /// snapshot return `invalid_request`.
    pub async fn create_revision_download_with_options(
        &self,
        absolute_path: &str,
        revision_no: RevisionNo,
        options: &DownloadOptions,
    ) -> Result<DirectDownloadTarget> {
        trace::in_operation(
            operation_span!(self.core, "create_revision_download", self.namespace_id),
            self.direct_download(absolute_path, Some(revision_no), options),
        )
        .await
    }

    async fn direct_download(
        &self,
        absolute_path: &str,
        revision_no: Option<RevisionNo>,
        options: &DownloadOptions,
    ) -> Result<DirectDownloadTarget> {
        let permit = self.core.read_permit().await;
        ensure_revision_read_at(&options.at, revision_no)?;
        self.read_at(
            &options.at,
            &permit,
            |pinned| async move {
                pinned
                    .create_download(absolute_path, revision_no, options)
                    .await
            },
            |view, permit| async move {
                view.create_download_with_permit(absolute_path, options, permit)
                    .await
            },
        )
        .await
    }

    /// Prepares the content object of the current revision of a visible file
    /// inode, wherever it is bound, for a direct download.
    pub async fn create_download_by_inode(
        &self,
        inode_id: InodeId,
    ) -> Result<DirectDownloadByInodeTarget> {
        self.create_download_by_inode_with_options(inode_id, &DownloadOptions::default())
            .await
    }

    /// Prepares the content object of the current revision of a visible file
    /// inode for a direct download, as `options` asks.
    pub async fn create_download_by_inode_with_options(
        &self,
        inode_id: InodeId,
        options: &DownloadOptions,
    ) -> Result<DirectDownloadByInodeTarget> {
        trace::in_operation(
            operation_span!(self.core, "create_download_by_inode", self.namespace_id),
            self.direct_download_by_inode(inode_id, None, options),
        )
        .await
    }

    /// Prepares the content object of one retained inode revision for a
    /// direct download, without requiring a current path.
    pub async fn create_revision_download_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<DirectDownloadByInodeTarget> {
        self.create_revision_download_by_inode_with_options(
            inode_id,
            revision_no,
            &DownloadOptions::default(),
        )
        .await
    }

    /// Prepares the content object of one retained inode revision for a
    /// direct download, as `options` asks.
    ///
    /// A snapshot captures one revision of each file, so options that read a
    /// snapshot return `invalid_request`.
    pub async fn create_revision_download_by_inode_with_options(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        options: &DownloadOptions,
    ) -> Result<DirectDownloadByInodeTarget> {
        trace::in_operation(
            operation_span!(
                self.core,
                "create_revision_download_by_inode",
                self.namespace_id
            ),
            self.direct_download_by_inode(inode_id, Some(revision_no), options),
        )
        .await
    }

    async fn direct_download_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: Option<RevisionNo>,
        options: &DownloadOptions,
    ) -> Result<DirectDownloadByInodeTarget> {
        let permit = self.core.read_permit().await;
        ensure_revision_read_at(&options.at, revision_no)?;
        self.read_at(
            &options.at,
            &permit,
            |pinned| async move {
                pinned
                    .create_download_by_inode(inode_id, revision_no, options)
                    .await
            },
            |view, permit| async move {
                view.create_download_by_inode_with_permit(inode_id, options, permit)
                    .await
            },
        )
        .await
    }

    /// Lists files visible at a snapshot in ascending inode-ID order.
    ///
    /// The pinned manifest is read without replaying later WAL entries.
    /// Directories are omitted. A missing or expired snapshot returns
    /// `snapshot_not_found` rather than falling back to current state.
    pub fn list_checkpoint_files(&self, checkpoint_id: &PinId) -> CheckpointFilesPager {
        self.list_checkpoint_files_with_options(
            checkpoint_id,
            &ListCheckpointFilesOptions::default(),
        )
    }

    /// Lists files at a snapshot as `options` asks. With
    /// [`ListCheckpointFilesOptions::include_deleted`], the pages also list
    /// deleted files and files under a deleted directory.
    pub fn list_checkpoint_files_with_options(
        &self,
        checkpoint_id: &PinId,
        options: &ListCheckpointFilesOptions,
    ) -> CheckpointFilesPager {
        loonfs_types::Pager::with_state(
            (self.read_only(), checkpoint_id.clone(), *options),
            |(reader, checkpoint_id, options), request| async move {
                reader
                    .checkpoint_files_page(&checkpoint_id, request, options)
                    .await
            },
        )
    }

    async fn checkpoint_files_page(
        &self,
        checkpoint_id: &PinId,
        request: PageRequest<CheckpointFilesPageCursor>,
        options: ListCheckpointFilesOptions,
    ) -> Result<CheckpointFilesPage> {
        trace::in_operation(
            operation_span!(self.core, "list_checkpoint_files", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                let pinned = self
                    .core
                    .pinned_read(&self.namespace_id, Consistency::Strong)
                    .await?;
                Ok(pinned
                    .engine
                    .list_checkpoint_files_page(checkpoint_id, request, options, &pinned.context)
                    .await?)
            },
        )
        .await
    }

    /// Resolves the current state of each inode ID.
    ///
    /// Results use one read view and preserve input order. Missing or unreadable
    /// inodes return [`CurrentFile::Hidden`](crate::CurrentFile::Hidden), with no path or
    /// revision. Readable directories have a path but no revision.
    ///
    /// A batch larger than [`MAX_CURRENT_FILES_PER_STAT`](crate::MAX_CURRENT_FILES_PER_STAT)
    /// returns `invalid_request` before reading metadata.
    pub async fn stat_current_files_by_inode(
        &self,
        inode_ids: &[InodeId],
    ) -> Result<Vec<CurrentFileState>> {
        trace::in_operation(
            operation_span!(self.core, "stat_current_files_by_inode", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                self.core
                    .read(&self.namespace_id, None, |pinned| async move {
                        pinned.stat_current_files_by_inode(inode_ids).await
                    })
                    .await
            },
        )
        .await
    }

    /// Reads one immutable content object by reference.
    ///
    /// Requires namespace administrator access. Content must be published in
    /// the namespace's read view, including through a fork. Otherwise returns
    /// `path_not_found` without reading content bytes. A published reference
    /// whose object collection reclaimed also returns `path_not_found`.
    ///
    /// `max_bytes` is checked against the declared size before fetching. It is
    /// independent of the deployment's download limit so callers can apply a
    /// smaller memory budget. The read fails if the returned size or digest
    /// does not match the reference.
    pub async fn read_content(&self, content_ref: &ContentRef, max_bytes: u64) -> Result<Vec<u8>> {
        trace::in_operation(
            operation_span!(self.core, "read_content", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                self.core
                    .read(&self.namespace_id, None, |pinned| async move {
                        pinned.read_content(content_ref, max_bytes).await
                    })
                    .await
            },
        )
        .await
    }

    /// Lists the namespace's recoverable deletions, oldest deletion first.
    /// A deletion stays listed until it's undeleted, or until the retention
    /// floor reaches it and it expires.
    pub fn list_trash(&self) -> TrashPager {
        self.list_trash_with_options(&ListTrashOptions::default())
    }

    /// Lists the namespace's recoverable deletions, oldest deletion first,
    /// as `options` asks.
    pub fn list_trash_with_options(&self, options: &ListTrashOptions) -> TrashPager {
        loonfs_types::Pager::with_state(
            (self.read_only(), options.consistency),
            |(reader, consistency), request| async move {
                reader
                    .trash_page(decode_page_request(request)?, consistency)
                    .await
            },
        )
    }

    async fn trash_page(
        &self,
        request: PageRequest<TrashPageCursor>,
        consistency: Option<Consistency>,
    ) -> Result<loonfs_types::ListTrashResponse> {
        trace::in_operation(
            operation_span!(self.core, "list_trash", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                self.core
                    .read(&self.namespace_id, consistency, |pinned| {
                        let request = request.clone();
                        async move {
                            let page = pinned
                                .engine
                                .list_trash_page(request, &pinned.context)
                                .await?;
                            let next_cursor = encode_next_cursor(page.next_cursor.as_ref())?;
                            Ok(loonfs_types::ListTrashResponse {
                                namespace_id: self.namespace_id.clone(),
                                head_seq: pinned.head_seq(),
                                entries: page.items,
                                next_cursor,
                            })
                        }
                    })
                    .await
            },
        )
        .await
    }

    /// Lists a file path's revision history, newest first.
    pub fn list_file_revisions(&self, absolute_path: &str) -> FileRevisionsPager {
        self.list_file_revisions_with_options(absolute_path, &ListFileRevisionsOptions::default())
    }

    /// Lists a file path's revision history, newest first, as `options`
    /// asks.
    pub fn list_file_revisions_with_options(
        &self,
        absolute_path: &str,
        options: &ListFileRevisionsOptions,
    ) -> FileRevisionsPager {
        loonfs_types::Pager::with_state(
            (
                self.read_only(),
                absolute_path.to_owned(),
                options.consistency,
            ),
            |(reader, absolute_path, consistency), request| async move {
                reader
                    .file_revisions_page(&absolute_path, decode_page_request(request)?, consistency)
                    .await
            },
        )
    }

    async fn file_revisions_page(
        &self,
        absolute_path: &str,
        request: PageRequest<FileRevisionsPageCursor>,
        consistency: Option<Consistency>,
    ) -> Result<ListFileRevisionsResponse> {
        trace::in_operation(
            operation_span!(self.core, "list_file_revisions", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                let absolute_path = AbsolutePath::parse(absolute_path).map_err(CoreError::from)?;
                self.core
                    .read(&self.namespace_id, consistency, |pinned| {
                        let request = request.clone();
                        let absolute_path = absolute_path.clone();
                        async move {
                            let revisions = pinned
                                .engine
                                .list_file_revisions_page(
                                    absolute_path.as_str(),
                                    request,
                                    &pinned.context,
                                )
                                .await?;
                            Ok(file_revisions_page_response(
                                self.namespace_id.clone(),
                                pinned.head_seq(),
                                revisions.page,
                                revisions.inode_id,
                            )?)
                        }
                    })
                    .await
            },
        )
        .await
    }

    /// Lists the retained revisions of a file inode, newest first.
    pub fn list_file_revisions_by_inode(&self, inode_id: InodeId) -> FileRevisionsPager {
        self.list_file_revisions_by_inode_with_options(
            inode_id,
            &ListFileRevisionsOptions::default(),
        )
    }

    /// Lists the retained revisions of a file inode, newest first, as
    /// `options` asks.
    pub fn list_file_revisions_by_inode_with_options(
        &self,
        inode_id: InodeId,
        options: &ListFileRevisionsOptions,
    ) -> FileRevisionsPager {
        loonfs_types::Pager::with_state(
            (self.read_only(), inode_id, options.consistency),
            |(reader, inode_id, consistency), request| async move {
                reader
                    .file_revisions_by_inode_page(
                        inode_id,
                        decode_page_request(request)?,
                        consistency,
                    )
                    .await
            },
        )
    }

    async fn file_revisions_by_inode_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
        consistency: Option<Consistency>,
    ) -> Result<ListFileRevisionsResponse> {
        trace::in_operation(
            operation_span!(self.core, "list_file_revisions_by_inode", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                self.core
                    .read(&self.namespace_id, consistency, |pinned| {
                        let request = request.clone();
                        async move {
                            let page = pinned
                                .engine
                                .list_file_revisions_by_inode_page(
                                    inode_id,
                                    request,
                                    &pinned.context,
                                )
                                .await?;
                            Ok(file_revisions_page_response(
                                self.namespace_id.clone(),
                                pinned.head_seq(),
                                page,
                                inode_id,
                            )?)
                        }
                    })
                    .await
            },
        )
        .await
    }

    /// Reads the content of one historical file revision by path.
    pub async fn read_file_revision(
        &self,
        absolute_path: &str,
        revision_no: RevisionNo,
    ) -> Result<FileBytes> {
        trace::in_operation(
            operation_span!(self.core, "read_file_revision", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                let max_bytes = self.core.max_read_content_bytes();
                self.core
                    .read(&self.namespace_id, None, |pinned| async move {
                        Ok(pinned
                            .engine
                            .read_file_revision(
                                absolute_path,
                                revision_no,
                                &pinned.context,
                                Some(max_bytes),
                            )
                            .await?)
                    })
                    .await
            },
        )
        .await
    }

    /// Reads one historical file revision by path as bounded chunks instead
    /// of one buffer.
    ///
    /// See [`Self::read_file_stream_with_options`] for how the stream is
    /// verified.
    pub async fn read_file_revision_stream(
        &self,
        absolute_path: &str,
        revision_no: RevisionNo,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        self.read_file_revision_stream_with_options(
            absolute_path,
            revision_no,
            &ReadFileStreamOptions::default(),
        )
        .await
    }

    /// Reads one historical file revision by path as bounded chunks, as
    /// `options` asks. See [`Self::read_file_stream_with_options`].
    ///
    /// A snapshot captures one revision of each file, so options that read a
    /// snapshot return `invalid_request`.
    pub async fn read_file_revision_stream_with_options(
        &self,
        absolute_path: &str,
        revision_no: RevisionNo,
        options: &ReadFileStreamOptions,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        trace::in_operation(
            operation_span!(self.core, "read_file_revision_stream", self.namespace_id),
            self.file_stream(absolute_path, Some(revision_no), options),
        )
        .await
    }

    /// Reads the current content of a visible file inode, wherever it is
    /// bound, plus the metadata entry it came from.
    pub async fn read_file_by_inode(&self, inode_id: InodeId) -> Result<FileBytes> {
        self.read_file_by_inode_with_options(inode_id, &ReadFileOptions::default())
            .await
    }

    /// Reads the current content of a visible file inode, wherever it is
    /// bound, plus the metadata entry it came from, as `options` asks.
    pub async fn read_file_by_inode_with_options(
        &self,
        inode_id: InodeId,
        options: &ReadFileOptions,
    ) -> Result<FileBytes> {
        trace::in_operation(
            operation_span!(self.core, "read_file_by_inode", self.namespace_id),
            async {
                let permit = self.core.read_permit().await;
                let max_bytes = self.core.max_read_content_bytes();
                self.read_at(
                    &options.at,
                    &permit,
                    |pinned| async move { pinned.read_file_by_inode(inode_id, max_bytes).await },
                    |view, permit| async move {
                        view.read_file_by_inode_with_permit(inode_id, permit).await
                    },
                )
                .await
            },
        )
        .await
    }

    /// Streams the current content of a visible file inode, wherever it is
    /// bound. Complete verification requires consuming the stream to its end.
    pub async fn read_file_stream_by_inode(
        &self,
        inode_id: InodeId,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        self.read_file_stream_by_inode_with_options(inode_id, &ReadFileStreamOptions::default())
            .await
    }

    /// Streams the current content of a visible file inode, wherever it is
    /// bound, as `options` asks. Complete verification requires consuming the
    /// stream to its end.
    pub async fn read_file_stream_by_inode_with_options(
        &self,
        inode_id: InodeId,
        options: &ReadFileStreamOptions,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        trace::in_operation(
            operation_span!(self.core, "read_file_stream_by_inode", self.namespace_id),
            self.file_stream_by_inode(inode_id, None, options),
        )
        .await
    }

    /// Streams a retained inode revision, including content without a visible path.
    /// Complete verification requires consuming the stream to its end.
    pub async fn read_file_revision_stream_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        self.read_file_revision_stream_by_inode_with_options(
            inode_id,
            revision_no,
            &ReadFileStreamOptions::default(),
        )
        .await
    }

    /// Streams a retained inode revision, including content without a visible
    /// path, as `options` asks. Complete verification requires consuming the
    /// stream to its end.
    ///
    /// A snapshot captures one revision of each file, so options that read a
    /// snapshot return `invalid_request`.
    pub async fn read_file_revision_stream_by_inode_with_options(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        options: &ReadFileStreamOptions,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        trace::in_operation(
            operation_span!(
                self.core,
                "read_file_revision_stream_by_inode",
                self.namespace_id
            ),
            self.file_stream_by_inode(inode_id, Some(revision_no), options),
        )
        .await
    }

    async fn file_stream_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: Option<RevisionNo>,
        options: &ReadFileStreamOptions,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        let permit = self.core.read_permit().await;
        ensure_revision_read_at(&options.at, revision_no)?;
        self.read_at(
            &options.at,
            &permit,
            |pinned| async move {
                pinned
                    .read_file_stream_by_inode(inode_id, revision_no, options)
                    .await
            },
            |view, permit| async move {
                view.read_file_stream_by_inode_with_permit(inode_id, options, permit)
                    .await
            },
        )
        .await
    }

    /// Reads and verifies one retained file revision by inode identity.
    /// Current visibility and path are not required.
    pub async fn read_file_revision_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        trace::in_operation(
            operation_span!(self.core, "read_file_revision_by_inode", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                let max_bytes = self.core.max_read_content_bytes();
                self.core
                    .read(&self.namespace_id, None, |pinned| async move {
                        pinned
                            .read_file_revision_by_inode(inode_id, revision_no, max_bytes)
                            .await
                    })
                    .await
            },
        )
        .await
    }

    /// Lists the ordered change feed after `after_seq`.
    pub fn list_changes(&self, after_seq: ChangeSeq) -> ChangesPager {
        self.list_changes_with_options(after_seq, &ListChangesOptions::default())
    }

    /// Lists the ordered change feed after `after_seq`, as `options` asks.
    ///
    /// A page whose `after_seq` is above the head it reads returns
    /// `invalid_request`. An eventual page can read an older head than the
    /// read that the caller learned `after_seq` from, so a caller that gets
    /// this error from an eventual page reads the page again with
    /// [`Consistency::Strong`].
    pub fn list_changes_with_options(
        &self,
        after_seq: ChangeSeq,
        options: &ListChangesOptions,
    ) -> ChangesPager {
        loonfs_types::Pager::with_state(
            (self.read_only(), after_seq, options.at.clone()),
            |(reader, after_seq, at), request: PageRequest<ChangeSeq>| async move {
                reader
                    .changes_page(request.cursor.unwrap_or(after_seq), request.limit, &at)
                    .await
            },
        )
    }

    async fn changes_page(
        &self,
        after_seq: ChangeSeq,
        limit: EffectiveLimit,
        at: &ReadAt,
    ) -> Result<ListChangesResponse> {
        trace::in_operation(
            operation_span!(self.core, "list_changes", self.namespace_id),
            async {
                let permit = self.core.read_permit().await;
                self.read_at(
                    at,
                    &permit,
                    |pinned| async move { pinned.changes_page(after_seq, limit).await },
                    |view, permit| async move { view.changes_page(after_seq, limit, permit).await },
                )
                .await
            },
        )
        .await
    }
}
