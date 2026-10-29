//! Inode-addressed reads on a [`Namespace`] handle in any mode: stats,
//! listings, file bytes, and download targets by inode ID.

use crate::downloads::DirectDownloadByInodeTarget;
use crate::namespace::reads::ensure_revision_read_at;
use crate::namespace::reads::reject_pinned_directory_cursor;
use crate::pagers::{decode_list_page_request, InodeChildrenPager};
use crate::trace::{self, operation_span};
use crate::{
    CurrentFileState, DownloadOptions, FileBytes, FileContentStream, InodeId,
    ListInodeChildrenResponse, ListOptions, Namespace, PathEntry, ReadAt, ReadFileOptions,
    ReadFileStreamOptions, Result, RevisionNo, SharedObjectStore, StatOptions,
};
use loonfs_types::{ListPageCursor, PageRequest};

impl<M: crate::Mode> Namespace<M> {
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

    pub(crate) async fn direct_download_by_inode(
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

    pub(crate) async fn file_stream_by_inode(
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
}
