//! File revision reads on a [`Namespace`] handle in any mode: revision
//! history, one revision's bytes, and revision download targets.

use crate::downloads::{DirectDownloadByInodeTarget, DirectDownloadTarget};
use crate::namespace::reads::reject_snapshot_cursor;
use crate::pagers::{decode_page_request, FileRevisionsPager};
use crate::trace::{self, operation_span};
use crate::{
    CoreError, DownloadOptions, FileBytes, FileContentStream, InodeId, ListFileRevisionsResponse,
    Namespace, ReadFileStreamOptions, Result, RevisionNo, SharedObjectStore,
};
use loonfs_types::{AbsolutePath, FileRevisionsPageCursor, PageRequest};

impl<M: crate::Mode> Namespace<M> {
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

    /// Lists a file path's revision history, newest first.
    pub fn list_file_revisions(&self, absolute_path: &str) -> FileRevisionsPager {
        loonfs_types::Pager::with_state(
            (self.read_only(), absolute_path.to_owned()),
            |(reader, absolute_path), request| async move {
                reader
                    .file_revisions_page(&absolute_path, decode_page_request(request)?)
                    .await
            },
        )
    }

    async fn file_revisions_page(
        &self,
        absolute_path: &str,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
        trace::in_operation(
            operation_span!(self.core, "list_file_revisions", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                reject_snapshot_cursor(request.cursor.as_ref())?;
                let absolute_path = AbsolutePath::parse(absolute_path).map_err(CoreError::from)?;
                let cursor_head_seq = request.cursor.as_ref().map(|cursor| cursor.head_seq);
                self.read_head_at_least(cursor_head_seq, |pinned| {
                    let request = request.clone();
                    let absolute_path = absolute_path.clone();
                    async move {
                        pinned
                            .file_revisions_page(&absolute_path, request, None)
                            .await
                    }
                })
                .await
            },
        )
        .await
    }

    /// Lists the retained revisions of a file inode, newest first.
    pub fn list_file_revisions_by_inode(&self, inode_id: InodeId) -> FileRevisionsPager {
        loonfs_types::Pager::with_state(
            (self.read_only(), inode_id),
            |(reader, inode_id), request| async move {
                reader
                    .file_revisions_by_inode_page(inode_id, decode_page_request(request)?)
                    .await
            },
        )
    }

    async fn file_revisions_by_inode_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
        trace::in_operation(
            operation_span!(self.core, "list_file_revisions_by_inode", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                reject_snapshot_cursor(request.cursor.as_ref())?;
                let cursor_head_seq = request.cursor.as_ref().map(|cursor| cursor.head_seq);
                self.read_head_at_least(cursor_head_seq, |pinned| {
                    let request = request.clone();
                    async move {
                        pinned
                            .file_revisions_by_inode_page(inode_id, request, None)
                            .await
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
                self.read_head(|pinned| async move {
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
                self.read_head(|pinned| async move {
                    Ok(pinned
                        .engine
                        .read_file_revision_by_inode(
                            inode_id,
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
}
