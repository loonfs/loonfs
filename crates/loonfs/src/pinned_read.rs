//! A reader engine pinned to one read context, and the reads that a live
//! [`Namespace`](crate::Namespace) read and a [`ReadView`](crate::ReadView)
//! both make through it.

use crate::downloads::{DirectDownloadByInodeTarget, DirectDownloadTarget};
use crate::pagers::encode_next_list_cursor;
use crate::{
    ChangeSeq, ContentRef, CoreError, CurrentFileState, DownloadOptions, Error, FileBytes,
    FileContentStream, InodeId, ListChangesResponse, ListInodeChildrenResponse, ListOptions,
    ListPathEntriesResponse, NamespaceId, PathEntry, PinId, ReadFileStreamOptions, Result,
    RevisionNo, SharedObjectStore, StatOptions,
};
use loonfs_core::{NamespaceReaderEngine, RuntimeReadContext};
use loonfs_types::{AbsolutePath, EffectiveLimit, ListPageCursor, PageRequest};

/// A reader engine and the read context it reads at.
pub(crate) struct PinnedRead {
    pub(crate) engine: NamespaceReaderEngine<SharedObjectStore>,
    pub(crate) context: RuntimeReadContext,
}

impl PinnedRead {
    /// Returns the namespace that this read reads.
    pub(crate) fn namespace_id(&self) -> &NamespaceId {
        self.engine.namespace_id()
    }

    /// Returns the head sequence that this read reads at.
    pub(crate) fn head_seq(&self) -> ChangeSeq {
        self.context.read_basis.head.seq
    }

    /// Reads one page of the change feed. The feed's position is `after_seq`,
    /// and it has no cursor parameter.
    pub(crate) async fn changes_page(
        &self,
        after_seq: ChangeSeq,
        limit: EffectiveLimit,
    ) -> Result<ListChangesResponse> {
        self.engine.ensure_administrator(&self.context).await?;
        self.engine
            .list_changes_after(after_seq, limit, &self.context)
            .await
            .map_err(|error| match error {
                CoreError::InvalidCursor(message) => Error::InvalidRequest {
                    message,
                    param: "after_seq",
                },
                error => error.into(),
            })
    }

    pub(crate) async fn stat(
        &self,
        absolute_path: &str,
        options: &StatOptions,
    ) -> Result<PathEntry> {
        Ok(self
            .engine
            .resolve_path(absolute_path, options.clone(), &self.context)
            .await?)
    }

    pub(crate) async fn stat_by_inode(
        &self,
        inode_id: InodeId,
        options: &StatOptions,
    ) -> Result<PathEntry> {
        Ok(self
            .engine
            .stat_inode(inode_id, options.clone(), &self.context)
            .await?)
    }

    /// Lists one page of the directory at `listed_path`, and marks the next
    /// cursor with `pin_id`, the snapshot that the read reads.
    pub(crate) async fn path_entries_page(
        &self,
        listed_path: AbsolutePath,
        request: PageRequest<ListPageCursor>,
        options: ListOptions,
        pin_id: Option<&PinId>,
    ) -> Result<ListPathEntriesResponse> {
        let mut page = self
            .engine
            .list_path_page(listed_path.as_str(), request, options, &self.context)
            .await?;
        if let Some(cursor) = page.next_cursor.as_mut() {
            *cursor.pin_id_mut() = pin_id.cloned();
        }
        Ok(ListPathEntriesResponse {
            namespace_id: self.namespace_id().clone(),
            path: listed_path,
            head_seq: self.head_seq(),
            entries: page.items,
            next_cursor: encode_next_list_cursor(page.next_cursor.as_ref())?,
        })
    }

    /// Lists one page of the children of the directory inode `inode_id`, and
    /// marks the next cursor with `pin_id`, the snapshot that the read reads.
    pub(crate) async fn inode_children_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<ListPageCursor>,
        options: ListOptions,
        pin_id: Option<&PinId>,
    ) -> Result<ListInodeChildrenResponse> {
        let mut page = self
            .engine
            .list_inode_children_page(inode_id, request, options, &self.context)
            .await?;
        if let Some(cursor) = page.next_cursor.as_mut() {
            *cursor.pin_id_mut() = pin_id.cloned();
        }
        Ok(ListInodeChildrenResponse {
            namespace_id: self.namespace_id().clone(),
            parent_inode_id: inode_id,
            head_seq: self.head_seq(),
            entries: page.items,
            next_cursor: encode_next_list_cursor(page.next_cursor.as_ref())?,
        })
    }

    pub(crate) async fn stat_current_files_by_inode(
        &self,
        inode_ids: &[InodeId],
    ) -> Result<Vec<CurrentFileState>> {
        Ok(self
            .engine
            .stat_current_files_by_inode(inode_ids, &self.context)
            .await?)
    }

    pub(crate) async fn read_content(
        &self,
        content_ref: &ContentRef,
        max_bytes: u64,
    ) -> Result<Vec<u8>> {
        Ok(self
            .engine
            .read_content_ref(content_ref, max_bytes, &self.context)
            .await?)
    }

    pub(crate) async fn read_file(&self, absolute_path: &str, max_bytes: u64) -> Result<FileBytes> {
        Ok(self
            .engine
            .read_file(absolute_path, &self.context, Some(max_bytes))
            .await?)
    }

    pub(crate) async fn read_file_by_inode(
        &self,
        inode_id: InodeId,
        max_bytes: u64,
    ) -> Result<FileBytes> {
        Ok(self
            .engine
            .read_file_by_inode(inode_id, &self.context, Some(max_bytes))
            .await?)
    }

    pub(crate) async fn read_file_revision_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        max_bytes: u64,
    ) -> Result<Vec<u8>> {
        Ok(self
            .engine
            .read_file_revision_by_inode(inode_id, revision_no, &self.context, Some(max_bytes))
            .await?)
    }

    pub(crate) async fn read_file_stream(
        &self,
        absolute_path: &str,
        revision_no: Option<RevisionNo>,
        options: &ReadFileStreamOptions,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        Ok(self
            .engine
            .read_file_stream(
                absolute_path,
                &self.context,
                revision_no,
                options.chunk_bytes,
                options.range,
            )
            .await?)
    }

    pub(crate) async fn read_file_stream_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: Option<RevisionNo>,
        options: &ReadFileStreamOptions,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        Ok(self
            .engine
            .read_file_stream_by_inode(
                inode_id,
                revision_no,
                &self.context,
                options.chunk_bytes,
                options.range,
            )
            .await?)
    }

    pub(crate) async fn create_download(
        &self,
        absolute_path: &str,
        revision_no: Option<RevisionNo>,
        options: &DownloadOptions,
    ) -> Result<DirectDownloadTarget> {
        Ok(self
            .engine
            .direct_download_target(
                absolute_path,
                revision_no,
                options.start_offset,
                &self.context,
            )
            .await?)
    }

    pub(crate) async fn create_download_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: Option<RevisionNo>,
        options: &DownloadOptions,
    ) -> Result<DirectDownloadByInodeTarget> {
        Ok(self
            .engine
            .direct_download_target_by_inode(
                inode_id,
                revision_no,
                options.start_offset,
                &self.context,
            )
            .await?)
    }
}
