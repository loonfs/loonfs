//! A reader engine and the read context that one read is pinned to. A live
//! [`Namespace`](crate::Namespace) read and a [`ReadView`](crate::ReadView)
//! both read through one.
//!
//! A read calls the engine and passes the pinned context, as in
//! `pinned.engine.read_file(path, &pinned.context, Some(max_bytes))`. The
//! methods here are the reads that do more than that one call: the change
//! feed maps the core's cursor error to the request's parameter, and the
//! directory and revision listings build the response and its next cursor.

use crate::pagers::{encode_next_list_cursor, file_revisions_page_response};
use crate::{
    CommitSeq, CoreError, Error, InodeId, ListChangesResponse, ListFileRevisionsResponse,
    ListInodeChildrenResponse, ListOptions, ListPathEntriesResponse, NamespaceId, Result,
    SharedObjectStore, SnapshotId,
};
use loonfs_core::{NamespaceReaderEngine, RuntimeReadContext};
use loonfs_types::options::CommitSplitting;
use loonfs_types::{
    AbsolutePath, ChangeFeedPosition, EffectiveLimit, FileRevisionsPageCursor, ListPageCursor,
    PageRequest,
};

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

    /// Returns the sequence that this read reads at: the head, or the
    /// retained sequence below the head that the read's context sets.
    pub(crate) fn head_seq(&self) -> CommitSeq {
        self.context.visible_seq()
    }

    /// Reads one page of the change feed. The feed's position is `position`,
    /// and it has no cursor parameter.
    pub(crate) async fn changes_page(
        &self,
        position: ChangeFeedPosition,
        limit: EffectiveLimit,
        splitting: CommitSplitting,
    ) -> Result<ListChangesResponse> {
        self.engine
            .list_changes_after(position, limit, splitting, &self.context)
            .await
            .map_err(|error| match error {
                CoreError::InvalidCursor(message) => Error::InvalidRequest {
                    message,
                    param: "after_seq",
                },
                error => error.into(),
            })
    }

    /// Lists one page of the directory at `listed_path`, and marks the next
    /// cursor with `snapshot_id`, the snapshot that the read reads.
    pub(crate) async fn path_entries_page(
        &self,
        listed_path: AbsolutePath,
        request: PageRequest<ListPageCursor>,
        options: ListOptions,
        snapshot_id: Option<&SnapshotId>,
    ) -> Result<ListPathEntriesResponse> {
        let mut page = self
            .engine
            .list_path_page(listed_path.as_str(), request, options, &self.context)
            .await?;
        if let Some(cursor) = page.next_cursor.as_mut() {
            *cursor.snapshot_id_mut() = snapshot_id.cloned();
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
    /// marks the next cursor with `snapshot_id`, the snapshot that the read reads.
    pub(crate) async fn inode_children_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<ListPageCursor>,
        options: ListOptions,
        snapshot_id: Option<&SnapshotId>,
    ) -> Result<ListInodeChildrenResponse> {
        let mut page = self
            .engine
            .list_inode_children_page(inode_id, request, options, &self.context)
            .await?;
        if let Some(cursor) = page.next_cursor.as_mut() {
            *cursor.snapshot_id_mut() = snapshot_id.cloned();
        }
        Ok(ListInodeChildrenResponse {
            namespace_id: self.namespace_id().clone(),
            parent_inode_id: inode_id,
            head_seq: self.head_seq(),
            entries: page.items,
            next_cursor: encode_next_list_cursor(page.next_cursor.as_ref())?,
        })
    }

    /// Lists one page of the revisions of the file at `absolute_path`, and
    /// marks the next cursor with `snapshot_id`, the snapshot that the read
    /// reads.
    pub(crate) async fn file_revisions_page(
        &self,
        absolute_path: &AbsolutePath,
        request: PageRequest<FileRevisionsPageCursor>,
        snapshot_id: Option<&SnapshotId>,
    ) -> Result<ListFileRevisionsResponse> {
        let mut revisions = self
            .engine
            .list_file_revisions_page(absolute_path.as_str(), request, &self.context)
            .await?;
        if let Some(cursor) = revisions.page.next_cursor.as_mut() {
            cursor.snapshot_id = snapshot_id.cloned();
        }
        Ok(file_revisions_page_response(
            self.namespace_id().clone(),
            self.head_seq(),
            revisions.page,
            revisions.inode_id,
        )?)
    }

    /// Lists one page of the revisions of the file inode `inode_id`, and
    /// marks the next cursor with `snapshot_id`, the snapshot that the read
    /// reads.
    pub(crate) async fn file_revisions_by_inode_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
        snapshot_id: Option<&SnapshotId>,
    ) -> Result<ListFileRevisionsResponse> {
        let mut page = self
            .engine
            .list_file_revisions_by_inode_page(inode_id, request, &self.context)
            .await?;
        if let Some(cursor) = page.next_cursor.as_mut() {
            cursor.snapshot_id = snapshot_id.cloned();
        }
        Ok(file_revisions_page_response(
            self.namespace_id().clone(),
            self.head_seq(),
            page,
            inode_id,
        )?)
    }
}
