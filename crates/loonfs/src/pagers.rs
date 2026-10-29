//! The pagers that the handles return, and the cursor helpers their pages
//! use.

use crate::{
    ChangeSeq, CheckpointFilesPage, CoreError, Error, InodeId, ListChangesResponse,
    ListCheckpointsResponse, ListFileRevisionsResponse, ListInodeChildrenResponse, ListOptions,
    ListPathEntriesResponse, ListSnapshotsResponse, NamespaceId,
};
use loonfs_types::{
    decode_cursor, encode_cursor, FileRevision, FileRevisionsPageCursor, ListPageCursor, Page,
    PageCursor, PageRequest,
};

/// A pager over directory entries.
pub type PathEntriesPager = loonfs_types::Pager<ListPathEntriesResponse, Error>;
/// A pager over directory children addressed by inode.
pub type InodeChildrenPager = loonfs_types::Pager<ListInodeChildrenResponse, Error>;
/// A pager over retained file revisions.
pub type FileRevisionsPager = loonfs_types::Pager<ListFileRevisionsResponse, Error>;
/// A pager over recoverable deletions.
pub type TrashPager = loonfs_types::Pager<loonfs_types::ListTrashResponse, Error>;
/// A pager over committed changes.
pub type ChangesPager = loonfs_types::Pager<ListChangesResponse, Error>;
/// A pager over the files a snapshot pins.
pub type CheckpointFilesPager = loonfs_types::Pager<CheckpointFilesPage, Error>;
/// A pager over existing checkpoints.
pub type CheckpointsPager = loonfs_types::Pager<ListCheckpointsResponse, Error>;
/// A pager over snapshots.
pub type SnapshotsPager = loonfs_types::Pager<ListSnapshotsResponse, Error>;

/// Decodes the wire cursor a pager has into the typed cursor a read
/// takes.
pub(crate) fn decode_page_request<C: PageCursor>(
    request: PageRequest<String>,
) -> std::result::Result<PageRequest<C>, CoreError> {
    let cursor = request
        .cursor
        .as_deref()
        .map(decode_cursor)
        .transpose()
        .map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
    Ok(PageRequest {
        limit: request.limit,
        cursor,
    })
}

/// Decodes a directory listing's wire cursor as the kind of cursor that a
/// listing with `options` issues.
pub(crate) fn decode_list_page_request(
    request: PageRequest<String>,
    options: &ListOptions,
) -> std::result::Result<PageRequest<ListPageCursor>, CoreError> {
    let cursor = request
        .cursor
        .as_deref()
        .map(|cursor| ListPageCursor::decode(cursor, options))
        .transpose()
        .map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
    Ok(PageRequest {
        limit: request.limit,
        cursor,
    })
}

pub(crate) fn encode_next_list_cursor(
    cursor: Option<&ListPageCursor>,
) -> std::result::Result<Option<String>, CoreError> {
    cursor
        .map(ListPageCursor::encode)
        .transpose()
        .map_err(|error| CoreError::InvalidCursor(error.to_string()))
}

pub(crate) fn encode_next_cursor<C: PageCursor>(
    cursor: Option<&C>,
) -> std::result::Result<Option<String>, CoreError> {
    cursor
        .map(encode_cursor)
        .transpose()
        .map_err(|error| CoreError::InvalidCursor(error.to_string()))
}

pub(crate) fn file_revisions_page_response(
    namespace_id: NamespaceId,
    head_seq: ChangeSeq,
    page: Page<FileRevision, FileRevisionsPageCursor>,
    inode_id: InodeId,
) -> std::result::Result<ListFileRevisionsResponse, CoreError> {
    let next_cursor = encode_next_cursor(page.next_cursor.as_ref())?;
    Ok(ListFileRevisionsResponse {
        namespace_id,
        inode_id,
        head_seq,
        revisions: page.items,
        next_cursor,
    })
}
