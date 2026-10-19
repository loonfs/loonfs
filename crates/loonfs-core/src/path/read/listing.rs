//! Directory listing pagination: cursor validation and page framing.

use super::tree_listing::MAX_TREE_LISTING_DEPTH;
use crate::error::{CoreError, Result};
use crate::metadata::ResolvedVisiblePath;
use loonfs_types::options::ListOptions;
use loonfs_types::{
    ChangeSeq, DirectoryPageCursor, DirectoryTreePageCursor, InodeId, InodeKind, ListPageCursor,
    PageRequest,
};

/// A cursor is an ordering resume: any head at or past the one that minted
/// it serves the next page, resuming strictly after the last returned key,
/// the same forward-only drift grep and change-feed cursors tolerate. Only a
/// cursor from a head ahead of this view's is refused, as an invalid cursor:
/// this view did not issue it.
pub(super) fn validate_cursor_head(
    current_head_seq: ChangeSeq,
    cursor_head_seq: Option<ChangeSeq>,
) -> Result<()> {
    let Some(cursor_head_seq) = cursor_head_seq else {
        return Ok(());
    };
    if cursor_head_seq > current_head_seq {
        return Err(invalid_cursor(format!(
            "the cursor was minted at seq `{cursor_head_seq}`, ahead of the serving head `{current_head_seq}`"
        )));
    }
    Ok(())
}

/// One page of a directory listing: of the directory's children, or of its
/// whole subtree.
pub(super) enum DirectoryListingRequest {
    Children(PageRequest<DirectoryPageCursor>),
    Tree(PageRequest<DirectoryTreePageCursor>),
}

impl DirectoryListingRequest {
    /// Pairs a page request with the listing that `options` selects, and
    /// validates its cursor against the listed target.
    ///
    /// Each kind of listing accepts only its own kind of cursor.
    pub(super) fn new(
        request: PageRequest<ListPageCursor>,
        options: &ListOptions,
        resolved: &ResolvedVisiblePath,
    ) -> Result<Self> {
        let PageRequest { limit, cursor } = request;
        match (options.recursive, cursor) {
            (false, None) => Ok(Self::Children(PageRequest {
                limit,
                cursor: None,
            })),
            (false, Some(ListPageCursor::Children(cursor))) => {
                validate_cursor_directory(cursor.directory_inode_id, resolved)?;
                Ok(Self::Children(PageRequest {
                    limit,
                    cursor: Some(cursor),
                }))
            }
            (true, None) => Ok(Self::Tree(PageRequest {
                limit,
                cursor: None,
            })),
            (true, Some(ListPageCursor::Tree(cursor))) => {
                validate_tree_cursor(&cursor, resolved)?;
                Ok(Self::Tree(PageRequest {
                    limit,
                    cursor: Some(cursor),
                }))
            }
            (false, Some(ListPageCursor::Tree(_))) => Err(invalid_cursor(
                "a recursive listing cursor can't resume a listing that isn't recursive",
            )),
            (true, Some(ListPageCursor::Children(_))) => Err(invalid_cursor(
                "a directory cursor can't resume a recursive listing",
            )),
        }
    }
}

/// Checks a recursive listing cursor's shape: one frame for each level from
/// the listed directory down, at most the walk's depth, and a name key in
/// every frame but the last.
fn validate_tree_cursor(
    cursor: &DirectoryTreePageCursor,
    resolved: &ResolvedVisiblePath,
) -> Result<()> {
    let Some((last, levels)) = cursor.frames.split_last() else {
        return Err(invalid_cursor("recursive listing cursor has no frames"));
    };
    if cursor.frames.len() > MAX_TREE_LISTING_DEPTH {
        return Err(invalid_cursor(format!(
            "recursive listing cursor has `{}` frames, more than the walk's depth `{MAX_TREE_LISTING_DEPTH}`",
            cursor.frames.len()
        )));
    }
    if levels.iter().any(|frame| frame.last_name_key.is_none()) {
        return Err(invalid_cursor(
            "only the last frame of a recursive listing cursor can omit its name key",
        ));
    }
    let root = levels.first().unwrap_or(last);
    validate_cursor_directory(root.directory_inode_id, resolved)
}

fn validate_cursor_directory(
    cursor_directory_inode_id: InodeId,
    resolved: &ResolvedVisiblePath,
) -> Result<()> {
    if resolved.inode_kind != InodeKind::Directory {
        return Err(invalid_cursor(
            "directory cursor resolved to a non-directory target",
        ));
    }
    if resolved.inode_id != cursor_directory_inode_id {
        return Err(invalid_cursor(format!(
            "cursor directory inode `{}` does not match the requested directory inode `{}`",
            cursor_directory_inode_id.0, resolved.inode_id.0
        )));
    }
    Ok(())
}

pub(super) fn invalid_cursor(message: impl Into<String>) -> CoreError {
    CoreError::InvalidCursor(message.into())
}
