//! Directory listing pagination: cursor validation and page framing.

use crate::error::{CoreError, Result};
use crate::metadata::ResolvedVisiblePath;
use loonfs_api::{ChangeSeq, DirectoryPageCursor, InodeKind};

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

pub(super) fn validate_directory_cursor(
    cursor: &DirectoryPageCursor,
    resolved: &ResolvedVisiblePath,
) -> Result<()> {
    if resolved.inode_kind != InodeKind::Directory {
        return Err(invalid_cursor(
            "directory cursor resolved to a non-directory target",
        ));
    }
    if resolved.inode_id != cursor.directory_inode_id {
        return Err(invalid_cursor(format!(
            "cursor directory inode `{}` does not match the requested directory inode `{}`",
            cursor.directory_inode_id.0, resolved.inode_id.0
        )));
    }
    Ok(())
}

pub(super) fn invalid_cursor(message: impl Into<String>) -> CoreError {
    CoreError::InvalidCursor(message.into())
}
