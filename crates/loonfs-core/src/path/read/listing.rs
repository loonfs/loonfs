//! Directory listing pagination: cursor validation and page framing.

use crate::error::{CoreError, MetadataViewError, Result};
use crate::metadata::ResolvedVisiblePath;
use loonfs_api::{ChangeSeq, DirectoryPageCursor, InodeKind};

/// A directory cursor is only honored at the head it was minted at: a cursor
/// from the future is unanswerable, and a cursor from an older head would
/// need a historical listing this view does not serve.
pub(super) fn validate_cursor_head(
    current_head_seq: ChangeSeq,
    cursor: Option<&DirectoryPageCursor>,
) -> Result<()> {
    let Some(cursor) = cursor else {
        return Ok(());
    };
    if cursor.head_seq > current_head_seq {
        return Err(MetadataViewError::SnapshotUnavailable {
            requested_seq: cursor.head_seq,
            head_seq: current_head_seq,
        }
        .into());
    }
    if cursor.head_seq < current_head_seq {
        return Err(MetadataViewError::UnsupportedHistoricalRead {
            requested_seq: cursor.head_seq,
            head_seq: current_head_seq,
        }
        .into());
    }
    Ok(())
}

pub(super) fn validate_directory_cursor(
    cursor: &DirectoryPageCursor,
    resolved: &ResolvedVisiblePath,
) -> Result<()> {
    if resolved.inode_kind != InodeKind::Directory {
        return Err(invalid_cursor(
            "directory cursor resolved to a non-directory path",
        ));
    }
    if resolved.inode_id != cursor.directory_inode_id {
        return Err(invalid_cursor(format!(
            "cursor directory inode `{}` does not match requested path inode `{}`",
            cursor.directory_inode_id.0, resolved.inode_id.0
        )));
    }
    Ok(())
}

pub(super) fn invalid_cursor(message: impl Into<String>) -> CoreError {
    CoreError::InvalidCursor(message.into())
}
