use crate::error::{CoreError, MetadataViewError};
use crate::metadata::ResolvedVisiblePath;
use loonfs_api::{ChangeSeq, DirectoryPageCursor, InodeKind};

pub(super) fn page_head_seq(
    current_head_seq: ChangeSeq,
    snapshot_floor_seq: ChangeSeq,
    cursor: Option<&DirectoryPageCursor>,
) -> Result<ChangeSeq, CoreError> {
    let Some(cursor) = cursor else {
        return Ok(current_head_seq);
    };
    if cursor.head_seq > current_head_seq {
        return Err(MetadataViewError::SnapshotUnavailable {
            requested_seq: cursor.head_seq,
            snapshot_floor_seq,
            head_seq: current_head_seq,
        }
        .into());
    }
    if cursor.head_seq < snapshot_floor_seq {
        return Err(MetadataViewError::SnapshotUnavailable {
            requested_seq: cursor.head_seq,
            snapshot_floor_seq,
            head_seq: current_head_seq,
        }
        .into());
    }
    Ok(cursor.head_seq)
}

pub(super) fn validate_directory_cursor(
    cursor: &DirectoryPageCursor,
    resolved: &ResolvedVisiblePath,
) -> Result<(), CoreError> {
    if resolved.inode_kind != InodeKind::Dir {
        return Err(invalid_cursor(
            "directory cursor resolved to a non-directory path",
        ));
    }
    if resolved.inode_id != cursor.dir_inode_id {
        return Err(invalid_cursor(format!(
            "cursor directory inode `{}` does not match requested path inode `{}`",
            cursor.dir_inode_id.0, resolved.inode_id.0
        )));
    }
    Ok(())
}

pub(super) fn invalid_cursor(message: impl Into<String>) -> CoreError {
    CoreError::InvalidCursor(message.into())
}
