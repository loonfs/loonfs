#[cfg(any(test, feature = "inspection"))]
use super::resolver::build_authoritative_path_entry;
use crate::error::{CoreError, MetadataViewError};
use crate::metadata::ResolvedVisiblePath;
#[cfg(any(test, feature = "inspection"))]
use crate::namespace::full_materialization::FullNamespaceMaterialization;
#[cfg(any(test, feature = "inspection"))]
use crate::path::helpers::{map_path_error_to_core, parse_absolute_path_for_core};
#[cfg(any(test, feature = "inspection"))]
use loonfs_api::{AbsolutePath, AuthoritativePathEntry, DisplayName};
use loonfs_api::{ChangeSeq, DirectoryPageCursor, InodeKind};

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "walk_path")
)]
#[cfg(any(test, feature = "inspection"))]
pub(crate) fn list_path_from_full_materialization(
    materialization: &FullNamespaceMaterialization,
    absolute_path: &str,
) -> Result<Vec<AuthoritativePathEntry>, CoreError> {
    let absolute_path = parse_absolute_path_for_core(absolute_path)?;
    let resolved = materialization.metadata_state.resolve_visible_path(
        &absolute_path,
        materialization.head.name_policy,
        materialization.head.seq,
    )?;
    if resolved.inode_kind == InodeKind::File {
        return Ok(vec![build_authoritative_path_entry(
            &materialization.head.namespace_id,
            materialization.head.seq,
            &materialization.metadata_state,
            &resolved,
        )?]);
    }
    if resolved.inode_kind != InodeKind::Dir {
        return Err(CoreError::ExpectedDirectory {
            path: resolved.absolute_path,
            kind: resolved.inode_kind,
        });
    }

    materialization
        .metadata_state
        .visible_children(resolved.inode_id, materialization.head.seq)
        .into_iter()
        .map(|direntry| {
            let child = materialization
                .metadata_state
                .visible_inode(direntry.child_inode_id, materialization.head.seq)
                .expect("visible child listing should resolve inode");
            let child_path = AbsolutePath::parse(&resolved.absolute_path)
                .map_err(map_path_error_to_core)?
                .join(&DisplayName::parse(&direntry.display_name).map_err(map_path_error_to_core)?);
            build_authoritative_path_entry(
                &materialization.head.namespace_id,
                materialization.head.seq,
                &materialization.metadata_state,
                &ResolvedVisiblePath {
                    absolute_path: child_path.as_str().to_owned(),
                    inode_id: direntry.child_inode_id,
                    inode_kind: child.inode_kind,
                    parent_inode_id: Some(direntry.parent_inode_id),
                    display_name: direntry.display_name,
                },
            )
        })
        .collect()
}

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
