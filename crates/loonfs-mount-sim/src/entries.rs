//! The single-entry reads: `GET /filesystem/entry` and `GET /inodes/{inode_id}`.

use crate::error::{MountSimError, Result};
use crate::query::Query;
use crate::reply::Reply;
use crate::state::ServerState;
use loonfs_model::visibility::PathLookup;
use loonfs_types::{ErrorCode, InodeId, NamespaceId};

const ENTRY_PARAMS: &[&str] = &[
    "path",
    "include_attributes",
    "consistency",
    "max_staleness_ms",
    "snapshot_id",
];
const INODE_PARAMS: &[&str] = &[
    "include_attributes",
    "consistency",
    "max_staleness_ms",
    "snapshot_id",
];

pub(crate) fn get_path_entry(
    state: &ServerState,
    namespace_id: &NamespaceId,
    query: &Query,
) -> Result<Reply> {
    query.ensure_only(ENTRY_PARAMS)?;
    let path = query.path("path")?;
    let include_attributes = query.bool("include_attributes", true)?;
    let at = query.read_point()?;
    let namespace = state.namespace(namespace_id)?;
    let view = namespace.view(namespace.read_seq(&at, state.now_ms())?);
    match view.resolve_path(&path) {
        PathLookup::Found { inode, .. } => {
            let path = view.inode_path(inode.inode_id);
            Ok(Reply::json(&view.path_entry(
                inode,
                path,
                include_attributes,
            )))
        }
        lookup => Err(path_lookup_error(&lookup)),
    }
}

pub(crate) fn get_inode(
    state: &ServerState,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    query: &Query,
) -> Result<Reply> {
    query.ensure_only(INODE_PARAMS)?;
    let include_attributes = query.bool("include_attributes", true)?;
    let at = query.read_point()?;
    let namespace = state.namespace(namespace_id)?;
    let view = namespace.view(namespace.read_seq(&at, state.now_ms())?);
    let inode = view
        .visible_inode(inode_id)
        .ok_or_else(|| inode_not_found(inode_id))?;
    let path = view.inode_path(inode_id);
    Ok(Reply::json(&view.path_entry(
        inode,
        path,
        include_attributes,
    )))
}

/// Returns the error for a path that didn't resolve to a visible entry.
pub(crate) fn path_lookup_error(lookup: &PathLookup<'_>) -> MountSimError {
    match lookup {
        PathLookup::NotADirectory { absolute_path, .. } => MountSimError::new(
            ErrorCode::PathConflict,
            format!("`{absolute_path}` isn't a directory"),
        ),
        PathLookup::NotFound { absolute_path } | PathLookup::Found { absolute_path, .. } => {
            MountSimError::new(
                ErrorCode::PathNotFound,
                format!("no visible entry at `{absolute_path}`"),
            )
        }
    }
}

pub(crate) fn inode_not_found(inode_id: InodeId) -> MountSimError {
    MountSimError::new(
        ErrorCode::InodeNotFound,
        format!(
            "inode `{}` isn't visible",
            loonfs_types::public_inode_id::encode(inode_id)
        ),
    )
}
