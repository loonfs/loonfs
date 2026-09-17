//! Publish plans that delete visible paths.

use super::ensure_expected_inode;
use super::publish_path_planning::{
    source_binding, CompiledFilesystemOperation, PublishPathPlanningView,
};
use crate::authorize::Absence;
use crate::commit::CommitOp;
use crate::error::{CoreError, Result};
use crate::metadata::ResolvedVisiblePath;
use crate::path::mutation_path::{ensure_mutation_path, final_component};
use loonfs_api::{
    AbsolutePath, AccessRight, AccessRights, DeleteDirectoryBehavior, InodeId, InodeKind,
};
use loonfs_objectstore::ObjectStore;

pub(super) async fn plan_delete_path<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    behavior: DeleteDirectoryBehavior,
    expected_inode_id: Option<InodeId>,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    ensure_mutation_path(absolute_path)?;
    let resolved = view.view.resolve_visible_path(absolute_path).await?;
    let name = final_component(absolute_path)?;
    plan_delete(
        view,
        &resolved,
        behavior,
        Absence::Path(absolute_path.as_str()),
        || ensure_expected_inode(&resolved, expected_inode_id, &name),
    )
    .await
}

/// `expectation` runs after authorization: its errors name the inode the
/// caller found, which a subject without rights must not learn.
pub(super) async fn plan_delete<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    resolved: &ResolvedVisiblePath,
    behavior: DeleteDirectoryBehavior,
    absence: Absence<'_>,
    expectation: impl FnOnce() -> Result<()>,
) -> Result<CompiledFilesystemOperation> {
    view.authorize(
        resolved
            .parent_inode_id
            .ok_or(CoreError::RootMutationForbidden)?,
        AccessRights::from_iter([AccessRight::Remove]),
        absence,
    )
    .await?;
    expectation()?;
    let recursive = behavior == DeleteDirectoryBehavior::Recursive;
    let source_binding = source_binding(view, resolved).await?;
    let op = match resolved.inode_kind {
        InodeKind::File => CommitOp::DeleteFile {
            inode_id: resolved.inode_id,
            source_binding,
        },
        InodeKind::Directory if recursive => CommitOp::DeleteSubtree {
            root_inode_id: resolved.inode_id,
            source_binding,
            require_empty: false,
        },
        InodeKind::Directory => CommitOp::DeleteSubtree {
            root_inode_id: resolved.inode_id,
            source_binding,
            require_empty: true,
        },
    };
    Ok(CompiledFilesystemOperation::new(vec![op]))
}
