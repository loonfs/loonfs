//! Publish plans that restore an earlier file revision.

use super::publish_path_planning::{
    reject_tombstoned_path_ancestor, resolve_visible_path_for_authorization,
    CompiledFilesystemOperation, PublishPathPlanningView,
};
use crate::authorize::Absence;
use crate::commit::CommitOp;
use crate::error::{CoreError, Result};
use crate::path::mutation_path::ensure_mutation_path;
use loonfs_api::{AbsolutePath, AccessRight, AccessRights, InodeKind, RevisionNo};
use loonfs_objectstore::ObjectStore;

pub(super) async fn plan_restore_revision<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    source_revision_no: RevisionNo,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    ensure_mutation_path(absolute_path)?;
    reject_tombstoned_path_ancestor(view, absolute_path).await?;
    let required = AccessRights::from_iter([AccessRight::Write, AccessRight::History]);
    let target = resolve_visible_path_for_authorization(
        view,
        absolute_path,
        required,
        Absence::Path(absolute_path.as_str()),
    )
    .await?;
    view.authorize(
        target.inode_id,
        required,
        Absence::Path(absolute_path.as_str()),
    )
    .await?;
    if target.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            target: absolute_path.as_str().to_owned(),
            kind: target.inode_kind,
        });
    }
    let revision = view
        .view
        .latest_revision_head(target.inode_id)
        .await?
        .ok_or_else(|| CoreError::PathNotFound(absolute_path.as_str().to_owned()))?;

    Ok(CompiledFilesystemOperation::new(vec![
        CommitOp::RestoreRevision {
            inode_id: target.inode_id,
            source_revision_no,
            base_revision_no: revision.revision_no,
        },
    ]))
}
