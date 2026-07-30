//! Publish plans that restore an earlier file revision.

use super::planning_helpers::{
    publish_binding_is_precondition, publish_reject_tombstoned_path_ancestor, PlannedOperation,
    PublishPathPlanningView,
};
use crate::commit::{CommitOp as ApiCommitOp, CommitPrecondition as ApiCommitPrecondition};
use crate::error::{CoreError, Result};
use crate::path::helpers::ensure_mutation_path;
use loonfs_api::{AbsolutePath, InodeKind, RevisionNo};
use loonfs_objectstore::ObjectStore;

pub(super) async fn plan_publish_restore_revision<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    source_revision_no: RevisionNo,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<PlannedOperation> {
    ensure_mutation_path(absolute_path)?;
    publish_reject_tombstoned_path_ancestor(view, absolute_path).await?;
    let target = view
        .metadata_state
        .resolve_visible_path(absolute_path)
        .await?;
    if target.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: absolute_path.as_str().to_owned(),
            kind: target.inode_kind,
        });
    }
    let revision = view
        .metadata_state
        .latest_revision_head(target.inode_id)
        .await?
        .ok_or_else(|| CoreError::PathNotFound(absolute_path.as_str().to_owned()))?;

    Ok(PlannedOperation::new(
        vec![ApiCommitOp::RestoreRevision {
            inode_id: target.inode_id,
            source_revision_no,
            base_revision_no: revision.revision_no,
        }],
        vec![
            publish_binding_is_precondition(view, &target).await?,
            ApiCommitPrecondition::InodeRevisionIs {
                inode_id: target.inode_id,
                revision_no: revision.revision_no,
            },
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: target.inode_id,
            },
        ],
    ))
}
