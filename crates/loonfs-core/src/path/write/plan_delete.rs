//! Publish plans that delete visible paths.

use super::planner::OperationPlanningSession;
use super::publish_path_planning::{CompiledFilesystemOperation, Target};
use crate::commit::CommitOperation;
use crate::error::Result;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{AccessRight, AccessRights, BindingVersion, DeleteDirectoryBehavior, InodeKind};

impl<S: ObjectStore + ?Sized> OperationPlanningSession<'_, S> {
    /// Plans the delete of `target`, after checking its binding version when
    /// the request names one.
    pub(super) async fn plan_delete(
        &self,
        target: Target<'_>,
        expected_binding_version: Option<&BindingVersion>,
        behavior: DeleteDirectoryBehavior,
    ) -> Result<CompiledFilesystemOperation> {
        let remove = AccessRights::from(AccessRight::Remove);
        let target = self.view.resolve_target(target, remove).await?;
        self.view
            .authorize(target.parent_inode_id()?, remove, target.absence())
            .await?;
        // The expectations run after authorization: their errors name the
        // inode the caller found, which a subject without rights must not
        // learn.
        target.ensure_expected_inode()?;
        if let Some(expected) = expected_binding_version {
            self.view
                .ensure_binding_version(&target.resolved, expected)?;
        }
        let resolved = &target.resolved;
        let source_binding = self.view.source_binding(resolved).await?;
        let op = match resolved.inode_kind {
            InodeKind::File => CommitOperation::DeleteFile {
                inode_id: resolved.inode_id,
                source_binding,
            },
            InodeKind::Directory => CommitOperation::DeleteSubtree {
                root_inode_id: resolved.inode_id,
                source_binding,
                require_empty: behavior != DeleteDirectoryBehavior::Recursive,
            },
        };
        Ok(CompiledFilesystemOperation::new(vec![op]))
    }
}
