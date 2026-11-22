//! Publish plans that delete visible paths.

use super::planner::OperationPlanningSession;
use super::publish_path_planning::Target;
use crate::error::Result;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{AccessRight, AccessRights, BindingVersion, DeleteDirectoryBehavior, InodeKind};

impl<S: ObjectStore + ?Sized> OperationPlanningSession<'_, '_, S> {
    /// Plans the delete of `target`, after checking its binding version when
    /// the request names one.
    pub(super) async fn plan_delete(
        &mut self,
        target: Target<'_>,
        expected_binding_version: Option<&BindingVersion>,
        behavior: DeleteDirectoryBehavior,
    ) -> Result<()> {
        let remove = AccessRights::from(AccessRight::Remove);
        let target = self.view().resolve_target(target, remove).await?;
        self.view()
            .authorize(target.parent_inode_id()?, remove, target.absence())
            .await?;
        // The expectations run after authorization: their errors name the
        // inode the caller found, which a subject without rights must not
        // learn.
        target.ensure_expected_inode()?;
        if let Some(expected) = expected_binding_version {
            self.view()
                .ensure_binding_version(&target.resolved, expected)?;
        }
        let resolved = &target.resolved;
        let source_binding = self.view().source_binding(resolved).await?;
        match resolved.inode_kind {
            InodeKind::File => {
                self.validation
                    .delete_file(resolved.inode_id, &source_binding)
                    .await
            }
            InodeKind::Directory => {
                self.validation
                    .delete_subtree(
                        resolved.inode_id,
                        &source_binding,
                        behavior != DeleteDirectoryBehavior::Recursive,
                    )
                    .await
            }
        }
    }
}
