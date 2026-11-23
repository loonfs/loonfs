//! Publish plans that restore an earlier file revision.

use super::planner::OperationPlanningSession;
use super::publish_path_planning::Target;
use crate::error::Result;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{AccessRight, AccessRights, RevisionNo};

impl<S: ObjectStore + ?Sized> OperationPlanningSession<'_, '_, S> {
    pub(super) async fn plan_restore_revision(
        &mut self,
        target: Target<'_>,
        source_revision_no: RevisionNo,
    ) -> Result<()> {
        let required = AccessRights::from_iter([AccessRight::Write, AccessRight::History]);
        let target = self
            .view()
            .resolve_authorized_target(target, required)
            .await?;
        target.ensure_file()?;
        let binding = self.view().source_binding(target.resolved()).await?;
        self.commit
            .restore_revision(binding, source_revision_no)
            .await
    }
}
