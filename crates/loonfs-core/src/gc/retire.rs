//! The retirement of a namespace whose tombstone is eligible.

use super::live_set::{LiveSet, RetirementStatus};
use crate::error::Result;
use crate::namespace::store::NamespaceStore;
use loonfs_objectstore::ObjectStore;
use loonfs_types::GcResponse;

impl<S: ObjectStore> NamespaceStore<S> {
    /// Retires the namespace once its tombstone is eligible (format spec,
    /// "Retiring an eligible tombstone") in three steps: deletes the fork
    /// snapshot that the tombstone's fork basis refers to, deletes the
    /// namespace's member entry, and then raises the manifest boundary to
    /// retired. A namespace created without a source has no fork snapshot.
    ///
    /// The tombstone records the pool ID and the fork basis, which can't
    /// change after manifest 1, and the tombstone stays until the boundary is
    /// retired. So a collection that stops before the boundary is raised
    /// leaves the next collection to start again at the first step, and
    /// deleting a missing object succeeds.
    pub(super) async fn retire_namespace(
        &self,
        live: &LiveSet,
        report: &mut GcResponse,
    ) -> Result<()> {
        if live.retirement_status() != RetirementStatus::Eligible {
            return Ok(());
        }
        let tombstone = live
            .current_tombstone
            .as_ref()
            .expect("eligible namespace should have a tombstone");
        if let Some(basis) = &tombstone.fork_basis {
            if self.delete_source_snapshot(basis).await? {
                report.deleted_snapshots_by_owner.fork += 1;
            }
        }
        self.delete_member_entry(&tombstone.pool_id).await?;
        self.retire_boundary().await
    }
}
