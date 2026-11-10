//! Source-pin cleanup for an eligible tombstone.

use super::live_set::{LiveSet, RetirementStatus};
use crate::error::Result;
use crate::namespace::store::NamespaceStore;
use loonfs_objectstore::ObjectStore;
use loonfs_types::GcResponse;

impl<S: ObjectStore> NamespaceStore<S> {
    /// Deletes the source pin that the tombstone's fork basis refers to, once
    /// the namespace is eligible for retirement. A namespace created without a
    /// source has no source pin, so its retirement deletes nothing.
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
            if self.delete_source_pin(basis).await? {
                report.deleted_checkpoints_by_owner.fork += 1;
            }
        }
        Ok(())
    }
}
