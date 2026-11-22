//! Snapshot deletion decisions and the collection rules for fork snapshots.

use super::deletes::{AgeWindow, Decision};
use super::roots::RetirementStatus;
use crate::control_object::present;
use crate::error::{CoreError, Result};
use crate::limits::GC_GRACE_WINDOW_MS;
use crate::namespace::store::NamespaceStore;
use crate::snapshot::record::load_snapshot_record_at_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ForkBasis, SnapshotOwner, SnapshotPayload};
use loonfs_types::{GcResponse, RetainedReason};

impl<S: ObjectStore> NamespaceStore<S> {
    /// Loads the snapshot record at `key`, a key from a listing, or returns `None`
    /// when the record is gone.
    pub(super) async fn load_listed_snapshot_record(
        &self,
        key: &str,
    ) -> Result<Option<SnapshotPayload>> {
        Ok(
            present(load_snapshot_record_at_key(&self.store, key).await)?
                .map(|loaded| loaded.state),
        )
    }

    /// Deletes the two fork snapshots that `basis`, this namespace's fork
    /// basis, records: the fork snapshot on the source and this namespace's
    /// own fork point snapshot. Returns how many of them were present.
    pub(super) async fn delete_fork_snapshots(&self, basis: &ForkBasis) -> Result<u64> {
        let mut deleted = 0;
        for (namespace_id, snapshot_id) in [
            (
                &basis.manifest.owner_namespace_id,
                &basis.source_snapshot_id,
            ),
            (&self.namespace_id, &basis.fork_point_snapshot_id),
        ] {
            let key = loonfs_objectstore::keys::snapshot_record(namespace_id, snapshot_id);
            let present = self
                .store
                .get_with_metadata(&key)
                .await
                .map_err(|error| CoreError::store(&key, &error))?
                .is_some();
            self.with_namespace(namespace_id.clone())
                .delete_snapshot_record(snapshot_id)
                .await?;
            deleted += u64::from(present);
        }
        Ok(deleted)
    }
}

/// Decides at `now_ms` what to do with the snapshot at `key`, whose record is
/// `record`, of a namespace whose retirement status is `status`.
///
/// The source's collection keeps every fork snapshot, and a fork's own
/// collection keeps its fork point snapshot, because the target's retirement
/// deletes both (format spec, "Snapshot cleanup").
pub(super) fn sweep_snapshot_record(
    key: &str,
    record: &SnapshotPayload,
    status: RetirementStatus,
    now_ms: u64,
) -> Decision<GcResponse> {
    if let SnapshotOwner::Fork { .. } = record.owner {
        return Decision::retain(RetainedReason::SnapshotNotDeletable, None, now_ms);
    }
    let reclaimable = [
        record.owner.expires_at_ms(),
        record.owner.deleted_at_ms(),
        status.is_deleted().then_some(record.created_at_ms),
    ]
    .into_iter()
    .flatten()
    .map(|at_ms| AgeWindow::after(at_ms, GC_GRACE_WINDOW_MS))
    .min_by_key(|window| window.ends_at_ms);
    match reclaimable {
        Some(window) if window.has_passed(now_ms) => Decision::Delete {
            keys: vec![key.to_owned()],
            counter: |report| &mut report.deleted_snapshots_by_owner.user,
        },
        _ => Decision::retain(
            RetainedReason::SnapshotNotDeletable,
            reclaimable.map(|window| window.ends_at_ms),
            now_ms,
        ),
    }
}
