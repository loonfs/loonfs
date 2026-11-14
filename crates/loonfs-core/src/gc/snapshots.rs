//! Snapshot deletion decisions, the collection rules for fork snapshots, and grace
//! window age checks.

use super::deletes::Decision;
use super::live_set::LiveSet;
use crate::control_object::present;
use crate::error::{CoreError, Result};
use crate::limits::GC_GRACE_WINDOW_MS;
use crate::namespace::store::NamespaceStore;
use crate::snapshot::record::load_snapshot_record_at_key;
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ForkBasis, SnapshotOwner, SnapshotPayload};
use loonfs_types::{GcResponse, RetainedReason};

impl<S: ObjectStore> NamespaceStore<S> {
    /// Reads the snapshot record at `key` and decides what to do with the snapshot.
    pub(super) async fn sweep_snapshot(
        &self,
        key: &str,
        live: &LiveSet,
        clock: &OperationClock,
    ) -> Result<Decision<GcResponse>> {
        Ok(match self.load_listed_snapshot_record(key).await? {
            Some(record) => sweep_snapshot_record(key, &record, live, clock),
            None => Decision::Gone,
        })
    }

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

    pub(super) async fn delete_source_snapshot(&self, basis: &ForkBasis) -> Result<bool> {
        let key = loonfs_objectstore::keys::snapshot_record(
            &basis.manifest.owner_namespace_id,
            &basis.source_snapshot_id,
        );
        let present = self
            .store
            .get_with_metadata(&key)
            .await
            .map_err(|error| CoreError::store(&key, &error))?
            .is_some();
        self.with_namespace(basis.manifest.owner_namespace_id.clone())
            .delete_snapshot_record(&basis.source_snapshot_id)
            .await?;
        Ok(present)
    }
}

/// Decides what to do with the snapshot at `key`, whose record is `record`.
///
/// The source's collection keeps every fork snapshot, because the target's
/// retirement deletes it (format spec, "Snapshot cleanup").
pub(super) fn sweep_snapshot_record(
    key: &str,
    record: &SnapshotPayload,
    live: &LiveSet,
    clock: &OperationClock,
) -> Decision<GcResponse> {
    let now_ms = clock.request_at_ms();
    if let SnapshotOwner::Fork { .. } = record.owner {
        return Decision::retain(RetainedReason::SnapshotNotDeletable, None, now_ms);
    }
    let expires_at_ms = record
        .owner
        .expires_at_ms()
        .map(|expiry| expiry.saturating_add(GC_GRACE_WINDOW_MS));
    let deleted_at_ms = record
        .owner
        .deleted_at_ms()
        .map(|deletion| deletion.saturating_add(GC_GRACE_WINDOW_MS));
    let ages_out_at_ms = live
        .current_tombstone
        .is_some()
        .then(|| record.created_at_ms.saturating_add(GC_GRACE_WINDOW_MS));
    let reclaimable_at_ms = expires_at_ms
        .into_iter()
        .chain(deleted_at_ms)
        .chain(ages_out_at_ms)
        .min();
    match reclaimable_at_ms {
        Some(at_ms) if now_ms >= at_ms => Decision::Delete {
            keys: vec![key.to_owned()],
            counter: |report| &mut report.deleted_snapshots_by_owner.user,
        },
        _ => Decision::retain(
            RetainedReason::SnapshotNotDeletable,
            reclaimable_at_ms,
            now_ms,
        ),
    }
}

/// Where one unreferenced candidate stands against the grace window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GraceAge {
    /// The window has passed over the object.
    Aged,
    /// Younger than the window by its own provider timestamp.
    Young,
    /// The provider reported no last-modified time, so the object's age is
    /// unknown and it is treated as young (rule 1).
    Unknown,
}

impl GraceAge {
    /// Compares a listed modification time with the collection's grace window.
    pub(crate) fn of(last_modified_at_ms: Option<u64>, grace_window_ms: u64, now_ms: u64) -> Self {
        match last_modified_at_ms {
            None => Self::Unknown,
            Some(modified) if now_ms.saturating_sub(modified) < grace_window_ms => Self::Young,
            Some(_) => Self::Aged,
        }
    }
}
