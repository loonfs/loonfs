//! The API summaries of a snapshot record.

use loonfs_types::format::control::{SnapshotOwner, SnapshotPayload};
use loonfs_types::{Snapshot, SnapshotRecord, SnapshotRecordOwner};

/// Converts a snapshot record of any owner to the row the snapshot inventory
/// returns.
pub(crate) fn snapshot_record_summary(record: SnapshotPayload) -> SnapshotRecord {
    let expires_at_ms = record.owner.expires_at_ms();
    let owner = match record.owner {
        SnapshotOwner::User { name, .. } => SnapshotRecordOwner::User { name },
        SnapshotOwner::Fork {
            target_namespace_id,
        } => SnapshotRecordOwner::Fork {
            target_namespace_id,
        },
    };
    SnapshotRecord {
        namespace_id: record.namespace_id,
        manifest_no: record.snapshot_id.manifest_no(),
        snapshot_id: record.snapshot_id,
        owner,
        created_at_ms: record.created_at_ms,
        expires_at_ms,
        captured_seq: record.head_seq,
    }
}

/// Converts a user snapshot record to the snapshot the API returns, or returns
/// `None` for a fork snapshot.
pub(crate) fn user_snapshot(record: SnapshotPayload) -> Option<Snapshot> {
    let SnapshotOwner::User {
        name,
        expires_at_ms,
    } = record.owner
    else {
        return None;
    };
    Some(Snapshot {
        snapshot_id: record.snapshot_id,
        namespace_id: record.namespace_id,
        name,
        captured_seq: record.head_seq,
        created_at_ms: record.created_at_ms,
        expires_at_ms,
    })
}

/// Converts a user snapshot record to the snapshot the API returns.
pub(crate) fn snapshot_summary(record: SnapshotPayload) -> Snapshot {
    user_snapshot(record)
        .expect("a snapshot record created or loaded as a user snapshot should have a user owner")
}
