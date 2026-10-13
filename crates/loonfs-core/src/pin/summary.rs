//! The API summary of a pin record.

pub(crate) fn checkpoint_summary(
    record: loonfs_types::format::control::PinPayload,
) -> loonfs_types::Checkpoint {
    let expires_at_ms = record.owner.expires_at_ms();
    let owner = match record.owner {
        loonfs_types::format::control::PinOwner::User { name, .. } => {
            loonfs_types::CheckpointOwnerSummary::User { name }
        }
        loonfs_types::format::control::PinOwner::Fork {
            target_namespace_id,
            ..
        } => loonfs_types::CheckpointOwnerSummary::Fork {
            target_namespace_id,
        },
        loonfs_types::format::control::PinOwner::Snapshot { name, .. } => {
            loonfs_types::CheckpointOwnerSummary::Snapshot { name }
        }
    };
    loonfs_types::Checkpoint {
        namespace_id: record.namespace_id,
        manifest_no: record.pin_id.manifest_no(),
        checkpoint_id: record.pin_id,
        owner,
        created_at_ms: record.created_at_ms,
        expires_at_ms,
        captured_seq: record.head_seq,
    }
}

/// Converts a snapshot-owned pin record to the snapshot summary the API
/// returns.
pub(crate) fn snapshot_summary(
    record: loonfs_types::format::control::PinPayload,
) -> loonfs_types::SnapshotSummary {
    loonfs_types::SnapshotSummary::from_checkpoint(checkpoint_summary(record))
        .expect("a pin created with, or loaded as, a snapshot owner should have one")
}
