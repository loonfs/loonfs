//! The API summaries of a pin record.

use loonfs_types::format::control::{PinOwner, PinPayload};
use loonfs_types::{Checkpoint, CheckpointOwner, Snapshot};

/// Converts a pin record of any owner to the row the checkpoint inventory
/// returns.
pub(crate) fn checkpoint_summary(record: PinPayload) -> Checkpoint {
    let expires_at_ms = record.owner.expires_at_ms();
    let owner = match record.owner {
        PinOwner::User { name, .. } => CheckpointOwner::User { name },
        PinOwner::Fork {
            target_namespace_id,
        } => CheckpointOwner::Fork {
            target_namespace_id,
        },
    };
    Checkpoint {
        namespace_id: record.namespace_id,
        manifest_no: record.pin_id.manifest_no(),
        checkpoint_id: record.pin_id,
        owner,
        created_at_ms: record.created_at_ms,
        expires_at_ms,
        captured_seq: record.head_seq,
    }
}

/// Converts a user pin record to the snapshot the API returns, or returns
/// `None` for a fork pin.
pub(crate) fn user_pin_snapshot(record: PinPayload) -> Option<Snapshot> {
    let PinOwner::User {
        name,
        expires_at_ms,
    } = record.owner
    else {
        return None;
    };
    Some(Snapshot {
        snapshot_id: record.pin_id,
        namespace_id: record.namespace_id,
        name,
        captured_seq: record.head_seq,
        created_at_ms: record.created_at_ms,
        expires_at_ms,
    })
}

/// Converts a user pin record to the snapshot the API returns.
pub(crate) fn snapshot_summary(record: PinPayload) -> Snapshot {
    user_pin_snapshot(record)
        .expect("a pin created or loaded as a user pin should have a user owner")
}
