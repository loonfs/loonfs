//! Deletes user-owned checkpoints.
//!
//! Forks and snapshots have separate lifecycle rules and cannot be deleted
//! through this operation.

use super::record::{checkpoint_is_visible, delete_checkpoint_record, load_checkpoint_record};
use crate::error::{CoreError, Result};
use crate::namespace::control::load_namespace_read_state;
use loonfs_api::wire::control::PinOwner;
use loonfs_api::{DeleteCheckpointResponse, NamespaceId, PinId};
use loonfs_objectstore::ObjectStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CheckpointOwnerKind {
    User,
    Fork,
    Snapshot,
    Retired,
}

impl CheckpointOwnerKind {
    fn of(owner: &PinOwner) -> Self {
        match owner {
            PinOwner::User { .. } => Self::User,
            PinOwner::Fork { .. } => Self::Fork,
            PinOwner::Snapshot { .. } => Self::Snapshot,
            PinOwner::Retired {} => Self::Retired,
        }
    }

    fn delete_guidance(self) -> &'static str {
        match self {
            Self::User => "delete it through the checkpoint delete operation",
            Self::Fork => "it is deleted by deleting that namespace",
            Self::Snapshot => {
                "it is deleted through the snapshot delete operation or by its expiry"
            }
            Self::Retired => "it is deleted when its generation is reclaimed",
        }
    }
}

pub(super) fn ensure_owner_is(
    checkpoint_id: &PinId,
    owner: &PinOwner,
    expected: CheckpointOwnerKind,
) -> Result<()> {
    let actual = CheckpointOwnerKind::of(owner);
    if actual == expected {
        return Ok(());
    }
    Err(CoreError::InvalidCheckpointRequest(format!(
        "checkpoint `{checkpoint_id}` is {}; {}",
        owner_description(owner),
        actual.delete_guidance()
    )))
}

fn owner_description(owner: &PinOwner) -> String {
    match owner {
        PinOwner::User { .. } => "a user checkpoint".to_owned(),
        PinOwner::Fork {
            target_namespace_id,
            ..
        } => format!("owned by fork target `{target_namespace_id}`"),
        PinOwner::Snapshot { .. } => "a snapshot".to_owned(),
        PinOwner::Retired {} => "a retired generation record".to_owned(),
    }
}

pub(super) async fn delete_owned_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &PinId,
    expected: CheckpointOwnerKind,
) -> Result<()> {
    let head = load_namespace_read_state(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    if !checkpoint_is_visible(&head, checkpoint_id) {
        return Err(not_found(checkpoint_id, expected));
    }
    let Some(loaded) = load_checkpoint_record(store, namespace_id, checkpoint_id).await? else {
        return Err(not_found(checkpoint_id, expected));
    };
    ensure_owner_is(checkpoint_id, &loaded.state.owner, expected)?;
    delete_checkpoint_record(store, namespace_id, checkpoint_id).await
}

fn not_found(checkpoint_id: &PinId, expected: CheckpointOwnerKind) -> CoreError {
    match expected {
        CheckpointOwnerKind::Snapshot => CoreError::SnapshotNotFound {
            snapshot_id: checkpoint_id.clone(),
        },
        _ => CoreError::CheckpointNotFound {
            checkpoint_id: checkpoint_id.clone(),
        },
    }
}

pub(crate) async fn delete_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &PinId,
) -> Result<DeleteCheckpointResponse> {
    delete_owned_checkpoint(
        store,
        namespace_id,
        checkpoint_id,
        CheckpointOwnerKind::User,
    )
    .await?;
    Ok(DeleteCheckpointResponse {
        namespace_id: namespace_id.clone(),
        checkpoint_id: checkpoint_id.clone(),
    })
}
