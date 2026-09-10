//! Deletes user-owned checkpoints.
//!
//! Forks and snapshots have separate lifecycle rules and cannot be deleted
//! through this operation.

use super::record::{delete_checkpoint_record, load_checkpoint_record};
use crate::error::{CoreError, Result};
use loonfs_api::wire::control::CheckpointOwner;
use loonfs_api::{CheckpointId, DeleteCheckpointResponse, NamespaceId};
use loonfs_objectstore::ObjectStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CheckpointOwnerKind {
    User,
    Fork,
    Snapshot,
}

impl CheckpointOwnerKind {
    fn of(owner: &CheckpointOwner) -> Self {
        match owner {
            CheckpointOwner::User { .. } => Self::User,
            CheckpointOwner::Fork { .. } => Self::Fork,
            CheckpointOwner::Snapshot { .. } => Self::Snapshot,
        }
    }

    fn delete_guidance(self) -> &'static str {
        match self {
            Self::User => "delete it through the checkpoint delete operation",
            Self::Fork => "it is deleted by deleting that namespace",
            Self::Snapshot => {
                "it is deleted through the snapshot delete operation or by its expiry"
            }
        }
    }
}

pub(super) fn ensure_owner_is(
    checkpoint_id: &CheckpointId,
    owner: &CheckpointOwner,
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

fn owner_description(owner: &CheckpointOwner) -> String {
    match owner {
        CheckpointOwner::User { .. } => "a user checkpoint".to_owned(),
        CheckpointOwner::Fork {
            target_namespace_id,
            ..
        } => format!("owned by fork target `{target_namespace_id}`"),
        CheckpointOwner::Snapshot { .. } => "a snapshot".to_owned(),
    }
}

pub(super) async fn delete_owned_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    expected: CheckpointOwnerKind,
) -> Result<()> {
    let Some(loaded) = load_checkpoint_record(store, namespace_id, checkpoint_id).await? else {
        return Err(match expected {
            CheckpointOwnerKind::Snapshot => CoreError::SnapshotNotFound {
                snapshot_id: checkpoint_id.clone(),
            },
            CheckpointOwnerKind::User | CheckpointOwnerKind::Fork => {
                CoreError::CheckpointNotFound {
                    checkpoint_id: checkpoint_id.clone(),
                }
            }
        });
    };
    ensure_owner_is(checkpoint_id, &loaded.state.owner, expected)?;
    delete_checkpoint_record(store, namespace_id, checkpoint_id).await
}

pub(crate) async fn delete_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
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
