//! Releases user-owned checkpoints.
//!
//! Forks and snapshots have separate lifecycle rules and cannot be released
//! through this operation.

use super::record::{load_checkpoint_record, release_checkpoint_record};
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use loonfs_api::wire::control::CheckpointOwner;
use loonfs_api::{CheckpointId, NamespaceId, ReleaseCheckpointResponse};
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

    fn release_guidance(self) -> &'static str {
        match self {
            Self::User => "release it through the checkpoint release operation",
            Self::Fork => "it is released by deleting that namespace",
            Self::Snapshot => {
                "it is released through the snapshot release operation or by its expiry"
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
        actual.release_guidance()
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

pub(super) async fn release_owned_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    expected: CheckpointOwnerKind,
    context: &MutationContext,
) -> Result<()> {
    let Some(loaded) = load_checkpoint_record(store, namespace_id, checkpoint_id).await? else {
        return Ok(());
    };
    ensure_owner_is(checkpoint_id, &loaded.state.owner, expected)?;
    release_checkpoint_record(store, namespace_id, checkpoint_id, context.now_ms).await
}

pub(crate) async fn release_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    context: &MutationContext,
) -> Result<ReleaseCheckpointResponse> {
    release_owned_checkpoint(
        store,
        namespace_id,
        checkpoint_id,
        CheckpointOwnerKind::User,
        context,
    )
    .await?;
    Ok(ReleaseCheckpointResponse {
        namespace_id: namespace_id.clone(),
        checkpoint_id: checkpoint_id.clone(),
    })
}
