//! Deletes user checkpoints and snapshots; fork pins cannot be deleted here.

use super::record::{delete_checkpoint_record, load_owned_checkpoint_record, CheckpointOwnerKind};
use crate::error::Result;
use loonfs_api::{DeleteCheckpointResponse, NamespaceId, PinId};
use loonfs_objectstore::ObjectStore;

pub(super) async fn delete_owned_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &PinId,
    owner_kind: CheckpointOwnerKind,
) -> Result<()> {
    load_owned_checkpoint_record(store, namespace_id, checkpoint_id, owner_kind).await?;
    delete_checkpoint_record(store, namespace_id, checkpoint_id).await
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
