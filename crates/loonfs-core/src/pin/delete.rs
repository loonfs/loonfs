//! Deletes user checkpoints and snapshots; fork pins cannot be deleted here.

use super::record::{delete_pin, load_owned_pin, PinOwnerKind};
use crate::error::Result;
use loonfs_api::{DeleteCheckpointResponse, NamespaceId, PinId};
use loonfs_objectstore::ObjectStore;

pub(super) async fn delete_owned_pin<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    pin_id: &PinId,
    owner_kind: PinOwnerKind,
) -> Result<()> {
    load_owned_pin(store, namespace_id, pin_id, owner_kind).await?;
    delete_pin(store, namespace_id, pin_id).await
}

pub(crate) async fn delete_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &PinId,
) -> Result<DeleteCheckpointResponse> {
    delete_owned_pin(store, namespace_id, checkpoint_id, PinOwnerKind::User).await?;
    Ok(DeleteCheckpointResponse {
        namespace_id: namespace_id.clone(),
        checkpoint_id: checkpoint_id.clone(),
    })
}
