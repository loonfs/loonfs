//! Collection rules for fork pins.

use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::fork::target_retains_pin;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ForkBasis, PinPayload};
use loonfs_types::NamespaceId;

pub(super) async fn delete_source_pin<S: ObjectStore + ?Sized>(
    store: &S,
    basis: &ForkBasis,
) -> Result<bool> {
    let key =
        loonfs_objectstore::keys::pin(&basis.manifest.owner_namespace_id, &basis.source_pin_id);
    let present = store
        .get_with_metadata(&key)
        .await
        .map_err(|error| CoreError::store(&key, &error))?
        .is_some();
    crate::pin::record::delete_pin(
        store,
        &basis.manifest.owner_namespace_id,
        &basis.source_pin_id,
    )
    .await?;
    Ok(present)
}

pub(super) async fn fork_pin_is_retained<S: ObjectStore + ?Sized>(
    store: &S,
    record: &PinPayload,
    target_namespace_id: &NamespaceId,
    grace_window_ms: u64,
    context: &MutationContext,
) -> Result<bool> {
    if context.now_ms.saturating_sub(record.created_at_ms) < grace_window_ms {
        return Ok(true);
    }
    match target_retains_pin(store, record, target_namespace_id).await {
        Err(CoreError::ControlObjectLoad(error @ ControlObjectLoadError::Store { .. })) => {
            tracing::warn!(
                namespace_id = %target_namespace_id,
                error = %error,
                "the fork target did not read; retaining its source pin"
            );
            Ok(true)
        }
        result => result,
    }
}
