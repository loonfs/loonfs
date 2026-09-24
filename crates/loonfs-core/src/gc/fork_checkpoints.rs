//! Collection rules for fork pins.

use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::control::load_current_manifest_if_present;
use loonfs_api::wire::control::{ForkBasis, PinPayload};
use loonfs_api::NamespaceId;
use loonfs_objectstore::ObjectStore;

pub(super) async fn delete_source_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    basis: &ForkBasis,
) -> Result<bool> {
    let key = loonfs_objectstore::keys::checkpoint_record(
        &basis.manifest.owner_namespace_id,
        &basis.source_pin_id,
    );
    let present = store
        .head(&key)
        .await
        .map_err(|error| CoreError::store(&key, &error))?
        .is_some();
    crate::checkpoint::record::delete_checkpoint_record(
        store,
        &basis.manifest.owner_namespace_id,
        &basis.source_pin_id,
    )
    .await?;
    Ok(present)
}

pub(super) async fn fork_checkpoint_is_retained<S: ObjectStore + ?Sized>(
    store: &S,
    record: &PinPayload,
    target_namespace_id: &NamespaceId,
    grace_window_ms: u64,
    context: &MutationContext,
) -> Result<bool> {
    if context.now_ms.saturating_sub(record.created_at_ms) < grace_window_ms {
        return Ok(true);
    }
    match target_retains_checkpoint(store, record, target_namespace_id).await {
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

async fn target_retains_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    record: &PinPayload,
    target_namespace_id: &NamespaceId,
) -> Result<bool> {
    let Some(target) = load_current_manifest_if_present(store, target_namespace_id).await? else {
        return Ok(false);
    };
    let basis = target.envelope.payload().fork_basis.as_ref();
    Ok(basis.is_some_and(|basis| {
        basis.source_pin_id == record.pin_id && basis.manifest == record.manifest()
    }))
}
