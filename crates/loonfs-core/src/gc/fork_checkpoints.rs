//! Collection rules for fork pins.

use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::control::load_current_manifest;
use loonfs_api::wire::control::{CheckpointRecordState, ForkBasis};
use loonfs_api::NamespaceId;
use loonfs_objectstore::ObjectStore;

pub(super) enum ForkCheckpointReachability {
    Reclaimable,
    Retained { reason: &'static str },
}

pub(super) async fn delete_source_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    basis: &ForkBasis,
) -> Result<bool> {
    let key = loonfs_objectstore::keys::checkpoint_record(
        &basis.manifest.owner_namespace_id,
        &basis.source_checkpoint_id,
    );
    let present = store
        .head(&key)
        .await
        .map_err(|error| CoreError::store(&key, &error))?
        .is_some();
    crate::checkpoint::record::delete_checkpoint_record(
        store,
        &basis.manifest.owner_namespace_id,
        &basis.source_checkpoint_id,
    )
    .await?;
    Ok(present)
}

pub(super) async fn classify_fork_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    record: &CheckpointRecordState,
    target_namespace_id: &NamespaceId,
    grace_window_ms: u64,
    context: &MutationContext,
) -> Result<ForkCheckpointReachability> {
    if context.now_ms.saturating_sub(record.created_at_ms) < grace_window_ms {
        return Ok(ForkCheckpointReachability::Retained {
            reason: "target_creation_in_flight",
        });
    }
    let target = match load_current_manifest(store, target_namespace_id).await {
        Ok(loaded) => loaded,
        Err(ControlObjectLoadError::MissingObject { .. }) => {
            return Ok(ForkCheckpointReachability::Reclaimable)
        }
        Err(error) => match &error {
            ControlObjectLoadError::Store { object_key, .. } => {
                tracing::warn!(
                    namespace_id = %target_namespace_id,
                    object_key,
                    error = %error,
                    "the fork target manifest did not read; retaining its source pin"
                );
                return Ok(ForkCheckpointReachability::Retained {
                    reason: "target_head_unreadable",
                });
            }
            _ => {
                return Err(CoreError::NamespaceCorrupt(format!(
                    "the fork target manifest does not load: {error}"
                )))
            }
        },
    };
    // A target that names another pin, or none, was installed by a later
    // attempt or a plain create; this pin is an abandoned attempt's.
    let Some(basis) = target
        .envelope
        .payload()
        .fork_basis
        .as_ref()
        .filter(|basis| basis.source_checkpoint_id == record.pin_id)
    else {
        return Ok(ForkCheckpointReachability::Reclaimable);
    };
    if basis.manifest != record.manifest() {
        return Err(CoreError::NamespaceCorrupt(format!(
            "fork target `{target_namespace_id}` names pin `{}` with a different manifest reference",
            record.pin_id
        )));
    }
    Ok(ForkCheckpointReachability::Retained {
        reason: "referenced_by_target",
    })
}
