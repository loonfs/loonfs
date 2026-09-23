//! Collection rules for fork pins.

use crate::checkpoint::load_namespace_manifest_envelope_if_present;
use crate::checkpoint::record::checkpoint_key_ids;
use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::control::load_current_manifest;
use futures::StreamExt;
use loonfs_api::wire::control::{ForkBasis, PinPayload};
use loonfs_api::{NamespaceGeneration, NamespaceId, PinId};
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

pub(super) async fn classify_fork_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    record: &PinPayload,
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
    let Some(basis) = target
        .envelope
        .payload()
        .fork_basis
        .as_ref()
        .filter(|basis| basis.source_pin_id == record.pin_id)
    else {
        if target.envelope.payload().generation > NamespaceGeneration(1) {
            return classify_prior_generations(store, target_namespace_id, &record.pin_id).await;
        }
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

async fn classify_prior_generations<S: ObjectStore + ?Sized>(
    store: &S,
    target_namespace_id: &NamespaceId,
    pin_id: &PinId,
) -> Result<ForkCheckpointReachability> {
    let prefix = loonfs_objectstore::keys::checkpoint_prefix(target_namespace_id);
    let mut listing = store.list_prefix_stream(&prefix);
    while let Some(key) = listing.next().await {
        let Ok(key) = key else {
            return Ok(ForkCheckpointReachability::Retained {
                reason: "target_head_unreadable",
            });
        };
        let Ok((_, retired_id)) = checkpoint_key_ids(&key) else {
            continue;
        };
        if !is_retired_pin(target_namespace_id, &retired_id) {
            continue;
        }
        let manifest_key = loonfs_objectstore::keys::metadata_manifest_object(
            target_namespace_id,
            &retired_id.manifest_no(),
        );
        let tombstone = match load_namespace_manifest_envelope_if_present(
            store,
            target_namespace_id,
            &retired_id.manifest_no(),
            &manifest_key,
        )
        .await
        {
            Ok(Some(tombstone)) if tombstone.payload().status.is_deleted() => tombstone,
            _ => {
                return Ok(ForkCheckpointReachability::Retained {
                    reason: "target_head_unreadable",
                })
            }
        };
        if tombstone
            .payload()
            .fork_basis
            .as_ref()
            .is_some_and(|basis| &basis.source_pin_id == pin_id)
        {
            return Ok(ForkCheckpointReachability::Retained {
                reason: "referenced_by_prior_generation",
            });
        }
    }
    Ok(ForkCheckpointReachability::Reclaimable)
}

pub(super) fn is_retired_pin(namespace_id: &NamespaceId, pin_id: &PinId) -> bool {
    *pin_id == PinId::retired(namespace_id, pin_id.manifest_no())
}
