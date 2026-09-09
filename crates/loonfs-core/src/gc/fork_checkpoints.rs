//! Collection rules for fork pins.

use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::control::load_current_manifest;
use crate::namespace::state::NamespaceReadState;
use loonfs_api::wire::control::CheckpointRecordState;
use loonfs_api::NamespaceId;
use loonfs_objectstore::ObjectStore;

/// Whether a fork checkpoint is still needed.
pub(super) enum ForkCheckpointReachability {
    Reclaimable,
    Retained { reason: &'static str },
}

/// Compares a fork checkpoint with the target head that may reference it.
/// An absent target retains its pin for the creation grace.
pub(super) async fn classify_fork_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    record: &CheckpointRecordState,
    target_namespace_id: &NamespaceId,
    grace_window_ms: u64,
    context: &MutationContext,
) -> Result<ForkCheckpointReachability> {
    let head = match load_current_manifest(store, target_namespace_id).await {
        Ok(loaded) => NamespaceReadState::from(loaded.envelope.payload()),
        Err(ControlObjectLoadError::MissingObject { .. }) => {
            return Ok(
                if context.now_ms.saturating_sub(record.created_at_ms) >= grace_window_ms {
                    ForkCheckpointReachability::Reclaimable
                } else {
                    ForkCheckpointReachability::Retained {
                        reason: "target_creation_in_flight",
                    }
                },
            )
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
    let Some(basis) = head.fork_basis else {
        return Ok(ForkCheckpointReachability::Reclaimable);
    };
    if basis.manifest.owner_namespace_id != record.namespace_id
        || basis.source_checkpoint_id != record.pin_id
    {
        return Ok(ForkCheckpointReachability::Reclaimable);
    }
    if head.status.is_deleted() {
        return Ok(match head.status.reclaim_after_ms() {
            None => ForkCheckpointReachability::Retained {
                reason: "target_not_retired",
            },
            Some(deadline) if deadline > context.now_ms => ForkCheckpointReachability::Retained {
                reason: "target_retirement_grace",
            },
            Some(_) => ForkCheckpointReachability::Reclaimable,
        });
    }
    if basis.manifest != record.manifest() {
        return Err(CoreError::NamespaceCorrupt(format!(
            "the fork target `{target_namespace_id}` names checkpoint `{}` but names a \
             different manifest reference",
            record.pin_id
        )));
    }
    Ok(ForkCheckpointReachability::Retained {
        reason: "referenced_by_live_target",
    })
}
