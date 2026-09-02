//! Release rules for fork-owned and missing-basis checkpoint records.

use crate::checkpoint::record::{
    load_checkpoint_record_at_key, release_checkpoint_record, release_inspected_checkpoint_record,
    CheckpointRelease,
};
use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::control::{load_head_object, load_metadata_root_object_if_present};
use loonfs_api::wire::control::{CheckpointOwner, CheckpointRecordState, CheckpointStatus};
use loonfs_api::NamespaceId;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::ObjectStore;

pub(super) enum ForkCheckpointSweep {
    /// The record was released.
    Released,
    /// The record must survive this pass.
    Retained,
    /// The normal checkpoint path handles this record.
    NotAnActiveFork,
}

pub(super) enum MissingBasisCheckpointSweep {
    Released,
    Retained,
}

/// Releases an active, non-fork checkpoint whose basis manifest is still
/// missing after the grace period.
pub(super) async fn release_missing_basis_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    key: &str,
    grace_window_ms: u64,
    context: &MutationContext,
) -> Result<MissingBasisCheckpointSweep> {
    let loaded = load_checkpoint_record_at_key(store, key).await;
    let loaded = match loaded {
        Ok(loaded) => loaded,
        Err(ControlObjectLoadError::MissingObject { .. }) => {
            return Ok(MissingBasisCheckpointSweep::Retained)
        }
        Err(error) => return Err(CoreError::ControlObjectLoad(error)),
    };
    let record = loaded.state;
    if record.status != (CheckpointStatus::Active {}) {
        return Ok(MissingBasisCheckpointSweep::Retained);
    }
    // Fork checkpoints remain until their target namespace is deleted.
    if matches!(record.owner, CheckpointOwner::Fork { .. }) {
        return Ok(MissingBasisCheckpointSweep::Retained);
    }
    if context.now_ms.saturating_sub(record.created_at_ms) < grace_window_ms {
        return Ok(MissingBasisCheckpointSweep::Retained);
    }
    let manifest_key = metadata_manifest_object(namespace_id, &record.manifest.manifest_object_id);
    if store
        .head(&manifest_key)
        .await
        .map_err(|error| CoreError::store(&manifest_key, &error))?
        .is_some()
    {
        return Ok(MissingBasisCheckpointSweep::Retained);
    }
    release_checkpoint_record(store, namespace_id, &record.checkpoint_id, context.now_ms).await?;
    Ok(MissingBasisCheckpointSweep::Released)
}

/// Rechecks a fork checkpoint and releases it if its target no longer uses it.
pub(super) async fn maybe_release_fork_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    context: &MutationContext,
) -> Result<ForkCheckpointSweep> {
    let loaded = load_checkpoint_record_at_key(store, key).await;
    let loaded = match loaded {
        Ok(loaded) => loaded,
        Err(ControlObjectLoadError::MissingObject { .. }) => {
            return Ok(ForkCheckpointSweep::NotAnActiveFork)
        }
        Err(error) => return Err(CoreError::ControlObjectLoad(error)),
    };
    let CheckpointOwner::Fork {
        target_namespace_id,
        expires_at_ms,
    } = loaded.state.owner.clone()
    else {
        return Ok(ForkCheckpointSweep::NotAnActiveFork);
    };
    match classify_fork_checkpoint(
        store,
        &loaded.state,
        &target_namespace_id,
        expires_at_ms,
        context,
    )
    .await?
    {
        ForkCheckpointReachability::Reclaimable => {}
        ForkCheckpointReachability::Retained { reason } => {
            tracing::debug!(
                namespace_id = %loaded.state.namespace_id,
                target_namespace_id = %target_namespace_id,
                checkpoint_id = %loaded.state.checkpoint_id,
                reason,
                "retaining a fork checkpoint its target may still need"
            );
            return Ok(ForkCheckpointSweep::Retained);
        }
    }
    if loaded.state.status != (CheckpointStatus::Active {}) {
        return Ok(ForkCheckpointSweep::NotAnActiveFork);
    }
    match release_inspected_checkpoint_record(store, key, loaded, context.now_ms).await? {
        CheckpointRelease::Released => Ok(ForkCheckpointSweep::Released),
        // Another pass won the record; retain and let a later pass re-decide
        // against the fresh state.
        CheckpointRelease::LostRace => Ok(ForkCheckpointSweep::Retained),
    }
}

/// Whether a fork checkpoint is still needed.
pub(super) enum ForkCheckpointReachability {
    Reclaimable,
    Retained { reason: &'static str },
}

/// Compares a fork checkpoint with the target head that may reference it.
/// An absent target remains in flight until the checkpoint lease expires.
pub(super) async fn classify_fork_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    record: &CheckpointRecordState,
    target_namespace_id: &NamespaceId,
    lease_expires_at_ms: u64,
    context: &MutationContext,
) -> Result<ForkCheckpointReachability> {
    let head = match load_head_object(store, target_namespace_id).await {
        Ok(loaded) => loaded.state,
        Err(ControlObjectLoadError::MissingObject { .. }) => {
            return Ok(if lease_expires_at_ms <= context.now_ms {
                ForkCheckpointReachability::Reclaimable
            } else {
                ForkCheckpointReachability::Retained {
                    reason: "target_creation_in_flight",
                }
            })
        }
        Err(error) => match &error {
            ControlObjectLoadError::Store { object_key, .. } => {
                tracing::warn!(
                    namespace_id = %target_namespace_id,
                    object_key,
                    error = %error,
                    "the fork target head did not read; retaining its source checkpoint"
                );
                return Ok(ForkCheckpointReachability::Retained {
                    reason: "target_head_unreadable",
                });
            }
            _ => {
                return Err(CoreError::NamespaceCorrupt(format!(
                    "the fork target head does not load: {error}"
                )))
            }
        },
    };
    if head.status.is_deleted() {
        // A nested fork must materialize its immediate source's metadata root
        // before it can publish the descendant checkpoint. That root may name
        // metadata and content owned by earlier ancestors, and it is a direct,
        // non-swept control object rather than listing evidence. Retain this
        // source record conservatively whenever the deleted target ever
        // materialized one.
        if load_metadata_root_object_if_present(store, target_namespace_id)
            .await
            .map_err(CoreError::ControlObjectLoad)?
            .is_some()
        {
            return Ok(ForkCheckpointReachability::Retained {
                reason: "target_may_have_live_descendant",
            });
        }
        return Ok(ForkCheckpointReachability::Reclaimable);
    }
    let Some(basis) = head.fork_basis else {
        return Ok(ForkCheckpointReachability::Reclaimable);
    };
    if basis.manifest.owner_namespace_id != record.namespace_id
        || basis.source_checkpoint_id != record.checkpoint_id
    {
        return Ok(ForkCheckpointReachability::Reclaimable);
    }
    if basis.manifest != record.manifest {
        return Err(CoreError::NamespaceCorrupt(format!(
            "the fork target `{target_namespace_id}` reads through checkpoint `{}` but names a \
             different manifest reference",
            record.checkpoint_id
        )));
    }
    Ok(ForkCheckpointReachability::Retained {
        reason: "referenced_by_live_target",
    })
}
