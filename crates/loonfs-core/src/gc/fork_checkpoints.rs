//! Release rules for fork-owned and missing-basis checkpoint records.

use crate::checkpoint::record::{
    load_checkpoint_record_at_key, release_checkpoint_record, release_inspected_checkpoint_record,
    CheckpointRelease,
};
use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::control::load_head_object;
use loonfs_api::wire::control::{
    CheckpointOwner, CheckpointRecordState, CheckpointStatus, NamespaceStatus,
};
use loonfs_api::NamespaceId;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::ObjectStore;

pub(super) enum ForkCheckpointSweep {
    /// The record was flipped `active -> released` under its etag.
    Released,
    /// The record must survive this pass (its target still reaches it, an
    /// attempt may still install one, the head did not read, or the release
    /// compare-and-swap lost a race).
    Retained,
    /// Not an active fork-owned record; the normal delete path decides.
    NotAnActiveFork,
}

/// Releases an active, non-fork checkpoint whose basis manifest is still
/// missing after the grace period.
pub(super) async fn release_missing_basis_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    key: &str,
    grace_window_ms: u64,
    context: &MutationContext,
) -> Result<bool> {
    let loaded = load_checkpoint_record_at_key(store, key).await;
    let loaded = match loaded {
        Ok(loaded) => loaded,
        Err(ControlObjectLoadError::MissingObject { .. }) => return Ok(false),
        Err(error) => return Err(CoreError::ControlObjectLoad(error)),
    };
    let record = loaded.state;
    if record.status != (CheckpointStatus::Active {}) {
        return Ok(false);
    }
    // Fork checkpoints remain until their target namespace is deleted.
    if matches!(record.owner, CheckpointOwner::Fork { .. }) {
        return Ok(false);
    }
    if context.now_ms.saturating_sub(record.created_at_ms) < grace_window_ms {
        return Ok(false);
    }
    let manifest_key = metadata_manifest_object(namespace_id, &record.manifest.manifest_object_id);
    if store
        .head(&manifest_key)
        .await
        .map_err(|error| CoreError::store(&manifest_key, &error))?
        .is_some()
    {
        return Ok(false);
    }
    release_checkpoint_record(store, namespace_id, &record.checkpoint_id, context.now_ms).await?;
    Ok(true)
}

/// Decides one fork-owned sweep candidate immediately before acting
/// (rule 3): re-reads the record, re-classifies its target, and releases the
/// record by compare-and-swap on the just-observed etag. That swap is what a
/// forker's renewal contends with.
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
        ForkCheckpointReachability::ReferencedByLiveTarget
        | ForkCheckpointReachability::InFlight
        | ForkCheckpointReachability::Ambiguous => return Ok(ForkCheckpointSweep::Retained),
    }
    // Classification runs before this so a released record whose target still
    // names it exactly is retained, not handed to the released-record reaper.
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

/// Where one fork-owned record stands against its target namespace.
pub(super) enum ForkCheckpointReachability {
    /// The target head is active and its fork basis names this exact record.
    ReferencedByLiveTarget,
    /// No target head yet, and the attempt's lease has not passed.
    InFlight,
    /// No target can ever read through this record again.
    Reclaimable,
    /// The target head did not read, so this pass decides nothing.
    Ambiguous,
}

/// Classifies a fork-owned record against the durable evidence its target
/// leaves behind (rule 10's fork arm).
///
/// The target head is the only object a fork installation writes, and
/// `HeadState::ensure_successor_identity` refuses to rewrite `fork_basis`, so
/// a head naming this record is permanent proof the basis is reachable and a
/// head naming anything else is permanent proof it is not. Name existence is
/// not enough on its own: a second fork attempt against a target that already
/// exists creates a record no target will ever read through.
///
/// An absent head means no attempt has landed, and the lease decides whether
/// one still can. The forker renews the lease under this record's etag
/// immediately before installing, so a collector reading an expired lease has
/// already won the race against every attempt that could still install.
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
                ForkCheckpointReachability::InFlight
            })
        }
        Err(error) => match &error {
            // An unreadable target head is not evidence of anything; retain.
            ControlObjectLoadError::Store { object_key, .. } => {
                tracing::warn!(
                    namespace_id = %target_namespace_id,
                    object_key,
                    error = %error,
                    "the fork target head did not read; retaining its source checkpoint"
                );
                return Ok(ForkCheckpointReachability::Ambiguous);
            }
            _ => {
                return Err(CoreError::NamespaceCorrupt(format!(
                    "the fork target head does not load: {error}"
                )))
            }
        },
    };
    if head.status == (NamespaceStatus::Deleted {}) {
        return Ok(ForkCheckpointReachability::Reclaimable);
    }
    let Some(basis) = head.fork_basis else {
        return Ok(ForkCheckpointReachability::Reclaimable);
    };
    if basis.source_checkpoint_id != record.checkpoint_id {
        return Ok(ForkCheckpointReachability::Reclaimable);
    }
    if basis.manifest != record.manifest {
        return Err(CoreError::NamespaceCorrupt(format!(
            "the fork target `{target_namespace_id}` reads through checkpoint `{}` but names a \
             different manifest reference",
            record.checkpoint_id
        )));
    }
    Ok(ForkCheckpointReachability::ReferencedByLiveTarget)
}
