//! Release rules for fork-owned and missing-basis checkpoint records.

use super::reap::lease_expired;
use crate::checkpoint::record::{
    encode_checkpoint_record, load_checkpoint_record_at_key, release_checkpoint_record,
};
use crate::context::MutationContext;
use crate::control_object::{core_control_load_error, ControlObjectLoadError};
use crate::error::{CoreError, Result};
use crate::namespace::control::read_head_object;
use loonfs_api::wire::control::{
    CheckpointOwner, CheckpointRecordLifecycle, CheckpointRecordState, NamespaceState,
};
use loonfs_api::NamespaceId;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::ObjectStore;

pub(super) enum ForkCheckpointSweep {
    /// The record was flipped `active -> released` under its etag.
    Released,
    /// The record must survive this pass (target ambiguous or still live, or
    /// the release compare-and-swap lost a race).
    Retained,
    /// Not an active fork-owned record; the normal delete path decides.
    NotAnActiveFork,
}

/// Releases a still-active record whose basis manifest is verifiably gone.
/// Every check runs against fresh reads at decision time: the record must
/// still be active, older than the grace window by its own `created_at_ms`
/// (an in-flight create is never raced), and the basis manifest must still
/// be absent. The release is the compare-and-swap the creator's own
/// verification failure would have performed; the released record then ages
/// out through the normal delete path on a later pass.
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
        Err(error) => return Err(core_control_load_error(error)),
    };
    let record = loaded.state;
    if record.state != (CheckpointRecordLifecycle::Active {}) {
        return Ok(false);
    }
    if context.now_ms.saturating_sub(record.created_at_ms) < grace_window_ms {
        return Ok(false);
    }
    let manifest_key = metadata_manifest_object(namespace_id.as_str(), &record.manifest_object_id);
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

/// Decides one active fork-owned sweep candidate immediately before acting
/// (rule 3): re-reads the record, re-proves the target namespace is gone,
/// and releases the record by compare-and-swap on the just-observed etag.
/// The etag check means one pass releases and any other observes it; the
/// forking side is serialized by its own lease, not by this swap.
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
        Err(error) => return Err(core_control_load_error(error)),
    };
    let record = loaded.state;
    if record.state != (CheckpointRecordLifecycle::Active {}) {
        return Ok(ForkCheckpointSweep::NotAnActiveFork);
    }
    let CheckpointOwner::Fork {
        target_namespace_id,
    } = &record.owner
    else {
        return Ok(ForkCheckpointSweep::NotAnActiveFork);
    };
    if !fork_target_proven_gone(store, target_namespace_id, &record, context).await? {
        return Ok(ForkCheckpointSweep::Retained);
    }
    let mut released = record;
    released.state = CheckpointRecordLifecycle::Released {
        released_at_ms: context.now_ms,
    };
    let encoded = encode_checkpoint_record(&released)?;
    match store.compare_and_swap(key, &loaded.etag, encoded).await {
        Ok(_) => Ok(ForkCheckpointSweep::Released),
        // Another pass won the record; retain and let a later pass re-decide
        // against the fresh state.
        Err(loonfs_objectstore::ObjectStoreError::PreconditionFailed { .. }) => {
            Ok(ForkCheckpointSweep::Retained)
        }
        Err(error) => Err(CoreError::store(key, &error)),
    }
}

/// Proves a fork target namespace is gone (rule 10's fork arm). Either its
/// head says the namespace is terminally deleted, or the head is absent —
/// and since the head is the target's only installation write, an absent
/// head means the fork never landed.
///
/// The absent case waits for the record's lease. A fork in flight right now
/// has not written its head yet, and its lease covers the whole attempt with
/// margin to spare, so only an attempt that is really gone can have let the
/// lease pass. Other objects under the target prefix prove nothing either
/// way, because no installation writes any before the head.
///
/// A live target is the other half of the rule, and it is unconditional: a
/// target head that exists and is not deleted keeps the record whatever the
/// lease says. That is what protects a fork that published just before its
/// lease ran out, and it is why nothing has to clear the lease afterwards.
pub(super) async fn fork_target_proven_gone<S: ObjectStore + ?Sized>(
    store: &S,
    target_namespace_id: &NamespaceId,
    record: &CheckpointRecordState,
    context: &MutationContext,
) -> Result<bool> {
    match read_head_object(store, target_namespace_id).await {
        Ok(loaded) => Ok(loaded.state.state == NamespaceState::Deleted),
        Err(ControlObjectLoadError::MissingObject { .. }) => {
            Ok(lease_expired(record, context.now_ms))
        }
        Err(error) => match &error {
            // An unreadable target head is not verifiably deleted; retain.
            ControlObjectLoadError::Store { object_key, .. } => {
                tracing::warn!(
                    namespace_id = %target_namespace_id,
                    object_key,
                    error = %error,
                    "the fork target head did not read; retaining its source checkpoint"
                );
                Ok(false)
            }
            _ => Err(CoreError::NamespaceCorrupt(format!(
                "the fork target head does not load: {error}"
            ))),
        },
    }
}
