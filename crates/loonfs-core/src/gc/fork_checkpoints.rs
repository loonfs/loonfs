//! Release rules for fork-owned and missing-basis checkpoint records.

use super::config::GcConfig;
use crate::checkpoint::record::{encode_checkpoint_record, set_checkpoint_record_state};
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::namespace::control::{read_head_object, ControlObjectLoadError};
use futures::StreamExt;
use loonfs_api::wire::control::{
    decode_control_object, CheckpointOwner, CheckpointRecordLifecycle, CheckpointRecordState,
    ControlObjectKind, NamespaceState,
};
use loonfs_api::NamespaceId;
use loonfs_objectstore::keys::{metadata_manifest_object, namespace_prefix};
use loonfs_objectstore::ObjectStore;

pub(super) enum ForkCheckpointSweep {
    /// The record was flipped `active -> released` under its etag.
    Released,
    /// The record must survive this pass (young, target ambiguous, or the
    /// release compare-and-swap lost a race).
    Retained,
    /// Not an active fork-owned record; the normal delete path decides.
    NotAnActiveFork,
}

/// Releases a still-active record whose basis manifest is verifiably gone.
/// Every check runs against fresh reads at decision time: the record must
/// still be active and unexpired, older than the grace window (an in-flight
/// create is never raced), and the basis manifest must still be absent.
/// The release is the compare-and-swap the creator's own verification
/// failure would have performed; the released record then ages out through
/// the normal delete path on a later pass.
pub(super) async fn release_missing_basis_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    key: &str,
    config: &GcConfig,
    context: &MutationContext,
) -> Result<bool> {
    let Some(body) = store
        .get_with_metadata(key)
        .await
        .map_err(|error| CoreError::store(key, &error))?
    else {
        return Ok(false);
    };
    let Ok(envelope) = decode_control_object::<CheckpointRecordState>(
        &body.bytes,
        ControlObjectKind::CheckpointRecord,
    ) else {
        // Unreadable records are ambiguous; never mutate one here.
        return Ok(false);
    };
    let record = envelope.state;
    let expired = record
        .expires_at_ms
        .is_some_and(|expires_at_ms| expires_at_ms <= context.now_ms);
    if record.state != CheckpointRecordLifecycle::Active || expired {
        return Ok(false);
    }
    let Some(last_modified_ms) = body.metadata.last_modified_ms else {
        return Ok(false);
    };
    if context.now_ms.saturating_sub(last_modified_ms) < config.grace_window_ms {
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
    set_checkpoint_record_state(
        store,
        namespace_id,
        &record.checkpoint_id,
        CheckpointRecordLifecycle::Released,
        &context.writer_version,
    )
    .await?;
    Ok(true)
}

/// Decides one active fork-owned sweep candidate immediately before acting
/// (rule 3): re-reads the record, re-proves the target namespace is gone,
/// and releases the record by compare-and-swap on the just-observed etag.
/// The etag check means a concurrent fork freshen either wins the swap
/// outright or observes the release — the two can never both succeed.
pub(super) async fn maybe_release_fork_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    config: &GcConfig,
    context: &MutationContext,
) -> Result<ForkCheckpointSweep> {
    let Some(body) = store
        .get_with_metadata(key)
        .await
        .map_err(|error| CoreError::store(key, &error))?
    else {
        return Ok(ForkCheckpointSweep::NotAnActiveFork);
    };
    let Ok(envelope) = decode_control_object::<CheckpointRecordState>(
        &body.bytes,
        ControlObjectKind::CheckpointRecord,
    ) else {
        // Unreadable records are ambiguous roots; never delete one here.
        return Ok(ForkCheckpointSweep::Retained);
    };
    let record = envelope.state;
    if record.state != CheckpointRecordLifecycle::Active {
        return Ok(ForkCheckpointSweep::NotAnActiveFork);
    }
    let CheckpointOwner::Fork {
        target_namespace_id,
    } = &record.owner
    else {
        return Ok(ForkCheckpointSweep::NotAnActiveFork);
    };
    // Rule 1 applies to state changes too: GC leaves objects younger than
    // the grace window alone.
    let Some(last_modified_ms) = body.metadata.last_modified_ms else {
        return Ok(ForkCheckpointSweep::Retained);
    };
    if context.now_ms.saturating_sub(last_modified_ms) < config.grace_window_ms {
        return Ok(ForkCheckpointSweep::Retained);
    }
    if !fork_target_proven_gone(
        store,
        target_namespace_id,
        last_modified_ms,
        config,
        context,
    )
    .await?
    {
        return Ok(ForkCheckpointSweep::Retained);
    }
    let Some(etag) = body.metadata.etag.as_deref() else {
        return Ok(ForkCheckpointSweep::Retained);
    };
    let mut released = record;
    released.state = CheckpointRecordLifecycle::Released;
    let encoded = encode_checkpoint_record(&released, &context.writer_version)?;
    match store.compare_and_swap(key, etag, encoded).await {
        Ok(_) => Ok(ForkCheckpointSweep::Released),
        // A fork freshen (or another pass) won the record; retain and let a
        // later pass re-decide against the fresh state.
        Err(loonfs_objectstore::ObjectStoreError::PreconditionFailed { .. })
        | Err(loonfs_objectstore::ObjectStoreError::Conflict { .. }) => {
            Ok(ForkCheckpointSweep::Retained)
        }
        Err(error) => Err(CoreError::store(key, &error)),
    }
}

/// Proves a fork target namespace is gone (rule 10's fork arm). Either its
/// head object says the namespace is terminally deleted, or nothing exists
/// under the target prefix at all and the record has aged past the reap
/// window. The age matters because a live fork retry would have freshened
/// the record; an old record over an empty tree means the fork was
/// abandoned or its partial bootstrap was already reaped.
pub(super) async fn fork_target_proven_gone<S: ObjectStore + ?Sized>(
    store: &S,
    target_namespace_id: &NamespaceId,
    record_last_modified_ms: u64,
    config: &GcConfig,
    context: &MutationContext,
) -> Result<bool> {
    match read_head_object(store, target_namespace_id).await {
        Ok(loaded) => Ok(loaded.envelope.state.state == NamespaceState::Deleted),
        Err(ControlObjectLoadError::MissingObject { .. }) => {
            let target_prefix = namespace_prefix(target_namespace_id);
            let mut tree = store.list_prefix_stream(&target_prefix);
            if let Some(item) = tree.next().await {
                item.map_err(|error| CoreError::store(&target_prefix, &error))?;
                // Either a bootstrap is in progress or rule 10 has not
                // reaped the partial tree yet; ambiguity retains.
                return Ok(false);
            }
            Ok(context.now_ms.saturating_sub(record_last_modified_ms) >= config.reap_window_ms)
        }
        // An unreadable target head is not verifiably deleted; retain.
        Err(_) => Ok(false),
    }
}
