//! Reaping: age-gated deletion of unreachable objects and the explicit
//! namespace-repair reap for abandoned installation debris.

use super::config::GcReport;
use crate::checkpoint::record::encode_checkpoint_record;
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::limits::GC_MIN_GRACE_WINDOW_MS;
use crate::namespace::control::{map_control_codec_error, ControlObjectLoadError};
use bytes::Bytes;
use futures::StreamExt;
use loonfs_api::wire::control::{
    decode_control_object, encode_control_object, CheckpointRecordLifecycle, CheckpointRecordState,
    ControlObjectKind, HeadState, HeadStateEnvelope, NamespaceState,
};
use loonfs_api::{GeneratedIdValidationError, ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::{namespace_config, namespace_prefix, wal_head};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

// Decode failure may only ever bias toward retaining bytes, never toward
// deleting or fabricating them.
enum HeadObservation {
    Missing,
    Valid(HeadState),
    Unreadable(ControlObjectLoadError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AbandonedBootstrapReap {
    Reaped,
    InFlight,
    AlreadyComplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CheckpointCondemn {
    Delete,
    Retain,
}

/// Condemns one collectable checkpoint under the exact etag inspected with
/// its lifecycle and provider age. A failed CAS means the record changed and
/// is retained without retry. Already-condemned records need no age check:
/// they are absorbing crash residue from an earlier pass.
pub(super) async fn condemn_checkpoint_if_aged<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    key: &str,
    grace_window_ms: u64,
    namespace_deleted: bool,
    context: &MutationContext,
) -> Result<CheckpointCondemn> {
    let Some(body) = store
        .get_with_metadata(key)
        .await
        .map_err(|error| CoreError::store(key, &error))?
    else {
        return Ok(CheckpointCondemn::Retain);
    };
    let Ok(envelope) = decode_control_object::<CheckpointRecordState>(
        &body.bytes,
        ControlObjectKind::CheckpointRecord,
    ) else {
        return Ok(CheckpointCondemn::Retain);
    };
    let record = envelope.state;
    if record.namespace_id != *namespace_id {
        return Ok(CheckpointCondemn::Retain);
    }
    if record.state == CheckpointRecordLifecycle::Condemned {
        return Ok(CheckpointCondemn::Delete);
    }
    let expired = record
        .expires_at_ms
        .is_some_and(|expires_at_ms| expires_at_ms <= context.now_ms);
    if record.state == CheckpointRecordLifecycle::Active && !expired && !namespace_deleted {
        return Ok(CheckpointCondemn::Retain);
    }
    let Some(last_modified_ms) = body.metadata.last_modified_ms else {
        return Ok(CheckpointCondemn::Retain);
    };
    if context.now_ms.saturating_sub(last_modified_ms) < grace_window_ms {
        return Ok(CheckpointCondemn::Retain);
    }
    let Some(etag) = body.metadata.etag.as_deref() else {
        return Ok(CheckpointCondemn::Retain);
    };
    let mut condemned = record;
    condemned.state = CheckpointRecordLifecycle::Condemned;
    let bytes = encode_checkpoint_record(&condemned, &context.writer_version)?;
    match store.compare_and_swap(key, etag, bytes).await {
        Ok(_) => Ok(CheckpointCondemn::Delete),
        Err(ObjectStoreError::PreconditionFailed { .. } | ObjectStoreError::Conflict { .. }) => {
            tracing::debug!(
                namespace_id = %namespace_id,
                object_key = key,
                "checkpoint condemn lost its inspected etag; retaining"
            );
            Ok(CheckpointCondemn::Retain)
        }
        Err(error) => Err(CoreError::store(key, &error)),
    }
}

/// Reaps a non-completable namespace tree for explicit admin repair once its
/// newest object is older than [`GC_MIN_GRACE_WINDOW_MS`]. The WAL head is the
/// install gate: repair conditionally creates or swaps it to `condemned`, then
/// deletes the subtree and the condemned head last. A create/fork racing that
/// conditional write either owns the gate first (repair retains) or fails its
/// own create-if-absent with `namespace_partial`.
pub(crate) async fn reap_abandoned_bootstrap<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<AbandonedBootstrapReap> {
    let namespace_prefix = namespace_prefix(namespace_id);
    let mut initial_keys = store.list_prefix_stream(&namespace_prefix);
    let Some(first_key) = initial_keys.next().await else {
        return Ok(AbandonedBootstrapReap::Reaped);
    };
    let first_key = first_key.map_err(|error| CoreError::store(&namespace_prefix, &error))?;

    let descriptor_key = namespace_config(namespace_id.as_str());
    let head_key = wal_head(namespace_id.as_str());
    let descriptor_exists = store
        .head(&descriptor_key)
        .await
        .map_err(|error| CoreError::store(&descriptor_key, &error))?
        .is_some();
    let head_body = store
        .get_with_metadata(&head_key)
        .await
        .map_err(|error| CoreError::store(&head_key, &error))?;
    let observed_head = match &head_body {
        None => HeadObservation::Missing,
        Some(body) => {
            match decode_control_object::<HeadState>(&body.bytes, ControlObjectKind::WalHead) {
                Ok(envelope) => HeadObservation::Valid(envelope.state),
                Err(error) => {
                    HeadObservation::Unreadable(map_control_codec_error(&head_key, error))
                }
            }
        }
    };
    let decoded_head = match observed_head {
        HeadObservation::Missing => None,
        HeadObservation::Valid(state) => Some(state),
        HeadObservation::Unreadable(error) => return Err(CoreError::load_head(error)),
    };
    if let Some(head) = &decoded_head {
        if head.namespace_id != *namespace_id {
            return Ok(AbandonedBootstrapReap::InFlight);
        }
        if head.state == NamespaceState::Deleted {
            // A deleted head retires the id permanently even if the other
            // half of its tombstone pair is damaged; repair must not erase it.
            return Ok(AbandonedBootstrapReap::AlreadyComplete);
        }
        if descriptor_exists && head.state == NamespaceState::Active {
            return Ok(AbandonedBootstrapReap::AlreadyComplete);
        }
    }

    let already_condemned = decoded_head
        .as_ref()
        .is_some_and(|head| head.state == NamespaceState::Condemned);
    if !already_condemned {
        // The newest object bounds the tree's age; a missing timestamp reads
        // as "young" and blocks the reap. An already-condemned gate bypasses
        // age on retry because no installer may legally leave that state.
        if first_key != head_key
            && !old_enough_for_bootstrap_reap(store, &first_key, context).await?
        {
            return Ok(AbandonedBootstrapReap::InFlight);
        }
        while let Some(item) = initial_keys.next().await {
            let key = item.map_err(|error| CoreError::store(&namespace_prefix, &error))?;
            if key != head_key && !old_enough_for_bootstrap_reap(store, &key, context).await? {
                return Ok(AbandonedBootstrapReap::InFlight);
            }
        }
        // The head may have appeared after the prefix listing. Its body,
        // lifecycle, etag, and provider age were inspected atomically above,
        // so include that exact observation in the unchanged grace policy.
        if let Some(body) = &head_body {
            let Some(last_modified_ms) = body.metadata.last_modified_ms else {
                return Ok(AbandonedBootstrapReap::InFlight);
            };
            if context.now_ms.saturating_sub(last_modified_ms) < GC_MIN_GRACE_WINDOW_MS {
                return Ok(AbandonedBootstrapReap::InFlight);
            }
        }

        let mut condemned_head = match decoded_head.clone() {
            Some(head) => head,
            None => HeadState::initial(namespace_id.clone()),
        };
        condemned_head.state = NamespaceState::Condemned;
        let envelope = HeadStateEnvelope::from_state(
            ControlObjectKind::WalHead,
            &context.writer_version,
            condemned_head,
        )
        .map_err(|error| {
            CoreError::Internal(format!("failed to build condemned namespace head: {error}"))
        })?;
        let bytes = encode_control_object(&envelope).map_err(|error| {
            CoreError::Internal(format!(
                "failed to encode condemned namespace head: {error}"
            ))
        })?;
        let condemn = match head_body {
            Some(body) => {
                let Some(etag) = body.metadata.etag.as_deref() else {
                    return Ok(AbandonedBootstrapReap::InFlight);
                };
                store
                    .compare_and_swap(&head_key, etag, Bytes::from(bytes))
                    .await
            }
            None => store.put_if_absent(&head_key, Bytes::from(bytes)).await,
        };
        match condemn {
            Ok(_) => {}
            Err(
                ObjectStoreError::PreconditionFailed { .. } | ObjectStoreError::Conflict { .. },
            ) => {
                tracing::debug!(
                    namespace_id = %namespace_id,
                    object_key = head_key,
                    "namespace-install gate condemnation lost its inspected state; retaining"
                );
                return Ok(AbandonedBootstrapReap::InFlight);
            }
            Err(error) => return Err(CoreError::store(&head_key, &error)),
        }
    }

    // Re-list after winning the gate so pre-head debris, including an
    // immutable manifest written by the losing installer before its admission
    // attempt, is included. A loser performs no later fixed-key writes.
    let mut keys = store.list_prefix_stream(&namespace_prefix);
    while let Some(item) = keys.next().await {
        let key = item.map_err(|error| CoreError::store(&namespace_prefix, &error))?;
        if key == head_key {
            continue;
        }
        store
            .delete(&key)
            .await
            .map_err(|error| CoreError::store(&key, &error))?;
    }
    store
        .delete(&head_key)
        .await
        .map_err(|error| CoreError::store(&head_key, &error))?;
    Ok(AbandonedBootstrapReap::Reaped)
}

async fn old_enough_for_bootstrap_reap<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    context: &MutationContext,
) -> Result<bool> {
    let Some(metadata) = store
        .head(key)
        .await
        .map_err(|error| CoreError::store(key, &error))?
    else {
        return Ok(true);
    };
    let Some(last_modified_ms) = metadata.last_modified_ms else {
        return Ok(false);
    };
    Ok(context.now_ms.saturating_sub(last_modified_ms) >= GC_MIN_GRACE_WINDOW_MS)
}

pub(super) async fn delete_if_aged<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    grace_window_ms: u64,
    context: &MutationContext,
    report: &mut GcReport,
) -> Result<bool> {
    let Some(metadata) = store
        .head(key)
        .await
        .map_err(|error| CoreError::store(key, &error))?
    else {
        // Already gone; nothing to count.
        return Ok(false);
    };
    let Some(last_modified_ms) = metadata.last_modified_ms else {
        // No provider timestamp: treat as young, retain (rule 1).
        report.retained_candidates += 1;
        return Ok(false);
    };
    if context.now_ms.saturating_sub(last_modified_ms) < grace_window_ms {
        report.retained_candidates += 1;
        return Ok(false);
    }
    store
        .delete(key)
        .await
        .map_err(|error| CoreError::store(key, &error))?;
    Ok(true)
}

pub(super) fn manifest_object_id_of(
    key: &str,
) -> Option<std::result::Result<ManifestObjectId, GeneratedIdValidationError>> {
    let name = key.rsplit('/').next()?;
    let object_id = name.strip_suffix(".manifest.json")?;
    Some(ManifestObjectId::parse(object_id))
}
