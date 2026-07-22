//! Reaping: age-gated deletion of unreachable objects and the explicit
//! namespace-repair reap for abandoned installation debris.

use super::config::GcReport;
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError};
use crate::limits::GC_MIN_GRACE_WINDOW_MS;
use crate::namespace::control::ControlObjectLoadError;
use bytes::Bytes;
use loonfs_api::wire::control::{
    decode_control_object, encode_control_object, CheckpointRecordEnvelope,
    CheckpointRecordLifecycle, CheckpointRecordState, ControlObjectKind, HeadState,
    HeadStateEnvelope, NamespaceState,
};
use loonfs_api::{ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::{namespace_config, wal_head};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

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
) -> Result<CheckpointCondemn, CoreError> {
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
    let envelope = CheckpointRecordEnvelope::from_state(
        ControlObjectKind::CheckpointRecord,
        &context.writer_version,
        condemned,
    )
    .map_err(|error| {
        CoreError::Internal(format!(
            "failed to build checkpoint record envelope: {error}"
        ))
    })?;
    let bytes = encode_control_object(&envelope).map_err(|error| {
        CoreError::Internal(format!(
            "failed to encode checkpoint record object: {error}"
        ))
    })?;
    match store.compare_and_swap(key, etag, Bytes::from(bytes)).await {
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
) -> Result<AbandonedBootstrapReap, CoreError> {
    let namespace_prefix = format!("namespaces/{}/", namespace_id.as_str());
    let keys = list_prefix(store, &namespace_prefix).await?;
    if keys.is_empty() {
        return Ok(AbandonedBootstrapReap::Reaped);
    }

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
    let decoded_head = head_body.as_ref().and_then(|body| {
        decode_control_object::<HeadState>(&body.bytes, ControlObjectKind::WalHead)
            .ok()
            .map(|envelope| envelope.state)
    });
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
        for key in keys.iter().filter(|key| *key != &head_key) {
            let Some(metadata) = store
                .head(key)
                .await
                .map_err(|error| CoreError::store(key, &error))?
            else {
                continue;
            };
            let Some(last_modified_ms) = metadata.last_modified_ms else {
                return Ok(AbandonedBootstrapReap::InFlight);
            };
            if context.now_ms.saturating_sub(last_modified_ms) < GC_MIN_GRACE_WINDOW_MS {
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

        let mut condemned_head = decoded_head
            .clone()
            .unwrap_or_else(|| HeadState::initial(namespace_id.clone()));
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
    let keys = list_prefix(store, &namespace_prefix).await?;
    for key in keys.iter().filter(|key| *key != &head_key) {
        store
            .delete(key)
            .await
            .map_err(|error| CoreError::store(key, &error))?;
    }
    store
        .delete(&head_key)
        .await
        .map_err(|error| CoreError::store(&head_key, &error))?;
    Ok(AbandonedBootstrapReap::Reaped)
}

pub(super) async fn delete_if_aged<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    grace_window_ms: u64,
    context: &MutationContext,
    report: &mut GcReport,
) -> Result<bool, CoreError> {
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

pub(super) async fn list_prefix<S: ObjectStore + ?Sized>(
    store: &S,
    prefix: &str,
) -> Result<Vec<String>, CoreError> {
    store
        .list_prefix(prefix)
        .await
        .map_err(|error| CoreError::store(prefix, &error))
}

pub(super) fn manifest_object_id_of(key: &str) -> Option<ManifestObjectId> {
    let name = key.rsplit('/').next()?;
    let object_id = name.strip_suffix(".manifest.json")?;
    ManifestObjectId::parse(object_id).ok()
}

pub(super) fn load_error(error: ControlObjectLoadError) -> CoreError {
    CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
}
