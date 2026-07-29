//! Reaping: age-gated deletion of unreachable objects.

use crate::checkpoint::record::encode_checkpoint_record;
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use loonfs_api::v0::GcResponse;
use loonfs_api::wire::control::{
    decode_control_object, CheckpointRecordLifecycle, CheckpointRecordState, ControlObjectKind,
};
use loonfs_api::{GeneratedIdValidationError, ManifestObjectId, NamespaceId};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

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
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
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

pub(super) async fn delete_if_aged<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    grace_window_ms: u64,
    context: &MutationContext,
    report: &mut GcResponse,
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
