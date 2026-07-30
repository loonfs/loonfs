//! Reaping: age-gated deletion of unreachable objects.
//!
//! Immutable families age by provider timestamp — nothing else records when
//! they were written. Checkpoint records do not: their lifecycle instants
//! (`created_at_ms`, `expires_at_ms`, `released_at_ms`) live in the record,
//! so no checkpoint state transition depends on object metadata.

use crate::checkpoint::record::encode_checkpoint_record;
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use loonfs_api::v0::GcResponse;
use loonfs_api::wire::control::{
    decode_control_object, CheckpointOwner, CheckpointRecordLifecycle, CheckpointRecordState,
    ControlObjectKind,
};
use loonfs_api::{GeneratedIdValidationError, ManifestObjectId, NamespaceId};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CheckpointSweep {
    /// The record is released and its release has aged past the grace
    /// window; the key may be deleted.
    Delete,
    /// This pass flipped the record `active -> released`.
    Released,
    Retain,
}

/// True once a record's own lease has passed. Checkpoint aging reads the
/// record, never the object's provider timestamp: the record carries every
/// instant its lifecycle depends on.
pub(super) fn lease_expired(record: &CheckpointRecordState, now_ms: u64) -> bool {
    record
        .expires_at_ms
        .is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
}

/// Advances one collectable checkpoint record along the only path it has.
///
/// An active record whose lease has passed — or one on a terminally deleted
/// namespace, where nothing can read it again — is released by
/// compare-and-swap on the exact etag inspected, stamping the release
/// instant. A released record is deletable once that stamp is a grace window
/// old. Nothing here reads a provider timestamp: `released_at_ms` and
/// `created_at_ms` are what the record's own age is measured from, and
/// `expires_at_ms` is what its lease is measured from.
///
/// A failed CAS means the record changed and is retained without retry.
pub(super) async fn sweep_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    key: &str,
    grace_window_ms: u64,
    namespace_deleted: bool,
    context: &MutationContext,
) -> Result<CheckpointSweep> {
    let Some(body) = store
        .get_with_metadata(key)
        .await
        .map_err(|error| CoreError::store(key, &error))?
    else {
        return Ok(CheckpointSweep::Retain);
    };
    let Ok(envelope) = decode_control_object::<CheckpointRecordState>(
        &body.bytes,
        ControlObjectKind::CheckpointRecord,
    ) else {
        return Ok(CheckpointSweep::Retain);
    };
    let record = envelope.state;
    if record.namespace_id != *namespace_id {
        return Ok(CheckpointSweep::Retain);
    }
    if let CheckpointRecordLifecycle::Released { released_at_ms } = record.state {
        let aged = context.now_ms.saturating_sub(released_at_ms) >= grace_window_ms;
        return Ok(if aged {
            CheckpointSweep::Delete
        } else {
            CheckpointSweep::Retain
        });
    }
    // A fork pin is never released here, whatever its lease says: only the
    // fork arm knows whether the target is still reading through it
    // (`fork_checkpoints.rs`), and it has already had its say by this point.
    if matches!(record.owner, CheckpointOwner::Fork { .. }) {
        return Ok(CheckpointSweep::Retain);
    }
    // An unexpired pin on a live namespace is exactly what a checkpoint is
    // for. On a tombstone every pin is dead weight, but a create still in
    // flight must not be raced, so that arm waits out the grace window from
    // the record's own creation stamp.
    let releasable = lease_expired(&record, context.now_ms)
        || (namespace_deleted
            && context.now_ms.saturating_sub(record.created_at_ms) >= grace_window_ms);
    if !releasable {
        return Ok(CheckpointSweep::Retain);
    }
    let Some(etag) = body.metadata.etag.as_deref() else {
        return Ok(CheckpointSweep::Retain);
    };
    let mut released = record;
    released.state = CheckpointRecordLifecycle::Released {
        released_at_ms: context.now_ms,
    };
    let bytes = encode_checkpoint_record(&released, &context.writer_version)?;
    match store.compare_and_swap(key, etag, bytes).await {
        Ok(_) => Ok(CheckpointSweep::Released),
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            tracing::debug!(
                namespace_id = %namespace_id,
                object_key = key,
                "checkpoint release lost its inspected etag; retaining"
            );
            Ok(CheckpointSweep::Retain)
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
