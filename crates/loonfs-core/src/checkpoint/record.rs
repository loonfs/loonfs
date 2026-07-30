//! Checkpoint records stored under `checkpoints/`.
//!
//! A checkpoint record pins one metadata manifest (its "basis") so that
//! garbage collection keeps everything the manifest references. Creation is
//! write-then-verify: the record is written as `active`, then the basis is
//! checked against the live retention floor, and the record flips to
//! `released` if the check fails. Release is the one state change a record
//! ever makes and it is one-way; garbage collection deletes the released
//! record outright once it has aged. Records never decide what readers see
//! as latest, and they are stored as standalone objects, never inside
//! manifests.

use super::load::load_namespace_manifest_envelope;
use crate::error::{CoreError, Result};
use crate::namespace::basis::resolve_retention_floor_seq;
use crate::namespace::control::read_head_object;
use bytes::Bytes;
use loonfs_api::wire::control::{
    decode_control_object, encode_control_object, CheckpointRecordEnvelope,
    CheckpointRecordLifecycle, CheckpointRecordState, ControlObjectKind,
};
use loonfs_api::{CheckpointId, NamespaceId};
use loonfs_objectstore::keys::checkpoint_record;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

pub(crate) fn encode_checkpoint_record(
    record: &CheckpointRecordState,
) -> crate::error::Result<Bytes> {
    let envelope =
        CheckpointRecordEnvelope::from_state(ControlObjectKind::CheckpointRecord, record.clone())
            .map_err(|error| {
            CoreError::Internal(format!(
                "failed to build checkpoint record envelope: {error}"
            ))
        })?;
    encode_control_object(&envelope)
        .map(Bytes::from)
        .map_err(|error| {
            CoreError::Internal(format!(
                "failed to encode checkpoint record object: {error}"
            ))
        })
}

/// Writes a record under its freshly generated id with create-if-absent.
///
/// The id comes from [`CheckpointId::generate`], so the key is this pin's
/// alone: an occupied key means two generated ids collided, which is a
/// broken generator rather than a lifecycle a caller could resolve.
pub(crate) async fn write_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    record: &CheckpointRecordState,
) -> Result<()> {
    let encoded = encode_checkpoint_record(record)?;
    let object_key = checkpoint_record(record.namespace_id.as_str(), record.checkpoint_id.as_str());
    match store.put_if_absent(&object_key, encoded).await {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::PreconditionFailed { .. }) => Err(CoreError::Internal(format!(
            "generated checkpoint id collided with the existing record `{object_key}`"
        ))),
        Err(error) => Err(CoreError::store(&object_key, &error)),
    }
}

pub(crate) struct LoadedCheckpointRecord {
    pub(crate) etag: Option<String>,
    pub(crate) state: CheckpointRecordState,
}

pub(crate) async fn read_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
) -> Result<Option<LoadedCheckpointRecord>> {
    let object_key = checkpoint_record(namespace_id.as_str(), checkpoint_id.as_str());
    let Some(body) = store
        .get_with_metadata(&object_key)
        .await
        .map_err(|error| CoreError::store(&object_key, &error))?
    else {
        return Ok(None);
    };
    let envelope: CheckpointRecordEnvelope =
        decode_control_object(&body.bytes, ControlObjectKind::CheckpointRecord)
            .map_err(|err| CoreError::NamespaceCorrupt(format!("`{object_key}`: {err}")))?;
    if envelope.state.namespace_id != *namespace_id {
        return Err(CoreError::NamespaceCorrupt(format!(
            "checkpoint record `{object_key}` names namespace `{}`",
            envelope.state.namespace_id
        )));
    }
    Ok(Some(LoadedCheckpointRecord {
        etag: body.metadata.etag,
        state: envelope.state,
    }))
}

/// Moves a record `active -> released` by compare-and-swap, stamping
/// `released_at_ms`.
///
/// This is the only state change a checkpoint record ever makes, and it is
/// one-way, so every caller converges: the owner asking for it and garbage
/// collection acting on a passed expiry reach the same end state, and a
/// record that is already released is left exactly as the winner wrote it.
pub(crate) async fn release_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    released_at_ms: u64,
) -> Result<()> {
    const RELEASE_CAS_ATTEMPTS: usize = 4;
    let object_key = checkpoint_record(namespace_id.as_str(), checkpoint_id.as_str());
    for _attempt in 0..RELEASE_CAS_ATTEMPTS {
        let Some(loaded) = read_checkpoint_record(store, namespace_id, checkpoint_id).await? else {
            // Reaped underneath us: no active pin under this id is exactly
            // what the release asked for.
            return Ok(());
        };
        if matches!(
            loaded.state.state,
            CheckpointRecordLifecycle::Released { .. }
        ) {
            return Ok(());
        }
        let mut next = loaded.state;
        next.state = CheckpointRecordLifecycle::Released { released_at_ms };
        let encoded = encode_checkpoint_record(&next)?;
        let Some(etag) = loaded.etag.as_deref() else {
            return Err(CoreError::NamespaceCorrupt(format!(
                "missing etag for checkpoint record `{object_key}`"
            )));
        };
        match store.compare_and_swap(&object_key, etag, encoded).await {
            Ok(_) => return Ok(()),
            Err(ObjectStoreError::PreconditionFailed { .. }) => continue,
            Err(error) => return Err(CoreError::store(&object_key, &error)),
        }
    }
    Err(CoreError::Internal(format!(
        "checkpoint record `{object_key}` release retries exhausted"
    )))
}

/// Checks that a record's basis is still intact: the retention floor has
/// not passed it, and the basis manifest still loads with the expected
/// checksum.
///
/// Creation calls this after the record is durable. Without the re-check, a
/// record written just as garbage collection decides to trim the same
/// manifest could pin state that is already gone.
pub(crate) async fn verify_checkpoint_basis<S: ObjectStore + ?Sized>(
    store: &S,
    record: &CheckpointRecordState,
) -> Result<bool> {
    let head = read_head_object(store, &record.namespace_id)
        .await
        .map_err(CoreError::load_head)?
        .envelope
        .state;
    let floor_seq = resolve_retention_floor_seq(store, &head)
        .await
        .map_err(CoreError::load_head)?;
    if floor_seq > record.manifest_head_seq {
        return Ok(false);
    }
    let manifest = match load_namespace_manifest_envelope(
        store,
        &record.namespace_id,
        &record.manifest_object_id,
    )
    .await
    {
        Ok(manifest) => manifest,
        Err(_) => return Ok(false),
    };
    Ok(manifest.payload_checksum == record.manifest_payload_checksum)
}
