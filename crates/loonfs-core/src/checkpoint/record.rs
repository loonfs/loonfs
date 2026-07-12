//! First-class checkpoint records under `checkpoints/`.
//!
//! A checkpoint is a durable stable-view pin to a metadata manifest,
//! created write-then-verify: the record is written `active`, then the
//! basis is re-verified against the live floor; a failed verification
//! flips the record to `released`. Records never define latest visibility
//! and never live inside manifests.

use super::load::load_namespace_manifest_envelope;
use crate::error::CoreError;
use crate::error::MetadataProjectionLoadError;
use crate::namespace::control::read_wal_floor_seq_or_zero;
use bytes::Bytes;
use loonfs_api::wire::control::{
    decode_control_object, encode_control_object, CheckpointOwner, CheckpointRecordEnvelope,
    CheckpointRecordLifecycle, CheckpointRecordState, ControlObjectKind,
};
use loonfs_api::{CheckpointId, ManifestId, ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::checkpoint_record;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use sha2::{Digest, Sha256};

/// Repeated checkpoint creation for the same pinned manifest and owner must
/// return the existing record instead of stacking duplicates, without
/// listing the collection. Deriving the id from the basis identity plus the
/// owner identity makes the record naturally idempotent under
/// `put_if_absent`, while distinct owners of one basis hold distinct records
/// with independent lifecycles.
pub(crate) fn deterministic_checkpoint_id(
    namespace_id: &NamespaceId,
    manifest_id: ManifestId,
    manifest_object_id: &ManifestObjectId,
    manifest_payload_checksum: &str,
    owner: &CheckpointOwner,
) -> CheckpointId {
    let mut hasher = Sha256::new();
    hasher.update(b"loonfs.checkpoint.basis.v1\0");
    hasher.update(namespace_id.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(manifest_id.0.to_be_bytes());
    hasher.update(b"\0");
    hasher.update(manifest_object_id.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(manifest_payload_checksum.as_bytes());
    hasher.update(b"\0");
    match owner {
        CheckpointOwner::User { name } => {
            hasher.update(b"user\0");
            hasher.update(name.as_bytes());
        }
        CheckpointOwner::Fork {
            target_namespace_id,
        } => {
            hasher.update(b"fork\0");
            hasher.update(target_namespace_id.as_str().as_bytes());
        }
    }
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(36);
    hex.push_str("chk_");
    for byte in &digest[..16] {
        hex.push_str(&format!("{byte:02x}"));
    }
    CheckpointId::parse(hex).expect("derived checkpoint id is valid")
}

pub(crate) enum CheckpointRecordWrite {
    Created,
    Existing(Box<CheckpointRecordState>),
}

pub(crate) async fn write_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    record: &CheckpointRecordState,
    writer_version: &str,
) -> Result<CheckpointRecordWrite, CoreError> {
    let envelope = CheckpointRecordEnvelope::from_state(
        ControlObjectKind::CheckpointRecord,
        writer_version,
        record.clone(),
    )
    .map_err(|err| {
        CoreError::Internal(format!("failed to build checkpoint record envelope: {err}"))
    })?;
    let encoded = encode_control_object(&envelope).map_err(|err| {
        CoreError::Internal(format!("failed to encode checkpoint record object: {err}"))
    })?;
    let object_key = checkpoint_record(record.namespace_id.as_str(), record.checkpoint_id.as_str());
    match store.put_if_absent(&object_key, Bytes::from(encoded)).await {
        Ok(_) => Ok(CheckpointRecordWrite::Created),
        Err(ObjectStoreError::PreconditionFailed { .. } | ObjectStoreError::Conflict { .. }) => {
            let existing =
                read_checkpoint_record(store, &record.namespace_id, &record.checkpoint_id)
                    .await?
                    .ok_or_else(|| {
                        CoreError::Internal(format!(
                            "checkpoint record `{object_key}` conflicted but cannot be read back"
                        ))
                    })?;
            Ok(CheckpointRecordWrite::Existing(Box::new(existing.state)))
        }
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
) -> Result<Option<LoadedCheckpointRecord>, CoreError> {
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

/// Flips a record's lifecycle by compare-and-swap; idempotent when the
/// record already carries the target state.
pub(crate) async fn set_checkpoint_record_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    target: CheckpointRecordLifecycle,
    writer_version: &str,
) -> Result<(), CoreError> {
    const STATE_CAS_ATTEMPTS: usize = 4;
    let object_key = checkpoint_record(namespace_id.as_str(), checkpoint_id.as_str());
    for _attempt in 0..STATE_CAS_ATTEMPTS {
        let Some(loaded) = read_checkpoint_record(store, namespace_id, checkpoint_id).await? else {
            return Err(CoreError::Internal(format!(
                "checkpoint record `{object_key}` disappeared during a state change"
            )));
        };
        if loaded.state.state == target {
            return Ok(());
        }
        let mut next = loaded.state;
        next.state = target;
        let envelope = CheckpointRecordEnvelope::from_state(
            ControlObjectKind::CheckpointRecord,
            writer_version,
            next,
        )
        .map_err(|err| {
            CoreError::Internal(format!("failed to build checkpoint record envelope: {err}"))
        })?;
        let encoded = encode_control_object(&envelope).map_err(|err| {
            CoreError::Internal(format!("failed to encode checkpoint record object: {err}"))
        })?;
        let Some(etag) = loaded.etag.as_deref() else {
            return Err(CoreError::NamespaceCorrupt(format!(
                "missing etag for checkpoint record `{object_key}`"
            )));
        };
        match store
            .compare_and_swap(&object_key, etag, Bytes::from(encoded))
            .await
        {
            Ok(_) => return Ok(()),
            Err(
                ObjectStoreError::PreconditionFailed { .. } | ObjectStoreError::Conflict { .. },
            ) => continue,
            Err(error) => return Err(CoreError::store(&object_key, &error)),
        }
    }
    Err(CoreError::Internal(format!(
        "checkpoint record `{object_key}` state change retries exhausted"
    )))
}

/// Re-stamps a fork-owned record by compare-and-swap before its fork writes
/// any target object.
///
/// The rewrite does two jobs at once. It refreshes the record's provider
/// timestamp, so the abandoned-fork age rule ("Garbage collection", rule 9)
/// cannot fire under a live retry. And it serializes the fork against a
/// concurrent GC release on the record's etag: whichever compare-and-swap
/// lands second fails, so the fork either owns a fresh active record or
/// observes the release and revives it here — re-verifying the basis —
/// before proceeding.
pub(crate) async fn freshen_fork_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    expected_target: &NamespaceId,
    writer_version: &str,
) -> Result<CheckpointRecordState, CoreError> {
    const FRESHEN_CAS_ATTEMPTS: usize = 4;
    let object_key = checkpoint_record(namespace_id.as_str(), checkpoint_id.as_str());
    for _attempt in 0..FRESHEN_CAS_ATTEMPTS {
        let Some(loaded) = read_checkpoint_record(store, namespace_id, checkpoint_id).await? else {
            return Err(CoreError::CheckpointUnavailable(format!(
                "fork checkpoint `{checkpoint_id}` disappeared before the fork could freshen it"
            )));
        };
        match &loaded.state.owner {
            CheckpointOwner::Fork {
                target_namespace_id,
            } if target_namespace_id == expected_target => {}
            other => {
                return Err(CoreError::NamespaceCorrupt(format!(
                    "checkpoint record `{object_key}` carries owner {other:?}, not the fork \
                     target `{expected_target}`"
                )));
            }
        }
        let revived = loaded.state.state == CheckpointRecordLifecycle::Released;
        let mut next = loaded.state;
        next.state = CheckpointRecordLifecycle::Active;
        let envelope = CheckpointRecordEnvelope::from_state(
            ControlObjectKind::CheckpointRecord,
            writer_version,
            next.clone(),
        )
        .map_err(|err| {
            CoreError::Internal(format!("failed to build checkpoint record envelope: {err}"))
        })?;
        let encoded = encode_control_object(&envelope).map_err(|err| {
            CoreError::Internal(format!("failed to encode checkpoint record object: {err}"))
        })?;
        let Some(etag) = loaded.etag.as_deref() else {
            return Err(CoreError::NamespaceCorrupt(format!(
                "missing etag for checkpoint record `{object_key}`"
            )));
        };
        match store
            .compare_and_swap(&object_key, etag, Bytes::from(encoded))
            .await
        {
            Ok(_) => {
                // A revival raced a release; the record rooted its basis the
                // whole time it existed, but re-verify before trusting it.
                if revived && !verify_checkpoint_basis(store, &next).await? {
                    set_checkpoint_record_state(
                        store,
                        namespace_id,
                        checkpoint_id,
                        CheckpointRecordLifecycle::Released,
                        writer_version,
                    )
                    .await?;
                    return Err(CoreError::CheckpointUnavailable(format!(
                        "fork checkpoint `{checkpoint_id}` failed re-verification after a \
                         release race"
                    )));
                }
                return Ok(next);
            }
            Err(
                ObjectStoreError::PreconditionFailed { .. } | ObjectStoreError::Conflict { .. },
            ) => continue,
            Err(error) => return Err(CoreError::store(&object_key, &error)),
        }
    }
    Err(CoreError::Internal(format!(
        "checkpoint record `{object_key}` freshen retries exhausted"
    )))
}

/// The post-write verification that closes the create-vs-collect race:
/// after the record is durable, the basis must still be at or above the
/// live floor and the basis manifest must still load and validate.
pub(crate) async fn verify_checkpoint_basis<S: ObjectStore + ?Sized>(
    store: &S,
    record: &CheckpointRecordState,
) -> Result<bool, CoreError> {
    let floor_seq = read_wal_floor_seq_or_zero(store, &record.namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
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
