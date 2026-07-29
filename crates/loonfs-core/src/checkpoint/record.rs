//! Checkpoint records stored under `checkpoints/`.
//!
//! A checkpoint record pins one metadata manifest (its "basis") so that
//! garbage collection keeps everything the manifest references. Creation is
//! write-then-verify: the record is written as `active`, then the basis is
//! checked against the live retention floor, and the record flips to
//! `released` if the check fails. Garbage collection compare-and-swaps a
//! collectable record into absorbing `condemned` before physical deletion.
//! Records never decide what readers see as latest, and they are stored as
//! standalone objects, never inside manifests.

use super::load::load_namespace_manifest_envelope;
use crate::error::{CoreError, Result};
use crate::namespace::basis::resolve_retention_floor_seq;
use crate::namespace::control::read_head_object;
use bytes::Bytes;
use loonfs_api::wire::control::{
    decode_control_object, encode_control_object, CheckpointOwner, CheckpointRecordEnvelope,
    CheckpointRecordLifecycle, CheckpointRecordState, ControlObjectKind,
};
use loonfs_api::{CheckpointId, ManifestId, ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::checkpoint_record;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use sha2::{Digest, Sha256};

/// Derives a checkpoint id by hashing the basis identity together with the
/// owner.
///
/// The same manifest and owner always produce the same id, so retrying
/// checkpoint creation writes to the same key and `put_if_absent` hands back
/// the existing record instead of creating a duplicate. Different owners
/// pinning the same manifest get different ids, so each owner's record has
/// its own independent lifecycle.
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
    CheckpointId::parse(hex).expect("derived checkpoint id should be valid")
}

pub(crate) enum CheckpointRecordWrite {
    Created,
    /// The record already exists. The caller renews it — the renew path
    /// re-reads and validates the record, so no state rides along here.
    Existing,
}

pub(crate) fn encode_checkpoint_record(
    record: &CheckpointRecordState,
    writer_version: &str,
) -> crate::error::Result<Bytes> {
    let envelope = CheckpointRecordEnvelope::from_state(
        ControlObjectKind::CheckpointRecord,
        writer_version,
        record.clone(),
    )
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

pub(crate) async fn write_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    record: &CheckpointRecordState,
    writer_version: &str,
) -> Result<CheckpointRecordWrite> {
    let encoded = encode_checkpoint_record(record, writer_version)?;
    let object_key = checkpoint_record(record.namespace_id.as_str(), record.checkpoint_id.as_str());
    match store.put_if_absent(&object_key, encoded).await {
        Ok(_) => Ok(CheckpointRecordWrite::Created),
        Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CheckpointRecordWrite::Existing),
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

/// Sets a record's lifecycle state via compare-and-swap. Returns `Ok`
/// without writing when the record already carries the target state.
pub(crate) async fn set_checkpoint_record_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    target: CheckpointRecordLifecycle,
    writer_version: &str,
) -> Result<()> {
    cas_checkpoint_record(store, namespace_id, checkpoint_id, writer_version, |next| {
        refuse_condemned_checkpoint(next)?;
        if next.state == target {
            return Ok(false);
        }
        next.state = target;
        Ok(true)
    })
    .await
}

/// Renews a record: `active` with exactly the requested expiry, last write
/// wins. Re-creating an existing checkpoint routes here, so the durable
/// expiry always matches what the latest create acknowledged — extended,
/// shortened, or cleared — and a released record is revived. `created_at_ms`
/// keeps the original creation instant. Returns `Ok` without writing when
/// the record already matches.
pub(crate) async fn renew_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    expires_at_ms: Option<u64>,
    writer_version: &str,
) -> Result<()> {
    cas_checkpoint_record(store, namespace_id, checkpoint_id, writer_version, |next| {
        refuse_condemned_checkpoint(next)?;
        if next.state == CheckpointRecordLifecycle::Active && next.expires_at_ms == expires_at_ms {
            return Ok(false);
        }
        next.state = CheckpointRecordLifecycle::Active;
        next.expires_at_ms = expires_at_ms;
        Ok(true)
    })
    .await
}

fn refuse_condemned_checkpoint(record: &CheckpointRecordState) -> Result<()> {
    if record.state == CheckpointRecordLifecycle::Condemned {
        return Err(CoreError::CheckpointStateConflict {
            checkpoint_id: record.checkpoint_id.clone(),
            state: record.state,
        });
    }
    Ok(())
}

/// The compare-and-swap loop behind the record mutators: `apply` edits the
/// loaded state and returns `false` when the record already matches, in
/// which case nothing is written.
async fn cas_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    writer_version: &str,
    apply: impl Fn(&mut CheckpointRecordState) -> Result<bool>,
) -> Result<()> {
    const STATE_CAS_ATTEMPTS: usize = 4;
    let object_key = checkpoint_record(namespace_id.as_str(), checkpoint_id.as_str());
    for _attempt in 0..STATE_CAS_ATTEMPTS {
        let Some(loaded) = read_checkpoint_record(store, namespace_id, checkpoint_id).await? else {
            return Err(CoreError::Internal(format!(
                "checkpoint record `{object_key}` disappeared during a state change"
            )));
        };
        let mut next = loaded.state;
        if !apply(&mut next)? {
            return Ok(());
        }
        let encoded = encode_checkpoint_record(&next, writer_version)?;
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
        "checkpoint record `{object_key}` state change retries exhausted"
    )))
}

/// Rewrites a fork-owned record via compare-and-swap, marking it `active`,
/// before the fork writes anything into its target namespace. A condemned
/// record is absorbing and is refused until garbage collection deletes it.
///
/// The rewrite matters for two reasons. First, it gives the record a fresh
/// provider timestamp, so a fork that is still retrying never looks
/// abandoned to garbage collection (format spec, "Garbage collection"
/// rule 10). Second, the compare-and-swap races any concurrent GC release of
/// the record, and only one side can win the etag. If the fork wins, it
/// holds a fresh active record. If GC won, the fork sees the released
/// record, re-verifies the basis, and revives the record before proceeding.
pub(crate) async fn freshen_fork_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    expected_target: &NamespaceId,
    writer_version: &str,
) -> Result<CheckpointRecordState> {
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
        refuse_condemned_checkpoint(&loaded.state)?;
        let revived = loaded.state.state == CheckpointRecordLifecycle::Released;
        let mut next = loaded.state;
        next.state = CheckpointRecordLifecycle::Active;
        let encoded = encode_checkpoint_record(&next, writer_version)?;
        let Some(etag) = loaded.etag.as_deref() else {
            return Err(CoreError::NamespaceCorrupt(format!(
                "missing etag for checkpoint record `{object_key}`"
            )));
        };
        match store.compare_and_swap(&object_key, etag, encoded).await {
            Ok(_) => {
                // We just revived a released record, so check that the
                // basis still verifies before trusting it.
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
            Err(ObjectStoreError::PreconditionFailed { .. }) => continue,
            Err(error) => return Err(CoreError::store(&object_key, &error)),
        }
    }
    Err(CoreError::Internal(format!(
        "checkpoint record `{object_key}` freshen retries exhausted"
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
