//! Durable manifest publication: write manifest objects idempotently and
//! advance `current_manifest_id` on the head by compare-and-swap.

use super::create::checkpoint_record_by_id;
use super::error::ManifestLoadError;
use super::load::{load_namespace_manifest_envelope, load_namespace_manifest_envelope_if_present};
use crate::error::CoreError;
use crate::error::MetadataProjectionLoadError;
use crate::namespace::control::read_head_object;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_object, ControlObjectKind, HeadState, HeadStateEnvelope,
};
use loonfs_api::wire::manifest::{encode_namespace_manifest_json, NamespaceManifestEnvelope};
use loonfs_api::{CheckpointId, ManifestId, NamespaceId};
use loonfs_objectstore::keys::namespace_manifest;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

// Mutable head CAS retries are about writer concurrency, not manifest id
// allocation. Keep this separate so errors describe the failed phase.
pub(super) const HEAD_CAS_RETRY_LIMIT: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ManifestPublicationOutcome {
    Published(Box<HeadState>),
    CurrentManifestMissingCheckpoint { current_manifest_id: ManifestId },
    HeadCasRaceLost,
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "write_namespace_manifest", key_class = "manifest_table")
)]
pub(crate) async fn write_namespace_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    manifest: &NamespaceManifestEnvelope,
) -> Result<(), MetadataProjectionLoadError> {
    let manifest_key = namespace_manifest(
        manifest.payload.namespace_id.as_str(),
        manifest.payload.manifest_id,
    );
    let manifest_bytes = encode_namespace_manifest_json(manifest).map_err(|err| {
        MetadataProjectionLoadError::ManifestLoad(ManifestLoadError::ManifestCodec {
            object_key: manifest_key.clone(),
            message: err.to_string(),
        })
    })?;
    match store
        .put_if_absent(&manifest_key, Bytes::from(manifest_bytes))
        .await
    {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => {
            let Some(existing) = load_namespace_manifest_envelope_if_present(
                store,
                &manifest.payload.namespace_id,
                manifest.payload.manifest_id,
                &manifest_key,
            )
            .await
            .map_err(MetadataProjectionLoadError::ManifestLoad)?
            else {
                return Err(MetadataProjectionLoadError::ManifestLoad(
                    ManifestLoadError::MissingManifest {
                        object_key: manifest_key,
                    },
                ));
            };
            if existing.payload_checksum == manifest.payload_checksum {
                Ok(())
            } else {
                Err(MetadataProjectionLoadError::ManifestLoad(
                    ManifestLoadError::ManifestConflict {
                        object_key: manifest_key,
                        manifest_id: manifest.payload.manifest_id,
                        expected_payload_checksum: manifest.payload_checksum.clone(),
                        actual_payload_checksum: existing.payload_checksum,
                    },
                ))
            }
        }
        Err(error) => Err(MetadataProjectionLoadError::ManifestLoad(
            ManifestLoadError::ReadManifest {
                object_key: manifest_key,
                message: error.to_string(),
            },
        )),
    }
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "publish_compacted_head", key_class = "namespace_head")
)]
pub(super) async fn publish_current_manifest_id<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_id: ManifestId,
    checkpoint_id: &CheckpointId,
    writer_version: &str,
) -> Result<ManifestPublicationOutcome, CoreError> {
    for _attempt in 0..HEAD_CAS_RETRY_LIMIT {
        let loaded_head = read_head_object(store, namespace_id)
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
            })?;
        let current_head = loaded_head.envelope.state;
        if current_head.current_manifest_id >= Some(manifest_id) {
            let current_manifest_id = current_head.current_manifest_id.ok_or_else(|| {
                CoreError::CheckpointUnavailable(format!(
                    "namespace `{}` has no published manifest",
                    namespace_id.as_str()
                ))
            })?;
            let current_manifest =
                load_namespace_manifest_envelope(store, namespace_id, current_manifest_id)
                    .await
                    .map_err(|error| {
                        CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(
                            error,
                        ))
                    })?;
            if checkpoint_record_by_id(&current_manifest, checkpoint_id).is_some() {
                return Ok(ManifestPublicationOutcome::Published(Box::new(
                    current_head,
                )));
            }
            return Ok(
                ManifestPublicationOutcome::CurrentManifestMissingCheckpoint {
                    current_manifest_id,
                },
            );
        }

        // Maintenance head update, not semantic writer publication.
        //
        // This operation does not acquire writer_epoch. It only moves the
        // manifest/checkpoint pointer forward and is linearized by the head
        // CAS. If the head changes concurrently, the caller reloads and
        // retries/rebases. Operations that publish user mutations or
        // intentionally stop writers must acquire writer_epoch first.
        let next_head = HeadState {
            namespace_id: current_head.namespace_id.clone(),
            seq: current_head.seq,
            head_commit_id: current_head.head_commit_id.clone(),
            writer_epoch: current_head.writer_epoch,
            writer_lease: current_head.writer_lease.clone(),
            next_inode_id: current_head.next_inode_id,
            name_policy: current_head.name_policy,
            current_manifest_id: Some(manifest_id),
            latest_checkpoint_id: Some(checkpoint_id.clone()),
            retention_floor_seq: current_head.retention_floor_seq,
            visible_wal_tip: current_head.visible_wal_tip.clone(),
            state: current_head.state,
        };
        match compare_and_swap_head(
            store,
            &loaded_head.object_key,
            loaded_head.metadata.etag.as_deref(),
            writer_version,
            &next_head,
        )
        .await
        {
            Ok(()) => return Ok(ManifestPublicationOutcome::Published(Box::new(next_head))),
            Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => continue,
            Err(error) => return Err(CoreError::Store(error.to_string())),
        }
    }

    Ok(ManifestPublicationOutcome::HeadCasRaceLost)
}

pub(super) async fn compare_and_swap_head<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    expected_head_etag: Option<&str>,
    writer_version: &str,
    next_head: &HeadState,
) -> Result<(), ObjectStoreError> {
    let expected_head_etag = expected_head_etag.ok_or_else(|| {
        ObjectStoreError::Transport(format!("missing head etag for `{object_key}`"))
    })?;
    let envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        writer_version,
        next_head.clone(),
    )
    .map_err(|err| ObjectStoreError::Transport(err.to_string()))?;
    let encoded = encode_control_object(&envelope)
        .map_err(|err| ObjectStoreError::Transport(err.to_string()))?;
    store
        .compare_and_swap(object_key, expected_head_etag, Bytes::from(encoded))
        .await
        .map(|_| ())
}
