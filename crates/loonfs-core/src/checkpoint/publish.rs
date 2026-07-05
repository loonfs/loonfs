//! Durable manifest publication: write manifest objects idempotently and
//! advance `current_manifest_id` on the head by compare-and-swap.

use super::create::checkpoint_record_by_id;
use super::error::ManifestLoadError;
use super::load::{load_namespace_manifest_envelope, load_namespace_manifest_envelope_if_present};
use crate::control_update::{update_head, ControlUpdateError, HeadUpdate};
use crate::error::CoreError;
use crate::error::MetadataProjectionLoadError;
use bytes::Bytes;
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::manifest::{encode_namespace_manifest_json, NamespaceManifestEnvelope};
use loonfs_api::{CheckpointId, ManifestId, NamespaceId};
use loonfs_objectstore::keys::metadata_manifest;
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
    let manifest_key = metadata_manifest(
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
    fields(phase = "publish_compacted_head", key_class = "wal_head")
)]
pub(super) async fn publish_current_manifest_id<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_id: ManifestId,
    checkpoint_id: &CheckpointId,
    writer_version: &str,
) -> Result<ManifestPublicationOutcome, CoreError> {
    enum HeadAdvance {
        AlreadyCurrent(Box<HeadState>),
        Published(Box<HeadState>),
    }

    let advance = update_head(
        store,
        namespace_id,
        writer_version,
        HEAD_CAS_RETRY_LIMIT,
        |loaded| {
            let current_head = &loaded.envelope.state;
            if current_head.current_manifest_id >= Some(manifest_id) {
                return Ok(HeadUpdate::Noop(HeadAdvance::AlreadyCurrent(Box::new(
                    current_head.clone(),
                ))));
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
            Ok(HeadUpdate::Replace {
                next: Box::new(next_head.clone()),
                outcome: HeadAdvance::Published(Box::new(next_head)),
            })
        },
    )
    .await;

    let advance = match advance {
        Ok(advance) => advance,
        Err(ControlUpdateError::RetryExhausted { .. }) => {
            return Ok(ManifestPublicationOutcome::HeadCasRaceLost);
        }
        Err(ControlUpdateError::LoadHead(error)) => {
            return Err(CoreError::MetadataProjection(
                MetadataProjectionLoadError::LoadHead(error),
            ));
        }
        Err(other) => return Err(CoreError::Store(other.to_string())),
    };

    match advance {
        HeadAdvance::Published(next_head) => Ok(ManifestPublicationOutcome::Published(next_head)),
        HeadAdvance::AlreadyCurrent(current_head) => {
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
                return Ok(ManifestPublicationOutcome::Published(current_head));
            }
            Ok(
                ManifestPublicationOutcome::CurrentManifestMissingCheckpoint {
                    current_manifest_id,
                },
            )
        }
    }
}
