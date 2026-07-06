//! Durable manifest publication: write manifest objects idempotently and
//! advance `metadata/root.json` by monotonic compare-and-swap.

use super::error::ManifestLoadError;
use super::load::load_namespace_manifest_envelope_if_present;
use crate::error::CoreError;
use crate::error::MetadataProjectionLoadError;
use crate::namespace::control::read_metadata_root_object;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_object, ControlObjectKind, MetadataRootEnvelope, MetadataRootState,
};
use loonfs_api::wire::manifest::{encode_namespace_manifest_json, NamespaceManifestEnvelope};
use loonfs_api::NamespaceId;
use loonfs_objectstore::keys::metadata_manifest;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

// Keep root-CAS retry errors distinct from manifest-id allocation errors.
pub(super) const ROOT_CAS_RETRY_LIMIT: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ManifestPublicationOutcome {
    Published(MetadataRootState),
    /// Another writer already published an equal or newer root.
    Superseded(MetadataRootState),
    RootCasRaceLost,
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
        Err(ObjectStoreError::PreconditionFailed { .. } | ObjectStoreError::Conflict { .. }) => {
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
    fields(phase = "publish_metadata_root", key_class = "metadata_root")
)]
pub(super) async fn publish_metadata_root<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest: &NamespaceManifestEnvelope,
    updated_at_ms: u64,
    writer_version: &str,
) -> Result<ManifestPublicationOutcome, CoreError> {
    // Manifest publication updates metadata/root.json, not the WAL head.
    // Same-seq replacements are allowed for compaction-only manifest updates.
    let manifest_id = manifest.payload.manifest_id;
    let manifest_head_seq = manifest.payload.head_seq;
    for _attempt in 0..ROOT_CAS_RETRY_LIMIT {
        let loaded = read_metadata_root_object(store, namespace_id)
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
            })?;
        let current = &loaded.envelope.state;
        let superseded = current.manifest_head_seq > manifest_head_seq
            || (current.manifest_head_seq == manifest_head_seq
                && current.manifest_id == manifest_id
                && current.manifest_payload_checksum == manifest.payload_checksum);
        if superseded {
            return Ok(ManifestPublicationOutcome::Superseded(current.clone()));
        }

        let next = MetadataRootState {
            namespace_id: namespace_id.clone(),
            manifest_id,
            manifest_head_seq,
            manifest_payload_checksum: manifest.payload_checksum.clone(),
            updated_at_ms,
        };
        let envelope = MetadataRootEnvelope::from_state(
            ControlObjectKind::MetadataRoot,
            writer_version,
            next.clone(),
        )
        .map_err(|err| {
            CoreError::Internal(format!("failed to build metadata root envelope: {err}"))
        })?;
        let encoded = encode_control_object(&envelope).map_err(|err| {
            CoreError::Internal(format!("failed to encode metadata root object: {err}"))
        })?;
        let expected_etag = loaded.metadata.etag.as_deref().ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!("missing root etag for `{}`", loaded.object_key))
        })?;
        match store
            .compare_and_swap(&loaded.object_key, expected_etag, Bytes::from(encoded))
            .await
        {
            Ok(_) => return Ok(ManifestPublicationOutcome::Published(next)),
            Err(
                ObjectStoreError::PreconditionFailed { .. } | ObjectStoreError::Conflict { .. },
            ) => continue,
            Err(error) => return Err(CoreError::store(&loaded.object_key, &error)),
        }
    }
    Ok(ManifestPublicationOutcome::RootCasRaceLost)
}
