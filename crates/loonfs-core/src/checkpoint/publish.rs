//! Durable manifest publication: write manifest objects idempotently and
//! advance `metadata/root.json` by monotonic compare-and-swap.

use super::error::ManifestLoadError;
use super::load::load_namespace_manifest_envelope_if_present;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::limits::CONTENTION_RETRY_LIMIT;
use crate::namespace::control::read_metadata_root_object;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_object, ControlObjectKind, MetadataRootEnvelope, MetadataRootState,
};
use loonfs_api::wire::manifest::{encode_namespace_manifest_json, NamespaceManifestEnvelope};
use loonfs_api::{ChangeSeq, ManifestId, ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ManifestPublicationOutcome {
    Published(MetadataRootState),
    /// Someone already published something at least as new; the caller's
    /// manifest stays durable and valid, the newer root simply wins.
    Superseded(MetadataRootState),
    RootCasRaceLost,
}

#[tracing::instrument(
    level = "info",
    name = "loonfs.phase",
    err,
    skip_all,
    fields(phase = "write_namespace_manifest", key_class = "manifest_table")
)]
pub(crate) async fn write_namespace_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    manifest: &NamespaceManifestEnvelope,
) -> std::result::Result<(), MetadataProjectionLoadError> {
    let manifest_key = metadata_manifest_object(
        manifest.payload.namespace_id.as_str(),
        &manifest.payload.manifest_object_id,
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
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            let Some(existing) = load_namespace_manifest_envelope_if_present(
                store,
                &manifest.payload.namespace_id,
                &manifest.payload.manifest_object_id,
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
    name = "loonfs.phase",
    err,
    skip_all,
    fields(phase = "publish_metadata_root", key_class = "metadata_root")
)]
pub(super) async fn publish_metadata_root<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest: &NamespaceManifestEnvelope,
    expected_predecessor: &ManifestObjectId,
    updated_at_ms: u64,
    writer_version: &str,
) -> Result<ManifestPublicationOutcome> {
    // Manifest publication CASes metadata/root.json, never the WAL head:
    // head watchers see only commits. Updates are monotonic in
    // manifest_head_seq; a same-seq replacement may reference a different
    // manifest (pure compaction), and a lower-seq attempt no-ops in favor of
    // whatever newer root someone else already published.
    //
    // `expected_predecessor` is the root manifest the caller read when it
    // built this successor. Head ordering alone is not enough to decide the
    // winner: every manifest carries forward the retention floor, so a candidate built
    // from a superseded basis could win on a higher head while silently
    // reverting a sibling's acknowledged publication. A root that no longer
    // names the predecessor therefore supersedes the candidate, whatever
    // the head ordering says; the caller rebases against the current root
    // and retries.
    let manifest_id = manifest.payload.manifest_id;
    let manifest_object_id = manifest.payload.manifest_object_id.clone();
    let manifest_head_seq = manifest.payload.head_seq;
    for _attempt in 0..CONTENTION_RETRY_LIMIT {
        let loaded = read_metadata_root_object(store, namespace_id)
            .await
            .map_err(CoreError::load_head)?;
        let current = &loaded.envelope.state;
        if current.manifest_id == manifest_id
            && current.manifest_object_id == manifest_object_id
            && current.manifest_payload_checksum == manifest.payload_checksum
        {
            // Idempotent re-publication: the root already names this
            // candidate (a retried call, or a racing writer of the same
            // bytes).
            return Ok(ManifestPublicationOutcome::Published(current.clone()));
        }
        if root_supersedes_candidate(current, manifest_head_seq, manifest_id) {
            return Ok(ManifestPublicationOutcome::Superseded(current.clone()));
        }
        if current.manifest_object_id != *expected_predecessor {
            return Ok(ManifestPublicationOutcome::Superseded(current.clone()));
        }

        let next = MetadataRootState {
            namespace_id: namespace_id.clone(),
            manifest_id,
            manifest_object_id: manifest_object_id.clone(),
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
                ObjectStoreError::PreconditionFailed { .. },
            ) => continue,
            Err(error) => {
                let recovered = read_metadata_root_object(store, namespace_id)
                    .await
                    .map_err(CoreError::load_head)?
                    .envelope
                    .state;
                if root_points_to_candidate(&recovered, &next) {
                    return Ok(ManifestPublicationOutcome::Published(recovered));
                }
                if root_supersedes_candidate(&recovered, manifest_head_seq, manifest_id) {
                    return Ok(ManifestPublicationOutcome::Superseded(recovered));
                }
                return Err(CoreError::store(&loaded.object_key, &error));
            }
        }
    }
    Ok(ManifestPublicationOutcome::RootCasRaceLost)
}

fn root_points_to_candidate(current: &MetadataRootState, candidate: &MetadataRootState) -> bool {
    current.manifest_id == candidate.manifest_id
        && current.manifest_object_id == candidate.manifest_object_id
        && current.manifest_head_seq == candidate.manifest_head_seq
        && current.manifest_payload_checksum == candidate.manifest_payload_checksum
}

fn root_supersedes_candidate(
    current: &MetadataRootState,
    candidate_head_seq: ChangeSeq,
    candidate_manifest_id: ManifestId,
) -> bool {
    current.manifest_head_seq > candidate_head_seq
        || (current.manifest_head_seq == candidate_head_seq
            && current.manifest_id >= candidate_manifest_id)
}
