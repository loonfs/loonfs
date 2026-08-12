//! Durable manifest publication: write manifest objects idempotently and
//! advance `metadata/root.json` by monotonic compare-and-swap.

use super::error::ManifestLoadError;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::limits::CONTENTION_RETRY_LIMIT;
use crate::namespace::control::{read_metadata_root_object, read_metadata_root_object_if_present};
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_object, ControlObjectKind, MetadataRootEnvelope, MetadataRootState,
};
use loonfs_api::wire::manifest::{encode_namespace_manifest_json, NamespaceManifestEnvelope};
use loonfs_api::{ChangeSeq, ManifestId, ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::{ImmutableWriteError, ObjectStore, ObjectStoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ManifestPublicationOutcome {
    Published(MetadataRootState),
    /// Someone already published something at least as new; the caller's
    /// manifest stays durable and valid, the newer root simply wins.
    Superseded(MetadataRootState),
    RootCasRaceLost,
}

#[tracing::instrument(
    level = "debug",
    name = "loonfs.phase",
    err(level = "warn"),
    skip_all,
    fields(phase = "write_namespace_manifest", key_class = "manifest_table")
)]
pub(crate) async fn write_namespace_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    manifest: &NamespaceManifestEnvelope,
) -> std::result::Result<(), MetadataProjectionLoadError> {
    let manifest_key = metadata_manifest_object(
        &manifest.payload.namespace_id,
        &manifest.payload.manifest_object_id,
    );
    let manifest_bytes = Bytes::from(encode_namespace_manifest_json(manifest).map_err(|err| {
        MetadataProjectionLoadError::ManifestLoad(ManifestLoadError::ManifestCodec {
            object_key: manifest_key.clone(),
            message: err.to_string(),
        })
    })?);
    // Immutable format objects use verified writes so identical bytes are an idempotent success
    // and different bytes are corruption.
    match store
        .put_immutable_verified(&manifest_key, manifest_bytes)
        .await
    {
        Ok(()) => Ok(()),
        Err(ImmutableWriteError::DifferentObject { .. }) => Err(
            MetadataProjectionLoadError::ManifestLoad(ManifestLoadError::ManifestObjectConflict {
                object_key: manifest_key,
                manifest_id: manifest.payload.manifest_id,
            }),
        ),
        Err(ImmutableWriteError::Transport { source, .. }) => Err(
            MetadataProjectionLoadError::ManifestLoad(ManifestLoadError::ReadManifest {
                object_key: manifest_key,
                message: source.message(),
            }),
        ),
        Err(error) => Err(MetadataProjectionLoadError::ManifestLoad(
            ManifestLoadError::ReadManifest {
                object_key: manifest_key,
                message: error.to_string(),
            },
        )),
    }
}

/// Maps a manifest write failure onto a core error.
///
/// Callers write each manifest under a freshly generated object id, and
/// every generated id ends in 16 random hex characters, so no other writer
/// proposes the same key. [`write_namespace_manifest`] already accepts a
/// byte-identical rewrite of a key it wrote, which covers a retried request.
/// A different payload under the key is therefore corruption rather than
/// contention, and it is reported as such.
pub(super) fn manifest_write_failure(error: MetadataProjectionLoadError) -> CoreError {
    match error {
        MetadataProjectionLoadError::ManifestLoad(
            ManifestLoadError::ManifestObjectConflict { object_key, .. }
            | ManifestLoadError::ManifestConflict { object_key, .. },
        ) => CoreError::NamespaceCorrupt(format!(
            "namespace manifest `{object_key}` already exists with a different payload"
        )),
        error => CoreError::MetadataProjection(error),
    }
}

#[tracing::instrument(
    level = "debug",
    name = "loonfs.phase",
    err(level = "warn"),
    skip_all,
    fields(phase = "publish_metadata_root", key_class = "metadata_root")
)]
pub(super) async fn publish_metadata_root<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest: &NamespaceManifestEnvelope,
    expected_predecessor: Option<ManifestObjectId>,
    updated_at_ms: u64,
) -> Result<ManifestPublicationOutcome> {
    // Manifest publication CASes metadata/root.json, never the WAL head:
    // head watchers see only commits. Updates are monotonic in
    // manifest_head_seq; a same-seq replacement may reference a different
    // manifest (pure compaction), and a lower-seq attempt no-ops in favor of
    // whatever newer root someone else already published.
    //
    // `expected_predecessor` is the root manifest the caller read when it
    // built this successor, or `None` when the caller built on a basis this
    // namespace had not published a root for — a young namespace's genesis
    // or fork basis, whose successor creates the root object. Head ordering
    // alone is not enough to decide the winner: every manifest carries
    // forward the retention floor, so a candidate built from a superseded
    // basis could win on a higher head while silently reverting a sibling's
    // acknowledged publication. A root that no longer names the predecessor
    // therefore supersedes the candidate, whatever the head ordering says;
    // the caller rebases against the current root and retries.
    let manifest_id = manifest.payload.manifest_id;
    let manifest_object_id = manifest.payload.manifest_object_id.clone();
    let manifest_head_seq = manifest.payload.head_seq;
    for _attempt in 0..CONTENTION_RETRY_LIMIT {
        let Some(loaded) = read_metadata_root_object_if_present(store, namespace_id)
            .await
            .map_err(CoreError::load_head)?
        else {
            match create_first_metadata_root(store, namespace_id, manifest, updated_at_ms).await? {
                Some(published) => return Ok(ManifestPublicationOutcome::Published(published)),
                // Another publisher created the root first; re-read and let
                // the ordinary rules decide.
                None => continue,
            }
        };
        let current = &loaded.state;
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
        match &expected_predecessor {
            Some(expected_predecessor) if current.manifest_object_id == *expected_predecessor => {}
            // Either the root moved off the basis this candidate was built
            // on, or the candidate was built on no root at all and one now
            // exists.
            _ => return Ok(ManifestPublicationOutcome::Superseded(current.clone())),
        }

        let next = MetadataRootState {
            namespace_id: namespace_id.clone(),
            manifest_id,
            manifest_object_id: manifest_object_id.clone(),
            manifest_head_seq,
            manifest_payload_checksum: manifest.payload_checksum.clone(),
            updated_at_ms,
        };
        let envelope =
            MetadataRootEnvelope::from_state(ControlObjectKind::MetadataRoot, next.clone())
                .map_err(|err| {
                    CoreError::Internal(format!("failed to build metadata root envelope: {err}"))
                })?;
        let encoded = encode_control_object(&envelope).map_err(|err| {
            CoreError::Internal(format!("failed to encode metadata root object: {err}"))
        })?;
        match store
            .compare_and_swap(&loaded.object_key, &loaded.etag, Bytes::from(encoded))
            .await
        {
            Ok(_) => return Ok(ManifestPublicationOutcome::Published(next)),
            Err(ObjectStoreError::PreconditionFailed { .. }) => continue,
            Err(error) => {
                let recovered = read_metadata_root_object(store, namespace_id)
                    .await
                    .map_err(CoreError::load_head)?
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

/// Publishes the namespace's first `metadata/root.json`.
///
/// A namespace that has never flushed has no root object to compare and
/// swap, so its first publication is a create-if-absent. Losing that race
/// returns `None`: the winner's root is then read and the ordinary
/// supersession rules apply.
async fn create_first_metadata_root<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest: &NamespaceManifestEnvelope,
    updated_at_ms: u64,
) -> Result<Option<MetadataRootState>> {
    let object_key = loonfs_objectstore::keys::metadata_root(namespace_id);
    let next = MetadataRootState {
        namespace_id: namespace_id.clone(),
        manifest_id: manifest.payload.manifest_id,
        manifest_object_id: manifest.payload.manifest_object_id.clone(),
        manifest_head_seq: manifest.payload.head_seq,
        manifest_payload_checksum: manifest.payload_checksum.clone(),
        updated_at_ms,
    };
    let envelope = MetadataRootEnvelope::from_state(ControlObjectKind::MetadataRoot, next.clone())
        .map_err(|err| {
            CoreError::Internal(format!("failed to build metadata root envelope: {err}"))
        })?;
    let encoded = encode_control_object(&envelope).map_err(|err| {
        CoreError::Internal(format!("failed to encode metadata root object: {err}"))
    })?;
    // The metadata root is mutable control state, so its first publication is a conditional create.
    match store.put_if_absent(&object_key, Bytes::from(encoded)).await {
        Ok(_) => Ok(Some(next)),
        Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(None),
        Err(error) => Err(CoreError::store(&object_key, &error)),
    }
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
