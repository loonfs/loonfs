//! Durable manifest publication: write manifest objects idempotently and
//! advance `metadata/root.json` by monotonic compare-and-swap.

use super::error::ManifestLoadError;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::control::load_metadata_root_object_if_present;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_state, ControlObjectKind, ManifestRef, MetadataRootState,
};
use loonfs_api::wire::manifest::{encode_namespace_manifest_json, NamespaceManifestEnvelope};
use loonfs_api::{ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::{metadata_manifest_object, metadata_root};
use loonfs_objectstore::{ImmutableWriteError, ObjectStore, ObjectStoreError};

/// The result of trying to publish a manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ManifestPublicationOutcome {
    /// The candidate is the current root.
    Published(MetadataRootState),
    /// The current root already covers the candidate's state.
    CoveredByCurrent(MetadataRootState),
    /// The expected predecessor changed without covering the candidate.
    PredecessorChanged(MetadataRootState),
    /// The root changed during the compare-and-swap and should be retried.
    RootCasRaceLost,
}

#[tracing::instrument(
    level = "debug",
    name = "loonfs.phase",
    err(level = "warn"),
    skip_all,
    fields(phase = "write_namespace_manifest", key_class = "namespace_manifest")
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
        Ok(_) => Ok(()),
        Err(ImmutableWriteError::DifferentObject { .. }) => Err(
            MetadataProjectionLoadError::ManifestLoad(ManifestLoadError::ManifestObjectConflict {
                object_key: manifest_key,
                manifest_no: manifest.payload.manifest_no,
            }),
        ),
        Err(ImmutableWriteError::Transport { source, .. }) => Err(
            MetadataProjectionLoadError::ManifestLoad(ManifestLoadError::ReadManifest {
                object_key: manifest_key,
                message: source.public_message().into_owned(),
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
    fields(phase = "publish_metadata_root", key_class = "namespace_manifest")
)]
pub(super) async fn publish_metadata_root<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest: &NamespaceManifestEnvelope,
    expected_predecessor: Option<ManifestObjectId>,
    updated_at_ms: u64,
) -> Result<ManifestPublicationOutcome> {
    // Manifest publication updates the metadata root, not the WAL head. A
    // candidate may replace only the predecessor it was built from.
    let candidate = manifest_ref_for(namespace_id, manifest);
    let Some(loaded) = load_metadata_root_object_if_present(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?
    else {
        return create_first_metadata_root(
            store,
            namespace_id,
            &candidate,
            expected_predecessor.as_ref(),
            updated_at_ms,
        )
        .await;
    };
    match root_transition(&loaded.state, &candidate, expected_predecessor.as_ref()) {
        RootTransition::InstallAgainstCurrent => {}
        transition => return Ok(outcome_from(loaded.state, transition)),
    }
    ensure_legal_successor(namespace_id, &candidate, &loaded.state.manifest)?;
    let next = MetadataRootState {
        namespace_id: namespace_id.clone(),
        manifest: candidate.clone(),
        updated_at_ms,
    };
    let encoded =
        encode_control_state(ControlObjectKind::MetadataRoot, &next).map_err(|error| {
            CoreError::Codec {
                object_key: loaded.object_key.clone(),
                message: error.to_string(),
            }
        })?;
    match store
        .compare_and_swap(&loaded.object_key, &loaded.etag, Bytes::from(encoded))
        .await
    {
        Ok(_) => Ok(ManifestPublicationOutcome::Published(next)),
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            classify_current_root(
                store,
                namespace_id,
                &candidate,
                expected_predecessor.as_ref(),
            )
            .await
        }
        // Re-read the root to resolve an unknown write outcome.
        Err(error @ ObjectStoreError::Transport { .. }) => {
            match classify_current_root(
                store,
                namespace_id,
                &candidate,
                expected_predecessor.as_ref(),
            )
            .await?
            {
                ManifestPublicationOutcome::RootCasRaceLost => {
                    Err(CoreError::store(&loaded.object_key, &error))
                }
                outcome => Ok(outcome),
            }
        }
        Err(error) => Err(CoreError::store(&loaded.object_key, &error)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootTransition {
    InstallAgainstCurrent,
    Published,
    CoveredByCurrent,
    PredecessorChanged,
}

/// A candidate may replace only the predecessor it was built from.
fn root_transition(
    root: &MetadataRootState,
    candidate: &ManifestRef,
    expected_predecessor: Option<&ManifestObjectId>,
) -> RootTransition {
    if root.manifest == *candidate {
        return RootTransition::Published;
    }
    if root_supersedes_candidate(root, candidate) {
        return RootTransition::CoveredByCurrent;
    }
    match expected_predecessor {
        Some(predecessor) if root.manifest.manifest_object_id == *predecessor => {
            RootTransition::InstallAgainstCurrent
        }
        _ => RootTransition::PredecessorChanged,
    }
}

fn outcome_from(root: MetadataRootState, transition: RootTransition) -> ManifestPublicationOutcome {
    match transition {
        RootTransition::InstallAgainstCurrent => ManifestPublicationOutcome::RootCasRaceLost,
        RootTransition::Published => ManifestPublicationOutcome::Published(root),
        RootTransition::CoveredByCurrent => ManifestPublicationOutcome::CoveredByCurrent(root),
        RootTransition::PredecessorChanged => ManifestPublicationOutcome::PredecessorChanged(root),
    }
}

/// Re-reads the root after a lost or unconfirmed compare-and-swap.
async fn classify_current_root<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidate: &ManifestRef,
    expected_predecessor: Option<&ManifestObjectId>,
) -> Result<ManifestPublicationOutcome> {
    let Some(loaded) = load_metadata_root_object_if_present(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?
    else {
        return Ok(ManifestPublicationOutcome::RootCasRaceLost);
    };
    let transition = root_transition(&loaded.state, candidate, expected_predecessor);
    Ok(outcome_from(loaded.state, transition))
}

/// Rejects a candidate that does not advance its predecessor.
fn ensure_legal_successor(
    namespace_id: &NamespaceId,
    candidate: &ManifestRef,
    predecessor: &ManifestRef,
) -> Result<()> {
    if candidate.owner_namespace_id == *namespace_id
        && candidate.manifest_no > predecessor.manifest_no
        && candidate.manifest_head_seq >= predecessor.manifest_head_seq
    {
        return Ok(());
    }
    Err(CoreError::Internal(format!(
        "manifest `{}` is not a legal successor of `{}` in namespace `{namespace_id}`",
        candidate.manifest_object_id, predecessor.manifest_object_id
    )))
}

/// Publishes the namespace's first `metadata/root.json`.
///
/// A confirmed conflict and an ambiguous transport result are both resolved
/// by reading the authoritative root and applying the same classification as
/// the later CAS path.
async fn create_first_metadata_root<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidate: &ManifestRef,
    expected_predecessor: Option<&ManifestObjectId>,
    updated_at_ms: u64,
) -> Result<ManifestPublicationOutcome> {
    let object_key = metadata_root(namespace_id);
    let next = MetadataRootState {
        namespace_id: namespace_id.clone(),
        manifest: candidate.clone(),
        updated_at_ms,
    };
    let encoded =
        encode_control_state(ControlObjectKind::MetadataRoot, &next).map_err(|error| {
            CoreError::Codec {
                object_key: object_key.clone(),
                message: error.to_string(),
            }
        })?;
    match store.put_if_absent(&object_key, Bytes::from(encoded)).await {
        Ok(_) => Ok(ManifestPublicationOutcome::Published(next)),
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            classify_current_root(store, namespace_id, candidate, expected_predecessor).await
        }
        Err(error @ ObjectStoreError::Transport { .. }) => {
            match classify_current_root(store, namespace_id, candidate, expected_predecessor)
                .await?
            {
                ManifestPublicationOutcome::RootCasRaceLost => {
                    Err(CoreError::store(&object_key, &error))
                }
                outcome => Ok(outcome),
            }
        }
        Err(error) => Err(CoreError::store(&object_key, &error)),
    }
}

/// Builds a manifest reference owned by `namespace_id`.
pub(super) fn manifest_ref_for(
    namespace_id: &NamespaceId,
    manifest: &NamespaceManifestEnvelope,
) -> ManifestRef {
    ManifestRef {
        owner_namespace_id: namespace_id.clone(),
        manifest_no: manifest.payload.manifest_no,
        manifest_object_id: manifest.payload.manifest_object_id.clone(),
        manifest_head_seq: manifest.payload.head_seq,
        manifest_payload_checksum: manifest.payload_checksum.clone(),
    }
}

fn root_supersedes_candidate(current: &MetadataRootState, candidate: &ManifestRef) -> bool {
    current.manifest.manifest_head_seq > candidate.manifest_head_seq
        || (current.manifest.manifest_head_seq == candidate.manifest_head_seq
            && current.manifest.manifest_no >= candidate.manifest_no)
}
