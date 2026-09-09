//! Publication through the next immutable manifest number.

use crate::control_update::{settle_control_write, CasAttempt, WriteEvidence};
use crate::error::{CoreError, Result};
use crate::namespace::control::{
    load_current_manifest_if_present, update_manifest_hint, CurrentManifest,
};
use crate::time::MonotonicTimer;
use bytes::Bytes;
use loonfs_api::wire::control::ManifestRef;
use loonfs_api::wire::manifest::{
    encode_namespace_manifest_json, NamespaceManifestEnvelope, NamespaceManifestPayload,
};
use loonfs_api::{ManifestNo, NamespaceId};
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ManifestPublicationOutcome {
    Installable,
    Published(CurrentManifest),
    CoveredByCurrent(CurrentManifest),
    PredecessorChanged(CurrentManifest),
}

pub(super) fn encode_manifest(
    payload: NamespaceManifestPayload,
) -> Result<NamespaceManifestEnvelope> {
    let object_key = metadata_manifest_object(&payload.namespace_id, &payload.manifest_no);
    encode_namespace_manifest_json(payload)
        .map(|encoded| encoded.into_parts().0)
        .map_err(|error| CoreError::Codec {
            object_key,
            message: error.to_string(),
        })
}

#[tracing::instrument(
    level = "debug",
    name = "loonfs.phase",
    err(level = "warn"),
    skip_all,
    fields(phase = "publish_manifest", key_class = "namespace_manifest")
)]
pub(super) async fn publish_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest: &NamespaceManifestEnvelope,
    expected_predecessor: Option<ManifestNo>,
    timer: &dyn MonotonicTimer,
    started_ms: u64,
) -> Result<ManifestPublicationOutcome> {
    let candidate = CurrentManifest {
        manifest: manifest_ref_for(namespace_id, manifest),
        retention_floor_seq: manifest.payload().retention_floor_seq,
    };
    let current = load_current_manifest_if_present(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    if let Some(current) = &current {
        match classify_current(&current.state, &candidate, expected_predecessor) {
            ManifestPublicationOutcome::Installable => {}
            outcome => return Ok(outcome),
        }
    }
    let predecessor_no = current
        .as_ref()
        .map_or(ManifestNo(0), |loaded| loaded.state.manifest.manifest_no);
    if predecessor_no.successor().ok() != Some(candidate.manifest.manifest_no)
        || manifest.payload().namespace_id != *namespace_id
        || current.as_ref().is_some_and(|loaded| {
            candidate.manifest.manifest_head_seq < loaded.state.manifest.manifest_head_seq
                || candidate.retention_floor_seq < loaded.state.retention_floor_seq
        })
    {
        return Err(CoreError::Internal(format!("manifest `{}` is not a legal successor of `{predecessor_no}` in namespace `{namespace_id}`", candidate.manifest.manifest_no)));
    }
    let object_key = metadata_manifest_object(namespace_id, &candidate.manifest.manifest_no);
    let (_, bytes) = encode_namespace_manifest_json(manifest.payload().clone())
        .map_err(|error| CoreError::Codec {
            object_key: object_key.clone(),
            message: error.to_string(),
        })?
        .into_parts();
    super::flush::ensure_metadata_publication_budget(timer, started_ms, namespace_id)?;
    let outcome = match store.put_if_absent(&object_key, Bytes::from(bytes)).await {
        Ok(_) => ManifestPublicationOutcome::Published(candidate.clone()),
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            classify_current_manifest(store, namespace_id, &candidate, expected_predecessor).await?
        }
        Err(error @ ObjectStoreError::Transport { .. }) => {
            settle_control_write::<_, CoreError, (), CoreError, _, _>(
                CasAttempt::Ambiguous(error, ()),
                |_, ()| async {
                    match classify_current_manifest(
                        store,
                        namespace_id,
                        &candidate,
                        expected_predecessor,
                    )
                    .await?
                    {
                        ManifestPublicationOutcome::Installable => Ok(WriteEvidence::Unknown),
                        outcome => Ok(WriteEvidence::Landed(outcome)),
                    }
                },
            )
            .await??
        }
        Err(error) => return Err(CoreError::store(&object_key, &error)),
    };
    if matches!(outcome, ManifestPublicationOutcome::Published(_))
        && timer.monotonic_now_ms().saturating_sub(started_ms)
            <= crate::limits::METADATA_PUBLICATION_BUDGET_MS
    {
        // Publication is already durable; a failed hint update cannot undo it.
        if let Err(error) =
            update_manifest_hint(store, namespace_id, candidate.manifest.manifest_no).await
        {
            tracing::warn!(namespace_id = namespace_id.as_str(), error = %error, "manifest discovery hint update failed");
        }
    }
    Ok(outcome)
}

fn classify_current(
    current: &CurrentManifest,
    candidate: &CurrentManifest,
    expected_predecessor: Option<ManifestNo>,
) -> ManifestPublicationOutcome {
    if current.manifest == candidate.manifest {
        ManifestPublicationOutcome::Published(current.clone())
    } else if current.manifest.manifest_head_seq > candidate.manifest.manifest_head_seq
        || (current.manifest.manifest_head_seq == candidate.manifest.manifest_head_seq
            && current.manifest.manifest_no >= candidate.manifest.manifest_no)
    {
        ManifestPublicationOutcome::CoveredByCurrent(current.clone())
    } else if Some(current.manifest.manifest_no) == expected_predecessor {
        ManifestPublicationOutcome::Installable
    } else {
        ManifestPublicationOutcome::PredecessorChanged(current.clone())
    }
}

async fn classify_current_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidate: &CurrentManifest,
    expected_predecessor: Option<ManifestNo>,
) -> Result<ManifestPublicationOutcome> {
    Ok(load_current_manifest_if_present(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?
        .map_or(ManifestPublicationOutcome::Installable, |loaded| {
            classify_current(&loaded.state, candidate, expected_predecessor)
        }))
}

pub(super) fn manifest_ref_for(
    namespace_id: &NamespaceId,
    manifest: &NamespaceManifestEnvelope,
) -> ManifestRef {
    ManifestRef {
        owner_namespace_id: namespace_id.clone(),
        manifest_no: manifest.payload().manifest_no,
        manifest_head_seq: manifest.payload().head_seq,
        manifest_payload_checksum: manifest.payload_checksum().to_owned(),
    }
}
