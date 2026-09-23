//! Publication through the next immutable manifest number.

use crate::control_update::{settle_control_write, CasAttempt, WriteEvidence};
use crate::error::{CoreError, Result};
use crate::namespace::control::{
    load_current_manifest_if_present, load_discovered_manifest, raise_hint, CurrentManifest,
};
use crate::time::MonotonicTimer;
use bytes::Bytes;
use loonfs_api::wire::control::ManifestRef;
use loonfs_api::wire::envelope::EncodedEnvelope;
use loonfs_api::wire::manifest::{
    encode_namespace_manifest_json, NamespaceManifestEnvelope, NamespaceManifestPayload,
};
use loonfs_api::{ManifestNo, NamespaceId};
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ManifestPublicationOutcome {
    Installable,
    Published(CurrentManifest),
    CoveredByCurrent(CurrentManifest),
    PredecessorChanged(CurrentManifest),
}

pub(crate) fn encode_manifest(
    payload: NamespaceManifestPayload,
) -> Result<EncodedEnvelope<NamespaceManifestPayload>> {
    let object_key = metadata_manifest_object(&payload.namespace_id, &payload.manifest_no);
    encode_namespace_manifest_json(payload).map_err(|error| CoreError::Codec {
        object_key,
        message: error.to_string(),
    })
}

#[tracing::instrument(
    level = "debug",
    name = "loonfs.phase",
    err(level = "warn"),
    skip_all,
    fields(phase = "publish_manifest", key_class = "manifest")
)]
pub(crate) async fn publish_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest: EncodedEnvelope<NamespaceManifestPayload>,
    expected_predecessor: Option<ManifestNo>,
    timer: &dyn MonotonicTimer,
    started_ms: u64,
) -> Result<ManifestPublicationOutcome> {
    let starts_generation = manifest.envelope().payload().manifest_no
        == manifest.envelope().payload().generation_first_manifest_no;
    let candidate = CurrentManifest {
        manifest: manifest_ref_for(namespace_id, manifest.envelope()),
        generation: manifest.envelope().payload().generation,
        retention_floor_seq: manifest.envelope().payload().retention_floor_seq,
        folded_wal_no: manifest.envelope().payload().folded_wal_no,
        compactor_epoch: manifest.envelope().payload().compactor_epoch,
    };
    let current = load_current_manifest_if_present(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    if let Some(current) = &current {
        // A tombstone ends its generation; work that raced the deletion stops here.
        if current.envelope.payload().status.is_deleted()
            && current.state.generation == candidate.generation
            && current.state.manifest != candidate.manifest
        {
            return Err(CoreError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            });
        }
        if manifest.envelope().payload().writer_epoch < current.envelope.payload().writer_epoch {
            return Ok(ManifestPublicationOutcome::PredecessorChanged(
                current.state.clone(),
            ));
        }
        match classify_current(
            &current.state,
            &candidate,
            expected_predecessor,
            starts_generation,
        ) {
            ManifestPublicationOutcome::Installable => {}
            outcome => return Ok(outcome),
        }
    }
    let payload = manifest.envelope().payload();
    let legal = match &current {
        _ if payload.namespace_id != *namespace_id => {
            Err("belongs to another namespace".to_owned())
        }
        Some(current) => current
            .envelope
            .payload()
            .ensure_successor(payload)
            .map_err(|error| error.to_string()),
        None if payload.manifest_no == ManifestNo(1) => payload
            .ensure_generation_start()
            .map_err(|error| error.to_string()),
        None => Err("has no predecessor".to_owned()),
    };
    if let Err(reason) = legal {
        return Err(CoreError::Internal(format!(
            "manifest `{}` of namespace `{namespace_id}` {reason}",
            payload.manifest_no
        )));
    }
    let object_key = metadata_manifest_object(namespace_id, &candidate.manifest.manifest_no);
    super::flush::ensure_metadata_publication_budget(timer, started_ms, namespace_id)?;
    let outcome = match store
        .put_if_absent(&object_key, Bytes::from(manifest.into_bytes()))
        .await
    {
        Ok(_) => ManifestPublicationOutcome::Published(candidate.clone()),
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            classify_current_manifest(
                store,
                namespace_id,
                &candidate,
                expected_predecessor,
                starts_generation,
            )
            .await?
        }
        Err(error @ ObjectStoreError::Transport { .. }) => {
            settle_control_write::<_, CoreError, (), CoreError, _, _>(
                CasAttempt::Ambiguous(error, ()),
                |_, ()| async {
                    // Only the exact manifest at this number confirms the put;
                    // a later manifest may already cover it.
                    let landed = load_discovered_manifest(
                        store,
                        namespace_id,
                        candidate.manifest.manifest_no,
                    )
                    .await
                    .map_err(CoreError::ControlObjectLoad)?;
                    if landed.is_some_and(|landed| landed.state.manifest == candidate.manifest) {
                        return Ok(WriteEvidence::Landed(
                            ManifestPublicationOutcome::Published(candidate.clone()),
                        ));
                    }
                    match classify_current_manifest(
                        store,
                        namespace_id,
                        &candidate,
                        expected_predecessor,
                        starts_generation,
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
        if let Err(error) = raise_hint(
            store,
            namespace_id,
            candidate.manifest.manifest_no,
            candidate.folded_wal_no,
            None,
        )
        .await
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
    starts_generation: bool,
) -> ManifestPublicationOutcome {
    if current.manifest == candidate.manifest {
        // Concurrent creates can have identical payloads. Only a put or ambiguous read-back confirms installation.
        if starts_generation {
            ManifestPublicationOutcome::CoveredByCurrent(current.clone())
        } else {
            ManifestPublicationOutcome::Published(current.clone())
        }
    } else if current.compactor_epoch > candidate.compactor_epoch {
        ManifestPublicationOutcome::PredecessorChanged(current.clone())
    } else if current.folded_wal_no >= candidate.folded_wal_no
        && current.position() >= candidate.position()
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
    starts_generation: bool,
) -> Result<ManifestPublicationOutcome> {
    Ok(load_current_manifest_if_present(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?
        .map_or(ManifestPublicationOutcome::Installable, |loaded| {
            classify_current(
                &loaded.state,
                candidate,
                expected_predecessor,
                starts_generation,
            )
        }))
}

pub(crate) fn manifest_ref_for(
    namespace_id: &NamespaceId,
    manifest: &NamespaceManifestEnvelope,
) -> ManifestRef {
    ManifestRef {
        owner_namespace_id: namespace_id.clone(),
        manifest_no: manifest.payload().manifest_no,
        head_seq: manifest.payload().head_seq,
        payload_checksum: manifest.payload_checksum().to_owned(),
    }
}
