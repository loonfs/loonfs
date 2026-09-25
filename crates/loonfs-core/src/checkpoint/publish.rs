//! Publication through the next immutable manifest number.

use crate::control_update::{settle_control_write, CasAttempt, WriteEvidence};
use crate::error::{CoreError, Result};
use crate::namespace::control::{
    load_current_manifest_if_present, load_manifest_by_number, raise_hint, CurrentManifest,
};
use crate::time::Deadline;
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
    Published(CurrentManifest),
    CoveredByCurrent(CurrentManifest),
    PredecessorChanged(CurrentManifest),
}

enum ManifestClassification {
    Installable,
    Settled(ManifestPublicationOutcome),
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

pub(crate) enum ManifestChange<T> {
    Next(Box<NamespaceManifestPayload>, T),
    Finished(T),
    Again,
}

pub(crate) async fn update_manifest<S, T, F, Fut>(
    store: &S,
    namespace_id: &NamespaceId,
    deadline: &Deadline,
    mut change: F,
) -> Result<T>
where
    S: ObjectStore + ?Sized,
    F: FnMut(NamespaceManifestPayload) -> Fut,
    Fut: std::future::Future<Output = Result<ManifestChange<T>>>,
{
    loop {
        let current = crate::namespace::control::load_current_manifest(store, namespace_id).await?;
        let predecessor = current.state.envelope.payload().manifest_no;
        let (mut payload, result) = match change(current.state.envelope.payload().clone()).await? {
            ManifestChange::Next(payload, result) => (*payload, result),
            ManifestChange::Finished(result) => return Ok(result),
            ManifestChange::Again => continue,
        };
        payload.manifest_no = super::flush::next_manifest_no_after(predecessor)?;
        let manifest = encode_manifest(payload)?;
        if matches!(
            publish_manifest(store, manifest, deadline).await?,
            ManifestPublicationOutcome::Published(_)
        ) {
            return Ok(result);
        }
    }
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
    manifest: EncodedEnvelope<NamespaceManifestPayload>,
    deadline: &Deadline,
) -> Result<ManifestPublicationOutcome> {
    let namespace_id = manifest.envelope().payload().namespace_id.clone();
    let namespace_id = &namespace_id;
    let expected_predecessor = manifest
        .envelope()
        .payload()
        .manifest_no
        .0
        .checked_sub(1)
        .filter(|number| *number > 0)
        .map(ManifestNo);
    // A fork's first manifest names its source pin, which no other publisher
    // holds. Every other payload can be rebuilt byte for byte by a rival, so
    // identical bytes prove nothing about who wrote them.
    let names_own_pin = manifest.envelope().payload().manifest_no == ManifestNo(1)
        && manifest.envelope().payload().fork_basis.is_some();
    let candidate = CurrentManifest {
        envelope: std::sync::Arc::new(manifest.envelope().clone()),
    };
    let current = load_current_manifest_if_present(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    if let Some(current) = &current {
        // A tombstone ends its namespace; work that raced the deletion stops here.
        if current.state.envelope.payload().status.is_deleted()
            && current.state.manifest() != candidate.manifest()
        {
            return Err(CoreError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            });
        }
        if manifest.envelope().payload().writer_epoch
            < current.state.envelope.payload().writer_epoch
        {
            return Ok(ManifestPublicationOutcome::PredecessorChanged(
                current.state.clone(),
            ));
        }
        match classify_current(&current.state, &candidate, expected_predecessor, false) {
            ManifestClassification::Installable => {}
            ManifestClassification::Settled(outcome) => return Ok(outcome),
        }
    }
    let payload = manifest.envelope().payload();
    let legal = match &current {
        Some(current) => current
            .state
            .envelope
            .payload()
            .ensure_successor(payload)
            .map_err(|error| error.to_string()),
        None if payload.manifest_no == ManifestNo(1) => payload
            .ensure_first_manifest()
            .map_err(|error| error.to_string()),
        None => Err("has no predecessor".to_owned()),
    };
    if let Err(reason) = legal {
        return Err(CoreError::Internal(format!(
            "manifest `{}` of namespace `{namespace_id}` {reason}",
            payload.manifest_no
        )));
    }
    let object_key = metadata_manifest_object(namespace_id, &candidate.manifest().manifest_no);
    deadline.ensure_metadata_publication_budget(namespace_id)?;
    let outcome = match store
        .put_if_absent(&object_key, Bytes::from(manifest.into_bytes()))
        .await
    {
        Ok(_) => {
            let elapsed_ms = deadline.elapsed_ms();
            // A put that lands after the budget may have landed on a reclaimed number.
            if elapsed_ms > crate::limits::METADATA_PUBLICATION_BUDGET_MS {
                return Err(CoreError::Store {
                    object_key,
                    message: format!(
                        "manifest publication outcome is unknown after {elapsed_ms}ms (budget {}ms)",
                        crate::limits::METADATA_PUBLICATION_BUDGET_MS,
                    ),
                    class: crate::error::StoreFailureClass::RetryableTransport,
                });
            }
            ManifestPublicationOutcome::Published(candidate.clone())
        }
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            classify_current_manifest(store, namespace_id, &candidate, expected_predecessor, false)
                .await?
                .settled_after_conflict(namespace_id)?
        }
        Err(error @ ObjectStoreError::Transport { .. }) => {
            settle_control_write::<_, CoreError, (), CoreError, _, _>(
                CasAttempt::Ambiguous(error, ()),
                |_, ()| async {
                    // Only the exact manifest at this number confirms the put;
                    // a later manifest may already cover it.
                    let landed = load_manifest_by_number(
                        store,
                        namespace_id,
                        candidate.manifest().manifest_no,
                    )
                    .await
                    .map_err(CoreError::ControlObjectLoad)?;
                    let outcome = match landed {
                        Some(landed) => classify_current(
                            &landed.state,
                            &candidate,
                            expected_predecessor,
                            names_own_pin,
                        ),
                        None => {
                            classify_current_manifest(
                                store,
                                namespace_id,
                                &candidate,
                                expected_predecessor,
                                names_own_pin,
                            )
                            .await?
                        }
                    };
                    match outcome {
                        ManifestClassification::Installable => Ok(WriteEvidence::Unknown),
                        ManifestClassification::Settled(outcome) => {
                            Ok(WriteEvidence::Landed(outcome))
                        }
                    }
                },
            )
            .await??
        }
        Err(error) => return Err(CoreError::store(&object_key, &error)),
    };
    if matches!(outcome, ManifestPublicationOutcome::Published(_))
        && deadline.elapsed_ms() <= crate::limits::METADATA_PUBLICATION_BUDGET_MS
    {
        // Publication is already durable; a failed hint update cannot undo it.
        if let Err(error) = raise_hint(
            store,
            namespace_id,
            candidate.manifest().manifest_no,
            candidate.folded_wal_no(),
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
    confirm_authorship: bool,
) -> ManifestClassification {
    let outcome = if current.manifest() == candidate.manifest() {
        if confirm_authorship {
            ManifestPublicationOutcome::Published(current.clone())
        } else {
            ManifestPublicationOutcome::CoveredByCurrent(current.clone())
        }
    } else if current.compactor_epoch() > candidate.compactor_epoch() {
        ManifestPublicationOutcome::PredecessorChanged(current.clone())
    } else if current.folded_wal_no() >= candidate.folded_wal_no()
        && (current.manifest().head_seq, current.manifest().manifest_no)
            >= (
                candidate.manifest().head_seq,
                candidate.manifest().manifest_no,
            )
    {
        ManifestPublicationOutcome::CoveredByCurrent(current.clone())
    } else if Some(current.manifest().manifest_no) == expected_predecessor {
        return ManifestClassification::Installable;
    } else {
        ManifestPublicationOutcome::PredecessorChanged(current.clone())
    };
    ManifestClassification::Settled(outcome)
}

impl ManifestClassification {
    fn settled_after_conflict(
        self,
        namespace_id: &NamespaceId,
    ) -> Result<ManifestPublicationOutcome> {
        match self {
            Self::Settled(outcome) => Ok(outcome),
            Self::Installable => Err(CoreError::NamespaceCorrupt(format!(
                "namespace `{namespace_id}` has no current manifest covering a taken manifest number"
            ))),
        }
    }
}

async fn classify_current_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidate: &CurrentManifest,
    expected_predecessor: Option<ManifestNo>,
    confirm_authorship: bool,
) -> Result<ManifestClassification> {
    Ok(load_current_manifest_if_present(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?
        .map_or(ManifestClassification::Installable, |loaded| {
            classify_current(
                &loaded.state,
                candidate,
                expected_predecessor,
                confirm_authorship,
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
