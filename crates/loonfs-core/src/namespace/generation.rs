//! Publishes the first manifest of every namespace generation.

use super::control::load_current_manifest_if_present;
use crate::checkpoint::publish::{encode_manifest, publish_manifest, ManifestPublicationOutcome};
use crate::error::{CoreError, Result};
use crate::time::MonotonicTimer;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_state, ControlObjectKind, HintPayload, RetiredGenerationPayload,
};
use loonfs_api::wire::manifest::NamespaceManifestPayload;
use loonfs_api::{ManifestNo, NamespaceGeneration, WalNo};
use loonfs_objectstore::keys::{hint, retired_generation_record};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GenerationPublication {
    Published,
    Exists,
}

pub(super) async fn publish_generation<S: ObjectStore + ?Sized>(
    store: &S,
    start: &NamespaceManifestPayload,
    expected_generation: Option<NamespaceGeneration>,
    timer: &dyn MonotonicTimer,
    started_ms: u64,
) -> Result<GenerationPublication> {
    let namespace_id = &start.namespace_id;
    loop {
        let current = load_current_manifest_if_present(store, namespace_id)
            .await
            .map_err(CoreError::ControlObjectLoad)?;
        if expected_generation.is_some_and(|expected| {
            current.as_ref().is_some_and(|current| {
                let payload = current.envelope.payload();
                if payload.status.is_deleted() {
                    payload.generation.successor().ok() != Some(expected)
                } else {
                    payload.generation > expected
                }
            })
        }) {
            return Err(crate::commit::WalPublishError::StaleHead.into());
        }
        let mut payload = start.clone();
        match &current {
            Some(current) if !current.envelope.payload().status.is_deleted() => {
                return Ok(GenerationPublication::Exists);
            }
            Some(tombstone) => {
                continue_counters(tombstone.envelope.payload(), &mut payload)?;
                crate::checkpoint::ensure_metadata_publication_budget(
                    timer,
                    started_ms,
                    namespace_id,
                )?;
                let retired = RetiredGenerationPayload {
                    namespace_id: namespace_id.clone(),
                    generation: tombstone.state.generation,
                    tombstone: tombstone.state.manifest.clone(),
                    created_at_ms: payload.created_at_ms,
                };
                put_control_if_absent(
                    store,
                    retired_generation_record(namespace_id, retired.generation),
                    ControlObjectKind::RetiredGeneration,
                    &retired,
                )
                .await?;
            }
            None => {
                let first = HintPayload {
                    namespace_id: namespace_id.clone(),
                    manifest_no: ManifestNo(1),
                    wal_no: WalNo(0),
                };
                put_control_if_absent(store, hint(namespace_id), ControlObjectKind::Hint, &first)
                    .await?;
            }
        }
        let predecessor = current.map(|current| current.state.manifest.manifest_no);
        let manifest = encode_manifest(payload)?;
        if let ManifestPublicationOutcome::Published(_) = publish_manifest(
            store,
            namespace_id,
            manifest,
            predecessor,
            timer,
            started_ms,
        )
        .await?
        {
            return Ok(GenerationPublication::Published);
        }
    }
}

fn continue_counters(
    tombstone: &NamespaceManifestPayload,
    payload: &mut NamespaceManifestPayload,
) -> Result<()> {
    payload.manifest_no = tombstone
        .manifest_no
        .successor()
        .map_err(|error| CoreError::Internal(format!("manifest number {error}")))?;
    payload.generation = tombstone
        .generation
        .successor()
        .map_err(|error| CoreError::Internal(format!("namespace generation {error}")))?;
    payload.generation_first_manifest_no = payload.manifest_no;
    payload.writer_epoch = tombstone
        .writer_epoch
        .successor()
        .map_err(|error| CoreError::Internal(format!("writer epoch {error}")))?;
    payload.compactor_epoch = tombstone
        .compactor_epoch
        .checked_add(1)
        .ok_or_else(|| CoreError::Internal("compactor epoch overflow".to_owned()))?;
    payload.folded_wal_no = tombstone.folded_wal_no;
    Ok(())
}

async fn put_control_if_absent<S: ObjectStore + ?Sized, T: Serialize>(
    store: &S,
    object_key: String,
    kind: ControlObjectKind,
    state: &T,
) -> Result<()> {
    let bytes = encode_control_state(kind, state).map_err(|error| CoreError::Codec {
        object_key: object_key.clone(),
        message: error.to_string(),
    })?;
    match store.put_if_absent(&object_key, Bytes::from(bytes)).await {
        Ok(_) | Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(()),
        Err(error) => Err(CoreError::store(&object_key, &error)),
    }
}
