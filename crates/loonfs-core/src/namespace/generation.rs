//! Publishes the first manifest of every namespace generation.

use super::control::{load_current_manifest_if_present, LoadedManifest};
use crate::checkpoint::publish::{encode_manifest, publish_manifest, ManifestPublicationOutcome};
use crate::checkpoint::record::write_checkpoint_record_if_absent;
use crate::error::{CoreError, Result};
use crate::time::MonotonicTimer;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_state, ContentStorePayload, ControlObjectKind, HintPayload, PinOwner, PinPayload,
};
use loonfs_api::wire::manifest::NamespaceManifestPayload;
use loonfs_api::{ManifestNo, PinId, WalNo};
use loonfs_objectstore::keys::{content_store, hint};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GenerationPublication {
    Published,
    Exists,
}

/// Publishes `start` as the first manifest of a new generation of its id:
/// manifest 1 for an unused id, or the successor of the id's tombstone. An
/// active current manifest answers `Exists`.
pub(super) async fn publish_generation<S: ObjectStore + ?Sized>(
    store: &S,
    start: &NamespaceManifestPayload,
    timer: &dyn MonotonicTimer,
    started_ms: u64,
) -> Result<GenerationPublication> {
    let namespace_id = &start.namespace_id;
    loop {
        let current = load_current_manifest_if_present(store, namespace_id)
            .await
            .map_err(CoreError::ControlObjectLoad)?;
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
                write_retired_pin(store, tombstone, payload.created_at_ms).await?;
                write_content_store_descriptor(store, &payload).await?;
            }
            None => {
                write_content_store_descriptor(store, &payload).await?;
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

/// Carries the counters that outlive a generation from its tombstone into the
/// first manifest of the next one.
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

async fn write_retired_pin<S: ObjectStore + ?Sized>(
    store: &S,
    current: &LoadedManifest,
    created_at_ms: u64,
) -> Result<()> {
    let tombstone = current.envelope.payload();
    let retired = PinPayload {
        namespace_id: tombstone.namespace_id.clone(),
        pin_id: PinId::retired(&tombstone.namespace_id, tombstone.manifest_no),
        head_seq: tombstone.head_seq,
        payload_checksum: current.state.manifest.payload_checksum.clone(),
        created_at_ms,
        owner: PinOwner::Retired {},
    };
    write_checkpoint_record_if_absent(store, &retired).await
}

async fn write_content_store_descriptor<S: ObjectStore + ?Sized>(
    store: &S,
    payload: &NamespaceManifestPayload,
) -> Result<()> {
    let descriptor = ContentStorePayload {
        content_store_id: payload.content_store_id.clone(),
        created_at_ms: payload.created_at_ms,
    };
    put_control_if_absent(
        store,
        content_store(&payload.content_store_id),
        ControlObjectKind::ContentStore,
        &descriptor,
    )
    .await
}

/// Writes a control object that concurrent attempts write identically, so an
/// existing object is success.
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
