//! Retention floor advancement against the current verified manifest.

use super::load::load_verified_manifest_tables;
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::error::MetadataProjectionLoadError;
use crate::namespace::control::{read_metadata_root_object, read_wal_floor_object};
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_object, ControlObjectKind, WalFloorBasis, WalFloorEnvelope, WalFloorState,
};
use loonfs_api::{AdvanceRetentionResponse, NamespaceId};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

const MAX_RETENTION_PROBE_IO: usize = 8;
const FLOOR_CAS_RETRY_LIMIT: usize = 8;

pub(crate) async fn advance_retention_floor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<AdvanceRetentionResponse, CoreError> {
    // Floor advancement is a GC-family operation: it derives its target from
    // the published metadata root, verifies that basis, and CASes only
    // `wal/floor.json`. The WAL head is never touched — it changes only when
    // commits land. Root monotonicity keeps a mid-flight root swap benign:
    // any replacement covers at least this basis's coverage above the floor.
    let loaded_root = read_metadata_root_object(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    let root = loaded_root.envelope.state;
    let manifest_tables = load_verified_manifest_tables(store, namespace_id, root.manifest_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;
    if manifest_tables.manifest().payload_checksum != root.manifest_payload_checksum {
        return Err(CoreError::NamespaceCorrupt(format!(
            "metadata root for `{}` references manifest `{}` with checksum {} but the manifest carries {}",
            namespace_id.as_str(),
            root.manifest_id,
            root.manifest_payload_checksum,
            manifest_tables.manifest().payload_checksum,
        )));
    }
    let target_floor = manifest_tables.manifest().payload.head_seq;
    let current_floor = read_wal_floor_object(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    if current_floor.envelope.state.floor_seq >= target_floor {
        // Already advanced; skip the existence probes on the idempotent
        // re-invocation path.
        return Ok(AdvanceRetentionResponse {
            namespace_id: namespace_id.clone(),
            retention_floor_seq: current_floor.envelope.state.floor_seq,
        });
    }

    // Advancing the floor surrenders the WAL replay promise below the
    // target, so every metadata segment the basis manifest references must
    // still exist before that promise is given up. This probe is advisory
    // defense-in-depth: the atomic guarantee belongs to the deleter — GC
    // must never remove an object reachable from the current manifest or a
    // retained checkpoint or pin (format spec, "Garbage collection").
    // Corruption discovered later is caught by read-path checksums.
    for metadata_files in manifest_tables
        .manifest()
        .payload
        .metadata_files
        .chunks(MAX_RETENTION_PROBE_IO)
    {
        let probes = metadata_files.iter().map(|metadata_file| async move {
            let present = store
                .head(&metadata_file.object_key)
                .await
                .map_err(|error| CoreError::Store(error.to_string()))?
                .is_some();
            if present {
                Ok(())
            } else {
                Err(CoreError::CheckpointUnavailable(format!(
                    "retention floor cannot advance: missing metadata segment `{}`",
                    metadata_file.object_key
                )))
            }
        });
        futures::future::try_join_all(probes).await?;
    }

    // Monotonic floor CAS: never decrease, record the verified basis.
    for _attempt in 0..FLOOR_CAS_RETRY_LIMIT {
        let loaded = read_wal_floor_object(store, namespace_id)
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
            })?;
        if loaded.envelope.state.floor_seq >= target_floor {
            return Ok(AdvanceRetentionResponse {
                namespace_id: namespace_id.clone(),
                retention_floor_seq: loaded.envelope.state.floor_seq,
            });
        }
        let next = WalFloorState {
            namespace_id: namespace_id.clone(),
            floor_seq: target_floor,
            basis: WalFloorBasis {
                manifest_id: root.manifest_id,
                manifest_head_seq: root.manifest_head_seq,
                manifest_payload_checksum: root.manifest_payload_checksum.clone(),
            },
            verified_at_ms: context.now_ms,
            updated_at_ms: context.now_ms,
        };
        let envelope = WalFloorEnvelope::from_state(
            ControlObjectKind::WalFloor,
            &context.writer_version,
            next,
        )
        .map_err(|err| CoreError::Store(err.to_string()))?;
        let encoded =
            encode_control_object(&envelope).map_err(|err| CoreError::Store(err.to_string()))?;
        let expected_etag = loaded.metadata.etag.as_deref().ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!("missing floor etag for `{}`", loaded.object_key))
        })?;
        match store
            .compare_and_swap(&loaded.object_key, expected_etag, Bytes::from(encoded))
            .await
        {
            Ok(_) => {
                return Ok(AdvanceRetentionResponse {
                    namespace_id: namespace_id.clone(),
                    retention_floor_seq: target_floor,
                })
            }
            Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => continue,
            Err(error) => return Err(CoreError::Store(error.to_string())),
        }
    }
    Err(CoreError::Store(
        "retention floor cas retry exhausted".to_owned(),
    ))
}
