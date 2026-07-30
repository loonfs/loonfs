//! Retention floor advancement against the current verified manifest.

use super::load::{ensure_root_matches_manifest, load_verified_manifest_tables};
use crate::context::MutationContext;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::limits::CONTENTION_RETRY_LIMIT;
use crate::namespace::basis::resolve_retention_floor_seq;
use crate::namespace::control::{
    read_head_object, read_metadata_root_object_if_present, read_wal_floor_object,
    ControlObjectLoadError,
};
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_object, ControlObjectKind, WalFloorEnvelope, WalFloorState,
};
use loonfs_api::{AdvanceRetentionResponse, ChangeSeq, NamespaceId};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

const MAX_RETENTION_PROBE_IO: usize = 8;

pub(crate) async fn advance_retention_floor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<AdvanceRetentionResponse> {
    // Floor advancement is a GC-family operation: it derives its target from
    // the published metadata root, verifies that basis, and CASes only
    // `wal/floor.json`. The WAL head is never touched — it changes only when
    // commits land. Root monotonicity keeps a mid-flight root swap benign:
    // any replacement covers at least this basis's coverage above the floor.
    let head = read_head_object(store, namespace_id)
        .await
        .map_err(CoreError::load_head)?
        .envelope
        .state;
    // A namespace that has published no manifest has nothing to derive a
    // target floor from: its history is retained from birth either way.
    let Some(loaded_root) = read_metadata_root_object_if_present(store, namespace_id)
        .await
        .map_err(CoreError::load_head)?
    else {
        return Ok(AdvanceRetentionResponse {
            namespace_id: namespace_id.clone(),
            retention_floor_seq: resolve_retention_floor_seq(store, &head)
                .await
                .map_err(CoreError::load_head)?,
        });
    };
    let root = loaded_root.envelope.state;
    let manifest_tables =
        load_verified_manifest_tables(store, namespace_id, &root.manifest_object_id)
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
    ensure_root_matches_manifest(namespace_id, &root, manifest_tables.manifest())?;
    // Grep tolerates retention gaps by checkpointed rebootstrap, so its
    // independent watermark never holds the core WAL floor back.
    let target_floor = manifest_tables.manifest().payload.head_seq;
    let current_floor = resolve_retention_floor_seq(store, &head)
        .await
        .map_err(CoreError::load_head)?;
    if current_floor >= target_floor {
        // Already advanced; skip the existence probes on the idempotent
        // re-invocation path.
        return Ok(AdvanceRetentionResponse {
            namespace_id: namespace_id.clone(),
            retention_floor_seq: current_floor,
        });
    }

    // Advancing the floor surrenders the WAL replay promise below the
    // target, so every metadata segment the basis manifest references must
    // still exist before that promise is given up. This probe is advisory
    // defense-in-depth: the atomic guarantee belongs to the deleter — GC
    // must never remove an object reachable from the current manifest or a
    // retained checkpoint or pin (format spec, "Garbage collection").
    // Corruption discovered later is caught by read-path checksums.
    let segment_keys = manifest_tables
        .manifest()
        .payload
        .metadata_files
        .iter()
        .map(|metadata_file| metadata_file.object_key.as_str())
        .collect::<Vec<_>>();
    for segment_keys in segment_keys.chunks(MAX_RETENTION_PROBE_IO) {
        let probes = segment_keys.iter().copied().map(|object_key| async move {
            let present = store
                .head(object_key)
                .await
                .map_err(|error| CoreError::store(object_key, &error))?
                .is_some();
            if present {
                Ok(())
            } else {
                Err(CoreError::CheckpointUnavailable(format!(
                    "retention floor cannot advance: missing metadata segment `{object_key}`"
                )))
            }
        });
        futures::future::try_join_all(probes).await?;
    }

    // Monotonic floor publication: never decrease. The first advance
    // creates the object, because create and fork write no floor.
    for _attempt in 0..CONTENTION_RETRY_LIMIT {
        let loaded = match read_wal_floor_object(store, namespace_id).await {
            Ok(loaded) => Some(loaded),
            Err(ControlObjectLoadError::MissingObject { .. }) => None,
            Err(error) => return Err(CoreError::load_head(error)),
        };
        if loaded
            .as_ref()
            .is_some_and(|loaded| loaded.envelope.state.floor_seq >= target_floor)
        {
            return Ok(AdvanceRetentionResponse {
                namespace_id: namespace_id.clone(),
                retention_floor_seq: loaded
                    .map_or(ChangeSeq(0), |loaded| loaded.envelope.state.floor_seq),
            });
        }
        let next = WalFloorState {
            namespace_id: namespace_id.clone(),
            floor_seq: target_floor,
            verified_at_ms: context.now_ms,
            updated_at_ms: context.now_ms,
        };
        let envelope =
            WalFloorEnvelope::from_state(ControlObjectKind::WalFloor, next).map_err(|err| {
                CoreError::Internal(format!("failed to build wal floor envelope: {err}"))
            })?;
        let encoded = encode_control_object(&envelope).map_err(|err| {
            CoreError::Internal(format!("failed to encode wal floor object: {err}"))
        })?;
        let object_key = loonfs_objectstore::keys::wal_floor(namespace_id.as_str());
        let published = match &loaded {
            Some(loaded) => {
                let expected_etag = loaded.metadata.etag.as_deref().ok_or_else(|| {
                    CoreError::NamespaceCorrupt(format!(
                        "missing floor etag for `{}`",
                        loaded.object_key
                    ))
                })?;
                store
                    .compare_and_swap(&object_key, expected_etag, Bytes::from(encoded))
                    .await
            }
            None => store.put_if_absent(&object_key, Bytes::from(encoded)).await,
        };
        match published {
            Ok(_) => {
                return Ok(AdvanceRetentionResponse {
                    namespace_id: namespace_id.clone(),
                    retention_floor_seq: target_floor,
                })
            }
            Err(ObjectStoreError::PreconditionFailed { .. }) => continue,
            Err(error) => return Err(CoreError::store(&object_key, &error)),
        }
    }
    Err(CoreError::Internal(
        "retention floor compare-and-swap retry exhausted".to_owned(),
    ))
}
