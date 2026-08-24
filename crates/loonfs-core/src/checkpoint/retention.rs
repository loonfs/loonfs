//! Retention floor advancement against the current verified manifest.

use super::load::{ensure_root_matches_manifest, load_verified_manifest_segments};
use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::control_update::{retry_while_contended, CasAttempt};
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::basis::resolve_retention_floor_seq;
use crate::namespace::control::{
    load_head_object, load_metadata_root_object_if_present, load_wal_floor_object,
};
use bytes::Bytes;
use loonfs_api::wire::control::{encode_control_state, ControlObjectKind, WalFloorState};
use loonfs_api::{AdvanceRetentionResponse, NamespaceId};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

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
    //
    // Nothing here probes the basis manifest's segments before the floor
    // moves. Advancing surrenders the WAL replay promise below the target,
    // and what keeps that safe is the deleter: GC must never remove an
    // object reachable from the current manifest or a retained checkpoint or
    // pin (format spec, "Garbage collection"). Corruption that slips past it
    // is caught by read-path checksums, which an existence probe here could
    // not have caught anyway.
    let head = load_head_object(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?
        .state;
    // A namespace that has published no manifest has nothing to derive a
    // target floor from: its history is retained from birth either way.
    let Some(loaded_root) = load_metadata_root_object_if_present(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?
    else {
        return Ok(AdvanceRetentionResponse {
            retention_floor_seq: resolve_retention_floor_seq(store, &head)
                .await
                .map_err(CoreError::ControlObjectLoad)?,
        });
    };
    let root = loaded_root.state;
    let manifest_segments =
        load_verified_manifest_segments(store, namespace_id, &root.manifest.manifest_object_id)
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
    ensure_root_matches_manifest(namespace_id, &root, manifest_segments.manifest())?;
    // Grep tolerates retention gaps by checkpointed rebootstrap, so its
    // independent watermark never holds the core WAL floor back.
    let target_floor = manifest_segments.manifest().payload.head_seq;
    let current_floor = resolve_retention_floor_seq(store, &head)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    if current_floor >= target_floor {
        // Already advanced, so the idempotent re-invocation writes nothing.
        return Ok(AdvanceRetentionResponse {
            retention_floor_seq: current_floor,
        });
    }

    // Monotonic floor publication: never decrease. The first advance
    // creates the object, because create and fork write no floor.
    let object_key = &loonfs_objectstore::keys::wal_floor(namespace_id);
    let advanced = retry_while_contended(|| async move {
        let loaded = match load_wal_floor_object(store, namespace_id).await {
            Ok(loaded) => Some(loaded),
            Err(ControlObjectLoadError::MissingObject { .. }) => None,
            Err(error) => return Err(CoreError::ControlObjectLoad(error)),
        };
        let published_floor = loaded.as_ref().map(|loaded| loaded.state.floor_seq);
        if let Some(floor_seq) = published_floor.filter(|floor_seq| *floor_seq >= target_floor) {
            return Ok(CasAttempt::Settled(floor_seq));
        }
        let next = WalFloorState {
            namespace_id: namespace_id.clone(),
            floor_seq: target_floor,
            updated_at_ms: context.now_ms,
        };
        let encoded =
            encode_control_state(ControlObjectKind::WalFloor, &next).map_err(|error| {
                CoreError::Codec {
                    object_key: object_key.clone(),
                    message: error.to_string(),
                }
            })?;
        let published = match &loaded {
            Some(loaded) => {
                store
                    .compare_and_swap(object_key, &loaded.etag, Bytes::from(encoded))
                    .await
            }
            // The retention floor is mutable control state, so its first publication is a conditional create.
            None => store.put_if_absent(object_key, Bytes::from(encoded)).await,
        };
        match published {
            Ok(_) => Ok(CasAttempt::Settled(target_floor)),
            Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CasAttempt::Contended),
            Err(error) => Err(CoreError::store(object_key, &error)),
        }
    })
    .await?;
    Ok(AdvanceRetentionResponse {
        retention_floor_seq: advanced.ok_or_else(|| CoreError::contention_exhausted(object_key))?,
    })
}
