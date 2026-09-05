//! Retention floor advancement against the current verified manifest.

use super::error::ManifestLoadError;
use super::load::load_manifest_segments;
use super::runs::MAX_MAINTENANCE_SEGMENT_IO;
use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::control_update::{retry_while_contended, CasAttempt, WriteEvidence};
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::basis::namespace_birth_seq;
use crate::namespace::control::{
    load_head_object, load_metadata_root_object_if_present, load_wal_floor_object,
};
use crate::namespace::control_snapshot::resolve_retention_floor_seq;
use bytes::Bytes;
use loonfs_api::wire::control::{encode_control_state, ControlObjectKind, WalFloorState};
use loonfs_api::wire::manifest::NamespaceManifestEnvelope;
use loonfs_api::{AdvanceRetentionResponse, ChangeSeq, NamespaceId};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use std::collections::BTreeSet;

async fn verify_manifest_segments_exist<S: ObjectStore + ?Sized>(
    store: &S,
    manifest: &NamespaceManifestEnvelope,
) -> std::result::Result<(), ManifestLoadError> {
    let object_keys = manifest
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(metadata_segment_object_key)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    for chunk in object_keys.chunks(MAX_MAINTENANCE_SEGMENT_IO) {
        futures::future::try_join_all(chunk.iter().map(|object_key| async move {
            match store
                .head(object_key)
                .await
                .map_err(|error| ManifestLoadError::ReadSegment {
                    object_key: object_key.clone(),
                    message: error.public_message().into_owned(),
                })? {
                Some(_) => Ok(()),
                None => Err(ManifestLoadError::MissingSegment {
                    object_key: object_key.clone(),
                }),
            }
        }))
        .await?;
    }
    Ok(())
}

/// Reads back a monotonic floor after a write whose outcome is unknown.
///
/// Any value at or above the requested target proves that the operation is
/// already settled, whether this writer or a concurrent writer published it.
async fn load_floor_at_or_above<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    target_floor: ChangeSeq,
) -> Result<Option<ChangeSeq>> {
    match load_wal_floor_object(store, namespace_id).await {
        Ok(loaded) if loaded.state.floor_seq >= target_floor => Ok(Some(loaded.state.floor_seq)),
        Ok(_) | Err(ControlObjectLoadError::MissingObject { .. }) => Ok(None),
        Err(error) => Err(CoreError::ControlObjectLoad(error)),
    }
}

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
    // The deleter's reachability checks provide the atomic safety guarantee,
    // but an advisory existence probe below prevents a segment that has
    // already disappeared from surrendering the WAL recovery path. Read-path
    // checksums still detect corruption that an existence probe cannot.
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
            namespace_id: namespace_id.clone(),
            retention_floor_seq: resolve_retention_floor_seq(store, &head)
                .await
                .map_err(CoreError::ControlObjectLoad)?,
        });
    };
    let root = loaded_root.state;
    let manifest_segments = load_manifest_segments(store, None, &root.manifest).await?;
    // Grep tolerates retention gaps by checkpointed rebootstrap, so its
    // independent watermark never holds the core WAL floor back.
    let target_floor = manifest_segments.manifest().payload().head_seq;
    let initial_floor = match load_wal_floor_object(store, namespace_id).await {
        Ok(loaded) => Some(loaded),
        Err(ControlObjectLoadError::MissingObject { .. }) => None,
        Err(error) => return Err(CoreError::ControlObjectLoad(error)),
    };
    let current_floor = initial_floor.as_ref().map_or_else(
        || namespace_birth_seq(&head),
        |loaded| loaded.state.floor_seq,
    );
    if current_floor >= target_floor {
        // Already advanced, so the idempotent re-invocation writes nothing.
        return Ok(AdvanceRetentionResponse {
            namespace_id: namespace_id.clone(),
            retention_floor_seq: current_floor,
        });
    }
    verify_manifest_segments_exist(store, manifest_segments.manifest())
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;

    // Monotonic floor publication: never decrease. The first advance
    // creates the object, because create and fork write no floor.
    let object_key = loonfs_objectstore::keys::wal_floor(namespace_id);
    let mut first_floor = Some(initial_floor);
    let advanced =
        retry_while_contended(
            || {
                let first = first_floor.take();
                async {
                    let loaded = match first {
                        Some(loaded) => loaded,
                        None => match load_wal_floor_object(store, namespace_id).await {
                            Ok(loaded) => Some(loaded),
                            Err(ControlObjectLoadError::MissingObject { .. }) => None,
                            Err(error) => return Err(CoreError::ControlObjectLoad(error)),
                        },
                    };
                    if let Some(floor_seq) = loaded
                        .as_ref()
                        .map(|loaded| loaded.state.floor_seq)
                        .filter(|floor_seq| *floor_seq >= target_floor)
                    {
                        return Ok(CasAttempt::Settled(floor_seq));
                    }
                    let next = WalFloorState {
                        namespace_id: namespace_id.clone(),
                        floor_seq: target_floor,
                        updated_at_ms: context.now_ms,
                    };
                    let encoded = encode_control_state(ControlObjectKind::WalFloor, &next)
                        .map_err(|error| CoreError::Codec {
                            object_key: object_key.clone(),
                            message: error.to_string(),
                        })?;
                    let published = match &loaded {
                        Some(loaded) => {
                            store
                                .compare_and_swap(&object_key, &loaded.etag, Bytes::from(encoded))
                                .await
                        }
                        None => store.put_if_absent(&object_key, Bytes::from(encoded)).await,
                    };
                    match published {
                        Ok(_) => Ok(CasAttempt::Settled(target_floor)),
                        Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(
                            CasAttempt::Contended(CoreError::contention_exhausted(&object_key)),
                        ),
                        Err(error @ ObjectStoreError::Transport { .. }) => {
                            Ok(CasAttempt::Ambiguous(error, ()))
                        }
                        Err(error) => Err(CoreError::store(&object_key, &error)),
                    }
                }
            },
            |_, ()| async {
                match load_floor_at_or_above(store, namespace_id, target_floor).await? {
                    Some(floor_seq) => Ok(WriteEvidence::Landed(floor_seq)),
                    None => Ok(WriteEvidence::Lost(CoreError::contention_exhausted(
                        &object_key,
                    ))),
                }
            },
        )
        .await?;
    Ok(AdvanceRetentionResponse {
        namespace_id: namespace_id.clone(),
        retention_floor_seq: advanced?,
    })
}
