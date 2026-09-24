//! Retention floor advancement through a verified successor manifest.

use super::error::ManifestLoadError;
use super::publish::{update_manifest, ManifestChange};
use super::runs::MAX_MAINTENANCE_SEGMENT_IO;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::{control::ensure_namespace_live, state::NamespaceReadState};
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::manifest::NamespaceManifestPayload;
use loonfs_api::{AdvanceRetentionResponse, NamespaceId};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeSet;

async fn verify_manifest_segments_exist<S: ObjectStore + ?Sized>(
    store: &S,
    manifest: &NamespaceManifestPayload,
) -> std::result::Result<(), ManifestLoadError> {
    let object_keys = manifest
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

pub(crate) async fn advance_retention_floor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<AdvanceRetentionResponse> {
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    let floor = update_manifest(
        store,
        namespace_id,
        &timer,
        started_ms,
        |mut payload| async move {
            ensure_namespace_live(&NamespaceReadState::from(&payload))?;
            let target = payload.head_seq;
            if payload.retention_floor_seq >= target {
                return Ok(ManifestChange::Finished(payload.retention_floor_seq));
            }
            verify_manifest_segments_exist(store, &payload)
                .await
                .map_err(|error| {
                    CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
                })?;
            payload.retention_floor_seq = target;
            Ok(ManifestChange::Next(Box::new(payload), target))
        },
    )
    .await?;
    Ok(AdvanceRetentionResponse {
        namespace_id: namespace_id.clone(),
        retention_floor_seq: floor,
    })
}
