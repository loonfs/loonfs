//! Retention floor advancement through a verified successor manifest.

use super::error::ManifestLoadError;
use super::flush::next_manifest_no_after;
use super::publish::{encode_manifest, publish_manifest, ManifestPublicationOutcome};
use super::runs::MAX_MAINTENANCE_SEGMENT_IO;
use crate::context::MutationContext;
use crate::control_update::{retry_while_contended, CasAttempt, WriteEvidence};
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::control_snapshot::load_control_snapshot;
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::manifest::NamespaceManifestEnvelope;
use loonfs_api::{AdvanceRetentionResponse, NamespaceId};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
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

pub(crate) async fn advance_retention_floor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    _context: &MutationContext,
) -> Result<AdvanceRetentionResponse> {
    let floor = retry_while_contended(
        || async {
            let timer = StdMonotonicTimer::default();
            let started_ms = timer.monotonic_now_ms();
            let snapshot = load_control_snapshot(store, namespace_id)
                .await
                .map_err(CoreError::ControlObjectLoad)?;
            let Some(current) = snapshot.root else {
                return Result::Ok(CasAttempt::Settled(snapshot.retention_floor_seq));
            };
            let target = current.envelope.payload().head_seq;
            if current.state.retention_floor_seq >= target {
                return Ok(CasAttempt::Settled(current.state.retention_floor_seq));
            }
            verify_manifest_segments_exist(store, &current.envelope)
                .await
                .map_err(|error| {
                    CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
                })?;
            let mut payload = current.envelope.payload().clone();
            payload.manifest_no = next_manifest_no_after(payload.manifest_no)?;
            payload.retention_floor_seq = target;
            let manifest = encode_manifest(payload)?;
            match publish_manifest(
                store,
                namespace_id,
                &manifest,
                Some(current.state.manifest.manifest_no),
                &timer,
                started_ms,
            )
            .await?
            {
                ManifestPublicationOutcome::Published(current) => {
                    Ok(CasAttempt::Settled(current.retention_floor_seq))
                }
                _ => Ok(CasAttempt::Contended(CoreError::contention_exhausted(
                    &current.object_key,
                ))),
            }
        },
        |_, ()| async { Ok(WriteEvidence::Unknown) },
    )
    .await??;
    Ok(AdvanceRetentionResponse {
        namespace_id: namespace_id.clone(),
        retention_floor_seq: floor,
    })
}
