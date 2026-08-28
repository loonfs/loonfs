//! Reads the state pinned by a checkpoint.

use super::cache::MetadataSegmentCache;
use super::error::ManifestLoadError;
use super::load::{
    ensure_manifest_reference_matches, head_from_manifest,
    load_verified_manifest_segments_with_cache,
};
use super::record::load_checkpoint_record;
use super::scan::VerifiedMetadataSegments;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::basis::MetadataBasis;
use loonfs_api::wire::control::{CheckpointRecordState, CheckpointStatus, HeadState, ManifestRef};
use loonfs_api::{CheckpointId, NamespaceId};
use loonfs_objectstore::ObjectStore;

/// The manifest a checkpoint pins, loaded and verified.
pub(crate) struct PinnedCheckpointBasis<'a, S: ObjectStore + ?Sized> {
    /// The manifest reference stored in the checkpoint.
    pub(crate) manifest: ManifestRef,
    pub(crate) segments: VerifiedMetadataSegments<'a, S>,
}

/// The namespace state captured by a checkpoint.
#[derive(Debug, Clone)]
pub struct CheckpointReadBasis {
    /// The namespace head as of the captured sequence.
    pub head: HeadState,
    /// The pinned manifest checksum, used as the read-cache key.
    pub head_etag: String,
    /// The manifest the checkpoint pins.
    pub basis: MetadataBasis,
}

/// Loads the manifest `checkpoint_id` pins.
pub(crate) async fn load_pinned_checkpoint_basis<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
) -> Result<PinnedCheckpointBasis<'a, S>> {
    let record = load_pinning_checkpoint_record(store, namespace_id, checkpoint_id).await?;
    load_pinned_checkpoint_basis_from_record(store, segment_cache, record).await
}

pub(crate) async fn load_pinned_checkpoint_basis_from_record<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    record: CheckpointRecordState,
) -> Result<PinnedCheckpointBasis<'a, S>> {
    let checkpoint_id = &record.checkpoint_id;
    let namespace_id = &record.namespace_id;
    let segments = load_verified_manifest_segments_with_cache(
        store,
        segment_cache,
        namespace_id,
        &record.manifest.manifest_object_id,
    )
    .await
    .map_err(|error| match error {
        ManifestLoadError::MissingManifest { object_key } => CoreError::CheckpointUnavailable(
            format!("checkpoint `{checkpoint_id}` pins manifest `{object_key}`, which is gone"),
        ),
        other => CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(other)),
    })?;
    ensure_manifest_reference_matches(
        &format!("checkpoint `{checkpoint_id}` basis"),
        &record.manifest,
        segments.manifest(),
    )?;
    Ok(PinnedCheckpointBasis {
        manifest: record.manifest,
        segments,
    })
}

/// Loads the namespace state pinned by `checkpoint_id`.
///
/// Namespace identity and lifecycle fields come from `live_head`. Sequence
/// data comes from the checkpoint's manifest, so later WAL entries are not
/// replayed. Missing or released checkpoints return `checkpoint_unavailable`.
pub async fn load_checkpoint_read_basis<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    live_head: &HeadState,
    checkpoint_id: &CheckpointId,
) -> Result<CheckpointReadBasis> {
    let record =
        load_pinning_checkpoint_record(store, &live_head.namespace_id, checkpoint_id).await?;
    load_checkpoint_read_basis_from_record(store, segment_cache, live_head, record).await
}

pub(crate) async fn load_checkpoint_read_basis_from_record<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    live_head: &HeadState,
    record: CheckpointRecordState,
) -> Result<CheckpointReadBasis> {
    let PinnedCheckpointBasis { manifest, segments } =
        load_pinned_checkpoint_basis_from_record(store, segment_cache, record).await?;
    let envelope = segments.manifest();
    Ok(CheckpointReadBasis {
        head: head_from_manifest(live_head, envelope),
        head_etag: envelope.payload_checksum.clone(),
        basis: MetadataBasis::Manifest(manifest),
    })
}

/// Loads an active checkpoint record.
async fn load_pinning_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
) -> Result<CheckpointRecordState> {
    let Some(record) = load_checkpoint_record(store, namespace_id, checkpoint_id)
        .await?
        .map(|loaded| loaded.state)
    else {
        return Err(CoreError::CheckpointUnavailable(format!(
            "checkpoint `{checkpoint_id}` does not exist in namespace `{namespace_id}`"
        )));
    };
    if record.status != (CheckpointStatus::Active {}) {
        return Err(CoreError::CheckpointUnavailable(format!(
            "checkpoint `{checkpoint_id}` is `{}` and no longer pins its basis",
            record.status
        )));
    }
    Ok(record)
}
