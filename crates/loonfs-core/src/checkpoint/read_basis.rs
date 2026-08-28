//! Resolving a checkpoint id into the state it pinned.
//!
//! A checkpoint pins one immutable manifest. This module loads the record,
//! refuses the lifecycle that no longer pins a basis, and verifies the
//! manifest against every coordinate the record carries for it. Consumers
//! either scan the pinned segments directly or pin a read context to the
//! head the manifest describes; neither consults the live head, the WAL, or
//! any later manifest.

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
    /// Coordinates the record pins, including the sequence it captured.
    pub(crate) manifest: ManifestRef,
    pub(crate) segments: VerifiedMetadataSegments<'a, S>,
}

/// The head and metadata basis that describe a namespace exactly as one
/// checkpoint captured it.
///
/// This type supports the `loonfs` runtime, which pairs it with its own
/// caches to build a pinned read context.
#[derive(Debug, Clone)]
pub struct CheckpointReadBasis {
    /// The namespace head as of the captured sequence.
    pub head: HeadState,
    /// Identity of the immutable object this view reads, for keying read
    /// projections. A checkpoint pins a manifest rather than a head, so the
    /// manifest's payload checksum is what stands still here.
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

/// Resolves the read basis `checkpoint_id` pins.
///
/// `live_head` supplies only what the namespace carries for its whole life:
/// its identity, its content store, and its current lifecycle status. Every
/// sequence-bearing field comes from the pinned manifest, so a read over the
/// returned pair replays no WAL and answers the captured state. A missing
/// record, a released record, or a basis that no longer loads answers
/// `checkpoint_unavailable` instead of the current head.
pub async fn load_checkpoint_read_basis<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    live_head: &HeadState,
    checkpoint_id: &CheckpointId,
) -> Result<CheckpointReadBasis> {
    let PinnedCheckpointBasis { manifest, segments } =
        load_pinned_checkpoint_basis(store, segment_cache, &live_head.namespace_id, checkpoint_id)
            .await?;
    let envelope = segments.manifest();
    Ok(CheckpointReadBasis {
        head: head_from_manifest(live_head, envelope),
        head_etag: envelope.payload_checksum.clone(),
        basis: MetadataBasis::Manifest(manifest),
    })
}

/// Loads the record and refuses the one lifecycle that no longer pins its
/// basis, so a caller never reads state garbage collection may already be
/// reclaiming.
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
