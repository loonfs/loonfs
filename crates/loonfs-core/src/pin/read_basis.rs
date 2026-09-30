//! Reads the state pinned by a checkpoint.

use super::record::{load_owned_pin, PinOwnerKind};
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::manifest::{
    head_from_manifest, load_manifest_segments, ManifestLoadError, MetadataSegmentCache,
    VerifiedMetadataSegments,
};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::state::NamespaceReadState;
use loonfs_api::wire::control::{ManifestRef, PinPayload};
use loonfs_api::{NamespaceId, PinId};
use loonfs_objectstore::ObjectStore;

/// The manifest a pin holds, loaded and verified.
pub(crate) struct PinBasis<'a, S: ObjectStore + ?Sized> {
    /// The manifest reference stored in the pin.
    pub(crate) manifest: ManifestRef,
    pub(crate) segments: VerifiedMetadataSegments<'a, S>,
}

/// The namespace state captured by a checkpoint.
#[derive(Debug, Clone)]
pub struct CheckpointReadBasis {
    /// The namespace head as of the captured sequence.
    pub head: NamespaceReadState,
    /// The manifest the checkpoint pins.
    pub basis: MetadataBasis,
}

/// Loads the manifest the user pin `pin_id` holds.
pub(crate) async fn load_user_pin_basis<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    pin_id: &PinId,
) -> Result<PinBasis<'a, S>> {
    let record = load_owned_pin(store, namespace_id, pin_id, PinOwnerKind::User)
        .await?
        .state;
    load_pin_basis(store, segment_cache, record).await
}

pub(crate) async fn load_pin_basis<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    record: PinPayload,
) -> Result<PinBasis<'a, S>> {
    let pin_id = &record.pin_id;
    let manifest = record.manifest();
    let segments = load_manifest_segments(store, segment_cache, &manifest)
        .await
        .map_err(|error| match error {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(
                ManifestLoadError::MissingManifest { object_key },
            )) => CoreError::CheckpointUnavailable(format!(
                "checkpoint `{pin_id}` pins manifest `{object_key}`, which is gone"
            )),
            other => other,
        })?;
    Ok(PinBasis { manifest, segments })
}

/// Loads the namespace state pinned by `checkpoint_id`.
///
/// Namespace identity and lifecycle fields come from `live_head`. Sequence
/// data comes from the checkpoint's manifest, so later WAL entries are not
/// replayed. An id that names no user pin returns `checkpoint_not_found`, and
/// a pin whose manifest is gone returns `checkpoint_unavailable`.
pub async fn load_checkpoint_read_basis<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    live_head: &NamespaceReadState,
    checkpoint_id: &PinId,
) -> Result<CheckpointReadBasis> {
    let record = load_owned_pin(
        store,
        &live_head.namespace_id,
        checkpoint_id,
        PinOwnerKind::User,
    )
    .await?
    .state;
    load_checkpoint_read_basis_from_record(store, segment_cache, live_head, record).await
}

pub(crate) async fn load_checkpoint_read_basis_from_record<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    live_head: &NamespaceReadState,
    record: PinPayload,
) -> Result<CheckpointReadBasis> {
    let PinBasis { manifest, segments } = load_pin_basis(store, segment_cache, record).await?;
    let envelope = segments.manifest();
    Ok(CheckpointReadBasis {
        head: head_from_manifest(live_head, envelope),
        basis: MetadataBasis(manifest),
    })
}
