//! Reads the state pinned by a snapshot.

use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::manifest::{
    head_from_manifest, load_manifest_segments, ManifestLoadError, MetadataSegmentCache,
    VerifiedMetadataSegments,
};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::state::NamespaceReadState;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ManifestRef, PinPayload};

/// The manifest a pin holds, loaded and verified.
pub(crate) struct PinBasis<'a, S: ObjectStore + ?Sized> {
    /// The manifest reference stored in the pin.
    pub(crate) manifest: ManifestRef,
    pub(crate) segments: VerifiedMetadataSegments<'a, S>,
}

/// The namespace state captured by a snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotReadBasis {
    /// The namespace head as of the captured sequence.
    pub head: NamespaceReadState,
    /// The manifest the snapshot pins.
    pub basis: MetadataBasis,
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
                "pin `{pin_id}` refers to manifest `{object_key}`, which is gone"
            )),
            other => other,
        })?;
    Ok(PinBasis { manifest, segments })
}

/// Loads the namespace state that the pin `record` captured.
///
/// Namespace identity and lifecycle fields come from `live_head`. Sequence
/// data comes from the pin's manifest, so later WAL entries are not
/// replayed. A pin whose manifest is gone returns `checkpoint_unavailable`.
pub(crate) async fn load_snapshot_read_basis_from_record<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    live_head: &NamespaceReadState,
    record: PinPayload,
) -> Result<SnapshotReadBasis> {
    let PinBasis { manifest, segments } = load_pin_basis(store, segment_cache, record).await?;
    let envelope = segments.manifest();
    Ok(SnapshotReadBasis {
        head: head_from_manifest(live_head, envelope),
        basis: MetadataBasis(manifest),
    })
}
