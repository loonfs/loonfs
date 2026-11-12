//! Reads the state kept by a snapshot.

use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::manifest::{head_from_manifest, load_manifest_segments, ManifestLoadError};
use crate::namespace::basis::{MetadataBasis, ReadBasis};
use crate::namespace::state::NamespaceReadState;
use crate::segment::{MetadataSegmentCache, VerifiedMetadataSegments};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ManifestRef, SnapshotPayload};

/// The manifest a snapshot protects, loaded and verified.
pub(crate) struct SnapshotBasis<'a, S: ObjectStore + ?Sized> {
    /// The manifest reference stored in the snapshot.
    pub(crate) manifest: ManifestRef,
    pub(crate) segments: VerifiedMetadataSegments<'a, S>,
}

pub(crate) async fn load_snapshot_basis<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: &'a MetadataSegmentCache,
    record: SnapshotPayload,
) -> Result<SnapshotBasis<'a, S>> {
    let snapshot_id = &record.snapshot_id;
    let manifest = record.manifest();
    let segments = load_manifest_segments(store, Some(segment_cache), &manifest)
        .await
        .map_err(|error| match error {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(
                ManifestLoadError::MissingManifest { object_key },
            )) => CoreError::SnapshotUnavailable(format!(
                "snapshot `{snapshot_id}` refers to manifest `{object_key}`, which is gone"
            )),
            other => other,
        })?;
    Ok(SnapshotBasis { manifest, segments })
}

/// Loads the namespace state that the snapshot `record` captured.
///
/// Namespace identity and lifecycle fields come from `live_head`. Sequence
/// data comes from the snapshot's manifest, so later WAL entries are not
/// replayed. A snapshot whose manifest is gone returns `snapshot_unavailable`.
pub(crate) async fn load_snapshot_read_basis_from_record<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: &MetadataSegmentCache,
    live_head: &NamespaceReadState,
    record: SnapshotPayload,
) -> Result<ReadBasis> {
    let SnapshotBasis { manifest, segments } =
        load_snapshot_basis(store, segment_cache, record).await?;
    let envelope = segments.manifest();
    Ok(ReadBasis {
        head: head_from_manifest(live_head, envelope),
        basis: MetadataBasis::new(manifest),
    })
}
