//! Reads the state kept by a snapshot.

use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::manifest::{
    head_from_manifest, load_referenced_manifest, DecodedManifest, ManifestLoadError,
};
use crate::namespace::basis::{MetadataBasis, ReadBasis};
use crate::namespace::state::NamespaceReadState;
use crate::segment::MetadataSegmentCache;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ManifestRef, SnapshotPayload};

/// The manifest a snapshot protects, loaded and verified.
pub(crate) struct SnapshotBasis {
    /// The manifest reference stored in the snapshot.
    pub(crate) manifest: ManifestRef,
    pub(crate) decoded: DecodedManifest,
}

pub(crate) async fn load_snapshot_basis<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: &MetadataSegmentCache,
    record: SnapshotPayload,
) -> Result<SnapshotBasis> {
    let snapshot_id = &record.snapshot_id;
    let manifest = record.manifest();
    let decoded = load_referenced_manifest(store, Some(segment_cache), &manifest)
        .await
        .map_err(|error| match error {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(
                ManifestLoadError::MissingManifest { object_key },
            )) => CoreError::SnapshotManifestMissing {
                snapshot_id: snapshot_id.clone(),
                object_key,
            },
            other => other,
        })?;
    Ok(SnapshotBasis { manifest, decoded })
}

/// Loads the namespace state that the snapshot `record` captured.
///
/// Namespace identity and lifecycle fields come from `live_head`. Sequence
/// data comes from the snapshot's manifest, so later WAL entries are not
/// replayed. A snapshot whose manifest is gone returns `snapshot_not_found`
/// with the cause `manifest_missing`.
pub(crate) async fn load_snapshot_read_basis_from_record<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: &MetadataSegmentCache,
    live_head: &NamespaceReadState,
    record: SnapshotPayload,
) -> Result<ReadBasis> {
    let SnapshotBasis { manifest, decoded } =
        load_snapshot_basis(store, segment_cache, record).await?;
    let envelope = &decoded.manifest.envelope;
    Ok(ReadBasis {
        head: head_from_manifest(live_head, envelope),
        basis: MetadataBasis::new(manifest),
    })
}
