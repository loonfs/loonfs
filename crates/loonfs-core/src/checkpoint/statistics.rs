//! Activity and referenced storage at one immutable manifest.

use super::block_fetch::segment_object_len;
use super::publish::manifest_ref_for;
use super::read_basis::load_pinned_checkpoint_basis;
use crate::error::{CoreError, Result};
use crate::namespace::control::{load_current_manifest, LoadedManifest};
use loonfs_api::wire::control::{ForkBasis, ManifestRef, NamespaceStatus};
use loonfs_api::wire::manifest::{ManifestActivity, MetadataRowFamily, NamespaceManifestEnvelope};
use loonfs_api::{CheckpointId, NamespaceId, WalNo};
use loonfs_objectstore::ObjectStore;

/// Statistics through the selected manifest's folded head. Newer WAL commits
/// are excluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceStatistics {
    /// Namespace, manifest number, head sequence, and checksum of this observation.
    pub manifest: ManifestRef,
    /// Immutable namespace creation time in Unix milliseconds.
    pub created_at_ms: u64,
    /// Lifecycle at this manifest, including whether these are final totals.
    pub status: NamespaceStatus,
    /// Highest namespace-local WAL number included in this observation.
    pub last_folded_wal_no: WalNo,
    /// Activity committed in this namespace.
    pub activity: ManifestActivity,
    /// Explicit retained inode records. An implicit genesis root counts as zero.
    pub inode_record_count: u64,
    /// Stored lengths of referenced metadata segments. Shared objects count in
    /// each referencing manifest; this is not unique physical storage.
    pub metadata_stored_bytes: u64,
    /// Exact source manifest at fork creation, if this namespace is a fork.
    pub fork_basis: Option<ForkBasis>,
}

impl LoadedManifest {
    /// Calculates statistics from the loaded manifest without storage requests.
    pub fn statistics(&self) -> Result<NamespaceStatistics> {
        manifest_statistics(&self.envelope)
    }
}

/// Reads the current manifest's statistics without folding or scanning the WAL,
/// metadata segments, content, or pins. Deleted namespaces retain final totals.
pub async fn load_namespace_statistics<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<NamespaceStatistics> {
    load_current_manifest(store, namespace_id)
        .await?
        .statistics()
}

/// Reads the exact manifest referenced by an active checkpoint, including an
/// older snapshot. Later namespace activity is excluded.
pub async fn load_checkpoint_statistics<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
) -> Result<NamespaceStatistics> {
    let pinned = load_pinned_checkpoint_basis(store, None, namespace_id, checkpoint_id).await?;
    manifest_statistics(pinned.segments.manifest())
}

fn manifest_statistics(manifest: &NamespaceManifestEnvelope) -> Result<NamespaceStatistics> {
    let payload = manifest.payload();
    // Both loaders validate descriptor uniqueness and byte ranges before this call.
    let mut inode_record_count = 0_u64;
    let mut metadata_stored_bytes = 0_u64;
    for segment in payload.runs.iter().flat_map(|run| &run.segments) {
        metadata_stored_bytes = metadata_stored_bytes
            .checked_add(segment_object_len(segment))
            .ok_or_else(|| {
                CoreError::NamespaceCorrupt("metadata byte count overflow".to_owned())
            })?;
        if segment.family == MetadataRowFamily::Inodes {
            inode_record_count = inode_record_count
                .checked_add(segment.row_count)
                .ok_or_else(|| {
                    CoreError::NamespaceCorrupt("inode record count overflow".to_owned())
                })?;
        }
    }
    Ok(NamespaceStatistics {
        manifest: manifest_ref_for(&payload.namespace_id, manifest),
        created_at_ms: payload.created_at_ms,
        status: payload.status,
        last_folded_wal_no: payload.last_folded_wal_no,
        activity: payload.activity,
        inode_record_count,
        metadata_stored_bytes,
        fork_basis: payload.fork_basis.clone(),
    })
}
