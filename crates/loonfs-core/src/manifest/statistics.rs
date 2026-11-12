//! Activity and referenced storage at one immutable manifest.

use super::publish::manifest_ref_for;
use crate::error::{CoreError, Result};
use crate::namespace::control::LoadedManifest;
use crate::namespace::store::NamespaceStore;
use crate::segment::index_block_load::segment_object_bytes;
use crate::snapshot::load_snapshot_basis;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ForkBasis, ManifestRef, NamespaceStatus};
use loonfs_types::format::manifest::{
    ManifestActivity, MetadataRowFamily, NamespaceManifestEnvelope,
};
use loonfs_types::{SnapshotId, WalNo};
use std::collections::BTreeMap;

/// Statistics through the selected manifest's folded head. Newer WAL commits
/// are excluded. Counters start at zero and comparisons are valid within one namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceStatistics {
    /// Namespace, manifest number, head sequence, and checksum of this observation.
    pub manifest: ManifestRef,
    /// Stored length of the manifest object read by the loader, which is the
    /// compressed document.
    pub manifest_bytes: u64,
    /// Segment descriptor counts across all runs, including families with none.
    pub segments_by_family: BTreeMap<MetadataRowFamily, u64>,
    /// Immutable namespace creation time in Unix milliseconds.
    pub created_at_ms: u64,
    /// Lifecycle at this manifest, including whether these are final totals.
    pub status: NamespaceStatus,
    /// Highest namespace-local WAL number included in this observation.
    pub folded_wal_no: WalNo,
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
        manifest_statistics(&self.envelope, self.manifest_bytes)
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Reads the current manifest's statistics without folding or scanning the WAL,
    /// metadata segments, content, or snapshots. Deleted namespaces retain final totals.
    pub async fn load_statistics(&self) -> Result<NamespaceStatistics> {
        self.load_current_manifest().await?.statistics()
    }

    /// Reads the exact manifest that a snapshot of either owner references.
    /// Later namespace activity is excluded. A missing snapshot returns
    /// `snapshot_not_found`.
    pub async fn load_snapshot_statistics(
        &self,
        snapshot_id: &SnapshotId,
    ) -> Result<NamespaceStatistics> {
        let Some(loaded) = self.load_snapshot_record(snapshot_id).await? else {
            return Err(CoreError::SnapshotNotFound {
                snapshot_id: snapshot_id.clone(),
            });
        };
        let basis = load_snapshot_basis(&self.store, &self.segment_cache, loaded.state).await?;
        manifest_statistics(basis.segments.manifest(), basis.segments.manifest_bytes)
    }
}

fn manifest_statistics(
    manifest: &NamespaceManifestEnvelope,
    manifest_bytes: u64,
) -> Result<NamespaceStatistics> {
    let payload = manifest.payload();
    // Both loaders validate descriptor uniqueness and byte ranges before this call.
    let mut inode_record_count = 0_u64;
    let mut metadata_stored_bytes = 0_u64;
    let mut segments_by_family: BTreeMap<_, _> = MetadataRowFamily::ALL
        .into_iter()
        .map(|family| (family, 0))
        .collect();
    for segment in payload.runs.iter().flat_map(|run| &run.segments) {
        *segments_by_family.entry(segment.family).or_default() += 1;
        metadata_stored_bytes = metadata_stored_bytes
            .checked_add(segment_object_bytes(segment))
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
        manifest: manifest_ref_for(manifest),
        manifest_bytes,
        segments_by_family,
        created_at_ms: payload.created_at_ms,
        status: payload.status,
        folded_wal_no: payload.folded_wal_no,
        activity: payload.activity,
        inode_record_count,
        metadata_stored_bytes,
        fork_basis: payload.fork_basis.clone(),
    })
}
