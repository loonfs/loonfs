//! A manifest that a writer session's commit engine moves its position
//! onto (one that this runtime published or found current, or one that a
//! read found above the position), and the rule for a manifest at the head
//! of an earlier one.

use crate::namespace::basis::MetadataBasis;
use crate::namespace::state::NamespaceReadState;
use crate::time::Observation;
use loonfs_types::format::control::{ManifestRef, NamespaceStatus};
use loonfs_types::format::manifest::NamespaceManifestEnvelope;
use loonfs_types::{ChangeSeq, WalNo, WriterEpoch};

/// A manifest above a writer session's position, with the fields that
/// decide whether the position can move onto it.
///
/// The session's commit engine moves its position onto the manifest instead
/// of reloading the namespace (see
/// [`NamespaceCommitEngine::record_manifest_published`](crate::publish::NamespaceCommitEngine::record_manifest_published)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedManifest {
    pub manifest: ManifestRef,
    pub writer_epoch: WriterEpoch,
    pub status: NamespaceStatus,
    /// The WAL number that the manifest folds through.
    pub folded_wal_no: WalNo,
    /// The manifest's retention floor, which a position takes when it moves
    /// onto the manifest.
    pub retention_floor_seq: ChangeSeq,
    /// When this runtime started the call that published the manifest, or
    /// that found it current. No manifest followed it then, so a position
    /// that moves onto it takes this as its last successor check. `None`
    /// for a manifest that a read found above the position, because the read
    /// records its own check.
    pub started: Option<Observation>,
}

impl PublishedManifest {
    pub(crate) fn new(manifest: &NamespaceManifestEnvelope, started: Option<Observation>) -> Self {
        let payload = manifest.payload();
        Self {
            manifest: super::publish::manifest_ref_for(&payload.namespace_id, manifest),
            writer_epoch: payload.writer_epoch,
            status: payload.status,
            folded_wal_no: payload.folded_wal_no,
            retention_floor_seq: payload.retention_floor_seq,
            started,
        }
    }
}

/// Whether `manifest`, a manifest above `basis`, is at the basis's head: an
/// active manifest with the same writer epoch that folds the WAL through
/// the same number, to the same commit. `head` is a head above `basis`, so
/// it has the basis's folded WAL number and writer epoch.
///
/// Such a manifest is a compaction, a compactor claim, or a retention
/// advance over `basis`, or several of them, because another writer's claim
/// changes the writer epoch, a fold changes the folded WAL number, and a
/// tombstone changes the status. Those three publications copy every field
/// of their predecessor that replay reads (`head_seq`, `folded_wal_no`, and
/// `next_inode_id`), and they keep whether the manifest lists runs, which
/// decides whether the tail includes the root directory (replay adds it
/// above a manifest that lists none). So the WAL tail projected above
/// `basis` is also the tail above `manifest`. (A compaction needs runs to
/// merge, and it keeps every live row, including the root directory's.)
pub(crate) fn same_head(
    basis: &MetadataBasis,
    head: &NamespaceReadState,
    manifest: &PublishedManifest,
) -> bool {
    !manifest.status.is_deleted()
        && manifest.writer_epoch == head.writer_epoch
        && manifest.folded_wal_no == head.folded_wal_no
        && manifest.manifest.head_seq == basis.0.head_seq
}

/// A maintenance call's response, and the manifest the call published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationReport<T> {
    pub response: T,
    /// The manifest this call published, or `None` when it published none
    /// (for example, a fold of a tail that a manifest already covered).
    pub published: Option<PublishedManifest>,
}

impl<T> PublicationReport<T> {
    /// Replaces the response and keeps the published manifest.
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> PublicationReport<U> {
        PublicationReport {
            response: map(self.response),
            published: self.published,
        }
    }
}
