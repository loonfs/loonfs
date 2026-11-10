//! A manifest that a writer session's commit engine moves its position
//! onto (one that this runtime published or found current, or one that a
//! read found above the position), and the rule for a manifest at the head
//! of an earlier one.

use crate::namespace::basis::{MetadataBasis, ReadBasis};
use crate::namespace::state::NamespaceReadState;
use crate::time::Observation;
use crate::wal::ProjectedWalTail;
use loonfs_types::format::control::{ManifestRef, NamespaceStatus};
use loonfs_types::format::manifest::NamespaceManifestEnvelope;
use loonfs_types::{ChangeSeq, WalNo, WriterEpoch};
use std::sync::Arc;

/// A manifest above a writer session's position, with the fields that
/// determine whether the position can move onto it.
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
            manifest: super::publish::manifest_ref_for(manifest),
            writer_epoch: payload.writer_epoch,
            status: payload.status.clone(),
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
/// Such a manifest is a compaction, a compactor claim, a retention advance,
/// a retention window change, or an extension's publication over `basis`,
/// or several of them, because another writer's claim changes the writer
/// epoch, a fold changes the folded WAL number, and a tombstone changes the
/// status. Those five publications copy every field of their predecessor
/// that replay reads (`head_seq` and `folded_wal_no`), and they keep
/// whether the manifest lists runs. Replay adds the root directory above a
/// manifest that lists no runs. So the WAL tail projected above `basis` is
/// also the tail above `manifest`. (A compaction needs runs to
/// compact, and it keeps every live row, including the root directory's.)
pub(crate) fn same_head(
    basis: &MetadataBasis,
    head: &NamespaceReadState,
    manifest: &PublishedManifest,
) -> bool {
    !manifest.status.is_deleted()
        && manifest.writer_epoch == head.writer_epoch
        && manifest.folded_wal_no == head.folded_wal_no
        && manifest.manifest.head_seq == basis.manifest().head_seq
}

/// Moves `read_basis` and `tail`, the WAL tail projected above the basis,
/// onto `manifest`, a manifest above the basis, and returns `false` when the
/// head can't move there.
///
/// A manifest at the basis's head (see [`same_head`]) keeps the tail. A fold
/// moves the head when it's active, has the head's writer epoch, folds a WAL
/// number above the head's folded number and at most its head's, and its
/// `head_seq` is the last commit of a chunk of the tail. The tail above the
/// fold's manifest is then the chunks after its `head_seq`, because a fold
/// folds whole WAL objects, oldest first. For example, if a fold read the
/// WAL through the commit at sequence 10, and a session then wrote the
/// commits at 11 and 12, the tail above the fold's manifest is those two
/// commits. A fold's manifest lists runs, so the cut also drops the root
/// directory that replay adds above a manifest without runs. A manifest at
/// the basis's head keeps that directory, which is why it keeps the tail
/// instead of cutting it.
///
/// The rule doesn't depend on whether this runtime or another process
/// published the manifest, or on the order in which manifests arrive.
/// Writer epochs never decrease along manifests, so every manifest between
/// the basis and a later manifest with the head's writer epoch has that
/// epoch too, and none of them is another writer's claim. Each of them
/// folds the same WAL that the tail contains above the basis's folded
/// number, so the later manifest's folded WAL number and `head_seq` are
/// enough to find the tail above it, and the head can move onto it without
/// the manifests between. A manifest between the two that arrives later is
/// below the new basis, so it leaves the head where it is.
pub(crate) fn move_read_basis(
    read_basis: &mut ReadBasis,
    tail: &mut Arc<ProjectedWalTail>,
    manifest: &PublishedManifest,
) -> bool {
    let ReadBasis { head, basis } = read_basis;
    if !same_head(basis, head, manifest) {
        if manifest.status.is_deleted()
            || manifest.writer_epoch != head.writer_epoch
            || manifest.folded_wal_no <= head.folded_wal_no
            || manifest.folded_wal_no > head.wal_no
        {
            return false;
        }
        let Some(after) = tail.after_seq(manifest.manifest.head_seq, head.seq) else {
            return false;
        };
        head.folded_wal_no = manifest.folded_wal_no;
        *tail = Arc::new(after);
    }
    *basis = MetadataBasis::new(manifest.manifest.clone());
    true
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
