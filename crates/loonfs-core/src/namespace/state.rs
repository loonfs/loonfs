//! Current namespace state derived from a manifest and its numbered WAL.

use loonfs_types::format::control::{ForkBasis, NamespaceStatus, WriterBlock};
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{ActorId, ChangeSeq, NamespaceAccess, NamespaceId, WalNo, WriterEpoch};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceReadState {
    pub namespace_id: NamespaceId,
    pub created_at_ms: u64,
    pub created_by: ActorId,
    pub access: NamespaceAccess,
    pub fork_basis: Option<ForkBasis>,
    pub status: NamespaceStatus,
    pub writer_epoch: WriterEpoch,
    pub writer: Option<WriterBlock>,
    pub seq: ChangeSeq,
    pub wal_no: WalNo,
    pub folded_wal_no: WalNo,
    /// The lowest basis floor that the WAL object after `wal_no` can
    /// record: the basis floor of the WAL object at `wal_no`, or the
    /// manifest's acknowledged floor when no WAL object is above the
    /// manifest. Basis floors never decrease along the WAL (see the design
    /// note's [Acknowledging the floor](https://github.com/loonfs/loonfs/blob/main/docs/design/retention-window.md#acknowledging-the-floor)).
    pub basis_floor_seq: ChangeSeq,
}

impl NamespaceReadState {
    pub(crate) fn after_wal_object(
        &self,
        payload: &loonfs_types::format::wal::WalObjectPayload,
    ) -> Self {
        Self {
            seq: payload.head_seq,
            wal_no: payload.wal_no,
            basis_floor_seq: payload.basis_floor_seq,
            ..self.clone()
        }
    }

    /// Counts the WAL objects after the last folded position, including fences.
    pub fn unfolded_wal_objects(&self) -> u64 {
        self.wal_no.0 - self.folded_wal_no.0
    }
}

impl From<&NamespaceManifestPayload> for NamespaceReadState {
    fn from(manifest: &NamespaceManifestPayload) -> Self {
        Self {
            namespace_id: manifest.namespace_id.clone(),
            created_at_ms: manifest.created_at_ms,
            created_by: manifest.created_by.clone(),
            access: manifest.access.clone(),
            fork_basis: manifest.fork_basis.clone(),
            status: manifest.status,
            writer_epoch: manifest.writer_epoch,
            writer: manifest.writer.clone(),
            seq: manifest.head_seq,
            wal_no: manifest.folded_wal_no,
            folded_wal_no: manifest.folded_wal_no,
            basis_floor_seq: manifest.acknowledged_floor_seq,
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl NamespaceReadState {
    pub fn initial(namespace_id: NamespaceId, created_at_ms: u64, created_by: ActorId) -> Self {
        Self::from(&NamespaceManifestPayload::initial(
            namespace_id,
            created_at_ms,
            created_by,
            loonfs_types::NamespaceAccess::Unrestricted {},
        ))
    }
}
