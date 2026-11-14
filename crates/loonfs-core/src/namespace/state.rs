//! Current namespace state derived from a manifest and its numbered WAL.

use loonfs_types::format::control::{ForkBasis, NamespaceStatus, WriterBlock};
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{ActorId, CommitSeq, NamespaceAccess, NamespaceId, PoolId, WalNo, WriterEpoch};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceReadState {
    pub namespace_id: NamespaceId,
    pub created_at_ms: u64,
    pub created_by: ActorId,
    pub access: NamespaceAccess,
    pub fork_basis: Option<ForkBasis>,
    pub pool_id: PoolId,
    pub status: NamespaceStatus,
    pub writer_epoch: WriterEpoch,
    pub writer: Option<WriterBlock>,
    pub seq: CommitSeq,
    pub wal_no: WalNo,
    pub folded_wal_no: WalNo,
    /// The retention floor of the view at this head: the manifest's
    /// `retention_floor_seq`, raised by each floor object in the WAL tail
    /// that replay read to reach the head (format spec, "Replaying the
    /// visible WAL").
    pub retention_floor_seq: CommitSeq,
}

impl NamespaceReadState {
    /// Returns the head after the WAL object `payload`, whose floor is the
    /// higher of this head's floor and the floor that a floor object records.
    pub(crate) fn after_wal_object(
        &self,
        payload: &loonfs_types::format::wal::WalObjectPayload,
    ) -> Self {
        Self {
            seq: payload.head_seq,
            wal_no: payload.wal_no,
            retention_floor_seq: payload
                .retention_floor_seq
                .map_or(self.retention_floor_seq, |floor| {
                    floor.max(self.retention_floor_seq)
                }),
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
            pool_id: manifest.pool_id.clone(),
            status: manifest.status,
            writer_epoch: manifest.writer_epoch,
            writer: manifest.writer.clone(),
            seq: manifest.head_seq,
            wal_no: manifest.folded_wal_no,
            folded_wal_no: manifest.folded_wal_no,
            retention_floor_seq: manifest.retention_floor_seq,
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl NamespaceReadState {
    /// Returns the state of an active namespace without commits, for a test
    /// that builds a head without a create call.
    pub fn initial(namespace_id: NamespaceId, created_at_ms: u64, created_by: ActorId) -> Self {
        Self::from(
            &NamespaceManifestPayload::initial(
                namespace_id,
                PoolId::parse("pool_00000000000000000000000001")
                    .expect("a static pool ID should parse"),
                created_at_ms,
                created_by,
                loonfs_types::NamespaceAccess::Unrestricted {},
            )
            .activated(),
        )
    }
}
