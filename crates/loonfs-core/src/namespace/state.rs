//! Current namespace state derived from a manifest and its numbered WAL.

use loonfs_api::wire::control::{ForkBasis, NamespaceStatus, WriterBlock};
use loonfs_api::wire::manifest::NamespaceManifestPayload;
use loonfs_api::{
    ActorId, ChangeSeq, CommitId, ContentStoreId, InodeId, NamespaceAccess, NamespaceId, WalNo,
    WriterEpoch,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceReadState {
    pub namespace_id: NamespaceId,
    pub content_store_id: ContentStoreId,
    pub created_at_ms: u64,
    pub created_by: ActorId,
    pub access: NamespaceAccess,
    pub fork_basis: Option<ForkBasis>,
    pub status: NamespaceStatus,
    pub writer_epoch: WriterEpoch,
    pub writer: Option<WriterBlock>,
    pub seq: ChangeSeq,
    pub head_commit_id: CommitId,
    pub next_inode_id: InodeId,
    pub wal_no: WalNo,
    pub last_folded_wal_no: WalNo,
}

impl NamespaceReadState {
    /// Counts the WAL segments after the last folded position, including fences.
    pub fn unfolded_wal_segments(&self) -> u64 {
        self.wal_no.0 - self.last_folded_wal_no.0
    }
}

impl From<&NamespaceManifestPayload> for NamespaceReadState {
    fn from(manifest: &NamespaceManifestPayload) -> Self {
        Self {
            namespace_id: manifest.namespace_id.clone(),
            content_store_id: manifest.content_store_id.clone(),
            created_at_ms: manifest.created_at_ms,
            created_by: manifest.created_by.clone(),
            access: manifest.access.clone(),
            fork_basis: manifest.fork_basis.clone(),
            status: manifest.status,
            writer_epoch: manifest.writer_epoch,
            writer: manifest.writer.clone(),
            seq: manifest.head_seq,
            head_commit_id: manifest.head_commit_id.clone(),
            next_inode_id: manifest.next_inode_id,
            wal_no: manifest.last_folded_wal_no,
            last_folded_wal_no: manifest.last_folded_wal_no,
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl NamespaceReadState {
    pub fn initial(
        namespace_id: NamespaceId,
        content_store_id: ContentStoreId,
        created_at_ms: u64,
        created_by: ActorId,
    ) -> Self {
        Self::from(&NamespaceManifestPayload::initial(
            namespace_id,
            content_store_id,
            created_at_ms,
            created_by,
            loonfs_api::NamespaceAccess::Unrestricted {},
        ))
    }
}
