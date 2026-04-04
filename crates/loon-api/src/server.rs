use crate::{
    ChangeSeq, ClientMutationRequest, ClientMutationResponse, HeadState, InodeId, InodeKind,
    LeaseState, NamespaceId, ObservedRemoteInode, RevisionNo,
};
use serde::{Deserialize, Serialize};

// --- Wire types for client/server protocol ---

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceBootstrapParams {
    pub holder_id: String,
    pub writer_version: String,
    pub now_ms: u64,
    pub lease_duration_ms: u64,
    pub allow_existing: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrappedNamespace {
    pub namespace_id: NamespaceId,
    pub created: bool,
    pub head: HeadState,
    pub lease: LeaseState,
    pub checkpoint_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceStateSummary {
    pub namespace_id: NamespaceId,
    pub head: HeadState,
    pub lease: LeaseState,
    pub checkpoint_basis: Option<NamespaceCheckpointBasisSummary>,
    pub wal_tail: NamespaceWalTailSummary,
    pub metadata: NamespaceMetadataSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceCheckpointBasisSummary {
    pub checkpoint_seq: ChangeSeq,
    pub manifest_object_key: String,
    pub table_object_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceWalTailSummary {
    pub object_count: usize,
    pub first_seq: Option<ChangeSeq>,
    pub last_seq: Option<ChangeSeq>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceMetadataSummary {
    pub inode_count: usize,
    pub visible_inode_count: usize,
    pub direntry_count: usize,
    pub revision_count: usize,
    pub subtree_tombstone_count: usize,
}

// --- Authoritative file types ---

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativePathEntry {
    pub namespace_id: NamespaceId,
    pub absolute_path: String,
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub authoritative_head_seq: ChangeSeq,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub revision_no: Option<RevisionNo>,
    pub size_bytes: Option<u64>,
    pub content_digest: Option<String>,
    pub content_manifest_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeFileBytes {
    pub entry: AuthoritativePathEntry,
    pub bytes: Vec<u8>,
}

// --- ServerTransport trait ---

/// Trait abstracting the client's view of the server.
///
/// In local mode, the implementation calls `loon-server` functions in-process.
/// In remote mode, the implementation makes HTTP requests.
pub trait ServerTransport {
    type Error: std::error::Error + Send + Sync + 'static;

    fn execute_mutation(
        &self,
        request: &ClientMutationRequest,
    ) -> Result<ClientMutationResponse, Self::Error>;

    fn load_namespace_state_summary(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceStateSummary, Self::Error>;

    fn load_remote_observations(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<(ChangeSeq, Vec<ObservedRemoteInode>), Self::Error>;

    fn bootstrap_namespace(
        &self,
        namespace_id: &NamespaceId,
        params: &NamespaceBootstrapParams,
    ) -> Result<BootstrappedNamespace, Self::Error>;

    fn list_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<Vec<AuthoritativePathEntry>, Self::Error>;

    fn resolve_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativePathEntry, Self::Error>;

    fn read_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativeFileBytes, Self::Error>;
}
