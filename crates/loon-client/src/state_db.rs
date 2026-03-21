use loon_types::{
    ChangeSeq, ClientMutationOp, ClientMutationRequest, ClientMutationResponse, InodeId, InodeKind,
    NamespaceId, RevisionNo,
};
use rusqlite::{Connection, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

mod loads;
mod schema;
mod txn;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use schema::SCHEMA_VERSION;

#[derive(Debug, Error)]
pub enum StateDbError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("unsupported client state schema version {0}")]
    UnsupportedSchemaVersion(i32),
    #[error("client mutation request JSON codec error: {0}")]
    ClientMutationRequestCodec(#[from] serde_json::Error),
    #[error("conflict/error detail JSON codec error: {0}")]
    ConflictOrErrorDetailCodec(serde_json::Error),
    #[error("SQLite integer out of range for {field}: {value}")]
    IntegerOutOfRange { field: &'static str, value: i64 },
    #[error("value out of range for SQLite {field}: {value}")]
    UnsignedOutOfRange { field: &'static str, value: u64 },
    #[error("unknown inode kind `{0}` in SQLite row")]
    UnknownInodeKind(String),
    #[error("unknown transfer direction `{0}` in SQLite row")]
    UnknownTransferDirection(String),
    #[error("unknown transfer state `{0}` in SQLite row")]
    UnknownTransferState(String),
    #[error("client state schema foreign-key violation after migration: {0}")]
    SchemaForeignKeyViolation(String),
    #[error("unsupported local-only inode kind `{0:?}`")]
    UnsupportedLocalOnlyInodeKind(InodeKind),
    #[error(
        "local_only_parent_missing: namespace `{namespace_id}` parent inode `{parent_inode_id}`"
    )]
    LocalOnlyParentMissing {
        namespace_id: String,
        parent_inode_id: u64,
    },
    #[error(
        "local_only_parent_not_directory: namespace `{namespace_id}` parent inode `{parent_inode_id}`"
    )]
    LocalOnlyParentNotDirectory {
        namespace_id: String,
        parent_inode_id: u64,
    },
    #[error(
        "local_only_parent_not_bound: namespace `{namespace_id}` parent inode `{parent_inode_id}`"
    )]
    LocalOnlyParentNotBound {
        namespace_id: String,
        parent_inode_id: u64,
    },
    #[error("local_only_file_missing: `{client_file_id}`")]
    LocalOnlyFileMissing { client_file_id: String },
    #[error("uploaded_content_missing: `{client_file_id}`")]
    UploadedContentMissing { client_file_id: String },
    #[error("uploaded_content_requires_file: `{client_file_id}` kind `{inode_kind}`")]
    UploadedContentRequiresFile {
        client_file_id: String,
        inode_kind: String,
    },
    #[error("uploaded_content_local_digest_missing: `{client_file_id}`")]
    UploadedContentLocalDigestMissing { client_file_id: String },
    #[error(
        "uploaded_content_namespace_mismatch: `{client_file_id}` local namespace `{local_namespace_id}` != uploaded namespace `{uploaded_namespace_id}`"
    )]
    UploadedContentNamespaceMismatch {
        client_file_id: String,
        local_namespace_id: String,
        uploaded_namespace_id: String,
    },
    #[error(
        "uploaded_content_digest_mismatch: `{client_file_id}` local digest `{local_content_digest}` != uploaded digest `{uploaded_file_digest}`"
    )]
    UploadedContentDigestMismatch {
        client_file_id: String,
        local_content_digest: String,
        uploaded_file_digest: String,
    },
    #[error("inode_upload_missing: namespace `{namespace_id}` inode `{inode_id}`")]
    InodeUploadMissing { namespace_id: String, inode_id: u64 },
    #[error("inode_upload_requires_file: namespace `{namespace_id}` inode `{inode_id}` kind `{inode_kind}`")]
    InodeUploadRequiresFile {
        namespace_id: String,
        inode_id: u64,
        inode_kind: String,
    },
    #[error("inode_upload_local_digest_missing: namespace `{namespace_id}` inode `{inode_id}`")]
    InodeUploadLocalDigestMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "inode_upload_namespace_mismatch: namespace `{namespace_id}` inode `{inode_id}` local namespace `{local_namespace_id}` != uploaded namespace `{uploaded_namespace_id}`"
    )]
    InodeUploadNamespaceMismatch {
        namespace_id: String,
        inode_id: u64,
        local_namespace_id: String,
        uploaded_namespace_id: String,
    },
    #[error(
        "inode_upload_digest_mismatch: namespace `{namespace_id}` inode `{inode_id}` local digest `{local_content_digest}` != uploaded digest `{uploaded_file_digest}`"
    )]
    InodeUploadDigestMismatch {
        namespace_id: String,
        inode_id: u64,
        local_content_digest: String,
        uploaded_file_digest: String,
    },
    #[error("pending_client_mutation_missing: `{client_request_id}`")]
    PendingClientMutationMissing { client_request_id: String },
    #[error(
        "pending_client_mutation_conflict: `{client_request_id}` existing temp `{existing_client_file_id}` != new temp `{new_client_file_id}`"
    )]
    PendingClientMutationConflict {
        client_request_id: String,
        existing_client_file_id: String,
        new_client_file_id: String,
    },
    #[error(
        "pending_client_mutation_client_file_conflict: `{client_file_id}` existing request `{existing_client_request_id}` != new request `{new_client_request_id}`"
    )]
    PendingClientMutationClientFileConflict {
        client_file_id: String,
        existing_client_request_id: String,
        new_client_request_id: String,
    },
    #[error(
        "pending_client_mutation_namespace_mismatch: `{client_request_id}` pending namespace `{pending_namespace_id}` != response namespace `{response_namespace_id}`"
    )]
    PendingClientMutationNamespaceMismatch {
        client_request_id: String,
        pending_namespace_id: String,
        response_namespace_id: String,
    },
    #[error("pending_client_mutation_request_missing: `{client_request_id}`")]
    PendingClientMutationRequestMissing { client_request_id: String },
    #[error("pending_inode_mutation_missing: `{client_request_id}`")]
    PendingInodeMutationMissing { client_request_id: String },
    #[error(
        "pending_inode_mutation_conflict: `{client_request_id}` existing inode `{existing_inode_id}` != new inode `{new_inode_id}`"
    )]
    PendingInodeMutationConflict {
        client_request_id: String,
        existing_inode_id: u64,
        new_inode_id: u64,
    },
    #[error(
        "pending_inode_mutation_inode_conflict: namespace `{namespace_id}` inode `{inode_id}` existing request `{existing_client_request_id}` != new request `{new_client_request_id}`"
    )]
    PendingInodeMutationInodeConflict {
        namespace_id: String,
        inode_id: u64,
        existing_client_request_id: String,
        new_client_request_id: String,
    },
    #[error(
        "pending_inode_mutation_namespace_mismatch: `{client_request_id}` pending namespace `{pending_namespace_id}` != response namespace `{response_namespace_id}`"
    )]
    PendingInodeMutationNamespaceMismatch {
        client_request_id: String,
        pending_namespace_id: String,
        response_namespace_id: String,
    },
    #[error("pending_inode_mutation_request_missing: `{client_request_id}`")]
    PendingInodeMutationRequestMissing { client_request_id: String },
    #[error("client_mutation_response_missing_result: `{client_request_id}`")]
    ClientMutationResponseMissingResult { client_request_id: String },
    #[error("client_mutation_response_conflicting_results: `{client_request_id}`")]
    ClientMutationResponseConflictingResults { client_request_id: String },
    #[error("upload_local_edit_state_missing: namespace `{namespace_id}` inode `{inode_id}`")]
    UploadLocalEditStateMissing { namespace_id: String, inode_id: u64 },
    #[error("upload_local_edit_requires_file: namespace `{namespace_id}` inode `{inode_id}` kind `{inode_kind}`")]
    UploadLocalEditRequiresFile {
        namespace_id: String,
        inode_id: u64,
        inode_kind: String,
    },
    #[error("upload_local_edit_path_change_not_supported: namespace `{namespace_id}` inode `{inode_id}` field `{field}` local `{local}` != anchor `{anchor}`")]
    UploadLocalEditPathChangeNotSupported {
        namespace_id: String,
        inode_id: u64,
        field: &'static str,
        local: String,
        anchor: String,
    },
    #[error("upload_local_edit_remote_not_converged: namespace `{namespace_id}` inode `{inode_id}` field `{field}` remote `{remote}` != anchor `{anchor}`")]
    UploadLocalEditRemoteNotConverged {
        namespace_id: String,
        inode_id: u64,
        field: &'static str,
        remote: String,
        anchor: String,
    },
    #[error(
        "download_remote_edit_manifest_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    DownloadRemoteEditManifestMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "download_remote_edit_remote_digest_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    DownloadRemoteEditRemoteDigestMissing { namespace_id: String, inode_id: u64 },
    #[error("download_remote_edit_state_missing: namespace `{namespace_id}` inode `{inode_id}`")]
    DownloadRemoteEditStateMissing { namespace_id: String, inode_id: u64 },
    #[error("download_remote_edit_requires_file: namespace `{namespace_id}` inode `{inode_id}` kind `{inode_kind}`")]
    DownloadRemoteEditRequiresFile {
        namespace_id: String,
        inode_id: u64,
        inode_kind: String,
    },
    #[error("download_remote_edit_path_change_not_supported: namespace `{namespace_id}` inode `{inode_id}` field `{field}` remote `{remote}` != anchor `{anchor}`")]
    DownloadRemoteEditPathChangeNotSupported {
        namespace_id: String,
        inode_id: u64,
        field: &'static str,
        remote: String,
        anchor: String,
    },
    #[error("download_remote_edit_local_not_converged: namespace `{namespace_id}` inode `{inode_id}` field `{field}` local `{local}` != anchor `{anchor}`")]
    DownloadRemoteEditLocalNotConverged {
        namespace_id: String,
        inode_id: u64,
        field: &'static str,
        local: String,
        anchor: String,
    },
    #[error("apply_remote_rename_state_missing: namespace `{namespace_id}` inode `{inode_id}`")]
    ApplyRemoteRenameStateMissing { namespace_id: String, inode_id: u64 },
    #[error("apply_remote_rename_requires_file: namespace `{namespace_id}` inode `{inode_id}` kind `{inode_kind}`")]
    ApplyRemoteRenameRequiresFile {
        namespace_id: String,
        inode_id: u64,
        inode_kind: String,
    },
    #[error("apply_remote_rename_local_not_converged: namespace `{namespace_id}` inode `{inode_id}` field `{field}` local `{local}` != anchor `{anchor}`")]
    ApplyRemoteRenameLocalNotConverged {
        namespace_id: String,
        inode_id: u64,
        field: &'static str,
        local: String,
        anchor: String,
    },
    #[error("apply_remote_rename_remote_not_path_only: namespace `{namespace_id}` inode `{inode_id}` field `{field}` remote `{remote}` != anchor `{anchor}`")]
    ApplyRemoteRenameRemoteNotPathOnly {
        namespace_id: String,
        inode_id: u64,
        field: &'static str,
        remote: String,
        anchor: String,
    },
    #[error(
        "apply_remote_rename_path_change_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    ApplyRemoteRenamePathChangeMissing { namespace_id: String, inode_id: u64 },
    #[error("apply_remote_delete_state_missing: namespace `{namespace_id}` inode `{inode_id}`")]
    ApplyRemoteDeleteStateMissing { namespace_id: String, inode_id: u64 },
    #[error("apply_remote_delete_requires_file: namespace `{namespace_id}` inode `{inode_id}` kind `{inode_kind}`")]
    ApplyRemoteDeleteRequiresFile {
        namespace_id: String,
        inode_id: u64,
        inode_kind: String,
    },
    #[error("apply_remote_delete_local_not_converged: namespace `{namespace_id}` inode `{inode_id}` field `{field}` local `{local}` != anchor `{anchor}`")]
    ApplyRemoteDeleteLocalNotConverged {
        namespace_id: String,
        inode_id: u64,
        field: &'static str,
        local: String,
        anchor: String,
    },
    #[error(
        "apply_remote_delete_remote_not_deleted: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    ApplyRemoteDeleteRemoteNotDeleted { namespace_id: String, inode_id: u64 },
    #[error("materialize_remote_dir_state_missing: namespace `{namespace_id}` inode `{inode_id}`")]
    MaterializeRemoteDirStateMissing { namespace_id: String, inode_id: u64 },
    #[error("materialize_remote_dir_requires_directory: namespace `{namespace_id}` inode `{inode_id}` kind `{inode_kind}`")]
    MaterializeRemoteDirRequiresDirectory {
        namespace_id: String,
        inode_id: u64,
        inode_kind: String,
    },
    #[error("materialize_remote_dir_placeholder_mismatch: namespace `{namespace_id}` inode `{inode_id}` field `{field}` local `{local}` != remote `{remote}`")]
    MaterializeRemoteDirPlaceholderMismatch {
        namespace_id: String,
        inode_id: u64,
        field: &'static str,
        local: String,
        remote: String,
    },
    #[error("remote_observation_bind_ambiguous: namespace `{namespace_id}` inode `{inode_id}` matches `{matches}`")]
    RemoteObservationBindAmbiguous {
        namespace_id: String,
        inode_id: u64,
        matches: usize,
    },
    #[error(
        "bind_kind_mismatch: `{client_file_id}` local kind `{local_kind}` != remote kind `{remote_kind}`"
    )]
    BindKindMismatch {
        client_file_id: String,
        local_kind: String,
        remote_kind: String,
    },
    #[error(
        "bind_namespace_mismatch: `{client_file_id}` local namespace `{local_namespace_id}` != remote namespace `{remote_namespace_id}`"
    )]
    BindNamespaceMismatch {
        client_file_id: String,
        local_namespace_id: String,
        remote_namespace_id: String,
    },
    #[error(
        "bind_remote_deleted: `{client_file_id}` cannot bind to deleted remote inode `{inode_id}`"
    )]
    BindRemoteDeleted {
        client_file_id: String,
        inode_id: u64,
    },
    #[error(
        "bind_observation_mismatch: `{client_file_id}` field `{field}` local `{local}` != remote `{remote}`"
    )]
    BindObservationMismatch {
        client_file_id: String,
        field: &'static str,
        local: String,
        remote: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientFileId(pub String);

impl ClientFileId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ClientFileId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteFileStateRow {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub observed_seq: ChangeSeq,
    pub revision_no: RevisionNo,
    pub content_digest: Option<String>,
    pub content_manifest_digest: Option<String>,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub is_deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalFileStateRow {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub content_digest: Option<String>,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub exists_on_disk: bool,
    pub dirty: bool,
    pub last_local_change_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncAnchorRow {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub synced_seq: ChangeSeq,
    pub revision_no: RevisionNo,
    pub content_digest: Option<String>,
    pub content_manifest_digest: Option<String>,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSyncViews {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub remote: Option<RemoteFileStateRow>,
    pub local: Option<LocalFileStateRow>,
    pub sync_anchor: Option<SyncAnchorRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedActionRow {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub decision: String,
    pub reason: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalOnlyFileStateRow {
    pub client_file_id: ClientFileId,
    pub namespace_id: NamespaceId,
    pub inode_kind: InodeKind,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub content_digest: Option<String>,
    pub exists_on_disk: bool,
    pub dirty: bool,
    pub last_local_change_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedLocalOnlyInode {
    pub namespace_id: NamespaceId,
    pub inode_kind: InodeKind,
    pub parent_inode_id: InodeId,
    pub display_name: String,
    pub content_digest: Option<String>,
    pub exists_on_disk: bool,
    pub dirty: bool,
    pub last_local_change_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalOnlyPlannedActionRow {
    pub client_file_id: ClientFileId,
    pub namespace_id: NamespaceId,
    pub decision: String,
    pub reason: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalOnlyUploadRow {
    pub client_file_id: ClientFileId,
    pub namespace_id: NamespaceId,
    pub file_digest_sha256: String,
    pub content_manifest_digest: String,
    pub manifest_object_key: String,
    pub file_size_bytes: u64,
    pub uploaded_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalOnlyTransferLedgerRow {
    pub client_file_id: ClientFileId,
    pub namespace_id: NamespaceId,
    pub transfer_id: String,
    pub direction: TransferDirection,
    pub object_key: String,
    pub block_index: u64,
    pub block_count: u64,
    pub state: TransferState,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalOnlyConflictOrErrorRow {
    pub client_file_id: ClientFileId,
    pub namespace_id: NamespaceId,
    pub record_id: u64,
    pub kind: String,
    pub summary: String,
    pub detail_json: Value,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InodeUploadRow {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub file_digest_sha256: String,
    pub content_manifest_digest: String,
    pub manifest_object_key: String,
    pub file_size_bytes: u64,
    pub uploaded_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferDirection {
    Download,
    Upload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferState {
    Staging,
    Uploading,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferLedgerRow {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub transfer_id: String,
    pub direction: TransferDirection,
    pub object_key: String,
    pub block_index: u64,
    pub block_count: u64,
    pub state: TransferState,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingClientMutationRow {
    pub client_request_id: String,
    pub namespace_id: NamespaceId,
    pub client_file_id: ClientFileId,
    pub request: ClientMutationRequest,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingInodeMutationRow {
    pub client_request_id: String,
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub request: ClientMutationRequest,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConflictOrErrorRow {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub record_id: u64,
    pub kind: String,
    pub summary: String,
    pub detail_json: Value,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundLocalOnlyFile {
    pub client_file_id: ClientFileId,
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedInodeMutation {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedRemoteInode {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub observed_seq: ChangeSeq,
    pub revision_no: RevisionNo,
    pub content_digest: Option<String>,
    pub content_manifest_digest: Option<String>,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub is_deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppliedRemoteObservation {
    BoundLocalOnly(BoundLocalOnlyFile),
    ConvergedBoundInode(AppliedInodeMutation),
    DiscoveredRemoteOnly {
        namespace_id: NamespaceId,
        inode_id: InodeId,
    },
    RecordedConflictOrError {
        namespace_id: NamespaceId,
        inode_id: InodeId,
        kind: String,
    },
    UpdatedBoundRemoteState {
        namespace_id: NamespaceId,
        inode_id: InodeId,
    },
    IgnoredStale {
        namespace_id: NamespaceId,
        inode_id: InodeId,
    },
    IgnoredUnmatched {
        namespace_id: NamespaceId,
        inode_id: InodeId,
    },
}

pub struct SqliteStateDb {
    conn: Connection,
}

pub struct PlannerTxn<'db> {
    tx: Transaction<'db>,
}

fn inode_kind_as_str(kind: &InodeKind) -> &'static str {
    match kind {
        InodeKind::File => "file",
        InodeKind::Dir => "dir",
        InodeKind::Symlink => "symlink",
        InodeKind::Mount => "mount",
    }
}

fn inode_kind_from_str(value: &str) -> Result<InodeKind, StateDbError> {
    match value {
        "file" => Ok(InodeKind::File),
        "dir" => Ok(InodeKind::Dir),
        "symlink" => Ok(InodeKind::Symlink),
        "mount" => Ok(InodeKind::Mount),
        other => Err(StateDbError::UnknownInodeKind(other.to_owned())),
    }
}

fn transfer_direction_as_str(direction: TransferDirection) -> &'static str {
    match direction {
        TransferDirection::Download => "download",
        TransferDirection::Upload => "upload",
    }
}

fn transfer_direction_from_str(value: &str) -> Result<TransferDirection, StateDbError> {
    match value {
        "download" => Ok(TransferDirection::Download),
        "upload" => Ok(TransferDirection::Upload),
        other => Err(StateDbError::UnknownTransferDirection(other.to_owned())),
    }
}

fn transfer_state_as_str(state: TransferState) -> &'static str {
    match state {
        TransferState::Staging => "staging",
        TransferState::Uploading => "uploading",
    }
}

fn transfer_state_from_str(value: &str) -> Result<TransferState, StateDbError> {
    match value {
        "staging" => Ok(TransferState::Staging),
        "uploading" => Ok(TransferState::Uploading),
        other => Err(StateDbError::UnknownTransferState(other.to_owned())),
    }
}

fn ensure_bind_match(
    client_file_id: &ClientFileId,
    field: &'static str,
    local: String,
    remote: String,
) -> Result<(), StateDbError> {
    if local == remote {
        return Ok(());
    }

    Err(StateDbError::BindObservationMismatch {
        client_file_id: client_file_id.as_str().to_owned(),
        field,
        local,
        remote,
    })
}

fn ensure_upload_local_edit_local_anchor_match(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    field: &'static str,
    local: String,
    anchor: String,
) -> Result<(), StateDbError> {
    if local == anchor {
        return Ok(());
    }

    Err(StateDbError::UploadLocalEditPathChangeNotSupported {
        namespace_id: namespace_id.as_str().to_owned(),
        inode_id: inode_id.0,
        field,
        local,
        anchor,
    })
}

fn ensure_upload_local_edit_remote_anchor_match(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    field: &'static str,
    remote: String,
    anchor: String,
) -> Result<(), StateDbError> {
    if remote == anchor {
        return Ok(());
    }

    Err(StateDbError::UploadLocalEditRemoteNotConverged {
        namespace_id: namespace_id.as_str().to_owned(),
        inode_id: inode_id.0,
        field,
        remote,
        anchor,
    })
}

fn ensure_download_remote_edit_remote_anchor_match(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    field: &'static str,
    remote: String,
    anchor: String,
) -> Result<(), StateDbError> {
    if remote == anchor {
        return Ok(());
    }

    Err(StateDbError::DownloadRemoteEditPathChangeNotSupported {
        namespace_id: namespace_id.as_str().to_owned(),
        inode_id: inode_id.0,
        field,
        remote,
        anchor,
    })
}

fn ensure_download_remote_edit_local_anchor_match(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    field: &'static str,
    local: String,
    anchor: String,
) -> Result<(), StateDbError> {
    if local == anchor {
        return Ok(());
    }

    Err(StateDbError::DownloadRemoteEditLocalNotConverged {
        namespace_id: namespace_id.as_str().to_owned(),
        inode_id: inode_id.0,
        field,
        local,
        anchor,
    })
}

fn ensure_apply_remote_rename_local_anchor_match(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    field: &'static str,
    local: String,
    anchor: String,
) -> Result<(), StateDbError> {
    if local == anchor {
        return Ok(());
    }

    Err(StateDbError::ApplyRemoteRenameLocalNotConverged {
        namespace_id: namespace_id.as_str().to_owned(),
        inode_id: inode_id.0,
        field,
        local,
        anchor,
    })
}

fn ensure_apply_remote_rename_remote_anchor_match(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    field: &'static str,
    remote: String,
    anchor: String,
) -> Result<(), StateDbError> {
    if remote == anchor {
        return Ok(());
    }

    Err(StateDbError::ApplyRemoteRenameRemoteNotPathOnly {
        namespace_id: namespace_id.as_str().to_owned(),
        inode_id: inode_id.0,
        field,
        remote,
        anchor,
    })
}

fn ensure_apply_remote_delete_local_anchor_match(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    field: &'static str,
    local: String,
    anchor: String,
) -> Result<(), StateDbError> {
    if local == anchor {
        return Ok(());
    }

    Err(StateDbError::ApplyRemoteDeleteLocalNotConverged {
        namespace_id: namespace_id.as_str().to_owned(),
        inode_id: inode_id.0,
        field,
        local,
        anchor,
    })
}

fn bound_local_matches_remote_observation(
    local: &LocalFileStateRow,
    observed: &RemoteFileStateRow,
) -> bool {
    local.exists_on_disk
        && !observed.is_deleted
        && local.inode_kind == observed.inode_kind
        && local.content_digest == observed.content_digest
        && local.parent_inode_id == observed.parent_inode_id
        && local.display_name == observed.display_name
}

fn local_only_matches_remote_observation(
    local_only: &LocalOnlyFileStateRow,
    observed: &RemoteFileStateRow,
) -> bool {
    local_only.exists_on_disk
        && !observed.is_deleted
        && local_only.namespace_id == observed.namespace_id
        && local_only.inode_kind == observed.inode_kind
        && local_only.content_digest == observed.content_digest
        && local_only.parent_inode_id == observed.parent_inode_id
        && local_only.display_name == observed.display_name
}

fn remote_only_discovery_supported(observed: &RemoteFileStateRow) -> bool {
    matches!(observed.inode_kind, InodeKind::File | InodeKind::Dir) && !observed.is_deleted
}

fn remote_only_placeholder_matches_remote_state(
    local: &LocalFileStateRow,
    observed: &RemoteFileStateRow,
) -> bool {
    !local.exists_on_disk
        && !local.dirty
        && local.inode_kind == observed.inode_kind
        && local.parent_inode_id == observed.parent_inode_id
        && local.display_name == observed.display_name
}

fn validate_local_only_upload(
    local_only: &LocalOnlyFileStateRow,
    upload_namespace_id: &NamespaceId,
    uploaded_file_digest: &str,
) -> Result<(), StateDbError> {
    if local_only.inode_kind != InodeKind::File {
        return Err(StateDbError::UploadedContentRequiresFile {
            client_file_id: local_only.client_file_id.as_str().to_owned(),
            inode_kind: inode_kind_as_str(&local_only.inode_kind).to_owned(),
        });
    }

    if local_only.namespace_id != *upload_namespace_id {
        return Err(StateDbError::UploadedContentNamespaceMismatch {
            client_file_id: local_only.client_file_id.as_str().to_owned(),
            local_namespace_id: local_only.namespace_id.as_str().to_owned(),
            uploaded_namespace_id: upload_namespace_id.as_str().to_owned(),
        });
    }

    let local_content_digest = local_only.content_digest.as_deref().ok_or_else(|| {
        StateDbError::UploadedContentLocalDigestMissing {
            client_file_id: local_only.client_file_id.as_str().to_owned(),
        }
    })?;

    if local_content_digest != uploaded_file_digest {
        return Err(StateDbError::UploadedContentDigestMismatch {
            client_file_id: local_only.client_file_id.as_str().to_owned(),
            local_content_digest: local_content_digest.to_owned(),
            uploaded_file_digest: uploaded_file_digest.to_owned(),
        });
    }

    Ok(())
}

fn validate_inode_upload(
    local: &LocalFileStateRow,
    upload_namespace_id: &NamespaceId,
    uploaded_file_digest: &str,
) -> Result<(), StateDbError> {
    if local.inode_kind != InodeKind::File {
        return Err(StateDbError::InodeUploadRequiresFile {
            namespace_id: local.namespace_id.as_str().to_owned(),
            inode_id: local.inode_id.0,
            inode_kind: inode_kind_as_str(&local.inode_kind).to_owned(),
        });
    }

    if local.namespace_id != *upload_namespace_id {
        return Err(StateDbError::InodeUploadNamespaceMismatch {
            namespace_id: local.namespace_id.as_str().to_owned(),
            inode_id: local.inode_id.0,
            local_namespace_id: local.namespace_id.as_str().to_owned(),
            uploaded_namespace_id: upload_namespace_id.as_str().to_owned(),
        });
    }

    let local_content_digest = local.content_digest.as_deref().ok_or_else(|| {
        StateDbError::InodeUploadLocalDigestMissing {
            namespace_id: local.namespace_id.as_str().to_owned(),
            inode_id: local.inode_id.0,
        }
    })?;

    if local_content_digest != uploaded_file_digest {
        return Err(StateDbError::InodeUploadDigestMismatch {
            namespace_id: local.namespace_id.as_str().to_owned(),
            inode_id: local.inode_id.0,
            local_content_digest: local_content_digest.to_owned(),
            uploaded_file_digest: uploaded_file_digest.to_owned(),
        });
    }

    Ok(())
}

fn to_sql_u64(value: u64, field: &'static str) -> Result<i64, StateDbError> {
    i64::try_from(value).map_err(|_| StateDbError::UnsignedOutOfRange { field, value })
}

fn from_sql_u64(value: i64, field: &'static str) -> Result<u64, StateDbError> {
    u64::try_from(value).map_err(|_| StateDbError::IntegerOutOfRange { field, value })
}
