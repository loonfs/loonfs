use crate::conflict::ConflictArtifactError;
use crate::download::DownloadError;
use crate::local_apply::LocalApplyError;
use crate::planner::{PlannerDecision, PlannerError};
use crate::state_db::{
    AppliedInodeMutation, BoundLocalOnlyFile, ClientFileId, InodeUploadRow,
    LocalOnlyPlannedActionRow, LocalOnlyTransferLedgerRow, LocalOnlyUploadRow,
    PendingClientMutationRow, PendingInodeMutationRow, PlannedActionRow, StateDbError,
    TransferLedgerRow,
};
use crate::upload::UploadError;
use loon_types::{ClientMutationRequest, ClientMutationResponse, InodeKind};
use thiserror::Error;

mod dispatch;
mod inode;
mod local_only;
mod request;
mod schedule;

pub use dispatch::{dispatch_client_mutation_from_state, dispatch_inode_mutation_from_state};
pub use inode::{
    execute_apply_remote_delete_to_path, execute_apply_remote_rename_to_paths,
    execute_apply_remote_subtree_delete_to_path, execute_apply_remote_subtree_rename_to_paths,
    execute_download_remote_edit_to_path, execute_materialize_remote_dir_to_path,
    execute_upload_local_edit_from_path,
};
pub use local_only::{
    execute_create_remote_dir, execute_local_only_create, execute_upload_local_create_from_path,
};
pub use request::{
    build_client_mutation_request, build_client_mutation_request_from_state,
    build_inode_mutation_request, build_inode_mutation_request_from_state,
};
pub(crate) use schedule::execute_next_client_action_with_hooks;
pub use schedule::{execute_next_client_action, execute_next_local_only_create};

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("state DB error: {0}")]
    StateDb(#[from] StateDbError),
    #[error("planner error: {0}")]
    Planner(#[from] PlannerError),
    #[error("empty client request id")]
    EmptyClientRequestId,
    #[error("planned_local_only_action_missing: `{client_file_id}`")]
    PlannedLocalOnlyActionMissing { client_file_id: String },
    #[error("planned_action_missing: namespace `{namespace_id}` inode `{inode_id}`")]
    PlannedActionMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "client_file_id mismatch: local `{local_client_file_id}` != planned `{planned_client_file_id}`"
    )]
    ClientFileIdMismatch {
        local_client_file_id: String,
        planned_client_file_id: String,
    },
    #[error(
        "namespace mismatch: local `{local_namespace_id}` != planned `{planned_namespace_id}`"
    )]
    NamespaceMismatch {
        local_namespace_id: String,
        planned_namespace_id: String,
    },
    #[error(
        "planned inode namespace mismatch: local `{local_namespace_id}` != planned `{planned_namespace_id}` for inode `{inode_id}`"
    )]
    PlannedInodeNamespaceMismatch {
        inode_id: u64,
        local_namespace_id: String,
        planned_namespace_id: String,
    },
    #[error("missing parent inode for `{client_file_id}`")]
    MissingParentInode { client_file_id: String },
    #[error("missing content manifest digest for `{client_file_id}`")]
    MissingContentManifestDigest { client_file_id: String },
    #[error("missing content manifest digest for namespace `{namespace_id}` inode `{inode_id}`")]
    MissingInodeContentManifestDigest { namespace_id: String, inode_id: u64 },
    #[error("unsupported planner decision `{0:?}` for client mutation executor")]
    UnsupportedDecision(PlannerDecision),
    #[error(
        "planner decision `{decision:?}` is incompatible with inode kind `{inode_kind:?}` for `{client_file_id}`"
    )]
    DecisionKindMismatch {
        client_file_id: String,
        decision: PlannerDecision,
        inode_kind: InodeKind,
    },
    #[error(
        "planner decision `{decision:?}` is incompatible with inode kind `{inode_kind:?}` for namespace `{namespace_id}` inode `{inode_id}`"
    )]
    InodeDecisionKindMismatch {
        namespace_id: String,
        inode_id: u64,
        decision: PlannerDecision,
        inode_kind: InodeKind,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchedClientMutation {
    pub pending: PendingClientMutationRow,
    pub request: ClientMutationRequest,
    pub response: ClientMutationResponse,
    pub bound_identity: BoundLocalOnlyFile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchedInodeMutation {
    pub pending: PendingInodeMutationRow,
    pub request: ClientMutationRequest,
    pub response: ClientMutationResponse,
    pub applied: AppliedInodeMutation,
}

#[derive(Debug, Error)]
pub enum DispatchClientMutationError {
    #[error(transparent)]
    Executor(#[from] ExecutorError),
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error("dispatch_failed: `{client_request_id}` {message}")]
    DispatchFailed {
        client_request_id: String,
        message: String,
    },
}

#[derive(Debug, Error)]
pub enum DispatchInodeMutationError {
    #[error(transparent)]
    Executor(#[from] ExecutorError),
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error("dispatch_failed: `{client_request_id}` {message}")]
    DispatchFailed {
        client_request_id: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedUploadLocalCreate {
    pub ensured_upload: Option<LocalOnlyUploadRow>,
    pub upload_reused: bool,
    pub dispatched: DispatchedClientMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressedUploadLocalCreate {
    pub transfer: LocalOnlyTransferLedgerRow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedCreateRemoteDir {
    pub reused_pending_request: bool,
    pub dispatched: DispatchedClientMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedUploadLocalEdit {
    pub ensured_upload: Option<InodeUploadRow>,
    pub upload_reused: bool,
    pub dispatched: DispatchedInodeMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressedUploadLocalEdit {
    pub transfer: TransferLedgerRow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedDownloadRemoteEdit {
    pub downloaded_content_manifest_digest: String,
    pub downloaded_file_digest_sha256: String,
    pub applied: AppliedInodeMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressedDownloadRemoteEdit {
    pub transfer: TransferLedgerRow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedMaterializeRemoteDir {
    pub applied: AppliedInodeMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedApplyRemoteRename {
    pub applied: AppliedInodeMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedApplyRemoteRenameAndReplace {
    pub applied: AppliedInodeMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedApplyRemoteDelete {
    pub applied: AppliedInodeMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedResolveSameInodeConflict {
    pub applied: AppliedInodeMutation,
    pub conflict_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedResolveDeleteVsEditConflict {
    pub applied: AppliedInodeMutation,
    pub conflict_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedResolveRenameVsEditConflict {
    pub applied: AppliedInodeMutation,
    pub conflict_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedResolvePathBindingCollision {
    pub applied: AppliedInodeMutation,
    pub conflict_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedResolveSubtreeDeleteConflict {
    pub applied: AppliedInodeMutation,
    pub conflict_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedResolveSubtreeRenameConflict {
    pub applied: AppliedInodeMutation,
    pub conflict_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedApplyRemoteSubtreeDelete {
    pub applied: AppliedInodeMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedApplyRemoteSubtreeRename {
    pub applied: AppliedInodeMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutedLocalOnlyCreate {
    UploadLocalCreate(UploadLocalCreateExecution),
    CreateRemoteDir(ExecutedCreateRemoteDir),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadLocalCreateExecution {
    Progressed(ProgressedUploadLocalCreate),
    Completed(ExecutedUploadLocalCreate),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadLocalEditExecution {
    Progressed(ProgressedUploadLocalEdit),
    Completed(ExecutedUploadLocalEdit),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadRemoteEditExecution {
    Progressed(ProgressedDownloadRemoteEdit),
    Completed(ExecutedDownloadRemoteEdit),
}

#[derive(Debug, Error)]
pub enum ExecuteUploadLocalCreateError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Planner(#[from] PlannerError),
    #[error(transparent)]
    Executor(#[from] ExecutorError),
    #[error(transparent)]
    Upload(#[from] UploadError),
    #[error(transparent)]
    Dispatch(#[from] DispatchClientMutationError),
    #[error("upload_local_create_decision_missing: `{client_file_id}` decision `{decision:?}`")]
    UploadLocalCreateDecisionMissing {
        client_file_id: String,
        decision: PlannerDecision,
    },
    #[error("local_only_create_source_path_missing: `{client_file_id}`")]
    SourcePathMissing { client_file_id: String },
}

#[derive(Debug, Error)]
pub enum ExecuteCreateRemoteDirError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Planner(#[from] PlannerError),
    #[error(transparent)]
    Executor(#[from] ExecutorError),
    #[error(transparent)]
    Dispatch(#[from] DispatchClientMutationError),
    #[error("create_remote_dir_decision_missing: `{client_file_id}` decision `{decision:?}`")]
    CreateRemoteDirDecisionMissing {
        client_file_id: String,
        decision: PlannerDecision,
    },
}

#[derive(Debug, Error)]
pub enum ExecuteLocalOnlyCreateError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Planner(#[from] PlannerError),
    #[error(transparent)]
    Executor(#[from] ExecutorError),
    #[error(transparent)]
    UploadLocalCreate(#[from] ExecuteUploadLocalCreateError),
    #[error(transparent)]
    CreateRemoteDir(#[from] ExecuteCreateRemoteDirError),
}

#[derive(Debug, Error)]
pub enum ExecuteUploadLocalEditError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Planner(#[from] PlannerError),
    #[error(transparent)]
    Executor(#[from] ExecutorError),
    #[error(transparent)]
    Upload(#[from] UploadError),
    #[error(transparent)]
    Dispatch(#[from] DispatchInodeMutationError),
    #[error(
        "upload_local_edit_decision_missing: namespace `{namespace_id}` inode `{inode_id}` decision `{decision:?}`"
    )]
    UploadLocalEditDecisionMissing {
        namespace_id: String,
        inode_id: u64,
        decision: PlannerDecision,
    },
    #[error(
        "upload_local_edit_source_path_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    SourcePathMissing { namespace_id: String, inode_id: u64 },
}

#[derive(Debug, Error)]
pub enum ExecuteDownloadRemoteEditError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Planner(#[from] PlannerError),
    #[error(transparent)]
    Executor(#[from] ExecutorError),
    #[error(transparent)]
    Download(#[from] DownloadError),
    #[error(
        "download_remote_edit_decision_missing: namespace `{namespace_id}` inode `{inode_id}` decision `{decision:?}`"
    )]
    DownloadRemoteEditDecisionMissing {
        namespace_id: String,
        inode_id: u64,
        decision: PlannerDecision,
    },
    #[error(
        "download_remote_edit_source_path_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    SourcePathMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "download_remote_edit_remote_digest_mismatch: namespace `{namespace_id}` inode `{inode_id}` remote `{remote_content_digest}` != downloaded `{downloaded_file_digest}`"
    )]
    RemoteDigestMismatch {
        namespace_id: String,
        inode_id: u64,
        remote_content_digest: String,
        downloaded_file_digest: String,
    },
    #[error(
        "download_remote_edit_local_apply_failed: operation `{operation}` path `{path}` {source}"
    )]
    LocalApplyFailed {
        operation: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
}

impl From<LocalApplyError> for ExecuteDownloadRemoteEditError {
    fn from(error: LocalApplyError) -> Self {
        Self::LocalApplyFailed {
            operation: error.operation,
            path: error.path,
            source: error.source,
        }
    }
}

#[derive(Debug, Error)]
pub enum ExecuteApplyRemoteRenameError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Planner(#[from] PlannerError),
    #[error(transparent)]
    Executor(#[from] ExecutorError),
    #[error(
        "apply_remote_rename_decision_missing: namespace `{namespace_id}` inode `{inode_id}` decision `{decision:?}`"
    )]
    ApplyRemoteRenameDecisionMissing {
        namespace_id: String,
        inode_id: u64,
        decision: PlannerDecision,
    },
    #[error(
        "apply_remote_rename_current_path_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    CurrentPathMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "apply_remote_rename_target_path_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    TargetPathMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "apply_remote_rename_destination_occupied: namespace `{namespace_id}` inode `{inode_id}` path `{path}`"
    )]
    DestinationOccupied {
        namespace_id: String,
        inode_id: u64,
        path: String,
    },
    #[error(
        "apply_remote_rename_local_apply_failed: operation `{operation}` path `{path}` {source}"
    )]
    LocalApplyFailed {
        operation: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
}

impl From<LocalApplyError> for ExecuteApplyRemoteRenameError {
    fn from(error: LocalApplyError) -> Self {
        Self::LocalApplyFailed {
            operation: error.operation,
            path: error.path,
            source: error.source,
        }
    }
}

#[derive(Debug, Error)]
pub enum ExecuteApplyRemoteRenameAndReplaceError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Planner(#[from] PlannerError),
    #[error(transparent)]
    Executor(#[from] ExecutorError),
    #[error(transparent)]
    Download(#[from] DownloadError),
    #[error(
        "apply_remote_rename_and_replace_decision_missing: namespace `{namespace_id}` inode `{inode_id}` decision `{decision:?}`"
    )]
    DecisionMissing {
        namespace_id: String,
        inode_id: u64,
        decision: PlannerDecision,
    },
    #[error(
        "apply_remote_rename_and_replace_current_path_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    CurrentPathMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "apply_remote_rename_and_replace_target_path_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    TargetPathMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "apply_remote_rename_and_replace_destination_occupied: namespace `{namespace_id}` inode `{inode_id}` path `{path}`"
    )]
    DestinationOccupied {
        namespace_id: String,
        inode_id: u64,
        path: String,
    },
    #[error(
        "apply_remote_rename_and_replace_local_apply_failed: operation `{operation}` path `{path}` {source}"
    )]
    LocalApplyFailed {
        operation: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
}

impl From<LocalApplyError> for ExecuteApplyRemoteRenameAndReplaceError {
    fn from(error: LocalApplyError) -> Self {
        Self::LocalApplyFailed {
            operation: error.operation,
            path: error.path,
            source: error.source,
        }
    }
}

#[derive(Debug, Error)]
pub enum ExecuteApplyRemoteDeleteError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Planner(#[from] PlannerError),
    #[error(transparent)]
    Executor(#[from] ExecutorError),
    #[error(
        "apply_remote_delete_decision_missing: namespace `{namespace_id}` inode `{inode_id}` decision `{decision:?}`"
    )]
    ApplyRemoteDeleteDecisionMissing {
        namespace_id: String,
        inode_id: u64,
        decision: PlannerDecision,
    },
    #[error(
        "apply_remote_delete_current_path_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    CurrentPathMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "apply_remote_delete_local_apply_failed: operation `{operation}` path `{path}` {source}"
    )]
    LocalApplyFailed {
        operation: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
}

impl From<LocalApplyError> for ExecuteApplyRemoteDeleteError {
    fn from(error: LocalApplyError) -> Self {
        Self::LocalApplyFailed {
            operation: error.operation,
            path: error.path,
            source: error.source,
        }
    }
}

#[derive(Debug, Error)]
pub enum ExecuteResolveFileConflictError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Planner(#[from] PlannerError),
    #[error(transparent)]
    Executor(#[from] ExecutorError),
    #[error(transparent)]
    Download(#[from] DownloadError),
    #[error(transparent)]
    ConflictArtifact(#[from] ConflictArtifactError),
    #[error(
        "resolve_file_conflict_decision_missing: namespace `{namespace_id}` inode `{inode_id}` decision `{decision:?}`"
    )]
    DecisionMissing {
        namespace_id: String,
        inode_id: u64,
        decision: PlannerDecision,
    },
    #[error(
        "resolve_file_conflict_current_path_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    CurrentPathMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "resolve_file_conflict_target_path_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    TargetPathMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "resolve_file_conflict_destination_occupied: namespace `{namespace_id}` inode `{inode_id}` path `{path}`"
    )]
    DestinationOccupied {
        namespace_id: String,
        inode_id: u64,
        path: String,
    },
    #[error(
        "resolve_file_conflict_local_apply_failed: operation `{operation}` path `{path}` {source}"
    )]
    LocalApplyFailed {
        operation: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "resolve_path_binding_collision_source_path_missing: namespace `{namespace_id}` inode `{inode_id}` temp `{client_file_id}`"
    )]
    PathBindingSourcePathMissing {
        namespace_id: String,
        inode_id: u64,
        client_file_id: String,
    },
}

impl From<LocalApplyError> for ExecuteResolveFileConflictError {
    fn from(error: LocalApplyError) -> Self {
        Self::LocalApplyFailed {
            operation: error.operation,
            path: error.path,
            source: error.source,
        }
    }
}

#[derive(Debug, Error)]
pub enum ExecuteResolveSubtreeConflictError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Planner(#[from] PlannerError),
    #[error(transparent)]
    Executor(#[from] ExecutorError),
    #[error(transparent)]
    Download(#[from] DownloadError),
    #[error(transparent)]
    ConflictArtifact(#[from] ConflictArtifactError),
    #[error(
        "resolve_subtree_conflict_decision_missing: namespace `{namespace_id}` inode `{inode_id}` decision `{decision:?}`"
    )]
    DecisionMissing {
        namespace_id: String,
        inode_id: u64,
        decision: PlannerDecision,
    },
    #[error(
        "resolve_subtree_conflict_current_path_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    CurrentPathMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "resolve_subtree_conflict_target_path_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    TargetPathMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "resolve_subtree_conflict_destination_occupied: namespace `{namespace_id}` inode `{inode_id}` path `{path}`"
    )]
    DestinationOccupied {
        namespace_id: String,
        inode_id: u64,
        path: String,
    },
    #[error(
        "resolve_subtree_conflict_winner_restore_io: operation `{operation}` path `{path}` {source}"
    )]
    WinnerRestoreIo {
        operation: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "resolve_subtree_conflict_recursive_remove_io: operation `{operation}` path `{path}` {source}"
    )]
    RecursiveRemoveIo {
        operation: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("resolve_subtree_conflict_rename_io: operation `{operation}` path `{path}` {source}")]
    RenameIo {
        operation: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, Error)]
pub enum ExecuteApplyRemoteSubtreeDeleteError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Planner(#[from] PlannerError),
    #[error(transparent)]
    Executor(#[from] ExecutorError),
    #[error(
        "apply_remote_subtree_delete_decision_missing: namespace `{namespace_id}` inode `{inode_id}` decision `{decision:?}`"
    )]
    ApplyRemoteSubtreeDeleteDecisionMissing {
        namespace_id: String,
        inode_id: u64,
        decision: PlannerDecision,
    },
    #[error(
        "apply_remote_subtree_delete_current_path_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    CurrentPathMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "apply_remote_subtree_delete_local_apply_failed: operation `{operation}` path `{path}` {source}"
    )]
    LocalApplyFailed {
        operation: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
}

impl From<LocalApplyError> for ExecuteApplyRemoteSubtreeDeleteError {
    fn from(error: LocalApplyError) -> Self {
        Self::LocalApplyFailed {
            operation: error.operation,
            path: error.path,
            source: error.source,
        }
    }
}

#[derive(Debug, Error)]
pub enum ExecuteApplyRemoteSubtreeRenameError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Planner(#[from] PlannerError),
    #[error(transparent)]
    Executor(#[from] ExecutorError),
    #[error(
        "apply_remote_subtree_rename_decision_missing: namespace `{namespace_id}` inode `{inode_id}` decision `{decision:?}`"
    )]
    ApplyRemoteSubtreeRenameDecisionMissing {
        namespace_id: String,
        inode_id: u64,
        decision: PlannerDecision,
    },
    #[error(
        "apply_remote_subtree_rename_current_path_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    CurrentPathMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "apply_remote_subtree_rename_target_path_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    TargetPathMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "apply_remote_subtree_rename_destination_occupied: namespace `{namespace_id}` inode `{inode_id}` path `{path}`"
    )]
    DestinationOccupied {
        namespace_id: String,
        inode_id: u64,
        path: String,
    },
    #[error(
        "apply_remote_subtree_rename_local_apply_failed: operation `{operation}` path `{path}` {source}"
    )]
    LocalApplyFailed {
        operation: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
}

impl From<LocalApplyError> for ExecuteApplyRemoteSubtreeRenameError {
    fn from(error: LocalApplyError) -> Self {
        Self::LocalApplyFailed {
            operation: error.operation,
            path: error.path,
            source: error.source,
        }
    }
}

#[derive(Debug, Error)]
pub enum ExecuteMaterializeRemoteDirError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Planner(#[from] PlannerError),
    #[error(transparent)]
    Executor(#[from] ExecutorError),
    #[error(
        "materialize_remote_dir_decision_missing: namespace `{namespace_id}` inode `{inode_id}` decision `{decision:?}`"
    )]
    MaterializeRemoteDirDecisionMissing {
        namespace_id: String,
        inode_id: u64,
        decision: PlannerDecision,
    },
    #[error(
        "materialize_remote_dir_source_path_missing: namespace `{namespace_id}` inode `{inode_id}`"
    )]
    SourcePathMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "materialize_remote_dir_local_apply_failed: operation `{operation}` path `{path}` {source}"
    )]
    LocalApplyFailed {
        operation: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
}

impl From<LocalApplyError> for ExecuteMaterializeRemoteDirError {
    fn from(error: LocalApplyError) -> Self {
        Self::LocalApplyFailed {
            operation: error.operation,
            path: error.path,
            source: error.source,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedNextLocalOnlyCreate {
    pub planned_action: LocalOnlyPlannedActionRow,
    pub executed: ExecutedLocalOnlyCreate,
}

#[derive(Debug, Error)]
pub enum ExecuteNextLocalOnlyCreateError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    LocalOnlyCreate(#[from] ExecuteLocalOnlyCreateError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NextClientAction {
    ExecutedLocalOnlyCreate(ExecutedNextLocalOnlyCreate),
    ExecutedUploadLocalEdit(UploadLocalEditExecution),
    ExecutedDownloadRemoteEdit(DownloadRemoteEditExecution),
    ExecutedResolveSameInodeConflict(ExecutedResolveSameInodeConflict),
    ExecutedResolveDeleteVsEditConflict(ExecutedResolveDeleteVsEditConflict),
    ExecutedResolveRenameVsEditConflict(ExecutedResolveRenameVsEditConflict),
    ExecutedResolvePathBindingCollision(ExecutedResolvePathBindingCollision),
    ExecutedResolveSubtreeDeleteConflict(ExecutedResolveSubtreeDeleteConflict),
    ExecutedResolveSubtreeRenameConflict(ExecutedResolveSubtreeRenameConflict),
    ExecutedApplyRemoteRenameAndReplace(ExecutedApplyRemoteRenameAndReplace),
    ExecutedApplyRemoteDelete(ExecutedApplyRemoteDelete),
    ExecutedApplyRemoteSubtreeDelete(ExecutedApplyRemoteSubtreeDelete),
    ExecutedApplyRemoteSubtreeRename(ExecutedApplyRemoteSubtreeRename),
    ExecutedApplyRemoteRename(ExecutedApplyRemoteRename),
    ExecutedMaterializeRemoteDir(ExecutedMaterializeRemoteDir),
    SelectedPlannedAction(PlannedActionRow),
}

#[derive(Debug, Error)]
pub enum ExecuteNextClientActionError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    LocalOnlyCreate(#[from] ExecuteLocalOnlyCreateError),
    #[error(transparent)]
    UploadLocalEdit(#[from] ExecuteUploadLocalEditError),
    #[error(transparent)]
    DownloadRemoteEdit(#[from] ExecuteDownloadRemoteEditError),
    #[error(transparent)]
    ResolveFileConflict(#[from] ExecuteResolveFileConflictError),
    #[error(transparent)]
    ResolveSubtreeConflict(#[from] ExecuteResolveSubtreeConflictError),
    #[error(transparent)]
    ApplyRemoteRenameAndReplace(#[from] ExecuteApplyRemoteRenameAndReplaceError),
    #[error(transparent)]
    ApplyRemoteDelete(#[from] ExecuteApplyRemoteDeleteError),
    #[error(transparent)]
    ApplyRemoteSubtreeDelete(#[from] ExecuteApplyRemoteSubtreeDeleteError),
    #[error(transparent)]
    ApplyRemoteSubtreeRename(#[from] ExecuteApplyRemoteSubtreeRenameError),
    #[error(transparent)]
    ApplyRemoteRename(#[from] ExecuteApplyRemoteRenameError),
    #[error(transparent)]
    MaterializeRemoteDir(#[from] ExecuteMaterializeRemoteDirError),
}
