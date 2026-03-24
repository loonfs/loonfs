use crate::paths::{join_under_mirror_root, NamespacePathIndex};
use crate::{require_existing_file, OpsConfig};
use anyhow::Result;
use loon_client::executor::{
    execute_next_client_action, DownloadRemoteEditExecution, ExecuteNextClientActionError,
    ExecutedLocalOnlyCreate, NextClientAction, UploadLocalCreateExecution,
    UploadLocalEditExecution,
};
use loon_client::state_db::{ClientFileId, SqliteStateDb, TransferDirection, TransferState};
use loon_server::mutation::{execute_client_mutation, ClientMutationExecutionParams};
use loon_types::{ChangeSeq, InodeId, NamespaceId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncOnceOutcome {
    NoWork,
    ProgressedTransfer,
    RequestCommitted,
    AppliedRemoteChange,
    ResolvedConflict,
}

impl SyncOnceOutcome {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::NoWork => "no_work",
            Self::ProgressedTransfer => "progressed_transfer",
            Self::RequestCommitted => "request_committed",
            Self::AppliedRemoteChange => "applied_remote_change",
            Self::ResolvedConflict => "resolved_conflict",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncOnceTransferReport {
    pub transfer_id: String,
    pub direction: TransferDirection,
    pub block_index: u64,
    pub block_count: u64,
    pub state: TransferState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncOnceReport {
    pub namespace_id: NamespaceId,
    pub outcome: SyncOnceOutcome,
    pub action_kind: Option<String>,
    pub inode_id: Option<InodeId>,
    pub client_file_id: Option<ClientFileId>,
    pub client_request_id: Option<String>,
    pub committed_seq: Option<ChangeSeq>,
    pub conflict_id: Option<String>,
    pub transfer: Option<SyncOnceTransferReport>,
}

#[derive(Debug, Error)]
pub enum SyncOnceError {
    #[error(transparent)]
    StateDb(#[from] loon_client::state_db::StateDbError),
    #[error(transparent)]
    Execute(Box<ExecuteNextClientActionError>),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error("sync-once selected unsupported planner decision `{decision}`")]
    UnsupportedSelectedAction { decision: String },
}

impl From<ExecuteNextClientActionError> for SyncOnceError {
    fn from(value: ExecuteNextClientActionError) -> Self {
        Self::Execute(Box::new(value))
    }
}

pub fn sync_once(
    config: &OpsConfig,
    namespace_id: &NamespaceId,
) -> Result<SyncOnceReport, SyncOnceError> {
    require_existing_file(&config.client.state_db_path, "client state db")?;

    let store = config.open_store()?;
    let mut db = SqliteStateDb::open(&config.client.state_db_path)?;
    let summary = db.load_namespace_state_summary(namespace_id)?;
    let path_index = NamespacePathIndex::build(&summary);
    let mirror_root = config.client.mirror_root.clone();
    let now_ms = config.now_ms();
    let writer_id = config.server.writer_id.clone();
    let writer_version = config.server.writer_version.clone();

    let next_action = execute_next_client_action(
        &mut db,
        &store,
        |client_file_id| {
            path_index
                .resolve_local_only_source_relative_path(client_file_id)
                .map(|relative| join_under_mirror_root(&mirror_root, relative))
        },
        |_namespace_id, inode_id| {
            path_index
                .resolve_current_inode_relative_path(inode_id)
                .map(|relative| join_under_mirror_root(&mirror_root, relative))
        },
        |_namespace_id, inode_id, parent_inode_id, display_name| {
            path_index
                .resolve_target_inode_relative_path(inode_id, parent_inode_id, display_name)
                .map(|relative| join_under_mirror_root(&mirror_root, &relative))
        },
        now_ms,
        now_ms,
        |request| {
            execute_client_mutation(
                &store,
                request,
                &ClientMutationExecutionParams {
                    writer_id: writer_id.clone(),
                    writer_version: writer_version.clone(),
                    now_ms,
                },
            )
            .map(|executed| executed.response)
            .map_err(|err| err.to_string())
        },
    )?;

    match next_action {
        None => Ok(SyncOnceReport {
            namespace_id: namespace_id.clone(),
            outcome: SyncOnceOutcome::NoWork,
            action_kind: None,
            inode_id: None,
            client_file_id: None,
            client_request_id: None,
            committed_seq: None,
            conflict_id: None,
            transfer: None,
        }),
        Some(NextClientAction::SelectedPlannedAction(planned)) => {
            Err(SyncOnceError::UnsupportedSelectedAction {
                decision: planned.decision,
            })
        }
        Some(other) => Ok(report_for_next_action(namespace_id, other)),
    }
}

pub(crate) fn render_sync_once_report(report: &SyncOnceReport) -> Result<String> {
    let yaml = serde_yaml::to_string(report)?;
    Ok(format!(
        "command=ops/sync-once\nnamespace={}\noutcome={}\naction_kind={}\n---\n{}",
        report.namespace_id.as_str(),
        report.outcome.as_str(),
        report.action_kind.as_deref().unwrap_or("none"),
        yaml
    ))
}

fn report_for_next_action(
    namespace_id: &NamespaceId,
    next_action: NextClientAction,
) -> SyncOnceReport {
    match next_action {
        NextClientAction::ExecutedLocalOnlyCreate(executed) => match executed.executed {
            ExecutedLocalOnlyCreate::UploadLocalCreate(UploadLocalCreateExecution::Progressed(
                progressed,
            )) => SyncOnceReport {
                namespace_id: namespace_id.clone(),
                outcome: SyncOnceOutcome::ProgressedTransfer,
                action_kind: Some("upload_local_create".to_owned()),
                inode_id: None,
                client_file_id: Some(executed.planned_action.client_file_id.clone()),
                client_request_id: None,
                committed_seq: None,
                conflict_id: None,
                transfer: Some(SyncOnceTransferReport {
                    transfer_id: progressed.transfer.transfer_id,
                    direction: progressed.transfer.direction,
                    block_index: progressed.transfer.block_index,
                    block_count: progressed.transfer.block_count,
                    state: progressed.transfer.state,
                }),
            },
            ExecutedLocalOnlyCreate::UploadLocalCreate(UploadLocalCreateExecution::Completed(
                completed,
            )) => SyncOnceReport {
                namespace_id: namespace_id.clone(),
                outcome: SyncOnceOutcome::RequestCommitted,
                action_kind: Some("upload_local_create".to_owned()),
                inode_id: Some(completed.dispatched.bound_identity.inode_id),
                client_file_id: Some(completed.dispatched.bound_identity.client_file_id),
                client_request_id: Some(completed.dispatched.request.client_request_id),
                committed_seq: Some(completed.dispatched.response.committed_seq),
                conflict_id: None,
                transfer: None,
            },
            ExecutedLocalOnlyCreate::CreateRemoteDir(completed) => SyncOnceReport {
                namespace_id: namespace_id.clone(),
                outcome: SyncOnceOutcome::RequestCommitted,
                action_kind: Some("create_remote_dir".to_owned()),
                inode_id: Some(completed.dispatched.bound_identity.inode_id),
                client_file_id: Some(completed.dispatched.bound_identity.client_file_id),
                client_request_id: Some(completed.dispatched.request.client_request_id),
                committed_seq: Some(completed.dispatched.response.committed_seq),
                conflict_id: None,
                transfer: None,
            },
        },
        NextClientAction::ExecutedUploadLocalEdit(UploadLocalEditExecution::Progressed(
            progressed,
        )) => SyncOnceReport {
            namespace_id: namespace_id.clone(),
            outcome: SyncOnceOutcome::ProgressedTransfer,
            action_kind: Some("upload_local_edit".to_owned()),
            inode_id: None,
            client_file_id: None,
            client_request_id: None,
            committed_seq: None,
            conflict_id: None,
            transfer: Some(SyncOnceTransferReport {
                transfer_id: progressed.transfer.transfer_id,
                direction: progressed.transfer.direction,
                block_index: progressed.transfer.block_index,
                block_count: progressed.transfer.block_count,
                state: progressed.transfer.state,
            }),
        },
        NextClientAction::ExecutedUploadLocalEdit(UploadLocalEditExecution::Completed(
            completed,
        )) => SyncOnceReport {
            namespace_id: namespace_id.clone(),
            outcome: SyncOnceOutcome::RequestCommitted,
            action_kind: Some("upload_local_edit".to_owned()),
            inode_id: Some(completed.dispatched.applied.inode_id),
            client_file_id: None,
            client_request_id: Some(completed.dispatched.request.client_request_id),
            committed_seq: Some(completed.dispatched.response.committed_seq),
            conflict_id: None,
            transfer: None,
        },
        NextClientAction::ExecutedDownloadRemoteEdit(DownloadRemoteEditExecution::Progressed(
            progressed,
        )) => SyncOnceReport {
            namespace_id: namespace_id.clone(),
            outcome: SyncOnceOutcome::ProgressedTransfer,
            action_kind: Some("download_remote_edit".to_owned()),
            inode_id: None,
            client_file_id: None,
            client_request_id: None,
            committed_seq: None,
            conflict_id: None,
            transfer: Some(SyncOnceTransferReport {
                transfer_id: progressed.transfer.transfer_id,
                direction: progressed.transfer.direction,
                block_index: progressed.transfer.block_index,
                block_count: progressed.transfer.block_count,
                state: progressed.transfer.state,
            }),
        },
        NextClientAction::ExecutedDownloadRemoteEdit(DownloadRemoteEditExecution::Completed(
            completed,
        )) => SyncOnceReport {
            namespace_id: namespace_id.clone(),
            outcome: SyncOnceOutcome::AppliedRemoteChange,
            action_kind: Some("download_remote_edit".to_owned()),
            inode_id: Some(completed.applied.inode_id),
            client_file_id: None,
            client_request_id: None,
            committed_seq: None,
            conflict_id: None,
            transfer: None,
        },
        NextClientAction::ExecutedApplyRemoteRename(executed) => applied_report(
            namespace_id,
            "apply_remote_rename",
            executed.applied.inode_id,
        ),
        NextClientAction::ExecutedApplyRemoteRenameAndReplace(executed) => applied_report(
            namespace_id,
            "apply_remote_rename_and_replace",
            executed.applied.inode_id,
        ),
        NextClientAction::ExecutedApplyRemoteDelete(executed) => applied_report(
            namespace_id,
            "apply_remote_delete",
            executed.applied.inode_id,
        ),
        NextClientAction::ExecutedApplyRemoteSubtreeDelete(executed) => applied_report(
            namespace_id,
            "apply_remote_subtree_delete",
            executed.applied.inode_id,
        ),
        NextClientAction::ExecutedApplyRemoteSubtreeRename(executed) => applied_report(
            namespace_id,
            "apply_remote_subtree_rename",
            executed.applied.inode_id,
        ),
        NextClientAction::ExecutedMaterializeRemoteDir(executed) => applied_report(
            namespace_id,
            "materialize_remote_dir",
            executed.applied.inode_id,
        ),
        NextClientAction::ExecutedResolveSameInodeConflict(executed) => conflict_report(
            namespace_id,
            "resolve_same_inode_conflict",
            executed.applied.inode_id,
            executed.conflict_id,
        ),
        NextClientAction::ExecutedResolveDeleteVsEditConflict(executed) => conflict_report(
            namespace_id,
            "resolve_delete_vs_edit_conflict",
            executed.applied.inode_id,
            executed.conflict_id,
        ),
        NextClientAction::ExecutedResolveRenameVsEditConflict(executed) => conflict_report(
            namespace_id,
            "resolve_rename_vs_edit_conflict",
            executed.applied.inode_id,
            executed.conflict_id,
        ),
        NextClientAction::ExecutedResolvePathBindingCollision(executed) => conflict_report(
            namespace_id,
            "resolve_path_binding_collision",
            executed.applied.inode_id,
            executed.conflict_id,
        ),
        NextClientAction::ExecutedResolveSubtreeDeleteConflict(executed) => conflict_report(
            namespace_id,
            "resolve_subtree_delete_conflict",
            executed.applied.inode_id,
            executed.conflict_id,
        ),
        NextClientAction::ExecutedResolveSubtreeRenameConflict(executed) => conflict_report(
            namespace_id,
            "resolve_subtree_rename_conflict",
            executed.applied.inode_id,
            executed.conflict_id,
        ),
        NextClientAction::SelectedPlannedAction(_) => unreachable!("handled by caller"),
    }
}

fn applied_report(
    namespace_id: &NamespaceId,
    action_kind: &str,
    inode_id: InodeId,
) -> SyncOnceReport {
    SyncOnceReport {
        namespace_id: namespace_id.clone(),
        outcome: SyncOnceOutcome::AppliedRemoteChange,
        action_kind: Some(action_kind.to_owned()),
        inode_id: Some(inode_id),
        client_file_id: None,
        client_request_id: None,
        committed_seq: None,
        conflict_id: None,
        transfer: None,
    }
}

fn conflict_report(
    namespace_id: &NamespaceId,
    action_kind: &str,
    inode_id: InodeId,
    conflict_id: String,
) -> SyncOnceReport {
    SyncOnceReport {
        namespace_id: namespace_id.clone(),
        outcome: SyncOnceOutcome::ResolvedConflict,
        action_kind: Some(action_kind.to_owned()),
        inode_id: Some(inode_id),
        client_file_id: None,
        client_request_id: None,
        committed_seq: None,
        conflict_id: Some(conflict_id),
        transfer: None,
    }
}
