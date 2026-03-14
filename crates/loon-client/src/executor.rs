use crate::planner::{PlannedLocalOnlyActionRecord, PlannerDecision, PlannerError};
use crate::state_db::{
    BoundLocalOnlyFile, ClientFileId, LocalOnlyFileStateRow, PendingClientMutationRow,
    SqliteStateDb, StateDbError,
};
use loon_types::{ClientMutationOp, ClientMutationRequest, ClientMutationResponse, InodeKind};
use thiserror::Error;

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
    #[error("missing parent inode for `{client_file_id}`")]
    MissingParentInode { client_file_id: String },
    #[error("missing content manifest digest for `{client_file_id}`")]
    MissingContentManifestDigest { client_file_id: String },
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchedClientMutation {
    pub pending: PendingClientMutationRow,
    pub request: ClientMutationRequest,
    pub response: ClientMutationResponse,
    pub bound_identity: BoundLocalOnlyFile,
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

pub fn dispatch_client_mutation_from_state<F>(
    db: &mut SqliteStateDb,
    client_file_id: &ClientFileId,
    created_at_ms: u64,
    dispatch: F,
) -> Result<DispatchedClientMutation, DispatchClientMutationError>
where
    F: FnOnce(&ClientMutationRequest) -> Result<ClientMutationResponse, String>,
{
    let pending = match db.load_pending_client_mutation_for_client_file(client_file_id)? {
        Some(existing) => existing,
        None => {
            let client_request_id = db.allocate_client_request_id()?;
            let request =
                build_client_mutation_request_from_state(db, &client_request_id, client_file_id)?;
            db.record_pending_client_mutation(client_file_id, &request, created_at_ms)?
        }
    };
    let request = pending.request.clone();
    let response =
        dispatch(&request).map_err(|message| DispatchClientMutationError::DispatchFailed {
            client_request_id: request.client_request_id.clone(),
            message,
        })?;
    let bound_identity = db.apply_client_mutation_response(&response)?;

    Ok(DispatchedClientMutation {
        pending,
        request,
        response,
        bound_identity,
    })
}

pub fn build_client_mutation_request_from_state(
    db: &SqliteStateDb,
    client_request_id: &str,
    client_file_id: &ClientFileId,
) -> Result<ClientMutationRequest, ExecutorError> {
    let local_only = db.load_local_only_file(client_file_id)?.ok_or_else(|| {
        StateDbError::LocalOnlyFileMissing {
            client_file_id: client_file_id.as_str().to_owned(),
        }
    })?;
    let planned_row = db
        .load_planned_local_only_action(client_file_id)?
        .ok_or_else(|| ExecutorError::PlannedLocalOnlyActionMissing {
            client_file_id: client_file_id.as_str().to_owned(),
        })?;
    let planned = PlannedLocalOnlyActionRecord::try_from(planned_row)?;
    let content_manifest_digest = match planned.decision {
        PlannerDecision::UploadLocalCreate => {
            Some(db.resolve_local_only_upload_content_manifest_digest(&local_only)?)
        }
        _ => None,
    };

    build_client_mutation_request(
        client_request_id,
        &local_only,
        &planned,
        content_manifest_digest.as_deref(),
    )
}

pub fn build_client_mutation_request(
    client_request_id: &str,
    local_only: &LocalOnlyFileStateRow,
    planned: &PlannedLocalOnlyActionRecord,
    content_manifest_digest: Option<&str>,
) -> Result<ClientMutationRequest, ExecutorError> {
    if client_request_id.trim().is_empty() {
        return Err(ExecutorError::EmptyClientRequestId);
    }

    if local_only.client_file_id != planned.client_file_id {
        return Err(ExecutorError::ClientFileIdMismatch {
            local_client_file_id: local_only.client_file_id.as_str().to_owned(),
            planned_client_file_id: planned.client_file_id.as_str().to_owned(),
        });
    }

    if local_only.namespace_id != planned.namespace_id {
        return Err(ExecutorError::NamespaceMismatch {
            local_namespace_id: local_only.namespace_id.as_str().to_owned(),
            planned_namespace_id: planned.namespace_id.as_str().to_owned(),
        });
    }

    let parent_inode_id =
        local_only
            .parent_inode_id
            .ok_or_else(|| ExecutorError::MissingParentInode {
                client_file_id: local_only.client_file_id.as_str().to_owned(),
            })?;

    let op = match planned.decision {
        PlannerDecision::CreateRemoteDir => {
            if local_only.inode_kind != InodeKind::Dir {
                return Err(ExecutorError::DecisionKindMismatch {
                    client_file_id: local_only.client_file_id.as_str().to_owned(),
                    decision: planned.decision.clone(),
                    inode_kind: local_only.inode_kind.clone(),
                });
            }

            ClientMutationOp::CreateDir {
                parent_inode_id,
                display_name: local_only.display_name.clone(),
            }
        }
        PlannerDecision::UploadLocalCreate => {
            if local_only.inode_kind != InodeKind::File {
                return Err(ExecutorError::DecisionKindMismatch {
                    client_file_id: local_only.client_file_id.as_str().to_owned(),
                    decision: planned.decision.clone(),
                    inode_kind: local_only.inode_kind.clone(),
                });
            }

            ClientMutationOp::CreateFile {
                parent_inode_id,
                display_name: local_only.display_name.clone(),
                content_manifest_digest: content_manifest_digest.map(str::to_owned).ok_or_else(
                    || ExecutorError::MissingContentManifestDigest {
                        client_file_id: local_only.client_file_id.as_str().to_owned(),
                    },
                )?,
            }
        }
        ref other => return Err(ExecutorError::UnsupportedDecision(other.clone())),
    };

    Ok(ClientMutationRequest {
        namespace_id: local_only.namespace_id.clone(),
        client_request_id: client_request_id.to_owned(),
        op,
    })
}
