use crate::planner::{PlannedLocalOnlyActionRecord, PlannerDecision, PlannerError};
use crate::state_db::{
    BoundLocalOnlyFile, ClientFileId, LocalOnlyFileStateRow, LocalOnlyUploadRow,
    PendingClientMutationRow, SqliteStateDb, StateDbError,
};
use crate::upload::{upload_small_file_from_path, UploadError};
use loon_objectstore::ObjectStore;
use loon_types::{ClientMutationOp, ClientMutationRequest, ClientMutationResponse, InodeKind};
use std::path::Path;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedUploadLocalCreate {
    pub ensured_upload: Option<LocalOnlyUploadRow>,
    pub upload_reused: bool,
    pub dispatched: DispatchedClientMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedCreateRemoteDir {
    pub reused_pending_request: bool,
    pub dispatched: DispatchedClientMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutedLocalOnlyCreate {
    UploadLocalCreate(ExecutedUploadLocalCreate),
    CreateRemoteDir(ExecutedCreateRemoteDir),
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

pub fn execute_local_only_create<S: ObjectStore, F>(
    db: &mut SqliteStateDb,
    store: &S,
    client_file_id: &ClientFileId,
    source_path: Option<&Path>,
    uploaded_at_ms: u64,
    created_at_ms: u64,
    dispatch: F,
) -> Result<ExecutedLocalOnlyCreate, ExecuteLocalOnlyCreateError>
where
    F: FnOnce(&ClientMutationRequest) -> Result<ClientMutationResponse, String>,
{
    if let Some(pending) = db.load_pending_client_mutation_for_client_file(client_file_id)? {
        return match pending.request.op {
            ClientMutationOp::CreateDir { .. } => {
                execute_create_remote_dir(db, client_file_id, created_at_ms, dispatch)
                    .map(ExecutedLocalOnlyCreate::CreateRemoteDir)
                    .map_err(ExecuteLocalOnlyCreateError::from)
            }
            ClientMutationOp::CreateFile { .. } => execute_upload_local_create(
                db,
                store,
                client_file_id,
                source_path,
                uploaded_at_ms,
                created_at_ms,
                dispatch,
            )
            .map(ExecutedLocalOnlyCreate::UploadLocalCreate)
            .map_err(ExecuteLocalOnlyCreateError::from),
        };
    }

    let planned_row = db
        .load_planned_local_only_action(client_file_id)?
        .ok_or_else(|| ExecutorError::PlannedLocalOnlyActionMissing {
            client_file_id: client_file_id.as_str().to_owned(),
        })?;
    let planned = PlannedLocalOnlyActionRecord::try_from(planned_row)?;

    match planned.decision {
        PlannerDecision::UploadLocalCreate => execute_upload_local_create(
            db,
            store,
            client_file_id,
            source_path,
            uploaded_at_ms,
            created_at_ms,
            dispatch,
        )
        .map(ExecutedLocalOnlyCreate::UploadLocalCreate)
        .map_err(ExecuteLocalOnlyCreateError::from),
        PlannerDecision::CreateRemoteDir => {
            execute_create_remote_dir(db, client_file_id, created_at_ms, dispatch)
                .map(ExecutedLocalOnlyCreate::CreateRemoteDir)
                .map_err(ExecuteLocalOnlyCreateError::from)
        }
        ref other => Err(ExecuteLocalOnlyCreateError::Executor(
            ExecutorError::UnsupportedDecision(other.clone()),
        )),
    }
}

pub fn execute_create_remote_dir<F>(
    db: &mut SqliteStateDb,
    client_file_id: &ClientFileId,
    created_at_ms: u64,
    dispatch: F,
) -> Result<ExecutedCreateRemoteDir, ExecuteCreateRemoteDirError>
where
    F: FnOnce(&ClientMutationRequest) -> Result<ClientMutationResponse, String>,
{
    let reused_pending_request = db
        .load_pending_client_mutation_for_client_file(client_file_id)?
        .is_some();

    if !reused_pending_request {
        ensure_create_remote_dir_ready(db, client_file_id)?;
    }

    let dispatched =
        dispatch_client_mutation_from_state(db, client_file_id, created_at_ms, dispatch)?;

    Ok(ExecutedCreateRemoteDir {
        reused_pending_request,
        dispatched,
    })
}

pub fn execute_upload_local_create_from_path<S: ObjectStore, F>(
    db: &mut SqliteStateDb,
    store: &S,
    client_file_id: &ClientFileId,
    source_path: &Path,
    uploaded_at_ms: u64,
    created_at_ms: u64,
    dispatch: F,
) -> Result<ExecutedUploadLocalCreate, ExecuteUploadLocalCreateError>
where
    F: FnOnce(&ClientMutationRequest) -> Result<ClientMutationResponse, String>,
{
    execute_upload_local_create(
        db,
        store,
        client_file_id,
        Some(source_path),
        uploaded_at_ms,
        created_at_ms,
        dispatch,
    )
}

fn execute_upload_local_create<S: ObjectStore, F>(
    db: &mut SqliteStateDb,
    store: &S,
    client_file_id: &ClientFileId,
    source_path: Option<&Path>,
    uploaded_at_ms: u64,
    created_at_ms: u64,
    dispatch: F,
) -> Result<ExecutedUploadLocalCreate, ExecuteUploadLocalCreateError>
where
    F: FnOnce(&ClientMutationRequest) -> Result<ClientMutationResponse, String>,
{
    let had_pending_request = db
        .load_pending_client_mutation_for_client_file(client_file_id)?
        .is_some();

    let (ensured_upload, upload_reused) = if had_pending_request {
        (db.load_local_only_upload(client_file_id)?, true)
    } else {
        let (upload_row, reused_existing) = ensure_upload_local_create_ready(
            db,
            store,
            client_file_id,
            source_path,
            uploaded_at_ms,
        )?;
        (Some(upload_row), reused_existing)
    };

    let dispatched =
        dispatch_client_mutation_from_state(db, client_file_id, created_at_ms, dispatch)?;

    Ok(ExecutedUploadLocalCreate {
        ensured_upload,
        upload_reused,
        dispatched,
    })
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

fn ensure_upload_local_create_ready<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    client_file_id: &ClientFileId,
    source_path: Option<&Path>,
    uploaded_at_ms: u64,
) -> Result<(LocalOnlyUploadRow, bool), ExecuteUploadLocalCreateError> {
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
    if planned.decision != PlannerDecision::UploadLocalCreate {
        return Err(
            ExecuteUploadLocalCreateError::UploadLocalCreateDecisionMissing {
                client_file_id: client_file_id.as_str().to_owned(),
                decision: planned.decision,
            },
        );
    }

    if let Some(existing_upload) = db.load_local_only_upload(client_file_id)? {
        if existing_upload.namespace_id == local_only.namespace_id
            && local_only.content_digest.as_deref()
                == Some(existing_upload.file_digest_sha256.as_str())
        {
            return Ok((existing_upload, true));
        }
    }

    let source_path =
        source_path.ok_or_else(|| ExecuteUploadLocalCreateError::SourcePathMissing {
            client_file_id: client_file_id.as_str().to_owned(),
        })?;
    let uploaded = upload_small_file_from_path(store, &local_only.namespace_id, source_path)?;
    let recorded = db
        .record_local_only_upload(client_file_id, &uploaded, uploaded_at_ms)
        .map_err(ExecuteUploadLocalCreateError::from)?;
    Ok((recorded, false))
}

fn ensure_create_remote_dir_ready(
    db: &mut SqliteStateDb,
    client_file_id: &ClientFileId,
) -> Result<(), ExecuteCreateRemoteDirError> {
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
    if planned.decision != PlannerDecision::CreateRemoteDir {
        return Err(
            ExecuteCreateRemoteDirError::CreateRemoteDirDecisionMissing {
                client_file_id: client_file_id.as_str().to_owned(),
                decision: planned.decision,
            },
        );
    }

    if local_only.inode_kind != InodeKind::Dir {
        return Err(ExecutorError::DecisionKindMismatch {
            client_file_id: client_file_id.as_str().to_owned(),
            decision: planned.decision,
            inode_kind: local_only.inode_kind,
        }
        .into());
    }

    Ok(())
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
