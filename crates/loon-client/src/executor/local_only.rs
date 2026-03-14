use super::dispatch::dispatch_client_mutation_from_state;
use super::*;
use crate::planner::{PlannedLocalOnlyActionRecord, PlannerDecision};
use crate::state_db::{ClientFileId, LocalOnlyUploadRow, SqliteStateDb, StateDbError};
use crate::upload::upload_small_file_from_path;
use loon_objectstore::ObjectStore;
use loon_types::{ClientMutationOp, ClientMutationRequest, ClientMutationResponse, InodeKind};
use std::path::Path;

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
            ClientMutationOp::ReplaceFile { .. } => Err(ExecuteLocalOnlyCreateError::Executor(
                ExecutorError::UnsupportedDecision(PlannerDecision::UploadLocalEdit),
            )),
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
