use super::*;
use crate::planner::{PlannedLocalOnlyActionRecord, PlannerDecision};
use crate::state_db::{
    ClientFileId, LocalOnlyTransferLedgerRow, LocalOnlyUploadRow, SqliteStateDb, StateDbError,
    TransferDirection, TransferState,
};
use crate::testing::{ClientExecutionHooks, NoopClientExecutionHooks};
use crate::upload::{
    finalize_planned_upload, plan_upload_from_path, upload_planned_block_from_path,
};
use loon_server::objectstore::ObjectStore;
use loon_types::{ClientMutationOp, ClientMutationRequest, ClientMutationResponse, InodeKind};
use serde_json::json;
use std::path::Path;

enum PreparedUploadLocalCreate {
    Ready {
        upload: LocalOnlyUploadRow,
        upload_reused: bool,
    },
    Progressed {
        transfer: LocalOnlyTransferLedgerRow,
    },
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
    execute_local_only_create_with_hooks(
        db,
        store,
        client_file_id,
        source_path,
        uploaded_at_ms,
        created_at_ms,
        &NoopClientExecutionHooks,
        dispatch,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_local_only_create_with_hooks<S: ObjectStore, F>(
    db: &mut SqliteStateDb,
    store: &S,
    client_file_id: &ClientFileId,
    source_path: Option<&Path>,
    uploaded_at_ms: u64,
    created_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
    dispatch: F,
) -> Result<ExecutedLocalOnlyCreate, ExecuteLocalOnlyCreateError>
where
    F: FnOnce(&ClientMutationRequest) -> Result<ClientMutationResponse, String>,
{
    if let Some(pending) = db.load_pending_client_mutation_for_client_file(client_file_id)? {
        return match pending.request.op {
            ClientMutationOp::CreateDir { .. } => execute_create_remote_dir_with_hooks(
                db,
                client_file_id,
                created_at_ms,
                hooks,
                dispatch,
            )
            .map(ExecutedLocalOnlyCreate::CreateRemoteDir)
            .map_err(ExecuteLocalOnlyCreateError::from),
            ClientMutationOp::CreateFile { .. } => execute_upload_local_create_with_hooks(
                db,
                store,
                client_file_id,
                source_path,
                uploaded_at_ms,
                created_at_ms,
                hooks,
                dispatch,
            )
            .map(ExecutedLocalOnlyCreate::UploadLocalCreate)
            .map_err(ExecuteLocalOnlyCreateError::from),
            ClientMutationOp::ReplaceFile { .. }
            | ClientMutationOp::Rename { .. }
            | ClientMutationOp::DeleteFile { .. }
            | ClientMutationOp::DeleteSubtree { .. } => Err(ExecuteLocalOnlyCreateError::Executor(
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
        PlannerDecision::UploadLocalCreate => execute_upload_local_create_with_hooks(
            db,
            store,
            client_file_id,
            source_path,
            uploaded_at_ms,
            created_at_ms,
            hooks,
            dispatch,
        )
        .map(ExecutedLocalOnlyCreate::UploadLocalCreate)
        .map_err(ExecuteLocalOnlyCreateError::from),
        PlannerDecision::CreateRemoteDir => {
            execute_create_remote_dir_with_hooks(db, client_file_id, created_at_ms, hooks, dispatch)
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
    execute_create_remote_dir_with_hooks(
        db,
        client_file_id,
        created_at_ms,
        &NoopClientExecutionHooks,
        dispatch,
    )
}

pub(crate) fn execute_create_remote_dir_with_hooks<F>(
    db: &mut SqliteStateDb,
    client_file_id: &ClientFileId,
    created_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
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

    let dispatched = super::dispatch::dispatch_client_mutation_from_state_with_hooks(
        db,
        client_file_id,
        created_at_ms,
        hooks,
        dispatch,
    )?;

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
) -> Result<UploadLocalCreateExecution, ExecuteUploadLocalCreateError>
where
    F: FnOnce(&ClientMutationRequest) -> Result<ClientMutationResponse, String>,
{
    execute_upload_local_create_with_hooks(
        db,
        store,
        client_file_id,
        Some(source_path),
        uploaded_at_ms,
        created_at_ms,
        &NoopClientExecutionHooks,
        dispatch,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_upload_local_create_with_hooks<S: ObjectStore, F>(
    db: &mut SqliteStateDb,
    store: &S,
    client_file_id: &ClientFileId,
    source_path: Option<&Path>,
    uploaded_at_ms: u64,
    created_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
    dispatch: F,
) -> Result<UploadLocalCreateExecution, ExecuteUploadLocalCreateError>
where
    F: FnOnce(&ClientMutationRequest) -> Result<ClientMutationResponse, String>,
{
    let result = (|| {
        let had_pending_request = db
            .load_pending_client_mutation_for_client_file(client_file_id)?
            .is_some();

        let prepared = if had_pending_request {
            PreparedUploadLocalCreate::Ready {
                upload: db
                    .load_local_only_upload(client_file_id)?
                    .expect("pending client mutation should have durable upload"),
                upload_reused: true,
            }
        } else {
            ensure_upload_local_create_ready(
                db,
                store,
                client_file_id,
                source_path,
                uploaded_at_ms,
            )?
        };
        hooks.checkpoint("client.local_only_create.upload_ready");

        let (ensured_upload, upload_reused) = match prepared {
            PreparedUploadLocalCreate::Ready {
                upload,
                upload_reused,
            } => (Some(upload), upload_reused),
            PreparedUploadLocalCreate::Progressed { transfer } => {
                return Ok(UploadLocalCreateExecution::Progressed(
                    ProgressedUploadLocalCreate { transfer },
                ));
            }
        };

        let dispatched = super::dispatch::dispatch_client_mutation_from_state_with_hooks(
            db,
            client_file_id,
            created_at_ms,
            hooks,
            dispatch,
        )?;

        Ok(UploadLocalCreateExecution::Completed(
            ExecutedUploadLocalCreate {
                ensured_upload,
                upload_reused,
                dispatched,
            },
        ))
    })();

    match &result {
        Ok(UploadLocalCreateExecution::Completed(_)) => {
            clear_upload_local_create_issues(db, client_file_id)
        }
        Ok(UploadLocalCreateExecution::Progressed(_)) => {
            clear_upload_local_create_failure_issue(db, client_file_id)
        }
        Err(error) => record_upload_local_create_issue(db, client_file_id, error, uploaded_at_ms),
    }

    result
}

fn ensure_upload_local_create_ready<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    client_file_id: &ClientFileId,
    source_path: Option<&Path>,
    uploaded_at_ms: u64,
) -> Result<PreparedUploadLocalCreate, ExecuteUploadLocalCreateError> {
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
            db.delete_local_only_transfer_ledger(client_file_id, TransferDirection::Upload)?;
            return Ok(PreparedUploadLocalCreate::Ready {
                upload: existing_upload,
                upload_reused: true,
            });
        }
    }

    let source_path =
        source_path.ok_or_else(|| ExecuteUploadLocalCreateError::SourcePathMissing {
            client_file_id: client_file_id.as_str().to_owned(),
        })?;
    let plan = plan_upload_from_path(&local_only.namespace_id, source_path)?;
    let block_count = u64::try_from(plan.blocks.len()).expect("block count should fit in u64");
    let transfer_id = local_only_upload_transfer_id(client_file_id, &plan.content_manifest_digest);
    let existing_transfer =
        db.load_local_only_transfer_ledger(client_file_id, TransferDirection::Upload)?;
    let (requested_block_index, reset_reason) = reconcile_local_only_upload_transfer_resume(
        existing_transfer.as_ref(),
        &transfer_id,
        &plan.manifest_object_key,
        block_count,
    );
    if let Some(reason) = reset_reason {
        record_upload_local_create_transfer_reset(
            db,
            client_file_id,
            &local_only.namespace_id,
            reason,
            uploaded_at_ms,
        );
    }
    let resume_block_index = requested_block_index.min(block_count);
    let current_transfer = local_only_upload_transfer_row(
        client_file_id,
        &local_only.namespace_id,
        &transfer_id,
        &plan.manifest_object_key,
        resume_block_index,
        block_count,
        uploaded_at_ms,
    );
    db.upsert_local_only_transfer_ledger(&current_transfer)?;
    if usize::try_from(resume_block_index).expect("resume block index should fit")
        < plan.blocks.len()
    {
        upload_planned_block_from_path(store, source_path, &plan, resume_block_index)?;
        let next_block_index = resume_block_index.saturating_add(1);
        let next_transfer = local_only_upload_transfer_row(
            client_file_id,
            &local_only.namespace_id,
            &transfer_id,
            &plan.manifest_object_key,
            next_block_index,
            block_count,
            uploaded_at_ms,
        );
        db.upsert_local_only_transfer_ledger(&next_transfer)?;
        if next_block_index < block_count {
            return Ok(PreparedUploadLocalCreate::Progressed {
                transfer: next_transfer,
            });
        }
    }
    let uploaded = finalize_planned_upload(store, &plan)?;
    let recorded = db
        .record_local_only_upload(client_file_id, &uploaded, uploaded_at_ms)
        .map_err(ExecuteUploadLocalCreateError::from)?;
    db.delete_local_only_transfer_ledger(client_file_id, TransferDirection::Upload)?;
    Ok(PreparedUploadLocalCreate::Ready {
        upload: recorded,
        upload_reused: false,
    })
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

fn local_only_upload_transfer_id(
    client_file_id: &ClientFileId,
    content_manifest_digest: &str,
) -> String {
    format!(
        "upload-local-only:{}:{}",
        client_file_id.as_str(),
        content_manifest_digest
    )
}

fn local_only_upload_transfer_row(
    client_file_id: &ClientFileId,
    namespace_id: &loon_types::NamespaceId,
    transfer_id: &str,
    object_key: &str,
    block_index: u64,
    block_count: u64,
    updated_at_ms: u64,
) -> LocalOnlyTransferLedgerRow {
    LocalOnlyTransferLedgerRow {
        client_file_id: client_file_id.clone(),
        namespace_id: namespace_id.clone(),
        transfer_id: transfer_id.to_owned(),
        direction: TransferDirection::Upload,
        object_key: object_key.to_owned(),
        block_index,
        block_count,
        state: TransferState::Uploading,
        updated_at_ms,
    }
}

fn reconcile_local_only_upload_transfer_resume(
    existing: Option<&LocalOnlyTransferLedgerRow>,
    expected_transfer_id: &str,
    expected_object_key: &str,
    expected_block_count: u64,
) -> (u64, Option<&'static str>) {
    match existing {
        Some(existing) if existing.transfer_id != expected_transfer_id => {
            (0, Some("transfer_id_mismatch"))
        }
        Some(existing) if existing.object_key != expected_object_key => {
            (0, Some("object_key_mismatch"))
        }
        Some(existing) if existing.block_count != expected_block_count => {
            (0, Some("block_count_mismatch"))
        }
        Some(existing) if existing.state != TransferState::Uploading => {
            (0, Some("transfer_id_mismatch"))
        }
        Some(existing) => (existing.block_index.min(expected_block_count), None),
        None => (0, None),
    }
}

fn clear_upload_local_create_failure_issue(db: &mut SqliteStateDb, client_file_id: &ClientFileId) {
    let _ = db.clear_local_only_conflict_or_error_kind(
        client_file_id,
        "upload_local_create_upload_failed",
    );
}

fn clear_upload_local_create_issues(db: &mut SqliteStateDb, client_file_id: &ClientFileId) {
    clear_upload_local_create_failure_issue(db, client_file_id);
    let _ = db.clear_local_only_conflict_or_error_kind(
        client_file_id,
        "upload_local_create_transfer_reset",
    );
}

fn record_upload_local_create_transfer_reset(
    db: &mut SqliteStateDb,
    client_file_id: &ClientFileId,
    namespace_id: &loon_types::NamespaceId,
    reason: &'static str,
    created_at_ms: u64,
) {
    let _ = db.record_local_only_conflict_or_error(
        client_file_id,
        namespace_id,
        "upload_local_create_transfer_reset",
        "upload_local_create discarded stale transfer state and restarted from block 0",
        &json!({
            "reason": reason,
        }),
        created_at_ms,
    );
}

fn record_upload_local_create_issue(
    db: &mut SqliteStateDb,
    client_file_id: &ClientFileId,
    error: &ExecuteUploadLocalCreateError,
    created_at_ms: u64,
) {
    let namespace_id = match db
        .load_local_only_file(client_file_id)
        .ok()
        .flatten()
        .map(|row| row.namespace_id)
    {
        Some(namespace_id) => namespace_id,
        None => return,
    };

    let issue = match error {
        ExecuteUploadLocalCreateError::SourcePathMissing { .. } => Some((
            "upload_local_create_upload_failed",
            "upload_local_create could not prepare durable local content for upload",
            json!({
                "failure": "source_path_missing",
            }),
        )),
        ExecuteUploadLocalCreateError::Upload(upload_error) => Some((
            "upload_local_create_upload_failed",
            "upload_local_create could not prepare durable local content for upload",
            super::inode::upload_error_detail_json(upload_error),
        )),
        _ => None,
    };

    if let Some((kind, summary, detail_json)) = issue {
        let _ = db.record_local_only_conflict_or_error(
            client_file_id,
            &namespace_id,
            kind,
            summary,
            &detail_json,
            created_at_ms,
        );
    }
}
