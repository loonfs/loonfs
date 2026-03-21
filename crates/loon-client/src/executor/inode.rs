use super::dispatch::dispatch_inode_mutation_from_state;
use super::*;
use crate::download::{
    expected_staged_file_size, load_content_manifest, load_validated_content_block,
    verify_downloaded_file_path,
};
use crate::local_apply::{
    append_stage_bytes, create_directory_durably, finalize_stage_file, remove_path_durably,
    remove_tree_durably, rename_path_durably, reset_stage_file, stage_file_size,
    staging_path_for_target,
};
use crate::planner::{PlannedActionRecord, PlannerDecision};
use crate::state_db::{
    BoundApplyRemoteSubtreeDeleteViews, BoundApplyRemoteSubtreeRenameViews, InodeUploadRow,
    RemoteFileStateRow, SqliteStateDb, TransferDirection, TransferLedgerRow, TransferState,
};
use crate::upload::{
    finalize_planned_upload, plan_upload_from_path, upload_planned_block_from_path,
};
use loon_objectstore::ObjectStore;
use loon_types::{ClientMutationRequest, ClientMutationResponse, InodeId, InodeKind, NamespaceId};
use serde_json::json;
use std::path::Path;

enum PreparedUploadLocalEdit {
    Ready {
        upload: InodeUploadRow,
        upload_reused: bool,
    },
    Progressed {
        transfer: TransferLedgerRow,
    },
}

pub fn execute_upload_local_edit_from_path<S: ObjectStore, F>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    source_path: &Path,
    uploaded_at_ms: u64,
    created_at_ms: u64,
    dispatch: F,
) -> Result<UploadLocalEditExecution, ExecuteUploadLocalEditError>
where
    F: FnOnce(&ClientMutationRequest) -> Result<ClientMutationResponse, String>,
{
    execute_upload_local_edit(
        db,
        store,
        namespace_id,
        inode_id,
        Some(source_path),
        uploaded_at_ms,
        created_at_ms,
        dispatch,
    )
}

pub fn execute_download_remote_edit_to_path<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    target_path: &Path,
    applied_at_ms: u64,
) -> Result<DownloadRemoteEditExecution, ExecuteDownloadRemoteEditError> {
    execute_download_remote_edit(
        db,
        store,
        namespace_id,
        inode_id,
        Some(target_path),
        applied_at_ms,
    )
}

pub fn execute_apply_remote_rename_to_paths(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: &Path,
    target_path: &Path,
    applied_at_ms: u64,
) -> Result<ExecutedApplyRemoteRename, ExecuteApplyRemoteRenameError> {
    execute_apply_remote_rename(
        db,
        namespace_id,
        inode_id,
        Some(current_path),
        Some(target_path),
        applied_at_ms,
    )
}

pub fn execute_apply_remote_delete_to_path(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: &Path,
    applied_at_ms: u64,
) -> Result<ExecutedApplyRemoteDelete, ExecuteApplyRemoteDeleteError> {
    execute_apply_remote_delete(
        db,
        namespace_id,
        inode_id,
        Some(current_path),
        applied_at_ms,
    )
}

pub fn execute_apply_remote_subtree_delete_to_path(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: &Path,
    applied_at_ms: u64,
) -> Result<ExecutedApplyRemoteSubtreeDelete, ExecuteApplyRemoteSubtreeDeleteError> {
    execute_apply_remote_subtree_delete(
        db,
        namespace_id,
        inode_id,
        Some(current_path),
        applied_at_ms,
    )
}

pub fn execute_apply_remote_subtree_rename_to_paths(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: &Path,
    target_path: &Path,
    applied_at_ms: u64,
) -> Result<ExecutedApplyRemoteSubtreeRename, ExecuteApplyRemoteSubtreeRenameError> {
    execute_apply_remote_subtree_rename(
        db,
        namespace_id,
        inode_id,
        Some(current_path),
        Some(target_path),
        applied_at_ms,
    )
}

pub fn execute_materialize_remote_dir_to_path<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    target_path: &Path,
    applied_at_ms: u64,
) -> Result<ExecutedMaterializeRemoteDir, ExecuteMaterializeRemoteDirError> {
    let _ = store;
    execute_materialize_remote_dir(db, namespace_id, inode_id, Some(target_path), applied_at_ms)
}

pub(super) fn execute_apply_remote_rename(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    target_path: Option<&Path>,
    applied_at_ms: u64,
) -> Result<ExecutedApplyRemoteRename, ExecuteApplyRemoteRenameError> {
    let result = (|| {
        let (remote, _local, _anchor) =
            ensure_apply_remote_rename_ready(db, namespace_id, inode_id)?;
        let current_path =
            current_path.ok_or_else(|| ExecuteApplyRemoteRenameError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
        if !current_path.exists() {
            return Err(ExecuteApplyRemoteRenameError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            });
        }

        let target_path =
            target_path.ok_or_else(|| ExecuteApplyRemoteRenameError::TargetPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
        let target_parent_exists = target_path.parent().is_some_and(|parent| parent.exists());
        if !target_parent_exists || target_path == current_path {
            return Err(ExecuteApplyRemoteRenameError::TargetPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            });
        }
        if target_path.exists() {
            return Err(ExecuteApplyRemoteRenameError::DestinationOccupied {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                path: target_path.display().to_string(),
            });
        }

        rename_path_durably(current_path, target_path)?;

        let applied = db.apply_remote_rename(namespace_id, inode_id, applied_at_ms)?;
        debug_assert_eq!(applied.namespace_id, remote.namespace_id);
        debug_assert_eq!(applied.inode_id, remote.inode_id);
        Ok(ExecutedApplyRemoteRename { applied })
    })();

    if let Err(error) = &result {
        record_apply_remote_rename_issue(db, namespace_id, inode_id, error, applied_at_ms);
    }

    result
}

pub(super) fn execute_apply_remote_delete(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    applied_at_ms: u64,
) -> Result<ExecutedApplyRemoteDelete, ExecuteApplyRemoteDeleteError> {
    let result = (|| {
        let (remote, _local, _anchor) =
            ensure_apply_remote_delete_ready(db, namespace_id, inode_id)?;
        let current_path =
            current_path.ok_or_else(|| ExecuteApplyRemoteDeleteError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
        if !current_path.exists() {
            return Err(ExecuteApplyRemoteDeleteError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            });
        }

        remove_path_durably(current_path)?;

        let applied = db.apply_remote_delete(namespace_id, inode_id, applied_at_ms)?;
        debug_assert_eq!(applied.namespace_id, remote.namespace_id);
        debug_assert_eq!(applied.inode_id, remote.inode_id);
        Ok(ExecutedApplyRemoteDelete { applied })
    })();

    if let Err(error) = &result {
        record_apply_remote_delete_issue(db, namespace_id, inode_id, error, applied_at_ms);
    }

    result
}

pub(super) fn execute_apply_remote_subtree_delete(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    applied_at_ms: u64,
) -> Result<ExecutedApplyRemoteSubtreeDelete, ExecuteApplyRemoteSubtreeDeleteError> {
    let result = (|| {
        let BoundApplyRemoteSubtreeDeleteViews { root_remote, .. } =
            ensure_apply_remote_subtree_delete_ready(db, namespace_id, inode_id)?;
        let current_path = current_path.ok_or_else(|| {
            ExecuteApplyRemoteSubtreeDeleteError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            }
        })?;
        if !current_path.exists() {
            return Err(ExecuteApplyRemoteSubtreeDeleteError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            });
        }

        remove_tree_durably(current_path)?;

        let applied = db.apply_remote_subtree_delete(namespace_id, inode_id, applied_at_ms)?;
        debug_assert_eq!(applied.namespace_id, root_remote.namespace_id);
        debug_assert_eq!(applied.inode_id, root_remote.inode_id);
        Ok(ExecutedApplyRemoteSubtreeDelete { applied })
    })();

    if let Err(error) = &result {
        record_apply_remote_subtree_delete_issue(db, namespace_id, inode_id, error, applied_at_ms);
    }

    result
}

pub(super) fn execute_apply_remote_subtree_rename(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    target_path: Option<&Path>,
    applied_at_ms: u64,
) -> Result<ExecutedApplyRemoteSubtreeRename, ExecuteApplyRemoteSubtreeRenameError> {
    let result = (|| {
        let BoundApplyRemoteSubtreeRenameViews { root_remote, .. } =
            ensure_apply_remote_subtree_rename_ready(db, namespace_id, inode_id)?;
        let current_path = current_path.ok_or_else(|| {
            ExecuteApplyRemoteSubtreeRenameError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            }
        })?;
        if !current_path.exists() {
            return Err(ExecuteApplyRemoteSubtreeRenameError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            });
        }

        let target_path =
            target_path.ok_or_else(|| ExecuteApplyRemoteSubtreeRenameError::TargetPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
        let target_parent_exists = target_path.parent().is_some_and(|parent| parent.exists());
        if !target_parent_exists || target_path == current_path {
            return Err(ExecuteApplyRemoteSubtreeRenameError::TargetPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            });
        }
        if target_path.exists() {
            return Err(ExecuteApplyRemoteSubtreeRenameError::DestinationOccupied {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                path: target_path.display().to_string(),
            });
        }

        rename_path_durably(current_path, target_path)?;

        let applied = db.apply_remote_subtree_rename(namespace_id, inode_id, applied_at_ms)?;
        debug_assert_eq!(applied.namespace_id, root_remote.namespace_id);
        debug_assert_eq!(applied.inode_id, root_remote.inode_id);
        Ok(ExecutedApplyRemoteSubtreeRename { applied })
    })();

    if let Err(error) = &result {
        record_apply_remote_subtree_rename_issue(db, namespace_id, inode_id, error, applied_at_ms);
    }

    result
}

pub(super) fn execute_upload_local_edit<S: ObjectStore, F>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    source_path: Option<&Path>,
    uploaded_at_ms: u64,
    created_at_ms: u64,
    dispatch: F,
) -> Result<UploadLocalEditExecution, ExecuteUploadLocalEditError>
where
    F: FnOnce(&ClientMutationRequest) -> Result<ClientMutationResponse, String>,
{
    let result = (|| {
        let had_pending_request = db
            .load_pending_inode_mutation_for_inode(namespace_id, inode_id)?
            .is_some();

        let prepared = if had_pending_request {
            PreparedUploadLocalEdit::Ready {
                upload: db
                    .load_inode_upload(namespace_id, inode_id)?
                    .expect("pending inode mutation should have durable upload"),
                upload_reused: true,
            }
        } else {
            ensure_upload_local_edit_ready(
                db,
                store,
                namespace_id,
                inode_id,
                source_path,
                uploaded_at_ms,
            )?
        };

        let (ensured_upload, upload_reused) = match prepared {
            PreparedUploadLocalEdit::Ready {
                upload,
                upload_reused,
            } => (Some(upload), upload_reused),
            PreparedUploadLocalEdit::Progressed { transfer } => {
                return Ok(UploadLocalEditExecution::Progressed(
                    ProgressedUploadLocalEdit { transfer },
                ));
            }
        };

        let dispatched = dispatch_inode_mutation_from_state(
            db,
            namespace_id,
            inode_id,
            created_at_ms,
            dispatch,
        )?;

        Ok(UploadLocalEditExecution::Completed(
            ExecutedUploadLocalEdit {
                ensured_upload,
                upload_reused,
                dispatched,
            },
        ))
    })();

    match &result {
        Ok(UploadLocalEditExecution::Completed(_)) => {
            clear_upload_local_edit_issues(db, namespace_id, inode_id)
        }
        Ok(UploadLocalEditExecution::Progressed(_)) => {
            clear_upload_local_edit_failure_issue(db, namespace_id, inode_id)
        }
        Err(error) => {
            record_upload_local_edit_issue(db, namespace_id, inode_id, error, uploaded_at_ms)
        }
    }

    result
}

pub(super) fn execute_download_remote_edit<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    target_path: Option<&Path>,
    applied_at_ms: u64,
) -> Result<DownloadRemoteEditExecution, ExecuteDownloadRemoteEditError> {
    let result = (|| {
        let remote = ensure_download_remote_edit_ready(db, namespace_id, inode_id)?;
        let target_path =
            target_path.ok_or_else(|| ExecuteDownloadRemoteEditError::SourcePathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
        let manifest_digest = remote
            .content_manifest_digest
            .as_deref()
            .expect("download_remote_edit should require manifest digest");
        let loaded_manifest = load_content_manifest(store, namespace_id, manifest_digest)?;
        let remote_content_digest = remote
            .content_digest
            .as_deref()
            .expect("download_remote_edit should require remote content digest");
        let block_count = u64::try_from(loaded_manifest.manifest_envelope.payload.blocks.len())
            .expect("block count should fit in u64");
        let transfer_id = download_transfer_id(namespace_id, inode_id, manifest_digest);
        let existing_transfer =
            db.load_transfer_ledger_for_inode(namespace_id, inode_id, TransferDirection::Download)?;
        let (requested_block_index, reset_reason) = reconcile_download_transfer_resume(
            existing_transfer.as_ref(),
            &transfer_id,
            &loaded_manifest.object_key,
            block_count,
        );
        let staged_size_bytes = stage_file_size(target_path)?.unwrap_or(0);
        let resume_block_index = if staged_size_bytes
            == expected_staged_file_size(&loaded_manifest.manifest_envelope, requested_block_index)
        {
            requested_block_index
        } else {
            0
        };

        let stage_reset =
            resume_block_index == 0 && (requested_block_index != 0 || staged_size_bytes != 0);
        let effective_reset_reason = if stage_reset {
            Some("stage_size_mismatch")
        } else {
            reset_reason
        };

        if resume_block_index == 0 && (existing_transfer.is_some() || staged_size_bytes != 0) {
            reset_stage_file(target_path)?;
        }

        if let Some(reason) = effective_reset_reason {
            record_download_remote_edit_transfer_reset(
                db,
                namespace_id,
                inode_id,
                reason,
                applied_at_ms,
            );
        }

        let current_transfer = download_transfer_row(
            namespace_id,
            inode_id,
            &transfer_id,
            &loaded_manifest.object_key,
            resume_block_index,
            block_count,
            applied_at_ms,
        );
        db.upsert_transfer_ledger(&current_transfer)?;

        if usize::try_from(resume_block_index).expect("resume block index should fit")
            < loaded_manifest.manifest_envelope.payload.blocks.len()
        {
            let block = &loaded_manifest.manifest_envelope.payload.blocks
                [usize::try_from(resume_block_index).expect("resume block index should fit")];
            let block_bytes = load_validated_content_block(store, namespace_id, block)?;
            append_stage_bytes(target_path, &block_bytes)?;
            let next_block_index = resume_block_index.saturating_add(1);
            let next_transfer = download_transfer_row(
                namespace_id,
                inode_id,
                &transfer_id,
                &loaded_manifest.object_key,
                next_block_index,
                block_count,
                applied_at_ms,
            );
            db.upsert_transfer_ledger(&next_transfer)?;

            if next_block_index < block_count {
                return Ok(DownloadRemoteEditExecution::Progressed(
                    ProgressedDownloadRemoteEdit {
                        transfer: next_transfer,
                    },
                ));
            }
        }

        let stage_path = staging_path_for_target(target_path)?;
        let downloaded = verify_downloaded_file_path(&loaded_manifest, &stage_path)?;
        if downloaded.file_digest_sha256 != remote_content_digest {
            return Err(ExecuteDownloadRemoteEditError::RemoteDigestMismatch {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                remote_content_digest: remote_content_digest.to_owned(),
                downloaded_file_digest: downloaded.file_digest_sha256,
            });
        }

        finalize_stage_file(target_path)?;

        let applied = db.apply_download_remote_edit(namespace_id, inode_id, applied_at_ms)?;
        Ok(DownloadRemoteEditExecution::Completed(
            ExecutedDownloadRemoteEdit {
                downloaded_content_manifest_digest: downloaded.content_manifest_digest,
                downloaded_file_digest_sha256: downloaded.file_digest_sha256,
                applied,
            },
        ))
    })();

    if let Err(error) = &result {
        record_download_remote_edit_issue(db, namespace_id, inode_id, error, applied_at_ms);
    }

    result
}

fn download_transfer_id(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    content_manifest_digest: &str,
) -> String {
    format!(
        "download:{}:{}:{}",
        namespace_id.as_str(),
        inode_id.0,
        content_manifest_digest
    )
}

fn download_transfer_row(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    transfer_id: &str,
    object_key: &str,
    block_index: u64,
    block_count: u64,
    updated_at_ms: u64,
) -> TransferLedgerRow {
    TransferLedgerRow {
        namespace_id: namespace_id.clone(),
        inode_id,
        transfer_id: transfer_id.to_owned(),
        direction: TransferDirection::Download,
        object_key: object_key.to_owned(),
        block_index,
        block_count,
        state: TransferState::Staging,
        updated_at_ms,
    }
}

pub(super) fn execute_materialize_remote_dir(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    target_path: Option<&Path>,
    applied_at_ms: u64,
) -> Result<ExecutedMaterializeRemoteDir, ExecuteMaterializeRemoteDirError> {
    let result = (|| {
        ensure_materialize_remote_dir_ready(db, namespace_id, inode_id)?;
        let target_path =
            target_path.ok_or_else(|| ExecuteMaterializeRemoteDirError::SourcePathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;

        create_directory_durably(target_path)?;

        let applied = db.apply_materialize_remote_dir(namespace_id, inode_id, applied_at_ms)?;
        Ok(ExecutedMaterializeRemoteDir { applied })
    })();

    if let Err(error) = &result {
        record_materialize_remote_dir_issue(db, namespace_id, inode_id, error, applied_at_ms);
    }

    result
}

fn ensure_apply_remote_rename_ready(
    db: &SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<
    (
        RemoteFileStateRow,
        crate::state_db::LocalFileStateRow,
        crate::state_db::SyncAnchorRow,
    ),
    ExecuteApplyRemoteRenameError,
> {
    let planned_row = db
        .load_planned_action(namespace_id, inode_id)?
        .ok_or_else(|| ExecutorError::PlannedActionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        })?;
    let planned = PlannedActionRecord::try_from(planned_row)?;
    if planned.decision != PlannerDecision::ApplyRemoteRename {
        return Err(
            ExecuteApplyRemoteRenameError::ApplyRemoteRenameDecisionMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                decision: planned.decision,
            },
        );
    }

    db.load_bound_apply_remote_rename_views(namespace_id, inode_id)
        .map_err(ExecuteApplyRemoteRenameError::from)
}

fn ensure_apply_remote_delete_ready(
    db: &SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<
    (
        RemoteFileStateRow,
        crate::state_db::LocalFileStateRow,
        crate::state_db::SyncAnchorRow,
    ),
    ExecuteApplyRemoteDeleteError,
> {
    let planned_row = db
        .load_planned_action(namespace_id, inode_id)?
        .ok_or_else(|| ExecutorError::PlannedActionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        })?;
    let planned = PlannedActionRecord::try_from(planned_row)?;
    if planned.decision != PlannerDecision::ApplyRemoteDelete {
        return Err(
            ExecuteApplyRemoteDeleteError::ApplyRemoteDeleteDecisionMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                decision: planned.decision,
            },
        );
    }

    db.load_bound_apply_remote_delete_views(namespace_id, inode_id)
        .map_err(ExecuteApplyRemoteDeleteError::from)
}

fn ensure_apply_remote_subtree_delete_ready(
    db: &SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<BoundApplyRemoteSubtreeDeleteViews, ExecuteApplyRemoteSubtreeDeleteError> {
    let planned_row = db
        .load_planned_action(namespace_id, inode_id)?
        .ok_or_else(|| ExecutorError::PlannedActionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        })?;
    let planned = PlannedActionRecord::try_from(planned_row)?;
    if planned.decision != PlannerDecision::ApplyRemoteSubtreeDelete {
        return Err(
            ExecuteApplyRemoteSubtreeDeleteError::ApplyRemoteSubtreeDeleteDecisionMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                decision: planned.decision,
            },
        );
    }

    db.load_bound_apply_remote_subtree_delete_views(namespace_id, inode_id)
        .map_err(ExecuteApplyRemoteSubtreeDeleteError::from)
}

fn ensure_apply_remote_subtree_rename_ready(
    db: &SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<BoundApplyRemoteSubtreeRenameViews, ExecuteApplyRemoteSubtreeRenameError> {
    let planned_row = db
        .load_planned_action(namespace_id, inode_id)?
        .ok_or_else(|| ExecutorError::PlannedActionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        })?;
    let planned = PlannedActionRecord::try_from(planned_row)?;
    if planned.decision != PlannerDecision::ApplyRemoteSubtreeRename {
        return Err(
            ExecuteApplyRemoteSubtreeRenameError::ApplyRemoteSubtreeRenameDecisionMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                decision: planned.decision,
            },
        );
    }

    db.load_bound_apply_remote_subtree_rename_views(namespace_id, inode_id)
        .map_err(ExecuteApplyRemoteSubtreeRenameError::from)
}

fn ensure_download_remote_edit_ready(
    db: &SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<RemoteFileStateRow, ExecuteDownloadRemoteEditError> {
    let planned_row = db
        .load_planned_action(namespace_id, inode_id)?
        .ok_or_else(|| ExecutorError::PlannedActionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        })?;
    let planned = PlannedActionRecord::try_from(planned_row)?;
    if planned.decision != PlannerDecision::DownloadRemoteEdit {
        return Err(
            ExecuteDownloadRemoteEditError::DownloadRemoteEditDecisionMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                decision: planned.decision,
            },
        );
    }

    let views = db.load_file_sync_views(namespace_id, inode_id)?;
    match (views.remote, views.local, views.sync_anchor) {
        (Some(_), Some(_), Some(_)) => {
            let (remote, _local, _anchor) =
                db.load_bound_download_remote_edit_views(namespace_id, inode_id)?;
            Ok(remote)
        }
        (Some(remote), Some(local), None)
            if !local.exists_on_disk
                && !local.dirty
                && !remote.is_deleted
                && local.inode_kind == remote.inode_kind
                && local.parent_inode_id == remote.parent_inode_id
                && local.display_name == remote.display_name =>
        {
            if remote.inode_kind != loon_types::InodeKind::File
                || local.inode_kind != loon_types::InodeKind::File
            {
                return Err(StateDbError::DownloadRemoteEditRequiresFile {
                    namespace_id: namespace_id.as_str().to_owned(),
                    inode_id: inode_id.0,
                    inode_kind: format!("{:?}", local.inode_kind).to_lowercase(),
                }
                .into());
            }
            Ok(remote)
        }
        _ => Err(StateDbError::DownloadRemoteEditStateMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        }
        .into()),
    }
}

fn ensure_materialize_remote_dir_ready(
    db: &SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<(), ExecuteMaterializeRemoteDirError> {
    let planned_row = db
        .load_planned_action(namespace_id, inode_id)?
        .ok_or_else(|| ExecutorError::PlannedActionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        })?;
    let planned = PlannedActionRecord::try_from(planned_row)?;
    if planned.decision != PlannerDecision::MaterializeRemoteDir {
        return Err(
            ExecuteMaterializeRemoteDirError::MaterializeRemoteDirDecisionMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                decision: planned.decision,
            },
        );
    }

    let views = db.load_file_sync_views(namespace_id, inode_id)?;
    match (views.remote, views.local, views.sync_anchor) {
        (Some(remote), Some(local), None)
            if remote.inode_kind == InodeKind::Dir
                && local.inode_kind == InodeKind::Dir
                && !remote.is_deleted
                && !local.exists_on_disk
                && !local.dirty
                && local.parent_inode_id == remote.parent_inode_id
                && local.display_name == remote.display_name =>
        {
            Ok(())
        }
        (Some(remote), Some(local), None) => {
            Err(StateDbError::MaterializeRemoteDirRequiresDirectory {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                inode_kind: if remote.inode_kind == InodeKind::Dir {
                    format!("{:?}", local.inode_kind).to_lowercase()
                } else {
                    format!("{:?}", remote.inode_kind).to_lowercase()
                },
            }
            .into())
        }
        _ => Err(StateDbError::MaterializeRemoteDirStateMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        }
        .into()),
    }
}

fn ensure_upload_local_edit_ready<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    source_path: Option<&Path>,
    uploaded_at_ms: u64,
) -> Result<PreparedUploadLocalEdit, ExecuteUploadLocalEditError> {
    let planned_row = db
        .load_planned_action(namespace_id, inode_id)?
        .ok_or_else(|| ExecutorError::PlannedActionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        })?;
    let planned = PlannedActionRecord::try_from(planned_row)?;
    if planned.decision != PlannerDecision::UploadLocalEdit {
        return Err(
            ExecuteUploadLocalEditError::UploadLocalEditDecisionMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                decision: planned.decision,
            },
        );
    }

    let (_remote, local, _anchor) =
        db.load_bound_upload_local_edit_views(namespace_id, inode_id)?;
    if let Some(existing_upload) = db.load_inode_upload(namespace_id, inode_id)? {
        if existing_upload.namespace_id == *namespace_id
            && local.content_digest.as_deref() == Some(existing_upload.file_digest_sha256.as_str())
        {
            db.delete_transfer_ledger_for_inode(namespace_id, inode_id, TransferDirection::Upload)?;
            return Ok(PreparedUploadLocalEdit::Ready {
                upload: existing_upload,
                upload_reused: true,
            });
        }
    }

    let source_path =
        source_path.ok_or_else(|| ExecuteUploadLocalEditError::SourcePathMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        })?;
    let plan = plan_upload_from_path(namespace_id, source_path)?;
    let block_count = u64::try_from(plan.blocks.len()).expect("block count should fit in u64");
    let transfer_id = upload_transfer_id(namespace_id, inode_id, &plan.content_manifest_digest);
    let existing_transfer =
        db.load_transfer_ledger_for_inode(namespace_id, inode_id, TransferDirection::Upload)?;
    let (requested_block_index, reset_reason) = reconcile_upload_transfer_resume(
        existing_transfer.as_ref(),
        &transfer_id,
        &plan.manifest_object_key,
        block_count,
    );
    if let Some(reason) = reset_reason {
        record_upload_local_edit_transfer_reset(db, namespace_id, inode_id, reason, uploaded_at_ms);
    }
    let resume_block_index = requested_block_index.min(block_count);
    let current_transfer = upload_transfer_row(
        namespace_id,
        inode_id,
        &transfer_id,
        &plan.manifest_object_key,
        resume_block_index,
        block_count,
        uploaded_at_ms,
    );
    db.upsert_transfer_ledger(&current_transfer)?;

    if usize::try_from(resume_block_index).expect("resume block index should fit")
        < plan.blocks.len()
    {
        upload_planned_block_from_path(store, source_path, &plan, resume_block_index)?;
        let next_block_index = resume_block_index.saturating_add(1);
        let next_transfer = upload_transfer_row(
            namespace_id,
            inode_id,
            &transfer_id,
            &plan.manifest_object_key,
            next_block_index,
            block_count,
            uploaded_at_ms,
        );
        db.upsert_transfer_ledger(&next_transfer)?;
        if next_block_index < block_count {
            return Ok(PreparedUploadLocalEdit::Progressed {
                transfer: next_transfer,
            });
        }
    }

    let uploaded = finalize_planned_upload(store, &plan)?;
    let recorded = db
        .record_inode_upload(namespace_id, inode_id, &uploaded, uploaded_at_ms)
        .map_err(ExecuteUploadLocalEditError::from)?;
    db.delete_transfer_ledger_for_inode(namespace_id, inode_id, TransferDirection::Upload)?;
    Ok(PreparedUploadLocalEdit::Ready {
        upload: recorded,
        upload_reused: false,
    })
}

fn upload_transfer_id(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    content_manifest_digest: &str,
) -> String {
    format!(
        "upload:{}:{}:{}",
        namespace_id.as_str(),
        inode_id.0,
        content_manifest_digest
    )
}

fn upload_transfer_row(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    transfer_id: &str,
    object_key: &str,
    block_index: u64,
    block_count: u64,
    updated_at_ms: u64,
) -> TransferLedgerRow {
    TransferLedgerRow {
        namespace_id: namespace_id.clone(),
        inode_id,
        transfer_id: transfer_id.to_owned(),
        direction: TransferDirection::Upload,
        object_key: object_key.to_owned(),
        block_index,
        block_count,
        state: TransferState::Uploading,
        updated_at_ms,
    }
}

fn reconcile_download_transfer_resume(
    existing: Option<&TransferLedgerRow>,
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
        Some(existing) if existing.state != TransferState::Staging => {
            (0, Some("transfer_id_mismatch"))
        }
        Some(existing) => (existing.block_index.min(expected_block_count), None),
        None => (0, None),
    }
}

fn reconcile_upload_transfer_resume(
    existing: Option<&TransferLedgerRow>,
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

fn clear_upload_local_edit_failure_issue(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) {
    let _ =
        db.clear_conflict_or_error_kind(namespace_id, inode_id, "upload_local_edit_upload_failed");
}

fn clear_upload_local_edit_issues(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) {
    clear_upload_local_edit_failure_issue(db, namespace_id, inode_id);
    let _ =
        db.clear_conflict_or_error_kind(namespace_id, inode_id, "upload_local_edit_transfer_reset");
}

fn record_upload_local_edit_transfer_reset(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    reason: &'static str,
    created_at_ms: u64,
) {
    let _ = db.record_conflict_or_error(
        namespace_id,
        inode_id,
        "upload_local_edit_transfer_reset",
        "upload_local_edit discarded stale transfer state and restarted from block 0",
        &json!({
            "reason": reason,
        }),
        created_at_ms,
    );
}

fn record_upload_local_edit_issue(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    error: &ExecuteUploadLocalEditError,
    created_at_ms: u64,
) {
    let issue = match error {
        ExecuteUploadLocalEditError::SourcePathMissing { .. } => Some((
            "upload_local_edit_upload_failed",
            "upload_local_edit could not prepare durable local content for upload",
            json!({
                "failure": "source_path_missing",
            }),
        )),
        ExecuteUploadLocalEditError::Upload(upload_error) => Some((
            "upload_local_edit_upload_failed",
            "upload_local_edit could not prepare durable local content for upload",
            upload_error_detail_json(upload_error),
        )),
        _ => None,
    };

    if let Some((kind, summary, detail_json)) = issue {
        let _ = db.record_conflict_or_error(
            namespace_id,
            inode_id,
            kind,
            summary,
            &detail_json,
            created_at_ms,
        );
    }
}

fn record_download_remote_edit_transfer_reset(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    reason: &'static str,
    created_at_ms: u64,
) {
    let _ = db.record_conflict_or_error(
        namespace_id,
        inode_id,
        "download_remote_edit_transfer_reset",
        "download_remote_edit discarded stale staged transfer state and restarted from block 0",
        &json!({
            "reason": reason,
        }),
        created_at_ms,
    );
}

pub(super) fn upload_error_detail_json(error: &crate::upload::UploadError) -> serde_json::Value {
    match error {
        crate::upload::UploadError::LocalFileRead { path, message } => json!({
            "failure": "local_file_read",
            "path": path,
            "message": message,
        }),
        crate::upload::UploadError::ContentManifestCodec(source) => json!({
            "failure": "content_manifest_codec",
            "message": source.to_string(),
        }),
        crate::upload::UploadError::StoreWrite { object_key, source } => json!({
            "failure": "store_write",
            "object_key": object_key,
            "message": source.to_string(),
        }),
        crate::upload::UploadError::StoreRead { object_key, source } => json!({
            "failure": "store_read",
            "object_key": object_key,
            "message": source.to_string(),
        }),
        crate::upload::UploadError::ExistingObjectMissing { object_key } => json!({
            "failure": "existing_object_missing",
            "object_key": object_key,
        }),
        crate::upload::UploadError::ExistingObjectMismatch { object_key } => json!({
            "failure": "existing_object_mismatch",
            "object_key": object_key,
        }),
        crate::upload::UploadError::LocalFileChangedDuringUpload {
            path,
            block_index,
            expected_digest,
            actual_digest,
        } => json!({
            "failure": "local_file_changed_during_upload",
            "path": path,
            "block_index": block_index,
            "expected_digest": expected_digest,
            "actual_digest": actual_digest,
        }),
        crate::upload::UploadError::LocalFileTruncatedDuringUpload {
            path,
            block_index,
            expected_size,
            actual_size,
        } => json!({
            "failure": "local_file_truncated_during_upload",
            "path": path,
            "block_index": block_index,
            "expected_size": expected_size,
            "actual_size": actual_size,
        }),
    }
}

fn record_download_remote_edit_issue(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    error: &ExecuteDownloadRemoteEditError,
    created_at_ms: u64,
) {
    let issue = match error {
        ExecuteDownloadRemoteEditError::RemoteDigestMismatch {
            remote_content_digest,
            downloaded_file_digest,
            ..
        } => Some((
            "download_remote_edit_remote_digest_mismatch",
            "download_remote_edit downloaded bytes did not match the authoritative remote digest",
            json!({
                "remote_content_digest": remote_content_digest,
                "downloaded_file_digest": downloaded_file_digest,
            }),
        )),
        ExecuteDownloadRemoteEditError::LocalApplyFailed {
            operation,
            path,
            source,
        } => Some((
            "download_remote_edit_local_apply_failed",
            "download_remote_edit failed during local apply",
            json!({
                "operation": operation,
                "path": path,
                "source": source.to_string(),
            }),
        )),
        _ => None,
    };

    if let Some((kind, summary, detail_json)) = issue {
        let _ = db.record_conflict_or_error(
            namespace_id,
            inode_id,
            kind,
            summary,
            &detail_json,
            created_at_ms,
        );
    }
}

fn record_apply_remote_rename_issue(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    error: &ExecuteApplyRemoteRenameError,
    created_at_ms: u64,
) {
    let issue = match error {
        ExecuteApplyRemoteRenameError::CurrentPathMissing { .. } => Some((
            "apply_remote_rename_local_apply_failed",
            "apply_remote_rename could not resolve the current local path",
            json!({
                "failure": "current_path_missing",
            }),
        )),
        ExecuteApplyRemoteRenameError::TargetPathMissing { .. } => Some((
            "apply_remote_rename_local_apply_failed",
            "apply_remote_rename could not resolve the target local path",
            json!({
                "failure": "target_path_missing",
            }),
        )),
        ExecuteApplyRemoteRenameError::DestinationOccupied { .. } => Some((
            "apply_remote_rename_local_apply_failed",
            "apply_remote_rename found the destination slot already occupied",
            json!({
                "failure": "destination_occupied",
            }),
        )),
        ExecuteApplyRemoteRenameError::LocalApplyFailed {
            operation,
            path,
            source,
        } => Some((
            "apply_remote_rename_local_apply_failed",
            "apply_remote_rename failed during local apply",
            json!({
                "failure": "rename_io",
                "operation": operation,
                "path": path,
                "source": source.to_string(),
            }),
        )),
        _ => None,
    };

    if let Some((kind, summary, detail_json)) = issue {
        let _ = db.record_conflict_or_error(
            namespace_id,
            inode_id,
            kind,
            summary,
            &detail_json,
            created_at_ms,
        );
    }
}

fn record_apply_remote_delete_issue(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    error: &ExecuteApplyRemoteDeleteError,
    created_at_ms: u64,
) {
    let issue = match error {
        ExecuteApplyRemoteDeleteError::CurrentPathMissing { .. } => Some((
            "apply_remote_delete_local_apply_failed",
            "apply_remote_delete could not resolve the current local path",
            json!({
                "failure": "current_path_missing",
            }),
        )),
        ExecuteApplyRemoteDeleteError::LocalApplyFailed {
            operation,
            path,
            source,
        } => Some((
            "apply_remote_delete_local_apply_failed",
            "apply_remote_delete failed during local apply",
            json!({
                "failure": "unlink_io",
                "operation": operation,
                "path": path,
                "source": source.to_string(),
            }),
        )),
        _ => None,
    };

    if let Some((kind, summary, detail_json)) = issue {
        let _ = db.record_conflict_or_error(
            namespace_id,
            inode_id,
            kind,
            summary,
            &detail_json,
            created_at_ms,
        );
    }
}

fn record_apply_remote_subtree_delete_issue(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    error: &ExecuteApplyRemoteSubtreeDeleteError,
    created_at_ms: u64,
) {
    let issue = match error {
        ExecuteApplyRemoteSubtreeDeleteError::CurrentPathMissing { .. } => Some((
            "apply_remote_subtree_delete_local_apply_failed",
            "apply_remote_subtree_delete could not resolve the current local path",
            json!({
                "failure": "current_path_missing",
            }),
        )),
        ExecuteApplyRemoteSubtreeDeleteError::LocalApplyFailed {
            operation,
            path,
            source,
        } => Some((
            "apply_remote_subtree_delete_local_apply_failed",
            "apply_remote_subtree_delete failed during local apply",
            json!({
                "failure": "recursive_remove_io",
                "operation": operation,
                "path": path,
                "source": source.to_string(),
            }),
        )),
        _ => None,
    };

    if let Some((kind, summary, detail_json)) = issue {
        let _ = db.record_conflict_or_error(
            namespace_id,
            inode_id,
            kind,
            summary,
            &detail_json,
            created_at_ms,
        );
    }
}

fn record_apply_remote_subtree_rename_issue(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    error: &ExecuteApplyRemoteSubtreeRenameError,
    created_at_ms: u64,
) {
    let issue = match error {
        ExecuteApplyRemoteSubtreeRenameError::CurrentPathMissing { .. } => Some((
            "apply_remote_subtree_rename_local_apply_failed",
            "apply_remote_subtree_rename could not resolve the current local path",
            json!({
                "failure": "current_path_missing",
            }),
        )),
        ExecuteApplyRemoteSubtreeRenameError::TargetPathMissing { .. } => Some((
            "apply_remote_subtree_rename_local_apply_failed",
            "apply_remote_subtree_rename could not resolve the target local path",
            json!({
                "failure": "target_path_missing",
            }),
        )),
        ExecuteApplyRemoteSubtreeRenameError::DestinationOccupied { .. } => Some((
            "apply_remote_subtree_rename_local_apply_failed",
            "apply_remote_subtree_rename found the destination slot already occupied",
            json!({
                "failure": "destination_occupied",
            }),
        )),
        ExecuteApplyRemoteSubtreeRenameError::LocalApplyFailed {
            operation,
            path,
            source,
        } => Some((
            "apply_remote_subtree_rename_local_apply_failed",
            "apply_remote_subtree_rename failed during local apply",
            json!({
                "failure": "rename_io",
                "operation": operation,
                "path": path,
                "source": source.to_string(),
            }),
        )),
        _ => None,
    };

    if let Some((kind, summary, detail_json)) = issue {
        let _ = db.record_conflict_or_error(
            namespace_id,
            inode_id,
            kind,
            summary,
            &detail_json,
            created_at_ms,
        );
    }
}

fn record_materialize_remote_dir_issue(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    error: &ExecuteMaterializeRemoteDirError,
    created_at_ms: u64,
) {
    let issue = match error {
        ExecuteMaterializeRemoteDirError::LocalApplyFailed {
            operation,
            path,
            source,
        } => Some((
            "materialize_remote_dir_local_apply_failed",
            "materialize_remote_dir failed during local apply",
            json!({
                "operation": operation,
                "path": path,
                "source": source.to_string(),
            }),
        )),
        _ => None,
    };

    if let Some((kind, summary, detail_json)) = issue {
        let _ = db.record_conflict_or_error(
            namespace_id,
            inode_id,
            kind,
            summary,
            &detail_json,
            created_at_ms,
        );
    }
}
