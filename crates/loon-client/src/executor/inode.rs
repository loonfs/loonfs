use super::dispatch::dispatch_inode_mutation_from_state;
use super::*;
use crate::download::{
    expected_staged_file_size, load_content_manifest, load_validated_content_block,
    verify_downloaded_file_path,
};
use crate::local_apply::{
    append_stage_bytes, create_directory_durably, finalize_stage_file, reset_stage_file,
    stage_file_size, staging_path_for_target,
};
use crate::planner::{PlannedActionRecord, PlannerDecision};
use crate::state_db::{
    InodeUploadRow, RemoteFileStateRow, SqliteStateDb, TransferDirection, TransferLedgerRow,
    TransferState,
};
use crate::upload::{
    finalize_planned_upload, plan_upload_from_path, upload_planned_block_from_path,
};
use loon_objectstore::ObjectStore;
use loon_types::{ClientMutationRequest, ClientMutationResponse, InodeId, InodeKind, NamespaceId};
use serde_json::json;
use std::path::Path;

pub fn execute_upload_local_edit_from_path<S: ObjectStore, F>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    source_path: &Path,
    uploaded_at_ms: u64,
    created_at_ms: u64,
    dispatch: F,
) -> Result<ExecutedUploadLocalEdit, ExecuteUploadLocalEditError>
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
) -> Result<ExecutedDownloadRemoteEdit, ExecuteDownloadRemoteEditError> {
    execute_download_remote_edit(
        db,
        store,
        namespace_id,
        inode_id,
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

pub(super) fn execute_upload_local_edit<S: ObjectStore, F>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    source_path: Option<&Path>,
    uploaded_at_ms: u64,
    created_at_ms: u64,
    dispatch: F,
) -> Result<ExecutedUploadLocalEdit, ExecuteUploadLocalEditError>
where
    F: FnOnce(&ClientMutationRequest) -> Result<ClientMutationResponse, String>,
{
    let result = (|| {
        let had_pending_request = db
            .load_pending_inode_mutation_for_inode(namespace_id, inode_id)?
            .is_some();

        let (ensured_upload, upload_reused) = if had_pending_request {
            (db.load_inode_upload(namespace_id, inode_id)?, true)
        } else {
            let (upload_row, reused_existing) = ensure_upload_local_edit_ready(
                db,
                store,
                namespace_id,
                inode_id,
                source_path,
                uploaded_at_ms,
            )?;
            (Some(upload_row), reused_existing)
        };

        let dispatched = dispatch_inode_mutation_from_state(
            db,
            namespace_id,
            inode_id,
            created_at_ms,
            dispatch,
        )?;

        Ok(ExecutedUploadLocalEdit {
            ensured_upload,
            upload_reused,
            dispatched,
        })
    })();

    match &result {
        Ok(_) => clear_upload_local_edit_issue(db, namespace_id, inode_id),
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
) -> Result<ExecutedDownloadRemoteEdit, ExecuteDownloadRemoteEditError> {
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
        let requested_block_index = match db.load_transfer_ledger_for_inode(
            namespace_id,
            inode_id,
            TransferDirection::Download,
        )? {
            Some(existing)
                if existing.transfer_id == transfer_id
                    && existing.object_key == loaded_manifest.object_key
                    && existing.block_count == block_count
                    && existing.state == TransferState::Staging =>
            {
                existing.block_index
            }
            _ => 0,
        };
        let staged_size_bytes = stage_file_size(target_path)?.unwrap_or(0);
        let resume_block_index = if staged_size_bytes
            == expected_staged_file_size(&loaded_manifest.manifest_envelope, requested_block_index)
        {
            requested_block_index
        } else {
            0
        };

        if resume_block_index == 0 {
            reset_stage_file(target_path)?;
        }

        db.upsert_transfer_ledger(&download_transfer_row(
            namespace_id,
            inode_id,
            &transfer_id,
            &loaded_manifest.object_key,
            resume_block_index,
            block_count,
            applied_at_ms,
        ))?;

        for (block_offset, block) in loaded_manifest
            .manifest_envelope
            .payload
            .blocks
            .iter()
            .enumerate()
            .skip(usize::try_from(resume_block_index).expect("resume block index should fit"))
        {
            let block_bytes = load_validated_content_block(store, namespace_id, block)?;
            append_stage_bytes(target_path, &block_bytes)?;
            let next_block_index =
                u64::try_from(block_offset + 1).expect("block offset should fit in u64");
            db.upsert_transfer_ledger(&download_transfer_row(
                namespace_id,
                inode_id,
                &transfer_id,
                &loaded_manifest.object_key,
                next_block_index,
                block_count,
                applied_at_ms,
            ))?;
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
        Ok(ExecutedDownloadRemoteEdit {
            downloaded_content_manifest_digest: downloaded.content_manifest_digest,
            downloaded_file_digest_sha256: downloaded.file_digest_sha256,
            applied,
        })
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
) -> Result<(InodeUploadRow, bool), ExecuteUploadLocalEditError> {
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
            return Ok((existing_upload, true));
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
    let requested_block_index = match db.load_transfer_ledger_for_inode(
        namespace_id,
        inode_id,
        TransferDirection::Upload,
    )? {
        Some(existing)
            if existing.transfer_id == transfer_id
                && existing.object_key == plan.manifest_object_key
                && existing.block_count == block_count
                && existing.state == TransferState::Uploading =>
        {
            existing.block_index
        }
        _ => 0,
    };
    let resume_block_index = requested_block_index.min(block_count);
    db.upsert_transfer_ledger(&upload_transfer_row(
        namespace_id,
        inode_id,
        &transfer_id,
        &plan.manifest_object_key,
        resume_block_index,
        block_count,
        uploaded_at_ms,
    ))?;
    for block_index in usize::try_from(resume_block_index).expect("resume block index should fit")
        ..plan.blocks.len()
    {
        upload_planned_block_from_path(
            store,
            source_path,
            &plan,
            u64::try_from(block_index).expect("block index should fit in u64"),
        )?;
        let next_block_index =
            u64::try_from(block_index + 1).expect("block index should fit in u64");
        db.upsert_transfer_ledger(&upload_transfer_row(
            namespace_id,
            inode_id,
            &transfer_id,
            &plan.manifest_object_key,
            next_block_index,
            block_count,
            uploaded_at_ms,
        ))?;
    }
    let uploaded = finalize_planned_upload(store, &plan)?;
    let recorded = db
        .record_inode_upload(namespace_id, inode_id, &uploaded, uploaded_at_ms)
        .map_err(ExecuteUploadLocalEditError::from)?;
    Ok((recorded, false))
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

fn clear_upload_local_edit_issue(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) {
    let _ =
        db.clear_conflict_or_error_kind(namespace_id, inode_id, "upload_local_edit_upload_failed");
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
