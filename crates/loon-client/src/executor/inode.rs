use super::dispatch::dispatch_inode_mutation_from_state;
use super::*;
use crate::download::download_file_to_bytes;
use crate::local_apply::{apply_bytes_atomically, create_directory_durably};
use crate::planner::{PlannedActionRecord, PlannerDecision};
use crate::state_db::{InodeUploadRow, RemoteFileStateRow, SqliteStateDb};
use crate::upload::upload_small_file_from_path;
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

    let dispatched =
        dispatch_inode_mutation_from_state(db, namespace_id, inode_id, created_at_ms, dispatch)?;

    Ok(ExecutedUploadLocalEdit {
        ensured_upload,
        upload_reused,
        dispatched,
    })
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
        let downloaded = download_file_to_bytes(store, namespace_id, manifest_digest)?;
        let remote_content_digest = remote
            .content_digest
            .as_deref()
            .expect("download_remote_edit should require remote content digest");
        if downloaded.file_digest_sha256 != remote_content_digest {
            return Err(ExecuteDownloadRemoteEditError::RemoteDigestMismatch {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                remote_content_digest: remote_content_digest.to_owned(),
                downloaded_file_digest: downloaded.file_digest_sha256.clone(),
            });
        }

        apply_bytes_atomically(target_path, &downloaded.bytes)?;

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
            return Ok((existing_upload, true));
        }
    }

    let source_path =
        source_path.ok_or_else(|| ExecuteUploadLocalEditError::SourcePathMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        })?;
    let uploaded = upload_small_file_from_path(store, namespace_id, source_path)?;
    let recorded = db
        .record_inode_upload(namespace_id, inode_id, &uploaded, uploaded_at_ms)
        .map_err(ExecuteUploadLocalEditError::from)?;
    Ok((recorded, false))
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
