use super::*;
use crate::conflict::{
    load_conflict_artifact, load_subtree_conflict_artifact, prepare_conflict_artifact,
    prepare_subtree_conflict_artifact, upload_loser_content_from_path,
    write_conflict_artifact_if_absent, write_subtree_conflict_artifact_if_absent,
    ConflictArtifactError,
};
use crate::download::{
    download_file_to_bytes, expected_staged_file_size, load_content_manifest,
    load_validated_content_block, verify_downloaded_file_path,
};
use crate::local_apply::{
    append_stage_bytes_with_hooks, apply_bytes_atomically_with_hooks, clear_delete_marker,
    create_directory_durably_with_hooks, delete_marker_exists, finalize_stage_file_with_hooks,
    remove_path_durably_with_hooks, remove_stage_file_if_present_with_hooks,
    remove_tree_durably_with_hooks, rename_path_durably_with_hooks, reset_stage_file_with_hooks,
    stage_file_size, staging_path_for_target, LocalApplyError,
};
use crate::planner::{PlannedActionRecord, PlannerDecision};
use crate::state_db::{
    BoundApplyRemoteSubtreeDeleteViews, BoundApplyRemoteSubtreeRenameViews,
    BoundResolveSubtreeDeleteConflictViews, BoundResolveSubtreeRenameConflictViews,
    ConflictArtifactEnvelopeRecord, ConflictArtifactRow, InodeUploadRow, LocalOnlyFileStateRow,
    RemoteFileStateRow, SqliteStateDb, SyncAnchorRow, TransferDirection, TransferLedgerRow,
    TransferState,
};
use crate::testing::{ClientExecutionHooks, NoopClientExecutionHooks};
use crate::upload::{
    finalize_planned_upload, plan_upload_from_path, upload_planned_block_from_path,
};
use loon_server::objectstore::keys::conflict_artifact as conflict_artifact_key;
use loon_server::objectstore::ObjectStore;
use loon_types::{
    sha256_digest, ClientMutationRequest, ClientMutationResponse, ConflictArtifactKind,
    ConflictArtifactLoserSummary, ConflictArtifactWinnerSummary, ConflictClass, InodeId, InodeKind,
    NamespaceId, RevisionNo, SubtreeConflictArtifactEntry, SubtreeConflictArtifactRootSummary,
};
use serde_json::json;
use std::fs;
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

enum WinnerApplyError {
    CurrentPathMissing {
        namespace_id: String,
        inode_id: u64,
    },
    TargetPathMissing {
        namespace_id: String,
        inode_id: u64,
    },
    DestinationOccupied {
        namespace_id: String,
        inode_id: u64,
        path: String,
    },
    LocalApply(LocalApplyError),
}

impl From<LocalApplyError> for WinnerApplyError {
    fn from(error: LocalApplyError) -> Self {
        Self::LocalApply(error)
    }
}

impl From<WinnerApplyError> for ExecuteApplyRemoteRenameAndReplaceError {
    fn from(error: WinnerApplyError) -> Self {
        match error {
            WinnerApplyError::CurrentPathMissing {
                namespace_id,
                inode_id,
            } => Self::CurrentPathMissing {
                namespace_id,
                inode_id,
            },
            WinnerApplyError::TargetPathMissing {
                namespace_id,
                inode_id,
            } => Self::TargetPathMissing {
                namespace_id,
                inode_id,
            },
            WinnerApplyError::DestinationOccupied {
                namespace_id,
                inode_id,
                path,
            } => Self::DestinationOccupied {
                namespace_id,
                inode_id,
                path,
            },
            WinnerApplyError::LocalApply(error) => Self::from(error),
        }
    }
}

impl From<WinnerApplyError> for ExecuteResolveFileConflictError {
    fn from(error: WinnerApplyError) -> Self {
        match error {
            WinnerApplyError::CurrentPathMissing {
                namespace_id,
                inode_id,
            } => Self::CurrentPathMissing {
                namespace_id,
                inode_id,
            },
            WinnerApplyError::TargetPathMissing {
                namespace_id,
                inode_id,
            } => Self::TargetPathMissing {
                namespace_id,
                inode_id,
            },
            WinnerApplyError::DestinationOccupied {
                namespace_id,
                inode_id,
                path,
            } => Self::DestinationOccupied {
                namespace_id,
                inode_id,
                path,
            },
            WinnerApplyError::LocalApply(error) => Self::from(error),
        }
    }
}

fn winner_restore_error(error: LocalApplyError) -> ExecuteResolveSubtreeConflictError {
    ExecuteResolveSubtreeConflictError::WinnerRestoreIo {
        operation: error.operation,
        path: error.path,
        source: error.source,
    }
}

fn recursive_remove_error(error: LocalApplyError) -> ExecuteResolveSubtreeConflictError {
    ExecuteResolveSubtreeConflictError::RecursiveRemoveIo {
        operation: error.operation,
        path: error.path,
        source: error.source,
    }
}

fn rename_error(error: LocalApplyError) -> ExecuteResolveSubtreeConflictError {
    ExecuteResolveSubtreeConflictError::RenameIo {
        operation: error.operation,
        path: error.path,
        source: error.source,
    }
}

#[allow(clippy::too_many_arguments)]
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
    execute_apply_remote_rename_with_hooks(
        db,
        namespace_id,
        inode_id,
        current_path,
        target_path,
        applied_at_ms,
        &NoopClientExecutionHooks,
    )
}

pub(crate) fn execute_apply_remote_rename_with_hooks(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    target_path: Option<&Path>,
    applied_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<ExecutedApplyRemoteRename, ExecuteApplyRemoteRenameError> {
    let result = (|| {
        let (remote, _local, _anchor) =
            ensure_apply_remote_rename_ready(db, namespace_id, inode_id)?;
        let current_path =
            current_path.ok_or_else(|| ExecuteApplyRemoteRenameError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
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
        if current_path.exists() && target_path.exists() {
            return Err(ExecuteApplyRemoteRenameError::DestinationOccupied {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                path: target_path.display().to_string(),
            });
        }

        if !current_path.exists() {
            if !target_path.exists() {
                return Err(ExecuteApplyRemoteRenameError::CurrentPathMissing {
                    namespace_id: namespace_id.as_str().to_owned(),
                    inode_id: inode_id.0,
                });
            }
        } else {
            rename_path_durably_with_hooks(current_path, target_path, hooks)?;
        }
        hooks.checkpoint("client.apply_remote_rename.after_local_apply");

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

#[allow(dead_code)]
pub(super) fn execute_apply_remote_rename_and_replace<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    target_path: Option<&Path>,
    applied_at_ms: u64,
) -> Result<ExecutedApplyRemoteRenameAndReplace, ExecuteApplyRemoteRenameAndReplaceError> {
    execute_apply_remote_rename_and_replace_with_hooks(
        db,
        store,
        namespace_id,
        inode_id,
        current_path,
        target_path,
        applied_at_ms,
        &NoopClientExecutionHooks,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_apply_remote_rename_and_replace_with_hooks<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    target_path: Option<&Path>,
    applied_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<ExecutedApplyRemoteRenameAndReplace, ExecuteApplyRemoteRenameAndReplaceError> {
    let result = (|| {
        let (remote, local, anchor) =
            ensure_apply_remote_rename_and_replace_ready(db, namespace_id, inode_id)?;
        let downloaded = download_file_to_bytes(
            store,
            namespace_id,
            remote.content_manifest_digest.as_deref().ok_or(
                StateDbError::DownloadRemoteEditManifestMissing {
                    namespace_id: namespace_id.as_str().to_owned(),
                    inode_id: inode_id.0,
                },
            )?,
        )?;
        let current_path = current_path.ok_or_else(|| {
            ExecuteApplyRemoteRenameAndReplaceError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            }
        })?;
        let target_path = target_path.ok_or_else(|| {
            ExecuteApplyRemoteRenameAndReplaceError::TargetPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            }
        })?;
        apply_remote_winner_at_target(
            current_path,
            target_path,
            &downloaded.bytes,
            &downloaded.file_digest_sha256,
            namespace_id,
            inode_id,
            hooks,
        )?;
        hooks.checkpoint("client.apply_remote_rename_and_replace.after_local_apply");
        let applied = db.apply_remote_rename_and_replace(namespace_id, inode_id, applied_at_ms)?;
        debug_assert_eq!(applied.namespace_id, local.namespace_id);
        debug_assert_eq!(applied.inode_id, anchor.inode_id);
        Ok(ExecutedApplyRemoteRenameAndReplace { applied })
    })();

    if let Err(error) = &result {
        record_apply_remote_rename_and_replace_issue(
            db,
            namespace_id,
            inode_id,
            error,
            applied_at_ms,
        );
    }

    result
}

#[allow(dead_code)]
pub(super) fn execute_resolve_same_inode_conflict<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    applied_at_ms: u64,
) -> Result<ExecutedResolveSameInodeConflict, ExecuteResolveFileConflictError> {
    execute_resolve_same_inode_conflict_with_hooks(
        db,
        store,
        namespace_id,
        inode_id,
        current_path,
        applied_at_ms,
        &NoopClientExecutionHooks,
    )
}

pub(crate) fn execute_resolve_same_inode_conflict_with_hooks<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    applied_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<ExecutedResolveSameInodeConflict, ExecuteResolveFileConflictError> {
    let result = (|| {
        let (remote, local, anchor) = ensure_same_inode_conflict_ready(db, namespace_id, inode_id)?;
        let current_path =
            current_path.ok_or_else(|| ExecuteResolveFileConflictError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
        let artifact = ensure_conflict_artifact_for_bound_file(
            db,
            store,
            namespace_id,
            ConflictClass::SameInodeStaleBaseEdit,
            remote.observed_seq,
            &remote,
            inode_id,
            Some(anchor.revision_no),
            local.content_digest.clone(),
            local.parent_inode_id,
            local.display_name.clone(),
            current_path,
            applied_at_ms,
            hooks,
        )?;
        let downloaded = download_file_to_bytes(
            store,
            namespace_id,
            remote.content_manifest_digest.as_deref().ok_or(
                StateDbError::DownloadRemoteEditManifestMissing {
                    namespace_id: namespace_id.as_str().to_owned(),
                    inode_id: inode_id.0,
                },
            )?,
        )?;
        apply_remote_bytes_in_place(
            current_path,
            &downloaded.bytes,
            &downloaded.file_digest_sha256,
            namespace_id,
            inode_id,
            hooks,
        )?;
        hooks.checkpoint("client.resolve_same_inode_conflict.after_local_apply");
        let applied =
            db.apply_same_inode_conflict_resolution(namespace_id, inode_id, applied_at_ms)?;
        Ok(ExecutedResolveSameInodeConflict {
            applied,
            conflict_id: artifact.conflict_id,
        })
    })();

    if let Err(error) = &result {
        record_resolve_file_conflict_issue(
            db,
            namespace_id,
            inode_id,
            "resolve_same_inode_conflict_local_apply_failed",
            error,
            applied_at_ms,
        );
    }

    result
}

#[allow(dead_code)]
pub(super) fn execute_resolve_delete_vs_edit_conflict<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    applied_at_ms: u64,
) -> Result<ExecutedResolveDeleteVsEditConflict, ExecuteResolveFileConflictError> {
    execute_resolve_delete_vs_edit_conflict_with_hooks(
        db,
        store,
        namespace_id,
        inode_id,
        current_path,
        applied_at_ms,
        &NoopClientExecutionHooks,
    )
}

pub(crate) fn execute_resolve_delete_vs_edit_conflict_with_hooks<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    applied_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<ExecutedResolveDeleteVsEditConflict, ExecuteResolveFileConflictError> {
    let result = (|| {
        let (remote, local, anchor) =
            ensure_delete_vs_edit_conflict_ready(db, namespace_id, inode_id)?;
        let current_path =
            current_path.ok_or_else(|| ExecuteResolveFileConflictError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
        let artifact = ensure_conflict_artifact_for_bound_file(
            db,
            store,
            namespace_id,
            ConflictClass::DeleteVsEdit,
            remote.observed_seq,
            &remote,
            inode_id,
            Some(anchor.revision_no),
            local.content_digest.clone(),
            local.parent_inode_id,
            local.display_name.clone(),
            current_path,
            applied_at_ms,
            hooks,
        )?;
        if current_path.exists() {
            remove_path_durably_with_hooks(current_path, hooks)?;
            hooks.checkpoint("client.resolve_delete_vs_edit_conflict.after_local_apply");
        } else if !delete_marker_exists(current_path)? {
            return Err(ExecuteResolveFileConflictError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            });
        }
        let applied =
            db.apply_delete_vs_edit_conflict_resolution(namespace_id, inode_id, applied_at_ms)?;
        let _ = clear_delete_marker(current_path);
        Ok(ExecutedResolveDeleteVsEditConflict {
            applied,
            conflict_id: artifact.conflict_id,
        })
    })();

    if let Err(error) = &result {
        record_resolve_file_conflict_issue(
            db,
            namespace_id,
            inode_id,
            "resolve_delete_vs_edit_conflict_local_apply_failed",
            error,
            applied_at_ms,
        );
    }

    result
}

#[allow(dead_code)]
pub(super) fn execute_resolve_rename_vs_edit_conflict<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    target_path: Option<&Path>,
    applied_at_ms: u64,
) -> Result<ExecutedResolveRenameVsEditConflict, ExecuteResolveFileConflictError> {
    execute_resolve_rename_vs_edit_conflict_with_hooks(
        db,
        store,
        namespace_id,
        inode_id,
        current_path,
        target_path,
        applied_at_ms,
        &NoopClientExecutionHooks,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_resolve_rename_vs_edit_conflict_with_hooks<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    target_path: Option<&Path>,
    applied_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<ExecutedResolveRenameVsEditConflict, ExecuteResolveFileConflictError> {
    let result = (|| {
        let (remote, local, anchor) =
            ensure_rename_vs_edit_conflict_ready(db, namespace_id, inode_id)?;
        let current_path =
            current_path.ok_or_else(|| ExecuteResolveFileConflictError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
        let target_path =
            target_path.ok_or_else(|| ExecuteResolveFileConflictError::TargetPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
        let artifact = ensure_conflict_artifact_for_bound_file(
            db,
            store,
            namespace_id,
            ConflictClass::RenameVsEdit,
            remote.observed_seq,
            &remote,
            inode_id,
            Some(anchor.revision_no),
            local.content_digest.clone(),
            local.parent_inode_id,
            local.display_name.clone(),
            current_path,
            applied_at_ms,
            hooks,
        )?;
        let downloaded = download_file_to_bytes(
            store,
            namespace_id,
            remote.content_manifest_digest.as_deref().ok_or(
                StateDbError::DownloadRemoteEditManifestMissing {
                    namespace_id: namespace_id.as_str().to_owned(),
                    inode_id: inode_id.0,
                },
            )?,
        )?;
        apply_remote_winner_at_target(
            current_path,
            target_path,
            &downloaded.bytes,
            &downloaded.file_digest_sha256,
            namespace_id,
            inode_id,
            hooks,
        )?;
        hooks.checkpoint("client.resolve_rename_vs_edit_conflict.after_local_apply");
        let applied =
            db.apply_rename_vs_edit_conflict_resolution(namespace_id, inode_id, applied_at_ms)?;
        Ok(ExecutedResolveRenameVsEditConflict {
            applied,
            conflict_id: artifact.conflict_id,
        })
    })();

    if let Err(error) = &result {
        record_resolve_file_conflict_issue(
            db,
            namespace_id,
            inode_id,
            "resolve_rename_vs_edit_conflict_local_apply_failed",
            error,
            applied_at_ms,
        );
    }

    result
}

#[allow(dead_code)]
pub(super) fn execute_resolve_path_binding_collision<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    source_path: Option<&Path>,
    applied_at_ms: u64,
) -> Result<ExecutedResolvePathBindingCollision, ExecuteResolveFileConflictError> {
    execute_resolve_path_binding_collision_with_hooks(
        db,
        store,
        namespace_id,
        inode_id,
        source_path,
        applied_at_ms,
        &NoopClientExecutionHooks,
    )
}

pub(crate) fn execute_resolve_path_binding_collision_with_hooks<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    source_path: Option<&Path>,
    applied_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<ExecutedResolvePathBindingCollision, ExecuteResolveFileConflictError> {
    let result = (|| {
        let (remote, local_only) = ensure_path_binding_collision_ready(db, namespace_id, inode_id)?;
        let target_path = source_path.ok_or_else(|| {
            ExecuteResolveFileConflictError::PathBindingSourcePathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                client_file_id: local_only.client_file_id.as_str().to_owned(),
            }
        })?;
        let artifact = ensure_conflict_artifact_for_local_only(
            db,
            store,
            namespace_id,
            &remote,
            &local_only,
            target_path,
            applied_at_ms,
            hooks,
        )?;
        let downloaded = download_file_to_bytes(
            store,
            namespace_id,
            remote.content_manifest_digest.as_deref().ok_or(
                StateDbError::DownloadRemoteEditManifestMissing {
                    namespace_id: namespace_id.as_str().to_owned(),
                    inode_id: inode_id.0,
                },
            )?,
        )?;
        apply_remote_bytes_in_place(
            target_path,
            &downloaded.bytes,
            &downloaded.file_digest_sha256,
            namespace_id,
            inode_id,
            hooks,
        )?;
        hooks.checkpoint("client.resolve_path_binding_collision.after_local_apply");
        let applied = db.apply_path_binding_collision_resolution(
            namespace_id,
            inode_id,
            &local_only.client_file_id,
            applied_at_ms,
        )?;
        Ok(ExecutedResolvePathBindingCollision {
            applied,
            conflict_id: artifact.conflict_id,
        })
    })();

    if let Err(error) = &result {
        record_resolve_file_conflict_issue(
            db,
            namespace_id,
            inode_id,
            "resolve_path_binding_collision_local_apply_failed",
            error,
            applied_at_ms,
        );
    }

    result
}

#[allow(dead_code)]
pub(super) fn execute_resolve_subtree_delete_conflict<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    applied_at_ms: u64,
) -> Result<ExecutedResolveSubtreeDeleteConflict, ExecuteResolveSubtreeConflictError> {
    execute_resolve_subtree_delete_conflict_with_hooks(
        db,
        store,
        namespace_id,
        inode_id,
        current_path,
        applied_at_ms,
        &NoopClientExecutionHooks,
    )
}

pub(crate) fn execute_resolve_subtree_delete_conflict_with_hooks<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    applied_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<ExecutedResolveSubtreeDeleteConflict, ExecuteResolveSubtreeConflictError> {
    let result = (|| {
        let views = ensure_subtree_delete_conflict_ready(db, namespace_id, inode_id)?;
        let current_path =
            current_path.ok_or_else(|| ExecuteResolveSubtreeConflictError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
        if !current_path.exists()
            && !delete_marker_exists(current_path).map_err(recursive_remove_error)?
        {
            return Err(ExecuteResolveSubtreeConflictError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            });
        }

        let artifact = ensure_subtree_delete_conflict_artifact(
            db,
            store,
            namespace_id,
            &views,
            current_path,
            applied_at_ms,
            hooks,
        )?;

        if current_path.exists() {
            remove_tree_durably_with_hooks(current_path, hooks).map_err(recursive_remove_error)?;
            hooks.checkpoint("client.resolve_subtree_delete_conflict.after_local_apply");
        }
        let applied =
            db.apply_resolved_subtree_delete_conflict(namespace_id, inode_id, applied_at_ms)?;
        let _ = clear_delete_marker(current_path);
        Ok(ExecutedResolveSubtreeDeleteConflict {
            applied,
            conflict_id: artifact.conflict_id,
        })
    })();

    if let Err(error) = &result {
        record_resolve_subtree_conflict_issue(
            db,
            namespace_id,
            inode_id,
            "resolve_subtree_delete_conflict_failed",
            error,
            applied_at_ms,
        );
    }

    result
}

#[allow(dead_code)]
pub(super) fn execute_resolve_subtree_rename_conflict<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    target_path: Option<&Path>,
    applied_at_ms: u64,
) -> Result<ExecutedResolveSubtreeRenameConflict, ExecuteResolveSubtreeConflictError> {
    execute_resolve_subtree_rename_conflict_with_hooks(
        db,
        store,
        namespace_id,
        inode_id,
        current_path,
        target_path,
        applied_at_ms,
        &NoopClientExecutionHooks,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_resolve_subtree_rename_conflict_with_hooks<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    target_path: Option<&Path>,
    applied_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<ExecutedResolveSubtreeRenameConflict, ExecuteResolveSubtreeConflictError> {
    let result = (|| {
        let views = ensure_subtree_rename_conflict_ready(db, namespace_id, inode_id)?;
        let current_path =
            current_path.ok_or_else(|| ExecuteResolveSubtreeConflictError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
        let target_path =
            target_path.ok_or_else(|| ExecuteResolveSubtreeConflictError::TargetPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
        let target_parent_exists = target_path.parent().is_some_and(|parent| parent.exists());
        if !target_parent_exists || target_path == current_path {
            return Err(ExecuteResolveSubtreeConflictError::TargetPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            });
        }
        if target_path.exists() {
            return Err(ExecuteResolveSubtreeConflictError::DestinationOccupied {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                path: target_path.display().to_string(),
            });
        }

        let artifact = ensure_subtree_rename_conflict_artifact(
            db,
            store,
            namespace_id,
            &views,
            current_path,
            applied_at_ms,
            hooks,
        )?;

        if current_path.exists() {
            remove_tree_durably_with_hooks(current_path, hooks).map_err(recursive_remove_error)?;
            create_directory_durably_with_hooks(current_path, hooks)
                .map_err(winner_restore_error)?;
            rebuild_authoritative_subtree_under_root(
                store,
                namespace_id,
                current_path,
                &views.bound_descendants,
                inode_id,
                hooks,
            )?;
            rename_path_durably_with_hooks(current_path, target_path, hooks)
                .map_err(rename_error)?;
            hooks.checkpoint("client.resolve_subtree_rename_conflict.after_local_apply");
        } else if !target_path.exists() {
            return Err(ExecuteResolveSubtreeConflictError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            });
        }

        let applied =
            db.apply_resolved_subtree_rename_conflict(namespace_id, inode_id, applied_at_ms)?;
        Ok(ExecutedResolveSubtreeRenameConflict {
            applied,
            conflict_id: artifact.conflict_id,
        })
    })();

    if let Err(error) = &result {
        record_resolve_subtree_conflict_issue(
            db,
            namespace_id,
            inode_id,
            "resolve_subtree_rename_conflict_failed",
            error,
            applied_at_ms,
        );
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
    execute_apply_remote_delete_with_hooks(
        db,
        namespace_id,
        inode_id,
        current_path,
        applied_at_ms,
        &NoopClientExecutionHooks,
    )
}

pub(crate) fn execute_apply_remote_delete_with_hooks(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    applied_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<ExecutedApplyRemoteDelete, ExecuteApplyRemoteDeleteError> {
    let result = (|| {
        let (remote, _local, _anchor) =
            ensure_apply_remote_delete_ready(db, namespace_id, inode_id)?;
        let current_path =
            current_path.ok_or_else(|| ExecuteApplyRemoteDeleteError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
        if !current_path.exists() && !delete_marker_exists(current_path)? {
            return Err(ExecuteApplyRemoteDeleteError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            });
        }

        if current_path.exists() {
            remove_path_durably_with_hooks(current_path, hooks)?;
            hooks.checkpoint("client.apply_remote_delete.after_local_apply");
        }

        let applied = db.apply_remote_delete(namespace_id, inode_id, applied_at_ms)?;
        let _ = clear_delete_marker(current_path);
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
    execute_apply_remote_subtree_delete_with_hooks(
        db,
        namespace_id,
        inode_id,
        current_path,
        applied_at_ms,
        &NoopClientExecutionHooks,
    )
}

pub(crate) fn execute_apply_remote_subtree_delete_with_hooks(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    applied_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
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
        if !current_path.exists() && !delete_marker_exists(current_path)? {
            return Err(ExecuteApplyRemoteSubtreeDeleteError::CurrentPathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            });
        }

        if current_path.exists() {
            remove_tree_durably_with_hooks(current_path, hooks)?;
            hooks.checkpoint("client.apply_remote_subtree_delete.after_local_apply");
        }

        let applied = db.apply_remote_subtree_delete(namespace_id, inode_id, applied_at_ms)?;
        let _ = clear_delete_marker(current_path);
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
    execute_apply_remote_subtree_rename_with_hooks(
        db,
        namespace_id,
        inode_id,
        current_path,
        target_path,
        applied_at_ms,
        &NoopClientExecutionHooks,
    )
}

pub(crate) fn execute_apply_remote_subtree_rename_with_hooks(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    current_path: Option<&Path>,
    target_path: Option<&Path>,
    applied_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
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

        if !current_path.exists() {
            if !target_path.exists() {
                return Err(ExecuteApplyRemoteSubtreeRenameError::CurrentPathMissing {
                    namespace_id: namespace_id.as_str().to_owned(),
                    inode_id: inode_id.0,
                });
            }
        } else {
            rename_path_durably_with_hooks(current_path, target_path, hooks)?;
        }
        hooks.checkpoint("client.apply_remote_subtree_rename.after_local_apply");

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

#[allow(clippy::too_many_arguments)]
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
    execute_upload_local_edit_with_hooks(
        db,
        store,
        namespace_id,
        inode_id,
        source_path,
        uploaded_at_ms,
        created_at_ms,
        &NoopClientExecutionHooks,
        dispatch,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_upload_local_edit_with_hooks<S: ObjectStore, F>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    source_path: Option<&Path>,
    uploaded_at_ms: u64,
    created_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
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
        hooks.checkpoint("client.upload_local_edit.upload_ready");

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

        let dispatched = super::dispatch::dispatch_inode_mutation_from_state_with_hooks(
            db,
            namespace_id,
            inode_id,
            created_at_ms,
            hooks,
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
    execute_download_remote_edit_with_hooks(
        db,
        store,
        namespace_id,
        inode_id,
        target_path,
        applied_at_ms,
        &NoopClientExecutionHooks,
    )
}

pub(crate) fn execute_download_remote_edit_with_hooks<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    target_path: Option<&Path>,
    applied_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<DownloadRemoteEditExecution, ExecuteDownloadRemoteEditError> {
    let result = (|| {
        let remote = ensure_download_remote_edit_ready(db, namespace_id, inode_id)?;
        let target_path =
            target_path.ok_or_else(|| ExecuteDownloadRemoteEditError::SourcePathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
        if !target_path.parent().is_some_and(|parent| parent.exists()) {
            return Err(ExecuteDownloadRemoteEditError::SourcePathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            });
        }
        let manifest_digest = remote
            .content_manifest_digest
            .as_deref()
            .expect("download_remote_edit should require manifest digest");
        let remote_content_digest = remote
            .content_digest
            .as_deref()
            .expect("download_remote_edit should require remote content digest");
        if target_path.exists() {
            let existing = fs::read(target_path).map_err(|source| LocalApplyError {
                operation: "read_existing_file",
                path: target_path.display().to_string(),
                source,
            })?;
            if sha256_digest(&existing) == remote_content_digest {
                remove_stage_file_if_present_with_hooks(target_path, hooks)?;
                hooks.checkpoint("client.download_remote_edit.after_local_apply");
                let applied =
                    db.apply_download_remote_edit(namespace_id, inode_id, applied_at_ms)?;
                return Ok(DownloadRemoteEditExecution::Completed(
                    ExecutedDownloadRemoteEdit {
                        downloaded_content_manifest_digest: manifest_digest.to_owned(),
                        downloaded_file_digest_sha256: remote_content_digest.to_owned(),
                        applied,
                    },
                ));
            }
        }
        let loaded_manifest = load_content_manifest(store, namespace_id, manifest_digest)?;
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
            reset_stage_file_with_hooks(target_path, hooks)?;
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
            append_stage_bytes_with_hooks(target_path, &block_bytes, hooks)?;
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

        finalize_stage_file_with_hooks(target_path, hooks)?;
        hooks.checkpoint("client.download_remote_edit.after_local_apply");

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
    execute_materialize_remote_dir_with_hooks(
        db,
        namespace_id,
        inode_id,
        target_path,
        applied_at_ms,
        &NoopClientExecutionHooks,
    )
}

pub(crate) fn execute_materialize_remote_dir_with_hooks(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    target_path: Option<&Path>,
    applied_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<ExecutedMaterializeRemoteDir, ExecuteMaterializeRemoteDirError> {
    let result = (|| {
        ensure_materialize_remote_dir_ready(db, namespace_id, inode_id)?;
        let target_path =
            target_path.ok_or_else(|| ExecuteMaterializeRemoteDirError::SourcePathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
        if !target_path.parent().is_some_and(|parent| parent.exists()) {
            return Err(ExecuteMaterializeRemoteDirError::SourcePathMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            });
        }

        if !target_path.exists() || !target_path.is_dir() {
            create_directory_durably_with_hooks(target_path, hooks)?;
        }
        hooks.checkpoint("client.materialize_remote_dir.after_local_apply");

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

fn ensure_apply_remote_rename_and_replace_ready(
    db: &SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<
    (
        RemoteFileStateRow,
        crate::state_db::LocalFileStateRow,
        SyncAnchorRow,
    ),
    ExecuteApplyRemoteRenameAndReplaceError,
> {
    let planned_row = db
        .load_planned_action(namespace_id, inode_id)?
        .ok_or_else(|| ExecutorError::PlannedActionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        })?;
    let planned = PlannedActionRecord::try_from(planned_row)?;
    if planned.decision != PlannerDecision::ApplyRemoteRenameAndReplace {
        return Err(ExecuteApplyRemoteRenameAndReplaceError::DecisionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            decision: planned.decision,
        });
    }

    let views = db.load_file_sync_views(namespace_id, inode_id)?;
    match (views.remote, views.local, views.sync_anchor) {
        (Some(remote), Some(local), Some(anchor))
            if remote.inode_kind == InodeKind::File
                && local.inode_kind == InodeKind::File
                && anchor.inode_kind == InodeKind::File
                && !remote.is_deleted
                && local_matches_anchor(&local, &anchor)
                && !remote_content_matches_anchor(&remote, &anchor)
                && !remote_path_matches_anchor(&remote, &anchor) =>
        {
            Ok((remote, local, anchor))
        }
        _ => Err(StateDbError::ApplyRemoteRenameStateMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        }
        .into()),
    }
}

fn ensure_same_inode_conflict_ready(
    db: &SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<
    (
        RemoteFileStateRow,
        crate::state_db::LocalFileStateRow,
        SyncAnchorRow,
    ),
    ExecuteResolveFileConflictError,
> {
    let planned_row = db
        .load_planned_action(namespace_id, inode_id)?
        .ok_or_else(|| ExecutorError::PlannedActionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        })?;
    let planned = PlannedActionRecord::try_from(planned_row)?;
    if planned.decision != PlannerDecision::ResolveSameInodeConflict {
        return Err(ExecuteResolveFileConflictError::DecisionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            decision: planned.decision,
        });
    }

    let views = db.load_file_sync_views(namespace_id, inode_id)?;
    match (views.remote, views.local, views.sync_anchor) {
        (Some(remote), Some(local), Some(anchor))
            if remote.inode_kind == InodeKind::File
                && local.inode_kind == InodeKind::File
                && anchor.inode_kind == InodeKind::File
                && !remote.is_deleted
                && local_path_matches_anchor(&local, &anchor)
                && !local_matches_anchor(&local, &anchor)
                && !remote_content_matches_anchor(&remote, &anchor)
                && remote_path_matches_anchor(&remote, &anchor) =>
        {
            Ok((remote, local, anchor))
        }
        _ => Err(StateDbError::UploadLocalEditStateMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        }
        .into()),
    }
}

fn ensure_delete_vs_edit_conflict_ready(
    db: &SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<
    (
        RemoteFileStateRow,
        crate::state_db::LocalFileStateRow,
        SyncAnchorRow,
    ),
    ExecuteResolveFileConflictError,
> {
    let planned_row = db
        .load_planned_action(namespace_id, inode_id)?
        .ok_or_else(|| ExecutorError::PlannedActionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        })?;
    let planned = PlannedActionRecord::try_from(planned_row)?;
    if planned.decision != PlannerDecision::ResolveDeleteVsEditConflict {
        return Err(ExecuteResolveFileConflictError::DecisionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            decision: planned.decision,
        });
    }

    let views = db.load_file_sync_views(namespace_id, inode_id)?;
    match (views.remote, views.local, views.sync_anchor) {
        (Some(remote), Some(local), Some(anchor))
            if remote.inode_kind == InodeKind::File
                && local.inode_kind == InodeKind::File
                && anchor.inode_kind == InodeKind::File
                && remote.is_deleted
                && local.exists_on_disk
                && !local_matches_anchor(&local, &anchor) =>
        {
            Ok((remote, local, anchor))
        }
        _ => Err(StateDbError::ApplyRemoteDeleteStateMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        }
        .into()),
    }
}

fn ensure_rename_vs_edit_conflict_ready(
    db: &SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<
    (
        RemoteFileStateRow,
        crate::state_db::LocalFileStateRow,
        SyncAnchorRow,
    ),
    ExecuteResolveFileConflictError,
> {
    let planned_row = db
        .load_planned_action(namespace_id, inode_id)?
        .ok_or_else(|| ExecutorError::PlannedActionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        })?;
    let planned = PlannedActionRecord::try_from(planned_row)?;
    if planned.decision != PlannerDecision::ResolveRenameVsEditConflict {
        return Err(ExecuteResolveFileConflictError::DecisionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            decision: planned.decision,
        });
    }

    let views = db.load_file_sync_views(namespace_id, inode_id)?;
    match (views.remote, views.local, views.sync_anchor) {
        (Some(remote), Some(local), Some(anchor))
            if remote.inode_kind == InodeKind::File
                && local.inode_kind == InodeKind::File
                && anchor.inode_kind == InodeKind::File
                && !remote.is_deleted
                && local.exists_on_disk
                && !local_matches_anchor(&local, &anchor)
                && !remote_content_matches_anchor(&remote, &anchor)
                && !remote_path_matches_anchor(&remote, &anchor) =>
        {
            Ok((remote, local, anchor))
        }
        _ => Err(StateDbError::ApplyRemoteRenameStateMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        }
        .into()),
    }
}

fn ensure_path_binding_collision_ready(
    db: &SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<(RemoteFileStateRow, LocalOnlyFileStateRow), ExecuteResolveFileConflictError> {
    let planned_row = db
        .load_planned_action(namespace_id, inode_id)?
        .ok_or_else(|| ExecutorError::PlannedActionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        })?;
    let planned = PlannedActionRecord::try_from(planned_row)?;
    if planned.decision != PlannerDecision::ResolvePathBindingCollision {
        return Err(ExecuteResolveFileConflictError::DecisionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            decision: planned.decision,
        });
    }

    let views = db.load_file_sync_views(namespace_id, inode_id)?;
    let (Some(remote), local, None) = (views.remote, views.local, views.sync_anchor) else {
        return Err(StateDbError::DownloadRemoteEditStateMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        }
        .into());
    };
    if remote.is_deleted || remote.inode_kind != InodeKind::File {
        return Err(StateDbError::DownloadRemoteEditStateMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        }
        .into());
    }
    if let Some(local) = local.as_ref() {
        if !remote_only_placeholder_matches_remote(local, &remote) {
            return Err(StateDbError::DownloadRemoteEditStateMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            }
            .into());
        }
    }

    let collisions = db
        .load_local_only_candidates_for_namespace(namespace_id)?
        .into_iter()
        .filter(|candidate| {
            candidate.exists_on_disk
                && candidate.inode_kind == InodeKind::File
                && candidate.parent_inode_id == remote.parent_inode_id
                && candidate.display_name == remote.display_name
                && candidate.content_digest != remote.content_digest
        })
        .collect::<Vec<_>>();
    let [collision] = collisions.as_slice() else {
        return Err(StateDbError::DownloadRemoteEditStateMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        }
        .into());
    };

    Ok((remote, collision.clone()))
}

fn ensure_subtree_delete_conflict_ready(
    db: &SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<BoundResolveSubtreeDeleteConflictViews, ExecuteResolveSubtreeConflictError> {
    let planned_row = db
        .load_planned_action(namespace_id, inode_id)?
        .ok_or_else(|| ExecutorError::PlannedActionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        })?;
    let planned = PlannedActionRecord::try_from(planned_row)?;
    if planned.decision != PlannerDecision::ResolveSubtreeDeleteConflict {
        return Err(ExecuteResolveSubtreeConflictError::DecisionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            decision: planned.decision,
        });
    }
    Ok(db.load_bound_resolve_subtree_delete_conflict_views(namespace_id, inode_id)?)
}

fn ensure_subtree_rename_conflict_ready(
    db: &SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<BoundResolveSubtreeRenameConflictViews, ExecuteResolveSubtreeConflictError> {
    let planned_row = db
        .load_planned_action(namespace_id, inode_id)?
        .ok_or_else(|| ExecutorError::PlannedActionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        })?;
    let planned = PlannedActionRecord::try_from(planned_row)?;
    if planned.decision != PlannerDecision::ResolveSubtreeRenameConflict {
        return Err(ExecuteResolveSubtreeConflictError::DecisionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            decision: planned.decision,
        });
    }
    Ok(db.load_bound_resolve_subtree_rename_conflict_views(namespace_id, inode_id)?)
}

#[allow(clippy::too_many_arguments)]
fn ensure_conflict_artifact_for_bound_file<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_class: ConflictClass,
    detected_seq: loon_types::ChangeSeq,
    remote: &RemoteFileStateRow,
    loser_inode_id: InodeId,
    base_revision_no: Option<RevisionNo>,
    loser_content_digest: Option<String>,
    loser_parent_inode_id: Option<InodeId>,
    loser_display_name: String,
    source_path: &Path,
    created_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<ConflictArtifactRow, ExecuteResolveFileConflictError> {
    let winner = ConflictArtifactWinnerSummary {
        inode_id: remote.inode_id,
        parent_inode_id: remote.parent_inode_id,
        display_name: remote.display_name.clone(),
        revision_no: remote.revision_no,
        content_manifest_digest: remote.content_manifest_digest.clone(),
    };
    let loser = ConflictArtifactLoserSummary {
        inode_id: Some(loser_inode_id),
        client_file_id: None,
        base_revision_no,
        content_manifest_digest: String::new(),
        content_digest: loser_content_digest,
        parent_inode_id: loser_parent_inode_id,
        display_name: loser_display_name,
    };
    ensure_conflict_artifact(
        db,
        store,
        namespace_id,
        conflict_class,
        detected_seq,
        winner,
        loser,
        source_path,
        created_at_ms,
        hooks,
    )
}

#[allow(clippy::too_many_arguments)]
fn ensure_conflict_artifact_for_local_only<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    remote: &RemoteFileStateRow,
    local_only: &LocalOnlyFileStateRow,
    source_path: &Path,
    created_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<ConflictArtifactRow, ExecuteResolveFileConflictError> {
    let winner = ConflictArtifactWinnerSummary {
        inode_id: remote.inode_id,
        parent_inode_id: remote.parent_inode_id,
        display_name: remote.display_name.clone(),
        revision_no: remote.revision_no,
        content_manifest_digest: remote.content_manifest_digest.clone(),
    };
    let loser = ConflictArtifactLoserSummary {
        inode_id: None,
        client_file_id: Some(local_only.client_file_id.as_str().to_owned()),
        base_revision_no: None,
        content_manifest_digest: String::new(),
        content_digest: local_only.content_digest.clone(),
        parent_inode_id: local_only.parent_inode_id,
        display_name: local_only.display_name.clone(),
    };
    ensure_conflict_artifact(
        db,
        store,
        namespace_id,
        ConflictClass::PathBindingCollision,
        remote.observed_seq,
        winner,
        loser,
        source_path,
        created_at_ms,
        hooks,
    )
}

#[allow(clippy::too_many_arguments)]
fn ensure_conflict_artifact<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_class: ConflictClass,
    detected_seq: loon_types::ChangeSeq,
    winner: ConflictArtifactWinnerSummary,
    mut loser: ConflictArtifactLoserSummary,
    source_path: &Path,
    created_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<ConflictArtifactRow, ExecuteResolveFileConflictError> {
    let conflict_id = loon_types::deterministic_conflict_id(
        namespace_id,
        conflict_class,
        &winner,
        &loser,
        detected_seq,
    );
    if let Some(existing) = db.load_conflict_artifact(namespace_id, &conflict_id)? {
        return Ok(existing);
    }

    if let Some(existing) = load_conflict_artifact(store, namespace_id, &conflict_id)? {
        let row = ConflictArtifactRow {
            namespace_id: namespace_id.clone(),
            conflict_id: conflict_id.clone(),
            object_key: conflict_artifact_key(namespace_id.as_str(), &conflict_id),
            artifact_kind: ConflictArtifactKind::File,
            conflict_class: existing.conflict_class,
            created_at_ms: existing.created_at_ms,
            envelope: ConflictArtifactEnvelopeRecord::File(existing),
        };
        db.planner_transaction("cache_conflict_artifact", |tx| {
            tx.upsert_conflict_artifact(&row)?;
            Ok(())
        })?;
        return Ok(row);
    }

    let uploaded = upload_loser_content_from_path(store, namespace_id, source_path)?;
    loser.content_manifest_digest = uploaded.content_manifest_digest.clone();
    loser.content_digest = Some(uploaded.file_digest_sha256.clone());
    let artifact = prepare_conflict_artifact(
        namespace_id,
        conflict_class,
        detected_seq,
        winner,
        loser,
        created_at_ms,
    );
    write_conflict_artifact_if_absent(store, &artifact)?;
    hooks.checkpoint("client.conflict_artifact.after_store_write");
    let row = ConflictArtifactRow {
        namespace_id: namespace_id.clone(),
        conflict_id: artifact.envelope.conflict_id.clone(),
        object_key: artifact.object_key.clone(),
        artifact_kind: ConflictArtifactKind::File,
        conflict_class: artifact.envelope.conflict_class,
        created_at_ms: artifact.envelope.created_at_ms,
        envelope: ConflictArtifactEnvelopeRecord::File(artifact.envelope.clone()),
    };
    db.planner_transaction("persist_conflict_artifact", |tx| {
        tx.upsert_conflict_artifact(&row)?;
        Ok(())
    })?;
    hooks.checkpoint("client.conflict_artifact.after_cache");
    Ok(row)
}

fn ensure_subtree_delete_conflict_artifact<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    views: &BoundResolveSubtreeDeleteConflictViews,
    current_root_path: &Path,
    created_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<ConflictArtifactRow, ExecuteResolveSubtreeConflictError> {
    ensure_subtree_conflict_artifact_inner(
        db,
        store,
        namespace_id,
        ConflictClass::SubtreeDeleteVsLocalChanges,
        &views.root_remote,
        &views.root_local,
        &views.root_anchor,
        &views.bound_descendants,
        &views.local_only_descendants,
        current_root_path,
        created_at_ms,
        hooks,
    )
}

fn ensure_subtree_rename_conflict_artifact<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    views: &BoundResolveSubtreeRenameConflictViews,
    current_root_path: &Path,
    created_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<ConflictArtifactRow, ExecuteResolveSubtreeConflictError> {
    ensure_subtree_conflict_artifact_inner(
        db,
        store,
        namespace_id,
        ConflictClass::SubtreeRenameVsLocalChanges,
        &views.root_remote,
        &views.root_local,
        &views.root_anchor,
        &views.bound_descendants,
        &views.local_only_descendants,
        current_root_path,
        created_at_ms,
        hooks,
    )
}

#[allow(clippy::too_many_arguments)]
fn ensure_subtree_conflict_artifact_inner<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_class: ConflictClass,
    root_remote: &RemoteFileStateRow,
    root_local: &crate::state_db::LocalFileStateRow,
    root_anchor: &SyncAnchorRow,
    bound_descendants: &[crate::state_db::ConflictBoundSubtreeEntry],
    local_only_descendants: &[crate::state_db::ConflictLocalOnlySubtreeEntry],
    current_root_path: &Path,
    created_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<ConflictArtifactRow, ExecuteResolveSubtreeConflictError> {
    let winner = ConflictArtifactWinnerSummary {
        inode_id: root_remote.inode_id,
        parent_inode_id: root_remote.parent_inode_id,
        display_name: root_remote.display_name.clone(),
        revision_no: root_remote.revision_no,
        content_manifest_digest: root_remote.content_manifest_digest.clone(),
    };
    let loser_root = SubtreeConflictArtifactRootSummary {
        inode_id: root_local.inode_id,
        parent_inode_id: root_local.parent_inode_id,
        display_name: root_local.display_name.clone(),
        revision_no: Some(root_anchor.revision_no),
    };
    let entries = build_subtree_conflict_entries(
        store,
        namespace_id,
        bound_descendants,
        local_only_descendants,
        current_root_path,
    )?;
    let conflict_id = loon_types::deterministic_subtree_conflict_id(
        namespace_id,
        conflict_class,
        &winner,
        &loser_root,
        &entries,
        root_remote.observed_seq,
    );
    if let Some(existing) = db.load_conflict_artifact(namespace_id, &conflict_id)? {
        return Ok(existing);
    }
    if let Some(existing) = load_subtree_conflict_artifact(store, namespace_id, &conflict_id)? {
        let row = ConflictArtifactRow {
            namespace_id: namespace_id.clone(),
            conflict_id: conflict_id.clone(),
            object_key: conflict_artifact_key(namespace_id.as_str(), &conflict_id),
            artifact_kind: ConflictArtifactKind::Subtree,
            conflict_class: existing.conflict_class,
            created_at_ms: existing.created_at_ms,
            envelope: ConflictArtifactEnvelopeRecord::Subtree(existing),
        };
        db.planner_transaction("cache_subtree_conflict_artifact", |tx| {
            tx.upsert_conflict_artifact(&row)?;
            Ok(())
        })?;
        return Ok(row);
    }

    let artifact = prepare_subtree_conflict_artifact(
        namespace_id,
        conflict_class,
        root_remote.observed_seq,
        winner,
        loser_root,
        entries,
        created_at_ms,
    );
    write_subtree_conflict_artifact_if_absent(store, &artifact)?;
    hooks.checkpoint("client.subtree_conflict_artifact.after_store_write");
    let row = ConflictArtifactRow {
        namespace_id: namespace_id.clone(),
        conflict_id: artifact.envelope.conflict_id.clone(),
        object_key: artifact.object_key.clone(),
        artifact_kind: ConflictArtifactKind::Subtree,
        conflict_class: artifact.envelope.conflict_class,
        created_at_ms: artifact.envelope.created_at_ms,
        envelope: ConflictArtifactEnvelopeRecord::Subtree(artifact.envelope.clone()),
    };
    db.planner_transaction("persist_subtree_conflict_artifact", |tx| {
        tx.upsert_conflict_artifact(&row)?;
        Ok(())
    })?;
    hooks.checkpoint("client.subtree_conflict_artifact.after_cache");
    Ok(row)
}

fn build_subtree_conflict_entries<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    bound_descendants: &[crate::state_db::ConflictBoundSubtreeEntry],
    local_only_descendants: &[crate::state_db::ConflictLocalOnlySubtreeEntry],
    current_root_path: &Path,
) -> Result<Vec<SubtreeConflictArtifactEntry>, ExecuteResolveSubtreeConflictError> {
    let mut entries = Vec::new();

    for entry in bound_descendants {
        if entry.current_relative_path.is_empty() {
            continue;
        }
        let (content_manifest_digest, content_digest) = match entry.local.inode_kind {
            InodeKind::File => {
                if entry.local.dirty {
                    let uploaded = upload_loser_content_from_path(
                        store,
                        namespace_id,
                        &subtree_entry_path(current_root_path, &entry.current_relative_path),
                    )?;
                    (
                        Some(uploaded.content_manifest_digest),
                        Some(uploaded.file_digest_sha256),
                    )
                } else if let Some(anchor) = entry.sync_anchor.as_ref() {
                    (
                        anchor.content_manifest_digest.clone(),
                        anchor
                            .content_digest
                            .clone()
                            .or(entry.local.content_digest.clone()),
                    )
                } else if let Some(remote) = entry.remote.as_ref() {
                    (
                        remote.content_manifest_digest.clone(),
                        remote
                            .content_digest
                            .clone()
                            .or(entry.local.content_digest.clone()),
                    )
                } else {
                    (None, entry.local.content_digest.clone())
                }
            }
            _ => (None, None),
        };
        entries.push(SubtreeConflictArtifactEntry {
            relative_path: entry.current_relative_path.clone(),
            inode_kind: entry.local.inode_kind.clone(),
            inode_id: Some(entry.inode_id),
            client_file_id: None,
            base_revision_no: entry.sync_anchor.as_ref().map(|anchor| anchor.revision_no),
            content_manifest_digest,
            content_digest,
            parent_inode_id: entry.local.parent_inode_id,
            display_name: entry.local.display_name.clone(),
        });
    }

    for entry in local_only_descendants {
        let (content_manifest_digest, content_digest) = match entry.local_only.inode_kind {
            InodeKind::File => {
                if let Some(upload) = entry.upload.as_ref() {
                    if entry.local_only.content_digest.as_deref()
                        == Some(upload.file_digest_sha256.as_str())
                    {
                        (
                            Some(upload.content_manifest_digest.clone()),
                            Some(upload.file_digest_sha256.clone()),
                        )
                    } else {
                        let uploaded = upload_loser_content_from_path(
                            store,
                            namespace_id,
                            &subtree_entry_path(current_root_path, &entry.current_relative_path),
                        )?;
                        (
                            Some(uploaded.content_manifest_digest),
                            Some(uploaded.file_digest_sha256),
                        )
                    }
                } else {
                    let uploaded = upload_loser_content_from_path(
                        store,
                        namespace_id,
                        &subtree_entry_path(current_root_path, &entry.current_relative_path),
                    )?;
                    (
                        Some(uploaded.content_manifest_digest),
                        Some(uploaded.file_digest_sha256),
                    )
                }
            }
            _ => (None, None),
        };
        entries.push(SubtreeConflictArtifactEntry {
            relative_path: entry.current_relative_path.clone(),
            inode_kind: entry.local_only.inode_kind.clone(),
            inode_id: None,
            client_file_id: Some(entry.local_only.client_file_id.as_str().to_owned()),
            base_revision_no: None,
            content_manifest_digest,
            content_digest,
            parent_inode_id: entry.local_only.parent_inode_id,
            display_name: entry.local_only.display_name.clone(),
        });
    }

    entries.sort_by(|left, right| {
        left.relative_path
            .cmp(&right.relative_path)
            .then_with(|| left.client_file_id.cmp(&right.client_file_id))
            .then_with(|| left.inode_id.cmp(&right.inode_id))
    });
    Ok(entries)
}

fn subtree_entry_path(root_path: &Path, relative_path: &str) -> std::path::PathBuf {
    if relative_path.is_empty() {
        root_path.to_path_buf()
    } else {
        root_path.join(relative_path)
    }
}

fn rebuild_authoritative_subtree_under_root<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    current_root_path: &Path,
    bound_descendants: &[crate::state_db::ConflictBoundSubtreeEntry],
    root_inode_id: InodeId,
    hooks: &dyn ClientExecutionHooks,
) -> Result<(), ExecuteResolveSubtreeConflictError> {
    let mut directory_entries = Vec::new();
    let mut file_entries = Vec::new();
    for entry in bound_descendants {
        if entry.inode_id == root_inode_id {
            continue;
        }
        let Some(anchor) = entry.sync_anchor.as_ref() else {
            continue;
        };
        let Some(authoritative_relative_path) = entry.authoritative_relative_path.as_ref() else {
            continue;
        };
        match anchor.inode_kind {
            InodeKind::Dir => directory_entries.push(authoritative_relative_path.clone()),
            InodeKind::File => {
                file_entries.push((entry, anchor, authoritative_relative_path.clone()))
            }
            _ => {}
        }
    }

    directory_entries.sort_by(|left, right| {
        left.matches('/')
            .count()
            .cmp(&right.matches('/').count())
            .then_with(|| left.cmp(right))
    });
    for relative_path in directory_entries {
        create_directory_durably_with_hooks(
            &subtree_entry_path(current_root_path, &relative_path),
            hooks,
        )
        .map_err(winner_restore_error)?;
    }

    file_entries.sort_by(|(left_entry, _, left_path), (right_entry, _, right_path)| {
        left_path
            .cmp(right_path)
            .then_with(|| left_entry.inode_id.0.cmp(&right_entry.inode_id.0))
    });
    for (entry, anchor, relative_path) in file_entries {
        let manifest_digest = anchor
            .content_manifest_digest
            .as_deref()
            .or_else(|| {
                entry
                    .remote
                    .as_ref()
                    .and_then(|remote| remote.content_manifest_digest.as_deref())
            })
            .ok_or_else(|| StateDbError::DownloadRemoteEditManifestMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: entry.inode_id.0,
            })?;
        let downloaded = download_file_to_bytes(store, namespace_id, manifest_digest)?;
        apply_bytes_atomically_with_hooks(
            &subtree_entry_path(current_root_path, &relative_path),
            &downloaded.bytes,
            hooks,
        )
        .map_err(winner_restore_error)?;
    }

    Ok(())
}

fn apply_remote_bytes_in_place(
    target_path: &Path,
    bytes: &[u8],
    expected_digest: &str,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    hooks: &dyn ClientExecutionHooks,
) -> Result<(), WinnerApplyError> {
    if target_path.exists() {
        let existing = fs::read(target_path).map_err(|source| {
            WinnerApplyError::LocalApply(LocalApplyError {
                operation: "read_existing_file",
                path: target_path.display().to_string(),
                source,
            })
        })?;
        if sha256_digest(&existing) == expected_digest {
            return Ok(());
        }
    }

    let parent_exists = target_path.parent().is_some_and(|parent| parent.exists());
    if !parent_exists {
        return Err(WinnerApplyError::TargetPathMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        });
    }

    apply_bytes_atomically_with_hooks(target_path, bytes, hooks)?;
    Ok(())
}

fn apply_remote_winner_at_target(
    current_path: &Path,
    target_path: &Path,
    bytes: &[u8],
    expected_digest: &str,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    hooks: &dyn ClientExecutionHooks,
) -> Result<(), WinnerApplyError> {
    if current_path == target_path {
        return apply_remote_bytes_in_place(
            target_path,
            bytes,
            expected_digest,
            namespace_id,
            inode_id,
            hooks,
        );
    }

    let target_parent_exists = target_path.parent().is_some_and(|parent| parent.exists());
    if !target_parent_exists {
        return Err(WinnerApplyError::TargetPathMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        });
    }

    if target_path.exists() {
        let existing = fs::read(target_path).map_err(|source| {
            WinnerApplyError::LocalApply(LocalApplyError {
                operation: "read_existing_file",
                path: target_path.display().to_string(),
                source,
            })
        })?;
        if sha256_digest(&existing) != expected_digest {
            return Err(WinnerApplyError::DestinationOccupied {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                path: target_path.display().to_string(),
            });
        }
        if current_path.exists() {
            remove_path_durably_with_hooks(current_path, hooks)?;
        }
        return Ok(());
    }

    if !current_path.exists() {
        return Err(WinnerApplyError::CurrentPathMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        });
    }

    apply_remote_bytes_in_place(
        target_path,
        bytes,
        expected_digest,
        namespace_id,
        inode_id,
        hooks,
    )?;
    if current_path.exists() {
        remove_path_durably_with_hooks(current_path, hooks)?;
    }
    Ok(())
}

fn remote_content_matches_anchor(remote: &RemoteFileStateRow, anchor: &SyncAnchorRow) -> bool {
    !remote.is_deleted
        && remote.inode_kind == anchor.inode_kind
        && remote.revision_no == anchor.revision_no
        && remote.content_digest == anchor.content_digest
        && remote.content_manifest_digest == anchor.content_manifest_digest
}

fn remote_path_matches_anchor(remote: &RemoteFileStateRow, anchor: &SyncAnchorRow) -> bool {
    remote.parent_inode_id == anchor.parent_inode_id && remote.display_name == anchor.display_name
}

fn local_matches_anchor(
    local: &crate::state_db::LocalFileStateRow,
    anchor: &SyncAnchorRow,
) -> bool {
    local.exists_on_disk
        && !local.dirty
        && local.inode_kind == anchor.inode_kind
        && local.content_digest == anchor.content_digest
        && local.parent_inode_id == anchor.parent_inode_id
        && local.display_name == anchor.display_name
}

fn local_path_matches_anchor(
    local: &crate::state_db::LocalFileStateRow,
    anchor: &SyncAnchorRow,
) -> bool {
    local.exists_on_disk
        && local.parent_inode_id == anchor.parent_inode_id
        && local.display_name == anchor.display_name
}

fn remote_only_placeholder_matches_remote(
    local: &crate::state_db::LocalFileStateRow,
    remote: &RemoteFileStateRow,
) -> bool {
    !local.exists_on_disk
        && !local.dirty
        && !remote.is_deleted
        && local.inode_kind == remote.inode_kind
        && local.parent_inode_id == remote.parent_inode_id
        && local.display_name == remote.display_name
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

fn record_apply_remote_rename_and_replace_issue(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    error: &ExecuteApplyRemoteRenameAndReplaceError,
    created_at_ms: u64,
) {
    let issue = match error {
        ExecuteApplyRemoteRenameAndReplaceError::CurrentPathMissing { .. } => Some((
            "apply_remote_rename_and_replace_local_apply_failed",
            "apply_remote_rename_and_replace could not resolve the current local path",
            json!({
                "failure": "current_path_missing",
            }),
        )),
        ExecuteApplyRemoteRenameAndReplaceError::TargetPathMissing { .. } => Some((
            "apply_remote_rename_and_replace_local_apply_failed",
            "apply_remote_rename_and_replace could not resolve the target local path",
            json!({
                "failure": "target_path_missing",
            }),
        )),
        ExecuteApplyRemoteRenameAndReplaceError::DestinationOccupied { .. } => Some((
            "apply_remote_rename_and_replace_local_apply_failed",
            "apply_remote_rename_and_replace found the destination slot already occupied",
            json!({
                "failure": "destination_occupied",
            }),
        )),
        ExecuteApplyRemoteRenameAndReplaceError::LocalApplyFailed {
            operation,
            path,
            source,
        } => Some((
            "apply_remote_rename_and_replace_local_apply_failed",
            "apply_remote_rename_and_replace failed during local apply",
            json!({
                "failure": "replace_io",
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

fn record_resolve_file_conflict_issue(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    kind: &'static str,
    error: &ExecuteResolveFileConflictError,
    created_at_ms: u64,
) {
    let (summary, detail_json) = match error {
        ExecuteResolveFileConflictError::CurrentPathMissing { .. } => (
            "resolve file conflict could not resolve the current local path",
            json!({ "failure": "current_path_missing" }),
        ),
        ExecuteResolveFileConflictError::TargetPathMissing { .. } => (
            "resolve file conflict could not resolve the target local path",
            json!({ "failure": "target_path_missing" }),
        ),
        ExecuteResolveFileConflictError::DestinationOccupied { .. } => (
            "resolve file conflict found the destination slot already occupied",
            json!({ "failure": "destination_occupied" }),
        ),
        ExecuteResolveFileConflictError::PathBindingSourcePathMissing {
            client_file_id, ..
        } => (
            "resolve path binding collision could not resolve the local-only source path",
            json!({
                "failure": "source_path_missing",
                "client_file_id": client_file_id,
            }),
        ),
        ExecuteResolveFileConflictError::LocalApplyFailed {
            operation,
            path,
            source,
        } => (
            "resolve file conflict failed during local apply",
            json!({
                "failure": if kind == "resolve_delete_vs_edit_conflict_local_apply_failed" {
                    "unlink_io"
                } else {
                    "replace_io"
                },
                "operation": operation,
                "path": path,
                "source": source.to_string(),
            }),
        ),
        ExecuteResolveFileConflictError::ConflictArtifact(error) => (
            "resolve file conflict failed while preserving the loser artifact",
            json!({
                "failure": "artifact_persist_failed",
                "source": error.to_string(),
            }),
        ),
        ExecuteResolveFileConflictError::Download(error) => (
            "resolve file conflict failed while loading the authoritative winner",
            json!({
                "failure": "winner_download_failed",
                "source": error.to_string(),
            }),
        ),
        _ => return,
    };

    let _ = db.record_conflict_or_error(
        namespace_id,
        inode_id,
        kind,
        summary,
        &detail_json,
        created_at_ms,
    );
}

fn record_resolve_subtree_conflict_issue(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    kind: &'static str,
    error: &ExecuteResolveSubtreeConflictError,
    created_at_ms: u64,
) {
    let (summary, detail_json) = match error {
        ExecuteResolveSubtreeConflictError::CurrentPathMissing { .. } => (
            "resolve subtree conflict could not resolve the current local path",
            json!({ "failure": "current_path_missing" }),
        ),
        ExecuteResolveSubtreeConflictError::TargetPathMissing { .. } => (
            "resolve subtree conflict could not resolve the target local path",
            json!({ "failure": "target_path_missing" }),
        ),
        ExecuteResolveSubtreeConflictError::DestinationOccupied { .. } => (
            "resolve subtree conflict found the destination slot already occupied",
            json!({ "failure": "destination_occupied" }),
        ),
        ExecuteResolveSubtreeConflictError::WinnerRestoreIo {
            operation,
            path,
            source,
        } => (
            "resolve subtree conflict failed while restoring the authoritative winner subtree",
            json!({
                "failure": "winner_restore_io",
                "operation": operation,
                "path": path,
                "source": source.to_string(),
            }),
        ),
        ExecuteResolveSubtreeConflictError::RecursiveRemoveIo {
            operation,
            path,
            source,
        } => (
            "resolve subtree conflict failed while removing the loser subtree",
            json!({
                "failure": "recursive_remove_io",
                "operation": operation,
                "path": path,
                "source": source.to_string(),
            }),
        ),
        ExecuteResolveSubtreeConflictError::RenameIo {
            operation,
            path,
            source,
        } => (
            "resolve subtree conflict failed during the final authoritative rename",
            json!({
                "failure": "rename_io",
                "operation": operation,
                "path": path,
                "source": source.to_string(),
            }),
        ),
        ExecuteResolveSubtreeConflictError::ConflictArtifact(error) => (
            "resolve subtree conflict failed while preserving the loser subtree artifact",
            json!({
                "failure": match error {
                    ConflictArtifactError::Upload(_) => "artifact_upload",
                    _ => "artifact_write",
                },
                "source": error.to_string(),
            }),
        ),
        ExecuteResolveSubtreeConflictError::Download(error) => (
            "resolve subtree conflict failed while loading authoritative winner content",
            json!({
                "failure": "winner_restore_io",
                "source": error.to_string(),
            }),
        ),
        _ => return,
    };

    let _ = db.record_conflict_or_error(
        namespace_id,
        inode_id,
        kind,
        summary,
        &detail_json,
        created_at_ms,
    );
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
