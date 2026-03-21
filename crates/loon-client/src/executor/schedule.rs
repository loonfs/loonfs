use super::inode::{
    execute_apply_remote_delete, execute_apply_remote_rename, execute_apply_remote_subtree_delete,
    execute_download_remote_edit, execute_materialize_remote_dir, execute_upload_local_edit,
};
use super::local_only::execute_local_only_create;
use super::*;
use crate::state_db::{ClientFileId, SqliteStateDb};
use loon_objectstore::ObjectStore;
use loon_types::{ClientMutationRequest, ClientMutationResponse, InodeId, NamespaceId};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
enum NextClientActionCandidate {
    LocalOnlyCreate(LocalOnlyPlannedActionRow),
    ExecutablePlannedAction(PlannedActionRow),
    DeferredPlannedAction(PlannedActionRow),
}

pub fn execute_next_client_action<S, LP, IP, ITP, F>(
    db: &mut SqliteStateDb,
    store: &S,
    resolve_local_only_source_path: LP,
    resolve_inode_source_path: IP,
    resolve_inode_target_path: ITP,
    uploaded_at_ms: u64,
    created_at_ms: u64,
    dispatch: F,
) -> Result<Option<NextClientAction>, ExecuteNextClientActionError>
where
    S: ObjectStore,
    LP: FnOnce(&ClientFileId) -> Option<PathBuf>,
    IP: FnOnce(&NamespaceId, InodeId) -> Option<PathBuf>,
    ITP: FnOnce(&NamespaceId, InodeId, Option<InodeId>, &str) -> Option<PathBuf>,
    F: FnOnce(&ClientMutationRequest) -> Result<ClientMutationResponse, String>,
{
    let next_action = match select_next_client_action_candidate(
        db.load_next_planned_local_only_action()?,
        db.load_next_executable_planned_action()?,
        db.load_next_deferred_planned_action()?,
    ) {
        Some(action) => action,
        None => return Ok(None),
    };

    match next_action {
        NextClientActionCandidate::LocalOnlyCreate(planned_action) => {
            let client_file_id = planned_action.client_file_id.clone();
            let source_path = resolve_local_only_source_path(&client_file_id);
            let executed = execute_local_only_create(
                db,
                store,
                &client_file_id,
                source_path.as_deref(),
                uploaded_at_ms,
                created_at_ms,
                dispatch,
            )?;

            Ok(Some(NextClientAction::ExecutedLocalOnlyCreate(
                ExecutedNextLocalOnlyCreate {
                    planned_action,
                    executed,
                },
            )))
        }
        NextClientActionCandidate::ExecutablePlannedAction(planned_action)
        | NextClientActionCandidate::DeferredPlannedAction(planned_action) => {
            let namespace_id = planned_action.namespace_id.clone();
            let inode_id = planned_action.inode_id;
            match planned_action.decision.as_str() {
                value if value == PlannerDecision::UploadLocalEdit.as_str() => {
                    let source_path = resolve_inode_source_path(&namespace_id, inode_id);
                    let executed = execute_upload_local_edit(
                        db,
                        store,
                        &namespace_id,
                        inode_id,
                        source_path.as_deref(),
                        uploaded_at_ms,
                        created_at_ms,
                        dispatch,
                    )?;
                    Ok(Some(NextClientAction::ExecutedUploadLocalEdit(executed)))
                }
                value if value == PlannerDecision::DownloadRemoteEdit.as_str() => {
                    let target_path = resolve_inode_source_path(&namespace_id, inode_id);
                    let executed = execute_download_remote_edit(
                        db,
                        store,
                        &namespace_id,
                        inode_id,
                        target_path.as_deref(),
                        created_at_ms,
                    )?;
                    Ok(Some(NextClientAction::ExecutedDownloadRemoteEdit(executed)))
                }
                value if value == PlannerDecision::ApplyRemoteDelete.as_str() => {
                    let current_path = resolve_inode_source_path(&namespace_id, inode_id);
                    let executed = execute_apply_remote_delete(
                        db,
                        &namespace_id,
                        inode_id,
                        current_path.as_deref(),
                        created_at_ms,
                    )?;
                    Ok(Some(NextClientAction::ExecutedApplyRemoteDelete(executed)))
                }
                value if value == PlannerDecision::ApplyRemoteSubtreeDelete.as_str() => {
                    let current_path = resolve_inode_source_path(&namespace_id, inode_id);
                    let executed = execute_apply_remote_subtree_delete(
                        db,
                        &namespace_id,
                        inode_id,
                        current_path.as_deref(),
                        created_at_ms,
                    )?;
                    Ok(Some(NextClientAction::ExecutedApplyRemoteSubtreeDelete(
                        executed,
                    )))
                }
                value if value == PlannerDecision::ApplyRemoteRename.as_str() => {
                    let current_path = resolve_inode_source_path(&namespace_id, inode_id);
                    let (remote, _local, _anchor) =
                        db.load_bound_apply_remote_rename_views(&namespace_id, inode_id)?;
                    let target_path = resolve_inode_target_path(
                        &namespace_id,
                        inode_id,
                        remote.parent_inode_id,
                        &remote.display_name,
                    );
                    let executed = execute_apply_remote_rename(
                        db,
                        &namespace_id,
                        inode_id,
                        current_path.as_deref(),
                        target_path.as_deref(),
                        created_at_ms,
                    )?;
                    Ok(Some(NextClientAction::ExecutedApplyRemoteRename(executed)))
                }
                value if value == PlannerDecision::MaterializeRemoteDir.as_str() => {
                    let target_path = resolve_inode_source_path(&namespace_id, inode_id);
                    let executed = execute_materialize_remote_dir(
                        db,
                        &namespace_id,
                        inode_id,
                        target_path.as_deref(),
                        created_at_ms,
                    )?;
                    Ok(Some(NextClientAction::ExecutedMaterializeRemoteDir(
                        executed,
                    )))
                }
                _ => Ok(Some(NextClientAction::SelectedPlannedAction(
                    planned_action,
                ))),
            }
        }
    }
}

pub fn execute_next_local_only_create<S, P, F>(
    db: &mut SqliteStateDb,
    store: &S,
    resolve_source_path: P,
    uploaded_at_ms: u64,
    created_at_ms: u64,
    dispatch: F,
) -> Result<Option<ExecutedNextLocalOnlyCreate>, ExecuteNextLocalOnlyCreateError>
where
    S: ObjectStore,
    P: FnOnce(&ClientFileId) -> Option<PathBuf>,
    F: FnOnce(&ClientMutationRequest) -> Result<ClientMutationResponse, String>,
{
    let planned_action = match db.load_next_planned_local_only_action()? {
        Some(action) => action,
        None => return Ok(None),
    };

    let client_file_id = planned_action.client_file_id.clone();
    let source_path = resolve_source_path(&client_file_id);
    let executed = execute_local_only_create(
        db,
        store,
        &client_file_id,
        source_path.as_deref(),
        uploaded_at_ms,
        created_at_ms,
        dispatch,
    )?;

    Ok(Some(ExecutedNextLocalOnlyCreate {
        planned_action,
        executed,
    }))
}

fn select_next_client_action_candidate(
    next_local_only: Option<LocalOnlyPlannedActionRow>,
    next_executable_planned_action: Option<PlannedActionRow>,
    next_deferred_planned_action: Option<PlannedActionRow>,
) -> Option<NextClientActionCandidate> {
    match (
        next_local_only,
        next_executable_planned_action,
        next_deferred_planned_action,
    ) {
        (Some(local_only), _, _) => Some(NextClientActionCandidate::LocalOnlyCreate(local_only)),
        (None, Some(planned_action), _) => Some(
            NextClientActionCandidate::ExecutablePlannedAction(planned_action),
        ),
        (None, None, Some(planned_action)) => Some(
            NextClientActionCandidate::DeferredPlannedAction(planned_action),
        ),
        (None, None, None) => None,
    }
}
