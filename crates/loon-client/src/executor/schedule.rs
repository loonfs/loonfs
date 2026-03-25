use super::inode::{
    execute_apply_remote_delete_with_hooks, execute_apply_remote_rename_and_replace_with_hooks,
    execute_apply_remote_rename_with_hooks, execute_apply_remote_subtree_delete_with_hooks,
    execute_apply_remote_subtree_rename_with_hooks, execute_download_remote_edit_with_hooks,
    execute_materialize_remote_dir_with_hooks, execute_resolve_delete_vs_edit_conflict_with_hooks,
    execute_resolve_path_binding_collision_with_hooks,
    execute_resolve_rename_vs_edit_conflict_with_hooks,
    execute_resolve_same_inode_conflict_with_hooks,
    execute_resolve_subtree_delete_conflict_with_hooks,
    execute_resolve_subtree_rename_conflict_with_hooks, execute_upload_local_edit_with_hooks,
};
use super::local_only::{execute_local_only_create, execute_local_only_create_with_hooks};
use super::*;
use crate::executor::dispatch::dispatch_inode_mutation_from_state_with_hooks;
use crate::local_fs::NamespacePathIndex;
use crate::state_db::{ClientFileId, SqliteStateDb};
use crate::testing::{ClientExecutionHooks, NoopClientExecutionHooks};
use loon_objectstore::ObjectStore;
use loon_types::{ClientMutationRequest, ClientMutationResponse, InodeId, NamespaceId};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
enum NextClientActionCandidate {
    LocalOnlyCreate(LocalOnlyPlannedActionRow),
    ExecutablePlannedAction(PlannedActionRow),
    DeferredPlannedAction(PlannedActionRow),
}

#[allow(clippy::too_many_arguments, clippy::result_large_err)]
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
    execute_next_client_action_with_hooks(
        db,
        store,
        resolve_local_only_source_path,
        resolve_inode_source_path,
        resolve_inode_target_path,
        uploaded_at_ms,
        created_at_ms,
        &NoopClientExecutionHooks,
        dispatch,
    )
}

#[allow(clippy::too_many_arguments, clippy::result_large_err)]
pub(crate) fn execute_next_client_action_with_hooks<S, LP, IP, ITP, F>(
    db: &mut SqliteStateDb,
    store: &S,
    resolve_local_only_source_path: LP,
    resolve_inode_source_path: IP,
    resolve_inode_target_path: ITP,
    uploaded_at_ms: u64,
    created_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
    dispatch: F,
) -> Result<Option<NextClientAction>, ExecuteNextClientActionError>
where
    S: ObjectStore,
    LP: FnOnce(&ClientFileId) -> Option<PathBuf>,
    IP: FnOnce(&NamespaceId, InodeId) -> Option<PathBuf>,
    ITP: FnOnce(&NamespaceId, InodeId, Option<InodeId>, &str) -> Option<PathBuf>,
    F: FnOnce(&ClientMutationRequest) -> Result<ClientMutationResponse, String>,
{
    let next_local_only = db.load_next_planned_local_only_action()?;
    let next_executable = db.load_next_executable_planned_action()?;
    let next_deferred = db.load_next_deferred_planned_action()?;
    let same_path_replacement_override = match next_local_only.as_ref() {
        Some(local_only) => load_same_path_replacement_override(db, local_only)?,
        None => None,
    };

    let next_action = match select_next_client_action_candidate(
        next_local_only,
        next_executable,
        next_deferred,
        same_path_replacement_override,
    ) {
        Some(action) => action,
        None => return Ok(None),
    };

    match next_action {
        NextClientActionCandidate::LocalOnlyCreate(planned_action) => {
            let client_file_id = planned_action.client_file_id.clone();
            let source_path = resolve_local_only_source_path(&client_file_id);
            let executed = execute_local_only_create_with_hooks(
                db,
                store,
                &client_file_id,
                source_path.as_deref(),
                uploaded_at_ms,
                created_at_ms,
                hooks,
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
                    let executed = execute_upload_local_edit_with_hooks(
                        db,
                        store,
                        &namespace_id,
                        inode_id,
                        source_path.as_deref(),
                        uploaded_at_ms,
                        created_at_ms,
                        hooks,
                        dispatch,
                    )?;
                    Ok(Some(NextClientAction::ExecutedUploadLocalEdit(executed)))
                }
                value
                    if value == PlannerDecision::Rename.as_str()
                        || value == PlannerDecision::DeleteFile.as_str()
                        || value == PlannerDecision::DeleteSubtree.as_str() =>
                {
                    let decision = if value == PlannerDecision::Rename.as_str() {
                        PlannerDecision::Rename
                    } else if value == PlannerDecision::DeleteFile.as_str() {
                        PlannerDecision::DeleteFile
                    } else {
                        PlannerDecision::DeleteSubtree
                    };
                    let dispatched = dispatch_inode_mutation_from_state_with_hooks(
                        db,
                        &namespace_id,
                        inode_id,
                        created_at_ms,
                        hooks,
                        dispatch,
                    )?;
                    Ok(Some(NextClientAction::ExecutedDispatchInodeMutation(
                        ExecutedDispatchInodeMutation {
                            decision,
                            dispatched,
                        },
                    )))
                }
                value if value == PlannerDecision::DownloadRemoteEdit.as_str() => {
                    let target_path = resolve_inode_source_path(&namespace_id, inode_id);
                    let executed = execute_download_remote_edit_with_hooks(
                        db,
                        store,
                        &namespace_id,
                        inode_id,
                        target_path.as_deref(),
                        created_at_ms,
                        hooks,
                    )?;
                    Ok(Some(NextClientAction::ExecutedDownloadRemoteEdit(executed)))
                }
                value if value == PlannerDecision::ApplyRemoteDelete.as_str() => {
                    let current_path = resolve_inode_source_path(&namespace_id, inode_id);
                    let executed = execute_apply_remote_delete_with_hooks(
                        db,
                        &namespace_id,
                        inode_id,
                        current_path.as_deref(),
                        created_at_ms,
                        hooks,
                    )?;
                    Ok(Some(NextClientAction::ExecutedApplyRemoteDelete(executed)))
                }
                value if value == PlannerDecision::ApplyRemoteSubtreeDelete.as_str() => {
                    let current_path = resolve_inode_source_path(&namespace_id, inode_id);
                    let executed = execute_apply_remote_subtree_delete_with_hooks(
                        db,
                        &namespace_id,
                        inode_id,
                        current_path.as_deref(),
                        created_at_ms,
                        hooks,
                    )?;
                    Ok(Some(NextClientAction::ExecutedApplyRemoteSubtreeDelete(
                        executed,
                    )))
                }
                value if value == PlannerDecision::ApplyRemoteSubtreeRename.as_str() => {
                    let current_path = resolve_inode_source_path(&namespace_id, inode_id);
                    let views =
                        db.load_bound_apply_remote_subtree_rename_views(&namespace_id, inode_id)?;
                    let target_path = resolve_inode_target_path(
                        &namespace_id,
                        inode_id,
                        views.root_remote.parent_inode_id,
                        &views.root_remote.display_name,
                    );
                    let executed = execute_apply_remote_subtree_rename_with_hooks(
                        db,
                        &namespace_id,
                        inode_id,
                        current_path.as_deref(),
                        target_path.as_deref(),
                        created_at_ms,
                        hooks,
                    )?;
                    Ok(Some(NextClientAction::ExecutedApplyRemoteSubtreeRename(
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
                    let executed = execute_apply_remote_rename_with_hooks(
                        db,
                        &namespace_id,
                        inode_id,
                        current_path.as_deref(),
                        target_path.as_deref(),
                        created_at_ms,
                        hooks,
                    )?;
                    Ok(Some(NextClientAction::ExecutedApplyRemoteRename(executed)))
                }
                value if value == PlannerDecision::ApplyRemoteRenameAndReplace.as_str() => {
                    let current_path = resolve_inode_source_path(&namespace_id, inode_id);
                    let views = db.load_file_sync_views(&namespace_id, inode_id)?;
                    let remote = views.remote.ok_or_else(|| {
                        StateDbError::ApplyRemoteRenameStateMissing {
                            namespace_id: namespace_id.as_str().to_owned(),
                            inode_id: inode_id.0,
                        }
                    })?;
                    let target_path = resolve_inode_target_path(
                        &namespace_id,
                        inode_id,
                        remote.parent_inode_id,
                        &remote.display_name,
                    );
                    let executed = execute_apply_remote_rename_and_replace_with_hooks(
                        db,
                        store,
                        &namespace_id,
                        inode_id,
                        current_path.as_deref(),
                        target_path.as_deref(),
                        created_at_ms,
                        hooks,
                    )?;
                    Ok(Some(NextClientAction::ExecutedApplyRemoteRenameAndReplace(
                        executed,
                    )))
                }
                value if value == PlannerDecision::ResolveSameInodeConflict.as_str() => {
                    let current_path = resolve_inode_source_path(&namespace_id, inode_id);
                    let executed = execute_resolve_same_inode_conflict_with_hooks(
                        db,
                        store,
                        &namespace_id,
                        inode_id,
                        current_path.as_deref(),
                        created_at_ms,
                        hooks,
                    )?;
                    Ok(Some(NextClientAction::ExecutedResolveSameInodeConflict(
                        executed,
                    )))
                }
                value if value == PlannerDecision::ResolveDeleteVsEditConflict.as_str() => {
                    let current_path = resolve_inode_source_path(&namespace_id, inode_id);
                    let executed = execute_resolve_delete_vs_edit_conflict_with_hooks(
                        db,
                        store,
                        &namespace_id,
                        inode_id,
                        current_path.as_deref(),
                        created_at_ms,
                        hooks,
                    )?;
                    Ok(Some(NextClientAction::ExecutedResolveDeleteVsEditConflict(
                        executed,
                    )))
                }
                value if value == PlannerDecision::ResolveRenameVsEditConflict.as_str() => {
                    let current_path = resolve_inode_source_path(&namespace_id, inode_id);
                    let views = db.load_file_sync_views(&namespace_id, inode_id)?;
                    let remote = views.remote.ok_or_else(|| {
                        StateDbError::ApplyRemoteRenameStateMissing {
                            namespace_id: namespace_id.as_str().to_owned(),
                            inode_id: inode_id.0,
                        }
                    })?;
                    let target_path = resolve_inode_target_path(
                        &namespace_id,
                        inode_id,
                        remote.parent_inode_id,
                        &remote.display_name,
                    );
                    let executed = execute_resolve_rename_vs_edit_conflict_with_hooks(
                        db,
                        store,
                        &namespace_id,
                        inode_id,
                        current_path.as_deref(),
                        target_path.as_deref(),
                        created_at_ms,
                        hooks,
                    )?;
                    Ok(Some(NextClientAction::ExecutedResolveRenameVsEditConflict(
                        executed,
                    )))
                }
                value if value == PlannerDecision::ResolvePathBindingCollision.as_str() => {
                    let views = db.load_file_sync_views(&namespace_id, inode_id)?;
                    let remote = views.remote.ok_or_else(|| {
                        StateDbError::DownloadRemoteEditStateMissing {
                            namespace_id: namespace_id.as_str().to_owned(),
                            inode_id: inode_id.0,
                        }
                    })?;
                    let candidates = db.load_local_only_candidates_for_namespace(&namespace_id)?;
                    let collision = candidates
                        .into_iter()
                        .filter(|candidate| {
                            candidate.exists_on_disk
                                && candidate.parent_inode_id == remote.parent_inode_id
                                && candidate.display_name == remote.display_name
                                && candidate.content_digest != remote.content_digest
                        })
                        .collect::<Vec<_>>();
                    let source_path = match collision.as_slice() {
                        [candidate] => resolve_local_only_source_path(&candidate.client_file_id),
                        _ => None,
                    };
                    let executed = execute_resolve_path_binding_collision_with_hooks(
                        db,
                        store,
                        &namespace_id,
                        inode_id,
                        source_path.as_deref(),
                        created_at_ms,
                        hooks,
                    )?;
                    Ok(Some(NextClientAction::ExecutedResolvePathBindingCollision(
                        executed,
                    )))
                }
                value if value == PlannerDecision::ResolveSubtreeDeleteConflict.as_str() => {
                    let current_path = resolve_inode_source_path(&namespace_id, inode_id);
                    let executed = execute_resolve_subtree_delete_conflict_with_hooks(
                        db,
                        store,
                        &namespace_id,
                        inode_id,
                        current_path.as_deref(),
                        created_at_ms,
                        hooks,
                    )?;
                    Ok(Some(
                        NextClientAction::ExecutedResolveSubtreeDeleteConflict(executed),
                    ))
                }
                value if value == PlannerDecision::ResolveSubtreeRenameConflict.as_str() => {
                    let current_path = resolve_inode_source_path(&namespace_id, inode_id);
                    let views = db.load_bound_resolve_subtree_rename_conflict_views(
                        &namespace_id,
                        inode_id,
                    )?;
                    let target_path = resolve_inode_target_path(
                        &namespace_id,
                        inode_id,
                        views.root_remote.parent_inode_id,
                        &views.root_remote.display_name,
                    );
                    let executed = execute_resolve_subtree_rename_conflict_with_hooks(
                        db,
                        store,
                        &namespace_id,
                        inode_id,
                        current_path.as_deref(),
                        target_path.as_deref(),
                        created_at_ms,
                        hooks,
                    )?;
                    Ok(Some(
                        NextClientAction::ExecutedResolveSubtreeRenameConflict(executed),
                    ))
                }
                value if value == PlannerDecision::MaterializeRemoteDir.as_str() => {
                    let target_path = resolve_inode_source_path(&namespace_id, inode_id);
                    let executed = execute_materialize_remote_dir_with_hooks(
                        db,
                        &namespace_id,
                        inode_id,
                        target_path.as_deref(),
                        created_at_ms,
                        hooks,
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

#[allow(clippy::result_large_err)]
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
    same_path_replacement_override: Option<PlannedActionRow>,
) -> Option<NextClientActionCandidate> {
    if let Some(planned_action) = next_executable_planned_action.as_ref() {
        if matches!(
            planned_action.decision.as_str(),
            value
                if value == PlannerDecision::ResolveSubtreeDeleteConflict.as_str()
                    || value == PlannerDecision::ResolveSubtreeRenameConflict.as_str()
        ) {
            return Some(NextClientActionCandidate::ExecutablePlannedAction(
                planned_action.clone(),
            ));
        }
    }

    if let Some(planned_action) = same_path_replacement_override {
        return Some(NextClientActionCandidate::DeferredPlannedAction(
            planned_action,
        ));
    }

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

fn load_same_path_replacement_override(
    db: &SqliteStateDb,
    local_only_action: &LocalOnlyPlannedActionRow,
) -> Result<Option<PlannedActionRow>, StateDbError> {
    // This is a narrow correctness guard for exact same-path replacements observed through
    // observe-subtree. If this class of dependency grows, move it into planner-visible waiting
    // state instead of adding more scheduler-only path exceptions here.
    if !is_local_only_create_decision(local_only_action.decision.as_str()) {
        return Ok(None);
    }

    let summary = db.load_namespace_state_summary(&local_only_action.namespace_id)?;
    let parent_links =
        db.load_local_only_parent_links_for_namespace(&local_only_action.namespace_id)?;
    let path_index = NamespacePathIndex::build(&summary, &parent_links);
    let Some(relative_path) =
        path_index.resolve_local_only_source_relative_path(&local_only_action.client_file_id)
    else {
        return Ok(None);
    };

    let mut bound_matches = path_index
        .bound_file_matches(relative_path)
        .iter()
        .chain(path_index.bound_dir_matches(relative_path).iter());
    let Some(occupying_bound) = bound_matches.next() else {
        return Ok(None);
    };
    if bound_matches.next().is_some() {
        return Ok(None);
    }

    let planned_action =
        db.load_planned_action(&local_only_action.namespace_id, occupying_bound.inode_id)?;
    Ok(planned_action
        .filter(|planned_action| is_path_vacating_bound_decision(planned_action.decision.as_str())))
}

fn is_local_only_create_decision(decision: &str) -> bool {
    matches!(
        decision,
        value
            if value == PlannerDecision::UploadLocalCreate.as_str()
                || value == PlannerDecision::CreateRemoteDir.as_str()
    )
}

fn is_path_vacating_bound_decision(decision: &str) -> bool {
    matches!(
        decision,
        value
            if value == PlannerDecision::DeleteFile.as_str()
                || value == PlannerDecision::DeleteSubtree.as_str()
                || value == PlannerDecision::Rename.as_str()
    )
}
