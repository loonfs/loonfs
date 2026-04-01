use crate::executor::{
    execute_download_remote_edit_to_path, execute_materialize_remote_dir_to_path,
    DownloadRemoteEditExecution, ExecuteDownloadRemoteEditError, ExecuteMaterializeRemoteDirError,
};
use crate::local_fs::{join_under_mirror_root, NamespacePathIndex};
use crate::planner::{plan_file, PlannerDecision, PlannerError};
use crate::state_db::{
    ClientFileId, ClientNamespaceStateSummary, FileSyncViews, SqliteStateDb, StateDbError,
};
use loon_server::objectstore::ObjectStore;
use loon_types::{InodeId, InodeKind, NamespaceId};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderMaterializedPath {
    pub absolute_path: PathBuf,
    pub relative_path: String,
}

#[derive(Debug, Error)]
pub enum ProviderTargetedMaterializeError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Planner(#[from] PlannerError),
    #[error(transparent)]
    DownloadRemoteEdit(#[from] ExecuteDownloadRemoteEditError),
    #[error(transparent)]
    MaterializeRemoteDir(#[from] ExecuteMaterializeRemoteDirError),
    #[error("provider_inode_missing: namespace `{namespace_id}` inode `{inode_id}`")]
    InodeMissing { namespace_id: String, inode_id: u64 },
    #[error("provider_inode_unsupported_kind: namespace `{namespace_id}` inode `{inode_id}` kind `{inode_kind}`")]
    InodeUnsupportedKind {
        namespace_id: String,
        inode_id: u64,
        inode_kind: String,
    },
    #[error("provider_inode_path_unavailable: namespace `{namespace_id}` inode `{inode_id}`")]
    InodePathUnavailable { namespace_id: String, inode_id: u64 },
    #[error(
        "provider_inode_unavailable: namespace `{namespace_id}` inode `{inode_id}` decision `{decision}` reason `{reason}`"
    )]
    InodeUnavailable {
        namespace_id: String,
        inode_id: u64,
        decision: String,
        reason: String,
    },
    #[error("provider_local_only_missing: `{client_file_id}`")]
    LocalOnlyMissing { client_file_id: String },
    #[error("provider_local_only_path_unavailable: `{client_file_id}`")]
    LocalOnlyPathUnavailable { client_file_id: String },
    #[error("provider_local_only_unavailable: `{client_file_id}` reason `{reason}`")]
    LocalOnlyUnavailable {
        client_file_id: String,
        reason: String,
    },
}

struct NamespaceProviderView {
    summary: ClientNamespaceStateSummary,
    path_index: NamespacePathIndex,
}

pub fn materialize_local_only_to_mirror_root(
    db: &mut SqliteStateDb,
    client_file_id: &ClientFileId,
    mirror_root: &Path,
) -> Result<ProviderMaterializedPath, ProviderTargetedMaterializeError> {
    let local_only = db.load_local_only_file(client_file_id)?.ok_or_else(|| {
        ProviderTargetedMaterializeError::LocalOnlyMissing {
            client_file_id: client_file_id.as_str().to_owned(),
        }
    })?;
    if !local_only.exists_on_disk {
        return Err(ProviderTargetedMaterializeError::LocalOnlyUnavailable {
            client_file_id: client_file_id.as_str().to_owned(),
            reason: "not_materialized_on_disk".to_owned(),
        });
    }

    let view = load_namespace_provider_view(db, &local_only.namespace_id)?;
    if view
        .summary
        .local_only_conflicts_and_errors
        .iter()
        .any(|row| row.client_file_id == *client_file_id)
    {
        return Err(ProviderTargetedMaterializeError::LocalOnlyUnavailable {
            client_file_id: client_file_id.as_str().to_owned(),
            reason: "has_recorded_issue".to_owned(),
        });
    }
    if let Some(planned) = view
        .summary
        .local_only_planned_actions
        .iter()
        .find(|row| row.client_file_id == *client_file_id)
    {
        if planned.decision != PlannerDecision::UploadLocalCreate.as_str()
            && planned.decision != PlannerDecision::CreateRemoteDir.as_str()
            && planned.decision != PlannerDecision::NoOp.as_str()
        {
            return Err(ProviderTargetedMaterializeError::LocalOnlyUnavailable {
                client_file_id: client_file_id.as_str().to_owned(),
                reason: planned.decision.clone(),
            });
        }
    }

    let relative_path = view
        .path_index
        .resolve_local_only_source_relative_path(client_file_id)
        .ok_or_else(
            || ProviderTargetedMaterializeError::LocalOnlyPathUnavailable {
                client_file_id: client_file_id.as_str().to_owned(),
            },
        )?
        .to_owned();
    Ok(ProviderMaterializedPath {
        absolute_path: join_under_mirror_root(mirror_root, &relative_path),
        relative_path,
    })
}

pub fn materialize_inode_to_mirror_root<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    mirror_root: &Path,
    now_ms: u64,
) -> Result<ProviderMaterializedPath, ProviderTargetedMaterializeError> {
    let view = load_namespace_provider_view(db, namespace_id)?;
    let views = db.load_file_sync_views(namespace_id, inode_id)?;

    if let Some(local) = &views.local {
        if local.inode_kind == InodeKind::Symlink || local.inode_kind == InodeKind::Mount {
            return Err(ProviderTargetedMaterializeError::InodeUnsupportedKind {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                inode_kind: format!("{:?}", local.inode_kind).to_lowercase(),
            });
        }
        if local.exists_on_disk {
            let relative_path = resolve_inode_relative_path(&view, &views, inode_id)?;
            return Ok(ProviderMaterializedPath {
                absolute_path: join_under_mirror_root(mirror_root, &relative_path),
                relative_path,
            });
        }
    }

    let remote =
        views
            .remote
            .clone()
            .ok_or_else(|| ProviderTargetedMaterializeError::InodeMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
    if remote.inode_kind == InodeKind::Symlink || remote.inode_kind == InodeKind::Mount {
        return Err(ProviderTargetedMaterializeError::InodeUnsupportedKind {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            inode_kind: format!("{:?}", remote.inode_kind).to_lowercase(),
        });
    }

    materialize_remote_ancestors(db, store, namespace_id, inode_id, mirror_root, now_ms)?;

    let relative_path = resolve_inode_relative_path(&view, &views, inode_id)?;
    let absolute_path = join_under_mirror_root(mirror_root, &relative_path);
    let planned = plan_file(db, namespace_id, inode_id, now_ms)?;
    ensure_inode_runnable_for_provider(namespace_id, inode_id, &planned.decision, &planned.reason)?;

    match planned.decision {
        PlannerDecision::MaterializeRemoteDir => {
            execute_materialize_remote_dir_to_path(
                db,
                store,
                namespace_id,
                inode_id,
                &absolute_path,
                now_ms,
            )?;
        }
        PlannerDecision::DownloadRemoteEdit => {
            let mut applied_at_ms = now_ms;
            while let DownloadRemoteEditExecution::Progressed(_) =
                execute_download_remote_edit_to_path(
                    db,
                    store,
                    namespace_id,
                    inode_id,
                    &absolute_path,
                    applied_at_ms,
                )?
            {
                applied_at_ms = applied_at_ms.saturating_add(1);
            }
        }
        _ => {}
    }

    Ok(ProviderMaterializedPath {
        absolute_path,
        relative_path,
    })
}

fn load_namespace_provider_view(
    db: &SqliteStateDb,
    namespace_id: &NamespaceId,
) -> Result<NamespaceProviderView, StateDbError> {
    let summary = db.load_namespace_state_summary(namespace_id)?;
    let parent_links = db.load_local_only_parent_links_for_namespace(namespace_id)?;
    let path_index = NamespacePathIndex::build(&summary, &parent_links);
    Ok(NamespaceProviderView {
        summary,
        path_index,
    })
}

fn resolve_inode_relative_path(
    view: &NamespaceProviderView,
    views: &FileSyncViews,
    inode_id: InodeId,
) -> Result<String, ProviderTargetedMaterializeError> {
    if let Some(path) = view
        .path_index
        .resolve_current_inode_relative_path(inode_id)
    {
        return Ok(path.to_owned());
    }

    if let Some(remote) = &views.remote {
        if let Some(path) = view.path_index.resolve_target_inode_relative_path(
            inode_id,
            remote.parent_inode_id,
            &remote.display_name,
        ) {
            return Ok(path);
        }
    }
    if let Some(anchor) = &views.sync_anchor {
        if let Some(path) = view.path_index.resolve_target_inode_relative_path(
            inode_id,
            anchor.parent_inode_id,
            &anchor.display_name,
        ) {
            return Ok(path);
        }
    }
    if let Some(local) = &views.local {
        if let Some(path) = view.path_index.resolve_target_inode_relative_path(
            inode_id,
            local.parent_inode_id,
            &local.display_name,
        ) {
            return Ok(path);
        }
    }

    Err(ProviderTargetedMaterializeError::InodePathUnavailable {
        namespace_id: views.namespace_id.as_str().to_owned(),
        inode_id: inode_id.0,
    })
}

fn materialize_remote_ancestors<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    mirror_root: &Path,
    now_ms: u64,
) -> Result<(), ProviderTargetedMaterializeError> {
    let view = load_namespace_provider_view(db, namespace_id)?;
    let remote_by_inode = view
        .summary
        .remote_state
        .iter()
        .map(|row| (row.inode_id, row.clone()))
        .collect::<BTreeMap<_, _>>();
    let local_by_inode = view
        .summary
        .local_state
        .iter()
        .map(|row| (row.inode_id, row.clone()))
        .collect::<BTreeMap<_, _>>();

    let mut ancestors = Vec::new();
    let mut parent = remote_by_inode
        .get(&inode_id)
        .and_then(|row| row.parent_inode_id);
    while let Some(parent_inode_id) = parent {
        let Some(remote_parent) = remote_by_inode.get(&parent_inode_id) else {
            break;
        };
        if remote_parent.parent_inode_id.is_none() {
            ancestors.push(parent_inode_id);
            break;
        }
        ancestors.push(parent_inode_id);
        parent = remote_parent.parent_inode_id;
    }
    ancestors.reverse();

    let mut applied_at_ms = now_ms;
    for ancestor_inode_id in ancestors {
        let Some(remote_parent) = remote_by_inode.get(&ancestor_inode_id) else {
            continue;
        };
        if remote_parent.inode_kind != InodeKind::Dir || remote_parent.is_deleted {
            continue;
        }
        if local_by_inode
            .get(&ancestor_inode_id)
            .is_some_and(|local| local.exists_on_disk)
        {
            continue;
        }
        if view
            .summary
            .conflicts_and_errors
            .iter()
            .any(|row| row.inode_id == ancestor_inode_id)
        {
            return Err(ProviderTargetedMaterializeError::InodeUnavailable {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: ancestor_inode_id.0,
                decision: "no_op".to_owned(),
                reason: "has_recorded_issue".to_owned(),
            });
        }
        if view
            .summary
            .pending_inode_mutations
            .iter()
            .any(|row| row.inode_id == ancestor_inode_id)
        {
            return Err(ProviderTargetedMaterializeError::InodeUnavailable {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: ancestor_inode_id.0,
                decision: "no_op".to_owned(),
                reason: "has_pending_inode_mutation".to_owned(),
            });
        }

        let planned = plan_file(db, namespace_id, ancestor_inode_id, applied_at_ms)?;
        if planned.decision == PlannerDecision::MaterializeRemoteDir {
            let target_path = view
                .path_index
                .resolve_target_inode_relative_path(
                    ancestor_inode_id,
                    remote_parent.parent_inode_id,
                    &remote_parent.display_name,
                )
                .map(|path| join_under_mirror_root(mirror_root, &path))
                .ok_or_else(|| ProviderTargetedMaterializeError::InodePathUnavailable {
                    namespace_id: namespace_id.as_str().to_owned(),
                    inode_id: ancestor_inode_id.0,
                })?;
            execute_materialize_remote_dir_to_path(
                db,
                store,
                namespace_id,
                ancestor_inode_id,
                &target_path,
                applied_at_ms,
            )?;
            applied_at_ms = applied_at_ms.saturating_add(1);
            continue;
        }
        if planned.decision == PlannerDecision::NoOp
            && local_by_inode
                .get(&ancestor_inode_id)
                .is_some_and(|local| local.exists_on_disk)
        {
            continue;
        }

        return Err(ProviderTargetedMaterializeError::InodeUnavailable {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: ancestor_inode_id.0,
            decision: planned.decision.as_str().to_owned(),
            reason: planned.reason.as_str().to_owned(),
        });
    }

    Ok(())
}

fn ensure_inode_runnable_for_provider(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    decision: &PlannerDecision,
    reason: &crate::planner::PlannerReason,
) -> Result<(), ProviderTargetedMaterializeError> {
    match decision {
        PlannerDecision::DownloadRemoteEdit | PlannerDecision::MaterializeRemoteDir => Ok(()),
        other => Err(ProviderTargetedMaterializeError::InodeUnavailable {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            decision: other.as_str().to_owned(),
            reason: reason.as_str().to_owned(),
        }),
    }
}
