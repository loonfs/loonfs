use crate::state_db::{
    ClientFileId, FileSyncViews, HierarchyParentMaterializationAssessment, LocalOnlyFileStateRow,
    LocalOnlyPlannedActionRow, PlannedActionRow, RemoteSubtreeDeleteAssessment,
    RemoteSubtreeRenameAssessment, SqliteStateDb, StateDbError, SyncAnchorRow, TransferDirection,
};
use loon_types::{InodeId, InodeKind, NamespaceId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlannerDecision {
    CreateRemoteDir,
    UploadLocalCreate,
    UploadLocalEdit,
    DownloadRemoteEdit,
    ResolveSameInodeConflict,
    ResolveDeleteVsEditConflict,
    ResolveRenameVsEditConflict,
    ResolvePathBindingCollision,
    ResolveSubtreeDeleteConflict,
    ResolveSubtreeRenameConflict,
    ApplyRemoteRenameAndReplace,
    ApplyRemoteDelete,
    ApplyRemoteSubtreeDelete,
    ApplyRemoteSubtreeRename,
    ApplyRemoteRename,
    MaterializeRemoteDir,
    CreateConflictCopy,
    NoOp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlannerReason {
    AlreadyConverged,
    NoObservedState,
    LocalOnlyDirectoryWithoutRemoteIdentity,
    LocalOnlyFileWithoutRemoteIdentity,
    LocalDiffersFromAnchor,
    RemoteDiffersFromAnchor,
    RemoteDeletedFromAnchor,
    RemoteDeletedWhileLocalDiffersFromAnchor,
    RemoteDeletedWithoutAnchor,
    RemoteSubtreeDeletedFromAnchor,
    RemoteSubtreeDeletedWhileLocalDiffersFromAnchor,
    RemoteSubtreeDeletedWhileDescendantsDifferFromAnchor,
    RemoteSubtreeDeletedWhileDescendantsBusy,
    RemoteSubtreeDeletedWithoutAnchor,
    RemoteSubtreePathDiffersFromAnchor,
    RemoteSubtreePathDiffersWhileLocalDiffersFromAnchor,
    RemoteSubtreePathDiffersWhileDescendantsDifferFromAnchor,
    RemoteSubtreePathDiffersWhileDescendantsBusy,
    RemoteSubtreePathWaitingForTargetParentMaterialization,
    RemoteSubtreePathTargetParentUnusable,
    RemoteParentWaitingForMaterialization,
    RemotePathDiffersFromAnchor,
    RemotePathWaitingForTargetParentMaterialization,
    RemotePathTargetParentUnusable,
    RemotePathAndContentDifferFromAnchor,
    LocalOnlyPathBindingCollision,
    LocalAndRemoteDifferFromAnchor,
    LocalObservedWithoutAnchor,
    RemoteObservedWithoutAnchor,
    LocalAndRemoteObservedWithoutAnchor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedActionRecord {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub decision: PlannerDecision,
    pub reason: PlannerReason,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedLocalOnlyActionRecord {
    pub client_file_id: ClientFileId,
    pub namespace_id: NamespaceId,
    pub decision: PlannerDecision,
    pub reason: PlannerReason,
    pub created_at_ms: u64,
}

#[derive(Debug, Error)]
pub enum PlannerError {
    #[error("state DB error: {0}")]
    StateDb(#[from] StateDbError),
    #[error("unknown planner decision `{0}` in SQLite row")]
    UnknownDecision(String),
    #[error("unknown planner reason `{0}` in SQLite row")]
    UnknownReason(String),
}

pub fn plan_file(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    now_ms: u64,
) -> Result<PlannedActionRecord, PlannerError> {
    db.planner_transaction("plan_file", |tx| {
        plan_file_in_tx(tx, namespace_id, inode_id, now_ms)
    })
    .map_err(PlannerError::from)
}

pub(crate) fn plan_file_in_tx(
    tx: &mut crate::state_db::PlannerTxn<'_>,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    now_ms: u64,
) -> Result<PlannedActionRecord, StateDbError> {
    let views = tx.load_file_sync_views(namespace_id, inode_id)?;
    let mut action = if let Some(action) =
        decide_remote_subtree_delete_action(tx, namespace_id, inode_id, &views, now_ms)?
    {
        action
    } else if let Some(action) =
        decide_remote_subtree_rename_action(tx, namespace_id, inode_id, &views, now_ms)?
    {
        action
    } else if let Some(action) =
        decide_path_binding_collision_action(tx, namespace_id, inode_id, &views, now_ms)?
    {
        action
    } else if let Some(action) =
        decide_remote_only_materialization_wait_action(tx, namespace_id, inode_id, &views, now_ms)?
    {
        action
    } else if let Some(action) =
        decide_remote_file_rename_action(tx, namespace_id, inode_id, &views, now_ms)?
    {
        action
    } else {
        decide_file_action(&views, now_ms)
    };
    if let Some(preserved) =
        preserve_in_flight_file_action(tx, namespace_id, inode_id, &views, now_ms)?
    {
        action = preserved;
    }

    if action.decision == PlannerDecision::NoOp {
        tx.delete_planned_action(namespace_id, inode_id)?;
    } else {
        tx.upsert_planned_action(&action.to_row())?;
    }

    Ok(action)
}

fn decide_path_binding_collision_action(
    tx: &mut crate::state_db::PlannerTxn<'_>,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    views: &FileSyncViews,
    now_ms: u64,
) -> Result<Option<PlannedActionRecord>, StateDbError> {
    let Some(remote) = views.remote.as_ref() else {
        return Ok(None);
    };
    if views.sync_anchor.is_some() || remote.is_deleted || remote.inode_kind != InodeKind::File {
        return Ok(None);
    }
    if let Some(local) = views.local.as_ref() {
        if !remote_only_placeholder_matches_remote(local, remote) {
            return Ok(None);
        }
    }
    if !matches!(views.local.as_ref(), Some(_)) {
        return Ok(None);
    }

    let collisions = tx
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
        return Ok(None);
    };
    tx.delete_planned_local_only_action(&collision.client_file_id)?;
    Ok(Some(PlannedActionRecord {
        namespace_id: namespace_id.clone(),
        inode_id,
        decision: PlannerDecision::ResolvePathBindingCollision,
        reason: PlannerReason::LocalOnlyPathBindingCollision,
        created_at_ms: now_ms,
    }))
}

pub fn plan_local_only_file(
    db: &mut SqliteStateDb,
    client_file_id: &ClientFileId,
    now_ms: u64,
) -> Result<PlannedLocalOnlyActionRecord, PlannerError> {
    plan_local_only_inode(db, client_file_id, now_ms)
}

pub fn plan_local_only_inode(
    db: &mut SqliteStateDb,
    client_file_id: &ClientFileId,
    now_ms: u64,
) -> Result<PlannedLocalOnlyActionRecord, PlannerError> {
    db.planner_transaction("plan_local_only_inode", |tx| {
        let local_only = tx
            .load_local_only_file(client_file_id)?
            .ok_or_else(|| StateDbError::Sqlite(rusqlite::Error::QueryReturnedNoRows))?;
        let action = decide_local_only_inode_action(&local_only, now_ms)?;

        if action.decision == PlannerDecision::NoOp {
            tx.delete_planned_local_only_action(client_file_id)?;
        } else {
            tx.upsert_planned_local_only_action(&action.to_row())?;
        }

        Ok(action)
    })
    .map_err(PlannerError::from)
}

pub fn decide_file_action(views: &FileSyncViews, now_ms: u64) -> PlannedActionRecord {
    let (decision, reason) = match (&views.remote, &views.local, &views.sync_anchor) {
        (Some(remote), local, anchor) if remote.is_deleted => match (local, anchor) {
            (Some(local), Some(anchor))
                if remote.inode_kind == InodeKind::File
                    && local.inode_kind == InodeKind::File
                    && anchor.inode_kind == InodeKind::File
                    && local_matches_anchor(local, anchor) =>
            {
                (
                    PlannerDecision::ApplyRemoteDelete,
                    PlannerReason::RemoteDeletedFromAnchor,
                )
            }
            (_, Some(_)) if remote.inode_kind == InodeKind::File => (
                PlannerDecision::ResolveDeleteVsEditConflict,
                PlannerReason::RemoteDeletedWhileLocalDiffersFromAnchor,
            ),
            _ => (
                PlannerDecision::NoOp,
                PlannerReason::RemoteDeletedWithoutAnchor,
            ),
        },
        (Some(remote), Some(local), Some(anchor)) => {
            let local_matches_anchor = local_matches_anchor(local, anchor);
            let remote_matches_anchor = remote_matches_anchor(remote, anchor);
            let remote_content_matches_anchor = remote_content_matches_anchor(remote, anchor);
            let remote_path_matches_anchor = remote_path_matches_anchor(remote, anchor);

            match (local_matches_anchor, remote_matches_anchor) {
                (true, true) => (PlannerDecision::NoOp, PlannerReason::AlreadyConverged),
                (false, true) => (
                    PlannerDecision::UploadLocalEdit,
                    PlannerReason::LocalDiffersFromAnchor,
                ),
                (true, false)
                    if remote.inode_kind == InodeKind::File
                        && local.inode_kind == InodeKind::File
                        && anchor.inode_kind == InodeKind::File
                        && remote_content_matches_anchor
                        && !remote_path_matches_anchor =>
                {
                    (
                        PlannerDecision::ApplyRemoteRename,
                        PlannerReason::RemotePathDiffersFromAnchor,
                    )
                }
                (true, false) if !remote_content_matches_anchor && !remote_path_matches_anchor => (
                    PlannerDecision::ApplyRemoteRenameAndReplace,
                    PlannerReason::RemotePathAndContentDifferFromAnchor,
                ),
                (true, false) => (
                    PlannerDecision::DownloadRemoteEdit,
                    PlannerReason::RemoteDiffersFromAnchor,
                ),
                (false, false)
                    if remote_path_matches_anchor && local_path_matches_anchor(local, anchor) =>
                {
                    (
                        PlannerDecision::ResolveSameInodeConflict,
                        PlannerReason::LocalAndRemoteDifferFromAnchor,
                    )
                }
                (false, false) => (
                    PlannerDecision::ResolveRenameVsEditConflict,
                    PlannerReason::LocalAndRemoteDifferFromAnchor,
                ),
            }
        }
        (Some(_), None, Some(anchor)) if anchor.content_digest.is_some() => (
            PlannerDecision::DownloadRemoteEdit,
            PlannerReason::RemoteDiffersFromAnchor,
        ),
        (None, Some(_), Some(anchor)) if anchor.content_digest.is_some() => (
            PlannerDecision::UploadLocalEdit,
            PlannerReason::LocalDiffersFromAnchor,
        ),
        (Some(remote), None, None) => (
            match remote.inode_kind {
                InodeKind::File => PlannerDecision::DownloadRemoteEdit,
                InodeKind::Dir => PlannerDecision::MaterializeRemoteDir,
                InodeKind::Symlink | InodeKind::Mount => PlannerDecision::NoOp,
            },
            PlannerReason::RemoteObservedWithoutAnchor,
        ),
        (Some(remote), Some(local), None)
            if remote_only_placeholder_matches_remote(local, remote) =>
        {
            (
                match remote.inode_kind {
                    InodeKind::File => PlannerDecision::DownloadRemoteEdit,
                    InodeKind::Dir => PlannerDecision::MaterializeRemoteDir,
                    InodeKind::Symlink | InodeKind::Mount => PlannerDecision::NoOp,
                },
                PlannerReason::RemoteObservedWithoutAnchor,
            )
        }
        (None, Some(_), None) => (
            PlannerDecision::UploadLocalEdit,
            PlannerReason::LocalObservedWithoutAnchor,
        ),
        (Some(remote), Some(local), None) => {
            if local.exists_on_disk
                && !local.dirty
                && local.inode_kind == remote.inode_kind
                && local.content_digest == remote.content_digest
                && local.parent_inode_id == remote.parent_inode_id
                && local.display_name == remote.display_name
                && !remote.is_deleted
            {
                (PlannerDecision::NoOp, PlannerReason::AlreadyConverged)
            } else {
                (
                    PlannerDecision::CreateConflictCopy,
                    PlannerReason::LocalAndRemoteObservedWithoutAnchor,
                )
            }
        }
        _ => (PlannerDecision::NoOp, PlannerReason::NoObservedState),
    };

    PlannedActionRecord {
        namespace_id: views.namespace_id.clone(),
        inode_id: views.inode_id,
        decision,
        reason,
        created_at_ms: now_ms,
    }
}

pub fn decide_local_only_file_action(
    local_only: &LocalOnlyFileStateRow,
    now_ms: u64,
) -> Result<PlannedLocalOnlyActionRecord, StateDbError> {
    decide_local_only_inode_action(local_only, now_ms)
}

pub fn decide_local_only_inode_action(
    local_only: &LocalOnlyFileStateRow,
    now_ms: u64,
) -> Result<PlannedLocalOnlyActionRecord, StateDbError> {
    let (decision, reason) = if !local_only.exists_on_disk {
        (PlannerDecision::NoOp, PlannerReason::NoObservedState)
    } else {
        match local_only.inode_kind {
            InodeKind::File => {
                if local_only.dirty {
                    (
                        PlannerDecision::UploadLocalCreate,
                        PlannerReason::LocalOnlyFileWithoutRemoteIdentity,
                    )
                } else {
                    (PlannerDecision::NoOp, PlannerReason::NoObservedState)
                }
            }
            InodeKind::Dir => (
                PlannerDecision::CreateRemoteDir,
                PlannerReason::LocalOnlyDirectoryWithoutRemoteIdentity,
            ),
            InodeKind::Symlink | InodeKind::Mount => {
                return Err(StateDbError::UnsupportedLocalOnlyInodeKind(
                    local_only.inode_kind.clone(),
                ))
            }
        }
    };

    Ok(PlannedLocalOnlyActionRecord {
        client_file_id: local_only.client_file_id.clone(),
        namespace_id: local_only.namespace_id.clone(),
        decision,
        reason,
        created_at_ms: now_ms,
    })
}

impl PlannedActionRecord {
    pub fn to_row(&self) -> PlannedActionRow {
        PlannedActionRow {
            namespace_id: self.namespace_id.clone(),
            inode_id: self.inode_id,
            decision: self.decision.as_str().to_owned(),
            reason: self.reason.as_str().to_owned(),
            created_at_ms: self.created_at_ms,
        }
    }
}

impl PlannedLocalOnlyActionRecord {
    pub fn to_row(&self) -> LocalOnlyPlannedActionRow {
        LocalOnlyPlannedActionRow {
            client_file_id: self.client_file_id.clone(),
            namespace_id: self.namespace_id.clone(),
            decision: self.decision.as_str().to_owned(),
            reason: self.reason.as_str().to_owned(),
            created_at_ms: self.created_at_ms,
        }
    }
}

impl TryFrom<PlannedActionRow> for PlannedActionRecord {
    type Error = PlannerError;

    fn try_from(value: PlannedActionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            namespace_id: value.namespace_id,
            inode_id: value.inode_id,
            decision: PlannerDecision::from_str(&value.decision)?,
            reason: PlannerReason::from_str(&value.reason)?,
            created_at_ms: value.created_at_ms,
        })
    }
}

impl TryFrom<LocalOnlyPlannedActionRow> for PlannedLocalOnlyActionRecord {
    type Error = PlannerError;

    fn try_from(value: LocalOnlyPlannedActionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            client_file_id: value.client_file_id,
            namespace_id: value.namespace_id,
            decision: PlannerDecision::from_str(&value.decision)?,
            reason: PlannerReason::from_str(&value.reason)?,
            created_at_ms: value.created_at_ms,
        })
    }
}

impl PlannerDecision {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CreateRemoteDir => "create_remote_dir",
            Self::UploadLocalCreate => "upload_local_create",
            Self::UploadLocalEdit => "upload_local_edit",
            Self::DownloadRemoteEdit => "download_remote_edit",
            Self::ResolveSameInodeConflict => "resolve_same_inode_conflict",
            Self::ResolveDeleteVsEditConflict => "resolve_delete_vs_edit_conflict",
            Self::ResolveRenameVsEditConflict => "resolve_rename_vs_edit_conflict",
            Self::ResolvePathBindingCollision => "resolve_path_binding_collision",
            Self::ResolveSubtreeDeleteConflict => "resolve_subtree_delete_conflict",
            Self::ResolveSubtreeRenameConflict => "resolve_subtree_rename_conflict",
            Self::ApplyRemoteRenameAndReplace => "apply_remote_rename_and_replace",
            Self::ApplyRemoteDelete => "apply_remote_delete",
            Self::ApplyRemoteSubtreeDelete => "apply_remote_subtree_delete",
            Self::ApplyRemoteSubtreeRename => "apply_remote_subtree_rename",
            Self::ApplyRemoteRename => "apply_remote_rename",
            Self::MaterializeRemoteDir => "materialize_remote_dir",
            Self::CreateConflictCopy => "create_conflict_copy",
            Self::NoOp => "no_op",
        }
    }

    fn from_str(value: &str) -> Result<Self, PlannerError> {
        match value {
            "create_remote_dir" => Ok(Self::CreateRemoteDir),
            "upload_local_create" => Ok(Self::UploadLocalCreate),
            "upload_local_edit" => Ok(Self::UploadLocalEdit),
            "download_remote_edit" => Ok(Self::DownloadRemoteEdit),
            "resolve_same_inode_conflict" => Ok(Self::ResolveSameInodeConflict),
            "resolve_delete_vs_edit_conflict" => Ok(Self::ResolveDeleteVsEditConflict),
            "resolve_rename_vs_edit_conflict" => Ok(Self::ResolveRenameVsEditConflict),
            "resolve_path_binding_collision" => Ok(Self::ResolvePathBindingCollision),
            "resolve_subtree_delete_conflict" => Ok(Self::ResolveSubtreeDeleteConflict),
            "resolve_subtree_rename_conflict" => Ok(Self::ResolveSubtreeRenameConflict),
            "apply_remote_rename_and_replace" => Ok(Self::ApplyRemoteRenameAndReplace),
            "apply_remote_delete" => Ok(Self::ApplyRemoteDelete),
            "apply_remote_subtree_delete" => Ok(Self::ApplyRemoteSubtreeDelete),
            "apply_remote_subtree_rename" => Ok(Self::ApplyRemoteSubtreeRename),
            "apply_remote_rename" => Ok(Self::ApplyRemoteRename),
            "materialize_remote_dir" => Ok(Self::MaterializeRemoteDir),
            "create_conflict_copy" => Ok(Self::CreateConflictCopy),
            "no_op" => Ok(Self::NoOp),
            other => Err(PlannerError::UnknownDecision(other.to_owned())),
        }
    }
}

impl PlannerReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AlreadyConverged => "already_converged",
            Self::NoObservedState => "no_observed_state",
            Self::LocalOnlyDirectoryWithoutRemoteIdentity => {
                "local_only_directory_without_remote_identity"
            }
            Self::LocalOnlyFileWithoutRemoteIdentity => "local_only_file_without_remote_identity",
            Self::LocalDiffersFromAnchor => "local_differs_from_anchor",
            Self::RemoteDiffersFromAnchor => "remote_differs_from_anchor",
            Self::RemoteDeletedFromAnchor => "remote_deleted_from_anchor",
            Self::RemoteDeletedWhileLocalDiffersFromAnchor => {
                "remote_deleted_while_local_differs_from_anchor"
            }
            Self::RemoteDeletedWithoutAnchor => "remote_deleted_without_anchor",
            Self::RemoteSubtreeDeletedFromAnchor => "remote_subtree_deleted_from_anchor",
            Self::RemoteSubtreeDeletedWhileLocalDiffersFromAnchor => {
                "remote_subtree_deleted_while_local_differs_from_anchor"
            }
            Self::RemoteSubtreeDeletedWhileDescendantsDifferFromAnchor => {
                "remote_subtree_deleted_while_descendants_differ_from_anchor"
            }
            Self::RemoteSubtreeDeletedWhileDescendantsBusy => {
                "remote_subtree_deleted_while_descendants_busy"
            }
            Self::RemoteSubtreeDeletedWithoutAnchor => "remote_subtree_deleted_without_anchor",
            Self::RemoteSubtreePathDiffersFromAnchor => "remote_subtree_path_differs_from_anchor",
            Self::RemoteSubtreePathDiffersWhileLocalDiffersFromAnchor => {
                "remote_subtree_path_differs_while_local_differs_from_anchor"
            }
            Self::RemoteSubtreePathDiffersWhileDescendantsDifferFromAnchor => {
                "remote_subtree_path_differs_while_descendants_differ_from_anchor"
            }
            Self::RemoteSubtreePathDiffersWhileDescendantsBusy => {
                "remote_subtree_path_differs_while_descendants_busy"
            }
            Self::RemoteSubtreePathWaitingForTargetParentMaterialization => {
                "remote_subtree_path_waiting_for_target_parent_materialization"
            }
            Self::RemoteSubtreePathTargetParentUnusable => {
                "remote_subtree_path_target_parent_unusable"
            }
            Self::RemoteParentWaitingForMaterialization => {
                "remote_parent_waiting_for_materialization"
            }
            Self::RemotePathDiffersFromAnchor => "remote_path_differs_from_anchor",
            Self::RemotePathWaitingForTargetParentMaterialization => {
                "remote_path_waiting_for_target_parent_materialization"
            }
            Self::RemotePathTargetParentUnusable => "remote_path_target_parent_unusable",
            Self::RemotePathAndContentDifferFromAnchor => {
                "remote_path_and_content_differ_from_anchor"
            }
            Self::LocalOnlyPathBindingCollision => "local_only_path_binding_collision",
            Self::LocalAndRemoteDifferFromAnchor => "local_and_remote_differ_from_anchor",
            Self::LocalObservedWithoutAnchor => "local_observed_without_anchor",
            Self::RemoteObservedWithoutAnchor => "remote_observed_without_anchor",
            Self::LocalAndRemoteObservedWithoutAnchor => "local_and_remote_observed_without_anchor",
        }
    }

    fn from_str(value: &str) -> Result<Self, PlannerError> {
        match value {
            "already_converged" => Ok(Self::AlreadyConverged),
            "no_observed_state" => Ok(Self::NoObservedState),
            "local_only_directory_without_remote_identity" => {
                Ok(Self::LocalOnlyDirectoryWithoutRemoteIdentity)
            }
            "local_only_file_without_remote_identity" => {
                Ok(Self::LocalOnlyFileWithoutRemoteIdentity)
            }
            "local_differs_from_anchor" => Ok(Self::LocalDiffersFromAnchor),
            "remote_differs_from_anchor" => Ok(Self::RemoteDiffersFromAnchor),
            "remote_deleted_from_anchor" => Ok(Self::RemoteDeletedFromAnchor),
            "remote_deleted_while_local_differs_from_anchor" => {
                Ok(Self::RemoteDeletedWhileLocalDiffersFromAnchor)
            }
            "remote_deleted_without_anchor" => Ok(Self::RemoteDeletedWithoutAnchor),
            "remote_subtree_deleted_from_anchor" => Ok(Self::RemoteSubtreeDeletedFromAnchor),
            "remote_subtree_deleted_while_local_differs_from_anchor" => {
                Ok(Self::RemoteSubtreeDeletedWhileLocalDiffersFromAnchor)
            }
            "remote_subtree_deleted_while_descendants_differ_from_anchor" => {
                Ok(Self::RemoteSubtreeDeletedWhileDescendantsDifferFromAnchor)
            }
            "remote_subtree_deleted_while_descendants_busy" => {
                Ok(Self::RemoteSubtreeDeletedWhileDescendantsBusy)
            }
            "remote_subtree_deleted_without_anchor" => Ok(Self::RemoteSubtreeDeletedWithoutAnchor),
            "remote_subtree_path_differs_from_anchor" => {
                Ok(Self::RemoteSubtreePathDiffersFromAnchor)
            }
            "remote_subtree_path_differs_while_local_differs_from_anchor" => {
                Ok(Self::RemoteSubtreePathDiffersWhileLocalDiffersFromAnchor)
            }
            "remote_subtree_path_differs_while_descendants_differ_from_anchor" => {
                Ok(Self::RemoteSubtreePathDiffersWhileDescendantsDifferFromAnchor)
            }
            "remote_subtree_path_differs_while_descendants_busy" => {
                Ok(Self::RemoteSubtreePathDiffersWhileDescendantsBusy)
            }
            "remote_subtree_path_waiting_for_target_parent_materialization" => {
                Ok(Self::RemoteSubtreePathWaitingForTargetParentMaterialization)
            }
            "remote_subtree_path_target_parent_unusable" => {
                Ok(Self::RemoteSubtreePathTargetParentUnusable)
            }
            "remote_parent_waiting_for_materialization" => {
                Ok(Self::RemoteParentWaitingForMaterialization)
            }
            "remote_path_differs_from_anchor" => Ok(Self::RemotePathDiffersFromAnchor),
            "remote_path_waiting_for_target_parent_materialization" => {
                Ok(Self::RemotePathWaitingForTargetParentMaterialization)
            }
            "remote_path_target_parent_unusable" => Ok(Self::RemotePathTargetParentUnusable),
            "remote_path_and_content_differ_from_anchor" => {
                Ok(Self::RemotePathAndContentDifferFromAnchor)
            }
            "local_only_path_binding_collision" => Ok(Self::LocalOnlyPathBindingCollision),
            "local_and_remote_differ_from_anchor" => Ok(Self::LocalAndRemoteDifferFromAnchor),
            "local_observed_without_anchor" => Ok(Self::LocalObservedWithoutAnchor),
            "remote_observed_without_anchor" => Ok(Self::RemoteObservedWithoutAnchor),
            "local_and_remote_observed_without_anchor" => {
                Ok(Self::LocalAndRemoteObservedWithoutAnchor)
            }
            other => Err(PlannerError::UnknownReason(other.to_owned())),
        }
    }
}

fn remote_matches_anchor(
    remote: &crate::state_db::RemoteFileStateRow,
    anchor: &SyncAnchorRow,
) -> bool {
    remote_content_matches_anchor(remote, anchor) && remote_path_matches_anchor(remote, anchor)
}

fn remote_content_matches_anchor(
    remote: &crate::state_db::RemoteFileStateRow,
    anchor: &SyncAnchorRow,
) -> bool {
    !remote.is_deleted
        && remote.inode_kind == anchor.inode_kind
        && remote.revision_no == anchor.revision_no
        && remote.content_digest == anchor.content_digest
        && remote.content_manifest_digest == anchor.content_manifest_digest
}

fn remote_path_matches_anchor(
    remote: &crate::state_db::RemoteFileStateRow,
    anchor: &SyncAnchorRow,
) -> bool {
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
    remote: &crate::state_db::RemoteFileStateRow,
) -> bool {
    !local.exists_on_disk
        && !local.dirty
        && !remote.is_deleted
        && local.inode_kind == remote.inode_kind
        && local.parent_inode_id == remote.parent_inode_id
        && local.display_name == remote.display_name
}

fn decide_remote_only_materialization_wait_action(
    tx: &mut crate::state_db::PlannerTxn<'_>,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    views: &FileSyncViews,
    now_ms: u64,
) -> Result<Option<PlannedActionRecord>, StateDbError> {
    let (Some(remote), Some(local), None) = (
        views.remote.as_ref(),
        views.local.as_ref(),
        views.sync_anchor.as_ref(),
    ) else {
        return Ok(None);
    };
    if !matches!(remote.inode_kind, InodeKind::File | InodeKind::Dir)
        || !remote_only_placeholder_matches_remote(local, remote)
    {
        return Ok(None);
    }
    let Some(parent_inode_id) = remote.parent_inode_id else {
        return Ok(None);
    };

    let parent_assessment =
        tx.assess_hierarchy_parent_materialization(namespace_id, parent_inode_id)?;
    if parent_assessment == HierarchyParentMaterializationAssessment::Usable {
        return Ok(None);
    }

    Ok(Some(PlannedActionRecord {
        namespace_id: namespace_id.clone(),
        inode_id,
        decision: PlannerDecision::NoOp,
        reason: PlannerReason::RemoteParentWaitingForMaterialization,
        created_at_ms: now_ms,
    }))
}

fn decide_remote_file_rename_action(
    tx: &mut crate::state_db::PlannerTxn<'_>,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    views: &FileSyncViews,
    now_ms: u64,
) -> Result<Option<PlannedActionRecord>, StateDbError> {
    let (Some(remote), Some(local), Some(anchor)) = (
        views.remote.as_ref(),
        views.local.as_ref(),
        views.sync_anchor.as_ref(),
    ) else {
        return Ok(None);
    };
    if remote.inode_kind != InodeKind::File
        || local.inode_kind != InodeKind::File
        || anchor.inode_kind != InodeKind::File
        || !local_matches_anchor(local, anchor)
        || !remote_content_matches_anchor(remote, anchor)
        || remote_path_matches_anchor(remote, anchor)
    {
        return Ok(None);
    }

    let parent_assessment = if remote.parent_inode_id == anchor.parent_inode_id {
        HierarchyParentMaterializationAssessment::Usable
    } else {
        match remote.parent_inode_id {
            Some(target_parent_inode_id) => {
                tx.assess_hierarchy_parent_materialization(namespace_id, target_parent_inode_id)?
            }
            None => HierarchyParentMaterializationAssessment::Usable,
        }
    };
    let (decision, reason) = match parent_assessment {
        HierarchyParentMaterializationAssessment::Usable => (
            PlannerDecision::ApplyRemoteRename,
            PlannerReason::RemotePathDiffersFromAnchor,
        ),
        HierarchyParentMaterializationAssessment::WaitingForMaterialization => (
            PlannerDecision::NoOp,
            PlannerReason::RemotePathWaitingForTargetParentMaterialization,
        ),
        HierarchyParentMaterializationAssessment::Unusable => (
            PlannerDecision::CreateConflictCopy,
            PlannerReason::RemotePathTargetParentUnusable,
        ),
    };

    Ok(Some(PlannedActionRecord {
        namespace_id: namespace_id.clone(),
        inode_id,
        decision,
        reason,
        created_at_ms: now_ms,
    }))
}

fn preserve_in_flight_file_action(
    tx: &mut crate::state_db::PlannerTxn<'_>,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    views: &FileSyncViews,
    now_ms: u64,
) -> Result<Option<PlannedActionRecord>, StateDbError> {
    let Some(remote) = views.remote.as_ref() else {
        return Ok(None);
    };
    if remote.inode_kind != InodeKind::File || views.sync_anchor.is_none() {
        return Ok(None);
    }

    let has_active_download = tx
        .load_transfer_ledger_for_inode(namespace_id, inode_id, TransferDirection::Download)?
        .is_some();
    let has_active_upload = tx
        .load_transfer_ledger_for_inode(namespace_id, inode_id, TransferDirection::Upload)?
        .is_some();
    let has_pending_inode_mutation = tx
        .load_pending_inode_mutation_for_inode(namespace_id, inode_id)?
        .is_some();
    if !(has_active_download || has_active_upload || has_pending_inode_mutation) {
        return Ok(None);
    }

    let existing_planned_action = tx.load_planned_action(namespace_id, inode_id)?;
    if let Some(planned_row) = existing_planned_action.as_ref() {
        match planned_row.decision.as_str() {
            "upload_local_edit" => {
                return Ok(Some(PlannedActionRecord {
                    namespace_id: planned_row.namespace_id.clone(),
                    inode_id: planned_row.inode_id,
                    decision: PlannerDecision::UploadLocalEdit,
                    reason: PlannerReason::LocalDiffersFromAnchor,
                    created_at_ms: planned_row.created_at_ms,
                }));
            }
            "download_remote_edit" => {
                return Ok(Some(PlannedActionRecord {
                    namespace_id: planned_row.namespace_id.clone(),
                    inode_id: planned_row.inode_id,
                    decision: PlannerDecision::DownloadRemoteEdit,
                    reason: PlannerReason::RemoteDiffersFromAnchor,
                    created_at_ms: planned_row.created_at_ms,
                }));
            }
            _ => {}
        }
    }

    let (decision, reason) = if has_active_download {
        (
            PlannerDecision::DownloadRemoteEdit,
            PlannerReason::RemoteDiffersFromAnchor,
        )
    } else {
        (
            PlannerDecision::UploadLocalEdit,
            PlannerReason::LocalDiffersFromAnchor,
        )
    };

    Ok(Some(PlannedActionRecord {
        namespace_id: namespace_id.clone(),
        inode_id,
        decision,
        reason,
        created_at_ms: existing_planned_action
            .map(|planned| planned.created_at_ms)
            .unwrap_or(now_ms),
    }))
}

fn decide_remote_subtree_delete_action(
    tx: &mut crate::state_db::PlannerTxn<'_>,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    views: &FileSyncViews,
    now_ms: u64,
) -> Result<Option<PlannedActionRecord>, StateDbError> {
    let Some(remote) = views.remote.as_ref() else {
        return Ok(None);
    };
    if remote.inode_kind != InodeKind::Dir || !remote.is_deleted {
        return Ok(None);
    }

    let (decision, reason) = match tx.assess_remote_subtree_delete(namespace_id, inode_id)? {
        RemoteSubtreeDeleteAssessment::NotApplicable => return Ok(None),
        RemoteSubtreeDeleteAssessment::Ready(_) => (
            PlannerDecision::ApplyRemoteSubtreeDelete,
            PlannerReason::RemoteSubtreeDeletedFromAnchor,
        ),
        RemoteSubtreeDeleteAssessment::DeferredRootLocalDiffers => (
            PlannerDecision::ResolveSubtreeDeleteConflict,
            PlannerReason::RemoteSubtreeDeletedWhileLocalDiffersFromAnchor,
        ),
        RemoteSubtreeDeleteAssessment::DeferredDescendantsDiffer => (
            PlannerDecision::ResolveSubtreeDeleteConflict,
            PlannerReason::RemoteSubtreeDeletedWhileDescendantsDifferFromAnchor,
        ),
        RemoteSubtreeDeleteAssessment::DeferredDescendantsBusy => (
            PlannerDecision::CreateConflictCopy,
            PlannerReason::RemoteSubtreeDeletedWhileDescendantsBusy,
        ),
        RemoteSubtreeDeleteAssessment::InertWithoutAnchor => (
            PlannerDecision::NoOp,
            PlannerReason::RemoteSubtreeDeletedWithoutAnchor,
        ),
    };

    Ok(Some(PlannedActionRecord {
        namespace_id: namespace_id.clone(),
        inode_id,
        decision,
        reason,
        created_at_ms: now_ms,
    }))
}

fn decide_remote_subtree_rename_action(
    tx: &mut crate::state_db::PlannerTxn<'_>,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    views: &FileSyncViews,
    now_ms: u64,
) -> Result<Option<PlannedActionRecord>, StateDbError> {
    let Some(remote) = views.remote.as_ref() else {
        return Ok(None);
    };
    if remote.inode_kind != InodeKind::Dir || remote.is_deleted {
        return Ok(None);
    }

    let (decision, reason) = match tx.assess_remote_subtree_rename(namespace_id, inode_id)? {
        RemoteSubtreeRenameAssessment::NotApplicable => return Ok(None),
        RemoteSubtreeRenameAssessment::Ready(_) => (
            PlannerDecision::ApplyRemoteSubtreeRename,
            PlannerReason::RemoteSubtreePathDiffersFromAnchor,
        ),
        RemoteSubtreeRenameAssessment::DeferredRootLocalDiffers => (
            PlannerDecision::ResolveSubtreeRenameConflict,
            PlannerReason::RemoteSubtreePathDiffersWhileLocalDiffersFromAnchor,
        ),
        RemoteSubtreeRenameAssessment::DeferredDescendantsDiffer => (
            PlannerDecision::ResolveSubtreeRenameConflict,
            PlannerReason::RemoteSubtreePathDiffersWhileDescendantsDifferFromAnchor,
        ),
        RemoteSubtreeRenameAssessment::DeferredDescendantsBusy => (
            PlannerDecision::CreateConflictCopy,
            PlannerReason::RemoteSubtreePathDiffersWhileDescendantsBusy,
        ),
        RemoteSubtreeRenameAssessment::WaitingForTargetParentMaterialization => (
            PlannerDecision::NoOp,
            PlannerReason::RemoteSubtreePathWaitingForTargetParentMaterialization,
        ),
        RemoteSubtreeRenameAssessment::DeferredTargetParentUnusable => (
            PlannerDecision::CreateConflictCopy,
            PlannerReason::RemoteSubtreePathTargetParentUnusable,
        ),
    };

    Ok(Some(PlannedActionRecord {
        namespace_id: namespace_id.clone(),
        inode_id,
        decision,
        reason,
        created_at_ms: now_ms,
    }))
}

#[cfg(test)]
mod tests {
    use super::{
        decide_file_action, decide_local_only_file_action, plan_file, PlannerDecision,
        PlannerReason,
    };
    use crate::state_db::{
        ClientFileId, FileSyncViews, LocalFileStateRow, LocalOnlyFileStateRow, RemoteFileStateRow,
        SqliteStateDb, SyncAnchorRow, TransferDirection, TransferLedgerRow, TransferState,
    };
    use loon_types::{ChangeSeq, InodeId, InodeKind, NamespaceId, RevisionNo};

    #[test]
    fn planner_marks_same_inode_stale_base_edit_for_explicit_resolution() {
        let views = FileSyncViews {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            remote: Some(RemoteFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(420),
                revision_no: RevisionNo(18),
                content_digest: Some("sha256:remote-18".to_owned()),
                content_manifest_digest: Some("sha256:manifest-remote-18".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "report.txt".to_owned(),
                is_deleted: false,
            }),
            local: Some(LocalFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                content_digest: Some("sha256:local-edit".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "report.txt".to_owned(),
                exists_on_disk: true,
                dirty: true,
                last_local_change_ms: 1_700_000_001_000,
            }),
            sync_anchor: Some(SyncAnchorRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(419),
                revision_no: RevisionNo(17),
                content_digest: Some("sha256:anchor-17".to_owned()),
                content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "report.txt".to_owned(),
            }),
        };

        let planned = decide_file_action(&views, 1_700_000_002_000);

        assert_eq!(planned.decision, PlannerDecision::ResolveSameInodeConflict);
        assert_eq!(
            planned.reason,
            PlannerReason::LocalAndRemoteDifferFromAnchor
        );
    }

    #[test]
    fn planner_selects_apply_remote_rename_for_bound_remote_path_change() {
        let views = FileSyncViews {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            remote: Some(RemoteFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(420),
                revision_no: RevisionNo(17),
                content_digest: Some("sha256:anchor-17".to_owned()),
                content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
                parent_inode_id: Some(InodeId(3)),
                display_name: "report-renamed.txt".to_owned(),
                is_deleted: false,
            }),
            local: Some(LocalFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                content_digest: Some("sha256:anchor-17".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "report.txt".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_001_000,
            }),
            sync_anchor: Some(SyncAnchorRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(419),
                revision_no: RevisionNo(17),
                content_digest: Some("sha256:anchor-17".to_owned()),
                content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "report.txt".to_owned(),
            }),
        };

        let planned = decide_file_action(&views, 1_700_000_002_000);

        assert_eq!(planned.decision, PlannerDecision::ApplyRemoteRename);
        assert_eq!(planned.reason, PlannerReason::RemotePathDiffersFromAnchor);
    }

    #[test]
    fn planner_marks_remote_path_and_content_change_for_direct_apply() {
        let views = FileSyncViews {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            remote: Some(RemoteFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(420),
                revision_no: RevisionNo(18),
                content_digest: Some("sha256:remote-18".to_owned()),
                content_manifest_digest: Some("sha256:manifest-remote-18".to_owned()),
                parent_inode_id: Some(InodeId(3)),
                display_name: "report-renamed.txt".to_owned(),
                is_deleted: false,
            }),
            local: Some(LocalFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                content_digest: Some("sha256:anchor-17".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "report.txt".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_001_000,
            }),
            sync_anchor: Some(SyncAnchorRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(419),
                revision_no: RevisionNo(17),
                content_digest: Some("sha256:anchor-17".to_owned()),
                content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "report.txt".to_owned(),
            }),
        };

        let planned = decide_file_action(&views, 1_700_000_002_000);

        assert_eq!(
            planned.decision,
            PlannerDecision::ApplyRemoteRenameAndReplace
        );
        assert_eq!(
            planned.reason,
            PlannerReason::RemotePathAndContentDifferFromAnchor
        );
    }

    #[test]
    fn planner_marks_bound_tombstoned_file_for_apply_remote_delete() {
        let views = FileSyncViews {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            remote: Some(RemoteFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(420),
                revision_no: RevisionNo(17),
                content_digest: Some("sha256:anchor-17".to_owned()),
                content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "report.txt".to_owned(),
                is_deleted: true,
            }),
            local: Some(LocalFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                content_digest: Some("sha256:anchor-17".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "report.txt".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_001_000,
            }),
            sync_anchor: Some(SyncAnchorRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(419),
                revision_no: RevisionNo(17),
                content_digest: Some("sha256:anchor-17".to_owned()),
                content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "report.txt".to_owned(),
            }),
        };

        let planned = decide_file_action(&views, 1_700_000_002_000);

        assert_eq!(planned.decision, PlannerDecision::ApplyRemoteDelete);
        assert_eq!(planned.reason, PlannerReason::RemoteDeletedFromAnchor);
    }

    #[test]
    fn planner_marks_remote_delete_vs_local_edit_for_explicit_resolution() {
        let views = FileSyncViews {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            remote: Some(RemoteFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(420),
                revision_no: RevisionNo(17),
                content_digest: Some("sha256:anchor-17".to_owned()),
                content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "report.txt".to_owned(),
                is_deleted: true,
            }),
            local: Some(LocalFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                content_digest: Some("sha256:diverged".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "report.txt".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_001_000,
            }),
            sync_anchor: Some(SyncAnchorRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(419),
                revision_no: RevisionNo(17),
                content_digest: Some("sha256:anchor-17".to_owned()),
                content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "report.txt".to_owned(),
            }),
        };

        let planned = decide_file_action(&views, 1_700_000_002_000);

        assert_eq!(
            planned.decision,
            PlannerDecision::ResolveDeleteVsEditConflict
        );
        assert_eq!(
            planned.reason,
            PlannerReason::RemoteDeletedWhileLocalDiffersFromAnchor
        );
    }

    #[test]
    fn planner_marks_tombstoned_remote_without_anchor_as_no_op() {
        let views = FileSyncViews {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            remote: Some(RemoteFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(420),
                revision_no: RevisionNo(17),
                content_digest: Some("sha256:anchor-17".to_owned()),
                content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "report.txt".to_owned(),
                is_deleted: true,
            }),
            local: None,
            sync_anchor: None,
        };

        let planned = decide_file_action(&views, 1_700_000_002_000);

        assert_eq!(planned.decision, PlannerDecision::NoOp);
        assert_eq!(planned.reason, PlannerReason::RemoteDeletedWithoutAnchor);
    }

    #[test]
    fn planner_marks_clean_tombstoned_bound_directory_for_apply_remote_subtree_delete() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let namespace_id = NamespaceId::from("ns-1");
        let root_inode_id = InodeId(10);
        let child_inode_id = InodeId(11);

        db.planner_transaction("seed-subtree-delete-plan", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: root_inode_id,
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(420),
                revision_no: RevisionNo(7),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "reports".to_owned(),
                is_deleted: true,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: root_inode_id,
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "reports".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_001_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id: root_inode_id,
                inode_kind: InodeKind::Dir,
                synced_seq: ChangeSeq(419),
                revision_no: RevisionNo(7),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "reports".to_owned(),
            })?;
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: child_inode_id,
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(419),
                revision_no: RevisionNo(3),
                content_digest: Some("sha256:child-3".to_owned()),
                content_manifest_digest: Some("sha256:manifest-child-3".to_owned()),
                parent_inode_id: Some(root_inode_id),
                display_name: "q1.txt".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: child_inode_id,
                inode_kind: InodeKind::File,
                content_digest: Some("sha256:child-3".to_owned()),
                parent_inode_id: Some(root_inode_id),
                display_name: "q1.txt".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_001_010,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id: child_inode_id,
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(419),
                revision_no: RevisionNo(3),
                content_digest: Some("sha256:child-3".to_owned()),
                content_manifest_digest: Some("sha256:manifest-child-3".to_owned()),
                parent_inode_id: Some(root_inode_id),
                display_name: "q1.txt".to_owned(),
            })?;
            Ok(())
        })
        .expect("seed subtree delete state");

        let planned = plan_file(&mut db, &namespace_id, root_inode_id, 1_700_000_002_000)
            .expect("plan subtree delete");

        assert_eq!(planned.decision, PlannerDecision::ApplyRemoteSubtreeDelete);
        assert_eq!(
            planned.reason,
            PlannerReason::RemoteSubtreeDeletedFromAnchor
        );
    }

    #[test]
    fn planner_marks_remote_subtree_delete_descendant_divergence_for_explicit_resolution() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let namespace_id = NamespaceId::from("ns-1");
        let root_inode_id = InodeId(10);
        let child_inode_id = InodeId(11);

        db.planner_transaction("seed-subtree-delete-diverged-child", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: root_inode_id,
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(420),
                revision_no: RevisionNo(7),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "reports".to_owned(),
                is_deleted: true,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: root_inode_id,
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "reports".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_001_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id: root_inode_id,
                inode_kind: InodeKind::Dir,
                synced_seq: ChangeSeq(419),
                revision_no: RevisionNo(7),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "reports".to_owned(),
            })?;
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: child_inode_id,
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(419),
                revision_no: RevisionNo(3),
                content_digest: Some("sha256:child-3".to_owned()),
                content_manifest_digest: Some("sha256:manifest-child-3".to_owned()),
                parent_inode_id: Some(root_inode_id),
                display_name: "q1.txt".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: child_inode_id,
                inode_kind: InodeKind::File,
                content_digest: Some("sha256:diverged-child".to_owned()),
                parent_inode_id: Some(root_inode_id),
                display_name: "q1.txt".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_001_010,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id: child_inode_id,
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(419),
                revision_no: RevisionNo(3),
                content_digest: Some("sha256:child-3".to_owned()),
                content_manifest_digest: Some("sha256:manifest-child-3".to_owned()),
                parent_inode_id: Some(root_inode_id),
                display_name: "q1.txt".to_owned(),
            })?;
            Ok(())
        })
        .expect("seed subtree delete state");

        let planned = plan_file(&mut db, &namespace_id, root_inode_id, 1_700_000_002_000)
            .expect("plan subtree delete conflict");

        assert_eq!(
            planned.decision,
            PlannerDecision::ResolveSubtreeDeleteConflict
        );
        assert_eq!(
            planned.reason,
            PlannerReason::RemoteSubtreeDeletedWhileDescendantsDifferFromAnchor
        );
    }

    #[test]
    fn planner_marks_tombstoned_directory_without_anchor_as_no_op() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let namespace_id = NamespaceId::from("ns-1");
        let root_inode_id = InodeId(10);

        db.planner_transaction("seed-tombstoned-dir-without-anchor", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: root_inode_id,
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(420),
                revision_no: RevisionNo(7),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "reports".to_owned(),
                is_deleted: true,
            })?;
            Ok(())
        })
        .expect("seed tombstoned remote dir");

        let planned = plan_file(&mut db, &namespace_id, root_inode_id, 1_700_000_002_000)
            .expect("plan inert tombstone");

        assert_eq!(planned.decision, PlannerDecision::NoOp);
        assert_eq!(
            planned.reason,
            PlannerReason::RemoteSubtreeDeletedWithoutAnchor
        );
    }

    fn seed_remote_subtree_rename_plan_state(
        db: &mut SqliteStateDb,
        namespace_id: &NamespaceId,
        root_inode_id: InodeId,
        child_inode_id: InodeId,
        root_remote_parent_inode_id: Option<InodeId>,
        root_remote_display_name: &str,
        seed_target_parent: bool,
    ) {
        let current_parent_inode_id = InodeId(2);
        let target_parent_inode_id = InodeId(20);

        db.planner_transaction("seed-subtree-rename-state", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: current_parent_inode_id,
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(42),
                revision_no: RevisionNo(9),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(1)),
                display_name: "workspace".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: current_parent_inode_id,
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: Some(InodeId(1)),
                display_name: "workspace".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_001_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id: current_parent_inode_id,
                inode_kind: InodeKind::Dir,
                synced_seq: ChangeSeq(42),
                revision_no: RevisionNo(9),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(1)),
                display_name: "workspace".to_owned(),
            })?;
            if seed_target_parent {
                tx.upsert_remote_file(&RemoteFileStateRow {
                    namespace_id: namespace_id.clone(),
                    inode_id: target_parent_inode_id,
                    inode_kind: InodeKind::Dir,
                    observed_seq: ChangeSeq(42),
                    revision_no: RevisionNo(10),
                    content_digest: None,
                    content_manifest_digest: None,
                    parent_inode_id: None,
                    display_name: "archive".to_owned(),
                    is_deleted: false,
                })?;
                tx.upsert_local_file(&LocalFileStateRow {
                    namespace_id: namespace_id.clone(),
                    inode_id: target_parent_inode_id,
                    inode_kind: InodeKind::Dir,
                    content_digest: None,
                    parent_inode_id: None,
                    display_name: "archive".to_owned(),
                    exists_on_disk: true,
                    dirty: false,
                    last_local_change_ms: 1_700_000_001_005,
                })?;
                tx.upsert_sync_anchor(&SyncAnchorRow {
                    namespace_id: namespace_id.clone(),
                    inode_id: target_parent_inode_id,
                    inode_kind: InodeKind::Dir,
                    synced_seq: ChangeSeq(42),
                    revision_no: RevisionNo(10),
                    content_digest: None,
                    content_manifest_digest: None,
                    parent_inode_id: None,
                    display_name: "archive".to_owned(),
                })?;
            }
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: root_inode_id,
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(42),
                revision_no: RevisionNo(7),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: root_remote_parent_inode_id,
                display_name: root_remote_display_name.to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: root_inode_id,
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: Some(current_parent_inode_id),
                display_name: "reports".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_001_010,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id: root_inode_id,
                inode_kind: InodeKind::Dir,
                synced_seq: ChangeSeq(41),
                revision_no: RevisionNo(7),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(current_parent_inode_id),
                display_name: "reports".to_owned(),
            })?;
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: child_inode_id,
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(41),
                revision_no: RevisionNo(3),
                content_digest: Some("sha256:child-3".to_owned()),
                content_manifest_digest: Some("sha256:manifest-child-3".to_owned()),
                parent_inode_id: Some(root_inode_id),
                display_name: "q1.txt".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: child_inode_id,
                inode_kind: InodeKind::File,
                content_digest: Some("sha256:child-3".to_owned()),
                parent_inode_id: Some(root_inode_id),
                display_name: "q1.txt".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_001_020,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id: child_inode_id,
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(41),
                revision_no: RevisionNo(3),
                content_digest: Some("sha256:child-3".to_owned()),
                content_manifest_digest: Some("sha256:manifest-child-3".to_owned()),
                parent_inode_id: Some(root_inode_id),
                display_name: "q1.txt".to_owned(),
            })?;
            Ok(())
        })
        .expect("seed subtree rename state");
    }

    #[test]
    fn planner_marks_same_parent_directory_rename_for_apply_remote_subtree_rename() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let namespace_id = NamespaceId::from("ns-1");
        let root_inode_id = InodeId(10);
        let child_inode_id = InodeId(11);

        seed_remote_subtree_rename_plan_state(
            &mut db,
            &namespace_id,
            root_inode_id,
            child_inode_id,
            Some(InodeId(2)),
            "reports-2026",
            false,
        );

        let planned = plan_file(&mut db, &namespace_id, root_inode_id, 1_700_000_002_000)
            .expect("plan subtree rename");

        assert_eq!(planned.decision, PlannerDecision::ApplyRemoteSubtreeRename);
        assert_eq!(
            planned.reason,
            PlannerReason::RemoteSubtreePathDiffersFromAnchor
        );
    }

    #[test]
    fn planner_marks_directory_move_for_apply_remote_subtree_rename() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let namespace_id = NamespaceId::from("ns-1");
        let root_inode_id = InodeId(10);
        let child_inode_id = InodeId(11);

        seed_remote_subtree_rename_plan_state(
            &mut db,
            &namespace_id,
            root_inode_id,
            child_inode_id,
            Some(InodeId(20)),
            "reports",
            true,
        );

        let planned = plan_file(&mut db, &namespace_id, root_inode_id, 1_700_000_002_000)
            .expect("plan subtree move");

        assert_eq!(planned.decision, PlannerDecision::ApplyRemoteSubtreeRename);
        assert_eq!(
            planned.reason,
            PlannerReason::RemoteSubtreePathDiffersFromAnchor
        );
    }

    #[test]
    fn planner_defers_subtree_rename_when_target_parent_is_unusable() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let namespace_id = NamespaceId::from("ns-1");
        let root_inode_id = InodeId(10);
        let child_inode_id = InodeId(11);

        seed_remote_subtree_rename_plan_state(
            &mut db,
            &namespace_id,
            root_inode_id,
            child_inode_id,
            Some(InodeId(20)),
            "reports",
            false,
        );

        let planned = plan_file(&mut db, &namespace_id, root_inode_id, 1_700_000_002_000)
            .expect("plan subtree rename conflict");

        assert_eq!(planned.decision, PlannerDecision::CreateConflictCopy);
        assert_eq!(
            planned.reason,
            PlannerReason::RemoteSubtreePathTargetParentUnusable
        );
    }

    #[test]
    fn planner_marks_subtree_rename_descendant_divergence_for_explicit_resolution() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let namespace_id = NamespaceId::from("ns-1");
        let root_inode_id = InodeId(10);
        let child_inode_id = InodeId(11);

        seed_remote_subtree_rename_plan_state(
            &mut db,
            &namespace_id,
            root_inode_id,
            child_inode_id,
            Some(InodeId(2)),
            "reports-2026",
            false,
        );
        db.planner_transaction("diverge-subtree-child", |tx| {
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: child_inode_id,
                inode_kind: InodeKind::File,
                content_digest: Some("sha256:diverged-child".to_owned()),
                parent_inode_id: Some(root_inode_id),
                display_name: "q1.txt".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_001_030,
            })?;
            Ok(())
        })
        .expect("diverge descendant");

        let planned = plan_file(&mut db, &namespace_id, root_inode_id, 1_700_000_002_000)
            .expect("plan subtree rename descendant conflict");

        assert_eq!(
            planned.decision,
            PlannerDecision::ResolveSubtreeRenameConflict
        );
        assert_eq!(
            planned.reason,
            PlannerReason::RemoteSubtreePathDiffersWhileDescendantsDifferFromAnchor
        );
    }

    #[test]
    fn planner_defers_subtree_rename_when_descendant_is_busy() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let namespace_id = NamespaceId::from("ns-1");
        let root_inode_id = InodeId(10);
        let child_inode_id = InodeId(11);

        seed_remote_subtree_rename_plan_state(
            &mut db,
            &namespace_id,
            root_inode_id,
            child_inode_id,
            Some(InodeId(2)),
            "reports-2026",
            false,
        );
        db.upsert_transfer_ledger(&TransferLedgerRow {
            namespace_id: namespace_id.clone(),
            inode_id: child_inode_id,
            transfer_id: "upload:ns-1:11:sha256:manifest-child-3".to_owned(),
            direction: TransferDirection::Upload,
            object_key: "namespaces/ns-1/manifests/sha256:manifest-child-3.json".to_owned(),
            block_index: 1,
            block_count: 2,
            state: TransferState::Uploading,
            updated_at_ms: 1_700_000_001_040,
        })
        .expect("seed descendant transfer");

        let planned = plan_file(&mut db, &namespace_id, root_inode_id, 1_700_000_002_000)
            .expect("plan subtree rename busy conflict");

        assert_eq!(planned.decision, PlannerDecision::CreateConflictCopy);
        assert_eq!(
            planned.reason,
            PlannerReason::RemoteSubtreePathDiffersWhileDescendantsBusy
        );
    }

    #[test]
    fn planner_marks_subtree_rename_root_divergence_for_explicit_resolution() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let namespace_id = NamespaceId::from("ns-1");
        let root_inode_id = InodeId(10);
        let child_inode_id = InodeId(11);

        seed_remote_subtree_rename_plan_state(
            &mut db,
            &namespace_id,
            root_inode_id,
            child_inode_id,
            Some(InodeId(2)),
            "reports-2026",
            false,
        );
        db.planner_transaction("diverge-subtree-root", |tx| {
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: root_inode_id,
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "reports-local".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_001_050,
            })?;
            Ok(())
        })
        .expect("diverge root local path");

        let planned = plan_file(&mut db, &namespace_id, root_inode_id, 1_700_000_002_000)
            .expect("plan subtree rename root conflict");

        assert_eq!(
            planned.decision,
            PlannerDecision::ResolveSubtreeRenameConflict
        );
        assert_eq!(
            planned.reason,
            PlannerReason::RemoteSubtreePathDiffersWhileLocalDiffersFromAnchor
        );
    }

    #[test]
    fn planner_materializes_remote_only_file_without_anchor() {
        let views = FileSyncViews {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(601),
            remote: Some(RemoteFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(601),
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(42),
                revision_no: RevisionNo(1),
                content_digest: Some(
                    "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                        .to_owned(),
                ),
                content_manifest_digest: Some(
                    "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                        .to_owned(),
                ),
                parent_inode_id: Some(InodeId(2)),
                display_name: "welcome.txt".to_owned(),
                is_deleted: false,
            }),
            local: Some(LocalFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(601),
                inode_kind: InodeKind::File,
                content_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "welcome.txt".to_owned(),
                exists_on_disk: false,
                dirty: false,
                last_local_change_ms: 1_700_000_608_000,
            }),
            sync_anchor: None,
        };

        let planned = decide_file_action(&views, 1_700_000_610_000);

        assert_eq!(planned.decision, PlannerDecision::DownloadRemoteEdit);
        assert_eq!(planned.reason, PlannerReason::RemoteObservedWithoutAnchor);
    }

    #[test]
    fn planner_materializes_remote_only_directory_without_anchor() {
        let views = FileSyncViews {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(701),
            remote: Some(RemoteFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(701),
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(52),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "incoming".to_owned(),
                is_deleted: false,
            }),
            local: Some(LocalFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(701),
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "incoming".to_owned(),
                exists_on_disk: false,
                dirty: false,
                last_local_change_ms: 1_700_000_708_000,
            }),
            sync_anchor: None,
        };

        let planned = decide_file_action(&views, 1_700_000_710_000);

        assert_eq!(planned.decision, PlannerDecision::MaterializeRemoteDir);
        assert_eq!(planned.reason, PlannerReason::RemoteObservedWithoutAnchor);
    }

    #[test]
    fn planner_waits_remote_only_file_until_parent_materializes() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let namespace_id = NamespaceId::from("ns-1");
        db.planner_transaction("seed-remote-only-file-wait", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(52),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: None,
                display_name: "workspace".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: None,
                display_name: "workspace".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_712_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                synced_seq: ChangeSeq(52),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: None,
                display_name: "workspace".to_owned(),
            })?;
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(700),
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(53),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "incoming".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(700),
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "incoming".to_owned(),
                exists_on_disk: false,
                dirty: false,
                last_local_change_ms: 1_700_000_712_100,
            })?;
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(701),
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(54),
                revision_no: RevisionNo(1),
                content_digest: Some("sha256:child".to_owned()),
                content_manifest_digest: Some("sha256:manifest-child".to_owned()),
                parent_inode_id: Some(InodeId(700)),
                display_name: "welcome.txt".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(701),
                inode_kind: InodeKind::File,
                content_digest: None,
                parent_inode_id: Some(InodeId(700)),
                display_name: "welcome.txt".to_owned(),
                exists_on_disk: false,
                dirty: false,
                last_local_change_ms: 1_700_000_712_200,
            })?;
            Ok(())
        })
        .expect("seed remote-only wait state");

        let planned = plan_file(&mut db, &namespace_id, InodeId(701), 1_700_000_713_000)
            .expect("plan remote-only file child");

        assert_eq!(planned.decision, PlannerDecision::NoOp);
        assert_eq!(
            planned.reason,
            PlannerReason::RemoteParentWaitingForMaterialization
        );
    }

    #[test]
    fn planner_waits_remote_only_directory_until_parent_materializes() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let namespace_id = NamespaceId::from("ns-1");
        db.planner_transaction("seed-remote-only-dir-wait", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(52),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: None,
                display_name: "workspace".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: None,
                display_name: "workspace".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_722_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                synced_seq: ChangeSeq(52),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: None,
                display_name: "workspace".to_owned(),
            })?;
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(700),
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(53),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "incoming".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(700),
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "incoming".to_owned(),
                exists_on_disk: false,
                dirty: false,
                last_local_change_ms: 1_700_000_722_100,
            })?;
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(701),
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(54),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(700)),
                display_name: "nested".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(701),
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: Some(InodeId(700)),
                display_name: "nested".to_owned(),
                exists_on_disk: false,
                dirty: false,
                last_local_change_ms: 1_700_000_722_200,
            })?;
            Ok(())
        })
        .expect("seed remote-only dir wait state");

        let planned = plan_file(&mut db, &namespace_id, InodeId(701), 1_700_000_723_000)
            .expect("plan remote-only directory child");

        assert_eq!(planned.decision, PlannerDecision::NoOp);
        assert_eq!(
            planned.reason,
            PlannerReason::RemoteParentWaitingForMaterialization
        );
    }

    #[test]
    fn planner_allows_remote_subtree_rename_with_remote_only_descendant_placeholder() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let namespace_id = NamespaceId::from("ns-1");
        let root_inode_id = InodeId(10);
        let child_inode_id = InodeId(11);

        db.planner_transaction("seed-subtree-rename-remote-only-descendant", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(42),
                revision_no: RevisionNo(9),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: None,
                display_name: "workspace".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: None,
                display_name: "workspace".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_724_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                synced_seq: ChangeSeq(42),
                revision_no: RevisionNo(9),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: None,
                display_name: "workspace".to_owned(),
            })?;
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: root_inode_id,
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(43),
                revision_no: RevisionNo(7),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "reports-2026".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: root_inode_id,
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "reports".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_724_010,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id: root_inode_id,
                inode_kind: InodeKind::Dir,
                synced_seq: ChangeSeq(42),
                revision_no: RevisionNo(7),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "reports".to_owned(),
            })?;
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: child_inode_id,
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(42),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(root_inode_id),
                display_name: "incoming".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: child_inode_id,
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: Some(root_inode_id),
                display_name: "incoming".to_owned(),
                exists_on_disk: false,
                dirty: false,
                last_local_change_ms: 1_700_000_724_020,
            })?;
            Ok(())
        })
        .expect("seed subtree rename with remote-only descendant");

        let planned = plan_file(&mut db, &namespace_id, root_inode_id, 1_700_000_725_000)
            .expect("plan subtree rename with remote-only descendant");

        assert_eq!(planned.decision, PlannerDecision::ApplyRemoteSubtreeRename);
        assert_eq!(
            planned.reason,
            PlannerReason::RemoteSubtreePathDiffersFromAnchor
        );
    }

    #[test]
    fn planner_allows_remote_subtree_delete_with_remote_only_descendant_placeholder() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let namespace_id = NamespaceId::from("ns-1");
        let root_inode_id = InodeId(10);
        let child_inode_id = InodeId(11);

        db.planner_transaction("seed-subtree-delete-remote-only-descendant", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(42),
                revision_no: RevisionNo(9),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: None,
                display_name: "workspace".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: None,
                display_name: "workspace".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_726_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                synced_seq: ChangeSeq(42),
                revision_no: RevisionNo(9),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: None,
                display_name: "workspace".to_owned(),
            })?;
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: root_inode_id,
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(43),
                revision_no: RevisionNo(7),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "reports".to_owned(),
                is_deleted: true,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: root_inode_id,
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "reports".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_726_010,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id: root_inode_id,
                inode_kind: InodeKind::Dir,
                synced_seq: ChangeSeq(42),
                revision_no: RevisionNo(7),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "reports".to_owned(),
            })?;
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: child_inode_id,
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(42),
                revision_no: RevisionNo(1),
                content_digest: Some("sha256:child".to_owned()),
                content_manifest_digest: Some("sha256:manifest-child".to_owned()),
                parent_inode_id: Some(root_inode_id),
                display_name: "welcome.txt".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: child_inode_id,
                inode_kind: InodeKind::File,
                content_digest: None,
                parent_inode_id: Some(root_inode_id),
                display_name: "welcome.txt".to_owned(),
                exists_on_disk: false,
                dirty: false,
                last_local_change_ms: 1_700_000_726_020,
            })?;
            Ok(())
        })
        .expect("seed subtree delete with remote-only descendant");

        let planned = plan_file(&mut db, &namespace_id, root_inode_id, 1_700_000_727_000)
            .expect("plan subtree delete with remote-only descendant");

        assert_eq!(planned.decision, PlannerDecision::ApplyRemoteSubtreeDelete);
        assert_eq!(
            planned.reason,
            PlannerReason::RemoteSubtreeDeletedFromAnchor
        );
    }

    #[test]
    fn planner_marks_local_only_file_for_create_upload() {
        let local_only = LocalOnlyFileStateRow {
            client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
            namespace_id: NamespaceId::from("ns-1"),
            inode_kind: InodeKind::File,
            parent_inode_id: Some(InodeId(2)),
            display_name: "draft.txt".to_owned(),
            content_digest: Some("sha256:new-local-file".to_owned()),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 1_700_000_100_000,
        };

        let planned =
            decide_local_only_file_action(&local_only, 1_700_000_105_000).expect("plan file");

        assert_eq!(planned.decision, PlannerDecision::UploadLocalCreate);
        assert_eq!(
            planned.reason,
            PlannerReason::LocalOnlyFileWithoutRemoteIdentity
        );
    }

    #[test]
    fn planner_marks_local_only_directory_for_remote_create() {
        let local_only = LocalOnlyFileStateRow {
            client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000002"),
            namespace_id: NamespaceId::from("ns-1"),
            inode_kind: InodeKind::Dir,
            parent_inode_id: Some(InodeId(2)),
            display_name: "drafts".to_owned(),
            content_digest: None,
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 1_700_000_200_000,
        };

        let planned =
            decide_local_only_file_action(&local_only, 1_700_000_205_000).expect("plan directory");

        assert_eq!(planned.decision, PlannerDecision::CreateRemoteDir);
        assert_eq!(
            planned.reason,
            PlannerReason::LocalOnlyDirectoryWithoutRemoteIdentity
        );
    }
}
