use crate::state_db::{
    ClientFileId, FileSyncViews, LocalOnlyFileStateRow, LocalOnlyPlannedActionRow,
    PlannedActionRow, SqliteStateDb, StateDbError, SyncAnchorRow, TransferDirection,
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
    ApplyRemoteDelete,
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
    RemotePathDiffersFromAnchor,
    RemotePathAndContentDifferFromAnchor,
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
        let views = tx.load_file_sync_views(namespace_id, inode_id)?;
        let mut action = decide_file_action(&views, now_ms);
        if let Some(preserved) =
            preserve_in_flight_action_for_remote_delete(tx, namespace_id, inode_id, &views, now_ms)?
        {
            action = preserved;
        }

        if action.decision == PlannerDecision::NoOp {
            tx.delete_planned_action(namespace_id, inode_id)?;
        } else {
            tx.upsert_planned_action(&action.to_row())?;
        }

        Ok(action)
    })
    .map_err(PlannerError::from)
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
                PlannerDecision::CreateConflictCopy,
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
                (true, false) if remote_content_matches_anchor && !remote_path_matches_anchor => (
                    PlannerDecision::ApplyRemoteRename,
                    PlannerReason::RemotePathDiffersFromAnchor,
                ),
                (true, false) if !remote_content_matches_anchor && !remote_path_matches_anchor => (
                    PlannerDecision::CreateConflictCopy,
                    PlannerReason::RemotePathAndContentDifferFromAnchor,
                ),
                (true, false) => (
                    PlannerDecision::DownloadRemoteEdit,
                    PlannerReason::RemoteDiffersFromAnchor,
                ),
                (false, false) => (
                    PlannerDecision::CreateConflictCopy,
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
            Self::ApplyRemoteDelete => "apply_remote_delete",
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
            "apply_remote_delete" => Ok(Self::ApplyRemoteDelete),
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
            Self::RemotePathDiffersFromAnchor => "remote_path_differs_from_anchor",
            Self::RemotePathAndContentDifferFromAnchor => {
                "remote_path_and_content_differ_from_anchor"
            }
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
            "remote_path_differs_from_anchor" => Ok(Self::RemotePathDiffersFromAnchor),
            "remote_path_and_content_differ_from_anchor" => {
                Ok(Self::RemotePathAndContentDifferFromAnchor)
            }
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

fn preserve_in_flight_action_for_remote_delete(
    tx: &mut crate::state_db::PlannerTxn<'_>,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    views: &FileSyncViews,
    now_ms: u64,
) -> Result<Option<PlannedActionRecord>, StateDbError> {
    let Some(remote) = views.remote.as_ref() else {
        return Ok(None);
    };
    if !remote.is_deleted || remote.inode_kind != InodeKind::File {
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

#[cfg(test)]
mod tests {
    use super::{
        decide_file_action, decide_local_only_file_action, PlannerDecision, PlannerReason,
    };
    use crate::state_db::{
        ClientFileId, FileSyncViews, LocalFileStateRow, LocalOnlyFileStateRow, RemoteFileStateRow,
        SyncAnchorRow,
    };
    use loon_types::{ChangeSeq, InodeId, InodeKind, NamespaceId, RevisionNo};

    #[test]
    fn planner_prefers_conflict_copy_when_local_and_remote_both_diverge() {
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

        assert_eq!(planned.decision, PlannerDecision::CreateConflictCopy);
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
    fn planner_prefers_conflict_copy_for_remote_path_and_content_change() {
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

        assert_eq!(planned.decision, PlannerDecision::CreateConflictCopy);
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
    fn planner_prefers_conflict_copy_for_remote_delete_when_local_diverges() {
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

        assert_eq!(planned.decision, PlannerDecision::CreateConflictCopy);
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
