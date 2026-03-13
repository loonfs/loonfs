use crate::state_db::{
    FileSyncViews, PlannedActionRow, SqliteStateDb, StateDbError, SyncAnchorRow,
};
use loon_types::{InodeId, NamespaceId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlannerDecision {
    UploadLocalEdit,
    DownloadRemoteEdit,
    CreateConflictCopy,
    NoOp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlannerReason {
    AlreadyConverged,
    NoObservedState,
    LocalDiffersFromAnchor,
    RemoteDiffersFromAnchor,
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
        let action = decide_file_action(&views, now_ms);

        if action.decision == PlannerDecision::NoOp {
            tx.delete_planned_action(namespace_id, inode_id)?;
        } else {
            tx.upsert_planned_action(&action.to_row())?;
        }

        Ok(action)
    })
    .map_err(PlannerError::from)
}

pub fn decide_file_action(views: &FileSyncViews, now_ms: u64) -> PlannedActionRecord {
    let (decision, reason) = match (&views.remote, &views.local, &views.sync_anchor) {
        (Some(remote), Some(local), Some(anchor)) => {
            let local_matches_anchor = local_matches_anchor(local, anchor);
            let remote_matches_anchor = remote_matches_anchor(remote, anchor);

            match (local_matches_anchor, remote_matches_anchor) {
                (true, true) => (PlannerDecision::NoOp, PlannerReason::AlreadyConverged),
                (false, true) => (
                    PlannerDecision::UploadLocalEdit,
                    PlannerReason::LocalDiffersFromAnchor,
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
        (Some(_), None, None) => (
            PlannerDecision::DownloadRemoteEdit,
            PlannerReason::RemoteObservedWithoutAnchor,
        ),
        (None, Some(_), None) => (
            PlannerDecision::UploadLocalEdit,
            PlannerReason::LocalObservedWithoutAnchor,
        ),
        (Some(remote), Some(local), None) => {
            if local.exists_on_disk
                && !local.dirty
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

impl PlannerDecision {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::UploadLocalEdit => "upload_local_edit",
            Self::DownloadRemoteEdit => "download_remote_edit",
            Self::CreateConflictCopy => "create_conflict_copy",
            Self::NoOp => "no_op",
        }
    }

    fn from_str(value: &str) -> Result<Self, PlannerError> {
        match value {
            "upload_local_edit" => Ok(Self::UploadLocalEdit),
            "download_remote_edit" => Ok(Self::DownloadRemoteEdit),
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
            Self::LocalDiffersFromAnchor => "local_differs_from_anchor",
            Self::RemoteDiffersFromAnchor => "remote_differs_from_anchor",
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
            "local_differs_from_anchor" => Ok(Self::LocalDiffersFromAnchor),
            "remote_differs_from_anchor" => Ok(Self::RemoteDiffersFromAnchor),
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
    !remote.is_deleted
        && remote.revision_no == anchor.revision_no
        && remote.content_digest == anchor.content_digest
        && remote.parent_inode_id == anchor.parent_inode_id
        && remote.display_name == anchor.display_name
}

fn local_matches_anchor(
    local: &crate::state_db::LocalFileStateRow,
    anchor: &SyncAnchorRow,
) -> bool {
    local.exists_on_disk
        && !local.dirty
        && local.content_digest == anchor.content_digest
        && local.parent_inode_id == anchor.parent_inode_id
        && local.display_name == anchor.display_name
}

#[cfg(test)]
mod tests {
    use super::{decide_file_action, PlannerDecision, PlannerReason};
    use crate::state_db::{FileSyncViews, LocalFileStateRow, RemoteFileStateRow, SyncAnchorRow};
    use loon_types::{ChangeSeq, InodeId, NamespaceId, RevisionNo};

    #[test]
    fn planner_prefers_conflict_copy_when_local_and_remote_both_diverge() {
        let views = FileSyncViews {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            remote: Some(RemoteFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                observed_seq: ChangeSeq(420),
                revision_no: RevisionNo(18),
                content_digest: Some("sha256:remote-18".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "report.txt".to_owned(),
                is_deleted: false,
            }),
            local: Some(LocalFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
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
                synced_seq: ChangeSeq(419),
                revision_no: RevisionNo(17),
                content_digest: Some("sha256:anchor-17".to_owned()),
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
}
