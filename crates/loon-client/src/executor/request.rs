use super::*;
use crate::planner::{PlannedActionRecord, PlannedLocalOnlyActionRecord, PlannerDecision};
use crate::state_db::{
    ClientFileId, LocalFileStateRow, LocalOnlyFileStateRow, SqliteStateDb, StateDbError,
    SyncAnchorRow,
};
use loon_types::{ClientMutationOp, ClientMutationRequest, InodeId, InodeKind, NamespaceId};

pub fn build_inode_mutation_request_from_state(
    db: &SqliteStateDb,
    client_request_id: &str,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<ClientMutationRequest, ExecutorError> {
    let planned_row = db
        .load_planned_action(namespace_id, inode_id)?
        .ok_or_else(|| ExecutorError::PlannedActionMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        })?;
    let planned = PlannedActionRecord::try_from(planned_row)?;
    let (_remote, local, anchor) = db.load_bound_upload_local_edit_views(namespace_id, inode_id)?;
    let content_manifest_digest = match planned.decision {
        PlannerDecision::UploadLocalEdit => {
            Some(db.resolve_inode_upload_content_manifest_digest(&local)?)
        }
        _ => None,
    };

    build_inode_mutation_request(
        client_request_id,
        &local,
        &anchor,
        &planned,
        content_manifest_digest.as_deref(),
    )
}

pub fn build_inode_mutation_request(
    client_request_id: &str,
    local: &LocalFileStateRow,
    anchor: &SyncAnchorRow,
    planned: &PlannedActionRecord,
    content_manifest_digest: Option<&str>,
) -> Result<ClientMutationRequest, ExecutorError> {
    if client_request_id.trim().is_empty() {
        return Err(ExecutorError::EmptyClientRequestId);
    }

    if local.namespace_id != planned.namespace_id {
        return Err(ExecutorError::PlannedInodeNamespaceMismatch {
            inode_id: local.inode_id.0,
            local_namespace_id: local.namespace_id.as_str().to_owned(),
            planned_namespace_id: planned.namespace_id.as_str().to_owned(),
        });
    }

    if local.inode_id != planned.inode_id {
        return Err(ExecutorError::PlannedActionMissing {
            namespace_id: local.namespace_id.as_str().to_owned(),
            inode_id: local.inode_id.0,
        });
    }

    let op = match planned.decision {
        PlannerDecision::UploadLocalEdit => {
            if local.inode_kind != InodeKind::File {
                return Err(ExecutorError::InodeDecisionKindMismatch {
                    namespace_id: local.namespace_id.as_str().to_owned(),
                    inode_id: local.inode_id.0,
                    decision: planned.decision.clone(),
                    inode_kind: local.inode_kind.clone(),
                });
            }

            ClientMutationOp::ReplaceFile {
                inode_id: local.inode_id,
                base_revision_no: anchor.revision_no,
                content_manifest_digest: content_manifest_digest.map(str::to_owned).ok_or_else(
                    || ExecutorError::MissingInodeContentManifestDigest {
                        namespace_id: local.namespace_id.as_str().to_owned(),
                        inode_id: local.inode_id.0,
                    },
                )?,
            }
        }
        ref other => return Err(ExecutorError::UnsupportedDecision(other.clone())),
    };

    Ok(ClientMutationRequest {
        namespace_id: local.namespace_id.clone(),
        client_request_id: client_request_id.to_owned(),
        op,
    })
}

pub fn build_client_mutation_request_from_state(
    db: &SqliteStateDb,
    client_request_id: &str,
    client_file_id: &ClientFileId,
) -> Result<ClientMutationRequest, ExecutorError> {
    let local_only = db.load_local_only_file(client_file_id)?.ok_or_else(|| {
        StateDbError::LocalOnlyFileMissing {
            client_file_id: client_file_id.as_str().to_owned(),
        }
    })?;
    let planned_row = db
        .load_planned_local_only_action(client_file_id)?
        .ok_or_else(|| ExecutorError::PlannedLocalOnlyActionMissing {
            client_file_id: client_file_id.as_str().to_owned(),
        })?;
    let planned = PlannedLocalOnlyActionRecord::try_from(planned_row)?;
    let content_manifest_digest = match planned.decision {
        PlannerDecision::UploadLocalCreate => {
            Some(db.resolve_local_only_upload_content_manifest_digest(&local_only)?)
        }
        _ => None,
    };

    build_client_mutation_request(
        client_request_id,
        &local_only,
        &planned,
        content_manifest_digest.as_deref(),
    )
}

pub fn build_client_mutation_request(
    client_request_id: &str,
    local_only: &LocalOnlyFileStateRow,
    planned: &PlannedLocalOnlyActionRecord,
    content_manifest_digest: Option<&str>,
) -> Result<ClientMutationRequest, ExecutorError> {
    if client_request_id.trim().is_empty() {
        return Err(ExecutorError::EmptyClientRequestId);
    }

    if local_only.client_file_id != planned.client_file_id {
        return Err(ExecutorError::ClientFileIdMismatch {
            local_client_file_id: local_only.client_file_id.as_str().to_owned(),
            planned_client_file_id: planned.client_file_id.as_str().to_owned(),
        });
    }

    if local_only.namespace_id != planned.namespace_id {
        return Err(ExecutorError::NamespaceMismatch {
            local_namespace_id: local_only.namespace_id.as_str().to_owned(),
            planned_namespace_id: planned.namespace_id.as_str().to_owned(),
        });
    }

    let parent_inode_id =
        local_only
            .parent_inode_id
            .ok_or_else(|| ExecutorError::MissingParentInode {
                client_file_id: local_only.client_file_id.as_str().to_owned(),
            })?;

    let op = match planned.decision {
        PlannerDecision::CreateRemoteDir => {
            if local_only.inode_kind != InodeKind::Dir {
                return Err(ExecutorError::DecisionKindMismatch {
                    client_file_id: local_only.client_file_id.as_str().to_owned(),
                    decision: planned.decision.clone(),
                    inode_kind: local_only.inode_kind.clone(),
                });
            }

            ClientMutationOp::CreateDir {
                parent_inode_id,
                display_name: local_only.display_name.clone(),
            }
        }
        PlannerDecision::UploadLocalCreate => {
            if local_only.inode_kind != InodeKind::File {
                return Err(ExecutorError::DecisionKindMismatch {
                    client_file_id: local_only.client_file_id.as_str().to_owned(),
                    decision: planned.decision.clone(),
                    inode_kind: local_only.inode_kind.clone(),
                });
            }

            ClientMutationOp::CreateFile {
                parent_inode_id,
                display_name: local_only.display_name.clone(),
                content_manifest_digest: content_manifest_digest.map(str::to_owned).ok_or_else(
                    || ExecutorError::MissingContentManifestDigest {
                        client_file_id: local_only.client_file_id.as_str().to_owned(),
                    },
                )?,
            }
        }
        ref other => return Err(ExecutorError::UnsupportedDecision(other.clone())),
    };

    Ok(ClientMutationRequest {
        namespace_id: local_only.namespace_id.clone(),
        client_request_id: client_request_id.to_owned(),
        op,
    })
}
