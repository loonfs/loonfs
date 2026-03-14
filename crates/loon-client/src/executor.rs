use crate::planner::{PlannedLocalOnlyActionRecord, PlannerDecision};
use crate::state_db::LocalOnlyFileStateRow;
use loon_types::{ClientMutationOp, ClientMutationRequest, InodeKind};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("empty client request id")]
    EmptyClientRequestId,
    #[error(
        "client_file_id mismatch: local `{local_client_file_id}` != planned `{planned_client_file_id}`"
    )]
    ClientFileIdMismatch {
        local_client_file_id: String,
        planned_client_file_id: String,
    },
    #[error(
        "namespace mismatch: local `{local_namespace_id}` != planned `{planned_namespace_id}`"
    )]
    NamespaceMismatch {
        local_namespace_id: String,
        planned_namespace_id: String,
    },
    #[error("missing parent inode for `{client_file_id}`")]
    MissingParentInode { client_file_id: String },
    #[error("missing content manifest digest for `{client_file_id}`")]
    MissingContentManifestDigest { client_file_id: String },
    #[error("unsupported planner decision `{0:?}` for client mutation executor")]
    UnsupportedDecision(PlannerDecision),
    #[error(
        "planner decision `{decision:?}` is incompatible with inode kind `{inode_kind:?}` for `{client_file_id}`"
    )]
    DecisionKindMismatch {
        client_file_id: String,
        decision: PlannerDecision,
        inode_kind: InodeKind,
    },
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
