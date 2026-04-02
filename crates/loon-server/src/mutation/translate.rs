use crate::core::commit::{
    CommitOp, CommitPlan, CommitRequest, Precondition, PreparedCommitHeadPublish,
};
use crate::core::content::{
    validate_durable_content_reference, DurableContentValidationError, ValidatedDurableContent,
};
use crate::core::metadata::MetadataState;
use crate::mutation::{ClientMutationExecutionError, ClientMutationExecutionParams};
use crate::objectstore::ObjectStore;
use loon_types::{
    ClientMutationOp, ClientMutationRequest, ClientMutationResponse, CreatedRemoteInode,
    DeletedRemoteInode, HeadState, RenamedRemoteInode, ReplacedRemoteFile, RevisionNo,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum ClientMutationTranslationError {
    #[error("empty client request id")]
    EmptyClientRequestId,
    #[error("empty writer id")]
    EmptyWriterId,
    #[error("empty display name")]
    EmptyDisplayName,
    #[error("empty content manifest digest")]
    EmptyContentManifestDigest,
    #[error(
        "replace file revision overflow for inode `{inode_id:?}` base revision `{base_revision:?}`"
    )]
    ReplaceFileRevisionOverflow {
        inode_id: loon_types::InodeId,
        base_revision: RevisionNo,
    },
    #[error("rename response is missing the current binding for inode `{inode_id:?}`")]
    RenameResponseBindingMissing { inode_id: loon_types::InodeId },
    #[error("delete response is missing inode metadata for inode `{inode_id:?}`")]
    DeleteResponseInodeMissing { inode_id: loon_types::InodeId },
}

pub(crate) fn validate_referenced_durable_content<S: ObjectStore>(
    store: &S,
    request: &ClientMutationRequest,
) -> Result<Option<ValidatedDurableContent>, ClientMutationExecutionError> {
    match &request.op {
        ClientMutationOp::CreateFile {
            content_manifest_digest,
            ..
        }
        | ClientMutationOp::ReplaceFile {
            content_manifest_digest,
            ..
        } => validate_durable_content_reference(
            store,
            &request.namespace_id,
            content_manifest_digest,
        )
        .map(Some)
        .map_err(ClientMutationExecutionError::DurableContent),
        ClientMutationOp::CreateDir { .. }
        | ClientMutationOp::Rename { .. }
        | ClientMutationOp::DeleteFile { .. }
        | ClientMutationOp::DeleteSubtree { .. } => Ok(None),
    }
}

pub(crate) fn build_client_mutation_response(
    request: &ClientMutationRequest,
    validated_content: Option<&ValidatedDurableContent>,
    plan: &CommitPlan,
    resulting_metadata_state: &MetadataState,
    head_publish: &PreparedCommitHeadPublish,
) -> Result<ClientMutationResponse, ClientMutationExecutionError> {
    let (created_inode, replaced_file, renamed_inode, deleted_inode) = match &request.op {
        ClientMutationOp::CreateDir {
            parent_inode_id,
            display_name,
        } => {
            if plan.allocated_inode_ids.len() != 1 {
                return Err(
                    ClientMutationExecutionError::UnexpectedAllocatedInodeCount {
                        actual: plan.allocated_inode_ids.len(),
                    },
                );
            }
            let inode_id = plan.allocated_inode_ids[0];
            (
                Some(CreatedRemoteInode {
                    inode_id,
                    inode_kind: loon_types::InodeKind::Dir,
                    revision_no: RevisionNo(1),
                    parent_inode_id: *parent_inode_id,
                    display_name: display_name.clone(),
                    content_digest: None,
                }),
                None,
                None,
                None,
            )
        }
        ClientMutationOp::CreateFile {
            parent_inode_id,
            display_name,
            ..
        } => {
            if plan.allocated_inode_ids.len() != 1 {
                return Err(
                    ClientMutationExecutionError::UnexpectedAllocatedInodeCount {
                        actual: plan.allocated_inode_ids.len(),
                    },
                );
            }
            let inode_id = plan.allocated_inode_ids[0];
            (
                Some(CreatedRemoteInode {
                    inode_id,
                    inode_kind: loon_types::InodeKind::File,
                    revision_no: RevisionNo(1),
                    parent_inode_id: *parent_inode_id,
                    display_name: display_name.clone(),
                    content_digest: Some(
                        validated_content
                            .expect("create_file response requires validated durable content")
                            .file_digest_sha256
                            .clone(),
                    ),
                }),
                None,
                None,
                None,
            )
        }
        ClientMutationOp::ReplaceFile {
            inode_id,
            base_revision_no,
            ..
        } => {
            let revision_no = base_revision_no.0.checked_add(1).map(RevisionNo).ok_or(
                ClientMutationTranslationError::ReplaceFileRevisionOverflow {
                    inode_id: *inode_id,
                    base_revision: *base_revision_no,
                },
            )?;
            (
                None,
                Some(ReplacedRemoteFile {
                    inode_id: *inode_id,
                    inode_kind: loon_types::InodeKind::File,
                    revision_no,
                    content_digest: validated_content
                        .expect("replace_file response requires validated durable content")
                        .file_digest_sha256
                        .clone(),
                }),
                None,
                None,
            )
        }
        ClientMutationOp::Rename { inode_id, .. } => {
            let current_binding = resulting_metadata_state
                .current_parent_binding_for_child(*inode_id, head_publish.resulting_head.seq)
                .ok_or(
                    ClientMutationTranslationError::RenameResponseBindingMissing {
                        inode_id: *inode_id,
                    },
                )?;
            let inode_kind = resulting_metadata_state
                .inode_at_seq(*inode_id, head_publish.resulting_head.seq)
                .ok_or(ClientMutationTranslationError::DeleteResponseInodeMissing {
                    inode_id: *inode_id,
                })?
                .inode_kind;
            (
                None,
                None,
                Some(RenamedRemoteInode {
                    inode_id: *inode_id,
                    inode_kind,
                    parent_inode_id: current_binding.parent_inode_id,
                    display_name: current_binding.display_name,
                }),
                None,
            )
        }
        ClientMutationOp::DeleteFile { inode_id } => {
            let inode_kind = resulting_metadata_state
                .inode_at_seq(*inode_id, head_publish.resulting_head.seq)
                .ok_or(ClientMutationTranslationError::DeleteResponseInodeMissing {
                    inode_id: *inode_id,
                })?
                .inode_kind;
            (
                None,
                None,
                None,
                Some(DeletedRemoteInode {
                    inode_id: *inode_id,
                    inode_kind,
                }),
            )
        }
        ClientMutationOp::DeleteSubtree { root_inode_id } => {
            let inode_kind = resulting_metadata_state
                .inode_at_seq(*root_inode_id, head_publish.resulting_head.seq)
                .ok_or(ClientMutationTranslationError::DeleteResponseInodeMissing {
                    inode_id: *root_inode_id,
                })?
                .inode_kind;
            (
                None,
                None,
                None,
                Some(DeletedRemoteInode {
                    inode_id: *root_inode_id,
                    inode_kind,
                }),
            )
        }
    };

    Ok(ClientMutationResponse {
        namespace_id: request.namespace_id.clone(),
        client_request_id: request.client_request_id.clone(),
        committed_seq: head_publish.resulting_head.seq,
        created_inode,
        replaced_file,
        renamed_inode,
        deleted_inode,
    })
}

pub(crate) fn translate_client_mutation_request(
    request: &ClientMutationRequest,
    params: &ClientMutationExecutionParams,
    current_head: &HeadState,
) -> Result<CommitRequest, ClientMutationTranslationError> {
    if request.client_request_id.trim().is_empty() {
        return Err(ClientMutationTranslationError::EmptyClientRequestId);
    }

    if params.writer_id.trim().is_empty() {
        return Err(ClientMutationTranslationError::EmptyWriterId);
    }

    let (op, preconditions) = match &request.op {
        ClientMutationOp::CreateDir {
            parent_inode_id,
            display_name,
        } => (
            CommitOp::CreateDir {
                parent_inode: *parent_inode_id,
                display_name: display_name.clone(),
            },
            vec![
                Precondition::HeadSeqIs(current_head.seq),
                Precondition::ChildNameAbsent {
                    parent_inode: *parent_inode_id,
                    name_key: display_name.clone(),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: *parent_inode_id,
                },
            ],
        ),
        ClientMutationOp::CreateFile {
            parent_inode_id,
            display_name,
            content_manifest_digest,
        } => {
            if content_manifest_digest.trim().is_empty() {
                return Err(ClientMutationTranslationError::EmptyContentManifestDigest);
            }

            (
                CommitOp::CreateFile {
                    parent_inode: *parent_inode_id,
                    display_name: display_name.clone(),
                    content_manifest_digest: content_manifest_digest.clone(),
                },
                vec![
                    Precondition::HeadSeqIs(current_head.seq),
                    Precondition::ChildNameAbsent {
                        parent_inode: *parent_inode_id,
                        name_key: display_name.clone(),
                    },
                    Precondition::AncestorsNotSubtreeDeleted {
                        inode_id: *parent_inode_id,
                    },
                ],
            )
        }
        ClientMutationOp::ReplaceFile {
            inode_id,
            base_revision_no,
            content_manifest_digest,
        } => {
            if content_manifest_digest.trim().is_empty() {
                return Err(ClientMutationTranslationError::EmptyContentManifestDigest);
            }

            (
                CommitOp::ReplaceFile {
                    inode_id: *inode_id,
                    base_revision: *base_revision_no,
                    content_manifest_digest: content_manifest_digest.clone(),
                },
                vec![
                    Precondition::HeadSeqIs(current_head.seq),
                    Precondition::InodeRevisionIs {
                        inode_id: *inode_id,
                        revision: *base_revision_no,
                    },
                    Precondition::AncestorsNotSubtreeDeleted {
                        inode_id: *inode_id,
                    },
                ],
            )
        }
        ClientMutationOp::Rename {
            inode_id,
            new_parent_inode_id,
            new_display_name,
        } => (
            CommitOp::Rename {
                inode_id: *inode_id,
                new_parent_inode: *new_parent_inode_id,
                new_display_name: new_display_name.clone(),
            },
            vec![
                Precondition::HeadSeqIs(current_head.seq),
                Precondition::ChildNameAbsent {
                    parent_inode: *new_parent_inode_id,
                    name_key: new_display_name.clone(),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: *inode_id,
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: *new_parent_inode_id,
                },
            ],
        ),
        ClientMutationOp::DeleteFile { inode_id } => (
            CommitOp::DeleteFile {
                inode_id: *inode_id,
            },
            vec![
                Precondition::HeadSeqIs(current_head.seq),
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: *inode_id,
                },
            ],
        ),
        ClientMutationOp::DeleteSubtree { root_inode_id } => (
            CommitOp::DeleteSubtree {
                root_inode: *root_inode_id,
            },
            vec![
                Precondition::HeadSeqIs(current_head.seq),
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: *root_inode_id,
                },
            ],
        ),
    };

    if matches!(
        &request.op,
        ClientMutationOp::CreateDir { display_name, .. }
            | ClientMutationOp::CreateFile { display_name, .. }
            | ClientMutationOp::Rename {
                new_display_name: display_name,
                ..
            } if display_name.trim().is_empty()
    ) {
        return Err(ClientMutationTranslationError::EmptyDisplayName);
    }

    Ok(CommitRequest {
        namespace_id: request.namespace_id.clone(),
        request_id: request.client_request_id.clone(),
        writer_id: params.writer_id.clone(),
        writer_fence_token: current_head.active_fence_token,
        planned_head_seq: current_head.seq,
        ops: vec![op],
        preconditions,
    })
}

impl From<ClientMutationTranslationError> for ClientMutationExecutionError {
    fn from(value: ClientMutationTranslationError) -> Self {
        Self::Translate(value)
    }
}

impl From<DurableContentValidationError> for ClientMutationExecutionError {
    fn from(value: DurableContentValidationError) -> Self {
        Self::DurableContent(value)
    }
}
