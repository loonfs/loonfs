use super::{CommitExecutionContext, CommitOp, CommitRequest, Precondition};
use loon_api::{v0 as api_v0, CommitIdValidationError};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CommitConversionError {
    #[error(transparent)]
    InvalidCommitId(#[from] CommitIdValidationError),
}

pub fn commit_request_from_v0(
    ctx: CommitExecutionContext,
    request: api_v0::CommitRequest,
) -> Result<CommitRequest, CommitConversionError> {
    loon_api::CommitId::parse(request.commit_id.as_str())?;
    Ok(CommitRequest {
        namespace_id: ctx.namespace_id,
        commit_id: request.commit_id,
        writer_id: ctx.writer_id,
        writer_fence_token: ctx.writer_fence_token,
        ops: request.ops.into_iter().map(commit_op_from_v0).collect(),
        preconditions: request
            .preconditions
            .into_iter()
            .map(commit_precondition_from_v0)
            .collect(),
        message: request.message,
        annotations: request.annotations,
    })
}

pub(super) fn commit_op_from_v0(op: api_v0::CommitOp) -> CommitOp {
    match op {
        api_v0::CommitOp::CreateDir {
            parent_inode,
            display_name,
        } => CommitOp::CreateDir {
            parent_inode,
            display_name,
        },
        api_v0::CommitOp::CreateFile {
            parent_inode,
            display_name,
            content_ref,
        } => CommitOp::CreateFile {
            parent_inode,
            display_name,
            content_ref,
        },
        api_v0::CommitOp::ReplaceFile {
            inode_id,
            base_revision_no,
            content_ref,
        } => CommitOp::ReplaceFile {
            inode_id,
            base_revision_no,
            content_ref,
        },
        api_v0::CommitOp::RestoreRevision {
            inode_id,
            source_revision_no,
            base_revision_no,
        } => CommitOp::RestoreRevision {
            inode_id,
            source_revision_no,
            base_revision_no,
        },
        api_v0::CommitOp::DeleteFile { inode_id } => CommitOp::DeleteFile { inode_id },
        api_v0::CommitOp::Rename {
            inode_id,
            new_parent_inode,
            new_display_name,
            mode,
        } => CommitOp::Rename {
            inode_id,
            new_parent_inode,
            new_display_name,
            mode,
        },
        api_v0::CommitOp::DeleteSubtree { root_inode } => CommitOp::DeleteSubtree { root_inode },
    }
}

pub(super) fn commit_precondition_from_v0(
    precondition: api_v0::CommitPrecondition,
) -> Precondition {
    match precondition {
        api_v0::CommitPrecondition::InodeRevisionIs {
            inode_id,
            revision_no,
        } => Precondition::InodeRevisionIs {
            inode_id,
            revision_no,
        },
        api_v0::CommitPrecondition::AncestorsNotSubtreeDeleted { inode_id } => {
            Precondition::AncestorsNotSubtreeDeleted { inode_id }
        }
        api_v0::CommitPrecondition::ChildNameAbsent {
            parent_inode,
            name_key,
        } => Precondition::ChildNameAbsent {
            parent_inode,
            name_key,
        },
        api_v0::CommitPrecondition::ChildNameIs {
            parent_inode,
            name_key,
            child_inode,
        } => Precondition::ChildNameIs {
            parent_inode,
            name_key,
            child_inode,
        },
        api_v0::CommitPrecondition::BindingIs {
            parent_inode,
            name_key,
            child_inode,
            bind_seq,
            bind_delta_index,
        } => Precondition::BindingIs {
            parent_inode,
            name_key,
            child_inode,
            bind_seq,
            bind_delta_index,
        },
        api_v0::CommitPrecondition::DirectoryEmpty { inode_id } => {
            Precondition::DirectoryEmpty { inode_id }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loon_api::{
        v0::{CommitOp as ApiCommitOp, CommitPrecondition as ApiCommitPrecondition},
        CommitId, ContentRef, FenceToken, InodeId, NamespaceId, RevisionNo,
    };

    #[test]
    fn api_adapter_maps_all_v0_ops_and_preconditions() {
        let content_ref = ContentRef::whole_file_v0(b"hello");
        let request = api_v0::CommitRequest {
            commit_id: CommitId::from("commit-a"),
            preconditions: vec![
                ApiCommitPrecondition::InodeRevisionIs {
                    inode_id: InodeId(2),
                    revision_no: RevisionNo(3),
                },
                ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(2),
                },
                ApiCommitPrecondition::ChildNameAbsent {
                    parent_inode: InodeId(1),
                    name_key: "docs".to_owned(),
                },
                ApiCommitPrecondition::ChildNameIs {
                    parent_inode: InodeId(1),
                    name_key: "file.txt".to_owned(),
                    child_inode: InodeId(2),
                },
                ApiCommitPrecondition::DirectoryEmpty {
                    inode_id: InodeId(3),
                },
            ],
            ops: vec![
                ApiCommitOp::CreateDir {
                    parent_inode: InodeId(1),
                    display_name: "docs".to_owned(),
                },
                ApiCommitOp::CreateFile {
                    parent_inode: InodeId(1),
                    display_name: "a.txt".to_owned(),
                    content_ref: content_ref.clone(),
                },
                ApiCommitOp::ReplaceFile {
                    inode_id: InodeId(2),
                    base_revision_no: RevisionNo(1),
                    content_ref: content_ref.clone(),
                },
                ApiCommitOp::RestoreRevision {
                    inode_id: InodeId(2),
                    source_revision_no: RevisionNo(1),
                    base_revision_no: RevisionNo(2),
                },
                ApiCommitOp::DeleteFile {
                    inode_id: InodeId(2),
                },
                ApiCommitOp::Rename {
                    inode_id: InodeId(2),
                    new_parent_inode: InodeId(1),
                    new_display_name: "b.txt".to_owned(),
                    mode: loon_api::v0::RenameMode::NoReplace,
                },
                ApiCommitOp::DeleteSubtree {
                    root_inode: InodeId(3),
                },
            ],
            message: Some("all ops".to_owned()),
            annotations: None,
        };

        let core = commit_request_from_v0(
            CommitExecutionContext {
                namespace_id: NamespaceId::from("demo"),
                writer_id: "writer-a".to_owned(),
                writer_fence_token: FenceToken(9),
            },
            request,
        )
        .expect("convert request");

        assert_eq!(core.ops.len(), 7);
        assert_eq!(core.preconditions.len(), 5);
        assert_eq!(core.writer_fence_token, FenceToken(9));
    }
}
