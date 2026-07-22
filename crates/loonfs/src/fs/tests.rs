//! Behavior tests for the runtime core.

use crate::{
    ChangeSeq, CommitId, CommitOp, CommitPrecondition, CommitRequest, InodeId, NameKey, RevisionNo,
};
use loonfs_api::{DisplayName, NamePolicy};

#[test]
fn explicit_commit_facade_exports_constructor_types() {
    let display_name = DisplayName::parse("Report.txt").expect("valid display name");
    let name_key = NameKey::for_display_name(NamePolicy::default(), &display_name);
    let precondition = CommitPrecondition::BindingIs {
        parent_inode_id: InodeId(1),
        name_key,
        child_inode_id: InodeId(2),
        bind_seq: ChangeSeq(3),
        bind_delta_index: 4,
    };

    let request = CommitRequest {
        commit_id: CommitId::generate(),
        preconditions: vec![precondition],
        ops: vec![
            CommitOp::RestoreRevision {
                inode_id: InodeId(2),
                source_revision_no: RevisionNo(1),
                base_revision_no: RevisionNo(2),
            },
            CommitOp::Rename {
                inode_id: InodeId(2),
                new_parent_inode_id: InodeId(1),
                new_display_name: "report.txt".to_owned(),
            },
        ],
        message: None,
    };

    assert_eq!(request.preconditions.len(), 1);
    assert_eq!(request.ops.len(), 2);
}
