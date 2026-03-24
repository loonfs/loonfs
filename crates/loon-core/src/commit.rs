use crate::metadata::MetadataState;
use loon_types::{
    ChangeSeq, FenceToken, HeadState, HeadStateEnvelope, InodeId, InodeKind, LeaseState,
    NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};

mod frame;
mod ordered;
mod publish;

pub use self::ordered::build_commit_plan;
pub use self::publish::{prepare_commit_head_publish, publish_commit_head};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRequest {
    pub namespace_id: NamespaceId,
    pub request_id: String,
    pub writer_id: String,
    pub writer_fence_token: FenceToken,
    pub planned_head_seq: ChangeSeq,
    pub ops: Vec<CommitOp>,
    pub preconditions: Vec<Precondition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitOp {
    CreateDir {
        parent_inode: InodeId,
        display_name: String,
    },
    CreateFile {
        parent_inode: InodeId,
        display_name: String,
        content_manifest_digest: String,
    },
    ReplaceFile {
        inode_id: InodeId,
        base_revision: RevisionNo,
        content_manifest_digest: String,
    },
    DeleteFile {
        inode_id: InodeId,
    },
    Rename {
        inode_id: InodeId,
        new_parent_inode: InodeId,
        new_display_name: String,
    },
    DeleteSubtree {
        root_inode: InodeId,
    },
    RestoreRevision {
        inode_id: InodeId,
        base_revision: RevisionNo,
        restore_from_revision: RevisionNo,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Precondition {
    HeadSeqIs(ChangeSeq),
    InodeRevisionIs {
        inode_id: InodeId,
        revision: RevisionNo,
    },
    AncestorsNotSubtreeDeleted {
        inode_id: InodeId,
    },
    ChildNameAbsent {
        parent_inode: InodeId,
        name_key: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitPlan {
    pub namespace_id: NamespaceId,
    pub commit_id: String,
    pub base_head_seq: ChangeSeq,
    pub next_seq: ChangeSeq,
    pub allocated_inode_ids: Vec<InodeId>,
    pub resulting_next_inode_id: InodeId,
    pub durable_content_required: bool,
    pub wal_object_must_be_written: bool,
    pub head_cas_must_succeed: bool,
    pub metadata_preconditions: Vec<Precondition>,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitValidationContext {
    pub head: HeadState,
    pub lease: LeaseState,
    pub now_ms: u64,
    #[serde(default)]
    pub metadata_state: MetadataState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedCommitHeadPublish {
    pub object_key: String,
    pub resulting_head: HeadState,
    pub envelope: HeadStateEnvelope,
    pub encoded_bytes: Vec<u8>,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitValidationError {
    EmptyCommit,
    NamespaceMismatch,
    HeadLeaseNamespaceMismatch,
    HeadLeaseFenceMismatch {
        head: FenceToken,
        lease: FenceToken,
    },
    PlannedHeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    MissingHeadSeqPrecondition {
        expected: ChangeSeq,
    },
    ConflictingHeadSeqPrecondition {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    CreateParentMissing {
        parent_inode: InodeId,
    },
    CreateParentNotDirectory {
        parent_inode: InodeId,
        actual_kind: InodeKind,
    },
    CreateChildNameCollision {
        parent_inode: InodeId,
        name_key: String,
        child_inode: InodeId,
    },
    CreateUnderSubtreeTombstone {
        parent_inode: InodeId,
        root_inode: InodeId,
        tombstone_seq: ChangeSeq,
    },
    ReplaceFileInodeMissing {
        inode_id: InodeId,
    },
    ReplaceFileInodeNotFile {
        inode_id: InodeId,
        actual_kind: InodeKind,
    },
    ReplaceFileBaseRevisionMismatch {
        inode_id: InodeId,
        expected: RevisionNo,
        actual: Option<RevisionNo>,
    },
    ReplaceFileUnderSubtreeTombstone {
        inode_id: InodeId,
        root_inode: InodeId,
        tombstone_seq: ChangeSeq,
    },
    DeleteFileInodeMissing {
        inode_id: InodeId,
    },
    DeleteFileInodeNotFile {
        inode_id: InodeId,
        actual_kind: InodeKind,
    },
    DeleteFileCoveredByTombstone {
        inode_id: InodeId,
        covering_root_inode: InodeId,
        tombstone_seq: ChangeSeq,
    },
    RenameInodeMissing {
        inode_id: InodeId,
    },
    RenameSourceBindingMissing {
        inode_id: InodeId,
    },
    RenameTargetParentMissing {
        parent_inode: InodeId,
    },
    RenameTargetParentNotDirectory {
        parent_inode: InodeId,
        actual_kind: InodeKind,
    },
    RenameTargetNameCollision {
        parent_inode: InodeId,
        name_key: String,
        child_inode: InodeId,
    },
    RenameWouldCycleDirectory {
        inode_id: InodeId,
        new_parent_inode: InodeId,
    },
    RenameInodeUnderSubtreeTombstone {
        inode_id: InodeId,
        root_inode: InodeId,
        tombstone_seq: ChangeSeq,
    },
    RenameTargetParentUnderSubtreeTombstone {
        parent_inode: InodeId,
        root_inode: InodeId,
        tombstone_seq: ChangeSeq,
    },
    DeleteSubtreeRootMissing {
        root_inode: InodeId,
    },
    DeleteSubtreeRootNotDirectory {
        root_inode: InodeId,
        actual_kind: InodeKind,
    },
    DeleteSubtreeRootCoveredByTombstone {
        root_inode: InodeId,
        covering_root_inode: InodeId,
        tombstone_seq: ChangeSeq,
    },
    RestoreRevisionInodeMissing {
        inode_id: InodeId,
    },
    RestoreRevisionInodeNotFile {
        inode_id: InodeId,
        actual_kind: InodeKind,
    },
    RestoreRevisionBaseRevisionMismatch {
        inode_id: InodeId,
        expected: RevisionNo,
        actual: Option<RevisionNo>,
    },
    RestoreRevisionSourceMissing {
        inode_id: InodeId,
        restore_from_revision: RevisionNo,
    },
    RestoreRevisionSourceNotHistorical {
        inode_id: InodeId,
        base_revision: RevisionNo,
        restore_from_revision: RevisionNo,
    },
    RestoreRevisionUnderSubtreeTombstone {
        inode_id: InodeId,
        root_inode: InodeId,
        tombstone_seq: ChangeSeq,
    },
    ReplaceFileRevisionOverflow {
        inode_id: InodeId,
        base_revision: RevisionNo,
    },
    RestoreRevisionOverflow {
        inode_id: InodeId,
        base_revision: RevisionNo,
    },
    StaleWriterFenceToken {
        active: FenceToken,
        requested: FenceToken,
    },
    LeaseHolderMismatch {
        expected: String,
        actual: String,
    },
    LeaseExpired {
        lease_expires_at_ms: u64,
        now_ms: u64,
    },
    SeqOverflow,
    NextInodeOverflow,
    OpIndexOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitHeadPublishError {
    EmptyWriterVersion,
    EmptyExpectedHeadEtag,
    NamespaceMismatch {
        head: NamespaceId,
        plan: NamespaceId,
    },
    PlanBaseHeadSeqMismatch {
        head: ChangeSeq,
        plan: ChangeSeq,
    },
    PlanNextSeqMismatch {
        head: ChangeSeq,
        plan: ChangeSeq,
    },
    Codec(String),
    Store(String),
}

pub(crate) fn push_unique_invariant(invariants: &mut Vec<String>, name: &str) {
    if !invariants.iter().any(|existing| existing == name) {
        invariants.push(name.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_commit_plan, prepare_commit_head_publish, publish_commit_head, CommitOp,
        CommitRequest, CommitValidationContext, CommitValidationError, Precondition,
    };
    use crate::metadata::{
        DirentryRecord, InodeRecord, MetadataState, RevisionRecord, SubtreeTombstoneRecord,
    };
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::keys::namespace_head;
    use loon_objectstore::ObjectStore;
    use loon_types::{
        ChangeSeq, ControlObjectKind, FenceToken, HeadState, HeadStateEnvelope, InodeId, InodeKind,
        LeaseState, NamespaceId, RevisionNo,
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn build_commit_plan_accepts_active_writer() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-1".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(2),
                display_name: "drafts".to_owned(),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(41)),
                Precondition::ChildNameAbsent {
                    parent_inode: InodeId(2),
                    name_key: "drafts".to_owned(),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(2),
                },
            ],
        };

        let plan = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, create_parent_metadata_state(InodeId(2))),
        )
        .expect("valid plan");

        assert_eq!(plan.next_seq, ChangeSeq(42));
        assert!(!plan.durable_content_required);
        assert_eq!(plan.resulting_next_inode_id, InodeId(502));
        assert!(plan
            .checked_invariants
            .contains(&"stale_writer_cannot_publish".to_owned()));
    }

    #[test]
    fn build_commit_plan_requires_durable_content_for_replace_file() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-2".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::ReplaceFile {
                inode_id: InodeId(42),
                base_revision: RevisionNo(7),
                content_manifest_digest: "sha256:manifest".to_owned(),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(41)),
                Precondition::InodeRevisionIs {
                    inode_id: InodeId(42),
                    revision: RevisionNo(7),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(42),
                },
            ],
        };

        let plan = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, replace_metadata_state()),
        )
        .expect("valid plan");

        assert!(plan.durable_content_required);
    }

    #[test]
    fn build_commit_plan_accepts_delete_subtree_for_visible_directory_root() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-delete-subtree".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::DeleteSubtree {
                root_inode: InodeId(7),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(41)),
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(7),
                },
            ],
        };

        let plan = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, delete_root_metadata_state()),
        )
        .expect("delete subtree should validate");

        assert_eq!(plan.next_seq, ChangeSeq(42));
        assert_eq!(plan.resulting_next_inode_id, InodeId(501));
        assert!(plan
            .checked_invariants
            .contains(&"subtree_tombstone_blocks_descendant_mutation".to_owned()));
    }

    #[test]
    fn build_commit_plan_accepts_restore_revision_for_visible_file() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-restore-file".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(52),
            ops: vec![CommitOp::RestoreRevision {
                inode_id: InodeId(42),
                base_revision: RevisionNo(5),
                restore_from_revision: RevisionNo(3),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(52)),
                Precondition::InodeRevisionIs {
                    inode_id: InodeId(42),
                    revision: RevisionNo(5),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(42),
                },
            ],
        };

        let plan = build_commit_plan(
            &request,
            &validation_context_with_metadata_at_seq(
                999,
                ChangeSeq(52),
                restore_revision_metadata_state(),
            ),
        )
        .expect("restore revision should validate");

        assert_eq!(plan.next_seq, ChangeSeq(53));
        assert!(plan
            .checked_invariants
            .contains(&"subtree_tombstone_blocks_descendant_mutation".to_owned()));
    }

    #[test]
    fn build_commit_plan_accepts_rename_for_visible_child() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-rename-file".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(52),
            ops: vec![CommitOp::Rename {
                inode_id: InodeId(42),
                new_parent_inode: InodeId(7),
                new_display_name: "report-renamed.txt".to_owned(),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(52)),
                Precondition::ChildNameAbsent {
                    parent_inode: InodeId(7),
                    name_key: "report-renamed.txt".to_owned(),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(42),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(7),
                },
            ],
        };

        let plan = build_commit_plan(
            &request,
            &validation_context_with_metadata_at_seq(999, ChangeSeq(52), rename_metadata_state()),
        )
        .expect("rename should validate");

        assert_eq!(plan.next_seq, ChangeSeq(53));
        assert!(plan
            .checked_invariants
            .contains(&"subtree_tombstone_blocks_descendant_mutation".to_owned()));
    }

    #[test]
    fn build_commit_plan_allocates_inode_for_create_mutation() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-create-dir".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(2),
                display_name: "drafts".to_owned(),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(41)),
                Precondition::ChildNameAbsent {
                    parent_inode: InodeId(2),
                    name_key: "drafts".to_owned(),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(2),
                },
            ],
        };

        let plan = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, create_parent_metadata_state(InodeId(2))),
        )
        .expect("valid plan");

        assert_eq!(plan.allocated_inode_ids, vec![InodeId(501)]);
        assert_eq!(plan.resulting_next_inode_id, InodeId(502));
        assert!(plan
            .checked_invariants
            .contains(&"create_mutation_consumes_next_inode_id".to_owned()));
    }

    #[test]
    fn build_commit_plan_requires_durable_content_for_create_file() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-create-file".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(2),
                display_name: "note.txt".to_owned(),
                content_manifest_digest: "sha256:child-note".to_owned(),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(41)),
                Precondition::ChildNameAbsent {
                    parent_inode: InodeId(2),
                    name_key: "note.txt".to_owned(),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(2),
                },
            ],
        };

        let plan = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, create_parent_metadata_state(InodeId(2))),
        )
        .expect("valid plan");

        assert!(plan.durable_content_required);
        assert!(plan
            .checked_invariants
            .contains(&"create_file_requires_durable_content".to_owned()));
    }

    #[test]
    fn build_commit_plan_rejects_existing_child_name_collision() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-create-collision".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(2),
                display_name: "note.txt".to_owned(),
                content_manifest_digest: "sha256:new-note".to_owned(),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(41)),
                Precondition::ChildNameAbsent {
                    parent_inode: InodeId(2),
                    name_key: "note.txt".to_owned(),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(2),
                },
            ],
        };

        let error = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, create_collision_metadata_state()),
        )
        .expect_err("existing child name should be rejected");

        assert_eq!(
            error,
            CommitValidationError::CreateChildNameCollision {
                parent_inode: InodeId(2),
                name_key: "note.txt".to_owned(),
                child_inode: InodeId(42),
            }
        );
    }

    #[test]
    fn build_commit_plan_rejects_stale_replace_base_revision() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-replace-stale".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::ReplaceFile {
                inode_id: InodeId(42),
                base_revision: RevisionNo(16),
                content_manifest_digest: "sha256:report-v18".to_owned(),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(41)),
                Precondition::InodeRevisionIs {
                    inode_id: InodeId(42),
                    revision: RevisionNo(16),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(42),
                },
            ],
        };

        let error = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, replace_stale_revision_metadata_state()),
        )
        .expect_err("stale revision should be rejected");

        assert_eq!(
            error,
            CommitValidationError::ReplaceFileBaseRevisionMismatch {
                inode_id: InodeId(42),
                expected: RevisionNo(16),
                actual: Some(RevisionNo(17)),
            }
        );
    }

    #[test]
    fn build_commit_plan_rejects_restore_revision_missing_source_revision() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-restore-missing-source".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(52),
            ops: vec![CommitOp::RestoreRevision {
                inode_id: InodeId(42),
                base_revision: RevisionNo(5),
                restore_from_revision: RevisionNo(2),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(52)),
                Precondition::InodeRevisionIs {
                    inode_id: InodeId(42),
                    revision: RevisionNo(5),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(42),
                },
            ],
        };

        let error = build_commit_plan(
            &request,
            &validation_context_with_metadata_at_seq(
                999,
                ChangeSeq(52),
                restore_revision_metadata_state(),
            ),
        )
        .expect_err("missing restore source should be rejected");

        assert_eq!(
            error,
            CommitValidationError::RestoreRevisionSourceMissing {
                inode_id: InodeId(42),
                restore_from_revision: RevisionNo(2),
            }
        );
    }

    #[test]
    fn build_commit_plan_rejects_rename_target_name_collision() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-rename-collision".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(52),
            ops: vec![CommitOp::Rename {
                inode_id: InodeId(42),
                new_parent_inode: InodeId(7),
                new_display_name: "archive.txt".to_owned(),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(52)),
                Precondition::ChildNameAbsent {
                    parent_inode: InodeId(7),
                    name_key: "archive.txt".to_owned(),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(42),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(7),
                },
            ],
        };

        let error = build_commit_plan(
            &request,
            &validation_context_with_metadata_at_seq(
                999,
                ChangeSeq(52),
                rename_collision_metadata_state(),
            ),
        )
        .expect_err("rename collision should be rejected");

        assert_eq!(
            error,
            CommitValidationError::RenameTargetNameCollision {
                parent_inode: InodeId(7),
                name_key: "archive.txt".to_owned(),
                child_inode: InodeId(88),
            }
        );
    }

    #[test]
    fn build_commit_plan_rejects_directory_rename_cycle() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-rename-cycle".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(52),
            ops: vec![CommitOp::Rename {
                inode_id: InodeId(7),
                new_parent_inode: InodeId(9),
                new_display_name: "docs-moved".to_owned(),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(52)),
                Precondition::ChildNameAbsent {
                    parent_inode: InodeId(9),
                    name_key: "docs-moved".to_owned(),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(7),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(9),
                },
            ],
        };

        let error = build_commit_plan(
            &request,
            &validation_context_with_metadata_at_seq(
                999,
                ChangeSeq(52),
                rename_cycle_metadata_state(),
            ),
        )
        .expect_err("rename cycle should be rejected");

        assert_eq!(
            error,
            CommitValidationError::RenameWouldCycleDirectory {
                inode_id: InodeId(7),
                new_parent_inode: InodeId(9),
            }
        );
    }

    #[test]
    fn build_commit_plan_rejects_restore_revision_under_active_subtree_tombstone() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-restore-under-delete".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(52),
            ops: vec![CommitOp::RestoreRevision {
                inode_id: InodeId(42),
                base_revision: RevisionNo(5),
                restore_from_revision: RevisionNo(3),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(52)),
                Precondition::InodeRevisionIs {
                    inode_id: InodeId(42),
                    revision: RevisionNo(5),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(42),
                },
            ],
        };

        let error = build_commit_plan(
            &request,
            &validation_context_with_metadata_at_seq(
                999,
                ChangeSeq(52),
                restore_revision_covered_metadata_state(),
            ),
        )
        .expect_err("restore under tombstone should be rejected");

        assert_eq!(
            error,
            CommitValidationError::RestoreRevisionUnderSubtreeTombstone {
                inode_id: InodeId(42),
                root_inode: InodeId(7),
                tombstone_seq: ChangeSeq(52),
            }
        );
    }

    #[test]
    fn build_commit_plan_rejects_delete_subtree_for_file_root() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-delete-file-root".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::DeleteSubtree {
                root_inode: InodeId(42),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(41)),
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(42),
                },
            ],
        };

        let error = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, delete_root_metadata_state()),
        )
        .expect_err("file root should be rejected");

        assert_eq!(
            error,
            CommitValidationError::DeleteSubtreeRootNotDirectory {
                root_inode: InodeId(42),
                actual_kind: InodeKind::File,
            }
        );
    }

    #[test]
    fn build_commit_plan_rejects_create_under_active_subtree_tombstone() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-create-under-delete".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(7),
                display_name: "new.txt".to_owned(),
                content_manifest_digest: "sha256:new-v1".to_owned(),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(41)),
                Precondition::ChildNameAbsent {
                    parent_inode: InodeId(7),
                    name_key: "new.txt".to_owned(),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(7),
                },
            ],
        };

        let error = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, delete_covered_parent_metadata_state()),
        )
        .expect_err("create under tombstone should be rejected");

        assert_eq!(
            error,
            CommitValidationError::CreateUnderSubtreeTombstone {
                parent_inode: InodeId(7),
                root_inode: InodeId(7),
                tombstone_seq: ChangeSeq(41),
            }
        );
    }

    #[test]
    fn build_commit_plan_rejects_stale_writer_token() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-3".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(7),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::DeleteSubtree {
                root_inode: InodeId(2),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };

        let error = build_commit_plan(&request, &validation_context(1_000))
            .expect_err("stale writer should be rejected");

        assert_eq!(
            error,
            CommitValidationError::StaleWriterFenceToken {
                active: FenceToken(8),
                requested: FenceToken(7),
            }
        );
    }

    #[test]
    fn build_commit_plan_rejects_expired_lease() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-4".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::DeleteSubtree {
                root_inode: InodeId(2),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };

        let error = build_commit_plan(&request, &validation_context(1_001))
            .expect_err("expired lease should be rejected");

        assert_eq!(
            error,
            CommitValidationError::LeaseExpired {
                lease_expires_at_ms: 1_000,
                now_ms: 1_001,
            }
        );
    }

    #[test]
    fn build_commit_plan_requires_explicit_head_seq_precondition() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-5".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::DeleteSubtree {
                root_inode: InodeId(2),
            }],
            preconditions: Vec::new(),
        };

        let error = build_commit_plan(&request, &validation_context(1_000))
            .expect_err("missing head precondition should be rejected");

        assert_eq!(
            error,
            CommitValidationError::MissingHeadSeqPrecondition {
                expected: ChangeSeq(41),
            }
        );
    }

    #[test]
    fn prepare_commit_head_publish_advances_seq_and_next_inode_id() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-create-dir".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(2),
                display_name: "drafts".to_owned(),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(41)),
                Precondition::ChildNameAbsent {
                    parent_inode: InodeId(2),
                    name_key: "drafts".to_owned(),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(2),
                },
            ],
        };
        let context =
            validation_context_with_metadata(999, create_parent_metadata_state(InodeId(2)));
        let plan = build_commit_plan(&request, &context).expect("valid plan");

        let prepared =
            prepare_commit_head_publish(&context.head, &plan, "loon-core-test").expect("prepare");

        assert_eq!(prepared.object_key, namespace_head("ns-1"));
        assert_eq!(prepared.resulting_head.seq, ChangeSeq(42));
        assert_eq!(prepared.resulting_head.next_inode_id, InodeId(502));
        assert!(prepared
            .checked_invariants
            .contains(&"head_publish_requires_durable_wal".to_owned()));
    }

    #[test]
    fn publish_commit_head_compare_and_swap_writes_new_head() {
        let temp_dir = TestDir::new("commit-head-publish");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
        let initial_envelope = HeadStateEnvelope::from_state(
            ControlObjectKind::NamespaceHead,
            "loon-core-test",
            validation_context(999).head.clone(),
        )
        .expect("build initial head envelope");
        let head_key = namespace_head("ns-1");
        let initial_bytes =
            serde_json::to_vec(&initial_envelope).expect("encode initial head envelope");
        let initial_metadata = store
            .put_if_absent(&head_key, &initial_bytes)
            .expect("seed current head");
        let etag = initial_metadata.etag.expect("seeded head should have etag");
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-create-dir".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(2),
                display_name: "drafts".to_owned(),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(41)),
                Precondition::ChildNameAbsent {
                    parent_inode: InodeId(2),
                    name_key: "drafts".to_owned(),
                },
                Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(2),
                },
            ],
        };
        let context =
            validation_context_with_metadata(999, create_parent_metadata_state(InodeId(2)));
        let plan = build_commit_plan(&request, &context).expect("valid plan");
        let prepared =
            prepare_commit_head_publish(&context.head, &plan, "loon-core-test").expect("prepare");

        publish_commit_head(&store, &etag, &prepared).expect("publish head");

        let stored_bytes = store
            .get(&head_key, None)
            .expect("read published head")
            .expect("published head should exist");
        let stored: HeadStateEnvelope =
            serde_json::from_slice(&stored_bytes).expect("decode published head");

        assert_eq!(stored.state.seq, ChangeSeq(42));
        assert_eq!(stored.state.next_inode_id, InodeId(502));
    }

    #[test]
    fn build_commit_plan_accepts_ordered_multi_op_create_then_child_create() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-multi-create".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![
                CommitOp::CreateDir {
                    parent_inode: InodeId(2),
                    display_name: "drafts".to_owned(),
                },
                CommitOp::CreateFile {
                    parent_inode: InodeId(501),
                    display_name: "note.txt".to_owned(),
                    content_manifest_digest: "sha256:note-v1".to_owned(),
                },
            ],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };

        let plan = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, create_parent_metadata_state(InodeId(2))),
        )
        .expect("ordered multi-op create should validate");

        assert_eq!(plan.next_seq, ChangeSeq(42));
        assert_eq!(plan.allocated_inode_ids, vec![InodeId(501), InodeId(502)]);
        assert_eq!(plan.resulting_next_inode_id, InodeId(503));
    }

    #[test]
    fn build_commit_plan_rejects_duplicate_child_name_create_in_one_request() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-multi-collision".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![
                CommitOp::CreateFile {
                    parent_inode: InodeId(2),
                    display_name: "note.txt".to_owned(),
                    content_manifest_digest: "sha256:note-a".to_owned(),
                },
                CommitOp::CreateDir {
                    parent_inode: InodeId(2),
                    display_name: "note.txt".to_owned(),
                },
            ],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };

        let error = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, create_parent_metadata_state(InodeId(2))),
        )
        .expect_err("second create should see same-request name collision");

        assert_eq!(
            error,
            CommitValidationError::CreateChildNameCollision {
                parent_inode: InodeId(2),
                name_key: "note.txt".to_owned(),
                child_inode: InodeId(501),
            }
        );
    }

    #[test]
    fn build_commit_plan_rejects_double_replace_in_one_request() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-double-replace".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![
                CommitOp::ReplaceFile {
                    inode_id: InodeId(42),
                    base_revision: RevisionNo(7),
                    content_manifest_digest: "sha256:report-v8".to_owned(),
                },
                CommitOp::ReplaceFile {
                    inode_id: InodeId(42),
                    base_revision: RevisionNo(7),
                    content_manifest_digest: "sha256:report-v9".to_owned(),
                },
            ],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };

        let error = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, replace_metadata_state()),
        )
        .expect_err("second replace should see advanced same-request revision");

        assert_eq!(
            error,
            CommitValidationError::ReplaceFileBaseRevisionMismatch {
                inode_id: InodeId(42),
                expected: RevisionNo(7),
                actual: Some(RevisionNo(8)),
            }
        );
    }

    #[test]
    fn build_commit_plan_rejects_rename_plus_create_into_same_slot() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-rename-then-create".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![
                CommitOp::Rename {
                    inode_id: InodeId(42),
                    new_parent_inode: InodeId(7),
                    new_display_name: "archive.txt".to_owned(),
                },
                CommitOp::CreateFile {
                    parent_inode: InodeId(7),
                    display_name: "archive.txt".to_owned(),
                    content_manifest_digest: "sha256:archive".to_owned(),
                },
            ],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };

        let error = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, rename_metadata_state()),
        )
        .expect_err("create should see rename-applied slot collision");

        assert_eq!(
            error,
            CommitValidationError::CreateChildNameCollision {
                parent_inode: InodeId(7),
                name_key: "archive.txt".to_owned(),
                child_inode: InodeId(42),
            }
        );
    }

    #[test]
    fn build_commit_plan_rejects_restore_plus_replace_on_same_inode() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-restore-then-replace".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(52),
            ops: vec![
                CommitOp::RestoreRevision {
                    inode_id: InodeId(42),
                    base_revision: RevisionNo(5),
                    restore_from_revision: RevisionNo(3),
                },
                CommitOp::ReplaceFile {
                    inode_id: InodeId(42),
                    base_revision: RevisionNo(5),
                    content_manifest_digest: "sha256:report-v6".to_owned(),
                },
            ],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(52))],
        };

        let error = build_commit_plan(
            &request,
            &validation_context_with_metadata_at_seq(
                999,
                ChangeSeq(52),
                restore_revision_metadata_state(),
            ),
        )
        .expect_err("replace should see same-request restored revision head");

        assert_eq!(
            error,
            CommitValidationError::ReplaceFileBaseRevisionMismatch {
                inode_id: InodeId(42),
                expected: RevisionNo(5),
                actual: Some(RevisionNo(6)),
            }
        );
    }

    #[test]
    fn build_commit_plan_rejects_delete_plus_descendant_mutation_in_one_request() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-delete-then-mutate".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![
                CommitOp::DeleteSubtree {
                    root_inode: InodeId(7),
                },
                CommitOp::ReplaceFile {
                    inode_id: InodeId(42),
                    base_revision: RevisionNo(1),
                    content_manifest_digest: "sha256:report-v2".to_owned(),
                },
            ],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };

        let error = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, delete_root_metadata_state()),
        )
        .expect_err("descendant mutation should see same-request tombstone");

        assert_eq!(
            error,
            CommitValidationError::ReplaceFileUnderSubtreeTombstone {
                inode_id: InodeId(42),
                root_inode: InodeId(7),
                tombstone_seq: ChangeSeq(42),
            }
        );
    }

    fn validation_context(now_ms: u64) -> CommitValidationContext {
        validation_context_with_metadata(now_ms, MetadataState::default())
    }

    fn validation_context_with_metadata(
        now_ms: u64,
        metadata_state: MetadataState,
    ) -> CommitValidationContext {
        validation_context_with_metadata_at_seq(now_ms, ChangeSeq(41), metadata_state)
    }

    fn validation_context_with_metadata_at_seq(
        now_ms: u64,
        head_seq: ChangeSeq,
        metadata_state: MetadataState,
    ) -> CommitValidationContext {
        CommitValidationContext {
            head: HeadState {
                namespace_id: NamespaceId::from("ns-1"),
                seq: head_seq,
                active_fence_token: FenceToken(8),
                next_inode_id: InodeId(501),
                snapshot_hint_seq: Some(ChangeSeq(40)),
                retention_floor_seq: ChangeSeq(40),
            },
            lease: LeaseState {
                namespace_id: NamespaceId::from("ns-1"),
                holder_id: "writer-a".to_owned(),
                fence_token: FenceToken(8),
                lease_expires_at_ms: 1_000,
            },
            now_ms,
            metadata_state,
        }
    }

    fn create_parent_metadata_state(parent_inode: InodeId) -> MetadataState {
        MetadataState {
            inodes: vec![InodeRecord {
                inode_id: parent_inode,
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            }],
            direntries: Vec::new(),
            revisions: Vec::new(),
            subtree_tombstones: Vec::new(),
        }
    }

    fn create_collision_metadata_state() -> MetadataState {
        MetadataState {
            inodes: vec![
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(42),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(17),
                },
            ],
            direntries: vec![DirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "note.txt".to_owned(),
                display_name: "note.txt".to_owned(),
                child_inode_id: InodeId(42),
                bind_seq: ChangeSeq(41),
                bind_op_index: 0,
            }],
            revisions: vec![RevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(17),
                revision_op_index: 0,
                content_manifest_digest: "sha256:existing-note".to_owned(),
            }],
            subtree_tombstones: Vec::new(),
        }
    }

    fn replace_metadata_state() -> MetadataState {
        MetadataState {
            inodes: vec![InodeRecord {
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(12),
            }],
            direntries: Vec::new(),
            revisions: vec![RevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(7),
                committed_seq: ChangeSeq(41),
                revision_op_index: 0,
                content_manifest_digest: "sha256:manifest".to_owned(),
            }],
            subtree_tombstones: Vec::new(),
        }
    }

    fn replace_stale_revision_metadata_state() -> MetadataState {
        MetadataState {
            inodes: vec![InodeRecord {
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(12),
            }],
            direntries: Vec::new(),
            revisions: vec![
                RevisionRecord {
                    inode_id: InodeId(42),
                    revision_no: RevisionNo(16),
                    committed_seq: ChangeSeq(35),
                    revision_op_index: 0,
                    content_manifest_digest: "sha256:report-v16".to_owned(),
                },
                RevisionRecord {
                    inode_id: InodeId(42),
                    revision_no: RevisionNo(17),
                    committed_seq: ChangeSeq(41),
                    revision_op_index: 0,
                    content_manifest_digest: "sha256:report-v17".to_owned(),
                },
            ],
            subtree_tombstones: Vec::new(),
        }
    }

    fn delete_root_metadata_state() -> MetadataState {
        MetadataState {
            inodes: vec![
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(7),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(5),
                },
                InodeRecord {
                    inode_id: InodeId(42),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(17),
                },
            ],
            direntries: vec![
                DirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "docs".to_owned(),
                    display_name: "docs".to_owned(),
                    child_inode_id: InodeId(7),
                    bind_seq: ChangeSeq(5),
                    bind_op_index: 0,
                },
                DirentryRecord {
                    parent_inode_id: InodeId(7),
                    name_key: "report.txt".to_owned(),
                    display_name: "report.txt".to_owned(),
                    child_inode_id: InodeId(42),
                    bind_seq: ChangeSeq(17),
                    bind_op_index: 0,
                },
            ],
            revisions: vec![RevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(17),
                revision_op_index: 0,
                content_manifest_digest: "sha256:report-v1".to_owned(),
            }],
            subtree_tombstones: Vec::new(),
        }
    }

    fn restore_revision_metadata_state() -> MetadataState {
        MetadataState {
            inodes: vec![
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(42),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(9),
                },
            ],
            direntries: vec![DirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "report.txt".to_owned(),
                display_name: "report.txt".to_owned(),
                child_inode_id: InodeId(42),
                bind_seq: ChangeSeq(9),
                bind_op_index: 0,
            }],
            revisions: vec![
                RevisionRecord {
                    inode_id: InodeId(42),
                    revision_no: RevisionNo(3),
                    committed_seq: ChangeSeq(17),
                    revision_op_index: 0,
                    content_manifest_digest: "sha256:report-v3".to_owned(),
                },
                RevisionRecord {
                    inode_id: InodeId(42),
                    revision_no: RevisionNo(5),
                    committed_seq: ChangeSeq(52),
                    revision_op_index: 0,
                    content_manifest_digest: "sha256:report-v5".to_owned(),
                },
            ],
            subtree_tombstones: Vec::new(),
        }
    }

    fn rename_metadata_state() -> MetadataState {
        MetadataState {
            inodes: vec![
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(7),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(5),
                },
                InodeRecord {
                    inode_id: InodeId(42),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(17),
                },
            ],
            direntries: vec![
                DirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "docs".to_owned(),
                    display_name: "docs".to_owned(),
                    child_inode_id: InodeId(7),
                    bind_seq: ChangeSeq(5),
                    bind_op_index: 0,
                },
                DirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "report.txt".to_owned(),
                    display_name: "report.txt".to_owned(),
                    child_inode_id: InodeId(42),
                    bind_seq: ChangeSeq(17),
                    bind_op_index: 0,
                },
            ],
            revisions: vec![RevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(5),
                committed_seq: ChangeSeq(52),
                revision_op_index: 0,
                content_manifest_digest: "sha256:report-v5".to_owned(),
            }],
            subtree_tombstones: Vec::new(),
        }
    }

    fn rename_collision_metadata_state() -> MetadataState {
        let mut metadata_state = rename_metadata_state();
        metadata_state.inodes.push(InodeRecord {
            inode_id: InodeId(88),
            inode_kind: InodeKind::File,
            created_seq: ChangeSeq(21),
        });
        metadata_state.direntries.push(DirentryRecord {
            parent_inode_id: InodeId(7),
            name_key: "archive.txt".to_owned(),
            display_name: "archive.txt".to_owned(),
            child_inode_id: InodeId(88),
            bind_seq: ChangeSeq(21),
            bind_op_index: 0,
        });
        metadata_state
    }

    fn rename_cycle_metadata_state() -> MetadataState {
        MetadataState {
            inodes: vec![
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(7),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(5),
                },
                InodeRecord {
                    inode_id: InodeId(9),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(8),
                },
            ],
            direntries: vec![
                DirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "docs".to_owned(),
                    display_name: "docs".to_owned(),
                    child_inode_id: InodeId(7),
                    bind_seq: ChangeSeq(5),
                    bind_op_index: 0,
                },
                DirentryRecord {
                    parent_inode_id: InodeId(7),
                    name_key: "archive".to_owned(),
                    display_name: "archive".to_owned(),
                    child_inode_id: InodeId(9),
                    bind_seq: ChangeSeq(8),
                    bind_op_index: 0,
                },
            ],
            revisions: Vec::new(),
            subtree_tombstones: Vec::new(),
        }
    }

    fn delete_covered_parent_metadata_state() -> MetadataState {
        let mut metadata_state = delete_root_metadata_state();
        metadata_state
            .subtree_tombstones
            .push(SubtreeTombstoneRecord {
                root_inode_id: InodeId(7),
                tombstone_seq: ChangeSeq(41),
                tombstone_op_index: 0,
            });
        metadata_state
    }

    fn restore_revision_covered_metadata_state() -> MetadataState {
        MetadataState {
            inodes: vec![
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(7),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(5),
                },
                InodeRecord {
                    inode_id: InodeId(42),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(9),
                },
            ],
            direntries: vec![
                DirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "docs".to_owned(),
                    display_name: "docs".to_owned(),
                    child_inode_id: InodeId(7),
                    bind_seq: ChangeSeq(5),
                    bind_op_index: 0,
                },
                DirentryRecord {
                    parent_inode_id: InodeId(7),
                    name_key: "report.txt".to_owned(),
                    display_name: "report.txt".to_owned(),
                    child_inode_id: InodeId(42),
                    bind_seq: ChangeSeq(9),
                    bind_op_index: 0,
                },
            ],
            revisions: vec![
                RevisionRecord {
                    inode_id: InodeId(42),
                    revision_no: RevisionNo(3),
                    committed_seq: ChangeSeq(17),
                    revision_op_index: 0,
                    content_manifest_digest: "sha256:report-v3".to_owned(),
                },
                RevisionRecord {
                    inode_id: InodeId(42),
                    revision_no: RevisionNo(5),
                    committed_seq: ChangeSeq(52),
                    revision_op_index: 0,
                    content_manifest_digest: "sha256:report-v5".to_owned(),
                },
            ],
            subtree_tombstones: vec![SubtreeTombstoneRecord {
                root_inode_id: InodeId(7),
                tombstone_seq: ChangeSeq(52),
                tombstone_op_index: 0,
            }],
        }
    }

    #[derive(Debug)]
    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(label: &str) -> Self {
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "loondb-core-{label}-{}-{stamp}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
