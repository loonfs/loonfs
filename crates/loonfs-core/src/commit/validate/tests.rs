//! Commit validation tests driving the operation validator directly over an
//! in-memory metadata view.
//!
//! These tests live in the crate because the operation types are internal.

#![allow(clippy::panic)]

use super::super::{CommitOp, ResolvedBinding, ValidatedCommitPlan};
use super::checks::{validate_ops, CommitNumbering};
use super::error::CommitOperand;
use super::view::PublishValidationView;
use crate::commit::{
    materialize_commit, CommitFingerprint, CommitPlan, CommitValidationError, InodeAllocator,
};
use crate::error::{CoreError, ErrorCode};
use crate::metadata::{InMemoryMetadataView, MetadataState};
use loonfs_api::wire::control::{HeadState, NamespaceStatus, WriterBlock};
use loonfs_api::wire::{manifest::DeletedDirentry, wal::WalDelta};
use loonfs_api::{
    next_public_ordinal, AttributeKey, AttributeRevisionNo, AttributeValue, Attributes, ChangeSeq,
    CommitId, ContentRef, DisplayName, InodeId, InodeKind, NameKey, NamespaceId, RevisionNo,
    WriterEpoch, MAX_PUBLIC_INTEGER,
};

fn test_display_name(value: impl AsRef<str>) -> DisplayName {
    DisplayName::parse(value.as_ref()).expect("valid display name")
}

fn content_ref(seed: &str) -> ContentRef {
    loonfs_test_support::ids::content_ref(seed.as_bytes())
}

fn commit_id_for_seq(seq: ChangeSeq) -> CommitId {
    CommitId::parse(format!("c_validation_{}", seq.0)).expect("commit id")
}

fn test_attributes(entries: &[(&str, &str)]) -> Attributes {
    Attributes::new(
        entries
            .iter()
            .map(|(key, value)| {
                (
                    AttributeKey::parse(key).expect("valid attribute key"),
                    AttributeValue::parse(value).expect("valid attribute value"),
                )
            })
            .collect(),
    )
    .expect("valid attribute map")
}

fn wal_append_attributes(
    delta_index: u32,
    inode_id: InodeId,
    revision: u64,
    entries: &[(&str, &str)],
) -> Vec<WalDelta> {
    vec![WalDelta::AppendAttributesRevision {
        delta_index,
        inode_id,
        attributes_revision_no: AttributeRevisionNo(revision),
        attributes: test_attributes(entries),
    }]
}

fn planned(ops: Vec<CommitOp>) -> Vec<CommitOp> {
    ops
}

fn test_fingerprint() -> CommitFingerprint {
    CommitFingerprint::new_unchecked("v1:sha256:test".to_owned())
}

fn wal_create_directory(
    delta_index: u32,
    inode_id: InodeId,
    parent_inode_id: InodeId,
    display_name: String,
) -> Vec<WalDelta> {
    vec![
        WalDelta::CreateInode {
            delta_index,
            inode_id,
            inode_kind: InodeKind::Directory,
        },
        WalDelta::BindDirentry {
            delta_index: delta_index.saturating_add(1),
            parent_inode_id,
            name_key: NameKey::parse(loonfs_api::name_key_for_display_name(&display_name))
                .expect("derived name key"),
            display_name: test_display_name(display_name),
            child_inode_id: inode_id,
        },
    ]
}

fn wal_create_file(
    delta_index: u32,
    inode_id: InodeId,
    parent_inode_id: InodeId,
    display_name: String,
    content_ref: ContentRef,
) -> Vec<WalDelta> {
    vec![
        WalDelta::CreateInode {
            delta_index,
            inode_id,
            inode_kind: InodeKind::File,
        },
        WalDelta::BindDirentry {
            delta_index: delta_index.saturating_add(1),
            parent_inode_id,
            name_key: NameKey::parse(loonfs_api::name_key_for_display_name(&display_name))
                .expect("derived name key"),
            display_name: test_display_name(display_name),
            child_inode_id: inode_id,
        },
        WalDelta::AppendFileRevision {
            delta_index: delta_index.saturating_add(2),
            inode_id,
            revision_no: RevisionNo(1),
            content_ref,
        },
    ]
}

fn wal_append_revision(
    delta_index: u32,
    inode_id: InodeId,
    revision_no: RevisionNo,
    content_ref: ContentRef,
) -> Vec<WalDelta> {
    vec![WalDelta::AppendFileRevision {
        delta_index,
        inode_id,
        revision_no,
        content_ref,
    }]
}

fn wal_tombstone(delta_index: u32, root_inode_id: InodeId) -> Vec<WalDelta> {
    vec![WalDelta::TombstoneSubtree {
        delta_index,
        root_inode_id,
        deleted_direntry: DeletedDirentry {
            parent_inode_id: InodeId(1),
            name_key: NameKey::parse("docs").expect("valid name key"),
            display_name: test_display_name("docs"),
        },
    }]
}

fn metadata_state_after(sequences: &[Vec<WalDelta>]) -> MetadataState {
    let mut state = MetadataState::default().apply_committed_wal_deltas(
        ChangeSeq(0),
        &commit_id_for_seq(ChangeSeq(0)),
        &loonfs_test_support::test_actor(),
        4_200,
        &[WalDelta::CreateInode {
            delta_index: 0,
            inode_id: InodeId(1),
            inode_kind: InodeKind::Directory,
        }],
    );

    for (index, deltas) in sequences.iter().enumerate() {
        let seq = ChangeSeq(u64::try_from(index + 1).expect("seq"));
        state = state.apply_committed_wal_deltas(
            seq,
            &commit_id_for_seq(seq),
            &loonfs_test_support::test_actor(),
            4_200,
            deltas,
        )
    }

    state
}

struct TestValidationContext<'a> {
    head: HeadState,
    metadata_state: &'a MetadataState,
}

fn validation_context(
    metadata_state: &MetadataState,
    seq: ChangeSeq,
    next_inode_id: InodeId,
) -> TestValidationContext<'_> {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let head = HeadState {
        content_store_id: loonfs_api::ContentStoreId::generate(),
        created_at_ms: 1_000,
        fork_basis: None,
        namespace_id: namespace_id.clone(),
        seq,
        head_commit_id: CommitId::parse("c_00000000000000000000000000000000").expect("commit id"),
        writer_epoch: WriterEpoch(1),
        writer: Some(WriterBlock {
            writer_id: "writer-a".to_owned(),
            acquired_at_ms: 1_000,
        }),
        next_inode_id,
        visible_wal_tip: None,
        recent_segments: Vec::new(),
        status: NamespaceStatus::Active {},
    };
    TestValidationContext {
        head,
        metadata_state,
    }
}

async fn build_commit_plan(
    ops: &[CommitOp],
    committed_at_ms: u64,
    context: &TestValidationContext<'_>,
) -> Result<CommitPlan, CommitValidationError> {
    let accepted_rows = MetadataState::default();
    let mut allocator = InodeAllocator::new(context.head.next_inode_id);
    let mut allocation = allocator.begin_candidate();
    for op in ops {
        let assigned = match op {
            CommitOp::CreateDirectory { child_inode_id, .. }
            | CommitOp::CreateFile { child_inode_id, .. } => Some(*child_inode_id),
            _ => None,
        };
        if let Some(assigned) = assigned {
            assert_eq!(
                allocation.allocate().expect("test inode allocation"),
                assigned,
                "test create operations must carry their planned inode ids"
            );
        }
    }
    let committed_seq = next_public_ordinal(context.head.seq.0)
        .map(ChangeSeq)
        .expect("test heads stay under the sequence cap");
    let commit_id = CommitId::parse("validated-commit").expect("valid commit id");
    let mut metadata_state = PublishValidationView::new(
        InMemoryMetadataView::in_memory(context.metadata_state, None, context.head.seq),
        &accepted_rows,
        committed_seq,
    );
    let mut numbering = CommitNumbering::default();
    let result = validate_ops(
        ops,
        &mut metadata_state,
        &mut numbering,
        &commit_id,
        &loonfs_test_support::test_actor(),
        committed_at_ms,
    )
    .await;

    let validated_ops = result.map_err(|error| match error {
        CoreError::CommitValidation(error) => error,
        error => panic!("unexpected validation dependency error: {error}"),
    })?;
    let resulting_next_inode_id = allocator
        .commit_candidate(allocation)
        .expect("commit test allocation");
    Ok(ValidatedCommitPlan {
        namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
        commit_id,
        actor: loonfs_test_support::test_actor(),
        writer_epoch: context.head.writer_epoch,
        message: None,
        semantic_identity: test_fingerprint(),
        apply_after_seq: context.head.seq,
        assigned_seq: committed_seq,
        validated_ops,
    }
    .finish(resulting_next_inode_id))
}

#[tokio::test]
async fn a_stale_attribute_base_revision_is_rejected_by_the_updates_own_guard() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_append_attributes(0, InodeId(2), 1, &[("owner", "ada")]),
        wal_append_attributes(0, InodeId(2), 2, &[("owner", "grace")]),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(3), InodeId(3));
    let request = planned(vec![CommitOp::UpdateAttributes {
        inode_id: InodeId(2),
        base_attributes_revision_no: AttributeRevisionNo(1),
        attributes: test_attributes(&[("owner", "hopper")]),
    }]);

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("the base revision is stale");
    assert!(
        matches!(
            error,
            CommitValidationError::UpdateAttributesBaseRevisionMismatch {
                inode_id: InodeId(2),
                expected: AttributeRevisionNo(1),
                actual: AttributeRevisionNo(2),
            }
        ),
        "{error:?}"
    );
    assert_eq!(
        CoreError::from(error).code(),
        ErrorCode::StaleAttributes,
        "the guard reports the attribute-conflict code"
    );
}

#[tokio::test]
async fn a_first_attribute_write_states_revision_zero() {
    let metadata_state = metadata_state_after(&[wal_create_directory(
        0,
        InodeId(2),
        InodeId(1),
        "docs".to_owned(),
    )]);
    let context = validation_context(&metadata_state, ChangeSeq(1), InodeId(3));
    let request = |base: u64| {
        planned(vec![CommitOp::UpdateAttributes {
            inode_id: InodeId(2),
            base_attributes_revision_no: AttributeRevisionNo(base),
            attributes: test_attributes(&[("owner", "ada")]),
        }])
    };

    build_commit_plan(&request(0), 4_200, &context)
        .await
        .expect("a first write states revision zero");
    let error = build_commit_plan(&request(1), 4_200, &context)
        .await
        .expect_err("nothing has written revision 1 yet");
    assert!(
        matches!(
            error,
            CommitValidationError::UpdateAttributesBaseRevisionMismatch {
                expected: AttributeRevisionNo(1),
                actual: AttributeRevisionNo(0),
                ..
            }
        ),
        "{error:?}"
    );
}

#[tokio::test]
async fn an_attribute_update_of_a_missing_inode_is_rejected() {
    let metadata_state = metadata_state_after(&[]);
    let context = validation_context(&metadata_state, ChangeSeq(0), InodeId(2));
    let request = planned(vec![CommitOp::UpdateAttributes {
        inode_id: InodeId(9),
        base_attributes_revision_no: AttributeRevisionNo(0),
        attributes: test_attributes(&[("owner", "ada")]),
    }]);

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("the inode does not exist");
    assert!(
        matches!(
            error,
            CommitValidationError::InodeMissing {
                operand: CommitOperand::AttributeTarget,
                inode_id: InodeId(9)
            }
        ),
        "{error:?}"
    );
}

#[tokio::test]
async fn stale_revision_precondition_is_rejected() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-2")),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(3), InodeId(4));
    let request = planned(vec![CommitOp::ReplaceFile {
        inode_id: InodeId(3),
        base_revision_no: RevisionNo(1),
        content_ref: content_ref("content-3"),
    }]);

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("stale revision");
    assert!(matches!(
        error,
        CommitValidationError::BaseRevisionMismatch {
            inode_id: InodeId(3),
            expected: RevisionNo(1),
            actual: Some(RevisionNo(2)),
        }
    ));
}

#[tokio::test]
async fn a_create_for_a_bound_name_is_rejected() {
    let metadata_state = metadata_state_after(&[wal_create_directory(
        0,
        InodeId(2),
        InodeId(1),
        "docs".to_owned(),
    )]);
    let context = validation_context(&metadata_state, ChangeSeq(1), InodeId(3));
    let request = planned(vec![CommitOp::CreateDirectory {
        child_inode_id: InodeId(3),
        parent_inode_id: InodeId(1),
        display_name: test_display_name("docs"),
    }]);

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("the name is already bound");
    assert!(matches!(
        error,
        CommitValidationError::NameTaken {
            operand: CommitOperand::CreateParent,
            parent_inode_id: InodeId(1),
            child_inode_id: InodeId(2),
            ..
        }
    ));
}

#[tokio::test]
async fn a_binding_precondition_for_an_unbound_name_is_rejected() {
    let metadata_state = metadata_state_after(&[wal_create_directory(
        0,
        InodeId(2),
        InodeId(1),
        "docs".to_owned(),
    )]);
    let context = validation_context(&metadata_state, ChangeSeq(1), InodeId(3));
    let request = vec![CommitOp::Rename {
        source_binding: ResolvedBinding {
            parent_inode_id: InodeId(1),
            name_key: NameKey::parse("missing.txt").expect("valid name key"),
            display_name: test_display_name("missing.txt"),
            child_inode_id: InodeId(9),
            bind_seq: ChangeSeq(1),
            bind_delta_index: 0,
        },
        inode_id: InodeId(2),
        new_parent_inode_id: InodeId(1),
        new_display_name: test_display_name("renamed"),
    }];

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("the observed binding is gone");
    assert!(matches!(
        error,
        CommitValidationError::BindingPreconditionMissing {
            parent_inode_id: InodeId(1),
            ..
        }
    ));
}

#[tokio::test]
async fn failed_multi_op_plan_uses_preview_without_mutating_base_metadata() {
    let metadata_state = metadata_state_after(&[]);
    let context = validation_context(&metadata_state, ChangeSeq(0), InodeId(2));
    let request = planned(vec![
        CommitOp::CreateDirectory {
            child_inode_id: InodeId(2),
            parent_inode_id: InodeId(1),
            display_name: test_display_name("docs"),
        },
        CommitOp::CreateFile {
            child_inode_id: InodeId(3),
            parent_inode_id: InodeId(2),
            display_name: test_display_name("readme.txt"),
            content_ref: content_ref("content-1"),
        },
        CommitOp::ReplaceFile {
            inode_id: InodeId(99),
            base_revision_no: RevisionNo(1),
            content_ref: content_ref("content-2"),
        },
    ]);

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("late op fails");
    assert!(matches!(
        error,
        CommitValidationError::InodeMissing {
            operand: CommitOperand::ReplaceTarget,
            inode_id: InodeId(99)
        }
    ));
    assert!(metadata_state
        .visible_child(
            InodeId(1),
            &NameKey::parse("docs").expect("valid name key"),
            ChangeSeq(1),
        )
        .is_none());
}

#[tokio::test]
async fn create_and_replace_under_ancestor_tombstone_report_corruption() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_tombstone(0, InodeId(2)),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(3), InodeId(4));

    let create_error = build_commit_plan(
        &planned(vec![CommitOp::CreateFile {
            child_inode_id: InodeId(4),
            parent_inode_id: InodeId(2),
            display_name: test_display_name("new.txt"),
            content_ref: content_ref("content-2"),
        }]),
        4_200,
        &context,
    )
    .await
    .expect_err("create under tombstone");
    assert!(matches!(
        create_error,
        CommitValidationError::TargetUnderSubtreeTombstone {
            operand: CommitOperand::CreateParent,
            inode_id: InodeId(2),
            ..
        }
    ));
    assert_eq!(
        CoreError::from(create_error).code(),
        ErrorCode::NamespaceCorrupt
    );

    let replace_error = build_commit_plan(
        &planned(vec![CommitOp::ReplaceFile {
            inode_id: InodeId(3),
            base_revision_no: RevisionNo(1),
            content_ref: content_ref("content-2"),
        }]),
        4_200,
        &context,
    )
    .await
    .expect_err("replace under tombstone");
    assert!(matches!(
        replace_error,
        CommitValidationError::TargetUnderSubtreeTombstone {
            operand: CommitOperand::ReplaceTarget,
            inode_id: InodeId(3),
            ..
        }
    ));
    assert_eq!(
        CoreError::from(replace_error).code(),
        ErrorCode::NamespaceCorrupt
    );
}

#[tokio::test]
async fn restore_revision_validation_rejects_missing_inode() {
    let metadata_state = metadata_state_after(&[wal_create_directory(
        0,
        InodeId(2),
        InodeId(1),
        "docs".to_owned(),
    )]);
    let context = validation_context(&metadata_state, ChangeSeq(1), InodeId(3));
    let request = planned(vec![CommitOp::RestoreRevision {
        inode_id: InodeId(99),
        source_revision_no: RevisionNo(1),
        base_revision_no: RevisionNo(1),
    }]);

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("restore missing inode");
    assert!(matches!(
        error,
        CommitValidationError::InodeMissing {
            operand: CommitOperand::RestoreTarget,
            inode_id: InodeId(99),
        }
    ));
}

#[tokio::test]
async fn restore_revision_validation_rejects_non_file_target() {
    let metadata_state = metadata_state_after(&[wal_create_directory(
        0,
        InodeId(2),
        InodeId(1),
        "docs".to_owned(),
    )]);
    let context = validation_context(&metadata_state, ChangeSeq(1), InodeId(3));
    let request = planned(vec![CommitOp::RestoreRevision {
        inode_id: InodeId(2),
        source_revision_no: RevisionNo(1),
        base_revision_no: RevisionNo(1),
    }]);

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("restore non-file");
    assert!(matches!(
        error,
        CommitValidationError::InodeWrongKind {
            operand: CommitOperand::RestoreTarget,
            inode_id: InodeId(2),
            expected: InodeKind::File,
            actual: InodeKind::Directory,
        }
    ));
}

#[tokio::test]
async fn restore_revision_validation_rejects_stale_or_missing_source_revision() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-2")),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(3), InodeId(4));

    let stale_base = build_commit_plan(
        &planned(vec![CommitOp::RestoreRevision {
            inode_id: InodeId(3),
            source_revision_no: RevisionNo(1),
            base_revision_no: RevisionNo(1),
        }]),
        4_200,
        &context,
    )
    .await
    .expect_err("restore stale base");
    assert!(matches!(
        &stale_base,
        CommitValidationError::BaseRevisionMismatch {
            inode_id: InodeId(3),
            expected: RevisionNo(1),
            actual: Some(RevisionNo(2)),
        }
    ));
    assert_eq!(
        stale_base.to_string(),
        "base revision mismatch for inode `3`: expected revision 1, found revision 2"
    );

    let missing_source = build_commit_plan(
        &planned(vec![CommitOp::RestoreRevision {
            inode_id: InodeId(3),
            source_revision_no: RevisionNo(99),
            base_revision_no: RevisionNo(2),
        }]),
        4_200,
        &context,
    )
    .await
    .expect_err("restore missing source");
    assert!(matches!(
        missing_source,
        CommitValidationError::RestoreRevisionSourceRevisionMissing {
            inode_id: InodeId(3),
            source_revision_no: RevisionNo(99),
        }
    ));
}

#[tokio::test]
async fn restore_revision_can_reference_revision_created_earlier_in_same_request() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(2), InodeId(4));

    // The restore must resolve to the very object the replace attached, so
    // the expectation is that reference itself.
    let expected = content_ref("content-2");
    let request = planned(vec![
        CommitOp::ReplaceFile {
            inode_id: InodeId(3),
            base_revision_no: RevisionNo(1),
            content_ref: expected.clone(),
        },
        CommitOp::RestoreRevision {
            inode_id: InodeId(3),
            source_revision_no: RevisionNo(2),
            base_revision_no: RevisionNo(2),
        },
    ]);
    let plan = build_commit_plan(&request, 4_200, &context)
        .await
        .expect("replace then restore in same request should validate");
    let materialized = materialize_commit(plan, 4_200);
    assert!(matches!(
        &materialized.deltas[1].wal_delta,
        WalDelta::AppendFileRevision {
            content_ref,
            ..
        } if *content_ref == expected
    ));
}

#[tokio::test]
async fn restore_revision_can_reference_restore_created_earlier_in_same_request() {
    // Each restore resolves to an object an earlier revision already named,
    // so the expectations are those references themselves.
    let expected = content_ref("content-1");
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            expected.clone(),
        ),
        wal_append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-2")),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(3), InodeId(4));

    let request = planned(vec![
        CommitOp::RestoreRevision {
            inode_id: InodeId(3),
            source_revision_no: RevisionNo(1),
            base_revision_no: RevisionNo(2),
        },
        CommitOp::RestoreRevision {
            inode_id: InodeId(3),
            source_revision_no: RevisionNo(3),
            base_revision_no: RevisionNo(3),
        },
    ]);
    let plan = build_commit_plan(&request, 4_200, &context)
        .await
        .expect("restore then restore in same request should validate");
    let materialized = materialize_commit(plan, 4_200);
    assert!(matches!(
        &materialized.deltas[0].wal_delta,
        WalDelta::AppendFileRevision {
            content_ref,
            ..
        } if *content_ref == expected
    ));
    assert!(matches!(
        &materialized.deltas[1].wal_delta,
        WalDelta::AppendFileRevision {
            content_ref,
            ..
        } if *content_ref == expected
    ));
}

#[tokio::test]
async fn restore_revision_under_tombstoned_ancestor_reports_corruption() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_tombstone(0, InodeId(2)),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(3), InodeId(4));

    let error = build_commit_plan(
        &planned(vec![CommitOp::RestoreRevision {
            inode_id: InodeId(3),
            source_revision_no: RevisionNo(1),
            base_revision_no: RevisionNo(1),
        }]),
        4_200,
        &context,
    )
    .await
    .expect_err("restore under a covering tombstone");
    assert!(matches!(
        error,
        CommitValidationError::TargetUnderSubtreeTombstone {
            operand: CommitOperand::RestoreTarget,
            inode_id: InodeId(3),
            ..
        }
    ));
    assert_eq!(CoreError::from(error).code(), ErrorCode::NamespaceCorrupt);
}

#[tokio::test]
async fn restore_revision_overflow_is_rejected() {
    let mut deltas = wal_create_file(
        0,
        InodeId(2),
        InodeId(1),
        "overflow.txt".to_owned(),
        content_ref("content-max"),
    );
    deltas[2] = WalDelta::AppendFileRevision {
        delta_index: 2,
        inode_id: InodeId(2),
        revision_no: RevisionNo(MAX_PUBLIC_INTEGER),
        content_ref: content_ref("content-max"),
    };
    let metadata_state = MetadataState::default()
        .apply_committed_wal_deltas(
            ChangeSeq(0),
            &commit_id_for_seq(ChangeSeq(0)),
            &loonfs_test_support::test_actor(),
            4_200,
            &[WalDelta::CreateInode {
                delta_index: 0,
                inode_id: InodeId(1),
                inode_kind: InodeKind::Directory,
            }],
        )
        .apply_committed_wal_deltas(
            ChangeSeq(1),
            &commit_id_for_seq(ChangeSeq(1)),
            &loonfs_test_support::test_actor(),
            4_200,
            &deltas,
        );
    let context = validation_context(&metadata_state, ChangeSeq(1), InodeId(3));
    let request = planned(vec![CommitOp::RestoreRevision {
        inode_id: InodeId(2),
        source_revision_no: RevisionNo(MAX_PUBLIC_INTEGER),
        base_revision_no: RevisionNo(MAX_PUBLIC_INTEGER),
    }]);

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("restore overflow");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionOverflow {
            inode_id: InodeId(2),
            base_revision_no: RevisionNo(MAX_PUBLIC_INTEGER),
        }
    ));
}
