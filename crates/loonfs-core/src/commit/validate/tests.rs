//! Commit validation tests using the production plan-building entry point
//! over an in-memory metadata view.
//!
//! These exercise the planning IR directly, so they live in the crate rather
//! than in an integration test: the IR is internal, and callers reach it only
//! by planning a mutation request.

#![allow(clippy::panic)]

use super::super::{CommitIr, CommitOp, PlannedOp};
use super::validate_commit_for_publish;
use crate::commit::{
    materialize_commit, CommitFingerprint, CommitPlan, CommitValidationError, InodeAllocator,
};
use crate::error::{CoreError, ErrorCode};
use crate::metadata::{InMemoryMetadataView, MetadataState};
use loonfs_api::wire::control::{HeadState, WriterBlock};
use loonfs_api::wire::wal::WalDelta;
use loonfs_api::{
    AttributeKey, AttributeRevisionNo, AttributeValue, Attributes, ChangeSeq, CommitId, ContentId,
    ContentRef, DisplayName, InodeId, InodeKind, NameKey, NamespaceId, RevisionNo, WriterEpoch,
};

fn test_display_name(value: impl AsRef<str>) -> DisplayName {
    DisplayName::parse(value.as_ref()).expect("valid display name")
}

fn content_ref(seed: &str) -> ContentRef {
    ContentRef::blob_v1(ContentId::generate(), seed.as_bytes())
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

/// Operations with no race checks of their own: these tests drive the
/// validation loop, not the planner that emits the checks.
fn planned(ops: Vec<CommitOp>) -> Vec<PlannedOp> {
    ops.into_iter().map(PlannedOp::unchecked).collect()
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
        deleted_direntry: None,
    }]
}

fn metadata_state_after(sequences: &[Vec<WalDelta>]) -> MetadataState {
    let mut state = MetadataState::default().apply_committed_wal_deltas(
        ChangeSeq(0),
        4_200,
        &[WalDelta::CreateInode {
            delta_index: 0,
            inode_id: InodeId(1),
            inode_kind: InodeKind::Directory,
        }],
    );

    for (index, deltas) in sequences.iter().enumerate() {
        state = state.apply_committed_wal_deltas(
            ChangeSeq(u64::try_from(index + 1).expect("seq")),
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
        state: Default::default(),
    };
    TestValidationContext {
        head,
        metadata_state,
    }
}

async fn build_commit_plan(
    request: &CommitIr,
    committed_at_ms: u64,
    context: &TestValidationContext<'_>,
) -> Result<CommitPlan, CommitValidationError> {
    let accepted_rows = MetadataState::default();
    let mut allocator = InodeAllocator::new(context.head.next_inode_id);
    let mut allocation = allocator.begin_candidate();
    for planned in &request.ops {
        let assigned = match &planned.op {
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
    let result = validate_commit_for_publish(
        request,
        committed_at_ms,
        &context.head,
        InMemoryMetadataView::in_memory(context.metadata_state, None, context.head.seq),
        &accepted_rows,
    )
    .await;

    let validated = result.map_err(|error| match error {
        CoreError::CommitValidation(error) => error,
        error => panic!("unexpected validation dependency error: {error}"),
    })?;
    let resulting_next_inode_id = allocator
        .commit_candidate(allocation)
        .expect("commit test allocation");
    Ok(validated.prepare(request.clone(), test_fingerprint(), resulting_next_inode_id))
}

/// Every attribute update carries its own revision guard, whether or not the
/// caller stated an expectation. A plan built against a revision that is no
/// longer current is rejected here, not merged.
#[tokio::test]
async fn a_stale_attribute_base_revision_is_rejected_by_the_updates_own_guard() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_append_attributes(0, InodeId(2), 1, &[("owner", "ada")]),
        wal_append_attributes(0, InodeId(2), 2, &[("owner", "grace")]),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(3), InodeId(3));
    let request = CommitIr {
        namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
        commit_id: CommitId::parse("stale-attributes").expect("valid commit id"),
        actor: loonfs_test_support::test_actor(),
        writer_epoch: WriterEpoch(1),
        // The op alone, with no explicit precondition: the guard rides the
        // operation itself.
        ops: planned(vec![CommitOp::UpdateAttributes {
            inode_id: InodeId(2),
            base_attributes_revision_no: AttributeRevisionNo(1),
            attributes: test_attributes(&[("owner", "hopper")]),
        }]),
        message: None,
    };

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

/// An inode with no attribute row is at revision 0, so a first write states
/// zero — and a second first write of the same inode conflicts.
#[tokio::test]
async fn a_first_attribute_write_states_revision_zero() {
    let metadata_state = metadata_state_after(&[wal_create_directory(
        0,
        InodeId(2),
        InodeId(1),
        "docs".to_owned(),
    )]);
    let context = validation_context(&metadata_state, ChangeSeq(1), InodeId(3));
    let request = |base: u64| CommitIr {
        namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
        commit_id: CommitId::parse("first-attributes").expect("valid commit id"),
        actor: loonfs_test_support::test_actor(),
        writer_epoch: WriterEpoch(1),
        ops: planned(vec![CommitOp::UpdateAttributes {
            inode_id: InodeId(2),
            base_attributes_revision_no: AttributeRevisionNo(base),
            attributes: test_attributes(&[("owner", "ada")]),
        }]),
        message: None,
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

/// Attributes belong to the resource, so an inode nothing binds has none to
/// write.
#[tokio::test]
async fn an_attribute_update_of_a_missing_inode_is_rejected() {
    let metadata_state = metadata_state_after(&[]);
    let context = validation_context(&metadata_state, ChangeSeq(0), InodeId(2));
    let request = CommitIr {
        namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
        commit_id: CommitId::parse("missing-attributes").expect("valid commit id"),
        actor: loonfs_test_support::test_actor(),
        writer_epoch: WriterEpoch(1),
        ops: planned(vec![CommitOp::UpdateAttributes {
            inode_id: InodeId(9),
            base_attributes_revision_no: AttributeRevisionNo(0),
            attributes: test_attributes(&[("owner", "ada")]),
        }]),
        message: None,
    };

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("the inode does not exist");
    assert!(
        matches!(
            error,
            CommitValidationError::UpdateAttributesInodeMissing {
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
    let request = CommitIr {
        namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
        commit_id: CommitId::parse("stale-revision").expect("valid commit id"),
        actor: loonfs_test_support::test_actor(),
        writer_epoch: WriterEpoch(1),
        ops: planned(vec![CommitOp::ReplaceFile {
            inode_id: InodeId(3),
            base_revision_no: RevisionNo(1),
            content_ref: content_ref("content-3"),
        }]),
        message: None,
    };

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("stale revision");
    assert!(matches!(
        error,
        CommitValidationError::ReplaceFileBaseRevisionMismatch {
            inode_id: InodeId(3),
            expected: RevisionNo(1),
            actual: Some(RevisionNo(2)),
        }
    ));
}

#[tokio::test]
async fn failed_multi_op_plan_uses_preview_without_mutating_base_metadata() {
    let metadata_state = metadata_state_after(&[]);
    let context = validation_context(&metadata_state, ChangeSeq(0), InodeId(2));
    let request = CommitIr {
        namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
        commit_id: CommitId::parse("preview-rollback").expect("valid commit id"),
        actor: loonfs_test_support::test_actor(),
        writer_epoch: WriterEpoch(1),
        ops: planned(vec![
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
        ]),
        message: None,
    };

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("late op fails");
    assert!(matches!(
        error,
        CommitValidationError::ReplaceFileInodeMissing {
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

/// The rows here are deliberately corrupt: the tombstone lands without the
/// unbind a real delete emits with it, so `readme.txt` stays bound under a
/// tombstoned directory. No client history produces that — planning resolves
/// through visible bindings, and a visible inode is one no tombstone covers
/// — so the guards fire only on self-contradicting stored state and classify
/// as corruption.
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
        &CommitIr {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: CommitId::parse("create-under-tombstone").expect("valid commit id"),
            actor: loonfs_test_support::test_actor(),
            writer_epoch: WriterEpoch(1),
            ops: planned(vec![CommitOp::CreateFile {
                child_inode_id: InodeId(4),
                parent_inode_id: InodeId(2),
                display_name: test_display_name("new.txt"),
                content_ref: content_ref("content-2"),
            }]),
            message: None,
        },
        4_200,
        &context,
    )
    .await
    .expect_err("create under tombstone");
    assert!(matches!(
        create_error,
        CommitValidationError::CreateUnderSubtreeTombstone {
            parent_inode_id: InodeId(2),
            ..
        }
    ));
    assert_eq!(
        CoreError::from(create_error).code(),
        ErrorCode::NamespaceCorrupt
    );

    let replace_error = build_commit_plan(
        &CommitIr {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: CommitId::parse("replace-under-tombstone").expect("valid commit id"),
            actor: loonfs_test_support::test_actor(),
            writer_epoch: WriterEpoch(1),
            ops: planned(vec![CommitOp::ReplaceFile {
                inode_id: InodeId(3),
                base_revision_no: RevisionNo(1),
                content_ref: content_ref("content-2"),
            }]),
            message: None,
        },
        4_200,
        &context,
    )
    .await
    .expect_err("replace under tombstone");
    assert!(matches!(
        replace_error,
        CommitValidationError::ReplaceFileUnderSubtreeTombstone {
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
    let request = CommitIr {
        namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
        commit_id: CommitId::parse("restore-missing-inode").expect("valid commit id"),
        actor: loonfs_test_support::test_actor(),
        writer_epoch: WriterEpoch(1),
        ops: planned(vec![CommitOp::RestoreRevision {
            inode_id: InodeId(99),
            source_revision_no: RevisionNo(1),
            base_revision_no: RevisionNo(1),
        }]),
        message: None,
    };

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("restore missing inode");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionInodeMissing {
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
    let request = CommitIr {
        namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
        commit_id: CommitId::parse("restore-non-file").expect("valid commit id"),
        actor: loonfs_test_support::test_actor(),
        writer_epoch: WriterEpoch(1),
        ops: planned(vec![CommitOp::RestoreRevision {
            inode_id: InodeId(2),
            source_revision_no: RevisionNo(1),
            base_revision_no: RevisionNo(1),
        }]),
        message: None,
    };

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("restore non-file");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionInodeNotFile {
            inode_id: InodeId(2),
            actual_kind: InodeKind::Directory,
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
        &CommitIr {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: CommitId::parse("restore-stale-base").expect("valid commit id"),
            actor: loonfs_test_support::test_actor(),
            writer_epoch: WriterEpoch(1),
            ops: planned(vec![CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(1),
                base_revision_no: RevisionNo(1),
            }]),
            message: None,
        },
        4_200,
        &context,
    )
    .await
    .expect_err("restore stale base");
    assert!(matches!(
        stale_base,
        CommitValidationError::RestoreRevisionBaseRevisionMismatch {
            inode_id: InodeId(3),
            expected: RevisionNo(1),
            actual: Some(RevisionNo(2)),
        }
    ));

    let missing_source = build_commit_plan(
        &CommitIr {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: CommitId::parse("restore-missing-source").expect("valid commit id"),
            actor: loonfs_test_support::test_actor(),
            writer_epoch: WriterEpoch(1),
            ops: planned(vec![CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(99),
                base_revision_no: RevisionNo(2),
            }]),
            message: None,
        },
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
    let request = CommitIr {
        namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
        commit_id: CommitId::parse("restore-same-request-source").expect("valid commit id"),
        actor: loonfs_test_support::test_actor(),
        writer_epoch: WriterEpoch(1),
        ops: planned(vec![
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
        ]),
        message: None,
    };
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

    let request = CommitIr {
        namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
        commit_id: CommitId::parse("restore-after-restore-same-request").expect("valid commit id"),
        actor: loonfs_test_support::test_actor(),
        writer_epoch: WriterEpoch(1),
        ops: planned(vec![
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
        ]),
        message: None,
    };
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

/// Same corrupt shape as the create/replace guards above: a tombstone with
/// no matching unbind leaves a live binding under it, which is the only way
/// this guard can fire.
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
        &CommitIr {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: CommitId::parse("restore-under-tombstone").expect("valid commit id"),
            actor: loonfs_test_support::test_actor(),
            writer_epoch: WriterEpoch(1),
            ops: planned(vec![CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(1),
                base_revision_no: RevisionNo(1),
            }]),
            message: None,
        },
        4_200,
        &context,
    )
    .await
    .expect_err("restore under a covering tombstone");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionUnderSubtreeTombstone {
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
        revision_no: RevisionNo(u64::MAX),
        content_ref: content_ref("content-max"),
    };
    let metadata_state = MetadataState::default()
        .apply_committed_wal_deltas(
            ChangeSeq(0),
            4_200,
            &[WalDelta::CreateInode {
                delta_index: 0,
                inode_id: InodeId(1),
                inode_kind: InodeKind::Directory,
            }],
        )
        .apply_committed_wal_deltas(ChangeSeq(1), 4_200, &deltas);
    let context = validation_context(&metadata_state, ChangeSeq(1), InodeId(3));
    let request = CommitIr {
        namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
        commit_id: CommitId::parse("restore-overflow").expect("valid commit id"),
        actor: loonfs_test_support::test_actor(),
        writer_epoch: WriterEpoch(1),
        ops: planned(vec![CommitOp::RestoreRevision {
            inode_id: InodeId(2),
            source_revision_no: RevisionNo(u64::MAX),
            base_revision_no: RevisionNo(u64::MAX),
        }]),
        message: None,
    };

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("restore overflow");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionOverflow {
            inode_id: InodeId(2),
            base_revision_no: RevisionNo(u64::MAX),
        }
    ));
}
