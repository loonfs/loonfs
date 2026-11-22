//! Commit validation tests calling the steps directly over an in-memory
//! metadata view.
//!
//! These tests are kept in the crate because the steps are internal.

use super::super::{NewChild, ResolvedBinding};
use super::error::CommitOperand;
use super::session::CommitValidationSession;
use crate::commit::engine::CommitCandidate;
use crate::commit::{CommitFingerprint, CommitPlan, CommitValidationError};
use crate::error::{CoreError, ErrorCode, Result};
use crate::metadata::{MetadataState, MetadataView};
use crate::namespace::state::NamespaceReadState;
use crate::path::write::CommitIntent;
use crate::segment::VerifiedMetadataSegments;
use loonfs_test_support::stores::MemoryStore;
use loonfs_types::format::control::{NamespaceStatus, WriterBlock};
use loonfs_types::format::{
    manifest::{DeletedBinding, DeltaPosition},
    wal::{WalCommitDelta, WalDelta},
};
use loonfs_types::ChecksumAlgorithm;
use loonfs_types::ROOT_INODE_ID;
use loonfs_types::{
    next_public_ordinal, AttributeKey, AttributeValue, Attributes, AttributesRevisionNo, CommitId,
    CommitSeq, ContentRef, DisplayName, InodeId, InodeKind, NameKey, NamespaceId, RevisionNo,
    WriterEpoch, MAX_PUBLIC_INTEGER,
};
use std::assert_matches;

fn test_display_name(value: impl AsRef<str>) -> DisplayName {
    DisplayName::parse(value.as_ref()).expect("valid display name")
}

fn content_ref(seed: &str) -> ContentRef {
    loonfs_test_support::ids::content_ref(seed.as_bytes())
}

fn commit_id_for_seq(seq: CommitSeq) -> CommitId {
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
        attributes_revision_no: AttributesRevisionNo(revision),
        attributes: test_attributes(entries),
    }]
}

fn test_fingerprint() -> CommitFingerprint {
    loonfs_test_support::ids::commit_fingerprint("test")
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
            name_key: NameKey::parse(loonfs_types::name_key_for_display_name(&display_name))
                .expect("derived name key"),
            display_name: test_display_name(display_name),
            child_inode_id: inode_id,
            child_kind: loonfs_types::InodeKind::Directory,
            child_created_by: loonfs_types::ActorId::loonfs(),
            child_created_at_ms: 4_200,
            revision: None,
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
            name_key: NameKey::parse(loonfs_types::name_key_for_display_name(&display_name))
                .expect("derived name key"),
            display_name: test_display_name(display_name),
            child_inode_id: inode_id,
            child_kind: loonfs_types::InodeKind::File,
            child_created_by: loonfs_types::ActorId::loonfs(),
            child_created_at_ms: 4_200,
            revision: Some(crate::test_support::revision_summary()),
        },
        crate::test_support::file_revision_delta(
            delta_index.saturating_add(2),
            inode_id,
            RevisionNo(1),
            content_ref,
            None,
        ),
    ]
}

fn wal_append_revision(
    delta_index: u32,
    inode_id: InodeId,
    revision_no: RevisionNo,
    content_ref: ContentRef,
) -> Vec<WalDelta> {
    vec![crate::test_support::file_revision_delta(
        delta_index,
        inode_id,
        revision_no,
        content_ref,
        None,
    )]
}

fn wal_tombstone(delta_index: u32, root_inode_id: InodeId) -> Vec<WalDelta> {
    vec![WalDelta::TombstoneSubtree {
        delta_index,
        root_inode_id,
        root_kind: loonfs_types::InodeKind::Directory,
        deleted_binding: DeletedBinding {
            parent_inode_id: ROOT_INODE_ID,
            name_key: NameKey::parse("docs").expect("valid name key"),
            display_name: test_display_name("docs"),
        },
    }]
}

fn metadata_state_after(sequences: &[Vec<WalDelta>]) -> MetadataState {
    let mut state = MetadataState::default().apply_committed_wal_deltas(
        CommitSeq(0),
        &commit_id_for_seq(CommitSeq(0)),
        &loonfs_test_support::test_actor(),
        4_200,
        &[WalDelta::CreateInode {
            delta_index: 0,
            inode_id: ROOT_INODE_ID,
            inode_kind: InodeKind::Directory,
        }],
    );

    for (index, deltas) in sequences.iter().enumerate() {
        let seq = CommitSeq(u64::try_from(index + 1).expect("seq"));
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
    head: NamespaceReadState,
    metadata_state: &'a MetadataState,
}

fn validation_context(metadata_state: &MetadataState, seq: CommitSeq) -> TestValidationContext<'_> {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let head = NamespaceReadState {
        created_at_ms: 1_000,
        created_by: loonfs_test_support::test_actor(),
        fork_basis: None,
        pool_id: loonfs_test_support::ids::numbered_pool_id(1),
        namespace_id: namespace_id.clone(),
        seq,
        writer_epoch: WriterEpoch(1),
        writer: Some(WriterBlock {
            writer_id: loonfs_types::WriterId::parse("writer-a").expect("writer id"),
            acquired_at_ms: 1_000,
        }),
        wal_no: loonfs_types::WalNo(0),
        folded_wal_no: loonfs_types::WalNo(0),
        retention_floor_seq: CommitSeq(0),
        status: NamespaceStatus::Active {},
    };
    TestValidationContext {
        head,
        metadata_state,
    }
}

fn committed_seq(context: &TestValidationContext<'_>) -> CommitSeq {
    next_public_ordinal(context.head.seq.0)
        .map(CommitSeq)
        .expect("test heads stay under the sequence cap")
}

fn validated_commit_id() -> CommitId {
    CommitId::parse("validated-commit").expect("valid commit id")
}

/// Runs `steps` on a new commit's validation session and returns the deltas
/// that they wrote.
async fn validate_steps(
    committed_at_ms: u64,
    context: &TestValidationContext<'_>,
    steps: impl AsyncFnOnce(&mut CommitValidationSession<'_, MemoryStore>) -> Result<()>,
) -> Result<Vec<WalCommitDelta>> {
    let accepted_rows = MetadataState::default();
    let base = crate::wal::ProjectedWalTail::from_rows(context.metadata_state.clone());
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let candidate = CommitCandidate::new(CommitIntent {
        commit_id: validated_commit_id(),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        operations: Vec::new(),
        preconditions: Vec::new(),
    });
    let store = MemoryStore::new(ChecksumAlgorithm::Crc64nvme);
    let manifest = VerifiedMetadataSegments::initial(&store);
    let mut session = CommitValidationSession::new(
        MetadataView::in_memory(&manifest, &base, None, context.head.seq),
        &accepted_rows,
        &namespace_id,
        &candidate,
        committed_seq(context),
        committed_at_ms,
    );
    steps(&mut session).await?;
    Ok(session.take_deltas())
}

async fn build_commit_plan(
    committed_at_ms: u64,
    context: &TestValidationContext<'_>,
    steps: impl AsyncFnOnce(&mut CommitValidationSession<'_, MemoryStore>) -> Result<()>,
) -> std::result::Result<CommitPlan, CommitValidationError> {
    let deltas = validate_steps(committed_at_ms, context, steps)
        .await
        .map_err(|error| match error {
            CoreError::CommitValidation(error) => error,
            error => panic!("unexpected validation dependency error: {error}"),
        })?;
    Ok(CommitPlan {
        namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
        commit_id: validated_commit_id(),
        actor_id: loonfs_test_support::test_actor(),
        writer_epoch: context.head.writer_epoch,
        message: None,
        merge: None,
        semantic_identity: test_fingerprint(),
        apply_after_seq: context.head.seq,
        assigned_seq: committed_seq(context),
        committed_at_ms,
        deltas,
        appended: Vec::new(),
        inline_content: Vec::new(),
    })
}

#[tokio::test]
async fn a_stale_attribute_base_revision_is_rejected_by_the_updates_own_precondition() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), ROOT_INODE_ID, "docs".to_owned()),
        wal_append_attributes(0, InodeId(2), 1, &[("owner", "ada")]),
        wal_append_attributes(0, InodeId(2), 2, &[("owner", "grace")]),
    ]);
    let context = validation_context(&metadata_state, CommitSeq(3));
    let attributes = test_attributes(&[("owner", "hopper")]);

    let error = build_commit_plan(4_200, &context, async |session| {
        session
            .update_attributes(InodeId(2), AttributesRevisionNo(1), &attributes)
            .await
    })
    .await
    .expect_err("the base revision is stale");
    assert_matches!(
        error,
        CommitValidationError::UpdateAttributesBaseRevisionMismatch {
            inode_id: InodeId(2),
            expected: AttributesRevisionNo(1),
            actual: Some(AttributesRevisionNo(2)),
        }
    );
    assert_eq!(
        CoreError::from(error).code(),
        ErrorCode::StaleAttributes,
        "the precondition reports the attribute-conflict code"
    );
}

#[tokio::test]
async fn a_first_attribute_write_states_revision_zero() {
    let metadata_state = metadata_state_after(&[wal_create_directory(
        0,
        InodeId(2),
        ROOT_INODE_ID,
        "docs".to_owned(),
    )]);
    let context = validation_context(&metadata_state, CommitSeq(1));
    let attributes = test_attributes(&[("owner", "ada")]);
    let update = |base: u64| {
        let attributes = &attributes;
        async move |session: &mut CommitValidationSession<'_, MemoryStore>| {
            session
                .update_attributes(InodeId(2), AttributesRevisionNo(base), attributes)
                .await
        }
    };

    build_commit_plan(4_200, &context, update(0))
        .await
        .expect("a first write states revision zero");
    let error = build_commit_plan(4_200, &context, update(1))
        .await
        .expect_err("nothing has written revision 1 yet");
    assert_matches!(
        error,
        CommitValidationError::UpdateAttributesBaseRevisionMismatch {
            expected: AttributesRevisionNo(1),
            actual: Some(AttributesRevisionNo(0)),
            ..
        }
    );
}

#[tokio::test]
async fn an_attribute_update_of_a_missing_inode_is_rejected() {
    let metadata_state = metadata_state_after(&[]);
    let context = validation_context(&metadata_state, CommitSeq(0));
    let attributes = test_attributes(&[("owner", "ada")]);

    let error = build_commit_plan(4_200, &context, async |session| {
        session
            .update_attributes(InodeId(9), AttributesRevisionNo(0), &attributes)
            .await
    })
    .await
    .expect_err("the inode does not exist");
    assert_matches!(
        error,
        CommitValidationError::InodeMissing {
            operand: CommitOperand::AttributeTarget,
            inode_id: InodeId(9)
        }
    );
}

#[tokio::test]
async fn stale_revision_precondition_is_rejected() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), ROOT_INODE_ID, "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-2")),
    ]);
    let context = validation_context(&metadata_state, CommitSeq(3));
    let replacement = content_ref("content-3");

    let error = build_commit_plan(4_200, &context, async |session| {
        session
            .replace_file(InodeId(3), RevisionNo(1), &replacement, None)
            .await
    })
    .await
    .expect_err("stale revision");
    assert_matches!(
        error,
        CommitValidationError::BaseRevisionMismatch {
            inode_id: InodeId(3),
            expected: RevisionNo(1),
            actual: Some(RevisionNo(2)),
        }
    );
}

#[tokio::test]
async fn a_create_for_a_bound_name_is_rejected() {
    let metadata_state = metadata_state_after(&[wal_create_directory(
        0,
        InodeId(2),
        ROOT_INODE_ID,
        "docs".to_owned(),
    )]);
    let context = validation_context(&metadata_state, CommitSeq(1));
    let display_name = test_display_name("docs");

    let error = build_commit_plan(4_200, &context, async |session| {
        session
            .create(
                InodeId(3),
                ROOT_INODE_ID,
                &display_name,
                NewChild::Directory,
            )
            .await
    })
    .await
    .expect_err("the name is already bound");
    assert_matches!(
        error,
        CommitValidationError::NameTaken {
            operand: CommitOperand::CreateParent,
            parent_inode_id: ROOT_INODE_ID,
            child_inode_id: InodeId(2),
            ..
        }
    );
}

#[tokio::test]
async fn a_bind_for_an_already_bound_child_is_refused_as_corruption() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), ROOT_INODE_ID, "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
    ]);
    let context = validation_context(&metadata_state, CommitSeq(2));
    let docs_binding = ResolvedBinding {
        parent_inode_id: ROOT_INODE_ID,
        name_key: NameKey::parse("docs").expect("valid name key"),
        display_name: test_display_name("docs"),
        child_inode_id: InodeId(2),
        bind_position: loonfs_types::format::manifest::DeltaPosition {
            seq: CommitSeq(1),
            delta_index: 1,
        },
    };
    let copy_name = test_display_name("copy.txt");
    let moved_name = test_display_name("moved.txt");
    let reused_inode = validate_steps(4_200, &context, async |session| {
        session
            .create(
                InodeId(3),
                ROOT_INODE_ID,
                &copy_name,
                NewChild::File(content_ref("content-2")),
            )
            .await
    })
    .await;
    let unbinds_another_child = validate_steps(4_200, &context, async |session| {
        session
            .rename(InodeId(3), &docs_binding, ROOT_INODE_ID, &moved_name)
            .await
    })
    .await;

    for result in [reused_inode, unbinds_another_child] {
        let error = result.expect_err("inode 3 already has a parent");
        assert_matches!(error, CoreError::NamespaceCorrupt(_));
        assert_eq!(
            error.to_string(),
            format!(
                "namespace corrupt: a commit binds inode `{}`, which is already bound as \
                 `readme.txt` under parent inode `{}`",
                InodeId(3),
                InodeId(2)
            )
        );
    }
}

#[tokio::test]
async fn a_binding_precondition_for_an_unbound_name_is_rejected() {
    let metadata_state = metadata_state_after(&[wal_create_directory(
        0,
        InodeId(2),
        ROOT_INODE_ID,
        "docs".to_owned(),
    )]);
    let context = validation_context(&metadata_state, CommitSeq(1));
    let source_binding = ResolvedBinding {
        parent_inode_id: ROOT_INODE_ID,
        name_key: NameKey::parse("missing.txt").expect("valid name key"),
        display_name: test_display_name("missing.txt"),
        child_inode_id: InodeId(9),
        bind_position: loonfs_types::format::manifest::DeltaPosition {
            seq: CommitSeq(1),
            delta_index: 0,
        },
    };
    let renamed = test_display_name("renamed");

    let error = build_commit_plan(4_200, &context, async |session| {
        session
            .rename(InodeId(2), &source_binding, ROOT_INODE_ID, &renamed)
            .await
    })
    .await
    .expect_err("the observed binding is gone");
    assert_matches!(
        error,
        CommitValidationError::BindingPreconditionMissing {
            parent_inode_id: ROOT_INODE_ID,
            ..
        }
    );
}

#[tokio::test]
async fn create_and_replace_under_ancestor_tombstone_report_corruption() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), ROOT_INODE_ID, "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_tombstone(0, InodeId(2)),
    ]);
    let context = validation_context(&metadata_state, CommitSeq(3));
    let new_name = test_display_name("new.txt");
    let replacement = content_ref("content-2");

    let create_error = build_commit_plan(4_200, &context, async |session| {
        session
            .create(
                InodeId(4),
                InodeId(2),
                &new_name,
                NewChild::File(content_ref("content-2")),
            )
            .await
    })
    .await
    .expect_err("create under tombstone");
    assert_matches!(
        create_error,
        CommitValidationError::TargetUnderSubtreeTombstone {
            operand: CommitOperand::CreateParent,
            inode_id: InodeId(2),
            ..
        }
    );
    assert_eq!(
        CoreError::from(create_error).code(),
        ErrorCode::NamespaceCorrupt
    );

    let replace_error = build_commit_plan(4_200, &context, async |session| {
        session
            .replace_file(InodeId(3), RevisionNo(1), &replacement, None)
            .await
    })
    .await
    .expect_err("replace under tombstone");
    assert_matches!(
        replace_error,
        CommitValidationError::TargetUnderSubtreeTombstone {
            operand: CommitOperand::ReplaceTarget,
            inode_id: InodeId(3),
            ..
        }
    );
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
        ROOT_INODE_ID,
        "docs".to_owned(),
    )]);
    let context = validation_context(&metadata_state, CommitSeq(1));
    let error = build_commit_plan(4_200, &context, async |session| {
        session
            .restore_revision(InodeId(99), RevisionNo(1), RevisionNo(1))
            .await
    })
    .await
    .expect_err("restore missing inode");
    assert_matches!(
        error,
        CommitValidationError::InodeMissing {
            operand: CommitOperand::RestoreTarget,
            inode_id: InodeId(99),
        }
    );
}

#[tokio::test]
async fn restore_revision_validation_rejects_non_file_target() {
    let metadata_state = metadata_state_after(&[wal_create_directory(
        0,
        InodeId(2),
        ROOT_INODE_ID,
        "docs".to_owned(),
    )]);
    let context = validation_context(&metadata_state, CommitSeq(1));
    let error = build_commit_plan(4_200, &context, async |session| {
        session
            .restore_revision(InodeId(2), RevisionNo(1), RevisionNo(1))
            .await
    })
    .await
    .expect_err("restore non-file");
    assert_matches!(
        error,
        CommitValidationError::InodeWrongKind {
            operand: CommitOperand::RestoreTarget,
            inode_id: InodeId(2),
            expected: InodeKind::File,
            actual: InodeKind::Directory,
        }
    );
}

#[tokio::test]
async fn restore_revision_validation_rejects_stale_or_missing_source_revision() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), ROOT_INODE_ID, "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-2")),
    ]);
    let context = validation_context(&metadata_state, CommitSeq(3));

    let stale_base = build_commit_plan(4_200, &context, async |session| {
        session
            .restore_revision(InodeId(3), RevisionNo(1), RevisionNo(1))
            .await
    })
    .await
    .expect_err("restore stale base");
    assert_matches!(
        &stale_base,
        CommitValidationError::BaseRevisionMismatch {
            inode_id: InodeId(3),
            expected: RevisionNo(1),
            actual: Some(RevisionNo(2)),
        }
    );
    assert_eq!(
        stale_base.to_string(),
        format!(
            "base revision mismatch for inode `{}`: expected revision 1, found revision 2",
            InodeId(3)
        )
    );

    let missing_source = build_commit_plan(4_200, &context, async |session| {
        session
            .restore_revision(InodeId(3), RevisionNo(99), RevisionNo(2))
            .await
    })
    .await
    .expect_err("restore missing source");
    assert_matches!(
        missing_source,
        CommitValidationError::RestoreRevisionSourceRevisionMissing {
            inode_id: InodeId(3),
            source_revision_no: RevisionNo(99),
        }
    );
}

#[tokio::test]
async fn restore_revision_can_reference_revision_created_earlier_in_same_request() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), ROOT_INODE_ID, "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
    ]);
    let context = validation_context(&metadata_state, CommitSeq(2));

    // The restore must resolve to the very object the replace attached, so
    // the expectation is that reference itself.
    let expected = content_ref("content-2");
    let plan = build_commit_plan(4_200, &context, async |session| {
        session
            .replace_file(InodeId(3), RevisionNo(1), &expected, None)
            .await?;
        session
            .restore_revision(InodeId(3), RevisionNo(2), RevisionNo(2))
            .await
    })
    .await
    .expect("replace then restore in same request should validate");
    assert_matches!(
        &plan.deltas[1].delta,
        WalDelta::AppendFileRevision {
            content_ref,
            ..
        } if *content_ref == expected
    );
}

#[tokio::test]
async fn restore_revision_can_reference_restore_created_earlier_in_same_request() {
    // Each restore resolves to an object an earlier revision already named,
    // so the expectations are those references themselves.
    let expected = content_ref("content-1");
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), ROOT_INODE_ID, "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            expected.clone(),
        ),
        wal_append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-2")),
    ]);
    let context = validation_context(&metadata_state, CommitSeq(3));

    let plan = build_commit_plan(4_200, &context, async |session| {
        session
            .restore_revision(InodeId(3), RevisionNo(1), RevisionNo(2))
            .await?;
        session
            .restore_revision(InodeId(3), RevisionNo(3), RevisionNo(3))
            .await
    })
    .await
    .expect("restore then restore in same request should validate");
    assert_matches!(
        &plan.deltas[0].delta,
        WalDelta::AppendFileRevision {
            content_ref,
            ..
        } if *content_ref == expected
    );
    assert_matches!(
        &plan.deltas[1].delta,
        WalDelta::AppendFileRevision {
            content_ref,
            ..
        } if *content_ref == expected
    );
}

#[tokio::test]
async fn restore_revision_under_tombstoned_ancestor_reports_corruption() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), ROOT_INODE_ID, "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_tombstone(0, InodeId(2)),
    ]);
    let context = validation_context(&metadata_state, CommitSeq(3));

    let error = build_commit_plan(4_200, &context, async |session| {
        session
            .restore_revision(InodeId(3), RevisionNo(1), RevisionNo(1))
            .await
    })
    .await
    .expect_err("restore under a covering tombstone");
    assert_matches!(
        error,
        CommitValidationError::TargetUnderSubtreeTombstone {
            operand: CommitOperand::RestoreTarget,
            inode_id: InodeId(3),
            ..
        }
    );
    assert_eq!(CoreError::from(error).code(), ErrorCode::NamespaceCorrupt);
}

/// Checks that a create under a directory whose ancestry is disconnected, and
/// a replace of a file below that directory, fail validation with
/// `InodeMissing`. A base compaction of the bindings group leaves inodes
/// disconnected below an expired deletion, and this test builds the same state
/// by unbinding `/docs` without recording a tombstone.
#[tokio::test]
async fn create_and_replace_under_a_disconnected_ancestry_report_a_missing_inode() {
    let mut unbind_docs = wal_delete_docs(DeltaPosition {
        seq: CommitSeq(1),
        delta_index: 1,
    });
    unbind_docs.truncate(1);
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), ROOT_INODE_ID, "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        unbind_docs,
    ]);
    let context = validation_context(&metadata_state, CommitSeq(3));
    let new_name = test_display_name("new.txt");
    let replacement = content_ref("content-2");

    let create_error = build_commit_plan(4_200, &context, async |session| {
        session
            .create(
                InodeId(4),
                InodeId(2),
                &new_name,
                NewChild::File(content_ref("content-2")),
            )
            .await
    })
    .await
    .expect_err("create under a disconnected directory");
    assert_matches!(
        create_error,
        CommitValidationError::InodeMissing {
            operand: CommitOperand::CreateParent,
            inode_id: InodeId(2),
        }
    );

    let replace_error = build_commit_plan(4_200, &context, async |session| {
        session
            .replace_file(InodeId(3), RevisionNo(1), &replacement, None)
            .await
    })
    .await
    .expect_err("replace below a disconnected directory");
    assert_matches!(
        replace_error,
        CommitValidationError::InodeMissing {
            operand: CommitOperand::ReplaceTarget,
            inode_id: InodeId(3),
        }
    );
}

#[tokio::test]
async fn restore_revision_overflow_is_rejected() {
    let mut deltas = wal_create_file(
        0,
        InodeId(2),
        ROOT_INODE_ID,
        "overflow.txt".to_owned(),
        content_ref("content-max"),
    );
    deltas[2] = crate::test_support::file_revision_delta(
        2,
        InodeId(2),
        RevisionNo(MAX_PUBLIC_INTEGER),
        content_ref("content-max"),
        None,
    );
    let metadata_state = MetadataState::default()
        .apply_committed_wal_deltas(
            CommitSeq(0),
            &commit_id_for_seq(CommitSeq(0)),
            &loonfs_test_support::test_actor(),
            4_200,
            &[WalDelta::CreateInode {
                delta_index: 0,
                inode_id: ROOT_INODE_ID,
                inode_kind: InodeKind::Directory,
            }],
        )
        .apply_committed_wal_deltas(
            CommitSeq(1),
            &commit_id_for_seq(CommitSeq(1)),
            &loonfs_test_support::test_actor(),
            4_200,
            &deltas,
        );
    let context = validation_context(&metadata_state, CommitSeq(1));
    let error = build_commit_plan(4_200, &context, async |session| {
        session
            .restore_revision(
                InodeId(2),
                RevisionNo(MAX_PUBLIC_INTEGER),
                RevisionNo(MAX_PUBLIC_INTEGER),
            )
            .await
    })
    .await
    .expect_err("restore overflow");
    assert_matches!(
        error,
        CommitValidationError::RevisionOverflow {
            inode_id: InodeId(2),
            base_revision_no: RevisionNo(MAX_PUBLIC_INTEGER),
        }
    );
}

/// Deletes `/docs` (inode 2), whose binding was published at `bound_at`:
/// unbinds it, then records the tombstone's `set` at delta index 1.
fn wal_delete_docs(bound_at: DeltaPosition) -> Vec<WalDelta> {
    let mut deltas = vec![WalDelta::UnbindDirentry {
        delta_index: 0,
        parent_inode_id: ROOT_INODE_ID,
        name_key: NameKey::parse("docs").expect("valid name key"),
        display_name: test_display_name("docs"),
        child_inode_id: InodeId(2),
        child_kind: InodeKind::Directory,
        child_created_by: loonfs_types::ActorId::loonfs(),
        child_created_at_ms: 4_200,
        target: bound_at,
    }];
    deltas.extend(wal_tombstone(1, InodeId(2)));
    deltas
}

/// A restore of a deleted item applies only while the deletion it names is its root's
/// latest tombstone event, so a stale request can't cancel a later deletion.
#[tokio::test]
async fn a_restore_of_a_deleted_item_applies_only_while_its_deletion_is_the_roots_latest_event() {
    let position = |seq, delta_index| DeltaPosition {
        seq: CommitSeq(seq),
        delta_index,
    };
    let deletion = position(2, 1);
    let docs = test_display_name("docs");
    // Restores inode 2 as `/docs` once for each deletion sequence in
    // `deletion_seqs`, in order.
    let restore_deleted_at = |deletion_seqs: Vec<u64>| {
        let docs = &docs;
        async move |session: &mut CommitValidationSession<'_, MemoryStore>| {
            for seq in deletion_seqs {
                session
                    .restore_deleted(InodeId(2), CommitSeq(seq), ROOT_INODE_ID, docs)
                    .await?;
            }
            Ok(())
        }
    };
    // A restore at sequence 3: the revoke, then the binding at delta 1.
    let mut restored = vec![WalDelta::RevokeSubtreeTombstone {
        delta_index: 0,
        root_inode_id: InodeId(2),
        target: deletion,
    }];
    restored.extend(
        wal_create_directory(0, InodeId(2), ROOT_INODE_ID, "docs".to_owned())
            .into_iter()
            .skip(1),
    );
    let not_deleted = CommitValidationError::RestoreDeletedTargetNotDeleted {
        inode_id: InodeId(2),
    };
    let cases = [
        (
            "restored twice in one commit",
            Vec::new(),
            vec![2, 2],
            not_deleted.clone(),
        ),
        (
            "already restored",
            vec![restored.clone()],
            vec![2],
            not_deleted,
        ),
        (
            "deleted again since",
            vec![restored, wal_delete_docs(position(3, 1))],
            vec![2],
            CommitValidationError::RestoreDeletedSequenceMismatch {
                inode_id: InodeId(2),
                requested_seq: CommitSeq(2),
                actual_deletion_seq: CommitSeq(4),
            },
        ),
    ];
    let history = |later: Vec<Vec<WalDelta>>| {
        let mut history = vec![
            wal_create_directory(0, InodeId(2), ROOT_INODE_ID, "docs".to_owned()),
            wal_delete_docs(position(1, 1)),
        ];
        history.extend(later);
        history
    };
    for (name, later, deletion_seqs, expected) in cases {
        let history = history(later);
        let metadata_state = metadata_state_after(&history);
        let head_seq = CommitSeq(u64::try_from(history.len()).expect("seq"));
        let context = validation_context(&metadata_state, head_seq);
        let error = build_commit_plan(4_200, &context, restore_deleted_at(deletion_seqs))
            .await
            .expect_err(name);
        assert_eq!(error, expected, "{name}");
        assert_eq!(
            CoreError::from(error).code(),
            ErrorCode::NotDeleted,
            "{name}"
        );
    }

    let metadata_state = metadata_state_after(&history(Vec::new()));
    let context = validation_context(&metadata_state, CommitSeq(2));
    let plan = build_commit_plan(4_200, &context, restore_deleted_at(vec![2]))
        .await
        .expect("restore the latest deletion");
    assert_matches!(
        plan.deltas.as_slice(),
        [
            WalCommitDelta {
                delta: WalDelta::RevokeSubtreeTombstone { target, .. },
                ..
            },
            WalCommitDelta {
                delta: WalDelta::BindDirentry { .. },
                ..
            },
        ] if *target == deletion
    );
}
