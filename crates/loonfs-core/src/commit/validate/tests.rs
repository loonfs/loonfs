//! Commit validation tests calling the steps directly over an in-memory
//! metadata view.
//!
//! These tests are kept in the crate because the steps are internal.

use super::super::NewChild;
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
    next_public_ordinal, CommitId, CommitSeq, ContentRef, DisplayName, InodeId, InodeKind, NameKey,
    NamespaceId, RevisionNo, WriterEpoch,
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
    let copy_name = test_display_name("copy.txt");
    let error = validate_steps(4_200, &context, async |session| {
        session
            .create(
                InodeId(3),
                ROOT_INODE_ID,
                &copy_name,
                NewChild::File(content_ref("content-2")),
            )
            .await
    })
    .await
    .expect_err("inode 3 already has a parent");
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

#[tokio::test]
async fn a_create_under_an_ancestor_tombstone_reports_corruption() {
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
}

/// Checks that a create under a directory whose ancestry is disconnected
/// fails validation with `InodeMissing`. A base compaction of the bindings
/// group leaves inodes disconnected below an expired deletion, and this test
/// builds the same state by unbinding `/docs` without recording a tombstone.
#[tokio::test]
async fn a_create_under_a_disconnected_ancestry_reports_a_missing_inode() {
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
