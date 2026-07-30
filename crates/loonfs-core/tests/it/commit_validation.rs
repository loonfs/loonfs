//! Commit precondition, name, binding, and content validation guards.

#![allow(clippy::panic)]
// These integration tests use panic in unexpected match arms for precise diagnostics.

use crate::common::mutation_split_support::*;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::{
    v0::{
        CommitOp, CommitOp as ApiCommitOp, CommitPrecondition, CommitRequest as ApiCommitRequest,
        ValidatedContentToken,
    },
    wire::control::{HeadState, WriterBlock},
    wire::wal::WalDelta,
    AbsolutePath, ChangeSeq, CommitId, ContentRef, DestinationBehavior, DisplayName, InodeId,
    InodeKind, NameKey, NamespaceId, RevisionNo, WriterEpoch,
};
use loonfs_core::commit::{
    build_commit_plan, materialize_commit, CommitOpResult, CommitRequest, CommitValidationContext,
    CommitValidationError, PreparedCommit,
};
use loonfs_core::content::{mint_content_token, store_bytes_as_content, verify_content_token};
use loonfs_core::metadata::MetadataState;
use loonfs_core::publish::{NamespaceMutationCandidate, PathMutationIntent};
use loonfs_core::{Error as CoreError, ErrorCode};
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::OperationClass;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::tempdir;

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
        parent_inode_id: None,
        name_key: None,
        display_name: None,
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

fn validation_context(
    metadata_state: &MetadataState,
    seq: ChangeSeq,
    next_inode_id: InodeId,
) -> CommitValidationContext<'_> {
    let namespace_id = namespace_id("demo");
    let head = HeadState {
        content_store_id: loonfs_api::ContentStoreId::generate(),
        fork_basis: None,
        namespace_id: namespace_id.clone(),
        seq,
        head_commit_id: CommitId::parse("c_00000000000000000000000000000000").expect("commit id"),
        writer_epoch: WriterEpoch(1),
        writer: Some(WriterBlock {
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            acquired_at_ms: 1_000,
        }),
        next_inode_id,
        visible_wal_tip: None,
        recent_segments: Vec::new(),
        state: Default::default(),
    };
    CommitValidationContext {
        head,
        metadata_state,
    }
}

#[derive(Debug)]
struct ContentStoreAccessLimitStore {
    inner: LocalFsStore,
    content_store_accesses: AtomicUsize,
    max_content_store_accesses: usize,
}

impl ContentStoreAccessLimitStore {
    fn new(root: impl AsRef<Path>, max_content_store_accesses: usize) -> Self {
        Self {
            inner: LocalFsStore::new(root.as_ref()).expect("store"),
            content_store_accesses: AtomicUsize::new(0),
            max_content_store_accesses,
        }
    }

    fn content_store_access_count(&self) -> usize {
        self.content_store_accesses.load(Ordering::SeqCst)
    }

    fn record_content_store_access(&self, key: &str) -> Result<(), ObjectStoreError> {
        if !key.starts_with("content-stores/") {
            return Ok(());
        }

        let previous = self.content_store_accesses.fetch_add(1, Ordering::SeqCst);
        if previous >= self.max_content_store_accesses {
            return Err(ObjectStoreError::transport(
                key,
                "unexpected content-store descriptor access",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for ContentStoreAccessLimitStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.record_content_store_access(key)?;
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.record_content_store_access(key)?;
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.record_content_store_access(key)?;
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
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
    let request = CommitRequest {
        namespace_id: namespace_id("demo"),
        commit_id: CommitId::parse("stale-revision").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_session_id: "wrs_test".to_owned(),
        writer_epoch: WriterEpoch(1),
        ops: vec![CommitOp::ReplaceFile {
            inode_id: InodeId(3),
            base_revision_no: RevisionNo(1),
            content_ref: content_ref("content-3"),
        }],
        preconditions: Vec::new(),
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
    let request = CommitRequest {
        namespace_id: namespace_id("demo"),
        commit_id: CommitId::parse("preview-rollback").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_session_id: "wrs_test".to_owned(),
        writer_epoch: WriterEpoch(1),
        ops: vec![
            CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: test_display_name("docs"),
            },
            CommitOp::CreateFile {
                parent_inode_id: InodeId(2),
                display_name: test_display_name("readme.txt"),
                content_ref: content_ref("content-1"),
            },
            CommitOp::ReplaceFile {
                inode_id: InodeId(99),
                base_revision_no: RevisionNo(1),
                content_ref: content_ref("content-2"),
            },
        ],
        preconditions: Vec::new(),
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

#[tokio::test]
async fn create_and_replace_under_ancestor_tombstone_are_rejected() {
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
        &CommitRequest {
            namespace_id: namespace_id("demo"),
            commit_id: CommitId::parse("create-under-tombstone").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_epoch: WriterEpoch(1),
            ops: vec![CommitOp::CreateFile {
                parent_inode_id: InodeId(2),
                display_name: test_display_name("new.txt"),
                content_ref: content_ref("content-2"),
            }],
            preconditions: Vec::new(),
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

    let replace_error = build_commit_plan(
        &CommitRequest {
            namespace_id: namespace_id("demo"),
            commit_id: CommitId::parse("replace-under-tombstone").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_epoch: WriterEpoch(1),
            ops: vec![CommitOp::ReplaceFile {
                inode_id: InodeId(3),
                base_revision_no: RevisionNo(1),
                content_ref: content_ref("content-2"),
            }],
            preconditions: Vec::new(),
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
    let request = CommitRequest {
        namespace_id: namespace_id("demo"),
        commit_id: CommitId::parse("restore-missing-inode").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_session_id: "wrs_test".to_owned(),
        writer_epoch: WriterEpoch(1),
        ops: vec![CommitOp::RestoreRevision {
            inode_id: InodeId(99),
            source_revision_no: RevisionNo(1),
            base_revision_no: RevisionNo(1),
        }],
        preconditions: Vec::new(),
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
    let request = CommitRequest {
        namespace_id: namespace_id("demo"),
        commit_id: CommitId::parse("restore-non-file").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_session_id: "wrs_test".to_owned(),
        writer_epoch: WriterEpoch(1),
        ops: vec![CommitOp::RestoreRevision {
            inode_id: InodeId(2),
            source_revision_no: RevisionNo(1),
            base_revision_no: RevisionNo(1),
        }],
        preconditions: Vec::new(),
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
        &CommitRequest {
            namespace_id: namespace_id("demo"),
            commit_id: CommitId::parse("restore-stale-base").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_epoch: WriterEpoch(1),
            ops: vec![CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(1),
                base_revision_no: RevisionNo(1),
            }],
            preconditions: Vec::new(),
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
        &CommitRequest {
            namespace_id: namespace_id("demo"),
            commit_id: CommitId::parse("restore-missing-source").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_epoch: WriterEpoch(1),
            ops: vec![CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(99),
                base_revision_no: RevisionNo(2),
            }],
            preconditions: Vec::new(),
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

    let request = CommitRequest {
        namespace_id: namespace_id("demo"),
        commit_id: CommitId::parse("restore-same-request-source").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_session_id: "wrs_test".to_owned(),
        writer_epoch: WriterEpoch(1),
        ops: vec![
            CommitOp::ReplaceFile {
                inode_id: InodeId(3),
                base_revision_no: RevisionNo(1),
                content_ref: content_ref("content-2"),
            },
            CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(2),
                base_revision_no: RevisionNo(2),
            },
        ],
        preconditions: Vec::new(),
        message: None,
    };
    let plan = build_commit_plan(&request, 4_200, &context)
        .await
        .expect("replace then restore in same request should validate");
    let materialized = materialize_commit(
        PreparedCommit::new(request, plan).expect("prepare commit"),
        4_200,
    );
    let expected = content_ref("content-2");
    assert!(matches!(
        &materialized.results[1],
        CommitOpResult::RestoreRevision {
            content_ref,
            ..
        } if *content_ref == expected
    ));
}

#[tokio::test]
async fn restore_revision_can_reference_restore_created_earlier_in_same_request() {
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

    let request = CommitRequest {
        namespace_id: namespace_id("demo"),
        commit_id: CommitId::parse("restore-after-restore-same-request").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_session_id: "wrs_test".to_owned(),
        writer_epoch: WriterEpoch(1),
        ops: vec![
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
        ],
        preconditions: Vec::new(),
        message: None,
    };
    let plan = build_commit_plan(&request, 4_200, &context)
        .await
        .expect("restore then restore in same request should validate");
    let materialized = materialize_commit(
        PreparedCommit::new(request, plan).expect("prepare commit"),
        4_200,
    );
    let expected = content_ref("content-1");
    assert!(matches!(
        &materialized.results[0],
        CommitOpResult::RestoreRevision {
            content_ref,
            ..
        } if *content_ref == expected
    ));
    assert!(matches!(
        &materialized.results[1],
        CommitOpResult::RestoreRevision {
            content_ref,
            ..
        } if *content_ref == expected
    ));
}

#[tokio::test]
async fn restore_revision_under_tombstoned_ancestor_is_rejected() {
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
        &CommitRequest {
            namespace_id: namespace_id("demo"),
            commit_id: CommitId::parse("restore-under-tombstone").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_epoch: WriterEpoch(1),
            ops: vec![CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(1),
                base_revision_no: RevisionNo(1),
            }],
            preconditions: Vec::new(),
            message: None,
        },
        4_200,
        &context,
    )
    .await
    .expect_err("restore tombstone conflict");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionUnderSubtreeTombstone {
            inode_id: InodeId(3),
            ..
        }
    ));
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
    let request = CommitRequest {
        namespace_id: namespace_id("demo"),
        commit_id: CommitId::parse("restore-overflow").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_session_id: "wrs_test".to_owned(),
        writer_epoch: WriterEpoch(1),
        ops: vec![CommitOp::RestoreRevision {
            inode_id: InodeId(2),
            source_revision_no: RevisionNo(u64::MAX),
            base_revision_no: RevisionNo(u64::MAX),
        }],
        preconditions: Vec::new(),
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

#[tokio::test]
async fn path_put_file_without_admission_fails_without_reading_content() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"hello")
        .await
        .expect("stage content");

    store.reset();
    let responses = publish_namespace_mutations_batch(
        &store,
        &namespace_id,
        vec![NamespaceMutationCandidate::path(
            PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-cold-content").expect("valid commit id"),
                message: None,
                absolute_path: AbsolutePath::parse("/docs/hello.txt").expect("path"),
                content_ref: content.content_ref,
                behavior: DestinationBehavior::NoReplace,
                expected_revision_no: None,
            },
        )],
        &context,
    )
    .await;

    assert_eq!(
        responses[0]
            .as_ref()
            .expect_err("unadmitted path put must fail")
            .code(),
        ErrorCode::ContentNotPrepared
    );
    assert_eq!(store.count(OperationClass::Read), 0);
}

#[tokio::test]
async fn path_batch_rejects_repeated_unadmitted_content_without_reading_it() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"shared")
        .await
        .expect("stage content");

    store.reset();
    let responses = publish_namespace_mutations_batch(
        &store,
        &namespace_id,
        vec![
            NamespaceMutationCandidate::path(PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-shared-a").expect("valid commit id"),
                message: None,
                absolute_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                content_ref: content.content_ref.clone(),
                behavior: DestinationBehavior::NoReplace,
                expected_revision_no: None,
            }),
            NamespaceMutationCandidate::path(PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-shared-b").expect("valid commit id"),
                message: None,
                absolute_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
                content_ref: content.content_ref,
                behavior: DestinationBehavior::NoReplace,
                expected_revision_no: None,
            }),
        ],
        &context,
    )
    .await;

    assert!(responses.iter().all(|response| {
        response
            .as_ref()
            .is_err_and(|error| error.code() == ErrorCode::ContentNotPrepared)
    }));
    assert_eq!(store.count(OperationClass::Read), 0);
}

#[tokio::test]
async fn valid_content_admission_skips_durable_content_validation() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"admitted")
        .await
        .expect("stage content");
    let token = ValidatedContentToken {
        content_ref: content.content_ref.clone(),
        token: mint_content_token(
            "test-content-token-secret",
            &namespace_id,
            &content.content_ref,
            context.now_ms,
        )
        .expect("mint token"),
    };
    let catalog = loonfs_core::control::load_namespace_catalog_entry(&store, &namespace_id)
        .await
        .expect("load namespace catalog");
    let prepared = verify_content_token(
        "test-content-token-secret",
        &catalog,
        &token,
        context.now_ms,
    )
    .expect("verify token");

    store.reset();
    let responses = publish_namespace_mutations_batch(
        &store,
        &namespace_id,
        vec![NamespaceMutationCandidate::path_prepared(
            PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-admitted-content").expect("valid commit id"),
                message: None,
                absolute_path: AbsolutePath::parse("/docs/admitted.txt").expect("path"),
                content_ref: content.content_ref,
                behavior: DestinationBehavior::NoReplace,
                expected_revision_no: None,
            },
            vec![prepared],
        )],
        &context,
    )
    .await;

    assert!(responses[0].is_ok());
    // A live admission is the fast path: no content read at all.
    assert_eq!(store.count(OperationClass::Read), 0);
}

#[tokio::test]
async fn binding_is_precondition_observes_earlier_batch_candidate() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/readme.txt",
        b"hello",
        &context,
        Some("seed-child-name-is"),
    )
    .await
    .expect("seed file");
    let docs_inode = resolve_path(&store, &namespace_id, "/docs")
        .await
        .expect("resolve docs")
        .inode_id;
    let file_inode = resolve_path(&store, &namespace_id, "/docs/readme.txt")
        .await
        .expect("resolve file")
        .inode_id;
    let original_binding = current_binding_for_child(&store, &namespace_id, file_inode).await;

    let responses = commit_operations_batch(
        &store,
        &namespace_id,
        vec![
            ApiCommitRequest {
                commit_id: CommitId::parse("move-before-child-name-check")
                    .expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![ApiCommitOp::Rename {
                    inode_id: file_inode,
                    new_parent_inode_id: docs_inode,
                    new_display_name: test_display_name("moved.txt"),
                }],
                message: None,
            },
            ApiCommitRequest {
                commit_id: CommitId::parse("delete-with-stale-binding").expect("valid commit id"),
                preconditions: vec![CommitPrecondition::BindingIs {
                    parent_inode_id: docs_inode,
                    name_key: original_binding.name_key.clone(),
                    child_inode_id: file_inode,
                    bind_seq: original_binding.bind_seq,
                    bind_delta_index: original_binding.bind_delta_index,
                }],
                ops: vec![ApiCommitOp::DeleteFile {
                    inode_id: file_inode,
                }],
                message: None,
            },
        ],
        &context,
    )
    .await;

    assert_eq!(
        responses[0].as_ref().expect("rename").committed_seq,
        ChangeSeq(2)
    );
    let error = responses[1]
        .as_ref()
        .expect_err("stale binding precondition");
    assert_eq!(error.code(), ErrorCode::PathConflict);
}

#[tokio::test]
async fn directory_empty_precondition_observes_earlier_batch_candidate() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    commit_operations(
        &store,
        &namespace_id,
        ApiCommitRequest {
            commit_id: CommitId::parse("seed-empty-dir").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: test_display_name("docs"),
            }],
            message: None,
        },
        &context,
    )
    .await
    .expect("seed docs");
    let docs_inode = resolve_path(&store, &namespace_id, "/docs")
        .await
        .expect("resolve seeded directory")
        .inode_id;
    let content = store_bytes_as_content(&store, &namespace_id, b"child")
        .await
        .expect("stage content");

    let responses = commit_operations_batch(
        &store,
        &namespace_id,
        vec![
            ApiCommitRequest {
                commit_id: CommitId::parse("create-child-before-empty-check")
                    .expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![ApiCommitOp::CreateFile {
                    parent_inode_id: docs_inode,
                    display_name: test_display_name("child.txt"),
                    content_ref: content.content_ref,
                }],
                message: None,
            },
            ApiCommitRequest {
                commit_id: CommitId::parse("delete-dir-with-stale-empty-check")
                    .expect("valid commit id"),
                preconditions: vec![CommitPrecondition::DirectoryEmpty {
                    inode_id: docs_inode,
                }],
                ops: vec![ApiCommitOp::DeleteSubtree {
                    root_inode_id: docs_inode,
                }],
                message: None,
            },
        ],
        &context,
    )
    .await;

    assert_eq!(
        responses[0].as_ref().expect("create child").committed_seq,
        ChangeSeq(2)
    );
    let error = responses[1]
        .as_ref()
        .expect_err("directory empty precondition");
    assert_eq!(error.code(), ErrorCode::DirectoryNotEmpty);
}

#[tokio::test]
async fn explicit_commit_rejects_invalid_display_names() {
    assert!(DisplayName::parse("a/b").is_err());
    assert!(DisplayName::parse(".").is_err());
}

#[tokio::test]
async fn restore_revision_does_not_revalidate_retained_content_before_publish() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");

    let first = store_bytes_as_content(&store, &namespace_id("demo"), b"first")
        .await
        .expect("stage first");
    commit_operations(
        &store,
        &namespace_id("demo"),
        ApiCommitRequest {
            commit_id: CommitId::parse("restore-create").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::CreateFile {
                parent_inode_id: InodeId(1),
                display_name: test_display_name("restore.txt"),
                content_ref: first.content_ref.clone(),
            }],
            message: None,
        },
        &context,
    )
    .await
    .expect("create file");
    let inode_id = resolve_path(&store, &namespace_id("demo"), "/restore.txt")
        .await
        .expect("resolve created file")
        .inode_id;

    let second = store_bytes_as_content(&store, &namespace_id("demo"), b"second")
        .await
        .expect("stage second");
    commit_operations(
        &store,
        &namespace_id("demo"),
        ApiCommitRequest {
            commit_id: CommitId::parse("restore-replace").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::ReplaceFile {
                inode_id,
                base_revision_no: RevisionNo(1),
                content_ref: second.content_ref,
            }],
            message: None,
        },
        &context,
    )
    .await
    .expect("replace file");

    store
        .delete(
            &content_blob(first.content_store_id.as_str(), &first.content_ref.digest)
                .expect("first content key"),
        )
        .await
        .expect("delete first content");

    let response = commit_operations(
        &store,
        &namespace_id("demo"),
        ApiCommitRequest {
            commit_id: CommitId::parse("restore-missing-content").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::RestoreRevision {
                inode_id,
                source_revision_no: RevisionNo(1),
                base_revision_no: RevisionNo(2),
            }],
            message: None,
        },
        &context,
    )
    .await
    .expect("restore trusts retained content metadata");
    assert_eq!(response.committed_seq, ChangeSeq(3));
}

#[tokio::test]
async fn metadata_only_commit_does_not_validate_content_store_refs() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/delete-me.txt",
        b"hello",
        &context,
        Some("seed-metadata-only-delete"),
    )
    .await
    .expect("seed file");
    let inode_id = resolve_path(&store, &namespace_id("demo"), "/docs/delete-me.txt")
        .await
        .expect("resolve seeded file")
        .inode_id;

    let guarded_store = ContentStoreAccessLimitStore::new(temp_dir.path(), 0);
    let response = commit_operations(
        &guarded_store,
        &namespace_id("demo"),
        ApiCommitRequest {
            commit_id: CommitId::parse("metadata-only-delete").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::DeleteFile { inode_id }],
            message: None,
        },
        &context,
    )
    .await
    .expect("metadata-only delete should not perform content validation");

    assert_eq!(response.committed_seq, ChangeSeq(2));
    assert_eq!(
        guarded_store.content_store_access_count(),
        0,
        "the namespace's content store is a field in its head; metadata-only validation must not touch the content-store keyspace at all",
    );
}

#[tokio::test]
async fn explicit_create_file_fails_unprepared_before_parent_validation_without_content_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");

    let guarded_store = ContentStoreAccessLimitStore::new(temp_dir.path(), 0);
    let missing_content = content_ref("missing-content");
    let error = publish_namespace_mutations_batch(
        &guarded_store,
        &namespace_id("demo"),
        vec![NamespaceMutationCandidate::commit(ApiCommitRequest {
            commit_id: CommitId::parse("create-missing-parent-unprepared-content")
                .expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::CreateFile {
                parent_inode_id: InodeId(99),
                display_name: test_display_name("missing.txt"),
                content_ref: missing_content.clone(),
            }],
            message: None,
        })],
        &context,
    )
    .await
    .into_iter()
    .next()
    .expect("one result")
    .expect_err("unprepared content should win before missing parent");
    assert!(matches!(
        error,
        CoreError::ContentPreparation(
            loonfs_core::publish::ContentPreparationError::ContentNotPrepared {
                content_ref_digest
            }
        ) if content_ref_digest == missing_content.digest
    ));
    assert_eq!(guarded_store.content_store_access_count(), 0);
}

#[tokio::test]
async fn explicit_replace_file_fails_unprepared_before_revision_validation_without_content_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/replace.txt",
        b"first",
        &context,
        Some("seed-replace"),
    )
    .await
    .expect("seed replace target");
    let inode_id = resolve_path(&store, &namespace_id("demo"), "/docs/replace.txt")
        .await
        .expect("resolve path")
        .inode_id;

    let guarded_store = ContentStoreAccessLimitStore::new(temp_dir.path(), 0);
    let missing_content = content_ref("missing-content");
    let error = publish_namespace_mutations_batch(
        &guarded_store,
        &namespace_id("demo"),
        vec![NamespaceMutationCandidate::commit(ApiCommitRequest {
            commit_id: CommitId::parse("replace-stale-missing-content").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::ReplaceFile {
                inode_id,
                base_revision_no: RevisionNo(99),
                content_ref: missing_content.clone(),
            }],
            message: None,
        })],
        &context,
    )
    .await
    .into_iter()
    .next()
    .expect("one result")
    .expect_err("unprepared content should win before stale revision");
    assert!(matches!(
        error,
        CoreError::ContentPreparation(
            loonfs_core::publish::ContentPreparationError::ContentNotPrepared {
                content_ref_digest
            }
        ) if content_ref_digest == missing_content.digest
    ));
    assert_eq!(guarded_store.content_store_access_count(), 0);
}

#[tokio::test]
async fn restore_revision_missing_source_is_revision_not_found() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/restore.txt",
        b"first",
        &context,
        Some("seed-restore"),
    )
    .await
    .expect("seed restore target");
    let inode_id = resolve_path(&store, &namespace_id("demo"), "/docs/restore.txt")
        .await
        .expect("resolve path")
        .inode_id;

    let error = commit_operations(
        &store,
        &namespace_id("demo"),
        ApiCommitRequest {
            commit_id: CommitId::parse("restore-missing-source").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::RestoreRevision {
                inode_id,
                source_revision_no: RevisionNo(99),
                base_revision_no: RevisionNo(1),
            }],
            message: None,
        },
        &context,
    )
    .await
    .expect_err("missing restore source should fail");
    assert_eq!(error.code(), ErrorCode::RevisionNotFound);
}
