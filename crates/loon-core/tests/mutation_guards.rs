use loon_api::{
    decode_wal_segment_envelope_zstd, sha256_digest,
    v0::{
        CommitOp as ApiCommitOp, CommitPrecondition, CommitRequest as ApiCommitRequest,
        CompleteUploadRequest,
    },
    ChangeSeq, ContentRef, ContentRefKind, ContentStoreDescriptorEnvelope, ControlObjectKind,
    FenceToken, HeadState, InodeId, InodeKind, LeaseState, NamespaceDescriptorEnvelope,
    NamespaceDescriptorState, NamespaceId, RevisionNo,
};
use loon_core::commit::{
    build_commit_plan, CommitOp, CommitRequest, CommitValidationContext, CommitValidationError,
    Precondition,
};
use loon_core::metadata::{InodeRecord, MetadataState};
use loon_core::{
    bootstrap_namespace, commit_operations, commit_operations_batch, complete_upload,
    copy_file_path, delete_path, delete_path_non_recursive, fork_namespace, list_changes_after,
    list_namespaces, load_verified_namespace_basis, move_path, publish_namespace_mutations_batch,
    put_file_bytes, read_file_bytes, resolve_path, store_bytes_as_content, write_file_bytes,
    CoreError, CoreErrorKind, DirectObjectStorePublisher, MutationContext,
    NamespaceMutationCandidate, PathMutationIntent, PublishOptions, PutFileBehavior,
};
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::{
    content_blob, content_store_descriptor, namespace_descriptor, namespace_head, namespace_lease,
};
use loon_objectstore::{ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tempfile::tempdir;

#[test]
fn stale_head_precondition_is_rejected() {
    let metadata_state = metadata_state_after(&[
        vec![loon_api::WalOp::CreateDir {
            op_index: 0,
            inode_id: InodeId(2),
            parent_inode: InodeId(1),
            display_name: "docs".to_owned(),
        }],
        vec![loon_api::WalOp::CreateFile {
            op_index: 0,
            inode_id: InodeId(3),
            parent_inode: InodeId(2),
            display_name: "readme.txt".to_owned(),
            content_ref: content_ref("content-1"),
        }],
    ]);
    let context = validation_context(metadata_state, ChangeSeq(2), InodeId(4));
    let request = CommitRequest {
        namespace_id: namespace_id(),
        request_id: "stale-head".to_owned(),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(1),
        planned_head_seq: ChangeSeq(2),
        source_request_checksum_sha256: None,
        ops: vec![CommitOp::DeleteFile {
            inode_id: InodeId(3),
        }],
        preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(1))],
        message: None,
        annotations: None,
    };

    let error = build_commit_plan(&request, &context).expect_err("stale head");
    assert!(matches!(
        error,
        CommitValidationError::ConflictingHeadSeqPrecondition {
            expected: ChangeSeq(2),
            actual: ChangeSeq(1),
        }
    ));
}

#[test]
fn missing_head_seq_precondition_is_rejected() {
    let metadata_state = metadata_state_after(&[
        vec![loon_api::WalOp::CreateDir {
            op_index: 0,
            inode_id: InodeId(2),
            parent_inode: InodeId(1),
            display_name: "docs".to_owned(),
        }],
        vec![loon_api::WalOp::CreateFile {
            op_index: 0,
            inode_id: InodeId(3),
            parent_inode: InodeId(2),
            display_name: "readme.txt".to_owned(),
            content_ref: content_ref("content-1"),
        }],
    ]);
    let context = validation_context(metadata_state, ChangeSeq(2), InodeId(4));
    let request = CommitRequest {
        namespace_id: namespace_id(),
        request_id: "missing-head-seq".to_owned(),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(1),
        planned_head_seq: ChangeSeq(2),
        source_request_checksum_sha256: None,
        ops: vec![CommitOp::DeleteFile {
            inode_id: InodeId(3),
        }],
        preconditions: Vec::new(),
        message: None,
        annotations: None,
    };

    let error = build_commit_plan(&request, &context).expect_err("missing head seq");
    assert!(matches!(
        error,
        CommitValidationError::MissingHeadSeqPrecondition {
            expected: ChangeSeq(2),
        }
    ));
}

#[test]
fn planned_head_seq_mismatch_is_rejected() {
    let metadata_state = metadata_state_after(&[
        vec![loon_api::WalOp::CreateDir {
            op_index: 0,
            inode_id: InodeId(2),
            parent_inode: InodeId(1),
            display_name: "docs".to_owned(),
        }],
        vec![loon_api::WalOp::CreateFile {
            op_index: 0,
            inode_id: InodeId(3),
            parent_inode: InodeId(2),
            display_name: "readme.txt".to_owned(),
            content_ref: content_ref("content-1"),
        }],
    ]);
    let context = validation_context(metadata_state, ChangeSeq(2), InodeId(4));
    let request = CommitRequest {
        namespace_id: namespace_id(),
        request_id: "planned-head-mismatch".to_owned(),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(1),
        planned_head_seq: ChangeSeq(1),
        source_request_checksum_sha256: None,
        ops: vec![CommitOp::DeleteFile {
            inode_id: InodeId(3),
        }],
        preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(1))],
        message: None,
        annotations: None,
    };

    let error = build_commit_plan(&request, &context).expect_err("planned head mismatch");
    assert!(matches!(
        error,
        CommitValidationError::PlannedHeadSeqMismatch {
            expected: ChangeSeq(2),
            actual: ChangeSeq(1),
        }
    ));
}

#[test]
fn stale_revision_precondition_is_rejected() {
    let metadata_state = metadata_state_after(&[
        vec![loon_api::WalOp::CreateDir {
            op_index: 0,
            inode_id: InodeId(2),
            parent_inode: InodeId(1),
            display_name: "docs".to_owned(),
        }],
        vec![loon_api::WalOp::CreateFile {
            op_index: 0,
            inode_id: InodeId(3),
            parent_inode: InodeId(2),
            display_name: "readme.txt".to_owned(),
            content_ref: content_ref("content-1"),
        }],
        vec![loon_api::WalOp::ReplaceFile {
            op_index: 0,
            inode_id: InodeId(3),
            base_revision: RevisionNo(1),
            content_ref: content_ref("content-2"),
        }],
    ]);
    let context = validation_context(metadata_state, ChangeSeq(3), InodeId(4));
    let request = CommitRequest {
        namespace_id: namespace_id(),
        request_id: "stale-revision".to_owned(),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(1),
        planned_head_seq: ChangeSeq(3),
        source_request_checksum_sha256: None,
        ops: vec![CommitOp::ReplaceFile {
            inode_id: InodeId(3),
            base_revision: RevisionNo(1),
            content_ref: content_ref("content-3"),
        }],
        preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(3))],
        message: None,
        annotations: None,
    };

    let error = build_commit_plan(&request, &context).expect_err("stale revision");
    assert!(matches!(
        error,
        CommitValidationError::ReplaceFileBaseRevisionMismatch {
            inode_id: InodeId(3),
            expected: RevisionNo(1),
            actual: Some(RevisionNo(2)),
        }
    ));
}

#[test]
fn create_and_replace_under_ancestor_tombstone_are_rejected() {
    let metadata_state = metadata_state_after(&[
        vec![loon_api::WalOp::CreateDir {
            op_index: 0,
            inode_id: InodeId(2),
            parent_inode: InodeId(1),
            display_name: "docs".to_owned(),
        }],
        vec![loon_api::WalOp::CreateFile {
            op_index: 0,
            inode_id: InodeId(3),
            parent_inode: InodeId(2),
            display_name: "readme.txt".to_owned(),
            content_ref: content_ref("content-1"),
        }],
        vec![loon_api::WalOp::DeleteSubtree {
            op_index: 0,
            root_inode: InodeId(2),
        }],
    ]);
    let context = validation_context(metadata_state.clone(), ChangeSeq(3), InodeId(4));

    let create_error = build_commit_plan(
        &CommitRequest {
            namespace_id: namespace_id(),
            request_id: "create-under-tombstone".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            planned_head_seq: ChangeSeq(3),
            source_request_checksum_sha256: None,
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(2),
                display_name: "new.txt".to_owned(),
                content_ref: content_ref("content-2"),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(3))],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("create under tombstone");
    assert!(matches!(
        create_error,
        CommitValidationError::CreateUnderSubtreeTombstone {
            parent_inode: InodeId(2),
            ..
        }
    ));

    let replace_error = build_commit_plan(
        &CommitRequest {
            namespace_id: namespace_id(),
            request_id: "replace-under-tombstone".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            planned_head_seq: ChangeSeq(3),
            source_request_checksum_sha256: None,
            ops: vec![CommitOp::ReplaceFile {
                inode_id: InodeId(3),
                base_revision: RevisionNo(1),
                content_ref: content_ref("content-2"),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(3))],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("replace under tombstone");
    assert!(matches!(
        replace_error,
        CommitValidationError::ReplaceFileUnderSubtreeTombstone {
            inode_id: InodeId(3),
            ..
        }
    ));
}

#[test]
fn restore_revision_validation_rejects_missing_inode() {
    let metadata_state = metadata_state_after(&[vec![loon_api::WalOp::CreateDir {
        op_index: 0,
        inode_id: InodeId(2),
        parent_inode: InodeId(1),
        display_name: "docs".to_owned(),
    }]]);
    let context = validation_context(metadata_state, ChangeSeq(1), InodeId(3));
    let request = CommitRequest {
        namespace_id: namespace_id(),
        request_id: "restore-missing-inode".to_owned(),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(1),
        planned_head_seq: ChangeSeq(1),
        source_request_checksum_sha256: None,
        ops: vec![CommitOp::RestoreRevision {
            inode_id: InodeId(99),
            source_revision: RevisionNo(1),
            base_revision: RevisionNo(1),
        }],
        preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(1))],
        message: None,
        annotations: None,
    };

    let error = build_commit_plan(&request, &context).expect_err("restore missing inode");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionInodeMissing {
            inode_id: InodeId(99),
        }
    ));
}

#[test]
fn restore_revision_validation_rejects_non_file_target() {
    let metadata_state = metadata_state_after(&[vec![loon_api::WalOp::CreateDir {
        op_index: 0,
        inode_id: InodeId(2),
        parent_inode: InodeId(1),
        display_name: "docs".to_owned(),
    }]]);
    let context = validation_context(metadata_state, ChangeSeq(1), InodeId(3));
    let request = CommitRequest {
        namespace_id: namespace_id(),
        request_id: "restore-non-file".to_owned(),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(1),
        planned_head_seq: ChangeSeq(1),
        source_request_checksum_sha256: None,
        ops: vec![CommitOp::RestoreRevision {
            inode_id: InodeId(2),
            source_revision: RevisionNo(1),
            base_revision: RevisionNo(1),
        }],
        preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(1))],
        message: None,
        annotations: None,
    };

    let error = build_commit_plan(&request, &context).expect_err("restore non-file");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionInodeNotFile {
            inode_id: InodeId(2),
            actual_kind: InodeKind::Dir,
        }
    ));
}

#[test]
fn restore_revision_validation_rejects_stale_or_missing_source_revision() {
    let metadata_state = metadata_state_after(&[
        vec![loon_api::WalOp::CreateDir {
            op_index: 0,
            inode_id: InodeId(2),
            parent_inode: InodeId(1),
            display_name: "docs".to_owned(),
        }],
        vec![loon_api::WalOp::CreateFile {
            op_index: 0,
            inode_id: InodeId(3),
            parent_inode: InodeId(2),
            display_name: "readme.txt".to_owned(),
            content_ref: content_ref("content-1"),
        }],
        vec![loon_api::WalOp::ReplaceFile {
            op_index: 0,
            inode_id: InodeId(3),
            base_revision: RevisionNo(1),
            content_ref: content_ref("content-2"),
        }],
    ]);
    let context = validation_context(metadata_state, ChangeSeq(3), InodeId(4));

    let stale_base = build_commit_plan(
        &CommitRequest {
            namespace_id: namespace_id(),
            request_id: "restore-stale-base".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            planned_head_seq: ChangeSeq(3),
            source_request_checksum_sha256: None,
            ops: vec![CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision: RevisionNo(1),
                base_revision: RevisionNo(1),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(3))],
            message: None,
            annotations: None,
        },
        &context,
    )
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
            namespace_id: namespace_id(),
            request_id: "restore-missing-source".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            planned_head_seq: ChangeSeq(3),
            source_request_checksum_sha256: None,
            ops: vec![CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision: RevisionNo(99),
                base_revision: RevisionNo(2),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(3))],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("restore missing source");
    assert!(matches!(
        missing_source,
        CommitValidationError::RestoreRevisionSourceRevisionMissing {
            inode_id: InodeId(3),
            source_revision: RevisionNo(99),
        }
    ));
}

#[test]
fn restore_revision_can_reference_revision_created_earlier_in_same_request() {
    let metadata_state = metadata_state_after(&[
        vec![loon_api::WalOp::CreateDir {
            op_index: 0,
            inode_id: InodeId(2),
            parent_inode: InodeId(1),
            display_name: "docs".to_owned(),
        }],
        vec![loon_api::WalOp::CreateFile {
            op_index: 0,
            inode_id: InodeId(3),
            parent_inode: InodeId(2),
            display_name: "readme.txt".to_owned(),
            content_ref: content_ref("content-1"),
        }],
    ]);
    let context = validation_context(metadata_state, ChangeSeq(2), InodeId(4));

    let plan = build_commit_plan(
        &CommitRequest {
            namespace_id: namespace_id(),
            request_id: "restore-same-request-source".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            planned_head_seq: ChangeSeq(2),
            source_request_checksum_sha256: None,
            ops: vec![
                CommitOp::ReplaceFile {
                    inode_id: InodeId(3),
                    base_revision: RevisionNo(1),
                    content_ref: content_ref("content-2"),
                },
                CommitOp::RestoreRevision {
                    inode_id: InodeId(3),
                    source_revision: RevisionNo(2),
                    base_revision: RevisionNo(2),
                },
            ],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(2))],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect("replace then restore in same request should validate");
    assert_eq!(
        plan.resolved_restore_content_refs,
        vec![None, Some(content_ref("content-2"))]
    );
}

#[test]
fn restore_revision_can_reference_restore_created_earlier_in_same_request() {
    let metadata_state = metadata_state_after(&[
        vec![loon_api::WalOp::CreateDir {
            op_index: 0,
            inode_id: InodeId(2),
            parent_inode: InodeId(1),
            display_name: "docs".to_owned(),
        }],
        vec![loon_api::WalOp::CreateFile {
            op_index: 0,
            inode_id: InodeId(3),
            parent_inode: InodeId(2),
            display_name: "readme.txt".to_owned(),
            content_ref: content_ref("content-1"),
        }],
        vec![loon_api::WalOp::ReplaceFile {
            op_index: 0,
            inode_id: InodeId(3),
            base_revision: RevisionNo(1),
            content_ref: content_ref("content-2"),
        }],
    ]);
    let context = validation_context(metadata_state, ChangeSeq(3), InodeId(4));

    let plan = build_commit_plan(
        &CommitRequest {
            namespace_id: namespace_id(),
            request_id: "restore-after-restore-same-request".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            planned_head_seq: ChangeSeq(3),
            source_request_checksum_sha256: None,
            ops: vec![
                CommitOp::RestoreRevision {
                    inode_id: InodeId(3),
                    source_revision: RevisionNo(1),
                    base_revision: RevisionNo(2),
                },
                CommitOp::RestoreRevision {
                    inode_id: InodeId(3),
                    source_revision: RevisionNo(3),
                    base_revision: RevisionNo(3),
                },
            ],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(3))],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect("restore then restore in same request should validate");
    assert_eq!(
        plan.resolved_restore_content_refs,
        vec![
            Some(content_ref("content-1")),
            Some(content_ref("content-1"))
        ]
    );
}

#[test]
fn restore_revision_under_tombstoned_ancestor_is_rejected() {
    let metadata_state = metadata_state_after(&[
        vec![loon_api::WalOp::CreateDir {
            op_index: 0,
            inode_id: InodeId(2),
            parent_inode: InodeId(1),
            display_name: "docs".to_owned(),
        }],
        vec![loon_api::WalOp::CreateFile {
            op_index: 0,
            inode_id: InodeId(3),
            parent_inode: InodeId(2),
            display_name: "readme.txt".to_owned(),
            content_ref: content_ref("content-1"),
        }],
        vec![loon_api::WalOp::DeleteSubtree {
            op_index: 0,
            root_inode: InodeId(2),
        }],
    ]);
    let context = validation_context(metadata_state, ChangeSeq(3), InodeId(4));

    let error = build_commit_plan(
        &CommitRequest {
            namespace_id: namespace_id(),
            request_id: "restore-under-tombstone".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            planned_head_seq: ChangeSeq(3),
            source_request_checksum_sha256: None,
            ops: vec![CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision: RevisionNo(1),
                base_revision: RevisionNo(1),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(3))],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("restore tombstone conflict");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionUnderSubtreeTombstone {
            inode_id: InodeId(3),
            ..
        }
    ));
}

#[test]
fn restore_revision_overflow_is_rejected() {
    let metadata_state = MetadataState {
        inodes: vec![
            InodeRecord {
                inode_id: InodeId(1),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(0),
            },
            InodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(1),
            },
        ],
        direntries: vec![loon_core::metadata::DirentryRecord {
            parent_inode_id: InodeId(1),
            name_key: "overflow.txt".to_owned(),
            display_name: "overflow.txt".to_owned(),
            child_inode_id: InodeId(2),
            bind_seq: ChangeSeq(1),
            bind_op_index: 0,
        }],
        revisions: vec![loon_core::metadata::RevisionRecord {
            inode_id: InodeId(2),
            revision_no: RevisionNo(u64::MAX),
            committed_seq: ChangeSeq(1),
            revision_op_index: 0,
            content_ref: content_ref("content-max"),
        }],
        subtree_tombstones: Vec::new(),
        request_receipts: Vec::new(),
    };
    let context = validation_context(metadata_state, ChangeSeq(1), InodeId(3));
    let request = CommitRequest {
        namespace_id: namespace_id(),
        request_id: "restore-overflow".to_owned(),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(1),
        planned_head_seq: ChangeSeq(1),
        source_request_checksum_sha256: None,
        ops: vec![CommitOp::RestoreRevision {
            inode_id: InodeId(2),
            source_revision: RevisionNo(u64::MAX),
            base_revision: RevisionNo(u64::MAX),
        }],
        preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(1))],
        message: None,
        annotations: None,
    };

    let error = build_commit_plan(&request, &context).expect_err("restore overflow");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionOverflow {
            inode_id: InodeId(2),
            base_revision: RevisionNo(u64::MAX),
        }
    ));
}

#[test]
fn namespace_creation_writes_descriptors_and_listing_uses_completion_marker() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id();

    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap namespace");

    let basis = load_verified_namespace_basis(&store, &namespace_id).expect("load namespace basis");
    let descriptor_key = namespace_descriptor(namespace_id.as_str());
    let descriptor_bytes = store
        .get(&descriptor_key, None)
        .expect("read namespace descriptor")
        .expect("namespace descriptor exists");
    let descriptor: NamespaceDescriptorEnvelope =
        serde_json::from_slice(&descriptor_bytes).expect("decode namespace descriptor");
    assert_eq!(descriptor.kind, ControlObjectKind::NamespaceDescriptor);
    assert_eq!(descriptor.state.namespace_id, namespace_id);
    assert_eq!(descriptor.state.content_store_id, basis.content_store_id);
    assert!(descriptor.has_valid_payload_checksum().expect("checksum"));

    let content_descriptor_key = content_store_descriptor(basis.content_store_id.as_str());
    let content_descriptor_bytes = store
        .get(&content_descriptor_key, None)
        .expect("read content-store descriptor")
        .expect("content-store descriptor exists");
    let content_descriptor: ContentStoreDescriptorEnvelope =
        serde_json::from_slice(&content_descriptor_bytes).expect("decode content-store descriptor");
    assert_eq!(
        content_descriptor.kind,
        ControlObjectKind::ContentStoreDescriptor
    );
    assert_eq!(
        content_descriptor.state.content_store_id,
        basis.content_store_id
    );
    assert!(content_descriptor
        .has_valid_payload_checksum()
        .expect("checksum"));

    let content_store_descriptors = store
        .list_prefix("content-stores/")
        .expect("list content stores");
    assert_eq!(
        content_store_descriptors,
        vec![content_descriptor_key],
        "new root namespace should create exactly one content store descriptor"
    );

    store
        .put_if_absent(&namespace_head("partial"), br#"{"not":"a descriptor"}"#)
        .expect("write partial namespace key");
    let listed = list_namespaces(&store).expect("list namespaces");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].namespace_id.as_str(), "demo");

    let partial_error = bootstrap_namespace(&store, &NamespaceId::from("partial"), &context, false)
        .expect_err("partial namespace should be rejected");
    assert!(matches!(
        partial_error,
        loon_core::BootstrapNamespaceError::NamespacePartiallyInitialized { .. }
    ));
}

#[test]
fn bootstrap_head_reservation_failure_does_not_allocate_content_store() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Exact(namespace_head(namespace_id.as_str())),
        InjectedCreateFailure::PreconditionFailed {
            write_attempted_object: true,
            additional_writes: Vec::new(),
        },
    );

    let error = bootstrap_namespace(&store, &namespace_id, &context, false)
        .expect_err("target head precondition should fail bootstrap");
    assert!(matches!(
        error,
        loon_core::BootstrapNamespaceError::HeadWrite(_)
    ));
    assert!(
        store
            .list_prefix("content-stores/")
            .expect("list content stores")
            .is_empty(),
        "content-store descriptor must not be allocated before namespace head reservation"
    );
    assert_namespace_partial(&store, &namespace_id, &context);
}

#[test]
fn public_namespace_operations_reject_invalid_namespace_id_before_key_construction() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let invalid_namespace = NamespaceId::from("bad/name");

    let bootstrap_error = bootstrap_namespace(&store, &invalid_namespace, &context, false)
        .expect_err("invalid namespace_id");
    match bootstrap_error {
        loon_core::BootstrapNamespaceError::InvalidNamespaceId(error) => {
            assert_eq!(error.value(), "bad/name");
        }
        other => panic!("expected invalid namespace_id, got {other:?}"),
    }
    assert_eq!(
        store
            .list_prefix("namespaces/")
            .expect("list namespace objects"),
        Vec::<String>::new()
    );

    let read_error = resolve_path(&store, &invalid_namespace, "/")
        .expect_err("invalid namespace_id should be rejected before lookup");
    assert_eq!(read_error.kind(), CoreErrorKind::InvalidNamespaceId);

    let delete_error = delete_path_non_recursive(
        &store,
        &invalid_namespace,
        "/missing.txt",
        &context,
        Some("invalid-delete"),
    )
    .expect_err("invalid namespace_id should be rejected before retry lookup");
    assert_eq!(delete_error.kind(), CoreErrorKind::InvalidNamespaceId);

    let complete_error = complete_upload(
        &store,
        &invalid_namespace,
        "upl_invalid",
        &CompleteUploadRequest {
            content_ref: ContentRef::whole_file_v0(b""),
        },
        &context,
    )
    .expect_err("invalid namespace_id should be rejected before upload key lookup");
    assert_eq!(complete_error.kind(), CoreErrorKind::InvalidNamespaceId);

    assert_eq!(
        store
            .list_prefix("namespaces/")
            .expect("list namespace objects"),
        Vec::<String>::new()
    );
}

#[test]
fn batch_commit_writes_one_segment_and_expands_change_feed() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::from("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

    let responses = commit_operations_batch(
        &store,
        &namespace_id,
        vec![
            ApiCommitRequest {
                request_id: "req-batch-a".to_owned(),
                planned_head_seq: ChangeSeq(0),
                preconditions: api_head_seq_is(ChangeSeq(0)),
                ops: vec![ApiCommitOp::CreateDir {
                    parent_inode: InodeId(1),
                    display_name: "alpha".to_owned(),
                }],
                message: None,
                annotations: None,
            },
            ApiCommitRequest {
                request_id: "req-batch-b".to_owned(),
                planned_head_seq: ChangeSeq(1),
                preconditions: api_head_seq_is(ChangeSeq(1)),
                ops: vec![ApiCommitOp::CreateDir {
                    parent_inode: InodeId(1),
                    display_name: "beta".to_owned(),
                }],
                message: None,
                annotations: None,
            },
        ],
        &context,
    );
    let first = responses[0].as_ref().expect("first commit");
    let second = responses[1].as_ref().expect("second commit");
    assert_eq!(first.committed_seq, ChangeSeq(1));
    assert_eq!(second.committed_seq, ChangeSeq(2));

    let wal_keys = store.list_prefix("namespaces/demo/wal/").expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload.start_seq, ChangeSeq(1));
    assert_eq!(segment.payload.end_seq, ChangeSeq(2));
    assert_eq!(segment.payload.records.len(), 2);
    store
        .put_if_absent(
            "namespaces/demo/wal/00000000000000000999-00000000000000000999-orphan.cbor.zst",
            &wal_bytes,
        )
        .expect("write unreachable orphan");

    let changes = list_changes_after(&store, &namespace_id, ChangeSeq(0)).expect("changes");
    assert_eq!(changes.changes.len(), 2);
    assert_eq!(changes.changes[0].request_id, "req-batch-a");
    assert_eq!(changes.changes[1].request_id, "req-batch-b");
}

#[test]
fn direct_publisher_retries_after_wal_orphaned_by_stale_head_cas() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::from("demo");
    let context = mutation_context();
    let store = StaleHeadAfterWalWriteStore::new(temp_dir.path(), &namespace_id);
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"retry").expect("stage content");
    let publisher = DirectObjectStorePublisher::new(&store);

    let result = publisher
        .submit_path_intent(
            &namespace_id,
            PathMutationIntent::PutFile {
                request_id: "retry-after-orphan".to_owned(),
                absolute_path: "/retry.txt".to_owned(),
                content_ref: content.content_ref,
                behavior: PutFileBehavior::CreateOnly,
            },
            &context,
            PublishOptions::default(),
        )
        .expect("path intent retries stale head");
    assert_eq!(result.committed_seq, ChangeSeq(1));
    assert!(store.injected_stale_head());

    let wal_keys = store.list_prefix("namespaces/demo/wal/").expect("list wal");
    assert_eq!(wal_keys.len(), 2);

    let basis = load_verified_namespace_basis(&store, &namespace_id).expect("load basis");
    assert_eq!(basis.head.seq, ChangeSeq(1));
    let visible_tip = basis
        .head
        .visible_wal_tip
        .as_ref()
        .expect("visible wal tip");
    assert!(wal_keys.contains(&visible_tip.object_key));
    let orphan_keys = wal_keys
        .iter()
        .filter(|key| *key != &visible_tip.object_key)
        .collect::<Vec<_>>();
    assert_eq!(orphan_keys.len(), 1);

    let visible_wal = store
        .get(&visible_tip.object_key, None)
        .expect("read visible wal")
        .expect("visible wal exists");
    let visible_segment =
        decode_wal_segment_envelope_zstd(&visible_wal).expect("decode visible segment");
    assert_eq!(visible_segment.payload.start_seq, ChangeSeq(1));
    assert_eq!(visible_segment.payload.end_seq, ChangeSeq(1));
    assert_eq!(visible_segment.payload.records.len(), 1);
    assert_eq!(
        visible_segment.payload.records[0].request_id,
        "retry-after-orphan"
    );

    let changes = list_changes_after(&store, &namespace_id, ChangeSeq(0)).expect("changes");
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(changes.changes[0].request_id, "retry-after-orphan");
}

#[test]
fn batch_commit_aliases_duplicate_request_id_with_same_fingerprint() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::from("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

    let request = ApiCommitRequest {
        request_id: "req-duplicate".to_owned(),
        planned_head_seq: ChangeSeq(0),
        preconditions: api_head_seq_is(ChangeSeq(0)),
        ops: vec![ApiCommitOp::CreateDir {
            parent_inode: InodeId(1),
            display_name: "alpha".to_owned(),
        }],
        message: None,
        annotations: None,
    };

    let responses = commit_operations_batch(
        &store,
        &namespace_id,
        vec![request.clone(), request],
        &context,
    );
    let first = responses[0].as_ref().expect("primary commit");
    let duplicate = responses[1].as_ref().expect("duplicate commit");
    assert_eq!(first, duplicate);

    let wal_keys = store.list_prefix("namespaces/demo/wal/").expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload.records.len(), 1);

    let changes = list_changes_after(&store, &namespace_id, ChangeSeq(0)).expect("changes");
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(changes.changes[0].request_id, "req-duplicate");
}

#[test]
fn batch_commit_rejects_duplicate_request_id_with_different_fingerprint() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::from("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

    let responses = commit_operations_batch(
        &store,
        &namespace_id,
        vec![
            ApiCommitRequest {
                request_id: "req-conflict".to_owned(),
                planned_head_seq: ChangeSeq(0),
                preconditions: api_head_seq_is(ChangeSeq(0)),
                ops: vec![ApiCommitOp::CreateDir {
                    parent_inode: InodeId(1),
                    display_name: "alpha".to_owned(),
                }],
                message: None,
                annotations: None,
            },
            ApiCommitRequest {
                request_id: "req-conflict".to_owned(),
                planned_head_seq: ChangeSeq(0),
                preconditions: api_head_seq_is(ChangeSeq(0)),
                ops: vec![ApiCommitOp::CreateDir {
                    parent_inode: InodeId(1),
                    display_name: "beta".to_owned(),
                }],
                message: None,
                annotations: None,
            },
        ],
        &context,
    );

    responses[0].as_ref().expect("primary commit");
    let error = responses[1].as_ref().expect_err("duplicate conflict");
    assert!(matches!(
        error,
        CoreError::RequestIdConflict(request_id) if request_id == "req-conflict"
    ));

    let wal_keys = store.list_prefix("namespaces/demo/wal/").expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload.records.len(), 1);

    let changes = list_changes_after(&store, &namespace_id, ChangeSeq(0)).expect("changes");
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(changes.changes[0].request_id, "req-conflict");
}

#[test]
fn rejected_batch_candidate_does_not_mutate_preview_state() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::from("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

    let responses = commit_operations_batch(
        &store,
        &namespace_id,
        vec![
            ApiCommitRequest {
                request_id: "bad-parent".to_owned(),
                planned_head_seq: ChangeSeq(0),
                preconditions: api_head_seq_is(ChangeSeq(0)),
                ops: vec![ApiCommitOp::CreateDir {
                    parent_inode: InodeId(999),
                    display_name: "ghost".to_owned(),
                }],
                message: None,
                annotations: None,
            },
            ApiCommitRequest {
                request_id: "good-root".to_owned(),
                planned_head_seq: ChangeSeq(0),
                preconditions: api_head_seq_is(ChangeSeq(0)),
                ops: vec![ApiCommitOp::CreateDir {
                    parent_inode: InodeId(1),
                    display_name: "alpha".to_owned(),
                }],
                message: None,
                annotations: None,
            },
        ],
        &context,
    );

    let first_error = responses[0].as_ref().expect_err("first candidate rejected");
    assert!(matches!(
        first_error,
        CoreError::CommitValidation(CommitValidationError::CreateParentMissing {
            parent_inode: InodeId(999),
        })
    ));
    assert_eq!(
        responses[1]
            .as_ref()
            .expect("second candidate commits")
            .committed_seq,
        ChangeSeq(1)
    );
    let alpha = resolve_path(&store, &namespace_id, "/alpha").expect("alpha exists");
    assert_eq!(alpha.inode_id, InodeId(2));

    let wal_keys = store.list_prefix("namespaces/demo/wal/").expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload.records.len(), 1);
    assert_eq!(segment.payload.records[0].request_id, "good-root");
}

#[test]
fn direct_publisher_path_intents_cover_basic_mutations() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::from("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    let publisher = DirectObjectStorePublisher::new(&store);

    let content = store_bytes_as_content(&store, &namespace_id, b"hello").expect("stage content");
    let put = publisher
        .submit_path_intent(
            &namespace_id,
            PathMutationIntent::PutFile {
                request_id: "put-path".to_owned(),
                absolute_path: "/docs/a.txt".to_owned(),
                content_ref: content.content_ref.clone(),
                behavior: PutFileBehavior::CreateOnly,
            },
            &context,
            PublishOptions::default(),
        )
        .expect("put path");
    assert_eq!(put.committed_seq, ChangeSeq(1));

    let moved = publisher
        .submit_path_intent(
            &namespace_id,
            PathMutationIntent::MovePath {
                request_id: "move-path".to_owned(),
                from_path: "/docs/a.txt".to_owned(),
                to_path: "/docs/b.txt".to_owned(),
            },
            &context,
            PublishOptions::default(),
        )
        .expect("move path");
    assert_eq!(moved.committed_seq, ChangeSeq(2));

    let copied = publisher
        .submit_path_intent(
            &namespace_id,
            PathMutationIntent::CopyFilePath {
                request_id: "copy-path".to_owned(),
                from_path: "/docs/b.txt".to_owned(),
                to_path: "/docs/c.txt".to_owned(),
            },
            &context,
            PublishOptions::default(),
        )
        .expect("copy path");
    assert_eq!(copied.committed_seq, ChangeSeq(3));

    let deleted = publisher
        .submit_path_intent(
            &namespace_id,
            PathMutationIntent::DeletePath {
                request_id: "delete-path".to_owned(),
                absolute_path: "/docs/b.txt".to_owned(),
                recursive: false,
            },
            &context,
            PublishOptions::default(),
        )
        .expect("delete path");
    assert_eq!(deleted.committed_seq, ChangeSeq(4));

    let copied_bytes =
        read_file_bytes(&store, &namespace_id, "/docs/c.txt").expect("read copied file");
    assert_eq!(copied_bytes.bytes, b"hello");
}

#[test]
fn direct_publisher_uses_durable_path_request_id_receipts() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::from("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    let publisher = DirectObjectStorePublisher::new(&store);
    let content = store_bytes_as_content(&store, &namespace_id, b"hello").expect("stage content");

    let intent = PathMutationIntent::PutFile {
        request_id: "same-path-request".to_owned(),
        absolute_path: "/same.txt".to_owned(),
        content_ref: content.content_ref.clone(),
        behavior: PutFileBehavior::CreateOnly,
    };
    let first = publisher
        .submit_path_intent(
            &namespace_id,
            intent.clone(),
            &context,
            PublishOptions::default(),
        )
        .expect("first publish");
    let retry = publisher
        .submit_path_intent(&namespace_id, intent, &context, PublishOptions::default())
        .expect("idempotent retry");
    assert_eq!(retry.committed_seq, first.committed_seq);

    let conflict = publisher
        .submit_path_intent(
            &namespace_id,
            PathMutationIntent::DeletePath {
                request_id: "same-path-request".to_owned(),
                absolute_path: "/same.txt".to_owned(),
                recursive: false,
            },
            &context,
            PublishOptions::default(),
        )
        .expect_err("conflicting retry");
    assert!(matches!(
        conflict,
        CoreError::RequestIdConflict(request_id) if request_id == "same-path-request"
    ));

    let wal_keys = store.list_prefix("namespaces/demo/wal/").expect("list wal");
    assert_eq!(wal_keys.len(), 1);
}

#[test]
fn path_intents_in_one_batch_see_tentative_state() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::from("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"hello").expect("stage content");

    let responses = publish_namespace_mutations_batch(
        &store,
        &namespace_id,
        vec![
            NamespaceMutationCandidate::Path(PathMutationIntent::PutFile {
                request_id: "put-batched-path".to_owned(),
                absolute_path: "/docs/a.txt".to_owned(),
                content_ref: content.content_ref,
                behavior: PutFileBehavior::CreateOnly,
            }),
            NamespaceMutationCandidate::Path(PathMutationIntent::MovePath {
                request_id: "move-batched-path".to_owned(),
                from_path: "/docs/a.txt".to_owned(),
                to_path: "/docs/b.txt".to_owned(),
            }),
        ],
        &context,
    );

    assert_eq!(
        responses[0].as_ref().expect("put").committed_seq,
        ChangeSeq(1)
    );
    assert_eq!(
        responses[1].as_ref().expect("move").committed_seq,
        ChangeSeq(2)
    );
    let moved_bytes =
        read_file_bytes(&store, &namespace_id, "/docs/b.txt").expect("read moved file");
    assert_eq!(moved_bytes.bytes, b"hello");

    let wal_keys = store.list_prefix("namespaces/demo/wal/").expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload.records.len(), 2);
}

#[test]
fn namespace_descriptor_checksum_is_validated() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id();

    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap namespace");

    let descriptor_key = namespace_descriptor(namespace_id.as_str());
    let descriptor_bytes = store
        .get(&descriptor_key, None)
        .expect("read namespace descriptor")
        .expect("namespace descriptor exists");
    let mut descriptor: NamespaceDescriptorEnvelope =
        serde_json::from_slice(&descriptor_bytes).expect("decode namespace descriptor");
    descriptor.payload_checksum_sha256 = "not-the-payload-checksum".to_owned();
    let corrupted = serde_json::to_vec(&descriptor).expect("encode corrupted descriptor");
    store
        .put_overwrite(&descriptor_key, &corrupted)
        .expect("overwrite descriptor");

    let error =
        load_verified_namespace_basis(&store, &namespace_id).expect_err("descriptor checksum");
    assert!(
        error.to_string().contains("checksum mismatch"),
        "unexpected error: {error}"
    );
}

#[test]
fn fork_namespace_reuses_content_store_and_isolates_metadata() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let source_namespace_id = namespace_id();
    let clone_namespace_id = NamespaceId::from("clone");

    bootstrap_namespace(&store, &source_namespace_id, &context, false)
        .expect("bootstrap source namespace");
    write_file_bytes(
        &store,
        &source_namespace_id,
        "/docs/shared.txt",
        b"base",
        &context,
        Some("seed-shared"),
    )
    .expect("seed shared file");

    let source_basis =
        load_verified_namespace_basis(&store, &source_namespace_id).expect("source basis");
    assert_eq!(source_basis.head.seq, ChangeSeq(1));
    let content_store_id = source_basis.content_store_id.clone();
    let blobs_before = store
        .list_prefix(&format!(
            "content-stores/{}/blobs/",
            content_store_id.as_str()
        ))
        .expect("list blobs before fork");

    fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .expect("fork namespace");

    let blobs_after = store
        .list_prefix(&format!(
            "content-stores/{}/blobs/",
            content_store_id.as_str()
        ))
        .expect("list blobs after fork");
    assert_eq!(blobs_after, blobs_before, "fork must not copy content");

    let clone_basis =
        load_verified_namespace_basis(&store, &clone_namespace_id).expect("clone basis");
    assert_eq!(clone_basis.content_store_id, content_store_id);
    assert_eq!(clone_basis.head.seq, ChangeSeq(1));
    assert_eq!(clone_basis.head.snapshot_hint_seq, Some(ChangeSeq(1)));
    assert_eq!(clone_basis.head.retention_floor_seq, ChangeSeq(1));

    let duplicate_error =
        fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
            .expect_err("duplicate fork target");
    assert_eq!(duplicate_error.kind(), CoreErrorKind::NamespaceExists);

    let source_entry =
        resolve_path(&store, &source_namespace_id, "/docs/shared.txt").expect("source stat");
    let clone_entry =
        resolve_path(&store, &clone_namespace_id, "/docs/shared.txt").expect("clone stat");
    assert_eq!(source_entry.content_ref, clone_entry.content_ref);
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .expect("read clone")
            .bytes,
        b"base"
    );

    let stale_clone_changes =
        list_changes_after(&store, &clone_namespace_id, ChangeSeq(0)).expect_err("old cursor");
    assert_eq!(
        stale_clone_changes.kind(),
        CoreErrorKind::RebootstrapRequired
    );
    let empty_clone_changes =
        list_changes_after(&store, &clone_namespace_id, ChangeSeq(1)).expect("empty changes");
    assert!(empty_clone_changes.changes.is_empty());

    write_file_bytes(
        &store,
        &source_namespace_id,
        "/docs/shared.txt",
        b"source-after-fork",
        &context,
        Some("source-after-fork"),
    )
    .expect("source replace");
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .expect("read clone after source write")
            .bytes,
        b"base"
    );

    let clone_write = write_file_bytes(
        &store,
        &clone_namespace_id,
        "/docs/shared.txt",
        b"clone-after-fork",
        &context,
        Some("clone-after-fork"),
    )
    .expect("clone replace");
    assert_eq!(clone_write.committed_seq, ChangeSeq(2));
    assert_eq!(
        read_file_bytes(&store, &source_namespace_id, "/docs/shared.txt")
            .expect("read source")
            .bytes,
        b"source-after-fork"
    );
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .expect("read clone")
            .bytes,
        b"clone-after-fork"
    );

    let clone_changes =
        list_changes_after(&store, &clone_namespace_id, ChangeSeq(1)).expect("clone changes");
    assert_eq!(clone_changes.changes.len(), 1);
    assert_eq!(clone_changes.changes[0].seq, ChangeSeq(2));

    for key in store
        .list_prefix(&format!("namespaces/{}/", source_namespace_id.as_str()))
        .expect("list source namespace keys")
    {
        store
            .delete(&key)
            .expect("delete source namespace metadata");
    }
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .expect("clone remains readable")
            .bytes,
        b"clone-after-fork"
    );
}

#[test]
fn fork_target_head_reservation_failure_writes_no_checkpoint_artifacts() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id();
    let clone_namespace_id = NamespaceId::from("clone");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Exact(namespace_head(clone_namespace_id.as_str())),
        InjectedCreateFailure::PreconditionFailed {
            write_attempted_object: true,
            additional_writes: Vec::new(),
        },
    );
    seed_source_namespace_for_fork(&store, &source_namespace_id, &context);

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .expect_err("target head precondition should re-check partial namespace");
    assert_eq!(error.kind(), CoreErrorKind::NamespacePartial);
    assert!(
        store
            .list_prefix(&format!(
                "namespaces/{}/snapshots/",
                clone_namespace_id.as_str()
            ))
            .expect("list target snapshots")
            .is_empty(),
        "target checkpoint artifacts must not be written before target head reservation"
    );
    assert_namespace_partial(&store, &clone_namespace_id, &context);
}

#[test]
fn fork_failure_after_target_head_reserves_partial_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id();
    let clone_namespace_id = NamespaceId::from("clone");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Prefix(format!(
            "namespaces/{}/snapshots/",
            clone_namespace_id.as_str()
        )),
        InjectedCreateFailure::Transport {
            message: "injected target checkpoint failure",
        },
    );
    seed_source_namespace_for_fork(&store, &source_namespace_id, &context);

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .expect_err("target checkpoint write should fail");
    assert_eq!(error.kind(), CoreErrorKind::ServerError);
    assert!(
        store
            .head(&namespace_head(clone_namespace_id.as_str()))
            .expect("head target head")
            .is_some(),
        "target head should reserve namespace before target checkpoint writes"
    );
    assert!(
        store
            .head(&namespace_descriptor(clone_namespace_id.as_str()))
            .expect("head target descriptor")
            .is_none(),
        "descriptor must remain unpublished"
    );
    assert_namespace_partial(&store, &clone_namespace_id, &context);
}

#[test]
fn fork_failure_after_target_checkpoint_artifacts_remains_partial() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id();
    let clone_namespace_id = NamespaceId::from("clone");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Exact(namespace_lease(clone_namespace_id.as_str())),
        InjectedCreateFailure::Transport {
            message: "injected target lease failure",
        },
    );
    seed_source_namespace_for_fork(&store, &source_namespace_id, &context);

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .expect_err("target lease write should fail");
    assert_eq!(error.kind(), CoreErrorKind::ServerError);
    assert!(
        store
            .head(&namespace_head(clone_namespace_id.as_str()))
            .expect("head target head")
            .is_some(),
        "target head should still reserve namespace"
    );
    assert!(
        store
            .head(&namespace_descriptor(clone_namespace_id.as_str()))
            .expect("head target descriptor")
            .is_none(),
        "descriptor must remain unpublished"
    );
    let target_snapshot_keys = store
        .list_prefix(&format!(
            "namespaces/{}/snapshots/",
            clone_namespace_id.as_str()
        ))
        .expect("list target snapshots");
    assert!(
        !target_snapshot_keys.is_empty(),
        "target checkpoint artifacts should have been written before lease failure"
    );
    assert_namespace_partial(&store, &clone_namespace_id, &context);
}

#[test]
fn fork_target_control_conflict_rechecks_complete_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id();
    let clone_namespace_id = NamespaceId::from("clone");
    let context = mutation_context();
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    seed_source_namespace_for_fork(&inner, &source_namespace_id, &context);
    let content_store_id = load_verified_namespace_basis(&inner, &source_namespace_id)
        .expect("source basis")
        .content_store_id;
    let descriptor = NamespaceDescriptorEnvelope::from_state(
        ControlObjectKind::NamespaceDescriptor,
        &context.writer_version,
        NamespaceDescriptorState {
            namespace_id: clone_namespace_id.clone(),
            content_store_id,
        },
    )
    .expect("descriptor envelope");
    let store = InjectCreateFailureStore::new(
        inner,
        KeyMatcher::Exact(namespace_head(clone_namespace_id.as_str())),
        InjectedCreateFailure::Conflict {
            write_attempted_object: true,
            additional_writes: vec![
                (
                    namespace_lease(clone_namespace_id.as_str()),
                    b"lease-present".to_vec(),
                ),
                (
                    namespace_descriptor(clone_namespace_id.as_str()),
                    serde_json::to_vec(&descriptor).expect("descriptor bytes"),
                ),
            ],
        },
    );

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .expect_err("target head conflict should re-check complete namespace");
    assert_eq!(error.kind(), CoreErrorKind::NamespaceExists);
    assert_eq!(
        list_namespaces(&store)
            .expect("list namespaces")
            .into_iter()
            .map(|summary| summary.namespace_id)
            .collect::<Vec<_>>(),
        vec![clone_namespace_id, source_namespace_id]
    );
}

#[test]
fn restore_revision_revalidates_durable_content_before_publish() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");

    let first = store_bytes_as_content(&store, &namespace_id(), b"first").expect("stage first");
    let create = commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            request_id: "restore-create".to_owned(),
            planned_head_seq: ChangeSeq(0),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(0),
            }],
            ops: vec![ApiCommitOp::CreateFile {
                parent_inode: InodeId(1),
                display_name: "restore.txt".to_owned(),
                content_ref: first.content_ref.clone(),
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect("create file");
    let inode_id = match &create.results[0] {
        loon_api::v0::CommitOpResult::CreateFile { inode_id, .. } => *inode_id,
        other => panic!("unexpected create result: {other:?}"),
    };

    let second = store_bytes_as_content(&store, &namespace_id(), b"second").expect("stage second");
    commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            request_id: "restore-replace".to_owned(),
            planned_head_seq: ChangeSeq(1),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(1),
            }],
            ops: vec![ApiCommitOp::ReplaceFile {
                inode_id,
                base_revision_no: RevisionNo(1),
                content_ref: second.content_ref,
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect("replace file");

    store
        .delete(
            &content_blob(first.content_store_id.as_str(), &first.content_ref.digest)
                .expect("first content key"),
        )
        .expect("delete first content");

    let error = commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            request_id: "restore-missing-content".to_owned(),
            planned_head_seq: ChangeSeq(2),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(2),
            }],
            ops: vec![ApiCommitOp::RestoreRevision {
                inode_id,
                source_revision_no: RevisionNo(1),
                base_revision_no: RevisionNo(2),
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("restore missing durable content");
    assert!(matches!(
        error,
        CoreError::DurableContent(
            loon_core::content::DurableContentValidationError::MissingContentObject { .. }
        )
    ));
}

#[test]
fn metadata_only_commit_does_not_validate_content_store_refs() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/delete-me.txt",
        b"hello",
        &context,
        Some("seed-metadata-only-delete"),
    )
    .expect("seed file");
    let inode_id = resolve_path(&store, &namespace_id(), "/docs/delete-me.txt")
        .expect("resolve seeded file")
        .inode_id;

    let guarded_store = ContentStoreAccessLimitStore::new(temp_dir.path(), 2);
    let response = commit_operations(
        &guarded_store,
        &namespace_id(),
        ApiCommitRequest {
            request_id: "metadata-only-delete".to_owned(),
            planned_head_seq: ChangeSeq(1),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(1),
            }],
            ops: vec![ApiCommitOp::DeleteFile { inode_id }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect("metadata-only delete should not perform content validation");

    assert_eq!(response.committed_seq, ChangeSeq(2));
    assert_eq!(
        guarded_store.content_store_access_count(),
        2,
        "basis loading performs one content-store descriptor head/get; metadata-only validation must not add another lookup",
    );
}

#[test]
fn create_file_prioritizes_missing_durable_content_over_missing_parent() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");

    let error = commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            request_id: "create-missing-parent-missing-content".to_owned(),
            planned_head_seq: ChangeSeq(0),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(0),
            }],
            ops: vec![ApiCommitOp::CreateFile {
                parent_inode: InodeId(99),
                display_name: "missing.txt".to_owned(),
                content_ref: content_ref("missing-content"),
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("missing content should win before missing parent");
    assert!(matches!(
        error,
        CoreError::DurableContent(
            loon_core::content::DurableContentValidationError::MissingContentObject { .. }
        )
    ));
}

#[test]
fn replace_file_prioritizes_missing_durable_content_over_stale_revision() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/replace.txt",
        b"first",
        &context,
        Some("seed-replace"),
    )
    .expect("seed replace target");
    let inode_id = resolve_path(&store, &namespace_id(), "/docs/replace.txt")
        .expect("resolve path")
        .inode_id;

    let error = commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            request_id: "replace-stale-missing-content".to_owned(),
            planned_head_seq: ChangeSeq(1),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(1),
            }],
            ops: vec![ApiCommitOp::ReplaceFile {
                inode_id,
                base_revision_no: RevisionNo(99),
                content_ref: content_ref("missing-content"),
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("missing content should win before stale revision");
    assert!(matches!(
        error,
        CoreError::DurableContent(
            loon_core::content::DurableContentValidationError::MissingContentObject { .. }
        )
    ));
}

#[test]
fn restore_revision_missing_source_is_revision_not_found() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/restore.txt",
        b"first",
        &context,
        Some("seed-restore"),
    )
    .expect("seed restore target");
    let inode_id = resolve_path(&store, &namespace_id(), "/docs/restore.txt")
        .expect("resolve path")
        .inode_id;

    let error = commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            request_id: "restore-missing-source".to_owned(),
            planned_head_seq: ChangeSeq(1),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(1),
            }],
            ops: vec![ApiCommitOp::RestoreRevision {
                inode_id,
                source_revision_no: RevisionNo(99),
                base_revision_no: RevisionNo(1),
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("missing restore source should fail");
    assert_eq!(error.kind(), CoreErrorKind::RevisionNotFound);
}

#[test]
fn restore_revision_resolves_same_request_source_before_durable_content_validation() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");

    let first = store_bytes_as_content(&store, &namespace_id(), b"first").expect("stage first");
    let create = commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            request_id: "resolve-before-durable-check-create".to_owned(),
            planned_head_seq: ChangeSeq(0),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(0),
            }],
            ops: vec![ApiCommitOp::CreateFile {
                parent_inode: InodeId(1),
                display_name: "restore.txt".to_owned(),
                content_ref: first.content_ref.clone(),
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect("create file");
    let inode_id = match &create.results[0] {
        loon_api::v0::CommitOpResult::CreateFile { inode_id, .. } => *inode_id,
        other => panic!("unexpected create result: {other:?}"),
    };

    let second = store_bytes_as_content(&store, &namespace_id(), b"second").expect("stage second");
    store
        .delete(
            &content_blob(second.content_store_id.as_str(), &second.content_ref.digest)
                .expect("second content key"),
        )
        .expect("delete second content");

    let error = commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            request_id: "resolve-before-durable-check-commit".to_owned(),
            planned_head_seq: ChangeSeq(1),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(1),
            }],
            ops: vec![
                ApiCommitOp::ReplaceFile {
                    inode_id,
                    base_revision_no: RevisionNo(1),
                    content_ref: second.content_ref.clone(),
                },
                ApiCommitOp::RestoreRevision {
                    inode_id,
                    source_revision_no: RevisionNo(2),
                    base_revision_no: RevisionNo(2),
                },
            ],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("missing same-request content should fail durable-content validation");
    assert!(matches!(
        error,
        CoreError::DurableContent(
            loon_core::content::DurableContentValidationError::MissingContentObject { .. }
        )
    ));
}

#[test]
fn move_path_into_occupied_target_is_path_conflict() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/a.txt",
        b"docs-a",
        &context,
        Some("seed-docs"),
    )
    .expect("seed docs");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/tmp/a.txt",
        b"tmp-a",
        &context,
        Some("seed-tmp"),
    )
    .expect("seed tmp");

    let error = move_path(
        &store,
        &namespace_id(),
        "/tmp/a.txt",
        "/docs/a.txt",
        &context,
        Some("move-conflict"),
    )
    .expect_err("move conflict");
    assert_eq!(error.kind(), CoreErrorKind::PathConflict);
}

#[test]
fn move_path_directory_cycle_is_would_cycle() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/archive/leaf.txt",
        b"leaf",
        &context,
        Some("seed-cycle"),
    )
    .expect("seed cycle dirs");

    let error = move_path(
        &store,
        &namespace_id(),
        "/docs",
        "/docs/archive/docs",
        &context,
        Some("cycle"),
    )
    .expect_err("cycle");
    assert_eq!(error.kind(), CoreErrorKind::WouldCycle);
}

#[test]
fn write_and_move_under_tombstoned_ancestor_are_tombstone_conflicts() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/old.txt",
        b"old",
        &context,
        Some("seed-docs"),
    )
    .expect("seed docs");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/tmp/source.txt",
        b"source",
        &context,
        Some("seed-source"),
    )
    .expect("seed source");
    delete_path(
        &store,
        &namespace_id(),
        "/docs",
        &context,
        Some("delete-docs"),
    )
    .expect("delete docs");

    let put_error = write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/new.txt",
        b"new",
        &context,
        Some("put-under-tombstone"),
    )
    .expect_err("put tombstone conflict");
    assert_eq!(put_error.kind(), CoreErrorKind::TombstoneConflict);

    let move_error = move_path(
        &store,
        &namespace_id(),
        "/tmp/source.txt",
        "/docs/source.txt",
        &context,
        Some("move-under-tombstone"),
    )
    .expect_err("move tombstone conflict");
    assert_eq!(move_error.kind(), CoreErrorKind::TombstoneConflict);
}

#[test]
fn put_file_create_only_rejects_existing_target_without_force() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/hello.txt",
        b"hello",
        &context,
        Some("seed-hello"),
    )
    .expect("seed file");

    let error = put_file_bytes(
        &store,
        &namespace_id(),
        "/docs/hello.txt",
        b"new-bytes",
        PutFileBehavior::CreateOnly,
        &context,
        Some("put-no-force"),
    )
    .expect_err("put without force");
    assert_eq!(error.kind(), CoreErrorKind::PathConflict);
}

#[test]
fn delete_path_non_recursive_rejects_non_empty_directory() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/hello.txt",
        b"hello",
        &context,
        Some("seed-docs"),
    )
    .expect("seed file");

    let error = delete_path_non_recursive(
        &store,
        &namespace_id(),
        "/docs",
        &context,
        Some("delete-docs"),
    )
    .expect_err("non-recursive delete should reject non-empty dir");
    assert!(matches!(error, CoreError::DirectoryNotEmpty(path) if path == "/docs"));
}

#[test]
fn copy_file_path_creates_new_inode_and_reuses_content_blob() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/source.txt",
        b"hello",
        &context,
        Some("seed-source"),
    )
    .expect("seed source");

    copy_file_path(
        &store,
        &namespace_id(),
        "/docs/source.txt",
        "/docs/copy.txt",
        &context,
        Some("copy-file"),
    )
    .expect("copy file");

    let source = resolve_path(&store, &namespace_id(), "/docs/source.txt").expect("source stat");
    let copy = resolve_path(&store, &namespace_id(), "/docs/copy.txt").expect("copy stat");
    assert_ne!(source.inode_id, copy.inode_id);
    assert_eq!(
        source.content_ref, copy.content_ref,
        "copy should reuse stored content ref"
    );
}

#[test]
fn resolve_path_uses_nfc_casefold_name_policy() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let stored_path = "/Cafe\u{0301}.txt";
    let lookup_path = "/CAF\u{00c9}.TXT";
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        stored_path,
        b"hello",
        &context,
        Some("seed-unicode-name"),
    )
    .expect("seed unicode name");

    let resolved = resolve_path(&store, &namespace_id(), lookup_path).expect("resolve path");
    assert_eq!(resolved.absolute_path, stored_path);
    assert_eq!(resolved.display_name, "Cafe\u{0301}.txt");
}

#[test]
fn create_only_put_rejects_casefold_and_normalization_equivalent_name() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/Cafe\u{0301}.txt",
        b"hello",
        &context,
        Some("seed-unicode-name"),
    )
    .expect("seed unicode name");

    let error = put_file_bytes(
        &store,
        &namespace_id(),
        "/CAF\u{00c9}.TXT",
        b"new-bytes",
        PutFileBehavior::CreateOnly,
        &context,
        Some("create-only-conflict"),
    )
    .expect_err("create-only conflict");
    assert_eq!(error.kind(), CoreErrorKind::PathConflict);
}

fn seed_source_namespace_for_fork<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    context: &MutationContext,
) {
    bootstrap_namespace(store, source_namespace_id, context, false)
        .expect("bootstrap source namespace");
    write_file_bytes(
        store,
        source_namespace_id,
        "/docs/shared.txt",
        b"base",
        context,
        Some("seed-shared"),
    )
    .expect("seed shared file");
}

fn assert_namespace_partial<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) {
    let partial_error =
        bootstrap_namespace(store, namespace_id, context, false).expect_err("partial namespace");
    assert!(matches!(
        partial_error,
        loon_core::BootstrapNamespaceError::NamespacePartiallyInitialized { .. }
    ));
}

#[derive(Debug)]
struct InjectCreateFailureStore {
    inner: LocalFsStore,
    matcher: KeyMatcher,
    failure: InjectedCreateFailure,
    injected: Mutex<bool>,
}

impl InjectCreateFailureStore {
    fn new(inner: LocalFsStore, matcher: KeyMatcher, failure: InjectedCreateFailure) -> Self {
        Self {
            inner,
            matcher,
            failure,
            injected: Mutex::new(false),
        }
    }
}

#[derive(Debug)]
enum KeyMatcher {
    Exact(String),
    Prefix(String),
}

impl KeyMatcher {
    fn matches(&self, key: &str) -> bool {
        match self {
            Self::Exact(expected) => key == expected,
            Self::Prefix(prefix) => key.starts_with(prefix),
        }
    }
}

#[derive(Debug)]
enum InjectedCreateFailure {
    Transport {
        message: &'static str,
    },
    Conflict {
        write_attempted_object: bool,
        additional_writes: Vec<(String, Vec<u8>)>,
    },
    PreconditionFailed {
        write_attempted_object: bool,
        additional_writes: Vec<(String, Vec<u8>)>,
    },
}

impl InjectedCreateFailure {
    fn apply_before_error(
        &self,
        inner: &LocalFsStore,
        attempted_key: &str,
        attempted_bytes: &[u8],
    ) -> Result<(), ObjectStoreError> {
        match self {
            Self::Transport { .. } => Ok(()),
            Self::Conflict {
                write_attempted_object,
                additional_writes,
            }
            | Self::PreconditionFailed {
                write_attempted_object,
                additional_writes,
            } => {
                if *write_attempted_object {
                    inner.put_overwrite(attempted_key, attempted_bytes)?;
                }
                for (key, bytes) in additional_writes {
                    inner.put_overwrite(key, bytes)?;
                }
                Ok(())
            }
        }
    }

    fn error(&self) -> ObjectStoreError {
        match self {
            Self::Transport { message } => ObjectStoreError::Transport((*message).to_owned()),
            Self::Conflict { .. } => ObjectStoreError::Conflict,
            Self::PreconditionFailed { .. } => ObjectStoreError::PreconditionFailed,
        }
    }
}

impl ObjectStore for InjectCreateFailureStore {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key)
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.inner.get(key, range)
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if matches!(&mode, PutMode::CreateIfAbsent) && self.matcher.matches(key) {
            let should_inject = {
                let mut injected = self
                    .injected
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if *injected {
                    false
                } else {
                    *injected = true;
                    true
                }
            };
            if should_inject {
                self.failure.apply_before_error(&self.inner, key, bytes)?;
                return Err(self.failure.error());
            }
        }

        self.inner.put(key, bytes, mode)
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.inner.list_prefix(prefix)
    }
}

fn metadata_state_after(sequences: &[Vec<loon_api::WalOp>]) -> MetadataState {
    let mut state = MetadataState {
        inodes: vec![InodeRecord {
            inode_id: InodeId(1),
            inode_kind: InodeKind::Dir,
            created_seq: ChangeSeq(0),
        }],
        direntries: Vec::new(),
        revisions: Vec::new(),
        subtree_tombstones: Vec::new(),
        request_receipts: Vec::new(),
    };

    for (index, ops) in sequences.iter().enumerate() {
        state = state
            .apply_committed_wal_ops(ChangeSeq(u64::try_from(index + 1).expect("seq")), ops)
            .expect("apply ops")
            .metadata_state;
    }

    state
}

fn validation_context(
    metadata_state: MetadataState,
    seq: ChangeSeq,
    next_inode_id: InodeId,
) -> CommitValidationContext {
    let namespace_id = namespace_id();
    let head = HeadState {
        namespace_id: namespace_id.clone(),
        seq,
        active_fence_token: FenceToken(1),
        next_inode_id,
        name_policy: loon_api::NamePolicy::default(),
        snapshot_hint_seq: Some(ChangeSeq(0)),
        retention_floor_seq: ChangeSeq(0),
        visible_wal_tip: None,
    };
    let lease = LeaseState {
        namespace_id,
        holder_id: "writer-a".to_owned(),
        fence_token: head.active_fence_token,
        lease_expires_at_ms: 10_000,
    };

    CommitValidationContext {
        head,
        lease,
        now_ms: 1_000,
        metadata_state,
    }
}

fn mutation_context() -> MutationContext {
    MutationContext {
        writer_id: "writer-a".to_owned(),
        writer_version: "writer-a/0.1.0".to_owned(),
        now_ms: 1_000,
        lease_duration_ms: 60_000,
    }
}

fn api_head_seq_is(expected_seq: ChangeSeq) -> Vec<CommitPrecondition> {
    vec![CommitPrecondition::HeadSeqIs { expected_seq }]
}

fn namespace_id() -> NamespaceId {
    NamespaceId::from("demo".to_owned())
}

fn content_ref(seed: &str) -> ContentRef {
    ContentRef {
        kind: ContentRefKind::WholeFileV0,
        digest: sha256_digest(seed.as_bytes()),
        size_bytes: seed.len() as u64,
    }
}

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
            return Err(ObjectStoreError::Transport(format!(
                "unexpected content-store descriptor access: {key}",
            )));
        }
        Ok(())
    }
}

impl ObjectStore for ContentStoreAccessLimitStore {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.record_content_store_access(key)?;
        self.inner.head(key)
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.record_content_store_access(key)?;
        self.inner.get(key, range)
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode)
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.inner.list_prefix(prefix)
    }
}

struct StaleHeadAfterWalWriteStore {
    inner: LocalFsStore,
    head_key: String,
    injected_stale_head: Mutex<bool>,
}

impl StaleHeadAfterWalWriteStore {
    fn new(root: impl AsRef<Path>, namespace_id: &NamespaceId) -> Self {
        Self {
            inner: LocalFsStore::new(root.as_ref()).expect("store"),
            head_key: namespace_head(namespace_id.as_str()),
            injected_stale_head: Mutex::new(false),
        }
    }

    fn injected_stale_head(&self) -> bool {
        *self
            .injected_stale_head
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl ObjectStore for StaleHeadAfterWalWriteStore {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key)
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.inner.get(key, range)
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if key == self.head_key && matches!(mode, PutMode::CompareAndSwap { .. }) {
            let should_inject = {
                let mut injected = self
                    .injected_stale_head
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if *injected {
                    false
                } else {
                    *injected = true;
                    true
                }
            };
            if should_inject {
                if let Some(existing) = self.inner.get(key, None)? {
                    self.inner.put_overwrite(key, &existing)?;
                }
                return Err(ObjectStoreError::PreconditionFailed);
            }
        }
        self.inner.put(key, bytes, mode)
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.inner.list_prefix(prefix)
    }
}
