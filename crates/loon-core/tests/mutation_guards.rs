use loon_api::{
    sha256_digest,
    v0::{CommitOp as V0CommitOp, CommitPrecondition, CommitRequest as V0CommitRequest},
    ChangeSeq, ContentRef, ContentRefKind, ContentStoreDescriptorEnvelope, ControlObjectKind,
    FenceToken, HeadState, InodeId, InodeKind, LeaseState, NamespaceDescriptorEnvelope,
    NamespaceId, RevisionNo,
};
use loon_core::commit::{
    build_commit_plan, CommitOp, CommitRequest, CommitValidationContext, CommitValidationError,
    Precondition,
};
use loon_core::metadata::{InodeRecord, MetadataState};
use loon_core::{
    bootstrap_namespace, commit_operations, copy_file_path, delete_path, delete_path_non_recursive,
    fork_namespace, list_changes_after, list_namespaces, load_verified_namespace_basis, move_path,
    put_file_bytes, read_file_bytes, resolve_path, store_bytes_as_content, write_file_bytes,
    CoreError, CoreErrorKind, MutationContext, PutFileBehavior,
};
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::{
    content_blob, content_store_descriptor, namespace_descriptor, namespace_head,
};
use loon_objectstore::ObjectStore;
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
    assert_eq!(listed[0].name.as_str(), "demo");

    let partial_error = bootstrap_namespace(&store, &NamespaceId::from("partial"), &context, false)
        .expect_err("partial namespace should be rejected");
    assert!(matches!(
        partial_error,
        loon_core::BootstrapNamespaceError::NamespacePartiallyInitialized { .. }
    ));
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
fn restore_revision_revalidates_durable_content_before_publish() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");

    let first = store_bytes_as_content(&store, &namespace_id(), b"first").expect("stage first");
    let create = commit_operations(
        &store,
        &namespace_id(),
        V0CommitRequest {
            request_id: "restore-create".to_owned(),
            planned_head_seq: ChangeSeq(0),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(0),
            }],
            ops: vec![V0CommitOp::CreateFile {
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
        V0CommitRequest {
            request_id: "restore-replace".to_owned(),
            planned_head_seq: ChangeSeq(1),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(1),
            }],
            ops: vec![V0CommitOp::ReplaceFile {
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
        V0CommitRequest {
            request_id: "restore-missing-content".to_owned(),
            planned_head_seq: ChangeSeq(2),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(2),
            }],
            ops: vec![V0CommitOp::RestoreRevision {
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
fn create_file_prioritizes_missing_durable_content_over_missing_parent() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");

    let error = commit_operations(
        &store,
        &namespace_id(),
        V0CommitRequest {
            request_id: "create-missing-parent-missing-content".to_owned(),
            planned_head_seq: ChangeSeq(0),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(0),
            }],
            ops: vec![V0CommitOp::CreateFile {
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
        V0CommitRequest {
            request_id: "replace-stale-missing-content".to_owned(),
            planned_head_seq: ChangeSeq(1),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(1),
            }],
            ops: vec![V0CommitOp::ReplaceFile {
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
        V0CommitRequest {
            request_id: "restore-missing-source".to_owned(),
            planned_head_seq: ChangeSeq(1),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(1),
            }],
            ops: vec![V0CommitOp::RestoreRevision {
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
        V0CommitRequest {
            request_id: "resolve-before-durable-check-create".to_owned(),
            planned_head_seq: ChangeSeq(0),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(0),
            }],
            ops: vec![V0CommitOp::CreateFile {
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
        V0CommitRequest {
            request_id: "resolve-before-durable-check-commit".to_owned(),
            planned_head_seq: ChangeSeq(1),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(1),
            }],
            ops: vec![
                V0CommitOp::ReplaceFile {
                    inode_id,
                    base_revision_no: RevisionNo(1),
                    content_ref: second.content_ref.clone(),
                },
                V0CommitOp::RestoreRevision {
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
