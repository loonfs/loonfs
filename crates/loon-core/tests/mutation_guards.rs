use loon_api::{
    ChangeSeq, FenceToken, HeadState, InodeId, InodeKind, LeaseState, NamespaceId, RevisionNo,
};
use loon_core::commit::{
    build_commit_plan, CommitOp, CommitRequest, CommitValidationContext, CommitValidationError,
    Precondition,
};
use loon_core::metadata::{InodeRecord, MetadataState};
use loon_core::{
    bootstrap_namespace, copy_file_path, delete_path, delete_path_non_recursive, move_path,
    put_file_bytes, resolve_path, write_file_bytes, CoreError, CoreErrorKind, MutationContext,
    PutFileBehavior,
};
use loon_objectstore::fs::LocalFsStore;
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
            content_manifest_digest: "sha256:manifest-1".to_owned(),
        }],
    ]);
    let context = validation_context(metadata_state, ChangeSeq(2), InodeId(4));
    let request = CommitRequest {
        namespace_id: namespace_id(),
        request_id: "stale-head".to_owned(),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(1),
        planned_head_seq: ChangeSeq(2),
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
            content_manifest_digest: "sha256:manifest-1".to_owned(),
        }],
        vec![loon_api::WalOp::ReplaceFile {
            op_index: 0,
            inode_id: InodeId(3),
            base_revision: RevisionNo(1),
            content_manifest_digest: "sha256:manifest-2".to_owned(),
        }],
    ]);
    let context = validation_context(metadata_state, ChangeSeq(3), InodeId(4));
    let request = CommitRequest {
        namespace_id: namespace_id(),
        request_id: "stale-revision".to_owned(),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(1),
        planned_head_seq: ChangeSeq(3),
        ops: vec![CommitOp::ReplaceFile {
            inode_id: InodeId(3),
            base_revision: RevisionNo(1),
            content_manifest_digest: "sha256:manifest-3".to_owned(),
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
            content_manifest_digest: "sha256:manifest-1".to_owned(),
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
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(2),
                display_name: "new.txt".to_owned(),
                content_manifest_digest: "sha256:manifest-2".to_owned(),
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
            ops: vec![CommitOp::ReplaceFile {
                inode_id: InodeId(3),
                base_revision: RevisionNo(1),
                content_manifest_digest: "sha256:manifest-2".to_owned(),
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
fn copy_file_path_creates_new_inode_and_reuses_content_manifest() {
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
        source.content_manifest_digest, copy.content_manifest_digest,
        "copy should reuse stored content manifest"
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
