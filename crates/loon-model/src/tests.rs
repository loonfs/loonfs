use super::*;
use crate::client::{
    authoritative_snapshot_import_batch_rollback_is_atomic,
    authoritative_snapshot_import_discovers_remote_only_state,
    authoritative_snapshot_import_is_idempotent,
    bound_directory_delete_observation_plans_delete_subtree,
    bound_file_delete_observation_plans_delete_file,
    bound_file_observation_plans_upload_local_edit, bound_move_observation_plans_rename,
    fs_event_atomic_save_returns_error, fs_event_conflicting_native_id_reuse_returns_error,
    fs_event_conflicting_rename_edges_return_error,
    fs_event_create_then_write_reduces_to_observe_local,
    fs_event_delete_burst_reduces_to_highest_root_delete,
    fs_event_descendants_under_root_move_or_delete_are_absorbed,
    fs_event_rename_reduces_to_observe_move, fs_event_repeated_edits_reduce_to_one_subtree,
    local_only_delete_clears_temp_state, local_only_move_preserves_identity,
    local_only_observation_under_bound_parent_plans_upload_local_create,
    recursive_subtree_ambiguous_directory_pairing_fails_closed,
    recursive_subtree_ambiguous_file_pairing_fails_closed,
    recursive_subtree_changed_digest_file_does_not_pair,
    recursive_subtree_directory_move_with_descendant_drift_does_not_pair,
    recursive_subtree_missing_bound_directory_plans_delete_subtree,
    recursive_subtree_missing_bound_file_plans_delete_file,
    recursive_subtree_missing_local_only_directory_clears_subtree,
    recursive_subtree_observation_updates_bound_edit_and_local_only_create,
    recursive_subtree_repeat_reuses_local_only_identity,
    recursive_subtree_unique_bound_directory_move_is_paired,
    recursive_subtree_unique_bound_file_move_is_paired,
    recursive_subtree_unique_local_only_directory_move_preserves_identity,
    recursive_subtree_unique_local_only_file_move_preserves_identity,
    repeated_local_only_observation_reuses_identity, subtree_observation_batch_rollback_is_atomic,
    sync_until_idle_fails_on_max_steps, sync_until_idle_stops_on_no_work,
};
use loon_types::{
    ChangeSeq, FenceToken, InodeId, InodeKind, LeaseState, NamespaceId, RevisionNo,
    SubtreeConflictArtifactEntry, CONTENT_BLOCK_SIZE_BYTES,
};
use serde_json::json;
use std::collections::BTreeMap;

fn seeded_metadata_state() -> ModelMetadataState {
    ModelMetadataState {
        inodes: vec![
            ModelInodeRecord {
                inode_id: InodeId(1),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(0),
            },
            ModelInodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            },
            ModelInodeRecord {
                inode_id: InodeId(7),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(5),
            },
            ModelInodeRecord {
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(12),
            },
            ModelInodeRecord {
                inode_id: InodeId(88),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(21),
            },
        ],
        direntries: vec![
            ModelDirentryRecord {
                parent_inode_id: InodeId(1),
                name_key: "workspace".to_owned(),
                display_name: "workspace".to_owned(),
                child_inode_id: InodeId(2),
                bind_seq: ChangeSeq(1),
                bind_op_index: 0,
            },
            ModelDirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "note.txt".to_owned(),
                display_name: "note.txt".to_owned(),
                child_inode_id: InodeId(42),
                bind_seq: ChangeSeq(41),
                bind_op_index: 0,
            },
            ModelDirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "docs".to_owned(),
                display_name: "docs".to_owned(),
                child_inode_id: InodeId(7),
                bind_seq: ChangeSeq(5),
                bind_op_index: 0,
            },
            ModelDirentryRecord {
                parent_inode_id: InodeId(7),
                name_key: "report.txt".to_owned(),
                display_name: "report.txt".to_owned(),
                child_inode_id: InodeId(88),
                bind_seq: ChangeSeq(21),
                bind_op_index: 0,
            },
        ],
        revisions: vec![
            ModelRevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(17),
                revision_op_index: 0,
                content_manifest_digest: "sha256:note-v1".to_owned(),
            },
            ModelRevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(2),
                committed_seq: ChangeSeq(41),
                revision_op_index: 0,
                content_manifest_digest: "sha256:note-v2".to_owned(),
            },
            ModelRevisionRecord {
                inode_id: InodeId(88),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(21),
                revision_op_index: 0,
                content_manifest_digest: "sha256:report-v1".to_owned(),
            },
        ],
        subtree_tombstones: vec![ModelSubtreeTombstoneRecord {
            root_inode_id: InodeId(7),
            tombstone_seq: ChangeSeq(40),
            tombstone_op_index: 0,
        }],
    }
}

fn bootstrapped_metadata_state() -> ModelMetadataState {
    ModelMetadataState {
        inodes: vec![ModelInodeRecord {
            inode_id: InodeId(1),
            inode_kind: InodeKind::Dir,
            created_seq: ChangeSeq(0),
        }],
        direntries: Vec::new(),
        revisions: Vec::new(),
        subtree_tombstones: Vec::new(),
    }
}

#[test]
fn model_advances_seq() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::BumpSeq {
        writer_fence_token: FenceToken(0),
    })
    .expect("active writer should advance seq");
    assert_eq!(ns.head_seq.0, 1);
}

#[test]
fn model_new_namespace_seeds_root_inode_and_allocator() {
    let ns = ModelNamespace::new(NamespaceId::from("ns-1"));

    assert_eq!(ns.head_seq, ChangeSeq(0));
    assert_eq!(ns.next_inode_id, InodeId(2));
    assert_eq!(
        ns.metadata_state,
        ModelMetadataState {
            inodes: vec![ModelInodeRecord {
                inode_id: InodeId(1),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(0),
            }],
            direntries: Vec::new(),
            revisions: Vec::new(),
            subtree_tombstones: Vec::new(),
        }
    );
}

#[test]
fn model_create_dir_advances_next_inode_id() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::CreateDir {
        inode_id: InodeId(7),
        writer_fence_token: FenceToken(0),
    })
    .expect("create dir should advance next inode id");

    assert_eq!(ns.head_seq, ChangeSeq(1));
    assert_eq!(ns.next_inode_id, InodeId(8));
}

#[test]
fn model_create_file_advances_next_inode_id() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::CreateFile {
        inode_id: InodeId(7),
        writer_fence_token: FenceToken(0),
    })
    .expect("create file should advance next inode id");

    assert_eq!(ns.head_seq, ChangeSeq(1));
    assert_eq!(ns.next_inode_id, InodeId(8));
}

#[test]
fn model_bound_file_observation_plans_upload_local_edit() {
    assert!(bound_file_observation_plans_upload_local_edit(
        "upload_local_edit",
        true,
        true
    ));
    assert!(!bound_file_observation_plans_upload_local_edit(
        "download_remote_edit",
        true,
        true
    ));
}

#[test]
fn model_local_only_observation_under_bound_parent_plans_upload_local_create() {
    assert!(
        local_only_observation_under_bound_parent_plans_upload_local_create(
            "upload_local_create",
            true,
            true,
            true
        )
    );
    assert!(
        !local_only_observation_under_bound_parent_plans_upload_local_create(
            "upload_local_create",
            false,
            true,
            true
        )
    );
}

#[test]
fn model_repeated_local_only_observation_reuses_temp_identity() {
    assert!(repeated_local_only_observation_reuses_identity(
        "tmp:ns-1:00000000000000000001",
        "tmp:ns-1:00000000000000000001"
    ));
    assert!(!repeated_local_only_observation_reuses_identity(
        "tmp:ns-1:00000000000000000001",
        "tmp:ns-1:00000000000000000002"
    ));
}

#[test]
fn model_bound_file_delete_observation_plans_delete_file() {
    assert!(bound_file_delete_observation_plans_delete_file(
        "delete_file",
        false,
        true
    ));
    assert!(!bound_file_delete_observation_plans_delete_file(
        "upload_local_edit",
        false,
        true
    ));
}

#[test]
fn model_bound_directory_delete_observation_plans_delete_subtree() {
    assert!(bound_directory_delete_observation_plans_delete_subtree(
        "delete_subtree",
        false,
        true
    ));
    assert!(!bound_directory_delete_observation_plans_delete_subtree(
        "rename", false, true
    ));
}

#[test]
fn model_bound_move_observation_plans_rename() {
    assert!(bound_move_observation_plans_rename(
        "rename", true, true, true
    ));
    assert!(!bound_move_observation_plans_rename(
        "upload_local_edit",
        true,
        true,
        true
    ));
}

#[test]
fn model_local_only_delete_clears_temp_state() {
    assert!(local_only_delete_clears_temp_state(true, true));
    assert!(!local_only_delete_clears_temp_state(true, false));
}

#[test]
fn model_local_only_move_preserves_identity() {
    assert!(local_only_move_preserves_identity(
        "tmp:ns-1:00000000000000000001",
        "tmp:ns-1:00000000000000000001"
    ));
    assert!(!local_only_move_preserves_identity(
        "tmp:ns-1:00000000000000000001",
        "tmp:ns-1:00000000000000000002"
    ));
}

#[test]
fn model_recursive_subtree_observation_updates_bound_edit_and_local_only_create() {
    assert!(
        recursive_subtree_observation_updates_bound_edit_and_local_only_create(
            "upload_local_edit",
            "upload_local_create"
        )
    );
    assert!(
        !recursive_subtree_observation_updates_bound_edit_and_local_only_create(
            "rename",
            "upload_local_create"
        )
    );
}

#[test]
fn model_recursive_subtree_unique_bound_file_move_is_paired() {
    assert!(recursive_subtree_unique_bound_file_move_is_paired(
        "rename", 1
    ));
    assert!(!recursive_subtree_unique_bound_file_move_is_paired(
        "upload_local_edit",
        1
    ));
}

#[test]
fn model_recursive_subtree_unique_local_only_file_move_preserves_identity() {
    assert!(
        recursive_subtree_unique_local_only_file_move_preserves_identity(
            "tmp:ns-1:00000000000000000001",
            "tmp:ns-1:00000000000000000001",
            1
        )
    );
    assert!(
        !recursive_subtree_unique_local_only_file_move_preserves_identity(
            "tmp:ns-1:00000000000000000001",
            "tmp:ns-1:00000000000000000002",
            1
        )
    );
}

#[test]
fn model_recursive_subtree_ambiguous_file_pairing_fails_closed() {
    assert!(recursive_subtree_ambiguous_file_pairing_fails_closed(
        "ambiguous_move_pairing"
    ));
    assert!(!recursive_subtree_ambiguous_file_pairing_fails_closed(
        "source_not_tracked"
    ));
}

#[test]
fn model_recursive_subtree_unique_bound_directory_move_is_paired() {
    assert!(recursive_subtree_unique_bound_directory_move_is_paired(
        "rename", 1
    ));
    assert!(!recursive_subtree_unique_bound_directory_move_is_paired(
        "rename", 0
    ));
}

#[test]
fn model_recursive_subtree_unique_local_only_directory_move_preserves_identity() {
    assert!(
        recursive_subtree_unique_local_only_directory_move_preserves_identity(
            "tmp:ns-1:00000000000000000001",
            "tmp:ns-1:00000000000000000001",
            "tmp:ns-1:00000000000000000002",
            "tmp:ns-1:00000000000000000002",
            1
        )
    );
    assert!(
        !recursive_subtree_unique_local_only_directory_move_preserves_identity(
            "tmp:ns-1:00000000000000000001",
            "tmp:ns-1:00000000000000000003",
            "tmp:ns-1:00000000000000000002",
            "tmp:ns-1:00000000000000000002",
            1
        )
    );
}

#[test]
fn model_recursive_subtree_ambiguous_directory_pairing_fails_closed() {
    assert!(recursive_subtree_ambiguous_directory_pairing_fails_closed(
        "ambiguous_move_pairing"
    ));
    assert!(!recursive_subtree_ambiguous_directory_pairing_fails_closed(
        "untracked_parent"
    ));
}

#[test]
fn model_recursive_subtree_directory_move_with_descendant_drift_does_not_pair() {
    assert!(recursive_subtree_directory_move_with_descendant_drift_does_not_pair(0, 0));
    assert!(!recursive_subtree_directory_move_with_descendant_drift_does_not_pair(1, 0));
}

#[test]
fn model_recursive_subtree_changed_digest_file_does_not_pair() {
    assert!(recursive_subtree_changed_digest_file_does_not_pair(0, 0));
    assert!(!recursive_subtree_changed_digest_file_does_not_pair(0, 1));
}

#[test]
fn model_recursive_subtree_missing_bound_file_plans_delete_file() {
    assert!(recursive_subtree_missing_bound_file_plans_delete_file(
        "delete_file"
    ));
    assert!(!recursive_subtree_missing_bound_file_plans_delete_file(
        "upload_local_edit"
    ));
}

#[test]
fn model_recursive_subtree_missing_bound_directory_plans_delete_subtree() {
    assert!(recursive_subtree_missing_bound_directory_plans_delete_subtree("delete_subtree"));
    assert!(!recursive_subtree_missing_bound_directory_plans_delete_subtree("delete_file"));
}

#[test]
fn model_recursive_subtree_missing_local_only_directory_clears_subtree() {
    assert!(recursive_subtree_missing_local_only_directory_clears_subtree(2));
    assert!(!recursive_subtree_missing_local_only_directory_clears_subtree(0));
}

#[test]
fn model_recursive_subtree_repeat_reuses_local_only_identity() {
    assert!(recursive_subtree_repeat_reuses_local_only_identity(
        "tmp:ns-1:00000000000000000001",
        "tmp:ns-1:00000000000000000001"
    ));
    assert!(!recursive_subtree_repeat_reuses_local_only_identity(
        "tmp:ns-1:00000000000000000001",
        "tmp:ns-1:00000000000000000002"
    ));
}

#[test]
fn model_fs_event_create_then_write_reduces_to_observe_local() {
    assert!(fs_event_create_then_write_reduces_to_observe_local(&[
        "observe_local",
    ]));
    assert!(!fs_event_create_then_write_reduces_to_observe_local(&[
        "observe_subtree",
    ]));
}

#[test]
fn model_fs_event_rename_reduces_to_observe_move() {
    assert!(fs_event_rename_reduces_to_observe_move(&["observe_move"]));
    assert!(!fs_event_rename_reduces_to_observe_move(&[
        "observe_delete"
    ]));
}

#[test]
fn model_fs_event_repeated_edits_reduce_to_one_subtree() {
    assert!(fs_event_repeated_edits_reduce_to_one_subtree(
        &["observe_subtree"],
        &["docs"]
    ));
    assert!(!fs_event_repeated_edits_reduce_to_one_subtree(
        &["observe_local"],
        &["docs"]
    ));
}

#[test]
fn model_fs_event_delete_burst_reduces_to_highest_root_delete() {
    assert!(fs_event_delete_burst_reduces_to_highest_root_delete(
        &["observe_delete"],
        &["docs"],
        "docs"
    ));
    assert!(!fs_event_delete_burst_reduces_to_highest_root_delete(
        &["observe_delete", "observe_delete"],
        &["docs", "docs/note.txt"],
        "docs"
    ));
}

#[test]
fn model_fs_event_atomic_save_returns_error() {
    assert!(fs_event_atomic_save_returns_error(
        "contradictory_path_events"
    ));
    assert!(!fs_event_atomic_save_returns_error(
        "ambiguous_rename_source"
    ));
}

#[test]
fn model_fs_event_conflicting_rename_edges_return_error() {
    assert!(fs_event_conflicting_rename_edges_return_error(
        "ambiguous_rename_source"
    ));
    assert!(!fs_event_conflicting_rename_edges_return_error(
        "ambiguous_native_object_id"
    ));
}

#[test]
fn model_fs_event_conflicting_native_id_reuse_returns_error() {
    assert!(fs_event_conflicting_native_id_reuse_returns_error(
        "ambiguous_native_object_id"
    ));
    assert!(!fs_event_conflicting_native_id_reuse_returns_error(
        "contradictory_path_events"
    ));
}

#[test]
fn model_fs_event_descendants_under_root_move_or_delete_are_absorbed() {
    assert!(fs_event_descendants_under_root_move_or_delete_are_absorbed(
        &["observe_move"],
        1
    ));
    assert!(fs_event_descendants_under_root_move_or_delete_are_absorbed(
        &["observe_delete"],
        1
    ));
    assert!(
        !fs_event_descendants_under_root_move_or_delete_are_absorbed(
            &["observe_move", "observe_local"],
            2
        )
    );
}

#[test]
fn model_subtree_observation_batch_rollback_is_atomic() {
    let before = vec!["hello.txt", "drafts/note.txt"];
    let after = vec!["hello.txt", "drafts/note.txt"];
    let changed = vec!["hello.txt"];
    assert!(subtree_observation_batch_rollback_is_atomic(
        &before, &after
    ));
    assert!(!subtree_observation_batch_rollback_is_atomic(
        &before, &changed
    ));
}

#[test]
fn model_sync_until_idle_stops_on_no_work() {
    assert!(sync_until_idle_stops_on_no_work("no_work", 2, 50));
    assert!(!sync_until_idle_stops_on_no_work(
        "request_committed",
        2,
        50
    ));
}

#[test]
fn model_sync_until_idle_fails_on_max_steps() {
    assert!(sync_until_idle_fails_on_max_steps(1, 1));
    assert!(!sync_until_idle_fails_on_max_steps(1, 2));
}

#[test]
fn model_builds_uploaded_content_for_small_file() {
    let uploaded = build_uploaded_content(NamespaceId::from("ns-1"), b"hello from loon\n")
        .expect("build uploaded content");

    assert_eq!(uploaded.file_size_bytes, 16);
    assert_eq!(
        uploaded.file_digest_sha256,
        "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
    );
    assert_eq!(uploaded.manifest_envelope.payload.blocks.len(), 1);
    assert_eq!(
        uploaded.manifest_envelope.payload.blocks[0].plaintext_size_bytes,
        16
    );
}

#[test]
fn model_splits_content_at_fixed_block_boundary() {
    let mut bytes = vec![b'a'; CONTENT_BLOCK_SIZE_BYTES as usize];
    bytes.push(b'b');

    let uploaded =
        build_uploaded_content(NamespaceId::from("ns-1"), &bytes).expect("build uploaded content");

    assert_eq!(uploaded.manifest_envelope.payload.blocks.len(), 2);
    assert_eq!(
        uploaded.manifest_envelope.payload.blocks[0].plaintext_size_bytes,
        CONTENT_BLOCK_SIZE_BYTES
    );
    assert_eq!(
        uploaded.manifest_envelope.payload.blocks[1].plaintext_size_bytes,
        1
    );
}

#[test]
fn model_validates_uploaded_content_reference() {
    let uploaded = build_uploaded_content(NamespaceId::from("ns-1"), b"hello from loon\n")
        .expect("build uploaded content");
    let mut blocks = BTreeMap::new();
    blocks.insert(
        "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9".to_owned(),
        b"hello from loon\n".to_vec(),
    );

    let validated = validate_uploaded_content_reference(
        &NamespaceId::from("ns-1"),
        &uploaded.content_manifest_digest,
        &uploaded.manifest_envelope,
        &blocks,
    )
    .expect("validate uploaded content reference");

    assert_eq!(validated.file_size_bytes, 16);
    assert_eq!(
        validated.file_digest_sha256,
        "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
    );
    assert_eq!(validated.block_count, 1);
}

#[test]
fn model_materializes_uploaded_content_reference() {
    let uploaded = build_uploaded_content(NamespaceId::from("ns-1"), b"hello from loon\n")
        .expect("build uploaded content");
    let mut blocks = BTreeMap::new();
    blocks.insert(
        "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9".to_owned(),
        b"hello from loon\n".to_vec(),
    );

    let materialized = materialize_uploaded_content_reference(
        &NamespaceId::from("ns-1"),
        &uploaded.content_manifest_digest,
        &uploaded.manifest_envelope,
        &blocks,
    )
    .expect("materialize uploaded content reference");

    assert_eq!(materialized.file_size_bytes, 16);
    assert_eq!(
        materialized.file_digest_sha256,
        "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
    );
    assert_eq!(materialized.bytes, b"hello from loon\n");
}

#[test]
fn model_validates_local_only_upload_record() {
    let upload = ModelLocalOnlyUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

    let resolved = validate_local_only_upload_record(
        &NamespaceId::from("ns-1"),
        Some("sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"),
        &upload,
    )
    .expect("validate local-only upload");

    assert_eq!(
        resolved,
        "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
    );
}

#[test]
fn model_validates_inode_upload_record() {
    let upload = ModelInodeUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

    let resolved = validate_inode_upload_record(
        &NamespaceId::from("ns-1"),
        Some("sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"),
        &upload,
    )
    .expect("validate inode upload");

    assert_eq!(
        resolved,
        "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
    );
}

#[test]
fn model_restores_file_conflict_artifact_into_explicit_destination() {
    let restored = model_restore_file_conflict_artifact(
        "/tmp/recovered/report.txt",
        false,
        true,
        true,
        Some("sha256:loser-manifest"),
    )
    .expect("restore file conflict artifact");

    assert_eq!(restored.destination_path, "/tmp/recovered/report.txt");
    assert_eq!(restored.content_manifest_digest, "sha256:loser-manifest");
}

#[test]
fn model_file_conflict_restore_rejects_occupied_destination() {
    let error = model_restore_file_conflict_artifact(
        "/tmp/recovered/report.txt",
        true,
        true,
        true,
        Some("sha256:loser-manifest"),
    )
    .expect_err("occupied destination should fail");

    assert_eq!(
        error,
        ModelConflictArtifactRestoreError::DestinationExists {
            destination_path: "/tmp/recovered/report.txt".to_owned(),
        }
    );
}

#[test]
fn model_conflict_artifacts_are_implicitly_active_without_archive_sidecar() {
    assert_eq!(
        model_conflict_artifact_lifecycle_state(None),
        ModelConflictArtifactLifecycleState::Active
    );
}

#[test]
fn model_archive_transition_marks_conflict_artifact_archived() {
    assert_eq!(
        model_archive_conflict_artifact(),
        ModelConflictArtifactLifecycleState::Archived
    );
    assert_eq!(
        model_conflict_artifact_lifecycle_state(Some(1_700_000_000_000)),
        ModelConflictArtifactLifecycleState::Archived
    );
}

#[test]
fn model_unarchive_transition_restores_active_visibility() {
    assert_eq!(
        model_unarchive_conflict_artifact(),
        ModelConflictArtifactLifecycleState::Active
    );
    assert!(model_conflict_artifact_matches_filter(
        ModelConflictArtifactLifecycleState::Active,
        ModelConflictArtifactListFilter::Active
    ));
    assert!(!model_conflict_artifact_matches_filter(
        ModelConflictArtifactLifecycleState::Archived,
        ModelConflictArtifactListFilter::Active
    ));
    assert!(model_conflict_artifact_matches_filter(
        ModelConflictArtifactLifecycleState::Archived,
        ModelConflictArtifactListFilter::Archived
    ));
    assert!(model_conflict_artifact_matches_filter(
        ModelConflictArtifactLifecycleState::Archived,
        ModelConflictArtifactListFilter::All
    ));
}

#[test]
fn model_restores_subtree_conflict_artifact_in_deterministic_entry_order() {
    let restored = model_restore_subtree_conflict_artifact(
        "/tmp/recovered/reports-restored",
        false,
        true,
        true,
        &[
            SubtreeConflictArtifactEntry {
                relative_path: "docs".to_owned(),
                inode_kind: InodeKind::Dir,
                inode_id: Some(InodeId(7)),
                client_file_id: None,
                base_revision_no: None,
                content_manifest_digest: None,
                content_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "docs".to_owned(),
            },
            SubtreeConflictArtifactEntry {
                relative_path: "docs/report.txt".to_owned(),
                inode_kind: InodeKind::File,
                inode_id: Some(InodeId(8)),
                client_file_id: None,
                base_revision_no: Some(RevisionNo(2)),
                content_manifest_digest: Some("sha256:loser-report".to_owned()),
                content_digest: Some("sha256:loser-bytes".to_owned()),
                parent_inode_id: Some(InodeId(7)),
                display_name: "report.txt".to_owned(),
            },
        ],
    )
    .expect("restore subtree artifact");

    assert_eq!(restored.destination_root, "/tmp/recovered/reports-restored");
    assert_eq!(restored.entries.len(), 2);
    assert_eq!(restored.entries[0].relative_path, "docs");
    assert_eq!(restored.entries[1].relative_path, "docs/report.txt");
}

#[test]
fn model_subtree_conflict_restore_rejects_unsorted_entries() {
    let error = model_restore_subtree_conflict_artifact(
        "/tmp/recovered/reports-restored",
        false,
        true,
        true,
        &[
            SubtreeConflictArtifactEntry {
                relative_path: "docs/report.txt".to_owned(),
                inode_kind: InodeKind::File,
                inode_id: Some(InodeId(8)),
                client_file_id: None,
                base_revision_no: Some(RevisionNo(2)),
                content_manifest_digest: Some("sha256:loser-report".to_owned()),
                content_digest: Some("sha256:loser-bytes".to_owned()),
                parent_inode_id: Some(InodeId(7)),
                display_name: "report.txt".to_owned(),
            },
            SubtreeConflictArtifactEntry {
                relative_path: "docs".to_owned(),
                inode_kind: InodeKind::Dir,
                inode_id: Some(InodeId(7)),
                client_file_id: None,
                base_revision_no: None,
                content_manifest_digest: None,
                content_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "docs".to_owned(),
            },
        ],
    )
    .expect_err("unsorted subtree entries should fail");

    assert_eq!(
        error,
        ModelConflictArtifactRestoreError::SubtreeEntriesUnordered {
            previous_relative_path: "docs/report.txt".to_owned(),
            current_relative_path: "docs".to_owned(),
        }
    );
}

#[test]
fn model_restore_does_not_change_conflict_artifact_archive_state() {
    let lifecycle_before = model_conflict_artifact_lifecycle_state(Some(1_700_000_000_000));
    let _restored = model_restore_file_conflict_artifact(
        "/tmp/recovered/report.txt",
        false,
        true,
        true,
        Some("sha256:loser-manifest"),
    )
    .expect("restore file conflict artifact");

    assert_eq!(
        lifecycle_before,
        ModelConflictArtifactLifecycleState::Archived
    );
    assert_eq!(
        lifecycle_before,
        model_conflict_artifact_lifecycle_state(Some(1_700_000_000_000))
    );
}

#[test]
fn model_reuses_matching_local_only_upload_record() {
    let upload = ModelLocalOnlyUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

    let decision = decide_local_only_upload_action(
        &NamespaceId::from("ns-1"),
        Some("sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"),
        Some(&upload),
    )
    .expect("decide upload action");

    assert_eq!(
        decision,
        ModelLocalOnlyUploadDecision::ReuseExisting {
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6".to_owned(),
        }
    );
}

#[test]
fn model_reuses_matching_inode_upload_record() {
    let upload = ModelInodeUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

    let decision = decide_inode_upload_action(
        &NamespaceId::from("ns-1"),
        Some("sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"),
        Some(&upload),
    )
    .expect("decide inode upload action");

    assert_eq!(
        decision,
        ModelInodeUploadDecision::ReuseExisting {
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6".to_owned(),
        }
    );
}

#[test]
fn model_reuploads_when_existing_local_only_upload_is_stale() {
    let upload = ModelLocalOnlyUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

    let decision = decide_local_only_upload_action(
        &NamespaceId::from("ns-1"),
        Some("sha256:edited-after-upload"),
        Some(&upload),
    )
    .expect("stale upload should trigger reupload");

    assert_eq!(decision, ModelLocalOnlyUploadDecision::UploadFresh);
}

#[test]
fn model_reuploads_when_existing_inode_upload_is_stale() {
    let upload = ModelInodeUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

    let decision = decide_inode_upload_action(
        &NamespaceId::from("ns-1"),
        Some("sha256:edited-after-upload"),
        Some(&upload),
    )
    .expect("stale inode upload should trigger reupload");

    assert_eq!(decision, ModelInodeUploadDecision::UploadFresh);
}

#[test]
fn model_allocates_client_request_ids_monotonically() {
    assert_eq!(
        allocate_client_request_id(1),
        "client-req-00000000000000000001"
    );
    assert_eq!(
        allocate_client_request_id(2),
        "client-req-00000000000000000002"
    );
}

#[test]
fn model_reuses_existing_client_request_id_for_retry() {
    let (request_id, allocated_new) =
        reuse_or_allocate_client_request_id(Some("client-req-00000000000000000007"), 8);

    assert_eq!(request_id, "client-req-00000000000000000007");
    assert!(!allocated_new);
}

#[test]
fn model_retry_reuse_requires_stable_pending_request_id() {
    assert!(retry_reuses_pending_request_id(
        Some("client-req-00000000000000000007"),
        Some("client-req-00000000000000000007"),
        Some("client-req-00000000000000000007"),
    ));
    assert!(!retry_reuses_pending_request_id(
        Some("client-req-00000000000000000007"),
        Some("client-req-00000000000000000008"),
        Some("client-req-00000000000000000008"),
    ));
}

#[test]
fn model_duplicate_response_idempotence_requires_no_duplicate_winner_apply() {
    assert!(duplicate_response_delivery_is_idempotent(true, true, 0));
    assert!(!duplicate_response_delivery_is_idempotent(true, true, 1));
}

#[test]
fn model_late_remote_observation_converges_once() {
    assert!(late_remote_observation_converges_once(true, true, 1));
    assert!(!late_remote_observation_converges_once(true, true, 2));
}

#[test]
fn model_delivery_order_stability_is_exact_match() {
    assert!(delivery_order_is_seed_stable(
        &[1_u64, 2, 3],
        &[1_u64, 2, 3]
    ));
    assert!(!delivery_order_is_seed_stable(
        &[1_u64, 3, 2],
        &[1_u64, 2, 3]
    ));
}

#[test]
fn model_stale_writer_remains_fenced_after_handover() {
    assert!(stale_writer_stays_fenced_after_handover(
        true,
        true,
        FenceToken(8),
        FenceToken(9),
    ));
    assert!(!stale_writer_stays_fenced_after_handover(
        false,
        true,
        FenceToken(8),
        FenceToken(9),
    ));
}

#[test]
fn model_checkpoint_publish_wait_requires_block_then_success() {
    assert!(checkpoint_publish_waits_for_required_progress(true, true));
    assert!(!checkpoint_publish_waits_for_required_progress(true, false));
}

#[test]
fn model_checkpoint_publish_head_summary_monotonicity_rejects_regression() {
    assert!(checkpoint_publish_head_summary_is_monotonic(
        Some(ChangeSeq(40)),
        Some(ChangeSeq(40)),
        Some(ChangeSeq(42)),
        ChangeSeq(40),
        ChangeSeq(40),
        ChangeSeq(42),
    ));
    assert!(!checkpoint_publish_head_summary_is_monotonic(
        Some(ChangeSeq(40)),
        Some(ChangeSeq(39)),
        Some(ChangeSeq(42)),
        ChangeSeq(40),
        ChangeSeq(39),
        ChangeSeq(42),
    ));
}

#[test]
fn model_snapshot_repair_targets_latest_visible_head_seq() {
    assert!(snapshot_repair_tracks_latest_visible_head_seq(
        Some(ChangeSeq(45)),
        ChangeSeq(45),
    ));
    assert!(!snapshot_repair_tracks_latest_visible_head_seq(
        Some(ChangeSeq(44)),
        ChangeSeq(45),
    ));
}

#[test]
fn model_response_after_newer_observation_is_idempotent() {
    assert!(response_after_newer_observation_is_idempotent(
        true, true, false, 1,
    ));
    assert!(!response_after_newer_observation_is_idempotent(
        true, true, true, 1,
    ));
}

#[test]
fn model_checkpoint_publish_uses_latest_visible_head_seq() {
    assert!(checkpoint_publish_uses_latest_visible_head(
        Some(ChangeSeq(42)),
        ChangeSeq(42),
    ));
    assert!(!checkpoint_publish_uses_latest_visible_head(
        Some(ChangeSeq(41)),
        ChangeSeq(42),
    ));
}

#[test]
fn model_stale_writer_fence_survives_inflight_client_request() {
    assert!(stale_writer_fence_survives_inflight_client_request(
        true, true, true,
    ));
    assert!(!stale_writer_fence_survives_inflight_client_request(
        false, true, true,
    ));
}

#[test]
fn model_builds_deterministic_download_transfer_id() {
    assert_eq!(
        download_transfer_id(
            &NamespaceId::from("ns-1"),
            InodeId(601),
            "sha256:manifest-abc"
        ),
        "download:ns-1:601:sha256:manifest-abc"
    );
}

#[test]
fn model_builds_deterministic_upload_transfer_id() {
    assert_eq!(
        upload_transfer_id(
            &NamespaceId::from("ns-1"),
            InodeId(42),
            "sha256:manifest-abc"
        ),
        "upload:ns-1:42:sha256:manifest-abc"
    );
}

#[test]
fn model_builds_deterministic_local_only_upload_transfer_id() {
    assert_eq!(
        local_only_upload_transfer_id("tmp:ns-1:00000000000000000001", "sha256:manifest-abc"),
        "upload-local-only:tmp:ns-1:00000000000000000001:sha256:manifest-abc"
    );
}

#[test]
fn model_sums_expected_download_prefix_size() {
    assert_eq!(expected_download_staged_size(&[6, 10, 4], 0), 0);
    assert_eq!(expected_download_staged_size(&[6, 10, 4], 1), 6);
    assert_eq!(expected_download_staged_size(&[6, 10, 4], 2), 16);
    assert_eq!(expected_download_staged_size(&[6, 10, 4], 99), 20);
}

#[test]
fn model_resumes_download_only_when_stage_matches_expected_prefix() {
    assert_eq!(
        reconcile_download_resume_block_index(1, &[6, 10], 6),
        1,
        "matching stage length should resume at the recorded next block index"
    );
    assert_eq!(
        reconcile_download_resume_block_index(1, &[6, 10], 5),
        0,
        "mismatched stage length should reset to block zero"
    );
    assert_eq!(
        reconcile_download_resume_block_index(99, &[6, 10], 16),
        2,
        "resume block index should clamp to the manifest block count"
    );
}

#[test]
fn model_resumes_upload_only_when_transfer_row_matches_current_plan() {
    assert_eq!(
        reconcile_upload_resume_block_index(1, 2, true),
        1,
        "matching upload plan should resume at the recorded next block index"
    );
    assert_eq!(
        reconcile_upload_resume_block_index(1, 2, false),
        0,
        "mismatched upload plan should restart from block zero"
    );
    assert_eq!(
        reconcile_upload_resume_block_index(99, 2, true),
        2,
        "resume block index should clamp to the planned block count"
    );
}

#[test]
fn model_selects_next_local_only_action_deterministically() {
    let selected = select_next_local_only_action(&[
        ModelPlannedLocalOnlyAction {
            client_file_id: "tmp:ns-1:00000000000000000003".to_owned(),
            created_at_ms: 1_700_000_300_000,
        },
        ModelPlannedLocalOnlyAction {
            client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
            created_at_ms: 1_700_000_200_000,
        },
        ModelPlannedLocalOnlyAction {
            client_file_id: "tmp:ns-1:00000000000000000002".to_owned(),
            created_at_ms: 1_700_000_200_000,
        },
    ])
    .expect("one action should be selected");

    assert_eq!(
        selected,
        ModelPlannedLocalOnlyAction {
            client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
            created_at_ms: 1_700_000_200_000,
        }
    );
}

#[test]
fn model_selects_next_client_action_preferring_local_only_on_tie() {
    let selected = select_next_client_action(
        Some(&ModelPlannedLocalOnlyAction {
            client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
            created_at_ms: 1_700_000_200_000,
        }),
        Some(&ModelPlannedInodeAction {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            created_at_ms: 1_700_000_200_000,
        }),
        None,
    )
    .expect("one action should be selected");

    assert_eq!(
        selected,
        ModelScheduledClientAction::LocalOnlyCreate(ModelPlannedLocalOnlyAction {
            client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })
    );
}

#[test]
fn model_selects_executable_inode_action_before_deferred_inode_action() {
    let selected = select_next_client_action(
        None,
        Some(&ModelPlannedInodeAction {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            created_at_ms: 1_700_000_205_000,
        }),
        Some(&ModelPlannedInodeAction {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(7),
            created_at_ms: 1_700_000_200_000,
        }),
    )
    .expect("one action should be selected");

    assert_eq!(
        selected,
        ModelScheduledClientAction::PlannedInodeAction(ModelPlannedInodeAction {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            created_at_ms: 1_700_000_205_000,
        })
    );
}

#[test]
fn model_selects_unique_local_only_bind_candidate_for_remote_observation() {
    let selected = select_local_only_observation_bind_candidate(
        &[
            ModelLocalOnlyObservationCandidate {
                client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
                namespace_id: NamespaceId::from("ns-1"),
                inode_kind: InodeKind::File,
                content_digest: Some(
                    "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                        .to_owned(),
                ),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
                exists_on_disk: true,
            },
            ModelLocalOnlyObservationCandidate {
                client_file_id: "tmp:ns-1:00000000000000000002".to_owned(),
                namespace_id: NamespaceId::from("ns-1"),
                inode_kind: InodeKind::File,
                content_digest: Some("sha256:different".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "other.txt".to_owned(),
                exists_on_disk: true,
            },
        ],
        &ModelObservedRemoteInode {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(501),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(42),
            revision_no: RevisionNo(1),
            content_digest: Some(
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            ),
            content_manifest_digest: Some(
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            ),
            parent_inode_id: Some(InodeId(2)),
            display_name: "draft.txt".to_owned(),
            is_deleted: false,
        },
    )
    .expect("select unique bind candidate");

    assert_eq!(selected, Some("tmp:ns-1:00000000000000000001".to_owned()));
}

#[test]
fn model_rejects_ambiguous_local_only_bind_candidate_for_remote_observation() {
    let error = select_local_only_observation_bind_candidate(
        &[
            ModelLocalOnlyObservationCandidate {
                client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
                namespace_id: NamespaceId::from("ns-1"),
                inode_kind: InodeKind::File,
                content_digest: Some(
                    "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                        .to_owned(),
                ),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
                exists_on_disk: true,
            },
            ModelLocalOnlyObservationCandidate {
                client_file_id: "tmp:ns-1:00000000000000000002".to_owned(),
                namespace_id: NamespaceId::from("ns-1"),
                inode_kind: InodeKind::File,
                content_digest: Some(
                    "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                        .to_owned(),
                ),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
                exists_on_disk: true,
            },
        ],
        &ModelObservedRemoteInode {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(501),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(42),
            revision_no: RevisionNo(1),
            content_digest: Some(
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            ),
            content_manifest_digest: Some(
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            ),
            parent_inode_id: Some(InodeId(2)),
            display_name: "draft.txt".to_owned(),
            is_deleted: false,
        },
    )
    .expect_err("ambiguous local-only bind should be rejected");

    assert_eq!(
        error,
        ModelRemoteObservationSelectionError::AmbiguousLocalOnlyBind { matches: 2 }
    );
}

#[test]
fn model_detects_bound_local_match_for_remote_observation() {
    let matches = bound_local_matches_remote_observation(
        &InodeKind::File,
        Some("sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"),
        Some(InodeId(2)),
        "report.txt",
        true,
        &ModelObservedRemoteInode {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(42),
            revision_no: RevisionNo(18),
            content_digest: Some(
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            ),
            content_manifest_digest: Some(
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            ),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            is_deleted: false,
        },
    );

    assert!(matches);
    assert!(remote_observation_is_stale(
        Some(ChangeSeq(42)),
        ChangeSeq(42)
    ));
    assert!(!remote_observation_is_stale(
        Some(ChangeSeq(41)),
        ChangeSeq(42)
    ));
}

#[test]
fn model_supports_remote_only_discovery_from_authoritative_observation() {
    let observed = ModelObservedRemoteInode {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(601),
        inode_kind: InodeKind::File,
        observed_seq: ChangeSeq(42),
        revision_no: RevisionNo(1),
        content_digest: Some(
            "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9".to_owned(),
        ),
        content_manifest_digest: Some(
            "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6".to_owned(),
        ),
        parent_inode_id: Some(InodeId(2)),
        display_name: "welcome.txt".to_owned(),
        is_deleted: false,
    };

    assert!(remote_only_discovery_supported(&observed));

    let observed_dir = ModelObservedRemoteInode {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(701),
        inode_kind: InodeKind::Dir,
        observed_seq: ChangeSeq(52),
        revision_no: RevisionNo(1),
        content_digest: None,
        content_manifest_digest: None,
        parent_inode_id: Some(InodeId(2)),
        display_name: "incoming".to_owned(),
        is_deleted: false,
    };

    assert!(remote_only_discovery_supported(&observed_dir));
}

#[test]
fn model_authoritative_snapshot_import_discovers_remote_only_state_deterministically() {
    assert!(authoritative_snapshot_import_discovers_remote_only_state(
        2, 2
    ));
    assert!(!authoritative_snapshot_import_discovers_remote_only_state(
        2, 1
    ));
}

#[test]
fn model_authoritative_snapshot_import_is_idempotent_on_repeat() {
    assert!(authoritative_snapshot_import_is_idempotent(2, 2, 0));
    assert!(!authoritative_snapshot_import_is_idempotent(2, 1, 1));
}

#[test]
fn model_authoritative_snapshot_import_batch_rollback_leaves_state_unchanged() {
    let before = vec![
        (NamespaceId::from("ns-1"), InodeId(1), ChangeSeq(42)),
        (NamespaceId::from("ns-1"), InodeId(2), ChangeSeq(42)),
    ];
    let after_failed_batch = before.clone();
    let changed_after_failed_batch = vec![
        (NamespaceId::from("ns-1"), InodeId(1), ChangeSeq(42)),
        (NamespaceId::from("ns-1"), InodeId(2), ChangeSeq(43)),
    ];

    assert!(authoritative_snapshot_import_batch_rollback_is_atomic(
        &before,
        &after_failed_batch
    ));
    assert!(!authoritative_snapshot_import_batch_rollback_is_atomic(
        &before,
        &changed_after_failed_batch
    ));
}

#[test]
fn model_builds_remote_observation_bind_ambiguous_issue() {
    let observed = ModelObservedRemoteInode {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(601),
        inode_kind: InodeKind::File,
        observed_seq: ChangeSeq(42),
        revision_no: RevisionNo(1),
        content_digest: Some(
            "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9".to_owned(),
        ),
        content_manifest_digest: Some(
            "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6".to_owned(),
        ),
        parent_inode_id: Some(InodeId(2)),
        display_name: "welcome.txt".to_owned(),
        is_deleted: false,
    };

    let issue = remote_observation_bind_ambiguous_issue(&observed, 2, 1700000708000);

    assert_eq!(issue.kind, "remote_observation_bind_ambiguous");
    assert_eq!(
        issue.summary,
        "ambiguous remote observation bind matched 2 local-only candidates"
    );
    assert_eq!(
        issue.detail_json,
        json!({
            "matches": 2,
            "observed_seq": 42,
            "revision_no": 1,
            "inode_kind": "file",
            "parent_inode_id": 2,
            "display_name": "welcome.txt",
        })
    );
}

#[test]
fn model_upserts_client_issue_by_inode_and_kind() {
    let first = ModelClientIssue {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(701),
        kind: "materialize_remote_dir_local_apply_failed".to_owned(),
        summary: "old summary".to_owned(),
        detail_json: json!({"operation": "create_target_dir"}),
        created_at_ms: 1,
    };
    let second = ModelClientIssue {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(701),
        kind: "materialize_remote_dir_local_apply_failed".to_owned(),
        summary: "new summary".to_owned(),
        detail_json: json!({"operation": "sync_target_dir"}),
        created_at_ms: 2,
    };

    let issues = upsert_client_issue(&[first], second.clone());

    assert_eq!(issues, vec![second]);
}

#[test]
fn model_builds_upload_failed_issue() {
    let issue = upload_failed_issue(
        &NamespaceId::from("ns-1"),
        InodeId(42),
        "upload_local_edit_upload_failed",
        "upload_local_edit could not prepare durable local content for upload",
        json!({
            "failure": "local_file_read",
            "path": "/tmp/report.txt",
            "message": "No such file or directory",
        }),
        1_700_000_507_000,
    );

    assert_eq!(issue.namespace_id, NamespaceId::from("ns-1"));
    assert_eq!(issue.inode_id, InodeId(42));
    assert_eq!(issue.kind, "upload_local_edit_upload_failed");
    assert_eq!(
        issue.summary,
        "upload_local_edit could not prepare durable local content for upload"
    );
    assert_eq!(
        issue.detail_json,
        json!({
            "failure": "local_file_read",
            "path": "/tmp/report.txt",
            "message": "No such file or directory",
        })
    );
}

#[test]
fn model_builds_inode_transfer_reset_issue() {
    let issue = crate::client::transfer_reset_issue(
        &NamespaceId::from("ns-1"),
        InodeId(42),
        "upload_local_edit_transfer_reset",
        "upload_local_edit discarded stale transfer state and restarted from block 0",
        "block_count_mismatch",
        1_700_000_507_100,
    );

    assert_eq!(issue.namespace_id, NamespaceId::from("ns-1"));
    assert_eq!(issue.inode_id, InodeId(42));
    assert_eq!(issue.kind, "upload_local_edit_transfer_reset");
    assert_eq!(
        issue.detail_json,
        json!({
            "reason": "block_count_mismatch",
        })
    );
}

#[test]
fn model_upserts_local_only_issue_by_client_file_and_kind() {
    let first = ModelLocalOnlyIssue {
        client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
        namespace_id: NamespaceId::from("ns-1"),
        kind: "upload_local_create_upload_failed".to_owned(),
        summary: "old summary".to_owned(),
        detail_json: json!({"failure": "source_path_missing"}),
        created_at_ms: 1,
    };
    let second = ModelLocalOnlyIssue {
        client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
        namespace_id: NamespaceId::from("ns-1"),
        kind: "upload_local_create_upload_failed".to_owned(),
        summary: "new summary".to_owned(),
        detail_json: json!({"failure": "local_file_read"}),
        created_at_ms: 2,
    };

    let issues = upsert_local_only_issue(&[first], second.clone());

    assert_eq!(issues, vec![second]);
}

#[test]
fn model_builds_local_only_upload_failed_issue() {
    let issue = local_only_upload_failed_issue(
        "tmp:ns-1:00000000000000000001",
        &NamespaceId::from("ns-1"),
        "upload_local_create_upload_failed",
        "upload_local_create could not prepare durable local content for upload",
        json!({
            "failure": "local_file_read",
            "path": "/tmp/draft.txt",
            "message": "No such file or directory",
        }),
        1_700_000_507_000,
    );

    assert_eq!(issue.client_file_id, "tmp:ns-1:00000000000000000001");
    assert_eq!(issue.namespace_id, NamespaceId::from("ns-1"));
    assert_eq!(issue.kind, "upload_local_create_upload_failed");
    assert_eq!(
        issue.summary,
        "upload_local_create could not prepare durable local content for upload"
    );
    assert_eq!(
        issue.detail_json,
        json!({
            "failure": "local_file_read",
            "path": "/tmp/draft.txt",
            "message": "No such file or directory",
        })
    );
}

#[test]
fn model_builds_local_only_transfer_reset_issue() {
    let issue = crate::client::local_only_transfer_reset_issue(
        "tmp:ns-1:00000000000000000001",
        &NamespaceId::from("ns-1"),
        "upload_local_create_transfer_reset",
        "upload_local_create discarded stale transfer state and restarted from block 0",
        "object_key_mismatch",
        1_700_000_507_200,
    );

    assert_eq!(issue.client_file_id, "tmp:ns-1:00000000000000000001");
    assert_eq!(issue.namespace_id, NamespaceId::from("ns-1"));
    assert_eq!(issue.kind, "upload_local_create_transfer_reset");
    assert_eq!(
        issue.detail_json,
        json!({
            "reason": "object_key_mismatch",
        })
    );
}

#[test]
fn model_advances_transfer_one_block_per_tick() {
    assert_eq!(crate::client::advance_transfer_one_block(0, 2), (1, false));
    assert_eq!(crate::client::advance_transfer_one_block(1, 2), (2, true));
    assert_eq!(crate::client::advance_transfer_one_block(2, 2), (2, true));
}

#[test]
fn model_detects_remote_only_placeholder_match_for_materialization() {
    let observed = ModelObservedRemoteInode {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(601),
        inode_kind: InodeKind::File,
        observed_seq: ChangeSeq(42),
        revision_no: RevisionNo(1),
        content_digest: Some(
            "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9".to_owned(),
        ),
        content_manifest_digest: Some(
            "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6".to_owned(),
        ),
        parent_inode_id: Some(InodeId(2)),
        display_name: "welcome.txt".to_owned(),
        is_deleted: false,
    };

    assert!(remote_only_placeholder_matches_remote_observation(
        &InodeKind::File,
        Some(InodeId(2)),
        "welcome.txt",
        false,
        false,
        &observed,
    ));
    assert!(!remote_only_placeholder_matches_remote_observation(
        &InodeKind::File,
        Some(InodeId(2)),
        "welcome.txt",
        true,
        false,
        &observed,
    ));
}

#[test]
fn model_child_name_absent_rejects_existing_bound_name() {
    let metadata = seeded_metadata_state();

    let error = metadata
        .ensure_child_name_absent(InodeId(2), "note.txt", ChangeSeq(41))
        .expect_err("existing child name should collide");

    assert_eq!(
        error,
        ModelMetadataPreconditionError::ChildNameCollision {
            parent_inode_id: InodeId(2),
            name_key: "note.txt".to_owned(),
            child_inode_id: InodeId(42),
        }
    );
}

#[test]
fn model_inode_revision_is_rejects_stale_revision() {
    let metadata = seeded_metadata_state();

    let error = metadata
        .ensure_inode_revision_is(InodeId(42), RevisionNo(1), ChangeSeq(41))
        .expect_err("stale base revision should be rejected");

    assert_eq!(
        error,
        ModelMetadataPreconditionError::InodeRevisionMismatch {
            inode_id: InodeId(42),
            expected: RevisionNo(1),
            actual: Some(RevisionNo(2)),
        }
    );
}

#[test]
fn model_inode_is_directory_rejects_file_inode() {
    let metadata = seeded_metadata_state();

    let error = metadata
        .ensure_inode_is_directory(InodeId(42), ChangeSeq(41))
        .expect_err("file inode should be rejected");

    assert_eq!(
        error,
        ModelMetadataPreconditionError::InodeNotDirectory {
            inode_id: InodeId(42),
            actual_kind: InodeKind::File,
        }
    );
}

#[test]
fn model_ancestors_not_subtree_deleted_rejects_covered_inode() {
    let metadata = seeded_metadata_state();

    let error = metadata
        .ensure_ancestors_not_subtree_deleted(InodeId(88), ChangeSeq(41))
        .expect_err("covered descendant should be rejected");

    assert_eq!(
        error,
        ModelMetadataPreconditionError::AncestorCoveredBySubtreeTombstone {
            inode_id: InodeId(88),
            root_inode_id: InodeId(7),
            tombstone_seq: ChangeSeq(40),
        }
    );
}

#[test]
fn model_distinguishes_raw_and_visible_metadata_queries() {
    let metadata = seeded_metadata_state();

    assert_eq!(
        metadata.inode_at_seq(InodeId(7), ChangeSeq(41)),
        Some(ModelInodeRecord {
            inode_id: InodeId(7),
            inode_kind: InodeKind::Dir,
            created_seq: ChangeSeq(5),
        })
    );
    assert_eq!(metadata.visible_inode(InodeId(7), ChangeSeq(41)), None);
    assert_eq!(
        metadata.bound_child_at_seq(InodeId(2), "docs", ChangeSeq(41)),
        Some(ModelDirentryRecord {
            parent_inode_id: InodeId(2),
            name_key: "docs".to_owned(),
            display_name: "docs".to_owned(),
            child_inode_id: InodeId(7),
            bind_seq: ChangeSeq(5),
            bind_op_index: 0,
        })
    );
    assert_eq!(
        metadata.visible_child(InodeId(2), "docs", ChangeSeq(41)),
        None
    );
    assert_eq!(
        metadata.current_revision_head(InodeId(42), ChangeSeq(41)),
        Some(ModelRevisionRecord {
            inode_id: InodeId(42),
            revision_no: RevisionNo(2),
            committed_seq: ChangeSeq(41),
            revision_op_index: 0,
            content_manifest_digest: "sha256:note-v2".to_owned(),
        })
    );
}

#[test]
fn model_apply_create_dir_appends_inode_and_direntry_rows() {
    let applied = ModelMetadataState::default()
        .apply_committed_mutations(
            ChangeSeq(42),
            &[ModelMetadataMutation::CreateDir {
                inode_id: InodeId(501),
                parent_inode_id: InodeId(2),
                display_name: "drafts".to_owned(),
            }],
        )
        .expect("apply create_dir metadata");

    assert_eq!(
        applied.metadata_state.inodes,
        vec![ModelInodeRecord {
            inode_id: InodeId(501),
            inode_kind: InodeKind::Dir,
            created_seq: ChangeSeq(42),
        }]
    );
    assert_eq!(
        applied.metadata_state.direntries,
        vec![ModelDirentryRecord {
            parent_inode_id: InodeId(2),
            name_key: "drafts".to_owned(),
            display_name: "drafts".to_owned(),
            child_inode_id: InodeId(501),
            bind_seq: ChangeSeq(42),
            bind_op_index: 0,
        }]
    );
    assert!(applied
        .checked_invariants
        .contains(&"create_dir_writes_inode_and_direntry_rows".to_owned()));
}

#[test]
fn model_apply_create_file_appends_initial_revision_row() {
    let applied = ModelMetadataState::default()
        .apply_committed_mutations(
            ChangeSeq(42),
            &[ModelMetadataMutation::CreateFile {
                inode_id: InodeId(501),
                parent_inode_id: InodeId(2),
                display_name: "note.txt".to_owned(),
                content_manifest_digest: "sha256:note-v1".to_owned(),
            }],
        )
        .expect("apply create_file metadata");

    assert_eq!(
        applied.metadata_state.inodes,
        vec![ModelInodeRecord {
            inode_id: InodeId(501),
            inode_kind: InodeKind::File,
            created_seq: ChangeSeq(42),
        }]
    );
    assert_eq!(
        applied.metadata_state.direntries,
        vec![ModelDirentryRecord {
            parent_inode_id: InodeId(2),
            name_key: "note.txt".to_owned(),
            display_name: "note.txt".to_owned(),
            child_inode_id: InodeId(501),
            bind_seq: ChangeSeq(42),
            bind_op_index: 0,
        }]
    );
    assert_eq!(
        applied.metadata_state.revisions,
        vec![ModelRevisionRecord {
            inode_id: InodeId(501),
            revision_no: RevisionNo(1),
            committed_seq: ChangeSeq(42),
            revision_op_index: 0,
            content_manifest_digest: "sha256:note-v1".to_owned(),
        }]
    );
    assert!(applied
        .checked_invariants
        .contains(&"create_file_writes_inode_direntry_and_initial_revision".to_owned()));
}

#[test]
fn model_apply_replace_file_appends_next_revision_row() {
    let applied = seeded_metadata_state()
        .apply_committed_mutations(
            ChangeSeq(42),
            &[ModelMetadataMutation::ReplaceFile {
                inode_id: InodeId(42),
                base_revision_no: RevisionNo(2),
                content_manifest_digest: "sha256:note-v3".to_owned(),
            }],
        )
        .expect("apply replace_file metadata");

    assert_eq!(
        applied
            .metadata_state
            .latest_revision_head_at_seq(InodeId(42), ChangeSeq(42))
            .expect("revision head after replace"),
        ModelRevisionRecord {
            inode_id: InodeId(42),
            revision_no: RevisionNo(3),
            committed_seq: ChangeSeq(42),
            revision_op_index: 0,
            content_manifest_digest: "sha256:note-v3".to_owned(),
        }
    );
    assert!(applied
        .checked_invariants
        .contains(&"replace_file_appends_new_revision_head".to_owned()));
}

#[test]
fn model_apply_restore_revision_appends_new_head_from_historical_content() {
    let applied = seeded_metadata_state()
        .apply_committed_mutations(
            ChangeSeq(42),
            &[ModelMetadataMutation::RestoreRevision {
                inode_id: InodeId(42),
                base_revision_no: RevisionNo(2),
                restore_from_revision_no: RevisionNo(1),
            }],
        )
        .expect("apply restore_revision metadata");

    assert_eq!(
        applied
            .metadata_state
            .latest_revision_head_at_seq(InodeId(42), ChangeSeq(42))
            .expect("revision head after restore"),
        ModelRevisionRecord {
            inode_id: InodeId(42),
            revision_no: RevisionNo(3),
            committed_seq: ChangeSeq(42),
            revision_op_index: 0,
            content_manifest_digest: "sha256:note-v1".to_owned(),
        }
    );
    assert!(applied
        .checked_invariants
        .contains(&"restore_creates_new_revision_head".to_owned()));
}

#[test]
fn model_apply_rename_appends_new_binding_and_hides_old_visible_name() {
    let applied = seeded_metadata_state()
        .apply_committed_mutations(
            ChangeSeq(42),
            &[ModelMetadataMutation::Rename {
                inode_id: InodeId(42),
                new_parent_inode_id: InodeId(2),
                new_display_name: "renamed.txt".to_owned(),
            }],
        )
        .expect("apply rename metadata");

    assert_eq!(
        applied
            .metadata_state
            .visible_child(InodeId(2), "note.txt", ChangeSeq(42)),
        None
    );
    assert_eq!(
        applied
            .metadata_state
            .visible_child(InodeId(2), "renamed.txt", ChangeSeq(42))
            .expect("renamed visible child")
            .child_inode_id,
        InodeId(42)
    );
    assert!(applied
        .checked_invariants
        .contains(&"rename_appends_new_direntry_binding".to_owned()));
}

#[test]
fn model_apply_delete_subtree_appends_tombstone_row_and_hides_descendants() {
    let applied = ModelMetadataState {
        inodes: vec![
            ModelInodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            },
            ModelInodeRecord {
                inode_id: InodeId(7),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(5),
            },
            ModelInodeRecord {
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(17),
            },
        ],
        direntries: vec![
            ModelDirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "docs".to_owned(),
                display_name: "docs".to_owned(),
                child_inode_id: InodeId(7),
                bind_seq: ChangeSeq(5),
                bind_op_index: 0,
            },
            ModelDirentryRecord {
                parent_inode_id: InodeId(7),
                name_key: "report.txt".to_owned(),
                display_name: "report.txt".to_owned(),
                child_inode_id: InodeId(42),
                bind_seq: ChangeSeq(17),
                bind_op_index: 0,
            },
        ],
        revisions: vec![ModelRevisionRecord {
            inode_id: InodeId(42),
            revision_no: RevisionNo(1),
            committed_seq: ChangeSeq(17),
            revision_op_index: 0,
            content_manifest_digest: "sha256:report-v1".to_owned(),
        }],
        subtree_tombstones: Vec::new(),
    }
    .apply_committed_mutations(
        ChangeSeq(42),
        &[ModelMetadataMutation::DeleteSubtree {
            root_inode_id: InodeId(7),
        }],
    )
    .expect("apply delete_subtree metadata");

    assert_eq!(
        applied.metadata_state.subtree_tombstones,
        vec![ModelSubtreeTombstoneRecord {
            root_inode_id: InodeId(7),
            tombstone_seq: ChangeSeq(42),
            tombstone_op_index: 0,
        }]
    );
    assert_eq!(
        applied
            .metadata_state
            .visible_inode(InodeId(7), ChangeSeq(42)),
        None
    );
    assert_eq!(
        applied
            .metadata_state
            .visible_inode(InodeId(42), ChangeSeq(42)),
        None
    );
    assert!(applied
        .checked_invariants
        .contains(&"delete_subtree_writes_tombstone_row".to_owned()));
}

#[test]
fn model_restore_source_must_be_historical() {
    let error = seeded_metadata_state()
        .ensure_restore_source_revision_exists(
            InodeId(42),
            RevisionNo(2),
            RevisionNo(2),
            ChangeSeq(41),
        )
        .expect_err("current head cannot be restore source");

    assert_eq!(
        error,
        ModelMetadataPreconditionError::SourceRevisionNotHistorical {
            inode_id: InodeId(42),
            base_revision_no: RevisionNo(2),
            restore_from_revision: RevisionNo(2),
        }
    );
}

#[test]
fn model_rejects_directory_rename_cycle() {
    let metadata_state = ModelMetadataState {
        inodes: vec![
            ModelInodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            },
            ModelInodeRecord {
                inode_id: InodeId(7),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(5),
            },
            ModelInodeRecord {
                inode_id: InodeId(9),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(8),
            },
        ],
        direntries: vec![
            ModelDirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "docs".to_owned(),
                display_name: "docs".to_owned(),
                child_inode_id: InodeId(7),
                bind_seq: ChangeSeq(5),
                bind_op_index: 0,
            },
            ModelDirentryRecord {
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
    };

    let error = metadata_state
        .ensure_rename_does_not_cycle(InodeId(7), InodeId(9), ChangeSeq(52))
        .expect_err("directory cycle should be rejected");

    assert_eq!(
        error,
        ModelMetadataPreconditionError::RenameWouldCycle {
            inode_id: InodeId(7),
            new_parent_inode_id: InodeId(9),
        }
    );
}

#[test]
fn model_visible_child_prefers_latest_slot_binding_when_name_is_reused() {
    let metadata_state = ModelMetadataState {
        inodes: vec![
            ModelInodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            },
            ModelInodeRecord {
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(10),
            },
            ModelInodeRecord {
                inode_id: InodeId(77),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(30),
            },
        ],
        direntries: vec![
            ModelDirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "note.txt".to_owned(),
                display_name: "note.txt".to_owned(),
                child_inode_id: InodeId(42),
                bind_seq: ChangeSeq(10),
                bind_op_index: 0,
            },
            ModelDirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "archive.txt".to_owned(),
                display_name: "archive.txt".to_owned(),
                child_inode_id: InodeId(42),
                bind_seq: ChangeSeq(20),
                bind_op_index: 0,
            },
            ModelDirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "note.txt".to_owned(),
                display_name: "note.txt".to_owned(),
                child_inode_id: InodeId(77),
                bind_seq: ChangeSeq(30),
                bind_op_index: 0,
            },
        ],
        revisions: vec![
            ModelRevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(10),
                revision_op_index: 0,
                content_manifest_digest: "sha256:note-v1".to_owned(),
            },
            ModelRevisionRecord {
                inode_id: InodeId(77),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(30),
                revision_op_index: 0,
                content_manifest_digest: "sha256:note-v2".to_owned(),
            },
        ],
        subtree_tombstones: Vec::new(),
    };

    assert_eq!(
        metadata_state
            .visible_child(InodeId(2), "note.txt", ChangeSeq(30))
            .expect("latest visible note.txt binding")
            .child_inode_id,
        InodeId(77)
    );
    let old_child_binding = metadata_state
        .current_parent_binding_for_child(InodeId(42), ChangeSeq(30))
        .expect("latest binding for renamed-away inode");
    assert_eq!(old_child_binding.parent_inode_id, InodeId(2));
    assert_eq!(old_child_binding.name_key, "archive.txt");
}

#[test]
fn model_rejects_local_only_upload_record_when_digest_mismatches() {
    let upload = ModelLocalOnlyUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

    let error = validate_local_only_upload_record(
        &NamespaceId::from("ns-1"),
        Some("sha256:different"),
        &upload,
    )
    .expect_err("mismatched digest should be rejected");

    assert_eq!(
        error,
        ModelLocalOnlyUploadValidationError::FileDigestMismatch {
            expected: "sha256:different".to_owned(),
            actual: "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                .to_owned(),
        }
    );
}

#[test]
fn model_rejects_uploaded_content_reference_when_block_is_missing() {
    let uploaded = build_uploaded_content(NamespaceId::from("ns-1"), b"hello from loon\n")
        .expect("build uploaded content");
    let error = validate_uploaded_content_reference(
        &NamespaceId::from("ns-1"),
        &uploaded.content_manifest_digest,
        &uploaded.manifest_envelope,
        &BTreeMap::new(),
    )
    .expect_err("missing block should be rejected");

    assert_eq!(
        error,
        ModelContentValidationError::MissingBlock {
            digest: "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                .to_owned(),
        }
    );
}

#[test]
fn model_rejects_stale_writer_after_fence_rotation() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::RotateFence {
        new_fence_token: FenceToken(9),
    })
    .expect("fence rotation should succeed");

    let error = ns
        .apply(ModelAction::BumpSeq {
            writer_fence_token: FenceToken(8),
        })
        .expect_err("stale writer should be rejected");

    assert_eq!(
        error,
        ModelError::StaleWriterFenceToken {
            expected: FenceToken(9),
            actual: FenceToken(8),
        }
    );
}

#[test]
fn model_accepts_active_commit_attempt() {
    let ns = ModelNamespace {
        namespace_id: NamespaceId::from("ns-1"),
        head_seq: ChangeSeq(41),
        active_fence_token: FenceToken(8),
        next_inode_id: InodeId(501),
        snapshot_hint_seq: Some(ChangeSeq(40)),
        retention_floor_seq: ChangeSeq(40),
        metadata_state: bootstrapped_metadata_state(),
    };
    let lease = LeaseState {
        namespace_id: NamespaceId::from("ns-1"),
        holder_id: "writer-a".to_owned(),
        fence_token: FenceToken(8),
        lease_expires_at_ms: 1_000,
    };
    let request = ModelCommitValidationRequest {
        namespace_id: NamespaceId::from("ns-1"),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(8),
        planned_head_seq: ChangeSeq(41),
    };

    let outcome = ns
        .validate_commit_attempt(&request, &lease, 999)
        .expect("active writer should validate");

    assert_eq!(
        outcome,
        ModelCommitValidationOutcome {
            next_seq: ChangeSeq(42),
        }
    );
}

#[test]
fn model_stale_commit_attempt_hits_planned_head_seq_mismatch_after_handover_publish() {
    let ns = ModelNamespace {
        namespace_id: NamespaceId::from("ns-1"),
        head_seq: ChangeSeq(42),
        active_fence_token: FenceToken(9),
        next_inode_id: InodeId(504),
        snapshot_hint_seq: Some(ChangeSeq(40)),
        retention_floor_seq: ChangeSeq(40),
        metadata_state: bootstrapped_metadata_state(),
    };
    let lease = LeaseState {
        namespace_id: NamespaceId::from("ns-1"),
        holder_id: "writer-b".to_owned(),
        fence_token: FenceToken(9),
        lease_expires_at_ms: 2_000,
    };
    let request = ModelCommitValidationRequest {
        namespace_id: NamespaceId::from("ns-1"),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(8),
        planned_head_seq: ChangeSeq(41),
    };

    let error = ns
        .validate_commit_attempt(&request, &lease, 1_500)
        .expect_err("stale writer should be rejected");

    assert_eq!(
        error,
        ModelCommitValidationError::PlannedHeadSeqMismatch {
            expected: ChangeSeq(42),
            actual: ChangeSeq(41),
        }
    );
}

#[test]
fn model_prepares_next_wal_commit_seq_for_active_writer() {
    let ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    let wal = ns
        .prepare_wal_commit("req-20260311-0001", FenceToken(0))
        .expect("active writer should prepare WAL");

    assert_eq!(wal.seq, ChangeSeq(1));
    assert_eq!(wal.base_head_seq, ChangeSeq(0));
    assert_eq!(wal.commit_id, "req-20260311-0001");
}

#[test]
fn model_replays_contiguous_wal_commit() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    let wal = ns
        .prepare_wal_commit("req-20260311-0001", FenceToken(0))
        .expect("active writer should prepare WAL");

    ns.replay_wal_commit(&wal)
        .expect("contiguous WAL should replay");

    assert_eq!(ns.head_seq, ChangeSeq(1));
    assert_eq!(ns.active_fence_token, FenceToken(0));
}

#[test]
fn model_rejects_non_contiguous_wal_commit() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    let wal = ModelWalCommit {
        namespace_id: NamespaceId::from("ns-1"),
        seq: ChangeSeq(2),
        base_head_seq: ChangeSeq(0),
        commit_id: "req-20260311-0001".to_owned(),
        writer_fence_token: FenceToken(0),
        ops: Vec::new(),
    };

    let error = ns
        .replay_wal_commit(&wal)
        .expect_err("gap should be rejected");

    assert_eq!(
        error,
        ModelError::NonContiguousSeq {
            expected: ChangeSeq(1),
            actual: ChangeSeq(2),
        }
    );
}

#[test]
fn model_restores_from_verified_checkpoint() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::RotateFence {
        new_fence_token: FenceToken(9),
    })
    .expect("fence rotation should succeed");
    ns.apply(ModelAction::CreateDir {
        inode_id: InodeId(41),
        writer_fence_token: FenceToken(9),
    })
    .expect("active writer should advance seq");

    let checkpoint = ns.checkpoint();
    let available_segment_keys = checkpoint
        .tables
        .iter()
        .flat_map(|table| {
            table
                .segments
                .iter()
                .map(|segment| segment.object_key.clone())
        })
        .collect::<Vec<_>>();
    let restored = ModelNamespace::restore_from_checkpoint(&checkpoint, &available_segment_keys)
        .expect("checkpoint restore");

    assert_eq!(restored.namespace_id, NamespaceId::from("ns-1"));
    assert_eq!(restored.head_seq, ChangeSeq(1));
    assert_eq!(restored.active_fence_token, FenceToken(9));
    assert_eq!(restored.next_inode_id, InodeId(42));
    assert_eq!(restored.snapshot_hint_seq, Some(ChangeSeq(1)));
    assert_eq!(restored.retention_floor_seq, ChangeSeq(0));
}

#[test]
fn model_publishes_verified_checkpoint_into_head_summary() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::RotateFence {
        new_fence_token: FenceToken(9),
    })
    .expect("fence rotation should succeed");
    ns.apply(ModelAction::CreateDir {
        inode_id: InodeId(41),
        writer_fence_token: FenceToken(9),
    })
    .expect("active writer should advance seq");

    let checkpoint = ns.checkpoint();
    ns.publish_checkpoint(
        &checkpoint,
        &available_segment_keys(&checkpoint),
        Some(ChangeSeq(1)),
        Some(&sample_publish_authorizers(ChangeSeq(1))),
    )
    .expect("checkpoint publication should succeed");

    assert_eq!(ns.head_seq, ChangeSeq(1));
    assert_eq!(ns.active_fence_token, FenceToken(9));
    assert_eq!(ns.next_inode_id, InodeId(42));
    assert_eq!(ns.snapshot_hint_seq, Some(ChangeSeq(1)));
    assert_eq!(ns.retention_floor_seq, ChangeSeq(1));
}

#[test]
fn model_progress_publish_is_monotonic() {
    let ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    let current = ModelProgressObject {
        namespace_id: NamespaceId::from("ns-1"),
        work_class: "BuildSnapshot".to_owned(),
        through_seq: ChangeSeq(42),
    };

    let published = ns
        .publish_progress(Some(&current), "BuildSnapshot", ChangeSeq(41))
        .expect("stale progress publish should no-op");

    assert_eq!(published, current);
}

#[test]
fn model_repair_enqueues_snapshot_job_when_progress_lags() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::BumpSeq {
        writer_fence_token: FenceToken(0),
    })
    .expect("active writer should advance seq");
    let progress = ModelProgressObject {
        namespace_id: NamespaceId::from("ns-1"),
        work_class: "BuildSnapshot".to_owned(),
        through_seq: ChangeSeq(0),
    };
    let mut queue = ModelQueueShard {
        work_class: ModelQueueWorkClass::BuildSnapshot,
        shard_id: 17,
        broker: None,
        jobs: vec![],
    };

    let outcome = ns
        .repair_lost_snapshot_enqueue(&mut queue, Some(&progress))
        .expect("repair should enqueue missing snapshot job");

    assert_eq!(
        outcome,
        ModelQueueRepairOutcome::Enqueued {
            through_seq: ChangeSeq(1),
        }
    );
    assert_eq!(queue.jobs.len(), 1);
    assert_eq!(queue.jobs[0].dedupe_key, "BuildSnapshot:ns-1");
    assert_eq!(queue.jobs[0].payload.through_seq, ChangeSeq(1));
}

#[test]
fn model_repair_attaches_follow_up_for_claimed_snapshot_job() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::BumpSeq {
        writer_fence_token: FenceToken(0),
    })
    .expect("active writer should advance seq");
    ns.apply(ModelAction::BumpSeq {
        writer_fence_token: FenceToken(0),
    })
    .expect("active writer should advance seq again");
    let progress = ModelProgressObject {
        namespace_id: NamespaceId::from("ns-1"),
        work_class: "BuildSnapshot".to_owned(),
        through_seq: ChangeSeq(0),
    };
    let mut queue = ModelQueueShard {
        work_class: ModelQueueWorkClass::BuildSnapshot,
        shard_id: 17,
        broker: None,
        jobs: vec![ModelQueueJob {
            job_id: "job-1".to_owned(),
            dedupe_key: "BuildSnapshot:ns-1".to_owned(),
            state: ModelQueueJobState::Claimed,
            payload: ModelQueueSeqPayload {
                namespace_id: NamespaceId::from("ns-1"),
                through_seq: ChangeSeq(1),
            },
            follow_up: None,
            claim: Some(ModelQueueClaim {
                worker_id: "worker-a".to_owned(),
                claim_token: "claim-a".to_owned(),
                heartbeat_at_ms: 0,
                timeout_at_ms: 10_000,
            }),
            attempts: 1,
        }],
    };

    let outcome = ns
        .repair_lost_snapshot_enqueue(&mut queue, Some(&progress))
        .expect("repair should attach follow-up to claimed job");

    assert_eq!(
        outcome,
        ModelQueueRepairOutcome::AttachedFollowUp {
            through_seq: ChangeSeq(2),
        }
    );
    assert_eq!(
        queue.jobs[0].follow_up,
        Some(ModelQueueSeqPayload {
            namespace_id: NamespaceId::from("ns-1"),
            through_seq: ChangeSeq(2),
        })
    );
}

#[test]
fn model_broker_lease_takeover_fences_old_generation() {
    let mut queue = ModelQueueShard {
        work_class: ModelQueueWorkClass::BuildSnapshot,
        shard_id: 17,
        broker: None,
        jobs: vec![],
    };

    assert_eq!(
        queue
            .renew_broker_lease("broker-a", 0, 10_000)
            .expect("first lease should be acquired"),
        ModelBrokerLeaseOutcome::Acquired { epoch: 1 }
    );
    assert_eq!(
        queue
            .renew_broker_lease("broker-b", 30_000, 10_000)
            .expect("expired lease should be takeable"),
        ModelBrokerLeaseOutcome::TakenOver { epoch: 2 }
    );
    assert_eq!(
        ensure_active_broker_lease(&queue, "broker-a", 1, 30_001)
            .expect_err("old broker generation should be fenced"),
        ModelError::BrokerLeaseMismatch {
            expected_broker_id: "broker-b".to_owned(),
            expected_epoch: 2,
            actual_broker_id: "broker-a".to_owned(),
            actual_epoch: 1,
        }
    );
}

#[test]
fn model_claim_timeout_then_steal_rejects_stale_complete() {
    let mut queue = ModelQueueShard {
        work_class: ModelQueueWorkClass::BuildSnapshot,
        shard_id: 17,
        broker: None,
        jobs: vec![ModelQueueJob {
            job_id: "job-1".to_owned(),
            dedupe_key: "BuildSnapshot:ns-1".to_owned(),
            state: ModelQueueJobState::Ready,
            payload: ModelQueueSeqPayload {
                namespace_id: NamespaceId::from("ns-1"),
                through_seq: ChangeSeq(420),
            },
            follow_up: None,
            claim: None,
            attempts: 0,
        }],
    };
    queue
        .renew_broker_lease("broker-a", 0, 10_000)
        .expect("broker-a should acquire lease");
    assert_eq!(
        queue
            .claim_job(
                "job-1",
                &ModelJobClaimParams {
                    broker_id: "broker-a".to_owned(),
                    broker_epoch: 1,
                    worker_id: "worker-a".to_owned(),
                    claim_token: "claim-a".to_owned(),
                    now_ms: 0,
                    claim_timeout_ms: 10_000,
                },
            )
            .expect("worker-a should claim job"),
        ModelJobClaimOutcome::Claimed {
            claim_token: "claim-a".to_owned(),
        }
    );

    queue
        .renew_broker_lease("broker-b", 30_000, 10_000)
        .expect("broker-b should take over after expiry");
    assert_eq!(
        queue
            .claim_job(
                "job-1",
                &ModelJobClaimParams {
                    broker_id: "broker-b".to_owned(),
                    broker_epoch: 2,
                    worker_id: "worker-b".to_owned(),
                    claim_token: "claim-b".to_owned(),
                    now_ms: 30_000,
                    claim_timeout_ms: 10_000,
                },
            )
            .expect("worker-b should steal expired job"),
        ModelJobClaimOutcome::Stolen {
            claim_token: "claim-b".to_owned(),
        }
    );

    assert_eq!(
        queue
            .complete_job("broker-b", 2, "job-1", "claim-a", 30_001)
            .expect_err("stale claim token should be rejected"),
        ModelError::ClaimTokenMismatch {
            expected: "claim-b".to_owned(),
            actual: "claim-a".to_owned(),
        }
    );
    assert_eq!(
        queue
            .complete_job("broker-b", 2, "job-1", "claim-b", 30_001)
            .expect("fresh claim should complete"),
        ModelJobCompleteOutcome::Removed
    );
    assert!(queue.jobs.is_empty());
}

#[test]
fn model_rejects_retention_floor_without_authorizers() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::BumpSeq {
        writer_fence_token: FenceToken(0),
    })
    .expect("active writer should advance seq");
    let checkpoint = ns.checkpoint();

    let error = ns
        .publish_checkpoint(
            &checkpoint,
            &available_segment_keys(&checkpoint),
            Some(ChangeSeq(1)),
            None,
        )
        .expect_err("missing authorizers should fail");

    assert_eq!(
        error,
        ModelError::MissingRetentionAuthorizers {
            requested: ChangeSeq(1),
        }
    );
}

#[test]
fn model_rejects_retention_floor_when_required_progress_lags() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::BumpSeq {
        writer_fence_token: FenceToken(0),
    })
    .expect("active writer should advance seq");
    let checkpoint = ns.checkpoint();
    let authorizers = sample_publish_authorizers(ChangeSeq(0));

    let error = ns
        .publish_checkpoint(
            &checkpoint,
            &available_segment_keys(&checkpoint),
            Some(ChangeSeq(1)),
            Some(&authorizers),
        )
        .expect_err("lagging required progress should fail");

    assert_eq!(
        error,
        ModelError::RequiredProgressLag {
            work_class: "BuildListingIndex".to_owned(),
            requested: ChangeSeq(1),
            available: ChangeSeq(0),
        }
    );
}

#[test]
fn model_rejects_retention_floor_above_checkpoint() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::BumpSeq {
        writer_fence_token: FenceToken(0),
    })
    .expect("active writer should advance seq");
    let checkpoint = ns.checkpoint();

    let error = ns
        .publish_checkpoint(
            &checkpoint,
            &available_segment_keys(&checkpoint),
            Some(ChangeSeq(2)),
            Some(&sample_publish_authorizers(ChangeSeq(2))),
        )
        .expect_err("retention floor beyond checkpoint should fail");

    assert_eq!(
        error,
        ModelError::RetentionFloorBeyondCheckpoint {
            checkpoint_seq: ChangeSeq(1),
            requested: ChangeSeq(2),
        }
    );
}

#[test]
fn model_checkpoint_includes_one_empty_segment_per_family() {
    let ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    let checkpoint = ns.checkpoint();

    assert_eq!(checkpoint.tables.len(), 4);
    assert!(checkpoint
        .tables
        .iter()
        .all(|table| table.segments.len() == 1));
    assert!(checkpoint
        .tables
        .iter()
        .all(|table| table.segments[0].segment_index == 0));
    let inode_table = checkpoint
        .tables
        .iter()
        .find(|table| table.family == ModelCheckpointFamily::Inodes)
        .expect("inode table");
    assert_eq!(inode_table.segments[0].row_count, 1);
    assert!(checkpoint
        .tables
        .iter()
        .filter(|table| table.family != ModelCheckpointFamily::Inodes)
        .all(|table| table.segments[0].row_count == 0));
    assert!(checkpoint
        .tables
        .iter()
        .all(|table| table.segments[0].object_key.contains("/tables/")));
}

#[test]
fn model_rejects_restore_when_checkpoint_segment_is_missing() {
    let checkpoint = ModelNamespace::new(NamespaceId::from("ns-1")).checkpoint();
    let error = ModelNamespace::restore_from_checkpoint(&checkpoint, &[])
        .expect_err("missing checkpoint segment should fail");

    assert_eq!(
        error,
        ModelError::MissingCheckpointSegment {
            object_key:
                "namespaces/ns-1/snapshots/00000000000000000000/tables/inodes-00000.sst.zst"
                    .to_owned(),
        }
    );
}

#[test]
fn model_rejects_unverified_checkpoint() {
    let checkpoint = ModelCheckpoint {
        namespace_id: NamespaceId::from("ns-1"),
        checkpoint_seq: ChangeSeq(40),
        active_fence_token: FenceToken(8),
        next_inode_id: InodeId(501),
        retention_floor_seq: ChangeSeq(40),
        verified: false,
        tables: vec![],
    };

    let error = ModelNamespace::restore_from_checkpoint(&checkpoint, &[])
        .expect_err("unverified checkpoint should fail");

    assert_eq!(
        error,
        ModelError::UnverifiedCheckpoint {
            checkpoint_seq: ChangeSeq(40),
        }
    );
}

fn available_segment_keys(checkpoint: &ModelCheckpoint) -> Vec<String> {
    checkpoint
        .tables
        .iter()
        .flat_map(|table| {
            table
                .segments
                .iter()
                .map(|segment| segment.object_key.clone())
        })
        .collect()
}

fn sample_publish_authorizers(through_seq: ChangeSeq) -> ModelCheckpointPublishAuthorizers {
    ModelCheckpointPublishAuthorizers {
        required_progress: vec![ModelProgressObject {
            namespace_id: NamespaceId::from("ns-1"),
            work_class: "BuildListingIndex".to_owned(),
            through_seq,
        }],
        retention_policy: ModelProgressObject {
            namespace_id: NamespaceId::from("ns-1"),
            work_class: "RetentionPolicy".to_owned(),
            through_seq,
        },
    }
}
