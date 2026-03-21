use loon_client::executor::{
    execute_next_client_action, ExecutedApplyRemoteRenameAndReplace,
    ExecutedResolveDeleteVsEditConflict, ExecutedResolvePathBindingCollision,
    ExecutedResolveRenameVsEditConflict, ExecutedResolveSameInodeConflict, NextClientAction,
};
use loon_client::planner::{plan_file, PlannerDecision, PlannerReason};
use loon_client::state_db::{
    ClientFileId, ConflictArtifactRow, LocalFileStateRow, LocalOnlyFileStateRow,
    LocalOnlyPlannedActionRow, RemoteFileStateRow, SqliteStateDb, SyncAnchorRow,
};
use loon_client::upload::{upload_small_file_from_path, UploadedContent};
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::{blob, content_manifest};
use loon_objectstore::ObjectStore;
use loon_testkit::invariants::{
    evaluate_file_conflict_resolution_invariants, FileConflictResolutionInvariantInputs,
    FileConflictResolutionKind,
};
use loon_testkit::tempdir::TestDir;
use loon_types::{
    decode_content_manifest_json, sha256_digest, ChangeSeq, ConflictClass, InodeId, InodeKind,
    NamespaceId, RevisionNo,
};
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn same_inode_conflict_preserves_artifact_and_keeps_canonical_path() {
    let fixture = BoundFileFixture::new("same-inode-conflict");
    let namespace_id = fixture.namespace_id.clone();
    let current_path = fixture.mirror_root.join("reports/report.txt");
    write_file(&current_path, "local loser");

    let anchor_upload = fixture.upload_content("anchor.txt", "anchor bytes");
    let remote_upload = fixture.upload_content("remote.txt", "remote winner");

    seed_bound_state(
        &fixture.db_path,
        RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(12),
            revision_no: RevisionNo(2),
            content_digest: Some(remote_upload.file_digest_sha256.clone()),
            content_manifest_digest: Some(remote_upload.content_manifest_digest.clone()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            is_deleted: false,
        },
        LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            content_digest: Some(sha256_digest(b"local loser")),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 1,
        },
        Some(SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            synced_seq: ChangeSeq(11),
            revision_no: RevisionNo(1),
            content_digest: Some(anchor_upload.file_digest_sha256.clone()),
            content_manifest_digest: Some(anchor_upload.content_manifest_digest.clone()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
        }),
    );

    let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
    let planned =
        plan_file(&mut db, &namespace_id, InodeId(42), 100).expect("plan same inode conflict");
    assert_eq!(planned.decision, PlannerDecision::ResolveSameInodeConflict);
    assert_eq!(
        planned.reason,
        PlannerReason::LocalAndRemoteDifferFromAnchor
    );
    drop(db);

    let executed = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen db");
        execute_next_client_action(
            &mut db,
            &fixture.store,
            |_client_file_id| None,
            |_namespace_id, _inode_id| Some(current_path.clone()),
            |_namespace_id, _inode_id, _parent_inode_id, _display_name| None,
            101,
            102,
            |_request| panic!("conflict resolution should not dispatch mutations"),
        )
        .expect("execute same inode conflict")
    };
    let conflict_id = match executed {
        Some(NextClientAction::ExecutedResolveSameInodeConflict(
            ExecutedResolveSameInodeConflict {
                applied,
                conflict_id,
            },
        )) => {
            assert_eq!(applied.inode_id, InodeId(42));
            conflict_id
        }
        other => panic!("unexpected execution result: {other:?}"),
    };

    assert_eq!(
        fs::read_to_string(&current_path).expect("read canonical file"),
        "remote winner"
    );

    let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen after execution");
    let views = db
        .load_file_sync_views(&namespace_id, InodeId(42))
        .expect("load sync views");
    let artifacts = db
        .load_conflict_artifacts_for_namespace(&namespace_id)
        .expect("load conflict artifacts");
    assert_eq!(artifacts.len(), 1);
    let artifact = &artifacts[0];
    assert_eq!(artifact.conflict_id, conflict_id);
    assert_eq!(
        artifact.conflict_class,
        ConflictClass::SameInodeStaleBaseEdit
    );
    assert!(conflict_artifact_is_durable(
        &fixture.store,
        &namespace_id,
        artifact
    ));
    let report =
        evaluate_file_conflict_resolution_invariants(FileConflictResolutionInvariantInputs {
            conflict_kind: FileConflictResolutionKind::SameInode,
            namespace_id: &namespace_id,
            artifact_conflict_id: Some(artifact.conflict_id.as_str()),
            artifact_object_key: Some(artifact.object_key.as_str()),
            artifact_loser_manifest_digest: Some(
                artifact.envelope.loser.content_manifest_digest.as_str(),
            ),
            artifact_loser_content_durable: true,
            local_present_after: views.local.is_some(),
            sync_anchor_present_after: views.sync_anchor.is_some(),
            final_canonical_path_exists: current_path.exists(),
            final_visible_sibling_exists: false,
            local_content_digest_after: views
                .local
                .as_ref()
                .and_then(|row| row.content_digest.as_deref()),
            remote_content_digest_after: views
                .remote
                .as_ref()
                .and_then(|row| row.content_digest.as_deref()),
            sync_anchor_content_digest_after: views
                .sync_anchor
                .as_ref()
                .and_then(|row| row.content_digest.as_deref()),
            local_parent_inode_after: views.local.as_ref().and_then(|row| row.parent_inode_id),
            remote_parent_inode_after: views.remote.as_ref().and_then(|row| row.parent_inode_id),
            sync_anchor_parent_inode_after: views
                .sync_anchor
                .as_ref()
                .and_then(|row| row.parent_inode_id),
            local_display_name_after: views.local.as_ref().map(|row| row.display_name.as_str()),
            remote_display_name_after: views.remote.as_ref().map(|row| row.display_name.as_str()),
            sync_anchor_display_name_after: views
                .sync_anchor
                .as_ref()
                .map(|row| row.display_name.as_str()),
        });
    assert_invariants(
        &report,
        &[
            "same_inode_conflict_preserves_loser_artifact",
            "same_inode_conflict_keeps_canonical_path",
            "stable_paths_default_does_not_materialize_visible_sibling",
            "conflict_artifact_key_matches_namespace_and_id",
            "conflict_artifact_loser_content_is_durable",
        ],
    );
    let replanned = plan_file(&mut db, &namespace_id, InodeId(42), 103).expect("replan");
    assert_eq!(replanned.decision, PlannerDecision::NoOp);
    assert_eq!(replanned.reason, PlannerReason::AlreadyConverged);
}

#[test]
fn delete_vs_edit_conflict_preserves_artifact_and_removes_canonical_path() {
    let fixture = BoundFileFixture::new("delete-vs-edit-conflict");
    let namespace_id = fixture.namespace_id.clone();
    let current_path = fixture.mirror_root.join("reports/report.txt");
    write_file(&current_path, "local loser");

    let anchor_upload = fixture.upload_content("anchor.txt", "anchor bytes");
    seed_bound_state(
        &fixture.db_path,
        RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(12),
            revision_no: RevisionNo(1),
            content_digest: Some(anchor_upload.file_digest_sha256.clone()),
            content_manifest_digest: Some(anchor_upload.content_manifest_digest.clone()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            is_deleted: true,
        },
        LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            content_digest: Some(sha256_digest(b"local loser")),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 1,
        },
        Some(SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            synced_seq: ChangeSeq(11),
            revision_no: RevisionNo(1),
            content_digest: Some(anchor_upload.file_digest_sha256.clone()),
            content_manifest_digest: Some(anchor_upload.content_manifest_digest.clone()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
        }),
    );

    let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
    let planned =
        plan_file(&mut db, &namespace_id, InodeId(42), 100).expect("plan delete conflict");
    assert_eq!(
        planned.decision,
        PlannerDecision::ResolveDeleteVsEditConflict
    );
    drop(db);

    let executed = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen db");
        execute_next_client_action(
            &mut db,
            &fixture.store,
            |_client_file_id| None,
            |_namespace_id, _inode_id| Some(current_path.clone()),
            |_namespace_id, _inode_id, _parent_inode_id, _display_name| None,
            101,
            102,
            |_request| panic!("delete conflict should not dispatch mutations"),
        )
        .expect("execute delete conflict")
    };
    let conflict_id = match executed {
        Some(NextClientAction::ExecutedResolveDeleteVsEditConflict(
            ExecutedResolveDeleteVsEditConflict {
                applied,
                conflict_id,
            },
        )) => {
            assert_eq!(applied.inode_id, InodeId(42));
            conflict_id
        }
        other => panic!("unexpected execution result: {other:?}"),
    };

    assert!(!current_path.exists(), "canonical path should be removed");

    let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen after execution");
    let views = db
        .load_file_sync_views(&namespace_id, InodeId(42))
        .expect("load sync views");
    assert!(views.local.is_none());
    assert!(views.sync_anchor.is_none());
    let artifacts = db
        .load_conflict_artifacts_for_namespace(&namespace_id)
        .expect("load conflict artifacts");
    assert_eq!(artifacts.len(), 1);
    let artifact = &artifacts[0];
    assert_eq!(artifact.conflict_id, conflict_id);
    assert_eq!(artifact.conflict_class, ConflictClass::DeleteVsEdit);
    assert!(conflict_artifact_is_durable(
        &fixture.store,
        &namespace_id,
        artifact
    ));
    let report =
        evaluate_file_conflict_resolution_invariants(FileConflictResolutionInvariantInputs {
            conflict_kind: FileConflictResolutionKind::DeleteVsEdit,
            namespace_id: &namespace_id,
            artifact_conflict_id: Some(artifact.conflict_id.as_str()),
            artifact_object_key: Some(artifact.object_key.as_str()),
            artifact_loser_manifest_digest: Some(
                artifact.envelope.loser.content_manifest_digest.as_str(),
            ),
            artifact_loser_content_durable: true,
            local_present_after: views.local.is_some(),
            sync_anchor_present_after: views.sync_anchor.is_some(),
            final_canonical_path_exists: current_path.exists(),
            final_visible_sibling_exists: false,
            local_content_digest_after: None,
            remote_content_digest_after: views
                .remote
                .as_ref()
                .and_then(|row| row.content_digest.as_deref()),
            sync_anchor_content_digest_after: None,
            local_parent_inode_after: None,
            remote_parent_inode_after: views.remote.as_ref().and_then(|row| row.parent_inode_id),
            sync_anchor_parent_inode_after: None,
            local_display_name_after: None,
            remote_display_name_after: views.remote.as_ref().map(|row| row.display_name.as_str()),
            sync_anchor_display_name_after: None,
        });
    assert_invariants(
        &report,
        &[
            "delete_vs_edit_conflict_preserves_loser_artifact",
            "delete_vs_edit_conflict_removes_canonical_path",
            "stable_paths_default_does_not_materialize_visible_sibling",
            "conflict_artifact_key_matches_namespace_and_id",
            "conflict_artifact_loser_content_is_durable",
        ],
    );
    let replanned = plan_file(&mut db, &namespace_id, InodeId(42), 103).expect("replan");
    assert_eq!(replanned.decision, PlannerDecision::NoOp);
    assert_eq!(replanned.reason, PlannerReason::RemoteDeletedWithoutAnchor);
}

#[test]
fn rename_vs_edit_conflict_preserves_artifact_and_converges_remote_winner() {
    let fixture = BoundFileFixture::new("rename-vs-edit-conflict");
    let namespace_id = fixture.namespace_id.clone();
    let current_path = fixture.mirror_root.join("reports/report.txt");
    let target_path = fixture.mirror_root.join("archive/report-final.txt");
    fs::create_dir_all(target_path.parent().expect("target parent")).expect("create target parent");
    write_file(&current_path, "local loser");

    let anchor_upload = fixture.upload_content("anchor.txt", "anchor bytes");
    let remote_upload = fixture.upload_content("remote.txt", "remote winner");

    seed_bound_state(
        &fixture.db_path,
        RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(12),
            revision_no: RevisionNo(2),
            content_digest: Some(remote_upload.file_digest_sha256.clone()),
            content_manifest_digest: Some(remote_upload.content_manifest_digest.clone()),
            parent_inode_id: Some(InodeId(9)),
            display_name: "report-final.txt".to_owned(),
            is_deleted: false,
        },
        LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            content_digest: Some(sha256_digest(b"local loser")),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 1,
        },
        Some(SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            synced_seq: ChangeSeq(11),
            revision_no: RevisionNo(1),
            content_digest: Some(anchor_upload.file_digest_sha256.clone()),
            content_manifest_digest: Some(anchor_upload.content_manifest_digest.clone()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
        }),
    );

    let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
    let planned =
        plan_file(&mut db, &namespace_id, InodeId(42), 100).expect("plan rename conflict");
    assert_eq!(
        planned.decision,
        PlannerDecision::ResolveRenameVsEditConflict
    );
    drop(db);

    let executed = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen db");
        execute_next_client_action(
            &mut db,
            &fixture.store,
            |_client_file_id| None,
            |_namespace_id, _inode_id| Some(current_path.clone()),
            |_namespace_id, _inode_id, _parent_inode_id, _display_name| Some(target_path.clone()),
            101,
            102,
            |_request| panic!("rename conflict should not dispatch mutations"),
        )
        .expect("execute rename conflict")
    };
    let conflict_id = match executed {
        Some(NextClientAction::ExecutedResolveRenameVsEditConflict(
            ExecutedResolveRenameVsEditConflict {
                applied,
                conflict_id,
            },
        )) => {
            assert_eq!(applied.inode_id, InodeId(42));
            conflict_id
        }
        other => panic!("unexpected execution result: {other:?}"),
    };

    assert!(!current_path.exists(), "old path should be removed");
    assert_eq!(
        fs::read_to_string(&target_path).expect("read target path"),
        "remote winner"
    );

    let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen after execution");
    let views = db
        .load_file_sync_views(&namespace_id, InodeId(42))
        .expect("load sync views");
    let artifacts = db
        .load_conflict_artifacts_for_namespace(&namespace_id)
        .expect("load conflict artifacts");
    assert_eq!(artifacts.len(), 1);
    let artifact = &artifacts[0];
    assert_eq!(artifact.conflict_id, conflict_id);
    assert_eq!(artifact.conflict_class, ConflictClass::RenameVsEdit);
    let report =
        evaluate_file_conflict_resolution_invariants(FileConflictResolutionInvariantInputs {
            conflict_kind: FileConflictResolutionKind::RenameVsEdit,
            namespace_id: &namespace_id,
            artifact_conflict_id: Some(artifact.conflict_id.as_str()),
            artifact_object_key: Some(artifact.object_key.as_str()),
            artifact_loser_manifest_digest: Some(
                artifact.envelope.loser.content_manifest_digest.as_str(),
            ),
            artifact_loser_content_durable: conflict_artifact_is_durable(
                &fixture.store,
                &namespace_id,
                artifact,
            ),
            local_present_after: views.local.is_some(),
            sync_anchor_present_after: views.sync_anchor.is_some(),
            final_canonical_path_exists: target_path.exists(),
            final_visible_sibling_exists: false,
            local_content_digest_after: views
                .local
                .as_ref()
                .and_then(|row| row.content_digest.as_deref()),
            remote_content_digest_after: views
                .remote
                .as_ref()
                .and_then(|row| row.content_digest.as_deref()),
            sync_anchor_content_digest_after: views
                .sync_anchor
                .as_ref()
                .and_then(|row| row.content_digest.as_deref()),
            local_parent_inode_after: views.local.as_ref().and_then(|row| row.parent_inode_id),
            remote_parent_inode_after: views.remote.as_ref().and_then(|row| row.parent_inode_id),
            sync_anchor_parent_inode_after: views
                .sync_anchor
                .as_ref()
                .and_then(|row| row.parent_inode_id),
            local_display_name_after: views.local.as_ref().map(|row| row.display_name.as_str()),
            remote_display_name_after: views.remote.as_ref().map(|row| row.display_name.as_str()),
            sync_anchor_display_name_after: views
                .sync_anchor
                .as_ref()
                .map(|row| row.display_name.as_str()),
        });
    assert_invariants(
        &report,
        &[
            "rename_vs_edit_conflict_preserves_loser_artifact",
            "rename_vs_edit_conflict_converges_remote_winner",
            "stable_paths_default_does_not_materialize_visible_sibling",
            "conflict_artifact_key_matches_namespace_and_id",
            "conflict_artifact_loser_content_is_durable",
        ],
    );
    let replanned = plan_file(&mut db, &namespace_id, InodeId(42), 103).expect("replan");
    assert_eq!(replanned.decision, PlannerDecision::NoOp);
}

#[test]
fn path_binding_collision_preserves_loser_artifact_and_keeps_winner_path() {
    let fixture = BoundFileFixture::new("path-binding-collision");
    let namespace_id = fixture.namespace_id.clone();
    let collision_path = fixture.mirror_root.join("reports/report.txt");
    write_file(&collision_path, "local-only loser");

    let remote_upload = fixture.upload_content("remote.txt", "authoritative winner");
    seed_path_binding_collision_state(
        &fixture.db_path,
        RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(12),
            revision_no: RevisionNo(2),
            content_digest: Some(remote_upload.file_digest_sha256.clone()),
            content_manifest_digest: Some(remote_upload.content_manifest_digest.clone()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            is_deleted: false,
        },
        LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            content_digest: None,
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            exists_on_disk: false,
            dirty: false,
            last_local_change_ms: 1,
        },
        LocalOnlyFileStateRow {
            client_file_id: ClientFileId::new("tmp:ns-1:1"),
            namespace_id: namespace_id.clone(),
            inode_kind: InodeKind::File,
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            content_digest: Some(sha256_digest(b"local-only loser")),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 1,
        },
    );

    let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
    let planned = plan_file(&mut db, &namespace_id, InodeId(42), 100).expect("plan collision");
    assert_eq!(
        planned.decision,
        PlannerDecision::ResolvePathBindingCollision
    );
    drop(db);

    let executed = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen db");
        execute_next_client_action(
            &mut db,
            &fixture.store,
            |_client_file_id| Some(collision_path.clone()),
            |_namespace_id, _inode_id| None,
            |_namespace_id, _inode_id, _parent_inode_id, _display_name| None,
            101,
            102,
            |_request| panic!("path binding collision should not dispatch mutations"),
        )
        .expect("execute path binding collision")
    };
    let conflict_id = match executed {
        Some(NextClientAction::ExecutedResolvePathBindingCollision(
            ExecutedResolvePathBindingCollision {
                applied,
                conflict_id,
            },
        )) => {
            assert_eq!(applied.inode_id, InodeId(42));
            conflict_id
        }
        other => panic!("unexpected execution result: {other:?}"),
    };

    assert_eq!(
        fs::read_to_string(&collision_path).expect("read canonical winner path"),
        "authoritative winner"
    );

    let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen after execution");
    let views = db
        .load_file_sync_views(&namespace_id, InodeId(42))
        .expect("load sync views");
    assert!(db
        .load_local_only_file(&ClientFileId::new("tmp:ns-1:1"))
        .expect("load local only")
        .is_none());
    let artifacts = db
        .load_conflict_artifacts_for_namespace(&namespace_id)
        .expect("load conflict artifacts");
    assert_eq!(artifacts.len(), 1);
    let artifact = &artifacts[0];
    assert_eq!(artifact.conflict_id, conflict_id);
    assert_eq!(artifact.conflict_class, ConflictClass::PathBindingCollision);
    let report =
        evaluate_file_conflict_resolution_invariants(FileConflictResolutionInvariantInputs {
            conflict_kind: FileConflictResolutionKind::PathBindingCollision,
            namespace_id: &namespace_id,
            artifact_conflict_id: Some(artifact.conflict_id.as_str()),
            artifact_object_key: Some(artifact.object_key.as_str()),
            artifact_loser_manifest_digest: Some(
                artifact.envelope.loser.content_manifest_digest.as_str(),
            ),
            artifact_loser_content_durable: conflict_artifact_is_durable(
                &fixture.store,
                &namespace_id,
                artifact,
            ),
            local_present_after: views.local.is_some(),
            sync_anchor_present_after: views.sync_anchor.is_some(),
            final_canonical_path_exists: collision_path.exists(),
            final_visible_sibling_exists: false,
            local_content_digest_after: views
                .local
                .as_ref()
                .and_then(|row| row.content_digest.as_deref()),
            remote_content_digest_after: views
                .remote
                .as_ref()
                .and_then(|row| row.content_digest.as_deref()),
            sync_anchor_content_digest_after: views
                .sync_anchor
                .as_ref()
                .and_then(|row| row.content_digest.as_deref()),
            local_parent_inode_after: views.local.as_ref().and_then(|row| row.parent_inode_id),
            remote_parent_inode_after: views.remote.as_ref().and_then(|row| row.parent_inode_id),
            sync_anchor_parent_inode_after: views
                .sync_anchor
                .as_ref()
                .and_then(|row| row.parent_inode_id),
            local_display_name_after: views.local.as_ref().map(|row| row.display_name.as_str()),
            remote_display_name_after: views.remote.as_ref().map(|row| row.display_name.as_str()),
            sync_anchor_display_name_after: views
                .sync_anchor
                .as_ref()
                .map(|row| row.display_name.as_str()),
        });
    assert_invariants(
        &report,
        &[
            "path_binding_collision_preserves_loser_artifact",
            "path_binding_collision_keeps_winner_canonical_path",
            "stable_paths_default_does_not_materialize_visible_sibling",
            "conflict_artifact_key_matches_namespace_and_id",
            "conflict_artifact_loser_content_is_durable",
        ],
    );
    let replanned = plan_file(&mut db, &namespace_id, InodeId(42), 103).expect("replan");
    assert_eq!(replanned.decision, PlannerDecision::NoOp);
}

#[test]
fn remote_path_and_content_change_applies_without_conflict_artifact() {
    let fixture = BoundFileFixture::new("rename-and-replace");
    let namespace_id = fixture.namespace_id.clone();
    let current_path = fixture.mirror_root.join("reports/report.txt");
    let target_path = fixture.mirror_root.join("archive/report-final.txt");
    fs::create_dir_all(target_path.parent().expect("target parent")).expect("create target parent");
    write_file(&current_path, "anchor bytes");

    let anchor_upload = fixture.upload_content("anchor.txt", "anchor bytes");
    let remote_upload = fixture.upload_content("remote.txt", "remote winner");

    seed_bound_state(
        &fixture.db_path,
        RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(12),
            revision_no: RevisionNo(2),
            content_digest: Some(remote_upload.file_digest_sha256.clone()),
            content_manifest_digest: Some(remote_upload.content_manifest_digest.clone()),
            parent_inode_id: Some(InodeId(9)),
            display_name: "report-final.txt".to_owned(),
            is_deleted: false,
        },
        LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            content_digest: Some(anchor_upload.file_digest_sha256.clone()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1,
        },
        Some(SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            synced_seq: ChangeSeq(11),
            revision_no: RevisionNo(1),
            content_digest: Some(anchor_upload.file_digest_sha256.clone()),
            content_manifest_digest: Some(anchor_upload.content_manifest_digest.clone()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
        }),
    );

    let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
    let planned = plan_file(&mut db, &namespace_id, InodeId(42), 100).expect("plan rename replace");
    assert_eq!(
        planned.decision,
        PlannerDecision::ApplyRemoteRenameAndReplace
    );
    assert_eq!(
        planned.reason,
        PlannerReason::RemotePathAndContentDifferFromAnchor
    );
    drop(db);

    let executed = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen db");
        execute_next_client_action(
            &mut db,
            &fixture.store,
            |_client_file_id| None,
            |_namespace_id, _inode_id| Some(current_path.clone()),
            |_namespace_id, _inode_id, _parent_inode_id, _display_name| Some(target_path.clone()),
            101,
            102,
            |_request| panic!("rename and replace should not dispatch mutations"),
        )
        .expect("execute rename replace")
    };
    match executed {
        Some(NextClientAction::ExecutedApplyRemoteRenameAndReplace(
            ExecutedApplyRemoteRenameAndReplace { applied },
        )) => {
            assert_eq!(applied.inode_id, InodeId(42));
        }
        other => panic!("unexpected execution result: {other:?}"),
    }

    assert!(!current_path.exists(), "old path should be removed");
    assert_eq!(
        fs::read_to_string(&target_path).expect("read target"),
        "remote winner"
    );

    let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen after execution");
    assert!(
        db.load_conflict_artifacts_for_namespace(&namespace_id)
            .expect("load artifacts")
            .is_empty(),
        "direct authoritative apply should not create a conflict artifact"
    );
    let replanned = plan_file(&mut db, &namespace_id, InodeId(42), 103).expect("replan");
    assert_eq!(replanned.decision, PlannerDecision::NoOp);
    assert_eq!(replanned.reason, PlannerReason::AlreadyConverged);
}

struct BoundFileFixture {
    namespace_id: NamespaceId,
    db_path: PathBuf,
    mirror_root: PathBuf,
    upload_root: PathBuf,
    store: LocalFsStore,
}

impl BoundFileFixture {
    fn new(label: &str) -> Self {
        let temp_dir = TestDir::new(label);
        let db_path = temp_dir.path().join("client.sqlite3");
        let mirror_root = temp_dir.path().join("mirror");
        let upload_root = temp_dir.path().join("uploads");
        let store_root = temp_dir.path().join("objectstore");
        fs::create_dir_all(&mirror_root).expect("create mirror root");
        fs::create_dir_all(&upload_root).expect("create upload root");
        fs::create_dir_all(&store_root).expect("create store root");
        let store = LocalFsStore::new(&store_root).expect("create local store");
        Self {
            namespace_id: NamespaceId::new("ns-1"),
            db_path,
            mirror_root,
            upload_root,
            store,
        }
    }

    fn upload_content(&self, file_name: &str, content: &str) -> UploadedContent {
        let path = self.upload_root.join(file_name);
        write_file(&path, content);
        upload_small_file_from_path(&self.store, &self.namespace_id, &path)
            .expect("upload content fixture")
    }
}

fn seed_bound_state(
    db_path: &Path,
    remote: RemoteFileStateRow,
    local: LocalFileStateRow,
    anchor: Option<SyncAnchorRow>,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open state db");
    db.planner_transaction("seed-bound-state", |tx| {
        tx.upsert_remote_file(&remote)?;
        tx.upsert_local_file(&local)?;
        if let Some(anchor) = &anchor {
            tx.upsert_sync_anchor(anchor)?;
        }
        Ok(())
    })
    .expect("seed bound state");
}

fn seed_path_binding_collision_state(
    db_path: &Path,
    remote: RemoteFileStateRow,
    placeholder: LocalFileStateRow,
    local_only: LocalOnlyFileStateRow,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open state db");
    db.planner_transaction("seed-path-binding-collision", |tx| {
        tx.upsert_remote_file(&remote)?;
        tx.upsert_local_file(&placeholder)?;
        tx.upsert_local_only_file(&local_only)?;
        tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
            client_file_id: local_only.client_file_id.clone(),
            namespace_id: local_only.namespace_id.clone(),
            decision: "upload_local_create".to_owned(),
            reason: "local_only_file_without_remote_identity".to_owned(),
            created_at_ms: 1,
        })?;
        Ok(())
    })
    .expect("seed path binding collision state");
}

fn conflict_artifact_is_durable(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    artifact: &ConflictArtifactRow,
) -> bool {
    let manifest_digest = artifact.envelope.loser.content_manifest_digest.as_str();
    let Some(manifest_bytes) = store
        .get(
            &content_manifest(namespace_id.as_str(), manifest_digest),
            None,
        )
        .expect("load loser manifest object")
    else {
        return false;
    };
    let manifest = decode_content_manifest_json(&manifest_bytes).expect("decode loser manifest");
    manifest.payload.blocks.iter().all(|block| {
        store
            .get(
                &blob(namespace_id.as_str(), &block.content_digest_sha256),
                None,
            )
            .expect("load loser block object")
            .is_some()
    })
}

fn write_file(path: &Path, content: &str) {
    fs::create_dir_all(path.parent().expect("parent")).expect("create parent dir");
    fs::write(path, content.as_bytes()).expect("write file");
}

fn assert_invariants(
    report: &loon_testkit::invariants::ClientReconciliationInvariantReport,
    expected: &[&str],
) {
    for name in expected {
        let check = report
            .check(name)
            .unwrap_or_else(|| panic!("missing invariant `{name}`"));
        assert!(check.passed, "invariant `{name}` failed: {}", check.detail);
    }
}
