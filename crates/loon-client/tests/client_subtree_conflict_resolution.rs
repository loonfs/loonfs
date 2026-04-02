use loon_client::executor::{
    execute_next_client_action, ExecutedResolveSubtreeDeleteConflict,
    ExecutedResolveSubtreeRenameConflict, NextClientAction,
};
use loon_client::planner::{plan_file, PlannerDecision, PlannerReason};
use loon_client::state_db::{
    ClientFileId, ConflictArtifactRow, LocalFileStateRow, LocalOnlyFileStateRow,
    LocalOnlyPlannedActionRow, RemoteFileStateRow, SqliteStateDb, SyncAnchorRow,
};
use loon_client::upload::{upload_small_file_from_path, UploadedContent};
use loon_server::objectstore::fs::LocalFsStore;
use loon_server::objectstore::keys::{blob, content_manifest};
use loon_server::objectstore::ObjectStore;
use loon_testkit::invariants::{
    evaluate_subtree_conflict_resolution_invariants, SubtreeConflictResolutionInvariantInputs,
    SubtreeConflictResolutionKind,
};
use loon_testkit::tempdir::TestDir;
use loon_types::{
    decode_content_manifest_json, sha256_digest, ChangeSeq, ConflictArtifactKind, ConflictClass,
    InodeId, InodeKind, NamespaceId, RevisionNo,
};
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn subtree_delete_conflict_preserves_artifact_and_removes_canonical_path() {
    let fixture = BoundSubtreeFixture::new("subtree-delete-conflict");
    let namespace_id = fixture.namespace_id.clone();
    let root_path = fixture.mirror_root.join("projects/root");
    let report_path = root_path.join("nested/report.txt");
    let temp_path = root_path.join("scratch.txt");
    write_file(&report_path, "local loser");
    write_file(&temp_path, "temp draft");

    let authoritative_upload = fixture.upload_content("authoritative.txt", "anchor bytes");
    seed_subtree_delete_conflict_state(&fixture.db_path, &namespace_id, &authoritative_upload);

    let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
    let planned = plan_file(&mut db, &namespace_id, InodeId(10), 100).expect("plan delete");
    assert_eq!(
        planned.decision,
        PlannerDecision::ResolveSubtreeDeleteConflict
    );
    assert_eq!(
        planned.reason,
        PlannerReason::RemoteSubtreeDeletedWhileDescendantsDifferFromAnchor
    );
    drop(db);

    let executed = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen db");
        execute_next_client_action(
            &mut db,
            &fixture.store,
            |_client_file_id| None,
            |_namespace_id, _inode_id| Some(root_path.clone()),
            |_namespace_id, _inode_id, _parent_inode_id, _display_name| None,
            101,
            102,
            |_request| panic!("subtree conflict resolution should not dispatch mutations"),
        )
        .expect("execute subtree delete conflict")
    };
    let conflict_id = match executed {
        Some(NextClientAction::ExecutedResolveSubtreeDeleteConflict(
            ExecutedResolveSubtreeDeleteConflict {
                applied,
                conflict_id,
            },
        )) => {
            assert_eq!(applied.inode_id, InodeId(10));
            conflict_id
        }
        other => panic!("unexpected execution result: {other:?}"),
    };

    assert!(
        !root_path.exists(),
        "deleted subtree root should be removed"
    );

    let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen after execution");
    let artifacts = db
        .load_conflict_artifacts_for_namespace(&namespace_id)
        .expect("load subtree artifacts");
    assert_eq!(artifacts.len(), 1);
    let artifact = &artifacts[0];
    assert_eq!(artifact.conflict_id, conflict_id);
    assert_eq!(artifact.artifact_kind, ConflictArtifactKind::Subtree);
    assert_eq!(
        artifact.conflict_class,
        ConflictClass::SubtreeDeleteVsLocalChanges
    );
    let envelope = artifact
        .subtree_envelope()
        .expect("subtree artifact envelope");
    assert_eq!(envelope.entries.len(), 3);
    assert!(subtree_conflict_artifact_entries_are_durable(
        &fixture.store,
        &namespace_id,
        artifact
    ));

    assert!(
        db.load_file_sync_views(&namespace_id, InodeId(10))
            .expect("load root views")
            .remote
            .expect("root remote row")
            .is_deleted
    );
    assert!(db
        .load_file_sync_views(&namespace_id, InodeId(12))
        .expect("load nested dir views")
        .local
        .is_none());
    assert!(db
        .load_local_only_file(&ClientFileId::new("tmp:ns-1:00000000000000000001"))
        .expect("load local-only child")
        .is_none());

    let report =
        evaluate_subtree_conflict_resolution_invariants(SubtreeConflictResolutionInvariantInputs {
            conflict_kind: SubtreeConflictResolutionKind::Delete,
            namespace_id: &namespace_id,
            artifact_conflict_id: Some(artifact.conflict_id.as_str()),
            artifact_object_key: Some(artifact.object_key.as_str()),
            artifact_entry_count: envelope.entries.len(),
            expected_artifact_entry_count: 3,
            artifact_entries_durable: subtree_conflict_artifact_entries_are_durable(
                &fixture.store,
                &namespace_id,
                artifact,
            ),
            preserved_temp_rows_after: usize::from(
                db.load_local_only_file(&ClientFileId::new("tmp:ns-1:00000000000000000001"))
                    .expect("load local-only child after delete")
                    .is_some(),
            ),
            final_root_exists: root_path.exists(),
            final_target_exists: false,
            bound_descendants_match_winner_after: false,
            final_visible_sibling_tree_exists: false,
        });
    assert_invariants(
        &report,
        &[
            "subtree_delete_conflict_preserves_loser_artifact",
            "subtree_delete_conflict_preserves_full_subtree_entries",
            "subtree_delete_conflict_removes_canonical_path",
            "subtree_delete_conflict_clears_preserved_temp_rows",
            "subtree_conflict_artifact_key_matches_namespace_and_id",
            "subtree_conflict_artifact_entries_are_durable",
            "stable_paths_default_does_not_materialize_visible_sibling_tree",
        ],
    );

    let replanned = plan_file(&mut db, &namespace_id, InodeId(10), 103).expect("replan");
    assert_eq!(replanned.decision, PlannerDecision::NoOp);
    assert_eq!(
        replanned.reason,
        PlannerReason::RemoteSubtreeDeletedWithoutAnchor
    );
}

#[test]
fn subtree_rename_conflict_preserves_artifact_and_keeps_winner_path() {
    let fixture = BoundSubtreeFixture::new("subtree-rename-conflict");
    let namespace_id = fixture.namespace_id.clone();
    let root_path = fixture.mirror_root.join("projects/root");
    let target_path = fixture.mirror_root.join("archive/root-renamed");
    let report_path = root_path.join("nested/report.txt");
    let temp_path = root_path.join("scratch.txt");
    write_file(&report_path, "local loser");
    write_file(&temp_path, "temp draft");
    fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create target parent dir");

    let authoritative_upload = fixture.upload_content("authoritative.txt", "anchor bytes");
    seed_subtree_rename_conflict_state(&fixture.db_path, &namespace_id, &authoritative_upload);

    let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
    let planned = plan_file(&mut db, &namespace_id, InodeId(10), 100).expect("plan rename");
    assert_eq!(
        planned.decision,
        PlannerDecision::ResolveSubtreeRenameConflict
    );
    assert_eq!(
        planned.reason,
        PlannerReason::RemoteSubtreePathDiffersWhileDescendantsDifferFromAnchor
    );
    drop(db);

    let executed = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen db");
        execute_next_client_action(
            &mut db,
            &fixture.store,
            |_client_file_id| None,
            |_namespace_id, _inode_id| Some(root_path.clone()),
            |_namespace_id, _inode_id, _parent_inode_id, _display_name| Some(target_path.clone()),
            101,
            102,
            |_request| panic!("subtree conflict resolution should not dispatch mutations"),
        )
        .expect("execute subtree rename conflict")
    };
    let conflict_id = match executed {
        Some(NextClientAction::ExecutedResolveSubtreeRenameConflict(
            ExecutedResolveSubtreeRenameConflict {
                applied,
                conflict_id,
            },
        )) => {
            assert_eq!(applied.inode_id, InodeId(10));
            conflict_id
        }
        other => panic!("unexpected execution result: {other:?}"),
    };

    assert!(!root_path.exists(), "old subtree root should be removed");
    assert!(target_path.exists(), "renamed subtree target should exist");
    assert_eq!(
        fs::read_to_string(target_path.join("nested/report.txt")).expect("read restored file"),
        "anchor bytes"
    );
    assert!(
        !target_path.join("scratch.txt").exists(),
        "temp loser file should not survive in winner subtree"
    );

    let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen after execution");
    let artifacts = db
        .load_conflict_artifacts_for_namespace(&namespace_id)
        .expect("load subtree artifacts");
    assert_eq!(artifacts.len(), 1);
    let artifact = &artifacts[0];
    assert_eq!(artifact.conflict_id, conflict_id);
    assert_eq!(artifact.artifact_kind, ConflictArtifactKind::Subtree);
    assert_eq!(
        artifact.conflict_class,
        ConflictClass::SubtreeRenameVsLocalChanges
    );
    let envelope = artifact
        .subtree_envelope()
        .expect("subtree artifact envelope");
    assert_eq!(envelope.entries.len(), 3);
    assert!(subtree_conflict_artifact_entries_are_durable(
        &fixture.store,
        &namespace_id,
        artifact
    ));

    let root_views = db
        .load_file_sync_views(&namespace_id, InodeId(10))
        .expect("load root views");
    let report_views = db
        .load_file_sync_views(&namespace_id, InodeId(13))
        .expect("load report file views");
    assert_eq!(
        root_views
            .local
            .as_ref()
            .and_then(|row| row.parent_inode_id),
        Some(InodeId(20))
    );
    assert_eq!(
        root_views
            .local
            .as_ref()
            .map(|row| row.display_name.as_str()),
        Some("root-renamed")
    );
    assert_eq!(
        report_views
            .local
            .as_ref()
            .and_then(|row| row.content_digest.as_deref()),
        report_views
            .sync_anchor
            .as_ref()
            .and_then(|row| row.content_digest.as_deref())
    );
    assert!(db
        .load_local_only_file(&ClientFileId::new("tmp:ns-1:00000000000000000001"))
        .expect("load local-only child")
        .is_none());

    let report =
        evaluate_subtree_conflict_resolution_invariants(SubtreeConflictResolutionInvariantInputs {
            conflict_kind: SubtreeConflictResolutionKind::Rename,
            namespace_id: &namespace_id,
            artifact_conflict_id: Some(artifact.conflict_id.as_str()),
            artifact_object_key: Some(artifact.object_key.as_str()),
            artifact_entry_count: envelope.entries.len(),
            expected_artifact_entry_count: 3,
            artifact_entries_durable: subtree_conflict_artifact_entries_are_durable(
                &fixture.store,
                &namespace_id,
                artifact,
            ),
            preserved_temp_rows_after: usize::from(
                db.load_local_only_file(&ClientFileId::new("tmp:ns-1:00000000000000000001"))
                    .expect("load local-only child after rename")
                    .is_some(),
            ),
            final_root_exists: root_path.exists(),
            final_target_exists: target_path.exists(),
            bound_descendants_match_winner_after: report_views.local.as_ref().is_some_and(
                |local| {
                    report_views.sync_anchor.as_ref().is_some_and(|anchor| {
                        local.content_digest == anchor.content_digest
                            && local.parent_inode_id == anchor.parent_inode_id
                            && local.display_name == anchor.display_name
                            && !local.dirty
                    })
                },
            ),
            final_visible_sibling_tree_exists: false,
        });
    assert_invariants(
        &report,
        &[
            "subtree_rename_conflict_preserves_loser_artifact",
            "subtree_rename_conflict_reverts_bound_descendants_to_winner_state",
            "subtree_rename_conflict_keeps_winner_canonical_path",
            "subtree_rename_conflict_clears_preserved_temp_rows",
            "subtree_conflict_artifact_key_matches_namespace_and_id",
            "subtree_conflict_artifact_entries_are_durable",
            "stable_paths_default_does_not_materialize_visible_sibling_tree",
        ],
    );

    let replanned = plan_file(&mut db, &namespace_id, InodeId(10), 103).expect("replan");
    assert_eq!(replanned.decision, PlannerDecision::NoOp);
    assert_eq!(replanned.reason, PlannerReason::AlreadyConverged);
}

struct BoundSubtreeFixture {
    namespace_id: NamespaceId,
    db_path: PathBuf,
    mirror_root: PathBuf,
    upload_root: PathBuf,
    store: LocalFsStore,
}

impl BoundSubtreeFixture {
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

fn seed_subtree_delete_conflict_state(
    db_path: &Path,
    namespace_id: &NamespaceId,
    authoritative_upload: &UploadedContent,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open state db");
    db.planner_transaction("seed-subtree-delete-conflict", |tx| {
        seed_common_subtree(tx, namespace_id, authoritative_upload, true)?;
        Ok(())
    })
    .expect("seed subtree delete conflict state");
}

fn seed_subtree_rename_conflict_state(
    db_path: &Path,
    namespace_id: &NamespaceId,
    authoritative_upload: &UploadedContent,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open state db");
    db.planner_transaction("seed-subtree-rename-conflict", |tx| {
        seed_common_subtree(tx, namespace_id, authoritative_upload, false)?;
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(20),
            inode_kind: InodeKind::Dir,
            observed_seq: ChangeSeq(11),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: None,
            display_name: "archive".to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(20),
            inode_kind: InodeKind::Dir,
            content_digest: None,
            parent_inode_id: None,
            display_name: "archive".to_owned(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1,
        })?;
        tx.upsert_sync_anchor(&SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(20),
            inode_kind: InodeKind::Dir,
            synced_seq: ChangeSeq(11),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: None,
            display_name: "archive".to_owned(),
        })?;
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(10),
            inode_kind: InodeKind::Dir,
            observed_seq: ChangeSeq(12),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: Some(InodeId(20)),
            display_name: "root-renamed".to_owned(),
            is_deleted: false,
        })?;
        Ok(())
    })
    .expect("seed subtree rename conflict state");
}

fn seed_common_subtree(
    tx: &mut loon_client::state_db::PlannerTxn<'_>,
    namespace_id: &NamespaceId,
    authoritative_upload: &UploadedContent,
    root_deleted: bool,
) -> Result<(), loon_client::state_db::StateDbError> {
    tx.upsert_remote_file(&RemoteFileStateRow {
        namespace_id: namespace_id.clone(),
        inode_id: InodeId(10),
        inode_kind: InodeKind::Dir,
        observed_seq: ChangeSeq(12),
        revision_no: RevisionNo(1),
        content_digest: None,
        content_manifest_digest: None,
        parent_inode_id: None,
        display_name: "root".to_owned(),
        is_deleted: root_deleted,
    })?;
    tx.upsert_local_file(&LocalFileStateRow {
        namespace_id: namespace_id.clone(),
        inode_id: InodeId(10),
        inode_kind: InodeKind::Dir,
        content_digest: None,
        parent_inode_id: None,
        display_name: "root".to_owned(),
        exists_on_disk: true,
        dirty: false,
        last_local_change_ms: 1,
    })?;
    tx.upsert_sync_anchor(&SyncAnchorRow {
        namespace_id: namespace_id.clone(),
        inode_id: InodeId(10),
        inode_kind: InodeKind::Dir,
        synced_seq: ChangeSeq(11),
        revision_no: RevisionNo(1),
        content_digest: None,
        content_manifest_digest: None,
        parent_inode_id: None,
        display_name: "root".to_owned(),
    })?;

    tx.upsert_remote_file(&RemoteFileStateRow {
        namespace_id: namespace_id.clone(),
        inode_id: InodeId(12),
        inode_kind: InodeKind::Dir,
        observed_seq: ChangeSeq(11),
        revision_no: RevisionNo(1),
        content_digest: None,
        content_manifest_digest: None,
        parent_inode_id: Some(InodeId(10)),
        display_name: "nested".to_owned(),
        is_deleted: false,
    })?;
    tx.upsert_local_file(&LocalFileStateRow {
        namespace_id: namespace_id.clone(),
        inode_id: InodeId(12),
        inode_kind: InodeKind::Dir,
        content_digest: None,
        parent_inode_id: Some(InodeId(10)),
        display_name: "nested".to_owned(),
        exists_on_disk: true,
        dirty: false,
        last_local_change_ms: 1,
    })?;
    tx.upsert_sync_anchor(&SyncAnchorRow {
        namespace_id: namespace_id.clone(),
        inode_id: InodeId(12),
        inode_kind: InodeKind::Dir,
        synced_seq: ChangeSeq(11),
        revision_no: RevisionNo(1),
        content_digest: None,
        content_manifest_digest: None,
        parent_inode_id: Some(InodeId(10)),
        display_name: "nested".to_owned(),
    })?;

    tx.upsert_remote_file(&RemoteFileStateRow {
        namespace_id: namespace_id.clone(),
        inode_id: InodeId(13),
        inode_kind: InodeKind::File,
        observed_seq: ChangeSeq(11),
        revision_no: RevisionNo(1),
        content_digest: Some(authoritative_upload.file_digest_sha256.clone()),
        content_manifest_digest: Some(authoritative_upload.content_manifest_digest.clone()),
        parent_inode_id: Some(InodeId(12)),
        display_name: "report.txt".to_owned(),
        is_deleted: false,
    })?;
    tx.upsert_local_file(&LocalFileStateRow {
        namespace_id: namespace_id.clone(),
        inode_id: InodeId(13),
        inode_kind: InodeKind::File,
        content_digest: Some(sha256_digest(b"local loser")),
        parent_inode_id: Some(InodeId(12)),
        display_name: "report.txt".to_owned(),
        exists_on_disk: true,
        dirty: true,
        last_local_change_ms: 1,
    })?;
    tx.upsert_sync_anchor(&SyncAnchorRow {
        namespace_id: namespace_id.clone(),
        inode_id: InodeId(13),
        inode_kind: InodeKind::File,
        synced_seq: ChangeSeq(11),
        revision_no: RevisionNo(1),
        content_digest: Some(authoritative_upload.file_digest_sha256.clone()),
        content_manifest_digest: Some(authoritative_upload.content_manifest_digest.clone()),
        parent_inode_id: Some(InodeId(12)),
        display_name: "report.txt".to_owned(),
    })?;

    let local_only = LocalOnlyFileStateRow {
        client_file_id: ClientFileId::new("tmp:ns-1:00000000000000000001"),
        namespace_id: namespace_id.clone(),
        inode_kind: InodeKind::File,
        parent_inode_id: Some(InodeId(10)),
        display_name: "scratch.txt".to_owned(),
        content_digest: Some(sha256_digest(b"temp draft")),
        exists_on_disk: true,
        dirty: true,
        last_local_change_ms: 1,
    };
    tx.upsert_local_only_file(&local_only)?;
    tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
        client_file_id: local_only.client_file_id.clone(),
        namespace_id: namespace_id.clone(),
        decision: "upload_local_create".to_owned(),
        reason: "local_only_file_without_remote_identity".to_owned(),
        created_at_ms: 1,
    })?;

    Ok(())
}

fn subtree_conflict_artifact_entries_are_durable(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    artifact: &ConflictArtifactRow,
) -> bool {
    let Some(envelope) = artifact.subtree_envelope() else {
        return false;
    };
    envelope.entries.iter().all(|entry| {
        let Some(manifest_digest) = entry.content_manifest_digest.as_deref() else {
            return true;
        };
        let Some(manifest_bytes) = store
            .get(
                &content_manifest(namespace_id.as_str(), manifest_digest),
                None,
            )
            .expect("load subtree loser manifest")
        else {
            return false;
        };
        let manifest =
            decode_content_manifest_json(&manifest_bytes).expect("decode subtree loser manifest");
        manifest.payload.blocks.iter().all(|block| {
            store
                .get(
                    &blob(namespace_id.as_str(), &block.content_digest_sha256),
                    None,
                )
                .expect("load subtree loser block")
                .is_some()
        })
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
