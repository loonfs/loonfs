use loon_client::local_fs::{
    observe_local_path, observe_subtree_path, reduce_fs_event_batch, NormalizedFsEvent,
    NormalizedFsEventBatch, ReducedLocalObservationIntent,
};
use loon_client::state_db::{
    LocalFileStateRow, ObservedBoundInode, ObservedLocalOnlySubtreeInode, RemoteFileStateRow,
    SqliteStateDb, SubtreeLocalOnlyParentRef, SubtreeObservationOp, SubtreeObservationOutcome,
    SyncAnchorRow,
};
use loon_testkit::tempdir::TestDir;
use loon_types::{sha256_digest, ChangeSeq, InodeId, InodeKind, NamespaceId, RevisionNo};
use serde::Deserialize;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::symlink;

#[test]
fn reducer_fixtures_cover_generic_filesystem_event_normalization() {
    for fixture_path in [
        "client/fs_events_create_then_write_reduces_to_observe_local.yaml",
        "client/fs_events_explicit_rename_reduces_to_observe_move.yaml",
        "client/fs_events_remove_plus_create_with_unique_native_id_reduces_to_observe_move.yaml",
        "client/fs_events_directory_rename_reduces_to_observe_move.yaml",
        "client/fs_events_repeated_edits_under_one_directory_reduce_to_observe_subtree.yaml",
        "client/fs_events_subtree_delete_burst_reduces_to_highest_root_delete.yaml",
        "client/fs_events_atomic_save_without_unique_pairing_returns_error.yaml",
        "client/fs_events_conflicting_rename_edges_return_error.yaml",
        "client/fs_events_conflicting_native_id_reuse_returns_error.yaml",
        "client/fs_events_descendants_under_reduced_root_are_absorbed.yaml",
    ] {
        run_reducer_fixture(fixture_path);
    }
}

#[test]
fn client_local_fs_observe_local_path_preserves_bound_edit_behavior() {
    let temp_dir = TestDir::new("client-local-fs-observe-local");
    let db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    fs::create_dir_all(&mirror_root).expect("create mirror root");

    let mut db = SqliteStateDb::open(&db_path).expect("open db");
    seed_bound_root_directory(&mut db);
    seed_bound_file(
        &mut db,
        InodeId(2),
        "hello.txt",
        InodeId(1),
        "sha256:hello-v1",
    );

    fs::write(mirror_root.join("hello.txt"), b"hello v2\n").expect("write current file");

    let report = observe_local_path(
        &db_path,
        &demo_namespace(),
        &mirror_root,
        temp_dir.path(),
        &mirror_root.join("hello.txt"),
        2_000,
    )
    .expect("observe local path");

    assert_eq!(report.relative_path, "hello.txt");
    assert_eq!(report.observation_kind.as_str(), "bound_file");
    assert_eq!(report.planned_decision, "upload_local_edit");
    assert_eq!(report.inode_id, Some(InodeId(2)));
}

#[test]
fn client_local_fs_observe_local_path_keeps_clean_bound_file_noop_when_digest_matches() {
    let temp_dir = TestDir::new("client-local-fs-observe-local-noop");
    let db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    fs::create_dir_all(&mirror_root).expect("create mirror root");

    let mut db = SqliteStateDb::open(&db_path).expect("open db");
    seed_bound_root_directory(&mut db);
    let hello_bytes = b"hello v1\n";
    let hello_digest = sha256_digest(hello_bytes);
    seed_bound_file(&mut db, InodeId(2), "hello.txt", InodeId(1), &hello_digest);

    fs::write(mirror_root.join("hello.txt"), hello_bytes).expect("write current file");

    let report = observe_local_path(
        &db_path,
        &demo_namespace(),
        &mirror_root,
        temp_dir.path(),
        &mirror_root.join("hello.txt"),
        2_000,
    )
    .expect("observe local path");

    assert_eq!(report.planned_decision, "no_op");

    let db = SqliteStateDb::open(&db_path).expect("reopen db");
    let views = db
        .load_file_sync_views(&demo_namespace(), InodeId(2))
        .expect("load sync views");
    let local = views.local.expect("local row");
    assert!(!local.dirty);
    assert_eq!(local.last_local_change_ms, 1_000);
}

#[test]
fn client_local_fs_observe_subtree_path_preserves_bound_directory_move_pairing() {
    let temp_dir = TestDir::new("client-local-fs-observe-subtree");
    let db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    fs::create_dir_all(&mirror_root).expect("create mirror root");

    let mut db = SqliteStateDb::open(&db_path).expect("open db");
    seed_bound_root_directory(&mut db);
    seed_bound_directory(&mut db, InodeId(2), "docs", Some(InodeId(1)));
    seed_bound_directory(&mut db, InodeId(3), "archive", Some(InodeId(1)));
    let note_bytes = b"note v1\n";
    let note_digest = sha256_digest(note_bytes);
    seed_bound_file(&mut db, InodeId(4), "note.txt", InodeId(2), &note_digest);

    fs::create_dir_all(mirror_root.join("archive/docs")).expect("create moved docs dir");
    fs::write(mirror_root.join("archive/docs/note.txt"), note_bytes).expect("write moved note");

    let report = observe_subtree_path(
        &db_path,
        &demo_namespace(),
        &mirror_root,
        temp_dir.path(),
        &mirror_root,
        2_000,
    )
    .expect("observe subtree path");

    assert_eq!(report.relative_path, "");
    assert_eq!(report.paired_bound_move_count, 1);
    assert_eq!(report.planned_decision_counts.get("rename"), Some(&1));
}

#[test]
fn client_local_fs_observe_subtree_reobserve_after_idle_keeps_bound_files_clean() {
    let temp_dir = TestDir::new("client-local-fs-observe-subtree-noop");
    let db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    fs::create_dir_all(&mirror_root).expect("create mirror root");

    let mut db = SqliteStateDb::open(&db_path).expect("open db");
    seed_bound_root_directory(&mut db);
    let alpha_bytes = b"alpha v1\n";
    let beta_bytes = b"beta v1\n";
    let alpha_digest = sha256_digest(alpha_bytes);
    let beta_digest = sha256_digest(beta_bytes);
    seed_bound_file(&mut db, InodeId(2), "alpha.txt", InodeId(1), &alpha_digest);
    seed_bound_file(&mut db, InodeId(3), "beta.txt", InodeId(1), &beta_digest);

    fs::write(mirror_root.join("alpha.txt"), alpha_bytes).expect("write alpha file");
    fs::write(mirror_root.join("beta.txt"), beta_bytes).expect("write beta file");

    let report = observe_subtree_path(
        &db_path,
        &demo_namespace(),
        &mirror_root,
        temp_dir.path(),
        &mirror_root,
        2_000,
    )
    .expect("observe subtree path");

    assert_eq!(report.bound_observe_count, 2);
    assert_eq!(
        report.planned_decision_counts.get("upload_local_edit"),
        None
    );
    assert_eq!(report.planned_decision_counts.get("no_op"), Some(&2));

    let db = SqliteStateDb::open(&db_path).expect("reopen db");
    for inode_id in [InodeId(2), InodeId(3)] {
        let views = db
            .load_file_sync_views(&demo_namespace(), inode_id)
            .expect("load sync views");
        let local = views.local.expect("local row");
        assert!(!local.dirty);
        assert_eq!(local.last_local_change_ms, 1_000);
        assert_eq!(
            db.load_planned_action(&demo_namespace(), inode_id)
                .expect("load planned action"),
            None
        );
    }
}

#[test]
fn client_local_fs_observe_subtree_recreated_bound_directory_restores_parent_usability() {
    let temp_dir = TestDir::new("client-local-fs-observe-subtree-restored-dir");
    let db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    fs::create_dir_all(&mirror_root).expect("create mirror root");

    let mut db = SqliteStateDb::open(&db_path).expect("open db");
    seed_bound_root_directory(&mut db);
    seed_bound_directory(&mut db, InodeId(2), "docs", Some(InodeId(1)));
    db.observe_bound_inode_and_plan(
        &ObservedBoundInode {
            namespace_id: demo_namespace(),
            inode_id: InodeId(2),
            inode_kind: InodeKind::Dir,
            content_digest: None,
            parent_inode_id: Some(InodeId(1)),
            display_name: "docs".to_owned(),
            exists_on_disk: false,
            dirty: true,
            last_local_change_ms: 1_500,
        },
        1_500,
    )
    .expect("mark bound directory deleted");

    fs::create_dir_all(mirror_root.join("docs")).expect("recreate docs dir");
    fs::write(mirror_root.join("docs/note.txt"), b"note v1\n").expect("write child file");

    let report = observe_subtree_path(
        &db_path,
        &demo_namespace(),
        &mirror_root,
        temp_dir.path(),
        &mirror_root,
        2_000,
    )
    .expect("observe subtree path");

    assert_eq!(report.bound_observe_count, 1);
    assert_eq!(report.local_only_observe_count, 1);
    assert_eq!(report.planned_decision_counts.get("no_op"), Some(&1));
    assert_eq!(
        report.planned_decision_counts.get("upload_local_create"),
        Some(&1)
    );

    let db = SqliteStateDb::open(&db_path).expect("reopen db");
    let docs_views = db
        .load_file_sync_views(&demo_namespace(), InodeId(2))
        .expect("load docs views");
    let docs_local = docs_views.local.expect("restored local docs row");
    assert!(docs_local.exists_on_disk);
    assert!(!docs_local.dirty);
    assert_eq!(
        db.load_planned_action(&demo_namespace(), InodeId(2))
            .expect("load planned action"),
        None
    );
}

#[test]
fn client_local_fs_observe_subtree_replaces_bound_file_with_directory() {
    let temp_dir = TestDir::new("client-local-fs-observe-subtree-file-to-dir");
    let db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    fs::create_dir_all(&mirror_root).expect("create mirror root");

    let mut db = SqliteStateDb::open(&db_path).expect("open db");
    seed_bound_root_directory(&mut db);
    let note_digest = sha256_digest(b"note v1\n");
    seed_bound_file(&mut db, InodeId(2), "notes", InodeId(1), &note_digest);

    fs::create_dir_all(mirror_root.join("notes")).expect("create replacement dir");
    fs::write(mirror_root.join("notes/child.txt"), b"child v1\n").expect("write child file");

    let report = observe_subtree_path(
        &db_path,
        &demo_namespace(),
        &mirror_root,
        temp_dir.path(),
        &mirror_root,
        2_000,
    )
    .expect("observe subtree path");

    assert_eq!(report.bound_delete_count, 1);
    assert_eq!(report.local_only_observe_count, 2);
    assert_eq!(report.planned_decision_counts.get("delete_file"), Some(&1));
    assert_eq!(
        report.planned_decision_counts.get("create_remote_dir"),
        Some(&1)
    );
    assert_eq!(report.planned_decision_counts.get("no_op"), Some(&1));
}

#[test]
fn client_local_fs_observe_subtree_replaces_bound_directory_with_file() {
    let temp_dir = TestDir::new("client-local-fs-observe-subtree-dir-to-file");
    let db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    fs::create_dir_all(&mirror_root).expect("create mirror root");

    let mut db = SqliteStateDb::open(&db_path).expect("open db");
    seed_bound_root_directory(&mut db);
    seed_bound_directory(&mut db, InodeId(2), "notes", Some(InodeId(1)));
    let child_digest = sha256_digest(b"child v1\n");
    seed_bound_file(&mut db, InodeId(3), "child.txt", InodeId(2), &child_digest);

    fs::write(mirror_root.join("notes"), b"replacement file\n").expect("write replacement file");

    let report = observe_subtree_path(
        &db_path,
        &demo_namespace(),
        &mirror_root,
        temp_dir.path(),
        &mirror_root,
        2_000,
    )
    .expect("observe subtree path");

    assert_eq!(report.bound_delete_count, 1);
    assert_eq!(report.local_only_observe_count, 1);
    assert_eq!(
        report.planned_decision_counts.get("delete_subtree"),
        Some(&1)
    );
    assert_eq!(
        report.planned_decision_counts.get("upload_local_create"),
        Some(&1)
    );
}

#[test]
fn client_local_fs_observe_subtree_local_only_kind_change_replaces_temp_identity() {
    let temp_dir = TestDir::new("client-local-fs-observe-subtree-local-only-replacement");
    let db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    fs::create_dir_all(&mirror_root).expect("create mirror root");

    let mut db = SqliteStateDb::open(&db_path).expect("open db");
    seed_bound_root_directory(&mut db);
    let created = db
        .observe_subtree_and_plan(
            &[SubtreeObservationOp::ObserveLocalOnly {
                observed: ObservedLocalOnlySubtreeInode {
                    relative_path: "draft".to_owned(),
                    namespace_id: demo_namespace(),
                    inode_kind: InodeKind::File,
                    parent: SubtreeLocalOnlyParentRef::Bound {
                        parent_inode_id: InodeId(1),
                    },
                    display_name: "draft".to_owned(),
                    content_digest: Some("sha256:draft-v1".to_owned()),
                    exists_on_disk: true,
                    dirty: true,
                    last_local_change_ms: 1_000,
                },
            }],
            1_000,
        )
        .expect("seed local-only file");
    let original_id = match &created[0] {
        SubtreeObservationOutcome::ObservedLocalOnly { result, .. } => {
            result.local_only.client_file_id.clone()
        }
        other => panic!("unexpected local-only seed outcome: {other:?}"),
    };

    fs::create_dir_all(mirror_root.join("draft")).expect("create replacement dir");
    fs::write(mirror_root.join("draft/note.txt"), b"note v1\n").expect("write child file");

    let report = observe_subtree_path(
        &db_path,
        &demo_namespace(),
        &mirror_root,
        temp_dir.path(),
        &mirror_root,
        2_000,
    )
    .expect("observe subtree path");

    assert_eq!(report.local_only_delete_count, 1);
    assert_eq!(report.local_only_observe_count, 2);
    assert_eq!(
        report.planned_decision_counts.get("create_remote_dir"),
        Some(&1)
    );
    assert_eq!(report.planned_decision_counts.get("no_op"), Some(&2));

    let db = SqliteStateDb::open(&db_path).expect("reopen db");
    assert_eq!(
        db.load_local_only_file(&original_id)
            .expect("load original local-only file"),
        None
    );
}

#[cfg(unix)]
#[test]
fn client_local_fs_observe_subtree_skips_untracked_symlink_and_processes_siblings() {
    let temp_dir = TestDir::new("client-local-fs-observe-subtree-skipped-symlink");
    let db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    fs::create_dir_all(&mirror_root).expect("create mirror root");

    let mut db = SqliteStateDb::open(&db_path).expect("open db");
    seed_bound_root_directory(&mut db);

    fs::write(mirror_root.join("alpha.txt"), b"alpha v1\n").expect("write regular sibling");
    let symlink_target = temp_dir.path().join("outside-target.txt");
    fs::write(&symlink_target, b"target\n").expect("write symlink target");
    symlink(&symlink_target, mirror_root.join("link.txt")).expect("create symlink");

    let report = observe_subtree_path(
        &db_path,
        &demo_namespace(),
        &mirror_root,
        temp_dir.path(),
        &mirror_root,
        2_000,
    )
    .expect("observe subtree path");

    assert_eq!(report.skipped_unsupported_count, 1);
    assert_eq!(
        report.skipped_unsupported_paths,
        vec!["link.txt".to_owned()]
    );
    assert_eq!(report.local_only_observe_count, 1);
    assert_eq!(
        report.planned_decision_counts.get("upload_local_create"),
        Some(&1)
    );
}

#[cfg(unix)]
#[test]
fn client_local_fs_observe_subtree_skipped_tracked_symlink_root_suppresses_delete_inference() {
    let temp_dir = TestDir::new("client-local-fs-observe-subtree-tracked-symlink");
    let db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    fs::create_dir_all(&mirror_root).expect("create mirror root");

    let mut db = SqliteStateDb::open(&db_path).expect("open db");
    seed_bound_root_directory(&mut db);
    let link_digest = sha256_digest(b"link v1\n");
    seed_bound_file(&mut db, InodeId(2), "link.txt", InodeId(1), &link_digest);

    fs::write(mirror_root.join("fresh.txt"), b"fresh v1\n").expect("write regular sibling");
    let symlink_target = temp_dir.path().join("outside-target.txt");
    fs::write(&symlink_target, b"target\n").expect("write symlink target");
    symlink(&symlink_target, mirror_root.join("link.txt")).expect("create symlink");

    let report = observe_subtree_path(
        &db_path,
        &demo_namespace(),
        &mirror_root,
        temp_dir.path(),
        &mirror_root,
        2_000,
    )
    .expect("observe subtree path");

    assert_eq!(report.skipped_unsupported_count, 1);
    assert_eq!(
        report.skipped_unsupported_paths,
        vec!["link.txt".to_owned()]
    );
    assert_eq!(report.bound_delete_count, 0);
    assert_eq!(report.local_only_observe_count, 1);

    let db = SqliteStateDb::open(&db_path).expect("reopen db");
    let link_views = db
        .load_file_sync_views(&demo_namespace(), InodeId(2))
        .expect("load link views");
    let link_local = link_views.local.expect("tracked local file should remain");
    assert!(link_local.exists_on_disk);
    assert!(!link_local.dirty);
    assert_eq!(
        db.load_planned_action(&demo_namespace(), InodeId(2))
            .expect("load planned action"),
        None
    );
}

fn run_reducer_fixture(relative_path: &str) {
    let fixture: ReducerFixture = load_fixture(relative_path);
    let batch = NormalizedFsEventBatch {
        events: fixture.events,
    };
    match reduce_fs_event_batch(&batch) {
        Ok(intents) => {
            let expected = fixture.expect.intents.expect("fixture intents");
            assert_eq!(intents, expected, "fixture mismatch for {relative_path}");
        }
        Err(error) => {
            let expected_reason = fixture
                .expect
                .error_reason_code
                .expect("fixture error reason");
            assert_eq!(error.reason_code(), expected_reason);
            if let Some(expected_paths) = fixture.expect.affected_relative_paths {
                let actual_paths = match &error {
                    loon_client::local_fs::FsEventReductionError::InvalidRelativePath {
                        affected_relative_paths,
                        ..
                    }
                    | loon_client::local_fs::FsEventReductionError::AmbiguousRenameSource {
                        affected_relative_paths,
                        ..
                    }
                    | loon_client::local_fs::FsEventReductionError::AmbiguousRenameTarget {
                        affected_relative_paths,
                        ..
                    }
                    | loon_client::local_fs::FsEventReductionError::AmbiguousNativeObjectId {
                        affected_relative_paths,
                        ..
                    }
                    | loon_client::local_fs::FsEventReductionError::ContradictoryPathEvents {
                        affected_relative_paths,
                        ..
                    }
                    | loon_client::local_fs::FsEventReductionError::InvalidCrossKindRenameHint {
                        affected_relative_paths,
                        ..
                    } => affected_relative_paths.clone(),
                };
                assert_eq!(
                    actual_paths, expected_paths,
                    "fixture mismatch for {relative_path}"
                );
            }
        }
    }
}

fn seed_bound_root_directory(db: &mut SqliteStateDb) {
    db.planner_transaction("seed-bound-root-directory", |tx| {
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: demo_namespace(),
            inode_id: InodeId(1),
            inode_kind: InodeKind::Dir,
            observed_seq: ChangeSeq(1),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: None,
            display_name: String::new(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: demo_namespace(),
            inode_id: InodeId(1),
            inode_kind: InodeKind::Dir,
            content_digest: None,
            parent_inode_id: None,
            display_name: String::new(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1_000,
        })?;
        tx.upsert_sync_anchor(&SyncAnchorRow {
            namespace_id: demo_namespace(),
            inode_id: InodeId(1),
            inode_kind: InodeKind::Dir,
            synced_seq: ChangeSeq(1),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: None,
            display_name: String::new(),
        })?;
        Ok(())
    })
    .expect("seed bound root");
}

fn seed_bound_file(
    db: &mut SqliteStateDb,
    inode_id: InodeId,
    display_name: &str,
    parent_inode_id: InodeId,
    content_digest: &str,
) {
    db.planner_transaction("seed-bound-file", |tx| {
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: demo_namespace(),
            inode_id,
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(1),
            revision_no: RevisionNo(1),
            content_digest: Some(content_digest.to_owned()),
            content_manifest_digest: Some(format!("manifest:{content_digest}")),
            parent_inode_id: Some(parent_inode_id),
            display_name: display_name.to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: demo_namespace(),
            inode_id,
            inode_kind: InodeKind::File,
            content_digest: Some(content_digest.to_owned()),
            parent_inode_id: Some(parent_inode_id),
            display_name: display_name.to_owned(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1_000,
        })?;
        tx.upsert_sync_anchor(&SyncAnchorRow {
            namespace_id: demo_namespace(),
            inode_id,
            inode_kind: InodeKind::File,
            synced_seq: ChangeSeq(1),
            revision_no: RevisionNo(1),
            content_digest: Some(content_digest.to_owned()),
            content_manifest_digest: Some(format!("manifest:{content_digest}")),
            parent_inode_id: Some(parent_inode_id),
            display_name: display_name.to_owned(),
        })?;
        Ok(())
    })
    .expect("seed bound file");
}

fn seed_bound_directory(
    db: &mut SqliteStateDb,
    inode_id: InodeId,
    display_name: &str,
    parent_inode_id: Option<InodeId>,
) {
    db.planner_transaction("seed-bound-directory", |tx| {
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: demo_namespace(),
            inode_id,
            inode_kind: InodeKind::Dir,
            observed_seq: ChangeSeq(1),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id,
            display_name: display_name.to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: demo_namespace(),
            inode_id,
            inode_kind: InodeKind::Dir,
            content_digest: None,
            parent_inode_id,
            display_name: display_name.to_owned(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1_000,
        })?;
        tx.upsert_sync_anchor(&SyncAnchorRow {
            namespace_id: demo_namespace(),
            inode_id,
            inode_kind: InodeKind::Dir,
            synced_seq: ChangeSeq(1),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id,
            display_name: display_name.to_owned(),
        })?;
        Ok(())
    })
    .expect("seed bound directory");
}

fn demo_namespace() -> NamespaceId {
    NamespaceId::from("demo")
}

fn load_fixture<T>(relative_path: &str) -> T
where
    T: for<'de> Deserialize<'de>,
{
    let path = loon_testkit::fixtures::fixture_path(relative_path);
    let text = fs::read_to_string(&path).expect("read local fs fixture");
    serde_yaml::from_str(&text).expect("decode local fs fixture")
}

#[derive(Debug, Deserialize)]
struct ReducerFixture {
    events: Vec<NormalizedFsEvent>,
    expect: ReducerExpectation,
}

#[derive(Debug, Deserialize)]
struct ReducerExpectation {
    #[serde(default)]
    intents: Option<Vec<ReducedLocalObservationIntent>>,
    #[serde(default)]
    error_reason_code: Option<String>,
    #[serde(default)]
    affected_relative_paths: Option<Vec<String>>,
}
