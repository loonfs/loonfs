use loon_client::local_fs::{
    observe_local_path, observe_subtree_path, reduce_fs_event_batch, NormalizedFsEvent,
    NormalizedFsEventBatch, ReducedLocalObservationIntent,
};
use loon_client::state_db::{LocalFileStateRow, RemoteFileStateRow, SqliteStateDb, SyncAnchorRow};
use loon_testkit::tempdir::TestDir;
use loon_types::{sha256_digest, ChangeSeq, InodeId, InodeKind, NamespaceId, RevisionNo};
use serde::Deserialize;
use std::fs;

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
