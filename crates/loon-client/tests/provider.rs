use loon_client::planner::{PlannerDecision, PlannerReason};
use loon_client::provider::{
    materialize_inode_to_mirror_root, materialize_local_only_to_mirror_root,
    ProviderTargetedMaterializeError,
};
use loon_client::state_db::{
    ClientFileId, LocalFileStateRow, LocalOnlyFileStateRow, LocalOnlyPlannedActionRow,
    RemoteFileStateRow, SqliteStateDb, SyncAnchorRow,
};
use loon_client::upload::upload_small_file_from_path;
use loon_objectstore::fs::LocalFsStore;
use loon_testkit::tempdir::TestDir;
use loon_types::{ChangeSeq, InodeId, InodeKind, NamespaceId, RevisionNo};
use std::fs;

#[test]
fn provider_materializes_remote_only_directory_to_mirror_root() {
    let temp_dir = TestDir::new("provider-materialize-remote-dir");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let mirror_root = temp_dir.path().join("mirror");
    fs::create_dir_all(&store_root).expect("create store root");
    let store = LocalFsStore::new(&store_root).expect("open local fs store");
    let namespace_id = NamespaceId::from("provider-ns");

    seed_remote_root_and_directory(&db_path, &namespace_id, InodeId(7), "docs");

    let mut db = SqliteStateDb::open(&db_path).expect("open client db");
    let materialized = materialize_inode_to_mirror_root(
        &mut db,
        &store,
        &namespace_id,
        InodeId(7),
        &mirror_root,
        1_700_000_000_000,
    )
    .expect("materialize remote-only directory");

    assert_eq!(materialized.relative_path, "docs");
    assert!(
        materialized.absolute_path.is_dir(),
        "materialized directory should exist"
    );

    let root_views = db
        .load_file_sync_views(&namespace_id, InodeId(1))
        .expect("load root views");
    let dir_views = db
        .load_file_sync_views(&namespace_id, InodeId(7))
        .expect("load dir views");
    assert!(
        root_views.local.expect("root local row").exists_on_disk,
        "root should be materialized before child directory"
    );
    assert!(
        dir_views.local.expect("dir local row").exists_on_disk,
        "directory should be materialized on disk"
    );
}

#[test]
fn provider_materializes_nested_remote_only_file_with_ancestors_first() {
    let temp_dir = TestDir::new("provider-materialize-remote-file");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let mirror_root = temp_dir.path().join("mirror");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create store root");
    fs::create_dir_all(&source_root).expect("create source root");
    let store = LocalFsStore::new(&store_root).expect("open local fs store");
    let namespace_id = NamespaceId::from("provider-ns");

    let source_path = source_root.join("report.txt");
    fs::write(&source_path, "hello from the provider spike\n").expect("write source file");
    let uploaded = upload_small_file_from_path(&store, &namespace_id, &source_path)
        .expect("upload source file to store");

    seed_remote_root_directory_and_file(
        &db_path,
        &namespace_id,
        InodeId(7),
        "docs",
        InodeId(8),
        "report.txt",
        &uploaded.file_digest_sha256,
        &uploaded.content_manifest_digest,
    );

    let mut db = SqliteStateDb::open(&db_path).expect("open client db");
    let materialized = materialize_inode_to_mirror_root(
        &mut db,
        &store,
        &namespace_id,
        InodeId(8),
        &mirror_root,
        1_700_000_000_000,
    )
    .expect("materialize remote-only file");

    assert_eq!(materialized.relative_path, "docs/report.txt");
    assert_eq!(
        fs::read_to_string(&materialized.absolute_path).expect("read hydrated file"),
        "hello from the provider spike\n"
    );
    assert!(
        mirror_root.join("docs").is_dir(),
        "ancestor directory should be materialized first"
    );

    let root_views = db
        .load_file_sync_views(&namespace_id, InodeId(1))
        .expect("load root views");
    let dir_views = db
        .load_file_sync_views(&namespace_id, InodeId(7))
        .expect("load ancestor dir views");
    let file_views = db
        .load_file_sync_views(&namespace_id, InodeId(8))
        .expect("load file views");
    assert!(root_views.local.expect("root local row").exists_on_disk);
    assert!(dir_views.local.expect("ancestor local row").exists_on_disk);
    assert!(file_views.local.expect("file local row").exists_on_disk);
}

#[test]
fn provider_local_only_materialization_fails_closed_for_blocked_item() {
    let temp_dir = TestDir::new("provider-local-only-blocked");
    let db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    fs::create_dir_all(&mirror_root).expect("create mirror root");
    let namespace_id = NamespaceId::from("provider-ns");
    let client_file_id = ClientFileId::from("tmp:provider-ns:00000000000000000001");

    seed_blocked_local_only_file(&db_path, &namespace_id, &client_file_id, &mirror_root);

    let mut db = SqliteStateDb::open(&db_path).expect("open client db");
    let error = materialize_local_only_to_mirror_root(&mut db, &client_file_id, &mirror_root)
        .expect_err("blocked local-only item should fail closed");

    match error {
        ProviderTargetedMaterializeError::LocalOnlyUnavailable {
            client_file_id: actual_client_file_id,
            reason,
        } => {
            assert_eq!(actual_client_file_id, client_file_id.as_str());
            assert_eq!(reason, PlannerDecision::WaitForExactPathVacate.as_str());
        }
        other => panic!("expected LocalOnlyUnavailable error, got {other:?}"),
    }
}

#[test]
fn provider_remote_inode_materialization_fails_closed_for_non_materialization_work() {
    let temp_dir = TestDir::new("provider-remote-blocked");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let mirror_root = temp_dir.path().join("mirror");
    fs::create_dir_all(&store_root).expect("create store root");
    let store = LocalFsStore::new(&store_root).expect("open local fs store");
    let namespace_id = NamespaceId::from("provider-ns");

    seed_remote_rename_case(&db_path, &namespace_id);

    let mut db = SqliteStateDb::open(&db_path).expect("open client db");
    let error = materialize_inode_to_mirror_root(
        &mut db,
        &store,
        &namespace_id,
        InodeId(9),
        &mirror_root,
        1_700_000_000_000,
    )
    .expect_err("rename-backed placeholder should fail closed");

    match error {
        ProviderTargetedMaterializeError::InodeUnavailable {
            namespace_id: actual_namespace_id,
            inode_id,
            decision,
            ..
        } => {
            assert_eq!(actual_namespace_id, namespace_id.as_str());
            assert_eq!(inode_id, 9);
            assert_eq!(
                decision,
                PlannerDecision::ResolveRenameVsEditConflict.as_str()
            );
        }
        other => panic!("expected InodeUnavailable error, got {other:?}"),
    }
}

fn seed_remote_root_and_directory(
    db_path: &std::path::Path,
    namespace_id: &NamespaceId,
    directory_inode_id: InodeId,
    directory_name: &str,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open client db");
    db.planner_transaction("seed-provider-remote-dir", |tx| {
        tx.upsert_remote_file(&remote_root(namespace_id))?;
        tx.upsert_local_file(&local_placeholder(
            namespace_id,
            InodeId(1),
            InodeKind::Dir,
            None,
            "",
        ))?;
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: directory_inode_id,
            inode_kind: InodeKind::Dir,
            observed_seq: ChangeSeq(2),
            revision_no: RevisionNo(0),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: Some(InodeId(1)),
            display_name: directory_name.to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&local_placeholder(
            namespace_id,
            directory_inode_id,
            InodeKind::Dir,
            Some(InodeId(1)),
            directory_name,
        ))?;
        Ok(())
    })
    .expect("seed remote dir state");
}

#[allow(clippy::too_many_arguments)]
fn seed_remote_root_directory_and_file(
    db_path: &std::path::Path,
    namespace_id: &NamespaceId,
    directory_inode_id: InodeId,
    directory_name: &str,
    file_inode_id: InodeId,
    file_name: &str,
    content_digest: &str,
    content_manifest_digest: &str,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open client db");
    db.planner_transaction("seed-provider-remote-file", |tx| {
        tx.upsert_remote_file(&remote_root(namespace_id))?;
        tx.upsert_local_file(&local_placeholder(
            namespace_id,
            InodeId(1),
            InodeKind::Dir,
            None,
            "",
        ))?;
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: directory_inode_id,
            inode_kind: InodeKind::Dir,
            observed_seq: ChangeSeq(2),
            revision_no: RevisionNo(0),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: Some(InodeId(1)),
            display_name: directory_name.to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&local_placeholder(
            namespace_id,
            directory_inode_id,
            InodeKind::Dir,
            Some(InodeId(1)),
            directory_name,
        ))?;
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: file_inode_id,
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(3),
            revision_no: RevisionNo(1),
            content_digest: Some(content_digest.to_owned()),
            content_manifest_digest: Some(content_manifest_digest.to_owned()),
            parent_inode_id: Some(directory_inode_id),
            display_name: file_name.to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&local_placeholder(
            namespace_id,
            file_inode_id,
            InodeKind::File,
            Some(directory_inode_id),
            file_name,
        ))?;
        Ok(())
    })
    .expect("seed remote file state");
}

fn seed_blocked_local_only_file(
    db_path: &std::path::Path,
    namespace_id: &NamespaceId,
    client_file_id: &ClientFileId,
    mirror_root: &std::path::Path,
) {
    fs::write(mirror_root.join("draft.txt"), "draft\n").expect("write draft file");
    let mut db = SqliteStateDb::open(db_path).expect("open client db");
    db.planner_transaction("seed-provider-local-only", |tx| {
        tx.upsert_remote_file(&remote_root(namespace_id))?;
        tx.upsert_local_file(&local_placeholder(
            namespace_id,
            InodeId(1),
            InodeKind::Dir,
            None,
            "",
        ))?;
        tx.upsert_local_only_file(&LocalOnlyFileStateRow {
            client_file_id: client_file_id.clone(),
            namespace_id: namespace_id.clone(),
            inode_kind: InodeKind::File,
            parent_inode_id: Some(InodeId(1)),
            display_name: "draft.txt".to_owned(),
            content_digest: Some("sha256:draft".to_owned()),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 1_700_000_000_000,
        })?;
        tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
            client_file_id: client_file_id.clone(),
            namespace_id: namespace_id.clone(),
            decision: PlannerDecision::WaitForExactPathVacate.as_str().to_owned(),
            reason: PlannerReason::ExactPathBlockedByBoundOccupant
                .as_str()
                .to_owned(),
            created_at_ms: 1_700_000_000_000,
        })?;
        Ok(())
    })
    .expect("seed local-only state");
}

fn seed_remote_rename_case(db_path: &std::path::Path, namespace_id: &NamespaceId) {
    let mut db = SqliteStateDb::open(db_path).expect("open client db");
    db.planner_transaction("seed-provider-remote-rename", |tx| {
        tx.upsert_remote_file(&remote_root(namespace_id))?;
        tx.upsert_local_file(&local_placeholder(
            namespace_id,
            InodeId(1),
            InodeKind::Dir,
            None,
            "",
        ))?;
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(9),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(4),
            revision_no: RevisionNo(2),
            content_digest: Some("sha256:rename".to_owned()),
            content_manifest_digest: Some("sha256:manifest-rename".to_owned()),
            parent_inode_id: Some(InodeId(1)),
            display_name: "new-name.txt".to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&local_placeholder(
            namespace_id,
            InodeId(9),
            InodeKind::File,
            Some(InodeId(1)),
            "new-name.txt",
        ))?;
        tx.upsert_sync_anchor(&SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(9),
            inode_kind: InodeKind::File,
            synced_seq: ChangeSeq(3),
            revision_no: RevisionNo(2),
            content_digest: Some("sha256:rename".to_owned()),
            content_manifest_digest: Some("sha256:manifest-rename".to_owned()),
            parent_inode_id: Some(InodeId(1)),
            display_name: "old-name.txt".to_owned(),
        })?;
        Ok(())
    })
    .expect("seed remote rename state");
}

fn remote_root(namespace_id: &NamespaceId) -> RemoteFileStateRow {
    RemoteFileStateRow {
        namespace_id: namespace_id.clone(),
        inode_id: InodeId(1),
        inode_kind: InodeKind::Dir,
        observed_seq: ChangeSeq(0),
        revision_no: RevisionNo(0),
        content_digest: None,
        content_manifest_digest: None,
        parent_inode_id: None,
        display_name: String::new(),
        is_deleted: false,
    }
}

fn local_placeholder(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    inode_kind: InodeKind,
    parent_inode_id: Option<InodeId>,
    display_name: &str,
) -> LocalFileStateRow {
    LocalFileStateRow {
        namespace_id: namespace_id.clone(),
        inode_id,
        inode_kind,
        content_digest: None,
        parent_inode_id,
        display_name: display_name.to_owned(),
        exists_on_disk: false,
        dirty: false,
        last_local_change_ms: 0,
    }
}
