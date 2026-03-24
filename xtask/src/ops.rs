use anyhow::Result;

pub fn run(args: impl IntoIterator<Item = String>) -> Result<String> {
    loon_ops::run_args(args)
}

#[cfg(test)]
mod tests {
    use super::run;
    use loon_client::state_db::{
        LocalFileStateRow, RemoteFileStateRow, SqliteStateDb, SyncAnchorRow,
    };
    use loon_types::{sha256_digest, ChangeSeq, InodeId, InodeKind, NamespaceId, RevisionNo};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn forwards_bootstrap_namespace_to_loon_ops() {
        let temp_dir = unique_temp_dir("xtask-ops-bootstrap");
        let config_path = write_local_fs_config(&temp_dir);

        let rendered = run([
            "bootstrap-namespace".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run xtask ops bootstrap");

        assert_eq!(
            rendered,
            include_str!(
                "../../tests/snapshots/ops-bootstrap-namespace/ops_bootstrap_namespace.txt"
            )
        );
    }

    #[test]
    fn forwards_show_client_state_to_loon_ops() {
        let temp_dir = unique_temp_dir("xtask-ops-show-client");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let _db = SqliteStateDb::open(&db_path).expect("open client db");

        let rendered = run([
            "show-client-state".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run xtask ops show-client-state");

        assert_eq!(
            rendered,
            include_str!("../../tests/snapshots/ops-show-client-state/ops_show_client_state.txt")
        );
    }

    #[test]
    fn forwards_import_remote_observations_to_loon_ops() {
        let temp_dir = unique_temp_dir("xtask-ops-import");
        let config_path = write_local_fs_config(&temp_dir);

        run([
            "bootstrap-namespace".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("bootstrap namespace");

        let rendered = run([
            "import-remote-observations".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run xtask ops import-remote-observations");

        assert_eq!(
            rendered,
            include_str!(
                "../../tests/snapshots/ops-import-remote-observations/ops_import_remote_observations.txt"
            )
        );
    }

    #[test]
    fn forwards_observe_local_to_loon_ops() {
        let temp_dir = unique_temp_dir("xtask-ops-observe-local");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        fs::write(temp_dir.join("mirror/draft.txt"), b"draft local file\n")
            .expect("write local-only file bytes");

        let rendered = run([
            "observe-local".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror/draft.txt").display().to_string(),
        ])
        .expect("run xtask ops observe-local");

        assert_eq!(
            rendered,
            include_str!(
                "../../tests/snapshots/ops-observe-local/ops_observe_local_local_only_file.txt"
            )
        );
    }

    #[test]
    fn forwards_observe_delete_to_loon_ops() {
        let temp_dir = unique_temp_dir("xtask-ops-observe-delete");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "hello.txt",
            "sha256:remote-hello-v1",
            "sha256:manifest-remote-hello-v1",
        );

        let rendered = run([
            "observe-delete".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror/hello.txt").display().to_string(),
        ])
        .expect("run xtask ops observe-delete");

        assert_eq!(
            rendered,
            include_str!(
                "../../tests/snapshots/ops-observe-delete/ops_observe_delete_bound_file.txt"
            )
        );
    }

    #[test]
    fn forwards_observe_move_to_loon_ops() {
        let temp_dir = unique_temp_dir("xtask-ops-observe-move");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        db.planner_transaction("seed-archive-parent", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: NamespaceId::from("demo"),
                inode_id: InodeId(9),
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(1)),
                display_name: "archive".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: NamespaceId::from("demo"),
                inode_id: InodeId(9),
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: Some(InodeId(1)),
                display_name: "archive".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: NamespaceId::from("demo"),
                inode_id: InodeId(9),
                inode_kind: InodeKind::Dir,
                synced_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(1)),
                display_name: "archive".to_owned(),
            })?;
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: NamespaceId::from("demo"),
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: Some("sha256:remote-hello-v1".to_owned()),
                content_manifest_digest: Some("sha256:manifest-remote-hello-v1".to_owned()),
                parent_inode_id: Some(InodeId(1)),
                display_name: "hello.txt".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: NamespaceId::from("demo"),
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
                content_digest: Some("sha256:remote-hello-v1".to_owned()),
                parent_inode_id: Some(InodeId(1)),
                display_name: "hello.txt".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: NamespaceId::from("demo"),
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: Some("sha256:remote-hello-v1".to_owned()),
                content_manifest_digest: Some("sha256:manifest-remote-hello-v1".to_owned()),
                parent_inode_id: Some(InodeId(1)),
                display_name: "hello.txt".to_owned(),
            })?;
            Ok(())
        })
        .expect("seed bound move state");
        fs::create_dir_all(temp_dir.join("mirror/archive")).expect("create archive dir");
        fs::write(temp_dir.join("mirror/archive/hello.txt"), b"hello v1\n")
            .expect("write moved file");

        let rendered = run([
            "observe-move".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--from".to_owned(),
            temp_dir.join("mirror/hello.txt").display().to_string(),
            "--to".to_owned(),
            temp_dir
                .join("mirror/archive/hello.txt")
                .display()
                .to_string(),
        ])
        .expect("run xtask ops observe-move");

        assert_eq!(
            rendered,
            include_str!("../../tests/snapshots/ops-observe-move/ops_observe_move_bound_file.txt")
        );
    }

    #[test]
    fn forwards_observe_subtree_to_loon_ops() {
        let temp_dir = unique_temp_dir("xtask-ops-observe-subtree");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "hello.txt",
            "sha256:remote-hello-v1",
            "sha256:manifest-remote-hello-v1",
        );
        fs::create_dir_all(temp_dir.join("mirror/drafts")).expect("create local-only dir");
        fs::write(
            temp_dir.join("mirror/hello.txt"),
            b"hello from subtree edit\n",
        )
        .expect("write bound file edit");
        fs::write(temp_dir.join("mirror/drafts/note.txt"), b"draft note\n")
            .expect("write nested local-only file");

        let rendered = run([
            "observe-subtree".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror").display().to_string(),
        ])
        .expect("run xtask ops observe-subtree");

        assert_eq!(
            rendered,
            include_str!(
                "../../tests/snapshots/ops-observe-subtree/ops_observe_subtree_bound_edit_and_nested_local_only_create.txt"
            )
        );
    }

    #[test]
    fn forwards_observe_subtree_inferred_bound_move_to_loon_ops() {
        let temp_dir = unique_temp_dir("xtask-ops-observe-subtree-bound-move");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        let file_digest = sha256_digest(b"hello v1\n");
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "hello.txt",
            &file_digest,
            &format!("manifest:{file_digest}"),
        );
        fs::write(temp_dir.join("mirror/renamed.txt"), b"hello v1\n")
            .expect("write moved bound file");

        let rendered = run([
            "observe-subtree".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror").display().to_string(),
        ])
        .expect("run xtask ops observe-subtree");

        assert_eq!(
            rendered,
            include_str!(
                "../../tests/snapshots/ops-observe-subtree/ops_observe_subtree_inferred_bound_file_move.txt"
            )
        );
    }

    #[test]
    fn forwards_sync_once_to_loon_ops() {
        let temp_dir = unique_temp_dir("xtask-ops-sync-once");
        let config_path = write_local_fs_config(&temp_dir);
        let _db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client db");

        let rendered = run([
            "sync-once".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run xtask ops sync-once");

        assert_eq!(
            rendered,
            include_str!("../../tests/snapshots/ops-sync-once/ops_sync_once_no_work.txt")
        );
    }

    #[test]
    fn forwards_sync_until_idle_to_loon_ops() {
        let temp_dir = unique_temp_dir("xtask-ops-sync-until-idle");
        let config_path = write_local_fs_config(&temp_dir);
        let _db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client db");

        let rendered = run([
            "sync-until-idle".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run xtask ops sync-until-idle");

        assert_eq!(
            rendered,
            include_str!(
                "../../tests/snapshots/ops-sync-until-idle/ops_sync_until_idle_no_work.txt"
            )
        );
    }

    fn write_local_fs_config(temp_dir: &Path) -> PathBuf {
        let config_path = temp_dir.join("loondb-demo.toml");
        fs::write(
            &config_path,
            format!(
                r#"[object_store]
kind = "local-fs"
root = "{}"
key_prefix = "tenant-a"

[client]
state_db_path = "{}"
mirror_root = "{}"

[server]
writer_id = "writer-a"
writer_version = "xtask-ops-test"
lease_duration_ms = 60000

[ops]
now_ms = 1000
"#,
                temp_dir.join("store").display(),
                temp_dir.join("client.sqlite3").display(),
                temp_dir.join("mirror").display(),
            ),
        )
        .expect("write config");
        fs::create_dir_all(temp_dir.join("mirror")).expect("create mirror root");
        config_path
    }

    fn seed_bound_root_directory(db: &mut SqliteStateDb, namespace_id: NamespaceId) {
        db.planner_transaction("seed-bound-root-directory", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
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
                namespace_id: namespace_id.clone(),
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
                namespace_id,
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
        .expect("seed bound root directory");
    }

    fn seed_bound_file(
        db: &mut SqliteStateDb,
        namespace_id: NamespaceId,
        inode_id: InodeId,
        display_name: &str,
        content_digest: &str,
        content_manifest_digest: &str,
    ) {
        db.planner_transaction("seed-bound-file", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id,
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: Some(content_digest.to_owned()),
                content_manifest_digest: Some(content_manifest_digest.to_owned()),
                parent_inode_id: Some(InodeId(1)),
                display_name: display_name.to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id,
                inode_kind: InodeKind::File,
                content_digest: Some(content_digest.to_owned()),
                parent_inode_id: Some(InodeId(1)),
                display_name: display_name.to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id,
                inode_id,
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: Some(content_digest.to_owned()),
                content_manifest_digest: Some(content_manifest_digest.to_owned()),
                parent_inode_id: Some(InodeId(1)),
                display_name: display_name.to_owned(),
            })?;
            Ok(())
        })
        .expect("seed bound file");
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("loondb-{label}-{stamp}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }
}
