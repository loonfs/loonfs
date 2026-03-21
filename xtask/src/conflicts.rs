use anyhow::{anyhow, bail, Result};
use loon_client::state_db::{ConflictArtifactRow, SqliteStateDb};
use loon_client::{
    discover_conflict_artifacts_for_namespace, load_or_discover_conflict_artifact,
    restore_conflict_artifact_to_path, RestoredConflictArtifact,
};
use loon_objectstore::fs::LocalFsStore;
use loon_testkit::render::render_yaml;
use loon_types::NamespaceId;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictListArgs {
    pub namespace_id: NamespaceId,
    pub db_path: PathBuf,
    pub store_root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictShowArgs {
    pub namespace_id: NamespaceId,
    pub conflict_id: String,
    pub db_path: PathBuf,
    pub store_root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictRestoreArgs {
    pub namespace_id: NamespaceId,
    pub conflict_id: String,
    pub db_path: PathBuf,
    pub store_root: PathBuf,
    pub destination_path: PathBuf,
}

struct ConflictCommandContext {
    db: SqliteStateDb,
    store: LocalFsStore,
}

pub fn parse_conflict_list_args(
    mut args: impl Iterator<Item = String>,
) -> Result<ConflictListArgs> {
    let namespace_id = args.next().map(NamespaceId::from).ok_or_else(|| {
        anyhow!("usage: conflict-list <namespace_id> --db <path> --store-root <path>")
    })?;
    let (db_path, store_root, destination_path) = parse_conflict_flags(
        args,
        "usage: conflict-list <namespace_id> --db <path> --store-root <path>",
        false,
    )?;
    if destination_path.is_some() {
        bail!("unexpected --to argument for conflict-list");
    }
    Ok(ConflictListArgs {
        namespace_id,
        db_path,
        store_root,
    })
}

pub fn parse_conflict_show_args(
    mut args: impl Iterator<Item = String>,
) -> Result<ConflictShowArgs> {
    let namespace_id = args.next().map(NamespaceId::from).ok_or_else(|| {
        anyhow!("usage: conflict-show <namespace_id> <conflict_id> --db <path> --store-root <path>")
    })?;
    let conflict_id = args.next().ok_or_else(|| {
        anyhow!("usage: conflict-show <namespace_id> <conflict_id> --db <path> --store-root <path>")
    })?;
    let (db_path, store_root, destination_path) = parse_conflict_flags(
        args,
        "usage: conflict-show <namespace_id> <conflict_id> --db <path> --store-root <path>",
        false,
    )?;
    if destination_path.is_some() {
        bail!("unexpected --to argument for conflict-show");
    }
    Ok(ConflictShowArgs {
        namespace_id,
        conflict_id,
        db_path,
        store_root,
    })
}

pub fn parse_conflict_restore_args(
    mut args: impl Iterator<Item = String>,
) -> Result<ConflictRestoreArgs> {
    let namespace_id = args
        .next()
        .map(NamespaceId::from)
        .ok_or_else(|| anyhow!("usage: conflict-restore <namespace_id> <conflict_id> --db <path> --store-root <path> --to <path>"))?;
    let conflict_id = args.next().ok_or_else(|| {
        anyhow!(
            "usage: conflict-restore <namespace_id> <conflict_id> --db <path> --store-root <path> --to <path>"
        )
    })?;
    let (db_path, store_root, destination_path) = parse_conflict_flags(
        args,
        "usage: conflict-restore <namespace_id> <conflict_id> --db <path> --store-root <path> --to <path>",
        true,
    )?;
    Ok(ConflictRestoreArgs {
        namespace_id,
        conflict_id,
        db_path,
        store_root,
        destination_path: destination_path.expect("conflict-restore requires destination"),
    })
}

pub fn run_conflict_list(args: ConflictListArgs) -> Result<String> {
    let mut context = open_conflict_command_context(&args.db_path, &args.store_root)?;
    let discovered_rows = discover_conflict_artifacts_for_namespace(
        &mut context.db,
        &context.store,
        &args.namespace_id,
    )?;
    let cached_rows = context
        .db
        .load_conflict_artifacts_for_namespace(&args.namespace_id)?;

    let mut rendered = String::new();
    let _ = writeln!(
        &mut rendered,
        "namespace={} discovered_count={} cached_count_after={}",
        args.namespace_id.as_str(),
        discovered_rows.len(),
        cached_rows.len()
    );
    for (index, row) in cached_rows.iter().enumerate() {
        let _ = writeln!(
            &mut rendered,
            "index={} conflict_id={} artifact_kind={} conflict_class={} created_at_ms={} object_key={}",
            index + 1,
            row.conflict_id,
            row.artifact_kind.as_str(),
            row.conflict_class.as_str(),
            row.created_at_ms,
            row.object_key
        );
    }
    Ok(rendered)
}

pub fn run_conflict_show(args: ConflictShowArgs) -> Result<String> {
    let mut context = open_conflict_command_context(&args.db_path, &args.store_root)?;
    let Some(row) = load_or_discover_conflict_artifact(
        &mut context.db,
        &context.store,
        &args.namespace_id,
        &args.conflict_id,
    )?
    else {
        bail!(
            "conflict artifact not found: namespace=`{}` conflict_id=`{}`",
            args.namespace_id.as_str(),
            args.conflict_id
        );
    };

    let yaml = render_conflict_artifact_yaml(&row)?;
    let mut rendered = String::new();
    let _ = writeln!(
        &mut rendered,
        "namespace={} conflict_id={} artifact_kind={} conflict_class={} created_at_ms={} object_key={}",
        row.namespace_id.as_str(),
        row.conflict_id,
        row.artifact_kind.as_str(),
        row.conflict_class.as_str(),
        row.created_at_ms,
        row.object_key
    );
    let _ = writeln!(&mut rendered, "[artifact]");
    rendered.push_str(&yaml);
    Ok(rendered)
}

pub fn run_conflict_restore(args: ConflictRestoreArgs) -> Result<String> {
    let mut context = open_conflict_command_context(&args.db_path, &args.store_root)?;
    let restored = restore_conflict_artifact_to_path(
        &mut context.db,
        &context.store,
        &args.namespace_id,
        &args.conflict_id,
        &args.destination_path,
    )?;

    let mut rendered = String::new();
    match restored {
        RestoredConflictArtifact::File(restored) => {
            let _ = writeln!(
                &mut rendered,
                "namespace={} conflict_id={} artifact_kind=file destination={}",
                restored.namespace_id.as_str(),
                restored.conflict_id,
                restored.destination_path.display()
            );
            let _ = writeln!(
                &mut rendered,
                "file_digest_sha256={} content_manifest_digest={}",
                restored.file_digest_sha256, restored.content_manifest_digest
            );
        }
        RestoredConflictArtifact::Subtree(restored) => {
            let _ = writeln!(
                &mut rendered,
                "namespace={} conflict_id={} artifact_kind=subtree destination={}",
                restored.namespace_id.as_str(),
                restored.conflict_id,
                restored.destination_root.display()
            );
            let _ = writeln!(
                &mut rendered,
                "restored_entry_count={}",
                restored.restored_entry_count
            );
        }
    }

    Ok(rendered)
}

fn parse_conflict_flags(
    mut args: impl Iterator<Item = String>,
    usage: &'static str,
    require_destination: bool,
) -> Result<(PathBuf, PathBuf, Option<PathBuf>)> {
    let mut db_path = None;
    let mut store_root = None;
    let mut destination_path = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("missing value for --db\n{usage}"))?;
                db_path = Some(PathBuf::from(value));
            }
            "--store-root" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("missing value for --store-root\n{usage}"))?;
                store_root = Some(PathBuf::from(value));
            }
            "--to" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("missing value for --to\n{usage}"))?;
                destination_path = Some(PathBuf::from(value));
            }
            other => bail!("unexpected argument `{other}`\n{usage}"),
        }
    }

    let db_path = db_path.ok_or_else(|| anyhow!("missing required --db\n{usage}"))?;
    let store_root = store_root.ok_or_else(|| anyhow!("missing required --store-root\n{usage}"))?;
    let destination_path = match (require_destination, destination_path) {
        (true, Some(path)) => Some(path),
        (true, None) => bail!("missing required --to\n{usage}"),
        (false, Some(_)) => bail!("unexpected --to argument\n{usage}"),
        (false, None) => None,
    };

    Ok((db_path, store_root, destination_path))
}

fn render_conflict_artifact_yaml(row: &ConflictArtifactRow) -> Result<String> {
    match row.file_envelope() {
        Some(envelope) => render_yaml(envelope).map_err(|err| anyhow!("{err}")),
        None => render_yaml(
            row.subtree_envelope()
                .expect("subtree artifact row should have subtree envelope"),
        )
        .map_err(|err| anyhow!("{err}")),
    }
}

fn open_conflict_command_context(
    db_path: &Path,
    store_root: &Path,
) -> Result<ConflictCommandContext> {
    validate_db_path(db_path)?;
    validate_store_root(store_root)?;
    let db = SqliteStateDb::open(db_path)?;
    let store = LocalFsStore::new(store_root).map_err(|err| anyhow!("{err}"))?;
    Ok(ConflictCommandContext { db, store })
}

fn validate_db_path(db_path: &Path) -> Result<()> {
    if !db_path.exists() {
        bail!("db path does not exist: `{}`", db_path.display());
    }
    if !db_path.is_file() {
        bail!("db path is not a file: `{}`", db_path.display());
    }
    Ok(())
}

fn validate_store_root(store_root: &Path) -> Result<()> {
    if !store_root.exists() {
        bail!("store root does not exist: `{}`", store_root.display());
    }
    if !store_root.is_dir() {
        bail!("store root is not a directory: `{}`", store_root.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        run_conflict_list, run_conflict_restore, run_conflict_show, ConflictListArgs,
        ConflictRestoreArgs, ConflictShowArgs,
    };
    use loon_client::state_db::SqliteStateDb;
    use loon_client::upload::{upload_small_file_from_path, UploadedContent};
    use loon_client::{
        prepare_conflict_artifact, prepare_subtree_conflict_artifact,
        write_conflict_artifact_if_absent, write_subtree_conflict_artifact_if_absent,
    };
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::keys::{blob, content_manifest};
    use loon_objectstore::ObjectStore;
    use loon_testkit::tempdir::TestDir;
    use loon_types::{
        ChangeSeq, ConflictArtifactLoserSummary, ConflictArtifactWinnerSummary, ConflictClass,
        InodeId, InodeKind, NamespaceId, RevisionNo, SubtreeConflictArtifactEntry,
        SubtreeConflictArtifactRootSummary,
    };
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};

    #[test]
    fn conflict_list_discovers_artifacts_and_prints_sorted_output() {
        let fixture = ConflictArtifactFixture::new("xtask-conflict-list");
        let namespace_id = NamespaceId::from("ns-xtask-conflicts");
        let file_artifact = seed_file_conflict_artifact(
            &fixture,
            &namespace_id,
            "report.txt",
            "local loser",
            ChangeSeq(41),
            1_700_000_000_001,
        );
        let subtree_artifact = seed_subtree_conflict_artifact(
            &fixture,
            &namespace_id,
            ChangeSeq(42),
            1_700_000_000_002,
            vec![
                RestoreTreeEntry {
                    relative_path: "docs".to_owned(),
                    inode_kind: InodeKind::Dir,
                    content_utf8: None,
                    client_file_id: None,
                },
                RestoreTreeEntry {
                    relative_path: "docs/report.txt".to_owned(),
                    inode_kind: InodeKind::File,
                    content_utf8: Some("dirty subtree draft".to_owned()),
                    client_file_id: None,
                },
            ],
        );

        let output = run_conflict_list(ConflictListArgs {
            namespace_id: namespace_id.clone(),
            db_path: fixture.db_path.clone(),
            store_root: fixture.store_root.clone(),
        })
        .expect("run conflict-list");

        let db = SqliteStateDb::open(&fixture.db_path).expect("open db");
        let cached_rows = db
            .load_conflict_artifacts_for_namespace(&namespace_id)
            .expect("load cached artifacts");
        assert_eq!(cached_rows.len(), 2);
        assert_eq!(
            cached_rows
                .iter()
                .map(|row| row.conflict_id.clone())
                .collect::<Vec<_>>(),
            vec![
                file_artifact.envelope.conflict_id,
                subtree_artifact.envelope.conflict_id
            ]
        );
        assert_eq!(
            output,
            include_str!("../../tests/snapshots/conflict-list/conflict_list.txt")
        );
    }

    #[test]
    fn conflict_show_renders_file_artifact_details() {
        let fixture = ConflictArtifactFixture::new("xtask-conflict-show-file");
        let namespace_id = NamespaceId::from("ns-xtask-conflicts");
        let artifact = seed_file_conflict_artifact(
            &fixture,
            &namespace_id,
            "report.txt",
            "local loser",
            ChangeSeq(43),
            1_700_000_000_003,
        );

        let output = run_conflict_show(ConflictShowArgs {
            namespace_id,
            conflict_id: artifact.envelope.conflict_id.clone(),
            db_path: fixture.db_path.clone(),
            store_root: fixture.store_root.clone(),
        })
        .expect("run conflict-show for file");

        assert!(output.contains("artifact_kind=file"));
        assert!(output.contains("conflict_class=same_inode_stale_base_edit"));
        assert!(output.contains("display_name: report.txt"));
        assert!(output.contains("content_manifest_digest:"));
    }

    #[test]
    fn conflict_show_renders_subtree_artifact_details() {
        let fixture = ConflictArtifactFixture::new("xtask-conflict-show-subtree");
        let namespace_id = NamespaceId::from("ns-xtask-conflicts");
        let artifact = seed_subtree_conflict_artifact(
            &fixture,
            &namespace_id,
            ChangeSeq(42),
            1_700_000_000_002,
            vec![
                RestoreTreeEntry {
                    relative_path: "docs".to_owned(),
                    inode_kind: InodeKind::Dir,
                    content_utf8: None,
                    client_file_id: None,
                },
                RestoreTreeEntry {
                    relative_path: "docs/report.txt".to_owned(),
                    inode_kind: InodeKind::File,
                    content_utf8: Some("dirty subtree draft".to_owned()),
                    client_file_id: None,
                },
            ],
        );

        let output = run_conflict_show(ConflictShowArgs {
            namespace_id,
            conflict_id: artifact.envelope.conflict_id.clone(),
            db_path: fixture.db_path.clone(),
            store_root: fixture.store_root.clone(),
        })
        .expect("run conflict-show for subtree");

        assert_eq!(
            output,
            include_str!("../../tests/snapshots/conflict-show/conflict_show_subtree.txt")
        );
    }

    #[test]
    fn conflict_restore_restores_file_artifact_to_alternate_destination() {
        let fixture = ConflictArtifactFixture::new("xtask-conflict-restore-file");
        let namespace_id = NamespaceId::from("ns-xtask-conflicts");
        let canonical_path = fixture.mirror_root.join("workspace/report.txt");
        let destination_path = fixture.mirror_root.join("recovery/report-conflict.txt");
        write_file(&canonical_path, "authoritative winner");
        fs::create_dir_all(destination_path.parent().expect("destination parent"))
            .expect("create destination parent");

        let artifact = seed_file_conflict_artifact(
            &fixture,
            &namespace_id,
            "report.txt",
            "local loser",
            ChangeSeq(51),
            1_700_000_010_000,
        );

        let output = run_conflict_restore(ConflictRestoreArgs {
            namespace_id: namespace_id.clone(),
            conflict_id: artifact.envelope.conflict_id.clone(),
            db_path: fixture.db_path.clone(),
            store_root: fixture.store_root.clone(),
            destination_path: destination_path.clone(),
        })
        .expect("run conflict-restore for file");

        assert_eq!(
            fs::read_to_string(&destination_path).expect("read restored file"),
            "local loser"
        );
        assert_eq!(
            fs::read_to_string(&canonical_path).expect("read canonical file"),
            "authoritative winner"
        );
        let normalized =
            normalize_output_paths(&output, &[(&destination_path, "<restore-destination>")]);
        assert_eq!(
            normalized,
            include_str!("../../tests/snapshots/conflict-restore/conflict_restore_file.txt")
        );
    }

    #[test]
    fn conflict_restore_restores_subtree_artifact_to_alternate_root() {
        let fixture = ConflictArtifactFixture::new("xtask-conflict-restore-subtree");
        let namespace_id = NamespaceId::from("ns-xtask-conflicts");
        let canonical_root = fixture.mirror_root.join("workspace/reports");
        let destination_root = fixture.mirror_root.join("recovery/reports-conflict");
        write_file(&canonical_root.join("summary.txt"), "authoritative summary");
        fs::create_dir_all(destination_root.parent().expect("destination parent"))
            .expect("create destination parent");

        let artifact = seed_subtree_conflict_artifact(
            &fixture,
            &namespace_id,
            ChangeSeq(61),
            1_700_000_020_000,
            vec![
                RestoreTreeEntry {
                    relative_path: "drafts".to_owned(),
                    inode_kind: InodeKind::Dir,
                    content_utf8: None,
                    client_file_id: None,
                },
                RestoreTreeEntry {
                    relative_path: "drafts/notes.txt".to_owned(),
                    inode_kind: InodeKind::File,
                    content_utf8: Some("local subtree draft".to_owned()),
                    client_file_id: None,
                },
            ],
        );

        let output = run_conflict_restore(ConflictRestoreArgs {
            namespace_id,
            conflict_id: artifact.envelope.conflict_id.clone(),
            db_path: fixture.db_path.clone(),
            store_root: fixture.store_root.clone(),
            destination_path: destination_root.clone(),
        })
        .expect("run conflict-restore for subtree");

        assert!(output.contains("artifact_kind=subtree"));
        assert!(output.contains("restored_entry_count=2"));
        assert_eq!(
            collect_tree_state(&destination_root).expect("collect restored subtree"),
            vec![
                "dir:drafts".to_owned(),
                "file:drafts/notes.txt=local subtree draft".to_owned(),
            ]
        );
        assert_eq!(
            collect_tree_state(&canonical_root).expect("collect canonical subtree"),
            vec!["file:summary.txt=authoritative summary".to_owned()]
        );
    }

    #[test]
    fn conflict_list_rejects_missing_db_path() {
        let fixture = ConflictArtifactFixture::new("xtask-conflict-missing-db");
        let missing_db_path = fixture.temp_dir.path().join("missing.sqlite3");
        let error = run_conflict_list(ConflictListArgs {
            namespace_id: NamespaceId::from("ns-xtask-conflicts"),
            db_path: missing_db_path.clone(),
            store_root: fixture.store_root.clone(),
        })
        .expect_err("missing db path should fail");
        assert_eq!(
            error.to_string(),
            format!("db path does not exist: `{}`", missing_db_path.display())
        );
    }

    #[test]
    fn conflict_list_rejects_missing_store_root() {
        let fixture = ConflictArtifactFixture::new("xtask-conflict-missing-store");
        let missing_store_root = fixture.temp_dir.path().join("missing-store");
        let error = run_conflict_list(ConflictListArgs {
            namespace_id: NamespaceId::from("ns-xtask-conflicts"),
            db_path: fixture.db_path.clone(),
            store_root: missing_store_root.clone(),
        })
        .expect_err("missing store root should fail");
        assert_eq!(
            error.to_string(),
            format!(
                "store root does not exist: `{}`",
                missing_store_root.display()
            )
        );
    }

    #[test]
    fn conflict_restore_rejects_occupied_destination() {
        let fixture = ConflictArtifactFixture::new("xtask-conflict-restore-occupied");
        let namespace_id = NamespaceId::from("ns-xtask-conflicts");
        let canonical_path = fixture.mirror_root.join("workspace/report.txt");
        let destination_path = fixture.mirror_root.join("recovery/report-conflict.txt");
        write_file(&canonical_path, "authoritative winner");
        write_file(&destination_path, "already here");
        let artifact = seed_file_conflict_artifact(
            &fixture,
            &namespace_id,
            "report.txt",
            "local loser",
            ChangeSeq(71),
            1_700_000_030_000,
        );

        let error = run_conflict_restore(ConflictRestoreArgs {
            namespace_id,
            conflict_id: artifact.envelope.conflict_id.clone(),
            db_path: fixture.db_path.clone(),
            store_root: fixture.store_root.clone(),
            destination_path: destination_path.clone(),
        })
        .expect_err("occupied restore destination should fail");

        assert_eq!(
            error.to_string(),
            format!(
                "restore destination already exists: `{}`",
                destination_path.display()
            )
        );
    }

    #[test]
    fn conflict_restore_rejects_missing_preserved_content() {
        let fixture = ConflictArtifactFixture::new("xtask-conflict-missing-content");
        let namespace_id = NamespaceId::from("ns-xtask-conflicts");
        let canonical_path = fixture.mirror_root.join("workspace/report.txt");
        let destination_path = fixture.mirror_root.join("recovery/report-conflict.txt");
        write_file(&canonical_path, "authoritative winner");
        fs::create_dir_all(destination_path.parent().expect("destination parent"))
            .expect("create destination parent");
        let artifact = seed_file_conflict_artifact(
            &fixture,
            &namespace_id,
            "report.txt",
            "local loser",
            ChangeSeq(72),
            1_700_000_040_000,
        );
        remove_manifest_and_blocks(
            &fixture,
            &namespace_id,
            &artifact.envelope.loser.content_manifest_digest,
        );

        let error = run_conflict_restore(ConflictRestoreArgs {
            namespace_id,
            conflict_id: artifact.envelope.conflict_id.clone(),
            db_path: fixture.db_path.clone(),
            store_root: fixture.store_root.clone(),
            destination_path: destination_path.clone(),
        })
        .expect_err("missing preserved content should fail");

        assert!(
            error
                .to_string()
                .starts_with("missing manifest object `namespaces/ns-xtask-conflicts/manifests/"),
            "unexpected missing content error: {error}"
        );
        assert!(
            !destination_path.exists(),
            "failed restore should not materialize destination"
        );
    }

    #[test]
    fn conflict_list_fails_on_malformed_artifact_without_partial_cache_update() {
        let fixture = ConflictArtifactFixture::new("xtask-conflict-malformed");
        let namespace_id = NamespaceId::from("ns-xtask-conflicts");
        let _good_artifact = seed_file_conflict_artifact(
            &fixture,
            &namespace_id,
            "report.txt",
            "local loser",
            ChangeSeq(81),
            1_700_000_050_000,
        );
        let malformed_path = fixture
            .store_root
            .join("namespaces")
            .join(namespace_id.as_str())
            .join("conflicts")
            .join("malformed.json");
        fs::create_dir_all(malformed_path.parent().expect("malformed parent"))
            .expect("create malformed parent");
        fs::write(&malformed_path, b"{not-json").expect("write malformed artifact");

        let error = run_conflict_list(ConflictListArgs {
            namespace_id: namespace_id.clone(),
            db_path: fixture.db_path.clone(),
            store_root: fixture.store_root.clone(),
        })
        .expect_err("malformed artifact should fail");

        assert!(
            error
                .to_string()
                .contains("failed to decode conflict artifact object"),
            "unexpected malformed artifact error: {error}"
        );

        let db = SqliteStateDb::open(&fixture.db_path).expect("open db");
        assert!(
            db.load_conflict_artifacts_for_namespace(&namespace_id)
                .expect("load cached artifacts")
                .is_empty(),
            "discovery failure should not partially update the cache"
        );
    }

    struct ConflictArtifactFixture {
        temp_dir: TestDir,
        db_path: PathBuf,
        mirror_root: PathBuf,
        store_root: PathBuf,
        store: LocalFsStore,
    }

    impl ConflictArtifactFixture {
        fn new(label: &str) -> Self {
            let temp_dir = TestDir::new(label);
            let db_path = temp_dir.path().join("client.sqlite3");
            let mirror_root = temp_dir.path().join("mirror");
            let store_root = temp_dir.path().join("store");
            fs::create_dir_all(&mirror_root).expect("create mirror root");
            let _ = SqliteStateDb::open(&db_path).expect("initialize db");
            let store = LocalFsStore::new(&store_root).expect("create local store");
            Self {
                temp_dir,
                db_path,
                mirror_root,
                store_root,
                store,
            }
        }

        fn upload_content(
            &self,
            namespace_id: &NamespaceId,
            file_name: &str,
            content: &str,
        ) -> UploadedContent {
            let source_path = self.store_root.join("uploads").join(file_name);
            write_file(&source_path, content);
            upload_small_file_from_path(&self.store, namespace_id, &source_path)
                .expect("upload content to store")
        }
    }

    #[derive(Debug, Clone)]
    struct RestoreTreeEntry {
        relative_path: String,
        inode_kind: InodeKind,
        content_utf8: Option<String>,
        client_file_id: Option<String>,
    }

    fn seed_file_conflict_artifact(
        fixture: &ConflictArtifactFixture,
        namespace_id: &NamespaceId,
        display_name: &str,
        loser_content: &str,
        detected_seq: ChangeSeq,
        created_at_ms: u64,
    ) -> loon_client::PreparedConflictArtifact {
        let uploaded = fixture.upload_content(namespace_id, "loser-file.txt", loser_content);
        let artifact = prepare_conflict_artifact(
            namespace_id,
            ConflictClass::SameInodeStaleBaseEdit,
            detected_seq,
            ConflictArtifactWinnerSummary {
                inode_id: InodeId(42),
                parent_inode_id: Some(InodeId(2)),
                display_name: display_name.to_owned(),
                revision_no: RevisionNo(7),
                content_manifest_digest: Some("sha256:winner-manifest".to_owned()),
            },
            ConflictArtifactLoserSummary {
                inode_id: Some(InodeId(42)),
                client_file_id: None,
                base_revision_no: Some(RevisionNo(6)),
                content_manifest_digest: uploaded.content_manifest_digest.clone(),
                content_digest: Some(uploaded.file_digest_sha256.clone()),
                parent_inode_id: Some(InodeId(2)),
                display_name: display_name.to_owned(),
            },
            created_at_ms,
        );
        write_conflict_artifact_if_absent(&fixture.store, &artifact).expect("write file artifact");
        artifact
    }

    fn seed_subtree_conflict_artifact(
        fixture: &ConflictArtifactFixture,
        namespace_id: &NamespaceId,
        detected_seq: ChangeSeq,
        created_at_ms: u64,
        entries: Vec<RestoreTreeEntry>,
    ) -> loon_client::PreparedSubtreeConflictArtifact {
        let mut uploaded_by_path = BTreeMap::new();
        for (index, entry) in entries.iter().enumerate() {
            if entry.inode_kind == InodeKind::File {
                let content = entry
                    .content_utf8
                    .as_deref()
                    .expect("file entry should include content");
                let uploaded = fixture.upload_content(
                    namespace_id,
                    &format!("subtree-loser-{index}.txt"),
                    content,
                );
                uploaded_by_path.insert(entry.relative_path.clone(), uploaded);
            }
        }

        let artifact_entries = entries
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                let uploaded = uploaded_by_path.get(&entry.relative_path);
                SubtreeConflictArtifactEntry {
                    relative_path: entry.relative_path.clone(),
                    inode_kind: entry.inode_kind.clone(),
                    inode_id: entry
                        .client_file_id
                        .as_ref()
                        .map(|_| None)
                        .unwrap_or_else(|| Some(InodeId(100 + index as u64))),
                    client_file_id: entry.client_file_id.clone(),
                    base_revision_no: uploaded.map(|_| RevisionNo(1)),
                    content_manifest_digest: uploaded
                        .map(|uploaded| uploaded.content_manifest_digest.clone()),
                    content_digest: uploaded.map(|uploaded| uploaded.file_digest_sha256.clone()),
                    parent_inode_id: Some(InodeId(10)),
                    display_name: file_name_from_relative_path(&entry.relative_path).to_owned(),
                }
            })
            .collect::<Vec<_>>();

        let artifact = prepare_subtree_conflict_artifact(
            namespace_id,
            ConflictClass::SubtreeRenameVsLocalChanges,
            detected_seq,
            ConflictArtifactWinnerSummary {
                inode_id: InodeId(10),
                parent_inode_id: Some(InodeId(2)),
                display_name: "reports".to_owned(),
                revision_no: RevisionNo(9),
                content_manifest_digest: None,
            },
            SubtreeConflictArtifactRootSummary {
                inode_id: InodeId(10),
                parent_inode_id: Some(InodeId(2)),
                display_name: "reports".to_owned(),
                revision_no: Some(RevisionNo(8)),
            },
            artifact_entries,
            created_at_ms,
        );
        write_subtree_conflict_artifact_if_absent(&fixture.store, &artifact)
            .expect("write subtree artifact");
        artifact
    }

    fn remove_manifest_and_blocks(
        fixture: &ConflictArtifactFixture,
        namespace_id: &NamespaceId,
        manifest_digest: &str,
    ) {
        let manifest_bytes = fixture
            .store
            .get(
                &content_manifest(namespace_id.as_str(), manifest_digest),
                None,
            )
            .expect("load manifest before deletion")
            .expect("manifest should exist before deletion");
        let manifest = loon_types::decode_content_manifest_json(&manifest_bytes)
            .expect("decode manifest before deletion");
        for block in &manifest.payload.blocks {
            let block_path = fixture
                .store_root
                .join(blob(namespace_id.as_str(), &block.content_digest_sha256));
            if block_path.exists() {
                fs::remove_file(&block_path).expect("remove stored block");
            }
        }
        let manifest_path = fixture
            .store_root
            .join(content_manifest(namespace_id.as_str(), manifest_digest));
        if manifest_path.exists() {
            fs::remove_file(&manifest_path).expect("remove manifest object");
        }
    }

    fn collect_tree_state(root: &Path) -> std::io::Result<Vec<String>> {
        let mut entries = Vec::new();
        collect_tree_state_inner(root, root, &mut entries)?;
        entries.sort();
        Ok(entries)
    }

    fn collect_tree_state_inner(
        root: &Path,
        current: &Path,
        entries: &mut Vec<String>,
    ) -> std::io::Result<()> {
        for child in fs::read_dir(current)? {
            let child = child?;
            let child_path = child.path();
            let relative = child_path
                .strip_prefix(root)
                .expect("child should be below root")
                .to_string_lossy()
                .replace('\\', "/");
            let metadata = child.metadata()?;
            if metadata.is_dir() {
                entries.push(format!("dir:{relative}"));
                collect_tree_state_inner(root, &child_path, entries)?;
            } else {
                let content = fs::read_to_string(&child_path)?;
                entries.push(format!("file:{relative}={content}"));
            }
        }
        Ok(())
    }

    fn write_file(path: &Path, content: &str) {
        fs::create_dir_all(path.parent().expect("parent")).expect("create parent");
        fs::write(path, content).expect("write file");
    }

    fn file_name_from_relative_path(relative_path: &str) -> &str {
        Path::new(relative_path)
            .file_name()
            .and_then(|name| name.to_str())
            .expect("relative path should include file name")
    }

    fn normalize_output_paths(output: &str, replacements: &[(&Path, &str)]) -> String {
        let mut normalized = output.to_owned();
        for (path, token) in replacements {
            normalized = normalized.replace(&path.display().to_string(), token);
        }
        normalized
    }
}
