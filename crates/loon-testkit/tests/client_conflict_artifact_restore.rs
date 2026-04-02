use loon_client::state_db::SqliteStateDb;
use loon_client::upload::{upload_small_file_from_path, UploadedContent};
use loon_client::{
    discover_conflict_artifacts_for_namespace, prepare_conflict_artifact,
    prepare_subtree_conflict_artifact, restore_file_conflict_artifact_to_path,
    restore_subtree_conflict_artifact_to_path, write_conflict_artifact_if_absent,
    write_subtree_conflict_artifact_if_absent, ConflictArtifactError,
};
use loon_server::objectstore::fs::LocalFsStore;
use loon_server::objectstore::keys::{blob, conflict_artifact, content_manifest};
use loon_server::objectstore::ObjectStore;
use loon_testkit::invariants::{
    evaluate_conflict_artifact_discovery_invariants,
    evaluate_file_conflict_artifact_restore_invariants,
    evaluate_subtree_conflict_artifact_restore_invariants,
    ConflictArtifactDiscoveryInvariantInputs, ConflictArtifactRecoveryInvariantReport,
    FileConflictArtifactRestoreInvariantInputs, SubtreeConflictArtifactRestoreInvariantInputs,
};
use loon_testkit::render::render_trace;
use loon_testkit::scenario::Scenario;
use loon_testkit::tempdir::TestDir;
use loon_types::{
    ChangeSeq, ConflictArtifactLoserSummary, ConflictArtifactWinnerSummary, ConflictClass, InodeId,
    InodeKind, NamespaceId, RevisionNo, SubtreeConflictArtifactEntry,
    SubtreeConflictArtifactRootSummary,
};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn conflict_artifact_discovery_caches_namespace_artifacts() {
    let rendered_trace = run_conflict_artifact_discovery_report(
        "client/conflict_artifact_discovery_caches_namespace_artifacts.yaml",
        "client-conflict-artifact-discovery",
    );

    assert_eq!(
        rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-conflict-artifact-recovery/client/conflict_artifact_discovery_caches_namespace_artifacts.txt"
        )
    );
}

#[test]
fn restore_file_conflict_artifact_to_alternate_path() {
    let rendered_trace = run_file_restore_report(
        "client/restore_file_conflict_artifact_to_alternate_path.yaml",
        "client-file-conflict-artifact-restore",
    );

    assert_eq!(
        rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-conflict-artifact-recovery/client/restore_file_conflict_artifact_to_alternate_path.txt"
        )
    );
}

#[test]
fn restore_subtree_conflict_artifact_to_alternate_root() {
    let rendered_trace = run_subtree_restore_report(
        "client/restore_subtree_conflict_artifact_to_alternate_root.yaml",
        "client-subtree-conflict-artifact-restore",
    );

    assert_eq!(
        rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-conflict-artifact-recovery/client/restore_subtree_conflict_artifact_to_alternate_root.txt"
        )
    );
}

#[test]
fn restore_conflict_artifact_rejects_occupied_destination() {
    let scenario =
        load_fixture("client/restore_conflict_artifact_rejects_occupied_destination.yaml");
    let initial: FileRestoreInitial = scenario.decode_initial().expect("decode initial");
    let expect: ErrorExpect = scenario.decode_expect().expect("decode expect");
    let fixture = ConflictArtifactFixture::new("restore-conflict-occupied-destination");
    let canonical_path = fixture.mirror_root.join(&initial.canonical_relative_path);
    let destination_path = fixture.mirror_root.join(&initial.restore_relative_path);
    write_file(&canonical_path, &initial.canonical_content_utf8);
    write_file(&destination_path, "already here");

    let artifact = seed_file_conflict_artifact(
        &fixture,
        &initial.namespace_id,
        file_name_from_relative_path(&initial.canonical_relative_path),
        &initial.loser_content_utf8,
        ChangeSeq(41),
        1_700_000_030_000,
    );

    let error = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
        restore_file_conflict_artifact_to_path(
            &mut db,
            &fixture.store,
            &initial.namespace_id,
            artifact.envelope.conflict_id.as_str(),
            &destination_path,
        )
        .expect_err("occupied destination should fail")
    };

    assert_eq!(expect.error_kind, "destination_exists");
    assert!(matches!(
        error,
        ConflictArtifactError::DestinationExists { .. }
    ));
    assert_eq!(
        fs::read_to_string(&canonical_path).expect("read canonical file"),
        initial.canonical_content_utf8
    );
}

#[test]
fn restore_conflict_artifact_rejects_missing_preserved_content() {
    let scenario =
        load_fixture("client/restore_conflict_artifact_rejects_missing_preserved_content.yaml");
    let initial: FileRestoreInitial = scenario.decode_initial().expect("decode initial");
    let expect: ErrorExpect = scenario.decode_expect().expect("decode expect");
    let fixture = ConflictArtifactFixture::new("restore-conflict-missing-content");
    let canonical_path = fixture.mirror_root.join(&initial.canonical_relative_path);
    let destination_path = fixture.mirror_root.join(&initial.restore_relative_path);
    write_file(&canonical_path, &initial.canonical_content_utf8);
    fs::create_dir_all(destination_path.parent().expect("destination parent"))
        .expect("create destination parent");

    let artifact = seed_file_conflict_artifact(
        &fixture,
        &initial.namespace_id,
        file_name_from_relative_path(&initial.canonical_relative_path),
        &initial.loser_content_utf8,
        ChangeSeq(42),
        1_700_000_040_000,
    );
    let manifest_digest = artifact.envelope.loser.content_manifest_digest.clone();
    remove_manifest_and_blocks(&fixture, &initial.namespace_id, &manifest_digest);

    let error = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
        restore_file_conflict_artifact_to_path(
            &mut db,
            &fixture.store,
            &initial.namespace_id,
            artifact.envelope.conflict_id.as_str(),
            &destination_path,
        )
        .expect_err("missing preserved content should fail")
    };

    assert_eq!(expect.error_kind, "missing_preserved_content");
    assert!(matches!(error, ConflictArtifactError::Download(_)));
    assert!(
        !destination_path.exists(),
        "failed restore should not materialize destination"
    );
    assert!(
        fixture
            .store
            .get(
                &conflict_artifact(
                    initial.namespace_id.as_str(),
                    artifact.envelope.conflict_id.as_str()
                ),
                None
            )
            .expect("load artifact object")
            .is_some(),
        "artifact object should remain durable after failure"
    );
}

#[derive(Debug, Deserialize)]
struct DiscoveryInitial {
    namespace_id: NamespaceId,
    expected_artifact_count: usize,
}

#[derive(Debug, Deserialize)]
struct FileRestoreInitial {
    namespace_id: NamespaceId,
    canonical_relative_path: String,
    restore_relative_path: String,
    canonical_content_utf8: String,
    loser_content_utf8: String,
}

#[derive(Debug, Deserialize)]
struct SubtreeRestoreInitial {
    namespace_id: NamespaceId,
    canonical_root_relative_path: String,
    restore_root_relative_path: String,
    canonical_files: Vec<CanonicalTextFile>,
    loser_entries: Vec<RestoreTreeEntry>,
}

#[derive(Debug, Deserialize)]
struct CanonicalTextFile {
    relative_path: String,
    content_utf8: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RestoreTreeEntry {
    relative_path: String,
    inode_kind: InodeKind,
    content_utf8: Option<String>,
    client_file_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RecoveryExpect {
    invariants: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ErrorExpect {
    error_kind: String,
}

struct ConflictArtifactFixture {
    _temp_dir: TestDir,
    db_path: PathBuf,
    mirror_root: PathBuf,
    store_root: PathBuf,
    store: LocalFsStore,
}

impl ConflictArtifactFixture {
    fn new(name: &str) -> Self {
        let temp_dir = TestDir::new(name);
        let db_path = temp_dir.path().join("client.sqlite3");
        let mirror_root = temp_dir.path().join("mirror");
        let store_root = temp_dir.path().join("store");
        fs::create_dir_all(&mirror_root).expect("create mirror root");
        let store = LocalFsStore::new(&store_root).expect("create local store");
        Self {
            _temp_dir: temp_dir,
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

fn run_conflict_artifact_discovery_report(relative_path: &str, temp_dir_name: &str) -> String {
    let scenario = load_fixture(relative_path);
    let initial: DiscoveryInitial = scenario.decode_initial().expect("decode initial");
    let expect: RecoveryExpect = scenario.decode_expect().expect("decode expect");
    let fixture = ConflictArtifactFixture::new(temp_dir_name);

    let file_artifact = seed_file_conflict_artifact(
        &fixture,
        &initial.namespace_id,
        "report.txt",
        "local loser",
        ChangeSeq(41),
        1_700_000_000_001,
    );
    let subtree_artifact = seed_subtree_conflict_artifact(
        &fixture,
        &initial.namespace_id,
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

    let (discovered_rows, cached_rows, report) = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
        let discovered_rows = discover_conflict_artifacts_for_namespace(
            &mut db,
            &fixture.store,
            &initial.namespace_id,
        )
        .expect("discover conflict artifacts");
        let cached_rows = db
            .load_conflict_artifacts_for_namespace(&initial.namespace_id)
            .expect("load cached artifacts");
        let report = evaluate_conflict_artifact_discovery_invariants(
            ConflictArtifactDiscoveryInvariantInputs {
                discovered_artifact_count: discovered_rows.len(),
                cached_artifact_count_after: cached_rows.len(),
                expected_namespace_artifact_count: initial.expected_artifact_count,
            },
        );
        (discovered_rows, cached_rows, report)
    };

    assert_eq!(discovered_rows.len(), initial.expected_artifact_count);
    assert_eq!(cached_rows.len(), initial.expected_artifact_count);
    assert_eq!(
        discovered_rows
            .iter()
            .map(|row| row.conflict_id.clone())
            .collect::<Vec<_>>(),
        vec![
            file_artifact.envelope.conflict_id.clone(),
            subtree_artifact.envelope.conflict_id.clone(),
        ]
    );
    assert_expected_recovery_invariants(&report, &expect.invariants);

    let trace = vec![
        format!(
            "discovered_conflict_ids={:?}",
            discovered_rows
                .iter()
                .map(|row| row.conflict_id.clone())
                .collect::<Vec<_>>()
        ),
        format!(
            "cached_conflict_ids={:?}",
            cached_rows
                .iter()
                .map(|row| row.conflict_id.clone())
                .collect::<Vec<_>>()
        ),
    ]
    .into_iter()
    .chain(report.render_trace_lines("conflict-artifact-discovery"))
    .collect::<Vec<_>>();

    render_trace(&scenario, &trace)
}

fn run_file_restore_report(relative_path: &str, temp_dir_name: &str) -> String {
    let scenario = load_fixture(relative_path);
    let initial: FileRestoreInitial = scenario.decode_initial().expect("decode initial");
    let expect: RecoveryExpect = scenario.decode_expect().expect("decode expect");
    let fixture = ConflictArtifactFixture::new(temp_dir_name);
    let canonical_path = fixture.mirror_root.join(&initial.canonical_relative_path);
    let destination_path = fixture.mirror_root.join(&initial.restore_relative_path);
    write_file(&canonical_path, &initial.canonical_content_utf8);
    fs::create_dir_all(destination_path.parent().expect("destination parent"))
        .expect("create destination parent");

    let artifact = seed_file_conflict_artifact(
        &fixture,
        &initial.namespace_id,
        file_name_from_relative_path(&initial.canonical_relative_path),
        &initial.loser_content_utf8,
        ChangeSeq(51),
        1_700_000_010_000,
    );

    let restored = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
        restore_file_conflict_artifact_to_path(
            &mut db,
            &fixture.store,
            &initial.namespace_id,
            artifact.envelope.conflict_id.as_str(),
            &destination_path,
        )
        .expect("restore file artifact")
    };

    let restored_bytes = fs::read_to_string(&destination_path).expect("read restored file");
    let canonical_bytes = fs::read_to_string(&canonical_path).expect("read canonical file");
    let cached_artifact = {
        let db = SqliteStateDb::open(&fixture.db_path).expect("reopen db");
        db.load_conflict_artifact(
            &initial.namespace_id,
            artifact.envelope.conflict_id.as_str(),
        )
        .expect("load cached artifact")
        .expect("cached artifact row")
    };
    let report = evaluate_file_conflict_artifact_restore_invariants(
        FileConflictArtifactRestoreInvariantInputs {
            destination_was_absent_before: true,
            restored_content_matches_loser: restored_bytes == initial.loser_content_utf8,
            canonical_path_untouched: canonical_bytes == initial.canonical_content_utf8,
        },
    );

    assert_eq!(restored.conflict_id, artifact.envelope.conflict_id);
    assert_eq!(restored.destination_path, destination_path);
    assert_expected_recovery_invariants(&report, &expect.invariants);
    assert!(
        fixture
            .store
            .get(&artifact.object_key, None)
            .expect("load artifact object")
            .is_some(),
        "artifact object should remain present"
    );
    assert_eq!(cached_artifact.conflict_id, artifact.envelope.conflict_id);

    let trace = vec![
        format!("restored_conflict_id={}", restored.conflict_id),
        format!(
            "restored_destination={} canonical_destination={}",
            initial.restore_relative_path, initial.canonical_relative_path
        ),
        format!(
            "restored_digest={} manifest_digest={}",
            restored.file_digest_sha256, restored.content_manifest_digest
        ),
    ]
    .into_iter()
    .chain(report.render_trace_lines("file-conflict-artifact-restore"))
    .collect::<Vec<_>>();

    render_trace(&scenario, &trace)
}

fn run_subtree_restore_report(relative_path: &str, temp_dir_name: &str) -> String {
    let scenario = load_fixture(relative_path);
    let initial: SubtreeRestoreInitial = scenario.decode_initial().expect("decode initial");
    let expect: RecoveryExpect = scenario.decode_expect().expect("decode expect");
    let fixture = ConflictArtifactFixture::new(temp_dir_name);
    let canonical_root = fixture
        .mirror_root
        .join(&initial.canonical_root_relative_path);
    let destination_root = fixture
        .mirror_root
        .join(&initial.restore_root_relative_path);

    for file in &initial.canonical_files {
        write_file(
            &canonical_root.join(&file.relative_path),
            &file.content_utf8,
        );
    }
    fs::create_dir_all(destination_root.parent().expect("destination parent"))
        .expect("create destination parent");
    let expected_canonical = collect_tree_state(&canonical_root).expect("collect canonical tree");

    let artifact = seed_subtree_conflict_artifact(
        &fixture,
        &initial.namespace_id,
        ChangeSeq(61),
        1_700_000_020_000,
        initial.loser_entries.clone(),
    );

    let restored = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
        restore_subtree_conflict_artifact_to_path(
            &mut db,
            &fixture.store,
            &initial.namespace_id,
            artifact.envelope.conflict_id.as_str(),
            &destination_root,
        )
        .expect("restore subtree artifact")
    };

    let expected_tree = expected_tree_state(&initial.loser_entries);
    let restored_tree = collect_tree_state(&destination_root).expect("collect restored tree");
    let canonical_tree = collect_tree_state(&canonical_root).expect("collect canonical tree");
    let cached_artifact = {
        let db = SqliteStateDb::open(&fixture.db_path).expect("reopen db");
        db.load_conflict_artifact(
            &initial.namespace_id,
            artifact.envelope.conflict_id.as_str(),
        )
        .expect("load cached subtree artifact")
        .expect("cached subtree artifact row")
    };
    let report = evaluate_subtree_conflict_artifact_restore_invariants(
        SubtreeConflictArtifactRestoreInvariantInputs {
            destination_was_absent_before: true,
            restored_tree_matches_loser: restored_tree == expected_tree,
            deterministic_entry_order_preserved: artifact
                .envelope
                .entries
                .iter()
                .map(|entry| entry.relative_path.clone())
                .collect::<Vec<_>>()
                == initial
                    .loser_entries
                    .iter()
                    .map(|entry| entry.relative_path.clone())
                    .collect::<Vec<_>>(),
            canonical_tree_untouched: canonical_tree == expected_canonical,
        },
    );

    assert_eq!(restored.conflict_id, artifact.envelope.conflict_id);
    assert_eq!(restored.destination_root, destination_root);
    assert_eq!(restored.restored_entry_count, initial.loser_entries.len());
    assert_expected_recovery_invariants(&report, &expect.invariants);
    assert!(
        fixture
            .store
            .get(&artifact.object_key, None)
            .expect("load subtree artifact object")
            .is_some(),
        "artifact object should remain present"
    );
    assert_eq!(cached_artifact.conflict_id, artifact.envelope.conflict_id);

    let trace = vec![
        format!("restored_conflict_id={}", restored.conflict_id),
        format!(
            "restored_destination_root={} canonical_root={}",
            initial.restore_root_relative_path, initial.canonical_root_relative_path
        ),
        format!("restored_tree={restored_tree:?}"),
    ]
    .into_iter()
    .chain(report.render_trace_lines("subtree-conflict-artifact-restore"))
    .collect::<Vec<_>>();

    render_trace(&scenario, &trace)
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

fn expected_tree_state(entries: &[RestoreTreeEntry]) -> Vec<String> {
    entries
        .iter()
        .map(|entry| match &entry.inode_kind {
            InodeKind::Dir => format!("dir:{}", entry.relative_path),
            InodeKind::File => format!(
                "file:{}={}",
                entry.relative_path,
                entry
                    .content_utf8
                    .as_deref()
                    .expect("file entry content should exist")
            ),
            kind => panic!("unsupported restore test inode kind: {kind:?}"),
        })
        .collect()
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

fn load_fixture(relative_path: &str) -> Scenario {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/scenarios")
        .join(relative_path);
    Scenario::load(&path)
        .unwrap_or_else(|error| panic!("failed to load scenario {}: {error}", path.display()))
}

fn assert_expected_recovery_invariants(
    report: &ConflictArtifactRecoveryInvariantReport,
    expected: &[String],
) {
    for name in expected {
        let check = report
            .check(name)
            .unwrap_or_else(|| panic!("missing invariant `{name}`"));
        assert!(check.passed, "invariant `{name}` failed: {}", check.detail);
    }
}
