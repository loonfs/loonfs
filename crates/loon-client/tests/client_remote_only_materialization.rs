use loon_client::executor::{
    execute_next_client_action, DownloadRemoteEditExecution, NextClientAction,
};
use loon_client::planner::{plan_file, PlannedActionRecord};
use loon_client::state_db::{
    AppliedInodeMutation, FileSyncViews, LocalFileStateRow, PlannedActionRow, RemoteFileStateRow,
    SqliteStateDb, SyncAnchorRow, TransferDirection, TransferLedgerRow, TransferState,
};
use loon_client::upload::{upload_small_file_from_path, UploadedContent};
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::{blob, content_manifest};
use loon_objectstore::ObjectStore;
use loon_testkit::scenario::Scenario;
use loon_types::{
    content_manifest_digest_sha256, encode_content_manifest_json, sha256_digest,
    ContentBlockDescriptor, ContentManifestEnvelope, ContentManifestPayload, InodeId, NamespaceId,
    CONTENT_BLOCK_SIZE_BYTES,
};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn execute_next_client_action_materializes_remote_only_file() {
    run_remote_only_materialization_fixture(
        "client/execute_next_client_action_materializes_remote_only_file.yaml",
        "client-remote-only-materialization",
    );
}

#[test]
fn execute_next_client_action_resumes_remote_only_file_from_transfer_ledger() {
    run_remote_only_materialization_fixture(
        "client/execute_next_client_action_resumes_remote_only_file_from_transfer_ledger.yaml",
        "client-remote-only-materialization-resume",
    );
}

#[test]
fn execute_next_client_action_multitick_remote_only_file_download_survives_restart() {
    let scenario = load_fixture(
        "client/execute_next_client_action_multitick_remote_only_file_download_survives_restart.yaml",
    );
    let mut initial: MaterializeInitialState = scenario.decode_initial().expect("decode initial");
    let actions: Vec<MaterializeFixtureAction> = scenario.decode_actions().expect("decode actions");
    let mut expect: MaterializeExpectedState = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-only-materialization-multitick");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let local_root = temp_dir.path().join("local");
    let remote_seed_root = temp_dir.path().join("remote-seed");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&local_root).expect("create local root");
    fs::create_dir_all(&remote_seed_root).expect("create remote seed root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    let first_execute = actions[0].execute().expect("first execute action");
    assert!(actions[1].is_restart(), "restart should be second");
    let second_execute = actions[2].execute().expect("second execute action");
    assert!(actions[3].is_restart(), "restart should be fourth");
    let planner_tick = actions[4].planner().expect("planner action fifth");

    let target_relative = first_execute
        .source_path_relative
        .as_deref()
        .expect("fixture should include target path");
    let target_path = local_root.join(target_relative);
    fs::create_dir_all(target_path.parent().expect("target parent")).expect("create target parent");

    let seeded_remote = seed_remote_content(
        &store,
        &initial.remote_state.namespace_id,
        &initial.remote_file,
        &remote_seed_root,
    );
    fill_materialization_expectations(&mut initial, &mut expect, &seeded_remote);
    seed_remote_only_state(
        &db_path,
        &initial.remote_state,
        &initial.local_state,
        &initial.planned_actions,
        initial.transfer_ledger.as_ref(),
        &seeded_remote,
        &initial.remote_file,
        &target_path,
        initial.stale_stage_bytes_utf8.as_deref(),
    );

    let first = run_execute_next_client_action(
        &db_path,
        &store,
        Some(target_path.as_path()),
        &first_execute,
    )
    .expect("first action should be scheduled");
    let first_transfer = match first {
        NextClientAction::ExecutedDownloadRemoteEdit(DownloadRemoteEditExecution::Progressed(
            progressed,
        )) => progressed.transfer,
        other => panic!("expected progressed download on first tick, got {other:?}"),
    };
    assert_eq!(first_transfer.block_index, 1);
    assert_eq!(first_transfer.block_count, 2);

    let db = SqliteStateDb::open(&db_path).expect("reopen DB after first tick");
    assert_eq!(
        db.load_transfer_ledger_for_inode(
            &initial.remote_state.namespace_id,
            initial.remote_state.inode_id,
            TransferDirection::Download,
        )
        .expect("load download transfer ledger after first tick"),
        Some(first_transfer.clone())
    );
    assert!(
        !target_path.exists(),
        "target file should not be published before terminal block"
    );
    drop(db);

    let second = run_execute_next_client_action(
        &db_path,
        &store,
        Some(target_path.as_path()),
        &second_execute,
    )
    .expect("second action should be scheduled");
    match second {
        NextClientAction::ExecutedDownloadRemoteEdit(DownloadRemoteEditExecution::Completed(
            download,
        )) => {
            assert_eq!(
                download.downloaded_file_digest_sha256,
                expect
                    .downloaded_file_digest_sha256
                    .clone()
                    .expect("expected downloaded file digest"),
            );
        }
        other => panic!("expected completed download on second tick, got {other:?}"),
    }

    let mut db = SqliteStateDb::open(&db_path).expect("reopen client state DB after second tick");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan converged inode after restart");

    assert_eq!(planner_result, expect.planner_result);
    assert_eq!(
        db.load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load converged views"),
        FileSyncViews {
            namespace_id: planner_tick.namespace_id.clone(),
            inode_id: planner_tick.inode_id,
            remote: Some(expect.remote_state.clone()),
            local: Some(expect.local_state.clone()),
            sync_anchor: Some(expect.sync_anchor.clone()),
        }
    );
}

#[test]
fn execute_next_client_action_stale_download_stage_resets_and_records_issue() {
    let scenario = load_fixture(
        "client/execute_next_client_action_stale_download_stage_resets_and_records_issue.yaml",
    );
    let mut initial: MaterializeInitialState = scenario.decode_initial().expect("decode initial");
    let actions: Vec<MaterializeFixtureAction> = scenario.decode_actions().expect("decode actions");
    let expect: MaterializeProgressExpectedState = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-only-materialization-reset");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let local_root = temp_dir.path().join("local");
    let remote_seed_root = temp_dir.path().join("remote-seed");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&local_root).expect("create local root");
    fs::create_dir_all(&remote_seed_root).expect("create remote seed root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    let execute = actions[0].execute().expect("execute action first");
    let target_relative = execute
        .source_path_relative
        .as_deref()
        .expect("fixture should include target path");
    let target_path = local_root.join(target_relative);
    fs::create_dir_all(target_path.parent().expect("target parent")).expect("create target parent");

    let seeded_remote = seed_remote_content(
        &store,
        &initial.remote_state.namespace_id,
        &initial.remote_file,
        &remote_seed_root,
    );
    fill_remote_state_digests(&mut initial.remote_state, &seeded_remote);
    seed_remote_only_state(
        &db_path,
        &initial.remote_state,
        &initial.local_state,
        &initial.planned_actions,
        initial.transfer_ledger.as_ref(),
        &seeded_remote,
        &initial.remote_file,
        &target_path,
        initial.stale_stage_bytes_utf8.as_deref(),
    );

    let executed =
        run_execute_next_client_action(&db_path, &store, Some(target_path.as_path()), &execute)
            .expect("one action should be scheduled");
    let transfer = match executed {
        NextClientAction::ExecutedDownloadRemoteEdit(DownloadRemoteEditExecution::Progressed(
            progressed,
        )) => progressed.transfer,
        other => panic!("expected progressed download after reset, got {other:?}"),
    };
    assert_eq!(transfer.block_index, expect.transfer_ledger.block_index);
    assert_eq!(transfer.block_count, expect.transfer_ledger.block_count);

    let db = SqliteStateDb::open(&db_path).expect("reopen DB after reset");
    let issues = db
        .load_conflicts_and_errors(
            &initial.remote_state.namespace_id,
            initial.remote_state.inode_id,
        )
        .expect("load transfer reset issue");
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].kind, expect.issue.kind);
    assert_eq!(
        issues[0].detail_json["reason"].as_str(),
        Some(expect.issue.reason.as_str())
    );
    assert!(
        !target_path.exists(),
        "target file should still be absent after non-terminal tick"
    );
}

fn run_remote_only_materialization_fixture(relative_path: &str, temp_label: &str) {
    let scenario = load_fixture(relative_path);
    let mut initial: MaterializeInitialState = scenario.decode_initial().expect("decode initial");
    let actions: Vec<MaterializeFixtureAction> = scenario.decode_actions().expect("decode actions");
    let mut expect: MaterializeExpectedState = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new(temp_label);
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let local_root = temp_dir.path().join("local");
    let remote_seed_root = temp_dir.path().join("remote-seed");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&local_root).expect("create local root");
    fs::create_dir_all(&remote_seed_root).expect("create remote seed root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    let execute = actions[0].execute().expect("execute action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let planner_tick = actions[2].planner().expect("planner action third");

    let target_relative = execute
        .source_path_relative
        .as_deref()
        .expect("materialization fixture should include target path");
    let target_path = local_root.join(target_relative);
    fs::create_dir_all(target_path.parent().expect("target parent")).expect("create target parent");

    let seeded_remote = seed_remote_content(
        &store,
        &initial.remote_state.namespace_id,
        &initial.remote_file,
        &remote_seed_root,
    );
    fill_materialization_expectations(&mut initial, &mut expect, &seeded_remote);

    seed_remote_only_state(
        &db_path,
        &initial.remote_state,
        &initial.local_state,
        &initial.planned_actions,
        initial.transfer_ledger.as_ref(),
        &seeded_remote,
        &initial.remote_file,
        &target_path,
        initial.stale_stage_bytes_utf8.as_deref(),
    );

    let executed =
        run_execute_next_client_action(&db_path, &store, Some(target_path.as_path()), &execute)
            .expect("one action should be scheduled");

    let download = match executed {
        NextClientAction::ExecutedDownloadRemoteEdit(DownloadRemoteEditExecution::Completed(
            result,
        )) => result,
        NextClientAction::ExecutedDownloadRemoteEdit(DownloadRemoteEditExecution::Progressed(
            progress,
        )) => {
            panic!("expected completed download_remote_edit, got {progress:?}")
        }
        other => panic!("expected executed download_remote_edit, got {other:?}"),
    };

    assert_eq!(
        download.downloaded_content_manifest_digest,
        expect
            .downloaded_content_manifest_digest
            .as_deref()
            .expect("expected downloaded manifest digest")
    );
    assert_eq!(
        download.downloaded_file_digest_sha256,
        expect
            .downloaded_file_digest_sha256
            .as_deref()
            .expect("expected downloaded file digest")
    );
    assert_eq!(
        download.applied,
        expect
            .applied_inode_mutation
            .clone()
            .unwrap_or(AppliedInodeMutation {
                namespace_id: planner_tick.namespace_id.clone(),
                inode_id: planner_tick.inode_id,
            })
    );

    let mut db = SqliteStateDb::open(&db_path).expect("reopen client state DB after execute");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan materialized inode after restart");

    assert_eq!(planner_result, expect.planner_result);
    assert_eq!(
        db.load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load converged views"),
        FileSyncViews {
            namespace_id: planner_tick.namespace_id.clone(),
            inode_id: planner_tick.inode_id,
            remote: Some(expect.remote_state.clone()),
            local: Some(expect.local_state.clone()),
            sync_anchor: Some(expect.sync_anchor.clone()),
        }
    );
    assert_eq!(
        db.load_planned_action(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load planned action after materialization"),
        if expect.planned_action_cleared {
            None
        } else {
            Some(
                initial
                    .planned_actions
                    .iter()
                    .find(|row| {
                        row.namespace_id == planner_tick.namespace_id
                            && row.inode_id == planner_tick.inode_id
                    })
                    .expect("fixture should include target planned action")
                    .clone(),
            )
        }
    );
    assert_eq!(
        fs::read_to_string(&target_path).expect("read materialized local file"),
        expect.local_file_content_utf8
    );
    assert_eq!(
        read_directory_entries(target_path.parent().expect("target parent")),
        expect.local_parent_entries,
    );
    if expect.transfer_ledger_cleared {
        assert_eq!(
            db.load_transfer_ledger_for_inode(
                &planner_tick.namespace_id,
                planner_tick.inode_id,
                TransferDirection::Download,
            )
            .expect("load transfer ledger after materialization"),
            None
        );
    }
    if expect.stage_file_cleared {
        assert!(
            !stage_path_for_target(&target_path).exists(),
            "stage file should be cleared after successful materialization"
        );
    }
}

fn run_execute_next_client_action(
    db_path: &Path,
    store: &LocalFsStore,
    inode_path: Option<&Path>,
    action: &ExecuteNextClientActionAction,
) -> Option<NextClientAction> {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    execute_next_client_action(
        &mut db,
        store,
        |_client_file_id| None,
        |_namespace_id, _inode_id| inode_path.map(Path::to_path_buf),
        action.uploaded_at_ms,
        action.created_at_ms,
        |_request| panic!("remote-only materialization should not dispatch a mutation request"),
    )
    .expect("execute next client action")
}

fn seed_remote_only_state(
    db_path: &Path,
    remote_state: &RemoteFileStateRow,
    local_state: &LocalFileStateRow,
    planned_actions: &[PlannedActionRow],
    transfer_ledger: Option<&FixtureTransferLedgerSeed>,
    seeded_remote: &SeededRemoteFile,
    remote_file: &FixtureRemoteFile,
    target_path: &Path,
    stale_stage_bytes_utf8: Option<&str>,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-remote-only-state", |tx| {
        tx.upsert_remote_file(remote_state)?;
        tx.upsert_local_file(local_state)?;
        for planned_action in planned_actions {
            tx.upsert_planned_action(planned_action)?;
        }
        if let Some(transfer_ledger) = transfer_ledger {
            tx.upsert_transfer_ledger(&TransferLedgerRow {
                namespace_id: remote_state.namespace_id.clone(),
                inode_id: remote_state.inode_id,
                transfer_id: download_transfer_id(
                    &remote_state.namespace_id,
                    remote_state.inode_id,
                    &seeded_remote.content_manifest_digest,
                ),
                direction: transfer_ledger.direction,
                object_key: seeded_remote.manifest_object_key.clone(),
                block_index: transfer_ledger.block_index,
                block_count: transfer_ledger.block_count,
                state: transfer_ledger.state,
                updated_at_ms: transfer_ledger.updated_at_ms,
            })?;
        }
        Ok(())
    })
    .expect("seed remote-only state");

    if let Some(transfer_ledger) = transfer_ledger {
        let mut stage_bytes = stale_stage_bytes_utf8
            .map(|bytes| bytes.as_bytes().to_vec())
            .unwrap_or_default();
        if stale_stage_bytes_utf8.is_none() {
            for block in remote_file
                .block_bytes()
                .iter()
                .take(usize::try_from(transfer_ledger.block_index).expect("block index should fit"))
            {
                stage_bytes.extend_from_slice(block);
            }
        }
        let stage_path = stage_path_for_target(target_path);
        fs::create_dir_all(stage_path.parent().expect("stage parent"))
            .expect("create stage parent");
        fs::write(&stage_path, &stage_bytes).expect("seed stage file");
    }
}

fn seed_remote_content(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    remote_file: &FixtureRemoteFile,
    remote_seed_root: &Path,
) -> SeededRemoteFile {
    if let Some(content_utf8) = &remote_file.content_utf8 {
        let remote_seed_path = write_remote_seed_file(
            remote_seed_root,
            Path::new("remote/welcome.txt"),
            content_utf8,
        );
        let uploaded = upload_small_file_from_path(store, namespace_id, &remote_seed_path)
            .expect("upload remote content into object store");
        return SeededRemoteFile::from_uploaded(&uploaded);
    }

    let mut file_bytes = Vec::new();
    let mut manifest_blocks = Vec::new();
    for block_bytes in remote_file.block_bytes() {
        assert!(
            u64::try_from(block_bytes.len()).expect("block length should fit in u64")
                <= CONTENT_BLOCK_SIZE_BYTES,
            "fixture blocks should stay within the content block size"
        );
        let content_digest_sha256 = sha256_digest(&block_bytes);
        let object_key = blob(namespace_id.as_str(), &content_digest_sha256);
        store
            .put_overwrite(&object_key, &block_bytes)
            .expect("seed remote block object");
        manifest_blocks.push(ContentBlockDescriptor {
            content_digest_sha256,
            plaintext_size_bytes: u64::try_from(block_bytes.len())
                .expect("block length should fit in u64"),
        });
        file_bytes.extend_from_slice(&block_bytes);
    }

    let manifest_envelope = ContentManifestEnvelope::from_payload(ContentManifestPayload {
        namespace_id: namespace_id.clone(),
        file_size_bytes: u64::try_from(file_bytes.len()).expect("file length should fit in u64"),
        file_digest_sha256: sha256_digest(&file_bytes),
        block_size_bytes: CONTENT_BLOCK_SIZE_BYTES,
        blocks: manifest_blocks,
    })
    .expect("build content manifest");
    let manifest_bytes =
        encode_content_manifest_json(&manifest_envelope).expect("encode content manifest");
    let content_manifest_digest =
        content_manifest_digest_sha256(&manifest_envelope).expect("digest content manifest");
    let manifest_object_key = content_manifest(namespace_id.as_str(), &content_manifest_digest);
    store
        .put_overwrite(&manifest_object_key, &manifest_bytes)
        .expect("seed content manifest object");

    SeededRemoteFile {
        file_digest_sha256: manifest_envelope.payload.file_digest_sha256.clone(),
        content_manifest_digest,
        manifest_object_key,
    }
}

fn fill_materialization_expectations(
    initial: &mut MaterializeInitialState,
    expect: &mut MaterializeExpectedState,
    seeded_remote: &SeededRemoteFile,
) {
    fill_remote_state_digests(&mut initial.remote_state, seeded_remote);
    fill_remote_state_digests(&mut expect.remote_state, seeded_remote);
    fill_local_state_digest(&mut expect.local_state, seeded_remote);
    fill_sync_anchor_digests(&mut expect.sync_anchor, seeded_remote);

    if let Some(expected) = &expect.downloaded_content_manifest_digest {
        assert_eq!(expected, &seeded_remote.content_manifest_digest);
    } else {
        expect.downloaded_content_manifest_digest =
            Some(seeded_remote.content_manifest_digest.clone());
    }
    if let Some(expected) = &expect.downloaded_file_digest_sha256 {
        assert_eq!(expected, &seeded_remote.file_digest_sha256);
    } else {
        expect.downloaded_file_digest_sha256 = Some(seeded_remote.file_digest_sha256.clone());
    }
}

fn fill_remote_state_digests(state: &mut RemoteFileStateRow, seeded_remote: &SeededRemoteFile) {
    match &state.content_digest {
        Some(digest) => assert_eq!(digest, &seeded_remote.file_digest_sha256),
        None => state.content_digest = Some(seeded_remote.file_digest_sha256.clone()),
    }
    match &state.content_manifest_digest {
        Some(digest) => assert_eq!(digest, &seeded_remote.content_manifest_digest),
        None => state.content_manifest_digest = Some(seeded_remote.content_manifest_digest.clone()),
    }
}

fn fill_local_state_digest(state: &mut LocalFileStateRow, seeded_remote: &SeededRemoteFile) {
    match &state.content_digest {
        Some(digest) => assert_eq!(digest, &seeded_remote.file_digest_sha256),
        None => state.content_digest = Some(seeded_remote.file_digest_sha256.clone()),
    }
}

fn fill_sync_anchor_digests(state: &mut SyncAnchorRow, seeded_remote: &SeededRemoteFile) {
    match &state.content_digest {
        Some(digest) => assert_eq!(digest, &seeded_remote.file_digest_sha256),
        None => state.content_digest = Some(seeded_remote.file_digest_sha256.clone()),
    }
    match &state.content_manifest_digest {
        Some(digest) => assert_eq!(digest, &seeded_remote.content_manifest_digest),
        None => state.content_manifest_digest = Some(seeded_remote.content_manifest_digest.clone()),
    }
}

fn write_remote_seed_file(root: &Path, relative_path: &Path, content_utf8: &str) -> PathBuf {
    let path = root.join(relative_path);
    fs::create_dir_all(path.parent().expect("seed file parent")).expect("create seed parent");
    fs::write(&path, content_utf8.as_bytes()).expect("write remote seed file");
    path
}

fn read_directory_entries(path: &Path) -> Vec<String> {
    let mut entries = fs::read_dir(path)
        .expect("read directory")
        .map(|entry| {
            entry
                .expect("directory entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

fn stage_path_for_target(target_path: &Path) -> PathBuf {
    let file_name = target_path
        .file_name()
        .expect("target path should include a file name")
        .to_string_lossy()
        .into_owned();
    target_path
        .parent()
        .expect("target path should include a parent directory")
        .join(format!(".{file_name}.loon-stage"))
}

fn download_transfer_id(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    content_manifest_digest: &str,
) -> String {
    format!(
        "download:{}:{}:{}",
        namespace_id.as_str(),
        inode_id.0,
        content_manifest_digest
    )
}

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SeededRemoteFile {
    file_digest_sha256: String,
    content_manifest_digest: String,
    manifest_object_key: String,
}

impl SeededRemoteFile {
    fn from_uploaded(uploaded: &UploadedContent) -> Self {
        Self {
            file_digest_sha256: uploaded.file_digest_sha256.clone(),
            content_manifest_digest: uploaded.content_manifest_digest.clone(),
            manifest_object_key: uploaded.manifest_object_key.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct MaterializeInitialState {
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    remote_file: FixtureRemoteFile,
    planned_actions: Vec<PlannedActionRow>,
    #[serde(default)]
    transfer_ledger: Option<FixtureTransferLedgerSeed>,
    #[serde(default)]
    stale_stage_bytes_utf8: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MaterializeExpectedState {
    #[serde(default)]
    downloaded_content_manifest_digest: Option<String>,
    #[serde(default)]
    downloaded_file_digest_sha256: Option<String>,
    #[serde(default)]
    applied_inode_mutation: Option<AppliedInodeMutation>,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    planned_action_cleared: bool,
    local_file_content_utf8: String,
    #[serde(default)]
    local_parent_entries: Vec<String>,
    #[serde(default)]
    transfer_ledger_cleared: bool,
    #[serde(default)]
    stage_file_cleared: bool,
    planner_result: PlannedActionRecord,
}

#[derive(Debug, Deserialize)]
struct MaterializeProgressExpectedState {
    transfer_ledger: FixtureTransferLedgerSeed,
    issue: RawTransferResetIssueExpect,
    planned_action_retained: bool,
    local_file_absent: bool,
}

#[derive(Debug, Deserialize)]
struct RawTransferResetIssueExpect {
    kind: String,
    reason: String,
}

#[derive(Debug, Deserialize)]
struct FixtureRemoteFile {
    #[serde(default)]
    content_utf8: Option<String>,
    #[serde(default)]
    blocks_utf8: Vec<String>,
}

impl FixtureRemoteFile {
    fn block_bytes(&self) -> Vec<Vec<u8>> {
        if !self.blocks_utf8.is_empty() {
            return self
                .blocks_utf8
                .iter()
                .map(|block| block.as_bytes().to_vec())
                .collect();
        }

        match &self.content_utf8 {
            Some(content_utf8) => vec![content_utf8.as_bytes().to_vec()],
            None => Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureTransferLedgerSeed {
    direction: TransferDirection,
    block_index: u64,
    block_count: u64,
    state: TransferState,
    updated_at_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum MaterializeFixtureAction {
    ExecuteNextClientAction {
        execute_next_client_action: ExecuteNextClientActionAction,
    },
    RestartClientStateDb {
        restart_client_state_db: bool,
    },
    PlannerTick {
        planner_tick: PlannerTickAction,
    },
}

impl MaterializeFixtureAction {
    fn execute(&self) -> Option<ExecuteNextClientActionAction> {
        match self {
            Self::ExecuteNextClientAction {
                execute_next_client_action,
            } => Some(execute_next_client_action.clone()),
            _ => None,
        }
    }

    fn is_restart(&self) -> bool {
        matches!(
            self,
            Self::RestartClientStateDb {
                restart_client_state_db: true
            }
        )
    }

    fn planner(&self) -> Option<PlannerTickAction> {
        match self {
            Self::PlannerTick { planner_tick } => Some(planner_tick.clone()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ExecuteNextClientActionAction {
    source_path_relative: Option<PathBuf>,
    uploaded_at_ms: u64,
    created_at_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct PlannerTickAction {
    namespace_id: NamespaceId,
    inode_id: InodeId,
    now_ms: u64,
}

type TestDir = loon_testkit::tempdir::TestDir;
