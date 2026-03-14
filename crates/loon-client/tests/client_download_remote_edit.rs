use loon_client::executor::{execute_next_client_action, NextClientAction};
use loon_client::planner::{plan_file, PlannedActionRecord};
use loon_client::state_db::{
    AppliedInodeMutation, FileSyncViews, LocalFileStateRow, PlannedActionRow, RemoteFileStateRow,
    SqliteStateDb, SyncAnchorRow,
};
use loon_client::upload::upload_small_file_from_path;
use loon_objectstore::fs::LocalFsStore;
use loon_testkit::scenario::Scenario;
use loon_types::{InodeId, NamespaceId};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn execute_next_client_action_download_remote_edit_updates_bound_inode() {
    let scenario = load_fixture(
        "client/execute_next_client_action_download_remote_edit_updates_bound_inode.yaml",
    );
    let initial: DownloadInitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<DownloadFixtureAction> = scenario.decode_actions().expect("decode actions");
    let expect: DownloadExpectedState = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new("client-download-remote-edit");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let local_root = temp_dir.path().join("local");
    let remote_seed_root = temp_dir.path().join("remote-seed");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&local_root).expect("create local root");
    fs::create_dir_all(&remote_seed_root).expect("create remote seed root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    assert_eq!(
        actions.len(),
        3,
        "fixture should contain execute, restart, planner"
    );
    let execute = actions[0].execute().expect("execute action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let planner_tick = actions[2].planner().expect("planner action third");

    let local_path = write_local_file(
        &local_root,
        execute
            .source_path_relative
            .as_deref()
            .expect("download fixture should include target path"),
        &initial.local_file.content_utf8,
    );
    assert_eq!(
        execute
            .source_path_relative
            .as_deref()
            .expect("download fixture should include target path"),
        initial.local_file.relative_path.as_path(),
        "fixture target path should match local_file.relative_path"
    );
    let remote_seed_path = write_local_file(
        &remote_seed_root,
        Path::new("remote/report.txt"),
        &initial.remote_file.content_utf8,
    );
    let uploaded = upload_small_file_from_path(
        &store,
        &initial.remote_state.namespace_id,
        &remote_seed_path,
    )
    .expect("upload remote content into object store");

    assert_eq!(
        initial.remote_state.content_digest.as_deref(),
        Some(uploaded.file_digest_sha256.as_str())
    );
    assert_eq!(
        initial.remote_state.content_manifest_digest.as_deref(),
        Some(uploaded.content_manifest_digest.as_str())
    );

    seed_bound_download_state(
        &db_path,
        &initial.remote_state,
        &initial.local_state,
        &initial.sync_anchor,
        &initial.planned_action,
    );

    let executed =
        run_execute_next_client_action(&db_path, &store, Some(local_path.as_path()), &execute)
            .expect("one action should be scheduled");

    let download = match executed {
        NextClientAction::ExecutedDownloadRemoteEdit(result) => result,
        other => panic!("expected executed download_remote_edit, got {other:?}"),
    };

    assert_eq!(
        download.downloaded_content_manifest_digest,
        expect.downloaded_content_manifest_digest
    );
    assert_eq!(
        download.downloaded_file_digest_sha256,
        expect.downloaded_file_digest_sha256
    );
    assert_eq!(download.applied, expect.applied_inode_mutation);

    let mut db = SqliteStateDb::open(&db_path).expect("reopen client state DB after execute");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan downloaded inode after restart");

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
            .expect("load planned action after download"),
        if expect.planned_action_cleared {
            None
        } else {
            Some(initial.planned_action.clone())
        }
    );
    assert_eq!(
        fs::read_to_string(&local_path).expect("read downloaded local file"),
        expect.local_file_content_utf8
    );
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
        |_request| panic!("download_remote_edit should not dispatch a mutation request"),
    )
    .expect("execute next client action")
}

fn seed_bound_download_state(
    db_path: &Path,
    remote_state: &RemoteFileStateRow,
    local_state: &LocalFileStateRow,
    sync_anchor: &SyncAnchorRow,
    planned_action: &PlannedActionRow,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-bound-download-state", |tx| {
        tx.upsert_remote_file(remote_state)?;
        tx.upsert_local_file(local_state)?;
        tx.upsert_sync_anchor(sync_anchor)?;
        tx.upsert_planned_action(planned_action)?;
        Ok(())
    })
    .expect("seed bound download state");
}

fn write_local_file(root: &Path, relative_path: &Path, content_utf8: &str) -> PathBuf {
    let path = root.join(relative_path);
    fs::create_dir_all(path.parent().expect("file parent")).expect("create file parent");
    fs::write(&path, content_utf8.as_bytes()).expect("write file");
    path
}

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
}

#[derive(Debug, Deserialize)]
struct DownloadInitialState {
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    local_file: FixtureFile,
    remote_file: FixtureRemoteFile,
    planned_action: PlannedActionRow,
}

#[derive(Debug, Deserialize)]
struct DownloadExpectedState {
    downloaded_content_manifest_digest: String,
    downloaded_file_digest_sha256: String,
    applied_inode_mutation: AppliedInodeMutation,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    planned_action_cleared: bool,
    local_file_content_utf8: String,
    planner_result: PlannedActionRecord,
}

#[derive(Debug, Deserialize)]
struct FixtureFile {
    relative_path: PathBuf,
    content_utf8: String,
}

#[derive(Debug, Deserialize)]
struct FixtureRemoteFile {
    content_utf8: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum DownloadFixtureAction {
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

impl DownloadFixtureAction {
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
