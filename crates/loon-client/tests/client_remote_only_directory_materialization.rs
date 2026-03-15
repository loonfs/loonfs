use loon_client::executor::{execute_next_client_action, NextClientAction};
use loon_client::planner::{plan_file, PlannedActionRecord};
use loon_client::state_db::{
    AppliedInodeMutation, FileSyncViews, LocalFileStateRow, PlannedActionRow, RemoteFileStateRow,
    SqliteStateDb, SyncAnchorRow,
};
use loon_objectstore::fs::LocalFsStore;
use loon_testkit::scenario::Scenario;
use loon_types::{InodeId, NamespaceId};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn execute_next_client_action_materializes_remote_only_directory() {
    let scenario =
        load_fixture("client/execute_next_client_action_materializes_remote_only_directory.yaml");
    let initial: MaterializeDirectoryInitialState =
        scenario.decode_initial().expect("decode initial");
    let actions: Vec<MaterializeDirectoryFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: MaterializeDirectoryExpectedState =
        scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-only-dir-materialization");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let local_root = temp_dir.path().join("local");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&local_root).expect("create local root");
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

    seed_remote_only_directory_state(
        &db_path,
        &initial.remote_state,
        &initial.local_state,
        &initial.planned_actions,
    );

    let executed =
        run_execute_next_client_action(&db_path, &store, Some(target_path.as_path()), &execute)
            .expect("one action should be scheduled");

    let materialized = match executed {
        NextClientAction::ExecutedMaterializeRemoteDir(result) => result,
        other => panic!("expected executed materialize_remote_dir, got {other:?}"),
    };

    assert_eq!(materialized.applied, expect.applied_inode_mutation);

    let mut db = SqliteStateDb::open(&db_path).expect("reopen client state DB after execute");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan materialized directory after restart");

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
    assert!(target_path.is_dir(), "materialized directory should exist");
    assert_eq!(
        read_directory_entries(target_path.parent().expect("target parent")),
        expect.local_parent_entries,
    );
    assert_eq!(
        read_directory_entries(&target_path),
        expect.created_dir_entries,
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
        |_request| panic!("remote-only dir materialization should not dispatch a mutation request"),
    )
    .expect("execute next client action")
}

fn seed_remote_only_directory_state(
    db_path: &Path,
    remote_state: &RemoteFileStateRow,
    local_state: &LocalFileStateRow,
    planned_actions: &[PlannedActionRow],
) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-remote-only-directory-state", |tx| {
        tx.upsert_remote_file(remote_state)?;
        tx.upsert_local_file(local_state)?;
        for planned_action in planned_actions {
            tx.upsert_planned_action(planned_action)?;
        }
        Ok(())
    })
    .expect("seed remote-only directory state");
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

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
}

#[derive(Debug, Deserialize)]
struct MaterializeDirectoryInitialState {
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    planned_actions: Vec<PlannedActionRow>,
}

#[derive(Debug, Deserialize)]
struct MaterializeDirectoryExpectedState {
    applied_inode_mutation: AppliedInodeMutation,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    planned_action_cleared: bool,
    #[serde(default)]
    local_parent_entries: Vec<String>,
    #[serde(default)]
    created_dir_entries: Vec<String>,
    planner_result: PlannedActionRecord,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum MaterializeDirectoryFixtureAction {
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

impl MaterializeDirectoryFixtureAction {
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
