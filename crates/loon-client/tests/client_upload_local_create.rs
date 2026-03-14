#[path = "common/support.rs"]
mod support;

use loon_client::executor::{
    execute_upload_local_create_from_path, ExecuteUploadLocalCreateError, ExecutedUploadLocalCreate,
};
use loon_client::planner::PlannedActionRecord;
use loon_client::state_db::{
    BoundLocalOnlyFile, LocalFileStateRow, LocalOnlyFileStateRow, LocalOnlyPlannedActionRow,
    LocalOnlyUploadRow, RemoteFileStateRow, SqliteStateDb, StateDbError, SyncAnchorRow,
};
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::{namespace_head, namespace_lease};
use loon_objectstore::ObjectStore;
use loon_server::mutation::{execute_client_mutation, ClientMutationExecutionParams};
use loon_testkit::scenario::Scenario;
use loon_types::{
    ClientMutationOp, ClientMutationRequest, ClientMutationResponse, ControlObjectKind, HeadState,
    HeadStateEnvelope, InodeId, LeaseState, LeaseStateEnvelope, NamespaceId,
};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn upload_local_create_from_path_binds_and_restarts_converged() {
    let scenario =
        load_fixture("client/upload_local_create_from_path_binds_and_restarts_converged.yaml");
    let initial: InitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<FixtureAction> = scenario.decode_actions().expect("decode actions");
    let expect: ExpectedState = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new("client-upload-local-create");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    seed_client_state(&db_path, &initial);

    assert_eq!(
        actions.len(),
        3,
        "fixture should contain execute, restart, planner"
    );
    let execute = actions[0].execute().expect("execute action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let planner_tick = actions[2].planner().expect("planner action third");

    let source_path = write_source_file(&source_root, &initial.local_file);
    let executed = run_execute(
        &db_path,
        &store,
        &initial.local_only_state.client_file_id,
        &source_path,
        &execute,
    );

    assert_eq!(executed.upload_reused, expect.upload_reused);
    assert_eq!(
        executed.ensured_upload,
        Some(LocalOnlyUploadRow {
            client_file_id: initial.local_only_state.client_file_id.clone(),
            namespace_id: NamespaceId::from("ns-1"),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
            uploaded_at_ms: execute.uploaded_at_ms,
        })
    );
    assert_eq!(
        executed.dispatched.pending.client_request_id,
        expect.pending_request_id
    );
    assert_eq!(
        executed.dispatched.request,
        expect.request.clone().into_request()
    );
    assert_eq!(
        executed.dispatched.response,
        expect.mutation_response.clone()
    );
    assert_eq!(
        executed.dispatched.bound_identity,
        expect.bound_identity.clone()
    );

    let mut db = SqliteStateDb::open(&db_path).expect("reopen client state DB after execute");
    let planner_result = loon_client::planner::plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan bound inode after restart");

    assert_eq!(planner_result, expect.planner_result);
    assert_eq!(
        db.load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load converged views"),
        loon_client::state_db::FileSyncViews {
            namespace_id: planner_tick.namespace_id.clone(),
            inode_id: planner_tick.inode_id,
            remote: Some(expect.remote_state.clone()),
            local: Some(expect.local_state.clone()),
            sync_anchor: Some(expect.sync_anchor.clone()),
        }
    );
    assert_eq!(
        db.load_local_only_file(&initial.local_only_state.client_file_id)
            .expect("load temp local-only state"),
        if expect.local_only_state_cleared {
            None
        } else {
            Some(initial.local_only_state.clone())
        }
    );
    assert_eq!(
        db.load_planned_local_only_action(&initial.local_only_state.client_file_id)
            .expect("load temp planned action"),
        if expect.planned_local_only_action_cleared {
            None
        } else {
            Some(initial.planned_local_only_action.clone())
        }
    );
    assert_eq!(
        db.load_pending_client_mutation(&expect.pending_request_id)
            .expect("load pending mutation"),
        if expect.pending_mutation_cleared {
            None
        } else {
            panic!("fixture currently expects pending mutation to clear");
        }
    );

    let error = execute_upload_local_create_from_path(
        &mut db,
        &store,
        &initial.local_only_state.client_file_id,
        &source_path,
        execute.uploaded_at_ms,
        execute.created_at_ms,
        |_request| unreachable!("second call should fail before dispatch"),
    )
    .expect_err("second call should return explicit temp-identity error");
    assert!(matches!(
        error,
        ExecuteUploadLocalCreateError::StateDb(StateDbError::LocalOnlyFileMissing { .. })
    ));
}

#[test]
fn upload_local_create_retry_reuses_pending_request_without_rereading_source_path() {
    let scenario =
        load_fixture("client/upload_local_create_from_path_binds_and_restarts_converged.yaml");
    let initial: InitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<FixtureAction> = scenario.decode_actions().expect("decode actions");
    let temp_dir = TestDir::new("client-upload-local-create-retry");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    seed_client_state(&db_path, &initial);

    let execute = actions[0].execute().expect("execute action first");
    let source_path = write_source_file(&source_root, &initial.local_file);

    {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        let error = execute_upload_local_create_from_path(
            &mut db,
            &store,
            &initial.local_only_state.client_file_id,
            &source_path,
            execute.uploaded_at_ms,
            execute.created_at_ms,
            |_request| Err("temporary dispatch failure".to_owned()),
        )
        .expect_err("first dispatch should fail");
        let message = error.to_string();
        assert!(message.contains("temporary dispatch failure"));
    }

    fs::remove_file(&source_path).expect("remove source path before retry");

    let executed = run_execute(
        &db_path,
        &store,
        &initial.local_only_state.client_file_id,
        &source_path,
        &execute,
    );

    assert!(executed.upload_reused);
    assert_eq!(
        executed.dispatched.pending.client_request_id,
        "client-req-00000000000000000001"
    );
    assert_eq!(
        executed.dispatched.request.client_request_id,
        "client-req-00000000000000000001"
    );
}

fn run_execute(
    db_path: &Path,
    store: &LocalFsStore,
    client_file_id: &loon_client::state_db::ClientFileId,
    source_path: &Path,
    action: &ExecuteUploadLocalCreateAction,
) -> ExecutedUploadLocalCreate {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    execute_upload_local_create_from_path(
        &mut db,
        store,
        client_file_id,
        source_path,
        action.uploaded_at_ms,
        action.created_at_ms,
        |request| {
            execute_client_mutation(
                store,
                request,
                &ClientMutationExecutionParams {
                    writer_id: action.writer_id.clone(),
                    writer_version: action.writer_version.clone(),
                    now_ms: action.now_ms,
                    metadata_state: support::server_metadata_for_request(request),
                },
            )
            .map(|executed| executed.response)
            .map_err(|err| err.to_string())
        },
    )
    .expect("execute upload-local-create from path")
}

fn write_source_file(source_root: &Path, local_file: &FixtureLocalFile) -> PathBuf {
    let source_path = source_root.join(&local_file.relative_path);
    fs::create_dir_all(source_path.parent().expect("source file parent"))
        .expect("create source file parent");
    fs::write(&source_path, local_file.content_utf8.as_bytes()).expect("write source file");
    source_path
}

#[derive(Debug, Deserialize)]
struct InitialState {
    local_only_state: LocalOnlyFileStateRow,
    local_file: FixtureLocalFile,
    planned_local_only_action: LocalOnlyPlannedActionRow,
    head: HeadState,
    lease: LeaseState,
}

#[derive(Debug, Deserialize)]
struct ExpectedState {
    upload_reused: bool,
    pending_request_id: String,
    request: ExpectedRequest,
    mutation_response: ClientMutationResponse,
    bound_identity: BoundLocalOnlyFile,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    local_only_state_cleared: bool,
    planned_local_only_action_cleared: bool,
    pending_mutation_cleared: bool,
    planner_result: PlannedActionRecord,
}

#[derive(Debug, Deserialize)]
struct FixtureLocalFile {
    relative_path: PathBuf,
    content_utf8: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FixtureAction {
    ExecuteUploadLocalCreateFromPath {
        execute_upload_local_create_from_path: ExecuteUploadLocalCreateAction,
    },
    RestartClientStateDb {
        restart_client_state_db: bool,
    },
    PlannerTick {
        planner_tick: PlannerTickAction,
    },
}

impl FixtureAction {
    fn execute(&self) -> Option<ExecuteUploadLocalCreateAction> {
        match self {
            Self::ExecuteUploadLocalCreateFromPath {
                execute_upload_local_create_from_path,
            } => Some(execute_upload_local_create_from_path.clone()),
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
struct ExecuteUploadLocalCreateAction {
    uploaded_at_ms: u64,
    created_at_ms: u64,
    writer_id: String,
    writer_version: String,
    now_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct PlannerTickAction {
    namespace_id: NamespaceId,
    inode_id: InodeId,
    now_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct ExpectedRequest {
    namespace_id: NamespaceId,
    client_request_id: String,
    op: ExpectedRequestOp,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum ExpectedRequestOp {
    CreateFile { create_file: ExpectedCreateFileOp },
}

#[derive(Debug, Clone, Deserialize)]
struct ExpectedCreateFileOp {
    parent_inode_id: InodeId,
    display_name: String,
    content_manifest_digest: String,
}

impl ExpectedRequest {
    fn into_request(self) -> ClientMutationRequest {
        let op = match self.op {
            ExpectedRequestOp::CreateFile { create_file } => ClientMutationOp::CreateFile {
                parent_inode_id: create_file.parent_inode_id,
                display_name: create_file.display_name,
                content_manifest_digest: create_file.content_manifest_digest,
            },
        };

        ClientMutationRequest {
            namespace_id: self.namespace_id,
            client_request_id: self.client_request_id,
            op,
        }
    }
}

fn seed_client_state(db_path: &Path, initial: &InitialState) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-upload-local-create-initial-state", |tx| {
        tx.upsert_local_only_file(&initial.local_only_state)?;
        tx.upsert_planned_local_only_action(&initial.planned_local_only_action)?;
        Ok(())
    })
    .expect("seed initial client state");
}

fn seed_head_and_lease(store: &LocalFsStore, head: &HeadState, lease: &LeaseState) {
    let head_envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        "loon-server-test",
        head.clone(),
    )
    .expect("encode head envelope");
    let head_bytes = serde_json::to_vec(&head_envelope).expect("serialize head envelope");
    store
        .put_if_absent(&namespace_head(head.namespace_id.as_str()), &head_bytes)
        .expect("seed head object");

    let lease_envelope = LeaseStateEnvelope::from_state(
        ControlObjectKind::NamespaceLease,
        "loon-server-test",
        lease.clone(),
    )
    .expect("encode lease envelope");
    let lease_bytes = serde_json::to_vec(&lease_envelope).expect("serialize lease envelope");
    store
        .put_if_absent(&namespace_lease(lease.namespace_id.as_str()), &lease_bytes)
        .expect("seed lease object");
}

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
}

type TestDir = loon_testkit::tempdir::TestDir;
