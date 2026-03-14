use loon_client::executor::{dispatch_client_mutation_from_state, DispatchClientMutationError};
use loon_client::planner::{plan_file, PlannedActionRecord};
use loon_client::state_db::{
    BoundLocalOnlyFile, LocalFileStateRow, LocalOnlyFileStateRow, LocalOnlyPlannedActionRow,
    RemoteFileStateRow, SqliteStateDb, SyncAnchorRow,
};
use loon_client::upload::upload_small_file_from_path;
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn client_dispatch_file_from_state_binds_and_restarts_converged() {
    run_fixture("client/client_dispatch_file_from_state_binds_and_restarts_converged.yaml");
}

#[test]
fn client_dispatch_dir_from_state_binds_and_restarts_converged() {
    run_fixture("client/client_dispatch_dir_from_state_binds_and_restarts_converged.yaml");
}

#[test]
fn dispatch_retry_reuses_pending_request_id_after_failure() {
    let scenario =
        load_fixture("client/client_dispatch_file_from_state_binds_and_restarts_converged.yaml");
    let initial: DispatchInitial = scenario.decode_initial().expect("decode initial state");
    let temp_dir = TestDir::new("client-dispatch-retry");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    seed_client_state(&db_path, &initial);
    record_local_only_upload_for_fixture(
        &db_path,
        &store,
        &source_root,
        &initial,
        1_700_000_104_000,
    );

    let first_error = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        dispatch_client_mutation_from_state(
            &mut db,
            &initial.local_only_state.client_file_id,
            1_700_000_106_000,
            |_request| Err("temporary dispatch failure".to_owned()),
        )
        .expect_err("first dispatch should fail")
    };

    match first_error {
        DispatchClientMutationError::DispatchFailed {
            client_request_id,
            message,
        } => {
            assert_eq!(client_request_id, "client-req-00000000000000000001");
            assert_eq!(message, "temporary dispatch failure");
        }
        other => panic!("expected dispatch failure, got {other:?}"),
    }

    {
        let db = SqliteStateDb::open(&db_path).expect("reopen client state DB after failure");
        let pending = db
            .load_pending_client_mutation_for_client_file(&initial.local_only_state.client_file_id)
            .expect("load pending mutation after failure")
            .expect("pending mutation should persist");
        assert_eq!(pending.client_request_id, "client-req-00000000000000000001");
        assert_eq!(
            pending.request.client_request_id,
            "client-req-00000000000000000001"
        );
    }

    let dispatched = {
        let mut db = SqliteStateDb::open(&db_path).expect("reopen client state DB for retry");
        dispatch_client_mutation_from_state(
            &mut db,
            &initial.local_only_state.client_file_id,
            1_700_000_106_999,
            |request| {
                execute_client_mutation(
                    &store,
                    request,
                    &ClientMutationExecutionParams {
                        writer_id: "writer-a".to_owned(),
                        writer_version: "loon-server-test".to_owned(),
                        now_ms: 1_700_000_107_000,
                    },
                )
                .map(|executed| executed.response)
                .map_err(|err| err.to_string())
            },
        )
        .expect("retry dispatch should succeed")
    };

    assert_eq!(
        dispatched.pending.client_request_id,
        "client-req-00000000000000000001"
    );
    assert_eq!(
        dispatched.request.client_request_id,
        "client-req-00000000000000000001"
    );

    let db = SqliteStateDb::open(&db_path).expect("reopen client state DB after success");
    assert_eq!(
        db.load_pending_client_mutation_for_client_file(&initial.local_only_state.client_file_id)
            .expect("load pending after success"),
        None
    );
}

fn run_fixture(relative_path: &str) {
    let scenario = load_fixture(relative_path);
    let initial: DispatchInitial = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<FixtureAction> = scenario.decode_actions().expect("decode actions");
    let expect: DispatchExpect = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new("client-dispatch-from-state");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    seed_client_state(&db_path, &initial);

    for action in &actions {
        if let Some(upload) = action.record_upload() {
            record_local_only_upload_for_fixture(
                &db_path,
                &store,
                &source_root,
                &initial,
                upload.uploaded_at_ms,
            );
        }
    }

    let dispatch = actions
        .iter()
        .find_map(FixtureAction::dispatch)
        .expect("dispatch action must be present");
    assert_eq!(
        actions.iter().filter(|action| action.is_restart()).count(),
        1,
        "fixture should contain one restart after dispatch",
    );
    let planner_tick = actions
        .iter()
        .find_map(FixtureAction::planner)
        .expect("planner action must be present");

    let dispatched = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        dispatch_client_mutation_from_state(
            &mut db,
            &initial.local_only_state.client_file_id,
            dispatch.created_at_ms,
            |request| {
                execute_client_mutation(
                    &store,
                    request,
                    &ClientMutationExecutionParams {
                        writer_id: dispatch.writer_id.clone(),
                        writer_version: dispatch.writer_version.clone(),
                        now_ms: dispatch.now_ms,
                    },
                )
                .map(|executed| executed.response)
                .map_err(|err| err.to_string())
            },
        )
        .expect("dispatch client mutation from state")
    };

    assert_eq!(
        dispatched.pending.client_request_id,
        expect.pending_request_id
    );
    assert_eq!(dispatched.request, expect.request.clone().into_request());
    assert_eq!(dispatched.response, expect.mutation_response.clone());
    assert_eq!(dispatched.bound_identity, expect.bound_identity.clone());

    let mut db = SqliteStateDb::open(&db_path).expect("reopen client state DB after dispatch");
    let planner_result = plan_file(
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
            .expect("load local-only state after dispatch"),
        if expect.local_only_state_cleared {
            None
        } else {
            Some(initial.local_only_state.clone())
        }
    );
    assert_eq!(
        db.load_planned_local_only_action(&initial.local_only_state.client_file_id)
            .expect("load local-only plan after dispatch"),
        if expect.planned_local_only_action_cleared {
            None
        } else {
            Some(initial.planned_local_only_action.clone())
        }
    );
    assert_eq!(
        db.load_pending_client_mutation(&expect.pending_request_id)
            .expect("load pending row after dispatch"),
        if expect.pending_mutation_cleared {
            None
        } else {
            panic!("fixture currently expects pending mutation to clear");
        }
    );
}

fn record_local_only_upload_for_fixture(
    db_path: &Path,
    store: &LocalFsStore,
    source_root: &Path,
    initial: &DispatchInitial,
    uploaded_at_ms: u64,
) {
    let local_file = initial
        .local_file
        .as_ref()
        .expect("file fixture must include local_file for upload");
    let source_path = source_root.join(&local_file.relative_path);
    fs::create_dir_all(source_path.parent().expect("source file parent"))
        .expect("create source file parent");
    fs::write(&source_path, local_file.content_utf8.as_bytes()).expect("write source file");
    let uploaded =
        upload_small_file_from_path(store, &initial.local_only_state.namespace_id, &source_path)
            .expect("upload local file");

    let mut db = SqliteStateDb::open(db_path).expect("open client state DB for upload row");
    db.record_local_only_upload(
        &initial.local_only_state.client_file_id,
        &uploaded,
        uploaded_at_ms,
    )
    .expect("record local-only upload");
}

#[derive(Debug, Deserialize)]
struct DispatchInitial {
    local_only_state: LocalOnlyFileStateRow,
    local_file: Option<FixtureLocalFile>,
    planned_local_only_action: LocalOnlyPlannedActionRow,
    head: HeadState,
    lease: LeaseState,
}

#[derive(Debug, Deserialize)]
struct DispatchExpect {
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
    RecordLocalOnlyUpload {
        record_local_only_upload: RecordLocalOnlyUploadAction,
    },
    DispatchClientMutationFromState {
        dispatch_client_mutation_from_state: DispatchClientMutationFromStateAction,
    },
    RestartClientStateDb {
        restart_client_state_db: bool,
    },
    PlannerTick {
        planner_tick: PlannerTickAction,
    },
}

impl FixtureAction {
    fn record_upload(&self) -> Option<RecordLocalOnlyUploadAction> {
        match self {
            Self::RecordLocalOnlyUpload {
                record_local_only_upload,
            } => Some(record_local_only_upload.clone()),
            _ => None,
        }
    }

    fn dispatch(&self) -> Option<DispatchClientMutationFromStateAction> {
        match self {
            Self::DispatchClientMutationFromState {
                dispatch_client_mutation_from_state,
            } => Some(dispatch_client_mutation_from_state.clone()),
            _ => None,
        }
    }

    fn planner(&self) -> Option<PlannerTickAction> {
        match self {
            Self::PlannerTick { planner_tick } => Some(planner_tick.clone()),
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
}

#[derive(Debug, Clone, Deserialize)]
struct RecordLocalOnlyUploadAction {
    uploaded_at_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct DispatchClientMutationFromStateAction {
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
    CreateDir { create_dir: ExpectedCreateDirOp },
}

#[derive(Debug, Clone, Deserialize)]
struct ExpectedCreateFileOp {
    parent_inode_id: InodeId,
    display_name: String,
    content_manifest_digest: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ExpectedCreateDirOp {
    parent_inode_id: InodeId,
    display_name: String,
}

impl ExpectedRequest {
    fn into_request(self) -> ClientMutationRequest {
        let op = match self.op {
            ExpectedRequestOp::CreateFile { create_file } => ClientMutationOp::CreateFile {
                parent_inode_id: create_file.parent_inode_id,
                display_name: create_file.display_name,
                content_manifest_digest: create_file.content_manifest_digest,
            },
            ExpectedRequestOp::CreateDir { create_dir } => ClientMutationOp::CreateDir {
                parent_inode_id: create_dir.parent_inode_id,
                display_name: create_dir.display_name,
            },
        };

        ClientMutationRequest {
            namespace_id: self.namespace_id,
            client_request_id: self.client_request_id,
            op,
        }
    }
}

fn seed_client_state(db_path: &Path, initial: &DispatchInitial) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-dispatch-initial-state", |tx| {
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
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/scenarios")
        .join(relative_path);
    Scenario::load(&path).unwrap_or_else(|err| panic!("load fixture {}: {err}", path.display()))
}

#[derive(Debug)]
struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(label: &str) -> Self {
        static NEXT_TEST_DIR_ID: AtomicU64 = AtomicU64::new(1);
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let id = NEXT_TEST_DIR_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "loondb-client-{label}-{}-{id}-{stamp}",
            std::process::id(),
        ));
        fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
