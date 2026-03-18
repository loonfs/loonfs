#[path = "common/support.rs"]
mod support;

use loon_client::executor::{
    execute_upload_local_create_from_path, ExecuteUploadLocalCreateError, ExecutedUploadLocalCreate,
};
use loon_client::planner::PlannedActionRecord;
use loon_client::state_db::{
    BoundLocalOnlyFile, ClientFileId, LocalFileStateRow, LocalOnlyFileStateRow,
    LocalOnlyPlannedActionRow, LocalOnlyTransferLedgerRow, LocalOnlyUploadRow, RemoteFileStateRow,
    SqliteStateDb, StateDbError, SyncAnchorRow, TransferDirection, TransferState,
};
use loon_client::upload::{upload_small_file_from_path, UploadError, UploadedContent};
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::{namespace_head, namespace_lease};
use loon_objectstore::ObjectStore;
use loon_server::mutation::{execute_client_mutation, ClientMutationExecutionParams};
use loon_testkit::scenario::Scenario;
use loon_types::{
    ChangeSeq, ClientMutationOp, ClientMutationRequest, ClientMutationResponse, ControlObjectKind,
    CreatedRemoteInode, HeadState, HeadStateEnvelope, InodeId, LeaseState, LeaseStateEnvelope,
    NamespaceId, RevisionNo, CONTENT_BLOCK_SIZE_BYTES,
};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn upload_local_create_from_path_binds_and_restarts_converged() {
    run_upload_local_create_fixture(
        "client/upload_local_create_from_path_binds_and_restarts_converged.yaml",
        "client-upload-local-create",
    );
}

#[test]
fn upload_local_create_from_path_resumes_from_temp_transfer_ledger() {
    run_upload_local_create_fixture(
        "client/upload_local_create_from_path_resumes_from_temp_transfer_ledger.yaml",
        "client-upload-local-create-resume",
    );
}

#[test]
fn upload_local_create_retry_reuses_pending_request_without_rereading_source_path() {
    let scenario =
        load_fixture("client/upload_local_create_from_path_binds_and_restarts_converged.yaml");
    let mut initial: InitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<FixtureAction> = scenario.decode_actions().expect("decode actions");
    let mut expect: ExpectedState = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new("client-upload-local-create-retry");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let scratch_store_root = temp_dir.path().join("scratch-objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&scratch_store_root).expect("create scratch object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");
    let scratch_store =
        LocalFsStore::new(&scratch_store_root).expect("create scratch local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    let execute = actions[0].execute().expect("execute action first");
    let source_path = write_source_file(&source_root, &initial.local_file);
    let expected_upload = upload_small_file_from_path(
        &scratch_store,
        &initial.local_only_state.namespace_id,
        &source_path,
    )
    .expect("build expected upload");
    fill_upload_expectations(&mut initial, &mut expect, &expected_upload);
    seed_client_state(&db_path, &initial, None);

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
        expect.pending_request_id
    );
    assert_eq!(
        executed.dispatched.request,
        expect.request.clone().into_request()
    );

    let db = SqliteStateDb::open(&db_path).expect("reopen client state DB after retry");
    assert_eq!(
        db.load_local_only_transfer_ledger(
            &initial.local_only_state.client_file_id,
            TransferDirection::Upload,
        )
        .expect("load temp upload transfer ledger"),
        None
    );
}

#[test]
fn upload_local_create_missing_file_records_temp_issue_and_clears_on_recovery() {
    let scenario = load_fixture("client/upload_local_create_missing_file_records_temp_issue.yaml");
    let initial: InitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<FixtureAction> = scenario.decode_actions().expect("decode actions");
    let expect: FailureExpectedState = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new("client-upload-local-create-missing-file");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    seed_client_state(&db_path, &initial, None);

    let execute = actions[0].execute().expect("execute action first");
    let missing_source_path = source_root.join(&initial.local_file.relative_path);

    {
        let mut db = SqliteStateDb::open(&db_path).expect("open client DB for missing-file case");
        let error = execute_upload_local_create_from_path(
            &mut db,
            &store,
            &initial.local_only_state.client_file_id,
            &missing_source_path,
            execute.uploaded_at_ms,
            execute.created_at_ms,
            |_request| unreachable!("missing-file failure should happen before dispatch"),
        )
        .expect_err("missing source file should fail");
        assert!(matches!(
            error,
            ExecuteUploadLocalCreateError::Upload(UploadError::LocalFileRead { .. })
        ));
    }

    let db = SqliteStateDb::open(&db_path).expect("reopen DB after missing-file failure");
    assert_eq!(
        db.load_planned_local_only_action(&initial.local_only_state.client_file_id)
            .expect("load planned local-only action after failure"),
        if expect.planned_local_only_action_retained {
            Some(initial.planned_local_only_action.clone())
        } else {
            None
        }
    );
    assert_eq!(
        db.load_local_only_upload(&initial.local_only_state.client_file_id)
            .expect("load local-only upload after failure"),
        None
    );
    let issues = db
        .load_local_only_conflicts_and_errors(&initial.local_only_state.client_file_id)
        .expect("load temp issue rows after failure");
    assert_eq!(issues.len(), 1, "expected one temp upload failure issue");
    let issue = &issues[0];
    assert_eq!(issue.kind, expect.issue.kind);
    assert_eq!(issue.summary, expect.issue.summary);
    assert_eq!(
        issue.detail_json["failure"].as_str(),
        Some(expect.issue.failure.as_str())
    );
    let recorded_path = issue.detail_json["path"]
        .as_str()
        .expect("temp upload failure path should be present");
    assert!(
        recorded_path.ends_with(
            initial
                .local_file
                .relative_path
                .to_str()
                .expect("fixture path should be valid UTF-8")
        ),
        "expected `{recorded_path}` to end with relative source path"
    );
    drop(db);

    let source_path = write_source_file(&source_root, &initial.local_file);
    let recovered = run_execute(
        &db_path,
        &store,
        &initial.local_only_state.client_file_id,
        &source_path,
        &execute,
    );

    assert!(!recovered.upload_reused);

    let db = SqliteStateDb::open(&db_path).expect("reopen DB after recovered upload");
    assert_eq!(
        db.load_local_only_conflicts_and_errors(&initial.local_only_state.client_file_id)
            .expect("load temp issue rows after recovery"),
        Vec::new()
    );
    assert_eq!(
        db.load_local_only_file(&initial.local_only_state.client_file_id)
            .expect("load local-only row after recovery"),
        None
    );
}

fn run_upload_local_create_fixture(relative_path: &str, temp_label: &str) {
    let scenario = load_fixture(relative_path);
    let mut initial: InitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<FixtureAction> = scenario.decode_actions().expect("decode actions");
    let mut expect: ExpectedState = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new(temp_label);
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let scratch_store_root = temp_dir.path().join("scratch-objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&scratch_store_root).expect("create scratch object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");
    let scratch_store =
        LocalFsStore::new(&scratch_store_root).expect("create scratch local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);

    assert_eq!(
        actions.len(),
        3,
        "fixture should contain execute, restart, planner"
    );
    let execute = actions[0].execute().expect("execute action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let planner_tick = actions[2].planner().expect("planner action third");

    let source_path = write_source_file(&source_root, &initial.local_file);
    let expected_upload = upload_small_file_from_path(
        &scratch_store,
        &initial.local_only_state.namespace_id,
        &source_path,
    )
    .expect("build expected upload");
    fill_upload_expectations(&mut initial, &mut expect, &expected_upload);

    let transfer_row = initial
        .local_only_transfer_ledger
        .as_ref()
        .map(|seed| seed_transfer_ledger_row(&initial, seed, &expected_upload));
    seed_client_state(&db_path, &initial, transfer_row.as_ref());
    if let Some(seed) = &initial.local_only_transfer_ledger {
        seed_uploaded_prefix_for_transfer(&store, &source_path, &expected_upload, seed.block_index);
    }

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
        Some(expected_local_only_upload(
            &initial.local_only_state.client_file_id,
            &initial.local_only_state.namespace_id,
            &expected_upload,
            execute.uploaded_at_ms,
        ))
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
        expect.mutation_response.clone().into_response()
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
    if expect.local_only_transfer_ledger_cleared {
        assert_eq!(
            db.load_local_only_transfer_ledger(
                &initial.local_only_state.client_file_id,
                TransferDirection::Upload,
            )
            .expect("load temp upload transfer ledger"),
            None
        );
    }

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

fn run_execute(
    db_path: &Path,
    store: &LocalFsStore,
    client_file_id: &ClientFileId,
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
    fs::write(&source_path, local_file.file_bytes()).expect("write source file");
    source_path
}

fn fill_upload_expectations(
    initial: &mut InitialState,
    expect: &mut ExpectedState,
    uploaded: &UploadedContent,
) {
    match &initial.local_only_state.content_digest {
        Some(digest) => assert_eq!(digest, &uploaded.file_digest_sha256),
        None => initial.local_only_state.content_digest = Some(uploaded.file_digest_sha256.clone()),
    }
    match &mut expect.request.op {
        RawExpectedRequestOp::CreateFile { create_file } => match &create_file
            .content_manifest_digest
        {
            Some(digest) => assert_eq!(digest, &uploaded.content_manifest_digest),
            None => {
                create_file.content_manifest_digest = Some(uploaded.content_manifest_digest.clone())
            }
        },
    }
    if let Some(created_inode) = expect.mutation_response.created_inode.as_mut() {
        match &created_inode.content_digest {
            Some(digest) => assert_eq!(digest, &uploaded.file_digest_sha256),
            None => created_inode.content_digest = Some(uploaded.file_digest_sha256.clone()),
        }
    }
    fill_remote_state_digests(&mut expect.remote_state, uploaded);
    fill_local_state_digest(&mut expect.local_state, uploaded);
    fill_sync_anchor_digests(&mut expect.sync_anchor, uploaded);
}

fn fill_remote_state_digests(state: &mut RemoteFileStateRow, uploaded: &UploadedContent) {
    match &state.content_digest {
        Some(digest) => assert_eq!(digest, &uploaded.file_digest_sha256),
        None => state.content_digest = Some(uploaded.file_digest_sha256.clone()),
    }
    match &state.content_manifest_digest {
        Some(digest) => assert_eq!(digest, &uploaded.content_manifest_digest),
        None => state.content_manifest_digest = Some(uploaded.content_manifest_digest.clone()),
    }
}

fn fill_local_state_digest(state: &mut LocalFileStateRow, uploaded: &UploadedContent) {
    match &state.content_digest {
        Some(digest) => assert_eq!(digest, &uploaded.file_digest_sha256),
        None => state.content_digest = Some(uploaded.file_digest_sha256.clone()),
    }
}

fn fill_sync_anchor_digests(state: &mut SyncAnchorRow, uploaded: &UploadedContent) {
    match &state.content_digest {
        Some(digest) => assert_eq!(digest, &uploaded.file_digest_sha256),
        None => state.content_digest = Some(uploaded.file_digest_sha256.clone()),
    }
    match &state.content_manifest_digest {
        Some(digest) => assert_eq!(digest, &uploaded.content_manifest_digest),
        None => state.content_manifest_digest = Some(uploaded.content_manifest_digest.clone()),
    }
}

fn expected_local_only_upload(
    client_file_id: &ClientFileId,
    namespace_id: &NamespaceId,
    uploaded: &UploadedContent,
    uploaded_at_ms: u64,
) -> LocalOnlyUploadRow {
    LocalOnlyUploadRow {
        client_file_id: client_file_id.clone(),
        namespace_id: namespace_id.clone(),
        file_digest_sha256: uploaded.file_digest_sha256.clone(),
        content_manifest_digest: uploaded.content_manifest_digest.clone(),
        manifest_object_key: uploaded.manifest_object_key.clone(),
        file_size_bytes: uploaded.file_size_bytes,
        uploaded_at_ms,
    }
}

fn seed_transfer_ledger_row(
    initial: &InitialState,
    seed: &FixtureTransferLedgerSeed,
    uploaded: &UploadedContent,
) -> LocalOnlyTransferLedgerRow {
    LocalOnlyTransferLedgerRow {
        client_file_id: initial.local_only_state.client_file_id.clone(),
        namespace_id: initial.local_only_state.namespace_id.clone(),
        transfer_id: local_only_upload_transfer_id(
            &initial.local_only_state.client_file_id,
            &uploaded.content_manifest_digest,
        ),
        direction: seed.direction,
        object_key: uploaded.manifest_object_key.clone(),
        block_index: seed.block_index,
        block_count: seed.block_count,
        state: seed.state,
        updated_at_ms: seed.updated_at_ms,
    }
}

fn seed_uploaded_prefix_for_transfer(
    store: &LocalFsStore,
    source_path: &Path,
    uploaded: &UploadedContent,
    block_index: u64,
) {
    let bytes = fs::read(source_path).expect("read source bytes for transfer seeding");
    for (block_object, block_bytes) in uploaded
        .block_objects
        .iter()
        .zip(bytes.chunks(CONTENT_BLOCK_SIZE_BYTES as usize))
        .take(usize::try_from(block_index).expect("block index should fit"))
    {
        store
            .put_if_absent(&block_object.object_key, block_bytes)
            .expect("seed already-uploaded block object");
    }
}

fn local_only_upload_transfer_id(
    client_file_id: &ClientFileId,
    content_manifest_digest: &str,
) -> String {
    format!(
        "upload-local-only:{}:{}",
        client_file_id.as_str(),
        content_manifest_digest
    )
}

#[derive(Debug, Deserialize)]
struct InitialState {
    local_only_state: LocalOnlyFileStateRow,
    local_file: FixtureLocalFile,
    planned_local_only_action: LocalOnlyPlannedActionRow,
    #[serde(default)]
    local_only_transfer_ledger: Option<FixtureTransferLedgerSeed>,
    head: HeadState,
    lease: LeaseState,
}

#[derive(Debug, Deserialize)]
struct ExpectedState {
    upload_reused: bool,
    pending_request_id: String,
    request: RawExpectedRequest,
    mutation_response: RawClientMutationResponse,
    bound_identity: BoundLocalOnlyFile,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    local_only_state_cleared: bool,
    planned_local_only_action_cleared: bool,
    pending_mutation_cleared: bool,
    #[serde(default)]
    local_only_transfer_ledger_cleared: bool,
    planner_result: PlannedActionRecord,
}

#[derive(Debug, Deserialize)]
struct FailureExpectedState {
    issue: RawLocalOnlyUploadFailureIssueExpect,
    planned_local_only_action_retained: bool,
}

#[derive(Debug, Deserialize)]
struct RawLocalOnlyUploadFailureIssueExpect {
    kind: String,
    summary: String,
    failure: String,
}

#[derive(Debug, Deserialize)]
struct FixtureLocalFile {
    relative_path: PathBuf,
    #[serde(default)]
    content_utf8: Option<String>,
    #[serde(default)]
    generated_two_block: Option<GeneratedTwoBlockFile>,
}

impl FixtureLocalFile {
    fn file_bytes(&self) -> Vec<u8> {
        if let Some(content_utf8) = &self.content_utf8 {
            return content_utf8.as_bytes().to_vec();
        }
        let generated = self
            .generated_two_block
            .as_ref()
            .expect("fixture local file should provide content");
        generated.file_bytes()
    }
}

#[derive(Debug, Deserialize)]
struct GeneratedTwoBlockFile {
    first_block_fill_byte: String,
    second_block_utf8: String,
}

impl GeneratedTwoBlockFile {
    fn file_bytes(&self) -> Vec<u8> {
        let first_byte = self
            .first_block_fill_byte
            .as_bytes()
            .first()
            .copied()
            .expect("first_block_fill_byte should not be empty");
        let mut bytes = vec![first_byte; CONTENT_BLOCK_SIZE_BYTES as usize];
        bytes.extend_from_slice(self.second_block_utf8.as_bytes());
        bytes
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
struct RawExpectedRequest {
    namespace_id: NamespaceId,
    client_request_id: String,
    op: RawExpectedRequestOp,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum RawExpectedRequestOp {
    CreateFile {
        create_file: RawExpectedCreateFileOp,
    },
}

#[derive(Debug, Clone, Deserialize)]
struct RawExpectedCreateFileOp {
    parent_inode_id: InodeId,
    display_name: String,
    content_manifest_digest: Option<String>,
}

impl RawExpectedRequest {
    fn into_request(self) -> ClientMutationRequest {
        let op = match self.op {
            RawExpectedRequestOp::CreateFile { create_file } => ClientMutationOp::CreateFile {
                parent_inode_id: create_file.parent_inode_id,
                display_name: create_file.display_name,
                content_manifest_digest: create_file
                    .content_manifest_digest
                    .expect("expected create_file manifest digest"),
            },
        };

        ClientMutationRequest {
            namespace_id: self.namespace_id,
            client_request_id: self.client_request_id,
            op,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawClientMutationResponse {
    namespace_id: NamespaceId,
    client_request_id: String,
    committed_seq: ChangeSeq,
    created_inode: Option<RawCreatedRemoteInode>,
    #[serde(default)]
    replaced_file: Option<serde_json::Value>,
}

impl RawClientMutationResponse {
    fn into_response(self) -> ClientMutationResponse {
        assert!(
            self.replaced_file.is_none(),
            "local-only upload fixture should not include replaced_file"
        );
        ClientMutationResponse {
            namespace_id: self.namespace_id,
            client_request_id: self.client_request_id,
            committed_seq: self.committed_seq,
            created_inode: self.created_inode.map(|created_inode| CreatedRemoteInode {
                inode_id: created_inode.inode_id,
                inode_kind: created_inode.inode_kind,
                revision_no: created_inode.revision_no,
                parent_inode_id: created_inode.parent_inode_id,
                display_name: created_inode.display_name,
                content_digest: created_inode.content_digest,
            }),
            replaced_file: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawCreatedRemoteInode {
    inode_id: InodeId,
    inode_kind: loon_types::InodeKind,
    revision_no: RevisionNo,
    parent_inode_id: InodeId,
    display_name: String,
    content_digest: Option<String>,
}

fn seed_client_state(
    db_path: &Path,
    initial: &InitialState,
    transfer_ledger: Option<&LocalOnlyTransferLedgerRow>,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-upload-local-create-initial-state", |tx| {
        tx.upsert_local_only_file(&initial.local_only_state)?;
        tx.upsert_planned_local_only_action(&initial.planned_local_only_action)?;
        if let Some(transfer_ledger) = transfer_ledger {
            tx.upsert_local_only_transfer_ledger(transfer_ledger)?;
        }
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
