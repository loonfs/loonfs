use loon_client::planner::{plan_file, PlannedActionRecord};
use loon_client::state_db::{
    AppliedRemoteObservation, LocalFileStateRow, LocalOnlyFileStateRow, LocalOnlyPlannedActionRow,
    ObservedRemoteInode, PendingClientMutationRow, PendingInodeMutationRow, PlannedActionRow,
    RemoteFileStateRow, SqliteStateDb, SyncAnchorRow, TransferDirection, TransferLedgerRow,
    TransferState,
};
use loon_client::upload::upload_small_file_from_path;
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::content_manifest;
use loon_testkit::scenario::Scenario;
use loon_types::{ClientMutationOp, ClientMutationRequest, InodeId, NamespaceId, RevisionNo};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn remote_observation_binds_local_only_create_and_restarts_converged() {
    let scenario = load_fixture("client/client_remote_observation_binds_local_only_create.yaml");
    let initial: CreateObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: CreateObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-create");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create objectstore root");
    fs::create_dir_all(&source_root).expect("create source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");
    let expected_pending_client_mutation = initial.pending_client_mutation.clone().into_row();

    let source_path = write_source_file(
        &source_root,
        &initial.local_file.relative_path,
        &initial.local_file.content_utf8,
    );
    seed_create_observation_state(&db_path, &initial, &store, &source_path);

    let observe = actions[0].apply().expect("apply action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let planner_tick = actions[2].planner().expect("planner action third");

    let outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };

    assert_eq!(outcome, expect.outcome.clone().into_outcome());

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
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
            .expect("load temp local-only state"),
        if expect.local_only_state_cleared {
            None
        } else {
            Some(initial.local_only_state.clone())
        }
    );
    assert_eq!(
        db.load_local_only_upload(&initial.local_only_state.client_file_id)
            .expect("load temp local-only upload"),
        if expect.local_only_upload_cleared {
            None
        } else {
            panic!("fixture currently expects local-only upload to clear");
        }
    );
    assert_eq!(
        db.load_planned_local_only_action(&initial.local_only_state.client_file_id)
            .expect("load temp local-only action"),
        if expect.planned_local_only_action_cleared {
            None
        } else {
            Some(initial.planned_local_only_action.clone())
        }
    );
    assert_eq!(
        db.load_pending_client_mutation(&initial.pending_client_mutation.client_request_id)
            .expect("load pending client mutation"),
        if expect.pending_client_mutation_cleared {
            None
        } else {
            Some(expected_pending_client_mutation)
        }
    );
}

#[test]
fn remote_observation_converges_bound_file_edit_and_restarts_clean() {
    let scenario = load_fixture("client/client_remote_observation_converges_bound_file_edit.yaml");
    let initial: EditObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: EditObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-edit");
    let db_path = temp_dir.path().join("client.sqlite3");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&source_root).expect("create source root");
    let expected_pending_inode_mutation = initial.pending_inode_mutation.clone().into_row();
    let local_path = write_source_file(
        &source_root,
        &initial.local_file.relative_path,
        &initial.local_file.content_utf8,
    );

    seed_edit_observation_state(&db_path, &initial);

    let observe = actions[0].apply().expect("apply action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let planner_tick = actions[2].planner().expect("planner action third");

    let outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };

    assert_eq!(outcome, expect.outcome.clone().into_outcome());

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
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
        db.load_planned_action(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load planned action"),
        if expect.planned_action_cleared {
            None
        } else {
            Some(initial.planned_action.clone())
        }
    );
    assert_eq!(
        db.load_pending_inode_mutation(&initial.pending_inode_mutation.client_request_id)
            .expect("load pending inode mutation"),
        if expect.pending_inode_mutation_cleared {
            None
        } else {
            Some(expected_pending_inode_mutation)
        }
    );
    assert_eq!(
        fs::read_to_string(&local_path).expect("read local file after observation"),
        initial.local_file.content_utf8
    );
}

#[test]
fn remote_observation_ambiguous_bind_records_issue_and_preserves_local_only_state() {
    let scenario =
        load_fixture("client/client_remote_observation_ambiguous_bind_records_issue.yaml");
    let initial: AmbiguousObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: AmbiguousObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-ambiguous");
    let db_path = temp_dir.path().join("client.sqlite3");

    seed_ambiguous_observation_state(&db_path, &initial.local_only_state);

    let observe = actions[0].apply().expect("apply action first");
    assert!(actions[1].is_restart(), "restart should be second");

    let outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };

    assert_eq!(outcome, expect.outcome.clone().into_outcome());

    let db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    assert_eq!(
        db.load_file_sync_views(&NamespaceId::from("ns-1"), InodeId(601))
            .expect("load ambiguous views"),
        loon_client::state_db::FileSyncViews {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(601),
            remote: expect.remote_state.clone(),
            local: expect.local_state.clone(),
            sync_anchor: expect.sync_anchor.clone(),
        }
    );
    let actual_issues = db
        .load_conflicts_and_errors(&NamespaceId::from("ns-1"), InodeId(601))
        .expect("load durable issues")
        .into_iter()
        .map(RawConflictOrErrorExpect::from_row)
        .collect::<Vec<_>>();
    assert_eq!(actual_issues, expect.conflicts_and_errors);
    let surviving_local_only = initial
        .local_only_state
        .iter()
        .filter(|row| {
            db.load_local_only_file(&row.client_file_id)
                .expect("load local-only row")
                .is_some()
        })
        .count();
    assert_eq!(surviving_local_only, expect.local_only_state_count);
}

#[test]
fn remote_observation_updates_bound_file_while_upload_transfer_active() {
    let scenario = load_fixture(
        "client/client_remote_observation_updates_bound_file_while_upload_transfer_active.yaml",
    );
    let initial: EditObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: EditObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-active-upload");
    let db_path = temp_dir.path().join("client.sqlite3");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&source_root).expect("create source root");
    let expected_pending_inode_mutation = initial.pending_inode_mutation.clone().into_row();
    let local_path = write_source_file(
        &source_root,
        &initial.local_file.relative_path,
        &initial.local_file.content_utf8,
    );

    seed_edit_observation_state(&db_path, &initial);

    let observe = actions[0].apply().expect("apply action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let planner_tick = actions[2].planner().expect("planner action third");

    let outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };

    assert_eq!(outcome, expect.outcome.clone().into_outcome());

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan inode after restart");

    assert_eq!(planner_result, expect.planner_result);
    assert_eq!(
        db.load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load updated views"),
        loon_client::state_db::FileSyncViews {
            namespace_id: planner_tick.namespace_id.clone(),
            inode_id: planner_tick.inode_id,
            remote: Some(expect.remote_state.clone()),
            local: Some(expect.local_state.clone()),
            sync_anchor: Some(expect.sync_anchor.clone()),
        }
    );
    assert_eq!(
        db.load_pending_inode_mutation(&initial.pending_inode_mutation.client_request_id)
            .expect("load pending inode mutation"),
        Some(expected_pending_inode_mutation)
    );
    assert!(
        db.load_transfer_ledger_for_inode(
            &initial.remote_state.namespace_id,
            initial.remote_state.inode_id,
            TransferDirection::Upload,
        )
        .expect("load upload transfer ledger")
        .is_some(),
        "active upload transfer should survive late observation",
    );
    assert_eq!(
        fs::read_to_string(&local_path).expect("read local file after observation"),
        initial.local_file.content_utf8
    );
}

#[test]
fn remote_observation_updates_remote_only_file_while_download_transfer_active() {
    let scenario = load_fixture(
        "client/client_remote_observation_updates_remote_only_file_while_download_transfer_active.yaml",
    );
    let initial: DownloadObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: DownloadObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-active-download");
    let db_path = temp_dir.path().join("client.sqlite3");

    seed_remote_only_observation_state(&db_path, &initial);

    let observe = actions[0].apply().expect("apply action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let planner_tick = actions[2].planner().expect("planner action third");

    let outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };

    assert_eq!(outcome, expect.outcome.clone().into_outcome());

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan inode after restart");

    assert_eq!(planner_result, expect.planner_result);
    assert_eq!(
        db.load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load remote-only views"),
        loon_client::state_db::FileSyncViews {
            namespace_id: planner_tick.namespace_id.clone(),
            inode_id: planner_tick.inode_id,
            remote: Some(expect.remote_state.clone()),
            local: Some(expect.local_state.clone()),
            sync_anchor: None,
        }
    );
    assert!(
        db.load_transfer_ledger_for_inode(
            &planner_tick.namespace_id,
            planner_tick.inode_id,
            TransferDirection::Download,
        )
        .expect("load download transfer ledger")
        .is_some(),
        "active download transfer should survive late observation",
    );
}

fn seed_create_observation_state(
    db_path: &Path,
    initial: &CreateObservationInitial,
    store: &LocalFsStore,
    source_path: &Path,
) {
    let pending_client_mutation = initial.pending_client_mutation.clone().into_row();
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-create-observation-state", |tx| {
        tx.upsert_local_only_file(&initial.local_only_state)?;
        tx.upsert_planned_local_only_action(&initial.planned_local_only_action)?;
        Ok(())
    })
    .expect("seed create observation local-only state");

    let uploaded =
        upload_small_file_from_path(store, &initial.local_only_state.namespace_id, source_path)
            .expect("upload local-only file for upload ledger");
    db.record_local_only_upload(
        &initial.local_only_state.client_file_id,
        &uploaded,
        pending_client_mutation.created_at_ms,
    )
    .expect("record local-only upload");
    db.record_pending_client_mutation(
        &initial.local_only_state.client_file_id,
        &pending_client_mutation.request,
        pending_client_mutation.created_at_ms,
    )
    .expect("record pending client mutation");
}

fn seed_edit_observation_state(db_path: &Path, initial: &EditObservationInitial) {
    let pending_inode_mutation = initial.pending_inode_mutation.clone().into_row();
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-edit-observation-state", |tx| {
        tx.upsert_remote_file(&initial.remote_state)?;
        tx.upsert_local_file(&initial.local_state)?;
        tx.upsert_sync_anchor(&initial.sync_anchor)?;
        tx.upsert_planned_action(&initial.planned_action)?;
        Ok(())
    })
    .expect("seed edit observation state");
    db.record_pending_inode_mutation(
        &pending_inode_mutation.namespace_id,
        pending_inode_mutation.inode_id,
        &pending_inode_mutation.request,
        pending_inode_mutation.created_at_ms,
    )
    .expect("record pending inode mutation");
    if let Some(transfer_ledger) = &initial.transfer_ledger {
        db.upsert_transfer_ledger(&TransferLedgerRow {
            namespace_id: initial.remote_state.namespace_id.clone(),
            inode_id: initial.remote_state.inode_id,
            transfer_id: format!(
                "upload:{}:{}:{}",
                initial.remote_state.namespace_id.as_str(),
                initial.remote_state.inode_id.0,
                match &pending_inode_mutation.request.op {
                    ClientMutationOp::ReplaceFile {
                        content_manifest_digest,
                        ..
                    } => content_manifest_digest,
                    _ => unreachable!("pending inode mutation fixture should be replace_file"),
                }
            ),
            direction: transfer_ledger.direction,
            object_key: content_manifest(
                initial.remote_state.namespace_id.as_str(),
                match &pending_inode_mutation.request.op {
                    ClientMutationOp::ReplaceFile {
                        content_manifest_digest,
                        ..
                    } => content_manifest_digest,
                    _ => unreachable!("pending inode mutation fixture should be replace_file"),
                },
            ),
            block_index: transfer_ledger.block_index,
            block_count: transfer_ledger.block_count,
            state: transfer_ledger.state,
            updated_at_ms: transfer_ledger.updated_at_ms,
        })
        .expect("record active transfer ledger");
    }
}

fn seed_remote_only_observation_state(db_path: &Path, initial: &DownloadObservationInitial) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-remote-only-observation-state", |tx| {
        tx.upsert_remote_file(&initial.remote_state)?;
        tx.upsert_local_file(&initial.local_state)?;
        tx.upsert_planned_action(&initial.planned_action)?;
        Ok(())
    })
    .expect("seed remote-only observation state");
    db.upsert_transfer_ledger(&TransferLedgerRow {
        namespace_id: initial.remote_state.namespace_id.clone(),
        inode_id: initial.remote_state.inode_id,
        transfer_id: format!(
            "download:{}:{}:{}",
            initial.remote_state.namespace_id.as_str(),
            initial.remote_state.inode_id.0,
            initial
                .remote_state
                .content_manifest_digest
                .as_deref()
                .expect("remote-only observation fixture should include manifest digest"),
        ),
        direction: initial.transfer_ledger.direction,
        object_key: content_manifest(
            initial.remote_state.namespace_id.as_str(),
            initial
                .remote_state
                .content_manifest_digest
                .as_deref()
                .expect("remote-only observation fixture should include manifest digest"),
        ),
        block_index: initial.transfer_ledger.block_index,
        block_count: initial.transfer_ledger.block_count,
        state: initial.transfer_ledger.state,
        updated_at_ms: initial.transfer_ledger.updated_at_ms,
    })
    .expect("record active download transfer ledger");
}

fn seed_ambiguous_observation_state(db_path: &Path, local_only_state: &[LocalOnlyFileStateRow]) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-ambiguous-observation-state", |tx| {
        for row in local_only_state {
            tx.upsert_local_only_file(row)?;
        }
        Ok(())
    })
    .expect("seed ambiguous local-only state");
}

fn write_source_file(source_root: &Path, relative_path: &Path, content_utf8: &str) -> PathBuf {
    let source_path = source_root.join(relative_path);
    fs::create_dir_all(source_path.parent().expect("source file parent"))
        .expect("create source file parent");
    fs::write(&source_path, content_utf8.as_bytes()).expect("write source file");
    source_path
}

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
}

#[derive(Debug, Deserialize)]
struct CreateObservationInitial {
    local_only_state: LocalOnlyFileStateRow,
    local_file: FixtureLocalFile,
    planned_local_only_action: LocalOnlyPlannedActionRow,
    pending_client_mutation: RawPendingClientMutationRow,
}

#[derive(Debug, Deserialize)]
struct CreateObservationExpect {
    outcome: RawAppliedRemoteObservation,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    local_only_state_cleared: bool,
    local_only_upload_cleared: bool,
    planned_local_only_action_cleared: bool,
    pending_client_mutation_cleared: bool,
    planner_result: PlannedActionRecord,
}

#[derive(Debug, Deserialize)]
struct EditObservationInitial {
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    local_file: FixtureLocalFile,
    planned_action: PlannedActionRow,
    pending_inode_mutation: RawPendingInodeMutationRow,
    #[serde(default)]
    transfer_ledger: Option<FixtureTransferLedgerSeed>,
}

#[derive(Debug, Deserialize)]
struct EditObservationExpect {
    outcome: RawAppliedRemoteObservation,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    planned_action_cleared: bool,
    pending_inode_mutation_cleared: bool,
    planner_result: PlannedActionRecord,
}

#[derive(Debug, Deserialize)]
struct DownloadObservationInitial {
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    planned_action: PlannedActionRow,
    transfer_ledger: FixtureTransferLedgerSeed,
}

#[derive(Debug, Deserialize)]
struct DownloadObservationExpect {
    outcome: RawAppliedRemoteObservation,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    planned_action_cleared: bool,
    planner_result: PlannedActionRecord,
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
struct AmbiguousObservationInitial {
    local_only_state: Vec<LocalOnlyFileStateRow>,
}

#[derive(Debug, Deserialize)]
struct AmbiguousObservationExpect {
    outcome: RawAppliedRemoteObservation,
    remote_state: Option<RemoteFileStateRow>,
    local_state: Option<LocalFileStateRow>,
    sync_anchor: Option<SyncAnchorRow>,
    conflicts_and_errors: Vec<RawConflictOrErrorExpect>,
    local_only_state_count: usize,
}

#[derive(Debug, Deserialize)]
struct FixtureLocalFile {
    relative_path: PathBuf,
    content_utf8: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RemoteObservationFixtureAction {
    ApplyRemoteObservation {
        apply_remote_observation: ApplyRemoteObservationAction,
    },
    RestartClientStateDb {
        restart_client_state_db: bool,
    },
    PlannerTick {
        planner_tick: PlannerTickAction,
    },
}

impl RemoteObservationFixtureAction {
    fn apply(&self) -> Option<ApplyRemoteObservationAction> {
        match self {
            Self::ApplyRemoteObservation {
                apply_remote_observation,
            } => Some(apply_remote_observation.clone()),
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
struct ApplyRemoteObservationAction {
    applied_at_ms: u64,
    remote_observation: ObservedRemoteInode,
}

#[derive(Debug, Clone, Deserialize)]
struct PlannerTickAction {
    namespace_id: NamespaceId,
    inode_id: InodeId,
    now_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct RawPendingClientMutationRow {
    client_request_id: String,
    namespace_id: NamespaceId,
    client_file_id: loon_client::state_db::ClientFileId,
    request: RawClientMutationRequest,
    created_at_ms: u64,
}

impl RawPendingClientMutationRow {
    fn into_row(self) -> PendingClientMutationRow {
        PendingClientMutationRow {
            client_request_id: self.client_request_id,
            namespace_id: self.namespace_id,
            client_file_id: self.client_file_id,
            request: self.request.into_request(),
            created_at_ms: self.created_at_ms,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawPendingInodeMutationRow {
    client_request_id: String,
    namespace_id: NamespaceId,
    inode_id: InodeId,
    request: RawClientMutationRequest,
    created_at_ms: u64,
}

impl RawPendingInodeMutationRow {
    fn into_row(self) -> PendingInodeMutationRow {
        PendingInodeMutationRow {
            client_request_id: self.client_request_id,
            namespace_id: self.namespace_id,
            inode_id: self.inode_id,
            request: self.request.into_request(),
            created_at_ms: self.created_at_ms,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawClientMutationRequest {
    namespace_id: NamespaceId,
    client_request_id: String,
    op: RawClientMutationOp,
}

impl RawClientMutationRequest {
    fn into_request(self) -> ClientMutationRequest {
        ClientMutationRequest {
            namespace_id: self.namespace_id,
            client_request_id: self.client_request_id,
            op: match self.op {
                RawClientMutationOp::CreateFile { create_file } => ClientMutationOp::CreateFile {
                    parent_inode_id: create_file.parent_inode_id,
                    display_name: create_file.display_name,
                    content_manifest_digest: create_file.content_manifest_digest,
                },
                RawClientMutationOp::ReplaceFile { replace_file } => {
                    ClientMutationOp::ReplaceFile {
                        inode_id: replace_file.inode_id,
                        base_revision_no: replace_file.base_revision_no,
                        content_manifest_digest: replace_file.content_manifest_digest,
                    }
                }
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum RawAppliedRemoteObservation {
    BoundLocalOnly {
        bound_local_only: loon_client::state_db::BoundLocalOnlyFile,
    },
    ConvergedBoundInode {
        converged_bound_inode: loon_client::state_db::AppliedInodeMutation,
    },
    UpdatedBoundRemoteState {
        updated_bound_remote_state: RawAppliedRemoteObservationTarget,
    },
    DiscoveredRemoteOnly {
        discovered_remote_only: RawAppliedRemoteObservationTarget,
    },
    IgnoredStale {
        ignored_stale: RawAppliedRemoteObservationTarget,
    },
    IgnoredUnmatched {
        ignored_unmatched: RawAppliedRemoteObservationTarget,
    },
    RecordedConflictOrError {
        recorded_conflict_or_error: RawRecordedConflictOrError,
    },
}

impl RawAppliedRemoteObservation {
    fn into_outcome(self) -> AppliedRemoteObservation {
        match self {
            Self::BoundLocalOnly { bound_local_only } => {
                AppliedRemoteObservation::BoundLocalOnly(bound_local_only)
            }
            Self::ConvergedBoundInode {
                converged_bound_inode,
            } => AppliedRemoteObservation::ConvergedBoundInode(converged_bound_inode),
            Self::UpdatedBoundRemoteState {
                updated_bound_remote_state,
            } => AppliedRemoteObservation::UpdatedBoundRemoteState {
                namespace_id: updated_bound_remote_state.namespace_id,
                inode_id: updated_bound_remote_state.inode_id,
            },
            Self::DiscoveredRemoteOnly {
                discovered_remote_only,
            } => AppliedRemoteObservation::DiscoveredRemoteOnly {
                namespace_id: discovered_remote_only.namespace_id,
                inode_id: discovered_remote_only.inode_id,
            },
            Self::IgnoredStale { ignored_stale } => AppliedRemoteObservation::IgnoredStale {
                namespace_id: ignored_stale.namespace_id,
                inode_id: ignored_stale.inode_id,
            },
            Self::IgnoredUnmatched { ignored_unmatched } => {
                AppliedRemoteObservation::IgnoredUnmatched {
                    namespace_id: ignored_unmatched.namespace_id,
                    inode_id: ignored_unmatched.inode_id,
                }
            }
            Self::RecordedConflictOrError {
                recorded_conflict_or_error,
            } => AppliedRemoteObservation::RecordedConflictOrError {
                namespace_id: recorded_conflict_or_error.namespace_id,
                inode_id: recorded_conflict_or_error.inode_id,
                kind: recorded_conflict_or_error.kind,
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawAppliedRemoteObservationTarget {
    namespace_id: NamespaceId,
    inode_id: InodeId,
}

#[derive(Debug, Clone, Deserialize)]
struct RawRecordedConflictOrError {
    namespace_id: NamespaceId,
    inode_id: InodeId,
    kind: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct RawConflictOrErrorExpect {
    kind: String,
    summary: String,
    created_at_ms: u64,
    detail_json: serde_json::Value,
}

impl RawConflictOrErrorExpect {
    fn from_row(row: loon_client::state_db::ConflictOrErrorRow) -> Self {
        Self {
            kind: row.kind,
            summary: row.summary,
            created_at_ms: row.created_at_ms,
            detail_json: row.detail_json,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum RawClientMutationOp {
    CreateFile { create_file: RawCreateFileOp },
    ReplaceFile { replace_file: RawReplaceFileOp },
}

#[derive(Debug, Clone, Deserialize)]
struct RawCreateFileOp {
    parent_inode_id: InodeId,
    display_name: String,
    content_manifest_digest: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RawReplaceFileOp {
    inode_id: InodeId,
    base_revision_no: RevisionNo,
    content_manifest_digest: String,
}

type TestDir = loon_testkit::tempdir::TestDir;
