use loon_client::planner::{plan_file, PlannedActionRecord};
use loon_client::state_db::{
    AppliedRemoteObservation, LocalFileStateRow, LocalOnlyFileStateRow, LocalOnlyPlannedActionRow,
    ObservedRemoteInode, PendingClientMutationRow, PendingInodeMutationRow, PlannedActionRow,
    RemoteFileStateRow, SqliteStateDb, SyncAnchorRow,
};
use loon_client::upload::upload_small_file_from_path;
use loon_objectstore::fs::LocalFsStore;
use loon_testkit::scenario::Scenario;
use loon_types::{ClientMutationOp, ClientMutationRequest, InodeId, NamespaceId, RevisionNo};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

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
}

fn write_source_file(source_root: &Path, relative_path: &Path, content_utf8: &str) -> PathBuf {
    let source_path = source_root.join(relative_path);
    fs::create_dir_all(source_path.parent().expect("source file parent"))
        .expect("create source file parent");
    fs::write(&source_path, content_utf8.as_bytes()).expect("write source file");
    source_path
}

fn load_fixture(relative_path: &str) -> Scenario {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/scenarios")
        .join(relative_path);
    Scenario::load(&path).unwrap_or_else(|err| panic!("load fixture {}: {err}", path.display()))
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
    IgnoredStale {
        ignored_stale: RawAppliedRemoteObservationTarget,
    },
    IgnoredUnmatched {
        ignored_unmatched: RawAppliedRemoteObservationTarget,
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
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawAppliedRemoteObservationTarget {
    namespace_id: NamespaceId,
    inode_id: InodeId,
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
