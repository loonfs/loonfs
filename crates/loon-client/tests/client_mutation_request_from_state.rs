use loon_client::executor::build_client_mutation_request_from_state;
use loon_client::state_db::{
    LocalOnlyFileStateRow, LocalOnlyPlannedActionRow, LocalOnlyUploadRow, SqliteStateDb,
};
use loon_client::upload::upload_small_file_from_path;
use loon_objectstore::fs::LocalFsStore;
use loon_testkit::scenario::Scenario;
use loon_types::{ClientMutationOp, ClientMutationRequest, InodeId, NamespaceId};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn uploaded_local_only_file_builds_request_from_state() {
    let scenario = load_fixture("client/uploaded_local_only_file_builds_request_from_state.yaml");
    let initial: InitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<FixtureAction> = scenario.decode_actions().expect("decode actions");
    let expect: ExpectedState = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new("client-mutation-request-from-state");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    seed_client_state(&db_path, &initial);

    assert_eq!(
        actions.len(),
        3,
        "fixture should contain upload, restart, build"
    );
    let record = actions[0].record().expect("record upload action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let build = actions[2].build().expect("build action third");

    let source_path = source_root.join(&initial.local_file.relative_path);
    fs::create_dir_all(source_path.parent().expect("source file parent"))
        .expect("create source file parent");
    fs::write(&source_path, initial.local_file.content_utf8.as_bytes()).expect("write source");

    let uploaded =
        upload_small_file_from_path(&store, &initial.local_only_state.namespace_id, &source_path)
            .expect("upload local file");

    {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.record_local_only_upload(
            &initial.local_only_state.client_file_id,
            &uploaded,
            record.uploaded_at_ms,
        )
        .expect("record local-only upload");
    }

    let db = SqliteStateDb::open(&db_path).expect("reopen client state DB");
    assert_eq!(
        db.load_local_only_upload(&initial.local_only_state.client_file_id)
            .expect("load recorded upload row"),
        Some(expect.upload_row.clone())
    );
    assert_eq!(
        build_client_mutation_request_from_state(
            &db,
            &build.client_request_id,
            &initial.local_only_state.client_file_id,
        )
        .expect("build request from durable state"),
        expect.request.into_request()
    );
}

#[derive(Debug, Deserialize)]
struct InitialState {
    local_only_state: LocalOnlyFileStateRow,
    local_file: FixtureLocalFile,
    planned_local_only_action: LocalOnlyPlannedActionRow,
}

#[derive(Debug, Deserialize)]
struct ExpectedState {
    upload_row: LocalOnlyUploadRow,
    request: ExpectedRequest,
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
    RestartClientStateDb {
        restart_client_state_db: bool,
    },
    BuildClientMutationRequestFromState {
        build_client_mutation_request_from_state: BuildClientMutationRequestFromStateAction,
    },
}

impl FixtureAction {
    fn record(&self) -> Option<RecordLocalOnlyUploadAction> {
        match self {
            Self::RecordLocalOnlyUpload {
                record_local_only_upload,
            } => Some(record_local_only_upload.clone()),
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

    fn build(&self) -> Option<BuildClientMutationRequestFromStateAction> {
        match self {
            Self::BuildClientMutationRequestFromState {
                build_client_mutation_request_from_state,
            } => Some(build_client_mutation_request_from_state.clone()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RecordLocalOnlyUploadAction {
    uploaded_at_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct BuildClientMutationRequestFromStateAction {
    client_request_id: String,
}

#[derive(Debug, Deserialize)]
struct ExpectedRequest {
    namespace_id: NamespaceId,
    client_request_id: String,
    op: ExpectedRequestOp,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ExpectedRequestOp {
    CreateFile { create_file: ExpectedCreateFileOp },
    CreateDir { create_dir: ExpectedCreateDirOp },
}

#[derive(Debug, Deserialize)]
struct ExpectedCreateFileOp {
    parent_inode_id: InodeId,
    display_name: String,
    content_manifest_digest: String,
}

#[derive(Debug, Deserialize)]
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

fn seed_client_state(db_path: &Path, initial: &InitialState) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-request-from-state-initial-state", |tx| {
        tx.upsert_local_only_file(&initial.local_only_state)?;
        tx.upsert_planned_local_only_action(&initial.planned_local_only_action)?;
        Ok(())
    })
    .expect("seed initial client state");
}

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
}

type TestDir = loon_testkit::tempdir::TestDir;
