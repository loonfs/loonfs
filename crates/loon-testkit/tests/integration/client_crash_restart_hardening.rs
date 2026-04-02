use super::common::support;

use loon_client::executor::{
    execute_next_client_action, DownloadRemoteEditExecution, ExecutedApplyRemoteRename,
    ExecutedLocalOnlyCreate, ExecutedResolveSameInodeConflict, NextClientAction,
};
use loon_client::state_db::{
    ClientFileId, LocalFileStateRow, LocalOnlyFileStateRow, LocalOnlyPlannedActionRow,
    PlannedActionRow, RemoteFileStateRow, SqliteStateDb, SyncAnchorRow,
};
use loon_client::testing::{
    archive_conflict_artifact_with_faults, execute_next_client_action_with_faults,
    restore_conflict_artifact_to_path_with_faults, FaultController,
};
use loon_client::upload::{upload_small_file_from_path, UploadedContent};
use loon_client::{
    archive_conflict_artifact, discover_conflict_artifacts_for_namespace,
    prepare_conflict_artifact, prepare_subtree_conflict_artifact,
    restore_subtree_conflict_artifact_to_path, write_conflict_artifact_if_absent,
    write_subtree_conflict_artifact_if_absent,
};
use loon_server::mutation::{execute_client_mutation, ClientMutationExecutionParams};
use loon_server::objectstore::fs::LocalFsStore;
use loon_server::objectstore::keys::{
    conflict_artifact_archive, conflict_artifact_prefix, namespace_head, namespace_lease,
};
use loon_server::objectstore::ObjectStore;
use loon_testkit::fixtures::load_fixture;
use loon_testkit::tempdir::TestDir;
use loon_server::core::control_types::{ControlObjectKind, HeadStateEnvelope, LeaseStateEnvelope};
use loon_types::{
    ChangeSeq, ClientMutationRequest, ClientMutationResponse, ConflictArtifactLoserSummary,
    ConflictArtifactWinnerSummary, ConflictClass, HeadState, InodeId, InodeKind, LeaseState,
    NamespaceId, RevisionNo, SubtreeConflictArtifactEntry, SubtreeConflictArtifactRootSummary,
};
use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};

#[test]
#[allow(clippy::result_large_err)]
fn dispatch_retry_reuses_pending_request_after_crash_after_response() {
    let namespace_id = NamespaceId::new("ns-1");
    let client_file_id = ClientFileId::new("temp-dir-1");
    let fixture = ClientCrashFixture::new("crash-dispatch-after-response");
    seed_head_and_lease(
        &fixture.store,
        &HeadState::initial(namespace_id.clone()),
        &LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer-a".to_owned(),
            fence_token: loon_types::FenceToken(0),
            lease_expires_at_ms: 1_900_000_000_000,
        },
    );
    seed_local_only_dir_state(
        &fixture.db_path,
        &LocalOnlyFileStateRow {
            client_file_id: client_file_id.clone(),
            namespace_id: namespace_id.clone(),
            inode_kind: InodeKind::Dir,
            parent_inode_id: Some(InodeId(1)),
            display_name: "projects".to_owned(),
            content_digest: None,
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 10,
        },
        &LocalOnlyPlannedActionRow {
            client_file_id: client_file_id.clone(),
            namespace_id: namespace_id.clone(),
            decision: "create_remote_dir".to_owned(),
            reason: "local_only_directory_without_remote_identity".to_owned(),
            created_at_ms: 10,
        },
    );

    let controller = FaultController::new(
        load_fixture("client/client_fault_dispatch_after_response_once.yaml")
            .decode_fault_plan()
            .expect("decode fault plan"),
    );

    {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
        expect_injected_crash(|| {
            execute_next_client_action_with_faults(
                &mut db,
                &fixture.store,
                |_client_file_id| None,
                |_namespace_id, _inode_id| None,
                |_namespace_id, _inode_id, _parent_inode_id, _display_name| None,
                100,
                101,
                &controller,
                |request| dispatch_request(&fixture.store, request, 102),
            )
        });
    }

    {
        let db = SqliteStateDb::open(&fixture.db_path).expect("reopen db after crash");
        let pending = db
            .load_pending_client_mutation_for_client_file(&client_file_id)
            .expect("load pending request")
            .expect("pending request should survive crash");
        assert_eq!(pending.client_request_id, "client-req-00000000000000000001");
    }

    let retried = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen db for retry");
        execute_next_client_action(
            &mut db,
            &fixture.store,
            |_client_file_id| None,
            |_namespace_id, _inode_id| None,
            |_namespace_id, _inode_id, _parent_inode_id, _display_name| None,
            100,
            201,
            |request| dispatch_request(&fixture.store, request, 202),
        )
        .expect("retry should succeed")
        .expect("retry should execute work")
    };

    match retried {
        NextClientAction::ExecutedLocalOnlyCreate(executed)
            if matches!(
                executed.executed,
                ExecutedLocalOnlyCreate::CreateRemoteDir(ref create_remote_dir)
                    if create_remote_dir.reused_pending_request
            ) => {}
        other => panic!("expected create_remote_dir retry, got {other:?}"),
    }

    let db = SqliteStateDb::open(&fixture.db_path).expect("reopen db after retry");
    assert_eq!(
        db.load_pending_client_mutation_for_client_file(&client_file_id)
            .expect("load pending after retry"),
        None
    );
}

#[test]
fn apply_remote_rename_retry_accepts_already_applied_winner() {
    let namespace_id = NamespaceId::new("ns-1");
    let fixture = ClientCrashFixture::new("crash-apply-remote-rename");
    let current_path = fixture.mirror_root.join("reports/report.txt");
    let target_path = fixture.mirror_root.join("reports/report-renamed.txt");
    write_file(&current_path, "same bytes");

    let digest = loon_types::sha256_digest(b"same bytes");
    seed_bound_file_state(
        &fixture.db_path,
        RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(12),
            revision_no: RevisionNo(7),
            content_digest: Some(digest.clone()),
            content_manifest_digest: Some("sha256:manifest-shared".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report-renamed.txt".to_owned(),
            is_deleted: false,
        },
        LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            content_digest: Some(digest.clone()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 10,
        },
        SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            synced_seq: ChangeSeq(11),
            revision_no: RevisionNo(7),
            content_digest: Some(digest),
            content_manifest_digest: Some("sha256:manifest-shared".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
        },
        PlannedActionRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            decision: "apply_remote_rename".to_owned(),
            reason: "remote_path_differs_from_anchor".to_owned(),
            created_at_ms: 10,
        },
    );

    fs::rename(&current_path, &target_path).expect("simulate crash after local rename");

    let retried = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen db");
        execute_next_client_action(
            &mut db,
            &fixture.store,
            |_client_file_id| None,
            |_namespace_id, _inode_id| Some(current_path.clone()),
            |_namespace_id, _inode_id, _parent_inode_id, _display_name| Some(target_path.clone()),
            100,
            201,
            |_request| unreachable!("rename should not dispatch"),
        )
        .expect("retry should succeed")
        .expect("retry should execute action")
    };

    assert_eq!(
        retried,
        NextClientAction::ExecutedApplyRemoteRename(ExecutedApplyRemoteRename {
            applied: loon_client::state_db::AppliedInodeMutation {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(42),
            },
        })
    );

    let db = SqliteStateDb::open(&fixture.db_path).expect("reopen db after retry");
    let views = db
        .load_file_sync_views(&namespace_id, InodeId(42))
        .expect("load file views");
    assert_eq!(
        views.local.expect("local row").display_name,
        "report-renamed.txt"
    );
    assert_eq!(
        db.load_planned_action(&namespace_id, InodeId(42))
            .expect("load planned action"),
        None
    );
}

#[test]
#[allow(clippy::result_large_err)]
fn download_remote_edit_retry_accepts_already_applied_winner() {
    let namespace_id = NamespaceId::new("ns-1");
    let fixture = ClientCrashFixture::new("crash-download-remote-edit");
    let current_path = fixture.mirror_root.join("reports/report.txt");
    write_file(&current_path, "anchor bytes");

    let anchor_upload =
        fixture.upload_content(&namespace_id, "download-anchor.txt", "anchor bytes");
    let remote_upload =
        fixture.upload_content(&namespace_id, "download-remote.txt", "remote winner");

    seed_bound_file_state(
        &fixture.db_path,
        RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(31),
            revision_no: RevisionNo(7),
            content_digest: Some(remote_upload.file_digest_sha256.clone()),
            content_manifest_digest: Some(remote_upload.content_manifest_digest.clone()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            is_deleted: false,
        },
        LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            content_digest: Some(anchor_upload.file_digest_sha256.clone()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 10,
        },
        SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            synced_seq: ChangeSeq(30),
            revision_no: RevisionNo(6),
            content_digest: Some(anchor_upload.file_digest_sha256.clone()),
            content_manifest_digest: Some(anchor_upload.content_manifest_digest.clone()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
        },
        PlannedActionRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            decision: "download_remote_edit".to_owned(),
            reason: "remote_differs_from_anchor".to_owned(),
            created_at_ms: 10,
        },
    );

    let controller = FaultController::new(
        load_fixture("client/client_fault_download_remote_edit_after_local_apply_once.yaml")
            .decode_fault_plan()
            .expect("decode fault plan"),
    );

    {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
        expect_injected_crash(|| {
            execute_next_client_action_with_faults(
                &mut db,
                &fixture.store,
                |_client_file_id| None,
                |_namespace_id, _inode_id| Some(current_path.clone()),
                |_namespace_id, _inode_id, _parent_inode_id, _display_name| None,
                100,
                101,
                &controller,
                |_request| unreachable!("download should not dispatch"),
            )
        });
    }

    assert_eq!(
        fs::read_to_string(&current_path).expect("read already-applied winner"),
        "remote winner"
    );

    let retried = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen db");
        execute_next_client_action(
            &mut db,
            &fixture.store,
            |_client_file_id| None,
            |_namespace_id, _inode_id| Some(current_path.clone()),
            |_namespace_id, _inode_id, _parent_inode_id, _display_name| None,
            100,
            201,
            |_request| unreachable!("download should not dispatch"),
        )
        .expect("retry should succeed")
        .expect("retry should execute action")
    };

    match retried {
        NextClientAction::ExecutedDownloadRemoteEdit(DownloadRemoteEditExecution::Completed(
            completed,
        )) => {
            assert_eq!(
                completed.downloaded_content_manifest_digest,
                remote_upload.content_manifest_digest
            );
            assert_eq!(
                completed.downloaded_file_digest_sha256,
                remote_upload.file_digest_sha256
            );
        }
        other => panic!("expected completed download retry, got {other:?}"),
    }

    let db = SqliteStateDb::open(&fixture.db_path).expect("reopen db after retry");
    let views = db
        .load_file_sync_views(&namespace_id, InodeId(42))
        .expect("load file views");
    let local = views.local.expect("local row");
    let anchor = views.sync_anchor.expect("anchor row");
    assert_eq!(
        local.content_digest,
        Some(remote_upload.file_digest_sha256.clone())
    );
    assert_eq!(
        anchor.content_manifest_digest,
        Some(remote_upload.content_manifest_digest.clone())
    );
    assert_eq!(
        db.load_planned_action(&namespace_id, InodeId(42))
            .expect("load planned action"),
        None
    );
}

#[test]
#[allow(clippy::result_large_err)]
fn same_inode_conflict_retry_reuses_existing_artifact_after_store_write_crash() {
    let namespace_id = NamespaceId::new("ns-1");
    let fixture = ClientCrashFixture::new("crash-conflict-artifact-after-store-write");
    let current_path = fixture.mirror_root.join("report.txt");
    write_file(&current_path, "local loser");

    let anchor_upload = fixture.upload_content(&namespace_id, "anchor.txt", "anchor bytes");
    let remote_upload = fixture.upload_content(&namespace_id, "remote.txt", "remote winner");

    seed_bound_file_state(
        &fixture.db_path,
        RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(41),
            revision_no: RevisionNo(7),
            content_digest: Some(remote_upload.file_digest_sha256.clone()),
            content_manifest_digest: Some(remote_upload.content_manifest_digest.clone()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            is_deleted: false,
        },
        LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            content_digest: Some(loon_types::sha256_digest(b"local loser")),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 10,
        },
        SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            synced_seq: ChangeSeq(40),
            revision_no: RevisionNo(6),
            content_digest: Some(anchor_upload.file_digest_sha256.clone()),
            content_manifest_digest: Some(anchor_upload.content_manifest_digest.clone()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
        },
        PlannedActionRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(42),
            decision: "resolve_same_inode_conflict".to_owned(),
            reason: "local_and_remote_differ_from_anchor".to_owned(),
            created_at_ms: 10,
        },
    );

    let controller = FaultController::new(
        load_fixture("client/client_fault_conflict_artifact_after_store_write_once.yaml")
            .decode_fault_plan()
            .expect("decode fault plan"),
    );

    {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
        expect_injected_crash(|| {
            execute_next_client_action_with_faults(
                &mut db,
                &fixture.store,
                |_client_file_id| None,
                |_namespace_id, _inode_id| Some(current_path.clone()),
                |_namespace_id, _inode_id, _parent_inode_id, _display_name| None,
                100,
                200,
                &controller,
                |_request| unreachable!("conflict resolution should not dispatch"),
            )
        });
    }

    assert_eq!(
        fixture
            .store
            .list_prefix(&conflict_artifact_prefix(namespace_id.as_str()))
            .expect("list artifacts after crash")
            .len(),
        1,
        "artifact object should already be durable"
    );
    {
        let db = SqliteStateDb::open(&fixture.db_path).expect("reopen db");
        assert!(
            db.load_conflict_artifacts_for_namespace(&namespace_id)
                .expect("load cached artifacts")
                .is_empty(),
            "cache write should not have happened yet"
        );
    }

    let retried = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen db");
        execute_next_client_action(
            &mut db,
            &fixture.store,
            |_client_file_id| None,
            |_namespace_id, _inode_id| Some(current_path.clone()),
            |_namespace_id, _inode_id, _parent_inode_id, _display_name| None,
            100,
            201,
            |_request| unreachable!("conflict resolution should not dispatch"),
        )
        .expect("retry should succeed")
        .expect("retry should execute")
    };

    match retried {
        NextClientAction::ExecutedResolveSameInodeConflict(ExecutedResolveSameInodeConflict {
            conflict_id,
            ..
        }) => {
            assert_eq!(
                fixture
                    .store
                    .list_prefix(&conflict_artifact_prefix(namespace_id.as_str()))
                    .expect("list artifacts after retry")
                    .len(),
                1
            );
            let db = SqliteStateDb::open(&fixture.db_path).expect("reopen db");
            let cached = db
                .load_conflict_artifact(&namespace_id, &conflict_id)
                .expect("load cached artifact");
            assert!(cached.is_some(), "artifact should be cached on retry");
            assert_eq!(
                fs::read_to_string(&current_path).expect("read winner bytes"),
                "remote winner"
            );
        }
        other => panic!("expected same-inode conflict execution, got {other:?}"),
    }
}

#[test]
#[allow(clippy::result_large_err)]
fn archive_retry_reconciles_sidecar_after_store_write_crash() {
    let namespace_id = NamespaceId::new("ns-1");
    let fixture = ClientCrashFixture::new("crash-archive-after-store-write");
    let artifact =
        seed_file_conflict_artifact(&fixture, &namespace_id, "report.txt", "local loser");
    let sidecar_key = conflict_artifact_archive(
        namespace_id.as_str(),
        artifact.envelope.conflict_id.as_str(),
    );
    let controller = FaultController::new(
        load_fixture("client/client_fault_archive_after_store_write_once.yaml")
            .decode_fault_plan()
            .expect("decode fault plan"),
    );

    {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
        expect_injected_crash(|| {
            archive_conflict_artifact_with_faults(
                &mut db,
                &fixture.store,
                &namespace_id,
                artifact.envelope.conflict_id.as_str(),
                1_700_000_000_200,
                &controller,
            )
        });
    }

    assert!(
        fixture
            .store
            .head(&sidecar_key)
            .expect("head archive sidecar")
            .is_some(),
        "archive sidecar should already exist after crash"
    );
    {
        let db = SqliteStateDb::open(&fixture.db_path).expect("reopen db");
        assert_eq!(
            db.load_conflict_artifact_archive(
                &namespace_id,
                artifact.envelope.conflict_id.as_str()
            )
            .expect("load cached archive row"),
            None
        );
    }

    let archived = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen db");
        archive_conflict_artifact(
            &mut db,
            &fixture.store,
            &namespace_id,
            artifact.envelope.conflict_id.as_str(),
            1_700_000_000_201,
        )
        .expect("retry archive should succeed")
    };
    assert_eq!(archived.object_key, sidecar_key);
}

#[test]
#[allow(clippy::result_large_err)]
fn subtree_restore_retry_is_absent_or_complete_after_stage_ready_crash() {
    let namespace_id = NamespaceId::new("ns-1");
    let fixture = ClientCrashFixture::new("crash-subtree-restore-stage");
    let destination_root = fixture.mirror_root.join("recovered/reports-loser");
    fs::create_dir_all(destination_root.parent().expect("restore parent"))
        .expect("create restore parent");
    let artifact = seed_subtree_conflict_artifact(&fixture, &namespace_id);

    {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
        discover_conflict_artifacts_for_namespace(&mut db, &fixture.store, &namespace_id)
            .expect("discover artifact before restore");
    }

    let controller = FaultController::new(
        load_fixture("client/client_fault_restore_subtree_after_stage_ready_once.yaml")
            .decode_fault_plan()
            .expect("decode fault plan"),
    );

    {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("open db");
        expect_injected_crash(|| {
            restore_conflict_artifact_to_path_with_faults(
                &mut db,
                &fixture.store,
                &namespace_id,
                artifact.envelope.conflict_id.as_str(),
                &destination_root,
                &controller,
            )
        });
    }

    assert!(
        !destination_root.exists(),
        "explicit destination must remain absent after crash"
    );

    let restored = {
        let mut db = SqliteStateDb::open(&fixture.db_path).expect("reopen db");
        restore_subtree_conflict_artifact_to_path(
            &mut db,
            &fixture.store,
            &namespace_id,
            artifact.envelope.conflict_id.as_str(),
            &destination_root,
        )
        .expect("retry restore should succeed")
    };

    assert_eq!(
        restored.restored_entry_count,
        artifact.envelope.entries.len()
    );
    assert_eq!(
        fs::read_to_string(destination_root.join("docs/report.txt")).expect("read restored file"),
        "dirty subtree draft"
    );
    assert_eq!(
        fs::read_to_string(destination_root.join("notes.txt")).expect("read restored notes"),
        "extra loser note"
    );
}

struct ClientCrashFixture {
    _temp_dir: TestDir,
    db_path: PathBuf,
    mirror_root: PathBuf,
    upload_root: PathBuf,
    store: LocalFsStore,
}

impl ClientCrashFixture {
    fn new(label: &str) -> Self {
        let temp_dir = TestDir::new(label);
        let db_path = temp_dir.path().join("client.sqlite3");
        let mirror_root = temp_dir.path().join("mirror");
        let upload_root = temp_dir.path().join("uploads");
        let store_root = temp_dir.path().join("objectstore");
        fs::create_dir_all(&mirror_root).expect("create mirror root");
        fs::create_dir_all(&upload_root).expect("create upload root");
        fs::create_dir_all(&store_root).expect("create store root");
        let store = LocalFsStore::new(&store_root).expect("create local store");
        Self {
            _temp_dir: temp_dir,
            db_path,
            mirror_root,
            upload_root,
            store,
        }
    }

    fn upload_content(
        &self,
        namespace_id: &NamespaceId,
        file_name: &str,
        content: &str,
    ) -> UploadedContent {
        let path = self.upload_root.join(file_name);
        write_file(&path, content);
        upload_small_file_from_path(&self.store, namespace_id, &path)
            .expect("upload fixture content")
    }
}

fn seed_local_only_dir_state(
    db_path: &Path,
    local_only: &LocalOnlyFileStateRow,
    planned: &LocalOnlyPlannedActionRow,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open db");
    db.planner_transaction("seed-local-only-dir", |tx| {
        tx.upsert_local_only_file(local_only)?;
        tx.upsert_planned_local_only_action(planned)?;
        Ok(())
    })
    .expect("seed local-only dir state");
}

fn seed_bound_file_state(
    db_path: &Path,
    remote: RemoteFileStateRow,
    local: LocalFileStateRow,
    anchor: SyncAnchorRow,
    planned: PlannedActionRow,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open db");
    db.planner_transaction("seed-bound-file-state", |tx| {
        tx.upsert_remote_file(&remote)?;
        tx.upsert_local_file(&local)?;
        tx.upsert_sync_anchor(&anchor)?;
        tx.upsert_planned_action(&planned)?;
        Ok(())
    })
    .expect("seed bound file state");
}

fn seed_head_and_lease(store: &LocalFsStore, head: &HeadState, lease: &LeaseState) {
    let head_envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        "loon-server-test",
        head.clone(),
    )
    .expect("encode head");
    store
        .put_if_absent(
            &namespace_head(head.namespace_id.as_str()),
            &serde_json::to_vec(&head_envelope).expect("serialize head"),
        )
        .expect("seed head");

    let lease_envelope = LeaseStateEnvelope::from_state(
        ControlObjectKind::NamespaceLease,
        "loon-server-test",
        lease.clone(),
    )
    .expect("encode lease");
    store
        .put_if_absent(
            &namespace_lease(lease.namespace_id.as_str()),
            &serde_json::to_vec(&lease_envelope).expect("serialize lease"),
        )
        .expect("seed lease");
}

fn dispatch_request(
    store: &LocalFsStore,
    request: &ClientMutationRequest,
    now_ms: u64,
) -> Result<ClientMutationResponse, String> {
    support::seed_server_basis_for_request(store, request, "loon-server-test");
    execute_client_mutation(
        store,
        request,
        &ClientMutationExecutionParams {
            writer_id: "writer-a".to_owned(),
            writer_version: "loon-server-test".to_owned(),
            now_ms,
            lease_duration_ms: 60_000,
        },
    )
    .map(|executed| executed.response)
    .map_err(|err| err.to_string())
}

fn seed_file_conflict_artifact(
    fixture: &ClientCrashFixture,
    namespace_id: &NamespaceId,
    display_name: &str,
    loser_content: &str,
) -> loon_client::PreparedConflictArtifact {
    let uploaded = fixture.upload_content(namespace_id, "artifact-loser-file.txt", loser_content);
    let artifact = prepare_conflict_artifact(
        namespace_id,
        ConflictClass::SameInodeStaleBaseEdit,
        ChangeSeq(41),
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
        1_700_000_000_100,
    );
    write_conflict_artifact_if_absent(&fixture.store, &artifact).expect("write file artifact");
    artifact
}

fn seed_subtree_conflict_artifact(
    fixture: &ClientCrashFixture,
    namespace_id: &NamespaceId,
) -> loon_client::PreparedSubtreeConflictArtifact {
    let docs_upload =
        fixture.upload_content(namespace_id, "subtree-report.txt", "dirty subtree draft");
    let notes_upload =
        fixture.upload_content(namespace_id, "subtree-notes.txt", "extra loser note");
    let entries = vec![
        SubtreeConflictArtifactEntry {
            relative_path: "docs".to_owned(),
            inode_kind: InodeKind::Dir,
            inode_id: Some(InodeId(100)),
            client_file_id: None,
            base_revision_no: None,
            content_manifest_digest: None,
            content_digest: None,
            parent_inode_id: Some(InodeId(10)),
            display_name: "docs".to_owned(),
        },
        SubtreeConflictArtifactEntry {
            relative_path: "docs/report.txt".to_owned(),
            inode_kind: InodeKind::File,
            inode_id: Some(InodeId(101)),
            client_file_id: None,
            base_revision_no: Some(RevisionNo(1)),
            content_manifest_digest: Some(docs_upload.content_manifest_digest.clone()),
            content_digest: Some(docs_upload.file_digest_sha256.clone()),
            parent_inode_id: Some(InodeId(10)),
            display_name: "report.txt".to_owned(),
        },
        SubtreeConflictArtifactEntry {
            relative_path: "notes.txt".to_owned(),
            inode_kind: InodeKind::File,
            inode_id: None,
            client_file_id: Some("temp-note-1".to_owned()),
            base_revision_no: None,
            content_manifest_digest: Some(notes_upload.content_manifest_digest.clone()),
            content_digest: Some(notes_upload.file_digest_sha256.clone()),
            parent_inode_id: Some(InodeId(10)),
            display_name: "notes.txt".to_owned(),
        },
    ];
    let artifact = prepare_subtree_conflict_artifact(
        namespace_id,
        ConflictClass::SubtreeRenameVsLocalChanges,
        ChangeSeq(61),
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
        entries,
        1_700_000_000_300,
    );
    write_subtree_conflict_artifact_if_absent(&fixture.store, &artifact)
        .expect("write subtree artifact");
    artifact
}

fn write_file(path: &Path, content: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create file parent");
    }
    fs::write(path, content.as_bytes()).expect("write file");
}

fn expect_injected_crash<T, E>(f: impl FnOnce() -> Result<T, E>) {
    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = f();
    }))
    .expect_err("fault plan should crash exactly once");
    let message = panic_message(&panic);
    assert!(
        message.contains("injected_crash"),
        "unexpected panic payload: {message}"
    );
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_owned();
    }
    "non-string panic".to_owned()
}
