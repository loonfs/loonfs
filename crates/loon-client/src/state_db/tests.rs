use super::{
    BoundLocalOnlyFile, ClientFileId, FileSyncViews, LocalFileStateRow, LocalOnlyFileStateRow,
    LocalOnlyPlannedActionRow, LocalOnlyUploadRow, ObservedLocalOnlyInode,
    PendingClientMutationRow, PlannedActionRow, RemoteFileStateRow, SqliteStateDb, StateDbError,
    SyncAnchorRow, SCHEMA_VERSION,
};
use crate::upload::UploadedContent;
use loon_types::{
    ChangeSeq, ClientMutationOp, ClientMutationRequest, ClientMutationResponse,
    ContentManifestEnvelope, ContentManifestPayload, CreatedRemoteInode, InodeId, InodeKind,
    NamespaceId, RevisionNo, CONTENT_BLOCK_SIZE_BYTES,
};

#[test]
fn sqlite_state_db_applies_schema_v8() {
    let db = SqliteStateDb::open_in_memory().expect("open in-memory DB");

    assert_eq!(
        db.schema_version().expect("read schema version"),
        SCHEMA_VERSION
    );

    for table in [
        "remote_state",
        "local_state",
        "sync_anchor",
        "planned_actions",
        "client_metadata",
        "local_only_state",
        "planned_local_only_actions",
        "local_only_uploads",
        "inode_uploads",
        "pending_client_mutations",
        "pending_inode_mutations",
        "transfer_ledger",
        "conflicts_and_errors",
    ] {
        let exists: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                [table],
                |row| row.get(0),
            )
            .expect("query sqlite_master");
        assert_eq!(exists, 1, "expected table {table} to exist");
    }
}

#[test]
fn planner_transaction_rolls_back_partial_write() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let namespace_id = NamespaceId::from("ns-1");
    let inode_id = InodeId(42);

    let error = db
        .planner_transaction("rollback-test", |tx| {
            tx.upsert_remote_file(&sample_remote())?;
            Err::<(), _>(StateDbError::UnsupportedSchemaVersion(99))
        })
        .expect_err("transaction should fail");

    assert!(matches!(error, StateDbError::UnsupportedSchemaVersion(99)));
    assert_eq!(
        db.load_file_sync_views(&namespace_id, inode_id)
            .expect("load views after rollback"),
        FileSyncViews {
            namespace_id,
            inode_id,
            remote: None,
            local: None,
            sync_anchor: None,
        }
    );
}

#[test]
fn planner_transaction_persists_three_views() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let namespace_id = NamespaceId::from("ns-1");
    let inode_id = InodeId(42);

    db.planner_transaction("seed-views", |tx| {
        tx.upsert_remote_file(&sample_remote())?;
        tx.upsert_local_file(&sample_local())?;
        tx.upsert_sync_anchor(&sample_anchor())?;
        Ok(())
    })
    .expect("seed state");

    let views = db
        .load_file_sync_views(&namespace_id, inode_id)
        .expect("load seeded views");
    assert_eq!(views.remote, Some(sample_remote()));
    assert_eq!(views.local, Some(sample_local()));
    assert_eq!(views.sync_anchor, Some(sample_anchor()));
}

#[test]
fn allocate_local_file_ids_monotonically() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let first = db
        .allocate_local_file_id(&NamespaceId::from("ns-1"))
        .expect("allocate first temp id");
    let second = db
        .allocate_local_file_id(&NamespaceId::from("ns-1"))
        .expect("allocate second temp id");

    assert_eq!(first, ClientFileId::from("tmp:ns-1:00000000000000000001"));
    assert_eq!(second, ClientFileId::from("tmp:ns-1:00000000000000000002"));
}

#[test]
fn allocate_client_request_ids_monotonically() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let first = db
        .allocate_client_request_id()
        .expect("allocate first request id");
    let second = db
        .allocate_client_request_id()
        .expect("allocate second request id");

    assert_eq!(first, "client-req-00000000000000000001");
    assert_eq!(second, "client-req-00000000000000000002");
}

#[test]
fn planner_transaction_persists_local_only_state() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let local_only = sample_local_only();

    db.planner_transaction("seed-local-only", |tx| {
        tx.upsert_local_only_file(&local_only)?;
        Ok(())
    })
    .expect("seed local-only state");

    assert_eq!(
        db.load_local_only_file(&local_only.client_file_id)
            .expect("load local-only state"),
        Some(local_only)
    );
}

#[test]
fn load_next_planned_local_only_action_orders_deterministically() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");

    db.planner_transaction("seed-planned-local-only-actions", |tx| {
        tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
            client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000003"),
            namespace_id: NamespaceId::from("ns-1"),
            decision: "upload_local_create".to_owned(),
            reason: "local_only_file_without_remote_identity".to_owned(),
            created_at_ms: 1_700_000_300_000,
        })?;
        tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
            client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000002"),
            namespace_id: NamespaceId::from("ns-1"),
            decision: "create_remote_dir".to_owned(),
            reason: "local_only_directory_without_remote_identity".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })?;
        tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
            client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
            namespace_id: NamespaceId::from("ns-1"),
            decision: "upload_local_create".to_owned(),
            reason: "local_only_file_without_remote_identity".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })?;
        Ok(())
    })
    .expect("seed planned local-only actions");

    assert_eq!(
        db.load_next_planned_local_only_action()
            .expect("load next planned local-only action"),
        Some(LocalOnlyPlannedActionRow {
            client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
            namespace_id: NamespaceId::from("ns-1"),
            decision: "upload_local_create".to_owned(),
            reason: "local_only_file_without_remote_identity".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })
    );
}

#[test]
fn load_next_planned_action_orders_deterministically() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");

    db.planner_transaction("seed-planned-actions", |tx| {
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-2"),
            inode_id: InodeId(9),
            decision: "download_remote_edit".to_owned(),
            reason: "remote_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_300_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(8),
            decision: "upload_local_edit".to_owned(),
            reason: "local_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(7),
            decision: "download_remote_edit".to_owned(),
            reason: "remote_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })?;
        Ok(())
    })
    .expect("seed planned actions");

    assert_eq!(
        db.load_next_planned_action()
            .expect("load next planned action"),
        Some(PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(7),
            decision: "download_remote_edit".to_owned(),
            reason: "remote_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })
    );
}

#[test]
fn load_next_executable_planned_action_skips_deferred_rows() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");

    db.planner_transaction("seed-executable-and-deferred-actions", |tx| {
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(7),
            decision: "create_conflict_copy".to_owned(),
            reason: "local_and_remote_differ_from_anchor".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(8),
            decision: "download_remote_edit".to_owned(),
            reason: "remote_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_205_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(9),
            decision: "upload_local_edit".to_owned(),
            reason: "local_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_210_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(6),
            decision: "materialize_remote_dir".to_owned(),
            reason: "remote_observed_without_anchor".to_owned(),
            created_at_ms: 1_700_000_202_000,
        })?;
        Ok(())
    })
    .expect("seed planned actions");

    assert_eq!(
        db.load_next_executable_planned_action()
            .expect("load next executable planned action"),
        Some(PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(6),
            decision: "materialize_remote_dir".to_owned(),
            reason: "remote_observed_without_anchor".to_owned(),
            created_at_ms: 1_700_000_202_000,
        })
    );
}

#[test]
fn load_next_deferred_planned_action_skips_executable_rows() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");

    db.planner_transaction("seed-executable-and-deferred-actions", |tx| {
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(8),
            decision: "download_remote_edit".to_owned(),
            reason: "remote_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_205_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(7),
            decision: "create_conflict_copy".to_owned(),
            reason: "local_and_remote_differ_from_anchor".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(9),
            decision: "upload_local_edit".to_owned(),
            reason: "local_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_210_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(6),
            decision: "materialize_remote_dir".to_owned(),
            reason: "remote_observed_without_anchor".to_owned(),
            created_at_ms: 1_700_000_202_000,
        })?;
        Ok(())
    })
    .expect("seed planned actions");

    assert_eq!(
        db.load_next_deferred_planned_action()
            .expect("load next deferred planned action"),
        Some(PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(7),
            decision: "create_conflict_copy".to_owned(),
            reason: "local_and_remote_differ_from_anchor".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })
    );
}

#[test]
fn record_local_only_upload_persists_and_resolves_manifest_digest() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let local_only = sample_local_only();
    let uploaded = sample_uploaded_content();

    db.planner_transaction("seed-local-only", |tx| {
        tx.upsert_local_only_file(&local_only)?;
        Ok(())
    })
    .expect("seed local-only state");

    let recorded = db
        .record_local_only_upload(&local_only.client_file_id, &uploaded, 1_700_000_104_000)
        .expect("record local-only upload");

    assert_eq!(
        recorded,
        LocalOnlyUploadRow {
            client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
            namespace_id: NamespaceId::from("ns-1"),
            file_digest_sha256: "sha256:new-local-file".to_owned(),
            content_manifest_digest: "sha256:manifest-new-local-file".to_owned(),
            manifest_object_key: "namespaces/ns-1/manifests/sha256:manifest-new-local-file.json"
                .to_owned(),
            file_size_bytes: 15,
            uploaded_at_ms: 1_700_000_104_000,
        }
    );
    assert_eq!(
        db.load_local_only_upload(&local_only.client_file_id)
            .expect("load persisted upload"),
        Some(recorded)
    );
    assert_eq!(
        db.resolve_local_only_upload_content_manifest_digest(&local_only)
            .expect("resolve content manifest digest"),
        "sha256:manifest-new-local-file"
    );
}

#[test]
fn record_local_only_upload_rejects_mismatched_digest() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let mut local_only = sample_local_only();
    local_only.content_digest = Some("sha256:edited-after-upload".to_owned());

    db.planner_transaction("seed-local-only", |tx| {
        tx.upsert_local_only_file(&local_only)?;
        Ok(())
    })
    .expect("seed local-only state");

    let error = db
        .record_local_only_upload(
            &local_only.client_file_id,
            &sample_uploaded_content(),
            1_700_000_104_000,
        )
        .expect_err("stale upload should be rejected");

    assert!(matches!(
        error,
        StateDbError::UploadedContentDigestMismatch { .. }
    ));
}

#[test]
fn record_pending_client_mutation_persists_and_reuses_same_mapping() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let request = sample_client_create_file_request();
    let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");

    let recorded = db
        .record_pending_client_mutation(&client_file_id, &request, 1_700_000_106_000)
        .expect("record pending mutation");
    let reused = db
        .record_pending_client_mutation(&client_file_id, &request, 1_700_000_106_999)
        .expect("reuse same pending mutation");

    assert_eq!(
        recorded,
        PendingClientMutationRow {
            client_request_id: "client-req-0001".to_owned(),
            namespace_id: NamespaceId::from("ns-1"),
            client_file_id: client_file_id.clone(),
            request: request.clone(),
            created_at_ms: 1_700_000_106_000,
        }
    );
    assert_eq!(reused, recorded);
    assert_eq!(
        db.load_pending_client_mutation("client-req-0001")
            .expect("load pending mutation"),
        Some(recorded.clone())
    );
    assert_eq!(
        db.load_pending_client_mutation_for_client_file(&client_file_id)
            .expect("load pending mutation by client file"),
        Some(recorded)
    );
}

#[test]
fn record_pending_client_mutation_rejects_conflicting_temp_identity() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let request = sample_client_create_file_request();

    db.record_pending_client_mutation(
        &ClientFileId::from("tmp:ns-1:00000000000000000001"),
        &request,
        1_700_000_106_000,
    )
    .expect("record pending mutation");

    let error = db
        .record_pending_client_mutation(
            &ClientFileId::from("tmp:ns-1:00000000000000000002"),
            &request,
            1_700_000_106_001,
        )
        .expect_err("conflicting mapping should fail");

    assert!(matches!(
        error,
        StateDbError::PendingClientMutationConflict { .. }
    ));
}

#[test]
fn record_pending_client_mutation_rejects_conflicting_request_for_same_client_file() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");

    db.record_pending_client_mutation(
        &client_file_id,
        &sample_client_create_file_request(),
        1_700_000_106_000,
    )
    .expect("record first pending mutation");

    let error = db
        .record_pending_client_mutation(
            &client_file_id,
            &ClientMutationRequest {
                namespace_id: NamespaceId::from("ns-1"),
                client_request_id: "client-req-0002".to_owned(),
                op: ClientMutationOp::CreateFile {
                    parent_inode_id: InodeId(2),
                    display_name: "draft.txt".to_owned(),
                    content_manifest_digest: "sha256:different-manifest".to_owned(),
                },
            },
            1_700_000_106_001,
        )
        .expect_err("conflicting request for same client file should fail");

    assert!(matches!(
        error,
        StateDbError::PendingClientMutationClientFileConflict { .. }
    ));
}

#[test]
fn observe_local_only_inode_under_bound_parent_allocates_and_persists_child() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    seed_bound_directory_parent(&mut db, InodeId(902));

    let observed = ObservedLocalOnlyInode {
        namespace_id: NamespaceId::from("ns-1"),
        inode_kind: InodeKind::File,
        parent_inode_id: InodeId(902),
        display_name: "note.txt".to_owned(),
        content_digest: Some("sha256:child-note".to_owned()),
        exists_on_disk: true,
        dirty: true,
        last_local_change_ms: 1_700_000_300_000,
    };

    let persisted = db
        .observe_local_only_inode_under_parent(&observed)
        .expect("observe local-only child");

    assert_eq!(
        persisted,
        LocalOnlyFileStateRow {
            client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
            namespace_id: NamespaceId::from("ns-1"),
            inode_kind: InodeKind::File,
            parent_inode_id: Some(InodeId(902)),
            display_name: "note.txt".to_owned(),
            content_digest: Some("sha256:child-note".to_owned()),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 1_700_000_300_000,
        }
    );
    assert_eq!(
        db.load_local_only_file(&persisted.client_file_id)
            .expect("load persisted child"),
        Some(persisted)
    );
}

#[test]
fn observe_local_only_inode_under_parent_rejects_unbound_parent() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    db.planner_transaction("seed-unbound-parent", |tx| {
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(902),
            inode_kind: InodeKind::Dir,
            observed_seq: ChangeSeq(501),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: Some(InodeId(2)),
            display_name: "drafts".to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(902),
            inode_kind: InodeKind::Dir,
            content_digest: None,
            parent_inode_id: Some(InodeId(2)),
            display_name: "drafts-renamed".to_owned(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1_700_000_200_000,
        })?;
        tx.upsert_sync_anchor(&SyncAnchorRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(902),
            inode_kind: InodeKind::Dir,
            synced_seq: ChangeSeq(501),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: Some(InodeId(2)),
            display_name: "drafts".to_owned(),
        })?;
        Ok(())
    })
    .expect("seed unbound parent");

    let error = db
        .observe_local_only_inode_under_parent(&ObservedLocalOnlyInode {
            namespace_id: NamespaceId::from("ns-1"),
            inode_kind: InodeKind::File,
            parent_inode_id: InodeId(902),
            display_name: "note.txt".to_owned(),
            content_digest: Some("sha256:child-note".to_owned()),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 1_700_000_300_000,
        })
        .expect_err("observe should reject unbound parent");

    assert!(matches!(
        error,
        StateDbError::LocalOnlyParentNotBound {
            parent_inode_id: 902,
            ..
        }
    ));
}

#[test]
fn bind_local_only_file_to_remote_migrates_into_inode_keyed_tables() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");
    let remote = sample_bound_remote();

    db.planner_transaction("seed-bindable-local-only", |tx| {
        tx.upsert_local_only_file(&sample_local_only())?;
        tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
            client_file_id: client_file_id.clone(),
            namespace_id: NamespaceId::from("ns-1"),
            decision: "upload_local_create".to_owned(),
            reason: "local_only_file_without_remote_identity".to_owned(),
            created_at_ms: 1_700_000_105_000,
        })?;
        tx.record_local_only_upload(
            &client_file_id,
            &sample_uploaded_content(),
            1_700_000_104_000,
        )?;
        Ok(())
    })
    .expect("seed bindable local-only state");

    let bound = db
        .bind_local_only_file_to_remote(&client_file_id, &remote)
        .expect("bind local-only file");

    assert_eq!(
        bound,
        BoundLocalOnlyFile {
            client_file_id: client_file_id.clone(),
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(901),
        }
    );
    assert_eq!(
        db.load_file_sync_views(&NamespaceId::from("ns-1"), InodeId(901))
            .expect("load bound views"),
        FileSyncViews {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(901),
            remote: Some(remote.clone()),
            local: Some(LocalFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(901),
                inode_kind: InodeKind::File,
                content_digest: Some("sha256:new-local-file".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_100_000,
            }),
            sync_anchor: Some(SyncAnchorRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(901),
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(500),
                revision_no: RevisionNo(1),
                content_digest: Some("sha256:new-local-file".to_owned()),
                content_manifest_digest: Some("sha256:manifest-new-local-file".to_owned(),),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
            }),
        }
    );
    assert_eq!(
        db.load_local_only_file(&client_file_id)
            .expect("load temp local-only state"),
        None
    );
    assert_eq!(
        db.load_planned_local_only_action(&client_file_id)
            .expect("load temp planned action"),
        None
    );
    assert_eq!(
        db.load_local_only_upload(&client_file_id)
            .expect("load temp upload row"),
        None
    );
}

#[test]
fn bind_local_only_file_to_remote_rejects_mismatched_observation_without_partial_write() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");
    let mut remote = sample_bound_remote();
    remote.display_name = "renamed.txt".to_owned();

    db.planner_transaction("seed-mismatched-local-only", |tx| {
        tx.upsert_local_only_file(&sample_local_only())?;
        Ok(())
    })
    .expect("seed local-only state");

    let error = db
        .bind_local_only_file_to_remote(&client_file_id, &remote)
        .expect_err("bind should reject mismatched observation");

    assert!(matches!(
        error,
        StateDbError::BindObservationMismatch {
            field: "display_name",
            ..
        }
    ));
    assert_eq!(
        db.load_file_sync_views(&NamespaceId::from("ns-1"), InodeId(901))
            .expect("load views after rejected bind"),
        FileSyncViews {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(901),
            remote: None,
            local: None,
            sync_anchor: None,
        }
    );
    assert_eq!(
        db.load_local_only_file(&client_file_id)
            .expect("load temp local-only state after rejection"),
        Some(sample_local_only())
    );
}

#[test]
fn apply_client_mutation_response_binds_and_clears_pending_row() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let request = sample_client_create_file_request();
    let response = sample_client_create_file_response();
    let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");

    db.planner_transaction("seed-local-only-and-plan", |tx| {
        tx.upsert_local_only_file(&sample_local_only())?;
        tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
            client_file_id: client_file_id.clone(),
            namespace_id: NamespaceId::from("ns-1"),
            decision: "upload_local_create".to_owned(),
            reason: "local_only_file_without_remote_identity".to_owned(),
            created_at_ms: 1_700_000_105_000,
        })?;
        tx.record_local_only_upload(
            &client_file_id,
            &sample_uploaded_content(),
            1_700_000_104_000,
        )?;
        tx.record_pending_client_mutation(&client_file_id, &request, 1_700_000_106_000)?;
        Ok(())
    })
    .expect("seed local-only state and pending mutation");

    let bound = db
        .apply_client_mutation_response(&response)
        .expect("apply client mutation response");

    assert_eq!(
        bound,
        BoundLocalOnlyFile {
            client_file_id: client_file_id.clone(),
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(501),
        }
    );
    assert_eq!(
        db.load_file_sync_views(&NamespaceId::from("ns-1"), InodeId(501))
            .expect("load bound views"),
        FileSyncViews {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(501),
            remote: Some(RemoteFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(501),
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(42),
                revision_no: RevisionNo(1),
                content_digest: Some("sha256:new-local-file".to_owned()),
                content_manifest_digest: Some("sha256:new-local-file".to_owned(),),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
                is_deleted: false,
            }),
            local: Some(LocalFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(501),
                inode_kind: InodeKind::File,
                content_digest: Some("sha256:new-local-file".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_100_000,
            }),
            sync_anchor: Some(SyncAnchorRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(501),
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(42),
                revision_no: RevisionNo(1),
                content_digest: Some("sha256:new-local-file".to_owned()),
                content_manifest_digest: Some("sha256:new-local-file".to_owned(),),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
            }),
        }
    );
    assert_eq!(
        db.load_pending_client_mutation("client-req-0001")
            .expect("load pending mutation after bind"),
        None
    );
    assert_eq!(
        db.load_local_only_file(&client_file_id)
            .expect("load temp local-only state after bind"),
        None
    );
    assert_eq!(
        db.load_local_only_upload(&client_file_id)
            .expect("load temp upload row after bind"),
        None
    );
}

fn sample_remote() -> RemoteFileStateRow {
    RemoteFileStateRow {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(42),
        inode_kind: InodeKind::File,
        observed_seq: ChangeSeq(420),
        revision_no: RevisionNo(18),
        content_digest: Some("sha256:remote-18".to_owned()),
        content_manifest_digest: Some("sha256:manifest-remote-18".to_owned()),
        parent_inode_id: Some(InodeId(2)),
        display_name: "report.txt".to_owned(),
        is_deleted: false,
    }
}

fn sample_local() -> LocalFileStateRow {
    LocalFileStateRow {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(42),
        inode_kind: InodeKind::File,
        content_digest: Some("sha256:local-edit".to_owned()),
        parent_inode_id: Some(InodeId(2)),
        display_name: "report.txt".to_owned(),
        exists_on_disk: true,
        dirty: true,
        last_local_change_ms: 1_700_000_001_000,
    }
}

fn sample_anchor() -> SyncAnchorRow {
    SyncAnchorRow {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(42),
        inode_kind: InodeKind::File,
        synced_seq: ChangeSeq(419),
        revision_no: RevisionNo(17),
        content_digest: Some("sha256:anchor-17".to_owned()),
        content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
        parent_inode_id: Some(InodeId(2)),
        display_name: "report.txt".to_owned(),
    }
}

fn sample_local_only() -> LocalOnlyFileStateRow {
    LocalOnlyFileStateRow {
        client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
        namespace_id: NamespaceId::from("ns-1"),
        inode_kind: InodeKind::File,
        parent_inode_id: Some(InodeId(2)),
        display_name: "draft.txt".to_owned(),
        content_digest: Some("sha256:new-local-file".to_owned()),
        exists_on_disk: true,
        dirty: true,
        last_local_change_ms: 1_700_000_100_000,
    }
}

fn sample_bound_remote() -> RemoteFileStateRow {
    RemoteFileStateRow {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(901),
        inode_kind: InodeKind::File,
        observed_seq: ChangeSeq(500),
        revision_no: RevisionNo(1),
        content_digest: Some("sha256:new-local-file".to_owned()),
        content_manifest_digest: Some("sha256:manifest-new-local-file".to_owned()),
        parent_inode_id: Some(InodeId(2)),
        display_name: "draft.txt".to_owned(),
        is_deleted: false,
    }
}

fn sample_client_create_file_request() -> ClientMutationRequest {
    ClientMutationRequest {
        namespace_id: NamespaceId::from("ns-1"),
        client_request_id: "client-req-0001".to_owned(),
        op: ClientMutationOp::CreateFile {
            parent_inode_id: InodeId(2),
            display_name: "draft.txt".to_owned(),
            content_manifest_digest: "sha256:new-local-file".to_owned(),
        },
    }
}

fn sample_client_create_file_response() -> ClientMutationResponse {
    ClientMutationResponse {
        namespace_id: NamespaceId::from("ns-1"),
        client_request_id: "client-req-0001".to_owned(),
        committed_seq: ChangeSeq(42),
        created_inode: Some(CreatedRemoteInode {
            inode_id: InodeId(501),
            inode_kind: InodeKind::File,
            revision_no: RevisionNo(1),
            parent_inode_id: InodeId(2),
            display_name: "draft.txt".to_owned(),
            content_digest: Some("sha256:new-local-file".to_owned()),
        }),
        replaced_file: None,
    }
}

fn sample_uploaded_content() -> UploadedContent {
    UploadedContent {
        namespace_id: NamespaceId::from("ns-1"),
        file_size_bytes: 15,
        file_digest_sha256: "sha256:new-local-file".to_owned(),
        content_manifest_digest: "sha256:manifest-new-local-file".to_owned(),
        manifest_object_key: "namespaces/ns-1/manifests/sha256:manifest-new-local-file.json"
            .to_owned(),
        manifest_envelope: ContentManifestEnvelope::from_payload(ContentManifestPayload {
            namespace_id: NamespaceId::from("ns-1"),
            file_size_bytes: 15,
            file_digest_sha256: "sha256:new-local-file".to_owned(),
            block_size_bytes: CONTENT_BLOCK_SIZE_BYTES,
            blocks: Vec::new(),
        })
        .expect("build sample manifest envelope"),
        block_objects: Vec::new(),
    }
}

fn seed_bound_directory_parent(db: &mut SqliteStateDb, inode_id: InodeId) {
    db.planner_transaction("seed-bound-directory-parent", |tx| {
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id,
            inode_kind: InodeKind::Dir,
            observed_seq: ChangeSeq(501),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: Some(InodeId(2)),
            display_name: "drafts".to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id,
            inode_kind: InodeKind::Dir,
            content_digest: None,
            parent_inode_id: Some(InodeId(2)),
            display_name: "drafts".to_owned(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1_700_000_200_000,
        })?;
        tx.upsert_sync_anchor(&SyncAnchorRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id,
            inode_kind: InodeKind::Dir,
            synced_seq: ChangeSeq(501),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: Some(InodeId(2)),
            display_name: "drafts".to_owned(),
        })?;
        Ok(())
    })
    .expect("seed bound directory parent");
}
