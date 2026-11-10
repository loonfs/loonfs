//! Retention advances in a writer session's queue: the order of the
//! session's undeletes around an advance, a takeover between the advance's
//! fold and its floor, a commit of an older writer between the advance's
//! claim and its fence, and an advance by another runtime.

use super::*;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_types::format::manifest::{decode_namespace_manifest_json, NamespaceManifestPayload};
use loonfs_types::{
    DeleteDirectoryBehavior, ErrorCode, FilesystemChange, InodeId, ManifestNo, WalNo,
};

/// The inode rows that the current manifest's segments store.
async fn inode_rows(store: &dyn ObjectStore, namespace_id: &NamespaceId) -> u64 {
    loonfs_core::test_support::namespace_store(store, namespace_id)
        .load_statistics()
        .await
        .expect("load the namespace's statistics")
        .inode_record_count
}

/// The namespace's current manifest.
async fn current_manifest(
    store: &dyn ObjectStore,
    namespace_id: &NamespaceId,
) -> NamespaceManifestPayload {
    loonfs_core::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest")
        .payload()
        .clone()
}

/// The manifest numbered `manifest_no`.
async fn manifest_at(
    store: &dyn ObjectStore,
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
) -> NamespaceManifestPayload {
    let bytes = store
        .get(&metadata_manifest_object(namespace_id, &manifest_no), None)
        .await
        .expect("read the manifest")
        .expect("the manifest exists");
    decode_namespace_manifest_json(&bytes)
        .expect("decode the manifest")
        .into_payload()
}

/// The number of the WAL object that commits `committed_seq`.
async fn wal_no_of_commit(
    store: &dyn ObjectStore,
    namespace_id: &NamespaceId,
    committed_seq: ChangeSeq,
) -> WalNo {
    for key in store
        .list_prefix(&wal_prefix(namespace_id))
        .await
        .expect("list the WAL")
    {
        let bytes = store
            .get(&key, None)
            .await
            .expect("read a WAL object")
            .expect("the WAL object exists");
        let payload = decode_wal_object_envelope_zstd(&bytes)
            .expect("decode a WAL object")
            .into_payload();
        if payload
            .records
            .iter()
            .any(|record| record.committed_seq == committed_seq)
        {
            return payload.wal_no;
        }
    }
    panic!("no WAL object commits sequence {committed_seq}")
}

/// Runs compaction units through `maintenance` until none is needed, and
/// returns how many published.
async fn drain_compaction(maintenance: &crate::Maintenance, namespace_id: &NamespaceId) -> usize {
    for published in 0..32 {
        let response = maintenance
            .compact_metadata(namespace_id, &crate::MaintenanceCancellation::new())
            .await
            .expect("compact");
        if response.compaction == crate::MetadataCompactionOutcome::NotNeeded {
            return published;
        }
    }
    panic!("compaction kept publishing");
}

/// Creates `/{name}` with `/{name}/inner` in it through `namespace`'s
/// session.
async fn create_directories(namespace: &crate::Namespace<crate::Writable>, name: &str) {
    for path in [name.to_owned(), format!("{name}/inner")] {
        namespace
            .commit_candidate(CommitCandidate::new(create_directory_request(
                format!("create-{}", path.replace('/', "-")),
                &path,
            )))
            .await
            .expect("create a directory");
    }
}

/// Deletes `/{name}` through `namespace`'s session, and returns the deleted
/// root with the deletion's sequence.
async fn delete_a_directory(
    namespace: &crate::Namespace<crate::Writable>,
    name: &str,
) -> (InodeId, ChangeSeq) {
    let deleted = namespace
        .commit_candidate(CommitCandidate::new(CommitIntent::single(
            CommitId::parse(format!("delete-{name}")).expect("commit id"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::DeletePath {
                path: AbsolutePath::parse(format!("/{name}")).expect("path"),
                behavior: DeleteDirectoryBehavior::Recursive,
                expected_inode_id: None,
            },
        )))
        .await
        .expect("delete");
    let inode_id = deleted
        .events
        .iter()
        .find_map(|event| match event {
            FilesystemChange::Deleted { inode_id, .. } => Some(*inode_id),
            _ => None,
        })
        .expect("the delete reports its deletion");
    (inode_id, deleted.committed_seq)
}

/// A runtime over `store` that commits as `writer`, with no publication
/// pacing, on a wall clock and a monotonic timer that move only when the
/// test moves `clock`. A session of the runtime trusts its basis without a
/// successor check until `clock` moves.
async fn runtime_at(
    store: SharedStore,
    writer: &str,
    clock: &Arc<ManualClock>,
) -> crate::LoonFs<crate::Writable> {
    crate::LoonFs::builder_with_store(store)
        .writer_id(loonfs_test_support::ids::writer_id(writer))
        .config(crate::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..crate::RuntimeConfig::default()
        })
        .wall_clock(clock.clone())
        .monotonic_timer(clock.clone())
        .run_policy(loonfs_core::test_support::one_delta_run_policy())
        .build()
        .await
        .unwrap_or_else(|error| panic!("build {writer}: {error}"))
}

/// Yields until a retention advance sits at the tail of the publisher's
/// queue.
async fn wait_for_queued_advance(publisher: &NamespacePublisher) {
    bounded(Duration::from_secs(10), async {
        while !matches!(
            publisher_state(publisher).queue.back(),
            Some(WorkItem::RetentionAdvance(_))
        ) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the advance should queue");
}

/// Checks that a session's queue orders its undeletes around a retention
/// advance. An undelete queued before the advance is folded before the
/// advance's manifest, even though the advance expires its deletion, and an
/// undelete of that deletion queued after the advance returns `not_deleted`.
#[tokio::test]
async fn an_advance_orders_the_undeletes_queued_around_it() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let clock = Arc::new(ManualClock::at_system_time());
    let writer = runtime_at(store.clone(), "writer-a", &clock).await;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    create_directories(&namespace, "restored").await;
    create_directories(&namespace, "expired").await;
    let (restored, restored_seq) = delete_a_directory(&namespace, "restored").await;
    let (expired, expired_seq) = delete_a_directory(&namespace, "expired").await;
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("writer-a"));
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold the deletions");
    clock.advance_ms(crate::RETENTION_ADVANCE_MIN_COMMIT_AGE_MS);
    let publisher = writer
        .mode
        .publisher
        .live_publisher(&namespace_id)
        .expect("the namespace's session");

    // The worker takes nothing while every publication permit is taken, so
    // the three requests queue in the order they're made.
    let permits = hold_every_publication_permit(&writer).await;
    let before = tokio::spawn({
        let namespace = namespace.clone();
        let actor = actor.clone();
        async move { namespace.undelete(restored, restored_seq, &actor).await }
    });
    wait_for_queued_candidates(&publisher, 1).await;
    let advance = tokio::spawn({
        let maintenance = maintenance.clone();
        let namespace_id = namespace_id.clone();
        async move { maintenance.advance_retention_floor(&namespace_id).await }
    });
    wait_for_queued_advance(&publisher).await;
    let after = tokio::spawn({
        let namespace = namespace.clone();
        let actor = actor.clone();
        async move { namespace.undelete(expired, expired_seq, &actor).await }
    });
    wait_for_queued_candidates(&publisher, 2).await;
    drop(permits);

    let before = before
        .await
        .expect("join")
        .expect("the undelete before the advance");
    let advanced = advance.await.expect("join").expect("the advance");
    assert_eq!(advanced.retention_floor_seq, expired_seq);
    let error = after
        .await
        .expect("join")
        .expect_err("the undelete after the advance");
    assert_eq!(error.code(), ErrorCode::NotDeleted, "{error:?}");

    let floor = current_manifest(store.as_ref(), &namespace_id).await;
    assert_eq!(floor.retention_floor_seq, expired_seq);
    let predecessor = manifest_at(
        store.as_ref(),
        &namespace_id,
        ManifestNo(floor.manifest_no.0 - 1),
    )
    .await;
    assert!(
        predecessor.folded_wal_no
            >= wal_no_of_commit(store.as_ref(), &namespace_id, before.committed_seq).await,
        "the advance's predecessor folds the undelete queued before it"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that an advance whose writer epoch another runtime claims between
/// the advance's fold and its floor returns `writer_fenced` and publishes
/// nothing, so the new writer's manifest keeps the old floor.
#[tokio::test]
async fn a_takeover_between_the_fold_and_the_floor_fences_the_advance() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    // The advance's fold finds the tail folded, so the floor's put is the
    // advance's first manifest put, which the gate parks.
    let gated = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
        OperationSelector::Put,
    ));
    let clock = Arc::new(ManualClock::at_system_time());
    let writer = runtime_at(gated.clone() as SharedStore, "writer-a", &clock).await;
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    create_directories(&namespace, "history").await;
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("writer-a"));
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold the history");
    clock.advance_ms(crate::RETENTION_ADVANCE_MIN_COMMIT_AGE_MS);

    gated.block_next();
    let advance = tokio::spawn({
        let maintenance = maintenance.clone();
        let namespace_id = namespace_id.clone();
        async move { maintenance.advance_retention_floor(&namespace_id).await }
    });
    gated.wait_until_blocked().await;
    let parked = current_manifest(gated.inner(), &namespace_id).await;
    let takeover = runtime_at(
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore,
        "writer-b",
        &clock,
    )
    .await;
    takeover
        .open_namespace(&namespace_id)
        .expect("open namespace")
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "takeover", "takeover",
        )))
        .await
        .expect("the new writer commits");
    gated.release();

    let error = advance
        .await
        .expect("join")
        .expect_err("the advance's epoch was taken over");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
    let current = current_manifest(gated.inner(), &namespace_id).await;
    assert!(current.writer_epoch > parked.writer_epoch);
    assert_eq!(
        current.retention_floor_seq, parked.retention_floor_seq,
        "the new writer's manifest keeps the old floor"
    );
    for manifest_no in parked.manifest_no.0 + 1..=current.manifest_no.0 {
        assert_eq!(
            manifest_at(gated.inner(), &namespace_id, ManifestNo(manifest_no))
                .await
                .retention_floor_seq,
            parked.retention_floor_seq,
            "manifest {manifest_no}: the advance published no floor"
        );
    }
    assert_eq!(
        namespace.session_status(),
        crate::NamespaceSessionStatus::Fenced
    );
    takeover.shutdown().await.expect("shutdown");
    writer.shutdown().await.expect("shutdown");
}

/// Checks that an undelete that the old writer commits between an advance's
/// claim and its fence is folded before the advance's floor, so a base
/// compaction over the advance's manifest keeps the restored item's rows.
#[tokio::test]
async fn a_commit_between_the_claim_and_the_fence_is_folded_before_the_floor() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let writer_clock = Arc::new(ManualClock::at_system_time());
    let writer = runtime_at(store.clone(), "writer-a", &writer_clock).await;
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    create_directories(&namespace, "deleted").await;
    let maintainer_clock = Arc::new(ManualClock::at_system_time());
    maintainer_clock.advance_ms(crate::RETENTION_ADVANCE_MIN_COMMIT_AGE_MS);
    // The maintainer's first WAL put is its fence, which the gate parks.
    let gated = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
        OperationSelector::Put,
    ));
    let maintainer = runtime_at(
        gated.clone() as SharedStore,
        "maintainer",
        &maintainer_clock,
    )
    .await;
    let maintenance = maintainer.maintenance(loonfs_test_support::ids::writer_id("maintainer"));
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold the directories");
    let (inode_id, deletion_seq) = delete_a_directory(&namespace, "deleted").await;
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold the deletion");

    gated.block_next();
    let advance = tokio::spawn({
        let maintenance = maintenance.clone();
        let namespace_id = namespace_id.clone();
        async move { maintenance.advance_retention_floor(&namespace_id).await }
    });
    gated.wait_until_blocked().await;
    let claim = current_manifest(store.as_ref(), &namespace_id).await;
    // The writer's session trusts its basis, so it doesn't see the claim,
    // and its undelete takes the WAL number that the fence waits for.
    let restored = namespace
        .undelete(inode_id, deletion_seq, &actor)
        .await
        .expect("an undelete against the old floor");
    gated.release();

    let advanced = advance.await.expect("join").expect("the advance");
    assert_eq!(advanced.retention_floor_seq, deletion_seq);
    let floor = current_manifest(store.as_ref(), &namespace_id).await;
    assert_eq!(floor.writer_epoch, claim.writer_epoch);
    assert!(
        floor.head_seq >= restored.committed_seq,
        "the advance's fold covers the undelete"
    );
    let rows = inode_rows(store.as_ref(), &namespace_id).await;
    assert!(drain_compaction(&maintenance, &namespace_id).await > 0);
    assert_eq!(
        inode_rows(store.as_ref(), &namespace_id).await,
        rows,
        "no compaction removes the restored item's rows"
    );
    let reader = maintainer.read_only().namespace(&namespace_id);
    for path in ["/deleted", "/deleted/inner"] {
        reader
            .stat(path)
            .await
            .unwrap_or_else(|error| panic!("{path} is restored: {error}"));
    }
    maintainer.shutdown().await.expect("shutdown");
    writer.shutdown().await.expect("shutdown");
}

/// Checks that an advance by another runtime fences the writer's session,
/// so an undelete of a deletion that the advance expired returns
/// `writer_fenced` and then `not_deleted` from the writer's next session,
/// and that a base compaction over the advance's manifest removes the
/// deletion's rows.
#[tokio::test]
async fn an_advance_by_another_runtime_fences_a_session_on_the_old_floor() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let clock = Arc::new(ManualClock::at_system_time());
    let writer = runtime_at(store.clone(), "writer-a", &clock).await;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let maintainer_clock = Arc::new(ManualClock::at_system_time());
    maintainer_clock.advance_ms(crate::RETENTION_ADVANCE_MIN_COMMIT_AGE_MS);
    let maintainer = runtime_at(store.clone(), "maintainer", &maintainer_clock).await;
    let maintenance = maintainer.maintenance(loonfs_test_support::ids::writer_id("maintainer"));
    // Each fold writes a run, and the directories created after the
    // deletion give the `inodes` group a second run, so a base compaction of
    // the group rewrites the run that stores the deleted directories.
    create_directories(&namespace, "deleted").await;
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold the directories");
    let (inode_id, deletion_seq) = delete_a_directory(&namespace, "deleted").await;
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold the deletion");
    create_directories(&namespace, "later").await;
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold the later directories");
    let advanced = maintenance
        .advance_retention_floor(&namespace_id)
        .await
        .expect("advance the floor");
    assert!(advanced.retention_floor_seq >= deletion_seq);

    let error = namespace
        .undelete(inode_id, deletion_seq, &actor)
        .await
        .expect_err("the session's epoch was taken over");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
    let rows = inode_rows(store.as_ref(), &namespace_id).await;
    assert!(drain_compaction(&maintenance, &namespace_id).await > 0);
    assert!(
        inode_rows(store.as_ref(), &namespace_id).await < rows,
        "a compaction over the advance's manifest removes the deletion's rows"
    );

    drop(namespace);
    writer
        .drain()
        .await
        .expect("the fenced session's work ends");
    let error = writer
        .open_namespace(&namespace_id)
        .expect("open a new session")
        .undelete(inode_id, deletion_seq, &actor)
        .await
        .expect_err("the deletion has expired");
    assert_eq!(error.code(), ErrorCode::NotDeleted, "{error:?}");
    maintainer.shutdown().await.expect("shutdown");
    writer.shutdown().await.expect("shutdown");
}
