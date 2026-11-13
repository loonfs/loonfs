//! Retention advances through floor objects: the order of a session's
//! restores around an advance in its queue, a restore that takes the
//! number that another runtime's floor object was planned for, an advance
//! by another runtime that a session's restore collides with, and a read
//! from a cached head after a floor object.

use super::*;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{DeleteDirectoryBehavior, ErrorCode, FilesystemEvent, InodeId, WalNo};

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

/// The number of the WAL object that commits `committed_seq`.
async fn wal_no_of_commit(
    store: &dyn ObjectStore,
    namespace_id: &NamespaceId,
    committed_seq: CommitSeq,
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
) -> (InodeId, CommitSeq) {
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
            FilesystemEvent::Deleted { inode_id, .. } => Some(*inode_id),
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

/// The WAL object numbered `wal_no`.
#[expect(
    clippy::disallowed_methods,
    reason = "the test reads a floor object that the advance wrote"
)]
async fn wal_object_at(
    store: &dyn ObjectStore,
    namespace_id: &NamespaceId,
    wal_no: WalNo,
) -> loonfs_types::format::wal::WalObjectPayload {
    let bytes = store
        .get(
            &loonfs_objectstore::keys::wal_object(namespace_id, &wal_no),
            None,
        )
        .await
        .expect("read the WAL object")
        .expect("the WAL object exists");
    decode_wal_object_envelope_zstd(&bytes)
        .expect("decode the WAL object")
        .into_payload()
}

/// Checks the first example of a retention advance in the WAL: a session's
/// queue with a restore, an advance, and a second restore of the same
/// deletion. The first restore commits, the floor object follows it at the
/// next WAL number, and the second restore returns `not_deleted`, as does
/// a restore of another deletion that the new floor expired. The manifest
/// that records the floor contains the first restore.
#[tokio::test]
async fn an_advance_orders_the_restores_queued_around_it() {
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
    clock.advance_ms(crate::GC_GRACE_WINDOW_MS);
    let publisher = writer
        .mode
        .publisher
        .live_publisher(&namespace_id)
        .expect("the namespace's session");

    // The worker takes nothing while every publication permit is taken, so
    // the requests queue in the order they're made.
    let permits = hold_every_publication_permit(&writer).await;
    let restore_deleted = |inode_id, deletion_seq| {
        let namespace = namespace.clone();
        let actor = actor.clone();
        tokio::spawn(async move {
            namespace
                .restore_deleted(inode_id, deletion_seq, &actor)
                .await
        })
    };
    let before = restore_deleted(restored, restored_seq);
    wait_for_queued_candidates(&publisher, 1).await;
    let advance = tokio::spawn({
        let maintenance = maintenance.clone();
        let namespace_id = namespace_id.clone();
        async move { maintenance.advance_retention_floor(&namespace_id).await }
    });
    wait_for_queued_advance(&publisher).await;
    let again = restore_deleted(restored, restored_seq);
    wait_for_queued_candidates(&publisher, 2).await;
    let after = restore_deleted(expired, expired_seq);
    wait_for_queued_candidates(&publisher, 3).await;
    drop(permits);

    let before = before
        .await
        .expect("join")
        .expect("the restore before the advance");
    let advanced = advance.await.expect("join").expect("the advance");
    assert_eq!(advanced.retention_floor_seq, expired_seq);
    for refused in [again, after] {
        let error = refused
            .await
            .expect("join")
            .expect_err("a restore after the advance");
        assert_eq!(error.code(), ErrorCode::NotDeleted, "{error:?}");
    }

    let restored_at = wal_no_of_commit(store.as_ref(), &namespace_id, before.committed_seq).await;
    let floor_object = wal_object_at(
        store.as_ref(),
        &namespace_id,
        restored_at.successor().expect("next WAL number"),
    )
    .await;
    assert!(floor_object.records.is_empty());
    assert_eq!(floor_object.retention_floor_seq, Some(expired_seq));
    let floor = current_manifest(store.as_ref(), &namespace_id).await;
    assert_eq!(floor.retention_floor_seq, expired_seq);
    assert!(
        floor.folded_wal_no >= floor_object.wal_no && floor.head_seq >= before.committed_seq,
        "the manifest that records the floor folds the restore queued before the advance"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks the third example of a retention advance in the WAL: a writer's
/// restore takes the WAL number that another runtime planned its floor
/// object for. The advance's put collides, its floor object goes to the
/// next number, and the manifest that records the floor contains the
/// restore, so a base compaction keeps the restored item's rows.
#[tokio::test]
async fn a_restore_at_the_floor_objects_number_is_folded_before_the_floor() {
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
    maintainer_clock.advance_ms(crate::GC_GRACE_WINDOW_MS);
    // The maintainer's first WAL put is its floor object, which the gate
    // parks.
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
    let before = current_manifest(store.as_ref(), &namespace_id).await;

    gated.block_next();
    let advance = tokio::spawn({
        let maintenance = maintenance.clone();
        let namespace_id = namespace_id.clone();
        async move { maintenance.advance_retention_floor(&namespace_id).await }
    });
    gated.wait_until_blocked().await;
    // The writer's session trusts its tip, and its restore takes the WAL
    // number that the floor object waits for.
    let restored = namespace
        .restore_deleted(inode_id, deletion_seq, &actor)
        .await
        .expect("a restore against the old floor");
    gated.release();

    let advanced = advance.await.expect("join").expect("the advance");
    assert_eq!(advanced.retention_floor_seq, deletion_seq);
    let restored_at = wal_no_of_commit(store.as_ref(), &namespace_id, restored.committed_seq).await;
    let floor_object = wal_object_at(
        store.as_ref(),
        &namespace_id,
        restored_at.successor().expect("next WAL number"),
    )
    .await;
    assert_eq!(floor_object.retention_floor_seq, Some(deletion_seq));
    let floor = current_manifest(store.as_ref(), &namespace_id).await;
    assert_eq!(floor.retention_floor_seq, deletion_seq);
    assert_eq!(
        floor.writer_epoch, before.writer_epoch,
        "the advance claims no epoch"
    );
    assert!(
        floor.head_seq >= restored.committed_seq,
        "the advance's fold covers the restore"
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

/// Checks that an advance by another runtime leaves the writer's session
/// unfenced. The session's restore of a deletion that the advance expired
/// collides with the floor object, plans again against the new floor, and
/// returns `not_deleted`, and the session's next commit succeeds. A base
/// compaction over the manifest that records the floor removes the
/// deletion's rows.
#[tokio::test]
async fn an_advance_by_another_runtime_leaves_a_session_on_the_old_floor_unfenced() {
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
    maintainer_clock.advance_ms(crate::GC_GRACE_WINDOW_MS);
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
        .restore_deleted(inode_id, deletion_seq, &actor)
        .await
        .expect_err("the deletion has expired");
    assert_eq!(error.code(), ErrorCode::NotDeleted, "{error:?}");
    assert_ne!(
        namespace.session_status(),
        crate::NamespaceSessionStatus::Fenced
    );
    let rows = inode_rows(store.as_ref(), &namespace_id).await;
    assert!(drain_compaction(&maintenance, &namespace_id).await > 0);
    assert!(
        inode_rows(store.as_ref(), &namespace_id).await < rows,
        "a compaction over the manifest that records the floor removes the deletion's rows"
    );
    create_directories(&namespace, "after").await;
    maintainer.shutdown().await.expect("shutdown");
    writer.shutdown().await.expect("shutdown");
}

/// Checks that an advance that finds no session, and so creates its floor
/// object itself, folds the floor object even when a session opens in the
/// same runtime and commits before the floor object's discovery. The
/// session's position ends below the floor object, so the advance drops it,
/// and when the advance returns, the current manifest records the floor.
#[tokio::test]
async fn an_advance_without_a_session_folds_its_floor_object_after_a_session_opens() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let clock = Arc::new(ManualClock::at_system_time());
    let seed = runtime_at(
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore,
        "seed",
        &clock,
    )
    .await;
    let actor = loonfs_test_support::test_actor();
    seed.create_namespace(&namespace_id, &actor)
        .await
        .expect("create");
    create_directories(
        &seed.open_namespace(&namespace_id).expect("open namespace"),
        "history",
    )
    .await;
    seed.maintenance(loonfs_test_support::ids::writer_id("seed"))
        .fold_wal(&namespace_id)
        .await
        .expect("fold the history");
    seed.shutdown().await.expect("shutdown");
    clock.advance_ms(crate::GC_GRACE_WINDOW_MS);
    // The direct path checks the planned manifest's segments after it finds
    // no session and before its discovery, so the gate parks it there.
    let gated = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::metadata_segment(),
        OperationSelector::Head,
    ));
    let writer = runtime_at(gated.clone() as SharedStore, "writer-a", &clock).await;
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("writer-a"));

    gated.block_next();
    let advance = tokio::spawn({
        let maintenance = maintenance.clone();
        let namespace_id = namespace_id.clone();
        async move { maintenance.advance_retention_floor(&namespace_id).await }
    });
    gated.wait_until_blocked().await;
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    create_directories(&namespace, "later").await;
    assert!(writer
        .mode
        .publisher
        .current_position(&namespace_id)
        .is_some());
    gated.release();

    let advanced = advance.await.expect("join").expect("the advance");
    assert!(advanced.retention_floor_seq > CommitSeq(0));
    assert_eq!(
        current_manifest(gated.inner(), &namespace_id)
            .await
            .retention_floor_seq,
        advanced.retention_floor_seq,
        "the fold after the advance covers the floor object"
    );
    writer.shutdown().await.expect("shutdown");
}

/// The deletion sequences that a strong listing of the namespace's trash
/// returns.
async fn listed_deletions(namespace: &crate::Namespace<crate::Writable>) -> Vec<CommitSeq> {
    namespace
        .list_trash_with_options(&crate::ListTrashOptions {
            consistency: Some(crate::Consistency::Strong),
        })
        .page()
        .await
        .expect("list the trash")
        .entries
        .into_iter()
        .map(|entry| entry.deletion_seq)
        .collect()
}

/// Checks that a strong read in a runtime with a cached head finds the floor
/// of a floor object that no manifest records yet. The reader lists a
/// deletion, another runtime's advance creates its floor object and waits at
/// the put of the manifest that folds the floor object, and the reader's
/// next listing leaves the expired deletion out.
#[tokio::test]
async fn a_read_from_a_cached_head_finds_an_unfolded_floor_object() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let clock = Arc::new(ManualClock::at_system_time());
    let writer = runtime_at(store.clone(), "writer-a", &clock).await;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    create_directories(&namespace, "deleted").await;
    let (_, deletion_seq) = delete_a_directory(&namespace, "deleted").await;
    let gated = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
        OperationSelector::PutCreateIfAbsent,
    ));
    let maintainer_clock = Arc::new(ManualClock::at_system_time());
    maintainer_clock.advance_ms(crate::GC_GRACE_WINDOW_MS);
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
        .expect("fold the deletion");
    let reader = runtime_at(store.clone(), "reader", &clock).await;
    let reads = reader
        .open_namespace(&namespace_id)
        .expect("open namespace");
    assert_eq!(listed_deletions(&reads).await, [deletion_seq]);

    gated.block_next();
    let advance = tokio::spawn({
        let maintenance = maintenance.clone();
        let namespace_id = namespace_id.clone();
        async move { maintenance.advance_retention_floor(&namespace_id).await }
    });
    gated.wait_until_blocked().await;
    assert_eq!(
        current_manifest(store.as_ref(), &namespace_id)
            .await
            .retention_floor_seq,
        CommitSeq(0),
        "no manifest records the floor yet"
    );
    assert_eq!(listed_deletions(&reads).await, []);
    gated.release();

    let advanced = advance.await.expect("join").expect("the advance");
    assert!(advanced.retention_floor_seq >= deletion_seq);
    reader.shutdown().await.expect("shutdown");
    maintainer.shutdown().await.expect("shutdown");
    writer.shutdown().await.expect("shutdown");
}
