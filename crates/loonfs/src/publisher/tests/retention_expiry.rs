//! Retention advances through floor objects: the order of a session's
//! restores around an advance in its queue.

use super::*;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::format::wal::WalObjectBody;
use loonfs_types::{DeleteDirectoryBehavior, ErrorCode, FilesystemEvent, InodeId, WalNo};

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
            .records()
            .iter()
            .any(|record| record.committed_seq == committed_seq)
        {
            return payload.wal_no;
        }
    }
    panic!("no WAL object commits sequence {committed_seq}")
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
    crate::LoonFs::builder_with_store(store, crate::Context::Service)
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
    let maintenance = writer.clone();
    maintenance
        .open_namespace(&namespace_id)
        .expect("open namespace")
        .fold_wal()
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
        async move {
            maintenance
                .open_namespace(&namespace_id)
                .expect("open namespace")
                .advance_retention_floor()
                .await
        }
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
    assert_eq!(
        floor_object.body,
        WalObjectBody::Floor {
            retention_floor_seq: expired_seq
        }
    );
    let floor = current_manifest(store.as_ref(), &namespace_id).await;
    assert_eq!(floor.retention_floor_seq, expired_seq);
    assert!(
        floor.folded_wal_no >= floor_object.wal_no && floor.head_seq >= before.committed_seq,
        "the manifest that records the floor folds the restore queued before the advance"
    );
    writer.shutdown().await.expect("shutdown");
}

/// The deletion sequences that a strong listing of the namespace's trash
/// returns.
async fn listed_deletions(namespace: &crate::Namespace<crate::Writable>) -> Vec<CommitSeq> {
    namespace
        .strong()
        .list_trash()
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
/// deletion, the writer's own advance puts its floor object and waits at the
/// put of the manifest that folds the floor object, and the reader's next
/// listing leaves the expired deletion out.
#[tokio::test]
async fn a_read_from_a_cached_head_finds_an_unfolded_floor_object() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let gated = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
        OperationSelector::PutCreateIfAbsent,
    ));
    let writer_clock = Arc::new(ManualClock::at_system_time());
    let writer = runtime_at(gated.clone() as SharedStore, "writer-a", &writer_clock).await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    create_directories(&namespace, "deleted").await;
    let (_, deletion_seq) = delete_a_directory(&namespace, "deleted").await;
    namespace.fold_wal().await.expect("fold the deletion");
    let reader_clock = Arc::new(ManualClock::at_system_time());
    let reader = runtime_at(store.clone(), "reader", &reader_clock).await;
    let reads = reader
        .open_namespace(&namespace_id)
        .expect("open namespace");
    assert_eq!(listed_deletions(&reads).await, [deletion_seq]);

    // The writer's clock passes the grace window, so its advance can expire
    // the deletion.
    writer_clock.advance_ms(crate::GC_GRACE_WINDOW_MS);
    gated.block_next();
    let advance = tokio::spawn({
        let namespace = namespace.clone();
        async move { namespace.advance_retention_floor().await }
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
    writer.shutdown().await.expect("shutdown");
}
