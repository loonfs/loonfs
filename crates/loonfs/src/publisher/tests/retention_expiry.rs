//! Retention advances through a runtime: the order of a session's restores
//! around an advance's pause, and a read that finds a floor object in a
//! tail.

use super::*;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::format::wal::WalObjectBody;
use loonfs_types::{DeleteDirectoryBehavior, ErrorCode, FilesystemEvent, InodeId};

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

/// Whether `operation` is the put of a manifest whose floor is `floor`.
fn is_manifest_put_with_floor(operation: &OperationContext<'_>, floor: CommitSeq) -> bool {
    matches!(operation.kind(), OperationKind::Put { bytes, mode: PutMode::CreateIfAbsent }
        if loonfs_types::format::manifest::decode_namespace_manifest_json(bytes)
            .is_ok_and(|manifest| manifest.payload().retention_floor_seq == floor))
}

/// Checks the example of a retention advance in the format spec: a restore
/// acknowledged before the advance, an advance, and a second restore of the
/// same deletion sent while the manifest with the new floor waits at its
/// put. The first restore commits, the second restore returns
/// `not_deleted`, as does a restore of another deletion that the new floor
/// expired, and the advance's manifest contains the first
/// restore.
#[tokio::test]
async fn an_advance_orders_the_restores_around_its_pause() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let clock = Arc::new(ManualClock::at_system_time());
    // The floor of the manifest put that the store parks, set once the test
    // knows the deletion that the advance expires.
    let parked_floor = Arc::new(AtomicU64::new(u64::MAX));
    let gated = Arc::new(BlockingStore::matching(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        {
            let parked_floor = Arc::clone(&parked_floor);
            move |operation| {
                is_manifest_put_with_floor(
                    operation,
                    CommitSeq(parked_floor.load(AtomicOrdering::SeqCst)),
                )
            }
        },
    ));
    let writer = runtime_at(gated.clone() as SharedStore, "writer-a", &clock).await;
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
    namespace.fold_wal().await.expect("fold the deletions");
    clock.advance_ms(crate::GC_GRACE_WINDOW_MS);
    let before = bounded(
        Duration::from_secs(10),
        namespace.restore_deleted(restored, restored_seq, &actor),
    )
    .await
    .expect("the restore before the advance settles")
    .expect("the restore before the advance");

    parked_floor.store(expired_seq.0, AtomicOrdering::SeqCst);
    gated.block_next();
    let advance = tokio::spawn({
        let namespace = namespace.clone();
        async move { namespace.advance_retention_floor().await }
    });
    bounded(Duration::from_secs(10), gated.wait_until_blocked())
        .await
        .expect("the manifest with the new floor waits at its put");
    let restore_deleted = |inode_id, deletion_seq| {
        let namespace = namespace.clone();
        let actor = actor.clone();
        tokio::spawn(async move {
            namespace
                .restore_deleted(inode_id, deletion_seq, &actor)
                .await
        })
    };
    // The restores wait behind the pause, at their admission or in the
    // queue, until the manifest with the new floor is published.
    let again = restore_deleted(restored, restored_seq);
    let after = restore_deleted(expired, expired_seq);
    for _ in 0..1_000 {
        tokio::task::yield_now().await;
    }
    gated.release();

    let advanced = advance.await.expect("join").expect("the advance");
    assert_eq!(advanced.retention_floor_seq, expired_seq);
    for refused in [again, after] {
        let error = refused
            .await
            .expect("join")
            .expect_err("a restore after the advance");
        assert_eq!(error.code(), ErrorCode::NotDeleted, "{error:?}");
    }
    let floor = current_manifest(gated.as_ref(), &namespace_id).await;
    assert_eq!(floor.retention_floor_seq, expired_seq);
    assert!(
        floor.head_seq >= before.committed_seq,
        "the advance's manifest folds the restore made before the advance"
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
/// of a floor object that no manifest records. The reader lists a deletion,
/// a floor object at the tip expires it, and the reader's next listing
/// leaves the expired deletion out.
#[tokio::test]
#[expect(
    clippy::disallowed_methods,
    reason = "the test writes a floor object, which no writer creates"
)]
async fn a_read_from_a_cached_head_finds_an_unfolded_floor_object() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer_clock = Arc::new(ManualClock::at_system_time());
    let writer = runtime_at(store.clone(), "writer-a", &writer_clock).await;
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

    let head = loonfs_core::test_support::namespace_store(store.as_ref(), &namespace_id)
        .load_read_state()
        .await
        .expect("read the head");
    let floor_object = loonfs_types::format::wal::WalObjectPayload {
        namespace_id: namespace_id.clone(),
        wal_no: head.wal_no.successor().expect("next WAL number"),
        writer_epoch: head.writer_epoch,
        head_seq: head.seq,
        body: WalObjectBody::Floor {
            retention_floor_seq: deletion_seq,
        },
    };
    store
        .put(
            &loonfs_objectstore::keys::wal_object(&namespace_id, &floor_object.wal_no),
            loonfs_types::format::wal::encode_wal_object_envelope_zstd(floor_object)
                .expect("encode a floor object")
                .into_bytes()
                .into(),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("put a floor object");
    assert_eq!(
        current_manifest(store.as_ref(), &namespace_id)
            .await
            .retention_floor_seq,
        CommitSeq(0),
        "no manifest records the floor"
    );
    assert_eq!(listed_deletions(&reads).await, []);
    reader.shutdown().await.expect("shutdown");
    writer.shutdown().await.expect("shutdown");
}
