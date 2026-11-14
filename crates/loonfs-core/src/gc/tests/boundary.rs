//! The manifest boundary: a collection raises it to the highest manifest in
//! each batch of manifests that it deletes, before it deletes that batch, and
//! raises it to retired to retire a deleted namespace.

use super::*;
use loonfs_objectstore::keys::boundary;
use loonfs_types::format::control::ManifestBoundary;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A time strictly past the grace window for every object that reports an
/// ancient age.
const NOW_MS: u64 = GC_GRACE_WINDOW_MS + 1;

/// Creates `namespace_id` and publishes manifests until manifest `newest` is
/// current. Each writer claim publishes a manifest.
async fn publish_manifests<S: ObjectStore>(store: &S, namespace_id: &NamespaceId, newest: u64) {
    crate::test_support::NamespaceFixture::new(store, namespace_id, &context(1_000))
        .create()
        .await
        .expect("create");
    publish_more(store, namespace_id, newest).await;
}

/// Publishes manifests until manifest `newest` is current.
async fn publish_more<S: ObjectStore>(store: &S, namespace_id: &NamespaceId, newest: u64) {
    let namespace = crate::test_support::namespace_store(store, namespace_id);
    loop {
        let current = namespace
            .load_current_manifest()
            .await
            .expect("current manifest")
            .manifest()
            .manifest_no;
        if current == ManifestNo(newest) {
            break;
        }
        assert!(
            current < ManifestNo(newest),
            "manifest {current:?} is past {newest}"
        );
        namespace
            .acquire_writer_epoch(&context(1_000).writer_id)
            .await
            .expect("publish a manifest");
    }
}

async fn stored_boundary<S: ObjectStore>(store: &S, namespace_id: &NamespaceId) -> Option<u64> {
    crate::test_support::namespace_store(store, namespace_id)
        .load_boundary()
        .await
        .expect("load the boundary")
        .map(|loaded| match loaded.state.boundary {
            ManifestBoundary::Manifest { manifest_no } => manifest_no.0,
            ManifestBoundary::Retired {} => panic!("the boundary is retired"),
        })
}

/// Returns the stored boundary of `namespace_id`, or `None` when the object
/// is missing.
async fn boundary_state<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Option<ManifestBoundary> {
    crate::test_support::namespace_store(store, namespace_id)
        .load_boundary()
        .await
        .expect("load the boundary")
        .map(|loaded| loaded.state.boundary)
}

async fn manifest_exists<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    number: u64,
) -> bool {
    store
        .head(&metadata_manifest_object(namespace_id, &ManifestNo(number)))
        .await
        .expect("head manifest")
        .is_some()
}

/// Checks that a collection raises the boundary before its first manifest
/// delete: with every manifest delete failing, the boundary is raised and
/// the replaced manifests are still there.
#[tokio::test]
async fn a_collection_raises_the_boundary_before_it_deletes_a_manifest() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("boundary-first").expect("namespace");
    let store = Arc::new(MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
    ));
    publish_manifests(&store, &namespace_id, 3).await;
    let failing = FailStore::new(
        Arc::clone(&store),
        KeyPredicate::manifest(&namespace_id),
        OperationSelector::Delete,
        InjectedError::Transport("delete refused".to_owned()),
    );
    failing.fail_all();

    crate::test_support::namespace_store(&failing, &namespace_id)
        .gc(&context(NOW_MS).clock())
        .await
        .expect_err("the manifest deletes fail");
    assert_eq!(stored_boundary(&store, &namespace_id).await, Some(2));
    for number in 1..=3 {
        assert!(
            manifest_exists(&store, &namespace_id, number).await,
            "{number}"
        );
    }
}

/// Checks that a collection deletes only the manifests that it chose, and not
/// a manifest that another collection's raise passed: collection A finds
/// manifest 2 as the newest and lists no snapshot, then a snapshot is created
/// on manifest 2, manifests 3 and 4 are published, and collection B deletes
/// manifests 1 and 3 and raises the boundary to 3 before A lists the
/// manifests.
#[tokio::test]
async fn a_collection_deletes_only_the_manifests_that_it_chose() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("boundary-chosen").expect("namespace");
    let store = Arc::new(MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
    ));
    publish_manifests(&store, &namespace_id, 2).await;
    // The collection's first listing of the manifests finds its newest
    // manifest, and the gate parks the second, which lists its candidates.
    let manifests = metadata_manifest_prefix(&namespace_id);
    let listings = std::sync::atomic::AtomicUsize::new(0);
    let blocked = Arc::new(BlockingStore::matching(
        Arc::clone(&store),
        move |operation| {
            operation.key() == manifests
                && OperationSelector::List.matches(operation.kind())
                && listings.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1
        },
    ));
    let recorded = RecordingStore::new(
        Arc::clone(&blocked),
        KeyPredicate::exact(boundary(&namespace_id)),
    );
    let collection_a = crate::test_support::namespace_store(&recorded, &namespace_id);
    let clock_a = context(NOW_MS).clock();
    blocked.block_next();
    let (report_a, ()) = tokio::join!(collection_a.gc(&clock_a), async {
        blocked.wait_until_blocked().await;
        let snapshot = create_snapshot(&store, &namespace_id, &context(1_000))
            .await
            .expect("snapshot on manifest 2");
        assert_eq!(snapshot.snapshot_id.manifest_no(), ManifestNo(2));
        publish_more(&store, &namespace_id, 4).await;
        let report_b = collect(&store, &namespace_id, &context(NOW_MS)).await;
        assert_eq!(report_b.deleted.manifests, 2);
        assert!(!manifest_exists(&store, &namespace_id, 3).await);
        assert_eq!(stored_boundary(&store, &namespace_id).await, Some(3));
        blocked.release();
    });
    let report_a = report_a.expect("collection A");
    assert_eq!(report_a.deleted.manifests, 0);
    assert!(manifest_exists(&store, &namespace_id, 2).await);
    assert_eq!(recorded.count(OperationSelector::Put), 0);
}

/// Checks that the boundary ends at the highest manifest that a collection
/// deletes: below a young replaced manifest, never at the newest manifest
/// however old it is, and over a manifest that a snapshot keeps, which stays.
/// A later collection that deletes no manifest writes nothing to the
/// boundary.
#[tokio::test]
async fn the_boundary_ends_at_the_highest_deleted_manifest() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("boundary-stops").expect("namespace");
    let young_key = metadata_manifest_object(&namespace_id, &ManifestNo(6));
    let store = Arc::new(MetadataMapStore::new(
        MetadataMapStore::aged(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::manifest(&namespace_id),
        ),
        KeyPredicate::exact(&young_key),
        |mut metadata| {
            metadata.last_modified_at_ms = Some(NOW_MS - 1_000);
            metadata
        },
    ));
    publish_manifests(&store, &namespace_id, 3).await;
    // The snapshot's fold publishes manifest 4, and the snapshot keeps it.
    let snapshot = create_snapshot(&store, &namespace_id, &context(1_000))
        .await
        .expect("snapshot on manifest 4");
    assert_eq!(snapshot.snapshot_id.manifest_no(), ManifestNo(4));
    publish_more(&store, &namespace_id, 7).await;

    let recorded = RecordingStore::new(
        Arc::clone(&store),
        KeyPredicate::exact(boundary(&namespace_id)),
    );
    let report = collect(&recorded, &namespace_id, &context(NOW_MS)).await;
    assert_eq!(report.deleted.manifests, 4);
    assert_eq!(stored_boundary(&store, &namespace_id).await, Some(5));
    for (number, exists) in [
        (1, false),
        (2, false),
        (3, false),
        (4, true),
        (5, false),
        (6, true),
        (7, true),
    ] {
        assert_eq!(
            manifest_exists(&store, &namespace_id, number).await,
            exists,
            "{number}"
        );
    }

    recorded.reset();
    let report = collect(&recorded, &namespace_id, &context(NOW_MS)).await;
    assert_eq!(report.deleted.manifests, 0);
    assert_eq!(recorded.count(OperationSelector::Put), 0);
}

/// Runs one collection of `namespace_id` at `NOW_MS` with a read working
/// memory of `limit_bytes`.
async fn collect_within<S: ObjectStore>(
    store: S,
    namespace_id: &NamespaceId,
    limit_bytes: usize,
) -> crate::error::Result<GcResponse> {
    let memory = Arc::new(crate::cache::ReadWorkingMemory::new(limit_bytes, None));
    crate::NamespaceStore {
        segment_cache: Arc::new(root_scan_cache(memory)),
        ..crate::test_support::namespace_store(store, namespace_id)
    }
    .gc(&context(NOW_MS).clock())
    .await
}

/// Checks that a collection deletes more manifests than its read working
/// memory can keep at once: under 4 KiB, one collection deletes 100 replaced
/// manifests and raises the boundary to the highest of them.
#[tokio::test]
async fn a_collection_deletes_more_manifests_than_its_memory_keeps_at_once() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("boundary-batches").expect("namespace");
    let store = Arc::new(MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
    ));
    publish_manifests(&store, &namespace_id, 101).await;

    let report = collect_within(&store, &namespace_id, 4 * 1024)
        .await
        .expect("collection");
    assert_eq!(report.deleted.manifests, 100);
    assert_eq!(stored_boundary(&store, &namespace_id).await, Some(100));
    assert!(manifest_exists(&store, &namespace_id, 101).await);
}

/// Checks a collection that stops between two batches: every manifest delete
/// after the boundary's second raise fails. The first batch's manifests are
/// gone, the boundary is at or above the highest of them, and the later
/// manifests are still there. The next collection deletes the later
/// manifests and raises the boundary to the highest of them.
#[tokio::test]
async fn a_collection_that_stops_between_two_batches_leaves_the_rest_to_the_next() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("boundary-stops-between").expect("namespace");
    let store = Arc::new(MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
    ));
    publish_manifests(&store, &namespace_id, 101).await;
    let raises = Arc::new(AtomicUsize::new(0));
    let failing = FailStore::matching(
        Arc::clone(&store),
        {
            let boundary_key = boundary(&namespace_id);
            let manifests = KeyPredicate::manifest(&namespace_id);
            let raises = Arc::clone(&raises);
            move |operation: &OperationContext<'_>| match operation.kind() {
                OperationKind::Put { .. } if operation.key() == boundary_key => {
                    raises.fetch_add(1, Ordering::SeqCst);
                    false
                }
                OperationKind::Delete => {
                    manifests.matches(operation.key()) && raises.load(Ordering::SeqCst) >= 2
                }
                _ => false,
            }
        },
        InjectedError::Transport("delete refused".to_owned()),
    );
    failing.fail_all();

    collect_within(&failing, &namespace_id, 4 * 1024)
        .await
        .expect_err("the second batch's deletes fail");
    assert!(failing.attempts() > 0);
    let mut first_batch = 0;
    while !manifest_exists(&store, &namespace_id, first_batch + 1).await {
        first_batch += 1;
    }
    assert!(first_batch > 0 && first_batch < 100, "{first_batch}");
    for number in first_batch + 1..=101 {
        assert!(
            manifest_exists(&store, &namespace_id, number).await,
            "{number}"
        );
    }
    let raised = stored_boundary(&store, &namespace_id)
        .await
        .expect("the boundary was raised");
    assert!(raised >= first_batch, "{raised} {first_batch}");

    let report = collect_within(&store, &namespace_id, 4 * 1024)
        .await
        .expect("the next collection");
    assert_eq!(report.deleted.manifests, 100 - first_batch);
    assert_eq!(stored_boundary(&store, &namespace_id).await, Some(100));
    assert!(manifest_exists(&store, &namespace_id, 101).await);
}

/// Creates `source`, forks it as `target`, writes a file in `target`, and
/// deletes `target`. Returns the fork snapshot on `source` and a time at which
/// `target` is eligible for retirement.
async fn deleted_fork<S: ObjectStore>(
    store: &S,
    source: &NamespaceId,
    target: &NamespaceId,
) -> (SnapshotId, WriterAt) {
    let setup = context(1_000);
    let fixture = crate::test_support::NamespaceFixture::new(store, source, &setup);
    fixture.create().await.expect("create the source");
    fixture
        .engine()
        .fork_namespace(
            target,
            &loonfs_test_support::test_actor(),
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await
        .expect("fork");
    write_test_file(store, target, "/owned.txt", "target-write", &setup).await;
    crate::test_support::NamespaceFixture::new(store, target, &setup)
        .engine()
        .delete_namespace(Default::default())
        .await
        .expect("delete the target");
    let tombstone = crate::test_support::namespace_store(store, target)
        .load_current_manifest()
        .await
        .expect("tombstone");
    let snapshot_id = tombstone
        .payload()
        .fork_basis
        .as_ref()
        .expect("fork basis")
        .source_snapshot_id
        .clone();
    (snapshot_id, context(setup.now_ms + GC_GRACE_WINDOW_MS))
}

/// Checks that a collection retires an eligible fork by deleting its fork
/// snapshot on the source and then raising the boundary to retired. A
/// collection whose raise fails has deleted the snapshot and leaves the
/// tombstone, and the next collection retires the namespace.
#[tokio::test]
async fn retirement_deletes_the_fork_snapshot_and_then_retires_the_boundary() {
    let directory = tempdir().expect("directory");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let source = NamespaceId::parse("source").expect("source");
    let target = NamespaceId::parse("target").expect("target");
    let (snapshot_id, eligible) = deleted_fork(&inner, &source, &target).await;
    let store = FailStore::new(
        inner,
        KeyPredicate::exact(boundary(&target)),
        OperationSelector::Put,
        InjectedError::Transport("raise refused".to_owned()),
    );
    store.fail_all();

    crate::test_support::namespace_store(&store, &target)
        .gc(&eligible.clock())
        .await
        .expect_err("the raise to retired fails");
    assert!(!snapshot_record_exists(&store, &source, &snapshot_id).await);
    assert_eq!(boundary_state(&store, &target).await, None);
    assert!(crate::test_support::namespace_store(&store, &target)
        .load_current_manifest()
        .await
        .expect("the tombstone stays")
        .payload()
        .status
        .is_deleted());

    store.clear();
    let report = collect(&store, &target, &eligible).await;
    assert_eq!(report.deleted_snapshots_by_owner.fork, 0);
    assert_eq!(
        boundary_state(&store, &target).await,
        Some(ManifestBoundary::Retired {})
    );
}

/// Checks that retirement raises the boundary to retired, and not to the
/// tombstone's number, and that a later raise or collection writes nothing to
/// the boundary.
#[tokio::test]
async fn a_retired_boundary_stays_retired() {
    let directory = tempdir().expect("directory");
    let source = NamespaceId::parse("source").expect("source");
    let target = NamespaceId::parse("target").expect("target");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(boundary(&target)),
    );
    let (_, eligible) = deleted_fork(&store, &source, &target).await;
    collect(&store, &target, &eligible).await;
    assert_eq!(
        boundary_state(&store, &target).await,
        Some(ManifestBoundary::Retired {})
    );

    store.reset();
    crate::test_support::namespace_store(&store, &target)
        .raise_boundary(ManifestNo(u64::MAX))
        .await
        .expect("raise a retired boundary");
    let later = context(now_after_newest_object(&store, &target, GC_GRACE_WINDOW_MS + 1).await);
    collect(&store, &target, &later).await;
    assert_eq!(store.count(OperationSelector::Put), 0);
    assert_eq!(
        boundary_state(&store, &target).await,
        Some(ManifestBoundary::Retired {})
    );
}

/// Checks that a read, a commit through a writer session that committed
/// before the deletion, a create with the ID, and a fork to the ID each
/// return `namespace_deleted` once the namespace is retired, and that none
/// of them writes anything.
#[tokio::test]
async fn every_request_to_a_retired_namespace_returns_namespace_deleted() {
    let directory = tempdir().expect("directory");
    let source = NamespaceId::parse("source").expect("source");
    let target = NamespaceId::parse("target").expect("target");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let setup = context(1_000);
    let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &setup);
    fixture.create().await.expect("create the source");
    let manifest = crate::test_support::folded_manifest(&fixture.namespace)
        .await
        .expect("fold the WAL tail");
    fixture
        .engine()
        .fork_namespace(
            &target,
            &loonfs_test_support::test_actor(),
            manifest.clone(),
        )
        .await
        .expect("fork");
    let mut session = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &target),
        setup.writer_id.clone(),
    );
    let commit = |commit_id: &str| {
        vec![CommitCandidate::new(
            crate::test_support::create_directory_request(commit_id, commit_id),
        )]
    };
    session
        .publish_batch(commit("before"), &setup.clock())
        .await
        .results[0]
        .as_ref()
        .expect("commit before the deletion");
    crate::test_support::NamespaceFixture::new(&store, &target, &setup)
        .engine()
        .delete_namespace(Default::default())
        .await
        .expect("delete the target");
    let eligible = context(setup.now_ms + GC_GRACE_WINDOW_MS);
    collect(&store, &target, &eligible).await;
    assert_eq!(
        boundary_state(&store, &target).await,
        Some(ManifestBoundary::Retired {})
    );

    store.reset();
    assert_matches!(
        crate::test_support::namespace_store(&store, &target)
            .load_read_state()
            .await,
        Err(CoreError::NamespaceDeleted { .. })
    );
    assert_matches!(
        session
            .publish_batch(commit("after"), &eligible.clock())
            .await
            .results
            .remove(0),
        Err(CoreError::NamespaceDeleted { .. })
    );
    assert_matches!(
        crate::test_support::NamespaceFixture::new(&store, &target, &setup)
            .create()
            .await,
        Err(CoreError::NamespaceDeleted { .. })
    );
    assert_matches!(
        fixture
            .engine()
            .fork_namespace(&target, &loonfs_test_support::test_actor(), manifest)
            .await,
        Err(CoreError::NamespaceDeleted { .. })
    );
    let counts = store.counts();
    assert_eq!((counts.puts, counts.deletes), (0, 0));
}

/// Checks that the pool's collection keeps the content of a deleted first
/// member whose tombstone is eligible for retirement until the member's
/// boundary is retired, and deletes the content after the retirement.
#[tokio::test]
async fn the_pool_keeps_a_deleted_members_content_until_its_retirement() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("deleted-member").expect("namespace");
    let (deadline, pool_id, keys) = retired_content_namespace(&store, &namespace_id).await;
    let aged = context(
        now_after_newest_object(&store, &namespace_id, GC_GRACE_WINDOW_MS + 1)
            .await
            .max(deadline.now_ms),
    );
    let kept = collect_pool(&store, &pool_id, &aged).await;
    assert_eq!(kept.deleted.content_objects, 0);
    for key in &keys {
        assert!(store.head(key).await.expect("content").is_some());
    }

    collect(&store, &namespace_id, &aged).await;
    assert_eq!(
        boundary_state(&store, &namespace_id).await,
        Some(ManifestBoundary::Retired {})
    );
    let reclaimed = collect_pool(&store, &pool_id, &aged).await;
    assert_eq!(reclaimed.deleted.content_objects, keys.len() as u64);
}

/// Checks that the source's snapshot cleanup deletes a fork snapshot whose
/// target is retired.
#[tokio::test]
async fn snapshot_cleanup_deletes_a_fork_snapshot_whose_target_is_retired() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let source = NamespaceId::parse("source").expect("source");
    let target = NamespaceId::parse("target").expect("target");
    let (snapshot_id, eligible) = deleted_fork(&store, &source, &target).await;
    crate::test_support::namespace_store(&store, &target)
        .retire_boundary()
        .await
        .expect("retire the target");
    assert!(snapshot_record_exists(&store, &source, &snapshot_id).await);

    let report = collect(&store, &source, &eligible).await;
    assert_eq!(report.deleted_snapshots_by_owner.fork, 1);
    assert!(!snapshot_record_exists(&store, &source, &snapshot_id).await);
}
