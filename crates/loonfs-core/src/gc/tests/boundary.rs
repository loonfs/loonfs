//! The manifest boundary: a collection raises it to the highest manifest that
//! it deletes, before it deletes any manifest.

use super::*;
use loonfs_objectstore::keys::boundary;
use loonfs_types::format::control::ManifestBoundary;

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
        .map(|loaded| {
            let ManifestBoundary::Manifest { manifest_no } = loaded.state.boundary;
            manifest_no.0
        })
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
/// manifest 1 as the newest and lists no snapshot, then a snapshot is created
/// on manifest 1, manifests 2 and 3 are published, and collection B deletes
/// manifest 2 and raises the boundary to 2 before A lists the manifests.
#[tokio::test]
async fn a_collection_deletes_only_the_manifests_that_it_chose() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("boundary-chosen").expect("namespace");
    let store = Arc::new(MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
    ));
    publish_manifests(&store, &namespace_id, 1).await;
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
            .expect("snapshot on manifest 1");
        assert_eq!(snapshot.snapshot_id.manifest_no(), ManifestNo(1));
        publish_more(&store, &namespace_id, 3).await;
        let report_b = collect(&store, &namespace_id, &context(NOW_MS)).await;
        assert_eq!(report_b.deleted.manifests, 1);
        assert!(!manifest_exists(&store, &namespace_id, 2).await);
        assert_eq!(stored_boundary(&store, &namespace_id).await, Some(2));
        blocked.release();
    });
    let report_a = report_a.expect("collection A");
    assert_eq!(report_a.deleted.manifests, 0);
    assert!(manifest_exists(&store, &namespace_id, 1).await);
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
    let young_key = metadata_manifest_object(&namespace_id, &ManifestNo(5));
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
    publish_manifests(&store, &namespace_id, 2).await;
    // The snapshot's fold publishes manifest 3, and the snapshot keeps it.
    let snapshot = create_snapshot(&store, &namespace_id, &context(1_000))
        .await
        .expect("snapshot on manifest 3");
    assert_eq!(snapshot.snapshot_id.manifest_no(), ManifestNo(3));
    publish_more(&store, &namespace_id, 6).await;

    let recorded = RecordingStore::new(
        Arc::clone(&store),
        KeyPredicate::exact(boundary(&namespace_id)),
    );
    let report = collect(&recorded, &namespace_id, &context(NOW_MS)).await;
    assert_eq!(report.deleted.manifests, 3);
    assert_eq!(stored_boundary(&store, &namespace_id).await, Some(4));
    for (number, exists) in [
        (1, false),
        (2, false),
        (3, true),
        (4, false),
        (5, true),
        (6, true),
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
