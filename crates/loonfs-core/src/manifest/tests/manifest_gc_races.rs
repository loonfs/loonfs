//! Finding the newest manifest, and publishing one, while a collection raises
//! the manifest boundary and deletes old manifests.

use super::*;
use crate::manifest::publish::ManifestUpdate;
use loonfs_test_support::stores::MetadataMapStore;
use std::assert_matches;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Parks a search for the newest manifest after it listed manifests 1 to 3,
/// publishes manifests 4 and 5 and collects the manifests below 5 while the
/// search waits, and checks that the search starts again and returns
/// manifest 5.
///
/// With `park_last_boundary_read`, the parked request is the search's second
/// read of the boundary, after the search loaded manifest 3. Otherwise it's
/// the load of manifest 3, which the collection deletes.
async fn find_during_collection(park_last_boundary_read: bool) {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("discovery-gc").expect("namespace");
    let store = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
    );
    let context = test_context();
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    let claim = || async {
        crate::test_support::namespace_store(&store, &namespace_id)
            .acquire_writer_epoch(&context.writer_id)
            .await
            .expect("publish a manifest");
    };
    claim().await;
    claim().await;
    let listed = metadata_manifest_object(&namespace_id, &ManifestNo(3));
    let boundary_key = boundary(&namespace_id);
    let boundary_reads = AtomicUsize::new(0);
    let blocked = BlockingStore::matching(store, move |operation| {
        if park_last_boundary_read {
            operation.key() == boundary_key
                && OperationSelector::Read.matches(operation.kind())
                && boundary_reads.fetch_add(1, Ordering::SeqCst) == 1
        } else {
            operation.key() == listed && OperationSelector::Get.matches(operation.kind())
        }
    });
    blocked.block_next();
    let namespace = crate::test_support::namespace_store(&blocked, &namespace_id);
    let (found, ()) = tokio::join!(namespace.load_current_manifest(), async {
        blocked.wait_until_blocked().await;
        for _ in 4..=5 {
            crate::test_support::namespace_store(blocked.inner(), &namespace_id)
                .acquire_writer_epoch(&context.writer_id)
                .await
                .expect("publish a manifest");
        }
        let report = crate::test_support::namespace_store(blocked.inner(), &namespace_id)
            .gc(&WriterAt {
                now_ms: crate::limits::GC_GRACE_WINDOW_MS + 1,
                ..context.clone()
            }
            .clock())
            .await
            .expect("collect old manifests");
        assert_eq!(report.deleted.manifests, 4);
        blocked.release();
    });
    assert_eq!(
        found
            .expect("find the newest manifest")
            .manifest()
            .manifest_no,
        ManifestNo(5)
    );
}

/// Checks that a search for the newest manifest starts again when the
/// highest manifest that it listed is gone by its load.
#[tokio::test]
async fn the_cold_find_starts_again_when_the_listed_manifest_is_collected() {
    find_during_collection(false).await;
}

/// Checks that a search for the newest manifest starts again when a
/// collection raises the boundary over the manifest that it loaded, between
/// the load and the last read of the boundary, and returns the newer
/// manifest.
#[tokio::test]
async fn the_cold_find_starts_again_when_the_boundary_passes_the_loaded_manifest() {
    find_during_collection(true).await;
}

/// Checks that a search for the newest manifest that lists no manifest
/// returns `namespace_not_found` when the boundary object is missing, and
/// reports a corrupt store when the boundary object exists.
#[tokio::test]
async fn an_empty_listing_is_not_found_without_a_boundary_and_corrupt_with_one() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("missing-manifest").expect("namespace");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    assert_matches!(
        namespace.load_current_manifest().await,
        Err(CoreError::NamespaceNotFound { .. })
    );
    bootstrap_namespace(&store, &namespace_id, &test_context())
        .await
        .expect("bootstrap");
    let current = namespace
        .load_current_manifest()
        .await
        .expect("current manifest");
    // Cleanup never raises the boundary to the newest manifest.
    namespace
        .raise_boundary(current.manifest().manifest_no)
        .await
        .expect("raise the boundary");
    let error = namespace
        .load_current_manifest()
        .await
        .expect_err("a boundary with no manifest above it is a corrupt store");
    assert_matches!(
        error,
        CoreError::ControlObjectLoad(crate::control_object::ControlObjectLoadError::Codec {
            object_key,
            ..
        }) if object_key == boundary(&namespace_id)
    );
}

/// Checks that a create whose put of manifest 1 lands after a collection
/// deleted the namespace's manifest 1, and whose response is lost, settles
/// the put as its own and then counts it as lost because manifest 1 is at
/// the boundary. The create returns `namespace_exists`, and the newest
/// manifest stays the one that another create's namespace published.
#[tokio::test]
async fn a_late_ambiguous_put_at_an_emptied_number_is_lost() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("late-manifest").expect("namespace");
    let store = Arc::new(MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
    ));
    let context = test_context();
    let object_key = metadata_manifest_object(&namespace_id, &ManifestNo(1));
    let recorded = Arc::new(RecordingStore::new(store.clone(), KeyPredicate::any()));
    let failing = FailStore::new(
        recorded.clone(),
        KeyPredicate::exact(&object_key),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::Transport("lost acknowledgement".to_owned()),
    )
    .apply_then_fail();
    failing.fail_next(1);
    let blocked = BlockingStore::new(
        failing,
        KeyPredicate::exact(&object_key),
        OperationSelector::PutCreateIfAbsent,
    );
    let late = NamespaceManifestPayload::initial(
        namespace_id.clone(),
        loonfs_types::PoolId::generate(),
        context.now_ms,
        loonfs_test_support::test_actor(),
        loonfs_types::NamespaceAccess::Unrestricted {},
    );
    let late_namespace = crate::test_support::namespace_store(&blocked, &namespace_id);
    let clock = context.clock();
    blocked.block_next();
    let (outcome, ()) = tokio::join!(late_namespace.publish_first_manifest(late, &clock), async {
        blocked.wait_until_blocked().await;
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .create()
            .await
            .expect("another create");
        for _ in 2..=3 {
            crate::test_support::namespace_store(&store, &namespace_id)
                .acquire_writer_epoch(&context.writer_id)
                .await
                .expect("publish a manifest");
        }
        let report = crate::test_support::namespace_store(&store, &namespace_id)
            .gc(&WriterAt {
                now_ms: crate::limits::GC_GRACE_WINDOW_MS + 1,
                ..context.clone()
            }
            .clock())
            .await
            .expect("collect old manifests");
        assert_eq!(report.deleted.manifests, 2);
        recorded.reset();
        blocked.release();
    });
    assert_matches!(outcome, Err(CoreError::NamespaceExists { .. }));
    assert_eq!(blocked.inner().remaining(), 0);
    let manifest = |number| metadata_manifest_object(&namespace_id, &ManifestNo(number));
    let boundary = boundary(&namespace_id);
    // The put, the load that settles it as the create's own, the boundary
    // read that counts it as lost, and the search for the newest manifest.
    assert_eq!(
        control_requests(&namespace_id, &recorded.take()),
        vec![
            ("put", manifest(1)),
            ("get", manifest(1)),
            ("get", boundary.clone()),
            ("get", boundary.clone()),
            ("list", metadata_manifest_prefix(&namespace_id)),
            ("get", manifest(3)),
            ("get", boundary),
        ]
    );
    assert_eq!(
        crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("current manifest")
            .manifest()
            .manifest_no,
        ManifestNo(3)
    );
}

/// Checks that a publication whose basis's successor a collection deleted
/// puts the deleted number, counts that put as lost because the number is at
/// the boundary, and publishes after the newest manifest. A put whose
/// response is lost is read back, found at the boundary, and settled against
/// the newest manifest too, so the next attempt builds on that manifest.
#[tokio::test]
async fn a_put_at_a_number_that_a_collection_emptied_is_lost() {
    for ambiguous in [false, true] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("stale-basis").expect("namespace");
        let store = MetadataMapStore::aged(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::manifest(&namespace_id),
        );
        let context = test_context();
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .create()
            .await
            .expect("create");
        crate::test_support::namespace_store(&store, &namespace_id)
            .acquire_writer_epoch(&context.writer_id)
            .await
            .expect("publish manifest 2");
        let basis = crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_basis()
            .await
            .expect("manifest 2");
        // Other processes publish manifests 3 to 5, and a collection one grace
        // window later deletes every manifest below 5.
        for _ in 3..=5 {
            crate::test_support::namespace_store(&store, &namespace_id)
                .acquire_writer_epoch(&context.writer_id)
                .await
                .expect("another process publishes");
        }
        let report = crate::test_support::namespace_store(&store, &namespace_id)
            .gc(&WriterAt {
                now_ms: crate::limits::GC_GRACE_WINDOW_MS + 1,
                ..context.clone()
            }
            .clock())
            .await
            .expect("collect old manifests");
        assert_eq!(report.deleted.manifests, 4);
        let successor = metadata_manifest_object(&namespace_id, &ManifestNo(3));
        assert!(store
            .head(&successor)
            .await
            .expect("the basis's successor")
            .is_none());

        let recorded = Arc::new(RecordingStore::new(store, KeyPredicate::any()));
        let failing = FailStore::new(
            recorded.clone(),
            KeyPredicate::exact(successor),
            OperationSelector::PutCreateIfAbsent,
            InjectedError::Transport("lost acknowledgement".to_owned()),
        )
        .apply_then_fail();
        failing.fail_next(usize::from(ambiguous));
        let ManifestUpdate {
            current: published, ..
        } = crate::test_support::namespace_store(&failing, &namespace_id)
            .update_manifest(
                Some(basis),
                &OperationClock::start(0, crate::test_support::std_timer()),
                |current: crate::namespace::control::LoadedManifest| async move {
                    Ok(super::super::publish::ManifestChange::Next(
                        Box::new(current.payload().clone()),
                        (),
                    ))
                },
            )
            .await
            .expect("publish");
        assert_eq!(published.manifest.manifest().manifest_no, ManifestNo(6));
        let manifest = |number| metadata_manifest_object(&namespace_id, &ManifestNo(number));
        let boundary = boundary(&namespace_id);
        let read_back = ambiguous.then(|| ("get", manifest(3)));
        assert_eq!(
            control_requests(&namespace_id, &recorded.take()),
            [("put", manifest(3))]
                .into_iter()
                .chain(read_back)
                .chain([
                    ("get", boundary.clone()),
                    ("get", boundary.clone()),
                    ("list", metadata_manifest_prefix(&namespace_id)),
                    ("get", manifest(5)),
                    ("get", boundary.clone()),
                    ("put", manifest(6)),
                    ("get", boundary),
                ])
                .collect::<Vec<_>>(),
            "ambiguous: {ambiguous}"
        );
    }
}
