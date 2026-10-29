//! Manifest discovery while a lagging hint advances and old manifests are swept.

use super::*;
use loonfs_test_support::stores::MetadataMapStore;
use std::assert_matches;

/// Parks a request of discovery, raises the hint to manifest 4 and collects
/// the manifests below it while the request waits, and checks that
/// discovery then returns manifest 4 and the raised hint.
///
/// With `block_probe`, the parked request is the probe's HEAD of the number
/// after `start`, and otherwise it's the GET of manifest `start`.
async fn discover_during_collection(start: ManifestNo, block_probe: bool) {
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
    if start == ManifestNo(2) {
        crate::test_support::namespace_store(&store, &namespace_id)
            .acquire_writer_epoch(&context)
            .await
            .expect("publish manifest 2");
        crate::test_support::namespace_store(&store, &namespace_id)
            .raise_hint(start, None)
            .await
            .expect("advance initial hint");
    }
    // A publication doesn't raise the hint, so the hint lags at `start` until
    // the reader below has read it.
    for _ in start.0..4 {
        crate::test_support::namespace_store(&store, &namespace_id)
            .acquire_writer_epoch(&context)
            .await
            .expect("publish without a hint raise");
    }
    let (expected, initial_hint) = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest_with_hint()
        .await
        .expect("current manifest");
    assert_eq!(expected.state.manifest().manifest_no, ManifestNo(4));
    assert_eq!(initial_hint.state.manifest_no, start);
    let blocked_number = ManifestNo(start.0 + u64::from(block_probe));
    let blocked_key = metadata_manifest_object(&namespace_id, &blocked_number);
    let blocked = BlockingStore::new(
        store,
        KeyPredicate::exact(&blocked_key),
        if block_probe {
            OperationSelector::Head
        } else {
            OperationSelector::Get
        },
    );
    blocked.block_next();
    let namespace = crate::test_support::namespace_store(&blocked, &namespace_id);
    let (discovered, ()) = tokio::join!(namespace.load_current_manifest_with_hint(), async {
        blocked.wait_until_blocked().await;
        crate::test_support::namespace_store(blocked.inner(), &namespace_id)
            .raise_hint(expected.state.manifest().manifest_no, None)
            .await
            .expect("raise discovery hint");
        let options = crate::gc::GcOptions::default();
        let report = crate::test_support::namespace_store(blocked.inner(), &namespace_id)
            .gc(
                None,
                &options,
                &MutationContext {
                    now_ms: options.grace_window_ms + 1,
                    ..context.clone()
                },
            )
            .await
            .expect("collect old manifests");
        assert_eq!(report.deleted.manifests, 3);
        assert!(blocked
            .inner()
            .head(&blocked_key)
            .await
            .expect("collected manifest")
            .is_none());
        blocked.release();
    });
    let (discovered, hint) = discovered.expect("reload the advanced hint after collection");
    assert_eq!(discovered.state, expected.state);
    assert_eq!(hint.state.manifest_no, ManifestNo(4));
}

#[tokio::test]
async fn discovery_reloads_a_hint_when_gc_removes_its_starting_manifest() {
    for start in [ManifestNo(1), ManifestNo(2)] {
        discover_during_collection(start, false).await;
    }
}

/// Checks that discovery whose probe finds the manifest after the hinted one
/// collected reads the newer hint again and finds the newest manifest.
#[tokio::test]
async fn discovery_reloads_a_hint_when_gc_removes_the_next_manifest() {
    for start in [ManifestNo(1), ManifestNo(2)] {
        discover_during_collection(start, true).await;
    }
}

#[tokio::test]
async fn discovery_still_rejects_a_missing_manifest_when_the_hint_has_not_advanced() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("missing-manifest").expect("namespace");
    bootstrap_namespace(&store, &namespace_id, &test_context())
        .await
        .expect("bootstrap");
    // The runtime that published the bootstrap's manifests raises the hint.
    crate::test_support::namespace_store(&store, &namespace_id)
        .raise_hint_to_current_manifest()
        .await;
    let selected = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest");
    assert!(selected.state.manifest().manifest_no > ManifestNo(1));
    store
        .delete(&selected.object_key)
        .await
        .expect("remove hinted manifest");
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect_err("an unchanged hint cannot explain a missing manifest");
    assert_matches!(
        error,
        crate::control_object::ControlObjectLoadError::Codec { .. }
    );
}

#[tokio::test]
async fn a_late_ambiguous_put_cannot_confirm_a_recreated_manifest() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("late-manifest").expect("namespace");
    let store = Arc::new(MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
    ));
    let context = test_context();
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("create");
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current");
    let mut payload = current.state.envelope.payload().clone();
    payload.manifest_no = ManifestNo(2);
    let manifest = encode_manifest(payload.clone());
    let object_key = metadata_manifest_object(&namespace_id, &ManifestNo(2));
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
    let timer = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let deadline = Deadline::start(timer.clone());
    blocked.block_next();
    let (outcome, ()) = tokio::join!(
        publish_once_within(&blocked, manifest.clone(), &deadline),
        async {
            blocked.wait_until_blocked().await;
            publish_once(&store, manifest.clone())
                .await
                .expect("publish manifest two");
            payload.manifest_no = ManifestNo(3);
            publish_once(&store, encode_manifest(payload))
                .await
                .expect("publish manifest three");
            // The runtime that published the manifests raises the hint.
            crate::test_support::namespace_store(&store, &namespace_id)
                .raise_hint_to_current_manifest()
                .await;
            let options = crate::gc::GcOptions::default();
            timer.advance_ms(options.grace_window_ms + 1);
            let report = crate::NamespaceStore {
                timer: timer.clone(),
                ..crate::test_support::namespace_store(&store, &namespace_id)
            }
            .gc(
                None,
                &options,
                &MutationContext {
                    now_ms: timer.now_ms(),
                    ..context.clone()
                },
            )
            .await
            .expect("collect old manifests");
            assert_eq!(report.deleted.manifests, 2);
            assert!(store
                .get(&object_key, None)
                .await
                .expect("collected manifest")
                .is_none());
            recorded.reset();
            blocked.release();
        }
    );
    assert_matches!(
        outcome,
        Err(CoreError::OutcomeUnknown {
            object_key: actual_key,
            ..
        }) if actual_key == object_key
    );
    assert_eq!(blocked.inner().remaining(), 0);
    assert_eq!(recorded.counts().create_if_absent_puts, 1);
    assert_eq!(recorded.counts().compare_and_swaps, 0);
    assert_eq!(recorded.take_gets(), vec![(object_key.clone(), None)]);
    assert_eq!(
        store
            .get(&object_key, None)
            .await
            .expect("recreated manifest"),
        Some(Bytes::from(manifest.into_bytes()))
    );
    let (current, hint) = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest_with_hint()
        .await
        .expect("current manifest");
    assert_eq!(current.state.manifest().manifest_no, ManifestNo(3));
    assert_eq!(hint.state.manifest_no, ManifestNo(3));
}

/// Checks that a publication whose basis was checked the revalidation bound
/// ago or longer, and whose basis's successor has been deleted by garbage
/// collection, loads the current manifest through the hint and publishes
/// after it.
///
/// The basis's successor is missing, so a publication that put the
/// successor's number without discovery would create the deleted number
/// again, beside the newer manifests.
#[tokio::test]
async fn a_stale_basis_whose_successor_was_collected_publishes_after_the_current_manifest() {
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
        .acquire_writer_epoch(&context)
        .await
        .expect("publish manifest 2");
    let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let basis = crate::namespace::PublicationBasis {
        manifest: crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("manifest 2"),
        checked: crate::time::Observation::now(clock.clone()),
    };
    // Other processes publish manifests 3 to 5 and raise the hint, and a
    // garbage collection pass one grace later deletes every manifest below
    // the hint.
    for _ in 3..=5 {
        crate::test_support::namespace_store(&store, &namespace_id)
            .acquire_writer_epoch(&context)
            .await
            .expect("another process publishes");
    }
    crate::test_support::namespace_store(&store, &namespace_id)
        .raise_hint_to_current_manifest()
        .await;
    let options = crate::gc::GcOptions::default();
    clock.advance_ms(options.grace_window_ms + 1);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(
            None,
            &options,
            &MutationContext {
                now_ms: options.grace_window_ms + 1,
                ..context.clone()
            },
        )
        .await
        .expect("collect old manifests");
    assert_eq!(report.deleted.manifests, 4);
    let successor = metadata_manifest_object(&namespace_id, &ManifestNo(3));
    assert!(store
        .head(&successor)
        .await
        .expect("the basis's successor")
        .is_none());

    let ((), published, hint) = crate::test_support::namespace_store(&store, &namespace_id)
        .update_manifest(
            Some(basis),
            &Deadline::start(clock),
            crate::limits::MAX_CONTENTION_ATTEMPTS,
            |current: crate::namespace::control::LoadedManifest| async move {
                Ok(crate::manifest::publish::ManifestChange::Next(
                    Box::new(current.state.envelope.payload().clone()),
                    (),
                ))
            },
        )
        .await
        .expect("publish");
    assert_eq!(published.state.manifest().manifest_no, ManifestNo(6));
    assert_eq!(hint.expect("discovery").state.manifest_no, ManifestNo(5));
    assert!(
        store
            .head(&successor)
            .await
            .expect("the basis's successor")
            .is_none(),
        "the publication didn't put the deleted number again"
    );
}
