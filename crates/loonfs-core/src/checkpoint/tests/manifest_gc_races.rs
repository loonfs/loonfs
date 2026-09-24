//! Manifest discovery while a lagging hint advances and old manifests are swept.

use super::*;
use loonfs_test_support::stores::MetadataMapStore;

async fn discover_during_collection(start: ManifestNo, block_next_manifest: bool) {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("discovery-gc").expect("namespace");
    let store = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
    );
    let context = test_context();
    crate::namespace::bootstrap::bootstrap_namespace(
        &store,
        &namespace_id,
        &context,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("bootstrap");
    if start == ManifestNo(2) {
        acquire_writer_epoch(&store, &namespace_id, &context)
            .await
            .expect("advance initial hint");
    }
    // Failed hint updates are allowed after durable manifest publication.
    // Keep that real lagging hint until the reader has consumed it.
    let failed_hint = FailStore::new(
        store,
        KeyPredicate::hint(&namespace_id),
        OperationClass::CompareAndSwap,
        InjectedError::Transport("hint unavailable".into()),
    );
    failed_hint.fail_all();
    for _ in start.0..4 {
        acquire_writer_epoch(&failed_hint, &namespace_id, &context)
            .await
            .expect("publish despite failed hint update");
    }
    let expected = load_current_manifest(&failed_hint, &namespace_id)
        .await
        .expect("current manifest");
    assert_eq!(expected.state.manifest.manifest_no, ManifestNo(4));
    assert_eq!(expected.discovery_start_manifest_no, start);
    failed_hint.clear();
    let blocked_number = ManifestNo(start.0 + u64::from(block_next_manifest));
    let blocked_key = metadata_manifest_object(&namespace_id, &blocked_number);
    let blocked = BlockingStore::new(
        failed_hint,
        KeyPredicate::exact(&blocked_key),
        OperationClass::Get,
    );
    blocked.block_next();
    let (discovered, ()) = tokio::join!(load_current_manifest(&blocked, &namespace_id), async {
        blocked.wait_until_blocked().await;
        crate::namespace::control::raise_hint(
            blocked.inner(),
            &namespace_id,
            expected.state.manifest.manifest_no,
            loonfs_api::WalNo(0),
            None,
        )
        .await
        .expect("raise discovery hint");
        let config = crate::gc::GcConfig::default();
        let report = crate::gc::gc_namespace(
            blocked.inner(),
            &namespace_id,
            &config,
            &MutationContext {
                now_ms: config.grace_window_ms + 1,
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
    let discovered = discovered.expect("reload the advanced hint after collection");
    assert_eq!(discovered.state, expected.state);
    assert_eq!(discovered.discovery_start_manifest_no, ManifestNo(4));
}

#[tokio::test]
async fn discovery_reloads_a_hint_when_gc_removes_its_starting_manifest() {
    for start in [ManifestNo(1), ManifestNo(2)] {
        discover_during_collection(start, false).await;
    }
}

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
    let selected = load_current_manifest(&store, &namespace_id)
        .await
        .expect("current manifest");
    assert!(selected.state.manifest.manifest_no > ManifestNo(1));
    store
        .delete(&selected.object_key)
        .await
        .expect("remove hinted manifest");
    let error = load_current_manifest(&store, &namespace_id)
        .await
        .expect_err("an unchanged hint cannot explain a missing manifest");
    assert!(matches!(
        error,
        crate::control_object::ControlObjectLoadError::Codec { .. }
    ));
}
