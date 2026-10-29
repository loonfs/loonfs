//! Overlapping retirement passes preserve other owners and retry partial effects.

use super::*;
use loonfs_objectstore::keys::pin;

#[tokio::test]
async fn overlapping_retirement_retries_lost_delete_ack_and_preserves_a_live_sibling() {
    let directory = tempdir().expect("directory");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let source = NamespaceId::parse("source").expect("source");
    let target = NamespaceId::parse("target").expect("target");
    let sibling = NamespaceId::parse("sibling").expect("sibling");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&inner, &source, &setup)
        .create()
        .await
        .expect("bootstrap source");
    let source_keys = publish_owned_content(&inner, &source, 2).await;
    for fork in [&target, &sibling] {
        crate::test_support::NamespaceFixture::new(&inner, &source, &setup)
            .engine()
            .fork_namespace(fork, &loonfs_test_support::test_actor(), None)
            .await
            .expect("fork");
    }
    for number in 0..3 {
        write_test_file(
            &inner,
            &target,
            &format!("/owned-{number}"),
            &format!("owned-{number}"),
            &setup,
        )
        .await;
    }
    let target_content_prefix = format!("namespaces/{target}/content/");
    let target_keys = inner
        .list_prefix(&target_content_prefix)
        .await
        .expect("target content");
    let late_bytes = inner
        .get(&target_keys[0], None)
        .await
        .expect("read owned bytes")
        .expect("content");
    let target_manifest = crate::test_support::namespace_store(&inner, &target)
        .load_current_manifest()
        .await
        .expect("target");
    let target_pin = target_manifest
        .state
        .envelope
        .payload()
        .fork_basis
        .as_ref()
        .expect("target basis")
        .source_pin_id
        .clone();
    let sibling_manifest = crate::test_support::namespace_store(&inner, &sibling)
        .load_current_manifest()
        .await
        .expect("sibling");
    let sibling_pin = sibling_manifest
        .state
        .envelope
        .payload()
        .fork_basis
        .as_ref()
        .expect("sibling basis")
        .source_pin_id
        .clone();
    for namespace in [&source, &target] {
        crate::test_support::NamespaceFixture::new(&inner, namespace, &setup)
            .engine()
            .delete_namespace(Default::default())
            .await
            .expect("delete");
    }
    let deadline = context(setup.now_ms + retirement_ms());
    let gc_options = options();
    let store = BlockingStore::new(
        FailStore::new(
            inner,
            KeyPredicate::exact(&target_keys[0]),
            OperationSelector::Delete,
            InjectedError::Transport("delete acknowledgement lost".to_owned()),
        )
        .apply_then_fail(),
        KeyPredicate::exact(&target_keys[0]),
        OperationSelector::Delete,
    );
    store.block_next();
    let namespace = crate::test_support::namespace_store(&store, &target);
    let (delayed, ()) = tokio::join!(namespace.gc(None, &gc_options, &deadline), async {
        store.wait_until_blocked().await;
        store.inner().fail_next(1);
        crate::test_support::namespace_store(store.inner(), &target)
            .gc(None, &gc_options, &deadline)
            .await
            .expect_err("a delete landed but its acknowledgement was lost");
        assert!(store
            .inner()
            .head(&target_keys[0])
            .await
            .expect("landed delete")
            .is_none());
        assert!(
            pin_exists(store.inner(), &source, &target_pin).await,
            "failed sweep does not release its source pin"
        );
        store.inner().clear();
        crate::test_support::namespace_store(store.inner(), &source)
            .gc(None, &gc_options, &deadline)
            .await
            .expect("source stays pinned");
        for key in &source_keys {
            assert!(store
                .inner()
                .head(key)
                .await
                .expect("inherited content")
                .is_some());
        }

        crate::test_support::namespace_store(store.inner(), &target)
            .gc(None, &gc_options, &deadline)
            .await
            .expect("second collector retries from the beginning");
        assert!(!pin_exists(store.inner(), &source, &target_pin).await);
        assert!(pin_exists(store.inner(), &source, &sibling_pin).await);
        assert!(store
            .inner()
            .list_prefix(&target_content_prefix)
            .await
            .expect("target content")
            .is_empty());
        // Recreate an immutable object after one sweep finishes, while the
        // first collector still has its original delete in flight.
        store
            .inner()
            .put_if_absent(&target_keys[0], late_bytes.clone())
            .await
            .expect("late materialization");
        store.release();
    });
    delayed.expect("the older collector tolerates already-deleted objects and pin");
    assert!(store
        .list_prefix(&target_content_prefix)
        .await
        .expect("target content")
        .is_empty());
    assert!(store
        .head(&pin(&source, &sibling_pin))
        .await
        .expect("sibling pin")
        .is_some());
    for key in &source_keys {
        assert_eq!(
            store.get(key, None).await.expect("inherited content"),
            Some(Bytes::from_static(b"content"))
        );
    }
    let namespace = crate::test_support::namespace_store(&store, &sibling);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("fresh sibling view");
    for number in 0..2 {
        view.resolve_path(
            &format!("/file-{number}"),
            AttributeInclusion::Omit,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("inherited file stays visible");
    }
    crate::test_support::namespace_store(&store, &target)
        .gc(None, &gc_options, &deadline)
        .await
        .expect("repeated target retirement");
    crate::test_support::namespace_store(&store, &source)
        .gc(None, &gc_options, &deadline)
        .await
        .expect("sibling continues to retain source");
    for key in source_keys {
        assert!(store.head(&key).await.expect("source content").is_some());
    }
}
