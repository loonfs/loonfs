//! Overlapping retirement collections preserve other members and retry partial effects.

use super::*;
use loonfs_objectstore::keys::{content_prefix, pin};

/// Checks that overlapping retirements of a fork tolerate a source pin
/// delete whose acknowledgement was lost, that the fork's retirement deletes
/// none of the content the fork wrote, and that the pool's collection later
/// collects that content while a live sibling keeps what it inherited.
#[tokio::test]
async fn overlapping_retirement_retries_a_lost_pin_delete_and_preserves_a_live_sibling() {
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
        let fixture = crate::test_support::NamespaceFixture::new(&inner, &source, &setup);
        fixture
            .engine()
            .fork_namespace(
                fork,
                &loonfs_test_support::test_actor(),
                crate::test_support::folded_manifest(&fixture.namespace)
                    .await
                    .expect("fold the WAL tail"),
            )
            .await
            .expect("fork");
    }
    for number in 0..3 {
        put_file_content_with_id(
            &inner,
            &target,
            &format!("/owned-{number}"),
            b"owned",
            numbered_content_id(3 + number),
        )
        .await;
    }
    // The target's content is stored under the prefix of the source's pool.
    let pool_id = crate::test_support::load_pool_id(&inner, &source).await;
    assert_eq!(
        crate::test_support::load_pool_id(&inner, &target).await,
        pool_id
    );
    let target_keys = inner
        .list_prefix(&content_prefix(&pool_id))
        .await
        .expect("pool content")
        .into_iter()
        .filter(|key| !source_keys.contains(key))
        .collect::<Vec<_>>();
    assert_eq!(target_keys.len(), 3);
    let source_pin_of = |manifest: &crate::namespace::control::LoadedManifest| {
        manifest
            .payload()
            .fork_basis
            .as_ref()
            .expect("fork basis")
            .source_pin_id
            .clone()
    };
    let target_pin = source_pin_of(
        &crate::test_support::namespace_store(&inner, &target)
            .load_current_manifest()
            .await
            .expect("target"),
    );
    let sibling_pin = source_pin_of(
        &crate::test_support::namespace_store(&inner, &sibling)
            .load_current_manifest()
            .await
            .expect("sibling"),
    );
    for namespace in [&source, &target] {
        crate::test_support::NamespaceFixture::new(&inner, namespace, &setup)
            .engine()
            .delete_namespace(Default::default())
            .await
            .expect("delete");
    }
    let deadline = context(setup.now_ms + retirement_ms());
    let gc_options = options();
    let target_pin_key = pin(&source, &target_pin);
    let store = BlockingStore::new(
        FailStore::new(
            inner,
            KeyPredicate::exact(&target_pin_key),
            OperationSelector::Delete,
            InjectedError::Transport("delete acknowledgement lost".to_owned()),
        )
        .apply_then_fail(),
        KeyPredicate::exact(&target_pin_key),
        OperationSelector::Delete,
    );
    store.block_next();
    let namespace = crate::test_support::namespace_store(&store, &target);
    let clock = deadline.clock();
    let (delayed, ()) = tokio::join!(namespace.gc(&gc_options, &clock), async {
        store.wait_until_blocked().await;
        store.inner().fail_next(1);
        crate::test_support::namespace_store(store.inner(), &target)
            .gc(&gc_options, &deadline.clock())
            .await
            .expect_err("a delete succeeded but its acknowledgement was lost");
        assert!(!pin_exists(store.inner(), &source, &target_pin).await);
        store.inner().clear();
        let retried = crate::test_support::namespace_store(store.inner(), &target)
            .gc(&gc_options, &deadline.clock())
            .await
            .expect("second collector retries from the beginning");
        assert_eq!(retried.deleted_checkpoints_by_owner.fork, 0);
        assert!(pin_exists(store.inner(), &source, &sibling_pin).await);
        store.release();
    });
    delayed.expect("the older collector tolerates an already-deleted pin");
    for key in source_keys.iter().chain(&target_keys) {
        assert!(store.head(key).await.expect("pool content").is_some());
    }
    // The pool's collection keeps collecting while its deleted first member
    // is retained, and the retired target is no longer a member whose views
    // protect the content.
    let aged = context(
        now_after_newest_object(&store, &source, GRACE_MS + 1)
            .await
            .max(deadline.now_ms),
    );
    let report = collect_pool(&store, &pool_id, &aged).await;
    assert_eq!(report.deleted.content_objects, target_keys.len() as u64);
    for key in &target_keys {
        assert!(store.head(key).await.expect("target content").is_none());
    }
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
}
