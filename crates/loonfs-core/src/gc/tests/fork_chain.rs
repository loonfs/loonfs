//! Transitive fork protection for content initially published inline.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};

async fn assert_leaf_reads_every_owner(store: &LocalFsStore, namespaces: &[NamespaceId; 3]) {
    let namespace = crate::test_support::namespace_store(store, &namespaces[2]);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("fresh leaf view");
    for (index, owner) in namespaces.iter().enumerate() {
        let file = view
            .read_file_bytes(
                &format!("/owned-{index}"),
                None,
                &ReadAccess::live(Authorizer::Unrestricted),
            )
            .await
            .expect("read inherited or local content");
        assert_eq!(file.bytes, owner.as_str().as_bytes());
    }
}

#[tokio::test]
async fn live_grandchild_keeps_deleted_ancestors_pinned_until_retirement_runs_leaf_first() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespaces =
        ["root", "middle", "leaf"].map(|name| NamespaceId::parse(name).expect("namespace"));
    let setup = context(1_000);
    let mut keys = Vec::new();
    for (index, namespace_id) in namespaces.iter().enumerate() {
        if index == 0 {
            crate::test_support::NamespaceFixture::new(&store, namespace_id, &setup)
                .create()
                .await
                .expect("bootstrap root");
        } else {
            crate::test_support::NamespaceFixture::new(&store, &namespaces[index - 1], &setup)
                .engine()
                .fork_namespace(namespace_id, &loonfs_test_support::test_actor(), None)
                .await
                .expect("fork");
        }
        keys.push(publish_inline(&store, namespace_id, index, &setup).await);
    }
    let middle_pin = read_fork_record(&store, &namespaces[0]).await;
    let leaf_pin = read_fork_record(&store, &namespaces[1]).await;
    for namespace_id in &namespaces[..2] {
        crate::test_support::NamespaceFixture::new(&store, namespace_id, &setup)
            .engine()
            .delete_namespace(Default::default())
            .await
            .expect("delete ancestor");
    }
    let mut aged_now = 0;
    for namespace_id in &namespaces {
        aged_now = aged_now.max(
            now_after_newest_object(
                &store,
                namespace_id,
                UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + GRACE_MS + 1,
            )
            .await,
        );
    }
    let aged = context(aged_now);
    for index in [0, 1, 1, 0] {
        let report = crate::test_support::namespace_store(&store, &namespaces[index])
            .gc(None, &options(), &aged)
            .await
            .expect("collect ancestor while grandchild lives");
        assert_eq!(report.deleted.retired_content_objects, 0);
        assert_eq!(report.deleted_checkpoints_by_owner.fork, 0);
        assert!(pin_exists(&store, &namespaces[0], &middle_pin.pin_id).await);
        assert!(pin_exists(&store, &namespaces[1], &leaf_pin.pin_id).await);
        assert_leaf_reads_every_owner(&store, &namespaces).await;
    }

    crate::test_support::NamespaceFixture::new(&store, &namespaces[2], &setup)
        .engine()
        .delete_namespace(Default::default())
        .await
        .expect("delete leaf");
    // Deletion alone must not release either link in the protection chain.
    for index in [0, 1] {
        let report = crate::test_support::namespace_store(&store, &namespaces[index])
            .gc(None, &options(), &aged)
            .await
            .expect("ancestors still pinned by unretired descendants");
        assert_eq!(report.deleted.retired_content_objects, 0);
    }
    for index in [2, 1, 0] {
        crate::test_support::namespace_store(&store, &namespaces[index])
            .gc(None, &options(), &aged)
            .await
            .expect("retire from leaf to root");
        assert!(store
            .head(&keys[index])
            .await
            .expect("retired object")
            .is_none());
        // Retirement may delete only this owner's objects, not inherited ones.
        for key in &keys[..index] {
            assert!(store.head(key).await.expect("ancestor object").is_some());
        }
        if index > 0 {
            let pin = if index == 2 { &leaf_pin } else { &middle_pin };
            assert!(!pin_exists(&store, &namespaces[index - 1], &pin.pin_id).await);
        }
    }
}
