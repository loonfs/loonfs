//! Transitive fork protection for content initially published inline.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};

async fn assert_leaf_reads_every_members_file(store: &LocalFsStore, namespaces: &[NamespaceId; 3]) {
    let namespace = crate::test_support::namespace_store(store, &namespaces[2]);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("fresh leaf view");
    for (index, writer) in namespaces.iter().enumerate() {
        let file = view
            .read_file_bytes(
                &format!("/owned-{index}"),
                None,
                &ReadAccess::live(Authorizer::Service),
            )
            .await
            .expect("read inherited or local content");
        assert_eq!(file.bytes, writer.as_str().as_bytes());
    }
}

#[tokio::test]
async fn live_grandchild_keeps_deleted_ancestors_until_retirement_runs_leaf_first() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespaces =
        ["root", "middle", "leaf"].map(|name| NamespaceId::parse(name).expect("namespace"));
    let setup = context(1_000);
    let mut content_ids = Vec::new();
    let mut pool_id = None;
    for (index, namespace_id) in namespaces.iter().enumerate() {
        if index == 0 {
            let created = crate::test_support::NamespaceFixture::new(&store, namespace_id, &setup)
                .create()
                .await
                .expect("bootstrap root");
            pool_id = Some(created.pool_id);
        } else {
            let fixture =
                crate::test_support::NamespaceFixture::new(&store, &namespaces[index - 1], &setup);
            fixture
                .engine()
                .fork_namespace(
                    &crate::test_support::administrator(&fixture.engine()),
                    fixture
                        .engine()
                        .find_fork_target(namespace_id)
                        .await
                        .expect("find the fork target"),
                    &loonfs_test_support::test_actor(),
                    crate::test_support::fold_before_fork(&fixture.namespace)
                        .await
                        .expect("fold the WAL tail"),
                )
                .await
                .expect("fork");
        }
        content_ids.push(publish_inline(&store, namespace_id, index, &setup).await);
    }
    let pool_id = pool_id.expect("the pool of the chain");
    let middle_snapshot = read_fork_record(&store, &namespaces[0]).await;
    let leaf_snapshot = read_fork_record(&store, &namespaces[1]).await;
    for namespace_id in &namespaces[..2] {
        crate::test_support::NamespaceFixture::new(&store, namespace_id, &setup)
            .engine()
            .delete_namespace(
                &crate::test_support::administrator_of(&store, namespace_id),
                Default::default(),
            )
            .await
            .expect("delete ancestor");
    }
    let mut aged_now = 0;
    for namespace_id in &namespaces {
        aged_now = aged_now
            .max(now_after_newest_object(&store, namespace_id, 2 * GC_GRACE_WINDOW_MS + 1).await);
    }
    let aged = context(aged_now);
    for index in [0, 1, 1, 0] {
        let report = crate::test_support::namespace_store(&store, &namespaces[index])
            .gc(&aged.clock())
            .await
            .expect("collect ancestor while grandchild lives");
        assert_eq!(report.deleted_snapshots_by_owner.fork, 0);
        assert_eq!(
            collect_pool(&store, &pool_id, &aged)
                .await
                .deleted
                .content_objects,
            0
        );
        assert!(snapshot_record_exists(&store, &namespaces[0], &middle_snapshot.snapshot_id).await);
        assert!(snapshot_record_exists(&store, &namespaces[1], &leaf_snapshot.snapshot_id).await);
        assert_leaf_reads_every_members_file(&store, &namespaces).await;
    }

    crate::test_support::NamespaceFixture::new(&store, &namespaces[2], &setup)
        .engine()
        .delete_namespace(
            &crate::test_support::administrator_of(&store, &namespaces[2]),
            Default::default(),
        )
        .await
        .expect("delete leaf");
    // Deletion alone must not release either link in the protection chain.
    for index in [0, 1] {
        let report = crate::test_support::namespace_store(&store, &namespaces[index])
            .gc(&aged.clock())
            .await
            .expect("ancestors still kept by unretired descendants");
        assert_eq!(report.deleted_snapshots_by_owner.fork, 0);
    }
    let mut keys = Vec::new();
    for content_id in &content_ids {
        let chain = crate::test_support::span_object_keys(&store, &pool_id, content_id).await;
        assert!(!chain.is_empty(), "every member's file has a span object");
        keys.extend(chain);
    }
    for index in [2, 1, 0] {
        let report = crate::test_support::namespace_store(&store, &namespaces[index])
            .gc(&aged.clock())
            .await
            .expect("retire from leaf to root");
        if index > 0 {
            // A fork's retirement releases its two fork snapshots and
            // deletes no content.
            assert_eq!(report.deleted_snapshots_by_owner.fork, 2);
            for key in &keys {
                assert!(store.head(key).await.expect("pool object").is_some());
            }
            let snapshot = if index == 2 {
                &leaf_snapshot
            } else {
                &middle_snapshot
            };
            assert!(
                !snapshot_record_exists(&store, &namespaces[index - 1], &snapshot.snapshot_id)
                    .await
            );
        } else {
            // The namespace created with the pool retires last, and then the pool has no members,
            // so the pool's collection deletes the aged content.
            assert_eq!(report.deleted_snapshots_by_owner.fork, 0);
            let pool_report = collect_pool(&store, &pool_id, &aged).await;
            assert!(pool_report.deleted.content_objects >= keys.len() as u64);
            for key in &keys {
                assert!(store.head(key).await.expect("retired object").is_none());
            }
        }
    }
}
