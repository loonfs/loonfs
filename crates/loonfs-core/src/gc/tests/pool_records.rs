//! The pool record, which a pool's collection reads to find the pool's first
//! member, and deletes when the pool's create call failed or lost.

use super::*;
use loonfs_objectstore::keys::{content_blob, pool_prefix, pool_record};

#[tokio::test]
async fn a_pool_without_a_record_is_not_found_and_nothing_is_deleted() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        MetadataMapStore::aged(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ),
        KeyPredicate::any(),
    );
    let pool_id = PoolId::generate();
    let orphan = content_blob(&pool_id, &loonfs_types::ContentId::generate());
    store
        .put_if_absent(&orphan, Bytes::from_static(b"orphan"))
        .await
        .expect("orphan");
    store.reset();
    let error = crate::test_support::pool_store(&store, &pool_id)
        .gc(None, &context(GC_GRACE_WINDOW_MS + 1).clock())
        .await
        .expect_err("a pool without a record");
    assert_matches!(&error, CoreError::PoolNotFound { pool_id: missing } if *missing == pool_id);
    assert_eq!(error.code(), crate::error::ErrorCode::PoolNotFound);
    assert!(error.to_string().contains(pool_id.as_str()));
    assert_eq!(store.counts().deletes, 0);
    assert_eq!(store.counts().lists, 0);
    assert!(store.head(&orphan).await.expect("orphan").is_some());
}

/// Checks that the record of a pool without members, because the create call
/// failed or another create call for the namespace ID won, is kept inside its
/// creation grace and deleted after it, and that each collection lists only
/// the first member's manifests.
#[tokio::test]
async fn the_record_of_a_pool_without_members_is_deleted_after_its_creation_grace() {
    let created_at_ms = 1_000;
    for won_elsewhere in [false, true] {
        let directory = tempdir().expect("directory");
        let store = RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        );
        let namespace_id = NamespaceId::parse("lost-create").expect("namespace");
        if won_elsewhere {
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(1_000))
                .create()
                .await
                .expect("the winning create");
        }
        let pool_id = PoolId::generate();
        crate::test_support::write_pool_record(&store, &pool_id, &namespace_id, created_at_ms)
            .await;
        let key = pool_record(&pool_id);

        store.reset();
        let inside = collect_pool(
            &store,
            &pool_id,
            &context(created_at_ms + GC_GRACE_WINDOW_MS - 1),
        )
        .await;
        assert_eq!(
            inside.next_reclamation_at_ms,
            Some(created_at_ms + GC_GRACE_WINDOW_MS),
            "won elsewhere: {won_elsewhere}"
        );
        assert!(store.head(&key).await.expect("pool record").is_some());

        let after = collect_pool(
            &store,
            &pool_id,
            &context(created_at_ms + GC_GRACE_WINDOW_MS),
        )
        .await;
        assert_eq!(after, PoolGcResponse::empty(pool_id.clone()));
        assert!(store.head(&key).await.expect("pool record").is_none());
        assert_eq!(store.counts().lists, 2, "won elsewhere: {won_elsewhere}");
        assert_eq!(store.counts().deletes, 1);
        assert!(store
            .list_prefix(&pool_prefix(&pool_id))
            .await
            .expect("pool prefix")
            .is_empty());
    }
}
