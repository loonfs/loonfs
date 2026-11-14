//! The pool's member list, which a pool's collection lists to find the
//! pool's members, and the entries that the collection deletes.

use super::*;
use crate::test_support::{half_done_fork, write_member_entry};
use loonfs_objectstore::keys::{content_blob, namespace_prefix, pool_member, pool_prefix};

/// Checks that a collection of a pool with nothing under its prefix returns
/// `pool_not_found` and deletes nothing, and that a pool with one aged orphan
/// content object and no member entry is collected.
#[tokio::test]
async fn a_pool_with_nothing_under_its_prefix_is_not_found_and_nothing_is_deleted() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        MetadataMapStore::aged(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ),
        KeyPredicate::any(),
    );
    let pool_id = PoolId::generate();
    let error = crate::test_support::pool_store(&store, &pool_id)
        .gc(None, &context(GC_GRACE_WINDOW_MS + 1).clock())
        .await
        .expect_err("a pool with nothing under its prefix");
    assert_matches!(&error, CoreError::PoolNotFound { pool_id: missing } if *missing == pool_id);
    assert_eq!(error.code(), crate::error::ErrorCode::PoolNotFound);
    assert!(error.to_string().contains(pool_id.as_str()));
    assert_eq!(store.counts().deletes, 0);

    let orphan = content_blob(&pool_id, &loonfs_types::ContentId::generate());
    store
        .put_if_absent(&orphan, Bytes::from_static(b"orphan"))
        .await
        .expect("orphan");
    let report = collect_pool(&store, &pool_id, &context(GC_GRACE_WINDOW_MS + 1)).await;
    assert_eq!(report.deleted.content_objects, 1);
    assert!(store.head(&orphan).await.expect("orphan").is_none());
}

/// Checks that one collection deletes the member entry of a retired
/// namespace, of a namespace ID without a manifest, and of a namespace whose
/// manifest records another pool, and deletes nothing else.
#[tokio::test]
async fn the_entry_of_a_namespace_that_isnt_a_member_is_deleted() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let retired = NamespaceId::parse("retired").expect("namespace");
    let setup = context(1_000);
    let fixture = crate::test_support::NamespaceFixture::new(&store, &retired, &setup);
    let pool_id = fixture.create().await.expect("create").pool_id;
    fixture
        .engine()
        .delete_namespace(Default::default())
        .await
        .expect("delete");
    let deadline = collect(&store, &retired, &setup)
        .await
        .retires_at_ms
        .expect("a retirement deadline");
    collect(&store, &retired, &context(deadline)).await;
    assert!(store
        .head(&pool_member(&pool_id, &retired))
        .await
        .expect("head")
        .is_none());
    let absent = NamespaceId::parse("absent").expect("namespace");
    let elsewhere = NamespaceId::parse("elsewhere").expect("namespace");
    crate::test_support::NamespaceFixture::new(&store, &elsewhere, &setup)
        .create()
        .await
        .expect("create in another pool");
    for namespace_id in [&retired, &absent, &elsewhere] {
        write_member_entry(&store, &pool_id, namespace_id).await;
    }
    store.reset();
    let report = collect_pool(&store, &pool_id, &context(deadline)).await;
    assert_eq!(
        report,
        PoolGcResponse {
            listed_bytes: report.listed_bytes,
            ..PoolGcResponse::empty(pool_id.clone())
        }
    );
    assert_eq!(store.counts().deletes, 3);
    assert_eq!(store.counts().lists, NOT_A_MEMBER_LISTS);
    assert!(store
        .list_prefix(&pool_prefix(&pool_id))
        .await
        .expect("pool prefix")
        .is_empty());
}

/// The listings of the collection in
/// `the_entry_of_a_namespace_that_isnt_a_member_is_deleted`: the pool's
/// upload sessions, member list, content, and temporary objects, and the
/// manifest listings of the namespace without a manifest and of the namespace
/// in another pool. The retired namespace's boundary ends its lookup without
/// a listing.
const NOT_A_MEMBER_LISTS: usize = 6;

/// Checks that the pool's collection keeps the member entry of an
/// initializing member, reads nothing under the member's prefix beyond its
/// newest manifest, and deletes an unreferenced content object.
#[tokio::test]
async fn an_initializing_member_has_no_roots_and_keeps_its_entry() {
    let directory = tempdir().expect("directory");
    let local = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let source = NamespaceId::parse("source").expect("source");
    let target = NamespaceId::parse("target").expect("target");
    let (_, manifest_1) = half_done_fork(&local, &source, &target, &context(1_000)).await;
    write_member_entry(&local, &manifest_1.pool_id, &target).await;
    let unreferenced = content_blob(&manifest_1.pool_id, &loonfs_types::ContentId::generate());
    local
        .put_if_absent(&unreferenced, Bytes::from_static(b"unreferenced"))
        .await
        .expect("unreferenced content");
    let store = RecordingStore::new(
        MetadataMapStore::aged(Arc::clone(&local), KeyPredicate::content_blob()),
        KeyPredicate::prefix(namespace_prefix(&target)),
    );
    let report = collect_pool(
        &store,
        &manifest_1.pool_id,
        &context(1_000 + GC_GRACE_WINDOW_MS),
    )
    .await;
    assert_eq!(report.deleted.content_objects, 1);
    assert!(local.head(&unreferenced).await.expect("head").is_none());
    assert!(local
        .head(&pool_member(&manifest_1.pool_id, &target))
        .await
        .expect("head")
        .is_some());
    let operations = store.take();
    assert_eq!(operations.len(), 4, "{operations:?}");
    assert_eq!(
        crate::manifest::tests::control_requests(&target, &operations),
        [
            ("get", loonfs_objectstore::keys::boundary(&target)),
            ("list", metadata_manifest_prefix(&target)),
            (
                "get",
                loonfs_objectstore::keys::metadata_manifest_object(&target, &ManifestNo(1))
            ),
            ("get", loonfs_objectstore::keys::boundary(&target)),
        ]
    );
}
