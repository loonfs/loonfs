//! Listing the namespaces a store holds, including deleted ones.

use crate::common::commit_split_support::{bootstrap_namespace, mutation_context};
use crate::common::namespace_engine;
use bytes::Bytes;
use loonfs_objectstore::layout::{list_namespace_ids, NamespacePageCursor};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::{namespace_id, page_limit};
use tempfile::tempdir;

#[tokio::test]
async fn namespace_listing_returns_every_namespace_id_and_skips_other_children() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_ids = ["alpha", "beta", "gamma"].map(namespace_id);
    for namespace_id in &namespace_ids {
        bootstrap_namespace(&store, namespace_id, &context)
            .await
            .expect("create namespace");
    }
    let deleted = &namespace_ids[1];
    namespace_engine(&store, deleted, &context)
        .delete_namespace(Default::default())
        .await
        .expect("delete namespace");
    let mut aged = context.clone();
    aged.now_ms = u64::MAX / 2;
    for _ in 0..2 {
        loonfs_core::gc_namespace(&store, deleted, &loonfs_core::GcOptions::default(), &aged)
            .await
            .expect("collect the deleted namespace");
    }
    for stray in ["namespaces/Not-An-Id/hint.json", "namespaces/stray-object"] {
        store
            .put_overwrite(stray, Bytes::from_static(b"stray"))
            .await
            .expect("write a stray key");
    }

    let first = list_namespace_ids(&store, None, page_limit(1))
        .await
        .expect("first page");
    assert!(first.namespace_ids.is_empty());
    assert_eq!(first.skipped_entries, 1);
    let mut listed = Vec::new();
    let mut cursor = first.next_cursor;
    while let Some(start_after) = cursor {
        let page = list_namespace_ids(&store, Some(&start_after), page_limit(1))
            .await
            .expect("next page");
        assert_eq!(page.skipped_entries, 0);
        listed.extend(page.namespace_ids);
        cursor = page.next_cursor;
    }
    assert_eq!(listed, namespace_ids);

    let after_deleted = list_namespace_ids(
        &store,
        Some(&NamespacePageCursor::after(deleted)),
        page_limit(10),
    )
    .await
    .expect("page after the deleted namespace");
    assert_eq!(after_deleted.namespace_ids, namespace_ids[2..]);
    assert_eq!(after_deleted.next_cursor, None);
}
