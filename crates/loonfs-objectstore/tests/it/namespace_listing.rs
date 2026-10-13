//! What a namespace listing costs at the store.

use bytes::Bytes;
use loonfs_objectstore::keys::{hint, metadata_manifest_object};
use loonfs_objectstore::layout::list_namespace_ids;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::{namespace_id, page_limit};
use loonfs_test_support::stores::{KeyPredicate, RecordingStore, StoreCounts};
use loonfs_types::ManifestNo;

#[tokio::test]
async fn namespace_listing_costs_one_request_per_page() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::with_key_prefix(temp_dir.path(), Some("tenant-a")).expect("local store"),
        KeyPredicate::any(),
    );
    let namespace_ids: Vec<_> = (0..6)
        .map(|index| namespace_id(&format!("ns-{index}")))
        .collect();
    for namespace_id in &namespace_ids {
        for key in [
            hint(namespace_id),
            metadata_manifest_object(namespace_id, &ManifestNo(1)),
            metadata_manifest_object(namespace_id, &ManifestNo(2)),
        ] {
            store
                .put_overwrite(&key, Bytes::from_static(b"object"))
                .await
                .expect("write namespace object");
        }
    }

    for (page_size, pages) in [(3, 2), (4, 2), (6, 1), (1, 6)] {
        store.reset();
        let mut listed = Vec::new();
        let mut cursor = None;
        loop {
            let page = list_namespace_ids(&store, cursor.as_ref(), page_limit(page_size))
                .await
                .expect("list namespace ids");
            listed.extend(page.namespace_ids);
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        assert_eq!(listed, namespace_ids, "page size {page_size}");
        assert_eq!(
            store.counts(),
            StoreCounts {
                lists: pages,
                ..StoreCounts::default()
            },
            "page size {page_size}"
        );
    }
}
