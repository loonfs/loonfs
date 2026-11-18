//! Tests a query whose cached head includes a manifest that a compaction
//! replaced, after a collection removed that manifest's segments.

use crate::common::{grep_with, page_request, request, writable_namespace, GrepHost};
use loonfs::{LoonFs, NamespaceId, SharedObjectStore};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_types::GrepRequest;
use std::sync::Arc;

/// Checks that a query at the runtime's default consistency runs again from
/// a reloaded head and finds a file from the newest manifest. Before the
/// query, folds and a compaction replace the manifest that the reader's
/// cached head includes, and a collection removes that manifest's segments.
///
/// The reader's clock doesn't move, so neither the staleness bound nor the
/// revalidation bound passes, and the cached head stays confirmed.
#[tokio::test]
async fn a_query_at_a_collected_cached_head_reads_the_newest_manifest() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = NamespaceId::parse("grep-collected-head").expect("namespace");
    let (writer, namespace) = writable_namespace(&store, &namespace_id).await;
    let actor = loonfs_test_support::test_actor();
    namespace
        .put_file("/first.txt", b"needle one", &actor)
        .await
        .expect("commit the first file");
    writer.drain().await.expect("drain");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id(&format!(
        "{namespace_id}-writer"
    )));
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold the first file");
    let host = GrepHost::new(&store, "grep-collected-head-host").await;
    host.enable_grep_index(&namespace_id)
        .await
        .expect("enable grep");

    // The service reads through a reader whose first read is the first
    // query, so the reader's cached head includes grep's published entry.
    // The first query's pattern matches no file, so the query reads none of
    // the segments that the collection removes, and the reader caches none
    // of those segments.
    let reader = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .read_only()
        .monotonic_timer(Arc::new(ManualClock::new(0)))
        .build()
        .await
        .expect("build reader");
    let (service, reader, store, namespace_id) = (&host.service, &reader, &store, &namespace_id);
    let query = |pattern: &str| {
        let request = GrepRequest {
            consistency: None,
            ..request(pattern)
        };
        async move {
            grep_with(
                service,
                reader,
                store,
                namespace_id,
                &request,
                page_request(None, 10),
            )
            .await
        }
    };
    query("absent")
        .await
        .expect("the first query caches the head");
    let manifest_no = crate::common::control::metadata_manifest(store, namespace_id)
        .await
        .manifest()
        .manifest_no;

    // Two folds and a compaction publish manifests above the cached head's
    // manifest, and a collection that runs one grace window later deletes
    // the older manifests and the segments that the compaction rewrote.
    let mut head_seq = None;
    for (path, bytes) in [
        ("/second.txt", b"needle two"),
        ("/third.txt", b"needle six"),
    ] {
        let commit = namespace.put_file(path, bytes, &actor).await.expect(path);
        head_seq = Some(commit.committed_seq);
        writer.drain().await.expect("drain");
        maintenance
            .fold_wal(namespace_id)
            .await
            .expect("fold the WAL");
    }
    loonfs_core::test_support::compact_every_group(store.as_ref(), namespace_id).await;
    let gc = loonfs::test_support::maintenance_past_the_grace_window(store.clone())
        .await
        .gc(namespace_id)
        .await
        .expect("collect");
    assert!(gc.deleted.metadata_segments > 0, "{gc:?}");
    let successor = loonfs_objectstore::keys::metadata_manifest_object(
        namespace_id,
        &manifest_no.successor().expect("next manifest number"),
    );
    assert!(store.head(&successor).await.expect("head").is_none());

    let response = query("needle")
        .await
        .expect("the query runs again from a reloaded head");
    assert_eq!(Some(response.head_seq), head_seq);
    assert!(
        response
            .matches
            .iter()
            .any(|found| found.path.as_str() == "/third.txt"),
        "{response:?}"
    );
}
