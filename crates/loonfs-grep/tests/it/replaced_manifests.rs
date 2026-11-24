//! Tests a query whose cached head includes a manifest that a compaction or
//! a reorganization replaced, after a collection removed that manifest's
//! segments, and a query whose current manifest lists a missing segment.

use crate::common::{
    control, drive_worker_to_current, grep_with, page_request, request, writable_namespace,
    GrepHost,
};
use loonfs::{LoonFs, NamespaceId, SharedObjectStore};
use loonfs_grep::entry::grep_extension_name;
use loonfs_grep::{GramIndexBuildPolicy, GrepError};
use loonfs_objectstore::keys::extension_segment_object_key;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::nonzero_usize;
use loonfs_types::{ErrorCode, GrepRequest};
use std::sync::Arc;

/// Checks that an eventual query runs again from
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
            consistency: loonfs::Consistency::Eventual,
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

/// Returns the keys of the index segments that grep's entry in the
/// namespace's current manifest lists.
async fn current_index_segment_keys(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> Vec<String> {
    control::grep_entry(store, namespace_id)
        .await
        .expect("grep is enabled")
        .segments()
        .map(|segment| extension_segment_object_key(&grep_extension_name(), segment))
        .collect()
}

/// Checks that a query whose cached head lists a grep index segment that a
/// reorganization replaced, and that is then deleted, runs again from a
/// reloaded head and finds the files the newest manifest's index lists.
///
/// The test deletes the old segment from the store directly, as a namespace
/// collection does once the segment is older than the grace window. The
/// reader's clock doesn't move, so its cached head stays confirmed.
#[tokio::test]
async fn a_query_at_a_cached_head_whose_index_segment_was_deleted_reads_the_newest_manifest() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = NamespaceId::parse("grep-replaced-segment").expect("namespace");
    let (writer, namespace) = writable_namespace(&store, &namespace_id).await;
    let actor = loonfs_test_support::test_actor();
    namespace
        .put_file("/first.txt", b"needle one", &actor)
        .await
        .expect("commit the first file");
    writer.drain().await.expect("drain");
    let host = GrepHost::new(&store, "grep-replaced-segment-host").await;
    host.enable_grep_index(&namespace_id)
        .await
        .expect("enable grep");
    let replaced_keys = current_index_segment_keys(&store, &namespace_id).await;
    assert!(!replaced_keys.is_empty(), "the index lists a segment");

    // The reader's first read caches a head whose manifest lists the
    // segments above. The read is a stat, so no query caches their blocks.
    let reader = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .read_only()
        .monotonic_timer(Arc::new(ManualClock::new(0)))
        .build()
        .await
        .expect("build reader");
    reader
        .namespace(&namespace_id)
        .stat("/first.txt")
        .await
        .expect("the stat caches the head");

    // A second file adds a delta run, and a reorganization at one delta run
    // merges both runs into new segments, which a newer manifest lists.
    let commit = namespace
        .put_file("/second.txt", b"needle two", &actor)
        .await
        .expect("commit the second file");
    writer.drain().await.expect("drain");
    let policy = GramIndexBuildPolicy {
        max_delta_runs: nonzero_usize(1),
        ..GramIndexBuildPolicy::default()
    };
    drive_worker_to_current(&host.worker, &namespace_id, policy).await;
    let current_keys = current_index_segment_keys(&store, &namespace_id).await;
    for key in &replaced_keys {
        assert!(!current_keys.contains(key), "{key} was replaced");
        store
            .delete(key)
            .await
            .expect("delete the replaced segment");
    }

    let response = grep_with(
        &host.service,
        &reader,
        &store,
        &namespace_id,
        &GrepRequest {
            consistency: loonfs::Consistency::Eventual,
            ..request("needle")
        },
        page_request(None, 10),
    )
    .await
    .expect("the query runs again from a reloaded head");
    assert_eq!(response.head_seq, commit.committed_seq);
    let mut paths: Vec<&str> = response
        .matches
        .iter()
        .map(|found| found.path.as_str())
        .collect();
    paths.sort_unstable();
    assert_eq!(paths, ["/first.txt", "/second.txt"], "{response:?}");
}

/// Checks that a query whose current manifest lists a missing grep index
/// segment returns `index_corrupt`, because no newer manifest replaced the
/// manifest that lists the segment.
#[tokio::test]
async fn a_query_whose_current_manifest_lists_a_missing_index_segment_is_corrupt() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = NamespaceId::parse("grep-missing-segment").expect("namespace");
    let (writer, namespace) = writable_namespace(&store, &namespace_id).await;
    namespace
        .put_file(
            "/first.txt",
            b"needle one",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("commit the file");
    writer.drain().await.expect("drain");
    let host = GrepHost::new(&store, "grep-missing-segment-host").await;
    host.enable_grep_index(&namespace_id)
        .await
        .expect("enable grep");
    let keys = current_index_segment_keys(&store, &namespace_id).await;
    assert!(!keys.is_empty(), "the index lists a segment");
    for key in &keys {
        store.delete(key).await.expect("delete the segment");
    }

    let error = host
        .grep(&namespace_id, &request("needle"), page_request(None, 10))
        .await
        .expect_err("the manifest lists a missing segment");
    assert_eq!(error.code(), ErrorCode::IndexCorrupt);
    let GrepError::IndexSegmentMissing { object_key } = &error else {
        panic!("expected a missing index segment, got {error:?}");
    };
    assert!(
        keys.contains(object_key),
        "the error names a deleted segment: {object_key}"
    );
}
