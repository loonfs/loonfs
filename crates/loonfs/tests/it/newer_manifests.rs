//! Reads in a runtime with nothing cached while another process publishes
//! manifests of the namespace faster than one discovery reads its WAL tail.

use loonfs::test_support::writer;
use loonfs::SharedObjectStore;
use loonfs_objectstore::keys::wal_prefix;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    KeyPredicate, MemoryStore, NewerManifestStore, OperationSelector, RecordingStore,
};
use loonfs_types::ChecksumAlgorithm;
use std::collections::BTreeSet;
use std::sync::Arc;

/// Checks that a strong read in a runtime with no cached view succeeds when
/// another process publishes a manifest before every GET of the read's
/// discovery, which finds a newer manifest after every WAL read, and that
/// the discovery reads each WAL number once.
#[tokio::test]
async fn a_read_succeeds_when_every_wal_get_is_followed_by_a_newer_manifest() {
    let namespace_id = namespace_id("busy");
    let objects = Arc::new(MemoryStore::new(ChecksumAlgorithm::Crc64nvme));
    let publishing = Arc::new(NewerManifestStore::new(
        objects.clone(),
        objects.clone(),
        namespace_id.clone(),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
        OperationSelector::Get,
    ));
    let recording = Arc::new(RecordingStore::new(publishing.clone(), KeyPredicate::any()));
    let store: SharedObjectStore = objects.clone();
    let writer = writer(store, "busy").await;
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    for index in 0..10 {
        namespace
            .put_file(&format!("/file-{index}.txt"), b"file", &actor)
            .await
            .expect("commit a file");
    }
    writer.shutdown().await.expect("writer shutdown");

    let store: SharedObjectStore = recording.clone();
    let reader = loonfs::LoonFs::builder_with_store(store, loonfs::Context::Service)
        .read_only()
        .build()
        .await
        .expect("reader")
        .namespace(&namespace_id);
    publishing.arm();
    let file = reader
        .strong()
        .read_file("/file-9.txt")
        .await
        .expect("a read of a busy namespace");
    assert_eq!(file.bytes, b"file");

    let wal_gets = recording
        .take_get_keys()
        .into_iter()
        .filter(|key| key.starts_with(&wal_prefix(&namespace_id)))
        .collect::<Vec<_>>();
    let distinct = wal_gets.iter().collect::<BTreeSet<_>>();
    assert_eq!(distinct.len(), wal_gets.len(), "{wal_gets:?}");
    assert_eq!(publishing.published(), wal_gets.len());
    assert!(
        publishing.published() > loonfs_core::limits::MAX_CONTENTION_ATTEMPTS,
        "more newer manifests than a discovery has passes, {}",
        publishing.published()
    );
}
