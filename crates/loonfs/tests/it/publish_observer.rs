//! Publish-observer delivery: what a registered observer is handed, and
//! which calls do not reach it.

use loonfs::{CreateNamespaceOptions, FsWriter, PutFileOptions, SharedObjectStore};
use loonfs_api::{ChangeSeq, NamespaceId};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

#[tokio::test]
async fn registered_observer_sees_namespace_and_sequence_after_publish() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let observer_values = observed.clone();
    let writer = FsWriter::builder_with_store(store)
        .writer_id("observer-writer")
        .min_publish_interval_ms(0)
        .publish_observer(move |namespace_id, committed_seq| {
            observer_values
                .lock()
                .expect("observer values lock poisoned")
                .push((namespace_id.clone(), committed_seq));
        })
        .build()
        .await
        .expect("writer");
    let namespace_id = NamespaceId::parse("observer").expect("namespace id");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    assert!(
        observed
            .lock()
            .expect("observer values lock poisoned")
            .is_empty(),
        "namespace bootstrap is not a committed mutation publication"
    );

    let response = writer
        .put_file_bytes(
            &namespace_id,
            "/note.txt",
            b"observer needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("publish file");
    assert_eq!(response.committed_seq, ChangeSeq(1));
    assert_eq!(
        *observed.lock().expect("observer values lock poisoned"),
        vec![(namespace_id, ChangeSeq(1))]
    );
    writer.shutdown().await.expect("shutdown");
}
