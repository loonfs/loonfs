//! Store requests made by writer acquisition and the first publish after a fold.

use loonfs::{CreateDirectoryOptions, CreateNamespaceOptions, FsReader, FsWriter, NamespaceId};
use loonfs_objectstore::keys::{
    hint, metadata_manifest_object, metadata_manifest_prefix, wal_segment_prefix,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, OperationClass, RecordedOperation, RecordingStore,
};
use loonfs_test_support::test_actor;
use std::sync::Arc;

#[tokio::test]
async fn acquisition_shares_discovery_and_an_own_fold_needs_none() {
    let directory = tempfile::tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("discovery").expect("namespace");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let creator = FsWriter::builder_with_store(store.clone())
        .writer_id("discovery-test")
        .build()
        .await
        .expect("creator");
    creator
        .create_namespace(&namespace_id, CreateNamespaceOptions::new(test_actor()))
        .await
        .expect("namespace");
    creator.shutdown().await.expect("shutdown creator");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("discovery-test")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("fresh writer");
    store.reset();
    writer
        .create_directory(
            &namespace_id,
            "/first",
            CreateDirectoryOptions::new(test_actor()),
        )
        .await
        .expect("first publish");
    let requests = store.take();
    let hint_gets = requests
        .iter()
        .filter(|request| is_get(request, &hint(&namespace_id)))
        .count();
    assert_eq!(
        hint_gets, 2,
        "one discovery: initial hint and the required GC recheck; {requests:?}"
    );
    for number in 1..=3 {
        let key = metadata_manifest_object(&namespace_id, &loonfs::ManifestNo(number));
        assert!(
            requests
                .iter()
                .filter(|request| is_get(request, &key))
                .count()
                <= 1,
            "{requests:?}"
        );
    }
    assert_eq!(
        requests.len(),
        10,
        "one claim, one fence, and one semantic publish share discovery; {requests:?}"
    );

    for number in 1..31 {
        writer
            .create_directory(
                &namespace_id,
                &format!("/entry-{number}"),
                CreateDirectoryOptions::new(test_actor()),
            )
            .await
            .expect("reach fold threshold");
    }
    writer.wait_for_fold(&namespace_id).await.expect("fold");
    store.reset();
    writer
        .create_directory(
            &namespace_id,
            "/after-fold",
            CreateDirectoryOptions::new(test_actor()),
        )
        .await
        .expect("publish after fold");
    let requests = store.take();
    let before_put = requests
        .iter()
        .take_while(|request| {
            !(request
                .key()
                .starts_with(&wal_segment_prefix(&namespace_id))
                && matches!(request, RecordedOperation::Put { .. }))
        })
        .collect::<Vec<_>>();
    assert!(before_put.len() < requests.len(), "{requests:?}");
    assert_eq!(
        before_put
            .iter()
            .filter(|request| is_get(request, &hint(&namespace_id)))
            .count(),
        0,
        "{requests:?}"
    );
    assert!(before_put.iter().filter(|request| matches!(request, RecordedOperation::Get { key, .. } if key.contains("/manifests/"))).count() <= 1, "{requests:?}");
    assert!(!before_put.iter().any(|request| matches!(request, RecordedOperation::Head { key } if key.contains("/manifests/"))), "{requests:?}");
    assert!(
        !before_put.iter().any(|request| request
            .key()
            .starts_with(&wal_segment_prefix(&namespace_id))),
        "{requests:?}"
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_directory_created_during_a_fold_is_seen_after_the_projection_is_dropped() {
    let directory = tempfile::tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("dropped-projection").expect("namespace");
    let store = Arc::new(RecordingStore::new(
        BlockingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
            OperationClass::Put,
        ),
        KeyPredicate::any(),
    ));
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("discovery-test")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::new(test_actor()))
        .await
        .expect("namespace");
    writer
        .create_directory(
            &namespace_id,
            "/first",
            CreateDirectoryOptions::new(test_actor()),
        )
        .await
        .expect("first publish");
    store.inner().block_next();
    for number in 1..31 {
        writer
            .create_directory(
                &namespace_id,
                &format!("/entry-{number}"),
                CreateDirectoryOptions::new(test_actor()),
            )
            .await
            .expect("reach fold threshold");
    }
    store.inner().wait_until_blocked().await;
    store.reset();
    writer
        .create_directory(
            &namespace_id,
            "/during",
            CreateDirectoryOptions::new(test_actor()),
        )
        .await
        .expect("publish during fold");
    let during_wal = store
        .take()
        .into_iter()
        .find_map(|request| match request {
            RecordedOperation::Put { key, .. }
                if key.starts_with(&wal_segment_prefix(&namespace_id)) =>
            {
                Some(key)
            }
            _ => None,
        })
        .expect("WAL above the folded number");
    // Creating an existing namespace with allow_existing succeeds without a
    // manifest put, and finish_namespace_mutation drops the writer's projection.
    writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions {
                allow_existing: true,
                ..CreateNamespaceOptions::new(test_actor())
            },
        )
        .await
        .expect("drop projection during fold");
    store.inner().release();
    writer.wait_for_fold(&namespace_id).await.expect("fold");
    store.reset();

    writer
        .create_directory(
            &namespace_id,
            "/during/child",
            CreateDirectoryOptions::new(test_actor()),
        )
        .await
        .expect("publish depends on the commit made during the fold");
    let requests = store.take();
    let put = requests
        .iter()
        .position(|request| {
            matches!(request, RecordedOperation::Put { key, .. }
                if key.starts_with(&wal_segment_prefix(&namespace_id)))
        })
        .expect("child WAL put");
    let before_put = &requests[..put];
    assert!(
        !before_put.iter().any(|request| {
            is_get(request, &hint(&namespace_id))
                || (is_get(request, request.key())
                    && request
                        .key()
                        .starts_with(&metadata_manifest_prefix(&namespace_id)))
                || matches!(request, RecordedOperation::Head { .. })
        }),
        "{requests:?}"
    );
    assert!(
        before_put
            .iter()
            .any(|request| is_get(request, &during_wal)),
        "{requests:?}"
    );
    let during_wal_no = loonfs_objectstore::layout::wal_no_of(&during_wal).expect("WAL number");
    assert!(
        before_put
            .iter()
            .filter(|request| is_get(request, request.key()))
            .filter_map(|request| loonfs_objectstore::layout::wal_no_of(request.key()))
            .all(|number| number >= during_wal_no),
        "{requests:?}"
    );
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("fresh reader");
    reader
        .get_path_entry(&namespace_id, "/during/child", Default::default())
        .await
        .expect("durable child");
    writer.shutdown().await.expect("shutdown");
}

fn is_get(request: &RecordedOperation, expected_key: &str) -> bool {
    matches!(request, RecordedOperation::Get { key, .. } | RecordedOperation::GetWithMetadata { key, .. } if key == expected_key)
}
