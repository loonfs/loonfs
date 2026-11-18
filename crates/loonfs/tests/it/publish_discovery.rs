//! Store requests made by writer acquisition and the first publish after a fold.

use loonfs::{LoonFs, ManifestNo, NamespaceId};
use loonfs_core::limits::FOLD_AT_WAL_OBJECTS;
use loonfs_objectstore::keys::{
    boundary, metadata_manifest_object, metadata_manifest_prefix, wal_prefix,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, OperationSelector, RecordedOperation, RecordingStore,
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
    let creator = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("discovery-test"))
        .build()
        .await
        .expect("creator");
    creator
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("namespace");
    creator.shutdown().await.expect("shutdown creator");
    // The writer is a new runtime without a cached head of the namespace, so
    // its claim searches for the newest manifest first. The writer's timer
    // doesn't move, so no check for a newer manifest starts while the test
    // counts requests.
    let writer = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("discovery-test"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .monotonic_timer(Arc::new(ManualClock::new(0)))
        .build()
        .await
        .expect("fresh writer");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    store.reset();
    namespace
        .create_directory("/first", &test_actor())
        .await
        .expect("first publish");
    writer.drain().await.expect("drain");
    let requests = store.take();
    // The commit's WAL put is the last request.
    assert!(
        requests
            .last()
            .is_some_and(|request| request.key().starts_with(&wal_prefix(&namespace_id))),
        "{requests:?}"
    );
    let boundary_gets = requests
        .iter()
        .filter(|request| is_get(request, &boundary(&namespace_id)))
        .count();
    assert_eq!(
        boundary_gets, 4,
        "one search for the newest manifest reads the manifest boundary twice, the \
         claim's publication reads it once after its put, and the check of the claim once \
         more; {requests:?}"
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
        11,
        "one claim, one fence, and one semantic publish share discovery; {requests:?}"
    );

    for number in 1..31 {
        namespace
            .create_directory(&format!("/entry-{number}"), &test_actor())
            .await
            .expect("reach fold threshold");
    }
    namespace.wait_for_fold().await.expect("fold");
    writer.drain().await.expect("the fold's compaction settles");
    store.reset();
    namespace
        .create_directory("/after-fold", &test_actor())
        .await
        .expect("publish after fold");
    let requests = store.take();
    let before_put = requests
        .iter()
        .take_while(|request| {
            !(request.key().starts_with(&wal_prefix(&namespace_id))
                && matches!(request, RecordedOperation::Put { .. }))
        })
        .collect::<Vec<_>>();
    assert!(before_put.len() < requests.len(), "{requests:?}");
    assert_eq!(
        before_put
            .iter()
            .filter(|request| is_get(request, &boundary(&namespace_id)))
            .count(),
        0,
        "{requests:?}"
    );
    assert!(before_put.iter().filter(|request| matches!(request, RecordedOperation::Get { key, .. } if key.contains("/manifests/"))).count() <= 1, "{requests:?}");
    assert!(!before_put.iter().any(|request| matches!(request, RecordedOperation::Head { key } if key.contains("/manifests/"))), "{requests:?}");
    assert!(
        !before_put
            .iter()
            .any(|request| request.key().starts_with(&wal_prefix(&namespace_id))),
        "{requests:?}"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that the first commit after a create in the same runtime builds
/// its claim on the manifest 2 that the create published, so the commit
/// sends seven requests and no search for the newest manifest.
#[tokio::test]
async fn the_first_commit_after_a_create_claims_on_the_created_manifest() {
    let directory = tempfile::tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("created").expect("namespace");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let writer = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("discovery-test"))
        .monotonic_timer(Arc::new(ManualClock::new(0)))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    store.reset();
    namespace
        .create_directory("/first", &test_actor())
        .await
        .expect("first publish");
    writer.drain().await.expect("drain");
    let requests = store.take();
    assert_eq!(
        requests.iter().map(kind_and_key).collect::<Vec<_>>(),
        first_commit_requests(&namespace_id, ManifestNo(3)),
        "{requests:?}"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that the first read after a create in the same runtime starts from
/// the manifest 2 that the create installed as the namespace's cached head,
/// with no store request: the head's checks are younger than the read's
/// maximum staleness, its WAL tail is empty, and the create wrote the
/// manifest through the segment cache.
#[tokio::test]
async fn the_first_read_after_a_create_sends_no_request() {
    let directory = tempfile::tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("created").expect("namespace");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let writer = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("discovery-test"))
        .monotonic_timer(Arc::new(ManualClock::new(0)))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    store.reset();
    namespace.stat("/").await.expect("stat the root");
    let requests = store.take();
    assert_eq!(
        requests.iter().map(kind_and_key).collect::<Vec<_>>(),
        Vec::<(&str, String)>::new(),
        "{requests:?}"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that the first commit on a fork's new namespace in the runtime
/// that forked it builds its claim on the fork's active manifest 3, with the
/// same seven requests as the first commit after a create. The fork folded
/// the source's WAL tail into its manifest 2, and the commit's planning reads
/// the segments of that fold from the segment cache, because the fork wrote
/// them in this runtime.
#[tokio::test]
async fn the_first_commit_after_a_fork_claims_on_the_forked_manifest() {
    let directory = tempfile::tempdir().expect("directory");
    let source_id = NamespaceId::parse("source").expect("source");
    let fork_id = NamespaceId::parse("fork").expect("fork");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let writer = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("discovery-test"))
        .monotonic_timer(Arc::new(ManualClock::new(0)))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&source_id, &test_actor())
        .await
        .expect("source");
    writer
        .open_namespace(&source_id)
        .expect("open source")
        .create_directory("/inherited", &test_actor())
        .await
        .expect("source commit");
    writer
        .fork_namespace(&source_id, &fork_id, &test_actor())
        .await
        .expect("fork");
    let fork = writer.open_namespace(&fork_id).expect("open fork");
    store.reset();
    fork.create_directory("/inherited/child", &test_actor())
        .await
        .expect("first publish");
    writer.drain().await.expect("drain");
    let requests = store.take();
    assert_eq!(
        requests.iter().map(kind_and_key).collect::<Vec<_>>(),
        first_commit_requests(&fork_id, ManifestNo(4)),
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
            OperationSelector::Put,
        ),
        KeyPredicate::any(),
    ));
    let writer = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("discovery-test"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .monotonic_timer(Arc::new(ManualClock::new(0)))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer
        .create_directory("/first", &test_actor())
        .await
        .expect("first publish");
    store.inner().block_next();
    // The claim's fence and the first directory are two of the tail's WAL
    // objects, so the last of these directories reaches the fold trigger.
    for number in 1..FOLD_AT_WAL_OBJECTS.get() - 1 {
        namespace_writer
            .create_directory(&format!("/entry-{number}"), &test_actor())
            .await
            .expect("reach fold threshold");
    }
    store.inner().wait_until_blocked().await;
    store.reset();
    namespace_writer
        .create_directory("/during", &test_actor())
        .await
        .expect("publish during fold");
    let during_wal = store
        .take()
        .into_iter()
        .find_map(|request| match request {
            RecordedOperation::Put { key, .. } if key.starts_with(&wal_prefix(&namespace_id)) => {
                Some(key)
            }
            _ => None,
        })
        .expect("WAL above the folded number");
    // Dropping the writer's views drops the session's projection while the
    // fold runs.
    loonfs::test_support::drop_namespace_view(&writer, &namespace_id).await;
    store.inner().release();
    namespace_writer.wait_for_fold().await.expect("fold");
    writer.drain().await.expect("the fold's compaction settles");
    store.reset();

    namespace_writer
        .create_directory("/during/child", &test_actor())
        .await
        .expect("publish depends on the commit made during the fold");
    let requests = store.take();
    let put = requests
        .iter()
        .position(|request| {
            matches!(request, RecordedOperation::Put { key, .. }
                if key.starts_with(&wal_prefix(&namespace_id)))
        })
        .expect("child WAL put");
    let before_put = &requests[..put];
    assert_eq!(
        before_put
            .iter()
            .filter(|request| is_get(request, &boundary(&namespace_id)))
            .count(),
        3,
        "the search for the newest manifest reads the manifest boundary twice, and the \
         check of the loaded manifest once more; {requests:?}"
    );
    assert_eq!(
        before_put
            .iter()
            .filter(|request| is_get(request, request.key())
                && request
                    .key()
                    .starts_with(&metadata_manifest_prefix(&namespace_id)))
            .count(),
        0,
        "the fold's publication cached the fold's manifest; {requests:?}"
    );
    assert_eq!(
        before_put
            .iter()
            .filter(|request| matches!(request, RecordedOperation::Head { .. }))
            .count(),
        1,
        "one successor check; {requests:?}"
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
    let reader = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .read_only()
        .build()
        .await
        .expect("fresh reader");
    let namespace = reader.namespace(&namespace_id);
    namespace
        .stat("/during/child")
        .await
        .expect("durable child");
    writer.shutdown().await.expect("shutdown");
}

/// Returns the requests of a first commit whose claim builds on manifest 2
/// of `namespace_id` without a request first: the claim's put and the
/// boundary read after the put, a WAL read for the tip, the fence, the
/// successor check and boundary read after the fence, and the commit.
fn first_commit_requests(
    namespace_id: &NamespaceId,
    claim_no: ManifestNo,
) -> Vec<(&'static str, String)> {
    vec![
        ("put", metadata_manifest_object(namespace_id, &claim_no)),
        ("get", boundary(namespace_id)),
        ("get", "wal 1".to_owned()),
        ("put", "wal 1".to_owned()),
        (
            "head",
            metadata_manifest_object(namespace_id, &ManifestNo(claim_no.0 + 1)),
        ),
        ("get", boundary(namespace_id)),
        ("put", "wal 2".to_owned()),
    ]
}

/// Returns the request's kind and key, with a conditional read counted as a
/// read and a WAL object's key written as `wal N`.
fn kind_and_key(request: &RecordedOperation) -> (&'static str, String) {
    let kind = match request {
        RecordedOperation::Head { .. } => "head",
        RecordedOperation::Get { .. } | RecordedOperation::GetWithMetadata { .. } => "get",
        RecordedOperation::Put { .. } | RecordedOperation::PutStreamed { .. } => "put",
        RecordedOperation::List { .. } => "list",
        _ => "other",
    };
    let key = match loonfs_objectstore::layout::wal_no_of(request.key()) {
        Some(wal_no) => format!("wal {}", wal_no.0),
        None => request.key().to_owned(),
    };
    (kind, key)
}

fn is_get(request: &RecordedOperation, expected_key: &str) -> bool {
    matches!(request, RecordedOperation::Get { key, .. } | RecordedOperation::GetWithMetadata { key, .. } if key == expected_key)
}
