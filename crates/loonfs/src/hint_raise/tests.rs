//! Store request contracts for WAL freshness and periodic hint raises.

use crate::{CreateDirectoryOptions, CreateNamespaceOptions, FsReader, FsWriter, NamespaceId};
use loonfs_api::wire::control::{decode_control_object, ControlObjectKind, HintState};
use loonfs_api::{MonotonicTimer, WalNo};
use loonfs_core::limits::HINT_RAISE_SEGMENTS;
use loonfs_objectstore::{keys, local_fs_store::LocalFsStore, ObjectStore};
use loonfs_test_support::stores::{KeyPredicate, OperationClass, RecordingStore};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Default)]
struct ManualTimer(AtomicU64);

impl ManualTimer {
    fn set(&self, now_ms: u64) {
        self.0.store(now_ms, Ordering::SeqCst);
    }
}

impl MonotonicTimer for ManualTimer {
    fn monotonic_now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

async fn directory(writer: &FsWriter, namespace_id: &NamespaceId, index: u64) {
    writer
        .create_directory(
            namespace_id,
            &format!("/directory-{index}"),
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create directory");
}

async fn hint(store: &dyn ObjectStore, namespace_id: &NamespaceId) -> HintState {
    let bytes = store
        .get(&keys::hint(namespace_id), None)
        .await
        .expect("get hint")
        .expect("hint");
    decode_control_object::<HintState>(&bytes, ControlObjectKind::Hint)
        .expect("decode hint")
        .payload()
        .clone()
}

#[tokio::test]
async fn batches_write_only_the_wal_until_the_threshold_raise() {
    let directory_path = tempfile::tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("threshold").expect("namespace");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory_path.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let timer = Arc::new(ManualTimer::default());
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("writer")
        .min_publish_interval_ms(0)
        .monotonic_timer(timer.clone())
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create");
    // The first batch acquires the writer: a manifest with the new epoch,
    // whose publication raises the hint's manifest number, and a fence at
    // WAL number 1. Batch k then lands at number k + 1.
    directory(&writer, &namespace_id, 0).await;
    let initial = hint(store.as_ref(), &namespace_id).await;
    store.reset();
    for index in 1..HINT_RAISE_SEGMENTS - 2 {
        directory(&writer, &namespace_id, index).await;
    }
    assert_eq!(store.counts().compare_and_swaps, 0);
    assert_eq!(hint(store.as_ref(), &namespace_id).await, initial);
    directory(&writer, &namespace_id, HINT_RAISE_SEGMENTS - 2).await;
    assert_eq!(store.counts().compare_and_swaps, 1);
    assert_eq!(
        hint(store.as_ref(), &namespace_id).await.wal_no,
        WalNo(HINT_RAISE_SEGMENTS)
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn the_interval_raises_a_short_tail() {
    let directory_path = tempfile::tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("interval").expect("namespace");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory_path.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let timer = Arc::new(ManualTimer::default());
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("writer")
        .min_publish_interval_ms(0)
        .monotonic_timer(timer.clone())
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create");
    directory(&writer, &namespace_id, 0).await;
    let initial = hint(store.as_ref(), &namespace_id).await;
    timer.set(999);
    directory(&writer, &namespace_id, 1).await;
    assert_eq!(hint(store.as_ref(), &namespace_id).await, initial);
    store.reset();
    timer.set(1000);
    directory(&writer, &namespace_id, 2).await;
    assert_eq!(store.counts().compare_and_swaps, 1);
    assert_eq!(hint(store.as_ref(), &namespace_id).await.wal_no, WalNo(4));
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_failed_raise_keeps_the_batch_and_retries_at_the_next_trigger() {
    use loonfs_test_support::stores::{FailStore, InjectedError};
    let directory_path = tempfile::tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("retry").expect("namespace");
    let store = Arc::new(RecordingStore::new(
        FailStore::new(
            LocalFsStore::new(directory_path.path()).expect("store"),
            KeyPredicate::hint(&namespace_id),
            OperationClass::CompareAndSwap,
            InjectedError::Transport("hint raise failed".to_owned()),
        ),
        KeyPredicate::any(),
    ));
    let timer = Arc::new(ManualTimer::default());
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("writer")
        .min_publish_interval_ms(0)
        .monotonic_timer(timer.clone())
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create");
    directory(&writer, &namespace_id, 0).await;
    let initial = hint(store.as_ref(), &namespace_id).await;
    store.inner().fail_next(1);
    timer.set(1000);
    directory(&writer, &namespace_id, 1).await;
    assert_eq!(store.inner().attempts(), 1);
    assert_eq!(hint(store.as_ref(), &namespace_id).await, initial);
    store.reset();
    timer.set(2000);
    directory(&writer, &namespace_id, 2).await;
    assert_eq!(store.counts().compare_and_swaps, 1);
    assert_eq!(hint(store.as_ref(), &namespace_id).await.wal_no, WalNo(4));
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn readers_probe_commits_immediately_and_check_deletion_on_the_interval() {
    let directory_path = tempfile::tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("freshness").expect("namespace");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory_path.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let timer = Arc::new(ManualTimer::default());
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("writer")
        .min_publish_interval_ms(0)
        .monotonic_timer(Arc::new(ManualTimer::default()))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create");
    directory(&writer, &namespace_id, 0).await;
    let reader = FsReader::builder_with_store(store.clone())
        .monotonic_timer(timer.clone())
        .build()
        .await
        .expect("reader");
    reader
        .get_path_entry(&namespace_id, "/directory-0", Default::default())
        .await
        .expect("warm read");
    let initial = hint(store.as_ref(), &namespace_id).await;
    directory(&writer, &namespace_id, 1).await;
    assert_eq!(hint(store.as_ref(), &namespace_id).await, initial);
    store.reset();
    reader
        .get_path_entry(&namespace_id, "/directory-1", Default::default())
        .await
        .expect("observe WAL commit");
    assert_eq!(store.counts().gets, 2);
    assert_eq!(store.counts().heads, 0);
    timer.set(999);
    store.reset();
    reader
        .get_path_entry(&namespace_id, "/directory-1", Default::default())
        .await
        .expect("warm read");
    assert_eq!(store.take().len(), 1);
    timer.set(1000);
    reader
        .get_path_entry(&namespace_id, "/directory-1", Default::default())
        .await
        .expect("check hint");
    assert_eq!(store.counts().gets, 1);
    assert_eq!(store.counts().heads, 1);
    timer.set(2000);
    store.reset();
    let (first, second) = tokio::join!(
        reader.get_path_entry(&namespace_id, "/directory-1", Default::default()),
        reader.get_path_entry(&namespace_id, "/directory-1", Default::default()),
    );
    first.expect("first concurrent read");
    second.expect("second concurrent read");
    assert_eq!(store.counts().gets, 2);
    assert_eq!(store.counts().heads, 1);
    let always_check = FsReader::builder_with_store(store.clone())
        .runtime_cache(crate::RuntimeCacheConfig {
            control_revalidation_interval_ms: 0,
            ..Default::default()
        })
        .build()
        .await
        .expect("reader without an interval");
    always_check
        .get_path_entry(&namespace_id, "/directory-1", Default::default())
        .await
        .expect("warm read");
    store.reset();
    for _ in 0..2 {
        always_check
            .get_path_entry(&namespace_id, "/directory-1", Default::default())
            .await
            .expect("check every read");
    }
    assert_eq!(store.counts().gets, 2);
    assert_eq!(store.counts().heads, 2);
    writer
        .delete_namespace(&namespace_id, Default::default())
        .await
        .expect("delete");
    timer.set(2999);
    reader
        .get_path_entry(&namespace_id, "/directory-1", Default::default())
        .await
        .expect("cached before interval");
    timer.set(3000);
    let error = reader
        .get_path_entry(&namespace_id, "/directory-1", Default::default())
        .await
        .expect_err("observe deletion");
    assert_eq!(error.code(), crate::ErrorCode::NamespaceDeleted);
    writer.shutdown().await.expect("shutdown");
}
