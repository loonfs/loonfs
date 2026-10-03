//! Read admission at public runtime calls.

use super::ExecutionBudget;
use crate::metrics::{
    CounterHandle, DefaultMetricsRecorder, GaugeHandle, HistogramHandle, MetricsRecorder,
};
use crate::{
    InlineContentPolicy, ListOptions, LoonFs, Namespace, PageRequest, ReadFileStreamOptions,
    SnapshotPolicy, StatOptions, Writable,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, test_actor, writer_id};
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationClass, RecordingStore};
use loonfs_types::EffectiveLimit;
use std::future::Future;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::{tempdir, TempDir};

#[derive(Default)]
struct ReadRecorder {
    inner: DefaultMetricsRecorder,
    running: Arc<Mutex<Vec<i64>>>,
}

struct ReadGauge {
    inner: Arc<dyn GaugeHandle>,
    values: Arc<Mutex<Vec<i64>>>,
}

impl GaugeHandle for ReadGauge {
    fn set(&self, value: i64) {
        self.inner.set(value);
        self.values.lock().expect("gauge values").push(value);
    }
}

impl MetricsRecorder for ReadRecorder {
    fn register_counter(
        &self,
        name: &'static str,
        description: &'static str,
        labels: &[(&'static str, &'static str)],
    ) -> Arc<dyn CounterHandle> {
        self.inner.register_counter(name, description, labels)
    }

    fn register_gauge(
        &self,
        name: &'static str,
        description: &'static str,
        labels: &[(&'static str, &'static str)],
    ) -> Arc<dyn GaugeHandle> {
        let inner = self.inner.register_gauge(name, description, labels);
        if name == "loonfs.execution_budget.reads_running" {
            Arc::new(ReadGauge {
                inner,
                values: self.running.clone(),
            })
        } else {
            inner
        }
    }

    fn register_histogram(
        &self,
        name: &'static str,
        description: &'static str,
        labels: &[(&'static str, &'static str)],
        boundaries: &'static [f64],
    ) -> Arc<dyn HistogramHandle> {
        self.inner
            .register_histogram(name, description, labels, boundaries)
    }
}

struct Fixture {
    _root: TempDir,
    store: Arc<BlockingStore<RecordingStore<LocalFsStore>>>,
    runtime: LoonFs<Writable>,
    namespace: Namespace<Writable>,
    recorder: Arc<ReadRecorder>,
}

impl Fixture {
    async fn new(keys: KeyPredicate) -> Self {
        let root = tempdir().expect("tempdir");
        let store = Arc::new(BlockingStore::new(
            RecordingStore::new(
                LocalFsStore::new(root.path()).expect("store"),
                KeyPredicate::any(),
            ),
            keys,
            OperationClass::Read,
        ));
        let recorder = Arc::new(ReadRecorder::default());
        let budget = ExecutionBudget::builder()
            .max_concurrent_reads(NonZeroUsize::MIN)
            .metrics_recorder(recorder.clone())
            .build();
        let runtime = LoonFs::builder_with_store(store.clone())
            .writer_id("read-budget")
            .execution_budget(budget)
            .inline_content(InlineContentPolicy {
                inline_content_threshold_bytes: None,
                ..InlineContentPolicy::default()
            })
            .build()
            .await
            .expect("runtime");
        let namespace_id = namespace_id("reads");
        runtime
            .create_namespace(&namespace_id, &test_actor())
            .await
            .expect("namespace");
        let namespace = runtime.open_namespace(&namespace_id).expect("session");
        for name in ["a", "b", "c"] {
            namespace
                .put_file(&format!("/{name}"), b"body", &test_actor())
                .await
                .expect("file");
        }
        Self {
            _root: root,
            store,
            runtime,
            namespace,
            recorder,
        }
    }

    fn assert_counts(&self, running: usize, waiting: usize) {
        let stats = self.runtime.execution_budget().stats();
        assert_eq!(
            (stats.reads_running, stats.reads_waiting),
            (running, waiting)
        );
        assert_eq!(
            super::tests::gauge(
                &self.recorder.inner,
                "loonfs.execution_budget.reads_running"
            ),
            i64::try_from(running).expect("small count")
        );
        assert_eq!(
            super::tests::gauge(
                &self.recorder.inner,
                "loonfs.execution_budget.reads_waiting"
            ),
            i64::try_from(waiting).expect("small count")
        );
    }

    async fn one_read<T>(&self, read: impl Future<Output = crate::Result<T>>) -> T {
        self.recorder.running.lock().expect("gauge values").clear();
        let result = finish(read).await.expect("read");
        assert_eq!(
            *self.recorder.running.lock().expect("gauge values"),
            [0, 1, 0]
        );
        self.assert_counts(0, 0);
        result
    }
}

async fn finish<T>(work: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), work)
        .await
        .expect("operation completes")
}

fn page<C>(cursor: Option<C>) -> PageRequest<C> {
    PageRequest {
        limit: EffectiveLimit::new(NonZeroU32::MIN),
        cursor,
    }
}

#[tokio::test]
async fn lists_share_one_slot_across_writable_and_read_only_runtimes() {
    let fixture = Fixture::new(KeyPredicate::any()).await;
    let reader = LoonFs::builder_with_store(fixture.store.clone())
        .read_only()
        .execution_budget(fixture.runtime.execution_budget().clone())
        .build()
        .await
        .expect("reader");
    fixture.store.block_next();
    let first = tokio::spawn({
        let namespace = fixture.namespace.clone();
        async move { namespace.list("/").collect_up_to(10).await }
    });
    finish(fixture.store.wait_until_blocked()).await;
    fixture.assert_counts(1, 0);
    let before = fixture.store.inner().counts();
    let namespace = reader.namespace(fixture.namespace.id());
    let mut pager = namespace.list("/");
    let mut second = Box::pin(pager.collect_up_to(10));
    assert!(futures::poll!(second.as_mut()).is_pending());
    fixture.assert_counts(1, 1);
    assert_eq!(fixture.store.inner().counts(), before);
    fixture.store.release();
    let first = finish(first).await.expect("join list").expect("first list");
    let second = finish(second).await.expect("second list");
    assert_eq!(first.len(), 3);
    assert_eq!(first, second);
    fixture.assert_counts(0, 0);
    fixture.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn commits_folds_and_pin_folds_complete_while_a_read_holds_the_only_slot() {
    let fixture = Fixture::new(KeyPredicate::content_blob()).await;
    fixture.store.block_next();
    let held = tokio::spawn({
        let namespace = fixture.namespace.clone();
        async move { namespace.read_file("/a").await }
    });
    finish(fixture.store.wait_until_blocked()).await;
    fixture.assert_counts(1, 0);
    let maintenance = fixture
        .runtime
        .maintenance(writer_id("read-budget-maintenance"));
    finish(
        fixture
            .namespace
            .create_directory("/committed", &test_actor()),
    )
    .await
    .expect("commit");
    finish(maintenance.fold_wal(fixture.namespace.id()))
        .await
        .expect("fold");
    finish(
        fixture
            .namespace
            .create_directory("/checkpoint", &test_actor()),
    )
    .await
    .expect("commit before pin");
    finish(maintenance.create_checkpoint(fixture.namespace.id(), "pin"))
        .await
        .expect("pin fold");
    finish(
        fixture
            .namespace
            .create_directory("/snapshot", &test_actor()),
    )
    .await
    .expect("commit before snapshot");
    finish(
        fixture
            .namespace
            .create_snapshot("snapshot", u64::MAX, &SnapshotPolicy::default()),
    )
    .await
    .expect("snapshot fold");
    finish(maintenance.compact_metadata(fixture.namespace.id()))
        .await
        .expect("compaction");
    fixture.assert_counts(1, 0);
    fixture.store.release();
    assert_eq!(
        finish(held).await.expect("join read").expect("read").bytes,
        b"body"
    );
    fixture.assert_counts(0, 0);
    fixture.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn three_page_pagers_release_the_slot_between_pages() {
    let fixture = Fixture::new(KeyPredicate::any()).await;
    let view = fixture.one_read(fixture.namespace.read_view()).await;
    for mut pager in [fixture.namespace.list("/"), view.list("/")] {
        let mut cursor = None;
        for index in 0..3 {
            let response = fixture.one_read(pager.page(page(cursor))).await;
            assert_eq!(response.entries.len(), 1);
            cursor = response.next_cursor;
            assert_eq!(cursor.is_none(), index == 2);
        }
    }
    fixture.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn streams_release_the_slot_before_reading_body_bytes() {
    let fixture = Fixture::new(KeyPredicate::content_blob()).await;
    let view = fixture.one_read(fixture.namespace.read_view()).await;
    let entry = fixture.namespace.stat("/a").await.expect("entry");
    let revision = entry.revision_no().expect("file revision");
    let mut streams = vec![
        fixture
            .one_read(fixture.namespace.read_file_stream("/a"))
            .await,
        fixture
            .one_read(fixture.namespace.read_file_stream_with_options(
                "/a",
                &ReadFileStreamOptions {
                    start_offset: 1,
                    ..ReadFileStreamOptions::default()
                },
            ))
            .await,
        fixture
            .one_read(
                fixture
                    .namespace
                    .read_file_revision_stream_by_inode(entry.inode_id, revision),
            )
            .await,
        fixture.one_read(view.read_file_stream("/a")).await,
    ];
    streams[1]
        .fold_resumed_prefix(b"b")
        .expect("resumed prefix");
    for stream in &mut streams {
        fixture.store.block_next();
        let mut chunk = Box::pin(stream.next_chunk());
        assert!(futures::poll!(chunk.as_mut()).is_pending());
        finish(fixture.store.wait_until_blocked()).await;
        fixture.assert_counts(0, 0);
        fixture.one_read(fixture.namespace.stat("/b")).await;
        fixture.store.release();
        assert!(finish(chunk).await.expect("chunk").is_some());
    }
    fixture.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn dropping_waiting_and_running_reads_clears_the_counts() {
    let fixture = Fixture::new(KeyPredicate::any()).await;
    fixture.store.block_next();
    let held = tokio::spawn({
        let namespace = fixture.namespace.clone();
        async move { namespace.list("/").collect_up_to(10).await }
    });
    finish(fixture.store.wait_until_blocked()).await;
    let before = fixture.store.inner().counts();
    let mut waiting = Box::pin(fixture.namespace.stat("/a"));
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    fixture.assert_counts(1, 1);
    drop(waiting);
    fixture.assert_counts(1, 0);
    assert_eq!(fixture.store.inner().counts(), before);
    held.abort();
    assert!(finish(held)
        .await
        .expect_err("cancelled read")
        .is_cancelled());
    fixture.assert_counts(0, 0);
    fixture.store.release();
    fixture.one_read(fixture.namespace.stat("/a")).await;
    fixture.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn snapshot_delegation_takes_exactly_one_slot_per_stat_or_page() {
    let fixture = Fixture::new(KeyPredicate::any()).await;
    let snapshot = fixture
        .namespace
        .create_snapshot("snapshot", u64::MAX, &SnapshotPolicy::default())
        .await
        .expect("snapshot");
    let stat = StatOptions {
        snapshot_id: Some(snapshot.checkpoint_id.clone()),
        ..StatOptions::default()
    };
    let list = ListOptions {
        snapshot_id: stat.snapshot_id.clone(),
        ..ListOptions::default()
    };
    let root = fixture
        .one_read(fixture.namespace.stat_with_options("/", &stat))
        .await;
    fixture
        .one_read(
            fixture
                .namespace
                .stat_by_inode_with_options(root.inode_id, &stat),
        )
        .await;
    fixture
        .one_read(
            fixture
                .namespace
                .list_with_options("/", &list)
                .page(page(None)),
        )
        .await;
    fixture
        .one_read(
            fixture
                .namespace
                .list_by_inode_with_options(root.inode_id, &list)
                .page(page(None)),
        )
        .await;
    let view = fixture
        .one_read(
            fixture
                .namespace
                .read_view_at_snapshot(&snapshot.checkpoint_id),
        )
        .await;
    fixture.one_read(view.stat("/")).await;
    fixture.one_read(view.stat_by_inode(root.inode_id)).await;
    fixture
        .one_read(view.list_with_options("/", &list).page(page(None)))
        .await;
    fixture
        .one_read(
            view.list_by_inode_with_options(root.inode_id, &list)
                .page(page(None)),
        )
        .await;
    fixture.runtime.shutdown().await.expect("shutdown");
}
