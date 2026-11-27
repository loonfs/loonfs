//! Runtime handles that the test targets of this crate and of the crates
//! above it build the same way.
//!
//! It also holds readers of the metrics that those handles report.

#![allow(
    clippy::panic,
    reason = "a metric reader fails the test that calls it when the metric is missing"
)]

use crate::metrics::{MetricEntry, MetricValue, MetricsSnapshot};
use crate::{
    LoonFs, Maintenance, ReadOnly, SharedObjectStore, WallClock, Writable, WriterId,
    GC_GRACE_WINDOW_MS,
};
use loonfs_types::SystemWallClock;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A wall clock that reads the system clock plus an offset that a test sets,
/// so a test can date its writer's retention advance past the grace window
/// of every commit that it made.
#[derive(Debug, Default)]
pub struct OffsetWallClock {
    offset_ms: AtomicU64,
}

impl OffsetWallClock {
    /// Runs `work` with the clock [`GC_GRACE_WINDOW_MS`] ahead of the system
    /// clock, so a retention advance in `work` can pass every commit made
    /// before the call, and then sets the clock back to the system clock.
    pub async fn past_the_grace_window<T>(&self, work: impl Future<Output = T>) -> T {
        self.offset_ms.store(GC_GRACE_WINDOW_MS, Ordering::SeqCst);
        let output = work.await;
        self.offset_ms.store(0, Ordering::SeqCst);
        output
    }
}

impl WallClock for OffsetWallClock {
    fn now_ms(&self) -> u64 {
        SystemWallClock.now_ms() + self.offset_ms.load(Ordering::SeqCst)
    }
}

/// Builds a maintenance handle on `store` whose wall clock is a grace window
/// ahead of the system clock.
///
/// A collection through the handle can delete what every write made before
/// the call left behind. Collection runs in any process, so the handle
/// needs no writer session.
pub async fn maintenance_past_the_grace_window(store: SharedObjectStore) -> Maintenance {
    let clock = OffsetWallClock::default();
    clock.offset_ms.store(GC_GRACE_WINDOW_MS, Ordering::SeqCst);
    let writer_id = test_writer_id("retention-maintenance");
    LoonFs::builder_with_store(store, crate::Context::Service)
        .writer_id(writer_id)
        .wall_clock(Arc::new(clock))
        .build()
        .await
        .expect("build the collection handle")
        .maintenance()
}

/// Folds the WAL tail of `namespace_id` and advances its retention floor to
/// the folded head through a writer session of a new runtime on `store`.
///
/// The runtime's wall clock is a grace window ahead, so the advance can pass
/// every commit made before the call.
///
/// The session claims the namespace first, so a writer session of the
/// namespace in another runtime is fenced, as a server's is when another
/// server runs a maintenance job on its namespace. The runtime drains before
/// the call returns.
pub async fn advance_retention_floor_past_the_grace_window(
    store: SharedObjectStore,
    namespace_id: &crate::NamespaceId,
) -> crate::Result<crate::AdvanceRetentionResponse> {
    let (runtime, clock) = writer_with_offset_clock(store, "retention-advance").await;
    let namespace = runtime.open_namespace(namespace_id)?;
    let advanced = clock
        .past_the_grace_window(async {
            namespace.fold_wal().await?;
            namespace.advance_retention_floor().await
        })
        .await;
    drop(namespace);
    runtime.shutdown().await?;
    advanced
}

/// Builds a writer on `store` as `writer_id`, with no minimum interval
/// between its publications, whose wall clock is the returned clock.
pub async fn writer_with_offset_clock(
    store: SharedObjectStore,
    writer_id: &str,
) -> (LoonFs<Writable>, Arc<OffsetWallClock>) {
    let clock = Arc::new(OffsetWallClock::default());
    let writer = LoonFs::builder_with_store(store, crate::Context::Service)
        .writer_id(test_writer_id(writer_id))
        .wall_clock(Arc::clone(&clock) as Arc<dyn WallClock>)
        .config(crate::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..crate::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("build writer");
    (writer, clock)
}

/// Builds a writer on `store` as `writer_id`, with no minimum interval
/// between its publications.
pub async fn writer(store: SharedObjectStore, writer_id: &str) -> LoonFs<Writable> {
    LoonFs::builder_with_store(store, crate::Context::Service)
        .writer_id(test_writer_id(writer_id))
        .config(crate::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..crate::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("build writer")
}

/// Sets the root access row of `namespace_id` to `grants` with an
/// `update_access` commit in the service context.
///
/// An application sets a new namespace's first administrators this way.
/// `writer` acts in the service context.
pub async fn set_root_access(
    writer: &LoonFs<Writable>,
    namespace_id: &loonfs_types::NamespaceId,
    grants: loonfs_types::AccessGrants,
) {
    assert!(
        writer.context() == loonfs_types::Context::Service,
        "the root access row is set in the service context"
    );
    writer
        .open_namespace(namespace_id)
        .expect("open the namespace")
        .update_access(
            "/",
            &loonfs_types::ActorId::parse("test-actor").expect("the test actor id should parse"),
            loonfs_types::options::AccessState {
                boundary: false,
                grants,
            },
        )
        .await
        .expect("set the root access row");
}

/// Drops `writer`'s views of the namespace (its writer session's position and
/// its cached head) the way a change of unknown effect does, so the
/// runtime's next read of the namespace loads it from the store.
pub async fn drop_namespace_view(
    writer: &LoonFs<Writable>,
    namespace_id: &loonfs_types::NamespaceId,
) {
    writer
        .mode
        .publisher
        .invalidate_namespace(namespace_id, crate::publisher::NamespaceChange::Unknown)
        .await;
}

/// Builds a reader with nothing cached, so its first read loads the current
/// manifest.
pub async fn fresh_reader(store: SharedObjectStore) -> LoonFs<ReadOnly> {
    LoonFs::builder_with_store(store, crate::Context::Service)
        .read_only()
        .build()
        .await
        .expect("build a fresh reader")
}

fn test_writer_id(value: &str) -> WriterId {
    WriterId::parse(value).expect("a test writer id should parse")
}

/// Returns the counter `name` with exactly `labels` in `snapshot`.
pub fn counter(snapshot: &MetricsSnapshot, name: &str, labels: &[(&str, &str)]) -> u64 {
    match entry(snapshot, name, labels).value {
        MetricValue::Counter(value) => value,
        ref other => panic!("expected `{name}` to be a counter, found {other:?}"),
    }
}

/// Returns the sum of the counter `name` over all its label sets in
/// `snapshot`, or 0 if `name` is not registered.
pub fn counter_total(snapshot: &MetricsSnapshot, name: &str) -> u64 {
    snapshot
        .by_name(name)
        .map(|entry| match entry.value {
            MetricValue::Counter(value) => value,
            ref other => panic!("expected `{name}` to be a counter, found {other:?}"),
        })
        .sum()
}

/// Returns the gauge `name` with exactly `labels` in `snapshot`.
pub fn gauge(snapshot: &MetricsSnapshot, name: &str, labels: &[(&str, &str)]) -> i64 {
    match entry(snapshot, name, labels).value {
        MetricValue::Gauge(value) => value,
        ref other => panic!("expected `{name}` to be a gauge, found {other:?}"),
    }
}

/// Returns how many observations the histogram `name` holds over all its
/// label sets in `snapshot`, or 0 if `name` is not registered.
pub fn histogram_count(snapshot: &MetricsSnapshot, name: &str) -> u64 {
    snapshot
        .by_name(name)
        .map(|entry| match entry.value {
            MetricValue::Histogram { count, .. } => count,
            ref other => panic!("expected `{name}` to be a histogram, found {other:?}"),
        })
        .sum()
}

fn entry<'snapshot>(
    snapshot: &'snapshot MetricsSnapshot,
    name: &str,
    labels: &[(&str, &str)],
) -> &'snapshot MetricEntry {
    snapshot
        .all()
        .iter()
        .find(|entry| entry.name == name && entry.labels == labels)
        .unwrap_or_else(|| panic!("no `{name}` registered with labels {labels:?}"))
}
