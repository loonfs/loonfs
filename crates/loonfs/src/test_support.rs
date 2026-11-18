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
use std::sync::Arc;

/// A wall clock that reads the same time on every call.
#[derive(Debug)]
struct FixedWallClock {
    now_ms: u64,
}

impl WallClock for FixedWallClock {
    fn now_ms(&self) -> u64 {
        self.now_ms
    }
}

/// Builds a maintenance handle on `store` whose wall clock is a grace window
/// ahead.
///
/// The clock is [`GC_GRACE_WINDOW_MS`] ahead of the system
/// clock, so a retention advance through the handle can pass every commit
/// made before the call.
pub async fn maintenance_past_the_grace_window(store: SharedObjectStore) -> Maintenance {
    let clock = FixedWallClock {
        now_ms: SystemWallClock.now_ms() + GC_GRACE_WINDOW_MS,
    };
    let writer_id = test_writer_id("retention-maintenance");
    LoonFs::builder_with_store(store, crate::Context::Service)
        .writer_id(writer_id.clone())
        .wall_clock(Arc::new(clock))
        .build()
        .await
        .expect("build the retention maintenance handle")
        .maintenance(writer_id)
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
