//! What a writer built with a metrics recorder actually reports (the
//! object-store bridge and the publication instruments), and the head cache
//! lookups that reads record in the metadata cache's stats.

use crate::common::*;
use loonfs::metrics::{DefaultMetricsRecorder, MetricValue, MetricsSnapshot};
use loonfs::{LoonFs, MaintainMetadataOptions};
use loonfs_test_support::block_on::block_on;
use loonfs_test_support::ids::namespace_id;
use std::sync::Arc;
use tempfile::tempdir;

fn counter(snapshot: &MetricsSnapshot, name: &str, labels: &[(&str, &str)]) -> u64 {
    let entry = snapshot
        .by_name(name)
        .find(|entry| entry.labels == labels)
        .unwrap_or_else(|| panic!("no `{name}` registered with labels {labels:?}"));
    match entry.value {
        MetricValue::Counter(value) => value,
        ref other => panic!("expected a counter, found {other:?}"),
    }
}

fn gauge(snapshot: &MetricsSnapshot, name: &str, labels: &[(&str, &str)]) -> i64 {
    let entry = snapshot
        .by_name(name)
        .find(|entry| entry.labels == labels)
        .unwrap_or_else(|| panic!("no `{name}` registered with labels {labels:?}"));
    match entry.value {
        MetricValue::Gauge(value) => value,
        ref other => panic!("expected a gauge, found {other:?}"),
    }
}

fn histogram_count(snapshot: &MetricsSnapshot, name: &str) -> u64 {
    snapshot
        .by_name(name)
        .map(|entry| match entry.value {
            MetricValue::Histogram { count, .. } => count,
            ref other => panic!("expected a histogram, found {other:?}"),
        })
        .sum()
}

#[test]
fn a_writer_with_a_recorder_reports_stores_and_publications() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let namespace_id = namespace_id("demo");
    // Enough writes to push the WAL tail past its threshold, so the writer
    // folds it.
    let writes = MaintainMetadataOptions::default()
        .max_wal_tail_objects
        .get()
        + 1;
    let snapshot = block_on(async {
        let fs = open_runtime_with_async(store(temp_dir.path()), "metrics-writer", |builder| {
            builder.metrics_recorder(recorder.clone())
        })
        .await;
        fs.writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace = fs
            .writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        for file in 0..writes {
            namespace
                .put_file(
                    &format!("/docs/file-{file}.txt"),
                    b"body",
                    &loonfs_test_support::test_actor(),
                )
                .await
                .expect("put file");
        }
        namespace
            .wait_for_fold()
            .await
            .expect("publisher fold settles");
        recorder.snapshot()
    });

    // The bridge: writes went out, classified by the key they wrote, and
    // their latency was filed.
    assert!(
        counter(
            &snapshot,
            "loonfs.object_store.operations",
            &[
                ("key_class", "wal_object"),
                ("operation", "put"),
                ("result", "ok")
            ],
        ) >= writes
    );
    assert!(
        counter(
            &snapshot,
            "loonfs.object_store.bytes_in",
            &[("operation", "put")],
        ) > 0
    );
    assert!(histogram_count(&snapshot, "loonfs.object_store.operation_seconds") > 0);
    // Nothing here retried, so the instrument exists and reads zero.
    assert_eq!(
        counter(
            &snapshot,
            "loonfs.object_store.retries",
            &[("operation", "put")],
        ),
        0
    );

    // The publisher: creating a namespace installs a head rather than
    // publishing, so every batch here is one of the puts.
    assert_eq!(
        counter(&snapshot, "loonfs.publisher.publishes", &[("result", "ok")]),
        writes
    );
    assert_eq!(
        histogram_count(&snapshot, "loonfs.publisher.batch_size"),
        counter(&snapshot, "loonfs.publisher.batches", &[]),
        "every batch taken files its size"
    );
    assert_eq!(
        counter(&snapshot, "loonfs.publisher.wal_folds", &[]),
        1,
        "the threshold-crossing publish records its fold"
    );
    assert_eq!(
        gauge(&snapshot, "loonfs.execution_budget.folds_waiting", &[]),
        0,
        "the completed fold leaves no waiter"
    );
    assert_eq!(
        histogram_count(&snapshot, "loonfs.publisher.wal_fold_seconds"),
        1,
        "the completed fold records its duration"
    );
    assert_eq!(
        counter(&snapshot, "loonfs.publisher.write_stop_refusals", &[]),
        0,
        "the test stays below the write-stop bound"
    );
}

/// Checks that reads record their namespace head lookups, and the head
/// state and manifests they cached, in the metadata cache's stats.
#[test]
fn reads_record_head_cache_lookups_and_retained_bytes_in_the_cache_stats() {
    let temp_dir = tempdir().expect("tempdir");
    let (first, second) = (namespace_id("first"), namespace_id("second"));
    let stats = block_on(async {
        let setup = writer(store(temp_dir.path()), "head-cache-setup").await;
        for namespace_id in [&first, &second] {
            setup
                .create_namespace(namespace_id, &loonfs_test_support::test_actor())
                .await
                .expect("create namespace");
        }
        let reader = LoonFs::builder_with_store(store(temp_dir.path()))
            .read_only()
            .build()
            .await
            .expect("build reader");
        for namespace_id in [&first, &first, &second] {
            let namespace = reader.namespace(namespace_id);
            namespace.stat("/").await.expect("stat the root");
        }
        reader.metadata_cache().stats()
    });

    assert_eq!((stats.head_misses, stats.head_hits), (2, 1));
    assert!(
        stats.wal_tail_bytes > 0,
        "the head loads cached both namespaces' WAL tails"
    );
    assert!(
        stats.segment_bytes > 0,
        "the head loads cached the manifests they decoded"
    );
}
