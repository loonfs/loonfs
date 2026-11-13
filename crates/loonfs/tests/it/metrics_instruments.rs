//! What a writer built with a metrics recorder actually reports (the
//! object-store bridge and the publication instruments), and where reads
//! take their namespace heads from.

use crate::common::*;
use loonfs::metrics::DefaultMetricsRecorder;
use loonfs::test_support::{counter, gauge, histogram_count, writer};
use loonfs::{LoonFs, MaintainMetadataOptions};
use loonfs_test_support::block_on::block_on;
use loonfs_test_support::ids::namespace_id;
use std::sync::Arc;
use tempfile::tempdir;

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

/// Checks that reads record where they took their namespace heads from,
/// and that the metadata cache's stats record the head state and manifests
/// they cached.
#[test]
fn reads_record_their_head_sources_and_the_cache_stats_record_retained_bytes() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(DefaultMetricsRecorder::new());
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
            .metrics_recorder(recorder.clone())
            .build()
            .await
            .expect("build reader");
        for namespace_id in [&first, &first, &second] {
            let namespace = reader.namespace(namespace_id);
            namespace.stat("/").await.expect("stat the root");
        }
        reader.metadata_cache().stats()
    });

    let snapshot = recorder.snapshot();
    let reads_from = |source| counter(&snapshot, "loonfs.read_head.source", &[("source", source)]);
    assert_eq!(
        (
            reads_from("loaded"),
            reads_from("eventual") + reads_from("refreshed")
        ),
        (2, 1),
        "the first read of each namespace loads its head, and the second read of `first` \
         starts from the cached head"
    );
    assert!(
        stats.wal_tails.cached_decoded_bytes > 0,
        "the head loads cached both namespaces' WAL tails"
    );
    assert!(
        stats.manifests.cached_decoded_bytes > 0,
        "the head loads cached the manifests they decoded"
    );
}
