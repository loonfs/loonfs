//! What a writer built with a metrics recorder actually reports: the
//! object-store bridge, the publication instruments, and a maintenance step
//! that settled through the runner.

#![allow(clippy::panic)]
// A missing instrument is a wiring bug, and naming it beats an option.

use crate::common::*;
use loonfs::metrics::{DefaultMetricsRecorder, MetricValue, MetricsSnapshot};
use loonfs::{
    CreateNamespaceOptions, FsBackgroundWork, MaintenanceJobId, MaintenanceStepConclusion,
    MaintenanceStepOptions, PutFileOptions,
};
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

fn histogram_count(snapshot: &MetricsSnapshot, name: &str) -> u64 {
    snapshot
        .by_name(name)
        .map(|entry| match entry.value {
            MetricValue::Histogram { count, .. } => count,
            ref other => panic!("expected a histogram, found {other:?}"),
        })
        .sum()
}

/// One writer's ordinary work has to reach the recorder through three
/// separate paths: the store wrapper the builder installed, the publisher,
/// and the maintenance runner.
#[test]
fn a_writer_with_a_recorder_reports_stores_publications_and_steps() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let namespace_id = namespace_id("demo");
    // Enough writes to push the WAL tail past its threshold, which is what
    // nudges the metadata job and gives the runner a step to settle.
    let writes = MaintenanceStepOptions::default().max_wal_tail_segments + 1;
    // One runtime for the whole test: the writer's background work is
    // spawned on the runtime that opened it, so a second `block_on` would
    // be waiting on tasks that went away with the first.
    let snapshot = block_on(async {
        let fs = open_runtime_with_async(store(temp_dir.path()), "metrics-writer", |builder| {
            builder
                .background_work(FsBackgroundWork::Enabled)
                .metrics_recorder(recorder.clone())
        })
        .await;
        fs.writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        for file in 0..writes {
            fs.writer
                .put_file_bytes(
                    &namespace_id,
                    &format!("/docs/file-{file}.txt"),
                    b"body",
                    PutFileOptions::default(),
                )
                .await
                .expect("put file");
        }
        fs.writer
            .wait_for_background_work()
            .await
            .expect("background steps settle");
        recorder.snapshot()
    });

    // The bridge: writes went out and their latency was filed.
    assert!(
        counter(
            &snapshot,
            "loonfs.object_store.operations",
            &[("operation", "put"), ("result", "ok")],
        ) > 0
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

    // The runner: a write nudges the metadata job, and whatever it
    // concluded, the step settled under its own job label.
    let metadata_steps: u64 = MetadataConclusions::all()
        .map(|conclusion| {
            counter(
                &snapshot,
                "loonfs.maintenance.steps",
                &[("conclusion", conclusion), ("job", "metadata")],
            )
        })
        .sum();
    assert!(
        metadata_steps > 0,
        "a write should have settled at least one metadata step"
    );
    assert!(histogram_count(&snapshot, "loonfs.maintenance.step_seconds") > 0);
    assert!(histogram_count(&snapshot, "loonfs.maintenance.queue_wait_seconds") > 0);
}

/// The automatic collection path reads a pass as one conclusion; these are
/// the counts that would otherwise be dropped with the response.
#[test]
fn a_collection_step_reports_what_the_pass_retained() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let namespace_id = namespace_id("demo");
    let snapshot = block_on(async {
        let fs = open_runtime_with_async(store(temp_dir.path()), "metrics-gc-writer", |builder| {
            builder.metrics_recorder(recorder.clone())
        })
        .await;
        fs.writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        let job = fs
            .writer
            .maintenance_job(MaintenanceJobId::GC)
            .expect("the runtime registers its collection job");
        job.step(&namespace_id, None)
            .await
            .expect("run one collection pass");
        recorder.snapshot()
    });
    assert_eq!(
        snapshot.by_name("loonfs.gc.reclaimed").count(),
        9,
        "one pass registers the whole reclaimable vocabulary"
    );
    assert_eq!(
        counter(
            &snapshot,
            "loonfs.gc.reclaimed",
            &[("category", "deleted_wal_segments")],
        ),
        0,
        "a fresh namespace has nothing to reclaim yet"
    );
}

/// A handle built without a recorder must be exactly the handle it was
/// before: no wrapper on its store, and nothing registered anywhere.
#[test]
fn a_writer_without_a_recorder_registers_nothing() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let fs = runtime(temp_dir.path(), "unmetered-writer");

    fs.create_namespace_blocking(&namespace_id("demo"), CreateNamespaceOptions::default())
        .expect("create namespace");

    assert!(recorder.snapshot().all().is_empty());
}

/// The five conclusions a metadata step can settle on, as label values.
struct MetadataConclusions;

impl MetadataConclusions {
    fn all() -> impl Iterator<Item = &'static str> {
        [
            MaintenanceStepConclusion::Progressed,
            MaintenanceStepConclusion::Idle,
            MaintenanceStepConclusion::Blocked,
            MaintenanceStepConclusion::Superseded,
            MaintenanceStepConclusion::NotEnabled,
        ]
        .into_iter()
        .map(MaintenanceStepConclusion::as_str)
    }
}
