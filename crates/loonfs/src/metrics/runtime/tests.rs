//! The registration and the reports of a runtime's instruments.

use super::*;
use crate::metrics::object_store::fan_out_object_store_recorder;
use crate::metrics::{
    DefaultMetricsRecorder, MetricValue, MetricsSnapshot, ObjectKeyKind, ObjectStoreMetricSample,
    ObjectStoreOperation, ObjectStoreResultKind, PutModeKind, VecObjectStoreMetricsRecorder,
};
use crate::publisher::metrics::PublishOutcome;
use crate::GcResponse;
use loonfs_core::CompactionUnitOutcome;
use std::assert_matches;

fn sample(
    operation: ObjectStoreOperation,
    result: ObjectStoreResultKind,
    attempts: u32,
) -> ObjectStoreMetricSample {
    classified_sample(operation, result, ObjectKeyKind::Content, attempts)
}

fn classified_sample(
    operation: ObjectStoreOperation,
    result: ObjectStoreResultKind,
    key_class: ObjectKeyKind,
    attempts: u32,
) -> ObjectStoreMetricSample {
    ObjectStoreMetricSample {
        operation,
        elapsed_micros: 250_000,
        attempts,
        result,
        bytes_in: Some(64),
        bytes_out: Some(16),
        item_count: None,
        key_class,
        range_class: None,
        put_mode: Some(PutModeKind::CreateIfAbsent),
        store_kind: None,
    }
}

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

/// Checks that each read-head source reaches its own labeled counter,
/// and that every source is registered before any read records one.
#[test]
fn read_head_sources_reach_the_registered_instruments() {
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let instruments = RuntimeInstruments::new(Some(recorder.clone()));
    instruments.read_head(ReadHeadSource::Position);
    instruments.read_head(ReadHeadSource::Eventual);
    instruments.read_head(ReadHeadSource::Eventual);

    let snapshot = recorder.snapshot();
    for (source, value) in [("position", 1), ("anchor", 0), ("eventual", 2)] {
        assert_eq!(
            counter(&snapshot, "loonfs.read_head.source", &[("source", source)]),
            value,
            "source `{source}`"
        );
    }
}

#[test]
fn a_bridged_sample_moves_every_object_store_instrument() {
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let instruments = RuntimeInstruments::new(Some(recorder.clone()));
    let bridge = instruments
        .object_store_recorder()
        .expect("an installed recorder bridges its samples");

    bridge.record(sample(
        ObjectStoreOperation::Put,
        ObjectStoreResultKind::Ok,
        3,
    ));

    let snapshot = recorder.snapshot();
    assert_eq!(
        counter(
            &snapshot,
            "loonfs.object_store.operations",
            &[
                ("key_class", "content"),
                ("operation", "put"),
                ("result", "ok")
            ],
        ),
        1
    );
    assert_eq!(
        counter(
            &snapshot,
            "loonfs.object_store.bytes_in",
            &[("operation", "put")],
        ),
        64
    );
    assert_eq!(
        counter(
            &snapshot,
            "loonfs.object_store.bytes_out",
            &[("operation", "put")],
        ),
        16
    );
    // Three attempts is two retries: the first attempt is the call.
    assert_eq!(
        counter(
            &snapshot,
            "loonfs.object_store.retries",
            &[("operation", "put")],
        ),
        2
    );
    let latency = snapshot
        .by_name("loonfs.object_store.operation_seconds")
        .next()
        .expect("the latency histogram is registered");
    match &latency.value {
        MetricValue::Histogram { count, sum, .. } => {
            assert_eq!(*count, 1);
            assert!((sum - 0.25).abs() < 1e-9, "unexpected sum {sum}");
        }
        other => panic!("expected a histogram, found {other:?}"),
    }
}

#[test]
fn outcomes_and_key_classes_of_one_operation_count_separately() {
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let instruments = RuntimeInstruments::new(Some(recorder.clone()));
    let bridge = instruments.object_store_recorder().expect("bridge");

    for (result, key_class) in [
        (ObjectStoreResultKind::Ok, ObjectKeyKind::WalObject),
        (ObjectStoreResultKind::NotFound, ObjectKeyKind::WalObject),
        (ObjectStoreResultKind::NotFound, ObjectKeyKind::WalObject),
        (ObjectStoreResultKind::Ok, ObjectKeyKind::NamespaceManifest),
    ] {
        bridge.record(classified_sample(
            ObjectStoreOperation::Get,
            result,
            key_class,
            1,
        ));
    }

    let snapshot = recorder.snapshot();
    for (result, key_class, count) in [
        ("ok", "wal_object", 1),
        ("not_found", "wal_object", 2),
        ("ok", "namespace_manifest", 1),
    ] {
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.object_store.operations",
                &[
                    ("key_class", key_class),
                    ("operation", "get"),
                    ("result", result)
                ],
            ),
            count,
            "{result} {key_class}"
        );
    }
    assert_eq!(
        snapshot
            .by_name("loonfs.object_store.operation_seconds")
            .count(),
        1,
        "one operation keeps one latency histogram whatever it returned"
    );
}

#[test]
fn a_handle_with_both_recorders_delivers_each_sample_to_both() {
    let samples = Arc::new(VecObjectStoreMetricsRecorder::default());
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let instruments = RuntimeInstruments::new(Some(recorder.clone()));
    let composed =
        fan_out_object_store_recorder(Some(samples.clone()), instruments.object_store_recorder())
            .expect("a configured handle installs a recorder");

    composed.record(sample(
        ObjectStoreOperation::Delete,
        ObjectStoreResultKind::Ok,
        1,
    ));

    assert_eq!(samples.samples().len(), 1);
    assert_eq!(
        counter(
            &recorder.snapshot(),
            "loonfs.object_store.operations",
            &[
                ("key_class", "content"),
                ("operation", "delete"),
                ("result", "ok")
            ],
        ),
        1
    );
}

#[test]
fn a_handle_with_no_recorder_installs_nothing() {
    let instruments = RuntimeInstruments::new(None);
    assert!(instruments.object_store_recorder().is_none());
    assert!(fan_out_object_store_recorder(None, instruments.object_store_recorder()).is_none());

    instruments.publisher.batch(4);
    instruments.publisher.wal_fold();
    instruments.publisher.wal_fold_duration(5);
    instruments.publisher.write_stop_wait(2);
    instruments.publisher.write_stop_wait_duration(5);
    instruments.publisher.write_stop_refusal(1);
    instruments.publisher.publish(PublishOutcome::Ok);
}

#[test]
fn compaction_instruments_register_the_closed_vocabularies() {
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let instruments = RuntimeInstruments::new(Some(recorder.clone()));

    instruments.compactions.finished(
        &Ok(CompactionUnitOutcome::Published {
            group: loonfs_core::MetadataFamilyGroup::Bindings,
            input_runs: 2,
            manifest: loonfs_core::PublishedManifest {
                manifest: loonfs_types::format::control::ManifestRef {
                    owner_namespace_id: loonfs_types::NamespaceId::parse("demo")
                        .expect("valid namespace id"),
                    manifest_no: loonfs_types::ManifestNo(1),
                    head_seq: loonfs_types::ChangeSeq(1),
                    payload_checksum: loonfs_types::sha256_digest(b""),
                },
                writer_epoch: loonfs_types::WriterEpoch(1),
                status: loonfs_types::format::control::NamespaceStatus::Active {},
                folded_wal_no: loonfs_types::WalNo(1),
                retention_floor_seq: loonfs_types::ChangeSeq(0),
                started: None,
            },
            rows_read: 20,
            rows_written: 10,
            input_bytes: 2_000,
            output_bytes: 1_000,
            output_segments: 1,
        }),
        25,
    );
    instruments
        .compactions
        .finished(&Ok(CompactionUnitOutcome::Abandoned), 10);
    instruments
        .compactions
        .finished(&Ok(CompactionUnitOutcome::NotNeeded), 10);

    let snapshot = recorder.snapshot();
    assert_eq!(
        snapshot.by_name("loonfs.maintenance.compactions").count(),
        5
    );
    assert_eq!(
        snapshot
            .by_name("loonfs.maintenance.compaction_seconds")
            .count(),
        5
    );
    assert_eq!(
        snapshot
            .by_name("loonfs.maintenance.compaction_rows")
            .count(),
        2
    );
    assert_eq!(
        snapshot
            .by_name("loonfs.maintenance.compaction_bytes")
            .count(),
        2
    );
    assert_eq!(
        counter(
            &snapshot,
            "loonfs.maintenance.compactions",
            &[("outcome", "completed")],
        ),
        1
    );
    assert_eq!(
        counter(
            &snapshot,
            "loonfs.maintenance.compactions",
            &[("outcome", "abandoned")],
        ),
        1
    );
    let duration = snapshot
        .by_name("loonfs.maintenance.compaction_seconds")
        .find(|entry| entry.labels == [("outcome", "abandoned")])
        .expect("abandoned compaction duration");
    assert_matches!(duration.value, MetricValue::Histogram { count: 1, .. });
    assert!(snapshot
        .all()
        .iter()
        .all(|entry| !entry.labels.contains(&("outcome", "superseded"))));
    assert_eq!(
        counter(
            &snapshot,
            "loonfs.maintenance.compaction_rows",
            &[("direction", "input")],
        ),
        20
    );
    assert_eq!(
        counter(
            &snapshot,
            "loonfs.maintenance.compaction_bytes",
            &[("direction", "output")],
        ),
        1_000
    );
}

#[test]
fn a_collection_pass_counts_what_it_reclaimed_and_retained() {
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let instruments = RuntimeInstruments::new(Some(recorder.clone()));
    let mut gc = GcResponse {
        namespace_id: loonfs_test_support::ids::namespace_id("demo"),
        deleted: loonfs_types::DeletedObjectCounts {
            wal_objects: 3,
            content_objects: 5,
            temporary_objects: 11,
            retired_content_objects: 13,
            ..loonfs_types::DeletedObjectCounts::default()
        },
        deleted_checkpoints_by_owner: loonfs_types::DeletedCheckpointsByOwner { fork: 2, user: 3 },
        retained: loonfs_types::RetainedCandidates {
            referenced: 2,
            ..loonfs_types::RetainedCandidates::default()
        },
        listed_bytes: 0,
        next_reclamation_at_ms: None,
        reclaimable_at_ms: None,
        extensions: loonfs_types::ExtensionGcReports::default(),
    };

    instruments.gc.pass(&gc);
    gc.deleted.wal_objects = 1;
    instruments.gc.pass(&gc);

    let snapshot = recorder.snapshot();
    assert_eq!(
        counter(
            &snapshot,
            "loonfs.gc.reclaimed",
            &[("category", "deleted_wal_objects")],
        ),
        4
    );
    assert_eq!(
        counter(
            &snapshot,
            "loonfs.gc.reclaimed",
            &[("category", "deleted_content_objects")],
        ),
        10
    );
    assert_eq!(counter(&snapshot, "loonfs.gc.retained", &[]), 4);
    // Every family registers at construction, so a scrape names the
    // whole reclaimable vocabulary rather than only what has happened.
    assert_eq!(snapshot.by_name("loonfs.gc.reclaimed").count(), 9);
    for (category, expected) in [
        ("deleted_fork_checkpoints", 4),
        ("deleted_expired_checkpoints", 6),
        ("deleted_temporary_objects", 22),
        ("deleted_retired_content_objects", 26),
    ] {
        assert_eq!(
            counter(&snapshot, "loonfs.gc.reclaimed", &[("category", category)]),
            expected
        );
    }
}
