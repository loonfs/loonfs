//! Unit coverage for the recorder surface: registration identity, the
//! atomic-backed values, and histogram bucket arithmetic.

#![allow(clippy::panic)]
// A reading of the wrong kind is a bug in the test, not a case to handle.

use super::*;

fn counter_value(snapshot: &MetricsSnapshot, name: &str) -> u64 {
    match snapshot
        .by_name(name)
        .next()
        .expect("registered counter")
        .value
    {
        MetricValue::Counter(value) => value,
        ref other => panic!("expected a counter, found {other:?}"),
    }
}

#[test]
fn a_counter_reports_what_was_added_to_it() {
    let recorder = DefaultMetricsRecorder::new();
    let counter = recorder.register_counter("loonfs.test.calls", "Calls", &[("kind", "read")]);

    counter.increment(2);
    counter.increment(3);

    let snapshot = recorder.snapshot();
    let entry = snapshot.by_name("loonfs.test.calls").next().expect("entry");
    assert_eq!(entry.labels, vec![("kind", "read")]);
    assert_eq!(entry.description, "Calls");
    assert_eq!(entry.value, MetricValue::Counter(5));
}

#[test]
fn a_gauge_reports_only_its_latest_value() {
    let recorder = DefaultMetricsRecorder::new();
    let gauge = recorder.register_gauge("loonfs.test.depth", "Depth", &[]);

    gauge.set(9);
    gauge.set(4);

    assert_eq!(
        recorder
            .snapshot()
            .by_name("loonfs.test.depth")
            .next()
            .expect("entry")
            .value,
        MetricValue::Gauge(4)
    );
}

#[test]
fn histogram_observations_land_in_the_bucket_their_boundary_names() {
    const BOUNDARIES: &[f64] = &[1.0, 2.0, 4.0];
    let recorder = DefaultMetricsRecorder::new();
    let histogram = recorder.register_histogram("loonfs.test.seconds", "Seconds", &[], BOUNDARIES);

    for observation in [0.5, 1.0, 1.5, 2.0, 4.0, 4.5, 100.0] {
        histogram.record(observation);
    }

    let snapshot = recorder.snapshot();
    let entry = snapshot
        .by_name("loonfs.test.seconds")
        .next()
        .expect("entry");
    match &entry.value {
        MetricValue::Histogram {
            boundaries,
            bucket_counts,
            count,
            sum,
        } => {
            assert_eq!(*boundaries, BOUNDARIES);
            // 0.5 and 1.0 in `<= 1`, 1.5 and 2.0 in `<= 2`, 4.0 in `<= 4`,
            // 4.5 and 100.0 above every boundary.
            assert_eq!(bucket_counts, &[2, 2, 1, 2]);
            assert_eq!(*count, 7);
            assert!((sum - 113.5).abs() < 1e-9, "unexpected sum {sum}");
        }
        other => panic!("expected a histogram, found {other:?}"),
    }
}

#[test]
fn registering_one_name_and_label_set_twice_shares_one_value() {
    let recorder = DefaultMetricsRecorder::new();
    let first = recorder.register_counter("loonfs.test.calls", "Calls", &[("kind", "read")]);
    let second = recorder.register_counter("loonfs.test.calls", "Calls", &[("kind", "read")]);

    first.increment(1);
    second.increment(1);

    let snapshot = recorder.snapshot();
    assert_eq!(snapshot.by_name("loonfs.test.calls").count(), 1);
    assert_eq!(counter_value(&snapshot, "loonfs.test.calls"), 2);
}

#[test]
fn label_order_does_not_split_an_instrument_in_two() {
    let recorder = DefaultMetricsRecorder::new();
    recorder
        .register_counter(
            "loonfs.test.calls",
            "Calls",
            &[("kind", "read"), ("result", "ok")],
        )
        .increment(1);
    recorder
        .register_counter(
            "loonfs.test.calls",
            "Calls",
            &[("result", "ok"), ("kind", "read")],
        )
        .increment(1);

    let snapshot = recorder.snapshot();
    assert_eq!(snapshot.by_name("loonfs.test.calls").count(), 1);
    assert_eq!(counter_value(&snapshot, "loonfs.test.calls"), 2);
}

#[test]
fn distinct_label_sets_are_distinct_instruments() {
    let recorder = DefaultMetricsRecorder::new();
    recorder
        .register_counter("loonfs.test.calls", "Calls", &[("kind", "read")])
        .increment(1);
    recorder
        .register_counter("loonfs.test.calls", "Calls", &[("kind", "write")])
        .increment(4);

    let snapshot = recorder.snapshot();
    let entries: Vec<_> = snapshot.by_name("loonfs.test.calls").collect();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].labels, vec![("kind", "read")]);
    assert_eq!(entries[0].value, MetricValue::Counter(1));
    assert_eq!(entries[1].labels, vec![("kind", "write")]);
    assert_eq!(entries[1].value, MetricValue::Counter(4));
}

#[test]
fn a_snapshot_orders_entries_by_name_then_labels() {
    let recorder = DefaultMetricsRecorder::new();
    recorder.register_counter("loonfs.test.zebra", "Z", &[]);
    recorder.register_counter("loonfs.test.alpha", "A", &[("kind", "write")]);
    recorder.register_counter("loonfs.test.alpha", "A", &[("kind", "read")]);

    let snapshot = recorder.snapshot();
    let ordered: Vec<_> = snapshot
        .all()
        .iter()
        .map(|entry| (entry.name, entry.labels.clone()))
        .collect();
    assert_eq!(
        ordered,
        vec![
            ("loonfs.test.alpha", vec![("kind", "read")]),
            ("loonfs.test.alpha", vec![("kind", "write")]),
            ("loonfs.test.zebra", vec![]),
        ]
    );
}

#[test]
fn registering_on_the_noop_recorder_does_not_panic() {
    let recorder = NoopMetricsRecorder;
    recorder
        .register_counter("loonfs.test.calls", "Calls", &[])
        .increment(1);
    recorder
        .register_gauge("loonfs.test.depth", "Depth", &[])
        .set(1);
    recorder
        .register_histogram(
            "loonfs.test.seconds",
            "Seconds",
            &[],
            LATENCY_SECONDS_BOUNDARIES,
        )
        .record(1.0);
}
