#[path = "common/background_scheduler_support.rs"]
mod background_scheduler_support;

use background_scheduler_support::run_background_fixture_report;
use loon_testkit::invariants::{
    evaluate_background_sim_trace_determinism_invariants,
    BackgroundSimTraceDeterminismInvariantInputs,
};

#[test]
fn background_stale_writer_remains_fenced_after_handover() {
    let report = run_background_fixture_report(
        "sim/background_stale_writer_remains_fenced_after_handover.yaml",
    );
    assert!(matches!(
        report.status,
        background_scheduler_support::BackgroundSimRunStatus::Passed
    ));
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/sim-interleavings/sim/background_stale_writer_remains_fenced_after_handover.txt"
        )
    );
}

#[test]
fn background_checkpoint_publish_waits_for_required_progress() {
    let report = run_background_fixture_report(
        "sim/background_checkpoint_publish_waits_for_required_progress.yaml",
    );
    assert!(matches!(
        report.status,
        background_scheduler_support::BackgroundSimRunStatus::Passed
    ));
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/sim-interleavings/sim/background_checkpoint_publish_waits_for_required_progress.txt"
        )
    );
}

#[test]
fn background_repair_tracks_latest_visible_head_seq() {
    let report =
        run_background_fixture_report("sim/background_repair_tracks_latest_visible_head_seq.yaml");
    assert!(matches!(
        report.status,
        background_scheduler_support::BackgroundSimRunStatus::Passed
    ));
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/sim-interleavings/sim/background_repair_tracks_latest_visible_head_seq.txt"
        )
    );
}

#[test]
fn background_sim_trace_order_is_seed_stable_for_checkpoint_fixture() {
    let first = run_background_fixture_report(
        "sim/background_checkpoint_publish_waits_for_required_progress.yaml",
    );
    let second = run_background_fixture_report(
        "sim/background_checkpoint_publish_waits_for_required_progress.yaml",
    );
    let report = evaluate_background_sim_trace_determinism_invariants(
        BackgroundSimTraceDeterminismInvariantInputs {
            first_rendered_trace: &first.rendered_trace,
            second_rendered_trace: &second.rendered_trace,
        },
    );

    assert!(
        report
            .check("background_sim_trace_order_is_seed_stable")
            .expect("check should exist")
            .passed
    );
}
