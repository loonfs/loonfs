#[path = "common/queue_scheduler_support.rs"]
mod queue_scheduler_support;

use loon_testkit::invariants::{
    evaluate_queue_sim_trace_determinism_invariants, QueueSimTraceDeterminismInvariantInputs,
};
use queue_scheduler_support::run_queue_fixture_report;

#[test]
fn lost_enqueue_repair_fixture_matches_model_and_queue() {
    let report = run_queue_fixture_report("sim/lost_enqueue_repair_recreates_snapshot_job.yaml");
    assert!(matches!(
        report.status,
        queue_scheduler_support::QueueSimRunStatus::Passed
    ));
    assert!(!report.rendered_trace.is_empty());
}

#[test]
fn queue_claim_timeout_then_steal_fixture_matches_model_and_queue() {
    let report = run_queue_fixture_report("sim/queue_claim_timeout_then_steal.yaml");
    assert!(matches!(
        report.status,
        queue_scheduler_support::QueueSimRunStatus::Passed
    ));
    assert!(!report.rendered_trace.is_empty());
}

#[test]
fn queue_claim_timeout_then_steal_snapshot_matches_checked_in_artifact() {
    let report = run_queue_fixture_report("sim/queue_claim_timeout_then_steal.yaml");
    assert!(matches!(
        report.status,
        queue_scheduler_support::QueueSimRunStatus::Passed
    ));
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/queue-differential/sim/queue_claim_timeout_then_steal.txt"
        )
    );
}

#[test]
fn lost_enqueue_repair_snapshot_matches_checked_in_artifact() {
    let report = run_queue_fixture_report("sim/lost_enqueue_repair_recreates_snapshot_job.yaml");
    assert!(matches!(
        report.status,
        queue_scheduler_support::QueueSimRunStatus::Passed
    ));
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/queue-differential/sim/lost_enqueue_repair_recreates_snapshot_job.txt"
        )
    );
}

#[test]
fn queue_sim_trace_order_is_seed_stable_for_claim_fixture() {
    let first = run_queue_fixture_report("sim/queue_claim_timeout_then_steal.yaml");
    let second = run_queue_fixture_report("sim/queue_claim_timeout_then_steal.yaml");
    let report =
        evaluate_queue_sim_trace_determinism_invariants(QueueSimTraceDeterminismInvariantInputs {
            first_rendered_trace: &first.rendered_trace,
            second_rendered_trace: &second.rendered_trace,
        });

    assert!(
        report
            .check("queue_sim_trace_order_is_seed_stable")
            .expect("check should exist")
            .passed
    );
}
