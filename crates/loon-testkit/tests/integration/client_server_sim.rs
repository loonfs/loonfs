use super::common::client_server_support;

use client_server_support::run_client_server_fixture_report;
use loon_testkit::invariants::{
    evaluate_sim_trace_determinism_invariants, SimTraceDeterminismInvariantInputs,
};

#[test]
fn delayed_response_after_restart_reuses_pending_request_id_and_converges_once() {
    let report =
        run_client_server_fixture_report("sim/client_delayed_response_reuses_pending_request.yaml");
    assert!(matches!(
        report.status,
        client_server_support::ClientServerSimRunStatus::Passed
    ));
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../../tests/snapshots/sim-interleavings/sim/client_delayed_response_reuses_pending_request.txt"
        )
    );
}

#[test]
fn duplicate_authoritative_response_is_idempotent() {
    let report =
        run_client_server_fixture_report("sim/client_duplicate_authoritative_response.yaml");
    assert!(matches!(
        report.status,
        client_server_support::ClientServerSimRunStatus::Passed
    ));
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../../tests/snapshots/sim-interleavings/sim/client_duplicate_authoritative_response.txt"
        )
    );
}

#[test]
fn remote_observation_before_matching_response_converges_without_duplicate_apply() {
    let report =
        run_client_server_fixture_report("sim/client_remote_observation_before_response.yaml");
    assert!(matches!(
        report.status,
        client_server_support::ClientServerSimRunStatus::Passed
    ));
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../../tests/snapshots/sim-interleavings/sim/client_remote_observation_before_response.txt"
        )
    );
}

#[test]
fn sim_trace_order_is_seed_stable_for_client_server_fixture() {
    let first =
        run_client_server_fixture_report("sim/client_delayed_response_reuses_pending_request.yaml");
    let second =
        run_client_server_fixture_report("sim/client_delayed_response_reuses_pending_request.yaml");
    let report = evaluate_sim_trace_determinism_invariants(SimTraceDeterminismInvariantInputs {
        first_rendered_trace: &first.rendered_trace,
        second_rendered_trace: &second.rendered_trace,
    });

    assert!(
        report
            .check("sim_trace_order_is_seed_stable")
            .expect("check should exist")
            .passed
    );
}

#[test]
fn once_only_dispatch_faults_work_under_scheduler_runtime() {
    let report = run_client_server_fixture_report("sim/client_dispatch_error_fault_retry.yaml");
    assert!(matches!(
        report.status,
        client_server_support::ClientServerSimRunStatus::Passed
    ));
    assert!(report.rendered_trace.contains("fault_injected"));
    assert!(report.rendered_trace.contains("dispatch_failed"));
}
