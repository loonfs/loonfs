#[path = "common/namespace_interleaving_support.rs"]
mod namespace_interleaving_support;

use loon_testkit::invariants::{
    evaluate_unified_namespace_sim_trace_determinism_invariants,
    UnifiedNamespaceSimTraceDeterminismInvariantInputs,
};
use namespace_interleaving_support::run_namespace_fixture_report;

#[test]
fn delayed_response_after_newer_authoritative_observation_is_idempotent() {
    let report =
        run_namespace_fixture_report("sim/namespace_delayed_response_after_newer_observation.yaml");
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/sim-interleavings/sim/namespace_delayed_response_after_newer_observation.txt"
        )
    );
}

#[test]
fn checkpoint_publish_uses_latest_head_after_client_server_advance() {
    let report = run_namespace_fixture_report(
        "sim/namespace_checkpoint_publish_uses_latest_head_after_client_server_advance.yaml",
    );
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/sim-interleavings/sim/namespace_checkpoint_publish_uses_latest_head_after_client_server_advance.txt"
        )
    );
}

#[test]
fn snapshot_repair_uses_latest_head_after_client_server_advance() {
    let report = run_namespace_fixture_report(
        "sim/namespace_repair_uses_latest_head_after_client_server_advance.yaml",
    );
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/sim-interleavings/sim/namespace_repair_uses_latest_head_after_client_server_advance.txt"
        )
    );
}

#[test]
fn stale_writer_fence_survives_inflight_client_request() {
    let report = run_namespace_fixture_report(
        "sim/namespace_stale_writer_fence_survives_inflight_client_request.yaml",
    );
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/sim-interleavings/sim/namespace_stale_writer_fence_survives_inflight_client_request.txt"
        )
    );
}

#[test]
fn unified_namespace_sim_trace_order_is_seed_stable_for_checkpoint_fixture() {
    let first = run_namespace_fixture_report(
        "sim/namespace_checkpoint_publish_uses_latest_head_after_client_server_advance.yaml",
    );
    let second = run_namespace_fixture_report(
        "sim/namespace_checkpoint_publish_uses_latest_head_after_client_server_advance.yaml",
    );
    let report = evaluate_unified_namespace_sim_trace_determinism_invariants(
        UnifiedNamespaceSimTraceDeterminismInvariantInputs {
            first_rendered_trace: &first.rendered_trace,
            second_rendered_trace: &second.rendered_trace,
        },
    );

    assert!(
        report
            .check("unified_namespace_sim_trace_order_is_seed_stable")
            .expect("check should exist")
            .passed
    );
}
