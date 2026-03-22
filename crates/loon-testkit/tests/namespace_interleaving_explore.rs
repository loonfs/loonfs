#[path = "common/namespace_interleaving_support.rs"]
mod namespace_interleaving_support;

use anyhow::Result;
use loon_testkit::explore::{
    run_unified_namespace_exploration_scenario, ConcreteExplorationCase,
    ExplorationExecutionOutcome,
};
use loon_testkit::fixtures::load_fixture;
use loon_testkit::render::render_yaml;
use namespace_interleaving_support::{
    run_namespace_scenario_report, NamespaceExplorationCaseLabel, NamespaceSimRunOptions,
    NamespaceSimRunStatus,
};

#[test]
fn newer_observation_exploration_is_deterministic_and_passes() {
    let report =
        run_exploration_fixture("sim/namespace_explore_newer_observation_vs_delayed_response.yaml");
    assert!(
        report.first_failure.is_none(),
        "{}",
        report.render_summary()
    );
    assert_snapshot_eq(
        &report.render_summary(),
        include_str!(
            "../../../tests/snapshots/sim-explore/namespace_explore_newer_observation_vs_delayed_response.txt"
        ),
    );
}

#[test]
fn stale_writer_exploration_passes_under_bounded_reordering() {
    let report = run_exploration_fixture(
        "sim/namespace_explore_stale_writer_vs_inflight_client_request.yaml",
    );
    assert!(
        report.first_failure.is_none(),
        "{}",
        report.render_summary()
    );
    assert!(report.executed_cases > 0);
}

#[test]
fn checkpoint_and_repair_exploration_passes_under_bounded_reordering() {
    let report = run_exploration_fixture(
        "sim/namespace_explore_checkpoint_and_repair_after_client_server_advance.yaml",
    );
    assert!(
        report.first_failure.is_none(),
        "{}",
        report.render_summary()
    );
    assert!(report.executed_cases > 0);
}

#[test]
fn exploration_with_fault_pool_stays_deterministic() {
    let report = run_exploration_fixture("sim/namespace_explore_success_with_fault_pool.yaml");
    assert!(
        report.first_failure.is_none(),
        "{}",
        report.render_summary()
    );
    assert_snapshot_eq(
        &report.render_summary(),
        include_str!(
            "../../../tests/snapshots/sim-explore/namespace_explore_success_with_fault_pool.txt"
        ),
    );
}

#[test]
fn failing_fault_exploration_produces_minimized_plain_sim_repro() {
    let report = run_exploration_fixture("sim/namespace_explore_fault_failure_minimize.yaml");
    let failure = report
        .first_failure
        .as_ref()
        .expect("exploration should stop on first failing executable case");
    let minimized = failure
        .minimized
        .as_ref()
        .expect("failing exploration should produce a minimized case");
    let rendered = format!(
        "{}\nfirst_failure_fault={}\n[trace]\n{}\n[minimized_report]\n{}",
        report.render_summary(),
        failure.fault_summary,
        failure.rendered_trace,
        minimized.render_summary()
    );
    assert_snapshot_eq(
        &rendered,
        include_str!(
            "../../../tests/snapshots/sim-explore/namespace_explore_fault_failure_minimize.txt"
        ),
    );

    let plain_report =
        run_namespace_scenario_report(&minimized.scenario, NamespaceSimRunOptions::default());
    match &plain_report.status {
        NamespaceSimRunStatus::Failed { failure_headline } => {
            assert_eq!(failure_headline, &failure.failure_headline);
        }
        other => panic!("expected plain minimized repro to fail, got {other:?}"),
    }

    assert!(
        minimized.scenario.explore.is_none(),
        "minimized plain sim repro should not retain explore metadata:\n{}",
        render_yaml(&minimized.scenario).expect("render minimized scenario")
    );
}

fn run_exploration_fixture(relative_path: &str) -> loon_testkit::explore::ExplorationRunReport {
    let scenario = load_fixture(relative_path);
    run_unified_namespace_exploration_scenario(&scenario, None, |case| {
        execute_namespace_exploration_case(case)
    })
    .expect("run namespace exploration scenario")
}

fn assert_snapshot_eq(actual: &str, expected: &str) {
    assert_eq!(actual.trim_end(), expected.trim_end());
}

fn execute_namespace_exploration_case(
    case: &ConcreteExplorationCase,
) -> Result<ExplorationExecutionOutcome> {
    if let Some(reason) = semantic_skip_reason(case) {
        return Ok(ExplorationExecutionOutcome::NotExecutable {
            reason,
            rendered_trace: None,
        });
    }

    let report = run_namespace_scenario_report(
        &case.concrete_scenario,
        NamespaceSimRunOptions {
            exploration_case: Some(NamespaceExplorationCaseLabel {
                case_index: case.case_index,
                seed: case.effective_seed.map(|seed| seed.0),
                fault_summary: case.fault_summary.clone(),
                permuted_action_order: case.permuted_action_order.clone(),
            }),
        },
    );

    Ok(match report.status {
        NamespaceSimRunStatus::Passed => ExplorationExecutionOutcome::Passed,
        NamespaceSimRunStatus::Failed { failure_headline } => ExplorationExecutionOutcome::Failed {
            failure_headline,
            rendered_trace: report.rendered_trace,
        },
        NamespaceSimRunStatus::NotExecutable { reason } => {
            ExplorationExecutionOutcome::NotExecutable {
                reason,
                rendered_trace: Some(report.rendered_trace),
            }
        }
    })
}

fn semantic_skip_reason(case: &ConcreteExplorationCase) -> Option<String> {
    let action_kinds = case
        .permuted_actions
        .iter()
        .map(action_kind)
        .collect::<Vec<_>>();
    let name = case.concrete_scenario.name.as_str();

    if name == "namespace_explore_newer_observation_vs_delayed_response"
        || name == "namespace_explore_success_with_fault_pool"
        || name == "namespace_explore_fault_failure_minimize"
    {
        let observation = find_action_index(&action_kinds, "deliver_remote_observation")?;
        let response = find_action_index(&action_kinds, "deliver_next_response")?;
        if observation > response {
            return Some(
                "deliver_next_response requires a prior newer observation in this exploration window"
                    .to_owned(),
            );
        }
    }

    if name == "namespace_explore_fault_failure_minimize" && case.fault.is_none() {
        return Some(
            "synthetic failure minimization fixture explores only faulted variants".to_owned(),
        );
    }

    if name == "namespace_explore_stale_writer_vs_inflight_client_request" {
        let handover = find_action_index(&action_kinds, "lease_handover")?;
        let publish = find_action_index(&action_kinds, "writer_publish_head")?;
        let attempt = find_action_index(&action_kinds, "writer_attempt_commit")?;
        let response = find_action_index(&action_kinds, "deliver_next_response")?;
        if handover > publish || publish > attempt || attempt > response {
            return Some(
                "writer interleaving requires lease_handover -> writer_publish_head -> writer_attempt_commit -> deliver_next_response"
                    .to_owned(),
            );
        }
    }

    if name == "namespace_explore_checkpoint_and_repair_after_client_server_advance" {
        let publish_positions = find_action_indices(&action_kinds, "checkpoint_publish");
        if publish_positions.len() == 2 {
            let progress = find_action_index(&action_kinds, "publish_progress")?;
            if publish_positions[0] > progress || publish_positions[1] < progress {
                return Some(
                    "checkpoint publish exploration requires publish_progress between the two publish attempts"
                        .to_owned(),
                );
            }
        }
    }

    None
}

fn action_kind(action: &loon_testkit::explore::ScenarioFragment) -> String {
    action
        .keys()
        .next()
        .cloned()
        .expect("scenario action fragment should contain exactly one key")
}

fn find_action_index(action_kinds: &[String], target: &str) -> Option<usize> {
    action_kinds.iter().position(|kind| kind == target)
}

fn find_action_indices(action_kinds: &[String], target: &str) -> Vec<usize> {
    action_kinds
        .iter()
        .enumerate()
        .filter_map(|(index, kind)| (kind == target).then_some(index))
        .collect()
}
