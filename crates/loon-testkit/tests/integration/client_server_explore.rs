use super::common::client_server_support;

use anyhow::Result;
use client_server_support::{
    run_client_server_scenario_report, ClientServerExplorationCaseLabel, ClientServerSimRunOptions,
    ClientServerSimRunStatus,
};
use loon_testkit::explore::{
    run_client_server_exploration_scenario, ConcreteExplorationCase, ExplorationExecutionOutcome,
};
use loon_testkit::fixtures::load_fixture;
use loon_testkit::render::render_yaml;

#[test]
fn client_server_newer_observation_exploration_passes() {
    let report =
        run_exploration_fixture("sim/client_explore_newer_observation_vs_delayed_response.yaml");
    assert!(
        report.first_failure.is_none(),
        "{}",
        report.render_summary()
    );
    assert!(report.executed_cases > 0);
}

#[test]
fn client_server_retry_reuse_exploration_passes() {
    let report = run_exploration_fixture(
        "sim/client_explore_delayed_response_after_restart_request_reuse.yaml",
    );
    assert!(
        report.first_failure.is_none(),
        "{}",
        report.render_summary()
    );
    assert!(report.executed_cases > 0);
}

#[test]
fn client_server_fault_pool_exploration_is_deterministic() {
    let report = run_exploration_fixture("sim/client_explore_success_with_fault_pool.yaml");
    assert!(
        report.first_failure.is_none(),
        "{}",
        report.render_summary()
    );
    assert_snapshot_eq(
        &report.render_summary(),
        include_str!(
            "../../../../tests/snapshots/sim-explore/client_explore_success_with_fault_pool.txt"
        ),
    );
}

#[test]
fn client_server_fault_pool_failure_minimizes_to_plain_sim_repro() {
    let report = run_exploration_fixture("sim/client_explore_fault_failure_minimize.yaml");
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
            "../../../../tests/snapshots/sim-explore/client_explore_fault_failure_minimize.txt"
        ),
    );

    let plain_report = run_client_server_scenario_report(
        &minimized.scenario,
        ClientServerSimRunOptions::default(),
    );
    match &plain_report.status {
        ClientServerSimRunStatus::Failed { failure_headline } => {
            assert_eq!(failure_headline, &failure.failure_headline);
        }
        other => panic!("expected minimized plain sim repro to fail, got {other:?}"),
    }

    assert!(
        minimized.scenario.explore.is_none(),
        "minimized plain sim repro should not retain explore metadata:\n{}",
        render_yaml(&minimized.scenario).expect("render minimized scenario")
    );
}

fn run_exploration_fixture(relative_path: &str) -> loon_testkit::explore::ExplorationRunReport {
    let scenario = load_fixture(relative_path);
    run_client_server_exploration_scenario(&scenario, None, execute_client_server_case)
        .expect("run client/server exploration scenario")
}

fn execute_client_server_case(
    case: &ConcreteExplorationCase,
) -> Result<ExplorationExecutionOutcome> {
    if let Some(reason) = semantic_skip_reason(case) {
        return Ok(ExplorationExecutionOutcome::NotExecutable {
            reason,
            rendered_trace: None,
        });
    }

    let report = run_client_server_scenario_report(
        &case.concrete_scenario,
        ClientServerSimRunOptions {
            exploration_case: Some(ClientServerExplorationCaseLabel {
                case_index: case.case_index,
                seed: case.effective_seed.map(|seed| seed.0),
                fault_summary: case.fault_summary.clone(),
                permuted_action_order: case.permuted_action_order.clone(),
            }),
        },
    );

    Ok(match report.status {
        ClientServerSimRunStatus::Passed => ExplorationExecutionOutcome::Passed,
        ClientServerSimRunStatus::Failed { failure_headline } => {
            ExplorationExecutionOutcome::Failed {
                failure_headline,
                rendered_trace: report.rendered_trace,
            }
        }
        ClientServerSimRunStatus::NotExecutable { reason } => {
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

    if name == "client_explore_newer_observation_vs_delayed_response" {
        let observation = find_action_index(&action_kinds, "deliver_remote_observation")?;
        let response = find_action_index(&action_kinds, "deliver_next_response")?;
        if observation > response {
            return Some(
                "deliver_next_response requires a prior remote observation in this exploration window"
                    .to_owned(),
            );
        }
    }

    if name == "client_explore_delayed_response_after_restart_request_reuse"
        || name == "client_explore_success_with_fault_pool"
        || name == "client_explore_fault_failure_minimize"
    {
        let restart = find_action_index(&action_kinds, "restart_client")?;
        let retry_tick = find_action_index(&action_kinds, "client_tick")?;
        let response = find_action_index(&action_kinds, "deliver_next_response")?;
        if restart > retry_tick || retry_tick > response {
            return Some(
                "retry exploration requires restart_client -> client_tick -> deliver_next_response"
                    .to_owned(),
            );
        }
    }

    if name == "client_explore_fault_failure_minimize" && case.fault.is_none() {
        return Some("synthetic client failure fixture explores only faulted variants".to_owned());
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

fn assert_snapshot_eq(actual: &str, expected: &str) {
    assert_eq!(actual.trim_end(), expected.trim_end());
}
