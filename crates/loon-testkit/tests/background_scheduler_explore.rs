#[path = "common/background_scheduler_support.rs"]
mod background_scheduler_support;

use anyhow::Result;
use background_scheduler_support::{
    run_background_scenario_report, BackgroundExplorationCaseLabel, BackgroundSimRunOptions,
    BackgroundSimRunStatus,
};
use loon_testkit::explore::{
    run_background_exploration_scenario, ConcreteExplorationCase, ExplorationExecutionOutcome,
};
use loon_testkit::fixtures::load_fixture;

#[test]
fn background_stale_writer_exploration_passes() {
    let report = run_exploration_fixture("sim/background_explore_stale_writer_handover.yaml");
    assert!(
        report.first_failure.is_none(),
        "{}",
        report.render_summary()
    );
    assert!(report.executed_cases > 0);
}

#[test]
fn background_checkpoint_publish_exploration_is_deterministic() {
    let report = run_exploration_fixture(
        "sim/background_explore_checkpoint_publish_waits_and_succeeds.yaml",
    );
    assert!(
        report.first_failure.is_none(),
        "{}",
        report.render_summary()
    );
    assert_snapshot_eq(
        &report.render_summary(),
        include_str!(
            "../../../tests/snapshots/sim-explore/background_explore_checkpoint_publish_waits_and_succeeds.txt"
        ),
    );
}

fn run_exploration_fixture(relative_path: &str) -> loon_testkit::explore::ExplorationRunReport {
    let scenario = load_fixture(relative_path);
    run_background_exploration_scenario(&scenario, None, execute_background_case)
        .expect("run background exploration scenario")
}

fn execute_background_case(case: &ConcreteExplorationCase) -> Result<ExplorationExecutionOutcome> {
    if let Some(reason) = semantic_skip_reason(case) {
        return Ok(ExplorationExecutionOutcome::NotExecutable {
            reason,
            rendered_trace: None,
        });
    }

    let report = run_background_scenario_report(
        &case.concrete_scenario,
        BackgroundSimRunOptions {
            exploration_case: Some(BackgroundExplorationCaseLabel {
                case_index: case.case_index,
                seed: case.effective_seed.map(|seed| seed.0),
                fault_summary: case.fault_summary.clone(),
                permuted_action_order: case.permuted_action_order.clone(),
            }),
        },
    );

    Ok(match report.status {
        BackgroundSimRunStatus::Passed => ExplorationExecutionOutcome::Passed,
        BackgroundSimRunStatus::Failed { failure_headline } => {
            ExplorationExecutionOutcome::Failed {
                failure_headline,
                rendered_trace: report.rendered_trace,
            }
        }
        BackgroundSimRunStatus::NotExecutable { reason } => {
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

    if name == "background_explore_stale_writer_handover" {
        let handover = find_action_index(&action_kinds, "lease_handover")?;
        let publish = find_action_index(&action_kinds, "writer_publish_head")?;
        let attempt = find_action_index(&action_kinds, "writer_attempt_commit")?;
        if handover > publish || publish > attempt {
            return Some(
                "stale writer exploration requires lease_handover -> writer_publish_head -> writer_attempt_commit"
                    .to_owned(),
            );
        }
    }

    if name == "background_explore_checkpoint_publish_waits_and_succeeds" {
        let publish_positions = find_action_indices(&action_kinds, "checkpoint_publish");
        if publish_positions.len() == 2 {
            let progress = find_action_index(&action_kinds, "publish_progress")?;
            if publish_positions[0] > progress || publish_positions[1] < progress {
                return Some(
                    "background checkpoint exploration requires publish_progress between the two checkpoint_publish actions"
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

fn assert_snapshot_eq(actual: &str, expected: &str) {
    assert_eq!(actual.trim_end(), expected.trim_end());
}
