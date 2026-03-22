#[path = "common/queue_scheduler_support.rs"]
mod queue_scheduler_support;

use anyhow::Result;
use loon_testkit::explore::{
    run_queue_exploration_scenario, ConcreteExplorationCase, ExplorationExecutionOutcome,
};
use loon_testkit::fixtures::load_fixture;
use queue_scheduler_support::{
    run_queue_scenario_report, QueueExplorationCaseLabel, QueueSimRunOptions, QueueSimRunStatus,
};

#[test]
fn queue_claim_timeout_exploration_is_deterministic() {
    let report = run_exploration_fixture("sim/queue_explore_claim_timeout_then_steal.yaml");
    assert!(
        report.first_failure.is_none(),
        "{}",
        report.render_summary()
    );
    assert_snapshot_eq(
        &report.render_summary(),
        include_str!(
            "../../../tests/snapshots/sim-explore/queue_explore_claim_timeout_then_steal.txt"
        ),
    );
}

#[test]
fn queue_lost_enqueue_repair_exploration_passes() {
    let report = run_exploration_fixture("sim/queue_explore_lost_enqueue_repair.yaml");
    assert!(
        report.first_failure.is_none(),
        "{}",
        report.render_summary()
    );
    assert!(report.executed_cases > 0);
}

fn run_exploration_fixture(relative_path: &str) -> loon_testkit::explore::ExplorationRunReport {
    let scenario = load_fixture(relative_path);
    run_queue_exploration_scenario(&scenario, None, execute_queue_case)
        .expect("run queue exploration scenario")
}

fn execute_queue_case(case: &ConcreteExplorationCase) -> Result<ExplorationExecutionOutcome> {
    if let Some(reason) = semantic_skip_reason(case) {
        return Ok(ExplorationExecutionOutcome::NotExecutable {
            reason,
            rendered_trace: None,
        });
    }

    let report = run_queue_scenario_report(
        &case.concrete_scenario,
        QueueSimRunOptions {
            exploration_case: Some(QueueExplorationCaseLabel {
                case_index: case.case_index,
                seed: case.effective_seed.map(|seed| seed.0),
                fault_summary: case.fault_summary.clone(),
                permuted_action_order: case.permuted_action_order.clone(),
            }),
        },
    );

    Ok(match report.status {
        QueueSimRunStatus::Passed => ExplorationExecutionOutcome::Passed,
        QueueSimRunStatus::Failed { failure_headline } => ExplorationExecutionOutcome::Failed {
            failure_headline,
            rendered_trace: report.rendered_trace,
        },
        QueueSimRunStatus::NotExecutable { reason } => ExplorationExecutionOutcome::NotExecutable {
            reason,
            rendered_trace: Some(report.rendered_trace),
        },
    })
}

fn semantic_skip_reason(case: &ConcreteExplorationCase) -> Option<String> {
    let name = case.concrete_scenario.name.as_str();

    if name == "queue_explore_claim_timeout_then_steal" {
        let claim = find_action_index_by(case, "worker_claim", |action| {
            action_field(action, "worker").is_some_and(|worker| worker == "worker-b")
        })?;
        let stale_complete = find_action_index_by(case, "worker_complete", |action| {
            action_field(action, "worker").is_some_and(|worker| worker == "worker-a")
        })?;
        let winning_complete = find_action_index_by(case, "worker_complete", |action| {
            action_field(action, "worker").is_some_and(|worker| worker == "worker-b")
        })?;
        if claim > stale_complete || stale_complete > winning_complete {
            return Some(
                "queue steal exploration requires worker_claim -> stale worker_complete -> winning worker_complete"
                    .to_owned(),
            );
        }
    }

    None
}

fn find_action_index_by<F>(
    case: &ConcreteExplorationCase,
    target: &str,
    predicate: F,
) -> Option<usize>
where
    F: Fn(&serde_yaml::Value) -> bool,
{
    case.permuted_actions
        .iter()
        .enumerate()
        .find_map(|(index, action)| {
            let (kind, payload) = action.iter().next()?;
            (kind == target && predicate(payload)).then_some(index)
        })
}

fn action_field<'a>(action: &'a serde_yaml::Value, key: &str) -> Option<&'a str> {
    match action {
        serde_yaml::Value::Mapping(mapping) => mapping
            .get(serde_yaml::Value::String(key.to_owned()))
            .and_then(serde_yaml::Value::as_str),
        _ => None,
    }
}

fn assert_snapshot_eq(actual: &str, expected: &str) {
    assert_eq!(actual.trim_end(), expected.trim_end());
}
