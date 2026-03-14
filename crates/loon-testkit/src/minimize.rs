use crate::replay::{run_replay_scenario, ReplayHarnessKind};
use crate::scenario::Scenario;
use crate::seed::Seed;
use anyhow::{bail, Result};

#[derive(Debug, Clone)]
pub struct MinimizedScenarioReport {
    pub scenario_name: String,
    pub effective_seed: Option<Seed>,
    pub harness_kind: ReplayHarnessKind,
    pub original_failure: String,
    pub minimized_failure: String,
    pub original_wal_objects: usize,
    pub minimized_wal_objects: usize,
    pub scenario: Scenario,
}

pub fn minimize_replay_scenario(
    scenario: &Scenario,
    seed_override: Option<Seed>,
) -> Result<MinimizedScenarioReport> {
    let mut baseline = scenario.clone();
    if let Some(seed) = seed_override {
        baseline.seed = Some(seed.0);
    }

    let baseline_error = run_replay_scenario(&baseline, None).err().ok_or_else(|| {
        anyhow::anyhow!("minimize-case requires a replay scenario that currently fails")
    })?;
    let baseline_failure = baseline_error.to_string();
    let baseline_signature = failure_signature(&baseline_failure);
    let harness_kind = replay_harness_kind(&baseline)?;
    let original_wal_objects = wal_object_count(&baseline)?;

    let mut best = baseline.clone();
    let mut minimized_failure = baseline_failure.clone();

    for keep in 1..original_wal_objects {
        let candidate = scenario_with_wal_prefix(&baseline, keep)?;
        match run_replay_scenario(&candidate, None) {
            Ok(_) => continue,
            Err(error) => {
                let candidate_failure = error.to_string();
                if failure_signature(&candidate_failure) == baseline_signature {
                    best = candidate;
                    minimized_failure = candidate_failure;
                    break;
                }
            }
        }
    }

    let minimized_wal_objects = wal_object_count(&best)?;

    Ok(MinimizedScenarioReport {
        scenario_name: best.name.clone(),
        effective_seed: best.seed.map(Seed),
        harness_kind,
        original_failure: baseline_failure,
        minimized_failure,
        original_wal_objects,
        minimized_wal_objects,
        scenario: best,
    })
}

fn replay_harness_kind(scenario: &Scenario) -> Result<ReplayHarnessKind> {
    for action in &scenario.actions {
        if action
            .get("replay_wal_tail")
            .and_then(serde_yaml::Value::as_bool)
            == Some(true)
        {
            return Ok(ReplayHarnessKind::WalTail);
        }
        if action
            .get("replay_from_checkpoint_and_wal_tail")
            .and_then(serde_yaml::Value::as_bool)
            == Some(true)
        {
            return Ok(ReplayHarnessKind::CheckpointAndWalTail);
        }
    }

    bail!("minimize-case currently supports replay fixtures only");
}

fn wal_object_count(scenario: &Scenario) -> Result<usize> {
    scenario
        .initial
        .get("wal_objects")
        .and_then(serde_yaml::Value::as_sequence)
        .map(|objects| objects.len())
        .ok_or_else(|| anyhow::anyhow!("replay fixture is missing initial.wal_objects"))
}

fn scenario_with_wal_prefix(scenario: &Scenario, keep: usize) -> Result<Scenario> {
    let mut minimized = scenario.clone();
    let wal_objects = minimized
        .initial
        .get_mut("wal_objects")
        .and_then(serde_yaml::Value::as_sequence_mut)
        .ok_or_else(|| anyhow::anyhow!("replay fixture is missing initial.wal_objects"))?;
    wal_objects.truncate(keep);
    Ok(minimized)
}

fn failure_signature(error: &str) -> &str {
    error.lines().next().unwrap_or(error)
}

#[cfg(test)]
mod tests {
    use super::minimize_replay_scenario;
    use crate::fixtures::load_fixture;

    #[test]
    fn minimize_replay_scenario_keeps_same_failure_headline() {
        let scenario = load_fixture("native/wal_tail_replay_seq_gap_minimize_case.yaml");

        let report = minimize_replay_scenario(&scenario, None).expect("minimize replay fixture");

        assert_eq!(report.original_wal_objects, 4);
        assert_eq!(report.minimized_wal_objects, 2);
        assert_eq!(
            report.original_failure.lines().next(),
            report.minimized_failure.lines().next()
        );
    }
}
