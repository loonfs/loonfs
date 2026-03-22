use crate::render::render_yaml;
use crate::scenario::Scenario;
use crate::seed::Seed;
use anyhow::{bail, Result};
use loon_sim::faults::InjectedClientFault;
use serde::Deserialize;
use serde_yaml::Value;
use std::collections::BTreeMap;

pub type ScenarioFragment = BTreeMap<String, Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplorationHarnessKind {
    UnifiedNamespace,
    ClientServer,
    Background,
    Queue,
}

impl ExplorationHarnessKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::UnifiedNamespace => "unified_namespace",
            Self::ClientServer => "client_server",
            Self::Background => "background",
            Self::Queue => "queue",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConcreteExplorationCase {
    pub case_index: usize,
    pub effective_seed: Option<Seed>,
    pub fault: Option<InjectedClientFault>,
    pub fault_summary: String,
    pub permuted_actions: Vec<ScenarioFragment>,
    pub permuted_action_order: Vec<String>,
    pub concrete_scenario: Scenario,
}

#[derive(Debug, Clone)]
pub enum ExplorationExecutionOutcome {
    Passed,
    Failed {
        failure_headline: String,
        rendered_trace: String,
    },
    NotExecutable {
        reason: String,
        rendered_trace: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplorationCaseOutcomeKind {
    Passed,
    Failed,
    NotExecutable,
}

#[derive(Debug, Clone)]
pub struct ExplorationCaseReport {
    pub case_index: usize,
    pub fault_summary: String,
    pub permuted_action_order: Vec<String>,
    pub outcome_kind: ExplorationCaseOutcomeKind,
    pub failure_headline: Option<String>,
    pub not_executable_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MinimizedSimCaseReport {
    pub failure_headline: String,
    pub removed_fault: bool,
    pub original_permuted_action_count: usize,
    pub minimized_permuted_action_count: usize,
    pub scenario: Scenario,
}

#[derive(Debug, Clone)]
pub struct FailedExplorationCaseReport {
    pub case_index: usize,
    pub fault_summary: String,
    pub permuted_action_order: Vec<String>,
    pub failure_headline: String,
    pub rendered_trace: String,
    pub concrete_scenario: Scenario,
    pub minimized: Option<MinimizedSimCaseReport>,
}

#[derive(Debug, Clone)]
pub struct ExplorationRunReport {
    pub harness_kind: ExplorationHarnessKind,
    pub scenario_name: String,
    pub effective_seed: Option<Seed>,
    pub total_candidate_cases: usize,
    pub selected_candidate_cases: usize,
    pub executed_cases: usize,
    pub skipped_cases: usize,
    pub case_reports: Vec<ExplorationCaseReport>,
    pub first_failure: Option<FailedExplorationCaseReport>,
}

impl ExplorationRunReport {
    pub fn render_summary(&self) -> String {
        let mut lines = vec![format!(
            "harness={} scenario={} seed={:?} total_candidate_cases={} selected_candidate_cases={} executed_cases={} skipped_cases={}",
            self.harness_kind.as_str(),
            self.scenario_name,
            self.effective_seed.map(|seed| seed.0),
            self.total_candidate_cases,
            self.selected_candidate_cases,
            self.executed_cases,
            self.skipped_cases
        )];

        for case in &self.case_reports {
            let status = match case.outcome_kind {
                ExplorationCaseOutcomeKind::Passed => "passed".to_owned(),
                ExplorationCaseOutcomeKind::Failed => format!(
                    "failed headline={}",
                    case.failure_headline.as_deref().unwrap_or("<unknown>")
                ),
                ExplorationCaseOutcomeKind::NotExecutable => format!(
                    "not_executable reason={}",
                    case.not_executable_reason.as_deref().unwrap_or("<unknown>")
                ),
            };
            lines.push(format!(
                "  case={} status={} fault={} permuted_actions=[{}]",
                case.case_index,
                status,
                case.fault_summary,
                case.permuted_action_order.join(" | ")
            ));
        }

        if let Some(failure) = &self.first_failure {
            lines.push(format!(
                "first_failure_case={} headline={}",
                failure.case_index, failure.failure_headline
            ));
        } else {
            lines.push("first_failure_case=<none>".to_owned());
        }

        lines.join("\n")
    }
}

impl MinimizedSimCaseReport {
    pub fn render_summary(&self) -> String {
        let yaml = render_yaml(&self.scenario).expect("render minimized scenario YAML");
        format!(
            "failure_headline={}\nremoved_fault={}\noriginal_permuted_action_count={}\nminimized_permuted_action_count={}\n[minimized]\n{}",
            self.failure_headline,
            self.removed_fault,
            self.original_permuted_action_count,
            self.minimized_permuted_action_count,
            yaml
        )
    }
}

#[derive(Debug, Clone, Deserialize)]
struct BoundedInterleavingsExploreConfig {
    strategy: String,
    max_cases: u32,
    #[serde(default)]
    permuted_actions: Vec<ScenarioFragment>,
    #[serde(default)]
    tail_actions: Vec<ScenarioFragment>,
}

impl BoundedInterleavingsExploreConfig {
    fn validate(&self) -> Result<()> {
        if self.strategy != "bounded_interleavings" {
            bail!(
                "unsupported exploration strategy `{}` (expected `bounded_interleavings`)",
                self.strategy
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct FaultVariant {
    raw: Option<ScenarioFragment>,
    parsed: Option<InjectedClientFault>,
    summary: String,
}

#[derive(Debug, Clone)]
struct MaterializedExplorationCase {
    fault: Option<InjectedClientFault>,
    fault_summary: String,
    permuted_actions: Vec<ScenarioFragment>,
    permuted_action_order: Vec<String>,
    concrete_scenario: Scenario,
}

pub fn run_unified_namespace_exploration_scenario<F>(
    scenario: &Scenario,
    seed_override: Option<Seed>,
    execute_case: F,
) -> Result<ExplorationRunReport>
where
    F: FnMut(&ConcreteExplorationCase) -> Result<ExplorationExecutionOutcome>,
{
    run_exploration_scenario(
        ExplorationHarnessKind::UnifiedNamespace,
        FaultSupport::ClientOnly,
        scenario,
        seed_override,
        execute_case,
    )
}

pub fn run_client_server_exploration_scenario<F>(
    scenario: &Scenario,
    seed_override: Option<Seed>,
    execute_case: F,
) -> Result<ExplorationRunReport>
where
    F: FnMut(&ConcreteExplorationCase) -> Result<ExplorationExecutionOutcome>,
{
    run_exploration_scenario(
        ExplorationHarnessKind::ClientServer,
        FaultSupport::ClientOnly,
        scenario,
        seed_override,
        execute_case,
    )
}

pub fn run_background_exploration_scenario<F>(
    scenario: &Scenario,
    seed_override: Option<Seed>,
    execute_case: F,
) -> Result<ExplorationRunReport>
where
    F: FnMut(&ConcreteExplorationCase) -> Result<ExplorationExecutionOutcome>,
{
    run_exploration_scenario(
        ExplorationHarnessKind::Background,
        FaultSupport::None,
        scenario,
        seed_override,
        execute_case,
    )
}

pub fn run_queue_exploration_scenario<F>(
    scenario: &Scenario,
    seed_override: Option<Seed>,
    execute_case: F,
) -> Result<ExplorationRunReport>
where
    F: FnMut(&ConcreteExplorationCase) -> Result<ExplorationExecutionOutcome>,
{
    run_exploration_scenario(
        ExplorationHarnessKind::Queue,
        FaultSupport::None,
        scenario,
        seed_override,
        execute_case,
    )
}

#[derive(Debug, Clone, Copy)]
enum FaultSupport {
    ClientOnly,
    None,
}

fn run_exploration_scenario<F>(
    harness_kind: ExplorationHarnessKind,
    fault_support: FaultSupport,
    scenario: &Scenario,
    seed_override: Option<Seed>,
    mut execute_case: F,
) -> Result<ExplorationRunReport>
where
    F: FnMut(&ConcreteExplorationCase) -> Result<ExplorationExecutionOutcome>,
{
    let mut working = scenario.clone();
    if let Some(seed) = seed_override {
        working.seed = Some(seed.0);
    }

    let explore = working
        .decode_explore::<BoundedInterleavingsExploreConfig>()?
        .ok_or_else(|| anyhow::anyhow!("sim exploration requires an `explore` section"))?;
    explore.validate()?;

    let fault_variants = build_fault_variants(&working, fault_support)?;
    let permutations = permute_indices(explore.permuted_actions.len());
    let total_candidate_cases = fault_variants.len() * permutations.len();

    let mut enumerated = Vec::new();
    let mut canonical_index = 0usize;
    for fault_variant in &fault_variants {
        for permuted_indices in &permutations {
            canonical_index += 1;
            let materialized =
                materialize_case(&working, &explore, fault_variant, permuted_indices.to_vec());
            let hash = stable_seeded_case_hash(
                working.seed.unwrap_or_default(),
                &fault_variant.summary,
                permuted_indices,
            );
            enumerated.push((hash, canonical_index, materialized));
        }
    }

    enumerated.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| {
                left.2
                    .fault_summary
                    .cmp(&right.2.fault_summary)
                    .then_with(|| {
                        left.2
                            .permuted_action_order
                            .cmp(&right.2.permuted_action_order)
                    })
            })
    });

    let selected_candidate_cases = enumerated.len().min(explore.max_cases as usize);
    enumerated.truncate(selected_candidate_cases);

    let mut case_reports = Vec::new();
    let mut executed_cases = 0usize;
    let mut skipped_cases = 0usize;
    let mut first_failure = None;

    for (position, (_, _, materialized)) in enumerated.into_iter().enumerate() {
        let case = ConcreteExplorationCase {
            case_index: position + 1,
            effective_seed: working.seed.map(Seed),
            fault: materialized.fault.clone(),
            fault_summary: materialized.fault_summary.clone(),
            permuted_actions: materialized.permuted_actions.clone(),
            permuted_action_order: materialized.permuted_action_order.clone(),
            concrete_scenario: materialized.concrete_scenario.clone(),
        };
        let outcome = execute_case(&case)?;
        match outcome {
            ExplorationExecutionOutcome::Passed => {
                executed_cases += 1;
                case_reports.push(ExplorationCaseReport {
                    case_index: case.case_index,
                    fault_summary: case.fault_summary,
                    permuted_action_order: case.permuted_action_order,
                    outcome_kind: ExplorationCaseOutcomeKind::Passed,
                    failure_headline: None,
                    not_executable_reason: None,
                });
            }
            ExplorationExecutionOutcome::NotExecutable { reason, .. } => {
                skipped_cases += 1;
                case_reports.push(ExplorationCaseReport {
                    case_index: case.case_index,
                    fault_summary: case.fault_summary,
                    permuted_action_order: case.permuted_action_order,
                    outcome_kind: ExplorationCaseOutcomeKind::NotExecutable,
                    failure_headline: None,
                    not_executable_reason: Some(reason),
                });
            }
            ExplorationExecutionOutcome::Failed {
                failure_headline,
                rendered_trace,
            } => {
                executed_cases += 1;
                let minimized = minimize_failed_case(
                    &working,
                    &explore,
                    &case,
                    &failure_headline,
                    &mut execute_case,
                )?;
                case_reports.push(ExplorationCaseReport {
                    case_index: case.case_index,
                    fault_summary: case.fault_summary.clone(),
                    permuted_action_order: case.permuted_action_order.clone(),
                    outcome_kind: ExplorationCaseOutcomeKind::Failed,
                    failure_headline: Some(failure_headline.clone()),
                    not_executable_reason: None,
                });
                first_failure = Some(FailedExplorationCaseReport {
                    case_index: case.case_index,
                    fault_summary: case.fault_summary,
                    permuted_action_order: case.permuted_action_order,
                    failure_headline,
                    rendered_trace,
                    concrete_scenario: case.concrete_scenario,
                    minimized: Some(minimized),
                });
                break;
            }
        }
    }

    Ok(ExplorationRunReport {
        harness_kind,
        scenario_name: working.name.clone(),
        effective_seed: working.seed.map(Seed),
        total_candidate_cases,
        selected_candidate_cases,
        executed_cases,
        skipped_cases,
        case_reports,
        first_failure,
    })
}

fn minimize_failed_case<F>(
    original_scenario: &Scenario,
    explore: &BoundedInterleavingsExploreConfig,
    failed_case: &ConcreteExplorationCase,
    target_headline: &str,
    execute_case: &mut F,
) -> Result<MinimizedSimCaseReport>
where
    F: FnMut(&ConcreteExplorationCase) -> Result<ExplorationExecutionOutcome>,
{
    let mut current_fault = failed_case.fault.clone();
    let mut current_fault_raw = original_scenario
        .faults
        .iter()
        .find(|raw| summarize_fault_raw(raw) == failed_case.fault_summary)
        .cloned();
    let mut removed_fault = false;
    let mut current_actions = failed_case.permuted_actions.clone();
    let original_permuted_action_count = current_actions.len();

    if current_fault_raw.is_some() {
        let candidate = build_concrete_case(
            original_scenario,
            None,
            &current_actions,
            &explore.tail_actions,
            failed_case.effective_seed,
            failed_case.case_index,
        );
        if matches_failure_headline(execute_case(&candidate)?, target_headline) {
            current_fault = None;
            current_fault_raw = None;
            removed_fault = true;
        }
    }

    loop {
        let mut changed = false;
        for remove_index in 0..current_actions.len() {
            let mut candidate_actions = current_actions.clone();
            candidate_actions.remove(remove_index);
            let candidate = build_concrete_case(
                original_scenario,
                current_fault.clone(),
                &candidate_actions,
                &explore.tail_actions,
                failed_case.effective_seed,
                failed_case.case_index,
            );
            if matches_failure_headline(execute_case(&candidate)?, target_headline) {
                current_actions = candidate_actions;
                changed = true;
                break;
            }
        }
        if !changed {
            break;
        }
    }

    let mut scenario = build_scenario_from_case(
        original_scenario,
        current_fault_raw.clone(),
        &current_actions,
        &explore.tail_actions,
        failed_case.effective_seed,
    );
    scenario.name = format!("{}_minimized", original_scenario.name);

    Ok(MinimizedSimCaseReport {
        failure_headline: target_headline.to_owned(),
        removed_fault,
        original_permuted_action_count,
        minimized_permuted_action_count: current_actions.len(),
        scenario,
    })
}

fn matches_failure_headline(outcome: ExplorationExecutionOutcome, target_headline: &str) -> bool {
    match outcome {
        ExplorationExecutionOutcome::Failed {
            failure_headline, ..
        } => failure_signature(&failure_headline) == failure_signature(target_headline),
        ExplorationExecutionOutcome::Passed | ExplorationExecutionOutcome::NotExecutable { .. } => {
            false
        }
    }
}

fn build_fault_variants(
    scenario: &Scenario,
    fault_support: FaultSupport,
) -> Result<Vec<FaultVariant>> {
    if matches!(fault_support, FaultSupport::None) && !scenario.faults.is_empty() {
        bail!("this exploration harness does not support `faults`");
    }
    let parsed_faults: Vec<InjectedClientFault> = scenario.decode_faults()?;
    let mut variants = vec![FaultVariant {
        raw: None,
        parsed: None,
        summary: "none".to_owned(),
    }];

    for (raw, parsed) in scenario.faults.iter().cloned().zip(parsed_faults) {
        variants.push(FaultVariant {
            raw: Some(raw),
            summary: summarize_fault(&parsed),
            parsed: Some(parsed),
        });
    }

    Ok(variants)
}

fn materialize_case(
    original_scenario: &Scenario,
    explore: &BoundedInterleavingsExploreConfig,
    fault_variant: &FaultVariant,
    permuted_indices: Vec<usize>,
) -> MaterializedExplorationCase {
    let permuted_actions = permuted_indices
        .iter()
        .map(|index| explore.permuted_actions[*index].clone())
        .collect::<Vec<_>>();
    let permuted_action_order = permuted_actions
        .iter()
        .map(summarize_action)
        .collect::<Vec<_>>();
    let concrete_scenario = build_scenario_from_case(
        original_scenario,
        fault_variant.raw.clone(),
        &permuted_actions,
        &explore.tail_actions,
        original_scenario.seed.map(Seed),
    );

    MaterializedExplorationCase {
        fault: fault_variant.parsed.clone(),
        fault_summary: fault_variant.summary.clone(),
        permuted_actions,
        permuted_action_order,
        concrete_scenario,
    }
}

fn build_concrete_case(
    original_scenario: &Scenario,
    fault: Option<InjectedClientFault>,
    permuted_actions: &[ScenarioFragment],
    tail_actions: &[ScenarioFragment],
    effective_seed: Option<Seed>,
    case_index: usize,
) -> ConcreteExplorationCase {
    let fault_summary = fault
        .as_ref()
        .map(summarize_fault)
        .unwrap_or_else(|| "none".to_owned());
    let concrete_scenario = build_scenario_from_case(
        original_scenario,
        fault.as_ref().map(|fault| encode_fault(fault)),
        permuted_actions,
        tail_actions,
        effective_seed,
    );

    ConcreteExplorationCase {
        case_index,
        effective_seed,
        fault,
        fault_summary,
        permuted_actions: permuted_actions.to_vec(),
        permuted_action_order: permuted_actions.iter().map(summarize_action).collect(),
        concrete_scenario,
    }
}

fn build_scenario_from_case(
    original_scenario: &Scenario,
    fault_raw: Option<ScenarioFragment>,
    permuted_actions: &[ScenarioFragment],
    tail_actions: &[ScenarioFragment],
    effective_seed: Option<Seed>,
) -> Scenario {
    let mut scenario = original_scenario.clone();
    scenario.seed = effective_seed.map(|seed| seed.0);
    scenario.actions = original_scenario.actions.clone();
    scenario.actions.extend_from_slice(permuted_actions);
    scenario.actions.extend_from_slice(tail_actions);
    scenario.faults = fault_raw.into_iter().collect();
    scenario.explore = None;
    scenario
}

fn permute_indices(len: usize) -> Vec<Vec<usize>> {
    if len == 0 {
        return vec![Vec::new()];
    }
    let mut indices = (0..len).collect::<Vec<_>>();
    let mut permutations = Vec::new();
    permute_recursive(0, &mut indices, &mut permutations);
    permutations
}

fn permute_recursive(start: usize, indices: &mut Vec<usize>, output: &mut Vec<Vec<usize>>) {
    if start == indices.len() {
        output.push(indices.clone());
        return;
    }
    for current in start..indices.len() {
        indices.swap(start, current);
        permute_recursive(start + 1, indices, output);
        indices.swap(start, current);
    }
}

fn summarize_action(action: &ScenarioFragment) -> String {
    serde_json::to_string(action)
        .unwrap_or_else(|_| serde_yaml::to_string(action).unwrap_or_else(|_| "<action>".to_owned()))
        .replace('\n', " ")
        .trim()
        .to_owned()
}

fn summarize_fault_raw(fault: &ScenarioFragment) -> String {
    let parsed: InjectedClientFault = serde_yaml::from_value(Value::Mapping(
        fault
            .iter()
            .map(|(key, value)| (Value::String(key.clone()), value.clone()))
            .collect(),
    ))
    .expect("raw fault should decode");
    summarize_fault(&parsed)
}

fn summarize_fault(fault: &InjectedClientFault) -> String {
    match fault {
        InjectedClientFault::CrashAfterStepOnce { checkpoint_id } => {
            format!("crash_after_step_once checkpoint_id={checkpoint_id}")
        }
        InjectedClientFault::StoreErrorOnce {
            operation,
            key_contains,
            error_kind,
        } => format!(
            "store_error_once operation={operation:?} key_contains={key_contains} error_kind={error_kind:?}"
        ),
        InjectedClientFault::DispatchErrorOnce {
            checkpoint_id,
            message,
        } => {
            format!("dispatch_error_once checkpoint_id={checkpoint_id} message={message}")
        }
        InjectedClientFault::LocalApplyErrorOnce {
            operation,
            path_contains,
        } => {
            format!("local_apply_error_once operation={operation} path_contains={path_contains}")
        }
    }
}

fn encode_fault(fault: &InjectedClientFault) -> ScenarioFragment {
    let value = serde_yaml::to_value(fault).expect("encode fault to YAML value");
    match value {
        Value::Mapping(mapping) => mapping
            .into_iter()
            .map(|(key, value)| {
                let key = key
                    .as_str()
                    .expect("fault envelope keys should serialize as strings")
                    .to_owned();
                (key, value)
            })
            .collect(),
        other => panic!("fault should serialize as a mapping, got {other:?}"),
    }
}

fn stable_seeded_case_hash(seed: u64, fault_summary: &str, permuted_indices: &[usize]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in seed.to_le_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    for byte in fault_summary.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    for index in permuted_indices {
        for byte in (*index as u64).to_le_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    hash
}

fn failure_signature(error: &str) -> &str {
    error.lines().next().unwrap_or(error)
}

#[cfg(test)]
mod tests {
    use super::{
        build_concrete_case, minimize_failed_case, run_background_exploration_scenario,
        run_queue_exploration_scenario, run_unified_namespace_exploration_scenario,
        BoundedInterleavingsExploreConfig, ConcreteExplorationCase, ExplorationCaseOutcomeKind,
        ExplorationExecutionOutcome, ExplorationHarnessKind, InjectedClientFault,
    };
    use crate::scenario::{Scenario, ScenarioKind};
    use crate::seed::Seed;
    use serde_yaml::from_str;

    fn synthetic_scenario() -> Scenario {
        from_str(
            r#"
schema_version: 1
scenario_kind: sim
name: synthetic_explore
seed: 42
initial: {}
actions:
  - prefix_action: true
faults:
  - kind: dispatch_error_once
    checkpoint_id: dispatch.client.before_request
    message: boom
explore:
  strategy: bounded_interleavings
  max_cases: 6
  permuted_actions:
    - alpha: true
    - beta: true
  tail_actions:
    - omega: true
expect: {}
"#,
        )
        .expect("parse synthetic exploration scenario")
    }

    fn case_key(case: &ConcreteExplorationCase) -> String {
        format!(
            "{}|{}",
            case.fault_summary,
            case.permuted_action_order.join(" -> ")
        )
    }

    #[test]
    fn exploration_candidate_order_is_seed_stable() {
        let scenario = synthetic_scenario();
        let first = run_unified_namespace_exploration_scenario(&scenario, Some(Seed(17)), |case| {
            let _ = case;
            Ok(ExplorationExecutionOutcome::Passed)
        })
        .expect("run first exploration");
        let second =
            run_unified_namespace_exploration_scenario(&scenario, Some(Seed(17)), |case| {
                let _ = case;
                Ok(ExplorationExecutionOutcome::Passed)
            })
            .expect("run second exploration");

        let first_order = first
            .case_reports
            .iter()
            .map(|report| {
                format!(
                    "{}|{}",
                    report.fault_summary,
                    report.permuted_action_order.join(" -> ")
                )
            })
            .collect::<Vec<_>>();
        let second_order = second
            .case_reports
            .iter()
            .map(|report| {
                format!(
                    "{}|{}",
                    report.fault_summary,
                    report.permuted_action_order.join(" -> ")
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(first_order, second_order);
        assert_eq!(first.harness_kind, ExplorationHarnessKind::UnifiedNamespace);
    }

    #[test]
    fn exploration_skips_non_executable_cases_without_failing_run() {
        let scenario = synthetic_scenario();
        let report = run_unified_namespace_exploration_scenario(&scenario, None, |case| {
            if case.permuted_action_order[0].contains("beta") {
                return Ok(ExplorationExecutionOutcome::NotExecutable {
                    reason: "beta cannot lead".to_owned(),
                    rendered_trace: None,
                });
            }
            Ok(ExplorationExecutionOutcome::Passed)
        })
        .expect("run exploration");

        assert!(report.first_failure.is_none());
        assert!(report.skipped_cases > 0);
        assert!(report
            .case_reports
            .iter()
            .any(|case| case.outcome_kind == ExplorationCaseOutcomeKind::NotExecutable));
    }

    #[test]
    fn exploration_max_cases_truncation_is_deterministic() {
        let mut scenario = synthetic_scenario();
        scenario.explore = Some(
            serde_yaml::from_str::<serde_yaml::Mapping>(
                r#"
strategy: bounded_interleavings
max_cases: 2
permuted_actions:
  - alpha: true
  - beta: true
  - gamma: true
"#,
            )
            .expect("parse mapping")
            .into_iter()
            .map(|(key, value)| (key.as_str().expect("string key").to_owned(), value))
            .collect(),
        );

        let report =
            run_unified_namespace_exploration_scenario(&scenario, Some(Seed(99)), |case| {
                let _ = case;
                Ok(ExplorationExecutionOutcome::Passed)
            })
            .expect("run exploration");

        assert_eq!(report.selected_candidate_cases, 2);
        assert_eq!(report.case_reports.len(), 2);
    }

    #[test]
    fn exploration_first_failure_selection_is_seed_stable() {
        let scenario = synthetic_scenario();
        let target = "none|{\"alpha\":true} -> {\"beta\":true}";
        let first = run_unified_namespace_exploration_scenario(&scenario, Some(Seed(3)), |case| {
            if case_key(case) == target {
                return Ok(ExplorationExecutionOutcome::Failed {
                    failure_headline: "synthetic failure".to_owned(),
                    rendered_trace: "trace".to_owned(),
                });
            }
            Ok(ExplorationExecutionOutcome::Passed)
        })
        .expect("run first exploration");
        let second = run_unified_namespace_exploration_scenario(&scenario, Some(Seed(3)), |case| {
            if case_key(case) == target {
                return Ok(ExplorationExecutionOutcome::Failed {
                    failure_headline: "synthetic failure".to_owned(),
                    rendered_trace: "trace".to_owned(),
                });
            }
            Ok(ExplorationExecutionOutcome::Passed)
        })
        .expect("run second exploration");

        assert_eq!(
            first
                .first_failure
                .as_ref()
                .map(|failure| failure.case_index),
            second
                .first_failure
                .as_ref()
                .map(|failure| failure.case_index)
        );
    }

    #[test]
    fn exploration_minimizer_emits_plain_sim_scenario() {
        let scenario = synthetic_scenario();
        let report = run_unified_namespace_exploration_scenario(&scenario, None, |case| {
            if case
                .permuted_action_order
                .iter()
                .any(|action| action.contains("beta"))
            {
                return Ok(ExplorationExecutionOutcome::Failed {
                    failure_headline: "synthetic failure".to_owned(),
                    rendered_trace: "trace".to_owned(),
                });
            }
            Ok(ExplorationExecutionOutcome::Passed)
        })
        .expect("run exploration");

        let minimized = report
            .first_failure
            .as_ref()
            .and_then(|failure| failure.minimized.as_ref())
            .expect("minimized report should exist");

        assert!(minimized.scenario.explore.is_none());
        assert_eq!(minimized.scenario.scenario_kind, ScenarioKind::Sim);
        assert!(minimized.scenario.actions.len() >= 2);
    }

    #[test]
    fn exploration_minimizer_can_remove_irrelevant_fault() {
        let scenario = synthetic_scenario();
        let explore = scenario
            .decode_explore::<BoundedInterleavingsExploreConfig>()
            .expect("decode explore config")
            .expect("explore config should exist");
        let fault = scenario
            .decode_faults::<InjectedClientFault>()
            .expect("decode faults")
            .into_iter()
            .next()
            .expect("synthetic scenario should include one fault");
        let failed_case = build_concrete_case(
            &scenario,
            Some(fault),
            &explore.permuted_actions,
            &explore.tail_actions,
            scenario.seed.map(Seed),
            1,
        );
        let minimized = minimize_failed_case(
            &scenario,
            &explore,
            &failed_case,
            "alpha failure",
            &mut |case| {
                if case
                    .permuted_action_order
                    .iter()
                    .any(|action| action.contains("alpha"))
                {
                    return Ok(ExplorationExecutionOutcome::Failed {
                        failure_headline: "alpha failure".to_owned(),
                        rendered_trace: "trace".to_owned(),
                    });
                }
                Ok(ExplorationExecutionOutcome::Passed)
            },
        )
        .expect("minimize failing exploration case");

        assert!(minimized.removed_fault);
    }

    #[test]
    fn background_exploration_rejects_fault_pools() {
        let scenario = synthetic_scenario();
        let error = run_background_exploration_scenario(&scenario, None, |_case| {
            Ok(ExplorationExecutionOutcome::Passed)
        })
        .expect_err("background exploration should reject faults");

        assert!(error.to_string().contains("does not support `faults`"));
    }

    #[test]
    fn queue_exploration_rejects_fault_pools() {
        let scenario = synthetic_scenario();
        let error = run_queue_exploration_scenario(&scenario, None, |_case| {
            Ok(ExplorationExecutionOutcome::Passed)
        })
        .expect_err("queue exploration should reject faults");

        assert!(error.to_string().contains("does not support `faults`"));
    }
}
