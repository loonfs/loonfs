use loon_server::core::progress::{
    publish_progress, read_progress_object, LoadedProgressObject, ProgressLoadError,
    ProgressPublishOutcome,
};
use loon_server::objectstore::fs::LocalFsStore;
use loon_server::objectstore::keys::derived_progress;
use loon_server::objectstore::ObjectStore;
use loon_testkit::invariants::{
    evaluate_progress_publish_invariants, BackgroundWorkInvariantReport, ProgressInvariantSnapshot,
    ProgressPublishInvariantInputs, ProgressPublishOutcomeKind,
};
use loon_testkit::model::{ModelNamespace, ModelProgressObject};
use loon_testkit::render::render_trace;
use loon_testkit::scenario::Scenario;
use loon_server::core::control_types::{ControlObjectEnvelope, ControlObjectKind, ProgressState};
use loon_types::{ChangeSeq, NamespaceId};
use serde::Deserialize;

const TEST_WRITER_VERSION: &str = "loon-testkit-differential";

#[test]
fn progress_publish_fixture_matches_model_and_core() {
    let report = run_fixture_report("native/progress_publish_advances_monotonically.yaml");
    assert!(!report.rendered_trace.is_empty());
}

#[test]
fn progress_publish_snapshot_matches_checked_in_artifact() {
    let report = run_fixture_report("native/progress_publish_advances_monotonically.yaml");
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../../tests/snapshots/progress-differential/native/progress_publish_advances_monotonically.txt"
        )
    );
}

struct ProgressFixtureRunReport {
    rendered_trace: String,
}

fn run_fixture_report(relative_path: &str) -> ProgressFixtureRunReport {
    let scenario = load_fixture(relative_path);
    let initial: ProgressFixtureState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<ProgressActionEnvelope> = scenario.decode_actions().expect("decode actions");
    let expect: ProgressFixtureExpect = scenario.decode_expect().expect("decode expectations");

    let (namespace_id, work_class) = scenario_target(&initial, &actions, &expect);
    let model_namespace = ModelNamespace::new(namespace_id.clone());
    let mut model_progress = initial.progress.as_ref().map(model_progress_from_fixture);

    let temp_dir = TestDir::new("progress-differential");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    if let Some(progress) = &initial.progress {
        seed_progress_object(&store, progress);
    }
    let mut observed_invariants = Vec::new();

    let mut trace = vec![format!(
        "initial model={:?} core={:?}",
        model_progress.as_ref().map(snapshot_from_model_progress),
        read_core_snapshot(&store, &namespace_id, &work_class).expect("read initial core progress")
    )];

    assert_states_match(
        &scenario,
        &trace,
        0,
        model_progress.as_ref().map(snapshot_from_model_progress),
        read_core_snapshot(&store, &namespace_id, &work_class).expect("read initial core progress"),
    );

    for (index, action) in actions.iter().enumerate() {
        let action = &action.publish_progress;

        assert_eq!(
            &action.namespace_id, &namespace_id,
            "progress differential harness expects one namespace per scenario"
        );
        assert_eq!(
            &action.work_class, &work_class,
            "progress differential harness expects one work class per scenario"
        );

        let before_model = model_progress.as_ref().map(snapshot_from_model_progress);
        let expected_model = model_namespace
            .publish_progress(model_progress.as_ref(), &work_class, action.through_seq)
            .expect("model publish progress should succeed");
        let expected_model_outcome =
            classify_model_outcome(before_model.as_ref(), &expected_model, action.through_seq);
        model_progress = Some(expected_model);

        let core_outcome = publish_progress(
            &store,
            &namespace_id,
            &work_class,
            action.through_seq,
            TEST_WRITER_VERSION,
        )
        .expect("core publish progress should succeed");
        let actual_core_outcome = classify_core_outcome(&core_outcome);
        let after_model = model_progress.as_ref().map(snapshot_from_model_progress);
        let after_core = read_core_snapshot(&store, &namespace_id, &work_class)
            .expect("read core progress after publish");

        trace.push(format!(
            "step={} action=publish_progress(namespace_id={}, work_class={}, through_seq={}) model_outcome={:?} core_outcome={:?} model_state={:?} core_state={:?}",
            index + 1,
            action.namespace_id.as_str(),
            action.work_class,
            action.through_seq.0,
            expected_model_outcome,
            actual_core_outcome,
            after_model,
            after_core
        ));

        if expected_model_outcome != actual_core_outcome {
            panic!(
                "progress differential outcome mismatch at step {}:\n{}",
                index + 1,
                render_trace(&scenario, &trace)
            );
        }

        assert_states_match(&scenario, &trace, index + 1, after_model, after_core);

        let model_report = evaluate_progress_publish_invariants(ProgressPublishInvariantInputs {
            expected_namespace: &namespace_id,
            expected_work_class: &work_class,
            before_through_seq: before_model.as_ref().map(|snapshot| snapshot.through_seq),
            requested_through_seq: action.through_seq,
            outcome: progress_outcome_kind(&expected_model_outcome),
            after_progress: &progress_snapshot_from_model(
                model_progress
                    .as_ref()
                    .expect("model progress should exist after publish"),
            ),
        });
        let core_report = evaluate_progress_publish_invariants(ProgressPublishInvariantInputs {
            expected_namespace: &namespace_id,
            expected_work_class: &work_class,
            before_through_seq: before_model.as_ref().map(|snapshot| snapshot.through_seq),
            requested_through_seq: action.through_seq,
            outcome: progress_outcome_kind(&actual_core_outcome),
            after_progress: &progress_snapshot_from_core_outcome(&core_outcome),
        });
        assert_reports_pass(
            &scenario,
            &mut trace,
            &model_report,
            &core_report,
            index + 1,
            &mut observed_invariants,
        );
    }

    let expected_progress = expect.progress.as_ref().map(snapshot_from_fixture_progress);
    let actual_model = model_progress.as_ref().map(snapshot_from_model_progress);
    let actual_core =
        read_core_snapshot(&store, &namespace_id, &work_class).expect("read final core progress");

    trace.push(format!(
        "final expected={:?} model={:?} core={:?}",
        expected_progress, actual_model, actual_core
    ));

    if actual_model != expected_progress || actual_core != expected_progress {
        panic!(
            "progress differential final expectation mismatch:\n{}",
            render_trace(&scenario, &trace)
        );
    }

    if !expect.invariants.is_empty() {
        for invariant in &expect.invariants {
            assert!(
                observed_invariants.iter().any(|value| value == invariant),
                "missing expected invariant `{invariant}`:\n{}",
                render_trace(&scenario, &trace)
            );
        }
    }

    ProgressFixtureRunReport {
        rendered_trace: render_trace(&scenario, &trace),
    }
}

#[derive(Debug, Deserialize)]
struct ProgressFixtureState {
    #[serde(default)]
    progress: Option<FixtureProgressObject>,
}

#[derive(Debug, Deserialize)]
struct ProgressFixtureExpect {
    #[serde(default)]
    progress: Option<FixtureProgressObject>,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureProgressObject {
    key: String,
    payload: ProgressState,
}

#[derive(Debug, Deserialize)]
struct ProgressActionEnvelope {
    publish_progress: ProgressAction,
}

#[derive(Debug, Deserialize)]
struct ProgressAction {
    namespace_id: NamespaceId,
    work_class: String,
    through_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProgressSnapshot {
    object_key: String,
    namespace_id: NamespaceId,
    work_class: String,
    through_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProgressOutcome {
    Created { through_seq: ChangeSeq },
    Advanced { through_seq: ChangeSeq },
    NoChange { through_seq: ChangeSeq },
}

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
}

fn scenario_target(
    initial: &ProgressFixtureState,
    actions: &[ProgressActionEnvelope],
    expect: &ProgressFixtureExpect,
) -> (NamespaceId, String) {
    let mut namespace_id = None;
    let mut work_class = None;

    if let Some(progress) = &initial.progress {
        observe_target(
            &progress.payload.namespace_id,
            &progress.payload.work_class,
            &mut namespace_id,
            &mut work_class,
        );
    }

    for action in actions {
        observe_target(
            &action.publish_progress.namespace_id,
            &action.publish_progress.work_class,
            &mut namespace_id,
            &mut work_class,
        );
    }

    if let Some(progress) = &expect.progress {
        observe_target(
            &progress.payload.namespace_id,
            &progress.payload.work_class,
            &mut namespace_id,
            &mut work_class,
        );
    }

    (
        namespace_id.expect("scenario should contain a namespace id"),
        work_class.expect("scenario should contain a work class"),
    )
}

fn observe_target(
    candidate_namespace_id: &NamespaceId,
    candidate_work_class: &str,
    namespace_id: &mut Option<NamespaceId>,
    work_class: &mut Option<String>,
) {
    match namespace_id {
        Some(current) => assert_eq!(
            current, candidate_namespace_id,
            "progress differential harness expects one namespace per scenario"
        ),
        None => *namespace_id = Some(candidate_namespace_id.clone()),
    }

    match work_class {
        Some(current) => assert_eq!(
            current, candidate_work_class,
            "progress differential harness expects one work class per scenario"
        ),
        None => *work_class = Some(candidate_work_class.to_owned()),
    }
}

fn model_progress_from_fixture(progress: &FixtureProgressObject) -> ModelProgressObject {
    let expected_key = derived_progress(
        progress.payload.namespace_id.as_str(),
        &progress.payload.work_class,
    );
    assert_eq!(
        progress.key, expected_key,
        "fixture progress object key should match namespace/work-class"
    );

    ModelProgressObject {
        namespace_id: progress.payload.namespace_id.clone(),
        work_class: progress.payload.work_class.clone(),
        through_seq: progress.payload.through_seq,
    }
}

fn seed_progress_object(store: &LocalFsStore, progress: &FixtureProgressObject) {
    let envelope = ControlObjectEnvelope::from_state(
        ControlObjectKind::NamespaceProgress,
        TEST_WRITER_VERSION,
        progress.payload.clone(),
    )
    .expect("build progress envelope");

    store
        .put_if_absent(
            &progress.key,
            &serde_json::to_vec(&envelope).expect("encode progress envelope"),
        )
        .expect("seed progress object");
}

fn classify_model_outcome(
    before: Option<&ProgressSnapshot>,
    after: &ModelProgressObject,
    requested_through_seq: ChangeSeq,
) -> ProgressOutcome {
    match before {
        None => ProgressOutcome::Created {
            through_seq: after.through_seq,
        },
        Some(before) if before.through_seq >= requested_through_seq => ProgressOutcome::NoChange {
            through_seq: before.through_seq,
        },
        Some(_) => ProgressOutcome::Advanced {
            through_seq: after.through_seq,
        },
    }
}

fn classify_core_outcome(outcome: &ProgressPublishOutcome) -> ProgressOutcome {
    match outcome {
        ProgressPublishOutcome::Created(published) => ProgressOutcome::Created {
            through_seq: published.progress.envelope.state.through_seq,
        },
        ProgressPublishOutcome::Advanced(published) => ProgressOutcome::Advanced {
            through_seq: published.progress.envelope.state.through_seq,
        },
        ProgressPublishOutcome::NoChange(current) => ProgressOutcome::NoChange {
            through_seq: current.envelope.state.through_seq,
        },
    }
}

fn read_core_snapshot(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    work_class: &str,
) -> Result<Option<ProgressSnapshot>, ProgressLoadError> {
    match read_progress_object(store, namespace_id, work_class) {
        Ok(loaded) => Ok(Some(snapshot_from_loaded_progress(&loaded))),
        Err(ProgressLoadError::MissingObject { .. }) => Ok(None),
        Err(err) => Err(err),
    }
}

fn progress_outcome_kind(outcome: &ProgressOutcome) -> ProgressPublishOutcomeKind {
    match outcome {
        ProgressOutcome::Created { .. } => ProgressPublishOutcomeKind::Created,
        ProgressOutcome::Advanced { .. } => ProgressPublishOutcomeKind::Advanced,
        ProgressOutcome::NoChange { .. } => ProgressPublishOutcomeKind::NoChange,
    }
}

fn snapshot_from_fixture_progress(progress: &FixtureProgressObject) -> ProgressSnapshot {
    ProgressSnapshot {
        object_key: progress.key.clone(),
        namespace_id: progress.payload.namespace_id.clone(),
        work_class: progress.payload.work_class.clone(),
        through_seq: progress.payload.through_seq,
    }
}

fn snapshot_from_model_progress(progress: &ModelProgressObject) -> ProgressSnapshot {
    ProgressSnapshot {
        object_key: derived_progress(progress.namespace_id.as_str(), &progress.work_class),
        namespace_id: progress.namespace_id.clone(),
        work_class: progress.work_class.clone(),
        through_seq: progress.through_seq,
    }
}

fn snapshot_from_loaded_progress(progress: &LoadedProgressObject) -> ProgressSnapshot {
    ProgressSnapshot {
        object_key: progress.object_key.clone(),
        namespace_id: progress.envelope.state.namespace_id.clone(),
        work_class: progress.envelope.state.work_class.clone(),
        through_seq: progress.envelope.state.through_seq,
    }
}

fn progress_snapshot_from_model(progress: &ModelProgressObject) -> ProgressInvariantSnapshot {
    ProgressInvariantSnapshot {
        object_key: derived_progress(progress.namespace_id.as_str(), &progress.work_class),
        namespace_id: progress.namespace_id.clone(),
        work_class: progress.work_class.clone(),
        through_seq: progress.through_seq,
        payload_checksum_valid: true,
    }
}

fn progress_snapshot_from_core_outcome(
    outcome: &ProgressPublishOutcome,
) -> ProgressInvariantSnapshot {
    match outcome {
        ProgressPublishOutcome::Created(published)
        | ProgressPublishOutcome::Advanced(published) => ProgressInvariantSnapshot {
            object_key: published.progress.object_key.clone(),
            namespace_id: published.progress.envelope.state.namespace_id.clone(),
            work_class: published.progress.envelope.state.work_class.clone(),
            through_seq: published.progress.envelope.state.through_seq,
            payload_checksum_valid: true,
        },
        ProgressPublishOutcome::NoChange(current) => ProgressInvariantSnapshot {
            object_key: current.object_key.clone(),
            namespace_id: current.envelope.state.namespace_id.clone(),
            work_class: current.envelope.state.work_class.clone(),
            through_seq: current.envelope.state.through_seq,
            payload_checksum_valid: true,
        },
    }
}

fn assert_states_match(
    scenario: &Scenario,
    trace: &[String],
    step: usize,
    model: Option<ProgressSnapshot>,
    core: Option<ProgressSnapshot>,
) {
    if model != core {
        panic!(
            "progress differential state mismatch at step {}:\n{}",
            step,
            render_trace(scenario, trace)
        );
    }
}

fn assert_reports_pass(
    scenario: &Scenario,
    trace: &mut Vec<String>,
    model_report: &BackgroundWorkInvariantReport,
    core_report: &BackgroundWorkInvariantReport,
    step: usize,
    observed_invariants: &mut Vec<String>,
) {
    trace.extend(model_report.render_trace_lines("progress-model"));
    trace.extend(core_report.render_trace_lines("progress-core"));

    if model_report.checks != core_report.checks {
        panic!(
            "progress invariant report mismatch at step {}:\n{}",
            step,
            render_trace(scenario, trace)
        );
    }

    for check in &core_report.checks {
        if !check.passed {
            panic!(
                "progress invariant failed at step {}: {}:\n{}",
                step,
                check.name,
                render_trace(scenario, trace)
            );
        }
        if !observed_invariants.iter().any(|value| value == &check.name) {
            observed_invariants.push(check.name.clone());
        }
    }
}

type TestDir = loon_testkit::tempdir::TestDir;
