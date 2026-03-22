use loon_core::checkpoint::{
    load_checkpoint, prepare_checkpoint, prepare_checkpoint_head_publish, publish_checkpoint_head,
    CheckpointHeadPublishRequest, CheckpointPublishError, PreparedCheckpoint,
    StoredCheckpointManifest, StoredCheckpointSegment,
};
use loon_core::commit::{
    build_commit_plan, CommitOp, CommitRequest, CommitValidationContext, CommitValidationError,
    Precondition,
};
use loon_core::metadata::MetadataState;
use loon_core::namespace::head_and_lease_fence_tokens_agree;
use loon_core::progress::{
    load_retention_authorizers, publish_progress, read_progress_object, ProgressPublishOutcome,
};
use loon_model::{
    ModelAction, ModelCheckpoint, ModelCheckpointPublishAuthorizers, ModelCommitValidationError,
    ModelCommitValidationRequest, ModelError, ModelMetadataState, ModelNamespace,
    ModelProgressObject, ModelQueueBroker, ModelQueueClaim, ModelQueueJob, ModelQueueJobState,
    ModelQueueRepairOutcome, ModelQueueSeqPayload, ModelQueueShard, ModelQueueWorkClass,
};
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::{derived_progress, namespace_head, namespace_lease, queue_shard};
use loon_objectstore::ObjectStore;
use loon_queue::durable::{
    read_queue_shard, repair_lost_snapshot_enqueue_in_store, DurableSnapshotRepairOutcome,
};
use loon_queue::types::{
    JobState, QueueBroker, QueueClaim, QueueJob, QueueShardEnvelope, QueueShardState,
    SeqScopedPayload, WorkClass,
};
use loon_sim::SimRuntime;
use loon_testkit::fixtures::load_fixture;
use loon_testkit::invariants::{
    evaluate_background_checkpoint_publish_invariants, evaluate_background_repair_invariants,
    evaluate_background_sim_trace_determinism_invariants,
    evaluate_background_stale_writer_invariants, evaluate_checkpoint_head_publish_invariants,
    evaluate_progress_publish_invariants, evaluate_queue_repair_invariants,
    evaluate_queue_shard_object_invariants, BackgroundCheckpointPublishInvariantInputs,
    BackgroundRepairInvariantInputs, BackgroundSimTraceDeterminismInvariantInputs,
    BackgroundStaleWriterInvariantInputs, BackgroundWorkInvariantReport,
    CheckpointHeadPublishInvariantInputs, CheckpointProgressAuthorizer, ProgressInvariantSnapshot,
    ProgressPublishInvariantInputs, ProgressPublishOutcomeKind, QueueRepairInvariantInputs,
    QueueRepairOutcomeKind, QueueShardObjectInvariantInputs, SimInvariantReport,
};
use loon_testkit::render::render_trace;
use loon_testkit::scenario::Scenario;
use loon_testkit::tempdir::TestDir;
use loon_types::{
    ChangeSeq, ControlObjectEnvelope, ControlObjectKind, FenceToken, HeadState, HeadStateEnvelope,
    InodeId, LeaseState, LeaseStateEnvelope, NamespaceId, ProgressState,
};
use serde::Deserialize;
use std::collections::BTreeMap;

const TEST_WRITER_VERSION: &str = "loon-testkit-sim";

#[test]
fn background_stale_writer_remains_fenced_after_handover() {
    let report = run_background_fixture_report(
        "sim/background_stale_writer_remains_fenced_after_handover.yaml",
    );
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/sim-interleavings/sim/background_stale_writer_remains_fenced_after_handover.txt"
        )
    );
}

#[test]
fn background_checkpoint_publish_waits_for_required_progress() {
    let report = run_background_fixture_report(
        "sim/background_checkpoint_publish_waits_for_required_progress.yaml",
    );
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/sim-interleavings/sim/background_checkpoint_publish_waits_for_required_progress.txt"
        )
    );
}

#[test]
fn background_repair_tracks_latest_visible_head_seq() {
    let report =
        run_background_fixture_report("sim/background_repair_tracks_latest_visible_head_seq.yaml");
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/sim-interleavings/sim/background_repair_tracks_latest_visible_head_seq.txt"
        )
    );
}

#[test]
fn background_sim_trace_order_is_seed_stable_for_checkpoint_fixture() {
    let first = run_background_fixture_report(
        "sim/background_checkpoint_publish_waits_for_required_progress.yaml",
    );
    let second = run_background_fixture_report(
        "sim/background_checkpoint_publish_waits_for_required_progress.yaml",
    );
    let report = evaluate_background_sim_trace_determinism_invariants(
        BackgroundSimTraceDeterminismInvariantInputs {
            first_rendered_trace: &first.rendered_trace,
            second_rendered_trace: &second.rendered_trace,
        },
    );

    assert!(
        report
            .check("background_sim_trace_order_is_seed_stable")
            .expect("check should exist")
            .passed
    );
}

struct BackgroundFixtureRunReport {
    rendered_trace: String,
}

fn run_background_fixture_report(relative_path: &str) -> BackgroundFixtureRunReport {
    let scenario = load_fixture(relative_path);
    let initial: BackgroundSimInitial = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<BackgroundSimActionEnvelope> =
        scenario.decode_actions().expect("decode actions");
    let expect: BackgroundSimExpect = scenario.decode_expect().expect("decode expectations");

    let temp_dir = TestDir::new("background-scheduler-sim");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    let mut runtime = SimRuntime::new();
    let mut trace = Vec::new();
    let mut observed_invariants = Vec::new();

    let mut model_namespace = model_namespace_from_head(&initial.head, &initial.metadata_state);
    let mut core_head = initial.head.clone();
    let mut lease = initial.lease.clone();
    let mut model_progress = initial
        .progress_objects
        .iter()
        .map(|progress| {
            (
                progress.payload.work_class.clone(),
                ModelProgressObject {
                    namespace_id: progress.payload.namespace_id.clone(),
                    work_class: progress.payload.work_class.clone(),
                    through_seq: progress.payload.through_seq,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut model_queue = initial
        .queue_shard
        .as_ref()
        .map(|fixture| model_queue_from_fixture(&fixture.payload));
    let mut cached_writer_view = None::<WriterView>;
    let mut prepared_checkpoint = None::<PreparedCheckpoint>;
    let mut prepared_model_checkpoint = None::<ModelCheckpoint>;
    let mut checkpoint_publish_history = Vec::<CheckpointPublishAttempt>::new();
    let mut last_repair_tracking = None::<RepairTracking>;

    overwrite_head(&store, &core_head);
    overwrite_lease(&store, &lease);
    seed_progress_objects(&store, &initial.progress_objects);
    if let Some(queue_shard_fixture) = &initial.queue_shard {
        overwrite_queue_shard(
            &store,
            &queue_shard_state_from_fixture(&queue_shard_fixture.payload),
        );
    }

    runtime.record_actor_step(
        "fixture",
        format!(
            "initial head=(seq={}, fence={}, next_inode={}) lease=(holder={}, fence={}, expires_at_ms={}) metadata_rows=(inodes={}, direntries={}, revisions={}, tombstones={}) progress_objects={} queue_shard_present={}",
            core_head.seq.0,
            core_head.active_fence_token.0,
            core_head.next_inode_id.0,
            lease.holder_id,
            lease.fence_token.0,
            lease.lease_expires_at_ms,
            initial.metadata_state.inodes.len(),
            initial.metadata_state.direntries.len(),
            initial.metadata_state.revisions.len(),
            initial.metadata_state.subtree_tombstones.len(),
            initial.progress_objects.len(),
            initial.queue_shard.is_some()
        ),
    );
    push_latest_runtime_trace_line(&mut trace, &runtime);

    for (index, action) in actions.iter().enumerate() {
        match action.kind() {
            BackgroundSimActionRef::WriterReadCurrentHead => {
                let view = WriterView {
                    namespace_id: core_head.namespace_id.clone(),
                    writer_id: lease.holder_id.clone(),
                    planned_head_seq: core_head.seq,
                    writer_fence_token: core_head.active_fence_token,
                };
                cached_writer_view = Some(view.clone());
                runtime.record_actor_step(
                    "stale_writer",
                    format!(
                        "step={} action={} cached_view=(writer_id={}, planned_head_seq={}, writer_fence_token={})",
                        index + 1,
                        action.describe(),
                        view.writer_id,
                        view.planned_head_seq.0,
                        view.writer_fence_token.0
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);
            }
            BackgroundSimActionRef::LeaseHandover(action) => {
                lease = LeaseState {
                    namespace_id: lease.namespace_id.clone(),
                    holder_id: action.new_holder_id.clone(),
                    fence_token: action.new_fence_token,
                    lease_expires_at_ms: action.lease_expires_at_ms,
                };
                overwrite_lease(&store, &lease);
                runtime.record_actor_step(
                    "writer",
                    format!(
                        "step={} action={} resulting_lease=(holder={}, fence={}, expires_at_ms={})",
                        index + 1,
                        BackgroundSimActionEnvelope::describe_lease_handover(action),
                        lease.holder_id,
                        lease.fence_token.0,
                        lease.lease_expires_at_ms
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);
            }
            BackgroundSimActionRef::WriterPublishHead(action) => {
                apply_model_head_publish(&mut model_namespace, action);
                core_head = HeadState {
                    namespace_id: core_head.namespace_id.clone(),
                    seq: action.seq,
                    active_fence_token: action.active_fence_token,
                    next_inode_id: action.next_inode_id,
                    snapshot_hint_seq: core_head.snapshot_hint_seq,
                    retention_floor_seq: core_head.retention_floor_seq,
                };
                overwrite_head(&store, &core_head);
                runtime.record_actor_step(
                    "writer",
                    format!(
                        "step={} action={} resulting_head=(seq={}, fence={}, next_inode={})",
                        index + 1,
                        BackgroundSimActionEnvelope::describe_writer_publish(action),
                        core_head.seq.0,
                        core_head.active_fence_token.0,
                        core_head.next_inode_id.0
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);
            }
            BackgroundSimActionRef::WriterAttemptCommit(action) => {
                let view = cached_writer_view
                    .as_ref()
                    .expect("writer_attempt_commit requires writer_read_current_head first");
                let now_ms = runtime
                    .now_ms()
                    .max(lease.lease_expires_at_ms.saturating_sub(1));
                let model_outcome = model_namespace
                    .validate_commit_attempt(
                        &ModelCommitValidationRequest {
                            namespace_id: view.namespace_id.clone(),
                            writer_id: view.writer_id.clone(),
                            writer_fence_token: action.writer_fence_token,
                            planned_head_seq: action.planned_head_seq,
                        },
                        &lease,
                        now_ms,
                    )
                    .map(|outcome| CommitAttemptOutcome::Accepted {
                        next_seq: outcome.next_seq,
                    })
                    .unwrap_or_else(|err| {
                        CommitAttemptOutcome::Rejected(normalize_model_error(err))
                    });
                let core_outcome = build_commit_plan(
                    &CommitRequest {
                        namespace_id: view.namespace_id.clone(),
                        request_id: "background-sim-stale-writer".to_owned(),
                        writer_id: view.writer_id.clone(),
                        writer_fence_token: action.writer_fence_token,
                        planned_head_seq: action.planned_head_seq,
                        ops: vec![CommitOp::Rename {
                            inode_id: InodeId(42),
                            new_parent_inode: InodeId(2),
                            new_display_name: "renamed.txt".to_owned(),
                        }],
                        preconditions: vec![Precondition::HeadSeqIs(action.planned_head_seq)],
                    },
                    &CommitValidationContext {
                        head: core_head.clone(),
                        lease: lease.clone(),
                        now_ms,
                        metadata_state: initial.metadata_state.clone(),
                    },
                )
                .map(|plan| CommitAttemptOutcome::Accepted {
                    next_seq: plan.next_seq,
                })
                .unwrap_or_else(|err| CommitAttemptOutcome::Rejected(normalize_core_error(err)));

                if model_outcome != core_outcome {
                    panic!(
                        "background sim stale-writer model/core divergence at step {}:\n{}",
                        index + 1,
                        render_trace(&scenario, &trace)
                    );
                }

                runtime.record_actor_step(
                    "stale_writer",
                    format!(
                        "step={} action={} model_outcome={:?} core_outcome={:?} now_ms={} head=(seq={}, fence={}) lease=(holder={}, fence={})",
                        index + 1,
                        BackgroundSimActionEnvelope::describe_writer_attempt(action),
                        model_outcome,
                        core_outcome,
                        now_ms,
                        core_head.seq.0,
                        core_head.active_fence_token.0,
                        lease.holder_id,
                        lease.fence_token.0
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);

                if matches!(core_outcome, CommitAttemptOutcome::Rejected(_)) {
                    add_invariant(&mut observed_invariants, "stale_writer_cannot_publish");
                }
                let sim_report = evaluate_background_stale_writer_invariants(
                    BackgroundStaleWriterInvariantInputs {
                        stale_publish_rejected: matches!(
                            core_outcome,
                            CommitAttemptOutcome::Rejected(
                                CommitAttemptError::StaleWriterFenceToken { .. }
                            ) | CommitAttemptOutcome::Rejected(
                                CommitAttemptError::LeaseHolderMismatch { .. }
                            ) | CommitAttemptOutcome::Rejected(
                                CommitAttemptError::PlannedHeadSeqMismatch { .. }
                            ) | CommitAttemptOutcome::Rejected(
                                CommitAttemptError::HeadLeaseFenceMismatch { .. }
                            )
                        ),
                        head_fence_matches_lease: head_and_lease_fence_tokens_agree(
                            &core_head, &lease,
                        ),
                        stale_writer_fence_token: action.writer_fence_token,
                        active_fence_token: core_head.active_fence_token,
                    },
                );
                assert_sim_report_passes(
                    &scenario,
                    &mut trace,
                    "background-sim",
                    &sim_report,
                    index + 1,
                    &mut observed_invariants,
                );
            }
            BackgroundSimActionRef::CheckpointBuild => {
                let model_checkpoint = model_namespace.checkpoint();
                let core_checkpoint =
                    prepare_checkpoint(&core_head, &initial.metadata_state, TEST_WRITER_VERSION)
                        .expect("build checkpoint");
                let model_snapshot = checkpoint_build_snapshot_from_model(&model_checkpoint);
                let core_snapshot = checkpoint_build_snapshot_from_core(&core_checkpoint);
                if model_snapshot != core_snapshot {
                    panic!(
                        "background sim checkpoint build model/core divergence at step {}:\n{}",
                        index + 1,
                        render_trace(&scenario, &trace)
                    );
                }
                prepared_model_checkpoint = Some(model_checkpoint);
                prepared_checkpoint = Some(core_checkpoint);
                runtime.record_actor_step(
                    "checkpoint_builder",
                    format!(
                        "step={} action=checkpoint_build manifest_key={} segment_keys={:?}",
                        index + 1,
                        core_snapshot.manifest_key,
                        core_snapshot.segment_keys
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);
            }
            BackgroundSimActionRef::CheckpointPublish(action) => {
                let before_head = core_head.clone();
                let prepared_core = prepared_checkpoint
                    .as_ref()
                    .expect("checkpoint_publish requires a prior checkpoint_build action");
                let prepared_model = prepared_model_checkpoint
                    .as_ref()
                    .expect("checkpoint_publish requires a prior checkpoint_build action");
                let model_authorizers = checkpoint_model_authorizers(
                    &model_progress,
                    &action.required_progress_work_classes,
                    &action.retention_policy_work_class,
                );
                let mut model_after = model_namespace.clone();
                let model_outcome = model_after
                    .publish_checkpoint(
                        prepared_model,
                        &model_available_segment_keys(prepared_model),
                        action.requested_retention_floor_seq,
                        Some(&model_authorizers),
                    )
                    .map(|_| NormalizedCheckpointPublishOutcome::Published {
                        snapshot_hint_seq: model_after.snapshot_hint_seq,
                        retention_floor_seq: model_after.retention_floor_seq,
                    })
                    .unwrap_or_else(|err| normalize_model_checkpoint_publish_error(err));

                let loaded_checkpoint = load_checkpoint(
                    &core_head.namespace_id,
                    &StoredCheckpointManifest {
                        object_key: prepared_core.manifest.object_key.clone(),
                        encoded_bytes: prepared_core.manifest.encoded_bytes.clone(),
                    },
                    &prepared_core
                        .segments
                        .iter()
                        .map(|segment| StoredCheckpointSegment {
                            object_key: segment.object_key.clone(),
                            encoded_bytes: segment.encoded_bytes.clone(),
                        })
                        .collect::<Vec<_>>(),
                )
                .expect("load prepared checkpoint");
                let retention_authorizers = load_retention_authorizers(
                    &store,
                    &core_head.namespace_id,
                    &action.required_progress_work_classes,
                    &action.retention_policy_work_class,
                )
                .expect("load retention authorizers");
                let core_outcome = prepare_checkpoint_head_publish(
                    &core_head,
                    &loaded_checkpoint,
                    &CheckpointHeadPublishRequest {
                        requested_retention_floor_seq: action.requested_retention_floor_seq,
                        retention_authorizers: Some(retention_authorizers.clone()),
                    },
                    TEST_WRITER_VERSION,
                )
                .and_then(|prepared_head| {
                    let etag = current_head_etag(&store, &core_head.namespace_id);
                    publish_checkpoint_head(&store, &etag, &prepared_head)?;
                    Ok(NormalizedCheckpointPublishOutcome::Published {
                        snapshot_hint_seq: prepared_head.resulting_head.snapshot_hint_seq,
                        retention_floor_seq: prepared_head.resulting_head.retention_floor_seq,
                    })
                })
                .unwrap_or_else(normalize_core_checkpoint_publish_error);

                if model_outcome != core_outcome {
                    panic!(
                        "background sim checkpoint publish model/core divergence at step {}:\n{}",
                        index + 1,
                        render_trace(&scenario, &trace)
                    );
                }

                if let NormalizedCheckpointPublishOutcome::Published { .. } = core_outcome {
                    model_namespace = model_after;
                    core_head = read_head(&store, &core_head.namespace_id);
                }

                runtime.record_actor_step(
                    "checkpoint_publisher",
                    format!(
                        "step={} action={} model_outcome={:?} core_outcome={:?} resulting_head=(seq={}, snapshot_hint={:?}, retention_floor={})",
                        index + 1,
                        BackgroundSimActionEnvelope::describe_checkpoint_publish(action),
                        model_outcome,
                        core_outcome,
                        core_head.seq.0,
                        core_head.snapshot_hint_seq.map(|seq| seq.0),
                        core_head.retention_floor_seq.0
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);

                if let NormalizedCheckpointPublishOutcome::Published { .. } = core_outcome {
                    let core_required = retention_authorizers
                        .required_progress
                        .iter()
                        .map(core_authorizer)
                        .collect::<Vec<_>>();
                    let checkpoint_report = evaluate_checkpoint_head_publish_invariants(
                        CheckpointHeadPublishInvariantInputs {
                            current_head: &before_head,
                            checkpoint_namespace: &loaded_checkpoint.manifest.payload.namespace_id,
                            checkpoint_seq: loaded_checkpoint.manifest.payload.checkpoint_seq,
                            checkpoint_verified: loaded_checkpoint.manifest.payload.verified,
                            checkpoint_segments_verified: loaded_checkpoint
                                .checked_invariants
                                .iter()
                                .any(|name| {
                                    name == "checkpoint_segment_descriptor_matches_payload"
                                }),
                            requested_retention_floor_seq: action.requested_retention_floor_seq,
                            required_progress: &core_required,
                            retention_policy: Some(core_authorizer(
                                &retention_authorizers.retention_policy,
                            )),
                            resulting_head: &core_head,
                        },
                    );
                    assert_background_report_passes(
                        &scenario,
                        &mut trace,
                        "checkpoint-publish",
                        &checkpoint_report,
                        index + 1,
                        &mut observed_invariants,
                    );
                }

                let after_head = core_head.clone();
                checkpoint_publish_history.push(CheckpointPublishAttempt {
                    before_head,
                    after_head,
                    outcome: core_outcome.clone(),
                });

                if checkpoint_publish_history.len() >= 2 {
                    let first = &checkpoint_publish_history[0];
                    let second = &checkpoint_publish_history[1];
                    let sim_report = evaluate_background_checkpoint_publish_invariants(
                        BackgroundCheckpointPublishInvariantInputs {
                            first_publish_blocked: first.outcome.is_blocked_for_required_progress(),
                            second_publish_succeeded: second.outcome.is_published(),
                            snapshot_hint_before: first.before_head.snapshot_hint_seq,
                            snapshot_hint_after_blocked: first.after_head.snapshot_hint_seq,
                            snapshot_hint_after_success: second.after_head.snapshot_hint_seq,
                            retention_floor_before: first.before_head.retention_floor_seq,
                            retention_floor_after_blocked: first.after_head.retention_floor_seq,
                            retention_floor_after_success: second.after_head.retention_floor_seq,
                        },
                    );
                    assert_sim_report_passes(
                        &scenario,
                        &mut trace,
                        "background-sim",
                        &sim_report,
                        index + 1,
                        &mut observed_invariants,
                    );
                }
            }
            BackgroundSimActionRef::PublishProgress(action) => {
                let before_model = model_progress
                    .get(&action.work_class)
                    .map(|progress| progress.through_seq);
                let expected_model = model_namespace
                    .publish_progress(
                        model_progress.get(&action.work_class),
                        &action.work_class,
                        action.through_seq,
                    )
                    .expect("model progress publish should succeed");
                let model_outcome = classify_model_progress_outcome(
                    before_model,
                    expected_model.through_seq,
                    action.through_seq,
                );
                model_progress.insert(action.work_class.clone(), expected_model);

                let core_outcome = publish_progress(
                    &store,
                    &action.namespace_id,
                    &action.work_class,
                    action.through_seq,
                    TEST_WRITER_VERSION,
                )
                .expect("core progress publish should succeed");
                let core_loaded =
                    read_progress_object(&store, &action.namespace_id, &action.work_class)
                        .expect("progress should load after publish");
                let normalized_core_outcome = normalize_core_progress_outcome(&core_outcome);

                if model_outcome != normalized_core_outcome {
                    panic!(
                        "background sim progress publish model/core divergence at step {}:\n{}",
                        index + 1,
                        render_trace(&scenario, &trace)
                    );
                }

                runtime.record_actor_step(
                    "progress_publisher",
                    format!(
                        "step={} action={} model_outcome={:?} core_outcome={:?} resulting_through_seq={}",
                        index + 1,
                        BackgroundSimActionEnvelope::describe_publish_progress(action),
                        model_outcome,
                        normalized_core_outcome,
                        core_loaded.envelope.state.through_seq.0
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);

                let progress_report =
                    evaluate_progress_publish_invariants(ProgressPublishInvariantInputs {
                        expected_namespace: &action.namespace_id,
                        expected_work_class: &action.work_class,
                        before_through_seq: before_model,
                        requested_through_seq: action.through_seq,
                        outcome: progress_outcome_kind(&normalized_core_outcome),
                        after_progress: &ProgressInvariantSnapshot {
                            object_key: derived_progress(
                                action.namespace_id.as_str(),
                                &action.work_class,
                            ),
                            namespace_id: core_loaded.envelope.state.namespace_id.clone(),
                            work_class: core_loaded.envelope.state.work_class.clone(),
                            through_seq: core_loaded.envelope.state.through_seq,
                            payload_checksum_valid: true,
                        },
                    });
                assert_background_report_passes(
                    &scenario,
                    &mut trace,
                    "progress",
                    &progress_report,
                    index + 1,
                    &mut observed_invariants,
                );
            }
            BackgroundSimActionRef::RepairLostEnqueueToQueueShard(action) => {
                let queue_before = model_queue.clone().unwrap_or_else(|| ModelQueueShard {
                    work_class: ModelQueueWorkClass::BuildSnapshot,
                    shard_id: action.shard_id,
                    broker: None,
                    jobs: Vec::new(),
                });
                let mut next_model_queue = queue_before.clone();
                let progress = model_progress.get(WorkClass::BuildSnapshot.as_str());
                let model_outcome = model_namespace
                    .repair_lost_snapshot_enqueue(&mut next_model_queue, progress)
                    .map(NormalizedRepairOutcome::from)
                    .unwrap_or_else(|err| panic!("model repair should succeed: {err:?}"));
                let core_outcome = repair_lost_snapshot_enqueue_in_store(
                    &store,
                    action.shard_id,
                    &core_head,
                    read_progress_state_opt(
                        &store,
                        &core_head.namespace_id,
                        WorkClass::BuildSnapshot.as_str(),
                    )
                    .as_ref(),
                    TEST_WRITER_VERSION,
                )
                .map(NormalizedRepairOutcome::from)
                .unwrap_or_else(|err| panic!("core repair should succeed: {err:?}"));

                if model_outcome != core_outcome {
                    panic!(
                        "background sim repair model/core divergence at step {}:\n{}",
                        index + 1,
                        render_trace(&scenario, &trace)
                    );
                }
                model_queue = Some(next_model_queue);

                runtime.record_actor_step(
                    "repair",
                    format!(
                        "step={} action={} model_outcome={:?} core_outcome={:?} head_seq={}",
                        index + 1,
                        BackgroundSimActionEnvelope::describe_repair(action),
                        model_outcome,
                        core_outcome,
                        core_head.seq.0
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);

                let loaded_queue = read_queue_shard(&store, action.shard_id)
                    .expect("queue shard should load after repair");
                let queue_report = evaluate_queue_repair_invariants(QueueRepairInvariantInputs {
                    namespace_id: &action.namespace_id,
                    head_seq: core_head.seq,
                    progress_through_seq: read_progress_state_opt(
                        &store,
                        &core_head.namespace_id,
                        WorkClass::BuildSnapshot.as_str(),
                    )
                    .map(|progress| progress.through_seq),
                    outcome: queue_repair_outcome_kind(&core_outcome),
                    has_namespace_scoped_job_after: loaded_queue.envelope.state.jobs.iter().any(
                        |job| {
                            job.dedupe_key
                                == format!(
                                    "{}:{}",
                                    WorkClass::BuildSnapshot.as_str(),
                                    action.namespace_id
                                )
                        },
                    ),
                    ready_job_through_seq_after: loaded_queue
                        .envelope
                        .state
                        .jobs
                        .iter()
                        .find(|job| {
                            job.dedupe_key
                                == format!(
                                    "{}:{}",
                                    WorkClass::BuildSnapshot.as_str(),
                                    action.namespace_id
                                )
                        })
                        .filter(|job| matches!(job.state, JobState::Ready))
                        .map(|job| job.payload.through_seq),
                    follow_up_through_seq_after: loaded_queue
                        .envelope
                        .state
                        .jobs
                        .iter()
                        .find(|job| {
                            job.dedupe_key
                                == format!(
                                    "{}:{}",
                                    WorkClass::BuildSnapshot.as_str(),
                                    action.namespace_id
                                )
                        })
                        .and_then(|job| job.follow_up.as_ref().map(|payload| payload.through_seq)),
                });
                assert_background_report_passes(
                    &scenario,
                    &mut trace,
                    "queue-repair",
                    &queue_report,
                    index + 1,
                    &mut observed_invariants,
                );
                let queue_object_report =
                    evaluate_queue_shard_object_invariants(QueueShardObjectInvariantInputs {
                        shard_index: action.shard_id,
                        payload_checksum_valid: true,
                        object_key: &loaded_queue.object_key,
                        actual_shard_id: loaded_queue.envelope.state.shard_id,
                        cas_protected: true,
                    });
                assert_background_report_passes(
                    &scenario,
                    &mut trace,
                    "queue-shard",
                    &queue_object_report,
                    index + 1,
                    &mut observed_invariants,
                );

                last_repair_tracking = Some(RepairTracking {
                    repaired_through_seq: core_outcome.repaired_through_seq(),
                    latest_visible_head_seq: core_head.seq,
                });
                let sim_report =
                    evaluate_background_repair_invariants(BackgroundRepairInvariantInputs {
                        repaired_through_seq: core_outcome.repaired_through_seq(),
                        latest_visible_head_seq: core_head.seq,
                    });
                assert_sim_report_passes(
                    &scenario,
                    &mut trace,
                    "background-sim",
                    &sim_report,
                    index + 1,
                    &mut observed_invariants,
                );
            }
            BackgroundSimActionRef::AdvanceTimeMs(delta_ms) => {
                runtime.advance_time(delta_ms);
                push_latest_runtime_trace_line(&mut trace, &runtime);
                runtime.record_actor_step(
                    "clock",
                    format!(
                        "step={} action=advance_time_ms(delta_ms={}) now_ms={}",
                        index + 1,
                        delta_ms,
                        runtime.now_ms()
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);
            }
        }
    }

    if let Some(expected_head) = &expect.head {
        let actual_head = read_head(&store, &expected_head.namespace_id);
        if &actual_head != expected_head || core_head != *expected_head {
            panic!(
                "background sim final head mismatch expected={expected_head:?} actual_store={actual_head:?} actual_runtime={core_head:?}:\n{}",
                render_trace(&scenario, &trace)
            );
        }
    }

    if !expect.progress_objects.is_empty() {
        let actual_progress = expect
            .progress_objects
            .iter()
            .map(|expected| ProgressSnapshot {
                key: expected.key.clone(),
                payload: read_progress_object(
                    &store,
                    &expected.payload.namespace_id,
                    &expected.payload.work_class,
                )
                .expect("expected progress object should load")
                .envelope
                .state,
            })
            .collect::<Vec<_>>();
        let expected_progress = expect
            .progress_objects
            .iter()
            .map(|progress| ProgressSnapshot {
                key: progress.key.clone(),
                payload: progress.payload.clone(),
            })
            .collect::<Vec<_>>();
        if actual_progress != expected_progress {
            panic!(
                "background sim final progress mismatch expected={expected_progress:?} actual={actual_progress:?}:\n{}",
                render_trace(&scenario, &trace)
            );
        }
    }

    if let Some(expected_queue_shard) = &expect.queue_shard {
        let actual_queue = read_queue_shard(&store, expected_queue_shard.payload.shard_id)
            .expect("expected queue shard should load");
        let expected_state = queue_shard_state_from_fixture(&expected_queue_shard.payload);
        if actual_queue.object_key != expected_queue_shard.key
            || actual_queue.envelope.state != expected_state
        {
            panic!(
                "background sim final queue shard mismatch expected_key={} actual_key={} expected_state={:?} actual_state={:?}:\n{}",
                expected_queue_shard.key,
                actual_queue.object_key,
                expected_state,
                actual_queue.envelope.state,
                render_trace(&scenario, &trace)
            );
        }
    }

    if let Some(repair_tracking) = last_repair_tracking {
        let _ = repair_tracking;
    }

    for invariant in &expect.invariants {
        assert!(
            observed_invariants.iter().any(|value| value == invariant),
            "missing expected invariant `{invariant}`:\n{}",
            render_trace(&scenario, &trace)
        );
    }

    BackgroundFixtureRunReport {
        rendered_trace: render_trace(&scenario, &trace),
    }
}

#[derive(Debug, Deserialize)]
struct BackgroundSimInitial {
    head: HeadState,
    lease: LeaseState,
    #[serde(default)]
    metadata_state: MetadataState,
    #[serde(default)]
    progress_objects: Vec<FixtureProgressObject>,
    #[serde(default)]
    queue_shard: Option<FixtureQueueShardObject>,
}

#[derive(Debug, Deserialize)]
struct BackgroundSimExpect {
    #[serde(default)]
    head: Option<HeadState>,
    #[serde(default)]
    progress_objects: Vec<FixtureProgressObject>,
    #[serde(default)]
    queue_shard: Option<FixtureQueueShardObject>,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct BackgroundSimActionEnvelope {
    #[serde(default)]
    writer_read_current_head: Option<bool>,
    #[serde(default)]
    lease_handover: Option<LeaseHandoverAction>,
    #[serde(default)]
    writer_publish_head: Option<PublishHeadAction>,
    #[serde(default)]
    writer_attempt_commit: Option<AttemptPublishAction>,
    #[serde(default)]
    checkpoint_build: Option<bool>,
    #[serde(default)]
    checkpoint_publish: Option<CheckpointPublishAction>,
    #[serde(default)]
    publish_progress: Option<PublishProgressAction>,
    #[serde(default)]
    repair_lost_enqueue_to_queue_shard: Option<RepairLostEnqueueToQueueShardAction>,
    #[serde(default)]
    advance_time_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct LeaseHandoverAction {
    new_holder_id: String,
    new_fence_token: FenceToken,
    lease_expires_at_ms: u64,
}

#[derive(Debug, Deserialize)]
struct PublishHeadAction {
    seq: ChangeSeq,
    active_fence_token: FenceToken,
    next_inode_id: InodeId,
}

#[derive(Debug, Deserialize)]
struct AttemptPublishAction {
    planned_head_seq: ChangeSeq,
    writer_fence_token: FenceToken,
}

#[derive(Debug, Deserialize)]
struct CheckpointPublishAction {
    requested_retention_floor_seq: Option<ChangeSeq>,
    required_progress_work_classes: Vec<String>,
    retention_policy_work_class: String,
}

#[derive(Debug, Deserialize)]
struct PublishProgressAction {
    namespace_id: NamespaceId,
    work_class: String,
    through_seq: ChangeSeq,
}

#[derive(Debug, Deserialize)]
struct RepairLostEnqueueToQueueShardAction {
    shard_id: u32,
    namespace_id: NamespaceId,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
struct FixtureProgressObject {
    key: String,
    payload: ProgressState,
}

#[derive(Debug, Deserialize, Clone)]
struct FixtureQueueShardObject {
    key: String,
    payload: FixtureQueueShardPayload,
}

#[derive(Debug, Deserialize, Clone)]
struct FixtureQueueShardPayload {
    work_class: WorkClass,
    shard_id: u32,
    #[serde(default)]
    broker: Option<QueueBroker>,
    #[serde(default)]
    jobs: Vec<FixtureQueueShardJob>,
}

#[derive(Debug, Deserialize, Clone)]
struct FixtureQueueShardJob {
    #[serde(default)]
    job_id: Option<String>,
    dedupe_key: String,
    state: JobState,
    payload: SeqScopedPayload,
    #[serde(default)]
    follow_up: Option<SeqScopedPayload>,
    #[serde(default)]
    claim: Option<QueueClaim>,
    #[serde(default)]
    attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WriterView {
    namespace_id: NamespaceId,
    writer_id: String,
    planned_head_seq: ChangeSeq,
    writer_fence_token: FenceToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CommitAttemptOutcome {
    Accepted { next_seq: ChangeSeq },
    Rejected(CommitAttemptError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CommitAttemptError {
    NamespaceMismatch,
    HeadLeaseNamespaceMismatch,
    HeadLeaseFenceMismatch {
        head: FenceToken,
        lease: FenceToken,
    },
    PlannedHeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    StaleWriterFenceToken {
        expected: FenceToken,
        actual: FenceToken,
    },
    LeaseHolderMismatch {
        expected: String,
        actual: String,
    },
    LeaseExpired {
        lease_expires_at_ms: u64,
        now_ms: u64,
    },
    SeqOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckpointBuildSnapshot {
    manifest_key: String,
    checkpoint_seq: ChangeSeq,
    active_fence_token: FenceToken,
    next_inode_id: InodeId,
    retention_floor_seq: ChangeSeq,
    verified: bool,
    segment_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NormalizedCheckpointPublishOutcome {
    Published {
        snapshot_hint_seq: Option<ChangeSeq>,
        retention_floor_seq: ChangeSeq,
    },
    RequiredProgressLag {
        work_class: String,
        requested: ChangeSeq,
        available: ChangeSeq,
    },
    RetentionPolicyLag {
        work_class: String,
        requested: ChangeSeq,
        available: ChangeSeq,
    },
    Other(String),
}

impl NormalizedCheckpointPublishOutcome {
    fn is_published(&self) -> bool {
        matches!(self, Self::Published { .. })
    }

    fn is_blocked_for_required_progress(&self) -> bool {
        matches!(
            self,
            Self::RequiredProgressLag { .. } | Self::RetentionPolicyLag { .. }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NormalizedProgressOutcome {
    Created { through_seq: ChangeSeq },
    Advanced { through_seq: ChangeSeq },
    NoChange { through_seq: ChangeSeq },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NormalizedRepairOutcome {
    NoRepairNeeded,
    Enqueued { through_seq: ChangeSeq },
    RaisedReadyJob { through_seq: ChangeSeq },
    AttachedFollowUp { through_seq: ChangeSeq },
}

impl NormalizedRepairOutcome {
    fn repaired_through_seq(&self) -> Option<ChangeSeq> {
        match self {
            Self::NoRepairNeeded => None,
            Self::Enqueued { through_seq }
            | Self::RaisedReadyJob { through_seq }
            | Self::AttachedFollowUp { through_seq } => Some(*through_seq),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckpointPublishAttempt {
    before_head: HeadState,
    after_head: HeadState,
    outcome: NormalizedCheckpointPublishOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RepairTracking {
    repaired_through_seq: Option<ChangeSeq>,
    latest_visible_head_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProgressSnapshot {
    key: String,
    payload: ProgressState,
}

impl BackgroundSimActionEnvelope {
    fn kind(&self) -> BackgroundSimActionRef<'_> {
        let mut matches = Vec::new();

        if self.writer_read_current_head == Some(true) {
            matches.push(BackgroundSimActionRef::WriterReadCurrentHead);
        }
        if let Some(action) = &self.lease_handover {
            matches.push(BackgroundSimActionRef::LeaseHandover(action));
        }
        if let Some(action) = &self.writer_publish_head {
            matches.push(BackgroundSimActionRef::WriterPublishHead(action));
        }
        if let Some(action) = &self.writer_attempt_commit {
            matches.push(BackgroundSimActionRef::WriterAttemptCommit(action));
        }
        if self.checkpoint_build == Some(true) {
            matches.push(BackgroundSimActionRef::CheckpointBuild);
        }
        if let Some(action) = &self.checkpoint_publish {
            matches.push(BackgroundSimActionRef::CheckpointPublish(action));
        }
        if let Some(action) = &self.publish_progress {
            matches.push(BackgroundSimActionRef::PublishProgress(action));
        }
        if let Some(action) = &self.repair_lost_enqueue_to_queue_shard {
            matches.push(BackgroundSimActionRef::RepairLostEnqueueToQueueShard(
                action,
            ));
        }
        if let Some(delta_ms) = self.advance_time_ms {
            matches.push(BackgroundSimActionRef::AdvanceTimeMs(delta_ms));
        }

        assert_eq!(
            matches.len(),
            1,
            "background sim action envelope should contain exactly one action variant"
        );
        matches
            .into_iter()
            .next()
            .expect("one background sim action variant")
    }

    fn describe(&self) -> String {
        match self.kind() {
            BackgroundSimActionRef::WriterReadCurrentHead => "writer_read_current_head".to_owned(),
            BackgroundSimActionRef::LeaseHandover(action) => Self::describe_lease_handover(action),
            BackgroundSimActionRef::WriterPublishHead(action) => {
                Self::describe_writer_publish(action)
            }
            BackgroundSimActionRef::WriterAttemptCommit(action) => {
                Self::describe_writer_attempt(action)
            }
            BackgroundSimActionRef::CheckpointBuild => "checkpoint_build".to_owned(),
            BackgroundSimActionRef::CheckpointPublish(action) => {
                Self::describe_checkpoint_publish(action)
            }
            BackgroundSimActionRef::PublishProgress(action) => {
                Self::describe_publish_progress(action)
            }
            BackgroundSimActionRef::RepairLostEnqueueToQueueShard(action) => {
                Self::describe_repair(action)
            }
            BackgroundSimActionRef::AdvanceTimeMs(delta_ms) => {
                format!("advance_time_ms(delta_ms={delta_ms})")
            }
        }
    }

    fn describe_lease_handover(action: &LeaseHandoverAction) -> String {
        format!(
            "lease_handover(new_holder_id={}, new_fence_token={}, lease_expires_at_ms={})",
            action.new_holder_id, action.new_fence_token.0, action.lease_expires_at_ms
        )
    }

    fn describe_writer_publish(action: &PublishHeadAction) -> String {
        format!(
            "writer_publish_head(seq={}, active_fence_token={}, next_inode_id={})",
            action.seq.0, action.active_fence_token.0, action.next_inode_id.0
        )
    }

    fn describe_writer_attempt(action: &AttemptPublishAction) -> String {
        format!(
            "writer_attempt_commit(planned_head_seq={}, writer_fence_token={})",
            action.planned_head_seq.0, action.writer_fence_token.0
        )
    }

    fn describe_checkpoint_publish(action: &CheckpointPublishAction) -> String {
        format!(
            "checkpoint_publish(requested_retention_floor_seq={:?}, required_progress_work_classes={:?}, retention_policy_work_class={})",
            action.requested_retention_floor_seq.map(|seq| seq.0),
            action.required_progress_work_classes,
            action.retention_policy_work_class
        )
    }

    fn describe_publish_progress(action: &PublishProgressAction) -> String {
        format!(
            "publish_progress(namespace_id={}, work_class={}, through_seq={})",
            action.namespace_id.as_str(),
            action.work_class,
            action.through_seq.0
        )
    }

    fn describe_repair(action: &RepairLostEnqueueToQueueShardAction) -> String {
        format!(
            "repair_lost_enqueue_to_queue_shard(shard_id={}, namespace_id={})",
            action.shard_id,
            action.namespace_id.as_str()
        )
    }
}

enum BackgroundSimActionRef<'a> {
    WriterReadCurrentHead,
    LeaseHandover(&'a LeaseHandoverAction),
    WriterPublishHead(&'a PublishHeadAction),
    WriterAttemptCommit(&'a AttemptPublishAction),
    CheckpointBuild,
    CheckpointPublish(&'a CheckpointPublishAction),
    PublishProgress(&'a PublishProgressAction),
    RepairLostEnqueueToQueueShard(&'a RepairLostEnqueueToQueueShardAction),
    AdvanceTimeMs(u64),
}

fn push_latest_runtime_trace_line(trace: &mut Vec<String>, runtime: &SimRuntime) {
    let line = runtime
        .trace()
        .last()
        .expect("runtime should contain at least one trace event")
        .render_line();
    trace.push(line);
}

fn add_invariant(observed_invariants: &mut Vec<String>, invariant: &str) {
    if !observed_invariants.iter().any(|value| value == invariant) {
        observed_invariants.push(invariant.to_owned());
    }
}

fn assert_background_report_passes(
    scenario: &Scenario,
    trace: &mut Vec<String>,
    label: &str,
    report: &BackgroundWorkInvariantReport,
    step: usize,
    observed_invariants: &mut Vec<String>,
) {
    trace.extend(report.render_trace_lines(label));
    for check in &report.checks {
        if !check.passed {
            panic!(
                "background sim invariant failed at step {}: {}:\n{}",
                step,
                check.name,
                render_trace(scenario, trace)
            );
        }
        add_invariant(observed_invariants, &check.name);
    }
}

fn assert_sim_report_passes(
    scenario: &Scenario,
    trace: &mut Vec<String>,
    label: &str,
    report: &SimInvariantReport,
    step: usize,
    observed_invariants: &mut Vec<String>,
) {
    trace.extend(report.render_trace_lines(label));
    for check in &report.checks {
        if !check.passed {
            panic!(
                "background sim invariant failed at step {}: {}:\n{}",
                step,
                check.name,
                render_trace(scenario, trace)
            );
        }
        add_invariant(observed_invariants, &check.name);
    }
}

fn model_namespace_from_head(head: &HeadState, metadata_state: &MetadataState) -> ModelNamespace {
    ModelNamespace {
        namespace_id: head.namespace_id.clone(),
        head_seq: head.seq,
        active_fence_token: head.active_fence_token,
        next_inode_id: head.next_inode_id,
        snapshot_hint_seq: head.snapshot_hint_seq,
        retention_floor_seq: head.retention_floor_seq,
        metadata_state: model_metadata_from_core(metadata_state),
    }
}

fn model_metadata_from_core(metadata_state: &MetadataState) -> ModelMetadataState {
    ModelMetadataState {
        inodes: metadata_state
            .inodes
            .iter()
            .map(|inode| loon_model::ModelInodeRecord {
                inode_id: inode.inode_id,
                inode_kind: inode.inode_kind.clone(),
                created_seq: inode.created_seq,
            })
            .collect(),
        direntries: metadata_state
            .direntries
            .iter()
            .map(|direntry| loon_model::ModelDirentryRecord {
                parent_inode_id: direntry.parent_inode_id,
                name_key: direntry.name_key.clone(),
                display_name: direntry.display_name.clone(),
                child_inode_id: direntry.child_inode_id,
                bind_seq: direntry.bind_seq,
                bind_op_index: direntry.bind_op_index,
            })
            .collect(),
        revisions: metadata_state
            .revisions
            .iter()
            .map(|revision| loon_model::ModelRevisionRecord {
                inode_id: revision.inode_id,
                revision_no: revision.revision_no,
                committed_seq: revision.committed_seq,
                revision_op_index: revision.revision_op_index,
                content_manifest_digest: revision.content_manifest_digest.clone(),
            })
            .collect(),
        subtree_tombstones: metadata_state
            .subtree_tombstones
            .iter()
            .map(|tombstone| loon_model::ModelSubtreeTombstoneRecord {
                root_inode_id: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
                tombstone_op_index: tombstone.tombstone_op_index,
            })
            .collect(),
    }
}

fn apply_model_head_publish(namespace: &mut ModelNamespace, action: &PublishHeadAction) {
    assert_eq!(
        action.seq,
        ChangeSeq(namespace.head_seq.0 + 1),
        "fixture head publish should advance seq by exactly one"
    );
    if namespace.active_fence_token != action.active_fence_token {
        namespace
            .apply(ModelAction::RotateFence {
                new_fence_token: action.active_fence_token,
            })
            .expect("model fence rotation should succeed");
    }
    if action.next_inode_id == namespace.next_inode_id {
        namespace
            .apply(ModelAction::BumpSeq {
                writer_fence_token: action.active_fence_token,
            })
            .expect("model seq bump should succeed");
    } else {
        namespace
            .apply(ModelAction::CreateDir {
                inode_id: InodeId(action.next_inode_id.0.saturating_sub(1)),
                writer_fence_token: action.active_fence_token,
            })
            .expect("model head publish should succeed");
    }
}

fn normalize_model_error(error: ModelCommitValidationError) -> CommitAttemptError {
    match error {
        ModelCommitValidationError::NamespaceMismatch => CommitAttemptError::NamespaceMismatch,
        ModelCommitValidationError::HeadLeaseNamespaceMismatch => {
            CommitAttemptError::HeadLeaseNamespaceMismatch
        }
        ModelCommitValidationError::HeadLeaseFenceMismatch { head, lease } => {
            CommitAttemptError::HeadLeaseFenceMismatch { head, lease }
        }
        ModelCommitValidationError::PlannedHeadSeqMismatch { expected, actual } => {
            CommitAttemptError::PlannedHeadSeqMismatch { expected, actual }
        }
        ModelCommitValidationError::StaleWriterFenceToken { expected, actual } => {
            CommitAttemptError::StaleWriterFenceToken { expected, actual }
        }
        ModelCommitValidationError::LeaseHolderMismatch { expected, actual } => {
            CommitAttemptError::LeaseHolderMismatch { expected, actual }
        }
        ModelCommitValidationError::LeaseExpired {
            lease_expires_at_ms,
            now_ms,
        } => CommitAttemptError::LeaseExpired {
            lease_expires_at_ms,
            now_ms,
        },
        ModelCommitValidationError::SeqOverflow => CommitAttemptError::SeqOverflow,
    }
}

fn normalize_core_error(error: CommitValidationError) -> CommitAttemptError {
    match error {
        CommitValidationError::NamespaceMismatch => CommitAttemptError::NamespaceMismatch,
        CommitValidationError::HeadLeaseNamespaceMismatch => {
            CommitAttemptError::HeadLeaseNamespaceMismatch
        }
        CommitValidationError::HeadLeaseFenceMismatch { head, lease } => {
            CommitAttemptError::HeadLeaseFenceMismatch { head, lease }
        }
        CommitValidationError::PlannedHeadSeqMismatch { expected, actual } => {
            CommitAttemptError::PlannedHeadSeqMismatch { expected, actual }
        }
        CommitValidationError::StaleWriterFenceToken { active, requested } => {
            CommitAttemptError::StaleWriterFenceToken {
                expected: active,
                actual: requested,
            }
        }
        CommitValidationError::LeaseHolderMismatch { expected, actual } => {
            CommitAttemptError::LeaseHolderMismatch { expected, actual }
        }
        CommitValidationError::LeaseExpired {
            lease_expires_at_ms,
            now_ms,
        } => CommitAttemptError::LeaseExpired {
            lease_expires_at_ms,
            now_ms,
        },
        CommitValidationError::SeqOverflow => CommitAttemptError::SeqOverflow,
        other => panic!("unexpected core commit validation error: {other:?}"),
    }
}

fn checkpoint_build_snapshot_from_model(checkpoint: &ModelCheckpoint) -> CheckpointBuildSnapshot {
    CheckpointBuildSnapshot {
        manifest_key: loon_objectstore::keys::snapshot_manifest(
            checkpoint.namespace_id.as_str(),
            checkpoint.checkpoint_seq.0,
        ),
        checkpoint_seq: checkpoint.checkpoint_seq,
        active_fence_token: checkpoint.active_fence_token,
        next_inode_id: checkpoint.next_inode_id,
        retention_floor_seq: checkpoint.retention_floor_seq,
        verified: checkpoint.verified,
        segment_keys: checkpoint
            .tables
            .iter()
            .flat_map(|table| {
                table
                    .segments
                    .iter()
                    .map(|segment| segment.object_key.clone())
            })
            .collect(),
    }
}

fn checkpoint_build_snapshot_from_core(checkpoint: &PreparedCheckpoint) -> CheckpointBuildSnapshot {
    CheckpointBuildSnapshot {
        manifest_key: checkpoint.manifest.object_key.clone(),
        checkpoint_seq: checkpoint.manifest.envelope.payload.checkpoint_seq,
        active_fence_token: checkpoint.manifest.envelope.payload.active_fence_token,
        next_inode_id: checkpoint.manifest.envelope.payload.next_inode_id,
        retention_floor_seq: checkpoint.manifest.envelope.payload.retention_floor_seq,
        verified: checkpoint.manifest.envelope.payload.verified,
        segment_keys: checkpoint
            .segments
            .iter()
            .map(|segment| segment.object_key.clone())
            .collect(),
    }
}

fn normalize_model_checkpoint_publish_error(err: ModelError) -> NormalizedCheckpointPublishOutcome {
    match err {
        ModelError::RequiredProgressLag {
            work_class,
            requested,
            available,
        } => NormalizedCheckpointPublishOutcome::RequiredProgressLag {
            work_class,
            requested,
            available,
        },
        ModelError::RetentionPolicyLag {
            work_class,
            requested,
            available,
        } => NormalizedCheckpointPublishOutcome::RetentionPolicyLag {
            work_class,
            requested,
            available,
        },
        other => NormalizedCheckpointPublishOutcome::Other(format!("{other:?}")),
    }
}

fn normalize_core_checkpoint_publish_error(
    err: CheckpointPublishError,
) -> NormalizedCheckpointPublishOutcome {
    match err {
        CheckpointPublishError::RequiredProgressLag {
            work_class,
            requested,
            available,
        } => NormalizedCheckpointPublishOutcome::RequiredProgressLag {
            work_class,
            requested,
            available,
        },
        CheckpointPublishError::RetentionPolicyLag {
            work_class,
            requested,
            available,
        } => NormalizedCheckpointPublishOutcome::RetentionPolicyLag {
            work_class,
            requested,
            available,
        },
        other => NormalizedCheckpointPublishOutcome::Other(format!("{other:?}")),
    }
}

fn model_available_segment_keys(checkpoint: &ModelCheckpoint) -> Vec<String> {
    checkpoint
        .tables
        .iter()
        .flat_map(|table| {
            table
                .segments
                .iter()
                .map(|segment| segment.object_key.clone())
        })
        .collect()
}

fn checkpoint_model_authorizers(
    model_progress: &BTreeMap<String, ModelProgressObject>,
    required_progress_work_classes: &[String],
    retention_policy_work_class: &str,
) -> ModelCheckpointPublishAuthorizers {
    ModelCheckpointPublishAuthorizers {
        required_progress: required_progress_work_classes
            .iter()
            .map(|work_class| {
                model_progress
                    .get(work_class)
                    .unwrap_or_else(|| panic!("missing model progress for {work_class}"))
                    .clone()
            })
            .collect(),
        retention_policy: model_progress
            .get(retention_policy_work_class)
            .unwrap_or_else(|| {
                panic!("missing model retention policy progress for {retention_policy_work_class}")
            })
            .clone(),
    }
}

fn core_authorizer<'a>(
    progress: &'a loon_core::progress::LoadedProgressObject,
) -> CheckpointProgressAuthorizer<'a> {
    CheckpointProgressAuthorizer {
        namespace_id: &progress.envelope.state.namespace_id,
        work_class: &progress.envelope.state.work_class,
        through_seq: progress.envelope.state.through_seq,
    }
}

fn classify_model_progress_outcome(
    before_through_seq: Option<ChangeSeq>,
    after_through_seq: ChangeSeq,
    requested_through_seq: ChangeSeq,
) -> NormalizedProgressOutcome {
    match before_through_seq {
        None => NormalizedProgressOutcome::Created {
            through_seq: after_through_seq,
        },
        Some(before) if before >= requested_through_seq => NormalizedProgressOutcome::NoChange {
            through_seq: after_through_seq,
        },
        Some(_) => NormalizedProgressOutcome::Advanced {
            through_seq: after_through_seq,
        },
    }
}

fn normalize_core_progress_outcome(outcome: &ProgressPublishOutcome) -> NormalizedProgressOutcome {
    match outcome {
        ProgressPublishOutcome::Created(progress) => NormalizedProgressOutcome::Created {
            through_seq: progress.progress.envelope.state.through_seq,
        },
        ProgressPublishOutcome::Advanced(progress) => NormalizedProgressOutcome::Advanced {
            through_seq: progress.progress.envelope.state.through_seq,
        },
        ProgressPublishOutcome::NoChange(progress) => NormalizedProgressOutcome::NoChange {
            through_seq: progress.envelope.state.through_seq,
        },
    }
}

fn progress_outcome_kind(outcome: &NormalizedProgressOutcome) -> ProgressPublishOutcomeKind {
    match outcome {
        NormalizedProgressOutcome::Created { .. } => ProgressPublishOutcomeKind::Created,
        NormalizedProgressOutcome::Advanced { .. } => ProgressPublishOutcomeKind::Advanced,
        NormalizedProgressOutcome::NoChange { .. } => ProgressPublishOutcomeKind::NoChange,
    }
}

fn queue_repair_outcome_kind(outcome: &NormalizedRepairOutcome) -> QueueRepairOutcomeKind {
    match outcome {
        NormalizedRepairOutcome::NoRepairNeeded => QueueRepairOutcomeKind::NoRepairNeeded,
        NormalizedRepairOutcome::Enqueued { through_seq } => QueueRepairOutcomeKind::Enqueued {
            through_seq: *through_seq,
        },
        NormalizedRepairOutcome::RaisedReadyJob { through_seq } => {
            QueueRepairOutcomeKind::RaisedReadyJob {
                through_seq: *through_seq,
            }
        }
        NormalizedRepairOutcome::AttachedFollowUp { through_seq } => {
            QueueRepairOutcomeKind::AttachedFollowUp {
                through_seq: *through_seq,
            }
        }
    }
}

impl From<ModelQueueRepairOutcome> for NormalizedRepairOutcome {
    fn from(value: ModelQueueRepairOutcome) -> Self {
        match value {
            ModelQueueRepairOutcome::NoRepairNeeded => Self::NoRepairNeeded,
            ModelQueueRepairOutcome::Enqueued { through_seq } => Self::Enqueued { through_seq },
            ModelQueueRepairOutcome::RaisedReadyJob { through_seq } => {
                Self::RaisedReadyJob { through_seq }
            }
            ModelQueueRepairOutcome::AttachedFollowUp { through_seq } => {
                Self::AttachedFollowUp { through_seq }
            }
        }
    }
}

impl From<DurableSnapshotRepairOutcome> for NormalizedRepairOutcome {
    fn from(value: DurableSnapshotRepairOutcome) -> Self {
        match value {
            DurableSnapshotRepairOutcome::NoChange => Self::NoRepairNeeded,
            DurableSnapshotRepairOutcome::Created(repair)
            | DurableSnapshotRepairOutcome::Updated(repair) => match repair.repair {
                loon_queue::repair::SnapshotRepairOutcome::NoRepairNeeded => Self::NoRepairNeeded,
                loon_queue::repair::SnapshotRepairOutcome::Enqueued { through_seq } => {
                    Self::Enqueued { through_seq }
                }
                loon_queue::repair::SnapshotRepairOutcome::RaisedReadyJob { through_seq } => {
                    Self::RaisedReadyJob { through_seq }
                }
                loon_queue::repair::SnapshotRepairOutcome::AttachedFollowUp { through_seq } => {
                    Self::AttachedFollowUp { through_seq }
                }
            },
        }
    }
}

fn model_queue_from_fixture(fixture: &FixtureQueueShardPayload) -> ModelQueueShard {
    ModelQueueShard {
        work_class: match fixture.work_class {
            WorkClass::BuildSnapshot => ModelQueueWorkClass::BuildSnapshot,
            ref other => panic!("unsupported work class in background sim: {other:?}"),
        },
        shard_id: fixture.shard_id,
        broker: fixture.broker.as_ref().map(|broker| ModelQueueBroker {
            broker_id: broker.broker_id.clone(),
            epoch: broker.epoch,
            lease_expires_at_ms: broker.lease_expires_at_ms,
        }),
        jobs: fixture
            .jobs
            .iter()
            .map(|job| ModelQueueJob {
                job_id: fixture_job_id(job, &fixture.work_class),
                dedupe_key: job.dedupe_key.clone(),
                state: match job.state {
                    JobState::Ready => ModelQueueJobState::Ready,
                    JobState::Claimed => ModelQueueJobState::Claimed,
                },
                payload: ModelQueueSeqPayload {
                    namespace_id: job.payload.namespace_id.clone(),
                    through_seq: job.payload.through_seq,
                },
                follow_up: job.follow_up.as_ref().map(|payload| ModelQueueSeqPayload {
                    namespace_id: payload.namespace_id.clone(),
                    through_seq: payload.through_seq,
                }),
                claim: job.claim.as_ref().map(|claim| ModelQueueClaim {
                    worker_id: claim.worker_id.clone(),
                    claim_token: claim.claim_token.clone(),
                    heartbeat_at_ms: claim.heartbeat_at_ms,
                    timeout_at_ms: claim.timeout_at_ms,
                }),
                attempts: job.attempts,
            })
            .collect(),
    }
}

fn queue_shard_state_from_fixture(payload: &FixtureQueueShardPayload) -> QueueShardState {
    QueueShardState {
        work_class: payload.work_class.clone(),
        shard_id: payload.shard_id,
        broker: payload.broker.clone(),
        jobs: payload
            .jobs
            .iter()
            .map(|job| QueueJob {
                job_id: fixture_job_id(job, &payload.work_class),
                dedupe_key: job.dedupe_key.clone(),
                state: job.state.clone(),
                payload: job.payload.clone(),
                follow_up: job.follow_up.clone(),
                claim: job.claim.clone(),
                attempts: job.attempts,
            })
            .collect(),
    }
}

fn fixture_job_id(job: &FixtureQueueShardJob, work_class: &WorkClass) -> String {
    job.job_id.clone().unwrap_or_else(|| {
        if job.dedupe_key == format!("{}:{}", work_class.as_str(), job.payload.namespace_id) {
            format!(
                "repair-{}-{}",
                work_class.as_str(),
                job.payload.namespace_id
            )
        } else {
            format!("fixture-job-{}", job.dedupe_key)
        }
    })
}

fn overwrite_head(store: &LocalFsStore, head: &HeadState) {
    let envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        TEST_WRITER_VERSION,
        head.clone(),
    )
    .expect("build head envelope");
    store
        .put_overwrite(
            &namespace_head(head.namespace_id.as_str()),
            &serde_json::to_vec(&envelope).expect("encode head"),
        )
        .expect("write head");
}

fn overwrite_lease(store: &LocalFsStore, lease: &LeaseState) {
    let envelope = LeaseStateEnvelope::from_state(
        ControlObjectKind::NamespaceLease,
        TEST_WRITER_VERSION,
        lease.clone(),
    )
    .expect("build lease envelope");
    store
        .put_overwrite(
            &namespace_lease(lease.namespace_id.as_str()),
            &serde_json::to_vec(&envelope).expect("encode lease"),
        )
        .expect("write lease");
}

fn read_head(store: &LocalFsStore, namespace_id: &NamespaceId) -> HeadState {
    let bytes = store
        .get(&namespace_head(namespace_id.as_str()), None)
        .expect("read head bytes")
        .expect("head exists");
    let envelope: HeadStateEnvelope = serde_json::from_slice(&bytes).expect("decode head");
    envelope.state
}

fn current_head_etag(store: &LocalFsStore, namespace_id: &NamespaceId) -> String {
    store
        .head(&namespace_head(namespace_id.as_str()))
        .expect("head metadata")
        .expect("head exists")
        .etag
        .expect("head etag")
}

fn seed_progress_objects(store: &LocalFsStore, progress_objects: &[FixtureProgressObject]) {
    for progress in progress_objects {
        let envelope = ControlObjectEnvelope::from_state(
            ControlObjectKind::NamespaceProgress,
            TEST_WRITER_VERSION,
            progress.payload.clone(),
        )
        .expect("build progress envelope");
        store
            .put_if_absent(
                &progress.key,
                &serde_json::to_vec(&envelope).expect("encode progress"),
            )
            .expect("seed progress");
    }
}

fn overwrite_queue_shard(store: &LocalFsStore, state: &QueueShardState) {
    let envelope = QueueShardEnvelope::from_state(
        ControlObjectKind::QueueShard,
        TEST_WRITER_VERSION,
        state.clone(),
    )
    .expect("build queue shard envelope");
    store
        .put_overwrite(
            &queue_shard(state.shard_id),
            &serde_json::to_vec(&envelope).expect("encode queue shard"),
        )
        .expect("write queue shard");
}

fn read_progress_state_opt(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    work_class: &str,
) -> Option<ProgressState> {
    match store.get(&derived_progress(namespace_id.as_str(), work_class), None) {
        Ok(Some(bytes)) => {
            let envelope: ControlObjectEnvelope<ProgressState> =
                serde_json::from_slice(&bytes).expect("decode progress");
            Some(envelope.state)
        }
        Ok(None) => None,
        Err(err) => panic!("read progress object failed: {err:?}"),
    }
}
