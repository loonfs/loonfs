use loon_core::commit::{
    build_commit_plan, CommitOp, CommitRequest, CommitValidationContext, CommitValidationError,
    Precondition,
};
use loon_core::metadata::MetadataState;
use loon_core::namespace::head_and_lease_fence_tokens_agree;
use loon_model::{
    ModelAction, ModelCommitValidationError, ModelCommitValidationRequest, ModelMetadataState,
    ModelNamespace,
};
use loon_testkit::render::render_trace;
use loon_testkit::scenario::Scenario;
use loon_types::{ChangeSeq, FenceToken, HeadState, InodeId, LeaseState, NamespaceId};
use serde::Deserialize;

#[test]
fn stale_writer_fixture_matches_model_and_core() {
    let scenario = load_fixture("native/namespace_lease_handover_fences_stale_writer.yaml");
    let initial: StaleWriterInitial = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<StaleWriterActionEnvelope> =
        scenario.decode_actions().expect("decode actions");
    let expect: StaleWriterExpect = scenario.decode_expect().expect("decode expectations");

    let mut model_namespace = model_namespace_from_head(&initial.head);
    let mut core_head = initial.head.clone();
    let mut lease = initial.lease.clone();
    let mut writer_a_view = None;
    let mut observed_invariants = Vec::new();
    let mut trace = vec![format!(
        "initial model_head={:?} core_head={:?} lease={:?}",
        snapshot_from_model_namespace(&model_namespace),
        snapshot_from_head(&core_head),
        lease
    )];

    if head_and_lease_fence_tokens_agree(&core_head, &lease) {
        add_invariant(
            &mut observed_invariants,
            "head_and_lease_fence_tokens_agree",
        );
    }

    assert_head_states_match(
        &scenario,
        &trace,
        0,
        &snapshot_from_model_namespace(&model_namespace),
        &snapshot_from_head(&core_head),
    );

    for (index, action) in actions.iter().enumerate() {
        let step_outcome = match action.kind() {
            StaleWriterActionRef::WriterAReadsCurrentHead => {
                let view = WriterView {
                    namespace_id: core_head.namespace_id.clone(),
                    writer_id: lease.holder_id.clone(),
                    planned_head_seq: core_head.seq,
                    writer_fence_token: core_head.active_fence_token,
                };
                writer_a_view = Some(view.clone());
                StepOutcome::ReadCurrentHead(view)
            }
            StaleWriterActionRef::LeaseHandover(action) => {
                lease = LeaseState {
                    namespace_id: lease.namespace_id.clone(),
                    holder_id: action.new_holder_id.clone(),
                    fence_token: action.new_fence_token,
                    lease_expires_at_ms: action.lease_expires_at_ms,
                };
                StepOutcome::LeaseHandedOver {
                    holder_id: lease.holder_id.clone(),
                    fence_token: lease.fence_token,
                    lease_expires_at_ms: lease.lease_expires_at_ms,
                }
            }
            StaleWriterActionRef::WriterBPublishesNewHead(action) => {
                apply_model_head_publish(&mut model_namespace, action);
                core_head = HeadState {
                    namespace_id: core_head.namespace_id.clone(),
                    seq: action.seq,
                    active_fence_token: action.active_fence_token,
                    next_inode_id: action.next_inode_id,
                    snapshot_hint_seq: core_head.snapshot_hint_seq,
                    retention_floor_seq: core_head.retention_floor_seq,
                };

                if head_and_lease_fence_tokens_agree(&core_head, &lease) {
                    add_invariant(
                        &mut observed_invariants,
                        "head_and_lease_fence_tokens_agree",
                    );
                }

                StepOutcome::HeadPublished(snapshot_from_head(&core_head))
            }
            StaleWriterActionRef::WriterAAttemptsPublish(action) => {
                let view = writer_a_view
                    .as_ref()
                    .expect("stale writer publish requires a prior current-head read");
                assert_eq!(
                    view.planned_head_seq, action.planned_head_seq,
                    "fixture stale publish should use the previously observed planned_head_seq"
                );
                assert_eq!(
                    view.writer_fence_token, action.writer_fence_token,
                    "fixture stale publish should use the previously observed writer_fence_token"
                );

                let now_ms = lease.lease_expires_at_ms.saturating_sub(1);
                let model_request = ModelCommitValidationRequest {
                    namespace_id: view.namespace_id.clone(),
                    writer_id: view.writer_id.clone(),
                    writer_fence_token: action.writer_fence_token,
                    planned_head_seq: action.planned_head_seq,
                };
                let model_outcome = model_namespace
                    .validate_commit_attempt(&model_request, &lease, now_ms)
                    .map(|outcome| CommitAttemptOutcome::Accepted {
                        next_seq: outcome.next_seq,
                    })
                    .unwrap_or_else(|err| {
                        CommitAttemptOutcome::Rejected(normalize_model_error(err))
                    });

                let core_request = CommitRequest {
                    namespace_id: view.namespace_id.clone(),
                    request_id: "fixture-stale-writer-attempt".to_owned(),
                    writer_id: view.writer_id.clone(),
                    writer_fence_token: action.writer_fence_token,
                    planned_head_seq: action.planned_head_seq,
                    ops: vec![CommitOp::Rename {
                        inode_id: InodeId(42),
                        new_parent_inode: InodeId(2),
                        new_display_name: "renamed.txt".to_owned(),
                    }],
                    preconditions: vec![Precondition::HeadSeqIs(action.planned_head_seq)],
                };
                let core_outcome = build_commit_plan(
                    &core_request,
                    &CommitValidationContext {
                        head: core_head.clone(),
                        lease: lease.clone(),
                        now_ms,
                        metadata_state: MetadataState::default(),
                    },
                )
                .map(|plan| CommitAttemptOutcome::Accepted {
                    next_seq: plan.next_seq,
                })
                .unwrap_or_else(|err| CommitAttemptOutcome::Rejected(normalize_core_error(err)));

                if model_outcome != core_outcome {
                    let mut mismatch_trace = trace.clone();
                    mismatch_trace.push(format!(
                        "step={} action=writer_a_attempts_publish(planned_head_seq={}, writer_fence_token={}) model_outcome={:?} core_outcome={:?} model_head={:?} core_head={:?} lease={:?}",
                        index + 1,
                        action.planned_head_seq.0,
                        action.writer_fence_token.0,
                        model_outcome,
                        core_outcome,
                        snapshot_from_model_namespace(&model_namespace),
                        snapshot_from_head(&core_head),
                        lease
                    ));
                    panic!(
                        "stale writer differential outcome mismatch at step {}:\n{}",
                        index + 1,
                        render_trace(&scenario, &mismatch_trace)
                    );
                }

                if matches!(core_outcome, CommitAttemptOutcome::Rejected(_)) {
                    add_invariant(&mut observed_invariants, "stale_writer_cannot_publish");
                }

                StepOutcome::CommitAttempt(core_outcome)
            }
        };

        trace.push(format!(
            "step={} action={} outcome={:?} model_head={:?} core_head={:?} lease={:?}",
            index + 1,
            action.describe(),
            step_outcome,
            snapshot_from_model_namespace(&model_namespace),
            snapshot_from_head(&core_head),
            lease
        ));

        assert_head_states_match(
            &scenario,
            &trace,
            index + 1,
            &snapshot_from_model_namespace(&model_namespace),
            &snapshot_from_head(&core_head),
        );
    }

    assert_expected_invariants(&scenario, &trace, &expect.invariants, &observed_invariants);
}

#[derive(Debug, Deserialize)]
struct StaleWriterInitial {
    head: HeadState,
    lease: LeaseState,
}

#[derive(Debug, Deserialize)]
struct StaleWriterExpect {
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct StaleWriterActionEnvelope {
    #[serde(default)]
    writer_a_reads_current_head: Option<bool>,
    #[serde(default)]
    lease_handover: Option<LeaseHandoverAction>,
    #[serde(default)]
    writer_b_publishes_new_head: Option<PublishHeadAction>,
    #[serde(default)]
    writer_a_attempts_publish: Option<AttemptPublishAction>,
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

#[derive(Debug, Clone, Copy)]
enum StaleWriterActionRef<'a> {
    WriterAReadsCurrentHead,
    LeaseHandover(&'a LeaseHandoverAction),
    WriterBPublishesNewHead(&'a PublishHeadAction),
    WriterAAttemptsPublish(&'a AttemptPublishAction),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WriterView {
    namespace_id: NamespaceId,
    writer_id: String,
    planned_head_seq: ChangeSeq,
    writer_fence_token: FenceToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NamespaceSnapshot {
    namespace_id: NamespaceId,
    seq: ChangeSeq,
    active_fence_token: FenceToken,
    next_inode_id: InodeId,
    snapshot_hint_seq: Option<ChangeSeq>,
    retention_floor_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StepOutcome {
    ReadCurrentHead(WriterView),
    LeaseHandedOver {
        holder_id: String,
        fence_token: FenceToken,
        lease_expires_at_ms: u64,
    },
    HeadPublished(NamespaceSnapshot),
    CommitAttempt(CommitAttemptOutcome),
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

impl StaleWriterActionEnvelope {
    fn kind(&self) -> StaleWriterActionRef<'_> {
        let mut matches = Vec::new();

        if self.writer_a_reads_current_head == Some(true) {
            matches.push(StaleWriterActionRef::WriterAReadsCurrentHead);
        }
        if let Some(action) = &self.lease_handover {
            matches.push(StaleWriterActionRef::LeaseHandover(action));
        }
        if let Some(action) = &self.writer_b_publishes_new_head {
            matches.push(StaleWriterActionRef::WriterBPublishesNewHead(action));
        }
        if let Some(action) = &self.writer_a_attempts_publish {
            matches.push(StaleWriterActionRef::WriterAAttemptsPublish(action));
        }

        assert_eq!(
            matches.len(),
            1,
            "stale-writer action envelope should contain exactly one action variant"
        );
        matches
            .into_iter()
            .next()
            .expect("one stale-writer action variant")
    }

    fn describe(&self) -> String {
        match self.kind() {
            StaleWriterActionRef::WriterAReadsCurrentHead => {
                "writer_a_reads_current_head".to_owned()
            }
            StaleWriterActionRef::LeaseHandover(action) => format!(
                "lease_handover(new_holder_id={}, new_fence_token={}, lease_expires_at_ms={})",
                action.new_holder_id, action.new_fence_token.0, action.lease_expires_at_ms
            ),
            StaleWriterActionRef::WriterBPublishesNewHead(action) => format!(
                "writer_b_publishes_new_head(seq={}, active_fence_token={}, next_inode_id={})",
                action.seq.0, action.active_fence_token.0, action.next_inode_id.0
            ),
            StaleWriterActionRef::WriterAAttemptsPublish(action) => format!(
                "writer_a_attempts_publish(planned_head_seq={}, writer_fence_token={})",
                action.planned_head_seq.0, action.writer_fence_token.0
            ),
        }
    }
}

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
}

fn model_namespace_from_head(head: &HeadState) -> ModelNamespace {
    ModelNamespace {
        namespace_id: head.namespace_id.clone(),
        head_seq: head.seq,
        active_fence_token: head.active_fence_token,
        next_inode_id: head.next_inode_id,
        snapshot_hint_seq: head.snapshot_hint_seq,
        retention_floor_seq: head.retention_floor_seq,
        metadata_state: ModelMetadataState::default(),
    }
}

fn snapshot_from_model_namespace(namespace: &ModelNamespace) -> NamespaceSnapshot {
    NamespaceSnapshot {
        namespace_id: namespace.namespace_id.clone(),
        seq: namespace.head_seq,
        active_fence_token: namespace.active_fence_token,
        next_inode_id: namespace.next_inode_id,
        snapshot_hint_seq: namespace.snapshot_hint_seq,
        retention_floor_seq: namespace.retention_floor_seq,
    }
}

fn snapshot_from_head(head: &HeadState) -> NamespaceSnapshot {
    NamespaceSnapshot {
        namespace_id: head.namespace_id.clone(),
        seq: head.seq,
        active_fence_token: head.active_fence_token,
        next_inode_id: head.next_inode_id,
        snapshot_hint_seq: head.snapshot_hint_seq,
        retention_floor_seq: head.retention_floor_seq,
    }
}

fn apply_model_head_publish(namespace: &mut ModelNamespace, action: &PublishHeadAction) {
    assert_eq!(
        action.seq,
        ChangeSeq(namespace.head_seq.0 + 1),
        "fixture head publish should advance the namespace seq by exactly one"
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
        assert!(
            action.next_inode_id.0 >= namespace.next_inode_id.0,
            "fixture head publish should keep next_inode_id monotonic"
        );
        namespace
            .apply(ModelAction::CreateDir {
                inode_id: InodeId(action.next_inode_id.0.saturating_sub(1)),
                writer_fence_token: action.active_fence_token,
            })
            .expect("model head publish should succeed");
    }

    assert_eq!(
        namespace.head_seq, action.seq,
        "model head publish should reach the fixture seq"
    );
    assert_eq!(
        namespace.active_fence_token, action.active_fence_token,
        "model head publish should reach the fixture fence token"
    );
    assert_eq!(
        namespace.next_inode_id, action.next_inode_id,
        "model head publish should reach the fixture inode allocation state"
    );
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
        other => {
            panic!("unexpected core commit validation error in stale-writer harness: {other:?}")
        }
    }
}

fn add_invariant(observed_invariants: &mut Vec<String>, invariant: &str) {
    if !observed_invariants.iter().any(|value| value == invariant) {
        observed_invariants.push(invariant.to_owned());
    }
}

fn assert_expected_invariants(
    scenario: &Scenario,
    trace: &[String],
    expected: &[String],
    observed: &[String],
) {
    for invariant in expected {
        assert!(
            observed.iter().any(|value| value == invariant),
            "missing expected invariant `{invariant}`:\n{}",
            render_trace(scenario, trace)
        );
    }
}

fn assert_head_states_match(
    scenario: &Scenario,
    trace: &[String],
    step: usize,
    model_head: &NamespaceSnapshot,
    core_head: &NamespaceSnapshot,
) {
    if model_head != core_head {
        panic!(
            "stale-writer differential state mismatch at step {}:\n{}",
            step,
            render_trace(scenario, trace)
        );
    }
}
