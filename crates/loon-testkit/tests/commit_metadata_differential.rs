use loon_server::core::commit::{
    build_commit_plan, prepare_commit_head_publish, CommitOp, CommitRequest,
    CommitValidationContext, Precondition,
};
use loon_server::core::metadata::MetadataState;
use loon_server::core::wal::prepare_wal_commit;
use loon_testkit::model::{
    ModelCommitValidationRequest, ModelMetadataMutation, ModelMetadataPreconditionError,
    ModelMetadataState, ModelNamespace,
};
use loon_testkit::invariants::{
    evaluate_namespace_commit_invariants, CommitInvariantInputs, NamespaceCoreInvariantReport,
};
use loon_testkit::render::render_trace;
use loon_testkit::scenario::Scenario;
use loon_types::{ChangeSeq, HeadState, InodeId, LeaseState, NamespaceId, RevisionNo};
use serde::Deserialize;

const NOW_MS: u64 = 1_500;
const TEST_WRITER_VERSION: &str = "loon-testkit-differential";

#[test]
fn create_dir_fixture_matches_model_and_core_metadata() {
    run_fixture("native/create_dir_commit_applies_inode_and_direntry_rows.yaml");
}

#[test]
fn create_file_fixture_matches_model_and_core_metadata() {
    run_fixture("native/create_file_commit_applies_inode_direntry_and_revision_rows.yaml");
}

#[test]
fn replace_file_fixture_matches_model_and_core_metadata() {
    run_fixture("native/replace_file_commit_appends_new_revision_row.yaml");
}

#[test]
fn delete_subtree_fixture_matches_model_and_core_metadata() {
    run_fixture("native/delete_subtree_commit_appends_tombstone_row.yaml");
}

#[test]
fn restore_revision_fixture_matches_model_and_core_metadata() {
    run_fixture("native/restore_revision_commit_appends_new_head_from_historical_content.yaml");
}

#[test]
fn rename_fixture_matches_model_and_core_metadata() {
    run_fixture("native/rename_commit_rebinds_visible_child_under_new_parent.yaml");
}

fn run_fixture(relative_path: &str) {
    let report = run_fixture_report(relative_path);
    assert!(
        report.model_report.checks.len() == report.core_report.checks.len(),
        "mismatched report sizes:\n{}",
        report.rendered_trace
    );
}

#[test]
fn create_file_fixture_snapshot_trace_matches_checked_in_artifact() {
    let report = run_fixture_report(
        "native/create_file_commit_applies_inode_direntry_and_revision_rows.yaml",
    );
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/commit-metadata-differential/native/create_file_commit_applies_inode_direntry_and_revision_rows.txt"
        )
    );
}

struct CommitFixtureRunReport {
    model_report: NamespaceCoreInvariantReport,
    core_report: NamespaceCoreInvariantReport,
    rendered_trace: String,
}

fn run_fixture_report(relative_path: &str) -> CommitFixtureRunReport {
    let scenario = load_fixture(relative_path);
    let initial: MetadataFixtureInitial = scenario.decode_initial().expect("decode initial state");
    let expect: MetadataFixtureExpect = scenario.decode_expect().expect("decode expectations");
    let request = CommitRequest::from(initial.request.clone());

    let mut trace = vec![format!(
        "initial head={:?} metadata={:?} request_id={}",
        initial.head, initial.metadata_state, request.request_id
    )];

    let core_plan = build_commit_plan(
        &request,
        &CommitValidationContext {
            head: initial.head.clone(),
            lease: initial.lease.clone(),
            now_ms: NOW_MS,
            metadata_state: initial.metadata_state.clone(),
        },
    )
    .expect("core commit validation");
    let prepared_wal =
        prepare_wal_commit(&request, &core_plan, TEST_WRITER_VERSION).expect("prepare wal");
    let core_metadata = initial
        .metadata_state
        .apply_committed_wal_ops(core_plan.next_seq, &prepared_wal.envelope.payload.ops)
        .expect("apply core metadata");
    let core_head = prepare_commit_head_publish(&initial.head, &core_plan, TEST_WRITER_VERSION)
        .expect("prepare head publish")
        .resulting_head;

    let model_namespace = model_namespace_from_fixture(&initial);
    let model_validation = model_namespace
        .validate_commit_attempt(
            &ModelCommitValidationRequest {
                namespace_id: request.namespace_id.clone(),
                writer_id: request.writer_id.clone(),
                writer_fence_token: request.writer_fence_token,
                planned_head_seq: request.planned_head_seq,
            },
            &initial.lease,
            NOW_MS,
        )
        .expect("model frame validation");
    validate_model_metadata_preconditions(
        &model_namespace.metadata_state,
        &initial.request.preconditions,
        request.planned_head_seq,
    )
    .expect("model metadata preconditions");
    validate_model_request_ops(
        &model_namespace.metadata_state,
        &initial.request.ops,
        request.planned_head_seq,
    )
    .expect("model op validation");
    let (model_mutations, model_allocated_inode_ids, model_next_inode_id) =
        materialize_model_mutations(&initial.request, initial.head.next_inode_id);
    let model_metadata = model_namespace
        .metadata_state
        .apply_committed_mutations(model_validation.next_seq, &model_mutations)
        .expect("apply model metadata");
    let model_head = HeadState {
        namespace_id: model_namespace.namespace_id.clone(),
        seq: model_validation.next_seq,
        active_fence_token: request.writer_fence_token,
        next_inode_id: model_next_inode_id,
        snapshot_hint_seq: model_namespace.snapshot_hint_seq,
        retention_floor_seq: model_namespace.retention_floor_seq,
    };

    trace.push(format!(
        "validated core_plan(next_seq={}, allocated_inode_ids={:?}) model_next_seq={} model_allocated_inode_ids={:?}",
        core_plan.next_seq.0,
        core_plan.allocated_inode_ids,
        model_validation.next_seq.0,
        model_allocated_inode_ids
    ));

    if core_plan.allocated_inode_ids != model_allocated_inode_ids {
        panic!(
            "allocated inode mismatch:\n{}",
            render_trace(&scenario, &trace)
        );
    }

    let core_snapshot = NamespaceMetadataSnapshot {
        head: core_head.clone(),
        metadata_state: core_metadata.metadata_state.clone(),
    };
    let model_snapshot = NamespaceMetadataSnapshot {
        head: model_head.clone(),
        metadata_state: metadata_state_from_model(&model_metadata.metadata_state),
    };
    let expected_snapshot = NamespaceMetadataSnapshot {
        head: expect.head.clone(),
        metadata_state: expect.metadata_state.clone(),
    };

    trace.push(format!(
        "final expected={:?} model={:?} core={:?}",
        expected_snapshot, model_snapshot, core_snapshot
    ));

    if model_snapshot != core_snapshot {
        panic!(
            "model/core metadata divergence:\n{}",
            render_trace(&scenario, &trace)
        );
    }

    if core_snapshot != expected_snapshot {
        panic!(
            "fixture expectation mismatch:\n{}",
            render_trace(&scenario, &trace)
        );
    }

    let model_report = evaluate_namespace_commit_invariants(CommitInvariantInputs {
        request: &request,
        before_head: &initial.head,
        before_lease: &initial.lease,
        before_metadata: &initial.metadata_state,
        prepared_wal: &prepared_wal,
        after_head: &model_head,
        after_metadata: &model_snapshot.metadata_state,
        now_ms: NOW_MS,
    });
    let core_report = evaluate_namespace_commit_invariants(CommitInvariantInputs {
        request: &request,
        before_head: &initial.head,
        before_lease: &initial.lease,
        before_metadata: &initial.metadata_state,
        prepared_wal: &prepared_wal,
        after_head: &core_head,
        after_metadata: &core_snapshot.metadata_state,
        now_ms: NOW_MS,
    });
    trace.extend(model_report.render_trace_lines("commit-model"));
    trace.extend(core_report.render_trace_lines("commit-core"));

    assert_report_agreement(&scenario, &trace, &model_report, &core_report);
    assert_expected_invariants(&scenario, &trace, &expect.invariants, &core_report);

    CommitFixtureRunReport {
        model_report,
        core_report,
        rendered_trace: render_trace(&scenario, &trace),
    }
}

#[derive(Debug, Clone, Deserialize)]
struct MetadataFixtureInitial {
    head: HeadState,
    lease: LeaseState,
    metadata_state: MetadataState,
    request: RawCommitRequest,
}

#[derive(Debug, Clone, Deserialize)]
struct MetadataFixtureExpect {
    head: HeadState,
    metadata_state: MetadataState,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawCommitRequest {
    namespace_id: NamespaceId,
    request_id: String,
    writer_id: String,
    writer_fence_token: loon_types::FenceToken,
    planned_head_seq: ChangeSeq,
    ops: Vec<RawCommitOp>,
    preconditions: Vec<RawPrecondition>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum RawCommitOp {
    CreateDir {
        create_dir: RawCreateDir,
    },
    CreateFile {
        create_file: RawCreateFile,
    },
    ReplaceFile {
        replace_file: RawReplaceFile,
    },
    Rename {
        rename: RawRename,
    },
    DeleteSubtree {
        delete_subtree: RawDeleteSubtree,
    },
    RestoreRevision {
        restore_revision: RawRestoreRevision,
    },
}

#[derive(Debug, Clone, Deserialize)]
struct RawCreateDir {
    parent_inode_id: InodeId,
    display_name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RawCreateFile {
    parent_inode_id: InodeId,
    display_name: String,
    content_manifest_digest: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RawReplaceFile {
    inode_id: InodeId,
    base_revision_no: RevisionNo,
    content_manifest_digest: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RawRename {
    inode_id: InodeId,
    new_parent_inode_id: InodeId,
    new_display_name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RawDeleteSubtree {
    root_inode_id: InodeId,
}

#[derive(Debug, Clone, Deserialize)]
struct RawRestoreRevision {
    inode_id: InodeId,
    base_revision_no: RevisionNo,
    restore_from_revision_no: RevisionNo,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum RawPrecondition {
    HeadSeqIs {
        head_seq_is: ChangeSeq,
    },
    ChildNameAbsent {
        child_name_absent: RawChildNameAbsent,
    },
    InodeRevisionIs {
        inode_revision_is: RawInodeRevisionIs,
    },
    AncestorsNotSubtreeDeleted {
        ancestors_not_subtree_deleted: RawAncestorsNotSubtreeDeleted,
    },
}

#[derive(Debug, Clone, Deserialize)]
struct RawChildNameAbsent {
    parent_inode_id: InodeId,
    name_key: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RawInodeRevisionIs {
    inode_id: InodeId,
    revision_no: RevisionNo,
}

#[derive(Debug, Clone, Deserialize)]
struct RawAncestorsNotSubtreeDeleted {
    inode_id: InodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NamespaceMetadataSnapshot {
    head: HeadState,
    metadata_state: MetadataState,
}

impl From<RawCommitRequest> for CommitRequest {
    fn from(value: RawCommitRequest) -> Self {
        Self {
            namespace_id: value.namespace_id,
            request_id: value.request_id,
            writer_id: value.writer_id,
            writer_fence_token: value.writer_fence_token,
            planned_head_seq: value.planned_head_seq,
            ops: value.ops.into_iter().map(CommitOp::from).collect(),
            preconditions: value
                .preconditions
                .into_iter()
                .map(Precondition::from)
                .collect(),
        }
    }
}

impl From<RawCommitOp> for CommitOp {
    fn from(value: RawCommitOp) -> Self {
        match value {
            RawCommitOp::CreateDir { create_dir } => CommitOp::CreateDir {
                parent_inode: create_dir.parent_inode_id,
                display_name: create_dir.display_name,
            },
            RawCommitOp::CreateFile { create_file } => CommitOp::CreateFile {
                parent_inode: create_file.parent_inode_id,
                display_name: create_file.display_name,
                content_manifest_digest: create_file.content_manifest_digest,
            },
            RawCommitOp::ReplaceFile { replace_file } => CommitOp::ReplaceFile {
                inode_id: replace_file.inode_id,
                base_revision: replace_file.base_revision_no,
                content_manifest_digest: replace_file.content_manifest_digest,
            },
            RawCommitOp::Rename { rename } => CommitOp::Rename {
                inode_id: rename.inode_id,
                new_parent_inode: rename.new_parent_inode_id,
                new_display_name: rename.new_display_name,
            },
            RawCommitOp::DeleteSubtree { delete_subtree } => CommitOp::DeleteSubtree {
                root_inode: delete_subtree.root_inode_id,
            },
            RawCommitOp::RestoreRevision { restore_revision } => CommitOp::RestoreRevision {
                inode_id: restore_revision.inode_id,
                base_revision: restore_revision.base_revision_no,
                restore_from_revision: restore_revision.restore_from_revision_no,
            },
        }
    }
}

impl From<RawPrecondition> for Precondition {
    fn from(value: RawPrecondition) -> Self {
        match value {
            RawPrecondition::HeadSeqIs { head_seq_is } => Self::HeadSeqIs(head_seq_is),
            RawPrecondition::ChildNameAbsent { child_name_absent } => Self::ChildNameAbsent {
                parent_inode: child_name_absent.parent_inode_id,
                name_key: child_name_absent.name_key,
            },
            RawPrecondition::InodeRevisionIs { inode_revision_is } => Self::InodeRevisionIs {
                inode_id: inode_revision_is.inode_id,
                revision: inode_revision_is.revision_no,
            },
            RawPrecondition::AncestorsNotSubtreeDeleted {
                ancestors_not_subtree_deleted,
            } => Self::AncestorsNotSubtreeDeleted {
                inode_id: ancestors_not_subtree_deleted.inode_id,
            },
        }
    }
}

fn model_namespace_from_fixture(initial: &MetadataFixtureInitial) -> ModelNamespace {
    ModelNamespace {
        namespace_id: initial.head.namespace_id.clone(),
        head_seq: initial.head.seq,
        active_fence_token: initial.head.active_fence_token,
        next_inode_id: initial.head.next_inode_id,
        snapshot_hint_seq: initial.head.snapshot_hint_seq,
        retention_floor_seq: initial.head.retention_floor_seq,
        metadata_state: model_metadata_from_core(&initial.metadata_state),
    }
}

fn model_metadata_from_core(metadata_state: &MetadataState) -> ModelMetadataState {
    ModelMetadataState {
        inodes: metadata_state
            .inodes
            .iter()
            .map(|inode| loon_testkit::model::ModelInodeRecord {
                inode_id: inode.inode_id,
                inode_kind: inode.inode_kind.clone(),
                created_seq: inode.created_seq,
            })
            .collect(),
        direntries: metadata_state
            .direntries
            .iter()
            .map(|direntry| loon_testkit::model::ModelDirentryRecord {
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
            .map(|revision| loon_testkit::model::ModelRevisionRecord {
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
            .map(|tombstone| loon_testkit::model::ModelSubtreeTombstoneRecord {
                root_inode_id: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
                tombstone_op_index: tombstone.tombstone_op_index,
            })
            .collect(),
    }
}

fn metadata_state_from_model(metadata_state: &ModelMetadataState) -> MetadataState {
    MetadataState {
        inodes: metadata_state
            .inodes
            .iter()
            .map(|inode| loon_server::core::metadata::InodeRecord {
                inode_id: inode.inode_id,
                inode_kind: inode.inode_kind.clone(),
                created_seq: inode.created_seq,
            })
            .collect(),
        direntries: metadata_state
            .direntries
            .iter()
            .map(|direntry| loon_server::core::metadata::DirentryRecord {
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
            .map(|revision| loon_server::core::metadata::RevisionRecord {
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
            .map(|tombstone| loon_server::core::metadata::SubtreeTombstoneRecord {
                root_inode_id: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
                tombstone_op_index: tombstone.tombstone_op_index,
            })
            .collect(),
    }
}

fn validate_model_metadata_preconditions(
    metadata_state: &ModelMetadataState,
    preconditions: &[RawPrecondition],
    base_seq: ChangeSeq,
) -> Result<(), ModelMetadataPreconditionError> {
    for precondition in preconditions {
        match precondition {
            RawPrecondition::HeadSeqIs { .. } => {}
            RawPrecondition::ChildNameAbsent { child_name_absent } => metadata_state
                .ensure_child_name_absent(
                    child_name_absent.parent_inode_id,
                    &child_name_absent.name_key,
                    base_seq,
                )?,
            RawPrecondition::InodeRevisionIs { inode_revision_is } => metadata_state
                .ensure_inode_revision_is(
                    inode_revision_is.inode_id,
                    inode_revision_is.revision_no,
                    base_seq,
                )?,
            RawPrecondition::AncestorsNotSubtreeDeleted {
                ancestors_not_subtree_deleted,
            } => metadata_state.ensure_ancestors_not_subtree_deleted(
                ancestors_not_subtree_deleted.inode_id,
                base_seq,
            )?,
        }
    }

    Ok(())
}

fn materialize_model_mutations(
    request: &RawCommitRequest,
    initial_next_inode_id: InodeId,
) -> (Vec<ModelMetadataMutation>, Vec<InodeId>, InodeId) {
    let mut next_inode = initial_next_inode_id;
    let mut allocated_inode_ids = Vec::new();
    let mutations = request
        .ops
        .iter()
        .map(|op| match op {
            RawCommitOp::CreateDir { create_dir } => {
                let inode_id = next_inode;
                next_inode = InodeId(next_inode.0.saturating_add(1));
                allocated_inode_ids.push(inode_id);
                ModelMetadataMutation::CreateDir {
                    inode_id,
                    parent_inode_id: create_dir.parent_inode_id,
                    display_name: create_dir.display_name.clone(),
                }
            }
            RawCommitOp::CreateFile { create_file } => {
                let inode_id = next_inode;
                next_inode = InodeId(next_inode.0.saturating_add(1));
                allocated_inode_ids.push(inode_id);
                ModelMetadataMutation::CreateFile {
                    inode_id,
                    parent_inode_id: create_file.parent_inode_id,
                    display_name: create_file.display_name.clone(),
                    content_manifest_digest: create_file.content_manifest_digest.clone(),
                }
            }
            RawCommitOp::ReplaceFile { replace_file } => ModelMetadataMutation::ReplaceFile {
                inode_id: replace_file.inode_id,
                base_revision_no: replace_file.base_revision_no,
                content_manifest_digest: replace_file.content_manifest_digest.clone(),
            },
            RawCommitOp::Rename { rename } => ModelMetadataMutation::Rename {
                inode_id: rename.inode_id,
                new_parent_inode_id: rename.new_parent_inode_id,
                new_display_name: rename.new_display_name.clone(),
            },
            RawCommitOp::DeleteSubtree { delete_subtree } => ModelMetadataMutation::DeleteSubtree {
                root_inode_id: delete_subtree.root_inode_id,
            },
            RawCommitOp::RestoreRevision { restore_revision } => {
                ModelMetadataMutation::RestoreRevision {
                    inode_id: restore_revision.inode_id,
                    base_revision_no: restore_revision.base_revision_no,
                    restore_from_revision_no: restore_revision.restore_from_revision_no,
                }
            }
        })
        .collect();

    (mutations, allocated_inode_ids, next_inode)
}

fn validate_model_request_ops(
    metadata_state: &ModelMetadataState,
    ops: &[RawCommitOp],
    base_seq: ChangeSeq,
) -> Result<(), ModelMetadataPreconditionError> {
    for op in ops {
        match op {
            RawCommitOp::Rename { rename } => {
                metadata_state.ensure_rename_source_binding_exists(rename.inode_id, base_seq)?;
                metadata_state.ensure_child_name_absent(
                    rename.new_parent_inode_id,
                    &rename.new_display_name,
                    base_seq,
                )?;
                metadata_state.ensure_rename_does_not_cycle(
                    rename.inode_id,
                    rename.new_parent_inode_id,
                    base_seq,
                )?;
            }
            RawCommitOp::DeleteSubtree { delete_subtree } => {
                metadata_state.ensure_inode_is_directory(delete_subtree.root_inode_id, base_seq)?;
            }
            RawCommitOp::RestoreRevision { restore_revision } => {
                metadata_state.ensure_inode_revision_is(
                    restore_revision.inode_id,
                    restore_revision.base_revision_no,
                    base_seq,
                )?;
                metadata_state.ensure_restore_source_revision_exists(
                    restore_revision.inode_id,
                    restore_revision.base_revision_no,
                    restore_revision.restore_from_revision_no,
                    base_seq,
                )?;
            }
            RawCommitOp::CreateDir { .. }
            | RawCommitOp::CreateFile { .. }
            | RawCommitOp::ReplaceFile { .. } => {}
        }
    }

    Ok(())
}

fn assert_report_agreement(
    scenario: &Scenario,
    trace: &[String],
    model_report: &NamespaceCoreInvariantReport,
    core_report: &NamespaceCoreInvariantReport,
) {
    for core_check in &core_report.checks {
        let Some(model_check) = model_report.check(&core_check.name) else {
            panic!(
                "model report missing invariant `{}`:\n{}",
                core_check.name,
                render_trace(scenario, trace)
            );
        };
        assert_eq!(
            model_check.passed,
            core_check.passed,
            "model/core invariant disagreement for `{}`:\n{}",
            core_check.name,
            render_trace(scenario, trace)
        );
    }
}

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
}

fn assert_expected_invariants(
    scenario: &Scenario,
    trace: &[String],
    expected: &[String],
    report: &NamespaceCoreInvariantReport,
) {
    for invariant in expected {
        let Some(check) = report.check(invariant) else {
            panic!(
                "expected invariant `{invariant}` was not evaluated:\n{}",
                render_trace(scenario, trace)
            );
        };
        assert!(
            check.passed,
            "expected invariant `{invariant}` evaluated false: {}:\n{}",
            check.detail,
            render_trace(scenario, trace)
        );
    }
}
