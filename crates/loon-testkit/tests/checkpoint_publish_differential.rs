use loon_core::checkpoint::{
    load_checkpoint, prepare_checkpoint_head_publish, publish_checkpoint_head,
    CheckpointHeadPublishRequest, StoredCheckpointManifest, StoredCheckpointSegment,
};
use loon_core::progress::load_retention_authorizers;
use loon_model::{
    ModelCheckpoint, ModelCheckpointFamily, ModelCheckpointPage, ModelCheckpointPublishAuthorizers,
    ModelCheckpointRow, ModelCheckpointSegment, ModelCheckpointTable, ModelNamespace,
    ModelProgressObject,
};
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::ObjectStore;
use loon_testkit::invariants::{
    evaluate_checkpoint_head_publish_invariants, BackgroundWorkInvariantReport,
    CheckpointHeadPublishInvariantInputs, CheckpointProgressAuthorizer,
};
use loon_testkit::render::render_trace;
use loon_testkit::scenario::Scenario;
use loon_types::{
    checkpoint_page_checksum_sha256, encode_checkpoint_manifest_json,
    encode_checkpoint_segment_envelope_zstd, ChangeSeq, CheckpointManifestEnvelope,
    CheckpointManifestPayload, CheckpointPage, CheckpointRow, CheckpointSegmentDescriptor,
    CheckpointSegmentEnvelope, CheckpointSegmentPayload, CheckpointTableFamily,
    CheckpointTableManifest, ControlObjectEnvelope, ControlObjectKind, FenceToken, HeadState,
    HeadStateEnvelope, InodeId, NamespaceId, ProgressState,
};
use serde::Deserialize;

const TEST_WRITER_VERSION: &str = "loon-testkit-differential";

#[test]
fn verified_checkpoint_publish_fixture_matches_model_and_core() {
    let report = run_fixture_report("native/verified_checkpoint_publish_updates_head_summary.yaml");
    assert!(!report.rendered_trace.is_empty());
}

#[test]
fn verified_checkpoint_publish_snapshot_matches_checked_in_artifact() {
    let report = run_fixture_report("native/verified_checkpoint_publish_updates_head_summary.yaml");
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/checkpoint-publish-differential/native/verified_checkpoint_publish_updates_head_summary.txt"
        )
    );
}

struct CheckpointPublishFixtureRunReport {
    rendered_trace: String,
}

fn run_fixture_report(relative_path: &str) -> CheckpointPublishFixtureRunReport {
    let scenario = load_fixture(relative_path);
    let initial: CheckpointPublishFixtureInitial =
        scenario.decode_initial().expect("decode initial");
    let actions: Vec<CheckpointPublishActionEnvelope> =
        scenario.decode_actions().expect("decode actions");
    let expect: CheckpointPublishFixtureExpect = scenario.decode_expect().expect("decode expect");
    assert_eq!(
        actions.len(),
        1,
        "checkpoint publish fixture expects one action"
    );
    let action = &actions[0].publish_checkpoint_to_head;

    let namespace_id = initial.head.namespace_id.clone();
    let stored_segments = initial
        .checkpoint_segments
        .iter()
        .map(stored_segment_from_fixture)
        .collect::<Vec<_>>();
    let stored_manifest =
        stored_manifest_from_fixture(&initial.checkpoint_manifest, &initial.checkpoint_segments);
    let loaded_checkpoint = load_checkpoint(&namespace_id, &stored_manifest, &stored_segments)
        .expect("load checkpoint");
    let model_checkpoint = model_checkpoint_from_fixture(
        &initial.checkpoint_manifest.payload,
        &initial.checkpoint_segments,
    );

    let temp_dir = TestDir::new("checkpoint-publish-differential");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    let head_etag = seed_head(&store, &initial.head);
    seed_progress_objects(&store, &initial.progress_objects);

    let retention_authorizers = load_retention_authorizers(
        &store,
        &namespace_id,
        &action.required_progress_work_classes,
        &action.retention_policy_work_class,
    )
    .expect("load retention authorizers");

    let mut trace = vec![format!(
        "initial head={:?} checkpoint_seq={} requested_retention_floor={:?}",
        initial.head,
        initial.checkpoint_manifest.payload.checkpoint_seq.0,
        action.requested_retention_floor_seq.map(|seq| seq.0)
    )];

    let prepared = prepare_checkpoint_head_publish(
        &initial.head,
        &loaded_checkpoint,
        &CheckpointHeadPublishRequest {
            requested_retention_floor_seq: action.requested_retention_floor_seq,
            retention_authorizers: Some(retention_authorizers.clone()),
        },
        TEST_WRITER_VERSION,
    )
    .expect("prepare checkpoint head publish");
    publish_checkpoint_head(&store, &head_etag, &prepared).expect("publish checkpoint head");
    let core_head = read_head(&store, &namespace_id);

    let mut model_namespace = ModelNamespace {
        namespace_id: initial.head.namespace_id.clone(),
        head_seq: initial.head.seq,
        active_fence_token: initial.head.active_fence_token,
        next_inode_id: initial.head.next_inode_id,
        snapshot_hint_seq: initial.head.snapshot_hint_seq,
        retention_floor_seq: initial.head.retention_floor_seq,
        metadata_state: Default::default(),
    };
    let model_authorizers = ModelCheckpointPublishAuthorizers {
        required_progress: action
            .required_progress_work_classes
            .iter()
            .map(|work_class| {
                progress_object_from_fixture(&initial.progress_objects, &namespace_id, work_class)
            })
            .collect(),
        retention_policy: progress_object_from_fixture(
            &initial.progress_objects,
            &namespace_id,
            &action.retention_policy_work_class,
        ),
    };
    model_namespace
        .publish_checkpoint(
            &model_checkpoint,
            &available_segment_keys(&model_checkpoint),
            action.requested_retention_floor_seq,
            Some(&model_authorizers),
        )
        .expect("model publish checkpoint");
    let model_head = HeadState {
        namespace_id: model_namespace.namespace_id.clone(),
        seq: model_namespace.head_seq,
        active_fence_token: model_namespace.active_fence_token,
        next_inode_id: model_namespace.next_inode_id,
        snapshot_hint_seq: model_namespace.snapshot_hint_seq,
        retention_floor_seq: model_namespace.retention_floor_seq,
    };

    trace.push(format!(
        "final expected={:?} model={:?} core={:?}",
        expect.head, model_head, core_head
    ));

    if model_head != core_head {
        panic!(
            "checkpoint publish model/core divergence:\n{}",
            render_trace(&scenario, &trace)
        );
    }
    if core_head != expect.head {
        panic!(
            "checkpoint publish expectation mismatch:\n{}",
            render_trace(&scenario, &trace)
        );
    }

    let model_required_progress = model_authorizers
        .required_progress
        .iter()
        .map(model_authorizer)
        .collect::<Vec<_>>();
    let model_retention_policy = model_authorizer(&model_authorizers.retention_policy);
    let model_report =
        evaluate_checkpoint_head_publish_invariants(CheckpointHeadPublishInvariantInputs {
            current_head: &initial.head,
            checkpoint_namespace: &model_checkpoint.namespace_id,
            checkpoint_seq: model_checkpoint.checkpoint_seq,
            checkpoint_verified: model_checkpoint.verified,
            checkpoint_segments_verified: true,
            requested_retention_floor_seq: action.requested_retention_floor_seq,
            required_progress: &model_required_progress,
            retention_policy: Some(model_retention_policy),
            resulting_head: &model_head,
        });
    let core_required_progress = retention_authorizers
        .required_progress
        .iter()
        .map(core_authorizer)
        .collect::<Vec<_>>();
    let core_retention_policy = core_authorizer(&retention_authorizers.retention_policy);
    let core_report =
        evaluate_checkpoint_head_publish_invariants(CheckpointHeadPublishInvariantInputs {
            current_head: &initial.head,
            checkpoint_namespace: &loaded_checkpoint.manifest.payload.namespace_id,
            checkpoint_seq: loaded_checkpoint.manifest.payload.checkpoint_seq,
            checkpoint_verified: loaded_checkpoint.manifest.payload.verified,
            checkpoint_segments_verified: loaded_checkpoint
                .checked_invariants
                .iter()
                .any(|name| name == "checkpoint_segment_descriptor_matches_payload"),
            requested_retention_floor_seq: action.requested_retention_floor_seq,
            required_progress: &core_required_progress,
            retention_policy: Some(core_retention_policy),
            resulting_head: &core_head,
        });
    assert_reports_match_and_pass(
        &scenario,
        &mut trace,
        &model_report,
        &core_report,
        &expect.invariants,
    );

    CheckpointPublishFixtureRunReport {
        rendered_trace: render_trace(&scenario, &trace),
    }
}

#[derive(Debug, Deserialize)]
struct CheckpointPublishFixtureInitial {
    head: HeadState,
    checkpoint_manifest: FixtureCheckpointManifest,
    #[serde(default)]
    checkpoint_segments: Vec<FixtureCheckpointSegment>,
    #[serde(default)]
    progress_objects: Vec<FixtureProgressObject>,
}

#[derive(Debug, Deserialize)]
struct CheckpointPublishFixtureExpect {
    head: HeadState,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureCheckpointManifest {
    key: String,
    payload: FixtureCheckpointManifestPayload,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureCheckpointSegment {
    key: String,
    payload: FixtureCheckpointSegmentPayload,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureCheckpointManifestPayload {
    namespace_id: NamespaceId,
    checkpoint_seq: ChangeSeq,
    active_fence_token: FenceToken,
    next_inode_id: InodeId,
    retention_floor_seq: ChangeSeq,
    verified: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureCheckpointSegmentPayload {
    namespace_id: NamespaceId,
    checkpoint_seq: ChangeSeq,
    family: CheckpointTableFamily,
    segment_index: u32,
    row_count: u64,
    #[serde(default)]
    min_key: String,
    #[serde(default)]
    max_key: String,
    #[serde(default)]
    pages: Vec<CheckpointPage>,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureProgressObject {
    key: String,
    payload: ProgressState,
}

#[derive(Debug, Deserialize)]
struct CheckpointPublishActionEnvelope {
    publish_checkpoint_to_head: PublishCheckpointToHeadAction,
}

#[derive(Debug, Deserialize)]
struct PublishCheckpointToHeadAction {
    #[serde(default)]
    requested_retention_floor_seq: Option<ChangeSeq>,
    #[serde(default)]
    required_progress_work_classes: Vec<String>,
    retention_policy_work_class: String,
}

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
}

fn seed_head(store: &LocalFsStore, head: &HeadState) -> String {
    let envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        TEST_WRITER_VERSION,
        head.clone(),
    )
    .expect("build head envelope");
    store
        .put_if_absent(
            &loon_objectstore::keys::namespace_head(head.namespace_id.as_str()),
            &serde_json::to_vec(&envelope).expect("encode head"),
        )
        .expect("seed head")
        .etag
        .expect("seeded head etag")
}

fn read_head(store: &LocalFsStore, namespace_id: &NamespaceId) -> HeadState {
    let bytes = store
        .get(
            &loon_objectstore::keys::namespace_head(namespace_id.as_str()),
            None,
        )
        .expect("read head bytes")
        .expect("head object exists");
    let envelope: HeadStateEnvelope = serde_json::from_slice(&bytes).expect("decode head");
    envelope.state
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
            .expect("seed progress object");
    }
}

fn checkpoint_manifest_payload_from_fixture(
    manifest: &FixtureCheckpointManifestPayload,
    segments: &[FixtureCheckpointSegment],
) -> CheckpointManifestPayload {
    CheckpointManifestPayload {
        namespace_id: manifest.namespace_id.clone(),
        checkpoint_seq: manifest.checkpoint_seq,
        active_fence_token: manifest.active_fence_token,
        next_inode_id: manifest.next_inode_id,
        retention_floor_seq: manifest.retention_floor_seq,
        verified: manifest.verified,
        tables: [
            CheckpointTableFamily::Inodes,
            CheckpointTableFamily::Direntries,
            CheckpointTableFamily::Revisions,
            CheckpointTableFamily::Tombstones,
        ]
        .into_iter()
        .filter_map(|family| {
            let segments = segments
                .iter()
                .filter(|segment| segment.payload.family == family)
                .map(checkpoint_segment_descriptor_from_fixture)
                .collect::<Vec<_>>();
            if segments.is_empty() {
                None
            } else {
                Some(CheckpointTableManifest { family, segments })
            }
        })
        .collect(),
    }
}

fn checkpoint_segment_payload_from_fixture(
    payload: &FixtureCheckpointSegmentPayload,
) -> CheckpointSegmentPayload {
    let min_key = if payload.min_key.is_empty() {
        payload
            .pages
            .first()
            .map(|page| page.min_key.clone())
            .unwrap_or_default()
    } else {
        payload.min_key.clone()
    };
    let max_key = if payload.max_key.is_empty() {
        payload
            .pages
            .last()
            .map(|page| page.max_key.clone())
            .unwrap_or_default()
    } else {
        payload.max_key.clone()
    };

    CheckpointSegmentPayload {
        namespace_id: payload.namespace_id.clone(),
        checkpoint_seq: payload.checkpoint_seq,
        family: payload.family,
        segment_index: payload.segment_index,
        row_count: payload.row_count,
        min_key,
        max_key,
        pages: payload.pages.clone(),
    }
}

fn checkpoint_segment_descriptor_from_fixture(
    segment: &FixtureCheckpointSegment,
) -> CheckpointSegmentDescriptor {
    let payload = checkpoint_segment_payload_from_fixture(&segment.payload);
    let envelope = CheckpointSegmentEnvelope::from_payload(TEST_WRITER_VERSION, payload.clone())
        .expect("build checkpoint segment envelope");
    CheckpointSegmentDescriptor {
        object_key: segment.key.clone(),
        segment_index: payload.segment_index,
        row_count: payload.row_count,
        min_key: payload.min_key.clone(),
        max_key: payload.max_key.clone(),
        payload_checksum_sha256: envelope.payload_checksum_sha256.clone(),
        page_checksums_sha256: payload
            .pages
            .iter()
            .map(checkpoint_page_checksum_sha256)
            .collect::<Result<Vec<_>, _>>()
            .expect("checkpoint page checksums"),
    }
}

fn stored_manifest_from_fixture(
    fixture: &FixtureCheckpointManifest,
    segments: &[FixtureCheckpointSegment],
) -> StoredCheckpointManifest {
    let envelope = CheckpointManifestEnvelope::from_payload(
        TEST_WRITER_VERSION,
        checkpoint_manifest_payload_from_fixture(&fixture.payload, segments),
    )
    .expect("build checkpoint manifest");
    StoredCheckpointManifest {
        object_key: fixture.key.clone(),
        encoded_bytes: encode_checkpoint_manifest_json(&envelope)
            .expect("encode checkpoint manifest"),
    }
}

fn stored_segment_from_fixture(fixture: &FixtureCheckpointSegment) -> StoredCheckpointSegment {
    let envelope = CheckpointSegmentEnvelope::from_payload(
        TEST_WRITER_VERSION,
        checkpoint_segment_payload_from_fixture(&fixture.payload),
    )
    .expect("build checkpoint segment");
    StoredCheckpointSegment {
        object_key: fixture.key.clone(),
        encoded_bytes: encode_checkpoint_segment_envelope_zstd(&envelope)
            .expect("encode checkpoint segment"),
    }
}

fn model_checkpoint_from_fixture(
    manifest: &FixtureCheckpointManifestPayload,
    segments: &[FixtureCheckpointSegment],
) -> ModelCheckpoint {
    ModelCheckpoint {
        namespace_id: manifest.namespace_id.clone(),
        checkpoint_seq: manifest.checkpoint_seq,
        active_fence_token: manifest.active_fence_token,
        next_inode_id: manifest.next_inode_id,
        retention_floor_seq: manifest.retention_floor_seq,
        verified: manifest.verified,
        tables: checkpoint_manifest_payload_from_fixture(manifest, segments)
            .tables
            .iter()
            .map(|table| ModelCheckpointTable {
                family: model_family(table.family),
                segments: table
                    .segments
                    .iter()
                    .map(|descriptor| {
                        let fixture_segment = segments
                            .iter()
                            .find(|segment| segment.key == descriptor.object_key)
                            .expect("segment should exist");
                        ModelCheckpointSegment {
                            object_key: descriptor.object_key.clone(),
                            segment_index: descriptor.segment_index,
                            row_count: descriptor.row_count,
                            pages: fixture_segment
                                .payload
                                .pages
                                .iter()
                                .map(|page| ModelCheckpointPage {
                                    page_index: page.page_index,
                                    min_key: page.min_key.clone(),
                                    max_key: page.max_key.clone(),
                                    row_keys: page.row_keys.clone(),
                                    rows: page.rows.iter().map(model_row).collect(),
                                })
                                .collect(),
                        }
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn available_segment_keys(checkpoint: &ModelCheckpoint) -> Vec<String> {
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

fn progress_object_from_fixture(
    progress_objects: &[FixtureProgressObject],
    namespace_id: &NamespaceId,
    work_class: &str,
) -> ModelProgressObject {
    let progress = progress_objects
        .iter()
        .find(|progress| {
            progress.payload.namespace_id == *namespace_id
                && progress.payload.work_class == work_class
        })
        .expect("progress object should exist");
    ModelProgressObject {
        namespace_id: progress.payload.namespace_id.clone(),
        work_class: progress.payload.work_class.clone(),
        through_seq: progress.payload.through_seq,
    }
}

fn model_authorizer(progress: &ModelProgressObject) -> CheckpointProgressAuthorizer<'_> {
    CheckpointProgressAuthorizer {
        namespace_id: &progress.namespace_id,
        work_class: &progress.work_class,
        through_seq: progress.through_seq,
    }
}

fn core_authorizer(
    progress: &loon_core::progress::LoadedProgressObject,
) -> CheckpointProgressAuthorizer<'_> {
    CheckpointProgressAuthorizer {
        namespace_id: &progress.envelope.state.namespace_id,
        work_class: &progress.envelope.state.work_class,
        through_seq: progress.envelope.state.through_seq,
    }
}

fn model_family(family: CheckpointTableFamily) -> ModelCheckpointFamily {
    match family {
        CheckpointTableFamily::Inodes => ModelCheckpointFamily::Inodes,
        CheckpointTableFamily::Direntries => ModelCheckpointFamily::Direntries,
        CheckpointTableFamily::Revisions => ModelCheckpointFamily::Revisions,
        CheckpointTableFamily::Tombstones => ModelCheckpointFamily::Tombstones,
    }
}

fn model_row(row: &CheckpointRow) -> ModelCheckpointRow {
    match row {
        CheckpointRow::Inode {
            inode_id,
            inode_kind,
            created_seq,
        } => ModelCheckpointRow::Inode {
            inode_id: *inode_id,
            inode_kind: inode_kind.clone(),
            created_seq: *created_seq,
        },
        CheckpointRow::Direntry {
            parent_inode_id,
            name_key,
            display_name,
            child_inode_id,
            bind_seq,
            bind_op_index,
        } => ModelCheckpointRow::Direntry {
            parent_inode_id: *parent_inode_id,
            name_key: name_key.clone(),
            display_name: display_name.clone(),
            child_inode_id: *child_inode_id,
            bind_seq: *bind_seq,
            bind_op_index: *bind_op_index,
        },
        CheckpointRow::Revision {
            inode_id,
            revision_no,
            committed_seq,
            revision_op_index,
            content_manifest_digest,
        } => ModelCheckpointRow::Revision {
            inode_id: *inode_id,
            revision_no: *revision_no,
            committed_seq: *committed_seq,
            revision_op_index: *revision_op_index,
            content_manifest_digest: content_manifest_digest.clone(),
        },
        CheckpointRow::Tombstone {
            root_inode_id,
            tombstone_seq,
            tombstone_op_index,
        } => ModelCheckpointRow::Tombstone {
            root_inode_id: *root_inode_id,
            tombstone_seq: *tombstone_seq,
            tombstone_op_index: *tombstone_op_index,
        },
    }
}

fn assert_reports_match_and_pass(
    scenario: &Scenario,
    trace: &mut Vec<String>,
    model_report: &BackgroundWorkInvariantReport,
    core_report: &BackgroundWorkInvariantReport,
    expected_invariants: &[String],
) {
    trace.extend(model_report.render_trace_lines("checkpoint-publish-model"));
    trace.extend(core_report.render_trace_lines("checkpoint-publish-core"));
    if model_report.checks != core_report.checks {
        panic!(
            "checkpoint publish invariant mismatch:\n{}",
            render_trace(scenario, trace)
        );
    }
    for check in &core_report.checks {
        if !check.passed {
            panic!(
                "checkpoint publish invariant failed: {}:\n{}",
                check.name,
                render_trace(scenario, trace)
            );
        }
    }
    for invariant in expected_invariants {
        assert!(
            core_report
                .check(invariant)
                .is_some_and(|check| check.passed),
            "missing expected checkpoint publish invariant `{invariant}`:\n{}",
            render_trace(scenario, trace)
        );
    }
}

type TestDir = loon_testkit::tempdir::TestDir;
