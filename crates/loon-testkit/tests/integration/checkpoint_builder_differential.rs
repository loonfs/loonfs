use loon_server::core::checkpoint::{
    load_checkpoint, prepare_checkpoint, PreparedCheckpoint, StoredCheckpointManifest,
    StoredCheckpointSegment,
};
use loon_server::core::invariants::CHECKPOINT_OBJECT_IMMUTABLE_INVARIANTS;
use loon_server::core::metadata::MetadataState;
use loon_server::objectstore::keys::snapshot_manifest;
use loon_testkit::invariants::{
    evaluate_checkpoint_object_invariants, CheckpointObjectInvariantInputs,
    CheckpointObjectInvariantReport, CheckpointObjectInvariantSnapshot,
    StoredCheckpointSegmentSnapshot,
};
use loon_testkit::model::{
    ModelCheckpoint, ModelCheckpointFamily, ModelCheckpointPage, ModelCheckpointRow,
    ModelCheckpointSegment, ModelMetadataState, ModelNamespace,
};
use loon_testkit::render::render_trace;
use loon_testkit::scenario::Scenario;
use loon_types::{
    checkpoint_page_checksum_sha256, encode_checkpoint_manifest_json,
    encode_checkpoint_segment_envelope_zstd, CheckpointManifestEnvelope, CheckpointManifestPayload,
    CheckpointPage, CheckpointRow, CheckpointSegmentDescriptor, CheckpointSegmentEnvelope,
    CheckpointSegmentPayload, CheckpointTableFamily, CheckpointTableManifest, FenceToken,
    HeadState, InodeId, NamespaceId,
};
use serde::Deserialize;

const TEST_WRITER_VERSION: &str = "loon-testkit-differential";

#[test]
fn checkpoint_builder_fixture_matches_model_and_core() {
    let report =
        run_fixture_report("native/checkpoint_builder_writes_verified_manifest_and_segments.yaml");
    assert!(!report.rendered_trace.is_empty());
}

#[test]
fn checkpoint_builder_snapshot_matches_checked_in_artifact() {
    let report =
        run_fixture_report("native/checkpoint_builder_writes_verified_manifest_and_segments.yaml");
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../../tests/snapshots/checkpoint-builder-differential/native/checkpoint_builder_writes_verified_manifest_and_segments.txt"
        )
    );
}

struct CheckpointBuilderFixtureRunReport {
    rendered_trace: String,
}

fn run_fixture_report(relative_path: &str) -> CheckpointBuilderFixtureRunReport {
    let scenario = load_fixture(relative_path);
    let initial: CheckpointBuilderInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<CheckpointBuilderAction> = scenario.decode_actions().expect("decode actions");
    let expect: CheckpointBuilderExpect = scenario.decode_expect().expect("decode expect");

    assert_eq!(
        actions.len(),
        1,
        "checkpoint builder fixture expects one action"
    );
    assert!(
        actions[0].build_checkpoint,
        "fixture action should build checkpoint"
    );

    let model_namespace = ModelNamespace {
        namespace_id: initial.head.namespace_id.clone(),
        head_seq: initial.head.seq,
        active_fence_token: initial.head.active_fence_token,
        next_inode_id: initial.head.next_inode_id,
        snapshot_hint_seq: initial.head.snapshot_hint_seq,
        retention_floor_seq: initial.head.retention_floor_seq,
        metadata_state: model_metadata_from_core(&initial.metadata_state),
    };
    let model_checkpoint = model_namespace.checkpoint();
    let core_checkpoint =
        prepare_checkpoint(&initial.head, &initial.metadata_state, TEST_WRITER_VERSION)
            .expect("prepare checkpoint");

    let model_snapshot =
        checkpoint_snapshot_from_model(&initial.head, &initial.metadata_state, &model_checkpoint);
    let core_snapshot =
        checkpoint_snapshot_from_core(&initial.head, &initial.metadata_state, &core_checkpoint);

    let model_report = evaluate_checkpoint_object_invariants(CheckpointObjectInvariantInputs {
        checkpoint: &model_snapshot,
    });
    let core_report = evaluate_checkpoint_object_invariants(CheckpointObjectInvariantInputs {
        checkpoint: &core_snapshot,
    });

    let model_manifest_summary = manifest_summary_from_snapshot(&model_snapshot);
    let core_manifest_summary = manifest_summary_from_snapshot(&core_snapshot);
    let expected_manifest_summary = manifest_summary_from_expect(&expect.checkpoint_manifest);
    let model_segment_summaries = segment_summaries_from_snapshot(&model_snapshot);
    let core_segment_summaries = segment_summaries_from_snapshot(&core_snapshot);
    let expected_segment_summaries = expect
        .checkpoint_segments
        .iter()
        .map(segment_summary_from_expect)
        .collect::<Vec<_>>();
    let model_basis_metadata =
        load_basis_metadata_from_snapshot(&initial.head.namespace_id, &model_snapshot);
    let core_basis_metadata =
        load_basis_metadata_from_snapshot(&initial.head.namespace_id, &core_snapshot);

    let expected_invariants = filter_checkpoint_object_invariants(&expect.invariants);
    let mut trace = vec![
        format!(
            "initial head=(namespace={}, seq={}, fence={}, next_inode={}, retention_floor={}) metadata_rows=(inodes={}, direntries={}, revisions={}, tombstones={})",
            initial.head.namespace_id,
            initial.head.seq.0,
            initial.head.active_fence_token.0,
            initial.head.next_inode_id.0,
            initial.head.retention_floor_seq.0,
            initial.metadata_state.inodes.len(),
            initial.metadata_state.direntries.len(),
            initial.metadata_state.revisions.len(),
            initial.metadata_state.subtree_tombstones.len()
        ),
        format!(
            "manifest expected={:?} model={:?} core={:?}",
            expected_manifest_summary, model_manifest_summary, core_manifest_summary
        ),
        format!(
            "segments expected={:?} model={:?} core={:?}",
            expected_segment_summaries, model_segment_summaries, core_segment_summaries
        ),
        format!(
            "basis_metadata_matches expected_vs_model={} expected_vs_core={}",
            initial.metadata_state == model_basis_metadata,
            initial.metadata_state == core_basis_metadata
        ),
    ];

    assert_eq!(model_manifest_summary, core_manifest_summary);
    assert_eq!(core_manifest_summary, expected_manifest_summary);
    assert_eq!(model_segment_summaries, core_segment_summaries);
    assert_eq!(core_segment_summaries, expected_segment_summaries);
    assert_eq!(model_basis_metadata, core_basis_metadata);
    assert_eq!(core_basis_metadata, initial.metadata_state);

    assert_reports_match_and_pass(
        &scenario,
        &mut trace,
        &model_report,
        &core_report,
        &expected_invariants,
    );

    CheckpointBuilderFixtureRunReport {
        rendered_trace: render_trace(&scenario, &trace),
    }
}

#[derive(Debug, Deserialize)]
struct CheckpointBuilderInitial {
    head: HeadState,
    #[serde(default)]
    metadata_state: MetadataState,
}

#[derive(Debug, Deserialize)]
struct CheckpointBuilderAction {
    build_checkpoint: bool,
}

#[derive(Debug, Deserialize)]
struct CheckpointBuilderExpect {
    checkpoint_manifest: FixtureCheckpointManifest,
    #[serde(default)]
    checkpoint_segments: Vec<FixtureCheckpointSegment>,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureCheckpointManifest {
    key: String,
    payload: FixtureCheckpointManifestPayload,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureCheckpointManifestPayload {
    namespace_id: NamespaceId,
    checkpoint_seq: loon_types::ChangeSeq,
    active_fence_token: FenceToken,
    next_inode_id: InodeId,
    retention_floor_seq: loon_types::ChangeSeq,
    verified: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureCheckpointSegment {
    key: String,
    payload: FixtureCheckpointSegmentPayload,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureCheckpointSegmentPayload {
    namespace_id: NamespaceId,
    checkpoint_seq: loon_types::ChangeSeq,
    family: CheckpointTableFamily,
    segment_index: u32,
    row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckpointManifestSummary {
    key: String,
    namespace_id: NamespaceId,
    checkpoint_seq: loon_types::ChangeSeq,
    active_fence_token: FenceToken,
    next_inode_id: InodeId,
    retention_floor_seq: loon_types::ChangeSeq,
    verified: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckpointSegmentSummary {
    key: String,
    namespace_id: NamespaceId,
    checkpoint_seq: loon_types::ChangeSeq,
    family: CheckpointTableFamily,
    segment_index: u32,
    row_count: u64,
}

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
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
            .map(
                |tombstone| loon_testkit::model::ModelSubtreeTombstoneRecord {
                    root_inode_id: tombstone.root_inode_id,
                    tombstone_seq: tombstone.tombstone_seq,
                    tombstone_op_index: tombstone.tombstone_op_index,
                },
            )
            .collect(),
    }
}

fn checkpoint_snapshot_from_core(
    source_head: &HeadState,
    source_metadata: &MetadataState,
    prepared: &PreparedCheckpoint,
) -> CheckpointObjectInvariantSnapshot {
    CheckpointObjectInvariantSnapshot {
        source_head: source_head.clone(),
        source_basis_metadata: source_metadata.clone(),
        manifest_object_key: prepared.manifest.object_key.clone(),
        manifest_bytes: prepared.manifest.encoded_bytes.clone(),
        manifest_envelope: prepared.manifest.envelope.clone(),
        segments: prepared
            .segments
            .iter()
            .map(|segment| StoredCheckpointSegmentSnapshot {
                object_key: segment.object_key.clone(),
                encoded_bytes: segment.encoded_bytes.clone(),
                envelope: segment.envelope.clone(),
            })
            .collect(),
    }
}

fn checkpoint_snapshot_from_model(
    source_head: &HeadState,
    source_metadata: &MetadataState,
    checkpoint: &ModelCheckpoint,
) -> CheckpointObjectInvariantSnapshot {
    let segments = checkpoint
        .tables
        .iter()
        .flat_map(|table| {
            table.segments.iter().map(move |segment| {
                let payload = checkpoint_segment_payload_from_model(
                    &checkpoint.namespace_id,
                    checkpoint.checkpoint_seq,
                    table.family,
                    segment,
                );
                let envelope =
                    CheckpointSegmentEnvelope::from_payload(TEST_WRITER_VERSION, payload.clone())
                        .expect("build model checkpoint segment envelope");
                let encoded_bytes = encode_checkpoint_segment_envelope_zstd(&envelope)
                    .expect("encode model checkpoint segment");
                StoredCheckpointSegmentSnapshot {
                    object_key: segment.object_key.clone(),
                    encoded_bytes,
                    envelope,
                }
            })
        })
        .collect::<Vec<_>>();

    let tables = checkpoint
        .tables
        .iter()
        .map(|table| CheckpointTableManifest {
            family: checkpoint_family_from_model(table.family),
            segments: table
                .segments
                .iter()
                .map(|segment| {
                    let snapshot = segments
                        .iter()
                        .find(|candidate| candidate.object_key == segment.object_key)
                        .expect("segment snapshot should exist");
                    checkpoint_segment_descriptor_from_snapshot(snapshot)
                })
                .collect(),
        })
        .collect();
    let manifest_envelope = CheckpointManifestEnvelope::from_payload(
        TEST_WRITER_VERSION,
        CheckpointManifestPayload {
            namespace_id: checkpoint.namespace_id.clone(),
            checkpoint_seq: checkpoint.checkpoint_seq,
            active_fence_token: checkpoint.active_fence_token,
            next_inode_id: checkpoint.next_inode_id,
            retention_floor_seq: checkpoint.retention_floor_seq,
            verified: checkpoint.verified,
            tables,
        },
    )
    .expect("build model checkpoint manifest");
    let manifest_bytes = encode_checkpoint_manifest_json(&manifest_envelope)
        .expect("encode model checkpoint manifest");

    CheckpointObjectInvariantSnapshot {
        source_head: source_head.clone(),
        source_basis_metadata: source_metadata.clone(),
        manifest_object_key: snapshot_manifest(
            checkpoint.namespace_id.as_str(),
            checkpoint.checkpoint_seq.0,
        ),
        manifest_bytes,
        manifest_envelope,
        segments,
    }
}

fn checkpoint_segment_payload_from_model(
    namespace_id: &NamespaceId,
    checkpoint_seq: loon_types::ChangeSeq,
    family: ModelCheckpointFamily,
    segment: &ModelCheckpointSegment,
) -> CheckpointSegmentPayload {
    CheckpointSegmentPayload {
        namespace_id: namespace_id.clone(),
        checkpoint_seq,
        family: checkpoint_family_from_model(family),
        segment_index: segment.segment_index,
        row_count: segment.row_count,
        min_key: segment
            .pages
            .first()
            .map(|page| page.min_key.clone())
            .unwrap_or_default(),
        max_key: segment
            .pages
            .last()
            .map(|page| page.max_key.clone())
            .unwrap_or_default(),
        pages: segment
            .pages
            .iter()
            .map(checkpoint_page_from_model)
            .collect(),
    }
}

fn checkpoint_page_from_model(page: &ModelCheckpointPage) -> CheckpointPage {
    CheckpointPage {
        page_index: page.page_index,
        min_key: page.min_key.clone(),
        max_key: page.max_key.clone(),
        row_keys: page.row_keys.clone(),
        rows: page.rows.iter().map(checkpoint_row_from_model).collect(),
    }
}

fn checkpoint_row_from_model(row: &ModelCheckpointRow) -> CheckpointRow {
    match row {
        ModelCheckpointRow::Inode {
            inode_id,
            inode_kind,
            created_seq,
        } => CheckpointRow::Inode {
            inode_id: *inode_id,
            inode_kind: inode_kind.clone(),
            created_seq: *created_seq,
        },
        ModelCheckpointRow::Direntry {
            parent_inode_id,
            name_key,
            display_name,
            child_inode_id,
            bind_seq,
            bind_op_index,
        } => CheckpointRow::Direntry {
            parent_inode_id: *parent_inode_id,
            name_key: name_key.clone(),
            display_name: display_name.clone(),
            child_inode_id: *child_inode_id,
            bind_seq: *bind_seq,
            bind_op_index: *bind_op_index,
        },
        ModelCheckpointRow::Revision {
            inode_id,
            revision_no,
            committed_seq,
            revision_op_index,
            content_manifest_digest,
        } => CheckpointRow::Revision {
            inode_id: *inode_id,
            revision_no: *revision_no,
            committed_seq: *committed_seq,
            revision_op_index: *revision_op_index,
            content_manifest_digest: content_manifest_digest.clone(),
        },
        ModelCheckpointRow::Tombstone {
            root_inode_id,
            tombstone_seq,
            tombstone_op_index,
        } => CheckpointRow::Tombstone {
            root_inode_id: *root_inode_id,
            tombstone_seq: *tombstone_seq,
            tombstone_op_index: *tombstone_op_index,
        },
    }
}

fn checkpoint_segment_descriptor_from_snapshot(
    snapshot: &StoredCheckpointSegmentSnapshot,
) -> CheckpointSegmentDescriptor {
    CheckpointSegmentDescriptor {
        object_key: snapshot.object_key.clone(),
        segment_index: snapshot.envelope.payload.segment_index,
        row_count: snapshot.envelope.payload.row_count,
        min_key: snapshot.envelope.payload.min_key.clone(),
        max_key: snapshot.envelope.payload.max_key.clone(),
        payload_checksum_sha256: snapshot.envelope.payload_checksum_sha256.clone(),
        page_checksums_sha256: snapshot
            .envelope
            .payload
            .pages
            .iter()
            .map(checkpoint_page_checksum_sha256)
            .collect::<Result<Vec<_>, _>>()
            .expect("page checksums"),
    }
}

fn load_basis_metadata_from_snapshot(
    namespace_id: &NamespaceId,
    snapshot: &CheckpointObjectInvariantSnapshot,
) -> MetadataState {
    let loaded = load_checkpoint(
        namespace_id,
        &StoredCheckpointManifest {
            object_key: snapshot.manifest_object_key.clone(),
            encoded_bytes: snapshot.manifest_bytes.clone(),
        },
        &snapshot
            .segments
            .iter()
            .map(|segment| StoredCheckpointSegment {
                object_key: segment.object_key.clone(),
                encoded_bytes: segment.encoded_bytes.clone(),
            })
            .collect::<Vec<_>>(),
    )
    .expect("load checkpoint from snapshot");
    loaded.basis_metadata_state
}

fn manifest_summary_from_snapshot(
    snapshot: &CheckpointObjectInvariantSnapshot,
) -> CheckpointManifestSummary {
    CheckpointManifestSummary {
        key: snapshot.manifest_object_key.clone(),
        namespace_id: snapshot.manifest_envelope.payload.namespace_id.clone(),
        checkpoint_seq: snapshot.manifest_envelope.payload.checkpoint_seq,
        active_fence_token: snapshot.manifest_envelope.payload.active_fence_token,
        next_inode_id: snapshot.manifest_envelope.payload.next_inode_id,
        retention_floor_seq: snapshot.manifest_envelope.payload.retention_floor_seq,
        verified: snapshot.manifest_envelope.payload.verified,
    }
}

fn manifest_summary_from_expect(expect: &FixtureCheckpointManifest) -> CheckpointManifestSummary {
    CheckpointManifestSummary {
        key: expect.key.clone(),
        namespace_id: expect.payload.namespace_id.clone(),
        checkpoint_seq: expect.payload.checkpoint_seq,
        active_fence_token: expect.payload.active_fence_token,
        next_inode_id: expect.payload.next_inode_id,
        retention_floor_seq: expect.payload.retention_floor_seq,
        verified: expect.payload.verified,
    }
}

fn segment_summaries_from_snapshot(
    snapshot: &CheckpointObjectInvariantSnapshot,
) -> Vec<CheckpointSegmentSummary> {
    snapshot
        .segments
        .iter()
        .map(|segment| CheckpointSegmentSummary {
            key: segment.object_key.clone(),
            namespace_id: snapshot.manifest_envelope.payload.namespace_id.clone(),
            checkpoint_seq: snapshot.manifest_envelope.payload.checkpoint_seq,
            family: segment.envelope.payload.family,
            segment_index: segment.envelope.payload.segment_index,
            row_count: segment.envelope.payload.row_count,
        })
        .collect()
}

fn segment_summary_from_expect(expect: &FixtureCheckpointSegment) -> CheckpointSegmentSummary {
    CheckpointSegmentSummary {
        key: expect.key.clone(),
        namespace_id: expect.payload.namespace_id.clone(),
        checkpoint_seq: expect.payload.checkpoint_seq,
        family: expect.payload.family,
        segment_index: expect.payload.segment_index,
        row_count: expect.payload.row_count,
    }
}

fn checkpoint_family_from_model(family: ModelCheckpointFamily) -> CheckpointTableFamily {
    match family {
        ModelCheckpointFamily::Inodes => CheckpointTableFamily::Inodes,
        ModelCheckpointFamily::Direntries => CheckpointTableFamily::Direntries,
        ModelCheckpointFamily::Revisions => CheckpointTableFamily::Revisions,
        ModelCheckpointFamily::Tombstones => CheckpointTableFamily::Tombstones,
    }
}

fn assert_reports_match_and_pass(
    scenario: &Scenario,
    trace: &mut Vec<String>,
    model_report: &CheckpointObjectInvariantReport,
    core_report: &CheckpointObjectInvariantReport,
    expected_invariants: &[String],
) {
    if model_report != core_report {
        trace.extend(model_report.render_trace_lines("checkpoint-object-model"));
        trace.extend(core_report.render_trace_lines("checkpoint-object-core"));
        panic!(
            "checkpoint object invariant report mismatch:\n{}",
            render_trace(scenario, trace)
        );
    }

    trace.extend(model_report.render_trace_lines("checkpoint-object-model"));
    trace.extend(core_report.render_trace_lines("checkpoint-object-core"));

    for invariant in expected_invariants {
        let check = core_report
            .check(invariant)
            .unwrap_or_else(|| panic!("missing checkpoint object invariant `{invariant}`"));
        if !check.passed {
            panic!(
                "expected invariant `{invariant}` to pass:\n{}",
                render_trace(scenario, trace)
            );
        }
    }
}

fn filter_checkpoint_object_invariants(names: &[String]) -> Vec<String> {
    names
        .iter()
        .filter(|name| CHECKPOINT_OBJECT_IMMUTABLE_INVARIANTS.contains(&name.as_str()))
        .cloned()
        .collect()
}
