use loon_core::checkpoint::{
    load_checkpoint, replay_from_checkpoint_and_wal_tail, StoredCheckpointManifest,
    StoredCheckpointSegment,
};
use loon_core::wal::{replay_wal_commit, replay_wal_tail, StoredWalObject};
use loon_model::{
    ModelCheckpoint, ModelCheckpointFamily, ModelCheckpointSegment, ModelCheckpointTable,
    ModelMetadataState, ModelNamespace, ModelWalCommit,
};
use loon_objectstore::keys::{snapshot_manifest, snapshot_table, wal_commit, SnapshotTableFamily};
use loon_testkit::render::render_trace;
use loon_testkit::scenario::Scenario;
use loon_types::{
    encode_checkpoint_manifest_json, encode_checkpoint_segment_envelope_zstd,
    encode_wal_commit_envelope_zstd, ChangeSeq, CheckpointManifestEnvelope,
    CheckpointManifestPayload, CheckpointSegmentDescriptor, CheckpointSegmentEnvelope,
    CheckpointSegmentPayload, CheckpointTableFamily, FenceToken, HeadState, InodeId, NamespaceId,
    WalCommitEnvelope, WalCommitPayload,
};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;

const TEST_WRITER_VERSION: &str = "loon-testkit-differential";

#[test]
fn wal_tail_replay_fixture_matches_model_and_core() {
    let scenario = load_fixture("native/wal_tail_replay_advances_head.yaml");
    let initial: WalReplayInitial = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<ReplayActionEnvelope> = scenario.decode_actions().expect("decode actions");
    let expect: ReplayExpect = scenario.decode_expect().expect("decode expectations");

    assert_single_action(&actions, ReplayActionKind::ReplayWalTail);

    let mut model_namespace = model_namespace_from_head(&initial.replay_basis_head);
    let mut core_head = initial.replay_basis_head.clone();
    let stored_wal_objects = stored_wal_objects_from_fixture(&initial.wal_objects);

    let mut observed_invariants = Vec::new();
    let mut trace = vec![format!(
        "initial model={:?} core={:?}",
        snapshot_from_model_namespace(&model_namespace),
        snapshot_from_head(&core_head)
    )];

    assert_states_match(
        &scenario,
        &trace,
        0,
        &snapshot_from_model_namespace(&model_namespace),
        &snapshot_from_head(&core_head),
    );

    for (index, (fixture_wal, stored_wal)) in initial
        .wal_objects
        .iter()
        .zip(&stored_wal_objects)
        .enumerate()
    {
        let model_outcome =
            apply_model_wal(&mut model_namespace, &model_wal_from_fixture(fixture_wal));
        let core_outcome = apply_core_wal(&mut core_head, stored_wal, &mut observed_invariants);

        trace.push(format!(
            "step={} wal_key={} model_outcome={:?} core_outcome={:?} model_state={:?} core_state={:?}",
            index + 1,
            fixture_wal.key,
            model_outcome,
            core_outcome,
            snapshot_from_model_namespace(&model_namespace),
            snapshot_from_head(&core_head),
        ));

        if model_outcome != core_outcome {
            panic!(
                "WAL differential outcome mismatch at step {}:\n{}",
                index + 1,
                render_trace(&scenario, &trace)
            );
        }

        assert_states_match(
            &scenario,
            &trace,
            index + 1,
            &snapshot_from_model_namespace(&model_namespace),
            &snapshot_from_head(&core_head),
        );
    }

    let tail_outcome = replay_wal_tail(&initial.replay_basis_head, &stored_wal_objects)
        .map(|head| snapshot_from_head(&head))
        .map_err(|err| format!("{err:?}"));
    trace.push(format!("high_level_core_wal_tail={tail_outcome:?}"));

    let expected_head = snapshot_from_head(&expect.final_head);
    let actual_model = snapshot_from_model_namespace(&model_namespace);
    let actual_core = snapshot_from_head(&core_head);

    if tail_outcome != Ok(actual_core.clone()) {
        panic!(
            "high-level WAL replay diverged from stepwise replay:\n{}",
            render_trace(&scenario, &trace)
        );
    }

    if actual_model != expected_head || actual_core != expected_head {
        panic!(
            "WAL replay final expectation mismatch:\n{}",
            render_trace(&scenario, &trace)
        );
    }

    assert_expected_invariants(&scenario, &trace, &expect.invariants, &observed_invariants);
}

#[test]
fn checkpoint_plus_wal_tail_fixture_matches_model_and_core() {
    let scenario = load_fixture("native/checkpoint_manifest_plus_wal_tail_reproduces_head.yaml");
    let initial: CheckpointReplayInitial = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<ReplayActionEnvelope> = scenario.decode_actions().expect("decode actions");
    let expect: ReplayExpect = scenario.decode_expect().expect("decode expectations");

    assert_single_action(&actions, ReplayActionKind::ReplayFromCheckpointAndWalTail);

    let materialized =
        materialize_checkpoint_fixture(&initial.checkpoint_manifest, &initial.checkpoint_segments);
    let mut observed_invariants = Vec::new();
    let mut trace = vec![format!(
        "initial checkpoint_manifest={} segment_count={} wal_count={}",
        initial.checkpoint_manifest.key,
        initial.checkpoint_segments.len(),
        initial.wal_objects.len()
    )];

    let mut model_namespace = ModelNamespace::restore_from_checkpoint(
        &materialized.model_checkpoint,
        &materialized.available_segment_keys,
    )
    .map_err(|err| format!("{err:?}"));
    let loaded_checkpoint = load_checkpoint(
        &materialized.model_checkpoint.namespace_id,
        &materialized.stored_manifest,
        &materialized.stored_segments,
    )
    .map_err(|err| format!("{err:?}"));

    if let Ok(loaded) = &loaded_checkpoint {
        extend_invariants(&mut observed_invariants, &loaded.checked_invariants);
    }

    let basis_model = model_namespace.as_ref().map(snapshot_from_model_namespace);
    let basis_core = loaded_checkpoint
        .as_ref()
        .map(|loaded| snapshot_from_head(&loaded.basis_head));

    trace.push(format!(
        "checkpoint_basis model={basis_model:?} core={basis_core:?}"
    ));

    if basis_model != basis_core {
        panic!(
            "checkpoint basis mismatch before WAL replay:\n{}",
            render_trace(&scenario, &trace)
        );
    }

    let mut core_head = loaded_checkpoint
        .as_ref()
        .map(|loaded| loaded.basis_head.clone())
        .expect("checkpoint fixture should load successfully");

    let stored_wal_objects = stored_wal_objects_from_fixture(&initial.wal_objects);

    for (index, (fixture_wal, stored_wal)) in initial
        .wal_objects
        .iter()
        .zip(&stored_wal_objects)
        .enumerate()
    {
        let model_outcome = apply_model_wal(
            model_namespace
                .as_mut()
                .expect("model checkpoint restore should succeed"),
            &model_wal_from_fixture(fixture_wal),
        );
        let core_outcome = apply_core_wal(&mut core_head, stored_wal, &mut observed_invariants);

        trace.push(format!(
            "step={} wal_key={} model_outcome={:?} core_outcome={:?} model_state={:?} core_state={:?}",
            index + 1,
            fixture_wal.key,
            model_outcome,
            core_outcome,
            model_namespace.as_ref().map(snapshot_from_model_namespace),
            snapshot_from_head(&core_head),
        ));

        if model_outcome != core_outcome {
            panic!(
                "checkpoint differential outcome mismatch at WAL step {}:\n{}",
                index + 1,
                render_trace(&scenario, &trace)
            );
        }

        assert_states_match(
            &scenario,
            &trace,
            index + 1,
            &model_namespace
                .as_ref()
                .map(snapshot_from_model_namespace)
                .expect("model checkpoint restore should succeed"),
            &snapshot_from_head(&core_head),
        );
    }

    let high_level_core = replay_from_checkpoint_and_wal_tail(
        &materialized.model_checkpoint.namespace_id,
        &materialized.stored_manifest,
        &materialized.stored_segments,
        &stored_wal_objects,
    )
    .map(|head| snapshot_from_head(&head))
    .map_err(|err| format!("{err:?}"));
    trace.push(format!(
        "high_level_core_checkpoint_tail={high_level_core:?}"
    ));

    let expected_head = snapshot_from_head(&expect.final_head);
    let actual_model = model_namespace
        .as_ref()
        .map(snapshot_from_model_namespace)
        .expect("model checkpoint restore should succeed");
    let actual_core = snapshot_from_head(&core_head);

    if high_level_core != Ok(actual_core.clone()) {
        panic!(
            "high-level checkpoint replay diverged from stepwise replay:\n{}",
            render_trace(&scenario, &trace)
        );
    }

    if actual_model != expected_head || actual_core != expected_head {
        panic!(
            "checkpoint replay final expectation mismatch:\n{}",
            render_trace(&scenario, &trace)
        );
    }

    add_invariant(
        &mut observed_invariants,
        "checkpoint_plus_wal_tail_reproduces_head",
    );
    assert_expected_invariants(&scenario, &trace, &expect.invariants, &observed_invariants);
}

#[derive(Debug, Deserialize)]
struct WalReplayInitial {
    replay_basis_head: HeadState,
    wal_objects: Vec<FixtureWalObject>,
}

#[derive(Debug, Deserialize)]
struct CheckpointReplayInitial {
    checkpoint_manifest: FixtureCheckpointManifest,
    checkpoint_segments: Vec<FixtureCheckpointSegment>,
    wal_objects: Vec<FixtureWalObject>,
}

#[derive(Debug, Deserialize)]
struct ReplayExpect {
    final_head: HeadState,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureWalObject {
    key: String,
    payload: FixtureWalPayload,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureWalPayload {
    namespace_id: NamespaceId,
    seq: ChangeSeq,
    base_head_seq: ChangeSeq,
    commit_id: String,
    writer_fence_token: FenceToken,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureCheckpointManifest {
    key: String,
    payload: CheckpointManifestPayload,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureCheckpointSegment {
    key: String,
    payload: CheckpointSegmentPayload,
}

#[derive(Debug, Deserialize)]
struct ReplayActionEnvelope {
    #[serde(default)]
    replay_wal_tail: Option<bool>,
    #[serde(default)]
    replay_from_checkpoint_and_wal_tail: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayActionKind {
    ReplayWalTail,
    ReplayFromCheckpointAndWalTail,
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

#[derive(Debug)]
struct MaterializedCheckpointFixture {
    stored_manifest: StoredCheckpointManifest,
    stored_segments: Vec<StoredCheckpointSegment>,
    model_checkpoint: ModelCheckpoint,
    available_segment_keys: Vec<String>,
}

fn load_fixture(relative_path: &str) -> Scenario {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/scenarios")
        .join(relative_path);
    Scenario::load(&path).unwrap_or_else(|err| panic!("load fixture {}: {err}", path.display()))
}

fn assert_single_action(actions: &[ReplayActionEnvelope], expected: ReplayActionKind) {
    assert_eq!(
        actions.len(),
        1,
        "replay differential harness expects one action"
    );
    assert_eq!(actions[0].kind(), expected, "unexpected replay action");
}

impl ReplayActionEnvelope {
    fn kind(&self) -> ReplayActionKind {
        let mut matches = Vec::new();
        if self.replay_wal_tail == Some(true) {
            matches.push(ReplayActionKind::ReplayWalTail);
        }
        if self.replay_from_checkpoint_and_wal_tail == Some(true) {
            matches.push(ReplayActionKind::ReplayFromCheckpointAndWalTail);
        }

        assert_eq!(
            matches.len(),
            1,
            "replay action envelope should contain exactly one enabled action variant"
        );
        matches
            .into_iter()
            .next()
            .expect("one replay action variant")
    }
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

fn stored_wal_objects_from_fixture(objects: &[FixtureWalObject]) -> Vec<StoredWalObject> {
    objects
        .iter()
        .enumerate()
        .map(|(index, object)| stored_wal_object_from_fixture(index, object))
        .collect()
}

fn stored_wal_object_from_fixture(index: usize, object: &FixtureWalObject) -> StoredWalObject {
    let expected_key = wal_commit(
        object.payload.namespace_id.as_str(),
        object.payload.seq.0,
        &object.payload.commit_id,
    );
    assert_eq!(
        object.key, expected_key,
        "fixture WAL key should match namespace/seq/commit_id"
    );

    let payload = WalCommitPayload {
        namespace_id: object.payload.namespace_id.clone(),
        seq: object.payload.seq,
        base_head_seq: object.payload.base_head_seq,
        commit_id: object.payload.commit_id.clone(),
        request_id: format!("fixture-request-{index}"),
        writer_id: "loon-testkit".to_owned(),
        writer_fence_token: object.payload.writer_fence_token,
        ops: Vec::new(),
        preconditions: Vec::new(),
    };
    let envelope =
        WalCommitEnvelope::from_payload(TEST_WRITER_VERSION, payload).expect("build WAL envelope");
    let encoded_bytes = encode_wal_commit_envelope_zstd(&envelope).expect("encode WAL envelope");

    StoredWalObject {
        object_key: object.key.clone(),
        encoded_bytes,
    }
}

fn model_wal_from_fixture(object: &FixtureWalObject) -> ModelWalCommit {
    ModelWalCommit {
        namespace_id: object.payload.namespace_id.clone(),
        seq: object.payload.seq,
        base_head_seq: object.payload.base_head_seq,
        commit_id: object.payload.commit_id.clone(),
        writer_fence_token: object.payload.writer_fence_token,
    }
}

fn apply_model_wal(
    namespace: &mut ModelNamespace,
    wal: &ModelWalCommit,
) -> Result<NamespaceSnapshot, String> {
    namespace
        .replay_wal_commit(wal)
        .map(|()| snapshot_from_model_namespace(namespace))
        .map_err(|err| format!("{err:?}"))
}

fn apply_core_wal(
    head: &mut HeadState,
    wal: &StoredWalObject,
    observed_invariants: &mut Vec<String>,
) -> Result<NamespaceSnapshot, String> {
    replay_wal_commit(head, wal)
        .map(|replayed| {
            extend_invariants(observed_invariants, &replayed.checked_invariants);
            *head = replayed.resulting_head;
            snapshot_from_head(head)
        })
        .map_err(|err| format!("{err:?}"))
}

fn materialize_checkpoint_fixture(
    manifest: &FixtureCheckpointManifest,
    segments: &[FixtureCheckpointSegment],
) -> MaterializedCheckpointFixture {
    let mut stored_segments = Vec::with_capacity(segments.len());
    let mut descriptors_by_key = BTreeMap::new();

    for segment in segments {
        let expected_key = snapshot_table(
            segment.payload.namespace_id.as_str(),
            segment.payload.checkpoint_seq.0,
            snapshot_table_family(segment.payload.family),
            segment.payload.segment_index,
        );
        assert_eq!(
            segment.key, expected_key,
            "fixture checkpoint segment key should match payload"
        );

        let envelope =
            CheckpointSegmentEnvelope::from_payload(TEST_WRITER_VERSION, segment.payload.clone())
                .expect("build checkpoint segment envelope");
        let encoded_bytes = encode_checkpoint_segment_envelope_zstd(&envelope)
            .expect("encode checkpoint segment envelope");
        let descriptor = CheckpointSegmentDescriptor {
            object_key: segment.key.clone(),
            segment_index: segment.payload.segment_index,
            row_count: segment.payload.row_count,
            min_key: segment.payload.min_key.clone(),
            max_key: segment.payload.max_key.clone(),
            payload_checksum_sha256: envelope.payload_checksum_sha256.clone(),
            page_checksums_sha256: envelope
                .page_checksums_sha256()
                .expect("compute checkpoint page checksums"),
        };

        stored_segments.push(StoredCheckpointSegment {
            object_key: segment.key.clone(),
            encoded_bytes,
        });
        descriptors_by_key.insert(segment.key.clone(), descriptor);
    }

    let expected_manifest_key = snapshot_manifest(
        manifest.payload.namespace_id.as_str(),
        manifest.payload.checkpoint_seq.0,
    );
    assert_eq!(
        manifest.key, expected_manifest_key,
        "fixture checkpoint manifest key should match payload"
    );

    let normalized_tables = manifest
        .payload
        .tables
        .iter()
        .map(|table| loon_types::CheckpointTableManifest {
            family: table.family,
            segments: table
                .segments
                .iter()
                .map(|segment| {
                    let actual = descriptors_by_key
                        .get(&segment.object_key)
                        .unwrap_or_else(|| {
                            panic!("missing fixture segment {}", segment.object_key)
                        });
                    assert_eq!(
                        segment.segment_index, actual.segment_index,
                        "fixture checkpoint descriptor segment index should match payload"
                    );
                    assert_eq!(
                        segment.row_count, actual.row_count,
                        "fixture checkpoint descriptor row_count should match payload"
                    );
                    assert_eq!(
                        segment.min_key, actual.min_key,
                        "fixture checkpoint descriptor min_key should match payload"
                    );
                    assert_eq!(
                        segment.max_key, actual.max_key,
                        "fixture checkpoint descriptor max_key should match payload"
                    );
                    actual.clone()
                })
                .collect(),
        })
        .collect();
    let normalized_payload = CheckpointManifestPayload {
        namespace_id: manifest.payload.namespace_id.clone(),
        checkpoint_seq: manifest.payload.checkpoint_seq,
        active_fence_token: manifest.payload.active_fence_token,
        next_inode_id: manifest.payload.next_inode_id,
        retention_floor_seq: manifest.payload.retention_floor_seq,
        verified: manifest.payload.verified,
        tables: normalized_tables,
    };
    let manifest_envelope =
        CheckpointManifestEnvelope::from_payload(TEST_WRITER_VERSION, normalized_payload.clone())
            .expect("build checkpoint manifest envelope");
    let manifest_bytes = encode_checkpoint_manifest_json(&manifest_envelope)
        .expect("encode checkpoint manifest envelope");

    MaterializedCheckpointFixture {
        stored_manifest: StoredCheckpointManifest {
            object_key: manifest.key.clone(),
            encoded_bytes: manifest_bytes,
        },
        stored_segments,
        available_segment_keys: segments.iter().map(|segment| segment.key.clone()).collect(),
        model_checkpoint: model_checkpoint_from_payload(&normalized_payload),
    }
}

fn model_checkpoint_from_payload(payload: &CheckpointManifestPayload) -> ModelCheckpoint {
    ModelCheckpoint {
        namespace_id: payload.namespace_id.clone(),
        checkpoint_seq: payload.checkpoint_seq,
        active_fence_token: payload.active_fence_token,
        next_inode_id: payload.next_inode_id,
        retention_floor_seq: payload.retention_floor_seq,
        verified: payload.verified,
        tables: payload
            .tables
            .iter()
            .map(|table| ModelCheckpointTable {
                family: model_checkpoint_family(table.family),
                segments: table
                    .segments
                    .iter()
                    .map(|segment| ModelCheckpointSegment {
                        object_key: segment.object_key.clone(),
                        segment_index: segment.segment_index,
                        row_count: segment.row_count,
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn model_checkpoint_family(family: CheckpointTableFamily) -> ModelCheckpointFamily {
    match family {
        CheckpointTableFamily::Inodes => ModelCheckpointFamily::Inodes,
        CheckpointTableFamily::Direntries => ModelCheckpointFamily::Direntries,
        CheckpointTableFamily::Revisions => ModelCheckpointFamily::Revisions,
        CheckpointTableFamily::Tombstones => ModelCheckpointFamily::Tombstones,
    }
}

fn snapshot_table_family(family: CheckpointTableFamily) -> SnapshotTableFamily {
    match family {
        CheckpointTableFamily::Inodes => SnapshotTableFamily::Inodes,
        CheckpointTableFamily::Direntries => SnapshotTableFamily::Direntries,
        CheckpointTableFamily::Revisions => SnapshotTableFamily::Revisions,
        CheckpointTableFamily::Tombstones => SnapshotTableFamily::Tombstones,
    }
}

fn assert_states_match(
    scenario: &Scenario,
    trace: &[String],
    step: usize,
    model_state: &NamespaceSnapshot,
    core_state: &NamespaceSnapshot,
) {
    if model_state != core_state {
        panic!(
            "replay differential state mismatch at step {}:\n{}",
            step,
            render_trace(scenario, trace)
        );
    }
}

fn extend_invariants(observed_invariants: &mut Vec<String>, invariants: &[String]) {
    for invariant in invariants {
        add_invariant(observed_invariants, invariant);
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
