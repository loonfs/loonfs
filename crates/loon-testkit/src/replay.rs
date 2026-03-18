use crate::fixtures::load_fixture;
use crate::render::render_trace;
use crate::scenario::Scenario;
use crate::seed::Seed;
use anyhow::{bail, Result};
use loon_core::checkpoint::{
    load_checkpoint, replay_from_checkpoint_and_wal_tail_with_metadata, StoredCheckpointManifest,
    StoredCheckpointSegment,
};
use loon_core::metadata::{
    DirentryRecord, InodeRecord, MetadataState, RevisionRecord, SubtreeTombstoneRecord,
};
use loon_core::wal::{
    replay_wal_commit_with_metadata, replay_wal_tail_with_metadata, StoredWalObject,
};
use loon_model::{
    ModelCheckpoint, ModelCheckpointFamily, ModelCheckpointPage, ModelCheckpointRow,
    ModelCheckpointSegment, ModelCheckpointTable, ModelMetadataMutation, ModelMetadataState,
    ModelNamespace, ModelWalCommit,
};
use loon_objectstore::keys::{snapshot_manifest, snapshot_table, wal_commit, SnapshotTableFamily};
use loon_types::{
    encode_checkpoint_manifest_json, encode_checkpoint_segment_envelope_zstd,
    encode_wal_commit_envelope_zstd, ChangeSeq, CheckpointManifestEnvelope,
    CheckpointManifestPayload, CheckpointPage, CheckpointRow, CheckpointSegmentDescriptor,
    CheckpointSegmentEnvelope, CheckpointSegmentPayload, CheckpointTableFamily, FenceToken,
    HeadState, InodeId, NamespaceId, RevisionNo, WalCommitEnvelope, WalCommitPayload, WalOp,
};
use serde::Deserialize;
use std::collections::BTreeMap;

const TEST_WRITER_VERSION: &str = "loon-testkit-differential";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayHarnessKind {
    WalTail,
    CheckpointAndWalTail,
}

impl ReplayHarnessKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ReplayHarnessKind::WalTail => "wal-tail",
            ReplayHarnessKind::CheckpointAndWalTail => "checkpoint-and-wal-tail",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplayRunReport {
    pub scenario_name: String,
    pub effective_seed: Option<Seed>,
    pub harness_kind: ReplayHarnessKind,
    pub observed_invariants: Vec<String>,
    pub rendered_trace: String,
}

pub fn run_replay_fixture(
    relative_path: &str,
    seed_override: Option<Seed>,
) -> Result<ReplayRunReport> {
    let scenario = load_fixture(relative_path);
    run_replay_scenario(&scenario, seed_override)
}

pub fn run_replay_scenario(
    scenario: &Scenario,
    seed_override: Option<Seed>,
) -> Result<ReplayRunReport> {
    let mut scenario = scenario.clone();
    if let Some(seed) = seed_override {
        scenario.seed = Some(seed.0);
    }

    let actions: Vec<ReplayActionEnvelope> = scenario.decode_actions()?;
    let harness_kind = assert_single_action(&actions)?;

    match harness_kind {
        ReplayHarnessKind::WalTail => run_wal_replay_scenario(&scenario),
        ReplayHarnessKind::CheckpointAndWalTail => run_checkpoint_replay_scenario(&scenario),
    }
}

fn run_wal_replay_scenario(scenario: &Scenario) -> Result<ReplayRunReport> {
    let initial: WalReplayInitial = scenario.decode_initial()?;
    let expect: ReplayExpect = scenario.decode_expect()?;

    let mut model_namespace =
        model_namespace_from_head(&initial.replay_basis_head, &initial.replay_basis_metadata);
    let mut core_head = initial.replay_basis_head.clone();
    let mut core_metadata = initial.replay_basis_metadata.clone();
    let stored_wal_objects = stored_wal_objects_from_fixture(&initial.wal_objects)?;

    let mut observed_invariants = Vec::new();
    let mut trace = vec![format!(
        "initial model={:?} core={:?}",
        snapshot_from_model_namespace(&model_namespace),
        snapshot_from_core_state(&core_head, &core_metadata)
    )];

    assert_states_match(
        scenario,
        &trace,
        0,
        &snapshot_from_model_namespace(&model_namespace),
        &snapshot_from_core_state(&core_head, &core_metadata),
    )?;

    for (index, (fixture_wal, stored_wal)) in initial
        .wal_objects
        .iter()
        .zip(&stored_wal_objects)
        .enumerate()
    {
        let model_outcome =
            apply_model_wal(&mut model_namespace, &model_wal_from_fixture(fixture_wal));
        let core_outcome = apply_core_wal(
            &mut core_head,
            &mut core_metadata,
            stored_wal,
            &mut observed_invariants,
        );

        trace.push(format!(
            "step={} wal_key={} model_outcome={:?} core_outcome={:?} model_state={:?} core_state={:?}",
            index + 1,
            fixture_wal.key,
            model_outcome,
            core_outcome,
            snapshot_from_model_namespace(&model_namespace),
            snapshot_from_core_state(&core_head, &core_metadata),
        ));

        if model_outcome != core_outcome {
            return fail_with_trace(
                scenario,
                &trace,
                format!("WAL differential outcome mismatch at step {}", index + 1),
            );
        }

        assert_states_match(
            scenario,
            &trace,
            index + 1,
            &snapshot_from_model_namespace(&model_namespace),
            &snapshot_from_core_state(&core_head, &core_metadata),
        )?;
    }

    let tail_outcome = replay_wal_tail_with_metadata(
        &initial.replay_basis_head,
        &initial.replay_basis_metadata,
        &stored_wal_objects,
    )
    .map(|replayed| {
        snapshot_from_core_state(&replayed.resulting_head, &replayed.resulting_metadata_state)
    })
    .map_err(|err| format!("{err:?}"));
    trace.push(format!("high_level_core_wal_tail={tail_outcome:?}"));

    let expected_state = snapshot_from_expect(&expect);
    let actual_model = snapshot_from_model_namespace(&model_namespace);
    let actual_core = snapshot_from_core_state(&core_head, &core_metadata);

    if tail_outcome != Ok(actual_core.clone()) {
        return fail_with_trace(
            scenario,
            &trace,
            "high-level WAL replay diverged from stepwise replay",
        );
    }

    if actual_model != expected_state || actual_core != expected_state {
        return fail_with_trace(scenario, &trace, "WAL replay final expectation mismatch");
    }

    assert_expected_invariants(scenario, &trace, &expect.invariants, &observed_invariants)?;

    Ok(ReplayRunReport {
        scenario_name: scenario.name.clone(),
        effective_seed: scenario.seed.map(Seed),
        harness_kind: ReplayHarnessKind::WalTail,
        observed_invariants,
        rendered_trace: render_trace(scenario, &trace),
    })
}

fn run_checkpoint_replay_scenario(scenario: &Scenario) -> Result<ReplayRunReport> {
    let initial: CheckpointReplayInitial = scenario.decode_initial()?;
    let expect: ReplayExpect = scenario.decode_expect()?;

    let materialized =
        materialize_checkpoint_fixture(&initial.checkpoint_manifest, &initial.checkpoint_segments)?;
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
    .map_err(|err| anyhow::anyhow!("{err:?}"))?;
    let loaded_checkpoint = load_checkpoint(
        &materialized.model_checkpoint.namespace_id,
        &materialized.stored_manifest,
        &materialized.stored_segments,
    )
    .map_err(|err| anyhow::anyhow!("{err:?}"))?;

    extend_invariants(
        &mut observed_invariants,
        &loaded_checkpoint.checked_invariants,
    );

    let basis_model = snapshot_from_model_namespace(&model_namespace);
    let basis_core = snapshot_from_core_state(
        &loaded_checkpoint.basis_head,
        &loaded_checkpoint.basis_metadata_state,
    );

    trace.push(format!(
        "checkpoint_basis model={basis_model:?} core={basis_core:?}"
    ));

    if basis_model != basis_core {
        return fail_with_trace(
            scenario,
            &trace,
            "checkpoint basis mismatch before WAL replay",
        );
    }

    let mut core_head = loaded_checkpoint.basis_head.clone();
    let mut core_metadata = loaded_checkpoint.basis_metadata_state.clone();

    let stored_wal_objects = stored_wal_objects_from_fixture(&initial.wal_objects)?;

    for (index, (fixture_wal, stored_wal)) in initial
        .wal_objects
        .iter()
        .zip(&stored_wal_objects)
        .enumerate()
    {
        let model_outcome =
            apply_model_wal(&mut model_namespace, &model_wal_from_fixture(fixture_wal));
        let core_outcome = apply_core_wal(
            &mut core_head,
            &mut core_metadata,
            stored_wal,
            &mut observed_invariants,
        );

        trace.push(format!(
            "step={} wal_key={} model_outcome={:?} core_outcome={:?} model_state={:?} core_state={:?}",
            index + 1,
            fixture_wal.key,
            model_outcome,
            core_outcome,
            snapshot_from_model_namespace(&model_namespace),
            snapshot_from_core_state(&core_head, &core_metadata),
        ));

        if model_outcome != core_outcome {
            return fail_with_trace(
                scenario,
                &trace,
                format!(
                    "checkpoint differential outcome mismatch at WAL step {}",
                    index + 1
                ),
            );
        }

        assert_states_match(
            scenario,
            &trace,
            index + 1,
            &snapshot_from_model_namespace(&model_namespace),
            &snapshot_from_core_state(&core_head, &core_metadata),
        )?;
    }

    let high_level_core_result = replay_from_checkpoint_and_wal_tail_with_metadata(
        &materialized.model_checkpoint.namespace_id,
        &materialized.stored_manifest,
        &materialized.stored_segments,
        &stored_wal_objects,
    );
    if let Ok(replayed) = &high_level_core_result {
        extend_invariants(&mut observed_invariants, &replayed.checked_invariants);
    }
    let high_level_core = high_level_core_result
        .map(|replayed| {
            snapshot_from_core_state(&replayed.resulting_head, &replayed.resulting_metadata_state)
        })
        .map_err(|err| format!("{err:?}"));
    trace.push(format!(
        "high_level_core_checkpoint_tail={high_level_core:?}"
    ));

    let expected_state = snapshot_from_expect(&expect);
    let actual_model = snapshot_from_model_namespace(&model_namespace);
    let actual_core = snapshot_from_core_state(&core_head, &core_metadata);

    if high_level_core != Ok(actual_core.clone()) {
        return fail_with_trace(
            scenario,
            &trace,
            "high-level checkpoint replay diverged from stepwise replay",
        );
    }

    if actual_model != expected_state || actual_core != expected_state {
        return fail_with_trace(
            scenario,
            &trace,
            "checkpoint replay final expectation mismatch",
        );
    }

    add_invariant(
        &mut observed_invariants,
        "checkpoint_plus_wal_tail_reproduces_head",
    );
    assert_expected_invariants(scenario, &trace, &expect.invariants, &observed_invariants)?;

    Ok(ReplayRunReport {
        scenario_name: scenario.name.clone(),
        effective_seed: scenario.seed.map(Seed),
        harness_kind: ReplayHarnessKind::CheckpointAndWalTail,
        observed_invariants,
        rendered_trace: render_trace(scenario, &trace),
    })
}

#[derive(Debug, Deserialize)]
struct WalReplayInitial {
    replay_basis_head: HeadState,
    #[serde(default)]
    replay_basis_metadata: MetadataState,
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
    final_metadata_state: MetadataState,
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
    #[serde(default)]
    ops: Vec<FixtureWalOp>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op_kind", rename_all = "snake_case")]
enum FixtureWalOp {
    CreateDir {
        inode_id: InodeId,
        parent_inode: InodeId,
        display_name: String,
    },
    CreateFile {
        inode_id: InodeId,
        parent_inode: InodeId,
        display_name: String,
        content_manifest_digest: String,
    },
    ReplaceFile {
        inode_id: InodeId,
        base_revision: RevisionNo,
        content_manifest_digest: String,
    },
    Rename {
        inode_id: InodeId,
        new_parent_inode: InodeId,
        new_display_name: String,
    },
    DeleteSubtree {
        root_inode: InodeId,
    },
    RestoreRevision {
        inode_id: InodeId,
        base_revision: RevisionNo,
        restore_from_revision: RevisionNo,
    },
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
    metadata_state: MetadataSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataSnapshot {
    inodes: Vec<InodeRecord>,
    direntries: Vec<DirentryRecord>,
    revisions: Vec<RevisionRecord>,
    subtree_tombstones: Vec<SubtreeTombstoneRecord>,
}

#[derive(Debug)]
struct MaterializedCheckpointFixture {
    stored_manifest: StoredCheckpointManifest,
    stored_segments: Vec<StoredCheckpointSegment>,
    model_checkpoint: ModelCheckpoint,
    available_segment_keys: Vec<String>,
}

fn assert_single_action(actions: &[ReplayActionEnvelope]) -> Result<ReplayHarnessKind> {
    if actions.len() != 1 {
        bail!("replay differential harness expects one action");
    }

    match actions[0].kind()? {
        ReplayActionKind::ReplayWalTail => Ok(ReplayHarnessKind::WalTail),
        ReplayActionKind::ReplayFromCheckpointAndWalTail => {
            Ok(ReplayHarnessKind::CheckpointAndWalTail)
        }
    }
}

impl ReplayActionEnvelope {
    fn kind(&self) -> Result<ReplayActionKind> {
        let mut matches = Vec::new();
        if self.replay_wal_tail == Some(true) {
            matches.push(ReplayActionKind::ReplayWalTail);
        }
        if self.replay_from_checkpoint_and_wal_tail == Some(true) {
            matches.push(ReplayActionKind::ReplayFromCheckpointAndWalTail);
        }

        if matches.len() != 1 {
            bail!("replay action envelope should contain exactly one enabled action variant");
        }
        Ok(matches
            .into_iter()
            .next()
            .expect("one replay action variant"))
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
        metadata_state: model_metadata_state_from_core(metadata_state),
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
        metadata_state: metadata_snapshot_from_model(&namespace.metadata_state),
    }
}

fn snapshot_from_core_state(head: &HeadState, metadata_state: &MetadataState) -> NamespaceSnapshot {
    NamespaceSnapshot {
        namespace_id: head.namespace_id.clone(),
        seq: head.seq,
        active_fence_token: head.active_fence_token,
        next_inode_id: head.next_inode_id,
        snapshot_hint_seq: head.snapshot_hint_seq,
        retention_floor_seq: head.retention_floor_seq,
        metadata_state: metadata_snapshot_from_core(metadata_state),
    }
}

fn snapshot_from_expect(expect: &ReplayExpect) -> NamespaceSnapshot {
    snapshot_from_core_state(&expect.final_head, &expect.final_metadata_state)
}

fn stored_wal_objects_from_fixture(objects: &[FixtureWalObject]) -> Result<Vec<StoredWalObject>> {
    objects
        .iter()
        .enumerate()
        .map(|(index, object)| stored_wal_object_from_fixture(index, object))
        .collect()
}

fn stored_wal_object_from_fixture(
    index: usize,
    object: &FixtureWalObject,
) -> Result<StoredWalObject> {
    let expected_key = wal_commit(
        object.payload.namespace_id.as_str(),
        object.payload.seq.0,
        &object.payload.commit_id,
    );
    if object.key != expected_key {
        bail!("fixture WAL key should match namespace/seq/commit_id");
    }

    let payload = WalCommitPayload {
        namespace_id: object.payload.namespace_id.clone(),
        seq: object.payload.seq,
        base_head_seq: object.payload.base_head_seq,
        commit_id: object.payload.commit_id.clone(),
        request_id: format!("fixture-request-{index}"),
        writer_id: "loon-testkit".to_owned(),
        writer_fence_token: object.payload.writer_fence_token,
        ops: object
            .payload
            .ops
            .iter()
            .enumerate()
            .map(|(op_index, op)| {
                shared_wal_op_from_fixture(
                    u32::try_from(op_index).expect("fixture op index should fit in u32"),
                    op,
                )
            })
            .collect(),
        preconditions: Vec::new(),
    };
    let envelope =
        WalCommitEnvelope::from_payload(TEST_WRITER_VERSION, payload).expect("build WAL envelope");
    let encoded_bytes = encode_wal_commit_envelope_zstd(&envelope).expect("encode WAL envelope");

    Ok(StoredWalObject {
        object_key: object.key.clone(),
        encoded_bytes,
    })
}

fn model_wal_from_fixture(object: &FixtureWalObject) -> ModelWalCommit {
    ModelWalCommit {
        namespace_id: object.payload.namespace_id.clone(),
        seq: object.payload.seq,
        base_head_seq: object.payload.base_head_seq,
        commit_id: object.payload.commit_id.clone(),
        writer_fence_token: object.payload.writer_fence_token,
        ops: object
            .payload
            .ops
            .iter()
            .map(model_metadata_mutation_from_fixture)
            .collect(),
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
    metadata_state: &mut MetadataState,
    wal: &StoredWalObject,
    observed_invariants: &mut Vec<String>,
) -> Result<NamespaceSnapshot, String> {
    replay_wal_commit_with_metadata(head, metadata_state, wal)
        .map(|replayed| {
            extend_invariants(observed_invariants, &replayed.checked_invariants);
            *head = replayed.resulting_head;
            *metadata_state = replayed.resulting_metadata_state;
            snapshot_from_core_state(head, metadata_state)
        })
        .map_err(|err| format!("{err:?}"))
}

fn materialize_checkpoint_fixture(
    manifest: &FixtureCheckpointManifest,
    segments: &[FixtureCheckpointSegment],
) -> Result<MaterializedCheckpointFixture> {
    let mut stored_segments = Vec::with_capacity(segments.len());
    let mut descriptors_by_key = BTreeMap::new();

    for segment in segments {
        let expected_key = snapshot_table(
            segment.payload.namespace_id.as_str(),
            segment.payload.checkpoint_seq.0,
            snapshot_table_family(segment.payload.family),
            segment.payload.segment_index,
        );
        if segment.key != expected_key {
            bail!("fixture checkpoint segment key should match payload");
        }

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
    if manifest.key != expected_manifest_key {
        bail!("fixture checkpoint manifest key should match payload");
    }

    let normalized_tables = manifest
        .payload
        .tables
        .iter()
        .map(|table| {
            let segments = table
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
                .collect();
            loon_types::CheckpointTableManifest {
                family: table.family,
                segments,
            }
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

    Ok(MaterializedCheckpointFixture {
        stored_manifest: StoredCheckpointManifest {
            object_key: manifest.key.clone(),
            encoded_bytes: manifest_bytes,
        },
        stored_segments,
        available_segment_keys: segments.iter().map(|segment| segment.key.clone()).collect(),
        model_checkpoint: model_checkpoint_from_fixture(&normalized_payload, segments),
    })
}

fn model_checkpoint_from_fixture(
    payload: &CheckpointManifestPayload,
    segments: &[FixtureCheckpointSegment],
) -> ModelCheckpoint {
    let pages_by_object_key = segments
        .iter()
        .map(|segment| {
            (
                segment.key.clone(),
                segment
                    .payload
                    .pages
                    .iter()
                    .map(model_checkpoint_page_from_shared)
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();

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
                        pages: pages_by_object_key
                            .get(&segment.object_key)
                            .cloned()
                            .unwrap_or_default(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn model_checkpoint_page_from_shared(page: &CheckpointPage) -> ModelCheckpointPage {
    ModelCheckpointPage {
        page_index: page.page_index,
        min_key: page.min_key.clone(),
        max_key: page.max_key.clone(),
        row_keys: page.row_keys.clone(),
        rows: page
            .rows
            .iter()
            .map(model_checkpoint_row_from_shared)
            .collect(),
    }
}

fn model_checkpoint_row_from_shared(row: &CheckpointRow) -> ModelCheckpointRow {
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

fn shared_wal_op_from_fixture(op_index: u32, op: &FixtureWalOp) -> WalOp {
    match op {
        FixtureWalOp::CreateDir {
            inode_id,
            parent_inode,
            display_name,
        } => WalOp::CreateDir {
            op_index,
            inode_id: *inode_id,
            parent_inode: *parent_inode,
            display_name: display_name.clone(),
        },
        FixtureWalOp::CreateFile {
            inode_id,
            parent_inode,
            display_name,
            content_manifest_digest,
        } => WalOp::CreateFile {
            op_index,
            inode_id: *inode_id,
            parent_inode: *parent_inode,
            display_name: display_name.clone(),
            content_manifest_digest: content_manifest_digest.clone(),
        },
        FixtureWalOp::ReplaceFile {
            inode_id,
            base_revision,
            content_manifest_digest,
        } => WalOp::ReplaceFile {
            op_index,
            inode_id: *inode_id,
            base_revision: *base_revision,
            content_manifest_digest: content_manifest_digest.clone(),
        },
        FixtureWalOp::Rename {
            inode_id,
            new_parent_inode,
            new_display_name,
        } => WalOp::Rename {
            op_index,
            inode_id: *inode_id,
            new_parent_inode: *new_parent_inode,
            new_display_name: new_display_name.clone(),
        },
        FixtureWalOp::DeleteSubtree { root_inode } => WalOp::DeleteSubtree {
            op_index,
            root_inode: *root_inode,
        },
        FixtureWalOp::RestoreRevision {
            inode_id,
            base_revision,
            restore_from_revision,
        } => WalOp::RestoreRevision {
            op_index,
            inode_id: *inode_id,
            base_revision: *base_revision,
            restore_from_revision: *restore_from_revision,
        },
    }
}

fn model_metadata_mutation_from_fixture(op: &FixtureWalOp) -> ModelMetadataMutation {
    match op {
        FixtureWalOp::CreateDir {
            inode_id,
            parent_inode,
            display_name,
        } => ModelMetadataMutation::CreateDir {
            inode_id: *inode_id,
            parent_inode_id: *parent_inode,
            display_name: display_name.clone(),
        },
        FixtureWalOp::CreateFile {
            inode_id,
            parent_inode,
            display_name,
            content_manifest_digest,
        } => ModelMetadataMutation::CreateFile {
            inode_id: *inode_id,
            parent_inode_id: *parent_inode,
            display_name: display_name.clone(),
            content_manifest_digest: content_manifest_digest.clone(),
        },
        FixtureWalOp::ReplaceFile {
            inode_id,
            base_revision,
            content_manifest_digest,
        } => ModelMetadataMutation::ReplaceFile {
            inode_id: *inode_id,
            base_revision_no: *base_revision,
            content_manifest_digest: content_manifest_digest.clone(),
        },
        FixtureWalOp::DeleteSubtree { root_inode } => ModelMetadataMutation::DeleteSubtree {
            root_inode_id: *root_inode,
        },
        FixtureWalOp::Rename {
            inode_id,
            new_parent_inode,
            new_display_name,
        } => ModelMetadataMutation::Rename {
            inode_id: *inode_id,
            new_parent_inode_id: *new_parent_inode,
            new_display_name: new_display_name.clone(),
        },
        FixtureWalOp::RestoreRevision {
            inode_id,
            base_revision,
            restore_from_revision,
        } => ModelMetadataMutation::RestoreRevision {
            inode_id: *inode_id,
            base_revision_no: *base_revision,
            restore_from_revision_no: *restore_from_revision,
        },
    }
}

fn model_metadata_state_from_core(metadata_state: &MetadataState) -> ModelMetadataState {
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

fn metadata_snapshot_from_model(metadata_state: &ModelMetadataState) -> MetadataSnapshot {
    MetadataSnapshot {
        inodes: metadata_state
            .inodes
            .iter()
            .map(|inode| InodeRecord {
                inode_id: inode.inode_id,
                inode_kind: inode.inode_kind.clone(),
                created_seq: inode.created_seq,
            })
            .collect(),
        direntries: metadata_state
            .direntries
            .iter()
            .map(|direntry| DirentryRecord {
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
            .map(|revision| RevisionRecord {
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
            .map(|tombstone| SubtreeTombstoneRecord {
                root_inode_id: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
                tombstone_op_index: tombstone.tombstone_op_index,
            })
            .collect(),
    }
}

fn metadata_snapshot_from_core(metadata_state: &MetadataState) -> MetadataSnapshot {
    MetadataSnapshot {
        inodes: metadata_state.inodes.clone(),
        direntries: metadata_state.direntries.clone(),
        revisions: metadata_state.revisions.clone(),
        subtree_tombstones: metadata_state.subtree_tombstones.clone(),
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
) -> Result<()> {
    if model_state != core_state {
        return fail_with_trace(
            scenario,
            trace,
            format!("replay differential state mismatch at step {step}"),
        );
    }
    Ok(())
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
) -> Result<()> {
    for invariant in expected {
        if !observed.iter().any(|value| value == invariant) {
            return fail_with_trace(
                scenario,
                trace,
                format!("missing expected invariant `{invariant}`"),
            );
        }
    }
    Ok(())
}

fn fail_with_trace<T>(
    scenario: &Scenario,
    trace: &[String],
    message: impl AsRef<str>,
) -> Result<T> {
    bail!("{}:\n{}", message.as_ref(), render_trace(scenario, trace));
}
