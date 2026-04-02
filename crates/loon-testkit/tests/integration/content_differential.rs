use loon_client::upload::{upload_small_file_from_path, UploadedContent};
use loon_server::core::checkpoint::prepare_checkpoint;
use loon_server::core::content::{
    validate_durable_content_reference, DurableContentValidationError,
};
use loon_server::core::invariants::CONTENT_OBJECT_FILE_INVARIANTS;
use loon_server::core::metadata::MetadataState;
use loon_server::mutation::{
    execute_client_mutation, ClientMutationExecutionError, ClientMutationExecutionParams,
};
use loon_server::objectstore::fs::LocalFsStore;
use loon_server::objectstore::keys::{blob, content_manifest, namespace_head, namespace_lease};
use loon_server::objectstore::ObjectStore;
use loon_testkit::fixtures::load_fixture as load_scenario_fixture;
use loon_testkit::invariants::{
    evaluate_content_object_invariants, ContentObjectInvariantInputs, ContentObjectInvariantReport,
    ContentObjectInvariantSnapshot, StoredContentBlockSnapshot,
};
use loon_testkit::model::{
    build_uploaded_content, validate_uploaded_content_reference, ModelContentValidationError,
    ModelUploadedContent,
};
use loon_testkit::render::render_trace;
use loon_testkit::scenario::Scenario;
use loon_testkit::tempdir::TestDir;
use loon_types::{
    decode_content_manifest_json, encode_content_manifest_json, sha256_digest,
    ClientMutationRequest, ContentManifestEnvelope, ContentManifestPayload, ControlObjectKind,
    HeadState, HeadStateEnvelope, LeaseState, LeaseStateEnvelope, NamespaceId,
    CONTENT_BLOCK_SIZE_BYTES,
};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

const TEST_WRITER_VERSION: &str = "loon-testkit-differential";
const NOW_MS: u64 = 1_500;

#[test]
fn small_file_upload_fixture_matches_model_and_core_content() {
    let report =
        run_client_upload_fixture_report("client/small_file_upload_writes_block_and_manifest.yaml");
    assert!(!report.rendered_trace.is_empty());
}

#[test]
fn small_file_upload_snapshot_matches_checked_in_artifact() {
    let report =
        run_client_upload_fixture_report("client/small_file_upload_writes_block_and_manifest.yaml");
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../../tests/snapshots/content-differential/client/small_file_upload_writes_block_and_manifest.txt"
        )
    );
}

#[test]
fn native_create_file_fixture_matches_model_and_core_content() {
    let report = run_native_success_fixture_report(
        "native/client_create_file_commit_writes_wal_and_head.yaml",
    );
    assert!(!report.rendered_trace.is_empty());
}

#[test]
fn native_replace_file_fixture_matches_model_and_core_content() {
    let report = run_native_success_fixture_report(
        "native/client_replace_file_commit_writes_wal_and_head.yaml",
    );
    assert!(!report.rendered_trace.is_empty());
}

#[test]
fn native_create_file_missing_content_block_fails_closed() {
    let report = run_native_missing_block_fixture_report(
        "native/client_create_file_rejects_missing_content_block.yaml",
    );
    assert!(!report.rendered_trace.is_empty());
}

#[test]
fn native_create_file_snapshot_matches_checked_in_artifact() {
    let report = run_native_success_fixture_report(
        "native/client_create_file_commit_writes_wal_and_head.yaml",
    );
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../../tests/snapshots/content-differential/native/client_create_file_commit_writes_wal_and_head.txt"
        )
    );
}

struct ContentFixtureRunReport {
    rendered_trace: String,
}

fn run_client_upload_fixture_report(relative_path: &str) -> ContentFixtureRunReport {
    let scenario = load_fixture(relative_path);
    let initial: UploadInitial = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<UploadActionEnvelope> = scenario.decode_actions().expect("decode actions");
    let expect: UploadExpect = scenario.decode_expect().expect("decode expectations");

    assert_eq!(actions.len(), 1, "client upload fixture expects one action");
    assert!(actions[0].upload_small_file);

    let temp_dir = TestDir::new("content-differential-client-upload");
    let source_path = temp_dir.path().join(&initial.local_file.relative_path);
    let store_root = temp_dir.path().join("objectstore");
    let source_bytes = initial.local_file.content_utf8.as_bytes();

    fs::create_dir_all(source_path.parent().expect("source parent")).expect("create source parent");
    fs::write(&source_path, source_bytes).expect("write source file");
    fs::create_dir_all(&store_root).expect("create store root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    let uploaded = upload_small_file_from_path(&store, &initial.namespace_id, &source_path)
        .expect("upload small file");
    let model = build_uploaded_content(initial.namespace_id.clone(), source_bytes)
        .expect("build model upload");

    let model_snapshot = model_upload_snapshot(&initial.namespace_id, &model, source_bytes);
    let core_snapshot = load_store_content_snapshot(
        &store,
        &initial.namespace_id,
        &uploaded.content_manifest_digest,
    );

    let model_report = evaluate_content_object_invariants(ContentObjectInvariantInputs {
        expected_namespace: &initial.namespace_id,
        content: &model_snapshot,
    });
    let core_report = evaluate_content_object_invariants(ContentObjectInvariantInputs {
        expected_namespace: &initial.namespace_id,
        content: &core_snapshot,
    });

    let mut trace = vec![
        format!(
            "initial namespace_id={} source_path={} source_len={}",
            initial.namespace_id,
            initial.local_file.relative_path.display(),
            source_bytes.len()
        ),
        format!(
            "uploaded model=(file_size={}, file_digest={}, manifest_digest={}) core=(file_size={}, file_digest={}, manifest_digest={})",
            model.file_size_bytes,
            model.file_digest_sha256,
            model.content_manifest_digest,
            uploaded.file_size_bytes,
            uploaded.file_digest_sha256,
            uploaded.content_manifest_digest
        ),
    ];

    assert_eq!(uploaded.file_size_bytes, expect.file_size_bytes);
    assert_eq!(uploaded.file_digest_sha256, expect.file_digest_sha256);
    assert_eq!(
        uploaded.content_manifest_digest,
        expect.content_manifest_digest
    );
    assert_eq!(uploaded.manifest_object_key, expect.manifest_object_key);
    assert_eq!(uploaded.manifest_envelope.payload, expect.manifest_payload);
    assert_eq!(uploaded.file_size_bytes, model.file_size_bytes);
    assert_eq!(uploaded.file_digest_sha256, model.file_digest_sha256);
    assert_eq!(
        uploaded.content_manifest_digest,
        model.content_manifest_digest
    );
    assert_eq!(uploaded.manifest_envelope, model.manifest_envelope);
    assert_block_objects_match(&uploaded, &expect.block_objects);

    let stored_manifest = store
        .get(&expect.manifest_object_key, None)
        .expect("read stored manifest")
        .expect("stored manifest should exist");
    let decoded_manifest =
        decode_content_manifest_json(&stored_manifest).expect("decode stored manifest");
    assert_eq!(decoded_manifest, uploaded.manifest_envelope);

    assert_reports_match_and_pass(
        &scenario,
        &mut trace,
        &model_report,
        &core_report,
        &expect.invariants,
    );

    ContentFixtureRunReport {
        rendered_trace: render_trace(&scenario, &trace),
    }
}

fn run_native_success_fixture_report(relative_path: &str) -> ContentFixtureRunReport {
    let scenario = load_fixture(relative_path);
    let initial: MutationInitial = scenario.decode_initial().expect("decode initial state");
    let expect: NativeMutationExpect = scenario.decode_expect().expect("decode expectations");
    let content_objects = initial
        .content_objects
        .as_ref()
        .expect("success content fixture should seed content objects");
    let content_manifest_digest = initial
        .client_request
        .content_manifest_digest()
        .expect("content fixture should carry a manifest digest")
        .to_owned();

    let temp_dir = TestDir::new("content-differential-native-success");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    seed_verified_basis(&store, &initial.head, &initial.metadata_state);
    seed_content_objects(&store, &initial.head.namespace_id, content_objects);

    let manifest_envelope =
        ContentManifestEnvelope::from_payload(content_objects.manifest.payload.clone())
            .expect("build content manifest envelope");
    let model_blocks = content_blocks_by_digest(content_objects);
    let model_validated = validate_uploaded_content_reference(
        &initial.head.namespace_id,
        &content_manifest_digest,
        &manifest_envelope,
        &model_blocks,
    )
    .expect("model validate content");
    let core_validated = validate_durable_content_reference(
        &store,
        &initial.head.namespace_id,
        &content_manifest_digest,
    )
    .expect("core validate content");
    let executed = execute_client_mutation(
        &store,
        &ClientMutationRequest::from(initial.client_request.clone()),
        &ClientMutationExecutionParams {
            writer_id: initial.lease.holder_id.clone(),
            writer_version: TEST_WRITER_VERSION.to_owned(),
            now_ms: NOW_MS,
            lease_duration_ms: 60_000,
        },
    )
    .expect("execute client mutation");

    let model_snapshot = fixture_content_snapshot(
        &initial.head.namespace_id,
        &content_manifest_digest,
        &content_objects.manifest.payload,
        &content_objects.blocks,
    );
    let core_snapshot =
        load_store_content_snapshot(&store, &initial.head.namespace_id, &content_manifest_digest);
    let model_report = evaluate_content_object_invariants(ContentObjectInvariantInputs {
        expected_namespace: &initial.head.namespace_id,
        content: &model_snapshot,
    });
    let core_report = evaluate_content_object_invariants(ContentObjectInvariantInputs {
        expected_namespace: &initial.head.namespace_id,
        content: &core_snapshot,
    });

    let expected_content_invariants = filter_content_invariants(&expect.invariants);
    let mut trace = vec![
        format!(
            "initial namespace_id={} head_seq={} request_id={} manifest_digest={}",
            initial.head.namespace_id,
            initial.head.seq.0,
            initial.client_request.client_request_id,
            content_manifest_digest
        ),
        format!(
            "validated model=(file_size={}, file_digest={}, block_count={}) core=(file_size={}, file_digest={}, block_count={})",
            model_validated.file_size_bytes,
            model_validated.file_digest_sha256,
            model_validated.block_count,
            core_validated.file_size_bytes,
            core_validated.file_digest_sha256,
            core_validated.manifest_envelope.payload.blocks.len()
        ),
        format!(
            "mutation resulting_head={:?} checked_invariants={:?}",
            executed.head_publish.resulting_head,
            executed.checked_invariants
        ),
    ];

    assert_eq!(
        model_validated.file_size_bytes,
        core_validated.file_size_bytes
    );
    assert_eq!(
        model_validated.file_digest_sha256,
        core_validated.file_digest_sha256
    );
    assert_eq!(
        model_validated.block_count,
        core_validated.manifest_envelope.payload.blocks.len()
    );
    assert_eq!(executed.head_publish.resulting_head, expect.head);

    assert_reports_match_and_pass(
        &scenario,
        &mut trace,
        &model_report,
        &core_report,
        &expected_content_invariants,
    );

    ContentFixtureRunReport {
        rendered_trace: render_trace(&scenario, &trace),
    }
}

fn run_native_missing_block_fixture_report(relative_path: &str) -> ContentFixtureRunReport {
    let scenario = load_fixture(relative_path);
    let initial: MutationInitial = scenario.decode_initial().expect("decode initial state");
    let expect: NativeMutationErrorExpect = scenario.decode_expect().expect("decode expectations");
    let content_objects = initial
        .content_objects
        .as_ref()
        .expect("missing-block fixture should seed content objects");
    let content_manifest_digest = initial
        .client_request
        .content_manifest_digest()
        .expect("missing-block fixture should carry a manifest digest")
        .to_owned();

    let temp_dir = TestDir::new("content-differential-native-missing-block");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    seed_verified_basis(&store, &initial.head, &initial.metadata_state);
    seed_content_objects(&store, &initial.head.namespace_id, content_objects);

    let manifest_envelope =
        ContentManifestEnvelope::from_payload(content_objects.manifest.payload.clone())
            .expect("build content manifest envelope");
    let model_blocks = content_blocks_by_digest(content_objects);
    let model_error = validate_uploaded_content_reference(
        &initial.head.namespace_id,
        &content_manifest_digest,
        &manifest_envelope,
        &model_blocks,
    )
    .expect_err("model validation should fail for missing block");
    let core_error = validate_durable_content_reference(
        &store,
        &initial.head.namespace_id,
        &content_manifest_digest,
    )
    .expect_err("core validation should fail for missing block");
    let mutation_error = execute_client_mutation(
        &store,
        &ClientMutationRequest::from(initial.client_request.clone()),
        &ClientMutationExecutionParams {
            writer_id: initial.lease.holder_id.clone(),
            writer_version: TEST_WRITER_VERSION.to_owned(),
            now_ms: NOW_MS,
            lease_duration_ms: 60_000,
        },
    )
    .expect_err("execute_client_mutation should fail for missing block");

    let model_snapshot = fixture_content_snapshot(
        &initial.head.namespace_id,
        &content_manifest_digest,
        &content_objects.manifest.payload,
        &content_objects.blocks,
    );
    let core_snapshot =
        load_store_content_snapshot(&store, &initial.head.namespace_id, &content_manifest_digest);
    let model_report = evaluate_content_object_invariants(ContentObjectInvariantInputs {
        expected_namespace: &initial.head.namespace_id,
        content: &model_snapshot,
    });
    let core_report = evaluate_content_object_invariants(ContentObjectInvariantInputs {
        expected_namespace: &initial.head.namespace_id,
        content: &core_snapshot,
    });

    let mut trace = vec![
        format!(
            "initial namespace_id={} head_seq={} request_id={} manifest_digest={}",
            initial.head.namespace_id,
            initial.head.seq.0,
            initial.client_request.client_request_id,
            content_manifest_digest
        ),
        format!(
            "validation_errors model={:?} core={:?} mutation={:?}",
            model_error, core_error, mutation_error
        ),
    ];

    match expect.error.as_str() {
        "create_file_block_missing" => {
            assert!(matches!(
                model_error,
                ModelContentValidationError::MissingBlock { .. }
            ));
            assert!(matches!(
                core_error,
                DurableContentValidationError::MissingBlockObject { .. }
            ));
            assert!(matches!(
                mutation_error,
                ClientMutationExecutionError::DurableContent(
                    DurableContentValidationError::MissingBlockObject { .. }
                )
            ));
        }
        other => panic!("unexpected missing-block fixture error tag `{other}`"),
    }

    assert_reports_match(&scenario, &mut trace, &model_report, &core_report);
    let blocks_check = core_report
        .check("content_manifest_blocks_match_descriptors")
        .expect("blocks invariant should be present");
    assert!(
        !blocks_check.passed,
        "missing-block fixture should fail block-descriptor invariant:\n{}",
        render_trace(&scenario, &trace)
    );
    trace.extend(model_report.render_trace_lines("content-model"));
    trace.extend(core_report.render_trace_lines("content-core"));

    ContentFixtureRunReport {
        rendered_trace: render_trace(&scenario, &trace),
    }
}

fn assert_reports_match_and_pass(
    scenario: &Scenario,
    trace: &mut Vec<String>,
    model_report: &ContentObjectInvariantReport,
    core_report: &ContentObjectInvariantReport,
    expected_invariants: &[String],
) {
    assert_reports_match(scenario, trace, model_report, core_report);
    trace.extend(model_report.render_trace_lines("content-model"));
    trace.extend(core_report.render_trace_lines("content-core"));

    for invariant in expected_invariants {
        let check = core_report
            .check(invariant)
            .unwrap_or_else(|| panic!("missing invariant `{invariant}`"));
        if !check.passed {
            panic!(
                "expected invariant `{invariant}` to pass:\n{}",
                render_trace(scenario, trace)
            );
        }
    }
}

fn assert_reports_match(
    scenario: &Scenario,
    trace: &mut Vec<String>,
    model_report: &ContentObjectInvariantReport,
    core_report: &ContentObjectInvariantReport,
) {
    if model_report != core_report {
        trace.extend(model_report.render_trace_lines("content-model"));
        trace.extend(core_report.render_trace_lines("content-core"));
        panic!(
            "content invariant report mismatch:\n{}",
            render_trace(scenario, trace)
        );
    }
}

fn assert_block_objects_match(uploaded: &UploadedContent, expected: &[ExpectedBlockObject]) {
    let actual = uploaded
        .block_objects
        .iter()
        .map(|block| {
            (
                block.object_key.clone(),
                block.content_digest_sha256.clone(),
                block.plaintext_size_bytes,
            )
        })
        .collect::<Vec<_>>();
    let expected = expected
        .iter()
        .map(|block| {
            (
                block.object_key.clone(),
                block.content_digest_sha256.clone(),
                block.plaintext_size_bytes,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

fn filter_content_invariants(names: &[String]) -> Vec<String> {
    names
        .iter()
        .filter(|name| CONTENT_OBJECT_FILE_INVARIANTS.contains(&name.as_str()))
        .cloned()
        .collect()
}

fn model_upload_snapshot(
    namespace_id: &NamespaceId,
    uploaded: &ModelUploadedContent,
    content_bytes: &[u8],
) -> ContentObjectInvariantSnapshot {
    let manifest_bytes =
        encode_content_manifest_json(&uploaded.manifest_envelope).expect("encode manifest");
    ContentObjectInvariantSnapshot {
        content_manifest_digest: uploaded.content_manifest_digest.clone(),
        manifest_object_key: content_manifest(
            namespace_id.as_str(),
            &uploaded.content_manifest_digest,
        ),
        manifest_envelope: uploaded.manifest_envelope.clone(),
        manifest_bytes,
        available_blocks: chunked_content_blocks(namespace_id, content_bytes),
    }
}

fn fixture_content_snapshot(
    namespace_id: &NamespaceId,
    content_manifest_digest: &str,
    manifest_payload: &ContentManifestPayload,
    blocks: &[SeededBlockObject],
) -> ContentObjectInvariantSnapshot {
    let manifest_envelope = ContentManifestEnvelope::from_payload(manifest_payload.clone())
        .expect("build content manifest envelope");
    let manifest_bytes = encode_content_manifest_json(&manifest_envelope).expect("encode manifest");

    ContentObjectInvariantSnapshot {
        content_manifest_digest: content_manifest_digest.to_owned(),
        manifest_object_key: content_manifest(namespace_id.as_str(), content_manifest_digest),
        manifest_envelope,
        manifest_bytes,
        available_blocks: blocks
            .iter()
            .map(|block| {
                (
                    block.digest.clone(),
                    StoredContentBlockSnapshot {
                        object_key: blob(namespace_id.as_str(), &block.digest),
                        bytes: block.body_utf8.as_bytes().to_vec(),
                    },
                )
            })
            .collect(),
    }
}

fn load_store_content_snapshot(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    content_manifest_digest: &str,
) -> ContentObjectInvariantSnapshot {
    let manifest_object_key = content_manifest(namespace_id.as_str(), content_manifest_digest);
    let manifest_bytes = store
        .get(&manifest_object_key, None)
        .expect("read stored manifest")
        .expect("stored manifest should exist");
    let manifest_envelope =
        decode_content_manifest_json(&manifest_bytes).expect("decode stored manifest");

    let available_blocks = manifest_envelope
        .payload
        .blocks
        .iter()
        .filter_map(|descriptor| {
            let object_key = blob(namespace_id.as_str(), &descriptor.content_digest_sha256);
            let bytes = store
                .get(&object_key, None)
                .expect("read block from store")?;
            Some((
                descriptor.content_digest_sha256.clone(),
                StoredContentBlockSnapshot { object_key, bytes },
            ))
        })
        .collect();

    ContentObjectInvariantSnapshot {
        content_manifest_digest: content_manifest_digest.to_owned(),
        manifest_object_key,
        manifest_envelope,
        manifest_bytes,
        available_blocks,
    }
}

fn chunked_content_blocks(
    namespace_id: &NamespaceId,
    content_bytes: &[u8],
) -> BTreeMap<String, StoredContentBlockSnapshot> {
    content_bytes
        .chunks(CONTENT_BLOCK_SIZE_BYTES as usize)
        .map(|block_bytes| {
            let digest = sha256_digest(block_bytes);
            (
                digest.clone(),
                StoredContentBlockSnapshot {
                    object_key: blob(namespace_id.as_str(), &digest),
                    bytes: block_bytes.to_vec(),
                },
            )
        })
        .collect()
}

fn content_blocks_by_digest(content_objects: &SeededContentObjects) -> BTreeMap<String, Vec<u8>> {
    content_objects
        .blocks
        .iter()
        .map(|block| (block.digest.clone(), block.body_utf8.as_bytes().to_vec()))
        .collect()
}

fn seed_head_and_lease(store: &LocalFsStore, head: &HeadState, lease: &LeaseState) {
    let head_envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        TEST_WRITER_VERSION,
        head.clone(),
    )
    .expect("encode head envelope");
    let head_bytes = serde_json::to_vec(&head_envelope).expect("serialize head envelope");
    store
        .put_if_absent(&namespace_head(head.namespace_id.as_str()), &head_bytes)
        .expect("seed head object");

    let lease_envelope = LeaseStateEnvelope::from_state(
        ControlObjectKind::NamespaceLease,
        TEST_WRITER_VERSION,
        lease.clone(),
    )
    .expect("encode lease envelope");
    let lease_bytes = serde_json::to_vec(&lease_envelope).expect("serialize lease envelope");
    store
        .put_if_absent(&namespace_lease(lease.namespace_id.as_str()), &lease_bytes)
        .expect("seed lease object");
}

fn seed_verified_basis(store: &LocalFsStore, head: &HeadState, metadata_state: &MetadataState) {
    let basis_head = HeadState {
        snapshot_hint_seq: Some(head.seq),
        ..head.clone()
    };
    let checkpoint = prepare_checkpoint(&basis_head, metadata_state, TEST_WRITER_VERSION)
        .expect("prepare checkpoint");
    store
        .put_overwrite(
            &checkpoint.manifest.object_key,
            &checkpoint.manifest.encoded_bytes,
        )
        .expect("seed checkpoint manifest");
    for segment in &checkpoint.segments {
        store
            .put_overwrite(&segment.object_key, &segment.encoded_bytes)
            .expect("seed checkpoint segment");
    }

    let head_envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        TEST_WRITER_VERSION,
        basis_head,
    )
    .expect("encode basis head envelope");
    let head_bytes = serde_json::to_vec(&head_envelope).expect("serialize basis head envelope");
    store
        .put_overwrite(&namespace_head(head.namespace_id.as_str()), &head_bytes)
        .expect("overwrite head with checkpoint-backed basis");
}

fn seed_content_objects(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    content_objects: &SeededContentObjects,
) {
    let manifest_envelope =
        ContentManifestEnvelope::from_payload(content_objects.manifest.payload.clone())
            .expect("build content manifest envelope");
    let manifest_bytes = encode_content_manifest_json(&manifest_envelope).expect("encode manifest");
    assert_eq!(
        sha256_digest(&manifest_bytes),
        content_objects.manifest.digest,
        "fixture manifest digest should match encoded content manifest"
    );
    store
        .put_if_absent(
            &content_manifest(namespace_id.as_str(), &content_objects.manifest.digest),
            &manifest_bytes,
        )
        .expect("seed content manifest object");

    for block in &content_objects.blocks {
        let block_bytes = block.body_utf8.as_bytes();
        assert_eq!(
            sha256_digest(block_bytes),
            block.digest,
            "fixture block digest should match block body"
        );
        store
            .put_if_absent(&blob(namespace_id.as_str(), &block.digest), block_bytes)
            .expect("seed content block object");
    }
}

#[derive(Debug, Deserialize)]
struct UploadInitial {
    namespace_id: NamespaceId,
    local_file: FixtureLocalFile,
}

#[derive(Debug, Deserialize)]
struct FixtureLocalFile {
    relative_path: PathBuf,
    content_utf8: String,
}

#[derive(Debug, Deserialize)]
struct UploadActionEnvelope {
    upload_small_file: bool,
}

#[derive(Debug, Deserialize)]
struct UploadExpect {
    file_size_bytes: u64,
    file_digest_sha256: String,
    content_manifest_digest: String,
    manifest_object_key: String,
    manifest_payload: ContentManifestPayload,
    block_objects: Vec<ExpectedBlockObject>,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct ExpectedBlockObject {
    object_key: String,
    content_digest_sha256: String,
    plaintext_size_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct MutationInitial {
    head: HeadState,
    lease: LeaseState,
    #[serde(default)]
    metadata_state: MetadataState,
    content_objects: Option<SeededContentObjects>,
    client_request: RawClientMutationRequest,
}

#[derive(Debug, Deserialize)]
struct NativeMutationExpect {
    head: HeadState,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct NativeMutationErrorExpect {
    error: String,
}

#[derive(Debug, Deserialize)]
struct SeededContentObjects {
    manifest: SeededManifestObject,
    blocks: Vec<SeededBlockObject>,
}

#[derive(Debug, Deserialize)]
struct SeededManifestObject {
    digest: String,
    payload: ContentManifestPayload,
}

#[derive(Debug, Deserialize)]
struct SeededBlockObject {
    digest: String,
    body_utf8: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RawClientMutationRequest {
    namespace_id: NamespaceId,
    client_request_id: String,
    op: RawClientMutationOp,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum RawClientMutationOp {
    CreateFile { create_file: RawCreateFile },
    ReplaceFile { replace_file: RawReplaceFile },
}

#[derive(Debug, Clone, Deserialize)]
struct RawCreateFile {
    parent_inode_id: loon_types::InodeId,
    display_name: String,
    content_manifest_digest: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RawReplaceFile {
    inode_id: loon_types::InodeId,
    base_revision_no: loon_types::RevisionNo,
    content_manifest_digest: String,
}

impl From<RawClientMutationRequest> for ClientMutationRequest {
    fn from(value: RawClientMutationRequest) -> Self {
        let op = match value.op {
            RawClientMutationOp::CreateFile { create_file } => {
                loon_types::ClientMutationOp::CreateFile {
                    parent_inode_id: create_file.parent_inode_id,
                    display_name: create_file.display_name,
                    content_manifest_digest: create_file.content_manifest_digest,
                }
            }
            RawClientMutationOp::ReplaceFile { replace_file } => {
                loon_types::ClientMutationOp::ReplaceFile {
                    inode_id: replace_file.inode_id,
                    base_revision_no: replace_file.base_revision_no,
                    content_manifest_digest: replace_file.content_manifest_digest,
                }
            }
        };

        Self {
            namespace_id: value.namespace_id,
            client_request_id: value.client_request_id,
            op,
        }
    }
}

impl RawClientMutationRequest {
    fn content_manifest_digest(&self) -> Option<&str> {
        match &self.op {
            RawClientMutationOp::CreateFile { create_file } => {
                Some(create_file.content_manifest_digest.as_str())
            }
            RawClientMutationOp::ReplaceFile { replace_file } => {
                Some(replace_file.content_manifest_digest.as_str())
            }
        }
    }
}

fn load_fixture(relative_path: &str) -> Scenario {
    load_scenario_fixture(relative_path)
}
