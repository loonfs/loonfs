#![allow(clippy::panic)]
// These integration tests use panic for precise fixture-divergence diagnostics.

//! Golden-byte fixtures for every durable LoonFS encoding.
//!
//! These tests pin the exact bytes each format version writes. They exist to
//! make wire-format changes impossible to ship by accident:
//!
//! - If an encoder's output diverges from its fixture, a Rust-side change
//!   (field rename, reorder, serde attribute, removed field) silently changed
//!   the durable format. While LoonFS is pre-release, either revert it or
//!   regenerate the family's version-1 fixture with `UPDATE_GOLDEN=1 cargo
//!   test`; released formats follow the spec's evolution rules.
//! - If a fixture stops decoding, the current reader can no longer read bytes
//!   another implementation of the same format version wrote — a
//!   compatibility break once that format is deployed.
//! - Every durable family rejects unknown fields at every nesting level.
//!   Compaction and WAL folding write successor records, so permissive
//!   decoding could erase a field introduced by an unsupported writer.

use loonfs_api::wire::control::{
    decode_control_object, CheckpointOwner, CheckpointRecordState, ContentStoreState,
    ControlObjectEnvelope, ControlObjectKind, ForkBasis, HintState, ManifestRef, NamespaceStatus,
    ProxiedStaging, UploadSessionMode, UploadSessionRecordStatus, UploadSessionState, WriterBlock,
};
use loonfs_api::wire::envelope::EnvelopeCodecError;
use loonfs_api::wire::manifest::{
    decode_namespace_manifest_json, encode_namespace_manifest_json, ActiveDeletionRowAction,
    DeletedDirentry, MetadataRow, MetadataRowFamily, MetadataRunRef, MetadataSegmentRef,
    NamespaceManifestPayload, RunTier, TombstoneGeneration, TombstoneRowAction,
};
use loonfs_api::wire::wal::{
    decode_wal_segment_envelope_zstd, encode_wal_segment_envelope_zstd, WalCommitDelta,
    WalCommitPayload, WalDelta, WalSegmentPayload,
};
use loonfs_api::{
    sha256_digest, ActorId, AttributeKey, AttributeRevisionNo, AttributeValue, Attributes,
    ChangeSeq, CheckpointId, Checksum, ChecksumAlgorithm, CommitId, ContentId, ContentRef,
    ContentRefKind, ContentStoreId, InodeId, InodeKind, ManifestNo, MetadataSegmentId, NameKey,
    NamespaceId, RevisionNo, RunNo, UploadId, WalNo, WriterEpoch,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Golden helpers
// ---------------------------------------------------------------------------

fn golden_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name)
}

fn actor() -> ActorId {
    ActorId::parse("loonfs-golden").expect("valid actor id")
}

// Regenerate with `UPDATE_GOLDEN=1 cargo test -p loonfs-api -- --test-threads=1`:
// without the single thread, tests that read a fixture race the tests rewriting
// it and fail on a half-written file.
fn assert_matches_golden(name: &str, actual: &[u8]) {
    let path = golden_path(name);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().expect("fixture parent dir"))
            .expect("create golden dir");
        std::fs::write(&path, actual).expect("write golden fixture");
    }
    let expected = std::fs::read(&path).unwrap_or_else(|err| {
        panic!("read golden fixture `{name}` ({err}); run `UPDATE_GOLDEN=1 cargo test -p loonfs-api -- --test-threads=1` to generate it")
    });
    if expected != actual {
        let offset = expected
            .iter()
            .zip(actual.iter())
            .position(|(left, right)| left != right)
            .unwrap_or_else(|| expected.len().min(actual.len()));
        panic!(
            "golden fixture `{name}` diverged at byte {offset} \
             (expected {} bytes, actual {} bytes). The durable encoding changed: \
             while pre-release, regenerate the version-1 fixture with UPDATE_GOLDEN=1 \
             if the change is intentional.",
            expected.len(),
            actual.len(),
        );
    }
}

fn read_golden(name: &str) -> Vec<u8> {
    std::fs::read(golden_path(name)).unwrap_or_else(|err| {
        panic!("read golden fixture `{name}` ({err}); run `UPDATE_GOLDEN=1 cargo test -p loonfs-api -- --test-threads=1` to generate it")
    })
}

fn unzstd(bytes: &[u8]) -> Vec<u8> {
    zstd::stream::decode_all(bytes).expect("decompress envelope")
}

fn rezstd(bytes: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(bytes, 0).expect("compress envelope")
}

// ---------------------------------------------------------------------------
// Samples: fixed values covering every variant of every durable enum
// ---------------------------------------------------------------------------

fn namespace_id() -> NamespaceId {
    NamespaceId::parse("demo").expect("valid namespace id")
}

fn commit_id() -> CommitId {
    CommitId::parse("c_00000000000000000000000000000042").expect("valid commit id")
}

fn checkpoint_id(value: &str) -> CheckpointId {
    CheckpointId::parse(value).expect("valid checkpoint id")
}

fn content_id(value: &str) -> ContentId {
    ContentId::parse(value).expect("valid content id")
}

fn sample_content_ref() -> ContentRef {
    ContentRef::blob_v1(
        loonfs_api::NamespaceId::parse("demo").expect("namespace id"),
        content_id("con_0123456789abcdef0123456789abcdef"),
        b"golden bytes",
    )
}

/// A reference whose only evidence is a provider-computed full-object CRC.
///
/// No current write path produces one; it is here so the fixtures prove the
/// format decodes what direct multipart will write in the next wave.
fn sample_crc_content_ref() -> ContentRef {
    ContentRef {
        kind: ContentRefKind::BlobV1,
        owner_namespace_id: loonfs_api::NamespaceId::parse("demo").expect("namespace id"),
        content_id: content_id("con_fedcba9876543210fedcba9876543210"),
        size_bytes: 11_534_336,
        checksum: Checksum {
            algorithm: ChecksumAlgorithm::Crc64nvme,
            value: "bbb7305bdf118bcb".to_owned(),
        },
    }
}

#[test]
fn content_ref_matches_golden_bytes_for_every_checksum_algorithm() {
    let references = [
        sample_content_ref(),
        sample_crc_content_ref(),
        ContentRef {
            kind: ContentRefKind::BlobV1,
            owner_namespace_id: loonfs_api::NamespaceId::parse("demo").expect("namespace id"),
            content_id: content_id("con_00112233445566778899aabbccddeeff"),
            size_bytes: 4_096,
            checksum: Checksum {
                algorithm: ChecksumAlgorithm::Crc32c,
                value: "1a2b3c4d".to_owned(),
            },
        },
    ];
    let encoded = serde_json::to_vec_pretty(&references).expect("encode content refs");
    assert_matches_golden("content_refs.v1.json", &encoded);

    let decoded: Vec<ContentRef> =
        serde_json::from_slice(&read_golden("content_refs.v1.json")).expect("decode content refs");
    assert_eq!(decoded, references);
    for content_ref in &decoded {
        content_ref.validate().expect("golden references are valid");
    }
}

#[test]
fn content_ref_decode_rejects_unknown_fields() {
    let mut document: serde_json::Value =
        serde_json::to_value(sample_content_ref()).expect("encode content ref");
    document["checksum_type"] = serde_json::Value::from("full_object");

    let error = serde_json::from_value::<ContentRef>(document).expect_err("unknown field");
    assert!(
        error.to_string().contains("checksum_type"),
        "the rejection should name the field: {error}"
    );
}

fn segment_id() -> MetadataSegmentId {
    MetadataSegmentId::parse("seg_0123456789abcdef0123456789abcdef").expect("valid segment id")
}

fn name_key(value: &str) -> NameKey {
    NameKey::parse(value).expect("valid name key")
}

fn attribute_key(value: &str) -> AttributeKey {
    AttributeKey::parse(value).expect("valid attribute key")
}

/// One attribute map exercising ordinary and caller-encoded list values.
fn sample_attributes() -> Attributes {
    Attributes::new(std::collections::BTreeMap::from([
        (
            attribute_key("owner"),
            AttributeValue::parse("ada").expect("valid attribute value"),
        ),
        (
            attribute_key("tags"),
            AttributeValue::parse("draft,review").expect("valid attribute value"),
        ),
    ]))
    .expect("valid attribute map")
}

fn sample_wal_payload() -> WalSegmentPayload {
    let deltas = vec![
        WalCommitDelta {
            semantic_op_index: 0,
            delta: WalDelta::CreateInode {
                delta_index: 0,
                inode_id: InodeId(7),
                inode_kind: InodeKind::Directory,
            },
        },
        WalCommitDelta {
            semantic_op_index: 0,
            delta: WalDelta::BindDirentry {
                delta_index: 1,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("docs").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("Docs").expect("valid display name"),
                child_inode_id: InodeId(7),
            },
        },
        WalCommitDelta {
            semantic_op_index: 1,
            delta: WalDelta::UnbindDirentry {
                delta_index: 2,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("old.txt").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("Old.txt")
                    .expect("valid display name"),
                child_inode_id: InodeId(5),
                bind_seq: ChangeSeq(1),
                bind_delta_index: 0,
            },
        },
        WalCommitDelta {
            semantic_op_index: 2,
            delta: WalDelta::AppendFileRevision {
                delta_index: 3,
                inode_id: InodeId(5),
                revision_no: RevisionNo(2),
                content_ref: sample_content_ref(),
            },
        },
        WalCommitDelta {
            semantic_op_index: 3,
            delta: WalDelta::TombstoneSubtree {
                delta_index: 4,
                root_inode_id: InodeId(9),
                deleted_direntry: DeletedDirentry {
                    parent_inode_id: InodeId(1),
                    name_key: NameKey::parse("old.txt").expect("valid name key"),
                    display_name: loonfs_api::DisplayName::parse("Old.txt")
                        .expect("valid display name"),
                },
            },
        },
        WalCommitDelta {
            semantic_op_index: 4,
            delta: WalDelta::AppendAttributesRevision {
                delta_index: 5,
                inode_id: InodeId(5),
                attributes_revision_no: AttributeRevisionNo(2),
                attributes: sample_attributes(),
            },
        },
    ];
    WalSegmentPayload {
        namespace_id: namespace_id(),
        wal_no: WalNo(2),
        next_inode_id: InodeId(10),
        writer_epoch: WriterEpoch(3),
        base_head_seq: ChangeSeq(1),
        start_seq: ChangeSeq(2),
        end_seq: ChangeSeq(2),
        records: vec![WalCommitPayload {
            seq: ChangeSeq(2),
            commit_id: commit_id(),
            committed_by: actor(),
            semantic_commit_fingerprint: serde_json::from_str(
                r#""v3:sha256:0000000000000000000000000000000000000000000000000000000000000042""#,
            )
            .expect("fingerprint"),
            committed_at_ms: 4_000,
            message: Some("golden commit".to_owned()),
            deltas,
        }],
    }
}

fn sample_manifest_payload() -> NamespaceManifestPayload {
    let mut manifest = NamespaceManifestPayload {
        content_store_id: content_store_id(),
        created_at_ms: 1_000,
        fork_basis: None,
        status: NamespaceStatus::Active {},
        writer: Some(WriterBlock {
            writer_id: loonfs_api::WriterId::parse("writer-a").expect("writer"),
            acquired_at_ms: 2_000,
        }),
        last_folded_wal_no: WalNo(2),
        retention_floor_wal_no: WalNo(0),
        compactor_epoch: 0,
        namespace_id: namespace_id(),
        manifest_no: ManifestNo(2),

        head_seq: ChangeSeq(2),
        head_commit_id: commit_id(),
        base_seq: ChangeSeq(2),
        writer_epoch: WriterEpoch(3),
        next_inode_id: InodeId(10),
        next_run_no: RunNo(1),
        retention_floor_seq: ChangeSeq(0),
        runs: vec![MetadataRunRef {
            run_no: RunNo(0),
            run_seq: ChangeSeq(2),
            tier: RunTier::Delta,
            segments: vec![MetadataSegmentRef {
                owner_namespace_id: namespace_id(),
                segment_id: segment_id(),
                family: MetadataRowFamily::Inodes,
                segment_index: 0,
                row_count: 6,
                min_row_key: "commit-receipt".to_owned(),
                max_row_key: "tombstone".to_owned(),
                index_block: loonfs_api::wire::sst_blocks::BlockHandle {
                    offset: 4_000,
                    stored_len: 200,
                    decoded_len: 400,
                    crc32c: 0x1234_5678,
                },
                filter_block: loonfs_api::wire::sst_blocks::BlockHandle {
                    offset: 3_900,
                    stored_len: 100,
                    decoded_len: 100,
                    crc32c: 0x9abc_def0,
                },
                // Only small filters are inlined; this descriptor's filter
                // is read through its handle, so the field is omitted.
                filter_inline: None,
                object_checksum: sha256_digest(b"sst payload"),
            }],
        }],
    };
    let mut publication = manifest.runs[0].segments[0].clone();
    publication.family = MetadataRowFamily::ContentPublications;
    publication.segment_id =
        MetadataSegmentId::parse("seg_0123456789abcdef0123456789abcdee").expect("segment id");
    publication.min_row_key = sample_content_publication_row().row_key();
    publication.max_row_key = publication.min_row_key.clone();
    publication.row_count = 1;
    manifest.runs[0].segments.push(publication);
    manifest
}

/// Returns a manifest reference owned by the sample namespace.
fn sample_manifest_ref(number: u64) -> ManifestRef {
    ManifestRef {
        owner_namespace_id: namespace_id(),
        manifest_no: ManifestNo(number),

        manifest_head_seq: ChangeSeq(number),
        manifest_payload_checksum:
            "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_owned(),
    }
}

fn sample_deleted_manifest() -> NamespaceManifestPayload {
    NamespaceManifestPayload {
        status: NamespaceStatus::Deleted {
            reclaim_after_ms: None,
        },
        ..sample_manifest_payload()
    }
}

/// A fork manifest records its source pin and manifest.
fn sample_fork_manifest() -> NamespaceManifestPayload {
    NamespaceManifestPayload {
        fork_basis: Some(ForkBasis {
            manifest: ManifestRef {
                owner_namespace_id: NamespaceId::parse("source").expect("valid namespace id"),
                manifest_no: ManifestNo(2),

                manifest_head_seq: ChangeSeq(2),
                manifest_payload_checksum:
                    "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
                        .to_owned(),
            },
            source_checkpoint_id: checkpoint_id("pin_00000000000000000002-0000000000000002"),
        }),
        ..sample_manifest_payload()
    }
}

// ---------------------------------------------------------------------------
// Encode/decode round trips against committed fixtures
// ---------------------------------------------------------------------------

#[test]
fn wal_segment_document_matches_golden_bytes() {
    let encoded = encode_wal_segment_envelope_zstd(sample_wal_payload())
        .expect("encode wal")
        .into_bytes();
    // Compare the decompressed document: zstd frames may differ across zstd
    // versions, the document bytes (which the checksum covers) may not.
    assert_matches_golden("wal_segment.v1.cbor", &unzstd(&encoded));
}

#[test]
fn wal_segment_golden_decodes_to_sample() {
    let decoded = decode_wal_segment_envelope_zstd(&rezstd(&read_golden("wal_segment.v1.cbor")))
        .expect("decode golden wal segment");
    assert_eq!(decoded.into_payload(), sample_wal_payload());
}

/// Stores `payload` through the real codec and returns why decoding refused
/// it.

#[test]
fn namespace_manifest_matches_golden_bytes() {
    let encoded = encode_namespace_manifest_json(sample_manifest_payload())
        .expect("encode")
        .into_bytes();
    assert_matches_golden("namespace_manifest.v1.json", &encoded);
    let document: serde_json::Value = serde_json::from_slice(&encoded).expect("manifest json");
    let payload = document["payload"].as_object().expect("manifest payload");
    assert!(!payload.contains_key("index_files"));
    assert!(!payload.contains_key("features"));
}

#[test]
fn namespace_manifest_golden_decodes_to_sample() {
    let decoded = decode_namespace_manifest_json(&read_golden("namespace_manifest.v1.json"))
        .expect("decode golden manifest");
    assert_eq!(decoded.into_payload(), sample_manifest_payload());
}

fn check_control_golden<T>(fixture: &str, kind: ControlObjectKind, state: T)
where
    T: Serialize + DeserializeOwned + PartialEq + Debug,
{
    let encoded = loonfs_api::wire::control::encode_control_state(kind, &state)
        .expect("encode control object");
    assert_matches_golden(fixture, &encoded);

    let decoded: ControlObjectEnvelope<T> =
        decode_control_object(&read_golden(fixture), kind).expect("decode golden control object");
    assert_eq!(decoded.into_payload(), state);
}

fn control_document_with_payload_edit(
    fixture: &str,
    edit: impl FnOnce(&mut serde_json::Value),
) -> Vec<u8> {
    let mut document: serde_json::Value =
        serde_json::from_slice(&read_golden(fixture)).expect("decode control fixture");
    edit(&mut document["payload"]);
    let payload = serde_json::to_string(&document["payload"]).expect("encode edited payload");
    document["payload_checksum"] = serde_json::Value::from(sha256_digest(payload.as_bytes()));
    format!(
        "{{\"kind\":{},\"format_version\":{},\"payload_checksum\":{},\"payload\":{}}}",
        document["kind"], document["format_version"], document["payload_checksum"], payload,
    )
    .into_bytes()
}

/// Returns the refusal an edited payload must produce. Every schema
/// rejection and every hand-checked invariant shares one error variant, so a
/// caller that cares which rule fired reads it out of the message.
fn assert_control_payload_edit_is_corrupt<T>(
    fixture: &str,
    kind: ControlObjectKind,
    edit: impl FnOnce(&mut serde_json::Value),
) -> String
where
    T: DeserializeOwned + Debug,
{
    let edited = control_document_with_payload_edit(fixture, edit);
    let error = decode_control_object::<T>(&edited, kind)
        .expect_err("unknown mutable payload field must be rejected");
    match error {
        EnvelopeCodecError::PayloadDecode(message) => message,
        other => panic!("unexpected error for {kind:?}: {other}"),
    }
}

#[test]
fn manifest_status_reading_is_fail_closed_on_unknown_statuses() {
    // Every manifest writes the field, active manifests included, and an active
    // manifest round-trips through the tagged object it writes.
    let active = sample_manifest_payload();
    let encoded = serde_json::to_string(&active).expect("encode active manifest");
    assert!(
        encoded.contains("\"status\":{\"kind\":\"active\"}"),
        "an active manifest writes its status: {encoded}"
    );
    let round_tripped = serde_json::from_str::<NamespaceManifestPayload>(&encoded)
        .expect("an active manifest round-trips");
    assert_eq!(round_tripped, active);

    // A status this build does not know must fail decode, never default:
    // serving a namespace in an unrecognized status is the one mistake the
    // field exists to prevent.
    let future = encoded.replacen(
        "\"status\":{\"kind\":\"active\"}",
        "\"status\":{\"kind\":\"frozen\"}",
        1,
    );
    serde_json::from_str::<NamespaceManifestPayload>(&future)
        .expect_err("an unknown status must fail closed");

    let deleted = serde_json::to_string(&sample_deleted_manifest()).expect("encode deleted");
    assert!(deleted.contains("\"status\":{\"kind\":\"deleted\"}"));
}

#[test]
fn manifest_without_a_status_is_rejected() {
    let mut document = serde_json::to_value(sample_manifest_payload())
        .expect("encode active manifest as a document");
    document
        .as_object_mut()
        .expect("manifest document")
        .remove("status");

    let error = serde_json::from_value::<NamespaceManifestPayload>(document)
        .expect_err("a manifest without its status must be rejected");
    assert!(
        error.to_string().contains("status"),
        "the rejection should name the field: {error}"
    );
}

#[test]
fn manifest_status_rejects_unknown_fields_as_corruption() {
    let mut document = serde_json::to_value(sample_manifest_payload())
        .expect("encode active manifest as a document");
    document["status"]["field_from_the_future"] = serde_json::Value::from(true);

    let error = serde_json::from_value::<NamespaceManifestPayload>(document)
        .expect_err("a status carrying an unknown field must be rejected");
    assert!(
        error.to_string().contains("field_from_the_future"),
        "the rejection should name the field: {error}"
    );
}

#[test]
fn control_objects_match_golden_bytes() {
    check_control_golden(
        "control_content_store.v1.json",
        ControlObjectKind::ContentStore,
        ContentStoreState {
            content_store_id: content_store_id(),
            created_at_ms: 1_000,
        },
    );
    check_control_golden(
        "control_hint.v1.json",
        ControlObjectKind::Hint,
        HintState {
            namespace_id: namespace_id(),
            manifest_no: ManifestNo(2),
            wal_no: WalNo(2),
        },
    );
    check_control_golden(
        "control_checkpoint_record.v1.json",
        ControlObjectKind::CheckpointRecord,
        CheckpointRecordState {
            pin_id: checkpoint_id("pin_00000000000000000002-0000000000000002"),
            namespace_id: namespace_id(),
            manifest_no: (sample_manifest_ref(2)).manifest_no,
            manifest_head_seq: (sample_manifest_ref(2)).manifest_head_seq,
            manifest_payload_checksum: (sample_manifest_ref(2)).manifest_payload_checksum.clone(),
            head_commit_id: commit_id(),
            created_at_ms: 3_000,
            owner: CheckpointOwner::User {
                name: "nightly".to_owned(),
                expires_at_ms: None,
            },
        },
    );
    check_control_golden(
        "control_checkpoint_record_fork.v1.json",
        ControlObjectKind::CheckpointRecord,
        CheckpointRecordState {
            pin_id: checkpoint_id("pin_00000000000000000004-0000000000000004"),
            namespace_id: namespace_id(),
            manifest_no: (sample_manifest_ref(4)).manifest_no,
            manifest_head_seq: (sample_manifest_ref(4)).manifest_head_seq,
            manifest_payload_checksum: (sample_manifest_ref(4)).manifest_payload_checksum.clone(),
            head_commit_id: commit_id(),
            created_at_ms: 3_000,
            owner: CheckpointOwner::Fork {
                target_namespace_id: NamespaceId::parse("clone").expect("valid namespace id"),
            },
        },
    );
    check_control_golden(
        "control_checkpoint_record_snapshot.v1.json",
        ControlObjectKind::CheckpointRecord,
        CheckpointRecordState {
            pin_id: checkpoint_id("pin_00000000000000000006-0000000000000006"),
            namespace_id: namespace_id(),
            manifest_no: (sample_manifest_ref(6)).manifest_no,
            manifest_head_seq: (sample_manifest_ref(6)).manifest_head_seq,
            manifest_payload_checksum: (sample_manifest_ref(6)).manifest_payload_checksum.clone(),
            head_commit_id: commit_id(),
            created_at_ms: 3_000,
            owner: CheckpointOwner::Snapshot {
                name: "report-run".to_owned(),
                expires_at_ms: 9_000,
            },
        },
    );
    check_control_golden(
        "control_upload_session.v1.json",
        ControlObjectKind::UploadSession,
        UploadSessionState {
            namespace_id: namespace_id(),
            upload_id: UploadId::parse("upl_0123456789abcdef0123456789abcdef")
                .expect("valid upload id"),
            content_id: content_id("con_0123456789abcdef0123456789abcdef"),
            created_at_ms: 1_000,
            mode: UploadSessionMode::ServiceProxied {
                staging: ProxiedStaging::Idle,
            },
            status: UploadSessionRecordStatus::Completed {
                completed_at_ms: 2_000,
                content_ref: sample_content_ref(),
            },
        },
    );
    check_control_golden(
        "control_upload_session_direct_put.v1.json",
        ControlObjectKind::UploadSession,
        UploadSessionState {
            namespace_id: namespace_id(),
            upload_id: UploadId::parse("upl_abcdef0123456789abcdef0123456789")
                .expect("valid upload id"),
            content_id: content_id("con_0123456789abcdef0123456789abcdef"),
            created_at_ms: 1_000,
            mode: UploadSessionMode::DirectPut {
                checksum_algorithm: ChecksumAlgorithm::Sha256,
            },
            status: UploadSessionRecordStatus::Open {
                expires_at_ms: 87_400_000,
            },
        },
    );
    // The one session shape that carries a provider handle, and the one
    // that claims nothing at all: a multipart session is opened before its
    // payload is known, so it records identity, the provider upload, and
    // the geometry — and learns what it assembled only at completion.
    check_control_golden(
        "control_upload_session_direct_multipart.v1.json",
        ControlObjectKind::UploadSession,
        UploadSessionState {
            namespace_id: namespace_id(),
            upload_id: UploadId::parse("upl_22222222222222222222222222222222")
                .expect("valid upload id"),
            content_id: content_id("con_22222222222222222222222222222222"),
            created_at_ms: 1_000,
            mode: UploadSessionMode::DirectMultipart {
                provider_upload_id: "provider-upload-id".to_owned(),
                part_size_bytes: NonZeroU64::new(8 * 1024 * 1024).expect("part size"),
                checksum_algorithm: ChecksumAlgorithm::Crc64nvme,
            },
            status: UploadSessionRecordStatus::Open {
                expires_at_ms: 87_400_000,
            },
        },
    );
    // A proxied session mid-flight: it has written its bytes and recorded
    // what they were, and is still open to complete.
    check_control_golden(
        "control_upload_session_staged.v1.json",
        ControlObjectKind::UploadSession,
        UploadSessionState {
            namespace_id: namespace_id(),
            upload_id: UploadId::parse("upl_33333333333333333333333333333333")
                .expect("valid upload id"),
            content_id: content_id("con_0123456789abcdef0123456789abcdef"),
            created_at_ms: 1_000,
            mode: UploadSessionMode::ServiceProxied {
                staging: ProxiedStaging::Staged(sample_content_ref()),
            },
            status: UploadSessionRecordStatus::Open {
                expires_at_ms: 87_400_000,
            },
        },
    );
    check_control_golden(
        "control_upload_session_claimed.v1.json",
        ControlObjectKind::UploadSession,
        UploadSessionState {
            namespace_id: namespace_id(),
            upload_id: UploadId::parse("upl_44444444444444444444444444444444")
                .expect("valid upload id"),
            content_id: content_id("con_44444444444444444444444444444444"),
            created_at_ms: 1_000,
            mode: UploadSessionMode::ServiceProxied {
                staging: ProxiedStaging::Claimed,
            },
            status: UploadSessionRecordStatus::Open {
                expires_at_ms: 87_400_000,
            },
        },
    );
    check_control_golden(
        "control_upload_session_aborted.v1.json",
        ControlObjectKind::UploadSession,
        UploadSessionState {
            namespace_id: namespace_id(),
            upload_id: UploadId::parse("upl_11111111111111111111111111111111")
                .expect("valid upload id"),
            content_id: content_id("con_11111111111111111111111111111111"),
            created_at_ms: 1_000,
            mode: UploadSessionMode::ServiceProxied {
                staging: ProxiedStaging::Idle,
            },
            status: UploadSessionRecordStatus::Aborted {
                aborted_at_ms: 5_000,
            },
        },
    );
}

#[test]
fn every_durable_status_is_a_kind_tagged_object() {
    let fixtures = [
        "namespace_manifest.v1.json",
        "namespace_manifest.deleted.v1.json",
        "namespace_manifest.retired.v1.json",
        "control_upload_session.v1.json",
    ];
    for fixture in fixtures {
        let document: serde_json::Value =
            serde_json::from_slice(&read_golden(fixture)).expect("decode control fixture");
        let payload = document["payload"]
            .as_object()
            .unwrap_or_else(|| panic!("`{fixture}` has an object payload"));
        assert!(
            !payload.contains_key("state") && !payload.contains_key("lifecycle"),
            "`{fixture}` spells its lifecycle field `status`"
        );
        let status = payload
            .get("status")
            .unwrap_or_else(|| panic!("`{fixture}` writes a `status`"));
        let tag = status
            .as_object()
            .unwrap_or_else(|| panic!("`{fixture}` writes `status` as an object"))
            .get("kind")
            .unwrap_or_else(|| panic!("`{fixture}` tags its `status` with `kind`"));
        assert!(
            tag.is_string(),
            "`{fixture}` tags its `status` with a string, got {tag}"
        );
    }
}

#[test]
fn every_control_payload_rejects_unknown_fields_as_corruption() {
    let add_unknown = |payload: &mut serde_json::Value| {
        payload["field_from_the_future"] = serde_json::Value::from(true);
    };
    assert_control_payload_edit_is_corrupt::<HintState>(
        "control_hint.v1.json",
        ControlObjectKind::Hint,
        add_unknown,
    );
    assert_control_payload_edit_is_corrupt::<ContentStoreState>(
        "control_content_store.v1.json",
        ControlObjectKind::ContentStore,
        add_unknown,
    );
    assert_control_payload_edit_is_corrupt::<CheckpointRecordState>(
        "control_checkpoint_record.v1.json",
        ControlObjectKind::CheckpointRecord,
        add_unknown,
    );
    assert_control_payload_edit_is_corrupt::<UploadSessionState>(
        "control_upload_session.v1.json",
        ControlObjectKind::UploadSession,
        add_unknown,
    );
}

#[test]
fn mutable_control_nested_structs_reject_unknown_fields_as_corruption() {
    assert_control_payload_edit_is_corrupt::<CheckpointRecordState>(
        "control_checkpoint_record.v1.json",
        ControlObjectKind::CheckpointRecord,
        |payload| payload["owner"]["field_from_the_future"] = serde_json::Value::from(true),
    );
    assert_control_payload_edit_is_corrupt::<UploadSessionState>(
        "control_upload_session.v1.json",
        ControlObjectKind::UploadSession,
        |payload| payload["status"]["field_from_the_future"] = serde_json::Value::from(true),
    );
    assert_control_payload_edit_is_corrupt::<UploadSessionState>(
        "control_upload_session_staged.v1.json",
        ControlObjectKind::UploadSession,
        |payload| {
            payload["mode"]["staging"]["content_ref"]["field_from_the_future"] =
                serde_json::Value::from(true);
        },
    );
    let message = assert_control_payload_edit_is_corrupt::<UploadSessionState>(
        "control_upload_session_claimed.v1.json",
        ControlObjectKind::UploadSession,
        |payload| {
            payload["mode"]["staging"]["at_ms"] = serde_json::Value::from(1_500);
        },
    );
    assert!(
        message.contains("unknown field `at_ms`"),
        "unexpected refusal: {message}"
    );
    assert_control_payload_edit_is_corrupt::<UploadSessionState>(
        "control_upload_session_direct_multipart.v1.json",
        ControlObjectKind::UploadSession,
        |payload| {
            payload["mode"]["field_from_the_future"] = serde_json::Value::from(true);
        },
    );
}

#[test]
fn snapshot_checkpoint_records_reject_a_missing_expiry() {
    let fixture = "control_checkpoint_record_snapshot.v1.json";
    let message = assert_control_payload_edit_is_corrupt::<CheckpointRecordState>(
        fixture,
        ControlObjectKind::CheckpointRecord,
        |payload| {
            payload
                .get_mut("owner")
                .expect("owner")
                .as_object_mut()
                .expect("owner object")
                .remove("expires_at_ms");
        },
    );
    assert!(
        message.contains("missing field `expires_at_ms`"),
        "unexpected refusal for `{fixture}`: {message}"
    );
}

#[test]
fn checkpoint_records_reject_an_untagged_or_unknown_owner() {
    assert_control_payload_edit_is_corrupt::<CheckpointRecordState>(
        "control_checkpoint_record.v1.json",
        ControlObjectKind::CheckpointRecord,
        |payload| payload["owner"]["kind"] = serde_json::Value::from("unknown_owner"),
    );
    assert_control_payload_edit_is_corrupt::<CheckpointRecordState>(
        "control_checkpoint_record.v1.json",
        ControlObjectKind::CheckpointRecord,
        |payload| payload["owner"] = serde_json::Value::from("snapshot"),
    );
}

#[test]
fn upload_sessions_reject_an_untagged_or_incomplete_status() {
    for untagged in ["open", "condemned"] {
        assert_control_payload_edit_is_corrupt::<UploadSessionState>(
            "control_upload_session.v1.json",
            ControlObjectKind::UploadSession,
            |payload| payload["status"] = serde_json::Value::from(untagged),
        );
    }
    // Statuses this format does not define are refused by tag alone.
    for unknown_kind in ["active", "condemned"] {
        assert_control_payload_edit_is_corrupt::<UploadSessionState>(
            "control_upload_session.v1.json",
            ControlObjectKind::UploadSession,
            |payload| payload["status"]["kind"] = serde_json::Value::from(unknown_kind),
        );
    }
    // Every status is defined by its own stamp: without one it cannot be
    // aged, so it is not that status.
    for tagged_without_its_stamp in ["open", "completed", "aborted"] {
        assert_control_payload_edit_is_corrupt::<UploadSessionState>(
            "control_upload_session.v1.json",
            ControlObjectKind::UploadSession,
            |payload| {
                payload["status"] = serde_json::json!({ "kind": tagged_without_its_stamp });
            },
        );
    }
}

#[test]
fn mutable_control_enums_fail_closed_on_unknown_variants() {
    assert_control_payload_edit_is_corrupt::<UploadSessionState>(
        "control_upload_session_staged.v1.json",
        ControlObjectKind::UploadSession,
        |payload| {
            payload["mode"]["staging"]["content_ref"]["kind"] =
                serde_json::Value::from("future_content_kind");
        },
    );
    assert_control_payload_edit_is_corrupt::<UploadSessionState>(
        "control_upload_session_direct_multipart.v1.json",
        ControlObjectKind::UploadSession,
        |payload| payload["mode"]["kind"] = serde_json::Value::from("future_mode"),
    );
}

#[test]
fn upload_sessions_reject_the_pre_mode_flat_encoding() {
    // Reject a string mode and mode-specific fields at the top level.
    assert_control_payload_edit_is_corrupt::<UploadSessionState>(
        "control_upload_session.v1.json",
        ControlObjectKind::UploadSession,
        |payload| {
            let content_ref = payload["status"]["content_ref"].clone();
            let object = payload.as_object_mut().expect("payload object");
            object.insert("mode".to_owned(), serde_json::Value::from("direct_put"));
            object.insert(
                "claimed_checksum".to_owned(),
                content_ref["checksum"].clone(),
            );
            object.insert("direct_put_content_ref".to_owned(), content_ref.clone());
            object.insert("staged_content_ref".to_owned(), content_ref);
        },
    );
    // Reject each top-level mode-specific field on its own.
    for legacy_field in [
        "claimed_checksum",
        "direct_put_content_ref",
        "provider_multipart_upload_id",
        "multipart_part_size_bytes",
        "staged_content_ref",
    ] {
        assert_control_payload_edit_is_corrupt::<UploadSessionState>(
            "control_upload_session.v1.json",
            ControlObjectKind::UploadSession,
            |payload| payload[legacy_field] = serde_json::Value::from("direct_put"),
        );
    }
    // A mode must be a tagged object, not a string.
    assert_control_payload_edit_is_corrupt::<UploadSessionState>(
        "control_upload_session.v1.json",
        ControlObjectKind::UploadSession,
        |payload| payload["mode"] = serde_json::Value::from("direct_put"),
    );
    // Every session must declare a mode.
    assert_control_payload_edit_is_corrupt::<UploadSessionState>(
        "control_upload_session.v1.json",
        ControlObjectKind::UploadSession,
        |payload| {
            payload
                .as_object_mut()
                .expect("payload object")
                .remove("mode");
        },
    );
}

#[test]
fn upload_sessions_reject_a_reference_to_another_content_object() {
    let other = serde_json::Value::from("con_99999999999999999999999999999999");
    assert_control_payload_edit_is_corrupt::<UploadSessionState>(
        "control_upload_session.v1.json",
        ControlObjectKind::UploadSession,
        |payload| payload["status"]["content_ref"]["content_id"] = other.clone(),
    );
    assert_control_payload_edit_is_corrupt::<UploadSessionState>(
        "control_upload_session_staged.v1.json",
        ControlObjectKind::UploadSession,
        |payload| {
            payload["mode"]["staging"]["content_ref"]["content_id"] = other.clone();
        },
    );
}

#[test]
fn direct_put_sessions_reject_the_pre_completion_claim_record() {
    let message = assert_control_payload_edit_is_corrupt::<UploadSessionState>(
        "control_upload_session_direct_put.v1.json",
        ControlObjectKind::UploadSession,
        |payload| {
            payload["mode"]
                .as_object_mut()
                .expect("mode object")
                .remove("checksum_algorithm");
            payload["mode"]["promised_content"] =
                serde_json::to_value(sample_content_ref()).expect("content ref");
        },
    );
    assert!(
        message.contains("unknown field `promised_content`"),
        "unexpected refusal: {message}"
    );
}

#[test]
fn completed_direct_sessions_require_the_session_algorithm() {
    for mode in [
        serde_json::json!({
            "kind": "direct_put",
            "checksum_algorithm": "crc32c"
        }),
        serde_json::json!({
            "kind": "direct_multipart",
            "provider_upload_id": "provider-upload",
            "part_size_bytes": 8_388_608,
            "checksum_algorithm": "crc32c"
        }),
    ] {
        let message = assert_control_payload_edit_is_corrupt::<UploadSessionState>(
            "control_upload_session.v1.json",
            ControlObjectKind::UploadSession,
            |payload| payload["mode"] = mode,
        );
        assert!(
            message.contains("requires `crc32c` but its completed content uses `sha256`"),
            "unexpected refusal: {message}"
        );
    }
}

#[test]
fn upload_sessions_reject_a_mode_missing_its_own_fields() {
    assert_control_payload_edit_is_corrupt::<UploadSessionState>(
        "control_upload_session_direct_put.v1.json",
        ControlObjectKind::UploadSession,
        |payload| {
            payload["mode"]
                .as_object_mut()
                .expect("mode object")
                .remove("checksum_algorithm");
        },
    );
    for missing in [
        "provider_upload_id",
        "part_size_bytes",
        "checksum_algorithm",
    ] {
        assert_control_payload_edit_is_corrupt::<UploadSessionState>(
            "control_upload_session_direct_multipart.v1.json",
            ControlObjectKind::UploadSession,
            |payload| {
                payload["mode"]
                    .as_object_mut()
                    .expect("mode object")
                    .remove(missing);
            },
        );
    }
    // A mode cannot contain fields from another variant.
    assert_control_payload_edit_is_corrupt::<UploadSessionState>(
        "control_upload_session_direct_multipart.v1.json",
        ControlObjectKind::UploadSession,
        |payload| payload["mode"]["kind"] = serde_json::Value::from("service_proxied"),
    );
}

#[test]
fn upload_sessions_reject_a_zero_multipart_part_size() {
    assert_control_payload_edit_is_corrupt::<UploadSessionState>(
        "control_upload_session_direct_multipart.v1.json",
        ControlObjectKind::UploadSession,
        |payload| payload["mode"]["part_size_bytes"] = serde_json::Value::from(0),
    );
}

#[test]
fn mutable_control_envelope_rejects_unknown_fields_as_corruption() {
    let mut document: serde_json::Value =
        serde_json::from_slice(&read_golden("control_hint.v1.json"))
            .expect("decode control fixture");
    document["field_from_the_future"] = serde_json::Value::from(true);
    let edited = serde_json::to_vec(&document).expect("encode edited envelope");

    let error = decode_control_object::<HintState>(&edited, ControlObjectKind::Hint)
        .expect_err("unknown mutable envelope field must be rejected");
    assert!(
        matches!(error, EnvelopeCodecError::EnvelopeDecode(_)),
        "unexpected error: {error}"
    );
}

#[test]
fn control_object_decoders_reject_wrong_format_version_without_fallback() {
    let cases = [
        (
            ControlObjectKind::ContentStore,
            serde_json::to_value(ContentStoreState {
                content_store_id: content_store_id(),
                created_at_ms: 1_000,
            })
            .expect("content store state"),
        ),
        (
            ControlObjectKind::CheckpointRecord,
            serde_json::to_value(CheckpointRecordState {
                pin_id: checkpoint_id("pin_00000000000000000005-0000000000000005"),
                namespace_id: namespace_id(),
                manifest_no: ManifestNo(5),

                manifest_head_seq: ChangeSeq(5),
                manifest_payload_checksum: sha256_digest(b"manifest"),
                head_commit_id: commit_id(),
                created_at_ms: 3_000,
                owner: CheckpointOwner::User {
                    name: "nightly".to_owned(),
                    expires_at_ms: None,
                },
            })
            .expect("checkpoint state"),
        ),
        (
            ControlObjectKind::UploadSession,
            serde_json::to_value(UploadSessionState {
                namespace_id: namespace_id(),
                upload_id: UploadId::parse("upl_11111111111111111111111111111111")
                    .expect("valid upload id"),
                content_id: content_id("con_11111111111111111111111111111111"),
                created_at_ms: 1_000,
                mode: UploadSessionMode::ServiceProxied {
                    staging: ProxiedStaging::Idle,
                },
                status: UploadSessionRecordStatus::Aborted {
                    aborted_at_ms: 5_000,
                },
            })
            .expect("upload state"),
        ),
    ];
    for (kind, state) in cases {
        let encoded =
            loonfs_api::wire::control::encode_control_state(kind, &state).expect("encode control");
        let mut document: serde_json::Value =
            serde_json::from_slice(&encoded).expect("decode document");
        document["format_version"] = serde_json::Value::from(7);
        let wrong_version = serde_json::to_vec(&document).expect("encode wrong version");
        let error = decode_control_object::<serde_json::Value>(&wrong_version, kind)
            .expect_err("wrong version must not fall back");
        assert!(matches!(
            error,
            EnvelopeCodecError::UnsupportedFormatVersion {
                found: 7,
                supported,
                ..
            } if supported == kind.format_version()
        ));
    }
}

#[test]
fn metadata_row_family_wire_tags_are_pinned() {
    let tags: Vec<String> = [
        MetadataRowFamily::Inodes,
        MetadataRowFamily::DirentryBinds,
        MetadataRowFamily::DirentryChildBinds,
        MetadataRowFamily::DirentryUnbinds,
        MetadataRowFamily::Revisions,
        MetadataRowFamily::Tombstones,
        MetadataRowFamily::ActiveDeletions,
        MetadataRowFamily::CommitReceipts,
        MetadataRowFamily::ContentPublications,
        MetadataRowFamily::Attributes,
    ]
    .iter()
    .map(|family| serde_json::to_string(family).expect("family tag"))
    .collect();
    assert_eq!(
        tags,
        [
            "\"inodes\"",
            "\"direntry_binds\"",
            "\"direntry_child_binds\"",
            "\"direntry_unbinds\"",
            "\"revisions\"",
            "\"tombstones\"",
            "\"active_deletions\"",
            "\"commit_receipts\"",
            "\"content_publications\"",
            "\"attributes\"",
        ],
        "family tags are durable bytes in every manifest descriptor"
    );
}

fn content_store_id() -> ContentStoreId {
    ContentStoreId::parse("cs_0123456789abcdef0123456789abcdef").expect("valid content store id")
}

// ---------------------------------------------------------------------------
// Version, kind, and corruption semantics
// ---------------------------------------------------------------------------

/// Edits a WAL payload and updates its checksum.
fn wal_document_with_payload_edit(
    payload: &WalSegmentPayload,
    edit: impl FnOnce(&mut ciborium::Value),
) -> Vec<u8> {
    let document = unzstd(
        &encode_wal_segment_envelope_zstd(payload.clone())
            .expect("wal")
            .into_bytes(),
    );
    let document_value: ciborium::Value =
        ciborium::de::from_reader(document.as_slice()).expect("decode document map");
    let payload_bytes = document_value
        .as_map()
        .expect("document is a map")
        .iter()
        .find(|(key, _)| key.as_text() == Some("payload"))
        .and_then(|(_, value)| value.as_bytes())
        .expect("payload is a byte string")
        .clone();
    let mut payload: ciborium::Value =
        ciborium::de::from_reader(payload_bytes.as_slice()).expect("decode payload");
    edit(&mut payload);
    let mut edited = Vec::new();
    ciborium::ser::into_writer(&payload, &mut edited).expect("encode edited payload");

    let with_payload = with_cbor_document_entry(&document, "payload", |value| {
        *value = ciborium::Value::Bytes(edited.clone());
    });
    let restated = with_cbor_document_entry(&with_payload, "payload_checksum", |value| {
        *value = ciborium::Value::from(sha256_digest(&edited));
    });
    rezstd(&restated)
}

/// Adds an unknown field to a CBOR map.
fn with_future_field(value: &mut ciborium::Value) {
    cbor_map_of(value).push((
        ciborium::Value::from("field_from_the_future"),
        ciborium::Value::from(true),
    ));
}

/// Returns the sample WAL payload's only commit.
fn payload_commit(payload: &mut ciborium::Value) -> &mut ciborium::Value {
    cbor_entry(payload, "records")
        .as_array_mut()
        .expect("records is an array")
        .first_mut()
        .expect("the sample carries one commit")
}

/// Returns the delta at `position` in the sample commit.
fn commit_delta(payload: &mut ciborium::Value, position: usize) -> &mut ciborium::Value {
    let delta = cbor_entry(payload_commit(payload), "deltas")
        .as_array_mut()
        .expect("deltas is an array")
        .get_mut(position)
        .expect("the commit carries this delta");
    cbor_entry(delta, "delta")
}

/// Builds a sample segment with the supplied deltas.
fn wal_payload_with_deltas(deltas: Vec<WalCommitDelta>) -> WalSegmentPayload {
    let mut payload = sample_wal_payload();
    payload.records[0].deltas = deltas;
    payload
}

/// Rewrites one top-level entry of a CBOR document map.
fn with_cbor_document_entry(
    document: &[u8],
    key: &str,
    edit: impl FnOnce(&mut ciborium::Value),
) -> Vec<u8> {
    let mut value: ciborium::Value =
        ciborium::de::from_reader(document).expect("decode document map");
    let entries = value.as_map_mut().expect("document is a map");
    let entry = entries
        .iter_mut()
        .find(|(entry_key, _)| entry_key.as_text() == Some(key))
        .unwrap_or_else(|| panic!("document has `{key}` entry"));
    edit(&mut entry.1);
    let mut encoded = Vec::new();
    ciborium::ser::into_writer(&value, &mut encoded).expect("encode document map");
    encoded
}

#[test]
fn wal_delta_decode_rejects_invalid_name_key() {
    // The WAL delta's `name_key` field is a typed `NameKey`, so the wire
    // decode boundary is where malformed keys are rejected — nothing
    // downstream re-validates. Encode a valid bind delta, corrupt the key
    // in the CBOR map, and require the decode to fail.
    let valid = WalDelta::BindDirentry {
        delta_index: 0,
        parent_inode_id: InodeId(1),
        name_key: name_key("docs"),
        display_name: loonfs_api::DisplayName::parse("Docs").expect("valid display name"),
        child_inode_id: InodeId(2),
    };
    let mut encoded = Vec::new();
    ciborium::ser::into_writer(&valid, &mut encoded).expect("encode delta");
    let corrupted = with_cbor_document_entry(&encoded, "name_key", |value| {
        *value = ciborium::Value::from("bad/key");
    });

    ciborium::de::from_reader::<WalDelta, _>(corrupted.as_slice())
        .expect_err("invalid name key must fail wire decode");

    let round_tripped: WalDelta =
        ciborium::de::from_reader(encoded.as_slice()).expect("valid delta round-trips");
    assert_eq!(round_tripped, valid);
}

#[test]
fn wal_decode_rejects_wrong_format_version_cleanly() {
    let document = unzstd(
        &encode_wal_segment_envelope_zstd(sample_wal_payload())
            .expect("wal")
            .into_bytes(),
    );
    let wrong_version = with_cbor_document_entry(&document, "format_version", |value| {
        *value = ciborium::Value::from(7);
    });

    let err = decode_wal_segment_envelope_zstd(&rezstd(&wrong_version))
        .expect_err("wrong version must be rejected");
    assert!(
        matches!(
            err,
            EnvelopeCodecError::UnsupportedFormatVersion {
                found: 7,
                supported: 1,
                ..
            }
        ),
        "unexpected error: {err}"
    );
}

#[test]
fn wal_decode_rejects_unknown_kind_cleanly() {
    let document = unzstd(
        &encode_wal_segment_envelope_zstd(sample_wal_payload())
            .expect("wal")
            .into_bytes(),
    );
    let rekinded = with_cbor_document_entry(&document, "kind", |value| {
        *value = ciborium::Value::from("namespace_wal_index");
    });

    let err = decode_wal_segment_envelope_zstd(&rezstd(&rekinded))
        .expect_err("unknown kind must be rejected");
    assert!(
        matches!(err, EnvelopeCodecError::KindMismatch { .. }),
        "unexpected error: {err}"
    );
}

#[test]
fn wal_decode_rejects_tampered_payload_bytes_as_checksum_mismatch() {
    let document = unzstd(
        &encode_wal_segment_envelope_zstd(sample_wal_payload())
            .expect("wal")
            .into_bytes(),
    );
    let tampered = with_cbor_document_entry(&document, "payload", |value| {
        let bytes = match value {
            ciborium::Value::Bytes(bytes) => bytes,
            other => panic!("payload should be a CBOR byte string, got {other:?}"),
        };
        let last = bytes.last_mut().expect("payload is non-empty");
        *last ^= 0xff;
    });

    let err = decode_wal_segment_envelope_zstd(&rezstd(&tampered))
        .expect_err("tampered payload must be rejected");
    assert!(
        matches!(err, EnvelopeCodecError::ChecksumMismatch { .. }),
        "unexpected error: {err}"
    );
}

#[test]
fn wal_decode_rejects_unknown_payload_fields() {
    // A valid checksum does not authorize unknown payload fields.
    let envelope = sample_wal_payload();
    let document = wal_document_with_payload_edit(&envelope, with_future_field);

    let error = decode_wal_segment_envelope_zstd(&document)
        .expect_err("unknown durable fields must be rejected");
    assert!(matches!(error, EnvelopeCodecError::PayloadDecode(message)
        if message.contains("unknown field") && message.contains("field_from_the_future")));
}

#[test]
fn wal_decode_rejects_unknown_fields_inside_tombstone_deltas() {
    let envelope = wal_payload_with_deltas(vec![
        WalCommitDelta {
            semantic_op_index: 0,
            delta: WalDelta::TombstoneSubtree {
                delta_index: 0,
                root_inode_id: InodeId(9),
                deleted_direntry: DeletedDirentry {
                    parent_inode_id: InodeId(1),
                    name_key: name_key("old.txt"),
                    display_name: loonfs_api::DisplayName::parse("Old.txt")
                        .expect("valid display name"),
                },
            },
        },
        WalCommitDelta {
            semantic_op_index: 1,
            delta: WalDelta::RevokeSubtreeTombstone {
                delta_index: 1,
                root_inode_id: InodeId(9),
                target: TombstoneGeneration {
                    seq: ChangeSeq(1),
                    delta_index: 0,
                },
            },
        },
    ]);
    let document = wal_document_with_payload_edit(&envelope, |payload| {
        with_future_field(cbor_entry(commit_delta(payload, 0), "deleted_direntry"));
        with_future_field(cbor_entry(commit_delta(payload, 1), "target"));
    });

    let error = decode_wal_segment_envelope_zstd(&document)
        .expect_err("unknown durable fields must be rejected");
    assert!(matches!(error, EnvelopeCodecError::PayloadDecode(message)
        if message.contains("unknown field") && message.contains("field_from_the_future")));
}

#[test]
fn wal_decode_rejects_a_version_one_commit_without_committed_by() {
    let document = wal_document_with_payload_edit(&sample_wal_payload(), |payload| {
        cbor_map_of(payload_commit(payload))
            .retain(|(key, _)| key.as_text() != Some("committed_by"));
    });

    let error = decode_wal_segment_envelope_zstd(&document)
        .expect_err("version-one WAL commits require an actor");
    assert!(
        matches!(&error, EnvelopeCodecError::PayloadDecode(message) if message.contains("committed_by")),
        "unexpected corruption error: {error}"
    );
}

#[test]
fn control_object_decode_rejects_tampered_payload_as_checksum_mismatch() {
    let envelope = HintState {
        namespace_id: namespace_id(),
        manifest_no: ManifestNo(2),
        wal_no: WalNo(2),
    };
    let encoded =
        loonfs_api::wire::control::encode_control_state(ControlObjectKind::Hint, &envelope)
            .expect("encode control object");
    let mut document: serde_json::Value =
        serde_json::from_slice(&encoded).expect("decode document");
    document["payload"]["wal_no"] = serde_json::Value::from(999);
    let tampered = serde_json::to_vec(&document).expect("encode tampered document");

    let err = decode_control_object::<HintState>(&tampered, ControlObjectKind::Hint)
        .expect_err("tampered payload must be rejected");
    assert!(
        matches!(err, EnvelopeCodecError::ChecksumMismatch { .. }),
        "unexpected error: {err}"
    );
}

#[test]
fn namespace_manifest_decode_rejects_wrong_format_version_cleanly() {
    let encoded = encode_namespace_manifest_json(sample_manifest_payload())
        .expect("manifest")
        .into_bytes();
    let mut document: serde_json::Value =
        serde_json::from_slice(&encoded).expect("decode document");
    for version in [0, 2, 3, 7] {
        document["format_version"] = serde_json::Value::from(version);
        let wrong_version = serde_json::to_vec(&document).expect("encode document");
        let err = decode_namespace_manifest_json(&wrong_version)
            .expect_err("wrong version must be rejected");
        assert!(
            matches!(
                err,
                EnvelopeCodecError::UnsupportedFormatVersion {
                    found,
                    supported: 1,
                    ..
                } if found == version
            ),
            "unexpected error: {err}"
        );
    }
}

#[test]
fn namespace_manifest_decode_rejects_tampered_payload_as_checksum_mismatch() {
    let encoded = encode_namespace_manifest_json(sample_manifest_payload())
        .expect("manifest")
        .into_bytes();
    let mut document: serde_json::Value =
        serde_json::from_slice(&encoded).expect("decode document");
    document["payload"]["head_seq"] = serde_json::Value::from(999);
    let tampered = serde_json::to_vec(&document).expect("encode tampered document");

    let err =
        decode_namespace_manifest_json(&tampered).expect_err("tampered payload must be rejected");
    assert!(
        matches!(err, EnvelopeCodecError::ChecksumMismatch { .. }),
        "unexpected error: {err}"
    );
}

#[test]
fn namespace_manifest_decode_rejects_unknown_fields_at_every_level() {
    let payload = sample_manifest_payload();
    let encoded = encode_namespace_manifest_json(payload)
        .expect("manifest")
        .into_bytes();
    for path in [
        "",
        "/runs/0",
        "/runs/0/segments/0",
        "/runs/0/segments/0/index_block",
        "/runs/0/segments/0/filter_block",
    ] {
        let mut document: serde_json::Value = serde_json::from_slice(&encoded).expect("document");
        document["payload"]
            .pointer_mut(path)
            .expect("fixture object")["field_from_the_future"] = serde_json::Value::from(true);
        let payload = serde_json::to_string(&document["payload"]).expect("payload");
        let checksum = serde_json::to_string(&sha256_digest(payload.as_bytes())).expect("checksum");
        let future_document = format!(
            "{{\"kind\":{},\"format_version\":{},\"payload_checksum\":{},\"payload\":{}}}",
            document["kind"], document["format_version"], checksum, payload,
        );
        let error = decode_namespace_manifest_json(future_document.as_bytes())
            .expect_err("unknown durable fields must be rejected");
        assert!(
            matches!(error, EnvelopeCodecError::PayloadDecode(message)
            if message.contains("unknown field") && message.contains("field_from_the_future")),
            "path {path}"
        );
    }
}

#[test]
fn immutable_envelopes_reject_unknown_fields() {
    let mut manifest = encode_namespace_manifest_json(sample_manifest_payload())
        .expect("manifest")
        .into_bytes();
    assert_eq!(manifest.pop(), Some(b'}'));
    manifest.extend_from_slice(br#", "field_from_the_future": true}"#);
    assert!(matches!(decode_namespace_manifest_json(&manifest),
        Err(EnvelopeCodecError::EnvelopeDecode(message)) if message.contains("field_from_the_future")));

    let encoded = unzstd(
        &encode_wal_segment_envelope_zstd(sample_wal_payload())
            .expect("wal")
            .into_bytes(),
    );
    let mut document: ciborium::Value =
        ciborium::de::from_reader(encoded.as_slice()).expect("document");
    with_future_field(&mut document);
    let mut future = Vec::new();
    ciborium::ser::into_writer(&document, &mut future).expect("encode document");
    assert!(matches!(decode_wal_segment_envelope_zstd(&rezstd(&future)),
        Err(EnvelopeCodecError::EnvelopeDecode(message)) if message.contains("field_from_the_future")));
}

// ---------------------------------------------------------------------------
// Wire-name pinning: the durable delta and precondition names the format
// spec fixes ("Standard mutation operations" and "Preconditions")
// ---------------------------------------------------------------------------

#[test]
fn wal_delta_wire_tags_match_spec_names() {
    let cases = [
        (
            serde_json::to_value(WalDelta::CreateInode {
                delta_index: 0,
                inode_id: InodeId(1),
                inode_kind: InodeKind::Directory,
            }),
            "create_inode",
        ),
        (
            serde_json::to_value(WalDelta::BindDirentry {
                delta_index: 0,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("a").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("a").expect("valid display name"),
                child_inode_id: InodeId(2),
            }),
            "bind_direntry",
        ),
        (
            serde_json::to_value(WalDelta::UnbindDirentry {
                delta_index: 0,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("a").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("a").expect("valid display name"),
                child_inode_id: InodeId(2),
                bind_seq: ChangeSeq(1),
                bind_delta_index: 0,
            }),
            "unbind_direntry",
        ),
        (
            serde_json::to_value(WalDelta::AppendFileRevision {
                delta_index: 0,
                inode_id: InodeId(2),
                revision_no: RevisionNo(1),
                content_ref: sample_content_ref(),
            }),
            "append_file_revision",
        ),
        (
            serde_json::to_value(WalDelta::TombstoneSubtree {
                delta_index: 0,
                root_inode_id: InodeId(2),
                deleted_direntry: DeletedDirentry {
                    parent_inode_id: InodeId(1),
                    name_key: name_key("a"),
                    display_name: loonfs_api::DisplayName::parse("a").expect("valid display name"),
                },
            }),
            "tombstone_subtree",
        ),
        (
            serde_json::to_value(WalDelta::RevokeSubtreeTombstone {
                delta_index: 0,
                root_inode_id: InodeId(2),
                target: TombstoneGeneration {
                    seq: ChangeSeq(1),
                    delta_index: 1,
                },
            }),
            "revoke_subtree_tombstone",
        ),
        (
            serde_json::to_value(WalDelta::AppendAttributesRevision {
                delta_index: 0,
                inode_id: InodeId(2),
                attributes_revision_no: AttributeRevisionNo(1),
                attributes: Attributes::default(),
            }),
            "append_attributes_revision",
        ),
    ];
    for (value, expected_tag) in cases {
        let value = value.expect("serialize delta");
        assert_eq!(value["kind"], expected_tag, "in {value}");
    }
}

// ---------------------------------------------------------------------------
// Metadata segment blocks
// ---------------------------------------------------------------------------

/// A delete by path: the tombstone records the binding it removed.
fn sample_tombstone_set_row() -> MetadataRow {
    MetadataRow::Tombstone(loonfs_api::wire::manifest::SubtreeTombstoneRecord {
        root_inode_id: InodeId(5),
        generation: TombstoneGeneration {
            seq: ChangeSeq(8),
            delta_index: 0,
        },
        commit_id: commit_id(),
        action: TombstoneRowAction::Set {
            deleted_direntry: DeletedDirentry {
                parent_inode_id: InodeId(1),
                name_key: name_key("docs-archive"),
                display_name: loonfs_api::DisplayName::parse("Docs-Archive")
                    .expect("valid display name"),
            },
        },
        deleted_at_ms: 4_000,
        deleted_by: actor(),
    })
}

/// The undelete that cancels it, naming the exact generation it revokes.
fn sample_tombstone_revoke_row() -> MetadataRow {
    MetadataRow::Tombstone(loonfs_api::wire::manifest::SubtreeTombstoneRecord {
        root_inode_id: InodeId(5),
        generation: TombstoneGeneration {
            seq: ChangeSeq(9),
            delta_index: 0,
        },
        commit_id: commit_id(),
        action: TombstoneRowAction::Revoke {
            target: TombstoneGeneration {
                seq: ChangeSeq(8),
                delta_index: 0,
            },
        },
        deleted_at_ms: 4_100,
        deleted_by: actor(),
    })
}

/// The active-deletion row the materializer derives from the set above. It
/// carries the deletion's stamp and the binding the trash entry renders, both
/// copied from the tombstone event.
fn sample_active_deletion_listed_row() -> MetadataRow {
    MetadataRow::ActiveDeletion(loonfs_api::wire::manifest::ActiveDeletionRecord {
        root_inode_id: InodeId(5),
        deletion_seq: ChangeSeq(8),
        action: ActiveDeletionRowAction::Listed {
            deleted_at_ms: 4_000,
            deleted_by: actor(),
            deleted_direntry: DeletedDirentry {
                parent_inode_id: InodeId(1),
                name_key: name_key("docs-archive"),
                display_name: loonfs_api::DisplayName::parse("Docs-Archive")
                    .expect("valid display name"),
            },
        },
    })
}

/// The active-deletion row the materializer derives from the revoke above. It
/// repeats the deletion's sequence rather than the undelete's, so the two rows
/// share a key prefix, and its rank sorts it ahead of the row it removes.
fn sample_active_deletion_removed_row() -> MetadataRow {
    MetadataRow::ActiveDeletion(loonfs_api::wire::manifest::ActiveDeletionRecord {
        root_inode_id: InodeId(5),
        deletion_seq: ChangeSeq(8),
        action: ActiveDeletionRowAction::Removed {
            revocation_seq: ChangeSeq(9),
        },
    })
}

/// An attribute revision that cleared the map. The empty map has an encoding
/// of its own, so the sample carries a row that states it.
fn sample_cleared_attributes_row() -> MetadataRow {
    MetadataRow::AttributesRevision(loonfs_api::wire::manifest::AttributesRevisionRecord {
        inode_id: InodeId(2),
        attributes_revision_no: AttributeRevisionNo(3),
        committed_seq: ChangeSeq(7),
        commit_id: commit_id(),
        delta_index: 1,
        updated_by: actor(),
        updated_at_ms: 7_000,
        attributes: Attributes::default(),
    })
}

/// An attribute revision that states a populated map.
fn sample_populated_attributes_row() -> MetadataRow {
    MetadataRow::AttributesRevision(loonfs_api::wire::manifest::AttributesRevisionRecord {
        inode_id: InodeId(5),
        attributes_revision_no: AttributeRevisionNo(2),
        committed_seq: ChangeSeq(5),
        commit_id: commit_id(),
        delta_index: 0,
        updated_by: actor(),
        updated_at_ms: 5_000,
        attributes: sample_attributes(),
    })
}

fn sample_commit_receipt_row() -> MetadataRow {
    MetadataRow::CommitReceipt(loonfs_api::wire::manifest::CommitReceiptRecord {
        commit_id: commit_id(),
        committed_by: actor(),
        semantic_commit_fingerprint: serde_json::from_str(r#""fp:golden""#).expect("fingerprint"),
        committed_seq: ChangeSeq(9),
        committed_at_ms: 9_000,
        message: None,
    })
}

fn sample_inode_rows() -> [MetadataRow; 2] {
    [
        MetadataRow::Inode(loonfs_api::wire::manifest::InodeRecord {
            inode_id: InodeId(1),
            inode_kind: InodeKind::Directory,
            created_seq: ChangeSeq(1),
            commit_id: commit_id(),
            created_by: actor(),
            created_at_ms: 1_000,
        }),
        MetadataRow::Inode(loonfs_api::wire::manifest::InodeRecord {
            inode_id: InodeId(2),
            inode_kind: InodeKind::File,
            created_seq: ChangeSeq(3),
            commit_id: commit_id(),
            created_by: actor(),
            created_at_ms: 3_000,
        }),
    ]
}

fn sample_revision_rows() -> [MetadataRow; 2] {
    [
        MetadataRow::FileRevision(loonfs_api::wire::manifest::RevisionRecord {
            inode_id: InodeId(2),
            revision_no: RevisionNo(2),
            committed_seq: ChangeSeq(4),
            commit_id: commit_id(),
            committed_at_ms: 4_000,
            committed_by: actor(),
            delta_index: 0,
            content_ref: sample_crc_content_ref(),
        }),
        MetadataRow::FileRevision(loonfs_api::wire::manifest::RevisionRecord {
            inode_id: InodeId(2),
            revision_no: RevisionNo(1),
            committed_seq: ChangeSeq(3),
            commit_id: commit_id(),
            committed_at_ms: 3_000,
            committed_by: actor(),
            delta_index: 0,
            content_ref: sample_content_ref(),
        }),
    ]
}

fn sample_segment_blocks() -> loonfs_api::wire::sst_blocks::BuiltSegmentBlocks {
    use loonfs_api::wire::sst_blocks::SegmentBlocksBuilder;
    // A tiny target block size forces several data blocks, so the fixture
    // pins block splitting, restart points, and the index shape at once.
    let mut builder = SegmentBlocksBuilder::new(
        std::num::NonZeroUsize::new(256).expect("target block size should be non-zero"),
    );
    let mut rows = vec![
        // `active-deletion-` sorts ahead of every other family prefix, so
        // these two rows open the first data block the fixture below pins.
        // The pair covers both actions: the removal that an undelete writes,
        // and the listing it removes.
        sample_active_deletion_removed_row(),
        sample_active_deletion_listed_row(),
        // `attributes-` sorts after `active-deletion-` and ahead of
        // `commit-receipt-`, so both attribute rows share the block that
        // follows. The cleared state goes on the lower inode so it is the
        // smaller row that opens the family, which keeps both rows inside one
        // block; pinning the empty map's encoding is the point of carrying
        // two.
        sample_cleared_attributes_row(),
        sample_populated_attributes_row(),
        sample_commit_receipt_row(),
        MetadataRow::DirentryBind(loonfs_api::wire::manifest::DirentryBindRecord {
            parent_inode_id: InodeId(1),
            name_key: name_key("docs"),
            display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
            child_inode_id: InodeId(2),
            bind_seq: ChangeSeq(3),
            bind_delta_index: 0,
        }),
        MetadataRow::DirentryBind(loonfs_api::wire::manifest::DirentryBindRecord {
            parent_inode_id: InodeId(1),
            name_key: name_key("docs-archive"),
            display_name: loonfs_api::DisplayName::parse("docs-archive")
                .expect("valid display name"),
            child_inode_id: InodeId(5),
            bind_seq: ChangeSeq(6),
            bind_delta_index: 0,
        }),
        MetadataRow::DirentryUnbind(loonfs_api::wire::manifest::DirentryUnbindRecord {
            parent_inode_id: InodeId(1),
            name_key: name_key("docs-archive"),
            display_name: loonfs_api::DisplayName::parse("Docs-Archive")
                .expect("valid display name"),
            child_inode_id: InodeId(5),
            bind_seq: ChangeSeq(6),
            bind_delta_index: 0,
            unbind_seq: ChangeSeq(8),
            unbind_delta_index: 0,
        }),
    ];
    rows.extend(sample_inode_rows());
    rows.extend(sample_revision_rows());
    rows.extend([sample_tombstone_set_row(), sample_tombstone_revoke_row()]);
    for row in &rows {
        let key = row.row_key();
        builder.push(&key, &key, row).expect("push sample row");
    }
    builder.finish().expect("finish sample segment")
}

fn segment_section<'a>(
    bytes: &'a [u8],
    handle: &loonfs_api::wire::sst_blocks::BlockHandle,
) -> &'a [u8] {
    &bytes[handle.offset as usize..handle.offset as usize + handle.stored_len as usize]
}

fn sample_segment_index(
    built: &loonfs_api::wire::sst_blocks::BuiltSegmentBlocks,
) -> Vec<loonfs_api::wire::sst_blocks::SegmentIndexEntry> {
    loonfs_api::wire::sst_blocks::decode_index_block(
        segment_section(&built.bytes, &built.index),
        &built.index,
    )
    .expect("decode index")
}

/// Returns the index position of the block holding the first row of the family
/// `prefix` names. A guard locates its family this way rather than naming a
/// block position, because a family that starts sorting earlier pushes the
/// families after it into other blocks. A guard that named a position would
/// then read a block its family never reaches and assert nothing.
fn family_block_position(
    built: &loonfs_api::wire::sst_blocks::BuiltSegmentBlocks,
    index: &[loonfs_api::wire::sst_blocks::SegmentIndexEntry],
    prefix: &str,
) -> usize {
    use loonfs_api::wire::sst_blocks::decode_data_block;
    for (position, entry) in index.iter().enumerate() {
        let block = decode_data_block(segment_section(&built.bytes, &entry.block), &entry.block)
            .expect("decode data block");
        if block.row_keys.iter().any(|key| key.starts_with(prefix)) {
            return position;
        }
    }
    panic!("the sample carries no row under `{prefix}`");
}

/// Counts the rows one block holds for the family `prefix` names, so a guard
/// can state that every row of the family landed in the block a fixture pins.
fn rows_under_prefix(
    block: &loonfs_api::wire::sst_blocks::DecodedDataBlock,
    prefix: &str,
) -> usize {
    block
        .row_keys
        .iter()
        .filter(|key| key.starts_with(prefix))
        .count()
}

/// Reads back the block a fixture pins, so a decode test can state the rows it
/// expects to find there.
fn decode_golden_data_block(name: &str) -> loonfs_api::wire::sst_blocks::DecodedDataBlock {
    use loonfs_api::wire::sst_blocks::{decode_data_block, BlockHandle};
    let payload = read_golden(name);
    let stored = rezstd(&payload);
    let handle = BlockHandle {
        offset: 0,
        stored_len: stored.len() as u32,
        decoded_len: payload.len() as u32,
        crc32c: crc32c::crc32c(&stored),
    };
    decode_data_block(&stored, &handle).expect("decode golden data block")
}

fn assert_rows_match_single_block_golden(name: &str, rows: &[MetadataRow]) {
    use loonfs_api::wire::sst_blocks::{decode_data_block, SegmentBlocksBuilder};

    let mut builder = SegmentBlocksBuilder::new(
        std::num::NonZeroUsize::new(4096).expect("target block size should be non-zero"),
    );
    for row in rows {
        let key = row.row_key();
        builder.push(&key, &key, row).expect("push golden row");
    }
    let built = builder.finish().expect("finish golden segment");
    let index = sample_segment_index(&built);
    assert_eq!(index.len(), 1, "the row fixture should be one block");
    let entry = &index[0];
    let block = decode_data_block(segment_section(&built.bytes, &entry.block), &entry.block)
        .expect("decode golden block");
    assert_eq!(block.rows, rows);
    assert_matches_golden(name, &unzstd(segment_section(&built.bytes, &entry.block)));
}

#[test]
fn sst_block_data_payload_matches_golden_bytes() {
    let built = sample_segment_blocks();
    let index = sample_segment_index(&built);
    assert!(index.len() > 1, "sample should span several blocks");
    // Compare the decompressed block payload: zstd frames may differ across
    // zstd versions, the entry encoding (which the format defines) may not.
    assert_matches_golden(
        "sst_block_data.v1.bin",
        &unzstd(segment_section(&built.bytes, &index[0].block)),
    );
}

#[test]
fn sst_block_data_first_block_covers_the_active_deletion_prefix() {
    let built = sample_segment_blocks();
    let index = sample_segment_index(&built);
    let position = family_block_position(&built, &index, "active-deletion-");
    assert_eq!(position, 0, "the active-deletion family opens the segment");
    let block = loonfs_api::wire::sst_blocks::decode_data_block(
        segment_section(&built.bytes, &index[0].block),
        &index[0].block,
    )
    .expect("decode first block");
    assert_eq!(
        rows_under_prefix(&block, "active-deletion-"),
        2,
        "both active-deletion rows belong to the pinned first block: {:?}",
        block.row_keys
    );
}

#[test]
fn sst_block_data_golden_decodes_to_sample_rows() {
    let block = decode_golden_data_block("sst_block_data.v1.bin");
    assert_eq!(
        block.rows,
        [
            sample_active_deletion_removed_row(),
            sample_active_deletion_listed_row(),
        ],
    );
    assert_eq!(block.row_keys[0], block.rows[0].row_key());
}

#[test]
fn sst_block_data_inode_rows_match_golden_bytes() {
    assert_rows_match_single_block_golden("sst_block_data_inodes.v1.bin", &sample_inode_rows());
}

#[test]
fn sst_block_data_inode_golden_decodes_to_sample_rows() {
    let block = decode_golden_data_block("sst_block_data_inodes.v1.bin");
    assert_eq!(block.rows, sample_inode_rows());
}

#[test]
fn sst_block_data_revision_rows_match_golden_bytes() {
    assert_rows_match_single_block_golden(
        "sst_block_data_revisions.v1.bin",
        &sample_revision_rows(),
    );
}

#[test]
fn sst_block_data_revision_golden_decodes_to_sample_rows() {
    let block = decode_golden_data_block("sst_block_data_revisions.v1.bin");
    assert_eq!(block.rows, sample_revision_rows());
}

#[test]
fn sst_block_data_attribute_rows_match_golden_bytes() {
    assert_rows_match_single_block_golden(
        "sst_block_data_attributes.v1.bin",
        &[
            sample_cleared_attributes_row(),
            sample_populated_attributes_row(),
        ],
    );
}

#[test]
fn sst_block_data_attribute_golden_decodes_to_sample_rows() {
    let block = decode_golden_data_block("sst_block_data_attributes.v1.bin");
    assert_eq!(
        block.rows,
        [
            sample_cleared_attributes_row(),
            sample_populated_attributes_row(),
        ],
    );
}

#[test]
fn sst_block_data_commit_receipt_rows_match_golden_bytes() {
    assert_rows_match_single_block_golden(
        "sst_block_data_commit_receipts.v1.bin",
        &[sample_commit_receipt_row()],
    );
}

#[test]
fn sst_block_data_commit_receipt_golden_decodes_to_sample_row() {
    let block = decode_golden_data_block("sst_block_data_commit_receipts.v1.bin");
    assert_eq!(block.rows, [sample_commit_receipt_row()]);
}

#[test]
fn sst_block_data_tombstone_rows_match_golden_bytes() {
    assert_rows_match_single_block_golden(
        "sst_block_data_tombstones.v1.bin",
        &[sample_tombstone_set_row(), sample_tombstone_revoke_row()],
    );
}

#[test]
fn sst_block_data_tombstone_golden_decodes_to_sample_rows() {
    let block = decode_golden_data_block("sst_block_data_tombstones.v1.bin");
    assert_eq!(
        block.rows,
        [sample_tombstone_set_row(), sample_tombstone_revoke_row()],
    );
}

// ---------------------------------------------------------------------------
// Tombstone and active-deletion rows: the deleted binding is one value, or it
// is absent
// ---------------------------------------------------------------------------

/// Re-encodes a row as the CBOR map another writer would have produced, so a
/// test can rewrite it entry by entry.
fn row_cbor(row: &MetadataRow) -> ciborium::Value {
    let mut encoded = Vec::new();
    ciborium::ser::into_writer(row, &mut encoded).expect("encode row");
    ciborium::de::from_reader(encoded.as_slice()).expect("decode row map")
}

fn cbor_entry<'a>(value: &'a mut ciborium::Value, key: &str) -> &'a mut ciborium::Value {
    &mut value
        .as_map_mut()
        .unwrap_or_else(|| panic!("the value holding `{key}` is a map"))
        .iter_mut()
        .find(|(entry_key, _)| entry_key.as_text() == Some(key))
        .unwrap_or_else(|| panic!("map has `{key}` entry"))
        .1
}

fn cbor_map_of(value: &mut ciborium::Value) -> &mut Vec<(ciborium::Value, ciborium::Value)> {
    value.as_map_mut().expect("value is a map")
}

/// Returns the refusal an edited row produces, so the caller can pin which
/// rule fired rather than only that something did.
fn assert_row_is_corrupt(row: &ciborium::Value, why: &str) -> String {
    let mut encoded = Vec::new();
    ciborium::ser::into_writer(row, &mut encoded).expect("encode edited row");
    match ciborium::de::from_reader::<MetadataRow, _>(encoded.as_slice()) {
        Ok(decoded) => panic!("{why}, but the row decoded as {decoded:?}"),
        Err(error) => error.to_string(),
    }
}

/// Decodes a row after applying an edit to its CBOR representation.
#[test]
fn tombstone_rows_reject_a_partial_deleted_direntry() {
    for missing in ["parent_inode_id", "name_key", "display_name"] {
        let mut row = row_cbor(&sample_tombstone_set_row());
        let direntry = cbor_entry(cbor_entry(&mut row, "action"), "deleted_direntry");
        cbor_map_of(direntry).retain(|(key, _)| key.as_text() != Some(missing));
        let refusal = assert_row_is_corrupt(&row, "two thirds of a binding is not a binding");
        assert!(
            refusal.contains(&format!("missing field `{missing}`")),
            "unexpected refusal: {refusal}"
        );
    }
}

#[test]
fn tombstone_rows_reject_unknown_fields_at_every_level() {
    let expected = sample_tombstone_set_row();
    let paths: [&[&str]; 3] = [
        &["generation"],
        &["action"],
        &["action", "deleted_direntry"],
    ];

    for path in paths {
        let mut row = row_cbor(&expected);
        let mut target = &mut row;
        for key in path {
            target = cbor_entry(target, key);
        }
        with_future_field(target);
        let refusal = assert_row_is_corrupt(&row, "unknown durable fields must be rejected");
        assert!(
            refusal.contains("field_from_the_future"),
            "{path:?}: {refusal}"
        );
    }
}

#[test]
fn tombstone_revoke_rows_reject_a_deleted_direntry() {
    let expected = sample_tombstone_revoke_row();
    let mut row = row_cbor(&expected);
    cbor_map_of(cbor_entry(&mut row, "action")).push((
        ciborium::Value::from("deleted_direntry"),
        sample_deleted_direntry_cbor(),
    ));

    let refusal = assert_row_is_corrupt(&row, "a revoke carries no deleted direntry");
    assert!(refusal.contains("deleted_direntry"), "{refusal}");
}

#[test]
fn tombstone_rows_reject_flat_binding_fields() {
    let row = row_cbor(&sample_tombstone_set_row());
    assert_row_is_corrupt(
        &with_flat_binding(with_flat_generation(row.clone())),
        "flat binding fields are not a tombstone row",
    );

    // Each half of that encoding on its own, over a row that is otherwise
    // current: neither is a spelling this row accepts.
    let refusal = assert_row_is_corrupt(
        &with_flat_generation(row.clone()),
        "a tombstone states its generation as one value",
    );
    assert!(
        refusal.contains("unknown field `tombstone_seq`"),
        "unexpected refusal: {refusal}"
    );
    let refusal = assert_row_is_corrupt(
        &with_flat_binding(row),
        "a `set` states its binding, even when it has none",
    );
    assert!(
        refusal.contains("missing field `deleted_direntry`")
            || refusal.contains("unknown field `parent_inode_id`"),
        "unexpected refusal: {refusal}"
    );
}

#[test]
fn active_deletion_rows_reject_a_partial_or_absent_deleted_direntry() {
    for missing in ["parent_inode_id", "name_key", "display_name"] {
        let mut row = row_cbor(&sample_active_deletion_listed_row());
        let direntry = cbor_entry(cbor_entry(&mut row, "action"), "deleted_direntry");
        cbor_map_of(direntry).retain(|(key, _)| key.as_text() != Some(missing));
        let refusal = assert_row_is_corrupt(&row, "two thirds of a binding is not a binding");
        assert!(
            refusal.contains(&format!("missing field `{missing}`")),
            "unexpected refusal: {refusal}"
        );
    }

    let mut row = row_cbor(&sample_active_deletion_listed_row());
    cbor_map_of(cbor_entry(&mut row, "action"))
        .retain(|(key, _)| key.as_text() != Some("deleted_direntry"));
    let refusal =
        assert_row_is_corrupt(&row, "a `listed` states its binding, even when it has none");
    assert!(
        refusal.contains("missing field `deleted_direntry`")
            || refusal.contains("unknown field `parent_inode_id`"),
        "unexpected refusal: {refusal}"
    );
}

#[test]
fn provenance_rows_reject_every_missing_required_field() {
    let cases = [
        (
            MetadataRow::Inode(loonfs_api::wire::manifest::InodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(3),
                commit_id: commit_id(),
                created_by: actor(),
                created_at_ms: 3_000,
            }),
            &["commit_id", "created_by", "created_at_ms"][..],
        ),
        (
            MetadataRow::FileRevision(loonfs_api::wire::manifest::RevisionRecord {
                inode_id: InodeId(2),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(3),
                commit_id: commit_id(),
                committed_at_ms: 3_000,
                committed_by: actor(),
                delta_index: 0,
                content_ref: sample_content_ref(),
            }),
            &["commit_id", "committed_by"][..],
        ),
        (sample_tombstone_set_row(), &["commit_id", "deleted_by"][..]),
        (sample_active_deletion_listed_row(), &["deleted_by"][..]),
        (
            sample_populated_attributes_row(),
            &["commit_id", "updated_by", "updated_at_ms"][..],
        ),
        (
            MetadataRow::CommitReceipt(loonfs_api::wire::manifest::CommitReceiptRecord {
                commit_id: commit_id(),
                committed_by: actor(),
                semantic_commit_fingerprint: serde_json::from_str(r#""v3:sha256:receipt""#)
                    .expect("fingerprint"),
                committed_seq: ChangeSeq(9),
                committed_at_ms: 9_000,
                message: None,
            }),
            &["committed_by"][..],
        ),
    ];

    for (row, required_fields) in cases {
        // The active-deletion row states its attribution inside `action`;
        // every other row states it at the top level.
        let nested_in_action = matches!(
            row,
            MetadataRow::ActiveDeletion(loonfs_api::wire::manifest::ActiveDeletionRecord { .. })
        );
        for required_field in required_fields {
            let mut encoded = row_cbor(&row);
            let map = if nested_in_action {
                cbor_map_of(cbor_entry(&mut encoded, "action"))
            } else {
                cbor_map_of(&mut encoded)
            };
            map.retain(|(key, _)| key.as_text() != Some(required_field));
            let refusal =
                assert_row_is_corrupt(&encoded, "version-one attributed row fields are required");
            assert!(
                refusal.contains(&format!("missing field `{required_field}`")),
                "unexpected refusal for `{required_field}`: {refusal}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Attribute rows: the retired tagged value is not a value
// ---------------------------------------------------------------------------

#[test]
fn attribute_rows_reject_the_retired_tagged_value_shape() {
    let mut row = row_cbor(&MetadataRow::AttributesRevision(
        loonfs_api::wire::manifest::AttributesRevisionRecord {
            inode_id: InodeId(2),
            attributes_revision_no: AttributeRevisionNo(1),
            committed_seq: ChangeSeq(5),
            commit_id: commit_id(),
            delta_index: 0,
            updated_by: actor(),
            updated_at_ms: 5_000,
            attributes: sample_attributes(),
        },
    ));
    let owner = cbor_entry(cbor_entry(&mut row, "attributes"), "owner");
    *owner = ciborium::Value::Map(vec![
        (
            ciborium::Value::from("kind"),
            ciborium::Value::from("string"),
        ),
        (ciborium::Value::from("value"), ciborium::Value::from("ada")),
    ]);

    let refusal = assert_row_is_corrupt(&row, "an attribute value is one string");
    assert!(
        refusal.contains("string") || refusal.contains("map"),
        "unexpected refusal: {refusal}"
    );
}

#[test]
fn attribute_rows_reject_a_map_over_its_limits() {
    let mut row = row_cbor(&MetadataRow::AttributesRevision(
        loonfs_api::wire::manifest::AttributesRevisionRecord {
            inode_id: InodeId(2),
            attributes_revision_no: AttributeRevisionNo(1),
            committed_seq: ChangeSeq(5),
            commit_id: commit_id(),
            delta_index: 0,
            updated_by: actor(),
            updated_at_ms: 5_000,
            attributes: sample_attributes(),
        },
    ));
    let owner = cbor_entry(cbor_entry(&mut row, "attributes"), "owner");
    *owner = ciborium::Value::from("v".repeat(loonfs_api::MAX_ATTRIBUTE_VALUE_BYTES + 1));

    assert_row_is_corrupt(&row, "an oversized value is not a value this format stores");
}

/// The set row's binding, as the CBOR map another writer would have written.
fn sample_deleted_direntry_cbor() -> ciborium::Value {
    let mut set = row_cbor(&sample_tombstone_set_row());
    cbor_entry(cbor_entry(&mut set, "action"), "deleted_direntry").clone()
}

/// Moves generation fields out of their required nested object.
fn with_flat_generation(mut row: ciborium::Value) -> ciborium::Value {
    let mut generation = cbor_entry(&mut row, "generation").clone();
    let seq = cbor_entry(&mut generation, "seq").clone();
    let delta_index = cbor_entry(&mut generation, "delta_index").clone();
    let entries = cbor_map_of(&mut row);
    entries.retain(|(key, _)| key.as_text() != Some("generation"));
    entries.push((ciborium::Value::from("tombstone_seq"), seq));
    entries.push((ciborium::Value::from("tombstone_delta_index"), delta_index));
    row
}

/// Moves binding fields out of `set` to test rejection of a malformed action.
fn with_flat_binding(mut row: ciborium::Value) -> ciborium::Value {
    *cbor_entry(&mut row, "action") = ciborium::Value::Map(vec![(
        ciborium::Value::from("kind"),
        ciborium::Value::from("set"),
    )]);
    cbor_map_of(&mut row).append(cbor_map_of(&mut sample_deleted_direntry_cbor()));
    row
}

#[test]
fn sst_block_filter_matches_golden_bytes_and_answers() {
    use loonfs_api::wire::sst_blocks::decode_filter_block;
    let built = sample_segment_blocks();
    // The filter section is stored raw, so its bytes are pinned directly.
    let stored = segment_section(&built.bytes, &built.filter);
    assert_matches_golden("sst_block_filter.v1.bin", stored);
    let filter = decode_filter_block(stored, &built.filter).expect("decode filter");
    assert!(filter.may_contain(
        &MetadataRow::Inode(loonfs_api::wire::manifest::InodeRecord {
            inode_id: InodeId(1),
            inode_kind: InodeKind::Directory,
            created_seq: ChangeSeq(1),
            commit_id: commit_id(),
            created_by: actor(),
            created_at_ms: 1_000,
        })
        .row_key()
    ));
    assert!(!filter.may_contain("inode-99999999999999999999"));
}

#[test]
fn sst_block_index_entry_schema_matches_golden_bytes() {
    use loonfs_api::wire::sst_blocks::{BlockHandle, SegmentIndexEntry};
    // Fixed handle values: this fixture pins the index entry schema (field
    // names, order, integer widths) without coupling to zstd output.
    let entries = vec![SegmentIndexEntry {
        last_row_key: "inode-00000000000000000042".to_owned(),
        block: BlockHandle {
            offset: 7,
            stored_len: 512,
            decoded_len: 4096,
            crc32c: 0xdead_beef,
        },
    }];
    let mut encoded = Vec::new();
    ciborium::ser::into_writer(&entries, &mut encoded).expect("encode index entries");
    assert_matches_golden("sst_block_index_entry.v1.cbor", &encoded);
    let decoded: Vec<SegmentIndexEntry> =
        ciborium::de::from_reader(encoded.as_slice()).expect("decode index entries");
    assert_eq!(decoded, entries);
}

#[test]
fn every_metadata_row_rejects_unknown_fields() {
    use loonfs_api::wire::sst_blocks::decode_data_block;
    let built = sample_segment_blocks();
    for entry in sample_segment_index(&built) {
        let block = decode_data_block(segment_section(&built.bytes, &entry.block), &entry.block)
            .expect("decode fixture block");
        for row in block.rows {
            let mut encoded = row_cbor(&row);
            with_future_field(&mut encoded);
            let refusal = assert_row_is_corrupt(&encoded, "every durable row is strict");
            assert!(
                refusal.contains("field_from_the_future"),
                "{row:?}: {refusal}"
            );
        }
    }
}

fn sample_content_publication_row() -> MetadataRow {
    MetadataRow::ContentPublication(loonfs_api::wire::manifest::ContentPublicationRecord {
        content_id: sample_content_ref().content_id,
        committed_seq: ChangeSeq(2),
        delta_index: 3,
    })
}

#[test]
fn content_publication_rows_match_golden_bytes_and_lookup_grammar() {
    let row = sample_content_publication_row();
    let key = format!(
        "content-publication-{}-00000000000000000002",
        sample_content_ref().content_id
    );
    assert_eq!(row.row_key(), key);
    assert_eq!(
        row.filter_key_for_family(MetadataRowFamily::ContentPublications),
        format!("content-publication-{}", sample_content_ref().content_id)
    );
    assert_rows_match_single_block_golden(
        "sst_block_data_content_publications.v1.bin",
        std::slice::from_ref(&row),
    );
    assert_eq!(
        decode_golden_data_block("sst_block_data_content_publications.v1.bin").rows,
        [row]
    );
}

#[test]
fn commit_assertion_wire_shapes_match_golden() {
    use loonfs_api::{
        AbsolutePath, CommitAssertion, CommitRequest, ErrorDetails, FilesystemOperation,
    };

    let request = CommitRequest::single(
        CommitId::parse("guarded").expect("commit id"),
        actor(),
        None,
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse("/docs").expect("path"),
            parents: false,
        },
    );
    let guarded = request.clone().assertions(vec![
        CommitAssertion::NamespaceHead {
            expected_head_seq: ChangeSeq(42),
        },
        CommitAssertion::FileRevision {
            inode_id: InodeId(42),
            expected_revision_no: RevisionNo(3),
        },
        CommitAssertion::Binding {
            path: AbsolutePath::parse("/docs/input").expect("path"),
            expected_inode_id: Some(InodeId(42)),
            expected_binding_generation: Some(
                loonfs_api::BindingGeneration::parse("aaaa").expect("generation"),
            ),
        },
        CommitAssertion::Attributes {
            inode_id: InodeId(42),
            expected_attributes_revision_no: loonfs_api::AttributeRevisionNo(2),
        },
    ]);
    let details = ErrorDetails {
        assertion_index: Some(0),
        expected_head_seq: Some(ChangeSeq(42)),
        actual_head_seq: Some(ChangeSeq(43)),
        ..ErrorDetails::default()
    };
    let bytes = serde_json::to_vec_pretty(&(request, &guarded, details)).expect("wire shapes");
    assert_matches_golden("commit_assertions.v0.json", &bytes);
    let (_, decoded, _): (CommitRequest, CommitRequest, ErrorDetails) =
        serde_json::from_slice(&bytes).expect("decode wire shapes");
    assert_eq!(decoded, guarded);
}

#[test]
fn name_folding_matches_the_fixed_unicode_corpus() {
    let display_names = [
        "Cafe\u{301}.TXT",
        "CAFÉ.txt",
        "Straße",
        "STRASSE",
        "ẞ",
        "ﬀ",
        "ﬃ",
        "FFI",
        "Σ",
        "σ",
        "ς",
        "ΟΣ",
        "ΐ",
        "Ϊ\u{301}",
        "I",
        "i",
        "İ",
        "i\u{307}",
        "ı",
        "Kelvin",
        "Ångström",
        "A\u{30a}ngstro\u{308}m",
        "МОСКВА",
        "Αθήνα",
        "東京",
        "العربية",
        "שלום",
        "नमस्ते",
        "\u{1100}\u{1161}",
        "가",
        "𐐀",
        "Ა",
        "ა",
        "Ꞹ",
        "ꞹ",
        "Ɤ",
        "ɤ",
        "Ꭰ",
        "ꭰ",
        "readme-123.txt",
        "123_+-",
    ];
    let corpus: Vec<_> = display_names
        .into_iter()
        .map(|display_name| {
            serde_json::json!({
                "display_name": display_name,
                "name_key": loonfs_api::name_key_for_display_name(display_name),
            })
        })
        .collect();
    let mut bytes = serde_json::to_vec_pretty(&corpus).expect("encode folding corpus");
    bytes.push(b'\n');
    assert_matches_golden("name_folding.v1.json", &bytes);
}

#[test]
fn namespace_manifest_lifecycle_variants_match_golden_bytes() {
    let mut retired = sample_deleted_manifest();
    retired.status = NamespaceStatus::Deleted {
        reclaim_after_ms: Some(2_000_000),
    };
    for (name, payload) in [
        (
            "namespace_manifest.deleted.v1.json",
            sample_deleted_manifest(),
        ),
        ("namespace_manifest.retired.v1.json", retired),
        ("namespace_manifest.fork.v1.json", sample_fork_manifest()),
    ] {
        let encoded = encode_namespace_manifest_json(payload.clone())
            .expect("manifest")
            .into_bytes();
        assert_matches_golden(name, &encoded);
        assert_eq!(
            decode_namespace_manifest_json(&encoded)
                .expect("decode manifest")
                .into_payload(),
            payload
        );
    }
}
