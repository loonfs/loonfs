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
//! - Additive payload fields in immutable families must keep decoding
//!   (`*_tolerates_additive_*`), at every level of nesting. Mutable control
//!   objects reject unknown fields so an older reader cannot erase them
//!   during a guarded rewrite. `ContentRef`, `Checksum`, and `ActorRef` are
//!   closed shapes: they reject unknown fields everywhere, because the same
//!   types decode HTTP request bodies.

use loonfs_api::wire::control::{
    decode_control_object, encode_control_object, CheckpointOwner, CheckpointRecordState,
    CheckpointStatus, CompactionLeaseStatus, ControlObjectEnvelope, ControlObjectKind, ForkBasis,
    HeadState, ManifestRef, MetadataCompactionLeaseState, MetadataRootState, NamespaceStatus,
    ProxiedStaging, UploadSessionMode, UploadSessionRecordStatus, UploadSessionState,
    WalFloorState, WalSegmentPointer, WriterBlock,
};
use loonfs_api::wire::envelope::EnvelopeCodecError;
use loonfs_api::wire::manifest::{
    decode_namespace_manifest_json, encode_namespace_manifest_json, ActiveDeletionRowAction,
    DeletedDirentry, MetadataRow, MetadataRowFamily, MetadataSegmentRef, NamespaceManifestEnvelope,
    NamespaceManifestPayload, TombstoneGeneration, TombstoneRowAction,
};
use loonfs_api::wire::wal::{
    decode_wal_segment_envelope_zstd, encode_wal_segment_envelope_zstd, WalCommitDelta,
    WalCommitPayload, WalDelta, WalSegmentEnvelope, WalSegmentPayload,
};
use loonfs_api::{
    sha256_digest, ActorId, ActorRef, AttributeKey, AttributeRevisionNo, AttributeValue,
    Attributes, ChangeSeq, CheckpointId, Checksum, ChecksumAlgorithm, CommitId, ContentId,
    ContentRef, ContentRefKind, ContentStoreId, InodeId, InodeKind, ManifestNo, ManifestObjectId,
    MetadataCompactionId, MetadataSegmentId, NameKey, NamespaceId, RevisionNo, RunNo, UploadId,
    WalSegmentId, WriterEpoch,
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

fn actor() -> ActorRef {
    ActorRef::service(ActorId::parse("loonfs-golden").expect("valid actor id"))
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

fn manifest_object_id(manifest_no: u64, suffix: &str) -> ManifestObjectId {
    ManifestObjectId::parse(format!("{manifest_no:020}-{suffix}"))
        .expect("valid manifest object id")
}

fn content_id(value: &str) -> ContentId {
    ContentId::parse(value).expect("valid content id")
}

fn sample_content_ref() -> ContentRef {
    ContentRef::blob_v1(
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
        content_id: content_id("con_fedcba9876543210fedcba9876543210"),
        size_bytes: 11_534_336,
        checksum: Checksum {
            algorithm: ChecksumAlgorithm::Crc64nvme,
            value: "bbb7305bdf118bcb".to_owned(),
        },
    }
}

/// Pins the durable JSON of a content reference for every checksum algorithm
/// the format defines.
///
/// One reference per algorithm in the vocabulary, so the durable bytes of
/// each are pinned independently of which write path happens to mint it: a
/// producer arriving for one of them must need no format change, and this
/// fixture is what fails if someone reshapes the reference on the way.
#[test]
fn content_ref_matches_golden_bytes_for_every_checksum_algorithm() {
    let references = [
        sample_content_ref(),
        sample_crc_content_ref(),
        ContentRef {
            kind: ContentRefKind::BlobV1,
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

/// The reference rejects fields it does not define: an unexpected key is
/// corruption, not a newer writer's extension.
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

fn sample_wal_pointer() -> WalSegmentPointer {
    WalSegmentPointer {
        segment_id: WalSegmentId::parse("00000000000000000001-fedcba9876543210")
            .expect("valid segment id"),
        start_seq: ChangeSeq(1),
        end_seq: ChangeSeq(1),
        payload_checksum: sha256_digest(b"previous segment payload"),
    }
}

fn sample_wal_envelope() -> WalSegmentEnvelope {
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
                deleted_direntry: Some(DeletedDirentry {
                    parent_inode_id: InodeId(1),
                    name_key: NameKey::parse("old.txt").expect("valid name key"),
                    display_name: loonfs_api::DisplayName::parse("Old.txt")
                        .expect("valid display name"),
                }),
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
    WalSegmentEnvelope::from_payload(WalSegmentPayload {
        namespace_id: namespace_id(),
        segment_id: WalSegmentId::parse("00000000000000000002-0123456789abcdef")
            .expect("valid segment id"),
        writer_epoch: WriterEpoch(3),
        prev_visible_segment: Some(sample_wal_pointer()),
        base_head_seq: ChangeSeq(1),
        start_seq: ChangeSeq(2),
        end_seq: ChangeSeq(2),
        records: vec![WalCommitPayload {
            seq: ChangeSeq(2),
            commit_id: commit_id(),
            committed_by: actor(),
            semantic_commit_fingerprint:
                "v1:sha256:0000000000000000000000000000000000000000000000000000000000000042"
                    .to_owned(),
            committed_at_ms: 4_000,
            message: Some("golden commit".to_owned()),
            deltas,
        }],
    })
    .expect("wal envelope")
}

fn sample_manifest_envelope() -> NamespaceManifestEnvelope {
    NamespaceManifestEnvelope::from_payload(NamespaceManifestPayload {
        namespace_id: namespace_id(),
        manifest_no: ManifestNo(2),
        manifest_object_id: manifest_object_id(2, "0123456789abcdef"),
        head_seq: ChangeSeq(2),
        head_commit_id: commit_id(),
        base_seq: ChangeSeq(2),
        writer_epoch: WriterEpoch(3),
        next_inode_id: InodeId(10),
        next_run_no: RunNo(1),
        retention_floor_seq: ChangeSeq(0),
        segments: vec![MetadataSegmentRef {
            owner_namespace_id: namespace_id(),
            segment_id: segment_id(),
            // WAL flush segments use the standard metadata segment prefix.
            compaction_job_id: None,
            run_no: RunNo(0),
            run_seq: ChangeSeq(2),
            level: 0,
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
    })
    .expect("manifest envelope")
}

/// Returns a manifest reference owned by the sample namespace.
fn sample_manifest_ref(number: u64) -> ManifestRef {
    ManifestRef {
        owner_namespace_id: namespace_id(),
        manifest_no: ManifestNo(number),
        manifest_object_id: manifest_object_id(number, "0123456789abcdef"),
        manifest_head_seq: ChangeSeq(number),
        manifest_payload_checksum:
            "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_owned(),
    }
}

fn sample_head_state() -> HeadState {
    HeadState {
        namespace_id: namespace_id(),
        content_store_id: content_store_id(),
        created_at_ms: 1_000,
        fork_basis: None,
        seq: ChangeSeq(2),
        head_commit_id: commit_id(),
        writer_epoch: WriterEpoch(3),
        writer: Some(WriterBlock {
            writer_id: "writer-a".to_owned(),
            acquired_at_ms: 2_000,
        }),
        next_inode_id: InodeId(10),
        visible_wal_tip: Some(sample_wal_pointer()),
        recent_segments: Vec::new(),
        status: NamespaceStatus::Active {},
    }
}

fn sample_deleted_head_state() -> HeadState {
    HeadState {
        status: NamespaceStatus::Deleted {},
        ..sample_head_state()
    }
}

/// A fork target's head: the same shape plus the permanent fork basis that
/// authorizes reading the source's manifest before the target's first flush.
fn sample_fork_head_state() -> HeadState {
    HeadState {
        fork_basis: Some(ForkBasis {
            manifest: ManifestRef {
                owner_namespace_id: NamespaceId::parse("source").expect("valid namespace id"),
                manifest_no: ManifestNo(2),
                manifest_object_id: manifest_object_id(2, "0123456789abcdef"),
                manifest_head_seq: ChangeSeq(2),
                manifest_payload_checksum:
                    "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
                        .to_owned(),
            },
            source_checkpoint_id: checkpoint_id("chk_00000000000000000000000000000002"),
        }),
        ..sample_head_state()
    }
}

// ---------------------------------------------------------------------------
// Encode/decode round trips against committed fixtures
// ---------------------------------------------------------------------------

#[test]
fn wal_segment_document_matches_golden_bytes() {
    let encoded = encode_wal_segment_envelope_zstd(&sample_wal_envelope()).expect("encode wal");
    // Compare the decompressed document: zstd frames may differ across zstd
    // versions, the document bytes (which the checksum covers) may not.
    assert_matches_golden("wal_segment.v1.cbor", &unzstd(&encoded));
}

#[test]
fn wal_segment_golden_decodes_to_sample() {
    let decoded = decode_wal_segment_envelope_zstd(&rezstd(&read_golden("wal_segment.v1.cbor")))
        .expect("decode golden wal segment");
    assert_eq!(decoded, sample_wal_envelope());
}

/// The segment id's 20-digit prefix is the segment's start sequence, and
/// reclamation reads it back out of the object key. A stored segment whose
/// id disagrees with the sequence beside it does not decode, and neither
/// does one carrying a chain link that disagrees with itself.
#[test]
fn wal_segments_reject_an_id_that_disagrees_with_its_start_seq() {
    let agreeing = encode_wal_segment_envelope_zstd(&sample_wal_envelope()).expect("encode wal");
    decode_wal_segment_envelope_zstd(&agreeing)
        .expect("a segment whose id encodes its start seq decodes");

    let mut payload = sample_wal_envelope().payload;
    payload.segment_id =
        WalSegmentId::parse("00000000000000000009-0123456789abcdef").expect("valid segment id");
    let message = assert_wal_segment_is_corrupt(payload);
    assert!(
        message.contains("`00000000000000000009-0123456789abcdef`")
            && message.contains("start seq `2`"),
        "the rejection should name both values: {message}"
    );

    let mut payload = sample_wal_envelope().payload;
    let mut link = sample_wal_pointer();
    link.segment_id =
        WalSegmentId::parse("00000000000000000004-fedcba9876543210").expect("valid segment id");
    payload.prev_visible_segment = Some(link);
    let message = assert_wal_segment_is_corrupt(payload);
    assert!(
        message.contains("`00000000000000000004-fedcba9876543210`")
            && message.contains("start seq `1`"),
        "the rejection should name both values: {message}"
    );
}

/// Stores `payload` through the real codec and returns why decoding refused
/// it.
fn assert_wal_segment_is_corrupt(payload: WalSegmentPayload) -> String {
    let envelope = WalSegmentEnvelope::from_payload(payload).expect("rebuild the wal envelope");
    let encoded = encode_wal_segment_envelope_zstd(&envelope).expect("encode wal");
    let error =
        decode_wal_segment_envelope_zstd(&encoded).expect_err("the stored segment is corrupt");
    assert!(
        matches!(error, EnvelopeCodecError::PayloadDecode(_)),
        "unexpected refusal: {error}"
    );
    error.to_string()
}

#[test]
fn namespace_manifest_matches_golden_bytes() {
    let encoded = encode_namespace_manifest_json(&sample_manifest_envelope()).expect("encode");
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
    assert_eq!(decoded, sample_manifest_envelope());
}

fn check_control_golden<T>(fixture: &str, kind: ControlObjectKind, state: T)
where
    T: Serialize + DeserializeOwned + PartialEq + Debug,
{
    let envelope = ControlObjectEnvelope::from_state(kind, state).expect("control envelope");
    let encoded = encode_control_object(&envelope).expect("encode control object");
    assert_matches_golden(fixture, &encoded);

    let decoded: ControlObjectEnvelope<T> =
        decode_control_object(&read_golden(fixture), kind).expect("decode golden control object");
    assert_eq!(decoded, envelope);
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
fn head_status_reading_is_fail_closed_on_unknown_statuses() {
    // Every head writes the field, active heads included, and an active
    // head round-trips through the tagged object it writes.
    let active = sample_head_state();
    let encoded = serde_json::to_string(&active).expect("encode active head");
    assert!(
        encoded.contains("\"status\":{\"kind\":\"active\"}"),
        "an active head writes its status: {encoded}"
    );
    let round_tripped =
        serde_json::from_str::<HeadState>(&encoded).expect("an active head round-trips");
    assert_eq!(round_tripped, active);

    // A status this build does not know must fail decode, never default:
    // serving a namespace in an unrecognized status is the one mistake the
    // field exists to prevent.
    let future = encoded.replacen(
        "\"status\":{\"kind\":\"active\"}",
        "\"status\":{\"kind\":\"frozen\"}",
        1,
    );
    serde_json::from_str::<HeadState>(&future).expect_err("an unknown status must fail closed");

    let deleted = serde_json::to_string(&sample_deleted_head_state()).expect("encode deleted");
    assert!(deleted.contains("\"status\":{\"kind\":\"deleted\"}"));
}

/// The field is required. A head that omits it is malformed, exactly like a
/// head that omits its content store, and nothing defaults it to active.
#[test]
fn head_without_a_status_is_rejected() {
    let mut document =
        serde_json::to_value(sample_head_state()).expect("encode active head as a document");
    document
        .as_object_mut()
        .expect("head document")
        .remove("status");

    let error = serde_json::from_value::<HeadState>(document)
        .expect_err("a head without its status must be rejected");
    assert!(
        error.to_string().contains("status"),
        "the rejection should name the field: {error}"
    );
}

/// The status object holds a tag and nothing else, so a stray field inside
/// it is corruption a guarded rewrite must not erase.
#[test]
fn head_status_rejects_unknown_fields_as_corruption() {
    let mut document =
        serde_json::to_value(sample_head_state()).expect("encode active head as a document");
    document["status"]["field_from_the_future"] = serde_json::Value::from(true);

    let error = serde_json::from_value::<HeadState>(document)
        .expect_err("a status carrying an unknown field must be rejected");
    assert!(
        error.to_string().contains("field_from_the_future"),
        "the rejection should name the field: {error}"
    );
}

#[test]
fn control_objects_match_golden_bytes() {
    check_control_golden(
        "control_wal_head.v1.json",
        ControlObjectKind::WalHead,
        sample_head_state(),
    );
    check_control_golden(
        "control_namespace_head.deleted.v1.json",
        ControlObjectKind::WalHead,
        sample_deleted_head_state(),
    );
    check_control_golden(
        "control_namespace_head.fork.v1.json",
        ControlObjectKind::WalHead,
        sample_fork_head_state(),
    );
    check_control_golden(
        "control_wal_floor.v1.json",
        ControlObjectKind::WalFloor,
        WalFloorState {
            namespace_id: namespace_id(),
            floor_seq: ChangeSeq(1),
            verified_at_ms: 3_000,
            updated_at_ms: 3_000,
        },
    );
    check_control_golden(
        "control_metadata_root.v1.json",
        ControlObjectKind::MetadataRoot,
        MetadataRootState {
            namespace_id: namespace_id(),
            manifest: sample_manifest_ref(2),
            updated_at_ms: 3_000,
        },
    );
    check_control_golden(
        "control_checkpoint_record.v1.json",
        ControlObjectKind::CheckpointRecord,
        CheckpointRecordState {
            checkpoint_id: checkpoint_id("chk_00000000000000000000000000000002"),
            namespace_id: namespace_id(),
            manifest: sample_manifest_ref(2),
            head_commit_id: commit_id(),
            created_at_ms: 3_000,
            owner: CheckpointOwner::User {
                name: "nightly".to_owned(),
                expires_at_ms: None,
            },
            status: CheckpointStatus::Active {},
        },
    );
    // The fork owner is a durable encoding of its own: the tagged `owner`
    // changes the document, and a fork record always carries its lease.
    check_control_golden(
        "control_checkpoint_record_fork.v1.json",
        ControlObjectKind::CheckpointRecord,
        CheckpointRecordState {
            checkpoint_id: checkpoint_id("chk_00000000000000000000000000000004"),
            namespace_id: namespace_id(),
            manifest: sample_manifest_ref(4),
            head_commit_id: commit_id(),
            created_at_ms: 3_000,
            owner: CheckpointOwner::Fork {
                target_namespace_id: NamespaceId::parse("clone").expect("valid namespace id"),
                expires_at_ms: 2_463_000,
            },
            status: CheckpointStatus::Active {},
        },
    );
    // The lease is a control object of its own family, and the two things
    // that write it — the job and the collector that fences it — decide
    // ownership by compare-and-swapping this document. Its bytes are pinned
    // like every other family's so a field rename cannot silently change what
    // either party reads that claim out of.
    check_control_golden(
        "control_compaction_lease.v1.json",
        ControlObjectKind::CompactionLease,
        MetadataCompactionLeaseState {
            job_id: MetadataCompactionId::parse("cmp_0123456789abcdef0123456789abcdef")
                .expect("valid compaction id"),
            namespace_id: namespace_id(),
            writer_id: "writer-1".to_owned(),
            status: CompactionLeaseStatus::Active {},
            started_at_ms: 1_000,
            heartbeat_at_ms: 3_000,
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
                staging: ProxiedStaging::Staged(sample_content_ref()),
            },
            status: UploadSessionRecordStatus::Completed {
                completed_at_ms: 2_000,
                content_ref: sample_content_ref(),
            },
        },
    );
    // The released status and the direct-put session shape are durable
    // encodings of their own: `status` and `mode` change the document. A
    // released record carries the instant its grace window runs from, and
    // there is no third checkpoint status to pin.
    check_control_golden(
        "control_checkpoint_record_released.v1.json",
        ControlObjectKind::CheckpointRecord,
        CheckpointRecordState {
            checkpoint_id: checkpoint_id("chk_00000000000000000000000000000003"),
            namespace_id: namespace_id(),
            manifest: sample_manifest_ref(3),
            head_commit_id: commit_id(),
            created_at_ms: 3_000,
            owner: CheckpointOwner::User {
                name: "nightly".to_owned(),
                expires_at_ms: Some(9_000),
            },
            status: CheckpointStatus::Released {
                released_at_ms: 9_000,
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

/// Every durable record with a lifecycle spells that field `status` and
/// writes a `kind`-tagged object into it.
///
/// The fixtures are the durable bytes, so this reads the committed payloads
/// rather than a re-encoding. The fifth family, the grep manifest, is
/// checked the same way in `loonfs-grep`, which owns those bytes.
#[test]
fn every_durable_status_is_a_kind_tagged_object() {
    let fixtures = [
        "control_wal_head.v1.json",
        "control_namespace_head.deleted.v1.json",
        "control_checkpoint_record.v1.json",
        "control_checkpoint_record_released.v1.json",
        "control_upload_session.v1.json",
        "control_compaction_lease.v1.json",
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
fn every_mutable_control_payload_rejects_unknown_fields_as_corruption() {
    let add_unknown = |payload: &mut serde_json::Value| {
        payload["field_from_the_future"] = serde_json::Value::from(true);
    };
    assert_control_payload_edit_is_corrupt::<HeadState>(
        "control_wal_head.v1.json",
        ControlObjectKind::WalHead,
        add_unknown,
    );
    assert_control_payload_edit_is_corrupt::<WalFloorState>(
        "control_wal_floor.v1.json",
        ControlObjectKind::WalFloor,
        add_unknown,
    );
    assert_control_payload_edit_is_corrupt::<MetadataRootState>(
        "control_metadata_root.v1.json",
        ControlObjectKind::MetadataRoot,
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
    assert_control_payload_edit_is_corrupt::<HeadState>(
        "control_namespace_head.fork.v1.json",
        ControlObjectKind::WalHead,
        add_unknown,
    );
}

#[test]
fn mutable_control_nested_structs_reject_unknown_fields_as_corruption() {
    assert_control_payload_edit_is_corrupt::<HeadState>(
        "control_wal_head.v1.json",
        ControlObjectKind::WalHead,
        |payload| payload["writer"]["field_from_the_future"] = serde_json::Value::from(true),
    );
    // Both head pointer fields must reject data that a rewrite would drop.
    assert_control_payload_edit_is_corrupt::<HeadState>(
        "control_wal_head.v1.json",
        ControlObjectKind::WalHead,
        |payload| {
            payload["visible_wal_tip"]["field_from_the_future"] = serde_json::Value::from(true);
        },
    );
    assert_control_payload_edit_is_corrupt::<HeadState>(
        "control_wal_head.v1.json",
        ControlObjectKind::WalHead,
        |payload| {
            // The fixture has no predecessor hints, so add one based on the
            // visible tip and include an unknown field.
            let mut hint = payload["visible_wal_tip"].clone();
            hint["field_from_the_future"] = serde_json::Value::from(true);
            payload["recent_segments"] = serde_json::Value::Array(vec![hint]);
        },
    );
    assert_control_payload_edit_is_corrupt::<HeadState>(
        "control_namespace_head.fork.v1.json",
        ControlObjectKind::WalHead,
        |payload| payload["fork_basis"]["field_from_the_future"] = serde_json::Value::from(true),
    );
    assert_control_payload_edit_is_corrupt::<CheckpointRecordState>(
        "control_checkpoint_record.v1.json",
        ControlObjectKind::CheckpointRecord,
        |payload| payload["owner"]["field_from_the_future"] = serde_json::Value::from(true),
    );
    // Manifest references reject unknown fields in every control object.
    assert_control_payload_edit_is_corrupt::<MetadataRootState>(
        "control_metadata_root.v1.json",
        ControlObjectKind::MetadataRoot,
        |payload| payload["manifest"]["field_from_the_future"] = serde_json::Value::from(true),
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

/// The checkpoint status is a tagged object over two variants. A bare
/// string is not one of them, whatever it spells, and neither is a tag this
/// format does not define.
#[test]
fn checkpoint_records_reject_an_untagged_or_unknown_status() {
    for untagged in ["active", "released", "condemned"] {
        assert_control_payload_edit_is_corrupt::<CheckpointRecordState>(
            "control_checkpoint_record.v1.json",
            ControlObjectKind::CheckpointRecord,
            |payload| payload["status"] = serde_json::Value::from(untagged),
        );
    }
    // A third status is not a tag this format knows either.
    assert_control_payload_edit_is_corrupt::<CheckpointRecordState>(
        "control_checkpoint_record.v1.json",
        ControlObjectKind::CheckpointRecord,
        |payload| payload["status"]["kind"] = serde_json::Value::from("condemned"),
    );
    // A release without its stamp cannot be aged, so it is not a release.
    assert_control_payload_edit_is_corrupt::<CheckpointRecordState>(
        "control_checkpoint_record.v1.json",
        ControlObjectKind::CheckpointRecord,
        |payload| payload["status"]["kind"] = serde_json::Value::from("released"),
    );
}

#[test]
fn active_checkpoint_records_reject_release_stamps() {
    assert_control_payload_edit_is_corrupt::<CheckpointRecordState>(
        "control_checkpoint_record.v1.json",
        ControlObjectKind::CheckpointRecord,
        |payload| payload["status"]["released_at_ms"] = serde_json::Value::from(9_000),
    );
}

/// A fork-owned record is the lease over one fork attempt, and its shape
/// requires the expiry that makes an abandoned attempt collectable. A user
/// pin without an expiry remains an ordinary permanent pin.
#[test]
fn fork_checkpoint_records_reject_a_missing_lease_expiry() {
    let message = assert_control_payload_edit_is_corrupt::<CheckpointRecordState>(
        "control_checkpoint_record_fork.v1.json",
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
        "unexpected refusal: {message}"
    );
}

/// The upload status is a tagged object over three variants, each carrying
/// the instant its own transition happened. A bare string is not one of
/// them, an undefined tag is not one of them, and neither is a defined tag
/// without its stamp.
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
    assert_control_payload_edit_is_corrupt::<HeadState>(
        "control_wal_head.v1.json",
        ControlObjectKind::WalHead,
        |payload| payload["status"] = serde_json::Value::from("future_status"),
    );
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

/// Upload sessions reject mode fields outside the tagged `mode` object.
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

/// Every reference a record holds names the object the session owns. A
/// record that disagrees with itself describes two objects and could
/// publish one while verifying the other, so it is refused at load.
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
fn completed_direct_put_sessions_require_the_session_algorithm() {
    let message = assert_control_payload_edit_is_corrupt::<UploadSessionState>(
        "control_upload_session.v1.json",
        ControlObjectKind::UploadSession,
        |payload| {
            payload["mode"] = serde_json::json!({
                "kind": "direct_put",
                "checksum_algorithm": "crc32c"
            });
        },
    );
    assert!(
        message.contains("requires `crc32c` but its completed content uses `sha256`"),
        "unexpected refusal: {message}"
    );
}

/// Rejects upload sessions that omit required mode fields.
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

/// Multipart sessions reject a part size of zero.
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
        serde_json::from_slice(&read_golden("control_wal_head.v1.json"))
            .expect("decode control fixture");
    document["field_from_the_future"] = serde_json::Value::from(true);
    let edited = serde_json::to_vec(&document).expect("encode edited envelope");

    let error = decode_control_object::<HeadState>(&edited, ControlObjectKind::WalHead)
        .expect_err("unknown mutable envelope field must be rejected");
    assert!(
        matches!(error, EnvelopeCodecError::EnvelopeDecode(_)),
        "unexpected error: {error}"
    );
}

#[test]
fn checkpoint_and_upload_decoders_reject_wrong_format_version_without_fallback() {
    let cases = [
        (
            ControlObjectKind::CheckpointRecord,
            serde_json::to_value(CheckpointRecordState {
                checkpoint_id: checkpoint_id("chk_00000000000000000000000000000005"),
                namespace_id: namespace_id(),
                manifest: ManifestRef {
                    owner_namespace_id: namespace_id(),
                    manifest_no: ManifestNo(5),
                    manifest_object_id: manifest_object_id(5, "0123456789abcdef"),
                    manifest_head_seq: ChangeSeq(5),
                    manifest_payload_checksum: sha256_digest(b"manifest"),
                },
                head_commit_id: commit_id(),
                created_at_ms: 3_000,
                owner: CheckpointOwner::User {
                    name: "nightly".to_owned(),
                    expires_at_ms: None,
                },
                status: CheckpointStatus::Released {
                    released_at_ms: 4_000,
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
        let envelope = ControlObjectEnvelope::from_state(kind, state).expect("control envelope");
        let encoded = encode_control_object(&envelope).expect("encode control");
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
        MetadataRowFamily::RevisionsByInodeDesc,
        MetadataRowFamily::Tombstones,
        MetadataRowFamily::ActiveDeletions,
        MetadataRowFamily::CommitReceipts,
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
            "\"revisions_by_inode_desc\"",
            "\"tombstones\"",
            "\"active_deletions\"",
            "\"commit_receipts\"",
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
    envelope: &WalSegmentEnvelope,
    edit: impl FnOnce(&mut ciborium::Value),
) -> Vec<u8> {
    let document = unzstd(&encode_wal_segment_envelope_zstd(envelope).expect("wal"));
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
fn wal_envelope_with_deltas(deltas: Vec<WalCommitDelta>) -> WalSegmentEnvelope {
    let mut payload = sample_wal_envelope().payload;
    payload.records[0].deltas = deltas;
    WalSegmentEnvelope::from_payload(payload).expect("wal envelope")
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
    let document = unzstd(&encode_wal_segment_envelope_zstd(&sample_wal_envelope()).expect("wal"));
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
    let document = unzstd(&encode_wal_segment_envelope_zstd(&sample_wal_envelope()).expect("wal"));
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
    let document = unzstd(&encode_wal_segment_envelope_zstd(&sample_wal_envelope()).expect("wal"));
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
fn wal_decode_tolerates_additive_payload_fields() {
    // Simulate a same-format-version writer that added a payload field: the
    // payload bytes change (and so does their checksum), but readers that do
    // not know the field must still decode the segment.
    let envelope = sample_wal_envelope();
    let document = wal_document_with_payload_edit(&envelope, with_future_field);

    let decoded = decode_wal_segment_envelope_zstd(&document)
        .expect("additive payload fields must remain readable");
    assert_eq!(decoded.payload, envelope.payload);
}

/// Immutable WAL segments accept unknown predecessor-pointer fields.
#[test]
fn wal_decode_tolerates_an_additive_field_inside_the_predecessor_pointer() {
    let envelope = sample_wal_envelope();
    let document = wal_document_with_payload_edit(&envelope, |payload| {
        with_future_field(cbor_entry(payload, "prev_visible_segment"));
    });

    let decoded = decode_wal_segment_envelope_zstd(&document)
        .expect("an additive field inside the predecessor pointer must remain readable");
    assert_eq!(decoded.payload, envelope.payload);
}

/// Immutable WAL segments accept unknown fields nested in tombstone deltas.
#[test]
fn wal_decode_tolerates_additive_fields_inside_tombstone_deltas() {
    let envelope = wal_envelope_with_deltas(vec![
        WalCommitDelta {
            semantic_op_index: 0,
            delta: WalDelta::TombstoneSubtree {
                delta_index: 0,
                root_inode_id: InodeId(9),
                deleted_direntry: Some(DeletedDirentry {
                    parent_inode_id: InodeId(1),
                    name_key: name_key("old.txt"),
                    display_name: loonfs_api::DisplayName::parse("Old.txt")
                        .expect("valid display name"),
                }),
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

    let decoded = decode_wal_segment_envelope_zstd(&document)
        .expect("additive fields inside a delta must remain readable");
    assert_eq!(decoded.payload, envelope.payload);
}

/// Actor references reject unknown fields in every context because the same
/// type is also used in request bodies.
#[test]
fn wal_decode_rejects_an_additive_field_inside_the_commit_attribution() {
    let document = wal_document_with_payload_edit(&sample_wal_envelope(), |payload| {
        with_future_field(cbor_entry(payload_commit(payload), "committed_by"));
    });

    let error = decode_wal_segment_envelope_zstd(&document)
        .expect_err("the actor rejects a field it does not define");
    assert!(
        matches!(&error, EnvelopeCodecError::PayloadDecode(message)
            if message.contains("unknown field") && message.contains("field_from_the_future")),
        "unexpected corruption error: {error}"
    );
}

#[test]
fn wal_decode_rejects_a_version_one_commit_without_committed_by() {
    let document = wal_document_with_payload_edit(&sample_wal_envelope(), |payload| {
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
    let envelope =
        ControlObjectEnvelope::from_state(ControlObjectKind::WalHead, sample_head_state())
            .expect("control envelope");
    let encoded = encode_control_object(&envelope).expect("encode control object");
    let mut document: serde_json::Value =
        serde_json::from_slice(&encoded).expect("decode document");
    document["payload"]["seq"] = serde_json::Value::from(999);
    let tampered = serde_json::to_vec(&document).expect("encode tampered document");

    let err = decode_control_object::<HeadState>(&tampered, ControlObjectKind::WalHead)
        .expect_err("tampered payload must be rejected");
    assert!(
        matches!(err, EnvelopeCodecError::ChecksumMismatch { .. }),
        "unexpected error: {err}"
    );
}

#[test]
fn namespace_manifest_decode_rejects_wrong_format_version_cleanly() {
    let encoded = encode_namespace_manifest_json(&sample_manifest_envelope()).expect("manifest");
    let mut document: serde_json::Value =
        serde_json::from_slice(&encoded).expect("decode document");
    document["format_version"] = serde_json::Value::from(7);
    let wrong_version = serde_json::to_vec(&document).expect("encode document");

    let err =
        decode_namespace_manifest_json(&wrong_version).expect_err("wrong version must be rejected");
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
fn namespace_manifest_decode_rejects_tampered_payload_as_checksum_mismatch() {
    let encoded = encode_namespace_manifest_json(&sample_manifest_envelope()).expect("manifest");
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
fn namespace_manifest_decode_tolerates_additive_payload_fields() {
    let envelope = sample_manifest_envelope();
    let encoded = encode_namespace_manifest_json(&envelope).expect("manifest");
    let mut document: serde_json::Value =
        serde_json::from_slice(&encoded).expect("decode document");
    document["payload"]["field_from_the_future"] = serde_json::Value::from(true);
    let future_payload =
        serde_json::to_string(&document["payload"]).expect("encode future payload");
    document["payload_checksum"] =
        serde_json::Value::from(sha256_digest(future_payload.as_bytes()));
    // Rebuild the document so the embedded payload bytes are exactly the
    // bytes the checksum was computed over.
    let future_document = format!(
        "{{\"kind\":{},\"format_version\":{},\"payload_checksum\":{},\"payload\":{}}}",
        document["kind"], document["format_version"], document["payload_checksum"], future_payload,
    );

    let decoded = decode_namespace_manifest_json(future_document.as_bytes())
        .expect("additive payload fields must remain readable");
    assert_eq!(decoded.payload, envelope.payload);
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
                deleted_direntry: None,
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
    MetadataRow::Tombstone {
        root_inode_id: InodeId(5),
        generation: TombstoneGeneration {
            seq: ChangeSeq(8),
            delta_index: 0,
        },
        commit_id: commit_id(),
        action: TombstoneRowAction::Set {
            deleted_direntry: Some(DeletedDirentry {
                parent_inode_id: InodeId(1),
                name_key: name_key("docs-archive"),
                display_name: loonfs_api::DisplayName::parse("Docs-Archive")
                    .expect("valid display name"),
            }),
        },
        deleted_at_ms: 4_000,
        deleted_by: actor(),
    }
}

/// The undelete that cancels it, naming the exact generation it revokes.
fn sample_tombstone_revoke_row() -> MetadataRow {
    MetadataRow::Tombstone {
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
    }
}

/// The active-deletion row the materializer derives from the set above. It
/// carries the deletion's stamp and the binding the trash entry renders, both
/// copied from the tombstone event.
fn sample_active_deletion_listed_row() -> MetadataRow {
    MetadataRow::ActiveDeletion {
        root_inode_id: InodeId(5),
        deletion_seq: ChangeSeq(8),
        action: ActiveDeletionRowAction::Listed {
            deleted_at_ms: 4_000,
            deleted_by: actor(),
            deleted_direntry: Some(DeletedDirentry {
                parent_inode_id: InodeId(1),
                name_key: name_key("docs-archive"),
                display_name: loonfs_api::DisplayName::parse("Docs-Archive")
                    .expect("valid display name"),
            }),
        },
    }
}

/// The active-deletion row the materializer derives from the revoke above. It
/// repeats the deletion's sequence rather than the undelete's, so the two rows
/// share a key prefix, and its rank sorts it ahead of the row it removes.
fn sample_active_deletion_removed_row() -> MetadataRow {
    MetadataRow::ActiveDeletion {
        root_inode_id: InodeId(5),
        deletion_seq: ChangeSeq(8),
        action: ActiveDeletionRowAction::Removed {
            revocation_seq: ChangeSeq(9),
        },
    }
}

/// An attribute revision that cleared the map. The empty map has an encoding
/// of its own, so the sample carries a row that states it.
fn sample_cleared_attributes_row() -> MetadataRow {
    MetadataRow::AttributesRevision {
        inode_id: InodeId(2),
        attributes_revision_no: AttributeRevisionNo(3),
        committed_seq: ChangeSeq(7),
        commit_id: commit_id(),
        delta_index: 1,
        updated_by: actor(),
        updated_at_ms: 7_000,
        attributes: Attributes::default(),
    }
}

/// An attribute revision that states a populated map.
fn sample_populated_attributes_row() -> MetadataRow {
    MetadataRow::AttributesRevision {
        inode_id: InodeId(5),
        attributes_revision_no: AttributeRevisionNo(2),
        committed_seq: ChangeSeq(5),
        commit_id: commit_id(),
        delta_index: 0,
        updated_by: actor(),
        updated_at_ms: 5_000,
        attributes: sample_attributes(),
    }
}

fn sample_commit_receipt_row() -> MetadataRow {
    MetadataRow::CommitReceipt {
        commit_id: commit_id(),
        committed_by: actor(),
        semantic_commit_fingerprint: "fp:golden".to_owned(),
        committed_seq: ChangeSeq(9),
        committed_at_ms: 9_000,
        message: None,
    }
}

fn sample_inode_rows() -> [MetadataRow; 2] {
    [
        MetadataRow::Inode {
            inode_id: InodeId(1),
            inode_kind: InodeKind::Directory,
            created_seq: ChangeSeq(1),
            commit_id: commit_id(),
            created_by: actor(),
            created_at_ms: 1_000,
        },
        MetadataRow::Inode {
            inode_id: InodeId(2),
            inode_kind: InodeKind::File,
            created_seq: ChangeSeq(3),
            commit_id: commit_id(),
            created_by: actor(),
            created_at_ms: 3_000,
        },
    ]
}

fn sample_revision_rows() -> [MetadataRow; 2] {
    [
        MetadataRow::FileRevision {
            inode_id: InodeId(2),
            revision_no: RevisionNo(1),
            committed_seq: ChangeSeq(3),
            commit_id: commit_id(),
            committed_at_ms: 3_000,
            committed_by: actor(),
            delta_index: 0,
            content_ref: sample_content_ref(),
        },
        MetadataRow::FileRevision {
            inode_id: InodeId(2),
            revision_no: RevisionNo(2),
            committed_seq: ChangeSeq(4),
            commit_id: commit_id(),
            committed_at_ms: 4_000,
            committed_by: actor(),
            delta_index: 0,
            content_ref: sample_crc_content_ref(),
        },
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
        MetadataRow::DirentryBind {
            parent_inode_id: InodeId(1),
            name_key: name_key("docs"),
            display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
            child_inode_id: InodeId(2),
            bind_seq: ChangeSeq(3),
            bind_delta_index: 0,
        },
        MetadataRow::DirentryBind {
            parent_inode_id: InodeId(1),
            name_key: name_key("docs-archive"),
            display_name: loonfs_api::DisplayName::parse("docs-archive")
                .expect("valid display name"),
            child_inode_id: InodeId(5),
            bind_seq: ChangeSeq(6),
            bind_delta_index: 0,
        },
        MetadataRow::DirentryUnbind {
            parent_inode_id: InodeId(1),
            name_key: name_key("docs-archive"),
            display_name: loonfs_api::DisplayName::parse("Docs-Archive")
                .expect("valid display name"),
            child_inode_id: InodeId(5),
            bind_seq: ChangeSeq(6),
            bind_delta_index: 0,
            unbind_seq: ChangeSeq(8),
            unbind_delta_index: 0,
        },
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

/// The active-deletion family sorts ahead of every other prefix, so both of
/// its rows — the listing and the removal that cancels it — sit inside the
/// block the fixture above pins. This says so, the way the guards below say
/// the same for the attribute and tombstone families.
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

/// The fixture above pins the block's bytes. This test names the rows those
/// bytes hold, so a regeneration that changed the sample cannot pass
/// unnoticed.
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

/// Covers the encodings for both populated and cleared attribute maps.
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

/// Checks the stable encoding of a tombstone and its matching revoke. A
/// separate fixture keeps block splitting from separating the pair.
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
fn decode_edited_row(row: &ciborium::Value, why: &str) -> MetadataRow {
    let mut encoded = Vec::new();
    ciborium::ser::into_writer(row, &mut encoded).expect("encode edited row");
    ciborium::de::from_reader::<MetadataRow, _>(encoded.as_slice())
        .unwrap_or_else(|error| panic!("{why}, but the row failed to decode: {error}"))
}

#[test]
fn commit_receipt_rows_without_committed_by_are_corrupt() {
    let mut row = row_cbor(&MetadataRow::CommitReceipt {
        commit_id: commit_id(),
        committed_by: actor(),
        semantic_commit_fingerprint: "v1:sha256:receipt".to_owned(),
        committed_seq: ChangeSeq(9),
        committed_at_ms: 9_000,
        message: None,
    });
    cbor_map_of(&mut row).retain(|(key, _)| key.as_text() != Some("committed_by"));
    let refusal = assert_row_is_corrupt(&row, "a receipt without attribution is corrupt");
    assert!(
        refusal.contains("missing field `committed_by`"),
        "unexpected refusal: {refusal}"
    );
}

/// The deleted binding is one value with three required parts. Two of the
/// three is not a binding anyone can restore or render, and the decoder says
/// so rather than filling the gap in with a default.
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

/// Immutable metadata rows accept unknown tombstone fields at every level.
#[test]
fn tombstone_rows_tolerate_additive_fields_at_every_level() {
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
        assert_eq!(
            decode_edited_row(
                &row,
                "an immutable row tolerates a field it does not define"
            ),
            expected,
            "the unknown field under {path:?} changed the decoded row"
        );
    }
}

/// A `revoke` ignores `deleted_direntry`, which is valid only for `set`.
#[test]
fn tombstone_revoke_rows_ignore_a_deleted_direntry() {
    let expected = sample_tombstone_revoke_row();
    let mut row = row_cbor(&expected);
    cbor_map_of(cbor_entry(&mut row, "action")).push((
        ciborium::Value::from("deleted_direntry"),
        sample_deleted_direntry_cbor(),
    ));

    assert_eq!(
        decode_edited_row(&row, "a revoke tolerates a field it does not define"),
        expected
    );
}

/// The pre-grouping layout, exactly as it was written before the generation
/// and the binding each became a shape of their own: `tombstone_seq` and
/// `tombstone_delta_index` beside three optional binding fields, and a `set`
/// with nothing in it.
#[test]
fn tombstone_rows_reject_the_pre_grouping_flat_encoding() {
    let row = row_cbor(&sample_tombstone_set_row());
    assert_row_is_corrupt(
        &with_flat_binding(with_flat_generation(row.clone())),
        "the pre-grouping layout is not a row this format has",
    );

    // Each half of that encoding on its own, over a row that is otherwise
    // current: neither is a spelling this row accepts.
    let refusal = assert_row_is_corrupt(
        &with_flat_generation(row.clone()),
        "a tombstone states its generation as one value",
    );
    assert!(
        refusal.contains("missing field `generation`"),
        "unexpected refusal: {refusal}"
    );
    let refusal = assert_row_is_corrupt(
        &with_flat_binding(row),
        "a `set` states its binding, even when it has none",
    );
    assert!(
        refusal.contains("missing field `deleted_direntry`"),
        "unexpected refusal: {refusal}"
    );
}

/// A listed row copies the binding from the tombstone it derives from, so the
/// same two rules hold on this side: the binding is one value with three
/// required parts, and a `listed` states the field even when the deletion
/// recorded no name.
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
        refusal.contains("missing field `deleted_direntry`"),
        "unexpected refusal: {refusal}"
    );
}

/// Version-one provenance rows require a commit ID, actor, and timestamp.
/// Decoding fails when any required field is missing.
#[test]
fn provenance_rows_reject_every_missing_required_field() {
    let cases = [
        (
            MetadataRow::Inode {
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(3),
                commit_id: commit_id(),
                created_by: actor(),
                created_at_ms: 3_000,
            },
            &["commit_id", "created_by", "created_at_ms"][..],
        ),
        (
            MetadataRow::FileRevision {
                inode_id: InodeId(2),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(3),
                commit_id: commit_id(),
                committed_at_ms: 3_000,
                committed_by: actor(),
                delta_index: 0,
                content_ref: sample_content_ref(),
            },
            &["commit_id", "committed_by"][..],
        ),
        (sample_tombstone_set_row(), &["commit_id", "deleted_by"][..]),
        (sample_active_deletion_listed_row(), &["deleted_by"][..]),
        (
            sample_populated_attributes_row(),
            &["commit_id", "updated_by", "updated_at_ms"][..],
        ),
    ];

    for (row, required_fields) in cases {
        // The active-deletion row states its attribution inside `action`;
        // every other row states it at the top level.
        let nested_in_action = matches!(row, MetadataRow::ActiveDeletion { .. });
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

/// Attribute values are strings in this format. The retired tagged object is
/// corruption rather than a shape the reader translates.
#[test]
fn attribute_rows_reject_the_retired_tagged_value_shape() {
    let mut row = row_cbor(&MetadataRow::AttributesRevision {
        inode_id: InodeId(2),
        attributes_revision_no: AttributeRevisionNo(1),
        committed_seq: ChangeSeq(5),
        commit_id: commit_id(),
        delta_index: 0,
        updated_by: actor(),
        updated_at_ms: 5_000,
        attributes: sample_attributes(),
    });
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

/// The map's limits are enforced on the way in, so a stored row that breaks
/// one fails to decode instead of decoding to something within the limits.
#[test]
fn attribute_rows_reject_a_map_over_its_limits() {
    let mut row = row_cbor(&MetadataRow::AttributesRevision {
        inode_id: InodeId(2),
        attributes_revision_no: AttributeRevisionNo(1),
        committed_seq: ChangeSeq(5),
        commit_id: commit_id(),
        delta_index: 0,
        updated_by: actor(),
        updated_at_ms: 5_000,
        attributes: sample_attributes(),
    });
    let owner = cbor_entry(cbor_entry(&mut row, "attributes"), "owner");
    *owner = ciborium::Value::from("v".repeat(loonfs_api::MAX_ATTRIBUTE_VALUE_BYTES + 1));

    assert_row_is_corrupt(&row, "an oversized value is not a value this format stores");
}

/// The set row's binding, as the CBOR map another writer would have written.
fn sample_deleted_direntry_cbor() -> ciborium::Value {
    let mut set = row_cbor(&sample_tombstone_set_row());
    cbor_entry(cbor_entry(&mut set, "action"), "deleted_direntry").clone()
}

/// Spells the row's generation as the two loose fields it used to be.
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

/// Spells the deleted binding as the three loose row fields it used to be,
/// leaving the `set` empty the way the old encoding left it.
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
        &MetadataRow::Inode {
            inode_id: InodeId(1),
            inode_kind: InodeKind::Directory,
            created_seq: ChangeSeq(1),
            commit_id: commit_id(),
            created_by: actor(),
            created_at_ms: 1_000,
        }
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
