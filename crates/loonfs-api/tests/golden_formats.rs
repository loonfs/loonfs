#![allow(clippy::panic)]
// These integration tests use panic for precise fixture-divergence diagnostics.

//! Golden-byte fixtures for every durable LoonFS encoding.
//!
//! These tests pin the exact bytes each format version writes. They exist to
//! make wire-format changes impossible to ship by accident:
//!
//! - If an encoder's output diverges from its fixture, a Rust-side change
//!   (field rename, reorder, serde attribute, removed field) silently changed
//!   the durable format. Either revert it, or bump the family's format
//!   version and regenerate the fixture with `UPDATE_GOLDEN=1 cargo test`.
//! - If a fixture stops decoding, the current reader can no longer read
//!   bytes an earlier build of the same format version wrote — a
//!   compatibility break with deployed data.
//! - Additive payload fields must keep decoding (`*_tolerates_additive_*`):
//!   that is the format's only same-version evolution mechanism, made
//!   possible by checksumming the stored payload bytes rather than a
//!   re-encoding.

use loonfs_api::wire::control::{
    decode_control_object, encode_control_object, CompletedUpload, ContentStoreDescriptorState,
    ControlCodecError, ControlObjectEnvelope, ControlObjectKind, HeadState, LeaseState,
    NamespaceDescriptorState, NamespaceForkState, NamespaceGcPinState, NamespaceState,
    ProgressState, UploadSessionState, WalSegmentPointer,
};
use loonfs_api::wire::manifest::{
    decode_metadata_sst_envelope_zstd, decode_namespace_manifest_json,
    encode_metadata_sst_envelope_zstd, encode_namespace_manifest_json, MetadataFileRef,
    MetadataPage, MetadataRow, MetadataSegmentKey, MetadataSstCodecError, MetadataSstEnvelope,
    MetadataSstPayload, MetadataTableFamily, NamespaceCheckpointRecord,
    NamespaceManifestCodecError, NamespaceManifestEnvelope, NamespaceManifestFork,
    NamespaceManifestPayload,
};
use loonfs_api::wire::wal::{
    decode_wal_segment_envelope_zstd, encode_wal_segment_envelope_zstd, WalCodecError,
    WalCommitDelta, WalCommitPayload, WalDelta, WalPrecondition, WalSegmentEnvelope,
    WalSegmentPayload,
};
use loonfs_api::{
    sha256_digest,
    v0::{CommitOpResult, UploadMode},
    ChangeSeq, CommitId, ContentRef, ContentStoreId, FenceToken, InodeId, InodeKind, ManifestId,
    NamePolicy, NamespaceId, RevisionNo,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

const WRITER_VERSION: &str = "golden-writer/1.0";

// ---------------------------------------------------------------------------
// Golden helpers
// ---------------------------------------------------------------------------

fn golden_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name)
}

fn assert_matches_golden(name: &str, actual: &[u8]) {
    let path = golden_path(name);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().expect("fixture parent dir"))
            .expect("create golden dir");
        std::fs::write(&path, actual).expect("write golden fixture");
    }
    let expected = std::fs::read(&path).unwrap_or_else(|err| {
        panic!("read golden fixture `{name}` ({err}); run `UPDATE_GOLDEN=1 cargo test -p loonfs-api` to generate it")
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
             if intentional, bump the format version and regenerate with UPDATE_GOLDEN=1.",
            expected.len(),
            actual.len(),
        );
    }
}

fn read_golden(name: &str) -> Vec<u8> {
    std::fs::read(golden_path(name)).unwrap_or_else(|err| {
        panic!("read golden fixture `{name}` ({err}); run `UPDATE_GOLDEN=1 cargo test -p loonfs-api` to generate it")
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

fn sample_content_ref() -> ContentRef {
    ContentRef::whole_file_v0(b"golden bytes")
}

fn sample_wal_pointer() -> WalSegmentPointer {
    WalSegmentPointer {
        object_key: "namespaces/demo/wal/00000000000000000002-fedcba9876543210.wal.zst".to_owned(),
        segment_id: "00000000000000000002-fedcba9876543210".to_owned(),
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
                inode_kind: InodeKind::Dir,
            },
        },
        WalCommitDelta {
            semantic_op_index: 0,
            delta: WalDelta::BindDirentry {
                delta_index: 1,
                parent_inode: InodeId(1),
                name_key: "docs".to_owned(),
                display_name: "Docs".to_owned(),
                child_inode: InodeId(7),
            },
        },
        WalCommitDelta {
            semantic_op_index: 1,
            delta: WalDelta::UnbindDirentry {
                delta_index: 2,
                parent_inode: InodeId(1),
                name_key: "old.txt".to_owned(),
                child_inode: InodeId(5),
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
                root_inode: InodeId(9),
            },
        },
    ];
    let preconditions = vec![
        WalPrecondition::InodeRevisionIs {
            inode_id: InodeId(5),
            revision_no: RevisionNo(1),
        },
        WalPrecondition::AncestorsNotSubtreeDeleted {
            inode_id: InodeId(5),
        },
        WalPrecondition::ChildNameAbsent {
            parent_inode: InodeId(1),
            name_key: "docs".to_owned(),
        },
        WalPrecondition::BindingIs {
            parent_inode: InodeId(1),
            name_key: "old.txt".to_owned(),
            child_inode: InodeId(5),
            bind_seq: ChangeSeq(1),
            bind_delta_index: 0,
        },
        WalPrecondition::DirectoryEmpty {
            inode_id: InodeId(9),
        },
    ];
    let mut annotations = BTreeMap::new();
    annotations.insert(
        "source".to_owned(),
        serde_json::Value::String("golden".to_owned()),
    );

    WalSegmentEnvelope::from_payload(
        WRITER_VERSION,
        WalSegmentPayload {
            namespace_id: namespace_id(),
            segment_id: "00000000000000000001-0123456789abcdef".to_owned(),
            prev_visible_segment: Some(sample_wal_pointer()),
            base_head_seq: ChangeSeq(1),
            start_seq: ChangeSeq(2),
            end_seq: ChangeSeq(2),
            records: vec![WalCommitPayload {
                namespace_id: namespace_id(),
                seq: ChangeSeq(2),
                apply_after_seq: ChangeSeq(1),
                commit_id: commit_id(),
                semantic_commit_fingerprint:
                    "v0:sha256:0000000000000000000000000000000000000000000000000000000000000042"
                        .to_owned(),
                writer_id: "writer-a".to_owned(),
                writer_fence_token: FenceToken(3),
                message: Some("golden commit".to_owned()),
                annotations: Some(annotations),
                deltas,
                preconditions,
                results: vec![CommitOpResult::CreateDirectory {
                    op_index: 0,
                    inode_id: InodeId(7),
                }],
            }],
        },
    )
    .expect("wal envelope")
}

fn sample_sst_envelope() -> MetadataSstEnvelope {
    let rows = vec![
        MetadataRow::Inode {
            inode_id: InodeId(7),
            inode_kind: InodeKind::Dir,
            created_seq: ChangeSeq(2),
        },
        MetadataRow::DirentryBind {
            parent_inode_id: InodeId(1),
            name_key: "docs".to_owned(),
            display_name: "Docs".to_owned(),
            child_inode_id: InodeId(7),
            bind_seq: ChangeSeq(2),
            bind_delta_index: 1,
        },
        MetadataRow::DirentryUnbind {
            parent_inode_id: InodeId(1),
            name_key: "old.txt".to_owned(),
            child_inode_id: InodeId(5),
            bind_seq: ChangeSeq(1),
            bind_delta_index: 0,
            unbind_seq: ChangeSeq(2),
            unbind_delta_index: 2,
        },
        MetadataRow::Revision {
            inode_id: InodeId(5),
            revision_no: RevisionNo(2),
            committed_seq: ChangeSeq(2),
            revision_delta_index: 3,
            content_ref: sample_content_ref(),
        },
        MetadataRow::Tombstone {
            root_inode_id: InodeId(9),
            tombstone_seq: ChangeSeq(2),
            tombstone_delta_index: 4,
        },
        MetadataRow::CommitReceipt {
            commit_id: commit_id(),
            semantic_commit_fingerprint:
                "v0:sha256:0000000000000000000000000000000000000000000000000000000000000042"
                    .to_owned(),
            committed_seq: ChangeSeq(2),
            results: vec![CommitOpResult::CreateDirectory {
                op_index: 0,
                inode_id: InodeId(7),
            }],
        },
    ];
    let row_keys = rows.iter().map(MetadataRow::row_key).collect::<Vec<_>>();
    let page = MetadataPage {
        page_index: 0,
        min_key: row_keys.first().cloned().expect("min key"),
        max_key: row_keys.last().cloned().expect("max key"),
        row_keys,
        rows,
    };

    MetadataSstEnvelope::from_payload(
        WRITER_VERSION,
        MetadataSstPayload {
            namespace_id: namespace_id(),
            table_id: "tbl_0123456789abcdef0123456789abcdef".to_owned(),
            run_seq: ChangeSeq(2),
            level: 0,
            family: MetadataTableFamily::Inodes,
            segment_index: 0,
            segment_key: MetadataSegmentKey::Full,
            row_count: page.rows.len() as u64,
            min_key: page.min_key.clone(),
            max_key: page.max_key.clone(),
            pages: vec![page],
        },
    )
    .expect("sst envelope")
}

fn sample_manifest_envelope() -> NamespaceManifestEnvelope {
    NamespaceManifestEnvelope::from_payload(
        WRITER_VERSION,
        NamespaceManifestPayload {
            namespace_id: namespace_id(),
            manifest_id: ManifestId(2),
            head_seq: ChangeSeq(2),
            head_commit_id: commit_id(),
            base_seq: ChangeSeq(2),
            active_fence_token: FenceToken(3),
            next_inode_id: InodeId(10),
            name_policy: NamePolicy::default(),
            retention_floor_seq: ChangeSeq(0),
            initialized: true,
            verified: true,
            fork: Some(NamespaceManifestFork {
                source_namespace_id: NamespaceId::parse("source").expect("valid namespace id"),
                fork_seq: ChangeSeq(2),
                source_checkpoint_id: "chk_00000000000000000000000000000001".to_owned(),
                source_manifest_id: ManifestId(1),
                source_head_seq: ChangeSeq(2),
            }),
            checkpoints: vec![NamespaceCheckpointRecord {
                checkpoint_id: "chk_00000000000000000000000000000002".to_owned(),
                manifest_id: ManifestId(2),
                head_seq: ChangeSeq(2),
                head_commit_id: commit_id(),
                created_at_ms: 1_000,
                expires_at_ms: Some(2_000),
                name: Some("golden".to_owned()),
            }],
            features: BTreeMap::from([(
                "index.fulltext".to_owned(),
                serde_json::json!({ "version": 2 }),
            )]),
            metadata_files: vec![MetadataFileRef {
                owner_namespace_id: namespace_id(),
                table_id: "tbl_0123456789abcdef0123456789abcdef".to_owned(),
                object_key:
                    "namespaces/demo/tables/metadata/tbl_0123456789abcdef0123456789abcdef.sst.zst"
                        .to_owned(),
                run_seq: ChangeSeq(2),
                level: 0,
                family: MetadataTableFamily::Inodes,
                segment_index: 0,
                segment_key: MetadataSegmentKey::Full,
                row_count: 6,
                min_key: "commit-receipt".to_owned(),
                max_key: "tombstone".to_owned(),
                payload_checksum: sha256_digest(b"sst payload"),
            }],
        },
    )
    .expect("manifest envelope")
}

fn sample_head_state() -> HeadState {
    HeadState {
        namespace_id: namespace_id(),
        seq: ChangeSeq(2),
        head_commit_id: commit_id(),
        active_fence_token: FenceToken(3),
        next_inode_id: InodeId(10),
        name_policy: NamePolicy::default(),
        current_manifest_id: Some(ManifestId(2)),
        latest_checkpoint_id: Some("chk_00000000000000000000000000000002".to_owned()),
        retention_floor_seq: ChangeSeq(0),
        visible_wal_tip: Some(sample_wal_pointer()),
        state: NamespaceState::Active,
    }
}

fn sample_deleted_head_state() -> HeadState {
    HeadState {
        state: NamespaceState::Deleted,
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

#[test]
fn metadata_sst_document_matches_golden_bytes() {
    let encoded = encode_metadata_sst_envelope_zstd(&sample_sst_envelope()).expect("encode sst");
    assert_matches_golden("metadata_sst.v1.cbor", &unzstd(&encoded));
}

#[test]
fn metadata_sst_golden_decodes_to_sample() {
    let decoded = decode_metadata_sst_envelope_zstd(&rezstd(&read_golden("metadata_sst.v1.cbor")))
        .expect("decode golden metadata sst");
    assert_eq!(decoded, sample_sst_envelope());
}

#[test]
fn namespace_manifest_matches_golden_bytes() {
    let encoded = encode_namespace_manifest_json(&sample_manifest_envelope()).expect("encode");
    assert_matches_golden("namespace_manifest.v1.json", &encoded);
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
    let envelope =
        ControlObjectEnvelope::from_state(kind, WRITER_VERSION, state).expect("control envelope");
    let encoded = encode_control_object(&envelope).expect("encode control object");
    assert_matches_golden(fixture, &encoded);

    let decoded: ControlObjectEnvelope<T> =
        decode_control_object(&read_golden(fixture), kind).expect("decode golden control object");
    assert_eq!(decoded, envelope);
}

#[test]
fn head_state_reading_is_fail_closed_on_unknown_lifecycle_states() {
    // An active head encodes without the field at all: the golden fixture
    // for the pre-state format decodes as Active (additive evolution), and
    // re-encoding it stays byte-identical.
    let active = sample_head_state();
    let encoded = serde_json::to_string(&active).expect("encode active head");
    assert!(
        !encoded.contains("\"state\""),
        "active heads must omit the lifecycle field"
    );

    // A state this build does not know must fail decode, never default:
    // serving a namespace in an unrecognized lifecycle state is the one
    // mistake the field exists to prevent.
    let future = encoded.replacen('{', "{\"state\":\"frozen\",", 1);
    let decoded = serde_json::from_str::<HeadState>(&future);
    assert!(decoded.is_err(), "unknown lifecycle state must fail closed");

    let deleted = serde_json::to_string(&sample_deleted_head_state()).expect("encode deleted");
    assert!(deleted.contains("\"state\":\"deleted\""));
}

#[test]
fn control_objects_match_golden_bytes() {
    check_control_golden(
        "control_namespace_head.v1.json",
        ControlObjectKind::NamespaceHead,
        sample_head_state(),
    );
    check_control_golden(
        "control_namespace_head.deleted.v1.json",
        ControlObjectKind::NamespaceHead,
        sample_deleted_head_state(),
    );
    check_control_golden(
        "control_namespace_lease.v1.json",
        ControlObjectKind::NamespaceLease,
        LeaseState {
            namespace_id: namespace_id(),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(3),
            lease_expires_at_ms: 2_000,
        },
    );
    check_control_golden(
        "control_namespace_descriptor.v1.json",
        ControlObjectKind::NamespaceDescriptor,
        NamespaceDescriptorState {
            namespace_id: namespace_id(),
            content_store_id: content_store_id(),
        },
    );
    check_control_golden(
        "control_content_store_descriptor.v1.json",
        ControlObjectKind::ContentStoreDescriptor,
        ContentStoreDescriptorState {
            content_store_id: content_store_id(),
        },
    );
    check_control_golden(
        "control_namespace_fork_state.v1.json",
        ControlObjectKind::NamespaceForkState,
        NamespaceForkState {
            namespace_id: NamespaceId::parse("clone").expect("valid namespace id"),
            source_namespace_id: namespace_id(),
            fork_seq: ChangeSeq(2),
            source_checkpoint_id: "chk_00000000000000000000000000000002".to_owned(),
            source_manifest_id: ManifestId(2),
            source_head_seq: ChangeSeq(2),
            created_at_ms: 1_000,
        },
    );
    check_control_golden(
        "control_namespace_gc_pin_state.v1.json",
        ControlObjectKind::NamespaceGcPinState,
        NamespaceGcPinState {
            pin_id: "pin_0123456789abcdef0123456789abcdef".to_owned(),
            source_namespace_id: namespace_id(),
            target_namespace_id: NamespaceId::parse("clone").expect("valid namespace id"),
            source_checkpoint_id: "chk_00000000000000000000000000000002".to_owned(),
            source_manifest_id: ManifestId(2),
            source_head_seq: ChangeSeq(2),
            created_at_ms: 1_000,
        },
    );
    check_control_golden(
        "control_namespace_progress.v1.json",
        ControlObjectKind::NamespaceProgress,
        ProgressState {
            namespace_id: namespace_id(),
            work_class: "checkpoint".to_owned(),
            through_seq: ChangeSeq(2),
        },
    );
    check_control_golden(
        "control_upload_session.v1.json",
        ControlObjectKind::UploadSession,
        UploadSessionState {
            namespace_id: namespace_id(),
            upload_id: "up_0123456789abcdef0123456789abcdef".to_owned(),
            mode: UploadMode::ServiceProxied,
            direct_put_content_ref: None,
            staged_content_ref: Some(sample_content_ref()),
            completed: Some(CompletedUpload {
                content_ref: sample_content_ref(),
            }),
            created_at_ms: 1_000,
        },
    );
}

fn content_store_id() -> ContentStoreId {
    ContentStoreId::parse("cs_0123456789abcdef0123456789abcdef").expect("valid content store id")
}

// ---------------------------------------------------------------------------
// Version, kind, and corruption semantics
// ---------------------------------------------------------------------------

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
fn wal_decode_rejects_future_format_version_cleanly() {
    let document = unzstd(&encode_wal_segment_envelope_zstd(&sample_wal_envelope()).expect("wal"));
    let bumped = with_cbor_document_entry(&document, "format_version", |value| {
        *value = ciborium::Value::from(2);
    });

    let err = decode_wal_segment_envelope_zstd(&rezstd(&bumped))
        .expect_err("future version must be rejected");
    assert!(
        matches!(
            err,
            WalCodecError::UnsupportedFormatVersion {
                found: 2,
                supported: 1,
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
        matches!(err, WalCodecError::UnexpectedKind { .. }),
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
        matches!(err, WalCodecError::ChecksumMismatch { .. }),
        "unexpected error: {err}"
    );
}

#[test]
fn wal_decode_tolerates_additive_payload_fields() {
    // Simulate a same-format-version writer that added a payload field: the
    // payload bytes change (and so does their checksum), but readers that do
    // not know the field must still decode the segment.
    let envelope = sample_wal_envelope();
    let document = unzstd(&encode_wal_segment_envelope_zstd(&envelope).expect("wal"));

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
    let mut payload_value: ciborium::Value =
        ciborium::de::from_reader(payload_bytes.as_slice()).expect("decode payload");
    payload_value.as_map_mut().expect("payload is a map").push((
        ciborium::Value::from("field_from_the_future"),
        ciborium::Value::from(true),
    ));
    let mut future_payload = Vec::new();
    ciborium::ser::into_writer(&payload_value, &mut future_payload).expect("encode payload");

    let with_payload = with_cbor_document_entry(&document, "payload", |value| {
        *value = ciborium::Value::Bytes(future_payload.clone());
    });
    let future_document = with_cbor_document_entry(&with_payload, "payload_checksum", |value| {
        *value = ciborium::Value::from(sha256_digest(&future_payload));
    });

    let decoded = decode_wal_segment_envelope_zstd(&rezstd(&future_document))
        .expect("additive payload fields must remain readable");
    assert_eq!(decoded.payload, envelope.payload);
}

#[test]
fn control_object_decode_rejects_tampered_payload_as_checksum_mismatch() {
    let envelope = ControlObjectEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        WRITER_VERSION,
        sample_head_state(),
    )
    .expect("control envelope");
    let encoded = encode_control_object(&envelope).expect("encode control object");
    let mut document: serde_json::Value =
        serde_json::from_slice(&encoded).expect("decode document");
    document["payload"]["seq"] = serde_json::Value::from(999);
    let tampered = serde_json::to_vec(&document).expect("encode tampered document");

    let err = decode_control_object::<HeadState>(&tampered, ControlObjectKind::NamespaceHead)
        .expect_err("tampered payload must be rejected");
    assert!(
        matches!(err, ControlCodecError::ChecksumMismatch { .. }),
        "unexpected error: {err}"
    );
}

#[test]
fn metadata_sst_decode_rejects_future_format_version_cleanly() {
    let document = unzstd(&encode_metadata_sst_envelope_zstd(&sample_sst_envelope()).expect("sst"));
    let bumped = with_cbor_document_entry(&document, "format_version", |value| {
        *value = ciborium::Value::from(2);
    });

    let err = decode_metadata_sst_envelope_zstd(&rezstd(&bumped))
        .expect_err("future version must be rejected");
    assert!(
        matches!(
            err,
            MetadataSstCodecError::UnsupportedFormatVersion {
                found: 2,
                supported: 1,
            }
        ),
        "unexpected error: {err}"
    );
}

#[test]
fn metadata_sst_decode_rejects_tampered_payload_bytes_as_checksum_mismatch() {
    let document = unzstd(&encode_metadata_sst_envelope_zstd(&sample_sst_envelope()).expect("sst"));
    let tampered = with_cbor_document_entry(&document, "payload", |value| {
        let bytes = match value {
            ciborium::Value::Bytes(bytes) => bytes,
            other => panic!("payload should be a CBOR byte string, got {other:?}"),
        };
        let last = bytes.last_mut().expect("payload is non-empty");
        *last ^= 0xff;
    });

    let err = decode_metadata_sst_envelope_zstd(&rezstd(&tampered))
        .expect_err("tampered payload must be rejected");
    assert!(
        matches!(err, MetadataSstCodecError::ChecksumMismatch { .. }),
        "unexpected error: {err}"
    );
}

#[test]
fn namespace_manifest_decode_rejects_future_format_version_cleanly() {
    let encoded = encode_namespace_manifest_json(&sample_manifest_envelope()).expect("manifest");
    let mut document: serde_json::Value =
        serde_json::from_slice(&encoded).expect("decode document");
    document["format_version"] = serde_json::Value::from(2);
    let bumped = serde_json::to_vec(&document).expect("encode document");

    let err = decode_namespace_manifest_json(&bumped).expect_err("future version must be rejected");
    assert!(
        matches!(
            err,
            NamespaceManifestCodecError::UnsupportedFormatVersion {
                found: 2,
                supported: 1,
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
        matches!(err, NamespaceManifestCodecError::ChecksumMismatch { .. }),
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
        "{{\"kind\":{},\"format_version\":{},\"writer_version\":{},\"payload_checksum\":{},\"payload\":{}}}",
        document["kind"], document["format_version"], document["writer_version"],
        document["payload_checksum"], future_payload,
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
                inode_kind: InodeKind::Dir,
            }),
            "create_inode",
        ),
        (
            serde_json::to_value(WalDelta::BindDirentry {
                delta_index: 0,
                parent_inode: InodeId(1),
                name_key: "a".to_owned(),
                display_name: "a".to_owned(),
                child_inode: InodeId(2),
            }),
            "bind_direntry",
        ),
        (
            serde_json::to_value(WalDelta::UnbindDirentry {
                delta_index: 0,
                parent_inode: InodeId(1),
                name_key: "a".to_owned(),
                child_inode: InodeId(2),
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
                root_inode: InodeId(2),
            }),
            "tombstone_subtree",
        ),
    ];
    for (value, expected_tag) in cases {
        let value = value.expect("serialize delta");
        assert_eq!(value["delta"], expected_tag, "in {value}");
    }
}

#[test]
fn wal_precondition_wire_tags_match_spec_names() {
    let cases = [
        (
            serde_json::to_value(WalPrecondition::InodeRevisionIs {
                inode_id: InodeId(1),
                revision_no: RevisionNo(1),
            }),
            "inode_revision_is",
        ),
        (
            serde_json::to_value(WalPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: InodeId(1),
            }),
            "ancestors_not_subtree_deleted",
        ),
        (
            serde_json::to_value(WalPrecondition::ChildNameAbsent {
                parent_inode: InodeId(1),
                name_key: "a".to_owned(),
            }),
            "child_name_absent",
        ),
        (
            serde_json::to_value(WalPrecondition::BindingIs {
                parent_inode: InodeId(1),
                name_key: "a".to_owned(),
                child_inode: InodeId(2),
                bind_seq: ChangeSeq(1),
                bind_delta_index: 0,
            }),
            "binding_is",
        ),
        (
            serde_json::to_value(WalPrecondition::DirectoryEmpty {
                inode_id: InodeId(1),
            }),
            "directory_empty",
        ),
    ];
    for (value, expected_tag) in cases {
        let value = value.expect("serialize precondition");
        assert_eq!(value["type"], expected_tag, "in {value}");
    }
}
