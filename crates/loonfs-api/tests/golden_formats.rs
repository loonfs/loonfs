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
//! - Additive payload fields must keep decoding (`*_tolerates_additive_*`):
//!   that is the format's only same-version evolution mechanism, made
//!   possible by checksumming the stored payload bytes rather than a
//!   re-encoding.

use loonfs_api::wire::control::{
    decode_control_object, encode_control_object, CheckpointOwner, CheckpointRecordLifecycle,
    CheckpointRecordState, CompletedUpload, ContentStoreDescriptorState, ControlObjectEnvelope,
    ControlObjectKind, HeadState, MetadataRootState, NamespaceConfigState, NamespaceState,
    UploadSessionLifecycle, UploadSessionState, WalFloorState, WalSegmentPointer, WriterBlock,
};
use loonfs_api::wire::envelope::EnvelopeCodecError;
use loonfs_api::wire::manifest::{
    decode_namespace_manifest_json, encode_namespace_manifest_json, MetadataFileRef, MetadataRow,
    MetadataTableFamily, NamespaceManifestEnvelope, NamespaceManifestFork,
    NamespaceManifestPayload, TombstoneRowAction,
};
use loonfs_api::wire::wal::{
    decode_wal_segment_envelope_zstd, encode_wal_segment_envelope_zstd, WalCommitDelta,
    WalCommitPayload, WalDelta, WalSegmentEnvelope, WalSegmentPayload,
};
use loonfs_api::{
    sha256_digest, v0::UploadMode, ChangeSeq, CheckpointId, CommitId, ContentRef, ContentStoreId,
    InodeId, InodeKind, ManifestId, ManifestObjectId, MetadataTableId, NameKey, NamePolicy,
    NamespaceId, RevisionNo, UploadId, WalSegmentId, WriterEpoch,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
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
             while pre-release, regenerate the version-1 fixture with UPDATE_GOLDEN=1 \
             if the change is intentional.",
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

fn checkpoint_id(value: &str) -> CheckpointId {
    CheckpointId::parse(value).expect("valid checkpoint id")
}

fn manifest_object_id(manifest_id: u64, suffix: &str) -> ManifestObjectId {
    ManifestObjectId::parse(format!("{manifest_id:020}-{suffix}"))
        .expect("valid manifest object id")
}

fn sample_content_ref() -> ContentRef {
    ContentRef::whole_file_v0(b"golden bytes")
}

fn table_id() -> MetadataTableId {
    MetadataTableId::parse("tbl_0123456789abcdef0123456789abcdef").expect("valid table id")
}

fn name_key(value: &str) -> NameKey {
    NameKey::parse(value).expect("valid name key")
}

fn sample_wal_pointer() -> WalSegmentPointer {
    WalSegmentPointer {
        object_key: "namespaces/demo/wal/00000000000000000002-fedcba9876543210.wal.zst".to_owned(),
        segment_id: WalSegmentId::parse("00000000000000000002-fedcba9876543210")
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
                display_name: "Docs".to_owned(),
                child_inode_id: InodeId(7),
            },
        },
        WalCommitDelta {
            semantic_op_index: 1,
            delta: WalDelta::UnbindDirentry {
                delta_index: 2,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("old.txt").expect("valid name key"),
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
            },
        },
    ];
    WalSegmentEnvelope::from_payload(
        WRITER_VERSION,
        WalSegmentPayload {
            namespace_id: namespace_id(),
            segment_id: WalSegmentId::parse("00000000000000000001-0123456789abcdef")
                .expect("valid segment id"),
            writer_epoch: WriterEpoch(3),
            prev_visible_segment: Some(sample_wal_pointer()),
            base_head_seq: ChangeSeq(1),
            start_seq: ChangeSeq(2),
            end_seq: ChangeSeq(2),
            records: vec![WalCommitPayload {
                seq: ChangeSeq(2),
                commit_id: commit_id(),
                semantic_commit_fingerprint:
                    "v0:sha256:0000000000000000000000000000000000000000000000000000000000000042"
                        .to_owned(),
                committed_at_ms: 4_000,
                message: Some("golden commit".to_owned()),
                deltas,
            }],
        },
    )
    .expect("wal envelope")
}

fn sample_manifest_envelope() -> NamespaceManifestEnvelope {
    NamespaceManifestEnvelope::from_payload(
        WRITER_VERSION,
        NamespaceManifestPayload {
            namespace_id: namespace_id(),
            manifest_id: ManifestId(2),
            manifest_object_id: manifest_object_id(2, "0123456789abcdef"),
            head_seq: ChangeSeq(2),
            head_commit_id: commit_id(),
            base_seq: ChangeSeq(2),
            writer_epoch: WriterEpoch(3),
            next_inode_id: InodeId(10),
            retention_floor_seq: ChangeSeq(0),
            fork: Some(NamespaceManifestFork {
                source_namespace_id: NamespaceId::parse("source").expect("valid namespace id"),
                fork_seq: ChangeSeq(2),
                source_checkpoint_id: checkpoint_id("chk_00000000000000000000000000000001"),
                source_manifest_id: ManifestId(1),
                source_manifest_object_id: manifest_object_id(1, "0123456789abcdef"),
                source_head_seq: ChangeSeq(2),
            }),
            metadata_files: vec![MetadataFileRef {
                owner_namespace_id: namespace_id(),
                table_id: table_id(),
                object_key:
                    "namespaces/demo/metadata/tables/tbl_0123456789abcdef0123456789abcdef.sst.zst"
                        .to_owned(),
                run_seq: ChangeSeq(2),
                level: 0,
                family: MetadataTableFamily::Inodes,
                segment_index: 0,
                row_count: 6,
                min_key: "commit-receipt".to_owned(),
                max_key: "tombstone".to_owned(),
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
        writer_epoch: WriterEpoch(3),
        writer: Some(WriterBlock {
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_00000000000000000000000000000001".to_owned(),
            acquired_at_ms: 2_000,
        }),
        next_inode_id: InodeId(10),
        visible_wal_tip: Some(sample_wal_pointer()),
        recent_segments: vec![sample_wal_pointer()],
        state: NamespaceState::Active,
    }
}

fn sample_deleted_head_state() -> HeadState {
    HeadState {
        state: NamespaceState::Deleted,
        ..sample_head_state()
    }
}

fn sample_condemned_head_state() -> HeadState {
    HeadState {
        state: NamespaceState::Condemned,
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
    let condemned =
        serde_json::to_string(&sample_condemned_head_state()).expect("encode condemned");
    assert!(condemned.contains("\"state\":\"condemned\""));
}

#[test]
fn control_objects_match_golden_bytes() {
    check_control_golden(
        "control_namespace_head.v1.json",
        ControlObjectKind::WalHead,
        sample_head_state(),
    );
    check_control_golden(
        "control_namespace_head.deleted.v1.json",
        ControlObjectKind::WalHead,
        sample_deleted_head_state(),
    );
    check_control_golden(
        "control_namespace_head.condemned.v1.json",
        ControlObjectKind::WalHead,
        sample_condemned_head_state(),
    );
    check_control_golden(
        "control_namespace_descriptor.v1.json",
        ControlObjectKind::NamespaceConfig,
        NamespaceConfigState {
            namespace_id: namespace_id(),
            content_store_id: content_store_id(),
            name_policy: NamePolicy::default(),
        },
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
            manifest_id: ManifestId(2),
            manifest_object_id: manifest_object_id(2, "0123456789abcdef"),
            manifest_head_seq: ChangeSeq(2),
            manifest_payload_checksum:
                "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_owned(),
            updated_at_ms: 3_000,
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
        "control_checkpoint_record.v1.json",
        ControlObjectKind::CheckpointRecord,
        CheckpointRecordState {
            checkpoint_id: checkpoint_id("chk_00000000000000000000000000000002"),
            namespace_id: namespace_id(),
            manifest_id: ManifestId(2),
            manifest_object_id: manifest_object_id(2, "0123456789abcdef"),
            manifest_head_seq: ChangeSeq(2),
            manifest_payload_checksum:
                "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_owned(),
            head_commit_id: commit_id(),
            created_at_ms: 3_000,
            expires_at_ms: None,
            owner: CheckpointOwner::User {
                name: "nightly".to_owned(),
            },
            state: CheckpointRecordLifecycle::Active,
        },
    );
    // The fork owner is a durable encoding of its own: the tagged `owner`
    // changes the document.
    check_control_golden(
        "control_checkpoint_record_fork.v1.json",
        ControlObjectKind::CheckpointRecord,
        CheckpointRecordState {
            checkpoint_id: checkpoint_id("chk_00000000000000000000000000000004"),
            namespace_id: namespace_id(),
            manifest_id: ManifestId(4),
            manifest_object_id: manifest_object_id(4, "0123456789abcdef"),
            manifest_head_seq: ChangeSeq(4),
            manifest_payload_checksum:
                "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_owned(),
            head_commit_id: commit_id(),
            created_at_ms: 3_000,
            expires_at_ms: None,
            owner: CheckpointOwner::Fork {
                target_namespace_id: NamespaceId::parse("clone").expect("valid namespace id"),
            },
            state: CheckpointRecordLifecycle::Active,
        },
    );
    check_control_golden(
        "control_upload_session.v1.json",
        ControlObjectKind::UploadSession,
        UploadSessionState {
            namespace_id: namespace_id(),
            upload_id: UploadId::parse("upl_0123456789abcdef0123456789abcdef")
                .expect("valid upload id"),
            mode: UploadMode::ServiceProxied,
            direct_put_content_ref: None,
            staged_content_ref: Some(sample_content_ref()),
            completed: Some(CompletedUpload {
                content_ref: sample_content_ref(),
            }),
            created_at_ms: 1_000,
            state: UploadSessionLifecycle::Active,
        },
    );
    // The released lifecycle and the direct-put session shape are durable
    // encodings of their own: `state` and `mode` change the document.
    check_control_golden(
        "control_checkpoint_record_released.v1.json",
        ControlObjectKind::CheckpointRecord,
        CheckpointRecordState {
            checkpoint_id: checkpoint_id("chk_00000000000000000000000000000003"),
            namespace_id: namespace_id(),
            manifest_id: ManifestId(3),
            manifest_object_id: manifest_object_id(3, "0123456789abcdef"),
            manifest_head_seq: ChangeSeq(3),
            manifest_payload_checksum:
                "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_owned(),
            head_commit_id: commit_id(),
            created_at_ms: 3_000,
            expires_at_ms: Some(9_000),
            owner: CheckpointOwner::User {
                name: "nightly".to_owned(),
            },
            state: CheckpointRecordLifecycle::Released,
        },
    );
    check_control_golden(
        "control_checkpoint_record_condemned.v1.json",
        ControlObjectKind::CheckpointRecord,
        CheckpointRecordState {
            checkpoint_id: checkpoint_id("chk_00000000000000000000000000000005"),
            namespace_id: namespace_id(),
            manifest_id: ManifestId(5),
            manifest_object_id: manifest_object_id(5, "0123456789abcdef"),
            manifest_head_seq: ChangeSeq(5),
            manifest_payload_checksum:
                "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_owned(),
            head_commit_id: commit_id(),
            created_at_ms: 3_000,
            expires_at_ms: None,
            owner: CheckpointOwner::User {
                name: "nightly".to_owned(),
            },
            state: CheckpointRecordLifecycle::Condemned,
        },
    );
    check_control_golden(
        "control_upload_session_direct_put.v1.json",
        ControlObjectKind::UploadSession,
        UploadSessionState {
            namespace_id: namespace_id(),
            upload_id: UploadId::parse("upl_abcdef0123456789abcdef0123456789")
                .expect("valid upload id"),
            mode: UploadMode::DirectPut,
            direct_put_content_ref: Some(sample_content_ref()),
            staged_content_ref: None,
            completed: None,
            created_at_ms: 1_000,
            state: UploadSessionLifecycle::Active,
        },
    );
    check_control_golden(
        "control_upload_session_condemned.v1.json",
        ControlObjectKind::UploadSession,
        UploadSessionState {
            namespace_id: namespace_id(),
            upload_id: UploadId::parse("upl_11111111111111111111111111111111")
                .expect("valid upload id"),
            mode: UploadMode::ServiceProxied,
            direct_put_content_ref: None,
            staged_content_ref: None,
            completed: None,
            created_at_ms: 1_000,
            state: UploadSessionLifecycle::Condemned,
        },
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
                manifest_id: ManifestId(5),
                manifest_object_id: manifest_object_id(5, "0123456789abcdef"),
                manifest_head_seq: ChangeSeq(5),
                manifest_payload_checksum: sha256_digest(b"manifest"),
                head_commit_id: commit_id(),
                created_at_ms: 3_000,
                expires_at_ms: None,
                owner: CheckpointOwner::User {
                    name: "nightly".to_owned(),
                },
                state: CheckpointRecordLifecycle::Condemned,
            })
            .expect("checkpoint state"),
        ),
        (
            ControlObjectKind::UploadSession,
            serde_json::to_value(UploadSessionState {
                namespace_id: namespace_id(),
                upload_id: UploadId::parse("upl_11111111111111111111111111111111")
                    .expect("valid upload id"),
                mode: UploadMode::ServiceProxied,
                direct_put_content_ref: None,
                staged_content_ref: None,
                completed: None,
                created_at_ms: 1_000,
                state: UploadSessionLifecycle::Condemned,
            })
            .expect("upload state"),
        ),
    ];
    for (kind, state) in cases {
        let envelope = ControlObjectEnvelope::from_state(kind, WRITER_VERSION, state)
            .expect("control envelope");
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
                supported: 1,
                ..
            }
        ));
    }
}

#[test]
fn metadata_table_family_wire_tags_are_pinned() {
    let tags: Vec<String> = [
        MetadataTableFamily::Inodes,
        MetadataTableFamily::DirentryBinds,
        MetadataTableFamily::DirentryChildBinds,
        MetadataTableFamily::DirentryUnbinds,
        MetadataTableFamily::Revisions,
        MetadataTableFamily::RevisionsByInodeDesc,
        MetadataTableFamily::Tombstones,
        MetadataTableFamily::CommitReceipts,
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
            "\"commit_receipts\"",
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
        display_name: "Docs".to_owned(),
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
        ControlObjectKind::WalHead,
        WRITER_VERSION,
        sample_head_state(),
    )
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
                inode_kind: InodeKind::Directory,
            }),
            "create_inode",
        ),
        (
            serde_json::to_value(WalDelta::BindDirentry {
                delta_index: 0,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("a").expect("valid name key"),
                display_name: "a".to_owned(),
                child_inode_id: InodeId(2),
            }),
            "bind_direntry",
        ),
        (
            serde_json::to_value(WalDelta::UnbindDirentry {
                delta_index: 0,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("a").expect("valid name key"),
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
            }),
            "tombstone_subtree",
        ),
        (
            serde_json::to_value(WalDelta::RevokeSubtreeTombstone {
                delta_index: 0,
                root_inode_id: InodeId(2),
                target_seq: ChangeSeq(1),
                target_delta_index: 1,
            }),
            "revoke_subtree_tombstone",
        ),
    ];
    for (value, expected_tag) in cases {
        let value = value.expect("serialize delta");
        assert_eq!(value["kind"], expected_tag, "in {value}");
    }
}

// ---------------------------------------------------------------------------
// Metadata SST blocks
// ---------------------------------------------------------------------------

fn sample_segment_blocks() -> loonfs_api::wire::sst_blocks::BuiltSegmentBlocks {
    use loonfs_api::wire::sst_blocks::SegmentBlocksBuilder;
    // A tiny target block size forces several data blocks, so the fixture
    // pins block splitting, restart points, and the index shape at once.
    let mut builder = SegmentBlocksBuilder::new(256);
    let rows = [
        MetadataRow::CommitReceipt {
            commit_id: commit_id(),
            semantic_commit_fingerprint: "fp:golden".to_owned(),
            committed_seq: ChangeSeq(9),
            committed_at_ms: 9_000,
            message: None,
        },
        MetadataRow::DirentryBind {
            parent_inode_id: InodeId(1),
            name_key: name_key("docs"),
            display_name: "docs".to_owned(),
            child_inode_id: InodeId(2),
            bind_seq: ChangeSeq(3),
            bind_delta_index: 0,
        },
        MetadataRow::DirentryBind {
            parent_inode_id: InodeId(1),
            name_key: name_key("docs-archive"),
            display_name: "docs-archive".to_owned(),
            child_inode_id: InodeId(5),
            bind_seq: ChangeSeq(6),
            bind_delta_index: 0,
        },
        MetadataRow::DirentryUnbind {
            parent_inode_id: InodeId(1),
            name_key: name_key("docs-archive"),
            child_inode_id: InodeId(5),
            bind_seq: ChangeSeq(6),
            bind_delta_index: 0,
            unbind_seq: ChangeSeq(8),
            unbind_delta_index: 0,
        },
        MetadataRow::Inode {
            inode_id: InodeId(1),
            inode_kind: InodeKind::Directory,
            created_seq: ChangeSeq(1),
        },
        MetadataRow::Inode {
            inode_id: InodeId(2),
            inode_kind: InodeKind::File,
            created_seq: ChangeSeq(3),
        },
        MetadataRow::Revision {
            inode_id: InodeId(2),
            revision_no: RevisionNo(1),
            committed_seq: ChangeSeq(3),
            committed_at_ms: 3_000,
            revision_delta_index: 0,
            content_ref: sample_content_ref(),
        },
        MetadataRow::Tombstone {
            root_inode_id: InodeId(5),
            tombstone_seq: ChangeSeq(8),
            tombstone_delta_index: 0,
            action: TombstoneRowAction::Set,
        },
        MetadataRow::Tombstone {
            root_inode_id: InodeId(5),
            tombstone_seq: ChangeSeq(9),
            tombstone_delta_index: 0,
            action: TombstoneRowAction::Revoke {
                target_seq: ChangeSeq(8),
                target_delta_index: 0,
            },
        },
    ];
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

#[test]
fn sst_block_data_payload_matches_golden_bytes() {
    use loonfs_api::wire::sst_blocks::decode_index_block;
    let built = sample_segment_blocks();
    let index = decode_index_block(segment_section(&built.bytes, &built.index), &built.index)
        .expect("decode index");
    assert!(index.len() > 1, "sample should span several blocks");
    // Compare the decompressed block payload: zstd frames may differ across
    // zstd versions, the entry encoding (which the format defines) may not.
    assert_matches_golden(
        "sst_block_data.v1.bin",
        &unzstd(segment_section(&built.bytes, &index[0].block)),
    );
}

#[test]
fn sst_block_data_golden_decodes_to_sample_rows() {
    use loonfs_api::wire::sst_blocks::{decode_data_block, BlockHandle};
    let payload = read_golden("sst_block_data.v1.bin");
    let stored = rezstd(&payload);
    let handle = BlockHandle {
        offset: 0,
        stored_len: stored.len() as u32,
        decoded_len: payload.len() as u32,
        crc32c: crc32c::crc32c(&stored),
    };
    let block = decode_data_block(&stored, &handle).expect("decode golden data block");
    assert!(!block.rows.is_empty());
    assert_eq!(block.row_keys[0], block.rows[0].row_key());
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
        last_key: "inode-00000000000000000042".to_owned(),
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
