//! Frozen grep root and manifest format fixtures.

#![allow(clippy::panic)]

use loonfs_api::wire::envelope::EnvelopeCodecError;
use loonfs_api::wire::sst_blocks::BlockHandle;
use loonfs_api::{ChangeSeq, CheckpointId, IndexSegmentId, InodeId, RunNo};
use loonfs_grep::root::{
    decode_grep_manifest, decode_grep_root, encode_grep_manifest, encode_grep_root,
    GrepEnvelopeCodecError, GrepIndexState, GrepIndexStatus, GrepManifestEnvelope,
    GrepManifestObjectId, GrepManifestState, GrepManifestStateError, GrepReorganizeState,
    GrepRootEnvelope, GrepRootPointer, GrepSegmentRef,
};
use loonfs_test_support::ids::namespace_id;

// Frozen format pins. These byte strings are the durable fixtures: changing
// field names, ordering, enum tags, or omission rules must change them
// deliberately. Together they represent every index status.
//
// Every pre-release grep format stays at version 1. These current bytes pin
// the schema where each phase owns its own watermark.
const BACKFILLING_V1: &str = r#"{"kind":"grep_manifest","format_version":1,"payload_checksum":"sha256:358aea64c965cea7287dd10d1dcf6c75100ae2539b814f6d0dae7043d38fc818","payload":{"namespace_id":"docs","status":{"kind":"backfilling","target_seq":7,"cursor_inode_id":7,"checkpoint_id":"chk_00000000000000000000000000000009"},"index":{"format_version":1,"reorganize":{"snapshot_segment_ids":["idx_00000000000000000000000000000001","idx_00000000000000000000000000000002"],"output_segment_ids":["idx_00000000000000000000000000000003"],"row_key_cursor":"gram-6d6e6f-00000000000000000042","output_level":1,"run_no":3},"next_run_no":4},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_no":1,"run_seq":8,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","object_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},{"segment_id":"idx_00000000000000000000000000000002","run_no":2,"run_seq":9,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"object_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},{"segment_id":"idx_00000000000000000000000000000003","run_no":3,"run_seq":10,"level":1,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"object_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}]}}"#;
const ACTIVE_V1: &str = r#"{"kind":"grep_manifest","format_version":1,"payload_checksum":"sha256:fc23e318c24a9243a1f3a665a2fa4523836ffd3c5ca01f144e21866623a50323","payload":{"namespace_id":"docs","status":{"kind":"active","built_through_seq":11,"next_event_index":5},"index":{"format_version":1,"next_run_no":2},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_no":1,"run_seq":8,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","object_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}]}}"#;
const DISABLED_V1: &str = r#"{"kind":"grep_manifest","format_version":1,"payload_checksum":"sha256:b93963eec6c2f0c2f452c4ea85f50538cf002bf0aa3fe53a16b3cf81892319af","payload":{"namespace_id":"docs","status":{"kind":"disabled"},"index":{"format_version":1,"next_run_no":4},"segments":[]}}"#;
const ADDITIVE_V1: &str = r#"{"kind":"grep_manifest","format_version":1,"payload_checksum":"sha256:3c9820b53c34f61fa2a3516bdacfd43972d1426ef2aada4dab4dfd76ab55e2d0","payload":{"namespace_id":"docs","status":{"kind":"active","built_through_seq":11,"future_status":"ignored"},"index":{"format_version":1,"next_run_no":2,"future_index":17},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_no":1,"run_seq":8,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","object_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","future_segment":"ignored"}],"future_root":true},"future_envelope":{"retained":true}}"#;

// Pointer ids are minted, not derived, so each fixture names an arbitrary
// id and carries the digest of the manifest it points at. That pairing is
// the whole binding between a pointer and its bytes.
const BACKFILLING_MANIFEST_OBJECT_ID: &str = "gmf_1a2b3c4d5e6f70819a2b3c4d5e6f7081";
const ACTIVE_MANIFEST_OBJECT_ID: &str = "gmf_2b3c4d5e6f70819a2b3c4d5e6f708192";
const DISABLED_MANIFEST_OBJECT_ID: &str = "gmf_3c4d5e6f70819a2b3c4d5e6f70819a2b";

const BACKFILLING_POINTER_V1: &str = r#"{"kind":"grep_root","format_version":1,"payload_checksum":"sha256:bce6d9aadc818f1623593c9d29eaf17d0b0250905f273a2aabb219f04d7aef12","payload":{"namespace_id":"docs","manifest_object_id":"gmf_1a2b3c4d5e6f70819a2b3c4d5e6f7081","manifest_payload_checksum":"sha256:358aea64c965cea7287dd10d1dcf6c75100ae2539b814f6d0dae7043d38fc818"}}"#;
const ACTIVE_POINTER_V1: &str = r#"{"kind":"grep_root","format_version":1,"payload_checksum":"sha256:588b936ed66ec60812872776983452c2f398e31393ad520248e91d06913788db","payload":{"namespace_id":"docs","manifest_object_id":"gmf_2b3c4d5e6f70819a2b3c4d5e6f708192","manifest_payload_checksum":"sha256:fc23e318c24a9243a1f3a665a2fa4523836ffd3c5ca01f144e21866623a50323"}}"#;
const DISABLED_POINTER_V1: &str = r#"{"kind":"grep_root","format_version":1,"payload_checksum":"sha256:246f206d2ffe87c601d124ac9c79a31d37e5f35262028fdc4a6178bb96f06e9a","payload":{"namespace_id":"docs","manifest_object_id":"gmf_3c4d5e6f70819a2b3c4d5e6f70819a2b","manifest_payload_checksum":"sha256:b93963eec6c2f0c2f452c4ea85f50538cf002bf0aa3fe53a16b3cf81892319af"}}"#;
const ADDITIVE_POINTER_V1: &str = r#"{"kind":"grep_root","format_version":1,"payload_checksum":"sha256:061e11c2fe0a07ebcc55661e468e6cbd4fc3d89e6cc73d284c4eec5a4efe1c96","payload":{"namespace_id":"docs","manifest_object_id":"gmf_4d5e6f70819a2b3c4d5e6f70819a2b3c","manifest_payload_checksum":"sha256:fc23e318c24a9243a1f3a665a2fa4523836ffd3c5ca01f144e21866623a50323","future_pointer":true},"future_envelope":{"retained":true}}"#;

// The string spelling every grep envelope carried before the version became
// a number, kept only to prove it is refused.
const STRING_VERSION_MANIFEST: &str = r#"{"kind":"grep_manifest","format_version":"v1","payload_checksum":"sha256:b93963eec6c2f0c2f452c4ea85f50538cf002bf0aa3fe53a16b3cf81892319af","payload":{"namespace_id":"docs","status":{"kind":"disabled"},"index":{"format_version":1,"next_run_no":4},"segments":[]}}"#;

#[test]
fn encoded_manifests_and_pointers_match_frozen_bytes() {
    let cases = [
        (sample_backfilling_root(), BACKFILLING_V1),
        (sample_active_root(ChangeSeq(11), 5), ACTIVE_V1),
        (sample_disabled_root(), DISABLED_V1),
    ];

    for (state, expected) in cases {
        let manifest = GrepManifestEnvelope::from_state(state).expect("build manifest envelope");
        let actual =
            String::from_utf8(encode_grep_manifest(&manifest).expect("encode grep manifest"))
                .expect("manifest JSON is UTF-8");
        assert_eq!(actual, expected);

        let (manifest_object_id, expected) = match manifest.manifest_state().status() {
            GrepIndexStatus::Backfilling { .. } => {
                (BACKFILLING_MANIFEST_OBJECT_ID, BACKFILLING_POINTER_V1)
            }
            GrepIndexStatus::Active { .. } => (ACTIVE_MANIFEST_OBJECT_ID, ACTIVE_POINTER_V1),
            GrepIndexStatus::Disabled {} => (DISABLED_MANIFEST_OBJECT_ID, DISABLED_POINTER_V1),
        };
        let pointer = GrepRootEnvelope::from_pointer(GrepRootPointer::new(
            namespace_id("docs"),
            GrepManifestObjectId::parse(manifest_object_id).expect("valid manifest object id"),
            manifest.payload_checksum().to_owned(),
        ))
        .expect("build pointer envelope");
        let actual = String::from_utf8(encode_grep_root(&pointer).expect("encode grep pointer"))
            .expect("pointer JSON is UTF-8");
        assert_eq!(actual, expected);
    }
}

/// A manifest's identity is minted, never read out of its bytes, so two
/// candidates over identical state are two distinct objects.
#[test]
fn identical_manifest_state_mints_distinct_ids() {
    let first = GrepManifestObjectId::generate();
    let second = GrepManifestObjectId::generate();

    assert_ne!(first, second);
    assert!(first.as_str().starts_with("gmf_"));
    assert_eq!(
        GrepManifestObjectId::parse(first.as_str()).expect("a generated id parses"),
        first
    );
}

/// Every phase survives a round trip carrying exactly its own fields, and
/// the encoded bytes never name the other phase's sequence.
#[test]
fn every_status_round_trips_carrying_only_its_own_position() {
    for (state, absent_field) in [
        (sample_backfilling_root(), "built_through_seq"),
        (sample_active_root(ChangeSeq(11), 5), "target_seq"),
        (sample_disabled_root(), "built_through_seq"),
    ] {
        let encoded = encode_grep_manifest(
            &GrepManifestEnvelope::from_state(state.clone()).expect("build manifest envelope"),
        )
        .expect("encode grep manifest");
        assert!(
            !String::from_utf8(encoded.clone())
                .expect("manifest JSON is UTF-8")
                .contains(absent_field),
            "{:?} must not spell `{absent_field}`",
            state.status()
        );
        assert_eq!(
            decode_grep_manifest(&encoded)
                .expect("decode grep manifest")
                .manifest_state(),
            &state
        );
    }
}

/// The manifest spells its lifecycle field `status` and writes a
/// `kind`-tagged object into it, exactly as every durable control object
/// does. The control families are checked the same way in `loonfs-api`.
#[test]
fn the_manifest_status_is_a_kind_tagged_object() {
    for fixture in [BACKFILLING_V1, ACTIVE_V1, DISABLED_V1] {
        let document: serde_json::Value =
            serde_json::from_str(fixture).expect("decode manifest fixture");
        let payload = document["payload"].as_object().expect("object payload");
        assert!(
            !payload.contains_key("lifecycle") && !payload.contains_key("state"),
            "the manifest spells its lifecycle field `status`: {fixture}"
        );
        let tag = payload
            .get("status")
            .expect("the manifest writes a `status`")
            .as_object()
            .expect("the manifest writes `status` as an object")
            .get("kind")
            .expect("the manifest tags its `status` with `kind`");
        assert!(tag.is_string(), "the tag is a string, got {tag}");
    }
}

#[test]
fn immutable_manifest_decoder_reads_frozen_bytes_with_additive_fields() {
    let decoded =
        decode_grep_manifest(ADDITIVE_V1.as_bytes()).expect("decode additive manifest fixture");

    assert_eq!(
        decoded.manifest_state(),
        &sample_active_root(ChangeSeq(11), 0)
    );
}

#[test]
fn mutable_pointer_payload_rejects_unknown_fields_as_corruption() {
    let mut document: serde_json::Value =
        serde_json::from_str(ACTIVE_POINTER_V1).expect("decode pointer fixture");
    document["payload"]["field_from_the_future"] = serde_json::Value::from(true);
    let payload = serde_json::to_string(&document["payload"]).expect("encode edited payload");
    document["payload_checksum"] =
        serde_json::Value::from(loonfs_api::sha256_digest(payload.as_bytes()));
    let edited = format!(
        "{{\"kind\":{},\"format_version\":{},\"payload_checksum\":{},\"payload\":{}}}",
        document["kind"], document["format_version"], document["payload_checksum"], payload,
    );

    assert!(matches!(
        decode_grep_root(edited.as_bytes()),
        Err(GrepEnvelopeCodecError::Envelope(
            EnvelopeCodecError::PayloadDecode(_)
        ))
    ));
}

#[test]
fn mutable_pointer_envelope_rejects_unknown_fields_as_corruption() {
    assert!(matches!(
        decode_grep_root(ADDITIVE_POINTER_V1.as_bytes()),
        Err(GrepEnvelopeCodecError::Envelope(
            EnvelopeCodecError::EnvelopeDecode(_)
        ))
    ));
}

/// The envelope version is a number, like every other durable family's.
/// The string spelling grep used to write is not an older version to be
/// tolerated — it is not a version at all, and the probe says so.
#[test]
fn decoder_rejects_the_string_format_version_without_a_shim() {
    assert!(matches!(
        decode_grep_manifest(STRING_VERSION_MANIFEST.as_bytes()),
        Err(GrepEnvelopeCodecError::Envelope(
            EnvelopeCodecError::EnvelopeDecode(_)
        ))
    ));
    assert!(matches!(
        decode_grep_root(
            STRING_VERSION_MANIFEST
                .replacen("grep_manifest", "grep_root", 1)
                .as_bytes()
        ),
        Err(GrepEnvelopeCodecError::Envelope(
            EnvelopeCodecError::EnvelopeDecode(_)
        ))
    ));
}

#[test]
fn decoder_rejects_unknown_index_version_without_fallback() {
    let mut document: serde_json::Value =
        serde_json::from_str(DISABLED_V1).expect("decode manifest fixture");
    document["payload"]["index"]["format_version"] = serde_json::Value::from(7);
    let payload = serde_json::to_string(&document["payload"]).expect("encode edited payload");
    document["payload_checksum"] =
        serde_json::Value::from(loonfs_api::sha256_digest(payload.as_bytes()));
    let edited = format!(
        "{{\"kind\":{},\"format_version\":{},\"payload_checksum\":{},\"payload\":{}}}",
        document["kind"], document["format_version"], document["payload_checksum"], payload,
    );

    assert!(matches!(
        decode_grep_manifest(edited.as_bytes()),
        Err(GrepEnvelopeCodecError::InvalidState(
            GrepManifestStateError::UnsupportedIndexFormatVersion {
                found: 7,
                supported: 1
            }
        ))
    ));
}

#[test]
fn decoder_rejects_unknown_version_without_fallback() {
    let wrong_version = ACTIVE_V1.replacen("\"format_version\":1", "\"format_version\":7", 1);

    assert!(matches!(
        decode_grep_manifest(wrong_version.as_bytes()),
        Err(GrepEnvelopeCodecError::Envelope(
            EnvelopeCodecError::UnsupportedFormatVersion { kind, found, supported }
        )) if kind == "grep_manifest" && found == 7 && supported == 1
    ));
}

#[test]
fn decoder_rejects_corrupted_checksum() {
    let corrupted = ACTIVE_V1.replacen("\"active\"", "\"disabled\"", 1);

    assert!(matches!(
        decode_grep_manifest(corrupted.as_bytes()),
        Err(GrepEnvelopeCodecError::Envelope(
            EnvelopeCodecError::ChecksumMismatch { .. }
        ))
    ));
}

#[test]
fn decoder_rejects_truncated_payload() {
    let truncated = &ACTIVE_V1.as_bytes()[..ACTIVE_V1.len() - 8];

    assert!(matches!(
        decode_grep_manifest(truncated),
        Err(GrepEnvelopeCodecError::Envelope(
            EnvelopeCodecError::EnvelopeDecode(_)
        ))
    ));
}

#[test]
fn constructor_rejects_reorganization_segment_mismatch() {
    let mut index = GrepIndexState::new(None, RunNo(2));
    index.reorganize = Some(GrepReorganizeState {
        snapshot_segment_ids: vec![segment_id(9)],
        output_segment_ids: Vec::new(),
        row_key_cursor: String::new(),
        output_level: 1,
        run_no: RunNo(1),
    });

    assert!(matches!(
        GrepManifestState::new(
            namespace_id("docs"),
            GrepIndexStatus::Active {
                built_through_seq: ChangeSeq(7),
                next_event_index: 0,
            },
            index,
            vec![segment_ref(1, 1, 0, 0)]
        ),
        Err(GrepManifestStateError::MissingReorganizeSnapshotSegment { .. })
    ));
}

fn sample_backfilling_root() -> GrepManifestState {
    let reorganization = GrepReorganizeState {
        snapshot_segment_ids: vec![segment_id(1), segment_id(2)],
        output_segment_ids: vec![segment_id(3)],
        row_key_cursor: "gram-6d6e6f-00000000000000000042".to_owned(),
        output_level: 1,
        run_no: RunNo(3),
    };
    GrepManifestState::new(
        namespace_id("docs"),
        GrepIndexStatus::Backfilling {
            target_seq: ChangeSeq(7),
            cursor_inode_id: Some(InodeId(7)),
            checkpoint_id: CheckpointId::parse("chk_00000000000000000000000000000009")
                .expect("valid checkpoint id"),
        },
        GrepIndexState::new(Some(reorganization), RunNo(4)),
        vec![
            segment_ref(1, 1, 0, 0),
            segment_ref(2, 2, 0, 0),
            segment_ref(3, 3, 1, 0),
        ],
    )
    .expect("valid backfilling root")
}

fn sample_active_root(built_through_seq: ChangeSeq, next_event_index: u32) -> GrepManifestState {
    GrepManifestState::new(
        namespace_id("docs"),
        GrepIndexStatus::Active {
            built_through_seq,
            next_event_index,
        },
        GrepIndexState::new(None, RunNo(2)),
        vec![segment_ref(1, 1, 0, 0)],
    )
    .expect("valid active root")
}

fn sample_disabled_root() -> GrepManifestState {
    GrepManifestState::new(
        namespace_id("docs"),
        GrepIndexStatus::Disabled {},
        GrepIndexState::new(None, RunNo(4)),
        Vec::new(),
    )
    .expect("valid disabled root")
}

fn segment_ref(number: u8, run_no: u64, level: u32, segment_index: u32) -> GrepSegmentRef {
    GrepSegmentRef {
        segment_id: segment_id(number),
        run_no: RunNo(run_no),
        run_seq: ChangeSeq(7 + u64::from(number)),
        level,
        segment_index,
        min_row_key: "gram-616263-00000000000000000001".to_owned(),
        max_row_key: "gram-7a7a7a-00000000000000000099".to_owned(),
        index_block: BlockHandle {
            offset: 128,
            stored_len: 48,
            decoded_len: 96,
            crc32c: 305_419_896,
        },
        filter_block: BlockHandle {
            offset: 176,
            stored_len: 16,
            decoded_len: 16,
            crc32c: 2_591_069_104,
        },
        filter_inline: (number == 1).then(|| "00112233445566778899aabbccddeeff".to_owned()),
        object_checksum: "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
            .to_owned(),
    }
}

fn segment_id(number: u8) -> IndexSegmentId {
    IndexSegmentId::parse(format!("idx_{number:032x}")).expect("valid segment id")
}
