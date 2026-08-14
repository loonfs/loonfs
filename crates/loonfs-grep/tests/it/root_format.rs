//! Frozen grep root and manifest format fixtures.

#![allow(clippy::panic)]

use loonfs_api::wire::envelope::EnvelopeCodecError;
use loonfs_api::wire::sst_blocks::BlockHandle;
use loonfs_api::{ChangeSeq, CheckpointId, IndexSegmentId, InodeId};
use loonfs_grep::root::{
    decode_grep_manifest, decode_grep_root, encode_grep_manifest, encode_grep_root,
    GrepEnvelopeCodecError, GrepIndexState, GrepLifecycle, GrepManifestEnvelope, GrepManifestId,
    GrepManifestState, GrepManifestStateError, GrepReorganizeState, GrepRootEnvelope,
    GrepRootPointer, GrepSegmentRef,
};
use loonfs_test_support::ids::namespace_id;

// Frozen format pins. These byte strings are the durable fixtures: changing
// field names, ordering, enum tags, or omission rules must change them
// deliberately. Together they represent every lifecycle state.
//
// Every pre-release grep format stays at version 1. These current bytes pin
// the schema where each phase owns its own watermark.
const BACKFILLING_V1: &str = r#"{"kind":"grep_manifest","format_version":1,"payload_checksum":"sha256:de7c0a92722040ae68768f4bbe7250a85ac6dc4eb06dd1485e481d1a6eaae66b","payload":{"namespace_id":"docs","lifecycle":{"kind":"backfilling","target_seq":7,"cursor":7,"checkpoint_id":"chk_00000000000000000000000000000009"},"index":{"format_version":1,"reorganize":{"snapshot_segment_ids":["idx_00000000000000000000000000000001","idx_00000000000000000000000000000002"],"output_segment_ids":["idx_00000000000000000000000000000003"],"row_key_cursor":"gram-6d6e6f-00000000000000000042","output_level":1,"run_ordinal":3},"next_run_ordinal":4},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_seq":8,"run_ordinal":1,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},{"segment_id":"idx_00000000000000000000000000000002","run_seq":9,"run_ordinal":2,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},{"segment_id":"idx_00000000000000000000000000000003","run_seq":10,"run_ordinal":3,"level":1,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}]}}"#;
const ACTIVE_V1: &str = r#"{"kind":"grep_manifest","format_version":1,"payload_checksum":"sha256:78c0d1b0031a4286cf6668202a35d8de8fdfdaef8b97f3b5388d06fb2ea5d7cd","payload":{"namespace_id":"docs","lifecycle":{"kind":"active","built_through_seq":11,"next_event_index":5},"index":{"format_version":1,"next_run_ordinal":2},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_seq":8,"run_ordinal":1,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}]}}"#;
const DISABLED_V1: &str = r#"{"kind":"grep_manifest","format_version":1,"payload_checksum":"sha256:dd73bacf797be51a706ba8737ba11eef2097fd8d547f8024461184ef4a4927c6","payload":{"namespace_id":"docs","lifecycle":{"kind":"disabled"},"index":{"format_version":1,"next_run_ordinal":4},"segments":[]}}"#;
const ADDITIVE_V1: &str = r#"{"kind":"grep_manifest","format_version":1,"payload_checksum":"sha256:0e6089d2807ba8ee47db058cc5ea17a27ff3b78dbd55f20cc4e8f225c817312c","payload":{"namespace_id":"docs","lifecycle":{"kind":"active","built_through_seq":11,"future_lifecycle":"ignored"},"index":{"format_version":1,"next_run_ordinal":2,"future_index":17},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_seq":8,"run_ordinal":1,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","future_segment":"ignored"}],"future_root":true},"future_envelope":{"retained":true}}"#;

// Pointer ids are minted, not derived, so each fixture names an arbitrary
// id and carries the digest of the manifest it points at. That pairing is
// the whole binding between a pointer and its bytes.
const BACKFILLING_MANIFEST_ID: &str = "gmf_1a2b3c4d5e6f70819a2b3c4d5e6f7081";
const ACTIVE_MANIFEST_ID: &str = "gmf_2b3c4d5e6f70819a2b3c4d5e6f708192";
const DISABLED_MANIFEST_ID: &str = "gmf_3c4d5e6f70819a2b3c4d5e6f70819a2b";

const BACKFILLING_POINTER_V1: &str = r#"{"kind":"grep_root","format_version":1,"payload_checksum":"sha256:0606a247e11f23e938081a0ff717d018b27f563ea8f59f9a1b09bfd2f6135563","payload":{"namespace_id":"docs","manifest_id":"gmf_1a2b3c4d5e6f70819a2b3c4d5e6f7081","manifest_payload_checksum":"sha256:de7c0a92722040ae68768f4bbe7250a85ac6dc4eb06dd1485e481d1a6eaae66b"}}"#;
const ACTIVE_POINTER_V1: &str = r#"{"kind":"grep_root","format_version":1,"payload_checksum":"sha256:a2022cd5caef40768e90d5df381f2b1f36cc1e0617b70619fd9a10f4efd08e1a","payload":{"namespace_id":"docs","manifest_id":"gmf_2b3c4d5e6f70819a2b3c4d5e6f708192","manifest_payload_checksum":"sha256:78c0d1b0031a4286cf6668202a35d8de8fdfdaef8b97f3b5388d06fb2ea5d7cd"}}"#;
const DISABLED_POINTER_V1: &str = r#"{"kind":"grep_root","format_version":1,"payload_checksum":"sha256:3aa5e6a5f1be03c8f70581f6d5992c4b350188b55af2c7c03544935031cb850d","payload":{"namespace_id":"docs","manifest_id":"gmf_3c4d5e6f70819a2b3c4d5e6f70819a2b","manifest_payload_checksum":"sha256:dd73bacf797be51a706ba8737ba11eef2097fd8d547f8024461184ef4a4927c6"}}"#;
const ADDITIVE_POINTER_V1: &str = r#"{"kind":"grep_root","format_version":1,"payload_checksum":"sha256:cbe44cbeee7aa8159062e46671dd576660a2bb2e434830348621af5d51ebb6f7","payload":{"namespace_id":"docs","manifest_id":"gmf_4d5e6f70819a2b3c4d5e6f70819a2b3c","manifest_payload_checksum":"sha256:78c0d1b0031a4286cf6668202a35d8de8fdfdaef8b97f3b5388d06fb2ea5d7cd","future_pointer":true},"future_envelope":{"retained":true}}"#;

// The string spelling every grep envelope carried before the version became
// a number, kept only to prove it is refused.
const STRING_VERSION_MANIFEST: &str = r#"{"kind":"grep_manifest","format_version":"v1","payload_checksum":"sha256:dd73bacf797be51a706ba8737ba11eef2097fd8d547f8024461184ef4a4927c6","payload":{"namespace_id":"docs","lifecycle":{"kind":"disabled"},"index":{"format_version":1,"next_run_ordinal":4},"segments":[]}}"#;

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

        let (manifest_id, expected) = match manifest.manifest_state().lifecycle() {
            GrepLifecycle::Backfilling { .. } => (BACKFILLING_MANIFEST_ID, BACKFILLING_POINTER_V1),
            GrepLifecycle::Active { .. } => (ACTIVE_MANIFEST_ID, ACTIVE_POINTER_V1),
            GrepLifecycle::Disabled => (DISABLED_MANIFEST_ID, DISABLED_POINTER_V1),
        };
        let pointer = GrepRootEnvelope::from_pointer(GrepRootPointer::new(
            namespace_id("docs"),
            GrepManifestId::parse(manifest_id).expect("valid manifest id"),
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
    let first = GrepManifestId::generate();
    let second = GrepManifestId::generate();

    assert_ne!(first, second);
    assert!(first.as_str().starts_with("gmf_"));
    assert_eq!(
        GrepManifestId::parse(first.as_str()).expect("a generated id parses"),
        first
    );
}

/// Every phase survives a round trip carrying exactly its own fields, and
/// the encoded bytes never name the other phase's sequence.
#[test]
fn every_lifecycle_phase_round_trips_carrying_only_its_own_position() {
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
            state.lifecycle()
        );
        assert_eq!(
            decode_grep_manifest(&encoded)
                .expect("decode grep manifest")
                .manifest_state(),
            &state
        );
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
fn constructor_rejects_fold_segment_mismatch() {
    let mut index = GrepIndexState::new(None, 2);
    index.reorganize = Some(GrepReorganizeState {
        snapshot_segment_ids: vec![segment_id(9)],
        output_segment_ids: Vec::new(),
        row_key_cursor: String::new(),
        output_level: 1,
        run_ordinal: 1,
    });

    assert!(matches!(
        GrepManifestState::new(
            namespace_id("docs"),
            GrepLifecycle::Active {
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
    let fold = GrepReorganizeState {
        snapshot_segment_ids: vec![segment_id(1), segment_id(2)],
        output_segment_ids: vec![segment_id(3)],
        row_key_cursor: "gram-6d6e6f-00000000000000000042".to_owned(),
        output_level: 1,
        run_ordinal: 3,
    };
    GrepManifestState::new(
        namespace_id("docs"),
        GrepLifecycle::Backfilling {
            target_seq: ChangeSeq(7),
            cursor: Some(InodeId(7)),
            checkpoint_id: CheckpointId::parse("chk_00000000000000000000000000000009")
                .expect("valid checkpoint id"),
        },
        GrepIndexState::new(Some(fold), 4),
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
        GrepLifecycle::Active {
            built_through_seq,
            next_event_index,
        },
        GrepIndexState::new(None, 2),
        vec![segment_ref(1, 1, 0, 0)],
    )
    .expect("valid active root")
}

fn sample_disabled_root() -> GrepManifestState {
    GrepManifestState::new(
        namespace_id("docs"),
        GrepLifecycle::Disabled,
        GrepIndexState::new(None, 4),
        Vec::new(),
    )
    .expect("valid disabled root")
}

fn segment_ref(number: u8, run_ordinal: u64, level: u32, segment_index: u32) -> GrepSegmentRef {
    GrepSegmentRef {
        segment_id: segment_id(number),
        run_seq: ChangeSeq(7 + u64::from(number)),
        run_ordinal,
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
        payload_checksum: "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
            .to_owned(),
    }
}

fn segment_id(number: u8) -> IndexSegmentId {
    IndexSegmentId::parse(format!("idx_{number:032x}")).expect("valid segment id")
}
