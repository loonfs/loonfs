//! Frozen grep root and manifest format fixtures.

#![allow(clippy::panic)]

use loonfs_api::wire::sst_blocks::BlockHandle;
use loonfs_api::{ChangeSeq, CheckpointId, IndexSegmentId, InodeId};
use loonfs_grep::root::{
    decode_grep_manifest, decode_grep_root, encode_grep_manifest, encode_grep_root, GrepIndexState,
    GrepLifecycle, GrepManifestEnvelope, GrepReorganizeState, GrepRootCodecError, GrepRootEnvelope,
    GrepRootPointer, GrepRootState, GrepRootStateError, GrepSegmentRef,
};
use loonfs_test_support::ids::namespace_id;

// Frozen format pins. These byte strings are the durable fixtures: changing
// field names, ordering, enum tags, or omission rules must change them
// deliberately. Together they represent every lifecycle state.
//
// The envelope is still `v1`; what version 2 changed is the index state
// nested inside it, which moved each phase's own watermark into the phase.
const BACKFILLING_V2: &str = r#"{"kind":"grep_manifest","format_version":"v1","payload_checksum":"sha256:5002cd1db3aef16bdc6cc413185add75a3d392f66f80f51a54e37278d413bbdf","payload":{"namespace_id":"docs","lifecycle":{"kind":"backfilling","target_seq":7,"cursor":7,"checkpoint_id":"chk_00000000000000000000000000000009"},"index":{"format_version":2,"reorganize":{"snapshot_segment_ids":["idx_00000000000000000000000000000001","idx_00000000000000000000000000000002"],"output_segment_ids":["idx_00000000000000000000000000000003"],"row_key_cursor":"gram-6d6e6f-00000000000000000042","output_level":1,"run_ordinal":3},"next_run_ordinal":4},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_seq":8,"run_ordinal":1,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},{"segment_id":"idx_00000000000000000000000000000002","run_seq":9,"run_ordinal":2,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},{"segment_id":"idx_00000000000000000000000000000003","run_seq":10,"run_ordinal":3,"level":1,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}]}}"#;
const STEADY_V2: &str = r#"{"kind":"grep_manifest","format_version":"v1","payload_checksum":"sha256:b1890700a91365f8f7663705f662387f913ab9e05571640cd2eff792da8cacff","payload":{"namespace_id":"docs","lifecycle":{"kind":"steady","built_through_seq":11,"next_event_index":5},"index":{"format_version":2,"next_run_ordinal":2},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_seq":8,"run_ordinal":1,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}]}}"#;
const DISABLED_V2: &str = r#"{"kind":"grep_manifest","format_version":"v1","payload_checksum":"sha256:cad95bd09384a8d651c38a9070dfc30cb94276edb752c5438d5646a687c9bbdc","payload":{"namespace_id":"docs","lifecycle":{"kind":"disabled"},"index":{"format_version":2,"next_run_ordinal":4},"segments":[]}}"#;
const ADDITIVE_V2: &str = r#"{"kind":"grep_manifest","format_version":"v1","payload_checksum":"sha256:f015075bf0949612b508d5d3a765028264cdbbcb01be060bd997c4d1904621b2","payload":{"namespace_id":"docs","lifecycle":{"kind":"steady","built_through_seq":11,"future_lifecycle":"ignored"},"index":{"format_version":2,"next_run_ordinal":2,"future_index":17},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_seq":8,"run_ordinal":1,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","future_segment":"ignored"}],"future_root":true},"future_envelope":{"retained":true}}"#;

const BACKFILLING_POINTER_V2: &str = r#"{"kind":"grep_root","format_version":"v1","payload_checksum":"sha256:c5573e40ba68f25bb977c0527efad030fa92eaa6c8303308ce6af5c7a4f6bc5a","payload":{"namespace_id":"docs","manifest_id":"5002cd1db3aef16bdc6cc413185add75a3d392f66f80f51a54e37278d413bbdf"}}"#;
const STEADY_POINTER_V2: &str = r#"{"kind":"grep_root","format_version":"v1","payload_checksum":"sha256:ca61e2f34ae4808e7a8de27df7457c3d477d8374c35178982d65512e4a891c74","payload":{"namespace_id":"docs","manifest_id":"b1890700a91365f8f7663705f662387f913ab9e05571640cd2eff792da8cacff"}}"#;
const DISABLED_POINTER_V2: &str = r#"{"kind":"grep_root","format_version":"v1","payload_checksum":"sha256:a40f6d8acfcc8a1c11bc625ab46ab782885deb521277943e4ced2a5d74c4ef46","payload":{"namespace_id":"docs","manifest_id":"cad95bd09384a8d651c38a9070dfc30cb94276edb752c5438d5646a687c9bbdc"}}"#;
const ADDITIVE_POINTER_V1: &str = r#"{"kind":"grep_root","format_version":"v1","payload_checksum":"sha256:b09014e32fe81501ef34ca037c33ad6b3cbdf4424a16bd376462f84dd2d1b4bc","payload":{"namespace_id":"docs","manifest_id":"9b347bb59f8d589465bddb1104e57be2d6a3babfe8b54d0fc8721b91f1a8b6ad","future_pointer":true},"future_envelope":{"retained":true}}"#;

// Version-1 index states, kept only to prove they are refused. Each spelled
// a backfill's target and a steady index's progress with one field, so a
// decoder that accepted them could not tell which meaning it had read.
const BACKFILLING_INDEX_V1: &str = r#"{"kind":"grep_manifest","format_version":"v1","payload_checksum":"sha256:a629f619356fda3c1d5789df1082221494bdec8ae182b6883748c73346a0fae9","payload":{"namespace_id":"docs","lifecycle":{"kind":"backfilling","backfill_cursor":7,"checkpoint_id":"chk_00000000000000000000000000000009"},"index":{"format_version":1,"built_through_seq":7,"reorganize":{"snapshot_segment_ids":["idx_00000000000000000000000000000001","idx_00000000000000000000000000000002"],"output_segment_ids":["idx_00000000000000000000000000000003"],"row_key_cursor":"gram-6d6e6f-00000000000000000042","output_level":1,"run_ordinal":3},"next_run_ordinal":4},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_seq":8,"run_ordinal":1,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},{"segment_id":"idx_00000000000000000000000000000002","run_seq":9,"run_ordinal":2,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},{"segment_id":"idx_00000000000000000000000000000003","run_seq":10,"run_ordinal":3,"level":1,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}]}}"#;
const STEADY_INDEX_V1: &str = r#"{"kind":"grep_manifest","format_version":"v1","payload_checksum":"sha256:c7d26cf67191665af1c6985ad742fa9ef36cfe635a6577b4e02d79e112fe02cf","payload":{"namespace_id":"docs","lifecycle":{"kind":"steady"},"index":{"format_version":1,"built_through_seq":11,"next_event_index":5,"next_run_ordinal":2},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_seq":8,"run_ordinal":1,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}]}}"#;
const DISABLED_INDEX_V1: &str = r#"{"kind":"grep_manifest","format_version":"v1","payload_checksum":"sha256:70b4ba748d1ddffcb28baa05e0874993a4650007e09bf32c85c215ee298baf6c","payload":{"namespace_id":"docs","lifecycle":{"kind":"disabled"},"index":{"format_version":1,"built_through_seq":15,"next_run_ordinal":4},"segments":[]}}"#;

#[test]
fn encoded_manifests_and_pointers_match_frozen_bytes() {
    let cases = [
        (sample_backfilling_root(), BACKFILLING_V2),
        (sample_steady_root(ChangeSeq(11), 5), STEADY_V2),
        (sample_disabled_root(), DISABLED_V2),
    ];

    for (state, expected) in cases {
        let manifest = GrepManifestEnvelope::from_state(state).expect("build manifest envelope");
        let actual =
            String::from_utf8(encode_grep_manifest(&manifest).expect("encode grep manifest"))
                .expect("manifest JSON is UTF-8");
        assert_eq!(actual, expected);

        let pointer = GrepRootEnvelope::from_pointer(GrepRootPointer::new(
            namespace_id("docs"),
            manifest.manifest_id().clone(),
        ))
        .expect("build pointer envelope");
        let actual = String::from_utf8(encode_grep_root(&pointer).expect("encode grep pointer"))
            .expect("pointer JSON is UTF-8");
        let expected = match manifest.state().lifecycle() {
            GrepLifecycle::Backfilling { .. } => BACKFILLING_POINTER_V2,
            GrepLifecycle::Steady { .. } => STEADY_POINTER_V2,
            GrepLifecycle::Disabled => DISABLED_POINTER_V2,
        };
        assert_eq!(actual, expected);
    }
}

/// Every phase survives a round trip carrying exactly its own fields, and
/// the encoded bytes never name the other phase's sequence.
#[test]
fn every_lifecycle_phase_round_trips_carrying_only_its_own_position() {
    for (state, absent_field) in [
        (sample_backfilling_root(), "built_through_seq"),
        (sample_steady_root(ChangeSeq(11), 5), "target_seq"),
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
                .state(),
            &state
        );
    }
}

#[test]
fn immutable_manifest_decoder_reads_frozen_bytes_with_additive_fields() {
    let decoded =
        decode_grep_manifest(ADDITIVE_V2.as_bytes()).expect("decode additive manifest fixture");

    assert_eq!(decoded.state(), &sample_steady_root(ChangeSeq(11), 0));
}

#[test]
fn mutable_pointer_payload_rejects_unknown_fields_as_corruption() {
    let mut document: serde_json::Value =
        serde_json::from_str(STEADY_POINTER_V2).expect("decode pointer fixture");
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
        Err(GrepRootCodecError::PayloadDecode { .. })
    ));
}

#[test]
fn mutable_pointer_envelope_rejects_unknown_fields_as_corruption() {
    assert!(matches!(
        decode_grep_root(ADDITIVE_POINTER_V1.as_bytes()),
        Err(GrepRootCodecError::EnvelopeDecode { .. })
    ));
}

/// Version 1 is refused outright, with no shim and no salvage.
///
/// Its `built_through_seq` meant the backfill's target in one phase and real
/// indexed progress in another, and nothing in the bytes says which. A
/// disabled root — whose lifecycle happens to still parse — proves the
/// version guard itself does the refusing, not just the changed field
/// shapes.
#[test]
fn decoder_rejects_version_one_index_state_without_a_shim() {
    assert!(matches!(
        decode_grep_manifest(DISABLED_INDEX_V1.as_bytes()),
        Err(GrepRootCodecError::InvalidState(
            GrepRootStateError::UnsupportedIndexFormatVersion {
                found: 1,
                supported: 2
            }
        ))
    ));

    // The other two cannot even be read into the current shape: their
    // phases no longer carry the fields version 1 put beside them.
    for fixture in [BACKFILLING_INDEX_V1, STEADY_INDEX_V1] {
        assert!(matches!(
            decode_grep_manifest(fixture.as_bytes()),
            Err(GrepRootCodecError::PayloadDecode { .. })
        ));
    }
}

#[test]
fn decoder_rejects_unknown_version_without_fallback() {
    let wrong_version =
        STEADY_V2.replacen("\"format_version\":\"v1\"", "\"format_version\":\"v7\"", 1);

    assert!(matches!(
        decode_grep_manifest(wrong_version.as_bytes()),
        Err(GrepRootCodecError::UnsupportedFormatVersion { found, supported })
            if found == "v7" && supported == "v1"
    ));
}

#[test]
fn decoder_rejects_corrupted_checksum() {
    let corrupted = STEADY_V2.replacen("\"steady\"", "\"disabled\"", 1);

    assert!(matches!(
        decode_grep_manifest(corrupted.as_bytes()),
        Err(GrepRootCodecError::ChecksumMismatch { .. })
    ));
}

#[test]
fn decoder_rejects_truncated_payload() {
    let truncated = &STEADY_V2.as_bytes()[..STEADY_V2.len() - 8];

    assert!(matches!(
        decode_grep_manifest(truncated),
        Err(GrepRootCodecError::EnvelopeDecode { .. })
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
        GrepRootState::new(
            namespace_id("docs"),
            GrepLifecycle::Steady {
                built_through_seq: ChangeSeq(7),
                next_event_index: 0,
            },
            index,
            vec![segment_ref(1, 1, 0, 0)]
        ),
        Err(GrepRootStateError::MissingReorganizeSnapshotSegment { .. })
    ));
}

fn sample_backfilling_root() -> GrepRootState {
    let fold = GrepReorganizeState {
        snapshot_segment_ids: vec![segment_id(1), segment_id(2)],
        output_segment_ids: vec![segment_id(3)],
        row_key_cursor: "gram-6d6e6f-00000000000000000042".to_owned(),
        output_level: 1,
        run_ordinal: 3,
    };
    GrepRootState::new(
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

fn sample_steady_root(built_through_seq: ChangeSeq, next_event_index: u32) -> GrepRootState {
    GrepRootState::new(
        namespace_id("docs"),
        GrepLifecycle::Steady {
            built_through_seq,
            next_event_index,
        },
        GrepIndexState::new(None, 2),
        vec![segment_ref(1, 1, 0, 0)],
    )
    .expect("valid steady root")
}

fn sample_disabled_root() -> GrepRootState {
    GrepRootState::new(
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
