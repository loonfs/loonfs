#![allow(clippy::panic)]

use loonfs_api::wire::sst_blocks::BlockHandle;
use loonfs_api::{ChangeSeq, CheckpointId, IndexSegmentId, NamespaceId};
use loonfs_grep::root::{
    decode_grep_root, encode_grep_root, GrepFoldState, GrepIndexState, GrepLifecycle,
    GrepRootCodecError, GrepRootEnvelope, GrepRootState, GrepRootStateError, GrepSegmentRef,
};

// Frozen v0 format pins. These byte strings are the durable compatibility
// fixtures: changing field names, ordering, enum tags, or omission rules must
// change them deliberately. Together they represent every lifecycle state.
const BACKFILLING_V0: &str = r#"{"kind":"grep_root","format_version":"v0","writer_version":"loonfs-grep-tests/0.1.1","payload_checksum":"sha256:85e924a85e511959db7c82e342f7c2f6417fae1b8a57038ffa913084873e3f5e","payload":{"namespace_id":"docs","lifecycle":{"kind":"backfilling","backfill_cursor":"revision-00000000000000000007","checkpoint_id":"chk_00000000000000000000000000000009"},"index":{"format_version":0,"built_through_seq":7,"fold":{"snapshot_segment_ids":["idx_00000000000000000000000000000001","idx_00000000000000000000000000000002"],"output_segment_ids":["idx_00000000000000000000000000000003"],"row_key_cursor":"gram-6d6e6f-00000000000000000042","output_level":1,"run_ordinal":3},"next_run_ordinal":4},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_seq":8,"run_ordinal":1,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},{"segment_id":"idx_00000000000000000000000000000002","run_seq":9,"run_ordinal":2,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},{"segment_id":"idx_00000000000000000000000000000003","run_seq":10,"run_ordinal":3,"level":1,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}]}}"#;
const STEADY_V0: &str = r#"{"kind":"grep_root","format_version":"v0","writer_version":"loonfs-grep-tests/0.1.1","payload_checksum":"sha256:f49e60b4af9f8e074e7c31107e5a7260698c791cfa3d2b9319d65b993a26ec09","payload":{"namespace_id":"docs","lifecycle":{"kind":"steady"},"index":{"format_version":0,"built_through_seq":11,"next_run_ordinal":2},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_seq":8,"run_ordinal":1,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}]}}"#;
const DISABLED_V0: &str = r#"{"kind":"grep_root","format_version":"v0","writer_version":"loonfs-grep-tests/0.1.1","payload_checksum":"sha256:4711db321ec737abf7334d89865e3488d1d79c4d567823402f25bb971b989eee","payload":{"namespace_id":"docs","lifecycle":{"kind":"disabled"},"index":{"format_version":0,"built_through_seq":15,"next_run_ordinal":4},"segments":[]}}"#;
const ADDITIVE_V0: &str = r#"{"kind":"grep_root","format_version":"v0","writer_version":"future-writer/9.0","payload_checksum":"sha256:2d8afef7322d76189d288b50b3b02e8f475a6249b6d6f89659d6561d0f193870","payload":{"namespace_id":"docs","lifecycle":{"kind":"steady","future_lifecycle":"ignored"},"index":{"format_version":0,"built_through_seq":11,"next_run_ordinal":2,"future_index":17},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_seq":8,"run_ordinal":1,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","future_segment":"ignored"}],"future_root":true},"future_envelope":{"retained":true}}"#;

#[test]
fn encoded_roots_match_frozen_v0_bytes() {
    let cases = [
        (sample_backfilling_root(), BACKFILLING_V0),
        (sample_steady_root(ChangeSeq(11)), STEADY_V0),
        (sample_disabled_root(), DISABLED_V0),
    ];

    for (state, expected) in cases {
        let envelope =
            GrepRootEnvelope::from_state("loonfs-grep-tests/0.1.1", state).expect("build envelope");
        let actual = String::from_utf8(encode_grep_root(&envelope).expect("encode root"))
            .expect("root JSON is UTF-8");
        assert_eq!(actual, expected);
    }
}

#[test]
fn decoder_reads_frozen_v0_bytes_with_additive_fields() {
    let decoded = decode_grep_root(ADDITIVE_V0.as_bytes()).expect("decode additive v0 fixture");

    assert_eq!(decoded.state(), &sample_steady_root(ChangeSeq(11)));
}

#[test]
fn decoder_rejects_unknown_version_without_fallback() {
    let future = STEADY_V0.replacen("\"format_version\":\"v0\"", "\"format_version\":\"v1\"", 1);

    assert!(matches!(
        decode_grep_root(future.as_bytes()),
        Err(GrepRootCodecError::UnsupportedFormatVersion { found, supported })
            if found == "v1" && supported == "v0"
    ));
}

#[test]
fn decoder_rejects_corrupted_checksum() {
    let corrupted = STEADY_V0.replacen("\"steady\"", "\"disabled\"", 1);

    assert!(matches!(
        decode_grep_root(corrupted.as_bytes()),
        Err(GrepRootCodecError::ChecksumMismatch { .. })
    ));
}

#[test]
fn decoder_rejects_truncated_payload() {
    let truncated = &STEADY_V0.as_bytes()[..STEADY_V0.len() - 8];

    assert!(matches!(
        decode_grep_root(truncated),
        Err(GrepRootCodecError::EnvelopeDecode { .. })
    ));
}

#[test]
fn constructor_rejects_fold_segment_mismatch() {
    let mut index = GrepIndexState::new(ChangeSeq(7), None, 2);
    index.fold = Some(GrepFoldState {
        snapshot_segment_ids: vec![segment_id(9)],
        output_segment_ids: Vec::new(),
        row_key_cursor: String::new(),
        output_level: 1,
        run_ordinal: 1,
    });

    assert!(matches!(
        GrepRootState::new(
            namespace_id("docs"),
            GrepLifecycle::Steady,
            index,
            vec![segment_ref(1, 1, 0, 0)]
        ),
        Err(GrepRootStateError::MissingFoldSnapshotSegment { .. })
    ));
}

fn sample_backfilling_root() -> GrepRootState {
    let fold = GrepFoldState {
        snapshot_segment_ids: vec![segment_id(1), segment_id(2)],
        output_segment_ids: vec![segment_id(3)],
        row_key_cursor: "gram-6d6e6f-00000000000000000042".to_owned(),
        output_level: 1,
        run_ordinal: 3,
    };
    GrepRootState::new(
        namespace_id("docs"),
        GrepLifecycle::Backfilling {
            backfill_cursor: "revision-00000000000000000007".to_owned(),
            checkpoint_id: Some(
                CheckpointId::parse("chk_00000000000000000000000000000009")
                    .expect("valid checkpoint id"),
            ),
        },
        GrepIndexState::new(ChangeSeq(7), Some(fold), 4),
        vec![
            segment_ref(1, 1, 0, 0),
            segment_ref(2, 2, 0, 0),
            segment_ref(3, 3, 1, 0),
        ],
    )
    .expect("valid backfilling root")
}

fn sample_steady_root(built_through_seq: ChangeSeq) -> GrepRootState {
    GrepRootState::new(
        namespace_id("docs"),
        GrepLifecycle::Steady,
        GrepIndexState::new(built_through_seq, None, 2),
        vec![segment_ref(1, 1, 0, 0)],
    )
    .expect("valid steady root")
}

fn sample_disabled_root() -> GrepRootState {
    GrepRootState::new(
        namespace_id("docs"),
        GrepLifecycle::Disabled,
        GrepIndexState::new(ChangeSeq(15), None, 4),
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

fn namespace_id(value: &str) -> NamespaceId {
    NamespaceId::parse(value).expect("valid namespace id")
}

fn segment_id(number: u8) -> IndexSegmentId {
    IndexSegmentId::parse(format!("idx_{number:032x}")).expect("valid segment id")
}
