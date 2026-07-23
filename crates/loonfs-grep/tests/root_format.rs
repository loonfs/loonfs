//! Frozen grep root and manifest format fixtures.

#![allow(clippy::panic)]

use loonfs_api::wire::sst_blocks::BlockHandle;
use loonfs_api::{ChangeSeq, CheckpointId, IndexSegmentId, NamespaceId};
use loonfs_grep::root::{
    decode_grep_manifest, decode_grep_root, encode_grep_manifest, encode_grep_root, GrepFoldState,
    GrepIndexState, GrepLifecycle, GrepManifestEnvelope, GrepManifestId, GrepRootCodecError,
    GrepRootEnvelope, GrepRootPointer, GrepRootState, GrepRootStateError, GrepSegmentRef,
};

// Frozen v1 format pins. These byte strings are the durable compatibility
// fixtures: changing field names, ordering, enum tags, or omission rules must
// change them deliberately. Together they represent every lifecycle state.
const BACKFILLING_V1: &str = r#"{"kind":"grep_manifest","format_version":"v1","writer_version":"loonfs-grep-tests/0.1.1","payload_checksum":"sha256:15277f5e958c561781b58ca8faff30a9284ef154e184bd662d76d1b7d34c6824","payload":{"namespace_id":"docs","lifecycle":{"kind":"backfilling","backfill_cursor":"revision-00000000000000000007","checkpoint_id":"chk_00000000000000000000000000000009"},"index":{"format_version":1,"built_through_seq":7,"fold":{"snapshot_segment_ids":["idx_00000000000000000000000000000001","idx_00000000000000000000000000000002"],"output_segment_ids":["idx_00000000000000000000000000000003"],"row_key_cursor":"gram-6d6e6f-00000000000000000042","output_level":1,"run_ordinal":3},"next_run_ordinal":4},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_seq":8,"run_ordinal":1,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},{"segment_id":"idx_00000000000000000000000000000002","run_seq":9,"run_ordinal":2,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},{"segment_id":"idx_00000000000000000000000000000003","run_seq":10,"run_ordinal":3,"level":1,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}]}}"#;
const STEADY_V1: &str = r#"{"kind":"grep_manifest","format_version":"v1","writer_version":"loonfs-grep-tests/0.1.1","payload_checksum":"sha256:ed1974bcc4b70b0d2df55ca19c9aa1f6080abdebdbbaa873e963af80b32e9124","payload":{"namespace_id":"docs","lifecycle":{"kind":"steady"},"index":{"format_version":1,"built_through_seq":11,"next_delta_index":5,"next_run_ordinal":2},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_seq":8,"run_ordinal":1,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}]}}"#;
const DISABLED_V1: &str = r#"{"kind":"grep_manifest","format_version":"v1","writer_version":"loonfs-grep-tests/0.1.1","payload_checksum":"sha256:70b4ba748d1ddffcb28baa05e0874993a4650007e09bf32c85c215ee298baf6c","payload":{"namespace_id":"docs","lifecycle":{"kind":"disabled"},"index":{"format_version":1,"built_through_seq":15,"next_run_ordinal":4},"segments":[]}}"#;
const ADDITIVE_V1: &str = r#"{"kind":"grep_manifest","format_version":"v1","writer_version":"future-writer/9.0","payload_checksum":"sha256:2582065ee15d356cbaf1b40d245207ab8e2b61e3ae2a86a108957baf41336be9","payload":{"namespace_id":"docs","lifecycle":{"kind":"steady","future_lifecycle":"ignored"},"index":{"format_version":1,"built_through_seq":11,"next_run_ordinal":2,"future_index":17},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_seq":8,"run_ordinal":1,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","future_segment":"ignored"}],"future_root":true},"future_envelope":{"retained":true}}"#;

const BACKFILLING_POINTER_V1: &str = r#"{"kind":"grep_root","format_version":"v1","writer_version":"loonfs-grep-tests/0.1.1","payload_checksum":"sha256:05115c1932ff163b89f566ff9b04601120830830898ec57129e781f19490c30f","payload":{"namespace_id":"docs","manifest_id":"15277f5e958c561781b58ca8faff30a9284ef154e184bd662d76d1b7d34c6824"}}"#;
const STEADY_POINTER_V1: &str = r#"{"kind":"grep_root","format_version":"v1","writer_version":"loonfs-grep-tests/0.1.1","payload_checksum":"sha256:073ed9d53d4b99a865e05d0874580488d99f5ff1ff1635e50693958517c0d2e3","payload":{"namespace_id":"docs","manifest_id":"ed1974bcc4b70b0d2df55ca19c9aa1f6080abdebdbbaa873e963af80b32e9124"}}"#;
const DISABLED_POINTER_V1: &str = r#"{"kind":"grep_root","format_version":"v1","writer_version":"loonfs-grep-tests/0.1.1","payload_checksum":"sha256:e5c0fbe19e4bee1cfb5ddf3a99f7c4439a83e4c323b1a6054bfa62e16a9c1d73","payload":{"namespace_id":"docs","manifest_id":"70b4ba748d1ddffcb28baa05e0874993a4650007e09bf32c85c215ee298baf6c"}}"#;
const ADDITIVE_POINTER_V1: &str = r#"{"kind":"grep_root","format_version":"v1","writer_version":"future-writer/9.0","payload_checksum":"sha256:b09014e32fe81501ef34ca037c33ad6b3cbdf4424a16bd376462f84dd2d1b4bc","payload":{"namespace_id":"docs","manifest_id":"9b347bb59f8d589465bddb1104e57be2d6a3babfe8b54d0fc8721b91f1a8b6ad","future_pointer":true},"future_envelope":{"retained":true}}"#;

#[test]
fn encoded_manifests_and_pointers_match_frozen_v1_bytes() {
    let cases = [
        (sample_backfilling_root(), BACKFILLING_V1),
        (sample_steady_root(ChangeSeq(11), 5), STEADY_V1),
        (sample_disabled_root(), DISABLED_V1),
    ];

    for (state, expected) in cases {
        let manifest = GrepManifestEnvelope::from_state("loonfs-grep-tests/0.1.1", state)
            .expect("build manifest envelope");
        let actual =
            String::from_utf8(encode_grep_manifest(&manifest).expect("encode grep manifest"))
                .expect("manifest JSON is UTF-8");
        assert_eq!(actual, expected);

        let pointer = GrepRootEnvelope::from_pointer(
            "loonfs-grep-tests/0.1.1",
            GrepRootPointer::new(namespace_id("docs"), manifest.manifest_id().clone()),
        )
        .expect("build pointer envelope");
        let actual = String::from_utf8(encode_grep_root(&pointer).expect("encode grep pointer"))
            .expect("pointer JSON is UTF-8");
        let expected = match manifest.state().lifecycle() {
            GrepLifecycle::Backfilling { .. } => BACKFILLING_POINTER_V1,
            GrepLifecycle::Steady => STEADY_POINTER_V1,
            GrepLifecycle::Disabled => DISABLED_POINTER_V1,
        };
        assert_eq!(actual, expected);
    }
}

#[test]
fn decoders_read_frozen_v1_bytes_with_additive_fields() {
    let decoded =
        decode_grep_manifest(ADDITIVE_V1.as_bytes()).expect("decode additive manifest fixture");

    assert_eq!(decoded.state(), &sample_steady_root(ChangeSeq(11), 0));
    let pointer =
        decode_grep_root(ADDITIVE_POINTER_V1.as_bytes()).expect("decode additive pointer fixture");
    assert_eq!(
        pointer.pointer(),
        &GrepRootPointer::new(
            namespace_id("docs"),
            GrepManifestId::parse(
                "9b347bb59f8d589465bddb1104e57be2d6a3babfe8b54d0fc8721b91f1a8b6ad"
            )
            .expect("valid manifest id")
        )
    );
}

#[test]
fn decoder_rejects_unknown_version_without_fallback() {
    let wrong_version =
        STEADY_V1.replacen("\"format_version\":\"v1\"", "\"format_version\":\"v7\"", 1);

    assert!(matches!(
        decode_grep_manifest(wrong_version.as_bytes()),
        Err(GrepRootCodecError::UnsupportedFormatVersion { found, supported })
            if found == "v7" && supported == "v1"
    ));
}

#[test]
fn decoder_rejects_corrupted_checksum() {
    let corrupted = STEADY_V1.replacen("\"steady\"", "\"disabled\"", 1);

    assert!(matches!(
        decode_grep_manifest(corrupted.as_bytes()),
        Err(GrepRootCodecError::ChecksumMismatch { .. })
    ));
}

#[test]
fn decoder_rejects_truncated_payload() {
    let truncated = &STEADY_V1.as_bytes()[..STEADY_V1.len() - 8];

    assert!(matches!(
        decode_grep_manifest(truncated),
        Err(GrepRootCodecError::EnvelopeDecode { .. })
    ));
}

#[test]
fn constructor_rejects_fold_segment_mismatch() {
    let mut index = GrepIndexState::new(ChangeSeq(7), 0, None, 2);
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
            checkpoint_id: CheckpointId::parse("chk_00000000000000000000000000000009")
                .expect("valid checkpoint id"),
        },
        GrepIndexState::new(ChangeSeq(7), 0, Some(fold), 4),
        vec![
            segment_ref(1, 1, 0, 0),
            segment_ref(2, 2, 0, 0),
            segment_ref(3, 3, 1, 0),
        ],
    )
    .expect("valid backfilling root")
}

fn sample_steady_root(built_through_seq: ChangeSeq, next_delta_index: u32) -> GrepRootState {
    GrepRootState::new(
        namespace_id("docs"),
        GrepLifecycle::Steady,
        GrepIndexState::new(built_through_seq, next_delta_index, None, 2),
        vec![segment_ref(1, 1, 0, 0)],
    )
    .expect("valid steady root")
}

fn sample_disabled_root() -> GrepRootState {
    GrepRootState::new(
        namespace_id("docs"),
        GrepLifecycle::Disabled,
        GrepIndexState::new(ChangeSeq(15), 0, None, 4),
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
