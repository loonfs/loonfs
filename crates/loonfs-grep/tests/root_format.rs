#![allow(clippy::panic)]

use loonfs_api::wire::sst_blocks::BlockHandle;
use loonfs_api::{ChangeSeq, CheckpointId, IndexSegmentId, NamespaceId};
use loonfs_grep::root::{
    decode_grep_manifest, decode_grep_root, encode_grep_manifest, encode_grep_root, GrepFoldState,
    GrepIndexState, GrepLifecycle, GrepManifestEnvelope, GrepManifestId, GrepRootCodecError,
    GrepRootEnvelope, GrepRootPointer, GrepRootState, GrepRootStateError, GrepSegmentRef,
};

// Frozen v0 format pins. These byte strings are the durable compatibility
// fixtures: changing field names, ordering, enum tags, or omission rules must
// change them deliberately. Together they represent every lifecycle state.
const BACKFILLING_V0: &str = r#"{"kind":"grep_manifest","format_version":"v0","writer_version":"loonfs-grep-tests/0.1.1","payload_checksum":"sha256:85e924a85e511959db7c82e342f7c2f6417fae1b8a57038ffa913084873e3f5e","payload":{"namespace_id":"docs","lifecycle":{"kind":"backfilling","backfill_cursor":"revision-00000000000000000007","checkpoint_id":"chk_00000000000000000000000000000009"},"index":{"format_version":0,"built_through_seq":7,"fold":{"snapshot_segment_ids":["idx_00000000000000000000000000000001","idx_00000000000000000000000000000002"],"output_segment_ids":["idx_00000000000000000000000000000003"],"row_key_cursor":"gram-6d6e6f-00000000000000000042","output_level":1,"run_ordinal":3},"next_run_ordinal":4},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_seq":8,"run_ordinal":1,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},{"segment_id":"idx_00000000000000000000000000000002","run_seq":9,"run_ordinal":2,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},{"segment_id":"idx_00000000000000000000000000000003","run_seq":10,"run_ordinal":3,"level":1,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}]}}"#;
const STEADY_V0: &str = r#"{"kind":"grep_manifest","format_version":"v0","writer_version":"loonfs-grep-tests/0.1.1","payload_checksum":"sha256:f49e60b4af9f8e074e7c31107e5a7260698c791cfa3d2b9319d65b993a26ec09","payload":{"namespace_id":"docs","lifecycle":{"kind":"steady"},"index":{"format_version":0,"built_through_seq":11,"next_run_ordinal":2},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_seq":8,"run_ordinal":1,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}]}}"#;
const DISABLED_V0: &str = r#"{"kind":"grep_manifest","format_version":"v0","writer_version":"loonfs-grep-tests/0.1.1","payload_checksum":"sha256:4711db321ec737abf7334d89865e3488d1d79c4d567823402f25bb971b989eee","payload":{"namespace_id":"docs","lifecycle":{"kind":"disabled"},"index":{"format_version":0,"built_through_seq":15,"next_run_ordinal":4},"segments":[]}}"#;
const ADDITIVE_V0: &str = r#"{"kind":"grep_manifest","format_version":"v0","writer_version":"future-writer/9.0","payload_checksum":"sha256:2d8afef7322d76189d288b50b3b02e8f475a6249b6d6f89659d6561d0f193870","payload":{"namespace_id":"docs","lifecycle":{"kind":"steady","future_lifecycle":"ignored"},"index":{"format_version":0,"built_through_seq":11,"next_run_ordinal":2,"future_index":17},"segments":[{"segment_id":"idx_00000000000000000000000000000001","run_seq":8,"run_ordinal":1,"level":0,"segment_index":0,"min_row_key":"gram-616263-00000000000000000001","max_row_key":"gram-7a7a7a-00000000000000000099","index_block":{"offset":128,"stored_len":48,"decoded_len":96,"crc32c":305419896},"filter_block":{"offset":176,"stored_len":16,"decoded_len":16,"crc32c":2591069104},"filter_inline":"00112233445566778899aabbccddeeff","payload_checksum":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","future_segment":"ignored"}],"future_root":true},"future_envelope":{"retained":true}}"#;

const BACKFILLING_POINTER_V0: &str = r#"{"kind":"grep_root","format_version":"v0","writer_version":"loonfs-grep-tests/0.1.1","payload_checksum":"sha256:cb37939961048d1e11ad75ed9ee12048289c64e7b658183e37ee54d3dd4775bc","payload":{"namespace_id":"docs","manifest_id":"85e924a85e511959db7c82e342f7c2f6417fae1b8a57038ffa913084873e3f5e"}}"#;
const STEADY_POINTER_V0: &str = r#"{"kind":"grep_root","format_version":"v0","writer_version":"loonfs-grep-tests/0.1.1","payload_checksum":"sha256:7f8626871c6be491b412bd6f51ca6ea801bfc7bea4f963da81ab1f4666b39f3a","payload":{"namespace_id":"docs","manifest_id":"f49e60b4af9f8e074e7c31107e5a7260698c791cfa3d2b9319d65b993a26ec09"}}"#;
const DISABLED_POINTER_V0: &str = r#"{"kind":"grep_root","format_version":"v0","writer_version":"loonfs-grep-tests/0.1.1","payload_checksum":"sha256:aa4229c568fce32c8130970b4ca439cc73766739f444bdac5005ef68426a2123","payload":{"namespace_id":"docs","manifest_id":"4711db321ec737abf7334d89865e3488d1d79c4d567823402f25bb971b989eee"}}"#;
const ADDITIVE_POINTER_V0: &str = r#"{"kind":"grep_root","format_version":"v0","writer_version":"future-writer/9.0","payload_checksum":"sha256:eedcda6d70834cf888191e747ea90ea440a41f9cf1f1189e49f803af38f5797e","payload":{"namespace_id":"docs","manifest_id":"f49e60b4af9f8e074e7c31107e5a7260698c791cfa3d2b9319d65b993a26ec09","future_pointer":true},"future_envelope":{"retained":true}}"#;

#[test]
fn encoded_manifests_and_pointers_match_frozen_v0_bytes() {
    let cases = [
        (sample_backfilling_root(), BACKFILLING_V0),
        (sample_steady_root(ChangeSeq(11)), STEADY_V0),
        (sample_disabled_root(), DISABLED_V0),
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
            GrepLifecycle::Backfilling { .. } => BACKFILLING_POINTER_V0,
            GrepLifecycle::Steady => STEADY_POINTER_V0,
            GrepLifecycle::Disabled => DISABLED_POINTER_V0,
        };
        assert_eq!(actual, expected);
    }
}

#[test]
fn decoders_read_frozen_v0_bytes_with_additive_fields() {
    let decoded =
        decode_grep_manifest(ADDITIVE_V0.as_bytes()).expect("decode additive manifest fixture");

    assert_eq!(decoded.state(), &sample_steady_root(ChangeSeq(11)));
    let pointer =
        decode_grep_root(ADDITIVE_POINTER_V0.as_bytes()).expect("decode additive pointer fixture");
    assert_eq!(
        pointer.pointer(),
        &GrepRootPointer::new(
            namespace_id("docs"),
            GrepManifestId::parse(
                "f49e60b4af9f8e074e7c31107e5a7260698c791cfa3d2b9319d65b993a26ec09"
            )
            .expect("valid manifest id")
        )
    );
}

#[test]
fn decoder_rejects_unknown_version_without_fallback() {
    let future = STEADY_V0.replacen("\"format_version\":\"v0\"", "\"format_version\":\"v1\"", 1);

    assert!(matches!(
        decode_grep_manifest(future.as_bytes()),
        Err(GrepRootCodecError::UnsupportedFormatVersion { found, supported })
            if found == "v1" && supported == "v0"
    ));
}

#[test]
fn decoder_rejects_corrupted_checksum() {
    let corrupted = STEADY_V0.replacen("\"steady\"", "\"disabled\"", 1);

    assert!(matches!(
        decode_grep_manifest(corrupted.as_bytes()),
        Err(GrepRootCodecError::ChecksumMismatch { .. })
    ));
}

#[test]
fn decoder_rejects_truncated_payload() {
    let truncated = &STEADY_V0.as_bytes()[..STEADY_V0.len() - 8];

    assert!(matches!(
        decode_grep_manifest(truncated),
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
