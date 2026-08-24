//! Golden-byte fixtures for the durable grep encodings.
//!
//! These tests pin the exact bytes the grep root pointer, the grep manifest,
//! and a gram-index data block write. They run the mechanism the core
//! families use in `crates/loonfs-api/tests/golden_formats.rs`:
//!
//! - If an encoder's output diverges from its fixture, a Rust-side change
//!   (field rename, reorder, serde attribute, removed field) silently changed
//!   the durable format. While LoonFS is pre-release, either revert it or
//!   regenerate the family's version-1 fixture with `UPDATE_GOLDEN=1 cargo
//!   test`; released formats follow the spec's evolution rules.
//! - If a fixture stops decoding, the current reader can no longer read bytes
//!   another implementation of the same format version wrote. Each fixture
//!   therefore has a second test that decodes the stored bytes and states the
//!   sample they hold, so a decoder change fails even while the encoder still
//!   matches.
//!
//! The decode-rejection rules sit in `root_format`, which builds its inputs
//! from these same files.

#![allow(clippy::panic)]

use loonfs_api::wire::sst_blocks::{
    decode_data_block_rows, decode_index_block, BlockHandle, BuiltSegmentBlocks, DecodedDataBlock,
    SegmentBlocksBuilder,
};
use loonfs_api::{ChangeSeq, CheckpointId, IndexSegmentId, InodeId, RevisionNo, RunNo};
use loonfs_grep::codec::{Gram, GramPosting, IndexRow};
use loonfs_grep::root::{
    decode_grep_manifest, decode_grep_root, encode_grep_manifest, encode_grep_root, GrepIndexState,
    GrepIndexStatus, GrepManifestEnvelope, GrepManifestObjectId, GrepManifestState,
    GrepReorganizeState, GrepRootEnvelope, GrepRootPointer, GrepSegmentRef,
};
use loonfs_test_support::ids::namespace_id;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Fixture names
// ---------------------------------------------------------------------------

/// The root pointer that names [`ACTIVE_MANIFEST_FIXTURE`].
pub(crate) const ROOT_POINTER_FIXTURE: &str = "grep_root.v1.json";
/// An active index reorganizing a set of segments.
pub(crate) const ACTIVE_MANIFEST_FIXTURE: &str = "grep_manifest.v1.json";
/// A backfill in progress, with segments and no reorganization.
pub(crate) const BACKFILLING_MANIFEST_FIXTURE: &str = "grep_manifest.backfilling.v1.json";
/// A disabled index, which carries neither segments nor a reorganization.
pub(crate) const DISABLED_MANIFEST_FIXTURE: &str = "grep_manifest.disabled.v1.json";
/// One data block of `gram_postings` rows.
const GRAM_POSTINGS_BLOCK_FIXTURE: &str = "grep_segment_gram_postings.v1.bin";

/// Pointer ids are minted, not derived, so a fixture names an arbitrary id
/// and carries the digest of the manifest it points at. That pairing is the
/// whole binding between a pointer and its bytes.
const ACTIVE_MANIFEST_OBJECT_ID: &str = "gmf_2b3c4d5e6f70819a2b3c4d5e6f708192";

// ---------------------------------------------------------------------------
// Golden helpers
// ---------------------------------------------------------------------------
//
// Copied from the core fixture helpers in
// `crates/loonfs-api/tests/golden_formats.rs`, which name their own package
// in the same message. Both crates store their fixtures under `tests/golden`
// and rewrite them the same way.

fn golden_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name)
}

// Regenerate with `UPDATE_GOLDEN=1 cargo test -p loonfs-grep -- --test-threads=1`:
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
        panic!("read golden fixture `{name}` ({err}); run `UPDATE_GOLDEN=1 cargo test -p loonfs-grep -- --test-threads=1` to generate it")
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

pub(crate) fn read_golden(name: &str) -> Vec<u8> {
    std::fs::read(golden_path(name)).unwrap_or_else(|err| {
        panic!("read golden fixture `{name}` ({err}); run `UPDATE_GOLDEN=1 cargo test -p loonfs-grep -- --test-threads=1` to generate it")
    })
}

fn unzstd(bytes: &[u8]) -> Vec<u8> {
    zstd::stream::decode_all(bytes).expect("decompress the block")
}

// ---------------------------------------------------------------------------
// Samples: fixed values covering every grep status and both index shapes
// ---------------------------------------------------------------------------

/// An active index with a reorganization running over its segments. The
/// reorganization merges the two level-0 segments into the level-1 segment,
/// so every id the reorganize state names is a segment this manifest holds.
pub(crate) fn sample_active_manifest(
    built_through_seq: ChangeSeq,
    next_event_index: u32,
) -> GrepManifestState {
    let reorganize = GrepReorganizeState {
        snapshot_segment_ids: vec![segment_id(1), segment_id(2)],
        output_segment_ids: vec![segment_id(3)],
        row_key_cursor: "gram-6d6e6f-00000000000000000042".to_owned(),
        output_level: 1,
        run_no: RunNo(3),
    };
    GrepManifestState::new(
        namespace_id("docs"),
        GrepIndexStatus::Active {
            built_through_seq,
            next_event_index,
        },
        GrepIndexState {
            reorganize: Some(reorganize),
            next_run_no: RunNo(4),
        },
        vec![
            segment_ref(1, 1, 0, 0),
            segment_ref(2, 2, 0, 0),
            segment_ref(3, 3, 1, 0),
        ],
    )
    .expect("valid active manifest state")
}

/// A backfill walking a pinned checkpoint. It carries the segments the walk
/// has written so far and no reorganization, which is the index shape the
/// active fixture above does not pin.
pub(crate) fn sample_backfilling_manifest() -> GrepManifestState {
    GrepManifestState::new(
        namespace_id("docs"),
        GrepIndexStatus::Backfilling {
            target_seq: ChangeSeq(7),
            cursor_inode_id: Some(InodeId(7)),
            checkpoint_id: CheckpointId::parse("chk_00000000000000000000000000000009")
                .expect("valid checkpoint id"),
        },
        GrepIndexState {
            reorganize: None,
            next_run_no: RunNo(2),
        },
        vec![segment_ref(1, 1, 0, 0)],
    )
    .expect("valid backfilling manifest state")
}

/// A disabled index. The status carries no fields, and the manifest holds
/// neither segments nor a reorganization.
pub(crate) fn sample_disabled_manifest() -> GrepManifestState {
    GrepManifestState::new(
        namespace_id("docs"),
        GrepIndexStatus::Disabled {},
        GrepIndexState {
            reorganize: None,
            next_run_no: RunNo(4),
        },
        Vec::new(),
    )
    .expect("valid disabled manifest state")
}

/// The pointer a publisher installs over the manifest
/// [`ACTIVE_MANIFEST_FIXTURE`] pins. Its digest is that manifest envelope's
/// own `payload_checksum`.
fn sample_root_pointer() -> GrepRootPointer {
    let manifest = GrepManifestEnvelope::from_state(sample_active_manifest(ChangeSeq(11), 5))
        .expect("build a grep manifest envelope");
    GrepRootPointer::new(
        namespace_id("docs"),
        GrepManifestObjectId::parse(ACTIVE_MANIFEST_OBJECT_ID).expect("valid manifest object id"),
        manifest.payload_checksum().to_owned(),
    )
}

/// One segment descriptor. `number` picks the segment id and its run
/// sequence, and only the first segment carries an inlined filter, so a
/// manifest holding several of these pins both the present and the absent
/// spelling of that field.
pub(crate) fn segment_ref(
    number: u8,
    run_no: u64,
    level: u32,
    segment_index: u32,
) -> GrepSegmentRef {
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

pub(crate) fn segment_id(number: u8) -> IndexSegmentId {
    IndexSegmentId::parse(format!("idx_{number:032x}")).expect("valid segment id")
}

fn posting(inode_id: u64, revision_no: u64) -> GramPosting {
    GramPosting {
        inode_id: InodeId(inode_id),
        revision_no: RevisionNo(revision_no),
    }
}

/// The rows one gram-index data block holds. Two rows carry the same gram,
/// which the format permits and readers union, and the first batch spans
/// several postings whose inode deltas and revision numbers need more than
/// one varint byte.
fn sample_gram_postings_rows() -> Vec<IndexRow> {
    vec![
        IndexRow::gram_postings(
            Gram(*b"abc"),
            &[posting(1, 1), posting(2, 300), posting(500, 7)],
        )
        .expect("valid gram postings row"),
        IndexRow::gram_postings(Gram(*b"abc"), &[posting(900, 2), posting(901, 3)])
            .expect("valid gram postings row"),
        IndexRow::gram_postings(Gram(*b"fox"), &[posting(7, 1)]).expect("valid gram postings row"),
    ]
}

/// Builds the sample segment through the block builder and the target block
/// size the grep worker builds with, so the fixture pins the bytes a real
/// segment holds.
fn sample_gram_postings_segment() -> BuiltSegmentBlocks {
    let mut builder = SegmentBlocksBuilder::default();
    for row in sample_gram_postings_rows() {
        builder
            .push(&row.row_key(), &row.filter_key(), &row)
            .expect("push a gram postings row");
    }
    builder.finish().expect("finish the gram index segment")
}

fn segment_section<'a>(bytes: &'a [u8], handle: &BlockHandle) -> &'a [u8] {
    &bytes[handle.offset as usize..handle.offset as usize + handle.stored_len as usize]
}

/// Reads back the block a fixture pins. The fixture stores the decompressed
/// payload, so this recompresses it and describes it with a handle before
/// handing both to the real decoder.
fn decode_golden_data_block(name: &str) -> DecodedDataBlock<IndexRow> {
    let payload = read_golden(name);
    let stored = zstd::stream::encode_all(payload.as_slice(), 0).expect("compress the block");
    let handle = BlockHandle {
        offset: 0,
        stored_len: stored.len() as u32,
        decoded_len: payload.len() as u32,
        crc32c: crc32c::crc32c(&stored),
    };
    decode_data_block_rows::<IndexRow>(&stored, &handle).expect("decode golden data block")
}

// ---------------------------------------------------------------------------
// The fixtures
// ---------------------------------------------------------------------------

fn assert_manifest_matches_golden(fixture: &str, state: GrepManifestState) {
    let envelope = GrepManifestEnvelope::from_state(state).expect("build a grep manifest envelope");
    let encoded = encode_grep_manifest(&envelope).expect("encode a grep manifest");
    assert_matches_golden(fixture, &encoded);
}

fn assert_manifest_golden_decodes(fixture: &str, state: GrepManifestState) {
    let expected = GrepManifestEnvelope::from_state(state).expect("build a grep manifest envelope");
    let decoded = decode_grep_manifest(&read_golden(fixture)).expect("decode the golden manifest");
    assert_eq!(decoded, expected);
}

#[test]
fn grep_root_matches_golden_bytes() {
    let envelope =
        GrepRootEnvelope::from_pointer(sample_root_pointer()).expect("build a grep root envelope");
    let encoded = encode_grep_root(&envelope).expect("encode a grep root pointer");
    assert_matches_golden(ROOT_POINTER_FIXTURE, &encoded);
}

#[test]
fn grep_root_golden_decodes_to_sample() {
    let expected =
        GrepRootEnvelope::from_pointer(sample_root_pointer()).expect("build a grep root envelope");
    let decoded =
        decode_grep_root(&read_golden(ROOT_POINTER_FIXTURE)).expect("decode the golden pointer");
    assert_eq!(decoded, expected);
}

#[test]
fn grep_root_golden_carries_the_manifest_golden_checksum() {
    let pointer =
        decode_grep_root(&read_golden(ROOT_POINTER_FIXTURE)).expect("decode the golden pointer");
    let manifest = decode_grep_manifest(&read_golden(ACTIVE_MANIFEST_FIXTURE))
        .expect("decode the golden manifest");

    assert_eq!(
        pointer.pointer().manifest_payload_checksum(),
        manifest.payload_checksum()
    );
}

#[test]
fn grep_manifest_matches_golden_bytes() {
    assert_manifest_matches_golden(
        ACTIVE_MANIFEST_FIXTURE,
        sample_active_manifest(ChangeSeq(11), 5),
    );
}

#[test]
fn grep_manifest_golden_decodes_to_sample() {
    assert_manifest_golden_decodes(
        ACTIVE_MANIFEST_FIXTURE,
        sample_active_manifest(ChangeSeq(11), 5),
    );
}

#[test]
fn grep_manifest_backfilling_matches_golden_bytes() {
    assert_manifest_matches_golden(BACKFILLING_MANIFEST_FIXTURE, sample_backfilling_manifest());
}

#[test]
fn grep_manifest_backfilling_golden_decodes_to_sample() {
    assert_manifest_golden_decodes(BACKFILLING_MANIFEST_FIXTURE, sample_backfilling_manifest());
}

#[test]
fn grep_manifest_disabled_matches_golden_bytes() {
    assert_manifest_matches_golden(DISABLED_MANIFEST_FIXTURE, sample_disabled_manifest());
}

#[test]
fn grep_manifest_disabled_golden_decodes_to_sample() {
    assert_manifest_golden_decodes(DISABLED_MANIFEST_FIXTURE, sample_disabled_manifest());
}

#[test]
fn grep_segment_gram_postings_matches_golden_bytes() {
    let built = sample_gram_postings_segment();
    let index = decode_index_block(segment_section(&built.bytes, &built.index), &built.index)
        .expect("decode the segment index");
    assert_eq!(index.len(), 1, "the row fixture should be one data block");
    // Compare the decompressed block payload: zstd frames may differ across
    // zstd versions, the entry encoding (which the format defines) may not.
    assert_matches_golden(
        GRAM_POSTINGS_BLOCK_FIXTURE,
        &unzstd(segment_section(&built.bytes, &index[0].block)),
    );
}

#[test]
fn grep_segment_gram_postings_golden_decodes_to_sample_rows() {
    let rows = sample_gram_postings_rows();
    let block = decode_golden_data_block(GRAM_POSTINGS_BLOCK_FIXTURE);

    assert_eq!(block.rows, rows);
    assert_eq!(
        block.row_keys,
        rows.iter().map(IndexRow::row_key).collect::<Vec<_>>()
    );
    assert_eq!(
        block.rows[0].postings().expect("unpack the first batch"),
        [posting(1, 1), posting(2, 300), posting(500, 7)]
    );
}
