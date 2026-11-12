//! Golden-byte fixtures for the durable grep encodings.
//!
//! These tests pin the exact bytes of grep's entry in a namespace manifest,
//! and of a data block of each of the index's five row families. They run the mechanism the core
//! families use in `crates/loonfs-types/tests/golden_formats.rs`:
//!
//! - If an encoder's output diverges from its fixture, a Rust-side change
//!   (field rename, reorder, serde attribute, removed field) silently changed
//!   the durable format. While LoonFS is pre-release, either revert it or
//!   regenerate the fixture of the family's current version with
//!   `UPDATE_GOLDEN=1 cargo test`; released formats follow the spec's
//!   evolution rules.
//! - If a fixture stops decoding, the current reader can no longer read bytes
//!   another implementation of the same format version wrote. Each fixture
//!   therefore has a second test that decodes the stored bytes and states the
//!   sample they hold, so a decoder change fails even while the encoder still
//!   matches.
//!
//! The decode-rejection rules sit in `entry_format`, which builds its inputs
//! from these same files.

use loonfs_grep::codec::{ContentValue, Gram, IndexRow};
use loonfs_grep::entry::{GrepEntry, GrepIndexStatus, GrepPruning, GrepReorganizeState};
use loonfs_test_support::ids::namespace_id;
use loonfs_types::format::manifest::{ExtensionEntry, ExtensionRunRef, ExtensionSegmentRef};
use loonfs_types::format::segment_blocks::{
    decode_data_block_rows, decode_index_block, BlockHandle, BuiltSegmentBlocks, DecodedDataBlock,
    SegmentBlocksBuilder, SegmentFilter, SegmentFilterBuilder,
};
use loonfs_types::{CommitSeq, ContentId, IndexSegmentId, InodeId, PinId, RevisionNo, RunNo};
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Fixture names
// ---------------------------------------------------------------------------

/// The entry of an active index reorganizing a set of segments.
pub(crate) const ACTIVE_ENTRY_FIXTURE: &str = "grep_entry.v1.json";
/// The entry of a backfill in progress, with segments and no
/// reorganization.
pub(crate) const BACKFILLING_ENTRY_FIXTURE: &str = "grep_entry.backfilling.v1.json";
/// The entry of a disabled index, which lists neither segments nor a
/// reorganization.
pub(crate) const DISABLED_ENTRY_FIXTURE: &str = "grep_entry.disabled.v1.json";
/// One data block of `gram_postings` rows.
const GRAM_POSTINGS_BLOCK_FIXTURE: &str = "grep_segment_gram_postings.v1.bin";
/// One data block of `text` rows.
const TEXTS_BLOCK_FIXTURE: &str = "grep_segment_texts.v1.bin";
/// One data block of `content_revision` rows.
const CONTENT_REVISIONS_BLOCK_FIXTURE: &str = "grep_segment_content_revisions.v1.bin";
/// One data block of `value` rows.
const VALUES_BLOCK_FIXTURE: &str = "grep_segment_values.v1.bin";
/// One data block of `filter` rows.
const FILTERS_BLOCK_FIXTURE: &str = "grep_segment_filters.v1.bin";

// ---------------------------------------------------------------------------
// Golden helpers
// ---------------------------------------------------------------------------

fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden")
}

fn golden_path(name: &str) -> PathBuf {
    golden_dir().join(name)
}

// Without the single thread, tests that read a fixture race the tests
// rewriting it and fail on a half-written file.
const REGENERATE_GOLDEN: &str = "UPDATE_GOLDEN=1 cargo test -p loonfs-grep -- --test-threads=1";

fn assert_matches_golden(name: &str, actual: &[u8]) {
    loonfs_test_support::golden::assert_matches_golden(
        &golden_dir(),
        REGENERATE_GOLDEN,
        name,
        actual,
    );
}

pub(crate) fn read_golden(name: &str) -> Vec<u8> {
    std::fs::read(golden_path(name)).unwrap_or_else(|err| {
        panic!("read golden fixture `{name}` ({err}); run `{REGENERATE_GOLDEN}` to generate it")
    })
}

fn unzstd(bytes: &[u8]) -> Vec<u8> {
    zstd::stream::decode_all(bytes).expect("decompress the block")
}

// ---------------------------------------------------------------------------
// Samples: fixed values covering every grep status and both index shapes
// ---------------------------------------------------------------------------

/// An active index with a reorganization into the base level running over
/// its runs. The reorganization rewrites the two level-1 runs into the
/// level-2 run, so every id the reorganize state lists is a segment this
/// entry lists, and it records the revisions its pruning keeps.
pub(crate) fn sample_active_entry(
    built_through_seq: CommitSeq,
    next_event_index: u32,
) -> GrepEntry {
    let reorganize = GrepReorganizeState {
        input_segment_ids: vec![segment_id(1), segment_id(2)],
        output_segment_ids: vec![segment_id(3)],
        row_key_cursor: "gram-6d6e6f-00000000000000000026".to_owned(),
        output_level: 2,
        run_no: RunNo(3),
        pruning: Some(GrepPruning {
            filter_seq: CommitSeq(9),
            retention_floor_seq: CommitSeq(4),
        }),
    };
    GrepEntry::new(
        GrepIndexStatus::Active {
            captured_seq: CommitSeq(3),
            built_through_seq,
            next_event_index,
        },
        Some(reorganize),
        vec![
            run_ref(1, 1, &[1]),
            run_ref(2, 1, &[2]),
            run_ref(3, 2, &[3]),
        ],
    )
    .expect("valid active entry")
}

/// A backfill walking a pinned checkpoint. It lists the run the walk has
/// written so far and no reorganization, which is the index shape the active
/// fixture above does not pin.
pub(crate) fn sample_backfilling_entry() -> GrepEntry {
    GrepEntry::new(
        GrepIndexStatus::Backfilling {
            captured_seq: CommitSeq(7),
            cursor_inode_id: Some(InodeId(7)),
            pin_id: PinId::parse("pin_00000000000000000001-0000000000000009")
                .expect("valid pin id"),
        },
        None,
        vec![run_ref(1, 0, &[1])],
    )
    .expect("valid backfilling entry")
}

/// A disabled index. The status has no fields, and the entry lists neither
/// segments nor a reorganization.
pub(crate) fn sample_disabled_entry() -> GrepEntry {
    GrepEntry::new(GrepIndexStatus::Disabled {}, None, Vec::new()).expect("valid disabled entry")
}

/// Returns run `run_no` at `level`, with the segments that [`segment_ref`]
/// returns for `numbers`.
pub(crate) fn run_ref(run_no: u64, level: u32, numbers: &[u8]) -> ExtensionRunRef {
    ExtensionRunRef {
        run_no: RunNo(run_no),
        level,
        segments: numbers.iter().copied().map(segment_ref).collect(),
    }
}

pub(crate) fn segment_ref(number: u8) -> ExtensionSegmentRef {
    ExtensionSegmentRef {
        owner_namespace_id: namespace_id("docs"),
        segment_id: segment_id(number),
        row_count: 99,
        min_row_key: "gram-616263-00000000000000000000".to_owned(),
        max_row_key: "value-00000000000000000051".to_owned(),
        index_block: BlockHandle {
            offset: 128,
            stored_bytes: 48,
            decoded_bytes: 96,
            crc32c: 305_419_896,
        },
        filter_block: BlockHandle {
            offset: 112,
            stored_bytes: 16,
            decoded_bytes: 16,
            crc32c: 2_591_069_104,
        },
        filter_inline: (number == 1).then(|| "00112233445566778899aabbccddeeff".to_owned()),
    }
}

pub(crate) fn segment_id(number: u8) -> IndexSegmentId {
    IndexSegmentId::parse(format!("idx_{number:026}")).expect("valid segment id")
}

fn content_id(value: u128) -> ContentId {
    ContentId::from_be_bytes(value.to_be_bytes())
}

fn content_value(content: u128, size_bytes: u64) -> ContentValue {
    ContentValue::new(&content_id(content), size_bytes)
}

/// The rows one gram-index data block holds. Two rows carry the same gram,
/// which the format permits and readers union, and the first batch has a
/// gap that needs more than one varint byte.
fn sample_gram_postings_rows() -> Vec<IndexRow> {
    vec![
        IndexRow::gram_postings(Gram(*b"abc"), &[0, 1, 300]).expect("valid gram postings row"),
        IndexRow::gram_postings(Gram(*b"abc"), &[900, 901]).expect("valid gram postings row"),
        IndexRow::gram_postings(Gram(*b"fox"), &[7]).expect("valid gram postings row"),
    ]
}

/// `texts` rows in row-key order: two sizes of one content ID, one eligible
/// and one not, then another content ID.
fn sample_text_rows() -> Vec<IndexRow> {
    vec![
        IndexRow::text(&content_value(1, 5), true),
        IndexRow::text(&content_value(1, 12), false),
        IndexRow::text(&content_value(7, 1), true),
    ]
}

/// A revision filter's rows in row-key order: the payload of a filter block
/// over two revision keys, split into two chunks.
fn sample_filter_rows() -> Vec<IndexRow> {
    let mut filter = SegmentFilterBuilder::new(2);
    filter.insert("0000000000000000000000000a-00000000000000000003");
    filter.insert("0000000000000000000000000b-00000000000000000001");
    let payload = filter.finish();
    let (first, second) = payload.split_at(10);
    vec![IndexRow::filter(0, first), IndexRow::filter(1, second)]
}

/// A value table's rows in row-key order: the values of the `texts` sample,
/// numbered in content order.
fn sample_value_rows() -> Vec<IndexRow> {
    [
        content_value(1, 5),
        content_value(1, 12),
        content_value(7, 1),
    ]
    .iter()
    .zip(0..)
    .map(|(value, ordinal)| IndexRow::value(ordinal, value))
    .collect()
}

/// `content_revisions` rows in row-key order: one content value's revisions
/// in two files, newest first within each file, then a longer value of the
/// same chain.
fn sample_content_revision_rows() -> Vec<IndexRow> {
    let content_id = ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff").expect("valid content ID");
    [(5, 2, 4, 9), (5, 2, 1, 3), (5, 7, 2, 6), (12, 2, 5, 11)]
        .into_iter()
        .map(|(size_bytes, inode_id, revision_no, committed_seq)| {
            IndexRow::content_revision(
                &ContentValue::new(&content_id, size_bytes),
                InodeId(inode_id),
                RevisionNo(revision_no),
                CommitSeq(committed_seq),
            )
        })
        .collect()
}

/// Builds one segment of `rows` through the block builder and the target
/// block size the grep worker builds with, so a fixture pins the bytes a
/// real segment holds.
fn sample_segment(rows: &[IndexRow]) -> BuiltSegmentBlocks {
    let mut builder = SegmentBlocksBuilder::default();
    for row in rows {
        builder
            .push(&row.row_key(), &row.filter_key(), row)
            .expect("push a grep index row");
    }
    builder.build().expect("finish the grep index segment")
}

fn segment_section<'a>(bytes: &'a [u8], handle: &BlockHandle) -> &'a [u8] {
    let start = loonfs_types::usize_from_u64(handle.offset);
    &bytes[start..start + handle.stored_bytes as usize]
}

/// Reads back the block a fixture pins. The fixture stores the decompressed
/// payload, so this recompresses it and describes it with a handle before
/// handing both to the real decoder.
fn decode_golden_data_block(name: &str) -> DecodedDataBlock<IndexRow> {
    let payload = read_golden(name);
    let stored = zstd::stream::encode_all(payload.as_slice(), 0).expect("compress the block");
    let handle = BlockHandle {
        offset: 0,
        stored_bytes: u32::try_from(stored.len()).expect("the test value should fit u32"),
        decoded_bytes: u32::try_from(payload.len()).expect("the test value should fit u32"),
        crc32c: u32::try_from(crc_fast::checksum(
            crc_fast::CrcAlgorithm::Crc32Iscsi,
            &stored,
        ))
        .expect("a CRC-32 value should fit in 32 bits"),
    };
    decode_data_block_rows::<IndexRow>(&stored, &handle).expect("decode golden data block")
}

// ---------------------------------------------------------------------------
// The fixtures
// ---------------------------------------------------------------------------

/// The bytes of `entry` as its core entry, the JSON object that a namespace
/// manifest lists under `grep`.
fn entry_bytes(entry: &GrepEntry) -> Vec<u8> {
    serde_json::to_vec(&entry.encode()).expect("encode the grep entry")
}

fn assert_entry_matches_golden(fixture: &str, entry: &GrepEntry) {
    assert_matches_golden(fixture, &entry_bytes(entry));
}

fn assert_entry_golden_decodes(fixture: &str, entry: &GrepEntry) {
    let stored: ExtensionEntry =
        serde_json::from_slice(&read_golden(fixture)).expect("decode the core entry");
    assert_eq!(
        &GrepEntry::decode(&stored).expect("decode the golden entry"),
        entry
    );
}

#[test]
fn grep_entry_matches_golden_bytes() {
    assert_entry_matches_golden(ACTIVE_ENTRY_FIXTURE, &sample_active_entry(CommitSeq(11), 5));
}

#[test]
fn grep_entry_golden_decodes_to_sample() {
    assert_entry_golden_decodes(ACTIVE_ENTRY_FIXTURE, &sample_active_entry(CommitSeq(11), 5));
}

#[test]
fn grep_entry_backfilling_matches_golden_bytes() {
    assert_entry_matches_golden(BACKFILLING_ENTRY_FIXTURE, &sample_backfilling_entry());
}

#[test]
fn grep_entry_backfilling_golden_decodes_to_sample() {
    assert_entry_golden_decodes(BACKFILLING_ENTRY_FIXTURE, &sample_backfilling_entry());
}

#[test]
fn grep_entry_disabled_matches_golden_bytes() {
    assert_entry_matches_golden(DISABLED_ENTRY_FIXTURE, &sample_disabled_entry());
}

#[test]
fn grep_entry_disabled_golden_decodes_to_sample() {
    assert_entry_golden_decodes(DISABLED_ENTRY_FIXTURE, &sample_disabled_entry());
}

/// Checks that `rows` encode as one data block whose payload matches
/// `fixture`.
fn assert_rows_match_golden(fixture: &str, rows: &[IndexRow]) {
    let built = sample_segment(rows);
    let index = decode_index_block(segment_section(&built.bytes, &built.index), &built.index)
        .expect("decode the segment index");
    assert_eq!(index.len(), 1, "the row fixture should be one data block");
    // Compare the decompressed block payload: zstd frames may differ across
    // zstd versions, the entry encoding (which the format defines) may not.
    assert_matches_golden(
        fixture,
        &unzstd(segment_section(&built.bytes, &index[0].block)),
    );
}

/// Checks that the block `fixture` stores decodes to `rows` under their keys.
fn assert_rows_golden_decodes(fixture: &str, rows: &[IndexRow]) {
    let block = decode_golden_data_block(fixture);
    assert_eq!(block.rows, rows);
    assert_eq!(
        block.row_keys,
        rows.iter().map(IndexRow::row_key).collect::<Vec<_>>()
    );
}

#[test]
fn grep_segment_gram_postings_matches_golden_bytes() {
    assert_rows_match_golden(GRAM_POSTINGS_BLOCK_FIXTURE, &sample_gram_postings_rows());
}

#[test]
fn grep_segment_gram_postings_golden_decodes_to_sample_rows() {
    assert_rows_golden_decodes(GRAM_POSTINGS_BLOCK_FIXTURE, &sample_gram_postings_rows());
    let IndexRow::GramPostings(row) =
        &decode_golden_data_block(GRAM_POSTINGS_BLOCK_FIXTURE).rows[0]
    else {
        panic!("the first row should be a gram postings row");
    };
    assert_eq!(row.ordinals().expect("unpack the first batch"), [0, 1, 300]);
}

#[test]
fn grep_segment_texts_matches_golden_bytes() {
    assert_rows_match_golden(TEXTS_BLOCK_FIXTURE, &sample_text_rows());
}

#[test]
fn grep_segment_texts_golden_decodes_to_sample_rows() {
    assert_rows_golden_decodes(TEXTS_BLOCK_FIXTURE, &sample_text_rows());
}

#[test]
fn grep_segment_content_revisions_matches_golden_bytes() {
    assert_rows_match_golden(
        CONTENT_REVISIONS_BLOCK_FIXTURE,
        &sample_content_revision_rows(),
    );
}

#[test]
fn grep_segment_content_revisions_golden_decodes_to_sample_rows() {
    assert_rows_golden_decodes(
        CONTENT_REVISIONS_BLOCK_FIXTURE,
        &sample_content_revision_rows(),
    );
}

#[test]
fn grep_segment_values_matches_golden_bytes() {
    assert_rows_match_golden(VALUES_BLOCK_FIXTURE, &sample_value_rows());
}

#[test]
fn grep_segment_values_golden_decodes_to_sample_rows() {
    assert_rows_golden_decodes(VALUES_BLOCK_FIXTURE, &sample_value_rows());
}

#[test]
fn grep_segment_filters_matches_golden_bytes() {
    assert_rows_match_golden(FILTERS_BLOCK_FIXTURE, &sample_filter_rows());
}

#[test]
fn grep_segment_filters_golden_decodes_to_sample_rows() {
    let rows = sample_filter_rows();
    assert_rows_golden_decodes(FILTERS_BLOCK_FIXTURE, &rows);
    let payload: Vec<u8> = rows
        .iter()
        .flat_map(|row| match row {
            IndexRow::Filter(row) => row.payload.to_vec(),
            _ => panic!("the sample has filter rows only"),
        })
        .collect();
    let filter = SegmentFilter::from_payload(&payload).expect("the chunks form a filter");
    assert!(filter.may_contain("0000000000000000000000000a-00000000000000000003"));
}
