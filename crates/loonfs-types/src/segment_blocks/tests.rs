//! Unit tests for block-segment encoding, filters, and decoding bounds.

use super::*;
use crate::{CommitSeq, InodeId, InodeKind};
use std::assert_matches;

#[test]
fn decoded_byte_accounting_includes_closed_and_open_blocks() {
    let mut builder =
        SegmentBlocksBuilder::new(NonZeroUsize::new(512).expect("nonzero block target"));
    for index in 0..17 {
        let (key, filter, row) = inode_row(index);
        builder.push(&key, &filter, &row).expect("encode row");
    }
    let decoded_bytes = builder.decoded_data_bytes();
    assert_eq!(builder.row_count(), 17);
    assert!(
        !builder.index.is_empty(),
        "completed blocks are already compressed"
    );
    let built = builder.build().expect("finish segment");
    let start = crate::usize_from_u64(built.index.offset);
    let end = start + built.index.stored_bytes as usize;
    let index = decode_index_block(&built.bytes[start..end], &built.index).expect("index");
    assert_eq!(
        decoded_bytes,
        index
            .iter()
            .map(|entry| entry.block.decoded_bytes as usize)
            .sum::<usize>(),
    );
}

fn inode_row(inode_id: u64) -> (String, String, MetadataRow) {
    let row = MetadataRow::Inode(crate::metadata_rows::InodeRecord {
        inode_id: InodeId(u128::from(inode_id)),
        inode_kind: InodeKind::File,
        committed_seq: CommitSeq(inode_id),
        commit_id: crate::CommitId::parse(format!("c_row_{inode_id}")).expect("valid commit id"),
        committed_by: crate::ActorId::loonfs(),
        committed_at_ms: inode_id,
    });
    let key = row.row_key();
    (key.clone(), key, row)
}

fn build_segment(rows: usize) -> BuiltSegmentBlocks {
    let mut builder = SegmentBlocksBuilder::default();
    for index in 0..rows {
        let (key, filter_key, row) = inode_row(index as u64);
        builder.push(&key, &filter_key, &row).expect("push row");
    }
    builder.build().expect("finish segment")
}

const SHARED_SEGMENT_ROWS: usize = 5_000;

fn shared_segment() -> &'static BuiltSegmentBlocks {
    static SEGMENT: std::sync::OnceLock<BuiltSegmentBlocks> = std::sync::OnceLock::new();
    SEGMENT.get_or_init(|| build_segment(SHARED_SEGMENT_ROWS))
}

fn section<'a>(bytes: &'a [u8], handle: &BlockHandle) -> &'a [u8] {
    let start = crate::usize_from_u64(handle.offset);
    &bytes[start..start + handle.stored_bytes as usize]
}

fn encode_index(entries: &[SegmentIndexEntry]) -> (Vec<u8>, BlockHandle) {
    let mut payload = Vec::new();
    ciborium::ser::into_writer(entries, &mut payload).expect("encode index");
    let mut bytes = Vec::new();
    let handle = append_section(&mut bytes, &payload, true).expect("append section");
    (bytes, handle)
}

fn index_entry(last_row_key: &str, offset: u64, stored_bytes: u32) -> SegmentIndexEntry {
    SegmentIndexEntry {
        last_row_key: last_row_key.to_owned(),
        block: BlockHandle {
            offset,
            stored_bytes,
            decoded_bytes: stored_bytes,
            crc32c: 0,
        },
    }
}

#[test]
fn index_narrows_point_lookups_to_one_block() {
    let built = shared_segment();
    let index =
        decode_index_block(section(&built.bytes, &built.index), &built.index).expect("index");
    let (key, _, row) = inode_row(3_217);
    let upper = format!("{key}\0");
    let range = index_blocks_for_key_range(&index, &key, Some(&upper));
    assert_eq!(range.len(), 1, "a point lookup should touch one block");
    let entry = &index[range.start];
    let block =
        decode_data_block(section(&built.bytes, &entry.block), &entry.block).expect("block");
    let position = block
        .row_keys
        .binary_search_by(|candidate| candidate.as_str().cmp(key.as_str()))
        .expect("row should be present");
    assert_eq!(block.rows[position], row);
}

#[test]
fn key_range_scan_covers_exactly_the_matching_blocks() {
    let built = shared_segment();
    let index =
        decode_index_block(section(&built.bytes, &built.index), &built.index).expect("index");
    let lower = inode_row(1_000).0;
    let upper = inode_row(1_500).0;
    let range = index_blocks_for_key_range(&index, &lower, Some(&upper));
    let mut keys = Vec::new();
    for entry in &index[range] {
        let block =
            decode_data_block(section(&built.bytes, &entry.block), &entry.block).expect("block");
        keys.extend(block.row_keys);
    }
    let keys: Vec<&String> = keys
        .iter()
        .filter(|key| key.as_str() >= lower.as_str() && key.as_str() < upper.as_str())
        .collect();
    assert_eq!(keys.len(), 500);
}

#[test]
fn out_of_order_and_empty_segments_are_rejected() {
    let mut builder = SegmentBlocksBuilder::default();
    let (key_b, filter_b, row_b) = inode_row(2);
    let (key_a, filter_a, row_a) = inode_row(1);
    builder.push(&key_b, &filter_b, &row_b).expect("first row");
    let error = builder
        .push(&key_a, &filter_a, &row_a)
        .expect_err("descending key should be rejected");
    assert_matches!(error, SegmentBlockCodecError::RowKeysOutOfOrder { .. });

    let error = SegmentBlocksBuilder::default()
        .build()
        .expect_err("empty segment should be rejected");
    assert_matches!(error, SegmentBlockCodecError::EmptySegment);
}

#[test]
fn max_key_survives_a_last_row_that_closes_its_block() {
    // Calibrate the target so the crossing falls on the final row.
    // Building the same rows as one block reports how many entry bytes
    // they occupy: a block payload is the entries, then one `u32` per
    // restart point, then the restart count.
    let rows = 100usize;
    let single_block = build_segment(rows);
    let calibration = decode_index_block(
        section(&single_block.bytes, &single_block.index),
        &single_block.index,
    )
    .expect("index");
    assert_eq!(calibration.len(), 1, "the calibration segment is one block");
    let restarts = rows.div_ceil(RESTART_INTERVAL);
    let entry_bytes = calibration[0].block.decoded_bytes as usize - 4 * restarts - 4;

    let mut builder =
        SegmentBlocksBuilder::new(NonZeroUsize::new(entry_bytes).expect("positive target"));
    for index in 0..rows {
        let (key, filter_key, row) = inode_row(index as u64);
        builder.push(&key, &filter_key, &row).expect("push row");
    }
    let built = builder.build().expect("finish segment");

    let expected_max = inode_row((rows - 1) as u64).0;
    assert_eq!(built.min_row_key, inode_row(0).0);
    assert_eq!(built.max_row_key, expected_max);
    assert_eq!(built.row_count, rows as u64);
    // The last push closed the block, so `build` appended nothing. The
    // object must still be the same bytes a larger target produces.
    assert_eq!(built.bytes, single_block.bytes);
    let index =
        decode_index_block(section(&built.bytes, &built.index), &built.index).expect("index");
    assert_eq!(index.len(), 1);
    assert_eq!(index[0].last_row_key, expected_max);
}

#[test]
fn block_geometry_does_not_change_the_segment_key_range() {
    let rows = 400usize;
    let expected: Vec<String> = (0..rows).map(|index| inode_row(index as u64).0).collect();
    for target in [1usize, 64, 257, 1_024, 4_096, 65_536] {
        let mut builder =
            SegmentBlocksBuilder::new(NonZeroUsize::new(target).expect("positive target"));
        for index in 0..rows {
            let (key, filter_key, row) = inode_row(index as u64);
            builder.push(&key, &filter_key, &row).expect("push row");
        }
        let built = builder.build().expect("finish segment");
        assert_eq!(built.min_row_key, expected[0], "target {target}");
        assert_eq!(
            &built.max_row_key,
            expected.last().expect("rows"),
            "target {target}"
        );
        assert_eq!(built.row_count, rows as u64, "target {target}");

        let index =
            decode_index_block(section(&built.bytes, &built.index), &built.index).expect("index");
        let mut recovered = Vec::new();
        for entry in &index {
            let block = decode_data_block(section(&built.bytes, &entry.block), &entry.block)
                .expect("data block");
            // Each block still names its own last row key, so the index the
            // reader binary-searches keeps its shape.
            assert_eq!(
                block.row_keys.last().expect("blocks are never empty"),
                &entry.last_row_key,
                "target {target}"
            );
            recovered.extend(block.row_keys);
        }
        assert_eq!(recovered, expected, "target {target}");
        assert_eq!(
            index.last().expect("blocks").last_row_key,
            built.max_row_key,
            "target {target}"
        );
    }
}

#[test]
fn a_descending_row_after_a_block_boundary_is_rejected() {
    let mut builder = SegmentBlocksBuilder::new(NonZeroUsize::MIN);
    let (key_high, filter_high, row_high) = inode_row(9);
    builder
        .push(&key_high, &filter_high, &row_high)
        .expect("first row");
    let (key_low, filter_low, row_low) = inode_row(3);
    let error = builder
        .push(&key_low, &filter_low, &row_low)
        .expect_err("a descending key across a block boundary should be rejected");
    assert_matches!(
        &error,
        SegmentBlockCodecError::RowKeysOutOfOrder { previous, offered }
            if previous == &key_high && offered == &key_low,
        "unexpected error: {error}"
    );
}

#[test]
fn adjacent_equal_keys_are_permitted() {
    let mut builder = SegmentBlocksBuilder::default();
    let (key, filter_key, row) = inode_row(7);
    builder.push(&key, &filter_key, &row).expect("first copy");
    builder.push(&key, &filter_key, &row).expect("second copy");
    let built = builder.build().expect("finish");
    assert_eq!(built.row_count, 2);
}

#[test]
fn corrupted_sections_fail_their_checksums() {
    let built = build_segment(200);
    let index =
        decode_index_block(section(&built.bytes, &built.index), &built.index).expect("index");

    let mut corrupted = built.bytes.clone();
    let target = crate::usize_from_u64(index[0].block.offset) + 3;
    corrupted[target] ^= 0xff;
    let error = decode_data_block(section(&corrupted, &index[0].block), &index[0].block)
        .expect_err("corrupted data block should fail");
    assert_matches!(error, SegmentBlockCodecError::ChecksumMismatch { .. });

    let mut corrupted = built.bytes.clone();
    let target = crate::usize_from_u64(built.index.offset) + 3;
    corrupted[target] ^= 0xff;
    let error = decode_index_block(section(&corrupted, &built.index), &built.index)
        .expect_err("corrupted index should fail");
    assert_matches!(error, SegmentBlockCodecError::ChecksumMismatch { .. });

    let mut corrupted = built.bytes.clone();
    let target = crate::usize_from_u64(built.filter.offset) + 12;
    corrupted[target] ^= 0xff;
    let error = decode_filter_block(section(&corrupted, &built.filter), &built.filter)
        .expect_err("corrupted filter should fail");
    assert_matches!(error, SegmentBlockCodecError::ChecksumMismatch { .. });
}

#[test]
fn block_expansion_stops_after_the_first_byte_beyond_the_declared_length() {
    let mut bytes = Vec::new();
    let mut handle = append_section(&mut bytes, &[0; 1024], true).expect("compress block");
    handle.decoded_bytes = 8;
    assert_eq!(
        decode_data_block(&bytes, &handle).expect_err("expansion exceeds the handle"),
        SegmentBlockCodecError::DecodedLengthMismatch {
            expected: 8,
            actual: 9,
        }
    );
}

#[test]
fn a_large_declared_length_does_not_require_a_large_initial_allocation() {
    let mut bytes = Vec::new();
    let mut handle = append_section(&mut bytes, &[0; 4], true).expect("compress block");
    handle.decoded_bytes = u32::MAX;
    assert_eq!(
        decode_index_block(&bytes, &handle).expect_err("declared length exceeds expansion"),
        SegmentBlockCodecError::DecodedLengthMismatch {
            expected: u32::MAX,
            actual: 4,
        }
    );
}

#[test]
fn truncated_index_and_filter_sections_are_rejected_before_decoding() {
    let built = shared_segment();
    let index = section(&built.bytes, &built.index);
    let filter = section(&built.bytes, &built.filter);
    for (handle, error) in [
        (
            built.index,
            decode_index_block(&index[..index.len() - 1], &built.index)
                .expect_err("truncated index"),
        ),
        (
            built.filter,
            decode_filter_block(&filter[..filter.len() - 1], &built.filter)
                .expect_err("truncated filter"),
        ),
    ] {
        assert_eq!(
            error,
            SegmentBlockCodecError::StoredLengthMismatch {
                expected: handle.stored_bytes,
                actual: handle.stored_bytes as usize - 1,
            }
        );
    }
}

#[test]
fn filter_headers_reject_zero_bits_and_invalid_hash_counts() {
    for (n_hashes, bit_len) in [(FILTER_HASH_COUNT, 0u64), (u32::MAX, 64), (0, 64)] {
        let mut payload = Vec::new();
        payload.extend_from_slice(&n_hashes.to_le_bytes());
        payload.extend_from_slice(&bit_len.to_le_bytes());
        payload.resize(12 + crate::usize_from_u64(bit_len.div_ceil(8)), 0xff);
        let mut bytes = Vec::new();
        let handle = append_section(&mut bytes, &payload, false).expect("filter section");
        assert_matches!(
            decode_filter_block(&bytes, &handle),
            Err(SegmentBlockCodecError::Malformed(_))
        );
    }
}

#[test]
fn restart_entries_bound_shared_prefix_reconstruction() {
    let mut payload = Vec::new();
    let mut restart_offsets = Vec::new();
    for index in 0..=RESTART_INTERVAL {
        if index.is_multiple_of(RESTART_INTERVAL) {
            restart_offsets.push(u32::try_from(payload.len()).expect("the offset should fit u32"));
        }
        write_varint(&mut payload, u64::from(index != 0));
        write_varint(&mut payload, u64::from(index == 0));
        if index == 0 {
            payload.push(b'a');
        }
        write_varint(&mut payload, 1);
        payload.push(0);
    }
    for offset in restart_offsets {
        payload.extend_from_slice(&offset.to_le_bytes());
    }
    payload.extend_from_slice(&2u32.to_le_bytes());
    let mut bytes = Vec::new();
    let handle = append_section(&mut bytes, &payload, true).expect("data section");
    assert_eq!(
        decode_data_block_rows::<u8>(&bytes, &handle).expect_err("shared restart prefix"),
        SegmentBlockCodecError::Malformed("restart entry has a shared prefix".to_owned())
    );
}

#[test]
fn restart_offsets_must_name_their_entries() {
    let payload = [0, 1, b'a', 1, 0, 1, 0, 0, 0, 1, 0, 0, 0];
    let mut bytes = Vec::new();
    let handle = append_section(&mut bytes, &payload, true).expect("data section");
    assert_eq!(
        decode_data_block_rows::<u8>(&bytes, &handle).expect_err("wrong restart offset"),
        SegmentBlockCodecError::Malformed("restart offset disagrees with its entry".to_owned())
    );
}

#[test]
fn varints_reject_overflow_in_the_tenth_byte() {
    let mut bytes = [0xff; 10];
    for last in [2, 0x80] {
        bytes[9] = last;
        assert_eq!(
            read_varint(&bytes, &mut 0).expect_err("overflowing varint"),
            SegmentBlockCodecError::Malformed("varint exceeds 64 bits".to_owned())
        );
    }
    bytes[9] = 1;
    assert_eq!(
        read_varint(&bytes, &mut 0).expect("largest varint"),
        u64::MAX
    );
}

#[test]
fn filter_has_no_false_negatives_and_few_false_positives() {
    for (mut builder, min_bits) in [
        (SegmentFilterBuilder::new(0), FILTER_MIN_BITS),
        (SegmentFilterBuilder::new(1), FILTER_MIN_BITS),
        (SegmentFilterBuilder::keep_filter(0), MIN_KEEP_FILTER_BITS),
        (SegmentFilterBuilder::keep_filter(1), MIN_KEEP_FILTER_BITS),
        (SegmentFilterBuilder::with_sizing(1, 1, 1), FILTER_MIN_BITS),
        (SegmentFilterBuilder::with_sizing(0, 0, 0), FILTER_MIN_BITS),
    ] {
        builder.insert("inserted");
        let payload = builder.finish();
        assert_eq!(payload.len(), 12 + min_bits / 8);
        let filter = SegmentFilter::from_payload(&payload).expect("a valid filter");
        assert!(filter.may_contain("inserted"));
    }
    let rows = 2_000;
    let built = build_segment(rows);
    let filter =
        decode_filter_block(section(&built.bytes, &built.filter), &built.filter).expect("filter");
    for index in 0..rows {
        let (key, _, _) = inode_row(index as u64);
        assert!(filter.may_contain(&key), "inserted key must stay positive");
    }
    let mut false_positives = 0usize;
    let probes = 10_000usize;
    for index in 0..probes {
        let (absent, _, _) = inode_row((rows + 10_000 + index) as u64);
        if filter.may_contain(&absent) {
            false_positives += 1;
        }
    }
    let rate = false_positives as f64 / probes as f64;
    assert!(rate < 0.02, "false positive rate {rate} exceeds 2%");
}

/// Checks that a filter key without a row of its own is admitted and is
/// counted when the filter is sized.
#[test]
fn an_extra_filter_key_is_admitted_and_sizes_the_filter() {
    let rows = 100_usize;
    let mut builder = SegmentBlocksBuilder::default();
    for index in 0..rows {
        let (key, filter_key, row) = inode_row(index as u64);
        builder.push(&key, &filter_key, &row).expect("push row");
    }
    builder.push_filter_key("group-probe");
    let built = builder.build().expect("segment");
    let filter =
        decode_filter_block(section(&built.bytes, &built.filter), &built.filter).expect("filter");
    assert!(filter.may_contain("group-probe"));
    assert_eq!(
        built.filter.decoded_bytes as usize,
        12 + ((rows + 1) * FILTER_BITS_PER_KEY).div_ceil(8)
    );
}

/// Checks that an extent filter sits between the data blocks and the
/// filter, finds every name pushed into it, and reports few names that
/// weren't.
#[test]
fn an_extent_filter_round_trips_ahead_of_the_filter() {
    let names = 1_000_usize;
    let mut builder = SegmentBlocksBuilder::default().with_extent_filter();
    for index in 0..names {
        let (key, filter_key, row) = inode_row(index as u64);
        builder.push(&key, &filter_key, &row).expect("push row");
        builder.push_extent_name(&format!("con_{index:026}"));
    }
    let built = builder.build().expect("segment");
    let extent_filter = built.extent_filter.expect("an extent filter");
    assert_eq!(
        extent_filter.offset + u64::from(extent_filter.stored_bytes),
        built.filter.offset
    );
    assert_eq!(
        built.filter.offset + u64::from(built.filter.stored_bytes),
        built.index.offset
    );
    assert_eq!(
        extent_filter.decoded_bytes as usize,
        12 + (names * EXTENT_FILTER_BITS_PER_KEY).div_ceil(8)
    );
    let filter = decode_filter_block(section(&built.bytes, &extent_filter), &extent_filter)
        .expect("extent filter");
    for index in 0..names {
        assert!(filter.may_contain(&format!("con_{index:026}")));
    }
    let false_positives = (names..names + 10_000)
        .filter(|index| filter.may_contain(&format!("con_{index:026}")))
        .count();
    assert!(
        false_positives <= 10,
        "{false_positives} of 10,000 absent names matched"
    );
}

/// Checks that a builder with an extent filter and no names emits the
/// smallest filter, and that a builder without one emits none.
#[test]
fn an_empty_extent_filter_is_emitted_only_when_enabled() {
    for enabled in [false, true] {
        let mut builder = SegmentBlocksBuilder::default();
        if enabled {
            builder = builder.with_extent_filter();
        }
        let (key, filter_key, row) = inode_row(1);
        builder.push(&key, &filter_key, &row).expect("push row");
        let built = builder.build().expect("segment");
        assert_eq!(
            built
                .extent_filter
                .map(|handle| handle.decoded_bytes as usize),
            enabled.then_some(12 + MIN_KEEP_FILTER_BITS / 8)
        );
    }
}

#[test]
fn decoding_rejects_out_of_order_rows_in_a_block() {
    // A hostile block with descending keys and a valid CRC: encode two
    // full-key entries in the wrong order through the private helpers.
    let mut entries = Vec::new();
    for inode in [9u64, 3u64] {
        let (key, _, row) = inode_row(inode);
        let mut row_bytes = Vec::new();
        ciborium::ser::into_writer(&row, &mut row_bytes).expect("encode row");
        write_varint(&mut entries, 0);
        write_varint(&mut entries, key.len() as u64);
        entries.extend_from_slice(key.as_bytes());
        write_varint(&mut entries, row_bytes.len() as u64);
        entries.extend_from_slice(&row_bytes);
    }
    let mut payload = entries;
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(&1u32.to_le_bytes());
    let mut bytes = Vec::new();
    let handle = append_section(&mut bytes, &payload, true).expect("append section");

    let error = decode_data_block(&bytes, &handle).expect_err("descending rows should be rejected");
    assert_matches!(
        &error,
        SegmentBlockCodecError::Malformed(message) if message.contains("row-key order"),
        "unexpected error: {error}"
    );
}

#[test]
fn decoding_rejects_a_shared_prefix_inside_a_utf8_code_point() {
    let (_, _, row) = inode_row(1);
    let mut row_bytes = Vec::new();
    ciborium::ser::into_writer(&row, &mut row_bytes).expect("encode row");
    let mut payload = Vec::new();
    write_varint(&mut payload, 0);
    write_varint(&mut payload, "é".len() as u64);
    payload.extend_from_slice("é".as_bytes());
    write_varint(&mut payload, row_bytes.len() as u64);
    payload.extend_from_slice(&row_bytes);
    write_varint(&mut payload, 1);
    write_varint(&mut payload, 0);
    write_varint(&mut payload, row_bytes.len() as u64);
    payload.extend_from_slice(&row_bytes);
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(&1u32.to_le_bytes());

    let mut bytes = Vec::new();
    let handle = append_section(&mut bytes, &payload, true).expect("append section");
    let error =
        decode_data_block(&bytes, &handle).expect_err("a partial utf-8 prefix should be rejected");
    assert_matches!(
        &error,
        SegmentBlockCodecError::Malformed(message)
            if message == "shared prefix exceeds previous key"
    );
}

#[test]
fn decoding_rejects_out_of_order_index_entries() {
    for (entries, expected) in [
        (
            [
                index_entry("inode-00000000000000000009", 0, 1),
                index_entry("inode-00000000000000000003", 0, 1),
            ],
            "key order",
        ),
        (
            [index_entry("a", 10, 1), index_entry("b", 5, 1)],
            "does not start where",
        ),
        (
            [index_entry("a", 10, 5), index_entry("b", 14, 1)],
            "does not start where",
        ),
        (
            [index_entry("a", 0, 10), index_entry("b", 20, 1)],
            "does not start where",
        ),
        (
            [
                index_entry("a", u64::MAX - 110, 100),
                index_entry("b", u64::MAX - 10, 100),
            ],
            "byte range overflows",
        ),
    ] {
        let (bytes, handle) = encode_index(&entries);

        let error = decode_index_block(&bytes, &handle).expect_err(expected);
        assert_matches!(
            &error,
            SegmentBlockCodecError::Malformed(message) if message.contains(expected),
            "unexpected error: {error}"
        );
    }
}
