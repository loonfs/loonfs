//! Block-granular encoding for metadata and derived-index segments.
//!
//! A segment is a run of data blocks, then one filter block and one index
//! block, for any CBOR row type, including [`MetadataRow`] and grep index
//! rows. A `content_layouts` segment also has an extent filter between its
//! data blocks and its filter block. [Appendix A.7](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a7-block-segment-encoding)
//! defines the byte layout.

use crate::format::manifest::MetadataRow;
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::num::NonZeroUsize;
use thiserror::Error;
use xxhash_rust::xxh64::xxh64;

/// Target uncompressed size of one data block.
pub const DEFAULT_TARGET_BLOCK_BYTES: NonZeroUsize = NonZeroUsize::new(64 * 1024).unwrap();
/// Number of delta runs at which a compaction is due, whatever the runs' sizes.
pub const DEFAULT_MAX_DELTA_RUNS: NonZeroUsize = NonZeroUsize::new(8).unwrap();
/// Number of similar-size metadata runs in one family group that makes a
/// compaction due.
pub const DEFAULT_MIN_COMPACTION_RUNS: NonZeroUsize = NonZeroUsize::new(4).unwrap();
/// Target number of rows in one immutable segment.
pub const DEFAULT_MAX_ROWS_PER_SEGMENT: NonZeroUsize = NonZeroUsize::new(65_536).unwrap();
/// Target decoded data bytes in one metadata segment.
///
/// A row is never split.
pub const DEFAULT_TARGET_SEGMENT_BYTES: usize = 8 * 1024 * 1024;
/// Maximum number of runs read by one compaction unit.
pub const DEFAULT_MAX_COMPACTION_INPUT_RUNS: NonZeroUsize = NonZeroUsize::new(8).unwrap();
// A compaction above a group's oldest run needs two runs, and a group at the delta
// run limit has enough delta runs for one unit to compact the minimum.
const _: () = assert!(
    2 <= DEFAULT_MIN_COMPACTION_RUNS.get()
        && DEFAULT_MIN_COMPACTION_RUNS.get() <= DEFAULT_MAX_DELTA_RUNS.get()
        && DEFAULT_MIN_COMPACTION_RUNS.get() <= DEFAULT_MAX_COMPACTION_INPUT_RUNS.get()
);
/// Maximum decoded input that one build or compaction keeps in memory at once.
pub const DEFAULT_MAX_COMPACTION_INPUT_BYTES: NonZeroUsize =
    NonZeroUsize::new(64 * 1024 * 1024).unwrap();
/// Maximum stored filter size embedded in a segment descriptor.
pub const DEFAULT_MAX_INLINE_FILTER_BYTES: u32 = 1024;
/// Entries between restart points inside a data block.
pub const RESTART_INTERVAL: usize = 16;
/// Bloom filter sizing: bits reserved per inserted filter key.
pub const FILTER_BITS_PER_KEY: usize = 10;
/// Bloom filter probe count, chosen for [`FILTER_BITS_PER_KEY`].
pub const FILTER_HASH_COUNT: u32 = 7;
/// Bloom filter sizing: the fewest bits of a filter.
pub const FILTER_MIN_BITS: usize = 64;
/// Extent filter sizing: bits reserved per inserted content object name.
pub const EXTENT_FILTER_BITS_PER_KEY: usize = 20;
/// Bloom filter sizing: the fewest bits of a filter whose match keeps data.
///
/// The extent filter, a compaction's layout filter and kept inode filter,
/// and grep's pruning filters keep data that LoonFS would otherwise delete
/// when they match a key. A filter with 10 bits for each key matches another
/// key about once in 100 to 120 at any size, and a 4,096-bit filter of 30
/// keys matches about once in 1.2 billion.
pub const MIN_KEEP_FILTER_BITS: usize = 4096;

const FILTER_HASH_SEED_ONE: u64 = 0;
const FILTER_HASH_SEED_TWO: u64 = 0x9e37_79b9_7f4a_7c15;
/// Compression level for every zstd-compressed durable artifact.
pub(crate) const ZSTD_LEVEL: i32 = 3;

/// Where one stored section is inside a segment object, and how to
/// verify it: `stored_bytes`, `decoded_bytes`, and the CRC32C of the stored bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockHandle {
    /// Zero-based byte offset of the section within its immutable segment object.
    pub offset: u64,
    /// Number of bytes to range-read and checksum before decoding.
    pub stored_bytes: u32,
    /// Expected byte length after optional section decompression.
    pub decoded_bytes: u32,
    /// CRC32C over the exact `stored_bytes` bytes at `offset`.
    pub crc32c: u32,
}

/// One index entry: the last row key of a data block plus its handle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentIndexEntry {
    /// Greatest row key in `block`, used to binary-search candidate blocks.
    pub last_row_key: String,
    /// Data-section location and integrity metadata.
    pub block: BlockHandle,
}

/// A finished segment: the object bytes plus everything the manifest
/// descriptor must include to read them back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltSegmentBlocks {
    /// Complete immutable object body: the data sections, the extent filter
    /// when there is one, the filter, and the index section.
    pub bytes: Vec<u8>,
    /// Handle callers persist in the segment descriptor to bootstrap reads.
    pub index: BlockHandle,
    /// Handle callers persist for negative point-lookup filtering.
    pub filter: BlockHandle,
    /// Handle of the filter over the content object names that the rows'
    /// extents reference, present when the builder enabled it.
    pub extent_filter: Option<BlockHandle>,
    /// Number of rows accepted by the builder, including adjacent duplicate keys.
    pub row_count: u64,
    /// Least row key in the non-empty segment.
    pub min_row_key: String,
    /// Greatest row key in the non-empty segment.
    pub max_row_key: String,
}

impl BuiltSegmentBlocks {
    /// Encodes a small stored filter for an inline segment descriptor.
    pub fn inline_filter_hex(&self) -> Option<String> {
        (self.filter.stored_bytes <= DEFAULT_MAX_INLINE_FILTER_BYTES).then(|| {
            let start = crate::usize_from_u64(self.filter.offset);
            crate::format::hex::hex_encode_bytes(
                &self.bytes[start..start + self.filter.stored_bytes as usize],
            )
        })
    }
}

/// Returns whether a non-empty segment key range can intersect a query range.
pub fn key_range_may_intersect(
    min_row_key: &str,
    max_row_key: &str,
    row_count: u64,
    lower_bound: &str,
    upper_bound: Option<&str>,
) -> bool {
    row_count != 0
        && max_row_key >= lower_bound
        && upper_bound.is_none_or(|upper_bound| min_row_key < upper_bound)
}

/// One decoded data block: row keys and rows, parallel and in key order.
///
/// The row type defaults to [`MetadataRow`]; index segments decode their own
/// row payload through [`decode_data_block_rows`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedDataBlock<R = MetadataRow> {
    /// Reconstructed row keys in the same ascending order as `rows`.
    pub row_keys: Vec<String>,
    /// Decoded row payloads positionally paired with `row_keys`.
    pub rows: Vec<R>,
}

/// A decoded bloom filter; returns "definitely absent" or "maybe present".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFilter {
    n_hashes: u32,
    bit_len: u64,
    bits: Vec<u8>,
}

/// Describes a violation encountered while building or validating an SST section.
///
/// See [metadata segments](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a7-block-segment-encoding).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum SegmentBlockCodecError {
    /// Reports a request to finish a segment before any row was supplied.
    #[error("segment must contain at least one row")]
    EmptySegment,
    /// Reports a builder input that would violate durable ascending row-key order.
    #[error("row key `{offered}` is not in ascending order after `{previous}`")]
    RowKeysOutOfOrder {
        /// Last key the builder accepted.
        previous: String,
        /// Descending key rejected by the builder.
        offered: String,
    },
    /// Reports a range-read body whose byte count disagrees with its handle.
    #[error("stored bytes length {actual} does not match handle length {expected}")]
    StoredLengthMismatch {
        /// Stored byte count recorded in the persisted `BlockHandle`.
        expected: u32,
        /// Byte count returned to the decoder.
        actual: usize,
    },
    /// Reports stored section bytes that fail the CRC32C recorded in their handle.
    #[error("block checksum mismatch: expected {expected:#010x}, actual {actual:#010x}")]
    ChecksumMismatch {
        /// CRC32C recorded in the persisted `BlockHandle`.
        expected: u32,
        /// CRC32C recomputed from the supplied stored bytes.
        actual: u32,
    },
    /// Reports a section whose decompressed size disagrees with its handle.
    #[error("decoded length {actual} does not match handle length {expected}")]
    DecodedLengthMismatch {
        /// Decoded byte count recorded in the persisted `BlockHandle`.
        expected: u32,
        /// Byte count produced by section decompression.
        actual: usize,
    },
    /// Reports structurally invalid framing, ordering, UTF-8, or filter metadata.
    #[error("malformed block: {0}")]
    Malformed(String),
    /// Reports a CBOR or zstd failure while encoding or decoding a section.
    #[error("block codec error: {0}")]
    Codec(String),
}

/// Builds one segment's blocks from rows fed in ascending row-key order.
#[derive(Debug)]
#[must_use]
pub struct SegmentBlocksBuilder {
    target_block_bytes: usize,
    entries: Vec<u8>,
    restarts: Vec<u32>,
    entry_count: usize,
    /// Last row key the builder accepted. It anchors prefix compression
    /// inside a block, floors the ascending-order guard, and becomes the
    /// segment's max row key. A block's first entry stores its key in full
    /// regardless, because a restart point always begins a block.
    previous_key: String,
    bytes: Vec<u8>,
    index: Vec<SegmentIndexEntry>,
    decoded_data_bytes: usize,
    filter_hashes: Vec<(u64, u64)>,
    /// The hashes of the extent names, or `None` when the segment has no
    /// extent filter.
    extent_hashes: Option<Vec<(u64, u64)>>,
    row_count: u64,
    min_row_key: String,
}

impl Default for SegmentBlocksBuilder {
    fn default() -> Self {
        Self::new(DEFAULT_TARGET_BLOCK_BYTES)
    }
}

impl SegmentBlocksBuilder {
    /// Creates a builder that closes a data block after reaching the target decoded byte size.
    pub fn new(target_block_bytes: NonZeroUsize) -> Self {
        Self {
            target_block_bytes: target_block_bytes.get(),
            entries: Vec::new(),
            restarts: Vec::new(),
            entry_count: 0,
            previous_key: String::new(),
            bytes: Vec::new(),
            index: Vec::new(),
            decoded_data_bytes: 0,
            filter_hashes: Vec::new(),
            extent_hashes: None,
            row_count: 0,
            min_row_key: String::new(),
        }
    }

    /// Returns the builder with an extent filter, which `build` emits even
    /// when no extent name was pushed.
    pub fn with_extent_filter(mut self) -> Self {
        self.extent_hashes = Some(Vec::new());
        self
    }

    /// Adds the name of a content object that a row's extent references to
    /// the extent filter.
    ///
    /// # Panics
    ///
    /// Panics if the builder has no extent filter.
    pub fn push_extent_name(&mut self, name: &str) {
        self.extent_hashes
            .as_mut()
            .expect("a segment builder should have an extent filter before it takes extent names")
            .push(filter_key_hashes(name));
    }

    /// Appends one row.
    ///
    /// `filter_key` is the lookup prefix point reads will probe for this row;
    /// the caller derives it per family. The row is any CBOR payload; a segment
    /// must contain one row type throughout, named by the descriptor family that
    /// references it.
    pub fn push<R: Serialize>(
        &mut self,
        row_key: &str,
        filter_key: &str,
        row: &R,
    ) -> Result<(), SegmentBlockCodecError> {
        self.push_with_encoded_row(row_key, filter_key, row)
            .map(drop)
    }

    /// Adds a filter key that no row of its own carries, such as the key that
    /// a run of rows sharing a prefix adds for a scan of that prefix.
    ///
    /// The key counts as one more insertion when the filter is sized.
    pub fn push_filter_key(&mut self, filter_key: &str) {
        self.filter_hashes.push(filter_key_hashes(filter_key));
    }

    /// Appends one row and returns its CBOR encoding.
    pub fn push_with_encoded_row<R: Serialize>(
        &mut self,
        row_key: &str,
        filter_key: &str,
        row: &R,
    ) -> Result<Vec<u8>, SegmentBlockCodecError> {
        if self.row_count > 0 && row_key < self.previous_key.as_str() {
            return Err(SegmentBlockCodecError::RowKeysOutOfOrder {
                previous: self.previous_key.clone(),
                offered: row_key.to_owned(),
            });
        }
        if self.row_count == 0 {
            self.min_row_key = row_key.to_owned();
        }
        self.filter_hashes.push(filter_key_hashes(filter_key));

        let restart = self.entry_count.is_multiple_of(RESTART_INTERVAL);
        if restart {
            self.restarts.push(
                u32::try_from(self.entries.len())
                    .expect("an unfinished data block should be below its target size"),
            );
        }
        let shared_len = if restart {
            0
        } else {
            shared_prefix_len(&self.previous_key, row_key)
        };
        let suffix = &row_key.as_bytes()[shared_len..];
        let mut row_bytes = Vec::new();
        ciborium::ser::into_writer(row, &mut row_bytes)
            .map_err(|error| SegmentBlockCodecError::Codec(error.to_string()))?;
        let previous_len = self.entries.len();
        write_varint(&mut self.entries, shared_len as u64);
        write_varint(&mut self.entries, suffix.len() as u64);
        self.entries.extend_from_slice(suffix);
        write_varint(&mut self.entries, row_bytes.len() as u64);
        self.entries.extend_from_slice(&row_bytes);

        self.decoded_data_bytes += self.entries.len() - previous_len;
        self.entry_count += 1;
        self.row_count += 1;
        self.previous_key.clear();
        self.previous_key.push_str(row_key);
        if self.entries.len() >= self.target_block_bytes {
            self.finish_data_block()?;
        }
        Ok(row_bytes)
    }

    fn finish_data_block(&mut self) -> Result<(), SegmentBlockCodecError> {
        if self.entries.is_empty() {
            return Ok(());
        }
        let mut payload = std::mem::take(&mut self.entries);
        for restart in &self.restarts {
            payload.extend_from_slice(&restart.to_le_bytes());
        }
        let restart_count =
            u32::try_from(self.restarts.len()).expect("a block's restart count should fit u32");
        payload.extend_from_slice(&restart_count.to_le_bytes());
        self.decoded_data_bytes += (self.restarts.len() + 1) * 4;
        self.restarts.clear();
        self.entry_count = 0;
        let block = append_section(&mut self.bytes, &payload, true)?;
        self.index.push(SegmentIndexEntry {
            last_row_key: self.previous_key.clone(),
            block,
        });
        Ok(())
    }

    /// Returns the number of rows accepted by this builder.
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Returns the decoded data bytes accepted so far, including block restart
    /// tables.
    ///
    /// The open block's pending restart table is included.
    pub fn decoded_data_bytes(&self) -> usize {
        self.decoded_data_bytes
            + if self.entries.is_empty() {
                0
            } else {
                (self.restarts.len() + 1) * 4
            }
    }

    /// Encodes the remaining rows and assembles the object bytes.
    pub fn build(mut self) -> Result<BuiltSegmentBlocks, SegmentBlockCodecError> {
        if self.row_count == 0 {
            return Err(SegmentBlockCodecError::EmptySegment);
        }
        self.finish_data_block()?;
        let mut bytes = self.bytes;
        let index = self.index;

        // The extent filter goes ahead of the filter, so the fetch of a
        // filter and the index after it doesn't read the extent filter.
        let extent_filter = match &self.extent_hashes {
            Some(hashes) => Some(append_section(
                &mut bytes,
                &build_filter_payload(hashes, EXTENT_FILTER_BITS_PER_KEY, MIN_KEEP_FILTER_BITS),
                false,
            )?),
            None => None,
        };
        let filter_payload =
            build_filter_payload(&self.filter_hashes, FILTER_BITS_PER_KEY, FILTER_MIN_BITS);
        let filter = append_section(&mut bytes, &filter_payload, false)?;

        let mut index_payload = Vec::new();
        ciborium::ser::into_writer(&index, &mut index_payload)
            .map_err(|error| SegmentBlockCodecError::Codec(error.to_string()))?;
        let index = append_section(&mut bytes, &index_payload, true)?;

        Ok(BuiltSegmentBlocks {
            bytes,
            index,
            filter,
            extent_filter,
            row_count: self.row_count,
            min_row_key: self.min_row_key,
            max_row_key: self.previous_key,
        })
    }
}

/// Decodes the index block from exactly the bytes its handle names.
pub fn decode_index_block(
    stored: &[u8],
    handle: &BlockHandle,
) -> Result<Vec<SegmentIndexEntry>, SegmentBlockCodecError> {
    let payload = decode_section(stored, handle, true)?;
    let entries: Vec<SegmentIndexEntry> = ciborium::de::from_reader(payload.as_slice())
        .map_err(|error| SegmentBlockCodecError::Codec(error.to_string()))?;
    // Index keys must be sorted, and block ranges must tile their region:
    // every range fits in `u64`, and each block starts exactly where its
    // predecessor ends, because the builder writes blocks back to back. Range
    // lookup relies on key order; span loading and its bulk-read budget
    // rely on contiguous, overflow-free ranges.
    let mut previous: Option<(&String, u64)> = None;
    for entry in &entries {
        let end = entry
            .block
            .offset
            .checked_add(u64::from(entry.block.stored_bytes))
            .ok_or_else(|| {
                SegmentBlockCodecError::Malformed(format!(
                    "index block `{}` byte range overflows",
                    entry.last_row_key
                ))
            })?;
        if let Some((previous_key, previous_end)) = previous {
            if previous_key > &entry.last_row_key {
                return Err(SegmentBlockCodecError::Malformed(format!(
                    "index blocks out of key order: `{}` follows `{previous_key}`",
                    entry.last_row_key
                )));
            }
            if entry.block.offset != previous_end {
                return Err(SegmentBlockCodecError::Malformed(format!(
                    "index block `{}` does not start where `{previous_key}` ends",
                    entry.last_row_key
                )));
            }
        }
        previous = Some((&entry.last_row_key, end));
    }
    Ok(entries)
}

/// Decodes one data block from exactly the bytes its handle names.
pub fn decode_data_block(
    stored: &[u8],
    handle: &BlockHandle,
) -> Result<DecodedDataBlock, SegmentBlockCodecError> {
    decode_data_block_rows::<MetadataRow>(stored, handle)
}

/// Decodes one data block whose rows are `R`, for segment families whose
/// row payload is not [`MetadataRow`] (gram index segments).
pub fn decode_data_block_rows<R: serde::de::DeserializeOwned>(
    stored: &[u8],
    handle: &BlockHandle,
) -> Result<DecodedDataBlock<R>, SegmentBlockCodecError> {
    let payload = decode_section(stored, handle, true)?;
    if payload.len() < 4 {
        return Err(SegmentBlockCodecError::Malformed(
            "data block shorter than its restart count".to_owned(),
        ));
    }
    let (body, restart_count_bytes) = payload.split_at(payload.len() - 4);
    let restart_count = u32::from_le_bytes(
        restart_count_bytes
            .try_into()
            .expect("split_at should leave exactly four bytes"),
    ) as usize;
    let restarts_len = restart_count
        .checked_mul(4)
        .filter(|len| *len <= body.len())
        .ok_or_else(|| {
            SegmentBlockCodecError::Malformed("restart array exceeds block".to_owned())
        })?;
    let (entries, restarts) = body.split_at(body.len() - restarts_len);
    let (restarts, _) = restarts.as_chunks::<4>();
    let mut restarts = restarts.iter();

    let mut row_keys = Vec::new();
    let mut rows = Vec::new();
    let mut cursor = 0usize;
    let mut previous_key = String::new();
    while cursor < entries.len() {
        let restart = rows.len().is_multiple_of(RESTART_INTERVAL);
        if restart {
            let offset = restarts.next().map(|bytes| u32::from_le_bytes(*bytes));
            if offset.map(u64::from) != Some(cursor as u64) {
                return Err(SegmentBlockCodecError::Malformed(
                    "restart offset disagrees with its entry".to_owned(),
                ));
            }
        }
        let shared_len = read_entry_length(entries, &mut cursor)?;
        let suffix_len = read_entry_length(entries, &mut cursor)?;
        if restart && shared_len != 0 {
            return Err(SegmentBlockCodecError::Malformed(
                "restart entry has a shared prefix".to_owned(),
            ));
        }
        if shared_len > previous_key.len() || !previous_key.is_char_boundary(shared_len) {
            return Err(SegmentBlockCodecError::Malformed(
                "shared prefix exceeds previous key".to_owned(),
            ));
        }
        let suffix = take_slice(entries, &mut cursor, suffix_len)?;
        let suffix = std::str::from_utf8(suffix)
            .map_err(|_| SegmentBlockCodecError::Malformed("row key is not utf-8".to_owned()))?;
        let key_len = shared_len.checked_add(suffix.len()).ok_or_else(|| {
            SegmentBlockCodecError::Malformed("row key length exceeds address space".to_owned())
        })?;
        let mut key = String::with_capacity(key_len);
        key.push_str(&previous_key[..shared_len]);
        key.push_str(suffix);
        let row_len = read_entry_length(entries, &mut cursor)?;
        let row_bytes = take_slice(entries, &mut cursor, row_len)?;
        let row: R = ciborium::de::from_reader(row_bytes)
            .map_err(|error| SegmentBlockCodecError::Codec(error.to_string()))?;
        // Ascending row-key order is a format requirement; readers
        // binary-search on it, so an out-of-order block is malformed.
        if key.as_str() < previous_key.as_str() {
            return Err(SegmentBlockCodecError::Malformed(format!(
                "rows out of row-key order: `{key}` follows `{previous_key}`"
            )));
        }
        previous_key.clear();
        previous_key.push_str(&key);
        row_keys.push(key);
        rows.push(row);
    }
    if restarts.next().is_some() {
        return Err(SegmentBlockCodecError::Malformed(
            "restart array contains unused offsets".to_owned(),
        ));
    }
    // Growth by doubling reserves up to twice the slots a block fills, and
    // a cached block keeps them for as long as it lives.
    row_keys.shrink_to_fit();
    rows.shrink_to_fit();
    Ok(DecodedDataBlock { row_keys, rows })
}

/// Decodes a filter block or an extent filter from exactly the bytes its
/// handle names.
pub fn decode_filter_block(
    stored: &[u8],
    handle: &BlockHandle,
) -> Result<SegmentFilter, SegmentBlockCodecError> {
    SegmentFilter::from_payload(&decode_section(stored, handle, false)?)
}

impl SegmentFilter {
    /// Decodes a filter from the payload of a filter block, which is how a
    /// filter that's stored outside a segment's filter block is read.
    pub fn from_payload(payload: &[u8]) -> Result<Self, SegmentBlockCodecError> {
        if payload.len() < 12 {
            return Err(SegmentBlockCodecError::Malformed(
                "filter block shorter than its header".to_owned(),
            ));
        }
        let n_hashes =
            u32::from_le_bytes(payload[0..4].try_into().expect(
                "the filter header length check should leave four bytes for the hash count",
            ));
        let bit_len =
            u64::from_le_bytes(payload[4..12].try_into().expect(
                "the filter header length check should leave eight bytes for the bit length",
            ));
        if n_hashes != FILTER_HASH_COUNT {
            return Err(SegmentBlockCodecError::Malformed(
                "filter hash count must be seven".to_owned(),
            ));
        }
        if bit_len < FILTER_MIN_BITS as u64 {
            return Err(SegmentBlockCodecError::Malformed(
                "filter bit length must be at least 64".to_owned(),
            ));
        }
        let bits = &payload[12..];
        if bit_len.div_ceil(8) != bits.len() as u64 {
            return Err(SegmentBlockCodecError::Malformed(
                "filter bit length disagrees with its bytes".to_owned(),
            ));
        }
        Ok(SegmentFilter {
            n_hashes,
            bit_len,
            bits: bits.to_vec(),
        })
    }

    /// Includes the filter value and the allocated capacity of its bit vector.
    pub fn decoded_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.bits.capacity()
    }

    /// Reports whether a row with this filter key may be in the segment: false
    /// means none is, and true means one may be.
    pub fn may_contain(&self, filter_key: &str) -> bool {
        self.may_contain_hashes(filter_key_hashes(filter_key))
    }

    /// Reports whether a key whose [`filter_key_hashes`] are `hashes` may be
    /// in the filter, so a caller that probes many filters with one key
    /// hashes the key once.
    pub fn may_contain_hashes(&self, (h1, h2): (u64, u64)) -> bool {
        for probe in 0..u64::from(self.n_hashes) {
            let bit = filter_bit((h1, h2), probe, self.bit_len);
            let byte = self.bits[crate::usize_from_u64(bit / 8)];
            if byte & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }
}

/// Returns the exclusive upper bound for every row key beginning with `prefix`.
///
/// Row keys are ordered as byte strings, so a prefix scan is the range
/// `[prefix, string_prefix_upper_bound(prefix))`. `None` means the prefix is
/// all `0xff` bytes and nothing sorts above it, so the scan runs to the end.
pub fn string_prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut bytes = prefix.as_bytes().to_vec();
    for index in (0..bytes.len()).rev() {
        if bytes[index] != u8::MAX {
            bytes[index] += 1;
            bytes.truncate(index + 1);
            return String::from_utf8(bytes).ok();
        }
    }
    None
}

/// Returns the index positions of the blocks that can contain keys in `[lower_bound,
/// upper_bound)`; `None` bounds the range at the last block.
pub fn index_blocks_for_key_range(
    index: &[SegmentIndexEntry],
    lower_bound: &str,
    upper_bound: Option<&str>,
) -> std::ops::Range<usize> {
    let start = index.partition_point(|entry| entry.last_row_key.as_str() < lower_bound);
    let end = upper_bound.map_or(index.len(), |upper_bound| {
        // A block whose last row key equals the exclusive upper bound can still
        // contain keys below it, so the bound block itself is included.
        index
            .partition_point(|entry| entry.last_row_key.as_str() < upper_bound)
            .saturating_add(1)
            .min(index.len())
    });
    start..end.max(start)
}

fn append_section(
    bytes: &mut Vec<u8>,
    payload: &[u8],
    compress: bool,
) -> Result<BlockHandle, SegmentBlockCodecError> {
    let stored = if compress {
        zstd::bulk::compress(payload, ZSTD_LEVEL)
            .map_err(|error| SegmentBlockCodecError::Codec(error.to_string()))?
    } else {
        payload.to_vec()
    };
    let handle_bytes = |len: usize| {
        u32::try_from(len).map_err(|_| {
            SegmentBlockCodecError::Malformed(format!("a {len}-byte block does not fit a handle"))
        })
    };
    let handle = BlockHandle {
        offset: bytes.len() as u64,
        stored_bytes: handle_bytes(stored.len())?,
        decoded_bytes: handle_bytes(payload.len())?,
        crc32c: crate::content::crc32c(&stored),
    };
    bytes.extend_from_slice(&stored);
    Ok(handle)
}

fn decode_section(
    stored: &[u8],
    handle: &BlockHandle,
    compressed: bool,
) -> Result<Vec<u8>, SegmentBlockCodecError> {
    if stored.len() != handle.stored_bytes as usize {
        return Err(SegmentBlockCodecError::StoredLengthMismatch {
            expected: handle.stored_bytes,
            actual: stored.len(),
        });
    }
    let actual = crate::content::crc32c(stored);
    if actual != handle.crc32c {
        return Err(SegmentBlockCodecError::ChecksumMismatch {
            expected: handle.crc32c,
            actual,
        });
    }
    let payload = if compressed {
        let mut payload = Vec::with_capacity(
            (handle.decoded_bytes as usize).min(DEFAULT_TARGET_BLOCK_BYTES.get()),
        );
        zstd::Decoder::new(stored)
            .and_then(|decoder| {
                decoder
                    .take(u64::from(handle.decoded_bytes) + 1)
                    .read_to_end(&mut payload)
            })
            .map_err(|error| SegmentBlockCodecError::Codec(error.to_string()))?;
        payload
    } else {
        if stored.len() != handle.decoded_bytes as usize {
            return Err(SegmentBlockCodecError::DecodedLengthMismatch {
                expected: handle.decoded_bytes,
                actual: stored.len(),
            });
        }
        stored.to_vec()
    };
    if payload.len() != handle.decoded_bytes as usize {
        return Err(SegmentBlockCodecError::DecodedLengthMismatch {
            expected: handle.decoded_bytes,
            actual: payload.len(),
        });
    }
    Ok(payload)
}

fn build_filter_payload(hashes: &[(u64, u64)], bits_per_key: usize, min_bits: usize) -> Vec<u8> {
    let mut builder = SegmentFilterBuilder::with_sizing(hashes.len(), bits_per_key, min_bits);
    for hash in hashes {
        builder.insert_hashes(*hash);
    }
    builder.finish()
}

/// Builds the payload of a filter block, for a filter that's stored outside
/// a segment's filter block.
#[derive(Debug)]
pub struct SegmentFilterBuilder {
    bit_len: u64,
    bits: Vec<u8>,
}

impl SegmentFilterBuilder {
    /// Returns a builder with [`FILTER_BITS_PER_KEY`] bits for each of
    /// `expected_keys` keys, and at least [`FILTER_MIN_BITS`] bits. Inserting
    /// more keys than expected raises the filter's false positive rate.
    pub fn new(expected_keys: usize) -> Self {
        Self::with_sizing(expected_keys, FILTER_BITS_PER_KEY, FILTER_MIN_BITS)
    }

    /// Returns a builder for a filter whose false match keeps data, with
    /// [`FILTER_BITS_PER_KEY`] bits for each of `expected_keys` keys, and at
    /// least [`MIN_KEEP_FILTER_BITS`] bits.
    pub fn keep_filter(expected_keys: usize) -> Self {
        Self::with_sizing(expected_keys, FILTER_BITS_PER_KEY, MIN_KEEP_FILTER_BITS)
    }

    fn with_sizing(expected_keys: usize, bits_per_key: usize, min_bits: usize) -> Self {
        let bit_len = expected_keys
            .saturating_mul(bits_per_key)
            .max(min_bits.max(FILTER_MIN_BITS));
        Self {
            bit_len: bit_len as u64,
            bits: vec![0; bit_len.div_ceil(8)],
        }
    }

    /// Inserts `filter_key`, so the filter may contain it.
    pub fn insert(&mut self, filter_key: &str) {
        self.insert_hashes(filter_key_hashes(filter_key));
    }

    fn insert_hashes(&mut self, hashes: (u64, u64)) {
        for probe in 0..u64::from(FILTER_HASH_COUNT) {
            let bit = filter_bit(hashes, probe, self.bit_len);
            self.bits[crate::usize_from_u64(bit / 8)] |= 1 << (bit % 8);
        }
    }

    /// Returns the filter block payload: the hash count, the bit length, and
    /// the bits.
    pub fn finish(self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(12 + self.bits.len());
        payload.extend_from_slice(&FILTER_HASH_COUNT.to_le_bytes());
        payload.extend_from_slice(&self.bit_len.to_le_bytes());
        payload.extend_from_slice(&self.bits);
        payload
    }
}

/// Returns the bit of a `bit_len`-bit filter that probe `probe` of a key with
/// the two hashes `(h1, h2)` sets and tests.
///
/// The bit depends on all 64 bits of both hashes. Without the mix, the bit
/// would depend only on each hash modulo `bit_len`, so a small filter would
/// have few distinct probe sequences, and a step of a small order (such as
/// zero) would test the same one or two bits seven times.
pub fn filter_bit((h1, h2): (u64, u64), probe: u64, bit_len: u64) -> u64 {
    crate::entropy::mix64(h1.wrapping_add(probe.wrapping_mul(h2))) % bit_len
}

/// Returns the two hashes of `filter_key` that every probe of a filter
/// derives its bit from.
pub fn filter_key_hashes(filter_key: &str) -> (u64, u64) {
    (
        xxh64(filter_key.as_bytes(), FILTER_HASH_SEED_ONE),
        xxh64(filter_key.as_bytes(), FILTER_HASH_SEED_TWO),
    )
}

fn shared_prefix_len(previous: &str, current: &str) -> usize {
    let mut len = previous
        .as_bytes()
        .iter()
        .zip(current.as_bytes())
        .take_while(|(a, b)| a == b)
        .count();
    // Both inputs are valid UTF-8 strings; back the byte-wise prefix off to
    // a character boundary so key reconstruction can slice the previous key.
    while !current.is_char_boundary(len) {
        len -= 1;
    }
    len
}

/// Appends `value` as an unsigned LEB128 integer.
pub fn write_varint(bytes: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            bytes.push(byte);
            return;
        }
        bytes.push(byte | 0x80);
    }
}

/// Reads the LEB128 varint at `cursor` and advances `cursor` past it.
pub fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, SegmentBlockCodecError> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(*cursor).ok_or_else(|| {
            SegmentBlockCodecError::Malformed("varint runs past the block".to_owned())
        })?;
        *cursor += 1;
        if shift == 63 && byte > 1 {
            return Err(SegmentBlockCodecError::Malformed(
                "varint exceeds 64 bits".to_owned(),
            ));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}

fn read_entry_length(bytes: &[u8], cursor: &mut usize) -> Result<usize, SegmentBlockCodecError> {
    usize::try_from(read_varint(bytes, cursor)?).map_err(|_| {
        SegmentBlockCodecError::Malformed("entry length exceeds address space".to_owned())
    })
}

fn take_slice<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
) -> Result<&'a [u8], SegmentBlockCodecError> {
    let end = cursor.checked_add(len).filter(|end| *end <= bytes.len());
    match end {
        Some(end) => {
            let slice = &bytes[*cursor..end];
            *cursor = end;
            Ok(slice)
        }
        None => Err(SegmentBlockCodecError::Malformed(
            "entry runs past the block".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests;
