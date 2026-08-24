//! Block-granular encoding for metadata and derived-index segments.
//!
//! A segment contains data blocks followed by one filter block and one index
//! block. The manifest descriptor stores the index and filter handles. Each
//! handle identifies a byte range and its CRC32C.
//!
//! The block format supports any CBOR row type, including [`MetadataRow`] and
//! grep index rows. The durable layout is:
//!
//! - A **data block** holds prefix-compressed entries: each entry stores
//!   `(shared_prefix_len, key_suffix_len)` as LEB128 varints, the key
//!   suffix, and a length-prefixed CBOR row. Every [`RESTART_INTERVAL`]th
//!   entry stores its full key. Restart offsets and their count end the
//!   zstd-compressed block.
//! - The **index block** is a zstd-compressed CBOR list with one entry per
//!   data block: the block's last row key and its [`BlockHandle`].
//! - The **filter block** is a bloom filter over caller-chosen filter keys
//!   and is stored without compression.
//! - Every section's CRC32C is stored in its handle.
//! - Bloom hashing uses two xxh64 hashes with fixed seeds.

use crate::wire::manifest::MetadataRow;
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::num::NonZeroUsize;
use thiserror::Error;
use xxhash_rust::xxh64::xxh64;

/// Target uncompressed size of one data block.
pub const DEFAULT_TARGET_BLOCK_BYTES: usize = 64 * 1024;
/// Number of level-zero runs that triggers reorganization.
pub const DEFAULT_MAX_DELTA_RUNS: usize = 8;
/// Target number of rows in one immutable segment.
pub const DEFAULT_MAX_ROWS_PER_SEGMENT: usize = 65_536;
/// Maximum number of runs read by one reorganization step.
pub const DEFAULT_MAX_REORGANIZATION_INPUT_RUNS: usize = 8;
/// Maximum number of decoded rows read by one reorganization step.
pub const DEFAULT_MAX_REORGANIZATION_INPUT_ROWS: usize = 131_072;
/// Maximum decoded input size for one build or reorganization step.
pub const DEFAULT_MAX_REORGANIZATION_INPUT_BYTES: usize = 64 * 1024 * 1024;
/// Maximum stored filter size embedded in a segment descriptor.
pub const DEFAULT_INLINE_FILTER_MAX_BYTES: u32 = 1024;
/// Entries between restart points inside a data block.
pub const RESTART_INTERVAL: usize = 16;
/// Bloom filter sizing: bits reserved per inserted filter key.
pub const FILTER_BITS_PER_KEY: usize = 10;
/// Bloom filter probe count, chosen for [`FILTER_BITS_PER_KEY`].
pub const FILTER_HASH_COUNT: u32 = 7;

const FILTER_HASH_SEED_ONE: u64 = 0;
const FILTER_HASH_SEED_TWO: u64 = 0x9e37_79b9_7f4a_7c15;
/// Compression level for every zstd-compressed durable artifact.
pub(crate) const ZSTD_LEVEL: i32 = 3;

/// Where one stored section lives inside a segment object, and how to
/// verify it: the CRC32C of the stored bytes and their decoded length.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockHandle {
    /// Zero-based byte offset of the section within its immutable segment object.
    pub offset: u64,
    /// Number of bytes to range-read and checksum before decoding.
    pub stored_len: u32,
    /// Expected byte length after optional section decompression.
    pub decoded_len: u32,
    /// CRC32C over the exact `stored_len` bytes at `offset`.
    pub crc32c: u32,
}

/// One index entry: the last row key of a data block plus its handle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentIndexEntry {
    /// Greatest row key in `block`, used to binary-search candidate blocks.
    pub last_row_key: String,
    /// Data-section location and integrity metadata.
    pub block: BlockHandle,
}

/// A finished segment: the object bytes plus everything the manifest
/// descriptor must carry to read them back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltSegmentBlocks {
    /// Complete immutable object body, with data sections followed by filter and index sections.
    pub bytes: Vec<u8>,
    /// Handle callers persist in the segment descriptor to bootstrap reads.
    pub index: BlockHandle,
    /// Handle callers persist for negative point-lookup filtering.
    pub filter: BlockHandle,
    /// Number of rows accepted by the builder, including adjacent duplicate keys.
    pub row_count: u64,
    /// Least row key in the non-empty segment.
    pub min_row_key: String,
    /// Greatest row key in the non-empty segment.
    pub max_row_key: String,
}

/// One decoded data block: row keys and rows, parallel and in key order.
/// The row type defaults to [`MetadataRow`]; index segments decode their
/// own row payload through [`decode_data_block_rows`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedDataBlock<R = MetadataRow> {
    /// Reconstructed row keys in the same ascending order as `rows`.
    pub row_keys: Vec<String>,
    /// Decoded row payloads positionally paired with `row_keys`.
    pub rows: Vec<R>,
}

/// A decoded bloom filter; answers "definitely absent" or "maybe present".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFilter {
    n_hashes: u32,
    bit_len: u64,
    bits: Vec<u8>,
}

/// Describes a violation encountered while building or validating an SST section.
///
/// See [metadata segments](../../../docs/specs/format.md#421-metadata-segments).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum SstBlockCodecError {
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
    finished_blocks: Vec<(String, Vec<u8>)>,
    filter_hashes: Vec<(u64, u64)>,
    row_count: u64,
    min_row_key: String,
}

impl Default for SegmentBlocksBuilder {
    fn default() -> Self {
        Self::new(const { NonZeroUsize::new(DEFAULT_TARGET_BLOCK_BYTES).unwrap() })
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
            finished_blocks: Vec::new(),
            filter_hashes: Vec::new(),
            row_count: 0,
            min_row_key: String::new(),
        }
    }

    /// Appends one row. `filter_key` is the lookup prefix point reads will
    /// probe for this row; the caller derives it per family. The row is any
    /// CBOR payload; a segment must hold one row type throughout, named by
    /// the descriptor family that references it.
    pub fn push<R: Serialize>(
        &mut self,
        row_key: &str,
        filter_key: &str,
        row: &R,
    ) -> Result<(), SstBlockCodecError> {
        if self.row_count > 0 && row_key < self.previous_key.as_str() {
            return Err(SstBlockCodecError::RowKeysOutOfOrder {
                previous: self.previous_key.clone(),
                offered: row_key.to_owned(),
            });
        }
        if self.row_count == 0 {
            self.min_row_key = row_key.to_owned();
        }
        self.filter_hashes.push(filter_key_hashes(filter_key));

        let restart = self.entry_count % RESTART_INTERVAL == 0;
        if restart {
            self.restarts.push(self.entries.len() as u32);
        }
        let shared_len = if restart {
            0
        } else {
            shared_prefix_len(&self.previous_key, row_key)
        };
        let suffix = &row_key.as_bytes()[shared_len..];
        let mut row_bytes = Vec::new();
        ciborium::ser::into_writer(row, &mut row_bytes)
            .map_err(|error| SstBlockCodecError::Codec(error.to_string()))?;
        write_varint(&mut self.entries, shared_len as u64);
        write_varint(&mut self.entries, suffix.len() as u64);
        self.entries.extend_from_slice(suffix);
        write_varint(&mut self.entries, row_bytes.len() as u64);
        self.entries.extend_from_slice(&row_bytes);

        self.entry_count += 1;
        self.row_count += 1;
        self.previous_key.clear();
        self.previous_key.push_str(row_key);
        if self.entries.len() >= self.target_block_bytes {
            self.finish_data_block();
        }
        Ok(())
    }

    fn finish_data_block(&mut self) {
        if self.entries.is_empty() {
            return;
        }
        let mut payload = std::mem::take(&mut self.entries);
        for restart in &self.restarts {
            payload.extend_from_slice(&restart.to_le_bytes());
        }
        payload.extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        self.restarts.clear();
        self.entry_count = 0;
        // The block copies the last row key it holds. Taking it would leave the
        // builder without one, and the builder still needs it: as the prefix
        // anchor inside the next block, as the order guard's floor across the
        // boundary, and as the segment's max row key once every row is in.
        self.finished_blocks
            .push((self.previous_key.clone(), payload));
    }

    /// Encodes the remaining rows and assembles the object bytes.
    pub fn finish(mut self) -> Result<BuiltSegmentBlocks, SstBlockCodecError> {
        if self.row_count == 0 {
            return Err(SstBlockCodecError::EmptySegment);
        }
        self.finish_data_block();

        let mut bytes = Vec::new();
        let mut index = Vec::with_capacity(self.finished_blocks.len());
        for (last_row_key, payload) in std::mem::take(&mut self.finished_blocks) {
            let block = append_section(&mut bytes, &payload, true)?;
            index.push(SegmentIndexEntry {
                last_row_key,
                block,
            });
        }

        let filter_payload = build_filter_payload(&self.filter_hashes);
        let filter = append_section(&mut bytes, &filter_payload, false)?;

        let mut index_payload = Vec::new();
        ciborium::ser::into_writer(&index, &mut index_payload)
            .map_err(|error| SstBlockCodecError::Codec(error.to_string()))?;
        let index = append_section(&mut bytes, &index_payload, true)?;

        Ok(BuiltSegmentBlocks {
            bytes,
            index,
            filter,
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
) -> Result<Vec<SegmentIndexEntry>, SstBlockCodecError> {
    let payload = decode_section(stored, handle, true)?;
    let entries: Vec<SegmentIndexEntry> = ciborium::de::from_reader(payload.as_slice())
        .map_err(|error| SstBlockCodecError::Codec(error.to_string()))?;
    // Index keys must be sorted, and block ranges must tile their region:
    // every range fits in `u64`, and each block starts exactly where its
    // predecessor ends — the builder writes blocks back to back. Range
    // lookup relies on key order; span loading and its bulk-read budget
    // rely on contiguous, overflow-free ranges.
    let mut previous: Option<(&String, u64)> = None;
    for entry in &entries {
        let end = entry
            .block
            .offset
            .checked_add(u64::from(entry.block.stored_len))
            .ok_or_else(|| {
                SstBlockCodecError::Malformed(format!(
                    "index block `{}` byte range overflows",
                    entry.last_row_key
                ))
            })?;
        if let Some((previous_key, previous_end)) = previous {
            if previous_key > &entry.last_row_key {
                return Err(SstBlockCodecError::Malformed(format!(
                    "index blocks out of key order: `{}` follows `{previous_key}`",
                    entry.last_row_key
                )));
            }
            if entry.block.offset != previous_end {
                return Err(SstBlockCodecError::Malformed(format!(
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
) -> Result<DecodedDataBlock, SstBlockCodecError> {
    decode_data_block_rows::<MetadataRow>(stored, handle)
}

/// Decodes one data block whose rows are `R`, for segment families whose
/// row payload is not [`MetadataRow`] (gram index segments).
pub fn decode_data_block_rows<R: serde::de::DeserializeOwned>(
    stored: &[u8],
    handle: &BlockHandle,
) -> Result<DecodedDataBlock<R>, SstBlockCodecError> {
    let payload = decode_section(stored, handle, true)?;
    if payload.len() < 4 {
        return Err(SstBlockCodecError::Malformed(
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
        .ok_or_else(|| SstBlockCodecError::Malformed("restart array exceeds block".to_owned()))?;
    let entries = &body[..body.len() - restarts_len];

    let mut row_keys = Vec::new();
    let mut rows = Vec::new();
    let mut cursor = 0usize;
    let mut previous_key = String::new();
    while cursor < entries.len() {
        let shared_len = read_varint(entries, &mut cursor)? as usize;
        let suffix_len = read_varint(entries, &mut cursor)? as usize;
        if shared_len > previous_key.len() || !previous_key.is_char_boundary(shared_len) {
            return Err(SstBlockCodecError::Malformed(
                "shared prefix exceeds previous key".to_owned(),
            ));
        }
        let suffix = take_slice(entries, &mut cursor, suffix_len)?;
        let suffix = std::str::from_utf8(suffix)
            .map_err(|_| SstBlockCodecError::Malformed("row key is not utf-8".to_owned()))?;
        let mut key = String::with_capacity(shared_len + suffix.len());
        key.push_str(&previous_key[..shared_len]);
        key.push_str(suffix);
        let row_len = read_varint(entries, &mut cursor)? as usize;
        let row_bytes = take_slice(entries, &mut cursor, row_len)?;
        let row: R = ciborium::de::from_reader(row_bytes)
            .map_err(|error| SstBlockCodecError::Codec(error.to_string()))?;
        // Ascending row-key order is a format requirement; readers
        // binary-search on it, so an out-of-order block is malformed.
        if key.as_str() < previous_key.as_str() {
            return Err(SstBlockCodecError::Malformed(format!(
                "rows out of row-key order: `{key}` follows `{previous_key}`"
            )));
        }
        previous_key.clear();
        previous_key.push_str(&key);
        row_keys.push(key);
        rows.push(row);
    }
    Ok(DecodedDataBlock { row_keys, rows })
}

/// Decodes the filter block from exactly the bytes its handle names.
pub fn decode_filter_block(
    stored: &[u8],
    handle: &BlockHandle,
) -> Result<SegmentFilter, SstBlockCodecError> {
    let payload = decode_section(stored, handle, false)?;
    if payload.len() < 12 {
        return Err(SstBlockCodecError::Malformed(
            "filter block shorter than its header".to_owned(),
        ));
    }
    let n_hashes = u32::from_le_bytes(
        payload[0..4]
            .try_into()
            .expect("header length should be checked above"),
    );
    let bit_len = u64::from_le_bytes(
        payload[4..12]
            .try_into()
            .expect("header length should be checked above"),
    );
    let bits = payload[12..].to_vec();
    if bit_len.div_ceil(8) != bits.len() as u64 {
        return Err(SstBlockCodecError::Malformed(
            "filter bit length disagrees with its bytes".to_owned(),
        ));
    }
    Ok(SegmentFilter {
        n_hashes,
        bit_len,
        bits,
    })
}

impl SegmentFilter {
    /// False means no row with this filter key is in the segment; true
    /// means one may be.
    pub fn may_contain(&self, filter_key: &str) -> bool {
        if self.bit_len == 0 {
            return false;
        }
        let (h1, h2) = filter_key_hashes(filter_key);
        for probe in 0..u64::from(self.n_hashes) {
            let bit = h1.wrapping_add(probe.wrapping_mul(h2)) % self.bit_len;
            let byte = self.bits[(bit / 8) as usize];
            if byte & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }
}

/// The exclusive upper bound for every row key beginning with `prefix`.
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

/// Index positions of the blocks that can hold keys in
/// `[lower_bound, upper_bound)`; `None` bounds the range at the last block.
pub fn index_blocks_for_key_range(
    index: &[SegmentIndexEntry],
    lower_bound: &str,
    upper_bound: Option<&str>,
) -> std::ops::Range<usize> {
    let start = index.partition_point(|entry| entry.last_row_key.as_str() < lower_bound);
    let end = upper_bound.map_or(index.len(), |upper_bound| {
        // A block whose last row key equals the exclusive upper bound can still
        // hold keys below it, so the bound block itself is included.
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
) -> Result<BlockHandle, SstBlockCodecError> {
    let stored = if compress {
        zstd::bulk::compress(payload, ZSTD_LEVEL)
            .map_err(|error| SstBlockCodecError::Codec(error.to_string()))?
    } else {
        payload.to_vec()
    };
    let handle = BlockHandle {
        offset: bytes.len() as u64,
        stored_len: stored.len() as u32,
        decoded_len: payload.len() as u32,
        crc32c: crc32c::crc32c(&stored),
    };
    bytes.extend_from_slice(&stored);
    Ok(handle)
}

fn decode_section(
    stored: &[u8],
    handle: &BlockHandle,
    compressed: bool,
) -> Result<Vec<u8>, SstBlockCodecError> {
    if stored.len() != handle.stored_len as usize {
        return Err(SstBlockCodecError::StoredLengthMismatch {
            expected: handle.stored_len,
            actual: stored.len(),
        });
    }
    let actual = crc32c::crc32c(stored);
    if actual != handle.crc32c {
        return Err(SstBlockCodecError::ChecksumMismatch {
            expected: handle.crc32c,
            actual,
        });
    }
    let payload = if compressed {
        let mut payload = Vec::with_capacity(handle.decoded_len as usize);
        zstd::Decoder::new(stored)
            .and_then(|mut decoder| decoder.read_to_end(&mut payload))
            .map_err(|error| SstBlockCodecError::Codec(error.to_string()))?;
        payload
    } else {
        stored.to_vec()
    };
    if payload.len() != handle.decoded_len as usize {
        return Err(SstBlockCodecError::DecodedLengthMismatch {
            expected: handle.decoded_len,
            actual: payload.len(),
        });
    }
    Ok(payload)
}

fn build_filter_payload(hashes: &[(u64, u64)]) -> Vec<u8> {
    let bit_len = (hashes.len() * FILTER_BITS_PER_KEY).max(64) as u64;
    let mut bits = vec![0u8; bit_len.div_ceil(8) as usize];
    for (h1, h2) in hashes {
        for probe in 0..u64::from(FILTER_HASH_COUNT) {
            let bit = h1.wrapping_add(probe.wrapping_mul(*h2)) % bit_len;
            bits[(bit / 8) as usize] |= 1 << (bit % 8);
        }
    }
    let mut payload = Vec::with_capacity(12 + bits.len());
    payload.extend_from_slice(&FILTER_HASH_COUNT.to_le_bytes());
    payload.extend_from_slice(&bit_len.to_le_bytes());
    payload.extend_from_slice(&bits);
    payload
}

fn filter_key_hashes(filter_key: &str) -> (u64, u64) {
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
pub fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, SstBlockCodecError> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(*cursor).ok_or_else(|| {
            SstBlockCodecError::Malformed("varint runs past the block".to_owned())
        })?;
        *cursor += 1;
        if shift >= 64 {
            return Err(SstBlockCodecError::Malformed(
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

fn take_slice<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
) -> Result<&'a [u8], SstBlockCodecError> {
    let end = cursor.checked_add(len).filter(|end| *end <= bytes.len());
    match end {
        Some(end) => {
            let slice = &bytes[*cursor..end];
            *cursor = end;
            Ok(slice)
        }
        None => Err(SstBlockCodecError::Malformed(
            "entry runs past the block".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChangeSeq, InodeId, InodeKind};

    fn inode_row(inode_id: u64) -> (String, String, MetadataRow) {
        let row = MetadataRow::Inode {
            inode_id: InodeId(inode_id),
            inode_kind: InodeKind::File,
            created_seq: ChangeSeq(inode_id),
            commit_id: crate::CommitId::parse(format!("c_row_{inode_id}"))
                .expect("valid commit id"),
            created_by: crate::ActorRef::loonfs_system(),
            created_at_ms: inode_id,
        };
        let key = row.row_key();
        (key.clone(), key, row)
    }

    fn build_segment(rows: usize) -> BuiltSegmentBlocks {
        let mut builder = SegmentBlocksBuilder::default();
        for index in 0..rows {
            let (key, filter_key, row) = inode_row(index as u64);
            builder.push(&key, &filter_key, &row).expect("push row");
        }
        builder.finish().expect("finish segment")
    }

    const SHARED_SEGMENT_ROWS: usize = 5_000;

    fn shared_segment() -> &'static BuiltSegmentBlocks {
        static SEGMENT: std::sync::OnceLock<BuiltSegmentBlocks> = std::sync::OnceLock::new();
        SEGMENT.get_or_init(|| build_segment(SHARED_SEGMENT_ROWS))
    }

    fn section<'a>(bytes: &'a [u8], handle: &BlockHandle) -> &'a [u8] {
        &bytes[handle.offset as usize..handle.offset as usize + handle.stored_len as usize]
    }

    fn encode_index(entries: &[SegmentIndexEntry]) -> (Vec<u8>, BlockHandle) {
        let mut payload = Vec::new();
        ciborium::ser::into_writer(entries, &mut payload).expect("encode index");
        let mut bytes = Vec::new();
        let handle = append_section(&mut bytes, &payload, true).expect("append section");
        (bytes, handle)
    }

    fn index_entry(last_row_key: &str, offset: u64, stored_len: u32) -> SegmentIndexEntry {
        SegmentIndexEntry {
            last_row_key: last_row_key.to_owned(),
            block: BlockHandle {
                offset,
                stored_len,
                decoded_len: stored_len,
                crc32c: 0,
            },
        }
    }

    #[test]
    fn segment_round_trips_every_row_through_index_and_blocks() {
        let rows = SHARED_SEGMENT_ROWS;
        let built = shared_segment();
        let index =
            decode_index_block(section(&built.bytes, &built.index), &built.index).expect("index");
        assert!(index.len() > 1, "5k inode rows should span several blocks");

        let mut recovered = Vec::new();
        for entry in &index {
            let block = decode_data_block(section(&built.bytes, &entry.block), &entry.block)
                .expect("data block");
            assert_eq!(block.row_keys.len(), block.rows.len());
            assert_eq!(
                block.row_keys.last().expect("blocks are never empty"),
                &entry.last_row_key
            );
            recovered.extend(block.row_keys.iter().cloned());
        }
        let expected: Vec<String> = (0..rows).map(|i| inode_row(i as u64).0).collect();
        assert_eq!(recovered, expected);
        assert_eq!(built.row_count, rows as u64);
        assert_eq!(built.min_row_key, expected[0]);
        assert_eq!(&built.max_row_key, expected.last().expect("rows"));
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
            let block = decode_data_block(section(&built.bytes, &entry.block), &entry.block)
                .expect("block");
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
        assert!(matches!(
            error,
            SstBlockCodecError::RowKeysOutOfOrder { .. }
        ));

        let error = SegmentBlocksBuilder::default()
            .finish()
            .expect_err("empty segment should be rejected");
        assert!(matches!(error, SstBlockCodecError::EmptySegment));
    }

    #[test]
    fn max_key_survives_a_last_row_that_closes_its_block() {
        // Calibrate the target so the crossing lands on the final row.
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
        let entry_bytes = calibration[0].block.decoded_len as usize - 4 * restarts - 4;

        let mut builder =
            SegmentBlocksBuilder::new(NonZeroUsize::new(entry_bytes).expect("positive target"));
        for index in 0..rows {
            let (key, filter_key, row) = inode_row(index as u64);
            builder.push(&key, &filter_key, &row).expect("push row");
        }
        let built = builder.finish().expect("finish segment");

        let expected_max = inode_row((rows - 1) as u64).0;
        assert_eq!(built.min_row_key, inode_row(0).0);
        assert_eq!(built.max_row_key, expected_max);
        assert_eq!(built.row_count, rows as u64);
        // The last push closed the block, so `finish` appended nothing. The
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
            let built = builder.finish().expect("finish segment");
            assert_eq!(built.min_row_key, expected[0], "target {target}");
            assert_eq!(
                &built.max_row_key,
                expected.last().expect("rows"),
                "target {target}"
            );
            assert_eq!(built.row_count, rows as u64, "target {target}");

            let index = decode_index_block(section(&built.bytes, &built.index), &built.index)
                .expect("index");
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
        assert!(
            matches!(
                &error,
                SstBlockCodecError::RowKeysOutOfOrder { previous, offered }
                    if previous == &key_high && offered == &key_low
            ),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn adjacent_equal_keys_are_permitted() {
        let mut builder = SegmentBlocksBuilder::default();
        let (key, filter_key, row) = inode_row(7);
        builder.push(&key, &filter_key, &row).expect("first copy");
        builder.push(&key, &filter_key, &row).expect("second copy");
        let built = builder.finish().expect("finish");
        assert_eq!(built.row_count, 2);
    }

    #[test]
    fn corrupted_sections_fail_their_checksums() {
        let built = build_segment(200);
        let index =
            decode_index_block(section(&built.bytes, &built.index), &built.index).expect("index");

        let mut corrupted = built.bytes.clone();
        let target = index[0].block.offset as usize + 3;
        corrupted[target] ^= 0xff;
        let error = decode_data_block(section(&corrupted, &index[0].block), &index[0].block)
            .expect_err("corrupted data block should fail");
        assert!(matches!(error, SstBlockCodecError::ChecksumMismatch { .. }));

        let mut corrupted = built.bytes.clone();
        let target = built.index.offset as usize + 3;
        corrupted[target] ^= 0xff;
        let error = decode_index_block(section(&corrupted, &built.index), &built.index)
            .expect_err("corrupted index should fail");
        assert!(matches!(error, SstBlockCodecError::ChecksumMismatch { .. }));

        let mut corrupted = built.bytes.clone();
        let target = built.filter.offset as usize + 12;
        corrupted[target] ^= 0xff;
        let error = decode_filter_block(section(&corrupted, &built.filter), &built.filter)
            .expect_err("corrupted filter should fail");
        assert!(matches!(error, SstBlockCodecError::ChecksumMismatch { .. }));
    }

    #[test]
    fn filter_has_no_false_negatives_and_few_false_positives() {
        let rows = 2_000;
        let built = build_segment(rows);
        let filter = decode_filter_block(section(&built.bytes, &built.filter), &built.filter)
            .expect("filter");
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
        payload.extend_from_slice(&0u32.to_le_bytes());
        let mut bytes = Vec::new();
        let handle = append_section(&mut bytes, &payload, true).expect("append section");

        let error =
            decode_data_block(&bytes, &handle).expect_err("descending rows should be rejected");
        assert!(
            matches!(&error, SstBlockCodecError::Malformed(message) if message.contains("row-key order")),
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

        let mut bytes = Vec::new();
        let handle = append_section(&mut bytes, &payload, true).expect("append section");
        let error = decode_data_block(&bytes, &handle)
            .expect_err("a partial utf-8 prefix should be rejected");
        assert!(matches!(
            &error,
            SstBlockCodecError::Malformed(message)
                if message == "shared prefix exceeds previous key"
        ));
    }

    #[test]
    fn decoding_rejects_out_of_order_index_entries() {
        let entries = vec![
            index_entry("inode-00000000000000000009", 0, 1),
            index_entry("inode-00000000000000000003", 0, 1),
        ];
        let (bytes, handle) = encode_index(&entries);

        let error =
            decode_index_block(&bytes, &handle).expect_err("descending index should be rejected");
        assert!(
            matches!(&error, SstBlockCodecError::Malformed(message) if message.contains("key order")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn decoding_rejects_out_of_order_index_offsets() {
        let entries = [index_entry("a", 10, 1), index_entry("b", 5, 1)];
        let (bytes, handle) = encode_index(&entries);

        let error = decode_index_block(&bytes, &handle)
            .expect_err("descending block offsets should be rejected");
        assert!(
            matches!(&error, SstBlockCodecError::Malformed(message) if message.contains("does not start where")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn decoding_rejects_overlapping_index_ranges() {
        let entries = [index_entry("a", 10, 5), index_entry("b", 14, 1)];
        let (bytes, handle) = encode_index(&entries);

        let error = decode_index_block(&bytes, &handle)
            .expect_err("overlapping block ranges should be rejected");
        assert!(
            matches!(&error, SstBlockCodecError::Malformed(message) if message.contains("does not start where")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn decoding_rejects_a_gap_between_index_blocks() {
        let entries = [index_entry("a", 0, 10), index_entry("b", 20, 1)];
        let (bytes, handle) = encode_index(&entries);

        let error = decode_index_block(&bytes, &handle)
            .expect_err("a gap between block ranges should be rejected");
        assert!(
            matches!(&error, SstBlockCodecError::Malformed(message) if message.contains("does not start where")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn decoding_rejects_a_single_block_range_past_the_integer_edge() {
        let entries = [index_entry("a", u64::MAX - 10, 100)];
        let (bytes, handle) = encode_index(&entries);

        let error = decode_index_block(&bytes, &handle)
            .expect_err("an overflowing single range should be rejected");
        assert!(
            matches!(&error, SstBlockCodecError::Malformed(message) if message.contains("byte range overflows")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn decoding_rejects_a_final_block_range_past_the_integer_edge() {
        let entries = [
            index_entry("a", u64::MAX - 110, 100),
            index_entry("b", u64::MAX - 10, 100),
        ];
        let (bytes, handle) = encode_index(&entries);

        let error = decode_index_block(&bytes, &handle)
            .expect_err("a trailing overflowing range should be rejected");
        assert!(
            matches!(&error, SstBlockCodecError::Malformed(message) if message.contains("byte range overflows")),
            "unexpected error: {error}"
        );
    }
}
