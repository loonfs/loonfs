//! Gram tokenizer and durable segment codecs for LoonFS grep.
//!
//! The frozen tokenizer, posting-batch, and row-key grammar is specified in
//! [`docs/specs/format.md` Appendix D](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#appendix-d-grep-extension-format).

use loonfs_types::format::segment_blocks::{read_varint, write_varint, SegmentBlockCodecError};
use loonfs_types::{InodeId, RevisionNo};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

/// Bytes per gram. Fixed by the frozen durable format.
pub(crate) const GRAM_LEN: usize = 3;
/// Largest content the version-1 eligibility rule admits, in bytes.
pub const MAX_INDEX_GRAMS_FILE_BYTES: u64 = 8 * 1024 * 1024;

/// True when content participates in the gram index: within the size cap
/// and text by the grep-family sniff (no NUL byte and valid UTF-8 in the
/// leading sample; a sample that ends inside a multi-byte character still
/// counts as valid).
pub(crate) fn is_indexable_text_content(content: &[u8]) -> bool {
    const ELIGIBILITY_SAMPLE_BYTES: usize = 8 * 1024;

    if content.len() as u64 > MAX_INDEX_GRAMS_FILE_BYTES {
        return false;
    }
    let sample = &content[..content.len().min(ELIGIBILITY_SAMPLE_BYTES)];
    if sample.contains(&0) {
        return false;
    }
    match std::str::from_utf8(sample) {
        Ok(_) => true,
        Err(error) => error.error_len().is_none() && error.valid_up_to() > 0,
    }
}

/// One tokenizer gram: three content bytes, ASCII-case-folded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Gram(pub [u8; GRAM_LEN]);

impl Gram {
    /// The gram as the six lowercase hex characters row keys embed.
    pub(crate) fn as_hex(&self) -> String {
        loonfs_types::format::hex::hex_encode_bytes(&self.0)
    }

    /// Parses the row-key hex form back into a gram.
    pub(crate) fn from_hex(hex: &str) -> Result<Self, IndexGramsCodecError> {
        let bytes = loonfs_types::format::hex::hex_decode_bytes(hex).map_err(|reason| {
            IndexGramsCodecError::InvalidGram {
                reason: reason.to_string(),
            }
        })?;
        let bytes: [u8; GRAM_LEN] =
            bytes
                .try_into()
                .map_err(|bytes: Vec<u8>| IndexGramsCodecError::InvalidGram {
                    reason: format!("expected {GRAM_LEN} bytes, got {}", bytes.len()),
                })?;
        Ok(Self(bytes))
    }
}

impl Serialize for Gram {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.as_hex())
    }
}

impl<'de> Deserialize<'de> for Gram {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let hex = String::deserialize(deserializer)?;
        Self::from_hex(&hex).map_err(serde::de::Error::custom)
    }
}

/// Extracts the distinct grams of one content buffer, in gram order.
/// Content shorter than one gram contributes nothing.
pub(crate) fn extract_grams(content: &[u8]) -> BTreeSet<Gram> {
    content
        .windows(GRAM_LEN)
        .map(|window| {
            let mut gram = [0u8; GRAM_LEN];
            for (folded, byte) in gram.iter_mut().zip(window) {
                *folded = byte.to_ascii_lowercase();
            }
            Gram(gram)
        })
        .collect()
}

/// One posting: a file revision whose content contains the gram.
///
/// Postings name durable `(inode_id, revision_no)` identity, never paths:
/// inode identity survives renames and forks, and paths are derived at
/// query time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct GramPosting {
    pub inode_id: InodeId,
    pub revision_no: RevisionNo,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum IndexGramsCodecError {
    #[error("a posting batch must contain at least one posting")]
    EmptyPostings,
    #[error(
        "posting inode {inode}/revision {revision} is not in strictly ascending \
         order after inode {previous_inode}/revision {previous_revision}"
    )]
    PostingsOutOfOrder {
        previous_inode: InodeId,
        previous_revision: u64,
        inode: InodeId,
        revision: u64,
    },
    #[error("malformed posting batch: {0}")]
    Malformed(String),
    #[error("invalid gram hex: {reason}")]
    InvalidGram { reason: String },
    #[error("gram postings row first inode {declared} does not match its batch's {actual}")]
    FirstInodeMismatch { declared: InodeId, actual: InodeId },
}

/// Bytes of one posting's inode ID in a batch.
const POSTING_INODE_ID_BYTES: usize = 16;

/// Packs postings into the frozen batch layout. The slice must be
/// non-empty and strictly ascending by `(inode_id, revision_no)`.
pub(crate) fn encode_gram_postings(
    postings: &[GramPosting],
) -> Result<Vec<u8>, IndexGramsCodecError> {
    if postings.is_empty() {
        return Err(IndexGramsCodecError::EmptyPostings);
    }
    // Each posting is its 16 inode ID bytes and a revision varint, which is
    // usually one byte.
    let mut bytes = Vec::with_capacity(1 + postings.len() * (POSTING_INODE_ID_BYTES + 1));
    write_varint(&mut bytes, postings.len() as u64);
    let mut previous: Option<GramPosting> = None;
    for posting in postings {
        if let Some(previous) = previous.filter(|previous| *posting <= *previous) {
            return Err(out_of_order(previous, *posting));
        }
        bytes.extend_from_slice(&posting.inode_id.to_be_bytes());
        write_varint(&mut bytes, posting.revision_no.0);
        previous = Some(*posting);
    }
    Ok(bytes)
}

/// Unpacks a posting batch, validating the count, strict ascending order,
/// and that the batch ends exactly where its bytes do.
pub(crate) fn decode_gram_postings(bytes: &[u8]) -> Result<Vec<GramPosting>, IndexGramsCodecError> {
    let mut cursor = 0usize;
    let count = read_varint(bytes, &mut cursor).map_err(malformed_varint)?;
    if count == 0 {
        return Err(IndexGramsCodecError::EmptyPostings);
    }
    let count = usize::try_from(count)
        .map_err(|_| IndexGramsCodecError::Malformed("posting count exceeds usize".to_owned()))?;
    if count > bytes.len() / (POSTING_INODE_ID_BYTES + 1) {
        // Every posting costs at least seventeen bytes past the count varint;
        // a count beyond that is corrupt, not just implausible.
        return Err(IndexGramsCodecError::Malformed(format!(
            "posting count {count} exceeds batch bytes {}",
            bytes.len()
        )));
    }
    let mut postings: Vec<GramPosting> = Vec::with_capacity(count);
    for _ in 0..count {
        let posting = GramPosting {
            inode_id: read_inode_id(bytes, &mut cursor)?,
            revision_no: read_revision_no(bytes, &mut cursor)?,
        };
        if let Some(previous) = postings.last().filter(|previous| posting <= **previous) {
            return Err(out_of_order(*previous, posting));
        }
        postings.push(posting);
    }
    if cursor != bytes.len() {
        return Err(IndexGramsCodecError::Malformed(format!(
            "{} trailing bytes after the last posting",
            bytes.len() - cursor
        )));
    }
    Ok(postings)
}

fn out_of_order(previous: GramPosting, posting: GramPosting) -> IndexGramsCodecError {
    IndexGramsCodecError::PostingsOutOfOrder {
        previous_inode: previous.inode_id,
        previous_revision: previous.revision_no.0,
        inode: posting.inode_id,
        revision: posting.revision_no.0,
    }
}

fn read_inode_id(bytes: &[u8], cursor: &mut usize) -> Result<InodeId, IndexGramsCodecError> {
    let end = cursor
        .checked_add(POSTING_INODE_ID_BYTES)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| {
            IndexGramsCodecError::Malformed("posting inode ID is truncated".to_owned())
        })?;
    let inode_id: [u8; POSTING_INODE_ID_BYTES] = bytes[*cursor..end]
        .try_into()
        .expect("the slice should have the inode ID's length");
    *cursor = end;
    Ok(InodeId::from_be_bytes(inode_id))
}

fn read_revision_no(bytes: &[u8], cursor: &mut usize) -> Result<RevisionNo, IndexGramsCodecError> {
    let value = read_varint(bytes, cursor).map_err(malformed_varint)?;
    RevisionNo::parse(value).map_err(|error| {
        IndexGramsCodecError::Malformed(format!("invalid posting revision `{value}`: {error}"))
    })
}

fn malformed_varint(error: SegmentBlockCodecError) -> IndexGramsCodecError {
    IndexGramsCodecError::Malformed(error.to_string())
}

/// One row of a gram index segment. Kind-tagged so a later released format
/// version can define additional row kinds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum IndexRow {
    GramPostings {
        gram: Gram,
        /// The batch's first inode id, repeated from the packed postings so
        /// row keys derive without decoding the batch. [`Self::postings`]
        /// validates the pairing.
        first_inode_id: InodeId,
        postings: serde_bytes::ByteBuf,
    },
}

impl IndexRow {
    /// Packs postings into a row for `gram`. The postings must be
    /// non-empty and strictly ascending, as for `encode_gram_postings`.
    pub fn gram_postings(
        gram: Gram,
        postings: &[GramPosting],
    ) -> Result<Self, IndexGramsCodecError> {
        let first = postings
            .first()
            .ok_or(IndexGramsCodecError::EmptyPostings)?;
        let first_inode_id = first.inode_id;
        let bytes = encode_gram_postings(postings)?;
        Ok(Self::GramPostings {
            gram,
            first_inode_id,
            postings: serde_bytes::ByteBuf::from(bytes),
        })
    }

    pub fn row_key(&self) -> String {
        match self {
            Self::GramPostings {
                gram,
                first_inode_id,
                ..
            } => format!(
                "{}{}-{}",
                lookup::GRAM_ROW_PREFIX,
                gram.as_hex(),
                first_inode_id.body()
            ),
        }
    }

    /// The lookup prefix a query probes for this row, and therefore the key
    /// inserted into the segment's bloom filter.
    pub fn filter_key(&self) -> String {
        match self {
            Self::GramPostings { gram, .. } => lookup::gram_probe(*gram),
        }
    }

    /// Unpacks and validates this row's posting batch, including that the
    /// declared first inode matches the batch.
    pub fn postings(&self) -> Result<Vec<GramPosting>, IndexGramsCodecError> {
        match self {
            Self::GramPostings {
                first_inode_id,
                postings,
                ..
            } => {
                let decoded = decode_gram_postings(postings)?;
                let actual = decoded
                    .first()
                    .expect("decoded posting batches should be nonempty")
                    .inode_id;
                if actual != *first_inode_id {
                    return Err(IndexGramsCodecError::FirstInodeMismatch {
                        declared: *first_inode_id,
                        actual,
                    });
                }
                Ok(decoded)
            }
        }
    }
}

/// Reader-side lookup grammar for gram index rows, defined beside the row
/// keys they select: a probe must equal the filter key the writer stored,
/// and a prefix must be a prefix of the row keys it selects.
pub mod lookup {
    use super::Gram;

    /// Scan floor shared by every gram row key.
    pub(crate) const GRAM_ROW_PREFIX: &str = "gram-";

    pub(crate) fn gram_probe(gram: Gram) -> String {
        format!("{GRAM_ROW_PREFIX}{}", gram.as_hex())
    }

    pub(crate) fn gram_prefix(gram: Gram) -> String {
        format!("{}-", gram_probe(gram))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_matches;

    fn posting(inode: u128, revision: u64) -> GramPosting {
        GramPosting {
            inode_id: InodeId(inode),
            revision_no: RevisionNo(revision),
        }
    }

    #[test]
    fn extraction_case_folds_ascii_and_dedups() {
        let grams = extract_grams(b"AbaBA");
        let expected: BTreeSet<Gram> = [Gram(*b"aba"), Gram(*b"bab")].into_iter().collect();
        assert_eq!(grams, expected);
    }

    #[test]
    fn extraction_keeps_non_ascii_bytes_verbatim() {
        // UTF-8 continuation bytes are above the ASCII range; folding must
        // not touch them, so the grams of multi-byte text stay exact.
        let content = "Grüße".as_bytes();
        let grams = extract_grams(content);
        assert!(grams.contains(&Gram([b'g', b'r', 0xc3])));
        assert!(grams.contains(&Gram([0xc3, 0xbc, 0xc3])));
    }

    #[test]
    fn extraction_of_short_content_is_empty() {
        assert!(extract_grams(b"").is_empty());
        assert!(extract_grams(b"ab").is_empty());
        assert_eq!(extract_grams(b"abc").len(), 1);
    }

    #[test]
    fn posting_batches_round_trip() {
        let postings = vec![
            posting(1, 1),
            posting(1, 4),
            posting(7, 2),
            posting(900, 1),
            posting(900, 2),
        ];
        let bytes = encode_gram_postings(&postings).expect("encode");
        assert_eq!(decode_gram_postings(&bytes).expect("decode"), postings);
    }

    #[test]
    fn posting_batches_reject_disorder_duplicates_and_empties() {
        assert_matches!(
            encode_gram_postings(&[]),
            Err(IndexGramsCodecError::EmptyPostings)
        );
        assert_matches!(
            encode_gram_postings(&[posting(2, 1), posting(1, 1)]),
            Err(IndexGramsCodecError::PostingsOutOfOrder { .. })
        );
        assert_matches!(
            encode_gram_postings(&[posting(1, 1), posting(1, 1)]),
            Err(IndexGramsCodecError::PostingsOutOfOrder { .. })
        );
    }

    #[test]
    fn posting_batch_decode_rejects_trailing_and_truncated_bytes() {
        let bytes = encode_gram_postings(&[posting(1, 1), posting(2, 1)]).expect("encode");

        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_matches!(
            decode_gram_postings(&trailing),
            Err(IndexGramsCodecError::Malformed(_))
        );

        let truncated = &bytes[..bytes.len() - 1];
        assert_matches!(
            decode_gram_postings(truncated),
            Err(IndexGramsCodecError::Malformed(_))
        );

        assert_matches!(
            decode_gram_postings(&[]),
            Err(IndexGramsCodecError::Malformed(_))
        );
    }

    #[test]
    fn posting_batches_pin_their_byte_layout() {
        let bytes = encode_gram_postings(&[posting(7, 1), posting(1 << 120, 300)]).expect("encode");
        let mut expected = vec![2];
        expected.extend_from_slice(&7_u128.to_be_bytes());
        expected.push(1);
        expected.extend_from_slice(&(1_u128 << 120).to_be_bytes());
        expected.extend_from_slice(&[0xac, 0x02]);
        assert_eq!(bytes, expected);
    }

    /// Builds a batch from raw `(inode bytes, revision varint)` postings.
    fn hand_built_batch(postings: &[(u128, u64)]) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_varint(&mut bytes, postings.len() as u64);
        for (inode, revision) in postings {
            bytes.extend_from_slice(&inode.to_be_bytes());
            write_varint(&mut bytes, *revision);
        }
        bytes
    }

    #[test]
    fn posting_batch_decode_rejects_oversized_revisions_but_allows_full_inode_range() {
        let full_inode_range = hand_built_batch(&[(u128::MAX, loonfs_types::MAX_PUBLIC_INTEGER)]);
        assert_eq!(
            decode_gram_postings(&full_inode_range).expect("full-range inode id"),
            vec![posting(u128::MAX, loonfs_types::MAX_PUBLIC_INTEGER)]
        );

        let first_revision_overflow =
            hand_built_batch(&[(1, loonfs_types::MAX_PUBLIC_INTEGER + 1)]);
        let error = decode_gram_postings(&first_revision_overflow)
            .expect_err("first revision above the public range");
        assert_matches!(
            error,
            IndexGramsCodecError::Malformed(message)
                if message.contains("must be an integer from 0 through 9007199254740991")
        );

        let later_revision_overflow =
            hand_built_batch(&[(1, 1), (2, loonfs_types::MAX_PUBLIC_INTEGER + 1)]);
        let error = decode_gram_postings(&later_revision_overflow)
            .expect_err("later revision above the public range");
        assert_matches!(
            error,
            IndexGramsCodecError::Malformed(message)
                if message.contains("must be an integer from 0 through 9007199254740991")
        );
    }

    #[test]
    fn posting_batch_decode_rejects_reordered_bytes() {
        // Valid postings in the wrong order: a repeat, then a lower inode.
        for postings in [[(5, 3), (5, 3)], [(1 << 100, 1), (5, 1)]] {
            assert_matches!(
                decode_gram_postings(&hand_built_batch(&postings)),
                Err(IndexGramsCodecError::PostingsOutOfOrder { .. })
            );
        }
    }

    #[test]
    fn row_keys_and_filter_keys_pin_their_shapes() {
        let row =
            IndexRow::gram_postings(Gram(*b"fox"), &[posting(42, 7), posting(43, 1)]).expect("row");
        assert_eq!(row.row_key(), "gram-666f78-0000000000000000000000001a");
        assert_eq!(row.filter_key(), "gram-666f78");
        assert_eq!(lookup::gram_prefix(Gram(*b"fox")), "gram-666f78-");
        assert_eq!(
            row.postings().expect("postings"),
            vec![posting(42, 7), posting(43, 1)]
        );
    }

    #[test]
    fn segment_max_row_key_survives_a_full_final_block() {
        use loonfs_types::format::segment_blocks::SegmentBlocksBuilder;

        let mut builder = SegmentBlocksBuilder::new(std::num::NonZeroUsize::MIN);
        let mut last_row_key = String::new();
        for gram in [Gram(*b"abc"), Gram(*b"abd"), Gram(*b"abe")] {
            let row = IndexRow::gram_postings(gram, &[posting(42, 7)]).expect("row");
            last_row_key = row.row_key();
            builder
                .push(&last_row_key, &row.filter_key(), &row)
                .expect("push");
        }
        let built = builder.build().expect("finish");
        assert_eq!(built.max_row_key, last_row_key);
        assert!(built.min_row_key <= built.max_row_key);
    }

    #[test]
    fn rows_reject_first_inode_mismatch() {
        let row = IndexRow::GramPostings {
            gram: Gram(*b"fox"),
            first_inode_id: InodeId(9),
            postings: serde_bytes::ByteBuf::from(
                encode_gram_postings(&[posting(42, 7)]).expect("encode"),
            ),
        };
        assert_matches!(
            row.postings(),
            Err(IndexGramsCodecError::FirstInodeMismatch {
                declared: InodeId(9),
                actual: InodeId(42)
            })
        );
    }

    #[test]
    fn gram_hex_round_trips_and_rejects_bad_input() {
        let gram = Gram([0x00, 0xab, 0xff]);
        assert_eq!(gram.as_hex(), "00abff");
        assert_eq!(Gram::from_hex("00abff").expect("parse"), gram);
        Gram::from_hex("00ab").expect_err("a two-byte gram should be rejected");
        Gram::from_hex("00abff00").expect_err("a four-byte gram should be rejected");
        Gram::from_hex("00abzz").expect_err("a gram with non-hex digits should be rejected");
    }
}
